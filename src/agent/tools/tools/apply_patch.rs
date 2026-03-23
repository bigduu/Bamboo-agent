use crate::agent::core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;

use super::edit::EditTool;

#[derive(Debug, Deserialize)]
struct ApplyPatchArgs {
    file_path: String,
    patch: String,
    #[serde(default)]
    line_number: Option<usize>,
}

pub struct ApplyPatchTool {
    edit_tool: EditTool,
}

impl ApplyPatchTool {
    pub fn new() -> Self {
        Self {
            edit_tool: EditTool::new(),
        }
    }
}

impl Default for ApplyPatchTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for ApplyPatchTool {
    fn name(&self) -> &str {
        "apply_patch"
    }

    fn description(&self) -> &str {
        "Apply SEARCH/REPLACE patch blocks to an existing file. This is the patch-only Edit flow."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "Absolute path to the file to patch"
                },
                "patch": {
                    "type": "string",
                    "description": "One or more <<<<<<< SEARCH / ======= / >>>>>>> REPLACE blocks"
                },
                "line_number": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Optional 1-based line hint to disambiguate duplicate SEARCH matches"
                }
            },
            "required": ["file_path", "patch"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        self.execute_with_context(args, ToolExecutionContext::none("apply_patch"))
            .await
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let parsed: ApplyPatchArgs = serde_json::from_value(args).map_err(|error| {
            ToolError::InvalidArguments(format!("Invalid apply_patch args: {error}"))
        })?;

        let mut edit_args = serde_json::Map::new();
        edit_args.insert("file_path".to_string(), json!(parsed.file_path));
        edit_args.insert("patch".to_string(), json!(parsed.patch));
        if let Some(line_number) = parsed.line_number {
            edit_args.insert("line_number".to_string(), json!(line_number));
        }

        let mut result = self
            .edit_tool
            .execute_with_context(serde_json::Value::Object(edit_args), ctx)
            .await?;

        if let Ok(mut payload) = serde_json::from_str::<serde_json::Value>(&result.result) {
            if payload
                .get("operation")
                .and_then(|value| value.as_str())
                .is_some_and(|value| value == "Edit")
            {
                payload["operation"] = json!("apply_patch");
            }
            if let Some(message) = payload.get("message").and_then(|value| value.as_str()) {
                payload["message"] = json!(message.replacen("Edited file", "Patched file", 1));
            }
            result.result = payload.to_string();
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::tools::tools::ReadTool;

    #[tokio::test]
    async fn apply_patch_delegates_to_edit_patch_mode() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "alpha\nbeta\n")
            .await
            .unwrap();
        let session_id = "session_apply_patch";
        let read_tool = ReadTool::new();
        let _ = read_tool
            .execute_with_context(
                json!({ "file_path": file.path() }),
                ToolExecutionContext {
                    session_id: Some(session_id),
                    tool_call_id: "call_1",
                    event_tx: None,
                },
            )
            .await
            .unwrap();

        let tool = ApplyPatchTool::new();
        let result = tool
            .execute_with_context(
                json!({
                    "file_path": file.path(),
                    "patch": "<<<<<<< SEARCH\nbeta\n=======\ngamma\n>>>>>>> REPLACE"
                }),
                ToolExecutionContext {
                    session_id: Some(session_id),
                    tool_call_id: "call_2",
                    event_tx: None,
                },
            )
            .await
            .unwrap();

        let updated = tokio::fs::read_to_string(file.path()).await.unwrap();
        assert_eq!(updated, "alpha\ngamma\n");
        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(payload["operation"], "apply_patch");
    }
}
