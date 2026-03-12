use crate::agent::core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use std::path::Path;

use super::read_tracker::ReadState;
use super::{file_change, read_tracker};

const MAX_PATCH_BYTES: usize = 256 * 1024;
const MAX_PATCH_BLOCKS: usize = 128;
const MAX_PATCH_BLOCK_BYTES: usize = 64 * 1024;

#[derive(Debug, Deserialize)]
struct EditArgs {
    file_path: String,
    #[serde(default)]
    old_string: Option<String>,
    #[serde(default)]
    new_string: Option<String>,
    #[serde(default)]
    replace_all: Option<bool>,
    #[serde(default)]
    patch: Option<String>,
}

pub struct EditTool;

impl EditTool {
    pub fn new() -> Self {
        Self
    }

    fn apply_single_replacement(
        content: &str,
        old_string: &str,
        new_string: &str,
        replace_all: bool,
    ) -> Result<(String, usize), ToolError> {
        if old_string == new_string {
            return Err(ToolError::InvalidArguments(
                "new_string must be different from old_string".to_string(),
            ));
        }
        if old_string.is_empty() {
            return Err(ToolError::InvalidArguments(
                "old_string must be non-empty".to_string(),
            ));
        }

        let matches: Vec<usize> = content
            .match_indices(old_string)
            .map(|(index, _)| index)
            .collect();

        if matches.is_empty() {
            return Err(ToolError::Execution(
                "old_string not found in target file".to_string(),
            ));
        }

        if !replace_all && matches.len() != 1 {
            return Err(ToolError::Execution(format!(
                "old_string matched {} times; provide a more specific old_string, use patch mode, or set replace_all=true",
                matches.len()
            )));
        }

        let updated = if replace_all {
            content.replace(old_string, new_string)
        } else {
            let first = matches[0];
            let mut next = String::with_capacity(content.len() + new_string.len());
            next.push_str(&content[..first]);
            next.push_str(new_string);
            next.push_str(&content[first + old_string.len()..]);
            next
        };

        Ok((updated, if replace_all { matches.len() } else { 1 }))
    }

    fn parse_patch_blocks(patch: &str) -> Result<Vec<(String, String)>, ToolError> {
        const SEARCH: &str = "<<<<<<< SEARCH\n";
        const SEP: &str = "\n=======\n";
        const REPLACE: &str = "\n>>>>>>> REPLACE";

        let normalized = patch.replace("\r\n", "\n");
        if normalized.trim().is_empty() {
            return Err(ToolError::InvalidArguments(
                "patch must be non-empty".to_string(),
            ));
        }
        if normalized.len() > MAX_PATCH_BYTES {
            return Err(ToolError::InvalidArguments(format!(
                "patch exceeds max size of {} bytes",
                MAX_PATCH_BYTES
            )));
        }

        let mut cursor = 0usize;
        let mut blocks = Vec::new();
        while let Some(start_rel) = normalized[cursor..].find(SEARCH) {
            if blocks.len() >= MAX_PATCH_BLOCKS {
                return Err(ToolError::InvalidArguments(format!(
                    "patch exceeds max block count of {}",
                    MAX_PATCH_BLOCKS
                )));
            }
            let search_start = cursor + start_rel + SEARCH.len();
            let sep_rel = normalized[search_start..].find(SEP).ok_or_else(|| {
                ToolError::InvalidArguments("Malformed patch block: missing =======".to_string())
            })?;
            let sep_idx = search_start + sep_rel;
            let replace_start = sep_idx + SEP.len();
            let replace_rel = normalized[replace_start..].find(REPLACE).ok_or_else(|| {
                ToolError::InvalidArguments(
                    "Malformed patch block: missing >>>>>>> REPLACE".to_string(),
                )
            })?;
            let replace_idx = replace_start + replace_rel;

            let old_block = normalized[search_start..sep_idx].to_string();
            let new_block = normalized[replace_start..replace_idx].to_string();
            if old_block.is_empty() {
                return Err(ToolError::InvalidArguments(
                    "Patch SEARCH block must be non-empty".to_string(),
                ));
            }
            if old_block.len() > MAX_PATCH_BLOCK_BYTES || new_block.len() > MAX_PATCH_BLOCK_BYTES {
                return Err(ToolError::InvalidArguments(format!(
                    "Patch block exceeds max block size of {} bytes",
                    MAX_PATCH_BLOCK_BYTES
                )));
            }
            blocks.push((old_block, new_block));

            cursor = replace_idx + REPLACE.len();
            if normalized[cursor..].starts_with('\n') {
                cursor += 1;
            }
        }

        if blocks.is_empty() {
            return Err(ToolError::InvalidArguments(
                "patch must contain at least one SEARCH/REPLACE block".to_string(),
            ));
        }

        Ok(blocks)
    }

    fn apply_patch_mode(content: &str, patch: &str) -> Result<(String, usize), ToolError> {
        let blocks = Self::parse_patch_blocks(patch)?;
        let mut updated = content.to_string();
        let mut replacements = 0usize;

        for (idx, (old_block, new_block)) in blocks.iter().enumerate() {
            let matches: Vec<usize> = updated
                .match_indices(old_block)
                .map(|(index, _)| index)
                .collect();
            if matches.is_empty() {
                return Err(ToolError::Execution(format!(
                    "Patch block {} SEARCH content not found in target file",
                    idx + 1
                )));
            }
            if matches.len() != 1 {
                return Err(ToolError::Execution(format!(
                    "Patch block {} SEARCH content matched {} times; add more context to make it unique",
                    idx + 1,
                    matches.len()
                )));
            }

            let first = matches[0];
            let mut next = String::with_capacity(updated.len() + new_block.len());
            next.push_str(&updated[..first]);
            next.push_str(new_block);
            next.push_str(&updated[first + old_block.len()..]);
            updated = next;
            replacements += 1;
        }

        Ok((updated, replacements))
    }
}

impl Default for EditTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for EditTool {
    fn name(&self) -> &str {
        "Edit"
    }

    fn description(&self) -> &str {
        "Perform exact string replacements in files"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "The absolute path to the file to modify"
                },
                "old_string": {
                    "type": "string",
                    "description": "Legacy mode: exact text to replace"
                },
                "new_string": {
                    "type": "string",
                    "description": "Legacy mode: replacement text"
                },
                "replace_all": {
                    "type": "boolean",
                    "default": false,
                    "description": "Legacy mode only: replace all occurrences"
                },
                "patch": {
                    "type": "string",
                    "description": "Patch mode: one or more blocks using <<<<<<< SEARCH / ======= / >>>>>>> REPLACE"
                }
            },
            "required": ["file_path"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        self.execute_with_context(args, ToolExecutionContext::none("Edit"))
            .await
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let parsed: EditArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid Edit args: {}", e)))?;

        let file_path = parsed.file_path.trim();
        let path = Path::new(file_path);
        if !path.is_absolute() {
            return Err(ToolError::InvalidArguments(
                "file_path must be an absolute path".to_string(),
            ));
        }

        if let Some(session_id) = ctx.session_id {
            match read_tracker::read_state(session_id, file_path).await {
                ReadState::Unread => {
                    return Err(ToolError::Execution(
                        "Edit requires reading the target file first via Read".to_string(),
                    ));
                }
                ReadState::Stale => {
                    return Err(ToolError::Execution(
                        "Target file changed after last Read; call Read again before Edit"
                            .to_string(),
                    ));
                }
                ReadState::Fresh => {}
            }
        }

        let content = tokio::fs::read_to_string(path)
            .await
            .map_err(|e| ToolError::Execution(format!("Failed to read file: {}", e)))?;

        let patch = parsed
            .patch
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let old_string = parsed.old_string.as_deref();
        let new_string = parsed.new_string.as_deref();

        let (updated, replacements, mode_label) = if let Some(patch_text) = patch {
            if old_string.is_some() || new_string.is_some() || parsed.replace_all.is_some() {
                return Err(ToolError::InvalidArguments(
                    "patch mode cannot be combined with old_string/new_string/replace_all"
                        .to_string(),
                ));
            }
            let (next, count) = Self::apply_patch_mode(&content, patch_text)?;
            (next, count, "patch")
        } else {
            let old = old_string.ok_or_else(|| {
                ToolError::InvalidArguments(
                    "old_string is required unless patch mode is used".to_string(),
                )
            })?;
            let new = new_string.ok_or_else(|| {
                ToolError::InvalidArguments(
                    "new_string is required unless patch mode is used".to_string(),
                )
            })?;
            let (next, count) = Self::apply_single_replacement(
                &content,
                old,
                new,
                parsed.replace_all.unwrap_or(false),
            )?;
            (next, count, "legacy")
        };

        let checkpoint = file_change::create_checkpoint(path, Some(content.as_bytes())).await?;

        file_change::atomic_write_text(path, &updated).await?;

        let payload = file_change::build_file_change_payload(
            "Edit",
            path,
            format!(
                "Edited file: {} (mode: {}, replacements: {})",
                file_path, mode_label, replacements
            ),
            checkpoint,
            &content,
            &updated,
        );

        Ok(ToolResult {
            success: true,
            result: payload,
            display_preference: Some("Default".to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::tools::tools::ReadTool;
    use serde_json::json;

    #[tokio::test]
    async fn edit_requires_unique_match_without_replace_all() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "foo\nfoo\n").await.unwrap();

        let tool = EditTool::new();
        let result = tool
            .execute(json!({
                "file_path": file.path(),
                "old_string": "foo",
                "new_string": "bar"
            }))
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn edit_supports_replace_all() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "foo\nfoo\n").await.unwrap();

        let tool = EditTool::new();
        let result = tool
            .execute(json!({
                "file_path": file.path(),
                "old_string": "foo",
                "new_string": "bar",
                "replace_all": true
            }))
            .await
            .unwrap();

        assert!(result.success);
        let updated = tokio::fs::read_to_string(file.path()).await.unwrap();
        assert_eq!(updated, "bar\nbar\n");
    }

    #[tokio::test]
    async fn edit_requires_read_first_when_session_context_exists() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "hello world\n")
            .await
            .unwrap();
        let call_id = "call_1";

        let edit_tool = EditTool::new();
        let read_tool = ReadTool::new();

        let denied = edit_tool
            .execute_with_context(
                json!({
                    "file_path": file.path(),
                    "old_string": "world",
                    "new_string": "rust"
                }),
                ToolExecutionContext {
                    session_id: Some("session_1"),
                    tool_call_id: call_id,
                    event_tx: None,
                },
            )
            .await;
        assert!(denied.is_err());

        let _ = read_tool
            .execute_with_context(
                json!({"file_path": file.path()}),
                ToolExecutionContext {
                    session_id: Some("session_1"),
                    tool_call_id: call_id,
                    event_tx: None,
                },
            )
            .await
            .unwrap();

        let allowed = edit_tool
            .execute_with_context(
                json!({
                    "file_path": file.path(),
                    "old_string": "world",
                    "new_string": "rust"
                }),
                ToolExecutionContext {
                    session_id: Some("session_1"),
                    tool_call_id: call_id,
                    event_tx: None,
                },
            )
            .await
            .unwrap();

        assert!(allowed.success);
    }

    #[tokio::test]
    async fn edit_rejects_empty_old_string() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "hello").await.unwrap();

        let tool = EditTool::new();
        let result = tool
            .execute(json!({
                "file_path": file.path(),
                "old_string": "",
                "new_string": "x",
                "replace_all": true
            }))
            .await;

        assert!(matches!(result, Err(ToolError::InvalidArguments(_))));
    }

    #[tokio::test]
    async fn edit_patch_mode_can_target_second_duplicate_with_context() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(
            file.path(),
            "fn a() {\n    let v = 1;\n}\n\nfn b() {\n    let v = 1;\n}\n",
        )
        .await
        .unwrap();

        let tool = EditTool::new();
        let result = tool
            .execute(json!({
                "file_path": file.path(),
                "patch": "<<<<<<< SEARCH\nfn b() {\n    let v = 1;\n}\n=======\nfn b() {\n    let v = 2;\n}\n>>>>>>> REPLACE"
            }))
            .await
            .unwrap();
        assert!(result.success);

        let updated = tokio::fs::read_to_string(file.path()).await.unwrap();
        assert!(updated.contains("fn a() {\n    let v = 1;\n}"));
        assert!(updated.contains("fn b() {\n    let v = 2;\n}"));
    }

    #[tokio::test]
    async fn edit_patch_mode_rejects_ambiguous_search_block() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "x = 1;\nx = 1;\n")
            .await
            .unwrap();

        let tool = EditTool::new();
        let result = tool
            .execute(json!({
                "file_path": file.path(),
                "patch": "<<<<<<< SEARCH\nx = 1;\n=======\nx = 2;\n>>>>>>> REPLACE"
            }))
            .await;

        assert!(
            matches!(result, Err(ToolError::Execution(msg)) if msg.contains("matched 2 times"))
        );
    }

    #[tokio::test]
    async fn edit_rejects_mixed_patch_and_legacy_args() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "hello").await.unwrap();

        let tool = EditTool::new();
        let result = tool
            .execute(json!({
                "file_path": file.path(),
                "old_string": "hello",
                "new_string": "world",
                "patch": "<<<<<<< SEARCH\nhello\n=======\nworld\n>>>>>>> REPLACE"
            }))
            .await;

        assert!(
            matches!(result, Err(ToolError::InvalidArguments(msg)) if msg.contains("cannot be combined"))
        );
    }

    #[tokio::test]
    async fn edit_patch_rejects_oversized_patch_payload() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "hello world").await.unwrap();
        let huge = "a".repeat(MAX_PATCH_BYTES + 1);

        let tool = EditTool::new();
        let result = tool
            .execute(json!({
                "file_path": file.path(),
                "patch": huge
            }))
            .await;

        assert!(
            matches!(result, Err(ToolError::InvalidArguments(msg)) if msg.contains("max size"))
        );
    }

    #[tokio::test]
    async fn edit_patch_rejects_excessive_block_count() {
        let file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(file.path(), "hello world").await.unwrap();
        let mut patch = String::new();
        for _ in 0..=MAX_PATCH_BLOCKS {
            patch.push_str("<<<<<<< SEARCH\nx\n=======\ny\n>>>>>>> REPLACE\n");
        }

        let tool = EditTool::new();
        let result = tool
            .execute(json!({
                "file_path": file.path(),
                "patch": patch
            }))
            .await;

        assert!(
            matches!(result, Err(ToolError::InvalidArguments(msg)) if msg.contains("max block count"))
        );
    }
}
