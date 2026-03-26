use crate::agent::core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;

const MAX_LIST_ITEMS: usize = 8;

#[derive(Debug, Deserialize)]
struct ConclusionArgs {
    conclusion: String,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    key_points: Vec<String>,
    #[serde(default)]
    next_steps: Vec<String>,
    #[serde(default)]
    confidence: Option<String>,
}

pub struct ConclusionTool;

impl ConclusionTool {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ConclusionTool {
    fn default() -> Self {
        Self::new()
    }
}

fn normalize_optional_text(value: Option<String>) -> Option<String> {
    value.and_then(|text| {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn normalize_text_list(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .take(MAX_LIST_ITEMS)
        .collect()
}

#[async_trait]
impl Tool for ConclusionTool {
    fn name(&self) -> &str {
        "conclusion"
    }

    fn description(&self) -> &str {
        "Present a structured final conclusion card with key points and optional next steps."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "conclusion": {
                    "type": "string",
                    "description": "Main conclusion text to show in the conclusion card."
                },
                "title": {
                    "type": "string",
                    "description": "Optional title for the card. Defaults to \"Conclusion\"."
                },
                "key_points": {
                    "type": "array",
                    "description": "Optional short bullet points supporting the conclusion.",
                    "items": {
                        "type": "string"
                    }
                },
                "next_steps": {
                    "type": "array",
                    "description": "Optional follow-up actions.",
                    "items": {
                        "type": "string"
                    }
                },
                "confidence": {
                    "type": "string",
                    "description": "Optional confidence label, for example high/medium/low."
                }
            },
            "required": ["conclusion"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let parsed: ConclusionArgs = serde_json::from_value(args).map_err(|error| {
            ToolError::InvalidArguments(format!("Invalid conclusion args: {error}"))
        })?;

        let conclusion = parsed.conclusion.trim();
        if conclusion.is_empty() {
            return Err(ToolError::InvalidArguments(
                "conclusion must be a non-empty string".to_string(),
            ));
        }

        let payload = json!({
            "type": "conclusion",
            "title": normalize_optional_text(parsed.title).unwrap_or_else(|| "Conclusion".to_string()),
            "conclusion": conclusion,
            "key_points": normalize_text_list(parsed.key_points),
            "next_steps": normalize_text_list(parsed.next_steps),
            "confidence": normalize_optional_text(parsed.confidence),
        });

        Ok(ToolResult {
            success: true,
            result: payload.to_string(),
            display_preference: Some("Collapsible".to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn conclusion_tool_has_correct_name() {
        let tool = ConclusionTool::new();
        assert_eq!(tool.name(), "conclusion");
    }

    #[test]
    fn conclusion_tool_schema_requires_conclusion() {
        let tool = ConclusionTool::new();
        let schema = tool.parameters_schema();

        assert_eq!(schema["type"], "object");
        assert!(schema["required"]
            .as_array()
            .expect("required array")
            .contains(&json!("conclusion")));
        assert_eq!(schema["additionalProperties"], false);
    }

    #[tokio::test]
    async fn conclusion_tool_returns_structured_payload() {
        let tool = ConclusionTool::new();
        let result = tool
            .execute(json!({
                "title": "Release Readiness",
                "conclusion": "Core flows are stable and ready.",
                "key_points": ["Tests passed", "No blocking regressions"],
                "next_steps": ["Ship release train"],
                "confidence": "high"
            }))
            .await
            .expect("tool should execute");

        assert!(result.success);
        assert_eq!(result.display_preference.as_deref(), Some("Collapsible"));

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("json payload");
        assert_eq!(payload["type"], "conclusion");
        assert_eq!(payload["title"], "Release Readiness");
        assert_eq!(payload["conclusion"], "Core flows are stable and ready.");
        assert_eq!(
            payload["key_points"],
            json!(["Tests passed", "No blocking regressions"])
        );
        assert_eq!(payload["next_steps"], json!(["Ship release train"]));
        assert_eq!(payload["confidence"], "high");
    }

    #[tokio::test]
    async fn conclusion_tool_defaults_title_and_rejects_empty_conclusion() {
        let tool = ConclusionTool::new();

        let ok_result = tool
            .execute(json!({
                "conclusion": "Done."
            }))
            .await
            .expect("tool should execute");
        let payload: serde_json::Value =
            serde_json::from_str(&ok_result.result).expect("json payload");
        assert_eq!(payload["title"], "Conclusion");

        let error = tool
            .execute(json!({
                "conclusion": "   "
            }))
            .await
            .expect_err("should reject empty conclusion");
        assert!(matches!(error, ToolError::InvalidArguments(_)));
    }

    #[tokio::test]
    async fn conclusion_tool_trims_and_limits_lists() {
        let tool = ConclusionTool::new();
        let result = tool
            .execute(json!({
                "conclusion": "Done",
                "key_points": ["  a  ", "", "b", "c", "d", "e", "f", "g", "h", "i"],
                "next_steps": ["  next  ", ""]
            }))
            .await
            .expect("tool should execute");
        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("json payload");
        assert_eq!(
            payload["key_points"],
            json!(["a", "b", "c", "d", "e", "f", "g", "h"])
        );
        assert_eq!(payload["next_steps"], json!(["next"]));
    }
}
