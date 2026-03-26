use crate::agent::core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
struct MermaidArgs {
    chart: String,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    summary: Option<String>,
}

pub struct MermaidTool;

impl MermaidTool {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MermaidTool {
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

#[async_trait]
impl Tool for MermaidTool {
    fn name(&self) -> &str {
        "mermaid"
    }

    fn description(&self) -> &str {
        "Render a Mermaid diagram result card with an optional title and summary."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "chart": {
                    "type": "string",
                    "description": "Mermaid diagram source text, for example: flowchart TD\\nA-->B."
                },
                "title": {
                    "type": "string",
                    "description": "Optional title displayed above the diagram."
                },
                "summary": {
                    "type": "string",
                    "description": "Optional one-line summary for the diagram."
                }
            },
            "required": ["chart"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let parsed: MermaidArgs = serde_json::from_value(args).map_err(|error| {
            ToolError::InvalidArguments(format!("Invalid mermaid args: {error}"))
        })?;

        let chart = parsed.chart.trim();
        if chart.is_empty() {
            return Err(ToolError::InvalidArguments(
                "chart must be a non-empty Mermaid definition".to_string(),
            ));
        }

        let payload = json!({
            "type": "mermaid",
            "title": normalize_optional_text(parsed.title),
            "summary": normalize_optional_text(parsed.summary),
            "chart": chart,
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
    fn mermaid_tool_has_correct_name() {
        let tool = MermaidTool::new();
        assert_eq!(tool.name(), "mermaid");
    }

    #[test]
    fn mermaid_schema_requires_chart() {
        let tool = MermaidTool::new();
        let schema = tool.parameters_schema();

        assert_eq!(schema["type"], "object");
        assert!(schema["required"]
            .as_array()
            .expect("required array")
            .contains(&json!("chart")));
        assert_eq!(schema["additionalProperties"], false);
    }

    #[tokio::test]
    async fn mermaid_tool_returns_structured_payload() {
        let tool = MermaidTool::new();
        let result = tool
            .execute(json!({
                "title": "Architecture",
                "summary": "Main request flow",
                "chart": "flowchart TD\nA[User] --> B[API]"
            }))
            .await
            .expect("tool should execute");

        assert!(result.success);
        assert_eq!(result.display_preference.as_deref(), Some("Collapsible"));

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("json payload");
        assert_eq!(payload["type"], "mermaid");
        assert_eq!(payload["title"], "Architecture");
        assert_eq!(payload["summary"], "Main request flow");
        assert_eq!(payload["chart"], "flowchart TD\nA[User] --> B[API]");
    }

    #[tokio::test]
    async fn mermaid_tool_rejects_empty_chart() {
        let tool = MermaidTool::new();
        let error = tool
            .execute(json!({
                "chart": "   "
            }))
            .await
            .expect_err("should reject empty chart");

        assert!(matches!(error, ToolError::InvalidArguments(_)));
    }
}
