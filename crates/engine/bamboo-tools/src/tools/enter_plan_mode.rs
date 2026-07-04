use async_trait::async_trait;
use bamboo_agent_core::{Tool, ToolCtx, ToolError, ToolOutcome, ToolResult};
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
struct EnterPlanModeArgs {
    #[serde(default)]
    reason: Option<String>,
}

pub struct EnterPlanModeTool;

impl EnterPlanModeTool {
    pub fn new() -> Self {
        Self
    }
}

impl Default for EnterPlanModeTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for EnterPlanModeTool {
    fn name(&self) -> &str {
        "EnterPlanMode"
    }

    fn description(&self) -> &str {
        "Switch to plan mode for complex tasks requiring exploration and design before implementation"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "reason": {
                    "type": "string",
                    "description": "Optional reason for entering plan mode"
                }
            },
            "additionalProperties": false
        })
    }

    async fn invoke(
        &self,
        args: serde_json::Value,
        _ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        let parsed: EnterPlanModeArgs = serde_json::from_value(args).map_err(|e| {
            ToolError::InvalidArguments(format!("Invalid EnterPlanMode args: {}", e))
        })?;

        let question = if let Some(ref reason) = parsed.reason {
            format!(
                "Enter plan mode? The assistant wants to switch to read-only exploration to design an approach before any changes. Reason: {}",
                reason
            )
        } else {
            "Enter plan mode? The assistant will switch to read-only exploration to design an approach before any changes.".to_string()
        };

        let payload = json!({
            "status": "awaiting_user_input",
            "question": question,
            "options": ["Enter plan mode", "Stay in normal mode"],
            "allow_custom": false,
        });

        Ok(ToolOutcome::Completed(ToolResult {
            success: true,
            result: payload.to_string(),
            display_preference: Some("conclusion_with_options".to_string()),
            images: Vec::new(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn enter_plan_mode_has_correct_name() {
        let tool = EnterPlanModeTool::new();
        assert_eq!(tool.name(), "EnterPlanMode");
    }

    #[test]
    fn enter_plan_mode_has_description() {
        let tool = EnterPlanModeTool::new();
        assert!(!tool.description().is_empty());
        assert!(tool.description().contains("plan"));
    }

    #[tokio::test]
    async fn enter_plan_mode_returns_conclusion_with_options() {
        let tool = EnterPlanModeTool::new();
        let out = tool.invoke(json!({}), ToolCtx::none("t")).await.unwrap();
        let ToolOutcome::Completed(result) = out else {
            panic!("expected Completed")
        };

        assert!(result.success);
        assert_eq!(
            result.display_preference,
            Some("conclusion_with_options".to_string())
        );

        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(payload["status"], "awaiting_user_input");
        assert!(payload["question"]
            .as_str()
            .unwrap()
            .contains("Enter plan mode"));
        let options = payload["options"].as_array().unwrap();
        assert_eq!(options.len(), 2);
        assert!(options.contains(&json!("Enter plan mode")));
        assert!(options.contains(&json!("Stay in normal mode")));
        assert_eq!(payload["allow_custom"], false);
    }

    #[tokio::test]
    async fn enter_plan_mode_includes_reason() {
        let tool = EnterPlanModeTool::new();
        let out = tool
            .invoke(
                json!({
                    "reason": "This is a complex refactor"
                }),
                ToolCtx::none("t"),
            )
            .await
            .unwrap();
        let ToolOutcome::Completed(result) = out else {
            panic!("expected Completed")
        };

        assert!(result.success);
        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        let question = payload["question"].as_str().unwrap();
        assert!(question.contains("This is a complex refactor"));
    }

    #[tokio::test]
    async fn enter_plan_mode_accepts_empty_args() {
        let tool = EnterPlanModeTool::new();
        let out = tool.invoke(json!({}), ToolCtx::none("t")).await.unwrap();
        let ToolOutcome::Completed(result) = out else {
            panic!("expected Completed")
        };
        assert!(result.success);
    }
}
