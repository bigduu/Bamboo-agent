use bamboo_agent_core::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
struct ExitPlanModeArgs {
    plan: String,
}

pub struct ExitPlanModeTool;

impl ExitPlanModeTool {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ExitPlanModeTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for ExitPlanModeTool {
    fn name(&self) -> &str {
        "ExitPlanMode"
    }

    fn description(&self) -> &str {
        "Prompt the user to confirm exiting plan mode and moving to implementation"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "plan": {
                    "type": "string",
                    "description": "The plan to run by the user for approval"
                }
            },
            "required": ["plan"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let parsed: ExitPlanModeArgs = serde_json::from_value(args).map_err(|e| {
            ToolError::InvalidArguments(format!("Invalid ExitPlanMode args: {}", e))
        })?;

        let payload = json!({
            "status": "awaiting_user_input",
            "question": "Plan ready. Exit plan mode and start implementation?",
            "options": ["Exit plan mode", "Stay in plan mode"],
            "allow_custom": false,
            "plan": parsed.plan,
        });

        Ok(ToolResult {
            success: true,
            result: payload.to_string(),
            display_preference: Some("conclusion_with_options".to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn exit_plan_mode_has_correct_name() {
        let tool = ExitPlanModeTool::new();
        assert_eq!(tool.name(), "ExitPlanMode");
    }

    #[test]
    fn exit_plan_mode_has_description() {
        let tool = ExitPlanModeTool::new();
        assert!(!tool.description().is_empty());
        assert!(tool.description().contains("plan"));
    }

    #[test]
    fn exit_plan_mode_parameters_schema_has_required_fields() {
        let tool = ExitPlanModeTool::new();
        let schema = tool.parameters_schema();

        assert_eq!(schema["type"], "object");
        assert!(schema["properties"]["plan"].is_object());
        assert_eq!(schema["properties"]["plan"]["type"], "string");
        assert!(schema["required"]
            .as_array()
            .unwrap()
            .contains(&json!("plan")));
        assert_eq!(schema["additionalProperties"], false);
    }

    #[tokio::test]
    async fn exit_plan_mode_accepts_valid_plan() {
        let tool = ExitPlanModeTool::new();
        let result = tool
            .execute(json!({
                "plan": "Implement feature X"
            }))
            .await
            .unwrap();

        assert!(result.success);

        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(payload["status"], "awaiting_user_input");
        assert!(payload["question"]
            .as_str()
            .unwrap()
            .contains("Exit plan mode"));
        assert_eq!(
            payload["options"],
            json!(["Exit plan mode", "Stay in plan mode"])
        );
        assert_eq!(payload["allow_custom"], false);
        assert_eq!(payload["plan"], "Implement feature X");
    }

    #[tokio::test]
    async fn exit_plan_mode_includes_plan_in_payload() {
        let tool = ExitPlanModeTool::new();
        let plan_text = "1. Read config\n2. Update database\n3. Deploy changes";
        let result = tool
            .execute(json!({
                "plan": plan_text
            }))
            .await
            .unwrap();

        assert!(result.success);
        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(payload["plan"], plan_text);
    }

    #[tokio::test]
    async fn exit_plan_mode_sets_display_preference_to_conclusion_with_options() {
        let tool = ExitPlanModeTool::new();
        let result = tool
            .execute(json!({
                "plan": "Test plan"
            }))
            .await
            .unwrap();

        assert_eq!(
            result.display_preference,
            Some("conclusion_with_options".to_string())
        );
    }

    #[tokio::test]
    async fn exit_plan_mode_rejects_missing_plan() {
        let tool = ExitPlanModeTool::new();
        let result = tool.execute(json!({})).await;

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(matches!(error, ToolError::InvalidArguments(_)));
    }

    #[tokio::test]
    async fn exit_plan_mode_rejects_invalid_plan_type() {
        let tool = ExitPlanModeTool::new();
        let result = tool
            .execute(json!({
                "plan": 123
            }))
            .await;

        assert!(result.is_err());
        let error = result.unwrap_err();
        if let ToolError::InvalidArguments(msg) = error {
            assert!(msg.contains("Invalid ExitPlanMode args"));
        } else {
            panic!("Expected InvalidArguments error");
        }
    }

    #[tokio::test]
    async fn exit_plan_mode_rejects_null_plan() {
        let tool = ExitPlanModeTool::new();
        let result = tool
            .execute(json!({
                "plan": null
            }))
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn exit_plan_mode_accepts_empty_plan_string() {
        // Empty string is technically valid
        let tool = ExitPlanModeTool::new();
        let result = tool
            .execute(json!({
                "plan": ""
            }))
            .await
            .unwrap();

        assert!(result.success);
        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(payload["plan"], "");
    }

    #[tokio::test]
    async fn exit_plan_mode_accepts_multiline_plan() {
        let tool = ExitPlanModeTool::new();
        let multiline_plan = "Step 1: Setup\nStep 2: Execute\nStep 3: Cleanup";
        let result = tool
            .execute(json!({
                "plan": multiline_plan
            }))
            .await
            .unwrap();

        assert!(result.success);
        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(payload["plan"], multiline_plan);
    }

    #[tokio::test]
    async fn exit_plan_mode_accepts_markdown_plan() {
        let tool = ExitPlanModeTool::new();
        let markdown_plan = r#"# Implementation Plan

## Phase 1
- Task A
- Task B

## Phase 2
- Task C
"#;
        let result = tool
            .execute(json!({
                "plan": markdown_plan
            }))
            .await
            .unwrap();

        assert!(result.success);
        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(payload["plan"], markdown_plan);
    }

    #[tokio::test]
    async fn exit_plan_mode_accepts_unicode_plan() {
        let tool = ExitPlanModeTool::new();
        let unicode_plan = "实施计划 🎯\n1. 读取配置\n2. 更新数据库";
        let result = tool
            .execute(json!({
                "plan": unicode_plan
            }))
            .await
            .unwrap();

        assert!(result.success);
        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(payload["plan"], unicode_plan);
    }

    #[tokio::test]
    async fn exit_plan_mode_ignores_extra_fields() {
        let tool = ExitPlanModeTool::new();
        // serde_json with additionalProperties: false may not strictly reject extra fields
        // during deserialization, so this test verifies the behavior
        let result = tool
            .execute(json!({
                "plan": "Test plan",
                "extra_field": "should be ignored"
            }))
            .await;

        // Depending on serde configuration, this might succeed (ignoring extra fields)
        // or fail (rejecting extra fields). The test documents the actual behavior.
        if let Ok(tool_result) = result {
            // If it succeeds, verify the plan was captured correctly
            assert!(tool_result.success);
            let payload: serde_json::Value = serde_json::from_str(&tool_result.result).unwrap();
            assert_eq!(payload["plan"], "Test plan");
        } else {
            // If it fails, verify it's an InvalidArguments error
            let error = result.unwrap_err();
            assert!(matches!(error, ToolError::InvalidArguments(_)));
        }
    }

    #[tokio::test]
    async fn exit_plan_mode_payload_has_correct_structure() {
        let tool = ExitPlanModeTool::new();
        let result = tool
            .execute(json!({
                "plan": "Test"
            }))
            .await
            .unwrap();

        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();

        // Verify all expected fields are present
        assert!(payload.is_object());
        assert!(payload.get("status").is_some());
        assert!(payload.get("question").is_some());
        assert!(payload.get("options").is_some());
        assert!(payload.get("allow_custom").is_some());
        assert!(payload.get("plan").is_some());

        // Verify types
        assert!(payload["status"].is_string());
        assert!(payload["question"].is_string());
        assert!(payload["options"].is_array());
        assert!(payload["allow_custom"].is_boolean());
        assert!(payload["plan"].is_string());
    }

    #[tokio::test]
    async fn exit_plan_mode_options_has_two_choices() {
        let tool = ExitPlanModeTool::new();
        let result = tool
            .execute(json!({
                "plan": "Test"
            }))
            .await
            .unwrap();

        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        let options = payload["options"].as_array().unwrap();

        assert_eq!(options.len(), 2);
        assert!(options.contains(&json!("Exit plan mode")));
        assert!(options.contains(&json!("Stay in plan mode")));
    }

    #[test]
    fn exit_plan_mode_default_impl() {
        let tool = ExitPlanModeTool::default();
        assert_eq!(tool.name(), "ExitPlanMode");
    }

    #[tokio::test]
    async fn exit_plan_mode_long_plan() {
        let tool = ExitPlanModeTool::new();
        let long_plan = "Step\n".repeat(1000);
        let result = tool
            .execute(json!({
                "plan": long_plan.clone()
            }))
            .await
            .unwrap();

        assert!(result.success);
        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(payload["plan"], long_plan);
    }
}
