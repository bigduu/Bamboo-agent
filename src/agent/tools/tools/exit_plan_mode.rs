use crate::agent::core::tools::{Tool, ToolError, ToolResult};
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
            display_preference: Some("ask_user".to_string()),
        })
    }
}
