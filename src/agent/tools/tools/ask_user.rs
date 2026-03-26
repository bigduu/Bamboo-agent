use crate::agent::core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use serde_json::json;

const DEFAULT_OPTIONS: [&str; 2] = ["OK", "Need changes"];

fn default_options() -> Vec<String> {
    DEFAULT_OPTIONS.iter().map(|s| (*s).to_string()).collect()
}

/// Tool for asking user a question with multiple choice options
pub struct AskUserTool;

impl AskUserTool {
    /// Create a new AskUserTool instance.
    ///
    /// This tool prompts the user with a question and provides multiple choice options.
    /// It supports custom answers when `allow_custom` is true.
    pub fn new() -> Self {
        Self
    }
}

impl Default for AskUserTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for AskUserTool {
    fn name(&self) -> &str {
        "ask_user"
    }

    fn description(&self) -> &str {
        "Ask the user a question with options and wait for the user to select or enter a custom answer. Use this tool when user decision or additional information is needed."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "question": {
                    "type": "string",
                    "description": "The question to display to the user"
                },
                "options": {
                    "type": "array",
                    "description": "Candidate answer options (optional). If omitted or invalid, defaults to [\"OK\", \"Need changes\"].",
                    "items": {
                        "type": "string"
                    }
                },
                "allow_custom": {
                    "type": "boolean",
                    "description": "Whether to allow user to enter a custom answer (instead of selecting from options), default true",
                    "default": true
                }
            },
            "required": ["question"]
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let question = args["question"].as_str().ok_or_else(|| {
            ToolError::InvalidArguments("Missing 'question' parameter".to_string())
        })?;

        let mut options: Vec<String> = args["options"]
            .as_array()
            .map(|options_array| {
                options_array
                    .iter()
                    .filter_map(|opt| opt.as_str())
                    .map(str::trim)
                    .filter(|opt| !opt.is_empty())
                    .map(ToString::to_string)
                    .collect::<Vec<String>>()
            })
            .unwrap_or_default();

        if options.len() < 2 {
            options = default_options();
        } else if options.len() > 6 {
            options.truncate(6);
        }

        let allow_custom = args["allow_custom"].as_bool().unwrap_or(true);

        // Build the result payload that will be handled by the agent loop
        let result_payload = json!({
            "status": "awaiting_user_input",
            "question": question,
            "options": options,
            "allow_custom": allow_custom
        });

        Ok(ToolResult {
            success: true,
            result: result_payload.to_string(),
            display_preference: Some("ask_user".to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ask_user_tool_name() {
        let tool = AskUserTool::new();
        assert_eq!(tool.name(), "ask_user");
    }

    #[tokio::test]
    async fn test_execute_valid_input() {
        let tool = AskUserTool::new();

        let result = tool
            .execute(json!({
                "question": "Please select deployment environment",
                "options": ["Development", "Testing", "Production"]
            }))
            .await
            .expect("tool should execute successfully");

        assert!(result.success);
        assert_eq!(result.display_preference, Some("ask_user".to_string()));

        let parsed: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(parsed["status"], "awaiting_user_input");
        assert_eq!(parsed["question"], "Please select deployment environment");
        assert!(parsed["allow_custom"].as_bool().unwrap());
    }

    #[tokio::test]
    async fn test_execute_accepts_two_options() {
        let tool = AskUserTool::new();

        let result = tool
            .execute(json!({
                "question": "Please confirm?",
                "options": ["Yes", "No"]
            }))
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_with_too_few_options_uses_defaults() {
        let tool = AskUserTool::new();

        let result = tool
            .execute(json!({
                "question": "Please select?",
                "options": ["Only one option"]
            }))
            .await
            .expect("tool should execute with fallback defaults");

        let parsed: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(parsed["options"], json!(["OK", "Need changes"]));
    }

    #[tokio::test]
    async fn test_execute_without_options_uses_defaults() {
        let tool = AskUserTool::new();

        let result = tool
            .execute(json!({
                "question": "Any other requests before I finish?"
            }))
            .await
            .expect("tool should execute without options");

        let parsed: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(parsed["options"], json!(["OK", "Need changes"]));
    }

    #[tokio::test]
    async fn test_execute_truncates_options_to_six_items() {
        let tool = AskUserTool::new();

        let result = tool
            .execute(json!({
                "question": "Please pick one",
                "options": ["1", "2", "3", "4", "5", "6", "7"]
            }))
            .await
            .expect("tool should execute and truncate options");

        let parsed: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(parsed["options"], json!(["1", "2", "3", "4", "5", "6"]));
    }

    #[tokio::test]
    async fn test_execute_with_allow_custom_false() {
        let tool = AskUserTool::new();

        let result = tool
            .execute(json!({
                "question": "Please confirm",
                "options": ["Yes", "No", "Cancel"],
                "allow_custom": false
            }))
            .await
            .expect("tool should execute");

        let parsed: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert!(!parsed["allow_custom"].as_bool().unwrap());
    }
}
