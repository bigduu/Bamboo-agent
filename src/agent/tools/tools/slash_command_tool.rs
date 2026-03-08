use crate::agent::core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
struct SlashCommandArgs {
    command: String,
}

pub struct SlashCommandTool;

impl SlashCommandTool {
    pub fn new() -> Self {
        Self
    }
}

impl Default for SlashCommandTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for SlashCommandTool {
    fn name(&self) -> &str {
        "SlashCommand"
    }

    fn description(&self) -> &str {
        "Execute a slash command within the main conversation"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "command": {
                    "type": "string",
                    "description": "Slash command text, including arguments"
                }
            },
            "required": ["command"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let parsed: SlashCommandArgs = serde_json::from_value(args).map_err(|e| {
            ToolError::InvalidArguments(format!("Invalid SlashCommand args: {}", e))
        })?;

        let raw = parsed.command.trim();
        if raw.is_empty() {
            return Err(ToolError::InvalidArguments(
                "command cannot be empty".to_string(),
            ));
        }

        let mut parts = raw.split_whitespace();
        let head = parts.next().unwrap_or_default();
        let tail = parts.collect::<Vec<_>>().join(" ");

        let project_path = std::env::current_dir()
            .ok()
            .map(|path| path.to_string_lossy().to_string());

        let commands = crate::commands::slash_commands::slash_commands_list(project_path)
            .await
            .map_err(ToolError::Execution)?;

        if let Some(command) = commands
            .into_iter()
            .find(|value| value.full_command == head)
        {
            let resolved = if command.accepts_arguments {
                command.content.replace("$ARGUMENTS", tail.trim())
            } else {
                command.content.clone()
            };

            return Ok(ToolResult {
                success: true,
                result: json!({
                    "command": raw,
                    "resolved_command": command.full_command,
                    "content": resolved,
                })
                .to_string(),
                display_preference: Some("Collapsible".to_string()),
            });
        }

        let fallback = crate::commands::slash_commands::slash_commands_list(None)
            .await
            .unwrap_or_default();
        let available = fallback
            .iter()
            .take(5)
            .map(|value| value.full_command.clone())
            .collect::<Vec<_>>();

        Err(ToolError::Execution(format!(
            "Slash command '{}' not found. Available commands: {}",
            head,
            available.join(", ")
        )))
    }
}
