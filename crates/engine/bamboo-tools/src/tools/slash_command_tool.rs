use async_trait::async_trait;
use bamboo_agent_core::{Tool, ToolCtx, ToolError, ToolOutcome, ToolResult};
use serde::Deserialize;
use serde_json::json;

use super::workspace_state;

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

    async fn invoke(
        &self,
        args: serde_json::Value,
        ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
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

        let project_path = Some(bamboo_config::paths::path_to_display_string(
            &workspace_state::workspace_or_process_cwd(ctx.session_id()),
        ));

        let commands = crate::slash_commands::slash_commands_list(project_path)
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

            return Ok(ToolOutcome::Completed(ToolResult {
                success: true,
                result: json!({
                    "command": raw,
                    "resolved_command": command.full_command,
                    "content": resolved,
                })
                .to_string(),
                display_preference: Some("Collapsible".to_string()),
                images: Vec::new(),
            }));
        }

        let fallback = crate::slash_commands::slash_commands_list(None)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn slash_command_uses_session_workspace_for_project_commands() {
        let dir = tempfile::tempdir().unwrap();
        let commands_dir = dir.path().join(".claude/commands");
        tokio::fs::create_dir_all(&commands_dir).await.unwrap();
        tokio::fs::write(commands_dir.join("hello.md"), "Hi $ARGUMENTS")
            .await
            .unwrap();

        let session = format!("session_{}", uuid::Uuid::new_v4());
        super::workspace_state::set_workspace(&session, dir.path().to_path_buf());

        let tool = SlashCommandTool::new();
        let out = tool
            .invoke(
                json!({ "command": "/hello world" }),
                ToolCtx {
                    session_id: Some(std::sync::Arc::from(session.as_str())),
                    tool_call_id: std::sync::Arc::from("call_1"),
                    event_tx: None,
                    available_tool_schemas: std::sync::Arc::from(Vec::new()),
                    bypass_permissions: false,
                    auto_approve_permissions: false,
                    plan_read_only: false,
                    can_async_resume: false,
                    async_completion_sink: None,
                    bash_completion_sink: None,
                },
            )
            .await
            .unwrap();
        let ToolOutcome::Completed(result) = out else {
            panic!("expected Completed")
        };

        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(payload["resolved_command"], "/hello");
        assert_eq!(payload["content"], "Hi world");
    }
}
