use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use tokio::io::AsyncWriteExt;
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::agent::core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};
use crate::agent::core::AgentEvent;

/// Built-in tool that runs Claude Code CLI (`claude`) and streams its output into the agent event
/// stream (SSE) while also returning the final assistant text as the tool result.
///
/// This is intended to integrate Claude Code as an optional "built-in tool": register this tool
/// only when `claude` is discoverable on the host.
pub struct ClaudeCodeTool {
    claude_path: String,
}

impl ClaudeCodeTool {
    pub fn new(claude_path: String) -> Self {
        Self { claude_path }
    }
}

#[derive(Debug, Deserialize)]
struct ClaudeCodeArgs {
    project_path: String,
    prompt: String,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    anthropic_base_url: Option<String>,
    #[serde(default)]
    json_schema: Option<String>,
    #[serde(default)]
    dangerously_skip_permissions: Option<bool>,
    #[serde(default)]
    include_partial_messages: Option<bool>,
}

#[async_trait]
impl Tool for ClaudeCodeTool {
    fn name(&self) -> &str {
        "claude_code"
    }

    fn description(&self) -> &str {
        "Run Claude Code CLI (stream-json) and stream its output as agent events"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "project_path": {
                    "type": "string",
                    "description": "Project directory to run Claude in"
                },
                "prompt": {
                    "type": "string",
                    "description": "Prompt to send to Claude Code"
                },
                "session_id": {
                    "type": "string",
                    "description": "Optional Claude session UUID. If omitted or invalid, Bamboo generates one."
                },
                "anthropic_base_url": {
                    "type": "string",
                    "description": "Anthropic base URL (e.g. http://127.0.0.1:9562/anthropic). If omitted, Bamboo uses ANTHROPIC_BASE_URL env var."
                },
                "json_schema": {
                    "type": "string",
                    "description": "Optional JSON Schema for structured output (passed to --json-schema)."
                },
                "dangerously_skip_permissions": {
                    "type": "boolean",
                    "description": "If true (default), pass --dangerously-skip-permissions to avoid interactive confirmation prompts."
                },
                "include_partial_messages": {
                    "type": "boolean",
                    "description": "If true (default), pass --include-partial-messages for better streaming UX."
                }
            },
            "required": ["project_path", "prompt"]
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        self.execute_with_context(args, ToolExecutionContext::none("claude_code"))
            .await
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let parsed: ClaudeCodeArgs = serde_json::from_value(args).map_err(|e| {
            ToolError::InvalidArguments(format!("Invalid arguments for claude_code: {e}"))
        })?;

        let project_path = std::path::PathBuf::from(parsed.project_path.trim());
        if !project_path.is_dir() {
            return Err(ToolError::InvalidArguments(format!(
                "project_path is not a directory: {}",
                project_path.display()
            )));
        }

        let base_url = parsed
            .anthropic_base_url
            .or_else(|| std::env::var("ANTHROPIC_BASE_URL").ok())
            .ok_or_else(|| {
                ToolError::InvalidArguments(
                    "anthropic_base_url is required (or set ANTHROPIC_BASE_URL)".to_string(),
                )
            })?;

        let include_partial_messages = parsed.include_partial_messages.unwrap_or(true);
        let dangerously_skip_permissions = parsed.dangerously_skip_permissions.unwrap_or(true);

        let claude_session_id = match parsed.session_id {
            Some(sid) if uuid::Uuid::parse_str(&sid).is_ok() => sid,
            _ => uuid::Uuid::new_v4().to_string(),
        };

        let mut cmd = crate::claude::create_tokio_command_with_env(&self.claude_path);
        cmd.current_dir(&project_path);
        cmd.env("ANTHROPIC_BASE_URL", &base_url);

        // Prefer stdin for the prompt (instead of passing a potentially multi-line string
        // as a command-line argument). This is especially important on Windows when the
        // `claude` entrypoint is a `.cmd`/`.bat` shim with stricter argument parsing.
        cmd.arg("-p");
        cmd.arg("--output-format").arg("stream-json");
        // Claude Code requires `--verbose` when using `--print/-p` with `--output-format=stream-json`.
        cmd.arg("--verbose");
        cmd.arg("--session-id").arg(&claude_session_id);

        if include_partial_messages {
            cmd.arg("--include-partial-messages");
        }
        if dangerously_skip_permissions {
            cmd.arg("--dangerously-skip-permissions");
        }
        if let Some(schema) = parsed.json_schema.as_ref() {
            cmd.arg("--json-schema").arg(schema);
        }

        cmd.stdin(std::process::Stdio::piped());
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());

        let mut child = cmd
            .spawn()
            .map_err(|e| ToolError::Execution(format!("Failed to spawn Claude Code CLI: {e}")))?;

        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(parsed.prompt.as_bytes())
                .await
                .map_err(|e| ToolError::Execution(format!("Failed writing Claude stdin: {e}")))?;
            stdin
                .write_all(b"\n")
                .await
                .map_err(|e| ToolError::Execution(format!("Failed writing Claude stdin: {e}")))?;
            let _ = stdin.shutdown().await;
        }

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| ToolError::Execution("Failed to capture Claude stdout".to_string()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| ToolError::Execution("Failed to capture Claude stderr".to_string()))?;

        let mut stdout_lines = BufReader::new(stdout).lines();
        let mut stderr_lines = BufReader::new(stderr).lines();

        let mut parser = crate::claude::ClaudeStreamJsonParser::default();
        let mut assistant_text = String::new();
        let mut stderr_buf = String::new();

        let mut stdout_done = false;
        let mut stderr_done = false;
        while !(stdout_done && stderr_done) {
            tokio::select! {
                line = stdout_lines.next_line(), if !stdout_done => {
                    match line {
                        Ok(Some(line)) => {
                            for event in parser.parse_line(&line) {
                                // NOTE: `Complete`/`Error` are reserved for the *agent run*.
                                // Never emit them from inside a tool, or we'd close the SSE stream.
                                match event {
                                    AgentEvent::Complete { .. } | AgentEvent::Error { .. } => {}
                                    AgentEvent::Token { content } => {
                                        assistant_text.push_str(&content);
                                        // `ToolExecutionContext::emit` will convert this into a tool-scoped stream event.
                                        ctx.emit(AgentEvent::Token { content }).await;
                                    }
                                    other => {
                                        ctx.emit(other).await;
                                    }
                                }
                            }
                        }
                        Ok(None) => stdout_done = true,
                        Err(e) => {
                            return Err(ToolError::Execution(format!("Failed reading Claude stdout: {e}")));
                        }
                    }
                }
                line = stderr_lines.next_line(), if !stderr_done => {
                    match line {
                        Ok(Some(line)) => {
                            stderr_buf.push_str(&line);
                            stderr_buf.push('\n');
                            // Best-effort: forward stderr as a token so clients can see errors early.
                            ctx.emit(AgentEvent::Token { content: format!("[claude stderr] {line}\n") }).await;
                        }
                        Ok(None) => stderr_done = true,
                        Err(e) => {
                            return Err(ToolError::Execution(format!("Failed reading Claude stderr: {e}")));
                        }
                    }
                }
            }
        }

        let status = child.wait().await.map_err(|e| {
            ToolError::Execution(format!("Failed to wait for Claude Code CLI: {e}"))
        })?;

        if !status.success() {
            let msg = if stderr_buf.trim().is_empty() {
                format!("Claude Code CLI exited with status: {status}")
            } else {
                format!("Claude Code CLI failed ({status}): {stderr_buf}")
            };
            return Err(ToolError::Execution(msg));
        }

        Ok(ToolResult {
            success: true,
            result: assistant_text,
            display_preference: None,
        })
    }
}
