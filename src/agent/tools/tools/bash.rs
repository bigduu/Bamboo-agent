use crate::agent::core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};
use crate::core::process_utils::{
    hide_window_for_tokio_command, render_command_line, trace_windows_command,
    windows_command_trace_enabled,
};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use std::path::Path;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::time::{Duration, Instant};

use super::{bash_runtime, workspace_state};

#[cfg(target_os = "windows")]
const SHELL: (&str, &str) = ("cmd", "/c");
#[cfg(not(target_os = "windows"))]
const SHELL: (&str, &str) = ("sh", "-c");

const DEFAULT_TIMEOUT_MS: u64 = 120_000;
const MAX_TIMEOUT_MS: u64 = 600_000;
const MAX_CAPTURE_BYTES: usize = 512 * 1024;

#[derive(Debug, Deserialize)]
struct BashArgs {
    command: String,
    #[serde(default)]
    timeout: Option<u64>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    run_in_background: Option<bool>,
}

pub struct BashTool;

impl BashTool {
    pub fn new() -> Self {
        Self
    }

    fn effective_timeout_ms(requested: Option<u64>) -> u64 {
        let value = requested.unwrap_or(DEFAULT_TIMEOUT_MS);
        value.clamp(1, MAX_TIMEOUT_MS)
    }

    fn append_capped(buffer: &mut String, line: &str, truncated: &mut bool) {
        if *truncated {
            return;
        }
        let needed = line.len() + 1;
        if buffer.len() + needed <= MAX_CAPTURE_BYTES {
            buffer.push_str(line);
            buffer.push('\n');
            return;
        }

        let remaining = MAX_CAPTURE_BYTES.saturating_sub(buffer.len());
        if remaining > 0 {
            let take = remaining.saturating_sub(1);
            if take > 0 {
                let mut end = take.min(line.len());
                while end > 0 && !line.is_char_boundary(end) {
                    end -= 1;
                }
                buffer.push_str(&line[..end]);
            }
            if buffer.len() < MAX_CAPTURE_BYTES {
                buffer.push('\n');
            }
        }
        *truncated = true;
    }

    async fn run_foreground(
        &self,
        command: &str,
        timeout_ms: u64,
        cwd: Option<&Path>,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let (shell, arg) = SHELL;
        trace_windows_command("agent.bash.foreground", shell, [arg, command]);
        if windows_command_trace_enabled() {
            let rendered = render_command_line(shell, [arg, command]);
            ctx.emit_tool_token(format!("[windows-cmd-trace] {rendered}\n"))
                .await;
        }

        let mut cmd = Command::new(shell);
        hide_window_for_tokio_command(&mut cmd);
        if let Some(cwd) = cwd {
            cmd.current_dir(cwd);
        }
        cmd.arg(arg)
            .arg(command)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let mut child = cmd
            .spawn()
            .map_err(|e| ToolError::Execution(format!("Failed to execute command: {}", e)))?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| ToolError::Execution("Failed to capture stdout".to_string()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| ToolError::Execution("Failed to capture stderr".to_string()))?;

        let mut stdout_lines = BufReader::new(stdout).lines();
        let mut stderr_lines = BufReader::new(stderr).lines();

        let mut stdout_buf = String::new();
        let mut stderr_buf = String::new();
        let mut stdout_truncated = false;
        let mut stderr_truncated = false;
        let mut stdout_done = false;
        let mut stderr_done = false;
        let deadline = Instant::now() + Duration::from_millis(timeout_ms);
        let mut timed_out = false;

        while !(stdout_done && stderr_done) {
            if Instant::now() >= deadline {
                timed_out = true;
                break;
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            tokio::select! {
                line = stdout_lines.next_line(), if !stdout_done => {
                    match line {
                        Ok(Some(line)) => {
                            Self::append_capped(&mut stdout_buf, &line, &mut stdout_truncated);
                            ctx.emit_tool_token(format!("{}\n", line)).await;
                        }
                        Ok(None) => stdout_done = true,
                        Err(e) => {
                            return Err(ToolError::Execution(format!("Failed reading stdout: {}", e)));
                        }
                    }
                }
                line = stderr_lines.next_line(), if !stderr_done => {
                    match line {
                        Ok(Some(line)) => {
                            Self::append_capped(&mut stderr_buf, &line, &mut stderr_truncated);
                            ctx.emit_tool_token(format!("{}\n", line)).await;
                        }
                        Ok(None) => stderr_done = true,
                        Err(e) => {
                            return Err(ToolError::Execution(format!("Failed reading stderr: {}", e)));
                        }
                    }
                }
                _ = tokio::time::sleep(remaining) => {
                    timed_out = true;
                    break;
                }
            }
        }

        let status = if timed_out {
            let _ = child.kill().await;
            None
        } else {
            Some(
                child
                    .wait()
                    .await
                    .map_err(|e| ToolError::Execution(format!("Failed waiting command: {}", e)))?,
            )
        };

        let exit_code = status.and_then(|s| s.code());
        let success = !timed_out && exit_code.unwrap_or(-1) == 0;

        Ok(ToolResult {
            success,
            result: json!({
                "command": command,
                "stdout": stdout_buf,
                "stderr": stderr_buf,
                "exit_code": exit_code,
                "timed_out": timed_out,
                "stdout_truncated": stdout_truncated,
                "stderr_truncated": stderr_truncated,
            })
            .to_string(),
            display_preference: Some("Collapsible".to_string()),
        })
    }
}

impl Default for BashTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for BashTool {
    fn name(&self) -> &str {
        "Bash"
    }

    fn description(&self) -> &str {
        "Execute a bash command, optionally in background, with streaming output support"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "command": {
                    "type": "string",
                    "description": "The command to execute"
                },
                "timeout": {
                    "type": "number",
                    "description": "Optional timeout in milliseconds (max 600000)"
                },
                "description": {
                    "type": "string",
                    "description": "Clear, concise description of what this command does in 5-10 words"
                },
                "run_in_background": {
                    "type": "boolean",
                    "description": "Set to true to run this command in the background"
                }
            },
            "required": ["command"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        self.execute_with_context(args, ToolExecutionContext::none("Bash"))
            .await
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let parsed: BashArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid Bash args: {}", e)))?;

        let command = parsed.command.trim();
        if command.is_empty() {
            return Err(ToolError::InvalidArguments(
                "'command' cannot be empty".to_string(),
            ));
        }

        let _ = parsed.description;
        let timeout_ms = Self::effective_timeout_ms(parsed.timeout);
        let workspace = workspace_state::workspace_or_process_cwd(ctx.session_id);
        if parsed.run_in_background.unwrap_or(false) {
            let shell = bash_runtime::spawn_background(command, Some(&workspace))
                .await
                .map_err(ToolError::Execution)?;

            if let Some(requested_timeout) = parsed.timeout {
                let kill_after_ms = Self::effective_timeout_ms(Some(requested_timeout));
                let shell_clone = shell.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(kill_after_ms)).await;
                    if shell_clone.status() == "running" {
                        let _ = shell_clone.kill().await;
                    }
                });
            }

            return Ok(ToolResult {
                success: true,
                result: json!({
                    "bash_id": shell.id,
                    "command": shell.command,
                    "status": "running",
                })
                .to_string(),
                display_preference: Some("Collapsible".to_string()),
            });
        }

        self.run_foreground(command, timeout_ms, Some(&workspace), ctx)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::core::AgentEvent;
    use serde_json::Value;
    use tokio::sync::mpsc;
    use tokio::time::{sleep, Duration, Instant};

    #[cfg(target_os = "windows")]
    fn mixed_output_command() -> &'static str {
        "echo out && echo err 1>&2"
    }

    #[cfg(not(target_os = "windows"))]
    fn mixed_output_command() -> &'static str {
        "printf 'out\\n'; printf 'err\\n' 1>&2"
    }

    #[tokio::test]
    async fn bash_foreground_returns_stdout_stderr_and_streams_tokens() {
        let tool = BashTool::new();
        let (tx, mut rx) = mpsc::channel(32);

        let result = tool
            .execute_with_context(
                json!({
                    "command": mixed_output_command()
                }),
                ToolExecutionContext {
                    session_id: Some("session_1"),
                    tool_call_id: "call_1",
                    event_tx: Some(&tx),
                },
            )
            .await
            .unwrap();

        assert!(result.success);

        let payload: Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(payload["timed_out"], false);
        assert_eq!(payload["exit_code"], 0);
        assert!(payload["stdout"]
            .as_str()
            .unwrap_or_default()
            .contains("out"));
        assert!(payload["stderr"]
            .as_str()
            .unwrap_or_default()
            .contains("err"));

        let mut streamed = Vec::new();
        while let Ok(event) = rx.try_recv() {
            if let AgentEvent::ToolToken { content, .. } = event {
                streamed.push(content);
            }
        }

        assert!(streamed.iter().any(|line| line.contains("out")));
        assert!(streamed.iter().any(|line| line.contains("err")));
    }

    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    async fn bash_foreground_sets_stdout_truncated_when_output_exceeds_cap() {
        let tool = BashTool::new();
        let result = tool
            .execute(json!({
                "command": "i=0; while [ $i -lt 70000 ]; do printf 'aaaaaaaaaa'; i=$((i+1)); done; printf '\\n'"
            }))
            .await
            .unwrap();

        let payload: Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(payload["timed_out"], false);
        assert_eq!(payload["stdout_truncated"], true);
    }

    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    async fn bash_background_honors_explicit_timeout() {
        let tool = BashTool::new();
        let result = tool
            .execute(json!({
                "command": "sleep 2",
                "run_in_background": true,
                "timeout": 50
            }))
            .await
            .unwrap();
        let payload: Value = serde_json::from_str(&result.result).unwrap();
        let shell_id = payload["bash_id"].as_str().unwrap().to_string();

        let started = Instant::now();
        loop {
            let shell = super::bash_runtime::get_shell(&shell_id).unwrap();
            if shell.status() == "completed" {
                break;
            }
            if started.elapsed() > Duration::from_secs(2) {
                panic!("background shell did not stop after timeout");
            }
            sleep(Duration::from_millis(25)).await;
        }
    }
}
