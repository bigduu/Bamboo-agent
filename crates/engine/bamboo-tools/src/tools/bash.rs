use async_trait::async_trait;
use bamboo_agent_core::{Tool, ToolError, ToolExecutionContext, ToolResult};
use bamboo_infrastructure::process::{
    build_command_environment, decode_process_line_lossy, hide_window_for_tokio_command,
    preferred_bash_shell, render_command_line, trace_windows_command,
    windows_command_trace_enabled, PreparedCommandEnvironment,
};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::time::{Duration, Instant};

use super::{bash_runtime, workspace_state};

const DEFAULT_TIMEOUT_MS: u64 = 120_000;
const MAX_TIMEOUT_MS: u64 = 600_000;
const MAX_CAPTURE_BYTES: usize = 512 * 1024;

/// Auto-sync promotion threshold (issue #84, phase 2d). Commands started via the
/// auto path (`run_in_background` omitted) that are still running after this many
/// milliseconds are promoted to background instead of continuing to block.
/// Deliberately generous so virtually all interactive commands stay synchronous.
const PROMOTE_TO_BACKGROUND_AFTER_MS: u64 = 10_000;

#[derive(Debug, Deserialize)]
struct BashArgs {
    command: String,
    #[serde(default)]
    timeout: Option<u64>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    run_in_background: Option<bool>,
    #[serde(default)]
    interactive: Option<bool>,
    #[serde(default)]
    workdir: Option<String>,
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

    /// Append a line to a promotion-seed buffer, draining the oldest entries
    /// when over the same budget the background registry enforces
    /// (`bash_runtime::MAX_OUTPUT_LINES`). Keeps a chatty command from ballooning
    /// memory during the ~10s promotion window (issue #84, phase 2d) — the
    /// `stdout_buf`/`stderr_buf` capture is byte-capped, but the seed Vecs feed
    /// the background buffer and must not grow unbounded. Mirrors `push_line`'s
    /// drain-oldest semantics.
    fn push_capped_seed_line(buf: &mut Vec<String>, line: String) {
        buf.push(line);
        let cap = bash_runtime::MAX_OUTPUT_LINES;
        if buf.len() > cap {
            let overflow = buf.len() - cap;
            buf.drain(0..overflow);
        }
    }

    fn python_diagnostics_json(
        diagnostics: &bamboo_infrastructure::process::PythonDiscoveryDiagnostics,
        include_full_tried: bool,
    ) -> Value {
        let mut python = Map::new();
        if let Some(configured) = diagnostics.configured.as_ref() {
            python.insert("configured".to_string(), json!(configured));
        }
        if let Some(resolved) = diagnostics.resolved.as_ref() {
            python.insert("resolved".to_string(), json!(resolved));
        }
        if let Some(invocation) = diagnostics.invocation.as_ref() {
            python.insert("invocation".to_string(), json!(invocation));
        }
        if let Some(source) = diagnostics.source.as_ref() {
            python.insert("source".to_string(), json!(source));
        }
        if !diagnostics.tried_preview.is_empty() {
            python.insert(
                "tried_preview".to_string(),
                json!(diagnostics.tried_preview),
            );
        }
        if diagnostics.tried_total > 0 {
            python.insert("tried_total".to_string(), json!(diagnostics.tried_total));
            python.insert(
                "tried_truncated".to_string(),
                json!(diagnostics.tried_truncated),
            );
        }
        if let Some(hint) = diagnostics.hint.as_ref() {
            python.insert("hint".to_string(), json!(hint));
        }
        if include_full_tried && !diagnostics.tried.is_empty() {
            python.insert("tried".to_string(), json!(diagnostics.tried));
        }
        Value::Object(python)
    }

    fn environment_json(
        diagnostics: &bamboo_infrastructure::process::CommandEnvironmentDiagnostics,
        include_full_python_tried: bool,
    ) -> Value {
        let mut environment = Map::new();
        environment.insert("source".to_string(), json!(diagnostics.source.as_str()));
        if let Some(import_shell) = diagnostics.import_shell.as_ref() {
            environment.insert("import_shell".to_string(), json!(import_shell));
        }
        if let Some(import_error) = diagnostics.import_error.as_ref() {
            environment.insert("import_error".to_string(), json!(import_error));
        }
        if let Some(path) = diagnostics.path.as_ref() {
            environment.insert("path".to_string(), json!(path));
        }
        if let Some(path_entries) = diagnostics.path_entries {
            environment.insert("path_entries".to_string(), json!(path_entries));
        }

        let python = Self::python_diagnostics_json(&diagnostics.python, include_full_python_tried);
        if python
            .as_object()
            .map(|map| !map.is_empty())
            .unwrap_or(false)
        {
            environment.insert("python".to_string(), python);
        }

        Value::Object(environment)
    }

    fn resolve_cwd(session_workspace: &Path, workdir: Option<&str>) -> Result<PathBuf, ToolError> {
        let resolved = match workdir {
            Some(raw) => {
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    return Err(ToolError::InvalidArguments(
                        "'workdir' cannot be empty".to_string(),
                    ));
                }
                let requested = Path::new(trimmed);
                if requested.is_absolute() {
                    requested.to_path_buf()
                } else {
                    session_workspace.join(requested)
                }
            }
            None => session_workspace.to_path_buf(),
        };

        let metadata = std::fs::metadata(&resolved).map_err(|error| {
            ToolError::InvalidArguments(format!(
                "Invalid workdir '{}': {}",
                bamboo_config::paths::path_to_display_string(&resolved),
                error
            ))
        })?;
        if !metadata.is_dir() {
            return Err(ToolError::InvalidArguments(format!(
                "workdir must be a directory: {}",
                bamboo_config::paths::path_to_display_string(&resolved)
            )));
        }

        resolved.canonicalize().map_err(|error| {
            ToolError::Execution(format!(
                "Failed to canonicalize workdir '{}': {}",
                bamboo_config::paths::path_to_display_string(&resolved),
                error
            ))
        })
    }

    async fn prepare_environment() -> PreparedCommandEnvironment {
        let overrides = bamboo_llm::Config::current_env_vars();
        build_command_environment(&overrides).await
    }

    /// Run a command with streaming output (issue #84, phase 2d).
    ///
    /// When `promote_after_ms` is `None`, this is pure synchronous foreground:
    /// blocks until the command completes or the timeout fires — byte-identical
    /// to the pre-2d `run_foreground` (same streaming, capture, exit code, result).
    ///
    /// When `promote_after_ms` is `Some(ms)`, the command starts foreground but
    /// is auto-promoted to background if still running after `ms` milliseconds.
    /// Fast commands that finish before the promotion deadline never reach the
    /// promotion branch, so their behavior is unchanged. Only the promotion
    /// deadline (not the timeout) triggers the hand-off: a timeout with
    /// `ms >= timeout_ms` kills the child exactly as before.
    async fn run_streaming_command(
        &self,
        command: &str,
        timeout_ms: u64,
        promote_after_ms: Option<u64>,
        cwd: &Path,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let shell = preferred_bash_shell();
        trace_windows_command(
            "agent.bash.foreground",
            &shell.program,
            [shell.arg, command],
        );
        if windows_command_trace_enabled() {
            let rendered = render_command_line(&shell.program, [shell.arg, command]);
            ctx.emit_tool_token(format!("[windows-cmd-trace] {rendered}\n"))
                .await;
        }

        let prepared_env = Self::prepare_environment().await;

        let mut cmd = Command::new(&shell.program);
        hide_window_for_tokio_command(&mut cmd);
        cmd.current_dir(cwd);
        prepared_env.apply_to_tokio_command(&mut cmd);
        cmd.arg(shell.arg)
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

        let mut stdout_reader = BufReader::new(stdout);
        let mut stderr_reader = BufReader::new(stderr);
        let mut stdout_line_bytes = Vec::new();
        let mut stderr_line_bytes = Vec::new();

        let mut stdout_buf = String::new();
        let mut stderr_buf = String::new();
        // Individual decoded lines collected for promotion seeding. Only
        // populated when promotion is enabled — a cheap Vec::push per line that
        // is never touched on the pure-foreground path.
        let mut stdout_lines: Vec<String> = Vec::new();
        let mut stderr_lines: Vec<String> = Vec::new();
        let mut stdout_truncated = false;
        let mut stderr_truncated = false;
        let mut stdout_done = false;
        let mut stderr_done = false;

        // Effective deadline: when promotion is enabled, the earlier of the
        // promotion threshold and the timeout; otherwise just the timeout.
        let timeout_deadline = Instant::now() + Duration::from_millis(timeout_ms);
        let effective_deadline = match promote_after_ms {
            Some(promote_ms) => {
                (Instant::now() + Duration::from_millis(promote_ms)).min(timeout_deadline)
            }
            None => timeout_deadline,
        };

        while !(stdout_done && stderr_done) {
            if Instant::now() >= effective_deadline {
                break;
            }

            let remaining = effective_deadline.saturating_duration_since(Instant::now());
            tokio::select! {
                line = stdout_reader.read_until(b'\n', &mut stdout_line_bytes), if !stdout_done => {
                    match line {
                        Ok(0) => stdout_done = true,
                        Ok(_) => {
                            let line = decode_process_line_lossy(&mut stdout_line_bytes);
                            Self::append_capped(&mut stdout_buf, &line, &mut stdout_truncated);
                            if promote_after_ms.is_some() {
                                Self::push_capped_seed_line(&mut stdout_lines, line.clone());
                            }
                            ctx.emit_tool_token(format!("{}\n", line)).await;
                        }
                        Err(e) => {
                            return Err(ToolError::Execution(format!("Failed reading stdout: {}", e)));
                        }
                    }
                }
                line = stderr_reader.read_until(b'\n', &mut stderr_line_bytes), if !stderr_done => {
                    match line {
                        Ok(0) => stderr_done = true,
                        Ok(_) => {
                            let line = decode_process_line_lossy(&mut stderr_line_bytes);
                            Self::append_capped(&mut stderr_buf, &line, &mut stderr_truncated);
                            if promote_after_ms.is_some() {
                                Self::push_capped_seed_line(&mut stderr_lines, line.clone());
                            }
                            ctx.emit_tool_token(format!("{}\n", line)).await;
                        }
                        Err(e) => {
                            return Err(ToolError::Execution(format!("Failed reading stderr: {}", e)));
                        }
                    }
                }
                _ = tokio::time::sleep(remaining) => {
                    break;
                }
            }
        }

        let streams_closed = stdout_done && stderr_done;

        // A promotion fires only when promotion is enabled AND the promotion
        // threshold is strictly less than the timeout. When promotion is
        // disabled or timeout <= promote, the deadline firing is a timeout.
        let promotion_fired =
            !streams_closed && promote_after_ms.is_some() && promote_after_ms.unwrap() < timeout_ms;

        if streams_closed {
            // Command completed normally — identical to the pre-2d foreground path.
            let status = child
                .wait()
                .await
                .map_err(|e| ToolError::Execution(format!("Failed waiting command: {}", e)))?;
            let exit_code = status.code();
            let success = exit_code.unwrap_or(-1) == 0;
            let cwd_display = bamboo_config::paths::path_to_display_string(cwd);
            let environment = Self::environment_json(&prepared_env.diagnostics, !success);

            return Ok(ToolResult {
                success,
                result: json!({
                    "command": command,
                    "cwd": cwd_display,
                    "stdout": stdout_buf,
                    "stderr": stderr_buf,
                    "exit_code": exit_code,
                    "timed_out": false,
                    "stdout_truncated": stdout_truncated,
                    "stderr_truncated": stderr_truncated,
                    "environment": environment,
                })
                .to_string(),
                display_preference: Some("Collapsible".to_string()),
                images: Vec::new(),
            });
        }

        if promotion_fired {
            // Promotion deadline fired while the child is still running —
            // hand off the live child to the background registry. Do NOT kill:
            // pump tasks continue draining and the completion poll emits
            // BashCompleted when the child exits (issue #84, phase 2d).
            //
            // Flush any partial line bytes left by a cancelled `read_until` —
            // they represent real bytes consumed from the pipe that must not be
            // lost across the hand-off.
            if !stdout_line_bytes.is_empty() {
                let partial = decode_process_line_lossy(&mut stdout_line_bytes);
                if !partial.is_empty() {
                    Self::push_capped_seed_line(&mut stdout_lines, partial);
                }
            }
            if !stderr_line_bytes.is_empty() {
                let partial = decode_process_line_lossy(&mut stderr_line_bytes);
                if !partial.is_empty() {
                    Self::push_capped_seed_line(&mut stderr_lines, partial);
                }
            }

            let session = bash_runtime::adopt_running_child(
                child,
                stdout_reader,
                stderr_reader,
                stdout_lines,
                stderr_lines,
                command,
                ctx.session_id.map(str::to_string),
                prepared_env.diagnostics.clone(),
                ctx.cloned_sender(),
                ctx.cloned_bash_completion_sink(),
            )
            .await
            .map_err(ToolError::Execution)?;

            return Ok(ToolResult {
                success: true,
                result: json!({
                    "bash_id": session.id,
                    "command": session.command,
                    "status": "running",
                    "cwd": bamboo_config::paths::path_to_display_string(cwd),
                    "environment": Self::environment_json(&session.environment, false),
                })
                .to_string(),
                display_preference: Some("Collapsible".to_string()),
                images: Vec::new(),
            });
        }

        // Timeout fired (promotion disabled or timeout <= promote threshold).
        let _ = child.kill().await;
        let cwd_display = bamboo_config::paths::path_to_display_string(cwd);
        let environment = Self::environment_json(&prepared_env.diagnostics, true);

        Ok(ToolResult {
            success: false,
            result: json!({
                "command": command,
                "cwd": cwd_display,
                "stdout": stdout_buf,
                "stderr": stderr_buf,
                "exit_code": serde_json::Value::Null,
                "timed_out": true,
                "stdout_truncated": stdout_truncated,
                "stderr_truncated": stderr_truncated,
                "environment": environment,
            })
            .to_string(),
            display_preference: Some("Collapsible".to_string()),
            images: Vec::new(),
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
        "Execute shell commands with streaming output (supports background mode). \
         By default (run_in_background omitted), commands run synchronously but are \
         auto-promoted to background if they run longer than ~10s — fast commands \
         behave exactly as foreground. Set run_in_background to false to force \
         synchronous (block until timeout), or true to force immediate background. \
         Set interactive to true to spawn in the background with a piped stdin so \
         input can be fed over time via BashInput (interactive implies background; \
         use it only to answer an interactive prompt). Backgrounded commands are \
         observed via BashOutput and the loop waits for them at turn end. Default \
         timeout is 120000ms (max 600000ms); captured stdout/stderr are each \
         capped at 512KB."
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
                    "description": "Optional timeout in milliseconds (default 120000, max 600000)"
                },
                "description": {
                    "type": "string",
                    "description": "Optional short context label for the command"
                },
                "run_in_background": {
                    "type": "boolean",
                    "description": "Controls execution mode. Omit (default) for auto: runs synchronously but auto-backgrounds if the command runs longer than ~10s. Set to false to force synchronous (block until timeout). Set to true to force immediate background (observe via BashOutput; the loop waits at turn end)."
                },
                "interactive": {
                    "type": "boolean",
                    "description": "Opt-in: spawn the command in the BACKGROUND with a piped stdin so input can be fed over time via BashInput (interactive:true implies run_in_background — returns a bash_id immediately). When omitted/false the command's stdin is closed (Stdio::null), so a command that reads stdin gets immediate EOF — the default behavior is unchanged. Use this only to answer an interactive prompt in a long-running background shell."
                },
                "workdir": {
                    "type": "string",
                    "description": "Optional working directory. Relative paths are resolved from the session workspace."
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
        let session_workspace = workspace_state::workspace_or_process_cwd(ctx.session_id);
        let cwd = Self::resolve_cwd(&session_workspace, parsed.workdir.as_deref())?;

        if parsed.interactive == Some(true) {
            // Interactive (issue #89): spawn in the background with a piped
            // stdin so the shell can be fed input over time via BashInput.
            // interactive:true implies run_in_background — return a bash_id
            // immediately. The non-interactive paths below keep Stdio::null(),
            // so default EOF-on-read is byte-for-byte unchanged.
            let shell = bash_runtime::spawn_background(
                command,
                Some(&cwd),
                ctx.cloned_sender(),
                ctx.session_id.map(str::to_string),
                true,
                ctx.cloned_bash_completion_sink(),
            )
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
                    "interactive": true,
                    "cwd": bamboo_config::paths::path_to_display_string(&cwd),
                    "environment": Self::environment_json(&shell.environment, false),
                })
                .to_string(),
                display_preference: Some("Collapsible".to_string()),
                images: Vec::new(),
            });
        }

        match parsed.run_in_background {
            Some(true) => {
                // Force background — spawn immediately (issue #84, phase 1).
                let shell = bash_runtime::spawn_background(
                    command,
                    Some(&cwd),
                    ctx.cloned_sender(),
                    ctx.session_id.map(str::to_string),
                    false,
                    ctx.cloned_bash_completion_sink(),
                )
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

                Ok(ToolResult {
                    success: true,
                    result: json!({
                        "bash_id": shell.id,
                        "command": shell.command,
                        "status": "running",
                        "cwd": bamboo_config::paths::path_to_display_string(&cwd),
                        "environment": Self::environment_json(&shell.environment, false),
                    })
                    .to_string(),
                    display_preference: Some("Collapsible".to_string()),
                    images: Vec::new(),
                })
            }
            Some(false) => {
                // Force synchronous — pure foreground, no promotion (issue #84,
                // phase 2d). Blocks until the command completes or times out,
                // exactly like the pre-2d behavior.
                self.run_streaming_command(command, timeout_ms, None, &cwd, ctx)
                    .await
            }
            None => {
                // Auto-sync promotion (issue #84, phase 2d). Runs foreground but
                // promotes to background if still running after the promotion
                // threshold (~10s). Fast commands finish synchronously. Promotion
                // is ONLY enabled when the executing loop can actually suspend
                // for and self-resume a backgrounded shell (`can_async_resume`):
                // on hook-less paths (schedule / external-child loops) it stays
                // purely synchronous so a long command's output is never orphaned.
                let promote_after_ms = if ctx.can_async_resume {
                    Some(PROMOTE_TO_BACKGROUND_AFTER_MS)
                } else {
                    None
                };
                self.run_streaming_command(command, timeout_ms, promote_after_ms, &cwd, ctx)
                    .await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::tools::ToolExecutionSessionFlags;
    use bamboo_agent_core::AgentEvent;
    use bamboo_infrastructure::process::{
        clear_command_environment_cache_for_tests, prime_command_environment_cache_for_tests,
        CommandEnvironmentDiagnostics, CommandEnvironmentSource, PythonDiscoveryDiagnostics,
    };
    use serde_json::Value;
    use std::collections::HashMap;
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

    #[cfg(target_os = "windows")]
    fn invalid_utf8_stderr_command() -> String {
        let shell = bamboo_infrastructure::process::preferred_bash_shell();
        if shell.arg == "-lc" {
            "printf '\\377\\n' 1>&2".to_string()
        } else {
            "powershell -NoProfile -Command \"$bytes = [byte[]](0xFF,0x0A); [Console]::OpenStandardError().Write($bytes,0,$bytes.Length)\"".to_string()
        }
    }

    #[cfg(not(target_os = "windows"))]
    fn invalid_utf8_stderr_command() -> String {
        "printf '\\377\\n' 1>&2".to_string()
    }

    fn test_environment_diagnostics() -> CommandEnvironmentDiagnostics {
        CommandEnvironmentDiagnostics {
            source: CommandEnvironmentSource::InheritedProcess,
            import_shell: None,
            import_error: Some("test-import-disabled".to_string()),
            path: Some("/usr/bin:/bin".to_string()),
            path_entries: Some(2),
            python: PythonDiscoveryDiagnostics {
                configured: Some("python3".to_string()),
                resolved: Some("/usr/bin/python3".to_string()),
                invocation: Some("/usr/bin/python3".to_string()),
                source: Some("path".to_string()),
                tried: vec!["python3".to_string(), "python".to_string()],
                tried_preview: vec!["python3".to_string(), "python".to_string()],
                tried_total: 2,
                tried_truncated: false,
                hint: None,
            },
        }
    }

    fn prime_test_command_environment() {
        clear_command_environment_cache_for_tests();
        prime_command_environment_cache_for_tests(
            HashMap::from([("PATH".to_string(), "/usr/bin:/bin".to_string())]),
            test_environment_diagnostics(),
        );
    }

    #[tokio::test]
    async fn bash_foreground_returns_stdout_stderr_and_streams_tokens() {
        prime_test_command_environment();
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
                    available_tool_schemas: None,
                    bypass_permissions: false,
                    can_async_resume: false,
                    bash_completion_sink: None,
                    pre_parsed_args: None,
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
        assert_eq!(payload["environment"]["source"], "process_env");
        assert_eq!(
            payload["environment"]["import_error"],
            "test-import-disabled"
        );
        assert_eq!(
            payload["environment"]["python"]["resolved"],
            "/usr/bin/python3"
        );
        assert_eq!(
            payload["environment"]["python"]["invocation"],
            "/usr/bin/python3"
        );
        assert_eq!(payload["environment"]["python"]["source"], "path");
        assert_eq!(
            payload["environment"]["python"]["tried_preview"][0],
            "python3"
        );
        assert_eq!(payload["environment"]["python"]["tried_total"], 1);
        assert!(payload["environment"]["python"].get("tried").is_none());

        let mut streamed = Vec::new();
        while let Ok(event) = rx.try_recv() {
            if let AgentEvent::ToolToken { content, .. } = event {
                streamed.push(content);
            }
        }

        assert!(streamed.iter().any(|line| line.contains("out")));
        assert!(streamed.iter().any(|line| line.contains("err")));
    }

    #[tokio::test]
    async fn bash_foreground_tolerates_invalid_utf8_stderr() {
        prime_test_command_environment();
        let tool = BashTool::new();
        let result = tool
            .execute(json!({
                "command": invalid_utf8_stderr_command()
            }))
            .await;

        assert!(result.is_ok(), "invalid UTF-8 stderr should not fail");
        let payload: Value = serde_json::from_str(&result.unwrap().result).unwrap();
        let stderr = payload["stderr"].as_str().unwrap_or_default();
        assert!(!stderr.is_empty());
    }

    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    async fn bash_foreground_failure_includes_full_python_tried_list() {
        prime_test_command_environment();
        let tool = BashTool::new();
        let result = tool
            .execute(json!({
                "command": "false"
            }))
            .await
            .unwrap();

        assert!(!result.success);
        let payload: Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(payload["exit_code"], 1);
        assert_eq!(payload["environment"]["python"]["tried_total"], 1);
        assert_eq!(payload["environment"]["python"]["tried"][0], "python3");
    }

    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    async fn bash_foreground_sets_stdout_truncated_when_output_exceeds_cap() {
        prime_test_command_environment();
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
        prime_test_command_environment();
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
        assert_eq!(payload["environment"]["source"], "process_env");
        assert_eq!(
            payload["environment"]["python"]["resolved"],
            "/usr/bin/python3"
        );
        assert_eq!(
            payload["environment"]["python"]["invocation"],
            "/usr/bin/python3"
        );
        assert_eq!(payload["environment"]["python"]["tried_total"], 1);
        assert!(payload["environment"]["python"].get("tried").is_none());
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

    /// A short background command must emit a `BashCompleted` signal carrying the
    /// session's `bash_id` and an exit code of `Some(0)` (issue #84, phase 1).
    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    async fn bash_background_emits_completion_event_with_exit_code() {
        prime_test_command_environment();
        let (tx, mut rx) = mpsc::channel(8);
        let shell = super::bash_runtime::spawn_background("true", None, Some(tx), None, false, None)
            .await
            .expect("background shell should spawn");
        let expected_id = shell.id.clone();

        let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for BashCompleted event")
            .expect("event channel closed before BashCompleted");

        match event {
            AgentEvent::BashCompleted {
                bash_id,
                command,
                exit_code,
                status,
            } => {
                assert_eq!(bash_id, expected_id);
                assert_eq!(command, "true");
                assert_eq!(exit_code, Some(0));
                assert_eq!(status, "completed");
            }
            other => panic!("expected BashCompleted, got {other:?}"),
        }
    }

    /// A failing background command still reports `status="completed"` with its
    /// non-zero exit code — a non-zero exit is a normal completion, not a kill.
    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    async fn bash_background_emits_completion_event_for_failing_command() {
        prime_test_command_environment();
        let (tx, mut rx) = mpsc::channel(8);
        let shell = super::bash_runtime::spawn_background("false", None, Some(tx), None, false, None)
            .await
            .expect("background shell should spawn");
        let expected_id = shell.id.clone();

        let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for BashCompleted event")
            .expect("event channel closed before BashCompleted");

        match event {
            AgentEvent::BashCompleted {
                bash_id,
                exit_code,
                status,
                ..
            } => {
                assert_eq!(bash_id, expected_id);
                assert_eq!(exit_code, Some(1));
                assert_eq!(status, "completed");
            }
            other => panic!("expected BashCompleted, got {other:?}"),
        }
    }

    /// A killed background command must report `status="killed"` with
    /// `exit_code=None` (no numeric code for signal termination on Unix).
    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    async fn bash_background_emits_killed_when_shell_is_killed() {
        prime_test_command_environment();
        let (tx, mut rx) = mpsc::channel(8);
        let shell = super::bash_runtime::spawn_background("sleep 30", None, Some(tx), None, false, None)
            .await
            .expect("background shell should spawn");
        let expected_id = shell.id.clone();

        shell.kill().await.expect("shell should be killable");

        let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for BashCompleted event")
            .expect("event channel closed before BashCompleted");

        match event {
            AgentEvent::BashCompleted {
                bash_id,
                exit_code,
                status,
                ..
            } => {
                assert_eq!(bash_id, expected_id);
                assert_eq!(exit_code, None);
                assert_eq!(status, "killed");
            }
            other => panic!("expected BashCompleted, got {other:?}"),
        }
    }

    /// With no event sender, the poll task must skip the emit entirely and still
    /// flip the shell to "completed" (issue #84, phase 1).
    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    async fn bash_background_without_sender_still_completes() {
        prime_test_command_environment();
        let shell = super::bash_runtime::spawn_background("true", None, None, None, false, None)
            .await
            .expect("background shell should spawn");

        let started = Instant::now();
        loop {
            if shell.status() == "completed" {
                break;
            }
            if started.elapsed() > Duration::from_secs(3) {
                panic!("shell never reached completed without a sender");
            }
            sleep(Duration::from_millis(25)).await;
        }
    }

    /// A saturated event channel must not hang or panic the poll task: the
    /// completion send hits the 500ms bounded-timeout path, logs a `warn!`, and
    /// the shell still completes. The signal is dropped (observable), not lost
    /// silently. The channel is kept full past the timeout window so the send is
    /// guaranteed to time out rather than succeed when a slot is freed.
    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    async fn bash_background_drops_completion_when_channel_saturated() {
        prime_test_command_environment();
        // Capacity-1 channel pre-filled so the single slot is occupied.
        let (tx, mut rx) = mpsc::channel::<AgentEvent>(1);
        tx.try_send(AgentEvent::Token {
            content: "occupy".into(),
        })
        .expect("prefill channel slot");

        let shell = super::bash_runtime::spawn_background("true", None, Some(tx), None, false, None)
            .await
            .expect("background shell should spawn");

        // Wait past the 500ms bounded-send window so the dropped BashCompleted
        // has been observed and the poll task has moved on.
        sleep(Duration::from_millis(650)).await;

        // The only event ever delivered is the pre-filled token; BashCompleted
        // was dropped (saturated channel) and never enqueued.
        let only = rx
            .recv()
            .await
            .expect("prefilled token should still be present");
        assert!(
            matches!(only, AgentEvent::Token { .. }),
            "expected only the pre-filled token, got {only:?}"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), rx.recv())
                .await
                .is_err(),
            "no BashCompleted should be delivered after a saturation drop"
        );
        assert_eq!(
            shell.status(),
            "completed",
            "shell must still reach completed after a dropped signal"
        );
    }

    /// Drives the real tool dispatch path: `BashTool::execute_with_context`
    /// with `run_in_background=true` and a context built via `for_dispatch`
    /// carrying an `event_tx`, so the signal flows through `ctx.cloned_sender()`
    /// (the production wiring), not just `spawn_background` directly.
    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    async fn bash_tool_background_dispatch_emits_completion_event() {
        prime_test_command_environment();
        let tool = BashTool::new();
        let (tx, mut rx) = mpsc::channel(8);
        let ctx = ToolExecutionContext::for_dispatch(
            "session_84",
            "call_84",
            &tx,
            &[],
            ToolExecutionSessionFlags::default(),
            true,
            None,
            None,
        );

        let result = tool
            .execute_with_context(json!({ "command": "true", "run_in_background": true }), ctx)
            .await
            .expect("background dispatch should succeed");
        assert!(result.success);

        let payload: Value = serde_json::from_str(&result.result).unwrap();
        let bash_id = payload["bash_id"].as_str().unwrap().to_string();
        assert_eq!(payload["status"], "running");

        let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for BashCompleted event")
            .expect("event channel closed before BashCompleted");

        match event {
            AgentEvent::BashCompleted {
                bash_id: id,
                exit_code,
                status,
                ..
            } => {
                assert_eq!(id, bash_id);
                assert_eq!(exit_code, Some(0));
                assert_eq!(status, "completed");
            }
            other => panic!("expected BashCompleted, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn bash_resolves_relative_workdir_from_session_workspace() {
        prime_test_command_environment();
        let tool = BashTool::new();
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base");
        let nested = base.join("nested");
        tokio::fs::create_dir_all(&nested).await.unwrap();

        let session_id = format!("session_{}", uuid::Uuid::new_v4());
        super::workspace_state::set_workspace(&session_id, base.canonicalize().unwrap());

        let result = tool
            .execute_with_context(
                json!({
                    "command": "pwd",
                    "workdir": "nested"
                }),
                ToolExecutionContext {
                    session_id: Some(&session_id),
                    tool_call_id: "call_1",
                    event_tx: None,
                    available_tool_schemas: None,
                    bypass_permissions: false,
                    can_async_resume: false,
                    bash_completion_sink: None,
                    pre_parsed_args: None,
                },
            )
            .await
            .unwrap();

        let payload: Value = serde_json::from_str(&result.result).unwrap();
        let expected =
            bamboo_config::paths::path_to_display_string(&nested.canonicalize().unwrap());
        assert_eq!(payload["cwd"].as_str().unwrap_or_default(), expected);
    }

    #[tokio::test]
    async fn bash_rejects_workdir_that_is_not_directory() {
        prime_test_command_environment();
        let tool = BashTool::new();
        let file = tempfile::NamedTempFile::new().unwrap();

        let result = tool
            .execute(json!({
                "command": "echo hello",
                "workdir": file.path()
            }))
            .await;

        assert!(
            matches!(result, Err(ToolError::InvalidArguments(msg)) if msg.contains("directory"))
        );
    }

    /// `running_shells_for_session` reports only the running shells tagged with the
    /// requested session, excluding completed shells and shells owned by another
    /// session (or none). (issue #84, phase 2a.)
    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    async fn running_shells_for_session_filters_by_session_and_status() {
        prime_test_command_environment();

        // Two long-running shells owned by sess-A.
        let a1 = super::bash_runtime::spawn_background(
            "sleep 30",
            None,
            None,
            Some("sess-A".to_string()),
            false,
            None,
        )
        .await
        .expect("spawn a1");
        let a2 = super::bash_runtime::spawn_background(
            "sleep 30",
            None,
            None,
            Some("sess-A".to_string()),
            false,
            None,
        )
        .await
        .expect("spawn a2");
        // One long-running shell owned by sess-B.
        let b = super::bash_runtime::spawn_background(
            "sleep 30",
            None,
            None,
            Some("sess-B".to_string()),
            false,
            None,
        )
        .await
        .expect("spawn b");
        // An untagged (None) long-running shell.
        let untagged = super::bash_runtime::spawn_background("sleep 30", None, None, None, false, None)
            .await
            .expect("spawn untagged");
        // A sess-A shell that completes immediately — must be excluded once done.
        let done = super::bash_runtime::spawn_background(
            "true",
            None,
            None,
            Some("sess-A".to_string()),
            false,
            None,
        )
        .await
        .expect("spawn done");

        // Wait for the fast shell to reach "completed" so we exercise the status filter.
        let started = Instant::now();
        loop {
            if done.status() == "completed" {
                break;
            }
            if started.elapsed() > Duration::from_secs(3) {
                panic!("sess-A fast shell never completed");
            }
            sleep(Duration::from_millis(25)).await;
        }

        // sess-A should report exactly the two long-running shells, order-independent.
        let mut running = super::bash_runtime::running_shells_for_session("sess-A");
        running.sort();
        let mut expected = vec![a1.id.clone(), a2.id.clone()];
        expected.sort();
        assert_eq!(running, expected);

        // sess-B should report exactly its own single running shell.
        assert_eq!(
            super::bash_runtime::running_shells_for_session("sess-B"),
            vec![b.id.clone()]
        );

        // Cleanup: kill the still-running shells so the GC isn't left holding them,
        // and drop the already-completed `done` shell from the process-global registry.
        for shell in [a1, a2, b, untagged] {
            let _ = shell.kill().await;
        }
        let _ = super::bash_runtime::remove_shell(&done.id);
    }

    // ── Auto-sync promotion tests (issue #84, phase 2d) ──────────────────

    // (a) A fast command via the auto (None) path returns a synchronous result
    // with stdout + exit_code and no bash_id — identical to the old foreground.
    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    async fn auto_path_fast_command_returns_synchronous_result() {
        prime_test_command_environment();
        let tool = BashTool::new();
        let (tx, _rx) = mpsc::channel(32);
        let ctx = ToolExecutionContext {
            session_id: Some("session_auto_fast"),
            tool_call_id: "call_auto_fast",
            event_tx: Some(&tx),
            available_tool_schemas: None,
            bypass_permissions: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        };

        let result = tool
            .execute_with_context(json!({ "command": "echo auto-fast-output" }), ctx)
            .await
            .expect("auto fast command should succeed");

        let payload: Value = serde_json::from_str(&result.result).unwrap();
        assert!(
            payload.get("bash_id").is_none(),
            "fast auto command must not return a bash_id"
        );
        assert_eq!(payload["exit_code"], 0);
        assert_eq!(payload["timed_out"], false);
        assert!(payload["stdout"]
            .as_str()
            .unwrap_or_default()
            .contains("auto-fast-output"));
    }

    // (b) Promotion: with a low test threshold, a long command (sleep) on the
    // auto path returns a background {bash_id, running} result, the adopted
    // shell appears in running_shells_for_session, and later emits
    // BashCompleted with the right id.
    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    async fn auto_path_promotes_long_command_to_background() {
        prime_test_command_environment();
        let tool = BashTool::new();
        let session_id = "session_auto_promote";
        let (tx, mut rx) = mpsc::channel(8);
        let ctx = ToolExecutionContext::for_dispatch(
            session_id,
            "call_auto_promote",
            &tx,
            &[],
            ToolExecutionSessionFlags::default(),
            // `can_async_resume` is irrelevant here — this test drives
            // promotion directly via run_streaming_command(Some(200)).
            true,
            None,
            None,
        );
        let cwd = super::workspace_state::workspace_or_process_cwd(Some(session_id));

        // Call run_streaming_command directly with a 200ms promotion threshold
        // so this test exercises promotion deterministically without depending
        // on the production 10s default or the None dispatch arm's
        // `can_async_resume` gating.
        let result = tool
            .run_streaming_command("sleep 10", 60000, Some(200), &cwd, ctx)
            .await
            .expect("auto promote should succeed");

        assert!(result.success);
        let payload: Value = serde_json::from_str(&result.result).unwrap();
        let bash_id = payload["bash_id"]
            .as_str()
            .expect("promoted result must have bash_id")
            .to_string();
        assert_eq!(payload["status"], "running");

        let running = super::bash_runtime::running_shells_for_session(session_id);
        assert!(
            running.contains(&bash_id),
            "adopted shell {bash_id} must appear in running_shells, got {running:?}"
        );

        let event = tokio::time::timeout(Duration::from_secs(15), rx.recv())
            .await
            .expect("timed out waiting for BashCompleted")
            .expect("event channel closed before BashCompleted");
        match event {
            AgentEvent::BashCompleted {
                bash_id: id,
                exit_code,
                status,
                ..
            } => {
                assert_eq!(id, bash_id);
                assert_eq!(exit_code, Some(0));
                assert_eq!(status, "completed");
            }
            other => panic!("expected BashCompleted, got {other:?}"),
        }

        if let Some(shell) = super::bash_runtime::get_shell(&bash_id) {
            let _ = shell.kill().await;
        }
    }

    // (c) run_in_background:Some(false) does NOT promote — blocks/times out
    // like the pre-2d foreground path.
    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    async fn force_sync_does_not_promote_and_times_out() {
        prime_test_command_environment();
        let tool = BashTool::new();

        let result = tool
            .execute(json!({
                "command": "sleep 10",
                "run_in_background": false,
                "timeout": 50
            }))
            .await
            .expect("force-sync should produce a timed-out result");

        let payload: Value = serde_json::from_str(&result.result).unwrap();
        assert!(
            payload.get("bash_id").is_none(),
            "force-sync must never promote"
        );
        assert_eq!(payload["timed_out"], true);
        assert!(!result.success, "timed-out result must not be successful");
    }

    // (e) Auto path (run_in_background OMITTED) with can_async_resume == false
    // also does NOT promote — it runs purely synchronously, exactly like (c),
    // even though promotion is now the default. This is the hook-less-path guard
    // (issue #84, phase 2d): a loop that can't suspend+resume a background shell
    // (schedule path, agent-core loop) must never orphan one via auto-promotion.
    // `execute()` builds `ToolExecutionContext::none`, whose can_async_resume is
    // false, so the None dispatch arm passes promote_after_ms = None.
    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    async fn auto_path_does_not_promote_when_not_resume_capable() {
        prime_test_command_environment();
        let tool = BashTool::new();

        let result = tool
            .execute(json!({
                "command": "sleep 10",
                "timeout": 50
            }))
            .await
            .expect("non-resume-capable auto path should produce a result");

        let payload: Value = serde_json::from_str(&result.result).unwrap();
        assert!(
            payload.get("bash_id").is_none(),
            "auto path must not promote when can_async_resume is false"
        );
        assert_eq!(payload["timed_out"], true);
        assert!(
            !result.success,
            "a timed-out command must not report success"
        );
    }

    // (d) adopt_running_child preserves already-captured output: seed lines
    // appear in subsequent read_output_since calls.
    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    async fn adopt_running_child_preserves_seeded_output() {
        prime_test_command_environment();
        let shell = bamboo_infrastructure::process::preferred_bash_shell();
        let mut cmd = tokio::process::Command::new(&shell.program);
        bamboo_infrastructure::process::hide_window_for_tokio_command(&mut cmd);
        cmd.arg(shell.arg)
            .arg("echo seeded-line-1; echo seeded-line-2; sleep 5")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let mut child = cmd.spawn().expect("spawn child");
        let stdout_reader = tokio::io::BufReader::new(child.stdout.take().unwrap());
        let stderr_reader = tokio::io::BufReader::new(child.stderr.take().unwrap());

        // Let the echo output land in the pipe buffer before adoption.
        sleep(Duration::from_millis(200)).await;

        let session = super::bash_runtime::adopt_running_child(
            child,
            stdout_reader,
            stderr_reader,
            vec!["seeded-line-1".to_string(), "seeded-line-2".to_string()],
            vec![],
            "echo seeded-line-1; echo seeded-line-2; sleep 5",
            Some("session_seed_test".to_string()),
            test_environment_diagnostics(),
            None,
            None,
        )
        .await
        .expect("adopt should succeed");

        let (lines, _cursor, _dropped) = session.read_output_since(0, None).await;
        assert!(
            lines.iter().any(|l| l.contains("seeded-line-1")),
            "seeded line 1 must be present, got {lines:?}"
        );
        assert!(
            lines.iter().any(|l| l.contains("seeded-line-2")),
            "seeded line 2 must be present, got {lines:?}"
        );

        let _ = session.kill().await;
        let _ = super::bash_runtime::remove_shell(&session.id);
    }

    // ── Phase 2b follow-up: loop-facing completion push (BashCompletionSink) ──

    /// A `BashCompletionSink` that records every completion it receives, so a
    /// test can assert the producer pushed the right info + output tail.
    #[derive(Clone, Default)]
    struct RecordingSink {
        received: std::sync::Arc<std::sync::Mutex<Vec<bamboo_agent_core::BashCompletionInfo>>>,
    }

    impl bamboo_agent_core::BashCompletionSink for RecordingSink {
        fn on_bash_completed(&self, info: bamboo_agent_core::BashCompletionInfo) {
            self.received.lock().unwrap().push(info);
        }
    }

    async fn wait_for_sink(
        recorder: &RecordingSink,
        what: &str,
    ) -> bamboo_agent_core::BashCompletionInfo {
        let started = Instant::now();
        loop {
            if let Some(info) = recorder.received.lock().unwrap().first().cloned() {
                return info;
            }
            if started.elapsed() > Duration::from_secs(5) {
                panic!("completion sink was not called for {what}");
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    #[tokio::test]
    async fn background_completion_pushes_to_sink_with_output_tail() {
        prime_test_command_environment();
        let recorder = RecordingSink::default();
        let sink: std::sync::Arc<dyn bamboo_agent_core::BashCompletionSink> =
            std::sync::Arc::new(recorder.clone());

        let shell = super::bash_runtime::spawn_background(
            "echo hello-sink",
            None,
            None,
            Some("sess-sink".to_string()),
            false,
            Some(sink),
        )
        .await
        .expect("spawn");

        let info = wait_for_sink(&recorder, "echo").await;
        assert_eq!(info.session_id, "sess-sink");
        assert_eq!(info.bash_id, shell.id);
        assert_eq!(info.status, "completed");
        assert_eq!(info.exit_code, Some(0));
        assert!(
            info.output_tail.contains("hello-sink"),
            "output tail should carry the command output, got: {:?}",
            info.output_tail
        );

        let _ = super::bash_runtime::remove_shell(&shell.id);
    }

    #[tokio::test]
    async fn background_completion_carries_nonzero_exit_code() {
        prime_test_command_environment();
        let recorder = RecordingSink::default();
        let sink: std::sync::Arc<dyn bamboo_agent_core::BashCompletionSink> =
            std::sync::Arc::new(recorder.clone());

        let shell = super::bash_runtime::spawn_background(
            "exit 3",
            None,
            None,
            Some("sess-exit".to_string()),
            false,
            Some(sink),
        )
        .await
        .expect("spawn");

        let info = wait_for_sink(&recorder, "exit 3").await;
        assert_eq!(info.status, "completed");
        assert_eq!(info.exit_code, Some(3));

        let _ = super::bash_runtime::remove_shell(&shell.id);
    }

    #[tokio::test]
    async fn untagged_shell_does_not_invoke_sink() {
        prime_test_command_environment();
        let recorder = RecordingSink::default();
        let sink: std::sync::Arc<dyn bamboo_agent_core::BashCompletionSink> =
            std::sync::Arc::new(recorder.clone());

        // session_id = None → no owning loop to notify → the sink must not fire,
        // even though it is wired.
        let shell =
            super::bash_runtime::spawn_background("true", None, None, None, false, Some(sink))
                .await
                .expect("spawn");

        let started = Instant::now();
        while shell.status() == "running" {
            if started.elapsed() > Duration::from_secs(5) {
                panic!("shell never completed");
            }
            sleep(Duration::from_millis(50)).await;
        }
        // Give the poll task's post-exit emit path time to run.
        sleep(Duration::from_millis(200)).await;
        assert!(
            recorder.received.lock().unwrap().is_empty(),
            "an untagged (session-less) shell must not push a completion"
        );

        let _ = super::bash_runtime::remove_shell(&shell.id);
    }
}
