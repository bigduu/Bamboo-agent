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

    async fn run_foreground(
        &self,
        command: &str,
        timeout_ms: u64,
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
                line = stdout_reader.read_until(b'\n', &mut stdout_line_bytes), if !stdout_done => {
                    match line {
                        Ok(0) => stdout_done = true,
                        Ok(_) => {
                            let line = decode_process_line_lossy(&mut stdout_line_bytes);
                            Self::append_capped(&mut stdout_buf, &line, &mut stdout_truncated);
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
                            ctx.emit_tool_token(format!("{}\n", line)).await;
                        }
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
        let cwd_display = bamboo_config::paths::path_to_display_string(cwd);

        let environment = Self::environment_json(&prepared_env.diagnostics, !success);

        Ok(ToolResult {
            success,
            result: json!({
                "command": command,
                "cwd": cwd_display,
                "stdout": stdout_buf,
                "stderr": stderr_buf,
                "exit_code": exit_code,
                "timed_out": timed_out,
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
        "Execute shell commands with streaming output (supports background mode). Default timeout is 120000ms (max 600000ms); captured stdout/stderr are each capped at 512KB."
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
                    "description": "Set to true to run this command in the background"
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
        if parsed.run_in_background.unwrap_or(false) {
            let shell = bash_runtime::spawn_background(command, Some(&cwd), ctx.cloned_sender())
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
                    "cwd": bamboo_config::paths::path_to_display_string(&cwd),
                    "environment": Self::environment_json(&shell.environment, false),
                })
                .to_string(),
                display_preference: Some("Collapsible".to_string()),
                images: Vec::new(),
            });
        }

        self.run_foreground(command, timeout_ms, &cwd, ctx).await
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
        let shell = super::bash_runtime::spawn_background("true", None, Some(tx))
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
        let shell = super::bash_runtime::spawn_background("false", None, Some(tx))
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
        let shell = super::bash_runtime::spawn_background("sleep 30", None, Some(tx))
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
        let shell = super::bash_runtime::spawn_background("true", None, None)
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

        let shell = super::bash_runtime::spawn_background("true", None, Some(tx))
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
}
