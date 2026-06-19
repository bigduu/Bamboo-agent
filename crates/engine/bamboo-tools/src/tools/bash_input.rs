use async_trait::async_trait;
use bamboo_agent_core::{Tool, ToolError, ToolResult};
use serde::Deserialize;
use serde_json::json;

use super::bash_runtime;

#[derive(Debug, Deserialize)]
struct BashInputArgs {
    bash_id: String,
    input: String,
    #[serde(default = "default_append_newline")]
    append_newline: bool,
}

fn default_append_newline() -> bool {
    true
}

pub struct BashInputTool;

impl BashInputTool {
    pub fn new() -> Self {
        Self
    }
}

impl Default for BashInputTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for BashInputTool {
    fn name(&self) -> &str {
        "BashInput"
    }

    fn description(&self) -> &str {
        "Send input to the stdin of an interactive background Bash shell. \
         The shell must have been spawned with Bash(interactive=true), which \
         gives it a piped stdin; non-interactive shells have no stdin pipe and \
         this tool returns an error. By default a trailing newline is appended \
         so the input is delivered as a complete line."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "bash_id": {
                    "type": "string",
                    "description": "The ID of the interactive background shell to send input to"
                },
                "input": {
                    "type": "string",
                    "description": "The text to write to the shell's stdin"
                },
                "append_newline": {
                    "type": "boolean",
                    "description": "Append a trailing newline to the input (default true). Set to false to send raw bytes without a line terminator."
                }
            },
            "required": ["bash_id", "input"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let parsed: BashInputArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid BashInput args: {}", e)))?;

        if parsed.input.is_empty() && !parsed.append_newline {
            return Err(ToolError::InvalidArguments(
                "'input' must not be empty when append_newline is false".to_string(),
            ));
        }

        let shell = bash_runtime::get_shell(parsed.bash_id.trim()).ok_or_else(|| {
            ToolError::Execution(format!("Background shell '{}' not found", parsed.bash_id))
        })?;

        shell
            .write_stdin(&parsed.input, parsed.append_newline)
            .await
            .map_err(ToolError::Execution)?;

        Ok(ToolResult {
            success: true,
            result: json!({
                "bash_id": shell.id,
                "status": shell.status(),
                "bytes_written": if parsed.append_newline {
                    parsed.input.len() + 1
                } else {
                    parsed.input.len()
                },
            })
            .to_string(),
            display_preference: Some("Collapsible".to_string()),
            images: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_infrastructure::process::{
        clear_command_environment_cache_for_tests, prime_command_environment_cache_for_tests,
        CommandEnvironmentDiagnostics, CommandEnvironmentSource, PythonDiscoveryDiagnostics,
    };
    use std::collections::HashMap;
    use tokio::time::{sleep, Duration, Instant};

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

    /// Helper: poll `shell`'s output until a line containing `needle` appears,
    /// or time out after `secs` seconds.
    async fn wait_for_output_contains(shell: &bash_runtime::ShellSession, needle: &str, secs: u64) {
        let deadline = Instant::now() + Duration::from_secs(secs);
        loop {
            let (lines, _, _) = shell.read_output_since(0, None).await;
            if lines.iter().any(|l| l.contains(needle)) {
                return;
            }
            if Instant::now() >= deadline {
                panic!("timed out waiting for '{needle}' in output; got: {lines:?}");
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    // (a) Interactive shell: BashInput feeds stdin, output appears via read_output_since.
    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    async fn bash_input_feeds_interactive_shell_and_output_appears() {
        prime_test_command_environment();
        // `cat` echoes its stdin to stdout — perfect for round-trip verification.
        let shell = bash_runtime::spawn_background("cat", None, None, None, true)
            .await
            .expect("spawn interactive shell");
        assert_eq!(shell.status(), "running");

        let tool = BashInputTool::new();
        let result = tool
            .execute(json!({
                "bash_id": shell.id,
                "input": "hello-from-bashinput"
            }))
            .await
            .expect("BashInput should succeed on interactive shell");
        assert!(result.success);

        // The echoed input must appear in the shell's captured output.
        wait_for_output_contains(&shell, "hello-from-bashinput", 5).await;

        let _ = shell.kill().await;
        let _ = bash_runtime::remove_shell(&shell.id);
    }

    // (b) write_stdin on a NON-interactive shell returns a clear error — never panics.
    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    async fn write_stdin_errors_on_non_interactive_shell() {
        prime_test_command_environment();
        let shell = bash_runtime::spawn_background("sleep 5", None, None, None, false)
            .await
            .expect("spawn non-interactive shell");

        let err = shell
            .write_stdin("hello", true)
            .await
            .expect_err("write_stdin must error on non-interactive shell");
        assert!(
            err.contains("interactive"),
            "error should explain the shell is not interactive: {err}"
        );

        let _ = shell.kill().await;
        let _ = bash_runtime::remove_shell(&shell.id);
    }

    // (c) write_stdin on an EXITED interactive shell returns a clear error — never panics.
    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    async fn write_stdin_errors_on_exited_interactive_shell() {
        prime_test_command_environment();
        let shell = bash_runtime::spawn_background("true", None, None, None, true)
            .await
            .expect("spawn interactive shell");

        // Wait for the shell to exit.
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            if shell.status() == "completed" {
                break;
            }
            if Instant::now() >= deadline {
                panic!("shell did not exit in time");
            }
            sleep(Duration::from_millis(25)).await;
        }
        // Give the OS a moment to close the pipe after process exit.
        sleep(Duration::from_millis(50)).await;

        let err = shell
            .write_stdin("hello", true)
            .await
            .expect_err("write_stdin must error on exited shell");
        assert!(
            !err.contains("interactive"),
            "error should be a pipe/write failure, not a missing-handle error: {err}"
        );

        let _ = bash_runtime::remove_shell(&shell.id);
    }

    // (d) A non-interactive command that reads stdin gets immediate EOF (Stdio::null)
    // and terminates — it must NOT hang waiting for input. This is the preserved
    // default behavior: Stdio::null() on every non-interactive path.
    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    async fn non_interactive_stdin_reader_gets_eof_and_terminates() {
        prime_test_command_environment();
        // `cat` reads stdin; with Stdio::null() it receives immediate EOF and exits 0.
        let shell = bash_runtime::spawn_background("cat", None, None, None, false)
            .await
            .expect("spawn non-interactive shell");

        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            if shell.status() == "completed" {
                break;
            }
            if Instant::now() >= deadline {
                panic!("non-interactive `cat` must terminate on EOF, not hang");
            }
            sleep(Duration::from_millis(25)).await;
        }

        let code = shell.exit_code().await;
        assert_eq!(code, Some(0), "cat should exit cleanly on immediate EOF");

        let _ = bash_runtime::remove_shell(&shell.id);
    }

    // BashInput on an unknown shell id returns a not-found error.
    #[tokio::test]
    async fn bash_input_errors_on_unknown_shell() {
        let tool = BashInputTool::new();
        let result = tool
            .execute(json!({
                "bash_id": "nonexistent-shell-id",
                "input": "hello"
            }))
            .await;
        assert!(result.is_err(), "BashInput must error on unknown shell id");
        match result {
            Err(ToolError::Execution(msg)) => {
                assert!(msg.contains("not found"), "unexpected error: {msg}");
            }
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    // BashInput on a non-interactive shell returns an error via the tool path.
    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    async fn bash_input_errors_on_non_interactive_shell_via_tool() {
        prime_test_command_environment();
        let shell = bash_runtime::spawn_background("sleep 5", None, None, None, false)
            .await
            .expect("spawn non-interactive shell");

        let tool = BashInputTool::new();
        let result = tool
            .execute(json!({
                "bash_id": shell.id,
                "input": "hello"
            }))
            .await;
        assert!(
            result.is_err(),
            "BashInput must error on non-interactive shell"
        );
        match result {
            Err(ToolError::Execution(msg)) => {
                assert!(
                    msg.contains("interactive"),
                    "error should mention interactive: {msg}"
                );
            }
            other => panic!("expected Execution error, got {other:?}"),
        }

        let _ = shell.kill().await;
        let _ = bash_runtime::remove_shell(&shell.id);
    }

    // append_newline=false sends raw bytes without a trailing newline.
    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    async fn bash_input_append_newline_false_sends_raw_bytes() {
        prime_test_command_environment();
        let shell = bash_runtime::spawn_background("cat", None, None, None, true)
            .await
            .expect("spawn interactive shell");

        let tool = BashInputTool::new();
        // Send "raw-payload" with no newline; cat won't produce a line until it
        // gets one, so send a second write WITH newline to flush it.
        let result = tool
            .execute(json!({
                "bash_id": shell.id,
                "input": "raw-payload",
                "append_newline": false
            }))
            .await
            .expect("raw write should succeed");
        assert!(result.success);

        // Now send a newline so cat emits the buffered line.
        tool.execute(json!({
            "bash_id": shell.id,
            "input": "",
        }))
        .await
        .expect("newline write should succeed");

        wait_for_output_contains(&shell, "raw-payload", 5).await;

        let _ = shell.kill().await;
        let _ = bash_runtime::remove_shell(&shell.id);
    }

    // BashInput rejects empty input with append_newline=false.
    #[tokio::test]
    async fn bash_input_rejects_empty_raw_input() {
        let tool = BashInputTool::new();
        let result = tool
            .execute(json!({
                "bash_id": "fake",
                "input": "",
                "append_newline": false
            }))
            .await;
        assert!(matches!(result, Err(ToolError::InvalidArguments(_))));
    }

    // The default append_newline is true (serde default function).
    #[test]
    fn default_append_newline_is_true() {
        assert!(default_append_newline());
    }
}
