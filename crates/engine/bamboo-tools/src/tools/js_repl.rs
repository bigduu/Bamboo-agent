use async_trait::async_trait;
use bamboo_agent_core::{Tool, ToolClass, ToolCtx, ToolError, ToolOutcome, ToolResult};
use serde::Deserialize;
use serde_json::json;
use std::path::PathBuf;
use std::process::Stdio;
use tokio::process::Command;
use tokio::time::{timeout, Duration};

use super::workspace_state;

const DEFAULT_TIMEOUT_MS: u64 = 30_000;
const MAX_TIMEOUT_MS: u64 = 120_000;
const MAX_OUTPUT_BYTES: usize = 256 * 1024;

#[derive(Debug, Deserialize)]
struct JsReplArgs {
    code: String,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

/// JavaScript REPL tool — executes JavaScript code using Node.js.
///
/// Uses a fresh Node.js subprocess per invocation.  The code is piped to
/// `node --input-type=module` on stdin so multi-line programs and `await`
/// at the top level are supported.
///
/// Inspired by Codex's `js_repl` tool but uses a simpler sub-process model
/// rather than a persistent kernel.
pub struct JsReplTool;

impl JsReplTool {
    pub fn new() -> Self {
        Self
    }

    fn effective_timeout(requested: Option<u64>) -> Duration {
        let ms = requested
            .unwrap_or(DEFAULT_TIMEOUT_MS)
            .clamp(1, MAX_TIMEOUT_MS);
        Duration::from_millis(ms)
    }

    fn truncate_output(s: &str) -> (&str, bool) {
        if s.len() <= MAX_OUTPUT_BYTES {
            (s, false)
        } else {
            let mut end = MAX_OUTPUT_BYTES;
            while end > 0 && !s.is_char_boundary(end) {
                end -= 1;
            }
            (&s[..end], true)
        }
    }
}

impl Default for JsReplTool {
    fn default() -> Self {
        Self::new()
    }
}

/// Resolve a Node.js binary, checking `BAMBOO_JS_REPL_NODE_PATH` env var
/// first, then falling back to a PATH lookup for "node".
fn resolve_node() -> Option<PathBuf> {
    if let Ok(path) = std::env::var("BAMBOO_JS_REPL_NODE_PATH") {
        let p = PathBuf::from(&path);
        if p.exists() {
            return Some(p);
        }
    }
    find_in_path("node")
}

/// Simple cross-platform PATH lookup (avoids the `which` crate dependency).
fn find_in_path(name: &str) -> Option<PathBuf> {
    let path_var = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path_var) {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
        // On Windows, try common extensions
        #[cfg(windows)]
        for ext in &["exe", "cmd", "bat"] {
            let with_ext = dir.join(format!("{}.{}", name, ext));
            if with_ext.is_file() {
                return Some(with_ext);
            }
        }
    }
    None
}

#[async_trait]
impl Tool for JsReplTool {
    fn name(&self) -> &str {
        "js_repl"
    }

    fn description(&self) -> &str {
        "Execute JavaScript code using Node.js. Supports top-level await and ES modules. The code is run in a fresh process each time; use js_repl_reset is not needed since state is not shared between calls."
    }

    fn classify(&self, _args: &serde_json::Value) -> ToolClass {
        ToolClass::MUTATING_SERIAL.promotable()
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "code": {
                    "type": "string",
                    "description": "JavaScript code to execute"
                },
                "timeout_ms": {
                    "type": "number",
                    "description": "Optional timeout in milliseconds (default 30000, max 120000)"
                }
            },
            "required": ["code"],
            "additionalProperties": false
        })
    }

    async fn invoke(
        &self,
        args: serde_json::Value,
        ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        let parsed: JsReplArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid js_repl args: {}", e)))?;

        let code = parsed.code.trim();
        if code.is_empty() {
            return Err(ToolError::InvalidArguments(
                "'code' cannot be empty".to_string(),
            ));
        }

        let node_path = resolve_node().ok_or_else(|| {
            ToolError::Execution(
                "Node.js not found. Install Node.js or set BAMBOO_JS_REPL_NODE_PATH.".to_string(),
            )
        })?;

        let effective_timeout = Self::effective_timeout(parsed.timeout_ms);

        // Wrap code in an async IIFE to support top-level await.
        let wrapper = format!(
            r#"(async () => {{
{}
}})().catch(e => {{ console.error(e); process.exit(1); }});"#,
            code
        );

        // #217: run in the session's workspace (falls back to the process cwd
        // when no workspace is tracked — see `workspace_state::workspace_or_
        // process_cwd`) instead of silently inheriting whatever directory the
        // host process happens to be in. This is the same shared cwd
        // resolution `Bash`/`Glob`/`Grep`/`Workspace`/`slash_command` already
        // use, so relative paths in REPL code (`fs.writeFileSync('out.txt',
        // ...)`) land in the same place a Bash command run in this session
        // would land them.
        let cwd = workspace_state::workspace_or_process_cwd(ctx.session_id());

        // `kill_on_drop(true)` ensures the child is killed when it goes out of
        // scope (i.e. on timeout). `wait_with_output()` takes ownership, so on
        // timeout the future is dropped and the child is killed automatically.
        let child = Command::new(&node_path)
            .arg("-e")
            .arg(&wrapper)
            .current_dir(&cwd)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| {
                ToolError::Execution(format!(
                    "Failed to start Node.js ({}): {}",
                    node_path.display(),
                    e
                ))
            })?;

        match timeout(effective_timeout, child.wait_with_output()).await {
            Ok(Ok(output)) => {
                let stdout_raw = String::from_utf8_lossy(&output.stdout);
                let stderr_raw = String::from_utf8_lossy(&output.stderr);
                let (stdout, stdout_truncated) = Self::truncate_output(&stdout_raw);
                let (stderr, stderr_truncated) = Self::truncate_output(&stderr_raw);
                let exit_code = output.status.code();
                let success = output.status.success();

                Ok(ToolOutcome::Completed(ToolResult {
                    success,
                    result: json!({
                        "exit_code": exit_code,
                        "stdout": stdout,
                        "stderr": stderr,
                        "stdout_truncated": stdout_truncated,
                        "stderr_truncated": stderr_truncated,
                        "timed_out": false,
                    })
                    .to_string(),
                    display_preference: Some("Collapsible".to_string()),
                    images: Vec::new(),
                }))
            }
            Ok(Err(e)) => Err(ToolError::Execution(format!(
                "Node.js process error: {}",
                e
            ))),
            Err(_) => {
                // Timeout — child is killed on drop via kill_on_drop(true)
                Ok(ToolOutcome::Completed(ToolResult {
                    success: false,
                    result: json!({
                        "exit_code": null,
                        "stdout": "",
                        "stderr": "Execution timed out",
                        "stdout_truncated": false,
                        "stderr_truncated": false,
                        "timed_out": true,
                    })
                    .to_string(),
                    display_preference: Some("Collapsible".to_string()),
                    images: Vec::new(),
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn run(tool: &JsReplTool, args: serde_json::Value) -> ToolResult {
        let out = tool.invoke(args, ToolCtx::none("t")).await.unwrap();
        let ToolOutcome::Completed(r) = out else {
            panic!("expected Completed")
        };
        r
    }

    #[test]
    fn test_tool_name() {
        let tool = JsReplTool::new();
        assert_eq!(tool.name(), "js_repl");
    }

    /// Helper: returns true when Node.js is available in PATH.
    fn has_node() -> bool {
        find_in_path("node").is_some()
    }

    #[test]
    fn test_resolve_node_finds_system_node() {
        if !has_node() {
            return;
        }
        assert!(resolve_node().is_some());
    }

    #[tokio::test]
    async fn test_execute_simple_expression() {
        if !has_node() {
            return;
        }
        let tool = JsReplTool::new();
        let result = run(&tool, json!({ "code": "console.log(2 + 2)" })).await;

        assert!(result.success);
        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(payload["timed_out"], false);
        assert_eq!(payload["exit_code"], 0);
        assert!(payload["stdout"].as_str().unwrap().contains("4"));
    }

    /// Issue #217: `js_repl` must run in the session's tracked workspace, not
    /// silently inherit the host process's cwd — the same shared cwd
    /// resolution Bash/Glob/Grep/Workspace already use.
    #[tokio::test]
    async fn test_runs_in_session_workspace_not_process_cwd() {
        if !has_node() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().canonicalize().unwrap();
        let session = format!("session_{}", uuid::Uuid::new_v4());
        workspace_state::set_workspace(&session, workspace.clone());

        let tool = JsReplTool::new();
        let ctx = ToolCtx {
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
        };
        let out = tool
            .invoke(json!({ "code": "console.log(process.cwd())" }), ctx)
            .await
            .unwrap();
        let ToolOutcome::Completed(result) = out else {
            panic!("expected Completed")
        };
        assert!(result.success);
        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        let stdout = payload["stdout"].as_str().unwrap().trim();
        assert_eq!(PathBuf::from(stdout), workspace);
    }

    #[tokio::test]
    async fn test_execute_async_await() {
        if !has_node() {
            return;
        }
        let tool = JsReplTool::new();
        let result = run(
            &tool,
            json!({
                "code": "const result = await Promise.resolve(42); console.log(result)"
            }),
        )
        .await;

        assert!(result.success);
        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert!(payload["stdout"].as_str().unwrap().contains("42"));
    }

    #[tokio::test]
    async fn test_execute_error_returns_nonzero_exit() {
        if !has_node() {
            return;
        }
        let tool = JsReplTool::new();
        let result = run(&tool, json!({ "code": "throw new Error('test error')" })).await;

        assert!(!result.success);
        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_ne!(payload["exit_code"], 0);
        assert!(payload["stderr"].as_str().unwrap().contains("test error"));
    }

    #[tokio::test]
    async fn test_empty_code_rejected() {
        let tool = JsReplTool::new();
        let err = tool
            .invoke(json!({ "code": "  " }), ToolCtx::none("t"))
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::InvalidArguments(msg) if msg.contains("empty")));
    }

    #[tokio::test]
    async fn test_missing_code_rejected() {
        let tool = JsReplTool::new();
        let err = tool
            .invoke(json!({}), ToolCtx::none("t"))
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::InvalidArguments(_)));
    }

    #[test]
    fn test_effective_timeout() {
        assert_eq!(
            JsReplTool::effective_timeout(None),
            Duration::from_millis(30_000)
        );
        assert_eq!(
            JsReplTool::effective_timeout(Some(500_000)),
            Duration::from_millis(MAX_TIMEOUT_MS)
        );
        assert_eq!(
            JsReplTool::effective_timeout(Some(5_000)),
            Duration::from_millis(5_000)
        );
    }

    #[test]
    fn test_truncate_output() {
        let short = "hello";
        let (out, trunc) = JsReplTool::truncate_output(short);
        assert_eq!(out, "hello");
        assert!(!trunc);
    }

    #[tokio::test]
    async fn test_multiline_code() {
        if !has_node() {
            return;
        }
        let tool = JsReplTool::new();
        let result = run(
            &tool,
            json!({
                "code": "const a = 10;\nconst b = 20;\nconsole.log(a + b);"
            }),
        )
        .await;

        assert!(result.success);
        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert!(payload["stdout"].as_str().unwrap().contains("30"));
    }
}
