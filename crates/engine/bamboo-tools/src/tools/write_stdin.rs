use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use bamboo_agent_core::{Tool, ToolCtx, ToolError, ToolOutcome, ToolResult};
use serde::Deserialize;
use serde_json::{json, Value};

use super::{
    bash_runtime,
    exec_command::{owner_session, poll_result, yield_duration},
};

const MAX_INPUT_BYTES: usize = 64 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteStdinArgs {
    session_id: String,
    #[serde(default)]
    chars: String,
    yield_time_ms: Option<u64>,
    max_output_tokens: Option<usize>,
    #[serde(default)]
    eof: bool,
}

fn owned_exec(owner: &str, id: &str) -> Option<Arc<bash_runtime::ShellSession>> {
    bash_runtime::get_shell(id)
        .filter(|shell| shell.polled.is_some() && shell.session_id.as_deref() == Some(owner))
}

/// Continue a successful empty read of a quiet command within the caller's
/// existing tool deadline. The foreground runner owns cancellation of the wait.
pub async fn continue_quiet_poll(
    owner: &str,
    arguments: &Value,
    previous: &ToolResult,
    remaining: Duration,
) -> Option<ToolOutcome> {
    if remaining.is_zero() || !previous.success || !previous.images.is_empty() {
        return None;
    }
    let deadline = tokio::time::Instant::now().checked_add(remaining)?;
    let args = WriteStdinArgs::deserialize(arguments).ok()?;
    let result: Value = serde_json::from_str(&previous.result).ok()?;
    if !args.chars.is_empty()
        || args.eof
        || result["session_id"].as_str() != Some(args.session_id.as_str())
        || result["status"] != "running"
        || result["output"] != ""
        || result.get("output_omitted_bytes").is_some()
        || result.get("error").is_some()
        || result["polling"]["state"] != "needs_diagnosis"
        || result["polling"]["exit_observed"] != false
    {
        return None;
    }
    let shell = owned_exec(owner, &args.session_id)?;
    let state = shell.polled.as_ref().expect("validated exec session");
    let _interaction = tokio::time::timeout_at(deadline, async {
        let guard = state.interaction.lock().await;
        // An interactive command may need the next model turn to send input.
        if shell.stdin_is_open().await {
            return None;
        }
        owned_exec(owner, &args.session_id).map(|_| guard)
    })
    .await
    .ok()??;
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    let wait_ms = u64::try_from(remaining.as_millis()).ok()?;
    Some(poll_result(&shell, wait_ms, args.max_output_tokens).await)
}

#[derive(Default)]
pub struct WriteStdinTool;

impl WriteStdinTool {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Tool for WriteStdinTool {
    fn name(&self) -> &str {
        "write_stdin"
    }

    fn description(&self) -> &str {
        "Continue an exec_command session owned by this conversation. Empty chars waits/polls for incremental output and final exit status. Running results include polling elapsed/idle times, empty-readout count and exit_observed (false does not prove liveness). Follow a polling advisory by checking progress or doing useful work before polling again; silence does not prove completion. Nonempty chars writes exact bytes to an interactive stdin pipe; use eof=true to close it. A lone Ctrl-C (\\u0003) stops the process. Finished results omit session_id; nonzero exit codes are command outcomes."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({"type":"object","properties":{
            "session_id":{"type":"string","description":"Running handle returned by exec_command"},
            "chars":{"type":"string","description":"Exact stdin bytes; empty or omitted waits/polls. A lone Ctrl-C stops the process"},
            "yield_time_ms":{"type":"integer","description":"Wait before yielding again; default 10000, clamped to 250..30000 ms"},
            "max_output_tokens":{"type":"integer","description":"Output string budget, default 2000, capped at 8192; four JSON-encoded bytes per token including quotes and escapes. Status/session/exit metadata is additional"},
            "eof":{"type":"boolean","description":"Close the stdin pipe after sending chars"}
        },"required":["session_id"],"additionalProperties":false})
    }

    async fn invoke(
        &self,
        args: serde_json::Value,
        ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        let parsed: WriteStdinArgs = serde_json::from_value(args).map_err(|error| {
            ToolError::InvalidArguments(format!("Invalid write_stdin args: {error}"))
        })?;
        let owner = owner_session(&ctx)?;
        let shell = owned_exec(owner, &parsed.session_id)
            .ok_or_else(|| {
                ToolError::Execution("Exec session not found or not owned by this session (unread completed results expire after five minutes; handles do not survive Host restart)".into())
            })?;
        if parsed.chars.len() > MAX_INPUT_BYTES {
            return Err(ToolError::InvalidArguments(
                "chars exceeds 65536 bytes".into(),
            ));
        }
        let state = shell.polled.as_ref().expect("validated exec session");
        // Serialize writes and the implicit output cursor, including callers
        // that already obtained an Arc while another poll was completing.
        let _interaction = state.interaction.lock().await;
        let mut wait_ms = yield_duration(parsed.yield_time_ms);
        if parsed.chars == "\u{3}" {
            shell.kill().await.map_err(ToolError::Execution)?;
        } else if !parsed.chars.is_empty() {
            if let Err(error) = shell.write_stdin(&parsed.chars, false).await {
                // A failed write may race process exit or output-pump drain.
                // Wait once without consuming output, then keep polling an
                // observed exit even if its captured result is not final yet.
                state.wait(wait_ms).await;
                if !state.is_complete().await && !state.process_exited() {
                    return Err(ToolError::Execution(error));
                }
                wait_ms = 0;
            } else {
                state.reset_empty_readouts().await;
            }
        }
        if parsed.eof {
            shell.close_stdin().await;
            state.reset_empty_readouts().await;
        }
        Ok(poll_result(&shell, wait_ms, parsed.max_output_tokens).await)
    }
}
