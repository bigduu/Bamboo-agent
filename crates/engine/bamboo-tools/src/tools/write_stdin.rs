use std::{future::Future, time::Duration};

use async_trait::async_trait;
use bamboo_agent_core::{Tool, ToolCtx, ToolError, ToolOutcome, ToolResult};
use serde::Deserialize;
use serde_json::{json, Value};

use super::exec_command::{owned_exec, owner_session, poll_result, yield_duration, EmptyPoll};

const MAX_INPUT_BYTES: usize = 64 * 1024;

tokio::task_local! {
    static DEFER_EMPTY_POLL_ADVISORY: ();
}

/// The runner will resolve the logical result through [`continue_empty_poll`].
pub async fn defer_empty_poll_advisory<F: Future>(dispatch: F) -> F::Output {
    DEFER_EMPTY_POLL_ADVISORY.scope((), dispatch).await
}

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

/// Honor a longer empty poll or continue a quiet one within the runner's
/// tool deadline. A deferred poll counts only when this call delivers a result.
pub async fn continue_empty_poll(
    owner: &str,
    tool_call_id: &str,
    arguments: &Value,
    mut previous: ToolResult,
    elapsed: Duration,
    remaining: Duration,
) -> ToolOutcome {
    if !previous.success || !previous.images.is_empty() {
        return ToolOutcome::Completed(previous);
    }
    let (Ok(args), Ok(mut result)) = (
        WriteStdinArgs::deserialize(arguments),
        serde_json::from_str::<Value>(&previous.result),
    ) else {
        return ToolOutcome::Completed(previous);
    };
    if !args.chars.is_empty()
        || args.eof
        || result["session_id"].as_str() != Some(args.session_id.as_str())
        || result["status"] != "running"
        || result["output"] != ""
        || result.get("output_omitted_bytes").is_some()
        || result.get("error").is_some()
        || result["polling"]["exit_observed"] != false
    {
        return ToolOutcome::Completed(previous);
    }
    let Some(shell) = owned_exec(owner, &args.session_id) else {
        return ToolOutcome::Completed(previous);
    };
    let state = shell.polled.as_ref().expect("validated exec session");
    let wait = match args
        .yield_time_ms
        .filter(|requested| *requested > yield_duration(Some(*requested)))
    {
        Some(requested) => remaining.min(Duration::from_millis(requested).saturating_sub(elapsed)),
        None => Duration::ZERO,
    };
    let deadline = if wait.is_zero() {
        None
    } else {
        tokio::time::Instant::now().checked_add(wait)
    };
    if let Some(deadline) = deadline {
        if let Ok(Some(_interaction)) = tokio::time::timeout_at(deadline, async {
            let guard = state.interaction.lock().await;
            // An interactive command may need the next model turn to send input.
            if shell.stdin_is_open().await {
                return None;
            }
            owned_exec(owner, &args.session_id).map(|_| guard)
        })
        .await
        {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if let Ok(wait_ms) = u64::try_from(remaining.as_millis()) {
                return poll_result(
                    &shell,
                    wait_ms,
                    args.max_output_tokens,
                    Some(EmptyPoll::Continue(tool_call_id)),
                )
                .await;
            }
        }
    }
    if let Some(advisory) = state.finish_empty_poll(tool_call_id).await {
        result["polling"]["advisory"] = json!(advisory);
        previous.result = result.to_string();
    }
    ToolOutcome::Completed(previous)
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
        "Continue the process identified by exec_command's session_id. Empty chars waits for output and exit status. Silence and exit_observed are advisory. Use terminal status and exit code to verify completion. After three empty polls, reassess the command. A sole empty poll with closed stdin can wait longer. Nonempty chars sends exact bytes to the interactive pipe, eof=true closes it, and a lone Ctrl-C (\\u0003) stops the process."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({"type":"object","properties":{
            "session_id":{"type":"string","description":"Running handle returned by exec_command"},
            "chars":{"type":"string","description":"Exact stdin bytes; empty or omitted waits/polls. A lone Ctrl-C stops the process"},
            "yield_time_ms":{"type":"integer","description":"Wait before yielding again, in milliseconds. Default 10000. An unattended empty poll can wait longer than 30000 within the runner's tool deadline."},
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
        let empty_poll = if parsed.chars.is_empty() && !parsed.eof {
            Some(if DEFER_EMPTY_POLL_ADVISORY.try_with(|_| ()).is_ok() {
                EmptyPoll::Defer(&ctx.tool_call_id)
            } else {
                EmptyPoll::Deliver
            })
        } else {
            state.reset_poll_advisory().await;
            None
        };
        let mut wait_ms = yield_duration(parsed.yield_time_ms);
        if parsed.chars == "\u{3}" {
            shell.kill().await.map_err(ToolError::Execution)?;
        } else if !parsed.chars.is_empty() {
            if let Err(error) = shell.write_stdin(&parsed.chars, false).await {
                // A failed write may race process exit or output-pump drain.
                // Wait once without consuming output, then keep polling an
                // observed exit even if its captured result is not final yet.
                shell.wait_for_polled_output(wait_ms).await;
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
        Ok(poll_result(&shell, wait_ms, parsed.max_output_tokens, empty_poll).await)
    }
}
