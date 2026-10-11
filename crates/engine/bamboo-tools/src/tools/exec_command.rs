//! Yielding shell execution whose results are pulled through ordinary tools.
use async_trait::async_trait;
use bamboo_agent_core::{Tool, ToolCtx, ToolError, ToolOutcome, ToolResult};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::{watch, Mutex, Notify, OwnedSemaphorePermit, Semaphore};
use tokio::time::{timeout, Duration};

use super::{bash_runtime, workspace_state};

const MAX_RETAINED_SESSIONS: usize = 64;
const MAX_CAPTURE_BYTES: usize = 256 * 1024;
const MAX_OUTPUT_TOKENS: usize = 8192;
const DEFAULT_OUTPUT_TOKENS: usize = 2000;
const MAX_COMMAND_BYTES: usize = 64 * 1024;

#[derive(Debug, Default)]
struct CapturedOutput {
    bytes: Vec<u8>,
    omitted_bytes: usize,
    completion: Option<(&'static str, Option<i32>)>,
    error: Option<String>,
}

/// Per-handle pull cursor and bounded capture. Process ownership remains in
/// `ManagedBashChild`; this state can outlive a drained local resource scope.
#[derive(Debug)]
pub(crate) struct PolledShellState {
    output: Mutex<CapturedOutput>,
    completed: Notify,
    // A reaped parent may still have descendants holding its output pipes.
    // This observation does not publish a terminal capture result.
    process_exited: AtomicBool,
    capture_stop: watch::Sender<bool>,
    pub(crate) interaction: Mutex<()>,
    _permit: OwnedSemaphorePermit,
}

impl PolledShellState {
    fn new() -> Result<Arc<Self>, ToolError> {
        static CAPACITY: OnceLock<Arc<Semaphore>> = OnceLock::new();
        let permit = CAPACITY
            .get_or_init(|| Arc::new(Semaphore::new(MAX_RETAINED_SESSIONS)))
            .clone()
            .try_acquire_owned()
            .map_err(|_| ToolError::Execution("Exec session capacity exhausted; finish or stop an existing session before starting another command".into()))?;
        Ok(Arc::new(Self {
            output: Mutex::new(CapturedOutput::default()),
            completed: Notify::new(),
            process_exited: AtomicBool::new(false),
            capture_stop: watch::channel(false).0,
            interaction: Mutex::new(()),
            _permit: permit,
        }))
    }

    async fn push(&self, bytes: &[u8]) {
        let mut output = self.output.lock().await;
        output.bytes.extend_from_slice(bytes);
        let mut excess = output.bytes.len().saturating_sub(MAX_CAPTURE_BYTES);
        if excess > 0 {
            while excess < output.bytes.len() && output.bytes[excess] & 0b1100_0000 == 0b1000_0000 {
                excess += 1;
            }
            output.bytes.drain(..excess);
            output.omitted_bytes = output.omitted_bytes.saturating_add(excess);
        }
    }

    pub(crate) async fn record_error(&self, error: String) {
        self.output.lock().await.error.get_or_insert(error);
    }

    pub(crate) fn stop_capture(&self) {
        self.capture_stop.send_replace(true);
    }

    pub(crate) async fn finish(&self, status: &'static str, exit_code: Option<i32>) {
        let mut output = self.output.lock().await;
        let status = if output.error.is_some() {
            "error"
        } else {
            status
        };
        output.completion = Some((status, exit_code));
        drop(output);
        self.completed.notify_waiters();
    }

    pub(super) fn mark_process_exited(&self) {
        self.process_exited.store(true, Ordering::Release);
    }

    pub(super) fn process_exited(&self) -> bool {
        self.process_exited.load(Ordering::Acquire)
    }

    pub(super) async fn wait(&self, yield_time_ms: u64) {
        let notified = self.completed.notified();
        tokio::pin!(notified);
        // Register before observing completion: even an immediate child exit
        // cannot slip between the state check and the notification wait.
        notified.as_mut().enable();
        let running = self.output.lock().await.completion.is_none();
        if running {
            let _ = timeout(Duration::from_millis(yield_time_ms), notified).await;
        }
    }

    pub(crate) async fn is_complete(&self) -> bool {
        self.output.lock().await.completion.is_some()
    }

    async fn take(&self, shell_id: &str, max_output_tokens: Option<usize>) -> ExecSessionResult {
        let mut output = self.output.lock().await;
        let bytes = std::mem::take(&mut output.bytes);
        let decoded = String::from_utf8_lossy(&bytes);
        let budget = max_output_tokens
            .unwrap_or(DEFAULT_OUTPUT_TOKENS)
            .clamp(1, MAX_OUTPUT_TOKENS)
            .saturating_mul(4);
        let end = json_output_prefix_end(&decoded, budget);
        let omitted = std::mem::take(&mut output.omitted_bytes)
            .saturating_add(decoded.len().saturating_sub(end));
        let (status, exit_code) = output.completion.unwrap_or(("running", None));
        ExecSessionResult {
            session_id: output.completion.is_none().then(|| shell_id.to_owned()),
            status,
            output: decoded[..end].to_owned(),
            exit_code,
            output_omitted_bytes: omitted,
            error: output.error.clone(),
        }
    }
}

/// Bound the serialized `output` string, including its JSON quotes and escapes.
/// Protocol fields such as status and session_id remain outside this body budget.
fn json_output_prefix_end(output: &str, budget: usize) -> usize {
    let mut encoded_bytes = 2; // The surrounding JSON string quotes.
    let mut end = 0;
    for (offset, character) in output.char_indices() {
        // Match serde_json's string escaping without allocating an encoded copy.
        let character_bytes = match character {
            '"' | '\\' | '\n' | '\r' | '\t' | '\u{8}' | '\u{c}' => 2,
            '\u{0}'..='\u{1f}' => 6,
            _ => character.len_utf8(),
        };
        if encoded_bytes + character_bytes > budget {
            break;
        }
        encoded_bytes += character_bytes;
        end = offset + character.len_utf8();
    }
    end
}

/// Fixed-size reads capture prompts without a newline and keep an unterminated
/// line from growing a BufReader allocation without a bound.
pub(crate) async fn pump_polled_stream(
    mut reader: impl AsyncRead + Unpin,
    state: Arc<PolledShellState>,
) {
    let mut buffer = [0; 8192];
    let mut pending = Vec::new();
    let mut stop = state.capture_stop.subscribe();
    loop {
        let stopped = *stop.borrow();
        if stopped {
            if !pending.is_empty() {
                state
                    .push(String::from_utf8_lossy(&pending).as_bytes())
                    .await;
            }
            break;
        }
        let read = tokio::select! {
            biased;
            _ = stop.changed() => continue,
            result = reader.read(&mut buffer) => result,
        };
        match read {
            Ok(0) => {
                if !pending.is_empty() {
                    state
                        .push(String::from_utf8_lossy(&pending).as_bytes())
                        .await;
                }
                break;
            }
            Ok(count) => {
                pending.extend_from_slice(&buffer[..count]);
                let mut consumed = 0;
                let mut decoded = String::new();
                while consumed < pending.len() {
                    match std::str::from_utf8(&pending[consumed..]) {
                        Ok(text) => {
                            decoded.push_str(text);
                            consumed = pending.len();
                        }
                        Err(error) => {
                            let valid_end = consumed + error.valid_up_to();
                            decoded.push_str(
                                std::str::from_utf8(&pending[consumed..valid_end]).unwrap(),
                            );
                            consumed = valid_end;
                            if let Some(invalid_len) = error.error_len() {
                                decoded.push('\u{fffd}');
                                consumed += invalid_len;
                            } else {
                                break;
                            }
                        }
                    }
                }
                pending.drain(..consumed);
                state.push(decoded.as_bytes()).await;
            }
            Err(error) => {
                state
                    .record_error(format!("Shell output read failed: {error}"))
                    .await;
                break;
            }
        }
    }
}

#[derive(Debug, Serialize)]
struct ExecSessionResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id: Option<String>,
    status: &'static str,
    output: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    exit_code: Option<i32>,
    #[serde(skip_serializing_if = "is_zero")]
    output_omitted_bytes: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

fn is_zero(value: &usize) -> bool {
    *value == 0
}

pub(crate) fn yield_duration(requested: Option<u64>) -> u64 {
    requested.unwrap_or(10_000).clamp(250, 30_000)
}

pub(crate) fn owner_session(ctx: &ToolCtx) -> Result<&str, ToolError> {
    ctx.session_id().filter(|id| !id.is_empty()).ok_or_else(|| {
        ToolError::InvalidArguments("Exec sessions require an owning session".into())
    })
}

pub(crate) async fn poll_result(
    shell: &bash_runtime::ShellSession,
    yield_time_ms: u64,
    max_output_tokens: Option<usize>,
) -> ToolOutcome {
    let state = shell.polled.as_ref().expect("validated exec session");
    state.wait(yield_time_ms).await;
    let result = state.take(&shell.id, max_output_tokens).await;
    if result.session_id.is_none() {
        bash_runtime::remove_shell(&shell.id);
    }
    ToolOutcome::Completed(ToolResult {
        // A command's nonzero exit is an observed outcome, not tool transport
        // failure. Models inspect exit_code/status to decide their next step.
        success: true,
        result: serde_json::to_string(&result).expect("exec result is serializable"),
        display_preference: Some("Collapsible".into()),
        images: Vec::new(),
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecCommandArgs {
    cmd: String,
    workdir: Option<String>,
    yield_time_ms: Option<u64>,
    max_output_tokens: Option<usize>,
    #[serde(default)]
    interactive: bool,
}

fn resolve_cwd(owner: &str, workdir: Option<&str>) -> Result<PathBuf, ToolError> {
    let workspace = workspace_state::workspace_or_process_cwd(Some(owner));
    let workspace = workspace.canonicalize().map_err(|error| {
        ToolError::InvalidArguments(format!(
            "Invalid session workspace '{}': {error}",
            workspace.display()
        ))
    })?;
    let requested = match workdir {
        Some(value) if value.trim().is_empty() => {
            return Err(ToolError::InvalidArguments(
                "'workdir' cannot be empty".into(),
            ));
        }
        Some(value) => Path::new(value.trim()).to_path_buf(),
        None => workspace.clone(),
    };
    let resolved = if requested.is_absolute() {
        requested
    } else {
        workspace.join(requested)
    };
    let canonical = resolved.canonicalize().map_err(|error| {
        ToolError::InvalidArguments(format!("Invalid workdir '{}': {error}", resolved.display()))
    })?;
    if !canonical.is_dir() {
        return Err(ToolError::InvalidArguments(
            "workdir must be a directory".into(),
        ));
    }
    if !canonical.starts_with(&workspace) {
        return Err(ToolError::InvalidArguments(
            "workdir must stay within the current session workspace".into(),
        ));
    }
    Ok(canonical)
}

#[derive(Default)]
pub struct ExecCommandTool;

impl ExecCommandTool {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Tool for ExecCommandTool {
    fn name(&self) -> &str {
        "exec_command"
    }

    fn description(&self) -> &str {
        "Execute a shell command and wait up to yield_time_ms. A running command returns a session_id; continue other work, then call write_stdin with that ID and empty chars to retrieve incremental output and its final exit code. Completion is delivered through these tool results. interactive=true keeps a stdin pipe open for later input; this is a pipe, not a terminal/PTY. Output is bounded and reports omitted bytes."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({"type":"object","properties":{
            "cmd":{"type":"string","description":"Shell command to execute"},
            "workdir":{"type":"string","description":"Working directory within the current session workspace, relative or absolute; paths and symlinks outside the workspace are rejected"},
            "yield_time_ms":{"type":"integer","description":"Wait before yielding a running session; default 10000, clamped to 250..30000 ms"},
            "max_output_tokens":{"type":"integer","description":"Output string budget, default 2000 and capped at 8192; estimated as four JSON-encoded bytes per token, including string quotes and escapes. Status/session/exit metadata is additional"},
            "interactive":{"type":"boolean","description":"Keep a piped stdin open for write_stdin; default false sends immediate EOF"}
        },"required":["cmd"],"additionalProperties":false})
    }

    async fn invoke(
        &self,
        args: serde_json::Value,
        ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        let parsed: ExecCommandArgs = serde_json::from_value(args).map_err(|error| {
            ToolError::InvalidArguments(format!("Invalid exec_command args: {error}"))
        })?;
        let owner = owner_session(&ctx)?;
        if parsed.cmd.trim().is_empty() || parsed.cmd.len() > MAX_COMMAND_BYTES {
            return Err(ToolError::InvalidArguments(
                "cmd must be nonempty and at most 65536 bytes".into(),
            ));
        }
        let cwd = resolve_cwd(owner, parsed.workdir.as_deref())?;
        let state = PolledShellState::new()?;
        let shell = bash_runtime::spawn_polled(
            &parsed.cmd,
            &cwd,
            None,
            owner.to_owned(),
            parsed.interactive,
            state,
        )
        .await
        .map_err(ToolError::Execution)?;
        Ok(poll_result(
            &shell,
            yield_duration(parsed.yield_time_ms),
            parsed.max_output_tokens,
        )
        .await)
    }
}

#[cfg(test)]
#[path = "exec_sessions_tests.rs"]
mod tests;
