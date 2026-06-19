use bamboo_agent_core::AgentEvent;
use bamboo_infrastructure::process::{
    build_command_environment, decode_process_line_lossy, hide_window_for_tokio_command,
    preferred_bash_shell, trace_windows_command, CommandEnvironmentDiagnostics,
};
use dashmap::DashMap;
use regex::Regex;
use std::path::Path;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout, Duration};
use tracing::warn;

/// Per-stream line cap for a background shell's captured output, AND for the
/// foreground promotion-seed buffers (`bash.rs`). Shared so a chatty command
/// can't balloon memory before it promotes (issue #84, phase 2d).
pub(crate) const MAX_OUTPUT_LINES: usize = 20_000;
const COMPLETED_SESSION_TTL_SECS: u64 = 300;

#[derive(Debug)]
pub struct ShellSession {
    pub id: String,
    pub command: String,
    /// Bamboo session id that owns this background shell, if any. Set from the
    /// dispatch context (issue #84, phase 2a) so the registry can be queried
    /// per-session. `None` means the shell is untagged (e.g. spawned from tests).
    pub session_id: Option<String>,
    pub environment: CommandEnvironmentDiagnostics,
    child: Arc<Mutex<Child>>,
    output: Arc<Mutex<Vec<String>>>,
    base_index: Arc<Mutex<usize>>,
    running: Arc<AtomicBool>,
    exit_code: Arc<Mutex<Option<i32>>>,
}

impl ShellSession {
    pub fn status(&self) -> &'static str {
        if self.running.load(Ordering::Relaxed) {
            "running"
        } else {
            "completed"
        }
    }

    pub async fn exit_code(&self) -> Option<i32> {
        *self.exit_code.lock().await
    }

    pub async fn read_output_since(
        &self,
        cursor: usize,
        filter: Option<&Regex>,
    ) -> (Vec<String>, usize, usize) {
        let output = self.output.lock().await;
        let base_index = self.base_index.lock().await;

        let base = *base_index;
        let effective_cursor = cursor.max(base);
        let dropped_lines = effective_cursor.saturating_sub(cursor);
        let start = effective_cursor.saturating_sub(base);
        let new_lines = if start >= output.len() {
            Vec::new()
        } else {
            output[start..]
                .iter()
                .filter(|line| filter.map(|re| re.is_match(line)).unwrap_or(true))
                .cloned()
                .collect()
        };

        let next_cursor = base + output.len();
        (new_lines, next_cursor, dropped_lines)
    }

    pub async fn kill(&self) -> Result<(), String> {
        let mut child = self.child.lock().await;
        child
            .kill()
            .await
            .map_err(|e| format!("Failed to kill shell '{}': {}", self.id, e))?;
        self.running.store(false, Ordering::Relaxed);
        Ok(())
    }
}

fn sessions() -> &'static DashMap<String, Arc<ShellSession>> {
    static SESSIONS: OnceLock<DashMap<String, Arc<ShellSession>>> = OnceLock::new();
    SESSIONS.get_or_init(DashMap::new)
}

async fn push_line(output: &Arc<Mutex<Vec<String>>>, base_index: &Arc<Mutex<usize>>, line: String) {
    let mut buffer = output.lock().await;
    buffer.push(line);
    if buffer.len() > MAX_OUTPUT_LINES {
        let overflow = buffer.len() - MAX_OUTPUT_LINES;
        buffer.drain(0..overflow);
        let mut base = base_index.lock().await;
        *base += overflow;
    }
}

async fn pump_stream_lines<T>(
    stream_name: &'static str,
    reader: T,
    output: Arc<Mutex<Vec<String>>>,
    base_index: Arc<Mutex<usize>>,
) where
    T: tokio::io::AsyncRead + Unpin,
{
    let mut reader = BufReader::new(reader);
    let mut line_bytes = Vec::new();

    loop {
        line_bytes.clear();
        match reader.read_until(b'\n', &mut line_bytes).await {
            Ok(0) => break,
            Ok(_) => {
                let line = decode_process_line_lossy(&mut line_bytes);
                push_line(&output, &base_index, line).await;
            }
            Err(e) => {
                warn!("Background shell {stream_name} read failed: {e}");
                break;
            }
        }
    }
}

pub async fn spawn_background(
    command: &str,
    cwd: Option<&Path>,
    event_tx: Option<mpsc::Sender<AgentEvent>>,
    session_id: Option<String>,
) -> Result<Arc<ShellSession>, String> {
    let shell = preferred_bash_shell();
    trace_windows_command(
        "agent.bash.background",
        &shell.program,
        [shell.arg, command],
    );
    let overrides = bamboo_llm::Config::current_env_vars();
    let prepared_env = build_command_environment(&overrides).await;
    let mut cmd = Command::new(&shell.program);
    hide_window_for_tokio_command(&mut cmd);
    if let Some(cwd) = cwd {
        cmd.current_dir(cwd);
    }
    prepared_env.apply_to_tokio_command(&mut cmd);
    cmd.arg(shell.arg)
        .arg(command)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let mut child = cmd
        .spawn()
        .map_err(|e| format!("Failed to spawn background shell: {}", e))?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "Failed to capture shell stdout".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "Failed to capture shell stderr".to_string())?;

    let shell_id = uuid::Uuid::new_v4().to_string();
    let output = Arc::new(Mutex::new(Vec::new()));
    let base_index = Arc::new(Mutex::new(0usize));
    let running = Arc::new(AtomicBool::new(true));
    let exit_code = Arc::new(Mutex::new(None));

    let session = Arc::new(ShellSession {
        id: shell_id.clone(),
        command: command.to_string(),
        session_id,
        environment: prepared_env.diagnostics.clone(),
        child: Arc::new(Mutex::new(child)),
        output: output.clone(),
        base_index: base_index.clone(),
        running: running.clone(),
        exit_code: exit_code.clone(),
    });

    {
        let output = output.clone();
        let base_index = base_index.clone();
        tokio::spawn(async move {
            pump_stream_lines("stdout", stdout, output, base_index).await;
        });
    }

    {
        let output = output.clone();
        let base_index = base_index.clone();
        tokio::spawn(async move {
            pump_stream_lines("stderr", stderr, output, base_index).await;
        });
    }

    spawn_completion_poll(
        session.child.clone(),
        shell_id.clone(),
        command.to_string(),
        running,
        exit_code,
        event_tx,
    );

    sessions().insert(shell_id, session.clone());
    Ok(session)
}

/// Shared completion-poll task. Polls the child until it exits, then sets the
/// exit code/running flags, emits a `BashCompleted` event (when a sender is
/// wired), and GCs the shell from the registry after the TTL. Used by both
/// [`spawn_background`] and [`adopt_running_child`] so the poll/emit logic is
/// never duplicated (issue #84, phase 2d).
fn spawn_completion_poll(
    child: Arc<Mutex<Child>>,
    shell_id: String,
    command: String,
    running: Arc<AtomicBool>,
    exit_code: Arc<Mutex<Option<i32>>>,
    event_tx: Option<mpsc::Sender<AgentEvent>>,
) {
    let session_id_for_gc = shell_id.clone();
    let bash_id_for_event = shell_id;
    let command_for_event = command;
    tokio::spawn(async move {
        let (status_str, exit_code_value) = loop {
            let poll = {
                let mut guard = child.lock().await;
                guard.try_wait()
            };
            match poll {
                Ok(Some(status)) => {
                    let code = status.code();
                    *exit_code.lock().await = code;
                    running.store(false, Ordering::Relaxed);
                    break (
                        if code.is_none() {
                            "killed"
                        } else {
                            "completed"
                        },
                        code,
                    );
                }
                Ok(None) => {
                    sleep(Duration::from_millis(100)).await;
                }
                Err(_) => {
                    running.store(false, Ordering::Relaxed);
                    break ("error", None);
                }
            }
        };

        // Phase 1 (issue #84): emit a completion signal so clients can react
        // to a long-running background command finishing. This is the ONLY
        // chance to deliver the signal — the poll task emits exactly once,
        // then sleeps the GC TTL and removes the shell. A non-blocking
        // `try_send` would silently drop it under a saturated event channel,
        // so we bound the await instead (500ms) and fall back to a visible
        // `warn!` if the channel stays full or closed.
        if let Some(tx) = &event_tx {
            let event = AgentEvent::BashCompleted {
                bash_id: bash_id_for_event,
                command: command_for_event,
                exit_code: exit_code_value,
                status: status_str.to_string(),
            };
            if timeout(Duration::from_millis(500), tx.send(event))
                .await
                .is_err()
            {
                warn!(
                    bash_id = %session_id_for_gc,
                    "BashCompleted signal dropped (event channel saturated or closed after 500ms)"
                );
            }
        }

        sleep(Duration::from_secs(COMPLETED_SESSION_TTL_SECS)).await;
        let _ = remove_shell(&session_id_for_gc);
    });
}

/// Adopt a child process that was spawned and partially drained by the
/// foreground streaming loop (auto-sync promotion, issue #84 phase 2d).
///
/// Builds a [`ShellSession`] seeded with the already-captured output lines so
/// they survive the hand-off and appear in subsequent `read_output_since`
/// calls, then spawns the same pump + completion-poll tasks as
/// [`spawn_background`] to keep draining the handed-over readers and eventually
/// emit `BashCompleted`. The poll/emit logic is shared via
/// [`spawn_completion_poll`] — it is never duplicated between the two entry
/// points.
#[allow(clippy::too_many_arguments)]
pub async fn adopt_running_child(
    child: Child,
    stdout_reader: impl tokio::io::AsyncRead + Unpin + Send + 'static,
    stderr_reader: impl tokio::io::AsyncRead + Unpin + Send + 'static,
    seeded_stdout_lines: Vec<String>,
    seeded_stderr_lines: Vec<String>,
    command: &str,
    session_id: Option<String>,
    environment: CommandEnvironmentDiagnostics,
    event_tx: Option<mpsc::Sender<AgentEvent>>,
) -> Result<Arc<ShellSession>, String> {
    let shell_id = uuid::Uuid::new_v4().to_string();
    let output = Arc::new(Mutex::new(Vec::new()));
    let base_index = Arc::new(Mutex::new(0usize));
    let running = Arc::new(AtomicBool::new(true));
    let exit_code = Arc::new(Mutex::new(None));

    // Seed the output buffer with already-captured lines so they are not lost
    // across the foreground→background hand-off. Lines captured by the
    // foreground phase are pushed here; the pump tasks below will append any
    // subsequent output produced after promotion.
    for line in seeded_stdout_lines.iter().chain(seeded_stderr_lines.iter()) {
        push_line(&output, &base_index, line.clone()).await;
    }

    let session = Arc::new(ShellSession {
        id: shell_id.clone(),
        command: command.to_string(),
        session_id,
        environment,
        child: Arc::new(Mutex::new(child)),
        output: output.clone(),
        base_index: base_index.clone(),
        running: running.clone(),
        exit_code: exit_code.clone(),
    });

    // Spawn pump tasks to continue draining the handed-over readers. The
    // readers may still hold buffered data from the foreground phase — wrapping
    // them in a new BufReader (as pump_stream_lines does) reads through that
    // buffer first, so no data is lost or double-counted.
    {
        let output = output.clone();
        let base_index = base_index.clone();
        tokio::spawn(async move {
            pump_stream_lines("stdout", stdout_reader, output, base_index).await;
        });
    }
    {
        let output = output.clone();
        let base_index = base_index.clone();
        tokio::spawn(async move {
            pump_stream_lines("stderr", stderr_reader, output, base_index).await;
        });
    }

    spawn_completion_poll(
        session.child.clone(),
        shell_id.clone(),
        command.to_string(),
        running,
        exit_code,
        event_tx,
    );

    sessions().insert(shell_id, session.clone());
    Ok(session)
}

pub fn get_shell(id: &str) -> Option<Arc<ShellSession>> {
    sessions().get(id).map(|entry| entry.value().clone())
}

pub fn remove_shell(id: &str) -> Option<Arc<ShellSession>> {
    sessions().remove(id).map(|(_, value)| value)
}

/// Returns the ids of background shells owned by `session_id` that are still
/// running (issue #84, phase 2a). Mirrors the sync `get_shell`/`remove_shell`
/// helpers over the global registry — not async because the registry is a sync
/// `DashMap` and `status()` is a sync read. A shell is included only when its
/// stored `session_id` equals `Some(session_id)` and `status()` is `"running"`,
/// so completed shells and shells belonging to another session (or none) are
/// excluded.
///
/// The result is a point-in-time snapshot: a returned shell may finish between
/// this call and the caller acting on its id, so callers must re-check liveness
/// (e.g. via `get_shell(id).status()`) before treating an id as still running.
pub fn running_shells_for_session(session_id: &str) -> Vec<String> {
    sessions()
        .iter()
        .filter(|entry| {
            entry
                .session_id
                .as_deref()
                .is_some_and(|sid| sid == session_id)
                && entry.status() == "running"
        })
        .map(|entry| entry.id.clone())
        .collect()
}
