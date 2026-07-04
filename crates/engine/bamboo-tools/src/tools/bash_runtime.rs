use bamboo_agent_core::{AgentEvent, BashCompletionInfo, BashCompletionSink};
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
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::time::{sleep, timeout, Duration};
use tracing::warn;

/// Per-stream line cap for a background shell's captured output, AND for the
/// foreground promotion-seed buffers (`bash.rs`). Shared so a chatty command
/// can't balloon memory before it promotes (issue #84, phase 2d).
pub(crate) const MAX_OUTPUT_LINES: usize = 20_000;
const COMPLETED_SESSION_TTL_SECS: u64 = 300;
/// Trailing captured lines carried on a background-Bash completion push so the
/// model sees the result without a mandatory `BashOutput` round-trip (issue #84
/// Phase 2b follow-up). The full output is still available via `BashOutput`.
const COMPLETION_TAIL_LINES: usize = 50;
/// Byte ceiling on that tail; the most recent bytes are kept (front-trimmed on a
/// char boundary) so a chatty final line can't bloat the injected message.
const COMPLETION_TAIL_MAX_BYTES: usize = 4096;
/// Upper bound on a single `write_stdin` so a wedged consumer (full pipe
/// buffer, child not draining) cannot pin the stdin mutex — and thus block any
/// queued writer — indefinitely. A timeout surfaces a clear error instead.
const STDIN_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub struct ShellSession {
    pub id: String,
    pub command: String,
    /// Bamboo session id that owns this background shell, if any. Set from the
    /// dispatch context (issue #84, phase 2a) so the registry can be queried
    /// per-session. `None` means the shell is untagged (e.g. spawned from tests).
    pub session_id: Option<String>,
    pub environment: CommandEnvironmentDiagnostics,
    /// Kill request for the still-running child. The child handle itself is owned
    /// by the completion task, which awaits its exit via `child.wait()` (truly
    /// event-driven — no polling). Because that task holds the handle across the
    /// await, a kill cannot lock the handle without deadlocking; instead `kill()`
    /// fires this `Notify` and the completion task's `select!` reaps the child.
    kill_notify: Arc<Notify>,
    /// Retained stdin handle for an interactive shell (issue #89). `Some` only
    /// when the shell was spawned with `interactive: true` (a piped stdin);
    /// `None` for every non-interactive shell so the default EOF-on-read
    /// behavior is byte-for-byte unchanged. Guarded by a Mutex so `write_stdin`
    /// can borrow it without racing the completion poll.
    stdin: Arc<Mutex<Option<ChildStdin>>>,
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

    /// Request termination of the background shell. Flips `running` optimistically
    /// so [`Self::status`] reflects the kill immediately, then signals the
    /// completion task (the sole owner of the child handle) to `start_kill` and
    /// reap. The real exit code + the completion event/push are recorded by that
    /// task when the process is reaped. Never locks the child handle — see
    /// [`ShellSession::kill_notify`].
    #[allow(clippy::unused_async)] // async kept: callers `.await` it; symmetry with the other handle ops.
    pub async fn kill(&self) -> Result<(), String> {
        self.running.store(false, Ordering::Relaxed);
        self.kill_notify.notify_one();
        Ok(())
    }

    /// Write `data` to the shell's retained stdin pipe (issue #89). When
    /// `append_newline` is true a trailing `\n` is appended so the input is
    /// delivered as a complete line (e.g. to satisfy a line-oriented prompt).
    ///
    /// Returns a clear error — never panics — when the shell was NOT spawned
    /// interactive (no stdin pipe to write to) or when the write/flush fails
    /// (the process has exited and the pipe is closed). Callers must not treat
    /// either case as fatal: a non-interactive shell simply has no stdin, and an
    /// exited shell's pipe is gone.
    pub async fn write_stdin(&self, data: &str, append_newline: bool) -> Result<(), String> {
        let mut guard = self.stdin.lock().await;
        let stdin = guard.as_mut().ok_or_else(|| {
            format!(
                "Shell '{}' has no interactive stdin pipe; spawn it via Bash with interactive=true",
                self.id
            )
        })?;
        let mut bytes = data.as_bytes().to_vec();
        if append_newline {
            bytes.push(b'\n');
        }
        // Bound the write+flush so a consumer that has stopped reading (full
        // pipe buffer) cannot hold the stdin mutex — and thus block any queued
        // writer — forever. A timeout surfaces a clear error instead of a hang.
        timeout(STDIN_WRITE_TIMEOUT, stdin.write_all(&bytes))
            .await
            .map_err(|_| {
                format!(
                    "Timed out after {}s writing to stdin of shell '{}' (consumer not draining)",
                    STDIN_WRITE_TIMEOUT.as_secs(),
                    self.id
                )
            })?
            .map_err(|e| format!("Failed to write to stdin of shell '{}': {}", self.id, e))?;
        timeout(STDIN_WRITE_TIMEOUT, stdin.flush())
            .await
            .map_err(|_| {
                format!(
                    "Timed out after {}s flushing stdin of shell '{}'",
                    STDIN_WRITE_TIMEOUT.as_secs(),
                    self.id
                )
            })?
            .map_err(|e| format!("Failed to flush stdin of shell '{}': {}", self.id, e))?;
        Ok(())
    }

    /// Close the retained stdin pipe (issue #89), sending end-of-file to the
    /// child. Dropping the `ChildStdin` handle closes the pipe's write end, so
    /// a consumer that reads stdin until EOF (e.g. `cat`, `sort`, a REPL) can
    /// terminate normally instead of running until killed.
    ///
    /// Idempotent: a no-op on a non-interactive shell (stdin was already
    /// `None`) or one whose stdin was already closed. Returns `true` when a
    /// handle was actually taken (i.e. this was an interactive shell whose
    /// stdin was still open), `false` otherwise, so callers can distinguish.
    pub async fn close_stdin(&self) -> bool {
        self.stdin.lock().await.take().is_some()
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

#[allow(clippy::too_many_arguments)]
pub async fn spawn_background(
    command: &str,
    cwd: Option<&Path>,
    event_tx: Option<mpsc::Sender<AgentEvent>>,
    session_id: Option<String>,
    interactive: bool,
    bash_completion_sink: Option<Arc<dyn BashCompletionSink>>,
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
    cmd.arg(shell.arg).arg(command);
    // Interactive (issue #89): a piped stdin lets callers feed input over time
    // via `write_stdin`/BashInput. Non-interactive keeps `Stdio::null()` so a
    // command that reads stdin gets immediate EOF — the default behavior is
    // byte-for-byte unchanged for every existing path.
    if interactive {
        cmd.stdin(Stdio::piped());
    } else {
        cmd.stdin(Stdio::null());
    }
    cmd.stdout(Stdio::piped())
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
    // Only take (and store) the stdin handle when spawned interactive; a
    // non-interactive child never has a stdin pipe to take.
    let stdin_handle = if interactive {
        child.stdin.take()
    } else {
        None
    };

    let shell_id = uuid::Uuid::new_v4().to_string();
    let output = Arc::new(Mutex::new(Vec::new()));
    let base_index = Arc::new(Mutex::new(0usize));
    let running = Arc::new(AtomicBool::new(true));
    let exit_code = Arc::new(Mutex::new(None));
    let kill_notify = Arc::new(Notify::new());

    let session = Arc::new(ShellSession {
        id: shell_id.clone(),
        command: command.to_string(),
        session_id,
        environment: prepared_env.diagnostics.clone(),
        kill_notify: kill_notify.clone(),
        stdin: Arc::new(Mutex::new(stdin_handle)),
        output: output.clone(),
        base_index: base_index.clone(),
        running: running.clone(),
        exit_code: exit_code.clone(),
    });

    let stdout_pump = {
        let output = output.clone();
        let base_index = base_index.clone();
        tokio::spawn(async move {
            pump_stream_lines("stdout", stdout, output, base_index).await;
        })
    };

    let stderr_pump = {
        let output = output.clone();
        let base_index = base_index.clone();
        tokio::spawn(async move {
            pump_stream_lines("stderr", stderr, output, base_index).await;
        })
    };

    spawn_completion_poll(
        child,
        kill_notify,
        shell_id.clone(),
        command.to_string(),
        running,
        exit_code,
        output.clone(),
        session.session_id.clone(),
        event_tx,
        bash_completion_sink,
        vec![stdout_pump, stderr_pump],
    );

    sessions().insert(shell_id, session.clone());
    Ok(session)
}

/// Shared completion-poll task. Polls the child until it exits, then sets the
/// exit code/running flags, emits a `BashCompleted` event (when a sender is
/// wired), pushes a loop-facing completion into the owning session via
/// `bash_completion_sink` (when wired and the shell is session-tagged), and GCs
/// the shell from the registry after the TTL. Used by both [`spawn_background`]
/// and [`adopt_running_child`] so the poll/emit logic is never duplicated
/// (issue #84, phase 2d + Phase 2b follow-up).
#[allow(clippy::too_many_arguments)]
fn spawn_completion_poll(
    mut child: Child,
    kill_notify: Arc<Notify>,
    shell_id: String,
    command: String,
    running: Arc<AtomicBool>,
    exit_code: Arc<Mutex<Option<i32>>>,
    output: Arc<Mutex<Vec<String>>>,
    session_id: Option<String>,
    event_tx: Option<mpsc::Sender<AgentEvent>>,
    bash_completion_sink: Option<Arc<dyn BashCompletionSink>>,
    // Stdout/stderr pump task handles. Awaited (bounded) before reading the
    // completion tail so it reflects the fully-drained output — observing exit
    // can race the pumps that are still flushing the final lines.
    pump_handles: Vec<tokio::task::JoinHandle<()>>,
) {
    let session_id_for_gc = shell_id.clone();
    let bash_id_for_event = shell_id.clone();
    let bash_id_for_sink = shell_id;
    let command_for_event = command.clone();
    let command_for_sink = command;
    tokio::spawn(async move {
        // Event-driven exit detection: await the process directly (`child.wait()`
        // is OS-notified via tokio's process driver — no polling). A kill request
        // (`kill_notify`, fired by `ShellSession::kill`) races the natural exit via
        // `select!`; whichever wins, we then reap and read the exit status. This
        // task owns the child handle, so no lock is contended and no killer blocks.
        let wait_result = tokio::select! {
            result = child.wait() => result,
            _ = kill_notify.notified() => {
                let _ = child.start_kill();
                child.wait().await
            }
        };
        let (status_str, exit_code_value) = match wait_result {
            Ok(status) => {
                let code = status.code();
                // `code.is_none()` ⇒ terminated by signal (our SIGKILL, or an
                // external one) ⇒ "killed"; otherwise a normal exit ⇒ "completed".
                (if code.is_none() { "killed" } else { "completed" }, code)
            }
            Err(_) => ("error", None),
        };
        *exit_code.lock().await = exit_code_value;
        running.store(false, Ordering::Relaxed);

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

        // Loop-facing completion push (issue #84 Phase 2b follow-up): deliver the
        // result into the owning session's agent loop the same way a sub-agent
        // completion is delivered — pushed, not polled. Only when a sink is wired
        // AND the shell is session-tagged (untagged shells, e.g. from tests, have
        // no loop to notify). The sink hands off to a detached task, so this call
        // is cheap. It is best-effort and idempotent with the durable end-of-turn
        // suspend/poll backstop, which still runs.
        if let (Some(sink), Some(session_id)) = (bash_completion_sink, session_id) {
            // Wait (bounded) for the pumps to reach EOF so the tail is complete;
            // the child's pipes close on exit, so this is the fast common case.
            for handle in pump_handles {
                let _ = timeout(Duration::from_secs(1), handle).await;
            }
            let output_tail = output_tail(&output).await;
            sink.on_bash_completed(BashCompletionInfo {
                session_id,
                bash_id: bash_id_for_sink,
                command: command_for_sink,
                exit_code: exit_code_value,
                status: status_str.to_string(),
                output_tail,
            });
        }

        sleep(Duration::from_secs(COMPLETED_SESSION_TTL_SECS)).await;
        let _ = remove_shell(&session_id_for_gc);
    });
}

/// Join the last [`COMPLETION_TAIL_LINES`] captured lines into a byte-bounded
/// tail for a completion push, keeping the most recent bytes (front-trimmed on a
/// char boundary) when over [`COMPLETION_TAIL_MAX_BYTES`].
async fn output_tail(output: &Arc<Mutex<Vec<String>>>) -> String {
    let buffer = output.lock().await;
    let start = buffer.len().saturating_sub(COMPLETION_TAIL_LINES);
    let joined = buffer[start..].join("\n");
    if joined.len() <= COMPLETION_TAIL_MAX_BYTES {
        return joined;
    }
    let mut cut = joined.len() - COMPLETION_TAIL_MAX_BYTES;
    while cut < joined.len() && !joined.is_char_boundary(cut) {
        cut += 1;
    }
    format!("…{}", &joined[cut..])
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
    bash_completion_sink: Option<Arc<dyn BashCompletionSink>>,
) -> Result<Arc<ShellSession>, String> {
    let shell_id = uuid::Uuid::new_v4().to_string();
    let output = Arc::new(Mutex::new(Vec::new()));
    let base_index = Arc::new(Mutex::new(0usize));
    let running = Arc::new(AtomicBool::new(true));
    let exit_code = Arc::new(Mutex::new(None));
    let kill_notify = Arc::new(Notify::new());

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
        kill_notify: kill_notify.clone(),
        // The foreground streamer always spawns with Stdio::null() (bash.rs), so
        // a promoted shell has no stdin pipe — None preserves EOF-on-read.
        stdin: Arc::new(Mutex::new(None)),
        output: output.clone(),
        base_index: base_index.clone(),
        running: running.clone(),
        exit_code: exit_code.clone(),
    });

    // Spawn pump tasks to continue draining the handed-over readers. The
    // readers may still hold buffered data from the foreground phase — wrapping
    // them in a new BufReader (as pump_stream_lines does) reads through that
    // buffer first, so no data is lost or double-counted.
    let stdout_pump = {
        let output = output.clone();
        let base_index = base_index.clone();
        tokio::spawn(async move {
            pump_stream_lines("stdout", stdout_reader, output, base_index).await;
        })
    };
    let stderr_pump = {
        let output = output.clone();
        let base_index = base_index.clone();
        tokio::spawn(async move {
            pump_stream_lines("stderr", stderr_reader, output, base_index).await;
        })
    };

    spawn_completion_poll(
        child,
        kill_notify,
        shell_id.clone(),
        command.to_string(),
        running,
        exit_code,
        output.clone(),
        session.session_id.clone(),
        event_tx,
        bash_completion_sink,
        vec![stdout_pump, stderr_pump],
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
