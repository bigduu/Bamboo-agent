//! `ClaudeCodeExecutor`: a [`ChildExecutor`] that drives the official Claude
//! Code CLI (`claude`) as an external sub-agent engine over its stream-json
//! wire protocol. See `docs/claude-code-executor.md` for the full protocol
//! reference (spawn flags, NDJSON frame table, permission relay, shutdown)
//! this implementation follows.
//!
//! MVP scope (issue #441): spawn a **fresh `claude` process per `run()` call**
//! (one activation = one turn), map its stdout frames onto the same
//! `AgentEvent`s the real bamboo runtime emits (so the parent's child preview
//! renders identically — see [`BambooRuntimeExecutor`](crate::subagent_worker::BambooRuntimeExecutor)),
//! and relay `can_use_tool` permission asks through [`EventSink::host`] when a
//! host bridge is wired. Mid-turn steering remains out of scope — see the doc
//! comment on [`ChildExecutor::run`]'s `steer` parameter below.
//!
//! Session resume (issue #444): `RunSpec.messages` empty/non-empty is the
//! discriminant a reactivation ships (`proto.rs:28`). A non-empty shipment
//! means this activation has prior context, resolved by [`ClaudeCodeExecutor::run`]
//! in four steps — see its doc comment for the full state-machine and
//! `docs/claude-code-executor.md` §4 for the on-disk state file shape.

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use bamboo_agent_core::{AgentEvent, TokenUsage, ToolResult};
use bamboo_subagent::executor::{ChildExecutor, ChildOutcome, EventSink, HostBridge, SteerInbox};
use bamboo_subagent::proto::RunSpec;

/// Upper bound on a single stdout NDJSON line. Tool results can be huge (the
/// protocol doc specifies a 10 MB scanner buffer at `docs/claude-code-executor.md`
/// §2); enforced incrementally via `fill_buf`/`consume` in [`read_bounded_line`]
/// so a runaway line is capped in memory as it streams in, not merely rejected
/// after already having been buffered in full.
const MAX_STDOUT_LINE_BYTES: usize = 10 * 1024 * 1024;

/// Tail of stderr retained (for the error message on an exit with no `result`
/// frame) — bounded so a chatty child can't grow this without limit.
const STDERR_TAIL_BYTES: usize = 16 * 1024;

/// Tool-result content is truncated to this many characters before riding the
/// `ToolComplete`/`ToolError` event (doc's "truncate" guidance — the full
/// result already lives in the Claude Code CLI's own transcript on disk).
const TOOL_RESULT_TRUNCATE_CHARS: usize = 20_000;

/// Phase 2 of shutdown (§5 of the protocol doc): bounded wait for a natural
/// exit after stdin closes (lets the CLI run its Stop hooks). Shorter than
/// cc-connect's 120s — this is a subagent activation, not an interactive
/// session, and the caller (the actor transport) has its own outer timeout.
const GRACEFUL_EXIT_WAIT: Duration = Duration::from_secs(5);
/// Phase 3: bounded wait after SIGTERM before escalating to SIGKILL.
const SIGTERM_WAIT: Duration = Duration::from_secs(2);

/// Issue #443: how long [`decide_and_respond`] waits on [`HostBridge::approval_call`]
/// before giving up and denying. A host-in-the-loop approver that never comes
/// back (crashed UI, orphaned session, dropped WS with no error surfaced)
/// would otherwise hang the CLI turn forever — this bounds the relay the same
/// way the graceful-shutdown phases bound the process lifecycle.
const APPROVAL_RELAY_TIMEOUT: Duration = Duration::from_secs(300);

/// Issue #443: fixed env allowlist forwarded from the parent process env to
/// every spawned `claude` child (on top of any executor-specific
/// `forward_env` names) — `env_clear()` in [`ClaudeCodeExecutor::build_command`]
/// strips everything else, including `*_API_KEY` and other ambient secrets.
/// `LC_*` (locale vars: `LC_ALL`, `LC_CTYPE`, …) is matched by prefix, not
/// listed here.
const ENV_ALLOWLIST: &[&str] = &[
    "HOME", "PATH", "SHELL", "TERM", "LANG", "TMPDIR", "USER", "LOGNAME",
];

/// Resolve this actor's stable per-child storage dir, exactly like
/// [`BambooRuntimeExecutor::build`](crate::subagent_worker::BambooRuntimeExecutor::build)
/// (`subagent_worker.rs:194-202`): `spec.storage_dir` when the parent already
/// isolated it, else a temp dir keyed by `child_id` — stable across
/// activations of the SAME child. Both `ExecutorSpec::ClaudeCode` factory
/// arms (`subagent_worker.rs`, `broker_agent.rs`) call this to give
/// [`ClaudeCodeExecutor::new`]'s `state_dir` a location the resumed-session
/// state file (issue #444) survives in.
pub fn resolve_claude_code_state_dir(storage_dir: &Option<String>, child_id: &str) -> PathBuf {
    storage_dir
        .clone()
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("bamboo-subagents").join(child_id))
}

/// State file name inside a child's stable storage dir (issue #444). Holds the
/// agent-assigned Claude Code session id this actor last saw, so the NEXT
/// activation of the SAME child can `--resume` it instead of losing context.
const STATE_FILE_NAME: &str = "claude-code-session.json";

/// Fallback history preamble cap (issue #444 test plan: "~24k chars / last
/// ~40 messages, oldest dropped first").
const HISTORY_PREAMBLE_MAX_CHARS: usize = 24_000;
const HISTORY_PREAMBLE_MAX_MESSAGES: usize = 40;

/// On-disk shape of [`STATE_FILE_NAME`]. `workspace` is recorded so a later
/// activation on a DIFFERENT workspace (or a different machine — Claude Code
/// transcripts are machine-local under `~/.claude/projects/<hashed-workdir>/`,
/// docs §4) treats the persisted id as unusable rather than resuming into the
/// wrong project.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ClaudeSessionState {
    session_id: String,
    workspace: Option<String>,
    updated_at: DateTime<Utc>,
}

/// Drives `claude --output-format stream-json --input-format stream-json ...`
/// as the engine behind one sub-agent run.
pub struct ClaudeCodeExecutor {
    /// Executable to spawn. Defaults to `"claude"` (resolved via `PATH`);
    /// tests override it with a stub script.
    binary: String,
    model: Option<String>,
    permission_mode: Option<String>,
    /// Working directory for the spawned CLI's file tools. `None` inherits the
    /// worker process's own cwd (mirrors how [`BambooRuntimeExecutor`](crate::subagent_worker::BambooRuntimeExecutor)
    /// treats an absent `ProvisionSpec.workspace`).
    workspace: Option<String>,
    /// This child's stable per-activation storage dir, used to persist
    /// [`STATE_FILE_NAME`] across activations (issue #444). Resolved by the
    /// caller exactly like [`BambooRuntimeExecutor::build`](crate::subagent_worker::BambooRuntimeExecutor::build)
    /// (`spec.storage_dir` else a temp dir keyed by `child_id`) — NOT inside
    /// this constructor, so tests can point it at an isolated tempdir. `None`
    /// disables resume persistence entirely (every activation is fresh).
    state_dir: Option<PathBuf>,
    /// Issue #443: `false` (the default) adds `--strict-mcp-config` and
    /// `--setting-sources project` so the child does NOT load the invoking
    /// user's `~/.claude` MCP servers/skills/settings. `true` omits both
    /// flags (the old inherit-everything behavior).
    inherit_user_config: bool,
    /// Issue #443: extra env var NAMES forwarded verbatim from the parent
    /// process env, on top of the fixed [`ENV_ALLOWLIST`].
    forward_env: Vec<String>,
    /// Issue #443: bound on [`HostBridge::approval_call`] in
    /// [`decide_and_respond`]. Always [`APPROVAL_RELAY_TIMEOUT`] outside
    /// tests; overridable via [`Self::with_relay_timeout_for_test`] so a unit
    /// test exercising the expiry path doesn't have to wait 300s.
    relay_timeout: Duration,
}

impl ClaudeCodeExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        binary: Option<String>,
        model: Option<String>,
        permission_mode: Option<String>,
        workspace: Option<String>,
        state_dir: Option<PathBuf>,
        inherit_user_config: bool,
        forward_env: Vec<String>,
    ) -> Self {
        Self {
            binary: binary.unwrap_or_else(|| "claude".to_string()),
            model,
            permission_mode,
            workspace,
            state_dir,
            inherit_user_config,
            forward_env,
            relay_timeout: APPROVAL_RELAY_TIMEOUT,
        }
    }

    /// Test-only override of [`Self::relay_timeout`] — production callers
    /// always get [`APPROVAL_RELAY_TIMEOUT`]. Lets a unit test exercise the
    /// timeout-expiry path in milliseconds instead of 300s.
    #[cfg(test)]
    fn with_relay_timeout_for_test(mut self, timeout: Duration) -> Self {
        self.relay_timeout = timeout;
        self
    }

    fn state_file_path(&self) -> Option<PathBuf> {
        self.state_dir.as_ref().map(|dir| dir.join(STATE_FILE_NAME))
    }

    /// Read and parse the state file, if any. Any failure (missing dir, no
    /// file, corrupt JSON) is treated as "no usable id" rather than an error
    /// — resume is a best-effort optimization, never a hard requirement.
    async fn read_state(&self) -> Option<ClaudeSessionState> {
        let path = self.state_file_path()?;
        let bytes = tokio::fs::read(&path).await.ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    /// Delete the state file (best-effort). Called before a fresh-session
    /// activation (`messages` empty) so a subsequent `rerun` never
    /// accidentally resumes stale context, and before a resume-failure retry
    /// so a garbage-collected/bad id isn't offered again.
    async fn delete_state_file(&self) {
        if let Some(path) = self.state_file_path() {
            let _ = tokio::fs::remove_file(&path).await;
        }
    }

    /// Atomically persist the session id this activation last saw (tmp file +
    /// rename, in the same dir so the rename is same-filesystem). Called on
    /// EVERY `system`/`result` frame that carries a session id — a resumed
    /// session may be assigned a brand-new one, so this always re-captures
    /// rather than assuming stability.
    async fn write_state(&self, session_id: &str) {
        let Some(dir) = &self.state_dir else { return };
        let path = dir.join(STATE_FILE_NAME);
        let state = ClaudeSessionState {
            session_id: session_id.to_string(),
            workspace: self.workspace.clone(),
            updated_at: Utc::now(),
        };
        let Ok(bytes) = serde_json::to_vec_pretty(&state) else {
            return;
        };
        if tokio::fs::create_dir_all(dir).await.is_err() {
            return;
        }
        // Unique tmp name (pid + session id) so concurrent activations of
        // different children never collide on the same tmp path.
        let tmp_path = dir.join(format!("{STATE_FILE_NAME}.{}.tmp", std::process::id()));
        if tokio::fs::write(&tmp_path, &bytes).await.is_err() {
            return;
        }
        let _ = tokio::fs::rename(&tmp_path, &path).await;
    }

    /// Step 2 of the activation logic (issue #444): a usable persisted id
    /// requires a non-empty `messages` shipment (the reactivation
    /// discriminant) AND a state file whose recorded `workspace` matches this
    /// executor's current one — a mismatch (different project, or a
    /// different machine entirely, since Claude Code transcripts are
    /// machine-local) makes the id unusable and falls through to step 3.
    async fn resolve_resume_id(&self) -> Option<String> {
        let state = self.read_state().await?;
        if state.workspace != self.workspace {
            tracing::warn!(
                recorded = ?state.workspace,
                current = ?self.workspace,
                "claude code: state file workspace mismatch; falling back to history rehydration"
            );
            return None;
        }
        Some(state.session_id)
    }

    /// `resume_id`: when `Some`, append `--resume <id>` (step 2 of the
    /// activation logic — reattach to a persisted Claude Code session
    /// instead of spawning fresh).
    fn build_command(&self, resume_id: Option<&str>) -> Command {
        let mut cmd = Command::new(&self.binary);
        cmd.arg("--output-format")
            .arg("stream-json")
            .arg("--input-format")
            .arg("stream-json")
            .arg("--permission-prompt-tool")
            .arg("stdio")
            .arg("--replay-user-messages")
            .arg("--verbose");
        // Issue #443 CRITICAL: the CLI's headless stream-json default
        // permission mode is `auto` (self-approve every tool, never asks) --
        // NOT `default`. Passing no `--permission-mode` flag therefore means
        // every actor silently self-approves. Always pass an EXPLICIT mode:
        // the configured value when set, else `default` -- which actually
        // engages the local-decide policy in `decide_and_respond` below
        // ("no host bridge -> deny unless bypassPermissions") instead of
        // that policy being unreachable dead code.
        cmd.arg("--permission-mode")
            .arg(self.permission_mode.as_deref().unwrap_or("default"));
        if let Some(model) = &self.model {
            cmd.arg("--model").arg(model);
        }
        if let Some(id) = resume_id {
            cmd.arg("--resume").arg(id);
        }
        // Issue #443: isolate from the invoking user's ~/.claude setup by
        // default -- an e2e run showed 6 MCP servers (incl. desktop
        // control), every skill, and ~8k cache-creation tokens leaking in
        // from global config for a single `touch`. `inherit_user_config:
        // true` opts back into the CLI's normal (inherit-everything)
        // behavior.
        if !self.inherit_user_config {
            cmd.arg("--strict-mcp-config");
            cmd.arg("--setting-sources").arg("project");
        }
        // Issue #443: env allowlist. `env_clear()` plus an explicit forward
        // list supersedes the old single `env_remove("CLAUDECODE")`
        // hardening -- a cleared env can no longer carry a leaked CLAUDECODE
        // (or any other ambient secret, e.g. `*_API_KEY`) from the parent at
        // all. The `env_remove` call below is kept anyway as executable
        // documentation of the specific nested-session hazard
        // (docs/claude-code-executor.md, spawn flags section) and as
        // defense-in-depth if the allowlist is ever loosened.
        cmd.env_clear();
        for (key, value) in std::env::vars() {
            if ENV_ALLOWLIST.contains(&key.as_str()) || key.starts_with("LC_") {
                cmd.env(key, value);
            }
        }
        for name in &self.forward_env {
            if let Ok(value) = std::env::var(name) {
                cmd.env(name, value);
            }
        }
        // Nested-session detection: Claude Code misbehaves if it inherits its
        // own env var from an outer session.
        cmd.env_remove("CLAUDECODE");
        if let Some(ws) = &self.workspace {
            cmd.current_dir(ws);
        }
        cmd.stdin(Stdio::piped());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
        // Safety net: if this future is ever dropped without running our own
        // shutdown sequence (panic, abort), don't leak the child.
        cmd.kill_on_drop(true);
        #[cfg(unix)]
        {
            // Own process group so shutdown can SIGTERM/SIGKILL the whole tree
            // (claude → any MCP servers it spawns), not just the leader.
            cmd.process_group(0);
        }
        cmd
    }

    /// Dispatch one parsed stdout frame. Returns `Some(outcome)` when the
    /// frame is turn-terminal (a non-compaction `result`); `None` otherwise —
    /// including for `control_request`/`control_cancel_request`, which are
    /// handled here but never end the run themselves.
    async fn handle_frame(
        &self,
        value: Value,
        events: &EventSink,
        write_tx: &mpsc::UnboundedSender<Value>,
        pending: &mut HashMap<String, JoinHandle<()>>,
        last_text: &mut String,
    ) -> Option<ChildOutcome> {
        let frame_type = value.get("type").and_then(Value::as_str).unwrap_or("");
        match frame_type {
            "system" => {
                let session_id = value
                    .get("session_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let model = value.get("model").and_then(|v| v.as_str()).unwrap_or("");
                tracing::debug!(session_id, model, "claude code: session bootstrap");
                if !session_id.is_empty() {
                    self.write_state(session_id).await;
                }
                None
            }
            "assistant" => {
                if let Some(blocks) = value.pointer("/message/content").and_then(Value::as_array) {
                    for block in blocks {
                        emit_assistant_block(block, events, last_text);
                    }
                }
                None
            }
            "user" => {
                if let Some(blocks) = value.pointer("/message/content").and_then(Value::as_array) {
                    for block in blocks {
                        emit_tool_result_block(block, events);
                    }
                }
                None
            }
            "result" => {
                let subtype = value.get("subtype").and_then(Value::as_str).unwrap_or("");
                if matches!(subtype, "compact" | "compaction") {
                    // Mid-turn compaction, NOT completion (cc-connect issue #481
                    // — see docs/claude-code-executor.md §2's `result` row).
                    tracing::debug!("claude code: mid-turn compaction result, continuing");
                    return None;
                }
                // A resumed session may be assigned a brand-new id — always
                // re-capture from `result` too, not just `system` (issue #444).
                if let Some(session_id) = value.get("session_id").and_then(Value::as_str) {
                    if !session_id.is_empty() {
                        self.write_state(session_id).await;
                    }
                }
                let final_text = value
                    .get("result")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .unwrap_or_else(|| last_text.clone());
                let usage = value
                    .get("usage")
                    .map(|u| {
                        let prompt = u.get("input_tokens").and_then(Value::as_u64).unwrap_or(0);
                        let completion =
                            u.get("output_tokens").and_then(Value::as_u64).unwrap_or(0);
                        TokenUsage {
                            prompt_tokens: prompt,
                            completion_tokens: completion,
                            total_tokens: prompt.saturating_add(completion),
                        }
                    })
                    .unwrap_or_default();
                events.emit(event_json(AgentEvent::Complete { usage }));
                Some(ChildOutcome::completed(final_text))
            }
            "control_request" => {
                self.handle_control_request(value, events, write_tx, pending);
                None
            }
            "control_cancel_request" => {
                let request_id = value
                    .get("request_id")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if let Some(handle) = pending.remove(request_id) {
                    handle.abort();
                }
                None
            }
            other => {
                tracing::debug!(frame_type = other, "claude code: unrecognized stdout frame");
                None
            }
        }
    }

    /// Handle one `control_request` (permission relay §3 of the protocol doc):
    /// spawns a background task so the read loop keeps consuming stdout while
    /// a (possibly slow, human-in-the-loop) approval decision is pending. The
    /// task is tracked in `pending` so a later `control_cancel_request` can
    /// abort it.
    fn handle_control_request(
        &self,
        value: Value,
        events: &EventSink,
        write_tx: &mpsc::UnboundedSender<Value>,
        pending: &mut HashMap<String, JoinHandle<()>>,
    ) {
        let request_id = value
            .get("request_id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let request = value.get("request").cloned().unwrap_or_else(|| json!({}));
        let subtype = request.get("subtype").and_then(Value::as_str).unwrap_or("");
        if subtype != "can_use_tool" {
            // Only the tool-permission ask is understood in the MVP. Deny
            // rather than ignore — an un-answered control_request otherwise
            // hangs the CLI turn waiting for a response that never comes.
            send_control_response(
                write_tx,
                &request_id,
                false,
                None,
                Some(format!("unsupported control_request subtype '{subtype}'")),
            );
            return;
        }
        let tool_name = request
            .get("tool_name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let input = request.get("input").cloned().unwrap_or_else(|| json!({}));
        let host = events.host().cloned();
        let permission_mode = self.permission_mode.clone();
        let relay_timeout = self.relay_timeout;
        let write_tx = write_tx.clone();
        let task_request_id = request_id.clone();
        let handle = tokio::spawn(async move {
            decide_and_respond(
                host,
                permission_mode,
                relay_timeout,
                &task_request_id,
                &tool_name,
                input,
                &write_tx,
            )
            .await;
        });
        pending.insert(request_id, handle);
    }

    /// Graceful 3-phase close (§5 of the protocol doc): the caller has already
    /// closed stdin (dropped every writer sender) before calling this — that
    /// is what lets the CLI's Stop hooks observe EOF and run. From here:
    /// bounded wait for a natural exit, then SIGTERM the process group, a
    /// shorter wait, then SIGKILL the process group.
    async fn shutdown_child(child: &mut Child) {
        if tokio::time::timeout(GRACEFUL_EXIT_WAIT, child.wait())
            .await
            .is_ok()
        {
            return;
        }
        signal_process_group(child, ProcessSignal::Term);
        if tokio::time::timeout(SIGTERM_WAIT, child.wait())
            .await
            .is_ok()
        {
            return;
        }
        signal_process_group(child, ProcessSignal::Kill);
        let _ = child.wait().await;
    }
}

impl ClaudeCodeExecutor {
    /// One `claude` child process activation: spawn (fresh or `--resume
    /// resume_id`), write `body` as the stdin user turn, read frames until a
    /// terminal `result` (or EOF/cancel), then run the graceful shutdown.
    /// Extracted out of [`ChildExecutor::run`] so the resume-failure retry
    /// (step 4 of the activation logic) doesn't duplicate the whole read
    /// loop — `run` calls this up to twice for a single activation, sharing
    /// one `events` sink and `cancel` token across both attempts.
    ///
    /// Returns `(outcome, exited_without_result)` — the second element is
    /// `true` only for the specific "process exited before a terminal
    /// `result` frame arrived" error path, which is the ONLY case `run`
    /// treats as retry-eligible.
    async fn run_once(
        &self,
        body: &str,
        resume_id: Option<&str>,
        events: &EventSink,
        cancel: &CancellationToken,
    ) -> (ChildOutcome, bool) {
        let mut child = match self.build_command(resume_id).spawn() {
            Ok(c) => c,
            Err(e) => {
                return (
                    ChildOutcome::error(format!("spawn '{}': {e}", self.binary)),
                    false,
                )
            }
        };
        let Some(stdin) = child.stdin.take() else {
            return (
                ChildOutcome::error("claude child has no stdin pipe".to_string()),
                false,
            );
        };
        let Some(stdout) = child.stdout.take() else {
            return (
                ChildOutcome::error("claude child has no stdout pipe".to_string()),
                false,
            );
        };
        let stderr = child.stderr.take();

        let stderr_tail = Arc::new(Mutex::new(String::new()));
        let stderr_task = stderr.map(|stderr| {
            let tail = stderr_tail.clone();
            tokio::spawn(async move { drain_stderr_tail(stderr, tail).await })
        });

        let (write_tx, writer_handle) = spawn_stdin_writer(stdin);
        let assignment_frame = json!({
            "type": "user",
            "message": { "role": "user", "content": body },
        });
        if write_tx.send(assignment_frame).is_err() {
            let _ = child.start_kill();
            return (
                ChildOutcome::error(
                    "claude code executor: failed to queue the assignment on stdin".to_string(),
                ),
                false,
            );
        }

        let mut reader = tokio::io::BufReader::with_capacity(64 * 1024, stdout);
        let mut pending: HashMap<String, JoinHandle<()>> = HashMap::new();
        let mut last_text = String::new();

        let (outcome, exited_without_result) = loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    break (ChildOutcome::cancelled(), false);
                }
                line = read_bounded_line(&mut reader, MAX_STDOUT_LINE_BYTES) => {
                    match line {
                        Ok(Some(bytes)) => {
                            if bytes.iter().all(u8::is_ascii_whitespace) {
                                continue;
                            }
                            let value: Value = match serde_json::from_slice(&bytes) {
                                Ok(v) => v,
                                Err(e) => {
                                    tracing::debug!("claude code: unparsable stdout line ({e}); skipping");
                                    continue;
                                }
                            };
                            if let Some(outcome) = self
                                .handle_frame(value, events, &write_tx, &mut pending, &mut last_text)
                                .await
                            {
                                break (outcome, false);
                            }
                        }
                        Ok(None) => {
                            // EOF with no terminal `result` frame — the process
                            // exited (or closed stdout) unexpectedly.
                            let code = child.wait().await.ok().and_then(|s| s.code());
                            let tail = stderr_tail.lock().await.clone();
                            break (ChildOutcome::error(format!(
                                "claude exited (code {code:?}) without a result frame; stderr tail: {}",
                                if tail.is_empty() { "<empty>" } else { tail.trim() }
                            )), true);
                        }
                        Err(e) => {
                            break (ChildOutcome::error(format!("claude stdout read error: {e}")), false);
                        }
                    }
                }
            }
        };

        for (_, handle) in pending.drain() {
            handle.abort();
        }
        // Close stdin (phase 1 of shutdown): drop every sender clone so the
        // writer task's channel drains and its `ChildStdin` is dropped, then
        // give it a brief bounded moment to actually finish — an aborted
        // control-request task's clone is dropped asynchronously, so this is
        // best-effort, not a hard requirement (the graceful-exit wait below
        // covers the remaining slack).
        drop(write_tx);
        let _ = tokio::time::timeout(Duration::from_millis(500), writer_handle).await;
        if let Some(stderr_task) = stderr_task {
            stderr_task.abort();
        }

        Self::shutdown_child(&mut child).await;
        (outcome, exited_without_result)
    }
}

#[async_trait]
impl ChildExecutor for ClaudeCodeExecutor {
    /// Activation logic (issue #444), driven by `spec.messages` — empty means
    /// first activation, non-empty means a reactivation carrying prior
    /// context (`RunSpec.messages` doc, `proto.rs:28`):
    ///
    /// 1. `messages` empty → fresh session; delete any stale state file (a
    ///    `rerun` must never accidentally resume).
    /// 2. `messages` non-empty AND the state file has an id recorded against
    ///    the SAME `workspace` → spawn with `--resume <id>`, sending just the
    ///    live assignment (the CLI already has the transcript).
    /// 3. `messages` non-empty but no usable id (first run on this machine,
    ///    storage GC'd, workspace changed) → fallback: render the shipped
    ///    history into a bounded preamble prepended to the assignment.
    /// 4. If a `--resume` spawn exits without ever producing a `result`
    ///    frame (bad/GC'd session id — the CLI errors out fast), retry
    ///    ONCE without `--resume`, using the same fallback rehydration as
    ///    step 3. No retry loop beyond this single attempt.
    async fn run(
        &self,
        spec: RunSpec,
        events: EventSink,
        // Claude Code's stream-json protocol has no mid-turn user-message
        // injection: a turn is one stdin write followed by a read to `result`
        // (docs/claude-code-executor.md §5 — "no reliable mid-turn interrupt
        // over this protocol"). Steering is drained (so an unbounded backlog
        // can't build up on the sender side) but never acted on — turning a
        // steer message into a genuinely new turn on the SAME (possibly
        // resumed) session is left to a future revision.
        mut steer: SteerInbox,
        cancel: CancellationToken,
    ) -> ChildOutcome {
        // Ignore steer messages (see doc comment on `steer` above) but keep
        // draining so the sender never sees an unbounded backlog. Spans BOTH
        // possible spawn attempts below — the inbox belongs to the whole
        // activation, not to one child process.
        let steer_drain = tokio::spawn(async move { while steer.recv().await.is_some() {} });

        // Step 1: a fresh activation must never resume stale context.
        if spec.messages.is_empty() {
            self.delete_state_file().await;
        }

        // Step 2: a usable persisted id under the SAME workspace.
        let resume_id = if spec.messages.is_empty() {
            None
        } else {
            self.resolve_resume_id().await
        };

        // Step 3: fallback body when there's history but no usable id.
        let body = build_turn_body(&spec, resume_id.as_deref());

        let used_resume = resume_id.is_some();
        let (outcome, exited_without_result) = self
            .run_once(&body, resume_id.as_deref(), &events, &cancel)
            .await;

        // Step 4: retry-once, ONLY when the failed attempt itself used
        // `--resume` and died before a `result` frame ever arrived.
        let outcome = if used_resume && exited_without_result {
            tracing::warn!(
                "claude code: --resume spawn exited without a result frame; \
                 retrying once without --resume"
            );
            self.delete_state_file().await;
            let fallback_body = build_turn_body(&spec, None);
            self.run_once(&fallback_body, None, &events, &cancel)
                .await
                .0
        } else {
            outcome
        };

        steer_drain.abort();
        outcome
    }
}

/// Which signal [`signal_process_group`] sends.
enum ProcessSignal {
    Term,
    Kill,
}

/// Best-effort signal to the whole process group the child leads (its pgid
/// equals its pid — `build_command` set `process_group(0)` at spawn on unix).
/// No-op on non-unix targets in this MVP; the final phase there falls back to
/// killing just the direct child via `Child::start_kill` in `shutdown_child`'s
/// caller-visible behavior (still bounded — [`Child::wait`] then reaps it).
#[cfg(unix)]
fn signal_process_group(child: &Child, signal: ProcessSignal) {
    if let Some(pid) = child.id() {
        let signo = match signal {
            ProcessSignal::Term => libc::SIGTERM,
            ProcessSignal::Kill => libc::SIGKILL,
        };
        // SAFETY: `kill(2)` with a pid_t derived from our own child's pid and
        // a fixed signal constant; a negative pid targets the whole process
        // group. Failure (e.g. ESRCH — already exited) is fine to ignore,
        // this call is best-effort cleanup.
        unsafe {
            libc::kill(-(pid as libc::pid_t), signo);
        }
    }
}

#[cfg(not(unix))]
fn signal_process_group(_child: &Child, _signal: ProcessSignal) {}

/// Serialize `value` to one NDJSON line and write it on the writer task owning
/// stdin; drops silently if the writer is gone (matches [`EventSink::emit`]'s
/// "dropped silently if the peer is gone" convention).
fn spawn_stdin_writer(
    mut stdin: tokio::process::ChildStdin,
) -> (mpsc::UnboundedSender<Value>, JoinHandle<()>) {
    let (tx, mut rx) = mpsc::unbounded_channel::<Value>();
    let handle = tokio::spawn(async move {
        while let Some(value) = rx.recv().await {
            let Ok(mut line) = serde_json::to_vec(&value) else {
                continue;
            };
            line.push(b'\n');
            if stdin.write_all(&line).await.is_err() {
                break;
            }
            if stdin.flush().await.is_err() {
                break;
            }
        }
        // `stdin` drops here (once every sender clone is gone and the channel
        // drains), closing the write half — the EOF the CLI's Stop hooks see.
    });
    (tx, handle)
}

/// Read one NDJSON line, bounded to `max_bytes` (enforced incrementally via
/// `fill_buf`/`consume`, not after buffering an unbounded amount). Returns
/// `Ok(None)` on a clean EOF with no trailing partial line.
async fn read_bounded_line<R>(reader: &mut R, max_bytes: usize) -> std::io::Result<Option<Vec<u8>>>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    let mut out = Vec::new();
    loop {
        let (found, consumed) = {
            let available = reader.fill_buf().await?;
            if available.is_empty() {
                return Ok(if out.is_empty() { None } else { Some(out) });
            }
            match available.iter().position(|&b| b == b'\n') {
                Some(pos) => {
                    out.extend_from_slice(&available[..pos]);
                    (true, pos + 1)
                }
                None => {
                    out.extend_from_slice(available);
                    (false, available.len())
                }
            }
        };
        reader.consume(consumed);
        if found {
            return Ok(Some(out));
        }
        if out.len() > max_bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("stdout line exceeded {max_bytes} bytes"),
            ));
        }
    }
}

/// Drain stderr into a bounded tail buffer (oldest bytes dropped once the cap
/// is exceeded) for the "exited without a result frame" error message.
async fn drain_stderr_tail(stderr: tokio::process::ChildStderr, tail: Arc<Mutex<String>>) {
    let mut reader = BufReader::new(stderr);
    let mut buf = Vec::new();
    loop {
        buf.clear();
        match reader.read_until(b'\n', &mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {
                let mut t = tail.lock().await;
                t.push_str(&String::from_utf8_lossy(&buf));
                if t.len() > STDERR_TAIL_BYTES {
                    let excess = t.len() - STDERR_TAIL_BYTES;
                    let cut = t
                        .char_indices()
                        .map(|(i, _)| i)
                        .find(|&i| i >= excess)
                        .unwrap_or(t.len());
                    t.drain(..cut);
                }
            }
        }
    }
}

/// Emit the `AgentEvent` for one `assistant` message content block (`text` /
/// `thinking` / `tool_use`); unrecognized block types are ignored.
fn emit_assistant_block(block: &Value, events: &EventSink, last_text: &mut String) {
    match block.get("type").and_then(Value::as_str) {
        Some("text") => {
            let text = block.get("text").and_then(Value::as_str).unwrap_or("");
            if !text.is_empty() {
                last_text.push_str(text);
                events.emit(event_json(AgentEvent::Token {
                    content: text.to_string(),
                }));
            }
        }
        Some("thinking") => {
            let text = block.get("thinking").and_then(Value::as_str).unwrap_or("");
            if !text.is_empty() {
                events.emit(event_json(AgentEvent::ReasoningToken {
                    content: text.to_string(),
                }));
            }
        }
        Some("tool_use") => {
            let tool_call_id = block
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let tool_name = block
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let arguments = block.get("input").cloned().unwrap_or_else(|| json!({}));
            events.emit(event_json(AgentEvent::ToolStart {
                tool_call_id,
                tool_name,
                arguments,
            }));
        }
        _ => {}
    }
}

/// Emit the `AgentEvent` for one `user` message content block, when it is a
/// `tool_result` (other block types in an echoed user message are ignored).
fn emit_tool_result_block(block: &Value, events: &EventSink) {
    if block.get("type").and_then(Value::as_str) != Some("tool_result") {
        return;
    }
    let tool_call_id = block
        .get("tool_use_id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let is_error = block
        .get("is_error")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let text = truncate_chars(
        &tool_result_text(block.get("content")),
        TOOL_RESULT_TRUNCATE_CHARS,
    );
    let event = if is_error {
        AgentEvent::ToolError {
            tool_call_id,
            error: text,
        }
    } else {
        AgentEvent::ToolComplete {
            tool_call_id,
            result: ToolResult::text(true, text),
        }
    };
    events.emit(event_json(event));
}

/// A `tool_result` block's `content` is either a plain string or an array of
/// content blocks (Anthropic message shape); flatten either into plain text.
fn tool_result_text(content: Option<&Value>) -> String {
    match content {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|b| b.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n"),
        Some(other) => other.to_string(),
        None => String::new(),
    }
}

/// Step 2/3 of the activation logic: the actual stdin body for one turn.
/// `resume_id: Some` (or an empty `spec.messages`) means the CLI already has
/// (or needs no) context, so the plain assignment is sent; otherwise the
/// fallback history preamble is prepended, clearly delimited from the live
/// task so the model doesn't confuse rehydrated context with the current ask.
fn build_turn_body(spec: &RunSpec, resume_id: Option<&str>) -> String {
    if resume_id.is_some() || spec.messages.is_empty() {
        return spec.assignment.clone();
    }
    match render_history_preamble(&spec.messages, &spec.assignment) {
        Some(preamble) => format!("{preamble}\n\n## Current task\n\n{}", spec.assignment),
        None => spec.assignment.clone(),
    }
}

/// Render `RunSpec.messages` (serialized domain `Message`s, oldest first,
/// INCLUDING the assignment's own trailing user message per the wire
/// contract — `proto.rs:28`) into a bounded fallback preamble. Unknown/
/// malformed entries (missing `role`/`content`, non-string `content`) are
/// skipped defensively rather than failing the run. Returns `None` when
/// there is nothing left to render (e.g. the only shipped message IS the
/// current assignment, already excluded below to avoid duplicating it).
fn render_history_preamble(messages: &[Value], assignment: &str) -> Option<String> {
    let mut entries: Vec<(String, String)> = messages
        .iter()
        .filter_map(|m| {
            let role = m.get("role").and_then(Value::as_str)?.to_string();
            let content = m.get("content").and_then(Value::as_str)?;
            if content.is_empty() {
                return None;
            }
            Some((role, content.to_string()))
        })
        .collect();

    // The assignment's own user message rides in `messages` too (contract) —
    // drop it here so the preamble doesn't duplicate the live task below it.
    if let Some((role, content)) = entries.last() {
        if role == "user" && content == assignment {
            entries.pop();
        }
    }
    if entries.is_empty() {
        return None;
    }

    // Cap by message count, oldest dropped first.
    let dropped_by_count = entries.len().saturating_sub(HISTORY_PREAMBLE_MAX_MESSAGES);
    if dropped_by_count > 0 {
        entries.drain(0..dropped_by_count);
    }

    let mut rendered: Vec<String> = entries
        .iter()
        .map(|(role, content)| format!("**{role}**: {content}"))
        .collect();

    // Cap by char budget, oldest rendered entry dropped first; if even the
    // single most-recent entry alone exceeds the budget, truncate it in place
    // (never silently drop the entire preamble).
    let mut dropped_by_chars = 0usize;
    while rendered.len() > 1
        && rendered
            .iter()
            .map(|s| s.chars().count() + 2)
            .sum::<usize>()
            > HISTORY_PREAMBLE_MAX_CHARS
    {
        rendered.remove(0);
        dropped_by_chars += 1;
    }
    if let [only] = rendered.as_mut_slice() {
        if only.chars().count() > HISTORY_PREAMBLE_MAX_CHARS {
            *only = truncate_chars(only, HISTORY_PREAMBLE_MAX_CHARS);
        }
    }

    let mut out = String::from("## Prior conversation (rehydrated)\n\n");
    if dropped_by_count > 0 || dropped_by_chars > 0 {
        out.push_str(&format!(
            "_[truncated: {} earlier message(s) omitted]_\n\n",
            dropped_by_count + dropped_by_chars
        ));
    }
    out.push_str(&rendered.join("\n\n"));
    Some(out)
}

fn truncate_chars(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        return s.to_string();
    }
    let head: String = s.chars().take(max_chars).collect();
    let dropped = s.chars().count() - max_chars;
    format!("{head}\n… [truncated, {dropped} more chars]")
}

/// Decide a `can_use_tool` permission ask and write the `control_response`
/// (§3 of the protocol doc). Runs off the read loop (spawned by the caller)
/// so a slow human-in-the-loop decision doesn't block consuming other frames.
async fn decide_and_respond(
    host: Option<HostBridge>,
    permission_mode: Option<String>,
    relay_timeout: Duration,
    request_id: &str,
    tool_name: &str,
    input: Value,
    write_tx: &mpsc::UnboundedSender<Value>,
) {
    if tool_name == "AskUserQuestion" {
        // Structured interactive questions need bamboo's QuestionDialog path,
        // not the permission path (docs/claude-code-executor.md §3) — not
        // wired yet. Deny promptly rather than hang the CLI turn.
        send_control_response(
            write_tx,
            request_id,
            false,
            None,
            Some(
                "interactive questions are not supported by the Claude Code executor yet"
                    .to_string(),
            ),
        );
        return;
    }

    let (allow, deny_message) = if let Some(host) = host {
        let body = json!({ "tool_name": tool_name, "input": input });
        // Issue #443: bound the relay so a host-in-the-loop approver that
        // never replies (crashed UI, orphaned session) can't hang this turn
        // forever — `approval_call`'s own error path (the reply oneshot
        // DROPPED) is already handled by the `Err(e)` arm below; this timeout
        // covers the complementary case where the sender is held open but
        // never sent.
        match tokio::time::timeout(relay_timeout, host.approval_call(body)).await {
            Ok(Ok(reply)) => {
                let approved = reply
                    .get("approved")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let msg = (!approved).then(|| "denied by host approver".to_string());
                (approved, msg)
            }
            Ok(Err(e)) => (false, Some(format!("approval relay failed: {e}"))),
            Err(_) => (
                false,
                Some(format!(
                    "approval relay timed out after {}s; denying",
                    relay_timeout.as_secs()
                )),
            ),
        }
    } else if permission_mode.as_deref() == Some("bypassPermissions") {
        (true, None)
    } else {
        (
            false,
            Some(
                "permission relay unavailable; run with bypassPermissions or attach a host bridge"
                    .to_string(),
            ),
        )
    };
    let updated_input = allow.then_some(input);
    send_control_response(write_tx, request_id, allow, updated_input, deny_message);
}

/// Write one `control_response` frame (§3 of the protocol doc).
fn send_control_response(
    write_tx: &mpsc::UnboundedSender<Value>,
    request_id: &str,
    allow: bool,
    updated_input: Option<Value>,
    deny_message: Option<String>,
) {
    let response = if allow {
        json!({
            "behavior": "allow",
            "updatedInput": updated_input.unwrap_or_else(|| json!({})),
        })
    } else {
        json!({
            "behavior": "deny",
            "message": deny_message.unwrap_or_default(),
        })
    };
    let frame = json!({
        "type": "control_response",
        "response": {
            "subtype": "success",
            "request_id": request_id,
            "response": response,
        },
    });
    let _ = write_tx.send(frame);
}

/// Serialize an `AgentEvent` for [`EventSink::emit`]. Mirrors how
/// [`BambooRuntimeExecutor`](crate::subagent_worker::BambooRuntimeExecutor)
/// forwards real engine events verbatim — this executor maps the Claude Code
/// wire protocol onto the SAME event enum rather than hand-rolled JSON, so the
/// parent's child preview renders identically regardless of which engine ran.
fn event_json(event: AgentEvent) -> Value {
    serde_json::to_value(event).unwrap_or_else(|_| json!({}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;

    use bamboo_subagent::executor::EventSink;
    use bamboo_subagent::proto::TerminalStatus;

    /// Write an executable `sh` stub at `dir/claude` with `body` as its
    /// script content, and return the path. Tests point `ClaudeCodeExecutor`'s
    /// `binary` override at this instead of a real `claude` install.
    fn write_stub(dir: &std::path::Path, body: &str) -> PathBuf {
        let path = dir.join("claude");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "#!/bin/sh").unwrap();
        f.write_all(body.as_bytes()).unwrap();
        let mut perms = std::fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&path, perms).unwrap();
        path
    }

    /// No state dir — matches the MVP's fresh-session-every-time behavior
    /// (used by tests that don't exercise resume at all).
    fn executor(binary: PathBuf) -> ClaudeCodeExecutor {
        executor_with_state(binary, None, None)
    }

    fn executor_with_state(
        binary: PathBuf,
        state_dir: Option<PathBuf>,
        workspace: Option<String>,
    ) -> ClaudeCodeExecutor {
        ClaudeCodeExecutor::new(
            Some(binary.to_string_lossy().into_owned()),
            None,
            None,
            workspace,
            state_dir,
            false,
            Vec::new(),
        )
    }

    fn run_spec(assignment: &str) -> RunSpec {
        RunSpec {
            assignment: assignment.to_string(),
            reasoning_effort: None,
            messages: Vec::new(),
        }
    }

    fn run_spec_with_messages(assignment: &str, messages: Vec<Value>) -> RunSpec {
        RunSpec {
            assignment: assignment.to_string(),
            reasoning_effort: None,
            messages,
        }
    }

    fn msg(role: &str, content: &str) -> Value {
        json!({ "role": role, "content": content })
    }

    fn read_argv(dir: &std::path::Path, name: &str) -> String {
        std::fs::read_to_string(dir.join(name)).unwrap_or_default()
    }

    /// Extract the JSON `message.content` string a stub captured from its
    /// first stdin line (the `{"type":"user","message":{...}}` assignment
    /// frame `spawn_stdin_writer` writes).
    fn stdin_body(dir: &std::path::Path, name: &str) -> String {
        let raw = std::fs::read_to_string(dir.join(name)).unwrap();
        let value: Value = serde_json::from_str(raw.trim()).unwrap();
        value["message"]["content"].as_str().unwrap().to_string()
    }

    #[tokio::test]
    async fn happy_path_streams_events_then_completes() {
        let dir = tempfile::tempdir().unwrap();
        let bin = write_stub(
            dir.path(),
            r#"
read -r _assignment
echo '{"type":"system","session_id":"s1","model":"stub-model"}'
echo '{"type":"assistant","message":{"content":[{"type":"text","text":"Working on it"},{"type":"tool_use","id":"toolu_1","name":"Bash","input":{"command":"echo hi"}}]}}'
echo '{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"hi","is_error":false}]}}'
echo '{"type":"result","subtype":"success","result":"done: hi","usage":{"input_tokens":10,"output_tokens":5}}'
"#,
        );
        let (sink, mut rx) = EventSink::channel();
        let outcome = executor(bin)
            .run(
                run_spec("say hi"),
                sink,
                SteerInbox::disconnected(),
                CancellationToken::new(),
            )
            .await;

        assert_eq!(outcome.status, TerminalStatus::Completed);
        assert_eq!(outcome.result.as_deref(), Some("done: hi"));

        let mut events = Vec::new();
        while let Ok(e) = rx.try_recv() {
            events.push(e);
        }
        let types: Vec<&str> = events
            .iter()
            .map(|e| e["type"].as_str().unwrap_or(""))
            .collect();
        assert_eq!(
            types,
            vec!["token", "tool_start", "tool_complete", "complete"]
        );
        assert_eq!(events[0]["content"], "Working on it");
        assert_eq!(events[1]["tool_name"], "Bash");
        assert_eq!(events[2]["result"]["result"], "hi");
        assert_eq!(events[3]["usage"]["prompt_tokens"], 10);
        assert_eq!(events[3]["usage"]["completion_tokens"], 5);
    }

    #[tokio::test]
    async fn compaction_result_does_not_complete_the_run() {
        let dir = tempfile::tempdir().unwrap();
        let bin = write_stub(
            dir.path(),
            r#"
read -r _assignment
echo '{"type":"result","subtype":"compact","result":"mid-turn compaction, ignore"}'
echo '{"type":"assistant","message":{"content":[{"type":"text","text":"back after compaction"}]}}'
echo '{"type":"result","subtype":"success","result":"final answer"}'
"#,
        );
        let (sink, _rx) = EventSink::channel();
        let outcome = executor(bin)
            .run(
                run_spec("do the thing"),
                sink,
                SteerInbox::disconnected(),
                CancellationToken::new(),
            )
            .await;
        assert_eq!(outcome.status, TerminalStatus::Completed);
        assert_eq!(outcome.result.as_deref(), Some("final answer"));
    }

    #[tokio::test]
    async fn control_request_with_no_host_denies_and_run_continues() {
        let dir = tempfile::tempdir().unwrap();
        let bin = write_stub(
            dir.path(),
            r#"
DIR="$(cd "$(dirname "$0")" && pwd)"
read -r _assignment
echo '{"type":"control_request","request_id":"r1","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"rm -rf /"}}}'
read -r control_response_line
printf '%s\n' "$control_response_line" > "$DIR/control_response.json"
echo '{"type":"result","subtype":"success","result":"continued after deny"}'
"#,
        );
        let (sink, _rx) = EventSink::channel(); // no host bridge attached
        let outcome = executor(bin.clone())
            .run(
                run_spec("do something dangerous"),
                sink,
                SteerInbox::disconnected(),
                CancellationToken::new(),
            )
            .await;
        assert_eq!(outcome.status, TerminalStatus::Completed);
        assert_eq!(outcome.result.as_deref(), Some("continued after deny"));

        let written = std::fs::read_to_string(dir.path().join("control_response.json")).unwrap();
        let value: Value = serde_json::from_str(written.trim()).unwrap();
        assert_eq!(value["response"]["request_id"], "r1");
        assert_eq!(value["response"]["response"]["behavior"], "deny");
        assert!(value["response"]["response"]["message"]
            .as_str()
            .unwrap()
            .contains("permission relay unavailable"));
    }

    #[tokio::test]
    async fn control_request_with_host_bridge_relays_and_allows() {
        let dir = tempfile::tempdir().unwrap();
        let bin = write_stub(
            dir.path(),
            r#"
DIR="$(cd "$(dirname "$0")" && pwd)"
read -r _assignment
echo '{"type":"control_request","request_id":"r1","request":{"subtype":"can_use_tool","tool_name":"Write","input":{"file_path":"/tmp/x"}}}'
read -r control_response_line
printf '%s\n' "$control_response_line" > "$DIR/control_response.json"
echo '{"type":"result","subtype":"success","result":"wrote file"}'
"#,
        );
        let (bridge, mut req_rx) = HostBridge::channel();
        let approver = tokio::spawn(async move {
            let req = req_rx.recv().await.expect("a host approval request");
            assert_eq!(req.body["tool_name"], "Write");
            let _ = req.reply.send(json!({ "approved": true }));
        });
        let (sink, _rx) = EventSink::channel();
        let sink = sink.with_host_bridge(bridge);
        let outcome = executor(bin)
            .run(
                run_spec("write a file"),
                sink,
                SteerInbox::disconnected(),
                CancellationToken::new(),
            )
            .await;
        approver.await.unwrap();
        assert_eq!(outcome.status, TerminalStatus::Completed);

        let written = std::fs::read_to_string(dir.path().join("control_response.json")).unwrap();
        let value: Value = serde_json::from_str(written.trim()).unwrap();
        assert_eq!(value["response"]["response"]["behavior"], "allow");
        assert_eq!(
            value["response"]["response"]["updatedInput"]["file_path"],
            "/tmp/x"
        );
    }

    #[tokio::test]
    async fn control_request_approval_relay_times_out_and_denies() {
        let dir = tempfile::tempdir().unwrap();
        let bin = write_stub(
            dir.path(),
            r#"
DIR="$(cd "$(dirname "$0")" && pwd)"
read -r _assignment
echo '{"type":"control_request","request_id":"r1","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"echo hi"}}}'
read -r control_response_line
printf '%s\n' "$control_response_line" > "$DIR/control_response.json"
echo '{"type":"result","subtype":"success","result":"continued after timeout"}'
"#,
        );
        let (bridge, mut req_rx) = HostBridge::channel();
        // Hold the request (and its reply oneshot::Sender) alive without ever
        // replying — exercises the "sender held open but never sent" path,
        // distinct from `approval_call`'s already-handled "reply dropped"
        // error (`control_request_with_no_host_denies_and_run_continues`
        // covers the no-bridge-at-all case; this is the bridge-present,
        // never-answers case).
        let held = tokio::spawn(async move { req_rx.recv().await });
        let (sink, _rx) = EventSink::channel();
        let sink = sink.with_host_bridge(bridge);
        let outcome = executor(bin)
            .with_relay_timeout_for_test(Duration::from_millis(50))
            .run(
                run_spec("do something"),
                sink,
                SteerInbox::disconnected(),
                CancellationToken::new(),
            )
            .await;
        assert_eq!(outcome.status, TerminalStatus::Completed);
        assert_eq!(outcome.result.as_deref(), Some("continued after timeout"));

        let written = std::fs::read_to_string(dir.path().join("control_response.json")).unwrap();
        let value: Value = serde_json::from_str(written.trim()).unwrap();
        assert_eq!(value["response"]["response"]["behavior"], "deny");
        let msg = value["response"]["response"]["message"].as_str().unwrap();
        assert!(msg.contains("timed out"), "unexpected deny message: {msg}");
        assert!(msg.contains("denying"), "unexpected deny message: {msg}");

        // Keep the reply sender alive until here (past the run's completion)
        // so the timeout path — not a dropped-sender race — is what fired.
        let _req = held.await.unwrap();
    }

    #[tokio::test]
    async fn env_allowlist_blocks_canary_secret_but_forwards_listed_var() {
        let dir = tempfile::tempdir().unwrap();
        let bin = write_stub(
            dir.path(),
            r#"
DIR="$(cd "$(dirname "$0")" && pwd)"
env > "$DIR/env-dump.txt"
read -r _assignment
echo '{"type":"result","subtype":"success","result":"ok"}'
"#,
        );
        // A secret-shaped var that must NOT reach the child (not on the fixed
        // allowlist, not in `forward_env`), and a var that MUST reach it
        // (explicitly named in `forward_env` — the billing opt-in escape
        // hatch, e.g. for ANTHROPIC_API_KEY).
        std::env::set_var("FAKE_SECRET_API_KEY", "leaked-if-broken");
        std::env::set_var("BAMBOO_TEST_FORWARD_ME", "forwarded-value");

        let exec = ClaudeCodeExecutor::new(
            Some(bin.to_string_lossy().into_owned()),
            None,
            None,
            None,
            None,
            false,
            vec!["BAMBOO_TEST_FORWARD_ME".to_string()],
        );
        let (sink, _rx) = EventSink::channel();
        let outcome = exec
            .run(
                run_spec("hi"),
                sink,
                SteerInbox::disconnected(),
                CancellationToken::new(),
            )
            .await;

        std::env::remove_var("FAKE_SECRET_API_KEY");
        std::env::remove_var("BAMBOO_TEST_FORWARD_ME");

        assert_eq!(outcome.status, TerminalStatus::Completed);
        let dump = std::fs::read_to_string(dir.path().join("env-dump.txt")).unwrap();
        assert!(
            !dump.contains("FAKE_SECRET_API_KEY"),
            "canary secret leaked into the child env:\n{dump}"
        );
        assert!(
            dump.contains("BAMBOO_TEST_FORWARD_ME=forwarded-value"),
            "forward_env-listed var missing from the child env:\n{dump}"
        );
        // Sanity: PATH (fixed allowlist) must still be present — otherwise
        // the stub couldn't have run `env`/`cat`-family commands at all, and
        // this test would be vacuously passing.
        assert!(dump.contains("PATH="), "PATH missing from the child env");
    }

    #[tokio::test]
    async fn cancel_kills_the_child_process_group() {
        let dir = tempfile::tempdir().unwrap();
        let bin = write_stub(
            dir.path(),
            r#"
read -r _assignment
echo '{"type":"system","session_id":"s1"}'
sleep 30
echo '{"type":"result","subtype":"success","result":"too late"}'
"#,
        );
        let (sink, _rx) = EventSink::channel();
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let run = tokio::spawn(async move {
            executor(bin)
                .run(
                    run_spec("a long task"),
                    sink,
                    SteerInbox::disconnected(),
                    cancel_clone,
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        cancel.cancel();

        let outcome = tokio::time::timeout(Duration::from_secs(15), run)
            .await
            .expect("run finished within the shutdown bound")
            .unwrap();
        assert_eq!(outcome.status, TerminalStatus::Cancelled);
    }

    #[tokio::test]
    async fn oversized_single_stdout_line_parses() {
        let dir = tempfile::tempdir().unwrap();
        // Build a >100KB single-line `assistant` frame plus a `result` frame,
        // written from a small python-free shell using `yes`/`head` to avoid
        // depending on any interpreter beyond POSIX sh + coreutils.
        let big_text = "x".repeat(150_000);
        let script = format!(
            r#"
read -r _assignment
echo '{{"type":"assistant","message":{{"content":[{{"type":"text","text":"{big_text}"}}]}}}}'
echo '{{"type":"result","subtype":"success","result":"ok"}}'
"#
        );
        let bin = write_stub(dir.path(), &script);
        let (sink, mut rx) = EventSink::channel();
        let outcome = executor(bin)
            .run(
                run_spec("emit a huge line"),
                sink,
                SteerInbox::disconnected(),
                CancellationToken::new(),
            )
            .await;
        assert_eq!(outcome.status, TerminalStatus::Completed);
        assert_eq!(outcome.result.as_deref(), Some("ok"));

        let mut saw_big_token = false;
        while let Ok(e) = rx.try_recv() {
            if e["type"] == "token" {
                assert_eq!(e["content"].as_str().unwrap().len(), 150_000);
                saw_big_token = true;
            }
        }
        assert!(saw_big_token, "expected the oversized token event");
    }

    #[tokio::test]
    async fn missing_binary_errors_without_hanging() {
        let (sink, _rx) = EventSink::channel();
        let outcome = ClaudeCodeExecutor::new(
            Some("/nonexistent/definitely-not-claude".into()),
            None,
            None,
            None,
            None,
            false,
            Vec::new(),
        )
        .run(
            run_spec("hi"),
            sink,
            SteerInbox::disconnected(),
            CancellationToken::new(),
        )
        .await;
        assert_eq!(outcome.status, TerminalStatus::Error);
        assert!(outcome.error.unwrap().contains("spawn"));
    }

    #[test]
    fn truncate_chars_caps_and_reports_dropped_count() {
        let long = "a".repeat(50);
        let out = truncate_chars(&long, 10);
        assert!(out.starts_with(&"a".repeat(10)));
        assert!(out.contains("40 more chars"));
        assert_eq!(truncate_chars("short", 10), "short");
    }

    #[test]
    fn tool_result_text_flattens_string_and_block_array() {
        assert_eq!(tool_result_text(Some(&json!("plain"))), "plain".to_string());
        assert_eq!(
            tool_result_text(Some(
                &json!([{"type":"text","text":"a"},{"type":"text","text":"b"}])
            )),
            "a\nb".to_string()
        );
        assert_eq!(tool_result_text(None), "".to_string());
    }

    // ---- issue #444: session resume across activations ----

    /// Stub that: (1) echoes its full argv into `argv-<N>.txt` (N = 1-based
    /// invocation counter, tracked via a `count` file so the SAME stub binary
    /// can be reused across sequential `run()` calls on one executor, just
    /// like the real `claude` binary is one binary reused across activations)
    /// and (2) echoes its first stdin line into `stdin-<N>.txt`, then emits a
    /// `system`+`result` pair whose `session_id` depends on N.
    const MULTI_RUN_STUB: &str = r#"
DIR="$(cd "$(dirname "$0")" && pwd)"
N=$(cat "$DIR/count" 2>/dev/null || echo 0)
N=$((N+1))
echo "$N" > "$DIR/count"
printf '%s\n' "$@" > "$DIR/argv-$N.txt"
read -r line
printf '%s\n' "$line" > "$DIR/stdin-$N.txt"
if [ "$N" = "1" ]; then
  echo '{"type":"system","session_id":"s-1"}'
  echo '{"type":"result","subtype":"success","result":"first"}'
elif [ "$N" = "2" ]; then
  echo '{"type":"system","session_id":"s-2"}'
  echo '{"type":"result","subtype":"success","result":"second"}'
else
  echo '{"type":"result","subtype":"success","result":"third"}'
fi
"#;

    #[tokio::test]
    async fn resume_state_written_reused_and_cleared_across_activations() {
        let bin_dir = tempfile::tempdir().unwrap();
        let state_dir = tempfile::tempdir().unwrap();
        let bin = write_stub(bin_dir.path(), MULTI_RUN_STUB);
        let exec = executor_with_state(bin, Some(state_dir.path().to_path_buf()), None);
        let state_path = state_dir.path().join("claude-code-session.json");

        // Run 1 (messages empty): fresh session, no --resume; state file
        // written from the `system` frame's session_id.
        let (sink, _rx) = EventSink::channel();
        let outcome = exec
            .run(
                run_spec("task one"),
                sink,
                SteerInbox::disconnected(),
                CancellationToken::new(),
            )
            .await;
        assert_eq!(outcome.status, TerminalStatus::Completed);
        assert!(!read_argv(bin_dir.path(), "argv-1.txt").contains("--resume"));
        let state: Value =
            serde_json::from_str(&std::fs::read_to_string(&state_path).unwrap()).unwrap();
        assert_eq!(state["session_id"], "s-1");

        // Run 2 (messages non-empty, same executor/state dir): resumes s-1;
        // stub assigns a NEW id s-2, which rewrites the state file. The
        // resumed turn sends only the live assignment (the CLI already has
        // the transcript) — no rehydrated preamble.
        let (sink, _rx) = EventSink::channel();
        let messages = vec![msg("user", "task one"), msg("assistant", "did it")];
        let outcome = exec
            .run(
                run_spec_with_messages("task two", messages),
                sink,
                SteerInbox::disconnected(),
                CancellationToken::new(),
            )
            .await;
        assert_eq!(outcome.status, TerminalStatus::Completed);
        let argv2 = read_argv(bin_dir.path(), "argv-2.txt");
        assert!(argv2.contains("--resume"));
        assert!(argv2.contains("s-1"));
        let state: Value =
            serde_json::from_str(&std::fs::read_to_string(&state_path).unwrap()).unwrap();
        assert_eq!(state["session_id"], "s-2");
        assert_eq!(stdin_body(bin_dir.path(), "stdin-2.txt"), "task two");

        // Run 3 (messages empty again): the stale state is deleted BEFORE
        // spawn (no accidental resume on a plain rerun), and this stub
        // invocation reports no session id at all — so the file stays gone.
        let (sink, _rx) = EventSink::channel();
        let outcome = exec
            .run(
                run_spec("task three"),
                sink,
                SteerInbox::disconnected(),
                CancellationToken::new(),
            )
            .await;
        assert_eq!(outcome.status, TerminalStatus::Completed);
        assert!(!read_argv(bin_dir.path(), "argv-3.txt").contains("--resume"));
        assert!(!state_path.exists());
    }

    #[tokio::test]
    async fn fallback_rehydration_renders_preamble_without_state() {
        let bin_dir = tempfile::tempdir().unwrap();
        let state_dir = tempfile::tempdir().unwrap();
        let bin = write_stub(
            bin_dir.path(),
            r#"
DIR="$(cd "$(dirname "$0")" && pwd)"
printf '%s\n' "$@" > "$DIR/argv.txt"
read -r line
printf '%s\n' "$line" > "$DIR/stdin.txt"
echo '{"type":"result","subtype":"success","result":"ok"}'
"#,
        );
        let exec = executor_with_state(bin, Some(state_dir.path().to_path_buf()), None);
        let messages = vec![
            msg("user", "please do X"),
            msg("assistant", "sure, doing X"),
            // Trailing user message duplicating the assignment — must be
            // excluded from the rendered preamble.
            msg("user", "continue"),
        ];
        let (sink, _rx) = EventSink::channel();
        let outcome = exec
            .run(
                run_spec_with_messages("continue", messages),
                sink,
                SteerInbox::disconnected(),
                CancellationToken::new(),
            )
            .await;
        assert_eq!(outcome.status, TerminalStatus::Completed);
        assert!(!read_argv(bin_dir.path(), "argv.txt").contains("--resume"));
        let body = stdin_body(bin_dir.path(), "stdin.txt");
        assert!(body.contains("## Prior conversation (rehydrated)"));
        assert!(body.contains("please do X"));
        assert!(body.contains("## Current task"));
        assert!(!body.contains("truncated"));
        // "continue" must appear exactly once (under "Current task"), not
        // duplicated by an un-deduplicated trailing history entry.
        assert_eq!(body.matches("continue").count(), 1);
    }

    #[tokio::test]
    async fn fallback_rehydration_truncates_over_message_cap() {
        let bin_dir = tempfile::tempdir().unwrap();
        let state_dir = tempfile::tempdir().unwrap();
        let bin = write_stub(
            bin_dir.path(),
            r#"
DIR="$(cd "$(dirname "$0")" && pwd)"
read -r line
printf '%s\n' "$line" > "$DIR/stdin.txt"
echo '{"type":"result","subtype":"success","result":"ok"}'
"#,
        );
        let exec = executor_with_state(bin, Some(state_dir.path().to_path_buf()), None);
        let mut messages: Vec<Value> = (0..50)
            .map(|i| {
                let role = if i % 2 == 0 { "user" } else { "assistant" };
                msg(role, &format!("message {i}"))
            })
            .collect();
        messages.push(msg("user", "final ask"));
        let (sink, _rx) = EventSink::channel();
        let outcome = exec
            .run(
                run_spec_with_messages("final ask", messages),
                sink,
                SteerInbox::disconnected(),
                CancellationToken::new(),
            )
            .await;
        assert_eq!(outcome.status, TerminalStatus::Completed);
        let body = stdin_body(bin_dir.path(), "stdin.txt");
        assert!(body.contains("truncated"));
        // Oldest (message-count cap) dropped first; most recent retained.
        assert!(!body.contains("message 0"));
        assert!(body.contains("message 49"));
    }

    #[tokio::test]
    async fn fallback_rehydration_truncates_oversized_single_message() {
        let bin_dir = tempfile::tempdir().unwrap();
        let state_dir = tempfile::tempdir().unwrap();
        let bin = write_stub(
            bin_dir.path(),
            r#"
DIR="$(cd "$(dirname "$0")" && pwd)"
read -r line
printf '%s\n' "$line" > "$DIR/stdin.txt"
echo '{"type":"result","subtype":"success","result":"ok"}'
"#,
        );
        let exec = executor_with_state(bin, Some(state_dir.path().to_path_buf()), None);
        let huge = "x".repeat(30_000);
        let messages = vec![msg("user", &huge), msg("user", "current ask")];
        let (sink, _rx) = EventSink::channel();
        let outcome = exec
            .run(
                run_spec_with_messages("current ask", messages),
                sink,
                SteerInbox::disconnected(),
                CancellationToken::new(),
            )
            .await;
        assert_eq!(outcome.status, TerminalStatus::Completed);
        let body = stdin_body(bin_dir.path(), "stdin.txt");
        assert!(body.contains("truncated"));
        assert!(body.len() < huge.len());
    }

    #[tokio::test]
    async fn workspace_mismatch_state_treated_as_unusable_falls_back() {
        let bin_dir = tempfile::tempdir().unwrap();
        let state_dir = tempfile::tempdir().unwrap();
        // A real directory — `build_command` does `cmd.current_dir(workspace)`,
        // which fails the spawn outright if it doesn't exist on disk.
        let workspace_dir = tempfile::tempdir().unwrap();
        std::fs::write(
            state_dir.path().join("claude-code-session.json"),
            serde_json::to_vec(&json!({
                "session_id": "stale-id",
                "workspace": "/some/other/workspace",
                "updated_at": chrono::Utc::now(),
            }))
            .unwrap(),
        )
        .unwrap();
        let bin = write_stub(
            bin_dir.path(),
            r#"
DIR="$(cd "$(dirname "$0")" && pwd)"
printf '%s\n' "$@" > "$DIR/argv.txt"
read -r _line
echo '{"type":"result","subtype":"success","result":"ok"}'
"#,
        );
        let exec = executor_with_state(
            bin,
            Some(state_dir.path().to_path_buf()),
            Some(workspace_dir.path().to_string_lossy().into_owned()),
        );
        let messages = vec![msg("user", "hi"), msg("user", "go")];
        let (sink, _rx) = EventSink::channel();
        let outcome = exec
            .run(
                run_spec_with_messages("go", messages),
                sink,
                SteerInbox::disconnected(),
                CancellationToken::new(),
            )
            .await;
        assert_eq!(outcome.status, TerminalStatus::Completed);
        let argv = read_argv(bin_dir.path(), "argv.txt");
        assert!(!argv.contains("--resume"));
        assert!(!argv.contains("stale-id"));
    }

    #[tokio::test]
    async fn resume_failure_retries_once_with_fallback_history() {
        let bin_dir = tempfile::tempdir().unwrap();
        let state_dir = tempfile::tempdir().unwrap();
        std::fs::write(
            state_dir.path().join("claude-code-session.json"),
            serde_json::to_vec(&json!({
                "session_id": "dead-id",
                "workspace": null,
                "updated_at": chrono::Utc::now(),
            }))
            .unwrap(),
        )
        .unwrap();
        let bin = write_stub(
            bin_dir.path(),
            r#"
DIR="$(cd "$(dirname "$0")" && pwd)"
N=$(cat "$DIR/count" 2>/dev/null || echo 0)
N=$((N+1))
echo "$N" > "$DIR/count"
printf '%s\n' "$@" > "$DIR/argv-$N.txt"
case "$*" in
  *--resume*)
    exit 1
    ;;
  *)
    read -r line
    printf '%s\n' "$line" > "$DIR/stdin-$N.txt"
    echo '{"type":"system","session_id":"s-fresh"}'
    echo '{"type":"result","subtype":"success","result":"recovered"}'
    ;;
esac
"#,
        );
        let exec = executor_with_state(bin, Some(state_dir.path().to_path_buf()), None);
        let messages = vec![
            msg("user", "earlier context"),
            msg("user", "please continue"),
        ];
        let (sink, _rx) = EventSink::channel();
        let outcome = exec
            .run(
                run_spec_with_messages("please continue", messages),
                sink,
                SteerInbox::disconnected(),
                CancellationToken::new(),
            )
            .await;
        assert_eq!(outcome.status, TerminalStatus::Completed);
        assert_eq!(outcome.result.as_deref(), Some("recovered"));
        assert_eq!(read_argv(bin_dir.path(), "count").trim(), "2");

        let argv1 = read_argv(bin_dir.path(), "argv-1.txt");
        assert!(argv1.contains("--resume"));
        assert!(argv1.contains("dead-id"));
        let argv2 = read_argv(bin_dir.path(), "argv-2.txt");
        assert!(!argv2.contains("--resume"));
        let body2 = stdin_body(bin_dir.path(), "stdin-2.txt");
        assert!(body2.contains("## Prior conversation (rehydrated)"));
        assert!(body2.contains("earlier context"));

        // The retry rewrites the state file from the fallback attempt's own
        // `system` frame, parses cleanly (atomic tmp+rename), and no leftover
        // tmp files remain in the state dir.
        let state: Value = serde_json::from_str(
            &std::fs::read_to_string(state_dir.path().join("claude-code-session.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(state["session_id"], "s-fresh");
        let leftover_tmp = std::fs::read_dir(state_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .any(|e| e.file_name().to_string_lossy().contains(".tmp"));
        assert!(!leftover_tmp);
    }
}
