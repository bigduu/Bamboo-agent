//! Process-owned browser sessions shared by HTTP workbench requests and agent tools.
//! Each chat session has one isolated Playwright context in a child Node process.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use base64::Engine as _;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{oneshot, watch, Mutex as AsyncMutex};

const BROWSER_IDLE_TTL: Duration = Duration::from_secs(15 * 60);
const BROWSER_IDLE_SWEEP_INTERVAL: Duration = Duration::from_secs(60);
const BROWSER_COMMAND_DEADLINE: Duration = Duration::from_secs(30);
const BROWSER_EVAL_DEADLINE: Duration = Duration::from_secs(5);
const BROWSER_REAP_DEADLINE: Duration = Duration::from_secs(2);

#[derive(Debug, thiserror::Error)]
pub enum BrowserError {
    #[error("browser runtime unavailable: {0}")]
    Unavailable(String),
    #[error("browser session not open")]
    NotOpen,
    #[error("browser page changed; refresh state and retry")]
    StaleEpoch,
    #[error("browser dialog is no longer pending")]
    StaleDialog,
    #[error("answer the pending browser dialog first; use browser tabs to read its dialog_id and page_epoch")]
    DialogPending,
    #[error("invalid browser request: {0}")]
    Invalid(String),
    #[error("browser action failed: {0}")]
    Failed(String),
}

#[derive(Clone, Debug)]
pub struct BrowserFrame {
    pub tab_id: String,
    pub page_epoch: u64,
    pub frame_seq: u64,
    pub viewport_width: u32,
    pub viewport_height: u32,
    pub jpeg: Arc<[u8]>,
}

#[derive(Clone, Default)]
pub struct BrowserManager {
    sessions: Arc<AsyncMutex<HashMap<String, Arc<BrowserSession>>>>,
    session_gates: Arc<Mutex<HashMap<String, Weak<AsyncMutex<()>>>>>,
    retired: Arc<Mutex<HashSet<String>>>,
    cleanup_started: Arc<AtomicBool>,
}

struct BrowserSession {
    child: Mutex<Child>,
    /// Owns the host's TMPDIR, including Chromium's download scratch files.
    temp_dir: Mutex<Option<tempfile::TempDir>>,
    #[cfg(unix)]
    process_group: Option<i32>,
    stdin: AsyncMutex<ChildStdin>,
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<Value, BrowserError>>>>,
    next_id: AtomicU64,
    frames: watch::Sender<Option<Arc<BrowserFrame>>>,
    shutdown: watch::Sender<bool>,
    termination: AsyncMutex<()>,
    kill_issued: AtomicBool,
    reaped: AtomicBool,
    frame_epoch: AtomicU64,
    active_tab_id: Mutex<Option<String>>,
    alive: AtomicBool,
    last_used: Mutex<Instant>,
}

impl Drop for BrowserSession {
    fn drop(&mut self) {
        if !self.reaped.load(Ordering::Acquire) {
            self.signal_stop();
        }
    }
}

#[cfg(unix)]
fn kill_browser_process_group(group: i32) {
    // A browser host starts in its own process group. Never signal Bamboo's
    // group if a platform unexpectedly failed to apply that setting.
    if group > 0 && group != unsafe { libc::getpgrp() } {
        let _ = unsafe { libc::killpg(group, libc::SIGKILL) };
    }
}

/// A cancelled HTTP/tool caller must not cancel the host deadline with it.
/// A queued request can otherwise run later on a page nobody is waiting for.
struct BrowserCallAbortGuard {
    session: Arc<BrowserSession>,
    managed: Option<(BrowserManager, String)>,
    armed: bool,
}

/// Read-only UI polls may be cancelled as a panel hides without retiring the
/// shared page. An unknown action remains guarded until explicitly classified.
fn action_may_mutate_browser(action: &str) -> bool {
    !matches!(action, "state" | "tab_list" | "dom" | "screenshot")
}

/// A cancelled read must not leave a sender in the host's pending map.
struct BrowserPendingCallGuard<'a> {
    session: &'a BrowserSession,
    id: u64,
}

impl Drop for BrowserPendingCallGuard<'_> {
    fn drop(&mut self) {
        self.session.pending.lock().unwrap().remove(&self.id);
    }
}

impl BrowserCallAbortGuard {
    fn new(session: Arc<BrowserSession>, managed: Option<(BrowserManager, String)>) -> Self {
        Self {
            session,
            managed,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for BrowserCallAbortGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.session.mark_dead("browser command cancelled");
        self.session.signal_stop();
        let session = self.session.clone();
        let managed = self.managed.take();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                session.terminate("browser command cancelled").await;
                if let Some((browser, session_id)) = managed {
                    browser.remove_if_same(&session_id, &session).await;
                }
            });
        }
    }
}

fn host_script() -> PathBuf {
    std::env::var_os("BAMBOO_BROWSER_HOST_SCRIPT")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../browser-runtime/host.cjs")
        })
}

fn restrict_host_environment(command: &mut Command) {
    // The browser loads untrusted websites. Provider credentials and Bamboo's
    // wider process environment must not be inherited by Node or Chromium.
    command.env_clear();
    for name in ["PATH", "HOME", "TMPDIR", "LANG", "LC_ALL"] {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
    for name in ["BAMBOO_BROWSER_EXECUTABLE", "PLAYWRIGHT_BROWSERS_PATH"] {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
}

impl BrowserSession {
    async fn spawn() -> Result<Arc<Self>, BrowserError> {
        let script = host_script();
        if !script.is_file() {
            return Err(BrowserError::Unavailable(format!(
                "host script not found at {} (set BAMBOO_BROWSER_HOST_SCRIPT)",
                script.display()
            )));
        }
        let node = std::env::var_os("BAMBOO_BROWSER_NODE").unwrap_or_else(|| "node".into());
        let mut command = Command::new(node);
        restrict_host_environment(&mut command);
        let temp_dir = tempfile::Builder::new()
            .prefix("bamboo-browser-session-")
            .tempdir()
            .map_err(|_| {
                BrowserError::Unavailable("unable to create browser temporary directory".into())
            })?;
        command.env("TMPDIR", temp_dir.path());
        command
            .arg(script)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        let child = command
            .spawn()
            .map_err(|error| BrowserError::Unavailable(error.to_string()))?;
        let session = Self::from_child(child, Some(temp_dir))?;
        let mut abort_guard = BrowserCallAbortGuard::new(session.clone(), None);
        // The first state request doubles as a readiness probe. A missing
        // Playwright package or Chromium binary fails here, not on first click.
        let readiness = session.call("state", json!({})).await;
        if readiness.is_err() {
            session.terminate("browser host unavailable").await;
        }
        abort_guard.disarm();
        readiness?;
        Ok(session)
    }

    fn from_child(
        mut child: Child,
        temp_dir: Option<tempfile::TempDir>,
    ) -> Result<Arc<Self>, BrowserError> {
        #[cfg(unix)]
        let process_group = child.id().and_then(|id| i32::try_from(id).ok());
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| BrowserError::Unavailable("browser host stdin not available".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| BrowserError::Unavailable("browser host stdout not available".into()))?;
        let (frames, _) = watch::channel(None);
        let (shutdown, _) = watch::channel(false);
        let session = Arc::new(Self {
            child: Mutex::new(child),
            temp_dir: Mutex::new(temp_dir),
            #[cfg(unix)]
            process_group,
            stdin: AsyncMutex::new(stdin),
            pending: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            frames,
            shutdown,
            termination: AsyncMutex::new(()),
            kill_issued: AtomicBool::new(false),
            reaped: AtomicBool::new(false),
            frame_epoch: AtomicU64::new(0),
            active_tab_id: Mutex::new(None),
            alive: AtomicBool::new(true),
            last_used: Mutex::new(Instant::now()),
        });
        let weak = Arc::downgrade(&session);
        tokio::spawn(async move { Self::read_messages(weak, stdout).await });
        Ok(session)
    }

    async fn read_messages(weak: Weak<Self>, stdout: tokio::process::ChildStdout) {
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let Some(session) = weak.upgrade() else { break };
            if !session.alive.load(Ordering::Acquire) {
                break;
            }
            let Ok(message) = serde_json::from_str::<Value>(&line) else {
                tracing::warn!("browser host sent malformed JSON");
                continue;
            };
            if message.get("event").and_then(Value::as_str) == Some("frame") {
                session.accept_frame(&message);
                continue;
            }
            if message.get("event").and_then(Value::as_str) == Some("frame_reset") {
                session.accept_frame_reset(&message);
                continue;
            }
            let Some(id) = message.get("id").and_then(Value::as_u64) else {
                continue;
            };
            let sender = session.pending.lock().unwrap().remove(&id);
            if let Some(sender) = sender {
                let result = if message.get("ok").and_then(Value::as_bool) == Some(true) {
                    Ok(message.get("result").cloned().unwrap_or(Value::Null))
                } else {
                    let error = message
                        .get("error")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown error");
                    Err(match message.get("code").and_then(Value::as_str) {
                        Some("stale_epoch") => BrowserError::StaleEpoch,
                        Some("stale_dialog") => BrowserError::StaleDialog,
                        Some("dialog_pending") => BrowserError::DialogPending,
                        Some("invalid_url" | "invalid_request") => {
                            BrowserError::Invalid(error.to_string())
                        }
                        _ => BrowserError::Failed(error.to_string()),
                    })
                };
                let _ = sender.send(result);
            }
        }
        if let Some(session) = weak.upgrade() {
            session.terminate("browser host exited").await;
        }
    }

    fn accept_frame(&self, value: &Value) {
        if !self.alive.load(Ordering::Acquire) {
            return;
        }
        let Some(tab_id) = value.get("active_tab_id").and_then(Value::as_str) else {
            return;
        };
        if self.active_tab_id.lock().unwrap().as_deref() != Some(tab_id) {
            return;
        }
        let Some(data) = value.get("data").and_then(Value::as_str) else {
            return;
        };
        let Ok(jpeg) = base64::engine::general_purpose::STANDARD.decode(data) else {
            return;
        };
        let Some(page_epoch) = value.get("page_epoch").and_then(Value::as_u64) else {
            return;
        };
        if page_epoch < self.frame_epoch.load(Ordering::Acquire) {
            return;
        }
        let Some(frame_seq) = value.get("frame_seq").and_then(Value::as_u64) else {
            return;
        };
        let Some(viewport_width) = value.get("viewport_width").and_then(Value::as_u64) else {
            return;
        };
        let Some(viewport_height) = value.get("viewport_height").and_then(Value::as_u64) else {
            return;
        };
        let Ok(viewport_width) = u32::try_from(viewport_width) else {
            return;
        };
        let Ok(viewport_height) = u32::try_from(viewport_height) else {
            return;
        };
        self.frame_epoch.store(page_epoch, Ordering::Release);
        self.frames.send_replace(Some(Arc::new(BrowserFrame {
            tab_id: tab_id.to_string(),
            page_epoch,
            frame_seq,
            viewport_width,
            viewport_height,
            jpeg: jpeg.into(),
        })));
    }

    fn accept_frame_reset(&self, value: &Value) {
        if !self.alive.load(Ordering::Acquire) {
            return;
        }
        let Some(page_epoch) = value.get("page_epoch").and_then(Value::as_u64) else {
            return;
        };
        if page_epoch < self.frame_epoch.load(Ordering::Acquire) {
            return;
        }
        self.frame_epoch.store(page_epoch, Ordering::Release);
        let tab_id = value
            .get("active_tab_id")
            .and_then(Value::as_str)
            .map(str::to_string);
        *self.active_tab_id.lock().unwrap() = tab_id;
        self.frames.send_replace(None);
    }

    fn touch(&self) {
        *self.last_used.lock().unwrap() = Instant::now();
    }

    fn idle_for(&self, ttl: Duration) -> bool {
        self.last_used.lock().unwrap().elapsed() >= ttl
    }

    fn mark_dead(&self, reason: &'static str) {
        if !self.alive.swap(false, Ordering::AcqRel) {
            return;
        }
        self.shutdown.send_replace(true);
        self.frames.send_replace(None);
        *self.active_tab_id.lock().unwrap() = None;
        let pending = std::mem::take(&mut *self.pending.lock().unwrap());
        for (_, sender) in pending {
            let _ = sender.send(Err(BrowserError::Unavailable(reason.into())));
        }
    }

    fn signal_stop(&self) {
        if self.kill_issued.swap(true, Ordering::AcqRel) {
            return;
        }
        #[cfg(unix)]
        if let Some(group) = self.process_group {
            kill_browser_process_group(group);
        }
        // `kill_on_drop` remains a fallback if Bamboo's runtime shuts down
        // before asynchronous reaping can finish.
        if let Ok(mut child) = self.child.lock() {
            let _ = child.start_kill();
        }
    }

    async fn terminate(&self, reason: &'static str) {
        self.mark_dead(reason);
        let _guard = self.termination.lock().await;
        if self.reaped.load(Ordering::Acquire) {
            self.cleanup_temp_dir();
            return;
        }
        self.signal_stop();
        let reaped = tokio::time::timeout(BROWSER_REAP_DEADLINE, async {
            loop {
                let status = self.child.lock().unwrap().try_wait();
                match status {
                    Ok(Some(_)) => return true,
                    Ok(None) => tokio::time::sleep(Duration::from_millis(10)).await,
                    Err(error) => {
                        tracing::warn!(%error, "failed to reap browser host");
                        return false;
                    }
                }
            }
        })
        .await;
        if reaped == Ok(true) {
            self.reaped.store(true, Ordering::Release);
        } else {
            tracing::warn!("browser host did not exit after process termination");
        }
        self.cleanup_temp_dir();
    }

    fn cleanup_temp_dir(&self) {
        if let Some(temp_dir) = self.temp_dir.lock().unwrap().take() {
            if temp_dir.close().is_err() {
                tracing::warn!("failed to remove browser temporary directory");
            }
        }
    }

    async fn call(&self, action: &str, args: Value) -> Result<Value, BrowserError> {
        self.call_with_deadline(action, args, BROWSER_COMMAND_DEADLINE)
            .await
    }

    async fn call_with_deadline(
        &self,
        action: &str,
        args: Value,
        deadline: Duration,
    ) -> Result<Value, BrowserError> {
        if !self.alive.load(Ordering::Acquire) {
            return Err(BrowserError::Unavailable("browser host exited".into()));
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let line = serde_json::to_vec(&json!({"id": id, "action": action, "args": args}))
            .map_err(|error| BrowserError::Invalid(error.to_string()))?;
        let (sender, receiver) = oneshot::channel();
        let mut shutdown = self.shutdown.subscribe();
        {
            let mut pending = self.pending.lock().unwrap();
            if !self.alive.load(Ordering::Acquire) {
                return Err(BrowserError::Unavailable("browser host exited".into()));
            }
            pending.insert(id, sender);
        }
        let _pending_guard = BrowserPendingCallGuard { session: self, id };
        let exchange = async {
            let mut stdin = self.stdin.lock().await;
            if !self.alive.load(Ordering::Acquire) {
                return Err(BrowserError::Unavailable("browser host exited".into()));
            }
            stdin
                .write_all(&line)
                .await
                .map_err(|error| BrowserError::Unavailable(error.to_string()))?;
            stdin
                .write_all(b"\n")
                .await
                .map_err(|error| BrowserError::Unavailable(error.to_string()))?;
            stdin
                .flush()
                .await
                .map_err(|error| BrowserError::Unavailable(error.to_string()))?;
            drop(stdin);
            receiver
                .await
                .map_err(|_| BrowserError::Unavailable("browser host exited".into()))?
        };
        let result = tokio::select! {
            biased;
            outcome = tokio::time::timeout(deadline, exchange) => outcome,
            _ = shutdown.changed() => return Err(BrowserError::Unavailable("browser host exited".into())),
        };
        match result {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => {
                if matches!(error, BrowserError::Unavailable(_)) {
                    self.terminate("browser host unavailable").await;
                }
                Err(error)
            }
            Err(_) => {
                self.mark_dead("browser host timed out");
                self.terminate("browser host timed out").await;
                Err(BrowserError::Failed("browser action timed out".into()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_known_read_actions_survive_caller_cancellation() {
        for action in ["state", "tab_list", "dom", "screenshot"] {
            assert!(!action_may_mutate_browser(action), "{action}");
        }
        for action in [
            "navigate",
            "history",
            "viewport",
            "input",
            "click_selector",
            "hover_selector",
            "hover_at",
            "drag_selector",
            "drag_at",
            "fill_selector",
            "press_selector",
            "dialog_respond",
            "tab_create",
            "tab_activate",
            "tab_close",
            "eval",
            "future_action",
        ] {
            assert!(action_may_mutate_browser(action), "{action}");
        }
    }

    #[cfg(unix)]
    fn stub_session(idle_for: Duration, alive: bool) -> Arc<BrowserSession> {
        let mut command = Command::new("/bin/sleep");
        command
            .arg("60")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .kill_on_drop(true);
        command.process_group(0);
        let mut child = command.spawn().unwrap();
        let process_group = child.id().and_then(|id| i32::try_from(id).ok());
        let stdin = child.stdin.take().unwrap();
        let (frames, _) = watch::channel(None);
        let (shutdown, _) = watch::channel(!alive);
        Arc::new(BrowserSession {
            child: Mutex::new(child),
            temp_dir: Mutex::new(None),
            process_group,
            stdin: AsyncMutex::new(stdin),
            pending: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            frames,
            shutdown,
            termination: AsyncMutex::new(()),
            kill_issued: AtomicBool::new(false),
            reaped: AtomicBool::new(false),
            frame_epoch: AtomicU64::new(0),
            active_tab_id: Mutex::new(None),
            alive: AtomicBool::new(alive),
            last_used: Mutex::new(Instant::now() - idle_for),
        })
    }

    #[cfg(unix)]
    fn fake_host(script: &str, paths: &[(&str, &std::path::Path)]) -> Arc<BrowserSession> {
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg(script)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        for (name, path) in paths {
            command.env(name, path);
        }
        command.process_group(0);
        BrowserSession::from_child(command.spawn().unwrap(), None).unwrap()
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancelling_a_read_discards_its_pending_reply_but_preserves_the_shared_host() {
        for via_open in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let read_marker = directory.path().join("first-read");
            let session = fake_host(
                r#"IFS= read -r first; : > "$READ_MARKER_FILE"; IFS= read -r second; printf '%s\n' '{"id":2,"ok":true,"result":{"page_epoch":7}}'; sleep 60"#,
                &[("READ_MARKER_FILE", &read_marker)],
            );
            let browser = BrowserManager::default();
            browser
                .sessions
                .lock()
                .await
                .insert("shared-chat".into(), session.clone());
            let cancelled = {
                let browser = browser.clone();
                tokio::spawn(async move {
                    if via_open {
                        browser.open("shared-chat").await
                    } else {
                        browser.state("shared-chat").await
                    }
                })
            };
            tokio::time::timeout(Duration::from_secs(1), async {
                while !read_marker.exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("the read should reach the host before cancellation");
            cancelled.abort();
            assert!(cancelled.await.unwrap_err().is_cancelled());
            assert!(session.alive.load(Ordering::Acquire));
            assert!(session.pending.lock().unwrap().is_empty());
            assert!(Arc::ptr_eq(
                browser.sessions.lock().await.get("shared-chat").unwrap(),
                &session
            ));
            let state = tokio::time::timeout(Duration::from_secs(1), browser.state("shared-chat"))
                .await
                .expect("a subsequent read should complete on the same host")
                .unwrap();
            assert_eq!(state["page_epoch"], 7);
            browser.close("shared-chat").await.unwrap();
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn deadline_covers_stdin_wait_and_wakes_pending_without_blocking_other_chat() {
        let browser = BrowserManager::default();
        let hung = fake_host("IFS= read -r request; sleep 60", &[]);
        let other = fake_host(
            r#"IFS= read -r request; printf '%s\n' '{"id":1,"ok":true,"result":{"page_epoch":7}}'; sleep 60"#,
            &[],
        );
        {
            let mut sessions = browser.sessions.lock().await;
            sessions.insert("hung-chat".into(), hung.clone());
            sessions.insert("other-chat".into(), other.clone());
        }
        let stdin_guard = hung.stdin.lock().await;
        let first = {
            let browser = browser.clone();
            tokio::spawn(async move {
                browser
                    .command_with_deadline(
                        "hung-chat",
                        "state",
                        json!({}),
                        Duration::from_millis(150),
                    )
                    .await
            })
        };
        let second = {
            let browser = browser.clone();
            tokio::spawn(async move {
                browser
                    .command_with_deadline("hung-chat", "dom", json!({}), Duration::from_secs(5))
                    .await
            })
        };
        let other_state = tokio::time::timeout(
            Duration::from_secs(1),
            browser.command("other-chat", "state", json!({})),
        )
        .await
        .expect("one hung chat must not block another chat")
        .unwrap();
        assert_eq!(other_state["page_epoch"], 7);
        assert!(
            matches!(first.await.unwrap(), Err(BrowserError::Failed(message)) if message.contains("timed out"))
        );
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), second)
                .await
                .unwrap()
                .unwrap(),
            Err(BrowserError::Unavailable(_))
        ));
        drop(stdin_guard);
        assert!(!browser.sessions.lock().await.contains_key("hung-chat"));
        assert!(hung.pending.lock().unwrap().is_empty());
        assert!(hung.child.lock().unwrap().try_wait().unwrap().is_some());
        browser.close("other-chat").await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn deadline_covers_a_blocked_stdin_write() {
        let browser = BrowserManager::default();
        let session = fake_host("sleep 60", &[]);
        browser
            .sessions
            .lock()
            .await
            .insert("blocked-write".into(), session.clone());
        // The fake host never reads stdin. This request exceeds the pipe
        // capacity, so write_all itself must be inside the command deadline.
        let result = browser
            .command_with_deadline(
                "blocked-write",
                "input",
                json!({"text": "x".repeat(2 * 1024 * 1024)}),
                Duration::from_millis(150),
            )
            .await;
        assert!(
            matches!(result, Err(BrowserError::Failed(message)) if message.contains("timed out"))
        );
        assert!(!browser.sessions.lock().await.contains_key("blocked-write"));
        assert!(session.child.lock().unwrap().try_wait().unwrap().is_some());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn deadline_reaps_host_descendant_and_discards_queued_action() {
        let directory = tempfile::tempdir().unwrap();
        let child_pid = directory.path().join("descendant.pid");
        let late_marker = directory.path().join("late-action");
        let session = fake_host(
            r#"sleep 60 & echo "$!" > "$CHILD_PID_FILE"; IFS= read -r first; sleep 60; IFS= read -r second; : > "$LATE_MARKER_FILE""#,
            &[
                ("CHILD_PID_FILE", &child_pid),
                ("LATE_MARKER_FILE", &late_marker),
            ],
        );
        let browser = BrowserManager::default();
        browser
            .sessions
            .lock()
            .await
            .insert("hung-chat".into(), session.clone());
        let first = {
            let browser = browser.clone();
            tokio::spawn(async move {
                browser
                    .command_with_deadline(
                        "hung-chat",
                        "state",
                        json!({}),
                        Duration::from_millis(300),
                    )
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while !child_pid.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("fake host should start a descendant");
        let second = {
            let browser = browser.clone();
            tokio::spawn(async move {
                browser
                    .command_with_deadline("hung-chat", "queued", json!({}), Duration::from_secs(5))
                    .await
            })
        };
        assert!(
            matches!(first.await.unwrap(), Err(BrowserError::Failed(message)) if message.contains("timed out"))
        );
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), second)
                .await
                .unwrap()
                .unwrap(),
            Err(BrowserError::Unavailable(_))
        ));
        assert!(!browser.sessions.lock().await.contains_key("hung-chat"));
        assert!(session.pending.lock().unwrap().is_empty());
        assert!(session.child.lock().unwrap().try_wait().unwrap().is_some());

        let pid: u32 = std::fs::read_to_string(&child_pid)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let output = Command::new("ps")
                    .args(["-o", "stat=", "-p", &pid.to_string()])
                    .output()
                    .await
                    .unwrap();
                let status = String::from_utf8_lossy(&output.stdout);
                if !output.status.success()
                    || status.trim().is_empty()
                    || status.trim().starts_with('Z')
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("host descendant should no longer run");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !late_marker.exists(),
            "retired host executed a queued action"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn retire_cancels_an_already_cloned_command_waiting_for_stdin() {
        let directory = tempfile::tempdir().unwrap();
        let late_marker = directory.path().join("late-action");
        let session = fake_host(
            r#"IFS= read -r request; : > "$LATE_MARKER_FILE"; sleep 60"#,
            &[("LATE_MARKER_FILE", &late_marker)],
        );
        let browser = BrowserManager::default();
        browser
            .sessions
            .lock()
            .await
            .insert("deleted-chat".into(), session.clone());
        let stdin_guard = session.stdin.lock().await;
        let queued = {
            let browser = browser.clone();
            tokio::spawn(async move {
                browser
                    .command_with_deadline(
                        "deleted-chat",
                        "click_selector",
                        json!({}),
                        Duration::from_secs(5),
                    )
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while session.pending.lock().unwrap().is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("command should hold a cloned session before retirement");
        browser.retire("deleted-chat").await.unwrap();
        drop(stdin_guard);
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), queued)
                .await
                .unwrap()
                .unwrap(),
            Err(BrowserError::Unavailable(_))
        ));
        assert!(session.pending.lock().unwrap().is_empty());
        assert!(!late_marker.exists());
        assert!(matches!(
            browser.open("deleted-chat").await,
            Err(BrowserError::NotOpen)
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn aborting_a_caller_retires_its_host_and_wakes_pending_commands() {
        let directory = tempfile::tempdir().unwrap();
        let read_marker = directory.path().join("first-read");
        let late_marker = directory.path().join("late-action");
        let session = fake_host(
            r#"IFS= read -r first; : > "$READ_MARKER_FILE"; sleep 60; IFS= read -r second; : > "$LATE_MARKER_FILE""#,
            &[
                ("READ_MARKER_FILE", &read_marker),
                ("LATE_MARKER_FILE", &late_marker),
            ],
        );
        let browser = BrowserManager::default();
        browser
            .sessions
            .lock()
            .await
            .insert("cancelled-chat".into(), session.clone());
        let first = {
            let browser = browser.clone();
            tokio::spawn(async move {
                browser
                    .command_with_deadline(
                        "cancelled-chat",
                        "navigate",
                        json!({}),
                        Duration::from_secs(5),
                    )
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while !read_marker.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("first request should reach the host");
        let second = {
            let browser = browser.clone();
            tokio::spawn(async move {
                browser
                    .command_with_deadline(
                        "cancelled-chat",
                        "queued",
                        json!({}),
                        Duration::from_secs(5),
                    )
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while session.pending.lock().unwrap().len() < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("second request should be pending");
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), second)
                .await
                .unwrap()
                .unwrap(),
            Err(BrowserError::Unavailable(_))
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            while browser.sessions.lock().await.contains_key("cancelled-chat") {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("cancelled host should be removed by exact identity");
        assert!(session.pending.lock().unwrap().is_empty());
        assert!(session.child.lock().unwrap().try_wait().unwrap().is_some());
        assert!(!late_marker.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancelling_input_retires_the_host_before_it_can_complete() {
        let directory = tempfile::tempdir().unwrap();
        let read_marker = directory.path().join("input-read");
        let session = fake_host(
            r#"IFS= read -r first; : > "$READ_MARKER_FILE"; sleep 60"#,
            &[("READ_MARKER_FILE", &read_marker)],
        );
        let browser = BrowserManager::default();
        browser
            .sessions
            .lock()
            .await
            .insert("input-chat".into(), session.clone());
        let input = {
            let browser = browser.clone();
            tokio::spawn(async move {
                browser
                    .command_with_deadline(
                        "input-chat",
                        "input",
                        json!({"kind":"type","text":"hello"}),
                        Duration::from_secs(5),
                    )
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while !read_marker.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("input should reach the host before cancellation");
        input.abort();
        assert!(input.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(1), async {
            while browser.sessions.lock().await.contains_key("input-chat") {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("cancelled input host should be retired");
        assert!(!session.alive.load(Ordering::Acquire));
        assert!(session.child.lock().unwrap().try_wait().unwrap().is_some());
    }

    #[cfg(unix)]
    #[tokio::test]
    #[ignore = "requires the Playwright Chromium runtime"]
    async fn chromium_temporary_download_directory_is_removed_on_close_and_timeout() {
        let unrelated = tempfile::tempdir().unwrap();
        let sentinel = unrelated.path().join("unrelated-tempdir-sentinel");
        std::fs::write(&sentinel, b"keep").unwrap();

        for (session_id, timed_out) in [
            ("close-download-temp", false),
            ("timeout-download-temp", true),
        ] {
            let browser = BrowserManager::default();
            browser.open(session_id).await.unwrap();
            let session = browser.sessions.lock().await[session_id].clone();
            let owned_temp = session
                .temp_dir
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .path()
                .to_path_buf();
            assert!(owned_temp.is_dir());
            assert!(
                std::fs::read_dir(&owned_temp).unwrap().any(|entry| entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("bamboo-browser-download-")),
                "Chromium download directory must be inside the session-owned TMPDIR"
            );

            if timed_out {
                let result = browser
                    .command_with_deadline(session_id, "state", json!({}), Duration::ZERO)
                    .await;
                assert!(
                    matches!(result, Err(BrowserError::Failed(message)) if message.contains("timed out"))
                );
                assert!(session.kill_issued.load(Ordering::Acquire));
                assert!(!browser.sessions.lock().await.contains_key(session_id));
            } else {
                browser.close(session_id).await.unwrap();
            }
            assert!(session.reaped.load(Ordering::Acquire));
            assert!(session.temp_dir.lock().unwrap().is_none());
            assert!(
                !owned_temp.exists(),
                "session close must remove its download scratch directory"
            );
            assert!(
                sentinel.is_file(),
                "another temporary directory must remain untouched"
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    #[ignore = "requires the Playwright Chromium runtime"]
    async fn slow_pointer_action_returns_before_host_deadline_without_restarting_session() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let fixture = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut request = [0u8; 2048];
                    let length = socket.read(&mut request).await.unwrap_or(0);
                    let slow = request[..length].starts_with(b"GET /slow ");
                    if slow {
                        tokio::time::sleep(Duration::from_secs(19)).await;
                    }
                    let body: &[u8] = if slow {
                        b"<main>slow destination</main>"
                    } else {
                        br#"<script>setTimeout(() => {
                            const button = document.createElement('button');
                            button.id = 'late-hover'; button.hidden = true;
                            button.onpointerenter = () => { location.href = '/slow' };
                            document.body.append(button);
                            setTimeout(() => { button.hidden = false }, 8000);
                        }, 8000)</script>"#
                    };
                    let headers = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = socket.write_all(headers.as_bytes()).await;
                    let _ = socket.write_all(body).await;
                });
            }
        });

        let browser = BrowserManager::default();
        let opened = browser.open("slow-pointer-chat").await.unwrap();
        let ready = browser
            .command(
                "slow-pointer-chat",
                "navigate",
                json!({"url": url, "expected_epoch": opened["page_epoch"]}),
            )
            .await
            .unwrap();
        let original = browser.sessions.lock().await["slow-pointer-chat"].clone();
        let started = Instant::now();
        let result = browser
            .command(
                "slow-pointer-chat",
                "hover_selector",
                json!({"selector": "#late-hover", "expected_epoch": ready["page_epoch"]}),
            )
            .await;
        assert!(
            matches!(&result, Err(BrowserError::Failed(message)) if message.contains("browser pointer action timed out")),
            "pointer action should report its own budget before Rust retires the host: {result:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(28));
        let current = browser.sessions.lock().await["slow-pointer-chat"].clone();
        assert!(Arc::ptr_eq(&original, &current));
        assert!(current.alive.load(Ordering::Acquire));
        assert!(current.child.lock().unwrap().try_wait().unwrap().is_none());
        let state = browser.state("slow-pointer-chat").await.unwrap();
        assert_eq!(state["active_tab_id"], ready["active_tab_id"]);
        browser.close("slow-pointer-chat").await.unwrap();
        fixture.abort();
    }

    #[cfg(unix)]
    #[tokio::test]
    #[ignore = "requires the Playwright Chromium runtime"]
    async fn renderer_hang_retires_chromium_and_reopens_chat_with_new_epoch() {
        use tokio::io::AsyncReadExt as _;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cancel_requested = Arc::new(AtomicBool::new(false));
        let fixture_cancel_requested = cancel_requested.clone();
        let fixture = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let cancel_requested = fixture_cancel_requested.clone();
                tokio::spawn(async move {
                    let mut request = [0u8; 2048];
                    if let Ok(length) = socket.read(&mut request).await {
                        if request[..length]
                            .windows(b"/cancel".len())
                            .any(|part| part == b"/cancel")
                        {
                            cancel_requested.store(true, Ordering::Release);
                        }
                    }
                    let body = b"<!doctype html><title>stalled</title><script>for (;;) {}</script>";
                    let headers = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = socket.write_all(headers.as_bytes()).await;
                    let _ = socket.write_all(body).await;
                });
            }
        });

        let browser = BrowserManager::default();
        let opened = browser.open("hung-chat").await.unwrap();
        let initial_epoch = opened["page_epoch"].as_u64().unwrap();
        let old_host = browser.sessions.lock().await["hung-chat"].clone();
        let result = browser
            .command_with_deadline(
                "hung-chat",
                "navigate",
                json!({"url": format!("http://{address}/timeout"), "expected_epoch": initial_epoch}),
                Duration::from_secs(1),
            )
            .await;
        assert!(
            matches!(result, Err(BrowserError::Failed(message)) if message.contains("timed out"))
        );
        assert!(!browser.sessions.lock().await.contains_key("hung-chat"));
        assert!(old_host.child.lock().unwrap().try_wait().unwrap().is_some());

        let reopened = browser.open("hung-chat").await.unwrap();
        let new_epoch = reopened["page_epoch"].as_u64().unwrap();
        assert_ne!(new_epoch, initial_epoch);
        let last_old_epoch = old_host.frame_epoch.load(Ordering::Acquire);
        if last_old_epoch != 0 {
            assert_ne!(new_epoch, last_old_epoch);
        }
        let cancelled_host = browser.sessions.lock().await["hung-chat"].clone();
        let cancelled_call = {
            let browser = browser.clone();
            tokio::spawn(async move {
                browser
                    .command_with_deadline(
                        "hung-chat",
                        "navigate",
                        json!({"url": format!("http://{address}/cancel"), "expected_epoch": new_epoch}),
                        Duration::from_secs(10),
                    )
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            while !cancel_requested.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Chromium should request the page before caller cancellation");
        cancelled_call.abort();
        assert!(cancelled_call.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(2), async {
            while browser.sessions.lock().await.contains_key("hung-chat") {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("cancelled Chromium host should be detached");
        assert!(cancelled_host
            .child
            .lock()
            .unwrap()
            .try_wait()
            .unwrap()
            .is_some());
        let after_cancel = browser.open("hung-chat").await.unwrap();
        assert_ne!(after_cancel["page_epoch"].as_u64().unwrap(), new_epoch);
        browser.close("hung-chat").await.unwrap();
        fixture.abort();
    }

    #[cfg(unix)]
    #[tokio::test]
    #[ignore = "requires the Playwright Chromium runtime"]
    async fn browser_eval_deadline_retires_sync_loop_and_unsettled_promise() {
        let browser = BrowserManager::default();
        let unaffected = browser.open("unaffected-chat").await.unwrap();
        let mut previous_epoch = None;

        for code in ["for (;;) {}", "new Promise(() => {})"] {
            let opened = browser.open("eval-hung-chat").await.unwrap();
            let epoch = opened["page_epoch"].as_u64().unwrap();
            if let Some(previous_epoch) = previous_epoch {
                assert_ne!(epoch, previous_epoch);
            }
            let old_host = browser.sessions.lock().await["eval-hung-chat"].clone();
            let started = std::time::Instant::now();
            let result = browser
                .eval(
                    "eval-hung-chat",
                    json!({
                        "code":code,
                        "expected_epoch":epoch,
                        "expected_url":"about:blank"
                    }),
                )
                .await;
            assert!(
                matches!(&result, Err(BrowserError::Failed(message)) if message.contains("timed out")),
                "script should time out: {result:?}"
            );
            assert!(started.elapsed() < Duration::from_secs(8));
            assert!(!browser.sessions.lock().await.contains_key("eval-hung-chat"));
            assert!(old_host.child.lock().unwrap().try_wait().unwrap().is_some());
            assert_eq!(
                browser.state("unaffected-chat").await.unwrap()["page_epoch"],
                unaffected["page_epoch"]
            );
            previous_epoch = Some(epoch);
        }

        let reopened = browser.open("eval-hung-chat").await.unwrap();
        assert_ne!(
            reopened["page_epoch"].as_u64().unwrap(),
            previous_epoch.unwrap()
        );
        browser.close("eval-hung-chat").await.unwrap();
        browser.close("unaffected-chat").await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn browser_host_environment_drops_parent_secrets() {
        let mut command = Command::new("/usr/bin/env");
        command.env("OPENAI_API_KEY", "sentinel-do-not-inherit");
        restrict_host_environment(&mut command);
        let output = command.output().await.unwrap();
        assert!(output.status.success());
        let environment = String::from_utf8(output.stdout).unwrap();
        assert!(!environment.contains("OPENAI_API_KEY"));
        assert!(!environment.contains("sentinel-do-not-inherit"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn active_tab_reset_discards_cached_and_late_epoch_frames() {
        let session = stub_session(Duration::ZERO, true);
        let frame = |tab_id, page_epoch, frame_seq| {
            json!({
                "active_tab_id":tab_id,
                "page_epoch":page_epoch,
                "frame_seq":frame_seq,
                "viewport_width":640,
                "viewport_height":480,
                "data":base64::engine::general_purpose::STANDARD.encode([1, 2, 3]),
            })
        };
        session.accept_frame_reset(&json!({"active_tab_id":"first","page_epoch":17}));
        session.accept_frame(&frame("first", 17, 1));
        assert_eq!(session.frames.borrow().as_ref().unwrap().tab_id, "first");

        session.accept_frame_reset(&json!({"active_tab_id":"second","page_epoch":18}));
        assert!(session.frames.borrow().is_none());
        session.accept_frame(&frame("first", 17, 2));
        session.accept_frame(&frame("second", 17, 3));
        assert!(session.frames.borrow().is_none());
        session.accept_frame(&frame("second", 18, 4));
        assert_eq!(session.frames.borrow().as_ref().unwrap().tab_id, "second");

        session.accept_frame_reset(&json!({"active_tab_id":"second","page_epoch":19}));
        session.accept_frame(&frame("second", 18, 5));
        assert!(session.frames.borrow().is_none());
        session.accept_frame(&frame("second", 19, 6));
        assert_eq!(session.frames.borrow().as_ref().unwrap().page_epoch, 19);
    }

    #[tokio::test]
    async fn slow_chat_lifecycle_does_not_block_another_chat() {
        let browser = BrowserManager::default();
        let gate = browser.session_gate("slow-chat");
        let _held = gate.lock().await;
        tokio::time::timeout(Duration::from_millis(100), browser.close("other-chat"))
            .await
            .expect("another chat must not wait for this lifecycle gate")
            .unwrap();
    }

    #[tokio::test]
    async fn deleted_chat_cannot_open_a_late_browser_host() {
        let browser = BrowserManager::default();
        browser.retire("deleted-chat").await.unwrap();
        assert!(matches!(
            browser.open("deleted-chat").await,
            Err(BrowserError::NotOpen)
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn idle_sweep_closes_inactive_host_but_keeps_polled_chat_reopenable() {
        let browser = BrowserManager::default();
        assert!(!browser.cleanup_started.load(Ordering::Acquire));
        let stale = stub_session(BROWSER_IDLE_TTL + Duration::from_secs(1), false);
        let polled = stub_session(BROWSER_IDLE_TTL + Duration::from_secs(1), true);
        {
            let mut sessions = browser.sessions.lock().await;
            sessions.insert("stale-chat".into(), stale.clone());
            sessions.insert("polled-chat".into(), polled);
        }
        assert!(browser.start_idle_cleanup());
        assert!(!browser.start_idle_cleanup());

        // A workbench frame poll counts as use even if the image did not change.
        assert!(browser.frame("polled-chat", 0, 0).await.unwrap().is_none());
        assert_eq!(browser.sweep_idle(BROWSER_IDLE_TTL).await, 1);
        let sessions = browser.sessions.lock().await;
        assert!(!sessions.contains_key("stale-chat"));
        assert!(sessions.contains_key("polled-chat"));
        drop(sessions);
        assert!(!browser.retired.lock().unwrap().contains("stale-chat"));

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if stale.child.lock().unwrap().try_wait().unwrap().is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("idle host process should exit after sweep");
    }
}

impl BrowserManager {
    /// Start one sweep only after a browser page exists. AppState instances that
    /// never open Chromium do not leave timers behind.
    fn start_idle_cleanup(&self) -> bool {
        if self.cleanup_started.swap(true, Ordering::AcqRel) {
            return false;
        }
        let weak_sessions = Arc::downgrade(&self.sessions);
        let weak_gates = Arc::downgrade(&self.session_gates);
        let weak_retired = Arc::downgrade(&self.retired);
        let weak_started = Arc::downgrade(&self.cleanup_started);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(BROWSER_IDLE_SWEEP_INTERVAL).await;
                let (Some(sessions), Some(session_gates), Some(retired), Some(cleanup_started)) = (
                    weak_sessions.upgrade(),
                    weak_gates.upgrade(),
                    weak_retired.upgrade(),
                    weak_started.upgrade(),
                ) else {
                    break;
                };
                let browser = BrowserManager {
                    sessions,
                    session_gates,
                    retired,
                    cleanup_started,
                };
                let reaped = browser.sweep_idle(BROWSER_IDLE_TTL).await;
                if reaped > 0 {
                    tracing::debug!(reaped, "reclaimed idle browser sessions");
                }
            }
        });
        true
    }

    fn session_gate(&self, session_id: &str) -> Arc<AsyncMutex<()>> {
        let mut gates = self.session_gates.lock().unwrap();
        if let Some(gate) = gates.get(session_id).and_then(Weak::upgrade) {
            return gate;
        }
        gates.retain(|_, gate| gate.strong_count() > 0);
        let gate = Arc::new(AsyncMutex::new(()));
        gates.insert(session_id.to_string(), Arc::downgrade(&gate));
        gate
    }

    pub async fn open(&self, session_id: &str) -> Result<Value, BrowserError> {
        // Serialize only this chat's open/close lifecycle. Starting Chromium
        // or probing a slow host must not block unrelated chat sessions.
        let gate = self.session_gate(session_id);
        let _guard = gate.lock().await;
        if self.retired.lock().unwrap().contains(session_id) {
            return Err(BrowserError::NotOpen);
        }
        let existing = {
            let sessions = self.sessions.lock().await;
            let session = sessions.get(session_id).cloned();
            if let Some(session) = &session {
                session.touch();
            }
            session
        };
        if let Some(session) = existing {
            if session.alive.load(Ordering::Acquire) {
                let result = session.call("state", json!({})).await;
                if !session.alive.load(Ordering::Acquire) {
                    session.terminate("browser host unavailable").await;
                    self.remove_if_same(session_id, &session).await;
                }
                match result {
                    Ok(state) => {
                        session.touch();
                        self.start_idle_cleanup();
                        return Ok(state);
                    }
                    Err(BrowserError::Unavailable(_)) => {}
                    Err(error) => return Err(error),
                }
            }
            session.terminate("browser host unavailable").await;
            self.remove_if_same(session_id, &session).await;
        }
        let session = BrowserSession::spawn().await?;
        let mut abort_guard = BrowserCallAbortGuard::new(
            session.clone(),
            Some((self.clone(), session_id.to_string())),
        );
        let state = match session.call("state", json!({})).await {
            Ok(state) => state,
            Err(error) => {
                session.terminate("browser host unavailable").await;
                abort_guard.disarm();
                return Err(error);
            }
        };
        session.touch();
        self.sessions
            .lock()
            .await
            .insert(session_id.to_string(), session);
        self.start_idle_cleanup();
        abort_guard.disarm();
        Ok(state)
    }

    pub async fn state(&self, session_id: &str) -> Result<Value, BrowserError> {
        self.command(session_id, "state", json!({})).await
    }

    pub async fn command(
        &self,
        session_id: &str,
        action: &str,
        args: Value,
    ) -> Result<Value, BrowserError> {
        self.command_with_deadline(session_id, action, args, BROWSER_COMMAND_DEADLINE)
            .await
    }

    /// Page-realm scripting is a separate, mutating capability with a shorter
    /// total stdin/write/response deadline. #1220 retires a timed-out host.
    pub async fn eval(&self, session_id: &str, args: Value) -> Result<Value, BrowserError> {
        self.command_with_deadline(session_id, "eval", args, BROWSER_EVAL_DEADLINE)
            .await
    }

    async fn command_with_deadline(
        &self,
        session_id: &str,
        action: &str,
        args: Value,
        deadline: Duration,
    ) -> Result<Value, BrowserError> {
        let session = {
            let sessions = self.sessions.lock().await;
            let session = sessions
                .get(session_id)
                .cloned()
                .ok_or(BrowserError::NotOpen)?;
            session.touch();
            session
        };
        let mut abort_guard = action_may_mutate_browser(action).then(|| {
            BrowserCallAbortGuard::new(
                session.clone(),
                Some((self.clone(), session_id.to_string())),
            )
        });
        let result = session.call_with_deadline(action, args, deadline).await;
        if session.alive.load(Ordering::Acquire) {
            session.touch();
        } else {
            session.terminate("browser host unavailable").await;
            // A timed-out call retires exactly the host it used. A concurrent
            // reopen must never be removed by the old call's completion.
            self.remove_if_same(session_id, &session).await;
        }
        if let Some(guard) = abort_guard.as_mut() {
            guard.disarm();
        }
        result
    }

    async fn remove_if_same(&self, session_id: &str, candidate: &Arc<BrowserSession>) {
        let mut sessions = self.sessions.lock().await;
        if sessions
            .get(session_id)
            .is_some_and(|current| Arc::ptr_eq(current, candidate))
        {
            sessions.remove(session_id);
        }
    }

    pub async fn command_or_open(
        &self,
        session_id: &str,
        action: &str,
        args: Value,
    ) -> Result<Value, BrowserError> {
        self.open(session_id).await?;
        self.command(session_id, action, args).await
    }

    pub async fn close(&self, session_id: &str) -> Result<(), BrowserError> {
        let gate = self.session_gate(session_id);
        let _guard = gate.lock().await;
        self.close_locked(session_id).await
    }

    /// Permanently end a deleted chat's browser lifecycle in this process.
    /// A tool call queued behind deletion cannot create a late orphaned host.
    pub async fn retire(&self, session_id: &str) -> Result<(), BrowserError> {
        let gate = self.session_gate(session_id);
        let _guard = gate.lock().await;
        self.retired.lock().unwrap().insert(session_id.to_string());
        self.close_locked(session_id).await
    }

    async fn close_locked(&self, session_id: &str) -> Result<(), BrowserError> {
        let session = {
            let mut sessions = self.sessions.lock().await;
            let session = sessions.remove(session_id);
            if let Some(session) = &session {
                session.mark_dead("browser session closed");
            }
            session
        };
        if let Some(session) = session {
            Self::stop_session(session).await;
        }
        Ok(())
    }

    async fn stop_session(session: Arc<BrowserSession>) {
        // The map entry is already gone. Do not enqueue a graceful `close`
        // behind an in-flight command: callers that hold this Arc must be
        // stopped before they can write another action to the retired page.
        session.terminate("browser session closed").await;
    }

    async fn sweep_idle(&self, ttl: Duration) -> usize {
        let candidates: Vec<String> = self
            .sessions
            .lock()
            .await
            .iter()
            .filter(|(_, session)| session.idle_for(ttl))
            .map(|(session_id, _)| session_id.clone())
            .collect();
        let mut reaped = 0;
        for session_id in candidates {
            // Open and close are serialized for this chat. Commands and frame
            // polls touch their session while holding the map lock, so the
            // recheck/removal cannot race a newly active request.
            let gate = self.session_gate(&session_id);
            let _guard = gate.lock().await;
            let session = {
                let mut sessions = self.sessions.lock().await;
                if sessions
                    .get(&session_id)
                    .is_some_and(|session| session.idle_for(ttl))
                {
                    let session = sessions.remove(&session_id);
                    if let Some(session) = &session {
                        session.mark_dead("browser session idle");
                    }
                    session
                } else {
                    None
                }
            };
            if let Some(session) = session {
                Self::stop_session(session).await;
                reaped += 1;
            }
        }
        reaped
    }

    pub async fn frame(
        &self,
        session_id: &str,
        after: u64,
        wait_ms: u64,
    ) -> Result<Option<Arc<BrowserFrame>>, BrowserError> {
        let session = {
            let sessions = self.sessions.lock().await;
            let session = sessions
                .get(session_id)
                .cloned()
                .ok_or(BrowserError::NotOpen)?;
            session.touch();
            session
        };
        let mut receiver = session.frames.subscribe();
        if !session.alive.load(Ordering::Acquire) {
            return Err(BrowserError::Unavailable("browser host exited".into()));
        }
        if let Some(frame) = receiver.borrow_and_update().clone() {
            if frame.frame_seq > after {
                return Ok(Some(frame));
            }
        }
        if wait_ms == 0 {
            return Ok(None);
        }
        let waiting = async {
            loop {
                if receiver.changed().await.is_err() {
                    return None;
                }
                if !session.alive.load(Ordering::Acquire) {
                    return None;
                }
                if let Some(frame) = receiver.borrow_and_update().clone() {
                    if frame.frame_seq > after {
                        return Some(frame);
                    }
                }
            }
        };
        let frame = tokio::time::timeout(Duration::from_millis(wait_ms.min(25_000)), waiting)
            .await
            .ok()
            .flatten();
        session.touch();
        if !session.alive.load(Ordering::Acquire) {
            return Err(BrowserError::Unavailable("browser host exited".into()));
        }
        Ok(frame)
    }
}
