//! Process-owned browser sessions shared by HTTP workbench requests and agent tools.
//! Each chat session has one isolated Playwright page in a child Node process.

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

#[derive(Debug, thiserror::Error)]
pub enum BrowserError {
    #[error("browser runtime unavailable: {0}")]
    Unavailable(String),
    #[error("browser session not open")]
    NotOpen,
    #[error("browser page changed; refresh state and retry")]
    StaleEpoch,
    #[error("invalid browser request: {0}")]
    Invalid(String),
    #[error("browser action failed: {0}")]
    Failed(String),
}

#[derive(Clone, Debug)]
pub struct BrowserFrame {
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
    stdin: AsyncMutex<ChildStdin>,
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<Value, BrowserError>>>>,
    next_id: AtomicU64,
    frames: watch::Sender<Option<Arc<BrowserFrame>>>,
    frame_epoch: AtomicU64,
    alive: AtomicBool,
    last_used: Mutex<Instant>,
}

impl Drop for BrowserSession {
    fn drop(&mut self) {
        // `kill_on_drop` also covers a Bamboo shutdown while a request is idle.
        if let Ok(mut child) = self.child.lock() {
            let _ = child.start_kill();
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
        let mut child = command
            .arg(script)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| BrowserError::Unavailable(error.to_string()))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| BrowserError::Unavailable("browser host stdin not available".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| BrowserError::Unavailable("browser host stdout not available".into()))?;
        let (frames, _) = watch::channel(None);
        let session = Arc::new(Self {
            child: Mutex::new(child),
            stdin: AsyncMutex::new(stdin),
            pending: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            frames,
            frame_epoch: AtomicU64::new(0),
            alive: AtomicBool::new(true),
            last_used: Mutex::new(Instant::now()),
        });
        let weak = Arc::downgrade(&session);
        tokio::spawn(async move { Self::read_messages(weak, stdout).await });
        // The first state request doubles as a readiness probe. A missing
        // Playwright package or Chromium binary fails here, not on first click.
        session.call("state", json!({})).await?;
        Ok(session)
    }

    async fn read_messages(weak: Weak<Self>, stdout: tokio::process::ChildStdout) {
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let Some(session) = weak.upgrade() else { break };
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
                        Some("invalid_url") => BrowserError::Invalid(error.to_string()),
                        _ => BrowserError::Failed(error.to_string()),
                    })
                };
                let _ = sender.send(result);
            }
        }
        if let Some(session) = weak.upgrade() {
            session.alive.store(false, Ordering::Release);
            session.frames.send_replace(None);
            let pending = std::mem::take(&mut *session.pending.lock().unwrap());
            for (_, sender) in pending {
                let _ = sender.send(Err(BrowserError::Unavailable("browser host exited".into())));
            }
        }
    }

    fn accept_frame(&self, value: &Value) {
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
            page_epoch,
            frame_seq,
            viewport_width,
            viewport_height,
            jpeg: jpeg.into(),
        })));
    }

    fn accept_frame_reset(&self, value: &Value) {
        let Some(page_epoch) = value.get("page_epoch").and_then(Value::as_u64) else {
            return;
        };
        if page_epoch < self.frame_epoch.load(Ordering::Acquire) {
            return;
        }
        self.frame_epoch.store(page_epoch, Ordering::Release);
        self.frames.send_replace(None);
    }

    fn touch(&self) {
        *self.last_used.lock().unwrap() = Instant::now();
    }

    fn idle_for(&self, ttl: Duration) -> bool {
        self.last_used.lock().unwrap().elapsed() >= ttl
    }

    async fn call(&self, action: &str, args: Value) -> Result<Value, BrowserError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, sender);
        let line = serde_json::to_vec(&json!({"id": id, "action": action, "args": args}))
            .map_err(|error| BrowserError::Invalid(error.to_string()))?;
        let write = async {
            let mut stdin = self.stdin.lock().await;
            stdin.write_all(&line).await?;
            stdin.write_all(b"\n").await?;
            stdin.flush().await
        };
        if let Err(error) = write.await {
            self.pending.lock().unwrap().remove(&id);
            return Err(BrowserError::Unavailable(error.to_string()));
        }
        match tokio::time::timeout(Duration::from_secs(30), receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(BrowserError::Unavailable("browser host exited".into())),
            Err(_) => {
                self.pending.lock().unwrap().remove(&id);
                Err(BrowserError::Failed("browser action timed out".into()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    fn stub_session(idle_for: Duration, alive: bool) -> Arc<BrowserSession> {
        let mut child = Command::new("/bin/sleep")
            .arg("60")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let (frames, _) = watch::channel(None);
        Arc::new(BrowserSession {
            child: Mutex::new(child),
            stdin: AsyncMutex::new(stdin),
            pending: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            frames,
            frame_epoch: AtomicU64::new(0),
            alive: AtomicBool::new(alive),
            last_used: Mutex::new(Instant::now() - idle_for),
        })
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
    async fn frame_reset_discards_old_epoch_jpeg_and_late_frames() {
        let session = stub_session(Duration::ZERO, true);
        let frame = |page_epoch, frame_seq| {
            json!({
                "page_epoch":page_epoch,
                "frame_seq":frame_seq,
                "viewport_width":640,
                "viewport_height":480,
                "data":base64::engine::general_purpose::STANDARD.encode([1, 2, 3]),
            })
        };
        session.accept_frame(&frame(17, 1));
        assert_eq!(session.frames.borrow().as_ref().unwrap().page_epoch, 17);
        session.accept_frame_reset(&json!({"page_epoch":18}));
        assert!(session.frames.borrow().is_none());
        session.accept_frame(&frame(17, 2));
        assert!(session.frames.borrow().is_none());
        session.accept_frame(&frame(18, 3));
        assert_eq!(session.frames.borrow().as_ref().unwrap().page_epoch, 18);
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
                match session.call("state", json!({})).await {
                    Ok(state) => {
                        session.touch();
                        self.start_idle_cleanup();
                        return Ok(state);
                    }
                    Err(BrowserError::Unavailable(_)) => {}
                    Err(error) => return Err(error),
                }
            }
            self.sessions.lock().await.remove(session_id);
        }
        let session = BrowserSession::spawn().await?;
        let state = session.call("state", json!({})).await?;
        session.touch();
        self.sessions
            .lock()
            .await
            .insert(session_id.to_string(), session);
        self.start_idle_cleanup();
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
        let session = {
            let sessions = self.sessions.lock().await;
            let session = sessions
                .get(session_id)
                .cloned()
                .ok_or(BrowserError::NotOpen)?;
            session.touch();
            session
        };
        let result = session.call(action, args).await;
        session.touch();
        result
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
        let session = self.sessions.lock().await.remove(session_id);
        if let Some(session) = session {
            Self::stop_session(session).await;
        }
        Ok(())
    }

    async fn stop_session(session: Arc<BrowserSession>) {
        if session.alive.load(Ordering::Acquire) {
            let _ = tokio::time::timeout(Duration::from_secs(2), session.call("close", json!({})))
                .await;
        }
        if let Ok(mut child) = session.child.lock() {
            let _ = child.start_kill();
        }
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
                    sessions.remove(&session_id)
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
