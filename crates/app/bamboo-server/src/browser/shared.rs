//! Account-owned process; chat-owned routes, pages and temporary contexts.
//! Physical process retirement remains exclusively in BrowserSession/Reaper.

use super::*;
use serde::{Deserialize, Serialize};

pub(super) const MAX_FRAME_BYTES: usize = 2 * 1024 * 1024;
const MAX_PENDING: usize = 64;
const MAX_CHATS: usize = 16;
// Keep one control request per admitted chat available under ordinary load.
const MAX_NORMAL_PENDING: usize = MAX_PENDING - MAX_CHATS;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserProfileMode {
    #[default]
    Shared,
    Clean,
}

pub(super) fn valid_handle(value: &str) -> bool {
    value.len() == 24
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn handle() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..24].to_owned()
}

pub(super) fn valid_handshake(value: &Value, generation: Option<&str>, bounded_storage: bool) -> bool {
    value.get("protocol_version").and_then(Value::as_u64) == Some(2)
        && generation.is_some_and(valid_handle)
        && value.get("host_generation").and_then(Value::as_str) == generation
        && value
            .get("browser_instance_id")
            .and_then(Value::as_str)
            .is_some_and(valid_handle)
        && value.get("scope_count").and_then(Value::as_u64) == Some(0)
        && value.get("profile_modes") == Some(&json!(["shared", "clean"]))
        && (!bounded_storage || value.get("storage_boundary") == Some(&json!({
            "provider": "mac_apfs_fixed_v1", "image_bytes": 536_870_912,
            "quota_bytes": 402_653_184, "native_root_tracking": 1,
            "native_group": "host_inherited_v1"
        })))
}

#[cfg(test)]
#[test]
fn production_readiness_requires_exact_bounded_storage_capability() {
    let generation = "a".repeat(24);
    let mut value = json!({"protocol_version":2,"host_generation":generation,
        "browser_instance_id":"b".repeat(24),"scope_count":0,"profile_modes":["shared","clean"]});
    assert!(valid_handshake(&value, Some(&generation), false));
    assert!(!valid_handshake(&value, Some(&generation), true));
    value["storage_boundary"] = json!({"provider":"mac_apfs_fixed_v1",
        "image_bytes":536_870_912,"quota_bytes":402_653_184,"native_root_tracking":1,
        "native_group":"host_inherited_v1"});
    assert!(valid_handshake(&value, Some(&generation), true));
    value["storage_boundary"]["quota_bytes"] = json!(402_653_185);
    assert!(!valid_handshake(&value, Some(&generation), true));
    value["storage_boundary"]["quota_bytes"] = json!(402_653_184);
    value["storage_boundary"]["all_descendants_proven"] = json!(true);
    assert!(!valid_handshake(&value, Some(&generation), true));
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Owner {
    id: String,
    generation: String,
}

impl Owner {
    fn new() -> Self {
        Self {
            id: handle(),
            generation: handle(),
        }
    }

    fn matches(&self, message: &Value) -> bool {
        message.get("scope_id").and_then(Value::as_str) == Some(self.id.as_str())
            && message.get("scope_generation").and_then(Value::as_str)
                == Some(self.generation.as_str())
    }
}

struct Pending {
    owner: Owner,
    sender: oneshot::Sender<Result<Value, BrowserError>>,
}

pub(super) struct SharedRuntime {
    // Exactly one owner/reaper/profile lease, never copied into a chat.
    pub(super) core: Arc<BrowserSession>,
    generation: String,
    routes: Mutex<HashMap<String, (String, Weak<SharedChat>)>>,
    pending: Mutex<HashMap<u64, Pending>>,
}

struct PendingGuard<'a> {
    runtime: &'a SharedRuntime,
    id: u64,
}

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        self.runtime.pending.lock().unwrap().remove(&self.id);
    }
}

/// Cancellation during write_all/flush can leave a partial JSON record. The
/// whole transport then becomes uncertain; a scope-close cannot repair it.
struct WriteGuard<'a> {
    runtime: &'a SharedRuntime,
    armed: bool,
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.runtime
                .core
                .mark_dead("shared browser write interrupted");
            self.runtime.core.signal_stop();
        }
    }
}

impl SharedRuntime {
    async fn spawn(
        root: &std::path::Path,
        lifecycle: &BrowserLifecycle,
    ) -> Result<Arc<Self>, BrowserError> {
        let lease = profile::acquire_shared(root)?;
        let core = BrowserSession::spawn_protocol(Some(lease), lifecycle, true).await?;
        let generation = core
            .handshake_generation
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| {
                BrowserError::Unavailable("shared browser identity unavailable".into())
            })?;
        let runtime = Arc::new(Self {
            core,
            generation,
            routes: Mutex::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
        });
        *runtime.core.shared_router.lock().unwrap() = Arc::downgrade(&runtime);
        Ok(runtime)
    }

    fn alive(&self) -> bool {
        self.core.alive.load(Ordering::Acquire)
    }

    pub(super) fn fail_all(&self, reason: &'static str) {
        let pending = std::mem::take(&mut *self.pending.lock().unwrap());
        for (_, entry) in pending {
            let _ = entry
                .sender
                .send(Err(BrowserError::Unavailable(reason.into())));
        }
        let chats: Vec<_> = self
            .routes
            .lock()
            .unwrap()
            .values()
            .filter_map(|(_, chat)| chat.upgrade())
            .collect();
        for chat in chats {
            chat.mark_dead();
        }
    }

    /// No reply or frame can select a different chat by changing an ID alone.
    /// Well-formed late events/replies are discarded, never rerouted.
    pub(super) fn deliver(&self, message: &Value) -> bool {
        if message.get("version").and_then(Value::as_u64) != Some(2)
            || message.get("host_generation").and_then(Value::as_str)
                != Some(self.generation.as_str())
            || !message
                .get("scope_id")
                .and_then(Value::as_str)
                .is_some_and(valid_handle)
            || !message
                .get("scope_generation")
                .and_then(Value::as_str)
                .is_some_and(valid_handle)
        {
            return false;
        }
        if let Some(event) = message.get("event").and_then(Value::as_str) {
            let chat = {
                let routes = self.routes.lock().unwrap();
                routes
                    .get(message["scope_id"].as_str().unwrap())
                    .filter(|(generation, _)| {
                        message["scope_generation"].as_str() == Some(generation.as_str())
                    })
                    .and_then(|(_, chat)| chat.upgrade())
            };
            if let Some(chat) = chat {
                match event {
                    "frame" => chat.accept_frame(message),
                    "frame_reset" => chat.accept_frame_reset(message),
                    _ => return false,
                }
            }
            return true;
        }
        let Some(id) = message.get("id").and_then(Value::as_u64) else {
            return false;
        };
        let entry = {
            let mut pending = self.pending.lock().unwrap();
            let Some(entry) = pending.get(&id) else {
                return true;
            };
            if !entry.owner.matches(message) || !message.get("ok").is_some_and(Value::is_boolean) {
                return false;
            }
            pending.remove(&id).unwrap()
        };
        let result = if message["ok"] == true {
            match message.get("result") {
                Some(value) => Ok(value.clone()),
                None => return false,
            }
        } else {
            let error = message
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("browser action failed");
            Err(match message.get("code").and_then(Value::as_str) {
                Some("browser_scope_closed") => BrowserError::NotOpen,
                Some("browser_profile_mode_conflict") => BrowserError::ProfileModeConflict,
                Some("stale_epoch") => BrowserError::StaleEpoch,
                Some("stale_dialog") => BrowserError::StaleDialog,
                Some("dialog_pending") => BrowserError::DialogPending,
                Some("browser_busy") => BrowserError::Busy,
                Some("invalid_url" | "invalid_request") => BrowserError::Invalid(error.into()),
                Some("browser_unavailable" | "browser_storage_unavailable") => {
                    self.core.mark_dead("shared browser runtime unavailable");
                    BrowserError::Unavailable(error.into())
                }
                _ => BrowserError::Failed(error.into()),
            })
        };
        let _ = entry.sender.send(result);
        true
    }

    async fn call(
        &self,
        owner: &Owner,
        action: &str,
        args: Value,
        deadline: Duration,
    ) -> Result<Value, BrowserError> {
        self.call_inner(owner, action, args, deadline, false).await
    }

    async fn call_inner(
        &self,
        owner: &Owner,
        action: &str,
        args: Value,
        deadline: Duration,
        cleanup: bool,
    ) -> Result<Value, BrowserError> {
        let mut shutdown = self.core.shutdown.subscribe();
        let exchange = async {
            let mut pipe = self.core.stdin.lock().await;
            if !self.alive() {
                return Err(BrowserError::Unavailable("shared browser exited".into()));
            }
            if !cleanup {
                let admitted = self
                    .routes
                    .lock()
                    .unwrap()
                    .get(&owner.id)
                    .filter(|(generation, _)| generation == &owner.generation)
                    .and_then(|(_, chat)| chat.upgrade())
                    .is_some_and(|chat| chat.alive.load(Ordering::Acquire));
                if !admitted {
                    return Err(BrowserError::NotOpen);
                }
            }
            // Allocate under the write lock, not before await: request order
            // on the wire is strictly monotonic across concurrent chats.
            let id = self.core.next_id.fetch_add(1, Ordering::Relaxed);
            if id > 9_007_199_254_740_991 {
                return Err(BrowserError::Unavailable(
                    "shared browser request IDs exhausted".into(),
                ));
            }
            let line = serde_json::to_vec(&json!({
                "version":2,"id":id,"host_generation":self.generation,
                "scope_id":owner.id,"scope_generation":owner.generation,
                "action":action,"args":args,
            }))
            .map_err(|error| BrowserError::Invalid(error.to_string()))?;
            if line.len() + 1 > MAX_FRAME_BYTES {
                return Err(BrowserError::Invalid(
                    "shared browser request too large".into(),
                ));
            }
            let (sender, receiver) = oneshot::channel();
            {
                let mut pending = self.pending.lock().unwrap();
                let limit = if cleanup {
                    MAX_PENDING
                } else {
                    MAX_NORMAL_PENDING
                };
                if pending.len() >= limit {
                    return Err(BrowserError::Busy);
                }
                pending.insert(
                    id,
                    Pending {
                        owner: owner.clone(),
                        sender,
                    },
                );
            }
            let _pending = PendingGuard { runtime: self, id };
            let stdin = pipe
                .as_mut()
                .ok_or_else(|| BrowserError::Unavailable("shared browser stdin closed".into()))?;
            let mut writing = WriteGuard {
                runtime: self,
                armed: true,
            };
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
            writing.armed = false;
            drop(pipe);
            receiver
                .await
                .map_err(|_| BrowserError::Unavailable("shared browser reply unavailable".into()))?
        };
        tokio::select! {
            biased;
            result = tokio::time::timeout(deadline, exchange) => match result {
                Ok(result) => result,
                Err(_) => Err(BrowserError::Failed("browser action timed out".into())),
            },
            _ = shutdown.changed() => Err(BrowserError::Unavailable("shared browser exited".into())),
        }
    }

    async fn close_owner(&self, owner: &Owner) -> Result<(), BrowserError> {
        if self.physical_close_owned() {
            return self.join_physical_close().await;
        }
        // Local/remote Busy is a known refusal, not an unknown native result.
        // Cleanup has reserved admission and may wait within the same original
        // command deadline; expiry on a live runtime still fails closed.
        let outcome = tokio::time::timeout(BROWSER_COMMAND_DEADLINE, async {
            loop {
                let result = self
                    .call_inner(
                        owner,
                        "scope_close",
                        json!({}),
                        BROWSER_COMMAND_DEADLINE,
                        true,
                    )
                    .await;
                if matches!(&result, Err(BrowserError::Busy)) {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    continue;
                }
                return result;
            }
        })
        .await;
        match outcome {
            Ok(Ok(value)) if value.get("closed").and_then(Value::as_bool) == Some(true) => Ok(()),
            Ok(Err(BrowserError::NotOpen)) => Ok(()),
            _ => {
                // A global drain may have deliberately woken this pending
                // scoped request. It already owns EOF/checkpoint/reaping.
                if self.physical_close_owned() {
                    return self.join_physical_close().await;
                }
                // Unknown scope cleanup is a shared-domain failure. Keep the
                // physical profile/scratch fence; do not claim sibling survival.
                self.core
                    .mark_dead("shared browser scope close unconfirmed");
                self.core.signal_stop();
                Box::pin(
                    self.core
                        .terminate("shared browser scope close unconfirmed"),
                )
                .await;
                Err(BrowserError::Unavailable(
                    "shared browser scope close unconfirmed".into(),
                ))
            }
        }
    }

    fn physical_close_owned(&self) -> bool {
        !self.alive()
            && self.core.retiring.load(Ordering::Acquire)
            && !self.core.kill_issued.load(Ordering::Acquire)
    }

    async fn join_physical_close(&self) -> Result<(), BrowserError> {
        Box::pin(self.core.terminate("joining shared browser physical close")).await;
        if self.core.clean_host_exit.load(Ordering::Acquire)
            && self.core.close_confirmed.load(Ordering::Acquire)
            && self.core.reaped.load(Ordering::Acquire)
            && !self.core.cleanup.is_quarantined()
        {
            Ok(())
        } else {
            Err(BrowserError::Unavailable(
                "shared browser physical close unconfirmed".into(),
            ))
        }
    }

    async fn close(&self) -> Result<(), BrowserError> {
        if self.alive() {
            self.core
                .begin_retirement("shared browser has no active chats")?;
        }
        self.core
            .terminate("shared browser has no active chats")
            .await;
        if self.core.clean_host_exit.load(Ordering::Acquire)
            && self.core.close_confirmed.load(Ordering::Acquire)
            && self.core.reaped.load(Ordering::Acquire)
            && !self.core.cleanup.is_quarantined()
            && self.core.profile_lease.lock().unwrap().is_none()
        {
            Ok(())
        } else {
            Err(BrowserError::Unavailable(
                "shared browser close unconfirmed".into(),
            ))
        }
    }
}

pub(super) struct SharedChat {
    runtime: Arc<SharedRuntime>,
    owner: Owner,
    mode: BrowserProfileMode,
    lease: Mutex<Option<profile::ChatLease>>,
    alive: AtomicBool,
    closed: AtomicBool,
    closing: AsyncMutex<()>,
    last_used: Mutex<Instant>,
    frames: watch::Sender<Option<Arc<BrowserFrame>>>,
    epoch: AtomicU64,
    active_tab: Mutex<Option<String>>,
}

impl SharedChat {
    fn mark_dead(&self) {
        self.alive.store(false, Ordering::Release);
        self.frames.send_replace(None);
        *self.active_tab.lock().unwrap() = None;
    }

    fn touch(&self) {
        *self.last_used.lock().unwrap() = Instant::now();
        self.runtime.core.touch();
    }

    fn idle(&self, ttl: Duration) -> bool {
        self.last_used.lock().unwrap().elapsed() >= ttl
    }

    fn accept_frame_reset(&self, message: &Value) {
        if !self.alive.load(Ordering::Acquire) {
            return;
        }
        let Some(epoch) = message.get("page_epoch").and_then(Value::as_u64) else {
            return;
        };
        if epoch < self.epoch.load(Ordering::Acquire) {
            return;
        }
        self.epoch.store(epoch, Ordering::Release);
        *self.active_tab.lock().unwrap() = message
            .get("active_tab_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        self.frames.send_replace(None);
    }

    fn accept_frame(&self, message: &Value) {
        if !self.alive.load(Ordering::Acquire) {
            return;
        }
        let Some(tab_id) = message.get("active_tab_id").and_then(Value::as_str) else {
            return;
        };
        if self.active_tab.lock().unwrap().as_deref() != Some(tab_id) {
            return;
        }
        let Some(epoch) = message.get("page_epoch").and_then(Value::as_u64) else {
            return;
        };
        if epoch < self.epoch.load(Ordering::Acquire) {
            return;
        }
        let Some(seq) = message.get("frame_seq").and_then(Value::as_u64) else {
            return;
        };
        let Some(width) = message
            .get("viewport_width")
            .and_then(Value::as_u64)
            .and_then(|v| u32::try_from(v).ok())
        else {
            return;
        };
        let Some(height) = message
            .get("viewport_height")
            .and_then(Value::as_u64)
            .and_then(|v| u32::try_from(v).ok())
        else {
            return;
        };
        let Some(data) = message.get("data").and_then(Value::as_str) else {
            return;
        };
        let Ok(jpeg) = base64::engine::general_purpose::STANDARD.decode(data) else {
            return;
        };
        self.epoch.store(epoch, Ordering::Release);
        self.frames.send_replace(Some(Arc::new(BrowserFrame {
            tab_id: tab_id.into(),
            page_epoch: epoch,
            frame_seq: seq,
            viewport_width: width,
            viewport_height: height,
            jpeg: jpeg.into(),
        })));
    }

    pub(super) async fn call(
        &self,
        action: &str,
        args: Value,
        deadline: Duration,
    ) -> Result<Value, BrowserError> {
        if !self.alive.load(Ordering::Acquire) || !self.runtime.alive() {
            return Err(BrowserError::NotOpen);
        }
        let result = self.runtime.call(&self.owner, action, args, deadline).await;
        if !self.alive.load(Ordering::Acquire) {
            return if self.runtime.alive() {
                Err(BrowserError::NotOpen)
            } else {
                Err(BrowserError::Unavailable("shared browser exited".into()))
            };
        }
        if matches!(
            &result,
            Err(BrowserError::Unavailable(_) | BrowserError::NotOpen)
        ) || matches!(&result, Err(BrowserError::Failed(reason)) if reason == "browser action timed out")
        {
            self.mark_dead();
        }
        if let Ok(value) = &result {
            // The transport owns mode identity; never infer it from a request
            // after a missing/contradictory state response.
            if matches!(action, "scope_open" | "state")
                && (value.get("profile_mode") != Some(&json!(self.mode))
                    || value.get("storage_shared").and_then(Value::as_bool)
                        != Some(self.mode == BrowserProfileMode::Shared))
            {
                self.mark_dead();
                return Err(BrowserError::Unavailable(
                    "shared browser mode receipt mismatch".into(),
                ));
            }
        }
        result
    }

    async fn close(&self) -> Result<(), BrowserError> {
        self.mark_dead();
        let _closing = self.closing.lock().await;
        if self.closed.load(Ordering::Acquire) {
            return Ok(());
        }
        let result = self.runtime.close_owner(&self.owner).await;
        // An unknown close fences the entire physical owner; its account lease
        // and scratch stay quarantined. A chat lease is not that writer proof.
        self.closed.store(true, Ordering::Release);
        self.lease.lock().unwrap().take();
        self.runtime.routes.lock().unwrap().remove(&self.owner.id);
        result
    }
}

impl Drop for SharedChat {
    fn drop(&mut self) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        self.mark_dead();
        let runtime = self.runtime.clone();
        let owner = self.owner.clone();
        let lease = self.lease.get_mut().unwrap().take();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let _ = runtime.close_owner(&owner).await;
                runtime.routes.lock().unwrap().remove(&owner.id);
                drop(lease);
            });
        } else {
            // No async scope cleanup is possible during runtime teardown.
            // Escalate to the physical owner's existing quarantine/reaper.
            runtime
                .core
                .mark_dead("shared chat dropped without executor");
            runtime.core.signal_stop();
            drop(lease);
        }
    }
}

/// Unlike BrowserCallAbortGuard, this never signals the physical process for
/// an ordinary chat cancellation. Cleanup is owned independently of the caller.
struct ChatAbortGuard {
    manager: Arc<SharedManager>,
    session_id: String,
    chat: Arc<SharedChat>,
    armed: bool,
}

impl Drop for ChatAbortGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.chat.mark_dead();
        let manager = self.manager.clone();
        let session_id = self.session_id.clone();
        let chat = self.chat.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let _ = chat.close().await;
                manager.remove_if_same(&session_id, &chat);
                let _ = manager.close_if_empty().await;
            });
        }
    }
}

#[derive(Default)]
pub(super) struct SharedManager {
    // This lock serializes physical startup/final close, not chat commands.
    runtime: AsyncMutex<Option<Arc<SharedRuntime>>>,
    chats: Mutex<HashMap<String, Arc<SharedChat>>>,
}

impl SharedManager {
    #[cfg(all(test, unix))]
    pub(super) async fn test_core(&self) -> Option<Arc<BrowserSession>> {
        self.runtime
            .lock()
            .await
            .as_ref()
            .map(|runtime| runtime.core.clone())
    }

    #[cfg(all(test, unix))]
    pub(super) fn test_chat(&self, id: &str) -> Option<Arc<SharedChat>> {
        self.chats.lock().unwrap().get(id).cloned()
    }

    #[cfg(all(test, unix))]
    pub(super) fn test_age(&self, id: &str, elapsed: Duration) {
        *self
            .chats
            .lock()
            .unwrap()
            .get(id)
            .unwrap()
            .last_used
            .lock()
            .unwrap() = Instant::now() - elapsed;
    }

    #[cfg(all(test, unix))]
    pub(super) fn test_epoch(&self, id: &str) -> u64 {
        self.chats
            .lock()
            .unwrap()
            .get(id)
            .unwrap()
            .epoch
            .load(Ordering::Acquire)
    }

    #[cfg(all(test, unix))]
    pub(super) async fn test_pending(&self) -> usize {
        self.runtime
            .lock()
            .await
            .as_ref()
            .map_or(0, |runtime| runtime.pending.lock().unwrap().len())
    }

    fn remove_if_same(&self, id: &str, chat: &Arc<SharedChat>) {
        let mut chats = self.chats.lock().unwrap();
        if chats
            .get(id)
            .is_some_and(|current| Arc::ptr_eq(current, chat))
        {
            chats.remove(id);
        }
    }

    pub(super) fn contains(&self, id: &str) -> bool {
        self.chats.lock().unwrap().contains_key(id)
    }

    pub(super) fn idle_candidates(&self, ttl: Duration) -> Vec<String> {
        self.chats
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, chat)| chat.idle(ttl))
            .map(|(id, _)| id.clone())
            .collect()
    }

    pub(super) fn is_idle(&self, id: &str, ttl: Duration) -> bool {
        self.chats
            .lock()
            .unwrap()
            .get(id)
            .is_some_and(|chat| chat.idle(ttl))
    }

    pub(super) async fn open(
        self: &Arc<Self>,
        root: &std::path::Path,
        lifecycle: &BrowserLifecycle,
        id: &str,
        requested: Option<BrowserProfileMode>,
    ) -> Result<Value, BrowserError> {
        let existing = self.chats.lock().unwrap().get(id).cloned();
        if let Some(chat) = existing {
            if chat.alive.load(Ordering::Acquire) && chat.runtime.alive() {
                if requested.is_some_and(|mode| mode != chat.mode) {
                    return Err(BrowserError::ProfileModeConflict);
                }
                chat.touch();
                let result = chat
                    .call("state", json!({}), BROWSER_COMMAND_DEADLINE)
                    .await;
                if chat.alive.load(Ordering::Acquire) {
                    return result;
                }
            }
            let _ = chat.close().await;
            self.remove_if_same(id, &chat);
        }
        lifecycle.ensure_writer_admission()?;
        let lease = profile::acquire_chat(root, id)?;
        let mut runtime = self.runtime.lock().await;
        lifecycle.ensure_writer_admission()?;
        if let Some(old) = runtime.as_ref() {
            if !old.alive() {
                old.close().await?;
                *runtime = None;
            }
        }
        if runtime.is_none() {
            *runtime = Some(SharedRuntime::spawn(root, lifecycle).await?);
        }
        let owned = runtime.as_ref().unwrap().clone();
        let chat = Arc::new(SharedChat {
            runtime: owned.clone(),
            owner: Owner::new(),
            mode: requested.unwrap_or_default(),
            lease: Mutex::new(Some(lease)),
            alive: AtomicBool::new(true),
            closed: AtomicBool::new(false),
            closing: AsyncMutex::new(()),
            last_used: Mutex::new(Instant::now()),
            frames: watch::channel(None).0,
            epoch: AtomicU64::new(0),
            active_tab: Mutex::new(None),
        });
        {
            let mut chats = self.chats.lock().unwrap();
            if chats.len() >= MAX_CHATS {
                // No remote scope was created; don't emit an unnecessary close.
                chat.closed.store(true, Ordering::Release);
                return Err(BrowserError::Busy);
            }
            chats.insert(id.into(), chat.clone());
        }
        owned.routes.lock().unwrap().insert(
            chat.owner.id.clone(),
            (chat.owner.generation.clone(), Arc::downgrade(&chat)),
        );
        // Insertion happens before releasing the startup/final-close lock, so
        // another chat's final close cannot destroy this startup's browser.
        drop(runtime);
        let mut guard = ChatAbortGuard {
            manager: self.clone(),
            session_id: id.into(),
            chat: chat.clone(),
            armed: true,
        };
        let result = chat
            .call(
                "scope_open",
                json!({"profile_mode":chat.mode}),
                BROWSER_COMMAND_DEADLINE,
            )
            .await;
        if result.is_err() {
            let _ = chat.close().await;
            self.remove_if_same(id, &chat);
            let _ = self.close_if_empty().await;
        }
        guard.armed = false;
        result
    }

    pub(super) async fn command(
        self: &Arc<Self>,
        id: &str,
        action: &str,
        args: Value,
        deadline: Duration,
    ) -> Result<Value, BrowserError> {
        let chat = {
            let chats = self.chats.lock().unwrap();
            let chat = chats.get(id).cloned().ok_or(BrowserError::NotOpen)?;
            chat.touch();
            chat
        };
        let mut guard = action_may_mutate_browser(action).then(|| ChatAbortGuard {
            manager: self.clone(),
            session_id: id.into(),
            chat: chat.clone(),
            armed: true,
        });
        let result = chat.call(action, args, deadline).await;
        if !chat.alive.load(Ordering::Acquire) {
            let _ = chat.close().await;
            self.remove_if_same(id, &chat);
            let _ = self.close_if_empty().await;
        } else {
            chat.touch();
        }
        if let Some(guard) = guard.as_mut() {
            guard.armed = false;
        }
        result
    }

    pub(super) async fn close(&self, id: &str) -> Result<(), BrowserError> {
        let chat = self.chats.lock().unwrap().get(id).cloned();
        if let Some(chat) = chat {
            let result = chat.close().await;
            self.remove_if_same(id, &chat);
            result?;
        }
        self.close_if_empty().await
    }

    async fn close_if_empty(&self) -> Result<(), BrowserError> {
        let mut runtime = self.runtime.lock().await;
        if !self.chats.lock().unwrap().is_empty() {
            return Ok(());
        }
        if let Some(owned) = runtime.as_ref() {
            owned.close().await?;
        }
        *runtime = None;
        Ok(())
    }

    /// Opens are fenced by the parent manager before this snapshot. Physical
    /// shutdown/reset owns EOF; mark views closed without issuing scope RPCs.
    pub(super) async fn drain(&self) -> Option<Arc<BrowserSession>> {
        let runtime = self.runtime.lock().await.take();
        if let Some(owned) = &runtime {
            // Keep the router alive until its physical owner is fenced. A
            // late frame between map drain and EOF must not look like a second
            // unscoped handshake and turn a normal close into quarantine.
            if let Err(error) = owned
                .core
                .begin_retirement("shared browser manager draining")
            {
                tracing::warn!(%error, "could not fence shared browser drain");
                owned.core.mark_dead("shared browser manager draining");
                owned.core.signal_stop();
            }
            let core = owned.core.clone();
            // Canonical reset/shutdown callers may be cancelled after taking
            // the map snapshot. EOF/reaping remains an owned physical task.
            tokio::spawn(async move {
                core.terminate("shared browser manager draining").await;
            });
        }
        let chats = std::mem::take(&mut *self.chats.lock().unwrap());
        for (_, chat) in chats {
            chat.mark_dead();
            chat.closed.store(true, Ordering::Release);
            chat.lease.lock().unwrap().take();
        }
        runtime.map(|owned| owned.core.clone())
    }

    pub(super) async fn frame(
        &self,
        id: &str,
        after: u64,
        wait_ms: u64,
    ) -> Result<Option<Arc<BrowserFrame>>, BrowserError> {
        let chat = {
            let chats = self.chats.lock().unwrap();
            let chat = chats.get(id).cloned().ok_or(BrowserError::NotOpen)?;
            chat.touch();
            chat
        };
        if !chat.alive.load(Ordering::Acquire) || !chat.runtime.alive() {
            return Err(BrowserError::NotOpen);
        }
        let mut frames = chat.frames.subscribe();
        if let Some(frame) = frames.borrow_and_update().clone() {
            if frame.frame_seq > after {
                return Ok(Some(frame));
            }
        }
        if wait_ms == 0 {
            return Ok(None);
        }
        let waiting = async {
            loop {
                if frames.changed().await.is_err() || !chat.alive.load(Ordering::Acquire) {
                    return None;
                }
                if let Some(frame) = frames.borrow_and_update().clone() {
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
        if !chat.alive.load(Ordering::Acquire) || !chat.runtime.alive() {
            return Err(BrowserError::NotOpen);
        }
        chat.touch();
        Ok(frame)
    }
}
