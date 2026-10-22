//! Host-local outer-task ownership. Logical terminal status is never idleness.
//!
//! This module owns real JoinHandles, not a serialized idle assertion. It does
//! not yet issue physical closure: ordinary Root tools have no complete resource
//! domain. A reset admission reservation is therefore not a reset write grant.
use std::{
    collections::HashMap,
    fmt, io,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
};

use bamboo_domain::{ConversationGenerationObservation, Session, SessionKind};
use futures::FutureExt;
use tokio::{
    sync::{watch, Notify},
    task::JoinHandle,
};
use uuid::Uuid;

const MAX_RETAINED_OUTER_TASKS: usize = 1024;

#[derive(Debug, Clone)]
struct RootObservation {
    birth: String,
    project: Option<String>,
    generation: ConversationGenerationObservation,
}
impl RootObservation {
    fn capture(session: &Session) -> io::Result<Self> {
        session
            .validate_conversation_state()
            .map_err(io::Error::other)?;
        if session.kind != SessionKind::Root
            || session.parent_session_id.is_some()
            || session.parent_created_at.is_some()
            || session.spawn_depth != 0
            || (!session.root_session_id.is_empty() && session.root_session_id != session.id)
        {
            return Err(unsupported("not a supported Root lifetime"));
        }
        Ok(Self {
            birth: session.root_mode_birth_token(),
            project: session.project_id_meta(),
            generation: session.conversation_generation_observation(),
        })
    }
    fn check(&self, session: &Session) -> io::Result<()> {
        let current = Self::capture(session)?;
        if self.birth != current.birth || self.project != current.project {
            return Err(busy("Root lifetime or Project changed"));
        }
        self.generation.check_canonical(session).map_err(busy)
    }
}

struct OuterTask {
    original: RootObservation,
    tail_completed: bool,
    actor_finished: bool,
    failure: Option<String>,
}
struct JoinedOuter {
    epoch: u64,
    original: RootObservation,
    actor_finished: bool,
}
#[derive(Default)]
struct State {
    run_id: Option<String>,
    attached: bool,
    unobserved_predecessor: bool,
    tasks: HashMap<u64, OuterTask>,
    joined: Option<JoinedOuter>,
    failure: Option<String>,
    next_reset: u64,
    joins: Vec<JoinHandle<()>>,
    shutdown_started: bool,
}

/// Private lineage carried by the existing runner slot, shared by successors.
/// No public constructor, Deserialize, or caller-supplied closure boolean.
pub(crate) struct RootExecutionLifetime {
    state: Mutex<State>,
    epoch: AtomicU64,
    reset: AtomicU64,
    closed: AtomicBool,
    changed: Notify,
    shutdown: watch::Sender<Option<Result<(), String>>>,
    #[cfg(test)]
    before_actor_finish: Mutex<Option<Arc<RootTailPause>>>,
}
impl Default for RootExecutionLifetime {
    fn default() -> Self {
        Self {
            state: Mutex::default(),
            epoch: AtomicU64::new(0),
            reset: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            changed: Notify::new(),
            shutdown: watch::channel(None).0,
            #[cfg(test)]
            before_actor_finish: Mutex::default(),
        }
    }
}
impl fmt::Debug for RootExecutionLifetime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RootExecutionLifetime")
            .field("epoch", &self.epoch.load(Ordering::Acquire))
            .field("closed", &self.closed.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub(super) struct RootRunAdmission {
    lifetime: Arc<RootExecutionLifetime>,
    epoch: u64,
    run_id: String,
}

/// Only the installed runner owns this slot. Snapshot clones are observers.
/// Arbitrary maintenance/removal invalidates retained reset tokens atomically;
/// an old token cannot stay valid after its map entry disappears.
#[derive(Debug)]
pub(super) struct RootRunnerSlot {
    lifetime: Arc<RootExecutionLifetime>,
    owner: bool,
}
impl RootRunnerSlot {
    pub(super) fn new(lifetime: Arc<RootExecutionLifetime>) -> Self {
        Self {
            lifetime,
            owner: true,
        }
    }
    pub(super) fn disarm(&mut self) {
        self.owner = false;
    }
    pub(super) fn reserve_reset_admission(
        &self,
        session: &Session,
        expected_epoch: u64,
        operation_id: Uuid,
    ) -> io::Result<RootResetAdmission> {
        if !self.owner {
            return Err(unsupported(
                "runner snapshot is not the installed Root owner",
            ));
        }
        self.lifetime
            .reserve_reset_admission(session, expected_epoch, operation_id)
    }
}
impl Clone for RootRunnerSlot {
    fn clone(&self) -> Self {
        Self {
            lifetime: self.lifetime.clone(),
            owner: false,
        }
    }
}
impl Drop for RootRunnerSlot {
    fn drop(&mut self) {
        if self.owner {
            self.lifetime.closed.store(true, Ordering::Release);
            self.lifetime.reset.store(0, Ordering::Release);
        }
    }
}

/// Observation only. Even Joined needs the complete Root resource issuer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RootOuterClosure {
    Unknown,
    Pending,
    Unconfirmed(String),
    JoinedResourcesUnavailable { actor_finished: bool },
}

impl RootExecutionLifetime {
    /// Called under the common runner reservation boundary, before publication.
    /// Never waits for a task, activation publication fence or Storage lock.
    pub(super) fn admit(self: &Arc<Self>, run_id: &str) -> io::Result<RootRunAdmission> {
        let mut state = self.state.lock().map_err(|_| busy("Root owner poisoned"))?;
        if self.closed.load(Ordering::Acquire) || self.reset.load(Ordering::Acquire) != 0 {
            return Err(busy("Root admission is closed or reserved for reset"));
        }
        // Poll and actually join ready coordinators; readiness alone is never
        // used to certify an outer task. This bounds retained completed handles.
        for mut join in std::mem::take(&mut state.joins) {
            match (&mut join).now_or_never() {
                Some(Ok(())) => {}
                Some(Err(error)) => {
                    state.failure.get_or_insert(error.to_string());
                }
                None => state.joins.push(join),
            }
        }
        let epoch = self
            .epoch
            .load(Ordering::Acquire)
            .checked_add(1)
            .ok_or_else(|| unsupported("Root admission epoch exhausted"))?;
        if state.run_id.is_some() && !state.attached {
            state.unobserved_predecessor = true;
        }
        state.run_id = Some(run_id.into());
        state.attached = false;
        state.joined = None;
        self.epoch.store(epoch, Ordering::Release);
        Ok(RootRunAdmission {
            lifetime: self.clone(),
            epoch,
            run_id: run_id.into(),
        })
    }

    pub(crate) fn outer_closure(&self) -> RootOuterClosure {
        let Ok(state) = self.state.try_lock() else {
            return RootOuterClosure::Pending;
        };
        if let Some(error) = &state.failure {
            return RootOuterClosure::Unconfirmed(error.clone());
        }
        if !state.tasks.is_empty() || (state.run_id.is_some() && !state.attached) {
            return RootOuterClosure::Pending;
        }
        if state.unobserved_predecessor {
            return RootOuterClosure::Unconfirmed("unobserved predecessor handoff".into());
        }
        match state.joined.as_ref() {
            Some(joined) if joined.epoch == self.epoch.load(Ordering::Acquire) => {
                RootOuterClosure::JoinedResourcesUnavailable {
                    actor_finished: joined.actor_finished,
                }
            }
            _ => RootOuterClosure::Unknown,
        }
    }

    /// Admission reservation only; not an idle/Storage capability. The future
    /// resource issuer and canonical final authority must still refuse today.
    fn reserve_reset_admission(
        self: &Arc<Self>,
        session: &Session,
        expected_epoch: u64,
        operation_id: Uuid,
    ) -> io::Result<RootResetAdmission> {
        if operation_id.is_nil() {
            return Err(unsupported("reset operation identity is nil"));
        }
        let mut state = self.state.try_lock().map_err(|_| busy("Root owner busy"))?;
        if self.closed.load(Ordering::Acquire)
            || self.reset.load(Ordering::Acquire) != 0
            || self.epoch.load(Ordering::Acquire) != expected_epoch
            || !state.tasks.is_empty()
            || !state.attached
            || state.unobserved_predecessor
            || state.failure.is_some()
        {
            return Err(busy("Root outer lifetime is not observed closed"));
        }
        let joined = state
            .joined
            .as_ref()
            .filter(|j| j.epoch == expected_epoch)
            .ok_or_else(|| unsupported("no actual Root task join"))?;
        joined.original.check(session)?;
        let original = joined.original.clone();
        state.next_reset = state
            .next_reset
            .checked_add(1)
            .ok_or_else(|| unsupported("Root reset reservation exhausted"))?;
        let nonce = state.next_reset;
        self.reset.store(nonce, Ordering::Release);
        Ok(RootResetAdmission {
            lifetime: self.clone(),
            epoch: expected_epoch,
            nonce,
            operation_id,
            original,
        })
    }

    pub(crate) fn admission_epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }
    pub(super) fn reset_admission_reserved(&self) -> bool {
        self.reset.load(Ordering::Acquire) != 0
    }

    /// Close future admissions and join retained coordinators. Does not cancel
    /// any execution, background process, independent task or Child. The owned
    /// joining job survives a cancelled waiter. Release runner/publication and
    /// Storage guards before awaiting. Host assembly is a later seam.
    pub(crate) async fn close_and_join(self: &Arc<Self>) -> io::Result<()> {
        let mut completion = self.shutdown.subscribe();
        {
            let mut state = self.state.lock().map_err(|_| busy("Root owner poisoned"))?;
            self.closed.store(true, Ordering::Release);
            if !state.shutdown_started {
                state.shutdown_started = true;
                let completion = self.shutdown.clone();
                let weak = Arc::downgrade(self);
                tokio::spawn(async move {
                    // An attached outer task can race the synchronous retain
                    // handoff. Wait for actual join observations before taking
                    // the final complete coordinator set, with no lock held.
                    let (joins, unconfirmed) = loop {
                        let Some(lifetime) = weak.upgrade() else {
                            completion
                                .send_replace(Some(Err("Root join owner disappeared".into())));
                            return;
                        };
                        let changed = lifetime.changed.notified();
                        {
                            let Ok(mut state) = lifetime.state.lock() else {
                                completion
                                    .send_replace(Some(Err("Root join owner poisoned".into())));
                                return;
                            };
                            if state.tasks.is_empty() {
                                let unconfirmed = state.failure.clone().or_else(|| {
                                    (state.unobserved_predecessor
                                        || (state.run_id.is_some() && !state.attached))
                                        .then(|| "Root launch handoff is unconfirmed".into())
                                });
                                break (std::mem::take(&mut state.joins), unconfirmed);
                            }
                        }
                        changed.await;
                    };
                    let mut failure = unconfirmed;
                    for join in joins {
                        if let Err(error) = join.await {
                            failure.get_or_insert(error.to_string());
                        }
                    }
                    completion.send_replace(Some(failure.map_or(Ok(()), Err)));
                });
            }
        }
        loop {
            if let Some(result) = completion.borrow().clone() {
                return result.map_err(io::Error::other);
            }
            completion
                .changed()
                .await
                .map_err(|_| unsupported("Root join owner disappeared"))?;
        }
    }
}

impl RootRunAdmission {
    pub(super) fn attach(&self, session: &Session) -> io::Result<RootOuterTask> {
        let original = RootObservation::capture(session)?;
        let mut state = self
            .lifetime
            .state
            .lock()
            .map_err(|_| busy("Root owner poisoned"))?;
        if self.lifetime.closed.load(Ordering::Acquire)
            || self.lifetime.reset.load(Ordering::Acquire) != 0
            || self.lifetime.epoch.load(Ordering::Acquire) != self.epoch
            || state.run_id.as_deref() != Some(self.run_id.as_str())
            || state.attached
            || state.tasks.len() >= MAX_RETAINED_OUTER_TASKS
        {
            return Err(busy("Root launch handoff changed or is unavailable"));
        }
        state.attached = true;
        state.tasks.insert(
            self.epoch,
            OuterTask {
                original,
                tail_completed: false,
                actor_finished: false,
                failure: None,
            },
        );
        Ok(RootOuterTask {
            lifetime: Arc::downgrade(&self.lifetime),
            epoch: self.epoch,
        })
    }
}

#[derive(Clone)]
pub(super) struct RootOuterTask {
    lifetime: Weak<RootExecutionLifetime>,
    epoch: u64,
}
impl RootOuterTask {
    pub(super) fn unconfirmed(&self, reason: impl Into<String>) {
        self.update(|task| {
            task.failure.get_or_insert(reason.into());
        });
    }
    #[cfg(test)]
    pub(super) async fn pause_before_actor_finish(&self) {
        let pause = self.lifetime.upgrade().and_then(|lifetime| {
            let pause = lifetime
                .before_actor_finish
                .lock()
                .ok()
                .and_then(|pause| pause.clone());
            pause
        });
        if let Some(pause) = pause {
            pause.entered.notify_one();
            pause.release.notified().await;
        }
    }
    pub(super) fn actor_finished(&self, result: io::Result<()>) {
        self.update(|task| match result {
            Ok(()) => task.actor_finished = true,
            Err(error) => {
                task.failure.get_or_insert(error.to_string());
            }
        });
    }
    pub(super) fn tail_completed(&self) {
        self.update(|task| task.tail_completed = true);
    }
    fn update(&self, update: impl FnOnce(&mut OuterTask)) {
        let Some(lifetime) = self.lifetime.upgrade() else {
            return;
        };
        let Ok(mut state) = lifetime.state.lock() else {
            return;
        };
        if let Some(task) = state.tasks.get_mut(&self.epoch) {
            update(task);
        }
    }

    /// Own the actual outer JoinHandle in a retained coordinator. The producer
    /// never records join success itself or substitutes terminal registry state.
    pub(super) fn retain(self, task: JoinHandle<()>) {
        let Some(lifetime) = self.lifetime.upgrade() else {
            tokio::spawn(async move {
                let _ = task.await;
            });
            return;
        };
        let weak = self.lifetime.clone();
        let epoch = self.epoch;
        // Register the coordinator before it can remove the pending task.
        // Shutdown cannot snapshot an empty set between those two events.
        let mut state = match lifetime.state.lock() {
            Ok(state) => state,
            Err(_) => {
                lifetime.closed.store(true, Ordering::Release);
                tokio::spawn(async move {
                    let _ = task.await;
                });
                return;
            }
        };
        let join = tokio::spawn(async move {
            let joined = task.await;
            let Some(lifetime) = weak.upgrade() else {
                return;
            };
            let Ok(mut state) = lifetime.state.lock() else {
                return;
            };
            let Some(task) = state.tasks.remove(&epoch) else {
                state
                    .failure
                    .get_or_insert("Root task join lost its exact handoff".into());
                return;
            };
            let failure = joined
                .err()
                .map(|e| e.to_string())
                .or(task.failure)
                .or_else(|| {
                    (!task.tail_completed).then(|| "Root outer tail did not complete".into())
                });
            if let Some(error) = failure {
                state.failure.get_or_insert(error);
            } else if lifetime.epoch.load(Ordering::Acquire) == epoch {
                state.joined = Some(JoinedOuter {
                    epoch,
                    original: task.original,
                    actor_finished: task.actor_finished,
                });
            }
            drop(state);
            lifetime.changed.notify_one();
        });
        state.joins.push(join);
    }
}

/// Held by future physical jobs through Arc. Dropping an HTTP waiter cannot
/// release a reservation still held by such a job. No role/scope is granted.
#[derive(Debug)]
pub(crate) struct RootResetAdmission {
    lifetime: Arc<RootExecutionLifetime>,
    epoch: u64,
    nonce: u64,
    operation_id: Uuid,
    original: RootObservation,
}
impl RootResetAdmission {
    pub(crate) fn operation_id(&self) -> Uuid {
        self.operation_id
    }
    pub(crate) fn check_session(&self, session: &Session) -> io::Result<()> {
        self.check_current()?;
        self.original.check(session)
    }
    /// Atomic-only final check: no publication mutex, Host map or Storage lock.
    pub(crate) fn check_current(&self) -> io::Result<()> {
        if self.lifetime.epoch.load(Ordering::Acquire) != self.epoch
            || self.lifetime.reset.load(Ordering::Acquire) != self.nonce
            || self.lifetime.closed.load(Ordering::Acquire)
        {
            return Err(busy("Root reset admission reservation changed"));
        }
        Ok(())
    }
    pub(crate) fn require_physical_closure(&self) -> io::Result<()> {
        self.check_current()?;
        Err(unsupported("Root resource closure issuer is not installed"))
    }
}

#[cfg(test)]
#[derive(Default)]
struct RootTailPause {
    entered: Notify,
    release: Notify,
}
impl Drop for RootResetAdmission {
    fn drop(&mut self) {
        let _ = self.lifetime.reset.compare_exchange(
            self.nonce,
            0,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }
}
fn busy(reason: impl fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::WouldBlock, reason.to_string())
}
fn unsupported(reason: impl fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::Unsupported, reason.to_string())
}

#[cfg(test)]
#[path = "root_execution_lifetime_tests.rs"]
mod tests;
