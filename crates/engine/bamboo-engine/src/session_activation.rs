//! Runtime-scoped logical-session activation router.
//!
//! Durable delivery and execution reservation are separate operations. This
//! router closes their terminal race without treating an execution id, process,
//! or warm-worker mailbox as the durable session address.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex, RwLock as StdRwLock, Weak};

use async_trait::async_trait;
use bamboo_domain::{
    SessionActivationDisposition, SessionActivationError, SessionActivationPort, SessionInboxPort,
    SessionInboxWakeCandidate,
};
use chrono::{DateTime, Duration, Utc};
use tokio::sync::{mpsc, watch, Mutex, RwLock};

type AsyncRollback =
    Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> + Send + 'static>;

/// A reservation whose runner slot already exists but whose task has not yet
/// been launched. The router publishes the logical owner before calling
/// `launch`, so even an immediately-completing task participates in the
/// finalization handshake.
pub struct SessionActivationLaunch {
    pub run_id: String,
    launch: Option<Box<dyn FnOnce() + Send + 'static>>,
    rollback: Option<AsyncRollback>,
    rollback_completion: Option<Arc<RollbackCompletion>>,
}

struct RollbackCompletion {
    completed: watch::Sender<bool>,
}

impl RollbackCompletion {
    fn new() -> Arc<Self> {
        let (completed, _receiver) = watch::channel(false);
        Arc::new(Self { completed })
    }

    fn complete(&self) {
        self.completed.send_replace(true);
    }

    async fn wait(&self) {
        let mut completed = self.completed.subscribe();
        while !*completed.borrow() {
            if completed.changed().await.is_err() {
                break;
            }
        }
    }
}

impl SessionActivationLaunch {
    pub fn new(run_id: impl Into<String>, launch: impl FnOnce() + Send + 'static) -> Self {
        Self {
            run_id: run_id.into(),
            launch: Some(Box::new(launch)),
            rollback: None,
            rollback_completion: None,
        }
    }

    /// Build a launch backed by an already-reserved external runner slot.
    ///
    /// If the launch is dropped before `launch` commits, `rollback` must release
    /// that exact reservation. This makes cancellation between the spawner
    /// returning and the router publishing ownership recoverable.
    pub fn new_with_rollback(
        run_id: impl Into<String>,
        launch: impl FnOnce() + Send + 'static,
        rollback: impl FnOnce() + Send + 'static,
    ) -> Self {
        Self::new_with_async_rollback(run_id, launch, move || async move {
            rollback();
        })
    }

    /// Build a launch whose exact external runner reservation is released
    /// asynchronously if publication is cancelled. The router does not release
    /// its coalescing token until this future completes, preventing a newer
    /// activation from adopting the still-present unlaunched slot.
    pub fn new_with_async_rollback<F, Fut>(
        run_id: impl Into<String>,
        launch: impl FnOnce() + Send + 'static,
        rollback: F,
    ) -> Self
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let rollback_completion = RollbackCompletion::new();
        Self {
            run_id: run_id.into(),
            launch: Some(Box::new(launch)),
            rollback: Some(Box::new(move || Box::pin(rollback()))),
            rollback_completion: Some(rollback_completion),
        }
    }

    fn rollback_completion(&self) -> Option<Arc<RollbackCompletion>> {
        self.rollback_completion.clone()
    }

    fn launch(mut self) {
        if let Some(launch) = self.launch.take() {
            self.rollback = None;
            self.rollback_completion = None;
            launch();
        }
    }
}

impl Drop for SessionActivationLaunch {
    fn drop(&mut self) {
        if self.launch.is_some() {
            if let Some(rollback) = self.rollback.take() {
                let completion = self.rollback_completion.take();
                if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                    runtime.spawn(async move {
                        rollback().await;
                        if let Some(completion) = completion {
                            completion.complete();
                        }
                    });
                }
            }
        }
    }
}

impl fmt::Debug for SessionActivationLaunch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionActivationLaunch")
            .field("run_id", &self.run_id)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub enum SessionActivationReserveOutcome {
    /// Existing runner reservation succeeded. The returned launch must be
    /// invoked exactly once.
    Reserved(SessionActivationLaunch),
    /// Some non-router path already owns the existing runner reservation.
    AlreadyRunning { run_id: String },
    /// The target disappeared before reservation.
    NotFound,
    /// The inbox was drained by another valid owner before reservation.
    NoWork,
}

/// A logical session already belongs to a different exact activation run.
///
/// Registration is the last line of defence between independently scheduled
/// entry points. A collision is returned to the later caller without mutating
/// the existing owner.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SessionRunRegistrationError {
    #[error(
        "session activation owner collision for {target_session_id}: \
         existing run {existing_run_id}, attempted run {attempted_run_id}"
    )]
    OwnerCollision {
        target_session_id: String,
        existing_run_id: String,
        attempted_run_id: String,
    },
}

impl SessionRunRegistrationError {
    /// The exact run that already owns the logical session.
    pub fn existing_run_id(&self) -> &str {
        match self {
            Self::OwnerCollision {
                existing_run_id, ..
            } => existing_run_id,
        }
    }
}

/// Adapter to the owning runtime's existing runner reservation and spawn path.
///
/// Implementations must reserve through the same per-session runner registry as
/// user/resume execution. They must not start a task before returning
/// [`SessionActivationReserveOutcome::Reserved`].
#[async_trait]
pub trait SessionActivationSpawner: Send + Sync {
    async fn reserve_activation(
        &self,
        target_session_id: &str,
        inbox_generation: u64,
    ) -> Result<SessionActivationReserveOutcome, SessionActivationError>;
}

#[derive(Debug, Clone)]
struct ActiveOwner {
    run_id: String,
    /// Set only by a real registration at its Session-bound execution entry.
    session_created_at: Option<DateTime<Utc>>,
    finalizing: bool,
    registrations: usize,
    /// Scheduling boundary for this owner, not durable queue/authority state.
    /// Keep wait/Guardian outcomes together until this exact Run leaves.
    child_completions_need_successor: bool,
    /// Present only while an external actor driver is alive. The payload is a
    /// wake generation; that driver remains the single consumer of the
    /// canonical FileSessionInbox and forwards the claimed typed envelope.
    delivery_sink: Option<mpsc::UnboundedSender<u64>>,
}

#[derive(Debug)]
struct TargetActivationState {
    latest_generation: u64,
    /// Delivery notification only. Never participates in successor scheduling.
    latest_delivery_generation: u64,
    /// Highest generation for which this process already launched an execution.
    /// Adopting an independently running owner does not consume this retry. If a
    /// launched execution leaves the same poison claim pending,
    /// terminal finalization must not hot-loop provider runs forever. A newer
    /// generation may still trigger one fresh activation; process restart also
    /// resets this bounded retry guard.
    last_dispatched_generation: u64,
    /// Immediate messages can run ahead of an unreleased staged prefix. A
    /// later coordinator release permits one more dispatch even when its
    /// delivery generation is older, without retrying the same poison prefix.
    last_dispatched_coordinator_generation: u64,
    /// Advances only when the exact logical owner leaves. A recovered ready
    /// claim may be redispatched in the same generation after this edge.
    owner_completion_epoch: u64,
    last_recovery_attempt: Option<RecoveryAttempt>,
    owner: Option<ActiveOwner>,
    activation_reserved: bool,
    /// Identity of the current reservation attempt. A cancellation cleanup or
    /// late spawner result may only release the token it acquired.
    activation_token: u64,
    /// Incremented whenever an in-flight reservation attempt releases the
    /// reservation. A delivery that coalesces behind that attempt waits for
    /// this edge and re-evaluates ownership instead of returning a success that
    /// could be stranded when the earlier attempt reports NoWork or fails.
    activation_epoch: watch::Sender<u64>,
    notify: watch::Sender<u64>,
}

impl Default for TargetActivationState {
    fn default() -> Self {
        let (activation_epoch, _activation_receiver) = watch::channel(0);
        let (notify, _receiver) = watch::channel(0);
        Self {
            latest_generation: 0,
            latest_delivery_generation: 0,
            last_dispatched_generation: 0,
            last_dispatched_coordinator_generation: 0,
            owner_completion_epoch: 0,
            last_recovery_attempt: None,
            owner: None,
            activation_reserved: false,
            activation_token: 0,
            activation_epoch,
            notify,
        }
    }
}

const INITIAL_RECOVERY_BACKOFF: Duration = Duration::seconds(30);
const MAX_RECOVERY_BACKOFF: Duration = Duration::minutes(30);

#[derive(Debug, Clone)]
struct RecoveryAttempt {
    candidate: SessionInboxWakeCandidate,
    completion_epoch: u64,
    backoff: Duration,
    next_retry_at: DateTime<Utc>,
    /// A reserved owner must finish before the same exact claim is retried.
    /// NoWork has no owner completion edge and is retried after backoff.
    requires_completion: bool,
}

fn recovery_retry_at(
    state: &TargetActivationState,
    candidate: &SessionInboxWakeCandidate,
    now: DateTime<Utc>,
) -> Option<Option<DateTime<Utc>>> {
    let previous = state
        .last_recovery_attempt
        .as_ref()
        .filter(|attempt| attempt.candidate == *candidate)?;
    if previous.requires_completion && state.owner_completion_epoch <= previous.completion_epoch {
        return Some(None);
    }
    (now < previous.next_retry_at).then_some(Some(previous.next_retry_at))
}

fn record_recovery_attempt(
    state: &mut TargetActivationState,
    candidate: SessionInboxWakeCandidate,
    now: DateTime<Utc>,
    requires_completion: bool,
) {
    let backoff = state
        .last_recovery_attempt
        .as_ref()
        .filter(|attempt| attempt.candidate == candidate)
        .map_or(INITIAL_RECOVERY_BACKOFF, |attempt| {
            (attempt.backoff * 2).min(MAX_RECOVERY_BACKOFF)
        });
    state.last_recovery_attempt = Some(RecoveryAttempt {
        candidate,
        completion_epoch: state.owner_completion_epoch,
        backoff,
        next_retry_at: now + backoff,
        requires_completion,
    });
}

/// Result of one exact Inbox wake inspection and router handoff. A due time
/// is a scheduling hint; the next attempt re-reads the canonical queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionWakeReconcileResult {
    pub disposition: Option<SessionActivationDisposition>,
    pub next_due_at: Option<DateTime<Utc>>,
}

fn reserve_activation_token(state: &mut TargetActivationState) -> u64 {
    state.activation_reserved = true;
    state.activation_token = state.activation_token.wrapping_add(1).max(1);
    state.activation_token
}

fn release_activation_token(state: &mut TargetActivationState, token: u64) -> bool {
    if !state.activation_reserved || state.activation_token != token {
        return false;
    }
    state.activation_reserved = false;
    let next_epoch = (*state.activation_epoch.borrow()).wrapping_add(1);
    state.activation_epoch.send_replace(next_epoch);
    true
}

fn inbox_activation_error(error: bamboo_domain::SessionInboxError) -> SessionActivationError {
    match error {
        bamboo_domain::SessionInboxError::TargetNotFound(target) => {
            SessionActivationError::TargetNotFound(target)
        }
        error => SessionActivationError::Internal(error.to_string()),
    }
}

/// Cancellation lease for one router reservation attempt. Every await after
/// publishing `activation_reserved` is covered; dropping the caller releases
/// only its exact token and wakes coalesced deliveries. Finalization-owned
/// attempts additionally schedule one bounded retry because their racing
/// producer already returned a coalesced success and is no longer waiting.
struct ActivationReservationLease {
    router: SessionActivationRouter,
    target_session_id: String,
    token: u64,
    recover_on_drop: bool,
    armed: bool,
    rollback_completion: Option<Arc<RollbackCompletion>>,
}

impl ActivationReservationLease {
    fn new(
        router: SessionActivationRouter,
        target_session_id: &str,
        token: u64,
        recover_on_drop: bool,
    ) -> Self {
        Self {
            router,
            target_session_id: target_session_id.to_string(),
            token,
            recover_on_drop,
            armed: true,
            rollback_completion: None,
        }
    }

    fn wait_for_rollback(&mut self, completion: Option<Arc<RollbackCompletion>>) {
        self.rollback_completion = completion;
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ActivationReservationLease {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let router = self.router.clone();
        let target_session_id = self.target_session_id.clone();
        let token = self.token;
        let recover_on_drop = self.recover_on_drop;
        let rollback_completion = self.rollback_completion.take();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if let Some(completion) = rollback_completion {
                    completion.wait().await;
                }
                router
                    .recover_cancelled_reservation(&target_session_id, token, recover_on_drop)
                    .await;
            });
        }
    }
}

struct ExpectedRunDeliveryGuard {
    target_session_id: String,
    session_created_at: DateTime<Utc>,
    run_id: String,
    states: Arc<Mutex<HashMap<String, TargetActivationState>>>,
    _publication: tokio::sync::OwnedMutexGuard<()>,
}

impl bamboo_domain::SessionRunDeliveryGuard for ExpectedRunDeliveryGuard {
    fn matches(
        &self,
        target_session_id: &str,
        target_created_at: DateTime<Utc>,
        expected_run_id: &str,
    ) -> bool {
        self.target_session_id == target_session_id
            && self.session_created_at == target_created_at
            && self.run_id == expected_run_id
    }

    fn notify_committed_delivery(&self, inbox_generation: u64) -> bool {
        // The blocking publication job already holds this target's fence.
        // Take the global map only for its short notification update.
        let mut states = self.states.blocking_lock();
        let Some(state) = states.get_mut(&self.target_session_id) else {
            return false;
        };
        let Some(owner) = state.owner.as_mut().filter(|owner| {
            owner.run_id == self.run_id
                && owner.session_created_at == Some(self.session_created_at)
                && !owner.finalizing
                && owner.registrations > 0
        }) else {
            return false;
        };
        // Same QueueOnly notification contract as v2: never reserve a successor.
        state.latest_delivery_generation = state.latest_delivery_generation.max(inbox_generation);
        state.notify.send_replace(inbox_generation);
        if owner
            .delivery_sink
            .as_ref()
            .is_some_and(|sink| sink.send(inbox_generation).is_err())
        {
            owner.delivery_sink = None;
        }
        true
    }
}

/// One router per owning runtime/AppState. All keys are logical Session ids.
#[derive(Clone, Default)]
pub struct SessionActivationRouter {
    states: Arc<Mutex<HashMap<String, TargetActivationState>>>,
    /// Live operations for one logical target share a fence. Weak entries are
    /// pruned on access; idle/deleted targets retain no separate lock state.
    publication_fences: TargetPublicationFences,
    spawner: Arc<RwLock<Option<Arc<dyn SessionActivationSpawner>>>>,
    inbox: Arc<StdRwLock<Option<Arc<dyn SessionInboxPort>>>>,
    #[cfg(test)]
    terminal_inspection_pause: Arc<Mutex<Option<TerminalInspectionPause>>>,
}

type TargetPublicationFences = Arc<StdMutex<HashMap<String, Weak<Mutex<()>>>>>;

#[cfg(test)]
type TerminalInspectionPause = (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);

type AbortCleanup =
    Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> + Send + 'static>;

/// Exact-run ownership lease returned by
/// [`SessionActivationRouter::register_run`].
///
/// The lease deliberately owns the safe-point receiver. Dropping it before the
/// normal finalization handshake schedules exact-run cleanup, reconciles the
/// durable activation watermark, and gives the router one chance to launch a
/// successor. A stale lease can never clear a newer owner.
pub struct SessionRunRegistration {
    router: Arc<SessionActivationRouter>,
    target_session_id: String,
    run_id: String,
    notifications: Option<watch::Receiver<u64>>,
    abort_cleanup: Option<AbortCleanup>,
    quarantine_on_drop: bool,
    // The host binds the actual opaque scope before polling direct execution.
    // It is not a Session/model field and cannot grant or weaken authority.
    tool_suspension_observation: Option<bamboo_agent_core::tools::SuspensionRestriction>,
    selected_input_wakes: Option<watch::Receiver<u64>>,
    armed: bool,
}

/// Private proof derived from a real registration, never reconstructed from
/// metadata or the presence of another caller's active run.
#[derive(Clone)]
pub(crate) struct SessionRunOwnership {
    router: Arc<SessionActivationRouter>,
    target_session_id: String,
    run_id: String,
}

impl SessionRunOwnership {
    pub(crate) fn matches_router(&self, router: &Arc<SessionActivationRouter>) -> bool {
        Arc::ptr_eq(&self.router, router)
    }

    pub(crate) async fn is_current(&self, session_id: &str) -> bool {
        self.target_session_id == session_id && self.router.owns_run(session_id, &self.run_id).await
    }
}

impl SessionRunRegistration {
    pub(crate) fn bind_tool_suspension_observation(
        &mut self,
        token: bamboo_agent_core::tools::SuspensionRestriction,
    ) {
        self.tool_suspension_observation = Some(token);
    }

    pub(crate) fn observed_forbidden_running(&self) -> bool {
        self.tool_suspension_observation
            .as_ref()
            .is_some_and(|token| token.observation().forbidden_running_seen)
    }

    pub(crate) fn ownership(&self) -> SessionRunOwnership {
        SessionRunOwnership {
            router: self.router.clone(),
            target_session_id: self.target_session_id.clone(),
            run_id: self.run_id.clone(),
        }
    }

    /// Observe independent producer wakeups without consuming the runtime's
    /// normal receiver. A selected input must not promote unrelated durable
    /// backlog merely by releasing its own registration. This is an existing
    /// watch subscription, not a second routing state or durable grant.
    pub(crate) fn restrict_to_selected_input(&mut self) {
        if self.selected_input_wakes.is_none() {
            self.selected_input_wakes = self.notifications.clone();
        }
    }

    pub fn notifications_mut(&mut self) -> &mut watch::Receiver<u64> {
        self.notifications
            .as_mut()
            .expect("run registration notifications are live before finalization")
    }

    /// Run host-specific exact-reservation cleanup before an abandoned owner
    /// asks the router to reserve a successor.
    pub fn set_abort_cleanup<F, Fut>(&mut self, cleanup: F)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.abort_cleanup = Some(Box::new(move || Box::pin(cleanup())));
    }

    pub async fn begin_finalization(&mut self) {
        self.router
            .begin_finalization(&self.target_session_id, &self.run_id)
            .await;
    }

    /// Physical execution now outlives this lease's future. Unexpected unwind
    /// must retain ownership until backend reconciliation, never launch a
    /// successor through the ordinary abandoned-startup cleanup.
    pub(crate) fn quarantine_on_drop(&mut self) {
        self.quarantine_on_drop = true;
    }

    /// Keep this exact logical owner finalizing when its physical stop or
    /// terminal delivery is unconfirmed. Dropping this lease must not run the
    /// usual abandoned-owner recovery and launch a successor.
    ///
    /// Recovery must explicitly reconcile the retained owner before finishing
    /// finalization. Disarm before the await so cancelling quarantine itself
    /// cannot accidentally release the owner.
    pub async fn quarantine(mut self) {
        self.armed = false;
        self.abort_cleanup = None;
        self.router
            .begin_finalization(&self.target_session_id, &self.run_id)
            .await;
        self.notifications.take();
    }

    /// Synchronously abandon this exact ownership lease.
    ///
    /// Most cancellation paths can rely on [`Drop`], which schedules the same
    /// cleanup. Startup paths that must return a truthful retryable response
    /// use this method so the runner slot and router owner are released before
    /// the response becomes observable. Cleanup runs in a detached owned task:
    /// cancelling the caller's wait cannot strand an already-taken host
    /// cleanup closure or this router owner.
    pub async fn abandon(mut self) {
        self.armed = false;
        if self.selected_input_wakes.is_some() {
            drop(self.notifications.take());
        }
        let abort_cleanup = self.abort_cleanup.take();
        let router = self.router.clone();
        let target_session_id = self.target_session_id.clone();
        let run_id = self.run_id.clone();
        let cleanup_target_session_id = target_session_id.clone();
        let cleanup_run_id = run_id.clone();
        let selected_input_wakes = self.selected_input_wakes.take();
        let cleanup = tokio::spawn(async move {
            if let Some(cleanup) = abort_cleanup {
                cleanup().await;
            }
            router
                .cleanup_abandoned_registration(
                    &cleanup_target_session_id,
                    &cleanup_run_id,
                    selected_input_wakes,
                )
                .await;
        });
        if let Err(error) = cleanup.await {
            tracing::error!(
                %target_session_id,
                %run_id,
                %error,
                "detached SessionInbox registration cleanup failed"
            );
        }
    }

    /// Complete the normal exact-owner handshake. The Drop fallback remains
    /// armed across the await and is disabled only after finalization returns.
    pub async fn finish(
        mut self,
        admitted_generation: u64,
    ) -> Result<Option<SessionActivationDisposition>, SessionActivationError> {
        // The router compacts caught-up routing state only after the last
        // safe-point receiver is gone.
        drop(self.notifications.take());
        let released = match self.selected_input_wakes.as_ref() {
            Some(wakes) => {
                self.router
                    .release_selected_if_unnotified(&self.target_session_id, &self.run_id, wakes)
                    .await
            }
            None => false,
        };
        let result = if released {
            Ok(None)
        } else {
            self.router
                .finish_finalization(&self.target_session_id, &self.run_id, admitted_generation)
                .await
        };
        if result.is_ok() {
            self.armed = false;
            self.abort_cleanup = None;
        }
        result
    }
}

impl Drop for SessionRunRegistration {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let router = self.router.clone();
        let target_session_id = self.target_session_id.clone();
        let run_id = self.run_id.clone();
        let abort_cleanup = self.abort_cleanup.take();
        let quarantine = self.quarantine_on_drop || self.observed_forbidden_running();
        if self.selected_input_wakes.is_some() {
            drop(self.notifications.take());
        }
        let selected_input_wakes = self.selected_input_wakes.take();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if quarantine {
                    router.begin_finalization(&target_session_id, &run_id).await;
                    return;
                }
                if let Some(cleanup) = abort_cleanup {
                    cleanup().await;
                }
                router
                    .cleanup_abandoned_registration(
                        &target_session_id,
                        &run_id,
                        selected_input_wakes,
                    )
                    .await;
            });
        }
    }
}

impl SessionActivationRouter {
    fn publication_fence(&self, target: &str) -> Arc<Mutex<()>> {
        let mut fences = self
            .publication_fences
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        fences.retain(|_, fence| fence.strong_count() > 0);
        if let Some(fence) = fences.get(target).and_then(Weak::upgrade) {
            return fence;
        }
        let fence = Arc::new(Mutex::new(()));
        fences.insert(target.into(), Arc::downgrade(&fence));
        fence
    }

    async fn lock_publication(&self, target: &str) -> tokio::sync::OwnedMutexGuard<()> {
        // Always acquire this target before the map. Never await a target
        // publication while holding the runtime-wide states mutex.
        self.publication_fence(target).lock_owned().await
    }

    async fn coordinator_generation(&self, target: &str) -> Result<u64, SessionActivationError> {
        let inbox = self
            .inbox
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        match inbox {
            Some(inbox) => inbox
                .coordinator_activation_generation(target)
                .await
                .map_err(inbox_activation_error),
            None => Ok(0),
        }
    }

    async fn released_pending_generation(
        &self,
        target: &str,
        after: u64,
    ) -> Result<u64, SessionActivationError> {
        if self.coordinator_generation(target).await? <= after {
            return Ok(0);
        }
        let inbox = self
            .inbox
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        match inbox {
            Some(inbox) => {
                let backlog = inbox
                    .inspect(target)
                    .await
                    .map_err(inbox_activation_error)?;
                Ok(
                    if backlog.activation_pending() && backlog.coordinator_generation > after {
                        backlog.coordinator_generation
                    } else {
                        0
                    },
                )
            }
            None => Ok(0),
        }
    }

    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Late-bind the real server/SDK reservation adapter after its dependency
    /// graph has been assembled.
    pub async fn set_spawner(&self, spawner: Arc<dyn SessionActivationSpawner>) {
        *self.spawner.write().await = Some(spawner);
    }

    /// Snapshot the current logical run, including its terminal handoff.
    pub async fn current_run_id(&self, target_session_id: &str) -> Option<String> {
        self.states
            .lock()
            .await
            .get(target_session_id)
            .and_then(|state| state.owner.as_ref().map(|owner| owner.run_id.clone()))
    }

    /// Select a completion's successor fence under the current owner lock.
    /// Once a wait or Guardian transition belongs to this owner, later sibling
    /// callbacks keep that boundary even after the durable wait was cleared.
    /// A background-only reasoning owner can instead consume its next safe point.
    pub(crate) async fn child_completion_successor_run(
        &self,
        target_session_id: &str,
        wait_or_guardian_transition: bool,
        previous_fenced_run: Option<&str>,
    ) -> Option<String> {
        let mut states = self.states.lock().await;
        let owner = states.get_mut(target_session_id)?.owner.as_mut()?;
        owner.child_completions_need_successor |=
            wait_or_guardian_transition || previous_fenced_run == Some(owner.run_id.as_str());
        (owner.finalizing || owner.child_completions_need_successor).then(|| owner.run_id.clone())
    }

    /// Bind the durable inbox used by abandoned-run reconciliation.
    pub fn set_inbox(&self, inbox: Arc<dyn SessionInboxPort>) {
        *self
            .inbox
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(inbox);
    }

    /// Reconcile one target against a storage-locked proof of claimable work.
    /// A later claim still CAS-checks the lease and activation policy. The
    /// router keeps only a bounded retry guard, never queue truth.
    pub async fn reconcile_wake(
        &self,
        target_session_id: &str,
        now: DateTime<Utc>,
    ) -> Result<SessionWakeReconcileResult, SessionActivationError> {
        let inbox = self
            .inbox
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .ok_or_else(|| {
                SessionActivationError::Internal("session inbox is not configured".into())
            })?;
        let readiness = inbox
            .inspect_wake_readiness(target_session_id, now)
            .await
            .map_err(inbox_activation_error)?;
        let Some(candidate) = readiness.ready else {
            return Ok(SessionWakeReconcileResult {
                disposition: None,
                next_due_at: readiness.next_due_at,
            });
        };
        let mut result = self
            .request_recovery_activation(target_session_id, candidate, now)
            .await?;
        result.next_due_at = match (result.next_due_at, readiness.next_due_at) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, right) => left.or(right),
        };
        Ok(result)
    }

    /// A precise grant can open a delivery below an already dispatched high
    /// watermark. Use the existing bounded recovery handshake for that hole;
    /// ordinary same-generation leftovers keep their existing retry cadence.
    async fn reconcile_older_message(
        &self,
        target: &str,
    ) -> Result<Option<SessionActivationDisposition>, SessionActivationError> {
        let high = self
            .states
            .lock()
            .await
            .get(target)
            .map_or(0, |state| state.last_dispatched_generation);
        self.reconcile_message_below(target, high).await
    }

    async fn reconcile_message_below(
        &self,
        target: &str,
        high: u64,
    ) -> Result<Option<SessionActivationDisposition>, SessionActivationError> {
        if high == 0 {
            return Ok(None);
        }
        let inbox = self
            .inbox
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(inbox) = inbox else {
            return Ok(None);
        };
        let now = Utc::now();
        let candidate = inbox
            .inspect_wake_readiness(target, now)
            .await
            .map_err(inbox_activation_error)?
            .ready;
        match candidate.filter(|candidate| {
            candidate.generation < high
                // Only an explicit coordinator upgrade opens this older hole.
                // A prefix may already cover the delivery at Respect policy.
                && candidate.coordinator_message_policy
                    == Some(bamboo_domain::SessionActivationPolicy::InterruptSpecificWait)
        }) {
            Some(candidate) => self
                .request_recovery_activation(target, candidate, now)
                .await
                .map(|result| result.disposition),
            None => Ok(None),
        }
    }

    #[cfg(test)]
    async fn pause_after_terminal_inspection(&self) {
        let pause = self.terminal_inspection_pause.lock().await.take();
        if let Some((entered, release)) = pause {
            entered.notify_one();
            release.notified().await;
        }
    }

    async fn request_recovery_activation(
        &self,
        target_session_id: &str,
        candidate: SessionInboxWakeCandidate,
        now: DateTime<Utc>,
    ) -> Result<SessionWakeReconcileResult, SessionActivationError> {
        loop {
            let (reservation_wait, reservation_token) = {
                let mut states = self.states.lock().await;
                let state = states.entry(target_session_id.to_string()).or_default();
                state.latest_generation = state.latest_generation.max(candidate.generation);
                if let Some(owner) = state.owner.as_mut() {
                    if !owner.finalizing {
                        state.notify.send_replace(candidate.generation);
                        if owner
                            .delivery_sink
                            .as_ref()
                            .is_some_and(|sink| sink.send(candidate.generation).is_err())
                        {
                            owner.delivery_sink = None;
                        }
                        return Ok(SessionWakeReconcileResult {
                            disposition: Some(SessionActivationDisposition::ActiveNotified),
                            next_due_at: None,
                        });
                    }
                    return Ok(SessionWakeReconcileResult {
                        disposition: Some(SessionActivationDisposition::ActivationCoalesced),
                        next_due_at: None,
                    });
                }
                if let Some(next_due_at) = recovery_retry_at(state, &candidate, now) {
                    return Ok(SessionWakeReconcileResult {
                        disposition: None,
                        next_due_at,
                    });
                } else if state.activation_reserved {
                    (Some(state.activation_epoch.subscribe()), None)
                } else {
                    let token = reserve_activation_token(state);
                    (None, Some(token))
                }
            };

            if let Some(mut reservation_wait) = reservation_wait {
                if reservation_wait.changed().await.is_err() {
                    return Err(SessionActivationError::TargetNotFound(
                        target_session_id.to_string(),
                    ));
                }
                continue;
            }
            let token = reservation_token.expect("recovery activation owns reservation token");
            let disposition = self
                .dispatch_reserved_with_recovery(
                    target_session_id,
                    candidate.generation,
                    token,
                    false,
                    Some((candidate, now)),
                )
                .await?;
            return Ok(SessionWakeReconcileResult {
                disposition: Some(disposition),
                next_due_at: None,
            });
        }
    }

    #[cfg(test)]
    pub(crate) async fn hold_state_lock_for_test(
        &self,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    ) {
        let _states = self.states.lock().await;
        entered.notify_one();
        release.notified().await;
    }

    #[cfg(test)]
    pub(crate) async fn install_owner_placeholder_for_test(
        &self,
        target_session_id: &str,
        run_id: &str,
    ) {
        let _publication = self.lock_publication(target_session_id).await;
        let mut states = self.states.lock().await;
        states
            .entry(target_session_id.to_string())
            .or_default()
            .owner = Some(ActiveOwner {
            run_id: run_id.to_string(),
            session_created_at: None,
            finalizing: false,
            registrations: 0,
            child_completions_need_successor: false,
            delivery_sink: None,
        });
    }

    /// Register an execution created by any entry point. The returned receiver
    /// is a safe-point wake signal; the loop also drains the durable inbox at
    /// every round boundary, so missed/coalesced notifications do not lose data.
    pub async fn register_run(
        self: &Arc<Self>,
        target_session_id: &str,
        run_id: &str,
    ) -> Result<SessionRunRegistration, SessionRunRegistrationError> {
        self.register_run_with_birth(target_session_id, run_id, None)
            .await
    }

    /// Register from the actual execution's Session, enabling birth-fenced
    /// steer. This is a trusted host entrypoint, not model-supplied authority.
    pub async fn register_session_run(
        self: &Arc<Self>,
        session: &bamboo_domain::Session,
        run_id: &str,
    ) -> Result<SessionRunRegistration, SessionRunRegistrationError> {
        self.register_run_with_birth(&session.id, run_id, Some(session.created_at))
            .await
    }

    pub(crate) async fn bind_run_lifetime(
        self: &Arc<Self>,
        ownership: &SessionRunOwnership,
        session: &bamboo_domain::Session,
    ) -> bool {
        if !ownership.matches_router(self) || ownership.target_session_id != session.id {
            return false;
        }
        let _publication = self.lock_publication(&session.id).await;
        let mut states = self.states.lock().await;
        let Some(owner) = states
            .get_mut(&session.id)
            .and_then(|state| state.owner.as_mut())
        else {
            return false;
        };
        if owner.run_id != ownership.run_id
            || owner.finalizing
            || owner.registrations == 0
            || owner
                .session_created_at
                .is_some_and(|birth| birth != session.created_at)
        {
            return false;
        }
        owner.session_created_at = Some(session.created_at);
        true
    }

    async fn register_run_with_birth(
        self: &Arc<Self>,
        target_session_id: &str,
        run_id: &str,
        birth: Option<DateTime<Utc>>,
    ) -> Result<SessionRunRegistration, SessionRunRegistrationError> {
        loop {
            let (notifications, reservation_wait) = {
                let _publication = self.lock_publication(target_session_id).await;
                let mut states = self.states.lock().await;
                let state = states.entry(target_session_id.to_string()).or_default();
                match state.owner.as_mut() {
                    Some(owner)
                        if owner.run_id == run_id
                            && owner.registrations == 0
                            && !owner.finalizing =>
                    {
                        // `dispatch_reserved` publishes the exact owner before
                        // making the task runnable. The task converts that
                        // placeholder into the one live registration here.
                        if owner
                            .session_created_at
                            .is_some_and(|existing| birth.is_some_and(|birth| existing != birth))
                        {
                            return Err(SessionRunRegistrationError::OwnerCollision {
                                target_session_id: target_session_id.into(),
                                existing_run_id: owner.run_id.clone(),
                                attempted_run_id: run_id.into(),
                            });
                        }
                        if birth.is_some() {
                            owner.session_created_at = birth;
                        }
                        owner.registrations = 1;
                        (Some(state.notify.subscribe()), None)
                    }
                    Some(owner) => {
                        return Err(SessionRunRegistrationError::OwnerCollision {
                            target_session_id: target_session_id.to_string(),
                            existing_run_id: owner.run_id.clone(),
                            attempted_run_id: run_id.to_string(),
                        });
                    }
                    None if state.activation_reserved => {
                        // The router's two-phase activation already owns the
                        // right to publish the next exact owner. A direct/manual
                        // entry must wait for that reservation to publish or
                        // roll back; superseding its token can strand a raw
                        // external runner behind a phantom router owner.
                        (None, Some(state.activation_epoch.subscribe()))
                    }
                    None => {
                        state.owner = Some(ActiveOwner {
                            run_id: run_id.to_string(),
                            session_created_at: birth,
                            finalizing: false,
                            registrations: 1,
                            child_completions_need_successor: false,
                            delivery_sink: None,
                        });
                        (Some(state.notify.subscribe()), None)
                    }
                }
            };

            if let Some(mut reservation_wait) = reservation_wait {
                let _ = reservation_wait.changed().await;
                continue;
            }
            return Ok(SessionRunRegistration {
                router: self.clone(),
                target_session_id: target_session_id.to_string(),
                run_id: run_id.to_string(),
                notifications,
                abort_cleanup: None,
                quarantine_on_drop: false,
                tool_suspension_observation: None,
                selected_input_wakes: None,
                armed: true,
            });
        }
    }

    /// Bind an external actor driver to the current logical-session owner.
    ///
    /// Returns the active run id used to correlate every forwarded claim and
    /// confirmation. A pending generation is pushed immediately, closing the
    /// delivery-before-bind race without making this in-memory signal durable.
    pub async fn attach_delivery_sink(
        &self,
        target_session_id: &str,
        sink: mpsc::UnboundedSender<u64>,
    ) -> Option<String> {
        let mut states = self.states.lock().await;
        let state = states.get_mut(target_session_id)?;
        let owner = state.owner.as_mut()?;
        if owner.finalizing {
            return None;
        }
        owner.delivery_sink = Some(sink.clone());
        let run_id = owner.run_id.clone();
        let delivery_generation = state
            .latest_generation
            .max(state.latest_delivery_generation);
        if delivery_generation > 0 && sink.send(delivery_generation).is_err() {
            owner.delivery_sink = None;
            return None;
        }
        Some(run_id)
    }

    /// Remove a driver only when it still belongs to the same activation run.
    /// A stale driver can therefore never unbind its successor.
    pub async fn detach_delivery_sink(&self, target_session_id: &str, run_id: &str) {
        let mut states = self.states.lock().await;
        let Some(state) = states.get_mut(target_session_id) else {
            return;
        };
        let Some(owner) = state.owner.as_mut() else {
            return;
        };
        if owner.run_id == run_id {
            owner.delivery_sink = None;
        }
    }

    /// True only while this exact activation run remains the logical owner.
    pub async fn owns_run(&self, target_session_id: &str, run_id: &str) -> bool {
        let states = self.states.lock().await;
        states
            .get(target_session_id)
            .and_then(|state| state.owner.as_ref())
            .is_some_and(|owner| owner.run_id == run_id && !owner.finalizing)
    }

    /// Mark the current run as finalizing before its runner slot becomes
    /// available. Deliveries racing this interval are retained in
    /// `latest_generation` and handed to one successor by
    /// [`finish_finalization`](Self::finish_finalization).
    pub async fn begin_finalization(&self, target_session_id: &str, run_id: &str) {
        let _publication = self.lock_publication(target_session_id).await;
        let mut states = self.states.lock().await;
        let state = states.entry(target_session_id.to_string()).or_default();
        match state.owner.as_mut() {
            Some(owner) if owner.run_id == run_id => owner.finalizing = true,
            Some(_) => {}
            None => {
                state.owner = Some(ActiveOwner {
                    run_id: run_id.to_string(),
                    session_created_at: None,
                    finalizing: true,
                    registrations: 0,
                    child_completions_need_successor: false,
                    delivery_sink: None,
                });
            }
        }
    }

    /// Close selection and notification atomically under the original router
    /// owner lock. A producer arriving afterwards sees no owner and reserves
    /// its own activation; a coalesced earlier notification retains the normal
    /// finalization/recovery path, including Child/Bash coordinator wakeups.
    async fn release_selected_if_unnotified(
        &self,
        target_session_id: &str,
        run_id: &str,
        wakes: &watch::Receiver<u64>,
    ) -> bool {
        let _publication = self.lock_publication(target_session_id).await;
        let mut states = self.states.lock().await;
        let Some(state) = states.get_mut(target_session_id) else {
            return true;
        };
        if !state
            .owner
            .as_ref()
            .is_some_and(|owner| owner.run_id == run_id)
        {
            return true;
        }
        if wakes.has_changed().unwrap_or(false) {
            return false;
        }
        state.owner = None;
        state.owner_completion_epoch = state.owner_completion_epoch.wrapping_add(1);
        if state.latest_generation == 0
            && !state.activation_reserved
            && state.notify.receiver_count() <= 1
            && state.activation_epoch.receiver_count() == 0
        {
            states.remove(target_session_id);
        }
        true
    }

    async fn cleanup_abandoned_registration(
        self: &Arc<Self>,
        target_session_id: &str,
        run_id: &str,
        selected_input_wakes: Option<watch::Receiver<u64>>,
    ) {
        if let Some(wakes) = selected_input_wakes {
            if self
                .release_selected_if_unnotified(target_session_id, run_id, &wakes)
                .await
            {
                return;
            }
        }
        {
            let _publication = self.lock_publication(target_session_id).await;
            let mut states = self.states.lock().await;
            let Some(state) = states.get_mut(target_session_id) else {
                return;
            };
            let Some(owner) = state.owner.as_mut() else {
                return;
            };
            if owner.run_id != run_id {
                return;
            }
            owner.registrations = 0;
            owner.finalizing = true;
        }

        // Delivery commits and its activation watermark are durable before the
        // producer asks the in-memory router to wake an owner. If that producer
        // or this execution is cancelled between those steps, recover the
        // authoritative eligible prefix rather than trusting only RAM.
        let inbox = self
            .inbox
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let (durable_generation, released_prefix) = match inbox {
            Some(inbox) => match inbox.inspect(target_session_id).await {
                Ok(backlog) if backlog.activation_pending() => (
                    backlog.activation_generation,
                    backlog.coordinator_generation,
                ),
                Ok(_) => (0, 0),
                Err(error) => {
                    tracing::warn!(
                        %target_session_id,
                        %run_id,
                        %error,
                        "failed to reconcile durable SessionInbox generation for abandoned run"
                    );
                    (0, 0)
                }
            },
            None => (0, 0),
        };

        #[cfg(test)]
        self.pause_after_terminal_inspection().await;
        let (reservation_to_dispatch, previous_high) = {
            let _publication = self.lock_publication(target_session_id).await;
            let mut states = self.states.lock().await;
            let Some(state) = states.get_mut(target_session_id) else {
                return;
            };
            if !state.owner.as_ref().is_some_and(|owner| {
                owner.run_id == run_id && owner.finalizing && owner.registrations == 0
            }) {
                return;
            }
            state.latest_generation = state.latest_generation.max(durable_generation);
            let previous_high = state.last_dispatched_generation;
            state.owner = None;
            state.owner_completion_epoch = state.owner_completion_epoch.wrapping_add(1);
            if (state.latest_generation > state.last_dispatched_generation
                || released_prefix > state.last_dispatched_coordinator_generation)
                && !state.activation_reserved
            {
                let generation = state.latest_generation;
                let token = reserve_activation_token(state);
                (Some((generation, token)), previous_high)
            } else {
                if state.latest_generation == 0
                    && !state.activation_reserved
                    && state.notify.receiver_count() == 0
                    && state.activation_epoch.receiver_count() == 0
                {
                    states.remove(target_session_id);
                }
                (None, previous_high)
            }
        };

        if let Some((generation, token)) = reservation_to_dispatch {
            if let Err(error) = self
                .dispatch_reserved(target_session_id, generation, token)
                .await
            {
                tracing::error!(
                    %target_session_id,
                    %run_id,
                    %error,
                    "failed to reserve successor for abandoned SessionInbox owner"
                );
            }
        } else {
            // A precise grant can race the pre-lock inspection while the
            // producer still observes this owner as finalizing. Re-read after
            // releasing the owner even when that earlier snapshot was empty.
            if let Err(error) = self
                .reconcile_message_below(target_session_id, previous_high)
                .await
            {
                tracing::warn!(%target_session_id, %error,
                    "failed to reconcile older eligible message after abandoned owner");
            }
        }
    }

    /// Release the terminal owner and, if the durable transcript cursor is
    /// behind a concurrently delivered inbox generation, reserve and launch
    /// exactly one successor using the injected real runtime adapter.
    pub async fn finish_finalization(
        &self,
        target_session_id: &str,
        run_id: &str,
        admitted_generation: u64,
    ) -> Result<Option<SessionActivationDisposition>, SessionActivationError> {
        // Deferred guidance may precede a later admitted message. A maximum
        // transcript cursor alone cannot prove that this authorized queue is empty.
        let inbox = self
            .inbox
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let durable_pending = match inbox {
            Some(inbox) => Some(
                inbox
                    .inspect(target_session_id)
                    .await
                    .map_err(|error| SessionActivationError::Internal(error.to_string()))?,
            ),
            None => None,
        };
        #[cfg(test)]
        self.pause_after_terminal_inspection().await;
        let (reservation_to_dispatch, previous_high) = {
            let _publication = self.lock_publication(target_session_id).await;
            let mut states = self.states.lock().await;
            let state = states.entry(target_session_id.to_string()).or_default();
            let previous_high = state.last_dispatched_generation;
            if state
                .owner
                .as_ref()
                .is_some_and(|owner| owner.run_id != run_id)
            {
                return Ok(None);
            }
            if state.owner.take().is_some() {
                state.owner_completion_epoch = state.owner_completion_epoch.wrapping_add(1);
            }
            let pending = durable_pending
                .as_ref()
                .is_some_and(|backlog| backlog.activation_pending());
            let released_prefix = durable_pending
                .as_ref()
                .filter(|_| pending)
                .map_or(0, |backlog| backlog.coordinator_generation);
            if let Some(backlog) = durable_pending.as_ref() {
                state.latest_generation =
                    state.latest_generation.max(backlog.activation_generation);
            }
            if (pending || state.latest_generation > admitted_generation)
                && (state.latest_generation > state.last_dispatched_generation
                    || released_prefix > state.last_dispatched_coordinator_generation)
                && !state.activation_reserved
            {
                let generation = state.latest_generation;
                let token = reserve_activation_token(state);
                (Some((generation, token)), previous_high)
            } else {
                // Both execution paths drop their safe-point receiver before
                // calling finish_finalization. Once the durable cursor has
                // caught up and no reservation exists, this target carries no
                // live routing state and must not remain in AppState forever.
                if state.latest_generation <= admitted_generation
                    && !pending
                    && state.owner.is_none()
                    && !state.activation_reserved
                    && state.notify.receiver_count() == 0
                    && state.activation_epoch.receiver_count() == 0
                {
                    states.remove(target_session_id);
                }
                (None, previous_high)
            }
        };

        match reservation_to_dispatch {
            Some((generation, token)) => self
                .dispatch_reserved(target_session_id, generation, token)
                .await
                .map(Some),
            // Keep the prior high even if the empty pre-lock snapshot allowed
            // state eviction. A late older grant still needs its one successor.
            None => {
                self.reconcile_message_below(target_session_id, previous_high)
                    .await
            }
        }
    }

    pub async fn subscribe(&self, target_session_id: &str) -> watch::Receiver<u64> {
        let mut states = self.states.lock().await;
        states
            .entry(target_session_id.to_string())
            .or_default()
            .notify
            .subscribe()
    }

    /// Release an activation attempt whose caller was cancelled or panicked.
    ///
    /// Finalization-racing producers already received a coalesced success while
    /// the old owner was visible, so no external waiter remains to retry after
    /// the reservation token is released. The first abandoned attempt therefore
    /// gets one detached, router-level retry for the newest still-undispatched
    /// generation. That retry is deliberately bounded: its own Drop releases
    /// the token but does not recursively hot-loop a broken spawner.
    async fn recover_cancelled_reservation(
        &self,
        target_session_id: &str,
        reservation_token: u64,
        recover_on_drop: bool,
    ) {
        let previous_prefix = {
            let mut states = self.states.lock().await;
            let Some(state) = states.get_mut(target_session_id) else {
                return;
            };
            if !release_activation_token(state, reservation_token) || !recover_on_drop {
                return;
            }
            state.last_dispatched_coordinator_generation
        };
        let released_prefix = self
            .released_pending_generation(target_session_id, previous_prefix)
            .await
            .unwrap_or(0);
        let reservation_to_dispatch = {
            let mut states = self.states.lock().await;
            let Some(state) = states.get_mut(target_session_id) else {
                return;
            };
            if state.owner.is_none()
                && (state.latest_generation > state.last_dispatched_generation
                    || released_prefix > state.last_dispatched_coordinator_generation)
                && !state.activation_reserved
            {
                let generation = state.latest_generation;
                let token = reserve_activation_token(state);
                Some((generation, token))
            } else {
                None
            }
        };

        if let Some((generation, token)) = reservation_to_dispatch {
            if let Err(error) = self
                .dispatch_reserved_with_recovery(target_session_id, generation, token, false, None)
                .await
            {
                tracing::error!(
                    %target_session_id,
                    %error,
                    "failed bounded retry after cancelled SessionInbox activation reservation"
                );
            }
        }
    }

    async fn dispatch_reserved(
        &self,
        target_session_id: &str,
        generation: u64,
        reservation_token: u64,
    ) -> Result<SessionActivationDisposition, SessionActivationError> {
        self.dispatch_reserved_with_recovery(
            target_session_id,
            generation,
            reservation_token,
            true,
            None,
        )
        .await
    }

    async fn dispatch_reserved_with_recovery(
        &self,
        target_session_id: &str,
        generation: u64,
        reservation_token: u64,
        recover_on_drop: bool,
        recovery: Option<(SessionInboxWakeCandidate, DateTime<Utc>)>,
    ) -> Result<SessionActivationDisposition, SessionActivationError> {
        let mut lease = ActivationReservationLease::new(
            self.clone(),
            target_session_id,
            reservation_token,
            recover_on_drop,
        );
        let Some(spawner) = self.spawner.read().await.clone() else {
            let mut states = self.states.lock().await;
            if let Some(state) = states.get_mut(target_session_id) {
                release_activation_token(state, reservation_token);
            }
            lease.disarm();
            return Err(SessionActivationError::Internal(
                "session activation spawner is not configured".to_string(),
            ));
        };

        // Sample before the spawner admits/launches its run. A coordinator
        // release arriving later remains fresh work for finalization.
        let coordinator_generation = self.coordinator_generation(target_session_id).await?;

        let outcome = spawner
            .reserve_activation(target_session_id, generation)
            .await;
        if let Ok(SessionActivationReserveOutcome::Reserved(launch)) = &outcome {
            lease.wait_for_rollback(launch.rollback_completion());
        }
        match outcome {
            Ok(SessionActivationReserveOutcome::Reserved(launch)) => {
                let run_id = launch.run_id.clone();
                {
                    let _publication = self.lock_publication(target_session_id).await;
                    let mut states = self.states.lock().await;
                    let state = states.entry(target_session_id.to_string()).or_default();
                    if !release_activation_token(state, reservation_token) {
                        // Cancellation recovery or a newer exact reservation
                        // may have superseded this attempt while its spawner
                        // was preparing. Direct/manual registration is
                        // serialized behind `activation_reserved`, so it
                        // cannot create an unrelated owner in this window.
                        // Dropping the unlaunched value rolls back its external
                        // slot.
                        lease.disarm();
                        return if state.owner.is_some() {
                            Ok(SessionActivationDisposition::ActiveNotified)
                        } else {
                            Err(SessionActivationError::Internal(
                                "session activation reservation was superseded".to_string(),
                            ))
                        };
                    }
                    state.owner = Some(ActiveOwner {
                        run_id,
                        session_created_at: None,
                        finalizing: false,
                        registrations: 0,
                        child_completions_need_successor: false,
                        delivery_sink: None,
                    });
                    state.last_dispatched_generation =
                        state.last_dispatched_generation.max(generation);
                    state.last_dispatched_coordinator_generation = state
                        .last_dispatched_coordinator_generation
                        .max(coordinator_generation);
                    if let Some((candidate, now)) = recovery {
                        record_recovery_attempt(state, candidate, now, true);
                    }
                }
                lease.disarm();
                // The existing runner slot is already reserved. Publish owner
                // state first, then make the task runnable exactly once.
                launch.launch();
                Ok(SessionActivationDisposition::ActivationReserved)
            }
            Ok(SessionActivationReserveOutcome::AlreadyRunning { run_id }) => {
                let _publication = self.lock_publication(target_session_id).await;
                let mut states = self.states.lock().await;
                let state = states.entry(target_session_id.to_string()).or_default();
                if !release_activation_token(state, reservation_token) {
                    lease.disarm();
                    return Ok(SessionActivationDisposition::ActiveNotified);
                }
                state.owner = Some(ActiveOwner {
                    run_id,
                    session_created_at: None,
                    finalizing: false,
                    registrations: 0,
                    child_completions_need_successor: false,
                    delivery_sink: None,
                });
                state.notify.send_replace(generation);
                if let Some((candidate, now)) = recovery {
                    record_recovery_attempt(state, candidate, now, true);
                }
                lease.disarm();
                Ok(SessionActivationDisposition::ActiveNotified)
            }
            Ok(SessionActivationReserveOutcome::NoWork) => {
                let mut states = self.states.lock().await;
                if let Some(state) = states.get_mut(target_session_id) {
                    if release_activation_token(state, reservation_token) {
                        if let Some((candidate, now)) = recovery {
                            record_recovery_attempt(state, candidate, now, false);
                        }
                    }
                }
                lease.disarm();
                Ok(SessionActivationDisposition::ActivationCoalesced)
            }
            Ok(SessionActivationReserveOutcome::NotFound) => {
                let _publication = self.lock_publication(target_session_id).await;
                let mut states = self.states.lock().await;
                if states
                    .get(target_session_id)
                    .is_some_and(|state| state.activation_token == reservation_token)
                {
                    states.remove(target_session_id);
                }
                lease.disarm();
                Err(SessionActivationError::TargetNotFound(
                    target_session_id.to_string(),
                ))
            }
            Err(error) => {
                let mut states = self.states.lock().await;
                if let Some(state) = states.get_mut(target_session_id) {
                    release_activation_token(state, reservation_token);
                }
                lease.disarm();
                Err(error)
            }
        }
    }
}

#[async_trait]
impl SessionActivationPort for SessionActivationRouter {
    fn retain_expected_run_blocking(
        &self,
        target_session_id: &str,
        target_created_at: DateTime<Utc>,
        expected_run_id: &str,
    ) -> Result<Arc<dyn bamboo_domain::SessionRunDeliveryGuard>, bamboo_domain::SessionRunConflict>
    {
        use bamboo_domain::{SessionRunConflict, SessionRunConflictReason};
        // Only this target is retained through disk publication. Every owner
        // installation/release/finalization takes the same target fence first.
        let publication = self
            .publication_fence(target_session_id)
            .blocking_lock_owned();
        let states = self.states.blocking_lock();
        let owner = states
            .get(target_session_id)
            .and_then(|state| state.owner.as_ref());
        let reason = match owner {
            None => Some(SessionRunConflictReason::NoActiveRun),
            Some(owner)
                if owner
                    .session_created_at
                    .is_some_and(|birth| birth != target_created_at) =>
            {
                Some(SessionRunConflictReason::SessionBirthChanged)
            }
            Some(owner) if owner.finalizing => Some(SessionRunConflictReason::Finalizing),
            Some(owner) if owner.registrations == 0 => {
                Some(SessionRunConflictReason::NotRegistered)
            }
            Some(owner) if owner.session_created_at.is_none() => {
                Some(SessionRunConflictReason::UnboundLifetime)
            }
            Some(owner) if owner.run_id != expected_run_id => {
                Some(SessionRunConflictReason::RunChanged)
            }
            Some(_) => None,
        };
        if let Some(reason) = reason {
            return Err(SessionRunConflict {
                reason,
                expected_run_id: expected_run_id.into(),
                observed_run_id: owner
                    .filter(|owner| owner.session_created_at == Some(target_created_at))
                    .map(|owner| owner.run_id.clone()),
            });
        }
        Ok(Arc::new(ExpectedRunDeliveryGuard {
            target_session_id: target_session_id.into(),
            session_created_at: target_created_at,
            run_id: expected_run_id.into(),
            states: self.states.clone(),
            _publication: publication,
        }))
    }

    async fn notify_active_delivery(&self, target_session_id: &str, inbox_generation: u64) -> bool {
        let mut states = self.states.lock().await;
        let Some(state) = states.get_mut(target_session_id) else {
            return false;
        };
        let Some(owner) = state.owner.as_mut().filter(|owner| !owner.finalizing) else {
            return false;
        };
        state.latest_delivery_generation = state.latest_delivery_generation.max(inbox_generation);
        state.notify.send_replace(inbox_generation);
        if owner
            .delivery_sink
            .as_ref()
            .is_some_and(|sink| sink.send(inbox_generation).is_err())
        {
            owner.delivery_sink = None;
        }
        true
    }

    async fn request_activation(
        &self,
        target_session_id: &str,
        inbox_generation: u64,
    ) -> Result<SessionActivationDisposition, SessionActivationError> {
        let previous_prefix = {
            let states = self.states.lock().await;
            states
                .get(target_session_id)
                .filter(|state| inbox_generation <= state.last_dispatched_generation)
                .map(|state| state.last_dispatched_coordinator_generation)
        };
        let released_prefix = match previous_prefix {
            Some(prefix) => {
                self.released_pending_generation(target_session_id, prefix)
                    .await?
            }
            None => 0,
        };
        loop {
            let (reservation_wait, reservation_token) = {
                let mut states = self.states.lock().await;
                let state = states.entry(target_session_id.to_string()).or_default();
                state.latest_generation = state.latest_generation.max(inbox_generation);
                if let Some(owner) = state.owner.as_mut() {
                    if !owner.finalizing {
                        state.notify.send_replace(inbox_generation);
                        if owner
                            .delivery_sink
                            .as_ref()
                            .is_some_and(|sink| sink.send(inbox_generation).is_err())
                        {
                            owner.delivery_sink = None;
                        }
                        return Ok(SessionActivationDisposition::ActiveNotified);
                    }
                    return Ok(SessionActivationDisposition::ActivationCoalesced);
                }
                if inbox_generation <= state.last_dispatched_generation
                    && released_prefix <= state.last_dispatched_coordinator_generation
                {
                    drop(states);
                    return self
                        .reconcile_older_message(target_session_id)
                        .await
                        .map(|result| {
                            result.unwrap_or(SessionActivationDisposition::ActivationCoalesced)
                        });
                }
                if state.activation_reserved {
                    (Some(state.activation_epoch.subscribe()), None)
                } else {
                    let token = reserve_activation_token(state);
                    (None, Some(token))
                }
            };

            if let Some(mut reservation_wait) = reservation_wait {
                // A coalesced caller does not claim success until the preceding
                // reservation publishes an owner or releases its slot. If that
                // attempt returns NoWork/error, this caller wakes and reserves
                // its own (newer) generation, so no durable delivery is left
                // behind a false-positive coalesced response.
                if reservation_wait.changed().await.is_err() {
                    return Err(SessionActivationError::TargetNotFound(
                        target_session_id.to_string(),
                    ));
                }
                continue;
            }

            let reservation_token =
                reservation_token.expect("non-waiting activation owns a reservation token");
            return self
                .dispatch_reserved_with_recovery(
                    target_session_id,
                    inbox_generation,
                    reservation_token,
                    false,
                    None,
                )
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::{Barrier, Notify};

    #[tokio::test]
    async fn wake_recovery_retries_same_candidate_only_after_owner_completion_and_backoff() {
        use bamboo_domain::{
            Session, SessionActivationPolicy, SessionInboxLimits, SessionMessageEnvelope, Storage,
        };
        use bamboo_storage::{FileSessionInbox, SessionStoreV2};

        let temp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(SessionStoreV2::new(temp.path().into()).await.unwrap());
        store
            .save_session(&Session::new("target", "model"))
            .await
            .unwrap();
        let inbox = Arc::new(FileSessionInbox::new(store, SessionInboxLimits::default()));
        inbox
            .deliver_with_activation_intent(
                &SessionMessageEnvelope::user_input("target", "still pending"),
                SessionActivationPolicy::RespectSpecificWait,
                None,
            )
            .await
            .unwrap();
        let router = SessionActivationRouter::new();
        router.set_inbox(inbox);
        let spawner = spawner();
        router.set_spawner(spawner.clone()).await;
        let now = Utc::now();
        assert_eq!(
            router
                .reconcile_wake("target", now)
                .await
                .unwrap()
                .disposition,
            Some(SessionActivationDisposition::ActivationReserved)
        );
        assert_eq!(
            router
                .reconcile_wake("target", now)
                .await
                .unwrap()
                .disposition,
            Some(SessionActivationDisposition::ActiveNotified)
        );
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 1);
        let mut registration = router.register_run("target", "run-1").await.unwrap();
        registration.begin_finalization().await;
        assert_eq!(registration.finish(0).await.unwrap(), None);

        let deferred = router
            .reconcile_wake("target", now + Duration::seconds(29))
            .await
            .unwrap();
        assert_eq!(deferred.disposition, None);
        assert_eq!(deferred.next_due_at, Some(now + INITIAL_RECOVERY_BACKOFF));
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 1);
        assert_eq!(
            router
                .reconcile_wake("target", now + INITIAL_RECOVERY_BACKOFF)
                .await
                .unwrap()
                .disposition,
            Some(SessionActivationDisposition::ActivationReserved)
        );
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn wake_recovery_waits_for_exact_owned_lease_expiry() {
        use bamboo_domain::{
            Session, SessionActivationPolicy, SessionInboxLeaseRequest, SessionInboxLimits,
            SessionMessageEnvelope, Storage,
        };
        use bamboo_storage::{FileSessionInbox, SessionStoreV2};

        let temp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(SessionStoreV2::new(temp.path().into()).await.unwrap());
        store
            .save_session(&Session::new("target", "model"))
            .await
            .unwrap();
        let inbox = Arc::new(FileSessionInbox::new(store, SessionInboxLimits::default()));
        inbox
            .deliver_with_activation_intent(
                &SessionMessageEnvelope::user_input("target", "unacknowledged"),
                SessionActivationPolicy::RespectSpecificWait,
                None,
            )
            .await
            .unwrap();
        let router = SessionActivationRouter::new();
        router.set_inbox(inbox.clone());
        let spawner = spawner();
        router.set_spawner(spawner.clone()).await;
        let now = Utc::now();
        assert_eq!(
            router
                .reconcile_wake("target", now)
                .await
                .unwrap()
                .disposition,
            Some(SessionActivationDisposition::ActivationReserved)
        );
        let claim = inbox
            .claim_owned(
                "target",
                1,
                None,
                &SessionInboxLeaseRequest {
                    consumer: bamboo_domain::SessionInboxConsumerId::new(),
                    now,
                    duration: Duration::seconds(10),
                },
            )
            .await
            .unwrap()
            .remove(0);
        let mut registration = router.register_run("target", "run-1").await.unwrap();
        registration.begin_finalization().await;
        assert_eq!(registration.finish(0).await.unwrap(), None);
        let deferred = router
            .reconcile_wake("target", claim.lease.expires_at - Duration::seconds(1))
            .await
            .unwrap();
        assert_eq!(deferred.disposition, None);
        assert_eq!(deferred.next_due_at, Some(claim.lease.expires_at));
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 1);
        assert_eq!(
            router
                .reconcile_wake("target", claim.lease.expires_at)
                .await
                .unwrap()
                .disposition,
            Some(SessionActivationDisposition::ActivationReserved)
        );
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn older_staged_release_after_immediate_ack_dispatches_once_without_poison_hot_loop() {
        use bamboo_domain::{
            Session, SessionActivationPolicy, SessionInboxLimits, SessionMessageEnvelope, Storage,
        };
        use bamboo_storage::{FileSessionInbox, SessionStoreV2};
        async fn release(
            inbox: &FileSessionInbox,
            receipt: &bamboo_domain::SessionInboxReceipt,
            precise: bool,
        ) {
            if precise {
                inbox
                    .mark_message_activation_eligible(
                        "parent",
                        receipt,
                        SessionActivationPolicy::InterruptSpecificWait,
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    inbox
                        .coordinator_activation_generation("parent")
                        .await
                        .unwrap(),
                    0
                );
            } else {
                inbox
                    .mark_activation_eligible(
                        "parent",
                        receipt.generation,
                        SessionActivationPolicy::RespectSpecificWait,
                    )
                    .await
                    .unwrap();
            }
        }
        for (release_during_finalization, precise) in
            [(false, false), (true, false), (false, true), (true, true)]
        {
            let temp = tempfile::TempDir::new().unwrap();
            let store = Arc::new(SessionStoreV2::new(temp.path().into()).await.unwrap());
            store
                .save_session(&Session::new("parent", "model"))
                .await
                .unwrap();
            let inbox = Arc::new(FileSessionInbox::new(
                store.clone(),
                SessionInboxLimits::default(),
            ));
            let staged = SessionMessageEnvelope::user_input("parent", "staged result");
            let first = inbox.deliver(&staged).await.unwrap();
            let immediate = SessionMessageEnvelope::user_input("parent", "immediate steer");
            let second = inbox
                .deliver_with_activation_intent(
                    &immediate,
                    SessionActivationPolicy::RespectSpecificWait,
                    None,
                )
                .await
                .unwrap();
            let reopened_store = Arc::new(SessionStoreV2::new(temp.path().into()).await.unwrap());
            let restarted = Arc::new(FileSessionInbox::new(
                reopened_store,
                SessionInboxLimits::default(),
            ));
            let router = SessionActivationRouter::new();
            router.set_inbox(restarted.clone());
            let spawner = spawner();
            router.set_spawner(spawner.clone()).await;
            // Keep the high-generation router state after an idle finalization
            // so the test exercises hole recovery rather than state eviction.
            let _observer = router.subscribe("parent").await;
            let backlog = restarted.inspect("parent").await.unwrap();
            assert_eq!(
                router
                    .request_activation("parent", backlog.activation_generation)
                    .await
                    .unwrap(),
                SessionActivationDisposition::ActivationReserved
            );
            let claims = restarted.claim("parent", 128).await.unwrap();
            assert_eq!(claims.len(), 1);
            assert_eq!(claims[0].generation, second.generation);
            restarted.ack("parent", &claims[0]).await.unwrap();
            let mut registration = router.register_run("parent", "run-1").await.unwrap();
            registration.begin_finalization().await;
            if release_during_finalization {
                release(&restarted, &first, precise).await;
                assert_eq!(
                    router
                        .request_activation("parent", first.generation)
                        .await
                        .unwrap(),
                    SessionActivationDisposition::ActivationCoalesced
                );
                assert_eq!(
                    registration.finish(second.generation).await.unwrap(),
                    Some(SessionActivationDisposition::ActivationReserved)
                );
            } else {
                assert_eq!(registration.finish(second.generation).await.unwrap(), None);
                release(&restarted, &first, precise).await;
                assert_eq!(
                    router
                        .request_activation("parent", first.generation)
                        .await
                        .unwrap(),
                    SessionActivationDisposition::ActivationReserved
                );
            }
            assert_eq!(spawner.reservations.load(Ordering::SeqCst), 2);
            let claims = restarted.claim("parent", 128).await.unwrap();
            assert_eq!(claims.len(), 1);
            assert_eq!(claims[0].envelope.id, staged.id);
            // An execution leaving the same claim pending gets no unbounded
            // retry just because its delivery generation is below the high one.
            let mut registration = router.register_run("parent", "run-2").await.unwrap();
            registration.begin_finalization().await;
            assert_eq!(registration.finish(second.generation).await.unwrap(), None);
            assert_eq!(
                router
                    .request_activation("parent", first.generation)
                    .await
                    .unwrap(),
                SessionActivationDisposition::ActivationCoalesced
            );
            assert_eq!(spawner.reservations.load(Ordering::SeqCst), 2);
            restarted.ack("parent", &claims[0]).await.unwrap();
        }
    }

    #[tokio::test]
    async fn precise_older_interrupt_below_released_prefix_reaches_one_successor() {
        use bamboo_domain::{
            Session, SessionActivationPolicy, SessionInboxLimits, SessionMessageEnvelope, Storage,
        };
        use bamboo_storage::{FileSessionInbox, SessionStoreV2};
        for with_older_sibling in [false, true] {
            let temp = tempfile::TempDir::new().unwrap();
            let store = Arc::new(SessionStoreV2::new(temp.path().into()).await.unwrap());
            store
                .save_session(&Session::new("parent", "model"))
                .await
                .unwrap();
            let inbox = Arc::new(FileSessionInbox::new(store, SessionInboxLimits::default()));
            let sibling = if with_older_sibling {
                Some(
                    inbox
                        .deliver(&SessionMessageEnvelope::user_input(
                            "parent",
                            "older Respect sibling",
                        ))
                        .await
                        .unwrap(),
                )
            } else {
                None
            };
            let older = inbox
                .deliver(&SessionMessageEnvelope::user_input("parent", "older child"))
                .await
                .unwrap();
            let newer = inbox
                .deliver(&SessionMessageEnvelope::user_input("parent", "newer Bash"))
                .await
                .unwrap();
            inbox
                .mark_activation_eligible(
                    "parent",
                    newer.generation,
                    SessionActivationPolicy::RespectSpecificWait,
                )
                .await
                .unwrap();
            let router = SessionActivationRouter::new();
            router.set_inbox(inbox.clone());
            let spawner = spawner();
            router.set_spawner(spawner.clone()).await;
            router
                .request_activation("parent", newer.generation)
                .await
                .unwrap();
            let claims = inbox.claim("parent", 128).await.unwrap();
            assert_eq!(claims.len(), 2 + usize::from(with_older_sibling));
            let older_claim = claims
                .iter()
                .find(|claim| claim.envelope.id == older.id)
                .unwrap();
            assert_eq!(older_claim.generation, older.generation);
            assert_eq!(
                older_claim.activation_policy,
                SessionActivationPolicy::RespectSpecificWait
            );
            let newer_claim = claims
                .iter()
                .find(|claim| claim.envelope.id == newer.id)
                .unwrap();
            inbox.ack("parent", newer_claim).await.unwrap();
            let mut owner = router.register_run("parent", "run-1").await.unwrap();
            owner.begin_finalization().await;
            assert_eq!(owner.finish(newer.generation).await.unwrap(), None);
            assert_eq!(spawner.reservations.load(Ordering::SeqCst), 1);

            inbox
                .mark_message_activation_eligible(
                    "parent",
                    &older,
                    SessionActivationPolicy::InterruptSpecificWait,
                )
                .await
                .unwrap();
            let candidate = inbox
                .inspect_wake_readiness("parent", Utc::now())
                .await
                .unwrap()
                .ready
                .unwrap();
            assert_eq!(candidate.coordinator_generation, newer.generation);
            assert_eq!(
                candidate.id, older.id,
                "a Respect sibling cannot obscure the precise wake"
            );
            assert_eq!(
                candidate.coordinator_message_policy,
                Some(SessionActivationPolicy::InterruptSpecificWait)
            );
            assert_eq!(
                router
                    .request_activation("parent", older.generation)
                    .await
                    .unwrap(),
                SessionActivationDisposition::ActivationReserved
            );
            assert_eq!(spawner.reservations.load(Ordering::SeqCst), 2);
            // Leaving the same exact item pending must retain bounded recovery.
            let mut successor = router.register_run("parent", "run-2").await.unwrap();
            successor.begin_finalization().await;
            assert_eq!(successor.finish(newer.generation).await.unwrap(), None);
            assert_eq!(
                router
                    .request_activation("parent", older.generation)
                    .await
                    .unwrap(),
                SessionActivationDisposition::ActivationCoalesced
            );
            assert_eq!(spawner.reservations.load(Ordering::SeqCst), 2);
            inbox.ack("parent", older_claim).await.unwrap();
            if let Some(sibling) = sibling {
                let claim = claims
                    .iter()
                    .find(|claim| claim.envelope.id == sibling.id)
                    .unwrap();
                inbox.ack("parent", claim).await.unwrap();
            }
        }
    }

    #[tokio::test]
    async fn precise_older_grant_after_terminal_inspection_reaches_one_successor() {
        use bamboo_domain::{
            Session, SessionActivationPolicy, SessionInboxLimits, SessionMessageEnvelope, Storage,
        };
        use bamboo_storage::{FileSessionInbox, SessionStoreV2};
        for abandon in [false, true] {
            let temp = tempfile::TempDir::new().unwrap();
            let store = Arc::new(SessionStoreV2::new(temp.path().into()).await.unwrap());
            store
                .save_session(&Session::new("parent", "model"))
                .await
                .unwrap();
            let inbox = Arc::new(FileSessionInbox::new(store, SessionInboxLimits::default()));
            let older = inbox
                .deliver(&SessionMessageEnvelope::user_input(
                    "parent",
                    "staged child",
                ))
                .await
                .unwrap();
            let newer = inbox
                .deliver_with_activation_intent(
                    &SessionMessageEnvelope::user_input("parent", "newer user input"),
                    SessionActivationPolicy::InterruptSpecificWait,
                    None,
                )
                .await
                .unwrap();
            let router = SessionActivationRouter::new();
            router.set_inbox(inbox.clone());
            let spawner = spawner();
            router.set_spawner(spawner.clone()).await;
            router
                .request_activation("parent", newer.generation)
                .await
                .unwrap();
            let claims = inbox.claim("parent", 128).await.unwrap();
            assert_eq!(claims.len(), 1);
            inbox.ack("parent", &claims[0]).await.unwrap();

            let mut owner = router.register_run("parent", "run-1").await.unwrap();
            owner.begin_finalization().await;
            let entered = Arc::new(Notify::new());
            let release = Arc::new(Notify::new());
            *router.terminal_inspection_pause.lock().await =
                Some((entered.clone(), release.clone()));
            let finalizing = tokio::spawn(async move {
                if abandon {
                    owner.abandon().await;
                    None
                } else {
                    owner.finish(newer.generation).await.unwrap()
                }
            });
            entered.notified().await;
            assert!(!inbox.inspect("parent").await.unwrap().activation_pending());
            inbox
                .mark_message_activation_eligible(
                    "parent",
                    &older,
                    SessionActivationPolicy::InterruptSpecificWait,
                )
                .await
                .unwrap();
            assert_eq!(
                router
                    .request_activation("parent", older.generation)
                    .await
                    .unwrap(),
                SessionActivationDisposition::ActivationCoalesced
            );
            release.notify_one();
            let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), finalizing)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                outcome,
                (!abandon).then_some(SessionActivationDisposition::ActivationReserved)
            );
            assert_eq!(spawner.reservations.load(Ordering::SeqCst), 2);
            assert!(router.owns_run("parent", "run-2").await);

            // Leave the newly processed item pending to prove the raced grant
            // does not repeatedly restart a poisoned successor.
            let claims = inbox.claim("parent", 128).await.unwrap();
            assert_eq!(claims.len(), 1);
            let mut successor = router.register_run("parent", "run-2").await.unwrap();
            successor.begin_finalization().await;
            assert_eq!(successor.finish(2).await.unwrap(), None);
            assert_eq!(
                router
                    .request_activation("parent", older.generation)
                    .await
                    .unwrap(),
                SessionActivationDisposition::ActivationCoalesced
            );
            assert_eq!(spawner.reservations.load(Ordering::SeqCst), 2);
            inbox.ack("parent", &claims[0]).await.unwrap();
        }
    }

    struct RecordingSpawner {
        reservations: AtomicUsize,
        launches: Arc<AtomicUsize>,
        entered: Option<Arc<Barrier>>,
        release: Option<Arc<Notify>>,
    }

    #[async_trait]
    impl SessionActivationSpawner for RecordingSpawner {
        async fn reserve_activation(
            &self,
            _target_session_id: &str,
            _inbox_generation: u64,
        ) -> Result<SessionActivationReserveOutcome, SessionActivationError> {
            let ordinal = self.reservations.fetch_add(1, Ordering::SeqCst) + 1;
            if let Some(entered) = &self.entered {
                entered.wait().await;
            }
            if let Some(release) = &self.release {
                release.notified().await;
            }
            let launches = self.launches.clone();
            Ok(SessionActivationReserveOutcome::Reserved(
                SessionActivationLaunch::new(format!("run-{ordinal}"), move || {
                    launches.fetch_add(1, Ordering::SeqCst);
                }),
            ))
        }
    }

    fn spawner() -> Arc<RecordingSpawner> {
        Arc::new(RecordingSpawner {
            reservations: AtomicUsize::new(0),
            launches: Arc::new(AtomicUsize::new(0)),
            entered: None,
            release: None,
        })
    }

    struct BlockingInspectInbox {
        entered: Arc<Notify>,
        release: Arc<Notify>,
        drained: Arc<std::sync::atomic::AtomicBool>,
        fail_once: Option<Arc<std::sync::atomic::AtomicBool>>,
    }

    #[async_trait]
    impl SessionInboxPort for BlockingInspectInbox {
        async fn inspect_wake_readiness(
            &self,
            _target_session_id: &str,
            _now: DateTime<Utc>,
        ) -> Result<bamboo_domain::SessionInboxWakeReadiness, bamboo_domain::SessionInboxError>
        {
            // This double models only coordinator-prefix work, never a
            // separately promoted per-message delivery below that prefix.
            Ok(bamboo_domain::SessionInboxWakeReadiness::default())
        }

        async fn coordinator_activation_generation(
            &self,
            _target_session_id: &str,
        ) -> Result<u64, bamboo_domain::SessionInboxError> {
            Ok(1)
        }

        async fn deliver(
            &self,
            _envelope: &bamboo_domain::SessionMessageEnvelope,
        ) -> Result<bamboo_domain::SessionInboxReceipt, bamboo_domain::SessionInboxError> {
            unreachable!("router cleanup only inspects the durable inbox")
        }

        async fn mark_activation_eligible(
            &self,
            _target_session_id: &str,
            _generation: u64,
            _policy: bamboo_domain::SessionActivationPolicy,
        ) -> Result<(), bamboo_domain::SessionInboxError> {
            unreachable!("router cleanup only inspects the durable inbox")
        }

        async fn claim(
            &self,
            _target_session_id: &str,
            _limit: usize,
        ) -> Result<Vec<bamboo_domain::SessionInboxClaim>, bamboo_domain::SessionInboxError>
        {
            unreachable!("router cleanup only inspects the durable inbox")
        }

        async fn was_admitted(
            &self,
            _target_session_id: &str,
            _id: &bamboo_domain::SessionMessageId,
        ) -> Result<bool, bamboo_domain::SessionInboxError> {
            unreachable!("router cleanup only inspects the durable inbox")
        }

        async fn ack(
            &self,
            _target_session_id: &str,
            _claim: &bamboo_domain::SessionInboxClaim,
        ) -> Result<(), bamboo_domain::SessionInboxError> {
            unreachable!("router cleanup only inspects the durable inbox")
        }

        async fn inspect(
            &self,
            _target_session_id: &str,
        ) -> Result<bamboo_domain::SessionInboxBacklog, bamboo_domain::SessionInboxError> {
            if self
                .fail_once
                .as_ref()
                .is_some_and(|flag| flag.swap(false, Ordering::SeqCst))
            {
                return Err(bamboo_domain::SessionInboxError::Storage(
                    "injected inspect failure".into(),
                ));
            }
            if self.drained.load(Ordering::SeqCst) {
                return Ok(bamboo_domain::SessionInboxBacklog {
                    generation: 1,
                    activation_generation: 1,
                    ..Default::default()
                });
            }
            self.entered.notify_one();
            self.release.notified().await;
            Ok(bamboo_domain::SessionInboxBacklog {
                pending: 1,
                claimed: 0,
                generation: 1,
                activation_generation: 1,
                coordinator_generation: 1,
                interrupt_generation: 1,
                oldest_generation: Some(1),
            })
        }
    }

    #[tokio::test]
    async fn rollback_completion_is_retained_before_waiter_subscribes() {
        let completion = RollbackCompletion::new();
        completion.complete();
        tokio::time::timeout(std::time::Duration::from_millis(100), completion.wait())
            .await
            .expect("completion-before-wait must not lose its wake");
    }

    #[derive(Clone, Copy)]
    enum InjectedFirstOutcome {
        NoWork,
        Error,
    }

    struct RetrySpawner {
        reservations: AtomicUsize,
        launches: Arc<AtomicUsize>,
        first_outcome: InjectedFirstOutcome,
        first_entered: Arc<Barrier>,
        release_first: Arc<Notify>,
    }

    struct CancelFirstSpawner {
        reservations: AtomicUsize,
        launches: Arc<AtomicUsize>,
        first_entered: Arc<Notify>,
    }

    struct PanicFirstSpawner {
        reservations: AtomicUsize,
        launches: Arc<AtomicUsize>,
    }

    struct AlreadyRunningThenReserveSpawner {
        reservations: AtomicUsize,
        launches: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl SessionActivationSpawner for AlreadyRunningThenReserveSpawner {
        async fn reserve_activation(
            &self,
            _target_session_id: &str,
            _inbox_generation: u64,
        ) -> Result<SessionActivationReserveOutcome, SessionActivationError> {
            let ordinal = self.reservations.fetch_add(1, Ordering::SeqCst) + 1;
            if ordinal == 1 {
                return Ok(SessionActivationReserveOutcome::AlreadyRunning {
                    run_id: "adopted-run".to_string(),
                });
            }
            let launches = self.launches.clone();
            Ok(SessionActivationReserveOutcome::Reserved(
                SessionActivationLaunch::new("successor-run", move || {
                    launches.fetch_add(1, Ordering::SeqCst);
                }),
            ))
        }
    }

    #[async_trait]
    impl SessionActivationSpawner for CancelFirstSpawner {
        async fn reserve_activation(
            &self,
            _target_session_id: &str,
            _inbox_generation: u64,
        ) -> Result<SessionActivationReserveOutcome, SessionActivationError> {
            let ordinal = self.reservations.fetch_add(1, Ordering::SeqCst) + 1;
            if ordinal == 1 {
                self.first_entered.notify_one();
                std::future::pending::<()>().await;
            }
            let launches = self.launches.clone();
            Ok(SessionActivationReserveOutcome::Reserved(
                SessionActivationLaunch::new(format!("run-{ordinal}"), move || {
                    launches.fetch_add(1, Ordering::SeqCst);
                }),
            ))
        }
    }

    #[async_trait]
    impl SessionActivationSpawner for PanicFirstSpawner {
        async fn reserve_activation(
            &self,
            _target_session_id: &str,
            _inbox_generation: u64,
        ) -> Result<SessionActivationReserveOutcome, SessionActivationError> {
            let ordinal = self.reservations.fetch_add(1, Ordering::SeqCst) + 1;
            if ordinal == 1 {
                panic!("injected first activation-spawner panic");
            }
            let launches = self.launches.clone();
            Ok(SessionActivationReserveOutcome::Reserved(
                SessionActivationLaunch::new(format!("run-{ordinal}"), move || {
                    launches.fetch_add(1, Ordering::SeqCst);
                }),
            ))
        }
    }

    struct RollbackSpawner {
        entered: Arc<Barrier>,
        release: Arc<Notify>,
        launch_ready: Arc<Notify>,
        rollbacks: Arc<AtomicUsize>,
    }

    struct RealRegistryRollbackSpawner {
        runners: Arc<
            tokio::sync::RwLock<std::collections::HashMap<String, crate::execution::AgentRunner>>,
        >,
        senders: Arc<
            tokio::sync::RwLock<
                std::collections::HashMap<
                    String,
                    tokio::sync::broadcast::Sender<bamboo_agent_core::AgentEvent>,
                >,
            >,
        >,
        sender: tokio::sync::broadcast::Sender<bamboo_agent_core::AgentEvent>,
        reservations: AtomicUsize,
        first_reserved: Arc<tokio::sync::Notify>,
        allow_first_return: Arc<tokio::sync::Notify>,
        first_returning: Arc<tokio::sync::Notify>,
        rollback_started: Arc<tokio::sync::Notify>,
        allow_rollback: Arc<tokio::sync::Notify>,
        launches: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl SessionActivationSpawner for RealRegistryRollbackSpawner {
        async fn reserve_activation(
            &self,
            target_session_id: &str,
            _inbox_generation: u64,
        ) -> Result<SessionActivationReserveOutcome, SessionActivationError> {
            let ordinal = self.reservations.fetch_add(1, Ordering::SeqCst) + 1;
            match crate::execution::reserve_runner_core(
                &self.runners,
                &self.senders,
                target_session_id,
                &self.sender,
            )
            .await
            {
                crate::execution::ReserveOutcome::AlreadyRunning(run_id) => {
                    Ok(SessionActivationReserveOutcome::AlreadyRunning { run_id })
                }
                crate::execution::ReserveOutcome::Reserved(reservation) => {
                    let run_id = reservation.run_id.clone();
                    if ordinal == 1 {
                        self.first_reserved.notify_one();
                        self.allow_first_return.notified().await;
                        self.first_returning.notify_one();
                    }
                    let rollback_runners = self.runners.clone();
                    let rollback_session_id = target_session_id.to_string();
                    let rollback_run_id = run_id.clone();
                    let rollback_started = self.rollback_started.clone();
                    let allow_rollback = self.allow_rollback.clone();
                    let launches = self.launches.clone();
                    Ok(SessionActivationReserveOutcome::Reserved(
                        SessionActivationLaunch::new_with_async_rollback(
                            run_id,
                            move || {
                                launches.fetch_add(1, Ordering::SeqCst);
                            },
                            move || async move {
                                rollback_started.notify_one();
                                allow_rollback.notified().await;
                                let mut runners = rollback_runners.write().await;
                                if runners
                                    .get(&rollback_session_id)
                                    .is_some_and(|runner| runner.run_id == rollback_run_id)
                                {
                                    runners.remove(&rollback_session_id);
                                }
                            },
                        ),
                    ))
                }
            }
        }
    }

    #[async_trait]
    impl SessionActivationSpawner for RollbackSpawner {
        async fn reserve_activation(
            &self,
            _target_session_id: &str,
            _inbox_generation: u64,
        ) -> Result<SessionActivationReserveOutcome, SessionActivationError> {
            self.entered.wait().await;
            self.release.notified().await;
            let rollbacks = self.rollbacks.clone();
            let outcome = SessionActivationReserveOutcome::Reserved(
                SessionActivationLaunch::new_with_rollback(
                    "reserved-run",
                    || {},
                    move || {
                        rollbacks.fetch_add(1, Ordering::SeqCst);
                    },
                ),
            );
            self.launch_ready.notify_one();
            Ok(outcome)
        }
    }

    #[async_trait]
    impl SessionActivationSpawner for RetrySpawner {
        async fn reserve_activation(
            &self,
            _target_session_id: &str,
            _inbox_generation: u64,
        ) -> Result<SessionActivationReserveOutcome, SessionActivationError> {
            let ordinal = self.reservations.fetch_add(1, Ordering::SeqCst) + 1;
            if ordinal == 1 {
                self.first_entered.wait().await;
                self.release_first.notified().await;
                return match self.first_outcome {
                    InjectedFirstOutcome::NoWork => Ok(SessionActivationReserveOutcome::NoWork),
                    InjectedFirstOutcome::Error => Err(SessionActivationError::Internal(
                        "injected first reservation failure".to_string(),
                    )),
                };
            }
            let launches = self.launches.clone();
            Ok(SessionActivationReserveOutcome::Reserved(
                SessionActivationLaunch::new(format!("run-{ordinal}"), move || {
                    launches.fetch_add(1, Ordering::SeqCst);
                }),
            ))
        }
    }

    async fn assert_newer_generation_retries_after(
        first_outcome: InjectedFirstOutcome,
    ) -> Result<SessionActivationDisposition, SessionActivationError> {
        let router = SessionActivationRouter::new();
        let first_entered = Arc::new(Barrier::new(2));
        let release_first = Arc::new(Notify::new());
        let spawner = Arc::new(RetrySpawner {
            reservations: AtomicUsize::new(0),
            launches: Arc::new(AtomicUsize::new(0)),
            first_outcome,
            first_entered: first_entered.clone(),
            release_first: release_first.clone(),
        });
        router.set_spawner(spawner.clone()).await;

        let first_router = router.clone();
        let first =
            tokio::spawn(async move { first_router.request_activation("session", 1).await });
        first_entered.wait().await;

        let second_router = router.clone();
        let second =
            tokio::spawn(async move { second_router.request_activation("session", 2).await });
        tokio::task::yield_now().await;
        assert_eq!(
            spawner.reservations.load(Ordering::SeqCst),
            1,
            "generation 2 must coalesce while generation 1 owns the reservation"
        );

        release_first.notify_one();
        let first_result = first.await.unwrap();
        assert_eq!(
            second.await.unwrap().unwrap(),
            SessionActivationDisposition::ActivationReserved,
            "the coalesced newer delivery must take the released reservation"
        );
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 2);
        assert_eq!(spawner.launches.load(Ordering::SeqCst), 1);
        first_result
    }

    #[tokio::test]
    async fn active_owner_is_notified_without_second_reservation() {
        let router = SessionActivationRouter::new();
        let spawner = spawner();
        router.set_spawner(spawner.clone()).await;
        let mut registration = router.register_run("session", "run-live").await.unwrap();

        assert_eq!(
            router.request_activation("session", 7).await.unwrap(),
            SessionActivationDisposition::ActiveNotified
        );
        registration.notifications_mut().changed().await.unwrap();
        assert_eq!(*registration.notifications_mut().borrow(), 7);
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 0);
        registration.begin_finalization().await;
        assert_eq!(registration.finish(7).await.unwrap(), None);
    }

    #[tokio::test]
    async fn different_run_registration_never_overwrites_live_owner() {
        let router = SessionActivationRouter::new();
        let mut first = router
            .register_run("session", "run-1")
            .await
            .expect("first run owns the logical session");

        let error = router
            .register_run("session", "run-2")
            .await
            .err()
            .expect("a second exact run must collide");
        assert_eq!(
            error,
            SessionRunRegistrationError::OwnerCollision {
                target_session_id: "session".to_string(),
                existing_run_id: "run-1".to_string(),
                attempted_run_id: "run-2".to_string(),
            }
        );
        assert!(router.owns_run("session", "run-1").await);
        assert!(!router.owns_run("session", "run-2").await);

        first.begin_finalization().await;
        assert_eq!(first.finish(0).await.unwrap(), None);
    }

    #[tokio::test]
    async fn direct_registration_waits_for_activation_to_publish_its_exact_owner() {
        let router = SessionActivationRouter::new();
        let reservation_entered = Arc::new(Barrier::new(2));
        let allow_reservation = Arc::new(Notify::new());
        let launches = Arc::new(AtomicUsize::new(0));
        let spawner = Arc::new(RecordingSpawner {
            reservations: AtomicUsize::new(0),
            launches: launches.clone(),
            entered: Some(reservation_entered.clone()),
            release: Some(allow_reservation.clone()),
        });
        router.set_spawner(spawner.clone()).await;

        let activation_router = router.clone();
        let activation =
            tokio::spawn(async move { activation_router.request_activation("session", 1).await });
        reservation_entered.wait().await;

        let direct_router = router.clone();
        let direct =
            tokio::spawn(async move { direct_router.register_run("session", "manual-run").await });
        tokio::task::yield_now().await;
        assert!(
            !direct.is_finished(),
            "manual registration must not supersede an in-flight activation token"
        );
        assert!(!router.owns_run("session", "manual-run").await);

        allow_reservation.notify_one();
        assert_eq!(
            activation.await.unwrap().unwrap(),
            SessionActivationDisposition::ActivationReserved
        );
        let collision = match direct.await.unwrap() {
            Ok(_) => panic!("manual registration must collide with the published activation"),
            Err(error) => error,
        };
        assert_eq!(
            collision,
            SessionRunRegistrationError::OwnerCollision {
                target_session_id: "session".to_string(),
                existing_run_id: "run-1".to_string(),
                attempted_run_id: "manual-run".to_string(),
            }
        );
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 1);
        assert_eq!(launches.load(Ordering::SeqCst), 1);
        assert!(router.owns_run("session", "run-1").await);
        assert!(!router.owns_run("session", "manual-run").await);

        let mut registration = router.register_run("session", "run-1").await.unwrap();
        registration.begin_finalization().await;
        assert_eq!(registration.finish(1).await.unwrap(), None);
    }

    #[tokio::test]
    async fn cancelled_explicit_abandon_still_finishes_owned_registration_cleanup() {
        let router = SessionActivationRouter::new();
        let cleanup_entered = Arc::new(Notify::new());
        let cleanup_release = Arc::new(Notify::new());
        let cleanup_completed = Arc::new(AtomicUsize::new(0));
        let mut registration = router.register_run("session", "run-abandon").await.unwrap();
        let entered = cleanup_entered.clone();
        let release = cleanup_release.clone();
        let completed = cleanup_completed.clone();
        registration.set_abort_cleanup(move || async move {
            entered.notify_one();
            release.notified().await;
            completed.fetch_add(1, Ordering::SeqCst);
        });

        let abandon = tokio::spawn(registration.abandon());
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            cleanup_entered.notified(),
        )
        .await
        .expect("explicit abandon must start its owned cleanup");
        abandon.abort();
        assert!(abandon.await.unwrap_err().is_cancelled());
        cleanup_release.notify_one();

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if cleanup_completed.load(Ordering::SeqCst) == 1
                    && !router.owns_run("session", "run-abandon").await
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("caller cancellation must not cancel detached registration cleanup");
    }

    #[tokio::test]
    async fn delayed_same_run_cannot_adopt_an_owner_during_abandoned_cleanup() {
        let router = SessionActivationRouter::new();
        let spawner = spawner();
        router.set_spawner(spawner.clone()).await;
        let inspect_entered = Arc::new(Notify::new());
        let inspect_release = Arc::new(Notify::new());
        let drained = Arc::new(std::sync::atomic::AtomicBool::new(false));
        router.set_inbox(Arc::new(BlockingInspectInbox {
            entered: inspect_entered.clone(),
            release: inspect_release.clone(),
            drained: drained.clone(),
            fail_once: None,
        }));

        let registration = router
            .register_run("session", "run-abandoned")
            .await
            .unwrap();
        drop(registration);
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            inspect_entered.notified(),
        )
        .await
        .expect("abandoned cleanup must reach durable inbox reconciliation");

        let error = router
            .register_run("session", "run-abandoned")
            .await
            .err()
            .expect("a finalizing abandoned owner is not an adoptable launch placeholder");
        assert_eq!(error.existing_run_id(), "run-abandoned");

        inspect_release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while spawner.launches.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("durable pending work must launch one successor");
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 1);
        assert_eq!(spawner.launches.load(Ordering::SeqCst), 1);
        assert!(!router.owns_run("session", "run-abandoned").await);

        let mut successor = router.register_run("session", "run-1").await.unwrap();
        drained.store(true, Ordering::SeqCst);
        successor.begin_finalization().await;
        assert_eq!(successor.finish(1).await.unwrap(), None);
    }

    #[tokio::test]
    async fn delayed_registry_owner_collision_rolls_back_only_its_exact_runner() {
        let router = SessionActivationRouter::new();
        let runners = Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new()));
        let senders = Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new()));
        let (sender, _receiver) = tokio::sync::broadcast::channel(8);
        let reserved =
            match crate::execution::reserve_runner_core(&runners, &senders, "session", &sender)
                .await
            {
                crate::execution::ReserveOutcome::Reserved(reservation) => reservation,
                crate::execution::ReserveOutcome::AlreadyRunning(_) => {
                    panic!("fixture must reserve a fresh server runner")
                }
            };

        // Model the gap between the server runner reservation and its delayed
        // router registration: an independent direct SDK entry point wins the
        // logical owner first.
        let mut direct = router.register_run("session", "sdk-direct").await.unwrap();
        let collision = router
            .register_run("session", &reserved.run_id)
            .await
            .err()
            .expect("delayed server registration must collide");
        let collision_result = Err(bamboo_agent_core::AgentError::LLM(collision.to_string()));
        assert!(
            crate::execution::finalize_runner_exact(
                &runners,
                "session",
                &reserved.run_id,
                &collision_result,
            )
            .await
        );
        assert!(router.owns_run("session", "sdk-direct").await);
        assert!(matches!(
            runners
                .read()
                .await
                .get("session")
                .map(|runner| &runner.status),
            Some(crate::execution::AgentStatus::Error(_))
        ));

        direct.begin_finalization().await;
        assert_eq!(direct.finish(0).await.unwrap(), None);
    }

    #[tokio::test]
    async fn concurrent_idle_deliveries_coalesce_into_one_launch() {
        let router = SessionActivationRouter::new();
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Notify::new());
        let spawner = Arc::new(RecordingSpawner {
            reservations: AtomicUsize::new(0),
            launches: Arc::new(AtomicUsize::new(0)),
            entered: Some(entered.clone()),
            release: Some(release.clone()),
        });
        router.set_spawner(spawner.clone()).await;

        let first_router = router.clone();
        let first =
            tokio::spawn(async move { first_router.request_activation("session", 1).await });
        entered.wait().await;
        let second_router = router.clone();
        let second =
            tokio::spawn(async move { second_router.request_activation("session", 2).await });
        release.notify_one();
        assert_eq!(
            first.await.unwrap().unwrap(),
            SessionActivationDisposition::ActivationReserved
        );
        assert_eq!(
            second.await.unwrap().unwrap(),
            SessionActivationDisposition::ActiveNotified
        );
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 1);
        assert_eq!(spawner.launches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn newer_generation_re_reserves_after_coalesced_no_work() {
        assert_eq!(
            assert_newer_generation_retries_after(InjectedFirstOutcome::NoWork)
                .await
                .unwrap(),
            SessionActivationDisposition::ActivationCoalesced
        );
    }

    #[tokio::test]
    async fn newer_generation_re_reserves_after_coalesced_error() {
        let error = assert_newer_generation_retries_after(InjectedFirstOutcome::Error)
            .await
            .unwrap_err();
        assert!(matches!(error, SessionActivationError::Internal(_)));
    }

    async fn assert_cancelled_finalization_recovers_without_redelivery(registered_owner: bool) {
        let router = SessionActivationRouter::new();
        let first_entered = Arc::new(Notify::new());
        let spawner = Arc::new(CancelFirstSpawner {
            reservations: AtomicUsize::new(0),
            launches: Arc::new(AtomicUsize::new(0)),
            first_entered: first_entered.clone(),
        });
        router.set_spawner(spawner.clone()).await;

        let finish = if registered_owner {
            let mut registration = router.register_run("session", "run-old").await.unwrap();
            registration.begin_finalization().await;
            assert_eq!(
                router.request_activation("session", 1).await.unwrap(),
                SessionActivationDisposition::ActivationCoalesced
            );
            tokio::spawn(async move { registration.finish(0).await })
        } else {
            // Reserved actor-child runs use the raw router handshake rather
            // than SessionRunRegistration, so recovery cannot depend on that
            // guard's Drop implementation.
            router.begin_finalization("session", "run-old").await;
            assert_eq!(
                router.request_activation("session", 1).await.unwrap(),
                SessionActivationDisposition::ActivationCoalesced
            );
            let finish_router = router.clone();
            tokio::spawn(async move {
                finish_router
                    .finish_finalization("session", "run-old", 0)
                    .await
            })
        };
        first_entered.notified().await;
        finish.abort();
        assert!(finish.await.unwrap_err().is_cancelled());

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while spawner.launches.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled finalization must retry pending work without redelivery");
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 2);
        assert_eq!(spawner.launches.load(Ordering::SeqCst), 1);
        assert!(router.owns_run("session", "run-2").await);

        let mut successor = router.register_run("session", "run-2").await.unwrap();
        successor.begin_finalization().await;
        assert_eq!(successor.finish(1).await.unwrap(), None);
    }

    #[tokio::test]
    async fn cancelled_registered_finalization_retries_without_another_delivery() {
        assert_cancelled_finalization_recovers_without_redelivery(true).await;
    }

    #[tokio::test]
    async fn cancelled_reserved_child_finalization_retries_without_another_delivery() {
        assert_cancelled_finalization_recovers_without_redelivery(false).await;
    }

    #[tokio::test]
    async fn panicking_finalization_spawner_gets_one_bounded_retry() {
        let router = SessionActivationRouter::new();
        let spawner = Arc::new(PanicFirstSpawner {
            reservations: AtomicUsize::new(0),
            launches: Arc::new(AtomicUsize::new(0)),
        });
        router.set_spawner(spawner.clone()).await;
        router.begin_finalization("session", "run-old").await;
        assert_eq!(
            router.request_activation("session", 1).await.unwrap(),
            SessionActivationDisposition::ActivationCoalesced
        );

        let finish_router = router.clone();
        let finish = tokio::spawn(async move {
            finish_router
                .finish_finalization("session", "run-old", 0)
                .await
        });
        assert!(finish.await.unwrap_err().is_panic());

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while spawner.launches.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("router Drop recovery must survive one spawner panic");
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 2);
        assert_eq!(spawner.launches.load(Ordering::SeqCst), 1);

        let mut successor = router.register_run("session", "run-2").await.unwrap();
        successor.begin_finalization().await;
        assert_eq!(successor.finish(1).await.unwrap(), None);
    }

    #[tokio::test]
    async fn cancelled_reservation_releases_token_and_newer_generation_launches() {
        let router = SessionActivationRouter::new();
        let first_entered = Arc::new(Notify::new());
        let spawner = Arc::new(CancelFirstSpawner {
            reservations: AtomicUsize::new(0),
            launches: Arc::new(AtomicUsize::new(0)),
            first_entered: first_entered.clone(),
        });
        router.set_spawner(spawner.clone()).await;

        let first_router = router.clone();
        let first =
            tokio::spawn(async move { first_router.request_activation("session", 1).await });
        first_entered.notified().await;
        let second_router = router.clone();
        let second =
            tokio::spawn(async move { second_router.request_activation("session", 2).await });
        tokio::task::yield_now().await;
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());

        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(1), second)
                .await
                .expect("coalesced caller must not hang behind a cancelled reservation")
                .unwrap()
                .unwrap(),
            SessionActivationDisposition::ActivationReserved
        );
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 2);
        assert_eq!(spawner.launches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cancellation_after_external_reservation_rolls_back_unlaunched_slot() {
        let router = SessionActivationRouter::new();
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Notify::new());
        let launch_ready = Arc::new(Notify::new());
        let rollbacks = Arc::new(AtomicUsize::new(0));
        let spawner = Arc::new(RollbackSpawner {
            entered: entered.clone(),
            release: release.clone(),
            launch_ready: launch_ready.clone(),
            rollbacks: rollbacks.clone(),
        });
        router.set_spawner(spawner).await;

        let task_router = router.clone();
        let task = tokio::spawn(async move { task_router.request_activation("session", 1).await });
        entered.wait().await;
        // The spawner can now return its reserved slot, but publishing the
        // logical owner is blocked. Aborting in this exact window must drop the
        // launch and invoke its external rollback.
        let states = router.states.lock().await;
        release.notify_one();
        launch_ready.notified().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        drop(states);

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while rollbacks.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("unlaunched reservation rollback");
        assert_eq!(rollbacks.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn redelivery_waits_for_real_runner_rollback_before_reserving() {
        let router = SessionActivationRouter::new();
        let runners = Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new()));
        let senders = Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new()));
        let (sender, _receiver) = tokio::sync::broadcast::channel(8);
        let first_reserved = Arc::new(tokio::sync::Notify::new());
        let allow_first_return = Arc::new(tokio::sync::Notify::new());
        let rollback_started = Arc::new(tokio::sync::Notify::new());
        let first_returning = Arc::new(tokio::sync::Notify::new());
        let allow_rollback = Arc::new(tokio::sync::Notify::new());
        let launches = Arc::new(AtomicUsize::new(0));
        let spawner = Arc::new(RealRegistryRollbackSpawner {
            runners: runners.clone(),
            senders,
            sender,
            reservations: AtomicUsize::new(0),
            first_reserved: first_reserved.clone(),
            allow_first_return: allow_first_return.clone(),
            first_returning: first_returning.clone(),
            rollback_started: rollback_started.clone(),
            allow_rollback: allow_rollback.clone(),
            launches: launches.clone(),
        });
        router.set_spawner(spawner.clone()).await;

        let first_router = router.clone();
        let first =
            tokio::spawn(async move { first_router.request_activation("session", 1).await });
        first_reserved.notified().await;

        // Force the first request to yield after receiving the rollback-capable
        // real registry reservation but before publishing router ownership.
        let state_guard = router.states.lock().await;
        allow_first_return.notify_one();
        first_returning.notified().await;
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        drop(state_guard);
        rollback_started.notified().await;

        let second_router = router.clone();
        let second =
            tokio::spawn(async move { second_router.request_activation("session", 2).await });
        tokio::task::yield_now().await;
        assert_eq!(
            spawner.reservations.load(Ordering::SeqCst),
            1,
            "redelivery must remain coalesced until exact external rollback completes"
        );

        allow_rollback.notify_one();
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(1), second)
                .await
                .expect("redelivery must reserve after rollback")
                .unwrap()
                .unwrap(),
            SessionActivationDisposition::ActivationReserved
        );
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 2);
        assert_eq!(launches.load(Ordering::SeqCst), 1);
        let current = runners
            .read()
            .await
            .get("session")
            .map(|runner| runner.run_id.clone())
            .expect("second real registry reservation remains");
        assert_ne!(
            current, "",
            "the redelivery must never adopt an empty/stale run id"
        );
    }

    #[tokio::test]
    async fn delivery_during_finalization_launches_one_successor() {
        let router = SessionActivationRouter::new();
        let spawner = spawner();
        router.set_spawner(spawner.clone()).await;
        let mut registration = router.register_run("session", "run-old").await.unwrap();
        registration.begin_finalization().await;

        assert_eq!(
            router.request_activation("session", 11).await.unwrap(),
            SessionActivationDisposition::ActivationCoalesced
        );
        let disposition = registration.finish(10).await.unwrap();
        assert_eq!(
            disposition,
            Some(SessionActivationDisposition::ActivationReserved)
        );
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 1);
        assert_eq!(spawner.launches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn finalization_does_not_restart_when_cursor_caught_up() {
        let router = SessionActivationRouter::new();
        let spawner = spawner();
        router.set_spawner(spawner.clone()).await;
        let mut registration = router.register_run("session", "run-old").await.unwrap();
        assert_eq!(
            router.request_activation("session", 4).await.unwrap(),
            SessionActivationDisposition::ActiveNotified
        );
        registration.begin_finalization().await;
        assert_eq!(registration.finish(4).await.unwrap(), None);
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 0);
        assert!(
            router.states.lock().await.get("session").is_none(),
            "a caught-up completed session must not leak routing state"
        );
    }

    #[tokio::test]
    async fn successor_finalization_removes_state_after_raced_generation_catches_up() {
        let router = SessionActivationRouter::new();
        let spawner = spawner();
        router.set_spawner(spawner).await;
        let mut registration = router.register_run("session", "run-old").await.unwrap();
        registration.begin_finalization().await;

        assert_eq!(
            router.request_activation("session", 11).await.unwrap(),
            SessionActivationDisposition::ActivationCoalesced
        );
        assert_eq!(
            registration.finish(10).await.unwrap(),
            Some(SessionActivationDisposition::ActivationReserved)
        );
        let mut successor_registration = router.register_run("session", "run-1").await.unwrap();
        successor_registration.begin_finalization().await;
        assert_eq!(successor_registration.finish(11).await.unwrap(), None);
        assert!(
            router.states.lock().await.get("session").is_none(),
            "the successor's caught-up terminal state must be compacted"
        );
    }

    #[tokio::test]
    async fn failed_terminal_inspection_keeps_the_exact_owner_cleanup_armed() {
        let router = SessionActivationRouter::new();
        let spawner = spawner();
        router.set_spawner(spawner.clone()).await;
        let inspect_release = Arc::new(Notify::new());
        inspect_release.notify_one();
        router.set_inbox(Arc::new(BlockingInspectInbox {
            entered: Arc::new(Notify::new()),
            release: inspect_release,
            drained: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            fail_once: Some(Arc::new(std::sync::atomic::AtomicBool::new(true))),
        }));
        let mut registration = router
            .register_run("session", "failed-inspect-run")
            .await
            .unwrap();
        router.request_activation("session", 1).await.unwrap();
        registration.begin_finalization().await;
        assert!(registration.finish(1).await.is_err());
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while spawner.launches.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("failed inspection must not leave the old run owning the inbox");
        assert!(!router.owns_run("session", "failed-inspect-run").await);
        assert_eq!(spawner.launches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn poison_generation_launches_only_one_successor_until_new_work_arrives() {
        let router = SessionActivationRouter::new();
        let spawner = spawner();
        router.set_spawner(spawner.clone()).await;
        let inspect_release = Arc::new(Notify::new());
        router.set_inbox(Arc::new(BlockingInspectInbox {
            entered: Arc::new(Notify::new()),
            release: inspect_release.clone(),
            drained: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            fail_once: None,
        }));
        let mut registration = router
            .register_run("session", "run-original")
            .await
            .unwrap();
        assert_eq!(
            router.request_activation("session", 7).await.unwrap(),
            SessionActivationDisposition::ActiveNotified
        );
        registration.begin_finalization().await;
        inspect_release.notify_one();
        assert_eq!(
            registration.finish(0).await.unwrap(),
            Some(SessionActivationDisposition::ActivationReserved)
        );
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 1);

        // Simulate the successor hitting the same permanently failing
        // checkpoint/poison claim. The same generation remains inspectable but
        // cannot recursively launch provider loops.
        let mut successor_registration = router.register_run("session", "run-1").await.unwrap();
        successor_registration.begin_finalization().await;
        inspect_release.notify_one();
        assert_eq!(successor_registration.finish(0).await.unwrap(), None);
        assert_eq!(
            router.request_activation("session", 7).await.unwrap(),
            SessionActivationDisposition::ActivationCoalesced
        );
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 1);

        // A genuinely newer delivery gets one new bounded attempt.
        assert_eq!(
            router.request_activation("session", 8).await.unwrap(),
            SessionActivationDisposition::ActivationReserved
        );
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn adopted_terminal_run_preserves_generation_for_one_real_successor() {
        let router = SessionActivationRouter::new();
        let spawner = Arc::new(AlreadyRunningThenReserveSpawner {
            reservations: AtomicUsize::new(0),
            launches: Arc::new(AtomicUsize::new(0)),
        });
        router.set_spawner(spawner.clone()).await;

        assert_eq!(
            router.request_activation("session", 12).await.unwrap(),
            SessionActivationDisposition::ActiveNotified
        );
        router.begin_finalization("session", "adopted-run").await;
        assert_eq!(
            router
                .finish_finalization("session", "adopted-run", 0)
                .await
                .unwrap(),
            Some(SessionActivationDisposition::ActivationReserved)
        );
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 2);
        assert_eq!(spawner.launches.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn quarantined_registration_never_releases_owner_or_launches_successor() {
        let router = SessionActivationRouter::new();
        let spawner = spawner();
        router.set_spawner(spawner.clone()).await;
        let mut registration = router
            .register_run("session", "quarantined-run")
            .await
            .unwrap();
        let cleaned = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cleanup_flag = cleaned.clone();
        registration.set_abort_cleanup(move || async move {
            cleanup_flag.store(true, Ordering::SeqCst);
        });
        registration.quarantine().await;
        assert_eq!(
            router.request_activation("session", 19).await.unwrap(),
            SessionActivationDisposition::ActivationCoalesced
        );
        assert!(router.register_run("session", "successor").await.is_err());
        assert!(router
            .register_run("session", "quarantined-run")
            .await
            .is_err());
        tokio::task::yield_now().await;
        assert!(!cleaned.load(Ordering::SeqCst));
        assert_eq!(spawner.launches.load(Ordering::SeqCst), 0);
        let states = router.states.lock().await;
        let owner = states["session"].owner.as_ref().unwrap();
        assert_eq!(owner.run_id, "quarantined-run");
        assert!(owner.finalizing);
    }

    #[tokio::test]
    async fn physical_execution_registration_drop_is_quarantined() {
        let router = SessionActivationRouter::new();
        let spawner = spawner();
        router.set_spawner(spawner.clone()).await;
        let mut registration = router
            .register_run("session", "physical-run")
            .await
            .unwrap();
        let cleaned = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cleanup_flag = cleaned.clone();
        registration.set_abort_cleanup(move || async move {
            cleanup_flag.store(true, Ordering::SeqCst);
        });
        registration.quarantine_on_drop();
        drop(registration);
        // Drop retains the owner synchronously, then an owned cleanup task
        // marks it finalizing. A racing delivery can still notify that retained
        // owner before the task is polled; it must never spawn a successor.
        assert!(matches!(
            router.request_activation("session", 7).await.unwrap(),
            SessionActivationDisposition::ActiveNotified
                | SessionActivationDisposition::ActivationCoalesced
        ));
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if router.states.lock().await["session"]
                    .owner
                    .as_ref()
                    .unwrap()
                    .finalizing
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            router.request_activation("session", 8).await.unwrap(),
            SessionActivationDisposition::ActivationCoalesced
        );
        assert!(!cleaned.load(Ordering::SeqCst));
        assert_eq!(spawner.launches.load(Ordering::SeqCst), 0);
        assert!(router.register_run("session", "successor").await.is_err());
        let states = router.states.lock().await;
        assert!(states["session"].owner.as_ref().unwrap().finalizing);
    }

    #[tokio::test]
    async fn cancelling_quarantine_wait_does_not_arm_abandoned_cleanup() {
        let router = SessionActivationRouter::new();
        let registration = router
            .register_run("session", "quarantined-run")
            .await
            .unwrap();
        let states_guard = router.states.lock().await;
        let mut quarantine = Box::pin(registration.quarantine());
        assert!(futures::poll!(quarantine.as_mut()).is_pending());
        drop(quarantine);
        drop(states_guard);
        tokio::task::yield_now().await;
        assert!(router.register_run("session", "successor").await.is_err());
        assert_eq!(
            router.current_run_id("session").await.as_deref(),
            Some("quarantined-run")
        );
    }
    async fn selected_fixture() -> (
        tempfile::TempDir,
        Arc<SessionActivationRouter>,
        Arc<RecordingSpawner>,
        Arc<bamboo_storage::FileSessionInbox>,
    ) {
        use bamboo_domain::{
            Session, SessionActivationPolicy, SessionInboxLimits, SessionMessageEnvelope, Storage,
        };
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(
            bamboo_storage::SessionStoreV2::new(temp.path().into())
                .await
                .unwrap(),
        );
        store
            .save_session(&Session::new("selected", "model"))
            .await
            .unwrap();
        let inbox = Arc::new(bamboo_storage::FileSessionInbox::new(
            store,
            SessionInboxLimits::default(),
        ));
        inbox
            .deliver_with_activation_intent(
                &SessionMessageEnvelope::user_input("selected", "other eligible input"),
                SessionActivationPolicy::RespectSpecificWait,
                None,
            )
            .await
            .unwrap();
        let router = SessionActivationRouter::new();
        router.set_inbox(inbox.clone());
        let spawner = spawner();
        router.set_spawner(spawner.clone()).await;
        (temp, router, spawner, inbox)
    }

    #[tokio::test]
    async fn receipt_run_selected_release_never_inherits_unrequested_durable_backlog() {
        for mode in ["finish", "abandon", "drop"] {
            let (_temp, router, spawner, inbox) = selected_fixture().await;
            let mut selected = router
                .register_run("selected", "manual-selected")
                .await
                .unwrap();
            selected.restrict_to_selected_input();
            match mode {
                "finish" => {
                    selected.begin_finalization().await;
                    assert_eq!(selected.finish(0).await.unwrap(), None);
                }
                "abandon" => selected.abandon().await,
                _ => drop(selected),
            }
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                while router.current_run_id("selected").await.is_some() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(spawner.reservations.load(Ordering::SeqCst), 0, "{mode}");
            assert_eq!(spawner.launches.load(Ordering::SeqCst), 0);
            assert_eq!(inbox.inspect("selected").await.unwrap().pending, 1);
            // A subsequent real producer request still owns normal scheduling.
            assert_eq!(
                router.request_activation("selected", 1).await.unwrap(),
                SessionActivationDisposition::ActivationReserved
            );
            assert_eq!(spawner.reservations.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn receipt_run_selected_release_preserves_explicit_coalesced_coordinator_wake() {
        for mode in ["finish", "abandon", "drop"] {
            for notify_before_scope in [false, true] {
                let (_temp, router, spawner, _inbox) = selected_fixture().await;
                let mut selected = router
                    .register_run("selected", "manual-selected")
                    .await
                    .unwrap();
                if !notify_before_scope {
                    selected.restrict_to_selected_input();
                }
                assert_eq!(
                    router.request_activation("selected", 1).await.unwrap(),
                    SessionActivationDisposition::ActiveNotified
                );
                if notify_before_scope {
                    selected.restrict_to_selected_input();
                }
                match mode {
                    "finish" => {
                        selected.begin_finalization().await;
                        assert_eq!(
                            selected.finish(0).await.unwrap(),
                            Some(SessionActivationDisposition::ActivationReserved)
                        );
                    }
                    "abandon" => selected.abandon().await,
                    _ => drop(selected),
                }
                tokio::time::timeout(std::time::Duration::from_secs(2), async {
                    while spawner.reservations.load(Ordering::SeqCst) == 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                assert_eq!(
                    spawner.reservations.load(Ordering::SeqCst),
                    1,
                    "{mode}/{notify_before_scope}"
                );
                assert_eq!(spawner.launches.load(Ordering::SeqCst), 1);
                assert!(router.owns_run("selected", "run-1").await);
            }
        }
    }

    #[tokio::test]
    async fn receipt_run_cancelled_selected_finish_retains_restricted_drop_cleanup() {
        let (_temp, router, spawner, inbox) = selected_fixture().await;
        let mut selected = router
            .register_run("selected", "manual-selected")
            .await
            .unwrap();
        selected.restrict_to_selected_input();
        let lock = router.states.lock().await;
        let mut finish = Box::pin(selected.finish(0));
        assert!(futures::poll!(finish.as_mut()).is_pending());
        drop(finish);
        drop(lock);
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while router.current_run_id("selected").await.is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 0);
        assert_eq!(inbox.inspect("selected").await.unwrap().pending, 1);
    }
    struct PeerIntentFixture {
        _temp: tempfile::TempDir,
        store: Arc<bamboo_storage::SessionStoreV2>,
        inbox: Arc<bamboo_storage::FileSessionInbox>,
        router: Arc<SessionActivationRouter>,
        spawner: Arc<RecordingSpawner>,
        messenger: crate::SessionMessenger,
    }

    impl PeerIntentFixture {
        async fn new() -> Self {
            use bamboo_domain::{Session, SessionInboxLimits, Storage};
            let temp = tempfile::tempdir().unwrap();
            let store = Arc::new(
                bamboo_storage::SessionStoreV2::new(temp.path().into())
                    .await
                    .unwrap(),
            );
            let target = Session::new("target", "model");
            let source = Session::new_child_of("sender", &target, "model", "task");
            store.save_session(&target).await.unwrap();
            store.save_session(&source).await.unwrap();
            let inbox = Arc::new(bamboo_storage::FileSessionInbox::new(
                store.clone(),
                SessionInboxLimits::default(),
            ));
            let router = SessionActivationRouter::new();
            router.set_inbox(inbox.clone());
            let spawner = spawner();
            router.set_spawner(spawner.clone()).await;
            let messenger =
                crate::SessionMessenger::new(store.clone(), inbox.clone(), router.clone());
            Self {
                _temp: temp,
                store,
                inbox,
                router,
                spawner,
                messenger,
            }
        }
    }

    #[tokio::test]
    async fn queue_only_delivery_never_schedules_idle_finalizing_or_restarted_target() {
        use bamboo_domain::SessionMessageDeliveryIntent::QueueOnly;
        for timing in [
            "idle",
            "active",
            "finalizing",
            "after_terminal_inspection",
            "selected_finish",
            "selected_abandon",
        ] {
            let fixture = PeerIntentFixture::new().await;
            let mut registration = if timing == "idle" {
                None
            } else {
                Some(
                    fixture
                        .router
                        .register_run("target", "manual-run")
                        .await
                        .unwrap(),
                )
            };
            if timing == "selected_finish" || timing == "selected_abandon" {
                registration.as_mut().unwrap().restrict_to_selected_input();
            }
            if timing == "finalizing" || timing == "after_terminal_inspection" {
                registration.as_mut().unwrap().begin_finalization().await;
            }
            let mut race = None;
            let release = Arc::new(Notify::new());
            if timing == "after_terminal_inspection" {
                let entered = Arc::new(Notify::new());
                *fixture.router.terminal_inspection_pause.lock().await =
                    Some((entered.clone(), release.clone()));
                let registration = registration.take().unwrap();
                race = Some(tokio::spawn(async move { registration.finish(0).await }));
                tokio::time::timeout(std::time::Duration::from_secs(2), entered.notified())
                    .await
                    .unwrap();
            }
            let receipt = fixture
                .messenger
                .send_peer_with_intent("sender", "target", "queue", "queued", QueueOnly)
                .await
                .unwrap();
            assert_eq!(receipt.activation, None, "{timing}");
            assert_eq!(
                receipt.active_delivery_notified,
                matches!(timing, "active" | "selected_finish" | "selected_abandon")
            );
            if let Some(mut registration) = registration {
                if timing == "selected_abandon" {
                    registration.abandon().await;
                } else {
                    registration.begin_finalization().await;
                    assert_eq!(registration.finish(0).await.unwrap(), None);
                }
            }
            if let Some(race) = race {
                release.notify_one();
                assert_eq!(race.await.unwrap().unwrap(), None);
            }
            assert_eq!(
                fixture.spawner.reservations.load(Ordering::SeqCst),
                0,
                "{timing}"
            );
            assert_eq!(fixture.spawner.launches.load(Ordering::SeqCst), 0);
            assert_eq!(fixture.inbox.inspect("target").await.unwrap().pending, 1);
            // Startup recovery uses the durable Inbox, not the live router hint.
            let restarted = SessionActivationRouter::new();
            restarted.set_inbox(fixture.inbox.clone());
            restarted.set_spawner(fixture.spawner.clone()).await;
            assert_eq!(
                restarted
                    .reconcile_wake("target", Utc::now())
                    .await
                    .unwrap()
                    .disposition,
                None
            );
            assert_eq!(fixture.spawner.reservations.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn queue_only_notifies_late_bound_active_driver_and_checkpoints_at_safe_point() {
        use bamboo_domain::{SessionMessageDeliveryIntent::QueueOnly, Storage};
        let fixture = PeerIntentFixture::new().await;
        let mut registration = fixture
            .router
            .register_run("target", "manual-run")
            .await
            .unwrap();
        let receipt = fixture
            .messenger
            .send_peer_with_intent("sender", "target", "queue", "queued", QueueOnly)
            .await
            .unwrap();
        assert!(receipt.active_delivery_notified);
        assert!(registration.notifications_mut().has_changed().unwrap());
        let (tx, mut rx) = mpsc::unbounded_channel();
        assert_eq!(
            fixture
                .router
                .attach_delivery_sink("target", tx)
                .await
                .as_deref(),
            Some("manual-run")
        );
        assert_eq!(rx.try_recv().unwrap(), receipt.delivery.generation);
        let claims = fixture
            .inbox
            .claim_for_turn("target", 8, Some("manual-run"))
            .await
            .unwrap();
        assert_eq!(claims.len(), 1);
        let mut saved = fixture.store.load_session("target").await.unwrap().unwrap();
        saved.add_message(claims[0].envelope.to_provider_message().unwrap());
        fixture.store.save_session(&saved).await.unwrap();
        fixture.inbox.ack("target", &claims[0]).await.unwrap();
        registration.begin_finalization().await;
        assert_eq!(
            registration
                .finish(receipt.delivery.generation)
                .await
                .unwrap(),
            None
        );
        assert_eq!(fixture.spawner.reservations.load(Ordering::SeqCst), 0);
        let saved = fixture.store.load_session("target").await.unwrap().unwrap();
        assert_eq!(
            saved
                .messages
                .iter()
                .filter(|message| message.id == receipt.delivery.id.as_str())
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn explicit_trigger_turn_can_start_a_turn_after_queue_only_delivery() {
        use bamboo_domain::SessionMessageDeliveryIntent::{QueueOnly, TriggerTurn};
        let fixture = PeerIntentFixture::new().await;
        fixture
            .messenger
            .send_peer_with_intent("sender", "target", "queue", "queued", QueueOnly)
            .await
            .unwrap();
        assert_eq!(fixture.spawner.reservations.load(Ordering::SeqCst), 0);
        let followup = fixture
            .messenger
            .send_peer_with_intent("sender", "target", "followup", "followup", TriggerTurn)
            .await
            .unwrap();
        assert_eq!(
            followup.activation,
            Some(SessionActivationDisposition::ActivationReserved)
        );
        assert_eq!(fixture.spawner.reservations.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.spawner.launches.load(Ordering::SeqCst), 1);
        let claims = fixture
            .inbox
            .claim_for_turn("target", 8, Some("run-1"))
            .await
            .unwrap();
        assert_eq!(claims.len(), 2);
        assert!(claims.iter().all(|claim| claim.activation_policy
            == bamboo_domain::SessionActivationPolicy::RespectSpecificWait));
    }

    #[tokio::test]
    async fn acknowledged_explicit_peer_retry_cannot_activate_or_notify_a_new_owner() {
        use bamboo_domain::{SessionMessageDeliveryIntent, Storage};
        for intent in [
            SessionMessageDeliveryIntent::TriggerTurn,
            SessionMessageDeliveryIntent::QueueOnly,
        ] {
            for active_owner in [false, true] {
                let fixture = PeerIntentFixture::new().await;
                let first = fixture
                    .messenger
                    .send_peer_with_intent("sender", "target", "ack-operation", "input", intent)
                    .await
                    .unwrap();
                let claim = fixture
                    .inbox
                    .claim_for_turn("target", 1, Some("first-run"))
                    .await
                    .unwrap()
                    .remove(0);
                let mut saved = fixture.store.load_session("target").await.unwrap().unwrap();
                saved.add_message(claim.envelope.to_provider_message().unwrap());
                saved
                    .session_inbox_admission_mut()
                    .record(claim.envelope.id.clone(), claim.generation);
                fixture.store.save_session(&saved).await.unwrap();
                fixture.inbox.ack("target", &claim).await.unwrap();

                // This spawner has no independent Inbox filter. The messenger
                // must not ask a fresh router to launch an acknowledged input.
                let restarted = SessionActivationRouter::new();
                restarted.set_inbox(fixture.inbox.clone());
                let fresh_spawner = spawner();
                restarted.set_spawner(fresh_spawner.clone()).await;
                let mut registration = if active_owner {
                    Some(restarted.register_run("target", "new-owner").await.unwrap())
                } else {
                    None
                };
                let messenger = crate::SessionMessenger::new(
                    fixture.store.clone(),
                    fixture.inbox.clone(),
                    restarted,
                );
                let retry = messenger
                    .send_peer_with_intent("sender", "target", "ack-operation", "input", intent)
                    .await
                    .unwrap();
                assert_eq!(retry.delivery, first.delivery);
                assert_eq!(retry.activation, None, "{intent:?} active={active_owner}");
                assert!(!retry.active_delivery_notified);
                assert_eq!(fresh_spawner.reservations.load(Ordering::SeqCst), 0);
                assert_eq!(fresh_spawner.launches.load(Ordering::SeqCst), 0);
                if let Some(mut registration) = registration.take() {
                    assert!(!registration.notifications_mut().has_changed().unwrap());
                    registration.begin_finalization().await;
                    assert_eq!(registration.finish(claim.generation).await.unwrap(), None);
                }
            }
        }
    }
    #[tokio::test]
    async fn expected_run_guard_conflicts_and_blocks_finalization_until_publication_finishes() {
        use bamboo_domain::{SessionRunConflictReason, SessionRunDeliveryGuard};
        async fn retain(
            router: Arc<SessionActivationRouter>,
            birth: DateTime<Utc>,
            expected: &'static str,
        ) -> Result<Arc<dyn SessionRunDeliveryGuard>, bamboo_domain::SessionRunConflict> {
            tokio::task::spawn_blocking(move || {
                router.retain_expected_run_blocking("target", birth, expected)
            })
            .await
            .unwrap()
        }
        let router = SessionActivationRouter::new();
        let target = bamboo_domain::Session::new("target", "model");
        let birth = target.created_at;
        assert_eq!(
            retain(router.clone(), birth, "run-a")
                .await
                .err()
                .unwrap()
                .reason,
            SessionRunConflictReason::NoActiveRun
        );
        let mut registration = router.register_session_run(&target, "run-a").await.unwrap();
        assert_eq!(
            retain(router.clone(), birth, "replacement")
                .await
                .err()
                .unwrap()
                .reason,
            SessionRunConflictReason::RunChanged
        );
        let stale_birth = retain(
            router.clone(),
            birth + chrono::Duration::seconds(1),
            "run-a",
        )
        .await
        .err()
        .unwrap();
        assert_eq!(
            stale_birth.reason,
            SessionRunConflictReason::SessionBirthChanged
        );
        assert_eq!(stale_birth.observed_run_id, None);
        let guard = retain(router.clone(), birth, "run-a").await.unwrap();
        assert!(guard.matches("target", birth, "run-a"));
        assert!(!guard.matches("other-target", birth, "run-a"));
        let notify = guard.clone();
        assert!(
            tokio::task::spawn_blocking(move || notify.notify_committed_delivery(7))
                .await
                .unwrap()
        );
        let mut finalizing = Box::pin(router.begin_finalization("target", "run-a"));
        assert!(futures::poll!(finalizing.as_mut()).is_pending());
        drop(guard);
        finalizing.await;
        assert_eq!(
            retain(router.clone(), birth, "run-a")
                .await
                .err()
                .unwrap()
                .reason,
            SessionRunConflictReason::Finalizing
        );
        assert_eq!(*registration.notifications_mut().borrow(), 7);
        assert_eq!(registration.finish(0).await.unwrap(), None);
        assert_eq!(router.current_run_id("target").await, None);
    }

    #[tokio::test]
    async fn expected_run_publication_fence_does_not_block_other_sessions() {
        let router = SessionActivationRouter::new();
        let target = bamboo_domain::Session::new("target", "model");
        let registration = router.register_session_run(&target, "run-a").await.unwrap();
        let query = router.clone();
        let guard = tokio::task::spawn_blocking(move || {
            query.retain_expected_run_blocking(&target.id, target.created_at, "run-a")
        })
        .await
        .unwrap()
        .unwrap();
        // Keep the actual publication guard alive as if the final filesystem
        // job is parked. Every unrelated owner transition must still complete.
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            let other = bamboo_domain::Session::new("independent", "model");
            let mut other_run = router
                .register_session_run(&other, "independent-run")
                .await
                .unwrap();
            assert!(router.notify_active_delivery(&other.id, 11).await);
            assert_eq!(*other_run.notifications_mut().borrow(), 11);
            other_run.begin_finalization().await;
            assert_eq!(other_run.finish(0).await.unwrap(), None);
            assert_eq!(router.current_run_id(&other.id).await, None);
        })
        .await
        .expect("a parked publication must not hold the global router lock");
        drop(guard);
        assert_eq!(registration.finish(0).await.unwrap(), None);
    }

    #[tokio::test]
    async fn expected_run_requires_real_lifetime_binding_and_cannot_rebind() {
        use bamboo_domain::SessionRunConflictReason;
        let router = SessionActivationRouter::new();
        let target = bamboo_domain::Session::new("target", "model");
        let mut registration = router.register_run(&target.id, "run-a").await.unwrap();
        let query = router.clone();
        let birth = target.created_at;
        let conflict = tokio::task::spawn_blocking(move || {
            query.retain_expected_run_blocking("target", birth, "run-a")
        })
        .await
        .unwrap()
        .err()
        .unwrap();
        assert_eq!(conflict.reason, SessionRunConflictReason::UnboundLifetime);
        assert_eq!(conflict.observed_run_id, None);
        let other_router = SessionActivationRouter::new();
        assert!(
            !other_router
                .bind_run_lifetime(&registration.ownership(), &target)
                .await
        );
        assert!(
            router
                .bind_run_lifetime(&registration.ownership(), &target)
                .await
        );
        let mut replacement = target.clone();
        replacement.created_at += chrono::Duration::seconds(1);
        assert!(
            !router
                .bind_run_lifetime(&registration.ownership(), &replacement)
                .await
        );
        let query = router.clone();
        let guard = tokio::task::spawn_blocking(move || {
            query.retain_expected_run_blocking("target", birth, "run-a")
        })
        .await
        .unwrap()
        .unwrap();
        assert!(guard.matches("target", birth, "run-a"));
        drop(guard);
        registration.begin_finalization().await;
        assert!(
            !router
                .bind_run_lifetime(&registration.ownership(), &target)
                .await
        );
        assert_eq!(registration.finish(0).await.unwrap(), None);
    }

    #[tokio::test]
    async fn expected_run_guard_rejects_unregistered_reservations_without_notification() {
        let router = SessionActivationRouter::new();
        router
            .install_owner_placeholder_for_test("target", "run-a")
            .await;
        let query = router.clone();
        let result = tokio::task::spawn_blocking(move || {
            query.retain_expected_run_blocking("target", Utc::now(), "run-a")
        })
        .await
        .unwrap();
        assert_eq!(
            result.err().unwrap().reason,
            bamboo_domain::SessionRunConflictReason::NotRegistered
        );
        assert_eq!(
            router.current_run_id("target").await.as_deref(),
            Some("run-a")
        );
        assert_eq!(
            router.states.lock().await["target"].latest_delivery_generation,
            0
        );
    }

    mod tool_suspension_registration_tests {
        use super::*;
        use bamboo_agent_core::tools::{
            capture_suspension_restriction, observe_tool_suspension, with_suspension_disabled,
            AsyncWaitKind, RunningCompletion, RunningHandle, ToolOutcome, ToolResult,
            ToolSuspensionObservation,
        };

        async fn bind_running_probe(
            registration: &mut SessionRunRegistration,
            deny: bool,
            kills: Arc<AtomicUsize>,
        ) -> ToolSuspensionObservation {
            let (_, report) = observe_tool_suspension(false, async {
                registration
                    .bind_tool_suspension_observation(capture_suspension_restriction().unwrap());
                let collapse = async move {
                    ToolOutcome::Running(RunningHandle {
                        tool_call_id: "registration-running-probe".into(),
                        ack: ToolResult::text(true, "ordinary running ack"),
                        completion: RunningCompletion::Detached,
                        wait_kind: AsyncWaitKind::AsyncTools,
                        kill: Box::new(move || {
                            kills.fetch_add(1, Ordering::SeqCst);
                        }),
                    })
                    .into_tool_result()
                };
                let result = if deny {
                    with_suspension_disabled(collapse).await
                } else {
                    collapse.await
                };
                assert_eq!(result.success, !deny);
            })
            .await;
            report
        }

        #[tokio::test]
        async fn forbidden_observation_retains_exact_owner_after_drop_or_observer_cancel() {
            for cancel_observer in [false, true] {
                let router = SessionActivationRouter::new();
                let spawner = spawner();
                router.set_spawner(spawner.clone()).await;
                let mut registration = router
                    .register_run("session", "observed-running-owner")
                    .await
                    .unwrap();
                let cleaned = Arc::new(AtomicUsize::new(0));
                let clean_count = cleaned.clone();
                registration.set_abort_cleanup(move || async move {
                    clean_count.fetch_add(1, Ordering::SeqCst);
                });
                assert_eq!(
                    router.request_activation("session", 7).await.unwrap(),
                    SessionActivationDisposition::ActiveNotified
                );
                let kills = Arc::new(AtomicUsize::new(0));
                let report = if cancel_observer {
                    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
                    let child_kills = kills.clone();
                    let task = tokio::spawn(async move {
                        observe_tool_suspension(false, async move {
                            let report =
                                bind_running_probe(&mut registration, true, child_kills).await;
                            assert!(registration.observed_forbidden_running());
                            let _ = ready_tx.send(report);
                            std::future::pending::<()>().await;
                            // Keep the registration owned across the pending
                            // await; cancellation drops it with its real token.
                            drop(registration);
                        })
                        .await;
                    });
                    let ready =
                        tokio::time::timeout(std::time::Duration::from_secs(2), ready_rx).await;
                    task.abort();
                    let task_exit = task.await;
                    assert!(task_exit.unwrap_err().is_cancelled());
                    ready.unwrap().unwrap()
                } else {
                    let report = bind_running_probe(&mut registration, true, kills.clone()).await;
                    assert!(registration.observed_forbidden_running());
                    // The observer is already gone: only the opaque token
                    // retained on this exact lease preserves the proof.
                    drop(registration);
                    report
                };
                assert!(report.running_seen && report.forbidden_running_seen);
                assert_eq!(kills.load(Ordering::SeqCst), 1);

                tokio::time::timeout(std::time::Duration::from_secs(2), async {
                    loop {
                        let finalized = router
                            .states
                            .lock()
                            .await
                            .get("session")
                            .and_then(|state| state.owner.as_ref())
                            .is_some_and(|owner| {
                                owner.run_id == "observed-running-owner" && owner.finalizing
                            });
                        if finalized {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("the exact retained owner must enter finalization");
                assert_eq!(
                    router.request_activation("session", 8).await.unwrap(),
                    SessionActivationDisposition::ActivationCoalesced
                );
                for attempted in ["successor", "observed-running-owner"] {
                    let collision = router
                        .register_run("session", attempted)
                        .await
                        .err()
                        .expect("a quarantined owner cannot be replaced or adopted");
                    assert_eq!(collision.existing_run_id(), "observed-running-owner");
                }
                assert_eq!(cleaned.load(Ordering::SeqCst), 0);
                assert_eq!(spawner.reservations.load(Ordering::SeqCst), 0);
                assert_eq!(spawner.launches.load(Ordering::SeqCst), 0);
                let states = router.states.lock().await;
                let owner = states["session"].owner.as_ref().unwrap();
                assert_eq!(owner.run_id, "observed-running-owner");
                assert!(owner.finalizing);
                assert_eq!(owner.registrations, 1);
            }
        }

        #[tokio::test]
        async fn allowed_observation_preserves_ordinary_drop_and_finish_successors() {
            for finish_normally in [false, true] {
                let router = SessionActivationRouter::new();
                let spawner = spawner();
                router.set_spawner(spawner.clone()).await;
                let mut registration = router
                    .register_run("session", "ordinary-observed-owner")
                    .await
                    .unwrap();
                let cleaned = Arc::new(AtomicUsize::new(0));
                let clean_count = cleaned.clone();
                registration.set_abort_cleanup(move || async move {
                    clean_count.fetch_add(1, Ordering::SeqCst);
                });
                let kills = Arc::new(AtomicUsize::new(0));
                let report = bind_running_probe(&mut registration, false, kills.clone()).await;
                assert!(report.running_seen);
                assert!(!report.forbidden_running_seen);
                assert!(!registration.observed_forbidden_running());
                assert_eq!(kills.load(Ordering::SeqCst), 0);
                assert_eq!(
                    router.request_activation("session", 7).await.unwrap(),
                    SessionActivationDisposition::ActiveNotified
                );
                if finish_normally {
                    registration.begin_finalization().await;
                    assert_eq!(
                        registration.finish(0).await.unwrap(),
                        Some(SessionActivationDisposition::ActivationReserved)
                    );
                } else {
                    drop(registration);
                }
                tokio::time::timeout(std::time::Duration::from_secs(2), async {
                    while spawner.launches.load(Ordering::SeqCst) == 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("ordinary handoff must still launch its one successor");
                assert_eq!(spawner.reservations.load(Ordering::SeqCst), 1);
                assert_eq!(spawner.launches.load(Ordering::SeqCst), 1);
                assert_eq!(
                    cleaned.load(Ordering::SeqCst),
                    usize::from(!finish_normally)
                );
                assert!(router.owns_run("session", "run-1").await);
                let mut successor = router.register_run("session", "run-1").await.unwrap();
                successor.begin_finalization().await;
                assert_eq!(successor.finish(7).await.unwrap(), None);
                assert!(router.current_run_id("session").await.is_none());
                assert_eq!(spawner.launches.load(Ordering::SeqCst), 1);
            }
        }
    }
}

#[cfg(test)]
#[path = "session_activation_child_completion_tests.rs"]
mod child_completion_fence_tests;
