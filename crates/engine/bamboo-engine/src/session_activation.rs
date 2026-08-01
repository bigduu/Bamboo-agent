//! Runtime-scoped logical-session activation router.
//!
//! Durable delivery and execution reservation are separate operations. This
//! router closes their terminal race without treating an execution id, process,
//! or warm-worker mailbox as the durable session address.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock as StdRwLock};

use async_trait::async_trait;
use bamboo_domain::{
    SessionActivationDisposition, SessionActivationError, SessionActivationPort, SessionInboxPort,
};
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
    finalizing: bool,
    registrations: usize,
    /// Present only while an external actor driver is alive. The payload is a
    /// wake generation; that driver remains the single consumer of the
    /// canonical FileSessionInbox and forwards the claimed typed envelope.
    delivery_sink: Option<mpsc::UnboundedSender<u64>>,
}

#[derive(Debug)]
struct TargetActivationState {
    latest_generation: u64,
    /// Highest generation for which this process already launched an execution.
    /// Adopting an independently running owner does not consume this retry. If a
    /// launched execution leaves the same poison claim pending,
    /// terminal finalization must not hot-loop provider runs forever. A newer
    /// generation may still trigger one fresh activation; process restart also
    /// resets this bounded retry guard.
    last_dispatched_generation: u64,
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
            last_dispatched_generation: 0,
            owner: None,
            activation_reserved: false,
            activation_token: 0,
            activation_epoch,
            notify,
        }
    }
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

/// One router per owning runtime/AppState. All keys are logical Session ids.
#[derive(Clone, Default)]
pub struct SessionActivationRouter {
    states: Arc<Mutex<HashMap<String, TargetActivationState>>>,
    spawner: Arc<RwLock<Option<Arc<dyn SessionActivationSpawner>>>>,
    inbox: Arc<StdRwLock<Option<Arc<dyn SessionInboxPort>>>>,
}

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
    armed: bool,
}

impl SessionRunRegistration {
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
        let abort_cleanup = self.abort_cleanup.take();
        let router = self.router.clone();
        let target_session_id = self.target_session_id.clone();
        let run_id = self.run_id.clone();
        let cleanup_target_session_id = target_session_id.clone();
        let cleanup_run_id = run_id.clone();
        let cleanup = tokio::spawn(async move {
            if let Some(cleanup) = abort_cleanup {
                cleanup().await;
            }
            router
                .cleanup_abandoned_registration(&cleanup_target_session_id, &cleanup_run_id)
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
        let result = self
            .router
            .finish_finalization(&self.target_session_id, &self.run_id, admitted_generation)
            .await;
        self.armed = false;
        self.abort_cleanup = None;
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
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if let Some(cleanup) = abort_cleanup {
                    cleanup().await;
                }
                router
                    .cleanup_abandoned_registration(&target_session_id, &run_id)
                    .await;
            });
        }
    }
}

impl SessionActivationRouter {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Late-bind the real server/SDK reservation adapter after its dependency
    /// graph has been assembled.
    pub async fn set_spawner(&self, spawner: Arc<dyn SessionActivationSpawner>) {
        *self.spawner.write().await = Some(spawner);
    }

    /// Bind the durable inbox used by abandoned-run reconciliation.
    pub fn set_inbox(&self, inbox: Arc<dyn SessionInboxPort>) {
        *self
            .inbox
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(inbox);
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
        let mut states = self.states.lock().await;
        states
            .entry(target_session_id.to_string())
            .or_default()
            .owner = Some(ActiveOwner {
            run_id: run_id.to_string(),
            finalizing: false,
            registrations: 0,
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
        loop {
            let (notifications, reservation_wait) = {
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
                            finalizing: false,
                            registrations: 1,
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
        if state.latest_generation > 0 && sink.send(state.latest_generation).is_err() {
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
        let mut states = self.states.lock().await;
        let state = states.entry(target_session_id.to_string()).or_default();
        match state.owner.as_mut() {
            Some(owner) if owner.run_id == run_id => owner.finalizing = true,
            Some(_) => {}
            None => {
                state.owner = Some(ActiveOwner {
                    run_id: run_id.to_string(),
                    finalizing: true,
                    registrations: 0,
                    delivery_sink: None,
                });
            }
        }
    }

    async fn cleanup_abandoned_registration(
        self: &Arc<Self>,
        target_session_id: &str,
        run_id: &str,
    ) {
        {
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
        let durable_generation = match inbox {
            Some(inbox) => match inbox.inspect(target_session_id).await {
                Ok(backlog) if backlog.activation_pending() => backlog.activation_generation,
                Ok(_) => 0,
                Err(error) => {
                    tracing::warn!(
                        %target_session_id,
                        %run_id,
                        %error,
                        "failed to reconcile durable SessionInbox generation for abandoned run"
                    );
                    0
                }
            },
            None => 0,
        };

        let reservation_to_dispatch = {
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
            state.owner = None;
            if state.latest_generation > state.last_dispatched_generation
                && !state.activation_reserved
            {
                let generation = state.latest_generation;
                let token = reserve_activation_token(state);
                Some((generation, token))
            } else {
                if state.latest_generation == 0
                    && !state.activation_reserved
                    && state.notify.receiver_count() == 0
                    && state.activation_epoch.receiver_count() == 0
                {
                    states.remove(target_session_id);
                }
                None
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
        let reservation_to_dispatch = {
            let mut states = self.states.lock().await;
            let state = states.entry(target_session_id.to_string()).or_default();
            if state
                .owner
                .as_ref()
                .is_some_and(|owner| owner.run_id != run_id)
            {
                return Ok(None);
            }
            state.owner = None;
            if state.latest_generation > admitted_generation
                && state.latest_generation > state.last_dispatched_generation
                && !state.activation_reserved
            {
                let generation = state.latest_generation;
                let token = reserve_activation_token(state);
                Some((generation, token))
            } else {
                // Both execution paths drop their safe-point receiver before
                // calling finish_finalization. Once the durable cursor has
                // caught up and no reservation exists, this target carries no
                // live routing state and must not remain in AppState forever.
                if state.latest_generation <= admitted_generation
                    && state.owner.is_none()
                    && !state.activation_reserved
                    && state.notify.receiver_count() == 0
                    && state.activation_epoch.receiver_count() == 0
                {
                    states.remove(target_session_id);
                }
                None
            }
        };

        match reservation_to_dispatch {
            Some((generation, token)) => self
                .dispatch_reserved(target_session_id, generation, token)
                .await
                .map(Some),
            None => Ok(None),
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
        let reservation_to_dispatch = {
            let mut states = self.states.lock().await;
            let Some(state) = states.get_mut(target_session_id) else {
                return;
            };
            if !release_activation_token(state, reservation_token) || !recover_on_drop {
                return;
            }
            if state.owner.is_none()
                && state.latest_generation > state.last_dispatched_generation
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
                .dispatch_reserved_with_recovery(target_session_id, generation, token, false)
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
        self.dispatch_reserved_with_recovery(target_session_id, generation, reservation_token, true)
            .await
    }

    async fn dispatch_reserved_with_recovery(
        &self,
        target_session_id: &str,
        generation: u64,
        reservation_token: u64,
        recover_on_drop: bool,
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
                        finalizing: false,
                        registrations: 0,
                        delivery_sink: None,
                    });
                    state.last_dispatched_generation =
                        state.last_dispatched_generation.max(generation);
                }
                lease.disarm();
                // The existing runner slot is already reserved. Publish owner
                // state first, then make the task runnable exactly once.
                launch.launch();
                Ok(SessionActivationDisposition::ActivationReserved)
            }
            Ok(SessionActivationReserveOutcome::AlreadyRunning { run_id }) => {
                let mut states = self.states.lock().await;
                let state = states.entry(target_session_id.to_string()).or_default();
                if !release_activation_token(state, reservation_token) {
                    lease.disarm();
                    return Ok(SessionActivationDisposition::ActiveNotified);
                }
                state.owner = Some(ActiveOwner {
                    run_id,
                    finalizing: false,
                    registrations: 0,
                    delivery_sink: None,
                });
                state.notify.send_replace(generation);
                lease.disarm();
                Ok(SessionActivationDisposition::ActiveNotified)
            }
            Ok(SessionActivationReserveOutcome::NoWork) => {
                let mut states = self.states.lock().await;
                if let Some(state) = states.get_mut(target_session_id) {
                    release_activation_token(state, reservation_token);
                }
                lease.disarm();
                Ok(SessionActivationDisposition::ActivationCoalesced)
            }
            Ok(SessionActivationReserveOutcome::NotFound) => {
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
    async fn request_activation(
        &self,
        target_session_id: &str,
        inbox_generation: u64,
    ) -> Result<SessionActivationDisposition, SessionActivationError> {
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
                if inbox_generation <= state.last_dispatched_generation {
                    return Ok(SessionActivationDisposition::ActivationCoalesced);
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
    }

    #[async_trait]
    impl SessionInboxPort for BlockingInspectInbox {
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
            self.entered.notify_one();
            self.release.notified().await;
            Ok(bamboo_domain::SessionInboxBacklog {
                pending: 1,
                claimed: 0,
                generation: 1,
                activation_generation: 1,
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
        router.set_inbox(Arc::new(BlockingInspectInbox {
            entered: inspect_entered.clone(),
            release: inspect_release.clone(),
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
    async fn poison_generation_launches_only_one_successor_until_new_work_arrives() {
        let router = SessionActivationRouter::new();
        let spawner = spawner();
        router.set_spawner(spawner.clone()).await;
        let mut registration = router
            .register_run("session", "run-original")
            .await
            .unwrap();
        assert_eq!(
            router.request_activation("session", 7).await.unwrap(),
            SessionActivationDisposition::ActiveNotified
        );
        registration.begin_finalization().await;
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
}
