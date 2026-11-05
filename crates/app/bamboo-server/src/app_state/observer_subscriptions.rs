//! One bounded Host pass on the existing wake tick. No model polling or queue.
use super::AppState;
use bamboo_agent_core::tools::ToolCtx;
use bamboo_domain::{
    ObserverChangeKind, ObserverSubscription, ObserverSubscriptionFailure as Failure,
    SessionCallerReference, SessionInboxPort, Storage, SupervisorReference,
    DEFAULT_SUPERVISOR_SESSION_ID,
};
use bamboo_engine::SessionActivationRouter;
use bamboo_orchestration::managed_session::{ManagedRootNotificationIo, ManagedRootNotifications};
use bamboo_server_tools::SessionInspectorTool;
use bamboo_storage::{FileSessionInbox, SessionStoreV2};
use chrono::{DateTime, Utc};
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    sync::Arc,
    time::Instant,
};
use tokio::sync::Mutex;
use uuid::Uuid;

/// Trusted Host input after Creator auto_run=false has returned the original ID.
/// No public HTTP/model surface allocates or adopts a replacement Observer.
pub struct ObserverSubscriptionStart {
    pub supervisor: SupervisorReference,
    pub expected_state_revision: u64,
    pub operation_id: Uuid,
    pub child_id: String,
    pub targets: BTreeSet<String>,
    pub filter: BTreeSet<ObserverChangeKind>,
    pub min_interval_ms: u32,
}

#[derive(serde::Deserialize)]
struct PublishedViewSelector {
    revision: String,
    #[serde(default)]
    root_outcome: Option<bamboo_domain::ObserverRootOutcomeCursor>,
}

struct CachedMaterialSource {
    hint: String,
    created_at: DateTime<Utc>,
    stamps: bamboo_storage::v2::MaterialNotificationStamps,
    candidates: Vec<bamboo_domain::ManagedRootNotification>,
    delivered: Vec<bamboo_domain::ManagedRootNotification>,
    delivery_due: bool,
    settled: bool,
    last_used: Instant,
}

enum MaterialProofCompletion {
    Source {
        source: String,
        result: Result<Box<CachedMaterialSource>, String>,
    },
    Delivered {
        selected: Box<bamboo_domain::ManagedRootNotification>,
        result: Result<bool, String>,
    },
}

pub(super) struct ObserverSubscriptionCoordinator {
    store: Arc<SessionStoreV2>,
    inbox: Arc<FileSessionInbox>,
    activation: Arc<SessionActivationRouter>,
    inspector: SessionInspectorTool,
    // Fairness/coalescing only. All event/cursor/retry truth remains canonical.
    next: Mutex<usize>,
    material_notifications: ManagedRootNotifications,
    material_cache: Mutex<BTreeMap<String, CachedMaterialSource>>,
    material_proof_io: Arc<tokio::sync::Semaphore>,
    material_pending: Mutex<Option<tokio::sync::oneshot::Receiver<MaterialProofCompletion>>>,
    #[cfg(test)]
    material_main_reads: std::sync::atomic::AtomicUsize,
    native: bool,
}

impl ObserverSubscriptionCoordinator {
    pub(super) fn new(
        store: Arc<SessionStoreV2>,
        inbox: Arc<FileSessionInbox>,
        activation: Arc<SessionActivationRouter>,
        storage: Arc<dyn Storage>,
        native: bool,
    ) -> Self {
        if native {
            inbox.enable_material_notifications();
        }
        Self {
            inspector: SessionInspectorTool::host_routed(store.clone(), storage)
                .with_observer_outcome_inbox(inbox.clone()),
            store,
            inbox,
            activation,
            next: Mutex::new(0),
            material_notifications: ManagedRootNotifications::default(),
            material_cache: Mutex::new(BTreeMap::new()),
            material_proof_io: Arc::new(tokio::sync::Semaphore::new(1)),
            material_pending: Mutex::new(None),
            #[cfg(test)]
            material_main_reads: std::sync::atomic::AtomicUsize::new(0),
            native,
        }
    }

    pub(super) fn spawn_material_notifications(
        self: &Arc<Self>,
        shutdown: tokio_util::sync::CancellationToken,
    ) {
        if !self.native {
            return;
        }
        let io = Arc::clone(self);
        tokio::spawn(async move {
            io.material_notifications.run(io.as_ref(), shutdown).await;
        });
    }

    #[cfg(test)]
    pub(super) async fn material_tick(&self) -> Result<(), String> {
        self.material_notifications.tick(self).await
    }

    async fn begin_material_proof(
        &self,
    ) -> Result<
        (
            tokio::sync::OwnedSemaphorePermit,
            tokio::sync::oneshot::Sender<MaterialProofCompletion>,
        ),
        String,
    > {
        let mut pending = self.material_pending.lock().await;
        if pending.is_some() {
            return Err("material proof I/O remains pending".into());
        }
        let permit = Arc::clone(&self.material_proof_io)
            .try_acquire_owned()
            .map_err(|_| "material proof I/O is busy".to_string())?;
        let (send, receive) = tokio::sync::oneshot::channel();
        *pending = Some(receive);
        Ok((permit, send))
    }

    async fn finish_material_proof(&self) -> Result<(), String> {
        let completion = {
            let mut pending = self.material_pending.lock().await;
            let Some(receive) = pending.as_mut() else {
                return Ok(());
            };
            match receive.try_recv() {
                Ok(completion) => {
                    *pending = None;
                    completion
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => return Ok(()),
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                    *pending = None;
                    return Err("material proof I/O completion is unavailable".into());
                }
            }
        };
        match completion {
            MaterialProofCompletion::Source { source, result } => {
                let cached = result?;
                let mut cache = self.material_cache.lock().await;
                if cache.len() >= 16 && !cache.contains_key(&source) {
                    if let Some(oldest) = cache
                        .iter()
                        .min_by_key(|(_, cached)| cached.last_used)
                        .map(|(id, _)| id.clone())
                    {
                        cache.remove(&oldest);
                    }
                }
                cache.insert(source, *cached);
            }
            MaterialProofCompletion::Delivered { selected, result } => {
                if let Some(cached) = self
                    .material_cache
                    .lock()
                    .await
                    .get_mut(&selected.target.session_id)
                    .filter(|cached| cached.candidates.contains(&selected))
                {
                    cached.delivery_due = false;
                    if matches!(result, Ok(true)) {
                        cached.delivered.push(*selected);
                    }
                }
                result?;
            }
        }
        Ok(())
    }

    /// At most one registered Observer and its fixed (<=8) targets per tick.
    pub(super) async fn tick(&self, now: DateTime<Utc>) {
        if !self.native {
            return;
        }
        let Ok(mut next) = self.next.try_lock() else {
            return;
        };
        let parent = match self
            .store
            .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
            .await
        {
            Ok(Some(parent)) => parent,
            Ok(None) => return,
            Err(error) => {
                tracing::warn!(%error,"Observer canonical Supervisor read failed");
                return;
            }
        };
        let Some(incarnation_id) = parent.supervisor_incarnation_id() else {
            return;
        };
        let Some(state) = parent.supervisor_management.as_ref() else {
            return;
        };
        let eligible: Vec<_> = state
            .subscriptions
            .iter()
            .filter(|(id, subscription)| {
                (subscription.ready(now)
                    || (subscription.failures == 8
                        && subscription
                            .terminal_fault
                            .as_ref()
                            .is_none_or(|fault| fault.ready(now))))
                    && state.observers.get(*id).is_some_and(|registration| {
                        registration.enabled
                            && registration.revision == subscription.window.scope_epoch
                    })
            })
            .collect();
        if eligible.is_empty() {
            return;
        }
        let (id, subscription) = eligible[*next % eligible.len()];
        *next = next.wrapping_add(1);
        let reference = SupervisorReference {
            session_id: parent.id.clone(),
            incarnation_id,
        };
        if subscription.failures == 8 {
            match self
                .inbox
                .deliver_supervisor_observer_fault(&reference, id, now)
                .await
            {
                Ok(_) => {
                    if let Err(error) = self
                        .activation
                        .reconcile_wake(&reference.session_id, now)
                        .await
                    {
                        tracing::warn!(observer_id=%id,%error,"Observer diagnostic enqueued; parent wake remains under Inbox recovery");
                    }
                }
                Err(error) => {
                    tracing::warn!(observer_id=%id,%error,"Observer fault remains unreported; bounded delivery retry is canonical")
                }
            }
            return;
        }
        let result = self.reconcile(&reference, id, subscription, now).await;
        if let Err((failure, error)) = result {
            tracing::warn!(observer_id=%id,%error,"Observer subscription pass failed; recording bounded retry");
            if let Err(error) = self
                .store
                .record_observer_subscription_failure(&reference, id, now, failure)
                .await
            {
                tracing::warn!(observer_id=%id,%error,"Observer retry diagnostic could not be persisted");
            }
        }
    }

    async fn reconcile(
        &self,
        reference: &SupervisorReference,
        id: &str,
        subscription: &ObserverSubscription,
        now: DateTime<Utc>,
    ) -> Result<(), (Failure, String)> {
        if subscription.window.pending.is_none() {
            // Receipt/cursor is producer truth only. A real pending/claimed
            // Inbox item or active native run prevents publishing another view.
            let backlog = self
                .inbox
                .inspect(id)
                .await
                .map_err(|error| (Failure::InboxUnavailable, error.to_string()))?;
            if backlog.pending > 0
                || backlog.claimed > 0
                || self.activation.current_run_id(id).await.is_some()
            {
                self.activation
                    .reconcile_wake(id, now)
                    .await
                    .map_err(|error| (Failure::InboxUnavailable, error.to_string()))?;
                return Ok(());
            }
            self.store
                .initialize_observer_root_outcome_lane(reference, id)
                .await
                .map_err(authority)?;
            let parent = self
                .store
                .load_root_authority(&reference.session_id)
                .await
                .map_err(authority)?
                .ok_or_else(|| authority("Observer parent disappeared"))?;
            let child = self
                .store
                .load_child_wait_session(&parent, id, true)
                .await
                .map_err(authority)?
                .ok_or_else(|| authority("Observer Child disappeared"))?;
            let registration = parent
                .supervisor_management
                .as_ref()
                .and_then(|state| state.observers.get(id))
                .ok_or_else(|| authority("Observer registration disappeared"))?;
            let ids: Vec<_> = registration.target_links.keys().collect();
            if ids.is_empty() {
                return Err(authority("Observer target set is empty"));
            }
            let mut ctx = ToolCtx::none("supervisor-observation");
            ctx.session_id = Some(Arc::from(child.id.as_str()));
            ctx.executing_caller = Some(Arc::new(SessionCallerReference::from(&child)));
            for offset in 0..ids.len() {
                let target =
                    ids[(subscription.window.enqueued_sequence as usize + offset) % ids.len()];
                let previous = subscription
                    .window
                    .enqueued_fingerprints
                    .get(target)
                    .map(String::as_str);
                let Some(view) = self
                    .inspector
                    .observe_next_root_outcome(&ctx, target, &subscription.filter, previous)
                    .await
                    .map_err(|error| (Failure::PublicationUnavailable, error.to_string()))?
                else {
                    continue;
                };
                // Deserialize only the selector. Absolute paths and the raw
                // ToolResult are never forwarded to Inbox/provider messages.
                let receipt: PublishedViewSelector = serde_json::from_str(&view.result)
                    .map_err(|error| (Failure::PublicationUnavailable, error.to_string()))?;
                self.store
                    .stage_supervisor_observation_with_root_outcome(
                        reference,
                        id,
                        target,
                        &receipt.revision,
                        receipt.root_outcome.as_ref(),
                        now,
                    )
                    .await
                    .map_err(|error| (Failure::PublicationUnavailable, error.to_string()))?;
                break;
            }
            // An unchanged pass publishes nothing and cannot wake a model.
            let parent = self
                .store
                .load_root_authority(&reference.session_id)
                .await
                .map_err(authority)?
                .ok_or_else(|| authority("Observer parent disappeared"))?;
            if parent
                .supervisor_management
                .as_ref()
                .and_then(|state| state.subscriptions.get(id))
                .is_none_or(|subscription| subscription.window.pending.is_none())
            {
                return Ok(());
            }
        }
        let parent = self
            .store
            .load_root_authority(&reference.session_id)
            .await
            .map_err(authority)?
            .ok_or_else(|| authority("Observer parent disappeared"))?;
        let pending = parent
            .supervisor_management
            .as_ref()
            .and_then(|state| state.subscriptions.get(id))
            .and_then(|subscription| subscription.window.pending.as_ref());
        if let Some(change) = pending.filter(|change| change.root_outcome.is_some()) {
            let child = self
                .store
                .load_child_wait_session(&parent, id, true)
                .await
                .map_err(authority)?
                .ok_or_else(|| authority("Observer Child disappeared"))?;
            let caller = SessionCallerReference::from(&child);
            let position = change.root_outcome.as_ref().expect("checked outcome");
            let delivered = self
                .inbox
                .deliver_observer_root_outcome_to_supervisor(
                    &caller,
                    reference,
                    &change.target_session_id,
                    position.sequence,
                    now,
                )
                .await
                .map_err(|error| (Failure::InboxUnavailable, error.to_string()))?;
            if delivered.is_none() {
                return Ok(());
            }
            self.activation
                .reconcile_wake(&reference.session_id, now)
                .await
                .map_err(|error| (Failure::InboxUnavailable, error.to_string()))?;
        }
        self.inbox
            .deliver_supervisor_observation(reference, id, now)
            .await
            .map_err(|error| (Failure::InboxUnavailable, error.to_string()))?;
        // Existing Inbox readiness/activation owns admission, leases and wake
        // recovery. No enqueue_child_run or synthetic completion is involved.
        self.activation
            .reconcile_wake(id, now)
            .await
            .map_err(|error| (Failure::InboxUnavailable, error.to_string()))?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl ManagedRootNotificationIo for ObserverSubscriptionCoordinator {
    async fn sources(&self) -> Vec<String> {
        if let Err(error) = self.finish_material_proof().await {
            tracing::debug!(%error, "Managed Session material proof remains pending");
        }
        // Attempt a completed observation once before refreshing its stamps or
        // admitting another source. The final writer always rechecks authority.
        let ready: Vec<_> = self
            .material_cache
            .lock()
            .await
            .iter()
            .filter(|(_, cached)| cached.delivery_due)
            .map(|(source, _)| source.clone())
            .collect();
        if !ready.is_empty() {
            return ready;
        }
        self.store
            .list_index_entries()
            .await
            .into_iter()
            .filter(|entry| {
                entry.kind == bamboo_domain::SessionKind::Root
                    && entry.id != DEFAULT_SUPERVISOR_SESSION_ID
                    && entry.material_notification_hint.is_some()
            })
            .map(|entry| entry.id)
            .collect()
    }

    async fn candidates(
        &self,
        source: &str,
    ) -> Result<Vec<bamboo_domain::ManagedRootNotification>, String> {
        if let Some(cached) = self
            .material_cache
            .lock()
            .await
            .get(source)
            .filter(|cached| cached.delivery_due)
        {
            return Ok(cached.candidates.clone());
        }
        let hint = self
            .store
            .get_index_entry(source)
            .await
            .and_then(|entry| entry.material_notification_hint)
            .ok_or_else(|| "managed notification hint is unavailable".to_string())?;
        let stamps = self
            .store
            .material_notification_stamps(source)
            .await
            .map_err(|error| error.to_string())?;
        {
            let mut cache = self.material_cache.lock().await;
            if let Some(cached) = cache
                .get_mut(source)
                .filter(|cached| cached.hint == hint && cached.stamps == stamps)
            {
                cached.last_used = Instant::now();
                return Ok(cached.candidates.clone());
            }
        }
        let (permit, send) = self.begin_material_proof().await?;
        #[cfg(test)]
        self.material_main_reads
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let store = Arc::clone(&self.store);
        let source_id = source.to_owned();
        let runtime = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let result = (|| {
                let session = runtime
                    .block_on(store.try_load_material_notification_source(&source_id))
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| "managed notification source is unavailable".to_string())?;
                let candidates =
                    bamboo_orchestration::managed_session::notification_candidates(&session)
                        .map_err(str::to_owned)?;
                let settled = bamboo_orchestration::managed_session::notification_source_settled(
                    &session, &hint,
                );
                Ok(Box::new(CachedMaterialSource {
                    hint,
                    created_at: session.created_at,
                    stamps,
                    delivery_due: !candidates.is_empty(),
                    candidates,
                    delivered: Vec::new(),
                    settled,
                    last_used: Instant::now(),
                }))
            })();
            let _ = send.send(MaterialProofCompletion::Source {
                source: source_id,
                result,
            });
        });
        Err("material proof I/O remains pending".into())
    }

    async fn deliver(
        &self,
        selected: &bamboo_domain::ManagedRootNotification,
    ) -> Result<(), String> {
        let now = Utc::now();
        let already_delivered = self
            .material_cache
            .lock()
            .await
            .get(&selected.target.session_id)
            .is_some_and(|cached| cached.delivered.contains(selected));
        if !already_delivered {
            let (permit, send) = self.begin_material_proof().await?;
            let inbox = Arc::clone(&self.inbox);
            let notification = selected.clone();
            let runtime = tokio::runtime::Handle::current();
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                let result = runtime
                    .block_on(inbox.deliver_managed_root_notification(&notification, now))
                    .map(|receipt| receipt.is_some())
                    .map_err(|error| error.to_string());
                let _ = send.send(MaterialProofCompletion::Delivered {
                    selected: Box::new(notification),
                    result,
                });
            });
            return Err("material proof I/O remains pending".into());
        }
        self.activation
            .reconcile_wake(&selected.caller.session_id, now)
            .await
            .map_err(|error| error.to_string())?;
        let retired_hint = self
            .material_cache
            .lock()
            .await
            .get(&selected.target.session_id)
            .filter(|cached| {
                cached.settled
                    && cached
                        .candidates
                        .iter()
                        .all(|candidate| cached.delivered.contains(candidate))
            })
            .map(|cached| (cached.created_at, cached.hint.clone()));
        if let Some((birth, hint)) = retired_hint {
            self.store
                .clear_material_notification_hint(&selected.target.session_id, birth, &hint)
                .await
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}

fn authority(error: impl std::fmt::Display) -> (Failure, String) {
    (Failure::AuthorityUnavailable, error.to_string())
}

impl AppState {
    /// Creator -> register-only -> persist configuration -> actual initial
    /// Inbox notice. Repeating the operation never enqueues a separate launch.
    pub async fn subscribe_supervisor_observer(
        &self,
        request: ObserverSubscriptionStart,
    ) -> io::Result<ObserverSubscription> {
        if !self.spawn_scheduler.supports_native_ticket_budget() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Observer subscriptions require the native backend",
            ));
        }
        let registration = self
            .session_store
            .register_supervisor_observer(
                &request.supervisor,
                request.expected_state_revision,
                request.operation_id,
                &request.child_id,
                request.targets,
            )
            .await?;
        if !registration.enabled {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "Observer registration is revoked",
            ));
        }
        let subscription = self
            .session_store
            .subscribe_supervisor_observer(
                &request.supervisor,
                registration.state_revision,
                &request.child_id,
                request.filter,
                request.min_interval_ms,
            )
            .await?;
        self.observer_subscriptions.tick(Utc::now()).await;
        Ok(subscription)
    }
}

#[cfg(test)]
#[path = "observer_subscription_tests.rs"]
pub(super) mod tests;
