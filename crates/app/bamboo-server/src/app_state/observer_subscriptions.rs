//! One bounded Host pass on the existing wake tick. No model polling or queue.
use super::AppState;
use bamboo_agent_core::tools::ToolCtx;
use bamboo_domain::{
    ObserverChangeKind, ObserverSubscription, ObserverSubscriptionFailure as Failure,
    SessionCallerReference, SessionInboxPort, Storage, SupervisorReference,
    DEFAULT_SUPERVISOR_SESSION_ID,
};
use bamboo_engine::SessionActivationRouter;
use bamboo_server_tools::SessionInspectorTool;
use bamboo_storage::{FileSessionInbox, SessionStoreV2};
use chrono::{DateTime, Utc};
use std::{collections::BTreeSet, io, sync::Arc};
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
}

pub(super) struct ObserverSubscriptionCoordinator {
    store: Arc<SessionStoreV2>,
    inbox: Arc<FileSessionInbox>,
    activation: Arc<SessionActivationRouter>,
    inspector: SessionInspectorTool,
    // Fairness/coalescing only. All event/cursor/retry truth remains canonical.
    next: Mutex<usize>,
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
        Self {
            inspector: SessionInspectorTool::host_routed(store.clone(), storage),
            store,
            inbox,
            activation,
            next: Mutex::new(0),
            native,
        }
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
                    .observe_context(&ctx, target, &subscription.filter, previous)
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
                    .stage_supervisor_observation(reference, id, target, &receipt.revision, now)
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
