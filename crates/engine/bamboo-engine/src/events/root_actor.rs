//! One immutable Root execution's actual per-session/account publication.

use super::{account_sink::ReservedAccountEvent, AccountEventSink};
use bamboo_agent_core::AgentEvent;
use bamboo_domain::{RootActorRuntimeWrite, Storage};
use std::sync::Arc;
use tokio::sync::oneshot;

#[derive(Clone)]
pub struct RootActorEventPublication {
    storage: Arc<dyn Storage>,
    account_sink: Arc<AccountEventSink>,
    owner: RootActorRuntimeWrite,
}

pub struct PreparedRootActorEvent {
    storage: Arc<dyn Storage>,
    owner: RootActorRuntimeWrite,
    account: Option<(ReservedAccountEvent, oneshot::Receiver<bool>)>,
}

impl RootActorEventPublication {
    pub fn new(
        storage: Arc<dyn Storage>,
        account_sink: Arc<AccountEventSink>,
        owner: RootActorRuntimeWrite,
    ) -> Self {
        Self {
            storage,
            account_sink,
            owner,
        }
    }

    pub fn matches_execution(&self, session_id: &str, run_id: &str) -> bool {
        self.owner.fence.actor_id == session_id && self.owner.fence.run_id == run_id
    }

    /// Numeric observations and a process-local opaque identifier, never the
    /// owner capability or lease-owner value.
    pub(crate) fn diagnostic_identity(&self) -> (u64, u64, String) {
        (
            self.owner.fence.lease_epoch,
            self.owner.fence.attempt,
            crate::execution::workspace_diagnostics::fence_fingerprint(&self.owner.fence),
        )
    }

    pub async fn publish(
        &self,
        session_id: &str,
        event: &AgentEvent,
        publish: Box<dyn FnOnce() -> bool + Send>,
    ) -> std::io::Result<()> {
        self.prepare(session_id, event)
            .await?
            .publish(publish)
            .await
    }

    /// Reserve account capacity before taking any local or physical publication guard.
    pub async fn prepare(
        &self,
        session_id: &str,
        event: &AgentEvent,
    ) -> std::io::Result<PreparedRootActorEvent> {
        let account = self
            .account_sink
            .reserve_root_actor_event(self.storage.clone(), self.owner.clone(), session_id, event)
            .await?;
        Ok(PreparedRootActorEvent {
            storage: self.storage.clone(),
            owner: self.owner.clone(),
            account,
        })
    }
}

impl PreparedRootActorEvent {
    pub async fn publish(self, publish: Box<dyn FnOnce() -> bool + Send>) -> std::io::Result<()> {
        let Self {
            storage,
            owner,
            account,
        } = self;
        let (reservation, confirmation) = account.unzip();
        storage
            .publish_root_actor_runtime_event(
                &owner,
                Box::new(move |check_current| {
                    check_current()?;
                    if !publish() {
                        return Err(std::io::Error::other(
                            "Root per-session event publication was retired",
                        ));
                    }
                    check_current()?;
                    if let Some(reservation) = reservation {
                        reservation.enqueue();
                    }
                    Ok(())
                }),
            )
            .await?;
        // The account writer needs the same physical Root guards. Await only
        // after the per-session publication job has released those guards.
        if let Some(confirmation) = confirmation {
            if !tokio::time::timeout(std::time::Duration::from_secs(30), confirmation)
                .await
                .ok()
                .and_then(Result::ok)
                .unwrap_or(false)
            {
                // The account writer reports a boolean receipt, so an owner
                // conflict there otherwise loses its typed cause. Revalidate
                // without publishing or writing runtime state before returning the
                // generic receipt failure; a lost owner must still interrupt
                // its exact live transport through the event forwarder.
                if let Err(error) = storage
                    .publish_root_actor_runtime_event(
                        &owner,
                        Box::new(|check_current| check_current()),
                    )
                    .await
                {
                    if error
                        .get_ref()
                        .is_some_and(|cause| cause.is::<bamboo_domain::SessionAuthorityConflict>())
                    {
                        return Err(error);
                    }
                }
                return Err(std::io::Error::other(
                    "Root account final publication was not confirmed",
                ));
            }
        }
        Ok(())
    }
}
