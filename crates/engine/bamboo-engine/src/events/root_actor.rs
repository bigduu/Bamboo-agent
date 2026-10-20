//! One immutable Root execution's actual per-session/account publication.

use super::AccountEventSink;
use bamboo_agent_core::AgentEvent;
use bamboo_domain::{RootActorRuntimeWrite, Storage};
use std::sync::Arc;

#[derive(Clone)]
pub struct RootActorEventPublication {
    storage: Arc<dyn Storage>,
    account_sink: Arc<AccountEventSink>,
    owner: RootActorRuntimeWrite,
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

    pub async fn publish(
        &self,
        session_id: &str,
        event: &AgentEvent,
        publish: Box<dyn FnOnce() -> bool + Send>,
    ) -> std::io::Result<()> {
        let storage = self.storage.clone();
        let sink = self.account_sink.clone();
        let owner_for_queue = self.owner.clone();
        let event = event.clone();
        let session_id = session_id.to_owned();
        let (queued, receipt) = tokio::sync::oneshot::channel();
        self.storage
            .publish_root_actor_runtime_event(
                &self.owner,
                Box::new(move |check_current| {
                    check_current()?;
                    if !publish() {
                        return Err(std::io::Error::other(
                            "Root per-session event publication was retired",
                        ));
                    }
                    check_current()?;
                    let confirmation = if event.is_durable_change() {
                        Some(
                            sink.record_root_actor(
                                storage,
                                owner_for_queue,
                                Some(&session_id),
                                &event,
                            )
                            .ok_or_else(|| {
                                std::io::Error::other(
                                    "Root account event queue rejected publication",
                                )
                            })?,
                        )
                    } else {
                        None
                    };
                    let _ = queued.send(confirmation);
                    Ok(())
                }),
            )
            .await?;
        // The account writer needs the same physical Root guards. Await only
        // after the per-session publication job has released those guards.
        let confirmation = receipt
            .await
            .map_err(|_| std::io::Error::other("Root account admission receipt closed"))?;
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
                if let Err(error) = self
                    .storage
                    .publish_root_actor_runtime_event(
                        &self.owner,
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
