//! Trusted Host composition over the existing Creator, registration and queue.
//! Provisioning remains an explicit first phase; this API never allocates a
//! replacement Child or adopts a failed creation merely from its role label.

use super::AppState;
use bamboo_domain::{Storage, SupervisorObserverReceipt, SupervisorReference};
use bamboo_engine::session_app::child_session::ChildSessionPort;
use std::{collections::BTreeSet, io};
use uuid::Uuid;

impl AppState {
    /// Use the production Creator with read_only=true and auto_run=false.
    /// Keep its Child ID before calling register/start. If provisioning ends
    /// before registration, the original draft remains ordinary and must be
    /// explicitly completed or removed by its owner; it gains no exemption.
    pub fn supervisor_observer_creator(
        &self,
    ) -> crate::tools::child_session_adapter::ChildSessionAdapter {
        let mut creator = crate::tools::child_session_adapter::ChildSessionAdapter::new(
            self.session_store.clone(),
            self.storage.clone(),
            self.persistence.clone(),
            self.spawn_scheduler.clone(),
            self.sessions.clone(),
            self.agent_runners.clone(),
            self.session_event_senders.clone(),
            Some(self.session_messenger.clone()),
            None,
            self.config.clone(),
        );
        creator.project_store = Some(self.project_store.clone());
        creator.workspace_resolver = self.workspace_resolver.clone();
        creator
    }

    /// Register one already-provisioned fresh Child, then use the existing
    /// generation-bound queue admission. Repeating the exact operation never
    /// allocates another Child or advances its launch generation. A durable
    /// launch intent is recovered by the existing Child boot reconciler.
    pub async fn start_supervisor_observer(
        &self,
        supervisor: &SupervisorReference,
        expected_state_revision: u64,
        operation_id: Uuid,
        child_id: &str,
        targets: BTreeSet<String>,
    ) -> io::Result<SupervisorObserverReceipt> {
        if !self.spawn_scheduler.supports_native_ticket_budget() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Observer registration requires the assembled native-only backend",
            ));
        }
        let receipt = self
            .session_store
            .register_supervisor_observer(
                supervisor,
                expected_state_revision,
                operation_id,
                child_id,
                targets,
            )
            .await?;
        if !receipt.enabled {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "Observer registration was revoked; retry cannot re-enable it",
            ));
        }
        let parent = self
            .storage
            .load_session(&supervisor.session_id)
            .await?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Observer parent is missing"))?;
        let child = self
            .storage
            .load_child_wait_session(&parent, child_id, true)
            .await?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Observer Child is missing"))?;
        if !self.storage.supervisor_observer_activation(&child).await? {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "Observer registration is unavailable",
            ));
        }
        if child.last_run_status().as_deref() == Some("pending") {
            self.supervisor_observer_creator()
                .enqueue_child_run(&parent, &child)
                .await
                .map_err(|error| io::Error::other(error.to_string()))?;
        }
        Ok(receipt)
    }
}

#[cfg(test)]
#[path = "supervisor_observer_tests.rs"]
mod tests;
