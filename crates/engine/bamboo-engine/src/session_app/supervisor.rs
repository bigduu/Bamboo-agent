//! Host-facing Supervisor identity bootstrap over the canonical Storage port.

use std::{collections::BTreeSet, io, sync::Arc};

use bamboo_domain::{
    ProjectId, Storage, SupervisorBootstrapReceipt, SupervisorLinkObservation,
    SupervisorManagementMutation, SupervisorManagementReceipt, SupervisorManagementRequest,
    SupervisorReference, SupervisorScopeObservation,
};

/// Trusted host service; not a model tool or an automatic Session startup hook.
/// Storage owns atomic publication and identity validation. This service never
/// creates an ordinary Session, changes a cache, or launches a run.
#[derive(Clone)]
pub struct SupervisorSessionService {
    storage: Arc<dyn Storage>,
}

impl SupervisorSessionService {
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self { storage }
    }

    /// Get the stable identity, using `initial_model` only on first creation.
    pub async fn get_or_create_default(
        &self,
        initial_model: &str,
    ) -> io::Result<SupervisorBootstrapReceipt> {
        self.storage
            .get_or_create_default_supervisor(initial_model)
            .await
    }

    /// Read the bounded host scope and current CAS revision. Defaults to empty.
    pub async fn inspect_scope(
        &self,
        supervisor: &SupervisorReference,
    ) -> io::Result<SupervisorScopeObservation> {
        self.storage.inspect_supervisor_scope(supervisor).await
    }

    /// Replace trusted Project scope. Removal disables associated links;
    /// subsequent regrant never silently enables those tombstones.
    pub async fn configure_project_scope(
        &self,
        supervisor: &SupervisorReference,
        expected_state_revision: u64,
        allowed_projects: BTreeSet<ProjectId>,
    ) -> io::Result<SupervisorManagementReceipt> {
        self.mutate(
            supervisor,
            expected_state_revision,
            SupervisorManagementMutation::ConfigureProjectScope { allowed_projects },
        )
        .await
    }

    /// Attach/revalidate an existing Ordinary Root in configured Project scope.
    /// Storage captures its exact current birth and metadata revision atomically.
    pub async fn attach(
        &self,
        supervisor: &SupervisorReference,
        expected_state_revision: u64,
        target_session_id: &str,
    ) -> io::Result<SupervisorManagementReceipt> {
        self.mutate(
            supervisor,
            expected_state_revision,
            SupervisorManagementMutation::Attach {
                target_session_id: target_session_id.to_string(),
            },
        )
        .await
    }

    /// Disable a stored link even if the target is now absent or damaged.
    pub async fn detach(
        &self,
        supervisor: &SupervisorReference,
        expected_state_revision: u64,
        target_session_id: &str,
    ) -> io::Result<SupervisorManagementReceipt> {
        self.mutate(
            supervisor,
            expected_state_revision,
            SupervisorManagementMutation::Detach {
                target_session_id: target_session_id.to_string(),
            },
        )
        .await
    }

    /// Strict single-link observation. A later command must validate again at
    /// durable admission; this returned value carries no retained lock/grant.
    pub async fn inspect_link(
        &self,
        supervisor: &SupervisorReference,
        target_session_id: &str,
    ) -> io::Result<SupervisorLinkObservation> {
        self.storage
            .inspect_supervisor_link(supervisor, target_session_id)
            .await
    }

    async fn mutate(
        &self,
        supervisor: &SupervisorReference,
        expected_state_revision: u64,
        mutation: SupervisorManagementMutation,
    ) -> io::Result<SupervisorManagementReceipt> {
        self.storage
            .mutate_supervisor_management(&SupervisorManagementRequest {
                supervisor: supervisor.clone(),
                expected_state_revision,
                mutation,
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::Session;

    struct UnsupportedStore;

    #[async_trait::async_trait]
    impl Storage for UnsupportedStore {
        async fn save_session(&self, _: &Session) -> io::Result<()> {
            panic!("bootstrap must not fall back to ordinary save")
        }
        async fn load_session(&self, _: &str) -> io::Result<Option<Session>> {
            panic!("authority must not fall back to ordinary load")
        }
        async fn delete_session(&self, _: &str) -> io::Result<bool> {
            panic!("bootstrap must not delete sessions")
        }
    }

    #[tokio::test]
    async fn unsupported_authority_ports_fail_without_ordinary_fallback() {
        let storage: Arc<dyn Storage> = Arc::new(UnsupportedStore);
        let service = SupervisorSessionService::new(storage.clone());
        assert_eq!(
            service
                .get_or_create_default("model")
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::Unsupported
        );
        assert_eq!(
            storage
                .load_root_authority("root")
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::Unsupported
        );
        let supervisor = SupervisorReference {
            session_id: bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID.into(),
            incarnation_id: uuid::Uuid::new_v4(),
        };
        assert_eq!(
            service.inspect_scope(&supervisor).await.unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
        assert_eq!(
            service
                .configure_project_scope(&supervisor, 0, BTreeSet::new())
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::Unsupported
        );
        assert_eq!(
            service
                .attach(&supervisor, 0, "target")
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::Unsupported
        );
        assert_eq!(
            service
                .detach(&supervisor, 0, "target")
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::Unsupported
        );
        assert_eq!(
            service
                .inspect_link(&supervisor, "target")
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::Unsupported
        );
    }
}
