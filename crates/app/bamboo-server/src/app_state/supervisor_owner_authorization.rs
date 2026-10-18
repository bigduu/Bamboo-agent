//! Trusted Host authorization for an exact Project-scope intent.
//!
//! The default Server has no authorizer. This port is installed only by Rust
//! embedding code before serving requests; no HTTP/config/model path installs
//! it or issues its proof. Locality, run/device tokens and ordinary access
//! cookies are not sufficient proof: their HTTP administration supports local
//! bypass. A Host implementation must verify explicit owner consent through a
//! channel outside that control plane and bind it to the complete intent.
use std::sync::Arc;

use actix_web::HttpRequest;
use bamboo_domain::SupervisorManagementRequest;

pub trait SupervisorProjectScopeOwnerAuthorizer: Send + Sync {
    /// Fail closed on missing/invalid proof. Verify all intent fields, not only
    /// caller identity. Do not grant scope or mutate configuration here.
    fn authorize(&self, request: &HttpRequest, intent: &SupervisorManagementRequest) -> bool;
}

impl super::AppState {
    /// Install a Host-owned verifier before wrapping this state for HTTP.
    /// This does not initialize scope or create any credential/permission.
    pub fn with_supervisor_project_scope_authorizer(
        mut self,
        authorizer: Arc<dyn SupervisorProjectScopeOwnerAuthorizer>,
    ) -> Self {
        self.supervisor_project_scope_owner_authorizer = Some(authorizer);
        self
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use actix_web::web;
    use bamboo_domain::{
        SupervisorManagementMutation, SupervisorReference, DEFAULT_SUPERVISOR_SESSION_ID,
    };
    use serde_json::Value;

    /// Test-native owner consent is installed in the trusted service factory,
    /// never translated from an HTTP header, bearer or caller JSON claim.
    pub struct ApprovedHostIntent;
    impl SupervisorProjectScopeOwnerAuthorizer for ApprovedHostIntent {
        fn authorize(&self, request: &HttpRequest, intent: &SupervisorManagementRequest) -> bool {
            request
                .app_data::<web::Data<SupervisorManagementRequest>>()
                .is_some_and(|approved| approved.get_ref() == intent)
        }
    }

    pub fn intent(body: &Value) -> Option<SupervisorManagementRequest> {
        Some(SupervisorManagementRequest {
            supervisor: SupervisorReference {
                session_id: DEFAULT_SUPERVISOR_SESSION_ID.into(),
                incarnation_id: serde_json::from_value(body.get("incarnation_id")?.clone()).ok()?,
            },
            expected_state_revision: body.get("expected_state_revision")?.as_u64()?,
            mutation: SupervisorManagementMutation::ConfigureProjectScope {
                allowed_projects: serde_json::from_value(body.get("allowed_projects")?.clone())
                    .ok()?,
            },
        })
    }
}
