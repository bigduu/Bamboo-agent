//! New supervised Roots compose the existing creation receipt, canonical
//! management writer and lifetime-fenced Inbox. There is no second queue,
//! relationship registry or persistent execution grant.

use std::sync::Arc;

use async_trait::async_trait;
use bamboo_agent_core::tools::{ToolError, ToolResult};
use bamboo_domain::{Session, SessionActivationDisposition, Storage, SupervisorReference};
use bamboo_engine::{SessionMessenger, SessionMessengerError, SessionRepository};
use bamboo_server_tools::session_control::root_creation::{
    SupervisedRootCreatePort, SupervisedRootCreateRequest,
};
use serde_json::{json, Value};
use tokio::sync::RwLock;

use super::session_create_operations::{
    key_digest, payload_fingerprint, SessionCreateOperationRecord, SessionCreateOperationStore,
    StoredOperationStatus, SupervisedRootDelivery,
};

#[derive(Clone)]
pub(super) struct SupervisedRootCreator {
    pub store: Arc<bamboo_storage::SessionStoreV2>,
    pub operations: Arc<SessionCreateOperationStore>,
    pub projects: Arc<bamboo_projects::ProjectStore>,
    pub workspace: bamboo_agent_core::workspace_state::WorkspaceResolver,
    pub project_context: Arc<bamboo_engine::project_context::ProjectContextResolver>,
    pub config: Arc<RwLock<bamboo_llm::Config>>,
    pub repo: SessionRepository,
    pub messenger: Arc<SessionMessenger>,
    pub account: Arc<bamboo_engine::events::AccountEventSink>,
}

fn blocked(
    operation_id: uuid::Uuid,
    target: Option<&str>,
    stage: &str,
    code: &str,
    message: impl ToString,
) -> ToolResult {
    ToolResult::text(false, json!({
        "action":"create", "operation_id":operation_id,
        "target_session_id":target, "stage":stage, "code":code,
        "message":message.to_string(), "admitted":null,
        "retry":"Inspect this exact operation/target; retry the complete unchanged request after resolving the blocker. Do not allocate a second worker to recover an uncertain creation."
    }).to_string())
}

fn workspace_code(error: &crate::project_context::SessionWorkspaceValidationError) -> &'static str {
    use crate::project_context::SessionWorkspaceValidationError;
    match error {
        SessionWorkspaceValidationError::Invalid { .. } => "workspace_invalid",
        SessionWorkspaceValidationError::Conflict { .. } => "project_workspace_conflict",
        SessionWorkspaceValidationError::Unbound { .. } => "project_workspace_unbound",
        SessionWorkspaceValidationError::ProjectArchived { .. } => "project_archived",
        SessionWorkspaceValidationError::ProjectUnavailable { .. } => "project_unavailable",
        SessionWorkspaceValidationError::Store(_) => "project_store_unavailable",
    }
}

fn io_code(error: &std::io::Error) -> &'static str {
    match error.kind() {
        std::io::ErrorKind::PermissionDenied => "scope_denied",
        std::io::ErrorKind::WouldBlock => "scope_revision_conflict",
        std::io::ErrorKind::InvalidInput | std::io::ErrorKind::InvalidData => "authority_invalid",
        _ => "storage_unavailable",
    }
}

#[async_trait]
impl SupervisedRootCreatePort for SupervisedRootCreator {
    async fn scope(&self, supervisor: &SupervisorReference) -> Result<ToolResult, ToolError> {
        let scope = self
            .store
            .inspect_supervisor_scope(supervisor)
            .await
            .map_err(|error| ToolError::Execution(format!("{}: {error}", io_code(&error))))?;
        Ok(ToolResult::text(true, json!({
            "action":"scope", "supervisor":scope.supervisor,
            "state_revision":scope.state_revision, "allowed_projects":scope.allowed_projects,
            "create_route_exposed":true, "project_scope_configured":!scope.allowed_projects.is_empty(),
            "code":if scope.allowed_projects.is_empty() {"project_scope_uninitialized"} else {"project_scope_configured"},
            "note":"Only these existing Host-authorized Projects may receive a new Ordinary Root. The workspace must already be bound to the explicit Project. This observation is not a grant and never initializes Work, changes permissions or attaches historical Sessions."
        }).to_string()))
    }

    async fn create(
        &self,
        supervisor: &SupervisorReference,
        request: SupervisedRootCreateRequest,
    ) -> Result<ToolResult, ToolError> {
        request.validate()?;
        // A response timeout or cancelled originating tool future must not drop
        // the operation claim between the three existing durable boundaries.
        let service = self.clone();
        let supervisor = supervisor.clone();
        tokio::spawn(async move { service.create_inner(&supervisor, request).await })
            .await
            .map_err(|error| {
                ToolError::Execution(format!("supervised Root creation task failed: {error}"))
            })?
    }
}

impl SupervisedRootCreator {
    async fn create_inner(
        &self,
        supervisor: &SupervisorReference,
        request: SupervisedRootCreateRequest,
    ) -> Result<ToolResult, ToolError> {
        let operation_id = request.operation_id;
        // Domain separation uses NUL, which a normal HTTP Idempotency-Key
        // cannot contain. No raw key, instruction or private context is stored.
        let digest = key_digest(&format!(
            "supervised-root:v1\0{}\0{}\0{}",
            supervisor.session_id, supervisor.incarnation_id, operation_id
        ));
        // The UUID is also bound to the source lifetime + operation digest.
        // A damaged outer receipt cannot select an unrelated historical Root,
        // and expiry can never allocate a second UUID for the same operation.
        let reserved_id = uuid::Uuid::parse_str(&digest[..32])
            .expect("SHA-256 prefix is a simple UUID")
            .to_string();
        let fingerprint = payload_fingerprint(&(supervisor, &request))
            .map_err(|error| ToolError::InvalidArguments(error.to_string()))?;
        let scope = match self.store.inspect_supervisor_scope(supervisor).await {
            Ok(scope) => scope,
            Err(error) => {
                return Ok(blocked(
                    operation_id,
                    None,
                    "authorization",
                    io_code(&error),
                    error,
                ))
            }
        };
        if !scope.allowed_projects.contains(&request.project_id) {
            return Ok(blocked(
                operation_id,
                None,
                "authorization",
                "scope_denied",
                "Project is outside the current explicit Supervisor scope",
            ));
        }
        let _claim = self.operations.lock(&digest).await.map_err(|error| {
            ToolError::Execution(format!("creation claim unavailable: {error}"))
        })?;
        let existing = self
            .operations
            .load_claimed(&digest)
            .await
            .map_err(|error| {
                ToolError::Execution(format!("creation receipt unavailable: {error}"))
            })?;
        if let Some(existing) = &existing {
            if existing.session_id != reserved_id {
                return Ok(blocked(
                    operation_id,
                    None,
                    "reservation",
                    "operation_identity_conflict",
                    "Creation receipt does not match its reserved operation UUID",
                ));
            }
            if existing.payload_fingerprint != fingerprint {
                return Ok(blocked(
                    operation_id,
                    Some(&existing.session_id),
                    "reservation",
                    "idempotency_conflict",
                    "The operation UUID is already bound to a different request",
                ));
            }
        }
        if let Some(operation) = existing
            .as_ref()
            .filter(|operation| operation.status == StoredOperationStatus::Failed)
        {
            return Ok(blocked(operation_id, Some(&operation.session_id), "receipt", "creation_failed",
                "This creation has a terminal failure receipt; inspect it before starting a distinct operation"));
        }
        let replayed = existing.is_some();
        if !replayed && scope.state_revision != request.expected_state_revision {
            return Ok(blocked(
                operation_id,
                None,
                "authorization",
                "scope_revision_conflict",
                "Supervisor management changed; reload scope before a new creation operation",
            ));
        }
        let mut operation = existing.unwrap_or_else(|| {
            SessionCreateOperationRecord::pending(digest, fingerprint, reserved_id)
        });
        let id = operation.session_id.clone();
        let workspace =
            match crate::project_context::validate_explicit_session_workspace_with_resolver(
                &self.projects,
                Some(&request.project_id),
                &request.workspace_path,
                &self.workspace,
            ) {
                Ok(workspace) => workspace,
                Err(error) => {
                    return Ok(blocked(
                        operation_id,
                        replayed.then_some(id.as_str()),
                        "workspace",
                        workspace_code(&error),
                        error,
                    ))
                }
            };
        let mut candidate = self.build_candidate(&operation, &request, &workspace).await;
        if let Err(error) = self
            .project_context
            .refresh_session_prompt_read_only(&mut candidate)
            .await
        {
            return Ok(blocked(
                operation_id,
                replayed.then_some(id.as_str()),
                "workspace",
                "project_context_unavailable",
                error,
            ));
        }
        if !replayed {
            // Reserve the same UUID and immutable birth before any Root/link
            // side effect, reusing the normal creation operation store/claim.
            self.operations.save(&operation).await.map_err(|error| {
                ToolError::Execution(format!("creation reservation unavailable: {error}"))
            })?;
        }
        let relation = match self
            .store
            .create_and_attach_supervised_root(
                supervisor,
                request.expected_state_revision,
                &candidate,
            )
            .await
        {
            Ok(relation) => relation,
            Err(error) => {
                return Ok(blocked(
                    operation_id,
                    Some(&id),
                    "creation_or_link",
                    io_code(&error),
                    error,
                ))
            }
        };
        if operation.status == StoredOperationStatus::Succeeded {
            let Some(delivery) = operation.supervised_delivery.as_ref() else {
                return Ok(blocked(
                    operation_id,
                    Some(&id),
                    "receipt",
                    "authority_invalid",
                    "Completed creation has no first-input receipt",
                ));
            };
            return Ok(completed(
                &request,
                &id,
                relation.state_revision,
                relation.link_revision,
                delivery,
                true,
            ));
        }
        // Repair only rebuildable projections, then merge through the existing
        // no-regression repository. Never replace an active cache snapshot.
        if let Err(error) = self.publish_created(&id).await {
            return Ok(blocked(
                operation_id,
                Some(&id),
                "creation_projection",
                "projection_pending",
                error,
            ));
        }
        let result = self
            .messenger
            .supervisor_followup(supervisor, &id, &operation_id.to_string(), &request.message)
            .await;
        let (receipt, activation) = match result {
            Ok(receipt) => (
                receipt.delivery,
                match receipt.activation {
                    SessionActivationDisposition::ActiveNotified => "active_notified",
                    SessionActivationDisposition::ActivationReserved => "activation_reserved",
                    SessionActivationDisposition::ActivationCoalesced => "activation_coalesced",
                },
            ),
            Err(SessionMessengerError::Activation { receipt, .. }) => {
                (receipt, "activation_pending")
            }
            Err(SessionMessengerError::ActivationEligibility { receipt, .. }) => {
                (receipt, "activation_retry_required")
            }
            Err(error) => {
                return Ok(blocked(
                    operation_id,
                    Some(&id),
                    "first_input",
                    "first_input_pending",
                    error,
                ))
            }
        };
        let delivery = SupervisedRootDelivery {
            receipt_id: receipt.id.to_string(),
            generation: receipt.generation,
            activation: activation.into(),
        };
        operation.supervised_delivery = Some(delivery.clone());
        if !matches!(
            activation,
            "activation_pending" | "activation_retry_required"
        ) {
            operation.mark_succeeded();
        }
        let mut response = completed(
            &request,
            &id,
            relation.state_revision,
            relation.link_revision,
            &delivery,
            replayed,
        );
        if self.operations.save(&operation).await.is_err() {
            // Admission is known durable even if this outer projection failed.
            let mut value: Value =
                serde_json::from_str(&response.result).expect("local creation receipt JSON");
            value["code"] = json!("receipt_pending");
            value["note"] = json!("First input is durable. Retry the complete unchanged request to reconcile this creation receipt; do not create another worker.");
            response.result = value.to_string();
        }
        Ok(response)
    }

    async fn build_candidate(
        &self,
        operation: &SessionCreateOperationRecord,
        request: &SupervisedRootCreateRequest,
        workspace: &std::path::Path,
    ) -> Session {
        use bamboo_engine::session_app::session_create::{
            build_new_session, CreateSessionConfig, CreateSessionInput,
        };
        let config = self.config.read().await;
        let mut candidate = build_new_session(
            &CreateSessionInput {
                id: operation.session_id.clone(),
                project_id: Some(request.project_id.clone()),
                title: request.title.clone(),
                title_generated: None,
                system_prompt: None,
                model: None,
                model_ref: None,
                reasoning_effort: None,
                gold_config_json: None,
                workspace_path: Some(bamboo_config::paths::path_to_display_string(workspace)),
                permission_mode: Some(bamboo_domain::SessionPermissionMode::Default),
            },
            &CreateSessionConfig {
                default_model: config.get_model(),
                default_reasoning_effort: config.get_reasoning_effort(),
                global_default_prompt:
                    bamboo_engine::prompt_defaults::read_global_default_system_prompt_template(),
                builtin_fallback_prompt: super::DEFAULT_BASE_PROMPT,
            },
        );
        // The existing reservation supplies an immutable retry birth as well
        // as the UUID. A recreated or unrelated lifetime can never match it.
        candidate.created_at = operation.created_at;
        candidate.updated_at = operation.created_at;
        candidate
    }

    async fn publish_created(&self, id: &str) -> std::io::Result<()> {
        self.store
            .recover_root_session_from_disk(id)
            .await?
            .ok_or_else(|| std::io::Error::other("reserved Root is unavailable"))?;
        let session = self
            .repo
            .load_merged_checked(id)
            .await?
            .ok_or_else(|| std::io::Error::other("reserved Root is unavailable"))?;
        let workspace = session
            .workspace_path_meta()
            .ok_or_else(|| std::io::Error::other("reserved Root has no workspace"))?;
        self.workspace
            .publish_resolved_workspace(id, workspace.into(), "explicit");
        let event = bamboo_agent_core::AgentEvent::SessionCreated {
            session_id: id.into(),
            project_id: session.project_id_meta(),
            title: session.title.clone(),
            kind: session.kind,
            created_at: session.created_at,
        };
        if !self.account.record_confirmed(Some(id), &event).await {
            return Err(std::io::Error::other(
                "SessionCreated publication is unavailable",
            ));
        }
        Ok(())
    }
}

fn completed(
    request: &SupervisedRootCreateRequest,
    id: &str,
    state_revision: u64,
    link_revision: Option<u64>,
    delivery: &SupervisedRootDelivery,
    replayed: bool,
) -> ToolResult {
    ToolResult::text(delivery.activation != "activation_retry_required", json!({
        "action":"create", "operation_id":request.operation_id, "target_session_id":id,
        "project_id":request.project_id, "created_durable":true, "linked":true,
        "state_revision":state_revision, "link_revision":link_revision,
        "admitted":true, "receipt_id":delivery.receipt_id, "generation":delivery.generation,
        "activation":delivery.activation, "replayed":replayed,
        "code":if matches!(delivery.activation.as_str(), "activation_pending" | "activation_retry_required") {
            delivery.activation.as_str()
        } else {"admitted"},
        "note":"Creation, relationship and first input are durable, not task completion. A replay is historical admission, not a new run or current activation status. Retry exactly this request for uncertain activation. No Work/Ticket, isolated worktree, Supervisor context or additional permission is created. Recovery uses the existing 24-hour terminal receipt window."
    }).to_string())
}

#[cfg(test)]
#[path = "supervised_root_create_contract_tests.rs"]
mod tests;
