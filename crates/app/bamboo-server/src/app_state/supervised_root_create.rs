//! Scoped Roots share one creation receipt, workspace/Project, candidate and
//! publication flow. Supervisor management and finite Host creation retain
//! distinct final authority and Inbox ports; neither is a second create chain.

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

#[path = "supervised_root_create_authority.rs"]
mod authority;
use authority::CreationAuthority;
pub(super) use authority::HostRootFirstInputPort;

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
    if error.to_string() == "host_session_create_pending_root_absent_requires_recovery" {
        return "creation_recovery_required";
    }
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
            "owner_action":{
                "method":"PUT", "path":"/api/v1/supervisor/default/scope",
                "requires":"existing_access", "model_callable":false,
                "required_fields":["incarnation_id","expected_state_revision","allowed_projects"],
                "next_step":"Using the existing application access mode, the owner checks GET /api/v1/supervisor/default/scope and explicitly submits the complete selected Project set through PUT. Then read scope again and use the selected Project and existing bound workspace with create under its separate tool approval.",
                "note":"The owner must explicitly select the complete existing active Project set using the current incarnation and management revision. Project scope is independent of workspace binding, native tool approval and Work initialization. Never issue this owner action through another tool or auto-grant Projects."
            },
            "work_scope":{
                "availability":"not_observed", "status_path":"/api/v1/tickets/scope",
                "note":"Formal Work has its own identity/mode, feature, binding and health checks. Its owner initialization is permitted only when that endpoint returns can_initialize=true with the current initialization payload. It grants no Project scope or shell/build permission. If an independent implementation route is blocked, report its specific blocker; a read-only Child is not that implementation."
            },
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
        let authority = CreationAuthority::legacy_supervisor(supervisor.clone());
        self.create_authorized_inner(&authority, request).await
    }

    async fn create_authorized_inner(
        &self,
        authority: &CreationAuthority,
        request: SupervisedRootCreateRequest,
    ) -> Result<ToolResult, ToolError> {
        let operation_id = request.operation_id;
        let (digest, reserved_id, fingerprint) = authority.operation_identity(&request)?;
        let state_revision = match authority.validate_request(self, &request).await {
            Ok(revision) => revision,
            Err(error) => {
                return Ok(blocked(
                    operation_id,
                    None,
                    "authorization",
                    error.code,
                    error.message,
                ))
            }
        };
        let _claim = self.operations.lock(&digest).await.map_err(|error| {
            ToolError::Execution(format!("creation claim unavailable: {error}"))
        })?;
        let mut existing = if authority.is_host() {
            // Exact Host operation truth is never expired/pruned by a retry.
            // Its reserved birth must survive process restart and grant rotation.
            self.operations.load_for_status(&digest).await
        } else {
            self.operations.load_claimed(&digest).await
        }
        .map_err(|error| ToolError::Execution(format!("creation receipt unavailable: {error}")))?;
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
        if authority.is_host() {
            if let Some(operation) = existing
                .as_mut()
                .filter(|record| record.expires_at.is_some())
            {
                // A genuine Supervisor can retain the same operation when an
                // explicit Host scope replaces its legacy route. Pin that
                // already validated receipt before any terminal early return.
                operation.expires_at = None;
                self.operations.save(operation).await.map_err(|error| {
                    ToolError::Execution(format!(
                        "Host creation receipt retention unavailable: {error}"
                    ))
                })?;
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
        if !replayed && state_revision != request.expected_state_revision {
            return Ok(blocked(
                operation_id,
                None,
                "authorization",
                "scope_revision_conflict",
                "Creation scope changed; reload scope before a new creation operation",
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
        if let Err(error) = authority.validate_workspace(&request, &workspace) {
            return Ok(blocked(
                operation_id,
                replayed.then_some(id.as_str()),
                "workspace",
                error.code,
                error.message,
            ));
        }
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
        let relation = match authority
            .create_root(self, &request, &candidate, replayed)
            .await
        {
            Ok(relation) => relation,
            Err(error) => {
                return Ok(blocked(
                    operation_id,
                    Some(&id),
                    if authority.is_host() {
                        "creation"
                    } else {
                        "creation_or_link"
                    },
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
                relation.linked,
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
        // A preflight never replaces the concrete port's final held-lock
        // authority. Keep the original caller/token through that handoff.
        if let Err(error) = authority.validate_request(self, &request).await {
            return Ok(blocked(
                operation_id,
                Some(&id),
                "first_input",
                error.code,
                error.message,
            ));
        }
        let result = authority.first_input(self, &request, &candidate).await;
        let (receipt, activation) = match result {
            Ok(receipt) => (
                receipt.delivery,
                match receipt.activation {
                    Some(SessionActivationDisposition::ActiveNotified) => "active_notified",
                    Some(SessionActivationDisposition::ActivationReserved) => "activation_reserved",
                    Some(SessionActivationDisposition::ActivationCoalesced) => {
                        "activation_coalesced"
                    }
                    None => "delivery_not_pending",
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
            if authority.is_host() {
                // Host operation identity is permanent even when a grant or
                // conversation rotates. Never make a missing receipt a new op.
                operation.expires_at = None;
            }
        }
        let mut response = completed(
            &request,
            &id,
            relation.state_revision,
            relation.link_revision,
            relation.linked,
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
    linked: Option<bool>,
    delivery: &SupervisedRootDelivery,
    replayed: bool,
) -> ToolResult {
    let mut response = json!({
        "action":"create", "operation_id":request.operation_id, "target_session_id":id,
        "project_id":request.project_id, "created_durable":true, "linked":linked,
        "state_revision":state_revision, "link_revision":link_revision,
        "admitted":true, "receipt_id":delivery.receipt_id, "generation":delivery.generation,
        "activation":delivery.activation, "replayed":replayed,
        "code":if matches!(delivery.activation.as_str(), "activation_pending" | "activation_retry_required") {
            delivery.activation.as_str()
        } else {"admitted"},
        "note":if delivery.activation == "delivery_not_pending" {
            "The first input has a durable historical admission and is no longer pending. This admission attempt requested no activation. This does not prove consumption, successful execution or task completion. No target rights or management link are inferred."
        } else if linked == Some(true) {
            "Creation, relationship and first input are durable, not task completion. A replay is historical admission, not a new run or current activation status. Retry exactly this request for uncertain activation. No Work/Ticket, isolated worktree, Supervisor context or additional permission is created. Recovery uses the existing 24-hour terminal receipt window."
        } else {
            "Creation and first input are durable, not task completion. No management link or target grant is created. A replay is historical admission, not a new run or current activation status. Retry only the unchanged request with its original operation UUID. The Host reservation remains retained across grant and conversation changes."
        }
    });
    if linked.is_none() {
        response["management_link_changed"] = json!(false);
    }
    if delivery.activation == "delivery_not_pending" {
        response["activation_requested"] = json!(false);
    }
    ToolResult::text(
        delivery.activation != "activation_retry_required",
        response.to_string(),
    )
}

#[cfg(test)]
#[path = "supervised_root_create_contract_tests.rs"]
mod tests;
