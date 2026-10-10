//! Stateless adapter for the existing typed Instruction selection and commit.
//! It owns no caller grant, writer, or alternate activation protocol.

use crate::error::ResponseResult;

use super::{
    persist_and_cache_session_locked, project_context_error_response, publish_committed_chat,
};
use crate::app_state::AppState;
use actix_web::{web, HttpResponse};

fn workflow_selection_error_response(
    diagnostic: bamboo_skills::WorkflowActivationDiagnostic,
) -> HttpResponse {
    use bamboo_skills::WorkflowActivationErrorCode;

    let (status, code) = match diagnostic.code {
        WorkflowActivationErrorCode::RevisionMissing => (
            actix_web::http::StatusCode::CONFLICT,
            "workflow_revision_missing",
        ),
        WorkflowActivationErrorCode::RevisionMismatch => (
            actix_web::http::StatusCode::CONFLICT,
            "workflow_revision_mismatch",
        ),
        WorkflowActivationErrorCode::SourceMismatch => (
            actix_web::http::StatusCode::CONFLICT,
            "workflow_source_mismatch",
        ),
        WorkflowActivationErrorCode::ManualOnly => (
            actix_web::http::StatusCode::UNPROCESSABLE_ENTITY,
            "workflow_manual_only",
        ),
        WorkflowActivationErrorCode::InvalidSelection => (
            actix_web::http::StatusCode::UNPROCESSABLE_ENTITY,
            "workflow_selection_invalid",
        ),
        WorkflowActivationErrorCode::SnapshotUnavailable => (
            actix_web::http::StatusCode::SERVICE_UNAVAILABLE,
            "workflow_snapshot_unavailable",
        ),
        WorkflowActivationErrorCode::SnapshotTooLarge => (
            actix_web::http::StatusCode::PAYLOAD_TOO_LARGE,
            "workflow_snapshot_too_large",
        ),
        WorkflowActivationErrorCode::ProviderFailed
        | WorkflowActivationErrorCode::ProviderOutputInvalid => (
            actix_web::http::StatusCode::UNPROCESSABLE_ENTITY,
            "workflow_context_invalid",
        ),
    };
    HttpResponse::build(status).json(serde_json::json!({
        "error": {
            "type": "api_error",
            "code": code,
            "message": diagnostic.message,
            "recoverable": diagnostic.recoverable
        }
    }))
}

fn workflow_catalog_unavailable_response(error: &bamboo_skills::SkillError) -> HttpResponse {
    tracing::error!(%error, "failed to pin typed workflow catalog revision");
    workflow_selection_error_response(bamboo_skills::WorkflowActivationDiagnostic {
        code: bamboo_skills::WorkflowActivationErrorCode::SnapshotUnavailable,
        message: "Workflow catalog is temporarily unavailable; retry the request".to_string(),
        recoverable: true,
    })
}

pub(super) async fn pin_explicit_workflow_candidate(
    state: &AppState,
    session: &mut bamboo_agent_core::Session,
    selection: &bamboo_skills::WorkflowSelection,
    disabled_skill_ids: &std::collections::BTreeSet<String>,
) -> ResponseResult<String> {
    let selected_ids = [selection.id.clone()];
    // Resolve into an isolated staging activation. A stale/invalid request must
    // never replace or release the activation currently serving this session.
    // The staged bytes become durable authority only after the session save;
    // execute then restores them under the canonical session id.
    let staging_activation_id = format!("{}:chat-candidate:{}", session.id, uuid::Uuid::new_v4());
    let workspace = session.workspace_path_meta().map(std::path::PathBuf::from);
    let resolved_project = state
        .project_context_resolver
        .resolve(session, workspace.as_deref())
        .await
        .map_err(project_context_error_response)?;
    let (store, activation) = if let Some(context) = resolved_project {
        let store = state
            .skill_manager
            .store_for_project_workspace(
                &context.project.id,
                &context.project.home,
                context.workspace.as_deref(),
            )
            .await
            .map_err(|error| workflow_catalog_unavailable_response(&error))?;
        let activation = state
            .skill_manager
            .resolve_and_pin_activation_in_project_workspace_with_mode_and_budget(
                &context.project.id,
                &context.project.home,
                context.workspace.as_deref(),
                &staging_activation_id,
                disabled_skill_ids,
                Some(&selected_ids),
                None,
                None,
                bamboo_skills::DEFAULT_WORKFLOW_CATALOG_CONTEXT_TOKENS,
            )
            .await;
        (store, activation)
    } else if let Some(workspace) = workspace.as_deref() {
        // A session-scoped fallback is previewed without filesystem mutation.
        // Materialize it before opening the workspace catalog, but do not
        // publish it into runtime state until the base session checkpoint is
        // durable below. The resolver refuses to recreate missing paths
        // outside its authoritative root.
        state
            .workspace_resolver
            .materialize_resolved_workspace(workspace)
            .map_err(|error| {
                workflow_catalog_unavailable_response(&bamboo_skills::SkillError::Io(error))
            })?;
        let store = state
            .skill_manager
            .store_for_workspace(Some(workspace))
            .await
            .map_err(|error| workflow_catalog_unavailable_response(&error))?;
        let activation = state
            .skill_manager
            .resolve_and_pin_activation_in_workspace_with_mode_and_budget(
                workspace,
                &staging_activation_id,
                disabled_skill_ids,
                Some(&selected_ids),
                None,
                None,
                bamboo_skills::DEFAULT_WORKFLOW_CATALOG_CONTEXT_TOKENS,
            )
            .await;
        (store, activation)
    } else {
        let store = state
            .skill_manager
            .store_for_workspace(None)
            .await
            .map_err(|error| workflow_catalog_unavailable_response(&error))?;
        let activation = state
            .skill_manager
            .resolve_and_pin_activation_for_request_with_mode_and_budget(
                &staging_activation_id,
                disabled_skill_ids,
                Some(&selected_ids),
                None,
                None,
                bamboo_skills::DEFAULT_WORKFLOW_CATALOG_CONTEXT_TOKENS,
            )
            .await;
        (store, activation)
    };
    let activation = match activation {
        Ok(activation) => activation,
        Err(error) => {
            let _ = state
                .skill_manager
                .release_activation_for_workspace(&staging_activation_id, workspace.as_deref())
                .await;
            return Err(workflow_catalog_unavailable_response(&error).into());
        }
    };
    let snapshot = match store
        .export_activation_snapshot(&staging_activation_id)
        .await
    {
        Some(snapshot) => snapshot,
        None => {
            let _ = state
                .skill_manager
                .release_activation_for_workspace(&staging_activation_id, workspace.as_deref())
                .await;
            return Err(workflow_selection_error_response(
                bamboo_skills::WorkflowActivationDiagnostic {
                    code: bamboo_skills::WorkflowActivationErrorCode::SnapshotUnavailable,
                    message: "selected workflow snapshot could not be retained".to_string(),
                    recoverable: true,
                },
            )
            .into());
        }
    };
    if let Err(diagnostic) = bamboo_skills::persist_explicit_workflow_candidate(
        &mut session.metadata,
        selection,
        &activation,
        &snapshot,
    ) {
        let _ = state
            .skill_manager
            .release_activation_for_workspace(&staging_activation_id, workspace.as_deref())
            .await;
        return Err(workflow_selection_error_response(diagnostic).into());
    }
    Ok(staging_activation_id)
}

pub(super) struct StagedWorkflowActivation {
    activation_id: String,
    metadata_upserts: Vec<(String, String)>,
    metadata_removals: Vec<String>,
    skill_manager: std::sync::Arc<bamboo_skills::SkillManager>,
    cleanup_armed: bool,
}

impl Drop for StagedWorkflowActivation {
    fn drop(&mut self) {
        if !self.cleanup_armed {
            return;
        }
        let skill_manager = self.skill_manager.clone();
        let activation_id = self.activation_id.clone();
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                let _cleanup = handle.spawn(async move {
                    if let Err(error) = skill_manager
                        .release_activation_for_workspace(&activation_id, None)
                        .await
                    {
                        tracing::error!(
                            %activation_id,
                            %error,
                            "failed to release abandoned staged Workflow activation"
                        );
                    }
                });
            }
            Err(error) => {
                tracing::error!(
                    activation_id = %self.activation_id,
                    %error,
                    "runtime unavailable while releasing staged Workflow activation"
                );
            }
        }
    }
}

#[derive(Default)]
pub(super) struct WorkflowMetadataCheckpoint {
    entries: std::collections::HashMap<String, String>,
}

impl WorkflowMetadataCheckpoint {
    pub(super) fn capture(session: Option<&bamboo_agent_core::Session>) -> Self {
        let entries = session
            .into_iter()
            .flat_map(|session| session.metadata.iter())
            .filter(|(key, _)| workflow_transaction_metadata_key(key))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        Self { entries }
    }

    pub(super) fn restore(&self, session: &mut bamboo_agent_core::Session) {
        session
            .metadata
            .retain(|key, _| !workflow_transaction_metadata_key(key));
        session.metadata.extend(self.entries.clone());
    }
}

fn workflow_transaction_metadata_key(key: &str) -> bool {
    key.starts_with("workflow.")
        || key.starts_with("skill_runtime_")
        || matches!(key, "selected_skill_ids" | "skill_mode")
}

pub(super) fn workflow_runner_is_active(runner: Option<&crate::app_state::AgentRunner>) -> bool {
    runner.is_some_and(|runner| {
        matches!(
            runner.status,
            crate::app_state::AgentStatus::Pending | crate::app_state::AgentStatus::Running
        )
    })
}

pub(super) fn workflow_activation_running_conflict_response(session_id: &str) -> HttpResponse {
    HttpResponse::Conflict().json(serde_json::json!({
        "error": {
            "type": "api_error",
            "code": "workflow_activation_running_conflict",
            "message": "A running or starting session cannot replace its active Workflow"
        },
        "session_id": session_id,
    }))
}

#[cfg(test)]
pub(super) struct WorkflowCommitTestBarrier {
    pub(super) reached: tokio::sync::Semaphore,
    pub(super) resume: tokio::sync::Semaphore,
}

#[cfg(test)]
impl Default for WorkflowCommitTestBarrier {
    fn default() -> Self {
        Self {
            reached: tokio::sync::Semaphore::new(0),
            resume: tokio::sync::Semaphore::new(0),
        }
    }
}

#[cfg(test)]
static WORKFLOW_COMMIT_TEST_BARRIERS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<String, std::sync::Arc<WorkflowCommitTestBarrier>>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

#[cfg(test)]
static WORKFLOW_POST_SAVE_TEST_BARRIERS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<String, std::sync::Arc<WorkflowCommitTestBarrier>>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

#[cfg(test)]
pub(super) fn install_workflow_commit_test_barrier(
    session_id: &str,
) -> std::sync::Arc<WorkflowCommitTestBarrier> {
    let barrier = std::sync::Arc::new(WorkflowCommitTestBarrier::default());
    WORKFLOW_COMMIT_TEST_BARRIERS
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .insert(session_id.to_string(), barrier.clone());
    barrier
}

#[cfg(test)]
pub(super) async fn wait_at_workflow_commit_test_barrier(session_id: &str) {
    let barrier = WORKFLOW_COMMIT_TEST_BARRIERS
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .remove(session_id);
    if let Some(barrier) = barrier {
        barrier.reached.add_permits(1);
        barrier
            .resume
            .acquire()
            .await
            .expect("workflow commit test barrier remains open")
            .forget();
    }
}

#[cfg(test)]
pub(super) fn install_workflow_post_save_test_barrier(
    session_id: &str,
) -> std::sync::Arc<WorkflowCommitTestBarrier> {
    let barrier = std::sync::Arc::new(WorkflowCommitTestBarrier::default());
    WORKFLOW_POST_SAVE_TEST_BARRIERS
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .insert(session_id.to_string(), barrier.clone());
    barrier
}

#[cfg(test)]
async fn wait_at_workflow_post_save_test_barrier(session_id: &str) {
    let barrier = WORKFLOW_POST_SAVE_TEST_BARRIERS
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .remove(session_id);
    if let Some(barrier) = barrier {
        barrier.reached.add_permits(1);
        barrier
            .resume
            .acquire()
            .await
            .expect("workflow post-save test barrier remains open")
            .forget();
    }
}

impl StagedWorkflowActivation {
    fn between(
        activation_id: String,
        current: &std::collections::HashMap<String, String>,
        candidate: &std::collections::HashMap<String, String>,
        skill_manager: std::sync::Arc<bamboo_skills::SkillManager>,
    ) -> Self {
        let metadata_upserts = candidate
            .iter()
            .filter(|(key, value)| {
                workflow_transaction_metadata_key(key) && current.get(*key) != Some(*value)
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let metadata_removals = current
            .keys()
            .filter(|key| workflow_transaction_metadata_key(key) && !candidate.contains_key(*key))
            .cloned()
            .collect();
        Self {
            activation_id,
            metadata_upserts,
            metadata_removals,
            skill_manager,
            cleanup_armed: true,
        }
    }

    pub(super) fn apply(&self, metadata: &mut std::collections::HashMap<String, String>) {
        for key in &self.metadata_removals {
            metadata.remove(key);
        }
        for (key, value) in &self.metadata_upserts {
            metadata.insert(key.clone(), value.clone());
        }
    }

    pub(super) async fn release(&mut self) {
        if !self.cleanup_armed {
            return;
        }
        match self
            .skill_manager
            .release_activation_for_workspace(&self.activation_id, None)
            .await
        {
            Ok(()) => self.cleanup_armed = false,
            Err(error) => tracing::error!(
                activation_id = %self.activation_id,
                %error,
                "failed to release staged Workflow activation"
            ),
        }
    }
}

pub(super) async fn stage_selection(
    state: &AppState,
    session: &bamboo_agent_core::Session,
    selection: Option<&bamboo_skills::WorkflowSelection>,
    selected_skill_ids: Option<&[String]>,
    message: &str,
    config_snapshot: &bamboo_config::Config,
) -> ResponseResult<Option<StagedWorkflowActivation>> {
    Ok(if let Some(selection) = selection {
        let mut candidate = session.clone();
        if let Err(error) = bamboo_engine::session_app::chat::resolve_workflow_selection(
            &mut candidate,
            Some(selection),
            selected_skill_ids,
            message,
        ) {
            return Err(HttpResponse::BadRequest()
                .json(serde_json::json!({
                    "error": crate::error::error_value(error.to_string())
                }))
                .into());
        }
        let disabled_skill_ids = config_snapshot.disabled_skill_ids();
        let staging_id = match pin_explicit_workflow_candidate(
            state,
            &mut candidate,
            selection,
            &disabled_skill_ids,
        )
        .await
        {
            Ok(staging_id) => staging_id,
            Err(response) => return Err(response),
        };
        Some(StagedWorkflowActivation::between(
            staging_id,
            &session.metadata,
            &candidate.metadata,
            state.skill_manager.clone(),
        ))
    } else {
        None
    })
}

pub(super) async fn commit_selected_input(
    state: web::Data<AppState>,
    session: bamboo_agent_core::Session,
    session_id: String,
    mut staging: Option<StagedWorkflowActivation>,
    workflow_changed: bool,
    queued_input: Option<bamboo_domain::SessionMessageEnvelope>,
    queued: bool,
    persistence_guard: bamboo_storage::session_merge::SessionLockGuard,
    workflow_commit_guard: Option<
        tokio::sync::OwnedRwLockReadGuard<
            std::collections::HashMap<String, crate::app_state::AgentRunner>,
        >,
    >,
) -> ResponseResult<()> {
    let commit_state = state;
    let commit_session_id = session_id;
    let commit = tokio::spawn(async move {
        // Keep final save -> old pin release cancellation-resistant for
        // replacement, retirement, and the first-chat Root switch.
        let _persistence_guard = persistence_guard;
        let _workflow_commit_guard = workflow_commit_guard;
        if let Err(error) = persist_and_cache_session_locked(commit_state.as_ref(), &session).await
        {
            if let Some(staging) = staging.as_mut() {
                staging.release().await;
            }
            return Err(error.to_string());
        }
        let admission = if let Some(envelope) = queued_input {
            match commit_state
                .session_messenger
                .admit_with_activation_intent(
                    envelope,
                    bamboo_domain::SessionActivationPolicy::InterruptSpecificWait,
                    None,
                )
                .await
            {
                Ok(admission) => Some(admission),
                Err(error) => {
                    if let Some(staging) = staging.as_mut() {
                        staging.release().await;
                    }
                    return Err(error.to_string());
                }
            }
        } else {
            None
        };
        #[cfg(test)]
        wait_at_workflow_post_save_test_barrier(&commit_session_id).await;

        // The durable user turn and exact snapshot now own the next
        // execution. Only now may the prior live activation be released.
        if workflow_changed {
            if let Err(error) = commit_state
                .skill_manager
                .release_activation_for_workspace(&commit_session_id, None)
                .await
            {
                tracing::error!(
                    session_id = %commit_session_id,
                    %error,
                    "failed to release prior Workflow activation after commit"
                );
            }
        }
        if let Some(staging) = staging.as_mut() {
            staging.release().await;
        }
        // Activation startup acquires the same Host lock. The complete
        // metadata/pin/admission transaction must release it first.
        drop(_workflow_commit_guard);
        drop(_persistence_guard);
        if let Some(admission) = admission {
            if let Err(error) = commit_state
                .session_messenger
                .activate_prepared(&admission)
                .await
            {
                // Body and immediate eligibility are already durable. The
                // existing activation recovery can retry this same input.
                tracing::warn!(session_id = %commit_session_id, %error, "Root chat input awaits activation");
            }
        } else if !queued {
            publish_committed_chat(&commit_state, &session);
        }
        Ok::<(), String>(())
    });
    match commit.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            return Err(HttpResponse::InternalServerError()
                .json(serde_json::json!({
                    "error": crate::error::error_value(format!(
                        "Failed to persist chat session: {error}"
                    ))
                }))
                .into());
        }
        Err(error) => {
            tracing::error!(%error, "Workflow authority chat commit task failed");
            return Err(crate::error::json_error(
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to commit Workflow authority chat",
            )
            .into());
        }
    }
    Ok(())
}
