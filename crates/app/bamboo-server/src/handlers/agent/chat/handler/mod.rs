use crate::error::ResponseResult;
use actix_web::{web, HttpRequest, HttpResponse, ResponseError};

use super::{ChatRequest, ChatResponse};
use crate::app_state::AppState;
use bamboo_engine::config::GoldConfig;
use bamboo_engine::model_config_helper::{
    parse_session_gold_config, resolve_gold_config, GOLD_CONFIG_METADATA_KEY,
};
use bamboo_engine::session_app::chat::{parse_goal_command, GoalCommand};
use bamboo_engine::session_app::metadata::SessionMetadataService;

mod images;
mod legacy_selection;
#[cfg(test)]
use legacy_selection::{
    install_workflow_commit_test_barrier, install_workflow_post_save_test_barrier,
    pin_explicit_workflow_candidate, wait_at_workflow_commit_test_barrier,
};
use legacy_selection::{
    workflow_activation_running_conflict_response, workflow_runner_is_active,
    WorkflowMetadataCheckpoint,
};
mod ingress;
pub(crate) use ingress::admit_for_execute;
mod request;

/// Publish the validated workspace after its session checkpoint is durable.
///
/// Project-context preview is AppState-scoped, so publication must use the
/// same provider pair rather than a sibling state's process-global first-wins
/// root. `workspace_source` is a non-secret diagnostic label.
fn sync_runtime_workspace(
    state: &AppState,
    session_id: &str,
    workspace_path: Option<&str>,
    workspace_source: &str,
) {
    if let Some(workspace) = workspace_path
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(std::path::PathBuf::from)
        .and_then(|path| std::fs::canonicalize(&path).ok().or(Some(path)))
    {
        state.workspace_resolver.publish_resolved_workspace(
            session_id,
            workspace,
            workspace_source,
        );
    }
}

/// Persist and refresh the cache while the caller holds the session's
/// `LockedSessionStore` guard.
///
/// Calling `save_and_cache_session` here would try to acquire the same
/// non-reentrant lock again. A plain storage save is safe because the chat
/// transaction owns the lock from its authoritative reload through its final
/// message checkpoint.
async fn persist_and_cache_session_locked(
    state: &AppState,
    session: &bamboo_agent_core::Session,
) -> std::io::Result<()> {
    state.persistence.storage().save_session(session).await?;
    state.sessions.insert(
        session.id.clone(),
        std::sync::Arc::new(bamboo_engine::SessionSnapshot::new(session.clone())),
    );
    Ok(())
}

async fn save_and_cache_session_locked(
    state: &AppState,
    session: &bamboo_agent_core::Session,
) -> ResponseResult<()> {
    persist_and_cache_session_locked(state, session)
        .await
        .map_err(|error| {
            HttpResponse::InternalServerError()
                .json(serde_json::json!({
                    "error": crate::error::error_value(format!(
                        "Failed to persist chat session: {error}"
                    ))
                }))
                .into()
        })
}

fn publish_committed_chat(state: &web::Data<AppState>, session: &bamboo_agent_core::Session) {
    if let Some(message) = session.messages.last() {
        state.account_sink.record(
            Some(&session.id),
            &bamboo_agent_core::AgentEvent::message_appended(&session.id, message),
        );
    }

    tracing::debug!(
        "[{}] Chat turn persisted: messages={}, last_role={:?} -> client should now POST /execute",
        session.id,
        session.messages.len(),
        session
            .messages
            .last()
            .map(|message| format!("{:?}", message.role)),
    );
    if !session.title_generated {
        crate::title_gen::spawn_title_generation(state.clone().into_inner(), session.id.clone());
    }
}

fn project_context_error_response(
    error: bamboo_engine::project_context::ProjectContextError,
) -> HttpResponse {
    use bamboo_engine::project_context::ProjectContextError;

    match error {
        ProjectContextError::WorkspaceConflict {
            workspace,
            owner_project_id,
            session_project_id,
        } => HttpResponse::Conflict().json(serde_json::json!({
            "error": {
                "type": "api_error",
                "code": "project_workspace_conflict",
                "message": "Workspace belongs to another Project"
            },
            "workspace": workspace,
            "owner_project_id": owner_project_id,
            "session_project_id": session_project_id,
        })),
        ProjectContextError::UnassignedWorkspaceConflict {
            workspace,
            owner_project_id,
        } => HttpResponse::Conflict().json(serde_json::json!({
            "error": {
                "type": "api_error",
                "code": "project_workspace_conflict",
                "message": "Workspace belongs to another Project"
            },
            "workspace": workspace,
            "owner_project_id": owner_project_id,
            "session_project_id": "unassigned",
        })),
        ProjectContextError::WorkspaceInvalid { workspace, message } => HttpResponse::BadRequest()
            .json(serde_json::json!({
                "error": {
                    "type": "api_error",
                    "code": "workspace_invalid",
                    "message": message
                },
                "workspace": workspace,
            })),
        ProjectContextError::InvalidProjectIdentity { raw, message } => HttpResponse::BadRequest()
            .json(serde_json::json!({
                "error": {
                    "type": "api_error",
                    "code": "invalid_project_identity",
                    "message": format!(
                        "Session carries an invalid Project identity '{raw}': {message}"
                    )
                }
            })),
        ProjectContextError::ProjectUnavailable { project_id } => {
            HttpResponse::Conflict().json(serde_json::json!({
                "error": {
                    "type": "api_error",
                    "code": "project_unavailable",
                    "message": "Assigned Project is unavailable"
                },
                "project_id": project_id,
            }))
        }
        ProjectContextError::ProjectPathMissing { project_id } => {
            HttpResponse::Conflict().json(serde_json::json!({
                "error": {
                    "type": "api_error",
                    "code": "project_path_missing",
                    "message": "Assigned Project has no configured project_path"
                },
                "project_id": project_id,
            }))
        }
        ProjectContextError::ProjectPathUnavailable {
            project_id,
            project_path,
            message,
        } => HttpResponse::Conflict().json(serde_json::json!({
            "error": {
                "type": "api_error",
                "code": "project_path_unavailable",
                "message": message
            },
            "project_id": project_id,
            "project_path": project_path,
        })),
        error @ (ProjectContextError::Source(_) | ProjectContextError::IdentityMismatch { .. }) => {
            tracing::error!(%error, "failed to resolve Project context");
            crate::error::json_error(
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to resolve Project context",
            )
        }
    }
}

/// A requested Root tool policy is part of the user turn, not the early
/// session checkpoint. Preserve its previous durable value until the message
/// and any attachments pass validation and the final turn is saved.
struct RootToolAuthorityCheckpoint {
    orchestration_only: bool,
    revision: u64,
    model_context_state: Option<bamboo_domain::ModelContextState>,
}

impl RootToolAuthorityCheckpoint {
    fn capture(session: Option<&bamboo_agent_core::Session>) -> Self {
        Self {
            orchestration_only: session.is_some_and(|session| session.root_orchestration_only),
            revision: session.map_or(0, |session| session.root_tool_authority_revision),
            model_context_state: session.and_then(|session| session.model_context_state.clone()),
        }
    }

    fn restore(&self, session: &mut bamboo_agent_core::Session) {
        if session.root_orchestration_only != self.orchestration_only
            || session.root_tool_authority_revision != self.revision
        {
            session.root_orchestration_only = self.orchestration_only;
            session.root_tool_authority_revision = self.revision;
            session
                .model_context_state
                .clone_from(&self.model_context_state);
        }
    }
}

#[cfg(test)]
mod tests;

/// Create a new chat message or update an existing session.
///
/// This endpoint accepts a user message and creates or updates a chat session.
/// After calling this endpoint, use the returned `stream_url` to execute
/// the agent and receive events.
pub async fn handler(
    state: web::Data<AppState>,
    http_request: HttpRequest,
    req: web::Json<ChatRequest>,
) -> HttpResponse {
    let session_id = request::resolve_session_id(req.session_id.as_deref());
    if let Err(error) = crate::handlers::agent::tickets::require_supervisor_owner(
        &state,
        &http_request,
        &session_id,
    )
    .await
    {
        return crate::handlers::agent::tickets::TicketHttpError::from(error).error_response();
    }
    let prepared = match crate::app_state::mutation_idempotency::prepare(
        &http_request,
        "chat",
        "POST /api/v1/chat",
        &*req,
    ) {
        Ok(prepared) => prepared,
        Err(response) => return *response,
    };
    let Some(prepared) = prepared else {
        return handle_chat(state, req, &http_request).await;
    };
    let store = state.mutation_idempotency.clone();
    store
        .execute(prepared, || handle_chat(state, req, &http_request))
        .await
}

async fn handle_chat(
    state: web::Data<AppState>,
    req: web::Json<ChatRequest>,
    http_request: &HttpRequest,
) -> HttpResponse {
    let root_mode_selection = match bamboo_domain::RootThinkingMode::resolve_selection(
        req.thinking_mode,
        req.root_orchestration_only,
    ) {
        Ok(selection) => selection,
        Err(error) => {
            return HttpResponse::BadRequest().json(serde_json::json!({
                "error": {
                    "type": "api_error",
                    "code": "root_thinking_mode_conflict",
                    "message": error.to_string(),
                }
            }));
        }
    };
    let session_id = request::resolve_session_id(req.session_id.as_deref());
    let (
        existing_session_found,
        existing_project_id,
        existing_workspace,
        existing_workspace_source,
    ) = match state.storage.load_session(&session_id).await {
        Ok(Some(existing)) => {
            let project_id = match bamboo_engine::project_context::ProjectContextResolver::session_project_identity(&existing) {
                    bamboo_engine::project_context::SessionProjectIdentity::Assigned(project_id) => Some(project_id),
                    bamboo_engine::project_context::SessionProjectIdentity::Unassigned => None,
                    bamboo_engine::project_context::SessionProjectIdentity::Invalid { raw, message } => {
                        return HttpResponse::BadRequest().json(serde_json::json!({
                            "error": {
                                "type": "api_error",
                                "code": "invalid_project_identity",
                                "message": format!(
                                    "Session carries an invalid Project identity '{raw}': {message}"
                                )
                            },
                            "session_id": session_id,
                        }));
                    }
                };
            (
                true,
                project_id,
                existing.workspace_path_meta(),
                existing
                    .metadata
                    .get(bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY)
                    .cloned(),
            )
        }
        Ok(None) => (false, None, None, None),
        Err(error) => {
            tracing::error!(%error, "failed to load chat session for Project validation");
            return crate::error::json_error(
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to validate session Project membership",
            );
        }
    };
    if let Some(project_id) = req.project_id.as_ref() {
        match state.project_store.get(project_id) {
            Ok(project) if project.status == bamboo_domain::ProjectStatus::Active => {}
            Ok(_) => {
                return HttpResponse::Conflict().json(serde_json::json!({
                    "error": {
                        "type": "api_error",
                        "code": "project_archived",
                        "message": "Sessions can only be created in an active Project"
                    },
                    "project_id": project_id,
                }));
            }
            Err(bamboo_projects::ProjectStoreError::NotFound(_)) => {
                return crate::error::json_error(
                    actix_web::http::StatusCode::NOT_FOUND,
                    "target Project not found",
                );
            }
            Err(error) => {
                tracing::error!(%error, "failed to validate chat Project");
                return crate::error::json_error(
                    actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to validate target Project",
                );
            }
        }
        if existing_session_found && existing_project_id.as_ref() != Some(project_id) {
            return HttpResponse::Conflict().json(serde_json::json!({
                "error": {
                    "type": "api_error",
                    "code": "session_project_reassignment_required",
                    "message": "Chat cannot change Project membership; use PATCH /sessions/{id}"
                },
                "session_id": session_id,
                "current_project_id": existing_project_id,
                "requested_project_id": project_id,
            }));
        }
    }
    let effective_project_id = req.project_id.clone().or(existing_project_id);
    let requested_workspace = req.workspace_path.as_deref();
    let fallback_workspace = || {
        request::optional_non_empty(req.workspace_path.as_deref()).or_else(|| {
            (existing_workspace_source.as_deref()
                != Some(bamboo_engine::project_context::WorkspaceSource::ProjectDefault.as_str()))
            .then_some(existing_workspace.as_deref())
            .flatten()
        })
    };
    let workspace_validation: ResponseResult<_> =
        if let Some(requested_workspace) = requested_workspace {
            crate::project_context::validate_explicit_session_workspace_with_resolver(
                &state.project_store,
                effective_project_id.as_ref(),
                requested_workspace,
                &state.workspace_resolver,
            )
            .map(Some)
            .map_err(|error| crate::project_context::session_workspace_error_response(error).into())
        } else {
            crate::project_context::validate_workspace_assignment_with_resolver(
                &state.project_store,
                effective_project_id.as_ref(),
                fallback_workspace(),
                &state.workspace_resolver,
            )
            .map_err(|error| {
                Box::new(match error {
                    crate::project_context::ProjectWorkspaceValidationError::Invalid {
                        code,
                        workspace,
                        message,
                    } => {
                        let mut response = if code.starts_with("project_path_") {
                            HttpResponse::Conflict()
                        } else {
                            HttpResponse::BadRequest()
                        };
                        response.json(serde_json::json!({
                            "error": {
                                "type": "api_error",
                                "code": code,
                                "message": message
                            },
                            "workspace": workspace,
                        }))
                    }
                    crate::project_context::ProjectWorkspaceValidationError::Conflict {
                        workspace,
                        owner_project_id,
                        session_project_id,
                    } => HttpResponse::Conflict().json(serde_json::json!({
                        "error": {
                            "type": "api_error",
                            "code": "project_workspace_conflict",
                            "message": "Workspace belongs to another Project"
                        },
                        "workspace": workspace,
                        "owner_project_id": owner_project_id,
                        "session_project_id": session_project_id,
                    })),
                    crate::project_context::ProjectWorkspaceValidationError::Store(error) => {
                        tracing::error!(%error, "failed to validate workspace Project ownership");
                        crate::error::json_error(
                            actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                            "Failed to validate workspace Project ownership",
                        )
                    }
                })
            })
        };
    let final_workspace = match workspace_validation {
        Ok(workspace) => workspace,
        Err(response) => return *response,
    };
    let final_workspace_display = final_workspace
        .as_deref()
        .map(bamboo_config::paths::path_to_display_string);
    tracing::debug!(
        "[{}] Chat requested: message_len={}, is_goal_command={}, image_count={}",
        session_id,
        req.message.len(),
        parse_goal_command(&req.message).is_some(),
        req.images.as_ref().map(|i| i.len()).unwrap_or(0),
    );
    // Only resolve the server-side default (a config read + the shared
    // resolution cascade) when the request omitted `model` — the common case
    // (an explicit model) pays none of that cost. #480: fall back to the SAME
    // resolved default the connect bridge and `GET /execute/defaults` use, so
    // there is one implementation of "what model does this server default to".
    let config_snapshot = state.config.read().await.clone();
    let default_model = if request::optional_non_empty(req.model.as_deref()).is_some() {
        None
    } else {
        bamboo_engine::resolved_defaults::resolve_default_run_config(
            &config_snapshot,
            &state.provider_registry,
        )
        .model_roster
        .model
    };
    let model = match request::resolve_model(req.model.as_deref(), default_model.as_deref()) {
        Ok(model) => model,
        Err(response) => return *response,
    };

    let global_default_prompt =
        bamboo_engine::prompt_defaults::read_global_default_system_prompt_template();
    let builtin_fallback_prompt = crate::app_state::DEFAULT_BASE_PROMPT;
    let configured_default_workspace = config_snapshot
        .get_default_work_area_path()
        .as_deref()
        .map(bamboo_config::paths::path_to_display_string);
    let session_fallback_path = state
        .workspace_resolver
        .preview_session_fallback(&session_id)
        .as_deref()
        .map(bamboo_config::paths::path_to_display_string);

    let data_dir = Some(state.app_data_dir.clone());
    let mut project_preflight = bamboo_agent_core::Session::new(&session_id, &model);
    if let Some(project_id) = effective_project_id.as_ref() {
        project_preflight.set_project_id_meta(project_id.to_string());
    }
    if let Some(workspace) = final_workspace_display.as_deref() {
        project_preflight.set_workspace_path_meta(workspace);
        let source = if req.workspace_path.is_some() {
            bamboo_engine::project_context::WorkspaceSource::Explicit
        } else if existing_workspace.is_some()
            && existing_workspace_source.as_deref()
                != Some(bamboo_engine::project_context::WorkspaceSource::ProjectDefault.as_str())
        {
            bamboo_engine::project_context::WorkspaceSource::Session
        } else if effective_project_id.is_some() {
            bamboo_engine::project_context::WorkspaceSource::ProjectDefault
        } else {
            bamboo_engine::project_context::WorkspaceSource::Session
        };
        project_preflight.metadata.insert(
            bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY.to_string(),
            source.as_str().to_string(),
        );
    } else if let Some(workspace) = configured_default_workspace.as_deref() {
        project_preflight.set_workspace_path_meta(workspace);
    } else if let Some(workspace) = session_fallback_path.as_deref() {
        // This owning-state fallback is explicit so Project preflight cannot
        // observe a same-id runtime entry published by another AppState.
        project_preflight.set_workspace_path_meta(workspace);
    }
    if let Err(error) = state
        .project_context_resolver
        .refresh_session_prompt_read_only(&mut project_preflight)
        .await
    {
        return project_context_error_response(error);
    }
    let workspace_was_explicit = req.workspace_path.is_some();
    if let Err(response) = ingress::validate_skill_request(&req) {
        return *response;
    }
    let requested_workflow_selection = req.workflow_selection.clone();
    // An explicit request value wins; otherwise stamp the durable
    // permission-policy seed for a NEW session. The engine applies this only
    // on the startup branch, so existing sessions stay authoritative.
    let chat_permission_mode = Some(req.permission_mode.unwrap_or_else(|| {
        state
            .permission_checker
            .permission_config()
            .map(|config| config.default_session_permission_mode())
            .unwrap_or_default()
    }));
    let mut input = bamboo_engine::session_app::types::ChatTurnInput {
        session_id: session_id.clone(),
        project_id: effective_project_id,
        model: model.clone(),
        model_ref: req.model_ref.clone(),
        provider: req.provider.clone(),
        reasoning_effort: req.reasoning_effort,
        permission_mode: chat_permission_mode,
        message: req.message.clone(),
        system_prompt: request::optional_non_empty(req.system_prompt.as_deref()).map(String::from),
        enhance_prompt: request::optional_non_empty(req.enhance_prompt.as_deref())
            .map(String::from),
        root_orchestration_prompt: req.root_orchestration_prompt,
        root_orchestration_only: root_mode_selection,
        // Preserve field presence. An omitted workspace must be resolved from
        // the fresh durable session after acquiring the lock, not from this
        // lock-free preflight snapshot.
        workspace_path: workspace_was_explicit
            .then(|| project_preflight.workspace_path_meta())
            .flatten(),
        default_workspace_path: configured_default_workspace,
        selected_skill_ids: if requested_workflow_selection.is_some() {
            None
        } else {
            req.selected_skill_ids.clone()
        },
        // A typed selection is resolved into a separate candidate below. The
        // authoritative session must retain its current activation until every
        // fallible hook, attachment and message write in this request succeeds.
        workflow_selection: None,
        orchestration_opt_in: req.orchestration_opt_in,
        data_dir,
    };

    // Serialize the authoritative reload, Project/workspace resolution and all
    // writes for this turn. In particular, Project reassignment PATCH uses the
    // same lock and therefore cannot slip between the second load and a stale
    // chat snapshot save.
    let persistence_guard = state.persistence.acquire_lock(&session_id).await;
    // Reject an already-running session before doing catalog or hook work. A
    // second check immediately before publication below closes the race with a
    // reservation that appears while this request is being prepared.
    if requested_workflow_selection.is_some() {
        let runners = state.agent_runners.read().await;
        let runner_is_active = workflow_runner_is_active(runners.get(&session_id));
        let startup_is_active = crate::handlers::agent::events::execute_startup_is_in_flight(
            state.as_ref(),
            &session_id,
        );
        if runner_is_active || startup_is_active {
            return workflow_activation_running_conflict_response(&session_id);
        }
    }
    let authoritative_session = match state.persistence.storage().load_session(&session_id).await {
        Ok(session) => session,
        Err(error) => {
            tracing::error!(%error, "failed to load authoritative chat session");
            return crate::error::json_error(
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to load authoritative chat session",
            );
        }
    };
    // An existing Root changes tool authority through the recoverable
    // mode-only operation. Keeping an unfenced inline path would allow a late
    // chat POST to undo a recovery result after the client has read detail.
    let existing_root = authoritative_session.as_ref().is_some_and(|session| {
        session.kind == bamboo_domain::SessionKind::Root && session.parent_session_id.is_none()
    });
    let existing_ordinary_root = existing_root
        && authoritative_session
            .as_ref()
            .is_some_and(|session| !session.root_orchestration_only_enabled());
    let existing_workflow_authority = authoritative_session.as_ref().is_some_and(|session| {
        let metadata = &session.metadata;
        metadata.contains_key(bamboo_skills::WORKFLOW_SELECTION_METADATA_KEY)
            || metadata
                .get(bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY)
                .is_some_and(|raw| {
                    serde_json::from_str::<bamboo_skills::ActiveWorkflow>(raw)
                        .map(|workflow| {
                            workflow.status != bamboo_skills::WorkflowActivationStatus::Deactivated
                        })
                        .unwrap_or(true)
                })
            || metadata.contains_key(bamboo_skills::ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY)
            || metadata
                .contains_key(bamboo_skills::runtime_metadata::SKILL_RUNTIME_PINNED_SNAPSHOT_KEY)
    });
    if root_mode_selection.is_some() && existing_root {
        return HttpResponse::build(actix_web::http::StatusCode::PRECONDITION_REQUIRED).json(
            serde_json::json!({
                "error": {
                    "type": "api_error",
                    "code": "root_mode_operation_required",
                    "message": "Change an existing Root mode with a recoverable mode operation before chat",
                },
                "session_id": session_id,
            }),
        );
    }
    let root_input_messages = if let Some(session) = authoritative_session.as_ref() {
        match state.session_store.root_actor_input_required(session).await {
            Ok(true) => Some(session.messages.clone()),
            Ok(false) => None,
            Err(error) => {
                return crate::error::json_error(
                    actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to classify Root input: {error}"),
                )
            }
        }
    } else {
        None
    };
    let workflow_metadata_checkpoint =
        WorkflowMetadataCheckpoint::capture(authoritative_session.as_ref());
    let root_tool_authority_checkpoint =
        RootToolAuthorityCheckpoint::capture(authoritative_session.as_ref());
    let authoritative_workspace_present = authoritative_session.as_ref().is_some_and(|session| {
        session.workspace_path_meta().is_some()
            && session
                .metadata
                .get(bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY)
                .map(String::as_str)
                != Some(bamboo_engine::project_context::WorkspaceSource::ProjectDefault.as_str())
    });
    if req.project_id.is_none() {
        input.project_id = authoritative_session.as_ref().and_then(|session| {
            match bamboo_engine::project_context::ProjectContextResolver::session_project_identity(
                session,
            ) {
                bamboo_engine::project_context::SessionProjectIdentity::Assigned(project_id) => {
                    Some(project_id)
                }
                bamboo_engine::project_context::SessionProjectIdentity::Unassigned
                | bamboo_engine::project_context::SessionProjectIdentity::Invalid { .. } => None,
            }
        });
    }
    if let Some(requested_workspace) = req.workspace_path.as_deref() {
        input.workspace_path =
            match crate::project_context::validate_explicit_session_workspace_with_resolver(
                &state.project_store,
                input.project_id.as_ref(),
                requested_workspace,
                &state.workspace_resolver,
            ) {
                Ok(workspace) => Some(bamboo_config::paths::path_to_display_string(&workspace)),
                Err(error) => {
                    return crate::project_context::session_workspace_error_response(error)
                }
            };
    } else {
        input.workspace_path = None;
    }
    let workspace_source = if workspace_was_explicit {
        "request"
    } else if authoritative_workspace_present {
        "session"
    } else if input.project_id.is_some() {
        "project_default"
    } else if input.default_workspace_path.is_some() {
        "configured_default"
    } else {
        "session_fallback"
    };
    let workspace_fallback_policy =
        bamboo_engine::session_app::types::ChatWorkspaceFallbackPolicy::Authoritative {
            session_fallback_path,
        };

    let mut session = match bamboo_engine::session_app::chat::prepare_chat_turn_from_authoritative_session_with_workspace_policy(
        authoritative_session,
        input,
        global_default_prompt.as_str(),
        builtin_fallback_prompt,
        workspace_fallback_policy,
    ) {
            Ok(session) => session,
            Err(bamboo_engine::session_app::errors::ChatError::InvalidWorkflowSelection(error)) => {
                return HttpResponse::BadRequest().json(serde_json::json!({
                    "error": crate::error::error_value(error)
                }));
            }
            Err(bamboo_engine::session_app::errors::ChatError::RootToolAuthority(error)) => {
                let (status, code) = match error {
                    bamboo_domain::RootToolAuthorityError::NotRoot => (
                        actix_web::http::StatusCode::BAD_REQUEST,
                        "root_orchestration_requires_root",
                    ),
                    bamboo_domain::RootToolAuthorityError::LegacyPlanActive
                    | bamboo_domain::RootToolAuthorityError::WorkflowSelected => (
                        actix_web::http::StatusCode::CONFLICT,
                        "root_orchestration_incompatible_mode",
                    ),
                    bamboo_domain::RootToolAuthorityError::RevisionOverflow
                    | bamboo_domain::RootToolAuthorityError::StaleSnapshot => (
                        actix_web::http::StatusCode::CONFLICT,
                        "root_orchestration_authority_conflict",
                    ),
                };
                return HttpResponse::build(status).json(serde_json::json!({
                    "error": {
                        "type": "api_error",
                        "code": code,
                        "message": error.to_string(),
                    },
                    "session_id": session_id,
                }));
            }
            Err(bamboo_engine::session_app::errors::ChatError::InvalidProjectIdentity {
                raw,
                message,
            }) => {
                return HttpResponse::BadRequest().json(serde_json::json!({
                    "error": {
                        "type": "api_error",
                        "code": "invalid_project_identity",
                        "message": format!(
                            "Session carries an invalid Project identity '{raw}': {message}"
                        )
                    },
                    "session_id": session_id,
                }));
            }
            Err(bamboo_engine::session_app::errors::ChatError::ProjectIdentityConflict {
                expected,
                actual,
            }) => {
                return HttpResponse::Conflict().json(serde_json::json!({
                    "error": {
                        "type": "api_error",
                        "code": "session_project_changed",
                        "message": "Session Project membership changed while preparing chat"
                    },
                    "session_id": session_id,
                    "expected_project_id": expected,
                    "actual_project_id": actual,
                }));
            }
            Err(error) => {
                tracing::error!("Chat turn preparation failed: {error}");
                return HttpResponse::InternalServerError().json(serde_json::json!({
                    "error": crate::error::error_value(format!("Failed to prepare chat: {error}"))
                }));
            }
        };
    let session_was_created = session
        .metadata
        .get(bamboo_engine::session_app::chat::SESSION_START_SOURCE_METADATA_KEY)
        .is_some_and(|source| source == "startup");
    // Explicitly clearing a Workflow on an existing ordinary Root must retire
    // its durable snapshot and live pin with the final user turn. The separate
    // recoverable Root mode operation can then enable orchestration-only mode.
    // Keep the first-chat combined switch on the same guarded commit path.
    let root_workflow_switch = req.root_orchestration_only == Some(true)
        && req
            .selected_skill_ids
            .as_ref()
            .is_some_and(|ids| ids.iter().all(|id| id.trim().is_empty()))
        && !root_tool_authority_checkpoint.orchestration_only
        && session.root_orchestration_only_enabled();
    let explicit_workflow_retirement = existing_ordinary_root
        && existing_workflow_authority
        && req.workflow_selection.is_none()
        && req
            .selected_skill_ids
            .as_ref()
            .is_some_and(|ids| ids.iter().all(|id| id.trim().is_empty()));
    let retire_workflow = root_workflow_switch || explicit_workflow_retirement;
    if requested_workflow_selection.is_some() && session.root_orchestration_only_enabled() {
        return HttpResponse::Conflict().json(serde_json::json!({
            "error": {
                "type": "api_error",
                "code": "root_orchestration_incompatible_mode",
                "message": "Disable Root orchestration-only mode before selecting a Workflow",
            },
            "session_id": session_id,
        }));
    }
    if let Err(error) = state
        .project_context_resolver
        .refresh_session_prompt_read_only(&mut session)
        .await
    {
        return project_context_error_response(error);
    }
    let mut staged_workflow_activation = match legacy_selection::stage_selection(
        state.as_ref(),
        &session,
        requested_workflow_selection.as_ref(),
        req.selected_skill_ids.as_deref(),
        &req.message,
        &config_snapshot,
    )
    .await
    {
        Ok(staging) => staging,
        Err(response) => return *response,
    };
    // Publish the prepared checkpoint without any speculative Workflow
    // normalization. The in-memory turn keeps those changes for a successful
    // final commit, while every rejected/failing path continues to expose the
    // exact pre-request Workflow authority.
    let mut durable_base = session.clone();
    workflow_metadata_checkpoint.restore(&mut durable_base);
    root_tool_authority_checkpoint.restore(&mut durable_base);
    if let Some(messages) = &root_input_messages {
        durable_base.messages = messages.clone();
    }
    if let Err(response) = save_and_cache_session_locked(state.as_ref(), &durable_base).await {
        if let Some(staging) = staged_workflow_activation.as_mut() {
            staging.release().await;
        }
        return *response;
    }
    sync_runtime_workspace(
        state.as_ref(),
        &session_id,
        session.workspace_path_meta().as_deref(),
        workspace_source,
    );
    if session_was_created {
        state.account_sink.record(
            Some(&session_id),
            &bamboo_agent_core::AgentEvent::SessionCreated {
                session_id: session_id.clone(),
                project_id: session.project_id_meta(),
                title: session.title.clone(),
                kind: session.kind,
                created_at: session.created_at,
            },
        );
    }

    let effective_message = match crate::lifecycle_hooks::apply_user_prompt_submit_hooks(
        &config_snapshot.lifecycle_hooks,
        Some(state.app_data_dir.clone()),
        &mut session,
        &req.message,
    )
    .await
    {
        Ok(message) => message,
        Err(reason) => {
            if let Some(staging) = staged_workflow_activation.as_mut() {
                staging.release().await;
            }
            // Persist the hook checkpoint but never the rejected user message.
            workflow_metadata_checkpoint.restore(&mut session);
            root_tool_authority_checkpoint.restore(&mut session);
            if let Some(messages) = &root_input_messages {
                session.messages = messages.clone();
            }
            if let Err(response) = save_and_cache_session_locked(state.as_ref(), &session).await {
                return *response;
            }
            return HttpResponse::BadRequest().json(serde_json::json!({
                "error": crate::error::error_value(reason),
                "hook_event": "UserPromptSubmit"
            }));
        }
    };

    // ---- Goal command interception ----
    if let Some(goal_cmd) = parse_goal_command(&req.message) {
        tracing::debug!(
            "[{}] Chat intercepted as /goal command: {:?}",
            session_id,
            goal_cmd
        );
        // Goal metadata writers acquire the same per-session lock and load the
        // latest durable session themselves. The prepared checkpoint is
        // already durable, so hand ownership over before invoking them.
        if let Some(staging) = staged_workflow_activation.as_mut() {
            staging.release().await;
        }
        drop(persistence_guard);
        return handle_goal_command(state.as_ref(), &session_id, &goal_cmd).await;
    }

    #[cfg(test)]
    wait_at_workflow_commit_test_barrier(&session_id).await;

    // Workflow activation or retirement changes both durable Workflow metadata
    // and the session-id keyed immutable skill pin. Re-check after all fallible
    // hook work, then retain the runners read guard through attachment
    // persistence, the final session save and pin handoff. The persistence lock
    // linearizes HTTP execute startup; the runners guard closes reservation
    // races from resume, schedule and connect entry points.
    let workflow_commit_guard = if staged_workflow_activation.is_some() || retire_workflow {
        let runners = state.agent_runners.clone().read_owned().await;
        let runner_is_active = workflow_runner_is_active(runners.get(&session_id));
        let startup_is_active = crate::handlers::agent::events::execute_startup_is_in_flight(
            state.as_ref(),
            &session_id,
        );
        if runner_is_active || startup_is_active {
            if let Some(staging) = staged_workflow_activation.as_mut() {
                staging.release().await;
            }
            return workflow_activation_running_conflict_response(&session_id);
        }
        Some(runners)
    } else {
        None
    };

    let queue_root_input = root_input_messages.is_some();
    let metadata_before_input = session.metadata.clone();
    let runtime_metadata_before_input = session.runtime_metadata.clone();
    // Image handling stays in the handler layer (depends on AppState attachment reader).
    let ingress_receipt =
        match ingress::queue(&state, &session, &req, &effective_message, http_request).await {
            Ok(receipt) => receipt,
            Err(response) => {
                if let Some(staging) = staged_workflow_activation.as_mut() {
                    staging.release().await;
                }
                return *response;
            }
        };
    let queued = ingress_receipt.is_some();
    if queued {
        let receipt = ingress_receipt.as_ref().expect("queued receipt");
        match state
            .session_inbox
            .was_admitted(&session.id, &receipt.id)
            .await
        {
            Ok(false) => {
                session
                    .metadata
                    .insert("chat.queued_ingress.v1".into(), receipt.id.to_string());
                crate::handlers::agent::events::mark_pending_turn(&mut session);
            }
            Ok(true) => {}
            Err(error) => {
                return crate::error::json_error(
                    actix_web::http::StatusCode::SERVICE_UNAVAILABLE,
                    error.to_string(),
                )
            }
        }
    } else if let Err(response) = images::append_user_message(
        &state,
        &mut session,
        &effective_message,
        req.images.as_deref(),
    )
    .await
    {
        if let Some(staging) = staged_workflow_activation.as_mut() {
            staging.release().await;
        }
        return *response;
    }

    let mut queued_input = if queue_root_input && !queued {
        let skill_request = match ingress::skill_request(&req) {
            Ok(data) => data,
            Err(response) => return *response,
        };
        let message = session
            .messages
            .pop()
            .expect("append_user_message produced a User turn");
        // A queued turn has not replaced the previous execution's handoff yet.
        // Restore the entire metadata checkpoint, including startup/error keys.
        session.metadata = metadata_before_input;
        session.runtime_metadata = runtime_metadata_before_input;
        let mut envelope =
            bamboo_domain::SessionMessageEnvelope::user_input(session.id.clone(), message.content);
        envelope.id = match bamboo_domain::SessionMessageId::parse(&message.id) {
            Ok(id) => id,
            Err(error) => {
                if let Some(staging) = staged_workflow_activation.as_mut() {
                    staging.release().await;
                }
                return crate::error::json_error(
                    actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Invalid Root input identity: {error}"),
                );
            }
        };
        envelope.created_at = message.created_at;
        envelope.body =
            bamboo_domain::SessionMessageBody::Content(bamboo_domain::SessionMessageContent {
                text: match &envelope.body {
                    bamboo_domain::SessionMessageBody::Content(content) => content.text.clone(),
                    _ => unreachable!(),
                },
                parts: message.content_parts.unwrap_or_default(),
                skill_request,
            });
        Some(envelope)
    } else {
        None
    };

    if retire_workflow {
        // The old candidate can otherwise be restored on the next execute,
        // even after the visible Workflow selection was removed. Preserve the
        // deactivation event and run history while retiring executable state.
        session.metadata.retain(|key, _| {
            !key.starts_with("skill_runtime_")
                && !matches!(
                    key.as_str(),
                    "skill.context"
                        | "workflow.context_cache.v1"
                        | "workflow.dynamic_context.last.v1"
                        | "workflow.catalog_diagnostic.v1"
                )
        });
        bamboo_engine::runner::refresh_prompt_snapshot(&mut session);
    }

    if staged_workflow_activation.is_some() || retire_workflow || queued_input.is_some() {
        let workflow_changed = staged_workflow_activation.is_some() || retire_workflow;
        let mut staging = staged_workflow_activation;
        if let Some(staging) = staging.as_ref() {
            staging.apply(&mut session.metadata);
        }
        if let (Some(envelope), Some(original)) =
            (queued_input.take(), root_input_messages.as_ref())
        {
            let desired = session
                .messages
                .iter()
                .find(|message| message.role == bamboo_domain::Role::System);
            let original_prompts: Vec<_> = original
                .iter()
                .filter(|message| message.role == bamboo_domain::Role::System)
                .collect();
            let prompt_changed = desired.is_some_and(|desired| {
                original_prompts.len() != 1
                    || original_prompts[0].content != desired.content
                    || original
                        .first()
                        .is_none_or(|message| message.role != bamboo_domain::Role::System)
            });
            queued_input = Some(if prompt_changed {
                match envelope
                    .with_root_chat_prompt(desired.expect("changed prompt").content.clone())
                {
                    Ok(envelope) => envelope,
                    Err(error) => {
                        if let Some(staging) = staging.as_mut() {
                            staging.release().await;
                        }
                        return crate::error::json_error(
                            actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                            format!("Invalid Root chat prompt: {error}"),
                        );
                    }
                }
            } else {
                envelope
            });
            // All default ingress saves retain canonical Main. The owned
            // consumer publishes prompt + typed turn + cursor together.
            session.messages = original.clone();
        }
        if let Err(response) = legacy_selection::commit_selected_input(
            state.clone(),
            session,
            session_id.clone(),
            staging,
            workflow_changed,
            queued_input,
            queued,
            persistence_guard,
            workflow_commit_guard,
        )
        .await
        {
            return *response;
        }
    } else {
        // Re-save to persist image attachments (if any).
        if let Err(response) = save_and_cache_session_locked(state.as_ref(), &session).await {
            return *response;
        }
        drop(workflow_commit_guard);
        if !queued {
            publish_committed_chat(&state, &session);
        }
        drop(persistence_guard);
    }

    HttpResponse::Created().json(ChatResponse {
        message_id: ingress_receipt.as_ref().map(|r| r.id.to_string()),
        ingress_seq: ingress_receipt.as_ref().map(|r| r.generation),
        session_id: session_id.clone(),
        stream_url: format!("/api/v1/events/{}", session_id),
        status: "streaming".to_string(),
        goal_command: None,
    })
}

/// Additional response payload for `/goal` control commands.
#[derive(Debug, serde::Serialize)]
pub struct GoalCommandResponse {
    /// The action taken: "status", "off", "clear", "on", "set_prompt", "on_no_prompt".
    pub action: String,
    /// Whether the frontend should proceed with execute after this response.
    pub should_execute: bool,
    /// The updated (or current) gold config for this session.
    pub gold_config: Option<GoldConfig>,
}

/// Handle a parsed `/goal` command by updating session metadata and optionally
/// injecting a hidden resume message to trigger the Gold mini-loop.
async fn handle_goal_command(
    state: &AppState,
    session_id: &str,
    cmd: &GoalCommand,
) -> HttpResponse {
    let config_snapshot = state.config.read().await.clone();

    // Load current session to read the existing gold_config override.
    let session = match state.load_session_merged(session_id).await {
        Some(s) => s,
        None => {
            return HttpResponse::NotFound().json(serde_json::json!({
                "error": crate::error::error_value("Session not found"),
                "session_id": session_id
            }));
        }
    };

    // Resolve the current effective gold config (session override → global default).
    let current_json = session.metadata.get(GOLD_CONFIG_METADATA_KEY).cloned();
    let current_effective = resolve_gold_config(&config_snapshot, current_json.as_deref());

    let (new_config, should_resume) = match cmd {
        GoalCommand::Status => {
            let response_config = current_effective.clone();
            return HttpResponse::Ok().json(ChatResponse {
                message_id: None,
                ingress_seq: None,
                session_id: session_id.to_string(),
                stream_url: format!("/api/v1/events/{}", session_id),
                status: "accepted".to_string(),
                goal_command: Some(GoalCommandResponse {
                    action: "status".to_string(),
                    should_execute: false,
                    gold_config: response_config,
                }),
            });
        }
        GoalCommand::Off => {
            let mut cfg = current_effective.unwrap_or_default();
            cfg.enabled = false;
            cfg.auto_answer_enabled = false;
            cfg.auto_continue_enabled = false;
            (cfg, false)
        }
        GoalCommand::Clear => {
            let mut cfg = current_effective.unwrap_or_default();
            cfg.enabled = false;
            cfg.auto_answer_enabled = false;
            cfg.auto_continue_enabled = false;
            cfg.goal = None;
            cfg.evaluation_prompt = None;
            (cfg, false)
        }
        GoalCommand::On => {
            let mut cfg = current_effective.unwrap_or_default();
            let has_prompt = cfg.effective_goal().is_some();
            if !has_prompt {
                return HttpResponse::Ok().json(ChatResponse {
                    message_id: None,
                    ingress_seq: None,
                    session_id: session_id.to_string(),
                    stream_url: format!("/api/v1/events/{}", session_id),
                    status: "accepted".to_string(),
                    goal_command: Some(GoalCommandResponse {
                        action: "on_no_prompt".to_string(),
                        should_execute: false,
                        gold_config: Some(cfg),
                    }),
                });
            }
            cfg.enabled = true;
            cfg.auto_answer_enabled = true;
            cfg.auto_continue_enabled = true;
            (cfg, false)
        }
        GoalCommand::SetPrompt(prompt) => {
            let mut cfg = current_effective.unwrap_or_default();
            cfg.enabled = true;
            cfg.auto_answer_enabled = true;
            cfg.auto_continue_enabled = true;
            cfg.goal = Some(prompt.clone());
            (cfg, true)
        }
    };

    // Serialize the new config and persist via authoritative metadata writer.
    let new_json = serde_json::to_string(&new_config).ok();
    match SessionMetadataService::set_gold_config_json(state, session_id, new_json.clone(), None)
        .await
    {
        Ok(_) => {}
        Err(e) => {
            tracing::error!(session_id = %session_id, "Failed to persist gold_config: {e}");
            return HttpResponse::InternalServerError().json(serde_json::json!({
                "error": crate::error::error_value(format!("Failed to update goal config: {e}"))
            }));
        }
    }

    // Reset stale goal runtime state on ANY goal-config change — off / clear / on
    // / set-prompt — not just set-prompt. Otherwise a finished goal's status and
    // its double-check eval history linger in `goal.state` and keep being
    // surfaced to the frontend (and over the history API) after the goal is
    // turned off or cleared. The suspension reset + clarification turn below are
    // specific to `/goal <prompt>` and stay gated on `should_resume`.
    if let Some(mut session) = state.load_session_merged(session_id).await {
        // Drop ALL `gold.*` runtime snapshot keys by prefix — last_evaluation /
        // last_decision / last_confidence / last_reasoning / last_checkpoint /
        // last_iteration / evaluation_count (written by
        // `apply_gold_evaluation_result`) plus the legacy auto_continue_count — so
        // this list can't drift out of sync with what the evaluator writes. The
        // config lives under `gold_config` (no dot), so it is left untouched.
        session.metadata.retain(|key, _| !key.starts_with("gold."));
        // Durable goal state (status, continuation budget, declared status, and
        // the double-check eval history). Keyed by
        // `goal_state::GOAL_STATE_METADATA_KEY`.
        session.metadata.remove("goal.state");

        if should_resume {
            // Reset runtime suspension so execute can proceed.
            if let Some(runtime_state) = session.agent_runtime_state.as_mut() {
                runtime_state.status = bamboo_domain::AgentStatusState::Idle;
                runtime_state.suspension = None;
                runtime_state.waiting_for_children = None;
            }
            session.metadata.remove("runtime.suspend_reason");

            // Read the goal text back so we can echo it into the discussion turn.
            let goal_text = parse_session_gold_config(new_json.as_deref())
                .as_ref()
                .and_then(|cfg| cfg.effective_goal().map(str::to_string))
                .unwrap_or_default();

            // Inject a runtime instruction that asks the agent to discuss the
            // goal with the user before executing. The instruction itself is
            // hidden from the UI; the agent's visible reply IS the discussion.
            let instruction = format!(
                "The user has set a session goal:\n\n{goal_text}\n\nBefore taking any action, briefly confirm your understanding of this goal, surface any ambiguities or assumptions, and outline how you plan to achieve it. If anything is genuinely unclear or you need a decision from the user, ask them now. Otherwise, state your plan and begin working toward the goal."
            );
            let mut resume_msg = bamboo_domain::Message::user(instruction);
            resume_msg.metadata = Some(serde_json::json!({
                "hidden_from_ui": true,
                "runtime_kind": "gold_goal_resume"
            }));
            session.add_message(resume_msg);
            crate::handlers::agent::events::mark_pending_turn(&mut session);
        }

        state.save_and_cache_session(&mut session).await;
    }

    // Parse back the persisted config for the response.
    let response_config = parse_session_gold_config(new_json.as_deref());

    HttpResponse::Ok().json(ChatResponse {
        message_id: None,
        ingress_seq: None,
        session_id: session_id.to_string(),
        stream_url: format!("/api/v1/events/{}", session_id),
        status: "accepted".to_string(),
        goal_command: Some(GoalCommandResponse {
            action: match cmd {
                GoalCommand::Off => "off".to_string(),
                GoalCommand::Clear => "clear".to_string(),
                GoalCommand::On => "on".to_string(),
                GoalCommand::SetPrompt(_) => "set_prompt".to_string(),
                GoalCommand::Status => unreachable!(),
            },
            should_execute: should_resume,
            gold_config: response_config,
        }),
    })
}

// Note: image attachments are stored on disk in SessionStoreV2, and message parts
// use `bamboo-attachment://<session_id>/<attachment_id>` references.
