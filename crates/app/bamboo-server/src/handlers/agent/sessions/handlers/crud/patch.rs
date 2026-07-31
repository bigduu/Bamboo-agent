use actix_web::{web, HttpRequest, HttpResponse, Result};

use crate::app_state::AppState;
use bamboo_engine::model_config_helper::normalize_gold_config_json;
use bamboo_engine::session_app::metadata::{MetadataError, SessionMetadataService};
use bamboo_engine::session_app::provider_model::{
    derive_model_ref, persist_legacy_model_provider, persist_model_ref,
};

use super::super::super::types::PatchSessionRequest;
use super::query::get_session;
use super::running::is_session_running;

/// Parse an `If-Match` header value into the expected `metadata_version`.
/// Accepts a bare integer or a (weak) quoted ETag: `7`, `"7"`, `W/"7"`.
fn parse_if_match(req: &HttpRequest) -> Option<u64> {
    let raw = req.headers().get(actix_web::http::header::IF_MATCH)?;
    let s = raw.to_str().ok()?.trim();
    let s = s.strip_prefix("W/").unwrap_or(s).trim();
    let s = s.trim_matches('"');
    s.parse::<u64>().ok()
}

/// 412 Precondition Failed, advertising the current version as the ETag so the
/// client can refetch, reapply its change, and retry.
fn precondition_failed(session_id: &str, current: u64) -> HttpResponse {
    HttpResponse::PreconditionFailed()
        .insert_header((actix_web::http::header::ETAG, format!("\"{current}\"")))
        .json(serde_json::json!({
            "error": crate::error::error_value(
                "Version conflict: the session was modified by another client"
            ),
            "session_id": session_id,
            "current_version": current,
        }))
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
            tracing::error!(%error, "failed to resolve Project context for reassignment");
            crate::error::json_error(
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to resolve Project context",
            )
        }
    }
}

/// `PATCH /api/v1/sessions/{session_id}`
///
/// Title and pinned are routed through [`SessionMetadataService`] so they go
/// through the canonical pipeline (load → re-check → bump version
/// → locked save → cache → publish_replayable_session_event). Non-metadata
/// fields (`model_ref`, `reasoning_effort`) are written via the locked
/// metadata-merge path to avoid clobbering concurrent UI edits.
///
/// An optional `If-Match: "<metadata_version>"` header enforces optimistic
/// concurrency: the precondition is checked inside the per-session lock (so it
/// is race-free) and a mismatch returns `412`. The precondition is applied to
/// the first authoritative write in the patch (each write bumps the version).
/// Project reassignment and Workspace switching are deliberately performed
/// first as one atomic metadata transaction and require an explicit
/// precondition, so a mixed-field request cannot consume the caller's CAS
/// token on a lower-risk title/pin update.
pub async fn patch_session(
    state: web::Data<AppState>,
    path: web::Path<String>,
    http_req: HttpRequest,
    req: web::Json<PatchSessionRequest>,
) -> Result<HttpResponse> {
    let session_id = path.into_inner();
    if req.permission_mode.is_some() && req.bypass_permissions.is_some() {
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "error": crate::error::error_value(
                "permission_mode and bypass_permissions cannot be set together"
            ),
            "session_id": session_id,
        })));
    }
    let request_precondition = parse_if_match(&http_req);
    if req.permission_mode.is_some() && request_precondition.is_none() {
        return Ok(HttpResponse::build(
            actix_web::http::StatusCode::PRECONDITION_REQUIRED,
        )
        .json(serde_json::json!({
            "error": crate::error::error_value(
                "If-Match with the current session metadata_version is required for permission_mode changes"
            ),
            "session_id": session_id,
        })));
    }
    let requested_permission_mode = req.permission_mode.or_else(|| {
        req.bypass_permissions.map(|enabled| {
            if enabled {
                bamboo_domain::SessionPermissionMode::Bypass
            } else {
                bamboo_domain::SessionPermissionMode::Default
            }
        })
    });
    // Consumed by the first authoritative setter invoked (see `.take()` below).
    let mut precondition = request_precondition;

    // Project reassignment and explicit Workspace switching are one
    // authoritative transaction. The entire validate -> mutate -> persist ->
    // cache/index/prompt/event sequence is serialized by the session lock, so
    // a combined request cannot persist either half before both validate.
    if req.project_id.is_some() || req.workspace_path.is_some() {
        let Some(expected_version) = precondition.take() else {
            return Ok(HttpResponse::build(
                actix_web::http::StatusCode::PRECONDITION_REQUIRED,
            )
            .json(serde_json::json!({
                "error": crate::error::error_value(
                    "If-Match with the current session metadata_version is required for Project or Workspace changes"
                ),
                "session_id": session_id,
            })));
        };
        let _guard = state.persistence.acquire_lock(&session_id).await;
        if is_session_running(&state, &session_id).await
            || crate::handlers::agent::events::execute_startup_is_in_flight(
                state.as_ref(),
                &session_id,
            )
        {
            return Ok(HttpResponse::Conflict().json(serde_json::json!({
                "error": {
                    "type": "api_error",
                    "code": "session_project_running_conflict",
                    "message": "A running or starting session cannot change Project or Workspace"
                },
                "session_id": session_id,
            })));
        }
        let Some(mut session) = state
            .persistence
            .storage()
            .load_session(&session_id)
            .await
            .map_err(|error| {
                crate::error::json_internal_server_error(format!(
                    "Failed to load session for Project/Workspace update: {error}"
                ))
            })?
        else {
            return Ok(HttpResponse::NotFound().json(serde_json::json!({
                "error": crate::error::error_value("Session not found"),
                "session_id": session_id,
            })));
        };

        if session.metadata_version != expected_version {
            return Ok(precondition_failed(&session_id, session.metadata_version));
        }

        let current_raw = session.project_id_meta();
        let current_project =
            match bamboo_engine::project_context::ProjectContextResolver::session_project_identity(
                &session,
            ) {
                bamboo_engine::project_context::SessionProjectIdentity::Assigned(project_id) => {
                    Some(project_id)
                }
                bamboo_engine::project_context::SessionProjectIdentity::Unassigned => None,
                bamboo_engine::project_context::SessionProjectIdentity::Invalid {
                    raw,
                    message,
                } if req.project_id.is_none() => {
                    let error =
                        bamboo_engine::project_context::ProjectContextError::InvalidProjectIdentity {
                        raw,
                        message,
                    };
                    return Ok(project_context_error_response(error));
                }
                // An explicit reassignment (including `null`) is the recovery path
                // for malformed legacy membership.
                bamboo_engine::project_context::SessionProjectIdentity::Invalid { .. } => None,
            };
        let target = match req.project_id.as_ref() {
            Some(Some(raw)) => {
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    return Ok(crate::error::json_error(
                        actix_web::http::StatusCode::BAD_REQUEST,
                        "project_id must be a non-empty opaque Project id or null",
                    ));
                }
                let project_id = match trimmed.parse::<bamboo_domain::ProjectId>() {
                    Ok(project_id) => project_id,
                    Err(_) => {
                        return Ok(crate::error::json_error(
                            actix_web::http::StatusCode::BAD_REQUEST,
                            "invalid Project id",
                        ));
                    }
                };
                let project = match state.project_store.get(&project_id) {
                    Ok(project) => project,
                    Err(bamboo_projects::ProjectStoreError::NotFound(_)) => {
                        return Ok(crate::error::json_error(
                            actix_web::http::StatusCode::NOT_FOUND,
                            "target Project not found",
                        ));
                    }
                    Err(error) => {
                        return Err(crate::error::json_internal_server_error(format!(
                            "Failed to validate target Project: {error}"
                        )));
                    }
                };
                if project.status != bamboo_domain::ProjectStatus::Active {
                    return Ok(HttpResponse::Conflict().json(serde_json::json!({
                        "error": {
                            "type": "api_error",
                            "code": "project_archived",
                            "message": "Sessions can only be assigned to an active Project"
                        },
                        "project_id": project_id,
                    })));
                }
                Some(project_id)
            }
            Some(None) => None,
            None => current_project.clone(),
        };

        let current_workspace = session.workspace_path_meta();
        let current_workspace_source = session
            .metadata
            .get(bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY)
            .cloned();
        let mut selected_project_default = false;
        let final_workspace = if let Some(requested_workspace) = req.workspace_path.as_deref() {
            match crate::project_context::validate_explicit_session_workspace_with_resolver(
                &state.project_store,
                target.as_ref(),
                requested_workspace,
                &state.workspace_resolver,
            ) {
                Ok(workspace) => Some(workspace),
                Err(error) => {
                    return Ok(crate::project_context::session_workspace_error_response(
                        error,
                    ));
                }
            }
        } else {
            let workspace_for_validation = (current_workspace_source.as_deref()
                != Some(bamboo_engine::project_context::WorkspaceSource::ProjectDefault.as_str()))
            .then_some(current_workspace.as_deref())
            .flatten();
            selected_project_default = target.is_some() && workspace_for_validation.is_none();
            match crate::project_context::validate_workspace_assignment_with_resolver(
                &state.project_store,
                target.as_ref(),
                workspace_for_validation,
                &state.workspace_resolver,
            ) {
                Ok(workspace) => workspace,
                Err(error) => {
                    return Ok(match error {
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
                            return Err(crate::error::json_internal_server_error(format!(
                                "Failed to validate workspace Project ownership: {error}"
                            )));
                        }
                    });
                }
            }
        };
        let final_workspace_display = final_workspace
            .as_deref()
            .map(bamboo_config::paths::path_to_display_string);
        let membership_changed = match target.as_ref() {
            Some(target) => current_project.as_ref() != Some(target),
            None => current_raw.is_some(),
        };
        let workspace_changed = req.workspace_path.is_some()
            && (current_workspace.as_deref() != final_workspace_display.as_deref()
                || (target.is_some()
                    && current_workspace_source.as_deref()
                        != Some(
                            bamboo_engine::project_context::WorkspaceSource::Explicit.as_str(),
                        )));
        if membership_changed || workspace_changed {
            match target.as_ref() {
                Some(project_id) if membership_changed => {
                    session.set_project_id_meta(project_id.to_string())
                }
                None if membership_changed => session.clear_project_id_meta(),
                _ => {}
            }
            if let Some(workspace) = final_workspace_display.as_deref() {
                session.set_workspace_path_meta(workspace);
                if req.workspace_path.is_some() {
                    session.metadata.insert(
                        bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY.to_string(),
                        bamboo_engine::project_context::WorkspaceSource::Explicit
                            .as_str()
                            .to_string(),
                    );
                } else if target.is_some() && selected_project_default {
                    session.metadata.insert(
                        bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY.to_string(),
                        bamboo_engine::project_context::WorkspaceSource::ProjectDefault
                            .as_str()
                            .to_string(),
                    );
                }
            }
            session.metadata_version = session.metadata_version.saturating_add(1);
            session.updated_at = chrono::Utc::now();

            // Resolve and replace the stable Project/Workspace markers through
            // the single engine resolver seam. Validation never mutates Project
            // workspace bindings.
            if let Err(error) = state
                .project_context_resolver
                .refresh_session_prompt_read_only(&mut session)
                .await
            {
                return Ok(project_context_error_response(error));
            }

            state
                .persistence
                .storage()
                .save_session(&session)
                .await
                .map_err(|error| {
                    crate::error::json_internal_server_error(format!(
                        "Failed to save Project/Workspace update: {error}"
                    ))
                })?;
            state.sessions.insert(
                session_id.clone(),
                std::sync::Arc::new(parking_lot::RwLock::new(session.clone())),
            );
            if let Some(workspace) = session
                .workspace_path_meta()
                .map(std::path::PathBuf::from)
                .and_then(|path| std::fs::canonicalize(&path).ok().or(Some(path)))
            {
                state.workspace_resolver.publish_resolved_workspace(
                    &session_id,
                    workspace,
                    "session_metadata_patch",
                );
            }
            state.account_sink.record(
                Some(&session_id),
                &bamboo_agent_core::AgentEvent::SessionProjectUpdated {
                    session_id: session_id.clone(),
                    project_id: target.as_ref().map(ToString::to_string),
                    workspace_path: session.workspace_path_meta(),
                    metadata_version: session.metadata_version,
                },
            );
        }
        // Preserve a valid CAS token for any lower-risk fields included in the
        // same PATCH. A real metadata change bumped it; an idempotent request
        // leaves it unchanged.
        precondition = Some(session.metadata_version);
    }

    if let Some(title) = req.title.as_ref() {
        match SessionMetadataService::set_title(
            state.get_ref(),
            &session_id,
            title,
            precondition.take(),
        )
        .await
        {
            Ok(_) => {}
            Err(MetadataError::NotFound(id)) => {
                return Ok(HttpResponse::NotFound().json(serde_json::json!({
                    "error": crate::error::error_value("Session not found"),
                    "session_id": id
                })));
            }
            Err(MetadataError::VersionConflict { current, .. }) => {
                return Ok(precondition_failed(&session_id, current));
            }
            Err(err) => {
                return Err(crate::error::json_internal_server_error(err.to_string()));
            }
        }
    }

    if let Some(pinned) = req.pinned {
        match SessionMetadataService::set_pinned(
            state.get_ref(),
            &session_id,
            pinned,
            precondition.take(),
        )
        .await
        {
            Ok(_) => {}
            Err(MetadataError::NotFound(id)) => {
                return Ok(HttpResponse::NotFound().json(serde_json::json!({
                    "error": crate::error::error_value("Session not found"),
                    "session_id": id
                })));
            }
            Err(MetadataError::VersionConflict { current, .. }) => {
                return Ok(precondition_failed(&session_id, current));
            }
            Err(err) => {
                return Err(crate::error::json_internal_server_error(err.to_string()));
            }
        }
    }

    if req.gold_config.is_some() {
        let gold_config_json = match req
            .gold_config
            .as_ref()
            .map(normalize_gold_config_json)
            .transpose()
        {
            Ok(value) => value,
            Err(error) => {
                return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                    "error": crate::error::error_value("Invalid gold_config"),
                    "message": error.to_string()
                })));
            }
        };
        match SessionMetadataService::set_gold_config_json(
            state.get_ref(),
            &session_id,
            gold_config_json,
            precondition.take(),
        )
        .await
        {
            Ok(_) => {}
            Err(MetadataError::NotFound(id)) => {
                return Ok(HttpResponse::NotFound().json(serde_json::json!({
                    "error": crate::error::error_value("Session not found"),
                    "session_id": id
                })));
            }
            Err(MetadataError::VersionConflict { current, .. }) => {
                return Ok(precondition_failed(&session_id, current));
            }
            Err(err) => {
                return Err(crate::error::json_internal_server_error(err.to_string()));
            }
        }
    }

    let touches_non_metadata = req.model_ref.is_some()
        || req.provider.is_some()
        || req.model.is_some()
        || req.reasoning_effort.is_some()
        || req.clear_reasoning_effort.unwrap_or(false)
        || requested_permission_mode.is_some();

    if touches_non_metadata {
        let request_model_ref = derive_model_ref(
            req.model_ref.as_ref(),
            req.provider.as_deref(),
            req.model.as_deref(),
        );

        // A typed permission write is authoritative and CAS-guarded. When an
        // earlier field in this same PATCH already consumed the caller's
        // precondition, rebase the permission write onto the exact version that
        // earlier write produced; a third-party edit in the gap then conflicts
        // instead of silently reordering the user's mode choice.
        let earlier_authoritative_field = req.project_id.is_some()
            || req.workspace_path.is_some()
            || req.title.is_some()
            || req.pinned.is_some()
            || req.gold_config.is_some();
        let permission_expected_version =
            if req.permission_mode.is_some() && earlier_authoritative_field {
                state
                    .persistence
                    .storage()
                    .load_session(&session_id)
                    .await
                    .map_err(|error| {
                        crate::error::json_internal_server_error(format!(
                            "Failed to load session revision: {error}"
                        ))
                    })?
                    .map(|session| session.metadata_version)
            } else if req.permission_mode.is_some() {
                request_precondition
            } else {
                None
            };

        // Apply ONLY the config fields after loading the freshest session under
        // the per-session lock. This cannot clobber messages appended by a
        // concurrent chat write, and the permission CAS check happens under the
        // same lock as its durable commit.
        let guard = state.persistence.acquire_lock(&session_id).await;
        let Some(mut session) = state
            .persistence
            .storage()
            .load_session(&session_id)
            .await
            .map_err(|error| {
                crate::error::json_internal_server_error(format!(
                    "Failed to load session for config update: {error}"
                ))
            })?
        else {
            return Ok(HttpResponse::NotFound().json(serde_json::json!({
                "error": crate::error::error_value("Session not found"),
                "session_id": session_id
            })));
        };
        if let Some(expected) = permission_expected_version {
            if session.metadata_version != expected {
                return Ok(precondition_failed(&session_id, session.metadata_version));
            }
        }

        let prev_model = session.model.clone();
        let prev_model_ref = session.model_ref.clone();
        let prev_reasoning = session.reasoning_effort;

        if let Some(model_ref) = request_model_ref.as_ref() {
            persist_model_ref(&mut session, model_ref);
        } else {
            persist_legacy_model_provider(
                &mut session,
                req.model.as_deref(),
                req.provider.as_deref(),
            );
        }
        if req.clear_reasoning_effort.unwrap_or(false) {
            session.reasoning_effort = None;
        } else if let Some(reasoning_effort) = req.reasoning_effort {
            session.reasoning_effort = Some(reasoning_effort);
        }

        // First-class per-session permission behavior. The typed mode and
        // legacy mirror are updated together so old clients remain
        // conservative without gaining a way to select Auto.
        let permission_transition = requested_permission_mode
            .and_then(|mode| {
                let runtime = session
                    .agent_runtime_state
                    .get_or_insert_with(bamboo_domain::AgentRuntimeState::default);
                let previous = runtime.effective_permission_mode();
                runtime.set_permission_mode(mode);
                (previous != mode).then_some((previous, mode))
            })
            .map(|(previous, effective)| (previous, effective, chrono::Utc::now().to_rfc3339()));
        if let Some((_, effective, transitioned_at)) = permission_transition.as_ref() {
            session.metadata.insert(
                "permission.requested_mode".to_string(),
                effective.as_str().to_string(),
            );
            session.metadata.insert(
                "permission.effective_mode".to_string(),
                effective.as_str().to_string(),
            );
            session.metadata.insert(
                "permission.transitioned_at".to_string(),
                transitioned_at.clone(),
            );
            session.metadata_version = session.metadata_version.saturating_add(1);
        }

        let model_changed = session.model != prev_model || session.model_ref != prev_model_ref;
        let reasoning_changed = session.reasoning_effort != prev_reasoning;
        session.updated_at = chrono::Utc::now();
        state
            .persistence
            .storage()
            .save_session(&session)
            .await
            .map_err(|error| {
                crate::error::json_internal_server_error(format!("Failed to save session: {error}"))
            })?;
        state.sessions.insert(
            session_id.clone(),
            std::sync::Arc::new(parking_lot::RwLock::new(session.clone())),
        );
        drop(guard);

        // Only worth a line when something actually changed; a no-op config
        // patch (the common case for repeated/echoed UI writes) stays quiet.
        if model_changed || reasoning_changed {
            tracing::debug!(
                "[{}] patch_session config update saved under lock: messages preserved={}, model_changed={}, reasoning_changed={}",
                session_id,
                session.messages.len(),
                model_changed,
                reasoning_changed,
            );
        }
        if let Some((previous, effective, transitioned_at)) = permission_transition {
            tracing::info!(
                telemetry_event = "session.permission_mode.transition",
                session_id = %session_id,
                previous_mode = previous.as_str(),
                requested_mode = requested_permission_mode
                    .map(bamboo_domain::SessionPermissionMode::as_str)
                    .unwrap_or("unchanged"),
                effective_mode = effective.as_str(),
                transitioned_at = %transitioned_at,
                "session permission mode changed"
            );
        }
    }

    // Advertise the new ETag (metadata_version) so clients can send it back as
    // `If-Match` on their next write.
    let etag = state
        .persistence
        .storage()
        .load_session(&session_id)
        .await
        .ok()
        .flatten()
        .map(|s| s.metadata_version);

    let mut response = get_session(state, web::Path::from(session_id)).await?;
    if let Some(version) = etag {
        if let Ok(value) = actix_web::http::header::HeaderValue::from_str(&format!("\"{version}\""))
        {
            response
                .headers_mut()
                .insert(actix_web::http::header::ETAG, value);
        }
    }
    Ok(response)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use actix_web::{http::header, http::StatusCode, test, web, App};
    use bamboo_domain::Storage as _;
    use bamboo_engine::runtime::execution::runner_state::{AgentRunner, AgentStatus};
    use serde_json::Value;
    use tempfile::tempdir;

    use crate::routes::configure_routes;
    use crate::AppState;

    async fn new_state() -> web::Data<AppState> {
        let temp_dir = tempdir().expect("tempdir").keep();
        bamboo_config::paths::init_bamboo_dir(temp_dir.clone());
        web::Data::new(AppState::new(temp_dir).await.expect("app state"))
    }

    macro_rules! create_session {
        ($app:expr) => {{
            let resp = test::call_service(
                &$app,
                test::TestRequest::post()
                    .uri("/api/v1/sessions")
                    .set_json(serde_json::json!({ "title": "Etag test" }))
                    .to_request(),
            )
            .await;
            let body: Value = test::read_body_json(resp).await;
            body["session"]["id"].as_str().unwrap().to_string()
        }};
    }

    fn display(path: &Path) -> String {
        bamboo_config::paths::path_to_display_string(
            &path.canonicalize().expect("canonical workspace"),
        )
    }

    fn binding(path: &Path) -> bamboo_domain::WorkspaceBinding {
        bamboo_domain::WorkspaceBinding {
            path: path.to_string_lossy().into_owned(),
            label: None,
            git_common_dir: None,
        }
    }

    #[actix_web::test]
    async fn permission_mode_auto_persists_and_is_indexed_distinctly() {
        let state = new_state().await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let id = create_session!(app);
        let initial_version = state
            .storage
            .load_session(&id)
            .await
            .expect("load")
            .expect("session")
            .metadata_version;

        let response = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri(&format!("/api/v1/sessions/{id}"))
                .insert_header((header::IF_MATCH, format!("\"{initial_version}\"")))
                .set_json(serde_json::json!({"permission_mode": "auto"}))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let expected_etag = format!("\"{}\"", initial_version.saturating_add(1));
        assert_eq!(
            response
                .headers()
                .get(header::ETAG)
                .and_then(|value| value.to_str().ok()),
            Some(expected_etag.as_str())
        );
        let body: Value = test::read_body_json(response).await;
        assert_eq!(body["session"]["permission_mode"], "auto");
        assert_eq!(body["session"]["bypass_permissions"], true);

        let persisted = state
            .storage
            .load_session(&id)
            .await
            .expect("load")
            .expect("session");
        assert_eq!(
            persisted.metadata.get("permission.requested_mode"),
            Some(&"auto".to_string())
        );
        assert_eq!(
            persisted.metadata.get("permission.effective_mode"),
            Some(&"auto".to_string())
        );
        assert!(persisted
            .metadata
            .get("permission.transitioned_at")
            .is_some_and(|timestamp| chrono::DateTime::parse_from_rfc3339(timestamp).is_ok()));
        let runtime = persisted.agent_runtime_state.expect("runtime state");
        assert_eq!(
            runtime.effective_permission_mode(),
            bamboo_domain::SessionPermissionMode::Auto
        );
        let indexed = state
            .session_store
            .get_index_entry(&id)
            .await
            .expect("index entry");
        assert_eq!(
            indexed.permission_mode,
            bamboo_domain::SessionPermissionMode::Auto
        );
    }

    #[actix_web::test]
    async fn typed_permission_mode_requires_cas_and_rejects_stale_reordering() {
        let state = new_state().await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let id = create_session!(app);
        let initial_version = state
            .storage
            .load_session(&id)
            .await
            .expect("load")
            .expect("session")
            .metadata_version;
        let initial_etag = format!("\"{initial_version}\"");

        let missing = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri(&format!("/api/v1/sessions/{id}"))
                .set_json(serde_json::json!({"permission_mode": "auto"}))
                .to_request(),
        )
        .await;
        assert_eq!(missing.status(), StatusCode::PRECONDITION_REQUIRED);

        let first = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri(&format!("/api/v1/sessions/{id}"))
                .insert_header((header::IF_MATCH, initial_etag.as_str()))
                .set_json(serde_json::json!({"permission_mode": "auto"}))
                .to_request(),
        )
        .await;
        assert_eq!(first.status(), StatusCode::OK);

        // A delayed request prepared from the same original snapshot must not
        // overwrite the newer Auto choice merely because it arrived later.
        let stale = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri(&format!("/api/v1/sessions/{id}"))
                .insert_header((header::IF_MATCH, initial_etag.as_str()))
                .set_json(serde_json::json!({"permission_mode": "default"}))
                .to_request(),
        )
        .await;
        assert_eq!(stale.status(), StatusCode::PRECONDITION_FAILED);
        let next_etag = format!("\"{}\"", initial_version.saturating_add(1));
        assert_eq!(
            stale
                .headers()
                .get(header::ETAG)
                .and_then(|value| value.to_str().ok()),
            Some(next_etag.as_str())
        );

        let persisted = state
            .storage
            .load_session(&id)
            .await
            .expect("load")
            .expect("session");
        assert_eq!(
            persisted.metadata_version,
            initial_version.saturating_add(1)
        );
        assert_eq!(
            persisted
                .agent_runtime_state
                .as_ref()
                .map(bamboo_domain::AgentRuntimeState::effective_permission_mode),
            Some(bamboo_domain::SessionPermissionMode::Auto)
        );
    }

    #[actix_web::test]
    async fn permission_patch_rejects_ambiguous_new_and_legacy_fields() {
        let state = new_state().await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let id = create_session!(app);

        let response = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri(&format!("/api/v1/sessions/{id}"))
                .set_json(serde_json::json!({
                    "permission_mode": "auto",
                    "bypass_permissions": true
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let persisted = state
            .storage
            .load_session(&id)
            .await
            .expect("load")
            .expect("session");
        assert!(persisted.agent_runtime_state.is_none());
    }

    async fn seed_session(
        state: &web::Data<AppState>,
        session_id: &str,
        project_id: Option<&bamboo_domain::ProjectId>,
        workspace: Option<&Path>,
    ) {
        let mut session = bamboo_agent_core::Session::new(session_id, "model");
        session.title = "Original title".to_string();
        if let Some(project_id) = project_id {
            session.set_project_id_meta(project_id.to_string());
        }
        if let Some(workspace) = workspace {
            session.set_workspace_path_meta(display(workspace));
            session.metadata.insert(
                bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY.to_string(),
                bamboo_engine::project_context::WorkspaceSource::Explicit
                    .as_str()
                    .to_string(),
            );
        }
        state
            .storage
            .save_session(&session)
            .await
            .expect("seed session");
        state.sessions.insert(
            session_id.to_string(),
            std::sync::Arc::new(parking_lot::RwLock::new(session)),
        );
    }

    #[actix_web::test]
    async fn patch_with_matching_if_match_succeeds_and_bumps_etag() {
        let state = new_state().await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let id = create_session!(app);

        // GET exposes the current ETag ("0").
        let get = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/api/v1/sessions/{id}"))
                .to_request(),
        )
        .await;
        let etag = get
            .headers()
            .get(header::ETAG)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert_eq!(etag, "\"0\"");

        // PATCH with If-Match: "0" succeeds and returns the bumped ETag.
        let patch = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri(&format!("/api/v1/sessions/{id}"))
                .insert_header((header::IF_MATCH, etag))
                .set_json(serde_json::json!({ "title": "Renamed" }))
                .to_request(),
        )
        .await;
        assert_eq!(patch.status(), StatusCode::OK);
        assert_eq!(
            patch.headers().get(header::ETAG).unwrap().to_str().unwrap(),
            "\"1\""
        );
    }

    #[actix_web::test]
    async fn patch_with_stale_if_match_returns_412() {
        let state = new_state().await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let id = create_session!(app);

        // Advance the version once (no precondition).
        let first = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri(&format!("/api/v1/sessions/{id}"))
                .set_json(serde_json::json!({ "pinned": true }))
                .to_request(),
        )
        .await;
        assert_eq!(first.status(), StatusCode::OK);

        // A stale If-Match ("0") must now be rejected with 412 + current ETag.
        let stale = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri(&format!("/api/v1/sessions/{id}"))
                .insert_header((header::IF_MATCH, "\"0\""))
                .set_json(serde_json::json!({ "title": "Should Fail" }))
                .to_request(),
        )
        .await;
        assert_eq!(stale.status(), StatusCode::PRECONDITION_FAILED);
        assert_eq!(
            stale.headers().get(header::ETAG).unwrap().to_str().unwrap(),
            "\"1\""
        );
        let body: Value = test::read_body_json(stale).await;
        assert_eq!(body["current_version"], 1);
    }

    #[actix_web::test]
    async fn explicit_null_clears_malformed_legacy_project_membership() {
        let state = new_state().await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let id = create_session!(app);

        let mut session = state
            .storage
            .load_session(&id)
            .await
            .expect("load session")
            .expect("session exists");
        session.set_project_id_meta("../malformed");
        state
            .storage
            .save_session(&session)
            .await
            .expect("persist malformed legacy Project id");

        let response = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri(&format!("/api/v1/sessions/{id}"))
                .insert_header((header::IF_MATCH, "\"0\""))
                .set_json(serde_json::json!({ "project_id": null }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        let reloaded = state
            .storage
            .load_session(&id)
            .await
            .expect("reload session")
            .expect("session exists");
        assert!(
            reloaded.project_id_meta().is_none(),
            "explicit null must clear even a malformed legacy value"
        );
        assert_eq!(reloaded.metadata_version, 1);
    }

    #[actix_web::test]
    async fn project_reassignment_rejects_cross_project_workspace_without_persistence() {
        let state = new_state().await;
        let workspace = tempdir().expect("workspace");
        let owner = state
            .project_store
            .create_with_bindings(
                "Workspace Owner",
                None,
                vec![bamboo_domain::WorkspaceBinding {
                    path: workspace.path().to_string_lossy().into_owned(),
                    label: None,
                    git_common_dir: None,
                }],
            )
            .expect("workspace owner");
        let target = state
            .project_store
            .create("Target Project", None)
            .expect("target Project");
        let mut session = bamboo_agent_core::Session::new("project-reassign-conflict", "model");
        session.title = "Original title".to_string();
        session.set_project_id_meta(owner.id.to_string());
        session.set_workspace_path_meta(workspace.path().to_string_lossy().into_owned());
        session.messages.push(bamboo_agent_core::Message::system(
            "ORIGINAL PROJECT PROMPT",
        ));
        let original_version = session.metadata_version;
        let original_prompt = session.messages[0].content.clone();
        let original_workspace = session.workspace_path_meta();
        state
            .storage
            .save_session(&session)
            .await
            .expect("persist session");
        state.sessions.insert(
            session.id.clone(),
            std::sync::Arc::new(parking_lot::RwLock::new(session)),
        );
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri("/api/v1/sessions/project-reassign-conflict")
                .insert_header((header::IF_MATCH, format!("\"{original_version}\"")))
                .set_json(serde_json::json!({
                    "project_id": target.id,
                    "title": "Must not persist"
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body: Value = test::read_body_json(response).await;
        assert_eq!(body["error"]["code"], "project_workspace_conflict");
        assert_eq!(body["owner_project_id"], owner.id.to_string());

        let reloaded = state
            .storage
            .load_session("project-reassign-conflict")
            .await
            .expect("reload")
            .expect("session exists");
        assert_eq!(
            reloaded.project_id_meta().as_deref(),
            Some(owner.id.as_str())
        );
        assert_eq!(reloaded.metadata_version, original_version);
        assert_eq!(reloaded.title, "Original title");
        assert_eq!(reloaded.workspace_path_meta(), original_workspace);
        assert_eq!(reloaded.messages[0].content, original_prompt);
    }

    #[actix_web::test]
    async fn project_reassignment_uses_target_path_but_unassignment_still_checks_global_default() {
        let state = new_state().await;
        let workspace = tempdir().expect("default workspace");
        let target_path = tempdir().expect("target Project path");
        let owner = state
            .project_store
            .create_with_bindings(
                "Default Owner",
                None,
                vec![bamboo_domain::WorkspaceBinding {
                    path: workspace.path().to_string_lossy().into_owned(),
                    label: None,
                    git_common_dir: None,
                }],
            )
            .expect("owner Project");
        let target = state
            .project_store
            .create_with_project_path(
                "Other Project",
                None,
                target_path.path().to_string_lossy(),
                Vec::new(),
            )
            .expect("target Project");
        state.config.write().await.default_work_area = Some(bamboo_config::DefaultWorkAreaConfig {
            path: Some(workspace.path().to_string_lossy().into_owned()),
        });

        let unassigned =
            bamboo_agent_core::Session::new("project-reassign-default-assigned", "model");
        state.storage.save_session(&unassigned).await.unwrap();
        let mut assigned =
            bamboo_agent_core::Session::new("project-reassign-default-unassigned", "model");
        assigned.set_project_id_meta(owner.id.to_string());
        state.storage.save_session(&assigned).await.unwrap();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri(&format!("/api/v1/sessions/{}", unassigned.id))
                .insert_header((header::IF_MATCH, "\"0\""))
                .set_json(serde_json::json!({"project_id": target.id}))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let persisted = state
            .storage
            .load_session(&unassigned.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            persisted.project_id_meta().as_deref(),
            Some(target.id.as_str())
        );
        assert_eq!(
            persisted.workspace_path_meta().as_deref(),
            target.project_path.as_deref()
        );
        assert_eq!(
            persisted
                .metadata
                .get(bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY)
                .map(String::as_str),
            Some("project_default")
        );

        let response = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri(&format!("/api/v1/sessions/{}", assigned.id))
                .insert_header((header::IF_MATCH, "\"0\""))
                .set_json(serde_json::json!({"project_id": null}))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body: Value = test::read_body_json(response).await;
        assert_eq!(body["error"]["code"], "project_workspace_conflict");
        assert_eq!(body["owner_project_id"], owner.id.as_str());
        let persisted = state
            .storage
            .load_session(&assigned.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            persisted.project_id_meta().as_deref(),
            Some(owner.id.as_str())
        );
        assert_eq!(persisted.metadata_version, 0);
        assert!(persisted.workspace_path_meta().is_none());
    }

    #[actix_web::test]
    async fn project_unassignment_rejects_cross_project_runtime_workspace_without_persistence() {
        let state = new_state().await;
        let workspace = tempdir().expect("runtime workspace");
        let owner = state
            .project_store
            .create_with_bindings(
                "Runtime Workspace Owner",
                None,
                vec![bamboo_domain::WorkspaceBinding {
                    path: workspace.path().to_string_lossy().into_owned(),
                    label: None,
                    git_common_dir: None,
                }],
            )
            .expect("workspace owner");
        let mut session =
            bamboo_agent_core::Session::new("project-reassign-runtime-workspace", "model");
        session.title = "Original title".to_string();
        session.set_project_id_meta(owner.id.to_string());
        state
            .storage
            .save_session(&session)
            .await
            .expect("persist session without workspace metadata");
        let published = bamboo_agent_core::workspace_state::publish_resolved_workspace(
            &session.id,
            workspace.path().to_path_buf(),
        );
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri("/api/v1/sessions/project-reassign-runtime-workspace")
                .insert_header((header::IF_MATCH, "\"0\""))
                .set_json(serde_json::json!({
                    "project_id": null,
                    "title": "Must not persist"
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body: Value = test::read_body_json(response).await;
        assert_eq!(body["error"]["code"], "project_workspace_conflict");
        assert_eq!(body["owner_project_id"], owner.id.as_str());

        let persisted = state
            .storage
            .load_session("project-reassign-runtime-workspace")
            .await
            .expect("load session")
            .expect("session exists");
        assert_eq!(
            persisted.project_id_meta().as_deref(),
            Some(owner.id.as_str())
        );
        assert_eq!(persisted.metadata_version, 0);
        assert_eq!(persisted.title, "Original title");
        assert!(persisted.workspace_path_meta().is_none());
        assert_eq!(
            bamboo_agent_core::workspace_state::peek_workspace(
                "project-reassign-runtime-workspace"
            )
            .as_deref(),
            Some(published.as_path())
        );
    }

    #[actix_web::test]
    async fn successful_project_reassignment_publishes_replayable_event_and_survives_rebuild() {
        let state = new_state().await;
        let project = state
            .project_store
            .create("Assigned Project", None)
            .expect("Project");
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let session_id = create_session!(app);
        let mut feed = state.account_sink.subscribe();

        let response = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri(&format!("/api/v1/sessions/{session_id}"))
                .insert_header((header::IF_MATCH, "\"0\""))
                .set_json(serde_json::json!({ "project_id": project.id }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        let persisted = state
            .storage
            .load_session(&session_id)
            .await
            .expect("load")
            .expect("session exists");
        let indexed = state
            .session_store
            .get_index_entry(&session_id)
            .await
            .expect("index entry");
        assert_eq!(
            persisted.project_id_meta().as_deref(),
            Some(project.id.as_str())
        );
        assert_eq!(indexed.project_id.as_deref(), Some(project.id.as_str()));

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let event = feed.recv().await.expect("Project event");
                if matches!(
                    &event.event,
                    bamboo_agent_core::AgentEvent::SessionProjectUpdated {
                        session_id: event_session_id,
                        project_id: Some(event_project_id),
                        metadata_version,
                        ..
                    } if event_session_id == session_id.as_str()
                        && event_project_id == project.id.as_str()
                        && *metadata_version == persisted.metadata_version
                ) {
                    break;
                }
            }
        })
        .await
        .expect("Project event timeout");

        tokio::fs::write(state.app_data_dir.join("sessions.json"), b"{corrupt")
            .await
            .expect("corrupt rebuildable index");
        let rebuilt = bamboo_storage::SessionStoreV2::new(state.app_data_dir.clone())
            .await
            .expect("rebuild session store");
        let rebuilt_entry = rebuilt
            .get_index_entry(&session_id)
            .await
            .expect("rebuilt entry");
        assert_eq!(
            rebuilt_entry.project_id.as_deref(),
            Some(project.id.as_str())
        );
    }

    #[actix_web::test]
    async fn project_reassignment_rejects_execute_startup_before_runner_reservation() {
        let state = new_state().await;
        let original = state
            .project_store
            .create("Original Project", None)
            .expect("original Project");
        let target = state
            .project_store
            .create("Target Project", None)
            .expect("target Project");
        let mut session = bamboo_agent_core::Session::new("project-reassign-starting", "model");
        session.set_project_id_meta(original.id.to_string());
        state
            .storage
            .save_session(&session)
            .await
            .expect("persist session");

        // Reproduce the precise execute startup window deterministically:
        // /execute acquires the persistence lock, registers startup ownership,
        // then releases the lock before the runner reservation appears.
        let persistence_guard = state.persistence.acquire_lock(&session.id).await;
        let startup_guard =
            crate::handlers::agent::events::begin_execute_startup(state.as_ref(), &session.id);
        drop(persistence_guard);

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri("/api/v1/sessions/project-reassign-starting")
                .insert_header((header::IF_MATCH, "\"0\""))
                .set_json(serde_json::json!({ "project_id": target.id }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body: Value = test::read_body_json(response).await;
        assert_eq!(body["error"]["code"], "session_project_running_conflict");

        let persisted = state
            .storage
            .load_session("project-reassign-starting")
            .await
            .expect("load")
            .expect("session");
        assert_eq!(
            persisted.project_id_meta().as_deref(),
            Some(original.id.as_str())
        );
        assert_eq!(persisted.metadata_version, 0);
        drop(startup_guard);
    }

    #[actix_web::test]
    async fn workspace_patch_persists_assigned_bound_switch_across_get_list_restart_and_event() {
        let state = new_state().await;
        let first = tempdir().expect("first workspace");
        let second = tempdir().expect("second workspace");
        let project = state
            .project_store
            .create_with_project_path(
                "Multi-workspace Project",
                None,
                first.path().to_string_lossy(),
                vec![binding(second.path())],
            )
            .expect("Project");
        let session_id = "workspace-patch-assigned";
        seed_session(&state, session_id, Some(&project.id), Some(first.path())).await;
        let mut feed = state.account_sink.subscribe();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri(&format!("/api/v1/sessions/{session_id}"))
                .insert_header((header::IF_MATCH, "\"0\""))
                .set_json(serde_json::json!({
                    "workspace_path": second.path()
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get(header::ETAG).unwrap(), "\"1\"");
        let response_body: Value = test::read_body_json(response).await;
        let expected_workspace = display(second.path());
        assert_eq!(response_body["session"]["project_id"], project.id.as_str());
        assert_eq!(
            response_body["session"]["workspace_path"],
            expected_workspace.as_str()
        );

        let persisted = state
            .storage
            .load_session(session_id)
            .await
            .expect("load")
            .expect("session");
        assert_eq!(
            persisted.project_id_meta().as_deref(),
            Some(project.id.as_str()),
            "ordinary Workspace switching must not change Project membership"
        );
        assert_eq!(
            persisted.workspace_path_meta().as_deref(),
            Some(expected_workspace.as_str())
        );
        assert_eq!(persisted.metadata_version, 1);
        assert_eq!(
            persisted
                .metadata
                .get(bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY)
                .map(String::as_str),
            Some("explicit")
        );

        let index_entry = state
            .session_store
            .get_index_entry(session_id)
            .await
            .expect("index entry");
        assert_eq!(
            index_entry.workspace_path.as_deref(),
            Some(expected_workspace.as_str())
        );
        assert_eq!(index_entry.project_id.as_deref(), Some(project.id.as_str()));

        let get = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/api/v1/sessions/{session_id}"))
                .to_request(),
        )
        .await;
        let get_body: Value = test::read_body_json(get).await;
        assert_eq!(
            get_body["session"]["workspace_path"],
            expected_workspace.as_str()
        );
        assert_eq!(get_body["session"]["project_id"], project.id.as_str());

        let list = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/sessions")
                .to_request(),
        )
        .await;
        let list_body: Value = test::read_body_json(list).await;
        let listed = list_body["sessions"]
            .as_array()
            .expect("sessions")
            .iter()
            .find(|entry| entry["id"] == session_id)
            .expect("listed session");
        assert_eq!(listed["workspace_path"], expected_workspace.as_str());
        assert_eq!(listed["project_id"], project.id.as_str());

        let event = tokio::time::timeout(std::time::Duration::from_secs(1), feed.recv())
            .await
            .expect("workspace event timeout")
            .expect("workspace event");
        assert!(matches!(
            &event.event,
            bamboo_agent_core::AgentEvent::SessionProjectUpdated {
                session_id: event_session_id,
                project_id: Some(event_project_id),
                workspace_path: Some(event_workspace),
                metadata_version: 1,
            } if event_session_id == session_id
                && event_project_id == project.id.as_str()
                && event_workspace == &expected_workspace
        ));

        let restarted = bamboo_storage::SessionStoreV2::new(state.app_data_dir.clone())
            .await
            .expect("restart session store");
        let restarted_session = restarted
            .load_session(session_id)
            .await
            .expect("restart load")
            .expect("restart session");
        assert_eq!(
            restarted_session.workspace_path_meta().as_deref(),
            Some(expected_workspace.as_str())
        );
        assert_eq!(
            restarted
                .get_index_entry(session_id)
                .await
                .expect("restart index")
                .workspace_path
                .as_deref(),
            Some(expected_workspace.as_str())
        );
    }

    #[actix_web::test]
    async fn workspace_patch_allows_unassigned_unbound_path_without_assigning_project() {
        let state = new_state().await;
        let first = tempdir().expect("first workspace");
        let second = tempdir().expect("second workspace");
        let session_id = "workspace-patch-unassigned";
        seed_session(&state, session_id, None, Some(first.path())).await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri(&format!("/api/v1/sessions/{session_id}"))
                .insert_header((header::IF_MATCH, "\"0\""))
                .set_json(serde_json::json!({ "workspace_path": second.path() }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = test::read_body_json(response).await;
        assert!(body["session"]["project_id"].is_null());
        assert_eq!(body["session"]["workspace_path"], display(second.path()));

        let persisted = state
            .storage
            .load_session(session_id)
            .await
            .unwrap()
            .unwrap();
        assert!(persisted.project_id_meta().is_none());
        assert_eq!(
            persisted.workspace_path_meta().as_deref(),
            Some(display(second.path()).as_str())
        );
        assert_eq!(persisted.metadata_version, 1);
    }

    #[actix_web::test]
    async fn workspace_patch_rejects_cross_project_path_without_partial_update() {
        let state = new_state().await;
        let own_workspace = tempdir().expect("own workspace");
        let foreign_workspace = tempdir().expect("foreign workspace");
        let own = state
            .project_store
            .create_with_project_path(
                "Own Project",
                None,
                own_workspace.path().to_string_lossy(),
                Vec::new(),
            )
            .expect("own Project");
        let foreign = state
            .project_store
            .create_with_project_path(
                "Foreign Project",
                None,
                foreign_workspace.path().to_string_lossy(),
                Vec::new(),
            )
            .expect("foreign Project");
        let session_id = "workspace-patch-foreign";
        seed_session(
            &state,
            session_id,
            Some(&own.id),
            Some(own_workspace.path()),
        )
        .await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri(&format!("/api/v1/sessions/{session_id}"))
                .insert_header((header::IF_MATCH, "\"0\""))
                .set_json(serde_json::json!({
                    "workspace_path": foreign_workspace.path(),
                    "title": "Must not persist"
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body: Value = test::read_body_json(response).await;
        assert_eq!(body["error"]["code"], "project_workspace_conflict");
        assert_eq!(body["owner_project_id"], foreign.id.as_str());
        assert_eq!(body["session_project_id"], own.id.as_str());

        let persisted = state
            .storage
            .load_session(session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            persisted.project_id_meta().as_deref(),
            Some(own.id.as_str())
        );
        assert_eq!(
            persisted.workspace_path_meta().as_deref(),
            Some(display(own_workspace.path()).as_str())
        );
        assert_eq!(persisted.title, "Original title");
        assert_eq!(persisted.metadata_version, 0);
    }

    #[actix_web::test]
    async fn workspace_patch_rejects_archived_running_invalid_and_unbound_targets() {
        // Archived Project.
        let archived_state = new_state().await;
        let archived_workspace = tempdir().expect("archived workspace");
        let archived_switch = tempdir().expect("archived switch");
        let archived = archived_state
            .project_store
            .create_with_project_path(
                "Archived Project",
                None,
                archived_workspace.path().to_string_lossy(),
                vec![binding(archived_switch.path())],
            )
            .expect("archived Project");
        archived_state
            .project_store
            .archive(&archived.id, archived.revision)
            .expect("archive Project");
        seed_session(
            &archived_state,
            "workspace-patch-archived",
            Some(&archived.id),
            Some(archived_workspace.path()),
        )
        .await;
        let archived_app = test::init_service(
            App::new()
                .app_data(archived_state.clone())
                .configure(configure_routes),
        )
        .await;
        let archived_response = test::call_service(
            &archived_app,
            test::TestRequest::patch()
                .uri("/api/v1/sessions/workspace-patch-archived")
                .insert_header((header::IF_MATCH, "\"0\""))
                .set_json(serde_json::json!({
                    "workspace_path": archived_switch.path()
                }))
                .to_request(),
        )
        .await;
        assert_eq!(archived_response.status(), StatusCode::CONFLICT);
        let archived_body: Value = test::read_body_json(archived_response).await;
        assert_eq!(archived_body["error"]["code"], "project_archived");

        // Running session.
        let running_state = new_state().await;
        let running_workspace = tempdir().expect("running workspace");
        let running_switch = tempdir().expect("running switch");
        let running_project = running_state
            .project_store
            .create_with_project_path(
                "Running Project",
                None,
                running_workspace.path().to_string_lossy(),
                vec![binding(running_switch.path())],
            )
            .expect("running Project");
        let running_id = "workspace-patch-running";
        seed_session(
            &running_state,
            running_id,
            Some(&running_project.id),
            Some(running_workspace.path()),
        )
        .await;
        let mut runner = AgentRunner::new();
        runner.status = AgentStatus::Running;
        running_state
            .agent_runners
            .write()
            .await
            .insert(running_id.to_string(), runner);
        let running_app = test::init_service(
            App::new()
                .app_data(running_state.clone())
                .configure(configure_routes),
        )
        .await;
        let running_response = test::call_service(
            &running_app,
            test::TestRequest::patch()
                .uri(&format!("/api/v1/sessions/{running_id}"))
                .insert_header((header::IF_MATCH, "\"0\""))
                .set_json(serde_json::json!({ "workspace_path": running_switch.path() }))
                .to_request(),
        )
        .await;
        assert_eq!(running_response.status(), StatusCode::CONFLICT);
        let running_body: Value = test::read_body_json(running_response).await;
        assert_eq!(
            running_body["error"]["code"],
            "session_project_running_conflict"
        );

        // Invalid and unbound paths.
        let validation_state = new_state().await;
        let bound_workspace = tempdir().expect("bound workspace");
        let unbound_workspace = tempdir().expect("unbound workspace");
        let validation_project = validation_state
            .project_store
            .create_with_project_path(
                "Validation Project",
                None,
                bound_workspace.path().to_string_lossy(),
                Vec::new(),
            )
            .expect("validation Project");
        let validation_id = "workspace-patch-validation";
        seed_session(
            &validation_state,
            validation_id,
            Some(&validation_project.id),
            Some(bound_workspace.path()),
        )
        .await;
        let validation_app = test::init_service(
            App::new()
                .app_data(validation_state.clone())
                .configure(configure_routes),
        )
        .await;
        let missing = bound_workspace.path().join("missing");
        let invalid_response = test::call_service(
            &validation_app,
            test::TestRequest::patch()
                .uri(&format!("/api/v1/sessions/{validation_id}"))
                .insert_header((header::IF_MATCH, "\"0\""))
                .set_json(serde_json::json!({ "workspace_path": missing }))
                .to_request(),
        )
        .await;
        assert_eq!(invalid_response.status(), StatusCode::BAD_REQUEST);
        let invalid_body: Value = test::read_body_json(invalid_response).await;
        assert_eq!(invalid_body["error"]["code"], "workspace_invalid");

        let unbound_response = test::call_service(
            &validation_app,
            test::TestRequest::patch()
                .uri(&format!("/api/v1/sessions/{validation_id}"))
                .insert_header((header::IF_MATCH, "\"0\""))
                .set_json(serde_json::json!({
                    "workspace_path": unbound_workspace.path()
                }))
                .to_request(),
        )
        .await;
        assert_eq!(unbound_response.status(), StatusCode::CONFLICT);
        let unbound_body: Value = test::read_body_json(unbound_response).await;
        assert_eq!(unbound_body["error"]["code"], "project_workspace_unbound");
        assert_eq!(
            unbound_body["session_project_id"],
            validation_project.id.as_str()
        );

        for (state, session_id, expected_workspace) in [
            (
                &archived_state,
                "workspace-patch-archived",
                archived_workspace.path(),
            ),
            (&running_state, running_id, running_workspace.path()),
            (&validation_state, validation_id, bound_workspace.path()),
        ] {
            let persisted = state
                .storage
                .load_session(session_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                persisted.workspace_path_meta().as_deref(),
                Some(display(expected_workspace).as_str())
            );
            assert_eq!(persisted.metadata_version, 0);
        }
    }

    #[actix_web::test]
    async fn combined_project_workspace_patch_validates_atomically_against_target_project() {
        let state = new_state().await;
        let original_workspace = tempdir().expect("original workspace");
        let target_workspace = tempdir().expect("target workspace");
        let unbound_workspace = tempdir().expect("unbound workspace");
        let original = state
            .project_store
            .create_with_project_path(
                "Original Project",
                None,
                original_workspace.path().to_string_lossy(),
                Vec::new(),
            )
            .expect("original Project");
        let target = state
            .project_store
            .create_with_project_path(
                "Target Project",
                None,
                target_workspace.path().to_string_lossy(),
                Vec::new(),
            )
            .expect("target Project");
        let session_id = "workspace-patch-combined";
        seed_session(
            &state,
            session_id,
            Some(&original.id),
            Some(original_workspace.path()),
        )
        .await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let rejected = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri(&format!("/api/v1/sessions/{session_id}"))
                .insert_header((header::IF_MATCH, "\"0\""))
                .set_json(serde_json::json!({
                    "project_id": target.id,
                    "workspace_path": unbound_workspace.path(),
                    "title": "Must not persist"
                }))
                .to_request(),
        )
        .await;
        assert_eq!(rejected.status(), StatusCode::CONFLICT);
        let rejected_body: Value = test::read_body_json(rejected).await;
        assert_eq!(rejected_body["error"]["code"], "project_workspace_unbound");
        assert_eq!(
            rejected_body["session_project_id"],
            target.id.as_str(),
            "combined validation must use the target Project"
        );
        let unchanged = state
            .storage
            .load_session(session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            unchanged.project_id_meta().as_deref(),
            Some(original.id.as_str())
        );
        assert_eq!(
            unchanged.workspace_path_meta().as_deref(),
            Some(display(original_workspace.path()).as_str())
        );
        assert_eq!(unchanged.title, "Original title");
        assert_eq!(unchanged.metadata_version, 0);

        let accepted = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri(&format!("/api/v1/sessions/{session_id}"))
                .insert_header((header::IF_MATCH, "\"0\""))
                .set_json(serde_json::json!({
                    "project_id": target.id,
                    "workspace_path": target_workspace.path()
                }))
                .to_request(),
        )
        .await;
        assert_eq!(accepted.status(), StatusCode::OK);
        let changed = state
            .storage
            .load_session(session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            changed.project_id_meta().as_deref(),
            Some(target.id.as_str())
        );
        assert_eq!(
            changed.workspace_path_meta().as_deref(),
            Some(display(target_workspace.path()).as_str())
        );
        assert_eq!(changed.metadata_version, 1);
    }

    #[actix_web::test]
    async fn workspace_patch_requires_if_match_and_rejects_stale_version() {
        let state = new_state().await;
        let first = tempdir().expect("first workspace");
        let second = tempdir().expect("second workspace");
        let project = state
            .project_store
            .create_with_project_path(
                "CAS Project",
                None,
                first.path().to_string_lossy(),
                vec![binding(second.path())],
            )
            .expect("Project");
        let session_id = "workspace-patch-cas";
        seed_session(&state, session_id, Some(&project.id), Some(first.path())).await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let missing = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri(&format!("/api/v1/sessions/{session_id}"))
                .set_json(serde_json::json!({ "workspace_path": second.path() }))
                .to_request(),
        )
        .await;
        assert_eq!(
            missing.status(),
            actix_web::http::StatusCode::PRECONDITION_REQUIRED
        );
        assert_eq!(
            state
                .storage
                .load_session(session_id)
                .await
                .unwrap()
                .unwrap()
                .workspace_path_meta()
                .as_deref(),
            Some(display(first.path()).as_str())
        );

        let accepted = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri(&format!("/api/v1/sessions/{session_id}"))
                .insert_header((header::IF_MATCH, "\"0\""))
                .set_json(serde_json::json!({ "workspace_path": second.path() }))
                .to_request(),
        )
        .await;
        assert_eq!(accepted.status(), StatusCode::OK);

        let stale = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri(&format!("/api/v1/sessions/{session_id}"))
                .insert_header((header::IF_MATCH, "\"0\""))
                .set_json(serde_json::json!({ "workspace_path": first.path() }))
                .to_request(),
        )
        .await;
        assert_eq!(stale.status(), StatusCode::PRECONDITION_FAILED);
        assert_eq!(stale.headers().get(header::ETAG).unwrap(), "\"1\"");
        let stale_body: Value = test::read_body_json(stale).await;
        assert_eq!(stale_body["current_version"], 1);
        let persisted = state
            .storage
            .load_session(session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            persisted.workspace_path_meta().as_deref(),
            Some(display(second.path()).as_str())
        );
        assert_eq!(persisted.metadata_version, 1);
    }
}
