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
/// Project reassignment is deliberately performed first and requires an
/// explicit precondition so a mixed-field request cannot consume the caller's
/// CAS token on a lower-risk title/pin update.
pub async fn patch_session(
    state: web::Data<AppState>,
    path: web::Path<String>,
    http_req: HttpRequest,
    req: web::Json<PatchSessionRequest>,
) -> Result<HttpResponse> {
    let session_id = path.into_inner();
    // Consumed by the first authoritative setter invoked (see `.take()` below).
    let mut precondition = parse_if_match(&http_req);

    // Project reassignment is an explicit authoritative operation, distinct
    // from workspace changes. The entire validate -> mutate -> persist ->
    // cache/index/prompt/event sequence is serialized by the session lock.
    if let Some(requested_project) = req.project_id.as_ref() {
        let Some(expected_version) = precondition.take() else {
            return Ok(HttpResponse::build(
                actix_web::http::StatusCode::PRECONDITION_REQUIRED,
            )
            .json(serde_json::json!({
                "error": crate::error::error_value(
                    "If-Match with the current session metadata_version is required for Project reassignment"
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
                    "message": "A running or starting session cannot be reassigned to another Project"
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
                    "Failed to load session for Project reassignment: {error}"
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

        let target = match requested_project {
            Some(raw) => {
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
            None => None,
        };
        let workspace_for_validation = (session
            .metadata
            .get(bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY)
            .map(String::as_str)
            != Some(bamboo_engine::project_context::WorkspaceSource::ProjectDefault.as_str()))
        .then(|| session.workspace_path_meta())
        .flatten();
        let final_workspace =
            match crate::project_context::validate_workspace_assignment_with_resolver(
                &state.project_store,
                target.as_ref(),
                workspace_for_validation.as_deref(),
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
            };

        let current_raw = session.project_id_meta();
        let current = current_raw
            .as_deref()
            .and_then(|value| value.trim().parse::<bamboo_domain::ProjectId>().ok());
        let membership_changed = match target.as_ref() {
            Some(target) => current.as_ref() != Some(target),
            None => current_raw.is_some(),
        };
        if membership_changed {
            match target.as_ref() {
                Some(project_id) => session.set_project_id_meta(project_id.to_string()),
                None => session.clear_project_id_meta(),
            }
            if let Some(workspace) = final_workspace.as_deref() {
                session.set_workspace_path_meta(bamboo_config::paths::path_to_display_string(
                    workspace,
                ));
                if target.is_some() && workspace_for_validation.is_none() {
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

            // Resolve and replace the stable Project marker through the single
            // engine resolver seam. Workspace membership is not changed.
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
                        "Failed to save Project reassignment: {error}"
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
                    "project_reassignment",
                );
            }
            state.account_sink.record(
                Some(&session_id),
                &bamboo_agent_core::AgentEvent::SessionProjectUpdated {
                    session_id: session_id.clone(),
                    project_id: target.as_ref().map(ToString::to_string),
                    metadata_version: session.metadata_version,
                },
            );
        }
        // Preserve a valid CAS token for any lower-risk fields included in the
        // same PATCH. A real reassignment bumped it; an idempotent reassignment
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
        || req.bypass_permissions.is_some();

    if touches_non_metadata {
        let request_model_ref = derive_model_ref(
            req.model_ref.as_ref(),
            req.provider.as_deref(),
            req.model.as_deref(),
        );

        // Tracks whether the locked mutation actually changed model/reasoning,
        // so the log below reports real diffs (not merely "a field was present
        // in the request"). `Cell` is fine: the closure runs synchronously.
        let model_changed = std::cell::Cell::new(false);
        let reasoning_changed = std::cell::Cell::new(false);

        // Apply ONLY the config fields, loading the freshest session under the
        // per-session lock. This must never rewrite `messages`: a config patch
        // (e.g. model/reasoning-effort) can race a concurrent `POST /chat` that
        // just appended a user message, and a full-session save from a stale
        // snapshot would silently revert that append (lost-write bug).
        let updated = state
            .persistence
            .update_runtime_config(&session_id, |session| {
                let prev_model = session.model.clone();
                let prev_model_ref = session.model_ref.clone();
                let prev_reasoning = session.reasoning_effort;

                if let Some(model_ref) = request_model_ref.as_ref() {
                    persist_model_ref(session, model_ref);
                } else {
                    persist_legacy_model_provider(
                        session,
                        req.model.as_deref(),
                        req.provider.as_deref(),
                    );
                }
                if req.clear_reasoning_effort.unwrap_or(false) {
                    session.reasoning_effort = None;
                } else if let Some(reasoning_effort) = req.reasoning_effort {
                    session.reasoning_effort = Some(reasoning_effort);
                }

                // Per-session "bypass permissions" toggle. Stored on the session's
                // runtime state (runtime.json), creating it on demand so the flag
                // can be set before the session's first run.
                if let Some(bypass) = req.bypass_permissions {
                    session
                        .agent_runtime_state
                        .get_or_insert_with(bamboo_domain::AgentRuntimeState::default)
                        .bypass_permissions = bypass;
                }

                model_changed
                    .set(session.model != prev_model || session.model_ref != prev_model_ref);
                reasoning_changed.set(session.reasoning_effort != prev_reasoning);
                session.updated_at = chrono::Utc::now();
            })
            .await
            .map_err(|error| {
                crate::error::json_internal_server_error(format!("Failed to save session: {error}"))
            })?;

        let Some(session) = updated else {
            return Ok(HttpResponse::NotFound().json(serde_json::json!({
                "error": crate::error::error_value("Session not found"),
                "session_id": session_id
            })));
        };

        // Only worth a line when something actually changed; a no-op config
        // patch (the common case for repeated/echoed UI writes) stays quiet.
        if model_changed.get() || reasoning_changed.get() {
            tracing::debug!(
                "[{}] patch_session config update saved under lock: messages preserved={}, model_changed={}, reasoning_changed={}",
                session_id,
                session.messages.len(),
                model_changed.get(),
                reasoning_changed.get(),
            );
        }

        state.sessions.insert(
            session_id.clone(),
            std::sync::Arc::new(parking_lot::RwLock::new(session)),
        );
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
    use actix_web::{http::header, http::StatusCode, test, web, App};
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
}
