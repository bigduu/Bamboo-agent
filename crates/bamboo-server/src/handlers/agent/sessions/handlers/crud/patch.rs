use actix_web::{web, HttpRequest, HttpResponse, Result};

use crate::app_state::AppState;
use bamboo_engine::model_config_helper::normalize_gold_config_json;
use crate::session_app::metadata::{MetadataError, SessionMetadataService};
use crate::session_app::provider_model::{
    derive_model_ref, persist_legacy_model_provider, persist_model_ref,
};

use super::super::super::types::PatchSessionRequest;
use super::query::get_session;

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
            "error": "Version conflict: the session was modified by another client",
            "session_id": session_id,
            "current_version": current,
        }))
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
/// the first authoritative write in the patch (each write bumps the version),
/// matching single-field PATCH usage.
pub async fn patch_session(
    state: web::Data<AppState>,
    path: web::Path<String>,
    http_req: HttpRequest,
    req: web::Json<PatchSessionRequest>,
) -> Result<HttpResponse> {
    let session_id = path.into_inner();
    // Consumed by the first authoritative setter invoked (see `.take()` below).
    let mut precondition = parse_if_match(&http_req);

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
                    "error": "Session not found",
                    "session_id": id
                })));
            }
            Err(MetadataError::VersionConflict { current, .. }) => {
                return Ok(precondition_failed(&session_id, current));
            }
            Err(err) => {
                return Err(actix_web::error::ErrorInternalServerError(err.to_string()));
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
                    "error": "Session not found",
                    "session_id": id
                })));
            }
            Err(MetadataError::VersionConflict { current, .. }) => {
                return Ok(precondition_failed(&session_id, current));
            }
            Err(err) => {
                return Err(actix_web::error::ErrorInternalServerError(err.to_string()));
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
                    "error": "Invalid gold_config",
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
                    "error": "Session not found",
                    "session_id": id
                })));
            }
            Err(MetadataError::VersionConflict { current, .. }) => {
                return Ok(precondition_failed(&session_id, current));
            }
            Err(err) => {
                return Err(actix_web::error::ErrorInternalServerError(err.to_string()));
            }
        }
    }

    let touches_non_metadata = req.model_ref.is_some()
        || req.provider.is_some()
        || req.model.is_some()
        || req.reasoning_effort.is_some()
        || req.clear_reasoning_effort.unwrap_or(false);

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

                model_changed
                    .set(session.model != prev_model || session.model_ref != prev_model_ref);
                reasoning_changed.set(session.reasoning_effort != prev_reasoning);
                session.updated_at = chrono::Utc::now();
            })
            .await
            .map_err(|error| {
                actix_web::error::ErrorInternalServerError(format!(
                    "Failed to save session: {error}"
                ))
            })?;

        let Some(session) = updated else {
            return Ok(HttpResponse::NotFound().json(serde_json::json!({
                "error": "Session not found",
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

        let mut sessions = state.sessions.write().await;
        sessions.insert(session_id.clone(), session);
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
        if let Ok(value) =
            actix_web::http::header::HeaderValue::from_str(&format!("\"{version}\""))
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
        let temp_dir = tempdir().expect("tempdir");
        bamboo_infrastructure::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        web::Data::new(
            AppState::new(temp_dir.path().to_path_buf())
                .await
                .expect("app state"),
        )
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
            App::new().app_data(state.clone()).configure(configure_routes),
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
        let etag = get.headers().get(header::ETAG).unwrap().to_str().unwrap().to_string();
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
            App::new().app_data(state.clone()).configure(configure_routes),
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
}
