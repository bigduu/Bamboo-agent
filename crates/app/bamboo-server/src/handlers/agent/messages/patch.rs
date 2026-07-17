use actix_web::{web, HttpResponse, Result};

use super::shared::{
    clear_derived_context_state, ensure_session_not_running, load_session_or_404,
    save_and_cache_session,
};
use super::types::PatchMessageRequest;
use crate::app_state::AppState;
use bamboo_agent_core::Role;

/// `PATCH /api/v1/sessions/{session_id}/messages/{message_id}`
pub async fn patch_message(
    state: web::Data<AppState>,
    path: web::Path<(String, String)>,
    req: web::Json<PatchMessageRequest>,
) -> Result<HttpResponse> {
    let (session_id, message_id) = path.into_inner();
    let PatchMessageRequest { content } = req.into_inner();

    if content.trim().is_empty() {
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "error": crate::error::error_value("content must not be empty"),
            "session_id": session_id,
            "message_id": message_id,
        })));
    }

    if let Some(response) = ensure_session_not_running(&state, &session_id).await {
        return Ok(response);
    }

    let Some(mut session) = load_session_or_404(&state, &session_id).await? else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": crate::error::error_value("Session not found"),
            "session_id": session_id
        })));
    };

    let Some(message) = session
        .messages
        .iter_mut()
        .find(|message| message.id == message_id)
    else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": crate::error::error_value("Message not found"),
            "session_id": session_id,
            "message_id": message_id,
        })));
    };

    let has_tool_calls = message
        .tool_calls
        .as_ref()
        .map(|calls| !calls.is_empty())
        .unwrap_or(false);

    if !matches!(message.role, Role::Assistant) || has_tool_calls {
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "error": crate::error::error_value("Only assistant text messages can be updated"),
            "session_id": session_id,
            "message_id": message_id,
        })));
    }

    message.content = content;

    // Editing history invalidates derived context state.
    clear_derived_context_state(&mut session);
    let message_count = session.messages.len();
    save_and_cache_session(&state, &session_id, session).await?;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "session_id": session_id,
        "message_id": message_id,
        "message_count": message_count,
    })))
}

#[cfg(test)]
mod tests {
    use actix_web::{http::StatusCode, test, web, App};
    use serde_json::Value;
    use tempfile::tempdir;

    use crate::routes::configure_routes;
    use crate::AppState;

    async fn new_state() -> web::Data<AppState> {
        let temp_dir = tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        web::Data::new(
            AppState::new(temp_dir.path().to_path_buf())
                .await
                .expect("app state"),
        )
    }

    /// `PATCH /api/v1/sessions/{id}/messages/{id}` on an unknown session must
    /// use the canonical nested error envelope (`{"error": {"message",
    /// "type"}}`), not the old flat `{"error": "<string>"}` shape. #251/#507.
    #[actix_web::test]
    async fn patch_message_not_found_uses_canonical_error_envelope() {
        let state = new_state().await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let resp = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri("/api/v1/sessions/does-not-exist/messages/does-not-exist")
                .set_json(serde_json::json!({ "content": "edited" }))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["error"]["type"], "api_error");
        assert_eq!(body["error"]["message"], "Session not found");
        assert_eq!(body["session_id"], "does-not-exist");
    }

    /// A blank `content` is a 400 with the same canonical envelope shape.
    #[actix_web::test]
    async fn patch_message_empty_content_uses_canonical_error_envelope() {
        let state = new_state().await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let resp = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri("/api/v1/sessions/some-session/messages/some-message")
                .set_json(serde_json::json!({ "content": "   " }))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["error"]["type"], "api_error");
        assert_eq!(body["error"]["message"], "content must not be empty");
    }
}
