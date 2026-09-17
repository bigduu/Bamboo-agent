use actix_web::{web, HttpResponse, Result};
use chrono::Utc;

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

    if message.content == content {
        return Ok(HttpResponse::Ok().json(serde_json::json!({
            "success": true,
            "session_id": session_id,
            "message_id": message_id,
            "message_count": session.messages.len(),
        })));
    }

    message.content = content;
    message.mark_content_updated_at(Utc::now());

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
    use bamboo_agent_core::{Message, Session};

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

    #[actix_web::test]
    async fn patch_message_persists_content_revision_without_rewriting_creation_time() {
        let state = new_state().await;
        let mut session = Session::new("edited-session", "model");
        let message = Message::assistant("before", None);
        let message_id = message.id.clone();
        let created_at = message.created_at;
        session.messages.push(message);
        state
            .storage
            .save_session(&session)
            .await
            .expect("seed Session");
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri(&format!(
                    "/api/v1/sessions/edited-session/messages/{message_id}"
                ))
                .set_json(serde_json::json!({ "content": "after" }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        let saved = state
            .storage
            .load_session("edited-session")
            .await
            .expect("load Session")
            .expect("saved Session");
        let edited = saved
            .messages
            .iter()
            .find(|message| message.id == message_id)
            .expect("edited Message");
        assert_eq!(edited.content, "after");
        assert_eq!(edited.created_at, created_at);
        assert!(edited
            .content_updated_at()
            .is_some_and(|updated_at| updated_at >= created_at));
    }

    #[actix_web::test]
    async fn patch_message_identical_content_preserves_history_generation_and_derived_state() {
        let state = new_state().await;
        let mut session = Session::new("idempotent-patch-session", "model");
        let message = Message::assistant("unchanged", None);
        let message_id = message.id.clone();
        session.messages.push(message);
        session.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
            "Keep this derived summary.",
            1,
            8,
        ));
        session.mark_authoritative_history_rewrite();
        let state_before = session
            .model_context_state
            .clone()
            .expect("history generation exists");
        state
            .storage
            .save_session(&session)
            .await
            .expect("seed Session");
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri(&format!(
                    "/api/v1/sessions/idempotent-patch-session/messages/{message_id}"
                ))
                .set_json(serde_json::json!({ "content": "unchanged" }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        let saved = state
            .storage
            .load_session("idempotent-patch-session")
            .await
            .expect("load Session")
            .expect("saved Session");
        assert_eq!(saved.model_context_state.as_ref(), Some(&state_before));
        assert!(saved.conversation_summary.is_some());
        assert!(saved.messages[0].content_updated_at().is_none());
    }
}
