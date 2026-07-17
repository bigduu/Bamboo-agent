use actix_web::{web, HttpResponse, Result};

use crate::app_state::AppState;

/// Get the pending question for a session (if any).
///
/// This endpoint retrieves the current pending question that the agent
/// is waiting for the user to answer.
///
/// # HTTP Method
///
/// `GET /api/v1/sessions/{session_id}/question`
pub async fn get_pending_question(
    state: web::Data<AppState>,
    session_id: web::Path<String>,
) -> Result<HttpResponse> {
    let session_id = session_id.into_inner();

    let Some(session) = state.load_session_merged(&session_id).await else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": crate::error::error_value("Session not found")
        })));
    };

    match session.pending_question {
        Some(pending) => Ok(HttpResponse::Ok().json(serde_json::json!({
            "has_pending_question": true,
            "question": pending.question,
            "options": pending.options,
            "allow_custom": pending.allow_custom,
            "tool_call_id": pending.tool_call_id,
            "tool_name": pending.tool_name,
            "source": pending.source,
        }))),
        None => Ok(HttpResponse::Ok().json(serde_json::json!({
            "has_pending_question": false
        }))),
    }
}

#[cfg(test)]
mod http_tests {
    use actix_web::{http::StatusCode, test, web, App};
    use serde_json::Value;
    use tempfile::tempdir;

    use crate::routes::configure_routes;
    use crate::AppState;

    /// `GET /api/v1/sessions/{id}/respond/pending` for an unknown session must
    /// use the canonical nested error envelope (`{"error": {"message",
    /// "type"}}`), not the old flat `{"error": "<string>"}` shape. #251/#507.
    #[actix_web::test]
    async fn get_pending_question_not_found_uses_canonical_error_envelope() {
        let temp_dir = tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let state = web::Data::new(
            AppState::new(temp_dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let resp = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/sessions/does-not-exist/respond/pending")
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["error"]["type"], "api_error");
        assert_eq!(body["error"]["message"], "Session not found");
    }
}
