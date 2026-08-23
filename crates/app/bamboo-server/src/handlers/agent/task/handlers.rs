use actix_web::{web, HttpResponse, Result};

use super::types::to_task_list_response;
use crate::app_state::AppState;
use bamboo_agent_core::SessionKind;

/// Get task list for a session.
pub async fn get_task_list(
    state: web::Data<AppState>,
    session_id: web::Path<String>,
) -> Result<HttpResponse> {
    let session_id = session_id.into_inner();
    let Some(session) = state.load_session(&session_id).await else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": crate::error::error_value("Session not found")
        })));
    };

    let shared_session = if session.kind == SessionKind::Child {
        state
            .load_session(&session.root_session_id)
            .await
            .unwrap_or_else(|| session.clone())
    } else {
        session.clone()
    };

    let version = shared_session
        .task_list_version_meta()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let Some(task_list) = shared_session.task_list.as_ref() else {
        return Ok(HttpResponse::Ok().json(super::types::TaskListResponse {
            session_id: shared_session.id.clone(),
            title: None,
            items: Vec::new(),
            progress: super::types::TaskProgress {
                completed: 0,
                total: 0,
                percentage: 0,
            },
            version,
            created_at: None,
            updated_at: None,
        }));
    };

    Ok(HttpResponse::Ok().json(to_task_list_response(task_list, version)))
}

/// Check if a session has a task list.
pub async fn has_task_list(
    state: web::Data<AppState>,
    session_id: web::Path<String>,
) -> Result<HttpResponse> {
    let session_id = session_id.into_inner();
    let Some(session) = state.load_session(&session_id).await else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": crate::error::error_value("Session not found")
        })));
    };

    let shared_session = if session.kind == SessionKind::Child {
        state
            .load_session(&session.root_session_id)
            .await
            .unwrap_or_else(|| session.clone())
    } else {
        session.clone()
    };

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "has_task_list": shared_session.task_list.is_some(),
        "session_id": shared_session.id
    })))
}

#[cfg(test)]
mod http_tests {
    use actix_web::{http::StatusCode, test, web, App};
    use serde_json::Value;
    use tempfile::tempdir;

    use crate::routes::configure_routes;
    use crate::AppState;

    /// `GET /api/v1/sessions/{id}/task` for an unknown session must use the
    /// canonical nested error envelope (`{"error": {"message", "type"}}`),
    /// not the old flat `{"error": "<string>"}` shape. #251/#507.
    #[actix_web::test]
    async fn get_task_list_not_found_uses_canonical_error_envelope() {
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
                .uri("/api/v1/sessions/does-not-exist/task")
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["error"]["type"], "api_error");
        assert_eq!(body["error"]["message"], "Session not found");
    }
}
