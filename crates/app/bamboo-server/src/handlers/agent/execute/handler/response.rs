use actix_web::HttpResponse;

use super::super::{ExecuteResponse, ExecuteSyncInfo};

pub(super) fn bad_request_error_response(message: impl Into<String>) -> HttpResponse {
    HttpResponse::BadRequest().json(serde_json::json!({
        "error": message.into()
    }))
}

pub(super) fn internal_server_error_response(message: impl Into<String>) -> HttpResponse {
    HttpResponse::InternalServerError().json(serde_json::json!({
        "error": message.into()
    }))
}

pub(super) fn completed_response(session_id: &str, sync: ExecuteSyncInfo) -> HttpResponse {
    HttpResponse::Ok().json(execute_response_payload(
        session_id,
        "completed",
        Some(sync),
        None,
    ))
}

pub(super) fn already_running_response(
    session_id: &str,
    sync: ExecuteSyncInfo,
    run_id: Option<String>,
) -> HttpResponse {
    HttpResponse::Ok().json(execute_response_payload(
        session_id,
        "already_running",
        Some(sync),
        run_id,
    ))
}

pub(super) fn started_response(
    session_id: &str,
    sync: ExecuteSyncInfo,
    run_id: String,
) -> HttpResponse {
    HttpResponse::Accepted().json(execute_response_payload(
        session_id,
        "started",
        Some(sync),
        Some(run_id),
    ))
}

pub(super) fn execute_response_payload(
    session_id: &str,
    status: &str,
    sync: Option<ExecuteSyncInfo>,
    run_id: Option<String>,
) -> ExecuteResponse {
    ExecuteResponse {
        session_id: session_id.to_string(),
        status: status.to_string(),
        events_url: format!("/api/v1/events/{}", session_id),
        sync,
        run_id,
    }
}
