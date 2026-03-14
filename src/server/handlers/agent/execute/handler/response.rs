use actix_web::HttpResponse;

use super::super::ExecuteResponse;

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

pub(super) fn completed_response(session_id: &str) -> HttpResponse {
    HttpResponse::Ok().json(execute_response_payload(session_id, "completed"))
}

pub(super) fn already_running_response(session_id: &str) -> HttpResponse {
    HttpResponse::Ok().json(execute_response_payload(session_id, "already_running"))
}

pub(super) fn started_response(session_id: &str) -> HttpResponse {
    HttpResponse::Accepted().json(execute_response_payload(session_id, "started"))
}

pub(super) fn execute_response_payload(session_id: &str, status: &str) -> ExecuteResponse {
    ExecuteResponse {
        session_id: session_id.to_string(),
        status: status.to_string(),
        events_url: format!("/api/v1/events/{}", session_id),
    }
}
