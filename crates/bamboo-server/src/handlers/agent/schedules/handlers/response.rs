use std::fmt::Display;

use actix_web::{error::ErrorInternalServerError, Error, HttpResponse};

pub(super) fn internal_server_error(action: &str, error: impl Display) -> Error {
    ErrorInternalServerError(format!("Failed to {action}: {error}"))
}

pub(super) fn schedule_not_found(schedule_id: &str) -> HttpResponse {
    HttpResponse::NotFound().json(serde_json::json!({
        "error": "Schedule not found",
        "schedule_id": schedule_id
    }))
}
