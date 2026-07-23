use std::fmt::Display;

use actix_web::{Error, HttpResponse};

pub(super) fn internal_server_error(action: &str, error: impl Display) -> Error {
    crate::error::json_internal_server_error(format!("Failed to {action}: {error}"))
}

pub(super) fn schedule_not_found(schedule_id: &str) -> HttpResponse {
    HttpResponse::NotFound().json(serde_json::json!({
        "error": crate::error::error_value("Schedule not found"),
        "schedule_id": schedule_id
    }))
}
