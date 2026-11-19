//! Server integration of the engine event infrastructure.
//!
//! The event cluster (account change feed + replayable-event helper) lives in
//! `bamboo_engine::events`; callers reference it directly.

pub(crate) fn session_event_queue_unavailable(error: std::io::Error) -> actix_web::HttpResponse {
    tracing::warn!(%error, "session mutation could not reserve an account event");
    crate::error::json_error(
        actix_web::http::StatusCode::SERVICE_UNAVAILABLE,
        "Session change notification queue is unavailable",
    )
}

#[cfg(test)]
mod replayable_tests;
