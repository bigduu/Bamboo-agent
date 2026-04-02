use actix_web::HttpResponse;

use super::response::bad_request_error_response;

pub(super) fn validate_and_normalize_model(
    model: Option<&str>,
) -> Result<Option<String>, HttpResponse> {
    let Some(model) = model else {
        return Ok(None);
    };
    let normalized = model.trim();
    if normalized.is_empty() {
        return Ok(None);
    }
    if normalized == "unknown" {
        return Ok(None);
    }
    Ok(Some(normalized.to_string()))
}

pub(super) fn require_resolved_model(model: Option<String>) -> Result<String, HttpResponse> {
    model.ok_or_else(|| bad_request_error_response("no model configured for session or provider"))
}
