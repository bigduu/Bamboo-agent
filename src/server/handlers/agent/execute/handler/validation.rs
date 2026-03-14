use actix_web::HttpResponse;

use super::response::bad_request_error_response;

pub(super) fn validate_and_normalize_model(model: &str) -> Result<String, HttpResponse> {
    let normalized = model.trim();
    if normalized.is_empty() {
        return Err(bad_request_error_response("model parameter is required"));
    }
    Ok(normalized.to_string())
}
