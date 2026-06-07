use actix_web::HttpResponse;
use uuid::Uuid;

pub(super) fn resolve_session_id(session_id: Option<&str>) -> String {
    session_id
        .map(ToString::to_string)
        .unwrap_or_else(|| Uuid::new_v4().to_string())
}

pub(super) fn validate_and_normalize_model(model: &str) -> Result<String, HttpResponse> {
    let normalized = model.trim();
    if normalized.is_empty() {
        return Err(HttpResponse::BadRequest().json(serde_json::json!({
            "error": "model is required"
        })));
    }

    Ok(normalized.to_string())
}

pub(super) fn optional_non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}
