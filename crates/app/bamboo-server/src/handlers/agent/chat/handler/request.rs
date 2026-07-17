use actix_web::HttpResponse;
use uuid::Uuid;

pub(super) fn resolve_session_id(session_id: Option<&str>) -> String {
    session_id
        .map(ToString::to_string)
        .unwrap_or_else(|| Uuid::new_v4().to_string())
}

/// Resolve the effective model for a chat turn: the request's `model` when
/// non-empty, otherwise `default_model` (the server's resolved default —
/// see `bamboo_engine::resolved_defaults::resolve_default_run_config`, the
/// SAME resolution the connect bridge and `GET /execute/defaults` use).
/// Errors only when NEITHER resolves to anything — i.e. the request omitted
/// `model` and the server has no default model configured at all (issue
/// #480: `model` is optional on `POST /chat`).
pub(super) fn resolve_model(
    requested_model: Option<&str>,
    default_model: Option<&str>,
) -> Result<String, HttpResponse> {
    if let Some(model) = optional_non_empty(requested_model) {
        return Ok(model.to_string());
    }

    match optional_non_empty(default_model) {
        Some(model) => Ok(model.to_string()),
        None => Err(HttpResponse::BadRequest().json(serde_json::json!({
            "error": crate::error::error_value(
                "model is required and no default model is configured on this server"
            )
        }))),
    }
}

pub(super) fn optional_non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}
