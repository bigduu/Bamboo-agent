//! Authenticated metadata only; selected definition bodies stay host-private.
use actix_web::{
    http::{header, StatusCode},
    web, HttpRequest, HttpResponse,
};
use bamboo_skills::named_agents::NamedAgentLimits;

use crate::app_state::AppState;
use crate::handlers::settings::{bootstrap_access_snapshot, BootstrapRequestState};
use crate::services::named_agent_catalog::{self, CatalogError};

pub async fn handler(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: HttpRequest,
) -> HttpResponse {
    let codex = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| {
            h.strip_prefix("Bearer ")
                .or_else(|| h.strip_prefix("bearer "))
        })
        .map(str::trim)
        .is_some_and(|token| token.starts_with("bcx1_"));
    let config = state.config.read().await;
    let authenticated = matches!(
        bootstrap_access_snapshot(&config, &req).request_state,
        BootstrapRequestState::LocalBypass | BootstrapRequestState::Authenticated
    );
    drop(config);
    if codex || !authenticated {
        return HttpResponse::Unauthorized().json(serde_json::json!({
            "error": crate::error::error_value("host_authentication_required")
        }));
    }
    if !req.query_string().is_empty() {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": crate::error::error_value("invalid_catalog_selector")
        }));
    }
    match named_agent_catalog::discover(&state, &path.into_inner(), NamedAgentLimits::default())
        .await
    {
        Ok(catalog) => HttpResponse::Ok()
            .insert_header((header::CACHE_CONTROL, "private, no-store"))
            .json(catalog.metadata()),
        Err(error) => {
            let (status, message) = match error {
                CatalogError::SessionNotFound => (StatusCode::NOT_FOUND, "session_not_found"),
                CatalogError::SessionUnavailable => {
                    (StatusCode::SERVICE_UNAVAILABLE, "session_unavailable")
                }
                CatalogError::ProjectUnavailable => {
                    (StatusCode::SERVICE_UNAVAILABLE, "project_unavailable")
                }
                CatalogError::CatalogUnavailable => {
                    (StatusCode::SERVICE_UNAVAILABLE, "catalog_unavailable")
                }
                CatalogError::CatalogRejected(_) => {
                    (StatusCode::PAYLOAD_TOO_LARGE, "catalog_rejected")
                }
            };
            let mut body = serde_json::json!({"error": crate::error::error_value(message)});
            if let CatalogError::CatalogRejected(code) = error {
                body["error"]["code"] = serde_json::json!(code);
            }
            HttpResponse::build(status).json(body)
        }
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
#[path = "named_agent_catalog_tests.rs"]
mod tests;
