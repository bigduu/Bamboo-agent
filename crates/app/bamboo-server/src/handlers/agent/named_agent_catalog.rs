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
        return HttpResponse::Unauthorized()
            .json(serde_json::json!({"error":"host_authentication_required"}));
    }
    if !req.query_string().is_empty() {
        return HttpResponse::BadRequest()
            .json(serde_json::json!({"error":"invalid_catalog_selector"}));
    }
    match named_agent_catalog::discover(&state, &path.into_inner(), NamedAgentLimits::default())
        .await
    {
        Ok(catalog) => HttpResponse::Ok()
            .insert_header((header::CACHE_CONTROL, "private, no-store"))
            .json(catalog.metadata()),
        Err(error) => {
            let status = match error {
                CatalogError::SessionNotFound => StatusCode::NOT_FOUND,
                CatalogError::CatalogRejected(_) => StatusCode::PAYLOAD_TOO_LARGE,
                _ => StatusCode::SERVICE_UNAVAILABLE,
            };
            HttpResponse::build(status).json(serde_json::json!({"error":error}))
        }
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
#[path = "named_agent_catalog_tests.rs"]
mod tests;
