use actix_web::{web, HttpResponse};

use crate::{app_state::AppState, error::AppError};

pub(super) async fn handle_reload_provider_config(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    // Reload config from disk first.
    let new_config = app_state.reload_config().await;

    if let Err(error) = bamboo_llm::validate_provider_config(&new_config) {
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "success": false,
            "error": error.to_string()
        })));
    }

    if let Err(error) = app_state.reload_provider().await {
        return Ok(HttpResponse::InternalServerError().json(serde_json::json!({
            "success": false,
            "error": format!("Failed to reload provider: {error}")
        })));
    }

    // Reconcile MCP runtimes in case file edits changed MCP config.
    app_state
        .mcp_manager
        .reconcile_from_config(&new_config.mcp)
        .await;

    tracing::info!("Provider reloaded successfully: {}", new_config.provider);

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "provider": new_config.provider
    })))
}
