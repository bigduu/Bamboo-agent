use crate::server::{
    app_state::{AppState, ConfigUpdateEffects},
    error::AppError,
};
use actix_web::{web, HttpResponse};

use super::types::ProxyAuthPayload;

/// Sets proxy authentication credentials
///
/// # HTTP Route
/// `POST /bamboo/proxy-auth`
///
/// # Request Body
/// ```json
/// {
///   "username": "user",
///   "password": "pass"
/// }
/// ```
///
/// # Response Format
/// ```json
/// {
///   "success": true
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Proxy auth saved and provider reloaded
/// - `500 Internal Server Error`: Failed to save or reload
///
/// # Security
/// Credentials are encrypted before storage in config.json.
///
/// # Example
/// ```bash
/// curl -X POST http://localhost:3000/bamboo/proxy-auth \
///   -H "Content-Type: application/json" \
///   -d '{"username": "user", "password": "pass"}'
/// ```
pub async fn set_proxy_auth(
    app_state: web::Data<AppState>,
    payload: web::Json<ProxyAuthPayload>,
) -> Result<HttpResponse, AppError> {
    let auth = payload.into_inner().into_proxy_auth();

    app_state
        .update_config(
            move |config| {
                config.proxy_auth = auth;
                config.refresh_proxy_auth_encrypted().map_err(|e| {
                    AppError::InternalError(anyhow::anyhow!(
                        "Failed to encrypt proxy auth before save: {e}"
                    ))
                })?;
                Ok(())
            },
            ConfigUpdateEffects {
                // Best-effort: setup flows often set proxy auth before provider config is complete.
                // Persisting should not fail just because provider init can't happen yet.
                reload_provider: false,
                // Proxy auth can affect SSE-based MCP servers too.
                reconcile_mcp: true,
            },
        )
        .await?;

    if let Err(e) = app_state.reload_provider().await {
        log::warn!("Proxy auth updated but provider reload failed: {}", e);
    }

    Ok(HttpResponse::Ok().json(serde_json::json!({ "success": true })))
}

/// Gets proxy authentication status
///
/// # HTTP Route
/// `GET /bamboo/proxy-auth/status`
///
/// # Response Format
/// ```json
/// {
///   "configured": true,
///   "username": "myuser"
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Status retrieved successfully
///
/// # Note
/// Password is never returned, only whether auth is configured and the username.
///
/// # Example
/// ```bash
/// curl http://localhost:3000/bamboo/proxy-auth/status
/// ```
pub async fn get_proxy_auth_status(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    // Defensive: ensure in-memory proxy_auth is hydrated from encrypted fields.
    // Some call paths update config via JSON patching and may only carry encrypted values.
    let mut config = app_state.config.write().await;
    config.hydrate_proxy_auth_from_encrypted();

    if let Some(auth) = config.proxy_auth.as_ref() {
        return Ok(HttpResponse::Ok().json(serde_json::json!({
            "configured": true,
            "username": auth.username,
        })));
    }

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "configured": false,
        "username": serde_json::Value::Null
    })))
}
