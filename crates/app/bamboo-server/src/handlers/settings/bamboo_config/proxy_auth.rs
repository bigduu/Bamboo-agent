use crate::{
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
/// Credentials are encrypted in the isolated credential store. `config.json`
/// retains only the stable `proxy.default.auth` reference.
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
        .update_proxy_auth_credential(
            auth,
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
        tracing::warn!("Proxy auth updated but provider reload failed: {}", e);
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
///   "credential_ref": "proxy.default.auth",
///   "configured": true,
///   "revision": 1,
///   "status": "healthy"
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Status retrieved successfully
///
/// # Note
/// Neither username nor password is returned; only credential and section metadata.
///
/// # Example
/// ```bash
/// curl http://localhost:3000/bamboo/proxy-auth/status
/// ```
pub async fn get_proxy_auth_status(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let reference = app_state
        .config
        .read()
        .await
        .proxy_auth_credential_ref
        .clone()
        .unwrap_or(
            bamboo_config::credential_ref("proxy", "default", "auth").map_err(|error| {
                AppError::InternalError(anyhow::anyhow!(
                    "invalid proxy credential reference: {error}"
                ))
            })?,
        );
    let (status, health) = app_state
        .credential_store
        .status_with_health(&reference)
        .map_err(super::credentials::map_store_read_error)?;
    Ok(HttpResponse::Ok().json(serde_json::json!({
        "credential_ref": status.credential_ref,
        "configured": status.configured,
        "source": status.source,
        "updated_at": status.updated_at,
        "revision": health.revision,
        "status": health.status,
        "source_kind": health.source,
        "last_error": health.last_error,
    })))
}
