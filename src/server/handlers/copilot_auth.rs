use crate::server::{app_state::AppState, error::AppError};
use actix_web::{web, HttpResponse};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
pub struct AuthStatus {
    authenticated: bool,
    message: Option<String>,
}

#[derive(Serialize)]
pub struct DeviceCodeInfo {
    device_code: String, // The actual device code for polling
    user_code: String,   // The code user enters in browser
    verification_uri: String,
    expires_in: u64,
    interval: u64, // Polling interval in seconds
}

#[derive(Deserialize)]
pub struct CompleteAuthRequest {
    device_code: String,
    interval: u64,
    expires_in: u64,
}

/// Start Copilot authentication - returns device code info
pub async fn start_copilot_auth(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
    use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
    use std::sync::Arc;
    use std::time::Duration;

    // Get config
    let config = app_state.config.read().await.clone();
    let app_data_dir = app_state.app_data_dir.clone();

    // Resolve headless_auth from providers.copilot, with fallback to deprecated root field.
    let headless_auth = config
        .providers
        .copilot
        .as_ref()
        .map(|c| c.headless_auth)
        .unwrap_or(config.headless_auth);

    // Build retry client
    let retry_policy = ExponentialBackoff::builder()
        .retry_bounds(Duration::from_millis(100), Duration::from_secs(5))
        .build_with_max_retries(3);

    let client = match crate::agent::llm::http_client::build_http_client(&config) {
        Ok(client) => client,
        Err(e) => {
            log::error!("Failed to build Copilot auth HTTP client (proxy?): {}", e);
            return Ok(HttpResponse::InternalServerError().json(serde_json::json!({
                "success": false,
                "error": format!("Failed to build HTTP client: {}", e),
            })));
        }
    };
    let client_with_middleware: Arc<ClientWithMiddleware> = Arc::new(
        ClientBuilder::new(client.clone())
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build(),
    );

    // Create auth handler
    let handler = crate::agent::llm::providers::copilot::auth::CopilotAuthHandler::new(
        client_with_middleware,
        app_data_dir,
        headless_auth,
    );

    match handler.start_authentication().await {
        Ok(device_code) => {
            log::info!("Device code obtained: {}", device_code.user_code);
            Ok(HttpResponse::Ok().json(DeviceCodeInfo {
                device_code: device_code.device_code,
                user_code: device_code.user_code,
                verification_uri: device_code.verification_uri,
                expires_in: device_code.expires_in,
                interval: device_code.interval,
            }))
        }
        Err(e) => {
            log::error!("Failed to get device code: {}", e);
            Ok(HttpResponse::InternalServerError().json(serde_json::json!({
                "success": false,
                "error": format!("Failed to get device code: {}", e)
            })))
        }
    }
}

/// Complete Copilot authentication after user enters device code
pub async fn complete_copilot_auth(
    app_state: web::Data<AppState>,
    payload: web::Json<CompleteAuthRequest>,
) -> Result<HttpResponse, AppError> {
    use crate::agent::llm::providers::copilot::auth::{CopilotAuthHandler, DeviceCodeResponse};
    use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
    use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
    use std::sync::Arc;
    use std::time::Duration;

    // Get config
    let config = app_state.config.read().await.clone();
    let app_data_dir = app_state.app_data_dir.clone();

    // Resolve headless_auth from providers.copilot, with fallback to deprecated root field.
    let headless_auth = config
        .providers
        .copilot
        .as_ref()
        .map(|c| c.headless_auth)
        .unwrap_or(config.headless_auth);

    // Build retry client
    let retry_policy = ExponentialBackoff::builder()
        .retry_bounds(Duration::from_millis(100), Duration::from_secs(5))
        .build_with_max_retries(3);

    let client = match crate::agent::llm::http_client::build_http_client(&config) {
        Ok(client) => client,
        Err(e) => {
            log::error!("Failed to build Copilot auth HTTP client (proxy?): {}", e);
            return Ok(HttpResponse::InternalServerError().json(serde_json::json!({
                "success": false,
                "error": format!("Failed to build HTTP client: {}", e),
            })));
        }
    };
    let client_with_middleware: Arc<ClientWithMiddleware> = Arc::new(
        ClientBuilder::new(client.clone())
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build(),
    );

    // Create auth handler
    let handler = CopilotAuthHandler::new(client_with_middleware, app_data_dir, headless_auth);

    // Create device code response from request
    let device_code = DeviceCodeResponse {
        device_code: payload.device_code.clone(),
        user_code: String::new(), // Not needed for completion
        verification_uri: String::new(),
        expires_in: payload.expires_in,
        interval: payload.interval,
    };

    match handler.complete_authentication(&device_code).await {
        Ok(_) => {
            log::info!("Copilot authentication completed successfully");

            // Reload the provider in AppState with the authenticated provider
            app_state.reload_provider().await.map_err(|e| {
                AppError::InternalError(anyhow::anyhow!(
                    "Failed to reload provider after authentication: {}",
                    e
                ))
            })?;

            Ok(HttpResponse::Ok().json(serde_json::json!({
                "success": true,
                "message": "Copilot authenticated successfully"
            })))
        }
        Err(e) => {
            log::error!("Copilot authentication completion failed: {}", e);
            Ok(HttpResponse::InternalServerError().json(serde_json::json!({
                "success": false,
                "error": format!("Authentication failed: {}", e)
            })))
        }
    }
}

/// Trigger Copilot authentication flow (legacy, for backward compatibility)
pub async fn authenticate_copilot(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    // Get the current config
    let config = app_state.config.read().await.clone();
    let app_data_dir = app_state.app_data_dir.clone();

    // Resolve headless_auth from providers.copilot, with fallback to deprecated root field.
    let headless_auth = config
        .providers
        .copilot
        .as_ref()
        .map(|c| c.headless_auth)
        .unwrap_or(config.headless_auth);

    // Check if provider is copilot
    if config.provider != "copilot" {
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "success": false,
            "error": "Current provider is not Copilot"
        })));
    }

    // Create a Copilot provider that respects configured proxy settings.
    let http_client = match crate::agent::llm::http_client::build_http_client(&config) {
        Ok(client) => client,
        Err(e) => {
            log::error!("Failed to build Copilot HTTP client (proxy?): {}", e);
            return Ok(HttpResponse::InternalServerError().json(serde_json::json!({
                "success": false,
                "error": format!("Failed to build HTTP client: {}", e),
            })));
        }
    };
    let mut provider = crate::agent::llm::providers::CopilotProvider::with_auth_handler(
        http_client,
        app_data_dir,
        headless_auth,
    );

    match provider.authenticate().await {
        Ok(_) => {
            log::info!("Copilot authentication successful");

            // Reload the provider in AppState with the authenticated provider
            app_state.reload_provider().await.map_err(|e| {
                AppError::InternalError(anyhow::anyhow!(
                    "Failed to reload provider after authentication: {}",
                    e
                ))
            })?;

            Ok(HttpResponse::Ok().json(serde_json::json!({
                "success": true,
                "message": "Copilot authenticated successfully"
            })))
        }
        Err(e) => {
            log::error!("Copilot authentication failed: {}", e);
            Ok(HttpResponse::InternalServerError().json(serde_json::json!({
                "success": false,
                "error": format!("Authentication failed: {}", e)
            })))
        }
    }
}

/// Check Copilot authentication status
pub async fn get_copilot_auth_status(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    use std::fs;

    let app_data_dir = app_state.app_data_dir.clone();
    let copilot_token_path = app_data_dir.join(".copilot_token.json");

    // Try to load cached token
    if copilot_token_path.exists() {
        if let Ok(content) = fs::read_to_string(&copilot_token_path) {
            if let Ok(token_data) = serde_json::from_str::<serde_json::Value>(&content) {
                if let Some(expires_at) = token_data.get("expires_at").and_then(|v| v.as_u64()) {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();

                    if expires_at.saturating_sub(60) > now {
                        let remaining = expires_at.saturating_sub(now);
                        return Ok(HttpResponse::Ok().json(AuthStatus {
                            authenticated: true,
                            message: Some(format!("Token expires in {} minutes", remaining / 60)),
                        }));
                    } else {
                        return Ok(HttpResponse::Ok().json(AuthStatus {
                            authenticated: false,
                            message: Some("Token expired".to_string()),
                        }));
                    }
                }
            }
        }
    }

    Ok(HttpResponse::Ok().json(AuthStatus {
        authenticated: false,
        message: Some("No cached token found".to_string()),
    }))
}

/// Logout from Copilot (delete cached token)
pub async fn logout_copilot(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    use std::fs;

    let app_data_dir = app_state.app_data_dir.clone();

    let token_path = app_data_dir.join(".token");
    let copilot_token_path = app_data_dir.join(".copilot_token.json");

    let mut success = true;
    let mut messages = Vec::new();

    if token_path.exists() {
        match fs::remove_file(&token_path) {
            Ok(_) => messages.push("Deleted .token".to_string()),
            Err(e) => {
                success = false;
                messages.push(format!("Failed to delete .token: {}", e));
            }
        }
    }

    if copilot_token_path.exists() {
        match fs::remove_file(&copilot_token_path) {
            Ok(_) => messages.push("Deleted .copilot_token.json".to_string()),
            Err(e) => {
                success = false;
                messages.push(format!("Failed to delete .copilot_token.json: {}", e));
            }
        }
    }

    if success {
        log::info!("Copilot logged out successfully");
        Ok(HttpResponse::Ok().json(serde_json::json!({
            "success": true,
            "message": "Logged out successfully"
        })))
    } else {
        log::error!("Failed to logout: {}", messages.join(", "));
        Ok(HttpResponse::InternalServerError().json(serde_json::json!({
            "success": false,
            "error": messages.join(", ")
        })))
    }
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.route(
        "/bamboo/copilot/auth/start",
        web::post().to(start_copilot_auth),
    )
    .route(
        "/bamboo/copilot/auth/complete",
        web::post().to(complete_copilot_auth),
    )
    .route(
        "/bamboo/copilot/authenticate",
        web::post().to(authenticate_copilot),
    )
    .route(
        "/bamboo/copilot/auth/status",
        web::post().to(get_copilot_auth_status),
    )
    .route("/bamboo/copilot/logout", web::post().to(logout_copilot));
}
