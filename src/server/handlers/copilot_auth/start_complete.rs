use std::time::Duration;

use actix_web::{web, HttpResponse};
use tokio::time::timeout;

use crate::{
    agent::llm::providers::copilot::auth::DeviceCodeResponse,
    server::{app_state::AppState, error::AppError},
};

use super::{
    client::build_auth_handler,
    types::{CompleteAuthRequest, DeviceCodeInfo},
};

pub async fn start_copilot_auth(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let config = app_state.config.read().await.clone();
    let app_data_dir = app_state.app_data_dir.clone();

    let handler = match build_auth_handler(&config, app_data_dir) {
        Ok(handler) => handler,
        Err(err) => {
            log::error!("Failed to build Copilot auth HTTP client (proxy?): {}", err);
            return Ok(HttpResponse::InternalServerError().json(serde_json::json!({
                "success": false,
                "error": format!("Failed to build HTTP client: {}", err),
            })));
        }
    };

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
        Err(err) => {
            log::error!("Failed to get device code: {}", err);
            Ok(HttpResponse::InternalServerError().json(serde_json::json!({
                "success": false,
                "error": format!("Failed to get device code: {}", err)
            })))
        }
    }
}

pub async fn complete_copilot_auth(
    app_state: web::Data<AppState>,
    payload: web::Json<CompleteAuthRequest>,
) -> Result<HttpResponse, AppError> {
    let config = app_state.config.read().await.clone();
    let app_data_dir = app_state.app_data_dir.clone();

    let handler = match build_auth_handler(&config, app_data_dir) {
        Ok(handler) => handler,
        Err(err) => {
            log::error!("Failed to build Copilot auth HTTP client (proxy?): {}", err);
            return Ok(HttpResponse::InternalServerError().json(serde_json::json!({
                "success": false,
                "error": format!("Failed to build HTTP client: {}", err),
            })));
        }
    };

    let device_code = DeviceCodeResponse {
        device_code: payload.device_code.clone(),
        user_code: String::new(),
        verification_uri: String::new(),
        expires_in: payload.expires_in,
        interval: payload.interval,
    };

    match handler.complete_authentication(&device_code).await {
        Ok(_) => {
            log::info!("Copilot authentication completed successfully");

            // Authentication is already persisted to disk above. Reloading the in-memory provider
            // is best-effort: in proxy-restricted environments this can occasionally block on
            // network-dependent refresh paths. Don't hold the auth response hostage.
            match timeout(Duration::from_secs(8), app_state.reload_provider()).await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    log::warn!(
                        "Copilot auth succeeded but provider reload failed (non-fatal): {}",
                        err
                    );
                }
                Err(_) => {
                    log::warn!(
                        "Copilot auth succeeded but provider reload timed out (non-fatal)"
                    );
                }
            }

            Ok(HttpResponse::Ok().json(serde_json::json!({
                "success": true,
                "message": "Copilot authenticated successfully"
            })))
        }
        Err(err) => {
            log::error!("Copilot authentication completion failed: {}", err);
            Ok(HttpResponse::InternalServerError().json(serde_json::json!({
                "success": false,
                "error": format!("Authentication failed: {}", err)
            })))
        }
    }
}
