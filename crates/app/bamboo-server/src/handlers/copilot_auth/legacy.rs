use actix_web::{web, HttpResponse};

use crate::{app_state::AppState, error::AppError};

use super::client::build_provider_with_auth_handler;

pub async fn authenticate_copilot(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let config = app_state.config.read().await.clone();
    let app_data_dir = app_state.app_data_dir.clone();

    if config.provider != "copilot" {
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "success": false,
            "error": crate::error::error_value("Current provider is not Copilot")
        })));
    }

    let mut provider = match build_provider_with_auth_handler(&config, app_data_dir) {
        Ok(provider) => provider,
        Err(err) => {
            tracing::error!("Failed to build Copilot HTTP client (proxy?): {}", err);
            return Ok(HttpResponse::InternalServerError().json(serde_json::json!({
                "success": false,
                "error": crate::error::error_value(format!("Failed to build HTTP client: {}", err)),
            })));
        }
    };

    match provider.authenticate().await {
        Ok(_) => {
            tracing::info!("Copilot authentication successful");

            app_state.reload_provider().await.map_err(|err| {
                AppError::InternalError(anyhow::anyhow!(
                    "Failed to reload provider after authentication: {}",
                    err
                ))
            })?;

            Ok(HttpResponse::Ok().json(serde_json::json!({
                "success": true,
                "message": "Copilot authenticated successfully"
            })))
        }
        Err(err) => {
            tracing::error!("Copilot authentication failed: {}", err);
            Ok(HttpResponse::InternalServerError().json(serde_json::json!({
                "success": false,
                "error": crate::error::error_value(format!("Authentication failed: {}", err))
            })))
        }
    }
}
