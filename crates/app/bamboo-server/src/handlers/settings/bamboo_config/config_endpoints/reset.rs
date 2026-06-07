use std::path::Path;

use actix_web::{web, HttpResponse};
use tokio::fs;

use crate::{app_state::AppState, error::AppError};

use super::common::{config_file_path, model_limits_file_path};

/// Resets (deletes) the Bamboo configuration file.
pub async fn reset_bamboo_config(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    remove_config_file_if_exists(&config_file_path(&app_state.app_data_dir)).await?;
    remove_config_file_if_exists(&model_limits_file_path(&app_state.app_data_dir)).await?;

    // Reset in-memory config and best-effort reload provider.
    let new_config = app_state.reload_config().await;
    if let Err(error) = app_state.reload_provider().await {
        tracing::warn!(
            "Config reset updated config to provider={}, but provider reload failed: {}",
            new_config.provider,
            error
        );
    }

    // Config reset may remove/disable MCP servers; reconcile to stop any running servers.
    app_state
        .mcp_manager
        .reconcile_from_config(&new_config.mcp)
        .await;

    Ok(HttpResponse::Ok().json(serde_json::json!({ "success": true })))
}

pub(super) async fn remove_config_file_if_exists(path: &Path) -> Result<(), AppError> {
    match fs::try_exists(path).await {
        Ok(true) => {
            fs::remove_file(path)
                .await
                .map_err(AppError::StorageError)?;
            Ok(())
        }
        Ok(false) => Ok(()),
        Err(error) => Err(AppError::StorageError(error)),
    }
}
