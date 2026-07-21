use std::path::Path;

use actix_web::{web, HttpResponse};
use tokio::fs;

use crate::{app_state::AppState, error::AppError};

use super::common::{
    config_file_path, connect_backup_file_path, connect_file_path, model_limits_file_path,
};

/// Resets (deletes) the Bamboo configuration file.
pub async fn reset_bamboo_config(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    if app_state.config_facade.is_some() {
        return Err(AppError::BadRequest(
            "full config reset spans multiple revisioned sections and is disabled without a recoverable manifest; reset sections individually through the typed section API"
                .to_string(),
        ));
    }
    remove_config_file_if_exists(&config_file_path(&app_state.app_data_dir)).await?;
    remove_config_file_if_exists(&model_limits_file_path(&app_state.app_data_dir)).await?;
    // #455: connect.json is a sibling of config.json now, not an inline key —
    // a full config reset must clear it too, or bamboo-connect bot
    // tokens/allowlists would silently survive (and get re-merged back onto
    // the freshly-defaulted config on the very next load). Mirrors
    // config.json/model_limits.json above: only the primary file is removed
    // here — same as config.json.bak, which is intentionally left alone
    // (it's a low-sensitivity, multi-generation config snapshot meant to
    // survive a reset for recovery).
    remove_config_file_if_exists(&connect_file_path(&app_state.app_data_dir)).await?;
    // #457: UNLIKE config.json.bak, connect.json.bak is NOT left alone — it
    // holds an encrypted IM bot token, an immediately-usable remote-control
    // credential, and a full reset must scrub it too or that token stays
    // recoverable straight off disk after the user asked to wipe everything.
    remove_config_file_if_exists(&connect_backup_file_path(&app_state.app_data_dir)).await?;

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
