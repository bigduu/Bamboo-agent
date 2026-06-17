//! Permission rule settings: the "always ask" rules that force a user
//! confirmation for matching tool calls even under bypass / permissive modes.
//!
//! Rules use the same pattern syntax as the rest of the permission system,
//! e.g. `Bash(rm -rf *)`, `Bash(git push *)`, `Write(/etc/**)`. Built-in
//! dangerous-command detection is layered on top and is not configurable here.

use actix_web::{web, HttpResponse};
use serde::{Deserialize, Serialize};

use crate::{app_state::AppState, error::AppError};

#[derive(Debug, Serialize)]
pub struct AskRulesResponse {
    pub rules: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateAskRulesRequest {
    #[serde(default)]
    pub rules: Vec<String>,
}

/// Returns the configured "always ask" rule patterns.
pub async fn get_permission_ask_rules(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let rules = app_state
        .permission_checker
        .permission_config()
        .map(|config| config.ask_rule_patterns())
        .unwrap_or_default();
    Ok(HttpResponse::Ok().json(AskRulesResponse { rules }))
}

/// Replaces the "always ask" rule patterns and persists them to disk.
pub async fn update_permission_ask_rules(
    app_state: web::Data<AppState>,
    payload: web::Json<UpdateAskRulesRequest>,
) -> Result<HttpResponse, AppError> {
    let req = payload.into_inner();
    // Normalize: trim and drop blank entries so the UI can send a raw list.
    let rules: Vec<String> = req
        .rules
        .into_iter()
        .map(|r| r.trim().to_string())
        .filter(|r| !r.is_empty())
        .collect();

    let Some(config) = app_state.permission_checker.permission_config() else {
        return Err(AppError::InternalError(anyhow::anyhow!(
            "Permission checker does not support configurable rules"
        )));
    };

    // Apply in-memory (takes effect immediately for subsequent tool calls)...
    config.set_ask_rules(rules.clone());

    // ...then persist to the same store `load_permission_checker` reads at boot.
    let storage =
        bamboo_tools::permission::PermissionStorage::new(app_state.app_data_dir.clone());
    storage.save(config.as_ref()).await.map_err(|error| {
        AppError::InternalError(anyhow::anyhow!(
            "Failed to persist permission rules: {error}"
        ))
    })?;

    Ok(HttpResponse::Ok().json(AskRulesResponse { rules }))
}
