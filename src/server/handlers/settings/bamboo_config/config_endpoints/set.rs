use crate::server::config_manager;
use crate::server::{
    app_state::{AppState, ConfigUpdateEffects},
    error::AppError,
};
use actix_web::{web, HttpResponse};
use serde_json::Value;

use super::common::redacted_config_json;

/// Updates the Bamboo application configuration.
pub async fn set_bamboo_config(
    app_state: web::Data<AppState>,
    payload: web::Json<Value>,
) -> Result<HttpResponse, AppError> {
    let patch = payload.into_inner();
    let mut patch_obj = config_manager::assert_json_object(patch)?;
    config_manager::sanitize_root_patch(&mut patch_obj);
    let api_key_intents = config_manager::provider_api_key_intents(&patch_obj);
    let effects = config_manager::effects_for_root_patch(&patch_obj);

    // Apply the patch under the config write lock to avoid clobbering concurrent updates.
    let new_config = app_state
        .update_config(
            move |config| {
                let current = config.clone();
                let mut patch_obj = patch_obj;
                config_manager::preserve_masked_provider_api_keys(&mut patch_obj, &current);
                let mut new_config = config_manager::build_merged_config(&current, patch_obj)?;
                config_manager::sync_provider_api_keys_encrypted_for_patch(
                    &mut new_config,
                    &api_key_intents,
                )?;
                *config = new_config;
                Ok(())
            },
            ConfigUpdateEffects {
                // Best-effort: setup/UX flows must be able to persist partial config even when
                // provider init isn't possible yet.
                reload_provider: false,
                reconcile_mcp: effects.reconcile_mcp,
            },
        )
        .await?;

    if effects.reload_provider == config_manager::ReloadMode::BestEffort {
        if let Err(error) = app_state.reload_provider().await {
            log::warn!(
                "Config updated (provider={}, requested_reload=true) but provider reload failed: {}",
                new_config.provider,
                error
            );
        }
    }

    Ok(HttpResponse::Ok().json(redacted_config_json(&new_config)?))
}
