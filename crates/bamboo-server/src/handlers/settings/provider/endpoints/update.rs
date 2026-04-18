use std::collections::BTreeSet;

use actix_web::{web, HttpResponse};
use serde_json::Value;

use bamboo_infrastructure_config::Config;
use crate::config_manager;
use crate::{
    app_state::{AppState, ConfigUpdateEffects},
    error::AppError,
};

use super::super::types::UpdateProviderRequest;

pub(super) async fn handle_update_provider_config(
    app_state: web::Data<AppState>,
    payload: web::Json<UpdateProviderRequest>,
) -> Result<HttpResponse, AppError> {
    let mut patch_obj = build_provider_patch(&payload);
    config_manager::sanitize_root_patch(&mut patch_obj);
    let api_key_intents = config_manager::provider_api_key_intents(&patch_obj);

    let new_config = match app_state
        .update_config(
            move |config| {
                let current = config.clone();
                let new_config = apply_provider_patch(&current, patch_obj, &api_key_intents)?;
                validate_provider_config(&new_config)?;

                *config = new_config;
                Ok(())
            },
            // Persist first; reload below so we can return a clear provider-reload error.
            ConfigUpdateEffects {
                reload_provider: false,
                reconcile_mcp: true,
            },
        )
        .await
    {
        Ok(cfg) => cfg,
        Err(AppError::BadRequest(message)) => return Ok(bad_request_response(message)),
        Err(error) => return Err(error),
    };

    if let Err(error) = app_state.reload_provider().await {
        return Ok(HttpResponse::InternalServerError().json(serde_json::json!({
            "success": false,
            "error": format!("Failed to reload provider: {error}")
        })));
    }

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "provider": new_config.provider
    })))
}

fn build_provider_patch(payload: &UpdateProviderRequest) -> serde_json::Map<String, Value> {
    let mut patch_obj = serde_json::Map::new();
    patch_obj.insert(
        "provider".to_string(),
        Value::String(payload.provider.clone()),
    );
    patch_obj.insert("providers".to_string(), payload.providers.clone());
    patch_obj
}

fn apply_provider_patch(
    current: &Config,
    mut patch_obj: serde_json::Map<String, Value>,
    api_key_intents: &BTreeSet<String>,
) -> Result<Config, AppError> {
    config_manager::preserve_masked_provider_api_keys(&mut patch_obj, current);
    let mut new_config = config_manager::build_merged_config(current, patch_obj)?;
    config_manager::sync_provider_api_keys_encrypted_for_patch(&mut new_config, api_key_intents)?;
    Ok(new_config)
}

fn validate_provider_config(config: &Config) -> Result<(), AppError> {
    if let Err(error) = bamboo_infrastructure_llm::validate_provider_config(config) {
        return Err(AppError::BadRequest(format!(
            "Invalid configuration: {error}"
        )));
    }
    Ok(())
}

fn bad_request_response(message: String) -> HttpResponse {
    HttpResponse::BadRequest().json(serde_json::json!({
        "success": false,
        "error": message
    }))
}

#[cfg(test)]
mod tests {
    use super::build_provider_patch;
    use crate::handlers::settings::provider::types::UpdateProviderRequest;

    #[test]
    fn build_provider_patch_sets_provider_and_providers_fields() {
        let request = UpdateProviderRequest {
            provider: "openai".to_string(),
            providers: serde_json::json!({"openai":{"model":"gpt-4.1"}}),
        };

        let patch = build_provider_patch(&request);
        assert_eq!(patch.get("provider"), Some(&serde_json::json!("openai")));
        assert_eq!(
            patch.get("providers"),
            Some(&serde_json::json!({"openai":{"model":"gpt-4.1"}}))
        );
    }
}
