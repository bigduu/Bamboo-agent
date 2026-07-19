use actix_web::{web, HttpResponse};
use serde_json::Value;

use crate::config_manager;
use crate::{
    app_state::{AppState, ConfigUpdateEffects},
    error::AppError,
};
use bamboo_config::patch::ProviderApiKeyIntents;
use bamboo_llm::Config;

use super::super::types::UpdateProviderRequest;

pub(super) async fn handle_update_provider_config(
    app_state: web::Data<AppState>,
    payload: web::Json<UpdateProviderRequest>,
) -> Result<HttpResponse, AppError> {
    let mut patch_obj = build_provider_patch(&payload);
    config_manager::sanitize_root_patch(&mut patch_obj);
    let api_key_intents = config_manager::provider_api_key_intents(&patch_obj);
    let credential_store = app_state.credential_store.clone();

    let new_config = match app_state
        .update_config(
            move |config| {
                let current = config.clone();
                let mut new_config = apply_provider_patch(&current, patch_obj, &api_key_intents)?;
                validate_provider_config(&new_config)?;
                config_manager::persist_provider_credentials_for_patch(
                    &mut new_config,
                    &api_key_intents,
                    &credential_store,
                )?;

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
            "error": crate::error::error_value(format!("Failed to reload provider: {error}"))
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
    if let Some(defaults) = &payload.defaults {
        patch_obj.insert(
            "defaults".to_string(),
            serde_json::to_value(defaults).expect("DefaultsConfig should serialize"),
        );
    }
    let mut features_patch = serde_json::Map::new();
    if let Some(enabled) = payload.features.provider_model_ref {
        features_patch.insert("provider_model_ref".to_string(), Value::Bool(enabled));
    }
    if let Some(enabled) = payload.features.dynamic_model_routing {
        features_patch.insert("dynamic_model_routing".to_string(), Value::Bool(enabled));
    }
    if !features_patch.is_empty() {
        patch_obj.insert("features".to_string(), Value::Object(features_patch));
    }
    patch_obj
}

fn apply_provider_patch(
    current: &Config,
    mut patch_obj: serde_json::Map<String, Value>,
    api_key_intents: &ProviderApiKeyIntents,
) -> Result<Config, AppError> {
    config_manager::preserve_masked_provider_api_keys(&mut patch_obj, current);
    let mut new_config = config_manager::build_merged_config(current, patch_obj)?;
    config_manager::sync_provider_api_keys_encrypted_for_patch(&mut new_config, api_key_intents)?;
    Ok(new_config)
}

fn validate_provider_config(config: &Config) -> Result<(), AppError> {
    if let Err(error) = bamboo_llm::validate_provider_config(config) {
        return Err(AppError::BadRequest(format!(
            "Invalid configuration: {error}"
        )));
    }
    Ok(())
}

fn bad_request_response(message: String) -> HttpResponse {
    HttpResponse::BadRequest().json(serde_json::json!({
        "success": false,
        "error": crate::error::error_value(message)
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
            defaults: None,
            features: Default::default(),
        };

        let patch = build_provider_patch(&request);
        assert_eq!(patch.get("provider"), Some(&serde_json::json!("openai")));
        assert_eq!(
            patch.get("providers"),
            Some(&serde_json::json!({"openai":{"model":"gpt-4.1"}}))
        );
        assert!(patch.get("defaults").is_none());
    }

    #[test]
    fn build_provider_patch_preserves_defaults_chat_model_ref() {
        let request = UpdateProviderRequest {
            provider: "copilot".to_string(),
            providers: serde_json::json!({"copilot":{"model":"gpt-5.5"}}),
            defaults: Some(bamboo_config::DefaultsConfig {
                chat: bamboo_domain::ProviderModelRef {
                    provider: "copilot".to_string(),
                    model: "gpt-5.5".to_string(),
                },
                fast: Some(bamboo_domain::ProviderModelRef {
                    provider: "openai".to_string(),
                    model: "qwen3.6-plus".to_string(),
                }),
                task_summary: Some(bamboo_domain::ProviderModelRef {
                    provider: "anthropic".to_string(),
                    model: "claude-3-7-sonnet".to_string(),
                }),
                vision: None,
                memory_background: None,
                planning: None,
                search: None,
                code_review: None,
                sub_agent: None,
                subagent_models: std::collections::HashMap::new(),
            }),
            features: Default::default(),
        };

        let patch = build_provider_patch(&request);
        assert_eq!(
            patch.get("defaults"),
            Some(&serde_json::json!({
                "chat": {"provider":"copilot", "model":"gpt-5.5"},
                "fast": {"provider":"openai", "model":"qwen3.6-plus"},
                "task_summary": {"provider":"anthropic", "model":"claude-3-7-sonnet"}
            }))
        );
    }

    #[test]
    fn build_provider_patch_includes_feature_flags_patch() {
        let request = UpdateProviderRequest {
            provider: "openai".to_string(),
            providers: serde_json::json!({}),
            defaults: None,
            features: crate::handlers::settings::provider::types::UpdateFeatureFlagsRequest {
                provider_model_ref: Some(true),
                dynamic_model_routing: None,
            },
        };

        let patch = build_provider_patch(&request);
        assert_eq!(
            patch.get("features"),
            Some(&serde_json::json!({"provider_model_ref": true}))
        );
    }
}
