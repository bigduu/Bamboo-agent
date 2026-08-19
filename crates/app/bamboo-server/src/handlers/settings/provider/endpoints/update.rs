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
    let provider_credential_intents = api_key_intents.providers.clone();

    let new_config = match app_state
        .update_config_with_provider_credentials(
            move |config| {
                // This guard must run against the transaction-current snapshot,
                // not a lock released before the mutation begins: a concurrent
                // canonical migration must never leave a window where this
                // legacy write is acknowledged and then discarded.
                if is_instance_native(config) {
                    return Err(AppError::BadRequest(
                        "The legacy provider endpoint is read-only after provider-instance migration; use /v1/bamboo/config/provider-settings (revisioned) or /v1/bamboo/settings/provider-instances"
                            .to_string(),
                    ));
                }
                let current = config.clone();
                let new_config = apply_provider_patch(&current, patch_obj, &api_key_intents)?;
                validate_provider_config(&new_config)?;
                *config = new_config;
                Ok(())
            },
            provider_credential_intents,
            std::collections::BTreeSet::new(),
            ConfigUpdateEffects {
                // The detached config transaction owns provider publication,
                // so cancellation cannot strand the committed generation.
                reload_provider: bamboo_config::patch::ReloadMode::Strict,
                // Preserve the endpoint's existing best-effort MCP reconcile:
                // provider persistence/reload errors are authoritative here,
                // while an unrelated MCP startup failure must not reject an
                // already-committed provider update.
                reconcile_mcp: bamboo_config::patch::ReloadMode::BestEffort,
            },
        )
        .await
    {
        Ok(cfg) => cfg,
        Err(AppError::BadRequest(message)) => return Ok(bad_request_response(message)),
        Err(error) => return Err(error),
    };

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "provider": new_config.provider
    })))
}

fn is_instance_native(config: &Config) -> bool {
    config
        .default_provider_instance
        .as_ref()
        .is_some_and(|id| config.provider_instances.contains_key(id))
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
    let providers = config.providers();
    let request_overrides = [
        providers
            .openai
            .as_ref()
            .and_then(|provider| provider.request_overrides.as_ref()),
        providers
            .anthropic
            .as_ref()
            .and_then(|provider| provider.request_overrides.as_ref()),
        providers
            .gemini
            .as_ref()
            .and_then(|provider| provider.request_overrides.as_ref()),
        providers
            .copilot
            .as_ref()
            .and_then(|provider| provider.request_overrides.as_ref()),
    ];
    for request_overrides in request_overrides.into_iter().flatten() {
        let original = serde_json::to_value(request_overrides).map_err(|error| {
            AppError::BadRequest(format!("Invalid provider request_overrides: {error}"))
        })?;
        let mut sanitized = original.clone();
        crate::handlers::settings::bamboo_config::scrub_unsafe_request_override_literals(
            &mut sanitized,
        );
        if sanitized != original {
            return Err(AppError::BadRequest(
                "Invalid configuration: provider request_overrides contain literal credential material"
                    .to_string(),
            ));
        }
    }
    let mut extra_maps = vec![&providers.extra];
    if let Some(provider) = &providers.openai {
        extra_maps.push(&provider.extra);
    }
    if let Some(provider) = &providers.anthropic {
        extra_maps.push(&provider.extra);
    }
    if let Some(provider) = &providers.gemini {
        extra_maps.push(&provider.extra);
    }
    if let Some(provider) = &providers.copilot {
        extra_maps.push(&provider.extra);
    }
    if let Some(provider) = &providers.bodhi {
        extra_maps.push(&provider.extra);
    }
    if extra_maps.into_iter().any(|extra| {
        !bamboo_config::provider_metadata_is_secret_free(&Value::Object(
            extra
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        ))
    }) {
        return Err(AppError::BadRequest(
            "Invalid configuration: provider metadata contains credential material outside api_key"
                .to_string(),
        ));
    }
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
    use super::{build_provider_patch, is_instance_native, validate_provider_config, AppError};
    use crate::app_state::AppState;
    use crate::handlers::settings::provider::types::UpdateProviderRequest;
    use actix_web::{test as actix_test, web, App};
    use bamboo_config::{Config, ProviderInstanceConfig};

    fn instance(provider_type: &str) -> ProviderInstanceConfig {
        serde_json::from_value(serde_json::json!({
            "provider_type": provider_type,
            "enabled": true
        }))
        .unwrap()
    }

    #[test]
    fn legacy_update_is_rejected_only_for_resolved_instance_authority() {
        let mut native = Config::default();
        native
            .provider_instances
            .insert("work".to_string(), instance("openai"));
        native.default_provider_instance = Some("work".to_string());
        assert!(is_instance_native(&native));

        let mut hybrid = native;
        hybrid.default_provider_instance = Some("anthropic".to_string());
        assert!(
            !is_instance_native(&hybrid),
            "a real legacy-default hybrid remains writable during the compatibility window"
        );
    }

    #[test]
    fn legacy_provider_write_rejects_credential_shaped_extra() {
        let mut config = Config::default();
        let mut openai = bamboo_config::OpenAIConfig {
            api_key: "sk-provider".to_string(),
            model: Some("gpt-test".to_string()),
            ..Default::default()
        };
        openai.extra.insert(
            "client_secret".to_string(),
            serde_json::json!("must-not-enter-extra"),
        );
        config.providers_mut().openai = Some(openai);

        assert!(matches!(
            validate_provider_config(&config),
            Err(AppError::BadRequest(_))
        ));
    }

    #[test]
    fn legacy_provider_write_rejects_literal_request_override_credentials() {
        let mut config = Config::default();
        config.providers_mut().openai = Some(
            serde_json::from_value(serde_json::json!({
                "api_key": "sk-provider",
                "model": "gpt-test",
                "request_overrides": {
                    "common": {
                        "headers": {"X-Access-Key": "must-not-enter-overrides"},
                        "body_patch": [{
                            "path": "/credential",
                            "value": "must-not-enter-overrides"
                        }]
                    }
                }
            }))
            .unwrap(),
        );

        assert!(matches!(
            validate_provider_config(&config),
            Err(AppError::BadRequest(_))
        ));
    }

    #[actix_web::test]
    async fn legacy_post_returns_bad_request_without_mutating_instance_native_config() {
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        {
            let mut config = state.config.write().await;
            let mut work = instance("openai");
            work.api_key = "sk-work".to_string();
            work.model = Some("instance-model".to_string());
            config.provider_instances.insert("work".to_string(), work);
            config.default_provider_instance = Some("work".to_string());
        }
        let app = actix_test::init_service(App::new().app_data(state.clone()).route(
            "/provider",
            web::post().to(super::handle_update_provider_config),
        ))
        .await;

        let response = actix_test::call_service(
            &app,
            actix_test::TestRequest::post()
                .uri("/provider")
                .set_json(serde_json::json!({
                    "provider": "openai",
                    "providers": {"openai": {"model": "legacy-write"}}
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
        let body = String::from_utf8(actix_test::read_body(response).await.to_vec()).unwrap();
        assert!(body.contains("provider-settings"));
        assert_eq!(
            state.config.read().await.provider_instances["work"]
                .model
                .as_deref(),
            Some("instance-model")
        );
    }

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
