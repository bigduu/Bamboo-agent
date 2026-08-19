use crate::{app_state::AppState, error::AppError};
use bamboo_config::{ProviderInstanceConfig, RequestOverridesConfig};
use bamboo_llm::Config;
use serde_json::Value;

use super::upstream::fetch_models_from_api;

/// Fully resolved model-discovery target. `routing_key` stays distinct from
/// `provider_type`: two OpenAI-compatible instances may have different keys,
/// credentials, base URLs and overrides, while using the same wire protocol.
pub(super) struct ModelFetchTarget {
    pub(super) routing_key: String,
    pub(super) provider_type: String,
    pub(super) api_key: Option<String>,
    pub(super) base_url: Option<String>,
    pub(super) request_overrides: Option<RequestOverridesConfig>,
}

pub(super) fn provider_key_from_payload(payload: &Value, config: &Config) -> String {
    payload
        .get("provider_instance_id")
        .and_then(Value::as_str)
        .or_else(|| payload.get("provider").and_then(Value::as_str))
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| config.effective_default_provider())
        .to_string()
}

pub(super) fn resolve_model_fetch_target(
    config: &Config,
    requested_key: &str,
) -> Result<ModelFetchTarget, AppError> {
    // An exact instance id is always authoritative, including the disabled
    // state. Never fall back to a stale legacy alias with the same id.
    if let Some(instance) = config.provider_instances.get(requested_key) {
        return target_from_instance(requested_key, instance);
    }

    if !bamboo_llm::AVAILABLE_PROVIDERS.contains(&requested_key) {
        return Err(AppError::BadRequest(format!(
            "Unknown provider instance or type: {requested_key}"
        )));
    }

    // Narrow hybrid seam: a missing effective default may still be a real
    // legacy alias while materialization is deferred. It wins over another
    // explicit instance of the same type because it is the selected provider.
    if requested_key == config.effective_default_provider() {
        if let Some(target) = target_from_legacy(config, requested_key) {
            return Ok(target);
        }
    }

    // Legacy clients send a built-in type, not an instance id. Map that type
    // deterministically to the selected instance of that type, then the
    // lexicographically first enabled instance. This seam is response/request
    // compatibility only; the returned target remains instance-native.
    if let Some(default_id) = config.default_provider_instance.as_deref() {
        if let Some(instance) = config.provider_instances.get(default_id) {
            if instance.provider_type == requested_key {
                return target_from_instance(default_id, instance);
            }
        }
    }
    let mut matching_ids = config
        .provider_instances
        .iter()
        .filter(|(_, instance)| instance.provider_type == requested_key && instance.enabled)
        .map(|(id, _)| id.as_str())
        .collect::<Vec<_>>();
    matching_ids.sort_unstable();
    if let Some(id) = matching_ids.first() {
        return target_from_instance(id, &config.provider_instances[*id]);
    }

    target_from_legacy(config, requested_key).ok_or_else(|| {
        AppError::BadRequest(format!(
            "{} configuration required",
            provider_display_name(requested_key)
        ))
    })
}

fn target_from_instance(
    id: &str,
    instance: &ProviderInstanceConfig,
) -> Result<ModelFetchTarget, AppError> {
    if !instance.enabled {
        return Err(AppError::BadRequest(format!(
            "Provider instance '{id}' is disabled"
        )));
    }
    Ok(ModelFetchTarget {
        routing_key: id.to_string(),
        provider_type: instance.provider_type.clone(),
        api_key: (instance.provider_type != "copilot").then(|| instance.api_key.clone()),
        base_url: instance.base_url.clone(),
        request_overrides: instance.request_overrides.clone(),
    })
}

fn target_from_legacy(config: &Config, provider_type: &str) -> Option<ModelFetchTarget> {
    let (api_key, base_url, request_overrides) = match provider_type {
        "openai" => {
            let provider = config.providers().openai.as_ref()?;
            (
                Some(provider.api_key.clone()),
                provider.base_url.clone(),
                provider.request_overrides.clone(),
            )
        }
        "anthropic" => {
            let provider = config.providers().anthropic.as_ref()?;
            (
                Some(provider.api_key.clone()),
                provider.base_url.clone(),
                provider.request_overrides.clone(),
            )
        }
        "gemini" => {
            let provider = config.providers().gemini.as_ref()?;
            (
                Some(provider.api_key.clone()),
                provider.base_url.clone(),
                provider.request_overrides.clone(),
            )
        }
        "copilot" => {
            if config.providers().copilot.is_none()
                && config.effective_default_provider() != "copilot"
            {
                return None;
            }
            (None, None, None)
        }
        "bodhi" => {
            let provider = config.providers().bodhi.as_ref()?;
            (
                Some(provider.api_key.clone()),
                provider.base_url.clone(),
                None,
            )
        }
        _ => return None,
    };
    Some(ModelFetchTarget {
        routing_key: provider_type.to_string(),
        provider_type: provider_type.to_string(),
        api_key,
        base_url,
        request_overrides,
    })
}

fn provider_display_name(provider_type: &str) -> &str {
    match provider_type {
        "openai" => "OpenAI",
        "anthropic" => "Anthropic",
        "gemini" => "Gemini",
        "copilot" => "Copilot",
        "bodhi" => "Bodhi",
        other => other,
    }
}

pub(super) fn build_proxy_aware_http_client(config: &Config) -> Result<reqwest::Client, AppError> {
    bamboo_llm::http_client::build_http_client(config).map_err(|error| {
        AppError::InternalError(anyhow::anyhow!("Failed to build HTTP client: {error}"))
    })
}

pub(super) async fn fetch_models_for_provider(
    app_state: &AppState,
    target: &ModelFetchTarget,
    client: &reqwest::Client,
) -> Result<Vec<String>, AppError> {
    if target.provider_type == "copilot" {
        return fetch_copilot_models(app_state, &target.routing_key).await;
    }
    let api_key = target.api_key.as_deref().unwrap_or_default();
    ensure_api_key(api_key)?;
    fetch_models_from_api(
        client,
        &target.provider_type,
        api_key,
        target.base_url.as_deref(),
        target.request_overrides.as_ref(),
    )
    .await
}

async fn fetch_copilot_models(
    app_state: &AppState,
    routing_key: &str,
) -> Result<Vec<String>, AppError> {
    let provider = app_state
        .provider_registry
        .get(routing_key)
        .ok_or_else(|| {
            AppError::BadRequest(format!(
                "Copilot provider instance '{routing_key}' is not available"
            ))
        })?;
    provider.list_models().await.map_err(|error| {
        let message = error.to_string();
        if message.contains("proxy") || message.contains("407") {
            AppError::ProxyAuthRequired
        } else {
            AppError::InternalError(anyhow::anyhow!("Failed to fetch models: {error}"))
        }
    })
}

fn ensure_api_key(api_key: &str) -> Result<(), AppError> {
    if api_key.trim().is_empty() {
        return Err(AppError::BadRequest("API key not configured".to_string()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{provider_key_from_payload, resolve_model_fetch_target};
    use bamboo_config::{Config, OpenAIConfig, ProviderInstanceConfig};

    fn instance(key: &str, base_url: &str) -> ProviderInstanceConfig {
        let mut instance: ProviderInstanceConfig = serde_json::from_value(serde_json::json!({
            "provider_type": "openai",
            "base_url": base_url,
            "enabled": true
        }))
        .unwrap();
        instance.api_key = key.to_string();
        instance
    }

    #[test]
    fn exact_instance_selection_keeps_per_instance_key_and_base_url() {
        let mut config = Config::default();
        config.provider_instances.insert(
            "work".to_string(),
            instance("sk-work", "https://work.example/v1"),
        );
        config.provider_instances.insert(
            "personal".to_string(),
            instance("sk-personal", "https://personal.example/v1"),
        );
        config.default_provider_instance = Some("work".to_string());

        let personal = resolve_model_fetch_target(&config, "personal").unwrap();
        assert_eq!(personal.routing_key, "personal");
        assert_eq!(personal.api_key.as_deref(), Some("sk-personal"));
        assert_eq!(
            personal.base_url.as_deref(),
            Some("https://personal.example/v1")
        );

        let legacy_type = resolve_model_fetch_target(&config, "openai").unwrap();
        assert_eq!(legacy_type.routing_key, "work");
        assert_eq!(legacy_type.api_key.as_deref(), Some("sk-work"));
    }

    #[test]
    fn explicit_default_instance_beats_stale_legacy_type_config() {
        let mut config = Config::default();
        config.providers_mut().openai = Some(OpenAIConfig {
            api_key: "sk-stale".to_string(),
            base_url: Some("https://stale.example/v1".to_string()),
            ..OpenAIConfig::default()
        });
        config.provider_instances.insert(
            "work".to_string(),
            instance("sk-work", "https://work.example/v1"),
        );
        config.default_provider_instance = Some("work".to_string());

        let target = resolve_model_fetch_target(&config, "openai").unwrap();
        assert_eq!(target.routing_key, "work");
        assert_eq!(target.api_key.as_deref(), Some("sk-work"));
    }

    #[test]
    fn missing_effective_hybrid_default_uses_real_legacy_alias() {
        let mut config = Config::default();
        config.provider = "openai".to_string();
        config.providers_mut().openai = Some(OpenAIConfig {
            api_key: "sk-legacy-default".to_string(),
            base_url: Some("https://legacy.example/v1".to_string()),
            ..OpenAIConfig::default()
        });
        config.provider_instances.insert(
            "work".to_string(),
            instance("sk-work", "https://work.example/v1"),
        );

        let target = resolve_model_fetch_target(&config, "openai").unwrap();
        assert_eq!(target.routing_key, "openai");
        assert_eq!(target.api_key.as_deref(), Some("sk-legacy-default"));
    }

    #[test]
    fn payload_instance_id_has_precedence_and_default_is_instance_aware() {
        let mut config = Config::default();
        config.provider_instances.insert(
            "work".to_string(),
            instance("sk-work", "https://work.example/v1"),
        );
        config.default_provider_instance = Some("work".to_string());

        assert_eq!(
            provider_key_from_payload(
                &serde_json::json!({
                    "provider": "openai",
                    "provider_instance_id": "work"
                }),
                &config
            ),
            "work"
        );
        assert_eq!(
            provider_key_from_payload(&serde_json::json!({}), &config),
            "work"
        );
    }

    #[test]
    fn selected_legacy_copilot_needs_no_provider_stanza() {
        let mut config = Config::default();
        config.provider = "copilot".to_string();
        *config.providers_mut() = bamboo_config::ProviderConfigs::default();

        let target = resolve_model_fetch_target(&config, "copilot").unwrap();
        assert_eq!(target.routing_key, "copilot");
        assert_eq!(target.provider_type, "copilot");
        assert!(target.api_key.is_none());
    }

    #[test]
    fn exact_copilot_instance_keeps_its_registry_routing_key() {
        let mut config = Config::default();
        for id in ["copilot-work", "copilot-personal"] {
            config.provider_instances.insert(
                id.to_string(),
                serde_json::from_value(serde_json::json!({
                    "provider_type": "copilot",
                    "enabled": true
                }))
                .unwrap(),
            );
        }
        config.default_provider_instance = Some("copilot-work".to_string());

        let target = resolve_model_fetch_target(&config, "copilot-personal").unwrap();
        assert_eq!(target.routing_key, "copilot-personal");
        assert_eq!(target.provider_type, "copilot");
    }
}
