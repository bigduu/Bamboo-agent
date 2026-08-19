//! Provider Factory
//!
//! Creates LLM providers based on configuration.

use crate::provider::{LLMError, LLMProvider};
use crate::providers::{
    AnthropicProvider, BodhiProvider, CopilotProvider, GeminiProvider, OpenAIProvider,
};
use bamboo_config::paths::bamboo_dir;
use bamboo_config::{Config, ProviderInstanceConfig};
use reqwest::Client;
use std::sync::Arc;

/// Available provider types
pub const AVAILABLE_PROVIDERS: &[&str] = &["copilot", "openai", "anthropic", "gemini", "bodhi"];

fn build_http_client(config: &Config) -> Result<Client, LLMError> {
    crate::http_client::build_http_client(config)
}

/// Create a provider based on the current configuration
pub async fn create_provider(config: &Config) -> Result<Arc<dyn LLMProvider>, LLMError> {
    let app_data_dir = bamboo_dir();
    create_provider_with_dir(config, app_data_dir).await
}

/// Create a provider with explicit app_data_dir
pub async fn create_provider_with_dir(
    config: &Config,
    app_data_dir: std::path::PathBuf,
) -> Result<Arc<dyn LLMProvider>, LLMError> {
    let provider_key = config.effective_default_provider();
    if let Some(instance) = config.provider_instances.get(provider_key) {
        create_provider_from_instance(config, instance, app_data_dir).await
    } else {
        create_provider_by_name(config, provider_key, app_data_dir).await
    }
}

/// Create a single named provider.
///
/// The name must be one of [`AVAILABLE_PROVIDERS`].
pub async fn create_provider_by_name(
    config: &Config,
    provider_name: &str,
    app_data_dir: std::path::PathBuf,
) -> Result<Arc<dyn LLMProvider>, LLMError> {
    if !AVAILABLE_PROVIDERS.contains(&provider_name) {
        return Err(LLMError::Auth(format!(
            "Unknown provider: {provider_name}. Available providers: {}",
            AVAILABLE_PROVIDERS.join(", ")
        )));
    }

    // Legacy compatibility seam. Runtime instance paths call
    // `create_provider_from_instance` directly and never project an instance
    // back into `Config.providers`.
    let mut legacy = config.clone();
    legacy.provider_instances.clear();
    let mut instance = bamboo_config::synthesize_legacy_instances(&legacy)
        .into_iter()
        .find_map(|(_, instance)| (instance.provider_type == provider_name).then_some(instance))
        .or_else(|| {
            (provider_name == "copilot").then(|| ProviderInstanceConfig {
                provider_type: "copilot".to_string(),
                label: None,
                api_key: String::new(),
                api_key_encrypted: None,
                credential_ref: None,
                base_url: None,
                model: None,
                fast_model: None,
                vision_model: None,
                reasoning_effort: None,
                responses_only_models: Vec::new(),
                request_overrides: None,
                enabled: true,
                extra: Default::default(),
            })
        })
        .ok_or_else(|| {
            LLMError::Auth(format!(
                "{} configuration required",
                provider_display_name(provider_name)
            ))
        })?;
    // Legacy callers historically selected a provider independently of its
    // optional discovery flag. Instance-native callers must honor `enabled`.
    instance.enabled = true;
    create_provider_from_instance(config, &instance, app_data_dir).await
}

/// Create a provider directly from the authoritative instance configuration.
///
/// This is intentionally the only factory used by the instance registry. It
/// must not mutate or populate legacy `Config.providers` slots.
pub async fn create_provider_from_instance(
    config: &Config,
    instance: &ProviderInstanceConfig,
    app_data_dir: std::path::PathBuf,
) -> Result<Arc<dyn LLMProvider>, LLMError> {
    if !instance.enabled {
        return Err(LLMError::Auth(format!(
            "Provider instance of type '{}' is disabled",
            instance.provider_type
        )));
    }

    let masking_config = config.keyword_masking.clone();
    let http_client = build_http_client(config)?;

    match instance.provider_type.as_str() {
        "copilot" => {
            let headless_auth = instance
                .extra
                .get("headless_auth")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(config.headless_auth);

            let mut provider = CopilotProvider::with_auth_handler(
                http_client.clone(),
                app_data_dir,
                headless_auth,
            );

            if !instance.responses_only_models.is_empty() {
                provider =
                    provider.with_responses_only_models(instance.responses_only_models.clone());
            }
            provider = provider.with_reasoning_effort(instance.reasoning_effort);
            provider = provider.with_request_overrides(instance.request_overrides.clone());

            // Try to authenticate (using cache if available)
            match provider.try_authenticate_silent().await {
                Ok(true) => {
                    tracing::info!("Copilot authenticated using cached token");
                }
                Ok(false) => {
                    tracing::warn!("Copilot not authenticated. Use POST /v1/bamboo/copilot/auth/start to authenticate.");
                    // Provider is created but not authenticated - will fail on first use
                    // This allows the user to see the authentication error and know what to do
                }
                Err(e) => {
                    tracing::warn!("Copilot silent authentication failed: {}. Use POST /v1/bamboo/copilot/auth/start to authenticate.", e);
                }
            }
            Ok(Arc::new(provider.with_masking(masking_config.clone())))
        }

        "openai" => {
            if instance.api_key.is_empty() {
                return Err(LLMError::Auth("OpenAI API key is required".to_string()));
            }

            let mut provider =
                OpenAIProvider::new(&instance.api_key).with_client(http_client.clone());

            if let Some(base_url) = &instance.base_url {
                if !base_url.is_empty() {
                    provider = provider.with_base_url(base_url);
                }
            }

            if !instance.responses_only_models.is_empty() {
                provider =
                    provider.with_responses_only_models(instance.responses_only_models.clone());
            }

            provider = provider.with_reasoning_effort(instance.reasoning_effort);
            provider = provider.with_explicit_prompt_cache(
                instance
                    .extra
                    .get(bamboo_config::OPENAI_EXPLICIT_PROMPT_CACHE_CONFIG_KEY)
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(true),
            );
            provider = provider.with_request_overrides(instance.request_overrides.clone());

            Ok(Arc::new(provider.with_masking(masking_config.clone())))
        }

        "anthropic" => {
            if instance.api_key.is_empty() {
                return Err(LLMError::Auth("Anthropic API key is required".to_string()));
            }

            let mut provider =
                AnthropicProvider::new(&instance.api_key).with_client(http_client.clone());

            if let Some(base_url) = &instance.base_url {
                if !base_url.is_empty() {
                    provider = provider.with_base_url(base_url);
                }
            }

            if let Some(max_tokens) = instance
                .extra
                .get("max_tokens")
                .and_then(serde_json::Value::as_u64)
                .and_then(|value| u32::try_from(value).ok())
            {
                provider = provider.with_max_tokens(max_tokens);
            }

            provider = provider.with_reasoning_effort(instance.reasoning_effort);
            provider = provider.with_request_overrides(instance.request_overrides.clone());
            provider = provider.with_thinking_replay_always(
                instance
                    .extra
                    .get("thinking_replay_always")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false),
            );

            Ok(Arc::new(provider.with_masking(masking_config.clone())))
        }

        "gemini" => {
            if instance.api_key.is_empty() {
                return Err(LLMError::Auth("Gemini API key is required".to_string()));
            }

            let mut provider =
                GeminiProvider::new(&instance.api_key).with_client(http_client.clone());

            if let Some(base_url) = &instance.base_url {
                if !base_url.is_empty() {
                    provider = provider.with_base_url(base_url);
                }
            }

            provider = provider.with_reasoning_effort(instance.reasoning_effort);
            provider = provider.with_request_overrides(instance.request_overrides.clone());

            Ok(Arc::new(provider.with_masking(masking_config.clone())))
        }

        "bodhi" => {
            if instance.api_key.is_empty() {
                return Err(LLMError::Auth("Bodhi API key is required".to_string()));
            }

            let target_provider = instance
                .extra
                .get("target_provider")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("openai");

            let mut provider =
                BodhiProvider::new(&instance.api_key).with_client(http_client.clone());

            if let Some(base_url) = &instance.base_url {
                if !base_url.is_empty() {
                    provider = provider.with_base_url(base_url);
                }
            }

            provider = provider
                .with_target_provider(target_provider)
                .with_reasoning_effort(instance.reasoning_effort);

            Ok(Arc::new(provider.with_masking(masking_config.clone())))
        }

        _ => Err(LLMError::Auth(format!(
            "Unknown provider: {}. Available providers: {}",
            instance.provider_type,
            AVAILABLE_PROVIDERS.join(", ")
        ))),
    }
}

/// Validate provider configuration without creating the provider
pub fn validate_provider_config(config: &Config) -> Result<(), LLMError> {
    if let Some(instance_id) = config.default_provider_instance.as_deref() {
        if let Some(instance) = config.provider_instances.get(instance_id) {
            if !instance.enabled {
                return Err(LLMError::Auth(format!(
                    "Default provider instance '{instance_id}' is disabled"
                )));
            }
            return match instance.provider_type.as_str() {
                "copilot" => Ok(()),
                "openai" | "anthropic" | "gemini" | "bodhi" => {
                    if instance.api_key.is_empty() {
                        Err(LLMError::Auth(format!(
                            "{} API key is required for provider instance '{instance_id}'",
                            provider_display_name(&instance.provider_type)
                        )))
                    } else {
                        Ok(())
                    }
                }
                other => Err(LLMError::Auth(format!(
                    "Unknown provider type '{other}' for provider instance '{instance_id}'"
                ))),
            };
        }
        if !AVAILABLE_PROVIDERS.contains(&instance_id) {
            return Err(LLMError::Auth(format!(
                "Default provider instance '{instance_id}' configuration required"
            )));
        }
    }

    let provider_name = config
        .default_provider_instance
        .as_deref()
        .unwrap_or(&config.provider);
    match provider_name {
        "copilot" => Ok(()),

        "openai" => {
            let openai_config = config
                .providers()
                .openai
                .as_ref()
                .ok_or_else(|| LLMError::Auth("OpenAI configuration required".to_string()))?;

            if openai_config.api_key.is_empty() {
                return Err(LLMError::Auth("OpenAI API key is required".to_string()));
            }

            Ok(())
        }

        "anthropic" => {
            let anthropic_config =
                config.providers().anthropic.as_ref().ok_or_else(|| {
                    LLMError::Auth("Anthropic configuration required".to_string())
                })?;

            if anthropic_config.api_key.is_empty() {
                return Err(LLMError::Auth("Anthropic API key is required".to_string()));
            }

            Ok(())
        }

        "gemini" => {
            let gemini_config = config
                .providers()
                .gemini
                .as_ref()
                .ok_or_else(|| LLMError::Auth("Gemini configuration required".to_string()))?;

            if gemini_config.api_key.is_empty() {
                return Err(LLMError::Auth("Gemini API key is required".to_string()));
            }

            Ok(())
        }

        "bodhi" => {
            let bodhi_config = config
                .providers()
                .bodhi
                .as_ref()
                .ok_or_else(|| LLMError::Auth("Bodhi configuration required".to_string()))?;

            if bodhi_config.api_key.is_empty() {
                return Err(LLMError::Auth("Bodhi API key is required".to_string()));
            }

            Ok(())
        }

        _ => Err(LLMError::Auth(format!("Unknown provider: {provider_name}"))),
    }
}

fn provider_display_name(provider_type: &str) -> &str {
    match provider_type {
        "openai" => "OpenAI",
        "anthropic" => "Anthropic",
        "gemini" => "Gemini",
        "bodhi" => "Bodhi",
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_config::{AnthropicConfig, GeminiConfig, OpenAIConfig, ProviderConfigs};

    fn config_with_provider(provider: &str, providers: ProviderConfigs) -> Config {
        let mut config = Config::default();
        config.provider = provider.to_string();
        *config.providers_mut() = providers;
        config
    }

    #[tokio::test]
    async fn test_create_copilot_provider() {
        let config = config_with_provider("copilot", ProviderConfigs::default());

        let result = create_provider(&config).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_openai_provider_without_config() {
        let config = config_with_provider("openai", ProviderConfigs::default());

        let result = create_provider(&config).await;
        assert!(result.is_err());
        match result {
            Err(LLMError::Auth(msg)) => {
                assert!(msg.contains("OpenAI configuration required"));
            }
            _ => panic!("Expected Auth error"),
        }
    }

    #[tokio::test]
    async fn test_create_openai_provider_with_empty_key() {
        let config = config_with_provider(
            "openai",
            ProviderConfigs {
                openai: Some(OpenAIConfig {
                    api_key: "".to_string(),
                    api_key_from_env: false,
                    api_key_encrypted: None,
                    credential_ref: None,
                    base_url: None,
                    model: None,
                    fast_model: None,
                    vision_model: None,
                    reasoning_effort: None,
                    responses_only_models: vec![],
                    request_overrides: None,
                    extra: Default::default(),
                }),
                ..ProviderConfigs::default()
            },
        );

        let result = create_provider(&config).await;
        assert!(result.is_err());
        match result {
            Err(LLMError::Auth(msg)) => {
                assert!(msg.contains("API key is required"));
            }
            _ => panic!("Expected Auth error"),
        }
    }

    #[tokio::test]
    async fn test_create_openai_provider_success() {
        let config = config_with_provider(
            "openai",
            ProviderConfigs {
                openai: Some(OpenAIConfig {
                    api_key: "sk-test123".to_string(),
                    api_key_from_env: false,
                    api_key_encrypted: None,
                    credential_ref: None,
                    base_url: Some("https://custom.openai.com/v1".to_string()),
                    model: Some("gpt-4o".to_string()),
                    fast_model: None,
                    vision_model: None,
                    reasoning_effort: None,
                    responses_only_models: vec![],
                    request_overrides: None,
                    extra: Default::default(),
                }),
                ..ProviderConfigs::default()
            },
        );

        let result = create_provider(&config).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn sdk_factory_rejects_disabled_default_instance() {
        let mut config = config_with_provider("openai", ProviderConfigs::default());
        config.provider_instances.insert(
            "work".to_string(),
            serde_json::from_value(serde_json::json!({
                "provider_type": "openai",
                "api_key": "sk-instance",
                "enabled": false
            }))
            .unwrap(),
        );
        config.default_provider_instance = Some("work".to_string());

        let error = create_provider_with_dir(&config, std::env::temp_dir())
            .await
            .err()
            .expect("disabled instance should fail")
            .to_string();

        assert!(error.contains("disabled"));
    }

    #[tokio::test]
    async fn test_create_anthropic_provider_success() {
        let config = config_with_provider(
            "anthropic",
            ProviderConfigs {
                anthropic: Some(AnthropicConfig {
                    api_key: "sk-ant-test123".to_string(),
                    api_key_from_env: false,
                    api_key_encrypted: None,
                    credential_ref: None,
                    base_url: None,
                    model: Some("claude-3-5-sonnet-20241022".to_string()),
                    fast_model: None,
                    vision_model: None,
                    max_tokens: Some(4096),
                    reasoning_effort: None,
                    request_overrides: None,
                    thinking_replay_always: None,
                    extra: Default::default(),
                }),
                ..ProviderConfigs::default()
            },
        );

        let result = create_provider(&config).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_gemini_provider_success() {
        let config = config_with_provider(
            "gemini",
            ProviderConfigs {
                gemini: Some(GeminiConfig {
                    api_key: "AIza-test123".to_string(),
                    api_key_from_env: false,
                    api_key_encrypted: None,
                    credential_ref: None,
                    base_url: None,
                    model: Some("gemini-pro".to_string()),
                    fast_model: None,
                    vision_model: None,
                    reasoning_effort: None,
                    request_overrides: None,
                    extra: Default::default(),
                }),
                ..ProviderConfigs::default()
            },
        );

        let result = create_provider(&config).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_unknown_provider() {
        let config = config_with_provider("unknown", ProviderConfigs::default());

        let result = create_provider(&config).await;
        assert!(result.is_err());
        match result {
            Err(LLMError::Auth(msg)) => {
                assert!(msg.contains("Unknown provider"));
            }
            _ => panic!("Expected Auth error"),
        }
    }

    #[test]
    fn test_validate_copilot_config() {
        let config = config_with_provider("copilot", ProviderConfigs::default());

        assert!(validate_provider_config(&config).is_ok());
    }

    #[test]
    fn test_validate_openai_config_missing() {
        let config = config_with_provider("openai", ProviderConfigs::default());

        let result = validate_provider_config(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_instance_default_without_legacy_provider_config() {
        let mut config = config_with_provider("anthropic", ProviderConfigs::default());
        config.provider_instances.insert(
            "work".to_string(),
            serde_json::from_value(serde_json::json!({
                "provider_type": "openai",
                "api_key": "sk-instance",
                "enabled": true
            }))
            .unwrap(),
        );
        config.default_provider_instance = Some("work".to_string());

        assert!(validate_provider_config(&config).is_ok());
    }

    #[test]
    fn test_validate_legacy_default_alongside_explicit_instances() {
        let mut config = config_with_provider(
            "anthropic",
            ProviderConfigs {
                openai: Some(OpenAIConfig {
                    api_key: "sk-legacy".to_string(),
                    ..OpenAIConfig::default()
                }),
                ..ProviderConfigs::default()
            },
        );
        config.provider_instances.insert(
            "work".to_string(),
            serde_json::from_value(serde_json::json!({
                "provider_type": "copilot",
                "enabled": true
            }))
            .unwrap(),
        );
        config.default_provider_instance = Some("openai".to_string());

        assert!(validate_provider_config(&config).is_ok());
    }

    #[test]
    fn test_validate_instance_default_requires_existing_enabled_instance() {
        let mut missing = config_with_provider("copilot", ProviderConfigs::default());
        missing.default_provider_instance = Some("missing".to_string());
        let error = validate_provider_config(&missing).unwrap_err().to_string();
        assert!(error.contains("Default provider instance 'missing' configuration required"));

        let mut disabled = config_with_provider("copilot", ProviderConfigs::default());
        disabled.provider_instances.insert(
            "work".to_string(),
            serde_json::from_value(serde_json::json!({
                "provider_type": "copilot",
                "enabled": false
            }))
            .unwrap(),
        );
        disabled.default_provider_instance = Some("work".to_string());
        let error = validate_provider_config(&disabled).unwrap_err().to_string();
        assert!(error.contains("Default provider instance 'work' is disabled"));
    }
}
