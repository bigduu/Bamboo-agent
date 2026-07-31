//! Provider Factory
//!
//! Creates LLM providers based on configuration.

use crate::provider::{LLMError, LLMProvider};
use crate::providers::{
    AnthropicProvider, BodhiProvider, CopilotProvider, GeminiProvider, OpenAIProvider,
};
use bamboo_config::paths::bamboo_dir;
use bamboo_config::Config;
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
    create_provider_by_name(config, &config.provider, app_data_dir).await
}

/// Create a single named provider.
///
/// The name must be one of [`AVAILABLE_PROVIDERS`].
pub async fn create_provider_by_name(
    config: &Config,
    provider_name: &str,
    app_data_dir: std::path::PathBuf,
) -> Result<Arc<dyn LLMProvider>, LLMError> {
    let masking_config = config.keyword_masking.clone();
    let http_client = build_http_client(config)?;

    match provider_name {
        "copilot" => {
            // Get headless_auth from providers.copilot config, with fallback to deprecated root field
            let headless_auth = config
                .providers()
                .copilot
                .as_ref()
                .map(|c| c.headless_auth)
                .unwrap_or(config.headless_auth);

            let mut provider = CopilotProvider::with_auth_handler(
                http_client.clone(),
                app_data_dir,
                headless_auth,
            );

            if let Some(copilot_cfg) = config.providers().copilot.as_ref() {
                if !copilot_cfg.responses_only_models.is_empty() {
                    provider = provider
                        .with_responses_only_models(copilot_cfg.responses_only_models.clone());
                }
                provider = provider.with_reasoning_effort(copilot_cfg.reasoning_effort);
                provider = provider.with_request_overrides(copilot_cfg.request_overrides.clone());
            }

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
            let openai_config = config
                .providers()
                .openai
                .as_ref()
                .ok_or_else(|| LLMError::Auth("OpenAI configuration required".to_string()))?;

            if openai_config.api_key.is_empty() {
                return Err(LLMError::Auth("OpenAI API key is required".to_string()));
            }

            let mut provider =
                OpenAIProvider::new(&openai_config.api_key).with_client(http_client.clone());

            if let Some(base_url) = &openai_config.base_url {
                if !base_url.is_empty() {
                    provider = provider.with_base_url(base_url);
                }
            }

            if !openai_config.responses_only_models.is_empty() {
                provider = provider
                    .with_responses_only_models(openai_config.responses_only_models.clone());
            }

            provider = provider.with_reasoning_effort(openai_config.reasoning_effort);
            provider = provider.with_request_overrides(openai_config.request_overrides.clone());

            Ok(Arc::new(provider.with_masking(masking_config.clone())))
        }

        "anthropic" => {
            let anthropic_config =
                config.providers().anthropic.as_ref().ok_or_else(|| {
                    LLMError::Auth("Anthropic configuration required".to_string())
                })?;

            if anthropic_config.api_key.is_empty() {
                return Err(LLMError::Auth("Anthropic API key is required".to_string()));
            }

            let mut provider =
                AnthropicProvider::new(&anthropic_config.api_key).with_client(http_client.clone());

            if let Some(base_url) = &anthropic_config.base_url {
                if !base_url.is_empty() {
                    provider = provider.with_base_url(base_url);
                }
            }

            if let Some(max_tokens) = anthropic_config.max_tokens {
                provider = provider.with_max_tokens(max_tokens);
            }

            provider = provider.with_reasoning_effort(anthropic_config.reasoning_effort);
            provider = provider.with_request_overrides(anthropic_config.request_overrides.clone());
            provider = provider.with_thinking_replay_always(
                anthropic_config.thinking_replay_always.unwrap_or(false),
            );

            Ok(Arc::new(provider.with_masking(masking_config.clone())))
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

            let mut provider =
                GeminiProvider::new(&gemini_config.api_key).with_client(http_client.clone());

            if let Some(base_url) = &gemini_config.base_url {
                if !base_url.is_empty() {
                    provider = provider.with_base_url(base_url);
                }
            }

            provider = provider.with_reasoning_effort(gemini_config.reasoning_effort);
            provider = provider.with_request_overrides(gemini_config.request_overrides.clone());

            Ok(Arc::new(provider.with_masking(masking_config.clone())))
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

            let target_provider = bodhi_config.target_provider.as_deref().unwrap_or("openai");

            let mut provider =
                BodhiProvider::new(&bodhi_config.api_key).with_client(http_client.clone());

            if let Some(base_url) = &bodhi_config.base_url {
                if !base_url.is_empty() {
                    provider = provider.with_base_url(base_url);
                }
            }

            provider = provider
                .with_target_provider(target_provider)
                .with_reasoning_effort(bodhi_config.reasoning_effort);

            Ok(Arc::new(provider.with_masking(masking_config.clone())))
        }

        _ => Err(LLMError::Auth(format!(
            "Unknown provider: {}. Available providers: {}",
            provider_name,
            AVAILABLE_PROVIDERS.join(", ")
        ))),
    }
}

/// Validate provider configuration without creating the provider
pub fn validate_provider_config(config: &Config) -> Result<(), LLMError> {
    if let Some(instance_id) = config.default_provider_instance.as_deref() {
        let instance = config.provider_instances.get(instance_id).ok_or_else(|| {
            LLMError::Auth(format!(
                "Default provider instance '{instance_id}' configuration required"
            ))
        })?;
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

    match config.provider.as_str() {
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

        _ => Err(LLMError::Auth(format!(
            "Unknown provider: {}",
            config.provider
        ))),
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
