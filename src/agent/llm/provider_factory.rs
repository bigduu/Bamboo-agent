//! Provider Factory
//!
//! Creates LLM providers based on configuration.

use crate::providers::{AnthropicProvider, CopilotProvider, GeminiProvider, OpenAIProvider};
use crate::provider::{LLMError, LLMProvider};
use crate::providers::common::MaskingProviderDecorator;
use chat_core::keyword_masking::KeywordMaskingConfig;
use chat_core::paths::{bamboo_dir, keyword_masking_json_path};
use chat_core::Config;
use std::sync::Arc;

/// Available provider types
pub const AVAILABLE_PROVIDERS: &[&str] = &["copilot", "openai", "anthropic", "gemini"];

/// Load keyword masking configuration.
fn load_masking_config() -> KeywordMaskingConfig {
    let path = keyword_masking_json_path();
    if !path.exists() {
        log::debug!("No keyword masking config found, using default");
        return KeywordMaskingConfig::default();
    }

    match std::fs::read_to_string(&path) {
        Ok(content) => match serde_json::from_str::<KeywordMaskingConfig>(&content) {
            Ok(config) => {
                log::info!(
                    "Loaded keyword masking config with {} entries",
                    config.entries.len()
                );
                config
            }
            Err(e) => {
                log::warn!("Failed to parse masking config: {}", e);
                KeywordMaskingConfig::default()
            }
        },
        Err(e) => {
            log::warn!("Failed to read masking config: {}", e);
            KeywordMaskingConfig::default()
        }
    }
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
    // Load masking config once (applies to all providers).
    let masking_config = load_masking_config();

    match config.provider.as_str() {
        "copilot" => {
            let mut provider = CopilotProvider::with_auth_handler(
                reqwest::Client::new(),
                app_data_dir,
                config.headless_auth,
            );

            // Try to authenticate (using cache if available)
            match provider.try_authenticate_silent().await {
                Ok(true) => {
                    log::info!("Copilot authenticated using cached token");
                }
                Ok(false) => {
                    log::warn!("Copilot not authenticated. Use POST /v1/bamboo/copilot/auth/start to authenticate.");
                    // Provider is created but not authenticated - will fail on first use
                    // This allows the user to see the authentication error and know what to do
                }
                Err(e) => {
                    log::warn!("Copilot silent authentication failed: {}. Use POST /v1/bamboo/copilot/auth/start to authenticate.", e);
                }
            }
            Ok(Arc::new(MaskingProviderDecorator::new(
                provider,
                masking_config.clone(),
            )))
        }

        "openai" => {
            let openai_config = config
                .providers
                .openai
                .as_ref()
                .ok_or_else(|| LLMError::Auth("OpenAI configuration required".to_string()))?;

            if openai_config.api_key.is_empty() {
                return Err(LLMError::Auth("OpenAI API key is required".to_string()));
            }

            let mut provider = OpenAIProvider::new(&openai_config.api_key);

            if let Some(base_url) = &openai_config.base_url {
                if !base_url.is_empty() {
                    provider = provider.with_base_url(base_url);
                }
            }

            Ok(Arc::new(MaskingProviderDecorator::new(
                provider,
                masking_config.clone(),
            )))
        }

        "anthropic" => {
            let anthropic_config = config
                .providers
                .anthropic
                .as_ref()
                .ok_or_else(|| LLMError::Auth("Anthropic configuration required".to_string()))?;

            if anthropic_config.api_key.is_empty() {
                return Err(LLMError::Auth("Anthropic API key is required".to_string()));
            }

            let mut provider = AnthropicProvider::new(&anthropic_config.api_key);

            if let Some(base_url) = &anthropic_config.base_url {
                if !base_url.is_empty() {
                    provider = provider.with_base_url(base_url);
                }
            }

            if let Some(max_tokens) = anthropic_config.max_tokens {
                provider = provider.with_max_tokens(max_tokens);
            }

            Ok(Arc::new(MaskingProviderDecorator::new(
                provider,
                masking_config.clone(),
            )))
        }

        "gemini" => {
            let gemini_config = config
                .providers
                .gemini
                .as_ref()
                .ok_or_else(|| LLMError::Auth("Gemini configuration required".to_string()))?;

            if gemini_config.api_key.is_empty() {
                return Err(LLMError::Auth("Gemini API key is required".to_string()));
            }

            let mut provider = GeminiProvider::new(&gemini_config.api_key);

            if let Some(base_url) = &gemini_config.base_url {
                if !base_url.is_empty() {
                    provider = provider.with_base_url(base_url);
                }
            }

            Ok(Arc::new(MaskingProviderDecorator::new(
                provider,
                masking_config.clone(),
            )))
        }

        _ => Err(LLMError::Auth(format!(
            "Unknown provider: {}. Available providers: {}",
            config.provider,
            AVAILABLE_PROVIDERS.join(", ")
        ))),
    }
}

/// Validate provider configuration without creating the provider
pub fn validate_provider_config(config: &Config) -> Result<(), LLMError> {
    match config.provider.as_str() {
        "copilot" => Ok(()),

        "openai" => {
            let openai_config = config
                .providers
                .openai
                .as_ref()
                .ok_or_else(|| LLMError::Auth("OpenAI configuration required".to_string()))?;

            if openai_config.api_key.is_empty() {
                return Err(LLMError::Auth("OpenAI API key is required".to_string()));
            }

            Ok(())
        }

        "anthropic" => {
            let anthropic_config = config
                .providers
                .anthropic
                .as_ref()
                .ok_or_else(|| LLMError::Auth("Anthropic configuration required".to_string()))?;

            if anthropic_config.api_key.is_empty() {
                return Err(LLMError::Auth("Anthropic API key is required".to_string()));
            }

            Ok(())
        }

        "gemini" => {
            let gemini_config = config
                .providers
                .gemini
                .as_ref()
                .ok_or_else(|| LLMError::Auth("Gemini configuration required".to_string()))?;

            if gemini_config.api_key.is_empty() {
                return Err(LLMError::Auth("Gemini API key is required".to_string()));
            }

            Ok(())
        }

        _ => Err(LLMError::Auth(format!(
            "Unknown provider: {}",
            config.provider
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chat_core::{AnthropicConfig, GeminiConfig, OpenAIConfig, ProviderConfigs};

    #[tokio::test]
    async fn test_create_copilot_provider() {
        let config = Config {
            provider: "copilot".to_string(),
            providers: ProviderConfigs::default(),
            http_proxy: String::new(),
            https_proxy: String::new(),
            proxy_auth: None,
            model: None,
            headless_auth: false,
        };

        let result = create_provider(&config).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_openai_provider_without_config() {
        let config = Config {
            provider: "openai".to_string(),
            providers: ProviderConfigs::default(),
            http_proxy: String::new(),
            https_proxy: String::new(),
            proxy_auth: None,
            model: None,
            headless_auth: false,
        };

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
        let config = Config {
            provider: "openai".to_string(),
            providers: ProviderConfigs {
                openai: Some(OpenAIConfig {
                    api_key: "".to_string(),
                    base_url: None,
                    model: None,
                }),
                anthropic: None,
                gemini: None,
                copilot: None,
            },
            http_proxy: String::new(),
            https_proxy: String::new(),
            proxy_auth: None,
            model: None,
            headless_auth: false,
        };

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
        let config = Config {
            provider: "openai".to_string(),
            providers: ProviderConfigs {
                openai: Some(OpenAIConfig {
                    api_key: "sk-test123".to_string(),
                    base_url: Some("https://custom.openai.com/v1".to_string()),
                    model: Some("gpt-4o".to_string()),
                }),
                anthropic: None,
                gemini: None,
                copilot: None,
            },
            http_proxy: String::new(),
            https_proxy: String::new(),
            proxy_auth: None,
            model: None,
            headless_auth: false,
        };

        let result = create_provider(&config).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_anthropic_provider_success() {
        let config = Config {
            provider: "anthropic".to_string(),
            providers: ProviderConfigs {
                openai: None,
                anthropic: Some(AnthropicConfig {
                    api_key: "sk-ant-test123".to_string(),
                    base_url: None,
                    model: Some("claude-3-5-sonnet-20241022".to_string()),
                    max_tokens: Some(4096),
                }),
                gemini: None,
                copilot: None,
            },
            http_proxy: String::new(),
            https_proxy: String::new(),
            proxy_auth: None,
            model: None,
            headless_auth: false,
        };

        let result = create_provider(&config).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_gemini_provider_success() {
        let config = Config {
            provider: "gemini".to_string(),
            providers: ProviderConfigs {
                openai: None,
                anthropic: None,
                gemini: Some(GeminiConfig {
                    api_key: "AIza-test123".to_string(),
                    base_url: None,
                    model: Some("gemini-pro".to_string()),
                }),
                copilot: None,
            },
            http_proxy: String::new(),
            https_proxy: String::new(),
            proxy_auth: None,
            model: None,
            headless_auth: false,
        };

        let result = create_provider(&config).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_unknown_provider() {
        let config = Config {
            provider: "unknown".to_string(),
            providers: ProviderConfigs::default(),
            http_proxy: String::new(),
            https_proxy: String::new(),
            proxy_auth: None,
            model: None,
            headless_auth: false,
        };

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
        let config = Config {
            provider: "copilot".to_string(),
            providers: ProviderConfigs::default(),
            http_proxy: String::new(),
            https_proxy: String::new(),
            proxy_auth: None,
            model: None,
            headless_auth: false,
        };

        assert!(validate_provider_config(&config).is_ok());
    }

    #[test]
    fn test_validate_openai_config_missing() {
        let config = Config {
            provider: "openai".to_string(),
            providers: ProviderConfigs::default(),
            http_proxy: String::new(),
            https_proxy: String::new(),
            proxy_auth: None,
            model: None,
            headless_auth: false,
        };

        let result = validate_provider_config(&config);
        assert!(result.is_err());
    }
}
