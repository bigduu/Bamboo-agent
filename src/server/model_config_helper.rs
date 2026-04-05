// Helper function to extract default model from config
// This should be used instead of hardcoding "gpt-4o-mini" or "default"

use crate::agent::llm::LLMError;
use crate::core::Config;

/// Get the default model for the current provider from config
/// Returns an error if no model is configured
pub fn get_default_model_from_config(config: &Config) -> Result<String, LLMError> {
    match config.provider.as_str() {
        "copilot" => {
            // Copilot supports multiple upstream model IDs; prefer provider-specific config.
            let provider_model = config
                .providers
                .copilot
                .as_ref()
                .and_then(|c| c.model.clone());

            Ok(provider_model.unwrap_or_else(|| "gpt-4o".to_string()))
        }
        "openai" => {
            let openai_config = config
                .providers
                .openai
                .as_ref()
                .ok_or_else(|| LLMError::Auth("OpenAI configuration required".to_string()))?;

            openai_config.model.clone().ok_or_else(|| {
                LLMError::Auth("OpenAI model must be specified in config".to_string())
            })
        }
        "anthropic" => {
            let anthropic_config =
                config.providers.anthropic.as_ref().ok_or_else(|| {
                    LLMError::Auth("Anthropic configuration required".to_string())
                })?;

            anthropic_config.model.clone().ok_or_else(|| {
                LLMError::Auth("Anthropic model must be specified in config".to_string())
            })
        }
        "gemini" => {
            let gemini_config = config
                .providers
                .gemini
                .as_ref()
                .ok_or_else(|| LLMError::Auth("Gemini configuration required".to_string()))?;

            gemini_config.model.clone().ok_or_else(|| {
                LLMError::Auth("Gemini model must be specified in config".to_string())
            })
        }
        _ => Err(LLMError::Auth(format!(
            "Unknown provider: {}",
            config.provider
        ))),
    }
}

/// Get the memory/background summarization model for the current provider from config.
///
/// Used for lightweight memory tasks like session summarization and reflection.
/// Falls back to the provider fast model when `memory.background_model` is not configured.
pub fn get_memory_background_model_from_config(config: &Config) -> Result<String, LLMError> {
    config.get_memory_background_model().ok_or_else(|| {
        LLMError::Auth(format!(
            "No background memory model configured for provider '{}'",
            config.provider
        ))
    })
}

/// Get the vision-capable model for the current provider from config.
///
/// Used for image understanding tasks.
/// Falls back to the default model when no vision_model is configured.
pub fn get_vision_model_from_config(config: &Config) -> Result<String, LLMError> {
    config.get_vision_model().ok_or_else(|| {
        LLMError::Auth(format!(
            "No model configured for provider '{}'",
            config.provider
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{CopilotConfig, OpenAIConfig, ProviderConfigs};

    #[test]
    fn test_get_model_from_openai_config() {
        let config = Config {
            provider: "openai".to_string(),
            providers: ProviderConfigs {
                openai: Some(OpenAIConfig {
                    api_key: "test".to_string(),
                    api_key_encrypted: None,
                    base_url: None,
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
            ..Config::default()
        };

        let result = get_default_model_from_config(&config);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "gpt-4o");
    }

    #[test]
    fn test_error_when_model_not_configured() {
        let config = Config {
            provider: "openai".to_string(),
            providers: ProviderConfigs {
                openai: Some(OpenAIConfig {
                    api_key: "test".to_string(),
                    api_key_encrypted: None,
                    base_url: None,
                    model: None, // No model configured
                    fast_model: None,
                    vision_model: None,
                    reasoning_effort: None,
                    responses_only_models: vec![],
                    request_overrides: None,
                    extra: Default::default(),
                }),
                ..ProviderConfigs::default()
            },
            ..Config::default()
        };

        let result = get_default_model_from_config(&config);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("model must be specified"));
    }

    #[test]
    fn test_get_model_from_copilot_provider_config() {
        let config = Config {
            provider: "copilot".to_string(),
            providers: ProviderConfigs {
                copilot: Some(CopilotConfig {
                    enabled: true,
                    headless_auth: false,
                    model: Some("gpt-4o-mini".to_string()),
                    fast_model: None,
                    vision_model: None,
                    reasoning_effort: None,
                    responses_only_models: vec![],
                    request_overrides: None,
                    extra: Default::default(),
                }),
                ..ProviderConfigs::default()
            },
            ..Config::default()
        };

        let result = get_default_model_from_config(&config);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "gpt-4o-mini");
    }

    #[test]
    fn test_get_model_from_copilot_default_fallback() {
        let config = Config {
            provider: "copilot".to_string(),
            providers: ProviderConfigs::default(),
            ..Config::default()
        };

        let result = get_default_model_from_config(&config);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "gpt-4o");
    }
}
