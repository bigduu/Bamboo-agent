// Helper function to extract default model from config
// This should be used instead of hardcoding "gpt-4o-mini" or "default"

use crate::agent::llm::LLMError;
use crate::core::Config;

/// Get the default model for the current provider from config
/// Returns an error if no model is configured
pub fn get_default_model_from_config(config: &Config) -> Result<String, LLMError> {
    match config.provider.as_str() {
        "copilot" => {
            // Copilot has default models, but can be overridden
            // If no model is specified, use a sensible default
            Ok(config.model.clone().unwrap_or_else(|| "gpt-4o".to_string()))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{OpenAIConfig, ProviderConfigs};

    #[test]
    fn test_get_model_from_openai_config() {
        let config = Config {
            provider: "openai".to_string(),
            providers: ProviderConfigs {
                openai: Some(OpenAIConfig {
                    api_key: "test".to_string(),
                    base_url: None,
                    model: Some("gpt-4o".to_string()),
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
                    base_url: None,
                    model: None, // No model configured
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
}
