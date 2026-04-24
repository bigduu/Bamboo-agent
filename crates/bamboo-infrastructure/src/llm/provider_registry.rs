//! Multi-provider registry that holds one initialized [`LLMProvider`] per configured provider.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::config::Config;
use crate::llm::provider::{LLMError, LLMProvider};
use crate::llm::provider_factory::create_provider_by_name;

/// Holds one initialized [`LLMProvider`] per provider name.
pub struct ProviderRegistry {
    providers: HashMap<String, Arc<dyn LLMProvider>>,
    default_provider: String,
}

impl ProviderRegistry {
    /// Create a registry from a pre-built provider map.
    ///
    /// Used by tests and higher-level orchestration that manages
    /// provider lifecycle independently of config files.
    pub fn new(
        providers: HashMap<String, Arc<dyn LLMProvider>>,
        default_provider: String,
    ) -> Self {
        Self {
            providers,
            default_provider,
        }
    }

    /// Create a registry by initializing every configured provider.
    ///
    /// Providers that fail to initialize (missing API key, auth failure, etc.)
    /// are skipped with a warning log rather than aborting the entire startup.
    pub async fn from_config(
        config: &Config,
        app_data_dir: PathBuf,
    ) -> Result<Self, LLMError> {
        let mut providers: HashMap<String, Arc<dyn LLMProvider>> = HashMap::new();

        for name in crate::llm::provider_factory::AVAILABLE_PROVIDERS {
            if !provider_is_configured(config, name) {
                continue;
            }

            match create_provider_by_name(config, name, app_data_dir.clone()).await {
                Ok(provider) => {
                    tracing::info!(provider = name, "Provider initialized");
                    providers.insert(name.to_string(), provider);
                }
                Err(e) => {
                    tracing::warn!(provider = name, error = %e, "Provider failed to initialize, skipping");
                }
            }
        }

        let default_provider = config.provider.clone();

        Ok(Self {
            providers,
            default_provider,
        })
    }

    /// Get a provider by name.
    pub fn get(&self, name: &str) -> Option<Arc<dyn LLMProvider>> {
        self.providers.get(name).cloned()
    }

    /// Get the default provider (the one configured as `config.provider`).
    pub fn get_default(&self) -> Option<Arc<dyn LLMProvider>> {
        self.get(&self.default_provider)
    }

    /// The default provider name.
    pub fn default_provider_name(&self) -> &str {
        &self.default_provider
    }

    /// All provider names that were successfully initialized.
    pub fn provider_names(&self) -> Vec<String> {
        self.providers.keys().cloned().collect()
    }

    /// Number of initialized providers.
    pub fn len(&self) -> usize {
        self.providers.len()
    }

    /// Whether any providers are registered.
    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }
}

/// Check whether a provider has enough configuration to attempt initialization.
fn provider_is_configured(config: &Config, name: &str) -> bool {
    match name {
        "copilot" => true, // Copilot can be attempted without explicit config
        "openai" => config
            .providers
            .openai
            .as_ref()
            .map(|c| !c.api_key.is_empty())
            .unwrap_or(false),
        "anthropic" => config
            .providers
            .anthropic
            .as_ref()
            .map(|c| !c.api_key.is_empty())
            .unwrap_or(false),
        "gemini" => config
            .providers
            .gemini
            .as_ref()
            .map(|c| !c.api_key.is_empty())
            .unwrap_or(false),
        "bodhi" => config
            .providers
            .bodhi
            .as_ref()
            .map(|c| !c.api_key.is_empty())
            .unwrap_or(false),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::OpenAIConfig;

    fn test_openai_config() -> OpenAIConfig {
        OpenAIConfig {
            api_key: "sk-test".to_string(),
            api_key_encrypted: None,
            base_url: None,
            model: None,
            fast_model: None,
            vision_model: None,
            reasoning_effort: None,
            responses_only_models: vec![],
            request_overrides: None,
            extra: Default::default(),
        }
    }

    #[test]
    fn test_provider_is_configured() {
        let config = Config {
            providers: crate::config::ProviderConfigs {
                openai: Some(test_openai_config()),
                ..crate::config::ProviderConfigs::default()
            },
            ..Config::default()
        };

        assert!(provider_is_configured(&config, "copilot"));
        assert!(provider_is_configured(&config, "openai"));
        assert!(!provider_is_configured(&config, "anthropic"));
        assert!(!provider_is_configured(&config, "gemini"));
    }

    #[test]
    fn test_provider_is_configured_empty_key() {
        let config = Config {
            providers: crate::config::ProviderConfigs {
                openai: Some(OpenAIConfig {
                    api_key: String::new(),
                    ..test_openai_config()
                }),
                ..crate::config::ProviderConfigs::default()
            },
            ..Config::default()
        };

        assert!(!provider_is_configured(&config, "openai"));
    }
}
