//! Multi-provider registry that holds one initialized [`LLMProvider`] per configured provider.
//!
//! Supports two modes:
//! 1. **Legacy** — keyed by provider type name (e.g. `"openai"`, `"anthropic"`).
//! 2. **Instance-keyed** — keyed by instance id (e.g. `"openai-work"`, `"anthropic-personal"`),
//!    with legacy providers automatically synthesized as default instances.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crate::config::Config;
use crate::config::ProviderInstanceConfig;
use crate::llm::provider::{LLMError, LLMProvider};
use crate::llm::provider_factory::create_provider_by_name;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderMetadata {
    pub id: String,
    pub provider_type: String,
    pub display_name: String,
}

/// Holds one initialized [`LLMProvider`] per provider name or instance id.
pub struct ProviderRegistry {
    providers: RwLock<HashMap<String, Arc<dyn LLMProvider>>>,
    metadata: RwLock<HashMap<String, ProviderMetadata>>,
    default_provider: RwLock<String>,
}

impl ProviderRegistry {
    /// Create a registry from a pre-built provider map.
    ///
    /// Used by tests and higher-level orchestration that manages
    /// provider lifecycle independently of config files.
    pub fn new(providers: HashMap<String, Arc<dyn LLMProvider>>, default_provider: String) -> Self {
        let metadata = providers
            .keys()
            .map(|id| {
                (
                    id.clone(),
                    ProviderMetadata {
                        id: id.clone(),
                        provider_type: id.clone(),
                        display_name: display_name_for_provider_type(id),
                    },
                )
            })
            .collect();

        Self::new_with_metadata(providers, metadata, default_provider)
    }

    pub fn new_with_metadata(
        providers: HashMap<String, Arc<dyn LLMProvider>>,
        metadata: HashMap<String, ProviderMetadata>,
        default_provider: String,
    ) -> Self {
        Self {
            providers: RwLock::new(providers),
            metadata: RwLock::new(metadata),
            default_provider: RwLock::new(default_provider),
        }
    }

    /// Create a registry by initializing every configured provider.
    ///
    /// When `config.provider_instances` is non-empty, each instance is initialized
    /// by translating the instance config into the shape expected by
    /// [`create_provider_by_name`]. Legacy providers are also included as
    /// synthesized instances (via [`crate::config::synthesize_legacy_instances`]).
    ///
    /// Providers that fail to initialize (missing API key, auth failure, etc.)
    /// are skipped with a warning log rather than aborting the entire startup.
    pub async fn from_config(config: &Config, app_data_dir: PathBuf) -> Result<Self, LLMError> {
        let (providers, metadata, default_provider) =
            Self::build_registry_state(config, app_data_dir).await?;
        Ok(Self::new_with_metadata(
            providers,
            metadata,
            default_provider,
        ))
    }

    /// Rebuild the registry in-place from config so existing holders of the outer
    /// `Arc<ProviderRegistry>` automatically observe refreshed providers.
    pub async fn reload_from_config(
        &self,
        config: &Config,
        app_data_dir: PathBuf,
    ) -> Result<(), LLMError> {
        let (providers, metadata, default_provider) =
            Self::build_registry_state(config, app_data_dir).await?;
        *self
            .providers
            .write()
            .expect("provider registry write lock poisoned") = providers;
        *self
            .metadata
            .write()
            .expect("provider registry metadata write lock poisoned") = metadata;
        *self
            .default_provider
            .write()
            .expect("provider registry default write lock poisoned") = default_provider;
        Ok(())
    }

    async fn build_registry_state(
        config: &Config,
        app_data_dir: PathBuf,
    ) -> Result<
        (
            HashMap<String, Arc<dyn LLMProvider>>,
            HashMap<String, ProviderMetadata>,
            String,
        ),
        LLMError,
    > {
        if config.has_provider_instances() {
            return Self::build_registry_state_from_instances(config, app_data_dir).await;
        }

        // Legacy path: iterate known provider types.
        let mut providers: HashMap<String, Arc<dyn LLMProvider>> = HashMap::new();
        let mut metadata: HashMap<String, ProviderMetadata> = HashMap::new();

        for name in crate::llm::provider_factory::AVAILABLE_PROVIDERS {
            if !provider_is_configured(config, name) {
                continue;
            }

            match create_provider_by_name(config, name, app_data_dir.clone()).await {
                Ok(provider) => {
                    tracing::info!(provider = name, "Provider initialized");
                    providers.insert(name.to_string(), provider);
                    metadata.insert(
                        name.to_string(),
                        ProviderMetadata {
                            id: name.to_string(),
                            provider_type: name.to_string(),
                            display_name: display_name_for_provider_type(name),
                        },
                    );
                }
                Err(e) => {
                    tracing::warn!(provider = name, error = %e, "Provider failed to initialize, skipping");
                }
            }
        }

        Ok((providers, metadata, config.provider.clone()))
    }

    /// Instance-keyed registry construction.
    ///
    /// Iterates all entries in `config.provider_instances` plus synthesized
    /// legacy instances, building a provider for each and keying by instance id.
    async fn build_registry_state_from_instances(
        config: &Config,
        app_data_dir: PathBuf,
    ) -> Result<
        (
            HashMap<String, Arc<dyn LLMProvider>>,
            HashMap<String, ProviderMetadata>,
            String,
        ),
        LLMError,
    > {
        let mut providers: HashMap<String, Arc<dyn LLMProvider>> = HashMap::new();
        let mut metadata: HashMap<String, ProviderMetadata> = HashMap::new();

        // Build a synthetic config per instance and create the provider.
        for (instance_id, instance) in &config.provider_instances {
            if !instance.enabled {
                tracing::info!(instance_id, "Provider instance disabled, skipping");
                continue;
            }

            match Self::create_instance_provider(config, instance, app_data_dir.clone()).await {
                Ok(provider) => {
                    tracing::info!(
                        instance_id,
                        provider_type = &instance.provider_type,
                        "Provider instance initialized"
                    );
                    providers.insert(instance_id.clone(), provider);
                    metadata.insert(
                        instance_id.clone(),
                        ProviderMetadata {
                            id: instance_id.clone(),
                            provider_type: instance.provider_type.clone(),
                            display_name: instance
                                .label
                                .clone()
                                .filter(|label| !label.trim().is_empty())
                                .unwrap_or_else(|| {
                                    display_name_for_provider_type(&instance.provider_type)
                                }),
                        },
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        instance_id,
                        provider_type = &instance.provider_type,
                        error = %e,
                        "Provider instance failed to initialize, skipping"
                    );
                }
            }
        }

        // Also synthesize legacy providers that don't overlap with explicit instances.
        let legacy = crate::config::synthesize_legacy_instances(config);
        for (instance_id, instance_cfg) in legacy {
            if !instance_cfg.enabled {
                continue;
            }
            if providers.contains_key(&instance_id) {
                continue; // explicit instance takes precedence
            }

            match Self::create_instance_provider(config, &instance_cfg, app_data_dir.clone()).await
            {
                Ok(provider) => {
                    tracing::info!(
                        instance_id,
                        provider_type = &instance_cfg.provider_type,
                        "Legacy provider instance synthesized"
                    );
                    providers.insert(instance_id.clone(), provider);
                    metadata.insert(
                        instance_id.clone(),
                        ProviderMetadata {
                            id: instance_id,
                            provider_type: instance_cfg.provider_type.clone(),
                            display_name: instance_cfg
                                .label
                                .clone()
                                .filter(|label| !label.trim().is_empty())
                                .unwrap_or_else(|| {
                                    display_name_for_provider_type(&instance_cfg.provider_type)
                                }),
                        },
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        instance_id,
                        error = %e,
                        "Legacy provider instance failed to initialize"
                    );
                }
            }
        }

        Ok((
            providers,
            metadata,
            config.effective_default_provider().to_string(),
        ))
    }

    /// Create a single provider from a [`ProviderInstanceConfig`].
    ///
    /// This works by building a temporary [`Config`] that has the instance's
    /// credentials projected into the legacy provider slot, then delegating
    /// to [`create_provider_by_name`].
    async fn create_instance_provider(
        base_config: &Config,
        instance: &ProviderInstanceConfig,
        app_data_dir: PathBuf,
    ) -> Result<Arc<dyn LLMProvider>, LLMError> {
        let mut temp_config = base_config.clone();
        // Project the instance's credentials into the legacy provider slots.
        apply_instance_to_config(&mut temp_config, instance);
        create_provider_by_name(&temp_config, &instance.provider_type, app_data_dir).await
    }

    /// Get a provider by name or instance id.
    pub fn get(&self, name: &str) -> Option<Arc<dyn LLMProvider>> {
        self.providers
            .read()
            .expect("provider registry read lock poisoned")
            .get(name)
            .cloned()
    }

    pub fn get_metadata(&self, name: &str) -> Option<ProviderMetadata> {
        self.metadata
            .read()
            .expect("provider registry metadata read lock poisoned")
            .get(name)
            .cloned()
    }

    pub fn provider_metadata(&self) -> Vec<ProviderMetadata> {
        self.metadata
            .read()
            .expect("provider registry metadata read lock poisoned")
            .values()
            .cloned()
            .collect()
    }

    /// Get the default provider (the one configured as `config.provider`).
    pub fn get_default(&self) -> Option<Arc<dyn LLMProvider>> {
        let default_name = self.default_provider_name();
        self.get(&default_name)
    }

    /// The default provider name.
    pub fn default_provider_name(&self) -> String {
        self.default_provider
            .read()
            .expect("provider registry default read lock poisoned")
            .clone()
    }

    /// All provider names that were successfully initialized.
    pub fn provider_names(&self) -> Vec<String> {
        self.providers
            .read()
            .expect("provider registry read lock poisoned")
            .keys()
            .cloned()
            .collect()
    }

    /// Number of initialized providers.
    pub fn len(&self) -> usize {
        self.providers
            .read()
            .expect("provider registry read lock poisoned")
            .len()
    }

    /// Whether any providers are registered.
    pub fn is_empty(&self) -> bool {
        self.providers
            .read()
            .expect("provider registry read lock poisoned")
            .is_empty()
    }

    /// Insert or replace a provider at runtime (used by instance CRUD / tests).
    pub fn insert(&self, key: String, provider: Arc<dyn LLMProvider>) {
        self.providers
            .write()
            .expect("provider registry write lock poisoned")
            .insert(key.clone(), provider);
        self.metadata
            .write()
            .expect("provider registry metadata write lock poisoned")
            .insert(
                key.clone(),
                ProviderMetadata {
                    id: key.clone(),
                    provider_type: key.clone(),
                    display_name: display_name_for_provider_type(&key),
                },
            );
    }

    /// Remove a provider by key at runtime (used by instance CRUD / tests).
    pub fn remove(&self, key: &str) -> Option<Arc<dyn LLMProvider>> {
        self.metadata
            .write()
            .expect("provider registry metadata write lock poisoned")
            .remove(key);
        self.providers
            .write()
            .expect("provider registry write lock poisoned")
            .remove(key)
    }

    /// Update the default provider key.
    pub fn set_default(&self, key: String) {
        *self
            .default_provider
            .write()
            .expect("provider registry default write lock poisoned") = key;
    }
}

/// Project an instance's credentials into the legacy `Config` provider slots
/// so that `create_provider_by_name` can consume them.
fn apply_instance_to_config(config: &mut Config, instance: &ProviderInstanceConfig) {
    match instance.provider_type.as_str() {
        "openai" => {
            config.providers.openai = Some(crate::config::OpenAIConfig {
                api_key: instance.api_key.clone(),
                api_key_encrypted: instance.api_key_encrypted.clone(),
                base_url: instance.base_url.clone(),
                model: instance.model.clone(),
                fast_model: instance.fast_model.clone(),
                vision_model: instance.vision_model.clone(),
                reasoning_effort: instance.reasoning_effort,
                responses_only_models: instance.responses_only_models.clone(),
                request_overrides: instance.request_overrides.clone(),
                extra: Default::default(),
            });
        }
        "anthropic" => {
            config.providers.anthropic = Some(crate::config::AnthropicConfig {
                api_key: instance.api_key.clone(),
                api_key_encrypted: instance.api_key_encrypted.clone(),
                base_url: instance.base_url.clone(),
                model: instance.model.clone(),
                fast_model: instance.fast_model.clone(),
                vision_model: instance.vision_model.clone(),
                max_tokens: None,
                reasoning_effort: instance.reasoning_effort,
                request_overrides: instance.request_overrides.clone(),
                extra: Default::default(),
            });
        }
        "gemini" => {
            config.providers.gemini = Some(crate::config::GeminiConfig {
                api_key: instance.api_key.clone(),
                api_key_encrypted: instance.api_key_encrypted.clone(),
                base_url: instance.base_url.clone(),
                model: instance.model.clone(),
                fast_model: instance.fast_model.clone(),
                vision_model: instance.vision_model.clone(),
                reasoning_effort: instance.reasoning_effort,
                request_overrides: instance.request_overrides.clone(),
                extra: Default::default(),
            });
        }
        "copilot" => {
            // Copilot uses device auth; just set the model overrides.
            let existing = config.providers.copilot.take();
            config.providers.copilot = Some(crate::config::CopilotConfig {
                enabled: true,
                headless_auth: existing.as_ref().map(|c| c.headless_auth).unwrap_or(false),
                model: instance.model.clone(),
                fast_model: instance.fast_model.clone(),
                vision_model: instance.vision_model.clone(),
                reasoning_effort: instance.reasoning_effort,
                responses_only_models: instance.responses_only_models.clone(),
                request_overrides: instance.request_overrides.clone(),
                extra: Default::default(),
            });
        }
        "bodhi" => {
            config.providers.bodhi = Some(crate::config::BodhiConfig {
                api_key: instance.api_key.clone(),
                api_key_encrypted: instance.api_key_encrypted.clone(),
                base_url: instance.base_url.clone(),
                target_provider: instance
                    .extra
                    .get("target_provider")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                reasoning_effort: instance.reasoning_effort,
                extra: Default::default(),
            });
        }
        _ => {}
    }
    // Set provider type so create_provider_by_name targets the right one.
    config.provider = instance.provider_type.clone();
}

fn display_name_for_provider_type(id: &str) -> String {
    match id {
        "openai" => "OpenAI".to_string(),
        "anthropic" => "Anthropic".to_string(),
        "gemini" => "Gemini".to_string(),
        "copilot" => "GitHub Copilot".to_string(),
        "bodhi" => "Bodhi".to_string(),
        other => other.to_string(),
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

    #[test]
    fn test_insert_and_remove() {
        use bamboo_domain::Message;
        use bamboo_domain::ToolSchema;

        struct NoopProvider;
        #[async_trait::async_trait]
        impl LLMProvider for NoopProvider {
            async fn chat_stream(
                &self,
                _messages: &[Message],
                _tools: &[ToolSchema],
                _max_output_tokens: Option<u32>,
                _model: &str,
            ) -> crate::llm::provider::Result<crate::llm::provider::LLMStream> {
                Err(LLMError::Api("noop".to_string()))
            }
        }

        let registry = ProviderRegistry::new(HashMap::new(), "default".to_string());
        assert!(registry.is_empty());

        registry.insert("test".to_string(), Arc::new(NoopProvider));
        assert_eq!(registry.len(), 1);
        assert!(registry.get("test").is_some());
        assert_eq!(
            registry.get_metadata("test").map(|m| m.display_name),
            Some("test".to_string())
        );

        let removed = registry.remove("test");
        assert!(removed.is_some());
        assert!(registry.is_empty());
        assert!(registry.get_metadata("test").is_none());
    }

    #[test]
    fn test_set_default() {
        let registry = ProviderRegistry::new(HashMap::new(), "old-default".to_string());
        assert_eq!(registry.default_provider_name(), "old-default");
        registry.set_default("new-default".to_string());
        assert_eq!(registry.default_provider_name(), "new-default");
    }

    #[test]
    fn test_apply_instance_to_config_openai() {
        let mut config = Config::default();
        let instance = ProviderInstanceConfig {
            provider_type: "openai".to_string(),
            label: Some("Test OpenAI".to_string()),
            api_key: "sk-instance-key".to_string(),
            api_key_encrypted: None,
            base_url: Some("https://custom.api.com/v1".to_string()),
            model: Some("gpt-4o".to_string()),
            fast_model: Some("gpt-4o-mini".to_string()),
            vision_model: None,
            reasoning_effort: None,
            responses_only_models: vec![],
            request_overrides: None,
            enabled: true,
            extra: Default::default(),
        };

        apply_instance_to_config(&mut config, &instance);

        let openai = config
            .providers
            .openai
            .as_ref()
            .expect("openai should be set");
        assert_eq!(openai.api_key, "sk-instance-key");
        assert_eq!(
            openai.base_url.as_deref(),
            Some("https://custom.api.com/v1")
        );
        assert_eq!(openai.model.as_deref(), Some("gpt-4o"));
        assert_eq!(config.provider, "openai");
    }
}
