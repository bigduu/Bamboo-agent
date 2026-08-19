//! Multi-provider registry that holds one initialized [`LLMProvider`] per configured provider.
//!
//! Supports two modes:
//! 1. **Legacy** — keyed by provider type name (e.g. `"openai"`, `"anthropic"`).
//! 2. **Instance-keyed** — keyed by instance id (e.g. `"openai-work"`, `"anthropic-personal"`).
//!
//! Instance configuration is authoritative in the second mode. A narrow
//! synthesized-legacy branch remains only for hybrid configurations whose
//! default still names a legacy alias during the Lotus migration window.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crate::provider::{LLMError, LLMProvider};
use crate::provider_factory::{create_provider_by_name, create_provider_from_instance};
use bamboo_config::Config;
use bamboo_config::ProviderInstanceConfig;
use bamboo_domain::poison::PoisonRecover;

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
    /// directly from its native configuration. Legacy providers are included only
    /// as a compatibility seam for non-overlapping hybrid aliases.
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
        // Recover from a poisoned lock rather than panicking the whole process: a
        // poisoned guard's inner data is still usable, and panicking here would be a
        // permanent DoS on the critical path for every LLM call.
        *self.providers.write().recover_poison() = providers;
        *self.metadata.write().recover_poison() = metadata;
        *self.default_provider.write().recover_poison() = default_provider;
        Ok(())
    }

    /// Replace this registry with an already constructed candidate.
    ///
    /// Live reload callers use this only after verifying that the candidate's
    /// default provider initialized successfully. Keeping construction separate
    /// from publication means a bad edit cannot tear down the working registry.
    pub fn replace_with(&self, candidate: Self) {
        *self.providers.write().recover_poison() =
            candidate.providers.into_inner().recover_poison();
        *self.metadata.write().recover_poison() = candidate.metadata.into_inner().recover_poison();
        *self.default_provider.write().recover_poison() =
            candidate.default_provider.into_inner().recover_poison();
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

        for name in crate::provider_factory::AVAILABLE_PROVIDERS {
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

        // Narrow compatibility seam for #780-era hybrid configs: the effective
        // default may still name a legacy alias while other native instances
        // exist. Never synthesize unrelated stale aliases, and never shadow an
        // explicit instance with the same id.
        {
            let legacy_default_id = config.effective_default_provider();
            let instance_cfg = (!config.provider_instances.contains_key(legacy_default_id))
                .then(|| {
                    bamboo_config::synthesize_legacy_instances(config)
                        .into_iter()
                        .find_map(|(id, instance)| (id == legacy_default_id).then_some(instance))
                })
                .flatten()
                .filter(|instance| instance.enabled);

            if let Some(instance_cfg) = instance_cfg {
                match Self::create_instance_provider(config, &instance_cfg, app_data_dir.clone())
                    .await
                {
                    Ok(provider) => {
                        tracing::info!(
                            instance_id = legacy_default_id,
                            provider_type = &instance_cfg.provider_type,
                            "Legacy default alias synthesized for hybrid compatibility"
                        );
                        providers.insert(legacy_default_id.to_string(), provider);
                        metadata.insert(
                            legacy_default_id.to_string(),
                            ProviderMetadata {
                                id: legacy_default_id.to_string(),
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
                            instance_id = legacy_default_id,
                            error = %e,
                            "Hybrid legacy default alias failed to initialize"
                        );
                    }
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
    /// The instance remains the runtime authority; no legacy config slot is
    /// populated as an intermediate representation.
    async fn create_instance_provider(
        base_config: &Config,
        instance: &ProviderInstanceConfig,
        app_data_dir: PathBuf,
    ) -> Result<Arc<dyn LLMProvider>, LLMError> {
        create_provider_from_instance(base_config, instance, app_data_dir).await
    }

    /// Get a provider by name or instance id.
    pub fn get(&self, name: &str) -> Option<Arc<dyn LLMProvider>> {
        self.providers.read().recover_poison().get(name).cloned()
    }

    pub fn get_metadata(&self, name: &str) -> Option<ProviderMetadata> {
        self.metadata.read().recover_poison().get(name).cloned()
    }

    pub fn provider_metadata(&self) -> Vec<ProviderMetadata> {
        self.metadata
            .read()
            .recover_poison()
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
        self.default_provider.read().recover_poison().clone()
    }

    /// All provider names that were successfully initialized.
    pub fn provider_names(&self) -> Vec<String> {
        self.providers
            .read()
            .recover_poison()
            .keys()
            .cloned()
            .collect()
    }

    /// Number of initialized providers.
    pub fn len(&self) -> usize {
        self.providers.read().recover_poison().len()
    }

    /// Whether any providers are registered.
    pub fn is_empty(&self) -> bool {
        self.providers.read().recover_poison().is_empty()
    }

    /// Insert or replace a provider at runtime (used by instance CRUD / tests).
    pub fn insert(&self, key: String, provider: Arc<dyn LLMProvider>) {
        self.providers
            .write()
            .recover_poison()
            .insert(key.clone(), provider);
        self.metadata.write().recover_poison().insert(
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
        self.metadata.write().recover_poison().remove(key);
        self.providers.write().recover_poison().remove(key)
    }

    /// Update the default provider key.
    pub fn set_default(&self, key: String) {
        *self.default_provider.write().recover_poison() = key;
    }
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
            .providers()
            .openai
            .as_ref()
            .map(|c| !c.api_key.is_empty())
            .unwrap_or(false),
        "anthropic" => config
            .providers()
            .anthropic
            .as_ref()
            .map(|c| !c.api_key.is_empty())
            .unwrap_or(false),
        "gemini" => config
            .providers()
            .gemini
            .as_ref()
            .map(|c| !c.api_key.is_empty())
            .unwrap_or(false),
        "bodhi" => config
            .providers()
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
    use bamboo_config::OpenAIConfig;

    fn test_openai_config() -> OpenAIConfig {
        OpenAIConfig {
            api_key: "sk-test".to_string(),
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
        }
    }

    #[test]
    fn test_provider_is_configured() {
        let mut config = Config::default();
        *config.providers_mut() = bamboo_config::ProviderConfigs {
            openai: Some(test_openai_config()),
            ..bamboo_config::ProviderConfigs::default()
        };

        assert!(provider_is_configured(&config, "copilot"));
        assert!(provider_is_configured(&config, "openai"));
        assert!(!provider_is_configured(&config, "anthropic"));
        assert!(!provider_is_configured(&config, "gemini"));
    }

    #[test]
    fn test_provider_is_configured_empty_key() {
        let mut config = Config::default();
        *config.providers_mut() = bamboo_config::ProviderConfigs {
            openai: Some(OpenAIConfig {
                api_key: String::new(),
                api_key_from_env: false,
                ..test_openai_config()
            }),
            ..bamboo_config::ProviderConfigs::default()
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
            ) -> crate::provider::Result<crate::provider::LLMStream> {
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

    #[tokio::test]
    async fn explicit_instance_failure_does_not_fall_back_to_stale_legacy_alias() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        *config.providers_mut() = bamboo_config::ProviderConfigs {
            openai: Some(OpenAIConfig {
                api_key: "sk-stale-legacy".to_string(),
                ..OpenAIConfig::default()
            }),
            ..Default::default()
        };
        config.provider_instances.insert(
            "openai".to_string(),
            serde_json::from_value(serde_json::json!({
                "provider_type": "openai",
                "api_key": "",
                "enabled": true
            }))
            .unwrap(),
        );
        config.default_provider_instance = Some("openai".to_string());

        let registry = ProviderRegistry::from_config(&config, temp.path().to_path_buf())
            .await
            .unwrap();

        assert!(registry.get("openai").is_none());
        assert!(registry.get_default().is_none());
    }

    #[tokio::test]
    async fn hybrid_legacy_default_alias_remains_resolvable_without_other_stale_aliases() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        *config.providers_mut() = bamboo_config::ProviderConfigs {
            openai: Some(OpenAIConfig {
                api_key: "sk-legacy-default".to_string(),
                ..OpenAIConfig::default()
            }),
            anthropic: Some(bamboo_config::AnthropicConfig {
                api_key: "sk-stale-anthropic".to_string(),
                ..bamboo_config::AnthropicConfig::default()
            }),
            ..Default::default()
        };
        config.provider_instances.insert(
            "work".to_string(),
            serde_json::from_value(serde_json::json!({
                "provider_type": "openai",
                "api_key": "sk-work",
                "enabled": true
            }))
            .unwrap(),
        );
        config.default_provider_instance = Some("openai".to_string());

        let registry = ProviderRegistry::from_config(&config, temp.path().to_path_buf())
            .await
            .unwrap();

        assert_eq!(registry.default_provider_name(), "openai");
        assert!(registry.get_default().is_some());
        assert!(registry.get("work").is_some());
        assert!(registry.get("anthropic").is_none());
    }

    #[tokio::test]
    async fn hybrid_legacy_provider_fallback_without_explicit_default_remains_resolvable() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.provider = "anthropic".to_string();
        *config.providers_mut() = bamboo_config::ProviderConfigs {
            anthropic: Some(bamboo_config::AnthropicConfig {
                api_key: "sk-legacy-default".to_string(),
                ..bamboo_config::AnthropicConfig::default()
            }),
            ..Default::default()
        };
        config.provider_instances.insert(
            "work".to_string(),
            serde_json::from_value(serde_json::json!({
                "provider_type": "openai",
                "api_key": "sk-work",
                "enabled": true
            }))
            .unwrap(),
        );

        let registry = ProviderRegistry::from_config(&config, temp.path().to_path_buf())
            .await
            .unwrap();

        assert_eq!(registry.default_provider_name(), "anthropic");
        assert!(registry.get_default().is_some());
        assert!(registry.get("work").is_some());
    }

    /// A poisoned lock must not brick the registry: every subsequent operation
    /// recovers the (still-usable) inner data instead of panicking the process.
    #[test]
    fn poisoned_lock_does_not_panic_subsequent_reads() {
        use std::panic::{catch_unwind, AssertUnwindSafe};

        let registry = ProviderRegistry::new(HashMap::new(), "default".to_string());

        // Poison the `providers` RwLock by panicking while holding a write guard.
        // The guard is dropped during unwinding, which is exactly what marks the
        // lock as poisoned.
        let poisoned = catch_unwind(AssertUnwindSafe(|| {
            let _guard = registry.providers.write().unwrap();
            panic!("intentional poison for test");
        }));
        assert!(poisoned.is_err(), "setup panic should have been caught");

        // Despite poisoning, all of these must succeed instead of propagating a
        // PoisonError panic — that is the regression this test guards against.
        assert_eq!(registry.len(), 0);
        assert!(registry.is_empty());
        assert!(registry.get("missing").is_none());
        assert!(registry.provider_names().is_empty());

        // Writes must recover too: a subsequent insert still lands.
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
            ) -> crate::provider::Result<crate::provider::LLMStream> {
                Err(LLMError::Api("noop".to_string()))
            }
        }
        registry.insert("after-poison".to_string(), Arc::new(NoopProvider));
        assert_eq!(registry.len(), 1);
        assert!(registry.get("after-poison").is_some());
        assert_eq!(
            registry.provider_metadata().len(),
            1,
            "metadata RwLock should also recover independently"
        );
    }
}
