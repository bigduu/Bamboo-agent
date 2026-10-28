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
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crate::provider::{LLMError, LLMProvider};
use crate::provider_factory::{create_provider_by_name, create_provider_from_instance};
use crate::providers::common::{llm_error_kind, log_identity};
use bamboo_config::Config;
use bamboo_config::ProviderInstanceConfig;
use bamboo_domain::poison::PoisonRecover;
use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};
use subtle::ConstantTimeEq;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderMetadata {
    pub id: String,
    pub provider_type: String,
    pub display_name: String,
}

/// Holds one initialized [`LLMProvider`] per provider name or instance id.
pub struct ProviderRegistry {
    state: RwLock<RegistryState>,
}

struct RegistryState {
    providers: HashMap<String, Arc<dyn LLMProvider>>,
    metadata: HashMap<String, ProviderMetadata>,
    default_provider: String,
    // None preserves registries managed by embedders rather than Config. A
    // config-built registry always has a map, including explicit empty lists.
    runtime_models: Option<HashMap<String, Vec<String>>>,
    origin: RegistryOrigin,
}

// Neither the key nor the tags are exposed, serialized, or formatted. Tags
// retain no plaintext; credentials are fed into HMAC through a writer.
enum RegistryOrigin {
    Unmanaged,
    Managed {
        key: ring::hmac::Key,
        bindings: HashMap<String, Binding>,
    },
}

struct Binding([u8; 32]);

/// Opaque proof of the provider factory inputs used for one managed run.
/// It retains only the registry's HMAC key/tag and the selected handle, never
/// a credential value. Do not serialize, format or log this object.
#[doc(hidden)]
pub struct ManagedProviderOrigin {
    registry: Arc<ProviderRegistry>,
    provider_id: String,
    key: ring::hmac::Key,
    original_binding: Option<[u8; 32]>,
    selected: Arc<dyn LLMProvider>,
}

impl ManagedProviderOrigin {
    pub(crate) fn validate_gold_target(
        &self,
        config: &Config,
        target: &bamboo_domain::ProviderModelRef,
        expected: &Arc<dyn LLMProvider>,
    ) -> Result<(), LLMError> {
        if target.provider != self.provider_id || !Arc::ptr_eq(&self.selected, expected) {
            return Err(LLMError::Api(
                "Gold provider differs from the selected run target".into(),
            ));
        }
        let state = self.registry.state.read().recover_poison();
        if !matches!(&state.origin, RegistryOrigin::Managed { .. }) {
            return Err(LLMError::Api(
                "Gold provider is no longer config-managed".into(),
            ));
        }
        // Verify the exact live provider/model and current factory generation
        // before comparing the factory inputs to the run's original proof.
        let current = ProviderRegistry::configured_model_in_snapshot(&state, config, target)?;
        let original = self.original_binding.as_ref().ok_or_else(stale_binding)?;
        // The original concrete handle keeps the existing same-generation
        // contract, including providers whose credentials may be dynamic.
        if Arc::ptr_eq(&current, expected) {
            return Ok(());
        }
        // A newly built handle needs a stronger proof. In particular, an
        // unchanged template or Copilot configuration does not establish an
        // unchanged credential at request time.
        if !gold_factory_identity_is_static(config, &self.provider_id) {
            return Err(LLMError::Api(
                "Gold provider identity cannot be proven stable across reload".into(),
            ));
        }
        let current =
            configured_binding(config, &self.provider_id, &self.key)?.ok_or_else(stale_binding)?;
        if !bool::from(original.as_slice().ct_eq(current.0.as_slice())) {
            return Err(stale_binding());
        }
        Ok(())
    }
}

// This is intentionally narrower than provider support: it controls only the
// cross-generation reuse of an already selected Host Gold provider. The
// original delegate keeps its already-built HTTP client and proxy matcher.
fn gold_factory_identity_is_static(config: &Config, id: &str) -> bool {
    fn static_extra(extra: &std::collections::BTreeMap<String, serde_json::Value>) -> bool {
        !extra
            .get(bamboo_config::PROVIDER_INSTANCE_API_KEY_FROM_ENV_CONFIG_KEY)
            .is_some_and(|value| value.as_bool() != Some(false))
            && !extra.contains_key("request_overrides")
    }
    if let Some(instance) = config.provider_instances.get(id) {
        return matches!(
            instance.provider_type.as_str(),
            "openai" | "anthropic" | "gemini" | "bodhi"
        ) && instance.request_overrides.is_none()
            && !bamboo_config::provider_instance_api_key_from_env(instance)
            && static_extra(&instance.extra);
    }
    if config.has_provider_instances() && config.effective_default_provider() != id {
        return false;
    }
    let providers = config.providers();
    match id {
        "openai" => providers.openai.as_ref().is_some_and(|entry| {
            !entry.api_key_from_env
                && entry.request_overrides.is_none()
                && static_extra(&entry.extra)
        }),
        "anthropic" => providers.anthropic.as_ref().is_some_and(|entry| {
            !entry.api_key_from_env
                && entry.request_overrides.is_none()
                && static_extra(&entry.extra)
        }),
        "gemini" => providers.gemini.as_ref().is_some_and(|entry| {
            !entry.api_key_from_env
                && entry.request_overrides.is_none()
                && static_extra(&entry.extra)
        }),
        "bodhi" => providers
            .bodhi
            .as_ref()
            .is_some_and(|entry| static_extra(&entry.extra)),
        _ => false,
    }
}

impl ProviderRegistry {
    fn managed_origin_for_snapshot(
        self: &Arc<Self>,
        state: &RegistryState,
        name: &str,
        selected: &Arc<dyn LLMProvider>,
    ) -> Option<Arc<ManagedProviderOrigin>> {
        let RegistryOrigin::Managed { key, bindings } = &state.origin else {
            return None;
        };
        Some(Arc::new(ManagedProviderOrigin {
            registry: self.clone(),
            provider_id: name.to_owned(),
            key: key.clone(),
            // Missing binding is still managed and fails closed on use.
            original_binding: bindings.get(name).map(|binding| binding.0),
            selected: selected.clone(),
        }))
    }

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
            state: RwLock::new(RegistryState {
                providers,
                metadata,
                default_provider,
                runtime_models: None,
                origin: RegistryOrigin::Unmanaged,
            }),
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
        bamboo_config::validate_runtime_model_admission(config).map_err(LLMError::Api)?;
        let (providers, metadata, default_provider) =
            Self::build_registry_state(config, app_data_dir).await?;
        let key =
            ring::hmac::Key::generate(ring::hmac::HMAC_SHA256, &ring::rand::SystemRandom::new())
                .map_err(|_| {
                    LLMError::Api("Could not initialize provider identity binding".into())
                })?;
        let mut models = HashMap::new();
        let mut bindings = HashMap::new();
        for provider in providers.keys() {
            models.insert(
                provider.clone(),
                bamboo_config::provider_runtime_models(config, provider).map_err(LLMError::Api)?,
            );
            let binding = configured_binding(config, provider, &key)?.ok_or_else(|| {
                LLMError::Api("Constructed provider lacks its configuration binding".into())
            })?;
            bindings.insert(provider.clone(), binding);
        }
        Ok(Self {
            state: RwLock::new(RegistryState {
                providers,
                metadata,
                default_provider,
                runtime_models: Some(models),
                origin: RegistryOrigin::Managed { key, bindings },
            }),
        })
    }

    /// Rebuild the registry in-place from config so existing holders of the outer
    /// `Arc<ProviderRegistry>` automatically observe refreshed providers.
    pub async fn reload_from_config(
        &self,
        config: &Config,
        app_data_dir: PathBuf,
    ) -> Result<(), LLMError> {
        self.replace_with(Self::from_config(config, app_data_dir).await?);
        Ok(())
    }

    /// Replace this registry with an already constructed candidate.
    ///
    /// Live reload callers use this only after verifying that the candidate's
    /// default provider initialized successfully. Keeping construction separate
    /// from publication means a bad edit cannot tear down the working registry.
    pub fn replace_with(&self, candidate: Self) {
        // The handle, identity, admission, metadata and origin are one
        // publication. In particular an empty managed generation stays managed.
        let candidate = candidate.state.into_inner().recover_poison();
        let old = {
            let mut state = self.state.write().recover_poison();
            std::mem::replace(&mut *state, candidate)
        };
        // Provider destructors run after publication and outside the lock.
        drop(old);
    }

    /// Explicit admission for hosts that construct providers independently.
    /// Once installed, entries absent from this map admit no models.
    pub fn set_runtime_models(&self, models: HashMap<String, Vec<String>>) {
        let models = models
            .into_iter()
            .map(|(provider, ids)| {
                let ids = ids
                    .into_iter()
                    .map(|id| id.trim().to_string())
                    .filter(|id| !id.is_empty())
                    .collect::<std::collections::BTreeSet<_>>();
                (provider, ids.into_iter().collect())
            })
            .collect();
        self.state.write().recover_poison().runtime_models = Some(models);
    }

    /// Admitted IDs; None is an unmanaged embedded registry, never discovery.
    pub fn runtime_models_for_provider(&self, provider: &str) -> Option<Vec<String>> {
        self.state
            .read()
            .recover_poison()
            .runtime_models
            .as_ref()
            .map(|models| models.get(provider).cloned().unwrap_or_default())
    }

    pub fn validate_model(&self, target: &bamboo_domain::ProviderModelRef) -> Result<(), LLMError> {
        let state = self.state.read().recover_poison();
        Self::validate_model_in_snapshot(state.runtime_models.as_ref(), target)
    }

    fn validate_model_in_snapshot(
        models: Option<&HashMap<String, Vec<String>>>,
        target: &bamboo_domain::ProviderModelRef,
    ) -> Result<(), LLMError> {
        if let Some(models) = models {
            if !models
                .get(&target.provider)
                .is_some_and(|models| models.iter().any(|model| model == target.model.trim()))
            {
                return Err(LLMError::Api(format!(
                    "Model '{}' is not admitted to runtime for provider '{}'; add it to runtime_models in provider settings",
                    target.model, target.provider
                )));
            }
        }
        Ok(())
    }

    /// Route a provider and validate its model in one coherent admission
    /// generation. Do not compose `get` + `validate_model` across reloads.
    pub fn provider_for_model(
        &self,
        target: &bamboo_domain::ProviderModelRef,
    ) -> Result<Arc<dyn LLMProvider>, LLMError> {
        let state = self.state.read().recover_poison();
        Self::provider_for_model_in_snapshot(&state, target)
    }

    /// Select the existing model route and its origin from the same generation.
    #[doc(hidden)]
    pub fn provider_for_model_with_origin(
        self: &Arc<Self>,
        target: &bamboo_domain::ProviderModelRef,
    ) -> Result<(Arc<dyn LLMProvider>, Option<Arc<ManagedProviderOrigin>>), LLMError> {
        let state = self.state.read().recover_poison();
        let provider = Self::provider_for_model_in_snapshot(&state, target)?;
        let origin = self.managed_origin_for_snapshot(&state, &target.provider, &provider);
        Ok((provider, origin))
    }

    fn provider_for_model_in_snapshot(
        state: &RegistryState,
        target: &bamboo_domain::ProviderModelRef,
    ) -> Result<Arc<dyn LLMProvider>, LLMError> {
        let provider = state
            .providers
            .get(&target.provider)
            .cloned()
            .ok_or_else(|| {
                LLMError::Auth(format!(
                    "Provider '{}' not available. Available: {}",
                    target.provider,
                    state
                        .providers
                        .keys()
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
            })?;
        Self::validate_model_in_snapshot(state.runtime_models.as_ref(), target)?;
        Ok(provider)
    }

    /// Select the handle and model admission from one registry generation and,
    /// for a config-managed registry, also verify that the caller's current
    /// configuration still describes that same account and admits the model.
    /// A registry created by `new` retains the embedder's routing semantics.
    /// The borrowed configuration must be the authoritative request snapshot.
    pub fn provider_for_configured_model(
        &self,
        config: &Config,
        target: &bamboo_domain::ProviderModelRef,
    ) -> Result<Arc<dyn LLMProvider>, LLMError> {
        let state = self.state.read().recover_poison();
        Self::configured_model_in_snapshot(&state, config, target)
    }

    fn configured_model_in_snapshot(
        state: &RegistryState,
        config: &Config,
        target: &bamboo_domain::ProviderModelRef,
    ) -> Result<Arc<dyn LLMProvider>, LLMError> {
        if let RegistryOrigin::Managed { key, bindings } = &state.origin {
            if config
                .provider_instances
                .get(&target.provider)
                .is_some_and(|instance| !instance.enabled)
            {
                return Err(LLMError::Auth(format!(
                    "Provider instance '{}' is disabled",
                    target.provider
                )));
            }
            let current = configured_binding(config, &target.provider, key)?.ok_or_else(|| {
                LLMError::Auth(format!(
                    "Provider '{}' has no available current configuration",
                    target.provider
                ))
            })?;
            let provider = Self::provider_for_model_in_snapshot(state, target)?;
            let saved = bindings.get(&target.provider).ok_or_else(stale_binding)?;
            if !bool::from(saved.0.as_slice().ct_eq(current.0.as_slice())) {
                return Err(stale_binding());
            }
            let admitted = bamboo_config::provider_runtime_models(config, &target.provider)
                .map_err(LLMError::Api)?;
            if !admitted.iter().any(|model| model == target.model.trim()) {
                return Err(LLMError::Api(
                    "Model is no longer admitted in the current provider configuration".into(),
                ));
            }
            return Ok(provider);
        }
        Self::provider_for_model_in_snapshot(state, target)
    }

    /// Return a coherent configured handle and whether its registry generation
    /// is Host-managed under the same lock. The keyed binding does not retain
    /// or expose plaintext credentials.
    #[doc(hidden)]
    pub fn provider_for_configured_model_with_origin(
        &self,
        config: &Config,
        target: &bamboo_domain::ProviderModelRef,
    ) -> Result<(Arc<dyn LLMProvider>, bool), LLMError> {
        let state = self.state.read().recover_poison();
        let managed = matches!(&state.origin, RegistryOrigin::Managed { .. });
        let provider = Self::configured_model_in_snapshot(&state, config, target)?;
        Ok((provider, managed))
    }

    /// The legacy direct-provider seam is only available to an explicitly
    /// embedder-managed empty registry. A managed registry remains managed
    /// after its last provider has been removed.
    pub fn is_unmanaged_and_empty(&self) -> bool {
        let state = self.state.read().recover_poison();
        matches!(&state.origin, RegistryOrigin::Unmanaged) && state.providers.is_empty()
    }

    /// The catalog must enrich exactly the provider generation that admitted
    /// these IDs, including during account or model-list reloads.
    pub fn runtime_provider_snapshot(
        &self,
        provider: &str,
    ) -> (Option<Arc<dyn LLMProvider>>, Vec<String>) {
        let state = self.state.read().recover_poison();
        (
            state.providers.get(provider).cloned(),
            state
                .runtime_models
                .as_ref()
                .and_then(|models| models.get(provider))
                .cloned()
                .unwrap_or_default(),
        )
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
                    tracing::warn!(
                        provider = name,
                        error_kind = llm_error_kind(&e),
                        "Provider failed to initialize, skipping"
                    );
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
                tracing::info!(instance_hash = %log_identity(instance_id), "Provider instance disabled, skipping");
                continue;
            }

            match Self::create_instance_provider(config, instance, app_data_dir.clone()).await {
                Ok(provider) => {
                    tracing::info!(
                        instance_hash = %log_identity(instance_id),
                        provider_type_hash = %log_identity(&instance.provider_type),
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
                        instance_hash = %log_identity(instance_id),
                        provider_type_hash = %log_identity(&instance.provider_type),
                        error_kind = llm_error_kind(&e),
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
                            instance_hash = %log_identity(legacy_default_id),
                            provider_type_hash = %log_identity(&instance_cfg.provider_type),
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
                            instance_hash = %log_identity(legacy_default_id),
                            error_kind = llm_error_kind(&e),
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
        self.state
            .read()
            .recover_poison()
            .providers
            .get(name)
            .cloned()
    }

    pub fn get_metadata(&self, name: &str) -> Option<ProviderMetadata> {
        self.state
            .read()
            .recover_poison()
            .metadata
            .get(name)
            .cloned()
    }

    pub fn provider_metadata(&self) -> Vec<ProviderMetadata> {
        self.state
            .read()
            .recover_poison()
            .metadata
            .values()
            .cloned()
            .collect()
    }

    /// The same unvalidated `get` route and origin, from one generation.
    #[doc(hidden)]
    pub fn get_with_origin(
        self: &Arc<Self>,
        name: &str,
    ) -> Option<(Arc<dyn LLMProvider>, Option<Arc<ManagedProviderOrigin>>)> {
        let state = self.state.read().recover_poison();
        let provider = state.providers.get(name)?.clone();
        let origin = self.managed_origin_for_snapshot(&state, name, &provider);
        Some((provider, origin))
    }

    /// Get the default provider (the one configured as `config.provider`).
    pub fn get_default(&self) -> Option<Arc<dyn LLMProvider>> {
        let state = self.state.read().recover_poison();
        state.providers.get(&state.default_provider).cloned()
    }

    /// The default provider name.
    pub fn default_provider_name(&self) -> String {
        self.state.read().recover_poison().default_provider.clone()
    }

    /// All provider names that were successfully initialized.
    pub fn provider_names(&self) -> Vec<String> {
        self.state
            .read()
            .recover_poison()
            .providers
            .keys()
            .cloned()
            .collect()
    }

    /// Number of initialized providers.
    pub fn len(&self) -> usize {
        self.state.read().recover_poison().providers.len()
    }

    /// Whether any providers are registered.
    pub fn is_empty(&self) -> bool {
        self.state.read().recover_poison().providers.is_empty()
    }

    /// Insert or replace a provider at runtime (used by instance CRUD / tests).
    pub fn insert(&self, key: String, provider: Arc<dyn LLMProvider>) {
        let mut state = self.state.write().recover_poison();
        // An arbitrary inserted handle has no proven configuration binding.
        // Invalidate first: if dropping a replaced provider panics, poison
        // recovery must never pair its replacement with the old binding.
        if let RegistryOrigin::Managed { bindings, .. } = &mut state.origin {
            bindings.remove(&key);
        }
        let replaced = state.providers.insert(key.clone(), provider);
        state.metadata.insert(
            key.clone(),
            ProviderMetadata {
                id: key.clone(),
                provider_type: key.clone(),
                display_name: display_name_for_provider_type(&key),
            },
        );
        drop(state);
        drop(replaced);
    }

    /// Remove a provider by key at runtime (used by instance CRUD / tests).
    pub fn remove(&self, key: &str) -> Option<Arc<dyn LLMProvider>> {
        let mut state = self.state.write().recover_poison();
        state.metadata.remove(key);
        if let Some(models) = state.runtime_models.as_mut() {
            models.remove(key);
        }
        if let RegistryOrigin::Managed { bindings, .. } = &mut state.origin {
            bindings.remove(key);
        }
        state.providers.remove(key)
    }

    /// Update the default provider key.
    pub fn set_default(&self, key: String) {
        self.state.write().recover_poison().default_provider = key;
    }
}

fn stale_binding() -> LLMError {
    LLMError::Auth("Provider configuration changed; reload the provider registry".into())
}

// Streams serialization directly into the keyed hash. Neither this writer nor
// the caller constructs a byte buffer containing plaintext credentials.
struct BindingWriter<'a>(&'a mut ring::hmac::Context);

impl Write for BindingWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

// runtime_models is enforced independently against the live Config and the
// registry snapshot; it is not an input to the provider factory. Omitting it
// here permits one model's admission to change without changing the account.
struct FactoryExtra<'a>(&'a std::collections::BTreeMap<String, serde_json::Value>);

impl Serialize for FactoryExtra<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        for (name, value) in self.0 {
            if name != bamboo_config::RUNTIME_MODELS_CONFIG_KEY {
                map.serialize_entry(name, value)?;
            }
        }
        map.end()
    }
}

fn hash_factory_inputs<T: Serialize>(
    key: &ring::hmac::Key,
    config: &Config,
    id: &str,
    source: &str,
    inputs: &T,
) -> Result<Binding, LLMError> {
    let http_proxy = config.http_proxy.trim();
    let https_proxy = config.https_proxy.trim();
    let proxy = if !http_proxy.is_empty() {
        http_proxy
    } else {
        https_proxy
    };
    let proxy_auth = if proxy.is_empty() {
        None
    } else {
        config.proxy_auth.as_ref()
    };
    let mut context = ring::hmac::Context::with_key(key);
    serde_json::to_writer(
        BindingWriter(&mut context),
        &(
            1u8,
            id,
            source,
            inputs,
            proxy,
            proxy_auth,
            &config.keyword_masking,
        ),
    )
    .map_err(|_| LLMError::Api("Could not bind provider configuration".into()))?;
    let tag = context.sign();
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(tag.as_ref());
    Ok(Binding(bytes))
}

// Keep the fields below aligned with create_provider_from_instance and
// synthesize_legacy_instances. Source tags make an explicit instance and a
// legacy alias distinct even when they happen to share an ID and credentials.
// Ambient system-proxy state and Copilot's dynamic authentication are not
// represented by Config and are intentionally outside this binding.
fn configured_binding(
    config: &Config,
    id: &str,
    key: &ring::hmac::Key,
) -> Result<Option<Binding>, LLMError> {
    if let Some(instance) = config.provider_instances.get(id) {
        if !instance.enabled {
            return Ok(None);
        }
        return hash_factory_inputs(
            key,
            config,
            id,
            "instance",
            &(
                &instance.provider_type,
                &instance.api_key,
                &instance.credential_ref,
                &instance.base_url,
                &instance.reasoning_effort,
                &instance.responses_only_models,
                &instance.request_overrides,
                FactoryExtra(&instance.extra),
                (instance.provider_type == "copilot").then_some(config.headless_auth),
            ),
        )
        .map(Some);
    }

    let hybrid = config.has_provider_instances();
    if hybrid && config.effective_default_provider() != id {
        return Ok(None);
    }

    let providers = config.providers();
    match id {
        "openai" => match providers
            .openai
            .as_ref()
            .filter(|entry| !entry.api_key.is_empty())
        {
            Some(entry) => hash_factory_inputs(
                key,
                config,
                id,
                "legacy-openai",
                &(
                    &entry.api_key,
                    &entry.credential_ref,
                    &entry.base_url,
                    &entry.reasoning_effort,
                    &entry.responses_only_models,
                    &entry.request_overrides,
                    entry.api_key_from_env,
                    FactoryExtra(&entry.extra),
                ),
            )
            .map(Some),
            None => Ok(None),
        },
        "anthropic" => match providers
            .anthropic
            .as_ref()
            .filter(|entry| !entry.api_key.is_empty())
        {
            Some(entry) => hash_factory_inputs(
                key,
                config,
                id,
                "legacy-anthropic",
                &(
                    &entry.api_key,
                    &entry.credential_ref,
                    &entry.base_url,
                    &entry.reasoning_effort,
                    &entry.request_overrides,
                    &entry.max_tokens,
                    &entry.thinking_replay_always,
                    entry.api_key_from_env,
                    FactoryExtra(&entry.extra),
                ),
            )
            .map(Some),
            None => Ok(None),
        },
        "gemini" => match providers
            .gemini
            .as_ref()
            .filter(|entry| !entry.api_key.is_empty())
        {
            Some(entry) => hash_factory_inputs(
                key,
                config,
                id,
                "legacy-gemini",
                &(
                    &entry.api_key,
                    &entry.credential_ref,
                    &entry.base_url,
                    &entry.reasoning_effort,
                    &entry.request_overrides,
                    entry.api_key_from_env,
                    FactoryExtra(&entry.extra),
                ),
            )
            .map(Some),
            None => Ok(None),
        },
        "bodhi" => match providers
            .bodhi
            .as_ref()
            .filter(|entry| !entry.api_key.is_empty())
        {
            Some(entry) => hash_factory_inputs(
                key,
                config,
                id,
                "legacy-bodhi",
                &(
                    &entry.api_key,
                    &entry.credential_ref,
                    &entry.base_url,
                    &entry.target_provider,
                    &entry.reasoning_effort,
                    FactoryExtra(&entry.extra),
                ),
            )
            .map(Some),
            None => Ok(None),
        },
        "copilot" => match providers.copilot.as_ref() {
            Some(entry) if !hybrid || entry.enabled || config.provider == "copilot" => {
                hash_factory_inputs(
                    key,
                    config,
                    id,
                    "legacy-copilot",
                    &(
                        entry.headless_auth,
                        config.headless_auth,
                        &entry.reasoning_effort,
                        &entry.responses_only_models,
                        &entry.request_overrides,
                        FactoryExtra(&entry.extra),
                    ),
                )
                .map(Some)
            }
            None if !hybrid => {
                hash_factory_inputs(key, config, id, "implicit-copilot", &config.headless_auth)
                    .map(Some)
            }
            _ => Ok(None),
        },
        _ => Ok(None),
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

    fn managed_config() -> Config {
        let mut config = Config::default();
        config.default_provider_instance = Some("relay".into());
        config.provider_instances.insert(
            "relay".into(),
            serde_json::from_value(serde_json::json!({
                "provider_type": "openai",
                "api_key": "fixture-key-a",
                "base_url": "http://127.0.0.1:32101",
                "model": "chat",
                "runtime_models": ["chat", "fast"]
            }))
            .unwrap(),
        );
        config
    }

    async fn assert_changed_static_inputs_are_rejected(original: &Config, changed: &Config) {
        let temp = tempfile::tempdir().unwrap();
        let target = bamboo_domain::ProviderModelRef::new("relay", "chat");
        let old_registry = ProviderRegistry::from_config(original, temp.path().to_path_buf())
            .await
            .unwrap();
        assert!(old_registry
            .provider_for_configured_model(original, &target)
            .is_ok());
        assert!(old_registry
            .provider_for_configured_model(changed, &target)
            .is_err());

        // Each changed configuration is itself valid. Rebuilding the registry
        // accepts it, so the negative above specifically exercises stale binding.
        let rebuilt = ProviderRegistry::from_config(changed, temp.path().to_path_buf())
            .await
            .unwrap();
        assert!(rebuilt
            .provider_for_configured_model(changed, &target)
            .is_ok());
    }

    #[tokio::test]
    async fn managed_gold_origin_proof_survives_only_equivalent_factory_reload() {
        let temp = tempfile::tempdir().unwrap();
        let config = managed_config();
        let registry = Arc::new(
            ProviderRegistry::from_config(&config, temp.path().to_path_buf())
                .await
                .unwrap(),
        );
        let target = bamboo_domain::ProviderModelRef::new("relay", "fast");
        let (selected, origin) = registry.provider_for_model_with_origin(&target).unwrap();
        let origin = origin.expect("managed origin captured with the selected handle");
        assert!(origin
            .validate_gold_target(&config, &target, &selected)
            .is_ok());

        registry
            .reload_from_config(&config, temp.path().to_path_buf())
            .await
            .unwrap();
        let rebuilt = registry.get("relay").unwrap();
        assert!(
            !Arc::ptr_eq(&selected, &rebuilt),
            "a new factory handle was built"
        );
        assert!(
            origin
                .validate_gold_target(&config, &target, &selected)
                .is_ok(),
            "unchanged factory inputs retain the original run target"
        );
        assert!(
            origin
                .validate_gold_target(&config, &target, &rebuilt)
                .is_err(),
            "a proof cannot be substituted for a different selected handle"
        );
        assert!(
            origin
                .validate_gold_target(
                    &config,
                    &bamboo_domain::ProviderModelRef::new("another-id", "fast"),
                    &selected,
                )
                .is_err(),
            "a proof is tied to one exact provider id"
        );

        for change in [
            "credential",
            "endpoint",
            "model_revoked",
            "model_changed",
            "provider_missing",
        ] {
            let mut changed = config.clone();
            match change {
                "credential" => {
                    changed.provider_instances.get_mut("relay").unwrap().api_key =
                        "synthetic-key-b".into()
                }
                "endpoint" => {
                    changed
                        .provider_instances
                        .get_mut("relay")
                        .unwrap()
                        .base_url = Some("http://127.0.0.1:32102".into())
                }
                "model_revoked" => {
                    changed
                        .provider_instances
                        .get_mut("relay")
                        .unwrap()
                        .extra
                        .insert("runtime_models".into(), serde_json::json!(["chat"]));
                }
                "model_changed" => {
                    changed
                        .provider_instances
                        .get_mut("relay")
                        .unwrap()
                        .extra
                        .insert(
                            "runtime_models".into(),
                            serde_json::json!(["chat", "other-fast"]),
                        );
                }
                "provider_missing" => {
                    changed.provider_instances.remove("relay");
                }
                _ => unreachable!(),
            }
            if change != "provider_missing" {
                registry
                    .reload_from_config(&changed, temp.path().to_path_buf())
                    .await
                    .unwrap();
            }
            assert!(
                origin
                    .validate_gold_target(&changed, &target, &selected)
                    .is_err(),
                "the original run must not use {change} as an equivalent target"
            );
            registry
                .reload_from_config(&config, temp.path().to_path_buf())
                .await
                .unwrap();
        }

        // Arbitrary replacement invalidates the registry's managed binding;
        // it remains managed so a missing proof must not become an SDK bypass.
        registry.insert("relay".into(), selected.clone());
        let (unbound, missing_origin) = registry.get_with_origin("relay").unwrap();
        let missing_origin = missing_origin.expect("origin remains managed");
        assert!(Arc::ptr_eq(&unbound, &selected));
        assert!(missing_origin
            .validate_gold_target(&config, &target, &unbound)
            .is_err());
    }

    #[tokio::test]
    async fn dynamic_host_gold_identity_is_not_reused_across_generations() {
        use crate::provider::LLMStream;
        use bamboo_domain::{Message, ToolSchema};
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct SyntheticDynamicProvider {
            account: Arc<AtomicUsize>,
            calls: Arc<AtomicUsize>,
            observed: Arc<AtomicUsize>,
        }
        #[async_trait::async_trait]
        impl LLMProvider for SyntheticDynamicProvider {
            async fn chat_stream(
                &self,
                _: &[Message],
                _: &[ToolSchema],
                _: Option<u32>,
                _: &str,
            ) -> Result<LLMStream, LLMError> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.observed
                    .store(self.account.load(Ordering::SeqCst), Ordering::SeqCst);
                Ok(Box::pin(futures::stream::empty()))
            }
        }

        fn synthetic_managed(config: &Config, provider: Arc<dyn LLMProvider>) -> ProviderRegistry {
            let key = ring::hmac::Key::generate(
                ring::hmac::HMAC_SHA256,
                &ring::rand::SystemRandom::new(),
            )
            .unwrap();
            let binding = configured_binding(config, "relay", &key).unwrap().unwrap();
            ProviderRegistry {
                state: RwLock::new(RegistryState {
                    providers: HashMap::from([("relay".into(), provider)]),
                    metadata: HashMap::new(),
                    default_provider: "relay".into(),
                    runtime_models: Some(HashMap::from([(
                        "relay".into(),
                        vec!["chat".into(), "fast".into()],
                    )])),
                    origin: RegistryOrigin::Managed {
                        key,
                        bindings: HashMap::from([("relay".into(), binding)]),
                    },
                }),
            }
        }

        for scenario in [
            "copilot_auth",
            "env_header",
            "env_key_marker",
            "unknown_env_marker",
        ] {
            let mut config = managed_config();
            let instance = config.provider_instances.get_mut("relay").unwrap();
            match scenario {
                "copilot_auth" => instance.provider_type = "copilot".into(),
                "env_header" => {
                    instance.request_overrides = Some(serde_json::from_value(serde_json::json!({
                        "common": {"headers": {"Authorization": {"type":"env_ref", "name":"SYNTHETIC_KEY"}}}
                    })).unwrap());
                }
                "env_key_marker" => {
                    instance.extra.insert(
                        bamboo_config::PROVIDER_INSTANCE_API_KEY_FROM_ENV_CONFIG_KEY.into(),
                        serde_json::json!(true),
                    );
                }
                "unknown_env_marker" => {
                    instance.extra.insert(
                        bamboo_config::PROVIDER_INSTANCE_API_KEY_FROM_ENV_CONFIG_KEY.into(),
                        serde_json::json!("unknown"),
                    );
                }
                _ => unreachable!(),
            }
            let account = Arc::new(AtomicUsize::new(1));
            let old_calls = Arc::new(AtomicUsize::new(0));
            let old_observed = Arc::new(AtomicUsize::new(0));
            let old: Arc<dyn LLMProvider> = Arc::new(SyntheticDynamicProvider {
                account: account.clone(),
                calls: old_calls.clone(),
                observed: old_observed.clone(),
            });
            let registry = Arc::new(synthetic_managed(&config, old));
            let target = bamboo_domain::ProviderModelRef::new("relay", "fast");
            let (selected, origin) = registry.provider_for_model_with_origin(&target).unwrap();
            let origin = origin.expect("selected synthetic route is managed");
            let guard = crate::gold_admission::guarded_gold_target_with_origin(
                Arc::new(tokio::sync::RwLock::new(config.clone())),
                origin,
                target,
                selected.clone(),
                selected.clone(),
            );
            assert!(
                guard.known_pre_dispatch_error().is_none(),
                "same handle remains usable: {scenario}"
            );
            let mut baseline = guard.chat_stream(&[], &[], None, "fast").await.unwrap();
            assert!(futures::StreamExt::next(&mut baseline).await.is_none());
            assert_eq!(old_calls.load(Ordering::SeqCst), 1);
            assert_eq!(old_observed.load(Ordering::SeqCst), 1);

            account.store(2, Ordering::SeqCst);
            let new_calls = Arc::new(AtomicUsize::new(0));
            let replacement: Arc<dyn LLMProvider> = Arc::new(SyntheticDynamicProvider {
                account: account.clone(),
                calls: new_calls.clone(),
                observed: Arc::new(AtomicUsize::new(0)),
            });
            registry.replace_with(synthetic_managed(&config, replacement));
            assert!(
                guard.known_pre_dispatch_error().is_some(),
                "cross-generation dynamic identity must fail: {scenario}"
            );
            assert!(guard.chat_stream(&[], &[], None, "fast").await.is_err());
            assert_eq!(
                old_calls.load(Ordering::SeqCst),
                1,
                "old dynamic account must not be called again: {scenario}"
            );
            assert_eq!(
                new_calls.load(Ordering::SeqCst),
                0,
                "must not switch to replacement: {scenario}"
            );
        }
    }

    #[test]
    fn legacy_and_instance_env_key_markers_are_not_cross_generation_proofs() {
        let mut instance = managed_config();
        for kind in ["openai", "anthropic", "gemini", "bodhi"] {
            instance
                .provider_instances
                .get_mut("relay")
                .unwrap()
                .provider_type = kind.into();
            assert!(
                gold_factory_identity_is_static(&instance, "relay"),
                "static {kind}"
            );
        }
        instance
            .provider_instances
            .get_mut("relay")
            .unwrap()
            .provider_type = "unknown".into();
        assert!(!gold_factory_identity_is_static(&instance, "relay"));
        instance
            .provider_instances
            .get_mut("relay")
            .unwrap()
            .provider_type = "openai".into();
        instance
            .provider_instances
            .get_mut("relay")
            .unwrap()
            .extra
            .insert(
                bamboo_config::PROVIDER_INSTANCE_API_KEY_FROM_ENV_CONFIG_KEY.into(),
                serde_json::json!(true),
            );
        assert!(!gold_factory_identity_is_static(&instance, "relay"));

        let mut legacy = Config::default();
        let mut openai = test_openai_config();
        openai.api_key_from_env = true;
        legacy.providers_mut().openai = Some(openai);
        assert!(!gold_factory_identity_is_static(&legacy, "openai"));
        legacy
            .providers_mut()
            .openai
            .as_mut()
            .unwrap()
            .api_key_from_env = false;
        assert!(gold_factory_identity_is_static(&legacy, "openai"));
    }

    #[tokio::test]
    async fn queue_host_provenance_checks_the_exact_target_in_one_snapshot() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = managed_config();
        let other = config.provider_instances.get("relay").unwrap().clone();
        config.provider_instances.insert("other".into(), other);
        let registry = ProviderRegistry::from_config(&config, temp.path().to_path_buf())
            .await
            .unwrap();
        let target = bamboo_domain::ProviderModelRef::new("relay", "fast");
        let (selected, managed) = registry
            .provider_for_configured_model_with_origin(&config, &target)
            .unwrap();
        assert!(managed);
        let sdk = ProviderRegistry::new(
            HashMap::from([("relay".into(), selected.clone())]),
            "relay".into(),
        );
        let (sdk_selected, sdk_managed) = sdk
            .provider_for_configured_model_with_origin(&config, &target)
            .unwrap();
        assert!(!sdk_managed);
        assert!(Arc::ptr_eq(&selected, &sdk_selected));
        {
            let mut state = registry.state.write().recover_poison();
            state.providers.remove("relay");
            // Deliberately reuse A under a different still-bound ID: scanning
            // all registry handles would incorrectly accept this selection.
            state.providers.insert("other".into(), selected.clone());
        }
        assert!(matches!(
            registry.provider_for_configured_model_with_origin(&config, &target),
            Err(LLMError::Auth(_))
        ));
        // A previous managed selection remains tagged even if the registry is
        // subsequently replaced by an SDK registry containing the same Arc.
        registry.replace_with(sdk);
        let (_, now_managed) = registry
            .provider_for_configured_model_with_origin(&config, &target)
            .unwrap();
        assert!(managed && !now_managed);
    }

    #[tokio::test]
    async fn managed_route_reports_disabled_missing_and_unadmitted_targets() {
        let temp = tempfile::tempdir().unwrap();
        let mut original = managed_config();
        // Retain instance mode even after deleting the selected relay; this
        // avoids constructing the unrelated implicit Copilot fallback.
        original.provider_instances.insert(
            "unused-disabled".into(),
            serde_json::from_value(serde_json::json!({"provider_type":"openai", "enabled":false}))
                .unwrap(),
        );
        let old_registry = ProviderRegistry::from_config(&original, temp.path().to_path_buf())
            .await
            .unwrap();
        let target = bamboo_domain::ProviderModelRef::new("relay", "fast");
        assert!(old_registry
            .provider_for_configured_model(&original, &target)
            .is_ok());
        for change in ["disabled", "missing", "unadmitted"] {
            let mut changed = original.clone();
            match change {
                "disabled" => changed.provider_instances.get_mut("relay").unwrap().enabled = false,
                "missing" => {
                    changed.provider_instances.remove("relay");
                }
                "unadmitted" => {
                    changed
                        .provider_instances
                        .get_mut("relay")
                        .unwrap()
                        .extra
                        .insert("runtime_models".into(), serde_json::json!(["chat"]));
                }
                _ => unreachable!(),
            }
            let expected = match change {
                "disabled" => "is disabled",
                "missing" => "has no available current configuration",
                "unadmitted" => "admitted",
                _ => unreachable!(),
            };
            let stale = old_registry
                .provider_for_configured_model(&changed, &target)
                .err()
                .unwrap()
                .to_string();
            assert!(stale.contains(expected), "{change}: {stale}");
            let rebuilt = ProviderRegistry::from_config(&changed, temp.path().to_path_buf())
                .await
                .unwrap();
            let current = rebuilt
                .provider_for_configured_model(&changed, &target)
                .err()
                .unwrap()
                .to_string();
            assert!(current.contains(expected), "rebuilt {change}: {current}");
        }
    }

    #[tokio::test]
    async fn configured_route_rejects_changed_credential_reference() {
        let mut original = managed_config();
        original
            .provider_instances
            .get_mut("relay")
            .unwrap()
            .credential_ref =
            Some(bamboo_config::CredentialRef::parse("fixture-credential-a").unwrap());
        let mut changed = original.clone();
        changed
            .provider_instances
            .get_mut("relay")
            .unwrap()
            .credential_ref =
            Some(bamboo_config::CredentialRef::parse("fixture-credential-b").unwrap());
        // The fixture key and endpoint stay identical; only the reference changes.
        assert_changed_static_inputs_are_rejected(&original, &changed).await;
    }

    #[tokio::test]
    async fn configured_route_rejects_changed_static_authorization_override() {
        let mut original = managed_config();
        original
            .provider_instances
            .get_mut("relay")
            .unwrap()
            .request_overrides = Some(
            serde_json::from_value(serde_json::json!({
                "common": {"headers": {"Authorization": "Bearer fixture-token-a"}}
            }))
            .unwrap(),
        );
        let mut changed = original.clone();
        changed
            .provider_instances
            .get_mut("relay")
            .unwrap()
            .request_overrides
            .as_mut()
            .unwrap()
            .common
            .headers
            .insert(
                "Authorization".into(),
                serde_json::from_value(serde_json::json!("Bearer fixture-token-b")).unwrap(),
            );
        assert_changed_static_inputs_are_rejected(&original, &changed).await;
    }

    #[tokio::test]
    async fn configured_route_rejects_changed_configured_proxy_auth() {
        let mut original = managed_config();
        original.http_proxy = "http://127.0.0.1:32103".into();
        original.proxy_auth = Some(bamboo_config::ProxyAuth {
            username: "fixture-proxy-user-a".into(),
            password: "fixture-proxy-password-a".into(),
        });

        let mut changed = original.clone();
        changed.proxy_auth.as_mut().unwrap().username = "fixture-proxy-user-b".into();
        assert_changed_static_inputs_are_rejected(&original, &changed).await;

        changed = original.clone();
        changed.proxy_auth.as_mut().unwrap().password = "fixture-proxy-password-b".into();
        assert_changed_static_inputs_are_rejected(&original, &changed).await;
    }

    #[tokio::test]
    async fn configured_route_rejects_deleted_disabled_or_changed_factory_input() {
        let temp = tempfile::tempdir().unwrap();
        let config = managed_config();
        let registry = ProviderRegistry::from_config(&config, temp.path().to_path_buf())
            .await
            .unwrap();
        let target = bamboo_domain::ProviderModelRef::new("relay", "chat");
        assert!(registry
            .provider_for_configured_model(&config, &target)
            .is_ok());

        let mut changed = config.clone();
        changed.provider_instances.remove("relay");
        assert!(registry
            .provider_for_configured_model(&changed, &target)
            .is_err());
        changed = config.clone();
        changed.provider_instances.get_mut("relay").unwrap().enabled = false;
        assert!(registry
            .provider_for_configured_model(&changed, &target)
            .is_err());
        changed = config.clone();
        changed.provider_instances.get_mut("relay").unwrap().api_key = "fixture-key-b".into();
        assert!(registry
            .provider_for_configured_model(&changed, &target)
            .is_err());
        changed = config.clone();
        changed
            .provider_instances
            .get_mut("relay")
            .unwrap()
            .base_url = Some("http://127.0.0.1:32102".into());
        assert!(registry
            .provider_for_configured_model(&changed, &target)
            .is_err());
        changed = config.clone();
        changed.http_proxy = "http://127.0.0.1:32103".into();
        assert!(registry
            .provider_for_configured_model(&changed, &target)
            .is_err());

        // An unrelated instance does not invalidate an otherwise unchanged
        // account; account and model checks are scoped to the selected target.
        changed = config.clone();
        changed.provider_instances.insert(
            "unrelated".into(),
            serde_json::from_value(
                serde_json::json!({"provider_type":"openai", "api_key":"fixture-other"}),
            )
            .unwrap(),
        );
        assert!(registry
            .provider_for_configured_model(&changed, &target)
            .is_ok());
        changed.headless_auth = !changed.headless_auth;
        assert!(registry
            .provider_for_configured_model(&changed, &target)
            .is_ok());
    }

    #[tokio::test]
    async fn managed_model_revocation_uses_live_admission_even_with_old_registry() {
        let temp = tempfile::tempdir().unwrap();
        let config = managed_config();
        let registry = ProviderRegistry::from_config(&config, temp.path().to_path_buf())
            .await
            .unwrap();
        let fast = bamboo_domain::ProviderModelRef::new("relay", "fast");
        let chat = bamboo_domain::ProviderModelRef::new("relay", "chat");
        assert!(registry
            .provider_for_configured_model(&config, &fast)
            .is_ok());

        let mut changed = config.clone();
        changed
            .provider_instances
            .get_mut("relay")
            .unwrap()
            .extra
            .insert("runtime_models".into(), serde_json::json!(["chat"]));
        assert!(registry
            .provider_for_configured_model(&changed, &fast)
            .is_err());
        assert!(registry
            .provider_for_configured_model(&changed, &chat)
            .is_ok());

        changed
            .provider_instances
            .get_mut("relay")
            .unwrap()
            .extra
            .insert("runtime_models".into(), serde_json::json!(null));
        assert!(registry
            .provider_for_configured_model(&changed, &chat)
            .is_err());
    }

    #[tokio::test]
    async fn derived_admission_tracks_removed_default_role_reference() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = managed_config();
        config
            .provider_instances
            .get_mut("relay")
            .unwrap()
            .extra
            .remove("runtime_models");
        config.defaults = Some(
            serde_json::from_value(serde_json::json!({
                "chat": {"provider":"relay", "model":"chat"},
                "fast": {"provider":"relay", "model":"derived-fast"}
            }))
            .unwrap(),
        );
        let registry = ProviderRegistry::from_config(&config, temp.path().to_path_buf())
            .await
            .unwrap();
        let target = bamboo_domain::ProviderModelRef::new("relay", "derived-fast");
        assert!(registry
            .provider_for_configured_model(&config, &target)
            .is_ok());
        config.defaults.as_mut().unwrap().fast = None;
        assert!(registry
            .provider_for_configured_model(&config, &target)
            .is_err());
        assert!(registry
            .provider_for_configured_model(
                &config,
                &bamboo_domain::ProviderModelRef::new("relay", "chat")
            )
            .is_ok());
    }

    #[tokio::test]
    async fn unmanaged_registry_retains_embedder_admission_semantics() {
        let temp = tempfile::tempdir().unwrap();
        let config = managed_config();
        let managed = ProviderRegistry::from_config(&config, temp.path().to_path_buf())
            .await
            .unwrap();
        let handle = managed.get("relay").unwrap();
        let external =
            ProviderRegistry::new(HashMap::from([("custom".into(), handle)]), "custom".into());
        let target = bamboo_domain::ProviderModelRef::new("custom", "any");
        assert!(external
            .provider_for_configured_model(&Config::default(), &target)
            .is_ok());
        external.set_runtime_models(HashMap::from([("custom".into(), vec!["only".into()])]));
        assert!(external
            .provider_for_configured_model(&Config::default(), &target)
            .is_err());
        assert!(external
            .provider_for_configured_model(
                &Config::default(),
                &bamboo_domain::ProviderModelRef::new("custom", "only")
            )
            .is_ok());
    }

    #[tokio::test]
    async fn managed_legacy_and_instance_with_same_id_never_share_a_binding() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = managed_config();
        let instance = config.provider_instances.remove("relay").unwrap();
        config
            .provider_instances
            .insert("openai".into(), instance.clone());
        config.provider_instances.insert("other".into(), instance);
        config.default_provider_instance = Some("openai".into());
        config.providers_mut().openai = Some(OpenAIConfig {
            api_key: "fixture-key-a".into(),
            base_url: Some("http://127.0.0.1:32101".into()),
            model: Some("chat".into()),
            ..OpenAIConfig::default()
        });
        let registry = ProviderRegistry::from_config(&config, temp.path().to_path_buf())
            .await
            .unwrap();
        let target = bamboo_domain::ProviderModelRef::new("openai", "chat");
        assert!(registry
            .provider_for_configured_model(&config, &target)
            .is_ok());
        config.provider_instances.remove("openai");
        assert!(registry
            .provider_for_configured_model(&config, &target)
            .is_err());
        registry
            .reload_from_config(&config, temp.path().to_path_buf())
            .await
            .unwrap();
        assert!(registry
            .provider_for_configured_model(&config, &target)
            .is_ok());
    }

    #[tokio::test]
    async fn managed_empty_and_insert_are_not_unmanaged_direct_seams() {
        let temp = tempfile::tempdir().unwrap();
        let config = managed_config();
        let registry = ProviderRegistry::from_config(&config, temp.path().to_path_buf())
            .await
            .unwrap();
        let target = bamboo_domain::ProviderModelRef::new("relay", "chat");
        let handle = registry.get("relay").unwrap();
        registry.insert("relay".into(), handle);
        assert!(registry
            .provider_for_configured_model(&config, &target)
            .is_err());
        registry.remove("relay");
        assert!(registry.is_empty());
        assert!(!registry.is_unmanaged_and_empty());

        let external = ProviderRegistry::new(HashMap::new(), "direct".into());
        assert!(external.is_unmanaged_and_empty());
        registry.replace_with(external);
        assert!(registry.is_unmanaged_and_empty());
        registry
            .reload_from_config(&config, temp.path().to_path_buf())
            .await
            .unwrap();
        assert!(!registry.is_unmanaged_and_empty());
        assert!(registry
            .provider_for_configured_model(&config, &target)
            .is_ok());
    }

    #[tokio::test]
    async fn configured_route_never_mixes_concurrent_managed_generations() {
        struct AccountProvider(&'static str);
        #[async_trait::async_trait]
        impl LLMProvider for AccountProvider {
            async fn chat_stream(
                &self,
                _messages: &[bamboo_domain::Message],
                _tools: &[bamboo_domain::ToolSchema],
                _max_output_tokens: Option<u32>,
                _model: &str,
            ) -> crate::provider::Result<crate::provider::LLMStream> {
                unreachable!("routing test does not execute requests")
            }
            async fn list_models(&self) -> crate::provider::Result<Vec<String>> {
                Ok(vec![self.0.into()])
            }
        }
        fn candidate(config: &Config, label: &'static str) -> ProviderRegistry {
            let key = ring::hmac::Key::generate(
                ring::hmac::HMAC_SHA256,
                &ring::rand::SystemRandom::new(),
            )
            .unwrap();
            let binding = configured_binding(config, "relay", &key).unwrap().unwrap();
            ProviderRegistry {
                state: RwLock::new(RegistryState {
                    providers: HashMap::from([(
                        "relay".into(),
                        Arc::new(AccountProvider(label)) as Arc<dyn LLMProvider>,
                    )]),
                    metadata: HashMap::new(),
                    default_provider: "relay".into(),
                    runtime_models: Some(HashMap::from([("relay".into(), vec!["chat".into()])])),
                    origin: RegistryOrigin::Managed {
                        key,
                        bindings: HashMap::from([("relay".into(), binding)]),
                    },
                }),
            }
        }
        let a = Arc::new(managed_config());
        let mut config_b = managed_config();
        config_b
            .provider_instances
            .get_mut("relay")
            .unwrap()
            .api_key = "fixture-key-b".into();
        let b = Arc::new(config_b);
        let registry = Arc::new(candidate(&a, "a"));
        let writer_registry = registry.clone();
        let writer_a = a.clone();
        let writer_b = b.clone();
        let writer = std::thread::spawn(move || {
            for n in 0..500 {
                let (config, label) = if n % 2 == 0 {
                    (&writer_a, "a")
                } else {
                    (&writer_b, "b")
                };
                writer_registry.replace_with(candidate(config, label));
            }
        });
        let target = bamboo_domain::ProviderModelRef::new("relay", "chat");
        let mut successful = 0;
        for _ in 0..500 {
            for (config, label) in [(&a, "a"), (&b, "b")] {
                if let Ok(provider) = registry.provider_for_configured_model(config, &target) {
                    assert_eq!(provider.list_models().await.unwrap(), [label]);
                    successful += 1;
                }
            }
        }
        writer.join().unwrap();
        assert!(successful > 0);
    }

    #[test]
    fn panicking_old_provider_drop_does_not_keep_old_binding_for_inserted_handle() {
        struct ProviderWithDrop(bool);
        impl Drop for ProviderWithDrop {
            fn drop(&mut self) {
                if self.0 {
                    panic!("synthetic provider drop panic");
                }
            }
        }
        #[async_trait::async_trait]
        impl LLMProvider for ProviderWithDrop {
            async fn chat_stream(
                &self,
                _messages: &[bamboo_domain::Message],
                _tools: &[bamboo_domain::ToolSchema],
                _max_output_tokens: Option<u32>,
                _model: &str,
            ) -> crate::provider::Result<crate::provider::LLMStream> {
                unreachable!("routing test does not execute requests")
            }
        }
        let config = managed_config();
        let key =
            ring::hmac::Key::generate(ring::hmac::HMAC_SHA256, &ring::rand::SystemRandom::new())
                .unwrap();
        let binding = configured_binding(&config, "relay", &key).unwrap().unwrap();
        let registry = ProviderRegistry {
            state: RwLock::new(RegistryState {
                providers: HashMap::from([(
                    "relay".into(),
                    Arc::new(ProviderWithDrop(true)) as Arc<dyn LLMProvider>,
                )]),
                metadata: HashMap::new(),
                default_provider: "relay".into(),
                runtime_models: Some(HashMap::from([("relay".into(), vec!["chat".into()])])),
                origin: RegistryOrigin::Managed {
                    key,
                    bindings: HashMap::from([("relay".into(), binding)]),
                },
            }),
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            registry.insert("relay".into(), Arc::new(ProviderWithDrop(false)));
        }));
        assert!(result.is_err());
        assert!(registry
            .provider_for_configured_model(
                &config,
                &bamboo_domain::ProviderModelRef::new("relay", "chat")
            )
            .is_err());
    }

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
    async fn concurrent_reload_never_routes_a_model_through_another_accounts_provider() {
        struct AccountProvider(&'static str);
        #[async_trait::async_trait]
        impl LLMProvider for AccountProvider {
            async fn chat_stream(
                &self,
                _messages: &[bamboo_domain::Message],
                _tools: &[bamboo_domain::ToolSchema],
                _max_output_tokens: Option<u32>,
                _model: &str,
            ) -> crate::provider::Result<crate::provider::LLMStream> {
                unreachable!("routing test does not execute requests")
            }
            async fn list_models(&self) -> crate::provider::Result<Vec<String>> {
                Ok(vec![self.0.into()])
            }
        }
        let candidate = |model: &'static str| {
            let registry = ProviderRegistry::new(
                HashMap::from([(
                    "relay".into(),
                    Arc::new(AccountProvider(model)) as Arc<dyn LLMProvider>,
                )]),
                "relay".into(),
            );
            registry.set_runtime_models(HashMap::from([("relay".into(), vec![model.into()])]));
            registry
        };
        let registry = Arc::new(candidate("account-a-model"));
        let reloading = registry.clone();
        let writer = std::thread::spawn(move || {
            for n in 0..4000 {
                reloading.replace_with(candidate(if n % 2 == 0 {
                    "account-a-model"
                } else {
                    "account-b-model"
                }));
            }
        });
        let mut routed = 0;
        for _ in 0..4000 {
            for model in ["account-a-model", "account-b-model"] {
                if let Ok(provider) = registry
                    .provider_for_model(&bamboo_domain::ProviderModelRef::new("relay", model))
                {
                    assert_eq!(provider.list_models().await.unwrap(), [model]);
                    routed += 1;
                }
            }
        }
        writer.join().unwrap();
        assert!(routed > 0);
    }

    #[tokio::test]
    async fn config_admission_is_preserved_on_reload_and_failed_reload() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.default_provider_instance = Some("relay".into());
        config.provider_instances.insert(
            "relay".into(),
            serde_json::from_value(serde_json::json!({
                "provider_type":"openai", "api_key":"sk-fixture", "model":"chat",
                "runtime_models":["chat", "custom/id"]
            }))
            .unwrap(),
        );
        let registry = Arc::new(
            ProviderRegistry::from_config(&config, temp.path().to_path_buf())
                .await
                .unwrap(),
        );
        let router = crate::router::ProviderModelRouter::new(registry.clone());
        assert!(router
            .route(&bamboo_domain::ProviderModelRef::new("relay", "custom/id"))
            .is_ok());
        assert!(router
            .route(&bamboo_domain::ProviderModelRef::new(
                "relay",
                "upstream-only"
            ))
            .is_err());
        assert_eq!(
            registry.runtime_models_for_provider("relay").unwrap(),
            ["chat", "custom/id"]
        );
        assert!(registry
            .validate_model(&bamboo_domain::ProviderModelRef::new("relay", "custom/id"))
            .is_ok());
        assert!(registry
            .validate_model(&bamboo_domain::ProviderModelRef::new(
                "relay",
                "upstream-only"
            ))
            .is_err());
        assert!(registry
            .validate_model(&bamboo_domain::ProviderModelRef::new("other", "custom/id"))
            .is_err());

        config
            .provider_instances
            .get_mut("relay")
            .unwrap()
            .extra
            .insert("runtime_models".into(), serde_json::json!(["chat"]));
        registry
            .reload_from_config(&config, temp.path().to_path_buf())
            .await
            .unwrap();
        assert!(registry
            .validate_model(&bamboo_domain::ProviderModelRef::new("relay", "custom/id"))
            .is_err());

        config
            .provider_instances
            .get_mut("relay")
            .unwrap()
            .extra
            .insert("runtime_models".into(), serde_json::json!(null));
        assert!(registry
            .reload_from_config(&config, temp.path().to_path_buf())
            .await
            .is_err());
        assert_eq!(
            registry.runtime_models_for_provider("relay").unwrap(),
            ["chat"]
        );
        assert!(registry.get("relay").is_some());
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

        // Poison the registry state RwLock by panicking while holding a write guard.
        // The guard is dropped during unwinding, which is exactly what marks the
        // lock as poisoned.
        let poisoned = catch_unwind(AssertUnwindSafe(|| {
            let _guard = registry.state.write().unwrap();
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
            "metadata should remain usable after recovering the shared lock"
        );
    }
}
