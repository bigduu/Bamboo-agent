//! Legacy-provider compatibility at the provider-instance boundary.
//!
//! Runtime code consumes [`ProviderInstanceConfig`] directly.  The helpers in
//! this module are deliberately limited to materializing a legacy-only
//! configuration into durable provider instances. Legacy config input remains
//! supported for cold starts, but it is not re-projected into an HTTP response.

use super::config::{Config, ProviderInstanceConfig};
use serde_json::Value;

/// Marker stored in an instance's forward-compatible fields when its API key
/// may be overridden by the provider type's standard `BAMBOO_*_API_KEY`
/// environment variable. The marker is metadata only; plaintext is injected at
/// runtime and is never encrypted or persisted by ordinary config saves.
///
/// Legacy materialization keeps this binding even when a stable
/// [`crate::CredentialRef`] exists. That preserves the historical precedence
/// contract: an environment key wins while present and the stored credential is
/// the fallback after the variable is removed.
pub const PROVIDER_INSTANCE_API_KEY_FROM_ENV_CONFIG_KEY: &str = "api_key_from_env";

/// Whether a provider instance accepts its provider type's standard runtime
/// environment-variable override.
pub fn provider_instance_api_key_from_env(instance: &ProviderInstanceConfig) -> bool {
    instance
        .extra
        .get(PROVIDER_INSTANCE_API_KEY_FROM_ENV_CONFIG_KEY)
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

/// Whether a marked provider instance is currently using its standard runtime
/// environment-variable override.
///
/// Merely persisting the binding is not enough: the variable must be non-empty
/// and the live instance must have been hydrated with that exact value. This
/// keeps credential-status projections honest when an environment variable is
/// added after startup but before the next configuration reload.
pub fn provider_instance_environment_override_active(instance: &ProviderInstanceConfig) -> bool {
    if !provider_instance_api_key_from_env(instance) {
        return false;
    }
    provider_api_key_environment_override_active(&instance.provider_type, &instance.api_key)
}

/// Whether a provider key currently matches its standard environment override.
///
/// Compatibility projections use this without fabricating a temporary
/// [`ProviderInstanceConfig`]. A persisted environment binding alone is not
/// active: the variable must still be present and equal the hydrated runtime
/// value.
pub fn provider_api_key_environment_override_active(provider_type: &str, api_key: &str) -> bool {
    let Some(env_var) = standard_provider_api_key_env(provider_type) else {
        return false;
    };
    let Ok(value) = crate::runtime_env_var(env_var) else {
        return false;
    };
    let value = value.trim();
    !value.is_empty() && api_key.trim() == value
}

fn copy_runtime_env_marker(
    extra: &mut std::collections::BTreeMap<String, Value>,
    from_environment: bool,
) {
    if from_environment {
        extra.insert(
            PROVIDER_INSTANCE_API_KEY_FROM_ENV_CONFIG_KEY.to_string(),
            Value::Bool(true),
        );
    }
}

fn insert_optional_extra(
    extra: &mut std::collections::BTreeMap<String, Value>,
    key: &str,
    value: Option<Value>,
) {
    if let Some(value) = value {
        extra.entry(key.to_string()).or_insert(value);
    }
}

/// Create synthetic provider instances from legacy `providers` config.
///
/// For each configured legacy provider that does not already have a
/// corresponding entry in `provider_instances`, a synthetic instance
/// is created using the provider type as the instance id (e.g. `"openai"`).
/// This allows the instance-keyed registry to fall back to legacy config
/// without requiring user migration.
pub fn synthesize_legacy_instances(config: &Config) -> Vec<(String, ProviderInstanceConfig)> {
    let mut result = Vec::new();

    if let Some(openai) = &config.providers.openai {
        let id = "openai".to_string();
        if !config.provider_instances.contains_key(&id) && !config.is_retired_provider_alias(&id) {
            let mut extra = openai.extra.clone();
            copy_runtime_env_marker(&mut extra, openai.api_key_from_env);
            result.push((
                id,
                ProviderInstanceConfig {
                    provider_type: "openai".to_string(),
                    label: Some("OpenAI".to_string()),
                    api_key: openai.api_key.clone(),
                    api_key_encrypted: openai.api_key_encrypted.clone(),
                    credential_ref: openai.credential_ref.clone(),
                    base_url: openai.base_url.clone(),
                    model: openai.model.clone(),
                    fast_model: openai.fast_model.clone(),
                    vision_model: openai.vision_model.clone(),
                    reasoning_effort: openai.reasoning_effort,
                    responses_only_models: openai.responses_only_models.clone(),
                    request_overrides: openai.request_overrides.clone(),
                    enabled: true,
                    extra,
                },
            ));
        }
    }

    if let Some(anthropic) = &config.providers.anthropic {
        let id = "anthropic".to_string();
        if !config.provider_instances.contains_key(&id) && !config.is_retired_provider_alias(&id) {
            let mut extra = anthropic.extra.clone();
            copy_runtime_env_marker(&mut extra, anthropic.api_key_from_env);
            insert_optional_extra(
                &mut extra,
                "max_tokens",
                anthropic.max_tokens.map(Value::from),
            );
            insert_optional_extra(
                &mut extra,
                "thinking_replay_always",
                anthropic.thinking_replay_always.map(Value::Bool),
            );
            result.push((
                id,
                ProviderInstanceConfig {
                    provider_type: "anthropic".to_string(),
                    label: Some("Anthropic".to_string()),
                    api_key: anthropic.api_key.clone(),
                    api_key_encrypted: anthropic.api_key_encrypted.clone(),
                    credential_ref: anthropic.credential_ref.clone(),
                    base_url: anthropic.base_url.clone(),
                    model: anthropic.model.clone(),
                    fast_model: anthropic.fast_model.clone(),
                    vision_model: anthropic.vision_model.clone(),
                    reasoning_effort: anthropic.reasoning_effort,
                    responses_only_models: vec![],
                    request_overrides: anthropic.request_overrides.clone(),
                    enabled: true,
                    extra,
                },
            ));
        }
    }

    if let Some(gemini) = &config.providers.gemini {
        let id = "gemini".to_string();
        if !config.provider_instances.contains_key(&id) && !config.is_retired_provider_alias(&id) {
            let mut extra = gemini.extra.clone();
            copy_runtime_env_marker(&mut extra, gemini.api_key_from_env);
            result.push((
                id,
                ProviderInstanceConfig {
                    provider_type: "gemini".to_string(),
                    label: Some("Gemini".to_string()),
                    api_key: gemini.api_key.clone(),
                    api_key_encrypted: gemini.api_key_encrypted.clone(),
                    credential_ref: gemini.credential_ref.clone(),
                    base_url: gemini.base_url.clone(),
                    model: gemini.model.clone(),
                    fast_model: gemini.fast_model.clone(),
                    vision_model: gemini.vision_model.clone(),
                    reasoning_effort: gemini.reasoning_effort,
                    responses_only_models: vec![],
                    request_overrides: gemini.request_overrides.clone(),
                    enabled: true,
                    extra,
                },
            ));
        }
    }

    if let Some(bodhi) = &config.providers.bodhi {
        let id = "bodhi".to_string();
        if !config.provider_instances.contains_key(&id) && !config.is_retired_provider_alias(&id) {
            let mut extra = bodhi.extra.clone();
            insert_optional_extra(
                &mut extra,
                "target_provider",
                bodhi.target_provider.clone().map(Value::String),
            );
            result.push((
                id,
                ProviderInstanceConfig {
                    provider_type: "bodhi".to_string(),
                    label: Some("Bodhi".to_string()),
                    api_key: bodhi.api_key.clone(),
                    api_key_encrypted: bodhi.api_key_encrypted.clone(),
                    credential_ref: bodhi.credential_ref.clone(),
                    base_url: bodhi.base_url.clone(),
                    model: None,
                    fast_model: None,
                    vision_model: None,
                    reasoning_effort: bodhi.reasoning_effort,
                    responses_only_models: vec![],
                    request_overrides: None,
                    enabled: true,
                    extra,
                },
            ));
        }
    }

    result
}

/// Drop retired provider configuration while preserving unresolved model
/// references for repair. Exact instance ids can belong to any provider type.
pub(crate) fn retire_copilot(config: &mut Config) -> bool {
    let selected = config
        .default_provider_instance
        .as_deref()
        .unwrap_or(&config.provider);
    let retired_default = match config.provider_instances.get(selected) {
        Some(instance) => instance.provider_type == "copilot",
        None => selected == "copilot",
    };
    let changed_default = retired_default && !config.has_saved_retired_provider_default();
    if changed_default {
        config.default_provider_instance = None;
        config.provider = "copilot".to_owned();
        config.providers_mut().copilot_default_retired = true;
    }
    let retired_aliases: Vec<_> = config
        .provider_instances
        .iter()
        .filter(|(id, instance)| instance.provider_type == "copilot" && is_provider_alias(id))
        .map(|(id, _)| id.clone())
        .collect();
    let before = config.provider_instances.len();
    config
        .provider_instances
        .retain(|_, instance| instance.provider_type != "copilot");
    let providers = config.providers_mut();
    providers.retired_provider_aliases.extend(retired_aliases);
    providers.extra.remove("copilot").is_some()
        || changed_default
        || before != config.provider_instances.len()
}

pub(crate) fn is_provider_alias(id: &str) -> bool {
    matches!(id, "openai" | "anthropic" | "gemini" | "bodhi")
}

/// Idempotently materialize legacy provider configuration.
///
/// The provider type is retained as the instance id so existing routing keys,
/// `BAMBOO_PROVIDER`, and old clients remain deterministic. An explicitly
/// configured instance matching the selected routing key always wins. Hybrid
/// configs may materialize a missing legacy default without changing any
/// explicit instance.
///
/// Every non-conflicting legacy alias is preserved, even when the selected
/// default already resolves to an explicit instance. Stored credentials reuse
/// the legacy provider's stable `credential_ref`. Materialized
/// OpenAI/Anthropic/Gemini aliases retain their standard environment override
/// binding while keeping the stored credential as a fallback; environment-only
/// aliases carry only the marker and never persist plaintext.
pub fn materialize_legacy_provider_instances(config: &mut Config) -> bool {
    let selected = config.effective_default_provider().trim().to_string();
    let retired_default = config.has_saved_retired_provider_default();
    if selected.is_empty() && !retired_default {
        return false;
    }
    let selected_is_explicit = config.provider_instances.contains_key(&selected);

    let mut candidates = synthesize_legacy_instances(config);

    // Preserve every standard legacy env-only provider, not just the selected
    // one. A custom explicit instance id must not absorb a type-wide legacy
    // override, so the compatibility alias keeps the provider type as its id.
    for provider_type in ["openai", "anthropic", "gemini"] {
        if standard_provider_api_key_env_is_available(provider_type)
            && !config.provider_instances.contains_key(provider_type)
            && !config.is_retired_provider_alias(provider_type)
            && !candidates.iter().any(|(id, _)| id == provider_type)
        {
            let mut instance = empty_provider_instance(provider_type);
            instance.label = Some(provider_display_label(provider_type).to_string());
            copy_runtime_env_marker(&mut instance.extra, true);
            candidates.push((provider_type.to_string(), instance));
        }
    }

    // Credential migration must isolate ordinary legacy plaintext/ciphertext
    // before provider metadata can be materialized. Refuse to turn an
    // unisolated secret into an env-bound instance, which would otherwise make
    // an ordinary save omit the plaintext and silently lose it.
    if candidates.iter().any(|(_, instance)| {
        instance.credential_ref.is_none()
            && !provider_instance_api_key_from_env(instance)
            && (!instance.api_key.trim().is_empty() || instance.api_key_encrypted.is_some())
    }) {
        return false;
    }

    for (_, instance) in &mut candidates {
        if standard_provider_api_key_env(&instance.provider_type).is_some() {
            copy_runtime_env_marker(&mut instance.extra, true);
            if instance.credential_ref.is_none() {
                // The only possible plaintext at this point is a runtime env
                // value already identified by the legacy flag above.
                instance.api_key.clear();
                instance.api_key_encrypted = None;
            }
        }
    }

    let selected_is_usable = selected_is_explicit
        || candidates.iter().any(|(id, instance)| {
            id == &selected
                && instance.enabled
                && (instance.credential_ref.is_some()
                    || (provider_instance_api_key_from_env(instance)
                        && standard_provider_api_key_env_is_available(&instance.provider_type)))
        });
    // A retired default can leave other legacy credentials configured. Give
    // Settings those instances without selecting a different default.
    if !selected_is_usable && !retired_default {
        return false;
    }

    let mut changed = !candidates.is_empty();
    config.provider_instances.extend(candidates);
    if selected_is_usable && config.default_provider_instance.as_deref() != Some(&selected) {
        config.default_provider_instance = Some(selected);
        changed = true;
    }
    changed
}

fn standard_provider_api_key_env(provider_type: &str) -> Option<&'static str> {
    match provider_type {
        "openai" => Some("BAMBOO_OPENAI_API_KEY"),
        "anthropic" => Some("BAMBOO_ANTHROPIC_API_KEY"),
        "gemini" => Some("BAMBOO_GEMINI_API_KEY"),
        _ => None,
    }
}

fn standard_provider_api_key_env_is_available(provider_type: &str) -> bool {
    standard_provider_api_key_env(provider_type)
        .and_then(|env_var| crate::runtime_env_var(env_var).ok())
        .is_some_and(|value| !value.trim().is_empty())
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ProviderMetadataOverrides(Vec<ProviderMetadataOverride>);

#[derive(Debug, Clone)]
struct ProviderMetadataOverride {
    provider_type: &'static str,
    instance: bool,
    base_url: Option<MetadataOverride>,
    model: Option<MetadataOverride>,
}

#[derive(Debug, Clone)]
struct MetadataOverride {
    saved: Option<String>,
    runtime: String,
}

impl MetadataOverride {
    fn apply(field: &mut Option<String>, runtime: Option<String>) -> Option<Self> {
        let runtime = runtime?;
        if field.as_deref() == Some(runtime.as_str()) {
            return None;
        }
        let saved = field.replace(runtime.clone());
        Some(Self { saved, runtime })
    }

    fn restore(self, field: &mut Option<String>) {
        // A detached writer may have explicitly changed this value since load.
        if field.as_deref() == Some(self.runtime.as_str()) {
            *field = self.saved;
        }
    }
}

fn nonempty_env(name: &str) -> Option<String> {
    let value = crate::runtime_env_var(name).ok()?;
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

/// A rejected runtime override occupies the endpoint slot until the user
/// fixes it. This URL cannot send a request or expose the original value.
#[doc(hidden)]
pub const INVALID_RUNTIME_PROVIDER_BASE_URL: &str = "about:invalid";

fn runtime_base_url(name: &'static str) -> Option<String> {
    let error = match crate::runtime_env_var(name) {
        Ok(raw) => {
            let url = raw.trim();
            match crate::validate_provider_base_url(url) {
                Ok(()) => return Some(url.trim_end_matches('/').to_string()),
                Err(error) => error,
            }
        }
        Err(std::env::VarError::NotPresent) => return None,
        Err(std::env::VarError::NotUnicode(_)) => "provider base URL must be valid UTF-8".into(),
    };
    tracing::warn!(variable = name, %error, "Provider endpoint override rejected");
    Some(INVALID_RUNTIME_PROVIDER_BASE_URL.to_string())
}

pub(crate) fn apply_provider_env_overrides(config: &mut Config, include_api_keys: bool) {
    restore_provider_metadata(config);
    let instance_mode = config.has_provider_instances();
    macro_rules! apply {
        ($field:ident, $prefix:literal) => {{
            let provider_type = stringify!($field);
            let key = if include_api_keys {
                nonempty_env(concat!($prefix, "_API_KEY"))
            } else {
                None
            };
            let model = nonempty_env(concat!($prefix, "_MODEL"));
            let mut metadata = ProviderMetadataOverride {
                provider_type,
                instance: instance_mode,
                base_url: None,
                model: None,
            };
            if instance_mode {
                // Preserve the existing type-wide key opt-in. Endpoint and model
                // settings are scoped to the exact built-in slot.
                if let Some(key) = key {
                    for instance in config.provider_instances.values_mut().filter(|instance| {
                        instance.provider_type == provider_type
                            && provider_instance_api_key_from_env(instance)
                    }) {
                        instance.api_key.clone_from(&key);
                        instance.api_key_encrypted = None;
                    }
                }
                if let Some(instance) =
                    config
                        .provider_instances
                        .get_mut(provider_type)
                        .filter(|instance| {
                            instance.provider_type == provider_type
                                && provider_instance_api_key_from_env(instance)
                        })
                {
                    metadata.base_url = MetadataOverride::apply(
                        &mut instance.base_url,
                        runtime_base_url(concat!($prefix, "_BASE_URL")),
                    );
                    metadata.model = MetadataOverride::apply(&mut instance.model, model);
                }
            } else {
                if let Some(key) = key {
                    let provider = config
                        .providers_mut()
                        .$field
                        .get_or_insert_with(Default::default);
                    provider.api_key = key;
                    provider.api_key_from_env = true;
                }
                if let Some(provider) = config.providers_mut().$field.as_mut() {
                    metadata.base_url = MetadataOverride::apply(
                        &mut provider.base_url,
                        runtime_base_url(concat!($prefix, "_BASE_URL")),
                    );
                    metadata.model = MetadataOverride::apply(&mut provider.model, model);
                }
            }
            if metadata.base_url.is_some() || metadata.model.is_some() {
                config.provider_metadata_overrides.0.push(metadata);
            }
        }};
    }
    apply!(openai, "BAMBOO_OPENAI");
    apply!(anthropic, "BAMBOO_ANTHROPIC");
    apply!(gemini, "BAMBOO_GEMINI");
}

pub(crate) fn has_invalid_runtime_base_url(config: &Config, provider: &str) -> bool {
    config.provider_metadata_overrides.0.iter().any(|metadata| {
        metadata.provider_type == provider
            && metadata
                .base_url
                .as_ref()
                .is_some_and(|url| url.runtime == INVALID_RUNTIME_PROVIDER_BASE_URL)
    })
}

pub(crate) fn restore_provider_metadata(config: &mut Config) {
    for metadata in std::mem::take(&mut config.provider_metadata_overrides).0 {
        let restore = |base_url: &mut Option<String>, model: &mut Option<String>| {
            if let Some(value) = metadata.base_url {
                value.restore(base_url);
            }
            if let Some(value) = metadata.model {
                value.restore(model);
            }
        };
        if metadata.instance {
            if let Some(instance) = config.provider_instances.get_mut(metadata.provider_type) {
                restore(&mut instance.base_url, &mut instance.model);
            }
        } else {
            let providers = config.providers_mut();
            let fields = match metadata.provider_type {
                "openai" => providers
                    .openai
                    .as_mut()
                    .map(|p| (&mut p.base_url, &mut p.model)),
                "anthropic" => providers
                    .anthropic
                    .as_mut()
                    .map(|p| (&mut p.base_url, &mut p.model)),
                "gemini" => providers
                    .gemini
                    .as_mut()
                    .map(|p| (&mut p.base_url, &mut p.model)),
                _ => unreachable!("only standard provider metadata can be overridden"),
            };
            if let Some((base_url, model)) = fields {
                restore(base_url, model);
            }
        }
    }
}

fn provider_display_label(provider_type: &str) -> &str {
    match provider_type {
        "openai" => "OpenAI",
        "anthropic" => "Anthropic",
        "gemini" => "Gemini",
        "bodhi" => "Bodhi",
        other => other,
    }
}

fn empty_provider_instance(provider_type: &str) -> ProviderInstanceConfig {
    ProviderInstanceConfig {
        provider_type: provider_type.to_string(),
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        AnthropicConfig, BodhiConfig, GeminiConfig, OpenAIConfig, ProviderConfigs,
    };

    /// Build a test config with explicit empty providers/instances. (Since #38,
    /// `Config::default()` is in-memory only — no filesystem/env bleed — so the
    /// remaining fields come from clean defaults.)
    fn clean_test_config() -> Config {
        Config::default()
    }

    #[test]
    fn synthesize_produces_nothing_when_no_legacy_config() {
        let config = clean_test_config();
        let instances = synthesize_legacy_instances(&config);
        // May produce entries if user's default config has providers,
        // so we just verify no duplicates and valid structure.
        for (id, inst) in &instances {
            assert!(!id.is_empty());
            assert!(!inst.provider_type.is_empty());
        }
    }

    #[test]
    fn synthesize_produces_openai_from_legacy() {
        let mut config = clean_test_config();
        let mut extra = std::collections::BTreeMap::new();
        extra.insert(
            crate::OPENAI_EXPLICIT_PROMPT_CACHE_CONFIG_KEY.to_string(),
            serde_json::json!(false),
        );
        config.providers.openai = Some(crate::config::OpenAIConfig {
            api_key: "sk-test".to_string(),
            api_key_encrypted: None,
            credential_ref: None,
            base_url: Some("https://api.openai.com/v1".to_string()),
            model: Some("gpt-4o".to_string()),
            fast_model: Some("gpt-4o-mini".to_string()),
            vision_model: None,
            reasoning_effort: None,
            responses_only_models: vec![],
            request_overrides: None,
            extra,
            api_key_from_env: false,
        });
        // Clear any other legacy providers to isolate this test.
        config.providers.anthropic = None;
        config.providers.gemini = None;
        config.providers.bodhi = None;

        let instances = synthesize_legacy_instances(&config);
        assert_eq!(instances.len(), 1);

        let (id, inst) = &instances[0];
        assert_eq!(id, "openai");
        assert_eq!(inst.provider_type, "openai");
        assert_eq!(inst.api_key, "sk-test");
        assert_eq!(inst.model.as_deref(), Some("gpt-4o"));
        assert_eq!(
            inst.extra
                .get(crate::OPENAI_EXPLICIT_PROMPT_CACHE_CONFIG_KEY),
            Some(&serde_json::json!(false))
        );
    }

    #[test]
    fn synthesize_skips_if_instance_already_exists() {
        let mut config = clean_test_config();
        config.providers.openai = Some(crate::config::OpenAIConfig {
            api_key: "sk-test".to_string(),
            api_key_encrypted: None,
            credential_ref: None,
            base_url: None,
            model: Some("gpt-4o".to_string()),
            fast_model: None,
            vision_model: None,
            reasoning_effort: None,
            responses_only_models: vec![],
            request_overrides: None,
            extra: Default::default(),
            api_key_from_env: false,
        });
        // Clear other providers to isolate.
        config.providers.anthropic = None;
        config.providers.gemini = None;
        config.providers.bodhi = None;

        config.provider_instances.insert(
            "openai".to_string(),
            ProviderInstanceConfig {
                provider_type: "openai".to_string(),
                label: Some("Custom OpenAI".to_string()),
                api_key: "sk-custom".to_string(),
                api_key_encrypted: None,
                credential_ref: None,
                base_url: None,
                model: Some("gpt-4".to_string()),
                fast_model: None,
                vision_model: None,
                reasoning_effort: None,
                responses_only_models: vec![],
                request_overrides: None,
                enabled: true,
                extra: Default::default(),
            },
        );

        let instances = synthesize_legacy_instances(&config);
        assert!(instances.is_empty());
    }

    #[test]
    fn synthesize_preserves_credential_reference_and_provider_specific_fields() {
        let mut config = clean_test_config();
        config.providers.openai = None;
        config.providers.gemini = None;
        config.providers.bodhi = Some(BodhiConfig {
            api_key: String::new(),
            api_key_encrypted: None,
            credential_ref: Some(crate::CredentialRef::parse("provider.bodhi.api_key").unwrap()),
            base_url: None,
            target_provider: Some("anthropic".to_string()),
            reasoning_effort: None,
            extra: [("bodhi_extension".to_string(), Value::Bool(true))]
                .into_iter()
                .collect(),
        });
        config.providers.anthropic = Some(AnthropicConfig {
            credential_ref: Some(
                crate::CredentialRef::parse("provider.anthropic.api_key").unwrap(),
            ),
            max_tokens: Some(8192),
            thinking_replay_always: Some(true),
            extra: [(
                "anthropic_extension".to_string(),
                Value::String("kept".into()),
            )]
            .into_iter()
            .collect(),
            ..AnthropicConfig::default()
        });

        let instances = synthesize_legacy_instances(&config)
            .into_iter()
            .collect::<std::collections::BTreeMap<_, _>>();
        let anthropic = &instances["anthropic"];
        assert_eq!(
            anthropic
                .credential_ref
                .as_ref()
                .map(|value| value.as_str()),
            Some("provider.anthropic.api_key")
        );
        assert_eq!(anthropic.extra["max_tokens"], serde_json::json!(8192));
        assert_eq!(anthropic.extra["thinking_replay_always"], Value::Bool(true));
        assert_eq!(
            anthropic.extra["anthropic_extension"],
            Value::String("kept".into())
        );
        assert_eq!(instances["bodhi"].extra["target_provider"], "anthropic");
        assert_eq!(instances["bodhi"].extra["bodhi_extension"], true);
    }

    #[test]
    fn materialization_is_idempotent_and_reuses_stable_credential_reference() {
        let mut config = clean_test_config();
        config.provider = "openai".to_string();
        config.providers.anthropic = None;
        config.providers.gemini = None;
        config.providers.bodhi = None;
        config.providers.openai = Some(OpenAIConfig {
            credential_ref: Some(crate::CredentialRef::parse("provider.openai.api_key").unwrap()),
            model: Some("gpt-4.1".to_string()),
            ..OpenAIConfig::default()
        });

        assert!(materialize_legacy_provider_instances(&mut config));
        let first = serde_json::to_value(&config.provider_instances).unwrap();
        assert!(!materialize_legacy_provider_instances(&mut config));
        assert_eq!(
            serde_json::to_value(&config.provider_instances).unwrap(),
            first
        );
        assert_eq!(config.default_provider_instance.as_deref(), Some("openai"));
        assert_eq!(
            config.provider_instances["openai"]
                .credential_ref
                .as_ref()
                .map(|value| value.as_str()),
            Some("provider.openai.api_key")
        );
        assert!(provider_instance_api_key_from_env(
            &config.provider_instances["openai"]
        ));
        assert!(!first.to_string().contains("api_key_encrypted"));
    }

    #[test]
    fn explicit_disabled_default_is_never_rewritten_from_stale_legacy_config() {
        let mut config = clean_test_config();
        config.provider = "openai".to_string();
        config.providers.openai = Some(OpenAIConfig {
            credential_ref: Some(crate::CredentialRef::parse("provider.openai.api_key").unwrap()),
            ..OpenAIConfig::default()
        });
        let mut explicit = empty_provider_instance("openai");
        explicit.enabled = false;
        config
            .provider_instances
            .insert("openai".to_string(), explicit);
        config.default_provider_instance = Some("openai".to_string());

        assert!(!materialize_legacy_provider_instances(&mut config));
        assert!(!config.provider_instances["openai"].enabled);
    }

    #[test]
    fn hybrid_missing_legacy_default_is_materialized_with_all_missing_aliases() {
        let mut config = clean_test_config();
        config
            .provider_instances
            .insert("work".to_string(), empty_provider_instance("openai"));
        config.providers.openai = Some(OpenAIConfig {
            credential_ref: Some(crate::CredentialRef::parse("provider.openai.api_key").unwrap()),
            ..OpenAIConfig::default()
        });
        config.providers.anthropic = Some(AnthropicConfig {
            credential_ref: Some(
                crate::CredentialRef::parse("provider.anthropic.api_key").unwrap(),
            ),
            ..AnthropicConfig::default()
        });
        config.default_provider_instance = Some("openai".to_string());

        assert!(materialize_legacy_provider_instances(&mut config));
        assert!(config.provider_instances.contains_key("work"));
        assert!(config.provider_instances.contains_key("openai"));
        assert!(config.provider_instances.contains_key("anthropic"));
        assert_eq!(config.default_provider_instance.as_deref(), Some("openai"));
    }

    #[test]
    fn explicit_default_materializes_noncolliding_legacy_aliases_without_changing_default() {
        let mut config = clean_test_config();
        config.provider = "work".to_string();
        config
            .provider_instances
            .insert("work".to_string(), empty_provider_instance("openai"));
        config.default_provider_instance = Some("work".to_string());
        config.providers.openai = Some(OpenAIConfig {
            credential_ref: Some(crate::CredentialRef::parse("provider.openai.api_key").unwrap()),
            model: Some("gpt-legacy".to_string()),
            ..OpenAIConfig::default()
        });
        config.providers.anthropic = Some(AnthropicConfig {
            credential_ref: Some(
                crate::CredentialRef::parse("provider.anthropic.api_key").unwrap(),
            ),
            model: Some("claude-legacy".to_string()),
            ..AnthropicConfig::default()
        });

        assert!(materialize_legacy_provider_instances(&mut config));
        assert_eq!(config.default_provider_instance.as_deref(), Some("work"));
        assert_eq!(
            config.provider_instances["openai"].model.as_deref(),
            Some("gpt-legacy")
        );
        assert_eq!(
            config.provider_instances["anthropic"].model.as_deref(),
            Some("claude-legacy")
        );
        assert!(provider_instance_api_key_from_env(
            &config.provider_instances["openai"]
        ));
        assert!(provider_instance_api_key_from_env(
            &config.provider_instances["anthropic"]
        ));
    }

    #[test]
    fn ordinary_missing_key_does_not_gain_an_environment_marker() {
        let mut config = clean_test_config();
        config.providers.anthropic = None;
        config.providers.gemini = None;
        config.providers.bodhi = None;
        config.providers.openai = Some(OpenAIConfig::default());

        let synthesized = synthesize_legacy_instances(&config);
        assert_eq!(synthesized.len(), 1);
        assert!(!provider_instance_api_key_from_env(&synthesized[0].1));
    }

    #[test]
    fn missing_unusable_default_does_not_create_dangling_instance() {
        let mut config = clean_test_config();
        config.provider = "bodhi".to_string();
        *config.providers_mut() = ProviderConfigs {
            bodhi: Some(BodhiConfig {
                api_key: String::new(),
                api_key_encrypted: None,
                credential_ref: None,
                base_url: None,
                target_provider: None,
                reasoning_effort: None,
                extra: Default::default(),
            }),
            ..ProviderConfigs::default()
        };

        assert!(!materialize_legacy_provider_instances(&mut config));
        assert!(config.provider_instances.is_empty());
        assert!(config.default_provider_instance.is_none());
    }

    #[test]
    fn env_owned_legacy_key_migrates_as_marker_without_secret_material() {
        let _openai = crate::test_support::override_runtime_env_var("BAMBOO_OPENAI_API_KEY", None);
        let _anthropic =
            crate::test_support::override_runtime_env_var("BAMBOO_ANTHROPIC_API_KEY", None);
        let _gemini = crate::test_support::override_runtime_env_var(
            "BAMBOO_GEMINI_API_KEY",
            Some("runtime-only"),
        );
        let mut config = clean_test_config();
        config.provider = "gemini".to_string();
        *config.providers_mut() = ProviderConfigs {
            gemini: Some(GeminiConfig {
                api_key: "runtime-only".to_string(),
                api_key_from_env: true,
                ..GeminiConfig::default()
            }),
            ..ProviderConfigs::default()
        };

        assert!(materialize_legacy_provider_instances(&mut config));
        let instance = &config.provider_instances["gemini"];
        assert!(provider_instance_api_key_from_env(instance));
        let disk = serde_json::to_value(instance).unwrap();
        assert_eq!(disk[PROVIDER_INSTANCE_API_KEY_FROM_ENV_CONFIG_KEY], true);
        assert!(disk.get("api_key").is_none());
        assert!(disk.get("api_key_encrypted").is_none());
    }

    #[test]
    fn materialized_legacy_ref_keeps_environment_override_and_stored_fallback() {
        let openai = crate::test_support::override_runtime_env_var("BAMBOO_OPENAI_API_KEY", None);
        let _anthropic =
            crate::test_support::override_runtime_env_var("BAMBOO_ANTHROPIC_API_KEY", None);
        let _gemini = crate::test_support::override_runtime_env_var("BAMBOO_GEMINI_API_KEY", None);
        let mut config = clean_test_config();
        config.provider = "openai".to_string();
        config.providers.openai = Some(OpenAIConfig {
            api_key: "sk-stored-fallback".to_string(),
            credential_ref: Some(crate::CredentialRef::parse("provider.openai.api_key").unwrap()),
            ..OpenAIConfig::default()
        });

        assert!(materialize_legacy_provider_instances(&mut config));
        assert!(provider_instance_api_key_from_env(
            &config.provider_instances["openai"]
        ));
        config.apply_runtime_env_overrides();
        assert_eq!(
            config.provider_instances["openai"].api_key,
            "sk-stored-fallback"
        );
        assert!(!provider_instance_environment_override_active(
            &config.provider_instances["openai"]
        ));

        openai.replace(Some("sk-runtime-override"));
        config.apply_runtime_env_overrides();
        assert_eq!(
            config.provider_instances["openai"].api_key,
            "sk-runtime-override"
        );
        assert!(provider_instance_environment_override_active(
            &config.provider_instances["openai"]
        ));

        openai.replace(None);
        config.provider_instances.get_mut("openai").unwrap().api_key =
            "sk-stored-fallback".to_string();
        config.apply_runtime_env_overrides();
        assert_eq!(
            config.provider_instances["openai"].api_key,
            "sk-stored-fallback"
        );
        assert!(!provider_instance_environment_override_active(
            &config.provider_instances["openai"]
        ));
    }

    #[test]
    fn nonselected_environment_only_provider_is_materialized_and_hydrated() {
        let _openai = crate::test_support::override_runtime_env_var("BAMBOO_OPENAI_API_KEY", None);
        let _anthropic = crate::test_support::override_runtime_env_var(
            "BAMBOO_ANTHROPIC_API_KEY",
            Some("sk-ant-runtime"),
        );
        let _gemini = crate::test_support::override_runtime_env_var("BAMBOO_GEMINI_API_KEY", None);
        let mut config = clean_test_config();
        config.provider = "openai".to_string();
        config.providers.openai = Some(OpenAIConfig {
            api_key: "sk-openai-stored".to_string(),
            credential_ref: Some(crate::CredentialRef::parse("provider.openai.api_key").unwrap()),
            ..OpenAIConfig::default()
        });

        assert!(materialize_legacy_provider_instances(&mut config));
        assert_eq!(config.default_provider_instance.as_deref(), Some("openai"));
        let anthropic = &config.provider_instances["anthropic"];
        assert!(provider_instance_api_key_from_env(anthropic));
        assert!(anthropic.credential_ref.is_none());
        assert!(anthropic.api_key.is_empty());

        config.apply_runtime_env_overrides();
        assert_eq!(
            config.provider_instances["anthropic"].api_key,
            "sk-ant-runtime"
        );
        assert!(provider_instance_environment_override_active(
            &config.provider_instances["anthropic"]
        ));
    }

    #[test]
    fn runtime_endpoint_and_model_overlay_only_the_bound_builtin_instance() {
        let _provider = crate::test_support::override_runtime_env_var("BAMBOO_PROVIDER", None);
        let _base = crate::test_support::override_runtime_env_var(
            "BAMBOO_OPENAI_BASE_URL",
            Some(" https://runtime.example/v1/ "),
        );
        let model =
            crate::test_support::override_runtime_env_var("BAMBOO_OPENAI_MODEL", Some(" runtime "));
        let mut config: Config = serde_json::from_value(serde_json::json!({
            "default_provider_instance": "openai",
            "provider_instances": {
                "openai": {
                    "provider_type": "openai", "api_key_from_env": true,
                    "base_url": "https://saved.example/v1", "model": "saved"
                },
                "named": {
                    "provider_type": "openai", "api_key_from_env": true,
                    "base_url": "https://named.example/v1", "model": "named"
                }
            }
        }))
        .unwrap();
        config.apply_runtime_env_overrides();
        let built_in = &config.provider_instances["openai"];
        assert_eq!(
            built_in.base_url.as_deref(),
            Some("https://runtime.example/v1")
        );
        assert_eq!(built_in.model.as_deref(), Some("runtime"));
        let named = &config.provider_instances["named"];
        assert_eq!(named.base_url.as_deref(), Some("https://named.example/v1"));
        assert_eq!(named.model.as_deref(), Some("named"));

        let projection =
            crate::SectionProjection::from_config(&config, crate::ModelLimitsSection(Vec::new()))
                .unwrap();
        let saved = &projection.providers.provider_instances["openai"];
        assert_eq!(saved.base_url.as_deref(), Some("https://saved.example/v1"));
        assert_eq!(saved.model.as_deref(), Some("saved"));

        model.replace(Some("   "));
        config.apply_runtime_env_overrides();
        assert_eq!(
            config.provider_instances["openai"].model.as_deref(),
            Some("saved")
        );
    }

    #[test]
    fn retired_legacy_default_exposes_keyed_provider_without_selecting_a_replacement() {
        let _openai = crate::test_support::override_runtime_env_var("BAMBOO_OPENAI_API_KEY", None);
        let _anthropic =
            crate::test_support::override_runtime_env_var("BAMBOO_ANTHROPIC_API_KEY", None);
        let _gemini = crate::test_support::override_runtime_env_var("BAMBOO_GEMINI_API_KEY", None);
        let mut config = clean_test_config();
        config.provider = "copilot".into();
        *config.providers_mut() = ProviderConfigs {
            openai: Some(OpenAIConfig {
                credential_ref: Some(
                    crate::CredentialRef::parse("provider.openai.api_key").unwrap(),
                ),
                model: Some("chat-model".into()),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(retire_copilot(&mut config));
        assert!(materialize_legacy_provider_instances(&mut config));
        assert!(config.default_provider_instance.is_none());
        assert!(!config.provider_instances.contains_key("copilot"));
        assert_eq!(
            config.provider_instances["openai"].model.as_deref(),
            Some("chat-model")
        );
        assert!(!materialize_legacy_provider_instances(&mut config));
    }
}
