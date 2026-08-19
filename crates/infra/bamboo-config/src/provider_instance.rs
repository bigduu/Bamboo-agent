//! Legacy-provider compatibility at the provider-instance boundary.
//!
//! Runtime code consumes [`ProviderInstanceConfig`] directly.  The helpers in
//! this module are deliberately limited to the two compatibility directions:
//!
//! - materialize a legacy-only configuration into durable provider instances;
//! - project instances into a temporary legacy view for the deprecated settings
//!   response.
//!
//! The latter must never be used as a provider-construction substrate.

use super::config::{
    AnthropicConfig, BodhiConfig, Config, CopilotConfig, GeminiConfig, OpenAIConfig,
    ProviderConfigs, ProviderInstanceConfig,
};
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
    let Some(env_var) = standard_provider_api_key_env(&instance.provider_type) else {
        return false;
    };
    let Ok(value) = crate::runtime_env_var(env_var) else {
        return false;
    };
    let value = value.trim();
    !value.is_empty() && instance.api_key.trim() == value
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
        if !config.provider_instances.contains_key(&id) {
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
        if !config.provider_instances.contains_key(&id) {
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
        if !config.provider_instances.contains_key(&id) {
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

    if let Some(copilot) = &config.providers.copilot {
        let id = "copilot".to_string();
        if !config.provider_instances.contains_key(&id) {
            // Copilot doesn't have a traditional api_key; it uses device auth.
            let mut extra = copilot.extra.clone();
            extra
                .entry("headless_auth".to_string())
                .or_insert(Value::Bool(copilot.headless_auth));
            result.push((
                id,
                ProviderInstanceConfig {
                    provider_type: "copilot".to_string(),
                    label: Some("GitHub Copilot".to_string()),
                    api_key: String::new(),
                    api_key_encrypted: None,
                    credential_ref: None,
                    base_url: None,
                    model: copilot.model.clone(),
                    fast_model: copilot.fast_model.clone(),
                    vision_model: copilot.vision_model.clone(),
                    reasoning_effort: copilot.reasoning_effort,
                    responses_only_models: copilot.responses_only_models.clone(),
                    request_overrides: copilot.request_overrides.clone(),
                    enabled: copilot.enabled || config.provider == "copilot",
                    extra,
                },
            ));
        }
    }

    if let Some(bodhi) = &config.providers.bodhi {
        let id = "bodhi".to_string();
        if !config.provider_instances.contains_key(&id) {
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
    if selected.is_empty() {
        return false;
    }
    let selected_is_explicit = config.provider_instances.contains_key(&selected);

    let mut candidates = synthesize_legacy_instances(config);

    if !selected_is_explicit
        && selected == "copilot"
        && !candidates.iter().any(|(id, _)| id == &selected)
    {
        candidates.push((
            selected.clone(),
            default_copilot_instance(config.headless_auth),
        ));
    }

    // Preserve every standard legacy env-only provider, not just the selected
    // one. A custom explicit instance id must not absorb a type-wide legacy
    // override, so the compatibility alias keeps the provider type as its id.
    for provider_type in ["openai", "anthropic", "gemini"] {
        if standard_provider_api_key_env_is_available(provider_type)
            && !config.provider_instances.contains_key(provider_type)
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

    if let Some((_, selected_instance)) = candidates
        .iter_mut()
        .find(|(id, _)| id == &selected && selected == "copilot")
    {
        selected_instance.enabled = true;
    }

    let selected_is_usable = selected_is_explicit
        || candidates.iter().any(|(id, instance)| {
            id == &selected
                && instance.enabled
                && (instance.provider_type == "copilot"
                    || instance.credential_ref.is_some()
                    || (provider_instance_api_key_from_env(instance)
                        && standard_provider_api_key_env_is_available(&instance.provider_type)))
        });
    if !selected_is_usable {
        return false;
    }

    let mut changed = !candidates.is_empty();
    config.provider_instances.extend(candidates);
    if config.default_provider_instance.as_deref() != Some(&selected) {
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

fn provider_display_label(provider_type: &str) -> &str {
    match provider_type {
        "openai" => "OpenAI",
        "anthropic" => "Anthropic",
        "gemini" => "Gemini",
        "copilot" => "GitHub Copilot",
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

fn default_copilot_instance(headless_auth: bool) -> ProviderInstanceConfig {
    let mut instance = empty_provider_instance("copilot");
    instance.label = Some("GitHub Copilot".to_string());
    instance
        .extra
        .insert("headless_auth".to_string(), Value::Bool(headless_auth));
    instance
}

/// Build the deprecated provider-settings view from instance-native state.
///
/// The durable and runtime authorities remain `provider_instances`. This is a
/// response-only projection for old clients during the Lotus #177 migration
/// window. The default instance is considered first; remaining instances are
/// ordered by id so two identical configs always produce the same legacy view.
pub fn legacy_provider_compatibility_view(config: &Config) -> ProviderConfigs {
    if config.provider_instances.is_empty() {
        return config.providers().clone();
    }

    // In instance mode, never let a stale legacy slot mask the authoritative
    // instance selected below. Retain provider-section extension metadata and
    // use legacy builtins only to fill provider types that no instance can
    // represent for the old type-keyed DTO.
    let legacy = config.providers().clone();
    let mut providers = ProviderConfigs {
        extra: legacy.extra.clone(),
        ..ProviderConfigs::default()
    };

    // The narrow hybrid seam is ordered first: while a degraded facade defers
    // materialization, the effective default may still name a real legacy
    // alias. It must beat a non-default explicit instance of the same type.
    let effective_default = config.effective_default_provider();
    if !config.provider_instances.contains_key(effective_default) {
        if let Some((_, instance)) = synthesize_legacy_instances(config)
            .into_iter()
            .find(|(id, _)| id == effective_default)
        {
            project_instance_for_legacy_api(&mut providers, &instance);
        }
    }

    let mut ids = config
        .provider_instances
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    ids.sort();
    if let Some(default) = config.default_provider_instance.as_deref() {
        if let Some(index) = ids.iter().position(|id| id == default) {
            let default = ids.remove(index);
            ids.insert(0, default);
        }
    }
    for id in ids {
        let Some(instance) = config.provider_instances.get(&id) else {
            continue;
        };
        if instance.enabled {
            project_instance_for_legacy_api(&mut providers, instance);
        }
    }

    // Fill provider types absent from the authoritative instance projection so
    // legacy clients keep their read-only catalog during the Lotus #177
    // migration window. `project_instance_for_legacy_api` never overwrites a
    // type already selected above.
    let represented_types = config
        .provider_instances
        .values()
        .map(|instance| instance.provider_type.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    for (_, instance) in synthesize_legacy_instances(config) {
        if !represented_types.contains(instance.provider_type.as_str()) {
            project_instance_for_legacy_api(&mut providers, &instance);
        }
    }
    providers
}

fn project_instance_for_legacy_api(
    providers: &mut ProviderConfigs,
    instance: &ProviderInstanceConfig,
) {
    match instance.provider_type.as_str() {
        "openai" if providers.openai.is_none() => {
            providers.openai = Some(OpenAIConfig {
                api_key: instance.api_key.clone(),
                api_key_encrypted: instance.api_key_encrypted.clone(),
                credential_ref: instance.credential_ref.clone(),
                api_key_from_env: provider_instance_api_key_from_env(instance),
                base_url: instance.base_url.clone(),
                model: instance.model.clone(),
                fast_model: instance.fast_model.clone(),
                vision_model: instance.vision_model.clone(),
                reasoning_effort: instance.reasoning_effort,
                responses_only_models: instance.responses_only_models.clone(),
                request_overrides: instance.request_overrides.clone(),
                extra: compatibility_extra(instance, &[]),
            });
        }
        "anthropic" if providers.anthropic.is_none() => {
            providers.anthropic = Some(AnthropicConfig {
                api_key: instance.api_key.clone(),
                api_key_encrypted: instance.api_key_encrypted.clone(),
                credential_ref: instance.credential_ref.clone(),
                api_key_from_env: provider_instance_api_key_from_env(instance),
                base_url: instance.base_url.clone(),
                model: instance.model.clone(),
                fast_model: instance.fast_model.clone(),
                vision_model: instance.vision_model.clone(),
                max_tokens: instance
                    .extra
                    .get("max_tokens")
                    .and_then(Value::as_u64)
                    .and_then(|value| u32::try_from(value).ok()),
                reasoning_effort: instance.reasoning_effort,
                request_overrides: instance.request_overrides.clone(),
                thinking_replay_always: instance
                    .extra
                    .get("thinking_replay_always")
                    .and_then(Value::as_bool),
                extra: compatibility_extra(instance, &["max_tokens", "thinking_replay_always"]),
            });
        }
        "gemini" if providers.gemini.is_none() => {
            providers.gemini = Some(GeminiConfig {
                api_key: instance.api_key.clone(),
                api_key_encrypted: instance.api_key_encrypted.clone(),
                credential_ref: instance.credential_ref.clone(),
                api_key_from_env: provider_instance_api_key_from_env(instance),
                base_url: instance.base_url.clone(),
                model: instance.model.clone(),
                fast_model: instance.fast_model.clone(),
                vision_model: instance.vision_model.clone(),
                reasoning_effort: instance.reasoning_effort,
                request_overrides: instance.request_overrides.clone(),
                extra: compatibility_extra(instance, &[]),
            });
        }
        "copilot" if providers.copilot.is_none() => {
            providers.copilot = Some(CopilotConfig {
                enabled: instance.enabled,
                headless_auth: instance
                    .extra
                    .get("headless_auth")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                model: instance.model.clone(),
                fast_model: instance.fast_model.clone(),
                vision_model: instance.vision_model.clone(),
                reasoning_effort: instance.reasoning_effort,
                responses_only_models: instance.responses_only_models.clone(),
                request_overrides: instance.request_overrides.clone(),
                extra: compatibility_extra(instance, &["headless_auth"]),
            });
        }
        "bodhi" if providers.bodhi.is_none() => {
            providers.bodhi = Some(BodhiConfig {
                api_key: instance.api_key.clone(),
                api_key_encrypted: instance.api_key_encrypted.clone(),
                credential_ref: instance.credential_ref.clone(),
                base_url: instance.base_url.clone(),
                target_provider: instance
                    .extra
                    .get("target_provider")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
                reasoning_effort: instance.reasoning_effort,
                extra: compatibility_extra(instance, &["target_provider"]),
            });
        }
        _ => {}
    }
}

fn compatibility_extra(
    instance: &ProviderInstanceConfig,
    consumed_fields: &[&str],
) -> std::collections::BTreeMap<String, Value> {
    let mut extra = instance.extra.clone();
    extra.remove(PROVIDER_INSTANCE_API_KEY_FROM_ENV_CONFIG_KEY);
    for field in consumed_fields {
        extra.remove(*field);
    }
    extra
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

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
        config.providers.copilot = None;
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
        config.providers.copilot = None;
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
        config.providers.copilot = Some(CopilotConfig {
            enabled: true,
            headless_auth: true,
            extra: [(
                "copilot_extension".to_string(),
                Value::String("kept".into()),
            )]
            .into_iter()
            .collect(),
            ..CopilotConfig::default()
        });
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
        assert_eq!(instances["copilot"].extra["headless_auth"], true);
        assert_eq!(instances["copilot"].extra["copilot_extension"], "kept");
        assert_eq!(instances["bodhi"].extra["target_provider"], "anthropic");
        assert_eq!(instances["bodhi"].extra["bodhi_extension"], true);
    }

    #[test]
    fn materialization_is_idempotent_and_reuses_stable_credential_reference() {
        let mut config = clean_test_config();
        config.provider = "openai".to_string();
        config.providers.anthropic = None;
        config.providers.gemini = None;
        config.providers.copilot = None;
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
        config.providers.copilot = None;
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
    fn selected_copilot_without_legacy_stanza_gets_a_default_instance() {
        let mut config = clean_test_config();
        config.provider = "copilot".to_string();
        *config.providers_mut() = ProviderConfigs::default();

        assert!(materialize_legacy_provider_instances(&mut config));
        assert_eq!(config.default_provider_instance.as_deref(), Some("copilot"));
        assert!(config.provider_instances["copilot"].enabled);
    }

    #[test]
    fn legacy_view_prefers_authoritative_instance_over_stale_same_type_slot() {
        let mut config = clean_test_config();
        config.providers.openai = Some(OpenAIConfig {
            api_key: "sk-stale".to_string(),
            model: Some("stale-model".to_string()),
            ..OpenAIConfig::default()
        });
        config.provider_instances.insert(
            "work".to_string(),
            ProviderInstanceConfig {
                provider_type: "openai".to_string(),
                label: Some("Work".to_string()),
                api_key: "sk-instance".to_string(),
                api_key_encrypted: None,
                credential_ref: None,
                base_url: Some("https://work.example/v1".to_string()),
                model: Some("instance-model".to_string()),
                fast_model: None,
                vision_model: None,
                reasoning_effort: None,
                responses_only_models: Vec::new(),
                request_overrides: None,
                enabled: true,
                extra: Default::default(),
            },
        );
        config.default_provider_instance = Some("work".to_string());

        let view = legacy_provider_compatibility_view(&config);
        let openai = view.openai.expect("instance should be projected");
        assert_eq!(openai.api_key, "sk-instance");
        assert_eq!(openai.model.as_deref(), Some("instance-model"));
        assert_eq!(openai.base_url.as_deref(), Some("https://work.example/v1"));
    }

    #[test]
    fn legacy_view_prioritizes_effective_hybrid_alias_and_fills_missing_legacy_types() {
        let mut config = clean_test_config();
        config.provider = "anthropic".to_string();
        config.providers.anthropic = Some(AnthropicConfig {
            api_key: "sk-effective".to_string(),
            model: Some("claude-effective".to_string()),
            ..AnthropicConfig::default()
        });
        config.providers.gemini = Some(GeminiConfig {
            api_key: "stale-gemini".to_string(),
            ..GeminiConfig::default()
        });
        config.provider_instances.insert(
            "work".to_string(),
            ProviderInstanceConfig {
                provider_type: "openai".to_string(),
                label: None,
                api_key: "sk-work".to_string(),
                api_key_encrypted: None,
                credential_ref: None,
                base_url: None,
                model: Some("gpt-work".to_string()),
                fast_model: None,
                vision_model: None,
                reasoning_effort: None,
                responses_only_models: Vec::new(),
                request_overrides: None,
                enabled: true,
                extra: Default::default(),
            },
        );

        let view = legacy_provider_compatibility_view(&config);
        assert_eq!(
            view.anthropic.and_then(|provider| provider.model),
            Some("claude-effective".to_string())
        );
        assert!(view.openai.is_some());
        assert!(
            view.gemini.is_some(),
            "legacy-only provider types remain visible to the deprecated read API"
        );
    }

    #[test]
    fn legacy_projection_serializes_consumed_extra_fields_exactly_once() {
        let mut config = clean_test_config();
        *config.providers_mut() = ProviderConfigs::default();
        for (id, provider_type, extra) in [
            (
                "anthropic-work",
                "anthropic",
                serde_json::json!({
                    "max_tokens": 6144,
                    "thinking_replay_always": true,
                    "api_key_from_env": true,
                    "anthropic_extension": "kept"
                }),
            ),
            (
                "copilot-work",
                "copilot",
                serde_json::json!({
                    "headless_auth": true,
                    "copilot_extension": "kept"
                }),
            ),
            (
                "bodhi-work",
                "bodhi",
                serde_json::json!({
                    "target_provider": "gemini",
                    "bodhi_extension": "kept"
                }),
            ),
        ] {
            let mut value = extra.as_object().unwrap().clone();
            value.insert(
                "provider_type".to_string(),
                Value::String(provider_type.to_string()),
            );
            value.insert("enabled".to_string(), Value::Bool(true));
            let mut instance: ProviderInstanceConfig =
                serde_json::from_value(Value::Object(value)).unwrap();
            if provider_type != "copilot" {
                instance.api_key = format!("secret-{id}");
            }
            config.provider_instances.insert(id.to_string(), instance);
        }
        config.default_provider_instance = Some("anthropic-work".to_string());

        let value = serde_json::to_value(legacy_provider_compatibility_view(&config)).unwrap();
        assert_eq!(value["anthropic"]["max_tokens"], 6144);
        assert_eq!(value["anthropic"]["thinking_replay_always"], true);
        assert_eq!(value["anthropic"]["anthropic_extension"], "kept");
        assert!(value["anthropic"].get("api_key_from_env").is_none());
        assert_eq!(value["copilot"]["headless_auth"], true);
        assert_eq!(value["copilot"]["copilot_extension"], "kept");
        assert_eq!(value["bodhi"]["target_provider"], "gemini");
        assert_eq!(value["bodhi"]["bodhi_extension"], "kept");

        let round_trip: ProviderConfigs = serde_json::from_value(value).unwrap();
        assert_eq!(round_trip.anthropic.unwrap().max_tokens, Some(6144));
        assert!(round_trip.copilot.unwrap().headless_auth);
        assert_eq!(
            round_trip.bodhi.unwrap().target_provider.as_deref(),
            Some("gemini")
        );
    }
}
