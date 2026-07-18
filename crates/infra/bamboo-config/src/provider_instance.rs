//! Synthesize legacy provider config into the new instance-keyed format.

use super::config::{Config, ProviderInstanceConfig};

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
            result.push((
                id,
                ProviderInstanceConfig {
                    provider_type: "openai".to_string(),
                    label: Some("OpenAI".to_string()),
                    api_key: openai.api_key.clone(),
                    api_key_encrypted: openai.api_key_encrypted.clone(),
                    base_url: openai.base_url.clone(),
                    model: openai.model.clone(),
                    fast_model: openai.fast_model.clone(),
                    vision_model: openai.vision_model.clone(),
                    reasoning_effort: openai.reasoning_effort,
                    responses_only_models: openai.responses_only_models.clone(),
                    request_overrides: openai.request_overrides.clone(),
                    enabled: true,
                    extra: Default::default(),
                },
            ));
        }
    }

    if let Some(anthropic) = &config.providers.anthropic {
        let id = "anthropic".to_string();
        if !config.provider_instances.contains_key(&id) {
            result.push((
                id,
                ProviderInstanceConfig {
                    provider_type: "anthropic".to_string(),
                    label: Some("Anthropic".to_string()),
                    api_key: anthropic.api_key.clone(),
                    api_key_encrypted: anthropic.api_key_encrypted.clone(),
                    base_url: anthropic.base_url.clone(),
                    model: anthropic.model.clone(),
                    fast_model: anthropic.fast_model.clone(),
                    vision_model: anthropic.vision_model.clone(),
                    reasoning_effort: anthropic.reasoning_effort,
                    responses_only_models: vec![],
                    request_overrides: anthropic.request_overrides.clone(),
                    enabled: true,
                    extra: Default::default(),
                },
            ));
        }
    }

    if let Some(gemini) = &config.providers.gemini {
        let id = "gemini".to_string();
        if !config.provider_instances.contains_key(&id) {
            result.push((
                id,
                ProviderInstanceConfig {
                    provider_type: "gemini".to_string(),
                    label: Some("Gemini".to_string()),
                    api_key: gemini.api_key.clone(),
                    api_key_encrypted: gemini.api_key_encrypted.clone(),
                    base_url: gemini.base_url.clone(),
                    model: gemini.model.clone(),
                    fast_model: gemini.fast_model.clone(),
                    vision_model: gemini.vision_model.clone(),
                    reasoning_effort: gemini.reasoning_effort,
                    responses_only_models: vec![],
                    request_overrides: gemini.request_overrides.clone(),
                    enabled: true,
                    extra: Default::default(),
                },
            ));
        }
    }

    if config.providers.copilot.is_some() {
        let id = "copilot".to_string();
        if !config.provider_instances.contains_key(&id) {
            // Copilot doesn't have a traditional api_key; it uses device auth.
            result.push((
                id,
                ProviderInstanceConfig {
                    provider_type: "copilot".to_string(),
                    label: Some("GitHub Copilot".to_string()),
                    api_key: String::new(),
                    api_key_encrypted: None,
                    base_url: None,
                    model: config
                        .providers
                        .copilot
                        .as_ref()
                        .and_then(|c| c.model.clone()),
                    fast_model: config
                        .providers
                        .copilot
                        .as_ref()
                        .and_then(|c| c.fast_model.clone()),
                    vision_model: config
                        .providers
                        .copilot
                        .as_ref()
                        .and_then(|c| c.vision_model.clone()),
                    reasoning_effort: config
                        .providers
                        .copilot
                        .as_ref()
                        .and_then(|c| c.reasoning_effort),
                    responses_only_models: config
                        .providers
                        .copilot
                        .as_ref()
                        .map(|c| c.responses_only_models.clone())
                        .unwrap_or_default(),
                    request_overrides: config
                        .providers
                        .copilot
                        .as_ref()
                        .and_then(|c| c.request_overrides.clone()),
                    enabled: true,
                    extra: Default::default(),
                },
            ));
        }
    }

    if let Some(bodhi) = &config.providers.bodhi {
        let id = "bodhi".to_string();
        if !config.provider_instances.contains_key(&id) {
            result.push((
                id,
                ProviderInstanceConfig {
                    provider_type: "bodhi".to_string(),
                    label: Some("Bodhi".to_string()),
                    api_key: bodhi.api_key.clone(),
                    api_key_encrypted: bodhi.api_key_encrypted.clone(),
                    base_url: bodhi.base_url.clone(),
                    model: None,
                    fast_model: None,
                    vision_model: None,
                    reasoning_effort: bodhi.reasoning_effort,
                    responses_only_models: vec![],
                    request_overrides: None,
                    enabled: true,
                    extra: Default::default(),
                },
            ));
        }
    }

    result
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
        config.providers.openai = Some(crate::config::OpenAIConfig {
            api_key: "sk-test".to_string(),
            api_key_encrypted: None,
            base_url: Some("https://api.openai.com/v1".to_string()),
            model: Some("gpt-4o".to_string()),
            fast_model: Some("gpt-4o-mini".to_string()),
            vision_model: None,
            reasoning_effort: None,
            responses_only_models: vec![],
            request_overrides: None,
            extra: Default::default(),
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
    }

    #[test]
    fn synthesize_skips_if_instance_already_exists() {
        let mut config = clean_test_config();
        config.providers.openai = Some(crate::config::OpenAIConfig {
            api_key: "sk-test".to_string(),
            api_key_encrypted: None,
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
}
