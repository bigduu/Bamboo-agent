//! Provider model admission is configuration authority, never upstream discovery.

use std::collections::{BTreeSet, HashMap};

use crate::{Config, DefaultsConfig, ProviderInstanceConfig};

/// Flattened provider-instance field exposed as `config.runtime_models` by CRUD.
pub const RUNTIME_MODELS_CONFIG_KEY: &str = "runtime_models";

/// An explicit list wins even when empty. Invalid lists must fail closed.
pub fn explicit_runtime_models(
    instance: &ProviderInstanceConfig,
) -> Result<Option<Vec<String>>, String> {
    let Some(value) = instance.extra.get(RUNTIME_MODELS_CONFIG_KEY) else {
        return Ok(None);
    };
    let values = value
        .as_array()
        .ok_or_else(|| "runtime_models must be an array of non-empty model IDs".to_string())?;
    let mut models = BTreeSet::new();
    for value in values {
        let model = value
            .as_str()
            .map(str::trim)
            .filter(|model| !model.is_empty())
            .ok_or_else(|| "runtime_models must contain only non-empty model IDs".to_string())?;
        models.insert(model.to_string());
    }
    Ok(Some(models.into_iter().collect()))
}

fn default_refs(
    defaults: &DefaultsConfig,
) -> impl Iterator<Item = &bamboo_domain::ProviderModelRef> {
    std::iter::once(&defaults.chat)
        .chain(defaults.fast.iter())
        .chain(defaults.task_summary.iter())
        .chain(defaults.vision.iter())
        .chain(defaults.memory_background.iter())
        .chain(defaults.planning.iter())
        .chain(defaults.search.iter())
        .chain(defaults.code_review.iter())
        .chain(defaults.sub_agent.iter())
        .chain(defaults.subagent_models.values())
}

/// Resolve configured type aliases using the same deterministic instance order
/// as runtime routing. An explicit instance or legacy alias retains its identity.
pub fn configured_provider_routing_key(config: &Config, provider: &str) -> String {
    let provider = provider.trim();
    if config.provider_instances.contains_key(provider)
        || ((!config.has_provider_instances() || config.effective_default_provider() == provider)
            && crate::synthesize_legacy_instances(config)
                .iter()
                .any(|(id, _)| id == provider))
    {
        return provider.to_string();
    }
    if let Some(id) = config.default_provider_instance.as_deref() {
        if config
            .provider_instances
            .get(id)
            .is_some_and(|instance| instance.provider_type == provider)
        {
            return id.to_string();
        }
    }
    config
        .provider_instances
        .iter()
        .filter(|(_, instance)| instance.enabled && instance.provider_type == provider)
        .map(|(id, _)| id)
        .min()
        .cloned()
        .unwrap_or_else(|| provider.to_string())
}

fn configured_models(config: &Config, id: &str, instance: &ProviderInstanceConfig) -> Vec<String> {
    let mut models: BTreeSet<String> = [
        instance.model.as_deref(),
        instance.fast_model.as_deref(),
        instance.vision_model.as_deref(),
    ]
    .into_iter()
    .flatten()
    .map(str::trim)
    .filter(|model| !model.is_empty())
    .map(ToString::to_string)
    .collect();
    if let Some(defaults) = config.defaults.as_ref() {
        for reference in default_refs(defaults) {
            if configured_provider_routing_key(config, &reference.provider) == id
                && !reference.model.trim().is_empty()
            {
                models.insert(reference.model.trim().to_string());
            }
        }
    }
    models.into_iter().collect()
}

/// Read one provider's admitted IDs. Missing fields migrate only configured
/// selections; no upstream catalog or hardcoded model is consulted.
pub fn provider_runtime_models(config: &Config, provider: &str) -> Result<Vec<String>, String> {
    let id = configured_provider_routing_key(config, provider);
    let synthesized = crate::synthesize_legacy_instances(config);
    let instance = config.provider_instances.get(&id).or_else(|| {
        synthesized
            .iter()
            .find(|(name, _)| name == &id)
            .map(|(_, instance)| instance)
    });
    let Some(instance) = instance else {
        return Ok(Vec::new());
    };
    if !instance.enabled {
        return Ok(Vec::new());
    }
    Ok(explicit_runtime_models(instance)?
        .unwrap_or_else(|| configured_models(config, &id, instance)))
}

/// Reject removal of a configured role model instead of silently re-admitting
/// it. Role changes and admission changes can be saved in one config update.
pub fn validate_runtime_model_admission(config: &Config) -> Result<(), String> {
    let mut instances: HashMap<String, ProviderInstanceConfig> =
        crate::synthesize_legacy_instances(config)
            .into_iter()
            .collect();
    instances.extend(config.provider_instances.clone());
    for (id, instance) in &instances {
        if let Some(admitted) = explicit_runtime_models(instance)? {
            for model in configured_models(config, id, instance) {
                if !admitted.contains(&model) {
                    return Err(format!(
                        "Model '{model}' is configured for provider '{id}' but is not in runtime_models; change its role assignment before removing it"
                    ));
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn config() -> Config {
        let mut config = Config::default();
        config.provider_instances.insert("relay".into(), serde_json::from_value(json!({
            "provider_type":"openai", "model":"chat", "fast_model":"fast", "vision_model":"vision"
        })).unwrap());
        config.defaults = Some(
            serde_json::from_value(json!({
                "chat":{"provider":"relay","model":"chat"},
                "sub_agent":{"provider":"relay","model":"child"},
                "fast":{"provider":"other","model":"other-fast"}
            }))
            .unwrap(),
        );
        config
    }

    #[test]
    fn missing_admission_seeds_only_configured_models_for_that_provider() {
        assert_eq!(
            provider_runtime_models(&config(), "relay").unwrap(),
            ["chat", "child", "fast", "vision"]
        );
    }

    #[test]
    fn explicit_custom_ids_are_trimmed_and_deduplicated_without_discovery() {
        let mut config = config();
        config
            .provider_instances
            .get_mut("relay")
            .unwrap()
            .extra
            .insert(
                RUNTIME_MODELS_CONFIG_KEY.into(),
                json!([" custom/id ", "custom/id"]),
            );
        assert_eq!(
            provider_runtime_models(&config, "relay").unwrap(),
            ["custom/id"]
        );
        assert!(validate_runtime_model_admission(&config).is_err());
    }

    #[test]
    fn explicit_empty_is_not_repopulated_from_defaults() {
        let mut config = config();
        config
            .provider_instances
            .get_mut("relay")
            .unwrap()
            .extra
            .insert(RUNTIME_MODELS_CONFIG_KEY.into(), json!([]));
        assert!(provider_runtime_models(&config, "relay")
            .unwrap()
            .is_empty());
        assert!(validate_runtime_model_admission(&config)
            .unwrap_err()
            .contains("role assignment"));
        config.defaults = None;
        let instance = config.provider_instances.get_mut("relay").unwrap();
        instance.model = None;
        instance.fast_model = None;
        instance.vision_model = None;
        assert!(validate_runtime_model_admission(&config).is_ok());
    }

    #[test]
    fn malformed_admission_is_never_treated_as_missing() {
        for value in [json!(null), json!("all"), json!([42]), json!([""])] {
            let mut config = config();
            config
                .provider_instances
                .get_mut("relay")
                .unwrap()
                .extra
                .insert(RUNTIME_MODELS_CONFIG_KEY.into(), value);
            assert!(provider_runtime_models(&config, "relay").is_err());
            assert!(validate_runtime_model_admission(&config).is_err());
        }
    }

    #[test]
    fn explicit_custom_admission_survives_save_and_cold_load() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = config();
        config.defaults = None;
        let instance = config.provider_instances.get_mut("relay").unwrap();
        instance.model = Some("custom/id".into());
        instance.fast_model = None;
        instance.vision_model = None;
        instance
            .extra
            .insert(RUNTIME_MODELS_CONFIG_KEY.into(), json!(["custom/id"]));
        config.default_provider_instance = Some("relay".into());
        config.save_to_dir(temp.path().to_path_buf()).unwrap();
        let loaded = Config::from_data_dir_without_publish(Some(temp.path().to_path_buf()));
        assert_eq!(
            provider_runtime_models(&loaded, "relay").unwrap(),
            ["custom/id"]
        );
        assert_eq!(
            explicit_runtime_models(&loaded.provider_instances["relay"]).unwrap(),
            Some(vec!["custom/id".into()])
        );
    }

    #[test]
    fn full_config_save_rejects_unadmitted_default_without_mutating_disk() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = config();
        config.save_to_dir(temp.path().to_path_buf()).unwrap();
        let before = std::fs::read(temp.path().join("config.json")).unwrap();
        config
            .provider_instances
            .get_mut("relay")
            .unwrap()
            .extra
            .insert(RUNTIME_MODELS_CONFIG_KEY.into(), json!([]));
        assert!(config.save_to_dir(temp.path().to_path_buf()).is_err());
        assert_eq!(
            std::fs::read(temp.path().join("config.json")).unwrap(),
            before
        );
    }
}
