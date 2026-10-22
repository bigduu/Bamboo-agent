use super::*;
use crate::{Config, ProviderInstanceConfig};

pub(super) fn configured_instances(config: &Config) -> BTreeMap<String, ProviderInstanceConfig> {
    // This helper only copies existing in-memory fields. It does not call the
    // similarly named env-override/hydration/materialization helpers.
    let mut instances: BTreeMap<_, _> = crate::synthesize_legacy_instances(config)
        .into_iter()
        .collect();
    instances.extend(config.provider_instances.clone());
    instances
}

pub(super) fn scenario_refs(config: &Config) -> BTreeMap<ShareScenario, ShareModelRef> {
    let Some(defaults) = &config.defaults else {
        return BTreeMap::new();
    };
    use ShareScenario::*;
    [
        (Chat, Some(&defaults.chat)),
        (Fast, defaults.fast.as_ref()),
        (TaskSummary, defaults.task_summary.as_ref()),
        (Vision, defaults.vision.as_ref()),
        (MemoryBackground, defaults.memory_background.as_ref()),
        (Planning, defaults.planning.as_ref()),
        (Search, defaults.search.as_ref()),
        (CodeReview, defaults.code_review.as_ref()),
        (SubAgent, defaults.sub_agent.as_ref()),
    ]
    .into_iter()
    .filter_map(|(scenario, reference)| reference.map(|r| (scenario, model_ref(config, r))))
    .collect()
}

fn model_ref(config: &Config, reference: &bamboo_domain::ProviderModelRef) -> ShareModelRef {
    ShareModelRef {
        provider: crate::configured_provider_routing_key(config, &reference.provider),
        model: reference.model.clone(),
        reasoning_effort: reference.reasoning_effort,
    }
}

/// Project only explicitly selected fields from a supplied, already-authorized
/// in-memory snapshot. This function never acquires or decrypts credentials.
pub fn select_bundle(config: &Config, selection: &ShareSelection) -> ShareResult<ShareBundle> {
    if selection.providers.len() > MAX_PROVIDERS
        || selection.env.len() > MAX_ENV_VARS
        || selection.subagent_models.len() > MAX_SUBAGENT_MODELS
    {
        return Err(ShareError::Limit);
    }
    if !selection.provider_api_keys.is_subset(&selection.providers)
        || !selection.provider_base_urls.is_subset(&selection.providers)
    {
        return Err(ShareError::Selection);
    }
    let instances = configured_instances(config);
    let mut bundle = ShareBundle::default();
    for id in &selection.providers {
        let instance = instances.get(id).ok_or(ShareError::Selection)?;
        let provider_type = match instance.provider_type.as_str() {
            "openai" => ShareProviderType::Openai,
            "anthropic" => ShareProviderType::Anthropic,
            "gemini" => ShareProviderType::Gemini,
            "copilot" => ShareProviderType::Copilot,
            "bodhi" => ShareProviderType::Bodhi,
            _ => return Err(ShareError::Unsupported),
        };
        let api_key = if selection.provider_api_keys.contains(id) {
            if instance.api_key.trim().is_empty() {
                return Err(ShareError::Selection);
            }
            Some(instance.api_key.clone())
        } else {
            None
        };
        let base_url = if selection.provider_base_urls.contains(id) {
            Some(instance.base_url.clone().ok_or(ShareError::Selection)?)
        } else {
            None
        };
        let runtime_models =
            crate::explicit_runtime_models(instance).map_err(|_| ShareError::Invalid)?;
        bundle.providers.push(ShareProvider {
            id: id.clone(),
            provider_type,
            label: instance.label.clone(),
            base_url,
            api_key,
            model: instance.model.clone(),
            fast_model: instance.fast_model.clone(),
            vision_model: instance.vision_model.clone(),
            reasoning_effort: instance.reasoning_effort,
            responses_only_models: instance.responses_only_models.clone(),
            runtime_models,
        });
    }
    let scenarios = scenario_refs(config);
    for scenario in &selection.scenarios {
        bundle.scenarios.insert(
            *scenario,
            scenarios
                .get(scenario)
                .ok_or(ShareError::Selection)?
                .clone(),
        );
    }
    for name in &selection.subagent_models {
        let reference = config
            .defaults
            .as_ref()
            .and_then(|d| d.subagent_models.get(name))
            .ok_or(ShareError::Selection)?;
        bundle
            .subagent_models
            .insert(name.clone(), model_ref(config, reference));
    }
    for (name, include_value) in &selection.env {
        let entries: Vec<_> = config
            .env_vars
            .iter()
            .filter(|entry| &entry.name == name)
            .collect();
        if entries.len() != 1 {
            return Err(ShareError::Selection);
        }
        let entry = entries[0];
        // Secret values must be hydrated by the explicitly authorized caller.
        // An unconfigured secret may not become an accidental empty replacement.
        if *include_value && entry.secret && (!entry.configured || entry.value.is_empty()) {
            return Err(ShareError::Selection);
        }
        bundle.env.push(ShareEnvVar {
            name: name.clone(),
            value: include_value.then(|| entry.value.clone()),
            secret: entry.secret,
        });
    }
    bundle.validate()?;
    Ok(bundle)
}
