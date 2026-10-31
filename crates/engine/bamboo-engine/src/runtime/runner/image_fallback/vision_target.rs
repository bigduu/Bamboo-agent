//! Resolve only explicit Vision configuration, without choosing another account.
use std::{path::PathBuf, sync::Arc};

use bamboo_agent_core::AgentError;
use bamboo_config::{Config, ProviderInstanceConfig};
use bamboo_domain::ProviderModelRef;
use bamboo_llm::ResolvedModel;
use tokio::sync::RwLock;

use super::{VisionModelResolver, VISION_CONFIGURATION_ERROR};

fn configuration_error() -> AgentError {
    AgentError::LLM(VISION_CONFIGURATION_ERROR.into())
}

fn rejected_target(reason: &str) -> AgentError {
    AgentError::LLM(format!("{reason} {VISION_CONFIGURATION_ERROR}"))
}

pub(super) fn configured_instance(
    config: &Config,
    provider: &str,
) -> Option<ProviderInstanceConfig> {
    config
        .provider_instances
        .get(provider)
        .cloned()
        .or_else(|| {
            bamboo_config::synthesize_legacy_instances(config)
                .into_iter()
                .find_map(|(id, instance)| (id == provider).then_some(instance))
        })
}

/// Never use the historical fast/chat fallback here: those are not explicit
/// permission to send local image bytes to another model or provider account.
fn configured_target(
    config: &Config,
    active_provider: &str,
) -> Result<(ProviderModelRef, ProviderInstanceConfig), AgentError> {
    // A saved Vision role explicitly authorizes this exact auxiliary target.
    // The primary-chat rollout flag must not hide that selection.
    let configured_role = config
        .defaults
        .as_ref()
        .and_then(|defaults| defaults.vision.clone());
    let target = if let Some(target) = configured_role {
        target
    } else {
        // The run may explicitly use another account than the process default.
        // Only its own vision_model authorizes this no-global-role fallback.
        let provider = active_provider;
        let instance = configured_instance(config, provider).ok_or_else(configuration_error)?;
        let model = instance
            .vision_model
            .as_deref()
            .map(str::trim)
            .filter(|model| !model.is_empty())
            .ok_or_else(configuration_error)?;
        let mut target = ProviderModelRef::new(provider, model);
        target.reasoning_effort = instance.reasoning_effort;
        target
    };
    let instance = validate_target(config, &target)?;
    Ok((target, instance))
}

/// Shared exact-target metadata checks. Selection and permission to use a
/// target remain with the independent strict tool or explicit legacy caller.
pub(super) fn validate_target(
    config: &Config,
    target: &ProviderModelRef,
) -> Result<ProviderInstanceConfig, AgentError> {
    // Use the exact configured instance identity. A provider-type alias must not
    // silently select the first account of that type when multiple exist.
    if target.provider.trim().is_empty()
        || target.model.trim().is_empty()
        || target.provider.trim() != target.provider
        || target.model.trim() != target.model
    {
        return Err(rejected_target("Configured Vision reference is invalid."));
    }
    let instance = configured_instance(config, &target.provider)
        .ok_or_else(|| rejected_target("Configured Vision provider is unavailable."))?;
    if !instance.enabled {
        return Err(rejected_target("Configured Vision provider is disabled."));
    }
    let admitted = bamboo_config::explicit_runtime_models(&instance)
        .map_err(|_| rejected_target("Configured Vision model catalog is invalid."))?;
    if admitted.is_some_and(|models| !models.contains(&target.model)) {
        return Err(rejected_target(
            "Configured Vision model is not admitted by its provider.",
        ));
    }
    let overrides = bamboo_config::model_vision_overrides(&instance)
        .map_err(|_| rejected_target("Configured Vision capability metadata is invalid."))?;
    if overrides.get(&target.model) == Some(&false) {
        return Err(AgentError::LLM(format!(
            "Configured Vision model does not support Vision. {VISION_CONFIGURATION_ERROR}"
        )));
    }
    Ok(instance)
}

/// Validate explicit configuration only; never construct/probe a provider here.
pub(crate) fn validate_configured_vision_target(
    config: &Config,
    active_provider: &str,
) -> Result<(), AgentError> {
    configured_target(config, active_provider).map(|_| ())
}

pub(crate) fn configured_vision_resolver(
    config: Arc<RwLock<Config>>,
    app_data_dir: Option<PathBuf>,
    active_provider: String,
) -> VisionModelResolver {
    Arc::new(move || {
        let config = config.clone();
        let app_data_dir = app_data_dir.clone();
        let active_provider = active_provider.clone();
        Box::pin(async move {
            let snapshot = config.read().await.clone();
            let (target, instance) = configured_target(&snapshot, &active_provider)?;
            // Reuse the common instance factory for native Anthropic, Gemini,
            // Copilot, Bodhi and OpenAI protocols and their credential handling.
            // Do not initialize or probe any other provider.
            let provider = bamboo_llm::provider_factory::create_provider_from_instance(
                &snapshot, &instance,
                app_data_dir.unwrap_or_else(bamboo_config::paths::bamboo_dir),
            ).await.map_err(|_| AgentError::LLM("Configured Vision provider could not be initialized. Check that provider's configuration and credentials. No image was sent.".into()))?;
            Ok(ResolvedModel::from_ref(provider, &target))
        })
    })
}

#[cfg(test)]
#[path = "vision_target_tests.rs"]
mod tests;
