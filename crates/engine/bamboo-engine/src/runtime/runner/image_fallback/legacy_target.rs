//! Target pairing for the independently opted-in conversation transform.
use std::{path::PathBuf, sync::Arc};

use bamboo_agent_core::AgentError;
use bamboo_config::Config;
use bamboo_domain::ProviderModelRef;
use bamboo_llm::{LLMProvider, ResolvedModel};

use super::vision_target::{configured_instance, validate_target};
use super::LegacyVisionModelResolver;

/// The legacy hook historically selects Vision -> Fast -> Chat. Retain that
/// separate contract while preserving the selected account. A caller's bare
/// model override belongs to its supplied active client; a matching configured
/// role retains its complete reference (including identical model IDs).
fn selected_target(
    config: &Config,
    active_provider: &str,
    requested_model: Option<&str>,
) -> Result<ProviderModelRef, AgentError> {
    let configured = config.defaults.as_ref().map(|defaults| {
        defaults
            .vision
            .as_ref()
            .or(defaults.fast.as_ref())
            .unwrap_or(&defaults.chat)
            .clone()
    });
    let requested = requested_model
        .map(str::trim)
        .filter(|model| !model.is_empty());
    if let Some(target) = configured.as_ref() {
        if requested.is_none_or(|model| model == target.model) {
            if target.provider != active_provider
                && config
                    .defaults
                    .as_ref()
                    .is_some_and(|defaults| defaults.vision.is_none())
            {
                return Err(AgentError::LLM(
                    "Legacy image conversion needs an explicit Vision role to use another account; Fast/Chat alone do not authorize that image destination. Original images were preserved and no image was sent.".into(),
                ));
            }
            return Ok(target.clone());
        }
    }
    let model = requested
        .map(str::to_string)
        .or_else(|| {
            configured_instance(config, active_provider).and_then(|instance| {
                instance
                    .vision_model
                    .or(instance.fast_model)
                    .or(instance.model)
            })
        })
        .ok_or_else(|| {
            AgentError::LLM(
                "Legacy Vision fallback requires a selected model; original images were preserved."
                    .into(),
            )
        })?;
    let mut target = ProviderModelRef::new(active_provider, model);
    target.reasoning_effort = config.reasoning_effort_for_key(active_provider);
    Ok(target)
}

/// This port belongs only to an explicit legacy ExecuteRequest. Its full target
/// is a request snapshot, as the old ImageFallbackConfig string was. Never use
/// the strict ViewImage port for automatic conversation/history conversion.
pub(crate) fn legacy_vision_resolver(
    config: Config,
    active_provider: String,
    active_client: Arc<dyn LLMProvider>,
    requested_model: Option<String>,
    app_data_dir: Option<PathBuf>,
) -> LegacyVisionModelResolver {
    Arc::new(move || {
        let config = config.clone();
        let active_provider = active_provider.clone();
        let active_client = active_client.clone();
        let requested_model = requested_model.clone();
        let app_data_dir = app_data_dir.clone();
        Box::pin(async move {
            let target = selected_target(&config, &active_provider, requested_model.as_deref())?;
            let instance = if target.provider == active_provider
                && configured_instance(&config, &target.provider).is_none()
            {
                // A hostless caller may supply its own client without managed
                // provider metadata. That is authority for this account only.
                None
            } else {
                Some(validate_target(&config, &target)?)
            };
            let provider = if target.provider == active_provider {
                active_client
            } else {
                bamboo_llm::provider_factory::create_provider_from_instance(
                    &config, &instance.expect("cross-account target requires exact configured metadata"),
                    app_data_dir.unwrap_or_else(bamboo_config::paths::bamboo_dir),
                ).await.map_err(|_| AgentError::LLM("Configured legacy Vision provider could not be initialized; original images were preserved and no image was sent.".into()))?
            };
            Ok(ResolvedModel::from_ref(provider, &target))
        })
    })
}
