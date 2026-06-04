// Helper function to extract default model from config
// This should be used instead of hardcoding "gpt-4o-mini" or "default"

use std::sync::Arc;

use bamboo_domain::reasoning::ReasoningEffort;
use bamboo_domain::subagent::ModelHint;
use crate::config::GoldConfig;
use bamboo_infrastructure::Config;
use bamboo_infrastructure::{LLMError, ProviderModelRouter, ProviderRegistry, ResolvedModel};

pub const GOLD_CONFIG_METADATA_KEY: &str = "gold_config";

/// Infer the underlying provider type from a bare model name.
///
/// This is the single, canonical place that maps a model-name string to a
/// provider type. Centralising it here (TD-3) keeps the scattered
/// `claude*`/`gpt*`/`gemini*` pattern matches from drifting apart.
///
/// Mapping:
/// - `claude*` → `anthropic`
/// - `gpt*` or an OpenAI o-series name (`o1`, `o3`, `o4-mini`, ...) → `openai`
/// - `gemini*` → `gemini`
/// - anything else → `None`
///
/// The match is case-insensitive and ignores surrounding whitespace.
pub fn infer_provider(model_name: &str) -> Option<String> {
    let name = model_name.trim().to_ascii_lowercase();
    if name.is_empty() {
        return None;
    }

    if name.starts_with("claude") {
        return Some("anthropic".to_string());
    }
    if name.starts_with("gpt") || is_openai_o_series(&name) {
        return Some("openai".to_string());
    }
    if name.starts_with("gemini") {
        return Some("gemini".to_string());
    }
    None
}

/// Whether a (already lower-cased) model name looks like an OpenAI o-series
/// reasoning model: an `o` immediately followed by a digit (`o1`, `o3-mini`,
/// `o4-mini`, ...).
fn is_openai_o_series(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some('o')) && matches!(chars.next(), Some(c) if c.is_ascii_digit())
}

/// Resolve a [`ModelHint`] into a concrete provider + model, honouring the
/// precedence **`model_ref` > `tier` > fallback chain**.
///
/// 1. `model_hint.model_ref` — an explicit pin. Accepts either a
///    `provider/model` pair or a bare model name (provider inferred via
///    [`infer_provider`], falling back to `provider_name`). Routed through the
///    registry when possible so the returned [`ResolvedModel`] carries a live
///    provider handle.
/// 2. `model_hint.tier` — a named tier (`fast`, `chat`, `sub_agent`,
///    `background`/`memory_background`, `task_summary`, `vision`, `planning`,
///    `search`, `code_review`). Resolved via the existing tier resolvers.
/// 3. Fallback chain — `subagent_models[subagent_type]` → `sub_agent` →
///    `fast` → `chat`, via [`resolve_subagent_model`]. `subagent_type` is the
///    profile id (e.g. `"researcher"`); pass `""` when not resolving for a
///    specific subagent.
///
/// Returns `None` when nothing in the chain resolves.
pub fn resolve_model(
    model_hint: &ModelHint,
    subagent_type: &str,
    provider_name: &str,
    config: &Config,
    provider_registry: &Arc<ProviderRegistry>,
) -> Option<ResolvedModel> {
    // 1. Explicit model_ref wins.
    if let Some(model_ref) = model_hint
        .model_ref
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        let (provider, model) = match model_ref.split_once('/') {
            Some((p, m)) if !p.trim().is_empty() && !m.trim().is_empty() => {
                (p.trim().to_string(), m.trim().to_string())
            }
            _ => {
                let inferred =
                    infer_provider(model_ref).unwrap_or_else(|| provider_name.to_string());
                (inferred, model_ref.to_string())
            }
        };

        let pmr = bamboo_domain::ProviderModelRef::new(provider.clone(), model.clone());
        let resolved_provider = ProviderModelRouter::new(provider_registry.clone())
            .route(&pmr)
            .ok()
            .or_else(|| provider_registry.get(&provider));
        if let Some(resolved_provider) = resolved_provider {
            return Some(ResolvedModel {
                provider: resolved_provider,
                model_name: model,
            });
        }
        // Fall through to tier/fallback if the pinned provider is unavailable.
    }

    // 2. Named tier.
    if let Some(tier) = model_hint
        .tier
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        if let Some(resolved) =
            resolve_tier_model(tier, subagent_type, config, provider_name, provider_registry)
        {
            return Some(resolved);
        }
    }

    // 3. Fallback chain (subagent_models[subagent_type] → sub_agent → fast → chat).
    resolve_subagent_model(config, provider_name, provider_registry, subagent_type)
}

/// Resolve a named tier to a concrete model via the existing tier resolvers.
fn resolve_tier_model(
    tier: &str,
    subagent_type: &str,
    config: &Config,
    provider_name: &str,
    provider_registry: &Arc<ProviderRegistry>,
) -> Option<ResolvedModel> {
    match tier.to_ascii_lowercase().as_str() {
        "fast" => resolve_fast_model(config, provider_name, provider_registry),
        "sub_agent" | "subagent" => {
            resolve_subagent_model(config, provider_name, provider_registry, subagent_type)
        }
        "background" | "memory_background" => {
            resolve_background_model(config, provider_name, provider_registry)
        }
        "task_summary" => resolve_task_summary_model(config, provider_name, provider_registry),
        "vision" => resolve_vision_model(config, provider_name, provider_registry),
        "planning" => resolve_planning_model(config, provider_name, provider_registry),
        "search" => resolve_search_model(config, provider_name, provider_registry),
        "code_review" => resolve_code_review_model(config, provider_name, provider_registry),
        // `chat` and any unknown tier fall back to the default chat model.
        _ => resolve_default_chat_model(config, provider_name, provider_registry),
    }
}

/// Resolve the underlying provider type for a provider routing key.
///
/// In legacy mode the routing key is already the provider type (for example
/// `"openai"` or `"copilot"`). In multi-instance mode the routing key may be an
/// instance id (for example `"copilot-work"`), so we first consult configured
/// provider instances, then the live registry metadata, and finally fall back to
/// the key itself for backward compatibility.
pub fn resolve_provider_type(
    config: &Config,
    provider_name: &str,
    provider_registry: &Arc<ProviderRegistry>,
) -> Option<String> {
    let trimmed = provider_name.trim();
    if trimmed.is_empty() {
        return None;
    }

    config
        .provider_instances
        .get(trimmed)
        .map(|instance| instance.provider_type.clone())
        .or_else(|| {
            provider_registry
                .get_metadata(trimmed)
                .map(|meta| meta.provider_type)
        })
        .or_else(|| Some(trimmed.to_string()))
}

pub fn parse_session_gold_config(session_gold_config_json: Option<&str>) -> Option<GoldConfig> {
    let raw = session_gold_config_json?.trim();
    if raw.is_empty() {
        return None;
    }
    serde_json::from_str::<GoldConfig>(raw).ok()
}

pub fn normalize_gold_config_json(value: &serde_json::Value) -> Result<String, serde_json::Error> {
    let parsed = serde_json::from_value::<GoldConfig>(value.clone())?;
    serde_json::to_string(&parsed)
}

pub fn resolve_global_gold_config(config: &Config) -> Option<GoldConfig> {
    config
        .extra
        .get("gold")
        .cloned()
        .and_then(|value| serde_json::from_value::<GoldConfig>(value).ok())
}

pub fn resolve_gold_config(
    config: &Config,
    session_gold_config_json: Option<&str>,
) -> Option<GoldConfig> {
    if session_gold_config_json.is_some() {
        return parse_session_gold_config(session_gold_config_json);
    }
    resolve_global_gold_config(config)
}

/// Get the default model for a specific provider from config.
pub fn get_default_model_for_provider(
    config: &Config,
    provider_name: &str,
) -> Result<String, LLMError> {
    match provider_name.trim() {
        "copilot" => {
            let provider_model = config
                .providers
                .copilot
                .as_ref()
                .and_then(|c| c.model.clone());

            Ok(provider_model.unwrap_or_else(|| "gpt-4o".to_string()))
        }
        "openai" => {
            let openai_config = config
                .providers
                .openai
                .as_ref()
                .ok_or_else(|| LLMError::Auth("OpenAI configuration required".to_string()))?;

            openai_config.model.clone().ok_or_else(|| {
                LLMError::Auth("OpenAI model must be specified in config".to_string())
            })
        }
        "anthropic" => {
            let anthropic_config =
                config.providers.anthropic.as_ref().ok_or_else(|| {
                    LLMError::Auth("Anthropic configuration required".to_string())
                })?;

            anthropic_config.model.clone().ok_or_else(|| {
                LLMError::Auth("Anthropic model must be specified in config".to_string())
            })
        }
        "gemini" => {
            let gemini_config = config
                .providers
                .gemini
                .as_ref()
                .ok_or_else(|| LLMError::Auth("Gemini configuration required".to_string()))?;

            gemini_config.model.clone().ok_or_else(|| {
                LLMError::Auth("Gemini model must be specified in config".to_string())
            })
        }
        other => Err(LLMError::Auth(format!("Unknown provider: {}", other))),
    }
}

/// Get the default model for the current provider from config.
/// Returns an error if no model is configured.
pub fn get_default_model_from_config(config: &Config) -> Result<String, LLMError> {
    get_default_model_for_provider(config, config.provider.as_str())
}

/// Get the schedule auto-execute model for the current provider from config.
///
/// Falls back from the provider fast model to the default chat model when no
/// dedicated fast model is configured.
pub fn get_schedule_model_from_config(config: &Config) -> Result<String, LLMError> {
    config
        .get_fast_model()
        .map(|model| model.trim().to_string())
        .filter(|model| !model.is_empty())
        .ok_or_else(|| {
            LLMError::Auth(format!(
                "No fast/default model configured for provider '{}'",
                config.provider
            ))
        })
}

/// Get the fast/cheap model for a specific provider from config.
pub fn get_fast_model_for_provider(config: &Config, provider_name: &str) -> Option<String> {
    let fast = match provider_name.trim() {
        "openai" => config
            .providers
            .openai
            .as_ref()
            .and_then(|c| c.fast_model.clone()),
        "anthropic" => config
            .providers
            .anthropic
            .as_ref()
            .and_then(|c| c.fast_model.clone()),
        "gemini" => config
            .providers
            .gemini
            .as_ref()
            .and_then(|c| c.fast_model.clone()),
        "copilot" => config
            .providers
            .copilot
            .as_ref()
            .and_then(|c| c.fast_model.clone()),
        _ => None,
    };

    fast.or_else(|| get_default_model_for_provider(config, provider_name).ok())
}

/// Get the memory/background model for a specific provider from config.
///
/// This uses provider-local fast model fallback and intentionally avoids coupling
/// to the globally active provider.
pub fn get_memory_background_model_for_provider(
    config: &Config,
    provider_name: &str,
) -> Option<String> {
    let configured = config
        .memory
        .as_ref()
        .and_then(|memory| memory.background_model.as_ref())
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);

    configured.or_else(|| get_fast_model_for_provider(config, provider_name))
}

/// Get the default reasoning effort for a specific provider routing key.
///
/// `provider_name` is the model_ref's provider, which in multi-instance mode is
/// an instance id (for example `"copilot-work"`). Delegates to
/// [`Config::reasoning_effort_for_key`] so instance ids, the `bodhi` provider,
/// and legacy provider types all resolve consistently with the session-create
/// path.
pub fn get_reasoning_effort_for_provider(
    config: &Config,
    provider_name: &str,
) -> Option<ReasoningEffort> {
    config.reasoning_effort_for_key(provider_name)
}

/// Get the task summarization model for the current provider from config.
///
/// Used for conversation/task summarization and context compression.
pub fn get_task_summary_model_from_config(config: &Config) -> Result<String, LLMError> {
    config.get_task_summary_model().ok_or_else(|| {
        LLMError::Auth(format!(
            "No task summary model configured for provider '{}'",
            config.provider
        ))
    })
}

/// Get the memory/background summarization model for the current provider from config.
///
/// Used for lightweight memory tasks like session summarization and reflection.
/// Falls back to the provider fast model when `memory.background_model` is not configured.
pub fn get_memory_background_model_from_config(config: &Config) -> Result<String, LLMError> {
    config.get_memory_background_model().ok_or_else(|| {
        LLMError::Auth(format!(
            "No background memory model configured for provider '{}'",
            config.provider
        ))
    })
}

/// Get the vision-capable model for the current provider from config.
///
/// Used for image understanding tasks.
/// Falls back to the default model when no vision_model is configured.
pub fn get_vision_model_from_config(config: &Config) -> Result<String, LLMError> {
    config.get_vision_model().ok_or_else(|| {
        LLMError::Auth(format!(
            "No model configured for provider '{}'",
            config.provider
        ))
    })
}

/// Resolve the task summarization model for conversation/task compression.
///
/// Fallback chain:
/// 1. `defaults.task_summary` (ProviderModelRef, routed via registry)
/// 2. `defaults.memory_background` / `defaults.fast` / legacy memory background
/// 3. `defaults.chat` / legacy default model
pub fn resolve_task_summary_model(
    config: &Config,
    provider_name: &str,
    provider_registry: &Arc<ProviderRegistry>,
) -> Option<ResolvedModel> {
    if config.features.provider_model_ref {
        if let Some(model_ref) = config
            .defaults
            .as_ref()
            .and_then(|d| d.task_summary.as_ref())
        {
            if let Ok(provider) =
                ProviderModelRouter::new(provider_registry.clone()).route(model_ref)
            {
                return Some(ResolvedModel {
                    provider,
                    model_name: model_ref.model.clone(),
                });
            }
        }
    }

    resolve_background_model(config, provider_name, provider_registry)
        .or_else(|| resolve_default_chat_model(config, provider_name, provider_registry))
}

/// Resolve the background/fast summarization model considering both
/// `DefaultsConfig` (ProviderModelRef) and legacy provider config paths.
///
/// Resolution order:
/// 1. `defaults.memory_background` (ProviderModelRef, routed via registry)
/// 2. `defaults.fast` (ProviderModelRef, routed via registry)
/// 3. Legacy: `memory.background_model` / provider `fast_model` string + registry lookup
pub fn resolve_background_model(
    config: &Config,
    provider_name: &str,
    provider_registry: &Arc<ProviderRegistry>,
) -> Option<ResolvedModel> {
    if config.features.provider_model_ref {
        if let Some(model_ref) = config
            .defaults
            .as_ref()
            .and_then(|d| d.memory_background.as_ref())
            .or_else(|| config.defaults.as_ref().and_then(|d| d.fast.as_ref()))
        {
            if let Ok(provider) =
                ProviderModelRouter::new(provider_registry.clone()).route(model_ref)
            {
                return Some(ResolvedModel {
                    provider,
                    model_name: model_ref.model.clone(),
                });
            }
        }
    }
    let model_name = get_memory_background_model_for_provider(config, provider_name)?;
    let provider = provider_registry.get(provider_name)?;
    Some(ResolvedModel {
        provider,
        model_name,
    })
}

/// Resolve the fast model for lightweight tasks like title generation.
pub fn resolve_fast_model(
    config: &Config,
    provider_name: &str,
    provider_registry: &Arc<ProviderRegistry>,
) -> Option<ResolvedModel> {
    if config.features.provider_model_ref {
        if let Some(model_ref) = config.defaults.as_ref().and_then(|d| d.fast.as_ref()) {
            if let Ok(provider) =
                ProviderModelRouter::new(provider_registry.clone()).route(model_ref)
            {
                return Some(ResolvedModel {
                    provider,
                    model_name: model_ref.model.clone(),
                });
            }
        }
    }
    let model_name = get_fast_model_for_provider(config, provider_name)?;
    let provider = provider_registry.get(provider_name)?;
    Some(ResolvedModel {
        provider,
        model_name,
    })
}

/// Resolve the vision-capable model for image understanding.
pub fn resolve_vision_model(
    config: &Config,
    provider_name: &str,
    provider_registry: &Arc<ProviderRegistry>,
) -> Option<ResolvedModel> {
    if config.features.provider_model_ref {
        if let Some(model_ref) = config.defaults.as_ref().and_then(|d| d.vision.as_ref()) {
            if let Ok(provider) =
                ProviderModelRouter::new(provider_registry.clone()).route(model_ref)
            {
                return Some(ResolvedModel {
                    provider,
                    model_name: model_ref.model.clone(),
                });
            }
        }
    }
    let model_name = config.get_vision_model()?;
    let provider = provider_registry.get(provider_name)?;
    Some(ResolvedModel {
        provider,
        model_name,
    })
}

/// Resolve the planning/coordination model for architecture and task decomposition.
///
/// Fallback chain: `defaults.planning` → `defaults.chat`.
pub fn resolve_planning_model(
    config: &Config,
    provider_name: &str,
    provider_registry: &Arc<ProviderRegistry>,
) -> Option<ResolvedModel> {
    if config.features.provider_model_ref {
        if let Some(model_ref) = config.defaults.as_ref().and_then(|d| d.planning.as_ref()) {
            if let Ok(provider) =
                ProviderModelRouter::new(provider_registry.clone()).route(model_ref)
            {
                return Some(ResolvedModel {
                    provider,
                    model_name: model_ref.model.clone(),
                });
            }
        }
    }
    resolve_default_chat_model(config, provider_name, provider_registry)
}

/// Resolve the search/navigation model for grep, file listing, and symbol resolution.
///
/// Fallback chain: `defaults.search` → `defaults.fast` → legacy fast model → default chat model.
pub fn resolve_search_model(
    config: &Config,
    provider_name: &str,
    provider_registry: &Arc<ProviderRegistry>,
) -> Option<ResolvedModel> {
    if config.features.provider_model_ref {
        if let Some(model_ref) = config.defaults.as_ref().and_then(|d| d.search.as_ref()) {
            if let Ok(provider) =
                ProviderModelRouter::new(provider_registry.clone()).route(model_ref)
            {
                return Some(ResolvedModel {
                    provider,
                    model_name: model_ref.model.clone(),
                });
            }
        }
    }
    resolve_fast_model(config, provider_name, provider_registry)
        .or_else(|| resolve_default_chat_model(config, provider_name, provider_registry))
}

/// Resolve the code review model for PR and code analysis tasks.
///
/// Fallback chain: `defaults.code_review` → `defaults.chat`.
pub fn resolve_code_review_model(
    config: &Config,
    provider_name: &str,
    provider_registry: &Arc<ProviderRegistry>,
) -> Option<ResolvedModel> {
    if config.features.provider_model_ref {
        if let Some(model_ref) = config
            .defaults
            .as_ref()
            .and_then(|d| d.code_review.as_ref())
        {
            if let Ok(provider) =
                ProviderModelRouter::new(provider_registry.clone()).route(model_ref)
            {
                return Some(ResolvedModel {
                    provider,
                    model_name: model_ref.model.clone(),
                });
            }
        }
    }
    resolve_default_chat_model(config, provider_name, provider_registry)
}

/// Resolve the provider+model reference for a specific subagent type.
///
/// Fallback chain: `defaults.subagent_models[type]` → `defaults.sub_agent` →
/// `defaults.fast`/legacy fast → `defaults.chat`/legacy default.
pub fn resolve_subagent_model_ref(
    config: &Config,
    provider_name: &str,
    provider_registry: &Arc<ProviderRegistry>,
    subagent_type: &str,
) -> Option<bamboo_domain::ProviderModelRef> {
    if config.features.provider_model_ref {
        let router = ProviderModelRouter::new(provider_registry.clone());
        if let Some(defaults) = config.defaults.as_ref() {
            let candidate_refs = [
                defaults.subagent_models.get(subagent_type),
                defaults.sub_agent.as_ref(),
                defaults.fast.as_ref(),
            ];

            for model_ref in candidate_refs.into_iter().flatten() {
                if router.route(model_ref).is_ok() {
                    return Some(model_ref.clone());
                }
            }
        }
    }

    resolve_fast_model(config, provider_name, provider_registry)
        .or_else(|| resolve_default_chat_model(config, provider_name, provider_registry))
        .map(|resolved| bamboo_domain::ProviderModelRef::new(provider_name, resolved.model_name))
}

/// Resolve the model for a specific subagent type.
///
/// Fallback chain: `defaults.subagent_models[type]` → `defaults.sub_agent` →
/// `defaults.fast`/legacy fast → `defaults.chat`/legacy default.
pub fn resolve_subagent_model(
    config: &Config,
    provider_name: &str,
    provider_registry: &Arc<ProviderRegistry>,
    subagent_type: &str,
) -> Option<ResolvedModel> {
    let model_ref =
        resolve_subagent_model_ref(config, provider_name, provider_registry, subagent_type)?;
    let provider = ProviderModelRouter::new(provider_registry.clone())
        .route(&model_ref)
        .or_else(|_| {
            provider_registry.get(&model_ref.provider).ok_or_else(|| {
                LLMError::Auth(format!("Provider '{}' not available", model_ref.provider))
            })
        })
        .ok()?;
    Some(ResolvedModel {
        provider,
        model_name: model_ref.model,
    })
}

/// Resolve the default chat model from config.
///
/// This is the terminal fallback for capability-specific model resolvers.
fn resolve_default_chat_model(
    config: &Config,
    provider_name: &str,
    provider_registry: &Arc<ProviderRegistry>,
) -> Option<ResolvedModel> {
    if config.features.provider_model_ref {
        if let Some(model_ref) = config.defaults.as_ref().map(|d| &d.chat) {
            if let Ok(provider) =
                ProviderModelRouter::new(provider_registry.clone()).route(model_ref)
            {
                return Some(ResolvedModel {
                    provider,
                    model_name: model_ref.model.clone(),
                });
            }
        }
    }
    let model_name = get_default_model_for_provider(config, provider_name).ok()?;
    let provider = provider_registry.get(provider_name)?;
    Some(ResolvedModel {
        provider,
        model_name,
    })
}

/// Resolve the image-fallback configuration from a config snapshot.
///
/// Returns `Ok(None)` when image fallback is disabled, `Ok(Some(..))` with the
/// resolved mode (and vision model, for `Vision` mode), or `Err` on an invalid
/// `hooks.image_fallback.mode` value.
pub fn resolve_image_fallback(
    config_snapshot: &Config,
) -> Result<Option<crate::ImageFallbackConfig>, String> {
    use crate::ImageFallbackMode;

    if !config_snapshot.hooks.image_fallback.enabled {
        return Ok(None);
    }

    let mode_str = config_snapshot
        .hooks
        .image_fallback
        .mode
        .trim()
        .to_ascii_lowercase();

    let mode = match mode_str.as_str() {
        "placeholder" => ImageFallbackMode::Placeholder,
        "error" => ImageFallbackMode::Error,
        "ocr" => ImageFallbackMode::Ocr,
        "vision" => ImageFallbackMode::Vision,
        other => {
            return Err(format!(
                "Invalid config: hooks.image_fallback.mode must be 'placeholder', 'error', 'ocr', or 'vision' (got '{other}')"
            ));
        }
    };

    let vision_model = if mode == ImageFallbackMode::Vision {
        config_snapshot.get_vision_model()
    } else {
        None
    };

    Ok(Some(crate::ImageFallbackConfig { mode, vision_model }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::tools::ToolSchema;
    use bamboo_agent_core::Message;
    use bamboo_domain::ProviderModelRef;
    use bamboo_infrastructure::{
        CopilotConfig, DefaultsConfig, LLMProvider, LLMStream, OpenAIConfig, ProviderConfigs,
    };
    use std::collections::HashMap;

    struct NoopProvider;

    #[async_trait::async_trait]
    impl LLMProvider for NoopProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            Err(LLMError::Api("noop".to_string()))
        }
    }

    fn test_registry() -> Arc<ProviderRegistry> {
        let mut providers: HashMap<String, Arc<dyn LLMProvider>> = HashMap::new();
        providers.insert("openai".to_string(), Arc::new(NoopProvider));
        Arc::new(ProviderRegistry::new(providers, "openai".to_string()))
    }

    #[test]
    fn test_get_model_from_openai_config() {
        let config = Config {
            provider: "openai".to_string(),
            providers: ProviderConfigs {
                openai: Some(OpenAIConfig {
                    api_key: "test".to_string(),
                    api_key_encrypted: None,
                    base_url: None,
                    model: Some("gpt-4o".to_string()),
                    fast_model: None,
                    vision_model: None,
                    reasoning_effort: None,
                    responses_only_models: vec![],
                    request_overrides: None,
                    extra: Default::default(),
                }),
                ..ProviderConfigs::default()
            },
            ..Config::default()
        };

        let result = get_default_model_from_config(&config);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "gpt-4o");
    }

    #[test]
    fn test_error_when_model_not_configured() {
        let config = Config {
            provider: "openai".to_string(),
            providers: ProviderConfigs {
                openai: Some(OpenAIConfig {
                    api_key: "test".to_string(),
                    api_key_encrypted: None,
                    base_url: None,
                    model: None, // No model configured
                    fast_model: None,
                    vision_model: None,
                    reasoning_effort: None,
                    responses_only_models: vec![],
                    request_overrides: None,
                    extra: Default::default(),
                }),
                ..ProviderConfigs::default()
            },
            ..Config::default()
        };

        let result = get_default_model_from_config(&config);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("model must be specified"));
    }

    #[test]
    fn test_get_model_from_copilot_provider_config() {
        let config = Config {
            provider: "copilot".to_string(),
            providers: ProviderConfigs {
                copilot: Some(CopilotConfig {
                    enabled: true,
                    headless_auth: false,
                    model: Some("gpt-4o-mini".to_string()),
                    fast_model: None,
                    vision_model: None,
                    reasoning_effort: None,
                    responses_only_models: vec![],
                    request_overrides: None,
                    extra: Default::default(),
                }),
                ..ProviderConfigs::default()
            },
            ..Config::default()
        };

        let result = get_default_model_from_config(&config);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "gpt-4o-mini");
    }

    #[test]
    fn test_get_model_from_copilot_default_fallback() {
        let config = Config {
            provider: "copilot".to_string(),
            providers: ProviderConfigs::default(),
            ..Config::default()
        };

        let result = get_default_model_from_config(&config);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "gpt-4o");
    }

    #[test]
    fn test_get_default_model_for_specific_provider() {
        let config = Config {
            provider: "anthropic".to_string(),
            providers: ProviderConfigs {
                openai: Some(OpenAIConfig {
                    api_key: "test".to_string(),
                    api_key_encrypted: None,
                    base_url: None,
                    model: Some("gpt-4o".to_string()),
                    fast_model: Some("gpt-4o-mini".to_string()),
                    vision_model: None,
                    reasoning_effort: Some(ReasoningEffort::Medium),
                    responses_only_models: vec![],
                    request_overrides: None,
                    extra: Default::default(),
                }),
                ..ProviderConfigs::default()
            },
            ..Config::default()
        };

        let result = get_default_model_for_provider(&config, "openai").expect("openai config");
        assert_eq!(result, "gpt-4o");
    }

    #[test]
    fn test_get_fast_model_for_specific_provider() {
        let config = Config {
            provider: "anthropic".to_string(),
            providers: ProviderConfigs {
                openai: Some(OpenAIConfig {
                    api_key: "test".to_string(),
                    api_key_encrypted: None,
                    base_url: None,
                    model: Some("gpt-4o".to_string()),
                    fast_model: Some("gpt-4o-mini".to_string()),
                    vision_model: None,
                    reasoning_effort: Some(ReasoningEffort::Medium),
                    responses_only_models: vec![],
                    request_overrides: None,
                    extra: Default::default(),
                }),
                ..ProviderConfigs::default()
            },
            ..Config::default()
        };

        assert_eq!(
            get_fast_model_for_provider(&config, "openai").as_deref(),
            Some("gpt-4o-mini")
        );
    }

    #[test]
    fn test_get_schedule_model_from_config_prefers_fast_model() {
        let config = Config {
            provider: "openai".to_string(),
            defaults: None,
            features: bamboo_infrastructure::FeatureFlags {
                provider_model_ref: false,
                ..Default::default()
            },
            providers: ProviderConfigs {
                openai: Some(OpenAIConfig {
                    api_key: "test".to_string(),
                    api_key_encrypted: None,
                    base_url: None,
                    model: Some("gpt-4o".to_string()),
                    fast_model: Some("gpt-4o-mini".to_string()),
                    vision_model: None,
                    reasoning_effort: None,
                    responses_only_models: vec![],
                    request_overrides: None,
                    extra: Default::default(),
                }),
                ..ProviderConfigs::default()
            },
            ..Config::default()
        };

        let result = get_schedule_model_from_config(&config);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "gpt-4o-mini");
    }

    #[test]
    fn test_get_schedule_model_from_config_falls_back_to_default_model() {
        let config = Config {
            provider: "openai".to_string(),
            defaults: None,
            features: bamboo_infrastructure::FeatureFlags {
                provider_model_ref: false,
                ..Default::default()
            },
            providers: ProviderConfigs {
                openai: Some(OpenAIConfig {
                    api_key: "test".to_string(),
                    api_key_encrypted: None,
                    base_url: None,
                    model: Some("gpt-4o".to_string()),
                    fast_model: None,
                    vision_model: None,
                    reasoning_effort: None,
                    responses_only_models: vec![],
                    request_overrides: None,
                    extra: Default::default(),
                }),
                ..ProviderConfigs::default()
            },
            ..Config::default()
        };

        let result = get_schedule_model_from_config(&config);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "gpt-4o");
    }

    #[test]
    fn test_get_schedule_model_from_config_prefers_defaults_fast_over_chat() {
        let config = Config {
            provider: "openai".to_string(),
            features: bamboo_infrastructure::FeatureFlags {
                provider_model_ref: true,
                ..Default::default()
            },
            defaults: Some(DefaultsConfig {
            chat: ProviderModelRef::new("openai", "gpt-chat"),
            fast: Some(ProviderModelRef::new("openai", "gpt-fast")),
            task_summary: None,
            vision: None,
            memory_background: None,
            planning: None,
            search: None,
            code_review: None,
            sub_agent: None,
            subagent_models: HashMap::new(),
        }),
            ..Default::default()
        };

        let result = get_schedule_model_from_config(&config);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "gpt-fast");
    }

    #[test]
    fn test_get_reasoning_effort_for_specific_provider() {
        let config = Config {
            provider: "anthropic".to_string(),
            providers: ProviderConfigs {
                openai: Some(OpenAIConfig {
                    api_key: "test".to_string(),
                    api_key_encrypted: None,
                    base_url: None,
                    model: Some("gpt-4o".to_string()),
                    fast_model: Some("gpt-4o-mini".to_string()),
                    vision_model: None,
                    reasoning_effort: Some(ReasoningEffort::Medium),
                    responses_only_models: vec![],
                    request_overrides: None,
                    extra: Default::default(),
                }),
                ..ProviderConfigs::default()
            },
            ..Config::default()
        };

        assert_eq!(
            get_reasoning_effort_for_provider(&config, "openai"),
            Some(ReasoningEffort::Medium)
        );
    }
    #[test]
    fn resolve_subagent_model_ref_prefers_sub_agent_over_fast() {
        let config = Config {
            provider: "openai".to_string(),
            features: bamboo_infrastructure::FeatureFlags {
                provider_model_ref: true,
                ..Default::default()
            },
            defaults: Some(DefaultsConfig {
            chat: ProviderModelRef::new("openai", "gpt-chat"),
            fast: Some(ProviderModelRef::new("openai", "gpt-fast")),
            task_summary: None,
            vision: None,
            memory_background: None,
            planning: None,
            search: None,
            code_review: None,
            sub_agent: Some(ProviderModelRef::new("openai", "gpt-sub-agent")),
            subagent_models: HashMap::new(),
        }),
            ..Default::default()
        };

        let resolved = resolve_subagent_model_ref(&config, "openai", &test_registry(), "coder")
            .expect("sub-agent model should resolve");

        assert_eq!(resolved, ProviderModelRef::new("openai", "gpt-sub-agent"));
    }

    #[test]
    fn resolve_subagent_model_ref_falls_back_to_fast_when_sub_agent_unset() {
        let config = Config {
            provider: "openai".to_string(),
            features: bamboo_infrastructure::FeatureFlags {
                provider_model_ref: true,
                ..Default::default()
            },
            defaults: Some(DefaultsConfig {
            chat: ProviderModelRef::new("openai", "gpt-chat"),
            fast: Some(ProviderModelRef::new("openai", "gpt-fast")),
            task_summary: None,
            vision: None,
            memory_background: None,
            planning: None,
            search: None,
            code_review: None,
            sub_agent: None,
            subagent_models: HashMap::new(),
        }),
            ..Default::default()
        };

        let resolved = resolve_subagent_model_ref(&config, "openai", &test_registry(), "coder")
            .expect("fast model should resolve");

        assert_eq!(resolved, ProviderModelRef::new("openai", "gpt-fast"));
    }

    // ---- S-T1.1: infer_provider mapping ----

    #[test]
    fn infer_provider_maps_known_families() {
        // claude* → anthropic
        assert_eq!(infer_provider("claude-3-7-sonnet").as_deref(), Some("anthropic"));
        assert_eq!(infer_provider("Claude-Opus").as_deref(), Some("anthropic"));
        // gpt* → openai
        assert_eq!(infer_provider("gpt-4o").as_deref(), Some("openai"));
        assert_eq!(infer_provider("GPT-4o-mini").as_deref(), Some("openai"));
        // o-series → openai
        assert_eq!(infer_provider("o1").as_deref(), Some("openai"));
        assert_eq!(infer_provider("o3-mini").as_deref(), Some("openai"));
        assert_eq!(infer_provider("o4-mini").as_deref(), Some("openai"));
        // gemini* → gemini
        assert_eq!(infer_provider("gemini-1.5-pro").as_deref(), Some("gemini"));
        // unknown / empty → None
        assert_eq!(infer_provider("llama-3"), None);
        assert_eq!(infer_provider("opus"), None); // no o-digit, not claude*
        assert_eq!(infer_provider("  "), None);
        assert_eq!(infer_provider(""), None);
    }

    // ---- S-T1.2: resolve_model precedence (model_ref > tier > fallback) ----

    fn precedence_config() -> Config {
        Config {
            provider: "openai".to_string(),
            features: bamboo_infrastructure::FeatureFlags {
                provider_model_ref: true,
                ..Default::default()
            },
            defaults: Some(DefaultsConfig {
                chat: ProviderModelRef::new("openai", "gpt-chat"),
                fast: Some(ProviderModelRef::new("openai", "gpt-fast")),
                task_summary: None,
                vision: None,
                memory_background: None,
                planning: None,
                search: None,
                code_review: None,
                sub_agent: Some(ProviderModelRef::new("openai", "gpt-sub-agent")),
                subagent_models: HashMap::new(),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn resolve_model_prefers_model_ref_over_tier_and_fallback() {
        let config = precedence_config();
        let hint = ModelHint {
            tier: Some("fast".to_string()),
            model_ref: Some("openai/gpt-explicit".to_string()),
        };
        let resolved = resolve_model(&hint, "", "openai", &config, &test_registry())
            .expect("model_ref should resolve");
        assert_eq!(resolved.model_name, "gpt-explicit");
    }

    #[test]
    fn resolve_model_uses_tier_when_no_model_ref() {
        let config = precedence_config();
        let hint = ModelHint {
            tier: Some("fast".to_string()),
            model_ref: None,
        };
        let resolved = resolve_model(&hint, "", "openai", &config, &test_registry())
            .expect("tier should resolve");
        assert_eq!(resolved.model_name, "gpt-fast");
    }

    #[test]
    fn resolve_model_falls_back_when_hint_empty() {
        let config = precedence_config();
        let hint = ModelHint::default();
        let resolved = resolve_model(&hint, "", "openai", &config, &test_registry())
            .expect("fallback chain should resolve");
        // Fallback chain: subagent_models[type] → sub_agent → fast → chat.
        // sub_agent is set, so it wins.
        assert_eq!(resolved.model_name, "gpt-sub-agent");
    }

    #[test]
    fn resolve_model_infers_provider_for_bare_model_ref() {
        let config = precedence_config();
        // Bare model name (no provider prefix); provider inferred as openai.
        let hint = ModelHint {
            tier: None,
            model_ref: Some("gpt-bare".to_string()),
        };
        let resolved = resolve_model(&hint, "", "openai", &config, &test_registry())
            .expect("bare model_ref should resolve via inferred provider");
        assert_eq!(resolved.model_name, "gpt-bare");
    }

    #[test]
    fn resolve_model_honors_per_subagent_override_keyed_by_type() {
        // Regression for the subagent_type-vs-provider_name mixup: the fallback
        // chain must look up `subagent_models` by the subagent TYPE, not by the
        // provider name.
        let mut config = precedence_config();
        config
            .defaults
            .as_mut()
            .unwrap()
            .subagent_models
            .insert(
                "researcher".to_string(),
                ProviderModelRef::new("openai", "gpt-researcher"),
            );
        let hint = ModelHint::default();

        // Matching subagent_type → per-subagent override wins over sub_agent.
        let resolved = resolve_model(&hint, "researcher", "openai", &config, &test_registry())
            .expect("subagent override should resolve");
        assert_eq!(resolved.model_name, "gpt-researcher");

        // Non-matching type → falls through to sub_agent (proves the key is the
        // type, not the provider name "openai").
        let other = resolve_model(&hint, "nonexistent", "openai", &config, &test_registry())
            .expect("fallback should resolve");
        assert_eq!(other.model_name, "gpt-sub-agent");
    }
}
