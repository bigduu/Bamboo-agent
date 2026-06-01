//! Single, explicit boundary between **global** and **session-bound** model
//! configuration.
//!
//! The product configures models for many distinct *areas* (chat, fast,
//! task-summary, memory-background, vision, sub-agent, …). The scope rules are:
//!
//! - **Session-bound** — ONLY the main *chat* model + *reasoning effort*. A
//!   session may override these; they cascade `session → request → provider
//!   default` (see `session_app::execute`).
//! - **Global** — every *auxiliary* area (fast, task-summary, memory-background,
//!   vision, sub-agent). These are read from server config (`defaults.<area>`
//!   with a provider/global fallback) and **must never be read from a session**.
//!
//! This module is the one place that enforces that split. The resolver here
//! takes no [`Session`](bamboo_domain::Session) and *cannot* — so an auxiliary
//! area model can't accidentally start tracking a per-session override. The
//! underlying per-area logic lives in [`crate::model_config_helper`]; this layer
//! groups the auxiliary trio that ~8 call sites previously resolved by hand.

use std::sync::Arc;

use bamboo_domain::reasoning::ReasoningEffort;
use bamboo_domain::ProviderModelRef;
use bamboo_infrastructure::{Config, ProviderRegistry, ResolvedModel};

use crate::model_config_helper::{
    resolve_background_model, resolve_fast_model, resolve_subagent_model, resolve_task_summary_model,
    resolve_vision_model,
};

/// The auxiliary (non-chat) models, all resolved from **global** config for a
/// given provider routing key. None of these are session-bound.
///
/// Each `*_ref` is the configured `defaults.<area>` [`ProviderModelRef`] (or
/// `None` in legacy mode), kept alongside the resolved model so callers that
/// snapshot the reference (e.g. the execute config snapshot) don't re-read it.
pub struct GlobalAreaModels {
    /// Fast/cheap model — title generation, lightweight tasks.
    pub fast: Option<ResolvedModel>,
    pub fast_ref: Option<ProviderModelRef>,
    /// Memory/background model — reflection, background memory work.
    pub background: Option<ResolvedModel>,
    pub background_ref: Option<ProviderModelRef>,
    /// Task-summary model — conversation/task summarization and compression.
    pub summarization: Option<ResolvedModel>,
    pub summarization_ref: Option<ProviderModelRef>,
}

/// Resolve the auxiliary area models from **global** config.
///
/// `provider_name` is only the routing/fallback key (request's provider, or the
/// globally active provider). It selects *which provider's* global config to
/// fall back to when an area is unconfigured — it is never a session value, and
/// `defaults.<area>` (when set) wins regardless of it.
///
/// Deliberately takes no `Session`: auxiliary models are global by design.
pub fn resolve_global_area_models(
    config: &Config,
    provider_name: &str,
    provider_registry: &Arc<ProviderRegistry>,
) -> GlobalAreaModels {
    let defaults = config.defaults.as_ref();
    GlobalAreaModels {
        fast: resolve_fast_model(config, provider_name, provider_registry),
        fast_ref: defaults.and_then(|d| d.fast.clone()),
        background: resolve_background_model(config, provider_name, provider_registry),
        background_ref: defaults.and_then(|d| d.memory_background.clone()),
        summarization: resolve_task_summary_model(config, provider_name, provider_registry),
        summarization_ref: defaults.and_then(|d| d.task_summary.clone()),
    }
}

/// Vision model — **global**. Resolved on demand (only when a request actually
/// carries an image), so it is not part of [`GlobalAreaModels`]'s eager trio.
/// Still global-only: takes no session. `provider_name` is the fallback key.
pub fn resolve_global_vision_model(
    config: &Config,
    provider_name: &str,
    provider_registry: &Arc<ProviderRegistry>,
) -> Option<ResolvedModel> {
    resolve_vision_model(config, provider_name, provider_registry)
}

/// Sub-agent model for a given subagent type — **global**. Resolved on demand
/// at spawn time. Global-only: takes no session. `provider_name` is the
/// fallback key; `subagent_type` selects a per-type override under
/// `defaults.subagent_models`.
pub fn resolve_global_subagent_model(
    config: &Config,
    provider_name: &str,
    provider_registry: &Arc<ProviderRegistry>,
    subagent_type: &str,
) -> Option<ResolvedModel> {
    resolve_subagent_model(config, provider_name, provider_registry, subagent_type)
}

/// The source layer a resolved reasoning effort came from. Surfaced in session
/// metadata (`reasoning_effort_source`) for observability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReasoningEffortSource {
    Session,
    Request,
    ProviderDefault,
    None,
}

impl ReasoningEffortSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::Request => "request",
            Self::ProviderDefault => "provider_default",
            Self::None => "none",
        }
    }
}

/// The single reasoning-effort cascade: `session → request → provider default`.
///
/// Returns `None` when nothing is configured (so non-reasoning models send no
/// reasoning parameter). When a *concrete* terminal value is required (e.g. the
/// UI display), callers should fall back to
/// [`bamboo_domain::DEFAULT_REASONING_EFFORT`] — that is the one canonical
/// `"medium"`, not a level hardcoded at the call site.
pub fn resolve_effective_reasoning_effort(
    session_effort: Option<ReasoningEffort>,
    request_effort: Option<ReasoningEffort>,
    provider_default: Option<ReasoningEffort>,
) -> (Option<ReasoningEffort>, ReasoningEffortSource) {
    if let Some(effort) = session_effort {
        (Some(effort), ReasoningEffortSource::Session)
    } else if let Some(effort) = request_effort {
        (Some(effort), ReasoningEffortSource::Request)
    } else if let Some(effort) = provider_default {
        (Some(effort), ReasoningEffortSource::ProviderDefault)
    } else {
        (None, ReasoningEffortSource::None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::tools::ToolSchema;
    use bamboo_agent_core::Message;
    use bamboo_domain::{Session, DEFAULT_REASONING_EFFORT};
    use bamboo_infrastructure::{
        DefaultsConfig, FeatureFlags, LLMError, LLMProvider, LLMStream, OpenAIConfig,
        ProviderConfigs,
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

    fn defaults_with_all_areas() -> DefaultsConfig {
        DefaultsConfig {
            chat: ProviderModelRef::new("openai", "gpt-chat"),
            fast: Some(ProviderModelRef::new("openai", "gpt-fast")),
            task_summary: Some(ProviderModelRef::new("openai", "gpt-summary")),
            vision: Some(ProviderModelRef::new("openai", "gpt-vision")),
            memory_background: Some(ProviderModelRef::new("openai", "gpt-memory")),
            planning: None,
            search: None,
            code_review: None,
            sub_agent: Some(ProviderModelRef::new("openai", "gpt-sub")),
            subagent_models: HashMap::new(),
        }
    }

    fn config_with_defaults(defaults: DefaultsConfig) -> Config {
        Config {
            provider: "openai".to_string(),
            features: FeatureFlags {
                provider_model_ref: true,
                ..Default::default()
            },
            defaults: Some(defaults),
            ..Config::default()
        }
    }

    // ---- Global area models read from defaults.<area> ----

    #[test]
    fn global_area_models_read_each_area_from_its_own_default() {
        let config = config_with_defaults(defaults_with_all_areas());
        let areas = resolve_global_area_models(&config, "openai", &test_registry());

        assert_eq!(areas.fast.as_ref().map(|m| m.model_name.as_str()), Some("gpt-fast"));
        assert_eq!(
            areas.summarization.as_ref().map(|m| m.model_name.as_str()),
            Some("gpt-summary")
        );
        assert_eq!(
            areas.background.as_ref().map(|m| m.model_name.as_str()),
            Some("gpt-memory")
        );
        // The kept refs match the configured defaults.
        assert_eq!(areas.fast_ref, Some(ProviderModelRef::new("openai", "gpt-fast")));
        assert_eq!(
            areas.summarization_ref,
            Some(ProviderModelRef::new("openai", "gpt-summary"))
        );
        assert_eq!(
            areas.background_ref,
            Some(ProviderModelRef::new("openai", "gpt-memory"))
        );
    }

    /// The core invariant the user asked for: auxiliary models are GLOBAL —
    /// they do not change with the session. We resolve once, then again after
    /// constructing a session that picks a totally different chat model, and
    /// assert the auxiliary models are byte-for-byte identical. (The resolver
    /// has no `Session` parameter, so this is also enforced at compile time;
    /// this test guards against a future signature change.)
    #[test]
    fn global_area_models_are_independent_of_any_session() {
        let config = config_with_defaults(defaults_with_all_areas());
        let registry = test_registry();

        let before = resolve_global_area_models(&config, "openai", &registry);

        // A session whose chat model is something exotic must not influence aux.
        let mut session = Session::new("s1", "some-exotic-session-model");
        session.model_ref = Some(ProviderModelRef::new("openai", "some-exotic-session-model"));
        session.reasoning_effort = Some(ReasoningEffort::Max);
        let _ = &session; // it is intentionally NOT passed to the resolver

        let after = resolve_global_area_models(&config, "openai", &registry);

        assert_eq!(
            before.fast.as_ref().map(|m| m.model_name.clone()),
            after.fast.as_ref().map(|m| m.model_name.clone())
        );
        assert_eq!(
            before.background.as_ref().map(|m| m.model_name.clone()),
            after.background.as_ref().map(|m| m.model_name.clone())
        );
        assert_eq!(
            before.summarization.as_ref().map(|m| m.model_name.clone()),
            after.summarization.as_ref().map(|m| m.model_name.clone())
        );
        // And definitely not the session's chat model.
        assert_ne!(
            after.fast.as_ref().map(|m| m.model_name.as_str()),
            Some("some-exotic-session-model")
        );
    }

    #[test]
    fn vision_model_is_global_from_defaults() {
        let config = config_with_defaults(defaults_with_all_areas());
        let vision = resolve_global_vision_model(&config, "openai", &test_registry());
        assert_eq!(vision.as_ref().map(|m| m.model_name.as_str()), Some("gpt-vision"));
    }

    #[test]
    fn subagent_model_is_global_from_defaults() {
        let config = config_with_defaults(defaults_with_all_areas());
        // No per-type override → falls back to defaults.sub_agent (global).
        let sub = resolve_global_subagent_model(&config, "openai", &test_registry(), "coder");
        assert_eq!(sub.as_ref().map(|m| m.model_name.as_str()), Some("gpt-sub"));
    }

    #[test]
    fn background_falls_back_to_fast_when_memory_background_unset() {
        let mut defaults = defaults_with_all_areas();
        defaults.memory_background = None;
        let config = config_with_defaults(defaults);

        let areas = resolve_global_area_models(&config, "openai", &test_registry());
        // memory_background unset → falls back to defaults.fast.
        assert_eq!(
            areas.background.as_ref().map(|m| m.model_name.as_str()),
            Some("gpt-fast")
        );
    }

    #[test]
    fn legacy_mode_resolves_fast_from_provider_config() {
        // Flag OFF: no `defaults`, fast comes from the provider's global config.
        let config = Config {
            provider: "openai".to_string(),
            features: FeatureFlags {
                provider_model_ref: false,
                ..Default::default()
            },
            defaults: None,
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

        let areas = resolve_global_area_models(&config, "openai", &test_registry());
        assert_eq!(areas.fast.as_ref().map(|m| m.model_name.as_str()), Some("gpt-4o-mini"));
    }

    // ---- reasoning effort cascade ----

    #[test]
    fn reasoning_prefers_session_then_request_then_provider() {
        assert_eq!(
            resolve_effective_reasoning_effort(
                Some(ReasoningEffort::Max),
                Some(ReasoningEffort::High),
                Some(ReasoningEffort::Low),
            ),
            (Some(ReasoningEffort::Max), ReasoningEffortSource::Session)
        );
        assert_eq!(
            resolve_effective_reasoning_effort(
                None,
                Some(ReasoningEffort::High),
                Some(ReasoningEffort::Low),
            ),
            (Some(ReasoningEffort::High), ReasoningEffortSource::Request)
        );
        assert_eq!(
            resolve_effective_reasoning_effort(None, None, Some(ReasoningEffort::Low)),
            (Some(ReasoningEffort::Low), ReasoningEffortSource::ProviderDefault)
        );
    }

    #[test]
    fn reasoning_none_when_nothing_configured() {
        let (effort, source) = resolve_effective_reasoning_effort(None, None, None);
        assert_eq!(effort, None);
        assert_eq!(source, ReasoningEffortSource::None);
    }

    #[test]
    fn canonical_default_is_medium_and_used_as_terminal() {
        // The one place "medium" is defined; callers needing a concrete value
        // use this rather than hardcoding a level.
        assert_eq!(DEFAULT_REASONING_EFFORT, ReasoningEffort::Medium);
        let (effort, _) = resolve_effective_reasoning_effort(None, None, None);
        assert_eq!(effort.unwrap_or(DEFAULT_REASONING_EFFORT), ReasoningEffort::Medium);
    }
}
