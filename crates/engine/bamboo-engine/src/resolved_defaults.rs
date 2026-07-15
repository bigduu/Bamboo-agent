//! Single, shared implementation of "resolve the server's current effective
//! run configuration purely from live `Config`" — no per-request overrides.
//!
//! This is the one place that combines [`crate::model_areas`]'s
//! auxiliary-area resolution, [`crate::model_config_helper`]'s
//! provider-type/gold-config resolution, [`crate::prompt_defaults`]'s
//! template read, and [`crate::context::assemble_system_prompt`] into a
//! single resolved snapshot. Two callers need EXACTLY this: `bamboo-server`'s
//! connect bridge (`resolve_connect_run_config`, called once per inbound chat
//! message) and the public `GET /api/v1/execute/defaults` HTTP handler (an
//! external connector's "what would the server currently run with" probe,
//! issue #480). Both call [`resolve_default_run_config`] rather than each
//! reimplementing the cascade — that reimplementation is exactly the drift
//! #480 set out to prevent.
//!
//! Deliberately mirrors `schedule_app::manager::resolve_run_config_from_config`
//! minus the per-job overrides a scheduled run supports (a chat message or a
//! defaults probe has none).

use std::sync::Arc;

use bamboo_domain::reasoning::ReasoningEffort;
use bamboo_llm::{Config, ProviderRegistry};

use crate::config::GoldConfig;
use crate::ModelRoster;

/// Resolved model/prompt/workspace configuration for a run that carries no
/// per-request overrides, derived purely from the live global config.
#[derive(Clone)]
pub struct ResolvedDefaultRunConfig {
    /// Primary + auxiliary model/provider selection. The primary model is
    /// resolved via `model_roster.model` (may be `None`/empty if the server
    /// has no model configured at all).
    pub model_roster: ModelRoster,
    pub reasoning_effort: Option<ReasoningEffort>,
    pub gold_config: Option<GoldConfig>,
    /// Assembled system prompt (base template + workspace note).
    pub system_prompt: String,
    pub base_system_prompt: String,
    pub workspace_path: Option<String>,
}

/// Resolve the server's current effective run configuration purely from live
/// `Config` — no per-request overrides. See the module docs for why this is
/// the sole implementation of this cascade.
pub fn resolve_default_run_config(
    config_snapshot: &Config,
    provider_registry: &Arc<ProviderRegistry>,
) -> ResolvedDefaultRunConfig {
    let model = config_snapshot.get_model().unwrap_or_default();
    let provider_name = Some(config_snapshot.effective_default_provider().to_string());
    let provider_type = provider_name.as_deref().and_then(|name| {
        crate::model_config_helper::resolve_provider_type(config_snapshot, name, provider_registry)
    });
    let capability_provider_name = provider_name
        .as_deref()
        .unwrap_or(config_snapshot.effective_default_provider());
    // Auxiliary models are global (config-derived), never session-bound —
    // see `crate::model_areas`'s module docs.
    let areas = crate::model_areas::resolve_global_area_models(
        config_snapshot,
        capability_provider_name,
        provider_registry,
    );
    let reasoning_effort = config_snapshot.get_reasoning_effort();
    let base_system_prompt = crate::prompt_defaults::read_global_default_system_prompt_template();
    let workspace_path = config_snapshot
        .get_default_work_area_path()
        .map(|path| bamboo_config::paths::path_to_display_string(&path));
    let system_prompt = crate::context::assemble_system_prompt(
        &base_system_prompt,
        None,
        workspace_path.as_deref(),
    );
    let model_roster = ModelRoster::from_areas(Some(model), provider_name, provider_type, areas);

    ResolvedDefaultRunConfig {
        model_roster,
        reasoning_effort,
        gold_config: crate::model_config_helper::resolve_gold_config(config_snapshot, None),
        system_prompt,
        base_system_prompt,
        workspace_path,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::tools::ToolSchema;
    use bamboo_agent_core::Message;
    use bamboo_config::{DefaultsConfig, FeatureFlags, OpenAIConfig, ProviderConfigs};
    use bamboo_domain::ProviderModelRef;
    use bamboo_llm::{LLMError, LLMProvider, LLMStream};
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

    fn config_with_area_defaults() -> Config {
        let defaults = DefaultsConfig {
            chat: ProviderModelRef::new("openai", "gpt-chat"),
            fast: Some(ProviderModelRef::new("openai", "gpt-fast")),
            task_summary: Some(ProviderModelRef::new("openai", "gpt-summary")),
            vision: None,
            memory_background: Some(ProviderModelRef::new("openai", "gpt-memory")),
            planning: None,
            search: None,
            code_review: None,
            sub_agent: None,
            subagent_models: HashMap::new(),
        };
        Config {
            provider: "openai".to_string(),
            features: FeatureFlags {
                provider_model_ref: true,
                ..Default::default()
            },
            providers: ProviderConfigs {
                openai: Some(OpenAIConfig {
                    api_key: "test-secret-key".to_string(),
                    model: Some("gpt-chat".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            defaults: Some(defaults),
            ..Config::default()
        }
    }

    /// The whole point of extraction: resolution must reflect *distinct*
    /// area models, not just echo the raw chat model into every field.
    #[test]
    fn resolve_default_run_config_resolves_distinct_area_models() {
        let config = config_with_area_defaults();
        let registry = test_registry();

        let resolved = resolve_default_run_config(&config, &registry);

        assert_eq!(resolved.model_roster.model.as_deref(), Some("gpt-chat"));
        assert_eq!(
            resolved.model_roster.fast_model().as_deref(),
            Some("gpt-fast")
        );
        assert_eq!(
            resolved.model_roster.background_model().as_deref(),
            Some("gpt-memory")
        );
        assert_eq!(
            resolved.model_roster.summarization_model().as_deref(),
            Some("gpt-summary")
        );
    }

    #[test]
    fn resolve_default_run_config_never_leaks_api_keys() {
        let config = config_with_area_defaults();
        let registry = test_registry();

        let resolved = resolve_default_run_config(&config, &registry);

        assert!(!resolved.system_prompt.contains("test-secret-key"));
        assert!(!resolved.base_system_prompt.contains("test-secret-key"));
        let gold_json =
            serde_json::to_string(&resolved.gold_config).unwrap_or_else(|_| "null".to_string());
        assert!(!gold_json.contains("test-secret-key"));
    }
}
