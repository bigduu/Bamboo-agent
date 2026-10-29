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
use bamboo_domain::ProviderModelRef;
use bamboo_llm::{Config, LLMProvider, ProviderRegistry};

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
    /// The admitted client for the selected primary model. Host consumers must
    /// dispatch this pair together rather than use a mutable default client.
    pub model_provider: Option<Arc<dyn LLMProvider>>,
    pub model_origin: Option<Arc<bamboo_llm::provider_registry::ManagedProviderOrigin>>,
    pub model_error: Option<String>,
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
    let mut target = default_chat_model_ref(config_snapshot);
    let (model_provider, model_origin, model_error) = match target.as_ref() {
        Some(requested) => match crate::model_config_helper::try_resolve_primary_model(
            config_snapshot,
            provider_registry,
            requested,
        ) {
            Ok(selected) => {
                target = Some(selected.target);
                (Some(selected.provider), selected.managed_origin, None)
            }
            Err(error) => (None, None, Some(error.to_string())),
        },
        None => (None, None, None),
    };
    let model = target.as_ref().map(|target| target.model.clone());
    let provider_name = Some(
        target
            .as_ref()
            .map(|target| target.provider.clone())
            .unwrap_or_else(|| config_snapshot.effective_default_provider().to_string()),
    );
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
    let reasoning_effort = match target.as_ref() {
        Some(target) => target
            .reasoning_effort
            .or_else(|| config_snapshot.reasoning_effort_for_key(&target.provider)),
        None => config_snapshot.get_reasoning_effort(),
    };
    let base_system_prompt = crate::prompt_defaults::read_global_default_system_prompt_template();
    let workspace_path = config_snapshot
        .get_default_work_area_path()
        .map(|path| bamboo_config::paths::path_to_display_string(&path));
    let system_prompt = crate::context::assemble_system_prompt(
        &base_system_prompt,
        None,
        workspace_path.as_deref(),
    );
    let model_roster = ModelRoster::from_areas(model, provider_name, provider_type, areas);

    ResolvedDefaultRunConfig {
        model_roster,
        model_provider,
        model_origin,
        model_error,
        reasoning_effort,
        gold_config: crate::model_config_helper::resolve_gold_config(config_snapshot, None),
        system_prompt,
        base_system_prompt,
        workspace_path,
    }
}

/// Keep configured intent separate from admission so a bad explicit target is
/// reported, never replaced by an unrelated default account.
pub fn default_chat_model_ref(config: &Config) -> Option<ProviderModelRef> {
    if config.features.provider_model_ref {
        if let Some(defaults) = config.defaults.as_ref() {
            return Some(defaults.chat.clone());
        }
    }
    let provider = config.effective_default_provider();
    let model = if let Some(instance) = config.provider_instances.get(provider) {
        // A disabled configured target is still intent; admission reports it.
        instance
            .model
            .clone()
            .or_else(|| (instance.provider_type == "copilot").then(|| "gpt-4o".to_string()))
    } else {
        config.get_model()
    }?;
    (!model.trim().is_empty()).then(|| ProviderModelRef::new(provider, model.trim()))
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! test_config {
        (@assign $config:ident, providers, $value:expr) => { *$config.providers_mut() = $value; };
        (@assign $config:ident, memory, $value:expr) => { *$config.memory_mut() = $value; };
        (@assign $config:ident, subagents, $value:expr) => { *$config.subagents_mut() = $value; };
        (@assign $config:ident, $field:ident, $value:expr) => { $config.$field = $value; };
        ($($field:ident: $value:expr),* $(,)?) => {{
            let mut config = Config::default();
            $(test_config!(@assign config, $field, $value);)*
            config
        }};
    }
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
        test_config! {
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
    fn alias_routing_uses_selected_instance_reasoning_without_legacy_fallback() {
        let mut config = config_with_area_defaults();
        config
            .providers_mut()
            .openai
            .as_mut()
            .unwrap()
            .reasoning_effort = Some(ReasoningEffort::High);
        config.default_provider_instance = Some("work-openai".into());
        config.provider_instances.insert(
            "work-openai".into(),
            serde_json::from_value(serde_json::json!({
                "provider_type": "openai", "enabled": true, "model": "gpt-chat",
                "runtime_models": ["gpt-chat"]
            }))
            .unwrap(),
        );
        let providers: HashMap<String, Arc<dyn LLMProvider>> = HashMap::from([(
            "work-openai".into(),
            Arc::new(NoopProvider) as Arc<dyn LLMProvider>,
        )]);
        let registry = Arc::new(ProviderRegistry::new(providers, "work-openai".into()));

        let selected = resolve_default_run_config(&config, &registry);
        assert!(selected.model_error.is_none(), "{:?}", selected.model_error);
        assert_eq!(
            selected.model_roster.provider_name.as_deref(),
            Some("work-openai")
        );
        assert_eq!(selected.reasoning_effort, None);

        config
            .provider_instances
            .get_mut("work-openai")
            .unwrap()
            .reasoning_effort = Some(ReasoningEffort::Low);
        let selected = resolve_default_run_config(&config, &registry);
        assert_eq!(selected.reasoning_effort, Some(ReasoningEffort::Low));
        config.defaults.as_mut().unwrap().chat.reasoning_effort = Some(ReasoningEffort::Medium);
        let selected = resolve_default_run_config(&config, &registry);
        assert_eq!(selected.reasoning_effort, Some(ReasoningEffort::Medium));
    }

    #[test]
    fn missing_chat_target_retains_legacy_reasoning_default() {
        let mut config = Config::default();
        config.provider = "openai".into();
        config.providers_mut().openai = Some(OpenAIConfig {
            reasoning_effort: Some(ReasoningEffort::High),
            ..Default::default()
        });
        let resolved = resolve_default_run_config(&config, &test_registry());
        assert!(resolved.model_roster.model.is_none());
        assert_eq!(resolved.reasoning_effort, Some(ReasoningEffort::High));
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
