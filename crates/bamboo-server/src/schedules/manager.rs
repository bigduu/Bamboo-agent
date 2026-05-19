//! Schedule manager — adapter layer over bamboo-application-schedule.
//!
//! Re-exports `ScheduleManager` and `ScheduleRunJob` from the crate, and
//! provides [`build_schedule_context`] to construct a `ScheduleContext`
//! with server-specific Config resolution baked in.

use std::sync::Arc;

use crate::model_config_helper::{
    get_schedule_model_from_config, resolve_background_model, resolve_fast_model,
    resolve_provider_type, resolve_task_summary_model,
};
pub use crate::schedule_app::{
    ResolvedRunConfig, ScheduleContext, ScheduleManager, ScheduleRunJob,
};
use bamboo_infrastructure::ProviderRegistry;

/// Build a [`ScheduleContext`] with server-specific config resolution.
///
/// Callers should prefer this over constructing `ScheduleContext` directly
/// to ensure the `resolve_run_config` callback correctly reads Config and
/// prompt defaults.
pub fn build_schedule_context(
    base: ScheduleContext,
    config: std::sync::Arc<tokio::sync::RwLock<bamboo_infrastructure::Config>>,
    provider_registry: Arc<ProviderRegistry>,
) -> ScheduleContext {
    ScheduleContext {
        schedule_store: base.schedule_store,
        agent: base.agent,
        tools: base.tools,
        sessions_cache: base.sessions_cache,
        agent_runners: base.agent_runners,
        session_event_senders: base.session_event_senders,
        app_data_dir: base.app_data_dir,
        trigger_engine: base.trigger_engine,
        persistence: base.persistence,
        resolve_run_config: std::sync::Arc::new(move |job: &ScheduleRunJob| {
            resolve_run_config_from_config(job, &config, &provider_registry)
        }),
    }
}

fn resolve_run_config_from_config(
    job: &ScheduleRunJob,
    config: &std::sync::Arc<tokio::sync::RwLock<bamboo_infrastructure::Config>>,
    provider_registry: &Arc<ProviderRegistry>,
) -> ResolvedRunConfig {
    let config_snapshot = config.try_read().map(|g| g.clone()).unwrap_or_default();

    let requested_model = job
        .run_config
        .model
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string());

    let model = if let Some(m) = requested_model {
        m
    } else {
        get_schedule_model_from_config(&config_snapshot).unwrap_or_default()
    };

    let provider_name = Some(config_snapshot.effective_default_provider().to_string());
    let provider_type = provider_name
        .as_deref()
        .and_then(|name| resolve_provider_type(&config_snapshot, name, provider_registry));

    let capability_provider_name = provider_name
        .as_deref()
        .unwrap_or(config_snapshot.effective_default_provider());
    let resolved_fast = resolve_fast_model(
        &config_snapshot,
        capability_provider_name,
        provider_registry,
    );
    let resolved_background = resolve_background_model(
        &config_snapshot,
        capability_provider_name,
        provider_registry,
    );
    let resolved_summarization = resolve_task_summary_model(
        &config_snapshot,
        capability_provider_name,
        provider_registry,
    );

    let requested_reasoning_effort = job.run_config.reasoning_effort;
    let reasoning_effort = requested_reasoning_effort.or(config_snapshot.get_reasoning_effort());

    let global_default_prompt =
        crate::prompt_defaults::read_global_default_system_prompt_template();
    let base_system_prompt = job
        .run_config
        .system_prompt
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or(global_default_prompt.as_str());

    let workspace_path = job
        .run_config
        .workspace_path
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string)
        .or_else(|| {
            config_snapshot
                .get_default_work_area_path()
                .map(|path| bamboo_infrastructure::paths::path_to_display_string(&path))
        });

    let enhance_prompt = job
        .run_config
        .enhance_prompt
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty());

    let system_prompt = bamboo_engine::context::assemble_system_prompt(
        base_system_prompt,
        enhance_prompt,
        workspace_path.as_deref(),
    );

    ResolvedRunConfig {
        model,
        provider_name,
        provider_type,
        fast_model: resolved_fast.as_ref().map(|m| m.model_name.clone()),
        fast_model_provider: resolved_fast.map(|m| m.provider),
        background_model: resolved_background.as_ref().map(|m| m.model_name.clone()),
        background_model_provider: resolved_background.map(|m| m.provider),
        summarization_model: resolved_summarization
            .as_ref()
            .map(|m| m.model_name.clone()),
        summarization_model_provider: resolved_summarization.map(|m| m.provider),
        reasoning_effort,
        system_prompt,
        base_system_prompt: base_system_prompt.to_string(),
        workspace_path,
    }
}

#[cfg(test)]
mod tests {
    use super::resolve_run_config_from_config;
    use crate::schedule_app::ScheduleRunJob;
    use bamboo_domain::{ProviderModelRef, ScheduleRunConfig};
    use bamboo_infrastructure::{
        Config, DefaultsConfig, OpenAIConfig, ProviderConfigs, ProviderRegistry,
    };
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    fn test_job() -> ScheduleRunJob {
        ScheduleRunJob {
            run_id: "run-1".to_string(),
            schedule_id: "schedule-1".to_string(),
            schedule_name: "nightly".to_string(),
            run_config: ScheduleRunConfig::default(),
            scheduled_for: chrono::Utc::now(),
            claimed_at: chrono::Utc::now(),
            was_catch_up: false,
        }
    }

    #[test]
    fn resolve_run_config_from_config_prefers_fast_model() {
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

        let registry = Arc::new(ProviderRegistry::new(
            Default::default(),
            "openai".to_string(),
        ));
        let resolved =
            resolve_run_config_from_config(&test_job(), &Arc::new(RwLock::new(config)), &registry);
        assert_eq!(resolved.model, "gpt-4o-mini");
    }

    #[test]
    fn resolve_run_config_from_config_falls_back_to_default_model_when_fast_missing() {
        let config = Config {
            provider: "openai".to_string(),
            defaults: Some(DefaultsConfig {
                chat: ProviderModelRef::new("openai", "gpt-chat"),
                fast: None,
                task_summary: None,
                vision: None,
                memory_background: None,
                planning: None,
                search: None,
                code_review: None,
                sub_agent: None,
                subagent_models: HashMap::new(),
            }),
            features: bamboo_infrastructure::FeatureFlags {
                provider_model_ref: true,
                ..Default::default()
            },
            providers: ProviderConfigs::default(),
            ..Config::default()
        };

        let registry = Arc::new(ProviderRegistry::new(
            Default::default(),
            "openai".to_string(),
        ));
        let resolved =
            resolve_run_config_from_config(&test_job(), &Arc::new(RwLock::new(config)), &registry);
        assert_eq!(resolved.model, "gpt-chat");
    }
}
