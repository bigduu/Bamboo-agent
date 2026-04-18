//! Schedule manager — adapter layer over bamboo-application-schedule.
//!
//! Re-exports `ScheduleManager` and `ScheduleRunJob` from the crate, and
//! provides [`build_schedule_context`] to construct a `ScheduleContext`
//! with server-specific Config resolution baked in.

pub use bamboo_application_schedule::{ResolvedRunConfig, ScheduleContext, ScheduleManager, ScheduleRunJob};

/// Build a [`ScheduleContext`] with server-specific config resolution.
///
/// Callers should prefer this over constructing `ScheduleContext` directly
/// to ensure the `resolve_run_config` callback correctly reads Config and
/// prompt defaults.
pub fn build_schedule_context(
    base: ScheduleContext,
    config: std::sync::Arc<tokio::sync::RwLock<bamboo_infrastructure_config::Config>>,
) -> ScheduleContext {
    ScheduleContext {
        schedule_store: base.schedule_store,
        agent: base.agent,
        tools: base.tools,
        sessions_cache: base.sessions_cache,
        agent_runners: base.agent_runners,
        session_event_senders: base.session_event_senders,
        trigger_engine: base.trigger_engine,
        resolve_run_config: std::sync::Arc::new(move |job: &ScheduleRunJob| {
            resolve_run_config_from_config(job, &config)
        }),
    }
}

fn resolve_run_config_from_config(
    job: &ScheduleRunJob,
    config: &std::sync::Arc<tokio::sync::RwLock<bamboo_infrastructure_config::Config>>,
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
        config_snapshot
            .get_model()
            .map(|m| m.trim().to_string())
            .filter(|m| !m.is_empty())
            .unwrap_or_default()
    };

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
                .map(|path| bamboo_infrastructure_config::paths::path_to_display_string(&path))
        });

    let enhance_prompt = job
        .run_config
        .enhance_prompt
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty());

    let system_prompt = bamboo_application_runtime::context::assemble_system_prompt(
        base_system_prompt,
        enhance_prompt,
        workspace_path.as_deref(),
    );

    ResolvedRunConfig {
        model,
        reasoning_effort,
        system_prompt,
        base_system_prompt: base_system_prompt.to_string(),
        workspace_path,
    }
}
