use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::{broadcast, mpsc, RwLock};

use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{AgentEvent, Message, Role};
use bamboo_domain::reasoning::ReasoningEffort;
use bamboo_engine::config::GoldConfig;
use bamboo_engine::execution::{
    create_event_forwarder, get_or_create_event_sender, spawn_session_execution,
    try_reserve_runner, AgentRunner, RunnerReservation, SessionCompletionHook,
    SessionExecutionArgs,
};
use bamboo_engine::{AuxiliaryModelConfig, ModelRoster};
use bamboo_infrastructure::LockedSessionStore;

use super::store::{ClaimedScheduleRun, ScheduleStore};
use super::trigger_engine::DynTriggerEngine;
use bamboo_domain::{ScheduleRunConfig, ScheduleRunStatus};

#[derive(Debug, Clone)]
pub struct ScheduleRunJob {
    pub run_id: String,
    pub schedule_id: String,
    pub schedule_name: String,
    pub run_config: ScheduleRunConfig,
    pub scheduled_for: chrono::DateTime<chrono::Utc>,
    pub claimed_at: chrono::DateTime<chrono::Utc>,
    pub was_catch_up: bool,
}

/// Resolved run configuration computed by the adapter layer.
///
/// The schedule crate delegates model/prompt/workspace resolution to the
/// caller via [`ScheduleContext::resolve_run_config`] so that server-specific
/// concerns (Config, filesystem prompt templates) stay out of the crate.
#[derive(Clone)]
pub struct ResolvedRunConfig {
    /// Primary + auxiliary model/provider selection for the scheduled run.
    /// The primary `model` is required; resolve it via `roster.model`.
    pub model_roster: ModelRoster,
    pub reasoning_effort: Option<ReasoningEffort>,
    pub gold_config: Option<GoldConfig>,
    pub system_prompt: String,
    pub base_system_prompt: String,
    pub workspace_path: Option<String>,
}

#[derive(Clone)]
pub struct ScheduleContext {
    pub schedule_store: Arc<ScheduleStore>,
    pub agent: Arc<bamboo_engine::Agent>,
    pub persistence: Arc<LockedSessionStore>,
    pub tools: Arc<dyn ToolExecutor>,
    pub sessions_cache: bamboo_engine::SessionCache,
    pub agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    pub session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
    /// Optional inbox to the account-wide change feed (durable multi-client sync).
    pub account_feed_inbox: Option<bamboo_engine::execution::AccountFeedInbox>,
    pub app_data_dir: Option<std::path::PathBuf>,
    pub trigger_engine: DynTriggerEngine,
    /// Adapter-provided callback that resolves model, system prompt, workspace path
    /// and reasoning effort for a schedule run job.
    pub resolve_run_config: Arc<dyn Fn(&ScheduleRunJob) -> ResolvedRunConfig + Send + Sync>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScheduleRunLifecycleResult {
    Terminal(ScheduleRunStatus),
    BackgroundExecutionInProgress,
}

#[derive(Clone)]
pub struct ScheduleManager {
    tx: mpsc::Sender<ScheduleRunJob>,
}

impl ScheduleManager {
    pub fn new(ctx: ScheduleContext) -> Self {
        let (tx, mut rx) = mpsc::channel::<ScheduleRunJob>(128);

        // Worker: executes jobs sequentially (simple + predictable).
        tokio::spawn({
            let ctx = ctx.clone();
            async move {
                while let Some(job) = rx.recv().await {
                    if let Err(error) = ctx
                        .schedule_store
                        .mark_run_started(&job.schedule_id, &job.run_id)
                        .await
                    {
                        tracing::warn!(
                            "failed to mark schedule run started for {} / {}: {}",
                            job.schedule_id,
                            job.run_id,
                            error
                        );
                    }
                    let schedule_id = job.schedule_id.clone();
                    let run_id = job.run_id.clone();
                    match run_schedule_job(ctx.clone(), job).await {
                        Ok(ScheduleRunLifecycleResult::Terminal(status)) => {
                            if let Err(error) = ctx
                                .schedule_store
                                .mark_run_terminal(&schedule_id, &run_id, status, None)
                                .await
                            {
                                tracing::warn!(
                                    "failed to mark schedule run terminal state for {} / {}: {}",
                                    schedule_id,
                                    run_id,
                                    error
                                );
                            }
                        }
                        Ok(ScheduleRunLifecycleResult::BackgroundExecutionInProgress) => {}
                        Err(e) => {
                            tracing::warn!("schedule job failed: {e}");
                            if let Err(error) = ctx
                                .schedule_store
                                .mark_run_terminal(
                                    &schedule_id,
                                    &run_id,
                                    ScheduleRunStatus::Failed,
                                    Some(e.clone()),
                                )
                                .await
                            {
                                tracing::warn!(
                                    "failed to mark schedule run failed state for {} / {}: {}",
                                    schedule_id,
                                    run_id,
                                    error
                                );
                            }
                        }
                    }
                }
            }
        });

        // Ticker: claims due schedules and enqueues jobs.
        tokio::spawn({
            let tx = tx.clone();
            let store = ctx.schedule_store.clone();
            let trigger_engine = ctx.trigger_engine.clone();
            async move {
                let mut ticker = tokio::time::interval(Duration::from_secs(15));
                loop {
                    ticker.tick().await;
                    let now = Utc::now();
                    let claimed: Vec<ClaimedScheduleRun> = match store
                        .claim_due_runs_with_engine(now, trigger_engine.as_ref())
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!("claim_due_runs failed: {e}");
                            continue;
                        }
                    };
                    for c in claimed {
                        let schedule_id = c.schedule_id.clone();
                        let run_id = c.run_id.clone();
                        if tx
                            .send(ScheduleRunJob {
                                run_id: c.run_id,
                                schedule_id: c.schedule_id,
                                schedule_name: c.schedule_name,
                                run_config: c.run_config,
                                scheduled_for: c.scheduled_for,
                                claimed_at: c.claimed_at,
                                was_catch_up: c.was_catch_up,
                            })
                            .await
                            .is_err()
                        {
                            let _ = store
                                .mark_run_dequeued_without_start(
                                    &schedule_id,
                                    &run_id,
                                    Some("schedule manager is not running".to_string()),
                                )
                                .await;
                        }
                    }
                }
            }
        });

        Self { tx }
    }

    pub async fn enqueue_run_now(&self, job: ScheduleRunJob) -> Result<(), String> {
        self.tx
            .send(job)
            .await
            .map_err(|_| "schedule manager is not running".to_string())
    }
}

async fn run_schedule_job(
    ctx: ScheduleContext,
    job: ScheduleRunJob,
) -> Result<ScheduleRunLifecycleResult, String> {
    let resolved = (ctx.resolve_run_config)(&job);
    // Primary model is required for a schedule run; the roster stores it as
    // `Option<String>`, so recover the owned String once for the checks/logging
    // below (an absent primary is treated as the old empty-string skip).
    let resolved_model = resolved.model_roster.model.clone().unwrap_or_default();

    // If the adapter resolved an empty model, skip the run.
    if resolved_model.trim().is_empty() {
        tracing::warn!(
            "[schedule:{}] skipping run: resolved model is empty",
            job.schedule_id
        );
        return Ok(ScheduleRunLifecycleResult::Terminal(
            ScheduleRunStatus::Skipped,
        ));
    }

    let requested_model = job
        .run_config
        .model
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string());
    let requested_reasoning_effort = job.run_config.reasoning_effort;

    let mut session = super::session_factory::create_schedule_session(
        &job,
        &resolved_model,
        &resolved.system_prompt,
        &resolved.base_system_prompt,
        resolved.workspace_path.as_deref(),
        resolved.reasoning_effort,
    );
    let session_id = session.id.clone();

    // Persist session and index entry.
    ctx.persistence
        .merge_save_runtime(&mut session)
        .await
        .map_err(|e| format!("failed to save scheduled session: {e}"))?;
    if let Err(error) = ctx
        .schedule_store
        .bind_run_session(&job.schedule_id, &job.run_id, &session_id)
        .await
    {
        tracing::warn!(
            "failed to bind session {} to schedule run {} / {}: {}",
            session_id,
            job.schedule_id,
            job.run_id,
            error
        );
    }
    ctx.sessions_cache.insert(
        session_id.clone(),
        Arc::new(parking_lot::RwLock::new(session.clone())),
    );

    // If no task message (or not configured to execute), we're done.
    let should_execute = job.run_config.auto_execute
        && session
            .messages
            .last()
            .map(|m| matches!(m.role, Role::User))
            .unwrap_or(false);

    tracing::info!(
        "[schedule:{}] created session {} (auto_execute={}, model={}, model_source={}, reasoning_effort={}, reasoning_source={})",
        job.schedule_id,
        session_id,
        job.run_config.auto_execute,
        resolved_model,
        if requested_model.is_some() {
            "schedule.run_config.model"
        } else {
            "resolved"
        },
        resolved.reasoning_effort.map(|value| value.as_str()).unwrap_or("none"),
        if requested_reasoning_effort.is_some() {
            "schedule.run_config.reasoning_effort"
        } else {
            "resolved"
        }
    );
    if !should_execute {
        return Ok(ScheduleRunLifecycleResult::Terminal(
            ScheduleRunStatus::Success,
        ));
    }

    // Model is required by the provider trait; if resolution failed we'd have returned earlier.
    if resolved_model.trim().is_empty() {
        let msg = "resolved model is empty".to_string();
        session.add_message(Message::assistant(format!("❌ {msg}"), None));
        let _ = ctx.persistence.merge_save_runtime(&mut session).await;
        return Err(msg);
    }

    let session_tx = get_or_create_event_sender(&ctx.session_event_senders, &session_id).await;

    // Insert runner status (for cancellation/status introspection).
    let Some(RunnerReservation { cancel_token, .. }) =
        try_reserve_runner(&ctx.agent_runners, &session_id, &session_tx).await
    else {
        return Ok(ScheduleRunLifecycleResult::Terminal(
            ScheduleRunStatus::Skipped,
        ));
    };

    let (mpsc_tx, _forwarder_handle) = create_event_forwarder(
        session_id.clone(),
        session_tx.clone(),
        ctx.agent_runners.clone(),
        ctx.account_feed_inbox.clone(),
    );

    // Run the agent loop in the background via the single canonical execution
    // path (`spawn_session_execution`), the same one the HTTP execute handler
    // and the child-completion coordinator use. Schedule-specific finalization
    // (marking the run terminal, and writing a visible failure marker) is
    // carried by the `on_complete` hook, which runs after the runner is
    // finalized but before the session is persisted — so the marker is saved.
    let aux_fast_model = resolved.model_roster.fast_model();
    let aux_fast_provider = resolved.model_roster.fast_model_provider();
    let aux_background_model = resolved.model_roster.background_model();
    let aux_background_provider = resolved.model_roster.background_model_provider();
    let aux_summarization_model = resolved.model_roster.summarization_model();
    let aux_summarization_provider = resolved.model_roster.summarization_model_provider();
    let auxiliary_model_resolver = Arc::new(move || AuxiliaryModelConfig {
        fast_model_name: aux_fast_model.clone(),
        fast_model_provider: aux_fast_provider.clone(),
        background_model_name: aux_background_model.clone(),
        planning_model_name: None,
        search_model_name: None,
        summarization_model_name: aux_summarization_model.clone(),
        background_model_provider: aux_background_provider.clone(),
        summarization_model_provider: aux_summarization_provider.clone(),
    });

    let schedule_store = ctx.schedule_store.clone();
    let schedule_id_for_state = job.schedule_id.clone();
    let run_id_for_state = job.run_id.clone();
    let log_session_id = session_id.clone();

    let on_complete: SessionCompletionHook = Box::new(move |outcome, session| {
        Box::pin(async move {
            let terminal_status = if outcome.success {
                tracing::info!(
                    "[schedule:{}][run:{}][session:{}] scheduled run completed",
                    schedule_id_for_state,
                    run_id_for_state,
                    log_session_id
                );
                ScheduleRunStatus::Success
            } else {
                let detail = outcome.error.as_deref().unwrap_or("unknown error");
                // Persist a visible failure marker so the user can open the
                // scheduled session and understand why it produced no output.
                session.add_message(Message::assistant(
                    format!("❌ Scheduled run failed: {detail}"),
                    None,
                ));
                tracing::warn!(
                    "[schedule:{}][run:{}][session:{}] scheduled run failed: {}",
                    schedule_id_for_state,
                    run_id_for_state,
                    log_session_id,
                    detail
                );
                if outcome.cancelled {
                    ScheduleRunStatus::Cancelled
                } else {
                    ScheduleRunStatus::Failed
                }
            };

            if let Err(error) = schedule_store
                .mark_run_terminal(
                    &schedule_id_for_state,
                    &run_id_for_state,
                    terminal_status,
                    None,
                )
                .await
            {
                tracing::warn!(
                    "failed to mark schedule run terminal state for {} / {}: {}",
                    schedule_id_for_state,
                    run_id_for_state,
                    error
                );
            }
        })
    });

    spawn_session_execution(SessionExecutionArgs {
        agent: ctx.agent.clone(),
        session_id,
        session,
        tools_override: Some(ctx.tools.clone()),
        provider_override: None,
        model_roster: resolved.model_roster.clone(),
        reasoning_effort: resolved.reasoning_effort,
        reasoning_effort_source: "schedule".to_string(),
        auxiliary_model_resolver: Some(auxiliary_model_resolver),
        disabled_tools: None,
        disabled_skill_ids: None,
        selected_skill_ids: None,
        selected_skill_mode: None,
        cancel_token,
        mpsc_tx,
        image_fallback: None,
        gold_config: resolved.gold_config.clone(),
        app_data_dir: ctx.app_data_dir.clone(),
        runners: ctx.agent_runners.clone(),
        sessions_cache: ctx.sessions_cache.clone(),
        on_complete: Some(on_complete),
    });

    Ok(ScheduleRunLifecycleResult::BackgroundExecutionInProgress)
}

/// Build a [`ScheduleContext`] with server-specific config resolution.
///
/// Callers should prefer this over constructing `ScheduleContext` directly
/// to ensure the `resolve_run_config` callback correctly reads Config and
/// prompt defaults.
pub fn build_schedule_context(
    base: ScheduleContext,
    config: std::sync::Arc<tokio::sync::RwLock<bamboo_infrastructure::Config>>,
    provider_registry: Arc<bamboo_infrastructure::ProviderRegistry>,
) -> ScheduleContext {
    ScheduleContext {
        schedule_store: base.schedule_store,
        agent: base.agent,
        tools: base.tools,
        sessions_cache: base.sessions_cache,
        agent_runners: base.agent_runners,
        session_event_senders: base.session_event_senders,
        account_feed_inbox: base.account_feed_inbox,
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
    provider_registry: &Arc<bamboo_infrastructure::ProviderRegistry>,
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
        bamboo_engine::model_config_helper::get_schedule_model_from_config(&config_snapshot)
            .unwrap_or_default()
    };

    let provider_name = Some(config_snapshot.effective_default_provider().to_string());
    let provider_type = provider_name.as_deref().and_then(|name| {
        bamboo_engine::model_config_helper::resolve_provider_type(
            &config_snapshot,
            name,
            provider_registry,
        )
    });

    let capability_provider_name = provider_name
        .as_deref()
        .unwrap_or(config_snapshot.effective_default_provider());
    // Auxiliary models are global (config-derived), never session-bound.
    let areas = bamboo_engine::model_areas::resolve_global_area_models(
        &config_snapshot,
        capability_provider_name,
        provider_registry,
    );

    let requested_reasoning_effort = job.run_config.reasoning_effort;
    let reasoning_effort = requested_reasoning_effort.or(config_snapshot.get_reasoning_effort());

    let global_default_prompt =
        bamboo_engine::prompt_defaults::read_global_default_system_prompt_template();
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

    let model_roster = bamboo_engine::ModelRoster::from_areas(
        Some(model),
        provider_name,
        provider_type,
        areas,
    );

    ResolvedRunConfig {
        model_roster,
        reasoning_effort,
        gold_config: bamboo_engine::model_config_helper::resolve_gold_config(&config_snapshot, None),
        system_prompt,
        base_system_prompt: base_system_prompt.to_string(),
        workspace_path,
    }
}

#[cfg(test)]
mod build_context_tests {
    use super::resolve_run_config_from_config;
    use super::ScheduleRunJob;
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
        assert_eq!(resolved.model_roster.model.as_deref(), Some("gpt-4o-mini"));
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
        assert_eq!(resolved.model_roster.model.as_deref(), Some("gpt-chat"));
    }
}
