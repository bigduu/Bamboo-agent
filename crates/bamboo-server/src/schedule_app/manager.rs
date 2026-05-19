use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::{broadcast, mpsc, RwLock};

use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{AgentEvent, Message, Role, Session};
use bamboo_domain::reasoning::ReasoningEffort;
use bamboo_engine::execution::{
    create_event_forwarder, finalize_runner, get_or_create_event_sender, try_reserve_runner,
    AgentRunner, RunnerReservation,
};
use bamboo_engine::ExecuteRequest;
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
    pub model: String,
    pub provider_name: Option<String>,
    pub provider_type: Option<String>,
    pub fast_model: Option<String>,
    pub fast_model_provider: Option<Arc<dyn bamboo_infrastructure::LLMProvider>>,
    pub background_model: Option<String>,
    pub background_model_provider: Option<Arc<dyn bamboo_infrastructure::LLMProvider>>,
    pub summarization_model: Option<String>,
    pub summarization_model_provider: Option<Arc<dyn bamboo_infrastructure::LLMProvider>>,
    pub reasoning_effort: Option<ReasoningEffort>,
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
    pub sessions_cache: Arc<RwLock<HashMap<String, Session>>>,
    pub agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    pub session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
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

    // If the adapter resolved an empty model, skip the run.
    if resolved.model.trim().is_empty() {
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
        &resolved.model,
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
    {
        let mut sessions = ctx.sessions_cache.write().await;
        sessions.insert(session_id.clone(), session.clone());
    }

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
        resolved.model,
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
    if resolved.model.trim().is_empty() {
        let msg = "resolved model is empty".to_string();
        session.add_message(Message::assistant(format!("❌ {msg}"), None));
        let _ = ctx.persistence.merge_save_runtime(&mut session).await;
        return Err(msg);
    }

    let session_tx = get_or_create_event_sender(&ctx.session_event_senders, &session_id).await;
    let schedule_id_for_log = job.schedule_id.clone();
    let run_id_for_log = job.run_id.clone();

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
    );

    // Run agent loop in background.
    let agent_runtime = ctx.agent.clone();
    let tools = ctx.tools.clone();
    let schedule_store = ctx.schedule_store.clone();
    let persistence = ctx.persistence.clone();
    let session_id_clone = session_id.clone();
    let schedule_id_for_state = job.schedule_id.clone();
    let run_id_for_state = job.run_id.clone();
    let agent_runners_for_status = ctx.agent_runners.clone();
    let sessions_cache = ctx.sessions_cache.clone();
    let model = resolved.model.clone();
    let provider_name = resolved.provider_name.clone();
    let provider_type = resolved.provider_type.clone();
    let fast_model = resolved.fast_model.clone();
    let fast_model_provider = resolved.fast_model_provider.clone();
    let background_model = resolved.background_model.clone();
    let background_model_provider = resolved.background_model_provider.clone();
    let summarization_model = resolved.summarization_model.clone();
    let summarization_model_provider = resolved.summarization_model_provider.clone();
    let reasoning_effort = resolved.reasoning_effort;

    tokio::spawn(async move {
        let initial_message = session
            .messages
            .last()
            .filter(|m| matches!(m.role, Role::User))
            .map(|m| m.content.clone())
            .unwrap_or_default();

        let result = agent_runtime
            .execute(
                &mut session,
                ExecuteRequest {
                    initial_message,
                    event_tx: mpsc_tx,
                    cancel_token,
                    tools: Some(tools),
                    provider_override: None,
                    model: Some(model.clone()),
                    provider_name,
                    provider_type,
                    fast_model,
                    fast_model_provider,
                    background_model,
                    background_model_provider,
                    summarization_model,
                    summarization_model_provider,
                    reasoning_effort,
                    disabled_tools: None,
                    disabled_skill_ids: None,
                    selected_skill_ids: None,
                    selected_skill_mode: None,
                    image_fallback: None,
                    app_data_dir: ctx.app_data_dir.clone(),
                },
            )
            .await;

        let terminal_status = if let Err(ref e) = result {
            // Persist a visible failure marker so the user can open the scheduled session
            // and understand why it didn't produce output.
            session.add_message(Message::assistant(
                format!("❌ Scheduled run failed: {e}"),
                None,
            ));
            tracing::warn!(
                "[schedule:{}][run:{}][session:{}] scheduled run failed: {}",
                schedule_id_for_log,
                run_id_for_log,
                session_id_clone,
                e
            );
            if e.to_string().contains("cancelled") {
                ScheduleRunStatus::Cancelled
            } else {
                ScheduleRunStatus::Failed
            }
        } else {
            tracing::info!(
                "[schedule:{}][run:{}][session:{}] scheduled run completed",
                schedule_id_for_log,
                run_id_for_log,
                session_id_clone
            );
            ScheduleRunStatus::Success
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

        finalize_runner(&agent_runners_for_status, &session_id_clone, &result).await;

        let _ = persistence.merge_save_runtime(&mut session).await;
        {
            let mut sessions = sessions_cache.write().await;
            sessions.insert(session_id_clone.clone(), session);
        }
    });

    Ok(ScheduleRunLifecycleResult::BackgroundExecutionInProgress)
}
