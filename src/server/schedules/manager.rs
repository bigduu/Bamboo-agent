use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::{broadcast, mpsc, RwLock};
use uuid::Uuid;

use crate::agent::core::storage::{SessionStoreV2, Storage};
use crate::agent::core::tools::ToolExecutor;
use crate::agent::core::{AgentEvent, Message, Role, Session};
use crate::agent::llm::LLMProvider;
use crate::agent::loop_module::{run_agent_loop_with_config, AgentLoopConfig};
use crate::agent::metrics::MetricsCollector;
use crate::agent::skill::SkillManager;
use crate::core::Config;
use crate::server::app_state::{AgentRunner, AgentStatus};

use super::domain::ScheduleRunStatus;
use super::store::{ClaimedScheduleRun, ScheduleRunConfig, ScheduleStore};
use super::trigger_engine::DynTriggerEngine;

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

#[derive(Clone)]
pub struct ScheduleContext {
    pub schedule_store: Arc<ScheduleStore>,
    pub session_store: Arc<SessionStoreV2>,
    pub storage: Arc<dyn Storage>,
    pub provider: Arc<dyn LLMProvider>,
    pub tools: Arc<dyn ToolExecutor>,
    pub skill_manager: Arc<SkillManager>,
    pub metrics_collector: MetricsCollector,
    pub sessions_cache: Arc<RwLock<HashMap<String, Session>>>,
    pub agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    pub session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
    pub config: Arc<RwLock<Config>>,
    pub trigger_engine: DynTriggerEngine,
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

async fn get_or_create_sender(
    senders: &Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
    session_id: &str,
) -> broadcast::Sender<AgentEvent> {
    let mut guard = senders.write().await;
    if let Some(existing) = guard.get(session_id) {
        return existing.clone();
    }
    let (tx, _) = broadcast::channel(1000);
    guard.insert(session_id.to_string(), tx.clone());
    tx
}

fn build_system_prompt(base: &str, enhance: Option<&str>, workspace_path: Option<&str>) -> String {
    let mut prompt = base.trim().to_string();
    if let Some(extra) = enhance.map(str::trim).filter(|v| !v.is_empty()) {
        if !prompt.is_empty() {
            prompt.push_str("\n\n");
        }
        prompt.push_str(extra);
    }
    if let Some(path) = workspace_path.map(str::trim).filter(|v| !v.is_empty()) {
        if let Some(segment) = crate::server::app_state::build_workspace_prompt_context(path) {
            if !prompt.is_empty() {
                prompt.push_str("\n\n");
            }
            prompt.push_str(&segment);
        }
        if let Some(instruction_segment) =
            crate::server::instruction_layer::build_instruction_prompt_context(path)
        {
            if !prompt.is_empty() {
                prompt.push_str("\n\n");
            }
            prompt.push_str(&instruction_segment);
        }
    }
    if let Some(segment) = crate::server::app_state::build_env_prompt_context() {
        if !prompt.is_empty() {
            prompt.push_str("\n\n");
        }
        prompt.push_str(&segment);
    }
    prompt
}

async fn run_schedule_job(
    ctx: ScheduleContext,
    job: ScheduleRunJob,
) -> Result<ScheduleRunLifecycleResult, String> {
    let now = Utc::now();
    let session_id = Uuid::new_v4().to_string();
    let config_snapshot = ctx.config.read().await.clone();

    let requested_model = job
        .run_config
        .model
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string());

    // Resolve model at run-time so schedules can follow the active provider model
    // when `run_config.model` is omitted.
    //
    // If we still can't resolve a model, skip the run (no session will be created).
    let model = if let Some(m) = requested_model.clone() {
        m
    } else {
        match config_snapshot
            .get_model()
            .map(|m| m.trim().to_string())
            .filter(|m| !m.is_empty())
        {
            Some(m) => m,
            None => {
                tracing::warn!(
                    "[schedule:{}] skipping run: no model configured (run_config.model is empty and config.get_model() returned None)",
                    job.schedule_id
                );
                return Ok(ScheduleRunLifecycleResult::Terminal(
                    ScheduleRunStatus::Skipped,
                ));
            }
        }
    };
    let requested_reasoning_effort = job.run_config.reasoning_effort;
    let reasoning_effort = requested_reasoning_effort.or(config_snapshot.get_reasoning_effort());
    let disabled_tools = config_snapshot.disabled_tool_names();
    let disabled_skill_ids = config_snapshot.disabled_skill_ids();

    let title = format!("{} ({})", job.schedule_name, now.to_rfc3339());
    let global_default_prompt =
        crate::server::prompt_defaults::read_global_default_system_prompt_template();
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
                .map(|path| crate::core::paths::path_to_display_string(&path))
        });
    let enhance_prompt = job
        .run_config
        .enhance_prompt
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty());

    let system_prompt = build_system_prompt(
        base_system_prompt,
        enhance_prompt,
        workspace_path.as_deref(),
    );

    let mut session = Session::new(session_id.clone(), model.clone());
    session.title = title;
    session.metadata.insert(
        "created_by_schedule_id".to_string(),
        job.schedule_id.clone(),
    );
    session
        .metadata
        .insert("schedule_run_id".to_string(), job.run_id.clone());
    session.metadata.insert(
        "base_system_prompt".to_string(),
        base_system_prompt.to_string(),
    );
    if let Some(path) = workspace_path.as_deref() {
        session
            .metadata
            .insert("workspace_path".to_string(), path.to_string());
        crate::agent::tools::tools::workspace_state::ensure_session_workspace(
            &session_id,
            Some(std::path::PathBuf::from(path)),
        );
    }
    if let Some(effort) = reasoning_effort {
        session
            .metadata
            .insert("reasoning_effort".to_string(), effort.as_str().to_string());
    }
    session.add_message(Message::system(system_prompt));
    crate::agent::loop_module::runner::refresh_prompt_snapshot(&mut session);

    if let Some(task) = job
        .run_config
        .task_message
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        session.add_message(Message::user(task.to_string()));
    }

    // Persist session and index entry.
    ctx.storage
        .save_session(&session)
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
        model,
        if requested_model.is_some() {
            "schedule.run_config.model"
        } else {
            "config.get_model()"
        },
        reasoning_effort.map(|value| value.as_str()).unwrap_or("none"),
        if requested_reasoning_effort.is_some() {
            "schedule.run_config.reasoning_effort"
        } else {
            "config.get_reasoning_effort()"
        }
    );
    if !should_execute {
        return Ok(ScheduleRunLifecycleResult::Terminal(
            ScheduleRunStatus::Success,
        ));
    }

    // Model is required by the provider trait; if resolution failed we'd have returned earlier.
    if model.trim().is_empty() {
        let msg = "resolved model is empty".to_string();
        session.add_message(Message::assistant(format!("❌ {msg}"), None));
        let _ = ctx.storage.save_session(&session).await;
        return Err(msg);
    }

    let session_tx = get_or_create_sender(&ctx.session_event_senders, &session_id).await;
    let schedule_id_for_log = job.schedule_id.clone();
    let run_id_for_log = job.run_id.clone();

    // Insert runner status (for cancellation/status introspection).
    let cancel_token = {
        let mut runners = ctx.agent_runners.write().await;
        if let Some(runner) = runners.get(&session_id) {
            if matches!(runner.status, AgentStatus::Running) {
                return Ok(ScheduleRunLifecycleResult::Terminal(
                    ScheduleRunStatus::Skipped,
                ));
            }
        }
        runners.remove(&session_id);

        let mut runner = AgentRunner::new();
        runner.status = AgentStatus::Running;
        runner.event_sender = session_tx.clone();
        let cancel_token = runner.cancel_token.clone();
        runners.insert(session_id.clone(), runner);
        cancel_token
    };

    let (mpsc_tx, mut mpsc_rx) = tokio::sync::mpsc::channel::<AgentEvent>(100);
    let session_id_forwarder = session_id.clone();
    let runners_for_budget = ctx.agent_runners.clone();
    let session_tx_for_forwarder = session_tx.clone();
    tokio::spawn(async move {
        while let Some(event) = mpsc_rx.recv().await {
            if matches!(&event, AgentEvent::TokenBudgetUpdated { .. }) {
                let mut runners = runners_for_budget.write().await;
                if let Some(runner) = runners.get_mut(&session_id_forwarder) {
                    runner.last_budget_event = Some(event.clone());
                }
            }
            let _ = session_tx_for_forwarder.send(event);
        }
    });

    // Run agent loop in background.
    let provider = ctx.provider.clone();
    let tools = ctx.tools.clone();
    let storage = ctx.storage.clone();
    let schedule_store = ctx.schedule_store.clone();
    let skill_manager = ctx.skill_manager.clone();
    let metrics = ctx.metrics_collector.clone();
    let attachment_reader = ctx.session_store.clone();
    let session_id_clone = session_id.clone();
    let schedule_id_for_state = job.schedule_id.clone();
    let run_id_for_state = job.run_id.clone();
    let agent_runners_for_status = ctx.agent_runners.clone();
    let sessions_cache = ctx.sessions_cache.clone();

    tokio::spawn(async move {
        let system_prompt = session
            .messages
            .iter()
            .find(|m| matches!(m.role, Role::System))
            .map(|m| m.content.clone());

        let initial_message = session
            .messages
            .last()
            .filter(|m| matches!(m.role, Role::User))
            .map(|m| m.content.clone())
            .unwrap_or_default();
        let provider_name = ctx.config.read().await.provider.clone();
        let memory_background_model = ctx.config.read().await.get_memory_background_model();

        let result = run_agent_loop_with_config(
            &mut session,
            initial_message,
            mpsc_tx,
            provider,
            tools,
            cancel_token,
            AgentLoopConfig {
                max_rounds: 200,
                system_prompt,
                skill_manager: Some(skill_manager),
                skip_initial_user_message: true,
                storage: Some(storage.clone()),
                attachment_reader: Some(attachment_reader),
                metrics_collector: Some(metrics),
                model_name: Some(model.clone()),
                fast_model_name: ctx.config.read().await.get_fast_model(),
                background_model_name: memory_background_model,
                provider_name: Some(provider_name),
                reasoning_effort,
                disabled_tools,
                disabled_skill_ids,
                prompt_memory_flags: ctx
                    .config
                    .read()
                    .await
                    .memory
                    .as_ref()
                    .map(crate::agent::loop_module::config::PromptMemoryFlags::from)
                    .unwrap_or_default(),
                ..Default::default()
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

        {
            let mut runners = agent_runners_for_status.write().await;
            if let Some(runner) = runners.get_mut(&session_id_clone) {
                runner.status = match result {
                    Ok(_) => AgentStatus::Completed,
                    Err(e) if e.to_string().contains("cancelled") => AgentStatus::Cancelled,
                    Err(e) => AgentStatus::Error(e.to_string()),
                };
                runner.completed_at = Some(Utc::now());
            }
        }

        let _ = storage.save_session(&session).await;
        {
            let mut sessions = sessions_cache.write().await;
            sessions.insert(session_id_clone.clone(), session);
        }
    });

    Ok(ScheduleRunLifecycleResult::BackgroundExecutionInProgress)
}
