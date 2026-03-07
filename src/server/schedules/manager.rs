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

use super::store::{ClaimedScheduleRun, ScheduleRunConfig, ScheduleStore};

#[derive(Debug, Clone)]
pub struct ScheduleRunJob {
    pub schedule_id: String,
    pub schedule_name: String,
    pub run_config: ScheduleRunConfig,
    pub claimed_at: chrono::DateTime<chrono::Utc>,
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
                    if let Err(e) = run_schedule_job(ctx.clone(), job).await {
                        log::warn!("schedule job failed: {e}");
                    }
                }
            }
        });

        // Ticker: claims due schedules and enqueues jobs.
        tokio::spawn({
            let tx = tx.clone();
            let store = ctx.schedule_store.clone();
            async move {
                let mut ticker = tokio::time::interval(Duration::from_secs(1));
                loop {
                    ticker.tick().await;
                    let now = Utc::now();
                    let claimed: Vec<ClaimedScheduleRun> = match store.claim_due_runs(now).await {
                        Ok(v) => v,
                        Err(e) => {
                            log::warn!("claim_due_runs failed: {e}");
                            continue;
                        }
                    };
                    for c in claimed {
                        let _ = tx
                            .send(ScheduleRunJob {
                                schedule_id: c.schedule_id,
                                schedule_name: c.schedule_name,
                                run_config: c.run_config,
                                claimed_at: now,
                            })
                            .await;
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
        let segment = format!(
            "Workspace path: {path}\n{}",
            crate::server::app_state::workspace_prompt_guidance()
        );
        if !prompt.is_empty() {
            prompt.push_str("\n\n");
        }
        prompt.push_str(&segment);
    }
    prompt
}

async fn run_schedule_job(ctx: ScheduleContext, job: ScheduleRunJob) -> Result<(), String> {
    let now = Utc::now();
    let session_id = Uuid::new_v4().to_string();

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
        let snapshot = ctx.config.read().await.clone();
        match snapshot
            .get_model()
            .map(|m| m.trim().to_string())
            .filter(|m| !m.is_empty())
        {
            Some(m) => m,
            None => {
                log::warn!(
                    "[schedule:{}] skipping run: no model configured (run_config.model is empty and config.get_model() returned None)",
                    job.schedule_id
                );
                return Ok(());
            }
        }
    };

    let title = format!("{} ({})", job.schedule_name, now.to_rfc3339());
    let base_system_prompt = job
        .run_config
        .system_prompt
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or(crate::server::app_state::DEFAULT_BASE_PROMPT);
    let workspace_path = job
        .run_config
        .workspace_path
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty());
    let enhance_prompt = job
        .run_config
        .enhance_prompt
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty());

    let system_prompt = build_system_prompt(base_system_prompt, enhance_prompt, workspace_path);

    let mut session = Session::new(session_id.clone(), model.clone());
    session.title = title;
    session.metadata.insert(
        "created_by_schedule_id".to_string(),
        job.schedule_id.clone(),
    );
    session.metadata.insert(
        "base_system_prompt".to_string(),
        base_system_prompt.to_string(),
    );
    if let Some(path) = workspace_path {
        session
            .metadata
            .insert("workspace_path".to_string(), path.to_string());
    }
    session.add_message(Message::system(system_prompt));

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

    log::info!(
        "[schedule:{}] created session {} (auto_execute={}, model={}, model_source={})",
        job.schedule_id,
        session_id,
        job.run_config.auto_execute,
        model,
        if requested_model.is_some() {
            "schedule.run_config.model"
        } else {
            "config.get_model()"
        }
    );
    if !should_execute {
        return Ok(());
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

    // Insert runner status (for cancellation/status introspection).
    let cancel_token = {
        let mut runners = ctx.agent_runners.write().await;
        if let Some(runner) = runners.get(&session_id) {
            if matches!(runner.status, AgentStatus::Running) {
                return Ok(());
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
    let skill_manager = ctx.skill_manager.clone();
    let metrics = ctx.metrics_collector.clone();
    let attachment_reader = ctx.session_store.clone();
    let session_id_clone = session_id.clone();
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

        let result = run_agent_loop_with_config(
            &mut session,
            initial_message,
            mpsc_tx,
            provider,
            tools,
            cancel_token,
            AgentLoopConfig {
                max_rounds: 50,
                system_prompt,
                skill_manager: Some(skill_manager),
                skip_initial_user_message: true,
                storage: Some(storage.clone()),
                attachment_reader: Some(attachment_reader),
                metrics_collector: Some(metrics),
                model_name: Some(model.clone()),
                ..Default::default()
            },
        )
        .await;

        if let Err(ref e) = result {
            // Persist a visible failure marker so the user can open the scheduled session
            // and understand why it didn't produce output.
            session.add_message(Message::assistant(
                format!("❌ Scheduled run failed: {e}"),
                None,
            ));
            log::warn!(
                "[schedule:{}][session:{}] scheduled run failed: {}",
                schedule_id_for_log,
                session_id_clone,
                e
            );
        } else {
            log::info!(
                "[schedule:{}][session:{}] scheduled run completed",
                schedule_id_for_log,
                session_id_clone
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

    Ok(())
}
