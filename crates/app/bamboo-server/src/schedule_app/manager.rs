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
    create_event_forwarder, get_or_create_event_sender, reserve_session_execution,
    spawn_session_execution, AgentRunner, SessionCompletionHook, SessionExecutionArgs,
    SessionExecutionReserveOutcome,
};
use bamboo_engine::{AuxiliaryModelConfig, ModelRoster};
use bamboo_storage::LockedSessionStore;

use crate::permission_audit::record_bamboo_runtime_permission_metadata;

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
    pub lifecycle_hooks: bamboo_config::LifecycleHooksConfig,
}

#[derive(Clone)]
pub struct ScheduleContext {
    pub schedule_store: Arc<ScheduleStore>,
    pub agent: Arc<bamboo_engine::Agent>,
    pub persistence: Arc<LockedSessionStore>,
    pub tools: Arc<dyn ToolExecutor>,
    pub permission_config: Option<Arc<bamboo_tools::permission::PermissionConfig>>,
    pub sessions_cache: bamboo_engine::SessionCache,
    pub agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    pub session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
    /// Optional inbox to the account-wide change feed (durable multi-client sync).
    pub account_feed_inbox: Option<bamboo_engine::execution::AccountFeedInbox>,
    pub app_data_dir: Option<std::path::PathBuf>,
    pub trigger_engine: DynTriggerEngine,
    /// Authoritative Project registry, rechecked when each persisted job fires.
    pub project_store: Arc<bamboo_projects::ProjectStore>,
    /// AppState-owned workspace policy used by every schedule preflight/fire.
    pub workspace_resolver: bamboo_agent_core::workspace_state::WorkspaceResolver,
    /// Dependencies to start the always-on notification relay (see
    /// `crate::app_state::session_events::ensure_notification_relay`).
    /// Scheduled runs previously never classified events into notifications
    /// at all — nothing spawned a relay for a session no SSE/WS client had
    /// ever subscribed to, which is the common case for a headless run.
    pub notification_relay: crate::app_state::session_events::NotificationRelayDeps,
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

/// Maximum length, in characters, of the final-assistant-message excerpt
/// used as a schedule-completion notification body (mirrors
/// `bamboo_notification::policy`'s `RUN_FAILED_BODY_MAX`, which isn't
/// exported for reuse here).
const SCHEDULE_NOTIFY_BODY_MAX: usize = 200;

/// Unicode-safe truncation to at most `max` chars, appending an ellipsis when
/// cut.
fn truncate_chars(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    let mut out: String = text.chars().take(max).collect();
    out.push('…');
    out
}

/// Builds the "Schedule '<name>' completed|failed" notification title.
fn schedule_run_title(schedule_name: &str, success: bool) -> String {
    if success {
        format!("Schedule '{schedule_name}' completed")
    } else {
        format!("Schedule '{schedule_name}' failed")
    }
}

/// Excerpts the most recent non-empty assistant message from `messages`
/// (walking back from the end), truncated to [`SCHEDULE_NOTIFY_BODY_MAX`]
/// chars. This is "cheaply reachable" because the session is already in
/// memory at the point the completion hook runs — no extra fetch or compute.
/// Returns `None` when there is no such message, so the caller can fall back
/// to a run-status string.
fn final_assistant_excerpt(messages: &[Message]) -> Option<String> {
    messages
        .iter()
        .rev()
        .find(|m| matches!(m.role, Role::Assistant) && !m.content.trim().is_empty())
        .map(|m| truncate_chars(m.content.trim(), SCHEDULE_NOTIFY_BODY_MAX))
}

/// Emits a schedule-specific completion/failure notification, enriching the
/// generic `run_completed`/`run_failed` notification the always-on relay
/// (`ensure_notification_relay`, wired in [`run_schedule_job`] below) already
/// produces from the raw `AgentEvent::Complete`/`Error` this run's agent loop
/// emits.
///
/// No-double-fire design: both sources mint through
/// [`bamboo_notification::NotificationService::notify_schedule_run`] /
/// `notify`, which share the SAME dedup key
/// (`bamboo_notification::policy::classify_schedule_run`'s doc comment has
/// the full rationale) within the service's 30s dedup window — so whichever
/// of the two actually reaches the service first "wins" the user-visible
/// copy and the second is silently coalesced. The run can therefore never
/// double-notify its owner; this call is always safe to make unconditionally
/// alongside the relay.
async fn notify_schedule_run_outcome(
    relay: &crate::app_state::session_events::NotificationRelayDeps,
    session_id: &str,
    success: bool,
    title: String,
    body: String,
) {
    let Some(notification) = relay
        .notification_service
        .notify_schedule_run(session_id, success, title, body)
    else {
        // Deduped away by the generic relay-classified notification (or
        // notifications/this category are disabled) — nothing to deliver.
        return;
    };

    // Build the sink payload before `notification` is moved into the
    // broadcast send below (mirrors `ensure_notification_relay`).
    let sink_notification = crate::notify_sinks::SinkNotification::from_event(&notification);

    let tx = relay
        .session_event_senders
        .read()
        .await
        .get(session_id)
        .cloned();
    if let Some(tx) = tx {
        let _ = tx.send(notification);
    }

    if let Some(sink_notification) = sink_notification {
        let has_watcher = relay.session_watchers.has_watcher(session_id);
        let config_snapshot = relay.config.read().await.clone();
        crate::AppState::dispatch_to_sinks(&config_snapshot, has_watcher, &sink_notification);
    }
}

async fn run_schedule_job(
    ctx: ScheduleContext,
    job: ScheduleRunJob,
) -> Result<ScheduleRunLifecycleResult, String> {
    validate_schedule_project_at_fire(&ctx.project_store, &job.run_config)?;
    let mut resolved = (ctx.resolve_run_config)(&job);
    let explicit_workspace = job
        .run_config
        .workspace_path
        .as_deref()
        .map(str::trim)
        .filter(|workspace| !workspace.is_empty());
    let requested_workspace = explicit_workspace.or_else(|| {
        job.run_config
            .project_id
            .is_none()
            .then_some(resolved.workspace_path.as_deref())
            .flatten()
    });
    let final_workspace = crate::project_context::validate_workspace_assignment_with_resolver(
        &ctx.project_store,
        job.run_config.project_id.as_ref(),
        requested_workspace,
        &ctx.workspace_resolver,
    )
    .map_err(|error| format!("validate schedule workspace at execution time: {error}"))?;
    resolved.workspace_path = final_workspace
        .as_deref()
        .map(bamboo_config::paths::path_to_display_string);
    let binding_status = match (
        job.run_config.project_id.as_ref(),
        final_workspace.as_deref(),
    ) {
        (Some(project_id), Some(workspace)) => {
            let workspace = bamboo_config::paths::path_to_display_string(workspace);
            if ctx
                .project_store
                .find_workspace_owner_for_path(&workspace)
                .map_err(|error| format!("resolve schedule workspace owner: {error}"))?
                .is_some_and(|owner| owner.id == *project_id)
            {
                bamboo_engine::project_context::WorkspaceBindingStatus::Registered
            } else {
                bamboo_engine::project_context::WorkspaceBindingStatus::Unregistered
            }
        }
        _ => bamboo_engine::project_context::WorkspaceBindingStatus::Unregistered,
    };
    let workspace_source = job.run_config.project_id.as_ref().map(|_| {
        if explicit_workspace.is_some() {
            bamboo_engine::project_context::WorkspaceSource::Explicit
        } else {
            bamboo_engine::project_context::WorkspaceSource::ProjectDefault
        }
    });
    resolved.system_prompt =
        bamboo_engine::runtime::context::upsert_workspace_prompt_context_with_source(
            &resolved.system_prompt,
            resolved.workspace_path.as_deref(),
            binding_status,
            workspace_source,
        );
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
        &ctx.workspace_resolver,
    );
    let session_id = session.id.clone();
    if let Some(config) = ctx.permission_config.as_ref() {
        if let Some(workspace) = session.workspace.as_ref() {
            config.register_session_workspace(session_id.clone(), workspace.clone());
        }
        record_bamboo_runtime_permission_metadata(&mut session, config.as_ref())
            .map_err(|error| error.to_string())?;
    }

    // #73: a scheduled run has no interactive human approver — mark the root so
    // its sub-agents (which inherit the flag) decide gated actions with the
    // off-loop model-reviewer locally instead of escalating to an absent human,
    // which would 300s-deny.
    session
        .agent_runtime_state
        .get_or_insert_with(bamboo_domain::AgentRuntimeState::default)
        .no_human_approver = true;

    let mut prompt_hook_block = None;
    if let Some(user_index) = session
        .messages
        .iter()
        .rposition(|message| matches!(message.role, Role::User))
    {
        let raw_prompt = session.messages[user_index].content.clone();
        match crate::lifecycle_hooks::apply_user_prompt_submit_hooks(
            &resolved.lifecycle_hooks,
            ctx.app_data_dir.clone(),
            &mut session,
            &raw_prompt,
        )
        .await
        {
            Ok(prompt) => session.messages[user_index].content = prompt,
            Err(reason) => {
                session.messages.remove(user_index);
                session.set_last_run_status("error");
                session.set_last_run_error(reason.clone());
                prompt_hook_block = Some(reason);
            }
        }
    }

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

    if let Some(reason) = prompt_hook_block {
        return Err(format!(
            "UserPromptSubmit hook blocked scheduled run: {reason}"
        ));
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

    // Reserve the shared runner and router before publishing any relay or
    // execution-specific state.
    let execution_reservation = match reserve_session_execution(
        &ctx.agent,
        &ctx.agent_runners,
        &ctx.session_event_senders,
        &session_id,
        &session_tx,
    )
    .await
    {
        SessionExecutionReserveOutcome::Reserved(reservation) => reservation,
        SessionExecutionReserveOutcome::AlreadyRunning { .. } => {
            return Ok(ScheduleRunLifecycleResult::Terminal(
                ScheduleRunStatus::Skipped,
            ));
        }
    };

    // Always-on relay (the critical gap this closes): a scheduled/headless
    // run has no SSE/WS client subscribed at start — often ever — so nothing
    // used to spawn a notification relay for it, and approval/clarification/
    // context/completion events for scheduled sessions never classified into
    // notifications. Idempotent (`try_begin_relay`), so this harmlessly races
    // a client that later opens the session's live stream.
    crate::app_state::session_events::ensure_notification_relay(
        &ctx.notification_relay,
        &session_id,
        session_tx.clone(),
    );

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
    let schedule_name_for_notify = job.schedule_name.clone();
    let notification_relay_for_hook = ctx.notification_relay.clone();

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

            // Owner notification, enriched with the schedule's name and the
            // final assistant message (see `notify_schedule_run_outcome`'s
            // doc comment for why this can never double-fire alongside the
            // always-on relay's generic classification of the same run's
            // raw `AgentEvent::Complete`/`Error`). Placed AFTER the failure
            // marker above is appended to `session.messages`, so a failed
            // run's body is that marker's text via `final_assistant_excerpt`.
            let notify_title = schedule_run_title(&schedule_name_for_notify, outcome.success);
            let notify_body = final_assistant_excerpt(&session.messages).unwrap_or_else(|| {
                if outcome.success {
                    "Run completed.".to_string()
                } else {
                    format!(
                        "Run failed: {}",
                        outcome.error.as_deref().unwrap_or("unknown error")
                    )
                }
            });
            notify_schedule_run_outcome(
                &notification_relay_for_hook,
                &log_session_id,
                outcome.success,
                notify_title,
                notify_body,
            )
            .await;

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
        execution_reservation,
        tools_override: Some(ctx.tools.clone()),
        provider_override: None,
        model_roster: resolved.model_roster.clone(),
        reasoning_effort: resolved.reasoning_effort,
        reasoning_effort_source: "schedule".to_string(),
        auxiliary_model_resolver: Some(auxiliary_model_resolver),
        // Scheduled runs use the per-run disabled snapshot (#136 lives on the
        // interactive agent path; a scheduled task is a discrete run).
        disabled_filter_resolver: None,
        disabled_tools: None,
        disabled_skill_ids: None,
        selected_skill_ids: None,
        selected_skill_mode: None,
        mpsc_tx,
        image_fallback: None,
        gold_config: resolved.gold_config.clone(),
        // Guardian review is not wired into the schedule path for now.
        guardian_config: None,
        guardian_spawner: None,
        // No bash self-resume hook on the schedule path: the end-of-turn bash
        // suspend gate is therefore inert here (it requires a wired hook).
        // Because the loop can't resume a backgrounded shell, the Bash tool's
        // auto path detects this (can_async_resume == false, derived from
        // hook+persistence) and stays purely synchronous — a long command on
        // the default path blocks to its timeout rather than promoting to an
        // orphaned background shell whose output this loop could never await
        // (issue #84, phase 2d). An explicitly backgrounded shell
        // (`run_in_background: true`) still runs detached and stays readable via
        // BashOutput; no strand can occur because the gate refuses to suspend
        // without the hook.
        bash_resume_hook: None,
        // Hook-less loop: no suspend/resume machinery, so stay push-free too
        // (consistent with `can_async_resume: false` on this path).
        bash_completion_sink: None,
        app_data_dir: ctx.app_data_dir.clone(),
        // Scheduled runs have no per-request override channel; the
        // config-level default (issue #221) still applies.
        run_budget: None,
        runners: ctx.agent_runners.clone(),
        sessions_cache: ctx.sessions_cache.clone(),
        on_complete: Some(on_complete),
        // Scheduled runs are root sessions — no parent to wake.
        child_completion_handler: None,
    });

    Ok(ScheduleRunLifecycleResult::BackgroundExecutionInProgress)
}

fn validate_schedule_project_at_fire(
    store: &bamboo_projects::ProjectStore,
    run_config: &ScheduleRunConfig,
) -> Result<(), String> {
    let Some(project_id) = run_config.project_id.as_ref() else {
        return Ok(());
    };
    match store.get(project_id) {
        Ok(project) if project.status == bamboo_domain::ProjectStatus::Active => Ok(()),
        Ok(_) => Err(format!(
            "schedule Project is archived at execution time: {project_id}"
        )),
        Err(error) => Err(format!(
            "schedule Project is unavailable at execution time ({project_id}): {error}"
        )),
    }
}

/// Build a [`ScheduleContext`] with server-specific config resolution.
///
/// Callers should prefer this over constructing `ScheduleContext` directly
/// to ensure the `resolve_run_config` callback correctly reads Config and
/// prompt defaults.
pub fn build_schedule_context(
    base: ScheduleContext,
    config: std::sync::Arc<tokio::sync::RwLock<bamboo_llm::Config>>,
    provider_registry: Arc<bamboo_llm::ProviderRegistry>,
) -> ScheduleContext {
    ScheduleContext {
        schedule_store: base.schedule_store,
        agent: base.agent,
        tools: base.tools,
        permission_config: base.permission_config,
        sessions_cache: base.sessions_cache,
        agent_runners: base.agent_runners,
        session_event_senders: base.session_event_senders,
        account_feed_inbox: base.account_feed_inbox,
        app_data_dir: base.app_data_dir,
        trigger_engine: base.trigger_engine,
        project_store: base.project_store,
        workspace_resolver: base.workspace_resolver,
        persistence: base.persistence,
        notification_relay: base.notification_relay,
        resolve_run_config: std::sync::Arc::new(move |job: &ScheduleRunJob| {
            resolve_run_config_from_config(job, &config, &provider_registry)
        }),
    }
}

fn resolve_run_config_from_config(
    job: &ScheduleRunJob,
    config: &std::sync::Arc<tokio::sync::RwLock<bamboo_llm::Config>>,
    provider_registry: &Arc<bamboo_llm::ProviderRegistry>,
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
                .map(|path| bamboo_config::paths::path_to_display_string(&path))
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

    let model_roster =
        bamboo_engine::ModelRoster::from_areas(Some(model), provider_name, provider_type, areas);

    ResolvedRunConfig {
        model_roster,
        reasoning_effort,
        gold_config: bamboo_engine::model_config_helper::resolve_gold_config(
            &config_snapshot,
            None,
        ),
        system_prompt,
        base_system_prompt: base_system_prompt.to_string(),
        workspace_path,
        lifecycle_hooks: config_snapshot.lifecycle_hooks.clone(),
    }
}

#[cfg(test)]
mod build_context_tests {
    use super::ScheduleRunJob;
    use super::{resolve_run_config_from_config, validate_schedule_project_at_fire};
    use bamboo_config::DefaultsConfig;
    use bamboo_config::{OpenAIConfig, ProviderConfigs};
    use bamboo_domain::{ProviderModelRef, ScheduleRunConfig};
    use bamboo_llm::{Config, ProviderRegistry};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

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
    fn archived_project_is_rejected_when_persisted_schedule_fires() {
        let dir = tempfile::tempdir().unwrap();
        let store = bamboo_projects::ProjectStore::open(dir.path()).unwrap();
        let project = store.create("Scheduled", None).unwrap();
        let run_config = ScheduleRunConfig {
            project_id: Some(project.id.clone()),
            ..ScheduleRunConfig::default()
        };
        assert!(validate_schedule_project_at_fire(&store, &run_config).is_ok());
        store.archive(&project.id, project.revision).unwrap();
        let error = validate_schedule_project_at_fire(&store, &run_config).unwrap_err();
        assert!(error.contains("archived at execution time"));
    }

    #[test]
    fn resolve_run_config_from_config_prefers_fast_model() {
        let config = test_config! {
            provider: "openai".to_string(),
            defaults: None,
            features: bamboo_config::FeatureFlags {
                provider_model_ref: false,
                ..Default::default()
            },
            providers: ProviderConfigs {
                openai: Some(OpenAIConfig {
                    api_key: "test".to_string(),
                    api_key_from_env: false,
                    api_key_encrypted: None,
                    credential_ref: None,
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
        let config = test_config! {
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
            features: bamboo_config::FeatureFlags {
                provider_model_ref: true,
                ..Default::default()
            },
            providers: ProviderConfigs::default(),
        };

        let registry = Arc::new(ProviderRegistry::new(
            Default::default(),
            "openai".to_string(),
        ));
        let resolved =
            resolve_run_config_from_config(&test_job(), &Arc::new(RwLock::new(config)), &registry);
        assert_eq!(resolved.model_roster.model.as_deref(), Some("gpt-chat"));
    }

    #[test]
    fn resolve_run_config_from_config_snapshots_lifecycle_hooks_for_scheduled_runs() {
        let lifecycle_hooks = bamboo_config::LifecycleHooksConfig {
            enabled: true,
            session_start: vec![bamboo_config::LifecycleHookGroup {
                enabled: true,
                matcher: None,
                hooks: vec![bamboo_config::LifecycleHookHandler::command(
                    "printf schedule-start",
                    bamboo_config::DEFAULT_LIFECYCLE_HOOK_TIMEOUT_MS,
                )],
            }],
            ..Default::default()
        };
        let config = test_config! {
            lifecycle_hooks: lifecycle_hooks.clone(),
        };
        let registry = Arc::new(ProviderRegistry::new(
            Default::default(),
            "openai".to_string(),
        ));

        let resolved =
            resolve_run_config_from_config(&test_job(), &Arc::new(RwLock::new(config)), &registry);

        assert_eq!(resolved.lifecycle_hooks, lifecycle_hooks);
    }
}

#[cfg(test)]
mod notify_outcome_tests {
    use super::*;
    use crate::app_state::session_events::NotificationRelayDeps;
    use crate::app_state::watchers::SessionWatchers;
    use std::collections::HashMap;
    use std::time::Duration;
    use tokio::sync::RwLock as TokioRwLock;

    fn relay_deps() -> (NotificationRelayDeps, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let notification_service = Arc::new(bamboo_notification::NotificationService::new(
            dir.path().join("prefs.json"),
        ));
        let deps = NotificationRelayDeps {
            notification_service,
            session_event_senders: Arc::new(TokioRwLock::new(HashMap::new())),
            session_watchers: SessionWatchers::new(),
            config: Arc::new(TokioRwLock::new(bamboo_llm::Config::default())),
        };
        (deps, dir)
    }

    #[test]
    fn truncate_chars_appends_ellipsis_only_when_cut() {
        assert_eq!(truncate_chars("short", 10), "short");
        let truncated = truncate_chars(&"x".repeat(300), SCHEDULE_NOTIFY_BODY_MAX);
        assert_eq!(truncated.chars().count(), SCHEDULE_NOTIFY_BODY_MAX + 1);
        assert!(truncated.ends_with('…'));
    }

    #[test]
    fn schedule_run_title_names_the_schedule_and_outcome() {
        assert_eq!(
            schedule_run_title("nightly", true),
            "Schedule 'nightly' completed"
        );
        assert_eq!(
            schedule_run_title("nightly", false),
            "Schedule 'nightly' failed"
        );
    }

    #[test]
    fn final_assistant_excerpt_finds_the_last_non_empty_assistant_message() {
        let messages = vec![
            Message::user("hi"),
            Message::assistant("first reply", None),
            Message::assistant("   ", None), // blank — skipped
            Message::assistant("final reply", None),
        ];
        assert_eq!(
            final_assistant_excerpt(&messages).as_deref(),
            Some("final reply")
        );
    }

    #[test]
    fn final_assistant_excerpt_truncates_long_content() {
        let long = "x".repeat(300);
        let messages = vec![Message::assistant(long, None)];
        let excerpt = final_assistant_excerpt(&messages).unwrap();
        assert_eq!(excerpt.chars().count(), SCHEDULE_NOTIFY_BODY_MAX + 1);
        assert!(excerpt.ends_with('…'));
    }

    #[test]
    fn final_assistant_excerpt_none_when_no_assistant_message() {
        let messages = vec![Message::user("hi")];
        assert!(final_assistant_excerpt(&messages).is_none());
    }

    /// The manager-level no-double-fire guarantee, exercised through
    /// [`notify_schedule_run_outcome`] itself (not just the underlying
    /// `NotificationService` primitive it wraps): the generic relay path
    /// (raw `AgentEvent::Complete`, classified via
    /// `NotificationService::notify`) firing FIRST must dedup away a
    /// subsequent schedule-level enrichment call for the same session — the
    /// scenario `ensure_notification_relay` (spawned before
    /// `spawn_session_execution` in `run_schedule_job`) races against this
    /// hook.
    #[tokio::test]
    async fn notify_schedule_run_outcome_is_deduped_by_a_prior_generic_complete() {
        let (deps, _dir) = relay_deps();
        let (tx, mut rx) = broadcast::channel(16);
        deps.session_event_senders
            .write()
            .await
            .insert("sess-1".to_string(), tx.clone());

        // Simulate the always-on relay having already classified the raw
        // AgentEvent::Complete for this session (inserts the shared dedup
        // key into the service's window).
        let relay_fired = deps.notification_service.notify(
            "sess-1",
            &AgentEvent::Complete {
                usage: bamboo_agent_core::TokenUsage {
                    prompt_tokens: 1,
                    completion_tokens: 1,
                    total_tokens: 2,
                },
            },
        );
        assert!(relay_fired.is_some());

        notify_schedule_run_outcome(
            &deps,
            "sess-1",
            true,
            "Schedule 'nightly' completed".to_string(),
            "All done.".to_string(),
        )
        .await;

        // The manager-level call was deduped — it must not have broadcast a
        // second notification onto the session channel.
        let outcome = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await;
        assert!(
            outcome.is_err(),
            "deduped schedule-level notification must not broadcast a second event"
        );
    }

    /// Same guarantee, exercised in the opposite call order: the
    /// schedule-level enrichment fires first and wins; a subsequent generic
    /// relay classification for the same raw event is what gets deduped.
    #[tokio::test]
    async fn notify_schedule_run_outcome_first_dedups_a_later_generic_complete() {
        let (deps, _dir) = relay_deps();

        notify_schedule_run_outcome(
            &deps,
            "sess-2",
            true,
            "Schedule 'nightly' completed".to_string(),
            "All done.".to_string(),
        )
        .await;

        let relay_fired = deps.notification_service.notify(
            "sess-2",
            &AgentEvent::Complete {
                usage: bamboo_agent_core::TokenUsage {
                    prompt_tokens: 1,
                    completion_tokens: 1,
                    total_tokens: 2,
                },
            },
        );
        assert!(
            relay_fired.is_none(),
            "the later generic classification must be deduped away"
        );
    }
}
