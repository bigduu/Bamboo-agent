//! NeedsHuman/QuestionDialog → buttons/text replies → respond path (epic
//! #447's planned `approvals.rs`, issue #458).
//!
//! Three concerns live here:
//! - Rendering a [`PendingAsk`][render_pending_ask_type] as an outbound
//!   message (buttons when the platform supports them, always ALSO a
//!   numbered text list — text replies are first-class on every platform).
//! - Matching an inbound text reply or button `callback_data` against a
//!   [`ParkedAsk`], including the binary-ask keyword mapping
//!   (允许/yes/allow vs deny/no).
//! - The [`Responder`] seam: the ONLY resolution path is
//!   `bamboo_engine::session_app::respond::submit_pending_response` followed
//!   by `resume::resume_session_execution` — exactly what
//!   `POST /sessions/{id}/respond` does. [`EngineResponder`] is the
//!   production implementation (in-proc, via [`super::bridge::ConnectContext`]);
//!   tests inject a fake instead of standing up a full `AppState`.
//!
//! [render_pending_ask_type]: crate::connect::render::PendingAsk

use std::sync::Arc;

use tokio::sync::broadcast;

use bamboo_agent_core::tools::{ToolCall, ToolExecutionContext};
use bamboo_agent_core::{AgentEvent, Session};
use bamboo_engine::execution::{
    create_event_forwarder, get_or_create_event_sender, reserve_session_execution,
    SessionExecutionReserveOutcome,
};
use bamboo_engine::runtime::execution::agent_spawn::{
    spawn_session_execution, SessionExecutionArgs,
};
use bamboo_engine::session_app::approval_replay::{
    refresh_approval_replay_posture, ApprovalReplayDecision,
};
use bamboo_engine::session_app::execute::consume_pending_clarification_resume;
use bamboo_engine::session_app::resolution::resolve_resume_config_snapshot;
use bamboo_engine::session_app::respond::{
    acquire_pending_response_guard, inspect_pending_response_guarded,
    submit_pending_response_checked_guarded, validate_pending_response,
    PERMISSION_REEXECUTE_METADATA_KEY,
};
use bamboo_engine::session_app::resume::{ResumeExecutionPort, ResumeSpawnRequest};
use bamboo_engine::session_app::types::RespondInput;
use bamboo_engine::{ModelRoster, RoleModel};

use super::bridge::ConnectContext;
use super::platform::{Button, OutboundMessage, Platform, PlatformResult, ReplyCtx};
use super::render::PendingAsk;

/// Longest a button's visible label is allowed to be — Telegram (and most IM
/// platforms) truncate/reject very long inline-button text, so keep it well
/// under any known limit.
const BUTTON_LABEL_MAX_CHARS: usize = 48;

// ---------------------------------------------------------------------------
// ParkedAsk — the bridge's one-ask-per-chat state
// ---------------------------------------------------------------------------

/// A pending question rendered to a chat and awaiting resolution (button
/// press or text reply). One per chat at a time (issue #458: "one parked ask
/// per chat — session serializes asks").
#[derive(Debug, Clone)]
pub struct ParkedAsk {
    /// Short nonce embedded in every button's `callback_data`
    /// (`"{nonce}:{option_index}"`). Validated on every callback so
    /// forged/stale data is ignored.
    pub nonce: String,
    pub session_id: String,
    /// Exact durable identity used as the response CAS guard. Legacy live
    /// events are reconciled against session persistence before a `ParkedAsk`
    /// can be constructed, so a new Connect client never submits an
    /// unguarded clarification answer.
    pub tool_call_id: String,
    pub tool_name: String,
    pub question: String,
    pub options: Vec<String>,
    pub allow_custom: bool,
}

impl ParkedAsk {
    pub fn new(nonce: String, session_id: String, ask: &PendingAsk) -> Option<Self> {
        Some(Self {
            nonce,
            session_id,
            tool_call_id: ask.tool_call_id.clone()?,
            tool_name: ask.tool_name.clone(),
            question: ask.question.clone(),
            options: ask.options.clone(),
            allow_custom: ask.allow_custom,
        })
    }
}

/// A short, hard-to-guess nonce for one parked ask. Not cryptographically
/// load-bearing on its own (it's paired with per-chat scoping + a single
/// live ask at a time) — just enough entropy that a stale/forged
/// `callback_data` from a different ask/session won't collide by accident.
pub fn new_nonce() -> String {
    let raw = uuid::Uuid::new_v4().to_string();
    raw.split('-').next().unwrap_or(&raw).to_string()
}

// ---------------------------------------------------------------------------
// Rendering an ask
// ---------------------------------------------------------------------------

fn truncate_label(text: &str) -> String {
    if text.chars().count() <= BUTTON_LABEL_MAX_CHARS {
        return text.to_string();
    }
    let mut out: String = text.chars().take(BUTTON_LABEL_MAX_CHARS - 1).collect();
    out.push('…');
    out
}

/// Format the ask's question + a numbered option list (text replies remain
/// first-class even when buttons are ALSO rendered).
fn format_ask_text(ask: &ParkedAsk) -> String {
    let mut text = ask.question.clone();
    if !ask.options.is_empty() {
        text.push_str("\n\n");
        for (index, option) in ask.options.iter().enumerate() {
            text.push_str(&format!("{}. {}\n", index + 1, option));
        }
    }
    if ask.allow_custom {
        text.push_str("\n(or reply with your own answer)");
    }
    text
}

/// Render `ask` to the chat: inline buttons (one per option, `callback_data =
/// "{nonce}:{index}"`) when `buttons_capable`, always alongside the numbered
/// text list — per issue #458, buttons are an enhancement, never a
/// requirement. Returns the platform error (if the send failed) so the
/// caller can log it; rendering failure does not itself invalidate the
/// parked ask (a text reply can still resolve it).
pub async fn render_ask(
    platform: &Arc<dyn Platform>,
    reply_ctx: &ReplyCtx,
    ask: &ParkedAsk,
    buttons_capable: bool,
) -> PlatformResult<()> {
    let text = format_ask_text(ask);
    let outbound = if buttons_capable && !ask.options.is_empty() {
        let rows: Vec<Vec<Button>> = ask
            .options
            .iter()
            .enumerate()
            .map(|(index, option)| {
                vec![Button::new(
                    truncate_label(option),
                    format!("{}:{index}", ask.nonce),
                )]
            })
            .collect();
        OutboundMessage::text(text).with_buttons(rows)
    } else {
        OutboundMessage::text(text)
    };
    platform.reply(reply_ctx, outbound).await.map(|_| ())
}

/// Render a legacy/external clarification that cannot be matched to a durable
/// pending tool call. It remains fully inspectable, but deliberately has no
/// buttons or reply instructions and is never registered as answerable chat
/// state.
pub async fn render_read_only_ask(
    platform: &Arc<dyn Platform>,
    reply_ctx: &ReplyCtx,
    ask: &PendingAsk,
    reason: &str,
) -> PlatformResult<()> {
    let mut text = ask.question.clone();
    if !ask.options.is_empty() {
        text.push_str("\n\n");
        for (index, option) in ask.options.iter().enumerate() {
            text.push_str(&format!("{}. {}\n", index + 1, option));
        }
    }
    text.push_str("\n(Response unavailable: ");
    text.push_str(reason);
    text.push(')');
    platform
        .reply(reply_ctx, OutboundMessage::text(text))
        .await
        .map(|_| ())
}

// ---------------------------------------------------------------------------
// Matching a text reply / callback against a ParkedAsk
// ---------------------------------------------------------------------------

const AFFIRMATIVE_KEYWORDS: &[&str] = &[
    "允许", "同意", "确定", "是", "yes", "allow", "approve", "ok",
];
/// "stay" comes from plan-mode's decline phrasing — ExitPlanMode's negative
/// option is literally "Stay in plan mode" (see
/// `session_app::respond::is_exit_plan_mode_approved`), so a user typing
/// "stay" declines the plan approval. Safe to keep in this fallback list
/// because [`match_text_answer`] tries EXACT (case-insensitive) option-text
/// matching BEFORE the keyword fallback: an ask whose positive option is
/// literally titled "Stay" resolves on the exact match and never reaches
/// here.
const NEGATIVE_KEYWORDS: &[&str] = &["拒绝", "不", "否", "no", "deny", "reject", "stay"];

fn classify_intent(text: &str) -> Option<bool> {
    let lower = text.trim().to_lowercase();
    if AFFIRMATIVE_KEYWORDS.iter().any(|keyword| lower == *keyword) {
        return Some(true);
    }
    if NEGATIVE_KEYWORDS.iter().any(|keyword| lower == *keyword) {
        return Some(false);
    }
    None
}

/// "First-affirmative mapping": prefer an option whose OWN text already
/// reads as affirmative/negative (e.g. "Approve" / "Deny"); for a plain
/// 2-option ask with no such wording, fall back to treating the first option
/// as the affirmative one.
fn pick_option_by_intent(options: &[String], affirmative: bool) -> Option<String> {
    let keywords: &[&str] = if affirmative {
        AFFIRMATIVE_KEYWORDS
    } else {
        NEGATIVE_KEYWORDS
    };
    if let Some(option) = options.iter().find(|option| {
        let lower = option.to_lowercase();
        keywords.iter().any(|keyword| lower.contains(keyword))
    }) {
        return Some(option.clone());
    }
    if options.len() == 2 {
        return Some(if affirmative {
            options[0].clone()
        } else {
            options[1].clone()
        });
    }
    None
}

/// Match a text reply against `ask`, returning the answer to submit, or
/// `None` when it doesn't resolve the ask at all (issue #458: a non-matching
/// text on a CLOSED ask — no free text allowed — falls through to the
/// caller's normal busy-queue handling instead of being submitted as a
/// doomed-to-fail answer).
///
/// Tried in order: 1-based numeric option index, exact (case-insensitive)
/// option text, then — for a closed (non-`allow_custom`) ask — the
/// affirmative/negative keyword mapping. An OPEN ask (`allow_custom`) always
/// matches: any non-empty text IS the answer, verbatim (matching
/// `validate_pending_response`'s server-side rule).
pub fn match_text_answer(ask: &ParkedAsk, text: &str) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(index) = trimmed.parse::<usize>() {
        if index >= 1 && index <= ask.options.len() {
            return Some(ask.options[index - 1].clone());
        }
    }
    if let Some(option) = ask
        .options
        .iter()
        .find(|option| option.eq_ignore_ascii_case(trimmed))
    {
        return Some(option.clone());
    }
    if !ask.allow_custom {
        if let Some(intent) = classify_intent(trimmed) {
            if let Some(option) = pick_option_by_intent(&ask.options, intent) {
                return Some(option);
            }
        }
    }
    if ask.allow_custom {
        return Some(trimmed.to_string());
    }
    None
}

/// Match a button press's `callback_data` (`"{nonce}:{index}"`) against
/// `ask`. Returns `None` for anything that doesn't EXACTLY match the parked
/// nonce and a valid option index — forged/stale data (issue #458: "always
/// answerCallbackQuery, even stale" — the caller acks regardless, but never
/// forwards a non-match as an answer).
pub fn match_callback_data(ask: &ParkedAsk, data: &str) -> Option<String> {
    let (nonce, index_str) = data.split_once(':')?;
    if nonce != ask.nonce {
        return None;
    }
    let index: usize = index_str.parse().ok()?;
    ask.options.get(index).cloned()
}

// ---------------------------------------------------------------------------
// Responder — the resolution seam
// ---------------------------------------------------------------------------

/// What happened after submitting an answer and attempting to resume.
pub enum RespondAndResumeOutcome {
    /// Execution resumed; `receiver` was subscribed BEFORE the resume was
    /// triggered, so the caller can keep rendering without missing events.
    Resumed(broadcast::Receiver<AgentEvent>),
    /// The answer was recorded, but nothing (more) is running — e.g. the
    /// runner slot was already taken by a concurrent run, or the session
    /// vanished between answering and resuming. `reason` is a short,
    /// user-facing explanation.
    NotResumed(String),
}

/// Error submitting an answer (mirrors `bamboo_engine::session_app::errors::RespondError`,
/// decoupled so `connect` doesn't leak that error type through its public
/// surface).
#[derive(Debug, thiserror::Error)]
pub enum ResponderError {
    #[error("session not found")]
    NotFound,
    #[error("no pending question waiting for a response")]
    NoPendingQuestion,
    #[error("the pending question changed; this action has expired")]
    PendingQuestionChanged,
    #[error("invalid response: {0}")]
    InvalidResponse(String),
    #[error("{0}")]
    Other(String),
}

/// The bridge's resolution seam (issue #458: "Design a small Responder seam
/// on the bridge so tests inject a fake instead of full AppState"). The ONLY
/// production implementation ([`EngineResponder`]) routes through
/// `submit_pending_response` + `resume_session_execution` — the exact same
/// use-case functions `POST /sessions/{id}/respond` calls — never a parallel
/// path.
#[async_trait::async_trait]
pub trait Responder: Send + Sync {
    async fn respond_and_resume(
        &self,
        session_id: &str,
        expected_tool_call_id: Option<&str>,
        answer: String,
    ) -> Result<RespondAndResumeOutcome, ResponderError>;
}

fn map_respond_error(error: bamboo_engine::session_app::errors::RespondError) -> ResponderError {
    use bamboo_engine::session_app::errors::RespondError;
    match error {
        RespondError::NotFound(_) => ResponderError::NotFound,
        RespondError::NoPendingQuestion => ResponderError::NoPendingQuestion,
        RespondError::PendingQuestionMismatch { .. } => ResponderError::PendingQuestionChanged,
        RespondError::InvalidResponse(message) => ResponderError::InvalidResponse(message),
        other => ResponderError::Other(other.to_string()),
    }
}

/// Production [`Responder`]: submits through `submit_pending_response`
/// (`SessionAccess` is implemented directly by `SessionRepository`, no
/// `AppState` wrapper needed), applies any permission grants the answer
/// implied (mirrors `handlers::agent::respond::handlers::submit`), then
/// resumes via [`ConnectResumePort`] — the connect-scoped
/// `ResumeExecutionPort` implementation that spawns through the same
/// crate-agnostic `spawn_session_execution` the bridge already uses for a
/// fresh prompt (`bridge::ConnectBridge::run_prompt`), including re-running a
/// gated tool call that was only a placeholder while awaiting approval.
pub struct EngineResponder {
    ctx: ConnectContext,
}

impl EngineResponder {
    pub fn new(ctx: ConnectContext) -> Self {
        Self { ctx }
    }
}

#[async_trait::async_trait]
impl Responder for EngineResponder {
    async fn respond_and_resume(
        &self,
        session_id: &str,
        expected_tool_call_id: Option<&str>,
        answer: String,
    ) -> Result<RespondAndResumeOutcome, ResponderError> {
        let response_guard = acquire_pending_response_guard(session_id).await;
        let current =
            inspect_pending_response_guarded(&self.ctx.session_repo, session_id, &response_guard)
                .await
                .map_err(map_respond_error)?
                .ok_or(ResponderError::NotFound)?;
        let pending = current
            .pending_question
            .as_ref()
            .ok_or(ResponderError::NoPendingQuestion)?;
        if expected_tool_call_id.is_some_and(|expected| expected != pending.tool_call_id) {
            return Err(ResponderError::PendingQuestionChanged);
        }
        validate_pending_response(pending, &answer).map_err(ResponderError::InvalidResponse)?;

        let port = ConnectResumePort {
            ctx: self.ctx.clone(),
        };
        let handoff = bamboo_engine::session_app::resume::reserve_response_resume_handoff(
            &port,
            session_id,
            std::time::Duration::from_secs(15),
        )
        .await
        .map_err(|_| {
            ResponderError::Other(
                "the suspending run is still finalizing; answer not consumed".to_string(),
            )
        })?;
        // Subscribe and resolve async configuration before the response CAS.
        // This intentionally gives one response a stable config snapshot;
        // concurrent config changes apply to later requests/runs. After the
        // answer commits, the reserved successor is transferred to a detached
        // owner synchronously, so callback cancellation cannot strand it.
        let receiver = handoff.subscribe();
        let config_snapshot = self.ctx.config.read().await.clone();
        let input = RespondInput {
            session_id: session_id.to_string(),
            user_response: answer,
            model: None,
            model_ref: None,
            provider: None,
            reasoning_effort: None,
        };

        let submission = submit_pending_response_checked_guarded(
            &self.ctx.session_repo,
            input,
            expected_tool_call_id.map(str::to_string),
            &response_guard,
        )
        .await;
        let (session, _submitted_answer, plan_mode_transition, permission_grants) = match submission
        {
            Ok(submission) => submission,
            Err(error) => {
                handoff.abandon().await;
                return Err(map_respond_error(error));
            }
        };

        // Mirrors `handlers::agent::respond::handlers::submit`: record any
        // permission grant so the resumed re-execution of the gated tool
        // passes the check without re-prompting.
        for (perm_type, resource) in &permission_grants {
            if let Some(request_id) = session.metadata.get(PERMISSION_REEXECUTE_METADATA_KEY) {
                self.ctx.permission_checker.grant_once(
                    session_id,
                    request_id,
                    *perm_type,
                    resource.clone(),
                );
            }
        }

        if let Some(event) = plan_mode_transition_event(session_id, plan_mode_transition.as_ref()) {
            handoff.publish_event(event);
        }

        let resume_config = resolve_resume_config_snapshot(
            &config_snapshot,
            &self.ctx.provider_registry,
            &session,
            None,
        );

        let outcome = bamboo_engine::session_app::resume::resume_session_execution_with_handoff(
            &port,
            session_id,
            session,
            resume_config,
            handoff,
        )
        .await;
        drop(response_guard);

        match outcome {
            bamboo_engine::session_app::types::ResumeOutcome::Started { .. } => {
                Ok(RespondAndResumeOutcome::Resumed(receiver))
            }
            bamboo_engine::session_app::types::ResumeOutcome::AlreadyRunning { .. } => Ok(
                RespondAndResumeOutcome::NotResumed("this session is already running".to_string()),
            ),
            bamboo_engine::session_app::types::ResumeOutcome::Completed => Ok(
                RespondAndResumeOutcome::NotResumed("nothing left to resume".to_string()),
            ),
            bamboo_engine::session_app::types::ResumeOutcome::NotFound => Ok(
                RespondAndResumeOutcome::NotResumed("session no longer exists".to_string()),
            ),
        }
    }
}

fn plan_mode_transition_event(
    session_id: &str,
    transition: Option<&bamboo_engine::session_app::respond::PlanModeTransition>,
) -> Option<AgentEvent> {
    use bamboo_engine::session_app::respond::PlanModeTransition;
    transition.map(|transition| match transition {
        PlanModeTransition::Entered {
            reason,
            pre_permission_mode,
            entered_at,
            status,
            plan_file_path,
        } => AgentEvent::PlanModeEntered {
            session_id: session_id.to_string(),
            reason: reason.clone(),
            pre_permission_mode: pre_permission_mode.clone(),
            entered_at: *entered_at,
            status: *status,
            plan_file_path: plan_file_path.clone(),
        },
        PlanModeTransition::Exited {
            approved,
            restored_mode,
            plan,
        } => AgentEvent::PlanModeExited {
            session_id: session_id.to_string(),
            approved: *approved,
            restored_mode: restored_mode.clone(),
            plan: plan.clone(),
        },
    })
}

// ---------------------------------------------------------------------------
// ConnectResumePort — ResumeExecutionPort for connect-bridged sessions
// ---------------------------------------------------------------------------

/// [`ResumeExecutionPort`] backed by [`ConnectContext`] instead of `AppState`
/// — the connect-scoped counterpart of the server's
/// `app_state::resume_adapter::AppStateResumeRef`. Spawns through the
/// crate-agnostic `spawn_session_execution` (matching
/// `bridge::ConnectBridge::run_prompt`'s fresh-prompt spawn exactly: same
/// tools/agent/model-roster resolution, no guardian/bash-resume-hook — those
/// remain a later phase, same as the fresh-prompt path), rather than the
/// server handler layer's `spawn_agent_execution` (which pulls in
/// `AppState`-specific wiring connect deliberately doesn't use).
struct ConnectResumePort {
    ctx: ConnectContext,
}

#[async_trait::async_trait]
impl ResumeExecutionPort for ConnectResumePort {
    async fn load_session(&self, session_id: &str) -> Option<Session> {
        self.ctx.session_repo.load_merged(session_id).await
    }

    async fn save_and_cache_session(&self, session: &mut Session) {
        self.ctx.session_repo.save_and_cache(session).await;
    }

    async fn reserve_session_execution(
        &self,
        session_id: &str,
        event_sender: &broadcast::Sender<AgentEvent>,
    ) -> SessionExecutionReserveOutcome {
        reserve_session_execution(
            &self.ctx.agent,
            &self.ctx.agent_runners,
            &self.ctx.session_event_senders,
            session_id,
            event_sender,
        )
        .await
    }

    async fn get_or_create_event_sender(&self, session_id: &str) -> broadcast::Sender<AgentEvent> {
        get_or_create_event_sender(&self.ctx.session_event_senders, session_id).await
    }

    fn dispatch_resume_execution(
        &self,
        request: ResumeSpawnRequest,
    ) -> Result<(), ResumeSpawnRequest> {
        let owner = ConnectResumePort {
            ctx: self.ctx.clone(),
        };
        tokio::spawn(async move {
            ResumeExecutionPort::spawn_resume_execution(&owner, request).await;
        });
        Ok(())
    }

    async fn spawn_resume_execution(&self, request: ResumeSpawnRequest) {
        let ResumeSpawnRequest {
            session_id,
            mut session,
            mut execution_reservation,
            event_sender,
            config,
        } = request;
        if let Err(error) = execution_reservation.ensure_registered().await {
            tracing::warn!(
                %session_id,
                run_id = %execution_reservation.run_id(),
                %error,
                "cannot resume connect session without exact router ownership"
            );
            return;
        }

        let model = session.model.clone();
        let reasoning_effort = session.reasoning_effort;
        let model_roster = ModelRoster {
            model: Some(model),
            provider_name: Some(config.provider_name.clone()),
            provider_type: config.provider_type.clone(),
            fast: RoleModel::from_parts(config.fast_model.clone(), None),
            background: RoleModel::from_parts(
                config.background_model.clone(),
                config.background_model_provider.clone(),
            ),
            summarization: RoleModel::from_parts(
                config.summarization_model.clone(),
                config.summarization_model_provider.clone(),
            ),
        };

        let (mpsc_tx, _forwarder_handle) = create_event_forwarder(
            session_id.clone(),
            execution_reservation.run_id().to_string(),
            event_sender,
            self.ctx.agent_runners.clone(),
            self.ctx.account_feed_inbox.clone(),
        );

        // If the user just approved a permission prompt, the gated tool call
        // was intercepted before it ran — its recorded result is only a
        // placeholder. Re-execute it for real now (the grant was already
        // applied to `ctx.permission_checker` in `EngineResponder`), write
        // the output back, then start the loop — mirrors
        // `app_state::resume_adapter::AppStateResumeRef::spawn_resume_execution`
        // exactly, minus the `AppState`-specific plumbing.
        let reexecute_tool_call_id = session
            .metadata
            .get(PERMISSION_REEXECUTE_METADATA_KEY)
            .cloned();

        let Some(reexecute_tool_call_id) = reexecute_tool_call_id else {
            consume_pending_clarification_resume(&mut session);
            spawn_session_execution(SessionExecutionArgs {
                agent: self.ctx.agent.clone(),
                session_id,
                session,
                execution_reservation,
                tools_override: Some(self.ctx.tools.clone()),
                provider_override: None,
                model_roster,
                reasoning_effort,
                reasoning_effort_source: "connect_resume".to_string(),
                auxiliary_model_resolver: None,
                disabled_filter_resolver: None,
                disabled_tools: Some(config.disabled_tools.clone()),
                disabled_skill_ids: Some(config.disabled_skill_ids.clone()),
                selected_skill_ids: None,
                selected_skill_mode: None,
                mpsc_tx,
                image_fallback: config.image_fallback.clone(),
                gold_config: config.gold_config.clone(),
                guardian_config: None,
                guardian_spawner: None,
                bash_resume_hook: None,
                bash_completion_sink: None,
                app_data_dir: self.ctx.app_data_dir.clone(),
                // No per-request override on this path; the config-level
                // default (issue #221) still applies.
                run_budget: None,
                runners: self.ctx.agent_runners.clone(),
                sessions_cache: self.ctx.session_repo.cache().clone(),
                on_complete: None,
                // Connect drives root sessions; a child finishing on this
                // path is backstopped by the child-wait watchdog (#546).
                child_completion_handler: None,
            });
            return;
        };

        let ctx = self.ctx.clone();
        tokio::spawn(async move {
            let mut session = session;

            if let Some(tool_call) = find_pending_tool_call(&session, &reexecute_tool_call_id) {
                let tool_name = tool_call.function.name.clone();
                let configured_mode = ctx
                    .permission_checker
                    .permission_config()
                    .map(|config| config.mode())
                    .unwrap_or_default();
                let decision = match refresh_approval_replay_posture(
                    ctx.session_repo.storage().as_ref(),
                    &mut session,
                    configured_mode,
                    &tool_name,
                )
                .await
                {
                    Ok(decision) => decision,
                    Err(error) => {
                        tracing::error!(
                            %session_id,
                            tool_call_id = %reexecute_tool_call_id,
                            %error,
                            "connect approval replay posture refresh failed closed"
                        );
                        return;
                    }
                };
                session.metadata.remove(PERMISSION_REEXECUTE_METADATA_KEY);

                let (content, success) = match decision {
                    ApprovalReplayDecision::BlockedByPlan(_) => (
                        format!(
                            "Plan mode blocked approved mutating tool '{tool_name}'; the stale approval was not executed"
                        ),
                        false,
                    ),
                    ApprovalReplayDecision::Execute(flags) => {
                        let executor = ctx.tools.clone();
                        let is_mutating = bamboo_tools::orchestrator::classify_tool(&tool_name)
                            == bamboo_tools::orchestrator::ToolMutability::Mutating;
                        let mut emitter = bamboo_tools::ToolEmitter::new(
                            &tool_call.id,
                            &tool_name,
                            is_mutating,
                        );
                        emitter.set_auto_approved(true);
                        let _ = mpsc_tx
                            .send(emitter.begin().clone().into_agent_event())
                            .await;
                        let exec_result = executor
                            .execute_with_context(
                                &tool_call,
                                ToolExecutionContext {
                                    session_id: Some(session.id.as_str()),
                                    tool_call_id: reexecute_tool_call_id.as_str(),
                                    event_tx: Some(&mpsc_tx),
                                    available_tool_schemas: None,
                                    bypass_permissions: flags.bypass_permissions,
                                    auto_approve_permissions: flags.auto_approve_permissions,
                                    plan_read_only: flags.plan_read_only,
                                    can_async_resume: false,
                                    bash_completion_sink: None,
                                    pre_parsed_args: None,
                                },
                            )
                            .await;

                        match exec_result {
                            Ok(tool_result) => {
                                let _ = mpsc_tx
                                    .send(
                                        emitter
                                            .finish(Some(
                                                "Re-executed after approval".to_string(),
                                            ))
                                            .clone()
                                            .into_agent_event(),
                                    )
                                    .await;
                                let _ = mpsc_tx
                                    .send(AgentEvent::ToolComplete {
                                        tool_call_id: tool_call.id.clone(),
                                        result: tool_result.clone(),
                                    })
                                    .await;
                                (tool_result.result, tool_result.success)
                            }
                            Err(error) => {
                                let message =
                                    format!("Tool re-execution after approval failed: {error}");
                                let _ = mpsc_tx
                                    .send(
                                        emitter.error(message.clone()).clone().into_agent_event(),
                                    )
                                    .await;
                                (message, false)
                            }
                        }
                    }
                };

                tracing::info!(
                    "[{}] connect: resolved approved tool replay '{}' ({}) -> success={}",
                    session_id,
                    tool_name,
                    reexecute_tool_call_id,
                    success
                );
                apply_tool_result(&mut session, &reexecute_tool_call_id, content, success);
                ctx.session_repo.save_and_cache(&mut session).await;
            } else {
                session.metadata.remove(PERMISSION_REEXECUTE_METADATA_KEY);
                tracing::warn!(
                    "[{}] connect: permission re-exec marker set but tool call '{}' not found in history",
                    session_id,
                    reexecute_tool_call_id
                );
            }

            consume_pending_clarification_resume(&mut session);
            spawn_session_execution(SessionExecutionArgs {
                agent: ctx.agent.clone(),
                session_id,
                session,
                execution_reservation,
                tools_override: Some(ctx.tools.clone()),
                provider_override: None,
                model_roster,
                reasoning_effort,
                reasoning_effort_source: "connect_resume".to_string(),
                auxiliary_model_resolver: None,
                disabled_filter_resolver: None,
                disabled_tools: Some(config.disabled_tools.clone()),
                disabled_skill_ids: Some(config.disabled_skill_ids.clone()),
                selected_skill_ids: None,
                selected_skill_mode: None,
                mpsc_tx,
                image_fallback: config.image_fallback.clone(),
                gold_config: config.gold_config.clone(),
                guardian_config: None,
                guardian_spawner: None,
                bash_resume_hook: None,
                bash_completion_sink: None,
                app_data_dir: ctx.app_data_dir.clone(),
                // No per-request override on this path; the config-level
                // default (issue #221) still applies.
                run_budget: None,
                runners: ctx.agent_runners.clone(),
                sessions_cache: ctx.session_repo.cache().clone(),
                on_complete: None,
                // Connect drives root sessions; a child finishing on this
                // path is backstopped by the child-wait watchdog (#546).
                child_completion_handler: None,
            });
        });
    }
}

/// Find the original tool call (with its arguments) by id in the session
/// history. Mirrors `app_state::resume_adapter::find_pending_tool_call`.
fn find_pending_tool_call(session: &Session, tool_call_id: &str) -> Option<ToolCall> {
    session.messages.iter().find_map(|message| {
        message
            .tool_calls
            .as_ref()
            .and_then(|calls| calls.iter().find(|call| call.id == tool_call_id).cloned())
    })
}

/// Overwrite the tool-result message for `tool_call_id` with the real tool
/// output. Mirrors `app_state::resume_adapter::apply_tool_result`.
fn apply_tool_result(session: &mut Session, tool_call_id: &str, content: String, success: bool) {
    for message in &mut session.messages {
        if message.tool_call_id.as_deref() == Some(tool_call_id) {
            message.content = content;
            message.tool_success = Some(success);
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ask(options: Vec<&str>, allow_custom: bool) -> ParkedAsk {
        ParkedAsk {
            nonce: "abc12345".to_string(),
            session_id: "sess-1".to_string(),
            tool_call_id: "call-1".to_string(),
            tool_name: "conclusion_with_options".to_string(),
            question: "Approve?".to_string(),
            options: options.into_iter().map(str::to_string).collect(),
            allow_custom,
        }
    }

    #[test]
    fn new_nonce_is_short_and_hex_like() {
        let nonce = new_nonce();
        assert!(!nonce.is_empty());
        assert!(nonce.len() <= 16);
        assert!(nonce.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn match_text_answer_numeric_index_selects_option() {
        let pending = ask(vec!["Approve", "Deny"], false);
        assert_eq!(
            match_text_answer(&pending, "1"),
            Some("Approve".to_string())
        );
        assert_eq!(match_text_answer(&pending, "2"), Some("Deny".to_string()));
        assert_eq!(match_text_answer(&pending, "3"), None);
    }

    #[test]
    fn match_text_answer_exact_text_is_case_insensitive() {
        let pending = ask(vec!["Approve", "Deny"], false);
        assert_eq!(
            match_text_answer(&pending, "approve"),
            Some("Approve".to_string())
        );
    }

    #[test]
    fn match_text_answer_binary_keyword_mapping() {
        let pending = ask(vec!["Approve", "Deny"], false);
        assert_eq!(
            match_text_answer(&pending, "允许"),
            Some("Approve".to_string())
        );
        assert_eq!(
            match_text_answer(&pending, "yes"),
            Some("Approve".to_string())
        );
        assert_eq!(
            match_text_answer(&pending, "deny"),
            Some("Deny".to_string())
        );
        assert_eq!(match_text_answer(&pending, "no"), Some("Deny".to_string()));
    }

    /// Ordering guarantee documented on [`NEGATIVE_KEYWORDS`]: an option
    /// literally titled "Stay" — even as the POSITIVE first choice — resolves
    /// via exact option-text matching BEFORE the keyword fallback, so the
    /// "stay"-is-negative heuristic can never misroute it.
    #[test]
    fn match_text_answer_exact_option_named_stay_beats_negative_keyword_fallback() {
        let pending = ask(vec!["Stay", "Leave"], false);
        assert_eq!(
            match_text_answer(&pending, "stay"),
            Some("Stay".to_string())
        );
        // And the fallback still works as intended for plan-mode phrasing,
        // where "stay" appears INSIDE the negative option's text.
        let plan_pending = ask(vec!["Approve", "Stay in plan mode"], false);
        assert_eq!(
            match_text_answer(&plan_pending, "stay"),
            Some("Stay in plan mode".to_string())
        );
    }

    #[test]
    fn match_text_answer_closed_ask_non_matching_text_falls_through() {
        let pending = ask(vec!["Approve", "Deny"], false);
        assert_eq!(match_text_answer(&pending, "banana"), None);
    }

    #[test]
    fn match_text_answer_open_question_accepts_any_free_text() {
        let pending = ask(vec!["OK", "Need changes"], true);
        assert_eq!(
            match_text_answer(&pending, "please add tests too"),
            Some("please add tests too".to_string())
        );
    }

    #[test]
    fn match_text_answer_empty_text_never_matches() {
        let pending = ask(vec!["OK", "Need changes"], true);
        assert_eq!(match_text_answer(&pending, "   "), None);
    }

    #[test]
    fn match_callback_data_requires_the_exact_nonce() {
        let pending = ask(vec!["Approve", "Deny"], false);
        assert_eq!(
            match_callback_data(&pending, "abc12345:0"),
            Some("Approve".to_string())
        );
        assert_eq!(match_callback_data(&pending, "stale-nonce:0"), None);
    }

    #[test]
    fn match_callback_data_rejects_out_of_range_index() {
        let pending = ask(vec!["Approve", "Deny"], false);
        assert_eq!(match_callback_data(&pending, "abc12345:9"), None);
    }

    #[test]
    fn match_callback_data_rejects_malformed_data() {
        let pending = ask(vec!["Approve", "Deny"], false);
        assert_eq!(match_callback_data(&pending, "not-a-valid-shape"), None);
        assert_eq!(match_callback_data(&pending, "abc12345:not-a-number"), None);
    }

    #[test]
    fn format_ask_text_numbers_every_option() {
        let pending = ask(vec!["Approve", "Deny"], false);
        let text = format_ask_text(&pending);
        assert!(text.contains("1. Approve"));
        assert!(text.contains("2. Deny"));
        assert!(!text.contains("reply with your own answer"));
    }

    #[test]
    fn format_ask_text_open_question_mentions_free_text() {
        let pending = ask(vec!["OK", "Need changes"], true);
        assert!(format_ask_text(&pending).contains("reply with your own answer"));
    }
}
