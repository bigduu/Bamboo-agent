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

use bamboo_agent_core::tools::ToolExecutionContext;
use bamboo_agent_core::{AgentEvent, Session};
use bamboo_engine::execution::{
    create_event_forwarder_with_root_actor, get_or_create_event_sender, reserve_session_execution,
    SessionExecutionReserveOutcome,
};
use bamboo_engine::runtime::execution::agent_spawn::{
    spawn_session_execution, SessionExecutionArgs,
};
use bamboo_engine::session_app::approval_replay::{
    apply_permission_replay_result, find_permission_replay_target,
    refresh_retained_approval_replay_posture, repark_permission_replay,
    resolve_permission_replay_owner, restore_permission_replay_authorization,
    restore_permission_replay_caller, restore_permission_replay_execution,
    validate_permission_replay_authority, ApprovalReplayDecision, PermissionReplayTarget,
};
use bamboo_engine::session_app::execute::consume_pending_clarification_resume;
use bamboo_engine::session_app::repository::SessionAccess;
use bamboo_engine::session_app::resolution::resolve_resume_config_snapshot;
use bamboo_engine::session_app::respond::{
    acquire_pending_response_guard, inspect_pending_response_guarded,
    submit_pending_response_checked_guarded, validate_pending_response,
    PERMISSION_REEXECUTE_GENERATION_METADATA_KEY, PERMISSION_REEXECUTE_METADATA_KEY,
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

// Preserve an explicit structured target, even if malformed; an invalid
// explicit target must never become permission to use the default provider.
fn connect_session_target(
    session: &Session,
) -> Result<Option<bamboo_domain::ProviderModelRef>, String> {
    if let Some(target) = session.model_ref.as_ref() {
        if session.model != target.model {
            return Err("The session model does not match its provider binding".to_string());
        }
        return Ok(Some(target.clone()));
    }
    Ok(session
        .provider_name()
        .map(|provider| bamboo_domain::ProviderModelRef::new(provider, session.model.clone())))
}

#[cfg(test)]
mod response_test_hooks {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, OnceLock};
    use tokio::sync::Notify;

    pub(super) struct Hook {
        pub(super) reached: Notify,
        pub(super) resume: Notify,
    }

    fn hooks() -> &'static Mutex<HashMap<String, Arc<Hook>>> {
        static HOOKS: OnceLock<Mutex<HashMap<String, Arc<Hook>>>> = OnceLock::new();
        HOOKS.get_or_init(|| Mutex::new(HashMap::new()))
    }

    pub(super) fn install(session_id: &str) -> Arc<Hook> {
        let hook = Arc::new(Hook {
            reached: Notify::new(),
            resume: Notify::new(),
        });
        hooks()
            .lock()
            .unwrap()
            .insert(session_id.to_string(), hook.clone());
        hook
    }

    pub(super) async fn pause(session_id: &str) {
        let hook = hooks().lock().unwrap().remove(session_id);
        if let Some(hook) = hook {
            hook.reached.notify_one();
            hook.resume.notified().await;
        }
    }

    fn before_submit_hooks() -> &'static Mutex<HashMap<String, Arc<Hook>>> {
        static HOOKS: OnceLock<Mutex<HashMap<String, Arc<Hook>>>> = OnceLock::new();
        HOOKS.get_or_init(|| Mutex::new(HashMap::new()))
    }

    pub(super) fn install_before_submit(session_id: &str) -> Arc<Hook> {
        let hook = Arc::new(Hook {
            reached: Notify::new(),
            resume: Notify::new(),
        });
        before_submit_hooks()
            .lock()
            .unwrap()
            .insert(session_id.to_string(), hook.clone());
        hook
    }

    pub(super) async fn pause_before_submit(session_id: &str) {
        let hook = before_submit_hooks().lock().unwrap().remove(session_id);
        if let Some(hook) = hook {
            hook.reached.notify_one();
            hook.resume.notified().await;
        }
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
        let pending_tool_call_id = pending.tool_call_id.clone();

        let mut port = ConnectResumePort {
            ctx: self.ctx.clone(),
            provider_override: None,
            model_origin: None,
            selected_provider_name: None,
            model_reasoning_effort: None,
        };
        let mut handoff = bamboo_engine::session_app::resume::reserve_response_resume_handoff(
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
        // The previous runner may have finished while the successor was
        // being reserved. Re-read its durable session under the same response
        // guard before selecting the provider and consuming the answer.
        let current = match inspect_pending_response_guarded(
            &self.ctx.session_repo,
            session_id,
            &response_guard,
        )
        .await
        {
            Ok(Some(session)) => session,
            Ok(None) => {
                handoff.abandon().await;
                return Err(ResponderError::NotFound);
            }
            Err(error) => {
                handoff.abandon().await;
                return Err(map_respond_error(error));
            }
        };
        let Some(pending) = current.pending_question.as_ref() else {
            handoff.abandon().await;
            return Err(ResponderError::NoPendingQuestion);
        };
        if pending.tool_call_id != pending_tool_call_id {
            handoff.abandon().await;
            return Err(ResponderError::PendingQuestionChanged);
        }
        if let Err(error) = validate_pending_response(pending, &answer) {
            handoff.abandon().await;
            return Err(ResponderError::InvalidResponse(error));
        }
        let current_model = current.model.clone();
        let terminal_stop =
            bamboo_engine::session_app::no_progress::is_no_progress_question(&current, pending)
                && answer == bamboo_engine::session_app::no_progress::STOP_OPTION;
        let current_target = if terminal_stop {
            None
        } else {
            match connect_session_target(&current) {
                Ok(Some(target)) => Some(target),
                Ok(None) => {
                    handoff.abandon().await;
                    return Err(ResponderError::Other(
                        "The session provider/model binding cannot be verified; answer not consumed".to_string(),
                    ));
                }
                Err(error) => {
                    handoff.abandon().await;
                    return Err(ResponderError::Other(format!(
                        "{error}; answer not consumed"
                    )));
                }
            }
        };

        // Resolve configuration before the response CAS. One response keeps
        // this snapshot; subsequent runs observe later configuration changes.
        let receiver = handoff.subscribe();
        let config_snapshot = self.ctx.config.read().await.clone();
        if let Some(target) = current_target.as_ref() {
            match bamboo_engine::model_config_helper::try_resolve_primary_model(
                &config_snapshot,
                &self.ctx.provider_registry,
                target,
            ) {
                Ok(selected) => {
                    port.model_reasoning_effort = selected.target.reasoning_effort.or_else(|| {
                        config_snapshot.reasoning_effort_for_key(&selected.target.provider)
                    });
                    port.selected_provider_name = Some(selected.target.provider);
                    port.provider_override = Some(selected.provider);
                    port.model_origin = selected.managed_origin;
                }
                Err(error) => {
                    handoff.abandon().await;
                    return Err(ResponderError::Other(format!(
                        "Configured model is unavailable: {error}; answer not consumed"
                    )));
                }
            }
        }
        // Do not consume a response if this execution cannot perform the
        // physical profile check required before a model continuation.
        let startup_persistence = if terminal_stop {
            None
        } else {
            match handoff.execution_persistence() {
                Some(persistence)
                    if persistence.supports_bound_startup_session()
                        && matches!(
                            (handoff.root_actor_writer(), persistence.root_actor_writer()),
                            (Some(handoff_owner), Some(persist_owner))
                                if handoff_owner.fence == persist_owner.fence
                                    && handoff_owner.created_at == persist_owner.created_at
                                    && handoff_owner.conversation_generation == persist_owner.conversation_generation
                        ) =>
                {
                    Some(persistence)
                }
                _ => {
                    handoff.abandon().await;
                    return Err(ResponderError::Other(
                        "The session has no bound startup writer; answer not consumed".to_string(),
                    ));
                }
            }
        };
        let input = RespondInput {
            session_id: session_id.to_string(),
            user_response: answer,
            model: None,
            model_ref: None,
            provider: None,
            reasoning_effort: None,
        };

        let root_owner = handoff.root_actor_writer();
        let inherited = root_owner.as_ref().and_then(|_| {
            handoff
                .execution_persistence()
                .and_then(|persistence| persistence.inherited_child_wait())
        });
        let bound_access = match self
            .ctx
            .session_repo
            .bind_response_writer_with_inherited(root_owner, inherited)
        {
            Ok(access) => access,
            Err(error) => {
                handoff.abandon().await;
                return Err(ResponderError::Other(format!(
                    "Response writer unavailable: {error}"
                )));
            }
        };
        // Own the durable response transaction before it can begin. Dropping
        // the caller cannot cancel a committed answer or strand its reservation.
        let ctx = self.ctx.clone();
        let session_id = session_id.to_string();
        tokio::spawn(async move {
            let session_id = session_id.as_str();
            let response_access: &dyn SessionAccess =
                bound_access.as_deref().unwrap_or(&ctx.session_repo);
            #[cfg(test)]
            response_test_hooks::pause_before_submit(session_id).await;
        let submission = submit_pending_response_checked_guarded(
            response_access,
            input,
            Some(pending_tool_call_id),
            &response_guard,
        )
        .await;
        let (mut session, _submitted_answer, plan_mode_transition, permission_grants) =
            match submission {
                Ok(submission) => submission,
                Err(error) => {
                    handoff.abandon().await;
                    return Err(map_respond_error(error));
                }
            };

            let target_changed = !terminal_stop
                && (session.model != current_model
                    || !matches!(
                        connect_session_target(&session),
                        Ok(target) if target == current_target
                    ));

            // The response is already committed. Recheck its complete profile
            // under the Root writer's physical guard before transferring the
            // reservation to a model continuation.
            let startup_error = if !target_changed && !terminal_stop {
                // Persist the selected account, not a legacy provider-type
                // alias that could later resolve to a different account.
                let expected = session.clone();
                let mut canonical_target = current_target
                    .clone()
                    .expect("non-terminal response requires a model target");
                canonical_target.provider = port
                    .selected_provider_name
                    .clone()
                    .expect("non-terminal response requires a selected provider");
                bamboo_engine::session_app::provider_model::persist_model_ref(
                    &mut session,
                    &canonical_target,
                );
                #[cfg(test)]
                response_test_hooks::pause(session_id).await;
                startup_persistence
                    .as_ref()
                    .expect("non-terminal response requires a bound writer")
                    .save_bound_startup_session(&mut session, &expected)
                    .await
                    .err()
                    .map(|error| format!(
                        "Answer was saved, but the session profile could not be confirmed before resuming: {error}"
                    ))
            } else {
                None
            };

            let transition = plan_mode_transition_event(session_id, plan_mode_transition.as_ref());
            if target_changed || terminal_stop || startup_error.is_some() {
                let stopped_message = terminal_stop.then(|| {
                    session
                        .messages
                        .last()
                        .expect("Stop acknowledgement")
                        .clone()
                });
                let reason = if terminal_stop {
                    "Stopped by the user.".to_string()
                } else {
                    startup_error.unwrap_or_else(|| {
                        "Answer was saved, but the session model or provider changed before resuming".to_string()
                    })
                };
                // Once the answer is committed, a detached owner must publish its
                // history even if the caller is cancelled while awaiting a receipt.
                let published = publish_recorded_response_and_abandon(
                    ctx.clone(),
                    session_id.to_string(),
                    handoff,
                    transition,
                    stopped_message,
                    reason.clone(),
                )
                .await;
                let message = if published {
                    reason
                } else {
                    format!("{reason}; response event publication failed")
                };
                return Ok(RespondAndResumeOutcome::NotResumed(message));
            }
            // Mirrors `handlers::agent::respond::handlers::submit`: record any
            // permission grant so the resumed re-execution of the gated tool
            // passes the check without re-prompting.
            for (perm_type, resource) in &permission_grants {
                if let Some(request_id) = session.metadata.get(PERMISSION_REEXECUTE_METADATA_KEY) {
                    ctx.permission_checker.grant_once(
                        session_id,
                        request_id,
                        *perm_type,
                        resource.clone(),
                    );
                }
            }

            if let Some(event) = transition {
                handoff.publish_event(event);
            }

            // The fenced save made the actual session use the canonical
            // account, including for auxiliary model resolution.
            let resume_config = resolve_resume_config_snapshot(
                &config_snapshot,
                &ctx.provider_registry,
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
        })
        .await
        .map_err(|error| ResponderError::Other(format!("Response handoff task failed: {error}")))?
    }
}

// A saved response owns its publication until the history barrier is accepted,
// even when no model can be started or a concurrent target edit blocks resume.
async fn publish_recorded_response_and_abandon(
    ctx: ConnectContext,
    session_id: String,
    handoff: bamboo_engine::session_app::resume::ResponseResumeHandoff,
    transition: Option<AgentEvent>,
    stopped_message: Option<bamboo_agent_core::Message>,
    reason: String,
) -> bool {
    let (reservation, sender) = handoff.into_parts();
    let root_publication = match (
        reservation.root_actor_writer(),
        ctx.root_account_sink.clone(),
    ) {
        (Some(owner), Some(sink)) => Some(bamboo_engine::events::RootActorEventPublication::new(
            ctx.session_repo.storage().clone(),
            sink,
            owner,
        )),
        (Some(_), None) => {
            tracing::warn!(%session_id, "Connect response has no Root account sink");
            reservation.abandon().await;
            return false;
        }
        (None, _) => None,
    };
    let (events, _forwarder, mut history) = create_event_forwarder_with_root_actor(
        session_id.clone(),
        reservation.run_id().to_string(),
        sender,
        ctx.agent_runners.clone(),
        ctx.account_feed_inbox.clone(),
        root_publication,
    );
    let is_stop = stopped_message.is_some();
    let mut published = true;
    if let Some(event) = transition {
        published = events.send(event).await.is_ok();
    }
    if let Some(message) = stopped_message {
        published = published
            && events
                .send(AgentEvent::message_appended(&session_id, &message))
                .await
                .is_ok();
    }
    let terminal = if is_stop {
        AgentEvent::Cancelled {
            message: Some(reason),
        }
    } else {
        AgentEvent::Error { message: reason }
    };
    published = published && events.send(terminal).await.is_ok();
    published = published && history.send_and_wait(&events, session_id.clone()).await;
    if !published {
        tracing::warn!(%session_id, "Connect response history publication was not confirmed");
    }
    drop(events);
    reservation.abandon().await;
    published
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
    provider_override: Option<Arc<dyn bamboo_llm::LLMProvider>>,
    model_origin: Option<Arc<bamboo_llm::provider_registry::ManagedProviderOrigin>>,
    selected_provider_name: Option<String>,
    model_reasoning_effort: Option<bamboo_domain::ReasoningEffort>,
}

fn connect_resume_auxiliary_model_resolver(
    config: Arc<tokio::sync::RwLock<bamboo_llm::Config>>,
    registry: Arc<bamboo_llm::ProviderRegistry>,
    provider_name: &str,
) -> Arc<dyn Fn() -> bamboo_engine::AuxiliaryModelConfig + Send + Sync> {
    bamboo_engine::model_areas::live_auxiliary_model_resolver(
        config,
        registry,
        provider_name.to_owned(),
    )
}

#[async_trait::async_trait]
impl ResumeExecutionPort for ConnectResumePort {
    async fn prepare_response_execution(
        &self,
        reservation: &mut bamboo_engine::execution::SessionExecutionReservation,
    ) -> std::io::Result<()> {
        let session = self
            .ctx
            .session_repo
            .inspect_for_response(reservation.session_id())
            .await
            .map_err(std::io::Error::other)?
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotFound, "response session missing")
            })?;
        self.ctx
            .bind_root_execution(reservation, &session)
            .await
            .map(|_| ())
    }

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
            provider_override: self.provider_override.clone(),
            model_origin: self.model_origin.clone(),
            selected_provider_name: self.selected_provider_name.clone(),
            model_reasoning_effort: self.model_reasoning_effort,
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
        let root_publication = match self
            .ctx
            .bind_root_execution(&mut execution_reservation, &session)
            .await
        {
            Ok(publication) => publication,
            Err(error) => {
                tracing::warn!(%session_id, %error, "Root Connect resume binding rejected");
                execution_reservation.abandon().await;
                return;
            }
        };

        let provider_override = self.provider_override.clone();
        let model = session.model.clone();
        let reasoning_effort = session.reasoning_effort.or(self.model_reasoning_effort);
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

        // Both continuation paths keep Fast tied to the resumed run's key.
        let auxiliary_model_resolver = connect_resume_auxiliary_model_resolver(
            self.ctx.config.clone(),
            self.ctx.provider_registry.clone(),
            &config.provider_name,
        );
        let resume_resolver = bamboo_engine::model_areas::live_resume_auxiliary_resolver(
            self.ctx.config.clone(),
            self.ctx.provider_registry.clone(),
            config.provider_name.clone(),
        );
        let mut resume_agent = self
            .ctx
            .agent
            .with_resume_auxiliary_resolver(resume_resolver);
        if let Some(policy) = crate::app_state::host_gold_origin::managed_selection_resolver(
            self.ctx.config.clone(),
            &config.provider_name,
            provider_override.as_ref(),
            self.model_origin.as_ref(),
        ) {
            resume_agent = resume_agent.with_composed_gold_target_resolver(policy);
        }
        let resume_agent = Arc::new(resume_agent);

        let (mpsc_tx, _forwarder_handle, mut history_commit_barrier) =
            create_event_forwarder_with_root_actor(
                session_id.clone(),
                execution_reservation.run_id().to_string(),
                event_sender,
                self.ctx.agent_runners.clone(),
                self.ctx.account_feed_inbox.clone(),
                root_publication,
            );
        let response_events = execution_reservation.take_root_response_events();
        if !response_events.is_empty() {
            for event in response_events {
                if mpsc_tx.send(event).await.is_err() {
                    execution_reservation.abandon().await;
                    return;
                }
            }
            if !history_commit_barrier
                .send_and_wait(&mpsc_tx, session_id.clone())
                .await
            {
                tracing::warn!(%session_id, "Root Connect response publication was not confirmed");
                execution_reservation.abandon().await;
                return;
            }
        }

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
        let reexecute_request_generation = session
            .metadata
            .get(PERMISSION_REEXECUTE_GENERATION_METADATA_KEY)
            .cloned();

        let Some(reexecute_tool_call_id) = reexecute_tool_call_id else {
            if reexecute_request_generation.is_some() {
                tracing::error!(
                    %session_id,
                    "connect found orphaned permission replay generation marker; refusing to resume"
                );
                return;
            }
            consume_pending_clarification_resume(&mut session);
            spawn_session_execution(SessionExecutionArgs {
                agent: resume_agent.clone(),
                session_id,
                session,
                execution_reservation,
                tools_override: Some(self.ctx.tools.clone()),
                provider_override: provider_override.clone(),
                model_roster,
                reasoning_effort,
                reasoning_effort_source: "connect_resume".to_string(),
                auxiliary_model_resolver: Some(auxiliary_model_resolver.clone()),
                disabled_filter_resolver: None,
                disabled_tools: Some(config.disabled_tools.clone()),
                disabled_skill_ids: Some(config.disabled_skill_ids.clone()),
                selected_skill_ids: None,
                selected_skill_mode: None,
                mpsc_tx,
                history_commit_barrier,
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

            if let Some(replay_target) = find_pending_tool_call(
                &session,
                &reexecute_tool_call_id,
                reexecute_request_generation.as_deref(),
            ) {
                if reexecute_request_generation.is_none()
                    && replay_target.request_generation().is_some()
                {
                    tracing::error!(
                        %session_id,
                        tool_call_id = %reexecute_tool_call_id,
                        "connect typed permission replay is missing its generation marker; refusing to resume"
                    );
                    return;
                }
                let tool_call = replay_target.tool_call().clone();
                let tool_name = tool_call.function.name.clone();
                let executor = ctx.tools.clone();
                let executor = executor.for_session(&session).unwrap_or(executor);
                let replay_owner = match resolve_permission_replay_owner(
                    &session,
                    &replay_target,
                    executor.as_ref(),
                ) {
                    Ok(owner) => owner,
                    Err(error) => {
                        tracing::error!(%session_id, %error, "retained permission operation refused; pending markers preserved");
                        return;
                    }
                };
                if replay_owner.is_none() && reexecute_request_generation.is_some() {
                    tracing::error!(%session_id, %tool_name, "connect approved replay has no registered execution owner; markers retained");
                    return;
                }
                let executing_supervisor = match validate_permission_replay_authority(
                    &session,
                    &replay_target,
                    replay_owner.as_deref().unwrap_or(&tool_name),
                ) {
                    Ok(observation) => observation,
                    Err(error) => {
                        tracing::error!(%session_id, %error, "Supervisor approval replay binding failed closed");
                        return;
                    }
                };
                let executing_caller = match restore_permission_replay_caller(
                    &session,
                    &replay_target,
                ) {
                    Ok(observation) => observation,
                    Err(error) => {
                        tracing::error!(%session_id, %error, "original caller replay binding failed closed; markers preserved");
                        return;
                    }
                };
                let retained_execution = match restore_permission_replay_execution(
                    &session,
                    &replay_target,
                    replay_owner.as_deref().unwrap_or(&tool_name),
                ) {
                    Ok(execution) => execution,
                    Err(error) => {
                        tracing::error!(%session_id, %error, "retained contract refused; markers preserved");
                        return;
                    }
                };
                let configured_mode = ctx
                    .permission_checker
                    .permission_config()
                    .map(|config| config.mode())
                    .unwrap_or_default();
                let decision = match refresh_retained_approval_replay_posture(
                    ctx.session_repo.storage().as_ref(),
                    &mut session,
                    configured_mode,
                    replay_owner.as_deref(),
                    retained_execution.as_ref(),
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
                let blocked_by_tool_authority = matches!(
                    decision,
                    ApprovalReplayDecision::BlockedByRootToolAuthority
                        | ApprovalReplayDecision::BlockedByUnavailableTool
                );
                session.metadata.remove(PERMISSION_REEXECUTE_METADATA_KEY);
                session
                    .metadata
                    .remove(PERMISSION_REEXECUTE_GENERATION_METADATA_KEY);

                let (content, success) = match decision {
                    ApprovalReplayDecision::BlockedByPlan(_) => (
                        format!(
                            "Plan mode blocked approved mutating tool '{tool_name}'; the stale approval was not executed"
                        ),
                        false,
                    ),
                    ApprovalReplayDecision::BlockedByRootToolAuthority => (
                        format!(
                            "Root orchestration policy blocked approved tool '{tool_name}'; the stale approval was not executed"
                        ),
                        false,
                    ),
                    ApprovalReplayDecision::BlockedByUnavailableTool => (
                        format!(
                            "Approved tool '{tool_name}' is no longer available; the stale approval was not executed"
                        ),
                        false,
                    ),
                    ApprovalReplayDecision::Execute(flags) => {
                        let replay_owner = replay_owner
                            .as_deref()
                            .expect("Execute requires a registered execution owner");
                        let Some(permission_config) =
                            ctx.permission_checker.permission_config()
                        else {
                            tracing::error!(
                                %session_id,
                                tool_call_id = %reexecute_tool_call_id,
                                "connect typed approval replay has no permission configuration; refusing to resume"
                            );
                            return;
                        };
                        if let Err(error) = restore_permission_replay_authorization(
                            permission_config.as_ref(),
                            &session,
                            &replay_target,
                            replay_owner,
                        ) {
                            tracing::error!(
                                %session_id,
                                tool_call_id = %reexecute_tool_call_id,
                                %error,
                                "connect typed approval replay authorization recovery failed closed"
                            );
                            return;
                        }
                        let is_mutating = bamboo_tools::orchestrator::classify_tool(&tool_name)
                            == bamboo_tools::orchestrator::ToolMutability::Mutating;
                        if let Some(persistence) = execution_reservation.execution_persistence() {
                            match persistence.load_runtime_session(&session_id).await {
                                Ok(Some(_)) => {}
                                result => {
                                    tracing::warn!(%session_id, ?result, "Root Connect replay lost its owner before tool handoff");
                                    return;
                                }
                            }
                        }
                        let mut emitter = bamboo_tools::ToolEmitter::new(
                            &tool_call.id,
                            &tool_name,
                            is_mutating,
                        );
                        emitter.set_auto_approved(true);
                        let _ = mpsc_tx
                            .send(emitter.begin().clone().into_agent_event())
                            .await;
                        let exec_result = bamboo_tools::permission::with_permission_replay_generation(
                            session.id.as_str(),
                            reexecute_tool_call_id.as_str(),
                            reexecute_request_generation.as_deref(),
                            bamboo_agent_core::tools::with_retained_tool_execution(
                                retained_execution,
                                bamboo_agent_core::tools::with_executing_session_caller(
                                executing_caller.map(std::sync::Arc::new),
                                executor.execute_exact_with_context_outcome(
                                &tool_call,
                                replay_owner,
                                ToolExecutionContext {
                                    executing_supervisor,
                                    session_id: Some(session.id.as_str()),
                                    root_session_id: Some(
                                        if session.root_session_id.trim().is_empty() {
                                            session.id.as_str()
                                        } else {
                                            session.root_session_id.as_str()
                                        },
                                    ),
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
                                ),
                                ),
                            ),
                        )
                        .await.map(bamboo_agent_core::tools::ToolOutcome::into_tool_result);

                        match exec_result {
                            Ok(tool_result) => {
                                match repark_permission_replay(
                                    &mut session,
                                    &replay_target,
                                    &tool_result,
                                    replay_owner,
                                ) {
                                    Ok(Some(reparked)) => {
                                        let _ = mpsc_tx
                                            .send(
                                                emitter
                                                    .finish(Some(
                                                        "Awaiting additional permission approval"
                                                            .to_string(),
                                                    ))
                                                    .clone()
                                                    .into_agent_event(),
                                            )
                                            .await;
                                        let _ = mpsc_tx
                                            .send(AgentEvent::ToolComplete {
                                                tool_call_id: tool_call.id.clone(),
                                                result: tool_result,
                                            })
                                            .await;
                                        let _ = mpsc_tx
                                            .send(AgentEvent::NeedClarification {
                                                question: reparked.question,
                                                options: (!reparked.options.is_empty())
                                                    .then_some(reparked.options),
                                                tool_call_id: Some(tool_call.id.clone()),
                                                tool_name: Some(tool_name.clone()),
                                                allow_custom: reparked.allow_custom,
                                                source: Some(
                                                    bamboo_agent_core::PendingQuestionSource::PauseTool,
                                                ),
                                            })
                                            .await;
                                        if let Some(persistence) = execution_reservation.execution_persistence() {
                                            if let Err(error) = persistence.save_runtime_session(&mut session).await {
                                                tracing::warn!(%session_id, %error, "Root Connect repark checkpoint rejected");
                                                return;
                                            }
                                        } else { ctx.session_repo.save_and_cache(&mut session).await; }
                                        if !history_commit_barrier.send_and_wait(&mpsc_tx, session_id.clone()).await {
                                            tracing::warn!(%session_id, "Root Connect repark history publication was not confirmed");
                                            return;
                                        }
                                        execution_reservation.finish_root_actor(bamboo_domain::ActorActivationFinish::Succeeded).await;
                                        execution_reservation.abandon().await;
                                        return;
                                    }
                                    Ok(None) => {}
                                    Err(error) => {
                                        tracing::error!(
                                            %session_id,
                                            tool_call_id = %reexecute_tool_call_id,
                                            %error,
                                            "connect additional permission replay could not be re-parked; refusing to resume"
                                        );
                                        return;
                                    }
                                }
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
                if !apply_tool_result(&mut session, &replay_target, content, success) {
                    tracing::error!(
                        %session_id,
                        tool_call_id = %reexecute_tool_call_id,
                        "connect approved tool replay result target changed unexpectedly; refusing to resume"
                    );
                    return;
                }
                if let Some(persistence) = execution_reservation.execution_persistence() {
                    if let Err(error) = persistence.save_runtime_session(&mut session).await {
                        tracing::warn!(%session_id, %error, "Root Connect replay checkpoint rejected before continuation");
                        return;
                    }
                } else if blocked_by_tool_authority {
                    if let Err(error) = ctx.session_repo.save_replay_resolution(&mut session).await
                    {
                        tracing::error!(%session_id, %error, "connect blocked approval replay result failed to persist; refusing to resume");
                        return;
                    }
                } else {
                    ctx.session_repo.save_and_cache(&mut session).await;
                }
            } else {
                tracing::error!(
                    %session_id,
                    tool_call_id = %reexecute_tool_call_id,
                    request_generation = ?reexecute_request_generation,
                    "connect permission replay target missing or generation-mismatched; markers retained and resume refused"
                );
                return;
            }

            consume_pending_clarification_resume(&mut session);
            spawn_session_execution(SessionExecutionArgs {
                agent: resume_agent,
                session_id,
                session,
                execution_reservation,
                tools_override: Some(ctx.tools.clone()),
                provider_override,
                model_roster,
                reasoning_effort,
                reasoning_effort_source: "connect_resume".to_string(),
                auxiliary_model_resolver: Some(auxiliary_model_resolver),
                disabled_filter_resolver: None,
                disabled_tools: Some(config.disabled_tools.clone()),
                disabled_skill_ids: Some(config.disabled_skill_ids.clone()),
                selected_skill_ids: None,
                selected_skill_mode: None,
                mpsc_tx,
                history_commit_barrier,
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

/// Find the concrete approved invocation, newest-first and generation-bound.
fn find_pending_tool_call(
    session: &Session,
    tool_call_id: &str,
    request_generation: Option<&str>,
) -> Option<PermissionReplayTarget> {
    find_permission_replay_target(session, tool_call_id, request_generation)
}

/// Overwrite only the exact generation-bound tool-result message.
fn apply_tool_result(
    session: &mut Session,
    target: &PermissionReplayTarget,
    content: String,
    success: bool,
) -> bool {
    apply_permission_replay_result(session, target, content, success)
}

#[cfg(test)]
mod tests {

    // Consumer-level resolver/dispatch regression; it does not launch an
    // approval session or claim an end-to-end Connect resume.
    #[tokio::test]
    async fn connect_resume_fast_rejects_invalid_or_busy_route_without_main_fallback() {
        use bamboo_llm::{LLMError, LLMProvider, LLMRequestOptions, LLMStream, ProviderRegistry};
        use futures::StreamExt;
        use serde_json::json;
        use std::time::Duration;
        use tokio::sync::RwLock;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        async fn dispatch(
            selected: bamboo_engine::AuxiliaryModelConfig,
            main: Arc<dyn LLMProvider>,
        ) -> Result<(), LLMError> {
            let model = selected.fast_model_name.unwrap_or_else(|| "main-a".into());
            let provider = selected.fast_model_provider.unwrap_or(main);
            let options = LLMRequestOptions {
                request_purpose: Some("task_evaluation".into()),
                ..Default::default()
            };
            let mut stream: LLMStream = provider
                .chat_stream_with_options(
                    &[Message::user("synthetic Connect resume Fast")],
                    &[],
                    Some(32),
                    &model,
                    Some(&options),
                )
                .await?;
            while let Some(chunk) = stream.next().await {
                chunk?;
            }
            Ok(())
        }

        let a = MockServer::start().await;
        let b = MockServer::start().await;
        for server in [&a, &b] {
            Mock::given(method("POST"))
                .and(path("/chat/completions"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .insert_header("content-type", "text/event-stream")
                        .set_body_string(concat!(
                            "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"}}]}\n\n",
                            "data: [DONE]\n\n",
                        )),
                )
                .mount(server)
                .await;
        }
        let home = tempfile::tempdir().unwrap();
        let mut baseline = bamboo_llm::Config::default();
        baseline.provider = "account-a".into();
        baseline.default_provider_instance = Some("account-a".into());
        baseline.http_proxy = "http://127.0.0.1:9".into();
        baseline.https_proxy.clear();
        baseline.proxy_auth = None;
        baseline.defaults = Some(
            serde_json::from_value(json!({
                "chat": {"provider": "account-a", "model": "main-a"},
                "fast": {"provider": "account-b", "model": "cheap-b"}
            }))
            .unwrap(),
        );
        for (name, model, server) in [("account-a", "main-a", &a), ("account-b", "cheap-b", &b)] {
            baseline.provider_instances.insert(
                name.into(),
                serde_json::from_value(json!({
                    "provider_type": "openai", "enabled": true,
                    "api_key": "synthetic-connect-only", "base_url": server.uri(),
                    "model": model, "runtime_models": [model]
                }))
                .unwrap(),
            );
        }
        let registry = Arc::new(
            ProviderRegistry::from_config(&baseline, home.path().to_path_buf())
                .await
                .unwrap(),
        );
        let main = registry.get("account-a").unwrap();
        let mut session = Session::new("connect-resume-fast", "main-a");
        session.set_provider_name("account-a");
        let resume = resolve_resume_config_snapshot(&baseline, &registry, &session, None);
        assert_eq!(resume.provider_name, "account-a");
        let live = Arc::new(RwLock::new(baseline.clone()));
        let resolver = connect_resume_auxiliary_model_resolver(
            live.clone(),
            registry.clone(),
            &resume.provider_name,
        );
        let selected = resolver();
        assert_eq!(selected.fast_model_name.as_deref(), Some("cheap-b"));
        tokio::time::timeout(Duration::from_secs(5), dispatch(selected, main.clone()))
            .await
            .expect("valid Fast dispatch timeout")
            .expect("valid Fast reaches B");
        assert_eq!(a.received_requests().await.unwrap().len(), 0);
        assert_eq!(b.received_requests().await.unwrap().len(), 1);

        let guard = live.write().await;
        let blocked = resolver();
        drop(guard);
        let result = tokio::time::timeout(Duration::from_secs(5), dispatch(blocked, main.clone()))
            .await
            .expect("busy Fast dispatch timeout");
        assert!(matches!(result, Err(LLMError::Api(_))));
        assert_eq!(a.received_requests().await.unwrap().len(), 0);
        assert_eq!(b.received_requests().await.unwrap().len(), 1);

        for invalid in ["disabled", "not_admitted"] {
            let mut current = baseline.clone();
            let instance = current.provider_instances.get_mut("account-b").unwrap();
            if invalid == "disabled" {
                instance.enabled = false;
            } else {
                instance
                    .extra
                    .insert("runtime_models".into(), json!(["other-b"]));
            }
            *live.write().await = current;
            let result =
                tokio::time::timeout(Duration::from_secs(5), dispatch(resolver(), main.clone()))
                    .await
                    .expect("invalid Fast dispatch timeout");
            assert!(result.is_err(), "{invalid} must reject");
            assert_eq!(a.received_requests().await.unwrap().len(), 0);
            assert_eq!(b.received_requests().await.unwrap().len(), 1);
        }
        let requests = b.received_requests().await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(body["model"], "cheap-b");
    }

    // Exercise the resolver constructed for a Connect resume. This stops at
    // the resolver boundary; it is not an approval-session E2E test.
    #[tokio::test]
    async fn connect_resume_legacy_background_keeps_session_provider_live() {
        let fixture = crate::background_live_consumer_tests::Fixture::new().await;
        let mut session = bamboo_agent_core::Session::new("resume-fixture", "chat-b");
        session.model_ref = Some(bamboo_domain::ProviderModelRef::new("account-b", "chat-b"));
        session.set_provider_name("account-b");
        let snapshot =
            tokio::time::timeout(std::time::Duration::from_secs(5), fixture.config.read())
                .await
                .expect("config snapshot timeout")
                .clone();
        assert_eq!(snapshot.effective_default_provider(), "account-a");
        let resume = resolve_resume_config_snapshot(&snapshot, &fixture.registry, &session, None);
        assert_eq!(resume.provider_name, "account-b");
        assert_eq!(resume.background_model.as_deref(), Some("memory-only"));
        assert!(resume
            .background_model_provider
            .as_ref()
            .is_some_and(|provider| {
                fixture
                    .registry
                    .get("account-b")
                    .is_some_and(|b| Arc::ptr_eq(provider, &b))
            }));
        let missing = bamboo_engine::model_areas::live_resume_auxiliary_resolver(
            fixture.config.clone(),
            fixture.registry.clone(),
            "account-missing".into(),
        );
        let (_, missing_background) = missing();
        assert!(missing_background.is_none());
        let resolver = bamboo_engine::model_areas::live_resume_auxiliary_resolver(
            fixture.config.clone(),
            fixture.registry.clone(),
            resume.provider_name,
        );
        // The callback captures the session's B key, not the global A key.
        fixture
            .verify_resume_background(Arc::new(move || resolver().1))
            .await;
    }

    use bamboo_agent_core::tools::{FunctionCall, ToolCall};
    use bamboo_agent_core::Message;

    use super::*;

    fn supervisor_context(state: &crate::app_state::AppState) -> ConnectContext {
        ConnectContext {
            agent: state.agent.clone(),
            tools: state.tools_for(crate::tools::ToolSurface::Root),
            session_repo: state.session_repo.clone(),
            agent_runners: state.agent_runners.clone(),
            session_event_senders: state.session_event_senders.clone(),
            account_feed_inbox: None,
            root_account_sink: Some(state.account_sink.clone()),
            app_data_dir: Some(state.app_data_dir.clone()),
            config: state.config.clone(),
            provider_registry: state.provider_registry.clone(),
            project_store: state.project_store.clone(),
            workspace_resolver: state.workspace_resolver.clone(),
            project_ids_by_platform: Arc::new(Default::default()),
            permission_checker: state.permission_checker.clone(),
        }
    }

    // Test the publication boundary directly; the caller supplies this handoff
    // only after the response transaction has already committed.
    #[tokio::test]
    async fn connect_changed_target_publishes_transition_and_history_before_release() {
        for with_transition in [false, true] {
            let (_dir, state, _) = crate::model_routing_test_support::state().await;
            let mut session = Session::new("changed-target", "chat-model");
            state.session_repo.save(&mut session).await.unwrap();
            let ctx = supervisor_context(&state);
            let sender = get_or_create_event_sender(&ctx.session_event_senders, &session.id).await;
            let mut events = sender.subscribe();
            let mut account_events = state.account_sink.subscribe();
            let port = ConnectResumePort {
                ctx: ctx.clone(),
                provider_override: None,
                model_origin: None,
                selected_provider_name: None,
                model_reasoning_effort: None,
            };
            let handoff = bamboo_engine::session_app::resume::reserve_response_resume_handoff(
                &port,
                &session.id,
                std::time::Duration::from_secs(5),
            )
            .await
            .expect("reserve response owner");
            let transition = with_transition.then(|| AgentEvent::PlanModeExited {
                session_id: session.id.clone(),
                approved: true,
                restored_mode: "default".into(),
                plan: None,
            });
            assert!(
                publish_recorded_response_and_abandon(
                    ctx,
                    session.id.clone(),
                    handoff,
                    transition,
                    None,
                    "The target changed".into(),
                )
                .await
            );
            let mut saw_transition = false;
            let mut saw_error = false;
            loop {
                let event = tokio::time::timeout(std::time::Duration::from_secs(5), events.recv())
                    .await
                    .expect("session history barrier")
                    .unwrap();
                match event {
                    AgentEvent::PlanModeExited { .. } => saw_transition = true,
                    AgentEvent::Error { message } if message == "The target changed" => {
                        saw_error = true
                    }
                    AgentEvent::SessionHistoryCommitted { .. } => break,
                    _ => {}
                }
            }
            assert_eq!(saw_transition, with_transition);
            assert!(saw_error);
            let mut account_transition = false;
            loop {
                let change =
                    tokio::time::timeout(std::time::Duration::from_secs(5), account_events.recv())
                        .await
                        .expect("account history barrier")
                        .unwrap();
                match &change.event {
                    AgentEvent::PlanModeExited { .. } => account_transition = true,
                    AgentEvent::SessionHistoryCommitted { session_id }
                        if session_id == &session.id =>
                    {
                        break
                    }
                    _ => {}
                }
            }
            assert_eq!(account_transition, with_transition);
            state.shutdown().await;
        }
    }

    #[tokio::test]
    async fn connect_resume_invalid_or_inconsistent_target_keeps_the_question() {
        for mismatch in [false, true] {
            let (_dir, state, providers) = crate::model_routing_test_support::state().await;
            let mut session = Session::new("bound-invalid", "chat-model");
            let target = if mismatch {
                bamboo_domain::ProviderModelRef::new("chat-b", "different-model")
            } else {
                bamboo_domain::ProviderModelRef::new("missing-account", "chat-model")
            };
            session.model_ref = Some(target);
            session.add_message(Message::user("synthetic task"));
            session.set_pending_question(
                "call-1".into(),
                "conclusion_with_options".into(),
                "Continue?".into(),
                vec!["Continue".into()],
                false,
            );
            state.session_repo.save(&mut session).await.unwrap();
            let responder = EngineResponder::new(supervisor_context(&state));
            let outcome = responder
                .respond_and_resume(&session.id, Some("call-1"), "Continue".into())
                .await;
            assert!(
                matches!(outcome, Err(ResponderError::Other(ref message)) if message.contains("answer not consumed"))
            );
            let saved = state.session_repo.load_merged(&session.id).await.unwrap();
            assert_eq!(
                saved
                    .pending_question
                    .as_ref()
                    .map(|ask| ask.tool_call_id.as_str()),
                Some("call-1")
            );
            for provider in providers.values() {
                assert!(provider.models.lock().unwrap().is_empty());
            }
            state.shutdown().await;
        }
    }

    #[tokio::test]
    async fn connect_unbound_legacy_question_keeps_answer_and_allows_stop() {
        let (_dir, state, providers) = crate::model_routing_test_support::state().await;
        let mut session = Session::new("legacy-unbound-pause", "chat-model");
        session
            .metadata
            .insert("created_by_connect_key".into(), "legacy-chat".into());
        assert!(session.model_ref.is_none());
        assert!(session.provider_name().is_none());
        let mut message = Message::assistant("Paused", None);
        message.metadata = Some(serde_json::json!({"runtime_kind":"observation_progress_pause"}));
        let question_id = message.id.clone();
        session.add_message(message);
        session.set_pending_question_with_source(
            question_id.clone(),
            "observation_progress".into(),
            "Paused".into(),
            vec!["Continue".into(), "Stop".into()],
            false,
            bamboo_agent_core::PendingQuestionSource::AgenticClarification,
        );
        state.session_repo.save(&mut session).await.unwrap();
        let responder = EngineResponder::new(supervisor_context(&state));
        let outcome = responder
            .respond_and_resume(&session.id, Some(&question_id), "Continue".into())
            .await;
        assert!(matches!(outcome, Err(ResponderError::Other(ref message))
            if message.contains("binding cannot be verified") && message.contains("answer not consumed")));
        let saved = state.session_repo.load_merged(&session.id).await.unwrap();
        assert_eq!(
            saved
                .pending_question
                .as_ref()
                .map(|ask| ask.tool_call_id.as_str()),
            Some(question_id.as_str())
        );
        assert_eq!(saved.messages.len(), session.messages.len());
        for provider in providers.values() {
            assert!(provider.models.lock().unwrap().is_empty());
        }
        let sender = get_or_create_event_sender(&state.session_event_senders, &session.id).await;
        let mut events = sender.subscribe();
        let outcome = responder
            .respond_and_resume(&session.id, Some(&question_id), "Stop".into())
            .await;
        assert!(
            matches!(outcome, Ok(RespondAndResumeOutcome::NotResumed(message))
            if message == "Stopped by the user.")
        );
        let mut saw_cancelled = false;
        loop {
            let event = tokio::time::timeout(std::time::Duration::from_secs(5), events.recv())
                .await
                .expect("Stop history publication must complete")
                .unwrap();
            match event {
                AgentEvent::Cancelled { .. } => saw_cancelled = true,
                AgentEvent::SessionHistoryCommitted { .. } => break,
                _ => {}
            }
        }
        assert!(saw_cancelled);
        let saved = state.session_repo.load_merged(&session.id).await.unwrap();
        assert!(saved.pending_question.is_none());
        for provider in providers.values() {
            assert!(provider.models.lock().unwrap().is_empty());
        }
        state.shutdown().await;
    }

    #[tokio::test]
    async fn connect_resume_accepts_admitted_legacy_provider_name() {
        let (_dir, state, providers) = crate::model_routing_test_support::state().await;
        let mut session = Session::new("legacy-provider-resume", "chat-model");
        session.set_provider_name("chat-b");
        assert!(session.model_ref.is_none());
        session.add_message(Message::user("synthetic task"));
        session.set_pending_question(
            "call-legacy".into(),
            "conclusion_with_options".into(),
            "Continue?".into(),
            vec!["Continue".into()],
            false,
        );
        state.session_repo.save(&mut session).await.unwrap();
        let responder = EngineResponder::new(supervisor_context(&state));
        let outcome = responder
            .respond_and_resume(&session.id, Some("call-legacy"), "Continue".into())
            .await;
        assert!(matches!(outcome, Ok(RespondAndResumeOutcome::Resumed(_))));
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if !providers["chat-b"].models.lock().unwrap().is_empty() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the admitted legacy account must receive the request");
        assert_eq!(
            *providers["chat-b"].models.lock().unwrap(),
            vec!["chat-model"]
        );
        assert!(providers["startup-a"].models.lock().unwrap().is_empty());
        state.shutdown().await;
    }

    #[tokio::test]
    async fn connect_cancel_before_submit_still_publishes_committed_answer() {
        use actix_web::{test, web, App};
        let (_dir, state, providers) = crate::model_routing_test_support::state().await;
        let state = web::Data::new(state);
        let mut session = Session::new("response-cancel-before-submit", "chat-model");
        let target = bamboo_domain::ProviderModelRef::new("chat-b", "chat-model");
        bamboo_engine::session_app::provider_model::persist_model_ref(&mut session, &target);
        session.add_message(Message::user("synthetic task"));
        session.add_message(Message::tool_result_with_status(
            "call-before",
            serde_json::json!({"status":"awaiting_permission_approval", "permission_type":"execute_command", "resource":"echo synthetic"}).to_string(),
            false,
        ));
        session.set_pending_question(
            "call-before".into(),
            "execute_command".into(),
            "Approve?".into(),
            vec!["Approve".into(), "Deny".into()],
            false,
        );
        state.session_repo.save(&mut session).await.unwrap();
        let sender = get_or_create_event_sender(&state.session_event_senders, &session.id).await;
        let mut events = sender.subscribe();
        let hook = response_test_hooks::install_before_submit(&session.id);
        let responder = EngineResponder::new(supervisor_context(&state));
        let session_id = session.id.clone();
        let caller = tokio::spawn(async move {
            responder
                .respond_and_resume(&session_id, Some("call-before"), "Approve".into())
                .await
        });
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(crate::routes::configure_routes),
        )
        .await;
        tokio::time::timeout(std::time::Duration::from_secs(5), hook.reached.notified())
            .await
            .expect("owner must reach the gate before submission");
        let before = state
            .storage
            .load_session(&session.id)
            .await
            .unwrap()
            .unwrap();
        caller.abort();
        let cancelled = matches!(caller.await, Err(error) if error.is_cancelled());
        let patch = test::call_service(&app, test::TestRequest::patch()
            .uri(&format!("/api/v1/sessions/{}", session.id))
            .set_json(serde_json::json!({"model_ref": {"provider": "fast-c", "model": "cheap-model"}}))
            .to_request()).await;
        hook.resume.notify_one();
        assert!(cancelled);
        assert!(before.pending_question.is_some());
        assert!(
            patch.status().is_success(),
            "PATCH status: {}",
            patch.status()
        );
        let mut saw_error = false;
        loop {
            let event = tokio::time::timeout(std::time::Duration::from_secs(5), events.recv())
                .await
                .expect("response history publication must complete after caller cancellation")
                .unwrap();
            match event {
                AgentEvent::Error { message } if message.contains("Answer was saved") => {
                    saw_error = true
                }
                AgentEvent::SessionHistoryCommitted { .. } => break,
                _ => {}
            }
        }
        assert!(saw_error);
        let saved = state
            .storage
            .load_session(&session.id)
            .await
            .unwrap()
            .unwrap();
        assert!(saved.pending_question.is_none());
        assert_eq!(
            saved.model_ref,
            Some(bamboo_domain::ProviderModelRef::new(
                "fast-c",
                "cheap-model"
            ))
        );
        assert!(saved
            .messages
            .iter()
            .any(
                |message| message.tool_call_id.as_deref() == Some("call-before")
                    && message.content.contains("Approve")
            ));
        assert_eq!(
            saved
                .metadata
                .get(PERMISSION_REEXECUTE_METADATA_KEY)
                .map(String::as_str),
            Some("call-before")
        );
        assert!(!state
            .permission_checker
            .permission_config()
            .unwrap()
            .consume_once(
                &session.id,
                "call-before",
                bamboo_tools::permission::PermissionType::ExecuteCommand,
                "echo synthetic",
            ));
        for provider in providers.values() {
            assert!(provider.models.lock().unwrap().is_empty());
        }
        state.shutdown().await;
    }

    #[tokio::test]
    async fn connect_legacy_alias_stays_on_selected_account_after_default_changes() {
        let (_dir, state, providers) = crate::model_routing_test_support::state().await;
        {
            let mut config = state.config.write().await;
            config.default_provider_instance = Some("chat-b".into());
            config
                .provider_instances
                .get_mut("fast-c")
                .unwrap()
                .extra
                .insert(
                    "runtime_models".into(),
                    serde_json::json!(["cheap-model", "chat-model"]),
                );
        }
        state
            .provider_registry
            .set_runtime_models(std::collections::HashMap::from([
                ("startup-a".into(), vec!["startup-model".into()]),
                ("chat-b".into(), vec!["chat-model".into()]),
                (
                    "fast-c".into(),
                    vec!["cheap-model".into(), "chat-model".into()],
                ),
            ]));
        let mut session = Session::new("legacy-alias-two-approvals", "chat-model");
        let alias = bamboo_domain::ProviderModelRef::new("openai", "chat-model")
            .with_reasoning_effort(bamboo_domain::ReasoningEffort::High);
        bamboo_engine::session_app::provider_model::persist_model_ref(&mut session, &alias);
        session.add_message(Message::user("first synthetic task"));
        session.set_pending_question(
            "call-alias-1".into(),
            "conclusion_with_options".into(),
            "Continue?".into(),
            vec!["Continue".into()],
            false,
        );
        state.session_repo.save(&mut session).await.unwrap();
        let responder = EngineResponder::new(supervisor_context(&state));
        let first = responder
            .respond_and_resume(&session.id, Some("call-alias-1"), "Continue".into())
            .await;
        assert!(matches!(first, Ok(RespondAndResumeOutcome::Resumed(_))));
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let requested = !providers["chat-b"].models.lock().unwrap().is_empty();
                let active = state
                    .agent_runners
                    .read()
                    .await
                    .get(&session.id)
                    .is_some_and(|runner| {
                        matches!(
                            runner.status,
                            bamboo_engine::execution::AgentStatus::Pending
                                | bamboo_engine::execution::AgentStatus::Running
                        )
                    });
                if requested
                    && !active
                    && state
                        .session_activation_router
                        .current_run_id(&session.id)
                        .await
                        .is_none()
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("first resumed execution must finish");
        assert_eq!(
            *providers["chat-b"].models.lock().unwrap(),
            vec!["chat-model"]
        );
        let mut saved = state
            .storage
            .load_session(&session.id)
            .await
            .unwrap()
            .unwrap();
        let canonical = bamboo_domain::ProviderModelRef::new("chat-b", "chat-model")
            .with_reasoning_effort(bamboo_domain::ReasoningEffort::High);
        assert_eq!(saved.model_ref, Some(canonical.clone()));
        assert_eq!(saved.provider_name().as_deref(), Some("chat-b"));
        state.config.write().await.default_provider_instance = Some("fast-c".into());
        saved.set_pending_question(
            "call-alias-2".into(),
            "conclusion_with_options".into(),
            "Continue?".into(),
            vec!["Continue".into()],
            false,
        );
        state.session_repo.save(&mut saved).await.unwrap();
        let second = responder
            .respond_and_resume(&session.id, Some("call-alias-2"), "Continue".into())
            .await;
        assert!(matches!(second, Ok(RespondAndResumeOutcome::Resumed(_))));
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if providers["chat-b"].models.lock().unwrap().len() >= 2 {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("second continuation must use the originally selected account");
        assert_eq!(
            *providers["chat-b"].models.lock().unwrap(),
            vec!["chat-model", "chat-model"]
        );
        assert_eq!(
            *providers["chat-b"].efforts.lock().unwrap(),
            vec![
                Some(bamboo_domain::ReasoningEffort::High),
                Some(bamboo_domain::ReasoningEffort::High)
            ]
        );
        assert!(providers["fast-c"].models.lock().unwrap().is_empty());
        assert!(providers["startup-a"].models.lock().unwrap().is_empty());
        state.shutdown().await;
    }

    #[tokio::test]
    async fn connect_response_profile_change_after_commit_publishes_without_resuming() {
        use actix_web::{test, web, App};
        for cancel_caller in [false, true] {
            let (_dir, state, providers) = crate::model_routing_test_support::state().await;
            let state = web::Data::new(state);
            let mut session = Session::new(
                format!("response-profile-race-{cancel_caller}"),
                "chat-model",
            );
            let target = bamboo_domain::ProviderModelRef::new("chat-b", "chat-model");
            bamboo_engine::session_app::provider_model::persist_model_ref(&mut session, &target);
            session.add_message(Message::user("synthetic task"));
            session.add_message(Message::tool_result_with_status(
                "call-race",
                serde_json::json!({"status":"awaiting_permission_approval", "permission_type":"execute_command", "resource":"echo synthetic"}).to_string(),
                false,
            ));
            session.set_pending_question(
                "call-race".into(),
                "execute_command".into(),
                "Approve?".into(),
                vec!["Approve".into(), "Deny".into()],
                false,
            );
            state.session_repo.save(&mut session).await.unwrap();
            let sender =
                get_or_create_event_sender(&state.session_event_senders, &session.id).await;
            let mut events = sender.subscribe();
            let hook = response_test_hooks::install(&session.id);
            let responder = EngineResponder::new(supervisor_context(&state));
            let session_id = session.id.clone();
            let answer = tokio::spawn(async move {
                responder
                    .respond_and_resume(&session_id, Some("call-race"), "Approve".into())
                    .await
            });
            let app = test::init_service(
                App::new()
                    .app_data(state.clone())
                    .configure(crate::routes::configure_routes),
            )
            .await;
            tokio::time::timeout(std::time::Duration::from_secs(5), hook.reached.notified())
                .await
                .expect("response must commit before the startup gate");
            let committed = state
                .storage
                .load_session(&session.id)
                .await
                .unwrap()
                .unwrap();
            let patch = test::call_service(&app, test::TestRequest::patch()
                .uri(&format!("/api/v1/sessions/{}", session.id))
                .set_json(serde_json::json!({"model_ref": {"provider": "fast-c", "model": "cheap-model"}}))
                .to_request()).await;
            if cancel_caller {
                answer.abort();
            }
            hook.resume.notify_one();
            if cancel_caller {
                assert!(matches!(answer.await, Err(error) if error.is_cancelled()));
            } else {
                let outcome = answer.await.expect("response task must finish");
                assert!(
                    matches!(outcome, Ok(RespondAndResumeOutcome::NotResumed(ref message))
                    if message.contains("Answer was saved") && message.contains("profile"))
                );
            }
            assert!(committed.pending_question.is_none());
            assert!(committed
                .messages
                .iter()
                .any(
                    |message| message.tool_call_id.as_deref() == Some("call-race")
                        && message.content.contains("Approve")
                ));
            assert!(
                patch.status().is_success(),
                "PATCH status: {}",
                patch.status()
            );
            let mut saw_error = false;
            loop {
                let event = tokio::time::timeout(std::time::Duration::from_secs(5), events.recv())
                    .await
                    .expect("response history publication must complete")
                    .unwrap();
                match event {
                    AgentEvent::Error { message } if message.contains("Answer was saved") => {
                        saw_error = true
                    }
                    AgentEvent::SessionHistoryCommitted { .. } => break,
                    _ => {}
                }
            }
            assert!(saw_error);
            let saved = state
                .storage
                .load_session(&session.id)
                .await
                .unwrap()
                .unwrap();
            assert!(saved.pending_question.is_none());
            assert_eq!(
                saved.model_ref,
                Some(bamboo_domain::ProviderModelRef::new(
                    "fast-c",
                    "cheap-model"
                ))
            );
            assert!(saved
                .messages
                .iter()
                .any(
                    |message| message.tool_call_id.as_deref() == Some("call-race")
                        && message.content.contains("Approve")
                ));
            assert_eq!(
                saved
                    .metadata
                    .get(PERMISSION_REEXECUTE_METADATA_KEY)
                    .map(String::as_str),
                Some("call-race")
            );
            assert!(!state
                .permission_checker
                .permission_config()
                .unwrap()
                .consume_once(
                    &session.id,
                    "call-race",
                    bamboo_tools::permission::PermissionType::ExecuteCommand,
                    "echo synthetic",
                ));
            for provider in providers.values() {
                assert!(provider.models.lock().unwrap().is_empty());
            }
            state.shutdown().await;
        }
    }

    #[tokio::test]
    async fn connect_resume_uses_bound_provider_and_legacy_role_effort() {
        let (_dir, state, providers) = crate::model_routing_test_support::state().await;
        let mut session = Session::new("bound-resume", "chat-model");
        session.model_ref = Some(
            bamboo_domain::ProviderModelRef::new("chat-b", "chat-model")
                .with_reasoning_effort(bamboo_domain::ReasoningEffort::High),
        );
        session.set_provider_name("chat-b");
        session.add_message(Message::user("synthetic task"));
        session.set_pending_question(
            "call-1".into(),
            "conclusion_with_options".into(),
            "Continue?".into(),
            vec!["Continue".into()],
            false,
        );
        state.session_repo.save(&mut session).await.unwrap();
        let responder = EngineResponder::new(supervisor_context(&state));
        let outcome = responder
            .respond_and_resume(&session.id, Some("call-1"), "Continue".into())
            .await;
        assert!(matches!(outcome, Ok(RespondAndResumeOutcome::Resumed(_))));
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if !providers["chat-b"].models.lock().unwrap().is_empty() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the selected provider must receive the resumed main request");
        assert_eq!(
            *providers["chat-b"].models.lock().unwrap(),
            vec!["chat-model"]
        );
        assert_eq!(
            providers["chat-b"]
                .efforts
                .lock()
                .unwrap()
                .first()
                .copied()
                .flatten(),
            Some(bamboo_domain::ReasoningEffort::High)
        );
        assert!(providers["startup-a"].models.lock().unwrap().is_empty());
        state.shutdown().await;
    }

    #[tokio::test]
    async fn connect_progress_stop_does_not_require_a_working_model() {
        let (_dir, state, providers) = crate::model_routing_test_support::state().await;
        let mut session = Session::new("bound-stop", "chat-model");
        session.model_ref = Some(bamboo_domain::ProviderModelRef::new(
            "missing-account",
            "chat-model",
        ));
        let mut message = Message::assistant("Paused", None);
        message.metadata = Some(serde_json::json!({"runtime_kind":"observation_progress_pause"}));
        let question_id = message.id.clone();
        session.add_message(message);
        session.set_pending_question_with_source(
            question_id.clone(),
            "observation_progress".into(),
            "Paused".into(),
            vec!["Continue".into(), "Stop".into()],
            false,
            bamboo_agent_core::PendingQuestionSource::AgenticClarification,
        );
        state.session_repo.save(&mut session).await.unwrap();
        let sender = get_or_create_event_sender(&state.session_event_senders, &session.id).await;
        let mut events = sender.subscribe();
        let responder = EngineResponder::new(supervisor_context(&state));
        let outcome = responder
            .respond_and_resume(&session.id, Some(&question_id), "Stop".into())
            .await;
        assert!(matches!(
            outcome,
            Ok(RespondAndResumeOutcome::NotResumed(message)) if message == "Stopped by the user."
        ));
        let mut saw_message = false;
        let mut saw_cancelled = false;
        loop {
            let event = tokio::time::timeout(std::time::Duration::from_secs(5), events.recv())
                .await
                .expect("Stop history publication must complete")
                .unwrap();
            match event {
                AgentEvent::MessageAppended { content, .. }
                    if content.starts_with("Stopped this run.") =>
                {
                    saw_message = true;
                }
                AgentEvent::Cancelled { .. } => saw_cancelled = true,
                AgentEvent::SessionHistoryCommitted { .. } => break,
                _ => {}
            }
        }
        assert!(saw_message && saw_cancelled);
        let saved = state.session_repo.load_merged(&session.id).await.unwrap();
        assert!(saved.pending_question.is_none());
        for provider in providers.values() {
            assert!(provider.models.lock().unwrap().is_empty());
        }
        state.shutdown().await;
    }

    #[derive(Default)]
    struct ResumePairProvider {
        models: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl bamboo_llm::LLMProvider for ResumePairProvider {
        async fn chat_stream(
            &self,
            _: &[Message],
            _: &[bamboo_agent_core::ToolSchema],
            _: Option<u32>,
            model: &str,
        ) -> Result<bamboo_llm::LLMStream, bamboo_llm::LLMError> {
            self.models.lock().unwrap().push(model.to_string());
            Ok(Box::pin(futures::stream::iter(vec![
                Ok(bamboo_llm::LLMChunk::Token("done".into())),
                Ok(bamboo_llm::LLMChunk::Done),
            ])))
        }
    }

    #[tokio::test]
    async fn connect_permission_replay_resumes_with_selected_provider() {
        use crate::app_state::resume_adapter::supervisor_tests::Fixture;
        let fixture = Box::pin(Fixture::pending()).await;
        fixture.prepare_workspace_catalog().await;
        fixture.formal_approve().await;
        let provider = Arc::new(ResumePairProvider::default());
        // The synthetic client is not created by the Host factory. Use an
        // unmanaged registry instead of claiming it has a managed binding.
        fixture
            .state
            .provider_registry
            .replace_with(bamboo_llm::ProviderRegistry::new(
                std::collections::HashMap::from([(
                    "chat-b".into(),
                    provider.clone() as Arc<dyn bamboo_llm::LLMProvider>,
                )]),
                "chat-b".into(),
            ));
        fixture
            .state
            .config
            .write()
            .await
            .provider_instances
            .insert(
                "chat-b".into(),
                serde_json::from_value(serde_json::json!({
                    "provider_type":"openai", "enabled":true, "model":"test-model"
                }))
                .unwrap(),
            );
        let mut session = fixture.reload().await;
        let target = bamboo_domain::ProviderModelRef::new("chat-b", "test-model");
        bamboo_engine::session_app::provider_model::persist_model_ref(&mut session, &target);
        fixture.state.session_repo.save(&mut session).await.unwrap();
        let ctx = supervisor_context(&fixture.state);
        let snapshot = ctx.config.read().await.clone();
        let selected = bamboo_engine::model_config_helper::try_resolve_primary_model(
            &snapshot,
            &ctx.provider_registry,
            &target,
        )
        .unwrap();
        let config =
            resolve_resume_config_snapshot(&snapshot, &ctx.provider_registry, &session, None);
        let port = ConnectResumePort {
            ctx,
            provider_override: Some(selected.provider),
            model_origin: selected.managed_origin,
            selected_provider_name: Some(selected.target.provider),
            model_reasoning_effort: None,
        };
        let outcome = bamboo_engine::session_app::resume::resume_session_execution(
            &port,
            &session.id,
            config,
        )
        .await;
        assert!(matches!(
            outcome,
            bamboo_engine::session_app::types::ResumeOutcome::Started { .. }
        ));
        fixture.settled(1, false).await;
        assert_eq!(*provider.models.lock().unwrap(), vec!["test-model"]);
    }

    #[tokio::test]
    async fn connect_supervisor_typed_replay_restores_identity_and_rejects_corruption() {
        use crate::app_state::resume_adapter::supervisor_tests::Fixture;
        for corrupt in [false, true] {
            let fixture = Box::pin(Fixture::pending()).await;
            fixture.prepare_workspace_catalog().await;
            fixture.formal_approve().await;
            if corrupt {
                fixture.corrupt().await;
            }
            let ctx = supervisor_context(&fixture.state);
            let session = fixture.reload().await;
            let config = resolve_resume_config_snapshot(
                &*ctx.config.read().await,
                &ctx.provider_registry,
                &session,
                None,
            );
            let outcome = bamboo_engine::session_app::resume::resume_session_execution(
                &ConnectResumePort {
                    ctx,
                    provider_override: None,
                    model_origin: None,
                    selected_provider_name: None,
                    model_reasoning_effort: None,
                },
                &session.id,
                config,
            )
            .await;
            assert!(matches!(
                outcome,
                bamboo_engine::session_app::types::ResumeOutcome::Started { .. }
            ));
            fixture.settled(usize::from(!corrupt), corrupt).await;
        }
    }

    #[tokio::test]
    async fn connect_resume_preserves_approved_call_after_root_tool_revision_changes() {
        use crate::app_state::resume_adapter::supervisor_tests::Fixture;
        let fixture = Box::pin(Fixture::ordinary_root_pending()).await;
        fixture.prepare_workspace_catalog().await;
        let handoff = fixture.ordinary_root_approval_handoff().await;
        let session = fixture.tighten_ordinary_root().await;

        let ctx = supervisor_context(&fixture.state);
        let config = resolve_resume_config_snapshot(
            &*ctx.config.read().await,
            &ctx.provider_registry,
            &session,
            None,
        );
        let outcome = bamboo_engine::session_app::resume::resume_session_execution_with_handoff(
            &ConnectResumePort {
                ctx,
                provider_override: None,
                model_origin: None,
                selected_provider_name: None,
                model_reasoning_effort: None,
            },
            &session.id,
            session.clone(),
            config,
            handoff,
        )
        .await;
        assert!(matches!(
            outcome,
            bamboo_engine::session_app::types::ResumeOutcome::Started { .. }
        ));
        fixture
            .settled_with_retained_revision_mismatch(&session)
            .await;
    }

    #[tokio::test]
    async fn connect_supervisor_text_answer_cannot_replace_a_typed_receipt() {
        use crate::app_state::resume_adapter::supervisor_tests::{Fixture, CALL};
        let fixture = Box::pin(Fixture::pending()).await;
        let responder = EngineResponder::new(supervisor_context(&fixture.state));
        let outcome = responder
            .respond_and_resume(&fixture.original.session_id, Some(CALL), "Approve".into())
            .await;
        // This legacy Supervisor fixture has no captured provider binding. A
        // text answer cannot establish one or create a typed decision receipt.
        assert!(matches!(outcome, Err(ResponderError::Other(ref message))
            if message.contains("binding cannot be verified") && message.contains("answer not consumed")));
        let session = fixture.reload().await;
        assert_eq!(
            session
                .pending_question
                .as_ref()
                .map(|ask| ask.tool_call_id.as_str()),
            Some(CALL)
        );
        let result = session
            .messages
            .iter()
            .rev()
            .find(|message| message.tool_call_id.as_deref() == Some(CALL))
            .unwrap();
        assert!(result
            .metadata
            .as_ref()
            .unwrap()
            .get("permission_decision_receipt")
            .is_none());
        fixture.state.shutdown().await;
    }

    #[tokio::test]
    async fn connect_supervisor_bound_model_without_root_cas_keeps_answer() {
        use crate::app_state::resume_adapter::supervisor_tests::{Fixture, CALL};
        for inherited in [false, true] {
            let fixture = Box::pin(Fixture::pending()).await;
            fixture.state.config.write().await.provider_instances.insert(
                "test-provider".into(),
                serde_json::from_value(serde_json::json!({
                    "provider_type":"openai", "enabled":true, "model":"test-model",
                    "runtime_models":["test-model"]
                })).unwrap(),
            );
            let mut session = fixture.reload().await;
            bamboo_engine::session_app::provider_model::persist_model_ref(
                &mut session,
                &bamboo_domain::ProviderModelRef::new("test-provider", "test-model"),
            );
            if inherited {
                let runtime = session.agent_runtime_state.get_or_insert_with(||
                    bamboo_domain::AgentRuntimeState::new("legacy-supervisor"));
                runtime.waiting_for_children = Some(bamboo_domain::WaitingForChildrenState::for_children(
                    vec!["ordinary-child".into()], bamboo_domain::ChildWaitPolicy::All, chrono::Utc::now(),
                ));
            }
            fixture.state.session_repo.save(&mut session).await.unwrap();
            let responder = EngineResponder::new(supervisor_context(&fixture.state));
            let outcome = responder.respond_and_resume(&session.id, Some(CALL), "Approve".into()).await;
            assert!(matches!(outcome, Err(ResponderError::Other(ref message))
                if message.contains("no bound startup writer") && message.contains("answer not consumed")));
            let saved = fixture.reload().await;
            assert_eq!(saved.pending_question.as_ref().map(|ask| ask.tool_call_id.as_str()), Some(CALL));
            assert!(saved.messages.iter().rev()
                .find(|message| message.tool_call_id.as_deref() == Some(CALL))
                .and_then(|message| message.metadata.as_ref())
                .is_none_or(|metadata| metadata.get("permission_decision_receipt").is_none()));
            fixture.state.shutdown().await;
        }
    }

    // Keep the original replay-boundary assertion independently of the
    // higher-level legacy binding preflight above.
    #[tokio::test]
    async fn connect_untyped_supervisor_response_does_not_create_receipt_or_replay() {
        use crate::app_state::resume_adapter::supervisor_tests::{Fixture, CALL};
        let fixture = Box::pin(Fixture::pending()).await;
        let input = RespondInput {
            session_id: fixture.original.session_id.clone(),
            user_response: "Approve".into(),
            model: None,
            model_ref: None,
            provider: None,
            reasoning_effort: None,
        };
        let (session, _, _, permission_grants) =
            bamboo_engine::session_app::respond::submit_pending_response_checked(
                &fixture.state.session_repo,
                input,
                Some(CALL.into()),
            )
            .await
            .unwrap();
        let ctx = supervisor_context(&fixture.state);
        assert!(
            !permission_grants.is_empty(),
            "the legacy answer must imply a grant"
        );
        let request_id = session
            .metadata
            .get(PERMISSION_REEXECUTE_METADATA_KEY)
            .expect("the response must mark the approved call");
        for (perm_type, resource) in &permission_grants {
            ctx.permission_checker.grant_once(
                &session.id,
                request_id,
                *perm_type,
                resource.clone(),
            );
        }
        let config = resolve_resume_config_snapshot(
            &*ctx.config.read().await,
            &ctx.provider_registry,
            &session,
            None,
        );
        let outcome = bamboo_engine::session_app::resume::resume_session_execution(
            &ConnectResumePort {
                ctx,
                provider_override: None,
                model_origin: None,
                selected_provider_name: None,
                model_reasoning_effort: None,
            },
            &session.id,
            config,
        )
        .await;
        assert!(matches!(
            outcome,
            bamboo_engine::session_app::types::ResumeOutcome::Started { .. }
        ));
        fixture.settled(0, true).await;
        let saved = fixture.reload().await;
        let result = saved
            .messages
            .iter()
            .rev()
            .find(|message| message.tool_call_id.as_deref() == Some(CALL))
            .unwrap();
        assert!(result
            .metadata
            .as_ref()
            .unwrap()
            .get("permission_decision_receipt")
            .is_none());
    }

    fn append_permission_round(
        session: &mut Session,
        call_id: &str,
        generation: &str,
        arguments: &str,
        result_id: &str,
    ) {
        session.add_message(Message::assistant(
            "",
            Some(vec![ToolCall {
                id: call_id.to_string(),
                tool_type: "function".to_string(),
                function: FunctionCall {
                    name: "Write".to_string(),
                    arguments: arguments.to_string(),
                },
            }]),
        ));
        let mut result = Message::tool_result(
            call_id,
            serde_json::json!({
                "status": "awaiting_permission_approval",
                "permission_request": { "request_generation": generation }
            })
            .to_string(),
        );
        result.id = result_id.to_string();
        session.add_message(result);
    }

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
    fn connect_replay_targets_current_generation_when_provider_reuses_id() {
        let mut session = Session::new("session", "model");
        append_permission_round(
            &mut session,
            "reused",
            "generation-old",
            r#"{"content":"old"}"#,
            "result-old",
        );
        append_permission_round(
            &mut session,
            "reused",
            "generation-current",
            r#"{"content":"current"}"#,
            "result-current",
        );

        let target = find_pending_tool_call(&session, "reused", Some("generation-current"))
            .expect("current generation target");
        assert_eq!(
            target.tool_call().function.arguments,
            r#"{"content":"current"}"#
        );
        assert!(apply_tool_result(
            &mut session,
            &target,
            "executed current".to_string(),
            true,
        ));
        assert!(session.messages[1].content.contains("generation-old"));
        assert_eq!(session.messages[3].content, "executed current");
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
