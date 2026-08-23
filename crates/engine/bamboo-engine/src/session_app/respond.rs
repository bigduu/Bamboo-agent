//! Respond use case: submit a user response to a pending question.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};

use bamboo_agent_core::{Message, PendingQuestion, Session};
use bamboo_domain::session::runtime_state::{AgentRuntimeState, PlanModeState, PlanModeStatus};
use bamboo_domain::{
    latest_response_occurrence, ResponseOccurrence, SessionPermissionMode,
    CONSUMED_CLARIFICATION_IDS_KEY, CONSUMED_RESPONSE_OCCURRENCES_KEY,
};
use bamboo_tools::permission::{PermissionDecisionKind, PermissionDecisionReceipt, PermissionType};
use chrono::Utc;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use tokio::sync::{Mutex, OwnedMutexGuard};

use super::errors::RespondError;
use super::execute::mark_startup_handoff;
use super::provider_model::{derive_model_ref, persist_legacy_model_provider, persist_model_ref};
use super::repository::SessionAccess;
use super::types::RespondInput;

const CLARIFICATION_RESUME_PENDING_KEY: &str = "clarification_resume_pending";
const CONCLUSION_WITH_OPTIONS_RESUME_PENDING_KEY: &str = "conclusion_with_options_resume_pending";

type AppliedPendingResponse = (
    String,
    Option<PlanModeTransition>,
    Vec<(PermissionType, String)>,
);

/// Process-local serialization for consuming one pending question. The
/// persistence layer serializes individual loads and saves, but releasing its
/// lock between those operations would let two responders both observe and
/// consume the same question. Every response entrypoint reaches this use case,
/// so holding this gate across load -> validate -> save makes one consumer win
/// and forces later callers to reload the already-consumed state.
struct PendingResponseGate {
    lock: Arc<Mutex<()>>,
    waiters: AtomicUsize,
}

impl PendingResponseGate {
    fn new() -> Self {
        Self {
            lock: Arc::new(Mutex::new(())),
            waiters: AtomicUsize::new(0),
        }
    }
}

struct PendingResponseWaiter(Arc<PendingResponseGate>);

impl Drop for PendingResponseWaiter {
    fn drop(&mut self) {
        self.0.waiters.fetch_sub(1, Ordering::SeqCst);
    }
}

fn pending_response_locks() -> &'static DashMap<String, Weak<PendingResponseGate>> {
    static LOCKS: OnceLock<DashMap<String, Weak<PendingResponseGate>>> = OnceLock::new();
    LOCKS.get_or_init(DashMap::new)
}

fn pending_response_lock(session_id: &str) -> Arc<PendingResponseGate> {
    let locks = pending_response_locks();
    locks.retain(|_, lock| lock.strong_count() > 0);
    match locks.entry(session_id.to_string()) {
        Entry::Occupied(mut entry) => {
            if let Some(lock) = entry.get().upgrade() {
                lock
            } else {
                let lock = Arc::new(PendingResponseGate::new());
                entry.insert(Arc::downgrade(&lock));
                lock
            }
        }
        Entry::Vacant(entry) => {
            let lock = Arc::new(PendingResponseGate::new());
            entry.insert(Arc::downgrade(&lock));
            lock
        }
    }
}

/// Process-local ownership for one session's complete response transaction.
/// High-level HTTP, Connect, and Gold paths hold this from authoritative
/// preflight through successor dispatch, so a stale duplicate cannot reserve
/// and later cancel a phantom runner after the real successor has completed.
pub struct PendingResponseGuard {
    session_id: String,
    _gate: Arc<PendingResponseGate>,
    _guard: OwnedMutexGuard<()>,
}

impl PendingResponseGuard {
    fn ensure_session(&self, session_id: &str) -> Result<(), RespondError> {
        if self.session_id == session_id {
            Ok(())
        } else {
            Err(RespondError::InvalidResponse(
                "response transaction guard belongs to a different session".to_string(),
            ))
        }
    }
}

/// Acquire the shared response single-flight used by every response source.
pub async fn acquire_pending_response_guard(session_id: &str) -> PendingResponseGuard {
    let gate = pending_response_lock(session_id);
    gate.waiters.fetch_add(1, Ordering::SeqCst);
    let waiter = PendingResponseWaiter(gate.clone());
    let guard = gate.lock.clone().lock_owned().await;
    drop(waiter);
    PendingResponseGuard {
        session_id: session_id.to_string(),
        _gate: gate,
        _guard: guard,
    }
}

/// Number of callers currently waiting to enter a session's response gate.
/// Exposed for deterministic concurrency tests and lightweight diagnostics.
#[doc(hidden)]
pub fn pending_response_waiter_count(session_id: &str) -> usize {
    pending_response_locks()
        .get(session_id)
        .and_then(|entry| entry.upgrade())
        .map(|gate| gate.waiters.load(Ordering::SeqCst))
        .unwrap_or(0)
}

/// Reload the exact snapshot a response CAS would mutate while holding the
/// shared response single-flight. This must happen before successor
/// reservation so an already-consumed/stale response allocates no runner.
pub async fn inspect_pending_response_guarded(
    repo: &dyn SessionAccess,
    session_id: &str,
    guard: &PendingResponseGuard,
) -> Result<Option<Session>, RespondError> {
    guard.ensure_session(session_id)?;
    Ok(repo.inspect_for_response(session_id).await?)
}

/// Session-metadata key marking a tool call that was approved through a permission
/// prompt and must be RE-EXECUTED on resume. The gated tool never actually ran
/// (the permission gate intercepted it before execution), so on approval the
/// server resume adapter re-runs it and writes the real output back — instead of
/// leaving the model to infer/fabricate it. Value = the tool_call_id.
pub const PERMISSION_REEXECUTE_METADATA_KEY: &str = "permission.reexecute_tool_call_id";
pub const PERMISSION_REEXECUTE_GENERATION_METADATA_KEY: &str =
    "permission.reexecute_request_generation";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseSource {
    Human,
    Gold,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanModeTransition {
    Entered {
        reason: Option<String>,
        pre_permission_mode: String,
        entered_at: chrono::DateTime<chrono::Utc>,
        status: PlanModeStatus,
        plan_file_path: Option<String>,
    },
    Exited {
        approved: bool,
        restored_mode: String,
        plan: Option<String>,
    },
}

/// Submit a pending response: load session, validate, update messages,
/// apply plan mode transitions, persist, and return the updated session.
///
/// The caller (handler) is responsible for auto-resume triggering.
pub async fn submit_pending_response(
    repo: &dyn SessionAccess,
    input: RespondInput,
) -> Result<
    (
        Session,
        String,
        Option<PlanModeTransition>,
        Vec<(PermissionType, String)>,
    ),
    RespondError,
> {
    submit_pending_response_checked(repo, input, None).await
}

/// Submit a response guarded by the exact pending tool-call identity displayed
/// to a typed client. The separate parameter keeps the established public
/// [`RespondInput`] struct source-compatible for SDK/in-process callers.
pub async fn submit_pending_response_checked(
    repo: &dyn SessionAccess,
    input: RespondInput,
    expected_tool_call_id: Option<String>,
) -> Result<
    (
        Session,
        String,
        Option<PlanModeTransition>,
        Vec<(PermissionType, String)>,
    ),
    RespondError,
> {
    submit_pending_response_with_source_checked(
        repo,
        input,
        expected_tool_call_id,
        ResponseSource::Human,
    )
    .await
}

/// Human-response variant for callers that already hold the shared response
/// transaction guard across preflight, reservation, CAS, and dispatch.
pub async fn submit_pending_response_checked_guarded(
    repo: &dyn SessionAccess,
    input: RespondInput,
    expected_tool_call_id: Option<String>,
    guard: &PendingResponseGuard,
) -> Result<
    (
        Session,
        String,
        Option<PlanModeTransition>,
        Vec<(PermissionType, String)>,
    ),
    RespondError,
> {
    submit_pending_response_with_source_checked_guarded(
        repo,
        input,
        expected_tool_call_id,
        ResponseSource::Human,
        guard,
    )
    .await
}

/// Typed-permission variant that persists the exact decision receipt in the
/// same durable session mutation that consumes the pending question.
pub async fn submit_pending_permission_response_checked_guarded(
    repo: &dyn SessionAccess,
    input: RespondInput,
    expected_tool_call_id: Option<String>,
    permission_receipt: PermissionDecisionReceipt,
    guard: &PendingResponseGuard,
) -> Result<
    (
        Session,
        String,
        Option<PlanModeTransition>,
        Vec<(PermissionType, String)>,
    ),
    RespondError,
> {
    submit_pending_response_with_source_checked_guarded_inner(
        repo,
        input,
        expected_tool_call_id,
        ResponseSource::Human,
        Some(permission_receipt),
        guard,
    )
    .await
}

pub async fn submit_pending_response_with_source(
    repo: &dyn SessionAccess,
    input: RespondInput,
    response_source: ResponseSource,
) -> Result<
    (
        Session,
        String,
        Option<PlanModeTransition>,
        Vec<(PermissionType, String)>,
    ),
    RespondError,
> {
    submit_pending_response_with_source_checked(repo, input, None, response_source).await
}

pub async fn submit_pending_response_with_source_checked(
    repo: &dyn SessionAccess,
    input: RespondInput,
    expected_tool_call_id: Option<String>,
    response_source: ResponseSource,
) -> Result<
    (
        Session,
        String,
        Option<PlanModeTransition>,
        Vec<(PermissionType, String)>,
    ),
    RespondError,
> {
    let guard = acquire_pending_response_guard(&input.session_id).await;
    submit_pending_response_with_source_checked_guarded(
        repo,
        input,
        expected_tool_call_id,
        response_source,
        &guard,
    )
    .await
}

/// Guarded form for high-level response + resume transactions. The caller
/// must retain `guard` until the exact successor reservation has been handed
/// to its detached execution owner.
pub async fn submit_pending_response_with_source_checked_guarded(
    repo: &dyn SessionAccess,
    input: RespondInput,
    expected_tool_call_id: Option<String>,
    response_source: ResponseSource,
    guard: &PendingResponseGuard,
) -> Result<
    (
        Session,
        String,
        Option<PlanModeTransition>,
        Vec<(PermissionType, String)>,
    ),
    RespondError,
> {
    submit_pending_response_with_source_checked_guarded_inner(
        repo,
        input,
        expected_tool_call_id,
        response_source,
        None,
        guard,
    )
    .await
}

async fn submit_pending_response_with_source_checked_guarded_inner(
    repo: &dyn SessionAccess,
    input: RespondInput,
    expected_tool_call_id: Option<String>,
    response_source: ResponseSource,
    permission_receipt: Option<PermissionDecisionReceipt>,
    guard: &PendingResponseGuard,
) -> Result<
    (
        Session,
        String,
        Option<PlanModeTransition>,
        Vec<(PermissionType, String)>,
    ),
    RespondError,
> {
    guard.ensure_session(&input.session_id)?;

    let applied = Arc::new(StdMutex::new(None));
    let mutation_outcome = applied.clone();
    let mutation_input = input.clone();
    let mutation_expected_tool_call_id = expected_tool_call_id.clone();
    let mutation_permission_receipt = permission_receipt.clone();
    let session = repo
        .mutate_for_response(
            &input.session_id,
            Box::new(move |session| {
                let outcome = apply_pending_response(
                    session,
                    &mutation_input,
                    mutation_expected_tool_call_id.as_deref(),
                    response_source,
                    mutation_permission_receipt.as_ref(),
                )?;
                *mutation_outcome
                    .lock()
                    .expect("response outcome lock poisoned") = Some(outcome);
                Ok(())
            }),
        )
        .await?
        .ok_or_else(|| RespondError::NotFound(input.session_id.clone()))?;
    let (user_response, plan_mode_transition, permission_grants) = applied
        .lock()
        .expect("response outcome lock poisoned")
        .take()
        .expect("successful response mutation records its outcome");

    tracing::info!(
        "[{}] Response processed successfully, agent loop can resume",
        input.session_id
    );

    Ok((
        session,
        user_response,
        plan_mode_transition,
        permission_grants,
    ))
}

fn apply_pending_response(
    session: &mut Session,
    input: &RespondInput,
    expected_tool_call_id: Option<&str>,
    response_source: ResponseSource,
    permission_receipt: Option<&PermissionDecisionReceipt>,
) -> Result<AppliedPendingResponse, RespondError> {
    let pending = session
        .pending_question
        .take()
        .ok_or(RespondError::NoPendingQuestion)?;

    if let Some(expected) = expected_tool_call_id {
        if pending.tool_call_id != expected {
            let actual = pending.tool_call_id.clone();
            session.pending_question = Some(pending);
            return Err(RespondError::PendingQuestionMismatch {
                expected: expected.to_string(),
                actual,
            });
        }
    }

    if let Some(receipt) = permission_receipt {
        if receipt.session_id != input.session_id
            || receipt.decision.request_id != pending.tool_call_id
        {
            session.pending_question = Some(pending);
            return Err(RespondError::InvalidResponse(
                "permission receipt identity does not match the pending question".to_string(),
            ));
        }
        let current_generation = session
            .messages
            .iter()
            .rev()
            .find(|message| message.tool_call_id.as_deref() == Some(pending.tool_call_id.as_str()))
            .and_then(permission_request_generation);
        if current_generation.as_deref() != Some(receipt.decision.request_generation.as_str()) {
            session.pending_question = Some(pending);
            return Err(RespondError::InvalidResponse(
                "permission receipt generation does not match the pending operation".to_string(),
            ));
        }
    }

    // Typed permission control flow comes exclusively from the structured
    // receipt. Display strings and localized options remain transcript-only.
    // Legacy clarifications still validate their selected display option.
    if permission_receipt.is_none() {
        if let Err(error_message) = validate_pending_response(&pending, &input.user_response) {
            // Put the pending question back when validation fails.
            session.pending_question = Some(pending);
            return Err(RespondError::InvalidResponse(error_message));
        }
    }

    let tool_call_id = pending.tool_call_id.clone();
    tracing::debug!(
        "[{}] Looking for tool result message with tool_call_id: {}",
        input.session_id,
        tool_call_id
    );

    let reviewed_plan = extract_exit_plan_from_tool_result_message(session, &tool_call_id);

    // Permission grants implied by approving a permission prompt. Read from the
    // (still-unmodified) synthesized tool-result payload, BEFORE it is overwritten
    // by the user's selection below.
    let typed_permission_approved = permission_receipt.is_some_and(|receipt| {
        matches!(
            receipt.decision.decision,
            PermissionDecisionKind::AllowOnce
                | PermissionDecisionKind::AllowSession
                | PermissionDecisionKind::AllowWorkspace
                | PermissionDecisionKind::AllowGlobal
        )
    });
    let permission_approved = permission_receipt
        .map(|_| typed_permission_approved)
        .unwrap_or_else(|| is_permission_approval(&input.user_response));
    let permission_grants = if permission_approved {
        extract_permission_grants_from_tool_result_message(session, &tool_call_id)
    } else {
        Vec::new()
    };
    let should_reexecute = permission_receipt
        .map(|_| typed_permission_approved)
        .unwrap_or(!permission_grants.is_empty());
    if should_reexecute {
        // Approved a permission prompt: mark the gated tool call for re-execution
        // on resume so the operation actually runs (real output) rather than the
        // model inferring it. Consumed by the server resume adapter.
        session.metadata.insert(
            PERMISSION_REEXECUTE_METADATA_KEY.to_string(),
            tool_call_id.clone(),
        );
        if let Some(receipt) = permission_receipt {
            session.metadata.insert(
                PERMISSION_REEXECUTE_GENERATION_METADATA_KEY.to_string(),
                receipt.decision.request_generation.clone(),
            );
        } else {
            session
                .metadata
                .remove(PERMISSION_REEXECUTE_GENERATION_METADATA_KEY);
        }
    } else if permission_receipt.is_some() {
        // A typed deny cannot inherit replay markers from an older occurrence.
        session.metadata.remove(PERMISSION_REEXECUTE_METADATA_KEY);
        session
            .metadata
            .remove(PERMISSION_REEXECUTE_GENERATION_METADATA_KEY);
    }

    // ---- Update or append tool result message ----
    let found = update_or_append_tool_result_message(
        session,
        &tool_call_id,
        &input.user_response,
        response_source,
    );
    if let Some(receipt) = permission_receipt {
        debug_assert!(persist_permission_decision_receipt(
            session,
            &tool_call_id,
            receipt
        ));
    }
    if found {
        tracing::info!(
            "[{}] Updated existing tool result message",
            input.session_id
        );
    } else {
        tracing::warn!(
            "[{}] Tool result message not found for tool_call_id: {}, added fallback message",
            input.session_id,
            tool_call_id
        );
    }

    // ---- Plan mode state transitions ----
    let plan_mode_transition =
        apply_plan_mode_transition(session, &pending, &input.user_response, reviewed_plan);

    // ---- Clear pending question and set resume marker ----
    session.clear_pending_question();
    record_consumed_clarification(session, &tool_call_id);
    session.metadata.remove("runtime.suspend_reason");
    session.metadata.insert(
        CLARIFICATION_RESUME_PENDING_KEY.to_string(),
        "true".to_string(),
    );
    session.metadata.insert(
        CONCLUSION_WITH_OPTIONS_RESUME_PENDING_KEY.to_string(),
        "true".to_string(),
    );
    mark_startup_handoff(session);

    // ---- Merge model/reasoning from request ----
    let request_model_ref = derive_model_ref(
        input.model_ref.as_ref(),
        input.provider.as_deref(),
        input.model.as_deref(),
    );
    if let Some(model_ref) = request_model_ref.as_ref() {
        persist_model_ref(session, model_ref);
    } else {
        persist_legacy_model_provider(session, input.model.as_deref(), input.provider.as_deref());
    }
    if let Some(reasoning_effort) = input.reasoning_effort {
        session.reasoning_effort = Some(reasoning_effort);
    }

    Ok((
        input.user_response.clone(),
        plan_mode_transition,
        permission_grants,
    ))
}

fn record_consumed_clarification(session: &mut Session, tool_call_id: &str) {
    let mut legacy_consumed = session
        .metadata
        .get(CONSUMED_CLARIFICATION_IDS_KEY)
        .and_then(|value| serde_json::from_str::<Vec<String>>(value).ok())
        .unwrap_or_default();
    legacy_consumed.retain(|existing| existing != tool_call_id);
    legacy_consumed.push(tool_call_id.to_string());
    if legacy_consumed.len() > 64 {
        legacy_consumed.drain(..legacy_consumed.len() - 64);
    }
    if let Ok(serialized) = serde_json::to_string(&legacy_consumed) {
        session
            .metadata
            .insert(CONSUMED_CLARIFICATION_IDS_KEY.to_string(), serialized);
    }

    let Some(occurrence) = latest_response_occurrence(session, tool_call_id) else {
        return;
    };
    let mut consumed = session
        .metadata
        .get(CONSUMED_RESPONSE_OCCURRENCES_KEY)
        .and_then(|value| serde_json::from_str::<Vec<ResponseOccurrence>>(value).ok())
        .unwrap_or_default();
    consumed.retain(|existing| existing != &occurrence);
    consumed.push(occurrence);
    if consumed.len() > 64 {
        consumed.drain(..consumed.len() - 64);
    }
    if let Ok(serialized) = serde_json::to_string(&consumed) {
        session
            .metadata
            .insert(CONSUMED_RESPONSE_OCCURRENCES_KEY.to_string(), serialized);
    }
}

/// Apply plan mode state transitions based on the pending question tool and user response.
fn apply_plan_mode_transition(
    session: &mut Session,
    pending: &PendingQuestion,
    user_response: &str,
    reviewed_plan: Option<String>,
) -> Option<PlanModeTransition> {
    match pending.tool_name.as_str() {
        "EnterPlanMode" if user_response.to_lowercase().contains("enter plan mode") => {
            let pre_mode = session
                .agent_runtime_state
                .as_ref()
                .map(|state| state.effective_permission_mode().as_str().to_string())
                .unwrap_or_else(|| SessionPermissionMode::Default.as_str().to_string());

            let entered_at = Utc::now();
            let status = PlanModeStatus::Exploring;
            let runtime_state = session
                .agent_runtime_state
                .get_or_insert_with(|| AgentRuntimeState::new(uuid::Uuid::new_v4().to_string()));
            runtime_state.plan_mode = Some(PlanModeState {
                entered_at,
                pre_permission_mode: pre_mode.clone(),
                plan_file_path: None,
                status,
            });
            tracing::info!(
                session_id = %session.id,
                "Entered plan mode"
            );
            Some(PlanModeTransition::Entered {
                reason: Some(pending.question.clone()),
                pre_permission_mode: pre_mode,
                entered_at,
                status,
                plan_file_path: None,
            })
        }
        "ExitPlanMode" if is_exit_plan_mode_approved(user_response) => {
            let restored_mode = session
                .agent_runtime_state
                .as_ref()
                .map(|state| state.effective_permission_mode().as_str().to_string())
                .unwrap_or_else(|| "default".to_string());
            if let Some(ref mut runtime_state) = session.agent_runtime_state {
                // The typed requested mode remains live while Plan is active and
                // may have been changed by a newer PATCH. Exiting Plan clears
                // only the overlay; the old pre-mode is event history, never a
                // write authority that may roll back the newer request.
                runtime_state.plan_mode = None;
            }
            tracing::info!(
                session_id = %session.id,
                "Exited plan mode"
            );
            Some(PlanModeTransition::Exited {
                approved: true,
                restored_mode,
                plan: reviewed_plan,
            })
        }
        _ => None,
    }
}

/// Check if the user response approves exiting plan mode.
fn is_exit_plan_mode_approved(user_response: &str) -> bool {
    let lower = user_response.to_lowercase();
    lower.contains("approve") && !lower.contains("stay in plan mode")
}

// ---- Internal helpers ----

pub fn validate_pending_response(
    pending: &PendingQuestion,
    user_response: &str,
) -> Result<(), String> {
    if pending.allow_custom {
        return Ok(());
    }

    let valid = pending.options.iter().any(|option| option == user_response);
    if valid {
        Ok(())
    } else {
        let options_str = pending.options.join(", ");
        Err(format!("Response must be one of: {options_str}"))
    }
}

pub fn update_or_append_tool_result_message(
    session: &mut Session,
    tool_call_id: &str,
    user_response: &str,
    response_source: ResponseSource,
) -> bool {
    for message in session.messages.iter_mut().rev() {
        if message.tool_call_id.as_deref() == Some(tool_call_id) {
            // Preserve the server-issued typed permission contract outside the
            // model-visible content before replacing the synthetic waiting
            // payload with the selected answer. This lets an exact durable
            // decision receipt be reconstructed after a daemon restart without
            // trusting display strings or replaying an already-consumed run.
            if let Ok(payload) = serde_json::from_str::<serde_json::Value>(&message.content) {
                if payload.get("status").and_then(serde_json::Value::as_str)
                    == Some("awaiting_permission_approval")
                {
                    if let Some(request) = payload.get("permission_request") {
                        insert_message_metadata(message, "permission_request", request.clone());
                    }
                }
            }
            message.content = selected_message_content(user_response, response_source);
            message.tool_success = Some(true);
            return true;
        }
    }

    session.add_message(bamboo_agent_core::Message::tool_result_with_status(
        tool_call_id,
        selected_message_content(user_response, response_source),
        true,
    ));
    false
}

fn insert_message_metadata(message: &mut Message, key: &str, value: serde_json::Value) {
    let metadata = message
        .metadata
        .get_or_insert_with(|| serde_json::Value::Object(Default::default()));
    if !metadata.is_object() {
        let previous = std::mem::replace(metadata, serde_json::Value::Object(Default::default()));
        metadata
            .as_object_mut()
            .expect("replacement metadata is an object")
            .insert("previous_metadata".to_string(), previous);
    }
    metadata
        .as_object_mut()
        .expect("message metadata is an object")
        .insert(key.to_string(), value);
}

fn permission_request_generation(message: &Message) -> Option<String> {
    message
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("permission_request"))
        .and_then(|request| request.get("request_generation"))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| {
            serde_json::from_str::<serde_json::Value>(&message.content)
                .ok()?
                .get("permission_request")?
                .get("request_generation")?
                .as_str()
                .map(ToOwned::to_owned)
        })
}

fn persist_permission_decision_receipt(
    session: &mut Session,
    tool_call_id: &str,
    receipt: &PermissionDecisionReceipt,
) -> bool {
    let Some(message) = session
        .messages
        .iter_mut()
        .rev()
        .find(|message| message.tool_call_id.as_deref() == Some(tool_call_id))
    else {
        return false;
    };
    if permission_request_generation(message).as_deref()
        != Some(receipt.decision.request_generation.as_str())
    {
        return false;
    }
    insert_message_metadata(
        message,
        "permission_decision_receipt",
        serde_json::to_value(receipt).expect("permission receipt is serializable"),
    );
    true
}

fn selected_message_content(user_response: &str, response_source: ResponseSource) -> String {
    match response_source {
        ResponseSource::Human => format!("Selected response: {}", user_response),
        ResponseSource::Gold => format!("Auto-selected response (gold): {}", user_response),
    }
}

fn extract_exit_plan_from_tool_result_message(
    session: &Session,
    tool_call_id: &str,
) -> Option<String> {
    let message = session
        .messages
        .iter()
        .rev()
        .find(|message| message.tool_call_id.as_deref() == Some(tool_call_id))?;
    let payload = serde_json::from_str::<serde_json::Value>(&message.content).ok()?;
    payload
        .get("plan")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

/// Detect whether the user response approves a pending permission request.
///
/// Permission prompts (synthesized by the permission gate, and the
/// `request_permissions` tool) offer exactly `["Approve", "Deny"]`.
fn is_permission_approval(user_response: &str) -> bool {
    user_response.trim().eq_ignore_ascii_case("approve")
}

/// Extract the permission grants implied by an approved permission prompt.
///
/// Reads the pending tool-result message (still the synthesized
/// `awaiting_permission_approval` payload, before it is overwritten by the
/// user's selection) and returns the `(PermissionType, resource)` pairs the
/// caller should grant for the session. Handles both the single-gated-tool shape
/// (top-level `permission_type` + `resource`) and the `request_permissions` shape
/// (a `permissions` array).
fn extract_permission_grants_from_tool_result_message(
    session: &Session,
    tool_call_id: &str,
) -> Vec<(PermissionType, String)> {
    let message = match session
        .messages
        .iter()
        .rev()
        .find(|message| message.tool_call_id.as_deref() == Some(tool_call_id))
    {
        Some(message) => message,
        None => return Vec::new(),
    };
    let payload = match serde_json::from_str::<serde_json::Value>(&message.content) {
        Ok(payload) => payload,
        Err(_) => return Vec::new(),
    };
    if payload.get("status").and_then(|value| value.as_str())
        != Some("awaiting_permission_approval")
    {
        return Vec::new();
    }

    let parse_one = |value: &serde_json::Value| -> Option<(PermissionType, String)> {
        let type_value = value
            .get("permission_type")
            .or_else(|| value.get("type"))?
            .clone();
        let perm_type: PermissionType = serde_json::from_value(type_value).ok()?;
        let resource = value.get("resource")?.as_str()?.trim().to_string();
        if resource.is_empty() {
            return None;
        }
        Some((perm_type, resource))
    };

    if let Some(array) = payload
        .get("permissions")
        .and_then(|value| value.as_array())
    {
        array.iter().filter_map(parse_one).collect()
    } else {
        parse_one(&payload).into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex;
    use tokio::sync::Notify;

    struct TestSessionAccess {
        session: Mutex<Session>,
        loads: AtomicUsize,
        saves: AtomicUsize,
        block_first_save: AtomicBool,
        save_started: Notify,
        release_save: Notify,
    }

    impl TestSessionAccess {
        fn with_pending() -> Self {
            Self::with_pending_and_blocked_save(false)
        }

        fn with_pending_and_blocked_save(block_first_save: bool) -> Self {
            let mut session = Session::new("sess-1", "test-model");
            session.pending_question = Some(make_pending("ConclusionWithOptions"));
            Self {
                session: Mutex::new(session),
                loads: AtomicUsize::new(0),
                saves: AtomicUsize::new(0),
                block_first_save: AtomicBool::new(block_first_save),
                save_started: Notify::new(),
                release_save: Notify::new(),
            }
        }
    }

    #[async_trait::async_trait]
    impl SessionAccess for TestSessionAccess {
        async fn load_session(
            &self,
            _id: &str,
        ) -> Result<Option<Session>, super::super::errors::SessionLoadError> {
            Ok(Some(self.session.lock().unwrap().clone()))
        }

        async fn load_or_create(
            &self,
            _id: &str,
            _model: &str,
        ) -> Result<Session, super::super::errors::SessionLoadError> {
            Ok(self.session.lock().unwrap().clone())
        }

        async fn load_merged(
            &self,
            _id: &str,
        ) -> Result<Option<Session>, super::super::errors::SessionLoadError> {
            self.loads.fetch_add(1, Ordering::SeqCst);
            Ok(Some(self.session.lock().unwrap().clone()))
        }

        async fn save_session(
            &self,
            session: &mut Session,
        ) -> Result<(), super::super::errors::SessionSaveError> {
            if self.block_first_save.swap(false, Ordering::SeqCst) {
                self.save_started.notify_one();
                self.release_save.notified().await;
            }
            *self.session.lock().unwrap() = session.clone();
            self.saves.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn save_and_cache(
            &self,
            session: &mut Session,
        ) -> Result<(), super::super::errors::SessionSaveError> {
            self.save_session(session).await
        }
    }

    fn respond_input() -> RespondInput {
        RespondInput {
            session_id: "sess-1".to_string(),
            user_response: "A".to_string(),
            model: None,
            model_ref: None,
            provider: None,
            reasoning_effort: None,
        }
    }

    fn make_pending(tool_name: &str) -> PendingQuestion {
        PendingQuestion {
            tool_call_id: "call-1".to_string(),
            tool_name: tool_name.to_string(),
            question: "Question?".to_string(),
            options: vec!["A".to_string(), "B".to_string()],
            allow_custom: false,
            source: bamboo_agent_core::PendingQuestionSource::PauseTool,
        }
    }

    #[tokio::test]
    async fn expected_tool_call_id_accepts_the_current_question() {
        let repo = TestSessionAccess::with_pending();

        submit_pending_response_checked(&repo, respond_input(), Some("call-1".to_string()))
            .await
            .expect("matching identity should submit");

        assert_eq!(repo.saves.load(Ordering::SeqCst), 1);
        assert!(repo.session.lock().unwrap().pending_question.is_none());
    }

    #[tokio::test]
    async fn stale_tool_call_id_is_rejected_without_consuming_the_question() {
        let repo = TestSessionAccess::with_pending();

        let error =
            submit_pending_response_checked(&repo, respond_input(), Some("stale-call".to_string()))
                .await
                .expect_err("stale identity must fail");

        assert!(matches!(
            error,
            RespondError::PendingQuestionMismatch {
                ref expected,
                ref actual,
            } if expected == "stale-call" && actual == "call-1"
        ));
        assert_eq!(repo.saves.load(Ordering::SeqCst), 0);
        assert_eq!(
            repo.session
                .lock()
                .unwrap()
                .pending_question
                .as_ref()
                .map(|question| question.tool_call_id.as_str()),
            Some("call-1")
        );
    }

    #[tokio::test]
    async fn omitted_tool_call_guard_remains_backwards_compatible() {
        let repo = TestSessionAccess::with_pending();

        submit_pending_response(&repo, respond_input())
            .await
            .expect("legacy client should remain accepted");

        assert_eq!(repo.saves.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn concurrent_responses_consume_a_pending_question_once() {
        let repo = Arc::new(TestSessionAccess::with_pending_and_blocked_save(true));

        let first_repo = repo.clone();
        let first = tokio::spawn(async move {
            submit_pending_response_checked(
                first_repo.as_ref(),
                respond_input(),
                Some("call-1".to_string()),
            )
            .await
        });
        repo.save_started.notified().await;

        let second_entered = Arc::new(Notify::new());
        let second_repo = repo.clone();
        let second_entered_task = second_entered.clone();
        let second = tokio::spawn(async move {
            second_entered_task.notify_one();
            submit_pending_response_checked(
                second_repo.as_ref(),
                respond_input(),
                Some("call-1".to_string()),
            )
            .await
        });
        second_entered.notified().await;
        tokio::task::yield_now().await;

        assert_eq!(
            repo.loads.load(Ordering::SeqCst),
            1,
            "the second responder must wait before loading the pending question"
        );
        repo.release_save.notify_one();

        first.await.unwrap().expect("first response should win");
        let error = second
            .await
            .unwrap()
            .expect_err("second response must observe the consumed question");
        assert!(matches!(error, RespondError::NoPendingQuestion));
        assert_eq!(repo.loads.load(Ordering::SeqCst), 2);
        assert_eq!(repo.saves.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cancelled_response_waiter_does_not_leak_diagnostics_or_the_gate() {
        let session_id = "cancelled-response-waiter";
        let owner = acquire_pending_response_guard(session_id).await;
        let waiter = tokio::spawn(async move { acquire_pending_response_guard(session_id).await });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while pending_response_waiter_count(session_id) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("waiter should reach the gate");

        waiter.abort();
        let _ = waiter.await;
        assert_eq!(pending_response_waiter_count(session_id), 0);
        drop(owner);

        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            acquire_pending_response_guard(session_id),
        )
        .await
        .expect("cancelled waiter must not retain the gate");
    }

    #[test]
    fn enter_plan_mode_activates_plan_mode_state() {
        let mut session = Session::new("sess-1", "test-model");
        let pending = make_pending("EnterPlanMode");

        apply_plan_mode_transition(&mut session, &pending, "Enter plan mode", None);

        assert!(session.agent_runtime_state.is_some());
        let state = session.agent_runtime_state.unwrap();
        assert!(state.plan_mode.is_some());
        let plan = state.plan_mode.unwrap();
        assert_eq!(plan.status, PlanModeStatus::Exploring);
        assert_eq!(plan.pre_permission_mode, "default");
    }

    #[test]
    fn enter_plan_mode_does_nothing_when_not_approved() {
        let mut session = Session::new("sess-1", "test-model");
        let pending = make_pending("EnterPlanMode");

        apply_plan_mode_transition(&mut session, &pending, "Stay in normal mode", None);

        assert!(session.agent_runtime_state.is_none());
    }

    #[test]
    fn plan_mode_preserves_and_restores_typed_auto_request() {
        let mut session = Session::new("sess-auto-plan", "test-model");
        session
            .agent_runtime_state
            .get_or_insert_with(|| AgentRuntimeState::new("run-1"))
            .set_permission_mode(SessionPermissionMode::Auto);
        let enter = make_pending("EnterPlanMode");
        let transition =
            apply_plan_mode_transition(&mut session, &enter, "Enter plan mode", None).unwrap();
        assert!(matches!(
            transition,
            PlanModeTransition::Entered {
                ref pre_permission_mode,
                ..
            } if pre_permission_mode == "auto"
        ));
        assert_eq!(
            session
                .agent_runtime_state
                .as_ref()
                .unwrap()
                .plan_mode
                .as_ref()
                .unwrap()
                .pre_permission_mode,
            "auto"
        );

        let exit = make_pending("ExitPlanMode");
        let transition = apply_plan_mode_transition(
            &mut session,
            &exit,
            "Approve (Auto mode)",
            Some("Reviewed plan".to_string()),
        )
        .unwrap();
        assert!(matches!(
            transition,
            PlanModeTransition::Exited {
                ref restored_mode,
                ..
            } if restored_mode == "auto"
        ));
        assert_eq!(
            session
                .agent_runtime_state
                .as_ref()
                .unwrap()
                .effective_permission_mode(),
            SessionPermissionMode::Auto
        );
    }

    #[test]
    fn exit_plan_mode_does_not_restore_over_a_newer_typed_mode() {
        let mut session = Session::new("sess-plan-patch", "test-model");
        let state = session
            .agent_runtime_state
            .get_or_insert_with(|| AgentRuntimeState::new("run-1"));
        state.set_permission_mode(SessionPermissionMode::Auto);
        state.plan_mode = Some(PlanModeState {
            entered_at: Utc::now(),
            pre_permission_mode: "auto".to_string(),
            plan_file_path: None,
            status: PlanModeStatus::AwaitingApproval,
        });
        // Simulate a newer PATCH while the Plan overlay is still active.
        state.set_permission_mode(SessionPermissionMode::Bypass);

        let transition = apply_plan_mode_transition(
            &mut session,
            &make_pending("ExitPlanMode"),
            "Approve (Default mode)",
            None,
        )
        .unwrap();

        assert!(matches!(
            transition,
            PlanModeTransition::Exited {
                ref restored_mode,
                ..
            } if restored_mode == "bypass"
        ));
        let state = session.agent_runtime_state.unwrap();
        assert!(state.plan_mode.is_none());
        assert_eq!(
            state.effective_permission_mode(),
            SessionPermissionMode::Bypass
        );
    }

    #[test]
    fn exit_plan_mode_clears_plan_mode_state() {
        let mut session = Session::new("sess-1", "test-model");
        session.agent_runtime_state = Some(AgentRuntimeState::new("run-1"));
        session.agent_runtime_state.as_mut().unwrap().plan_mode = Some(PlanModeState {
            entered_at: Utc::now(),
            pre_permission_mode: "default".to_string(),
            plan_file_path: None,
            status: PlanModeStatus::AwaitingApproval,
        });
        let pending = make_pending("ExitPlanMode");

        apply_plan_mode_transition(
            &mut session,
            &pending,
            "Approve (Default mode)",
            Some("Reviewed plan".to_string()),
        );

        assert!(session.agent_runtime_state.unwrap().plan_mode.is_none());
    }

    #[test]
    fn exit_plan_mode_keeps_plan_mode_when_not_approved() {
        let mut session = Session::new("sess-1", "test-model");
        session.agent_runtime_state = Some(AgentRuntimeState::new("run-1"));
        session.agent_runtime_state.as_mut().unwrap().plan_mode = Some(PlanModeState {
            entered_at: Utc::now(),
            pre_permission_mode: "default".to_string(),
            plan_file_path: None,
            status: PlanModeStatus::AwaitingApproval,
        });
        let pending = make_pending("ExitPlanMode");

        apply_plan_mode_transition(&mut session, &pending, "Stay in plan mode", None);

        assert!(session.agent_runtime_state.unwrap().plan_mode.is_some());
    }

    #[test]
    fn exit_plan_mode_ignores_other_tools() {
        let mut session = Session::new("sess-1", "test-model");
        let pending = make_pending("ConclusionWithOptions");

        apply_plan_mode_transition(&mut session, &pending, "Approve", None);

        assert!(session.agent_runtime_state.is_none());
    }

    #[test]
    fn is_exit_plan_mode_approved_detects_approval() {
        assert!(is_exit_plan_mode_approved("Approve (Default mode)"));
        assert!(is_exit_plan_mode_approved("Approve (Accept edits mode)"));
        assert!(!is_exit_plan_mode_approved("Stay in plan mode"));
        assert!(!is_exit_plan_mode_approved("Edit plan first"));
    }

    #[test]
    fn extract_exit_plan_from_tool_result_message_reads_plan_payload() {
        let mut session = Session::new("sess-1", "test-model");
        let mut tool_message = bamboo_agent_core::Message::tool_result(
            "call-1",
            serde_json::json!({
                "plan": "# Plan\n\n1. Step"
            })
            .to_string(),
        );
        tool_message.tool_success = Some(true);
        session.add_message(tool_message);

        let plan = extract_exit_plan_from_tool_result_message(&session, "call-1");
        assert_eq!(plan.as_deref(), Some("# Plan\n\n1. Step"));
    }

    #[test]
    fn selected_permission_preserves_typed_request_and_receipt_in_non_visible_metadata() {
        let mut session = Session::new("sess-1", "test-model");
        session.add_message(bamboo_agent_core::Message::tool_result(
            "permission-1",
            serde_json::json!({
                "status": "awaiting_permission_approval",
                "permission_request": {
                    "request_id": "permission-1",
                    "request_generation": "generation-1",
                    "session_id": "sess-1",
                    "allowed_decisions": ["allow_once", "deny_once"]
                }
            })
            .to_string(),
        ));
        session
            .messages
            .last_mut()
            .expect("permission tool result")
            .metadata = Some(serde_json::json!("legacy-metadata"));

        assert!(update_or_append_tool_result_message(
            &mut session,
            "permission-1",
            "Approve",
            ResponseSource::Human,
        ));
        let receipt = PermissionDecisionReceipt {
            session_id: "sess-1".to_string(),
            decision: bamboo_tools::permission::PermissionDecision {
                request_id: "permission-1".to_string(),
                request_generation: "generation-1".to_string(),
                decision: bamboo_tools::permission::PermissionDecisionKind::AllowOnce,
                matcher_id: None,
                expected_policy_revision: Some(4),
                confirm_global: false,
            },
            decided_at: Utc::now(),
        };
        assert!(persist_permission_decision_receipt(
            &mut session,
            "permission-1",
            &receipt
        ));

        let message = session
            .messages
            .iter()
            .find(|message| message.tool_call_id.as_deref() == Some("permission-1"))
            .expect("permission tool result");
        assert_eq!(message.content, "Selected response: Approve");
        assert_eq!(
            message
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("permission_request"))
                .and_then(|request| request.get("request_id"))
                .and_then(serde_json::Value::as_str),
            Some("permission-1")
        );
        assert_eq!(
            message
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("previous_metadata")),
            Some(&serde_json::json!("legacy-metadata"))
        );
        assert_eq!(
            message
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("permission_decision_receipt"))
                .cloned()
                .and_then(|receipt| {
                    serde_json::from_value::<PermissionDecisionReceipt>(receipt).ok()
                }),
            Some(receipt)
        );
    }

    #[test]
    fn reused_tool_call_id_requires_current_permission_generation() {
        let mut session = Session::new("sess-1", "test-model");
        for generation in ["generation-old", "generation-current"] {
            session.add_message(bamboo_agent_core::Message::tool_result(
                "permission-reused",
                serde_json::json!({
                    "status": "awaiting_permission_approval",
                    "permission_type": "execute_command",
                    "resource": format!("resource-{generation}"),
                    "permission_request": {
                        "request_id": "permission-reused",
                        "request_generation": generation,
                        "session_id": "sess-1",
                        "allowed_decisions": ["allow_once", "deny_once"]
                    }
                })
                .to_string(),
            ));
        }
        session.set_pending_question_with_source(
            "permission-reused".to_string(),
            "Bash".to_string(),
            "Allow the current operation?".to_string(),
            vec!["Approve".to_string(), "Deny".to_string()],
            false,
            bamboo_agent_core::PendingQuestionSource::PauseTool,
        );
        let input = RespondInput {
            session_id: "sess-1".to_string(),
            user_response: "Approve".to_string(),
            model: None,
            model_ref: None,
            provider: None,
            reasoning_effort: None,
        };
        let receipt = |generation: &str| PermissionDecisionReceipt {
            session_id: "sess-1".to_string(),
            decision: bamboo_tools::permission::PermissionDecision {
                request_id: "permission-reused".to_string(),
                request_generation: generation.to_string(),
                decision: bamboo_tools::permission::PermissionDecisionKind::AllowOnce,
                matcher_id: None,
                expected_policy_revision: None,
                confirm_global: false,
            },
            decided_at: Utc::now(),
        };

        let stale = receipt("generation-old");
        assert!(matches!(
            apply_pending_response(
                &mut session,
                &input,
                Some("permission-reused"),
                ResponseSource::Human,
                Some(&stale),
            ),
            Err(RespondError::InvalidResponse(message))
                if message.contains("generation")
        ));
        assert!(session.pending_question.is_some());

        let current = receipt("generation-current");
        apply_pending_response(
            &mut session,
            &input,
            Some("permission-reused"),
            ResponseSource::Human,
            Some(&current),
        )
        .expect("current generation resolves the parked operation");
        assert!(session.pending_question.is_none());
        assert_eq!(
            session
                .metadata
                .get(PERMISSION_REEXECUTE_GENERATION_METADATA_KEY)
                .map(String::as_str),
            Some("generation-current")
        );
        assert!(session.messages[0].content.contains("generation-old"));
        assert_eq!(session.messages[1].content, "Selected response: Approve");
        assert_eq!(
            session.messages[1]
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("permission_decision_receipt"))
                .and_then(|receipt| receipt.get("decision"))
                .and_then(|decision| decision.get("request_generation"))
                .and_then(serde_json::Value::as_str),
            Some("generation-current")
        );
    }

    #[test]
    fn typed_permission_receipt_controls_replay_independent_of_display_options() {
        let session = || {
            let mut session = Session::new("sess-typed", "test-model");
            session.add_message(bamboo_agent_core::Message::tool_result(
                "permission-localized",
                serde_json::json!({
                    "status": "awaiting_permission_approval",
                    "permission_type": "execute_command",
                    "resource": "cargo test --workspace",
                    "permission_request": {
                        "request_id": "permission-localized",
                        "request_generation": "generation-localized",
                        "session_id": "sess-typed",
                        "allowed_decisions": ["allow_once", "deny_once"]
                    }
                })
                .to_string(),
            ));
            session.set_pending_question_with_source(
                "permission-localized".to_string(),
                "Bash".to_string(),
                "允许执行吗？".to_string(),
                vec!["允许".to_string(), "拒绝".to_string()],
                false,
                bamboo_agent_core::PendingQuestionSource::PauseTool,
            );
            session
        };
        let receipt = |decision| PermissionDecisionReceipt {
            session_id: "sess-typed".to_string(),
            decision: bamboo_tools::permission::PermissionDecision {
                request_id: "permission-localized".to_string(),
                request_generation: "generation-localized".to_string(),
                decision,
                matcher_id: None,
                expected_policy_revision: None,
                confirm_global: false,
            },
            decided_at: Utc::now(),
        };

        let mut allowed = session();
        let allow_input = RespondInput {
            session_id: "sess-typed".to_string(),
            user_response: "已由结构化决定允许".to_string(),
            model: None,
            model_ref: None,
            provider: None,
            reasoning_effort: None,
        };
        apply_pending_response(
            &mut allowed,
            &allow_input,
            Some("permission-localized"),
            ResponseSource::Human,
            Some(&receipt(PermissionDecisionKind::AllowOnce)),
        )
        .expect("typed allow must not depend on localized display options");
        assert_eq!(
            allowed
                .metadata
                .get(PERMISSION_REEXECUTE_METADATA_KEY)
                .map(String::as_str),
            Some("permission-localized")
        );
        assert_eq!(
            allowed
                .metadata
                .get(PERMISSION_REEXECUTE_GENERATION_METADATA_KEY)
                .map(String::as_str),
            Some("generation-localized")
        );

        let mut denied = session();
        denied.metadata.insert(
            PERMISSION_REEXECUTE_METADATA_KEY.to_string(),
            "stale-call".to_string(),
        );
        denied.metadata.insert(
            PERMISSION_REEXECUTE_GENERATION_METADATA_KEY.to_string(),
            "stale-generation".to_string(),
        );
        let deny_input = RespondInput {
            session_id: "sess-typed".to_string(),
            // Deliberately approval-looking: the receipt enum must win.
            user_response: "Approve".to_string(),
            model: None,
            model_ref: None,
            provider: None,
            reasoning_effort: None,
        };
        apply_pending_response(
            &mut denied,
            &deny_input,
            Some("permission-localized"),
            ResponseSource::Human,
            Some(&receipt(PermissionDecisionKind::DenyOnce)),
        )
        .expect("typed deny must not depend on display text");
        assert!(!denied
            .metadata
            .contains_key(PERMISSION_REEXECUTE_METADATA_KEY));
        assert!(!denied
            .metadata
            .contains_key(PERMISSION_REEXECUTE_GENERATION_METADATA_KEY));

        let mut legacy = session();
        assert!(matches!(
            apply_pending_response(
                &mut legacy,
                &allow_input,
                Some("permission-localized"),
                ResponseSource::Human,
                None,
            ),
            Err(RespondError::InvalidResponse(_))
        ));
        assert!(legacy.pending_question.is_some());
    }
}
