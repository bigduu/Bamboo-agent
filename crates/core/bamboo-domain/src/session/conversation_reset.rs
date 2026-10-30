//! Pure preparation for a future fenced conversation-reset transaction.
//!
//! This module never reads/writes storage, stops a run or grants authority. A
//! prepared value is NOT a receipt or permission to call an ordinary writer.
//! The distinct storage transaction must revalidate canonical birth, revision,
//! snapshot and idle authority under its final physical guards before publishing
//! the proposed Session and reset ledger atomically. No HTTP route is installed.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{AgentStatusState, PlanModeStatus, Role, Session, SessionKind};

pub const CONVERSATION_RESET_SCHEMA_VERSION: u32 = 1;
pub const CONVERSATION_RESET_RECEIPT_LIMIT: usize = 8;

/// Separate from the retained Session behavior version and tool alias contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationResetRequest {
    pub session_id: String,
    /// Bind the UUID to its observed reset revision so evicted operations cannot
    /// be reused against a later conversation revision.
    pub operation_id: String,
    pub expected_birth_token: String,
    pub expected_conversation_revision: u64,
    /// Digest of the complete canonical Session observed during preview. A
    /// reset revision alone does not detect a newly appended message.
    pub expected_snapshot_sha256: String,
}

/// Prospective ledger data. A prepared copy does not prove durable completion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationResetReceipt {
    pub operation_id: String,
    pub birth_token: String,
    pub expected_snapshot_sha256: String,
    pub previous_revision: u64,
    pub conversation_revision: u64,
    pub retained_system_messages: u8,
    pub reset_at: DateTime<Utc>,
}

/// Future canonical inline ledger; not added to persisted Session in this slice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationResetState {
    pub schema_version: u32,
    pub conversation_revision: u64,
    pub receipts: Vec<ConversationResetReceipt>,
}

impl Default for ConversationResetState {
    fn default() -> Self {
        Self {
            schema_version: CONVERSATION_RESET_SCHEMA_VERSION,
            conversation_revision: 0,
            receipts: Vec::new(),
        }
    }
}

/// An external observation for preparation, never an HTTP argument or grant.
/// The storage owner must observe it again under the final reset transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConversationResetReadSafety {
    IdleObserved,
    ActiveRun,
    PendingInput,
    UncertainOperation,
    AuthorityUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConversationResetRejection {
    InvalidRequest,
    NotRoot,
    IncompatibleSession,
    BirthMismatch,
    StaleObservation,
    OperationConflict,
    InvalidLedger,
    ActiveRun,
    PendingQuestion,
    PendingApproval,
    PendingWait,
    PendingInput,
    UncertainOperation,
    AuthorityUnavailable,
    LegacyRuntimeState,
    CounterOverflow,
}

impl ConversationResetRejection {
    pub fn code(self) -> &'static str {
        match self {
            Self::InvalidRequest => "session_reset_invalid_request",
            Self::NotRoot => "session_reset_not_root",
            Self::IncompatibleSession => "session_execution_incompatible",
            Self::BirthMismatch | Self::StaleObservation => "session_reset_stale_observation",
            Self::OperationConflict => "session_reset_operation_conflict",
            Self::InvalidLedger => "session_reset_recovery_required",
            Self::ActiveRun => "session_reset_active_run",
            Self::PendingQuestion => "session_reset_pending_question",
            Self::PendingApproval => "session_reset_pending_approval",
            Self::PendingWait => "session_reset_pending_wait",
            Self::PendingInput => "session_reset_pending_input",
            Self::UncertainOperation => "session_reset_outcome_unknown",
            Self::AuthorityUnavailable => "session_reset_authority_unavailable",
            Self::LegacyRuntimeState => "session_reset_legacy_runtime_state",
            Self::CounterOverflow => "session_reset_counter_overflow",
        }
    }
}

#[derive(Debug)]
pub struct PreparedConversationReset {
    pub proposed_session: Session,
    pub proposed_state: ConversationResetState,
}

#[derive(Debug)]
pub enum ConversationResetPreparation {
    Prepared(Box<PreparedConversationReset>),
    /// Historical completion data only. Do not erase newer UI messages or
    /// execute reset again; the transaction owner still checks current access.
    AlreadyRecorded(ConversationResetReceipt),
    Rejected(ConversationResetRejection),
}

fn hash64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_operation_id(value: &str, revision: u64) -> bool {
    let Some((epoch, uuid)) = value.split_once(':') else {
        return false;
    };
    epoch == revision.to_string()
        && uuid::Uuid::parse_str(uuid)
            .is_ok_and(|parsed| !parsed.is_nil() && parsed.to_string() == uuid)
}

fn canonical_json(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(object) => {
            let mut fields = object.into_iter().collect::<Vec<_>>();
            fields.sort_by(|a, b| a.0.cmp(&b.0));
            serde_json::Value::Object(
                fields
                    .into_iter()
                    .map(|(key, value)| (key, canonical_json(value)))
                    .collect(),
            )
        }
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical_json).collect())
        }
        other => other,
    }
}

/// Opaque preview fence, not a public transcript or a grant. This is a full
/// Session serialization and makes no constant-history-I/O claim.
pub fn conversation_reset_snapshot_sha256(session: &Session) -> Result<String, serde_json::Error> {
    let source = serde_json::to_value(session)?;
    let bytes = serde_json::to_vec(&canonical_json(source))?;
    let mut digest = Sha256::new();
    digest.update(b"bamboo.conversation-reset.snapshot.v1\0");
    digest.update(bytes);
    Ok(hex::encode(digest.finalize()))
}

impl ConversationResetState {
    pub(crate) fn valid_for(&self, birth_token: &str) -> bool {
        if self.schema_version != CONVERSATION_RESET_SCHEMA_VERSION
            || self.receipts.len() > CONVERSATION_RESET_RECEIPT_LIMIT
            || (self.conversation_revision == 0) != self.receipts.is_empty()
        {
            return false;
        }
        let mut operations = HashSet::new();
        let mut previous = None;
        for receipt in &self.receipts {
            if receipt.birth_token != birth_token
                || !hash64(&receipt.expected_snapshot_sha256)
                || !valid_operation_id(&receipt.operation_id, receipt.previous_revision)
                || receipt.previous_revision.checked_add(1) != Some(receipt.conversation_revision)
                || receipt.retained_system_messages > 1
                || !operations.insert(&receipt.operation_id)
                || previous.is_some_and(|revision| revision != receipt.previous_revision)
            {
                return false;
            }
            previous = Some(receipt.conversation_revision);
        }
        previous.is_none_or(|revision| revision == self.conversation_revision)
    }
}

/// Inspect only pending or uncertain state present in this Session snapshot.
/// This is not a reset authorization, execution grant, or proof of physical idle.
pub fn conversation_reset_pending_rejection(
    session: &Session,
) -> Option<ConversationResetRejection> {
    pending_snapshot_rejection(session, false)
}

/// C1 preserves an idle, non-approval Plan. Every other pending or uncertain
/// state remains a blocker; this observation does not authorize the C1 write.
pub fn conversation_clear_readback_pending_rejection(
    session: &Session,
) -> Option<ConversationResetRejection> {
    pending_snapshot_rejection(session, true)
}

fn pending_snapshot_rejection(
    session: &Session,
    allow_idle_plan: bool,
) -> Option<ConversationResetRejection> {
    use ConversationResetRejection as Rejection;
    if session
        .metadata
        .contains_key("permission.replay_save_recovery_required")
    {
        return Some(Rejection::UncertainOperation);
    }
    if session
        .metadata
        .contains_key("permission.reexecute_tool_call_id")
        || session
            .metadata
            .contains_key("permission.reexecute_request_generation")
    {
        return Some(Rejection::PendingApproval);
    }
    if session.pending_question.is_some() {
        return Some(Rejection::PendingQuestion);
    }
    // The engine still reads this mirror as a fallback. Do not erase or
    // silently reinterpret a legacy control plane during conversation reset.
    if session.metadata.contains_key("agent.runtime.state") {
        return Some(Rejection::LegacyRuntimeState);
    }
    if session.has_pending_injected_messages() {
        return Some(Rejection::PendingInput);
    }
    if let Some(runtime) = &session.agent_runtime_state {
        if matches!(
            runtime.status,
            AgentStatusState::Initializing
                | AgentStatusState::Running
                | AgentStatusState::Finalizing
        ) || runtime.tools.active_tool_calls > 0
        {
            return Some(Rejection::ActiveRun);
        }
        if runtime.suspension.is_some()
            || runtime.waiting_for_children.is_some()
            || runtime.waiting_for_bash.is_some()
            || runtime.plan_mode.as_ref().is_some_and(|plan| {
                !allow_idle_plan
                    || !matches!(
                        runtime.status,
                        AgentStatusState::Idle
                            | AgentStatusState::Completed
                            | AgentStatusState::Cancelled
                            | AgentStatusState::Failed
                    )
                    || !matches!(
                        plan.status,
                        PlanModeStatus::Exploring
                            | PlanModeStatus::Designing
                            | PlanModeStatus::Reviewing
                            | PlanModeStatus::Finalizing
                    )
            })
            || runtime.status == AgentStatusState::Suspended
        {
            return Some(Rejection::PendingWait);
        }
    }
    None
}

/// Calculate only. A canonical same-Root storage transaction with separate idle
/// authority must publish the result; ordinary save/clear writers remain denied.
pub fn prepare_conversation_reset(
    session: &Session,
    state: &ConversationResetState,
    request: &ConversationResetRequest,
    safety: ConversationResetReadSafety,
    reset_at: DateTime<Utc>,
) -> Result<ConversationResetPreparation, serde_json::Error> {
    use ConversationResetPreparation::{AlreadyRecorded, Prepared, Rejected};
    use ConversationResetRejection as Rejection;
    if request.session_id != session.id
        || !hash64(&request.expected_birth_token)
        || !hash64(&request.expected_snapshot_sha256)
        || !valid_operation_id(
            &request.operation_id,
            request.expected_conversation_revision,
        )
    {
        return Ok(Rejected(Rejection::InvalidRequest));
    }
    if session.kind != SessionKind::Root
        || session.parent_session_id.is_some()
        || session.spawn_depth != 0
        || (!session.root_session_id.is_empty() && session.root_session_id != session.id)
    {
        return Ok(Rejected(Rejection::NotRoot));
    }
    if !session.execution_compatibility().execution_allowed {
        return Ok(Rejected(Rejection::IncompatibleSession));
    }
    // Existing opaque Root lifetime contract includes the trusted identity.
    let birth_token = session.root_mode_birth_token();
    if request.expected_birth_token != birth_token {
        return Ok(Rejected(Rejection::BirthMismatch));
    }
    if !state.valid_for(&birth_token) {
        return Ok(Rejected(Rejection::InvalidLedger));
    }
    // Even a historical receipt needs a currently readable authority boundary.
    // A stale cache must not turn unknown canonical authority into success.
    if safety == ConversationResetReadSafety::AuthorityUnavailable {
        return Ok(Rejected(Rejection::AuthorityUnavailable));
    }
    if let Some(receipt) = state
        .receipts
        .iter()
        .find(|r| r.operation_id == request.operation_id)
    {
        return Ok(
            if receipt.previous_revision == request.expected_conversation_revision
                && receipt.expected_snapshot_sha256 == request.expected_snapshot_sha256
            {
                AlreadyRecorded(receipt.clone())
            } else {
                Rejected(Rejection::OperationConflict)
            },
        );
    }
    if request.expected_conversation_revision != state.conversation_revision
        || conversation_reset_snapshot_sha256(session)? != request.expected_snapshot_sha256
    {
        return Ok(Rejected(Rejection::StaleObservation));
    }
    if let Some(rejection) = conversation_reset_pending_rejection(session) {
        return Ok(Rejected(rejection));
    }
    let rejection = match safety {
        ConversationResetReadSafety::IdleObserved => None,
        ConversationResetReadSafety::ActiveRun => Some(Rejection::ActiveRun),
        ConversationResetReadSafety::PendingInput => Some(Rejection::PendingInput),
        ConversationResetReadSafety::UncertainOperation => Some(Rejection::UncertainOperation),
        ConversationResetReadSafety::AuthorityUnavailable => Some(Rejection::AuthorityUnavailable),
    };
    if let Some(rejection) = rejection {
        return Ok(Rejected(rejection));
    }
    let Some(next) = state.conversation_revision.checked_add(1) else {
        return Ok(Rejected(Rejection::CounterOverflow));
    };
    if session
        .model_context_state
        .as_ref()
        .is_some_and(|ledger| ledger.prefix_epoch == u64::MAX || ledger.state_revision == u64::MAX)
        || session.provider_transcript.epoch() == u64::MAX
        || session.provider_transcript.state_revision() > u64::MAX - 2
    {
        return Ok(Rejected(Rejection::CounterOverflow));
    }

    let mut candidate = session.clone();
    let system = candidate
        .messages
        .iter()
        .find(|m| m.role == Role::System)
        .cloned();
    candidate.messages = system.into_iter().collect();
    // Purge payloads even when a native group was anchored to the retained
    // System message. Keep its monotonic boundary, then reuse the canonical
    // coupled ledger/native reset instead of restarting either epoch at zero.
    candidate
        .provider_transcript
        .prune_dangling_groups(&HashSet::new());
    candidate.clear_derived_context_state();
    candidate.prompt_snapshot = None;
    candidate.metadata.remove("runtime.plugin_hook_contexts");
    for key in [
        "runtime_prompt_snapshot",
        "runtime_prompt_composer_version",
        "runtime_prompt_component_flags",
        "runtime_prompt_component_lengths",
        "runtime_prompt_section_layout",
    ] {
        candidate.metadata.remove(key);
    }
    if let Some(runtime) = &mut candidate.agent_runtime_state {
        runtime.prompt = Default::default();
        runtime.hook_contexts.clear();
        runtime.llm.responses_previous_id = None;
        runtime.memory.external_memory_injected = false;
        runtime.memory.compression_events_count = 0;
        runtime.memory.last_compression_at = None;
    }
    candidate.updated_at = reset_at;
    let mut proposed_state = state.clone();
    proposed_state.conversation_revision = next;
    proposed_state.receipts.push(ConversationResetReceipt {
        operation_id: request.operation_id.clone(),
        birth_token,
        expected_snapshot_sha256: request.expected_snapshot_sha256.clone(),
        previous_revision: state.conversation_revision,
        conversation_revision: next,
        retained_system_messages: candidate.messages.len() as u8,
        reset_at,
    });
    if proposed_state.receipts.len() > CONVERSATION_RESET_RECEIPT_LIMIT {
        proposed_state.receipts.remove(0);
    }
    Ok(Prepared(Box::new(PreparedConversationReset {
        proposed_session: candidate,
        proposed_state,
    })))
}

#[cfg(test)]
#[path = "conversation_reset_tests.rs"]
mod tests;
