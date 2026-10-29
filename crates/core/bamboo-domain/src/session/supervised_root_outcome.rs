//! Bounded Root outcome facts carried by the existing canonical Main.
//!
//! These values are not authority, an Inbox, or an ACK. Storage must validate
//! the retained Actor fence and owned Inbox claim before applying a transition.
//! No transition silently removes a reserved, terminal, or decision fact.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    ActorActivationFence, Message, ResponseOccurrence, Session, SessionActivationPolicy,
    SessionInboxActivationIntent, SessionKind, SessionMessageBody, SessionMessageEnvelope,
    SessionMessageId, SessionMessageKind, SupervisorReference,
};
use crate::SupervisorCurrentUserTarget;

pub const SUPERVISED_ROOT_OUTCOMES_VERSION: u32 = 1;
pub const MAX_SUPERVISED_ROOT_OUTCOMES: usize = 64;
pub const MAX_SUPERVISED_ROOT_OUTCOME_INPUTS: usize = 128;
pub const MAX_SUPERVISED_ROOT_OUTCOME_QUESTIONS: usize = 8;
pub const MAX_SUPERVISED_ROOT_OUTCOMES_BYTES: usize = 256 * 1024;
// Each question keeps its digest and the input prefix seen at that moment.
// Reserve all eight bounded entries and a terminal variant before side effects.
const OUTCOME_TERMINAL_HEADROOM: usize = 128;
const OUTCOME_QUESTION_HEADROOM: usize = 160;
const OWNED_INPUT_CHECKPOINT_KEY: &str = "_bamboo_owned_input_checkpoint";

/// An admitted input is historical evidence, not proof of provider consumption
/// or of the sender's authority. Its original envelope remains in Main.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupervisedRootOutcomeInput {
    pub receipt_id: SessionMessageId,
    pub generation: u64,
    /// Intended historical source for the original authorized Inbox writer.
    /// None stays unknown; a later retry must not upgrade it into authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supervisor_origin: Option<SupervisedRootOutcomeInputOrigin>,
}

/// A retained observation, not a capability. Storage must capture it while
/// the original caller/target guards are held and revalidate before use.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "route", rename_all = "snake_case", deny_unknown_fields)]
pub enum SupervisedRootOutcomeInputOrigin {
    CurrentUser {
        supervisor: SupervisorReference,
        supervisor_created_at: DateTime<Utc>,
        /// Captured from the original executing operation, never reconstructed
        /// from the current Supervisor after a restart.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        caller: Option<crate::SessionCallerReference>,
        /// True if the original operation retained a finite Host scope; false
        /// only when its writer actually retained a never-installed fence.
        /// None is historical uncertainty, not permission to replay.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        requires_host_scope: Option<bool>,
        target: SupervisorCurrentUserTarget,
        operation_id: SessionMessageId,
    },
    LegacyOrCreatedRoot {
        supervisor: SupervisorReference,
        supervisor_created_at: DateTime<Utc>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        caller: Option<crate::SessionCallerReference>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        requires_host_scope: Option<bool>,
        target_created_at: DateTime<Utc>,
        operation_id: SessionMessageId,
    },
}

impl SupervisedRootOutcomeInputOrigin {
    /// Historical evidence is not a grant. Storage separately retains and
    /// validates current authority; an originally finite source cannot be
    /// widened merely because a restarted registry has no installation.
    pub fn permits_scope_replay(&self, scoped: bool) -> bool {
        match self.requires_host_scope() {
            Some(true) => scoped,
            Some(false) => true,
            None => false,
        }
    }
    pub fn requires_host_scope(&self) -> Option<bool> {
        match self {
            Self::CurrentUser {
                requires_host_scope,
                ..
            }
            | Self::LegacyOrCreatedRoot {
                requires_host_scope,
                ..
            } => *requires_host_scope,
        }
    }
    /// Historical executing identity. Absence is not permission to synthesize
    /// a caller from a later load of the Supervisor.
    pub fn original_caller(&self) -> Option<&crate::SessionCallerReference> {
        match self {
            Self::CurrentUser { caller, .. } | Self::LegacyOrCreatedRoot { caller, .. } => {
                caller.as_ref()
            }
        }
    }

    fn reference_and_operation(&self) -> (&SupervisorReference, &SessionMessageId) {
        match self {
            Self::CurrentUser {
                supervisor,
                operation_id,
                ..
            }
            | Self::LegacyOrCreatedRoot {
                supervisor,
                operation_id,
                ..
            } => (supervisor, operation_id),
        }
    }

    /// Historical identifier, not permission to use the referenced Session.
    pub fn operation_id(&self) -> &SessionMessageId {
        self.reference_and_operation().1
    }

    pub fn supervisor(&self) -> &SupervisorReference {
        self.reference_and_operation().0
    }

    pub fn supervisor_created_at(&self) -> DateTime<Utc> {
        match self {
            Self::CurrentUser {
                supervisor_created_at,
                ..
            }
            | Self::LegacyOrCreatedRoot {
                supervisor_created_at,
                ..
            } => *supervisor_created_at,
        }
    }

    /// Structural consistency only. The storage writer must separately retain
    /// and validate the original caller, target and Inbox guards.
    pub fn matches_receipt(
        &self,
        root_id: &str,
        root_created_at: DateTime<Utc>,
        receipt_id: &SessionMessageId,
    ) -> bool {
        let (supervisor, operation) = self.reference_and_operation();
        if supervisor.session_id != crate::DEFAULT_SUPERVISOR_SESSION_ID
            || supervisor.incarnation_id.is_nil()
            || supervisor.session_id == root_id
        {
            return false;
        }
        if self.original_caller().is_some_and(|caller| {
            caller.session_id != supervisor.session_id
                || caller.authority_identity
                    != (crate::SessionAuthorityIdentity::Supervisor {
                        incarnation_id: supervisor.incarnation_id,
                    })
                || match self {
                    Self::CurrentUser {
                        supervisor_created_at,
                        ..
                    }
                    | Self::LegacyOrCreatedRoot {
                        supervisor_created_at,
                        ..
                    } => caller.created_at != *supervisor_created_at,
                }
        }) {
            return false;
        }
        match self {
            Self::CurrentUser { target, .. } => {
                target.session_id == root_id
                    && target.created_at == root_created_at
                    && target.kind == SessionKind::Root
                    && target.parent_session_id.is_none()
                    && (target.root_session_id.is_empty() || target.root_session_id == root_id)
                    && valid_digest(&target.authority_sha256)
                    && valid_digest(&target.ancestry_sha256)
                    && crate::supervisor_current_user_message_id(
                        supervisor,
                        target,
                        operation.as_str(),
                    ) == *receipt_id
            }
            Self::LegacyOrCreatedRoot {
                target_created_at, ..
            } => {
                *target_created_at == root_created_at
                    && crate::supervisor_followup_message_id(
                        supervisor,
                        root_id,
                        operation.as_str(),
                    ) == *receipt_id
            }
        }
    }

    /// Check that an envelope is consistent with this historical observation.
    /// This does not prove who wrote it or authorize its target.
    pub fn matches_input_envelope(&self, envelope: &SessionMessageEnvelope) -> bool {
        let (supervisor, operation) = self.reference_and_operation();
        let target_birth = match self {
            Self::CurrentUser { target, .. } => target.created_at,
            Self::LegacyOrCreatedRoot {
                target_created_at, ..
            } => *target_created_at,
        };
        self.matches_receipt(&envelope.target_session_id, target_birth, &envelope.id)
            && envelope.source
                == (crate::SessionMessageSource::Session {
                    session_id: supervisor.session_id.clone(),
                })
            && envelope.kind == SessionMessageKind::PeerMessage
            && envelope.correlation_id.as_deref() == Some(operation.as_str())
    }

    /// Exact operation identity for an Inbox retry. Current-user target
    /// metadata may advance; the caller must validate the new observation and
    /// retain the first admitted observation rather than replacing it.
    pub fn same_operation(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::CurrentUser {
                    supervisor: left_supervisor,
                    supervisor_created_at: left_birth,
                    caller: _,
                    requires_host_scope: _,
                    target: left_target,
                    operation_id: left_operation,
                },
                Self::CurrentUser {
                    supervisor: right_supervisor,
                    supervisor_created_at: right_birth,
                    caller: _,
                    requires_host_scope: _,
                    target: right_target,
                    operation_id: right_operation,
                },
            ) => {
                left_supervisor == right_supervisor
                    && left_birth == right_birth
                    && left_target.session_id == right_target.session_id
                    && left_target.created_at == right_target.created_at
                    && left_target.kind == right_target.kind
                    && left_operation == right_operation
            }
            (
                Self::LegacyOrCreatedRoot {
                    supervisor: left_supervisor,
                    supervisor_created_at: left_birth,
                    caller: _,
                    requires_host_scope: _,
                    target_created_at: left_target_birth,
                    operation_id: left_operation,
                },
                Self::LegacyOrCreatedRoot {
                    supervisor: right_supervisor,
                    supervisor_created_at: right_birth,
                    caller: _,
                    requires_host_scope: _,
                    target_created_at: right_target_birth,
                    operation_id: right_operation,
                },
            ) => {
                left_supervisor == right_supervisor
                    && left_birth == right_birth
                    && left_target_birth == right_target_birth
                    && left_operation == right_operation
            }
            _ => false,
        }
    }
}

/// Bounded correlation projection of an actual Main transcript entry. It
/// returns an operation ID only when the input has a recorded Supervisor
/// origin; arbitrary correlation text is represented by a deterministic digest,
/// which is not a secrecy mechanism. Receipt and operation IDs may themselves
/// be sensitive. Nothing here proves sender, delivery or provider consumption.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisedRootOutcomeInputProjection {
    pub receipt_id: SessionMessageId,
    pub generation: u64,
    pub correlation_id_sha256: Option<String>,
    /// Historical operation only; its presence does not authorize disclosure.
    pub recorded_supervisor_operation_id: Option<SessionMessageId>,
}

/// Bounded data returned by an integrated Observer source read after the
/// current relationship, Root lifetime and original input ACKs are checked.
/// This value is an observation, not permission to send or mutate a Session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverSupervisedRootInput {
    pub receipt_id: SessionMessageId,
    pub generation: u64,
    pub operation_id: SessionMessageId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverSupervisedRootOutcome {
    pub target_session_id: String,
    pub target_created_at: DateTime<Utc>,
    pub sequence: u64,
    pub run_id: String,
    pub status: SupervisedRootOutcomeResult,
    pub inputs: Vec<ObserverSupervisedRootInput>,
}

/// The historical view is derived from the canonical fact, rather than from
/// an independently replaceable snapshot. Values are escaped and bounded;
/// full identities remain in the individually verified Supervisor notices.
pub fn observer_root_outcome_status(outcome: &ObserverSupervisedRootOutcome) -> String {
    fn preview(value: &str) -> String {
        let mut result = String::new();
        for ch in value.chars() {
            let escaped = match ch {
                '<' => "&lt;".to_string(),
                '>' => "&gt;".to_string(),
                '&' => "&amp;".to_string(),
                '\\' | '`' | '*' | '_' | '[' | ']' | '#' | '!' | '|' => format!("\\{ch}"),
                ch if ch.is_control() || matches!(ch, '\u{2028}' | '\u{2029}') => " ".to_string(),
                ch => ch.to_string(),
            };
            if result.len() + escaped.len() > 297 {
                result.push_str("...");
                break;
            }
            result.push_str(&escaped);
        }
        result
    }
    fn identity(output: &mut String, label: &str, value: &str) {
        output.push_str(&format!("- {label} preview: {}\n", preview(value)));
        let encoded = hex::encode(value.as_bytes());
        if encoded.len() <= 800 {
            for (index, chunk) in encoded.as_bytes().chunks(400).enumerate() {
                output.push_str(&format!(
                    "- {label} UTF-8 hex part {}: {}\n",
                    index + 1,
                    std::str::from_utf8(chunk).expect("hex is ASCII")
                ));
            }
        } else {
            output.push_str(&format!(
                "- {label} SHA-256: {} (full value in Supervisor notice)\n",
                hex::encode(Sha256::digest(value.as_bytes()))
            ));
        }
    }
    let mut output = format!("# Session status\n\nRecorded Root outcome; not live progress.\n\n- Last persisted status: {}\n\n## Recorded Root outcome\n", outcome.status.observer_status());
    identity(&mut output, "Run", &outcome.run_id);
    if let Some(input) = outcome.inputs.first() {
        identity(&mut output, "Input receipt", input.receipt_id.as_str());
        identity(
            &mut output,
            "Supervisor operation",
            input.operation_id.as_str(),
        );
    }
    if outcome.inputs.len() > 1 {
        output.push_str(&format!(
            "- Verified inputs: {} (first shown; each has a separate Supervisor notice)\n",
            outcome.inputs.len()
        ));
    }
    output
}

pub const OBSERVER_ROOT_OUTCOME_BRIEF: &str =
    "# Session task brief\n\nHistorical outcome only; task details are not included.\n";

/// The historical lane is opt-in for a current status subscription. Legacy
/// snapshots continue their original behavior and never become source cursors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObserverRootOutcomeRead {
    LegacySnapshot,
    CaughtUp,
    /// Main is committed but its owned input ACK has not yet appeared.
    PendingSource,
    Next(ObserverSupervisedRootOutcome),
}

impl SupervisedRootOutcomeResult {
    /// Whitelisted persisted labels; never a provider error message.
    pub fn observer_status(&self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "error",
            Self::Cancelled => "cancelled",
            Self::Suspended => "suspended",
            Self::InterruptedUnknown => "interrupted_unknown",
        }
    }
}

/// No question text, error string or provider output is copied into this record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SupervisedRootOutcomeResult {
    Completed,
    Failed,
    Cancelled,
    Suspended,
    /// The retained owner was conclusively lost before a final commit. This is
    /// never a fabricated completion, failure or cancellation.
    InterruptedUnknown,
}

/// The input prefix observed when a clarification was persisted. Later
/// admitted inputs must not be attributed to an earlier decision request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupervisedRootOutcomeQuestion {
    pub question_id_sha256: String,
    pub admitted_input_count: usize,
}

/// A position within one Root lifetime. A question and a later outcome in the
/// same run are separate positions; this is not an Inbox receipt or grant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupervisedRootOutcomeEventPosition {
    pub run_sequence: u64,
    pub ordinal: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SupervisedRootOutcomeEventKind {
    DecisionRequired(SupervisedRootOutcomeQuestion),
    /// Suspended is nonterminal; InterruptedUnknown does not prove physical
    /// termination or the outcome of any side effect.
    ActivationOutcome(SupervisedRootOutcomeResult),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisedRootOutcomeEvent {
    pub position: SupervisedRootOutcomeEventPosition,
    pub run_id: String,
    pub kind: SupervisedRootOutcomeEventKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupervisedRootOutcomeFact {
    pub sequence: u64,
    pub activation_id: String,
    pub attempt: u64,
    pub lease_epoch: u64,
    /// Historical owner commitment, not a lease or permission to publish.
    pub lease_owner_sha256: String,
    pub run_id: String,
    /// Historical projection of the exact canonical reset ledger observed at
    /// reservation. It is not a replacement for the live generation proof.
    pub conversation_reset_sha256: Option<String>,
    pub inputs: Vec<SupervisedRootOutcomeInput>,
    /// Separately persisted clarification identities and their input prefixes.
    /// A pending question can coexist with a failed or cancelled run.
    pub questions: Vec<SupervisedRootOutcomeQuestion>,
    /// None is an unresolved reservation, including after an unconfirmed crash.
    pub result: Option<SupervisedRootOutcomeResult>,
}

/// Ordered source facts, not a delivery cursor. Reclamation must be performed
/// separately under the actual Observer/target authority locks after durable
/// cursor proof; this type deliberately has no automatic eviction operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupervisedRootOutcomes {
    pub version: u32,
    pub root_session_id: String,
    pub root_created_at: DateTime<Utc>,
    pub facts: Vec<SupervisedRootOutcomeFact>,
}

/// An existing reservation is not permission to start the same run again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupervisedRootOutcomeReservation {
    New(u64),
    Existing { sequence: u64, finalized: bool },
}

// Actor Directory accepts any nonempty activation ID and any run ID whose
// trimmed form is nonempty. Do not silently narrow those identities to the
// separate Inbox expected-run wire format. The complete serialized ledger is
// still subject to its byte budget before a transition can be accepted.
fn valid_activation_id(value: &str) -> bool {
    !value.is_empty()
}

fn valid_run_id(value: &str) -> bool {
    !value.trim().is_empty()
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn lease_owner_digest(owner: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"bamboo.supervised-root-outcome-owner.v1\0");
    digest.update(owner.as_bytes());
    hex::encode(digest.finalize())
}

fn correlation_digest(correlation: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"bamboo.supervised-root-outcome-correlation.v1\0");
    digest.update(correlation.as_bytes());
    hex::encode(digest.finalize())
}

/// Hash one concrete response occurrence, not a provider tool-call ID alone.
/// A storage writer still has to prove that this occurrence is the actual
/// pending decision in the committed Main; the digest is not authority.
pub fn supervised_root_question_occurrence_digest(occurrence: &ResponseOccurrence) -> String {
    // Stream existing values without adding a limit or normalization rule to
    // provider IDs. Length prefixes and an option tag prevent ambiguity.
    let mut digest = Sha256::new();
    digest.update(b"bamboo.supervised-root-question-occurrence.v1\0");
    for value in [&occurrence.tool_call_id, &occurrence.tool_result_message_id] {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }
    match &occurrence.permission_generation {
        None => digest.update([0]),
        Some(value) => {
            digest.update([1]);
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value.as_bytes());
        }
    }
    hex::encode(digest.finalize())
}

fn fact_matches_fence(fact: &SupervisedRootOutcomeFact, fence: &ActorActivationFence) -> bool {
    fact.run_id == fence.run_id
        && fact.activation_id == fence.activation_id
        && fact.attempt == fence.attempt
        && fact.lease_epoch == fence.lease_epoch
        && fact.lease_owner_sha256 == lease_owner_digest(&fence.lease_owner)
}

// Projection-only decoding of the existing Actor input checkpoint metadata.
// Message metadata is already a Value, so duplicate raw JSON keys may have
// been folded before this check. The physical Actor/Inbox proof and strict raw
// transcript parsing remain exclusively in storage.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnedInputCheckpointProjection {
    version: u32,
    envelope_id: String,
    target_session_id: String,
    session_created_at: DateTime<Utc>,
    generation: u64,
    activation_intent: Option<SessionInboxActivationIntent>,
    activation_policy: SessionActivationPolicy,
}

fn matches_projected_input(
    message: &Message,
    envelope: &SessionMessageEnvelope,
    input: &SupervisedRootOutcomeInput,
    fact: &SupervisedRootOutcomeFact,
    session: &Session,
) -> Result<bool, &'static str> {
    let Some(checkpoint) = message
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get(OWNED_INPUT_CHECKPOINT_KEY))
    else {
        if input.supervisor_origin.is_some() {
            return Err("Supervisor input origin has no owned checkpoint");
        }
        return Ok(crate::is_matching_session_message(message, envelope));
    };
    let fields = checkpoint
        .as_object()
        .ok_or("invalid owned input checkpoint")?;
    let names = [
        "version",
        "envelope_id",
        "target_session_id",
        "session_created_at",
        "generation",
        "activation_intent",
        "activation_policy",
    ];
    if fields.len() != names.len() || names.iter().any(|name| !fields.contains_key(*name)) {
        return Err("invalid owned input checkpoint fields");
    }
    let checkpoint: OwnedInputCheckpointProjection =
        serde_json::from_value(checkpoint.clone()).map_err(|_| "invalid owned input checkpoint")?;
    if input.supervisor_origin.is_some()
        && checkpoint
            .activation_intent
            .as_ref()
            .and_then(SessionInboxActivationIntent::supervised_root_origin)
            != input.supervisor_origin.as_ref()
    {
        return Err("Supervisor input origin differs from its owned checkpoint");
    }
    let intent_conflicts = checkpoint.activation_intent.as_ref().is_some_and(|intent| {
        let Ok(policy) = intent.policy() else {
            return true;
        };
        !intent.admits_run(Some(&fact.run_id))
            || if intent.delivery().is_some() {
                checkpoint.activation_policy != SessionActivationPolicy::RespectSpecificWait
            } else {
                policy == SessionActivationPolicy::InterruptSpecificWait
                    && checkpoint.activation_policy
                        != SessionActivationPolicy::InterruptSpecificWait
            }
    });
    if checkpoint.version != 1
        || !matches!(
            envelope.kind,
            SessionMessageKind::UserInput | SessionMessageKind::PeerMessage
        )
        || !matches!(&envelope.body, SessionMessageBody::Content(content)
            if content.parts.is_empty() && !content.text.trim().is_empty())
        || checkpoint.envelope_id != input.receipt_id.as_str()
        || checkpoint.target_session_id != session.id
        || checkpoint.session_created_at != session.created_at
        || checkpoint.generation != input.generation
        || intent_conflicts
    {
        return Err("owned input checkpoint identity differs");
    }
    // A coordinator watermark may upgrade a legacy Respect intent to an
    // effective Interrupt policy; that evidence is not reconstructed here.
    let mut plain = message.clone();
    plain
        .metadata
        .as_mut()
        .and_then(serde_json::Value::as_object_mut)
        .ok_or("invalid owned input metadata")?
        .remove(OWNED_INPUT_CHECKPOINT_KEY);
    Ok(crate::is_matching_session_message(&plain, envelope))
}

impl SupervisedRootOutcomes {
    pub fn new(
        root_session_id: &str,
        root_created_at: DateTime<Utc>,
    ) -> Result<Self, &'static str> {
        crate::validate_supervisor_target_id(root_session_id)?;
        Ok(Self {
            version: SUPERVISED_ROOT_OUTCOMES_VERSION,
            root_session_id: root_session_id.into(),
            root_created_at,
            facts: Vec::new(),
        })
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        crate::validate_supervisor_target_id(&self.root_session_id)?;
        if self.version != SUPERVISED_ROOT_OUTCOMES_VERSION
            || self.facts.len() > MAX_SUPERVISED_ROOT_OUTCOMES
        {
            return Err("unsupported or oversized Root outcome facts");
        }
        let mut previous_sequence = 0_u64;
        let mut previous_attempt = 0_u64;
        let mut previous_lease_epoch = 0_u64;
        let mut run_ids = std::collections::BTreeSet::new();
        let mut activation_ids = std::collections::BTreeSet::new();
        let mut receipt_generations = std::collections::BTreeMap::new();
        let mut generation_receipts = std::collections::BTreeMap::new();
        let mut receipt_origins = std::collections::BTreeMap::new();
        for (index, fact) in self.facts.iter().enumerate() {
            // Version 1 does not reclaim any prefix. Gaps (including a
            // removed first fact) are corruption, not evidence of delivery.
            if previous_sequence.checked_add(1) != Some(fact.sequence)
                || fact.attempt <= previous_attempt
                || !valid_activation_id(&fact.activation_id)
                || !valid_run_id(&fact.run_id)
                || fact
                    .conversation_reset_sha256
                    .as_deref()
                    .is_some_and(|digest| !valid_digest(digest))
                || fact.lease_epoch < fact.attempt
                || fact.lease_epoch <= previous_lease_epoch
                || !valid_digest(&fact.lease_owner_sha256)
                || !activation_ids.insert(&fact.activation_id)
                || !run_ids.insert(&fact.run_id)
                || fact.inputs.len() > MAX_SUPERVISED_ROOT_OUTCOME_INPUTS
                || fact.questions.len() > MAX_SUPERVISED_ROOT_OUTCOME_QUESTIONS
                || (fact.result.is_none() && index + 1 != self.facts.len())
            {
                return Err("invalid Root outcome identity or order");
            }
            previous_sequence = fact.sequence;
            previous_attempt = fact.attempt;
            previous_lease_epoch = fact.lease_epoch;
            let mut questions = std::collections::BTreeSet::new();
            let mut previous_input_count = 0;
            for question in &fact.questions {
                if !valid_digest(&question.question_id_sha256)
                    || !questions.insert(&question.question_id_sha256)
                    || question.admitted_input_count < previous_input_count
                    || question.admitted_input_count > fact.inputs.len()
                {
                    return Err("invalid or repeated Root outcome question digest");
                }
                previous_input_count = question.admitted_input_count;
            }
            let mut ids = std::collections::BTreeSet::new();
            let mut generations = std::collections::BTreeSet::new();
            for input in &fact.inputs {
                if input.generation == 0
                    || input.supervisor_origin.as_ref().is_some_and(|origin| {
                        !origin.matches_receipt(
                            &self.root_session_id,
                            self.root_created_at,
                            &input.receipt_id,
                        )
                    })
                    || !ids.insert(input.receipt_id.as_str())
                    || !generations.insert(input.generation)
                    || receipt_origins
                        // Inbox tombstones are lifetime-wide and do not reset
                        // with the conversation generation.
                        .insert(input.receipt_id.as_str(), input.supervisor_origin.as_ref())
                        .is_some_and(|old| old != input.supervisor_origin.as_ref())
                    || receipt_generations
                        .insert(input.receipt_id.as_str(), input.generation)
                        .is_some_and(|old| old != input.generation)
                    || generation_receipts
                        .insert(input.generation, input.receipt_id.as_str())
                        .is_some_and(|old| old != input.receipt_id.as_str())
                {
                    return Err("invalid Root outcome input receipt");
                }
            }
        }
        let bytes = serde_json::to_vec(self)
            .map_err(|_| "Root outcome facts cannot be encoded")?
            .len();
        // Reserve every remaining question and the terminal result before
        // accepting inputs or starting a run. Each newly recorded digest
        // consumes its reserved allowance instead of reserving it twice.
        let finalization_headroom = self
            .facts
            .last()
            .filter(|fact| fact.result.is_none())
            .map_or(0, |fact| {
                OUTCOME_TERMINAL_HEADROOM
                    + (MAX_SUPERVISED_ROOT_OUTCOME_QUESTIONS - fact.questions.len())
                        * OUTCOME_QUESTION_HEADROOM
            });
        if bytes
            .checked_add(finalization_headroom)
            .is_none_or(|total| total > MAX_SUPERVISED_ROOT_OUTCOMES_BYTES)
        {
            return Err("Root outcome byte capacity exhausted");
        }
        Ok(())
    }

    /// Read one historical fact after an existing event position. A caller
    /// must bind the position to the Root birth and its authorized Observer,
    /// and advance it only with a real Inbox receipt. This pure selector does
    /// not publish or acknowledge anything.
    pub fn next_event_after(
        &self,
        after: Option<&SupervisedRootOutcomeEventPosition>,
    ) -> Result<Option<SupervisedRootOutcomeEvent>, &'static str> {
        self.validate()?;
        let mut found = after.is_none();
        for fact in &self.facts {
            let mut events = fact
                .questions
                .iter()
                .enumerate()
                .map(|(index, question)| SupervisedRootOutcomeEvent {
                    position: SupervisedRootOutcomeEventPosition {
                        run_sequence: fact.sequence,
                        ordinal: (index + 1) as u8,
                    },
                    run_id: fact.run_id.clone(),
                    kind: SupervisedRootOutcomeEventKind::DecisionRequired(question.clone()),
                })
                .collect::<Vec<_>>();
            if let Some(result) = &fact.result {
                events.push(SupervisedRootOutcomeEvent {
                    position: SupervisedRootOutcomeEventPosition {
                        run_sequence: fact.sequence,
                        ordinal: (fact.questions.len() + 1) as u8,
                    },
                    run_id: fact.run_id.clone(),
                    kind: SupervisedRootOutcomeEventKind::ActivationOutcome(result.clone()),
                });
            }
            for event in events {
                if found {
                    return Ok(Some(event));
                }
                if after == Some(&event.position) {
                    found = true;
                }
            }
        }
        if found {
            Ok(None)
        } else {
            Err("Root outcome event position is missing")
        }
    }

    /// Read the input markers for one fact from a complete Main snapshot.
    /// This checks identity and marker consistency, but cannot authenticate a
    /// caller, prove the original owned claim, or grant Observer access. The
    /// integrated caller must hold the real storage/relationship authorities.
    pub fn project_inputs_from_main(
        &self,
        session: &Session,
        sequence: u64,
    ) -> Result<Vec<SupervisedRootOutcomeInputProjection>, &'static str> {
        self.project_input_prefix_from_main(session, sequence, None)
    }

    /// Project only the inputs present at a particular question boundary.
    /// The index is zero-based and refers to this fact's immutable question
    /// order. This remains a consistency check, not Observer authorization.
    pub fn project_question_inputs_from_main(
        &self,
        session: &Session,
        sequence: u64,
        question_index: usize,
    ) -> Result<Vec<SupervisedRootOutcomeInputProjection>, &'static str> {
        self.project_input_prefix_from_main(session, sequence, Some(question_index))
    }

    fn project_input_prefix_from_main(
        &self,
        session: &Session,
        sequence: u64,
        question_index: Option<usize>,
    ) -> Result<Vec<SupervisedRootOutcomeInputProjection>, &'static str> {
        self.validate()?;
        if session.id != self.root_session_id
            || session.created_at != self.root_created_at
            || session.kind != SessionKind::Root
            || !session.authority_identity.is_ordinary()
            || session.parent_session_id.is_some()
            || session.parent_created_at.is_some()
            || session.spawn_depth != 0
            || (!session.root_session_id.is_empty() && session.root_session_id != session.id)
        {
            return Err("Root outcome Main identity differs");
        }
        if session
            .runtime_metadata
            .as_ref()
            .and_then(|metadata| metadata.supervised_root_outcomes.as_ref())
            != Some(self)
        {
            return Err("Root outcome facts differ from Main snapshot");
        }
        let fact = self
            .facts
            .iter()
            .find(|fact| fact.sequence == sequence)
            .ok_or("Root outcome sequence is missing")?;
        let input_count = match question_index {
            Some(index) => {
                fact.questions
                    .get(index)
                    .ok_or("Root outcome question is missing")?
                    .admitted_input_count
            }
            None => fact.inputs.len(),
        };
        let inputs = &fact.inputs[..input_count];
        let reset = session
            .conversation_reset_sha256()
            .map_err(|_| "Root outcome Main generation is invalid")?;
        if fact.conversation_reset_sha256 != reset {
            return Err("Root outcome Main generation differs");
        }
        let wanted: std::collections::BTreeSet<_> = inputs
            .iter()
            .map(|input| input.receipt_id.as_str())
            .collect();
        let mut messages = std::collections::BTreeMap::new();
        for message in &session.messages {
            if wanted.contains(message.id.as_str())
                && messages.insert(message.id.as_str(), message).is_some()
            {
                return Err("Root outcome Main repeats an input receipt");
            }
        }
        inputs
            .iter()
            .map(|input| {
                let message = messages
                    .get(input.receipt_id.as_str())
                    .ok_or("Root outcome Main input receipt is missing")?;
                let marker = message
                    .metadata
                    .as_ref()
                    .and_then(|value| value.get("session_message"))
                    .ok_or("Root outcome Main input marker is missing")?;
                let envelope: SessionMessageEnvelope = serde_json::from_value(marker.clone())
                    .map_err(|_| "Root outcome Main input marker is invalid")?;
                if envelope.id != input.receipt_id
                    || envelope.target_session_id != self.root_session_id
                    || !matches_projected_input(message, &envelope, input, fact, session)?
                {
                    return Err("Root outcome Main input marker differs");
                }
                if let Some(origin) = &input.supervisor_origin {
                    let (supervisor, operation) = origin.reference_and_operation();
                    if envelope.source
                        != (crate::SessionMessageSource::Session {
                            session_id: supervisor.session_id.clone(),
                        })
                        || envelope.kind != SessionMessageKind::PeerMessage
                        || envelope.correlation_id.as_deref() != Some(operation.as_str())
                    {
                        return Err("Root outcome Supervisor input marker differs");
                    }
                }
                Ok(SupervisedRootOutcomeInputProjection {
                    receipt_id: input.receipt_id.clone(),
                    generation: input.generation,
                    correlation_id_sha256: envelope
                        .correlation_id
                        .as_deref()
                        .map(correlation_digest),
                    recorded_supervisor_operation_id: input
                        .supervisor_origin
                        .as_ref()
                        .map(|origin| origin.reference_and_operation().1.clone()),
                })
            })
            .collect()
    }

    /// Pure transition only. The caller must first prove the Running Actor and
    /// persist this reservation before any provider or tool side effect.
    pub fn reserve(
        &mut self,
        fence: &ActorActivationFence,
        conversation_reset_sha256: Option<&str>,
    ) -> Result<SupervisedRootOutcomeReservation, &'static str> {
        self.validate()?;
        if fence.actor_id != self.root_session_id
            || !valid_activation_id(&fence.activation_id)
            || !valid_run_id(&fence.run_id)
            || fence.attempt == 0
            || fence.lease_epoch < fence.attempt
            || fence.lease_owner.trim().is_empty()
            || fence.schema_version != crate::ACTOR_DIRECTORY_SCHEMA_VERSION
            || conversation_reset_sha256.is_some_and(|digest| !valid_digest(digest))
        {
            return Err("Root outcome reservation identity differs");
        }
        if let Some(existing) = self.facts.iter().find(|fact| fact.run_id == fence.run_id) {
            return if existing.activation_id == fence.activation_id
                && existing.attempt == fence.attempt
                && existing.lease_epoch == fence.lease_epoch
                && existing.lease_owner_sha256 == lease_owner_digest(&fence.lease_owner)
                && existing.conversation_reset_sha256.as_deref() == conversation_reset_sha256
            {
                Ok(SupervisedRootOutcomeReservation::Existing {
                    sequence: existing.sequence,
                    finalized: existing.result.is_some(),
                })
            } else {
                Err("Root outcome run identity conflicts")
            };
        }
        if self.facts.len() >= MAX_SUPERVISED_ROOT_OUTCOMES {
            return Err("Root outcome capacity exhausted before execution");
        }
        if self.facts.last().is_some_and(|fact| fact.result.is_none()) {
            return Err("prior Root outcome reservation remains unresolved");
        }
        let sequence = self
            .facts
            .last()
            .map_or(Some(1), |fact| fact.sequence.checked_add(1))
            .ok_or("Root outcome sequence exhausted")?;
        let mut candidate = self.clone();
        candidate.facts.push(SupervisedRootOutcomeFact {
            sequence,
            activation_id: fence.activation_id.clone(),
            attempt: fence.attempt,
            lease_epoch: fence.lease_epoch,
            lease_owner_sha256: lease_owner_digest(&fence.lease_owner),
            run_id: fence.run_id.clone(),
            conversation_reset_sha256: conversation_reset_sha256.map(str::to_owned),
            inputs: Vec::new(),
            questions: Vec::new(),
            result: None,
        });
        candidate.validate()?;
        *self = candidate;
        Ok(SupervisedRootOutcomeReservation::New(sequence))
    }

    /// Apply only after the actual owned claim has been checked under the same
    /// Main/Inbox locks. Full capacity leaves the original claim unacknowledged.
    pub fn record_input(
        &mut self,
        fence: &ActorActivationFence,
        input: SupervisedRootOutcomeInput,
    ) -> Result<bool, &'static str> {
        self.validate()?;
        if !self.matches_fence_shape(fence) {
            return Err("Root outcome target differs");
        }
        if let Some(existing) = self
            .facts
            .iter()
            .find(|fact| fact_matches_fence(fact, fence))
        {
            if let Some(recorded) = existing
                .inputs
                .iter()
                .find(|entry| entry.receipt_id == input.receipt_id)
            {
                return if recorded == &input {
                    Ok(false)
                } else {
                    Err("Root input receipt conflicts")
                };
            }
        }
        let mut candidate = self.clone();
        let fact = candidate.current_mut(fence)?;
        if fact.result.is_some() || fact.inputs.len() >= MAX_SUPERVISED_ROOT_OUTCOME_INPUTS {
            return Err("Root outcome input capacity exhausted or already finalized");
        }
        fact.inputs.push(input);
        candidate.validate()?;
        *self = candidate;
        Ok(true)
    }

    /// Pure transition only. The caller must prove the exact response
    /// occurrence in the retained Actor's durable question checkpoint.
    pub fn record_question(
        &mut self,
        fence: &ActorActivationFence,
        occurrence: &ResponseOccurrence,
    ) -> Result<bool, &'static str> {
        self.validate()?;
        if !self.matches_fence_shape(fence) {
            return Err("Root outcome question identity differs");
        }
        let question_id_sha256 = supervised_root_question_occurrence_digest(occurrence);
        if let Some(existing) = self
            .facts
            .iter()
            .find(|fact| fact_matches_fence(fact, fence))
        {
            if existing
                .questions
                .iter()
                .any(|question| question.question_id_sha256 == question_id_sha256)
            {
                return Ok(false);
            }
        }
        let mut candidate = self.clone();
        let fact = candidate.current_mut(fence)?;
        if fact.result.is_some() || fact.questions.len() >= MAX_SUPERVISED_ROOT_OUTCOME_QUESTIONS {
            return Err("Root outcome question capacity exhausted or already finalized");
        }
        fact.questions.push(SupervisedRootOutcomeQuestion {
            question_id_sha256,
            admitted_input_count: fact.inputs.len(),
        });
        candidate.validate()?;
        *self = candidate;
        Ok(true)
    }

    /// The durable final writer must derive this value after its last state
    /// reconciliation. This function does not itself prove the final commit.
    pub fn finalize(
        &mut self,
        fence: &ActorActivationFence,
        result: SupervisedRootOutcomeResult,
    ) -> Result<bool, &'static str> {
        self.validate()?;
        if result == SupervisedRootOutcomeResult::InterruptedUnknown {
            return Err("interrupted Root outcome requires separate loss recovery");
        }
        if !self.matches_fence_shape(fence) {
            return Err("Root outcome target differs");
        }
        if let Some(existing) = self
            .facts
            .iter()
            .find(|fact| fact_matches_fence(fact, fence))
        {
            if let Some(current) = &existing.result {
                return if current == &result {
                    Ok(false)
                } else {
                    Err("Root outcome result conflicts")
                };
            }
        }
        let mut candidate = self.clone();
        let fact = candidate.current_mut(fence)?;
        fact.result = Some(result);
        candidate.validate()?;
        *self = candidate;
        Ok(true)
    }

    /// Atomically record an uncommitted predecessor as unknown and reserve its
    /// successor. The caller must independently prove the successor's live
    /// Actor fence, Root birth, reset generation and predecessor supersession
    /// under physical locks. Counters alone never grant that authority.
    pub fn recover_predecessor_and_reserve(
        &mut self,
        successor: &ActorActivationFence,
        conversation_reset_sha256: Option<&str>,
    ) -> Result<SupervisedRootOutcomeReservation, &'static str> {
        self.validate()?;
        if !self.matches_fence_shape(successor)
            || conversation_reset_sha256.is_some_and(|digest| !valid_digest(digest))
        {
            return Err("Root outcome successor identity differs");
        }
        if let Some((index, existing)) = self
            .facts
            .iter()
            .enumerate()
            .find(|(_, fact)| fact.run_id == successor.run_id)
        {
            let previous = index.checked_sub(1).and_then(|index| self.facts.get(index));
            return if fact_matches_fence(existing, successor)
                && existing.conversation_reset_sha256.as_deref() == conversation_reset_sha256
                && previous.is_some_and(|fact| {
                    fact.result == Some(SupervisedRootOutcomeResult::InterruptedUnknown)
                        && fact.conversation_reset_sha256.as_deref() == conversation_reset_sha256
                }) {
                Ok(SupervisedRootOutcomeReservation::Existing {
                    sequence: existing.sequence,
                    finalized: existing.result.is_some(),
                })
            } else {
                Err("Root outcome recovery replay conflicts")
            };
        }
        let Some(previous) = self.facts.last() else {
            return Err("Root outcome predecessor is missing");
        };
        if previous.conversation_reset_sha256.as_deref() != conversation_reset_sha256 {
            return Err("Root outcome predecessor generation changed");
        }
        if successor.attempt <= previous.attempt || successor.lease_epoch <= previous.lease_epoch {
            return Err("Root outcome predecessor was not superseded");
        }
        if previous.result.is_some() {
            return Err("Root outcome predecessor already finalized");
        }
        let mut candidate = self.clone();
        candidate
            .facts
            .last_mut()
            .expect("predecessor checked")
            .result = Some(SupervisedRootOutcomeResult::InterruptedUnknown);
        // Reserve on the clone: ID reuse, byte limits and full capacity leave
        // the original predecessor unresolved rather than partially committed.
        let reservation = candidate.reserve(successor, conversation_reset_sha256)?;
        *self = candidate;
        Ok(reservation)
    }

    /// Close a lost predecessor without creating a fact for an unrelated
    /// successor. Storage must prove this successor is the live owner under
    /// the Root's physical guard; counters alone are never authority.
    pub fn mark_predecessor_interrupted(
        &mut self,
        successor: &ActorActivationFence,
        conversation_reset_sha256: Option<&str>,
    ) -> Result<bool, &'static str> {
        self.validate()?;
        if !self.matches_fence_shape(successor)
            || conversation_reset_sha256.is_some_and(|digest| !valid_digest(digest))
        {
            return Err("Root outcome successor identity differs");
        }
        let previous = self
            .facts
            .last()
            .ok_or("Root outcome predecessor is missing")?;
        if previous.conversation_reset_sha256.as_deref() != conversation_reset_sha256
            || successor.attempt <= previous.attempt
            || successor.lease_epoch <= previous.lease_epoch
            || self.facts.iter().any(|fact| {
                fact.run_id == successor.run_id || fact.activation_id == successor.activation_id
            })
        {
            return Err("Root outcome predecessor was not superseded");
        }
        match previous.result.as_ref() {
            Some(SupervisedRootOutcomeResult::InterruptedUnknown) => return Ok(false),
            Some(_) => return Err("Root outcome predecessor already finalized"),
            None => {}
        }
        let mut candidate = self.clone();
        candidate
            .facts
            .last_mut()
            .expect("predecessor checked")
            .result = Some(SupervisedRootOutcomeResult::InterruptedUnknown);
        candidate.validate()?;
        *self = candidate;
        Ok(true)
    }

    fn current_mut(
        &mut self,
        fence: &ActorActivationFence,
    ) -> Result<&mut SupervisedRootOutcomeFact, &'static str> {
        if !self.matches_fence_shape(fence) {
            return Err("Root outcome target differs");
        }
        self.facts
            .last_mut()
            .filter(|fact| fact_matches_fence(fact, fence))
            .ok_or("Root outcome owner differs")
    }

    // This is a structural check, not proof of ownership. Storage must
    // validate the full live fence, including lease_owner, before *every*
    // transition and before returning an idempotent replay.
    fn matches_fence_shape(&self, fence: &ActorActivationFence) -> bool {
        fence.schema_version == crate::ACTOR_DIRECTORY_SCHEMA_VERSION
            && fence.actor_id == self.root_session_id
            && fence.attempt > 0
            && fence.lease_epoch >= fence.attempt
            && !fence.lease_owner.trim().is_empty()
            && valid_activation_id(&fence.activation_id)
            && valid_run_id(&fence.run_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn occurrence(id: &str) -> ResponseOccurrence {
        ResponseOccurrence {
            tool_call_id: format!("call-{id}"),
            tool_result_message_id: format!("result-{id}"),
            permission_generation: None,
        }
    }

    fn fence(attempt: u64) -> ActorActivationFence {
        ActorActivationFence {
            schema_version: crate::ACTOR_DIRECTORY_SCHEMA_VERSION,
            actor_id: "test-root".into(),
            activation_id: format!("activation-{attempt}"),
            attempt,
            run_id: format!("run-{attempt}"),
            lease_owner: "test-owner".into(),
            lease_epoch: attempt,
        }
    }

    fn input(id: &str, generation: u64) -> SupervisedRootOutcomeInput {
        SupervisedRootOutcomeInput {
            receipt_id: SessionMessageId::parse(id).unwrap(),
            generation,
            supervisor_origin: None,
        }
    }

    #[test]
    fn outcome_facts_keep_same_status_runs_and_exact_receipt_retries_distinct() {
        let mut facts = SupervisedRootOutcomes::new("test-root", Utc::now()).unwrap();
        let first = fence(1);
        let second = fence(3); // Actor attempts need not be contiguous.
        assert_eq!(
            facts.reserve(&first, None).unwrap(),
            SupervisedRootOutcomeReservation::New(1)
        );
        assert!(facts.record_input(&first, input("receipt-1", 1)).unwrap());
        assert!(!facts.record_input(&first, input("receipt-1", 1)).unwrap());
        assert!(facts
            .finalize(&first, SupervisedRootOutcomeResult::Completed)
            .unwrap());
        assert_eq!(
            facts.reserve(&second, None).unwrap(),
            SupervisedRootOutcomeReservation::New(2)
        );
        assert!(facts.record_input(&second, input("receipt-2", 2)).unwrap());
        assert!(facts
            .finalize(&second, SupervisedRootOutcomeResult::Completed)
            .unwrap());
        assert!(!facts
            .finalize(&first, SupervisedRootOutcomeResult::Completed)
            .unwrap());
        assert!(!facts.record_input(&first, input("receipt-1", 1)).unwrap());
        assert_eq!(facts.facts[0].run_id, "run-1");
        assert_eq!(facts.facts[1].run_id, "run-3");
        let saved = serde_json::to_vec(&facts).unwrap();
        let reopened: SupervisedRootOutcomes = serde_json::from_slice(&saved).unwrap();
        reopened.validate().unwrap();
        assert_eq!(reopened, facts);
    }

    #[test]
    fn outcome_capacity_and_unresolved_owner_never_evict_committed_facts() {
        let mut facts = SupervisedRootOutcomes::new("test-root", Utc::now()).unwrap();
        let first = fence(1);
        facts.reserve(&first, None).unwrap();
        let before = facts.clone();
        assert!(facts.reserve(&fence(2), None).is_err());
        assert_eq!(facts, before);
        assert_eq!(
            facts
                .recover_predecessor_and_reserve(&fence(2), None)
                .unwrap(),
            SupervisedRootOutcomeReservation::New(2)
        );
        facts
            .finalize(&fence(2), SupervisedRootOutcomeResult::Completed)
            .unwrap();
        for attempt in 3..=MAX_SUPERVISED_ROOT_OUTCOMES as u64 {
            let current = fence(attempt);
            facts.reserve(&current, None).unwrap();
            facts
                .finalize(&current, SupervisedRootOutcomeResult::Completed)
                .unwrap();
        }
        let before = facts.clone();
        assert!(facts
            .reserve(&fence(MAX_SUPERVISED_ROOT_OUTCOMES as u64 + 1), None)
            .is_err());
        assert_eq!(
            facts.reserve(&first, None).unwrap(),
            SupervisedRootOutcomeReservation::Existing {
                sequence: 1,
                finalized: true
            }
        );
        assert_eq!(facts, before);
        let mut pending_full = facts.clone();
        pending_full.facts.last_mut().unwrap().result = None;
        pending_full.validate().unwrap();
        let before = pending_full.clone();
        assert!(pending_full
            .recover_predecessor_and_reserve(&fence(MAX_SUPERVISED_ROOT_OUTCOMES as u64 + 1), None)
            .is_err());
        assert_eq!(pending_full, before);
    }

    #[test]
    fn outcome_input_limit_and_conflicting_replay_preserve_reservation() {
        let mut facts = SupervisedRootOutcomes::new("test-root", Utc::now()).unwrap();
        let current = fence(1);
        facts.reserve(&current, None).unwrap();
        for generation in 1..=MAX_SUPERVISED_ROOT_OUTCOME_INPUTS as u64 {
            facts
                .record_input(
                    &current,
                    input(&format!("receipt-{generation}"), generation),
                )
                .unwrap();
        }
        let before = facts.clone();
        assert!(facts.record_input(&current, input("extra", 999)).is_err());
        assert!(facts
            .record_input(&current, input("receipt-1", 999))
            .is_err());
        assert_eq!(facts, before);
        let question = occurrence("a");
        assert!(facts.record_question(&current, &question).unwrap());
        assert!(!facts.record_question(&current, &question).unwrap());
        assert!(facts
            .finalize(&current, SupervisedRootOutcomeResult::Failed)
            .unwrap());
        assert_eq!(
            facts.facts[0].questions,
            vec![SupervisedRootOutcomeQuestion {
                question_id_sha256: supervised_root_question_occurrence_digest(&question),
                admitted_input_count: MAX_SUPERVISED_ROOT_OUTCOME_INPUTS,
            }]
        );
        assert!(facts.record_question(&current, &occurrence("b")).is_err());
        assert!(facts
            .finalize(&current, SupervisedRootOutcomeResult::Completed)
            .is_err());
    }

    #[test]
    fn outcome_wrong_root_or_duplicate_run_fails_closed() {
        let mut facts = SupervisedRootOutcomes::new("test-root", Utc::now()).unwrap();
        let current = fence(1);
        facts.reserve(&current, None).unwrap();
        facts
            .finalize(&current, SupervisedRootOutcomeResult::Failed)
            .unwrap();
        let mut wrong_root = current.clone();
        wrong_root.actor_id = "other-root".into();
        assert!(facts
            .finalize(&wrong_root, SupervisedRootOutcomeResult::Failed)
            .is_err());
        let mut forged = facts.clone();
        let mut duplicate = forged.facts[0].clone();
        duplicate.sequence = 2;
        duplicate.attempt = 2;
        forged.facts.push(duplicate);
        assert!(forged.validate().is_err());
    }

    #[test]
    fn outcome_keeps_actor_run_ids_without_the_inbox_wire_restriction() {
        let mut facts = SupervisedRootOutcomes::new("test-root", Utc::now()).unwrap();
        let mut first = fence(1);
        first.run_id = "run with 空格".into();
        facts.reserve(&first, None).unwrap();
        facts
            .finalize(&first, SupervisedRootOutcomeResult::Completed)
            .unwrap();

        let mut second = fence(2);
        second.run_id = "r".repeat(129);
        facts.reserve(&second, None).unwrap();
        facts
            .finalize(&second, SupervisedRootOutcomeResult::Completed)
            .unwrap();
        let restored: SupervisedRootOutcomes =
            serde_json::from_slice(&serde_json::to_vec(&facts).unwrap()).unwrap();
        restored.validate().unwrap();
        assert_eq!(restored.facts[0].run_id, first.run_id);
        assert_eq!(restored.facts[1].run_id, second.run_id);

        let mut empty = fence(3);
        empty.run_id = " \t\n".into();
        let before = facts.clone();
        assert!(facts.reserve(&empty, None).is_err());
        assert_eq!(facts, before);

        // The ledger's total serialized limit still applies. Production must
        // check that capacity before any provider or tool side effect.
        let mut oversized = fence(3);
        oversized.run_id = "r".repeat(MAX_SUPERVISED_ROOT_OUTCOMES_BYTES + 1);
        assert!(facts.reserve(&oversized, None).is_err());
        assert_eq!(facts, before);
    }

    #[test]
    fn outcome_receipt_recovery_preserves_exact_global_generation() {
        let mut facts = SupervisedRootOutcomes::new("test-root", Utc::now()).unwrap();
        let first = fence(1);
        facts.reserve(&first, None).unwrap();
        facts.record_input(&first, input("receipt-1", 1)).unwrap();
        let second = fence(2);
        assert_eq!(
            facts
                .recover_predecessor_and_reserve(&second, None)
                .unwrap(),
            SupervisedRootOutcomeReservation::New(2)
        );
        // A successor can encounter the exact receipt after an uncertain ACK.
        facts.record_input(&second, input("receipt-1", 1)).unwrap();
        let before = facts.clone();
        assert!(facts
            .record_input(&second, input("receipt-other", 1))
            .is_err());
        assert!(facts.record_input(&second, input("receipt-1", 2)).is_err());
        assert_eq!(facts, before);
        let mut gap = facts.clone();
        gap.facts[1].sequence = 3;
        assert!(gap.validate().is_err());
        let mut epoch_regression = facts.clone();
        epoch_regression.facts[1].lease_epoch = 1;
        assert!(epoch_regression.validate().is_err());
    }

    #[test]
    fn unrelated_live_successor_marks_only_lost_predecessor_unknown() {
        let mut outcomes = SupervisedRootOutcomes::new("test-root", Utc::now()).unwrap();
        let first = fence(1);
        outcomes.reserve(&first, None).unwrap();
        let before = outcomes.clone();
        assert!(outcomes.mark_predecessor_interrupted(&first, None).is_err());
        assert!(outcomes
            .mark_predecessor_interrupted(&fence(2), Some(&"a".repeat(64)))
            .is_err());
        assert_eq!(outcomes, before);

        let successor = fence(2);
        assert!(outcomes
            .mark_predecessor_interrupted(&successor, None)
            .unwrap());
        assert!(!outcomes
            .mark_predecessor_interrupted(&successor, None)
            .unwrap());
        assert_eq!(outcomes.facts.len(), 1);
        assert_eq!(outcomes.facts[0].run_id, first.run_id);
        assert_eq!(
            outcomes.facts[0].result,
            Some(SupervisedRootOutcomeResult::InterruptedUnknown)
        );
    }

    #[test]
    fn recorded_supervisor_origin_is_lifetime_bound_and_cannot_be_retroactively_upgraded() {
        let mut session = Session::new("test-root", "synthetic-model");
        let supervisor = SupervisorReference {
            session_id: crate::DEFAULT_SUPERVISOR_SESSION_ID.into(),
            incarnation_id: uuid::Uuid::new_v4(),
        };
        let target = SupervisorCurrentUserTarget::capture(&session).unwrap();
        let operation = SessionMessageId::parse("operation-1").unwrap();
        let receipt =
            crate::supervisor_current_user_message_id(&supervisor, &target, operation.as_str());
        let origin = SupervisedRootOutcomeInputOrigin::CurrentUser {
            supervisor: supervisor.clone(),
            supervisor_created_at: Utc::now(),
            caller: None,
            requires_host_scope: None,
            target: target.clone(),
            operation_id: operation.clone(),
        };
        assert!(!origin.permits_scope_replay(false));
        assert!(!origin.permits_scope_replay(true));
        let mut finite = origin.clone();
        if let SupervisedRootOutcomeInputOrigin::CurrentUser {
            requires_host_scope,
            ..
        } = &mut finite
        {
            *requires_host_scope = Some(true);
        }
        assert!(!finite.permits_scope_replay(false));
        assert!(finite.permits_scope_replay(true));
        let mut default_current_user = finite.clone();
        if let SupervisedRootOutcomeInputOrigin::CurrentUser {
            requires_host_scope,
            ..
        } = &mut default_current_user
        {
            *requires_host_scope = Some(false);
        }
        assert!(default_current_user.permits_scope_replay(false));
        assert!(default_current_user.permits_scope_replay(true));
        assert!(origin.same_operation(&finite));
        assert!(finite.same_operation(&default_current_user));
        let owned_input = SupervisedRootOutcomeInput {
            receipt_id: receipt.clone(),
            generation: 1,
            supervisor_origin: Some(origin.clone()),
        };
        let owner = fence(1);
        let mut facts = SupervisedRootOutcomes::new(&session.id, session.created_at).unwrap();
        facts.reserve(&owner, None).unwrap();
        facts.record_input(&owner, owned_input.clone()).unwrap();

        let mut envelope = SessionMessageEnvelope::user_input(&session.id, "instruction");
        envelope.id = receipt.clone();
        envelope.source = crate::SessionMessageSource::Session {
            session_id: supervisor.session_id.clone(),
        };
        envelope.kind = SessionMessageKind::PeerMessage;
        envelope.correlation_id = Some(operation.to_string());
        let intent =
            SessionInboxActivationIntent::new(SessionActivationPolicy::RespectSpecificWait)
                .with_supervised_root_origin(origin.clone())
                .unwrap();
        let mut message = envelope.to_provider_message().unwrap();
        message
            .metadata
            .get_or_insert_with(|| serde_json::json!({}))
            .as_object_mut()
            .unwrap()
            .insert(
                OWNED_INPUT_CHECKPOINT_KEY.into(),
                serde_json::json!({
                    "version":1,
                    "envelope_id":receipt.as_str(),
                    "target_session_id":session.id,
                    "session_created_at":session.created_at,
                    "generation":1,
                    "activation_intent":intent,
                    "activation_policy":SessionActivationPolicy::RespectSpecificWait,
                }),
            );
        session.add_message(message);
        session
            .runtime_metadata
            .get_or_insert_with(Default::default)
            .supervised_root_outcomes = Some(facts.clone());
        let projected = facts.project_inputs_from_main(&session, 1).unwrap();
        assert_eq!(
            projected[0].recorded_supervisor_operation_id,
            Some(operation)
        );
        let mut no_checkpoint = session.clone();
        no_checkpoint.messages[0]
            .metadata
            .as_mut()
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove(OWNED_INPUT_CHECKPOINT_KEY);
        assert!(facts.project_inputs_from_main(&no_checkpoint, 1).is_err());
        let mut changed_checkpoint = session.clone();
        let mut changed_origin = origin.clone();
        if let SupervisedRootOutcomeInputOrigin::CurrentUser {
            supervisor_created_at,
            ..
        } = &mut changed_origin
        {
            *supervisor_created_at += chrono::Duration::seconds(1);
        }
        let changed_intent =
            SessionInboxActivationIntent::new(SessionActivationPolicy::RespectSpecificWait)
                .with_supervised_root_origin(changed_origin)
                .unwrap();
        changed_checkpoint.messages[0].metadata.as_mut().unwrap()[OWNED_INPUT_CHECKPOINT_KEY]
            ["activation_intent"] = serde_json::to_value(changed_intent).unwrap();
        assert!(facts
            .project_inputs_from_main(&changed_checkpoint, 1)
            .is_err());
        let mut wrong_sender = session.clone();
        envelope.source = crate::SessionMessageSource::Session {
            session_id: "other-session".into(),
        };
        let checkpoint =
            wrong_sender.messages[0].metadata.as_ref().unwrap()[OWNED_INPUT_CHECKPOINT_KEY].clone();
        let mut replacement = envelope.to_provider_message().unwrap();
        replacement
            .metadata
            .as_mut()
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert(OWNED_INPUT_CHECKPOINT_KEY.into(), checkpoint);
        wrong_sender.messages[0] = replacement;
        assert!(facts.project_inputs_from_main(&wrong_sender, 1).is_err());

        let mut wrong_birth = facts.clone();
        if let Some(SupervisedRootOutcomeInputOrigin::CurrentUser { target, .. }) =
            &mut wrong_birth.facts[0].inputs[0].supervisor_origin
        {
            target.created_at += chrono::Duration::seconds(1);
        }
        assert!(wrong_birth.validate().is_err());

        let mut unknown = SupervisedRootOutcomes::new(&session.id, session.created_at).unwrap();
        unknown.reserve(&owner, None).unwrap();
        let mut unproven = owned_input.clone();
        unproven.supervisor_origin = None;
        unknown.record_input(&owner, unproven).unwrap();
        let before = unknown.clone();
        assert!(unknown.record_input(&owner, owned_input.clone()).is_err());
        assert_eq!(
            unknown, before,
            "a retry must not manufacture historical authority"
        );
        unknown
            .finalize(&owner, SupervisedRootOutcomeResult::Completed)
            .unwrap();
        let successor = fence(2);
        let mut changed_reset = unknown.clone();
        changed_reset
            .reserve(&successor, Some(&"a".repeat(64)))
            .unwrap();
        let before_reset = changed_reset.clone();
        assert!(changed_reset
            .record_input(&successor, owned_input.clone())
            .is_err());
        assert_eq!(
            changed_reset, before_reset,
            "reset must not upgrade an old receipt"
        );
        unknown.reserve(&successor, None).unwrap();
        let before = unknown.clone();
        assert!(unknown.record_input(&successor, owned_input).is_err());
        assert_eq!(
            unknown, before,
            "a successor must not upgrade an old receipt either"
        );
    }

    #[test]
    fn legacy_origin_shape_rejects_a_different_target_birth() {
        let birth = Utc::now();
        let supervisor = SupervisorReference {
            session_id: crate::DEFAULT_SUPERVISOR_SESSION_ID.into(),
            incarnation_id: uuid::Uuid::new_v4(),
        };
        let operation = SessionMessageId::parse("operation-legacy").unwrap();
        let receipt =
            crate::supervisor_followup_message_id(&supervisor, "test-root", operation.as_str());
        let mut facts = SupervisedRootOutcomes::new("test-root", birth).unwrap();
        let owner = fence(1);
        facts.reserve(&owner, None).unwrap();
        facts
            .record_input(
                &owner,
                SupervisedRootOutcomeInput {
                    receipt_id: receipt,
                    generation: 1,
                    supervisor_origin: Some(
                        SupervisedRootOutcomeInputOrigin::LegacyOrCreatedRoot {
                            supervisor,
                            supervisor_created_at: birth,
                            caller: None,
                            requires_host_scope: None,
                            target_created_at: birth,
                            operation_id: operation,
                        },
                    ),
                },
            )
            .unwrap();
        let mut recreated = facts.clone();
        if let Some(SupervisedRootOutcomeInputOrigin::LegacyOrCreatedRoot {
            target_created_at,
            ..
        }) = &mut recreated.facts[0].inputs[0].supervisor_origin
        {
            *target_created_at += chrono::Duration::seconds(1);
        }
        assert!(recreated.validate().is_err());
    }

    #[test]
    fn outcome_question_and_terminal_fact_are_independent_and_bounded() {
        let mut facts = SupervisedRootOutcomes::new("test-root", Utc::now()).unwrap();
        let current = fence(1);
        facts.reserve(&current, None).unwrap();
        for index in 0..MAX_SUPERVISED_ROOT_OUTCOME_QUESTIONS {
            assert!(facts
                .record_question(&current, &occurrence(&index.to_string()))
                .unwrap());
        }
        let before = facts.clone();
        assert!(facts
            .record_question(&current, &occurrence("extra"))
            .is_err());
        assert_eq!(facts, before);
        facts
            .finalize(&current, SupervisedRootOutcomeResult::Cancelled)
            .unwrap();
        assert_eq!(
            facts.facts[0].questions.len(),
            MAX_SUPERVISED_ROOT_OUTCOME_QUESTIONS
        );
        assert_eq!(
            facts.facts[0].result,
            Some(SupervisedRootOutcomeResult::Cancelled)
        );
        assert!(!facts.record_question(&current, &occurrence("0")).unwrap());
    }

    #[test]
    fn outcome_questions_retain_the_input_prefix_at_their_own_boundary() {
        let mut facts = SupervisedRootOutcomes::new("test-root", Utc::now()).unwrap();
        let current = fence(1);
        facts.reserve(&current, None).unwrap();
        facts.record_input(&current, input("receipt-1", 1)).unwrap();
        let first = ResponseOccurrence {
            tool_call_id: "reused".into(),
            tool_result_message_id: "result-first".into(),
            permission_generation: None,
        };
        let second = ResponseOccurrence {
            tool_call_id: "reused".into(),
            tool_result_message_id: "result-second".into(),
            permission_generation: None,
        };
        facts.record_question(&current, &first).unwrap();
        facts.record_input(&current, input("receipt-2", 2)).unwrap();
        facts.record_question(&current, &second).unwrap();
        assert!(!facts.record_question(&current, &first).unwrap());
        assert_ne!(
            facts.facts[0].questions[0].question_id_sha256,
            facts.facts[0].questions[1].question_id_sha256
        );
        let mut new_permission_generation = first.clone();
        new_permission_generation.permission_generation = Some("next-generation".into());
        assert_ne!(
            supervised_root_question_occurrence_digest(&first),
            supervised_root_question_occurrence_digest(&new_permission_generation)
        );
        assert_eq!(facts.facts[0].questions[0].admitted_input_count, 1);
        assert_eq!(facts.facts[0].questions[1].admitted_input_count, 2);

        let mut beyond_inputs = facts.clone();
        beyond_inputs.facts[0].questions[1].admitted_input_count = 3;
        assert!(beyond_inputs.validate().is_err());
        let mut regressed = facts.clone();
        regressed.facts[0].questions[0].admitted_input_count = 2;
        regressed.facts[0].questions[1].admitted_input_count = 1;
        assert!(regressed.validate().is_err());
    }

    #[test]
    fn question_occurrence_hash_preserves_exact_ids_and_option_boundaries() {
        let left = ResponseOccurrence {
            tool_call_id: "ab".into(),
            tool_result_message_id: "c".into(),
            permission_generation: None,
        };
        let right = ResponseOccurrence {
            tool_call_id: "a".into(),
            tool_result_message_id: "bc".into(),
            permission_generation: None,
        };
        assert_ne!(
            supervised_root_question_occurrence_digest(&left),
            supervised_root_question_occurrence_digest(&right)
        );
        let mut empty_generation = left.clone();
        empty_generation.permission_generation = Some(String::new());
        assert_ne!(
            supervised_root_question_occurrence_digest(&left),
            supervised_root_question_occurrence_digest(&empty_generation)
        );
        let mut untrimmed = left.clone();
        untrimmed.tool_call_id = format!(" {} ", "a".repeat(300));
        assert_ne!(
            supervised_root_question_occurrence_digest(&left),
            supervised_root_question_occurrence_digest(&untrimmed)
        );
    }

    #[test]
    fn question_and_later_outcome_have_distinct_stable_event_positions() {
        let mut facts = SupervisedRootOutcomes::new("test-root", Utc::now()).unwrap();
        let first_run = fence(1);
        facts.reserve(&first_run, None).unwrap();
        facts
            .record_input(&first_run, input("receipt-1", 1))
            .unwrap();
        facts
            .record_question(&first_run, &occurrence("one"))
            .unwrap();
        let first = facts.next_event_after(None).unwrap().unwrap();
        assert_eq!(first.position.run_sequence, 1);
        assert_eq!(first.position.ordinal, 1);
        assert!(facts
            .next_event_after(Some(&first.position))
            .unwrap()
            .is_none());

        facts
            .record_input(&first_run, input("receipt-2", 2))
            .unwrap();
        facts
            .record_question(&first_run, &occurrence("two"))
            .unwrap();
        assert_eq!(facts.next_event_after(None).unwrap(), Some(first.clone()));
        let second = facts
            .next_event_after(Some(&first.position))
            .unwrap()
            .unwrap();
        assert_eq!(second.position.ordinal, 2);
        assert_ne!(first.kind, second.kind);
        facts
            .finalize(&first_run, SupervisedRootOutcomeResult::Suspended)
            .unwrap();
        let stopped = facts
            .next_event_after(Some(&second.position))
            .unwrap()
            .unwrap();
        assert_eq!(stopped.position.ordinal, 3);
        assert_eq!(
            stopped.kind,
            SupervisedRootOutcomeEventKind::ActivationOutcome(
                SupervisedRootOutcomeResult::Suspended,
            )
        );

        let next_run = fence(2);
        facts.reserve(&next_run, None).unwrap();
        facts
            .finalize(&next_run, SupervisedRootOutcomeResult::Completed)
            .unwrap();
        let next = facts
            .next_event_after(Some(&stopped.position))
            .unwrap()
            .unwrap();
        assert_eq!(next.position.run_sequence, 2);
        assert_eq!(next.position.ordinal, 1);
        assert!(facts
            .next_event_after(Some(&next.position))
            .unwrap()
            .is_none());
        let missing = SupervisedRootOutcomeEventPosition {
            run_sequence: 1,
            ordinal: 4,
        };
        assert!(facts.next_event_after(Some(&missing)).is_err());
    }

    #[test]
    fn question_input_projection_never_includes_a_later_admission() {
        let mut session = Session::new("test-root", "synthetic-model");
        let first = SessionMessageEnvelope::user_input(&session.id, "first input");
        let later = SessionMessageEnvelope::user_input(&session.id, "later input");
        session.add_message(first.to_provider_message().unwrap());
        session.add_message(later.to_provider_message().unwrap());
        let mut facts = SupervisedRootOutcomes::new(&session.id, session.created_at).unwrap();
        let current = fence(1);
        facts.reserve(&current, None).unwrap();
        facts
            .record_input(
                &current,
                SupervisedRootOutcomeInput {
                    receipt_id: first.id.clone(),
                    generation: 1,
                    supervisor_origin: None,
                },
            )
            .unwrap();
        facts.record_question(&current, &occurrence("one")).unwrap();
        facts
            .record_input(
                &current,
                SupervisedRootOutcomeInput {
                    receipt_id: later.id.clone(),
                    generation: 2,
                    supervisor_origin: None,
                },
            )
            .unwrap();
        session
            .runtime_metadata
            .get_or_insert_with(Default::default)
            .supervised_root_outcomes = Some(facts.clone());

        let question = facts
            .project_question_inputs_from_main(&session, 1, 0)
            .unwrap();
        assert_eq!(question.len(), 1);
        assert_eq!(question[0].receipt_id, first.id);
        assert_eq!(
            facts.project_inputs_from_main(&session, 1).unwrap().len(),
            2
        );
        assert!(facts
            .project_question_inputs_from_main(&session, 1, 1)
            .is_err());

        session
            .messages
            .retain(|message| message.id != later.id.as_str());
        assert_eq!(
            facts
                .project_question_inputs_from_main(&session, 1, 0)
                .unwrap(),
            question
        );
        assert!(facts.project_inputs_from_main(&session, 1).is_err());
    }

    #[test]
    fn outcome_full_ledger_rejects_input_before_consuming_terminal_headroom() {
        let mut facts = SupervisedRootOutcomes::new("test-root", Utc::now()).unwrap();
        let mut generation = 0_u64;
        // Construct a valid historical prefix to reach the byte boundary without
        // repeatedly serializing thousands of prior input transitions.
        for attempt in 1..=7 {
            let current = fence(attempt);
            let inputs = (0..MAX_SUPERVISED_ROOT_OUTCOME_INPUTS)
                .map(|_| {
                    generation += 1;
                    input(
                        &format!("receipt-{generation:08}{}", "x".repeat(230)),
                        generation,
                    )
                })
                .collect();
            facts.facts.push(SupervisedRootOutcomeFact {
                sequence: attempt,
                activation_id: current.activation_id,
                attempt,
                lease_epoch: current.lease_epoch,
                lease_owner_sha256: lease_owner_digest(&current.lease_owner),
                run_id: current.run_id,
                conversation_reset_sha256: None,
                inputs,
                questions: Vec::new(),
                result: Some(SupervisedRootOutcomeResult::Completed),
            });
        }
        facts.validate().unwrap();
        let current = fence(8);
        facts.reserve(&current, None).unwrap();
        let mut rejected = false;
        for _ in 0..MAX_SUPERVISED_ROOT_OUTCOME_INPUTS {
            generation += 1;
            let before = facts.clone();
            if facts
                .record_input(
                    &current,
                    input(
                        &format!("receipt-{generation:08}{}", "x".repeat(230)),
                        generation,
                    ),
                )
                .is_err()
            {
                assert_eq!(facts, before);
                rejected = true;
                break;
            }
        }
        assert!(rejected, "fixture must reach the encoded byte limit");
        for index in 0..MAX_SUPERVISED_ROOT_OUTCOME_QUESTIONS {
            facts
                .record_question(&current, &occurrence(&index.to_string()))
                .unwrap();
        }
        facts
            .finalize(&current, SupervisedRootOutcomeResult::Failed)
            .unwrap();
        assert!(serde_json::to_vec(&facts).unwrap().len() <= MAX_SUPERVISED_ROOT_OUTCOMES_BYTES);
    }

    #[test]
    fn outcome_reset_projection_is_bound_to_each_reservation() {
        let mut facts = SupervisedRootOutcomes::new("test-root", Utc::now()).unwrap();
        let first = fence(1);
        facts.reserve(&first, None).unwrap();
        facts.record_input(&first, input("receipt-1", 1)).unwrap();
        facts
            .finalize(&first, SupervisedRootOutcomeResult::Completed)
            .unwrap();
        let second = fence(2);
        let reset = "a".repeat(64);
        facts.reserve(&second, Some(&reset)).unwrap();
        // Conversation reset does not reset the Root Inbox's lifetime-wide
        // generation. A different receipt cannot reuse the earlier generation.
        let before = facts.clone();
        assert!(facts.record_input(&second, input("receipt-2", 1)).is_err());
        assert_eq!(facts, before);
        facts.record_input(&second, input("receipt-2", 2)).unwrap();
        let before = facts.clone();
        assert!(facts.record_input(&second, input("receipt-1", 3)).is_err());
        assert_eq!(facts, before);
        assert!(facts.reserve(&second, None).is_err());
        assert!(facts.reserve(&second, Some("invalid")).is_err());
        let saved = serde_json::to_vec(&facts).unwrap();
        let reopened: SupervisedRootOutcomes = serde_json::from_slice(&saved).unwrap();
        reopened.validate().unwrap();
        assert_eq!(reopened, facts);
    }

    #[test]
    fn interrupted_predecessor_requires_distinct_successor_and_never_invents_completion() {
        let mut facts = SupervisedRootOutcomes::new("test-root", Utc::now()).unwrap();
        let first = fence(1);
        facts.reserve(&first, None).unwrap();
        let before = facts.clone();
        assert!(facts.recover_predecessor_and_reserve(&first, None).is_err());
        assert!(facts
            .finalize(&first, SupervisedRootOutcomeResult::InterruptedUnknown)
            .is_err());
        assert!(facts
            .recover_predecessor_and_reserve(&fence(2), Some(&"a".repeat(64)))
            .is_err());
        let mut collision = fence(2);
        collision.run_id = first.run_id.clone();
        assert!(facts
            .recover_predecessor_and_reserve(&collision, None)
            .is_err());
        collision = fence(2);
        collision.activation_id = first.activation_id.clone();
        assert!(facts
            .recover_predecessor_and_reserve(&collision, None)
            .is_err());
        assert_eq!(facts, before);
        let second = fence(2);
        assert_eq!(
            facts
                .recover_predecessor_and_reserve(&second, None)
                .unwrap(),
            SupervisedRootOutcomeReservation::New(2)
        );
        assert_eq!(
            facts.facts[0].result,
            Some(SupervisedRootOutcomeResult::InterruptedUnknown)
        );
        assert_eq!(
            facts
                .recover_predecessor_and_reserve(&second, None)
                .unwrap(),
            SupervisedRootOutcomeReservation::Existing {
                sequence: 2,
                finalized: false
            }
        );
        facts
            .finalize(&second, SupervisedRootOutcomeResult::Failed)
            .unwrap();
        assert_eq!(
            facts
                .recover_predecessor_and_reserve(&second, None)
                .unwrap(),
            SupervisedRootOutcomeReservation::Existing {
                sequence: 2,
                finalized: true
            }
        );
        let mut different_owner = second.clone();
        different_owner.lease_owner = "different-owner".into();
        assert!(facts
            .recover_predecessor_and_reserve(&different_owner, None)
            .is_err());
    }

    #[test]
    fn recovery_never_replaces_a_known_terminal_or_suspension() {
        for result in [
            SupervisedRootOutcomeResult::Completed,
            SupervisedRootOutcomeResult::Failed,
            SupervisedRootOutcomeResult::Cancelled,
            SupervisedRootOutcomeResult::Suspended,
        ] {
            let mut facts = SupervisedRootOutcomes::new("test-root", Utc::now()).unwrap();
            let first = fence(1);
            facts.reserve(&first, None).unwrap();
            facts.finalize(&first, result).unwrap();
            let before = facts.clone();
            assert!(facts
                .recover_predecessor_and_reserve(&fence(2), None)
                .is_err());
            assert_eq!(facts, before);
        }
    }

    #[test]
    fn input_projection_requires_matching_main_receipt_and_omits_correlation_text() {
        let mut session = Session::new("test-root", "synthetic-model");
        let mut envelope = SessionMessageEnvelope::user_input(&session.id, "private input");
        // Exercise the PeerMessage shape used by Supervisor followups. This
        // fixture does not itself prove sender authorization.
        envelope.source = crate::SessionMessageSource::Session {
            session_id: "synthetic-supervisor".into(),
        };
        envelope.kind = crate::SessionMessageKind::PeerMessage;
        envelope.correlation_id = Some("request/a".into());
        session.add_message(envelope.to_provider_message().unwrap());
        let mut facts = SupervisedRootOutcomes::new(&session.id, session.created_at).unwrap();
        let owner = fence(1);
        facts.reserve(&owner, None).unwrap();
        facts
            .record_input(
                &owner,
                SupervisedRootOutcomeInput {
                    receipt_id: envelope.id.clone(),
                    generation: 7,
                    supervisor_origin: None,
                },
            )
            .unwrap();
        session
            .runtime_metadata
            .get_or_insert_with(Default::default)
            .supervised_root_outcomes = Some(facts.clone());
        let projection = facts.project_inputs_from_main(&session, 1).unwrap();
        assert_eq!(projection.len(), 1);
        assert_eq!(projection[0].receipt_id, envelope.id);
        assert_eq!(projection[0].generation, 7);
        assert_eq!(
            projection[0].correlation_id_sha256.as_deref(),
            Some("08705077fcd5163bbd2296b98d4f75066deacedb8047f4ab31c4e8f92ef990a4")
        );
        assert!(!format!("{projection:?}").contains("request/a"));
        assert!(!format!("{projection:?}").contains("private input"));

        let mut owned = session.clone();
        owned.messages[0].metadata.as_mut().unwrap().as_object_mut().unwrap().insert(
            OWNED_INPUT_CHECKPOINT_KEY.into(), serde_json::json!({
                "version": 1,
                "envelope_id": envelope.id.as_str(),
                "target_session_id": &session.id,
                "session_created_at": session.created_at,
                "generation": 7,
                "activation_intent": SessionInboxActivationIntent::for_expected_run("run-1").unwrap(),
                "activation_policy": SessionActivationPolicy::RespectSpecificWait,
            }));
        assert_eq!(
            facts.project_inputs_from_main(&owned, 1).unwrap(),
            projection
        );
        let mut promoted = owned.clone();
        promoted.messages[0].metadata.as_mut().unwrap()[OWNED_INPUT_CHECKPOINT_KEY]
            ["activation_intent"] = serde_json::json!(SessionInboxActivationIntent::new(
            SessionActivationPolicy::RespectSpecificWait
        ));
        promoted.messages[0].metadata.as_mut().unwrap()[OWNED_INPUT_CHECKPOINT_KEY]
            ["activation_policy"] =
            serde_json::json!(SessionActivationPolicy::InterruptSpecificWait);
        assert_eq!(
            facts.project_inputs_from_main(&promoted, 1).unwrap(),
            projection
        );
        for (key, value) in [
            ("version", serde_json::json!(2)),
            ("generation", serde_json::json!(8)),
            ("target_session_id", serde_json::json!("other-root")),
            (
                "session_created_at",
                serde_json::json!(session.created_at + chrono::Duration::seconds(1)),
            ),
            (
                "activation_intent",
                serde_json::json!(
                    SessionInboxActivationIntent::for_expected_run("other-run").unwrap()
                ),
            ),
            (
                "activation_policy",
                serde_json::json!(SessionActivationPolicy::InterruptSpecificWait),
            ),
        ] {
            let mut bad = owned.clone();
            bad.messages[0].metadata.as_mut().unwrap()[OWNED_INPUT_CHECKPOINT_KEY][key] = value;
            assert!(facts.project_inputs_from_main(&bad, 1).is_err(), "{key}");
        }
        let mut extra = owned.clone();
        extra.messages[0].metadata.as_mut().unwrap()[OWNED_INPUT_CHECKPOINT_KEY]["unexpected"] =
            serde_json::json!(true);
        assert!(facts.project_inputs_from_main(&extra, 1).is_err());

        let mut altered = session.clone();
        altered.messages[0].content = "different input".into();
        assert!(facts.project_inputs_from_main(&altered, 1).is_err());
        let mut altered_marker = session.clone();
        altered_marker.messages[0].metadata.as_mut().unwrap()["session_message"]
            ["correlation_id"] = serde_json::json!("other");
        assert!(facts.project_inputs_from_main(&altered_marker, 1).is_err());
        let mut duplicate = session.clone();
        duplicate.messages.push(duplicate.messages[0].clone());
        assert!(facts.project_inputs_from_main(&duplicate, 1).is_err());
        let mut recreated = session.clone();
        recreated.created_at += chrono::Duration::seconds(1);
        assert!(facts.project_inputs_from_main(&recreated, 1).is_err());
        let mut fake_facts = facts.clone();
        fake_facts.facts[0].inputs[0].generation = 8;
        assert!(fake_facts.project_inputs_from_main(&session, 1).is_err());
        let mut missing_facts = session.clone();
        missing_facts
            .runtime_metadata
            .as_mut()
            .unwrap()
            .supervised_root_outcomes = None;
        assert!(facts.project_inputs_from_main(&missing_facts, 1).is_err());
        assert!(facts.project_inputs_from_main(&session, 2).is_err());
    }
}
