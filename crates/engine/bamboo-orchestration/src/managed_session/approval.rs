//! Project the exact parked permission request of an execution Session.

use bamboo_domain::{
    pending_permission_occurrence, supervised_root_question_occurrence_digest, ResponseOccurrence,
    Session, SessionKind, SessionMessageContent, SessionProviderMessage, SupervisedRootOutcomeFact,
    SupervisedRootOutcomeResult, SupervisorReference,
};
use serde_json::{json, Value};

const SUPERVISOR_CREATED: &str = "supervisor_created";
pub const MAX_APPROVAL_CANDIDATES: usize = 24;
pub const MAX_APPROVAL_CARDS: usize = 8;
const MAX_LEGACY_CANDIDATES: usize = 4;

pub fn legacy_candidate_limit(current: usize) -> usize {
    MAX_LEGACY_CANDIDATES.min(MAX_APPROVAL_CANDIDATES.saturating_sub(current))
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ApprovalCandidate {
    Managed,
    Legacy,
}

/// Index entries only select bounded reads. The current Session owns the proof.
pub fn classify_candidate(
    kind: SessionKind,
    has_pending_question: bool,
    session_origin: Option<&str>,
    pending_material_run: Option<bool>,
    has_material_hint: bool,
) -> Option<ApprovalCandidate> {
    if kind != SessionKind::Root || !has_pending_question {
        return None;
    }
    if session_origin == Some(SUPERVISOR_CREATED)
        || pending_material_run.unwrap_or(has_material_hint)
    {
        Some(ApprovalCandidate::Managed)
    } else if pending_material_run.is_none() {
        Some(ApprovalCandidate::Legacy)
    } else {
        None
    }
}

pub struct PendingApproval {
    pub occurrence: ResponseOccurrence,
}

impl PendingApproval {
    pub fn capture(supervisor: &SupervisorReference, target: &Session) -> Option<Self> {
        if target.kind != SessionKind::Root
            || target.id == supervisor.session_id
            || !target.authority_identity.is_ordinary()
            || target.validate_conversation_state().is_err()
        {
            return None;
        }
        Some(Self {
            occurrence: pending_permission_occurrence(target)?,
        })
    }

    /// A user-opened Session only qualifies through an explicitly subscribed
    /// run that owns this exact pending occurrence.
    pub fn is_managed(&self, supervisor: &SupervisorReference, target: &Session) -> bool {
        if target.metadata.get("session.origin.v1").map(String::as_str) == Some(SUPERVISOR_CREATED)
        {
            return true;
        }
        let Some(outcomes) = target
            .runtime_metadata
            .as_ref()
            .and_then(|metadata| metadata.supervised_root_outcomes.as_ref())
        else {
            return false;
        };
        let Some(fact) = outcomes.facts.last() else {
            return false;
        };
        self.matches_request(fact)
            && outcomes.material_inputs(fact).any(|input| {
                input.supervisor_origin.as_ref().is_some_and(|origin| {
                    origin.supervisor() == supervisor
                        && origin
                            .current_user_target()
                            .is_some_and(|bound| bound.matches(target))
                })
            })
    }

    pub fn observation(&self, supervisor: &SupervisorReference, target: &Session) -> Value {
        let encoded_id =
            url::form_urlencoded::byte_serialize(target.id.as_bytes()).collect::<String>();
        json!({
            "status": "awaiting_response",
            "target_session_id": target.id,
            "title": target.title,
            "link": format!("#session={encoded_id}"),
            "completion_verified": false,
            "tool_name": target.pending_question.as_ref().map(|question| &question.tool_name),
            "approval_card": self.is_managed(supervisor, target),
        })
    }

    fn matches_request(&self, fact: &SupervisedRootOutcomeFact) -> bool {
        fact.questions.last().is_some_and(|question| {
            question.question_id_sha256
                == supervised_root_question_occurrence_digest(&self.occurrence)
        })
    }
}

/// Only the provider presentation changes; the canonical Inbox receipt and
/// its replay identity still belong to the underlying outcome.
pub(super) fn project_approval_notice(
    supervisor: &SupervisorReference,
    target: &Session,
    fact: &SupervisedRootOutcomeFact,
) -> Option<SessionProviderMessage> {
    if fact.result != Some(SupervisedRootOutcomeResult::Suspended) {
        return None;
    }
    let pending = PendingApproval::capture(supervisor, target)?;
    if !pending.matches_request(fact) || !pending.is_managed(supervisor, target) {
        return None;
    }
    Some(SessionProviderMessage {
        content: SessionMessageContent::text(format!(
            concat!(
                "# Managed Session approval required\n\n{}\n\n",
                "The Supervisor chat displays the approval card with the actual operation. ",
                "Let the card collect the user's decision and advance any independent work."
            ),
            pending.observation(supervisor, target)
        )),
        metadata: Default::default(),
        never_compress: false,
    })
}
