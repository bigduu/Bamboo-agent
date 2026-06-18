//! Durable parent-side record of child sub-agent approval requests delegated up.
//!
//! Phase 2 of the sub-agent roadmap (child→parent approval delegation): a
//! non-bypassed child that hits a gated tool cannot answer its own permission
//! prompt (no human is attached to a child session). Instead the request is
//! delegated UP to the parent — which surfaces it to the human via the parent's
//! existing pending-question / notification / `/respond` machinery — and the
//! decision is routed back DOWN to resume the waiting child.
//!
//! This module owns the durable mapping the parent needs to route an answer back
//! to the right child + tool call. Like [`crate::runtime::guardian_state`] it
//! lives in `session.metadata` under [`APPROVAL_DELEGATION_METADATA_KEY`] as a
//! single JSON blob, so it round-trips through the normal session save/load path
//! with no new storage entity. It is stored on the PARENT session.

use bamboo_agent_core::Session;
use chrono::Utc;
use serde::{Deserialize, Serialize};

/// Parent-session metadata key holding the serialized [`ApprovalDelegationState`].
pub const APPROVAL_DELEGATION_METADATA_KEY: &str = "approval.delegation.state";

/// Upper bound on retained pending approvals, so a runaway fan-out of children
/// cannot grow the persisted blob without limit. Oldest entries are dropped.
const MAX_PENDING: usize = 64;

/// One child approval request awaiting the parent's (human's) decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingChildApproval {
    /// The child session that is suspended waiting for this decision.
    pub child_session_id: String,
    /// The gated tool call on the child to re-execute once approved.
    pub child_tool_call_id: String,
    /// The gated tool name (for the surfaced question + diagnostics).
    pub tool_name: String,
    /// Permission type as a string (e.g. "WriteFile", "ExecuteCommand").
    pub permission_type: String,
    /// The concrete resource the permission applies to (path, command, …).
    pub resource: String,
    /// The synthetic pending-question id surfaced on the PARENT for this request
    /// — the key the parent's `/respond` answer is correlated back through.
    pub parent_pending_question_id: String,
    pub registered_at: String,
}

/// Durable parent-side set of in-flight child approval delegations.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ApprovalDelegationState {
    #[serde(default)]
    pub pending: Vec<PendingChildApproval>,
}

impl ApprovalDelegationState {
    /// Record a new pending approval (deduped on `parent_pending_question_id`),
    /// trimming the oldest if the cap is exceeded.
    pub fn upsert(&mut self, request: PendingChildApproval) {
        self.pending
            .retain(|p| p.parent_pending_question_id != request.parent_pending_question_id);
        self.pending.push(request);
        if self.pending.len() > MAX_PENDING {
            let overflow = self.pending.len() - MAX_PENDING;
            self.pending.drain(0..overflow);
        }
    }

    /// Remove and return the pending approval surfaced under `question_id`.
    pub fn take_by_question(&mut self, question_id: &str) -> Option<PendingChildApproval> {
        let idx = self
            .pending
            .iter()
            .position(|p| p.parent_pending_question_id == question_id)?;
        Some(self.pending.remove(idx))
    }

    /// Remove and return the pending approval for `child_session_id`, if any.
    pub fn take_by_child(&mut self, child_session_id: &str) -> Option<PendingChildApproval> {
        let idx = self
            .pending
            .iter()
            .position(|p| p.child_session_id == child_session_id)?;
        Some(self.pending.remove(idx))
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

/// Read the persisted parent-side delegation state, if present and parseable.
pub fn read_approval_delegation_state(session: &Session) -> Option<ApprovalDelegationState> {
    let raw = session.metadata.get(APPROVAL_DELEGATION_METADATA_KEY)?;
    serde_json::from_str::<ApprovalDelegationState>(raw).ok()
}

/// Read the existing delegation state, or a fresh empty one.
pub fn ensure_approval_delegation_state(session: &Session) -> ApprovalDelegationState {
    read_approval_delegation_state(session).unwrap_or_default()
}

/// Persist the delegation state into the parent `session.metadata`. An empty
/// state is removed (keeps metadata clean once all approvals resolve).
pub fn write_approval_delegation_state(session: &mut Session, state: ApprovalDelegationState) {
    if state.is_empty() {
        session.metadata.remove(APPROVAL_DELEGATION_METADATA_KEY);
        return;
    }
    match serde_json::to_string(&state) {
        Ok(json) => {
            session
                .metadata
                .insert(APPROVAL_DELEGATION_METADATA_KEY.to_string(), json);
        }
        Err(error) => {
            tracing::warn!(
                "failed to serialize approval delegation state for session {}: {error}",
                session.id
            );
        }
    }
}

/// Build a [`PendingChildApproval`] stamped with the current time.
pub fn new_pending_child_approval(
    child_session_id: impl Into<String>,
    child_tool_call_id: impl Into<String>,
    tool_name: impl Into<String>,
    permission_type: impl Into<String>,
    resource: impl Into<String>,
    parent_pending_question_id: impl Into<String>,
) -> PendingChildApproval {
    PendingChildApproval {
        child_session_id: child_session_id.into(),
        child_tool_call_id: child_tool_call_id.into(),
        tool_name: tool_name.into(),
        permission_type: permission_type.into(),
        resource: resource.into(),
        parent_pending_question_id: parent_pending_question_id.into(),
        registered_at: Utc::now().to_rfc3339(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::Session;

    fn req(child: &str, question: &str) -> PendingChildApproval {
        new_pending_child_approval(child, "tc-1", "Write", "WriteFile", "/tmp/x", question)
    }

    #[test]
    fn round_trips_through_parent_metadata() {
        let mut parent = Session::new("parent", "model");
        let mut state = ApprovalDelegationState::default();
        state.upsert(req("child-1", "q-1"));
        state.upsert(req("child-2", "q-2"));
        write_approval_delegation_state(&mut parent, state);

        let loaded = read_approval_delegation_state(&parent).expect("persists");
        assert_eq!(loaded.pending.len(), 2);
        assert_eq!(loaded.pending[0].child_session_id, "child-1");
        assert_eq!(loaded.pending[1].parent_pending_question_id, "q-2");
    }

    #[test]
    fn upsert_dedupes_on_question_id() {
        let mut state = ApprovalDelegationState::default();
        state.upsert(req("child-1", "q-1"));
        state.upsert(req("child-1b", "q-1")); // same question id → replaces
        assert_eq!(state.pending.len(), 1);
        assert_eq!(state.pending[0].child_session_id, "child-1b");
    }

    #[test]
    fn take_by_question_and_child() {
        let mut state = ApprovalDelegationState::default();
        state.upsert(req("child-1", "q-1"));
        state.upsert(req("child-2", "q-2"));
        let taken = state.take_by_question("q-1").expect("found by question");
        assert_eq!(taken.child_session_id, "child-1");
        assert_eq!(state.pending.len(), 1);
        let taken2 = state.take_by_child("child-2").expect("found by child");
        assert_eq!(taken2.parent_pending_question_id, "q-2");
        assert!(state.pending.is_empty());
    }

    #[test]
    fn empty_state_is_removed_from_metadata() {
        let mut parent = Session::new("parent", "model");
        let mut state = ApprovalDelegationState::default();
        state.upsert(req("child-1", "q-1"));
        write_approval_delegation_state(&mut parent, state);
        assert!(parent
            .metadata
            .contains_key(APPROVAL_DELEGATION_METADATA_KEY));

        // Resolving the only pending → empty → key removed.
        let mut state = ensure_approval_delegation_state(&parent);
        state.take_by_question("q-1");
        write_approval_delegation_state(&mut parent, state);
        assert!(!parent
            .metadata
            .contains_key(APPROVAL_DELEGATION_METADATA_KEY));
    }
}
