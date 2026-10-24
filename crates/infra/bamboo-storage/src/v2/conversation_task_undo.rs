//! Bounded original-target evidence for new Task undo journals. This is data,
//! not permission to roll back. Existing physical guards and recovery checks
//! still own the operation; legacy journals never acquire a nonzero generation.

use super::*;
use bamboo_domain::ConversationResetState;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TaskConversationWitness {
    birth_token: String,
    kind: SessionKind,
    root_session_id: String,
    parent_session_id: Option<String>,
    parent_created_at: Option<DateTime<Utc>>,
    spawn_depth: u32,
    project_id: Option<String>,
    conversation_reset: Option<ConversationResetState>,
}

fn conflict(reason: impl std::fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        bamboo_domain::SessionAuthorityConflict(format!(
            "Task undo conversation changed: {reason}"
        )),
    )
}

impl TaskConversationWitness {
    pub(super) fn capture(session: &Session) -> io::Result<Self> {
        session.validate_conversation_state().map_err(conflict)?;
        Ok(Self {
            birth_token: session.root_mode_birth_token(),
            kind: session.kind,
            root_session_id: if session.kind == SessionKind::Root
                && session.root_session_id.is_empty()
            {
                session.id.clone()
            } else {
                session.root_session_id.clone()
            },
            parent_session_id: session.parent_session_id.clone(),
            parent_created_at: session.parent_created_at,
            spawn_depth: session.spawn_depth,
            project_id: session.project_id_meta(),
            conversation_reset: session.conversation_reset.clone(),
        })
    }

    pub(super) fn check_current(&self, current: &Session) -> io::Result<()> {
        if self != &Self::capture(current)? {
            return Err(conflict(
                "source birth, scope or generation is no longer current",
            ));
        }
        Ok(())
    }
}

/// Version 1 had no source witness. Preserve its existing zero-state recovery
/// only, without inventing birth evidence; it can never act on a reset Session.
pub(super) fn check_legacy_zero_target(current: &Session) -> io::Result<()> {
    current.validate_conversation_state().map_err(conflict)?;
    if current.conversation_reset.is_some() {
        return Err(conflict("legacy undo has no nonzero conversation witness"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::{ConversationResetReceipt, ConversationResetState};

    fn nonzero(source: &Session) -> Session {
        let mut session = source.clone();
        session.conversation_reset = Some(ConversationResetState {
            schema_version: 1,
            conversation_revision: 1,
            receipts: vec![ConversationResetReceipt {
                operation_id: format!("0:{}", Uuid::new_v4()),
                birth_token: source.root_mode_birth_token(),
                expected_snapshot_sha256: "a".repeat(64),
                previous_revision: 0,
                conversation_revision: 1,
                retained_system_messages: 0,
                reset_at: source.updated_at,
            }],
        });
        session
    }

    #[test]
    fn task_witness_ignores_unrelated_runtime_but_refuses_reset_or_rebirth() {
        let source = Session::new("task-witness", "model");
        let witness = TaskConversationWitness::capture(&source).unwrap();
        let mut current = source.clone();
        current.pinned = true;
        current.metadata_version += 1;
        current.add_message(Message::user("unrelated runtime append"));
        assert!(witness.check_current(&current).is_ok());
        assert!(witness.check_current(&nonzero(&source)).is_err());
        current.created_at += chrono::Duration::seconds(1);
        assert!(witness.check_current(&current).is_err());
    }

    #[test]
    fn task_witness_compares_receipts_and_scope_and_contains_no_history() {
        let mut source = nonzero(&Session::new("task-witness-receipts", "model"));
        source.add_message(Message::user("private history must be absent"));
        let witness = TaskConversationWitness::capture(&source).unwrap();
        let serialized = serde_json::to_vec(&witness).unwrap();
        assert!(!String::from_utf8_lossy(&serialized).contains("private history"));
        let decoded: TaskConversationWitness = serde_json::from_slice(&serialized).unwrap();
        assert!(decoded.check_current(&source).is_ok());
        let mut changed = source.clone();
        changed.conversation_reset.as_mut().unwrap().receipts[0].expected_snapshot_sha256 =
            "b".repeat(64);
        assert!(decoded.check_current(&changed).is_err());
        let mut changed = source;
        changed.set_project_id_meta("new-project");
        assert!(decoded.check_current(&changed).is_err());
    }

    #[test]
    fn legacy_undo_is_zero_only_and_invalid_state_stays_unavailable() {
        let source = Session::new("legacy-task-undo", "model");
        assert!(check_legacy_zero_target(&source).is_ok());
        assert!(check_legacy_zero_target(&nonzero(&source)).is_err());
        let mut invalid = source;
        invalid.conversation_reset = Some(Default::default());
        assert!(check_legacy_zero_target(&invalid).is_err());
        assert!(TaskConversationWitness::capture(&invalid).is_err());
    }
}
