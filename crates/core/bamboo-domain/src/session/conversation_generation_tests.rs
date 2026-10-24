//! Synthetic domain fixtures only; no storage authority or reset execution.

use super::*;
use crate::{ConversationResetReceipt, ConversationResetState, Message};

fn at_revision(source: &Session, revision: u64) -> Session {
    let mut session = source.clone();
    session.conversation_reset = Some(ConversationResetState {
        schema_version: 1,
        conversation_revision: revision,
        receipts: vec![ConversationResetReceipt {
            operation_id: format!("{}:{}", revision - 1, uuid::Uuid::new_v4()),
            birth_token: source.root_mode_birth_token(),
            expected_snapshot_sha256: "a".repeat(64),
            previous_revision: revision - 1,
            conversation_revision: revision,
            retained_system_messages: 1,
            reset_at: source.created_at,
        }],
    });
    session
}

#[test]
fn absent_generation_is_the_only_original_zero_spelling() {
    let original = Session::new("generation-zero", "model");
    assert_eq!(original.conversation_revision(), 0);
    assert_eq!(original.conversation_reset_sha256().unwrap(), None);
    assert!(original.validate_conversation_state().is_ok());
    let raw = serde_json::to_value(&original).unwrap();
    assert!(raw.get("conversation_reset").is_none());
    let decoded: Session = serde_json::from_value(raw).unwrap();
    assert!(decoded.conversation_reset.is_none());

    let mut explicit_zero = original;
    explicit_zero.conversation_reset = Some(Default::default());
    assert_eq!(
        explicit_zero.validate_conversation_state(),
        Err(ConversationGenerationError::InvalidState)
    );
}

#[test]
fn exact_generation_preserves_the_full_ledger_not_just_the_counter() {
    let source = Session::new("generation-one", "model");
    let canonical = at_revision(&source, 1);
    let mut current = canonical.clone();
    current.add_message(Message::user("new conversation content"));
    current.metadata_version += 1;
    assert!(current.check_conversation_generation(&canonical).is_ok());
    assert_eq!(
        current.conversation_reset_sha256().unwrap(),
        canonical.conversation_reset_sha256().unwrap()
    );
    let raw = serde_json::to_vec(&canonical).unwrap();
    let roundtrip: Session = serde_json::from_slice(&raw).unwrap();
    assert_eq!(roundtrip.conversation_reset, canonical.conversation_reset);

    current.conversation_reset.as_mut().unwrap().receipts[0].expected_snapshot_sha256 =
        "b".repeat(64);
    assert!(current.validate_conversation_state().is_ok());
    assert_eq!(
        current.check_conversation_generation(&canonical),
        Err(ConversationGenerationError::StaleGeneration)
    );
    assert_ne!(
        current.conversation_reset_sha256().unwrap(),
        canonical.conversation_reset_sha256().unwrap()
    );
}

#[test]
fn stale_zero_newer_and_rollback_generations_cannot_be_adopted() {
    let source = Session::new("generation-stale", "model");
    let canonical = at_revision(&source, 1);
    for incoming in [source.clone(), at_revision(&source, 2)] {
        assert_eq!(
            incoming.check_conversation_generation(&canonical),
            Err(ConversationGenerationError::StaleGeneration)
        );
    }
    assert_eq!(
        canonical.check_conversation_generation(&source),
        Err(ConversationGenerationError::StaleGeneration)
    );
    assert!(source.check_conversation_generation(&source).is_ok());
}

#[test]
fn malformed_future_schema_and_other_lifetimes_fail_closed() {
    let source = Session::new("generation-birth", "model");
    let canonical = at_revision(&source, 1);
    let mut invalid = canonical.clone();
    invalid.conversation_reset.as_mut().unwrap().schema_version = 99;
    assert_eq!(
        invalid.conversation_reset_sha256(),
        Err(ConversationGenerationError::InvalidState)
    );
    let mut recreated = canonical.clone();
    recreated.created_at += chrono::Duration::seconds(1);
    assert!(recreated.validate_conversation_state().is_err());
    let mut other_id = canonical.clone();
    other_id.id = "different-id".into();
    other_id.root_session_id = other_id.id.clone();
    assert!(other_id.validate_conversation_state().is_err());
    // Even an otherwise valid independent ledger cannot authorize this birth.
    let other_source = Session::new("different-id", "model");
    let other = at_revision(&other_source, 1);
    assert_eq!(
        other.check_conversation_generation(&canonical),
        Err(ConversationGenerationError::LifetimeChanged)
    );
}

#[test]
fn a_child_or_new_root_never_inherits_the_parent_reset_ledger() {
    let parent = at_revision(&Session::new("generation-parent", "model"), 1);
    let mut child = Session::new_child_of("child", &parent, "model", "assignment");
    assert!(child.conversation_reset.is_none());
    assert!(Session::new("new-root", "model")
        .conversation_reset
        .is_none());
    child.conversation_reset = parent.conversation_reset.clone();
    assert_eq!(
        child.validate_conversation_state(),
        Err(ConversationGenerationError::InvalidState)
    );
}

#[test]
fn default_checkpoint_rejects_before_merging_or_saving_a_stale_transcript() {
    use crate::RuntimeSessionPersistence;
    use std::{
        future::Future,
        io,
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Poll, Waker},
    };

    struct Persister {
        canonical: Session,
        saves: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl RuntimeSessionPersistence for Persister {
        async fn save_runtime_session(&self, _session: &mut Session) -> io::Result<()> {
            self.saves.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn load_runtime_session(&self, _id: &str) -> io::Result<Option<Session>> {
            Ok(Some(self.canonical.clone()))
        }
    }

    let mut stale = Session::new("generation-checkpoint", "model");
    stale.add_message(Message::user("old runner-only message"));
    let mut canonical = at_revision(&stale, 1);
    canonical.messages = vec![Message::user("new canonical message")];
    let persister = Persister {
        canonical,
        saves: AtomicUsize::new(0),
    };
    let before = serde_json::to_value(&stale).unwrap();
    // These synthetic ports are immediately ready. Poll once without adding an
    // executor or runtime dependency to the domain crate.
    let error = {
        let mut future = std::pin::pin!(persister.checkpoint_runtime_session(&mut stale));
        let Poll::Ready(result) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("fixture persister unexpectedly suspended")
        };
        result.unwrap_err()
    };
    assert!(error
        .get_ref()
        .is_some_and(|error| error.is::<crate::SessionAuthorityConflict>()));
    assert_eq!(serde_json::to_value(&stale).unwrap(), before);
    assert_eq!(persister.saves.load(Ordering::SeqCst), 0);
}

#[test]
fn an_original_observation_cannot_refresh_itself_from_a_new_session_or_lease() {
    let mut caller = Session::new("generation-observation", "model");
    let observed = caller.conversation_generation_observation();
    let canonical = at_revision(&caller, 1);
    caller = canonical.clone();
    assert_eq!(
        observed.check_canonical(&caller),
        Err(ConversationGenerationError::StaleGeneration)
    );
    assert!(canonical
        .conversation_generation_observation()
        .check_canonical(&canonical)
        .is_ok());
    let mut invalid = canonical;
    invalid.conversation_reset.as_mut().unwrap().schema_version = 99;
    let observed_invalid = invalid.conversation_generation_observation();
    assert_eq!(
        observed_invalid.check_canonical(&invalid),
        Err(ConversationGenerationError::InvalidState)
    );
}
