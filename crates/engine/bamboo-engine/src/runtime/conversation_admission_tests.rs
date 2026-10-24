//! Synthetic canonical read ports; no real provider, account or reset writer.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

fn next_conversation(source: &Session) -> Session {
    let mut current = source.clone();
    current.conversation_reset = Some(bamboo_domain::ConversationResetState {
        schema_version: 1,
        conversation_revision: 1,
        receipts: vec![bamboo_domain::ConversationResetReceipt {
            operation_id: format!("0:{}", uuid::Uuid::new_v4()),
            birth_token: source.root_mode_birth_token(),
            expected_snapshot_sha256: "a".repeat(64),
            previous_revision: 0,
            conversation_revision: 1,
            retained_system_messages: 0,
            reset_at: source.updated_at,
        }],
    });
    current
}

#[test]
fn canonical_readback_requires_original_ledger_and_nonzero_availability() {
    let zero = Session::new("admission-generation", "model");
    let current = next_conversation(&zero);
    let check = super::super::runner::state_bridge::check_conversation_readback;
    assert!(check(&zero, None).is_ok());
    assert!(check(&zero, Some(&zero)).is_ok());
    assert!(check(&zero, Some(&current)).is_err());
    assert!(check(&current, Some(&zero)).is_err());
    assert!(check(&current, None).is_err());
    let mut unrelated = current.clone();
    unrelated.add_message(Message::user("same conversation append"));
    assert!(check(&current, Some(&unrelated)).is_ok());
    unrelated.conversation_reset.as_mut().unwrap().receipts[0].expected_snapshot_sha256 =
        "b".repeat(64);
    assert!(check(&current, Some(&unrelated)).is_err());
    let mut invalid = zero;
    invalid.conversation_reset = Some(Default::default());
    assert!(check(&invalid, None).is_err());
}

struct CanonicalRead {
    canonical: Option<Session>,
    writes: AtomicUsize,
}

#[async_trait::async_trait]
impl bamboo_domain::RuntimeSessionPersistence for CanonicalRead {
    async fn save_runtime_session(&self, _: &mut Session) -> std::io::Result<()> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        Err(std::io::Error::other("unexpected write"))
    }
    async fn load_runtime_admission_snapshot(&self, _: &str) -> std::io::Result<Option<Session>> {
        Ok(self.canonical.clone())
    }
}

#[tokio::test]
async fn non_counted_public_admission_cannot_short_circuit_original_generation() {
    let zero = Session::new("non-counted-admission", "model");
    let current = next_conversation(&zero);
    for (mut original, canonical, allowed) in [
        (zero.clone(), Some(current.clone()), false),
        (current.clone(), Some(zero.clone()), false),
        (current.clone(), None, false),
        (current.clone(), Some(current), true),
        (zero, None, true),
    ] {
        let before = serde_json::to_value(&original).unwrap();
        assert!(!original.metadata.contains_key(LOOP_STATE_METADATA_KEY));
        let persistence = CanonicalRead {
            canonical,
            writes: AtomicUsize::new(0),
        };
        let raw = reject_ambiguous_reentry(&original, &persistence).await;
        let registered = admit_registered(&mut original, None, None, &persistence).await;
        assert_eq!(raw.is_ok(), allowed);
        assert_eq!(registered.is_ok(), allowed);
        if !allowed {
            assert!(raw
                .unwrap_err()
                .to_string()
                .contains("session_conversation_changed:"));
            assert!(registered
                .unwrap_err()
                .to_string()
                .contains("session_conversation_changed:"));
        }
        assert_eq!(serde_json::to_value(&original).unwrap(), before);
        assert_eq!(persistence.writes.load(Ordering::SeqCst), 0);
    }
}
