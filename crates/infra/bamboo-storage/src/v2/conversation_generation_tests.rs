//! Synthetic files in fresh TempDirs. The private seeder represents a future
//! producer; it is not a reset API, crash-recovery implementation or real data.

use super::*;
use bamboo_domain::{
    conversation_reset_snapshot_sha256, prepare_conversation_reset, ConversationResetPreparation,
    ConversationResetReadSafety, ConversationResetRequest, SessionAuthorityConflict,
};
use serde_json::Value;
use tempfile::TempDir;

struct Fixture {
    store: SessionStoreV2,
    home: TempDir,
    original: Session,
    canonical: Session,
}

fn proposed(source: &Session) -> Session {
    let request = ConversationResetRequest {
        session_id: source.id.clone(),
        operation_id: format!("0:{}", Uuid::new_v4()),
        expected_birth_token: source.root_mode_birth_token(),
        expected_conversation_revision: 0,
        expected_snapshot_sha256: conversation_reset_snapshot_sha256(source).unwrap(),
    };
    let ConversationResetPreparation::Prepared(prepared) = prepare_conversation_reset(
        source,
        &Default::default(),
        &request,
        ConversationResetReadSafety::IdleObserved,
        Utc::now(),
    )
    .unwrap() else {
        panic!("expected pure preparation of an unactivated synthetic Root")
    };
    let mut candidate = prepared.proposed_session;
    candidate.conversation_reset = Some(prepared.proposed_state);
    candidate
}

impl Fixture {
    async fn new() -> Self {
        let home = tempfile::tempdir().unwrap();
        let store = SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap();
        let mut original = Session::new("conversation-generation", "model");
        original.set_project_id_meta("synthetic-project");
        original.add_message(Message::system("retained system"));
        original.add_message(Message::user("old synthetic history"));
        store.save_session(&original).await.unwrap();
        let canonical = proposed(&original);
        // Bypass ordinary writers only inside this synthetic fixture, before
        // there is any concurrent actor. Production has no such seeding port.
        let directory = store.sessions_root_dir().join(&original.id);
        fs::write(
            directory.join("session.json"),
            compact_main::serialize_main(&canonical).unwrap(),
        )
        .await
        .unwrap();
        fs::write(
            directory.join(RUNTIME_SIDECAR_FILE),
            serde_json::to_vec_pretty(&runtime_sidecar_snapshot(&canonical)).unwrap(),
        )
        .await
        .unwrap();
        SessionStoreV2::write_staged_root_tool_proof(&directory, &canonical)
            .await
            .unwrap();
        assert!(store.load_session(&canonical.id).await.unwrap().is_some());
        Self {
            store,
            home,
            original,
            canonical,
        }
    }

    async fn files(&self) -> [Option<Vec<u8>>; 4] {
        let directory = self.store.sessions_root_dir().join(&self.original.id);
        [
            fs::read(directory.join("session.json")).await.ok(),
            fs::read(directory.join(RUNTIME_SIDECAR_FILE)).await.ok(),
            fs::read(directory.join(root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE))
                .await
                .ok(),
            fs::read(self.store.index_path()).await.ok(),
        ]
    }

    async fn finish(self) {
        self.store.flush_search_index().await;
        drop(self.store);
        self.home.close().unwrap();
    }
}

fn assert_conflict(error: io::Error) {
    assert!(
        error
            .get_ref()
            .is_some_and(|error| error.is::<SessionAuthorityConflict>()),
        "{error:?}"
    );
}

#[tokio::test]
async fn ordinary_writers_cannot_publish_the_first_nonzero_generation() {
    let home = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    let original = Session::new("first-generation", "model");
    store.save_session(&original).await.unwrap();
    let candidate = proposed(&original);
    let directory = store.sessions_root_dir().join(&original.id);
    let before = fs::read(directory.join("session.json")).await.unwrap();
    assert_conflict(store.save_session(&candidate).await.unwrap_err());
    assert_conflict(store.save_runtime_state(&candidate).await.unwrap_err());
    assert_eq!(
        fs::read(directory.join("session.json")).await.unwrap(),
        before
    );

    let mut inherited = candidate;
    inherited.id = "new-with-receipts".into();
    inherited.root_session_id = inherited.id.clone();
    let birth = inherited.root_mode_birth_token();
    inherited.conversation_reset.as_mut().unwrap().receipts[0].birth_token = birth;
    assert!(inherited.validate_conversation_state().is_ok());
    assert_conflict(store.save_session(&inherited).await.unwrap_err());
    assert!(!store.sessions_root_dir().join(&inherited.id).exists());
    store.flush_search_index().await;
    drop(store);
    home.close().unwrap();
}

#[tokio::test]
async fn stale_zero_and_same_revision_receipt_changes_refuse_before_writes() {
    let fixture = Fixture::new().await;
    let before = fixture.files().await;
    let mut changed = fixture.canonical.clone();
    changed.conversation_reset.as_mut().unwrap().receipts[0].expected_snapshot_sha256 =
        "b".repeat(64);
    for stale in [&fixture.original, &changed] {
        assert_conflict(fixture.store.save_session(stale).await.unwrap_err());
        assert_eq!(fixture.files().await, before);
        assert_conflict(fixture.store.save_runtime_state(stale).await.unwrap_err());
        assert_eq!(fixture.files().await, before);
    }
    fixture.finish().await;
}

#[tokio::test]
async fn an_exact_generation_can_append_and_checkpoint_without_changing_its_ledger() {
    let fixture = Fixture::new().await;
    let mut fresh = fixture
        .store
        .load_session(&fixture.canonical.id)
        .await
        .unwrap()
        .unwrap();
    fresh.add_message(Message::user("fresh generation conversation"));
    fixture.store.save_session(&fresh).await.unwrap();
    let before = fixture.files().await;
    fresh.pinned = true;
    fixture.store.save_runtime_state(&fresh).await.unwrap();
    let after = fixture.files().await;
    assert_eq!(before[0], after[0], "runtime-only save rewrote Main");
    let loaded = fixture
        .store
        .load_session(&fresh.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        loaded.conversation_reset,
        fixture.canonical.conversation_reset
    );
    assert_eq!(loaded.messages.len(), 2);
    assert_eq!(loaded.messages[1].content, "fresh generation conversation");
    assert!(!serde_json::to_string(&loaded.messages)
        .unwrap()
        .contains("old synthetic history"));
    assert!(loaded.pinned);
    fixture.finish().await;
}

#[tokio::test]
async fn generation_uses_v3_bounded_proof_and_v3_compact_frame() {
    let fixture = Fixture::new().await;
    let files = fixture.files().await;
    let main = files[0].as_ref().unwrap();
    assert!(main.starts_with(compact_main::PREFIX_V3));
    let section_len = compact_main::section_length(&main[..compact_main::HEADER_BYTES]).unwrap();
    let projection =
        compact_main::decode_v1_section(&main[..section_len], compact_main::SECTION_CAP)
            .unwrap()
            .into_snapshot_session();
    assert_eq!(
        projection.conversation_reset,
        fixture.canonical.conversation_reset
    );
    assert!(projection.messages.is_empty());
    let proof_bytes = files[2].as_ref().unwrap();
    let proof: Value = serde_json::from_slice(proof_bytes).unwrap();
    assert_eq!(proof["version"], 3);
    assert_eq!(proof["conversation_reset"]["revision"], 1);
    assert_eq!(
        proof["conversation_reset"]["sha256"],
        fixture
            .canonical
            .conversation_reset_sha256()
            .unwrap()
            .unwrap()
    );
    assert!(proof_bytes.len() < 4096);
    assert!(proof["conversation_reset"].get("receipts").is_none());
    // A header-only old reader recognizes only v1/v2 and fails closed on v3.
    assert!(!main.starts_with(compact_main::PREFIX) && !main.starts_with(compact_main::PREFIX_V2));
    for prefix in [compact_main::PREFIX, compact_main::PREFIX_V2] {
        let mut forged = main.clone();
        forged[..prefix.len()].copy_from_slice(prefix);
        assert!(compact_main::validate_full_main(&forged).is_err());
    }
    let original = compact_main::serialize_main(&fixture.original).unwrap();
    assert!(original.starts_with(compact_main::PREFIX));
    let value: Value = serde_json::from_slice(&original).unwrap();
    assert!(value.get("conversation_reset").is_none());
    assert!(value[compact_main::MEMBER]["payload"]
        .get("conversation_reset")
        .is_none());
    fixture.finish().await;
}

#[tokio::test]
async fn a_downgraded_or_missing_root_proof_and_pair_mismatch_cannot_authorize_a_write() {
    for damage in ["downgrade", "missing", "sidecar"] {
        let fixture = Fixture::new().await;
        let directory = fixture.store.sessions_root_dir().join(&fixture.original.id);
        match damage {
            "downgrade" => {
                let path = directory.join(root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE);
                let mut proof: Value =
                    serde_json::from_slice(&fs::read(&path).await.unwrap()).unwrap();
                proof["version"] = serde_json::json!(2);
                fs::write(path, serde_json::to_vec(&proof).unwrap())
                    .await
                    .unwrap();
            }
            "missing" => {
                fs::remove_file(directory.join(root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE))
                    .await
                    .unwrap()
            }
            _ => fs::write(
                directory.join(RUNTIME_SIDECAR_FILE),
                serde_json::to_vec_pretty(&runtime_sidecar_snapshot(&fixture.original)).unwrap(),
            )
            .await
            .unwrap(),
        }
        let before = fixture.files().await;
        assert_conflict(
            fixture
                .store
                .save_session(&fixture.canonical)
                .await
                .unwrap_err(),
        );
        assert_eq!(fixture.files().await, before);
        assert_conflict(
            fixture
                .store
                .save_runtime_state(&fixture.canonical)
                .await
                .unwrap_err(),
        );
        assert_eq!(fixture.files().await, before);
        fixture.finish().await;
    }
}

#[tokio::test]
async fn legacy_clear_cannot_bypass_the_ledger_and_copy_is_an_independent_zero_root() {
    let fixture = Fixture::new().await;
    let before = fixture.files().await;
    assert_conflict(
        fixture
            .store
            .clear_session(&fixture.canonical.id)
            .await
            .unwrap_err(),
    );
    assert_eq!(fixture.files().await, before);
    let copied = fixture
        .store
        .copy_session(&fixture.canonical.id, "independent-copy")
        .await
        .unwrap()
        .unwrap();
    assert!(copied.conversation_reset.is_none());
    assert_ne!(
        copied.root_mode_birth_token(),
        fixture.canonical.root_mode_birth_token()
    );
    assert_eq!(fixture.files().await[0], before[0]);
    fixture.finish().await;
}

#[tokio::test]
async fn stale_merge_checkpoint_and_timestamp_newer_response_cache_do_not_publish() {
    let fixture = Fixture::new().await;
    let store = Arc::new(
        SessionStoreV2::new(fixture.home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let locked = crate::LockedSessionStore::new(store.clone());
    let mut stale = fixture.original.clone();
    stale.updated_at = fixture.canonical.updated_at + chrono::Duration::days(1);
    let before_stale = serde_json::to_value(&stale).unwrap();
    let before_files = fixture.files().await;
    let published = AtomicBool::new(false);
    assert_conflict(
        locked
            .merge_save_runtime_and_publish(&mut stale, |_, _| {
                published.store(true, Ordering::SeqCst);
            })
            .await
            .unwrap_err(),
    );
    assert!(!published.load(Ordering::SeqCst));
    assert_eq!(serde_json::to_value(&stale).unwrap(), before_stale);
    assert_conflict(
        locked
            .checkpoint_runtime_session(&mut stale)
            .await
            .unwrap_err(),
    );
    assert_eq!(serde_json::to_value(&stale).unwrap(), before_stale);
    assert_conflict(
        locked
            .inspect_runtime_session_for_response(&stale.id, || Some(stale.clone()))
            .await
            .unwrap_err(),
    );
    let expected = fixture.original.clone();
    let mut staged = expected.clone();
    staged.messages[0].content = "old-generation prompt rewrite".into();
    staged.metadata.remove("responses.previous_response_id");
    staged
        .reset_model_context_epoch(bamboo_domain::ModelContextResetReason::ExplicitHistoryRewrite);
    let before_staged = serde_json::to_value(&staged).unwrap();
    assert_conflict(
        locked
            .checkpoint_prompt_rewrite_and_publish(&expected, &mut staged, |_| {
                published.store(true, Ordering::SeqCst);
            })
            .await
            .unwrap_err(),
    );
    assert_eq!(
        serde_json::to_value(&staged).unwrap(),
        before_staged,
        "Rebased adopted stale context before refusing"
    );
    assert!(!published.load(Ordering::SeqCst));
    assert_eq!(fixture.files().await, before_files);
    store.flush_search_index().await;
    drop(locked);
    drop(store);
    fixture.finish().await;
}
