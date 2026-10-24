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

async fn running_generation_root(fixture: &Fixture) -> bamboo_domain::ActorActivationFence {
    use bamboo_domain::ActorDirectoryPort;
    let now = Utc::now();
    let activation = fixture
        .store
        .claim_activation(&bamboo_domain::ActorActivationClaim {
            actor_id: fixture.canonical.id.clone(),
            run_id: "generation-run".into(),
            lease_owner: "generation-host".into(),
            lease_expires_at: now + chrono::Duration::minutes(5),
            inbox_generation: 0,
            placement_ref: None,
            now,
        })
        .await
        .unwrap();
    fixture
        .store
        .start_activation(&activation.fence(), now)
        .await
        .unwrap();
    activation.fence()
}

#[tokio::test]
async fn a_fresh_actor_lease_cannot_refresh_an_old_root_writer_observation() {
    let fixture = Fixture::new().await;
    let fence = running_generation_root(&fixture).await;
    let stale_owner = bamboo_domain::RootActorRuntimeWrite {
        fence: fence.clone(),
        created_at: fixture.original.created_at,
        conversation_generation: fixture.original.conversation_generation_observation(),
    };
    let directory = fixture.store.sessions_root_dir().join(&fixture.original.id);
    let before = fixture.files().await;
    assert_conflict(
        root_actor_runtime::RootActorWriteProof::new(directory.clone(), stale_owner)
            .validate()
            .unwrap_err(),
    );
    let current_owner = bamboo_domain::RootActorRuntimeWrite {
        fence,
        created_at: fixture.canonical.created_at,
        conversation_generation: fixture.canonical.conversation_generation_observation(),
    };
    let proof = root_actor_runtime::RootActorWriteProof::new(directory, current_owner);
    assert!(proof.validate().is_ok());
    assert_conflict(proof.validate_candidate(&fixture.original).unwrap_err());
    assert_eq!(fixture.files().await, before);
    fixture.finish().await;
}

#[tokio::test]
async fn raw_actor_append_and_model_context_reject_an_old_generation_without_writes() {
    let fixture = Fixture::new().await;
    let fence = running_generation_root(&fixture).await;
    let before = fixture.files().await;
    let mut request = ActorTranscriptAppend {
        fence: fence.clone(),
        expected_created_at: fixture.canonical.created_at,
        expected_conversation_generation: fixture.original.conversation_generation_observation(),
        // Make prefix equality insufficient by supplying the current prefix;
        // the original execution observation must independently remain frozen.
        expected_messages: fixture.canonical.messages.clone(),
        expected_provider_transcript: fixture.canonical.provider_transcript.clone(),
        messages: vec![Message::assistant("new generation reply", None)],
        native_groups: vec![],
    };
    assert!(matches!(
        fixture.store.append_actor_transcript(request.clone()).await,
        Err(ActorTranscriptAppendError::ConversationChanged)
    ));
    assert_eq!(fixture.files().await, before);
    let mut expected = fixture.canonical.clone();
    expected.conversation_reset = None;
    let model_request = ActorModelContextCheckpoint {
        fence,
        expected,
        candidate: fixture.canonical.model_context_state.clone().unwrap(),
        snapshot_blocks: vec![],
    };
    assert!(matches!(
        fixture
            .store
            .checkpoint_actor_model_context(model_request)
            .await,
        Err(ActorModelContextError::SnapshotConflict)
    ));
    assert_eq!(fixture.files().await, before);
    request.expected_conversation_generation =
        fixture.canonical.conversation_generation_observation();
    let committed = fixture
        .store
        .append_actor_transcript(request)
        .await
        .unwrap();
    assert_eq!(
        committed.conversation_reset,
        fixture.canonical.conversation_reset
    );
    assert_eq!(
        committed.messages.len(),
        fixture.canonical.messages.len() + 1
    );
    fixture.finish().await;
}

#[tokio::test]
async fn raw_actor_input_refuses_a_stale_observation_before_admission_or_claim_ack() {
    use bamboo_domain::{ActorDirectoryPort, SessionInboxPort};
    let fixture = Fixture::new().await;
    let store = Arc::new(
        SessionStoreV2::new(fixture.home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let inbox =
        crate::FileSessionInbox::new(store.clone(), bamboo_domain::SessionInboxLimits::default());
    let envelope = bamboo_domain::SessionMessageEnvelope::user_input(
        &fixture.canonical.id,
        "synthetic new input",
    );
    inbox
        .deliver_with_activation_intent(
            &envelope,
            bamboo_domain::SessionActivationPolicy::RespectSpecificWait,
            None,
        )
        .await
        .unwrap();
    let now = Utc::now();
    let claim = inbox
        .claim_owned(
            &fixture.canonical.id,
            1,
            None,
            &bamboo_domain::SessionInboxLeaseRequest {
                consumer: bamboo_domain::SessionInboxConsumerId::new(),
                now,
                duration: chrono::Duration::minutes(5),
            },
        )
        .await
        .unwrap()
        .remove(0);
    let activation = store
        .claim_activation(&bamboo_domain::ActorActivationClaim {
            actor_id: fixture.canonical.id.clone(),
            run_id: "generation-input-run".into(),
            lease_owner: claim.lease.consumer.as_str().into(),
            lease_expires_at: claim.lease.expires_at,
            inbox_generation: claim.claim.generation,
            placement_ref: None,
            now,
        })
        .await
        .unwrap();
    store
        .start_activation(&activation.fence(), now)
        .await
        .unwrap();
    let mut request = ActorInputCheckpoint {
        fence: activation.fence(),
        expected_created_at: fixture.canonical.created_at,
        expected_conversation_generation: fixture.original.conversation_generation_observation(),
        claim: claim.clone(),
        expected_messages: fixture.canonical.messages.clone(),
        expected_provider_transcript: fixture.canonical.provider_transcript.clone(),
        expected_admission: fixture.canonical.session_inbox_admission().cloned(),
    };
    let before = fixture.files().await;
    assert!(matches!(
        inbox.checkpoint_actor_input(request.clone()).await,
        Err(ActorInputCheckpointError::InvalidAuthority(
            ActorTranscriptAppendError::ConversationChanged
        ))
    ));
    assert_eq!(fixture.files().await, before);
    assert!(!inbox
        .was_admitted(&fixture.canonical.id, &envelope.id)
        .await
        .unwrap());
    assert_eq!(
        inbox.inspect(&fixture.canonical.id).await.unwrap().claimed,
        1
    );
    request.expected_conversation_generation =
        fixture.canonical.conversation_generation_observation();
    let committed = inbox.checkpoint_actor_input(request).await.unwrap();
    assert_eq!(
        committed.session.conversation_reset,
        fixture.canonical.conversation_reset
    );
    assert!(committed
        .session
        .session_inbox_admission()
        .is_some_and(|cursor| cursor.contains(&envelope.id)));
    assert!(!inbox
        .was_admitted(&fixture.canonical.id, &envelope.id)
        .await
        .unwrap());
    // Checkpoint is not ACK; generation validation never disposes a claim.
    assert_eq!(
        inbox.inspect(&fixture.canonical.id).await.unwrap().claimed,
        1
    );
    inbox
        .ack_owned(&fixture.canonical.id, &claim, Utc::now())
        .await
        .unwrap();
    assert!(inbox
        .was_admitted(&fixture.canonical.id, &envelope.id)
        .await
        .unwrap());
    assert_eq!(
        inbox.inspect(&fixture.canonical.id).await.unwrap().claimed,
        0
    );
    store.flush_search_index().await;
    drop(inbox);
    drop(store);
    fixture.finish().await;
}

#[tokio::test]
async fn startup_does_not_reconstruct_a_nonzero_root_proof_when_both_marker_and_proof_are_missing()
{
    let fixture = Fixture::new().await;
    let directory = fixture.store.sessions_root_dir().join(&fixture.original.id);
    let proof = directory.join(root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE);
    fs::remove_file(&proof).await.unwrap();
    fs::remove_file(fixture.home.path().join(".root_tool_authority_proof_v1"))
        .await
        .unwrap();
    let before = fixture.files().await;
    let reopened = SessionStoreV2::new(fixture.home.path().to_path_buf())
        .await
        .unwrap();
    assert!(
        !proof.exists(),
        "startup manufactured a committed reset proof"
    );
    assert!(reopened.load_session(&fixture.canonical.id).await.is_err());
    let after = fixture.files().await;
    assert_eq!(before[0], after[0]);
    assert_eq!(before[1], after[1]);
    reopened.flush_search_index().await;
    drop(reopened);
    fixture.finish().await;
}

#[cfg(any(target_os = "linux", target_os = "macos", windows))]
#[tokio::test]
async fn retained_actor_projection_keeps_the_nonzero_ledger_in_its_checked_pair() {
    use bamboo_domain::ActorSnapshotPort;
    let fixture = Fixture::new().await;
    let before = fixture.files().await;
    // The retained-FD reader requires a canonical capability root; macOS /var
    // is a symlink. Do not weaken the reader to fit the fixture path.
    let retained = SessionStoreV2::new(fixture.home.path().canonicalize().unwrap())
        .await
        .unwrap();
    let result = retained
        .actor_subtree_snapshot(
            bamboo_domain::ActorSnapshotPrincipal::host_owner(),
            &fixture.canonical.id,
            &fixture.canonical.id,
            bamboo_domain::ActorSnapshotLimits::default(),
        )
        .await;
    assert!(
        result.is_ok(),
        "nonzero sidecar projection was silently erased: {result:?}"
    );
    assert_eq!(fixture.files().await, before);
    // Recognizing v3 must not accept the same reset ledger under a historical
    // frame version. Both Unix and Windows readers use this retained path.
    let main_path = retained
        .sessions_root_dir()
        .join(&fixture.canonical.id)
        .join("session.json");
    let mut downgraded = before[0].as_ref().unwrap().clone();
    downgraded[..compact_main::PREFIX_V2.len()].copy_from_slice(compact_main::PREFIX_V2);
    fs::write(&main_path, downgraded).await.unwrap();
    let damaged = fixture.files().await;
    assert_eq!(
        retained
            .actor_subtree_snapshot(
                bamboo_domain::ActorSnapshotPrincipal::host_owner(),
                &fixture.canonical.id,
                &fixture.canonical.id,
                bamboo_domain::ActorSnapshotLimits::default(),
            )
            .await
            .unwrap_err(),
        bamboo_domain::ActorSnapshotError::InconsistentAuthority
    );
    assert_eq!(fixture.files().await, damaged);
    fs::write(&main_path, before[0].as_ref().unwrap())
        .await
        .unwrap();
    retained.flush_search_index().await;
    drop(retained);
    fixture.finish().await;
}

#[tokio::test]
async fn task_single_and_paired_cas_cannot_reinterpret_an_old_conversation_snapshot() {
    let fixture = Fixture::new().await;
    let peer = Session::new("zz-task-peer", "model");
    fixture.store.save_session(&peer).await.unwrap();
    let stale = runtime_sidecar_snapshot(&fixture.original);
    let mut stale_updated = stale.clone();
    let evaluated = super::tests::transaction_task_list(&fixture.original.id, "new Task state");
    // Task CAS accepts only Task fields. The convenience setter also changes
    // updated_at and would reject the fixture before the generation fence.
    stale_updated.task_list = Some(evaluated.clone());
    stale_updated.set_task_list_version_meta("1");
    let peer_original = fixture
        .store
        .load_runtime_control_plane(&peer.id)
        .await
        .unwrap()
        .unwrap();
    let mut peer_updated = peer_original.clone();
    peer_updated.task_list = Some(evaluated);
    peer_updated.set_task_list_version_meta("1");
    let before = fixture.files().await;
    assert_conflict(
        fixture
            .store
            .save_task_control_plane_if_matches(&stale, &stale_updated)
            .await
            .unwrap_err(),
    );
    assert_conflict(
        fixture
            .store
            .save_task_control_planes_atomically(
                &stale,
                &stale_updated,
                &peer_original,
                &peer_updated,
            )
            .await
            .unwrap_err(),
    );
    assert_eq!(fixture.files().await, before);
    assert!(fixture
        .store
        .runtime_task_journal_paths()
        .await
        .unwrap()
        .is_empty());
    let current = fixture
        .store
        .load_runtime_control_plane(&fixture.canonical.id)
        .await
        .unwrap()
        .unwrap();
    let mut updated = current.clone();
    updated.task_list = stale_updated.task_list;
    updated.set_task_list_version_meta("1");
    assert!(fixture
        .store
        .save_task_control_plane_if_matches(&current, &updated)
        .await
        .unwrap());
    let confirmed = fixture
        .store
        .load_session(&fixture.canonical.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        confirmed.conversation_reset,
        fixture.canonical.conversation_reset
    );
    assert_eq!(confirmed.messages.len(), fixture.canonical.messages.len());
    fixture.finish().await;
}

#[tokio::test]
async fn task_recovery_preflights_both_original_targets_and_legacy_cannot_cross_reset() {
    let fixture = Fixture::new().await;
    let peer = Session::new("zz-undo-peer", "model");
    fixture.store.save_session(&peer).await.unwrap();
    let mut wrong_birth = peer.clone();
    wrong_birth.created_at += chrono::Duration::seconds(1);
    let mut journal = RuntimeTaskTransactionJournal {
        version: RUNTIME_TASK_TRANSACTION_VERSION,
        transaction_id: Uuid::new_v4().to_string(),
        first: TaskControlPlaneUndo {
            session_id: fixture.canonical.id.clone(),
            task_list: Some(super::tests::transaction_task_list(
                &fixture.canonical.id,
                "must not be written",
            )),
            task_list_version: "unexpected-undo".into(),
            conversation_witness: Some(
                TaskConversationWitness::capture(&fixture.canonical).unwrap(),
            ),
        },
        second: TaskControlPlaneUndo {
            session_id: peer.id.clone(),
            task_list: None,
            task_list_version: "unexpected-undo".into(),
            conversation_witness: Some(TaskConversationWitness::capture(&wrong_birth).unwrap()),
        },
    };
    let path = fixture
        .store
        .runtime_task_transaction_dir()
        .join(format!("{}.json", journal.transaction_id));
    SessionStoreV2::validate_runtime_task_journal(&path, &journal).unwrap();
    let before = fixture.files().await;
    let guard = fixture
        .store
        .lock_runtime_task_transaction_exclusive()
        .await
        .unwrap();
    assert_conflict(
        fixture
            .store
            .rollback_runtime_task_journal(&journal, &guard)
            .await
            .unwrap_err(),
    );
    assert_eq!(
        fixture.files().await,
        before,
        "first target was written before the second preflight rejected"
    );
    journal.version = 1;
    journal.first.conversation_witness = None;
    journal.second.conversation_witness = None;
    SessionStoreV2::validate_runtime_task_journal(&path, &journal).unwrap();
    assert_conflict(
        fixture
            .store
            .rollback_runtime_task_journal(&journal, &guard)
            .await
            .unwrap_err(),
    );
    assert_eq!(fixture.files().await, before);
    journal.version = RUNTIME_TASK_TRANSACTION_VERSION;
    assert!(
        SessionStoreV2::validate_runtime_task_journal(&path, &journal).is_err(),
        "v2 cannot omit original source evidence"
    );
    drop(guard);
    fixture.finish().await;
}

#[tokio::test]
async fn a_retained_copy_rollback_marker_cannot_remove_a_reset_root() {
    let fixture = Fixture::new().await;
    let journal = SessionCopyTransactionJournal {
        version: SESSION_COPY_TRANSACTION_VERSION,
        transaction_id: Uuid::new_v4().to_string(),
        source_id: "old-copy-source".into(),
        target_id: fixture.canonical.id.clone(),
    };
    let marker = fixture
        .store
        .write_session_copy_journal(&journal)
        .await
        .unwrap();
    let before = fixture.files().await;
    let lifecycle = fixture
        .store
        .lock_session_lifecycle_exclusive()
        .await
        .unwrap();
    let runtime_task = fixture
        .store
        .lock_runtime_task_transaction_exclusive()
        .await
        .unwrap();
    assert_conflict(
        fixture
            .store
            .rollback_session_copy_transaction(&marker, &journal)
            .await
            .unwrap_err(),
    );
    assert_eq!(fixture.files().await, before);
    assert!(fixture
        .store
        .sessions_root_dir()
        .join(&fixture.canonical.id)
        .is_dir());
    assert!(marker.is_file());
    assert!(!marker.with_extension("deleting").exists());
    assert!(!fixture.store.session_copy_staging_dir(&journal).exists());
    drop(runtime_task);
    drop(lifecycle);
    fixture.finish().await;
}

#[tokio::test]
async fn startup_cannot_reconstruct_missing_nonzero_supervisor_identity_proof() {
    let home = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    store
        .get_or_create_default_supervisor("model")
        .await
        .unwrap();
    let supervisor = store
        .load_session(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    let candidate = proposed(&supervisor);
    let directory = store
        .sessions_root_dir()
        .join(DEFAULT_SUPERVISOR_SESSION_ID);
    let main = compact_main::serialize_main(&candidate).unwrap();
    let runtime = serde_json::to_vec_pretty(&runtime_sidecar_snapshot(&candidate)).unwrap();
    fs::write(directory.join("session.json"), &main)
        .await
        .unwrap();
    fs::write(directory.join(RUNTIME_SIDECAR_FILE), &runtime)
        .await
        .unwrap();
    SessionStoreV2::write_staged_root_tool_proof(&directory, &candidate)
        .await
        .unwrap();
    fs::remove_file(directory.join(supervisor_proof::SUPERVISOR_PROOF_FILE))
        .await
        .unwrap();
    fs::remove_file(
        home.path()
            .join(supervisor_proof::SUPERVISOR_PROOF_MIGRATION_MARKER),
    )
    .await
    .unwrap();
    let reopened = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    assert!(!directory
        .join(supervisor_proof::SUPERVISOR_PROOF_FILE)
        .exists());
    assert!(reopened
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .is_err());
    assert_eq!(
        fs::read(directory.join("session.json")).await.unwrap(),
        main
    );
    assert_eq!(
        fs::read(directory.join(RUNTIME_SIDECAR_FILE))
            .await
            .unwrap(),
        runtime
    );
    reopened.flush_search_index().await;
    store.flush_search_index().await;
    drop(reopened);
    drop(store);
    home.close().unwrap();
}

#[tokio::test]
async fn legacy_v1_task_recovery_preserves_its_existing_zero_generation_behavior() {
    let home = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    let (first, first_updated, second, second_updated) =
        super::tests::seed_runtime_task_transaction_pair(&store)
            .await
            .unwrap();
    let journal = RuntimeTaskTransactionJournal {
        version: 1,
        transaction_id: Uuid::new_v4().to_string(),
        first: TaskControlPlaneUndo {
            session_id: first.id.clone(),
            task_list: first.task_list.clone(),
            task_list_version: first.task_list_version_meta().unwrap(),
            conversation_witness: None,
        },
        second: TaskControlPlaneUndo {
            session_id: second.id.clone(),
            task_list: second.task_list.clone(),
            task_list_version: second.task_list_version_meta().unwrap(),
            conversation_witness: None,
        },
    };
    let guard = store
        .lock_runtime_task_transaction_exclusive()
        .await
        .unwrap();
    store
        .write_existing_runtime_sidecar_durable_unchecked(
            &first_updated,
            RuntimeTaskDurabilityEvent::FirstUpdatedSidecarPublished,
            &guard,
        )
        .await
        .unwrap();
    store
        .write_existing_runtime_sidecar_durable_unchecked(
            &second_updated,
            RuntimeTaskDurabilityEvent::SecondUpdatedSidecarPublished,
            &guard,
        )
        .await
        .unwrap();
    let path = store.write_runtime_task_journal(&journal).await.unwrap();
    assert_eq!(
        store
            .read_runtime_task_journal(&path)
            .await
            .unwrap()
            .version,
        1
    );
    store
        .recover_all_runtime_task_transactions_locked(&guard)
        .await
        .unwrap();
    assert!(!path.exists());
    drop(guard);
    for expected in [&first, &second] {
        let recovered = store.load_session(&expected.id).await.unwrap().unwrap();
        assert!(recovered.conversation_reset.is_none());
        assert_eq!(
            recovered.task_list_version_meta(),
            expected.task_list_version_meta()
        );
        assert_eq!(
            serde_json::to_value(recovered.task_list).unwrap(),
            serde_json::to_value(&expected.task_list).unwrap()
        );
    }
    store.flush_search_index().await;
    drop(store);
    home.close().unwrap();
}

#[tokio::test]
async fn child_wait_cannot_adopt_or_append_from_a_previous_conversation() {
    let fixture = Fixture::new().await;
    let before = fixture.files().await;
    let mut updated = fixture.original.clone();
    updated.add_message(Message::user("old conversation child wait outcome"));
    let before_updated = serde_json::to_value(&updated).unwrap();
    let published = Arc::new(AtomicBool::new(false));
    let published_from_register = published.clone();
    assert_conflict(
        fixture
            .store
            .register_child_wait(
                &fixture.original,
                &[],
                bamboo_domain::ChildWaitPolicy::All,
                false,
                Arc::new(move |_| {
                    published_from_register.store(true, Ordering::SeqCst);
                }),
            )
            .await
            .unwrap_err(),
    );
    let published_from_cas = published.clone();
    assert_conflict(
        fixture
            .store
            .compare_exchange_child_wait(
                &fixture.original,
                &mut updated,
                false,
                Arc::new(move |_| {
                    published_from_cas.store(true, Ordering::SeqCst);
                }),
            )
            .await
            .unwrap_err(),
    );
    assert_eq!(fixture.files().await, before);
    assert_eq!(serde_json::to_value(&updated).unwrap(), before_updated);
    assert!(!published.load(Ordering::SeqCst));
    fixture.finish().await;
}

// Copy rollback fixtures exercise the production phase boundary and an actual
// store reopen. Every file belongs to a fresh synthetic TempDir.
async fn copy_rollback_fixture() -> (
    TempDir,
    SessionStoreV2,
    SessionCopyTransactionJournal,
    PathBuf,
) {
    let home = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    let mut source = Session::new("rollback-source", "model");
    source.add_message(Message::user("synthetic copy payload"));
    store.save_session(&source).await.unwrap();
    let copied = store
        .copy_session(&source.id, "rollback-target")
        .await
        .unwrap()
        .unwrap();
    let journal = SessionCopyTransactionJournal {
        version: SESSION_COPY_TRANSACTION_VERSION,
        transaction_id: Uuid::new_v4().to_string(),
        source_id: source.id,
        target_id: copied.id,
    };
    let marker = store.write_session_copy_journal(&journal).await.unwrap();
    (home, store, journal, marker)
}

async fn prepare_copy_rollback_phase(
    store: &SessionStoreV2,
    marker: &Path,
    journal: &SessionCopyTransactionJournal,
) -> PathBuf {
    let _lifecycle = store.lock_session_lifecycle_exclusive().await.unwrap();
    let _runtime_task = store
        .lock_runtime_task_transaction_exclusive()
        .await
        .unwrap();
    store
        .prepare_session_copy_rollback_deletion(marker, journal)
        .await
        .unwrap()
}

async fn copy_target_files(store: &SessionStoreV2, id: &str) -> [Vec<u8>; 3] {
    let target = store.sessions_root_dir().join(id);
    [
        fs::read(target.join("session.json")).await.unwrap(),
        fs::read(target.join(RUNTIME_SIDECAR_FILE)).await.unwrap(),
        fs::read(store.index_path()).await.unwrap(),
    ]
}

#[tokio::test]
async fn copy_rollback_deleting_resumes_after_partial_staging_cleanup() {
    let (home, store, journal, marker) = copy_rollback_fixture().await;
    let staging = prepare_copy_rollback_phase(&store, &marker, &journal).await;
    assert!(!store.sessions_root_dir().join(&journal.target_id).exists());
    assert!(store.get_index_entry(&journal.target_id).await.is_none());
    assert!(marker.with_extension("deleting").is_file());
    fs::write(staging.join("remaining-payload"), b"not yet removed")
        .await
        .unwrap();
    // Model interruption after remove_dir_all has removed both identity files.
    fs::remove_file(staging.join("session.json")).await.unwrap();
    fs::remove_file(staging.join(RUNTIME_SIDECAR_FILE))
        .await
        .unwrap();
    store.flush_search_index().await;
    drop(store);

    let reopened = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    assert!(!staging.exists());
    assert!(!reopened
        .sessions_root_dir()
        .join(&journal.target_id)
        .exists());
    assert!(reopened.get_index_entry(&journal.target_id).await.is_none());
    assert!(reopened
        .session_copy_journal_paths()
        .await
        .unwrap()
        .is_empty());
    reopened.flush_search_index().await;
    drop(reopened);
    home.close().unwrap();
}

#[tokio::test]
async fn copy_rollback_deleting_preserves_a_recreated_target_and_wins_over_siblings() {
    let (home, store, journal, marker) = copy_rollback_fixture().await;
    let staging = prepare_copy_rollback_phase(&store, &marker, &journal).await;
    fs::remove_dir_all(&staging).await.unwrap();
    sync_parent_directory_entry(&staging).await.unwrap();
    // Recursive cleanup finished, but the durable Deleting marker remains.
    // Generic saves can legally reuse this absent canonical ID between retries.
    let mut foreign = Session::new(&journal.target_id, "different-model");
    foreign.created_at += chrono::Duration::seconds(1);
    foreign.add_message(Message::user("new independent Root must survive"));
    store.save_session(&foreign).await.unwrap();
    let before = copy_target_files(&store, &journal.target_id).await;
    let body = serde_json::to_vec(&journal).unwrap();
    for extension in ["json", "committing", "committed"] {
        durable_atomic_write(&marker.with_extension(extension), &body)
            .await
            .unwrap();
    }
    store.flush_search_index().await;
    drop(store);

    let reopened = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    assert_eq!(
        copy_target_files(&reopened, &journal.target_id).await,
        before
    );
    assert!(reopened.get_index_entry(&journal.target_id).await.is_some());
    assert!(reopened
        .session_copy_journal_paths()
        .await
        .unwrap()
        .is_empty());
    assert!(!staging.exists());
    reopened.flush_search_index().await;
    drop(reopened);
    let again = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    assert_eq!(copy_target_files(&again, &journal.target_id).await, before);
    assert!(again.session_copy_journal_paths().await.unwrap().is_empty());
    again.flush_search_index().await;
    drop(again);
    home.close().unwrap();
}

#[tokio::test]
async fn copy_rollback_prepared_staging_preserves_a_foreign_canonical() {
    let home = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    let journal = SessionCopyTransactionJournal {
        version: SESSION_COPY_TRANSACTION_VERSION,
        transaction_id: Uuid::new_v4().to_string(),
        source_id: "prepublication-source".into(),
        target_id: "prepublication-target".into(),
    };
    let marker = store.write_session_copy_journal(&journal).await.unwrap();
    let staging = store.session_copy_staging_dir(&journal);
    fs::create_dir(&staging).await.unwrap();
    fs::write(staging.join("partial-copy"), b"unpublished")
        .await
        .unwrap();
    let mut foreign = Session::new(&journal.target_id, "independent-model");
    foreign.add_message(Message::user("foreign canonical history"));
    store.save_session(&foreign).await.unwrap();
    let before = copy_target_files(&store, &journal.target_id).await;
    store.flush_search_index().await;
    drop(store);

    let reopened = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    assert_eq!(
        copy_target_files(&reopened, &journal.target_id).await,
        before
    );
    assert!(reopened.get_index_entry(&journal.target_id).await.is_some());
    assert!(!staging.exists());
    assert!(!marker.exists());
    assert!(reopened
        .session_copy_journal_paths()
        .await
        .unwrap()
        .is_empty());
    reopened.flush_search_index().await;
    drop(reopened);
    home.close().unwrap();
}

#[tokio::test]
async fn copy_rollback_unmarked_partial_canonical_refuses_without_mutation() {
    for missing in [
        vec!["session.json"],
        vec![RUNTIME_SIDECAR_FILE],
        vec!["session.json", RUNTIME_SIDECAR_FILE],
    ] {
        let (home, store, journal, marker) = copy_rollback_fixture().await;
        let target = store.sessions_root_dir().join(&journal.target_id);
        for filename in &missing {
            fs::remove_file(target.join(filename)).await.unwrap();
        }
        fs::write(
            target.join("remaining-payload"),
            b"legacy incomplete cleanup",
        )
        .await
        .unwrap();
        let before = [
            fs::read(target.join("session.json")).await.ok(),
            fs::read(target.join(RUNTIME_SIDECAR_FILE)).await.ok(),
            fs::read(store.index_path()).await.ok(),
            fs::read(&marker).await.ok(),
        ];
        store.flush_search_index().await;
        drop(store);

        assert!(SessionStoreV2::new(home.path().to_path_buf())
            .await
            .is_err());
        assert_eq!(
            [
                fs::read(target.join("session.json")).await.ok(),
                fs::read(target.join(RUNTIME_SIDECAR_FILE)).await.ok(),
                fs::read(home.path().join("sessions.json")).await.ok(),
                fs::read(&marker).await.ok(),
            ],
            before,
            "legacy partial canonical must remain untouched: {missing:?}"
        );
        assert!(target.join("remaining-payload").is_file());
        assert!(!marker.with_extension("deleting").exists());
        assert!(!home
            .path()
            .join(format!(".session-copy-{}", journal.transaction_id))
            .exists());
        home.close().unwrap();
    }
}
