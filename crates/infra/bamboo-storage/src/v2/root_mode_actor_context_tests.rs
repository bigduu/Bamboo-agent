//! Mode control-plane resets share the real Actor writer/claim boundary.
use super::*;
use bamboo_domain::{
    ActorActivationClaim, ActorActivationFinish, ActorDirectoryPort, ConversationSummary,
    ModelContextResetReason, ModelContextState, RootActorRuntimeWrite, SessionAuthorityConflict,
};

fn root() -> Session {
    let mut root = Session::new("activated-mode-root", "model");
    root.model_context_state = Some(ModelContextState {
        state_revision: 7,
        prefix_epoch: 3,
        cache_scope_sha256: Some("a".repeat(64)),
        transcript_item_sha256: vec!["b".repeat(64)],
        ..Default::default()
    });
    root.conversation_summary = Some(ConversationSummary::new("preserved summary", 1, 10));
    root.add_message(bamboo_domain::Message::user("preserved history"));
    root.activate_provider_transcript_route(
        bamboo_domain::session::provider_transcript::ProviderFamily::OpenAi,
        bamboo_domain::session::provider_transcript::ProviderProtocol::OpenAiResponsesV1,
        &"c".repeat(64),
    )
    .unwrap();
    root
}

fn request(root: &Session, epoch: u64, enabled: bool) -> RootModeOperationRequest {
    RootModeOperationRequest {
        session_id: root.id.clone(),
        operation_id: format!("{epoch}:{}", Uuid::new_v4()),
        birth_token: root.root_mode_birth_token(),
        expected_epoch: epoch,
        requested_enabled: enabled,
        action: RootModeOperationAction::Select,
    }
}

async fn activate(store: &SessionStoreV2, root: &Session) -> RootActorRuntimeWrite {
    let now = Utc::now();
    let activation = store
        .claim_activation(&ActorActivationClaim {
            actor_id: root.id.clone(),
            run_id: Uuid::new_v4().to_string(),
            lease_owner: "fixture-host".into(),
            lease_expires_at: now + chrono::Duration::minutes(5),
            inbox_generation: 0,
            placement_ref: None,
            now,
        })
        .await
        .unwrap();
    store
        .start_activation(&activation.fence(), now)
        .await
        .unwrap();
    RootActorRuntimeWrite {
        fence: activation.fence(),
        created_at: root.created_at,
    }
}

fn assert_conflict(error: io::Error) {
    assert!(
        error
            .get_ref()
            .is_some_and(|cause| cause.is::<SessionAuthorityConflict>()),
        "{error:?}"
    );
}

fn canonical(directory: &Path) -> Vec<Vec<u8>> {
    [
        "session.json",
        RUNTIME_SIDECAR_FILE,
        root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE,
    ]
    .map(|name| std::fs::read(directory.join(name)).unwrap())
    .into()
}

#[tokio::test]
async fn root_mode_activated_idle_resets_context_both_directions_across_restart() {
    let home = tempfile::tempdir().unwrap();
    let first = SessionStoreV2::new(home.path().into()).await.unwrap();
    let original = root();
    first.save_session(&original).await.unwrap();
    let old_owner = activate(&first, &original).await;
    first
        .finish_activation(
            &old_owner.fence,
            Utc::now(),
            ActorActivationFinish::Succeeded,
        )
        .await
        .unwrap();
    let actor_before = first.inspect_actor(&original.id).await.unwrap();
    assert!(actor_before.actor.current_attempt > 0);
    assert!(actor_before.activation.is_some());
    let select = request(&original, 0, true);
    // Reproduce the original mode port's ordinary-save path on the exact
    // activated fixture before exercising the authorized control-plane path.
    let mut unfenced_mode = first.load_session(&original.id).await.unwrap().unwrap();
    unfenced_mode.set_root_orchestration_only(true).unwrap();
    unfenced_mode
        .record_root_mode_operation(RootModeOperationReceipt {
            operation_id: select.operation_id.clone(),
            expected_epoch: 0,
            resulting_epoch: 0,
            requested_enabled: true,
            enabled_at_completion: false,
            tool_authority_revision: 0,
            outcome: RootModeOperationOutcome::Committed,
        })
        .unwrap();
    let before = canonical(&first.sessions_dir.join(&original.id));
    let rejected = first.save_session(&unfenced_mode).await.unwrap_err();
    assert_eq!(rejected.kind(), io::ErrorKind::Unsupported);
    assert_conflict(rejected);
    assert_eq!(canonical(&first.sessions_dir.join(&original.id)), before);
    let committed = first.root_mode_operation(&select).await.unwrap();
    first.flush_search_index().await;
    drop(first);

    let restarted = SessionStoreV2::new(home.path().into()).await.unwrap();
    assert_eq!(
        restarted.root_mode_operation(&select).await.unwrap(),
        committed
    );
    assert_eq!(
        restarted
            .root_mode_operation(&RootModeOperationRequest {
                action: RootModeOperationAction::Recover,
                ..select.clone()
            })
            .await
            .unwrap(),
        committed
    );
    let ultra = restarted.load_session(&original.id).await.unwrap().unwrap();
    assert!(ultra.root_orchestration_only_enabled());
    assert_eq!(ultra.root_tool_authority_revision, 1);
    assert_eq!(ultra.root_mode_transition_epoch, 1);
    let state = ultra.model_context_state.as_ref().unwrap();
    assert_eq!(state.prefix_epoch, 4);
    assert_eq!(state.state_revision, 8);
    assert!(state.cache_scope_sha256.is_none());
    assert!(state.transcript_item_sha256.is_empty());
    assert_eq!(
        state.last_reset_reason,
        Some(ModelContextResetReason::CacheScopeChanged)
    );
    assert_eq!(
        serde_json::to_value(&ultra.messages).unwrap(),
        serde_json::to_value(&original.messages).unwrap()
    );
    assert_eq!(
        serde_json::to_value(&ultra.conversation_summary).unwrap(),
        serde_json::to_value(&original.conversation_summary).unwrap()
    );
    assert_eq!(
        restarted.inspect_actor(&original.id).await.unwrap(),
        actor_before
    );

    let standard = request(&original, 1, false);
    let second = restarted.root_mode_operation(&standard).await.unwrap();
    restarted.flush_search_index().await;
    drop(restarted);
    let final_store = SessionStoreV2::new(home.path().into()).await.unwrap();
    let durable = final_store
        .load_session(&original.id)
        .await
        .unwrap()
        .unwrap();
    assert!(!durable.root_orchestration_only_enabled());
    assert_eq!(durable.root_tool_authority_revision, 2);
    assert_eq!(durable.root_mode_transition_epoch, 2);
    assert_eq!(
        durable.model_context_state.as_ref().unwrap().prefix_epoch,
        4 // Both selections share the existing pending, unseeded reset.
    );
    assert_eq!(
        durable.model_context_state.as_ref().unwrap().state_revision,
        8
    );
    assert_eq!(durable.root_mode_operations.len(), 2);
    assert_eq!(
        durable.provider_transcript.epoch(),
        original.provider_transcript.epoch() + 2
    );
    assert_eq!(
        final_store.root_mode_operation(&standard).await.unwrap(),
        second
    );
    assert_eq!(
        final_store.inspect_actor(&original.id).await.unwrap(),
        actor_before
    );
    let before = canonical(&final_store.sessions_dir.join(&original.id));
    let mut unfenced = durable.clone();
    unfenced.model_context_state.as_mut().unwrap().prefix_epoch += 1;
    for runtime in [false, true] {
        let error = if runtime {
            final_store.save_runtime_state(&unfenced).await
        } else {
            final_store.save_session(&unfenced).await
        }
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        assert_conflict(error);
    }
    assert_conflict(
        final_store
            .save_root_actor_runtime(&old_owner, &durable, false, Arc::new(|_| {}))
            .await
            .unwrap_err(),
    );
    assert_eq!(
        canonical(&final_store.sessions_dir.join(&original.id)),
        before
    );
    final_store.flush_search_index().await;
}

#[tokio::test]
async fn root_mode_running_owner_cannot_restore_pre_transition_context() {
    let home = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(home.path().into()).await.unwrap();
    let original = root();
    store.save_session(&original).await.unwrap();
    let owner = activate(&store, &original).await;
    let actor_before = store.inspect_actor(&original.id).await.unwrap();
    store
        .root_mode_operation(&request(&original, 0, true))
        .await
        .unwrap();
    let directory = store.sessions_dir.join(&original.id);
    let before = canonical(&directory);
    for runtime in [false, true] {
        assert_conflict(
            store
                .save_root_actor_runtime(&owner, &original, runtime, Arc::new(|_| {}))
                .await
                .unwrap_err(),
        );
        assert_eq!(canonical(&directory), before);
    }
    // The control plane does not mint, replace or revoke the actual owner.
    assert_eq!(
        store.inspect_actor(&original.id).await.unwrap(),
        actor_before
    );
    let mut current = store.load_session(&original.id).await.unwrap().unwrap();
    current.conversation_summary.as_mut().unwrap().content = "current owner context".into();
    store
        .save_root_actor_runtime(&owner, &current, true, Arc::new(|_| {}))
        .await
        .unwrap();
    let loaded = store.load_session(&original.id).await.unwrap().unwrap();
    assert_eq!(loaded.model_context_state, current.model_context_state);
    assert_eq!(
        serde_json::to_value(&loaded.conversation_summary).unwrap(),
        serde_json::to_value(&current.conversation_summary).unwrap()
    );
    store.flush_search_index().await;
}

#[tokio::test]
async fn root_mode_supervisor_retains_its_existing_context_writer_authority() {
    let home = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(home.path().into()).await.unwrap();
    let bootstrap = store
        .get_or_create_default_supervisor("model")
        .await
        .unwrap();
    let mut original = store
        .load_session(&bootstrap.session_id)
        .await
        .unwrap()
        .unwrap();
    assert!(!original.authority_identity.is_ordinary());
    original.model_context_state = root().model_context_state;
    original.provider_transcript = root().provider_transcript;
    store.save_session(&original).await.unwrap();
    let original = store.load_session(&original.id).await.unwrap().unwrap();
    let select = request(&original, 0, true);
    let record = |request: &RootModeOperationRequest| RootModeOperationReceipt {
        operation_id: request.operation_id.clone(),
        expected_epoch: request.expected_epoch,
        resulting_epoch: 0,
        requested_enabled: request.requested_enabled,
        enabled_at_completion: false,
        tool_authority_revision: 0,
        outcome: RootModeOperationOutcome::Committed,
    };

    // This is the mode port's original full-writer path, using a Supervisor
    // minted by its bootstrap authority rather than a fabricated identity.
    let mut baseline = original.clone();
    baseline.set_root_orchestration_only(true).unwrap();
    baseline
        .record_root_mode_operation(record(&select))
        .unwrap();
    store.save_session(&baseline).await.unwrap();
    assert_eq!(
        store.root_mode_operation(&select).await.unwrap(),
        RootModeOperationDecision::Terminal(
            baseline
                .root_mode_operation(&select.operation_id)
                .unwrap()
                .clone()
        )
    );
    let standard = request(&original, 1, false);
    let mut expected = store.load_session(&original.id).await.unwrap().unwrap();
    expected.set_root_orchestration_only(false).unwrap();
    expected
        .record_root_mode_operation(record(&standard))
        .unwrap();
    let result = store.root_mode_operation(&standard).await.unwrap();
    assert_eq!(
        result,
        RootModeOperationDecision::Terminal(
            expected
                .root_mode_operation(&standard.operation_id)
                .unwrap()
                .clone()
        )
    );
    store.flush_search_index().await;
    drop(store);

    let restarted = SessionStoreV2::new(home.path().into()).await.unwrap();
    let durable = restarted.load_session(&original.id).await.unwrap().unwrap();
    let authority = restarted
        .load_root_authority(&original.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(durable.authority_identity, original.authority_identity);
    assert_eq!(authority.authority_identity, original.authority_identity);
    assert!(!durable.root_orchestration_only_enabled());
    assert_eq!(durable.root_tool_authority_revision, 2);
    assert_eq!(durable.root_mode_transition_epoch, 2);
    assert_eq!(durable.model_context_state, expected.model_context_state);
    assert_eq!(authority.model_context_state, expected.model_context_state);
    assert_eq!(durable.provider_transcript, expected.provider_transcript);
    assert_eq!(durable.root_mode_operations, expected.root_mode_operations);
    assert_eq!(
        authority.root_mode_operations,
        expected.root_mode_operations
    );
    assert_eq!(
        restarted.root_mode_operation(&standard).await.unwrap(),
        result
    );
    restarted.flush_search_index().await;
}

#[tokio::test]
async fn root_mode_actor_replacement_waits_for_committed_reset_and_old_owner_stays_fenced() {
    let home = tempfile::tempdir().unwrap();
    let first = SessionStoreV2::new(home.path().into()).await.unwrap();
    let original = root();
    first.save_session(&original).await.unwrap();
    let old_owner = activate(&first, &original).await;
    first
        .finish_activation(
            &old_owner.fence,
            Utc::now(),
            ActorActivationFinish::Succeeded,
        )
        .await
        .unwrap();
    let second = SessionStoreV2::new(home.path().into()).await.unwrap();
    let (reached, release) = first.pause_full_save_before_filesystem_commit_for_test(&original.id);
    let select = request(&original, 0, true);
    let (committed, owner) = tokio::join!(first.root_mode_operation(&select), async {
        reached.wait().await;
        let claim = activate(&second, &original);
        tokio::pin!(claim);
        assert!(tokio::time::timeout(Duration::from_millis(100), &mut claim)
            .await
            .is_err());
        release.wait().await;
        claim.await
    });
    assert!(matches!(
        committed.unwrap(),
        RootModeOperationDecision::Terminal(_)
    ));
    assert!(owner.fence.attempt > old_owner.fence.attempt);
    let mut current = second.load_session(&original.id).await.unwrap().unwrap();
    assert_eq!(
        current.model_context_state.as_ref().unwrap().prefix_epoch,
        4
    );
    assert_conflict(
        first
            .save_root_actor_runtime(&old_owner, &current, true, Arc::new(|_| {}))
            .await
            .unwrap_err(),
    );
    current.conversation_summary.as_mut().unwrap().content = "replacement owner".into();
    second
        .save_root_actor_runtime(&owner, &current, true, Arc::new(|_| {}))
        .await
        .unwrap();
    assert_eq!(
        first
            .load_session(&original.id)
            .await
            .unwrap()
            .unwrap()
            .conversation_summary
            .unwrap()
            .content,
        current.conversation_summary.unwrap().content
    );
    first.flush_search_index().await;
    second.flush_search_index().await;
}

#[tokio::test]
async fn root_mode_running_snapshot_adopts_missed_round_trip_before_owned_checkpoint() {
    let home = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(home.path().into()).await.unwrap();
    let mut running = root();
    store.save_session(&running).await.unwrap();
    let owner = activate(&store, &running).await;
    store
        .root_mode_operation(&request(&running, 0, true))
        .await
        .unwrap();
    store
        .root_mode_operation(&request(&running, 1, false))
        .await
        .unwrap();
    let durable = store.load_session(&running.id).await.unwrap().unwrap();
    assert_eq!(running.root_thinking_mode(), durable.root_thinking_mode());
    running.adopt_root_tool_authority_from(&durable).unwrap();
    assert_eq!(running.model_context_state, durable.model_context_state);
    assert_eq!(running.provider_transcript, durable.provider_transcript);
    store
        .save_root_actor_runtime(&owner, &running, false, Arc::new(|_| {}))
        .await
        .unwrap();
    let after = store.load_session(&running.id).await.unwrap().unwrap();
    assert_eq!(after.model_context_state, durable.model_context_state);
    assert_eq!(after.root_mode_transition_epoch, 2);
    assert_eq!(after.root_mode_operations, durable.root_mode_operations);
    store.flush_search_index().await;
}

#[tokio::test]
async fn root_mode_serializes_running_checkpoint_and_finish_without_context_rollback() {
    for finish in [false, true] {
        let home = tempfile::tempdir().unwrap();
        let first = SessionStoreV2::new(home.path().into()).await.unwrap();
        let original = root();
        first.save_session(&original).await.unwrap();
        let owner = activate(&first, &original).await;
        let second = SessionStoreV2::new(home.path().into()).await.unwrap();
        let (reached, release) =
            first.pause_full_save_before_filesystem_commit_for_test(&original.id);
        let select = request(&original, 0, true);
        let (committed, _) = tokio::join!(first.root_mode_operation(&select), async {
            reached.wait().await;
            let competing = async {
                if finish {
                    second
                        .finish_activation(
                            &owner.fence,
                            Utc::now(),
                            ActorActivationFinish::Succeeded,
                        )
                        .await
                        .unwrap();
                } else {
                    assert_conflict(
                        second
                            .save_root_actor_runtime(&owner, &original, false, Arc::new(|_| {}))
                            .await
                            .unwrap_err(),
                    );
                }
            };
            tokio::pin!(competing);
            assert!(
                tokio::time::timeout(Duration::from_millis(100), &mut competing)
                    .await
                    .is_err()
            );
            release.wait().await;
            competing.await
        });
        assert!(matches!(
            committed.unwrap(),
            RootModeOperationDecision::Terminal(_)
        ));
        let current = second.load_session(&original.id).await.unwrap().unwrap();
        assert!(current.root_orchestration_only_enabled());
        assert_eq!(current.root_mode_transition_epoch, 1);
        assert_eq!(
            current.model_context_state.as_ref().unwrap().prefix_epoch,
            4
        );
        assert_eq!(
            serde_json::to_value(&current.messages).unwrap(),
            serde_json::to_value(&original.messages).unwrap()
        );
        first.flush_search_index().await;
        second.flush_search_index().await;
    }
}

#[tokio::test]
async fn root_mode_unknown_actor_witness_rejects_reset_but_recovery_still_fences() {
    for witness in ["actor-authority.json", "actor-authority.initialized.json"] {
        let home = tempfile::tempdir().unwrap();
        let store = SessionStoreV2::new(home.path().into()).await.unwrap();
        let original = root();
        store.save_session(&original).await.unwrap();
        activate(&store, &original).await;
        let directory = store.sessions_dir.join(&original.id);
        std::fs::remove_file(directory.join(witness)).unwrap();
        let before = canonical(&directory);
        let select = request(&original, 0, true);
        assert_conflict(store.root_mode_operation(&select).await.unwrap_err());
        assert_eq!(canonical(&directory), before);
        let recovered = store
            .root_mode_operation(&RootModeOperationRequest {
                action: RootModeOperationAction::Recover,
                ..select.clone()
            })
            .await
            .unwrap();
        assert!(matches!(
            recovered,
            RootModeOperationDecision::Terminal(RootModeOperationReceipt {
                outcome: RootModeOperationOutcome::Fenced,
                ..
            })
        ));
        let current = store.load_session(&original.id).await.unwrap().unwrap();
        assert_eq!(current.model_context_state, original.model_context_state);
        assert!(!current.root_orchestration_only_enabled());
        assert_eq!(store.root_mode_operation(&select).await.unwrap(), recovered);
        store.flush_search_index().await;
    }
}

#[tokio::test]
async fn root_mode_context_capability_rejects_unrelated_or_fabricated_changes() {
    let home = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(home.path().into()).await.unwrap();
    let original = root();
    store.save_session(&original).await.unwrap();
    let original = store.load_session(&original.id).await.unwrap().unwrap();
    activate(&store, &original).await;
    store
        .root_mode_operation(&request(&original, 0, true))
        .await
        .unwrap();
    let candidate = store.load_session(&original.id).await.unwrap().unwrap();
    let directory = store.sessions_dir.join(&original.id);
    let permit =
        root_context::RootModeContextWrite::capture(directory.clone(), &original, &candidate)
            .unwrap();
    assert!(permit.validate_candidate(&candidate, false).is_err());
    for mutation in ["summary", "history", "ledger", "model"] {
        let mut extra = candidate.clone();
        match mutation {
            "summary" => extra.conversation_summary.as_mut().unwrap().content = "forged".into(),
            "history" => extra.add_message(bamboo_domain::Message::user("forged")),
            "ledger" => extra.model_context_state.as_mut().unwrap().prefix_epoch += 1,
            "model" => extra.model = "forged".into(),
            _ => unreachable!(),
        }
        assert!(
            root_context::RootModeContextWrite::capture(directory.clone(), &original, &extra)
                .is_err()
        );
        assert!(permit.validate_candidate(&extra, true).is_err());
    }
    store.flush_search_index().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_mode_rechecks_actor_witness_at_actual_filesystem_publication() {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
    let original = root();
    store.save_session(&original).await.unwrap();
    activate(&store, &original).await;
    let directory = store.sessions_dir.join(&original.id);
    let before = canonical(&directory);
    let hook = default_actor_context_tests::DefaultWriteHook::install(
        &store,
        root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE,
        DurableWritePhase::BeforeReplace,
        false,
    );
    let select = request(&original, 0, true);
    let writer = store.clone();
    let pending = tokio::spawn(async move { writer.root_mode_operation(&select).await });
    let wait = hook.clone();
    tokio::task::spawn_blocking(move || wait.wait())
        .await
        .unwrap();
    std::fs::write(directory.join("actor-authority.json"), "{}").unwrap();
    hook.release();
    assert_conflict(pending.await.unwrap().unwrap_err());
    assert_eq!(canonical(&directory), before);
    store.flush_search_index().await;
}
