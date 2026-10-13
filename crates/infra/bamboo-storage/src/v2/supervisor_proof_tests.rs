//! The Supervisor proof is independent of transcript size and fails closed at
//! every interrupted publication boundary.

use super::*;
use bamboo_domain::{
    Message, Storage, SupervisorManagementMutation, SupervisorManagementRequest,
    SupervisorReference,
};

async fn fixture() -> (tempfile::TempDir, SessionStoreV2, SupervisorReference) {
    let home = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    let reference = (&store
        .get_or_create_default_supervisor("model")
        .await
        .unwrap())
        .into();
    (home, store, reference)
}

fn directory(store: &SessionStoreV2) -> PathBuf {
    store
        .sessions_root_dir()
        .join(DEFAULT_SUPERVISOR_SESSION_ID)
}

async fn set_scope(store: &SessionStoreV2, reference: &SupervisorReference) -> io::Result<()> {
    store
        .mutate_supervisor_management(&SupervisorManagementRequest {
            supervisor: reference.clone(),
            expected_state_revision: 0,
            mutation: SupervisorManagementMutation::ConfigureProjectScope {
                allowed_projects: ["project-a".parse().unwrap()].into(),
            },
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn management_publication_fails_closed_at_each_crash_boundary() {
    for fault in [
        supervisor_proof::SupervisorProofFault::Prepared,
        supervisor_proof::SupervisorProofFault::Runtime,
        supervisor_proof::SupervisorProofFault::Committed,
    ] {
        let (home, store, reference) = fixture().await;
        let main = fs::read(directory(&store).join("session.json"))
            .await
            .unwrap();
        *store.supervisor_proof_fault.lock().unwrap() = Some(fault);
        assert!(set_scope(&store, &reference).await.is_err(), "{fault:?}");
        assert_eq!(
            fs::read(directory(&store).join("session.json"))
                .await
                .unwrap(),
            main
        );
        let restarted = SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap();
        if fault == supervisor_proof::SupervisorProofFault::Committed {
            let scope = restarted
                .inspect_supervisor_scope(&reference)
                .await
                .unwrap();
            assert_eq!(scope.state_revision, 1);
            assert_eq!(scope.allowed_projects.len(), 1);
        } else {
            assert!(
                restarted
                    .inspect_supervisor_scope(&reference)
                    .await
                    .is_err(),
                "{fault:?}"
            );
            assert!(restarted
                .load_root_authority(&reference.session_id)
                .await
                .is_err());
        }
    }
}

#[tokio::test]
async fn legacy_side_ahead_of_main_upgrades_once_but_later_proof_loss_does_not() {
    let (home, store, reference) = fixture().await;
    set_scope(&store, &reference).await.unwrap();
    let proof = directory(&store).join(supervisor_proof::SUPERVISOR_PROOF_FILE);
    let marker = home
        .path()
        .join(supervisor_proof::SUPERVISOR_PROOF_MIGRATION_MARKER);
    fs::remove_file(&proof).await.unwrap();
    fs::remove_file(&marker).await.unwrap();
    let upgraded = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    assert_eq!(
        upgraded
            .inspect_supervisor_scope(&reference)
            .await
            .unwrap()
            .state_revision,
        1
    );
    assert!(proof.exists());
    assert!(marker.exists());
    fs::remove_file(&proof).await.unwrap();
    let restarted = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    assert!(restarted
        .inspect_supervisor_scope(&reference)
        .await
        .is_err());
    assert!(
        !proof.exists(),
        "a missing current proof must never be regenerated"
    );
}

#[tokio::test]
async fn migration_does_not_certify_a_divergent_legacy_management_pair() {
    let (home, store, reference) = fixture().await;
    set_scope(&store, &reference).await.unwrap();
    let dir = directory(&store);
    let proof = dir.join(supervisor_proof::SUPERVISOR_PROOF_FILE);
    let marker = home
        .path()
        .join(supervisor_proof::SUPERVISOR_PROOF_MIGRATION_MARKER);
    let main_path = dir.join("session.json");
    let mut main: serde_json::Value =
        serde_json::from_slice(&fs::read(&main_path).await.unwrap()).unwrap();
    let side: serde_json::Value =
        serde_json::from_slice(&fs::read(dir.join(RUNTIME_SIDECAR_FILE)).await.unwrap()).unwrap();
    main["supervisor_management"] = side["supervisor_management"].clone();
    main["supervisor_management"]["revision"] = serde_json::json!(2);
    fs::write(main_path, serde_json::to_vec(&main).unwrap())
        .await
        .unwrap();
    fs::remove_file(&proof).await.unwrap();
    fs::remove_file(&marker).await.unwrap();
    let restarted = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    assert!(marker.exists());
    assert!(!proof.exists());
    assert!(restarted
        .inspect_supervisor_scope(&reference)
        .await
        .is_err());
}

#[tokio::test]
async fn tampered_sidecar_proof_and_recreated_incarnation_are_rejected() {
    for damage in ["management", "identity", "proof-corrupt", "proof-stale"] {
        let (_home, store, reference) = fixture().await;
        set_scope(&store, &reference).await.unwrap();
        let dir = directory(&store);
        let runtime = dir.join(RUNTIME_SIDECAR_FILE);
        let proof = dir.join(supervisor_proof::SUPERVISOR_PROOF_FILE);
        match damage {
            "management" | "identity" => {
                let mut value: serde_json::Value =
                    serde_json::from_slice(&fs::read(&runtime).await.unwrap()).unwrap();
                if damage == "management" {
                    value["supervisor_management"]["revision"] = serde_json::json!(2);
                } else {
                    value["authority_identity"]["incarnation_id"] =
                        serde_json::json!(Uuid::new_v4());
                }
                fs::write(runtime, serde_json::to_vec(&value).unwrap())
                    .await
                    .unwrap();
            }
            "proof-corrupt" => fs::write(proof, b"not-json").await.unwrap(),
            "proof-stale" => {
                let mut value: serde_json::Value =
                    serde_json::from_slice(&fs::read(&proof).await.unwrap()).unwrap();
                value["supervisor_management"]["revision"] = serde_json::json!(0);
                fs::write(proof, serde_json::to_vec(&value).unwrap())
                    .await
                    .unwrap();
            }
            _ => unreachable!(),
        }
        assert!(
            store.inspect_supervisor_scope(&reference).await.is_err(),
            "{damage}"
        );
        assert!(
            store
                .load_runtime_control_plane(&reference.session_id)
                .await
                .is_err(),
            "{damage}"
        );
    }

    let (_home, store, old_reference) = fixture().await;
    let old_proof = fs::read(directory(&store).join(supervisor_proof::SUPERVISOR_PROOF_FILE))
        .await
        .unwrap();
    assert!(store
        .delete_session(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap());
    let new_reference: SupervisorReference = (&store
        .get_or_create_default_supervisor("new-model")
        .await
        .unwrap())
        .into();
    assert_ne!(new_reference.incarnation_id, old_reference.incarnation_id);
    fs::write(
        directory(&store).join(supervisor_proof::SUPERVISOR_PROOF_FILE),
        old_proof,
    )
    .await
    .unwrap();
    assert!(store
        .inspect_supervisor_scope(&new_reference)
        .await
        .is_err());
}

#[tokio::test]
async fn large_transcript_does_not_enter_supervisor_control_plane_or_management() {
    let (_home, store, _old_reference) = fixture().await;
    assert!(store
        .delete_session(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap());
    let reference: SupervisorReference = (&store
        .get_or_create_default_supervisor("new-model")
        .await
        .unwrap())
        .into();
    let mut supervisor = store
        .load_session(&reference.session_id)
        .await
        .unwrap()
        .unwrap();
    supervisor.add_message(Message::user("x".repeat(4 * 1024 * 1024)));
    store.save_session(&supervisor).await.unwrap();
    let dir = directory(&store);
    assert!(fs::metadata(dir.join("session.json")).await.unwrap().len() > 4 * 1024 * 1024);
    assert!(
        fs::metadata(dir.join(supervisor_proof::SUPERVISOR_PROOF_FILE))
            .await
            .unwrap()
            .len()
            < 1024
    );
    let control = store
        .load_root_authority(&reference.session_id)
        .await
        .unwrap()
        .unwrap();
    assert!(control.messages.is_empty());
    set_scope(&store, &reference).await.unwrap();
    // The large main file can become unreadable as a full Session without
    // granting or hiding authority on the bounded control-plane path.
    let main = dir.join("session.json");
    let mut value: serde_json::Value =
        serde_json::from_slice(&fs::read(&main).await.unwrap()).unwrap();
    value["messages"] = serde_json::json!({"invalid": "transcript shape"});
    fs::write(&main, serde_json::to_vec(&value).unwrap())
        .await
        .unwrap();
    assert!(store.load_session(&reference.session_id).await.is_err());
    assert_eq!(
        store
            .inspect_supervisor_scope(&reference)
            .await
            .unwrap()
            .state_revision,
        1
    );
    let changed = store
        .mutate_supervisor_management(&SupervisorManagementRequest {
            supervisor: reference.clone(),
            expected_state_revision: 1,
            mutation: SupervisorManagementMutation::ConfigureProjectScope {
                allowed_projects: ["project-b".parse().unwrap()].into(),
            },
        })
        .await
        .unwrap();
    assert_eq!(changed.state_revision, 2);
}

#[tokio::test]
async fn full_save_tool_selection_coordinates_both_proofs() {
    for fault in [
        supervisor_proof::SupervisorProofFault::Prepared,
        supervisor_proof::SupervisorProofFault::Runtime,
        supervisor_proof::SupervisorProofFault::Committed,
    ] {
        let (home, store, reference) = fixture().await;
        let mut supervisor = store
            .load_session(&reference.session_id)
            .await
            .unwrap()
            .unwrap();
        supervisor.set_root_orchestration_only(true).unwrap();
        *store.supervisor_proof_fault.lock().unwrap() = Some(fault);
        assert!(store.save_session(&supervisor).await.is_err(), "{fault:?}");
        let restarted = SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap();
        if fault == supervisor_proof::SupervisorProofFault::Committed {
            let current = restarted
                .load_root_authority(&reference.session_id)
                .await
                .unwrap()
                .unwrap();
            assert!(current.root_orchestration_only);
            assert_eq!(current.root_tool_authority_revision, 1);
        } else {
            assert!(
                restarted
                    .load_root_authority(&reference.session_id)
                    .await
                    .is_err(),
                "{fault:?}"
            );
        }
    }
}

#[tokio::test]
async fn paired_task_recovery_keeps_supervisor_proof_and_management_scope() {
    let (home, store, reference) = fixture().await;
    set_scope(&store, &reference).await.unwrap();
    let mut supervisor = store
        .load_session(&reference.session_id)
        .await
        .unwrap()
        .unwrap();
    let task = TaskList {
        session_id: reference.session_id.clone(),
        title: "original".into(),
        items: Vec::new(),
        created_at: supervisor.created_at,
        updated_at: supervisor.updated_at,
    };
    supervisor.set_task_list(task.clone());
    supervisor.set_task_list_version_meta("1");
    store.save_session(&supervisor).await.unwrap();
    let mut peer = Session::new("z-task-peer", "model");
    peer.set_task_list(task.clone());
    peer.set_task_list_version_meta("1");
    store.save_session(&peer).await.unwrap();

    let first = store
        .load_runtime_control_plane(&reference.session_id)
        .await
        .unwrap()
        .unwrap();
    let second = store
        .load_runtime_control_plane(&peer.id)
        .await
        .unwrap()
        .unwrap();
    let mut updated_first = first.clone();
    let mut updated_second = second.clone();
    let mut updated_task = task;
    updated_task.title = "evaluated".into();
    updated_first.task_list = Some(updated_task.clone());
    updated_second.task_list = Some(updated_task);
    updated_first.set_task_list_version_meta("2");
    updated_second.set_task_list_version_meta("2");
    let proof_path = directory(&store).join(supervisor_proof::SUPERVISOR_PROOF_FILE);
    let proof_before = fs::read(&proof_path).await.unwrap();
    store.inject_runtime_task_transaction_fault(RuntimeTaskTransactionFault::SecondUpdatedWrite);
    store.inject_runtime_task_transaction_fault(RuntimeTaskTransactionFault::FirstRollbackWrite);
    assert!(store
        .save_task_control_planes_atomically(&first, &updated_first, &second, &updated_second)
        .await
        .is_err());
    assert_eq!(store.runtime_task_journal_paths().await.unwrap().len(), 1);
    assert!(store.inspect_supervisor_scope(&reference).await.is_err());

    // Simulate an upgrade of an old Supervisor while its paired Task undo
    // journal is pending. Startup must establish the management proof before
    // the journal's sidecar rollback passes through the strict writer.
    fs::remove_file(&proof_path).await.unwrap();
    fs::remove_file(
        home.path()
            .join(supervisor_proof::SUPERVISOR_PROOF_MIGRATION_MARKER),
    )
    .await
    .unwrap();

    let restarted = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    assert!(restarted
        .runtime_task_journal_paths()
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        restarted
            .inspect_supervisor_scope(&reference)
            .await
            .unwrap()
            .state_revision,
        1
    );
    assert_eq!(fs::read(&proof_path).await.unwrap(), proof_before);
    for id in [&reference.session_id, &peer.id] {
        let current = restarted
            .load_runtime_control_plane(id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.task_list_version_meta().as_deref(), Some("1"));
    }
}

#[tokio::test]
async fn full_save_rejects_main_management_ahead_or_invalid_without_republishing_it() {
    for damage in ["ahead", "invalid"] {
        let (_home, store, reference) = fixture().await;
        set_scope(&store, &reference).await.unwrap();
        let candidate = store
            .load_session(&reference.session_id)
            .await
            .unwrap()
            .unwrap();
        let main_path = directory(&store).join("session.json");
        let mut main: serde_json::Value =
            serde_json::from_slice(&fs::read(&main_path).await.unwrap()).unwrap();
        // A management-only mutation normally leaves main at revision zero.
        // Here a damaged main claims newer or invalid management authority.
        main["supervisor_management"] =
            serde_json::to_value(candidate.supervisor_management.as_ref().unwrap()).unwrap();
        if damage == "ahead" {
            main["supervisor_management"]["revision"] = serde_json::json!(2);
        } else {
            main["supervisor_management"]["schema_version"] = serde_json::json!(999);
        }
        fs::write(&main_path, serde_json::to_vec(&main).unwrap())
            .await
            .unwrap();
        let before_main = fs::read(&main_path).await.unwrap();
        let before_side = fs::read(directory(&store).join(RUNTIME_SIDECAR_FILE))
            .await
            .unwrap();
        let before_proof =
            fs::read(directory(&store).join(supervisor_proof::SUPERVISOR_PROOF_FILE))
                .await
                .unwrap();
        assert!(store.save_session(&candidate).await.is_err(), "{damage}");
        assert_eq!(fs::read(&main_path).await.unwrap(), before_main);
        assert_eq!(
            fs::read(directory(&store).join(RUNTIME_SIDECAR_FILE))
                .await
                .unwrap(),
            before_side
        );
        assert_eq!(
            fs::read(directory(&store).join(supervisor_proof::SUPERVISOR_PROOF_FILE))
                .await
                .unwrap(),
            before_proof
        );
        // The bounded management read continues to authorize only its matched
        // sidecar and proof. Full history reads expose the damaged main.
        assert_eq!(
            store
                .inspect_supervisor_scope(&reference)
                .await
                .unwrap()
                .state_revision,
            1
        );
        assert!(store.load_session(&reference.session_id).await.is_err());
    }
}
