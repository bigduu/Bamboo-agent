//! Trusted singleton identity, strict reads, and ordinary-writer regression tests.

use super::*;
use bamboo_domain::{
    Message, SessionAuthorityConflict, SessionAuthorityIdentity, SessionPermissionMode,
    SupervisorBootstrapReceipt, TaskItem, DEFAULT_SUPERVISOR_SESSION_ID,
};
use tempfile::TempDir;

async fn fixture() -> (SessionStoreV2, TempDir) {
    let home = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    (store, home)
}

fn directory(store: &SessionStoreV2) -> PathBuf {
    store
        .sessions_root_dir()
        .join(DEFAULT_SUPERVISOR_SESSION_ID)
}

async fn bootstrap(store: &SessionStoreV2) -> SupervisorBootstrapReceipt {
    store
        .get_or_create_default_supervisor("initial-model")
        .await
        .unwrap()
}

async fn authority(store: &SessionStoreV2) -> Session {
    store
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap()
}

fn incarnation(session: &Session) -> Uuid {
    match &session.authority_identity {
        SessionAuthorityIdentity::Supervisor { incarnation_id } => *incarnation_id,
        SessionAuthorityIdentity::Ordinary => panic!("expected trusted Supervisor identity"),
    }
}

async fn files(store: &SessionStoreV2) -> (Vec<u8>, Vec<u8>) {
    let path = directory(store);
    (
        fs::read(path.join("session.json")).await.unwrap(),
        fs::read(path.join(RUNTIME_SIDECAR_FILE)).await.unwrap(),
    )
}

async fn save(store: &SessionStoreV2, session: &Session, runtime: bool) -> io::Result<()> {
    if runtime {
        store.save_runtime_state(session).await
    } else {
        store.save_session(session).await
    }
}

async fn reject_without_writes(store: &SessionStoreV2, candidate: &Session) {
    let before = files(store).await;
    for runtime in [false, true] {
        let error = save(store, candidate, runtime).await.unwrap_err();
        assert!(
            error
                .get_ref()
                .is_some_and(|error| error.is::<SessionAuthorityConflict>()),
            "runtime={runtime}"
        );
        assert_eq!(
            files(store).await,
            before,
            "rejected writer changed authoritative files"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn independent_stores_bootstrap_one_incarnation_and_restart_reuses_it() {
    let (first, home) = fixture().await;
    let second = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    let (a, b) = tokio::join!(
        first.get_or_create_default_supervisor("model-a"),
        second.get_or_create_default_supervisor("model-b"),
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_eq!(a.session_id, DEFAULT_SUPERVISOR_SESSION_ID);
    assert_eq!(b.session_id, a.session_id);
    assert_eq!(a.incarnation_id, b.incarnation_id);
    assert_ne!(
        a.created, b.created,
        "exactly one caller created the singleton"
    );
    let current = authority(&first).await;
    assert_eq!(current.kind, SessionKind::Root);
    assert_eq!(current.root_session_id, DEFAULT_SUPERVISOR_SESSION_ID);
    assert!(current.parent_session_id.is_none());
    assert_eq!(current.spawn_depth, 0);
    assert!(current.project_id_meta().is_none());
    assert!(current.messages.is_empty());
    assert!(matches!(current.model.as_str(), "model-a" | "model-b"));
    let before = files(&first).await;
    let restarted = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    let replay = restarted
        .get_or_create_default_supervisor("ignored-new-model")
        .await
        .unwrap();
    assert!(!replay.created);
    assert_eq!(replay.incarnation_id, a.incarnation_id);
    assert_eq!(incarnation(&authority(&restarted).await), a.incarnation_id);
    assert_eq!(
        files(&restarted).await,
        before,
        "replay overwrote creation defaults"
    );
    assert!(restarted
        .get_index_entry(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .is_some());
}

#[tokio::test]
async fn ordinary_reserved_id_is_a_conflict_and_cannot_be_promoted() {
    let (store, _home) = fixture().await;
    let mut ordinary = Session::new(DEFAULT_SUPERVISOR_SESSION_ID, "ordinary-model");
    ordinary.add_message(Message::user("caller-owned history"));
    ordinary.set_project_id_meta("original-project");
    store.save_session(&ordinary).await.unwrap();
    let before = files(&store).await;
    assert_eq!(
        store
            .get_or_create_default_supervisor("requested-model")
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    assert_eq!(files(&store).await, before);
    let persisted = store
        .load_session(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        persisted.authority_identity,
        SessionAuthorityIdentity::Ordinary
    );
    assert_eq!(
        persisted.project_id_meta().as_deref(),
        Some("original-project")
    );
    assert_eq!(persisted.messages[0].content, "caller-owned history");
    fs::remove_file(directory(&store).join(RUNTIME_SIDECAR_FILE))
        .await
        .unwrap();
    assert_eq!(
        store
            .get_or_create_default_supervisor("requested-model")
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    assert!(store
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .is_err());
    assert_eq!(
        fs::read(directory(&store).join("session.json"))
            .await
            .unwrap(),
        before.0
    );
    assert!(!directory(&store).join(RUNTIME_SIDECAR_FILE).exists());
}

#[tokio::test]
async fn child_reserved_id_blocks_bootstrap_despite_missing_or_stale_index() {
    for index_state in ["indexed", "missing", "stale"] {
        let (store, home) = fixture().await;
        let stale = SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap();
        let root = Session::new("other-root", "model");
        store.save_session(&root).await.unwrap();
        let id = DEFAULT_SUPERVISOR_SESSION_ID;
        let mut child = Session::new_child_of(id, &root, "model", "child");
        child.add_message(Message::user("existing child history"));
        store.save_session(&child).await.unwrap();
        let control = store.load_runtime_control_plane(id).await.unwrap().unwrap();
        assert_eq!(control.kind, SessionKind::Child);
        assert!(control.authority_identity.is_ordinary());
        assert!(control.messages.is_empty());
        if index_state == "missing" {
            store
                .update_index(|index| {
                    index.sessions.remove(id);
                    Ok(())
                })
                .await
                .unwrap();
        }
        let child_dir = store
            .sessions_root_dir()
            .join(&root.id)
            .join("children")
            .join(id);
        let paths = [
            child_dir.join("session.json"),
            child_dir.join(RUNTIME_SIDECAR_FILE),
            store.index_path().to_path_buf(),
        ];
        let mut before = Vec::new();
        for path in &paths {
            before.push(fs::read(path).await.unwrap());
        }
        let caller = if index_state == "stale" {
            &stale
        } else {
            &store
        };
        assert_eq!(
            caller
                .get_or_create_default_supervisor("supervisor")
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::AlreadyExists,
            "{index_state}"
        );
        for (path, expected) in paths.iter().zip(before) {
            assert_eq!(fs::read(path).await.unwrap(), expected, "{index_state}");
        }
        assert!(!directory(&store).exists(), "{index_state}");
    }
}

#[tokio::test]
async fn ordinary_save_cannot_forge_supervisor_identity_on_new_or_existing_root() {
    for id in ["forged-supervisor", DEFAULT_SUPERVISOR_SESSION_ID] {
        let (store, _home) = fixture().await;
        let mut candidate = Session::new(id, "model");
        candidate.authority_identity = SessionAuthorityIdentity::Supervisor {
            incarnation_id: Uuid::new_v4(),
        };
        for runtime in [false, true] {
            assert!(save(&store, &candidate, runtime).await.is_err());
            assert!(!store.sessions_root_dir().join(id).exists());
            assert!(store.get_index_entry(id).await.is_none());
        }
    }
    let (store, _home) = fixture().await;
    let mut existing = Session::new("existing-ordinary", "model");
    store.save_session(&existing).await.unwrap();
    let path = store.sessions_root_dir().join(&existing.id);
    let before = fs::read(path.join(RUNTIME_SIDECAR_FILE)).await.unwrap();
    existing.authority_identity = SessionAuthorityIdentity::Supervisor {
        incarnation_id: Uuid::new_v4(),
    };
    for runtime in [false, true] {
        assert!(save(&store, &existing, runtime).await.is_err());
        assert_eq!(
            fs::read(path.join(RUNTIME_SIDECAR_FILE)).await.unwrap(),
            before
        );
    }
}

#[tokio::test]
async fn strict_authority_rejects_missing_corrupt_and_mismatched_sidecars() {
    for fault in [
        "missing",
        "corrupt",
        "id",
        "root",
        "kind",
        "incarnation",
        "ordinary",
    ] {
        let (store, _home) = fixture().await;
        bootstrap(&store).await;
        let path = directory(&store).join(RUNTIME_SIDECAR_FILE);
        let original_main = fs::read(directory(&store).join("session.json"))
            .await
            .unwrap();
        match fault {
            "missing" => fs::remove_file(&path).await.unwrap(),
            "corrupt" => fs::write(&path, b"not valid runtime JSON").await.unwrap(),
            _ => {
                let mut sidecar: serde_json::Value =
                    serde_json::from_slice(&fs::read(&path).await.unwrap()).unwrap();
                match fault {
                    "id" => sidecar["id"] = serde_json::json!("different-root"),
                    "root" => sidecar["root_session_id"] = serde_json::json!("different-root"),
                    "kind" => sidecar["kind"] = serde_json::json!("child"),
                    "incarnation" => {
                        sidecar["authority_identity"]["incarnation_id"] =
                            serde_json::json!(Uuid::new_v4())
                    }
                    "ordinary" => {
                        sidecar["authority_identity"] = serde_json::json!({"kind":"ordinary"})
                    }
                    _ => unreachable!(),
                }
                fs::write(&path, serde_json::to_vec(&sidecar).unwrap())
                    .await
                    .unwrap();
            }
        }
        assert!(
            store
                .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
                .await
                .is_err(),
            "{fault}"
        );
        assert!(
            store
                .get_or_create_default_supervisor("replacement-model")
                .await
                .is_err(),
            "{fault}"
        );
        assert_eq!(
            fs::read(directory(&store).join("session.json"))
                .await
                .unwrap(),
            original_main
        );
    }
}

#[tokio::test]
async fn strict_reads_distinguish_absence_from_legacy_compatibility_fallback() {
    let (store, _home) = fixture().await;
    assert!(store
        .load_root_authority("absent-root")
        .await
        .unwrap()
        .is_none());
    let mut legacy = Session::new("legacy-root", "legacy-model");
    legacy.add_message(Message::user("legacy transcript stays available"));
    store.save_session(&legacy).await.unwrap();
    let path = store
        .sessions_root_dir()
        .join(&legacy.id)
        .join(RUNTIME_SIDECAR_FILE);
    fs::remove_file(&path).await.unwrap();
    assert!(store.load_root_authority(&legacy.id).await.is_err());
    let compatible = store
        .load_runtime_control_plane(&legacy.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        compatible.authority_identity,
        SessionAuthorityIdentity::Ordinary
    );
    assert_eq!(compatible.model, "legacy-model");
    assert_eq!(
        store
            .load_session(&legacy.id)
            .await
            .unwrap()
            .unwrap()
            .messages
            .len(),
        1
    );
    assert_eq!(store.migrate_runtime_sidecars().await.unwrap(), 1);
    assert_eq!(
        store
            .load_root_authority(&legacy.id)
            .await
            .unwrap()
            .unwrap()
            .authority_identity,
        SessionAuthorityIdentity::Ordinary
    );
}

#[tokio::test]
async fn strict_authority_reads_identity_without_deserializing_full_conversation() {
    let (store, _home) = fixture().await;
    let receipt = bootstrap(&store).await;
    let path = directory(&store).join("session.json");
    let mut main: serde_json::Value =
        serde_json::from_slice(&fs::read(&path).await.unwrap()).unwrap();
    main["messages"] = serde_json::json!({"invalid": "transcript shape"});
    fs::write(&path, serde_json::to_vec(&main).unwrap())
        .await
        .unwrap();
    assert!(store
        .load_session(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .is_err());
    let current = authority(&store).await;
    assert_eq!(incarnation(&current), receipt.incarnation_id);
    assert!(current.messages.is_empty());
}

#[tokio::test]
async fn raw_ordinary_writers_must_reload_authority_before_saving_supervisor() {
    let (store, _home) = fixture().await;
    let receipt = bootstrap(&store).await;
    let mut stale = store
        .load_session(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    stale.authority_identity = SessionAuthorityIdentity::Ordinary;
    stale.add_message(Message::user("unbound writer history"));
    stale.set_last_run_status("running");
    reject_without_writes(&store, &stale).await;
    assert!(stale.authority_identity.is_ordinary());
    assert_eq!(
        incarnation(&authority(&store).await),
        receipt.incarnation_id
    );
}

#[tokio::test]
async fn wrong_incarnation_full_and_runtime_writers_fail_without_mutating_files() {
    let (store, _home) = fixture().await;
    bootstrap(&store).await;
    let mut candidate = store
        .load_session(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    candidate.authority_identity = SessionAuthorityIdentity::Supervisor {
        incarnation_id: Uuid::new_v4(),
    };
    candidate.add_message(Message::user("must not enter current history"));
    candidate.set_last_run_status("cancelled");
    reject_without_writes(&store, &candidate).await;
    candidate.spawn_depth = 1;
    reject_without_writes(&store, &candidate).await;
}

#[tokio::test]
async fn deleted_and_recreated_supervisor_rejects_the_previous_incarnation_history() {
    let (store, _home) = fixture().await;
    let first = bootstrap(&store).await;
    let mut old = store
        .load_session(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    old.add_message(Message::user("old incarnation history"));
    store.save_session(&old).await.unwrap();
    assert!(store
        .delete_session(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap());
    let second = bootstrap(&store).await;
    assert!(second.created);
    assert_ne!(second.incarnation_id, first.incarnation_id);
    reject_without_writes(&store, &old).await;
    assert!(store
        .load_session(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap()
        .messages
        .is_empty());
}

#[tokio::test]
async fn copying_supervisor_preserves_conversation_context_but_creates_ordinary_root() {
    let (store, _home) = fixture().await;
    let receipt = bootstrap(&store).await;
    let mut source = store
        .load_session(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    source.title = "Coordinator discussion".into();
    source.set_project_id_meta("project-original");
    source.set_workspace_path_meta("/workspace/original");
    source.agent_runtime_state = Some(bamboo_domain::AgentRuntimeState::default());
    source
        .agent_runtime_state
        .as_mut()
        .unwrap()
        .set_permission_mode(SessionPermissionMode::Auto);
    source.add_message(Message::system("Preserved system context"));
    source.add_message(Message::user("Preserved user context"));
    store.save_session(&source).await.unwrap();
    let before = files(&store).await;
    let copied = store
        .copy_session(&source.id, "ordinary-copy")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        copied.authority_identity,
        SessionAuthorityIdentity::Ordinary
    );
    assert_eq!(copied.kind, SessionKind::Root);
    assert_eq!(copied.root_session_id, "ordinary-copy");
    assert!(copied.parent_session_id.is_none());
    assert_eq!(copied.project_id_meta(), source.project_id_meta());
    assert_eq!(copied.workspace_path_meta(), source.workspace_path_meta());
    assert_eq!(copied.model, source.model);
    assert_eq!(copied.messages.len(), source.messages.len());
    for (copied, original) in copied.messages.iter().zip(&source.messages) {
        assert_eq!(copied.content, original.content);
    }
    assert_eq!(
        copied
            .agent_runtime_state
            .unwrap()
            .effective_permission_mode(),
        SessionPermissionMode::Auto
    );
    assert_eq!(files(&store).await, before);
    assert_eq!(
        incarnation(&authority(&store).await),
        receipt.incarnation_id
    );
}

#[tokio::test]
async fn legacy_sidecar_migration_cannot_recreate_missing_supervisor_authority() {
    let (store, _home) = fixture().await;
    bootstrap(&store).await;
    let path = directory(&store).join(RUNTIME_SIDECAR_FILE);
    fs::remove_file(&path).await.unwrap();
    let before = fs::read(directory(&store).join("session.json"))
        .await
        .unwrap();
    // Skipping or explicitly failing is safe; restoring authority from the
    // conversation snapshot would resurrect a non-authoritative identity.
    let _ = store.migrate_runtime_sidecars().await;
    assert!(!path.exists());
    assert!(store
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .is_err());
    assert_eq!(
        fs::read(directory(&store).join("session.json"))
            .await
            .unwrap(),
        before
    );
}

#[tokio::test]
async fn bootstrap_failure_before_publish_leaves_no_final_identity_and_can_retry() {
    let (store, _home) = fixture().await;
    *store.supervisor_bootstrap_fault.lock().unwrap() =
        Some(supervisor::SupervisorBootstrapFault::BeforePublish);
    assert!(store
        .get_or_create_default_supervisor("model")
        .await
        .is_err());
    assert!(!directory(&store).exists());
    assert!(store
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .is_none());
    assert!(store
        .get_index_entry(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .is_none());
    assert!(bootstrap(&store).await.created);
}

#[tokio::test]
async fn bootstrap_failure_before_index_keeps_complete_identity_and_retry_repairs_index() {
    let (store, home) = fixture().await;
    *store.supervisor_bootstrap_fault.lock().unwrap() =
        Some(supervisor::SupervisorBootstrapFault::BeforeIndex);
    assert!(store
        .get_or_create_default_supervisor("first-model")
        .await
        .is_err());
    let current = authority(&store).await;
    let original_incarnation = incarnation(&current);
    assert_eq!(current.model, "first-model");
    assert!(store
        .get_index_entry(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .is_none());
    let before = files(&store).await;
    let restarted = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    let result = restarted
        .get_or_create_default_supervisor("ignored-model")
        .await
        .unwrap();
    assert!(!result.created);
    assert_eq!(result.incarnation_id, original_incarnation);
    assert_eq!(files(&restarted).await, before);
    assert!(restarted
        .get_index_entry(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .is_some());
}

fn task_seed(mut session: Session) -> Session {
    session.task_list = Some(TaskList {
        session_id: session.id.clone(),
        title: "One task".into(),
        items: vec![TaskItem {
            id: "task-1".into(),
            description: "Original task".into(),
            ..Default::default()
        }],
        created_at: session.created_at,
        updated_at: session.updated_at,
    });
    session.set_task_list_version_meta("1");
    session
}

#[tokio::test]
async fn broken_authority_cannot_be_recovered_by_ordinary_lifecycle_or_task_operations() {
    for missing in [true, false] {
        let (store, _home) = fixture().await;
        let id = DEFAULT_SUPERVISOR_SESSION_ID;
        bootstrap(&store).await;
        let original = task_seed(authority(&store).await);
        store.save_session(&original).await.unwrap();
        let mut updated = original.clone();
        updated.task_list.as_mut().unwrap().items[0].description = "Changed task".into();
        updated.set_task_list_version_meta("2");
        let sidecar = directory(&store).join(RUNTIME_SIDECAR_FILE);
        if missing {
            fs::remove_file(&sidecar).await.unwrap();
        } else {
            fs::write(&sidecar, b"corrupt runtime authority")
                .await
                .unwrap();
        }
        let main_before = fs::read(directory(&store).join("session.json"))
            .await
            .unwrap();
        let side_before = fs::read(&sidecar).await.ok();
        for runtime in [false, true] {
            let error = save(&store, &original, runtime).await.unwrap_err();
            assert!(error
                .get_ref()
                .is_some_and(|error| error.is::<SessionAuthorityConflict>()));
        }
        assert!(store.load_session(id).await.is_err());
        assert!(store.load_runtime_control_plane(id).await.is_err());
        assert!(store.clear_session(id).await.is_err());
        assert!(store.copy_session(id, "bypass-copy").await.is_err());
        assert!(store.recover_root_session_from_disk(id).await.is_err());
        assert!(store
            .save_task_control_plane_if_matches(&original, &updated)
            .await
            .is_err());
        assert_eq!(fs::read(&sidecar).await.ok(), side_before);
        assert_eq!(
            fs::read(directory(&store).join("session.json"))
                .await
                .unwrap(),
            main_before
        );
        assert!(!store.sessions_root_dir().join("bypass-copy").exists());
    }
}

#[tokio::test]
async fn bootstrap_replay_and_index_repair_preserve_existing_message_count() {
    let (store, _home) = fixture().await;
    let id = DEFAULT_SUPERVISOR_SESSION_ID;
    bootstrap(&store).await;
    let mut session = authority(&store).await;
    for index in 0..3 {
        session.add_message(Message::user(format!("history {index}")));
    }
    store.save_session(&session).await.unwrap();
    let before = files(&store).await;
    for remove_index in [false, true] {
        if remove_index {
            store
                .update_index(|index| {
                    index.sessions.remove(id);
                    Ok(())
                })
                .await
                .unwrap();
        }
        assert!(!bootstrap(&store).await.created);
        assert_eq!(store.get_index_entry(id).await.unwrap().message_count, 3);
        assert_eq!(files(&store).await, before);
    }
}

#[tokio::test]
async fn task_cas_rejects_old_incarnation_despite_matching_task_shape_and_generation() {
    let (store, _home) = fixture().await;
    let id = DEFAULT_SUPERVISOR_SESSION_ID;
    bootstrap(&store).await;
    let original = task_seed(authority(&store).await);
    store.save_session(&original).await.unwrap();
    assert!(store.delete_session(id).await.unwrap());
    bootstrap(&store).await;
    let mut replacement = authority(&store).await;
    replacement.task_list = original.task_list.clone();
    replacement.set_task_list_version_meta("1");
    store.save_session(&replacement).await.unwrap();
    assert_ne!(incarnation(&replacement), incarnation(&original));
    let mut updated = original.clone();
    updated.task_list.as_mut().unwrap().items[0].description = "Stale task result".into();
    updated.set_task_list_version_meta("2");
    let before = files(&store).await;
    assert!(!store
        .save_task_control_plane_if_matches(&original, &updated)
        .await
        .unwrap());
    assert_eq!(files(&store).await, before);
}
