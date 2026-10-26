//! Explicit migration uses only synthetic TempDir sessions and private faults.
use super::*;
use bamboo_domain::{AgentRuntimeState, AgentStatusState, CURRENT_SESSION_VERSION};

fn recovery_hooks() -> &'static std::sync::Mutex<HashMap<PathBuf, Arc<Notify>>> {
    static HOOKS: std::sync::OnceLock<std::sync::Mutex<HashMap<PathBuf, Arc<Notify>>>> =
        std::sync::OnceLock::new();
    HOOKS.get_or_init(Default::default)
}
pub(super) fn lifecycle_attempted(path: &Path) {
    if let Some(reached) = recovery_hooks().lock().unwrap().remove(path) {
        reached.notify_one();
    }
}

async fn fixture(supervisor: bool) -> (tempfile::TempDir, SessionStoreV2, Session) {
    let home = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(home.path().into()).await.unwrap();
    let mut session = if supervisor {
        store
            .get_or_create_default_supervisor("retained-model")
            .await
            .unwrap();
        store
            .load_session(bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID)
            .await
            .unwrap()
            .unwrap()
    } else {
        let mut session = Session::new("migration-root", "retained-model");
        session.session_version = None;
        session
    };
    session.add_message(Message::system("retained system"));
    session.add_message(Message::user("retained user history"));
    session.set_workspace_path_meta("/retained/workspace");
    session.set_provider_name("retained-provider");
    session.agent_runtime_state = Some(AgentRuntimeState::new("finished-run"));
    session.agent_runtime_state.as_mut().unwrap().status = AgentStatusState::Completed;
    store.save_session(&session).await.unwrap();
    if supervisor {
        // The private fixture represents an old producer. No production load
        // or ordinary writer may remove or upgrade the behavior stamp.
        for filename in ["session.json", RUNTIME_SIDECAR_FILE] {
            let path = home
                .path()
                .join("sessions")
                .join(&session.id)
                .join(filename);
            let mut raw: serde_json::Value =
                serde_json::from_slice(&fs::read(&path).await.unwrap()).unwrap();
            raw.as_object_mut()
                .unwrap()
                .remove("_bamboo_main_authority");
            raw.as_object_mut().unwrap().remove("session_version");
            fs::write(path, serde_json::to_vec_pretty(&raw).unwrap())
                .await
                .unwrap();
        }
        session = store.load_session(&session.id).await.unwrap().unwrap();
    }
    (home, store, session)
}

fn request(view: &SessionVersionMigrationObservation) -> SessionVersionMigrationRequest {
    SessionVersionMigrationRequest {
        operation_id: Uuid::new_v4().to_string(),
        birth_token: view.birth_token.clone(),
        expected_session_version: view.session_version,
        expected_metadata_version: view.metadata_version,
        expected_snapshot_sha256: view.snapshot_sha256.clone(),
    }
}

async fn pair(home: &Path, id: &str) -> [Vec<u8>; 2] {
    let directory = home.join("sessions").join(id);
    [
        fs::read(directory.join("session.json")).await.unwrap(),
        fs::read(directory.join(RUNTIME_SIDECAR_FILE))
            .await
            .unwrap(),
    ]
}

#[tokio::test]
async fn version_migration_preserves_ordinary_and_supervisor_state_after_reopen() {
    for supervisor in [false, true] {
        let (home, store, original) = fixture(supervisor).await;
        let view = store
            .inspect_session_version_migration(&original.id)
            .await
            .unwrap()
            .unwrap();
        assert!(view.eligible, "{view:?}");
        assert!(view.session_version.is_none());
        let intent = request(&view);
        let outcome = store
            .migrate_session_version(&original.id, &intent)
            .await
            .unwrap();
        let SessionVersionMigrationDecision::Committed(receipt) = outcome else {
            panic!("{outcome:?}")
        };
        let receipt = *receipt;
        assert_eq!(receipt.target_version, CURRENT_SESSION_VERSION);
        let mut expected = serde_json::to_value(&original).unwrap();
        expected["session_version"] = serde_json::json!(CURRENT_SESSION_VERSION);
        let migrated = store.load_session(&original.id).await.unwrap().unwrap();
        assert_eq!(serde_json::to_value(&migrated).unwrap(), expected);
        assert_eq!(migrated.created_at, original.created_at);
        assert_eq!(migrated.authority_identity, original.authority_identity);
        assert_eq!(
            migrated.supervisor_management,
            original.supervisor_management
        );
        assert_eq!(
            store
                .migrate_session_version(&original.id, &intent)
                .await
                .unwrap(),
            SessionVersionMigrationDecision::Committed(Box::new(receipt.clone()))
        );
        let before_reopen = pair(home.path(), &original.id).await;
        store.flush_search_index().await;
        drop(store);
        let reopened = SessionStoreV2::new(home.path().into()).await.unwrap();
        assert_eq!(
            reopened
                .migrate_session_version(&original.id, &intent)
                .await
                .unwrap(),
            SessionVersionMigrationDecision::Committed(Box::new(receipt))
        );
        assert_eq!(pair(home.path(), &original.id).await, before_reopen);
        let mut stale = original.clone();
        stale.add_message(Message::user(
            "a stale writer must not downgrade the migration",
        ));
        assert!(reopened
            .save_session(&stale)
            .await
            .unwrap_err()
            .to_string()
            .contains("session_version_change_requires_new_session"));
        reopened.flush_search_index().await;
    }
}

#[tokio::test]
async fn version_migration_preconditions_and_busy_refusals_do_not_write_source() {
    let (home, store, original) = fixture(false).await;
    let view = store
        .inspect_session_version_migration(&original.id)
        .await
        .unwrap()
        .unwrap();
    let intent = request(&view);
    let original_pair = pair(home.path(), &original.id).await;
    for modified in [
        SessionVersionMigrationRequest {
            birth_token: "0".repeat(64),
            ..intent.clone()
        },
        SessionVersionMigrationRequest {
            expected_metadata_version: view.metadata_version + 1,
            ..intent.clone()
        },
        SessionVersionMigrationRequest {
            expected_snapshot_sha256: "0".repeat(64),
            ..intent.clone()
        },
    ] {
        assert_eq!(
            store
                .migrate_session_version(&original.id, &modified)
                .await
                .unwrap(),
            SessionVersionMigrationDecision::PreconditionFailed
        );
        assert_eq!(pair(home.path(), &original.id).await, original_pair);
    }
    let mut busy = original.clone();
    busy.agent_runtime_state.as_mut().unwrap().status = AgentStatusState::Running;
    store.save_session(&busy).await.unwrap();
    let view = store
        .inspect_session_version_migration(&original.id)
        .await
        .unwrap()
        .unwrap();
    assert!(!view.eligible);
    let before = pair(home.path(), &original.id).await;
    assert!(matches!(
        store
            .migrate_session_version(&original.id, &request(&view))
            .await
            .unwrap(),
        SessionVersionMigrationDecision::Blocked(_)
    ));
    assert_eq!(pair(home.path(), &original.id).await, before);
    assert!(store
        .session_version_migration_journals()
        .await
        .unwrap()
        .is_empty());
    store.flush_search_index().await;
}

#[tokio::test]
async fn version_migration_unknown_schema_and_pending_inbox_remain_untouched() {
    let (home, store, original) = fixture(false).await;
    let directory = home.path().join("sessions").join(&original.id);
    let queued = directory.join("inbox/new");
    fs::create_dir_all(&queued).await.unwrap();
    fs::write(queued.join("unconsumed-entry"), b"not yet admitted")
        .await
        .unwrap();
    let view = store
        .inspect_session_version_migration(&original.id)
        .await
        .unwrap()
        .unwrap();
    assert!(!view.eligible);
    assert!(view.blockers.iter().any(|b| b.code == "inbox_not_empty"));
    let before = pair(home.path(), &original.id).await;
    assert!(matches!(
        store
            .migrate_session_version(&original.id, &request(&view))
            .await
            .unwrap(),
        SessionVersionMigrationDecision::Blocked(_)
    ));
    assert_eq!(pair(home.path(), &original.id).await, before);
    fs::remove_file(queued.join("unconsumed-entry"))
        .await
        .unwrap();
    for filename in ["session.json", RUNTIME_SIDECAR_FILE] {
        let path = directory.join(filename);
        let mut value: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).await.unwrap()).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .remove("_bamboo_main_authority");
        value["session_version"] = serde_json::json!(99);
        fs::write(path, serde_json::to_vec_pretty(&value).unwrap())
            .await
            .unwrap();
    }
    let view = store
        .inspect_session_version_migration(&original.id)
        .await
        .unwrap()
        .unwrap();
    assert!(!view.eligible);
    assert!(view
        .blockers
        .iter()
        .any(|b| b.code == "session_execution_incompatible"));
    let before = pair(home.path(), &original.id).await;
    assert!(matches!(
        store
            .migrate_session_version(&original.id, &request(&view))
            .await
            .unwrap(),
        SessionVersionMigrationDecision::Blocked(_)
    ));
    assert_eq!(pair(home.path(), &original.id).await, before);
    store.flush_search_index().await;
}

#[tokio::test]
async fn version_migration_cleanup_loss_cannot_reblock_later_commits() {
    cleanup_loss_regression().await;
}

pub(super) async fn cleanup_loss_regression() {
    use super::session_version_migration::{is_published_migration_journal, MigrationFault};
    use bamboo_domain::{ActorSnapshotLimits, ActorSnapshotPort, ActorSnapshotPrincipal};

    let (home, store, original) = fixture(false).await;
    let intent = request(
        &store
            .inspect_session_version_migration(&original.id)
            .await
            .unwrap()
            .unwrap(),
    );
    *store.session_version_migration_fault.lock().unwrap() = Some(MigrationFault::Receipt);
    assert!(store
        .migrate_session_version(&original.id, &intent)
        .await
        .is_err());
    let active = store
        .session_version_migration_journals()
        .await
        .unwrap()
        .remove(0);
    let journal = fs::read(&active).await.unwrap();
    *store.session_version_migration_fault.lock().unwrap() = Some(MigrationFault::Deactivated);
    assert!(store
        .migrate_session_version(&original.id, &intent)
        .await
        .is_err());
    let mut entries = fs::read_dir(active.parent().unwrap()).await.unwrap();
    // Capture the actual cleanup destination. The old unlink-only path has no
    // destination: losing its unlink revives the original active .json name.
    let cleanup = entries
        .next_entry()
        .await
        .unwrap()
        .map_or_else(|| active.clone(), |entry| entry.path());
    if cleanup.exists() {
        assert_eq!(fs::read(&cleanup).await.unwrap(), journal);
        fs::remove_file(&cleanup).await.unwrap();
    }
    let mut later = store.load_session(&original.id).await.unwrap().unwrap();
    later.add_message(Message::user("valid history after migration committed"));
    store.save_session(&later).await.unwrap();
    let committed = pair(home.path(), &original.id).await;
    store.flush_search_index().await;
    drop(store);

    // Namespace fault model, not a native power-loss claim: resurrect exactly
    // the name and bytes left by production cleanup after a later legal save.
    fs::write(&cleanup, &journal).await.unwrap();
    let reopened = SessionStoreV2::new(home.path().into()).await.unwrap();
    assert!(!is_published_migration_journal(&cleanup));
    assert_eq!(pair(home.path(), &original.id).await, committed);
    assert_eq!(
        serde_json::to_value(
            reopened
                .load_session(&original.id)
                .await
                .unwrap()
                .unwrap()
                .messages
        )
        .unwrap(),
        serde_json::to_value(&later.messages).unwrap()
    );
    assert!(matches!(
        reopened
            .migrate_session_version(&original.id, &intent)
            .await
            .unwrap(),
        SessionVersionMigrationDecision::Committed(_)
    ));
    reopened
        .actor_subtree_snapshot(
            ActorSnapshotPrincipal::host_owner(),
            &original.id,
            &original.id,
            ActorSnapshotLimits::default(),
        )
        .await
        .unwrap();
    reopened.actor_tree_cursor(&original.id).await.unwrap();
    assert_eq!(pair(home.path(), &original.id).await, committed);
    assert_eq!(fs::read(cleanup).await.unwrap(), journal);
    reopened.flush_search_index().await;
}

#[tokio::test]
async fn version_migration_recovers_each_pair_publication_boundary_after_restart() {
    use super::session_version_migration::MigrationFault;
    for fault in [
        MigrationFault::Prepared,
        MigrationFault::Runtime,
        MigrationFault::Main,
        MigrationFault::Receipt,
    ] {
        let (home, store, original) = fixture(true).await;
        let view = store
            .inspect_session_version_migration(&original.id)
            .await
            .unwrap()
            .unwrap();
        let intent = request(&view);
        let initial = pair(home.path(), &original.id).await;
        *store.session_version_migration_fault.lock().unwrap() = Some(fault);
        assert!(store
            .migrate_session_version(&original.id, &intent)
            .await
            .is_err());
        assert!(
            store.load_session(&original.id).await.is_err(),
            "ordinary loads must not observe an unconfirmed pair"
        );
        let pending = store
            .inspect_session_version_migration(&original.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pending.pending_operation.as_ref(), Some(&intent));
        let interrupted = pair(home.path(), &original.id).await;
        if fault == MigrationFault::Prepared {
            assert_eq!(interrupted, initial);
        }
        let changed = SessionVersionMigrationRequest {
            expected_metadata_version: intent.expected_metadata_version + 1,
            ..intent.clone()
        };
        assert_eq!(
            store
                .migrate_session_version(&original.id, &changed)
                .await
                .unwrap(),
            SessionVersionMigrationDecision::OperationConflict
        );
        assert_eq!(pair(home.path(), &original.id).await, interrupted);
        store.flush_search_index().await;
        drop(store);
        let reopened = SessionStoreV2::new(home.path().into()).await.unwrap();
        let receipt = reopened
            .migrate_session_version(&original.id, &intent)
            .await
            .unwrap();
        assert!(matches!(
            receipt,
            SessionVersionMigrationDecision::Committed(_)
        ));
        assert_eq!(
            reopened
                .load_session(&original.id)
                .await
                .unwrap()
                .unwrap()
                .session_version,
            Some(CURRENT_SESSION_VERSION)
        );
        assert!(reopened
            .session_version_migration_journals()
            .await
            .unwrap()
            .is_empty());
        reopened.flush_search_index().await;
    }
}

#[tokio::test]
async fn version_migration_unpublished_staging_never_blocks_actor_views_after_restart_or_retry() {
    use bamboo_domain::{
        ActorSnapshotError, ActorSnapshotLimits, ActorSnapshotPort, ActorSnapshotPrincipal,
    };
    async fn readable(store: &SessionStoreV2, id: &str) {
        store
            .actor_subtree_snapshot(
                ActorSnapshotPrincipal::host_owner(),
                id,
                id,
                ActorSnapshotLimits::default(),
            )
            .await
            .unwrap();
        store.actor_tree_cursor(id).await.unwrap();
    }
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().canonicalize().unwrap();
    let store = SessionStoreV2::new(home.clone()).await.unwrap();
    let mut root = Session::new("orphan-migration-root", "model");
    root.session_version = None;
    store.save_session(&root).await.unwrap();
    let intent = request(
        &store
            .inspect_session_version_migration(&root.id)
            .await
            .unwrap()
            .unwrap(),
    );
    let directory = home.join(super::session_version_migration::MIGRATION_DIR);
    let unpublished = directory.join(format!("{}.json", intent.operation_id));
    // Interrupt the real durable primitive after its staging file is synced,
    // before publication; unwinding simulates a process stop without cleanup.
    assert!(tokio::task::spawn_blocking(move || {
        durable_atomic_write_blocking(&unpublished, b"unpublished migration staging", |phase| {
            assert_ne!(
                phase,
                DurableWritePhase::BeforeReplace,
                "simulated process stop"
            );
            Ok(())
        })
    })
    .await
    .unwrap_err()
    .is_panic());
    let orphan = fs::read_dir(&directory)
        .await
        .unwrap()
        .next_entry()
        .await
        .unwrap()
        .unwrap()
        .path();
    assert!(orphan
        .file_name()
        .unwrap()
        .to_string_lossy()
        .contains(".durable.tmp."));
    let initial = pair(&home, &root.id).await;
    readable(&store, &root.id).await;
    assert_eq!(pair(&home, &root.id).await, initial);
    store.flush_search_index().await;
    drop(store);
    let store = SessionStoreV2::new(home.clone()).await.unwrap();
    readable(&store, &root.id).await;
    assert_eq!(pair(&home, &root.id).await, initial);
    *store.session_version_migration_fault.lock().unwrap() =
        Some(super::session_version_migration::MigrationFault::Prepared);
    assert!(store
        .migrate_session_version(&root.id, &intent)
        .await
        .is_err());
    let journals = store.session_version_migration_journals().await.unwrap();
    assert_eq!(journals.len(), 1);
    let journal = fs::read(&journals[0]).await.unwrap();
    assert_eq!(
        store
            .actor_subtree_snapshot(
                ActorSnapshotPrincipal::host_owner(),
                &root.id,
                &root.id,
                ActorSnapshotLimits::default()
            )
            .await
            .unwrap_err(),
        ActorSnapshotError::PendingTransaction
    );
    assert_eq!(
        store.actor_tree_cursor(&root.id).await.unwrap_err(),
        ActorSnapshotError::PendingTransaction
    );
    assert_eq!(fs::read(&journals[0]).await.unwrap(), journal);
    assert_eq!(pair(&home, &root.id).await, initial);
    assert!(matches!(
        store
            .migrate_session_version(&root.id, &intent)
            .await
            .unwrap(),
        SessionVersionMigrationDecision::Committed(_)
    ));
    readable(&store, &root.id).await;
    let committed = pair(&home, &root.id).await;
    store.flush_search_index().await;
    drop(store);
    let reopened = SessionStoreV2::new(home.clone()).await.unwrap();
    assert!(matches!(
        reopened
            .migrate_session_version(&root.id, &intent)
            .await
            .unwrap(),
        SessionVersionMigrationDecision::Committed(_)
    ));
    readable(&reopened, &root.id).await;
    assert_eq!(pair(&home, &root.id).await, committed);
    assert!(reopened
        .session_version_migration_journals()
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        fs::read(orphan).await.unwrap(),
        b"unpublished migration staging"
    );
    reopened.flush_search_index().await;
}

#[tokio::test]
async fn version_migration_recovery_never_overwrites_unexpected_canonical_bytes() {
    let (home, store, original) = fixture(false).await;
    let view = store
        .inspect_session_version_migration(&original.id)
        .await
        .unwrap()
        .unwrap();
    let intent = request(&view);
    *store.session_version_migration_fault.lock().unwrap() =
        Some(super::session_version_migration::MigrationFault::Runtime);
    assert!(store
        .migrate_session_version(&original.id, &intent)
        .await
        .is_err());
    let main = home
        .path()
        .join("sessions")
        .join(&original.id)
        .join("session.json");
    let mut bytes = fs::read(&main).await.unwrap();
    bytes.extend_from_slice(b" \n");
    fs::write(&main, bytes).await.unwrap();
    let changed = pair(home.path(), &original.id).await;
    assert!(store
        .migrate_session_version(&original.id, &intent)
        .await
        .is_err());
    assert_eq!(pair(home.path(), &original.id).await, changed);
    store.flush_search_index().await;
    drop(store);
    assert!(SessionStoreV2::new(home.path().into()).await.is_err());
    assert_eq!(pair(home.path(), &original.id).await, changed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn version_migration_concurrent_constructor_waits_for_actual_publication() {
    use super::default_actor_context_tests::DefaultWriteHook;
    struct Release(Arc<DefaultWriteHook>);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.release();
        }
    }
    let (home, store, original) = fixture(false).await;
    let store = Arc::new(store);
    let view = store
        .inspect_session_version_migration(&original.id)
        .await
        .unwrap()
        .unwrap();
    let intent = request(&view);
    let hook = DefaultWriteHook::install(
        &store,
        RUNTIME_SIDECAR_FILE,
        DurableWritePhase::BeforeReplace,
        false,
    );
    let _release = Release(hook.clone());
    let migrate = {
        let store = store.clone();
        let id = original.id.clone();
        tokio::spawn(async move { store.migrate_session_version(&id, &intent).await })
    };
    hook.wait();
    assert_eq!(
        store
            .session_version_migration_journals()
            .await
            .unwrap()
            .len(),
        1
    );
    let attempted = Arc::new(Notify::new());
    recovery_hooks().lock().unwrap().insert(
        home.path().join(SESSION_LIFECYCLE_LOCK_FILE),
        attempted.clone(),
    );
    let directory = home.path().to_path_buf();
    let constructor = tokio::spawn(async move { SessionStoreV2::new(directory).await });
    tokio::time::timeout(Duration::from_secs(10), attempted.notified())
        .await
        .unwrap();
    assert!(
        !constructor.is_finished(),
        "the real lifecycle gate must exclude construction during publication"
    );
    hook.release();
    assert!(matches!(
        migrate.await.unwrap().unwrap(),
        SessionVersionMigrationDecision::Committed(_)
    ));
    let reopened = tokio::time::timeout(Duration::from_secs(10), constructor)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        reopened
            .load_session(&original.id)
            .await
            .unwrap()
            .unwrap()
            .session_version,
        Some(CURRENT_SESSION_VERSION)
    );
    assert!(reopened
        .session_version_migration_journals()
        .await
        .unwrap()
        .is_empty());
    reopened.flush_search_index().await;
    store.flush_search_index().await;
}

#[tokio::test]
async fn version_migration_refuses_idle_child_with_queued_work_without_writes() {
    let (home, store, parent) = fixture(false).await;
    let child = Session::new_child_of("queued-child", &parent, "child-model", "worker");
    store.save_session(&child).await.unwrap();
    let child_directory = home
        .path()
        .join("sessions")
        .join(&parent.id)
        .join("children")
        .join(&child.id);
    let queue = child_directory.join("inbox/new");
    fs::create_dir_all(&queue).await.unwrap();
    fs::write(
        queue.join("admitted-awaiting-activation"),
        b"retained queued work",
    )
    .await
    .unwrap();
    let parent_before = pair(home.path(), &parent.id).await;
    let child_before = [
        fs::read(child_directory.join("session.json"))
            .await
            .unwrap(),
        fs::read(child_directory.join(RUNTIME_SIDECAR_FILE))
            .await
            .unwrap(),
    ];
    let view = store
        .inspect_session_version_migration(&parent.id)
        .await
        .unwrap()
        .unwrap();
    assert!(!view.eligible);
    assert!(view
        .blockers
        .iter()
        .any(|reason| reason.code == "child_execution_pending"));
    assert!(matches!(
        store
            .migrate_session_version(&parent.id, &request(&view))
            .await
            .unwrap(),
        SessionVersionMigrationDecision::Blocked(_)
    ));
    assert_eq!(pair(home.path(), &parent.id).await, parent_before);
    assert_eq!(
        [
            fs::read(child_directory.join("session.json"))
                .await
                .unwrap(),
            fs::read(child_directory.join(RUNTIME_SIDECAR_FILE))
                .await
                .unwrap()
        ],
        child_before
    );
    assert!(queue.join("admitted-awaiting-activation").exists());
    assert!(store
        .session_version_migration_journals()
        .await
        .unwrap()
        .is_empty());
    store.flush_search_index().await;
}

#[tokio::test]
async fn version_migration_refuses_pending_legacy_control_planes_for_root_and_child() {
    let mut running = AgentRuntimeState::new("legacy-active");
    running.status = AgentStatusState::Running;
    let legacy_running = serde_json::to_string(&running).unwrap();
    let mut unknown = running;
    unknown.status = AgentStatusState::Completed;
    unknown.version = 99;
    let legacy_unknown = serde_json::to_string(&unknown).unwrap();
    for child in [false, true] {
        let (home, store, root) = fixture(false).await;
        let target = if child {
            let child = Session::new_child_of("legacy-child", &root, "model", "worker");
            store.save_session(&child).await.unwrap();
            child
        } else {
            root.clone()
        };
        let directory = store.abs_path_from_rel(&store.resolve_rel_path(&target.id).await.unwrap());
        let path = directory.join(RUNTIME_SIDECAR_FILE);
        let original = fs::read(&path).await.unwrap();
        let main = fs::read(directory.join("session.json")).await.unwrap();
        for (key, value) in [
            ("agent.runtime.state", legacy_running.as_str()),
            ("agent.runtime.state", legacy_unknown.as_str()),
            ("agent.runtime.state", "malformed legacy runtime"),
            ("pending_injected_messages", "malformed accepted input"),
            ("typed_pending_injected_messages", ""),
            ("clarification_resume_pending", "true"),
            ("conclusion_with_options_resume_pending", "true"),
            ("permission.replay_save_recovery_required", ""),
        ] {
            let mut raw: serde_json::Value = serde_json::from_slice(&original).unwrap();
            raw["agent_runtime_state"] = serde_json::Value::Null;
            raw["runtime_metadata"] = serde_json::Value::Null;
            if key == "typed_pending_injected_messages" {
                raw["runtime_metadata"] =
                    serde_json::json!({"pending_injected_messages":[{"content":"accepted"}]});
            } else {
                raw["metadata"][key] = serde_json::json!(value);
            }
            fs::write(&path, serde_json::to_vec_pretty(&raw).unwrap())
                .await
                .unwrap();
            let root_before = pair(home.path(), &root.id).await;
            let target_before = fs::read(&path).await.unwrap();
            let view = store
                .inspect_session_version_migration(&root.id)
                .await
                .unwrap()
                .unwrap();
            assert!(!view.eligible, "child={child}, key={key}");
            let code = if child {
                "child_execution_pending"
            } else if key == "agent.runtime.state" {
                "execution_not_idle"
            } else {
                "pending_response"
            };
            assert!(
                view.blockers.iter().any(|reason| reason.code == code),
                "{view:?}"
            );
            assert!(matches!(
                store
                    .migrate_session_version(&root.id, &request(&view))
                    .await
                    .unwrap(),
                SessionVersionMigrationDecision::Blocked(_)
            ));
            assert_eq!(pair(home.path(), &root.id).await, root_before);
            assert_eq!(fs::read(&path).await.unwrap(), target_before);
            assert_eq!(
                fs::read(directory.join("session.json")).await.unwrap(),
                main
            );
            assert!(store
                .session_version_migration_journals()
                .await
                .unwrap()
                .is_empty());
        }
        store.flush_search_index().await;
    }
}

#[tokio::test]
async fn version_migration_pending_pair_refuses_both_public_task_paths_without_writes() {
    async fn task_pair_bytes(store: &SessionStoreV2) -> Vec<Vec<u8>> {
        let mut bytes = Vec::new();
        for id in ["tx-child", "tx-root"] {
            let directory = store.abs_path_from_rel(&store.resolve_rel_path(id).await.unwrap());
            for name in ["session.json", RUNTIME_SIDECAR_FILE] {
                bytes.push(fs::read(directory.join(name)).await.unwrap());
            }
        }
        bytes
    }
    let home = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(home.path().into()).await.unwrap();
    let (_, child_updated, root, root_updated) =
        super::tests::seed_runtime_task_transaction_pair(&store)
            .await
            .unwrap();
    let directory = home.path().join("sessions").join(&root.id);
    for filename in ["session.json", RUNTIME_SIDECAR_FILE] {
        let path = directory.join(filename);
        let mut raw: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).await.unwrap()).unwrap();
        raw.as_object_mut()
            .unwrap()
            .remove("_bamboo_main_authority");
        raw.as_object_mut().unwrap().remove("session_version");
        fs::write(path, serde_json::to_vec_pretty(&raw).unwrap())
            .await
            .unwrap();
    }
    let child = store
        .load_runtime_control_plane("tx-child")
        .await
        .unwrap()
        .unwrap();
    let root = store
        .load_runtime_control_plane("tx-root")
        .await
        .unwrap()
        .unwrap();
    let mut updated = root.clone();
    updated.task_list = root_updated.task_list;
    updated.set_task_list_version_meta("2");
    let view = store
        .inspect_session_version_migration(&root.id)
        .await
        .unwrap()
        .unwrap();
    assert!(view.eligible, "{view:?}");
    *store.session_version_migration_fault.lock().unwrap() =
        Some(super::session_version_migration::MigrationFault::Prepared);
    assert!(store
        .migrate_session_version(&root.id, &request(&view))
        .await
        .is_err());
    let before = task_pair_bytes(&store).await;
    let journal_paths = store.session_version_migration_journals().await.unwrap();
    let journal = fs::read(&journal_paths[0]).await.unwrap();
    store.take_runtime_task_durability_events();
    assert!(store
        .recover_task_control_plane_transaction(&child.id, &root.id)
        .await
        .unwrap_err()
        .to_string()
        .contains("migration"));
    assert!(store
        .save_task_control_planes_atomically(&child, &child_updated, &root, &updated)
        .await
        .unwrap_err()
        .to_string()
        .contains("migration"));
    let task_guard = store
        .lock_runtime_task_transaction_exclusive()
        .await
        .unwrap();
    assert!(store
        .write_existing_runtime_sidecar_durable_unchecked(
            &root,
            RuntimeTaskDurabilityEvent::FirstUpdatedSidecarPublished,
            &task_guard
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("migration"));
    drop(task_guard);
    assert_eq!(task_pair_bytes(&store).await, before);
    assert_eq!(fs::read(&journal_paths[0]).await.unwrap(), journal);
    assert!(store.take_runtime_task_durability_events().is_empty());
    assert!(store.runtime_task_journal_paths().await.unwrap().is_empty());
    assert_eq!(
        store
            .session_version_migration_journals()
            .await
            .unwrap()
            .len(),
        1
    );
    store.flush_search_index().await;
}

#[tokio::test]
async fn version_migration_preserves_raw_unknown_numbers_and_frozen_argument_bytes() {
    let (home, store, mut source) = fixture(false).await;
    let arguments = r#"{"number":18446744073709551617,"escaped":"\u4e2d"}"#;
    let mut call = Message::assistant("completed tool request", None);
    call.tool_calls = Some(vec![bamboo_domain::ToolCall {
        id: "frozen-call".into(),
        tool_type: "function".into(),
        function: bamboo_domain::FunctionCall {
            name: "Read".into(),
            arguments: arguments.into(),
        },
    }]);
    source.add_message(call);
    source.add_message(Message::tool_result("frozen-call", "completed"));
    store.save_session(&source).await.unwrap();
    let directory = home.path().join("sessions").join(&source.id);
    for filename in ["session.json", RUNTIME_SIDECAR_FILE] {
        let path = directory.join(filename);
        let mut bytes = fs::read(&path).await.unwrap();
        assert_eq!(bytes.pop(), Some(b'}'));
        bytes.extend_from_slice(br#", "retained_future":{"integer":18446744073709551617,"decimal":1.2345678901234567890123456789}}"#);
        fs::write(path, bytes).await.unwrap();
    }
    let before = pair(home.path(), &source.id).await;
    let view = store
        .inspect_session_version_migration(&source.id)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        store
            .migrate_session_version(&source.id, &request(&view))
            .await
            .unwrap(),
        SessionVersionMigrationDecision::Committed(_)
    ));
    let after = pair(home.path(), &source.id).await;
    for (before, after) in before.iter().zip(&after) {
        let original: std::collections::BTreeMap<String, Box<serde_json::value::RawValue>> =
            serde_json::from_slice(before).unwrap();
        let migrated: std::collections::BTreeMap<String, Box<serde_json::value::RawValue>> =
            serde_json::from_slice(after).unwrap();
        for field in ["retained_future", "messages"] {
            assert_eq!(migrated[field].get(), original[field].get());
        }
    }
    let length =
        super::compact_main::section_length(&before[0][..super::compact_main::HEADER_BYTES])
            .unwrap();
    assert_eq!(&before[0][..length], &after[0][..length]);
    assert_eq!(
        store
            .load_session(&source.id)
            .await
            .unwrap()
            .unwrap()
            .messages[2]
            .tool_calls
            .as_ref()
            .unwrap()[0]
            .function
            .arguments,
        arguments
    );
    store.flush_search_index().await;
}

#[tokio::test]
async fn version_migration_refuses_transformed_main_over_real_byte_bound_before_journaling() {
    use bamboo_domain::{ActorSnapshotLimits, ActorSnapshotPort, ActorSnapshotPrincipal};
    const LIMIT: usize = 64 * 1024 * 1024;
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().canonicalize().unwrap();
    let store = SessionStoreV2::new(home.clone()).await.unwrap();
    let mut legacy = Session::new("near-limit-legacy", "model");
    legacy.session_version = None;
    store.save_session(&legacy).await.unwrap();
    let healthy = Session::new("unrelated-healthy-root", "model");
    store.save_session(&healthy).await.unwrap();
    let directory = home.join("sessions").join(&legacy.id);
    let main_path = directory.join("session.json");
    let runtime_path = directory.join(RUNTIME_SIDECAR_FILE);
    let mut raw: serde_json::Value =
        serde_json::from_slice(&fs::read(&main_path).await.unwrap()).unwrap();
    raw.as_object_mut()
        .unwrap()
        .remove("_bamboo_main_authority");
    raw.as_object_mut().unwrap().remove("session_version");
    let mut main = serde_json::to_vec(&raw).unwrap();
    assert_eq!(main.pop(), Some(b'}'));
    main.extend_from_slice(b",\"retained_padding\":\"");
    main.resize(LIMIT - 2, b'x');
    main.extend_from_slice(b"\"}");
    // A valid compact legacy Main exactly at the real 64 MiB bound cannot
    // accommodate the new stamp or pretty-printed top-level separators.
    assert_eq!(main.len(), LIMIT);
    fs::write(&main_path, &main).await.unwrap();
    let runtime = fs::read(&runtime_path).await.unwrap();
    let view = store
        .inspect_session_version_migration(&legacy.id)
        .await
        .unwrap()
        .unwrap();
    assert!(view.eligible, "{view:?}");
    assert_eq!(view.session_version, None);
    let error = store
        .migrate_session_version(&legacy.id, &request(&view))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(fs::read(&main_path).await.unwrap(), main);
    assert_eq!(fs::read(&runtime_path).await.unwrap(), runtime);
    assert!(
        store
            .session_version_migration_journals()
            .await
            .unwrap()
            .is_empty(),
        "an unrecoverable transformed pair must never publish its journal"
    );
    assert!(
        fs::read_dir(home.join(super::session_version_migration::MIGRATION_DIR))
            .await
            .unwrap()
            .next_entry()
            .await
            .unwrap()
            .is_none()
    );
    store.flush_search_index().await;
    drop(store);
    let reopened = SessionStoreV2::new(home).await.unwrap();
    assert_eq!(fs::read(&main_path).await.unwrap(), main);
    assert_eq!(fs::read(&runtime_path).await.unwrap(), runtime);
    reopened
        .actor_subtree_snapshot(
            ActorSnapshotPrincipal::host_owner(),
            &healthy.id,
            &healthy.id,
            ActorSnapshotLimits::default(),
        )
        .await
        .unwrap();
    reopened.actor_tree_cursor(&healthy.id).await.unwrap();
    reopened.flush_search_index().await;
}
