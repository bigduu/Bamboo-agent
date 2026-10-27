use bamboo_agent::ticket_cli;
use bamboo_domain::{
    Session, SessionActivationPolicy, SessionAuthorityIdentity, SessionInboxLimits,
    SessionInboxPort, SessionMessageEnvelope, Storage, TaskItemStatus, TaskList,
    DEFAULT_SUPERVISOR_SESSION_ID,
};
use bamboo_engine::{ticket_runtime, ticket_worker_plan::tickets::*};
use bamboo_storage::{FileSessionInbox, SessionStoreV2};
use serde_json::json;

async fn legacy(root: &std::path::Path) -> SessionStoreV2 {
    let storage = SessionStoreV2::new(root.to_path_buf()).await.unwrap();
    storage
        .get_or_create_default_supervisor("fixture-model")
        .await
        .unwrap();
    let mut session = storage
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    session.task_list = Some(serde_json::from_value::<TaskList>(json!({"session_id":session.id,"title":"Legacy tasks","items":[{"id":"old-a","description":"旧已完成任务","status":"completed","notes":"original evidence"},{"id":"old-b","description":"旧待执行任务","status":"pending"}],"created_at":chrono::Utc::now(),"updated_at":chrono::Utc::now()})).unwrap());
    storage.save_session(&session).await.unwrap();
    storage
}

#[tokio::test]
async fn offline_cli_preserves_historical_work_revision_without_restart_reconciliation() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("source");
    let destination = dir.path().join("destination");
    std::fs::create_dir(&data).unwrap();
    std::fs::create_dir(&destination).unwrap();
    let storage = legacy(&data).await;
    let (mut historical, _) =
        ticket_runtime::verified_scope_binding(&storage, DEFAULT_SUPERVISOR_SESSION_ID)
            .await
            .unwrap();
    historical.binding_revision = 7;
    let root = data
        .join("tickets")
        .join(historical.scope_id.strip_prefix("supervisor/").unwrap());
    let original = TicketService::initialize_empty(&root, historical.clone()).unwrap();
    let before = original.published().unwrap();
    drop(original);
    let head = std::fs::read(root.join("HEAD")).unwrap();

    let plan = ticket_cli::migration_plan(&data, &destination, "historical-readonly")
        .await
        .expect("historical Work must remain readable without a writer restart");
    let request: MigrationRequest = serde_json::from_value(plan["request"].clone()).unwrap();
    assert_eq!(request.binding, historical);
    assert_eq!(request.expected_seq, before.1.seq);
    assert_eq!(request.expected_epoch, before.1.authority_epoch);
    assert_eq!(std::fs::read(root.join("HEAD")).unwrap(), head);

    let backup_root = dir.path().join("backup");
    ticket_cli::run(ticket_cli::TicketCommands::Backup {
        data_dir: data.clone(),
        destination: backup_root.clone(),
    })
    .await
    .unwrap();
    let backup = TicketService::open_offline(&backup_root, historical.clone()).unwrap();
    assert_eq!(backup.published().unwrap().1.binding, historical);
    drop(backup);
    assert_eq!(std::fs::read(root.join("HEAD")).unwrap(), head);

    // Reuse the original offline import/receipt path, then migrate the same
    // sealed binding. No Worker, provider, claim or runtime lease is created.
    let preview = ticket_cli::preview(&data, DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap();
    let hash = preview["source_snapshot_hash"].as_str().unwrap();
    let first = ticket_cli::legacy_commit(
        &data,
        DEFAULT_SUPERVISOR_SESSION_ID,
        hash,
        "historical-import",
        &["old-a".into()],
        true,
    )
    .await
    .unwrap();
    let replay = ticket_cli::legacy_commit(
        &data,
        DEFAULT_SUPERVISOR_SESSION_ID,
        hash,
        "historical-import",
        &["old-a".into()],
        true,
    )
    .await
    .unwrap();
    assert_eq!(first["receipt"], replay["receipt"]);
    let plan = ticket_cli::migration_plan(&data, &destination, "historical-migrate")
        .await
        .unwrap();
    let request: MigrationRequest = serde_json::from_value(plan["request"].clone()).unwrap();
    assert_eq!(request.binding, historical);
    let moved = ticket_cli::migrate(&data, &request).await.unwrap();
    let repeated = ticket_cli::migrate(&data, &request).await.unwrap();
    assert_eq!(moved["status"], "migrated");
    assert_eq!(repeated["receipt"], moved["receipt"]);
    let copy = TicketService::open_offline(&request.destination_root, historical.clone()).unwrap();
    let copied = copy.published().unwrap().1;
    assert_eq!(copied.binding, historical);
    assert_eq!(
        serde_json::to_value(&copied.receipts["historical-import"]).unwrap(),
        first["receipt"]
    );
    assert!(copied.assignments.is_empty());
    assert!(copied
        .tickets
        .values()
        .all(|work| work.accepted_submission.is_none()));
}

#[tokio::test]
async fn explicit_attach_preview_full_source_receipt_replay_and_old_completed_needs_review() {
    let dir = tempfile::tempdir().unwrap();
    let storage = legacy(dir.path()).await;
    let preview = ticket_cli::preview(dir.path(), DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap();
    assert_eq!(preview["read_only"], false);
    assert_eq!(preview["task_count"], 2);
    let hash = preview["source_snapshot_hash"].as_str().unwrap();
    let first = ticket_cli::legacy_commit(
        dir.path(),
        DEFAULT_SUPERVISOR_SESSION_ID,
        hash,
        "attach",
        &["old-a".into(), "old-b".into()],
        true,
    )
    .await
    .unwrap();
    let second = ticket_cli::legacy_commit(
        dir.path(),
        DEFAULT_SUPERVISOR_SESSION_ID,
        hash,
        "attach",
        &["old-a".into(), "old-b".into()],
        true,
    )
    .await
    .unwrap();
    assert_eq!(first["receipt"], second["receipt"]);
    assert_eq!(second["replayed"], true);
    assert!(ticket_cli::legacy_commit(
        dir.path(),
        DEFAULT_SUPERVISOR_SESSION_ID,
        hash,
        "attach",
        &["old-a".into()],
        true
    )
    .await
    .is_err());
    let (binding, session) =
        ticket_runtime::verified_scope_binding(&storage, DEFAULT_SUPERVISOR_SESSION_ID)
            .await
            .unwrap();
    assert_eq!(
        session.task_list.as_ref().unwrap().items[0].status,
        TaskItemStatus::Completed
    );
    let SessionAuthorityIdentity::Supervisor {
        incarnation_id: incarnation,
    } = session.authority_identity
    else {
        panic!()
    };
    let service = TicketService::open_offline(
        dir.path().join("tickets").join(incarnation.to_string()),
        binding.clone(),
    )
    .unwrap();
    let snapshot = service.published().unwrap().1;
    assert_eq!(snapshot.schema, 2);
    assert_eq!(snapshot.tickets.len(), 2);
    assert!(snapshot
        .tickets
        .values()
        .all(|w| w.state == WorkState::Blocked
            && w.current_submission.is_none()
            && w.accepted_submission.is_none()));
    let owner = Authority::from_verified_host(
        binding,
        Principal::User {
            user_id: "offline-host-owner".into(),
        },
    );
    let source = snapshot.legacy_attachment.unwrap();
    let bytes = service
        .read_artifact(&owner, source.artifact.as_ref().unwrap(), 1024 * 1024)
        .unwrap();
    assert_eq!(content_hash(&bytes), hash);
    assert!(serde_json::from_slice::<Session>(&bytes)
        .unwrap()
        .task_list
        .is_some());
}

#[tokio::test]
async fn running_legacy_source_preview_is_readonly_and_changed_snapshot_cannot_import() {
    let dir = tempfile::tempdir().unwrap();
    let storage = legacy(dir.path()).await;
    let mut session = storage
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    session.set_last_run_status("running");
    storage.save_session(&session).await.unwrap();
    let preview = ticket_cli::preview(dir.path(), DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap();
    assert_eq!(preview["read_only"], true);
    assert!(ticket_cli::legacy_commit(
        dir.path(),
        DEFAULT_SUPERVISOR_SESSION_ID,
        preview["source_snapshot_hash"].as_str().unwrap(),
        "running-import",
        &["old-a".into()],
        true
    )
    .await
    .is_err());
    assert!(!dir.path().join("tickets").exists());
    let other = tempfile::tempdir().unwrap();
    legacy(other.path()).await;
    assert!(ticket_cli::legacy_commit(
        other.path(),
        DEFAULT_SUPERVISOR_SESSION_ID,
        &"a".repeat(64),
        "wrong-source",
        &["old-a".into()],
        true
    )
    .await
    .is_err());
    assert!(!other.path().join("tickets").exists());
}

#[tokio::test]
async fn interrupted_attach_finishes_root_binding_without_republishing_ticket_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let storage = legacy(dir.path()).await;
    let original = storage
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    let bytes = canonical_bytes(&original).unwrap();
    let hash = content_hash(&bytes);
    let mut attached = original;
    attached.set_root_orchestration_only(true).unwrap();
    let SessionAuthorityIdentity::Supervisor { incarnation_id } = attached.authority_identity
    else {
        panic!()
    };
    let binding = ScopeBinding {
        scope_id: format!("supervisor/{incarnation_id}"),
        supervisor_session_id: attached.id.clone(),
        binding_revision: attached.root_tool_authority_revision,
    };
    let root = dir.path().join("tickets").join(incarnation_id.to_string());
    let service = TicketService::open(&root, binding.clone()).unwrap();
    let artifact = service
        .store_artifact(
            &Authority::from_verified_host(binding.clone(), Principal::Runtime),
            &bytes,
        )
        .unwrap();
    let owner = Authority::from_verified_host(
        binding,
        Principal::User {
            user_id: "offline-host-owner".into(),
        },
    );
    let command = service
        .prepare_command(
            &owner,
            "interrupted-attach",
            vec![Operation::AttachLegacy {
                source: ImportSource {
                    session_id: attached.id,
                    task_id: "_scope_attach".into(),
                    snapshot_hash: hash.clone(),
                    original_state: "explicit_scope_attach".into(),
                    artifact: Some(artifact),
                },
            }],
        )
        .unwrap();
    let receipt = service.execute(&owner, &command).unwrap();
    let frozen_head = service.published().unwrap().0;
    drop(service); // The Ticket commit is durable; the old Root flag is still unchanged.
    let result = ticket_cli::legacy_commit(
        dir.path(),
        DEFAULT_SUPERVISOR_SESSION_ID,
        &hash,
        "interrupted-attach",
        &[],
        true,
    )
    .await
    .unwrap();
    assert_eq!(result["receipt"], serde_json::to_value(receipt).unwrap());
    assert_eq!(result["replayed"], true);
    assert_eq!(
        std::fs::read_to_string(root.join("HEAD")).unwrap(),
        frozen_head
    );
    ticket_runtime::verified_scope_binding(&storage, DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap();
}

#[tokio::test]
async fn offline_migration_accepts_actual_terminal_run_statuses_and_rejects_live_or_unknown() {
    for status in [
        "completed",
        "error",
        "timeout",
        "cancelled",
        "skipped",
        "running",
        "waiting",
        "failed",
        "unknown",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        let destination = dir.path().join("destination");
        std::fs::create_dir(&source).unwrap();
        std::fs::create_dir(&destination).unwrap();
        let storage = legacy(&source).await;
        let preview = ticket_cli::preview(&source, DEFAULT_SUPERVISOR_SESSION_ID)
            .await
            .unwrap();
        ticket_cli::legacy_commit(
            &source,
            DEFAULT_SUPERVISOR_SESSION_ID,
            preview["source_snapshot_hash"].as_str().unwrap(),
            "terminal-attach",
            &["old-a".into()],
            true,
        )
        .await
        .unwrap();
        let mut session = storage
            .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
            .await
            .unwrap()
            .unwrap();
        session.set_last_run_status(status);
        storage.save_session(&session).await.unwrap();
        let terminal = matches!(
            status,
            "completed" | "error" | "timeout" | "cancelled" | "skipped"
        );
        let result = ticket_cli::migration_plan(&source, &destination, "terminal-migrate").await;
        assert_eq!(result.is_ok(), terminal, "{status}: {result:?}");
        if terminal {
            let request: MigrationRequest =
                serde_json::from_value(result.unwrap()["request"].clone()).unwrap();
            let receipt = ticket_cli::migrate(&source, &request).await.unwrap();
            assert_eq!(receipt["status"], "migrated", "{status}");
        }
    }
}

#[tokio::test]
async fn offline_host_migration_preserves_canonical_supervisor_inbox_and_exact_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    let destination = dir.path().join("destination");
    std::fs::create_dir(&source).unwrap();
    std::fs::create_dir(&destination).unwrap();
    let original = std::sync::Arc::new(legacy(&source).await);
    let p = ticket_cli::preview(&source, DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap();
    ticket_cli::legacy_commit(
        &source,
        DEFAULT_SUPERVISOR_SESSION_ID,
        p["source_snapshot_hash"].as_str().unwrap(),
        "attach",
        &["old-a".into()],
        true,
    )
    .await
    .unwrap();
    let message =
        SessionMessageEnvelope::user_input(DEFAULT_SUPERVISOR_SESSION_ID, "迁移保留的原始输入");
    let inbox = FileSessionInbox::new(original, SessionInboxLimits::default());
    let delivered = inbox.deliver(&message).await.unwrap();
    let plan = ticket_cli::migration_plan(&source, &destination, "migrate")
        .await
        .unwrap();
    let request: MigrationRequest = serde_json::from_value(plan["request"].clone()).unwrap();
    let first = ticket_cli::migrate(&source, &request).await.unwrap();
    let second = ticket_cli::migrate(&source, &request).await.unwrap();
    assert_eq!(first["receipt"], second["receipt"]);
    let storage = std::sync::Arc::new(SessionStoreV2::new(destination.clone()).await.unwrap());
    let (binding, session) =
        ticket_runtime::verified_scope_binding(storage.as_ref(), DEFAULT_SUPERVISOR_SESSION_ID)
            .await
            .unwrap();
    assert_eq!(binding, request.binding);
    let restored = FileSessionInbox::new(storage.clone(), SessionInboxLimits::default());
    assert_eq!(
        restored.deliver(&message).await.unwrap(),
        delivered,
        "Inbox duplicate must return original durable receipt"
    );
    let claims = restored
        .claim(DEFAULT_SUPERVISOR_SESSION_ID, 10)
        .await
        .unwrap();
    assert!(
        claims.is_empty(),
        "staged input cannot gain activation permission from migration"
    );
    restored
        .mark_activation_eligible(
            DEFAULT_SUPERVISOR_SESSION_ID,
            delivered.generation,
            SessionActivationPolicy::RespectSpecificWait,
        )
        .await
        .unwrap();
    let activated = restored
        .claim(DEFAULT_SUPERVISOR_SESSION_ID, 10)
        .await
        .unwrap();
    assert_eq!(activated.len(), 1);
    assert_eq!(activated[0].envelope, message);
    assert_eq!(
        session.task_list.unwrap().items[0].status,
        TaskItemStatus::Completed
    );
    let scope = destination.join("tickets").join(
        std::path::Path::new(&request.destination_root)
            .file_name()
            .unwrap(),
    );
    let active = TicketService::open(scope, binding).unwrap();
    assert_eq!(active.health(), Health::Writable);
    assert!(active.published().unwrap().1.authority_epoch > request.expected_epoch);
    assert!(active
        .published()
        .unwrap()
        .1
        .tickets
        .values()
        .all(|w| w.accepted_submission.is_none()));
    let old = TicketService::open(&request.source_root, request.binding.clone()).unwrap();
    assert!(matches!(old.health(), Health::ReadOnly { .. }));
}
