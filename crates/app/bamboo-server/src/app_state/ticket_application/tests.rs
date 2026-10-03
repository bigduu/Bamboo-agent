use super::*;
use bamboo_storage::SessionStoreV2;
use std::collections::BTreeSet;

fn config(enabled: bool) -> Arc<RwLock<Config>> {
    Arc::new(RwLock::new(
        serde_json::from_value(json!({
            "provider":"openai", "features":{"ticket_mutation":enabled},
            "providers":{"openai":{"api_key":"fixture","model":"fixture-model"}}
        }))
        .unwrap(),
    ))
}

fn contract() -> Contract {
    Contract {
        title: "work".into(),
        objective: "one result".into(),
        constraints: vec!["bounded".into()],
        acceptance: vec!["explicit acceptance".into()],
        user_acceptance_required: true,
        allowed_tools: BTreeSet::from(["Task".into()]),
    }
}

#[tokio::test]
async fn ticket_application_flags_preserve_records_and_never_double_write_old_tasks() {
    let root = tempfile::tempdir().unwrap();
    let storage = Arc::new(SessionStoreV2::new(root.path().join("host")).await.unwrap());
    let config = config(true);
    let app = TicketApplication::open(root.path(), storage.clone(), config.clone()).await;
    assert_eq!(app.status().await["available"], true);
    let user = Principal::User {
        user_id: "host-owner".into(),
    };
    let (service, authority) = app.authority(user.clone()).await.unwrap();
    let command = service
        .prepare_command(
            &authority,
            "create",
            vec![Operation::Create {
                temp_id: "work".into(),
                kind: TicketKind::Work,
                parent: None,
                contract: contract(),
                depends_on: BTreeSet::new(),
            }],
        )
        .unwrap();
    let receipt = app.update(user.clone(), &command).await.unwrap();
    let canonical = storage
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    assert!(canonical.root_orchestration_only_enabled());
    assert!(canonical.task_list.is_none());
    config.write().await.features.ticket_mutation = false;
    assert!(app.update(user, &command).await.is_err());
    assert_eq!(
        service.work_overview(&authority).unwrap().data.work_count,
        1
    );
    drop(service);
    drop(app);
    let reopened = TicketApplication::open(root.path(), storage, config).await;
    assert_eq!(reopened.status().await["available"], true);
    assert_eq!(reopened.status().await["mutation_enabled"], false);
    assert!(reopened
        .service()
        .unwrap()
        .published()
        .unwrap()
        .1
        .tickets
        .contains_key(&receipt.ids["work"]));
}

#[tokio::test]
async fn ticket_application_default_off_does_not_bootstrap_or_attach_existing_supervisor() {
    let root = tempfile::tempdir().unwrap();
    let storage = Arc::new(SessionStoreV2::new(root.path().join("host")).await.unwrap());
    let app = TicketApplication::open(root.path(), storage.clone(), config(false)).await;
    assert!(app.service().is_err());
    assert!(storage
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .is_none());
    storage
        .get_or_create_default_supervisor("existing-model")
        .await
        .unwrap();
    let app = TicketApplication::open(root.path(), storage.clone(), config(true)).await;
    assert!(app.service().is_err());
    let canonical = storage
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    assert!(!canonical.root_orchestration_only_enabled());
    assert!(!root.path().join("tickets").exists());
}

#[tokio::test]
async fn ticket_application_changed_root_authority_fences_every_call() {
    let root = tempfile::tempdir().unwrap();
    let storage = Arc::new(SessionStoreV2::new(root.path().join("host")).await.unwrap());
    let app = TicketApplication::open(root.path(), storage.clone(), config(true)).await;
    assert!(app.service().is_ok());
    let mut canonical = storage
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    canonical.set_root_orchestration_only(false).unwrap();
    storage.save_session(&canonical).await.unwrap();
    assert!(matches!(
        app.authority(Principal::Runtime).await,
        Err(Error::ScopeDenied(_))
    ));
}

#[tokio::test]
async fn ticket_application_rejects_unverified_json_legacy_sources() {
    let root = tempfile::tempdir().unwrap();
    let storage = Arc::new(SessionStoreV2::new(root.path().join("host")).await.unwrap());
    let app = TicketApplication::open(root.path(), storage, config(true)).await;
    let user = Principal::User {
        user_id: "host-owner".into(),
    };
    let (service, authority) = app.authority(user.clone()).await.unwrap();
    let source = ImportSource {
        session_id: "legacy".into(),
        task_id: "old".into(),
        snapshot_hash: "a".repeat(64),
        original_state: "completed".into(),
        artifact: None,
    };
    let before = service.published().unwrap();
    for (index, operation) in [
        Operation::Import {
            temp_id: "work".into(),
            contract: contract(),
            source: source.clone(),
        },
        Operation::AttachLegacy { source },
    ]
    .into_iter()
    .enumerate()
    {
        let command = service
            .prepare_command(&authority, &format!("unverified-{index}"), vec![operation])
            .unwrap();
        assert!(matches!(
            app.update(user.clone(), &command).await,
            Err(Error::ScopeDenied(_))
        ));
    }
    assert_eq!(service.published().unwrap().0, before.0);
}
