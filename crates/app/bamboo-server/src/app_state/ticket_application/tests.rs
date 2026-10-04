use super::*;
use bamboo_storage::SessionStoreV2;
use std::collections::{BTreeMap, BTreeSet};

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
async fn non_dispatch_update_rejects_start_without_claiming_or_publishing() {
    let root = tempfile::tempdir().unwrap();
    let storage = Arc::new(
        SessionStoreV2::new(root.path().to_path_buf())
            .await
            .unwrap(),
    );
    let app = TicketApplication::open(root.path(), storage, config(true)).await;
    let principal = Principal::User {
        user_id: "host-owner".into(),
    };
    let (service, authority) = app.authority(principal.clone()).await.unwrap();
    let create = service
        .prepare_command(
            &authority,
            "update-create",
            vec![
                Operation::Create {
                    temp_id: "work".into(),
                    kind: TicketKind::Work,
                    parent: None,
                    contract: contract(),
                    depends_on: BTreeSet::new(),
                },
                Operation::Ready {
                    work_id: "work".into(),
                },
            ],
        )
        .unwrap();
    let work = app.update(principal.clone(), &create).await.unwrap().ids["work"].clone();
    let start = service
        .prepare_command(
            &authority,
            "wrong-endpoint-start",
            vec![Operation::Start {
                work_id: work.clone(),
                temp_id: "assignment".into(),
                workspace: None,
            }],
        )
        .unwrap();
    let before = service.published().unwrap();
    assert!(matches!(
        app.update(principal.clone(), &start).await,
        Err(Error::ScopeDenied(_))
    ));
    assert_eq!(
        serde_json::to_value(service.published().unwrap()).unwrap(),
        serde_json::to_value(before).unwrap()
    );
    let result = app.dispatch(principal, &start).await.unwrap();
    assert_eq!(result["receipt"]["operation_id"], "wrong-endpoint-start");
    let snapshot = service.published().unwrap().1;
    assert_eq!(snapshot.tickets[&work].state, WorkState::Active);
    assert_eq!(snapshot.assignments.len(), 1);
}

async fn missing_child_fixture(
    enqueue_failure: bool,
) -> (
    tempfile::TempDir,
    TicketApplication,
    Arc<TicketService>,
    Authority,
    ExecutionWorkspace,
    BTreeMap<String, String>,
) {
    let root = tempfile::tempdir().unwrap();
    let state = crate::AppState::new(root.path().to_path_buf())
        .await
        .unwrap();
    *state.config.write().await = config(true).read().await.clone();
    state.config.write().await.features.ticket_dispatch = enqueue_failure;
    let app =
        TicketApplication::open(root.path(), state.storage.clone(), state.config.clone()).await;
    app.bind_adapter(state.tickets.adapter.get().unwrap().clone());
    if enqueue_failure {
        std::fs::write(
            root.path().join("workspaces"),
            b"block enqueue before Child creation",
        )
        .unwrap();
    }
    let worktree = root.path().join("worktree");
    std::fs::create_dir(&worktree).unwrap();
    let canonical = worktree
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let workspace = ExecutionWorkspace {
        repo: canonical.clone(),
        base_commit: "a".repeat(40),
        branch: "fixture".into(),
        worktree: canonical.clone(),
        write_roots: vec![canonical.clone()],
        claims: BTreeSet::from([format!("worktree:{canonical}")]),
    };
    let (service, authority) = app
        .authority(Principal::User {
            user_id: "host-owner".into(),
        })
        .await
        .unwrap();
    let create = service
        .prepare_command(
            &authority,
            "create-cancel-fixture",
            vec![
                Operation::Create {
                    temp_id: "one".into(),
                    kind: TicketKind::Work,
                    parent: None,
                    contract: contract(),
                    depends_on: BTreeSet::new(),
                },
                Operation::Ready {
                    work_id: "one".into(),
                },
                Operation::Create {
                    temp_id: "two".into(),
                    kind: TicketKind::Work,
                    parent: None,
                    contract: contract(),
                    depends_on: BTreeSet::new(),
                },
                Operation::Ready {
                    work_id: "two".into(),
                },
            ],
        )
        .unwrap();
    let mut ids = app
        .update(authority.principal().clone(), &create)
        .await
        .unwrap()
        .ids;
    let start = service
        .prepare_command(
            &authority,
            "start-cancel-fixture",
            vec![Operation::Start {
                work_id: ids["one"].clone(),
                temp_id: "assignment".into(),
                workspace: Some(workspace.clone()),
            }],
        )
        .unwrap();
    let response = app
        .dispatch(authority.principal().clone(), &start)
        .await
        .unwrap();
    assert_eq!(response["errors"].as_array().unwrap().len(), 1);
    ids.insert(
        "assignment".into(),
        response["receipt"]["ids"]["assignment"]
            .as_str()
            .unwrap()
            .into(),
    );
    let assignment = service.published().unwrap().1.assignments[&ids["assignment"]].clone();
    assert!(assignment.runtime.is_none());
    assert!(app
        .storage
        .load_runtime_control_plane(&ticket_runtime::ticket_child_id(&assignment.dispatch_key))
        .await
        .unwrap()
        .is_none());
    (root, app, service, authority, workspace, ids)
}

#[tokio::test]
async fn ticket_review_unadmitted_cancel_and_pause_release_missing_child_claims() {
    for enqueue_failure in [false, true] {
        for pause in [false, true] {
            let (_root, app, service, authority, workspace, ids) =
                missing_child_fixture(enqueue_failure).await;
            let operation = if pause {
                Operation::Pause {
                    work_id: ids["one"].clone(),
                    reason: "explicit pause".into(),
                }
            } else {
                Operation::Cancel {
                    work_id: ids["one"].clone(),
                }
            };
            let command = service
                .prepare_command(&authority, "stop-missing-child", vec![operation])
                .unwrap();
            app.update(authority.principal().clone(), &command)
                .await
                .unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                while !service.published().unwrap().1.assignments[&ids["assignment"]]
                    .process_stopped
                {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("canonical missing, unadmitted attempt must release its claim");
            let snapshot = service.published().unwrap().1;
            assert!(snapshot.assignments[&ids["assignment"]].runtime.is_none());
            assert!(snapshot.tickets[&ids["one"]].active_assignment.is_none());
            assert_eq!(snapshot.tickets[&ids["one"]].paused, pause);
            let next = service
                .prepare_command(
                    &authority,
                    "reuse-released-claim",
                    vec![Operation::Start {
                        work_id: ids["two"].clone(),
                        temp_id: "next-assignment".into(),
                        workspace: Some(workspace),
                    }],
                )
                .unwrap();
            service
                .execute(&authority, &next)
                .expect("the same resource claim must be reusable");
        }
    }
}

#[tokio::test]
async fn ticket_review_admitted_missing_child_preserves_quarantine() {
    let (_root, app, service, authority, workspace, ids) = missing_child_fixture(false).await;
    let snapshot = service.published().unwrap().1;
    let assignment = &snapshot.assignments[&ids["assignment"]];
    // Trusted admission fixture only; canonical absence is never physical stop proof.
    let runtime = Authority::from_verified_host(snapshot.binding, Principal::Runtime);
    let admitted = service
        .prepare_command(
            &runtime,
            "admit-fixture",
            vec![Operation::Admitted {
                assignment_id: assignment.id.clone(),
                receipt: RuntimeReceipt {
                    dispatch_key: assignment.dispatch_key.clone(),
                    spec_hash: service.published().unwrap().1.intents[&assignment.dispatch_key]
                        .spec_hash
                        .clone(),
                    session_id: ticket_runtime::ticket_child_id(&assignment.dispatch_key),
                    run_id: "unknown-run".into(),
                },
            }],
        )
        .unwrap();
    service.execute(&runtime, &admitted).unwrap();
    let cancel = service
        .prepare_command(
            &authority,
            "cancel-admitted-missing",
            vec![Operation::Cancel {
                work_id: ids["one"].clone(),
            }],
        )
        .unwrap();
    app.update(authority.principal().clone(), &cancel)
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(!service.published().unwrap().1.assignments[&ids["assignment"]].process_stopped);
    let next = service
        .prepare_command(
            &authority,
            "preserve-admitted-claim",
            vec![Operation::Start {
                work_id: ids["two"].clone(),
                temp_id: "blocked-assignment".into(),
                workspace: Some(workspace),
            }],
        )
        .unwrap();
    assert!(matches!(
        service.execute(&authority, &next),
        Err(Error::ResourceBlocked(_))
    ));
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
async fn ticket_application_store_init_failure_preserves_ordinary_supervisor_authority() {
    let root = tempfile::tempdir().unwrap();
    let storage = Arc::new(SessionStoreV2::new(root.path().join("host")).await.unwrap());
    // A file where the Ticket directory must be makes actual FileStore
    // initialization fail without depending on platform-specific permissions.
    std::fs::write(root.path().join("tickets"), b"not a directory").unwrap();
    let app = TicketApplication::open(root.path(), storage.clone(), config(true)).await;
    assert!(app.service().is_err());
    assert_eq!(app.status().await["available"], false);
    let canonical = storage
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    assert!(!canonical.root_orchestration_only_enabled());
    assert!(canonical.allows_model_tool_execution("Write"));

    // A later open still requires explicit attach, but must not leave the
    // ordinary Supervisor restricted by the failed optional initialization.
    std::fs::remove_file(root.path().join("tickets")).unwrap();
    let reopened = TicketApplication::open(root.path(), storage.clone(), config(true)).await;
    assert!(reopened.service().is_err());
    let recovered = storage
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    assert!(!recovered.root_orchestration_only_enabled());
    assert_eq!(
        recovered.root_tool_authority_revision,
        canonical.root_tool_authority_revision
    );
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

#[derive(Default)]
struct DecisionWakeActivation {
    fail: std::sync::atomic::AtomicBool,
    calls: tokio::sync::Mutex<Vec<(String, u64)>>,
}

#[async_trait::async_trait]
impl bamboo_domain::SessionActivationPort for DecisionWakeActivation {
    async fn request_activation(
        &self,
        target: &str,
        generation: u64,
    ) -> std::result::Result<
        bamboo_domain::SessionActivationDisposition,
        bamboo_domain::SessionActivationError,
    > {
        self.calls.lock().await.push((target.into(), generation));
        if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
            Err(bamboo_domain::SessionActivationError::Internal(
                "fixture activation unavailable".into(),
            ))
        } else {
            Ok(bamboo_domain::SessionActivationDisposition::ActiveNotified)
        }
    }
}

#[tokio::test]
async fn committed_decision_wake_failure_retries_exact_receipt_and_one_inbox_message() {
    use bamboo_domain::SessionInboxPort;
    for failure in ["missing-messenger", "inbox-storage", "activation"] {
        for approval in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let storage = Arc::new(SessionStoreV2::new(root.path().join("host")).await.unwrap());
            let app = TicketApplication::open(root.path(), storage.clone(), config(true)).await;
            let principal = Principal::User {
                user_id: "host-owner".into(),
            };
            let (service, user) = app.authority(principal.clone()).await.unwrap();
            let supervisor = Authority::from_verified_host(
                service.published().unwrap().1.binding,
                Principal::Supervisor {
                    session_id: DEFAULT_SUPERVISOR_SESSION_ID.into(),
                },
            );
            let create = service
                .prepare_command(
                    &supervisor,
                    "wake-work",
                    vec![Operation::Create {
                        temp_id: "work".into(),
                        kind: TicketKind::Work,
                        parent: None,
                        contract: contract(),
                        depends_on: Default::default(),
                    }],
                )
                .unwrap();
            let work = service.execute(&supervisor, &create).unwrap().ids["work"].clone();
            let action = approval.then(|| Action {
                kind: "fixture".into(),
                target: "exact target".into(),
                data_hash: content_hash(b"fixture data"),
                amount: None,
                permissions: Default::default(),
                risk: "fixture".into(),
            });
            let ask = service
                .prepare_command(
                    &supervisor,
                    "wake-question",
                    vec![Operation::Ask {
                        work_id: work,
                        temp_id: "request".into(),
                        prompt: "Exact question".into(),
                        action,
                    }],
                )
                .unwrap();
            let request_id = service.execute(&supervisor, &ask).unwrap().ids["request"].clone();
            let request = service.published().unwrap().1.requests[&request_id].clone();
            let decision = match &request.kind {
                RequestKind::Question => Operation::Answer {
                    request_id: request_id.clone(),
                    prompt_revision: request.prompt_revision,
                    answer: "verbatim answer".into(),
                },
                RequestKind::Approval { fingerprint, .. } => Operation::DecideApproval {
                    request_id: request_id.clone(),
                    prompt_revision: request.prompt_revision,
                    fingerprint: fingerprint.clone(),
                    approve: true,
                },
            };
            let command = service
                .prepare_command(&user, "wake-decision", vec![decision])
                .unwrap();
            let inbox = Arc::new(bamboo_storage::FileSessionInbox::new(
                storage.clone(),
                Default::default(),
            ));
            let activation = Arc::new(DecisionWakeActivation::default());
            let messenger = Arc::new(bamboo_engine::SessionMessenger::new(
                storage.clone(),
                inbox.clone(),
                activation.clone(),
            ));
            if failure != "missing-messenger" {
                app.bind_messenger(messenger.clone());
            }
            let inbox_dir = storage
                .bamboo_home_dir()
                .join(
                    storage
                        .resolve_rel_path(DEFAULT_SUPERVISOR_SESSION_ID)
                        .await
                        .unwrap(),
                )
                .join("inbox");
            if failure == "inbox-storage" {
                std::fs::write(&inbox_dir, b"obstruct actual inbox directory").unwrap();
            }
            if failure == "activation" {
                activation
                    .fail
                    .store(true, std::sync::atomic::Ordering::SeqCst);
            }
            let error = app
                .update(principal.clone(), &command)
                .await
                .expect_err("post-commit wake failure must be visible");
            assert_eq!(error.status_code(), 503, "{failure} / approval={approval}");
            assert!(error
                .to_string()
                .contains("committed; retry the exact command"));
            let committed = service.published().unwrap();
            let receipt = committed.1.receipts["wake-decision"].clone();
            assert_ne!(
                committed.1.requests[&request_id].status,
                RequestStatus::Open
            );
            if failure == "missing-messenger" {
                app.bind_messenger(messenger);
            }
            if failure == "inbox-storage" {
                std::fs::remove_file(&inbox_dir).unwrap();
            }
            activation
                .fail
                .store(false, std::sync::atomic::Ordering::SeqCst);
            assert_eq!(
                app.update(principal.clone(), &command).await.unwrap(),
                receipt
            );
            assert_eq!(
                canonical_bytes(&service.published().unwrap()).unwrap(),
                canonical_bytes(&committed).unwrap()
            );
            assert_eq!(app.update(principal, &command).await.unwrap(), receipt);
            assert_eq!(
                canonical_bytes(&service.published().unwrap()).unwrap(),
                canonical_bytes(&committed).unwrap()
            );
            let backlog = inbox.inspect(DEFAULT_SUPERVISOR_SESSION_ID).await.unwrap();
            assert_eq!(backlog.pending, 1);
            assert_eq!(backlog.generation, 1);
            let claims = inbox
                .claim(DEFAULT_SUPERVISOR_SESSION_ID, 10)
                .await
                .unwrap();
            assert_eq!(claims.len(), 1);
            let expected = bamboo_domain::SessionMessageId::stable(
                "ticket-decision-wake-v1",
                &json!({"operation_id":receipt.operation_id,"request_hash":receipt.request_hash}),
            );
            assert_eq!(claims[0].envelope.id, expected);
            assert!(activation
                .calls
                .lock()
                .await
                .iter()
                .all(|(id, generation)| id == DEFAULT_SUPERVISOR_SESSION_ID && *generation == 1));
        }
    }
}
