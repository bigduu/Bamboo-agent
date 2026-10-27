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

fn fixture_git(path: &Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(path)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn prepared_git_workspace(root: &Path) -> ExecutionWorkspace {
    let repo = root.join("repo");
    std::fs::create_dir(&repo).unwrap();
    fixture_git(&repo, &["init", "-b", "fixture"]);
    std::fs::write(repo.join("result.txt"), b"fixture\n").unwrap();
    fixture_git(&repo, &["add", "result.txt"]);
    fixture_git(
        &repo,
        &[
            "-c",
            "user.name=Ticket Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "-m",
            "fixture",
        ],
    );
    let base_commit = fixture_git(&repo, &["rev-parse", "HEAD"]);
    let worktree = root.join("worktree");
    fixture_git(
        &repo,
        &[
            "worktree",
            "add",
            "-b",
            "ticket-fixture",
            worktree.to_str().unwrap(),
            &base_commit,
        ],
    );
    let worktree = worktree
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    ExecutionWorkspace {
        repo: repo.canonicalize().unwrap().to_string_lossy().into_owned(),
        base_commit,
        branch: "ticket-fixture".into(),
        worktree: worktree.clone(),
        write_roots: vec![worktree.clone()],
        claims: BTreeSet::from([format!("worktree:{worktree}")]),
    }
}

#[tokio::test]
async fn ticket_start_preflight_rejects_git_identity_before_publication_and_preserves_replay() {
    let root = tempfile::tempdir().unwrap();
    let storage = Arc::new(SessionStoreV2::new(root.path().join("host")).await.unwrap());
    let app = TicketApplication::open(root.path(), storage, config(true)).await;
    let principal = Principal::User {
        user_id: "host-owner".into(),
    };
    let (service, authority) = app.authority(principal.clone()).await.unwrap();
    let create = service
        .prepare_command(
            &authority,
            "git-create",
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
    let workspace = prepared_git_workspace(root.path());
    let other = tempfile::tempdir().unwrap();
    let other_workspace = prepared_git_workspace(other.path());
    for mismatch in ["repo", "base", "branch", "non-git"] {
        let mut bad = workspace.clone();
        match mismatch {
            "repo" => bad.repo = other_workspace.repo.clone(),
            "base" => bad.base_commit = "a".repeat(40),
            "branch" => bad.branch = "wrong-branch".into(),
            _ => {
                let path = root.path().join("non-git");
                std::fs::create_dir(&path).unwrap();
                bad.worktree = path.canonicalize().unwrap().to_string_lossy().into_owned();
                bad.write_roots = vec![bad.worktree.clone()];
                bad.claims = BTreeSet::from([format!("worktree:{}", bad.worktree)]);
            }
        }
        let command = service
            .prepare_command(
                &authority,
                &format!("bad-{mismatch}"),
                vec![Operation::Start {
                    work_id: work.clone(),
                    temp_id: "assignment".into(),
                    workspace: Some(bad),
                }],
            )
            .unwrap();
        let before = serde_json::to_value(service.published().unwrap()).unwrap();
        assert!(
            app.dispatch(principal.clone(), &command).await.is_err(),
            "{mismatch} must fail before Start"
        );
        assert_eq!(
            serde_json::to_value(service.published().unwrap()).unwrap(),
            before,
            "{mismatch} must not publish claims/assignment/receipt"
        );
    }
    let good = service
        .prepare_command(
            &authority,
            "good-start",
            vec![Operation::Start {
                work_id: work,
                temp_id: "assignment".into(),
                workspace: Some(workspace.clone()),
            }],
        )
        .unwrap();
    let response = app.dispatch(principal.clone(), &good).await.unwrap();
    assert_eq!(response["receipt"]["operation_id"], "good-start");
    let before = serde_json::to_value(service.published().unwrap()).unwrap();
    fixture_git(
        Path::new(&workspace.worktree),
        &["checkout", "-b", "changed-after-commit"],
    );
    let replay = app.dispatch(principal.clone(), &good).await.unwrap();
    assert_eq!(response["receipt"], replay["receipt"]);
    assert_eq!(
        serde_json::to_value(service.published().unwrap()).unwrap(),
        before
    );
    let mut altered = good.clone();
    if let Operation::Start {
        workspace: Some(workspace),
        ..
    } = &mut altered.operations[0]
    {
        workspace.branch = "changed-after-commit".into();
    }
    assert!(matches!(
        app.dispatch(principal, &altered).await,
        Err(Error::IdempotencyConflict)
    ));
}

#[tokio::test]
async fn ticket_start_preflight_semantic_failure_does_not_freeze_proposal() {
    let root = tempfile::tempdir().unwrap();
    let storage = Arc::new(SessionStoreV2::new(root.path().join("host")).await.unwrap());
    let app = TicketApplication::open(root.path(), storage, config(true)).await;
    let principal = Principal::User {
        user_id: "host-owner".into(),
    };
    let (service, authority) = app.authority(principal).await.unwrap();
    let create = service
        .prepare_command(
            &authority,
            "semantic-git-create",
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
    let work = service.execute(&authority, &create).unwrap().ids["work"].clone();
    service
        .register_user_ingress(
            &authority,
            &VerifiedUserIngress::from_verified_host(
                "git-human".into(),
                HumanIngressRecord {
                    user_id: "host-owner".into(),
                    source_ingress_seq: 1,
                    text: "开始 work".into(),
                    thread_id: None,
                    in_reply_to: None,
                    correlation_id: None,
                },
            )
            .unwrap(),
        )
        .unwrap();
    let workspace = prepared_git_workspace(root.path());
    let target = TicketReference::from_ticket(&service.published().unwrap().1.tickets[&work]);
    let mut proposal = MessageProposal {
        groups: vec![SemanticGroup {
            group_id: "start".into(),
            item_ids: vec!["start-item".into()],
            source_quote: "开始 work".into(),
            operations: vec![SemanticOperation::Start {
                target,
                temp_id: "assignment".into(),
                workspace: Some(workspace.clone()),
            }],
            clarification: None,
        }],
    };
    if let SemanticOperation::Start {
        workspace: Some(workspace),
        ..
    } = &mut proposal.groups[0].operations[0]
    {
        workspace.branch = "wrong".into();
    }
    let before = serde_json::to_value(service.published().unwrap()).unwrap();
    assert!(app.resolve_message("git-human", &proposal).await.is_err());
    assert_eq!(
        serde_json::to_value(service.published().unwrap()).unwrap(),
        before
    );
    if let SemanticOperation::Start {
        workspace: Some(value),
        ..
    } = &mut proposal.groups[0].operations[0]
    {
        *value = workspace.clone();
    }
    let response = app.resolve_message("git-human", &proposal).await.unwrap();
    assert_eq!(
        response["resolution"]["groups"][0]["status"], "committed",
        "{response}"
    );
    let before = serde_json::to_value(service.published().unwrap()).unwrap();
    fixture_git(
        Path::new(&workspace.worktree),
        &["checkout", "-b", "changed-after-semantic-commit"],
    );
    let replay = app.resolve_message("git-human", &proposal).await.unwrap();
    assert_eq!(response["resolution"], replay["resolution"]);
    assert_eq!(
        serde_json::to_value(service.published().unwrap()).unwrap(),
        before
    );
    let mut altered = proposal;
    altered.groups[0].source_quote = "work".into();
    assert!(matches!(
        app.resolve_message("git-human", &altered).await,
        Err(Error::IdempotencyConflict)
    ));
}

#[tokio::test]
async fn ticket_start_preflight_rejects_nonlocal_worker_before_typed_or_semantic_publication() {
    for placement in ["remote", "schedulable", "ordinary-other-role"] {
        let root = tempfile::tempdir().unwrap();
        let storage = Arc::new(SessionStoreV2::new(root.path().join("host")).await.unwrap());
        let config = config(true);
        {
            let mut config = config.write().await;
            if placement == "schedulable" {
                config.subagents_mut().schedulable_placements.push(
                    bamboo_config::config::SchedulablePlacement {
                        role: "worker".into(),
                        pool: "fixture-pool".into(),
                        ..Default::default()
                    },
                );
            } else {
                config.subagents_mut().remote_placements.push(
                    bamboo_config::config::RemoteActorPlacement {
                        role: if placement == "remote" {
                            "worker"
                        } else {
                            "ordinary-remote"
                        }
                        .into(),
                        endpoint: "wss://fixture.invalid/actor".into(),
                        ..Default::default()
                    },
                );
            }
        }
        let app = TicketApplication::open(root.path(), storage, config).await;
        let principal = Principal::User {
            user_id: "host-owner".into(),
        };
        let (service, authority) = app.authority(principal.clone()).await.unwrap();
        let command = service
            .prepare_command(
                &authority,
                "placement-create",
                vec![
                    Operation::Create {
                        temp_id: "typed".into(),
                        kind: TicketKind::Work,
                        parent: None,
                        contract: contract(),
                        depends_on: BTreeSet::new(),
                    },
                    Operation::Ready {
                        work_id: "typed".into(),
                    },
                    Operation::Create {
                        temp_id: "semantic".into(),
                        kind: TicketKind::Work,
                        parent: None,
                        contract: contract(),
                        depends_on: BTreeSet::new(),
                    },
                    Operation::Ready {
                        work_id: "semantic".into(),
                    },
                ],
            )
            .unwrap();
        let ids = app.update(principal.clone(), &command).await.unwrap().ids;
        let start = service
            .prepare_command(
                &authority,
                "placement-start",
                vec![Operation::Start {
                    work_id: ids["typed"].clone(),
                    temp_id: "assignment".into(),
                    workspace: None,
                }],
            )
            .unwrap();
        let before = serde_json::to_value(service.published().unwrap()).unwrap();
        let result = app.dispatch(principal.clone(), &start).await;
        if placement == "ordinary-other-role" {
            assert!(result.is_ok());
        } else {
            assert!(
                matches!(result, Err(Error::ScopeDenied(_))),
                "{placement}: {result:?}"
            );
            assert_eq!(
                serde_json::to_value(service.published().unwrap()).unwrap(),
                before
            );
        }
        service
            .register_user_ingress(
                &authority,
                &VerifiedUserIngress::from_verified_host(
                    "placement-human".into(),
                    HumanIngressRecord {
                        user_id: "host-owner".into(),
                        source_ingress_seq: 1,
                        text: "开始 work".into(),
                        thread_id: None,
                        in_reply_to: None,
                        correlation_id: None,
                    },
                )
                .unwrap(),
            )
            .unwrap();
        let target =
            TicketReference::from_ticket(&service.published().unwrap().1.tickets[&ids["semantic"]]);
        let proposal = MessageProposal {
            groups: vec![SemanticGroup {
                group_id: "start".into(),
                item_ids: vec!["start-item".into()],
                source_quote: "开始 work".into(),
                operations: vec![SemanticOperation::Start {
                    target,
                    temp_id: "assignment".into(),
                    workspace: None,
                }],
                clarification: None,
            }],
        };
        let before = serde_json::to_value(service.published().unwrap()).unwrap();
        let result = app.resolve_message("placement-human", &proposal).await;
        if placement == "ordinary-other-role" {
            assert!(result.is_ok());
        } else {
            assert!(
                matches!(result, Err(Error::ScopeDenied(_))),
                "{placement}: {result:?}"
            );
            assert_eq!(
                serde_json::to_value(service.published().unwrap()).unwrap(),
                before
            );
        }
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

#[tokio::test]
async fn revoking_upstream_reaches_dependent_runtime_cancellation_and_stop() {
    let (_root, app, service, authority, workspace, ids) = missing_child_fixture(false).await;
    let snapshot = service.published().unwrap().1;
    let runtime = Authority::from_verified_host(snapshot.binding.clone(), Principal::Runtime);
    let upstream_assignment = &snapshot.assignments[&ids["assignment"]];
    let receipt = RuntimeReceipt {
        dispatch_key: upstream_assignment.dispatch_key.clone(),
        spec_hash: snapshot.intents[&upstream_assignment.dispatch_key]
            .spec_hash
            .clone(),
        run_id: "upstream-fixture-run".into(),
        session_id: "upstream-fixture-child".into(),
    };
    let command = service
        .prepare_command(
            &runtime,
            "upstream-fixture-finish",
            vec![
                Operation::Admitted {
                    assignment_id: upstream_assignment.id.clone(),
                    receipt: receipt.clone(),
                },
                Operation::Running {
                    assignment_id: upstream_assignment.id.clone(),
                },
            ],
        )
        .unwrap();
    service.execute(&runtime, &command).unwrap();
    let worker = Authority::from_verified_host(
        snapshot.binding,
        Principal::Worker {
            assignment_id: upstream_assignment.id.clone(),
            generation: upstream_assignment.generation,
            run_id: receipt.run_id,
            session_id: receipt.session_id,
        },
    );
    let artifact = service
        .store_artifact(&runtime, b"accepted upstream bytes")
        .unwrap();
    let submit = service
        .prepare_command(
            &worker,
            "upstream-submit",
            vec![Operation::Submit {
                assignment_id: upstream_assignment.id.clone(),
                temp_id: "submission".into(),
                artifacts: vec![artifact],
                evidence: vec!["verified fixture output".into()],
            }],
        )
        .unwrap();
    let submission = service.execute(&worker, &submit).unwrap().ids["submission"].clone();
    let stopped = service
        .prepare_command(
            &runtime,
            "upstream-confirm-stopped",
            vec![Operation::ConfirmStopped {
                assignment_id: upstream_assignment.id.clone(),
                effects_reconciled: true,
            }],
        )
        .unwrap();
    service.execute(&runtime, &stopped).unwrap();
    let accept = service
        .prepare_command(
            &authority,
            "upstream-accept-and-depend",
            vec![
                Operation::Accept {
                    work_id: ids["one"].clone(),
                    submission_id: submission,
                    evidence: vec!["reviewed".into()],
                },
                Operation::SetDependencies {
                    work_id: ids["two"].clone(),
                    depends_on: BTreeSet::from([ids["one"].clone()]),
                },
            ],
        )
        .unwrap();
    app.update(authority.principal().clone(), &accept)
        .await
        .unwrap();
    let start = service
        .prepare_command(
            &authority,
            "dependent-dispatch",
            vec![Operation::Start {
                work_id: ids["two"].clone(),
                temp_id: "dependent-assignment".into(),
                workspace: Some(workspace),
            }],
        )
        .unwrap();
    let dispatched = app
        .dispatch(authority.principal().clone(), &start)
        .await
        .unwrap();
    let dependent_id = dispatched["receipt"]["ids"]["dependent-assignment"]
        .as_str()
        .unwrap()
        .to_string();
    let revoke = service
        .prepare_command(
            &authority,
            "reopen-upstream",
            vec![Operation::Reopen {
                work_id: ids["one"].clone(),
            }],
        )
        .unwrap();
    app.update(authority.principal().clone(), &revoke)
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let snapshot = service.published().unwrap().1;
            let dependent = &snapshot.assignments[&dependent_id];
            if dependent.process_stopped {
                assert_eq!(dependent.state, AssignmentState::Cancelled);
                assert_eq!(snapshot.tickets[&ids["two"]].state, WorkState::Blocked);
                assert!(snapshot.tickets[&ids["two"]].active_assignment.is_none());
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("dependent reaches the existing Runtime cancellation/stop port");
}

#[tokio::test]
async fn ticket_start_preflight_replay_cannot_admit_after_nonlocal_placement_change() {
    let (_root, app, service, authority, _workspace, ids) = missing_child_fixture(false).await;
    let receipt = service.published().unwrap().1.receipts["start-cancel-fixture"].clone();
    let command: Command = serde_json::from_str(&receipt.canonical_request).unwrap();
    {
        let mut config = app.config.write().await;
        config.features.ticket_dispatch = true;
        config.subagents_mut().remote_placements.push(
            bamboo_config::config::RemoteActorPlacement {
                role: "worker".into(),
                endpoint: "wss://fixture.invalid/actor".into(),
                ..Default::default()
            },
        );
    }
    let before = serde_json::to_value(service.published().unwrap()).unwrap();
    let response = app
        .dispatch(authority.principal().clone(), &command)
        .await
        .unwrap();
    assert_eq!(response["receipt"], serde_json::to_value(receipt).unwrap());
    assert_eq!(response["errors"][0]["status_code"], 403);
    assert!(response["errors"][0]["reason"]
        .as_str()
        .unwrap()
        .contains("requires local placement"));
    assert_eq!(
        serde_json::to_value(service.published().unwrap()).unwrap(),
        before
    );
    let snapshot = service.published().unwrap().1;
    let assignment = &snapshot.assignments[&ids["assignment"]];
    assert!(matches!(
        app.query_dispatch(&assignment.dispatch_key).await.unwrap(),
        ticket_runtime::DispatchObservation::Missing
    ));
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
    let workspace = prepared_git_workspace(root.path());
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
    assert!(canonical.model_orchestration_only_enabled());
    assert!(canonical.allows_model_tool_execution("SubAgent"));
    assert!(canonical.allows_model_tool_execution("session_control"));
    assert!(!canonical.allows_model_tool_execution("Write"));
    assert!(!canonical.allows_model_tool_execution("exec_command"));

    // A later open still requires explicit attach, but must not leave the
    // standard Supervisor with changed authority after optional init failure.
    std::fs::remove_file(root.path().join("tickets")).unwrap();
    let reopened = TicketApplication::open(root.path(), storage.clone(), config(true)).await;
    assert!(reopened.service().is_err());
    let recovered = storage
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    assert!(!recovered.root_orchestration_only_enabled());
    assert!(recovered.model_orchestration_only_enabled());
    assert!(recovered.allows_model_tool_execution("SubAgent"));
    assert!(recovered.allows_model_tool_execution("session_control"));
    assert!(!recovered.allows_model_tool_execution("Write"));
    assert!(!recovered.allows_model_tool_execution("exec_command"));
    assert_eq!(
        recovered.supervisor_incarnation_id(),
        canonical.supervisor_incarnation_id()
    );
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

// Controlled canonical-store fixture for the startup publication boundary.
// This does not claim that a task was executed or joined; the unchanged real
// Host-exit test and engine's actual LocalTask runner tests establish that proof.
struct NativeResultFixture {
    root: tempfile::TempDir,
    storage: Arc<SessionStoreV2>,
    app: TicketApplication,
    child: bamboo_domain::Session,
    work_id: String,
    assignment_id: String,
}

impl NativeResultFixture {
    async fn new() -> Self {
        use bamboo_domain::{Message, Session, SessionInboxLimits};
        use bamboo_engine::execution::spawn::SessionInboxRuntimeBinding;
        use bamboo_storage::{FileSessionInbox, LockedSessionStore};

        let root = tempfile::tempdir().unwrap();
        let storage = Arc::new(
            SessionStoreV2::new(root.path().to_path_buf())
                .await
                .unwrap(),
        );
        let app = TicketApplication::open(root.path(), storage.clone(), config(true)).await;
        let (service, authority) = app
            .authority(Principal::User {
                user_id: "host-owner".into(),
            })
            .await
            .unwrap();
        let command = service
            .prepare_command(
                &authority,
                "native-result-create",
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
                    Operation::Start {
                        work_id: "work".into(),
                        temp_id: "assignment".into(),
                        workspace: None,
                    },
                ],
            )
            .unwrap();
        let ids = service.execute(&authority, &command).unwrap().ids;
        let snapshot = service.published().unwrap().1;
        let assignment = &snapshot.assignments[&ids["assignment"]];
        let intent = &snapshot.intents[&assignment.dispatch_key];
        let dispatch = ticket_runtime::dispatch_binding(
            &service,
            &assignment.dispatch_key,
            &intent.immutable_spec,
        )
        .unwrap();
        let supervisor = storage
            .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
            .await
            .unwrap()
            .unwrap();
        let mut child = Session::new_child_of(
            ticket_runtime::ticket_child_id(&assignment.dispatch_key),
            &supervisor,
            "fixture-model",
            "native result",
        );
        child.metadata.insert(
            ticket_runtime::TICKET_DISPATCH_KEY.into(),
            serde_json::to_string(&dispatch).unwrap(),
        );
        child.metadata.insert(
            bamboo_engine::ticket_worker_plan::TICKET_LOCAL_PLAN_KEY.into(),
            assignment.id.clone(),
        );
        child.set_last_run_status("pending");
        storage.save_session(&child).await.unwrap();
        let persistence = Arc::new(LockedSessionStore::new(storage.clone()));
        let inbox = Arc::new(FileSessionInbox::new(
            storage.clone(),
            SessionInboxLimits::default(),
        ));
        let router = bamboo_engine::SessionActivationRouter::new();
        router.set_inbox(inbox.clone());
        let runtime = SessionInboxRuntimeBinding {
            router,
            inbox,
            storage: storage.clone(),
            persistence: persistence.clone(),
            parent_question_lock: Some(persistence),
        };
        let _registration = runtime
            .router
            .register_run(&child.id, "native-result-run")
            .await
            .unwrap();
        ticket_runtime::admit_ticket_run(&service, &runtime, &mut child, "native-result-run")
            .await
            .unwrap();
        let instance = uuid::Uuid::new_v4().to_string();
        let receipt = ticket_runtime::read_dispatch(&child)
            .unwrap()
            .unwrap()
            .receipt
            .unwrap();
        child
            .metadata
            .insert("runtime.local_task.state".into(), "stopped".into());
        child
            .metadata
            .insert("runtime.local_task.instance".into(), instance.clone());
        child
            .metadata
            .insert("runtime.local_task.run_id".into(), receipt.run_id.clone());
        child.metadata.insert(
            "runtime.local_task.child_birth".into(),
            child.created_at.to_rfc3339(),
        );
        child.metadata.insert("ticket.runtime.owned_stop.v1".into(), json!({"receipt":receipt,"child_birth":child.created_at,"task_instance":instance,"completed":true}).to_string());
        child.metadata_version += 1;
        child.set_last_run_status("completed");
        child.add_message(Message::assistant("canonical native result bytes", None));
        storage.save_session(&child).await.unwrap();
        Self {
            root,
            storage,
            app,
            child,
            work_id: ids["work"].clone(),
            assignment_id: assignment.id.clone(),
        }
    }

    async fn restart(&mut self) {
        Arc::get_mut(&mut self.app.service)
            .expect("no active initializer in restart fixture")
            .take();
        self.app =
            TicketApplication::open(self.root.path(), self.storage.clone(), config(false)).await;
    }

    fn snapshot_bytes(&self) -> Vec<u8> {
        canonical_bytes(&self.app.service().unwrap().published().unwrap()).unwrap()
    }
}

#[tokio::test]
async fn native_result_recovery_replays_only_canonical_completed_publication() {
    let mut f = NativeResultFixture::new().await;
    f.restart().await;
    let service = f.app.service().unwrap();
    let before = service.published().unwrap().1;
    assert_eq!(
        before.assignments[&f.assignment_id].state,
        AssignmentState::OutcomeUnknown
    );
    assert!(before.submissions.is_empty());
    let child_before =
        serde_json::to_value(f.storage.load_session(&f.child.id).await.unwrap()).unwrap();
    f.app.reconcile_completed_native_results().await.unwrap();
    let recovered = service.published().unwrap().1;
    assert_eq!(recovered.tickets[&f.work_id].state, WorkState::Submitted);
    assert!(recovered.assignments[&f.assignment_id].process_stopped);
    assert_eq!(recovered.submissions.len(), 1);
    let submission = recovered.submissions.values().next().unwrap();
    assert!(!submission.stale);
    let (_, authority) = f
        .app
        .authority(Principal::User {
            user_id: "host-owner".into(),
        })
        .await
        .unwrap();
    assert_eq!(
        service
            .read_artifact(&authority, &submission.artifacts[0], 65536)
            .unwrap(),
        b"canonical native result bytes"
    );
    assert!(
        bamboo_engine::ticket_worker_plan::TicketWorkerPlan::from_runtime_receipt(
            service.clone(),
            &f.assignment_id
        )
        .is_err()
    );
    let after = f.snapshot_bytes();
    f.app.reconcile_completed_native_results().await.unwrap();
    assert_eq!(f.snapshot_bytes(), after);
    assert_eq!(
        serde_json::to_value(f.storage.load_session(&f.child.id).await.unwrap()).unwrap(),
        child_before
    );
    drop(service);
    f.restart().await;
    let reopened = f.snapshot_bytes();
    f.app.reconcile_completed_native_results().await.unwrap();
    assert_eq!(f.snapshot_bytes(), reopened);
    assert_eq!(
        f.app
            .service()
            .unwrap()
            .published()
            .unwrap()
            .1
            .submissions
            .len(),
        1
    );
    assert!(
        f.app.adapter.get().is_none(),
        "publication needs no runner or provider"
    );
}

#[tokio::test]
async fn native_result_recovery_rejects_incomplete_or_wrong_canonical_evidence() {
    for case in [
        "missing-stop",
        "malformed-stop",
        "oversized-stop",
        "stop-birth",
        "stop-run",
        "stop-instance",
        "nil-instance",
        "missing-instance",
        "process-stop",
        "mixed-stop",
        "incomplete-stop",
        "active",
        "active-task",
        "missing-child",
        "revoked-supervisor",
        "native-run",
        "native-birth",
        "missing-dispatch",
        "malformed-dispatch",
        "dispatch-birth",
        "dispatch-run",
        "wrong-plan",
        "wrong-parent",
        "wrong-root",
        "external-route",
        "wrong-scope",
    ] {
        let mut f = NativeResultFixture::new().await;
        f.restart().await;
        let mut stop: Value =
            serde_json::from_str(&f.child.metadata["ticket.runtime.owned_stop.v1"]).unwrap();
        let mut dispatch = ticket_runtime::read_dispatch(&f.child).unwrap().unwrap();
        match case {
            "stop-birth" => {
                stop["child_birth"] = json!(f.child.created_at - chrono::Duration::seconds(1))
            }
            "stop-run" => stop["receipt"]["run_id"] = json!("wrong-run"),
            "stop-instance" => stop["task_instance"] = json!(uuid::Uuid::new_v4()),
            "nil-instance" => {
                stop["task_instance"] = json!(uuid::Uuid::nil());
                f.child.metadata.insert(
                    "runtime.local_task.instance".into(),
                    uuid::Uuid::nil().to_string(),
                );
            }
            "missing-instance" => {
                stop.as_object_mut().unwrap().remove("task_instance");
            }
            "process-stop" => {
                stop.as_object_mut().unwrap().remove("task_instance");
                stop["pid"] = json!(1234);
            }
            "mixed-stop" => stop["pid"] = json!(1234),
            "incomplete-stop" => stop["completed"] = json!(false),
            "active" => {
                f.child.set_last_run_status("running");
                f.child
                    .metadata
                    .insert("runtime.local_task.state".into(), "running".into());
            }
            "native-run" => {
                f.child
                    .metadata
                    .insert("runtime.local_task.run_id".into(), "wrong-run".into());
            }
            "native-birth" => {
                f.child.metadata.insert(
                    "runtime.local_task.child_birth".into(),
                    (f.child.created_at - chrono::Duration::seconds(1)).to_rfc3339(),
                );
            }
            "dispatch-birth" => {
                dispatch.receipt_created_at =
                    Some(f.child.created_at - chrono::Duration::seconds(1))
            }
            "dispatch-run" => dispatch.receipt.as_mut().unwrap().run_id = "wrong-run".into(),
            "wrong-plan" => {
                f.child.metadata.insert(
                    bamboo_engine::ticket_worker_plan::TICKET_LOCAL_PLAN_KEY.into(),
                    "another-assignment".into(),
                );
            }
            "active-task" => {
                f.child
                    .metadata
                    .insert("runtime.local_task.state".into(), "running".into());
            }
            "external-route" => {
                f.child
                    .metadata
                    .insert("external.agent_id".into(), "process-worker".into());
            }
            "wrong-scope" => dispatch.binding.scope_id = "another-scope".into(),
            _ => {}
        }
        f.child
            .metadata
            .insert("ticket.runtime.owned_stop.v1".into(), stop.to_string());
        f.child.metadata.insert(
            ticket_runtime::TICKET_DISPATCH_KEY.into(),
            serde_json::to_string(&dispatch).unwrap(),
        );
        match case {
            "missing-stop" => {
                f.child.metadata.remove("ticket.runtime.owned_stop.v1");
            }
            "malformed-stop" => {
                f.child
                    .metadata
                    .insert("ticket.runtime.owned_stop.v1".into(), "not-json".into());
            }
            "oversized-stop" => {
                f.child
                    .metadata
                    .insert("ticket.runtime.owned_stop.v1".into(), " ".repeat(4097));
            }
            "missing-dispatch" => {
                f.child.metadata.remove(ticket_runtime::TICKET_DISPATCH_KEY);
            }
            "malformed-dispatch" => {
                f.child.metadata.insert(
                    ticket_runtime::TICKET_DISPATCH_KEY.into(),
                    "not-json".into(),
                );
            }
            _ => {}
        }
        f.child.metadata_version += 1;
        f.storage.save_session(&f.child).await.unwrap();
        match case {
            "wrong-parent" | "wrong-root" => {
                // Ordinary writers correctly reject a Child lineage rewrite.
                // Corrupt only this disposable canonical sidecar to exercise
                // recovery's fail-closed read, without weakening that guard.
                let mut changed = f.child.clone();
                if case == "wrong-parent" {
                    changed.parent_session_id = Some("another-parent".into());
                } else {
                    changed.root_session_id = "another-root".into();
                }
                assert!(f.storage.save_session(&changed).await.is_err());
                let path = f
                    .root
                    .path()
                    .join("sessions")
                    .join(DEFAULT_SUPERVISOR_SESSION_ID)
                    .join("children")
                    .join(&f.child.id)
                    .join("runtime.json");
                let mut raw: Value =
                    serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                if case == "wrong-parent" {
                    raw["parent_session_id"] = json!("another-parent");
                } else {
                    raw["root_session_id"] = json!("another-root");
                }
                std::fs::write(path, serde_json::to_vec(&raw).unwrap()).unwrap();
            }
            "missing-child" => {
                assert!(f.storage.delete_session(&f.child.id).await.unwrap());
            }
            "revoked-supervisor" => {
                let mut supervisor = f
                    .storage
                    .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
                    .await
                    .unwrap()
                    .unwrap();
                supervisor.set_root_orchestration_only(false).unwrap();
                f.storage.save_session(&supervisor).await.unwrap();
            }
            _ => {}
        }
        let before = f.snapshot_bytes();
        let _ = f.app.reconcile_completed_native_results().await;
        assert_eq!(
            f.snapshot_bytes(),
            before,
            "{case} must retain the exact Ticket HEAD"
        );
    }
}

#[tokio::test]
async fn native_result_recovery_cannot_revive_cancelled_work() {
    let mut f = NativeResultFixture::new().await;
    let (service, authority) = f
        .app
        .authority(Principal::User {
            user_id: "host-owner".into(),
        })
        .await
        .unwrap();
    let cancel = service
        .prepare_command(
            &authority,
            "cancel-native-result",
            vec![Operation::Cancel {
                work_id: f.work_id.clone(),
            }],
        )
        .unwrap();
    service.execute(&authority, &cancel).unwrap();
    drop(service);
    f.restart().await;
    f.app.reconcile_completed_native_results().await.unwrap();
    let snapshot = f.app.service().unwrap().published().unwrap().1;
    assert_eq!(snapshot.tickets[&f.work_id].state, WorkState::Cancelled);
    assert!(snapshot.tickets[&f.work_id].current_submission.is_none());
    assert!(snapshot
        .submissions
        .values()
        .all(|submission| submission.stale));
    assert!(snapshot.assignments[&f.assignment_id].process_stopped);
}

#[tokio::test]
async fn native_result_startup_recovers_before_any_provider_or_child_activation() {
    use async_trait::async_trait;
    use bamboo_llm::{LLMError, LLMProvider, LLMStream};
    #[derive(Default)]
    struct NoExecution(std::sync::atomic::AtomicUsize);
    #[async_trait]
    impl LLMProvider for NoExecution {
        async fn chat_stream(
            &self,
            _: &[bamboo_domain::Message],
            _: &[bamboo_agent_core::tools::ToolSchema],
            _: Option<u32>,
            _: &str,
        ) -> std::result::Result<LLMStream, LLMError> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(LLMError::Api(
                "startup result recovery must not execute a provider".into(),
            ))
        }
    }
    let mut f = NativeResultFixture::new().await;
    Arc::get_mut(&mut f.app.service)
        .expect("no active initializer in startup fixture")
        .take();
    let provider = Arc::new(NoExecution::default());
    let mut startup_config = config(false).read().await.clone();
    // The new Supervisor is also an AutoDream candidate. Isolate that unrelated
    // background model call while retaining the real startup recovery paths.
    startup_config
        .memory_mut()
        .get_or_insert_with(Default::default)
        .auto_dream_enabled = false;
    let state = crate::AppState::new_with_provider(
        f.root.path().to_path_buf(),
        startup_config,
        provider.clone(),
    )
    .await
    .unwrap();
    let snapshot = state.tickets.service().unwrap().published().unwrap().1;
    assert_eq!(snapshot.tickets[&f.work_id].state, WorkState::Submitted);
    assert_eq!(snapshot.submissions.len(), 1);
    assert!(snapshot.assignments[&f.assignment_id].process_stopped);
    assert!(state.agent_runners.read().await.is_empty());
    assert!(state.embedded_broker.is_none());
    state.shutdown().await;
    assert_eq!(provider.0.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn native_result_recovery_never_submits_missing_or_nonfinal_output() {
    for case in ["missing", "empty", "commentary", "tool"] {
        let mut f = NativeResultFixture::new().await;
        match case {
            "missing" => f.child.messages.clear(),
            "empty" => f.child.messages.last_mut().unwrap().content.clear(),
            "commentary" => {
                f.child.messages.last_mut().unwrap().phase =
                    Some(bamboo_domain::MessagePhase::Commentary)
            }
            "tool" => f.child.messages.last_mut().unwrap().role = bamboo_domain::Role::Tool,
            _ => unreachable!(),
        }
        f.storage.save_session(&f.child).await.unwrap();
        f.restart().await;
        assert!(
            f.app.reconcile_completed_native_results().await.is_err(),
            "{case}"
        );
        let snapshot = f.app.service().unwrap().published().unwrap().1;
        assert!(snapshot.submissions.is_empty(), "{case}");
        assert_ne!(
            snapshot.tickets[&f.work_id].state,
            WorkState::Submitted,
            "{case}"
        );
        let stopped = f.snapshot_bytes();
        assert!(f.app.reconcile_completed_native_results().await.is_err());
        assert_eq!(
            f.snapshot_bytes(),
            stopped,
            "{case} stop replay must be idempotent"
        );
    }
}

#[tokio::test]
async fn ticket_scope_status_distinguishes_empty_uninitialized_and_disabled() {
    let root = tempfile::tempdir().unwrap();
    let storage = Arc::new(SessionStoreV2::new(root.path().join("host")).await.unwrap());
    storage
        .get_or_create_default_supervisor("model")
        .await
        .unwrap();
    let mut supervisor = storage
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    supervisor.set_root_orchestration_only(true).unwrap();
    storage.save_session(&supervisor).await.unwrap();
    let scope_root = root
        .path()
        .join("tickets")
        .join(supervisor.supervisor_incarnation_id().unwrap().to_string());
    for enabled in [true, false] {
        let app = TicketApplication::open(root.path(), storage.clone(), config(enabled)).await;
        let status = app.status().await;
        assert_eq!(status["available"], false);
        assert_eq!(status["readable"], false);
        assert_eq!(
            status["availability"]["kind"],
            if enabled { "uninitialized" } else { "disabled" }
        );
        assert_eq!(status["can_initialize"], enabled);
        assert!(
            !scope_root.exists(),
            "status initialized a scope without a User action"
        );
    }
}

fn scope_payload(supervisor: &bamboo_domain::Session) -> ScopeInitialization {
    ScopeInitialization {
        supervisor_session_id: supervisor.id.clone(),
        incarnation_id: supervisor.supervisor_incarnation_id().unwrap(),
        binding_revision: supervisor.root_tool_authority_revision,
    }
}

async fn uninitialized_work_fixture() -> (
    tempfile::TempDir,
    Arc<SessionStoreV2>,
    TicketApplication,
    bamboo_domain::Session,
) {
    let root = tempfile::tempdir().unwrap();
    let storage = Arc::new(SessionStoreV2::new(root.path().join("host")).await.unwrap());
    storage
        .get_or_create_default_supervisor("model")
        .await
        .unwrap();
    let mut supervisor = storage
        .load_session(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    supervisor.add_message(bamboo_domain::Message::user("existing Supervisor history"));
    supervisor.set_root_orchestration_only(true).unwrap();
    storage.save_session(&supervisor).await.unwrap();
    let app = TicketApplication::open(root.path(), storage.clone(), config(true)).await;
    (root, storage, app, supervisor)
}

fn scope_owner() -> Principal {
    Principal::User {
        user_id: "host-owner".into(),
    }
}

#[tokio::test]
async fn ticket_scope_initialization_is_explicit_empty_idempotent_and_survives_restart() {
    let (root, storage, app, supervisor) = uninitialized_work_fixture().await;
    let payload = scope_payload(&supervisor);
    let before = serde_json::to_value(storage.load_session(&supervisor.id).await.unwrap()).unwrap();
    let config_before = serde_json::to_value(app.config.read().await.clone()).unwrap();
    let observed: ScopeInitialization =
        serde_json::from_value(app.status().await["initialization"].clone()).unwrap();
    assert_eq!(observed, payload);
    let (first, second) = tokio::join!(
        app.initialize_scope(scope_owner(), payload.clone()),
        app.initialize_scope(scope_owner(), payload.clone())
    );
    let mut created = [first.unwrap(), second.unwrap()];
    created.sort();
    assert_eq!(created, [false, true]);
    let service = app.service().unwrap();
    let before_retry = service.published().unwrap();
    assert!(before_retry.1.tickets.is_empty());
    assert!(before_retry.1.assignments.is_empty());
    assert!(!app
        .initialize_scope(scope_owner(), payload.clone())
        .await
        .unwrap());
    assert_eq!(service.published().unwrap().0, before_retry.0);
    let status = app.status().await;
    assert_eq!(status["availability"]["kind"], "ready");
    assert_eq!(status["readable"], true);
    assert_eq!(status["can_initialize"], false);
    assert_eq!(status["initialization"], Value::Null);
    assert_eq!(
        serde_json::to_value(storage.load_session(&supervisor.id).await.unwrap()).unwrap(),
        before
    );
    assert_eq!(
        serde_json::to_value(app.config.read().await.clone()).unwrap(),
        config_before
    );
    let scope = storage
        .inspect_supervisor_scope(&bamboo_domain::SupervisorReference {
            session_id: supervisor.id.clone(),
            incarnation_id: payload.incarnation_id,
        })
        .await
        .unwrap();
    assert_eq!(scope.state_revision, 0);
    assert!(scope.allowed_projects.is_empty());
    drop(service);
    drop(app);
    let app = TicketApplication::open(root.path(), storage.clone(), config(false)).await;
    let status = app.status().await;
    assert_eq!(status["availability"]["kind"], "ready");
    assert_eq!(status["readable"], true);
    assert_eq!(status["mutation_enabled"], false);
    assert_eq!(status["dispatch_enabled"], false);
    assert!(!app.initialize_scope(scope_owner(), payload).await.unwrap());
}

#[tokio::test]
async fn ticket_scope_initialization_refuses_gates_stale_identity_and_non_owner_without_effects() {
    for scenario in [
        "disabled",
        "standard",
        "revision",
        "incarnation",
        "non-owner",
        "recreated",
    ] {
        let (_root, storage, app, mut supervisor) = uninitialized_work_fixture().await;
        let mut payload = scope_payload(&supervisor);
        let mut principal = scope_owner();
        let (status, code) = match scenario {
            "disabled" => {
                app.config.write().await.features.ticket_mutation = false;
                (503, "ticket_mutation_disabled")
            }
            "standard" => {
                supervisor.set_root_orchestration_only(false).unwrap();
                storage.save_session(&supervisor).await.unwrap();
                payload = scope_payload(&supervisor);
                (403, "supervisor_mode_required")
            }
            "revision" => {
                payload.binding_revision += 1;
                (409, "scope_binding_mismatch")
            }
            "incarnation" => {
                payload.incarnation_id = uuid::Uuid::new_v4();
                (409, "scope_binding_mismatch")
            }
            "non-owner" => {
                principal = Principal::Runtime;
                (403, "supervisor_identity_mismatch")
            }
            "recreated" => {
                storage.delete_session(&supervisor.id).await.unwrap();
                storage
                    .get_or_create_default_supervisor("new-model")
                    .await
                    .unwrap();
                supervisor = storage.load_session(&supervisor.id).await.unwrap().unwrap();
                supervisor.set_root_orchestration_only(true).unwrap();
                storage.save_session(&supervisor).await.unwrap();
                (409, "scope_binding_mismatch")
            }
            _ => unreachable!(),
        };
        let before =
            serde_json::to_value(storage.load_session(&supervisor.id).await.unwrap()).unwrap();
        let error = app
            .initialize_scope(principal, payload.clone())
            .await
            .unwrap_err();
        assert_eq!((error.status, error.code), (status, code), "{scenario}");
        assert!(
            !app.scope_root(payload.incarnation_id).exists(),
            "{scenario}"
        );
        assert_eq!(
            serde_json::to_value(storage.load_session(&supervisor.id).await.unwrap()).unwrap(),
            before,
            "{scenario}"
        );
        assert!(app.service().is_err());
    }
}

#[tokio::test]
async fn ticket_scope_initialization_never_recovers_unknown_partial_or_symlinked_stores() {
    for scenario in ["unknown", "partial", "symlink"] {
        let (root, storage, app, supervisor) = uninitialized_work_fixture().await;
        let payload = scope_payload(&supervisor);
        let scope = app.scope_root(payload.incarnation_id);
        std::fs::create_dir_all(scope.parent().unwrap()).unwrap();
        let outside = root.path().join("outside");
        if scenario == "symlink" {
            std::fs::create_dir(&outside).unwrap();
            #[cfg(unix)]
            std::os::unix::fs::symlink(&outside, &scope).unwrap();
        } else {
            std::fs::create_dir(&scope).unwrap();
            if scenario == "partial" {
                std::fs::create_dir(scope.join("objects")).unwrap();
            }
            std::fs::write(scope.join("preserve.txt"), b"historical bytes").unwrap();
        }
        let before =
            serde_json::to_value(storage.load_session(&supervisor.id).await.unwrap()).unwrap();
        let status = app.status().await;
        assert_eq!(status["availability"]["kind"], "error");
        assert_eq!(status["can_initialize"], false);
        let error = app
            .initialize_scope(scope_owner(), payload)
            .await
            .unwrap_err();
        assert_eq!(
            error.code,
            if scenario == "symlink" {
                "scope_io_error"
            } else {
                "scope_requires_recovery"
            }
        );
        assert!(!scope.join("HEAD").exists());
        assert!(!scope.join("writer.lock").exists());
        assert_eq!(
            serde_json::to_value(storage.load_session(&supervisor.id).await.unwrap()).unwrap(),
            before
        );
        if scenario != "symlink" {
            assert_eq!(
                std::fs::read(scope.join("preserve.txt")).unwrap(),
                b"historical bytes"
            );
        }
    }
}

#[tokio::test]
async fn ticket_scope_status_reports_binding_mismatch_without_rebinding_old_head() {
    let (root, storage, app, mut supervisor) = uninitialized_work_fixture().await;
    let payload = scope_payload(&supervisor);
    app.initialize_scope(scope_owner(), payload.clone())
        .await
        .unwrap();
    let head = std::fs::read(app.scope_root(payload.incarnation_id).join("HEAD")).unwrap();
    drop(app);
    supervisor.set_root_orchestration_only(false).unwrap();
    storage.save_session(&supervisor).await.unwrap();
    supervisor.set_root_orchestration_only(true).unwrap();
    storage.save_session(&supervisor).await.unwrap();
    let app = TicketApplication::open(root.path(), storage, config(true)).await;
    let status = app.status().await;
    assert_eq!(status["availability"]["kind"], "forbidden");
    assert_eq!(
        status["availability"]["reason_code"],
        "scope_binding_mismatch"
    );
    assert_eq!(status["readable"], false);
    assert_eq!(status["can_initialize"], false);
    assert_eq!(
        app.initialize_scope(scope_owner(), scope_payload(&supervisor))
            .await
            .unwrap_err()
            .code,
        "scope_binding_mismatch"
    );
    assert_eq!(
        std::fs::read(app.scope_root(payload.incarnation_id).join("HEAD")).unwrap(),
        head
    );
}

#[tokio::test]
async fn ticket_scope_status_distinguishes_forged_identity_and_io_authority_failure() {
    for corrupted in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let storage = Arc::new(SessionStoreV2::new(root.path().join("host")).await.unwrap());
        let payload = if corrupted {
            let receipt = storage
                .get_or_create_default_supervisor("model")
                .await
                .unwrap();
            let mut session = storage
                .load_session(&receipt.session_id)
                .await
                .unwrap()
                .unwrap();
            session.set_root_orchestration_only(true).unwrap();
            storage.save_session(&session).await.unwrap();
            let payload = scope_payload(&session);
            std::fs::write(
                storage
                    .sessions_root_dir()
                    .join(&session.id)
                    .join("runtime.json"),
                b"corrupt authority",
            )
            .unwrap();
            payload
        } else {
            let mut ordinary = bamboo_domain::Session::new(DEFAULT_SUPERVISOR_SESSION_ID, "model");
            ordinary.metadata.insert("role".into(), "supervisor".into());
            ordinary.set_root_orchestration_only(true).unwrap();
            storage.save_session(&ordinary).await.unwrap();
            ScopeInitialization {
                supervisor_session_id: ordinary.id,
                incarnation_id: uuid::Uuid::new_v4(),
                binding_revision: ordinary.root_tool_authority_revision,
            }
        };
        let app = TicketApplication::open(root.path(), storage, config(true)).await;
        let status = app.status().await;
        assert_eq!(
            status["availability"]["kind"],
            if corrupted { "error" } else { "forbidden" }
        );
        assert_eq!(
            status["availability"]["reason_code"],
            if corrupted {
                "scope_io_error"
            } else {
                "supervisor_identity_mismatch"
            }
        );
        assert_eq!(status["can_initialize"], false);
        let error = app
            .initialize_scope(scope_owner(), payload)
            .await
            .unwrap_err();
        assert_eq!(error.status, if corrupted { 503 } else { 403 });
        assert!(!root.path().join("tickets").exists());
    }
}

#[tokio::test]
async fn ticket_scope_platform_contract_denies_get_eligibility_and_post_without_effects() {
    let (_root, storage, app, supervisor) = uninitialized_work_fixture().await;
    let payload = scope_payload(&supervisor);
    let before = serde_json::to_value(storage.load_session(&supervisor.id).await.unwrap()).unwrap();
    let status = app.status_for_platform(false).await;
    assert_eq!(status["availability"]["kind"], "error");
    assert_eq!(status["availability"]["reason_code"], "scope_io_error");
    assert_eq!(status["readable"], false);
    assert_eq!(status["can_initialize"], false);
    assert_eq!(status["initialization"], Value::Null);
    let error = app
        .initialize_for_platform(scope_owner(), payload.clone(), false)
        .await
        .unwrap_err();
    assert_eq!((error.status, error.code), (503, "scope_io_error"));
    assert!(!app.scope_root(payload.incarnation_id).exists());
    assert_eq!(
        serde_json::to_value(storage.load_session(&supervisor.id).await.unwrap()).unwrap(),
        before
    );
}

#[tokio::test]
async fn ticket_scope_initialization_publishes_runtime_service_once_after_authorized_creation() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (_root, _storage, app, supervisor) = uninitialized_work_fixture().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let called = calls.clone();
    let expected = scope_payload(&supervisor);
    let expected_binding = expected.clone();
    app.bind_service_publisher(Arc::new(move |service| {
        let snapshot = service.published().unwrap().1;
        assert_eq!(
            snapshot.binding.supervisor_session_id,
            expected_binding.supervisor_session_id
        );
        assert_eq!(
            snapshot.binding.binding_revision,
            expected_binding.binding_revision
        );
        called.fetch_add(1, Ordering::SeqCst);
    }));
    let mut stale = expected.clone();
    stale.binding_revision += 1;
    assert!(app.initialize_scope(scope_owner(), stale).await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(app
        .initialize_scope(scope_owner(), expected.clone())
        .await
        .unwrap());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(!app.initialize_scope(scope_owner(), expected).await.unwrap());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

mod owned_cancel_shutdown {
    use super::*;
    use async_trait::async_trait;
    use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolSchema};
    use bamboo_domain::{Message, WorkspaceBinding};
    use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMStream, ProviderRegistry};
    use futures::FutureExt;
    use std::{
        collections::HashMap,
        panic::AssertUnwindSafe,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    use tokio::sync::{mpsc, oneshot};

    const WAIT: Duration = Duration::from_secs(20);
    const TASK: &str = "owned-cancel-task";

    struct Request {
        messages: Vec<Message>,
        reply: oneshot::Sender<LLMChunk>,
    }

    struct Provider {
        requests: mpsc::UnboundedSender<Request>,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl LLMProvider for Provider {
        async fn chat_stream(
            &self,
            messages: &[Message],
            tools: &[ToolSchema],
            _: Option<u32>,
            model: &str,
        ) -> std::result::Result<LLMStream, LLMError> {
            assert_eq!(model, "fixture-model");
            assert!(tools.iter().any(|tool| tool.function.name == "Task"));
            assert!(self.calls.fetch_add(1, Ordering::SeqCst) < 2);
            let (reply, response) = oneshot::channel();
            self.requests
                .send(Request {
                    messages: messages.to_vec(),
                    reply,
                })
                .map_err(|_| LLMError::Api("owned cancellation observer closed".into()))?;
            let chunk = response
                .await
                .map_err(|_| LLMError::Api("owned cancellation provider closed".into()))?;
            Ok(Box::pin(futures::stream::iter([
                Ok(chunk),
                Ok(LLMChunk::Done),
            ])))
        }
    }

    struct Fixture {
        state: Option<crate::AppState>,
        service: Option<Arc<TicketService>>,
        release: Option<oneshot::Sender<()>>,
        entered: oneshot::Receiver<()>,
        registered: oneshot::Receiver<tokio::task::JoinHandle<()>>,
        draining: oneshot::Receiver<()>,
        task: Option<tokio::task::JoinHandle<()>>,
    }

    impl Fixture {
        async fn new(root: &Path, provider: Arc<Provider>) -> Self {
            let mut config = config(true).read().await.clone();
            config.features.ticket_dispatch = true;
            config.run_budget.max_rounds = Some(4);
            config.subagents_mut().max_concurrent = 1;
            config
                .memory_mut()
                .get_or_insert_with(Default::default)
                .auto_dream_enabled = false;
            let state = crate::AppState::new_with_provider_and_memory_store(
                root.to_path_buf(),
                config,
                provider.clone(),
                bamboo_memory::memory_store::MemoryStore::new(root.join("jiandu")),
            )
            .await
            .unwrap();
            state.provider_registry.replace_with(ProviderRegistry::new(
                HashMap::from([("openai".into(), provider as Arc<dyn LLMProvider>)]),
                "openai".into(),
            ));
            let service = state.tickets.service().unwrap();
            let (entered, entered_rx) = oneshot::channel();
            let (release, release_rx) = oneshot::channel();
            let (registered, registered_rx) = oneshot::channel();
            let (draining, draining_rx) = oneshot::channel();
            *state.tickets.cancellation_probe.lock() = CancellationProbe {
                tail: Some((entered, release_rx)),
                registered: Some(registered),
                draining: Some(draining),
            };
            Self {
                state: Some(state),
                service: Some(service),
                release: Some(release),
                entered: entered_rx,
                registered: registered_rx,
                draining: draining_rx,
                task: None,
            }
        }

        fn release_tail(&mut self) {
            if let Some(release) = self.release.take() {
                let _ = release.send(());
            }
        }

        async fn join_task(&mut self) -> std::result::Result<(), tokio::task::JoinError> {
            if self.task.is_none() {
                self.task = self.registered.try_recv().ok();
            }
            match self.task.take() {
                Some(task) => task.await,
                None => Ok(()),
            }
        }

        // The caller catches assertion panics before reaching this cleanup.
        // Even the broken, untracked task has an independently retained join.
        async fn cleanup(&mut self) -> std::result::Result<(), tokio::task::JoinError> {
            self.release_tail();
            let joined = self.join_task().await;
            if let Some(state) = &self.state {
                state.shutdown().await;
            }
            drop(self.service.take());
            drop(self.state.take());
            joined
        }
    }

    fn task_chunk() -> LLMChunk {
        LLMChunk::ToolCalls(vec![ToolCall {
            id: TASK.into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "Task".into(),
                arguments: json!({"tasks":[{
                    "id":"before-cancel", "content":"One canonical plan write", "status":"completed"
                }]})
                .to_string(),
            },
        }])
    }

    async fn start(
        state: &crate::AppState,
        service: &Arc<TicketService>,
        owner: &Authority,
        workspace: &ExecutionWorkspace,
    ) -> (String, String) {
        let project = state
            .project_store
            .create("Owned cancellation", None)
            .unwrap();
        let project = state
            .project_store
            .bind_workspace(
                &project.id,
                project.revision,
                WorkspaceBinding {
                    path: workspace.repo.clone(),
                    label: None,
                    git_common_dir: None,
                },
            )
            .unwrap();
        let create = service
            .prepare_command(
                owner,
                "owned-cancel-create",
                vec![
                    Operation::Create {
                        temp_id: "work".into(),
                        kind: TicketKind::Work,
                        parent: None,
                        contract: contract(),
                        depends_on: BTreeSet::new(),
                    },
                    Operation::AssociateProject {
                        ticket_id: "work".into(),
                        project_id: project.id.clone(),
                    },
                ],
            )
            .unwrap();
        let work = state
            .tickets
            .update(owner.principal().clone(), &create)
            .await
            .unwrap()
            .ids["work"]
            .clone();
        let snapshot = service.published().unwrap().1;
        let ready = service
            .prepare_command(
                owner,
                "owned-cancel-ready",
                vec![
                    Operation::SetProjectContext {
                        work_id: work.clone(),
                        context: ProjectContextSnapshot {
                            project_id: project.id.clone(),
                            project_revision: project.revision,
                            resource_revision: project.resource_revision,
                            workspace_binding: project.workspace_bindings[0].clone(),
                            contract_revision: snapshot.tickets[&work].contract_revision,
                            contract: contract(),
                            sources: BTreeMap::new(),
                        },
                    },
                    Operation::Ready {
                        work_id: work.clone(),
                    },
                ],
            )
            .unwrap();
        state
            .tickets
            .update(owner.principal().clone(), &ready)
            .await
            .unwrap();
        let start = service
            .prepare_command(
                owner,
                "owned-cancel-start",
                vec![Operation::Start {
                    work_id: work.clone(),
                    temp_id: "assignment".into(),
                    workspace: Some(workspace.clone()),
                }],
            )
            .unwrap();
        let dispatched = state
            .tickets
            .dispatch(owner.principal().clone(), &start)
            .await
            .unwrap();
        assert_eq!(dispatched["errors"], json!([]));
        (
            work,
            dispatched["receipt"]["ids"]["assignment"]
                .as_str()
                .unwrap()
                .into(),
        )
    }

    #[tokio::test]
    async fn native_cancel_shutdown_joins_owned_tail_and_preserves_writer_authority() {
        let root = tempfile::tempdir().unwrap();
        let physical = root.path().canonicalize().unwrap();
        let workspace = prepared_git_workspace(&physical);
        let (requests_tx, mut requests) = mpsc::unbounded_channel();
        let provider = Arc::new(Provider {
            requests: requests_tx,
            calls: AtomicUsize::new(0),
        });
        let mut f = Fixture::new(&physical, provider.clone()).await;
        let writer = Arc::downgrade(f.service.as_ref().unwrap());
        let mut early_return = false;
        let exercise = async {
            let state = f.state.as_ref().unwrap();
            let service = f.service.as_ref().unwrap();
            let (_, owner) = state
                .tickets
                .authority(Principal::User {
                    user_id: "host-owner".into(),
                })
                .await
                .unwrap();
            let binding = service.published().unwrap().1.binding;
            let scope = physical
                .join("tickets")
                .join(binding.scope_id.strip_prefix("supervisor/").unwrap());
            let (work, assignment) = start(state, service, &owner, &workspace).await;
            let first = tokio::time::timeout(WAIT, requests.recv())
                .await
                .unwrap()
                .unwrap();
            first.reply.send(task_chunk()).unwrap();
            let held = tokio::time::timeout(WAIT, requests.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(held
                .messages
                .iter()
                .any(|message| message.tool_call_id.as_deref() == Some(TASK)
                    && message.tool_success == Some(true)));
            let running = service.published().unwrap().1;
            let runtime = running.assignments[&assignment].runtime.clone().unwrap();
            let plan = canonical_bytes(&running.assignments[&assignment].plan).unwrap();
            let task_receipt_id = format!(
                "worker-plan/{}/{TASK}",
                running.assignments[&assignment].dispatch_key
            );
            let task_receipt = canonical_bytes(&running.receipts[&task_receipt_id]).unwrap();
            let cancel = service
                .prepare_command(
                    &owner,
                    "owned-cancel",
                    vec![Operation::Cancel {
                        work_id: work.clone(),
                    }],
                )
                .unwrap();
            state
                .tickets
                .update(owner.principal().clone(), &cancel)
                .await
                .unwrap();
            f.task = Some(
                tokio::time::timeout(WAIT, &mut f.registered)
                    .await
                    .unwrap()
                    .unwrap(),
            );
            tokio::time::timeout(WAIT, &mut f.entered)
                .await
                .unwrap()
                .unwrap();
            assert!(service.published().unwrap().1.assignments[&assignment].process_stopped);
            assert!(held.reply.send(task_chunk()).is_err());
            assert_eq!(provider.calls.load(Ordering::SeqCst), 2);

            let mut shutdown = Box::pin(state.shutdown());
            early_return = tokio::time::timeout(WAIT, async {
                tokio::select! {
                    biased;
                    _ = &mut shutdown => true,
                    draining = &mut f.draining => { draining.unwrap(); false }
                }
            })
            .await
            .unwrap();
            drop(shutdown);
            let child = state
                .storage
                .load_session(&runtime.session_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(child.metadata["runtime.local_task.state"], "stopped");
            assert!(matches!(
                child.last_run_status().as_deref(),
                Some("completed" | "error" | "timeout" | "cancelled" | "skipped")
            ));
            let stop_receipt: Value =
                serde_json::from_str(&child.metadata["ticket.runtime.owned_stop.v1"]).unwrap();
            assert_eq!(stop_receipt["receipt"], json!(runtime));
            assert_eq!(stop_receipt["completed"], false);
            assert!(
                !uuid::Uuid::parse_str(stop_receipt["task_instance"].as_str().unwrap())
                    .unwrap()
                    .is_nil()
            );
            let cancelled = service.published().unwrap().1;
            let cancel_receipt = canonical_bytes(&cancelled.receipts["owned-cancel"]).unwrap();
            assert_eq!(cancelled.tickets[&work].state, WorkState::Cancelled);
            assert_eq!(
                cancelled.assignments[&assignment].state,
                AssignmentState::Cancelled
            );

            let retained = if early_return {
                None
            } else {
                // Drop the first shutdown waiter while the tail is held. A
                // second shutdown must still wait on the same owned future.
                let (draining, mut observed) = oneshot::channel();
                state.tickets.cancellation_probe.lock().draining = Some(draining);
                let mut resumed = Box::pin(state.shutdown());
                tokio::time::timeout(WAIT, async {
                    tokio::select! {
                        biased;
                        _ = &mut resumed => panic!("second shutdown returned before the owned tail"),
                        draining = &mut observed => draining.unwrap(),
                    }
                }).await.unwrap();
                assert!(futures::poll!(&mut resumed).is_pending());
                if let Some(release) = f.release.take() {
                    let _ = release.send(());
                }
                f.task.take().unwrap().await.unwrap();
                tokio::time::timeout(WAIT, resumed).await.unwrap();
                Some(service.clone())
            };
            drop(f.service.take());
            drop(f.state.take());
            // In RED the *only* strong Arc is the real detached canceller;
            // in PASS it is our explicitly retained external negative control.
            assert_eq!(writer.strong_count(), 1);
            assert!(
                matches!(TicketService::open(&scope, binding.clone()), Err(Error::AuthorityUnavailable(message)) if message.contains("another local writer holds the process lock"))
            );
            drop(retained);
            f.release_tail();
            f.join_task().await.unwrap();
            assert!(
                writer.upgrade().is_none(),
                "every original TicketService owner must be gone"
            );

            f = Fixture::new(&physical, provider.clone()).await;
            let snapshot = f.service.as_ref().unwrap().published().unwrap().1;
            assert_eq!(snapshot.tickets[&work].state, WorkState::Cancelled);
            assert!(snapshot.tickets[&work].active_assignment.is_none());
            assert!(snapshot.tickets[&work].accepted_submission.is_none());
            assert!(snapshot.assignments[&assignment].process_stopped);
            assert_eq!(
                canonical_bytes(&snapshot.assignments[&assignment].plan).unwrap(),
                plan
            );
            assert_eq!(
                canonical_bytes(&snapshot.receipts[&task_receipt_id]).unwrap(),
                task_receipt
            );
            assert_eq!(
                canonical_bytes(&snapshot.receipts["owned-cancel"]).unwrap(),
                cancel_receipt
            );
            assert!(
                bamboo_engine::ticket_worker_plan::TicketWorkerPlan::from_runtime_receipt(
                    f.service.as_ref().unwrap().clone(),
                    &assignment
                )
                .is_err()
            );
            let saved_child = f
                .state
                .as_ref()
                .unwrap()
                .storage
                .load_session(&runtime.session_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                saved_child.metadata["ticket.runtime.owned_stop.v1"],
                child.metadata["ticket.runtime.owned_stop.v1"]
            );
            assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
        };
        let outcome = AssertUnwindSafe(exercise).catch_unwind().await;
        let cleanup = f.cleanup().await;
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
        cleanup.unwrap();
        assert!(!early_return, "Host shutdown returned while its detached cancellation task retained the sole writer Arc; real same-root open was denied until that task exited");
    }

    #[tokio::test]
    async fn closed_cancellation_admission_cannot_spawn_a_new_writer_owner() {
        let root = tempfile::tempdir().unwrap();
        let physical = root.path().canonicalize().unwrap();
        let workspace = prepared_git_workspace(&physical);
        let (requests, _received) = mpsc::unbounded_channel();
        let provider = Arc::new(Provider {
            requests,
            calls: AtomicUsize::new(0),
        });
        let mut f = Fixture::new(&physical, provider.clone()).await;
        let exercise = async {
            let state = f.state.as_ref().unwrap();
            let service = f.service.as_ref().unwrap();
            let (_, owner) = state
                .tickets
                .authority(Principal::User {
                    user_id: "host-owner".into(),
                })
                .await
                .unwrap();
            // Canonical queued work deliberately never receives a RunSpec.
            let create = service
                .prepare_command(
                    &owner,
                    "closed-create",
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
                        Operation::Start {
                            work_id: "work".into(),
                            temp_id: "assignment".into(),
                            workspace: Some(workspace),
                        },
                    ],
                )
                .unwrap();
            let ids = service.execute(&owner, &create).unwrap().ids;
            state.tickets.close_cancellation_tasks();
            let cancel = service
                .prepare_command(
                    &owner,
                    "closed-cancel",
                    vec![Operation::Cancel {
                        work_id: ids["work"].clone(),
                    }],
                )
                .unwrap();
            state
                .tickets
                .update(owner.principal().clone(), &cancel)
                .await
                .unwrap();
            match f.registered.try_recv() {
                Ok(task) => {
                    f.task = Some(task);
                    panic!("closed cancellation admission created a new asynchronous writer owner");
                }
                Err(oneshot::error::TryRecvError::Empty) => {}
                Err(error) => panic!("cancellation registration observer disappeared: {error}"),
            }
            assert!(state.tickets.cancellation_probe.lock().tail.is_some());
            assert!(state.tickets.cancellation_tasks.lock().is_empty());
            let snapshot = service.published().unwrap().1;
            assert_eq!(snapshot.tickets[&ids["work"]].state, WorkState::Cancelled);
            assert!(snapshot.assignments[&ids["assignment"]].runtime.is_none());
            assert!(!snapshot.assignments[&ids["assignment"]].process_stopped);
            assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        };
        let outcome = AssertUnwindSafe(exercise).catch_unwind().await;
        let cleanup = f.cleanup().await;
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
        cleanup.unwrap();
    }
}
