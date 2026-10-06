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
