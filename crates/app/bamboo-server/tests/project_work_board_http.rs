use actix_web::{http::StatusCode, web};
use bamboo_server::AppState;

#[actix_web::test]
async fn review_create_revalidates_inherited_project_and_keeps_exact_replays() {
    use bamboo_tickets::*;
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::Arc,
    };

    fn create(temp_id: &str, kind: TicketKind, parent: Option<String>) -> Operation {
        Operation::Create {
            temp_id: temp_id.into(),
            kind,
            parent,
            depends_on: BTreeSet::new(),
            contract: Contract {
                title: temp_id.into(),
                objective: "Synthetic membership validation".into(),
                constraints: vec!["No provider execution".into()],
                acceptance: vec!["Fresh Project observation".into()],
                user_acceptance_required: true,
                allowed_tools: BTreeSet::from(["Task".into()]),
            },
        }
    }

    let root = tempfile::tempdir().unwrap();
    let mut state = AppState::new_with_memory_store(
        root.path().to_path_buf(),
        bamboo_memory::memory_store::MemoryStore::new(root.path().join("jiandu")),
    )
    .await
    .unwrap();
    *state.config.write().await = serde_json::from_value(serde_json::json!({
        "provider":"openai","features":{"ticket_mutation":true,"ticket_dispatch":false},
        "providers":{"openai":{"api_key":"fixture-unused","model":"fixture-unused"}}
    }))
    .unwrap();
    state.tickets = Arc::new(
        bamboo_server::app_state::ticket_application::TicketApplication::open(
            root.path(),
            state.storage.clone(),
            state.config.clone(),
        )
        .await,
    );
    let project = state
        .project_store
        .create("Inherited membership", None)
        .unwrap();
    // The caller is an already verified typed Host owner, not HTTP-derived authority.
    let principal = Principal::User {
        user_id: "host-owner".into(),
    };
    let (service, owner) = state.tickets.authority(principal.clone()).await.unwrap();
    let seed = service
        .prepare_command(
            &owner,
            "existing-parent",
            vec![
                create("parent", TicketKind::Goal, None),
                Operation::AssociateProject {
                    ticket_id: "parent".into(),
                    project_id: project.id.clone(),
                },
            ],
        )
        .unwrap();
    let seeded = state.tickets.update(principal.clone(), &seed).await.unwrap();
    let parent = seeded.ids["parent"].clone();
    let chain = service
        .prepare_command(
            &owner,
            "active-temp-chain",
            vec![
                create("root", TicketKind::Goal, None),
                Operation::AssociateProject {
                    ticket_id: "root".into(),
                    project_id: project.id.clone(),
                },
                create("nested", TicketKind::Goal, Some("root".into())),
                create("work", TicketKind::Work, Some("nested".into())),
                create("spare", TicketKind::Work, Some("root".into())),
            ],
        )
        .unwrap();
    let receipt = state.tickets.update(principal.clone(), &chain).await.unwrap();
    let snapshot = service.published().unwrap().1;
    for temp in ["root", "nested", "work", "spare"] {
        assert_eq!(snapshot.project.links[&receipt.ids[temp]], project.id);
    }
    let definition = |name: &str| SprintDefinition {
        name: name.into(),
        goal: "Synthetic archive contract".into(),
        starts_at: None,
        ends_at: None,
    };
    let sprint_command = |operation_id: &str, id: &str, action| {
        let snapshot = service.published().unwrap().1;
        SprintCommand {
            operation_id: operation_id.into(),
            binding: snapshot.binding,
            expected_seq: snapshot.seq,
            expected_epoch: snapshot.authority_epoch,
            project_id: project.id.clone(),
            sprint_id: SprintId::parse(id).unwrap(),
            source: "typed Host owner archive contract".into(),
            action,
        }
    };
    let sprint_create = sprint_command(
        "create-first-sprint",
        "one",
        SprintAction::Create {
            definition: definition("one"),
        },
    );
    let sprint_receipt = state
        .tickets
        .sprint_command(principal.clone(), &sprint_create)
        .await
        .unwrap();
    for (operation_id, id, action) in [
        (
            "create-second-sprint",
            "two",
            SprintAction::Create {
                definition: definition("two"),
            },
        ),
        (
            "add-before-archive",
            "one",
            SprintAction::Add {
                expected_revision: 1,
                work_id: receipt.ids["work"].clone(),
            },
        ),
        (
            "start-before-archive",
            "one",
            SprintAction::Start {
                expected_revision: 2,
            },
        ),
    ] {
        state
            .tickets
            .sprint_command(principal.clone(), &sprint_command(operation_id, id, action))
            .await
            .unwrap();
    }
    state
        .project_store
        .archive(&project.id, project.revision)
        .unwrap();
    let before = service.published().unwrap();
    assert_eq!(
        state.tickets.update(principal.clone(), &chain).await.unwrap(),
        receipt
    );
    assert_eq!(service.published().unwrap().0, before.0);
    assert_eq!(
        state
            .tickets
            .sprint_command(principal.clone(), &sprint_create)
            .await
            .unwrap(),
        sprint_receipt
    );
    let mut changed = chain.clone();
    changed.operations.push(create("changed-retry", TicketKind::Work, None));
    assert!(matches!(
        state.tickets.update(principal.clone(), &changed).await,
        Err(Error::IdempotencyConflict)
    ));
    let mut changed_sprint = sprint_create.clone();
    changed_sprint.source = "different retry input".into();
    assert!(matches!(
        state.tickets.sprint_command(principal.clone(), &changed_sprint).await,
        Err(Error::IdempotencyConflict)
    ));
    assert_eq!(service.published().unwrap().0, before.0);

    for (operation_id, operations) in [
        (
            "archived-explicit-project",
            vec![
                create("child", TicketKind::Work, None),
                Operation::AssociateProject {
                    ticket_id: "child".into(),
                    project_id: project.id.clone(),
                },
            ],
        ),
        (
            "archived-existing-parent",
            vec![create("child", TicketKind::Work, Some(parent.clone()))],
        ),
        (
            "archived-nested-parent",
            vec![
                create(
                    "new-goal",
                    TicketKind::Goal,
                    Some(receipt.ids["nested"].clone()),
                ),
                create("child", TicketKind::Work, Some("new-goal".into())),
            ],
        ),
    ] {
        let command = service
            .prepare_command(&owner, operation_id, operations)
            .unwrap();
        let result = state.tickets.update(principal.clone(), &command).await;
        assert!(
            matches!(&result, Err(Error::InvalidTransition(reason)) if reason.contains("archived")),
            "Create must revalidate inherited Project membership before publication: {result:?}"
        );
        let after = service.published().unwrap();
        assert_eq!(after.0, before.0);
        assert_eq!(
            canonical_bytes(&after.1).unwrap(),
            canonical_bytes(&before.1).unwrap()
        );
    }
    for (index, (id, action)) in [
        (
            "new",
            SprintAction::Create {
                definition: definition("new"),
            },
        ),
        (
            "one",
            SprintAction::Edit {
                expected_revision: 3,
                definition: definition("edited"),
            },
        ),
        (
            "two",
            SprintAction::Start {
                expected_revision: 1,
            },
        ),
        (
            "one",
            SprintAction::Add {
                expected_revision: 3,
                work_id: receipt.ids["spare"].clone(),
            },
        ),
        (
            "one",
            SprintAction::Remove {
                expected_revision: 3,
                work_id: receipt.ids["work"].clone(),
            },
        ),
        (
            "one",
            SprintAction::Close {
                expected_revision: 3,
                dispositions: BTreeMap::from([(
                    receipt.ids["work"].clone(),
                    SprintDestination::Backlog,
                )]),
                target_revisions: BTreeMap::new(),
            },
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let command = sprint_command(&format!("archived-sprint-{index}"), id, action);
        let result = state.tickets.sprint_command(principal.clone(), &command).await;
        assert!(
            matches!(&result, Err(Error::InvalidTransition(reason)) if reason.contains("archived")),
            "new Sprint mutations must revalidate the active Project: {result:?}"
        );
        let after = service.published().unwrap();
        assert_eq!(after.0, before.0);
        assert_eq!(
            canonical_bytes(&after.1).unwrap(),
            canonical_bytes(&before.1).unwrap()
        );
    }

    // A temporary ID shadows the old canonical spelling, exactly as core allocation does.
    // This new unlinked Goal and its child do not inherit the archived original Goal's link.
    let unlinked = service
        .prepare_command(
            &owner,
            "unlinked-shadowed-parent",
            vec![
                create(&parent, TicketKind::Goal, None),
                create("child", TicketKind::Work, Some(parent.clone())),
            ],
        )
        .unwrap();
    let unlinked = state.tickets.update(principal, &unlinked).await.unwrap();
    let after = service.published().unwrap().1;
    assert!(!after.project.links.contains_key(&unlinked.ids[&parent]));
    assert!(!after.project.links.contains_key(&unlinked.ids["child"]));
    assert_eq!(after.project.links[&parent], project.id);
    assert!(after.assignments.is_empty());
    assert!(after.intents.is_empty());
}

#[actix_web::test]
async fn project_board_owner_route_uses_explicit_membership_and_rejects_unknown_project() {
    use actix_web::{test, App};
    use std::sync::Arc;
    let root = tempfile::tempdir().unwrap();
    let mut state = AppState::new_with_memory_store(
        root.path().to_path_buf(),
        bamboo_memory::memory_store::MemoryStore::new(root.path().join("jiandu")),
    )
    .await
    .unwrap();
    *state.config.write().await = serde_json::from_value(serde_json::json!({
        "provider":"openai","features":{"ticket_mutation":true},
        "providers":{"openai":{"api_key":"fixture","model":"fixture-model"}}
    }))
    .unwrap();
    state.tickets = Arc::new(
        bamboo_server::app_state::ticket_application::TicketApplication::open(
            root.path(),
            state.storage.clone(),
            state.config.clone(),
        )
        .await,
    );
    let project = state.project_store.create("Board", None).unwrap();
    let service = state.tickets.service().unwrap();
    let state = web::Data::new(state);
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(bamboo_server::configure_routes),
    )
    .await;
    let request = serde_json::json!({"project_id":project.id,"limit":10});
    let denied = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/tickets/project-board")
            .peer_addr("203.0.113.1:5700".parse().unwrap())
            .set_json(&request)
            .to_request(),
    )
    .await;
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    let snapshot = service.published().unwrap().1;
    let command = serde_json::json!({"operation_id":"link-project", "binding":snapshot.binding,
        "expected_seq":snapshot.seq,"expected_epoch":snapshot.authority_epoch,
        "operations":[{"op":"create","temp_id":"w","kind":"work","parent":null,
        "depends_on":[],"contract":{"title":"Delivery","objective":"Build","constraints":[],
        "acceptance":["Test"],"user_acceptance_required":true,"allowed_tools":["Task"]}},
        {"op":"associate_project","ticket_id":"w","project_id":project.id}]});
    let updated = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/tickets/update")
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .set_json(&command)
            .to_request(),
    )
    .await;
    assert_eq!(updated.status(), StatusCode::OK);
    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/tickets/project-board")
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .set_json(&request)
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = test::read_body_json(response).await;
    assert_eq!(body["coverage"], "authoritative_scope");
    assert_eq!(body["data"]["counts"]["todo"], 1);
    assert_eq!(body["data"]["rows"][0]["stage"], "todo");
    assert!(body["data"]["rows"][0]["context"].is_null());
    let snapshot = service.published().unwrap().1;
    let mut invalid = command;
    invalid["operation_id"] = "missing-project".into();
    invalid["expected_seq"] = snapshot.seq.into();
    invalid["operations"][1]["project_id"] = "missing-project".into();
    let before = service.published().unwrap().0;
    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/tickets/update")
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .set_json(&invalid)
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(service.published().unwrap().0, before);
    // Host validation uses registered Project facts, not client-provided paths
    // or revisions. Context metadata cannot create a grant or Assignment.
    let workspace = tempfile::tempdir().unwrap();
    let manifest = state
        .project_store
        .bind_workspace(
            &project.id,
            project.revision,
            bamboo_domain::WorkspaceBinding {
                path: workspace.path().to_string_lossy().into(),
                label: None,
                git_common_dir: None,
            },
        )
        .unwrap();
    let snapshot = service.published().unwrap().1;
    let work = snapshot.tickets.values().next().unwrap();
    let context = serde_json::json!({"project_id":project.id,"project_revision":manifest.revision,
        "resource_revision":manifest.resource_revision,"workspace_binding":manifest.workspace_bindings[0],
        "contract_revision":work.contract_revision,"contract":work.contract,"sources":{"spec":"v1"}});
    for (operation_id, supplied, status) in [
        (
            "stale-resource",
            {
                let mut c = context.clone();
                c["resource_revision"] = 0.into();
                c
            },
            StatusCode::CONFLICT,
        ),
        (
            "unregistered-binding",
            {
                let mut c = context.clone();
                c["workspace_binding"]["path"] = "/unregistered".into();
                c
            },
            StatusCode::CONFLICT,
        ),
        ("adopt-context", context, StatusCode::OK),
    ] {
        let request = serde_json::json!({"operation_id":operation_id,"binding":snapshot.binding,
            "expected_seq":snapshot.seq,"expected_epoch":snapshot.authority_epoch,
            "operations":[{"op":"set_project_context","work_id":work.id,"context":supplied}]});
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/tickets/update")
                .peer_addr("127.0.0.1:5700".parse().unwrap())
                .set_json(&request)
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), status, "{operation_id}");
        if status != StatusCode::OK {
            assert_eq!(service.published().unwrap().0, before);
        }
    }
    let snapshot = service.published().unwrap().1;
    assert_eq!(snapshot.project.work_contexts.len(), 1);
    assert!(snapshot.assignments.is_empty());
}

/// Real loopback routes with synthetic domain records and no provider execution.
/// An explicit receipt path keeps the same test binary alive for Lotus QA.
#[actix_web::test]
async fn pending_context_live_routes_preserve_adoption_and_revalidate_original_dispatch() {
    use actix_web::{App, HttpServer};
    use bamboo_domain::WorkspaceBinding;
    use bamboo_tickets::*;
    use std::{collections::BTreeSet, sync::Arc};

    let root = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let mut state = AppState::new_with_memory_store(
        root.path().to_path_buf(),
        bamboo_memory::memory_store::MemoryStore::new(root.path().join("jiandu")),
    )
    .await
    .unwrap();
    *state.config.write().await = serde_json::from_value(serde_json::json!({
        "provider":"openai", "features":{"ticket_mutation":true,"ticket_dispatch":true},
        "providers":{"openai":{"api_key":"synthetic-unused","model":"synthetic-unused"}}
    }))
    .unwrap();
    state.tickets = Arc::new(
        bamboo_server::app_state::ticket_application::TicketApplication::open(
            root.path(),
            state.storage.clone(),
            state.config.clone(),
        )
        .await,
    );
    let project = state
        .project_store
        .create("Context boundary fixture", None)
        .unwrap();
    let manifest = state
        .project_store
        .bind_workspace(
            &project.id,
            project.revision,
            WorkspaceBinding {
                path: workspace
                    .path()
                    .canonicalize()
                    .unwrap()
                    .to_string_lossy()
                    .into(),
                label: None,
                git_common_dir: None,
            },
        )
        .unwrap();
    let principal = Principal::User {
        user_id: "host-owner".into(),
    };
    let (service, owner) = state.tickets.authority(principal.clone()).await.unwrap();
    let contract = Contract {
        title: "Live metadata boundary".into(),
        objective: "Synthetic context proof".into(),
        constraints: vec!["No provider or real user data".into()],
        acceptance: vec!["Exact adopted revision".into()],
        user_acceptance_required: true,
        allowed_tools: BTreeSet::from(["Task".into()]),
    };
    let adopted = ProjectContextSnapshot {
        project_id: project.id.clone(),
        project_revision: manifest.revision,
        resource_revision: manifest.resource_revision,
        workspace_binding: manifest.workspace_bindings[0].clone(),
        contract_revision: 1,
        contract: contract.clone(),
        sources: std::collections::BTreeMap::from([("fixture-spec".into(), "v1".into())]),
    };
    let command = service
        .prepare_command(
            &owner,
            "live-create",
            vec![
                Operation::Create {
                    temp_id: "w".into(),
                    kind: TicketKind::Work,
                    parent: None,
                    contract: contract.clone(),
                    depends_on: BTreeSet::new(),
                },
                Operation::AssociateProject {
                    ticket_id: "w".into(),
                    project_id: project.id.clone(),
                },
                Operation::SetProjectContext {
                    work_id: "w".into(),
                    context: adopted.clone(),
                },
                Operation::Ready {
                    work_id: "w".into(),
                },
                Operation::Create {
                    temp_id: "cancelled".into(),
                    kind: TicketKind::Work,
                    parent: None,
                    contract: Contract {
                        title: "Cancelled fixture".into(),
                        ..contract
                    },
                    depends_on: BTreeSet::new(),
                },
                Operation::AssociateProject {
                    ticket_id: "cancelled".into(),
                    project_id: project.id.clone(),
                },
            ],
        )
        .unwrap();
    let created = service.execute(&owner, &command).unwrap();
    let cancel = service
        .prepare_command(
            &owner,
            "live-cancel",
            vec![Operation::Cancel {
                work_id: created.ids["cancelled"].clone(),
            }],
        )
        .unwrap();
    service.execute(&owner, &cancel).unwrap();
    let work = created.ids["w"].clone();
    let path = workspace
        .path()
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let start = service
        .prepare_command(
            &owner,
            "live-start",
            vec![Operation::Start {
                work_id: work.clone(),
                temp_id: "assignment".into(),
                workspace: Some(ExecutionWorkspace {
                    repo: path.clone(),
                    base_commit: "a".repeat(40),
                    branch: "synthetic-fixture".into(),
                    worktree: path.clone(),
                    write_roots: vec![path.clone()],
                    claims: BTreeSet::from([format!("worktree:{path}")]),
                }),
            }],
        )
        .unwrap();
    let started = service.execute(&owner, &start).unwrap();
    let assignment = started.ids["assignment"].clone();
    let original = service.published().unwrap().1;
    let a = &original.assignments[&assignment];
    let dispatch_key = a.dispatch_key.clone();
    let spec_hash = original.intents[&dispatch_key].spec_hash.clone();
    let updated = state
        .project_store
        .bind_workspace(
            &project.id,
            manifest.revision,
            WorkspaceBinding {
                path: other
                    .path()
                    .canonicalize()
                    .unwrap()
                    .to_string_lossy()
                    .into(),
                label: None,
                git_common_dir: None,
            },
        )
        .unwrap();
    // Adding another binding changes Project metadata, not the resource
    // counter. Advance the actual resource ledger for the stale-intent guard.
    let updated = state
        .project_store
        .bump_resource_revision(&project.id, updated.revision)
        .unwrap();
    assert!(updated.resource_revision > adopted.resource_revision);
    let mut pending = adopted.clone();
    pending.project_revision = updated.revision;
    pending.resource_revision = updated.resource_revision;
    let queue = service
        .prepare_command(
            &owner,
            "live-queue",
            vec![Operation::SetProjectContext {
                work_id: work.clone(),
                context: pending.clone(),
            }],
        )
        .unwrap();
    state
        .tickets
        .update(principal.clone(), &queue)
        .await
        .unwrap();
    // The newer candidate must not authorize the old immutable pending intent.
    // Replaying reaches ensure() before any adapter/provider admission.
    let replay = state.tickets.dispatch(principal, &start).await.unwrap();
    assert_eq!(replay["runtime"], serde_json::json!([]));
    assert_eq!(replay["errors"][0]["status_code"], 409, "{replay}");
    assert!(service.published().unwrap().1.assignments[&assignment]
        .runtime
        .is_none());
    assert_eq!(
        service.published().unwrap().1.project.assignment_contexts,
        original.project.assignment_contexts
    );
    // Install only synthetic runtime facts for UI observation, never a real run.
    let runtime = Authority::from_verified_host(original.binding, Principal::Runtime);
    let command = service
        .prepare_command(
            &runtime,
            "live-synthetic-running",
            vec![
                Operation::Admitted {
                    assignment_id: assignment.clone(),
                    receipt: RuntimeReceipt {
                        dispatch_key,
                        spec_hash,
                        run_id: "synthetic-no-provider-run".into(),
                        session_id: "synthetic-worker".into(),
                    },
                },
                Operation::Running {
                    assignment_id: assignment.clone(),
                },
            ],
        )
        .unwrap();
    service.execute(&runtime, &command).unwrap();
    state.config.write().await.features.ticket_dispatch = false;
    let state = web::Data::new(state);
    let address =
        std::env::var("BAMBOO_PROJECT_BOARD_FIXTURE_ADDR").unwrap_or_else(|_| "127.0.0.1:0".into());
    let address: std::net::SocketAddr = address.parse().unwrap();
    assert!(address.ip().is_loopback());
    let listener = std::net::TcpListener::bind(address).unwrap();
    let address = listener.local_addr().unwrap();
    let server_state = state.clone();
    let frontend = std::env::var_os("BAMBOO_PROJECT_BOARD_FRONTEND").map(std::path::PathBuf::from);
    let server = HttpServer::new(move || {
        let frontend = frontend.clone();
        App::new()
            .app_data(server_state.clone())
            .configure(bamboo_server::configure_routes)
            .configure(move |cfg| {
                if let Some(path) = frontend {
                    cfg.service(actix_files::Files::new("/", path).index_file("index.html"));
                }
            })
    })
    .workers(1)
    .listen(listener)
    .unwrap()
    .run();
    let handle = server.handle();
    let task = actix_web::rt::spawn(server);
    let base = format!("http://{address}");
    let client = reqwest::Client::new();
    assert!(client
        .get(format!("{base}/healthz"))
        .send()
        .await
        .unwrap()
        .status()
        .is_success());
    assert!(client
        .get(format!("{base}/api/v1/projects"))
        .send()
        .await
        .unwrap()
        .status()
        .is_success());
    let board = client
        .post(format!("{base}/api/v1/tickets/project-board"))
        .json(&serde_json::json!({"project_id":project.id,"include_cancelled":true,"limit":100}))
        .send()
        .await
        .unwrap();
    assert!(board.status().is_success());
    let board: serde_json::Value = board.json().await.unwrap();
    assert_eq!(board["data"]["counts"]["doing"], 1);
    assert_eq!(board["data"]["counts"]["cancelled"], 1);
    let row = board["data"]["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["ticket"]["id"] == work)
        .unwrap();
    assert_eq!(
        row["adopted_context"],
        serde_json::to_value(&adopted).unwrap()
    );
    assert_eq!(
        row["pending_context"],
        serde_json::to_value(&pending).unwrap()
    );
    let inspect=client.post(format!("{base}/api/v1/tickets/inspect")).json(&serde_json::json!({"ids":[work],"depth":0,"budget_bytes":32768,"fixed_commit":board["snapshot"]["commit"]})).send().await.unwrap();
    assert!(inspect.status().is_success());
    assert!(inspect.text().await.unwrap().contains(&assignment));
    if let Some(receipt) = std::env::var_os("BAMBOO_PROJECT_BOARD_FIXTURE_RECEIPT") {
        let receipt = std::path::PathBuf::from(receipt);
        let stop = receipt.with_extension("stop");
        let pause = receipt.with_extension("pause-stop");
        let proof = serde_json::json!({"base_url":base,"root":root.path(),"project_id":project.id,"work_id":work,"assignment_id":assignment,
            "auth":"loopback local_bypass; no credentials","runtime":"synthetic records only; no provider execution","ticket_dispatch":false,
            "adopted":adopted,"pending":pending,"pause_stop_marker":pause,"shutdown_marker":stop,
            "source":std::env::var("BAMBOO_FIXTURE_SOURCE").ok(),"binary":std::env::current_exe().unwrap()});
        std::fs::write(&receipt, serde_json::to_vec_pretty(&proof).unwrap()).unwrap();
        let mut paused = false;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(900);
        while !stop.exists() && tokio::time::Instant::now() < deadline {
            if !paused && pause.exists() {
                let command = service
                    .prepare_command(
                        &owner,
                        "live-pause",
                        vec![Operation::Pause {
                            work_id: work.clone(),
                            reason: "fixture safe boundary".into(),
                        }],
                    )
                    .unwrap();
                service.execute(&owner, &command).unwrap();
                let command = service
                    .prepare_command(
                        &runtime,
                        "live-confirm-stop",
                        vec![Operation::ConfirmStopped {
                            assignment_id: assignment.clone(),
                            effects_reconciled: true,
                        }],
                    )
                    .unwrap();
                service.execute(&runtime, &command).unwrap();
                paused = true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }
    handle.stop(true).await;
    task.await.unwrap().unwrap();
}
