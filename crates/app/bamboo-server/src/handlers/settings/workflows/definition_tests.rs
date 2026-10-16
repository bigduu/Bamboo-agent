use super::*;
use actix_web::{test as webtest, App};

fn definition(id: &str, revision: u64) -> Value {
    json!({
        "workflow_schema": 1, "id": id, "revision": revision,
        "steps": [{"id":"inspect", "type":"tool", "tool":"Read", "args":{}, "capabilities":["read"]}],
        "plan": {"type":"step", "step":"inspect"}
    })
}

fn routes() -> impl actix_web::dev::HttpServiceFactory {
    web::scope("/definitions")
        .route("/validate", web::post().to(validate_workflow_definition))
        .route("", web::post().to(create_workflow_definition))
        .route("/{id}", web::get().to(get_workflow_definition))
        .route("/{id}", web::put().to(update_workflow_definition))
}

#[test]
fn definition_validation_reuses_compiler_and_rejects_authority_fields() {
    assert!(validate_definition(&definition("authored-flow", 1)).is_ok());
    for id in [
        "../escaped",
        "bad/name",
        "",
        "..",
        ".hidden",
        "trailing.",
        "Upper",
        "unsafe_id",
        "contains space",
    ][..]
        .iter()
    {
        assert!(validate_definition(&definition(id, 1)).is_err());
    }
    assert!(validate_definition(&definition(&"x".repeat(65), 1)).is_err());
    for (section, field) in [("plan", "unexpected"), ("budgets", "max_cost_usd")] {
        let mut invalid = definition("authored-flow", 1);
        if section == "budgets" {
            invalid[section] =
                serde_json::to_value(bamboo_domain::WorkflowBudgets::default()).unwrap();
        }
        invalid[section][field] = json!(3);
        assert!(validate_definition(&invalid).is_err());
    }
    let mut invalid_step = definition("authored-flow", 1);
    invalid_step["steps"][0]["timeout_ms"] = json!(1000);
    assert!(validate_definition(&invalid_step).is_err());
    let mut nullable = definition("authored-flow", 1);
    nullable["output_schema"] = Value::Null;
    nullable["steps"][0]["output_schema"] = Value::Null;
    assert!(validate_definition(&nullable).is_ok());
    let mut invalid = definition("authored-flow", 1);
    invalid["plan"]["step"] = json!("missing");
    assert!(validate_definition(&invalid).is_err());
    invalid = definition("authored-flow", 1);
    invalid["workflow_schema"] = json!(2);
    assert!(validate_definition(&invalid).is_err());
    invalid = definition("authored-flow", 1);
    invalid["invocation_policy"] = json!({"automatic":true});
    assert!(validate_definition(&invalid).is_err());
    invalid = definition("authored-flow", 1);
    invalid["input_schema"] = json!({"type":"unknown-schema-kind"});
    assert!(validate_definition(&invalid).is_err());
    invalid = definition("authored-flow", 1);
    invalid["steps"][0]["args"] = json!({"$ref":{"from":"step","step":"inspect"}});
    assert!(
        validate_definition(&invalid).is_err(),
        "self-dependency cannot publish"
    );
    invalid = definition("authored-flow", 1);
    invalid["steps"][0]["args"] = json!({"too_large":"x".repeat(MAX_DEFINITION_BYTES)});
    assert!(validate_definition(&invalid).is_err());
}

#[test]
fn authored_skill_metadata_is_bounded_and_not_a_frontmatter_injection() {
    let description = "A description\n---\nname: hostile\nmetadata: injected";
    let bytes = skill_bytes("authored-flow", description).unwrap();
    assert_eq!(
        verify_authored_skill("authored-flow", &bytes).unwrap(),
        description
    );
    assert!(skill_bytes("authored-flow", " ").is_err());
    assert!(skill_bytes("authored-flow", &"x".repeat(1025)).is_err());
    let mut edited = bytes;
    edited.extend_from_slice(b"changed prompt\n");
    assert!(verify_authored_skill("authored-flow", &edited).is_err());
}

#[actix_web::test]
async fn authoring_create_read_update_conflicts_and_pinned_revisions() {
    let data = tempfile::tempdir().unwrap();
    let state = web::Data::new(AppState::new(data.path().to_path_buf()).await.unwrap());
    let app = webtest::init_service(App::new().app_data(state.clone()).service(routes())).await;
    let create = json!({"target":"user","description":"Inspect the checkout", "definition":definition("authored-flow",1)});
    let response = webtest::call_service(
        &app,
        webtest::TestRequest::post()
            .uri("/definitions")
            .set_json(&create)
            .to_request(),
    )
    .await;
    let status = response.status();
    let bytes = webtest::read_body(response).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let first: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(first["saved"], true);
    assert_eq!(first["catalog_available"], true);
    assert_eq!(first["invocation_policy"]["automatic"], false);
    assert_eq!(first["source"], "user");
    let store = state.skill_manager.store_for_workspace(None).await.unwrap();
    let pinned = store
        .pin_workflow_definition_bundle("authored-flow", 1)
        .await
        .unwrap();
    let pinned_bytes = serde_json::to_vec(&pinned).unwrap();
    let catalog = store.workflow_catalog_snapshot().await;
    let entry = catalog
        .entries
        .iter()
        .find(|entry| entry.id == "authored-flow" && entry.winner)
        .unwrap();
    assert_eq!(
        entry.invocation_policy,
        json!({"explicit":true,"automatic":false})
    );
    assert!(!serde_json::to_string(&catalog)
        .unwrap()
        .contains("Run this versioned orchestration"));
    let duplicate = webtest::call_service(
        &app,
        webtest::TestRequest::post()
            .uri("/definitions")
            .set_json(&create)
            .to_request(),
    )
    .await;
    assert_eq!(duplicate.status(), StatusCode::CONFLICT);
    let read: Value = webtest::call_and_read_body_json(
        &app,
        webtest::TestRequest::get()
            .uri("/definitions/authored-flow?target=user")
            .to_request(),
    )
    .await;
    assert_eq!(read["content_digest"], first["content_digest"]);
    assert_eq!(read["skill_digest"], first["skill_digest"]);
    assert_eq!(read["saved"], false);
    let mut next = definition("authored-flow", 2);
    next["steps"][0]["args"] = json!({"path":"new-snapshot.txt"});
    let update = json!({"target":"user","expected_revision":1,"expected_digest":first["content_digest"],"expected_skill_digest":first["skill_digest"],"definition":next});
    let response = webtest::call_service(
        &app,
        webtest::TestRequest::put()
            .uri("/definitions/authored-flow")
            .set_json(&update)
            .to_request(),
    )
    .await;
    let status = response.status();
    let bytes = webtest::read_body(response).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let second: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(second["definition"]["revision"], 2);
    assert_eq!(second["catalog_available"], true);
    assert_ne!(first["content_digest"], second["content_digest"]);
    assert_eq!(first["skill_digest"], second["skill_digest"]);
    assert_eq!(
        serde_json::to_vec(&pinned).unwrap(),
        pinned_bytes,
        "already pinned runs retain the exact prior bundle"
    );
    let current = store
        .pin_workflow_definition_bundle("authored-flow", 2)
        .await
        .unwrap();
    assert_eq!(current.root().unwrap().revision, 2);
    assert_eq!(pinned.root().unwrap().revision, 1);
    assert!(
        store
            .pin_workflow_definition_bundle("authored-flow", 1)
            .await
            .is_err(),
        "new runs cannot silently select a stale revision"
    );
    let stale = webtest::call_service(
        &app,
        webtest::TestRequest::put()
            .uri("/definitions/authored-flow")
            .set_json(&update)
            .to_request(),
    )
    .await;
    assert_eq!(stale.status(), StatusCode::CONFLICT);
    let unchanged: Value = webtest::call_and_read_body_json(
        &app,
        webtest::TestRequest::get()
            .uri("/definitions/authored-flow?target=user")
            .to_request(),
    )
    .await;
    assert_eq!(unchanged["content_digest"], second["content_digest"]);

    let bundle_path = data.path().join("skills/authored-flow");
    std::fs::create_dir(bundle_path.join("agents")).unwrap();
    std::fs::write(
        bundle_path.join("agents/bamboo.yaml"),
        "invocation_policy: {explicit: false, automatic: false}\n",
    )
    .unwrap();
    let denied_policy: Value = webtest::call_and_read_body_json(
        &app,
        webtest::TestRequest::get()
            .uri("/definitions/authored-flow?target=user")
            .to_request(),
    )
    .await;
    assert_eq!(
        denied_policy["invocation_policy"]["explicit"], false,
        "read reflects actual host policy rather than inventing authority"
    );

    let path = bundle_path.join("workflow.yaml");
    let mut external: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    external["invocation_policy"] = json!({"explicit":true,"automatic":true});
    let external_bytes = serde_json::to_vec_pretty(&external).unwrap();
    std::fs::write(&path, &external_bytes).unwrap();
    let read = webtest::call_service(
        &app,
        webtest::TestRequest::get()
            .uri("/definitions/authored-flow?target=user")
            .to_request(),
    )
    .await;
    assert_eq!(
        read.status(),
        StatusCode::CONFLICT,
        "GET cannot silently drop externally added policy metadata"
    );
    use sha2::Digest;
    let external_digest = hex::encode(sha2::Sha256::digest(&external_bytes));
    let response = webtest::call_service(&app, webtest::TestRequest::put().uri("/definitions/authored-flow").set_json(json!({"target":"user","expected_revision":2,"expected_digest":external_digest,"expected_skill_digest":first["skill_digest"],"definition":definition("authored-flow",3)})).to_request()).await;
    assert_eq!(
        response.status(),
        StatusCode::CONFLICT,
        "even exact caller tokens do not authorize stripping unsupported metadata"
    );
    assert_eq!(std::fs::read(path).unwrap(), external_bytes);
}

#[actix_web::test]
async fn authoring_rejects_invalid_unknown_and_existing_bundles_without_writes() {
    let data = tempfile::tempdir().unwrap();
    let state = web::Data::new(AppState::new(data.path().to_path_buf()).await.unwrap());
    let app = webtest::init_service(App::new().app_data(state).service(routes())).await;
    for bad in [
        json!({"target":"user","description":"Bad","definition":definition("invalid-flow",0)}),
        json!({"target":"user","description":"Bad","definition":definition("invalid-flow",2)}),
        json!({"target":"user","description":"Bad","definition":definition("../escaped",1)}),
        json!({"target":"user","description":"Bad","definition":definition("invalid-flow",1),"path":"/outside"}),
        json!({"target":"plugin","description":"Bad","definition":definition("invalid-flow",1)}),
        json!({"target":"project","description":"Bad","definition":definition("invalid-flow",1)}),
        json!({"target":"user","session_id":"arbitrary","description":"Bad","definition":definition("invalid-flow",1)}),
    ] {
        let response = webtest::call_service(
            &app,
            webtest::TestRequest::post()
                .uri("/definitions")
                .set_json(bad)
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    assert!(!data.path().join("skills/invalid-flow").exists());
    let builtin = webtest::call_service(&app, webtest::TestRequest::post().uri("/definitions").set_json(json!({"target":"user","description":"Do not shadow builtins","definition":definition("review",1)})).to_request()).await;
    assert_eq!(builtin.status(), StatusCode::CONFLICT);
    assert!(!data.path().join("skills/review").exists());
    let response = webtest::call_service(
        &app,
        webtest::TestRequest::post()
            .uri("/definitions/validate")
            .set_json(json!({"definition":definition("validate-only",1)}))
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = webtest::read_body_json(response).await;
    assert_eq!(body["execution_authorized"], false);
    assert!(!data.path().join("skills/validate-only").exists());
}

#[actix_web::test]
async fn authoring_project_scope_uses_durable_session_and_remains_isolated() {
    let data = tempfile::tempdir().unwrap();
    let state = web::Data::new(AppState::new(data.path().to_path_buf()).await.unwrap());
    let project = state
        .project_store
        .create("Authoring Project", None)
        .unwrap();
    let other = state.project_store.create("Other Project", None).unwrap();
    let mut session = bamboo_agent_core::Session::new("definition-project-session", "test-model");
    session.set_project_id_meta(project.id.to_string());
    state.storage.save_session(&session).await.unwrap();
    // Deliberately poison the cache: writes must use the persisted assignment.
    session.set_project_id_meta(other.id.to_string());
    state.sessions.insert(
        session.id.clone(),
        std::sync::Arc::new(bamboo_engine::SessionSnapshot::new(session)),
    );
    let app = webtest::init_service(App::new().app_data(state.clone()).service(routes())).await;
    let fixture_file = data.path().join("readonly-fixture.txt");
    std::fs::write(&fixture_file, "PROJECT_AUTHORING_READ_ONLY_MARKER").unwrap();
    let mut project_definition = definition("project-flow", 1);
    project_definition["steps"][0]["args"] = json!({"file_path":fixture_file});
    let response = webtest::call_service(&app,webtest::TestRequest::post().uri("/definitions").set_json(json!({"target":"project","session_id":"definition-project-session","description":"Project-owned definition","definition":project_definition})).to_request()).await;
    let status = response.status();
    let bytes = webtest::read_body(response).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["source"], "project");
    assert_eq!(body["catalog_available"], true);
    assert!(state
        .project_store
        .paths()
        .project_home(&project.id)
        .join("skills/project-flow/workflow.yaml")
        .is_file());
    assert!(!state
        .project_store
        .paths()
        .project_home(&other.id)
        .join("skills/project-flow")
        .exists());
    assert!(!data.path().join("skills/project-flow").exists());
    let user_read = webtest::call_service(
        &app,
        webtest::TestRequest::get()
            .uri("/definitions/project-flow?target=user")
            .to_request(),
    )
    .await;
    assert_eq!(user_read.status(), StatusCode::NOT_FOUND);

    let catalog_response = super::super::list_workflow_catalog(
        state.clone(),
        web::Query(super::super::types::WorkflowCatalogQuery {
            session_id: Some("definition-project-session".to_string()),
        }),
    )
    .await
    .unwrap();
    let catalog_bytes = actix_web::body::to_bytes(catalog_response.into_body())
        .await
        .unwrap();
    let catalog: Value = serde_json::from_slice(&catalog_bytes).unwrap();
    assert!(catalog["entries"]
        .as_array()
        .unwrap()
        .iter()
        .any(|entry| entry["id"] == "project-flow" && entry["source"] == "project"));
    let started = state
        .workflow_runs
        .start(
            "definition-project-session",
            "project-flow",
            1,
            json!({}),
            None,
        )
        .await
        .expect("Project-home definition can execute without a checkout");
    let terminal = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let progress = state
                .workflow_runs
                .progress_for_session("definition-project-session", &started.run_id, 0)
                .await
                .unwrap();
            if progress.snapshot.status.is_terminal() {
                break progress.snapshot;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("real readonly workflow terminates");
    assert_eq!(
        terminal.status,
        bamboo_domain::WorkflowRunStatus::Succeeded,
        "{:?}",
        terminal.failure
    );
    assert!(terminal
        .steps
        .values()
        .any(|step| step.output.as_ref().is_some_and(|output| output
            .to_string()
            .contains("PROJECT_AUTHORING_READ_ONLY_MARKER"))));
    let mut other_session =
        bamboo_agent_core::Session::new("definition-other-project-session", "test-model");
    other_session.set_project_id_meta(other.id.to_string());
    state.storage.save_session(&other_session).await.unwrap();
    assert!(state
        .workflow_runs
        .start(&other_session.id, "project-flow", 1, json!({}), None)
        .await
        .is_err());
    assert!(state
        .workflow_runs
        .start_from_tool(
            "definition-project-session",
            "project-flow",
            1,
            json!({}),
            None
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("opt-in"));
}

#[actix_web::test]
async fn project_workflow_executes_relative_tools_in_durable_workspace_and_honors_revocation() {
    let data = tempfile::tempdir().unwrap();
    let workspace_a = data.path().join("source-a");
    let workspace_b = data.path().join("source-b");
    std::fs::create_dir(&workspace_a).unwrap();
    std::fs::create_dir(&workspace_b).unwrap();
    std::fs::write(workspace_a.join("only-a.txt"), "A").unwrap();
    std::fs::write(workspace_b.join("only-b.txt"), "B").unwrap();
    let state = web::Data::new(AppState::new(data.path().to_path_buf()).await.unwrap());
    let project_a = state
        .project_store
        .create_with_project_path("Workspace A", None, workspace_a.to_string_lossy(), vec![])
        .unwrap();
    let project_b = state
        .project_store
        .create_with_project_path("Workspace B", None, workspace_b.to_string_lossy(), vec![])
        .unwrap();
    let mut durable = bamboo_agent_core::Session::new("relative-project-authoring", "test-model");
    durable.set_project_id_meta(project_a.id.to_string());
    durable.set_workspace_path_meta(workspace_a.to_string_lossy());
    durable.workspace = Some(workspace_a.to_string_lossy().to_string());
    state.storage.save_session(&durable).await.unwrap();
    let mut stale = durable.clone();
    stale.set_project_id_meta(project_b.id.to_string());
    stale.set_workspace_path_meta(workspace_b.to_string_lossy());
    stale.workspace = Some(workspace_b.to_string_lossy().to_string());
    stale.metadata.insert(
        bamboo_skills::WORKFLOW_ORCHESTRATION_OPT_IN_METADATA_KEY.to_string(),
        "true".to_string(),
    );
    state.sessions.insert(
        stale.id.clone(),
        std::sync::Arc::new(bamboo_engine::SessionSnapshot::new(stale)),
    );
    state.workspace_resolver.publish_resolved_workspace(
        &durable.id,
        workspace_b.clone(),
        "test_stale_cache",
    );
    let mut flow = definition("relative-project-flow", 1);
    flow["steps"][0]["tool"] = json!("Glob");
    flow["steps"][0]["args"] = json!({"pattern":"*.txt"});
    let app = webtest::init_service(App::new().app_data(state.clone()).service(routes())).await;
    let create = webtest::call_service(&app, webtest::TestRequest::post().uri("/definitions").set_json(json!({"target":"project","session_id":durable.id,"description":"Relative Project inspection","definition":flow})).to_request()).await;
    let status = create.status();
    let bytes = webtest::read_body(create).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let denied = state
        .workflow_runs
        .start_from_tool(&durable.id, "relative-project-flow", 1, json!({}), None)
        .await
        .unwrap_err();
    assert!(
        denied.to_string().contains("opt-in"),
        "stale cached opt-in cannot override durable revocation: {denied}"
    );
    let started = state
        .workflow_runs
        .start(&durable.id, "relative-project-flow", 1, json!({}), None)
        .await
        .unwrap();
    let terminal = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let progress = state
                .workflow_runs
                .progress_for_session(&durable.id, &started.run_id, 0)
                .await
                .unwrap();
            if progress.snapshot.status.is_terminal() {
                break progress.snapshot;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("relative readonly workflow terminates");
    assert_eq!(
        terminal.status,
        bamboo_domain::WorkflowRunStatus::Succeeded,
        "{:?}",
        terminal.failure
    );
    let outputs = serde_json::to_string(&terminal.steps).unwrap();
    assert!(outputs.contains("only-a.txt"), "{outputs}");
    assert!(!outputs.contains("only-b.txt"), "{outputs}");
    let canonical_a = workspace_a.canonicalize().unwrap();
    assert_eq!(
        bamboo_agent_core::workspace_state::peek_workspace(&durable.id),
        Some(canonical_a.clone())
    );
    // Reintroduce a stale cache after index persistence. A read-only listing
    // must not change the validated execution workspace.
    let mut stale = durable.clone();
    stale.workspace = Some(workspace_b.to_string_lossy().to_string());
    stale.metadata.insert(
        bamboo_skills::WORKFLOW_ORCHESTRATION_OPT_IN_METADATA_KEY.to_string(),
        "true".to_string(),
    );
    state.sessions.insert(
        stale.id.clone(),
        std::sync::Arc::new(bamboo_engine::SessionSnapshot::new(stale)),
    );
    state
        .workflow_runs
        .list_for_session(&durable.id)
        .await
        .unwrap();
    assert_eq!(
        bamboo_agent_core::workspace_state::peek_workspace(&durable.id),
        Some(canonical_a.clone()),
        "listing cannot replace the execution workspace from stale cache"
    );

    // Recovery keeps the exact pinned definition while refreshing only the
    // current durable Session authority and execution scope.
    use bamboo_engine::WorkflowRunRepository;
    let mut recovery = terminal;
    recovery.run_id = "project-authoring-recovery".to_string();
    recovery.status = bamboo_domain::WorkflowRunStatus::Suspended;
    recovery.suspension = Some(bamboo_domain::WorkflowSuspensionContext::Recovery {
        reason: "test process restart".to_string(),
    });
    recovery.definition_bundle.root_invocation_policy["automatic"] = json!(true);
    recovery.output = None;
    recovery.last_sequence = 1;
    let repository =
        bamboo_engine::FileWorkflowRunRepository::new(data.path().join("workflow-runs")).unwrap();
    repository
        .create(
            &recovery,
            &bamboo_domain::WorkflowRunEvent {
                run_id: recovery.run_id.clone(),
                sequence: 1,
                at: recovery.updated_at,
                step_id: None,
                kind: bamboo_domain::WorkflowRunEventKind::RunSuspended {
                    reason: "test process restart".to_string(),
                },
            },
        )
        .await
        .unwrap();
    state.workspace_resolver.publish_resolved_workspace(
        &durable.id,
        workspace_b,
        "test_stale_recovery",
    );
    let denied = state
        .workflow_runs
        .restart_from_tool(&durable.id, &recovery.run_id)
        .await
        .unwrap_err();
    assert!(denied.to_string().contains("opt-in"), "{denied}");
    let restarted = state
        .workflow_runs
        .restart_for_session(&durable.id, &recovery.run_id)
        .await
        .unwrap();
    let terminal = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let progress = state
                .workflow_runs
                .progress_for_session(&durable.id, &restarted.run_id, 0)
                .await
                .unwrap();
            if progress.snapshot.status.is_terminal() {
                break progress.snapshot;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        terminal.status,
        bamboo_domain::WorkflowRunStatus::Succeeded,
        "{:?}",
        terminal.failure
    );
    let outputs = serde_json::to_string(&terminal.steps).unwrap();
    assert!(outputs.contains("only-a.txt"), "{outputs}");
    assert!(!outputs.contains("only-b.txt"), "{outputs}");
    assert_eq!(
        bamboo_agent_core::workspace_state::peek_workspace(&durable.id),
        Some(canonical_a)
    );
}

#[actix_web::test]
async fn unassigned_legacy_session_keeps_its_durable_workspace() {
    let data = tempfile::tempdir().unwrap();
    let workspace = data.path().join("legacy-workspace");
    std::fs::create_dir(&workspace).unwrap();
    std::fs::write(workspace.join("legacy-marker.txt"), "legacy").unwrap();
    let state = web::Data::new(AppState::new(data.path().to_path_buf()).await.unwrap());
    let mut session = bamboo_agent_core::Session::new("legacy-workflow-owner", "test-model");
    session.workspace = Some(workspace.to_string_lossy().into_owned());
    assert!(session.workspace_path_meta().is_none());
    state.storage.save_session(&session).await.unwrap();
    let mut flow = definition("legacy-workspace-flow", 1);
    flow["steps"][0]["tool"] = json!("Glob");
    flow["steps"][0]["args"] = json!({"pattern":"*.txt"});
    let app = webtest::init_service(App::new().app_data(state.clone()).service(routes())).await;
    let response = webtest::call_service(&app, webtest::TestRequest::post().uri("/definitions")
        .set_json(json!({"target":"user","description":"Legacy workspace inspection","definition":flow})).to_request()).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let started = state
        .workflow_runs
        .start(&session.id, "legacy-workspace-flow", 1, json!({}), None)
        .await
        .unwrap();
    let terminal = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let progress = state
                .workflow_runs
                .progress_for_session(&session.id, &started.run_id, 0)
                .await
                .unwrap();
            if progress.snapshot.status.is_terminal() {
                break progress.snapshot;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        terminal.status,
        bamboo_domain::WorkflowRunStatus::Succeeded,
        "{:?}",
        terminal.failure
    );
    assert!(serde_json::to_string(&terminal.steps)
        .unwrap()
        .contains("legacy-marker.txt"));
}

#[actix_web::test]
async fn nested_definitions_require_available_pins_and_cannot_cycle_through_replaced_root() {
    let data = tempfile::tempdir().unwrap();
    let state = web::Data::new(AppState::new(data.path().to_path_buf()).await.unwrap());
    let app = webtest::init_service(App::new().app_data(state).service(routes())).await;
    let mut nested = definition("nested-flow", 1);
    nested["steps"][0] =
        json!({"id":"inspect","type":"workflow","workflow_id":"leaf-flow","revision":1,"args":{}});
    let create_nested = json!({"target":"user","description":"Nested pin","definition":nested});
    let missing = webtest::call_service(
        &app,
        webtest::TestRequest::post()
            .uri("/definitions")
            .set_json(&create_nested)
            .to_request(),
    )
    .await;
    assert_eq!(missing.status(), StatusCode::BAD_REQUEST);
    assert!(!data.path().join("skills/nested-flow").exists());
    let first: Value = webtest::call_and_read_body_json(&app, webtest::TestRequest::post().uri("/definitions").set_json(json!({"target":"user","description":"Leaf pin","definition":definition("leaf-flow",1)})).to_request()).await;
    assert_eq!(first["catalog_available"], true);
    let nested: Value = webtest::call_and_read_body_json(
        &app,
        webtest::TestRequest::post()
            .uri("/definitions")
            .set_json(&create_nested)
            .to_request(),
    )
    .await;
    assert_eq!(nested["catalog_available"], true);
    let mut cyclic = definition("leaf-flow", 2);
    cyclic["steps"][0] = json!({"id":"inspect","type":"workflow","workflow_id":"nested-flow","revision":1,"args":{}});
    let update = |definition: Value| json!({"target":"user","expected_revision":1,"expected_digest":first["content_digest"],"expected_skill_digest":first["skill_digest"],"definition":definition});
    let rejected = webtest::call_service(
        &app,
        webtest::TestRequest::put()
            .uri("/definitions/leaf-flow")
            .set_json(update(cyclic))
            .to_request(),
    )
    .await;
    assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
    let updated = webtest::call_service(
        &app,
        webtest::TestRequest::put()
            .uri("/definitions/leaf-flow")
            .set_json(update(definition("leaf-flow", 2)))
            .to_request(),
    )
    .await;
    assert_eq!(updated.status(), StatusCode::OK);
    let unavailable: Value = webtest::call_and_read_body_json(
        &app,
        webtest::TestRequest::get()
            .uri("/definitions/nested-flow?target=user")
            .to_request(),
    )
    .await;
    assert_eq!(unavailable["definition"]["revision"], 1);
    assert_eq!(
        unavailable["catalog_available"], false,
        "missing nested pin is not reported as available"
    );
    assert_eq!(unavailable["content_digest"], nested["content_digest"]);
}
