use super::validation::is_safe_workflow_name;

#[actix_web::test]
async fn workflow_catalog_excludes_instruction_skills_and_returns_orchestration_metadata() {
    let data = tempfile::tempdir().expect("data dir");
    let skill = data.path().join("skills/review");
    tokio::fs::create_dir_all(&skill).await.expect("skill dir");
    tokio::fs::write(
        skill.join("SKILL.md"),
        "---\nname: review\ndescription: Reviews changes\n---\nTOP SECRET INSTRUCTIONS\n",
    )
    .await
    .expect("skill");
    let workflow = data.path().join("skills/deploy");
    tokio::fs::create_dir_all(&workflow)
        .await
        .expect("workflow dir");
    tokio::fs::write(
        workflow.join("SKILL.md"),
        "---\nname: deploy\ndescription: Deploys changes\n---\nWORKFLOW SECRET BODY\n",
    )
    .await
    .expect("workflow skill");
    tokio::fs::write(
        workflow.join("workflow.yaml"),
        "id: deploy\nname: Deploy\ndescription: Deploys changes\nversion: '1'\ncomposition:\n  type: call\n  tool: read_file\n  args: {}\n",
    )
    .await
    .expect("workflow metadata");
    let state = actix_web::web::Data::new(
        crate::app_state::AppState::new(data.path().to_path_buf())
            .await
            .expect("app state"),
    );
    let app = actix_web::test::init_service(actix_web::App::new().app_data(state).route(
        "/catalog",
        actix_web::web::get().to(super::list_workflow_catalog),
    ))
    .await;
    let request = actix_web::test::TestRequest::get()
        .uri("/catalog")
        .to_request();
    let body = actix_web::test::call_and_read_body(&app, request).await;
    let text = std::str::from_utf8(&body).expect("utf8 response");
    assert!(!text.contains("Reviews changes"));
    assert!(text.contains("Deploys changes"));
    assert!(text.contains("\"revision\""));
    assert!(!text.contains("TOP SECRET INSTRUCTIONS"));
    assert!(!text.contains("WORKFLOW SECRET BODY"));
    assert!(!text.contains("SKILL.md"));
}

#[actix_web::test]
async fn workflow_catalog_session_without_workspace_uses_global_snapshot() {
    let data = tempfile::tempdir().expect("data dir");
    let skill = data.path().join("skills/global-review");
    tokio::fs::create_dir_all(&skill).await.expect("skill dir");
    tokio::fs::write(
        skill.join("SKILL.md"),
        "---\nname: global-review\ndescription: Global review workflow\n---\nGlobal body\n",
    )
    .await
    .expect("skill");
    tokio::fs::write(
        skill.join("workflow.yaml"),
        "id: global-review\nname: Global review\ndescription: Global review workflow\nversion: '1'\ncomposition:\n  type: call\n  tool: read_file\n  args: {}\n",
    )
    .await
    .expect("workflow metadata");
    let state = actix_web::web::Data::new(
        crate::app_state::AppState::new(data.path().to_path_buf())
            .await
            .expect("app state"),
    );
    let session = bamboo_agent_core::Session::new("global-session", "test-model");
    state.sessions.insert(
        session.id.clone(),
        std::sync::Arc::new(parking_lot::RwLock::new(session)),
    );
    let app = actix_web::test::init_service(actix_web::App::new().app_data(state).route(
        "/catalog",
        actix_web::web::get().to(super::list_workflow_catalog),
    ))
    .await;
    let request = actix_web::test::TestRequest::get()
        .uri("/catalog?session_id=global-session")
        .to_request();
    let response = actix_web::test::call_service(&app, request).await;
    assert!(response.status().is_success());
    let body: serde_json::Value = actix_web::test::read_body_json(response).await;
    assert!(body["entries"]
        .as_array()
        .expect("catalog entries")
        .iter()
        .any(|entry| entry["id"] == "global-review"));
}

#[actix_web::test]
async fn assigned_project_workflow_catalog_reports_workspace_then_project_sources() {
    let data = tempfile::tempdir().expect("data dir");
    let workspace = tempfile::tempdir().expect("workspace");
    let project_store = bamboo_projects::ProjectStore::open(data.path()).expect("Project store");
    let project = project_store
        .create("Workflow Project", None)
        .expect("create Project");
    let project_skills = project_store
        .paths()
        .project_home(&project.id)
        .join("skills");
    for (id, description) in [
        ("shared-workflow", "Project shared workflow"),
        ("project-only", "Project only workflow"),
    ] {
        let skill = project_skills.join(id);
        std::fs::create_dir_all(&skill).expect("Project skill");
        std::fs::write(
            skill.join("SKILL.md"),
            format!("---\nname: {id}\ndescription: {description}\n---\nPROJECT BODY\n"),
        )
        .expect("write Project skill");
        std::fs::write(
            skill.join("workflow.yaml"),
            format!(
                "id: {id}\nname: {id}\ndescription: {description}\nversion: '1'\ncomposition:\n  type: call\n  tool: read_file\n  args: {{}}\n"
            ),
        )
        .expect("write Project workflow metadata");
    }
    let workspace_skill = workspace.path().join(".bamboo/skills/shared-workflow");
    std::fs::create_dir_all(&workspace_skill).expect("workspace skill");
    std::fs::write(
        workspace_skill.join("SKILL.md"),
        "---\nname: shared-workflow\ndescription: Workspace overlay workflow\n---\nWORKSPACE BODY\n",
    )
    .expect("write workspace skill");
    std::fs::write(
        workspace_skill.join("workflow.yaml"),
        "id: shared-workflow\nname: shared-workflow\ndescription: Workspace overlay workflow\nversion: '1'\ncomposition:\n  type: call\n  tool: read_file\n  args: {}\n",
    )
    .expect("write workspace workflow metadata");

    let state = actix_web::web::Data::new(
        crate::app_state::AppState::new(data.path().to_path_buf())
            .await
            .expect("app state"),
    );
    let mut session = bamboo_agent_core::Session::new("project-workflow-session", "test-model");
    session.set_project_id_meta(project.id.to_string());
    session.set_workspace_path_meta(workspace.path().to_string_lossy().into_owned());
    state.sessions.insert(
        session.id.clone(),
        std::sync::Arc::new(parking_lot::RwLock::new(session)),
    );
    let app = actix_web::test::init_service(actix_web::App::new().app_data(state).route(
        "/catalog",
        actix_web::web::get().to(super::list_workflow_catalog),
    ))
    .await;
    let body: serde_json::Value = actix_web::test::call_and_read_body_json(
        &app,
        actix_web::test::TestRequest::get()
            .uri("/catalog?session_id=project-workflow-session")
            .to_request(),
    )
    .await;
    let entries = body["entries"].as_array().expect("catalog entries");
    let shared = entries
        .iter()
        .find(|entry| entry["id"] == "shared-workflow")
        .expect("shared workflow");
    assert_eq!(shared["description"], "Workspace overlay workflow");
    assert_eq!(shared["source"], "workspace");
    let project_only = entries
        .iter()
        .find(|entry| entry["id"] == "project-only")
        .expect("Project workflow");
    assert_eq!(project_only["source"], "project");
}

#[actix_web::test]
async fn assigned_project_cannot_read_another_projects_workspace_workflows() {
    let data = tempfile::tempdir().expect("data dir");
    let workspace = tempfile::tempdir().expect("workspace");
    let workspace_skill = workspace.path().join(".bamboo/skills/other-project-secret");
    std::fs::create_dir_all(&workspace_skill).expect("workspace skill");
    let secret =
        "---\nname: other-project-secret\ndescription: Other Project Secret\n---\nSECRET BODY\n";
    std::fs::write(workspace_skill.join("SKILL.md"), secret).expect("workspace skill");

    let state = actix_web::web::Data::new(
        crate::app_state::AppState::new(data.path().to_path_buf())
            .await
            .expect("app state"),
    );
    let session_project = state
        .project_store
        .create("Session Project", None)
        .expect("session Project");
    let _workspace_owner = state
        .project_store
        .create_with_bindings(
            "Workspace Owner",
            None,
            vec![bamboo_domain::WorkspaceBinding {
                path: workspace.path().to_string_lossy().into_owned(),
                label: None,
                git_common_dir: None,
            }],
        )
        .expect("workspace owner");
    let mut session =
        bamboo_agent_core::Session::new("cross-project-workflow-session", "test-model");
    session.set_project_id_meta(session_project.id.to_string());
    session.set_workspace_path_meta(workspace.path().to_string_lossy().into_owned());
    state.sessions.insert(
        session.id.clone(),
        std::sync::Arc::new(parking_lot::RwLock::new(session)),
    );

    let app = actix_web::test::init_service(
        actix_web::App::new()
            .app_data(state)
            .route(
                "/catalog",
                actix_web::web::get().to(super::list_workflow_catalog),
            )
            .route(
                "/catalog/{workflow_id}/migrate",
                actix_web::web::post().to(super::migrate_workflow),
            ),
    )
    .await;
    let response = actix_web::test::call_service(
        &app,
        actix_web::test::TestRequest::get()
            .uri("/catalog?session_id=cross-project-workflow-session")
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
    let body = actix_web::test::read_body(response).await;
    assert!(
        !body
            .windows(b"Other Project Secret".len())
            .any(|window| window == b"Other Project Secret"),
        "cross-Project workflow metadata must not be returned"
    );
    let migration = actix_web::test::call_service(
        &app,
        actix_web::test::TestRequest::post()
            .uri("/catalog/other-project-secret/migrate")
            .set_json(serde_json::json!({
                "session_id": "cross-project-workflow-session"
            }))
            .to_request(),
    )
    .await;
    assert_eq!(migration.status(), actix_web::http::StatusCode::BAD_REQUEST);
    assert_eq!(
        std::fs::read_to_string(workspace_skill.join("SKILL.md")).expect("secret unchanged"),
        secret
    );
}

#[actix_web::test]
async fn workspace_legacy_workflow_migration_is_explicit_non_destructive_and_idempotent() {
    let data = tempfile::tempdir().expect("data dir");
    let workspace = tempfile::tempdir().expect("workspace");
    let legacy_dir = workspace.path().join(".bamboo/workflows");
    std::fs::create_dir_all(&legacy_dir).expect("legacy workflow dir");
    let source = legacy_dir.join("daily-report.md");
    let original = "# Daily report\n\nSummarize today's changes.\n";
    std::fs::write(&source, original).expect("legacy workflow");
    let protected_source = legacy_dir.join("protected.md");
    std::fs::write(&protected_source, "Legacy source must remain.\n")
        .expect("protected legacy workflow");
    let protected_target = workspace.path().join(".bamboo/skills/protected/SKILL.md");
    std::fs::create_dir_all(protected_target.parent().expect("protected target parent"))
        .expect("protected target dir");
    let protected_skill =
        "---\nname: protected\ndescription: Existing canonical Skill\n---\nKeep this target.\n";
    std::fs::write(&protected_target, protected_skill).expect("protected target");

    let state = actix_web::web::Data::new(
        crate::app_state::AppState::new(data.path().to_path_buf())
            .await
            .expect("app state"),
    );
    let mut session = bamboo_agent_core::Session::new("legacy-migration-session", "test-model");
    session.set_workspace_path_meta(workspace.path().to_string_lossy().into_owned());
    state.sessions.insert(
        session.id.clone(),
        std::sync::Arc::new(parking_lot::RwLock::new(session)),
    );
    let app = actix_web::test::init_service(
        actix_web::App::new()
            .app_data(state.clone())
            .route(
                "/catalog",
                actix_web::web::get().to(super::list_workflow_catalog),
            )
            .route(
                "/catalog/{workflow_id}/migrate",
                actix_web::web::post().to(super::migrate_workflow),
            ),
    )
    .await;

    let conflict = actix_web::test::call_service(
        &app,
        actix_web::test::TestRequest::post()
            .uri("/catalog/protected/migrate")
            .set_json(serde_json::json!({"session_id": "legacy-migration-session"}))
            .to_request(),
    )
    .await;
    assert_eq!(conflict.status(), actix_web::http::StatusCode::CONFLICT);
    assert_eq!(
        std::fs::read_to_string(&protected_target).expect("protected target unchanged"),
        protected_skill
    );
    assert_eq!(
        std::fs::read_to_string(&protected_source).expect("protected source unchanged"),
        "Legacy source must remain.\n"
    );

    let before: serde_json::Value = actix_web::test::call_and_read_body_json(
        &app,
        actix_web::test::TestRequest::get()
            .uri("/catalog?session_id=legacy-migration-session")
            .to_request(),
    )
    .await;
    let legacy = before["entries"]
        .as_array()
        .expect("catalog entries")
        .iter()
        .find(|entry| entry["id"] == "daily-report")
        .expect("legacy workflow entry");
    assert_eq!(legacy["legacy"], true);
    assert_eq!(legacy["migration_status"], "available");
    assert_eq!(legacy["invocation_policy"]["explicit"], true);
    assert_eq!(legacy["invocation_policy"]["automatic"], false);

    let migrate = || {
        actix_web::test::TestRequest::post()
            .uri("/catalog/daily-report/migrate")
            .set_json(serde_json::json!({"session_id": "legacy-migration-session"}))
            .to_request()
    };
    let first = actix_web::test::call_service(&app, migrate()).await;
    assert!(first.status().is_success());
    let first: serde_json::Value = actix_web::test::read_body_json(first).await;
    assert_eq!(first["outcome"], "migrated");
    assert_eq!(first["source_preserved"], true);
    assert_eq!(std::fs::read_to_string(&source).unwrap(), original);

    let target = workspace.path().join(".bamboo/skills/daily-report");
    let migrated = std::fs::read_to_string(target.join("SKILL.md")).expect("migrated Skill");
    assert!(migrated.contains("legacy_migration: true"));
    assert!(migrated.contains(".bamboo/workflows/daily-report.md"));
    assert!(!migrated.contains(workspace.path().to_string_lossy().as_ref()));
    assert!(migrated.contains("Summarize today's changes."));
    assert!(target.join("agents/bamboo.yaml").exists());

    let second = actix_web::test::call_service(&app, migrate()).await;
    assert!(second.status().is_success());
    let second: serde_json::Value = actix_web::test::read_body_json(second).await;
    assert_eq!(second["outcome"], "already_migrated");
    assert_eq!(std::fs::read_to_string(&source).unwrap(), original);

    let store = state
        .skill_manager
        .store_for_workspace(Some(workspace.path()))
        .await
        .expect("workspace SkillStore");
    let skill_catalog = store.skill_catalog_snapshot().await;
    let migrated_skill = skill_catalog
        .entries
        .iter()
        .find(|entry| entry.id == "daily-report")
        .expect("migrated Skill entry");
    assert_eq!(
        migrated_skill.migration_status,
        Some(bamboo_skills::LegacyWorkflowMigrationStatus::Migrated)
    );

    let after: serde_json::Value = actix_web::test::call_and_read_body_json(
        &app,
        actix_web::test::TestRequest::get()
            .uri("/catalog?session_id=legacy-migration-session")
            .to_request(),
    )
    .await;
    let source_workflow = after["entries"]
        .as_array()
        .expect("catalog entries")
        .iter()
        .find(|entry| entry["id"] == "daily-report")
        .expect("source Workflow entry");
    assert_eq!(source_workflow["migration_status"], "available");
    assert!(source_workflow.get("shadowed_candidates").is_none());
}

#[actix_web::test]
async fn global_legacy_workflow_advertised_as_available_migrates_into_session_workspace() {
    let data = tempfile::tempdir().expect("data dir");
    let workspace = tempfile::tempdir().expect("workspace");
    let global_workflows = data.path().join("workflows");
    std::fs::create_dir_all(&global_workflows).expect("global workflows");
    let source = global_workflows.join("global-review.md");
    let original =
        "---\ndescription: Review changes from the global Workflow.\n---\nReview the diff.\n";
    std::fs::write(&source, original).expect("global legacy workflow");

    let state = actix_web::web::Data::new(
        crate::app_state::AppState::new(data.path().to_path_buf())
            .await
            .expect("app state"),
    );
    let mut session = bamboo_agent_core::Session::new("global-migration-session", "test-model");
    session.set_workspace_path_meta(workspace.path().to_string_lossy().into_owned());
    state.sessions.insert(
        session.id.clone(),
        std::sync::Arc::new(parking_lot::RwLock::new(session)),
    );
    let app = actix_web::test::init_service(
        actix_web::App::new()
            .app_data(state.clone())
            .route(
                "/catalog",
                actix_web::web::get().to(super::list_workflow_catalog),
            )
            .route(
                "/catalog/{workflow_id}/migrate",
                actix_web::web::post().to(super::migrate_workflow),
            ),
    )
    .await;

    let before: serde_json::Value = actix_web::test::call_and_read_body_json(
        &app,
        actix_web::test::TestRequest::get()
            .uri("/catalog?session_id=global-migration-session")
            .to_request(),
    )
    .await;
    let advertised = before["entries"]
        .as_array()
        .expect("catalog entries")
        .iter()
        .find(|entry| entry["id"] == "global-review")
        .expect("global Workflow");
    assert_eq!(advertised["source"], "user");
    assert_eq!(advertised["migration_status"], "available");

    let response = actix_web::test::call_service(
        &app,
        actix_web::test::TestRequest::post()
            .uri("/catalog/global-review/migrate")
            .set_json(serde_json::json!({
                "session_id": "global-migration-session"
            }))
            .to_request(),
    )
    .await;
    assert!(response.status().is_success());
    let body: serde_json::Value = actix_web::test::read_body_json(response).await;
    assert_eq!(body["outcome"], "migrated");
    assert_eq!(body["source_preserved"], true);
    assert_eq!(std::fs::read_to_string(&source).unwrap(), original);

    let target = workspace
        .path()
        .join(".bamboo/skills/global-review/SKILL.md");
    let migrated = std::fs::read_to_string(&target).expect("migrated Skill");
    assert!(migrated.contains("workflows/global-review.md"));
    assert!(!migrated.contains(data.path().to_string_lossy().as_ref()));
    let scoped = state
        .skill_manager
        .store_for_workspace(Some(workspace.path()))
        .await
        .expect("workspace store");
    assert_eq!(
        scoped
            .get_skill("global-review")
            .await
            .expect("real migrated Skill")
            .prompt,
        "Review the diff."
    );
    assert_eq!(
        std::fs::canonicalize(
            scoped
                .get_legacy_workflow_source("global-review")
                .await
                .expect("preserved source Workflow")
        )
        .unwrap(),
        std::fs::canonicalize(&source).unwrap()
    );
}

#[actix_web::test]
async fn legacy_api_keeps_workflow_source_only_and_bridges_catalog_event() {
    let data = tempfile::tempdir().expect("data dir");
    let state = actix_web::web::Data::new(
        crate::app_state::AppState::new(data.path().to_path_buf())
            .await
            .expect("app state"),
    );
    let mut account_events = state.account_sink.subscribe();
    let app = actix_web::test::init_service(
        actix_web::App::new()
            .app_data(state.clone())
            .route(
                "/workflows",
                actix_web::web::post().to(super::save_workflow),
            )
            .route(
                "/workflows/{name}",
                actix_web::web::get().to(super::get_workflow),
            )
            .route(
                "/workflows/{name}",
                actix_web::web::delete().to(super::delete_workflow),
            ),
    )
    .await;
    for content in ["First body", "Second body"] {
        let request = actix_web::test::TestRequest::post()
            .uri("/workflows")
            .set_json(serde_json::json!({"name": "legacy", "content": content}))
            .to_request();
        let response = actix_web::test::call_service(&app, request).await;
        assert!(response.status().is_success());
    }
    let request = actix_web::test::TestRequest::get()
        .uri("/workflows/legacy")
        .to_request();
    let body: serde_json::Value = actix_web::test::call_and_read_body_json(&app, request).await;
    assert_eq!(body["content"], "Second body");
    assert!(
        !data.path().join("skills/legacy/SKILL.md").exists(),
        "legacy Workflow writes must not materialize a Skill"
    );

    let bridged = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let mut workflow_changes = 0;
        loop {
            let event = account_events.recv().await.expect("account event");
            if matches!(
                event.event,
                bamboo_agent_core::AgentEvent::WorkflowChanged { .. }
            ) {
                workflow_changes += 1;
                if workflow_changes == 2 {
                    break;
                }
            }
        }
    })
    .await;
    assert!(bridged.is_ok(), "workflow.changed must reach account feed");
}

#[actix_web::test]
async fn global_workflow_create_update_delete_is_immediate_in_cached_session_views() {
    let data = tempfile::tempdir().expect("data dir");
    let workspace = tempfile::tempdir().expect("workspace");
    let state = actix_web::web::Data::new(
        crate::app_state::AppState::new(data.path().to_path_buf())
            .await
            .expect("app state"),
    );
    let project = state
        .project_store
        .create("Workflow cache Project", None)
        .expect("Project");
    let mut workspace_session =
        bamboo_agent_core::Session::new("workspace-cache-session", "test-model");
    workspace_session.set_workspace_path_meta(workspace.path().to_string_lossy().into_owned());
    state.sessions.insert(
        workspace_session.id.clone(),
        std::sync::Arc::new(parking_lot::RwLock::new(workspace_session)),
    );
    let mut project_session =
        bamboo_agent_core::Session::new("project-cache-session", "test-model");
    project_session.set_project_id_meta(project.id.to_string());
    project_session.set_workspace_path_meta(workspace.path().to_string_lossy().into_owned());
    state.sessions.insert(
        project_session.id.clone(),
        std::sync::Arc::new(parking_lot::RwLock::new(project_session)),
    );
    let app = actix_web::test::init_service(
        actix_web::App::new()
            .app_data(state)
            .route(
                "/catalog",
                actix_web::web::get().to(super::list_workflow_catalog),
            )
            .route(
                "/workflows",
                actix_web::web::post().to(super::save_workflow),
            )
            .route(
                "/workflows/{name}",
                actix_web::web::delete().to(super::delete_workflow),
            ),
    )
    .await;

    let session_ids = ["workspace-cache-session", "project-cache-session"];
    let mut initial_revisions = Vec::new();
    for session_id in session_ids {
        let catalog: serde_json::Value = actix_web::test::call_and_read_body_json(
            &app,
            actix_web::test::TestRequest::get()
                .uri(&format!("/catalog?session_id={session_id}"))
                .to_request(),
        )
        .await;
        initial_revisions.push(catalog["revision"].as_u64().expect("catalog revision"));
        assert!(!catalog["entries"]
            .as_array()
            .expect("entries")
            .iter()
            .any(|entry| entry["id"] == "live-global"));
    }

    for (description, body) in [
        ("First global Workflow.", "First body"),
        ("Second global Workflow.", "Second body"),
    ] {
        let response = actix_web::test::call_service(
            &app,
            actix_web::test::TestRequest::post()
                .uri("/workflows")
                .set_json(serde_json::json!({
                    "name": "live-global",
                    "content": format!(
                        "---\ndescription: {description}\n---\n{body}\n"
                    )
                }))
                .to_request(),
        )
        .await;
        assert!(response.status().is_success());

        for (index, session_id) in session_ids.iter().enumerate() {
            let catalog: serde_json::Value = actix_web::test::call_and_read_body_json(
                &app,
                actix_web::test::TestRequest::get()
                    .uri(&format!("/catalog?session_id={session_id}"))
                    .to_request(),
            )
            .await;
            assert!(catalog["revision"].as_u64().unwrap() > initial_revisions[index]);
            let entry = catalog["entries"]
                .as_array()
                .expect("entries")
                .iter()
                .find(|entry| entry["id"] == "live-global")
                .expect("same-request publication");
            assert_eq!(entry["description"], description);
            assert_eq!(entry["migration_status"], "available");
            initial_revisions[index] = catalog["revision"].as_u64().unwrap();
        }
    }

    let deleted = actix_web::test::call_service(
        &app,
        actix_web::test::TestRequest::delete()
            .uri("/workflows/live-global")
            .to_request(),
    )
    .await;
    assert!(deleted.status().is_success());
    for (index, session_id) in session_ids.iter().enumerate() {
        let catalog: serde_json::Value = actix_web::test::call_and_read_body_json(
            &app,
            actix_web::test::TestRequest::get()
                .uri(&format!("/catalog?session_id={session_id}"))
                .to_request(),
        )
        .await;
        assert!(catalog["revision"].as_u64().unwrap() > initial_revisions[index]);
        assert!(!catalog["entries"]
            .as_array()
            .expect("entries")
            .iter()
            .any(|entry| entry["id"] == "live-global"));
    }
}

#[actix_web::test]
async fn historical_legacy_import_remains_a_workflow_without_rewriting_its_bundle() {
    let data = tempfile::tempdir().expect("data dir");
    let source = data.path().join("workflows/legacy.md");
    tokio::fs::create_dir_all(source.parent().expect("workflow parent"))
        .await
        .expect("workflow dir");
    tokio::fs::write(&source, "Current Workflow source\n")
        .await
        .expect("workflow source");
    let bundle = data.path().join("skills/legacy/SKILL.md");
    tokio::fs::create_dir_all(bundle.parent().expect("bundle parent"))
        .await
        .expect("bundle dir");
    let old_bundle = format!(
        "---\nname: legacy\ndescription: Imported legacy workflow\nmetadata:\n  legacy_import: true\n  legacy_name: legacy\n  original_source: '{}'\n---\nHistorical copied body\n",
        source.display()
    );
    tokio::fs::write(&bundle, &old_bundle)
        .await
        .expect("legacy bundle");

    let state = actix_web::web::Data::new(
        crate::app_state::AppState::new(data.path().to_path_buf())
            .await
            .expect("app state"),
    );
    let app = actix_web::test::init_service(
        actix_web::App::new()
            .app_data(state)
            .route(
                "/catalog",
                actix_web::web::get().to(super::list_workflow_catalog),
            )
            .route(
                "/workflows/{name}",
                actix_web::web::get().to(super::get_workflow),
            )
            .route(
                "/workflows/{name}",
                actix_web::web::delete().to(super::delete_workflow),
            ),
    )
    .await;

    let catalog: serde_json::Value = actix_web::test::call_and_read_body_json(
        &app,
        actix_web::test::TestRequest::get()
            .uri("/catalog")
            .to_request(),
    )
    .await;
    let entry = catalog["entries"]
        .as_array()
        .expect("catalog entries")
        .iter()
        .find(|entry| entry["id"] == "legacy")
        .expect("legacy Workflow");
    assert_eq!(entry["legacy"], true);
    assert_eq!(entry["migration_status"], "available");

    let workflow: serde_json::Value = actix_web::test::call_and_read_body_json(
        &app,
        actix_web::test::TestRequest::get()
            .uri("/workflows/legacy")
            .to_request(),
    )
    .await;
    assert_eq!(workflow["content"], "Current Workflow source\n");
    assert_eq!(
        tokio::fs::read_to_string(&bundle)
            .await
            .expect("bundle preserved"),
        old_bundle
    );

    let deleted = actix_web::test::call_service(
        &app,
        actix_web::test::TestRequest::delete()
            .uri("/workflows/legacy")
            .to_request(),
    )
    .await;
    assert!(deleted.status().is_success());
    assert!(!source.exists());
    assert!(
        !bundle.exists(),
        "explicit deletion may clean only the adapter owned by this source"
    );
    let after: serde_json::Value = actix_web::test::call_and_read_body_json(
        &app,
        actix_web::test::TestRequest::get()
            .uri("/catalog")
            .to_request(),
    )
    .await;
    assert!(!after["entries"]
        .as_array()
        .expect("catalog entries")
        .iter()
        .any(|entry| entry["id"] == "legacy"));
}

#[actix_web::test]
async fn deleting_legacy_source_never_deletes_a_same_id_ordinary_skill() {
    let data = tempfile::tempdir().expect("data dir");
    let source = data.path().join("workflows/shared.md");
    tokio::fs::create_dir_all(source.parent().expect("workflow parent"))
        .await
        .expect("workflow dir");
    tokio::fs::write(&source, "Workflow source\n")
        .await
        .expect("workflow source");
    let skill = data.path().join("skills/shared/SKILL.md");
    tokio::fs::create_dir_all(skill.parent().expect("skill parent"))
        .await
        .expect("skill dir");
    let ordinary = "---\nname: shared\ndescription: Ordinary Skill\n---\nOrdinary Skill body\n";
    tokio::fs::write(&skill, ordinary)
        .await
        .expect("ordinary skill");
    let state = actix_web::web::Data::new(
        crate::app_state::AppState::new(data.path().to_path_buf())
            .await
            .expect("app state"),
    );
    let app = actix_web::test::init_service(actix_web::App::new().app_data(state).route(
        "/workflows/{name}",
        actix_web::web::delete().to(super::delete_workflow),
    ))
    .await;

    let deleted = actix_web::test::call_service(
        &app,
        actix_web::test::TestRequest::delete()
            .uri("/workflows/shared")
            .to_request(),
    )
    .await;
    assert!(deleted.status().is_success());
    assert!(!source.exists());
    assert_eq!(
        tokio::fs::read_to_string(&skill)
            .await
            .expect("ordinary Skill preserved"),
        ordinary
    );
}

#[actix_web::test]
async fn concurrent_legacy_updates_leave_one_source_and_no_skill_bundle() {
    let data = tempfile::tempdir().expect("data dir");
    let state = actix_web::web::Data::new(
        crate::app_state::AppState::new(data.path().to_path_buf())
            .await
            .expect("app state"),
    );
    let app = actix_web::test::init_service(actix_web::App::new().app_data(state).route(
        "/workflows",
        actix_web::web::post().to(super::save_workflow),
    ))
    .await;
    let first = actix_web::test::TestRequest::post()
        .uri("/workflows")
        .set_json(serde_json::json!({"name": "race", "content": "body one"}))
        .to_request();
    let second = actix_web::test::TestRequest::post()
        .uri("/workflows")
        .set_json(serde_json::json!({"name": "race", "content": "body two"}))
        .to_request();
    let (first, second) = tokio::join!(
        actix_web::test::call_service(&app, first),
        actix_web::test::call_service(&app, second)
    );
    assert!(first.status().is_success());
    assert!(second.status().is_success());
    let source = tokio::fs::read_to_string(data.path().join("workflows/race.md"))
        .await
        .expect("source");
    assert!(source == "body one" || source == "body two");
    assert!(
        !data.path().join("skills/race/SKILL.md").exists(),
        "concurrent Workflow writes must not create a Skill"
    );
    let mut entries = tokio::fs::read_dir(data.path().join("workflows"))
        .await
        .expect("workflow dir");
    while let Some(entry) = entries.next_entry().await.expect("entry") {
        assert!(!entry.file_name().to_string_lossy().ends_with(".tmp"));
    }
}

#[actix_web::test]
async fn legacy_api_preserves_names_outside_skill_id_grammar() {
    let data = tempfile::tempdir().expect("data dir");
    let state = actix_web::web::Data::new(
        crate::app_state::AppState::new(data.path().to_path_buf())
            .await
            .expect("app state"),
    );
    let app = actix_web::test::init_service(
        actix_web::App::new()
            .app_data(state)
            .route(
                "/workflows",
                actix_web::web::get().to(super::list_workflows),
            )
            .route(
                "/workflows",
                actix_web::web::post().to(super::save_workflow),
            )
            .route(
                "/workflows/{name}",
                actix_web::web::get().to(super::get_workflow),
            ),
    )
    .await;
    let name = "发布 Workflow_v2";
    let request = actix_web::test::TestRequest::post()
        .uri("/workflows")
        .set_json(serde_json::json!({"name": name, "content": "Original body"}))
        .to_request();
    let response = actix_web::test::call_service(&app, request).await;
    assert!(response.status().is_success());

    let request = actix_web::test::TestRequest::get()
        .uri("/workflows")
        .to_request();
    let listed: serde_json::Value = actix_web::test::call_and_read_body_json(&app, request).await;
    assert!(listed
        .as_array()
        .expect("workflow list")
        .iter()
        .any(|item| item["name"] == name));

    let request = actix_web::test::TestRequest::get()
        .uri("/workflows/%E5%8F%91%E5%B8%83%20Workflow_v2")
        .to_request();
    let loaded: serde_json::Value = actix_web::test::call_and_read_body_json(&app, request).await;
    assert_eq!(loaded["name"], name);
    assert_eq!(loaded["content"], "Original body");
}

#[test]
fn safe_workflow_name_accepts_normal_names() {
    assert!(is_safe_workflow_name("my-workflow_01"));
    assert!(is_safe_workflow_name("workflow.v2"));
    assert!(is_safe_workflow_name("Workflow Name"));
}

#[test]
fn safe_workflow_name_rejects_path_traversal_and_control_chars() {
    assert!(!is_safe_workflow_name("../secret"));
    assert!(!is_safe_workflow_name("folder/name"));
    assert!(!is_safe_workflow_name("line\nbreak"));
    assert!(!is_safe_workflow_name(" null\0byte"));
}

#[test]
fn safe_workflow_name_rejects_reserved_windows_names() {
    assert!(!is_safe_workflow_name("CON"));
    assert!(!is_safe_workflow_name("nul.txt"));
    assert!(!is_safe_workflow_name("LPT1"));
}

#[test]
fn safe_workflow_name_rejects_empty_string() {
    assert!(!is_safe_workflow_name(""));
}

#[test]
fn safe_workflow_name_rejects_whitespace_only() {
    assert!(!is_safe_workflow_name("   "));
    assert!(!is_safe_workflow_name("\t"));
    assert!(!is_safe_workflow_name("\n"));
}

#[test]
fn safe_workflow_name_rejects_leading_trailing_whitespace() {
    assert!(!is_safe_workflow_name(" workflow"));
    assert!(!is_safe_workflow_name("workflow "));
    assert!(!is_safe_workflow_name(" workflow "));
}

#[test]
fn safe_workflow_name_rejects_path_separators() {
    assert!(!is_safe_workflow_name("path/to/workflow"));
    assert!(!is_safe_workflow_name("path\\to\\workflow"));
    assert!(!is_safe_workflow_name("a/b"));
    assert!(!is_safe_workflow_name("a\\b"));
}

#[test]
fn safe_workflow_name_rejects_double_dots() {
    assert!(!is_safe_workflow_name(".."));
    assert!(!is_safe_workflow_name("a..b"));
    assert!(!is_safe_workflow_name("test..workflow"));
}

#[test]
fn safe_workflow_name_rejects_control_characters() {
    assert!(!is_safe_workflow_name("\x01"));
    assert!(!is_safe_workflow_name("work\x02flow"));
    assert!(!is_safe_workflow_name("test\x1F"));
    assert!(!is_safe_workflow_name("\x7F"));
}

#[test]
fn safe_workflow_name_rejects_very_long_names() {
    let long_name = "a".repeat(256);
    assert!(!is_safe_workflow_name(&long_name));

    let exactly_255 = "a".repeat(255);
    assert!(is_safe_workflow_name(&exactly_255));
}

#[test]
fn safe_workflow_name_accepts_various_characters() {
    assert!(is_safe_workflow_name("workflow-1"));
    assert!(is_safe_workflow_name("workflow_2"));
    assert!(is_safe_workflow_name("workflow.3"));
    assert!(is_safe_workflow_name("workflow 4"));
    assert!(is_safe_workflow_name("Workflow5"));
    assert!(is_safe_workflow_name("123"));
}

#[test]
fn safe_workflow_name_rejects_all_reserved_windows_names() {
    let reserved = [
        "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
        "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    ];

    for name in reserved.iter() {
        assert!(!is_safe_workflow_name(name), "Should reject {}", name);
        assert!(
            !is_safe_workflow_name(&format!("{}.txt", name)),
            "Should reject {}.txt",
            name
        );
    }
}

#[test]
fn safe_workflow_name_rejects_special_characters() {
    assert!(!is_safe_workflow_name("workflow@home"));
    assert!(!is_safe_workflow_name("workflow#1"));
    assert!(!is_safe_workflow_name("workflow!"));
    assert!(!is_safe_workflow_name("workflow$"));
    assert!(!is_safe_workflow_name("workflow%"));
    assert!(!is_safe_workflow_name("workflow&"));
    assert!(!is_safe_workflow_name("workflow*"));
    assert!(!is_safe_workflow_name("workflow+"));
    assert!(!is_safe_workflow_name("workflow="));
    assert!(!is_safe_workflow_name("workflow|"));
    assert!(!is_safe_workflow_name("workflow?"));
    assert!(!is_safe_workflow_name("workflow["));
    assert!(!is_safe_workflow_name("workflow]"));
    assert!(!is_safe_workflow_name("workflow{"));
    assert!(!is_safe_workflow_name("workflow}"));
    assert!(!is_safe_workflow_name("workflow("));
    assert!(!is_safe_workflow_name("workflow)"));
    assert!(!is_safe_workflow_name("workflow<"));
    assert!(!is_safe_workflow_name("workflow>"));
    assert!(!is_safe_workflow_name("workflow,"));
    assert!(!is_safe_workflow_name("workflow:"));
    assert!(!is_safe_workflow_name("workflow;"));
}

#[test]
fn safe_workflow_name_accepts_edge_cases() {
    // Single character
    assert!(is_safe_workflow_name("a"));
    assert!(is_safe_workflow_name("Z"));
    assert!(is_safe_workflow_name("1"));

    // Numbers only
    assert!(is_safe_workflow_name("12345"));

    // Mixed case
    assert!(is_safe_workflow_name("MyWorkflow"));

    // With all allowed special chars
    assert!(is_safe_workflow_name("my-workflow_v2.3 test"));
}

#[test]
fn safe_workflow_name_accepts_unicode_alphanumeric() {
    // Unicode letters and numbers are considered alphanumeric by Rust
    assert!(is_safe_workflow_name("你好"));
    assert!(is_safe_workflow_name("工作流"));
    assert!(is_safe_workflow_name("ワークフロー"));
    assert!(is_safe_workflow_name("αβγ"));
    assert!(is_safe_workflow_name("τρόπος"));
}

#[test]
fn safe_workflow_name_rejects_unicode_special_chars() {
    // Special unicode symbols are not alphanumeric
    assert!(!is_safe_workflow_name("workflow©"));
    assert!(!is_safe_workflow_name("workflow®"));
    assert!(!is_safe_workflow_name("test™"));
    assert!(!is_safe_workflow_name("workflow€"));
}
