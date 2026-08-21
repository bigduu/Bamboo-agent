use super::validation::is_safe_workflow_name;

const MAX_LEGACY_FAILURE_BODY_BYTES: usize = 2 * 1024;

/// Preserve the endpoint's actual status and response body when a success
/// assertion fails without allowing an unexpectedly large or unescaped body to
/// flood CI logs. This endpoint and its synthetic fixture carry no credentials.
async fn assert_legacy_endpoint_success(context: &str, response: actix_web::dev::ServiceResponse) {
    let status = response.status();
    if status.is_success() {
        return;
    }

    let body = actix_web::test::read_body(response).await;
    let visible_len = body.len().min(MAX_LEGACY_FAILURE_BODY_BYTES);
    let visible = String::from_utf8_lossy(&body[..visible_len]);
    let truncated = if body.len() > visible_len {
        " <truncated>"
    } else {
        ""
    };
    panic!("{context} failed: status={status}, body={visible:?}{truncated}");
}

#[actix_web::test]
async fn workflow_catalog_unifies_instruction_and_orchestration_metadata_without_bodies() {
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
    let app = actix_web::test::init_service(actix_web::App::new().app_data(state.clone()).route(
        "/catalog",
        actix_web::web::get().to(super::list_workflow_catalog),
    ))
    .await;
    let request = actix_web::test::TestRequest::get()
        .uri("/catalog")
        .to_request();
    let body = actix_web::test::call_and_read_body(&app, request).await;
    let text = std::str::from_utf8(&body).expect("utf8 response");
    assert!(text.contains("Reviews changes"));
    assert!(text.contains("\"kind\":\"instruction\""));
    assert!(text.contains("Deploys changes"));
    assert!(text.contains("\"kind\":\"orchestration\""));
    assert!(text.contains("\"revision\""));
    assert!(!text.contains("TOP SECRET INSTRUCTIONS"));
    assert!(!text.contains("WORKFLOW SECRET BODY"));
    assert!(!text.contains("SKILL.md"));
    let initial: serde_json::Value = serde_json::from_slice(&body).expect("catalog json");
    let review = initial["entries"]
        .as_array()
        .expect("entries")
        .iter()
        .find(|entry| entry["id"] == "review")
        .expect("review entry");
    assert_eq!(review["source"], "user");
    assert_eq!(review["status"], "valid");
    assert!(review["shadowed_candidates"]
        .as_array()
        .expect("shadowed candidates")
        .iter()
        .any(|candidate| candidate["source"] == "builtin"));

    const PRIVATE_INVALID_FIELD: &str = "private_invalid_catalog_field";
    const PRIVATE_INVALID_BODY: &str = "PRIVATE INVALID REPLACEMENT BODY";
    tokio::fs::write(
        skill.join("SKILL.md"),
        format!(
            "---\nname: review\ndescription: changed too early\n{PRIVATE_INVALID_FIELD}: secret\n---\n{PRIVATE_INVALID_BODY}\n"
        ),
    )
    .await
    .expect("break instruction bundle");
    state
        .skill_manager
        .store()
        .reload_global_workflow_views()
        .await
        .expect("invalid publication stays isolated");
    let invalid: serde_json::Value = actix_web::test::call_and_read_body_json(
        &app,
        actix_web::test::TestRequest::get()
            .uri("/catalog")
            .to_request(),
    )
    .await;
    let invalid_review = invalid["entries"]
        .as_array()
        .expect("invalid entries")
        .iter()
        .find(|entry| entry["id"] == "review")
        .expect("invalid review remains visible");
    assert_eq!(invalid_review["status"], "invalid");
    assert_eq!(invalid_review["description"], "Reviews changes");
    assert!(invalid_review["last_error"].is_string());
    let rendered = invalid_review.to_string();
    assert!(!rendered.contains(PRIVATE_INVALID_FIELD));
    assert!(!rendered.contains(PRIVATE_INVALID_BODY));
    assert!(!rendered.contains(data.path().to_string_lossy().as_ref()));
    assert!(invalid_review["shadowed_candidates"]
        .as_array()
        .expect("invalid shadowed candidates")
        .iter()
        .any(|candidate| candidate["source"] == "builtin"));

    tokio::fs::write(
        skill.join("SKILL.md"),
        "---\nname: review\ndescription: Recovered review\n---\nRECOVERED PRIVATE BODY\n",
    )
    .await
    .expect("repair instruction bundle");
    state
        .skill_manager
        .store()
        .reload_global_workflow_views()
        .await
        .expect("recovered publication");
    let recovered: serde_json::Value = actix_web::test::call_and_read_body_json(
        &app,
        actix_web::test::TestRequest::get()
            .uri("/catalog")
            .to_request(),
    )
    .await;
    let recovered_review = recovered["entries"]
        .as_array()
        .expect("recovered entries")
        .iter()
        .find(|entry| entry["id"] == "review")
        .expect("recovered review");
    assert_eq!(recovered_review["status"], "valid");
    assert_eq!(recovered_review["description"], "Recovered review");
    assert!(recovered_review.get("last_error").is_none());
    assert!(!recovered_review
        .to_string()
        .contains("RECOVERED PRIVATE BODY"));
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
async fn builtin_clone_to_user_is_exact_read_only_and_never_overwrites() {
    let data = tempfile::tempdir().expect("data dir");
    let state = actix_web::web::Data::new(
        crate::app_state::AppState::new(data.path().to_path_buf())
            .await
            .expect("app state"),
    );
    let review = state
        .skill_manager
        .store()
        .skill_catalog_snapshot()
        .await
        .entries
        .into_iter()
        .find(|entry| {
            entry.id == "review" && entry.source == bamboo_skills::WorkflowSource::Builtin
        })
        .expect("builtin review");
    let app = actix_web::test::init_service(actix_web::App::new().app_data(state.clone()).route(
        "/catalog/{workflow_id}/clone",
        actix_web::web::post().to(super::clone_workflow),
    ))
    .await;
    let payload = serde_json::json!({
        "source": "builtin",
        "revision": review.revision,
        "target": "user",
        "session_id": "ignored-for-user-target"
    });
    let response = actix_web::test::call_service(
        &app,
        actix_web::test::TestRequest::post()
            .uri("/catalog/review/clone")
            .set_json(&payload)
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), actix_web::http::StatusCode::CREATED);
    let body: serde_json::Value = actix_web::test::read_body_json(response).await;
    assert_eq!(body["entry"]["source"], "user");
    assert_eq!(body["entry"]["id"], "review");
    assert_eq!(body["source_preserved"], true);
    let clone = data.path().join("skills/review/SKILL.md");
    let builtin = data.path().join("skills-builtin-v1/review/SKILL.md");
    assert!(clone.is_file());
    assert!(
        builtin.is_file(),
        "read-only builtin source must remain intact"
    );
    let cloned_before = std::fs::read(&clone).expect("clone bytes");
    assert!(!data.path().join("skills/.review.clone-v1.json").exists());

    let conflict = actix_web::test::call_service(
        &app,
        actix_web::test::TestRequest::post()
            .uri("/catalog/review/clone")
            .set_json(&payload)
            .to_request(),
    )
    .await;
    assert_eq!(conflict.status(), actix_web::http::StatusCode::CONFLICT);
    assert_eq!(
        std::fs::read(&clone).expect("clone remains"),
        cloned_before,
        "repeat clone must not overwrite an editable user bundle"
    );
}

#[actix_web::test]
async fn builtin_clone_to_project_uses_durable_session_identity_not_a_client_path() {
    let data = tempfile::tempdir().expect("data dir");
    let workspace = tempfile::tempdir().expect("workspace");
    let state = actix_web::web::Data::new(
        crate::app_state::AppState::new(data.path().to_path_buf())
            .await
            .expect("app state"),
    );
    let project = state
        .project_store
        .create("Clone Project", None)
        .expect("create Project");
    let mut session = bamboo_agent_core::Session::new("project-clone-session", "model");
    session.set_project_id_meta(project.id.to_string());
    session.set_workspace_path_meta(workspace.path().to_string_lossy().into_owned());
    state.save_and_cache_session(&mut session).await;
    let store = state
        .skill_manager
        .store_for_project_workspace(
            &project.id,
            &state.project_store.paths().project_home(&project.id),
            Some(workspace.path()),
        )
        .await
        .expect("Project Workflow store");
    let review = store
        .skill_catalog_snapshot()
        .await
        .entries
        .into_iter()
        .find(|entry| {
            entry.id == "review" && entry.source == bamboo_skills::WorkflowSource::Builtin
        })
        .expect("builtin review");
    let app = actix_web::test::init_service(actix_web::App::new().app_data(state.clone()).route(
        "/catalog/{workflow_id}/clone",
        actix_web::web::post().to(super::clone_workflow),
    ))
    .await;
    let response = actix_web::test::call_service(
        &app,
        actix_web::test::TestRequest::post()
            .uri("/catalog/review/clone")
            .set_json(serde_json::json!({
                "source": "builtin",
                "revision": review.revision,
                "target": "project",
                "session_id": "project-clone-session"
            }))
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), actix_web::http::StatusCode::CREATED);
    let body: serde_json::Value = actix_web::test::read_body_json(response).await;
    assert_eq!(body["entry"]["source"], "project");
    assert!(state
        .project_store
        .paths()
        .project_home(&project.id)
        .join("skills/review/SKILL.md")
        .is_file());
    assert!(
        !data.path().join("skills/review/SKILL.md").exists(),
        "Project clone must not fall back to the user layer"
    );
}

#[actix_web::test]
async fn builtin_clone_rejects_archived_project_without_writing() {
    let data = tempfile::tempdir().expect("data dir");
    let workspace = tempfile::tempdir().expect("workspace");
    let state = actix_web::web::Data::new(
        crate::app_state::AppState::new(data.path().to_path_buf())
            .await
            .expect("app state"),
    );
    let project = state
        .project_store
        .create_with_project_path(
            "Archived Clone Project",
            None,
            workspace.path().to_string_lossy(),
            Vec::new(),
        )
        .expect("create Project");
    state
        .project_store
        .archive(&project.id, project.revision)
        .expect("archive Project");
    let mut session = bamboo_agent_core::Session::new("archived-clone-session", "model");
    session.set_project_id_meta(project.id.to_string());
    session.set_workspace_path_meta(workspace.path().to_string_lossy().into_owned());
    session.metadata.insert(
        bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY.to_string(),
        bamboo_engine::project_context::WorkspaceSource::Explicit
            .as_str()
            .to_string(),
    );
    state.save_and_cache_session(&mut session).await;
    let review = state
        .skill_manager
        .store()
        .skill_catalog_snapshot()
        .await
        .entries
        .into_iter()
        .find(|entry| {
            entry.id == "review" && entry.source == bamboo_skills::WorkflowSource::Builtin
        })
        .expect("builtin review");
    let app = actix_web::test::init_service(actix_web::App::new().app_data(state.clone()).route(
        "/catalog/{workflow_id}/clone",
        actix_web::web::post().to(super::clone_workflow),
    ))
    .await;

    let response = actix_web::test::call_service(
        &app,
        actix_web::test::TestRequest::post()
            .uri("/catalog/review/clone")
            .set_json(serde_json::json!({
                "source": "builtin",
                "revision": review.revision,
                "target": "project",
                "session_id": session.id,
            }))
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), actix_web::http::StatusCode::FORBIDDEN);
    assert!(!state
        .project_store
        .paths()
        .project_home(&project.id)
        .join("skills/review")
        .exists());
}

#[actix_web::test]
async fn project_clone_serializes_scope_resolution_and_reassignment_through_publication() {
    let data = tempfile::tempdir().expect("data dir");
    let workspace_a = tempfile::tempdir().expect("workspace A");
    let workspace_b = tempfile::tempdir().expect("workspace B");
    let state = actix_web::web::Data::new(
        crate::app_state::AppState::new(data.path().to_path_buf())
            .await
            .expect("app state"),
    );
    let project_a = state
        .project_store
        .create_with_project_path(
            "Clone Project A",
            None,
            workspace_a.path().to_string_lossy(),
            Vec::new(),
        )
        .expect("Project A");
    let project_b = state
        .project_store
        .create_with_project_path(
            "Clone Project B",
            None,
            workspace_b.path().to_string_lossy(),
            Vec::new(),
        )
        .expect("Project B");
    let session_id = "clone-reassignment-race";
    let mut session = bamboo_agent_core::Session::new(session_id, "model");
    session.set_project_id_meta(project_a.id.to_string());
    session.set_workspace_path_meta(workspace_a.path().to_string_lossy().into_owned());
    session.metadata.insert(
        bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY.to_string(),
        bamboo_engine::project_context::WorkspaceSource::Explicit
            .as_str()
            .to_string(),
    );
    state.save_and_cache_session(&mut session).await;
    let review = state
        .skill_manager
        .store()
        .skill_catalog_snapshot()
        .await
        .entries
        .into_iter()
        .find(|entry| {
            entry.id == "review" && entry.source == bamboo_skills::WorkflowSource::Builtin
        })
        .expect("builtin review");
    let hook = super::handlers::clone_scope_test_hooks::install(session_id);
    let app = actix_web::test::init_service(
        actix_web::App::new()
            .app_data(state.clone())
            .configure(crate::routes::configure_routes),
    )
    .await;
    let clone_request = actix_web::test::TestRequest::post()
        .uri("/api/v1/bamboo/workflow-catalog/review/clone")
        .set_json(serde_json::json!({
            "source": "builtin",
            "revision": review.revision,
            "target": "project",
            "session_id": session_id,
        }))
        .to_request();
    let clone = actix_web::test::call_service(&app, clone_request);
    let reassign = async {
        hook.reached
            .acquire()
            .await
            .expect("clone scope barrier remains open")
            .forget();
        let patch_request = actix_web::test::TestRequest::patch()
            .uri(&format!("/api/v1/sessions/{session_id}"))
            .insert_header((actix_web::http::header::IF_MATCH, "\"0\""))
            .set_json(serde_json::json!({
                "project_id": project_b.id,
                "workspace_path": workspace_b.path(),
            }))
            .to_request();
        let mut patch = Box::pin(actix_web::test::call_service(&app, patch_request));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), patch.as_mut())
                .await
                .is_err(),
            "Project PATCH must wait while clone owns the session authority lock"
        );
        hook.resume.add_permits(1);
        patch.await
    };
    let (clone_response, patch_response) = futures::join!(clone, reassign);

    assert_eq!(
        clone_response.status(),
        actix_web::http::StatusCode::CREATED
    );
    assert_eq!(patch_response.status(), actix_web::http::StatusCode::OK);
    assert!(state
        .project_store
        .paths()
        .project_home(&project_a.id)
        .join("skills/review/SKILL.md")
        .is_file());
    assert!(!state
        .project_store
        .paths()
        .project_home(&project_b.id)
        .join("skills/review")
        .exists());
    let persisted = state
        .storage
        .load_session(session_id)
        .await
        .expect("load reassigned session")
        .expect("session");
    assert_eq!(
        persisted.project_id_meta().as_deref(),
        Some(project_b.id.as_str())
    );
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
    state.save_and_cache_session(&mut session).await;

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
    state.save_and_cache_session(&mut session).await;
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
    let entries = after["entries"].as_array().expect("catalog entries");
    let migrated_workflow = entries
        .iter()
        .find(|entry| entry["id"] == "daily-report" && entry["migration_status"] == "migrated")
        .expect("migrated instruction Workflow entry");
    let source_workflow = entries
        .iter()
        .find(|entry| entry["id"] == "daily-report" && entry["migration_status"] == "available")
        .expect("source Workflow entry");
    assert_ne!(
        migrated_workflow["revision"], source_workflow["revision"],
        "id/source/revision remains an unambiguous typed catalog identity"
    );
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
    state.save_and_cache_session(&mut session).await;
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
async fn legacy_migration_reloads_durable_workspace_after_waiting_for_reassignment() {
    let data = tempfile::tempdir().expect("data dir");
    let workspace_a = tempfile::tempdir().expect("workspace A");
    let workspace_b = tempfile::tempdir().expect("workspace B");
    let global_workflows = data.path().join("workflows");
    std::fs::create_dir_all(&global_workflows).expect("global workflows");
    std::fs::write(
        global_workflows.join("durable-scope.md"),
        "---\ndescription: Durable scope migration.\n---\nUse the current workspace.\n",
    )
    .expect("global legacy workflow");
    let state = actix_web::web::Data::new(
        crate::app_state::AppState::new(data.path().to_path_buf())
            .await
            .expect("app state"),
    );
    let session_id = "migration-durable-scope";
    let mut session = bamboo_agent_core::Session::new(session_id, "test-model");
    session.set_workspace_path_meta(workspace_a.path().to_string_lossy().into_owned());
    state.save_and_cache_session(&mut session).await;
    let app = actix_web::test::init_service(actix_web::App::new().app_data(state.clone()).route(
        "/catalog/{workflow_id}/migrate",
        actix_web::web::post().to(super::migrate_workflow),
    ))
    .await;

    let guard = state.persistence.acquire_lock(session_id).await;
    let migration = actix_web::test::call_service(
        &app,
        actix_web::test::TestRequest::post()
            .uri("/catalog/durable-scope/migrate")
            .set_json(serde_json::json!({"session_id": session_id}))
            .to_request(),
    );
    tokio::pin!(migration);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut migration)
            .await
            .is_err(),
        "migration must wait for the session authority lock"
    );

    let mut reassigned = state
        .persistence
        .storage()
        .load_session(session_id)
        .await
        .expect("load authoritative session")
        .expect("session");
    reassigned.set_workspace_path_meta(workspace_b.path().to_string_lossy().into_owned());
    state
        .persistence
        .storage()
        .save_session(&reassigned)
        .await
        .expect("persist reassignment while owning lock");
    // Deliberately leave the memory cache pointing at workspace A. The
    // migration must reload durable authority after it acquires the lock.
    drop(guard);

    let response = migration.await;
    assert!(response.status().is_success());
    assert!(workspace_b
        .path()
        .join(".bamboo/skills/durable-scope/SKILL.md")
        .is_file());
    assert!(
        !workspace_a
            .path()
            .join(".bamboo/skills/durable-scope")
            .exists(),
        "stale cache scope must never receive the migrated bundle"
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
async fn instruction_skill_changed_invalid_and_recovered_reach_account_feed() {
    let data = tempfile::tempdir().expect("data dir");
    let state = actix_web::web::Data::new(
        crate::app_state::AppState::new(data.path().to_path_buf())
            .await
            .expect("app state"),
    );
    let mut account_events = state.account_sink.subscribe();
    let skill_dir = data.path().join("skills/library-refresh");
    tokio::fs::create_dir_all(&skill_dir)
        .await
        .expect("skill dir");
    let skill_file = skill_dir.join("SKILL.md");
    let valid = "---\nname: library-refresh\ndescription: Refresh the Workflow Library.\n---\n\nRefresh it.\n";

    tokio::fs::write(&skill_file, valid)
        .await
        .expect("create instruction Skill");
    state
        .skill_manager
        .store()
        .reload()
        .await
        .expect("reload created Skill");
    tokio::fs::write(&skill_file, "---\nname: [\n")
        .await
        .expect("corrupt instruction Skill");
    state
        .skill_manager
        .store()
        .reload()
        .await
        .expect("publish invalid LKG Skill");
    tokio::fs::write(&skill_file, valid)
        .await
        .expect("recover instruction Skill");
    state
        .skill_manager
        .store()
        .reload()
        .await
        .expect("publish recovered Skill");

    let observed = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let mut changed = false;
        let mut invalid = false;
        let mut recovered = false;
        while !(changed && invalid && recovered) {
            let event = account_events.recv().await.expect("account event");
            match &event.event {
                bamboo_agent_core::AgentEvent::WorkflowChanged { workflow_id, .. }
                    if workflow_id == "library-refresh" =>
                {
                    changed = true
                }
                bamboo_agent_core::AgentEvent::WorkflowInvalid { workflow_id, .. }
                    if workflow_id == "library-refresh" =>
                {
                    invalid = true
                }
                bamboo_agent_core::AgentEvent::WorkflowRecovered { workflow_id, .. }
                    if workflow_id == "library-refresh" =>
                {
                    recovered = true
                }
                _ => {}
            }
        }
    })
    .await;
    assert!(
        observed.is_ok(),
        "instruction catalog transitions must invalidate Workflow clients"
    );
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
    const WRITES_PER_TASK: usize = 8;
    const EXPLICIT_RELOADS: usize = WRITES_PER_TASK * 2;

    let data = tempfile::tempdir().expect("data dir");
    let state = actix_web::web::Data::new(
        crate::app_state::AppState::new(data.path().to_path_buf())
            .await
            .expect("app state"),
    );
    let app = actix_web::test::init_service(actix_web::App::new().app_data(state.clone()).route(
        "/workflows",
        actix_web::web::post().to(super::save_workflow),
    ))
    .await;

    // Historically the handler and SkillStore::reload() both materialized the
    // same legacy source as a Skill bundle, but only the handler held the
    // legacy I/O lock. Concurrent first-time hard links could therefore turn a
    // successful source write into a non-success response. Exercise both paths
    // together for a fixed number of rounds and retain the source-only contract.
    let start = std::sync::Arc::new(tokio::sync::Barrier::new(3));
    let first_start = start.clone();
    let first_writer = async {
        first_start.wait().await;
        for round in 0..WRITES_PER_TASK {
            let response = actix_web::test::call_service(
                &app,
                actix_web::test::TestRequest::post()
                    .uri("/workflows")
                    .set_json(serde_json::json!({
                        "name": "race",
                        "content": format!("body one {round}")
                    }))
                    .to_request(),
            )
            .await;
            assert_legacy_endpoint_success("first concurrent POST /workflows", response).await;
            tokio::task::yield_now().await;
        }
    };
    let second_start = start.clone();
    let second_writer = async {
        second_start.wait().await;
        for round in 0..WRITES_PER_TASK {
            let response = actix_web::test::call_service(
                &app,
                actix_web::test::TestRequest::post()
                    .uri("/workflows")
                    .set_json(serde_json::json!({
                        "name": "race",
                        "content": format!("body two {round}")
                    }))
                    .to_request(),
            )
            .await;
            assert_legacy_endpoint_success("second concurrent POST /workflows", response).await;
            tokio::task::yield_now().await;
        }
    };
    let reload_start = start;
    let explicit_reloads = async {
        reload_start.wait().await;
        for _ in 0..EXPLICIT_RELOADS {
            state
                .skill_manager
                .store()
                .reload()
                .await
                .expect("explicit concurrent catalog reload");
            tokio::task::yield_now().await;
        }
    };
    tokio::join!(first_writer, second_writer, explicit_reloads);

    let source = tokio::fs::read_to_string(data.path().join("workflows/race.md"))
        .await
        .expect("source");
    assert!(
        source == format!("body one {}", WRITES_PER_TASK - 1)
            || source == format!("body two {}", WRITES_PER_TASK - 1)
    );
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
    assert_legacy_endpoint_success("POST /workflows with a legacy Unicode name", response).await;

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
