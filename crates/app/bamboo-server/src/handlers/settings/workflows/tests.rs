use super::validation::is_safe_workflow_name;

#[actix_web::test]
async fn workflow_catalog_api_returns_metadata_without_skill_body() {
    let data = tempfile::tempdir().expect("data dir");
    let skill = data.path().join("skills/review");
    tokio::fs::create_dir_all(&skill).await.expect("skill dir");
    tokio::fs::write(
        skill.join("SKILL.md"),
        "---\nname: review\ndescription: Reviews changes\n---\nTOP SECRET INSTRUCTIONS\n",
    )
    .await
    .expect("skill");
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
    assert!(text.contains("Reviews changes"));
    assert!(text.contains("\"revision\""));
    assert!(!text.contains("TOP SECRET INSTRUCTIONS"));
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
async fn legacy_api_writes_through_bundle_and_bridges_catalog_event() {
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
        tokio::fs::read_to_string(data.path().join("skills/legacy/SKILL.md"))
            .await
            .expect("adapter bundle")
            .contains("Second body")
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
async fn concurrent_legacy_updates_leave_source_and_bundle_consistent() {
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
    let bundle = tokio::fs::read_to_string(data.path().join("skills/race/SKILL.md"))
        .await
        .expect("bundle");
    assert!(bundle.contains(&source));
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
