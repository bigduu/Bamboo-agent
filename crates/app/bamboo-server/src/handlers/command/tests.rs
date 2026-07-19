use actix_web::{http::StatusCode, web};
use bamboo_skills::{WorkflowCatalogEntry, WorkflowKind, WorkflowSource, WorkflowStatus};

use std::collections::HashSet;

use super::handlers::{append_unique, expand_arguments};
use super::sources::{catalog_entry_to_command, list_markdown_commands};
use super::types::CommandItem;

#[test]
fn catalog_entry_to_command_maps_metadata_without_prompt() {
    let entry = WorkflowCatalogEntry {
        id: "sample".into(),
        name: "Sample".into(),
        description: "Demo skill".into(),
        kind: WorkflowKind::Instruction,
        source: WorkflowSource::Project,
        revision: 7,
        version: "1".into(),
        invocation_policy: serde_json::json!({"explicit": true}),
        argument_schema: serde_json::json!({"type": "object"}),
        status: WorkflowStatus::Valid,
        last_error: None,
        winner: true,
        shadowed_candidates: vec![],
    };
    let command = catalog_entry_to_command(&entry);

    assert_eq!(command.id, "skill-sample");
    assert_eq!(command.name, "sample");
    assert_eq!(command.display_name, "Sample");
    assert_eq!(command.command_type, "skill");
    assert!(command.metadata.get("prompt").is_none());
}

#[tokio::test]
async fn markdown_commands_support_namespace_frontmatter_and_arguments() {
    let project = tempfile::tempdir().expect("project");
    let commands_dir = bamboo_config::paths::project_commands_dir(project.path());
    tokio::fs::create_dir_all(commands_dir.join("db"))
        .await
        .expect("commands dir");
    tokio::fs::write(
        commands_dir.join("db/migrate.md"),
        "---\r\ndescription: Run a database migration\r\nargument-hint: <target>\r\n---\r\nMigrate $ARGUMENTS now.",
    )
    .await
    .expect("command");

    let commands = list_markdown_commands(&commands_dir, "project").await;
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].item.name, "db/migrate");
    assert_eq!(commands[0].item.description, "Run a database migration");
    assert_eq!(
        commands[0].item.metadata["argumentHint"],
        serde_json::json!("<target>")
    );
    assert_eq!(
        expand_arguments(&commands[0].content, "production"),
        "Migrate production now."
    );
}

#[actix_web::test]
async fn prompt_preset_list_item_can_be_resolved_and_expanded() {
    let data = tempfile::tempdir().expect("data dir");
    std::fs::write(
        data.path().join("prompt-presets.json"),
        r#"{"prompts":[{"id":"review","name":"Review","content":"Review $ARGUMENTS"}]}"#,
    )
    .expect("preset store");
    let app_state = web::Data::new(
        crate::app_state::AppState::new(data.path().to_path_buf())
            .await
            .expect("app state"),
    );
    let app = actix_web::test::init_service(
        actix_web::App::new()
            .app_data(app_state)
            .configure(super::config),
    )
    .await;
    let request = actix_web::test::TestRequest::get()
        .uri("/commands/prompt/review?arguments=code")
        .to_request();
    let body: serde_json::Value = actix_web::test::call_and_read_body_json(&app, request).await;
    assert_eq!(body["content"], "Review code");
}

#[cfg(unix)]
#[test]
fn project_command_root_cannot_escape_through_symlink() {
    use std::os::unix::fs::symlink;
    let workspace = tempfile::tempdir().expect("workspace");
    let outside = tempfile::tempdir().expect("outside");
    std::fs::create_dir_all(workspace.path().join(".bamboo")).expect("bamboo dir");
    symlink(outside.path(), workspace.path().join(".bamboo/commands")).expect("symlink");
    assert!(
        super::sources::safe_project_commands_dir(workspace.path().to_string_lossy().as_ref())
            .is_none()
    );
}

#[test]
fn nested_workspace_resolves_commands_from_nearest_git_project_root() {
    let project = tempfile::tempdir().expect("project");
    std::fs::create_dir_all(project.path().join(".git")).expect("git marker");
    let commands = project.path().join(".bamboo/commands");
    std::fs::create_dir_all(&commands).expect("commands");
    let nested = project.path().join("src/feature");
    std::fs::create_dir_all(&nested).expect("nested");

    let resolved = super::sources::safe_project_commands_dir(nested.to_string_lossy().as_ref())
        .expect("commands root");
    assert_eq!(
        resolved,
        std::fs::canonicalize(commands).expect("canonical")
    );
}

#[test]
fn nested_linked_worktree_uses_git_file_as_project_boundary() {
    let outer = tempfile::tempdir().expect("outer");
    let worktree = outer.path().join(".bamboo/worktree/task");
    let commands = worktree.join(".bamboo/commands");
    let nested = worktree.join("src/feature");
    std::fs::create_dir_all(&commands).expect("commands");
    std::fs::create_dir_all(&nested).expect("nested");
    std::fs::write(worktree.join(".git"), "gitdir: /repo/.git/worktrees/task").expect("git file");

    let resolved = super::sources::safe_project_commands_dir(nested.to_string_lossy().as_ref())
        .expect("commands root");
    assert_eq!(
        resolved,
        std::fs::canonicalize(commands).expect("canonical")
    );
}

fn command(name: &str, source: &str) -> CommandItem {
    CommandItem {
        id: format!("{source}-{name}"),
        name: name.to_string(),
        display_name: name.to_string(),
        description: source.to_string(),
        command_type: "prompt".to_string(),
        category: None,
        tags: None,
        metadata: serde_json::json!({"source": source}),
    }
}

#[test]
fn command_conflicts_keep_first_source_by_documented_precedence() {
    let mut merged = Vec::new();
    let mut seen = HashSet::new();
    append_unique(&mut merged, &mut seen, vec![command("review", "project")]);
    append_unique(&mut merged, &mut seen, vec![command("review", "global")]);
    append_unique(&mut merged, &mut seen, vec![command("review", "preset")]);
    append_unique(&mut merged, &mut seen, vec![command("review", "workflow")]);

    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0].metadata["source"], "project");
}

#[actix_web::test]
async fn get_command_rejects_path_traversal_in_workflow_id() {
    let temp = tempfile::tempdir().expect("create temp dir");
    std::fs::create_dir_all(temp.path().join("workflows")).expect("create workflows dir");
    // Secret file one level above the workflows dir. Without name validation,
    // `GET /commands/workflow/..%2Fsecret` resolves to this file and leaks it.
    std::fs::write(temp.path().join("secret.md"), "TOPSECRET")
        .expect("write secret file outside workflows dir");

    let app_state = web::Data::new(
        crate::app_state::AppState::new(temp.path().to_path_buf())
            .await
            .expect("app state should initialize"),
    );
    let app = actix_web::test::init_service(
        actix_web::App::new()
            .app_data(app_state)
            .configure(super::config),
    )
    .await;

    // Percent-encoded `..` keeps the payload within a single path segment so it
    // binds to the `{id}` route param instead of adding extra segments.
    let req = actix_web::test::TestRequest::get()
        .uri("/commands/workflow/..%2Fsecret")
        .to_request();
    let resp = actix_web::test::call_service(&app, req).await;

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[actix_web::test]
async fn project_command_is_listed_and_namespace_route_expands_arguments() {
    let data = tempfile::tempdir().expect("data dir");
    let project = tempfile::tempdir().expect("project dir");
    let commands_dir = bamboo_config::paths::project_commands_dir(project.path());
    std::fs::create_dir_all(commands_dir.join("db")).expect("commands dir");
    std::fs::write(
        commands_dir.join("db/migrate.md"),
        "Migrate $ARGUMENTS safely",
    )
    .expect("command");

    let app_state = web::Data::new(
        crate::app_state::AppState::new(data.path().to_path_buf())
            .await
            .expect("app state"),
    );
    let mut session = bamboo_agent_core::Session::new("workspace-session", "test-model");
    session.set_workspace_path_meta(project.path().to_string_lossy().to_string());
    app_state.sessions.insert(
        session.id.clone(),
        std::sync::Arc::new(parking_lot::RwLock::new(session)),
    );
    let app = actix_web::test::init_service(
        actix_web::App::new()
            .app_data(app_state)
            .configure(super::config),
    )
    .await;
    let list = actix_web::test::TestRequest::get()
        .uri("/commands?session_id=workspace-session")
        .to_request();
    let list_body: serde_json::Value = actix_web::test::call_and_read_body_json(&app, list).await;
    assert!(list_body["commands"]
        .as_array()
        .expect("commands")
        .iter()
        .any(|command| command["name"] == "db/migrate"));

    let get = actix_web::test::TestRequest::get()
        .uri("/commands/prompt/db/migrate?session_id=workspace-session&arguments=production")
        .to_request();
    let get_body: serde_json::Value = actix_web::test::call_and_read_body_json(&app, get).await;
    assert_eq!(get_body["content"], "Migrate production safely");
}

#[actix_web::test]
async fn arbitrary_workspace_path_is_rejected_for_list_and_get() {
    let data = tempfile::tempdir().expect("data dir");
    let outside = tempfile::tempdir().expect("outside");
    let app_state = web::Data::new(
        crate::app_state::AppState::new(data.path().to_path_buf())
            .await
            .expect("app state"),
    );
    let app = actix_web::test::init_service(
        actix_web::App::new()
            .app_data(app_state)
            .configure(super::config),
    )
    .await;
    for uri in [
        format!("/commands?workspace_path={}", outside.path().display()),
        format!(
            "/commands/prompt/secret?workspace_path={}",
            outside.path().display()
        ),
    ] {
        let response = actix_web::test::call_service(
            &app,
            actix_web::test::TestRequest::get().uri(&uri).to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
