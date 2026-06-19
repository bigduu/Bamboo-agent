use actix_web::{http::StatusCode, web};
use bamboo_skills::types::SkillDefinition;

use super::sources::skill_to_command;

#[test]
fn skill_to_command_maps_core_fields() {
    let skill =
        SkillDefinition::new("sample", "Sample", "Demo skill", "Use me").with_tool_ref("read_file");
    let command = skill_to_command(&skill);

    assert_eq!(command.id, "skill-sample");
    assert_eq!(command.name, "sample");
    assert_eq!(command.display_name, "Sample");
    assert_eq!(command.command_type, "skill");
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
