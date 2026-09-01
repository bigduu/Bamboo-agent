use actix_web::{test, web, App};
use bamboo_server::{configure_routes, AppState};

/// Regression for #717's chat publication seam.
///
/// `provider_owner` deterministically owns the process-global first-wins
/// workspace providers and advertises a real configured foreign default. A
/// second AppState with no configured default then creates and continues a chat
/// without an explicit workspace. The request state's authoritative `None`
/// must suppress that process-global default so preview and post-save
/// publication use its own session fallback.
#[actix_web::test]
async fn repeated_chat_materializes_request_states_own_session_fallback() {
    let provider_owner_home = tempfile::tempdir().expect("provider owner home");
    let foreign_default = tempfile::tempdir().expect("foreign default");
    let mut provider_owner_config = bamboo_config::Config::default();
    provider_owner_config.default_work_area = Some(bamboo_config::DefaultWorkAreaConfig {
        path: Some(foreign_default.path().to_string_lossy().into_owned()),
    });
    provider_owner_config
        .save_to_dir(provider_owner_home.path().to_path_buf())
        .expect("persist provider owner config");
    bamboo_config::paths::init_bamboo_dir(provider_owner_home.path().to_path_buf());
    let provider_owner = web::Data::new(
        AppState::new_with_memory_store(
            provider_owner_home.path().to_path_buf(),
            bamboo_memory::memory_store::MemoryStore::new(
                provider_owner_home.path().join("jiandu"),
            ),
        )
        .await
        .expect("provider owner state"),
    );
    assert_eq!(
        provider_owner
            .config
            .read()
            .await
            .get_default_work_area_path()
            .as_deref(),
        Some(foreign_default.path()),
        "provider owner must register the foreign default before request state construction"
    );

    let request_home = tempfile::tempdir().expect("request state home");
    let request_state = web::Data::new(
        AppState::new_with_memory_store(
            request_home.path().to_path_buf(),
            bamboo_memory::memory_store::MemoryStore::new(request_home.path().join("jiandu")),
        )
        .await
        .expect("request state"),
    );
    assert!(
        request_state
            .config
            .read()
            .await
            .get_default_work_area_path()
            .is_none(),
        "request state live config must authoritatively have no default"
    );
    let provider_owner_root =
        bamboo_config::paths::resolve_workspace_root_in(provider_owner_home.path());
    let request_root = bamboo_config::paths::resolve_workspace_root_in(request_home.path());
    let session_id = "chat-instance-workspace-isolation";
    let fallback = bamboo_agent_core::workspace_state::preview_default_session_workspace_dir(
        &request_root,
        session_id,
    );
    let expected = bamboo_agent_core::workspace_state::preview_pin_workspace_path(
        &fallback,
        &request_root,
        bamboo_config::paths::workspace_confinement_enforced(),
    );
    eprintln!(
        "chat_workspace_provider_schedule: provider_owner_root={}, foreign_default={}, \
         request_state_root={}, expected={}, source=session_fallback",
        provider_owner_root.display(),
        foreign_default.path().display(),
        request_root.display(),
        expected.display(),
    );

    let app = test::init_service(
        App::new()
            .app_data(request_state.clone())
            .configure(configure_routes),
    )
    .await;
    let first = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/chat")
            .set_json(serde_json::json!({
                "session_id": session_id,
                "message": "first message",
                "model": "test-model"
            }))
            .to_request(),
    )
    .await;
    assert!(
        first.status().is_success(),
        "first chat should persist the session, got {}",
        first.status()
    );

    let persisted = request_state
        .storage
        .load_session(session_id)
        .await
        .expect("load first chat")
        .expect("first chat session");
    let first_workspace = persisted
        .workspace_path_meta()
        .map(std::path::PathBuf::from)
        .expect("first chat must persist fallback metadata");
    assert_eq!(
        first_workspace,
        expected,
        "chat_workspace_provider_schedule: provider_owner_root={}, foreign_default={}, \
         request_state_root={}, resolved={}, source=session_fallback",
        provider_owner_root.display(),
        foreign_default.path().display(),
        request_root.display(),
        first_workspace.display(),
    );
    assert_ne!(
        first_workspace,
        foreign_default.path(),
        "request-state chat must not persist the first-wins provider owner's default"
    );
    assert_eq!(
        bamboo_agent_core::workspace_state::get_workspace(session_id).as_deref(),
        Some(first_workspace.as_path()),
        "first chat runtime publication must preserve the validated candidate"
    );
    let first_materialized = first_workspace.is_dir();

    let second = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/chat")
            .set_json(serde_json::json!({
                "session_id": session_id,
                "message": "second message",
                "model": "test-model"
            }))
            .to_request(),
    )
    .await;
    let second_status = second.status();
    let second_body = test::read_body(second).await;
    assert!(
        second_status.is_success(),
        "second chat failed after first durable fallback: status={second_status}, body={}, \
         provider_owner_root={}, foreign_default={}, request_state_root={}, resolved={}, \
         materialized={}, source=session_fallback",
        String::from_utf8_lossy(&second_body),
        provider_owner_root.display(),
        foreign_default.path().display(),
        request_root.display(),
        first_workspace.display(),
        first_materialized,
    );
    assert!(
        first_workspace.is_dir(),
        "request-state fallback was not materialized: request_state_root={}, resolved={}, \
         source=session_fallback",
        request_root.display(),
        first_workspace.display(),
    );
}
