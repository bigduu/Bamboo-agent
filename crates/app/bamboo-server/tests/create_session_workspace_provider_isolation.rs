use actix_web::{http::StatusCode, test, web, App};
use bamboo_server::{configure_routes, AppState};
use serde_json::Value;

/// Regression for #717.
///
/// `AppState::new` intentionally registers process-global workspace providers
/// with first-registration-wins semantics. This separate test binary gives us
/// a deterministic schedule: `provider_owner` registers first and advertises
/// a foreign configured default, while the create request is served by a
/// second state with no configured default. The request state must resolve,
/// persist, publish, and materialize its own session fallback without observing
/// the first state's provider.
#[actix_web::test]
async fn create_fallback_is_isolated_from_foreign_first_registered_provider() {
    let provider_owner_home = tempfile::tempdir().expect("provider owner home");
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
    let foreign_default = tempfile::tempdir().expect("foreign default workspace");
    provider_owner.config.write().await.default_work_area =
        Some(bamboo_config::DefaultWorkAreaConfig {
            path: Some(foreign_default.path().to_string_lossy().into_owned()),
        });

    let request_home = tempfile::tempdir().expect("request state home");
    let request_state = web::Data::new(
        AppState::new_with_memory_store(
            request_home.path().to_path_buf(),
            bamboo_memory::memory_store::MemoryStore::new(request_home.path().join("jiandu")),
        )
        .await
        .expect("request state"),
    );
    let request_root = bamboo_config::paths::resolve_workspace_root_in(request_home.path());
    let provider_owner_root =
        bamboo_config::paths::resolve_workspace_root_in(provider_owner_home.path());
    eprintln!(
        "workspace_provider_schedule: provider_owner_root={}, foreign_default={}, \
         request_state_root={}, expected_source=session_fallback",
        provider_owner_root.display(),
        foreign_default.path().display(),
        request_root.display(),
    );

    let app = test::init_service(
        App::new()
            .app_data(request_state.clone())
            .configure(configure_routes),
    )
    .await;
    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/sessions")
            .set_json(serde_json::json!({ "title": "isolated fallback" }))
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let body: Value = test::read_body_json(response).await;
    let session_id = body["session"]["id"].as_str().expect("session id");
    let resolved = body["session"]["workspace_path"]
        .as_str()
        .map(std::path::PathBuf::from)
        .expect("resolved fallback");
    let fallback = bamboo_agent_core::workspace_state::preview_default_session_workspace_dir(
        &request_root,
        session_id,
    );
    let expected = bamboo_agent_core::workspace_state::preview_pin_workspace_path(
        &fallback,
        &request_root,
        bamboo_config::paths::workspace_confinement_enforced(),
    );

    assert_eq!(
        resolved,
        expected,
        "workspace_provider_schedule: provider_owner_root={}, request_state_root={}, \
         resolved={}, source=session_fallback",
        provider_owner_root.display(),
        request_root.display(),
        resolved.display(),
    );
    assert!(
        resolved.is_dir(),
        "workspace_provider_schedule: resolved fallback was not materialized; \
         request_state_root={}, resolved={}, source=session_fallback",
        request_root.display(),
        resolved.display(),
    );
    assert_eq!(
        bamboo_agent_core::workspace_state::get_workspace(session_id).as_deref(),
        Some(resolved.as_path()),
        "workspace_provider_schedule: runtime publication drifted; \
         request_state_root={}, resolved={}, source=session_fallback",
        request_root.display(),
        resolved.display(),
    );
}
