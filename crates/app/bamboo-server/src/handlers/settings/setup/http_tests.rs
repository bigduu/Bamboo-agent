use std::{collections::BTreeMap, path::Path};

use actix_web::{http::StatusCode, test, web, App};
use bamboo_config::{ConfigFacade, SectionId};
use serde_json::{json, Value};

use crate::{
    app_state::{AppState, ConfigUpdateEffects},
    error::AppError,
};

use super::{get_setup_status, mark_setup_complete, mark_setup_incomplete, update_setup_config};

fn isolate_provider_env() -> Vec<bamboo_config::test_support::RuntimeEnvOverrideGuard> {
    [
        "BAMBOO_PROVIDER",
        "BAMBOO_OPENAI_API_KEY",
        "BAMBOO_ANTHROPIC_API_KEY",
        "BAMBOO_GEMINI_API_KEY",
    ]
    .into_iter()
    .map(|name| bamboo_config::test_support::override_runtime_env_var(name, None))
    .collect()
}

async fn isolated_state(path: &Path) -> AppState {
    let mut state = AppState::new(path.to_path_buf()).await.unwrap();
    state.stop_config_watcher_for_test();
    state
}

fn other_section_bytes(path: &Path) -> BTreeMap<&'static str, Option<Vec<u8>>> {
    bamboo_config::SECTION_DESCRIPTORS
        .iter()
        .filter(|section| section.id != SectionId::Core)
        .map(|section| {
            (
                section.file_name,
                std::fs::read(path.join(section.file_name)).ok(),
            )
        })
        .collect()
}

fn core(path: &Path) -> Value {
    serde_json::from_slice(&std::fs::read(path.join("core.json")).unwrap()).unwrap()
}

async fn complete_and_reset_through_http(state: web::Data<AppState>) {
    let app = test::init_service(
        App::new()
            .app_data(state)
            .route(
                "/api/v1/bamboo/setup/complete",
                web::post().to(mark_setup_complete),
            )
            .route(
                "/api/v1/bamboo/setup/incomplete",
                web::post().to(mark_setup_incomplete),
            )
            .route(
                "/api/v1/bamboo/setup/status",
                web::get().to(get_setup_status),
            ),
    )
    .await;
    for (uri, complete) in [
        ("/api/v1/bamboo/setup/complete", true),
        ("/api/v1/bamboo/setup/incomplete", false),
    ] {
        let response =
            test::call_service(&app, test::TestRequest::post().uri(uri).to_request()).await;
        assert_eq!(response.status(), StatusCode::OK, "{uri}");
        let result: Value = test::read_body_json(response).await;
        assert_eq!(result, json!({"success": true}));
        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/bamboo/setup/status")
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let status: Value = test::read_body_json(response).await;
        assert_eq!(status["is_complete"], complete);
    }
}

#[actix_web::test]
async fn fresh_install_setup_round_trip_changes_only_core_and_survives_cold_read() {
    let _env = isolate_provider_env();
    let _cache = bamboo_config::test_support::isolate_env_vars_cache();
    let dir = tempfile::tempdir().unwrap();
    let state = web::Data::new(isolated_state(dir.path()).await);
    let before_core = core(dir.path());
    let before_others = other_section_bytes(dir.path());

    complete_and_reset_through_http(state.clone()).await;

    let after = core(dir.path());
    assert_eq!(
        after["revision"],
        before_core["revision"].as_u64().unwrap() + 2
    );
    assert_eq!(after["data"]["setup"]["completed"], false);
    assert!(after["data"]["setup"]["completed_at"].is_string());
    assert!(after["data"]["setup"]["reset_at"].is_string());
    assert_eq!(other_section_bytes(dir.path()), before_others);
    let cold = ConfigFacade::open_or_migrate(dir.path())
        .unwrap()
        .effective_config();
    assert_eq!(cold.extra["setup"]["completed"], false);
}

#[actix_web::test]
async fn legacy_provider_first_boot_canonicalizes_before_setup_without_extra_writes() {
    let _env = isolate_provider_env();
    let _cache = bamboo_config::test_support::isolate_env_vars_cache();
    let _key = bamboo_config::encryption::set_test_encryption_key([0x61; 32]);
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("config.json"),
        serde_json::to_vec(&json!({"provider": "anthropic"})).unwrap(),
    )
    .unwrap();
    std::fs::write(
        dir.path().join("providers.json"),
        serde_json::to_vec(&json!({"anthropic": {
            "api_key_encrypted": bamboo_config::encryption::encrypt("setup-synthetic-key").unwrap(),
            "base_url": "http://127.0.0.1:9/v1", "model": "claude-3-5-sonnet-20241022"
        }}))
        .unwrap(),
    )
    .unwrap();
    let state = web::Data::new(isolated_state(dir.path()).await);
    let provider = state
        .config_facade
        .as_ref()
        .unwrap()
        .registry()
        .envelope_value(SectionId::Providers)
        .unwrap();
    assert!(provider.data.get("provider").is_none());
    assert_eq!(provider.data["default_provider_instance"], "anthropic");
    let before_others = other_section_bytes(dir.path());

    complete_and_reset_through_http(state).await;

    assert_eq!(other_section_bytes(dir.path()), before_others);
    assert_eq!(
        ConfigFacade::open_or_migrate(dir.path())
            .unwrap()
            .effective_config()
            .extra["setup"]["completed"],
        false
    );
}

#[actix_web::test]
async fn hybrid_provider_setup_uses_core_cas_without_weakening_multi_section_guard() {
    let _env = isolate_provider_env();
    let _cache = bamboo_config::test_support::isolate_env_vars_cache();
    let dir = tempfile::tempdir().unwrap();
    let facade = ConfigFacade::open_or_migrate(dir.path()).unwrap();
    let provider = facade
        .registry()
        .envelope_value(SectionId::Providers)
        .unwrap();
    let mut data = provider.data;
    data["provider"] = json!("anthropic");
    data["default_provider_instance"] = json!("anthropic");
    data["provider_instances"] = json!({"anthropic": {
        "provider_type": "anthropic", "enabled": true, "api_key_from_env": true,
        "model": "claude-3-5-sonnet-20241022", "base_url": "http://127.0.0.1:9/v1"
    }});
    // Seed the observed durable generation before boot through the typed store.
    // The ordinary runtime PUT API intentionally rejects provider writes.
    let providers: bamboo_config::ProvidersSection = serde_json::from_value(data).unwrap();
    facade
        .registry()
        .providers
        .commit(provider.revision, providers)
        .unwrap();
    drop(facade);
    let state = web::Data::new(isolated_state(dir.path()).await);
    let provider = state
        .config_facade
        .as_ref()
        .unwrap()
        .registry()
        .envelope_value(SectionId::Providers)
        .unwrap();
    assert_eq!(provider.data["provider"], "anthropic");
    assert_eq!(provider.data["default_provider_instance"], "anthropic");
    let before_core = std::fs::read(dir.path().join("core.json")).unwrap();
    let before_others = other_section_bytes(dir.path());

    let error = state
        .update_config(
            |config| super::updates::set_setup_complete(config, "2026-10-22T00:00:00Z".to_string()),
            ConfigUpdateEffects::default(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, AppError::BadRequest(ref message)
            if message.contains("multiple sections (core, providers)")),
        "{error}"
    );
    assert_eq!(
        std::fs::read(dir.path().join("core.json")).unwrap(),
        before_core
    );
    assert_eq!(other_section_bytes(dir.path()), before_others);

    complete_and_reset_through_http(state).await;

    assert_eq!(other_section_bytes(dir.path()), before_others);
    let cold = ConfigFacade::open_or_migrate(dir.path()).unwrap();
    assert_eq!(cold.effective_config().extra["setup"]["completed"], false);
    assert_eq!(
        cold.registry()
            .envelope_value(SectionId::Providers)
            .unwrap()
            .revision,
        provider.revision
    );
}

#[actix_web::test]
async fn setup_preserves_exact_core_winner_when_process_snapshot_is_stale() {
    let _env = isolate_provider_env();
    let _cache = bamboo_config::test_support::isolate_env_vars_cache();
    let dir = tempfile::tempdir().unwrap();
    let state = web::Data::new(isolated_state(dir.path()).await);
    let external = ConfigFacade::open(dir.path()).unwrap();
    let original = external.registry().envelope_value(SectionId::Core).unwrap();
    let mut winner = original.data;
    winner["external_marker"] = json!({"keep": true});
    winner["setup"] = json!({"custom": "preserved"});
    external
        .registry()
        .commit_value(SectionId::Core, original.revision, winner)
        .unwrap();
    assert!(!state
        .config
        .read()
        .await
        .extra
        .contains_key("external_marker"));
    let before_others = other_section_bytes(dir.path());

    mark_setup_complete(state.clone()).await.unwrap();

    let committed = core(dir.path());
    assert_eq!(committed["revision"], original.revision + 2);
    assert_eq!(committed["data"]["external_marker"], json!({"keep": true}));
    assert_eq!(committed["data"]["setup"]["custom"], "preserved");
    assert_eq!(committed["data"]["setup"]["completed"], true);
    assert_eq!(
        state.config.read().await.extra["external_marker"],
        json!({"keep": true})
    );
    assert_eq!(other_section_bytes(dir.path()), before_others);
}

#[actix_web::test]
async fn setup_core_revision_race_returns_conflict_without_overwriting_winner() {
    let _env = isolate_provider_env();
    let _cache = bamboo_config::test_support::isolate_env_vars_cache();
    let dir = tempfile::tempdir().unwrap();
    let state = isolated_state(dir.path()).await;
    let external = ConfigFacade::open(dir.path()).unwrap();
    let original = external.registry().envelope_value(SectionId::Core).unwrap();
    let mut winner = original.data;
    winner["external_marker"] = json!("wins");
    let before_others = other_section_bytes(dir.path());
    let mut winner_bytes = Vec::new();

    let error = update_setup_config(&state, |config| {
        external
            .registry()
            .commit_value(SectionId::Core, original.revision, winner)
            .unwrap();
        winner_bytes = std::fs::read(dir.path().join("core.json")).unwrap();
        super::updates::set_setup_complete(config, "2026-10-22T00:00:00Z".to_string())
    })
    .await
    .unwrap_err();

    assert!(
        matches!(error, AppError::ConfigConflict { expected, actual }
        if expected == original.revision && actual == original.revision + 1)
    );
    assert_eq!(
        std::fs::read(dir.path().join("core.json")).unwrap(),
        winner_bytes
    );
    assert!(!state.config.read().await.extra.contains_key("setup"));
    assert_eq!(other_section_bytes(dir.path()), before_others);
}

#[actix_web::test]
async fn invalid_setup_shape_still_returns_400_without_any_section_write() {
    let _env = isolate_provider_env();
    let _cache = bamboo_config::test_support::isolate_env_vars_cache();
    let dir = tempfile::tempdir().unwrap();
    let state = web::Data::new(isolated_state(dir.path()).await);
    let external = ConfigFacade::open(dir.path()).unwrap();
    let original = external.registry().envelope_value(SectionId::Core).unwrap();
    let mut invalid = original.data;
    invalid["setup"] = json!("invalid");
    external
        .registry()
        .commit_value(SectionId::Core, original.revision, invalid)
        .unwrap();
    let before_core = std::fs::read(dir.path().join("core.json")).unwrap();
    let before_others = other_section_bytes(dir.path());
    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/bamboo/setup/complete",
        web::post().to(mark_setup_complete),
    ))
    .await;

    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/bamboo/setup/complete")
            .to_request(),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        std::fs::read(dir.path().join("core.json")).unwrap(),
        before_core
    );
    assert_eq!(other_section_bytes(dir.path()), before_others);
}
