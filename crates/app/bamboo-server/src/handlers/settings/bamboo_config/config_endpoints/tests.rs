use std::collections::BTreeMap;

use tempfile::tempdir;

use bamboo_config::OpenAIConfig;
use bamboo_llm::Config;

use super::common::{
    config_file_path, model_limits_file_path, read_model_limits_file, redacted_config_json,
    write_model_limits_file,
};

fn access_control_fixture() -> bamboo_config::AccessControlConfig {
    bamboo_config::AccessControlConfig {
        password_enabled: true,
        repair_required: false,
        password_hash: Some("a".repeat(64)),
        password_salt: Some("00112233445566778899aabbccddeeff".to_string()),
        password_credential_ref: None,
        password_configured: false,
        updated_at: Some("2026-07-21T00:00:00Z".to_string()),
        devices: vec![
            bamboo_config::DeviceCredential {
                device_id: "bamboo_first".to_string(),
                label: "Phone".to_string(),
                token_hash: "b".repeat(64),
                token_salt: "11223344556677889900aabbccddeeff".to_string(),
                token_credential_ref: None,
                token_configured: false,
                created_at: "2026-07-20T00:00:00Z".to_string(),
                last_used_at: Some("2026-07-21T00:00:00Z".to_string()),
                revoked: false,
            },
            bamboo_config::DeviceCredential {
                device_id: "bamboo_second".to_string(),
                label: "Laptop".to_string(),
                token_hash: "c".repeat(64),
                token_salt: "22334455667788990011aabbccddeeff".to_string(),
                token_credential_ref: None,
                token_configured: false,
                created_at: "2026-07-19T00:00:00Z".to_string(),
                last_used_at: None,
                revoked: true,
            },
        ],
    }
}

#[test]
fn access_control_echo_is_checked_against_lock_time_config() {
    let mut original = Config::default();
    original.access_control = Some(access_control_fixture());
    let redacted = crate::handlers::settings::redaction::redact_config_for_api(
        original
            .to_compatibility_value()
            .expect("config should serialize"),
        &original,
    );
    let stale_echo = redacted["access_control"].clone();

    let mut exact_patch = serde_json::json!({
        "access_control": stale_echo.clone(),
        "model": "safe-update"
    });
    super::set::remove_unchanged_access_control_echo(
        &original,
        exact_patch.as_object_mut().unwrap(),
    )
    .expect("an exact lock-time echo should be ignored");
    assert!(exact_patch.get("access_control").is_none());
    assert_eq!(exact_patch["model"], "safe-update");

    let mut lock_time_current = original;
    lock_time_current.access_control.as_mut().unwrap().devices[0].revoked = true;
    let mut stale_patch = serde_json::json!({
        "access_control": stale_echo,
        "model": "must-not-be-applied"
    });
    assert!(matches!(
        super::set::remove_unchanged_access_control_echo(
            &lock_time_current,
            stale_patch.as_object_mut().unwrap(),
        ),
        Err(crate::error::AppError::BadRequest(_))
    ));
    assert!(stale_patch.get("access_control").is_some());
    assert_eq!(stale_patch["model"], "must-not-be-applied");
}

#[test]
fn config_file_path_appends_config_json_filename() {
    let dir = tempdir().expect("temp dir should be created");
    assert_eq!(config_file_path(dir.path()), dir.path().join("config.json"));
}

#[test]
fn model_limits_file_path_appends_model_limits_json_filename() {
    let dir = tempdir().expect("temp dir should be created");
    assert_eq!(
        model_limits_file_path(dir.path()),
        dir.path().join("model_limits.json")
    );
}

#[actix_web::test]
async fn redacted_config_json_masks_provider_api_key_and_hides_encrypted_proxy_auth() {
    let mut config = Config::default();
    config.providers_mut().openai = Some(OpenAIConfig {
        api_key: "sk-secret".to_string(),
        api_key_from_env: false,
        api_key_encrypted: None,
        credential_ref: None,
        base_url: None,
        model: None,
        fast_model: None,
        vision_model: None,
        reasoning_effort: None,
        responses_only_models: vec![],
        request_overrides: None,
        extra: BTreeMap::new(),
    });
    config.proxy_auth_encrypted = Some("enc:deadbeef".to_string());

    let dir = tempdir().expect("temp dir should be created");
    let value = redacted_config_json(&config, dir.path())
        .await
        .expect("redacted config should serialize");
    assert_eq!(value["providers"]["openai"]["api_key"], "****...****");
    assert!(value.get("proxy_auth_encrypted").is_none());
}

#[actix_web::test]
async fn compatibility_config_get_hides_all_access_control_verifiers() {
    use crate::app_state::AppState;
    use actix_web::{test, web, App};

    let temp_dir = tempdir().expect("temp dir should be created");
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");
    let app_state = web::Data::new(state);
    {
        let mut config = app_state.config.write().await;
        config.access_control = Some(access_control_fixture());
    }
    // The compatibility GET intentionally returns an empty object before the
    // first config file exists. Create that marker without persisting the
    // verifier-bearing in-memory fixture through an unrelated save path.
    tokio::fs::write(temp_dir.path().join("config.json"), "{}")
        .await
        .expect("config marker should be written");

    let app = test::init_service(
        App::new()
            .app_data(app_state)
            .route("/bamboo/config", web::get().to(super::get_bamboo_config)),
    )
    .await;
    let response: serde_json::Value = test::call_and_read_body_json(
        &app,
        test::TestRequest::get().uri("/bamboo/config").to_request(),
    )
    .await;

    let access = response["access_control"]
        .as_object()
        .expect("safe access-control metadata should remain visible");
    assert!(!access.contains_key("password_hash"));
    assert!(!access.contains_key("password_salt"));
    assert_eq!(access["password_enabled"], true);
    let devices = access["devices"]
        .as_array()
        .expect("safe device metadata should remain visible");
    assert_eq!(devices.len(), 2);
    for device in devices {
        let device = device.as_object().expect("device should be an object");
        assert!(!device.contains_key("token_hash"));
        assert!(!device.contains_key("token_salt"));
    }
    assert_eq!(devices[0]["device_id"], "bamboo_first");
    assert_eq!(devices[0]["label"], "Phone");
    assert_eq!(devices[1]["device_id"], "bamboo_second");
    assert_eq!(devices[1]["revoked"], true);
}

#[actix_web::test]
async fn redacted_full_payload_update_preserves_access_control_and_rejects_mutation() {
    use crate::app_state::AppState;
    use actix_web::{http::StatusCode, test, web, App};

    let temp_dir = tempdir().expect("temp dir should be created");
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");
    let data_dir = state.app_data_dir.clone();
    let app_state = web::Data::new(state);
    let fixture = access_control_fixture();
    let device_intents = fixture
        .devices
        .iter()
        .map(|device| device.device_id.clone())
        .collect();
    let expected_revision = app_state
        .config_facade
        .as_ref()
        .unwrap()
        .registry()
        .access_control
        .snapshot()
        .revision;
    let (committed, _, _, _) = app_state
        .update_access_control_credentials(expected_revision, true, device_intents, move |config| {
            config.access_control = Some(fixture);
            Ok(())
        })
        .await
        .expect("access-control fixture should persist through the exact transaction");
    let expected_access_control = committed.access_control.clone().unwrap();
    let access_path = data_dir.join("access-control.json");
    let persisted_before = std::fs::read(&access_path).expect("section should be readable");

    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .route("/bamboo/config", web::get().to(super::get_bamboo_config))
            .route("/bamboo/config", web::post().to(super::set_bamboo_config)),
    )
    .await;

    let mut full_payload: serde_json::Value = test::call_and_read_body_json(
        &app,
        test::TestRequest::get().uri("/bamboo/config").to_request(),
    )
    .await;
    assert!(full_payload["access_control"]
        .get("password_hash")
        .is_none());
    assert!(full_payload["access_control"]["devices"][0]
        .get("token_hash")
        .is_none());
    full_payload["model"] = serde_json::json!("unrelated-model-update");

    let update = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/bamboo/config")
            .set_json(full_payload)
            .to_request(),
    )
    .await;
    let status = update.status();
    let body = String::from_utf8(test::read_body(update).await.to_vec()).unwrap();
    assert!(
        status.is_success(),
        "full update failed with {status}: {body}"
    );
    assert_eq!(
        app_state
            .config
            .read()
            .await
            .to_compatibility_value()
            .expect("updated config should serialize")["model"],
        serde_json::json!("unrelated-model-update")
    );
    assert_eq!(
        app_state.config.read().await.access_control.as_ref(),
        Some(&expected_access_control)
    );
    assert_eq!(
        std::fs::read(access_path).unwrap(),
        persisted_before,
        "unrelated full-payload update must preserve every access-control verifier"
    );

    let baseline_files = transaction_file_snapshot(&data_dir);
    let baseline_live = app_state
        .config
        .read()
        .await
        .to_compatibility_value()
        .expect("live config should serialize");
    let mut malicious: serde_json::Value = test::call_and_read_body_json(
        &app,
        test::TestRequest::get().uri("/bamboo/config").to_request(),
    )
    .await;
    malicious["model"] = serde_json::json!("must-not-be-applied");
    malicious["access_control"]["devices"][0]["revoked"] = serde_json::json!(true);
    let rejected = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/bamboo/config")
            .set_json(malicious)
            .to_request(),
    )
    .await;
    assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
    assert_eq!(transaction_file_snapshot(&data_dir), baseline_files);
    assert_eq!(
        app_state
            .config
            .read()
            .await
            .to_compatibility_value()
            .expect("live config should serialize"),
        baseline_live,
        "changed access-control metadata and unrelated fields must both be rejected"
    );
}

#[actix_web::test]
async fn generic_config_patch_rejects_access_control_without_partial_mutation() {
    use crate::app_state::AppState;
    use actix_web::{http::StatusCode, test, web, App};

    let temp_dir = tempdir().expect("temp dir should be created");
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");
    let data_dir = state.app_data_dir.clone();
    let app_state = web::Data::new(state);
    let baseline_files = transaction_file_snapshot(&data_dir);
    let baseline_live = app_state
        .config
        .read()
        .await
        .to_compatibility_value()
        .expect("baseline config should serialize");
    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .route("/bamboo/config", web::post().to(super::set_bamboo_config)),
    )
    .await;

    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/bamboo/config")
            .set_json(serde_json::json!({
                "model": "must-not-be-applied",
                "access_control": {
                    "password_enabled": true,
                    "password_hash": "attacker-password-verifier",
                    "password_salt": "attacker-password-salt",
                    "devices": [
                        {
                            "device_id": "bamboo_attacker",
                            "label": "Injected device",
                            "token_hash": "attacker-device-verifier",
                            "token_salt": "attacker-device-salt",
                            "created_at": "2026-07-21T00:00:00Z",
                            "revoked": false
                        }
                    ]
                }
            }))
            .to_request(),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(transaction_file_snapshot(&data_dir), baseline_files);
    assert_eq!(
        app_state
            .config
            .read()
            .await
            .to_compatibility_value()
            .expect("live config should serialize"),
        baseline_live,
        "neither access_control nor the unrelated model field may be partially applied"
    );
}

#[actix_web::test]
async fn redacted_config_json_injects_model_limits_from_file() {
    let dir = tempdir().expect("temp dir should be created");
    write_model_limits_file(
        dir.path(),
        Some(&serde_json::json!([
            {
                "model_pattern": "gpt-5",
                "max_context_tokens": 400000,
                "max_output_tokens": 128000,
                "safety_margin": 1000
            }
        ])),
    )
    .await
    .expect("model limits file should be written");

    let value = redacted_config_json(&Config::default(), dir.path())
        .await
        .expect("redacted config should serialize");
    assert_eq!(value["model_limits"][0]["model_pattern"], "gpt-5");
}

// --- Full persistence (all user-provided overrides are stored) ------------

/// All user-provided rows must be persisted, including rows that happen to
/// match the global default.
#[actix_web::test]
async fn write_model_limits_file_persists_all_rows_round_trip() {
    let dir = tempdir().expect("temp dir should be created");

    write_model_limits_file(
        dir.path(),
        Some(&serde_json::json!([
            { "model_pattern": "gpt-4o", "max_context_tokens": 128000, "max_output_tokens": 16384 },
            { "model_pattern": "some-default-model", "max_context_tokens": 1000000, "max_output_tokens": 64000 }
        ])),
    )
    .await
    .expect("model limits file should be written");

    let persisted = read_model_limits_file(dir.path())
        .await
        .expect("read should succeed")
        .expect("file should exist");

    let rows = persisted.as_array().expect("array");
    assert_eq!(rows.len(), 2, "all rows must be persisted");
    assert_eq!(rows[0]["model_pattern"], "gpt-4o");
    assert_eq!(rows[1]["model_pattern"], "some-default-model");
}

/// Saving an empty overrides array must remove the file entirely so
/// everything falls back to the global default.
#[actix_web::test]
async fn write_model_limits_file_removes_file_when_empty() {
    let dir = tempdir().expect("temp dir should be created");
    let path = model_limits_file_path(dir.path());

    // Pre-existing override on disk.
    tokio::fs::write(
        &path,
        r#"[{"model_pattern":"gpt-4o","max_context_tokens":128000,"max_output_tokens":16384}]"#,
    )
    .await
    .expect("seed file");

    // User clears all overrides → empty array.
    write_model_limits_file(dir.path(), Some(&serde_json::json!([])))
        .await
        .expect("write should succeed");

    assert!(
        !path.exists(),
        "file should be removed when overrides list is empty"
    );
    assert!(read_model_limits_file(dir.path())
        .await
        .expect("read should succeed")
        .is_none());
}

/// An explicit safety margin must be persisted alongside the override.
#[actix_web::test]
async fn write_model_limits_file_keeps_custom_safety_margin() {
    let dir = tempdir().expect("temp dir should be created");

    write_model_limits_file(
        dir.path(),
        Some(&serde_json::json!([
            { "model_pattern": "gpt-4o", "max_context_tokens": 200000, "max_output_tokens": 64000, "safety_margin": 500 }
        ])),
    )
    .await
    .expect("write should succeed");

    let persisted = read_model_limits_file(dir.path())
        .await
        .expect("read should succeed")
        .expect("file should exist");
    let rows = persisted.as_array().expect("array");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["safety_margin"], 500);
}

/// End-to-end over the real HTTP handlers + `AppState`: POST a config carrying
/// `model_limits` (two overrides), then GET it back. Asserts:
/// - ALL overrides are persisted (including default-equal rows),
/// - `config.json` does NOT carry the `model_limits` key (it is split out),
/// - GET re-injects `model_limits` from the dedicated file.
#[actix_web::test]
async fn set_then_get_bamboo_config_round_trips_all_overrides() {
    use crate::app_state::AppState;
    use actix_web::{test, web, App};

    let temp_dir = tempdir().expect("temp dir should be created");
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");
    let data_dir = state.app_data_dir.clone();
    let app_state = web::Data::new(state);

    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .route("/bamboo/config", web::get().to(super::get_bamboo_config))
            .route("/bamboo/config", web::post().to(super::set_bamboo_config)),
    )
    .await;

    let config_post = test::TestRequest::post()
        .uri("/bamboo/config")
        .set_json(serde_json::json!({
            "model": "gpt-4o"
        }))
        .to_request();
    let config_post_resp = test::call_service(&app, config_post).await;
    assert!(
        config_post_resp.status().is_success(),
        "set config should succeed"
    );

    let limits_post = test::TestRequest::post()
        .uri("/bamboo/config")
        .set_json(serde_json::json!({
            "model_limits": [
                { "model_pattern": "gpt-4o", "max_context_tokens": 128000, "max_output_tokens": 16384 },
                { "model_pattern": "noop", "max_context_tokens": 200000, "max_output_tokens": 64000 }
            ]
        }))
        .to_request();
    let limits_post_resp = test::call_service(&app, limits_post).await;
    assert!(
        limits_post_resp.status().is_success(),
        "set model limits should succeed"
    );

    // All overrides survive on disk (no diff-only filtering).
    let on_disk = read_model_limits_file(&data_dir)
        .await
        .expect("read should succeed")
        .expect("model_limits.json should exist");
    let rows = on_disk.as_array().expect("array");
    assert_eq!(rows.len(), 2, "all rows must be persisted");
    assert_eq!(rows[0]["model_pattern"], "gpt-4o");
    assert_eq!(rows[1]["model_pattern"], "noop");

    // The active modular layout never recreates config.json, and the core
    // section must not carry the model_limits key.
    assert!(!config_file_path(&data_dir).exists());
    let config_text = tokio::fs::read_to_string(data_dir.join("core.json"))
        .await
        .expect("core.json should exist after set");
    let config_json: serde_json::Value =
        serde_json::from_str(&config_text).expect("config.json should be valid json");
    assert!(
        config_json.get("model_limits").is_none(),
        "model_limits must be split out of core.json"
    );

    // GET re-injects model_limits from the dedicated file.
    let get = test::TestRequest::get().uri("/bamboo/config").to_request();
    let body: serde_json::Value = test::call_and_read_body_json(&app, get).await;
    let limits = body["model_limits"]
        .as_array()
        .expect("model_limits should be present in GET response");
    assert_eq!(limits.len(), 2);
    assert_eq!(limits[0]["model_pattern"], "gpt-4o");
    assert_eq!(limits[0]["max_context_tokens"], 128000);
}

#[actix_web::test]
async fn omitted_model_limits_preserves_modular_section_data_and_revision() {
    use crate::app_state::AppState;
    use actix_web::{test, web, App};

    let temp_dir = tempdir().expect("temp dir should be created");
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");
    assert!(
        state.config_facade.is_some(),
        "AppState must use the modular section layout for this regression"
    );
    let data_dir = state.app_data_dir.clone();
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(state))
            .route("/bamboo/config", web::post().to(super::set_bamboo_config)),
    )
    .await;

    let seed = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/bamboo/config")
            .set_json(serde_json::json!({
                "model_limits": [{
                    "model_pattern": "preserved-model",
                    "max_context_tokens": 128000,
                    "max_output_tokens": 16000,
                    "safety_margin": 1000
                }]
            }))
            .to_request(),
    )
    .await;
    assert!(
        seed.status().is_success(),
        "seeding model limits should succeed"
    );

    let path = model_limits_file_path(&data_dir);
    let before: serde_json::Value = serde_json::from_slice(
        &tokio::fs::read(&path)
            .await
            .expect("model-limits section should exist"),
    )
    .expect("model-limits section should be valid JSON");
    assert_eq!(before["data"][0]["model_pattern"], "preserved-model");
    let before_revision = before["revision"]
        .as_u64()
        .expect("model-limits section should have a revision");

    let unrelated = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/bamboo/config")
            .set_json(serde_json::json!({"headless_auth": true}))
            .to_request(),
    )
    .await;
    assert!(
        unrelated.status().is_success(),
        "unrelated config patch should succeed"
    );

    let after: serde_json::Value = serde_json::from_slice(
        &tokio::fs::read(&path)
            .await
            .expect("model-limits section should remain present"),
    )
    .expect("model-limits section should remain valid JSON");
    assert_eq!(
        after["data"], before["data"],
        "omitting model_limits must preserve its data"
    );
    assert_eq!(
        after["revision"].as_u64(),
        Some(before_revision),
        "omitting model_limits must not create a new section revision"
    );
}

#[actix_web::test]
async fn explicit_empty_model_limits_clears_modular_section() {
    use crate::app_state::AppState;
    use actix_web::{test, web, App};

    let temp_dir = tempdir().expect("temp dir should be created");
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");
    let data_dir = state.app_data_dir.clone();
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(state))
            .route("/bamboo/config", web::post().to(super::set_bamboo_config)),
    )
    .await;

    for model_limits in [
        serde_json::json!([{
            "model_pattern": "cleared-model",
            "max_context_tokens": 64000,
            "max_output_tokens": 8000
        }]),
        serde_json::json!([]),
    ] {
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/bamboo/config")
                .set_json(serde_json::json!({"model_limits": model_limits}))
                .to_request(),
        )
        .await;
        assert!(response.status().is_success());
    }

    let section: serde_json::Value = serde_json::from_slice(
        &tokio::fs::read(model_limits_file_path(&data_dir))
            .await
            .expect("empty modular section should remain present"),
    )
    .expect("model-limits section should remain valid JSON");
    assert_eq!(section["data"], serde_json::json!([]));
    assert_eq!(
        section["revision"].as_u64(),
        Some(2),
        "seed and explicit clear must each commit exactly one revision"
    );
}

fn transaction_file_snapshot(data_dir: &std::path::Path) -> BTreeMap<String, Option<Vec<u8>>> {
    [
        "credentials.json",
        "providers.json",
        "config.json",
        "memory.json",
        "subagents.json",
        "connect.json",
        "model_limits.json",
    ]
    .into_iter()
    .map(|name| (name.to_string(), std::fs::read(data_dir.join(name)).ok()))
    .collect()
}

#[actix_web::test]
async fn provider_credential_patch_rejects_changed_sidecars_and_model_limits_without_mutation() {
    use crate::app_state::AppState;
    use actix_web::{http::StatusCode, test, web, App};

    let temp_dir = tempdir().expect("temp dir should be created");
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");
    let data_dir = state.app_data_dir.clone();
    let app_state = web::Data::new(state);
    let baseline_files = transaction_file_snapshot(&data_dir);
    let baseline_live = app_state
        .config
        .read()
        .await
        .to_compatibility_value()
        .unwrap();
    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .route("/bamboo/config", web::post().to(super::set_bamboo_config)),
    )
    .await;

    for (section, extra) in [
        (
            "core",
            serde_json::json!({"http_proxy": "http://proxy.invalid:8080"}),
        ),
        (
            "memory",
            serde_json::json!({"memory": {"background_model": "memory-model"}}),
        ),
        (
            "subagents",
            serde_json::json!({"subagents": {"max_concurrent": 3}}),
        ),
        (
            "connect",
            serde_json::json!({"connect": {"platforms": [{
                "type": "telegram", "token": "tg-mixed-secret", "allow_from": ["u1"]
            }]}}),
        ),
        (
            "model_limits",
            serde_json::json!({"model_limits": [{
                "model_pattern": "gpt-*", "max_context_tokens": 1000,
                "max_output_tokens": 100
            }]}),
        ),
    ] {
        let mut payload = serde_json::json!({
            "provider": "openai",
            "providers": {"openai": {"api_key": "sk-mixed-secret", "model": "gpt-test"}}
        });
        payload
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let request = test::TestRequest::post()
            .uri("/bamboo/config")
            .set_json(payload)
            .to_request();
        let response = test::call_service(&app, request).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{section}");
        assert_eq!(
            transaction_file_snapshot(&data_dir),
            baseline_files,
            "{section}"
        );
        assert_eq!(
            app_state
                .config
                .read()
                .await
                .to_compatibility_value()
                .unwrap(),
            baseline_live,
            "{section}"
        );
    }
}

#[actix_web::test]
async fn provider_instance_credential_patch_rejects_model_limits_without_partial_commit() {
    use crate::app_state::AppState;
    use actix_web::{http::StatusCode, test, web, App};

    let temp_dir = tempdir().expect("temp dir should be created");
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");
    let data_dir = state.app_data_dir.clone();
    let app_state = web::Data::new(state);
    let baseline_files = transaction_file_snapshot(&data_dir);
    let baseline_live = app_state
        .config
        .read()
        .await
        .to_compatibility_value()
        .unwrap();
    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .route("/bamboo/config", web::post().to(super::set_bamboo_config)),
    )
    .await;

    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/bamboo/config")
            .set_json(serde_json::json!({
                "provider_instances": {
                    "work": {"provider_type": "openai", "api_key": "sk-instance-mixed"}
                },
                "model_limits": [{
                    "model_pattern": "gpt-*", "max_context_tokens": 1000,
                    "max_output_tokens": 100
                }]
            }))
            .to_request(),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(transaction_file_snapshot(&data_dir), baseline_files);
    assert_eq!(
        app_state
            .config
            .read()
            .await
            .to_compatibility_value()
            .unwrap(),
        baseline_live
    );
}

#[actix_web::test]
async fn whole_provider_instance_null_clears_credential_and_metadata_transactionally() {
    use crate::app_state::AppState;
    use actix_web::{http::StatusCode, test, web, App};

    let temp_dir = tempdir().expect("temp dir should be created");
    let mut config = Config::default();
    let instance: bamboo_config::ProviderInstanceConfig = serde_json::from_value(
        serde_json::json!({"provider_type": "openai", "api_key": "sk-delete-root"}),
    )
    .unwrap();
    config
        .provider_instances
        .insert("work".to_string(), instance);
    bamboo_config::persist_provider_instance_credential_transaction(
        temp_dir.path(),
        &mut config,
        &std::collections::BTreeSet::new(),
        &std::collections::BTreeSet::from(["work".to_string()]),
    )
    .unwrap();
    let reference = config.provider_instances["work"]
        .credential_ref
        .clone()
        .unwrap();

    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");
    let legacy_root_before = std::fs::read(temp_dir.path().join("config.json")).ok();
    let app_state = web::Data::new(state);
    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .route("/bamboo/config", web::post().to(super::set_bamboo_config)),
    )
    .await;
    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/bamboo/config")
            .set_json(serde_json::json!({"provider_instances": {"work": null}}))
            .to_request(),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert!(!app_state
        .config
        .read()
        .await
        .provider_instances
        .contains_key("work"));
    assert!(bamboo_config::CredentialStore::open(temp_dir.path())
        .resolve(&reference)
        .unwrap()
        .is_none());
    let providers: serde_json::Value =
        serde_json::from_slice(&std::fs::read(temp_dir.path().join("providers.json")).unwrap())
            .unwrap();
    assert!(providers["data"]["provider_instances"]
        .get("work")
        .is_none());
    assert_eq!(
        std::fs::read(temp_dir.path().join("config.json")).ok(),
        legacy_root_before,
        "active provider credential transaction must not rewrite the legacy root"
    );
}

#[actix_web::test]
async fn provider_credential_full_payload_allows_unchanged_sidecars() {
    use crate::app_state::AppState;
    use actix_web::{test, web, App};

    let temp_dir = tempdir().expect("temp dir should be created");
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");
    let data_dir = state.app_data_dir.clone();
    let app_state = web::Data::new(state);
    let mut payload = app_state
        .config
        .read()
        .await
        .to_compatibility_value()
        .unwrap();
    let object = payload.as_object_mut().unwrap();
    object.insert("provider".to_string(), serde_json::json!("openai"));
    object.insert(
        "providers".to_string(),
        serde_json::json!({"openai": {
            "api_key": "sk-full-payload", "model": "gpt-test"
        }}),
    );

    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .route("/bamboo/config", web::post().to(super::set_bamboo_config)),
    )
    .await;
    let request = test::TestRequest::post()
        .uri("/bamboo/config")
        .set_json(payload)
        .to_request();
    let response = test::call_service(&app, request).await;
    let status = response.status();
    let response_body = String::from_utf8(test::read_body(response).await.to_vec()).unwrap();
    assert!(
        status.is_success(),
        "provider update failed with {status}: {response_body}"
    );
    let reference = bamboo_config::credential_ref("provider", "openai", "api_key").unwrap();
    assert_eq!(
        app_state
            .credential_store
            .resolve(&reference)
            .unwrap()
            .unwrap()
            .expose(),
        "sk-full-payload"
    );
    assert!(data_dir.join("credentials.json").exists());
    assert!(!data_dir.join("config.json").exists());
    let providers: serde_json::Value = serde_json::from_slice(
        &std::fs::read(data_dir.join("providers.json")).expect("providers section should exist"),
    )
    .unwrap();
    assert_eq!(providers["data"]["provider"], "openai");
    assert_eq!(
        providers["data"]["openai"]["credential_ref"],
        reference.as_str()
    );
    assert_eq!(providers["data"]["openai"]["model"], "gpt-test");
    let facade = app_state.config_facade.as_ref().unwrap();
    assert_eq!(
        facade.registry().providers.snapshot().revision,
        providers["revision"].as_u64().unwrap(),
        "API completion must publish the process-owned facade snapshot"
    );
}

/// #573: the Codex custom-provider field is a stable credential reference,
/// never a second copy of the provider secret. A subagents-only settings patch
/// may round-trip that reference and update unrelated Codex fields without
/// replacing, clearing, or exposing the provider-instance key.
#[actix_web::test]
async fn codex_provider_reference_round_trip_preserves_masked_provider_secret() {
    use crate::app_state::AppState;
    use actix_web::{test, web, App};

    let temp_dir = tempdir().expect("temp dir should be created");
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");
    let app_state = web::Data::new(state);
    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .route("/bamboo/config", web::get().to(super::get_bamboo_config))
            .route("/bamboo/config", web::post().to(super::set_bamboo_config)),
    )
    .await;

    let provider = test::TestRequest::post()
        .uri("/bamboo/config")
        .set_json(serde_json::json!({
            "provider_instances": {
                "codex-work": {
                    "provider_type": "openai",
                    "enabled": true,
                    "api_key": "sk-codex-provider-secret"
                }
            }
        }))
        .to_request();
    assert!(test::call_service(&app, provider)
        .await
        .status()
        .is_success());

    let reference = app_state.config.read().await.provider_instances["codex-work"]
        .credential_ref
        .clone()
        .expect("provider instance has a stable credential reference");
    let codex = test::TestRequest::post()
        .uri("/bamboo/config")
        .set_json(serde_json::json!({
            "subagents": {
                "executor": "codex",
                "codex_auth_mode": "custom",
                "codex_base_url": "https://provider.example/v1",
                "codex_wire_api": "responses",
                "codex_provider_key_ref": reference.as_str(),
                "codex_sandbox": "workspace-write",
                "codex_approval_policy": "never"
            }
        }))
        .to_request();
    assert!(test::call_service(&app, codex).await.status().is_success());

    let body: serde_json::Value = test::call_and_read_body_json(
        &app,
        test::TestRequest::get().uri("/bamboo/config").to_request(),
    )
    .await;
    assert_eq!(
        body["provider_instances"]["codex-work"]["api_key"],
        "****...****"
    );
    assert_eq!(
        body["subagents"]["codex_provider_key_ref"],
        reference.as_str(),
        "the non-secret reference remains visible and editable"
    );

    let update = test::TestRequest::post()
        .uri("/bamboo/config")
        .set_json(serde_json::json!({
            "subagents": {"codex_model": "gpt-5.4-mini"}
        }))
        .to_request();
    assert!(test::call_service(&app, update).await.status().is_success());

    assert_eq!(
        app_state
            .credential_store
            .resolve(&reference)
            .unwrap()
            .unwrap()
            .expose(),
        "sk-codex-provider-secret"
    );
    let subagents = std::fs::read_to_string(temp_dir.path().join("subagents.json")).unwrap();
    assert!(subagents.contains(reference.as_str()));
    assert!(!subagents.contains("sk-codex-provider-secret"));
    assert!(!subagents.contains("****...****"));
}

#[actix_web::test]
async fn secret_omitting_full_payload_provider_update_preserves_notification_credential() {
    use crate::app_state::AppState;
    use actix_web::{test, web, App};

    let temp_dir = tempdir().expect("temp dir should be created");
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");
    let app_state = web::Data::new(state);
    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .route("/bamboo/config", web::get().to(super::get_bamboo_config))
            .route("/bamboo/config", web::post().to(super::set_bamboo_config)),
    )
    .await;

    let set_notification = test::TestRequest::post()
        .uri("/bamboo/config")
        .set_json(serde_json::json!({
            "expected_revision": 0,
            "notifications": {
                "ntfy": {
                    "enabled": true,
                    "topic": "full-payload-alerts",
                    "token": "full-payload-notification-secret"
                }
            }
        }))
        .to_request();
    assert!(test::call_service(&app, set_notification)
        .await
        .status()
        .is_success());

    let reference = bamboo_config::credential_ref("notification", "ntfy", "token").unwrap();
    let before = app_state.config.read().await.notifications.ntfy.clone();
    assert_eq!(before.credential_ref.as_ref(), Some(&reference));
    let mut full_payload: serde_json::Value = test::call_and_read_body_json(
        &app,
        test::TestRequest::get().uri("/bamboo/config").to_request(),
    )
    .await;
    assert_eq!(
        full_payload["notifications"]["ntfy"]["token"],
        "****...****"
    );
    full_payload["notifications"]["ntfy"]
        .as_object_mut()
        .unwrap()
        .remove("token");
    full_payload["provider"] = serde_json::json!("openai");
    full_payload["providers"] = serde_json::json!({
        "openai": {"api_key": "sk-provider-from-full-payload", "model": "gpt-test"}
    });

    let update = test::TestRequest::post()
        .uri("/bamboo/config")
        .set_json(full_payload)
        .to_request();
    let update = test::call_service(&app, update).await;
    let status = update.status();
    let body = String::from_utf8(test::read_body(update).await.to_vec()).unwrap();
    assert!(
        status.is_success(),
        "full update failed with {status}: {body}"
    );

    let after = app_state.config.read().await.notifications.ntfy.clone();
    assert_eq!(after.enabled, before.enabled);
    assert_eq!(after.topic, before.topic);
    assert_eq!(after.credential_ref, before.credential_ref);
    assert_eq!(after.configured, before.configured);
    assert_eq!(
        after.token.as_deref(),
        Some("full-payload-notification-secret")
    );
    assert_eq!(
        app_state
            .credential_store
            .resolve(&reference)
            .unwrap()
            .unwrap()
            .expose(),
        "full-payload-notification-secret"
    );
    assert!(!temp_dir.path().join("config.json").exists());
    let notifications =
        std::fs::read_to_string(temp_dir.path().join("notifications.json")).unwrap();
    assert!(!notifications.contains("full-payload-notification-secret"));
    assert!(!notifications.contains("token_encrypted"));
}

/// Root compatibility writes require the owned Notifications revision, reject
/// a mask as data, and preserve the stored secret when the credential field is
/// omitted explicitly.
#[actix_web::test]
async fn notification_compatibility_write_rejects_masks_and_keeps_omitted_secret() {
    use crate::app_state::AppState;
    use actix_web::{test, web, App};

    let temp_dir = tempdir().expect("temp dir should be created");
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");
    let data_dir = state.app_data_dir.clone();
    let app_state = web::Data::new(state);

    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .route("/bamboo/config", web::get().to(super::get_bamboo_config))
            .route("/bamboo/config", web::post().to(super::set_bamboo_config)),
    )
    .await;

    // 1) Set a real ntfy token.
    let post = test::TestRequest::post()
        .uri("/bamboo/config")
        .set_json(serde_json::json!({
            "expected_revision": 0,
            "notifications": {
                "ntfy": {
                    "enabled": true,
                    "base_url": "https://ntfy.sh",
                    "topic": "bamboo-alerts",
                    "token": "tk-real-secret"
                }
            }
        }))
        .to_request();
    let post_resp = test::call_service(&app, post).await;
    assert!(post_resp.status().is_success(), "set config should succeed");

    // The stored notifications section holds only isolated-store metadata, never the
    // plaintext token, legacy ciphertext, or a UI mask.
    let config_text = tokio::fs::read_to_string(data_dir.join("notifications.json"))
        .await
        .expect("notifications.json should exist after set");
    assert!(
        config_text.contains("notification.ntfy.token"),
        "ntfy token must persist through a stable credential reference"
    );
    assert!(!config_text.contains("token_encrypted"));
    assert!(!config_text.contains("****...****"));
    assert!(
        !config_text.contains("tk-real-secret"),
        "plaintext ntfy token must never be persisted"
    );

    // 2) GET masks the token as configured.
    let get = test::TestRequest::get().uri("/bamboo/config").to_request();
    let body: serde_json::Value = test::call_and_read_body_json(&app, get).await;
    assert_eq!(body["notifications"]["ntfy"]["token"], "****...****");
    assert_eq!(body["notifications"]["ntfy"]["topic"], "bamboo-alerts");

    // 3) A masked placeholder is never accepted as data.
    let post2 = test::TestRequest::post()
        .uri("/bamboo/config")
        .set_json(serde_json::json!({
            "expected_revision": 1,
            "notifications": {
                "ntfy": {
                    "enabled": true,
                    "base_url": "https://ntfy.sh",
                    "topic": "renamed-topic",
                    "token": "****...****"
                }
            }
        }))
        .to_request();
    let post2_resp = test::call_service(&app, post2).await;
    assert_eq!(
        post2_resp.status(),
        actix_web::http::StatusCode::BAD_REQUEST
    );

    // 4) Omission is the bounded compatibility form of `keep`.
    let post3 = test::TestRequest::post()
        .uri("/bamboo/config")
        .set_json(serde_json::json!({
            "expected_revision": 1,
            "notifications": {
                "ntfy": {
                    "enabled": true,
                    "base_url": "https://ntfy.sh",
                    "topic": "renamed-topic"
                }
            }
        }))
        .to_request();
    let post3_resp = test::call_service(&app, post3).await;
    assert!(
        post3_resp.status().is_success(),
        "secret-omitting update should succeed"
    );

    let get2 = test::TestRequest::get().uri("/bamboo/config").to_request();
    let body2: serde_json::Value = test::call_and_read_body_json(&app, get2).await;
    assert_eq!(
        body2["notifications"]["ntfy"]["token"], "****...****",
        "token must still read as configured"
    );
    assert_eq!(
        body2["notifications"]["ntfy"]["topic"], "renamed-topic",
        "unrelated field change must apply"
    );
}

/// End-to-end for #455/#738: a compatibility settings write persists into the
/// standalone `connect.json`, uses the Connect section revision, rejects masks,
/// and treats omission as the bounded `keep` form.
#[actix_web::test]
async fn connect_compatibility_write_is_revisioned_and_keeps_omitted_token() {
    use crate::app_state::AppState;
    use actix_web::{test, web, App};

    // NOTE: deliberately NOT using `encryption::set_test_encryption_key` here —
    // the actual save happens inside `AppState::persist_config_snapshot`'s
    // `tokio::task::spawn_blocking`, a different OS thread than this test body,
    // and the test-key override is thread-local (by design, so parallel tests
    // don't clobber a process-global override). An override set here would
    // only apply to the `decrypt` calls below, not the `encrypt` that ran on
    // the blocking pool thread, and the two would silently use different keys.
    // The encryption key file itself is already stable for this whole test
    // binary process (`bamboo_dir()`'s "first call wins" OnceLock — see
    // `paths::tests::test_init_bamboo_dir_first_call_wins`), so plain
    // `encryption::decrypt` here round-trips correctly against whatever key
    // the blocking-thread `encrypt` used.
    let temp_dir = tempdir().expect("temp dir should be created");
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");
    let data_dir = state.app_data_dir.clone();
    let app_state = web::Data::new(state);

    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .route("/bamboo/config", web::get().to(super::get_bamboo_config))
            .route("/bamboo/config", web::post().to(super::set_bamboo_config)),
    )
    .await;

    // 1) Set a real Telegram bot token via the settings PATCH surface.
    let post = test::TestRequest::post()
        .uri("/bamboo/config")
        .set_json(serde_json::json!({
            "expected_revision": 0,
            "connect": {
                "platforms": [
                    { "type": "telegram", "token": "tg-real-secret", "allow_from": ["u1"] }
                ]
            }
        }))
        .to_request();
    let post_resp = test::call_service(&app, post).await;
    let post_status = post_resp.status();
    let post_body = test::read_body(post_resp).await;
    assert!(
        post_status.is_success(),
        "set config should succeed: {}",
        String::from_utf8_lossy(&post_body)
    );

    // The active facade never recreates the legacy root document.
    assert!(!config_file_path(&data_dir).exists());

    // connect.json holds only metadata and the stable credential reference.
    let connect_path = data_dir.join("connect.json");
    let connect_text = tokio::fs::read_to_string(&connect_path)
        .await
        .expect("connect.json should exist after set");
    assert!(
        !connect_text.contains("tg-real-secret"),
        "plaintext connect token must never be persisted in connect.json"
    );
    assert!(!connect_text.contains("token_encrypted"));
    let connect_json: serde_json::Value =
        serde_json::from_str(&connect_text).expect("connect.json should parse");
    let reference = connect_json["data"]["platforms"][0]["token_credential_ref"]
        .as_str()
        .expect("token credential ref should be present")
        .to_string();
    assert_eq!(
        connect_json["data"]["platforms"][0]["token_configured"],
        true
    );
    assert_eq!(
        bamboo_config::CredentialStore::open(&data_dir)
            .resolve(&bamboo_config::CredentialRef::parse(reference.clone()).unwrap())
            .unwrap()
            .unwrap()
            .expose(),
        "tg-real-secret"
    );

    // 2) GET masks the token exactly as before the split (settings API surface
    // is unchanged; it reads the in-memory Config, unaware of the file split).
    let get = test::TestRequest::get().uri("/bamboo/config").to_request();
    let body: serde_json::Value = test::call_and_read_body_json(&app, get).await;
    assert_eq!(body["connect"]["platforms"][0]["token"], "****...****");
    assert_eq!(body["connect"]["platforms"][0]["type"], "telegram");

    // 3) A masked placeholder is never accepted as data.
    let post2 = test::TestRequest::post()
        .uri("/bamboo/config")
        .set_json(serde_json::json!({
            "expected_revision": 1,
            "connect": {
                "platforms": [
                    { "type": "telegram", "token": "****...****", "allow_from": ["u1", "u2"] }
                ]
            }
        }))
        .to_request();
    let post2_resp = test::call_service(&app, post2).await;
    assert_eq!(
        post2_resp.status(),
        actix_web::http::StatusCode::BAD_REQUEST
    );

    // 4) Omission is the bounded compatibility form of `keep`.
    let post3 = test::TestRequest::post()
        .uri("/bamboo/config")
        .set_json(serde_json::json!({
            "expected_revision": 1,
            "connect": {
                "platforms": [
                    { "type": "telegram", "allow_from": ["u1", "u2"] }
                ]
            }
        }))
        .to_request();
    let post3_resp = test::call_service(&app, post3).await;
    assert!(
        post3_resp.status().is_success(),
        "secret-omitting update should succeed"
    );

    let get2 = test::TestRequest::get().uri("/bamboo/config").to_request();
    let body2: serde_json::Value = test::call_and_read_body_json(&app, get2).await;
    assert_eq!(
        body2["connect"]["platforms"][0]["token"], "****...****",
        "token must still read as configured"
    );
    assert_eq!(
        body2["connect"]["platforms"][0]["allow_from"],
        serde_json::json!(["u1", "u2"]),
        "unrelated field change must apply"
    );

    let connect_text_after = tokio::fs::read_to_string(&connect_path)
        .await
        .expect("connect.json should still exist");
    let connect_json_after: serde_json::Value =
        serde_json::from_str(&connect_text_after).expect("connect.json should parse");
    assert_eq!(
        connect_json_after["data"]["platforms"][0]["token_credential_ref"], reference,
        "omission preserves the stable token reference"
    );
    assert_eq!(
        bamboo_config::CredentialStore::open(&data_dir)
            .resolve(&bamboo_config::CredentialRef::parse(reference).unwrap())
            .unwrap()
            .unwrap()
            .expose(),
        "tg-real-secret",
        "omission preserves the real token in the credential store"
    );
    assert!(!config_file_path(&data_dir).exists());
}

/// Feishu adapter config plumbing (epic #447 phase 3, §2a): `app_id`/`domain`
/// are plain fields, while `app_secret` is isolated, masks are rejected on
/// write, and omission preserves the credential.
#[actix_web::test]
async fn feishu_compatibility_write_rejects_mask_and_keeps_omitted_app_secret() {
    use crate::app_state::AppState;
    use actix_web::{test, web, App};

    // See the long NOTE on the Telegram equivalent above for why the
    // process-global (not thread-local) encryption key is relied on here.
    let temp_dir = tempdir().expect("temp dir should be created");
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");
    let data_dir = state.app_data_dir.clone();
    let app_state = web::Data::new(state);

    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .route("/bamboo/config", web::get().to(super::get_bamboo_config))
            .route("/bamboo/config", web::post().to(super::set_bamboo_config)),
    )
    .await;

    // 1) Set a Feishu entry with app_id, app_secret, domain via the settings
    // PATCH surface.
    let post = test::TestRequest::post()
        .uri("/bamboo/config")
        .set_json(serde_json::json!({
            "expected_revision": 0,
            "connect": {
                "platforms": [
                    {
                        "type": "feishu",
                        "app_id": "cli_real_app_id",
                        "app_secret": "feishu-real-secret",
                        "domain": "lark",
                        "allow_from": ["ou_1"]
                    }
                ]
            }
        }))
        .to_request();
    let post_resp = test::call_service(&app, post).await;
    assert!(post_resp.status().is_success(), "set config should succeed");

    assert!(!config_file_path(&data_dir).exists());

    // connect.json holds app_id/domain plus isolated credential metadata.
    let connect_path = data_dir.join("connect.json");
    let connect_text = tokio::fs::read_to_string(&connect_path)
        .await
        .expect("connect.json should exist after set");
    assert!(
        !connect_text.contains("feishu-real-secret"),
        "plaintext app_secret must never be persisted in connect.json"
    );
    assert!(!connect_text.contains("app_secret_encrypted"));
    let connect_json: serde_json::Value =
        serde_json::from_str(&connect_text).expect("connect.json should parse");
    assert_eq!(
        connect_json["data"]["platforms"][0]["app_id"],
        "cli_real_app_id"
    );
    assert_eq!(connect_json["data"]["platforms"][0]["domain"], "lark");
    let reference = connect_json["data"]["platforms"][0]["app_secret_credential_ref"]
        .as_str()
        .expect("app_secret credential ref should be present")
        .to_string();
    assert_eq!(
        bamboo_config::CredentialStore::open(&data_dir)
            .resolve(&bamboo_config::CredentialRef::parse(reference.clone()).unwrap())
            .unwrap()
            .unwrap()
            .expose(),
        "feishu-real-secret"
    );

    // 2) GET masks app_secret but leaves app_id/domain visible.
    let get = test::TestRequest::get().uri("/bamboo/config").to_request();
    let body: serde_json::Value = test::call_and_read_body_json(&app, get).await;
    assert_eq!(body["connect"]["platforms"][0]["app_secret"], "****...****");
    assert_eq!(body["connect"]["platforms"][0]["app_id"], "cli_real_app_id");
    assert_eq!(body["connect"]["platforms"][0]["domain"], "lark");

    // 3) A masked placeholder is never accepted as data.
    let post2 = test::TestRequest::post()
        .uri("/bamboo/config")
        .set_json(serde_json::json!({
            "expected_revision": 1,
            "connect": {
                "platforms": [
                    {
                        "type": "feishu",
                        "app_id": "cli_real_app_id",
                        "app_secret": "****...****",
                        "domain": "lark",
                        "allow_from": ["ou_1", "ou_2"]
                    }
                ]
            }
        }))
        .to_request();
    let post2_resp = test::call_service(&app, post2).await;
    assert_eq!(
        post2_resp.status(),
        actix_web::http::StatusCode::BAD_REQUEST
    );

    // 4) Omission is the bounded compatibility form of `keep`.
    let post3 = test::TestRequest::post()
        .uri("/bamboo/config")
        .set_json(serde_json::json!({
            "expected_revision": 1,
            "connect": {
                "platforms": [
                    {
                        "type": "feishu",
                        "app_id": "cli_real_app_id",
                        "domain": "lark",
                        "allow_from": ["ou_1", "ou_2"]
                    }
                ]
            }
        }))
        .to_request();
    let post3_resp = test::call_service(&app, post3).await;
    assert!(
        post3_resp.status().is_success(),
        "secret-omitting update should succeed"
    );

    let get2 = test::TestRequest::get().uri("/bamboo/config").to_request();
    let body2: serde_json::Value = test::call_and_read_body_json(&app, get2).await;
    assert_eq!(
        body2["connect"]["platforms"][0]["app_secret"], "****...****",
        "app_secret must still read as configured"
    );
    assert_eq!(
        body2["connect"]["platforms"][0]["allow_from"],
        serde_json::json!(["ou_1", "ou_2"]),
        "unrelated field change must apply"
    );

    let connect_text_after = tokio::fs::read_to_string(&connect_path)
        .await
        .expect("connect.json should still exist");
    let connect_json_after: serde_json::Value =
        serde_json::from_str(&connect_text_after).expect("connect.json should parse");
    assert_eq!(
        connect_json_after["data"]["platforms"][0]["app_secret_credential_ref"], reference,
        "omission preserves the stable app-secret reference"
    );
    assert_eq!(
        bamboo_config::CredentialStore::open(&data_dir)
            .resolve(&bamboo_config::CredentialRef::parse(reference).unwrap())
            .unwrap()
            .unwrap()
            .expose(),
        "feishu-real-secret",
        "omission preserves the real app_secret in the credential store"
    );
}

/// #490/#738 end-to-end: omission must preserve a credential by stable
/// platform id even when removing a preceding entry shifts its array index.
#[actix_web::test]
async fn connect_omission_preserves_secret_when_preceding_platform_is_removed() {
    use crate::app_state::AppState;
    use actix_web::{test, web, App};

    // See the long NOTE on the Telegram test above for why the process-global
    // (not thread-local) encryption key is relied on here.
    let temp_dir = tempdir().expect("temp dir should be created");
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");
    let data_dir = state.app_data_dir.clone();
    let app_state = web::Data::new(state);

    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .route("/bamboo/config", web::get().to(super::get_bamboo_config))
            .route("/bamboo/config", web::post().to(super::set_bamboo_config)),
    )
    .await;

    // 1) Set both a Telegram bot token and a Feishu app_secret, in that order.
    let post = test::TestRequest::post()
        .uri("/bamboo/config")
        .set_json(serde_json::json!({
            "expected_revision": 0,
            "connect": {
                "platforms": [
                    { "type": "telegram", "token": "tg-real-secret", "allow_from": ["u1"] },
                    {
                        "type": "feishu",
                        "app_id": "cli_real_app_id",
                        "app_secret": "feishu-real-secret",
                        "domain": "lark",
                        "allow_from": ["ou_1"]
                    }
                ]
            }
        }))
        .to_request();
    let post_resp = test::call_service(&app, post).await;
    assert!(post_resp.status().is_success(), "set config should succeed");

    // 2) GET, confirm both entries round-trip masked, in stored order.
    let get = test::TestRequest::get().uri("/bamboo/config").to_request();
    let body: serde_json::Value = test::call_and_read_body_json(&app, get).await;
    assert_eq!(body["connect"]["platforms"][0]["type"], "telegram");
    assert_eq!(body["connect"]["platforms"][1]["type"], "feishu");
    assert_eq!(body["connect"]["platforms"][1]["app_secret"], "****...****");
    let feishu_id = body["connect"]["platforms"][1]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let feishu_ref = bamboo_config::credential_ref("connect", &feishu_id, "app_secret").unwrap();

    // 3) Re-POST with telegram removed and the Feishu secret omitted. The
    // stable id identifies the same entry even though it moves to index 0.
    let post2 = test::TestRequest::post()
        .uri("/bamboo/config")
        .set_json(serde_json::json!({
            "expected_revision": 1,
            "connect": {
                "platforms": [
                    {
                        "id": feishu_id,
                        "type": "feishu",
                        "app_id": "cli_real_app_id",
                        "domain": "lark",
                        "allow_from": ["ou_1"]
                    }
                ]
            }
        }))
        .to_request();
    let post2_resp = test::call_service(&app, post2).await;
    assert!(
        post2_resp.status().is_success(),
        "second set config should succeed"
    );

    // The feishu app_secret must still read as configured, not wiped.
    let get2 = test::TestRequest::get().uri("/bamboo/config").to_request();
    let body2: serde_json::Value = test::call_and_read_body_json(&app, get2).await;
    assert_eq!(
        body2["connect"]["platforms"][0]["type"], "feishu",
        "telegram entry should be gone, feishu now at index 0"
    );
    assert_eq!(
        body2["connect"]["platforms"][0]["app_secret"], "****...****",
        "app_secret must still read as configured after the index shift"
    );

    // And the plaintext on disk (encrypted at rest) must be unchanged, not
    // wiped by the removed-entry-shifted-index case.
    let connect_path = data_dir.join("connect.json");
    let connect_text_after = tokio::fs::read_to_string(&connect_path)
        .await
        .expect("connect.json should still exist");
    let connect_json_after: serde_json::Value =
        serde_json::from_str(&connect_text_after).expect("connect.json should parse");
    assert_eq!(
        connect_json_after["data"]["platforms"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        connect_json_after["data"]["platforms"][0]["app_secret_credential_ref"],
        feishu_ref.as_str()
    );
    assert!(!connect_text_after.contains("app_secret_encrypted"));
    assert_eq!(
        bamboo_config::CredentialStore::open(&data_dir)
            .resolve(&feishu_ref)
            .unwrap()
            .unwrap()
            .expose(),
        "feishu-real-secret",
        "omitted secret must resolve by stable identity after a preceding entry was removed"
    );
}

/// The modular facade must reject the legacy full reset before deleting any
/// member. A multi-section reset needs a recoverable manifest; callers use the
/// revisioned typed section API until that transaction exists.
#[actix_web::test]
async fn reset_bamboo_config_rejects_modular_layout_without_mutation() {
    use crate::app_state::AppState;
    use actix_web::{test, web, App};

    let temp_dir = tempdir().expect("temp dir should be created");
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");
    let data_dir = state.app_data_dir.clone();
    let app_state = web::Data::new(state);

    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .route("/bamboo/config", web::get().to(super::get_bamboo_config))
            .route("/bamboo/config", web::post().to(super::set_bamboo_config))
            .route(
                "/bamboo/config/reset",
                web::post().to(super::reset_bamboo_config),
            ),
    )
    .await;

    // Configure a connect platform (creates connect.json).
    let post = test::TestRequest::post()
        .uri("/bamboo/config")
        .set_json(serde_json::json!({
            "expected_revision": 0,
            "connect": {
                "platforms": [
                    { "type": "telegram", "token": "tg-reset-me", "allow_from": ["u1"] }
                ]
            }
        }))
        .to_request();
    assert!(test::call_service(&app, post).await.status().is_success());

    let connect_path = data_dir.join("connect.json");
    assert!(
        connect_path.exists(),
        "connect.json should exist before reset"
    );

    let connect_before = tokio::fs::read(&connect_path).await.unwrap();
    let reset = test::TestRequest::post()
        .uri("/bamboo/config/reset")
        .to_request();
    assert_eq!(
        test::call_service(&app, reset).await.status(),
        actix_web::http::StatusCode::BAD_REQUEST
    );
    assert_eq!(tokio::fs::read(connect_path).await.unwrap(), connect_before);
}

/// Rejection must also preserve the encrypted backup; no transaction member
/// may be partially removed.
#[actix_web::test]
async fn reset_bamboo_config_rejection_preserves_connect_backup() {
    use crate::app_state::AppState;
    use actix_web::{test, web, App};

    let temp_dir = tempdir().expect("temp dir should be created");
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");
    let data_dir = state.app_data_dir.clone();
    let app_state = web::Data::new(state);

    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .route("/bamboo/config", web::get().to(super::get_bamboo_config))
            .route("/bamboo/config", web::post().to(super::set_bamboo_config))
            .route(
                "/bamboo/config/reset",
                web::post().to(super::reset_bamboo_config),
            ),
    )
    .await;

    // Configure a connect platform, then change it again so a
    // `connect.json.bak` gets written (the save path backs up the previous
    // connect.json before overwriting it).
    for (expected_revision, token) in ["tg-first-token", "tg-second-token"]
        .into_iter()
        .enumerate()
    {
        let post = test::TestRequest::post()
            .uri("/bamboo/config")
            .set_json(serde_json::json!({
                "expected_revision": expected_revision,
                "connect": {
                    "platforms": [
                        { "type": "telegram", "token": token, "allow_from": ["u1"] }
                    ]
                }
            }))
            .to_request();
        assert!(test::call_service(&app, post).await.status().is_success());
    }

    let connect_path = data_dir.join("connect.json");
    let connect_backup_path = data_dir.join("connect.json.bak");
    assert!(
        connect_path.exists(),
        "connect.json should exist before reset"
    );
    assert!(
        connect_backup_path.exists(),
        "connect.json.bak should exist before reset (written by the second save)"
    );

    let connect_before = tokio::fs::read(&connect_path).await.unwrap();
    let backup_before = tokio::fs::read(&connect_backup_path).await.unwrap();
    let reset = test::TestRequest::post()
        .uri("/bamboo/config/reset")
        .to_request();
    assert_eq!(
        test::call_service(&app, reset).await.status(),
        actix_web::http::StatusCode::BAD_REQUEST
    );
    assert_eq!(tokio::fs::read(connect_path).await.unwrap(), connect_before);
    assert_eq!(
        tokio::fs::read(connect_backup_path).await.unwrap(),
        backup_before
    );
}

// ── config-corruption recovery status + confirm API (#153) ─────────────────

/// GET /bamboo/config/recovery-status reports no pending recovery on a clean
/// config.json, and a full pending status (source + quarantine path) after
/// booting against a corrupt one.
#[actix_web::test]
async fn get_config_recovery_status_reports_pending_state_after_corrupt_boot() {
    use crate::app_state::AppState;
    use actix_web::{test, web, App};

    // No pending recovery on a totally fresh data dir.
    let clean_dir = tempdir().expect("temp dir should be created");
    let clean_state = AppState::new(clean_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");
    let clean_app = test::init_service(App::new().app_data(web::Data::new(clean_state)).route(
        "/bamboo/config/recovery-status",
        web::get().to(super::get_config_recovery_status),
    ))
    .await;
    let get = test::TestRequest::get()
        .uri("/bamboo/config/recovery-status")
        .to_request();
    let body: serde_json::Value = test::call_and_read_body_json(&clean_app, get).await;
    assert_eq!(body["pending"], false);

    // Boot against a corrupt config.json (no .bak) -> defaults + pending recovery.
    let corrupt_dir = tempdir().expect("temp dir should be created");
    std::fs::write(corrupt_dir.path().join("config.json"), "}}} broken").unwrap();
    let corrupt_state = AppState::new(corrupt_dir.path().to_path_buf())
        .await
        .expect("app state should still initialize");
    let corrupt_app = test::init_service(App::new().app_data(web::Data::new(corrupt_state)).route(
        "/bamboo/config/recovery-status",
        web::get().to(super::get_config_recovery_status),
    ))
    .await;
    let get = test::TestRequest::get()
        .uri("/bamboo/config/recovery-status")
        .to_request();
    let body: serde_json::Value = test::call_and_read_body_json(&corrupt_app, get).await;
    assert_eq!(body["pending"], true);
    assert_eq!(body["status"]["confirmed"], false);
    assert_eq!(body["status"]["source"]["kind"], "defaults");
    assert!(
        body["status"]["quarantine_path"].is_string(),
        "quarantine_path should point at the preserved corrupt original"
    );
}

/// POST /bamboo/config/recovery/confirm with `{"accept": true}` persists the
/// recovered config and flips `pending` to `false`; a subsequent GET agrees.
#[actix_web::test]
async fn confirm_config_recovery_endpoint_accept_clears_pending_state() {
    use crate::app_state::AppState;
    use actix_web::{test, web, App};

    let temp_dir = tempdir().expect("temp dir should be created");
    std::fs::write(
        temp_dir.path().join("config.json"),
        r#"{"http_proxy":"http://salvaged","env_vars":"bad-type"}"#,
    )
    .unwrap();
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");
    let data_dir = state.app_data_dir.clone();
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(state))
            .route(
                "/bamboo/config/recovery-status",
                web::get().to(super::get_config_recovery_status),
            )
            .route(
                "/bamboo/config/recovery/confirm",
                web::post().to(super::confirm_config_recovery),
            ),
    )
    .await;

    let confirm = test::TestRequest::post()
        .uri("/bamboo/config/recovery/confirm")
        .set_json(serde_json::json!({ "accept": true }))
        .to_request();
    let confirm_resp = test::call_service(&app, confirm).await;
    assert!(
        confirm_resp.status().is_success(),
        "accepting a pending recovery should succeed"
    );
    let body: serde_json::Value = test::read_body_json(confirm_resp).await;
    assert_eq!(body["pending"], false);

    let get = test::TestRequest::get()
        .uri("/bamboo/config/recovery-status")
        .to_request();
    let body: serde_json::Value = test::call_and_read_body_json(&app, get).await;
    assert_eq!(
        body["pending"], false,
        "a follow-up GET agrees the recovery is resolved"
    );

    let on_disk = tokio::fs::read_to_string(config_file_path(&data_dir))
        .await
        .expect("config.json should exist");
    assert!(
        on_disk.contains("http://salvaged"),
        "the recovered state was actually persisted to config.json"
    );
}

/// POST with `{"accept": false}` leaves config.json untouched and `pending`
/// still `true`.
#[actix_web::test]
async fn confirm_config_recovery_endpoint_reject_leaves_pending_state() {
    use crate::app_state::AppState;
    use actix_web::{test, web, App};

    let temp_dir = tempdir().expect("temp dir should be created");
    let corrupt_bytes = "}}} broken";
    std::fs::write(temp_dir.path().join("config.json"), corrupt_bytes).unwrap();
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");
    let data_dir = state.app_data_dir.clone();
    let app = test::init_service(App::new().app_data(web::Data::new(state)).route(
        "/bamboo/config/recovery/confirm",
        web::post().to(super::confirm_config_recovery),
    ))
    .await;

    let confirm = test::TestRequest::post()
        .uri("/bamboo/config/recovery/confirm")
        .set_json(serde_json::json!({ "accept": false }))
        .to_request();
    let confirm_resp = test::call_service(&app, confirm).await;
    assert!(confirm_resp.status().is_success());
    let body: serde_json::Value = test::read_body_json(confirm_resp).await;
    assert_eq!(body["pending"], true, "reject leaves the recovery pending");

    let on_disk = tokio::fs::read_to_string(config_file_path(&data_dir))
        .await
        .expect("config.json should exist");
    assert_eq!(
        on_disk, corrupt_bytes,
        "reject must not touch the on-disk corrupt original"
    );
}

/// POST /bamboo/config/recovery/confirm with nothing pending is a 400, not a
/// silent success.
#[actix_web::test]
async fn confirm_config_recovery_endpoint_errors_when_nothing_pending() {
    use crate::app_state::AppState;
    use actix_web::{test, web, App};

    let temp_dir = tempdir().expect("temp dir should be created");
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");
    let app = test::init_service(App::new().app_data(web::Data::new(state)).route(
        "/bamboo/config/recovery/confirm",
        web::post().to(super::confirm_config_recovery),
    ))
    .await;

    let confirm = test::TestRequest::post()
        .uri("/bamboo/config/recovery/confirm")
        .set_json(serde_json::json!({ "accept": true }))
        .to_request();
    let confirm_resp = test::call_service(&app, confirm).await;
    assert_eq!(
        confirm_resp.status(),
        actix_web::http::StatusCode::BAD_REQUEST
    );
}
