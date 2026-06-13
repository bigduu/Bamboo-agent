use std::collections::BTreeMap;

use tempfile::tempdir;

use bamboo_config::OpenAIConfig;
use bamboo_llm::Config;

use super::common::{
    config_file_path, model_limits_file_path, read_model_limits_file, redacted_config_json,
    write_model_limits_file,
};
use super::reset::remove_config_file_if_exists;

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
    config.providers.openai = Some(OpenAIConfig {
        api_key: "sk-secret".to_string(),
        api_key_encrypted: None,
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
async fn remove_config_file_if_exists_deletes_existing_file() {
    let dir = tempdir().expect("temp dir should be created");
    let path = dir.path().join("config.json");
    tokio::fs::write(&path, "{}")
        .await
        .expect("test file should be written");

    remove_config_file_if_exists(&path)
        .await
        .expect("existing config file should be deleted");
    assert!(!path.exists());
}

#[actix_web::test]
async fn remove_config_file_if_exists_is_noop_when_missing() {
    let dir = tempdir().expect("temp dir should be created");
    let path = dir.path().join("config.json");

    remove_config_file_if_exists(&path)
        .await
        .expect("missing config file should not fail");
    assert!(!path.exists());
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

#[actix_web::test]
async fn remove_config_file_if_exists_deletes_model_limits_file() {
    let dir = tempdir().expect("temp dir should be created");
    let path = dir.path().join("model_limits.json");
    tokio::fs::write(&path, "[]")
        .await
        .expect("model limits file should be written");

    remove_config_file_if_exists(&path)
        .await
        .expect("existing model limits file should be deleted");
    assert!(!path.exists());
}

// --- Diff-only storage (integration over the persistence seam) ------------

/// Saving a mix of a real override and a no-op (default-equal) row must persist
/// only the real override, then read it back through the same seam GET uses.
#[actix_web::test]
async fn write_model_limits_file_drops_default_equal_rows_round_trip() {
    let dir = tempdir().expect("temp dir should be created");

    write_model_limits_file(
        dir.path(),
        Some(&serde_json::json!([
            // Real override: smaller context than the 200K default.
            { "model_pattern": "gpt-4o", "max_context_tokens": 128000, "max_output_tokens": 16384 },
            // No-op: identical to the global default (200K / 64K, no safety margin).
            { "model_pattern": "some-default-model", "max_context_tokens": 200000, "max_output_tokens": 64000 }
        ])),
    )
    .await
    .expect("model limits file should be written");

    let persisted = read_model_limits_file(dir.path())
        .await
        .expect("read should succeed")
        .expect("file should exist");

    let rows = persisted.as_array().expect("array");
    assert_eq!(rows.len(), 1, "default-equal row must be dropped");
    assert_eq!(rows[0]["model_pattern"], "gpt-4o");
    assert_eq!(rows[0]["max_context_tokens"], 128000);
}

/// Saving only no-op rows must remove the file entirely so everything falls
/// back to the global default.
#[actix_web::test]
async fn write_model_limits_file_removes_file_when_all_rows_default_equal() {
    let dir = tempdir().expect("temp dir should be created");
    let path = model_limits_file_path(dir.path());

    // Pre-existing override on disk.
    tokio::fs::write(
        &path,
        r#"[{"model_pattern":"gpt-4o","max_context_tokens":128000,"max_output_tokens":16384}]"#,
    )
    .await
    .expect("seed file");

    // User reverts everything to default → all rows are no-ops.
    write_model_limits_file(
        dir.path(),
        Some(&serde_json::json!([
            { "model_pattern": "gpt-4o", "max_context_tokens": 200000, "max_output_tokens": 64000 }
        ])),
    )
    .await
    .expect("write should succeed");

    assert!(
        !path.exists(),
        "file should be removed when no overrides remain"
    );
    assert!(read_model_limits_file(dir.path())
        .await
        .expect("read should succeed")
        .is_none());
}

/// An explicit safety margin is a real override even at the default size and
/// must be persisted.
#[actix_web::test]
async fn write_model_limits_file_keeps_default_size_with_custom_safety_margin() {
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
/// `model_limits` (one real override + one no-op), then GET it back. Asserts:
/// - only the real override is persisted (diff-only),
/// - `config.json` does NOT carry the `model_limits` key (it is split out),
/// - GET re-injects `model_limits` from the dedicated file.
#[actix_web::test]
async fn set_then_get_bamboo_config_round_trips_only_overrides() {
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

    let post = test::TestRequest::post()
        .uri("/bamboo/config")
        .set_json(serde_json::json!({
            "model": "gpt-4o",
            "model_limits": [
                { "model_pattern": "gpt-4o", "max_context_tokens": 128000, "max_output_tokens": 16384 },
                { "model_pattern": "noop", "max_context_tokens": 200000, "max_output_tokens": 64000 }
            ]
        }))
        .to_request();
    let post_resp = test::call_service(&app, post).await;
    assert!(post_resp.status().is_success(), "set config should succeed");

    // Diff-only: only the real override survives on disk.
    let on_disk = read_model_limits_file(&data_dir)
        .await
        .expect("read should succeed")
        .expect("model_limits.json should exist");
    let rows = on_disk.as_array().expect("array");
    assert_eq!(rows.len(), 1, "no-op row must be dropped");
    assert_eq!(rows[0]["model_pattern"], "gpt-4o");

    // config.json must not carry the model_limits key.
    let config_text = tokio::fs::read_to_string(config_file_path(&data_dir))
        .await
        .expect("config.json should exist after set");
    let config_json: serde_json::Value =
        serde_json::from_str(&config_text).expect("config.json should be valid json");
    assert!(
        config_json.get("model_limits").is_none(),
        "model_limits must be split out of config.json"
    );

    // GET re-injects model_limits from the dedicated file.
    let get = test::TestRequest::get().uri("/bamboo/config").to_request();
    let body: serde_json::Value = test::call_and_read_body_json(&app, get).await;
    let limits = body["model_limits"]
        .as_array()
        .expect("model_limits should be present in GET response");
    assert_eq!(limits.len(), 1);
    assert_eq!(limits[0]["model_pattern"], "gpt-4o");
    assert_eq!(limits[0]["max_context_tokens"], 128000);
}
