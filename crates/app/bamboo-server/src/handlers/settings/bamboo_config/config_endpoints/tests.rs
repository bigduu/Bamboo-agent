use std::collections::BTreeMap;

use tempfile::tempdir;

use bamboo_config::OpenAIConfig;
use bamboo_llm::Config;

use super::common::{
    config_file_path, connect_backup_file_path, connect_file_path, model_limits_file_path,
    read_model_limits_file, redacted_config_json, write_model_limits_file,
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

#[test]
fn connect_file_path_appends_connect_json_filename() {
    let dir = tempdir().expect("temp dir should be created");
    assert_eq!(
        connect_file_path(dir.path()),
        dir.path().join("connect.json")
    );
}

#[test]
fn connect_backup_file_path_appends_connect_json_bak_filename() {
    let dir = tempdir().expect("temp dir should be created");
    assert_eq!(
        connect_backup_file_path(dir.path()),
        dir.path().join("connect.json.bak")
    );
}

#[actix_web::test]
async fn redacted_config_json_masks_provider_api_key_and_hides_encrypted_proxy_auth() {
    let mut config = Config::default();
    config.providers.openai = Some(OpenAIConfig {
        api_key: "sk-secret".to_string(),
        api_key_from_env: false,
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

    // All overrides survive on disk (no diff-only filtering).
    let on_disk = read_model_limits_file(&data_dir)
        .await
        .expect("read should succeed")
        .expect("model_limits.json should exist");
    let rows = on_disk.as_array().expect("array");
    assert_eq!(rows.len(), 2, "all rows must be persisted");
    assert_eq!(rows[0]["model_pattern"], "gpt-4o");
    assert_eq!(rows[1]["model_pattern"], "noop");

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
    assert_eq!(limits.len(), 2);
    assert_eq!(limits[0]["model_pattern"], "gpt-4o");
    assert_eq!(limits[0]["max_context_tokens"], 128000);
}

/// End-to-end: POST an ntfy token, confirm the GET response masks it, then
/// POST again with the masked placeholder unchanged (as the UI would on an
/// unrelated field edit) — the exact-mask keep-on-save rule must resolve that
/// back to the live plaintext rather than wiping the stored secret. #430-style
/// contract, applied to the new notification channel secrets.
#[actix_web::test]
async fn set_then_get_bamboo_config_masks_and_preserves_ntfy_token() {
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

    // The stored config.json must hold ciphertext, never the plaintext token.
    let config_text = tokio::fs::read_to_string(config_file_path(&data_dir))
        .await
        .expect("config.json should exist after set");
    assert!(
        config_text.contains("token_encrypted"),
        "ntfy token must be persisted encrypted"
    );
    assert!(
        !config_text.contains("tk-real-secret"),
        "plaintext ntfy token must never be persisted"
    );

    // 2) GET masks the token as configured.
    let get = test::TestRequest::get().uri("/bamboo/config").to_request();
    let body: serde_json::Value = test::call_and_read_body_json(&app, get).await;
    assert_eq!(body["notifications"]["ntfy"]["token"], "****...****");
    assert_eq!(body["notifications"]["ntfy"]["topic"], "bamboo-alerts");

    // 3) Re-POST with the masked placeholder (as the UI echoes it back) plus
    // an unrelated field change — the secret must survive untouched.
    let post2 = test::TestRequest::post()
        .uri("/bamboo/config")
        .set_json(serde_json::json!({
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
    assert!(
        post2_resp.status().is_success(),
        "second set config should succeed"
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

/// End-to-end for #455: a settings PATCH that sets a bamboo-connect platform
/// must persist into the standalone `connect.json` — NOT into `config.json`
/// — while the settings API (GET redaction, masked-token keep-on-save)
/// behaves exactly as it did before the persistence split.
#[actix_web::test]
async fn set_bamboo_config_persists_connect_into_connect_json_only_and_preserves_masked_token() {
    use crate::app_state::AppState;
    use actix_web::{test, web, App};
    use bamboo_config::encryption;

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
            "connect": {
                "platforms": [
                    { "type": "telegram", "token": "tg-real-secret", "allow_from": ["u1"] }
                ]
            }
        }))
        .to_request();
    let post_resp = test::call_service(&app, post).await;
    assert!(post_resp.status().is_success(), "set config should succeed");

    // config.json must NOT carry the `connect` key at all (#455 split).
    let config_text = tokio::fs::read_to_string(config_file_path(&data_dir))
        .await
        .expect("config.json should exist after set");
    assert!(
        !config_text.contains("\"connect\""),
        "config.json must not persist the connect key"
    );
    assert!(
        !config_text.contains("tg-real-secret"),
        "plaintext connect token must never be persisted anywhere"
    );

    // connect.json holds the platform, encrypted at rest.
    let connect_path = data_dir.join("connect.json");
    let connect_text = tokio::fs::read_to_string(&connect_path)
        .await
        .expect("connect.json should exist after set");
    assert!(
        connect_text.contains("token_encrypted"),
        "connect token must be persisted encrypted in connect.json"
    );
    assert!(
        !connect_text.contains("tg-real-secret"),
        "plaintext connect token must never be persisted in connect.json"
    );
    let connect_json: serde_json::Value =
        serde_json::from_str(&connect_text).expect("connect.json should parse");
    let token_encrypted = connect_json["platforms"][0]["token_encrypted"]
        .as_str()
        .expect("token_encrypted should be present")
        .to_string();
    assert_eq!(
        encryption::decrypt(&token_encrypted).expect("token should decrypt"),
        "tg-real-secret"
    );

    // 2) GET masks the token exactly as before the split (settings API surface
    // is unchanged; it reads the in-memory Config, unaware of the file split).
    let get = test::TestRequest::get().uri("/bamboo/config").to_request();
    let body: serde_json::Value = test::call_and_read_body_json(&app, get).await;
    assert_eq!(body["connect"]["platforms"][0]["token"], "****...****");
    assert_eq!(body["connect"]["platforms"][0]["type"], "telegram");

    // 3) Re-POST with the masked placeholder (as the UI echoes it back) plus
    // an unrelated field change — the secret must survive untouched, and
    // still land only in connect.json.
    let post2 = test::TestRequest::post()
        .uri("/bamboo/config")
        .set_json(serde_json::json!({
            "connect": {
                "platforms": [
                    { "type": "telegram", "token": "****...****", "allow_from": ["u1", "u2"] }
                ]
            }
        }))
        .to_request();
    let post2_resp = test::call_service(&app, post2).await;
    assert!(
        post2_resp.status().is_success(),
        "second set config should succeed"
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
    let token_encrypted_after = connect_json_after["platforms"][0]["token_encrypted"]
        .as_str()
        .expect("token_encrypted should still be present")
        .to_string();
    assert_eq!(
        encryption::decrypt(&token_encrypted_after).expect("token should decrypt"),
        "tg-real-secret",
        "masked round-trip preserves the real token across the split files"
    );

    let config_text_after = tokio::fs::read_to_string(config_file_path(&data_dir))
        .await
        .expect("config.json should still exist");
    assert!(
        !config_text_after.contains("\"connect\""),
        "config.json must still not carry the connect key after the second PATCH"
    );
}

/// Feishu adapter config plumbing (epic #447 phase 3, §2a): `app_id`/`domain`
/// are plain fields, `app_secret` follows the exact same encrypted-at-rest +
/// masked-GET + masked-preserve-on-PATCH contract as the Telegram `token`
/// above.
#[actix_web::test]
async fn set_bamboo_config_persists_feishu_app_secret_encrypted_and_preserves_masked_value() {
    use crate::app_state::AppState;
    use actix_web::{test, web, App};
    use bamboo_config::encryption;

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

    // config.json must NOT carry the `connect` key at all (#455 split).
    let config_text = tokio::fs::read_to_string(config_file_path(&data_dir))
        .await
        .expect("config.json should exist after set");
    assert!(
        !config_text.contains("\"connect\""),
        "config.json must not persist the connect key"
    );
    assert!(
        !config_text.contains("feishu-real-secret"),
        "plaintext app_secret must never be persisted anywhere"
    );

    // connect.json holds app_id/domain in plain and app_secret encrypted.
    let connect_path = data_dir.join("connect.json");
    let connect_text = tokio::fs::read_to_string(&connect_path)
        .await
        .expect("connect.json should exist after set");
    assert!(
        connect_text.contains("app_secret_encrypted"),
        "app_secret must be persisted encrypted in connect.json"
    );
    assert!(
        !connect_text.contains("feishu-real-secret"),
        "plaintext app_secret must never be persisted in connect.json"
    );
    let connect_json: serde_json::Value =
        serde_json::from_str(&connect_text).expect("connect.json should parse");
    assert_eq!(connect_json["platforms"][0]["app_id"], "cli_real_app_id");
    assert_eq!(connect_json["platforms"][0]["domain"], "lark");
    let app_secret_encrypted = connect_json["platforms"][0]["app_secret_encrypted"]
        .as_str()
        .expect("app_secret_encrypted should be present")
        .to_string();
    assert_eq!(
        encryption::decrypt(&app_secret_encrypted).expect("app_secret should decrypt"),
        "feishu-real-secret"
    );

    // 2) GET masks app_secret but leaves app_id/domain visible.
    let get = test::TestRequest::get().uri("/bamboo/config").to_request();
    let body: serde_json::Value = test::call_and_read_body_json(&app, get).await;
    assert_eq!(body["connect"]["platforms"][0]["app_secret"], "****...****");
    assert_eq!(body["connect"]["platforms"][0]["app_id"], "cli_real_app_id");
    assert_eq!(body["connect"]["platforms"][0]["domain"], "lark");

    // 3) Re-POST with the masked placeholder plus an unrelated field change —
    // the secret must survive untouched.
    let post2 = test::TestRequest::post()
        .uri("/bamboo/config")
        .set_json(serde_json::json!({
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
    assert!(
        post2_resp.status().is_success(),
        "second set config should succeed"
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
    let app_secret_encrypted_after = connect_json_after["platforms"][0]["app_secret_encrypted"]
        .as_str()
        .expect("app_secret_encrypted should still be present")
        .to_string();
    assert_eq!(
        encryption::decrypt(&app_secret_encrypted_after).expect("app_secret should decrypt"),
        "feishu-real-secret",
        "masked round-trip preserves the real app_secret across the split files"
    );
}

/// #455 gap: a full config reset must also clear `connect.json`, or a
/// bamboo-connect bot token would silently survive `POST
/// /bamboo/config/reset` and get re-merged back onto the freshly-defaulted
/// config on the very next load (`Config::merge_connect_config` adopts
/// whatever connect.json still has).
#[actix_web::test]
async fn reset_bamboo_config_also_deletes_connect_json() {
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

    // Reset.
    let reset = test::TestRequest::post()
        .uri("/bamboo/config/reset")
        .to_request();
    assert!(
        test::call_service(&app, reset).await.status().is_success(),
        "reset should succeed"
    );

    assert!(
        !connect_path.exists(),
        "connect.json must be deleted by a full config reset"
    );

    // The in-memory config must also be clear, not silently re-merged.
    let get = test::TestRequest::get().uri("/bamboo/config").to_request();
    let body: serde_json::Value = test::call_and_read_body_json(&app, get).await;
    assert!(
        body["connect"]["platforms"]
            .as_array()
            .map(|a| a.is_empty())
            .unwrap_or(true),
        "connect config must be empty after reset, not re-adopted from a stale file"
    );
}

/// #457: a full config reset must also clear `connect.json.bak`. Unlike
/// `config.json.bak` (a low-sensitivity config snapshot intentionally left
/// alone by reset, for recovery), connect.json(.bak) holds an encrypted IM
/// bot token — an immediately-usable remote-control credential — that must
/// not stay recoverable straight off disk after the user asks for a full
/// reset.
#[actix_web::test]
async fn reset_bamboo_config_also_deletes_connect_json_bak() {
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
    for token in ["tg-first-token", "tg-second-token"] {
        let post = test::TestRequest::post()
            .uri("/bamboo/config")
            .set_json(serde_json::json!({
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

    // Reset.
    let reset = test::TestRequest::post()
        .uri("/bamboo/config/reset")
        .to_request();
    assert!(
        test::call_service(&app, reset).await.status().is_success(),
        "reset should succeed"
    );

    assert!(
        !connect_path.exists(),
        "connect.json must be deleted by a full config reset"
    );
    assert!(
        !connect_backup_path.exists(),
        "connect.json.bak must also be deleted by a full config reset — it holds \
         a live, immediately-usable bot token"
    );
}
