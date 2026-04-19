use std::collections::BTreeMap;

use tempfile::tempdir;

use bamboo_infrastructure::{Config, OpenAIConfig};

use super::common::{
    config_file_path, model_limits_file_path, redacted_config_json, write_model_limits_file,
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
