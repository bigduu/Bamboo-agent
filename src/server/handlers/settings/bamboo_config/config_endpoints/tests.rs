use std::collections::BTreeMap;

use tempfile::tempdir;

use crate::core::{Config, OpenAIConfig};

use super::common::{config_file_path, redacted_config_json};
use super::reset::remove_config_file_if_exists;

#[test]
fn config_file_path_appends_config_json_filename() {
    let dir = tempdir().expect("temp dir should be created");
    assert_eq!(config_file_path(dir.path()), dir.path().join("config.json"));
}

#[test]
fn redacted_config_json_masks_provider_api_key_and_hides_encrypted_proxy_auth() {
    let mut config = Config::default();
    config.providers.openai = Some(OpenAIConfig {
        api_key: "sk-secret".to_string(),
        api_key_encrypted: None,
        base_url: None,
        model: None,
        reasoning_effort: None,
        responses_only_models: vec![],
        extra: BTreeMap::new(),
    });
    config.proxy_auth_encrypted = Some("enc:deadbeef".to_string());

    let value = redacted_config_json(&config).expect("redacted config should serialize");
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
