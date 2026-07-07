use std::collections::BTreeMap;

use bamboo_config::{OpenAIConfig, ProviderConfigs};
use bamboo_llm::Config;

use super::types::ProxyAuthPayload;
use super::validation::provider_validation_issue;

#[test]
fn provider_validation_issue_returns_missing_key_path_for_unconfigured_openai() {
    let config = Config {
        provider: "openai".to_string(),
        providers: ProviderConfigs::default(),
        ..Default::default()
    };

    let (path, message) = provider_validation_issue(&config, "invalid".to_string());
    assert_eq!(path, "providers.openai.api_key");
    assert_eq!(message, "OpenAI API key is required");
}

#[test]
fn provider_validation_issue_returns_provider_path_when_openai_key_present() {
    let config = Config {
        provider: "openai".to_string(),
        providers: ProviderConfigs {
            openai: Some(OpenAIConfig {
                api_key_from_env: false,
                api_key: "sk-test".to_string(),
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
            }),
            ..Default::default()
        },
        ..Default::default()
    };

    let (path, message) = provider_validation_issue(&config, "invalid provider setup".to_string());
    assert_eq!(path, "provider");
    assert_eq!(message, "invalid provider setup");
}

#[test]
fn proxy_auth_payload_without_username_disables_proxy_auth() {
    let payload: ProxyAuthPayload =
        serde_json::from_value(serde_json::json!({ "password": "secret" })).unwrap();

    assert!(payload.into_proxy_auth().is_none());
}

#[test]
fn proxy_auth_payload_with_username_creates_proxy_auth() {
    let payload: ProxyAuthPayload = serde_json::from_value(serde_json::json!({
        "username": "alice",
        "password": "secret"
    }))
    .unwrap();

    let auth = payload.into_proxy_auth().expect("proxy auth should exist");
    assert_eq!(auth.username, "alice");
    assert_eq!(auth.password, "secret");
}
