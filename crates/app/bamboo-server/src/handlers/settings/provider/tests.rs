use bamboo_config::encryption::set_test_encryption_key;
use bamboo_config::{OpenAIConfig, ProviderConfigs};
use bamboo_llm::Config;
use bamboo_mcp::{HeaderConfig, McpServerConfig, SseConfig, StdioConfig, TransportConfig};
use std::collections::HashMap;

fn deep_merge_json(dst: &mut serde_json::Value, src: serde_json::Value) {
    match (dst, src) {
        (serde_json::Value::Object(dst_obj), serde_json::Value::Object(src_obj)) => {
            for (k, v) in src_obj {
                deep_merge_json(dst_obj.entry(k).or_insert(serde_json::Value::Null), v);
            }
        }
        (dst_slot, src_val) => {
            *dst_slot = src_val;
        }
    }
}

fn build_config_with_mcp_secrets(temp_dir: &std::path::Path) -> Config {
    let mut cfg = Config {
        provider: "openai".to_string(),
        providers: ProviderConfigs {
            openai: Some(OpenAIConfig {
                api_key_from_env: false,
                api_key: "sk-test".to_string(),
                api_key_encrypted: None,
                base_url: None,
                model: Some("gpt-4o".to_string()),
                fast_model: None,
                vision_model: None,
                reasoning_effort: None,
                responses_only_models: vec![],
                request_overrides: None,
                extra: Default::default(),
            }),
            ..ProviderConfigs::default()
        },
        ..Config::default()
    };

    cfg.mcp.servers = vec![
        McpServerConfig {
            id: "stdio-secret".to_string(),
            name: Some("Stdio Secret".to_string()),
            enabled: false, // tests must not spawn actual MCP servers
            transport: TransportConfig::Stdio(StdioConfig {
                command: "echo".to_string(),
                args: vec!["hello".to_string()],
                cwd: None,
                env: HashMap::from([("TOKEN".to_string(), "super-secret".to_string())]),
                env_encrypted: HashMap::new(),
                startup_timeout_ms: 5000,
            }),
            request_timeout_ms: 5000,
            healthcheck_interval_ms: 1000,
            reconnect: Default::default(),
            allowed_tools: vec![],
            denied_tools: vec![],
        },
        McpServerConfig {
            id: "sse-secret".to_string(),
            name: Some("SSE Secret".to_string()),
            enabled: false,
            transport: TransportConfig::Sse(SseConfig {
                url: "http://localhost:9999/sse".to_string(),
                headers: vec![HeaderConfig {
                    name: "Authorization".to_string(),
                    value: "Bearer super-secret".to_string(),
                    value_encrypted: None,
                }],
                connect_timeout_ms: 1000,
            }),
            request_timeout_ms: 5000,
            healthcheck_interval_ms: 1000,
            reconnect: Default::default(),
            allowed_tools: vec![],
            denied_tools: vec![],
        },
    ];

    // Ensure encrypted-at-rest blobs exist on disk (what the settings endpoints round-trip).
    cfg.refresh_provider_api_keys_encrypted().unwrap();
    cfg.refresh_mcp_secrets_encrypted().unwrap();

    cfg.save_to_dir(temp_dir.to_path_buf()).unwrap();
    Config::from_data_dir(Some(temp_dir.to_path_buf()))
}

#[test]
fn update_provider_config_preserves_mcp_secrets() {
    let _key_guard = set_test_encryption_key([7u8; 32]);
    let temp_dir = tempfile::tempdir().unwrap();
    let current = build_config_with_mcp_secrets(temp_dir.path());

    // Mimic `update_provider_config`'s core flow (JSON round-trip + hydrate + save).
    let mut merged = serde_json::to_value(&current).unwrap();
    let patch = serde_json::json!({
        "provider": "openai",
        "providers": {
            "openai": {
                "api_key": "****...****",
                "model": "gpt-4o"
            }
        }
    });
    deep_merge_json(&mut merged, patch);

    let mut new_config: Config = serde_json::from_value(merged).unwrap();
    new_config.hydrate_proxy_auth_from_encrypted();
    new_config.hydrate_provider_api_keys_from_encrypted();
    new_config.hydrate_mcp_secrets_from_encrypted();

    // This is the critical part: if MCP secrets weren't hydrated, the save would
    // re-encrypt empty placeholders and permanently lose credentials.
    new_config
        .save_to_dir(temp_dir.path().to_path_buf())
        .unwrap();

    // Reload from disk and ensure secrets survive.
    let reloaded = Config::from_data_dir(Some(temp_dir.path().to_path_buf()));

    let stdio = reloaded
        .mcp
        .servers
        .iter()
        .find(|s| s.id == "stdio-secret")
        .unwrap();
    match &stdio.transport {
        TransportConfig::Stdio(stdio) => {
            assert_eq!(
                stdio.env.get("TOKEN").map(|v| v.as_str()),
                Some("super-secret")
            );
        }
        _ => panic!("expected stdio transport"),
    }

    let sse = reloaded
        .mcp
        .servers
        .iter()
        .find(|s| s.id == "sse-secret")
        .unwrap();
    match &sse.transport {
        TransportConfig::Sse(sse) => {
            let header = sse
                .headers
                .iter()
                .find(|h| h.name == "Authorization")
                .unwrap();
            assert_eq!(header.value.as_str(), "Bearer super-secret");
        }
        _ => panic!("expected sse transport"),
    }
}
