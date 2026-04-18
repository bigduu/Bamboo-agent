use std::collections::{BTreeMap, HashMap};

use serde_json::json;

use bamboo_infrastructure_mcp::{McpServerConfig, StdioConfig, TransportConfig};
use bamboo_infrastructure_config::{Config, OpenAIConfig, ProviderConfigs};

use super::{redact_config_for_api, redact_providers_for_api};

fn config_with_openai_key() -> Config {
    let mut config = Config::default();
    config.providers = ProviderConfigs {
        openai: Some(OpenAIConfig {
            api_key: String::new(),
            api_key_encrypted: Some("enc-key".to_string()),
            base_url: None,
            model: None,
            fast_model: None,
            vision_model: None,
            reasoning_effort: None,
            responses_only_models: vec![],
            request_overrides: None,
            extra: BTreeMap::new(),
        }),
        ..ProviderConfigs::default()
    };
    config
}

#[test]
fn redact_config_masks_configured_provider_and_removes_proxy_encrypted_keys() {
    let config = config_with_openai_key();
    let input = json!({
        "proxy_auth_encrypted": "encrypted",
        "http_proxy_auth_encrypted": "legacy-http",
        "https_proxy_auth_encrypted": "legacy-https",
        "providers": {
            "openai": {
                "api_key": "sk-test",
                "api_key_encrypted": "enc-key"
            }
        }
    });

    let redacted = redact_config_for_api(input, &config);

    assert!(redacted.get("proxy_auth_encrypted").is_none());
    assert!(redacted.get("http_proxy_auth_encrypted").is_none());
    assert!(redacted.get("https_proxy_auth_encrypted").is_none());
    assert_eq!(redacted["providers"]["openai"]["api_key"], "****...****");
    assert!(redacted["providers"]["openai"]
        .as_object()
        .is_some_and(|obj| !obj.contains_key("api_key_encrypted")));
}

#[test]
fn redact_providers_removes_unconfigured_api_key_fields() {
    let mut config = Config::default();
    config.providers = ProviderConfigs::default();
    let input = json!({
        "openai": {
            "api_key": "sk-test",
            "api_key_encrypted": "enc-key"
        }
    });

    let redacted = redact_providers_for_api(input, &config);
    assert!(redacted["openai"]
        .as_object()
        .is_some_and(|obj| !obj.contains_key("api_key")));
    assert!(redacted["openai"]
        .as_object()
        .is_some_and(|obj| !obj.contains_key("api_key_encrypted")));
}

#[test]
fn redact_config_masks_mcp_servers_map_secrets() {
    let config = Config::default();
    let input = json!({
        "mcpServers": {
            "stdio-server": {
                "command": "node",
                "env": { "TOKEN": "super-secret" },
                "env_encrypted": { "TOKEN": "enc-value" }
            },
            "sse-server": {
                "url": "http://localhost:1234/sse",
                "headers": { "Authorization": "Bearer super-secret" }
            }
        }
    });

    let redacted = redact_config_for_api(input, &config);

    assert_eq!(
        redacted["mcpServers"]["stdio-server"]["env"]["TOKEN"],
        "****...****"
    );
    assert!(redacted["mcpServers"]["stdio-server"]
        .as_object()
        .is_some_and(|obj| !obj.contains_key("env_encrypted")));
    assert_eq!(
        redacted["mcpServers"]["sse-server"]["headers"]["Authorization"],
        "****...****"
    );
}

#[test]
fn redact_config_populates_env_from_encrypted_keys_when_env_missing() {
    let config = Config::default();
    let input = json!({
        "mcpServers": {
            "stdio-server": {
                "command": "node",
                "env_encrypted": {
                    "TOKEN": "enc-value",
                    "API_KEY": "enc-value-2"
                }
            }
        }
    });

    let redacted = redact_config_for_api(input, &config);
    let env = redacted["mcpServers"]["stdio-server"]["env"]
        .as_object()
        .expect("env should exist");
    assert_eq!(env["TOKEN"], "****...****");
    assert_eq!(env["API_KEY"], "****...****");
}

#[test]
fn redact_config_redacts_legacy_mcp_and_falls_back_to_runtime_env_keys() {
    let mut config = Config::default();
    config.mcp.servers.push(McpServerConfig {
        id: "legacy-stdio".to_string(),
        name: None,
        enabled: true,
        transport: TransportConfig::Stdio(StdioConfig {
            command: "echo".to_string(),
            args: vec!["hello".to_string()],
            cwd: None,
            env: HashMap::from([("TOKEN".to_string(), "runtime-secret".to_string())]),
            env_encrypted: HashMap::new(),
            startup_timeout_ms: 5_000,
        }),
        request_timeout_ms: 5_000,
        healthcheck_interval_ms: 1_000,
        reconnect: Default::default(),
        allowed_tools: vec![],
        denied_tools: vec![],
    });

    let input = json!({
        "mcp": {
            "servers": [
                {
                    "id": "legacy-stdio",
                    "transport": {
                        "type": "stdio"
                    }
                },
                {
                    "id": "legacy-sse",
                    "transport": {
                        "type": "sse",
                        "headers": [
                            {
                                "name": "Authorization",
                                "value": "Bearer test",
                                "value_encrypted": "enc-value"
                            }
                        ]
                    }
                }
            ]
        }
    });

    let redacted = redact_config_for_api(input, &config);
    let servers = redacted["mcp"]["servers"]
        .as_array()
        .expect("legacy servers should be array");

    let stdio_transport = servers[0]["transport"]
        .as_object()
        .expect("stdio transport should be object");
    assert_eq!(stdio_transport["env"]["TOKEN"], "****...****");
    assert!(!stdio_transport.contains_key("env_encrypted"));

    let sse_header = servers[1]["transport"]["headers"][0]
        .as_object()
        .expect("header should be object");
    assert_eq!(sse_header["value"], "****...****");
    assert!(!sse_header.contains_key("value_encrypted"));
}

// ── Env vars redaction tests ──────────────────────────────

#[test]
fn redact_config_masks_secret_env_var_values() {
    let config = Config::default();
    let input = json!({
        "env_vars": [
            {
                "name": "PLAIN",
                "value": "visible-value",
                "secret": false,
                "value_encrypted": null,
                "description": "A plain var"
            },
            {
                "name": "TOKEN",
                "value": "super-secret",
                "secret": true,
                "value_encrypted": "enc123",
                "description": null
            }
        ]
    });

    let redacted = redact_config_for_api(input, &config);
    let env_vars = redacted["env_vars"].as_array().unwrap();

    // Plain var: value visible, no encrypted field
    assert_eq!(env_vars[0]["name"], "PLAIN");
    assert_eq!(env_vars[0]["value"], "visible-value");
    assert!(env_vars[0].get("value_encrypted").is_none());

    // Secret var: value masked, encrypted removed
    assert_eq!(env_vars[1]["name"], "TOKEN");
    assert_eq!(env_vars[1]["value"], "****...****");
    assert!(env_vars[1].get("value_encrypted").is_none());
}

#[test]
fn redact_config_removes_value_encrypted_from_non_secret_entries() {
    let config = Config::default();
    let input = json!({
        "env_vars": [
            {
                "name": "X",
                "value": "hello",
                "secret": false,
                "value_encrypted": "should-be-removed"
            }
        ]
    });

    let redacted = redact_config_for_api(input, &config);
    let env_vars = redacted["env_vars"].as_array().unwrap();
    assert_eq!(env_vars[0]["value"], "hello");
    assert!(env_vars[0].get("value_encrypted").is_none());
}

#[test]
fn redact_config_handles_missing_env_vars() {
    let config = Config::default();
    let input = json!({
        "some_field": "value"
    });

    // Should not panic
    let redacted = redact_config_for_api(input.clone(), &config);
    assert_eq!(redacted["some_field"], "value");
}

#[test]
fn redact_config_handles_empty_env_vars() {
    let config = Config::default();
    let input = json!({
        "env_vars": []
    });

    let redacted = redact_config_for_api(input, &config);
    let env_vars = redacted["env_vars"].as_array().unwrap();
    assert!(env_vars.is_empty());
}

#[test]
fn redact_config_defaults_secret_false_when_missing() {
    let config = Config::default();
    // Entry without explicit "secret" field should be treated as non-secret
    let input = json!({
        "env_vars": [
            {
                "name": "MISSING_SECRET_FIELD",
                "value": "plaintext",
                "value_encrypted": "enc"
            }
        ]
    });

    let redacted = redact_config_for_api(input, &config);
    let env_vars = redacted["env_vars"].as_array().unwrap();
    // Without secret=true, value stays as-is, but encrypted is still removed
    assert_eq!(env_vars[0]["value"], "plaintext");
    assert!(env_vars[0].get("value_encrypted").is_none());
}
