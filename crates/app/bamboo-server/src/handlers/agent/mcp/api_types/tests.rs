use std::collections::HashMap;

use bamboo_mcp::{
    HeaderConfig, McpServerConfig, ReconnectConfig, StdioConfig, StreamableHttpConfig,
    TransportConfig,
};

use super::{MainstreamServerRequest, TransportConfigApi};

#[test]
fn into_internal_builds_stdio_server_with_defaults() {
    let request = MainstreamServerRequest {
        id: "server-a".to_string(),
        name: Some("Server A".to_string()),
        enabled: Some(true),
        disabled: true,
        command: Some("npx".to_string()),
        args: vec!["-y".to_string(), "example-mcp".to_string()],
        cwd: Some("/tmp".to_string()),
        env: HashMap::from([("API_KEY".to_string(), "secret".to_string())]),
        env_encrypted: HashMap::new(),
        startup_timeout_ms: None,
        url: None,
        headers: Vec::new(),
        connect_timeout_ms: None,
        transport_kind: None,
        request_timeout_ms: None,
        healthcheck_interval_ms: None,
        reconnect: None,
        allowed_tools: vec!["read".to_string()],
        denied_tools: vec!["write".to_string()],
    };

    let config = request
        .into_internal(None)
        .expect("stdio request should convert");

    assert!(config.enabled);
    assert_eq!(config.id, "server-a");
    assert_eq!(config.allowed_tools, vec!["read"]);
    assert_eq!(config.denied_tools, vec!["write"]);
    match config.transport {
        TransportConfig::Stdio(stdio) => {
            assert_eq!(stdio.command, "npx");
            assert_eq!(stdio.args, vec!["-y", "example-mcp"]);
            assert_eq!(stdio.cwd.as_deref(), Some("/tmp"));
            assert_eq!(stdio.env.get("API_KEY").map(String::as_str), Some("secret"));
        }
        _ => panic!("expected stdio transport"),
    }
}

#[test]
fn into_internal_rejects_conflicting_transport_fields() {
    let request = MainstreamServerRequest {
        id: "server-b".to_string(),
        name: None,
        enabled: None,
        disabled: false,
        command: Some("cmd".to_string()),
        args: Vec::new(),
        cwd: None,
        env: HashMap::new(),
        env_encrypted: HashMap::new(),
        startup_timeout_ms: None,
        url: Some("http://localhost:3000/mcp".to_string()),
        headers: Vec::new(),
        connect_timeout_ms: None,
        transport_kind: None,
        request_timeout_ms: None,
        healthcheck_interval_ms: None,
        reconnect: None,
        allowed_tools: Vec::new(),
        denied_tools: Vec::new(),
    };

    let error = request
        .into_internal(None)
        .expect_err("command+url should fail");
    assert!(error.contains("cannot contain both 'command' and 'url'"));
}

#[test]
fn to_api_config_masks_stdio_env_values() {
    let config = McpServerConfig {
        id: "stdio-server".to_string(),
        name: Some("Stdio Server".to_string()),
        enabled: true,
        transport: TransportConfig::Stdio(StdioConfig {
            command: "node".to_string(),
            args: vec!["server.js".to_string()],
            cwd: None,
            env: HashMap::from([("TOKEN".to_string(), "plaintext".to_string())]),
            env_encrypted: HashMap::from([("SECRET".to_string(), "cipher".to_string())]),
            env_credential_refs: std::collections::HashMap::new(),
            startup_timeout_ms: 5000,
        }),
        request_timeout_ms: 60000,
        healthcheck_interval_ms: 30000,
        reconnect: ReconnectConfig::default(),
        allowed_tools: Vec::new(),
        denied_tools: Vec::new(),
    };

    let api_config = super::to_api_config(&config);
    match api_config.transport {
        TransportConfigApi::Stdio { env, .. } => {
            assert_eq!(env.get("TOKEN").map(String::as_str), Some("****...****"));
            assert_eq!(env.get("SECRET").map(String::as_str), Some("****...****"));
        }
        _ => panic!("expected stdio transport"),
    }
}

#[test]
fn to_api_config_masks_http_header_values() {
    let config = McpServerConfig {
        id: "http-server".to_string(),
        name: None,
        enabled: true,
        transport: TransportConfig::StreamableHttp(StreamableHttpConfig {
            url: "http://localhost:3000/mcp".to_string(),
            headers: vec![HeaderConfig {
                name: "Authorization".to_string(),
                value: "Bearer abc".to_string(),
                value_encrypted: None,
                credential_ref: None,
            }],
            connect_timeout_ms: 4000,
        }),
        request_timeout_ms: 60000,
        healthcheck_interval_ms: 30000,
        reconnect: ReconnectConfig::default(),
        allowed_tools: Vec::new(),
        denied_tools: Vec::new(),
    };

    let api_config = super::to_api_config(&config);
    match api_config.transport {
        TransportConfigApi::StreamableHttp { headers, .. } => {
            assert_eq!(headers.len(), 1);
            assert_eq!(headers[0].name, "Authorization");
            assert_eq!(headers[0].value, "****...****");
        }
        _ => panic!("expected Streamable HTTP transport"),
    }
}

#[test]
fn mainstream_url_defaults_to_canonical_streamable_http() {
    let request: MainstreamServerRequest = serde_json::from_value(
        serde_json::json!({"id": "remote", "url": "https://example.test/mcp"}),
    )
    .unwrap();
    let config = request.into_internal(None).unwrap();
    let api = serde_json::to_value(super::to_api_config(&config)).unwrap();
    assert_eq!(api["transport"]["type"], "streamable_http");
    assert_eq!(api["transport"]["url"], "https://example.test/mcp");
}

#[test]
fn api_rejects_retired_sse_without_mainstream_fallback() {
    for body in [
        serde_json::json!({"id": "remote", "transport": {"type": "sse", "url": "https://example.test/sse"}}),
        serde_json::json!({"id": "remote", "transport": {"type": "sse", "url": "https://example.test/sse"}, "url": "https://example.test/mcp"}),
    ] {
        assert!(serde_json::from_value::<super::ServerRequest>(body).is_err());
    }
    let request: MainstreamServerRequest = serde_json::from_value(serde_json::json!({"id": "remote", "url": "https://example.test/sse", "transport_kind": "sse"})).unwrap();
    assert!(request.into_internal(None).is_err());
}
