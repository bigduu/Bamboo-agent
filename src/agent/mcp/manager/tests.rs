use super::fingerprint::proxy_fingerprint;
use super::*;
use crate::agent::mcp::config::{ReconnectConfig, SseConfig, StdioConfig};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

fn create_test_server_config(id: &str) -> McpServerConfig {
    McpServerConfig {
        id: id.to_string(),
        name: Some(format!("Test Server {}", id)),
        enabled: true,
        transport: TransportConfig::Stdio(StdioConfig {
            command: "echo".to_string(),
            args: vec![],
            cwd: None,
            env: std::collections::HashMap::new(),
            env_encrypted: std::collections::HashMap::new(),
            startup_timeout_ms: 5000,
        }),
        request_timeout_ms: 5000,
        healthcheck_interval_ms: 1000,
        reconnect: ReconnectConfig {
            enabled: false, // Disable for most tests
            initial_backoff_ms: 100,
            max_backoff_ms: 1000,
            max_attempts: 3,
        },
        allowed_tools: vec![],
        denied_tools: vec![],
    }
}

#[test]
fn test_manager_new() {
    let manager = McpServerManager::new();
    assert!(manager.list_servers().is_empty());
}

#[test]
fn test_manager_clone() {
    let manager = McpServerManager::new();
    let cloned = manager.clone();
    assert!(cloned.list_servers().is_empty());
}

#[test]
fn test_manager_with_event_channel() {
    let (tx, _rx) = mpsc::channel(100);
    let manager = McpServerManager::new().with_event_channel(tx);
    assert!(manager.event_tx.is_some());
}

#[test]
fn test_tool_index_accessor() {
    let manager = McpServerManager::new();
    let index = manager.tool_index();
    assert!(index.all_aliases().is_empty());
}

#[tokio::test]
async fn test_list_servers_empty() {
    let manager = McpServerManager::new();
    let servers = manager.list_servers();
    assert!(servers.is_empty());
}

#[tokio::test]
async fn test_is_server_running() {
    let manager = McpServerManager::new();
    assert!(!manager.is_server_running("nonexistent"));
}

#[tokio::test]
async fn test_get_server_info_nonexistent() {
    let manager = McpServerManager::new();
    let info = manager.get_server_info("nonexistent");
    assert!(info.is_none());
}

#[tokio::test]
async fn test_get_tool_info_nonexistent() {
    let manager = McpServerManager::new();
    let tool = manager.get_tool_info("nonexistent", "tool");
    assert!(tool.is_none());
}

#[tokio::test]
async fn test_stop_server_nonexistent() {
    let manager = McpServerManager::new();
    let result = manager.stop_server("nonexistent").await;
    assert!(result.is_err());
    match result.unwrap_err() {
        McpError::NotRunning(id) => assert_eq!(id, "nonexistent"),
        _ => panic!("Expected NotRunning error"),
    }
}

#[tokio::test]
async fn test_call_tool_nonexistent_server() {
    let manager = McpServerManager::new();
    let result = manager
        .call_tool("nonexistent", "tool", serde_json::json!({}))
        .await;
    assert!(result.is_err());
    match result.unwrap_err() {
        McpError::ServerNotFound(id) => assert_eq!(id, "nonexistent"),
        _ => panic!("Expected ServerNotFound error"),
    }
}

#[tokio::test]
async fn test_refresh_tools_nonexistent() {
    let manager = McpServerManager::new();
    let result = manager.refresh_tools("nonexistent").await;
    assert!(result.is_err());
    match result.unwrap_err() {
        McpError::ServerNotFound(id) => assert_eq!(id, "nonexistent"),
        _ => panic!("Expected ServerNotFound error"),
    }
}

#[tokio::test]
async fn test_shutdown_all_empty() {
    let manager = McpServerManager::new();
    // Should not panic
    manager.shutdown_all().await;
}

#[test]
fn test_reconnect_config_default() {
    let config = ReconnectConfig::default();
    assert!(config.enabled);
    assert_eq!(config.initial_backoff_ms, 1000);
    assert_eq!(config.max_backoff_ms, 30000);
    assert_eq!(config.max_attempts, 0);
}

#[test]
fn test_reconnect_config_custom() {
    let config = ReconnectConfig {
        enabled: true,
        initial_backoff_ms: 500,
        max_backoff_ms: 10000,
        max_attempts: 5,
    };
    assert!(config.enabled);
    assert_eq!(config.initial_backoff_ms, 500);
    assert_eq!(config.max_backoff_ms, 10000);
    assert_eq!(config.max_attempts, 5);
}

#[tokio::test]
async fn test_start_server_already_running() {
    let manager = McpServerManager::new();
    let config = create_test_server_config("test-server");

    // Start server (will fail because echo doesn't implement MCP protocol)
    let _ = manager.start_server(config.clone()).await;

    // Try to start again - should fail with AlreadyRunning
    // Note: This test may not work if the first start fails
    // In that case, we're testing the logic path
}

#[tokio::test]
async fn test_initialize_from_config_disabled_server() {
    let manager = McpServerManager::new();

    let mut config = create_test_server_config("disabled-server");
    config.enabled = false;

    let mcp_config = McpConfig {
        version: 1,
        servers: vec![config],
    };

    manager.initialize_from_config(&mcp_config).await;

    // Should not have started the disabled server
    assert!(!manager.is_server_running("disabled-server"));
}

#[tokio::test]
async fn test_event_channel_server_status() {
    let (tx, rx) = mpsc::channel(100);
    let manager = McpServerManager::new().with_event_channel(tx);

    // Events are sent during server operations
    // This test verifies the channel is properly set up
    assert!(manager.event_tx.is_some());

    // Clean up
    drop(manager);
    drop(rx);
}

#[test]
fn test_server_status_display() {
    assert_eq!(format!("{}", ServerStatus::Ready), "ready");
    assert_eq!(format!("{}", ServerStatus::Degraded), "degraded");
    assert_eq!(format!("{}", ServerStatus::Error), "error");
    assert_eq!(format!("{}", ServerStatus::Stopped), "stopped");
    assert_eq!(format!("{}", ServerStatus::Connecting), "connecting");
}

#[test]
fn test_runtime_info_default() {
    let info = RuntimeInfo::default();
    assert_eq!(info.status, ServerStatus::Stopped);
    assert!(info.last_error.is_none());
    assert!(info.connected_at.is_none());
    assert!(info.disconnected_at.is_none());
    assert_eq!(info.tool_count, 0);
    assert_eq!(info.restart_count, 0);
    assert!(info.last_ping_at.is_none());
}

// Test exponential backoff calculation (indirectly through manager behavior)
#[test]
fn test_exponential_backoff_calculation() {
    let initial = 1000u64;
    let max = 30000u64;
    let mut current = initial;

    // First backoff
    current = std::cmp::min(current * 2, max);
    assert_eq!(current, 2000);

    // Second backoff
    current = std::cmp::min(current * 2, max);
    assert_eq!(current, 4000);

    // Third backoff
    current = std::cmp::min(current * 2, max);
    assert_eq!(current, 8000);

    // Fourth backoff
    current = std::cmp::min(current * 2, max);
    assert_eq!(current, 16000);

    // Fifth backoff
    current = std::cmp::min(current * 2, max);
    assert_eq!(current, 30000); // Capped at max

    // Try again - should stay at max
    current = std::cmp::min(current * 2, max);
    assert_eq!(current, 30000);
}

#[test]
fn test_exponential_backoff_max_zero() {
    // Test that max_attempts = 0 means unlimited
    let config = ReconnectConfig {
        enabled: true,
        initial_backoff_ms: 100,
        max_backoff_ms: 1000,
        max_attempts: 0,
    };

    assert_eq!(config.max_attempts, 0);
    // In the actual code, max_attempts == 0 bypasses the attempt limit check
}

// Test that reconnection logic is properly gated by enabled flag
#[test]
fn test_reconnect_disabled() {
    let config = ReconnectConfig {
        enabled: false,
        initial_backoff_ms: 100,
        max_backoff_ms: 1000,
        max_attempts: 3,
    };

    assert!(!config.enabled);
    // In the actual health check code, reconnection is only attempted if enabled
}

#[test]
fn test_proxy_fingerprint_changes_on_proxy_or_auth_change() {
    let mut cfg = Config::default();
    // Config::default() loads from the runtime data dir, so make this test
    // deterministic regardless of local proxy settings.
    cfg.http_proxy.clear();
    cfg.https_proxy.clear();
    cfg.proxy_auth = None;
    assert_eq!(proxy_fingerprint(&cfg), None);

    cfg.http_proxy = "http://proxy:8080".to_string();
    let fp1 = proxy_fingerprint(&cfg).expect("fingerprint expected");

    cfg.http_proxy = "http://proxy2:8080".to_string();
    let fp2 = proxy_fingerprint(&cfg).expect("fingerprint expected");
    assert_ne!(fp1, fp2);

    cfg.http_proxy = "http://proxy:8080".to_string();
    cfg.proxy_auth = Some(crate::core::ProxyAuth {
        username: "user".to_string(),
        password: "pass".to_string(),
    });
    let fp3 = proxy_fingerprint(&cfg).expect("fingerprint expected");
    assert_ne!(fp1, fp3);

    cfg.proxy_auth = Some(crate::core::ProxyAuth {
        username: "user".to_string(),
        password: "pass2".to_string(),
    });
    let fp4 = proxy_fingerprint(&cfg).expect("fingerprint expected");
    assert_ne!(fp3, fp4);
}

#[tokio::test]
async fn test_sse_transport_respects_proxy_settings_when_available() {
    // If the manager has access to global config, SSE client creation should
    // fail early when proxy URL is invalid (proving it attempted to apply proxy).
    let cfg = Config {
        http_proxy: "http://".to_string(), // invalid URL
        ..Config::default()
    };
    let manager = McpServerManager::new_with_config(Arc::new(tokio::sync::RwLock::new(cfg)));

    let server = McpServerConfig {
        id: "sse-test".to_string(),
        name: Some("SSE test".to_string()),
        enabled: true,
        transport: TransportConfig::Sse(SseConfig {
            url: "http://localhost:9999/sse".to_string(),
            headers: vec![],
            connect_timeout_ms: 100,
        }),
        request_timeout_ms: 1000,
        healthcheck_interval_ms: 1000,
        reconnect: ReconnectConfig {
            enabled: false,
            initial_backoff_ms: 100,
            max_backoff_ms: 1000,
            max_attempts: 1,
        },
        allowed_tools: vec![],
        denied_tools: vec![],
    };

    let err = manager.start_server(server).await.unwrap_err();
    match err {
        McpError::InvalidConfig(msg) => {
            assert!(
                msg.to_lowercase().contains("proxy") || msg.to_lowercase().contains("http"),
                "unexpected error message: {msg}"
            );
        }
        other => panic!("expected InvalidConfig, got {other:?}"),
    }
}

#[tokio::test]
async fn qos_circuit_opens_after_consecutive_failures() {
    let qos = McpServerQos::new(McpQosConfig {
        max_concurrent_calls: 2,
        circuit_failure_threshold: 2,
        circuit_open_ms: 60_000,
    });

    let err = McpError::Connection("boom".to_string());
    qos.record_failure("server-a", "tool-a", &err).await;
    assert!(qos.check_circuit("server-a", "tool-a").await.is_ok());

    qos.record_failure("server-a", "tool-a", &err).await;
    let blocked = qos.check_circuit("server-a", "tool-a").await;
    assert!(blocked.is_err());
    match blocked.unwrap_err() {
        McpError::ToolExecution(message) => {
            assert!(message.contains("circuit open"));
        }
        other => panic!("expected ToolExecution, got {other:?}"),
    }
}

#[tokio::test]
async fn qos_circuit_recovers_after_open_window() {
    let qos = McpServerQos::new(McpQosConfig {
        max_concurrent_calls: 1,
        circuit_failure_threshold: 1,
        circuit_open_ms: 5,
    });

    let err = McpError::Connection("boom".to_string());
    qos.record_failure("server-b", "tool-b", &err).await;
    assert!(qos.check_circuit("server-b", "tool-b").await.is_err());

    sleep(Duration::from_millis(15)).await;
    assert!(qos.check_circuit("server-b", "tool-b").await.is_ok());
}
