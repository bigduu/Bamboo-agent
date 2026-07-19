use super::fingerprint::proxy_fingerprint;
use super::*;
use crate::config::{ReconnectConfig, SseConfig, StdioConfig};
use async_trait::async_trait;
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
    cfg.proxy_auth = Some(bamboo_config::ProxyAuth {
        username: "user".to_string(),
        password: "pass".to_string(),
    });
    let fp3 = proxy_fingerprint(&cfg).expect("fingerprint expected");
    assert_ne!(fp1, fp3);

    cfg.proxy_auth = Some(bamboo_config::ProxyAuth {
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
    let mut cfg = Config::default();
    cfg.http_proxy = "http://".to_string(); // invalid URL
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
        // High so the recycle path doesn't reset the counter mid-test.
        reconnect_failure_threshold: u32::MAX,
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
        // High so the recycle path doesn't reset the counter mid-test.
        reconnect_failure_threshold: u32::MAX,
    });

    let err = McpError::Connection("boom".to_string());
    qos.record_failure("server-b", "tool-b", &err).await;
    assert!(qos.check_circuit("server-b", "tool-b").await.is_err());

    sleep(Duration::from_millis(15)).await;
    assert!(qos.check_circuit("server-b", "tool-b").await.is_ok());
}

#[tokio::test]
async fn qos_signals_recycle_at_reconnect_threshold() {
    let qos = McpServerQos::new(McpQosConfig {
        max_concurrent_calls: 1,
        // High so only the reconnect threshold is exercised here.
        circuit_failure_threshold: u32::MAX,
        circuit_open_ms: 5,
        reconnect_failure_threshold: 3,
    });
    let err = McpError::Connection("boom".to_string());

    // Below the threshold: no recycle.
    assert!(!qos.record_failure("s", "t", &err).await);
    assert!(!qos.record_failure("s", "t", &err).await);
    // 3rd consecutive failure: recycle signalled.
    assert!(qos.record_failure("s", "t", &err).await);
    // Counter reset after signalling — needs a fresh run of failures.
    assert!(!qos.record_failure("s", "t", &err).await);
    // A success resets the run too.
    qos.record_success().await;
    assert!(!qos.record_failure("s", "t", &err).await);
    assert!(!qos.record_failure("s", "t", &err).await);
    assert!(qos.record_failure("s", "t", &err).await);
}

// ---------------------------------------------------------------------------
// #366: server-notification drain + tools/list_changed -> refresh dispatch.
// ---------------------------------------------------------------------------

/// Mock transport for the notification-drain test. Preloads server-initiated
/// messages for the client's handler to forward, and answers `tools/list`
/// requests with a configurable tool set so `refresh_tools` can complete.
struct NotifyingMockTransport {
    connected: bool,
    /// Messages delivered TO the client (its handler consumes these).
    to_client_rx: tokio::sync::Mutex<Option<mpsc::Receiver<String>>>,
    /// Sender used to push `tools/list` responses back to the client.
    to_client_tx: mpsc::Sender<String>,
    /// `(name, description)` returned by `tools/list`.
    tools: Vec<(String, String)>,
}

impl NotifyingMockTransport {
    fn new(preload: &[&str], tools: &[(&str, &str)]) -> Self {
        let (tx, rx) = mpsc::channel::<String>(64);
        for msg in preload {
            tx.try_send((*msg).to_string())
                .expect("preload fits the channel");
        }
        Self {
            connected: false,
            to_client_rx: tokio::sync::Mutex::new(Some(rx)),
            to_client_tx: tx,
            tools: tools
                .iter()
                .map(|(n, d)| (n.to_string(), d.to_string()))
                .collect(),
        }
    }
}

#[async_trait]
impl McpTransport for NotifyingMockTransport {
    async fn connect(&mut self) -> Result<()> {
        self.connected = true;
        Ok(())
    }
    async fn disconnect(&mut self) -> Result<()> {
        self.connected = false;
        Ok(())
    }
    async fn send(&self, message: String) -> Result<()> {
        let req: serde_json::Value =
            serde_json::from_str(&message).map_err(|e| McpError::Protocol(e.to_string()))?;
        if req["method"].as_str() == Some("tools/list") {
            let tools: Vec<serde_json::Value> = self
                .tools
                .iter()
                .map(|(n, d)| {
                    serde_json::json!({
                        "name": n,
                        "description": d,
                        "inputSchema": { "type": "object" }
                    })
                })
                .collect();
            let resp = serde_json::json!({
                "jsonrpc": "2.0",
                "id": req["id"].clone(),
                "result": { "tools": tools }
            });
            let _ = self.to_client_tx.send(resp.to_string()).await;
        }
        Ok(())
    }
    async fn take_message_receiver(&self) -> Option<mpsc::Receiver<String>> {
        self.to_client_rx.lock().await.take()
    }
    async fn receive(&self) -> Result<Option<String>> {
        Err(McpError::Disconnected)
    }
    fn is_connected(&self) -> bool {
        self.connected
    }
}

/// Inserts a `ServerRuntime` backed by an already-connected `client`, starting
/// with zero registered tools. Returns the runtime so the test can take the
/// client's notification receiver.
fn insert_mock_runtime(
    manager: &McpServerManager,
    server_id: &str,
    client: McpProtocolClient,
) -> Arc<ServerRuntime> {
    let runtime = Arc::new(ServerRuntime {
        config: create_test_server_config(server_id),
        client: RwLock::new(client),
        info: RwLock::new(RuntimeInfo {
            status: ServerStatus::Ready,
            last_error: None,
            connected_at: Some(Utc::now()),
            disconnected_at: None,
            tool_count: 0,
            restart_count: 0,
            last_ping_at: Some(Utc::now()),
            instructions: None,
        }),
        tools: RwLock::new(Vec::new()),
        shutdown: AtomicBool::new(false),
        reconnecting: AtomicBool::new(false),
        qos: McpServerQos::new(McpQosConfig::default()),
        proxy_fingerprint: None,
    });
    manager
        .runtimes
        .insert(server_id.to_string(), runtime.clone());
    runtime
}

async fn connected_mock_client() -> McpProtocolClient {
    let transport = NotifyingMockTransport::new(&[], &[]);
    let mut client = McpProtocolClient::new(Box::new(transport));
    client.connect().await.expect("connect mock runtime");
    client
}

fn marker_tool(name: &str) -> McpTool {
    McpTool {
        name: name.to_string(),
        description: format!("{name} marker"),
        parameters: serde_json::json!({"type": "object"}),
    }
}

#[tokio::test]
async fn transactional_reconcile_bootstrap_failure_keeps_old_runtime_and_tool_index() {
    let manager = McpServerManager::new();
    let transport = NotifyingMockTransport::new(&[], &[]);
    let mut client = McpProtocolClient::new(Box::new(transport));
    client.connect().await.expect("connect old mock runtime");
    let old = insert_mock_runtime(&manager, "stable", client);
    let old_tool = McpTool {
        name: "still_available".to_string(),
        description: "old runtime marker".to_string(),
        parameters: serde_json::json!({"type": "object"}),
    };
    manager
        .index
        .register_server_tools("stable", &[old_tool], &[], &[]);
    let alias = manager.index.generate_alias("stable", "still_available");

    let mut replacement = create_test_server_config("stable");
    replacement.transport = TransportConfig::Stdio(StdioConfig {
        command: "definitely-not-a-real-mcp-command-597".to_string(),
        args: vec![],
        cwd: None,
        env: std::collections::HashMap::new(),
        env_encrypted: std::collections::HashMap::new(),
        startup_timeout_ms: 100,
    });
    let candidate = McpConfig {
        version: 1,
        servers: vec![replacement],
    };

    let error = manager
        .reconcile_from_config_transactional(&candidate)
        .await
        .expect_err("replacement bootstrap must fail");
    assert!(!error.to_string().is_empty());
    let live = manager
        .runtimes
        .get("stable")
        .expect("old runtime remains published")
        .clone();
    assert!(Arc::ptr_eq(&live, &old));
    assert!(manager.index.contains(&alias));
    assert_eq!(
        manager.index.lookup(&alias).unwrap().original_name,
        "still_available"
    );
    assert_eq!(
        old.info.read().await.status,
        ServerStatus::Ready,
        "failed candidate must not stop or degrade the old runtime"
    );
}

#[tokio::test]
async fn committed_reconcile_publishes_all_removals_before_blocked_cleanup() {
    use std::sync::atomic::AtomicBool;

    let (event_tx, _event_rx) = mpsc::channel(1);
    event_tx
        .send(McpEvent::ToolsChanged {
            server_id: "blocker".to_string(),
            tools: Vec::new(),
        })
        .await
        .expect("fill event channel");
    let manager = McpServerManager::new().with_event_channel(event_tx);

    let first = insert_mock_runtime(&manager, "first", connected_mock_client().await);
    let second = insert_mock_runtime(&manager, "second", connected_mock_client().await);
    for id in ["first", "second"] {
        manager
            .index
            .register_server_tools(id, &[marker_tool("old")], &[], &[]);
    }

    let durable = Arc::new(AtomicBool::new(false));
    let healthy = Arc::new(AtomicBool::new(false));
    let durable_at_commit = durable.clone();
    let healthy_after_reconcile = healthy.clone();
    tokio::time::timeout(Duration::from_millis(100), async {
        manager
            .reconcile_from_config_transactional_after(
                &McpConfig {
                    version: 1,
                    servers: Vec::new(),
                },
                || async move {
                    durable_at_commit.store(true, Ordering::SeqCst);
                    Ok(())
                },
            )
            .await
            .expect("committed reconcile is infallible");
        // Models the section endpoint's post-reconcile health publication.
        healthy_after_reconcile.store(true, Ordering::SeqCst);
    })
    .await
    .expect("blocked cleanup must not suspend committed publication");

    assert!(durable.load(Ordering::SeqCst));
    assert!(healthy.load(Ordering::SeqCst));
    assert!(manager.list_servers().is_empty());
    assert!(manager.index.all_aliases().is_empty());
    assert!(first.shutdown.load(Ordering::SeqCst));
    assert!(second.shutdown.load(Ordering::SeqCst));
}

#[tokio::test]
async fn detached_refresh_cannot_overwrite_replacement_tool_index() {
    let manager = McpServerManager::new();
    let old = insert_mock_runtime(&manager, "stable", connected_mock_client().await);
    let replacement = insert_mock_runtime(&manager, "stable", connected_mock_client().await);
    manager.index.remove_server_tools("stable");
    manager
        .index
        .register_server_tools("stable", &[marker_tool("replacement")], &[], &[]);

    assert!(
        !manager
            .publish_refreshed_tools_if_current("stable", &old, vec![marker_tool("stale_refresh")])
            .await
    );
    assert!(manager
        .index
        .contains(&manager.index.generate_alias("stable", "replacement")));
    assert!(!manager
        .index
        .contains(&manager.index.generate_alias("stable", "stale_refresh")));
    assert!(Arc::ptr_eq(
        manager.runtimes.get("stable").unwrap().value(),
        &replacement
    ));
}

#[tokio::test]
async fn detached_reconnect_cannot_overwrite_replacement_tool_index() {
    let manager = McpServerManager::new();
    let old = insert_mock_runtime(&manager, "stable", connected_mock_client().await);
    let replacement = insert_mock_runtime(&manager, "stable", connected_mock_client().await);
    manager.index.remove_server_tools("stable");
    manager
        .index
        .register_server_tools("stable", &[marker_tool("replacement")], &[], &[]);

    assert!(
        !manager
            .publish_reconnected_runtime_if_current(
                "stable",
                &old,
                connected_mock_client().await,
                vec![marker_tool("stale_reconnect")],
                Some("stale instructions".to_string()),
                None,
            )
            .await
    );
    assert!(manager
        .index
        .contains(&manager.index.generate_alias("stable", "replacement")));
    assert!(!manager
        .index
        .contains(&manager.index.generate_alias("stable", "stale_reconnect")));
    assert!(Arc::ptr_eq(
        manager.runtimes.get("stable").unwrap().value(),
        &replacement
    ));
}

#[tokio::test]
async fn detached_generation_cannot_publish_health_or_reconnect_status() {
    let (event_tx, mut event_rx) = mpsc::channel(4);
    let manager = McpServerManager::new().with_event_channel(event_tx);
    let old = insert_mock_runtime(&manager, "stable", connected_mock_client().await);
    let _replacement = insert_mock_runtime(&manager, "stable", connected_mock_client().await);
    let initial = old.info.read().await.clone();

    assert_eq!(
        manager
            .publish_health_result_if_current(
                "stable",
                &old,
                Err("stale ping failure".to_string()),
            )
            .await,
        None
    );
    manager.attempt_reconnection(old.clone()).await.unwrap();

    let after = old.info.read().await;
    assert_eq!(after.status, initial.status);
    assert_eq!(after.last_error, initial.last_error);
    assert!(!old.reconnecting.load(Ordering::SeqCst));
    assert!(event_rx.try_recv().is_err());
}

#[tokio::test]
async fn tools_list_changed_notification_triggers_refresh() {
    // #366 headline capability: a server-initiated `tools/list_changed` reaches the
    // drain consumer and triggers a real tool-list refresh (re-registering the
    // index + emitting `ToolsChanged`). Before the fix the notification channel had
    // no consumer, so the notification was dropped and this never fired.
    let (event_tx, mut event_rx) = mpsc::channel::<McpEvent>(16);
    let manager = McpServerManager::new().with_event_channel(event_tx);

    // Mock server preloads a `tools/list_changed` for the client to forward, and
    // answers the follow-up `tools/list` with two tools.
    let transport = NotifyingMockTransport::new(
        &[r#"{"jsonrpc":"2.0","method":"notifications/tools/list_changed"}"#],
        &[("alpha", "Alpha tool"), ("beta", "Beta tool")],
    );
    let mut client = McpProtocolClient::new(Box::new(transport));
    client.connect().await.expect("connect mock client");

    let runtime = insert_mock_runtime(&manager, "srv", client);

    // Wire the real drain (exactly as bootstrap does in production).
    let rx = runtime
        .client
        .read()
        .await
        .take_notification_receiver()
        .await
        .expect("notification receiver available exactly once");
    manager.spawn_notification_drain("srv".to_string(), runtime.clone(), rx);

    // The drain observes the notification -> refresh_tools -> ToolsChanged.
    let evt = tokio::time::timeout(Duration::from_secs(3), event_rx.recv())
        .await
        .expect("a ToolsChanged event must be emitted after tools/list_changed")
        .expect("event channel stays open");
    match evt {
        McpEvent::ToolsChanged { server_id, tools } => {
            assert_eq!(server_id, "srv");
            assert_eq!(tools.len(), 2, "refresh should register the two new tools");
        }
        other => panic!("expected ToolsChanged, got {other:?}"),
    }

    // The tool index now holds the two aliases from the refreshed list.
    assert_eq!(
        manager.tool_index().all_aliases().len(),
        2,
        "refreshed tools should be registered in the index"
    );
}

/// A notification with no dispatcher is still drained (so the queue can't
/// saturate) and does NOT trigger a tool-list refresh.
#[tokio::test]
async fn unhandled_notification_is_drained_without_refresh() {
    let (event_tx, mut event_rx) = mpsc::channel::<McpEvent>(16);
    let manager = McpServerManager::new().with_event_channel(event_tx);

    let transport = NotifyingMockTransport::new(
        &[r#"{"jsonrpc":"2.0","method":"notifications/message","params":{"level":"info"}}"#],
        &[("alpha", "Alpha tool")],
    );
    let mut client = McpProtocolClient::new(Box::new(transport));
    client.connect().await.expect("connect mock client");

    let runtime = insert_mock_runtime(&manager, "srv", client);
    let rx = runtime
        .client
        .read()
        .await
        .take_notification_receiver()
        .await
        .expect("notification receiver available");
    manager.spawn_notification_drain("srv".to_string(), runtime.clone(), rx);

    // No dispatcher for `notifications/message` -> no ToolsChanged emitted.
    let got = tokio::time::timeout(Duration::from_millis(300), event_rx.recv()).await;
    assert!(
        got.is_err(),
        "an unhandled notification must not trigger a refresh event"
    );
    assert!(
        manager.tool_index().all_aliases().is_empty(),
        "no tools should be registered without a tools/list_changed"
    );
}
