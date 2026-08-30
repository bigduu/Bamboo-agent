use super::fingerprint::proxy_fingerprint;
use super::*;
use crate::config::{ReconnectConfig, SseConfig, StdioConfig};
use crate::error::ToolRegistrationError;
use crate::executor::McpToolExecutor;
use crate::manager::generation::FenceState;
use crate::protocol::models::JsonRpcNotification;
use async_trait::async_trait;
use bamboo_agent_core::{FunctionCall, ToolCall, ToolExecutor};
use bamboo_domain::ClassifiedToolIdentity;
use std::collections::{BTreeSet, HashMap};
use std::future::pending;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{Barrier, Mutex as StdMutex};
use tokio::sync::{mpsc, Semaphore};
use tokio::time::{sleep, Duration};

fn test_config(id: &str) -> McpServerConfig {
    McpServerConfig {
        id: id.to_string(),
        name: Some(format!("Test {id}")),
        enabled: true,
        transport: TransportConfig::Stdio(StdioConfig {
            command: "echo".to_string(),
            args: Vec::new(),
            cwd: None,
            env: HashMap::new(),
            env_encrypted: HashMap::new(),
            env_credential_refs: HashMap::new(),
            startup_timeout_ms: 2_000,
        }),
        request_timeout_ms: 5_000,
        healthcheck_interval_ms: 60_000,
        reconnect: ReconnectConfig {
            enabled: false,
            initial_backoff_ms: 1,
            max_backoff_ms: 2,
            max_attempts: 1,
        },
        allowed_tools: Vec::new(),
        denied_tools: Vec::new(),
    }
}

fn tool(name: &str, description: &str) -> McpTool {
    McpTool {
        name: name.to_string(),
        description: description.to_string(),
        parameters: serde_json::json!({"type": "object"}),
        output_schema: None,
    }
}

struct MockTransport {
    connected: AtomicBool,
    message_rx: tokio::sync::Mutex<Option<mpsc::Receiver<String>>>,
    message_tx: mpsc::Sender<String>,
    tools: Arc<StdMutex<Vec<McpTool>>>,
    marker: String,
    call_started: Option<mpsc::UnboundedSender<()>>,
    call_release: Option<Arc<Semaphore>>,
    call_is_error: bool,
    calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

struct MockControls {
    tools: Arc<StdMutex<Vec<McpTool>>>,
    message_tx: mpsc::Sender<String>,
    call_started: Option<mpsc::UnboundedReceiver<()>>,
    call_release: Option<Arc<Semaphore>>,
    calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

impl MockTransport {
    fn new(tools: Vec<McpTool>, marker: &str) -> (Self, MockControls) {
        Self::new_with_call_gate(tools, marker, false, false)
    }

    fn new_with_call_gate(
        tools: Vec<McpTool>,
        marker: &str,
        block_call: bool,
        call_is_error: bool,
    ) -> (Self, MockControls) {
        let (message_tx, message_rx) = mpsc::channel(64);
        let tools = Arc::new(StdMutex::new(tools));
        let calls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let (call_started_tx, call_started_rx) = block_call
            .then(mpsc::unbounded_channel)
            .map_or((None, None), |(tx, rx)| (Some(tx), Some(rx)));
        let call_release = block_call.then(|| Arc::new(Semaphore::new(0)));
        (
            Self {
                connected: AtomicBool::new(false),
                message_rx: tokio::sync::Mutex::new(Some(message_rx)),
                message_tx: message_tx.clone(),
                tools: tools.clone(),
                marker: marker.to_string(),
                call_started: call_started_tx,
                call_release: call_release.clone(),
                call_is_error,
                calls: calls.clone(),
                drops: drops.clone(),
            },
            MockControls {
                tools,
                message_tx,
                call_started: call_started_rx,
                call_release,
                calls,
                drops,
            },
        )
    }

    async fn respond(&self, request: &serde_json::Value, result: serde_json::Value) {
        let response = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request["id"].clone(),
            "result": result,
        });
        let _ = self.message_tx.send(response.to_string()).await;
    }
}

impl Drop for MockTransport {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl McpTransport for MockTransport {
    async fn connect(&mut self) -> Result<()> {
        self.connected.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.connected.store(false, Ordering::SeqCst);
        Ok(())
    }

    async fn send(&self, message: String) -> Result<()> {
        let request: serde_json::Value = serde_json::from_str(&message)?;
        match request["method"].as_str() {
            Some("tools/list") => {
                let tools = self
                    .tools
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .iter()
                    .map(|tool| {
                        serde_json::json!({
                            "name": tool.name,
                            "description": tool.description,
                            "inputSchema": tool.parameters,
                        })
                    })
                    .collect::<Vec<_>>();
                self.respond(&request, serde_json::json!({"tools": tools}))
                    .await;
            }
            Some("tools/call") => {
                self.calls.fetch_add(1, Ordering::SeqCst);
                if let Some(started) = &self.call_started {
                    let _ = started.send(());
                }
                if let Some(release) = &self.call_release {
                    let permit = release.acquire().await.expect("call gate stays open");
                    permit.forget();
                }
                self.respond(
                    &request,
                    serde_json::json!({
                        "content": [{"type": "text", "text": self.marker}],
                        "isError": self.call_is_error,
                    }),
                )
                .await;
            }
            Some("ping") => self.respond(&request, serde_json::json!({})).await,
            _ => self.respond(&request, serde_json::json!({})).await,
        }
        Ok(())
    }

    async fn take_message_receiver(&self) -> Option<mpsc::Receiver<String>> {
        self.message_rx.lock().await.take()
    }

    async fn receive(&self) -> Result<Option<String>> {
        Err(McpError::Disconnected)
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }
}

async fn prepare_mock(
    manager: &McpServerManager,
    server_id: &str,
    tools: Vec<McpTool>,
    transport: MockTransport,
) -> PreparedServerRuntime {
    prepare_mock_with_config(manager, test_config(server_id), tools, transport).await
}

async fn prepare_mock_with_config(
    manager: &McpServerManager,
    config: McpServerConfig,
    tools: Vec<McpTool>,
    transport: MockTransport,
) -> PreparedServerRuntime {
    let mut client = McpProtocolClient::new(Box::new(transport));
    client.connect().await.expect("connect mock transport");
    let notification_rx = client.take_notification_receiver().await;
    let server_id = config.id.clone();
    let catalog = manager
        .index
        .plan_server_tools(&server_id, &tools, &[], &[])
        .expect("valid mock catalog");
    let runtime = ServerRuntime {
        config,
        info: tokio::sync::RwLock::new(RuntimeInfo {
            status: ServerStatus::Ready,
            connected_at: Some(Utc::now()),
            last_ping_at: Some(Utc::now()),
            tool_count: tools.len(),
            ..RuntimeInfo::default()
        }),
        reconnecting: AtomicBool::new(false),
        qos: McpServerQos::new(McpQosConfig::default()),
        proxy_fingerprint: None,
    };
    let runtime = TransportRuntime::new(
        manager.allocate_runtime_id().expect("runtime id"),
        runtime,
        client,
    );
    let publication = ServerPublication::new(
        manager.allocate_publication_id().expect("publication id"),
        runtime,
        catalog,
        &tools,
    )
    .expect("coherent mock publication");
    let activation = manager.prepare_runtime_tasks(publication.clone(), notification_rx);
    PreparedServerRuntime {
        publication: Some(publication),
        activation: Some(activation),
    }
}

async fn publish_mock(
    manager: &McpServerManager,
    prepared: PreparedServerRuntime,
) -> Arc<ServerPublication> {
    publish_mock_inner(manager, prepared, false).await
}

async fn publish_mock_with_tasks(
    manager: &McpServerManager,
    prepared: PreparedServerRuntime,
) -> Arc<ServerPublication> {
    publish_mock_inner(manager, prepared, true).await
}

async fn publish_mock_inner(
    manager: &McpServerManager,
    prepared: PreparedServerRuntime,
    start_tasks: bool,
) -> Arc<ServerPublication> {
    let _reconcile = manager.reconcile_lock.lock().await;
    let replacement = prepared.publication().clone();
    let base = manager.authority.generation();
    let next = McpRuntimeGeneration::plan(
        &base,
        std::slice::from_ref(&replacement),
        &[],
        manager.authority.ledger_relationship_limit,
        true,
    )
    .expect("mock generation plan");
    let old = base.servers.get(&replacement.server_id).cloned();
    manager
        .authority
        .replace_prevalidated_with(&base, next, || {
            if let Some(old) = &old {
                if Arc::ptr_eq(&old.runtime, &replacement.runtime) {
                    old.close_admission();
                } else {
                    old.retire_with_runtime();
                }
            }
        });
    let mut commit = prepared.into_commit();
    let publication = commit.publication().clone();
    commit.mark_published();
    if start_tasks {
        commit.activate();
    } else {
        commit.discard_activation();
    }
    publication
}

fn expected(manager: &McpServerManager, server_id: &str) -> ExpectedPublication {
    manager
        .current_expected(server_id)
        .expect("published expected server")
}

fn assert_snapshot_coherent(snapshot: &McpRuntimeSnapshot) {
    let aliases = snapshot.aliases();
    let schemas = snapshot.list_tools();
    assert_eq!(aliases.len(), schemas.len());
    for alias in aliases {
        assert!(snapshot.contains_exact_alias(&alias.alias));
        assert!(snapshot
            .tool(&alias.server_id, &alias.original_name)
            .is_some());
        let resolved = snapshot
            .resolve_call(&alias.alias)
            .expect("advertised alias resolves in the same generation");
        assert_eq!(resolved.server_id(), alias.server_id);
        assert_eq!(resolved.original_name(), alias.original_name);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SnapshotShape {
    servers: Vec<String>,
    aliases: Vec<(String, String, String)>,
    schemas: Vec<String>,
}

fn snapshot_shape(snapshot: &McpRuntimeSnapshot) -> SnapshotShape {
    assert_snapshot_coherent(snapshot);
    SnapshotShape {
        servers: snapshot.server_ids(),
        aliases: snapshot
            .aliases()
            .into_iter()
            .map(|alias| (alias.alias, alias.server_id, alias.original_name))
            .collect(),
        schemas: snapshot
            .list_tools()
            .into_iter()
            .map(|schema| schema.function.name)
            .collect(),
    }
}

async fn assert_old_or_new_during_writer<F, T>(manager: Arc<McpServerManager>, writer: F) -> T
where
    F: std::future::Future<Output = T>,
{
    let old = snapshot_shape(&manager.snapshot());
    let start = Arc::new(Barrier::new(2));
    let done = Arc::new(AtomicBool::new(false));
    let reader_manager = manager.clone();
    let reader_start = start.clone();
    let reader_done = done.clone();
    let reader = std::thread::spawn(move || {
        let mut observed = Vec::new();
        reader_start.wait();
        while !reader_done.load(Ordering::SeqCst) {
            observed.push(snapshot_shape(&reader_manager.snapshot()));
            std::thread::yield_now();
        }
        observed.push(snapshot_shape(&reader_manager.snapshot()));
        observed
    });
    start.wait();
    let output = tokio::time::timeout(Duration::from_secs(5), writer)
        .await
        .expect("generation writer must complete under concurrent snapshot reads");
    done.store(true, Ordering::SeqCst);
    let observed = reader.join().unwrap();
    let new = snapshot_shape(&manager.snapshot());
    assert!(
        observed.iter().all(|shape| shape == &old || shape == &new),
        "observed mixed generation: old={old:?} new={new:?} observed={observed:?}"
    );
    output
}

#[test]
fn manager_and_tool_index_share_one_authority() {
    let manager = McpServerManager::new();
    assert!(manager.list_servers().is_empty());
    assert!(manager.tool_index().all_aliases().is_empty());
    assert!(manager.has_same_authority(&manager.tool_index()));
}

#[test]
fn manager_clone_and_event_channel_preserve_shared_authority() {
    let (tx, _rx) = mpsc::channel(100);
    let manager = McpServerManager::new().with_event_channel(tx);
    let cloned = manager.clone();

    assert!(manager.event_tx.is_some());
    assert!(cloned.event_tx.is_some());
    assert!(manager.has_same_authority(&cloned.tool_index()));
}

#[tokio::test]
async fn resolved_calls_are_affine_to_their_manager_authority() {
    let manager = McpServerManager::new();
    let (transport, controls) = MockTransport::new(vec![tool("echo", "owned")], "owned");
    let publication = publish_mock(
        &manager,
        prepare_mock(&manager, "owned", vec![tool("echo", "owned")], transport).await,
    )
    .await;
    let snapshot = manager.snapshot();
    let ticket = snapshot
        .resolve_call(&snapshot.aliases()[0].alias)
        .expect("owned ticket");

    let (foreign_events, mut foreign_event_rx) = mpsc::channel(4);
    let foreign = McpServerManager::new().with_event_channel(foreign_events);
    let error = foreign
        .call_resolved_tool(&ticket, serde_json::json!({}))
        .await
        .expect_err("independent manager must reject a foreign ticket before admission");
    assert!(matches!(error, McpError::ForeignRuntimeAuthority));
    assert_eq!(controls.calls.load(Ordering::SeqCst), 0);
    assert_eq!(publication.active_calls(), 0);
    assert_eq!(publication.runtime.active_calls(), 0);
    assert!(!publication
        .runtime
        .runtime
        .reconnecting
        .load(Ordering::SeqCst));
    assert_eq!(
        publication
            .runtime
            .runtime
            .qos
            .state
            .lock()
            .await
            .consecutive_failures,
        0
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), foreign_event_rx.recv())
            .await
            .is_err()
    );

    let result = manager
        .clone()
        .call_resolved_tool(&ticket, serde_json::json!({}))
        .await
        .expect("a manager clone shares the ticket authority");
    assert!(matches!(
        &result.content[0],
        crate::types::McpContentItem::Text { text, .. } if text == "owned"
    ));
    assert_eq!(controls.calls.load(Ordering::SeqCst), 1);
    manager.stop_server("owned").await.unwrap();
}

#[tokio::test]
async fn empty_manager_public_lifecycle_is_stable() {
    let manager = McpServerManager::new();
    assert!(manager.list_servers().is_empty());
    assert!(!manager.is_server_running("nonexistent"));
    assert!(manager.get_server_info("nonexistent").is_none());
    assert!(manager.get_tool_info("nonexistent", "tool").is_none());
    assert!(matches!(
        manager.stop_server("nonexistent").await,
        Err(McpError::NotRunning(id)) if id == "nonexistent"
    ));
    assert!(matches!(
        manager
            .call_tool("nonexistent", "tool", serde_json::json!({}))
            .await,
        Err(McpError::ServerNotFound(id)) if id == "nonexistent"
    ));
    assert!(matches!(
        manager.refresh_tools("nonexistent").await,
        Err(McpError::ServerNotFound(id)) if id == "nonexistent"
    ));
    manager.shutdown_all().await;
}

#[test]
fn reconnect_config_and_runtime_defaults_remain_compatible() {
    let default = ReconnectConfig::default();
    assert!(default.enabled);
    assert_eq!(default.initial_backoff_ms, 1_000);
    assert_eq!(default.max_backoff_ms, 30_000);
    assert_eq!(default.max_attempts, 0);

    let custom = ReconnectConfig {
        enabled: false,
        initial_backoff_ms: 500,
        max_backoff_ms: 10_000,
        max_attempts: 5,
    };
    assert!(!custom.enabled);
    assert_eq!(custom.initial_backoff_ms, 500);
    assert_eq!(custom.max_backoff_ms, 10_000);
    assert_eq!(custom.max_attempts, 5);

    let info = RuntimeInfo::default();
    assert_eq!(info.status, ServerStatus::Stopped);
    assert!(info.last_error.is_none());
    assert!(info.connected_at.is_none());
    assert!(info.disconnected_at.is_none());
    assert_eq!(info.tool_count, 0);
    assert_eq!(info.restart_count, 0);
    assert!(info.last_ping_at.is_none());
    assert_eq!(ServerStatus::Ready.to_string(), "ready");
    assert_eq!(ServerStatus::Degraded.to_string(), "degraded");
    assert_eq!(ServerStatus::Error.to_string(), "error");
    assert_eq!(ServerStatus::Stopped.to_string(), "stopped");
    assert_eq!(ServerStatus::Connecting.to_string(), "connecting");
}

#[test]
fn exponential_backoff_remains_bounded_and_zero_attempts_means_unlimited() {
    let mut current = 1_000u64;
    let max = 30_000u64;
    for expected in [2_000, 4_000, 8_000, 16_000, 30_000, 30_000] {
        current = current.saturating_mul(2).min(max);
        assert_eq!(current, expected);
    }
    assert_eq!(ReconnectConfig::default().max_attempts, 0);
}

#[test]
fn proxy_fingerprint_changes_on_proxy_or_auth_change() {
    let mut config = Config::default();
    config.http_proxy.clear();
    config.https_proxy.clear();
    config.proxy_auth = None;
    assert_eq!(proxy_fingerprint(&config), None);

    config.http_proxy = "http://proxy:8080".to_string();
    let first = proxy_fingerprint(&config).expect("proxy fingerprint");
    config.http_proxy = "http://proxy2:8080".to_string();
    assert_ne!(first, proxy_fingerprint(&config).unwrap());

    config.http_proxy = "http://proxy:8080".to_string();
    config.proxy_auth = Some(bamboo_config::ProxyAuth {
        username: "user".to_string(),
        password: "pass".to_string(),
    });
    let authenticated = proxy_fingerprint(&config).unwrap();
    assert_ne!(first, authenticated);
    config.proxy_auth.as_mut().unwrap().password = "pass2".to_string();
    assert_ne!(authenticated, proxy_fingerprint(&config).unwrap());
}

#[tokio::test]
async fn sse_transport_respects_proxy_settings_when_available() {
    let mut config = Config::default();
    config.http_proxy = "http://".to_string();
    let manager = McpServerManager::new_with_config(Arc::new(tokio::sync::RwLock::new(config)));
    let server = McpServerConfig {
        id: "sse-test".to_string(),
        name: Some("SSE test".to_string()),
        enabled: true,
        transport: TransportConfig::Sse(SseConfig {
            url: "http://localhost:9999/sse".to_string(),
            headers: Vec::new(),
            connect_timeout_ms: 100,
        }),
        request_timeout_ms: 1_000,
        healthcheck_interval_ms: 1_000,
        reconnect: ReconnectConfig {
            enabled: false,
            initial_backoff_ms: 100,
            max_backoff_ms: 1_000,
            max_attempts: 1,
        },
        allowed_tools: Vec::new(),
        denied_tools: Vec::new(),
    };

    match manager.start_server(server).await.unwrap_err() {
        McpError::InvalidConfig(message) => assert!(
            message.to_lowercase().contains("proxy") || message.to_lowercase().contains("http"),
            "unexpected error message: {message}"
        ),
        other => panic!("expected InvalidConfig, got {other:?}"),
    }
}

#[tokio::test]
async fn qos_circuit_opens_after_consecutive_failures() {
    let qos = McpServerQos::new(McpQosConfig {
        max_concurrent_calls: 2,
        circuit_failure_threshold: 2,
        circuit_open_ms: 60_000,
        reconnect_failure_threshold: u32::MAX,
    });
    let error = McpError::Connection("boom".to_string());
    assert!(!qos.record_failure("server-a", "tool-a", &error).await);
    assert!(qos.check_circuit("server-a", "tool-a").await.is_ok());
    assert!(!qos.record_failure("server-a", "tool-a", &error).await);
    assert!(matches!(
        qos.check_circuit("server-a", "tool-a").await,
        Err(McpError::ToolExecution(message)) if message.contains("circuit open")
    ));
}

#[tokio::test]
async fn qos_circuit_recovers_after_open_window() {
    let qos = McpServerQos::new(McpQosConfig {
        max_concurrent_calls: 1,
        circuit_failure_threshold: 1,
        circuit_open_ms: 5,
        reconnect_failure_threshold: u32::MAX,
    });
    let error = McpError::Connection("boom".to_string());
    qos.record_failure("server-b", "tool-b", &error).await;
    assert!(qos.check_circuit("server-b", "tool-b").await.is_err());
    sleep(Duration::from_millis(15)).await;
    assert!(qos.check_circuit("server-b", "tool-b").await.is_ok());
}

#[tokio::test]
async fn qos_signals_recycle_at_reconnect_threshold() {
    let qos = McpServerQos::new(McpQosConfig {
        max_concurrent_calls: 1,
        circuit_failure_threshold: u32::MAX,
        circuit_open_ms: 5,
        reconnect_failure_threshold: 3,
    });
    let error = McpError::Connection("boom".to_string());
    assert!(!qos.record_failure("s", "t", &error).await);
    assert!(!qos.record_failure("s", "t", &error).await);
    assert!(qos.record_failure("s", "t", &error).await);
    assert!(!qos.record_failure("s", "t", &error).await);
    qos.record_success().await;
    assert!(!qos.record_failure("s", "t", &error).await);
    assert!(!qos.record_failure("s", "t", &error).await);
    assert!(qos.record_failure("s", "t", &error).await);
}

#[tokio::test]
async fn public_start_rejects_an_existing_published_server() {
    let manager = McpServerManager::new();
    let (transport, _) = MockTransport::new(vec![tool("echo", "existing")], "existing");
    publish_mock(
        &manager,
        prepare_mock(
            &manager,
            "existing",
            vec![tool("echo", "existing")],
            transport,
        )
        .await,
    )
    .await;

    assert!(matches!(
        manager.start_server(test_config("existing")).await,
        Err(McpError::AlreadyRunning(id)) if id == "existing"
    ));
    manager.stop_server("existing").await.unwrap();
}

#[tokio::test]
async fn legacy_initialize_skips_disabled_servers() {
    let manager = McpServerManager::new();
    let mut disabled = test_config("disabled");
    disabled.enabled = false;
    manager
        .initialize_from_config(&McpConfig {
            version: 1,
            servers: vec![disabled],
        })
        .await;
    assert!(!manager.is_server_running("disabled"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn capacity_one_ready_batch_finishes_writer_and_excludes_successor() {
    let (event_tx, mut event_rx) = mpsc::channel(1);
    event_tx
        .send(McpEvent::ToolsChanged {
            server_id: "blocker".to_string(),
            tools: Vec::new(),
        })
        .await
        .unwrap();
    let manager = Arc::new(McpServerManager::new().with_event_channel(event_tx));
    let (old_transport, _) = MockTransport::new(vec![tool("old", "old")], "old");
    publish_mock(
        &manager,
        prepare_mock(&manager, "server", vec![tool("old", "old")], old_transport).await,
    )
    .await;

    let old = expected(&manager, "server");
    let (first_transport, _) = MockTransport::new(vec![tool("first", "first")], "first");
    let first_candidate = prepare_mock(
        &manager,
        "server",
        vec![tool("first", "first")],
        first_transport,
    )
    .await;
    let first_tools = first_candidate
        .publication()
        .catalog
        .aliases()
        .into_iter()
        .map(|alias| alias.alias)
        .collect::<Vec<_>>();
    let first_start = Arc::new(tokio::sync::Barrier::new(2));
    let first_manager = manager.clone();
    let first_task_start = first_start.clone();
    let first_writer = tokio::spawn(async move {
        first_task_start.wait().await;
        first_manager
            .publish_reconnected_runtime_if_current(old, first_candidate)
            .await
    });
    first_start.wait().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(250), first_writer)
            .await
            .expect("writer must not wait for event output capacity")
            .unwrap()
            .unwrap()
    );
    assert_eq!(manager.snapshot().aliases()[0].original_name, "first");
    assert!(manager.event_sequence_lock.try_lock().is_err());

    let first_expected = expected(&manager, "server");
    let (second_transport, _) = MockTransport::new(vec![tool("second", "second")], "second");
    let second_candidate = prepare_mock(
        &manager,
        "server",
        vec![tool("second", "second")],
        second_transport,
    )
    .await;
    let second_tools = second_candidate
        .publication()
        .catalog
        .aliases()
        .into_iter()
        .map(|alias| alias.alias)
        .collect::<Vec<_>>();
    let second_start = Arc::new(tokio::sync::Barrier::new(2));
    let second_manager = manager.clone();
    let second_task_start = second_start.clone();
    let mut second_writer = tokio::spawn(async move {
        second_task_start.wait().await;
        second_manager
            .publish_reconnected_runtime_if_current(first_expected, second_candidate)
            .await
    });
    second_start.wait().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut second_writer)
            .await
            .is_err()
    );
    assert_eq!(manager.snapshot().aliases()[0].original_name, "first");

    assert!(matches!(
        event_rx.recv().await,
        Some(McpEvent::ToolsChanged { server_id, tools })
            if server_id == "blocker" && tools.is_empty()
    ));
    assert!(matches!(
        event_rx.recv().await,
        Some(McpEvent::ServerStatusChanged {
            server_id,
            status: ServerStatus::Ready,
            error: None,
        }) if server_id == "server"
    ));
    assert!(matches!(
        event_rx.recv().await,
        Some(McpEvent::ToolsChanged { server_id, tools })
            if server_id == "server" && tools == first_tools
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(250), &mut second_writer)
            .await
            .expect("successor completes after the prior pair enters the channel")
            .unwrap()
            .unwrap()
    );
    assert_eq!(manager.snapshot().aliases()[0].original_name, "second");
    assert!(matches!(
        event_rx.recv().await,
        Some(McpEvent::ServerStatusChanged {
            server_id,
            status: ServerStatus::Ready,
            error: None,
        }) if server_id == "server"
    ));
    assert!(matches!(
        event_rx.recv().await,
        Some(McpEvent::ToolsChanged { server_id, tools })
            if server_id == "server" && tools == second_tools
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_reader_cannot_enter_after_fences_close_before_generation_swap() {
    let gate = Arc::new((StdMutex::new((false, false)), std::sync::Condvar::new()));
    let mut manager = McpServerManager::new();
    let (old_transport, _) = MockTransport::new(vec![tool("echo", "old")], "old");
    publish_mock(
        &manager,
        prepare_mock(&manager, "server", vec![tool("echo", "old")], old_transport).await,
    )
    .await;
    let old_snapshot = manager.snapshot();
    let old_ticket = old_snapshot
        .resolve_call(&old_snapshot.aliases()[0].alias)
        .unwrap();
    let old_publication_id = old_ticket.publication_id();
    let probe_gate = gate.clone();
    manager.publish_probe = Some(Arc::new(move |phase| {
        if phase != PublishProbePhase::AfterFencesBeforeSwap {
            return;
        }
        let (state, condition) = &*probe_gate;
        let mut state = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.0 = true;
        condition.notify_all();
        while !state.1 {
            state = condition
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }));
    let manager = Arc::new(manager);
    let expected = expected(&manager, "server");
    let (new_transport, _) = MockTransport::new(vec![tool("echo", "new")], "new");
    let candidate =
        prepare_mock(&manager, "server", vec![tool("echo", "new")], new_transport).await;
    let writer_manager = manager.clone();
    let writer = tokio::spawn(async move {
        writer_manager
            .publish_reconnected_runtime_if_current(expected, candidate)
            .await
    });

    let entered_gate = gate.clone();
    tokio::task::spawn_blocking(move || {
        let (state, condition) = &*entered_gate;
        let mut state = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while !state.0 {
            state = condition
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    })
    .await
    .unwrap();

    let (reader_started, reader_started_rx) = std::sync::mpsc::channel();
    let (reader_done, reader_done_rx) = std::sync::mpsc::channel();
    let reader_manager = manager.clone();
    let reader = std::thread::spawn(move || {
        reader_started.send(()).unwrap();
        reader_done
            .send(snapshot_shape(&reader_manager.snapshot()))
            .unwrap();
    });
    reader_started_rx.recv().unwrap();
    assert!(matches!(
        reader_done_rx.recv_timeout(std::time::Duration::from_millis(50)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));

    {
        let (state, condition) = &*gate;
        let mut state = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.1 = true;
        condition.notify_all();
    }
    assert!(writer.await.unwrap().unwrap());
    let reader_shape = reader_done_rx.recv().unwrap();
    reader.join().unwrap();
    assert_eq!(reader_shape, snapshot_shape(&manager.snapshot()));
    let current = manager.snapshot();
    let current_ticket = current.resolve_call(&current.aliases()[0].alias).unwrap();
    assert_ne!(current_ticket.publication_id(), old_publication_id);
    assert!(matches!(
        manager
            .call_resolved_tool(&old_ticket, serde_json::json!({}))
            .await,
        Err(McpError::StalePublication { .. })
    ));
}

#[tokio::test]
async fn install_collision_preserves_existing_generation_and_drops_candidate() {
    let manager = McpServerManager::new();
    let (old_transport, _) = MockTransport::new(vec![tool("old", "old")], "old");
    let old = publish_mock(
        &manager,
        prepare_mock(&manager, "stable", vec![tool("old", "old")], old_transport).await,
    )
    .await;
    let old_alias = old.catalog.aliases()[0].alias.clone();

    let candidate_tool = tool("candidate", "candidate");
    let mut catalog = manager
        .index
        .plan_server_tools("candidate", std::slice::from_ref(&candidate_tool), &[], &[])
        .unwrap();
    catalog.replace_first_canonical_alias_for_test(old_alias.clone());
    let (candidate_transport, controls) =
        MockTransport::new(vec![candidate_tool.clone()], "candidate");
    let mut client = McpProtocolClient::new(Box::new(candidate_transport));
    client.connect().await.unwrap();
    let runtime = TransportRuntime::new(
        manager.allocate_runtime_id().unwrap(),
        ServerRuntime {
            config: test_config("candidate"),
            info: tokio::sync::RwLock::new(RuntimeInfo {
                status: ServerStatus::Ready,
                tool_count: 1,
                ..RuntimeInfo::default()
            }),
            reconnecting: AtomicBool::new(false),
            qos: McpServerQos::new(McpQosConfig::default()),
            proxy_fingerprint: None,
        },
        client,
    );
    let candidate = ServerPublication::new(
        manager.allocate_publication_id().unwrap(),
        runtime,
        catalog,
        &[candidate_tool],
    )
    .unwrap();
    let activation = manager.prepare_runtime_tasks(candidate.clone(), None);
    let staged = PreparedServerRuntime {
        publication: Some(candidate),
        activation: Some(activation),
    };
    let base = manager.authority.generation();
    let error = McpRuntimeGeneration::plan(
        &base,
        std::slice::from_ref(staged.publication()),
        &[],
        manager.authority.ledger_relationship_limit,
        true,
    )
    .expect_err("colliding candidate must fail preflight");
    assert!(matches!(
        error,
        McpError::ToolRegistration(ToolRegistrationError::AliasCollision { alias })
            if alias == old_alias
    ));
    drop(staged);

    assert!(Arc::ptr_eq(&manager.authority.generation(), &base));
    assert!(Arc::ptr_eq(
        &manager.current_expected("stable").unwrap().publication,
        &old
    ));
    assert_eq!(controls.drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn transactional_bootstrap_failure_keeps_old_runtime_and_catalog() {
    let manager = McpServerManager::new();
    let (transport, _) = MockTransport::new(vec![tool("old", "old")], "old");
    let old = publish_mock(
        &manager,
        prepare_mock(&manager, "stable", vec![tool("old", "old")], transport).await,
    )
    .await;
    let old_snapshot = manager.snapshot();
    let mut replacement = test_config("stable");
    replacement.transport = TransportConfig::Stdio(StdioConfig {
        command: "definitely-not-a-real-mcp-command-995".to_string(),
        args: Vec::new(),
        cwd: None,
        env: HashMap::new(),
        env_encrypted: HashMap::new(),
        env_credential_refs: HashMap::new(),
        startup_timeout_ms: 100,
    });

    manager
        .reconcile_from_config_transactional(&McpConfig {
            version: 1,
            servers: vec![replacement],
        })
        .await
        .expect_err("replacement bootstrap must fail");

    let current = manager.current_expected("stable").unwrap();
    assert!(Arc::ptr_eq(&current.publication, &old));
    assert_eq!(manager.snapshot().aliases(), old_snapshot.aliases());
    assert_eq!(old.fence_state(), FenceState::Open);
    assert_eq!(old.runtime.fence_state(), FenceState::Open);
}

#[tokio::test]
async fn forced_transactional_reconcile_replaces_identical_effective_config() {
    let directory = tempfile::tempdir().unwrap();
    let Some(config) = fixture_config(&directory, &["forced"]) else {
        return;
    };
    let manager = McpServerManager::new();
    manager
        .reconcile_from_config_transactional(&config)
        .await
        .unwrap();
    let first = manager
        .snapshot()
        .resolve_call(&manager.snapshot().aliases()[0].alias)
        .unwrap();
    let first_publication = manager.current_expected("forced").unwrap().publication;

    manager
        .reconcile_from_config_transactional_after_forcing(
            &config,
            &std::collections::HashSet::from(["forced".to_string()]),
            || async { Ok(()) },
        )
        .await
        .unwrap();
    let current = manager.snapshot();
    let second = current.resolve_call(&current.aliases()[0].alias).unwrap();
    assert_ne!(first.publication_id(), second.publication_id());
    assert_ne!(first.runtime_id(), second.runtime_id());
    assert_eq!(first_publication.fence_state(), FenceState::Closed);
    assert_eq!(first_publication.runtime.fence_state(), FenceState::Closed);
    manager.shutdown_all().await;
}

#[tokio::test]
async fn committed_reconcile_publishes_all_removals_before_blocked_cleanup() {
    let (event_tx, event_rx) = mpsc::channel(1);
    event_tx
        .send(McpEvent::ToolsChanged {
            server_id: "blocker".to_string(),
            tools: Vec::new(),
        })
        .await
        .unwrap();
    let manager = McpServerManager::new().with_event_channel(event_tx);
    let mut old = Vec::new();
    for server_id in ["first", "second"] {
        let (transport, _) = MockTransport::new(vec![tool("old", server_id)], server_id);
        old.push(
            publish_mock(
                &manager,
                prepare_mock(&manager, server_id, vec![tool("old", server_id)], transport).await,
            )
            .await,
        );
    }
    let durable = Arc::new(AtomicBool::new(false));
    let durable_callback = durable.clone();

    tokio::time::timeout(
        Duration::from_millis(200),
        manager.reconcile_from_config_transactional_after(
            &McpConfig {
                version: 1,
                servers: Vec::new(),
            },
            || async move {
                durable_callback.store(true, Ordering::SeqCst);
                Ok(())
            },
        ),
    )
    .await
    .expect("blocked event cleanup cannot suspend committed publication")
    .unwrap();

    assert!(durable.load(Ordering::SeqCst));
    assert!(manager.snapshot().server_ids().is_empty());
    assert!(manager.snapshot().aliases().is_empty());
    assert!(old
        .iter()
        .all(|publication| publication.fence_state() == FenceState::Closed));
    assert!(old
        .iter()
        .all(|publication| publication.runtime.fence_state() == FenceState::Closed));
    drop(event_rx);
}

#[tokio::test]
async fn manager_stop_preserves_survivor_and_historical_legacy_ambiguity() {
    let manager = McpServerManager::new();
    let mut publications = Vec::new();
    for server_id in ["a::b", "a__b"] {
        let (transport, _) = MockTransport::new(vec![tool("c", server_id)], server_id);
        publications.push(
            publish_mock(
                &manager,
                prepare_mock(&manager, server_id, vec![tool("c", server_id)], transport).await,
            )
            .await,
        );
    }
    let removed_alias = publications[0].catalog.aliases()[0].alias.clone();
    let survivor_alias = publications[1].catalog.aliases()[0].alias.clone();
    assert!(manager.snapshot().lookup("mcp__a__b__c").is_none());

    manager.stop_server("a::b").await.unwrap();
    let current = manager.snapshot();
    assert!(current.lookup(&removed_alias).is_none());
    assert_eq!(current.lookup(&survivor_alias).unwrap().server_id, "a__b");
    assert!(current.lookup("mcp__a__b__c").is_none());
    assert_eq!(publications[0].fence_state(), FenceState::Closed);
    assert_eq!(publications[1].fence_state(), FenceState::Open);
    manager.stop_server("a__b").await.unwrap();
}

#[tokio::test]
async fn stale_reconnect_candidate_cannot_overwrite_successor() {
    let manager = McpServerManager::new();
    let (old_transport, _) = MockTransport::new(vec![tool("old", "old")], "old");
    publish_mock(
        &manager,
        prepare_mock(&manager, "stable", vec![tool("old", "old")], old_transport).await,
    )
    .await;
    let stale = expected(&manager, "stable");
    let (successor_transport, _) =
        MockTransport::new(vec![tool("successor", "successor")], "successor");
    let successor = publish_mock(
        &manager,
        prepare_mock(
            &manager,
            "stable",
            vec![tool("successor", "successor")],
            successor_transport,
        )
        .await,
    )
    .await;
    let (candidate_transport, controls) = MockTransport::new(vec![tool("stale", "stale")], "stale");
    let candidate = prepare_mock(
        &manager,
        "stable",
        vec![tool("stale", "stale")],
        candidate_transport,
    )
    .await;

    assert!(!manager
        .publish_reconnected_runtime_if_current(stale, candidate)
        .await
        .unwrap());
    assert_eq!(controls.drops.load(Ordering::SeqCst), 1);
    assert!(Arc::ptr_eq(
        &manager.current_expected("stable").unwrap().publication,
        &successor
    ));
    assert_eq!(manager.snapshot().aliases()[0].original_name, "successor");
}

#[tokio::test]
async fn refresh_registration_failure_preserves_old_publication() {
    let manager = McpServerManager::new();
    let (transport, _) = MockTransport::new(vec![tool("old", "old")], "old");
    let old = publish_mock(
        &manager,
        prepare_mock(&manager, "stable", vec![tool("old", "old")], transport).await,
    )
    .await;
    let before = manager.snapshot();
    let error = manager
        .publish_refreshed_tools_if_current(
            expected(&manager, "stable"),
            vec![tool("duplicate", "first"), tool("duplicate", "second")],
        )
        .await
        .expect_err("duplicate refresh must fail registration");
    assert!(matches!(
        error,
        McpError::ToolRegistration(ToolRegistrationError::DuplicateToolIdentity {
            first_position: 0,
            duplicate_position: 1,
        })
    ));
    assert!(Arc::ptr_eq(
        &manager.current_expected("stable").unwrap().publication,
        &old
    ));
    assert_eq!(manager.snapshot().revision(), before.revision());
    assert_eq!(manager.snapshot().aliases(), before.aliases());
    assert_eq!(old.fence_state(), FenceState::Open);
}

#[tokio::test]
async fn reconnect_bootstrap_failure_keeps_old_runtime_and_catalog() {
    let manager = McpServerManager::new();
    let mut config = test_config("stable");
    config.transport = TransportConfig::Stdio(StdioConfig {
        command: "definitely-not-a-real-mcp-command-995-reconnect".to_string(),
        args: Vec::new(),
        cwd: None,
        env: HashMap::new(),
        env_encrypted: HashMap::new(),
        env_credential_refs: HashMap::new(),
        startup_timeout_ms: 100,
    });
    let (transport, _) = MockTransport::new(vec![tool("old", "old")], "old");
    let old = publish_mock(
        &manager,
        prepare_mock_with_config(&manager, config, vec![tool("old", "old")], transport).await,
    )
    .await;
    let old_aliases = manager.snapshot().aliases();

    manager
        .reconnect_server(expected(&manager, "stable"))
        .await
        .expect_err("candidate transport bootstrap must fail");
    assert!(Arc::ptr_eq(
        &manager.current_expected("stable").unwrap().publication,
        &old
    ));
    assert_eq!(manager.snapshot().aliases(), old_aliases);
    assert_eq!(old.fence_state(), FenceState::Open);
    assert_eq!(old.runtime.fence_state(), FenceState::Open);
}

#[tokio::test]
async fn reconnect_registration_failure_after_bootstrap_rolls_back_old_generation() {
    let directory = tempfile::tempdir().unwrap();
    let Some(config) = fixture_config(&directory, &["stable"]) else {
        return;
    };
    let mut manager = McpServerManager::new();
    let (old_transport, _) = MockTransport::new(vec![tool("old", "old")], "old");
    let old = publish_mock(
        &manager,
        prepare_mock_with_config(
            &manager,
            config.servers[0].clone(),
            vec![tool("old", "old")],
            old_transport,
        )
        .await,
    )
    .await;
    let old_snapshot = manager.snapshot();
    manager.catalog_plan_probe = Some(Arc::new(|server_id, catalog| {
        if server_id == "stable" {
            catalog.replace_first_original_name_for_test("missing-after-bootstrap".to_string());
        }
    }));

    let error = manager
        .reconnect_server(expected(&manager, "stable"))
        .await
        .expect_err("provider-schema registration must fail after successful bootstrap");
    assert!(matches!(
        error,
        McpError::ToolRegistration(ToolRegistrationError::ProviderSchemaUnavailable)
    ));
    assert!(Arc::ptr_eq(
        &manager.current_expected("stable").unwrap().publication,
        &old
    ));
    assert_eq!(manager.snapshot().aliases(), old_snapshot.aliases());
    assert_eq!(old.fence_state(), FenceState::Open);
    assert_eq!(old.runtime.fence_state(), FenceState::Open);
}

#[tokio::test]
async fn reconnect_planning_failure_after_bootstrap_rolls_back_old_generation() {
    let directory = tempfile::tempdir().unwrap();
    let Some(config) = fixture_config(&directory, &["stable"]) else {
        return;
    };
    let mut manager = McpServerManager::new();
    let (stable_transport, _) = MockTransport::new(vec![tool("old", "old")], "old");
    let stable = publish_mock(
        &manager,
        prepare_mock_with_config(
            &manager,
            config.servers[0].clone(),
            vec![tool("old", "old")],
            stable_transport,
        )
        .await,
    )
    .await;
    let (blocker_transport, _) = MockTransport::new(vec![tool("block", "block")], "block");
    let blocker = publish_mock(
        &manager,
        prepare_mock(
            &manager,
            "blocker",
            vec![tool("block", "block")],
            blocker_transport,
        )
        .await,
    )
    .await;
    let blocker_alias = blocker.catalog.aliases()[0].alias.clone();
    let old_snapshot = manager.snapshot();
    manager.catalog_plan_probe = Some(Arc::new(move |server_id, catalog| {
        if server_id == "stable" {
            catalog.replace_first_canonical_alias_for_test(blocker_alias.clone());
        }
    }));

    let error = manager
        .reconnect_server(expected(&manager, "stable"))
        .await
        .expect_err("catalog collision must fail after successful bootstrap");
    assert!(matches!(
        error,
        McpError::ToolRegistration(ToolRegistrationError::AliasCollision { .. })
    ));
    assert!(Arc::ptr_eq(
        &manager.current_expected("stable").unwrap().publication,
        &stable
    ));
    assert!(Arc::ptr_eq(
        &manager.current_expected("blocker").unwrap().publication,
        &blocker
    ));
    assert_eq!(manager.snapshot().aliases(), old_snapshot.aliases());
    assert_eq!(stable.fence_state(), FenceState::Open);
    assert_eq!(stable.runtime.fence_state(), FenceState::Open);
    assert_eq!(blocker.fence_state(), FenceState::Open);
}

#[tokio::test]
async fn canonical_identity_is_exact_from_listing_through_execution() {
    let manager = Arc::new(McpServerManager::new());
    let (transport, _) = MockTransport::new(vec![tool("c", "remote")], "old-runtime");
    publish_mock(
        &manager,
        prepare_mock(&manager, "a::b", vec![tool("c", "remote")], transport).await,
    )
    .await;
    let executor = McpToolExecutor::from_manager(manager.clone());
    let listed = executor.list_tools();
    assert_eq!(listed.len(), 1);
    let alias = listed[0].function.name.clone();
    let identity = ClassifiedToolIdentity::from_schema_name(&alias).unwrap();
    assert_eq!(identity.execution_name(), alias);
    assert!(manager.snapshot().contains_exact_alias(&alias));

    let result = executor
        .execute(&ToolCall {
            id: "call-exact-mcp-alias".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: alias,
                arguments: "{}".to_string(),
            },
        })
        .await
        .unwrap();
    assert!(result.success);
    assert_eq!(result.result, "old-runtime");
}

#[tokio::test]
async fn tools_list_changed_notification_triggers_generation_refresh() {
    let (event_tx, mut event_rx) = mpsc::channel(16);
    let manager = McpServerManager::new().with_event_channel(event_tx);
    let (transport, controls) = MockTransport::new(vec![tool("old", "old")], "runtime");
    let old = publish_mock_with_tasks(
        &manager,
        prepare_mock(&manager, "server", vec![tool("old", "old")], transport).await,
    )
    .await;
    *controls
        .tools
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) =
        vec![tool("alpha", "Alpha"), tool("beta", "Beta")];
    controls
        .message_tx
        .send(
            serde_json::json!({
                "jsonrpc": "2.0",
                "method": "notifications/tools/list_changed"
            })
            .to_string(),
        )
        .await
        .unwrap();

    let event = tokio::time::timeout(Duration::from_secs(3), event_rx.recv())
        .await
        .expect("tools change event")
        .expect("event channel open");
    assert!(matches!(
        event,
        McpEvent::ToolsChanged { server_id, tools }
            if server_id == "server" && tools.len() == 2
    ));
    let snapshot = manager.snapshot();
    assert_eq!(snapshot.aliases().len(), 2);
    assert_snapshot_coherent(&snapshot);
    assert_eq!(old.fence_state(), FenceState::Closed);
    assert_eq!(old.runtime.fence_state(), FenceState::Open);
    manager.stop_server("server").await.unwrap();
}

#[tokio::test]
async fn unhandled_notification_is_drained_without_refresh() {
    let (event_tx, mut event_rx) = mpsc::channel(16);
    let manager = McpServerManager::new().with_event_channel(event_tx);
    let (transport, controls) = MockTransport::new(vec![tool("old", "old")], "runtime");
    let publication = publish_mock_with_tasks(
        &manager,
        prepare_mock(&manager, "server", vec![tool("old", "old")], transport).await,
    )
    .await;
    let revision = manager.snapshot().revision();
    controls
        .message_tx
        .send(
            serde_json::json!({
                "jsonrpc": "2.0",
                "method": "notifications/message",
                "params": {"level": "info"}
            })
            .to_string(),
        )
        .await
        .unwrap();

    assert!(
        tokio::time::timeout(Duration::from_millis(200), event_rx.recv())
            .await
            .is_err()
    );
    assert_eq!(manager.snapshot().revision(), revision);
    assert!(Arc::ptr_eq(
        &manager.current_expected("server").unwrap().publication,
        &publication
    ));
    manager.stop_server("server").await.unwrap();
}

#[tokio::test]
async fn stale_unadmitted_ticket_fails_after_refresh() {
    let manager = McpServerManager::new();
    let (transport, _) = MockTransport::new(vec![tool("echo", "old")], "old-runtime");
    let old = publish_mock(
        &manager,
        prepare_mock(&manager, "server", vec![tool("echo", "old")], transport).await,
    )
    .await;
    let snapshot = manager.snapshot();
    let alias = snapshot.aliases().pop().expect("old alias");
    let ticket = snapshot.resolve_call(&alias.alias).expect("old ticket");
    let old_runtime_id = ticket.runtime_id();

    manager
        .publish_refreshed_tools_if_current(
            expected(&manager, "server"),
            vec![tool("echo", "new schema")],
        )
        .await
        .expect("refresh publication")
        .then_some(())
        .expect("expected publication remained current");

    let error = manager
        .call_resolved_tool(&ticket, serde_json::json!({}))
        .await
        .expect_err("old passive ticket must fail admission");
    assert!(matches!(error, McpError::StalePublication { .. }));
    let current = manager.snapshot().resolve_call(&alias.alias).unwrap();
    assert_eq!(current.runtime_id(), old_runtime_id);
    assert_ne!(current.publication_id(), ticket.publication_id());
    assert_eq!(old.fence_state(), FenceState::Closed);
}

#[tokio::test]
async fn in_flight_old_call_never_retargets_across_reconnect() {
    let manager = Arc::new(McpServerManager::new());
    let (old_transport, mut old_controls) =
        MockTransport::new_with_call_gate(vec![tool("echo", "old")], "old-runtime", true, false);
    let old_publication = publish_mock(
        &manager,
        prepare_mock(&manager, "server", vec![tool("echo", "old")], old_transport).await,
    )
    .await;
    let old_expected = expected(&manager, "server");
    let ticket = manager
        .snapshot()
        .resolve_call(&manager.snapshot().aliases()[0].alias)
        .unwrap();
    let in_flight_ticket = ticket.clone();
    let call_manager = manager.clone();
    let call = tokio::spawn(async move {
        call_manager
            .call_resolved_tool(&in_flight_ticket, serde_json::json!({}))
            .await
    });
    old_controls
        .call_started
        .as_mut()
        .unwrap()
        .recv()
        .await
        .expect("old call admitted and sent");

    let (new_transport, _) = MockTransport::new(vec![tool("echo", "new")], "new-runtime");
    let prepared = prepare_mock(&manager, "server", vec![tool("echo", "new")], new_transport).await;
    assert!(manager
        .publish_reconnected_runtime_if_current(old_expected, prepared)
        .await
        .expect("reconnect publication"));
    assert_eq!(old_publication.fence_state(), FenceState::Retiring);
    assert_eq!(old_publication.runtime.fence_state(), FenceState::Retiring);
    assert_eq!(old_publication.runtime.active_calls(), 1);
    old_controls.call_release.unwrap().add_permits(1);
    let old_result = call.await.unwrap().expect("admitted old call completes");
    assert!(matches!(
        &old_result.content[0],
        crate::types::McpContentItem::Text { text, .. } if text == "old-runtime"
    ));
    assert_eq!(old_publication.fence_state(), FenceState::Closed);
    assert_eq!(old_publication.runtime.fence_state(), FenceState::Closed);

    let current = manager.snapshot();
    let new_ticket = current.resolve_call(&current.aliases()[0].alias).unwrap();
    assert_ne!(ticket.runtime_id(), new_ticket.runtime_id());
    let new_result = manager
        .call_resolved_tool(&new_ticket, serde_json::json!({}))
        .await
        .expect("successor call");
    assert!(matches!(
        &new_result.content[0],
        crate::types::McpContentItem::Text { text, .. } if text == "new-runtime"
    ));
}

#[tokio::test]
async fn stale_notification_cannot_refresh_successor() {
    let manager = McpServerManager::new();
    let (old_transport, _) = MockTransport::new(vec![tool("old", "old")], "old");
    publish_mock(
        &manager,
        prepare_mock(&manager, "server", vec![tool("old", "old")], old_transport).await,
    )
    .await;
    let stale = expected(&manager, "server");
    let (new_transport, controls) = MockTransport::new(vec![tool("new", "successor")], "new");
    publish_mock(
        &manager,
        prepare_mock(
            &manager,
            "server",
            vec![tool("new", "successor")],
            new_transport,
        )
        .await,
    )
    .await;
    *controls
        .tools
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = vec![tool("wrong", "stale")];

    manager
        .dispatch_server_notification(
            stale,
            JsonRpcNotification {
                jsonrpc: "2.0".to_string(),
                method: "notifications/tools/list_changed".to_string(),
                params: None,
            },
        )
        .await;
    let snapshot = manager.snapshot();
    assert_eq!(snapshot.aliases()[0].original_name, "new");
    assert_snapshot_coherent(&snapshot);
}

#[tokio::test]
async fn stale_health_and_qos_results_cannot_mutate_successor() {
    let manager = McpServerManager::new();
    let (old_transport, _) = MockTransport::new(vec![tool("echo", "old")], "old");
    let old = publish_mock(
        &manager,
        prepare_mock(&manager, "server", vec![tool("echo", "old")], old_transport).await,
    )
    .await;
    let stale = expected(&manager, "server");
    let (new_transport, _) = MockTransport::new(vec![tool("echo", "new")], "new");
    let new = publish_mock(
        &manager,
        prepare_mock(&manager, "server", vec![tool("echo", "new")], new_transport).await,
    )
    .await;

    assert_eq!(
        manager
            .publish_health_result_if_current(stale.clone(), Err("stale".to_string()))
            .await,
        None
    );
    old.runtime
        .runtime
        .qos
        .record_failure("server", "echo", &McpError::Disconnected)
        .await;
    manager.maybe_recycle_server(stale, true);
    assert_eq!(
        new.runtime.runtime.info.read().await.status,
        ServerStatus::Ready
    );
    assert_eq!(
        new.runtime
            .runtime
            .qos
            .state
            .lock()
            .await
            .consecutive_failures,
        0
    );
    assert!(!new.runtime.runtime.reconnecting.load(Ordering::SeqCst));
}

#[tokio::test]
async fn stopped_snapshot_ticket_fails_closed_and_never_retargets() {
    let manager = McpServerManager::new();
    let (transport, _) = MockTransport::new(vec![tool("echo", "old")], "old");
    publish_mock(
        &manager,
        prepare_mock(&manager, "server", vec![tool("echo", "old")], transport).await,
    )
    .await;
    let snapshot = manager.snapshot();
    let ticket = snapshot.resolve_call(&snapshot.aliases()[0].alias).unwrap();
    manager.stop_server("server").await.unwrap();
    assert!(matches!(
        manager
            .call_resolved_tool(&ticket, serde_json::json!({}))
            .await,
        Err(McpError::StalePublication { .. })
    ));
    assert!(manager.snapshot().aliases().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lifecycle_writers_publish_only_old_or_new_coherent_generations() {
    let start_dir = tempfile::tempdir().unwrap();
    let Some(mut start_config) = fixture_config(&start_dir, &["start"]) else {
        return;
    };
    let (start_events, _start_event_rx) = mpsc::channel(8);
    let start_manager = Arc::new(McpServerManager::new().with_event_channel(start_events));
    let start_writer_manager = start_manager.clone();
    assert_old_or_new_during_writer(start_manager.clone(), async move {
        start_writer_manager
            .start_server(start_config.servers.remove(0))
            .await
    })
    .await
    .unwrap();
    let start_snapshot = start_manager.snapshot();
    let start_ticket = start_snapshot
        .resolve_call(&start_snapshot.aliases()[0].alias)
        .unwrap();
    start_manager
        .call_resolved_tool(&start_ticket, serde_json::json!({}))
        .await
        .unwrap();

    let (refresh_events, _refresh_event_rx) = mpsc::channel(8);
    let refresh_manager = Arc::new(McpServerManager::new().with_event_channel(refresh_events));
    let (refresh_transport, _) = MockTransport::new(vec![tool("echo", "old")], "refresh");
    publish_mock(
        &refresh_manager,
        prepare_mock(
            &refresh_manager,
            "refresh",
            vec![tool("echo", "old")],
            refresh_transport,
        )
        .await,
    )
    .await;
    let refresh_snapshot = refresh_manager.snapshot();
    let old_refresh = refresh_snapshot
        .resolve_call(&refresh_snapshot.aliases()[0].alias)
        .unwrap();
    let refresh_expected = expected(&refresh_manager, "refresh");
    let refresh_writer_manager = refresh_manager.clone();
    assert!(
        assert_old_or_new_during_writer(refresh_manager.clone(), async move {
            refresh_writer_manager
                .publish_refreshed_tools_if_current(
                    refresh_expected,
                    vec![tool("echo", "new schema")],
                )
                .await
        })
        .await
        .unwrap()
    );
    assert!(matches!(
        refresh_manager
            .call_resolved_tool(&old_refresh, serde_json::json!({}))
            .await,
        Err(McpError::StalePublication { .. })
    ));

    let (reconnect_events, _reconnect_event_rx) = mpsc::channel(8);
    let reconnect_manager = Arc::new(McpServerManager::new().with_event_channel(reconnect_events));
    let (old_transport, _) = MockTransport::new(vec![tool("echo", "old")], "old");
    publish_mock(
        &reconnect_manager,
        prepare_mock(
            &reconnect_manager,
            "reconnect",
            vec![tool("echo", "old")],
            old_transport,
        )
        .await,
    )
    .await;
    let reconnect_snapshot = reconnect_manager.snapshot();
    let old_reconnect = reconnect_snapshot
        .resolve_call(&reconnect_snapshot.aliases()[0].alias)
        .unwrap();
    let reconnect_expected = expected(&reconnect_manager, "reconnect");
    let (new_transport, _) = MockTransport::new(vec![tool("echo", "new")], "new");
    let reconnect_candidate = prepare_mock(
        &reconnect_manager,
        "reconnect",
        vec![tool("echo", "new")],
        new_transport,
    )
    .await;
    let reconnect_writer_manager = reconnect_manager.clone();
    assert!(
        assert_old_or_new_during_writer(reconnect_manager.clone(), async move {
            reconnect_writer_manager
                .publish_reconnected_runtime_if_current(reconnect_expected, reconnect_candidate)
                .await
        })
        .await
        .unwrap()
    );
    assert!(matches!(
        reconnect_manager
            .call_resolved_tool(&old_reconnect, serde_json::json!({}))
            .await,
        Err(McpError::StalePublication { .. })
    ));

    let (stop_events, _stop_event_rx) = mpsc::channel(8);
    let stop_manager = Arc::new(McpServerManager::new().with_event_channel(stop_events));
    let (stop_transport, stop_controls) = MockTransport::new(vec![tool("echo", "stop")], "stop");
    publish_mock(
        &stop_manager,
        prepare_mock(
            &stop_manager,
            "stop",
            vec![tool("echo", "stop")],
            stop_transport,
        )
        .await,
    )
    .await;
    let stop_snapshot = stop_manager.snapshot();
    let stopped_ticket = stop_snapshot
        .resolve_call(&stop_snapshot.aliases()[0].alias)
        .unwrap();
    let stop_writer_manager = stop_manager.clone();
    assert_old_or_new_during_writer(stop_manager.clone(), async move {
        stop_writer_manager.stop_server("stop").await
    })
    .await
    .unwrap();
    assert!(matches!(
        stop_manager
            .call_resolved_tool(&stopped_ticket, serde_json::json!({}))
            .await,
        Err(McpError::StalePublication { .. })
    ));
    assert_eq!(stop_controls.calls.load(Ordering::SeqCst), 0);

    let old_dir = tempfile::tempdir().unwrap();
    let new_dir = tempfile::tempdir().unwrap();
    let old_config = fixture_config(&old_dir, &["old-a", "old-b"]).unwrap();
    let new_config = fixture_config(&new_dir, &["new-a", "new-b"]).unwrap();
    let (config_events, _config_event_rx) = mpsc::channel(32);
    let config_manager = Arc::new(McpServerManager::new().with_event_channel(config_events));
    config_manager
        .reconcile_from_config_transactional(&old_config)
        .await
        .unwrap();
    let old_snapshot = config_manager.snapshot();
    let old_tickets = old_snapshot
        .aliases()
        .iter()
        .map(|alias| old_snapshot.resolve_call(&alias.alias).unwrap())
        .collect::<Vec<_>>();
    let config_writer_manager = config_manager.clone();
    assert_old_or_new_during_writer(config_manager.clone(), async move {
        config_writer_manager
            .reconcile_from_config_transactional(&new_config)
            .await
    })
    .await
    .unwrap();
    for old_ticket in &old_tickets {
        assert!(matches!(
            config_manager
                .call_resolved_tool(old_ticket, serde_json::json!({}))
                .await,
            Err(McpError::StalePublication { .. })
        ));
    }
    let current = config_manager.snapshot();
    assert_eq!(
        current.server_ids(),
        vec!["new-a".to_string(), "new-b".to_string()]
    );
    let current_ticket = current.resolve_call(&current.aliases()[0].alias).unwrap();
    config_manager
        .call_resolved_tool(&current_ticket, serde_json::json!({}))
        .await
        .unwrap();

    start_manager.shutdown_all().await;
    refresh_manager.shutdown_all().await;
    reconnect_manager.shutdown_all().await;
    config_manager.shutdown_all().await;
}

#[tokio::test]
async fn snapshots_are_old_or_new_during_bulk_publication() {
    let manager = Arc::new(McpServerManager::new());
    for server_id in ["old-a", "old-b"] {
        let (transport, _) = MockTransport::new(vec![tool("echo", server_id)], server_id);
        publish_mock(
            &manager,
            prepare_mock(
                &manager,
                server_id,
                vec![tool("echo", server_id)],
                transport,
            )
            .await,
        )
        .await;
    }
    let mut replacements = Vec::new();
    for server_id in ["new-a", "new-b"] {
        let (transport, _) = MockTransport::new(vec![tool("echo", server_id)], server_id);
        let staged = prepare_mock(
            &manager,
            server_id,
            vec![tool("echo", server_id)],
            transport,
        )
        .await;
        let mut commit = staged.into_commit();
        let publication = commit.publication().clone();
        commit.mark_published();
        commit.discard_activation();
        replacements.push(publication);
    }
    let base = manager.authority.generation();
    let next = McpRuntimeGeneration::plan(
        &base,
        &replacements,
        &["old-a".to_string(), "old-b".to_string()],
        manager.authority.ledger_relationship_limit,
        true,
    )
    .unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let reader_manager = manager.clone();
    let reader_barrier = barrier.clone();
    let reader = std::thread::spawn(move || {
        reader_barrier.wait();
        for _ in 0..2_000 {
            let snapshot = reader_manager.snapshot();
            assert_snapshot_coherent(&snapshot);
            let ids = snapshot.server_ids().into_iter().collect::<BTreeSet<_>>();
            assert!(
                ids == BTreeSet::from(["old-a".to_string(), "old-b".to_string()])
                    || ids == BTreeSet::from(["new-a".to_string(), "new-b".to_string()])
            );
        }
    });
    barrier.wait();
    manager
        .authority
        .replace_prevalidated_with(&base, next, || {
            for publication in base.servers.values() {
                publication.retire_with_runtime();
            }
        });
    reader.join().unwrap();
    assert_eq!(
        manager.snapshot().server_ids(),
        vec!["new-a".to_string(), "new-b".to_string()]
    );
}

fn python_command() -> Option<&'static str> {
    ["python3", "python"].into_iter().find(|command| {
        std::process::Command::new(command)
            .arg("--version")
            .output()
            .is_ok_and(|output| output.status.success())
    })
}

fn fixture_config(temp: &tempfile::TempDir, ids: &[&str]) -> Option<McpConfig> {
    let python = python_command()?;
    let script = temp.path().join("mcp_fixture.py");
    std::fs::write(
        &script,
        r#"import json
import sys
for line in sys.stdin:
    request = json.loads(line)
    request_id = request.get("id")
    if request_id is None:
        continue
    method = request.get("method")
    if method == "server/discover":
        print(json.dumps({"jsonrpc":"2.0","id":request_id,"error":{"code":-32601,"message":"missing"}}), flush=True)
        continue
    if method == "initialize":
        result = {"protocolVersion":"2024-11-05","capabilities":{"tools":{"listChanged":False}},"serverInfo":{"name":"fixture","version":"1"}}
    elif method == "tools/list":
        result = {"tools":[{"name":"echo","description":"fixture","inputSchema":{"type":"object"}}]}
    elif method == "tools/call":
        result = {"content":[{"type":"text","text":"fixture"}],"isError":False}
    else:
        result = {}
    print(json.dumps({"jsonrpc":"2.0","id":request_id,"result":result}), flush=True)
"#,
    )
    .expect("write fixture");
    Some(McpConfig {
        version: 1,
        servers: ids
            .iter()
            .map(|id| {
                let mut config = test_config(id);
                config.transport = TransportConfig::Stdio(StdioConfig {
                    command: python.to_string(),
                    args: vec![script.to_string_lossy().into_owned()],
                    cwd: None,
                    env: HashMap::new(),
                    env_encrypted: HashMap::new(),
                    env_credential_refs: HashMap::new(),
                    startup_timeout_ms: 2_000,
                });
                config
            })
            .collect(),
    })
}

#[tokio::test]
async fn cancel_before_durable_and_after_durable_boundary() {
    if python_command().is_none() {
        return;
    }
    // Keep the script directory alive independently for each phase.
    let before_dir = tempfile::tempdir().unwrap();
    let before_config = fixture_config(&before_dir, &["candidate"]).unwrap();
    let task_phases = Arc::new(StdMutex::new(Vec::new()));
    let mut probed_manager = McpServerManager::new();
    let recorded_task_phases = task_phases.clone();
    probed_manager.task_probe = Some(Arc::new(move |phase, task_count| {
        recorded_task_phases
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((phase, task_count));
    }));
    let manager = Arc::new(probed_manager);
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let before_manager = manager.clone();
    let before = tokio::spawn(async move {
        before_manager
            .reconcile_from_config_transactional_after(&before_config, || async move {
                let _ = entered_tx.send(());
                pending::<Result<()>>().await
            })
            .await
    });
    entered_rx
        .await
        .expect("candidate staged before durable CAS");
    before.abort();
    let _ = before.await;
    assert!(manager.snapshot().server_ids().is_empty());
    let before_phases = task_phases
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    assert_eq!(
        before_phases,
        vec![
            (TaskProbePhase::PreparedAndGated, 2),
            (TaskProbePhase::Dropped, 2),
        ]
    );
    task_phases
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clear();

    let after_dir = tempfile::tempdir().unwrap();
    let after_config = fixture_config(&after_dir, &["candidate"]).expect("python fixture config");
    let (durable_tx, durable_rx) = tokio::sync::oneshot::channel();
    let after_manager = manager.clone();
    let after = tokio::spawn(async move {
        after_manager
            .reconcile_from_config_transactional_after(&after_config, || async move {
                let _ = durable_tx.send(());
                Ok(())
            })
            .await
    });
    durable_rx
        .await
        .expect("durable callback returned Ready(Ok)");
    after.abort();
    let _ = after.await;
    assert_eq!(
        manager.snapshot().server_ids(),
        vec!["candidate".to_string()]
    );
    assert_snapshot_coherent(&manager.snapshot());
    assert_eq!(
        *task_phases
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![
            (TaskProbePhase::PreparedAndGated, 2),
            (TaskProbePhase::Transferred, 2),
            (TaskProbePhase::Activated, 2),
        ]
    );
    manager.shutdown_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_publication_prepares_tasks_and_activates_in_one_sync_phase() {
    let directory = tempfile::tempdir().unwrap();
    let Some(config) = fixture_config(&directory, &["candidate"]) else {
        return;
    };
    let order = Arc::new(StdMutex::new(Vec::<&'static str>::new()));
    let (event_tx, mut event_rx) = mpsc::channel(4);
    let mut manager = McpServerManager::new().with_event_channel(event_tx);

    let task_order = order.clone();
    manager.task_probe = Some(Arc::new(move |phase, task_count| {
        assert_eq!(task_count, 2);
        let label = match phase {
            TaskProbePhase::PreparedAndGated => "task_prepared",
            TaskProbePhase::Transferred => "task_transferred",
            TaskProbePhase::Activated => "task_activated",
            TaskProbePhase::Dropped => "task_dropped",
        };
        task_order
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(label);
    }));
    let publish_order = order.clone();
    manager.publish_probe = Some(Arc::new(move |phase| {
        let label = match phase {
            PublishProbePhase::BeforeFenceAndSwap => "publish_before_fence",
            PublishProbePhase::AfterFencesBeforeSwap => "publish_after_fence",
            PublishProbePhase::AfterTransferAndSwapBeforeUnlock => "publish_after_transfer",
        };
        publish_order
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(label);
    }));
    let event_order = order.clone();
    manager.event_probe = Some(Arc::new(move |phase| {
        let label = match phase {
            EventProbePhase::BeforeBatchValidation => "event_before_validation",
            EventProbePhase::AcceptedBatchBeforeFirstDelivery => "event_accepted",
            EventProbePhase::BeforeOutputSend => "event_before_send",
        };
        event_order
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(label);
    }));

    let callback_order = order.clone();
    manager
        .reconcile_from_config_transactional_after(&config, || async move {
            let mut order = callback_order
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            order.push("callback_enter");
            order.push("callback_ok");
            Ok(())
        })
        .await
        .unwrap();
    assert!(matches!(
        event_rx.recv().await,
        Some(McpEvent::ServerStatusChanged {
            status: ServerStatus::Ready,
            ..
        })
    ));
    assert!(matches!(
        event_rx.recv().await,
        Some(McpEvent::ToolsChanged { .. })
    ));

    let order = order
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    let position = |label| {
        order
            .iter()
            .position(|entry| *entry == label)
            .unwrap_or_else(|| panic!("missing phase {label}: {order:?}"))
    };
    assert!(position("task_prepared") < position("callback_enter"));
    assert!(position("event_before_validation") < position("callback_enter"));
    assert!(position("callback_enter") < position("callback_ok"));
    assert!(position("callback_ok") < position("publish_before_fence"));
    assert!(position("publish_before_fence") < position("publish_after_fence"));
    assert!(position("publish_after_fence") < position("task_transferred"));
    assert!(position("task_transferred") < position("task_activated"));
    assert!(position("task_activated") < position("publish_after_transfer"));
    assert!(position("publish_after_transfer") < position("event_accepted"));
    assert!(position("event_accepted") < position("event_before_send"));
    assert!(!order.contains(&"task_dropped"));
    manager.shutdown_all().await;
}

#[tokio::test]
async fn rejected_durable_callback_drops_the_whole_event_batch() {
    let directory = tempfile::tempdir().unwrap();
    let Some(config) = fixture_config(&directory, &["candidate"]) else {
        return;
    };
    let phases = Arc::new(StdMutex::new(Vec::new()));
    let recorded = phases.clone();
    let (event_tx, mut event_rx) = mpsc::channel(1);
    let mut manager = McpServerManager::new().with_event_channel(event_tx);
    manager.event_probe = Some(Arc::new(move |phase| {
        recorded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(phase);
    }));

    let error = manager
        .reconcile_from_config_transactional_after(&config, || async {
            Err(McpError::Connection(
                "durable callback rejected".to_string(),
            ))
        })
        .await
        .expect_err("rejected durable callback must cancel publication");
    assert!(matches!(error, McpError::Connection(_)));
    assert!(manager.snapshot().server_ids().is_empty());
    assert!(
        tokio::time::timeout(Duration::from_millis(50), event_rx.recv())
            .await
            .is_err()
    );
    assert_eq!(
        *phases
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![EventProbePhase::BeforeBatchValidation]
    );
}

#[tokio::test]
async fn all_failures_precede_durable_callback() {
    let manager = McpServerManager::new();
    manager.index.set_revision_for_test(u64::MAX);
    let revision_callbacks = Arc::new(AtomicUsize::new(0));
    let revision_callback = revision_callbacks.clone();
    let error = manager
        .reconcile_from_config_transactional_after(
            &McpConfig {
                version: 1,
                servers: Vec::new(),
            },
            || async move {
                revision_callback.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .await
        .expect_err("revision exhaustion must preflight before durable CAS");
    assert!(matches!(
        error,
        McpError::ToolRegistration(ToolRegistrationError::PublicationRevisionExhausted)
    ));
    assert_eq!(revision_callbacks.load(Ordering::SeqCst), 0);

    let collision_dir = tempfile::tempdir().unwrap();
    let Some(collision_config) = fixture_config(&collision_dir, &["first", "second"]) else {
        return;
    };
    let first_alias = Arc::new(StdMutex::new(None::<String>));
    let mut collision_manager = McpServerManager::new();
    let collision_alias = first_alias.clone();
    collision_manager.catalog_plan_probe = Some(Arc::new(move |_, catalog| {
        let mut first = collision_alias
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(alias) = first.as_ref() {
            catalog.replace_first_canonical_alias_for_test(alias.clone());
        } else {
            *first = Some(catalog.aliases()[0].alias.clone());
        }
    }));
    let collision_callbacks = Arc::new(AtomicUsize::new(0));
    let collision_callback = collision_callbacks.clone();
    let collision = collision_manager
        .reconcile_from_config_transactional_after(&collision_config, || async move {
            collision_callback.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await
        .expect_err("collision must fail inside transactional generation planning");
    assert!(matches!(
        collision,
        McpError::ToolRegistration(ToolRegistrationError::AliasCollision { .. })
    ));
    assert_eq!(collision_callbacks.load(Ordering::SeqCst), 0);
    assert!(collision_manager.snapshot().server_ids().is_empty());

    let authority = GenerationAuthority::new(1);
    let limited = McpServerManager {
        index: Arc::new(ToolIndex::from_authority(authority.clone())),
        authority,
        event_tx: None,
        config: None,
        event_sequence_lock: Arc::new(tokio::sync::Mutex::new(())),
        reconcile_lock: Arc::new(tokio::sync::Mutex::new(())),
        next_publication_id: Arc::new(AtomicU64::new(1)),
        next_runtime_id: Arc::new(AtomicU64::new(1)),
        publish_probe: None,
        event_probe: None,
        task_probe: None,
        catalog_plan_probe: None,
    };
    let capacity_dir = tempfile::tempdir().unwrap();
    let capacity_config = fixture_config(&capacity_dir, &["capacity"]).unwrap();
    let capacity_callbacks = Arc::new(AtomicUsize::new(0));
    let capacity_callback = capacity_callbacks.clone();
    let error = limited
        .reconcile_from_config_transactional_after(&capacity_config, || async move {
            capacity_callback.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await
        .expect_err("ledger capacity must fail inside transactional generation planning");
    assert!(matches!(
        error,
        McpError::ToolRegistration(ToolRegistrationError::OwnershipLedgerCapacityExceeded { .. })
    ));
    assert_eq!(capacity_callbacks.load(Ordering::SeqCst), 0);
    assert!(limited.snapshot().server_ids().is_empty());

    let schema_dir = tempfile::tempdir().unwrap();
    let schema_config = fixture_config(&schema_dir, &["schema"]).unwrap();
    let mut schema_manager = McpServerManager::new();
    schema_manager.catalog_plan_probe = Some(Arc::new(|_, catalog| {
        catalog.replace_first_original_name_for_test("provider-schema-missing".to_string());
    }));
    let schema_callbacks = Arc::new(AtomicUsize::new(0));
    let schema_callback = schema_callbacks.clone();
    let schema_error = schema_manager
        .reconcile_from_config_transactional_after(&schema_config, || async move {
            schema_callback.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await
        .expect_err("schema mismatch must fail inside transactional candidate staging");
    assert!(matches!(
        schema_error,
        McpError::ToolRegistration(ToolRegistrationError::ProviderSchemaUnavailable)
    ));
    assert_eq!(schema_callbacks.load(Ordering::SeqCst), 0);
    assert!(schema_manager.snapshot().server_ids().is_empty());
}

#[tokio::test]
async fn legacy_initialize_and_facade_are_generation_coherent() {
    let directory = tempfile::tempdir().unwrap();
    let Some(config) = fixture_config(&directory, &["a", "b"]) else {
        return;
    };
    let manager = Arc::new(McpServerManager::new());
    manager.initialize_from_config(&config).await;
    let snapshot = manager.snapshot();
    assert_eq!(
        snapshot.server_ids(),
        vec!["a".to_string(), "b".to_string()]
    );
    assert_eq!(snapshot.aliases(), manager.tool_index().all_aliases());
    assert_snapshot_coherent(&snapshot);

    let attached = McpToolExecutor::from_manager(manager.clone());
    assert_eq!(attached.list_tools().len(), 2);
    let detached = McpToolExecutor::new(manager.clone(), Arc::new(ToolIndex::new()));
    assert!(detached.list_tools().is_empty());
    let call = ToolCall {
        id: "detached".to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: snapshot.aliases()[0].alias.clone(),
            arguments: "{}".to_string(),
        },
    };
    assert!(matches!(
        detached.execute(&call).await,
        Err(bamboo_agent_core::ToolError::NotFound(_))
    ));
    manager.shutdown_all().await;
}

#[tokio::test]
async fn rejected_staged_runtime_drops_transport_without_snapshot_retention() {
    let manager = McpServerManager::new();
    let (transport, controls) = MockTransport::new(vec![tool("echo", "candidate")], "candidate");
    let staged = prepare_mock(
        &manager,
        "candidate",
        vec![tool("echo", "candidate")],
        transport,
    )
    .await;
    let runtime = staged.publication().runtime.clone();
    drop(staged);
    assert_eq!(runtime.fence_state(), FenceState::Closed);
    assert_eq!(runtime.active_calls(), 0);
    assert_eq!(controls.drops.load(Ordering::SeqCst), 1);
}
