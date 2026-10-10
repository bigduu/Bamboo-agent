use super::*;
use crate::config::{ReconnectConfig, StdioConfig};
use crate::executor::McpToolExecutor;
use crate::protocol::models::JsonRpcNotification;
use bamboo_agent_core::{FunctionCall, ToolCall, ToolExecutor};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex as StdMutex;
use tokio::sync::mpsc;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

const SERVER_SECRET: &str = "server-api-key-unique-996";
const TOOL_SECRET: &str = "tool-bearer-unique-996";
const CALL_SECRET: &str = "call-token-unique-996";
const ARG_SECRET: &str = "argument-password-unique-996";
const ERROR_SECRET: &str = "remote-private-error-unique-996";
const SCHEMA_SECRET: &str = "schema-private-content-unique-996";
const METHOD_SECRET: &str = "notification-private-method-unique-996";

#[derive(Clone, Default)]
struct Capture(Arc<StdMutex<Vec<BTreeMap<String, String>>>>);

impl Capture {
    fn events(&self) -> Vec<BTreeMap<String, String>> {
        self.0.lock().unwrap().clone()
    }

    fn assert_private(&self) {
        let events = self.events();
        assert!(!events.is_empty(), "capture must observe real MCP events");
        let text = format!("{events:?}");
        for secret in [
            SERVER_SECRET,
            TOOL_SECRET,
            CALL_SECRET,
            ARG_SECRET,
            ERROR_SECRET,
            SCHEMA_SECRET,
            METHOD_SECRET,
        ] {
            assert!(
                !text.contains(secret),
                "raw fixture value in MCP log: {secret}"
            );
        }
        for event in &events {
            assert!(!event.contains_key("args_preview"));
            for (field, value) in event {
                if field.ends_with("_id") {
                    assert_eq!(value.len(), 64, "unbounded diagnostic {field}");
                    assert!(value.bytes().all(|byte| byte.is_ascii_hexdigit()));
                }
            }
        }
    }

    fn has_phase(&self, phase: &str) -> bool {
        self.events()
            .iter()
            .any(|event| event.get("phase").is_some_and(|value| value == phase))
    }
}

struct EventFields(BTreeMap<String, String>);

impl Visit for EventFields {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().into(), format!("{value:?}"));
    }
}

// A scoped, thread-local capture needs no new dependency or global subscriber.
// SDK wire tracing is outside #996; capture the Bamboo MCP adapter's events.
impl Subscriber for Capture {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target().starts_with("bamboo_mcp")
    }
    fn new_span(&self, _span: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _span: &Id, _values: &Record<'_>) {}
    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}
    fn event(&self, event: &Event<'_>) {
        let mut fields = EventFields(BTreeMap::new());
        event.record(&mut fields);
        self.0.lock().unwrap().push(fields.0);
    }
    fn enter(&self, _span: &Id) {}
    fn exit(&self, _span: &Id) {}
}

#[derive(Clone)]
struct ReplyState {
    fail_call: bool,
    arguments: Vec<Value>,
}

/// Use the production rmcp client/manager/executor with an in-memory peer.
/// Only the remote responses are fixtures; no execution or log seam is mocked.
struct FixtureTransport {
    responses: mpsc::Receiver<String>,
    sender: mpsc::Sender<String>,
    state: Arc<StdMutex<ReplyState>>,
}

impl FixtureTransport {
    fn new() -> (Self, Arc<StdMutex<ReplyState>>) {
        let (sender, responses) = mpsc::channel(32);
        let state = Arc::new(StdMutex::new(ReplyState {
            fail_call: false,
            arguments: Vec::new(),
        }));
        (
            Self {
                responses,
                sender,
                state: state.clone(),
            },
            state,
        )
    }
}

impl rmcp::transport::Transport<rmcp::RoleClient> for FixtureTransport {
    type Error = McpError;

    fn send(
        &mut self,
        message: rmcp::model::ClientJsonRpcMessage,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'static {
        let request = serde_json::to_value(message).unwrap();
        let sender = self.sender.clone();
        let state = self.state.clone();
        async move {
            if request.get("id").is_none() {
                return Ok(());
            }
            let method = request["method"].as_str().unwrap();
            let response = match method {
                "server/discover" => json!({
                    "jsonrpc":"2.0", "id":request["id"],
                    "error":{"code":-32601,"message":"legacy fixture"}
                }),
                "initialize" => json!({
                    "jsonrpc":"2.0", "id":request["id"], "result":{
                        "protocolVersion":"2025-11-25",
                        "capabilities":{"tools":{}},
                        "serverInfo":{"name":SERVER_SECRET,"version":"1"}
                    }
                }),
                "tools/list" => json!({
                    "jsonrpc":"2.0", "id":request["id"], "result":{"tools":[
                        {"name":TOOL_SECRET,"inputSchema":{"type":"object"}},
                        {"name":SCHEMA_SECRET,"inputSchema":{"type":"object"},
                         "outputSchema":{"type":SCHEMA_SECRET}}
                    ]}
                }),
                "tools/call" => {
                    let fail_call = {
                        let mut state = state.lock().unwrap();
                        state.arguments.push(request["params"]["arguments"].clone());
                        state.fail_call
                    };
                    if fail_call {
                        json!({"jsonrpc":"2.0", "id":request["id"], "error":{
                            "code":-32000,"message":ERROR_SECRET,
                            "data":{"credential":ARG_SECRET}
                        }})
                    } else {
                        json!({"jsonrpc":"2.0", "id":request["id"], "result":{
                            "content":[{"type":"text","text":ARG_SECRET}],"isError":false
                        }})
                    }
                }
                _ => json!({"jsonrpc":"2.0", "id":request["id"], "result":{}}),
            };
            sender
                .send(response.to_string())
                .await
                .map_err(|_| McpError::Disconnected)
        }
    }

    async fn receive(&mut self) -> Option<rmcp::model::ServerJsonRpcMessage> {
        Some(serde_json::from_str(&self.responses.recv().await?).unwrap())
    }

    async fn close(&mut self) -> Result<()> {
        self.responses.close();
        Ok(())
    }
}

fn config(id: &str) -> McpServerConfig {
    McpServerConfig {
        id: id.into(),
        name: None,
        enabled: true,
        transport: TransportConfig::Stdio(StdioConfig {
            command: "bamboo-996-missing-fixture-command".into(),
            args: Vec::new(),
            cwd: None,
            env: HashMap::new(),
            env_encrypted: HashMap::new(),
            env_credential_refs: HashMap::new(),
            startup_timeout_ms: 1_000,
        }),
        request_timeout_ms: 5_000,
        healthcheck_interval_ms: 60_000,
        reconnect: ReconnectConfig {
            enabled: false,
            initial_backoff_ms: 1,
            max_backoff_ms: 1,
            max_attempts: 1,
        },
        allowed_tools: Vec::new(),
        denied_tools: Vec::new(),
    }
}

async fn fixture_manager() -> (Arc<McpServerManager>, Arc<StdMutex<ReplyState>>) {
    let manager = Arc::new(McpServerManager::new());
    let (transport, state) = FixtureTransport::new();
    let client = McpProtocolClient::new(transport);
    client.initialize(5_000).await.unwrap();
    let tools = client.list_tools(5_000).await.unwrap();
    assert_eq!(tools.len(), 1, "invalid output schema remains excluded");
    assert_eq!(tools[0].name, TOOL_SECRET);
    let catalog = manager
        .index
        .plan_server_tools(SERVER_SECRET, &tools, &[], &[])
        .unwrap();
    let runtime = TransportRuntime::new(
        manager.allocate_runtime_id().unwrap(),
        ServerRuntime {
            config: config(SERVER_SECRET),
            info: tokio::sync::RwLock::new(RuntimeInfo {
                status: ServerStatus::Ready,
                tool_count: tools.len(),
                ..RuntimeInfo::default()
            }),
            reconnecting: AtomicBool::new(false),
            qos: McpServerQos::new(McpQosConfig::default()),
            proxy_fingerprint: None,
        },
        client,
    );
    let publication = ServerPublication::new(
        manager.allocate_publication_id().unwrap(),
        runtime,
        catalog,
        &tools,
    )
    .unwrap();
    let base = manager.authority.generation();
    let next = McpRuntimeGeneration::plan(
        &base,
        &[publication],
        &[],
        manager.authority.ledger_relationship_limit,
        true,
    )
    .unwrap();
    manager.authority.replace_prevalidated(&base, next);
    (manager, state)
}

#[test]
fn mcp_log_ids_hash_entire_labels_and_frame_owner_components() {
    let id = diagnostic_id("server", &[SERVER_SECRET]);
    assert_eq!(id, diagnostic_id("server", &[SERVER_SECRET]));
    assert_ne!(id, diagnostic_id("tool", &[SERVER_SECRET]));
    assert_ne!(
        diagnostic_id("owner", &["ab", "c"]),
        diagnostic_id("owner", &["a", "bc"])
    );
    let label = format!("mcp__safe_tag__{TOOL_SECRET}{}", "秘密".repeat(5_000));
    let hashed = diagnostic_id("tool", &[&label]);
    assert_eq!(hashed.len(), 64);
    assert!(hashed.bytes().all(|byte| byte.is_ascii_hexdigit()));
    assert!(!hashed.contains(TOOL_SECRET));
}

#[tokio::test(flavor = "current_thread")]
async fn mcp_log_privacy_preserves_real_execute_arguments_results_and_remote_errors() {
    let capture = Capture::default();
    let dispatch = tracing::Dispatch::new(capture.clone());
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let (manager, state) = fixture_manager().await;
    let alias = manager.snapshot().aliases()[0].alias.clone();
    let executor = McpToolExecutor::from_manager(manager.clone());
    let call = ToolCall {
        id: CALL_SECRET.into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: alias,
            // Missing brace exercises the real repair warning, which contains
            // the repaired raw preview but must never enter operational logs.
            arguments: format!("{{\"credential\":\"{ARG_SECRET}\""),
        },
    };
    let result = executor.execute(&call).await.unwrap();
    assert!(result.success);
    assert_eq!(result.result, ARG_SECRET, "tool output must remain intact");
    assert_eq!(
        state.lock().unwrap().arguments,
        [json!({"credential":ARG_SECRET})]
    );

    state.lock().unwrap().fail_call = true;
    let error = executor.execute(&call).await.unwrap_err();
    assert!(
        error.to_string().contains(ERROR_SECRET),
        "returned error is unchanged"
    );
    assert_eq!(state.lock().unwrap().arguments.len(), 2);
    assert!(capture.has_phase("parse_arguments"));
    assert!(capture.has_phase("execute"));
    assert!(capture.events().iter().any(|event| {
        event
            .get("error_kind")
            .is_some_and(|kind| kind == "remote_protocol")
            && event
                .get("error_text_len")
                .is_some_and(|len| len == &ERROR_SECRET.len().to_string())
    }));
    let expected_owner = diagnostic_id("owner", &[SERVER_SECRET, TOOL_SECRET]);
    assert!(capture
        .events()
        .iter()
        .filter(|event| event.contains_key("owner_id"))
        .all(|event| event.get("owner_id") == Some(&expected_owner)));
    capture.assert_private();
    manager.shutdown_all().await;
}

#[tokio::test(flavor = "current_thread")]
async fn mcp_log_privacy_covers_manager_lifecycle_qos_config_and_client_schema() {
    let capture = Capture::default();
    let dispatch = tracing::Dispatch::new(capture.clone());
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let (manager, _) = fixture_manager().await;
    manager.refresh_tools(SERVER_SECRET).await.unwrap();
    let expected = manager.current_expected(SERVER_SECRET).unwrap();
    assert_eq!(
        manager
            .publish_health_result_if_current(expected.clone(), Err(ERROR_SECRET.into()))
            .await,
        Some(false)
    );
    assert_eq!(
        expected
            .runtime()
            .runtime
            .info
            .read()
            .await
            .last_error
            .as_deref(),
        Some(ERROR_SECRET)
    );
    manager
        .dispatch_server_notification(
            expected,
            JsonRpcNotification {
                jsonrpc: "2.0".into(),
                method: METHOD_SECRET.into(),
                params: Some(json!({"token":ARG_SECRET})),
            },
        )
        .await;

    let qos = McpServerQos::new(McpQosConfig {
        circuit_failure_threshold: 1,
        reconnect_failure_threshold: 2,
        ..McpQosConfig::default()
    });
    let error = McpError::RemoteProtocol {
        code: -32000,
        message: ERROR_SECRET.into(),
        data: Some(json!({"token":ARG_SECRET})),
    };
    assert!(!qos.record_failure(SERVER_SECRET, TOOL_SECRET, &error).await);
    assert!(qos.record_failure(SERVER_SECRET, TOOL_SECRET, &error).await);
    let bad_config = McpConfig {
        servers: vec![config(SERVER_SECRET), config(SERVER_SECRET)],
        ..McpConfig::default()
    };
    manager.reconcile_from_config(&bad_config).await;
    manager.initialize_from_config(&bad_config).await;
    assert!(
        manager.is_server_running(SERVER_SECRET),
        "failed config cannot replace runtime"
    );
    assert!(manager.start_server(config(METHOD_SECRET)).await.is_err());
    manager.stop_server(SERVER_SECRET).await.unwrap();
    assert!(!manager.is_server_running(SERVER_SECRET));
    for phase in [
        "tools_list",
        "refresh_tools",
        "health_check",
        "notification",
        "qos_circuit",
        "qos_recycle",
        "config_reconcile",
        "config_initialize",
        "start",
        "stop",
    ] {
        assert!(capture.has_phase(phase), "missing real trace phase {phase}");
    }
    capture.assert_private();
}
