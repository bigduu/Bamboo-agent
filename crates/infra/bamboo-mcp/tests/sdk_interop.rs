use bamboo_mcp::{McpError, McpProtocolClient, McpStructuredContent};
use rmcp::model::*;
use rmcp::service::RequestContext;
use rmcp::{RoleServer, ServerHandler, ServiceExt};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

#[derive(Clone)]
struct Fixture {
    calls: Arc<AtomicUsize>,
    cancelled: Arc<AtomicUsize>,
    subscription_tx: Option<tokio::sync::mpsc::UnboundedSender<rmcp::service::SubscriptionSink>>,
    output_schema: Option<Value>,
}

impl ServerHandler for Fixture {
    fn get_info(&self) -> ServerConfig {
        let capabilities = ServerCapabilities::builder().enable_tools();
        let capabilities = if self.subscription_tx.is_some() {
            capabilities.enable_tool_list_changed()
        } else {
            capabilities
        };
        ServerConfig::new(capabilities.build())
            .with_server_info(Implementation::new("fixture", "1"))
            .with_instructions("SDK fixture instructions")
    }
    fn accepted_subscription_filter(
        &self,
        requested: &SubscriptionFilter,
    ) -> Option<SubscriptionFilter> {
        self.subscription_tx.as_ref().map(|_| requested.clone())
    }
    async fn listen(
        &self,
        context: rmcp::service::SubscriptionContext,
    ) -> Result<(), rmcp::ErrorData> {
        if let Some(tx) = &self.subscription_tx {
            let _ = tx.send(context.sink().clone());
        }
        context.cancelled().await;
        Ok(())
    }

    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, rmcp::ErrorData> {
        let second = request.and_then(|request| request.cursor).is_some();
        let mut definition = json!({"name":if second {"slow"} else {"echo"},
            "description":"SDK interop", "inputSchema":{"type":"object"}});
        if let Some(schema) = &self.output_schema {
            definition["outputSchema"] = schema.clone();
        }
        let tool: Tool = serde_json::from_value(definition).unwrap();
        Ok(ListToolsResult {
            next_cursor: (!second).then(|| "second-page".into()),
            ..ListToolsResult::with_all_items(vec![tool])
        })
    }
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, rmcp::ErrorData> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if request.name == "slow" {
            context.ct.cancelled().await;
            self.cancelled.fetch_add(1, Ordering::SeqCst);
            return Err(rmcp::ErrorData::internal_error("cancelled", None));
        }
        if request.name == "error" {
            return Err(rmcp::ErrorData::invalid_params(
                "fixture error",
                Some(json!({"reason":"expected"})),
            ));
        }
        let value = request
            .arguments
            .and_then(|args| args.get("value").cloned())
            .unwrap_or(json!([1, 2, 3]));
        Ok(CallToolResult::structured(value).into())
    }
}

fn fixture() -> Fixture {
    Fixture {
        calls: Arc::new(AtomicUsize::new(0)),
        cancelled: Arc::new(AtomicUsize::new(0)),
        subscription_tx: None,
        output_schema: None,
    }
}

async fn duplex() -> (
    McpProtocolClient,
    rmcp::service::RunningService<RoleServer, Fixture>,
) {
    let (client_io, server_io) = tokio::io::duplex(32 * 1024);
    let client = McpProtocolClient::new(client_io);
    let (info, server) = tokio::join!(client.initialize(1_000), fixture().serve(server_io));
    let info = info.unwrap();
    assert_eq!(info.protocol_version, ProtocolVersion::V_2026_07_28);
    assert_eq!(
        info.instructions.as_deref(),
        Some("SDK fixture instructions")
    );
    (client, server.unwrap())
}

#[tokio::test]
async fn modern_sdk_discovery_pagination_and_structured_result() {
    let (mut client, server) = duplex().await;
    assert!(client.is_connected().await);
    let tools = client.list_tools(1_000).await.unwrap();
    assert_eq!(
        tools
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>(),
        vec!["echo", "slow"]
    );
    let result = client
        .call_tool("echo", json!({"value":["typed", 7]}), 1_000)
        .await
        .unwrap();
    assert_eq!(
        result.structured_content,
        McpStructuredContent::Value(json!(["typed", 7]))
    );
    client.ping(1_000).await.unwrap();
    client.disconnect().await.unwrap();
    assert!(!client.is_connected().await);
    server.waiting().await.unwrap();
}

async fn assert_idn_output_schema_validation(draft: &str, format: &str, valid: &str) {
    let schema = json!({
        "$schema": draft,
        "type": "object",
        "properties": {"result": {"type": "string", "format": format}},
        "required": ["result"],
        "additionalProperties": false
    });
    let mut handler = fixture();
    handler.output_schema = Some(schema.clone());
    let (client_io, server_io) = tokio::io::duplex(32 * 1024);
    let mut client = McpProtocolClient::new(client_io);
    let (initialized, server) = tokio::join!(client.initialize(1_000), handler.serve(server_io));
    initialized.unwrap();
    let server = server.unwrap();

    // Discover the advertised schema so production list_tools caches the validator.
    let tools = client.list_tools(1_000).await.unwrap();
    let tool = tools.iter().find(|tool| tool.name == "echo").unwrap();
    assert_eq!(tool.output_schema.as_ref(), Some(&schema));

    let invalid = json!({"result": "not a valid domain or email"});
    match client
        .call_tool("echo", json!({"value": invalid}), 1_000)
        .await
        .unwrap_err()
    {
        McpError::Protocol(message) => {
            assert!(message.contains("structuredContent does not conform to outputSchema"));
            assert!(message.contains(format), "{draft} {format}: {message}");
        }
        error => panic!("{draft} {format}: unexpected error {error}"),
    }

    let valid = json!({"result": valid});
    let result = client
        .call_tool("echo", json!({"value": valid.clone()}), 1_000)
        .await
        .unwrap();
    assert!(!result.is_error);
    assert_eq!(
        result.structured_content,
        McpStructuredContent::Value(valid)
    );
    client.disconnect().await.unwrap();
    server.waiting().await.unwrap();
}

#[tokio::test]
async fn output_schema_rejects_invalid_draft7_idn_hostname_and_accepts_unicode() {
    assert_idn_output_schema_validation(
        "http://json-schema.org/draft-07/schema#",
        "idn-hostname",
        "例え.テスト",
    )
    .await;
}

#[tokio::test]
async fn output_schema_rejects_invalid_draft4_6_7_idn_email_and_accepts_unicode() {
    for draft in [
        "http://json-schema.org/draft-04/schema#",
        "http://json-schema.org/draft-06/schema#",
        "http://json-schema.org/draft-07/schema#",
    ] {
        assert_idn_output_schema_validation(draft, "idn-email", "user@例え.テスト").await;
    }
}

#[tokio::test]
async fn sdk_remote_error_code_and_data_survive_projection() {
    let (mut client, server) = duplex().await;
    match client
        .call_tool("error", json!({}), 1_000)
        .await
        .unwrap_err()
    {
        McpError::RemoteProtocol { code, data, .. } => {
            assert_eq!(code, -32602);
            assert_eq!(data, Some(json!({"reason":"expected"})));
        }
        error => panic!("unexpected error {error}"),
    }
    client.disconnect().await.unwrap();
    server.waiting().await.unwrap();
}

#[tokio::test]
async fn sdk_timeout_cancels_remote_call_and_keeps_connection_usable() {
    let (mut client, server) = duplex().await;
    assert!(matches!(
        client.call_tool("slow", json!({}), 25).await,
        Err(McpError::Timeout(_))
    ));
    client.call_tool("echo", json!({}), 1_000).await.unwrap();
    client.disconnect().await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), server.waiting())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn sdk_preserves_explicit_null_in_structured_content() {
    let (mut client, server) = duplex().await;
    let result = client
        .call_tool("echo", json!({"value":null}), 1_000)
        .await
        .unwrap();
    let content = result.structured_content;
    client.disconnect().await.unwrap();
    server.waiting().await.unwrap();
    assert_eq!(content, McpStructuredContent::Null);
}

#[tokio::test]
async fn streamable_http_uses_official_sdk_transport() {
    use rmcp::transport::streamable_http_server::{
        session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = StreamableHttpService::new(
        || Ok(fixture()),
        LocalSessionManager::default().into(),
        StreamableHttpServerConfig::default().with_json_response(true),
    );
    let task = tokio::spawn(async move {
        axum::serve(listener, axum::Router::new().nest_service("/mcp", service))
            .await
            .unwrap();
    });
    let transport =
        rmcp::transport::StreamableHttpClientTransport::from_uri(format!("http://{addr}/mcp"));
    let mut client = McpProtocolClient::new(transport);
    client.initialize(1_000).await.unwrap();
    assert_eq!(client.list_tools(1_000).await.unwrap().len(), 2);
    assert!(matches!(
        client
            .call_tool("echo", json!({}), 1_000)
            .await
            .unwrap()
            .structured_content,
        McpStructuredContent::Value(Value::Array(_))
    ));
    client.ping(1_000).await.unwrap();
    client.disconnect().await.unwrap();
    task.abort();
}

#[tokio::test]
async fn sdk_falls_back_to_initialize_for_legacy_server() {
    // A pre-discovery server cannot be simulated with a modern SDK server's
    // discovery bootstrap. This fixture models the historical NDJSON endpoint.
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let (client_io, server_io) = tokio::io::duplex(32 * 1024);
    let server = tokio::spawn(async move {
        let (reader, mut writer) = tokio::io::split(server_io);
        let mut lines = BufReader::new(reader).lines();
        let mut methods = Vec::new();
        while let Some(line) = lines.next_line().await.unwrap() {
            let request: Value = serde_json::from_str(&line).unwrap();
            let method = request["method"].as_str().unwrap();
            methods.push(method.to_owned());
            if request.get("id").is_none() {
                continue;
            }
            let mut response = json!({"jsonrpc":"2.0","id":request["id"]});
            match method {
                "server/discover" => {
                    response["error"] = json!({"code":-32601,"message":"legacy server"})
                }
                "initialize" => {
                    response["result"] = json!({"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"legacy","version":"1"}})
                }
                "tools/list" => {
                    response["result"] =
                        json!({"tools":[{"name":"echo","inputSchema":{"type":"object"}}]})
                }
                "tools/call" => {
                    response["result"] = json!({"content":[{"type":"text","text":"legacy"}]})
                }
                "ping" => response["result"] = json!({}),
                _ => panic!("unexpected legacy method {method}"),
            }
            writer
                .write_all(format!("{response}\n").as_bytes())
                .await
                .unwrap();
        }
        methods
    });
    let mut client = McpProtocolClient::new(client_io);
    assert_eq!(
        client.initialize(1_000).await.unwrap().protocol_version,
        ProtocolVersion::LATEST_WITH_INITIALIZE
    );
    assert_eq!(client.list_tools(1_000).await.unwrap().len(), 1);
    client.call_tool("echo", json!({}), 1_000).await.unwrap();
    client.ping(1_000).await.unwrap();
    client.disconnect().await.unwrap();
    let methods = server.await.unwrap();
    assert_eq!(
        &methods[..3],
        &["server/discover", "initialize", "notifications/initialized"]
    );
}

#[tokio::test]
async fn sdk_subscription_routes_tool_changes_and_does_not_duplicate_on_health_check() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let mut handler = fixture();
    handler.subscription_tx = Some(tx);
    let (client_io, server_io) = tokio::io::duplex(32 * 1024);
    let mut client = McpProtocolClient::new(client_io);
    let (info, server) = tokio::join!(client.initialize(1_000), handler.serve(server_io));
    info.unwrap();
    let sink = rx.recv().await.unwrap();
    let mut notifications = client.take_notification_receiver().await.unwrap();
    sink.notify_tool_list_changed().await.unwrap();
    let notification = tokio::time::timeout(Duration::from_secs(1), notifications.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(notification.method, "notifications/tools/list_changed");
    client.ping(1_000).await.unwrap();
    assert!(
        rx.try_recv().is_err(),
        "health checks retain the active subscription"
    );
    client.disconnect().await.unwrap();
    server.unwrap().waiting().await.unwrap();
}

#[tokio::test]
async fn timeout_reaches_the_remote_sdk_handler_before_disconnect() {
    let handler = fixture();
    let cancelled = handler.cancelled.clone();
    let (client_io, server_io) = tokio::io::duplex(32 * 1024);
    let mut client = McpProtocolClient::new(client_io);
    let (info, server) = tokio::join!(client.initialize(1_000), handler.serve(server_io));
    info.unwrap();
    assert!(matches!(
        client.call_tool("slow", json!({}), 25).await,
        Err(McpError::Timeout(_))
    ));
    tokio::time::timeout(Duration::from_secs(1), async {
        while cancelled.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(cancelled.load(Ordering::SeqCst), 1);
    client.disconnect().await.unwrap();
    server.unwrap().waiting().await.unwrap();
}

#[tokio::test]
async fn aborting_bamboo_caller_cancels_remote_call_and_preserves_connection() {
    let handler = fixture();
    let calls = handler.calls.clone();
    let cancelled = handler.cancelled.clone();
    let (client_io, server_io) = tokio::io::duplex(32 * 1024);
    let client = Arc::new(McpProtocolClient::new(client_io));
    let (info, server) = tokio::join!(client.initialize(1_000), handler.serve(server_io));
    info.unwrap();
    let task = tokio::spawn({
        let client = client.clone();
        async move { client.call_tool("slow", json!({}), 10_000).await }
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(1), async {
        while cancelled.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    client.call_tool("echo", json!({}), 1_000).await.unwrap();
    let mut client = Arc::try_unwrap(client).ok().unwrap();
    client.disconnect().await.unwrap();
    server.unwrap().waiting().await.unwrap();
}
