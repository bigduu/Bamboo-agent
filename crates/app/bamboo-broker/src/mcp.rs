//! MCP-over-broker proxy (P2): a remote/deployed worker invokes host-bound MCP
//! servers (e.g. nova — needs the screen/local creds) that physically run on the
//! orchestrator, by forwarding the tool calls over the broker.
//!
//! - Worker side: [`McpProxyExecutor`] advertises the orchestrator's proxiable
//!   MCP tools (fetched as a manifest) and forwards each call. It uses its own
//!   broker sub-connection (`<worker>#mcp`) so proxy replies don't collide with
//!   the worker's main ask mailbox.
//! - Orchestrator side: [`serve_mcp_proxy`] answers `McpRequest`s from a backend
//!   [`ToolExecutor`] (the real `McpServerManager`).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bamboo_agent_core::tools::{
    FunctionCall, ToolCall, ToolError, ToolExecutionContext, ToolExecutor, ToolResult, ToolSchema,
};
use bamboo_subagent::{AgentRef, InboxKind, InboxMessage, MsgId};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::ask::request_over;
use crate::client::BrokerClient;
use crate::error::{BrokerError, BrokerResult};

/// Body of an `McpRequest` (worker → orchestrator).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum McpRequest {
    /// Ask which (host-bound) MCP tools the orchestrator can proxy.
    Manifest,
    /// Invoke a proxiable tool with the LLM-provided JSON arguments string.
    Call { tool: String, arguments: String },
}

/// Body of an `McpReply` (orchestrator → worker).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct McpReply {
    /// Manifest response: the proxiable tool schemas.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest: Option<Vec<ToolSchema>>,
    /// Call response: the tool result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<ProxiedResult>,
    /// Set when the request could not be served.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// A proxied tool result (the wire-safe subset of `ToolResult`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxiedResult {
    pub success: bool,
    pub result: String,
}

// ---- orchestrator side --------------------------------------------------------

/// Run the orchestrator-side MCP proxy: connect as `me`, subscribe, and answer
/// each `McpRequest` from `backend` (the real MCP `ToolExecutor`). Serves until
/// the connection drops.
pub async fn serve_mcp_proxy(
    endpoint: &str,
    me: AgentRef,
    token: &str,
    backend: Arc<dyn ToolExecutor>,
) -> BrokerResult<()> {
    let mut client = BrokerClient::connect(endpoint, me.clone(), token).await?;
    client.subscribe().await?;
    while let Some(msg) = client.next_message().await {
        if msg.kind != InboxKind::McpRequest {
            let _ = client.ack(msg.id).await;
            continue;
        }
        let reply_to = msg.from.session_id.clone();
        let corr = msg.id.clone();
        let reply_body = handle_mcp_request(backend.as_ref(), msg).await;
        let reply = InboxMessage {
            id: MsgId::new(),
            from: me.clone(),
            kind: InboxKind::McpReply,
            body: serde_json::to_value(reply_body).unwrap_or_default(),
            created_at: Utc::now(),
            correlation_id: Some(corr.clone()),
        };
        client.deliver(&reply_to, reply).await?;
        client.ack(corr).await?;
    }
    Ok(())
}

async fn handle_mcp_request(backend: &dyn ToolExecutor, msg: InboxMessage) -> McpReply {
    match serde_json::from_value::<McpRequest>(msg.body) {
        Ok(McpRequest::Manifest) => McpReply {
            manifest: Some(backend.list_tools()),
            ..Default::default()
        },
        Ok(McpRequest::Call { tool, arguments }) => {
            let call = ToolCall {
                id: format!("mcp-{}", MsgId::new().as_str()),
                tool_type: "function".to_string(),
                function: FunctionCall {
                    name: tool,
                    arguments,
                },
            };
            match backend.execute(&call).await {
                Ok(r) => McpReply {
                    result: Some(ProxiedResult {
                        success: r.success,
                        result: r.result,
                    }),
                    ..Default::default()
                },
                Err(e) => McpReply {
                    error: Some(e.to_string()),
                    ..Default::default()
                },
            }
        }
        Err(e) => McpReply {
            error: Some(format!("bad mcp request: {e}")),
            ..Default::default()
        },
    }
}

// ---- worker side --------------------------------------------------------------

/// Worker-side proxy `ToolExecutor`: advertises the orchestrator's proxiable MCP
/// tools and forwards calls to them over the broker.
pub struct McpProxyExecutor {
    client: Mutex<BrokerClient>,
    me: AgentRef,
    orchestrator: String,
    manifest: Vec<ToolSchema>,
    timeout: Duration,
}

impl McpProxyExecutor {
    /// Connect (as `proxy_id` — keep it distinct from the worker's main mailbox,
    /// e.g. `<worker-id>#mcp`), fetch the proxiable-tool manifest from
    /// `orchestrator`, and build. Returns a proxy advertising those tools.
    pub async fn connect(
        endpoint: &str,
        proxy_id: impl Into<String>,
        token: &str,
        orchestrator: impl Into<String>,
        timeout: Duration,
    ) -> BrokerResult<Self> {
        let me = AgentRef {
            session_id: proxy_id.into(),
            role: None,
        };
        let orchestrator = orchestrator.into();
        let mut client = BrokerClient::connect(endpoint, me.clone(), token).await?;
        client.subscribe().await?;

        let reply = request_over(
            &mut client,
            &me,
            &orchestrator,
            InboxKind::McpRequest,
            serde_json::to_value(McpRequest::Manifest).expect("McpRequest serializes"),
            timeout,
        )
        .await?;
        let reply: McpReply = serde_json::from_value(reply)
            .map_err(|e| BrokerError::Protocol(format!("bad manifest reply: {e}")))?;
        let manifest = reply.manifest.unwrap_or_default();

        Ok(Self {
            client: Mutex::new(client),
            me,
            orchestrator,
            manifest,
            timeout,
        })
    }

    /// Number of proxiable tools advertised.
    pub fn tool_count(&self) -> usize {
        self.manifest.len()
    }
}

#[async_trait]
impl ToolExecutor for McpProxyExecutor {
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
        if !self
            .manifest
            .iter()
            .any(|s| s.function.name == call.function.name)
        {
            return Err(ToolError::NotFound(call.function.name.clone()));
        }
        let req = McpRequest::Call {
            tool: call.function.name.clone(),
            arguments: call.function.arguments.clone(),
        };
        let reply = {
            let mut client = self.client.lock().await;
            request_over(
                &mut client,
                &self.me,
                &self.orchestrator,
                InboxKind::McpRequest,
                serde_json::to_value(req).expect("McpRequest serializes"),
                self.timeout,
            )
            .await
        }
        .map_err(|e| ToolError::Execution(format!("mcp proxy: {e}")))?;

        let reply: McpReply = serde_json::from_value(reply)
            .map_err(|e| ToolError::Execution(format!("bad mcp reply: {e}")))?;
        if let Some(err) = reply.error {
            return Err(ToolError::Execution(err));
        }
        let r = reply
            .result
            .ok_or_else(|| ToolError::Execution("mcp reply missing result".to_string()))?;
        Ok(ToolResult {
            success: r.success,
            result: r.result,
            display_preference: None,
            images: Vec::new(),
        })
    }

    async fn execute_with_context(
        &self,
        call: &ToolCall,
        _ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        self.execute(call).await
    }

    fn list_tools(&self) -> Vec<ToolSchema> {
        self.manifest.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::BrokerCore;
    use crate::server::BrokerServer;
    use bamboo_agent_core::tools::FunctionSchema;
    use serde_json::json;
    use tokio::net::TcpListener;

    const TOKEN: &str = "t";

    /// A stand-in for a host-bound MCP server: one tool that echoes its args.
    struct StubMcp;

    #[async_trait]
    impl ToolExecutor for StubMcp {
        async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
            Ok(ToolResult {
                success: true,
                result: format!(
                    "ran {} args={}",
                    call.function.name, call.function.arguments
                ),
                display_preference: None,
                images: Vec::new(),
            })
        }
        async fn execute_with_context(
            &self,
            call: &ToolCall,
            _ctx: ToolExecutionContext<'_>,
        ) -> Result<ToolResult, ToolError> {
            self.execute(call).await
        }
        fn list_tools(&self) -> Vec<ToolSchema> {
            vec![ToolSchema {
                schema_type: "function".into(),
                function: FunctionSchema {
                    name: "nova_click".into(),
                    description: "click a mark".into(),
                    parameters: json!({ "type": "object" }),
                },
            }]
        }
    }

    #[tokio::test]
    async fn proxy_lists_and_forwards_calls_over_the_broker() {
        let dir = tempfile::tempdir().unwrap();
        let core = Arc::new(BrokerCore::new(dir.path()));
        let server = Arc::new(BrokerServer::new(core, TOKEN));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        let endpoint = format!("ws://{addr}");

        // Orchestrator runs the proxy service backed by the stub host-bound MCP.
        let ep = endpoint.clone();
        tokio::spawn(async move {
            let _ = serve_mcp_proxy(
                &ep,
                AgentRef {
                    session_id: "orchestrator".into(),
                    role: None,
                },
                TOKEN,
                Arc::new(StubMcp),
            )
            .await;
        });

        // Worker builds a proxy: it fetches the manifest and advertises the tool.
        let proxy = McpProxyExecutor::connect(
            &endpoint,
            "worker#mcp",
            TOKEN,
            "orchestrator",
            Duration::from_secs(5),
        )
        .await
        .expect("proxy connects + fetches manifest");
        let tools = proxy.list_tools();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].function.name, "nova_click");

        // A call is forwarded to the orchestrator and the result comes back.
        let call = ToolCall {
            id: "c1".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "nova_click".into(),
                arguments: "{\"mark\":7}".into(),
            },
        };
        let result = proxy.execute(&call).await.expect("proxied call returns");
        assert!(result.success);
        assert_eq!(result.result, "ran nova_click args={\"mark\":7}");

        // Unknown tools are not handled by the proxy.
        let miss = ToolCall {
            id: "c2".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "not_proxied".into(),
                arguments: "{}".into(),
            },
        };
        assert!(matches!(
            proxy.execute(&miss).await,
            Err(ToolError::NotFound(_))
        ));
    }
}
