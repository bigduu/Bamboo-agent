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

use std::future::Future;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use bamboo_agent_core::tools::{
    FunctionCall, ToolCall, ToolError, ToolExecutionContext, ToolExecutor, ToolResult, ToolSchema,
};
use bamboo_subagent::{AgentRef, InboxKind, InboxMessage, MsgId};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::ask::request_over;
use crate::client::BrokerClient;
use crate::error::{BrokerError, BrokerResult};

// --- supervised-reconnect tuning (issue #47) ----------------------------------

/// Initial backoff for the orchestrator-side MCP proxy supervisor. The proxy
/// connection is long-lived, so this only gates *restarts* after a drop.
const PROXY_RECONNECT_INITIAL_BACKOFF: Duration = Duration::from_millis(500);
/// Cap for the orchestrator proxy reconnect backoff.
const PROXY_RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(30);
/// A serve run that lasts at least this long counts as "healthy": the next
/// restart resets the backoff to the floor instead of continuing to grow it,
/// so a single blip doesn't leave a large lingering backoff.
const PROXY_BACKOFF_RESET_AFTER: Duration = Duration::from_secs(10);

/// Initial backoff for the worker-side lazy reconnect.
const WORKER_RECONNECT_INITIAL_BACKOFF: Duration = Duration::from_millis(200);
/// Cap for the worker-side lazy reconnect backoff.
const WORKER_RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(2);
/// Maximum reconnect attempts per call before surfacing a transient error. A
/// later call can try again; the executor is never permanently disabled.
const WORKER_RECONNECT_MAX_ATTEMPTS: u32 = 5;

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

/// Supervised orchestrator-side MCP proxy (issue #47): run [`serve_mcp_proxy`]
/// in a loop, restarting it with bounded exponential backoff whenever the broker
/// connection drops, and stop cleanly when `shutdown` is cancelled.
///
/// A healthy, long-lived connection behaves identically to the bare
/// [`serve_mcp_proxy`] — this only adds resilience to transient WebSocket drops.
///
/// - **Backoff**: starts at [`PROXY_RECONNECT_INITIAL_BACKOFF`], doubles on each
///   quick restart, and is capped at [`PROXY_RECONNECT_MAX_BACKOFF`]. Reset to
///   the floor after a run that lasted ≥ [`PROXY_BACKOFF_RESET_AFTER`] (a healthy
///   connection), so a brief blip doesn't leave a large backoff lingering.
/// - **Shutdown**: the in-flight serve is raced against `shutdown.cancelled()` so
///   an intended stop interrupts it promptly; the backoff sleep between restarts
///   is also raced against shutdown. The loop never restarts once shutdown is
///   requested, so there is no leaked task / infinite restart after a stop.
pub async fn serve_mcp_proxy_supervised(
    endpoint: &str,
    me: AgentRef,
    token: &str,
    backend: Arc<dyn ToolExecutor>,
    shutdown: CancellationToken,
) {
    supervise_reconnect(
        || serve_mcp_proxy(endpoint, me.clone(), token, backend.clone()),
        shutdown,
        PROXY_RECONNECT_INITIAL_BACKOFF,
        PROXY_RECONNECT_MAX_BACKOFF,
        PROXY_BACKOFF_RESET_AFTER,
    )
    .await
}

/// Generic reconnect supervisor: call `serve_once` repeatedly, restarting on
/// return/error with bounded exponential backoff, until `shutdown` cancels.
/// Factored out so the backoff/restart/shutdown behavior is unit-testable with a
/// stub `serve_once` and tiny backoff constants (no real broker needed).
async fn supervise_reconnect<F, Fut>(
    mut serve_once: F,
    shutdown: CancellationToken,
    initial_backoff: Duration,
    max_backoff: Duration,
    reset_after: Duration,
) where
    F: FnMut() -> Fut + Send,
    Fut: Future<Output = BrokerResult<()>> + Send,
{
    let mut backoff = initial_backoff;
    loop {
        // Race the in-flight serve against an intended shutdown so an otherwise
        // indefinitely-blocked healthy connection still stops promptly.
        let started = std::time::Instant::now();
        let outcome = tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                tracing::info!("MCP proxy supervisor: shutdown requested, stopping");
                return;
            }
            r = serve_once() => r,
        };

        // A run that outlasted `reset_after` was healthy → reset backoff.
        if started.elapsed() >= reset_after {
            backoff = initial_backoff;
        }

        match outcome {
            Ok(()) => tracing::warn!(
                "MCP proxy connection ended; restarting (backoff {:?})",
                backoff
            ),
            Err(e) => tracing::warn!(
                "MCP proxy service errored: {e}; restarting (backoff {:?})",
                backoff
            ),
        }

        // Backoff sleep, abortable by shutdown so we don't linger after a stop.
        let slept = tokio::select! {
            biased;
            _ = shutdown.cancelled() => false,
            _ = tokio::time::sleep(backoff) => true,
        };
        if !slept {
            tracing::info!("MCP proxy supervisor: shutdown during backoff, stopping");
            return;
        }
        backoff = std::cmp::min(backoff * 2, max_backoff);
    }
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
    /// Serializes reconnect attempts so concurrent callers don't each rebuild
    /// the client. Held only across the (bounded) reconnect — the `client`
    /// mutex above is never held across a backoff sleep, so a reconnect can't
    /// deadlock or stall an unrelated caller's lock acquisition.
    reconnect_lock: Mutex<()>,
    me: AgentRef,
    endpoint: String,
    token: String,
    orchestrator: String,
    /// Proxiable tool surface, refreshed on each (re)connect. Behind a sync
    /// `RwLock` because `list_tools` is a sync trait method; reads/writes are
    /// instantaneous (clone/swap a `Vec`), never held across an `.await`.
    manifest: RwLock<Vec<ToolSchema>>,
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
            reconnect_lock: Mutex::new(()),
            me,
            endpoint: endpoint.to_string(),
            token: token.to_string(),
            orchestrator,
            manifest: RwLock::new(manifest),
            timeout,
        })
    }

    /// Number of proxiable tools advertised.
    pub fn tool_count(&self) -> usize {
        self.manifest.read().map(|m| m.len()).unwrap_or(0)
    }

    /// One request/reply over the current client (under its lock). Does NOT
    /// reconnect — callers decide that from the error + connection state.
    async fn request_once(&self, body: serde_json::Value) -> BrokerResult<serde_json::Value> {
        let mut client = self.client.lock().await;
        request_over(
            &mut client,
            &self.me,
            &self.orchestrator,
            InboxKind::McpRequest,
            body,
            self.timeout,
        )
        .await
    }

    /// Forward one MCP call, lazily reconnecting a single time if the broker
    /// connection is *actually* broken (reader exited). A transient timeout or
    /// an unrelated error is returned as-is — only a dead connection triggers
    /// the reconnect+retry. On reconnect exhaustion a transient error is
    /// returned (a later call may succeed), never a permanent one.
    async fn request_with_reconnect(
        &self,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, ToolError> {
        match self.request_once(body.clone()).await {
            Ok(v) => Ok(v),
            Err(first) => {
                // Only worth a reconnect if the connection itself is gone; a
                // plain timeout (reader still alive) surfaces directly.
                if !self.connection_broken().await {
                    return Err(ToolError::Execution(format!("mcp proxy: {first}")));
                }
                tracing::warn!("mcp proxy connection dropped; reconnecting: {first}");
                self.reconnect_if_needed()
                    .await
                    .map_err(|re| ToolError::Execution(format!("mcp proxy (reconnect): {re}")))?;
                // Retry exactly once over the freshly established client.
                self.request_once(body)
                    .await
                    .map_err(|re| ToolError::Execution(format!("mcp proxy: {re}")))
            }
        }
    }

    /// `true` when the broker connection is known to be dead (the background
    /// reader has exited). Used to decide whether a failed request is worth a
    /// reconnect+retry rather than a plain error.
    async fn connection_broken(&self) -> bool {
        let client = self.client.lock().await;
        !client.reader_alive()
    }

    /// Lazily re-establish the broker sub-connection: re-Hello/Subscribe and
    /// re-fetch the manifest, then swap in the fresh client. Serialized by
    /// [`reconnect_lock`](Self.reconnect_lock); a concurrent caller that already
    /// reconnected is a no-op. Bounded exponential backoff so a dead broker
    /// can't hot-loop. Returns a *transient* error (not a permanent one) on
    /// exhaustion, so a later call can try again — the executor is never
    /// permanently disabled by a transient drop (issue #47).
    async fn reconnect_if_needed(&self) -> BrokerResult<()> {
        let _guard = self.reconnect_lock.lock().await;
        // While we waited for the guard, another caller may already have
        // rebuilt the client; if so, there is nothing to do.
        if !self.connection_broken().await {
            return Ok(());
        }
        let mut backoff = WORKER_RECONNECT_INITIAL_BACKOFF;
        for _ in 0..WORKER_RECONNECT_MAX_ATTEMPTS {
            match self.reconnect_once().await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    tracing::warn!("mcp proxy reconnect failed (backoff {:?}): {e}", backoff);
                }
            }
            // Backoff WITHOUT holding the client mutex — only the reconnect
            // serialization guard, which is the intended behavior (concurrent
            // callers await the same single reconnect instead of racing).
            tokio::time::sleep(backoff).await;
            backoff = std::cmp::min(backoff * 2, WORKER_RECONNECT_MAX_BACKOFF);
        }
        Err(BrokerError::Transport(
            "mcp proxy reconnect attempts exhausted".into(),
        ))
    }

    /// One reconnect attempt: connect, subscribe, re-fetch the manifest, and
    /// swap the new client + manifest into place. No backoff here — the caller
    /// (`reconnect_if_needed`) owns the bounded retry/backoff loop.
    async fn reconnect_once(&self) -> BrokerResult<()> {
        let mut client =
            BrokerClient::connect(&self.endpoint, self.me.clone(), &self.token).await?;
        client.subscribe().await?;
        // Re-fetch the manifest so any tool-surface change during the outage is
        // reflected (the only state the proxy keeps beyond the live connection).
        let reply = request_over(
            &mut client,
            &self.me,
            &self.orchestrator,
            InboxKind::McpRequest,
            serde_json::to_value(McpRequest::Manifest).expect("McpRequest serializes"),
            self.timeout,
        )
        .await?;
        let reply: McpReply = serde_json::from_value(reply)
            .map_err(|e| BrokerError::Protocol(format!("bad manifest reply: {e}")))?;
        let manifest = reply.manifest.unwrap_or_default();
        {
            let mut slot = self.client.lock().await;
            *slot = client;
        }
        if let Ok(mut m) = self.manifest.write() {
            *m = manifest;
        }
        Ok(())
    }
}

#[async_trait]
impl ToolExecutor for McpProxyExecutor {
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
        {
            let manifest = self
                .manifest
                .read()
                .map_err(|_| ToolError::Execution("mcp proxy manifest lock poisoned".into()))?;
            if !manifest
                .iter()
                .any(|s| s.function.name == call.function.name)
            {
                return Err(ToolError::NotFound(call.function.name.clone()));
            }
        }
        let body = serde_json::to_value(McpRequest::Call {
            tool: call.function.name.clone(),
            arguments: call.function.arguments.clone(),
        })
        .expect("McpRequest serializes");

        // Forward the call, lazily reconnecting once if the broker connection
        // is actually broken (reader exited). A healthy connection takes the
        // fast path below — identical bytes to the pre-reconnect behavior.
        let reply = self.request_with_reconnect(body).await?;

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
        self.manifest.read().map(|m| m.clone()).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::BrokerCore;
    use crate::server::BrokerServer;
    use bamboo_agent_core::tools::FunctionSchema;
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
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

    // --- issue #47: supervised reconnect on both sides ------------------------

    /// Orchestrator supervisor: it restarts `serve_once` after each return with
    /// bounded backoff, and stops cleanly on shutdown. Driven with a stub
    /// `serve_once` (no real broker) and tiny backoff constants so it is fast
    /// and deterministic.
    #[tokio::test]
    async fn supervisor_restarts_on_drop_and_stops_on_shutdown() {
        let shutdown = CancellationToken::new();
        let calls = Arc::new(AtomicU32::new(0));
        let calls_for_serve = calls.clone();
        // serve_once: succeed quickly 3 times (quick restarts), then block
        // forever — a "healthy", long-lived connection that only ends on cancel.
        let serve = move || {
            let c = calls_for_serve.clone();
            async move {
                let n = c.fetch_add(1, Ordering::SeqCst);
                if n < 3 {
                    Ok(())
                } else {
                    std::future::pending::<BrokerResult<()>>().await
                }
            }
        };

        let started = std::time::Instant::now();
        let task = tokio::spawn(supervise_reconnect(
            serve,
            shutdown.clone(),
            Duration::from_millis(2),
            Duration::from_millis(8),
            Duration::from_secs(60), // runs are instant → never "healthy", backoff grows
        ));

        // 4 serve calls = 3 quick restarts + 1 blocking run, all within the
        // bounded backoff window (not e.g. 30s — proves the backoff is bounded).
        tokio::time::timeout(Duration::from_secs(3), async {
            while calls.load(Ordering::SeqCst) < 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("supervisor restarted within bounded backoff");
        assert!(calls.load(Ordering::SeqCst) >= 4);
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "restarts were bounded-fast, took {:?}",
            started.elapsed()
        );

        // Shutdown interrupts the blocking serve (+ any backoff) and ends the loop.
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .expect("supervisor stops promptly on shutdown")
            .expect("supervisor task did not panic");
    }

    /// Build the correlated `McpReply` for one worker `McpRequest` (used by the
    /// stand-in broker below).
    fn answer_mcp_request(req_msg: InboxMessage, orch: &AgentRef) -> InboxMessage {
        let reply_body = match serde_json::from_value::<McpRequest>(req_msg.body) {
            Ok(McpRequest::Manifest) => McpReply {
                manifest: Some(vec![ToolSchema {
                    schema_type: "function".into(),
                    function: FunctionSchema {
                        name: "nova_click".into(),
                        description: "click a mark".into(),
                        parameters: json!({ "type": "object" }),
                    },
                }]),
                ..Default::default()
            },
            Ok(McpRequest::Call { tool, arguments }) => McpReply {
                result: Some(ProxiedResult {
                    success: true,
                    result: format!("ran {tool} args={arguments}"),
                }),
                ..Default::default()
            },
            Err(_) => McpReply {
                error: Some("bad mcp request".into()),
                ..Default::default()
            },
        };
        InboxMessage {
            id: MsgId::new(),
            from: orch.clone(),
            kind: InboxKind::McpReply,
            body: serde_json::to_value(reply_body).unwrap_or_default(),
            created_at: Utc::now(),
            correlation_id: Some(req_msg.id),
        }
    }

    /// A stand-in broker that ALSO answers `McpRequest`s as the orchestrator
    /// would (the worker can't tell the difference — it just needs correlated
    /// replies). The FIRST accepted connection serves the connect `Manifest` +
    /// one `Call`, then closes the socket — simulating a transient WebSocket
    /// drop. Subsequent connections (reconnects) stay open and keep answering.
    /// Returns (endpoint, connections-accepted-counter).
    async fn flaky_mcp_broker() -> (String, Arc<AtomicU32>) {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::accept_async;
        use tokio_tungstenite::tungstenite::Message;

        use crate::proto::{BrokerFrame, ClientFrame};

        let orch = AgentRef {
            session_id: "orchestrator".into(),
            role: None,
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let first_taken = Arc::new(AtomicBool::new(false));
        let conns = Arc::new(AtomicU32::new(0));
        let conns_for_loop = conns.clone();
        tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(s) => s,
                    Err(_) => break,
                };
                let is_first = !first_taken.swap(true, Ordering::SeqCst);
                conns_for_loop.fetch_add(1, Ordering::SeqCst);
                let orch = orch.clone();
                tokio::spawn(async move {
                    let ws = match accept_async(stream).await {
                        Ok(ws) => ws,
                        Err(_) => return,
                    };
                    let (mut sink, mut source) = ws.split();

                    // 1. Hello → Welcome.
                    while let Some(Ok(msg)) = source.next().await {
                        if let Message::Text(t) = msg {
                            if ClientFrame::from_text(&t).is_ok() {
                                let _ = sink
                                    .send(Message::Text(BrokerFrame::Welcome.to_text()))
                                    .await;
                                break;
                            }
                        }
                    }

                    // 2. Serve Deliver frames. The first connection closes right
                    //    after it has answered the connect Manifest + one Call
                    //    (the simulated blip). Reconnects stay open.
                    let mut delivered = 0u32;
                    while let Some(Ok(msg)) = source.next().await {
                        let message = match msg {
                            Message::Text(t) => match ClientFrame::from_text(&t) {
                                Ok(ClientFrame::Deliver { message, .. }) => message,
                                _ => continue,
                            },
                            _ => continue,
                        };
                        let _ = sink
                            .send(Message::Text(
                                BrokerFrame::Delivered {
                                    id: message.id.clone(),
                                }
                                .to_text(),
                            ))
                            .await;
                        let reply = answer_mcp_request(message, &orch);
                        let _ = sink
                            .send(Message::Text(
                                BrokerFrame::Message { message: reply }.to_text(),
                            ))
                            .await;
                        if is_first {
                            delivered += 1;
                            if delivered == 2 {
                                // manifest + call served → drop the connection
                                let _ = sink.send(Message::Close(None)).await;
                                break;
                            }
                        }
                    }
                });
            }
        });
        (format!("ws://{addr}"), conns)
    }

    /// Worker reconnect (issue #47): after the broker connection drops mid-run,
    /// a later call transparently reconnects and succeeds instead of surfacing a
    /// permanent transport error.
    #[tokio::test]
    async fn proxy_executor_reconnects_after_transient_drop() {
        let (endpoint, conns) = flaky_mcp_broker().await;

        let proxy = McpProxyExecutor::connect(
            &endpoint,
            "worker#mcp",
            TOKEN,
            "orchestrator",
            Duration::from_secs(5),
        )
        .await
        .expect("proxy connects + fetches manifest");
        assert_eq!(proxy.list_tools().len(), 1);

        let call = |n: usize| ToolCall {
            id: format!("c{n}"),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "nova_click".into(),
                arguments: "{}".into(),
            },
        };

        // conn1: the first call succeeds before the (simulated) drop.
        let r1 = proxy.execute(&call(1)).await.expect("call1 on conn1");
        assert!(r1.success);

        // conn1 is now closed. This call must lazily reconnect (conn2) and retry
        // — NOT return a permanent "connection closed before reply" error.
        let r2 = tokio::time::timeout(Duration::from_secs(15), proxy.execute(&call(2)))
            .await
            .expect("call2 did not hang")
            .expect("call2 succeeds after reconnect (not a permanent error)");
        assert!(r2.success);
        assert_eq!(r2.result, "ran nova_click args={}");

        // The reconnect opened a second broker connection.
        assert!(
            conns.load(Ordering::SeqCst) >= 2,
            "worker reconnected (>=2 connections accepted), got {}",
            conns.load(Ordering::SeqCst)
        );

        // A third call on the (now healthy) reconnected connection succeeds too,
        // proving the executor is not permanently disabled by the drop.
        let r3 = proxy.execute(&call(3)).await.expect("call3 on conn2");
        assert!(r3.success);
    }
}
