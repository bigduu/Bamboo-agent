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

use std::collections::{HashMap, HashSet};
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

use crate::client::BrokerClient;
use crate::error::{BrokerError, BrokerResult};
use crate::mux::MultiplexedClient;

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

// ---- role allowlist (issue #54) ----------------------------------------------

/// Per-role allowlist that scopes which host-bound MCP tools a worker may see
/// and call through the proxy (principle of least privilege).
///
/// The orchestrator's MCP host exposes powerful, host-bound tools (screen
/// capture, local credentials, …). Without scoping, *every* worker — regardless
/// of role — gets the orchestrator's entire MCP tool set in both its advertised
/// manifest and as a callable surface, so a hallucinating worker could invoke a
/// tool its role has no business touching. This policy lets the deployer restrict
/// each role to an explicit set of tools.
///
/// Resolution for a requesting worker's role (`AgentRef.role`):
/// - **Role with an explicit allowlist** → only the intersection of that
///   allowlist with the backend's tools is exposed; a `Call` for a tool not on
///   the allowlist is rejected (defense in depth — the manifest already hides it,
///   but a worker could still try to call it directly).
/// - **Role with no entry, or a request with no role** → all tools are exposed
///   (backward compatible). An empty/default allowlist therefore restricts
///   nothing, preserving the behavior of existing unrestricted workers.
///
/// The default is **allow-all for unlisted roles** (not deny-by-default) so that
/// dropping this feature in does not silently strip tools from already-deployed
/// workers; the issue (#54) asks for restricted roles to be filtered while
/// default/unrestricted roles keep all tools. Restrictions are therefore opt-in
/// and explicit per role.
#[derive(Debug, Clone, Default)]
pub struct RoleToolAllowlist {
    /// role → set of tool names that role is allowed to proxy. A role absent from
    /// this map is unrestricted (sees/can call all backend tools).
    by_role: HashMap<String, HashSet<String>>,
}

impl RoleToolAllowlist {
    /// An empty allowlist: every role is unrestricted (back-compat default).
    pub fn unrestricted() -> Self {
        Self::default()
    }

    /// Build from `(role, allowed_tool_names)` entries. A role mapped to an empty
    /// set is still "restricted" — it gets *no* tools (an explicit lockout),
    /// distinct from a role that is simply absent (unrestricted).
    pub fn from_entries<R, T, I>(entries: I) -> Self
    where
        R: Into<String>,
        T: Into<String>,
        I: IntoIterator<Item = (R, Vec<T>)>,
    {
        let by_role = entries
            .into_iter()
            .map(|(role, tools)| (role.into(), tools.into_iter().map(Into::into).collect()))
            .collect();
        Self { by_role }
    }

    /// Add/replace one role's allowlist (builder-style).
    pub fn with_role(
        mut self,
        role: impl Into<String>,
        tools: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.by_role
            .insert(role.into(), tools.into_iter().map(Into::into).collect());
        self
    }

    /// Whether `role` is restricted (has an explicit allowlist entry). A `None`
    /// role, or a role with no entry, is unrestricted.
    fn is_restricted(&self, role: Option<&str>) -> bool {
        role.is_some_and(|r| self.by_role.contains_key(r))
    }

    /// Whether `role` is allowed to use `tool`. Unrestricted roles allow any tool;
    /// a restricted role allows only the tools on its set.
    fn allows(&self, role: Option<&str>, tool: &str) -> bool {
        match role.and_then(|r| self.by_role.get(r)) {
            Some(allowed) => allowed.contains(tool),
            None => true, // unrestricted (no entry / no role)
        }
    }

    /// Filter a full tool manifest down to what `role` may see. Unrestricted
    /// roles get the manifest unchanged; restricted roles get the intersection.
    fn filter_manifest(&self, role: Option<&str>, mut tools: Vec<ToolSchema>) -> Vec<ToolSchema> {
        if let Some(allowed) = role.and_then(|r| self.by_role.get(r)) {
            tools.retain(|t| allowed.contains(&t.function.name));
        }
        tools
    }
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
    allowlist: Arc<RoleToolAllowlist>,
) -> BrokerResult<()> {
    let mut client = BrokerClient::connect(endpoint, me.clone(), token).await?;
    client.subscribe().await?;

    // Run each request's (potentially slow) backend call CONCURRENTLY in a spawned
    // task, and route the finished reply back to this loop — the single client
    // owner — which delivers + acks it. So N parallel McpRequests overlap their
    // backend work instead of the old serial `handle(msg).await` per message. The
    // worker side multiplexes replies by correlation_id, so out-of-order completion
    // is fine. (Spawns are unbounded but bounded in practice by the LLM's parallel
    // tool-call batch; a Semaphore cap is a future option if a backend needs one.)
    // #144.
    //
    // KEEP-ALIVE: this original `reply_tx` is intentionally retained in scope for
    // the whole loop (each spawn clones it). It guarantees `reply_rx.recv()` never
    // returns `None` while looping, which is what lets the reply arm's
    // `Some(..) = reply_rx.recv()` always eventually match — do NOT drop it (e.g.
    // by only cloning into the spawn) or the reply arm goes permanently dead.
    let (reply_tx, mut reply_rx) =
        tokio::sync::mpsc::unbounded_channel::<(MsgId, String, McpReply)>();
    loop {
        tokio::select! {
            // Deliver a completed reply + ack its request (cheap; serialized
            // through the owner, never blocks a backend call).
            Some((corr, reply_to, reply_body)) = reply_rx.recv() => {
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
            msg = client.next_message() => {
                let Some(msg) = msg else { break }; // connection closed
                if msg.kind != InboxKind::McpRequest {
                    let _ = client.ack(msg.id).await;
                    continue;
                }
                let backend = Arc::clone(&backend);
                let allowlist = Arc::clone(&allowlist);
                let reply_tx = reply_tx.clone();
                let corr = msg.id.clone();
                let reply_to = msg.from.session_id.clone();
                tokio::spawn(async move {
                    let reply_body = handle_mcp_request(backend.as_ref(), &allowlist, msg).await;
                    // Receiver gone == loop exited (connection dropped) -> drop.
                    let _ = reply_tx.send((corr, reply_to, reply_body));
                });
            }
        }
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
    allowlist: Arc<RoleToolAllowlist>,
    shutdown: CancellationToken,
) {
    supervise_reconnect(
        || {
            serve_mcp_proxy(
                endpoint,
                me.clone(),
                token,
                backend.clone(),
                allowlist.clone(),
            )
        },
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

async fn handle_mcp_request(
    backend: &dyn ToolExecutor,
    allowlist: &RoleToolAllowlist,
    msg: InboxMessage,
) -> McpReply {
    // The requesting worker's role scopes which host-bound tools it may proxy
    // (issue #54). `None`/unlisted roles are unrestricted (back-compat).
    let role = msg.from.role.as_deref();
    match serde_json::from_value::<McpRequest>(msg.body) {
        Ok(McpRequest::Manifest) => {
            let tools = allowlist.filter_manifest(role, backend.list_tools());
            if allowlist.is_restricted(role) {
                tracing::debug!(
                    role = role.unwrap_or("<none>"),
                    tools = tools.len(),
                    "mcp proxy: serving role-scoped manifest"
                );
            }
            McpReply {
                manifest: Some(tools),
                ..Default::default()
            }
        }
        Ok(McpRequest::Call { tool, arguments }) => {
            // Defense in depth: the manifest already hides disallowed tools, but a
            // worker could still try to call one directly — reject it here too.
            if !allowlist.allows(role, &tool) {
                tracing::warn!(
                    role = role.unwrap_or("<none>"),
                    tool = %tool,
                    "mcp proxy: rejecting tool call not on role allowlist"
                );
                return McpReply {
                    error: Some(format!(
                        "tool '{tool}' is not allowed for role '{}'",
                        role.unwrap_or("<none>")
                    )),
                    ..Default::default()
                };
            }
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
    /// The multiplexed driver over the proxy's broker sub-connection. A
    /// `RwLock<Arc<…>>` so reconnect can SWAP the whole driver while in-flight
    /// requests keep running on their cloned `Arc` of the old one. A request
    /// clones the `Arc` and releases the lock BEFORE the round-trip, so parallel
    /// MCP calls overlap instead of serializing behind one exclusive lock. #56.
    client: tokio::sync::RwLock<Arc<MultiplexedClient>>,
    /// Serializes reconnect attempts so concurrent callers don't each rebuild
    /// the client. Held only across the (bounded) reconnect — the `client` lock
    /// above is never held across a backoff sleep, so a reconnect can't deadlock
    /// or stall an unrelated caller's lock acquisition.
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
        let mux = client.into_multiplexed(me.clone());

        let reply = mux
            .request(
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
            client: tokio::sync::RwLock::new(Arc::new(mux)),
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
        // Clone the Arc and RELEASE the lock before the round-trip, so concurrent
        // proxy calls overlap instead of serializing behind one exclusive lock.
        let mux = self.client.read().await.clone();
        mux.request(
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
        !self.client.read().await.reader_alive()
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
        let mux = client.into_multiplexed(self.me.clone());
        // Re-fetch the manifest so any tool-surface change during the outage is
        // reflected (the only state the proxy keeps beyond the live connection).
        let reply = mux
            .request(
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
            // Swap in the new driver. The old Arc lives until in-flight requests
            // on it finish; its router ends when the old (dead) connection's
            // reader closes `messages`, failing any stragglers. #56.
            let mut slot = self.client.write().await;
            *slot = Arc::new(mux);
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

    /// A multi-tool host-bound MCP stub: a privileged screen tool + a benign one.
    struct MultiToolMcp;

    #[async_trait]
    impl ToolExecutor for MultiToolMcp {
        async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
            Ok(ToolResult {
                success: true,
                result: format!("ran {}", call.function.name),
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
            ["nova_screenshot", "nova_click", "fetch_url"]
                .into_iter()
                .map(|name| ToolSchema {
                    schema_type: "function".into(),
                    function: FunctionSchema {
                        name: name.into(),
                        description: "t".into(),
                        parameters: json!({ "type": "object" }),
                    },
                })
                .collect()
        }
    }

    /// Build a worker `McpRequest` inbox message with a given role, as the broker
    /// would deliver it to the orchestrator's `handle_mcp_request`.
    fn worker_request(role: Option<&str>, req: McpRequest) -> InboxMessage {
        InboxMessage {
            id: MsgId::new(),
            from: AgentRef {
                session_id: "worker#mcp".into(),
                role: role.map(Into::into),
            },
            kind: InboxKind::McpRequest,
            body: serde_json::to_value(req).unwrap(),
            created_at: Utc::now(),
            correlation_id: None,
        }
    }

    fn manifest_names(reply: &McpReply) -> Vec<String> {
        reply
            .manifest
            .as_ref()
            .expect("manifest reply")
            .iter()
            .map(|t| t.function.name.clone())
            .collect()
    }

    /// Issue #54: a role WITH an allowlist sees only its allowed tools in the
    /// manifest; an unrestricted role (no entry / no role) sees ALL tools.
    #[tokio::test]
    async fn manifest_is_filtered_by_role_allowlist() {
        let backend = MultiToolMcp;
        // "researcher" may only proxy the benign fetch tool — not the screen tools.
        let allowlist = RoleToolAllowlist::unrestricted().with_role("researcher", ["fetch_url"]);

        // Restricted role → intersection only.
        let reply = handle_mcp_request(
            &backend,
            &allowlist,
            worker_request(Some("researcher"), McpRequest::Manifest),
        )
        .await;
        assert_eq!(manifest_names(&reply), vec!["fetch_url".to_string()]);

        // A role with no allowlist entry is unrestricted → all tools.
        let reply = handle_mcp_request(
            &backend,
            &allowlist,
            worker_request(Some("operator"), McpRequest::Manifest),
        )
        .await;
        assert_eq!(manifest_names(&reply).len(), 3);

        // No role at all is unrestricted too (back-compat) → all tools.
        let reply = handle_mcp_request(
            &backend,
            &allowlist,
            worker_request(None, McpRequest::Manifest),
        )
        .await;
        assert_eq!(manifest_names(&reply).len(), 3);
    }

    /// Issue #54: defense in depth — a restricted role's `Call` for a tool not on
    /// its allowlist is REJECTED (the manifest hides it, but a worker could still
    /// try to call it directly). An allowed tool still executes.
    #[tokio::test]
    async fn call_is_rejected_when_tool_not_on_role_allowlist() {
        let backend = MultiToolMcp;
        let allowlist = RoleToolAllowlist::unrestricted().with_role("researcher", ["fetch_url"]);

        // Disallowed tool → error, backend NOT invoked.
        let reply = handle_mcp_request(
            &backend,
            &allowlist,
            worker_request(
                Some("researcher"),
                McpRequest::Call {
                    tool: "nova_screenshot".into(),
                    arguments: "{}".into(),
                },
            ),
        )
        .await;
        assert!(reply.result.is_none());
        let err = reply.error.expect("a rejection error");
        assert!(
            err.contains("nova_screenshot") && err.contains("not allowed"),
            "{err}"
        );

        // Allowed tool → executes normally.
        let reply = handle_mcp_request(
            &backend,
            &allowlist,
            worker_request(
                Some("researcher"),
                McpRequest::Call {
                    tool: "fetch_url".into(),
                    arguments: "{}".into(),
                },
            ),
        )
        .await;
        assert!(reply.error.is_none());
        assert_eq!(reply.result.expect("result").result, "ran fetch_url");

        // Unrestricted role may call any tool.
        let reply = handle_mcp_request(
            &backend,
            &allowlist,
            worker_request(
                None,
                McpRequest::Call {
                    tool: "nova_screenshot".into(),
                    arguments: "{}".into(),
                },
            ),
        )
        .await;
        assert!(reply.error.is_none());
        assert_eq!(reply.result.expect("result").result, "ran nova_screenshot");
    }

    /// A role mapped to an EMPTY allowlist is an explicit lockout (no tools),
    /// distinct from an absent role (unrestricted).
    #[tokio::test]
    async fn empty_allowlist_entry_is_explicit_lockout() {
        let backend = MultiToolMcp;
        let allowlist = RoleToolAllowlist::from_entries(vec![("sandbox", Vec::<String>::new())]);
        let reply = handle_mcp_request(
            &backend,
            &allowlist,
            worker_request(Some("sandbox"), McpRequest::Manifest),
        )
        .await;
        assert!(manifest_names(&reply).is_empty());
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
                Arc::new(RoleToolAllowlist::unrestricted()),
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

    #[tokio::test]
    async fn proxy_handles_concurrent_calls_correctly() {
        let dir = tempfile::tempdir().unwrap();
        let core = Arc::new(BrokerCore::new(dir.path()));
        let server = Arc::new(BrokerServer::new(core, TOKEN));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        let endpoint = format!("ws://{addr}");

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
                Arc::new(RoleToolAllowlist::unrestricted()),
            )
            .await;
        });

        let proxy = Arc::new(
            McpProxyExecutor::connect(
                &endpoint,
                "worker#mcp",
                TOKEN,
                "orchestrator",
                Duration::from_secs(5),
            )
            .await
            .expect("proxy connects"),
        );

        // Fire N concurrent proxied calls with DISTINCT args over the multiplexed
        // connection (no per-call exclusive lock). Each must get its OWN result —
        // proving concurrent execute() doesn't serialize-deadlock or mis-route
        // replies. (End-to-end latency is still capped by the serial orchestrator
        // serve_mcp_proxy — that's the complementary half, tracked separately.) #56.
        let mut handles = Vec::new();
        for i in 0..8u32 {
            let p = proxy.clone();
            handles.push(tokio::spawn(async move {
                let call = ToolCall {
                    id: format!("c{i}"),
                    tool_type: "function".into(),
                    function: FunctionCall {
                        name: "nova_click".into(),
                        arguments: format!("{{\"mark\":{i}}}"),
                    },
                };
                let r = p.execute(&call).await.expect("proxied call returns");
                (i, r.result)
            }));
        }
        for h in handles {
            let (i, result) = h.await.unwrap();
            assert_eq!(result, format!("ran nova_click args={{\"mark\":{i}}}"));
        }
    }

    #[tokio::test]
    async fn concurrent_proxy_calls_overlap_at_the_orchestrator() {
        use std::time::Instant;

        // A host-bound backend where each call takes 200ms. Serial handling of N
        // calls would take N*200ms; concurrent handling overlaps to ~200ms.
        struct SlowMcp;
        #[async_trait]
        impl ToolExecutor for SlowMcp {
            async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
                tokio::time::sleep(Duration::from_millis(200)).await;
                Ok(ToolResult {
                    success: true,
                    result: format!("done {}", call.function.arguments),
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

        let dir = tempfile::tempdir().unwrap();
        let core = Arc::new(BrokerCore::new(dir.path()));
        let server = Arc::new(BrokerServer::new(core, TOKEN));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        let endpoint = format!("ws://{addr}");

        let ep = endpoint.clone();
        tokio::spawn(async move {
            let _ = serve_mcp_proxy(
                &ep,
                AgentRef {
                    session_id: "orchestrator".into(),
                    role: None,
                },
                TOKEN,
                Arc::new(SlowMcp),
                Arc::new(RoleToolAllowlist::unrestricted()),
            )
            .await;
        });

        let proxy = Arc::new(
            McpProxyExecutor::connect(
                &endpoint,
                "worker#mcp",
                TOKEN,
                "orchestrator",
                Duration::from_secs(5),
            )
            .await
            .expect("proxy connects"),
        );

        // 4 concurrent 200ms calls: serial would be ~800ms, concurrent ~200ms.
        let start = Instant::now();
        let mut handles = Vec::new();
        for i in 0..4u32 {
            let p = proxy.clone();
            handles.push(tokio::spawn(async move {
                let call = ToolCall {
                    id: format!("c{i}"),
                    tool_type: "function".into(),
                    function: FunctionCall {
                        name: "nova_click".into(),
                        arguments: format!("{{\"i\":{i}}}"),
                    },
                };
                p.execute(&call).await.expect("returns")
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(500),
            "4 concurrent 200ms proxy calls must OVERLAP at the orchestrator \
             (serial would be ~800ms); took {elapsed:?}"
        );
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
