use tokio::time::{interval, Duration};

use super::fingerprint::desired_proxy_fingerprint;
use super::*;
use crate::protocol::models::JsonRpcNotification;

/// MCP methods a server sends when its tool list changes. Per the MCP spec the
/// wire method is `notifications/tools/list_changed`; the bare `tools/list_changed`
/// is accepted defensively for servers that omit the `notifications/` prefix. #366.
const TOOLS_LIST_CHANGED_METHODS: [&str; 2] =
    ["notifications/tools/list_changed", "tools/list_changed"];

impl McpServerManager {
    /// Start a new MCP server connection.
    pub async fn start_server(&self, config: McpServerConfig) -> Result<()> {
        let _reconcile = self.reconcile_lock.lock().await;
        self.start_server_unlocked(config).await
    }

    pub(super) async fn start_server_unlocked(&self, config: McpServerConfig) -> Result<()> {
        let server_id = config.id.clone();

        if self.runtimes.contains_key(&server_id) {
            return Err(McpError::AlreadyRunning(server_id));
        }

        info!("Starting MCP server '{}'", server_id);

        let prepared = self.prepare_server_runtime(config, "start").await?;
        debug_assert_eq!(prepared.runtime.config.id, server_id);
        let replaced = self.install_prepared_runtime(prepared).await?;
        debug_assert!(replaced.is_none());

        Ok(())
    }

    pub(super) async fn prepare_server_runtime(
        &self,
        config: McpServerConfig,
        phase: &'static str,
    ) -> Result<PreparedServerRuntime> {
        let server_id = config.id.clone();
        let runtime_proxy_fingerprint = desired_proxy_fingerprint(self.config.as_ref()).await;
        let (mut client, tools, instructions, notification_rx) = self
            .bootstrap_server_client(&server_id, &config, phase)
            .await?;
        let catalog = match self.index.plan_server_tools(
            &server_id,
            &tools,
            &config.allowed_tools,
            &config.denied_tools,
        ) {
            Ok(catalog) => catalog,
            Err(error) => {
                let _ = client.disconnect().await;
                return Err(error.into());
            }
        };
        let runtime = Arc::new(ServerRuntime {
            config,
            client: RwLock::new(client),
            info: RwLock::new(RuntimeInfo {
                status: ServerStatus::Ready,
                last_error: None,
                connected_at: Some(Utc::now()),
                disconnected_at: None,
                tool_count: tools.len(),
                restart_count: 0,
                last_ping_at: Some(Utc::now()),
                instructions,
            }),
            tools: RwLock::new(tools.clone()),
            shutdown: AtomicBool::new(false),
            reconnecting: AtomicBool::new(false),
            qos: McpServerQos::new(McpQosConfig::default()),
            proxy_fingerprint: runtime_proxy_fingerprint,
        });
        Ok(PreparedServerRuntime {
            runtime,
            catalog,
            notification_rx,
        })
    }

    /// Publish a fully initialized runtime. No fallible initialization remains
    /// in this method, so callers can stage all candidates before committing.
    pub(super) async fn install_prepared_runtime(
        &self,
        prepared: PreparedServerRuntime,
    ) -> Result<Option<Arc<ServerRuntime>>> {
        let transaction = match self
            .index
            .preflight_catalog_update(std::slice::from_ref(&prepared.catalog), &[])
        {
            Ok(transaction) => transaction,
            Err(error) => {
                let server_id = prepared.runtime.config.id.clone();
                self.shutdown_detached_runtime(&server_id, prepared.runtime)
                    .await;
                return Err(error.into());
            }
        };
        let (server_id, tool_names, replaced) = self.publish_prepared_runtime(prepared);
        self.index.commit_catalog_update(transaction);

        info!(
            "Registered {} MCP tools for server '{}'",
            tool_names.len(),
            server_id
        );

        self.emit_runtime_ready_events(server_id, tool_names).await;
        Ok(replaced)
    }

    /// Publish a fully initialized runtime without suspending. Configuration
    /// reconciliation uses this after its durable CAS boundary so cancellation
    /// cannot leave only part of a committed runtime set visible.
    pub(super) fn publish_prepared_runtime(
        &self,
        prepared: PreparedServerRuntime,
    ) -> (String, Vec<String>, Option<Arc<ServerRuntime>>) {
        let PreparedServerRuntime {
            runtime,
            catalog,
            notification_rx,
        } = prepared;
        let config = &runtime.config;
        let server_id = config.id.clone();
        let healthcheck_interval_ms = config.healthcheck_interval_ms;

        let tool_names = catalog
            .aliases()
            .into_iter()
            .map(|alias| alias.alias)
            .collect();

        // Store runtime only after its client initialized and tools were read.
        let replaced = self.runtimes.insert(server_id.clone(), runtime.clone());

        // Spawn the notification drain only AFTER the runtime is registered, so an
        // immediate `tools/list_changed` resolves via `refresh_tools` instead of
        // racing `ServerNotFound`. (#420)
        if let Some(rx) = notification_rx {
            self.spawn_notification_drain(server_id.clone(), runtime.clone(), rx);
        }

        // The new generation's health task and the old generation's shutdown
        // flag are part of the synchronous publication boundary. Event delivery
        // and client disconnection may suspend and happen afterward.
        self.start_health_check(runtime, healthcheck_interval_ms);
        if let Some(ref old) = replaced {
            old.shutdown.store(true, Ordering::SeqCst);
        }

        (server_id, tool_names, replaced)
    }

    pub(super) async fn emit_runtime_ready_events(
        &self,
        server_id: String,
        tool_names: Vec<String>,
    ) {
        if let Some(ref tx) = self.event_tx {
            let _ = tx
                .send(McpEvent::ServerStatusChanged {
                    server_id: server_id.clone(),
                    status: ServerStatus::Ready,
                    error: None,
                })
                .await;

            let _ = tx
                .send(McpEvent::ToolsChanged {
                    server_id,
                    tools: tool_names,
                })
                .await;
        }
    }

    /// Stop an MCP server connection.
    pub async fn stop_server(&self, server_id: &str) -> Result<()> {
        let _reconcile = self.reconcile_lock.lock().await;
        self.stop_server_unlocked(server_id).await
    }

    pub(super) async fn stop_server_unlocked(&self, server_id: &str) -> Result<()> {
        info!("Stopping MCP server '{}'", server_id);
        // `stop_server` owns the reconciliation lock. Preflight the catalog
        // removal before detaching the runtime, then publish both changes with
        // no suspension/cancellation point. Another OS thread may briefly see
        // the old alias after runtime removal, but execution then fails closed
        // on the missing runtime; it can never resolve to a different owner.
        // The checked commit is infallible under the single-writer invariant
        // and fail-stops before swapping if violated.
        let transaction = self
            .index
            .preflight_catalog_update(&[], &[server_id.to_string()])?;
        let runtime = self.detach_runtime_without_index(server_id)?;
        self.index.commit_catalog_update(transaction);
        self.finish_detached_stop(server_id.to_string(), runtime, true)
            .await;
        info!("MCP server '{}' stopped", server_id);
        Ok(())
    }

    /// Detach only the runtime. Transactional configuration reconciliation uses
    /// this while a preflighted whole-index replacement is waiting to commit.
    pub(super) fn detach_runtime_without_index(
        &self,
        server_id: &str,
    ) -> Result<Arc<ServerRuntime>> {
        let (_, runtime) = self
            .runtimes
            .remove(server_id)
            .ok_or_else(|| McpError::NotRunning(server_id.to_string()))?;
        runtime.shutdown.store(true, Ordering::SeqCst);
        Ok(runtime)
    }

    pub(super) async fn finish_detached_stop(
        &self,
        server_id: String,
        runtime: Arc<ServerRuntime>,
        emit_stopped: bool,
    ) {
        self.shutdown_detached_runtime(&server_id, runtime).await;
        if emit_stopped {
            if let Some(ref tx) = self.event_tx {
                let _ = tx
                    .send(McpEvent::ServerStatusChanged {
                        server_id,
                        status: ServerStatus::Stopped,
                        error: None,
                    })
                    .await;
            }
        }
    }

    /// Stop a runtime that has already been detached/replaced. This deliberately
    /// does not touch the runtime map or tool index, which now belong to its
    /// successfully committed replacement.
    pub(super) async fn shutdown_detached_runtime(
        &self,
        server_id: &str,
        runtime: Arc<ServerRuntime>,
    ) {
        runtime.shutdown.store(true, Ordering::SeqCst);
        let mut client = runtime.client.write().await;
        if let Err(error) = client.disconnect().await {
            warn!(
                "Error disconnecting replaced MCP server '{}': {}",
                server_id, error
            );
        }
        let mut info = runtime.info.write().await;
        info.status = ServerStatus::Stopped;
        info.disconnected_at = Some(Utc::now());
    }

    /// Call a tool on a specific server.
    pub async fn call_tool(
        &self,
        server_id: &str,
        tool_name: &str,
        args: serde_json::Value,
    ) -> Result<crate::types::McpCallResult> {
        let runtime = self
            .runtimes
            .get(server_id)
            .ok_or_else(|| McpError::ServerNotFound(server_id.to_string()))?;

        runtime.qos.check_circuit(server_id, tool_name).await?;
        let _permit = runtime.qos.acquire_permit().await?;

        let client = runtime.client.read().await;
        let timeout = runtime.config.request_timeout_ms;
        let result = client.call_tool(tool_name, args, timeout).await;
        drop(client);

        let result = match result {
            // A tool that RAN but reported failure (`is_error`) is still `Ok` at
            // the protocol level — so without this the QoS/health path would count
            // it as a success and a server with a wedged capability (e.g. nova when
            // its capture pipeline is stuck: it answers pings and returns an error
            // RESULT well within request_timeout_ms, so it never trips a protocol
            // timeout) would fail forever without ever being recycled.
            Ok(result) if result.is_error => {
                let synthetic =
                    McpError::ToolExecution(format!("tool '{tool_name}' returned an error result"));
                let should_recycle = runtime
                    .qos
                    .record_failure(server_id, tool_name, &synthetic)
                    .await;
                self.maybe_recycle_server(runtime.value(), should_recycle);
                result
            }
            Ok(result) => {
                runtime.qos.record_success().await;
                result
            }
            Err(error) => {
                let should_recycle = runtime
                    .qos
                    .record_failure(server_id, tool_name, &error)
                    .await;
                self.maybe_recycle_server(runtime.value(), should_recycle);
                return Err(error);
            }
        };

        // Emit event
        if let Some(ref tx) = self.event_tx {
            let _ = tx
                .send(McpEvent::ToolExecuted {
                    server_id: server_id.to_string(),
                    tool_name: tool_name.to_string(),
                    success: !result.is_error,
                })
                .await;
        }

        Ok(result)
    }

    /// Recycle a server that has failed too many times in a row — disconnect (the
    /// existing child process is killed) and reconnect (a fresh one is spawned).
    /// Non-blocking: runs in its own task with its own backoff, guarded against
    /// concurrent runs by `ServerRuntime::reconnecting`. Skipped when recycling
    /// isn't warranted, reconnect is disabled for the server, or it's shutting down.
    fn maybe_recycle_server(&self, runtime: &Arc<ServerRuntime>, should_recycle: bool) {
        if !should_recycle
            || !runtime.config.reconnect.enabled
            || runtime.shutdown.load(Ordering::SeqCst)
        {
            return;
        }
        let manager = self.clone();
        let runtime = runtime.clone();
        let server_id = runtime.config.id.clone();
        warn!(
            "Recycling MCP server '{}' after repeated tool failures (disconnect + reconnect)",
            server_id
        );
        tokio::spawn(async move {
            if let Err(e) = manager.attempt_reconnection(runtime).await {
                warn!("MCP server '{}' recycle failed: {}", server_id, e);
            }
        });
    }

    /// Get tool info for a specific tool.
    pub fn get_tool_info(&self, server_id: &str, tool_name: &str) -> Option<McpTool> {
        self.runtimes.get(server_id).and_then(|runtime| {
            let tools = runtime.tools.try_read().ok()?;
            tools.iter().find(|t| t.name == tool_name).cloned()
        })
    }

    /// Refresh tools from a server.
    pub async fn refresh_tools(&self, server_id: &str) -> Result<()> {
        let runtime = self
            .runtimes
            .get(server_id)
            .map(|runtime| runtime.value().clone())
            .ok_or_else(|| McpError::ServerNotFound(server_id.to_string()))?;

        info!("Refreshing tools for MCP server '{}'", server_id);

        let client = runtime.client.read().await;
        let new_tools = client.list_tools(runtime.config.request_timeout_ms).await?;
        drop(client);

        if !self
            .publish_refreshed_tools_if_current(server_id, &runtime, new_tools)
            .await?
        {
            tracing::debug!(
                "Discarding tool refresh for detached MCP runtime '{}'",
                server_id
            );
        }
        Ok(())
    }

    pub(super) async fn publish_refreshed_tools_if_current(
        &self,
        server_id: &str,
        runtime: &Arc<ServerRuntime>,
        new_tools: Vec<McpTool>,
    ) -> Result<bool> {
        // Serialize the generation check with transactional replacement. The
        // list_tools await above may span a replacement, so checking only when
        // the refresh starts is insufficient.
        let _reconcile = self.reconcile_lock.lock().await;
        if runtime.shutdown.load(Ordering::SeqCst) || !self.is_current_runtime(server_id, runtime) {
            return Ok(false);
        }

        let catalog = self.index.plan_server_tools(
            server_id,
            &new_tools,
            &runtime.config.allowed_tools,
            &runtime.config.denied_tools,
        )?;
        let aliases = catalog.aliases();
        let transaction = self
            .index
            .preflight_catalog_update(std::slice::from_ref(&catalog), &[])?;

        // Acquire every fallible/suspending guard before the publication point.
        // From the index swap through the runtime metadata update there is no
        // await, so a failed preflight leaves the prior catalog untouched.
        let mut tools = runtime.tools.write().await;
        let mut info = runtime.info.write().await;
        self.index.commit_catalog_update(transaction);
        *tools = new_tools.clone();
        info.tool_count = new_tools.len();
        drop(info);
        drop(tools);

        info!(
            "Refreshed {} tools for MCP server '{}'",
            aliases.len(),
            server_id
        );

        // Emit event
        if let Some(ref tx) = self.event_tx {
            let tool_names: Vec<String> = aliases.into_iter().map(|a| a.alias).collect();
            let _ = tx
                .send(McpEvent::ToolsChanged {
                    server_id: server_id.to_string(),
                    tools: tool_names,
                })
                .await;
        }

        Ok(true)
    }

    fn start_health_check(&self, runtime: Arc<ServerRuntime>, interval_ms: u64) {
        let server_id = runtime.config.id.clone();
        let manager = Arc::new(self.clone());

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(interval_ms));

            loop {
                interval.tick().await;

                if runtime.shutdown.load(Ordering::SeqCst)
                    || !manager.is_current_runtime(&server_id, &runtime)
                {
                    break;
                }

                // Skip health check if currently reconnecting
                if runtime.reconnecting.load(Ordering::SeqCst) {
                    continue;
                }

                let ping_result = {
                    let client = runtime.client.read().await;
                    client.ping(runtime.config.request_timeout_ms).await
                };

                let Some(should_reconnect) = manager
                    .publish_health_result_if_current(
                        &server_id,
                        &runtime,
                        ping_result.map_err(|error| error.to_string()),
                    )
                    .await
                else {
                    break;
                };

                if should_reconnect {
                    if let Err(reconnect_err) = manager.attempt_reconnection(runtime.clone()).await
                    {
                        error!(
                            "Reconnection failed for MCP server '{}': {}",
                            server_id, reconnect_err
                        );
                    }
                }
            }
        });
    }

    pub(super) async fn publish_health_result_if_current(
        &self,
        server_id: &str,
        runtime: &Arc<ServerRuntime>,
        result: std::result::Result<(), String>,
    ) -> Option<bool> {
        // A ping may span a transactional replacement. Keep this generation
        // check and all status/event publication serialized with replacement.
        let _reconcile = self.reconcile_lock.lock().await;
        if runtime.shutdown.load(Ordering::SeqCst) || !self.is_current_runtime(server_id, runtime) {
            return None;
        }

        match result {
            Ok(()) => {
                let mut info = runtime.info.write().await;
                info.last_ping_at = Some(Utc::now());
                let recovered = info.status == ServerStatus::Degraded;
                if recovered {
                    info.status = ServerStatus::Ready;
                }
                drop(info);
                if recovered {
                    if let Some(ref tx) = self.event_tx {
                        let _ = tx
                            .send(McpEvent::ServerStatusChanged {
                                server_id: server_id.to_string(),
                                status: ServerStatus::Ready,
                                error: None,
                            })
                            .await;
                    }
                }
                Some(false)
            }
            Err(error) => {
                warn!(
                    "Health check failed for MCP server '{}': {}",
                    server_id, error
                );
                {
                    let mut info = runtime.info.write().await;
                    info.status = ServerStatus::Degraded;
                    info.last_error = Some(error.clone());
                }
                if let Some(ref tx) = self.event_tx {
                    let _ = tx
                        .send(McpEvent::ServerStatusChanged {
                            server_id: server_id.to_string(),
                            status: ServerStatus::Degraded,
                            error: Some(error),
                        })
                        .await;
                }
                Some(runtime.config.reconnect.enabled)
            }
        }
    }

    /// Spawns the per-connection task that DRAINS this client's server-notification
    /// queue and dispatches each notification. #366.
    ///
    /// The task owns the receiver (taken from the client) so it parks on
    /// `recv().await` with zero wakeups while idle — no client lock held across the
    /// await, no polling. It exits cleanly when every notification sender closes
    /// (the client is disconnected/replaced on reconnect, or dropped on shutdown),
    /// mirroring the message-handler's channel-close contract.
    ///
    /// Without this consumer the queue would silently fill to capacity and drop
    /// every later notification (the #363 non-blocking send is a safety valve, not
    /// a drain), so any capability driven by server notifications — here,
    /// `tools/list_changed` -> tool-list refresh — would be inert.
    pub(super) fn spawn_notification_drain(
        &self,
        server_id: String,
        expected_runtime: Arc<ServerRuntime>,
        mut receiver: tokio::sync::mpsc::Receiver<JsonRpcNotification>,
    ) {
        let manager = self.clone();
        tokio::spawn(async move {
            while let Some(notification) = receiver.recv().await {
                let still_current = manager
                    .runtimes
                    .get(&server_id)
                    .is_some_and(|runtime| Arc::ptr_eq(runtime.value(), &expected_runtime));
                if !still_current {
                    break;
                }
                manager
                    .dispatch_server_notification(&server_id, notification)
                    .await;
            }
            tracing::trace!(
                "MCP notification drain for server '{}' exited (channel closed)",
                server_id
            );
        });
    }

    /// Dispatches a single server-initiated notification. Handles
    /// `tools/list_changed` by refreshing the server's tool list (re-registering
    /// the tool index + emitting `ToolsChanged`); all other methods are drained and
    /// traced so the queue can never saturate. #366.
    async fn dispatch_server_notification(
        &self,
        server_id: &str,
        notification: JsonRpcNotification,
    ) {
        let method = notification.method.as_str();
        if TOOLS_LIST_CHANGED_METHODS.contains(&method) {
            info!(
                "MCP server '{}' announced '{}'; refreshing tool list",
                server_id, method
            );
            if let Err(e) = self.refresh_tools(server_id).await {
                warn!(
                    "Failed to refresh tools for MCP server '{}' after '{}': {}",
                    server_id, method, e
                );
            }
        } else {
            tracing::trace!(
                "MCP server '{}' notification '{}' drained (no dispatcher)",
                server_id,
                method
            );
        }
    }
}
