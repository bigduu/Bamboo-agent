use tokio::time::{interval, Duration};

use super::fingerprint::desired_proxy_fingerprint;
use super::*;

impl McpServerManager {
    /// Start a new MCP server connection.
    pub async fn start_server(&self, config: McpServerConfig) -> Result<()> {
        let server_id = config.id.clone();

        if self.runtimes.contains_key(&server_id) {
            return Err(McpError::AlreadyRunning(server_id));
        }

        info!("Starting MCP server '{}'", server_id);

        let runtime_proxy_fingerprint = desired_proxy_fingerprint(self.config.as_ref()).await;
        let (client, tools, instructions) = self
            .bootstrap_server_client(&server_id, &config, "start")
            .await?;

        // Create runtime
        let runtime = Arc::new(ServerRuntime {
            config: config.clone(),
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

        // Register tools in index
        let aliases = self.index.register_server_tools(
            &server_id,
            &tools,
            &config.allowed_tools,
            &config.denied_tools,
        );

        info!(
            "Registered {} MCP tools for server '{}'",
            aliases.len(),
            server_id
        );

        // Store runtime
        self.runtimes.insert(server_id.clone(), runtime.clone());

        // Emit event
        if let Some(ref tx) = self.event_tx {
            let _ = tx
                .send(McpEvent::ServerStatusChanged {
                    server_id: server_id.clone(),
                    status: ServerStatus::Ready,
                    error: None,
                })
                .await;

            let tool_names: Vec<String> = aliases.into_iter().map(|a| a.alias).collect();
            let _ = tx
                .send(McpEvent::ToolsChanged {
                    server_id,
                    tools: tool_names,
                })
                .await;
        }

        // Start health check task
        self.start_health_check(runtime, config.healthcheck_interval_ms);

        Ok(())
    }

    /// Stop an MCP server connection.
    pub async fn stop_server(&self, server_id: &str) -> Result<()> {
        let (_, runtime) = self
            .runtimes
            .remove(server_id)
            .ok_or_else(|| McpError::NotRunning(server_id.to_string()))?;

        info!("Stopping MCP server '{}'", server_id);

        runtime.shutdown.store(true, Ordering::SeqCst);

        // Disconnect client
        let mut client = runtime.client.write().await;
        if let Err(e) = client.disconnect().await {
            warn!("Error disconnecting MCP server '{}': {}", server_id, e);
        }

        // Update info
        let mut info = runtime.info.write().await;
        info.status = ServerStatus::Stopped;
        info.disconnected_at = Some(Utc::now());

        // Remove tools from index
        self.index.remove_server_tools(server_id);

        // Emit event
        if let Some(ref tx) = self.event_tx {
            let _ = tx
                .send(McpEvent::ServerStatusChanged {
                    server_id: server_id.to_string(),
                    status: ServerStatus::Stopped,
                    error: None,
                })
                .await;
        }

        info!("MCP server '{}' stopped", server_id);
        Ok(())
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
            .ok_or_else(|| McpError::ServerNotFound(server_id.to_string()))?;

        info!("Refreshing tools for MCP server '{}'", server_id);

        let client = runtime.client.read().await;
        let new_tools = client.list_tools(runtime.config.request_timeout_ms).await?;
        drop(client);

        // Update tools
        let mut tools = runtime.tools.write().await;
        *tools = new_tools.clone();
        drop(tools);

        // Update info
        let mut info = runtime.info.write().await;
        info.tool_count = new_tools.len();

        // Re-register tools
        self.index.remove_server_tools(server_id);
        let aliases = self.index.register_server_tools(
            server_id,
            &new_tools,
            &runtime.config.allowed_tools,
            &runtime.config.denied_tools,
        );

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

        Ok(())
    }

    fn start_health_check(&self, runtime: Arc<ServerRuntime>, interval_ms: u64) {
        let server_id = runtime.config.id.clone();
        let manager = Arc::new(self.clone());

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(interval_ms));

            loop {
                interval.tick().await;

                if runtime.shutdown.load(Ordering::SeqCst) {
                    break;
                }

                // Skip health check if currently reconnecting
                if runtime.reconnecting.load(Ordering::SeqCst) {
                    continue;
                }

                let client = runtime.client.read().await;
                match client.ping(runtime.config.request_timeout_ms).await {
                    Ok(_) => {
                        let mut info = runtime.info.write().await;
                        info.last_ping_at = Some(Utc::now());
                        if info.status == ServerStatus::Degraded {
                            info.status = ServerStatus::Ready;
                            // Emit recovery event
                            if let Some(ref tx) = manager.event_tx {
                                let _ = tx
                                    .send(McpEvent::ServerStatusChanged {
                                        server_id: server_id.clone(),
                                        status: ServerStatus::Ready,
                                        error: None,
                                    })
                                    .await;
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Health check failed for MCP server '{}': {}", server_id, e);

                        // Drop client lock before attempting reconnection
                        drop(client);

                        // Update status to Degraded
                        {
                            let mut info = runtime.info.write().await;
                            info.status = ServerStatus::Degraded;
                            info.last_error = Some(e.to_string());
                        }

                        // Emit degraded event
                        if let Some(ref tx) = manager.event_tx {
                            let _ = tx
                                .send(McpEvent::ServerStatusChanged {
                                    server_id: server_id.clone(),
                                    status: ServerStatus::Degraded,
                                    error: Some(e.to_string()),
                                })
                                .await;
                        }

                        // Attempt reconnection if enabled
                        if runtime.config.reconnect.enabled {
                            if let Err(reconnect_err) =
                                manager.attempt_reconnection(runtime.clone()).await
                            {
                                error!(
                                    "Reconnection failed for MCP server '{}': {}",
                                    server_id, reconnect_err
                                );
                            }
                        }
                    }
                }
            }
        });
    }
}
