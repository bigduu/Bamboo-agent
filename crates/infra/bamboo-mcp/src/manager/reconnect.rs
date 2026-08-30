use tokio::time::Duration;

use crate::protocol::models::JsonRpcNotification;

use super::*;

impl McpServerManager {
    /// Attempt to reconnect a degraded server with exponential backoff.
    pub(super) async fn attempt_reconnection(&self, runtime: Arc<ServerRuntime>) -> Result<()> {
        let server_id = runtime.config.id.clone();

        // Check if already reconnecting
        if runtime
            .reconnecting
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            info!(
                "Reconnection already in progress for MCP server '{}'",
                server_id
            );
            return Ok(());
        }

        let reconnect_config = &runtime.config.reconnect;
        let mut current_backoff = reconnect_config.initial_backoff_ms;
        let mut attempt = 0u32;

        info!(
            "Starting reconnection attempts for MCP server '{}' (max_attempts: {})",
            server_id,
            if reconnect_config.max_attempts == 0 {
                "unlimited".to_string()
            } else {
                reconnect_config.max_attempts.to_string()
            }
        );

        loop {
            // Check if shutdown was requested
            if runtime.shutdown.load(Ordering::SeqCst)
                || !self.is_current_runtime(&server_id, &runtime)
            {
                info!(
                    "Reconnection cancelled due to shutdown or replacement for MCP server '{}'",
                    server_id
                );
                runtime.reconnecting.store(false, Ordering::SeqCst);
                return Ok(());
            }

            // Check max attempts
            if reconnect_config.max_attempts > 0 && attempt >= reconnect_config.max_attempts {
                let reconcile = self.reconcile_lock.lock().await;
                if runtime.shutdown.load(Ordering::SeqCst)
                    || !self.is_current_runtime(&server_id, &runtime)
                {
                    runtime.reconnecting.store(false, Ordering::SeqCst);
                    return Ok(());
                }
                error!(
                    "Max reconnection attempts ({}) reached for MCP server '{}'",
                    reconnect_config.max_attempts, server_id
                );

                // Update status to Error
                let mut info = runtime.info.write().await;
                info.status = ServerStatus::Error;
                info.last_error = Some("Max reconnection attempts reached".to_string());
                info.disconnected_at = Some(Utc::now());
                drop(info);

                // Emit error event
                if let Some(ref tx) = self.event_tx {
                    let _ = tx
                        .send(McpEvent::ServerStatusChanged {
                            server_id: server_id.clone(),
                            status: ServerStatus::Error,
                            error: Some("Max reconnection attempts reached".to_string()),
                        })
                        .await;
                }
                drop(reconcile);

                runtime.reconnecting.store(false, Ordering::SeqCst);
                return Err(McpError::Connection(format!(
                    "Max reconnection attempts reached for server '{}'",
                    server_id
                )));
            }

            attempt += 1;
            info!(
                "Reconnection attempt {} for MCP server '{}' (backoff: {}ms)",
                attempt, server_id, current_backoff
            );

            // Wait for backoff period
            tokio::time::sleep(Duration::from_millis(current_backoff)).await;
            if runtime.shutdown.load(Ordering::SeqCst)
                || !self.is_current_runtime(&server_id, &runtime)
            {
                runtime.reconnecting.store(false, Ordering::SeqCst);
                return Ok(());
            }

            // Attempt reconnection
            match self.reconnect_server(runtime.clone()).await {
                Ok(false) => {
                    runtime.reconnecting.store(false, Ordering::SeqCst);
                    return Ok(());
                }
                Ok(true) => {
                    let reconcile = self.reconcile_lock.lock().await;
                    if runtime.shutdown.load(Ordering::SeqCst)
                        || !self.is_current_runtime(&server_id, &runtime)
                    {
                        runtime.reconnecting.store(false, Ordering::SeqCst);
                        return Ok(());
                    }
                    info!(
                        "Successfully reconnected MCP server '{}' after {} attempt(s)",
                        server_id, attempt
                    );

                    // Update runtime info
                    let mut info = runtime.info.write().await;
                    info.status = ServerStatus::Ready;
                    info.last_error = None;
                    info.restart_count += 1;
                    info.disconnected_at = None;
                    drop(info);

                    // Emit recovery event
                    if let Some(ref tx) = self.event_tx {
                        let _ = tx
                            .send(McpEvent::ServerStatusChanged {
                                server_id: server_id.clone(),
                                status: ServerStatus::Ready,
                                error: None,
                            })
                            .await;
                    }
                    drop(reconcile);

                    runtime.reconnecting.store(false, Ordering::SeqCst);
                    return Ok(());
                }
                Err(e) => {
                    let reconcile = self.reconcile_lock.lock().await;
                    if runtime.shutdown.load(Ordering::SeqCst)
                        || !self.is_current_runtime(&server_id, &runtime)
                    {
                        runtime.reconnecting.store(false, Ordering::SeqCst);
                        return Ok(());
                    }
                    warn!(
                        "Reconnection attempt {} failed for MCP server '{}': {}",
                        attempt, server_id, e
                    );

                    // Update error info
                    let mut info = runtime.info.write().await;
                    info.last_error = Some(e.to_string());
                    drop(info);
                    drop(reconcile);

                    // Calculate next backoff with exponential increase
                    if reconnect_config.max_backoff_ms > current_backoff {
                        current_backoff =
                            std::cmp::min(current_backoff * 2, reconnect_config.max_backoff_ms);
                    }
                }
            }
        }
    }

    /// Internal method to reconnect a single server.
    pub(super) async fn reconnect_server(&self, runtime: Arc<ServerRuntime>) -> Result<bool> {
        let server_id = runtime.config.id.clone();

        info!("Attempting to reconnect MCP server '{}'", server_id);

        let (client, tools, instructions, notification_rx) = self
            .bootstrap_server_client(&server_id, &runtime.config, "reconnect")
            .await?;

        self.publish_reconnected_runtime_if_current(
            &server_id,
            &runtime,
            client,
            tools,
            instructions,
            notification_rx,
        )
        .await
    }

    pub(super) async fn publish_reconnected_runtime_if_current(
        &self,
        server_id: &str,
        runtime: &Arc<ServerRuntime>,
        mut client: McpProtocolClient,
        tools: Vec<McpTool>,
        instructions: Option<String>,
        notification_rx: Option<tokio::sync::mpsc::Receiver<JsonRpcNotification>>,
    ) -> Result<bool> {
        // Bootstrap may span a replacement. Serialize this final generation
        // check with transactional commit before touching runtime state or the
        // shared tool index.
        let _reconcile = self.reconcile_lock.lock().await;
        if runtime.shutdown.load(Ordering::SeqCst) || !self.is_current_runtime(server_id, runtime) {
            drop(_reconcile);
            let _ = client.disconnect().await;
            return Ok(false);
        }

        let catalog = match self.index.plan_server_tools(
            server_id,
            &tools,
            &runtime.config.allowed_tools,
            &runtime.config.denied_tools,
        ) {
            Ok(catalog) => catalog,
            Err(error) => {
                drop(_reconcile);
                let _ = client.disconnect().await;
                return Err(error.into());
            }
        };
        let aliases = catalog.aliases();
        let catalog_update = match self
            .index
            .preflight_catalog_update(std::slice::from_ref(&catalog), &[])
        {
            Ok(update) => update,
            Err(error) => {
                drop(_reconcile);
                let _ = client.disconnect().await;
                return Err(error.into());
            }
        };

        // Acquire every suspending guard before the publication point. Once
        // the new client is swapped, the index and runtime metadata are updated
        // without another await, so registration failure preserves the entire
        // prior generation.
        let mut client_lock = runtime.client.write().await;
        let mut tools_lock = runtime.tools.write().await;
        let mut info = runtime.info.write().await;
        let mut old_client = std::mem::replace(&mut *client_lock, client);
        self.index.commit_catalog_update(catalog_update);
        *tools_lock = tools;
        info.instructions = instructions;
        info.tool_count = tools_lock.len();
        drop(info);
        drop(tools_lock);
        drop(client_lock);

        // Spawn the notification drain only AFTER the new client is swapped in, so
        // an immediate `tools/list_changed` refreshes against the NEW connection
        // (not the old, disconnected one). (#420)
        if let Some(rx) = notification_rx {
            self.spawn_notification_drain(server_id.to_string(), runtime.clone(), rx);
        }

        info!(
            "Re-registered {} MCP tools for server '{}'",
            aliases.len(),
            server_id
        );

        // Transport shutdown and event-channel backpressure are post-commit
        // cleanup, so cancellation cannot strand a half-published reconnect.
        tokio::spawn(async move {
            if old_client.disconnect().await.is_err() {
                warn!("Failed to disconnect replaced MCP client");
            }
        });
        if let Some(ref tx) = self.event_tx {
            let tool_names: Vec<String> = aliases.into_iter().map(|a| a.alias).collect();
            let _ = tx
                .send(McpEvent::ToolsChanged {
                    server_id: server_id.to_string(),
                    tools: tool_names,
                })
                .await;
        }
        drop(_reconcile);

        Ok(true)
    }

    pub(super) async fn bootstrap_server_client(
        &self,
        server_id: &str,
        config: &McpServerConfig,
        phase: &'static str,
    ) -> Result<(
        McpProtocolClient,
        Vec<McpTool>,
        Option<String>,
        Option<tokio::sync::mpsc::Receiver<JsonRpcNotification>>,
    )> {
        let transport = self.build_transport(&config.transport).await?;
        let mut client = McpProtocolClient::new(transport);

        client.connect().await.map_err(|e| {
            error!(
                "Failed to connect MCP server '{}' during {}: {}",
                server_id, phase, e
            );
            e
        })?;

        let init_result = client
            .initialize(config.request_timeout_ms)
            .await
            .map_err(|e| {
                error!(
                    "Failed to initialize MCP server '{}' during {}: {}",
                    server_id, phase, e
                );
                e
            })?;

        info!(
            "MCP server '{}' initialized during {}: {} v{}",
            server_id, phase, init_result.server_info.name, init_result.server_info.version
        );

        // The server's optional `instructions` (how-to-use guidance) — surfaced
        // into the system prompt while this server is connected.
        let instructions = init_result
            .instructions
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        let tools = client.list_tools(config.request_timeout_ms).await?;
        info!(
            "MCP server '{}' has {} tools during {}",
            server_id,
            tools.len(),
            phase
        );

        // Take this client's server-notification receiver (#366) and hand it BACK
        // to the caller rather than spawning the drain here. The drain dispatches
        // `tools/list_changed` -> `refresh_tools`, which resolves the runtime by
        // `server_id` and reads `runtime.client` — so it must not start until the
        // caller has REGISTERED the runtime (`start`) / SWAPPED in the new client
        // (`reconnect`). Spawning it inside bootstrap raced those: an immediate
        // notification could hit `ServerNotFound` (start) or read the old,
        // disconnected client (reconnect). (#420)
        let notification_rx = client.take_notification_receiver().await;

        Ok((client, tools, instructions, notification_rx))
    }

    async fn build_transport(&self, config: &TransportConfig) -> Result<Box<dyn McpTransport>> {
        match config {
            TransportConfig::Stdio(stdio_config) => {
                Ok(Box::new(StdioTransport::new(stdio_config.clone())))
            }
            TransportConfig::Sse(sse_config) => {
                // SSE uses HTTP; ensure it respects user-configured proxy settings when available.
                if let Some(cfg_handle) = self.config.as_ref() {
                    let cfg = cfg_handle.read().await.clone();
                    let client = bamboo_llm::http_client::build_http_client(&cfg).map_err(|e| {
                        McpError::InvalidConfig(format!(
                            "Failed to build HTTP client for MCP SSE transport: {e}"
                        ))
                    })?;
                    Ok(Box::new(SseTransport::new_with_client(
                        sse_config.clone(),
                        client,
                    )))
                } else {
                    Ok(Box::new(SseTransport::new(sse_config.clone())))
                }
            }
            TransportConfig::StreamableHttp(sh_config) => {
                // Streamable HTTP uses HTTP; respect user-configured proxy settings.
                if let Some(cfg_handle) = self.config.as_ref() {
                    let cfg = cfg_handle.read().await.clone();
                    let client = bamboo_llm::http_client::build_http_client(&cfg).map_err(|e| {
                        McpError::InvalidConfig(format!(
                            "Failed to build HTTP client for MCP StreamableHTTP transport: {e}"
                        ))
                    })?;
                    Ok(Box::new(StreamableHttpTransport::new_with_client(
                        sh_config.clone(),
                        client,
                    )))
                } else {
                    Ok(Box::new(StreamableHttpTransport::new(sh_config.clone())))
                }
            }
        }
    }
}
