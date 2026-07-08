use tokio::time::Duration;

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
            if runtime.shutdown.load(Ordering::SeqCst) {
                info!(
                    "Reconnection cancelled due to shutdown for MCP server '{}'",
                    server_id
                );
                runtime.reconnecting.store(false, Ordering::SeqCst);
                return Ok(());
            }

            // Check max attempts
            if reconnect_config.max_attempts > 0 && attempt >= reconnect_config.max_attempts {
                error!(
                    "Max reconnection attempts ({}) reached for MCP server '{}'",
                    reconnect_config.max_attempts, server_id
                );

                // Update status to Error
                let mut info = runtime.info.write().await;
                info.status = ServerStatus::Error;
                info.last_error = Some("Max reconnection attempts reached".to_string());
                info.disconnected_at = Some(Utc::now());

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

            // Attempt reconnection
            match self.reconnect_server(runtime.clone()).await {
                Ok(_) => {
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

                    runtime.reconnecting.store(false, Ordering::SeqCst);
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        "Reconnection attempt {} failed for MCP server '{}': {}",
                        attempt, server_id, e
                    );

                    // Update error info
                    let mut info = runtime.info.write().await;
                    info.last_error = Some(e.to_string());

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
    async fn reconnect_server(&self, runtime: Arc<ServerRuntime>) -> Result<()> {
        let server_id = runtime.config.id.clone();

        info!("Attempting to reconnect MCP server '{}'", server_id);

        // Disconnect existing client if connected
        {
            let mut client = runtime.client.write().await;
            if client.is_connected().await {
                let _ = client.disconnect().await;
            }
        }

        let (client, tools, instructions) = self
            .bootstrap_server_client(&server_id, &runtime.config, "reconnect")
            .await?;

        // Update client
        {
            let mut client_lock = runtime.client.write().await;
            *client_lock = client;
        }

        // Update tools
        {
            let mut tools_lock = runtime.tools.write().await;
            *tools_lock = tools.clone();
        }

        // Refresh the server's prompt instructions from the new init result.
        {
            let mut info = runtime.info.write().await;
            info.instructions = instructions;
        }

        // Re-register tools in index
        self.index.remove_server_tools(&server_id);
        let aliases = self.index.register_server_tools(
            &server_id,
            &tools,
            &runtime.config.allowed_tools,
            &runtime.config.denied_tools,
        );

        info!(
            "Re-registered {} MCP tools for server '{}'",
            aliases.len(),
            server_id
        );

        // Emit tools changed event
        if let Some(ref tx) = self.event_tx {
            let tool_names: Vec<String> = aliases.into_iter().map(|a| a.alias).collect();
            let _ = tx
                .send(McpEvent::ToolsChanged {
                    server_id,
                    tools: tool_names,
                })
                .await;
        }

        Ok(())
    }

    pub(super) async fn bootstrap_server_client(
        &self,
        server_id: &str,
        config: &McpServerConfig,
        phase: &'static str,
    ) -> Result<(McpProtocolClient, Vec<McpTool>, Option<String>)> {
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

        // Wire the server-notification consumer (#366): take this client's
        // notification receiver and spawn a drain task that dispatches notifications
        // (e.g. `tools/list_changed` -> refresh_tools). Runs on every (re)connect,
        // since bootstrap is the sole client-construction path for both `start` and
        // `reconnect`; the previous connection's drain exits when its old client is
        // dropped (all notification senders close). Without a consumer the queue
        // fills to capacity and silently drops every later notification.
        if let Some(rx) = client.take_notification_receiver().await {
            self.spawn_notification_drain(server_id.to_string(), rx);
        }

        Ok((client, tools, instructions))
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
