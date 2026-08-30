use tokio::time::Duration;

use crate::protocol::models::JsonRpcNotification;

use super::*;

impl McpServerManager {
    /// Health and QoS share this runtime-local singleflight. Every attempt is
    /// affine to the exact expected publication/runtime and can never reconnect
    /// a successor selected only by server id.
    pub(super) async fn attempt_reconnection(&self, expected: ExpectedPublication) -> Result<()> {
        let runtime = expected.runtime().clone();
        if runtime
            .runtime
            .reconnecting
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Ok(());
        }

        let reconnect = runtime.runtime.config.reconnect.clone();
        let mut backoff = reconnect.initial_backoff_ms;
        let mut attempt = 0u32;
        loop {
            if !self.is_current_publication(&expected) {
                runtime.runtime.reconnecting.store(false, Ordering::SeqCst);
                return Ok(());
            }
            if reconnect.max_attempts > 0 && attempt >= reconnect.max_attempts {
                let sequence = self.event_sequence_lock.clone().lock_owned().await;
                let reconcile = self.reconcile_lock.lock().await;
                if !self.is_current_publication(&expected) {
                    runtime.runtime.reconnecting.store(false, Ordering::SeqCst);
                    return Ok(());
                }
                {
                    let mut info = runtime.runtime.info.write().await;
                    info.status = ServerStatus::Error;
                    info.last_error = Some("Max reconnection attempts reached".to_string());
                    info.disconnected_at = Some(Utc::now());
                }
                let events = self.prepare_event_batch(
                    sequence,
                    vec![McpEvent::ServerStatusChanged {
                        server_id: expected.server_id().to_string(),
                        status: ServerStatus::Error,
                        error: Some("Max reconnection attempts reached".to_string()),
                    }],
                    Vec::new(),
                );
                runtime.runtime.reconnecting.store(false, Ordering::SeqCst);
                drop(reconcile);
                events.activate();
                return Err(McpError::Connection(
                    "maximum MCP reconnection attempts reached".to_string(),
                ));
            }

            attempt = attempt.saturating_add(1);
            tokio::time::sleep(Duration::from_millis(backoff)).await;
            if !self.is_current_publication(&expected) {
                runtime.runtime.reconnecting.store(false, Ordering::SeqCst);
                return Ok(());
            }

            match self.reconnect_server(expected.clone()).await {
                Ok(false) => {
                    runtime.runtime.reconnecting.store(false, Ordering::SeqCst);
                    return Ok(());
                }
                Ok(true) => {
                    runtime.runtime.reconnecting.store(false, Ordering::SeqCst);
                    return Ok(());
                }
                Err(error) => {
                    let _sequence = self.event_sequence_lock.lock().await;
                    let _reconcile = self.reconcile_lock.lock().await;
                    if !self.is_current_publication(&expected) {
                        runtime.runtime.reconnecting.store(false, Ordering::SeqCst);
                        return Ok(());
                    }
                    runtime.runtime.info.write().await.last_error = Some(error.to_string());
                    if reconnect.max_backoff_ms > backoff {
                        backoff = backoff.saturating_mul(2).min(reconnect.max_backoff_ms);
                    }
                }
            }
        }
    }

    pub(super) async fn reconnect_server(&self, expected: ExpectedPublication) -> Result<bool> {
        let restart_count = expected
            .runtime()
            .runtime
            .info
            .read()
            .await
            .restart_count
            .saturating_add(1);
        let prepared = self
            .prepare_server_runtime_with_restart(
                expected.runtime().runtime.config.clone(),
                "reconnect",
                restart_count,
            )
            .await?;
        self.publish_reconnected_runtime_if_current(expected, prepared)
            .await
    }

    pub(super) async fn publish_reconnected_runtime_if_current(
        &self,
        expected: ExpectedPublication,
        prepared: PreparedServerRuntime,
    ) -> Result<bool> {
        let sequence = self.event_sequence_lock.clone().lock_owned().await;
        let reconcile = self.reconcile_lock.lock().await;
        if !self.is_current_publication(&expected) {
            return Ok(false);
        }
        let replacement = prepared.publication().clone();
        let base = self.authority.generation();
        if !Arc::ptr_eq(
            base.servers
                .get(expected.server_id())
                .expect("validated current server"),
            &expected.publication,
        ) {
            return Ok(false);
        }
        let next = McpRuntimeGeneration::plan(
            &base,
            std::slice::from_ref(&replacement),
            &[],
            self.authority.ledger_relationship_limit,
            true,
        )?;

        let events = self.prepare_event_batch(
            sequence,
            self.runtime_ready_events(&replacement),
            Vec::new(),
        );
        let mut commit = prepared.into_commit();

        #[cfg(test)]
        self.observe_publish(PublishProbePhase::BeforeFenceAndSwap);
        self.authority.replace_prevalidated_with(&base, next, || {
            expected.publication.retire_with_runtime();
            #[cfg(test)]
            self.observe_publish(PublishProbePhase::AfterFencesBeforeSwap);
        });
        commit.mark_published();
        commit.activate();
        #[cfg(test)]
        self.observe_publish(PublishProbePhase::AfterTransferAndSwapBeforeUnlock);
        drop(reconcile);
        events.activate();
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
        client.connect().await.map_err(|error| {
            error!(
                "Failed to connect MCP server '{}' during {}: {}",
                server_id, phase, error
            );
            error
        })?;
        let init_result = client
            .initialize(config.request_timeout_ms)
            .await
            .map_err(|error| {
                error!(
                    "Failed to initialize MCP server '{}' during {}: {}",
                    server_id, phase, error
                );
                error
            })?;
        let instructions = init_result
            .instructions
            .map(|instructions| instructions.trim().to_string())
            .filter(|instructions| !instructions.is_empty());
        let tools = client.list_tools(config.request_timeout_ms).await?;
        let notification_rx = client.take_notification_receiver().await;
        Ok((client, tools, instructions, notification_rx))
    }

    async fn build_transport(&self, config: &TransportConfig) -> Result<Box<dyn McpTransport>> {
        match config {
            TransportConfig::Stdio(stdio_config) => {
                Ok(Box::new(StdioTransport::new(stdio_config.clone())))
            }
            TransportConfig::Sse(sse_config) => {
                if let Some(config_handle) = self.config.as_ref() {
                    let config = config_handle.read().await.clone();
                    let client =
                        bamboo_llm::http_client::build_http_client(&config).map_err(|error| {
                            McpError::InvalidConfig(format!(
                                "Failed to build HTTP client for MCP SSE transport: {error}"
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
            TransportConfig::StreamableHttp(http_config) => {
                if let Some(config_handle) = self.config.as_ref() {
                    let config = config_handle.read().await.clone();
                    let client = bamboo_llm::http_client::build_http_client(&config).map_err(
                        |error| {
                            McpError::InvalidConfig(format!(
                                "Failed to build HTTP client for MCP StreamableHTTP transport: {error}"
                            ))
                        },
                    )?;
                    Ok(Box::new(StreamableHttpTransport::new_with_client(
                        http_config.clone(),
                        client,
                    )))
                } else {
                    Ok(Box::new(StreamableHttpTransport::new(http_config.clone())))
                }
            }
        }
    }
}
