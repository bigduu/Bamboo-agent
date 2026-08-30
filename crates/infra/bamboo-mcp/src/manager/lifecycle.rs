use tokio::sync::{oneshot, OwnedMutexGuard};
use tokio::time::{interval, Duration};

use super::fingerprint::desired_proxy_fingerprint;
use super::generation::{admit_resolved, AdmittedMcpCall};
use super::*;
use crate::protocol::models::JsonRpcNotification;

const TOOLS_LIST_CHANGED_METHODS: [&str; 2] =
    ["notifications/tools/list_changed", "tools/list_changed"];

pub(super) struct RetiredRuntimeCleanup {
    pub(super) runtime: Arc<TransportRuntime>,
}

/// One already-validated event batch. The task owns the global publication
/// sequencer before it can observe the activation gate. Generation writers can
/// therefore finish without waiting for bounded output capacity, while a
/// successor cannot publish until every event in this batch has been delivered.
pub(super) struct PreparedEventBatch {
    gate: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl PreparedEventBatch {
    pub(super) fn activate(mut self) {
        if let Some(gate) = self.gate.take() {
            let _ = gate.send(());
        }
        // Dropping a JoinHandle detaches rather than aborts the activated task.
        self.task = None;
    }
}

impl Drop for PreparedEventBatch {
    fn drop(&mut self) {
        self.gate = None;
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl McpServerManager {
    /// Start and publish one fully initialized MCP server.
    pub async fn start_server(&self, config: McpServerConfig) -> Result<()> {
        let sequence = self.event_sequence_lock.clone().lock_owned().await;
        let reconcile = self.reconcile_lock.lock().await;
        let events = self.start_server_unlocked(config, sequence).await;
        drop(reconcile);
        match events {
            Ok(events) => {
                events.activate();
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    async fn start_server_unlocked(
        &self,
        config: McpServerConfig,
        sequence: OwnedMutexGuard<()>,
    ) -> Result<PreparedEventBatch> {
        let server_id = config.id.clone();
        if self.is_server_running(&server_id) {
            return Err(McpError::AlreadyRunning(server_id));
        }

        info!("Starting MCP server '{}'", server_id);
        let prepared = self.prepare_server_runtime(config, "start").await?;
        let publication = prepared.publication().clone();
        let base = self.authority.generation();
        let next = McpRuntimeGeneration::plan(
            &base,
            std::slice::from_ref(&publication),
            &[],
            self.authority.ledger_relationship_limit,
            true,
        )?;
        let events = self.prepare_event_batch(
            sequence,
            self.runtime_ready_events(&publication),
            Vec::new(),
        );
        let mut commit = prepared.into_commit();
        debug_assert!(self.authority.is_current(&base));
        #[cfg(test)]
        self.observe_publish(PublishProbePhase::BeforeFenceAndSwap);
        self.authority.replace_prevalidated_with(&base, next, || {
            #[cfg(test)]
            self.observe_publish(PublishProbePhase::AfterFencesBeforeSwap);
        });
        commit.mark_published();
        commit.activate();
        #[cfg(test)]
        self.observe_publish(PublishProbePhase::AfterTransferAndSwapBeforeUnlock);
        Ok(events)
    }

    pub(super) async fn prepare_server_runtime(
        &self,
        config: McpServerConfig,
        phase: &'static str,
    ) -> Result<PreparedServerRuntime> {
        self.prepare_server_runtime_with_restart(config, phase, 0)
            .await
    }

    pub(super) async fn prepare_server_runtime_with_restart(
        &self,
        config: McpServerConfig,
        phase: &'static str,
        restart_count: u32,
    ) -> Result<PreparedServerRuntime> {
        let server_id = config.id.clone();
        let runtime_proxy_fingerprint = desired_proxy_fingerprint(self.config.as_ref()).await;
        let (client, tools, instructions, notification_rx) = self
            .bootstrap_server_client(&server_id, &config, phase)
            .await?;
        let catalog = self.index.plan_server_tools(
            &server_id,
            &tools,
            &config.allowed_tools,
            &config.denied_tools,
        )?;
        #[cfg(test)]
        let mut catalog = catalog;
        #[cfg(test)]
        if let Some(probe) = &self.catalog_plan_probe {
            probe(&server_id, &mut catalog);
        }
        let tool_count = catalog.aliases().len();
        let runtime = ServerRuntime {
            config,
            info: tokio::sync::RwLock::new(RuntimeInfo {
                status: ServerStatus::Ready,
                last_error: None,
                connected_at: Some(Utc::now()),
                disconnected_at: None,
                tool_count,
                restart_count,
                last_ping_at: Some(Utc::now()),
                instructions,
            }),
            reconnecting: AtomicBool::new(false),
            qos: McpServerQos::new(McpQosConfig::default()),
            proxy_fingerprint: runtime_proxy_fingerprint,
        };
        let runtime = TransportRuntime::new(self.allocate_runtime_id()?, runtime, client);
        let publication =
            ServerPublication::new(self.allocate_publication_id()?, runtime, catalog, &tools)?;
        let activation = self.prepare_runtime_tasks(publication.clone(), notification_rx);
        Ok(PreparedServerRuntime {
            publication: Some(publication),
            activation: Some(activation),
        })
    }

    pub(super) fn prepare_runtime_tasks(
        &self,
        publication: Arc<ServerPublication>,
        notification_rx: Option<tokio::sync::mpsc::Receiver<JsonRpcNotification>>,
    ) -> RuntimeActivation {
        let (gate, health_gate) = tokio::sync::watch::channel(false);
        let runtime = publication.runtime.clone();
        let mut handles = vec![self.spawn_health_check(runtime.clone(), health_gate)];
        if let Some(receiver) = notification_rx {
            handles.push(self.spawn_notification_drain(
                runtime.clone(),
                receiver,
                gate.subscribe(),
            ));
        }
        #[cfg(test)]
        let task_count = handles.len();
        runtime.install_tasks(handles);
        let activation = RuntimeActivation {
            gate: Some(gate),
            #[cfg(test)]
            task_count,
            #[cfg(test)]
            probe: self.task_probe.clone(),
        };
        #[cfg(test)]
        activation.observe(TaskProbePhase::PreparedAndGated);
        activation
    }

    pub(super) fn runtime_ready_events(
        &self,
        publication: &Arc<ServerPublication>,
    ) -> Vec<McpEvent> {
        vec![
            McpEvent::ServerStatusChanged {
                server_id: publication.server_id.clone(),
                status: ServerStatus::Ready,
                error: None,
            },
            McpEvent::ToolsChanged {
                server_id: publication.server_id.clone(),
                tools: publication
                    .catalog
                    .aliases()
                    .into_iter()
                    .map(|alias| alias.alias)
                    .collect(),
            },
        ]
    }

    pub(super) fn prepare_event_batch(
        &self,
        sequence: OwnedMutexGuard<()>,
        events: Vec<McpEvent>,
        retired: Vec<RetiredRuntimeCleanup>,
    ) -> PreparedEventBatch {
        #[cfg(test)]
        self.observe_event(EventProbePhase::BeforeBatchValidation);
        let tx = self.event_tx.clone();
        #[cfg(test)]
        let event_probe = self.event_probe.clone();
        let (gate, activated) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _sequence = sequence;
            if activated.await.is_err() {
                return;
            }
            #[cfg(test)]
            if let Some(probe) = &event_probe {
                probe(EventProbePhase::AcceptedBatchBeforeFirstDelivery);
            }
            for cleanup in retired {
                cleanup.runtime.retire();
                let mut info = cleanup.runtime.runtime.info.write().await;
                info.status = ServerStatus::Stopped;
                info.disconnected_at = Some(Utc::now());
            }
            let Some(tx) = tx else {
                return;
            };
            for event in events {
                #[cfg(test)]
                if let Some(probe) = &event_probe {
                    probe(EventProbePhase::BeforeOutputSend);
                }
                if tx.send(event).await.is_err() {
                    return;
                }
            }
        });
        PreparedEventBatch {
            gate: Some(gate),
            task: Some(task),
        }
    }

    /// Remove one server publication and retire its exact transport runtime.
    pub async fn stop_server(&self, server_id: &str) -> Result<()> {
        let sequence = self.event_sequence_lock.clone().lock_owned().await;
        let reconcile = self.reconcile_lock.lock().await;
        let events = self.stop_server_unlocked(server_id, sequence);
        drop(reconcile);
        match events {
            Ok(events) => {
                events.activate();
                info!("MCP server '{}' stopped", server_id);
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    fn stop_server_unlocked(
        &self,
        server_id: &str,
        sequence: OwnedMutexGuard<()>,
    ) -> Result<PreparedEventBatch> {
        info!("Stopping MCP server '{}'", server_id);
        let base = self.authority.generation();
        let publication = base
            .servers
            .get(server_id)
            .cloned()
            .ok_or_else(|| McpError::NotRunning(server_id.to_string()))?;
        let next = McpRuntimeGeneration::plan(
            &base,
            &[],
            &[server_id.to_string()],
            self.authority.ledger_relationship_limit,
            true,
        )?;
        let events = self.prepare_event_batch(
            sequence,
            vec![McpEvent::ServerStatusChanged {
                server_id: server_id.to_string(),
                status: ServerStatus::Stopped,
                error: None,
            }],
            vec![RetiredRuntimeCleanup {
                runtime: publication.runtime.clone(),
            }],
        );
        #[cfg(test)]
        self.observe_publish(PublishProbePhase::BeforeFenceAndSwap);
        self.authority.replace_prevalidated_with(&base, next, || {
            publication.retire_with_runtime();
            #[cfg(test)]
            self.observe_publish(PublishProbePhase::AfterFencesBeforeSwap);
        });
        #[cfg(test)]
        self.observe_publish(PublishProbePhase::AfterTransferAndSwapBeforeUnlock);
        Ok(events)
    }

    /// Execute an original server tool name through one resolved generation.
    pub async fn call_tool(
        &self,
        server_id: &str,
        tool_name: &str,
        args: serde_json::Value,
    ) -> Result<crate::types::McpCallResult> {
        let snapshot = self.snapshot();
        let resolved = snapshot
            .resolve_server_tool(server_id, tool_name)
            .ok_or_else(|| {
                if snapshot.contains_server(server_id) {
                    McpError::ToolNotFound(tool_name.to_string())
                } else {
                    McpError::ServerNotFound(server_id.to_string())
                }
            })?;
        self.call_resolved_tool(&resolved, args).await
    }

    /// Execute a passive exact ticket after linearizable admission.
    pub async fn call_resolved_tool(
        &self,
        resolved: &ResolvedMcpCall,
        args: serde_json::Value,
    ) -> Result<crate::types::McpCallResult> {
        let admitted = admit_resolved(&self.authority, resolved)?;
        self.call_admitted_tool(admitted, args).await
    }

    /// Execute one already-admitted exact lease without any live name lookup.
    async fn call_admitted_tool(
        &self,
        admitted: AdmittedMcpCall,
        args: serde_json::Value,
    ) -> Result<crate::types::McpCallResult> {
        if !admitted.resolved.belongs_to(&self.authority) {
            return Err(McpError::ForeignRuntimeAuthority);
        }
        let server_id = admitted.resolved.server_id().to_string();
        let tool_name = admitted.resolved.original_name().to_string();
        let runtime = admitted.runtime().clone();
        runtime
            .runtime
            .qos
            .check_circuit(&server_id, &tool_name)
            .await?;
        let _permit = runtime.runtime.qos.acquire_permit().await?;
        let timeout = runtime.runtime.config.request_timeout_ms;
        let result = admitted.client().call_tool(&tool_name, args, timeout).await;

        let result = match result {
            Ok(result) if result.is_error => {
                let synthetic =
                    McpError::ToolExecution(format!("tool '{tool_name}' returned an error result"));
                let should_recycle = runtime
                    .runtime
                    .qos
                    .record_failure(&server_id, &tool_name, &synthetic)
                    .await;
                self.maybe_recycle_server(admitted.resolved.expected(), should_recycle);
                result
            }
            Ok(result) => {
                runtime.runtime.qos.record_success().await;
                result
            }
            Err(error) => {
                let should_recycle = runtime
                    .runtime
                    .qos
                    .record_failure(&server_id, &tool_name, &error)
                    .await;
                self.maybe_recycle_server(admitted.resolved.expected(), should_recycle);
                return Err(error);
            }
        };

        let expected = admitted.resolved.expected();
        let sequence = self.event_sequence_lock.clone().lock_owned().await;
        let reconcile = self.reconcile_lock.lock().await;
        let events = self.is_current_publication(&expected).then(|| {
            self.prepare_event_batch(
                sequence,
                vec![McpEvent::ToolExecuted {
                    server_id,
                    tool_name,
                    success: !result.is_error,
                }],
                Vec::new(),
            )
        });
        drop(reconcile);
        if let Some(events) = events {
            events.activate();
        }
        Ok(result)
    }

    pub(super) fn maybe_recycle_server(&self, expected: ExpectedPublication, should_recycle: bool) {
        let runtime = expected.runtime();
        if !should_recycle
            || !runtime.runtime.config.reconnect.enabled
            || !self.is_current_publication(&expected)
        {
            return;
        }
        let manager = self.clone();
        let server_id = expected.server_id().to_string();
        warn!(
            "Recycling MCP server '{}' after repeated tool failures (disconnect + reconnect)",
            server_id
        );
        tokio::spawn(async move {
            if let Err(error) = manager.attempt_reconnection(expected).await {
                warn!("MCP server '{}' recycle failed: {}", server_id, error);
            }
        });
    }

    /// Return one published tool's immutable provider metadata.
    pub fn get_tool_info(&self, server_id: &str, tool_name: &str) -> Option<McpTool> {
        self.snapshot().tool(server_id, tool_name)
    }

    /// Refresh a server's catalog using its exact currently-published runtime.
    pub async fn refresh_tools(&self, server_id: &str) -> Result<()> {
        let expected = self
            .current_expected(server_id)
            .ok_or_else(|| McpError::ServerNotFound(server_id.to_string()))?;
        self.refresh_tools_for_expected(expected).await.map(|_| ())
    }

    async fn refresh_tools_for_expected(&self, expected: ExpectedPublication) -> Result<bool> {
        let server_id = expected.server_id().to_string();
        info!("Refreshing tools for MCP server '{}'", server_id);
        let client = expected.runtime().client_if_open()?;
        let new_tools = client
            .list_tools(expected.runtime().runtime.config.request_timeout_ms)
            .await?;
        self.publish_refreshed_tools_if_current(expected, new_tools)
            .await
    }

    pub(super) async fn publish_refreshed_tools_if_current(
        &self,
        expected: ExpectedPublication,
        new_tools: Vec<McpTool>,
    ) -> Result<bool> {
        let sequence = self.event_sequence_lock.clone().lock_owned().await;
        let reconcile = self.reconcile_lock.lock().await;
        if !self.is_current_publication(&expected) {
            return Ok(false);
        }
        let server_id = expected.server_id().to_string();
        let config = &expected.runtime().runtime.config;
        let catalog = self.index.plan_server_tools(
            &server_id,
            &new_tools,
            &config.allowed_tools,
            &config.denied_tools,
        )?;
        let replacement = ServerPublication::new(
            self.allocate_publication_id()?,
            expected.runtime().clone(),
            catalog,
            &new_tools,
        )?;
        let base = self.authority.generation();
        if !Arc::ptr_eq(
            base.servers
                .get(&server_id)
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
            vec![McpEvent::ToolsChanged {
                server_id,
                tools: replacement
                    .catalog
                    .aliases()
                    .into_iter()
                    .map(|alias| alias.alias)
                    .collect(),
            }],
            Vec::new(),
        );
        #[cfg(test)]
        self.observe_publish(PublishProbePhase::BeforeFenceAndSwap);
        self.authority.replace_prevalidated_with(&base, next, || {
            expected.publication.close_admission();
            #[cfg(test)]
            self.observe_publish(PublishProbePhase::AfterFencesBeforeSwap);
        });
        #[cfg(test)]
        self.observe_publish(PublishProbePhase::AfterTransferAndSwapBeforeUnlock);
        drop(reconcile);
        events.activate();
        Ok(true)
    }

    fn spawn_health_check(
        &self,
        runtime: Arc<TransportRuntime>,
        mut activation: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let manager = self.clone();
        tokio::spawn(async move {
            if !Self::wait_for_runtime_activation(&mut activation).await {
                return;
            }
            let mut ticker = interval(Duration::from_millis(
                runtime.runtime.config.healthcheck_interval_ms,
            ));
            loop {
                ticker.tick().await;
                let Some(expected) = manager.current_expected_for_runtime(&runtime) else {
                    break;
                };
                if runtime.runtime.reconnecting.load(Ordering::SeqCst) {
                    continue;
                }
                let result = match runtime.client_if_open() {
                    Ok(client) => client
                        .ping(runtime.runtime.config.request_timeout_ms)
                        .await
                        .map_err(|error| error.to_string()),
                    Err(_) => break,
                };
                let Some(should_reconnect) = manager
                    .publish_health_result_if_current(expected.clone(), result)
                    .await
                else {
                    continue;
                };
                if should_reconnect {
                    if let Err(error) = manager.attempt_reconnection(expected).await {
                        error!("MCP health-triggered reconnection failed: {}", error);
                    }
                }
            }
        })
    }

    pub(super) async fn publish_health_result_if_current(
        &self,
        expected: ExpectedPublication,
        result: std::result::Result<(), String>,
    ) -> Option<bool> {
        let sequence = self.event_sequence_lock.clone().lock_owned().await;
        let reconcile = self.reconcile_lock.lock().await;
        if !self.is_current_publication(&expected) {
            return None;
        }
        let runtime = expected.runtime();
        let (should_reconnect, event) = match result {
            Ok(()) => {
                let mut info = runtime.runtime.info.write().await;
                info.last_ping_at = Some(Utc::now());
                let recovered = info.status == ServerStatus::Degraded;
                if recovered {
                    info.status = ServerStatus::Ready;
                }
                drop(info);
                let event = recovered.then(|| McpEvent::ServerStatusChanged {
                    server_id: expected.server_id().to_string(),
                    status: ServerStatus::Ready,
                    error: None,
                });
                (false, event)
            }
            Err(error) => {
                warn!(
                    "Health check failed for MCP server '{}': {}",
                    expected.server_id(),
                    error
                );
                {
                    let mut info = runtime.runtime.info.write().await;
                    info.status = ServerStatus::Degraded;
                    info.last_error = Some(error.clone());
                }
                (
                    runtime.runtime.config.reconnect.enabled,
                    Some(McpEvent::ServerStatusChanged {
                        server_id: expected.server_id().to_string(),
                        status: ServerStatus::Degraded,
                        error: Some(error),
                    }),
                )
            }
        };
        let events = event.map(|event| self.prepare_event_batch(sequence, vec![event], Vec::new()));
        drop(reconcile);
        if let Some(events) = events {
            events.activate();
        }
        Some(should_reconnect)
    }

    fn spawn_notification_drain(
        &self,
        runtime: Arc<TransportRuntime>,
        mut receiver: tokio::sync::mpsc::Receiver<JsonRpcNotification>,
        mut activation: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let manager = self.clone();
        tokio::spawn(async move {
            if !Self::wait_for_runtime_activation(&mut activation).await {
                return;
            }
            loop {
                let Some(expected) = manager.current_expected_for_runtime(&runtime) else {
                    break;
                };
                let Some(notification) = receiver.recv().await else {
                    break;
                };
                manager
                    .dispatch_server_notification(expected, notification)
                    .await;
            }
        })
    }

    async fn wait_for_runtime_activation(
        activation: &mut tokio::sync::watch::Receiver<bool>,
    ) -> bool {
        if *activation.borrow() {
            return true;
        }
        activation.changed().await.is_ok() && *activation.borrow()
    }

    pub(super) async fn dispatch_server_notification(
        &self,
        expected: ExpectedPublication,
        notification: JsonRpcNotification,
    ) {
        let method = notification.method.as_str();
        if TOOLS_LIST_CHANGED_METHODS.contains(&method) {
            if let Err(error) = self.refresh_tools_for_expected(expected).await {
                warn!("Failed to refresh MCP tools after notification: {}", error);
            }
        } else {
            tracing::trace!("MCP notification '{}' drained (no dispatcher)", method);
        }
    }
}
