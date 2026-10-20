//! Application adapter around the official SDK. MCP dispatch, negotiation,
//! subscriptions, cancellation and transports belong exclusively to `rmcp`.
use super::models::{JsonRpcNotification, McpInitializeResult};
use crate::error::{McpError, Result};
use crate::manager::log_privacy::diagnostic_id;
use crate::types::{McpCallResult, McpStructuredContent, McpTool};
use futures::future::BoxFuture;
use rmcp::model::{
    ClientCapabilities, ClientConfig, ClientResult, PaginatedRequestParams, ProtocolVersion,
    ServerNotification, ServerRequest, SubscriptionFilter,
};
use rmcp::service::{
    ClientCacheConfig, NotificationContext, Peer, PeerRequestOptions, RequestContext,
    RunningService, Service, ServiceError,
};
use rmcp::transport::{IntoTransport, Transport};
use rmcp::{ClientLifecycleMode, ClientServiceExt, RoleClient};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, RwLock};

const NOTIFICATION_CHANNEL_CAPACITY: usize = 100;
type ClientService = RunningService<RoleClient, BambooClientHandler>;
type StartClient =
    Box<dyn FnOnce(BambooClientHandler) -> BoxFuture<'static, Result<ClientService>> + Send>;

pub struct BambooClientHandler {
    notifications: mpsc::Sender<JsonRpcNotification>,
}

impl BambooClientHandler {
    fn notify(&self, notification: ServerNotification) {
        match serde_json::to_value(notification)
            .and_then(serde_json::from_value::<JsonRpcNotification>)
        {
            Ok(notification) => {
                // A full UI queue must never block SDK response dispatch.
                let _ = self.notifications.try_send(notification);
            }
            Err(error) => tracing::warn!(
                phase = "notification_projection",
                error_kind = "serialization",
                error_line = error.line(),
                error_column = error.column(),
                "Unable to project MCP notification"
            ),
        }
    }
}

impl Service<RoleClient> for BambooClientHandler {
    async fn handle_request(
        &self,
        request: ServerRequest,
        _context: RequestContext<RoleClient>,
    ) -> std::result::Result<ClientResult, rmcp::ErrorData> {
        if matches!(request, ServerRequest::PingRequest(_)) {
            return Ok(ClientResult::empty(()));
        }
        Err(rmcp::ErrorData::new(
            rmcp::model::ErrorCode::METHOD_NOT_FOUND,
            "Bamboo does not advertise sampling, roots or elicitation",
            None,
        ))
    }
    async fn handle_notification(
        &self,
        notification: ServerNotification,
        _context: NotificationContext<RoleClient>,
    ) -> std::result::Result<(), rmcp::ErrorData> {
        self.notify(notification);
        Ok(())
    }
    fn get_info(&self) -> ClientConfig {
        ClientConfig::new(
            ClientCapabilities::default(),
            rmcp::model::Implementation::new("bamboo", env!("CARGO_PKG_VERSION")),
        )
    }
}

/// Bamboo owns catalog/output validation and deadlines; `rmcp` owns the protocol.
pub struct McpProtocolClient {
    start: Mutex<Option<StartClient>>,
    service: Mutex<Option<ClientService>>,
    notification_tx: mpsc::Sender<JsonRpcNotification>,
    notification_rx: Mutex<Option<mpsc::Receiver<JsonRpcNotification>>>,
    subscription: Mutex<Option<tokio::task::JoinHandle<()>>>,
    output_validators: RwLock<HashMap<String, Arc<jsonschema::Validator>>>,
}

impl McpProtocolClient {
    pub fn new<T, E, A>(transport: T) -> Self
    where
        T: IntoTransport<RoleClient, E, A> + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::from_transport(transport.into_transport())
    }
    fn from_transport<T>(transport: T) -> Self
    where
        T: Transport<RoleClient> + 'static,
        T::Error: Send + Sync,
    {
        let (notification_tx, notification_rx) = mpsc::channel(NOTIFICATION_CHANNEL_CAPACITY);
        let start: StartClient = Box::new(move |handler| {
            Box::pin(async move {
                handler
                    .serve_with_lifecycle(
                        transport,
                        ClientLifecycleMode::Auto {
                            preferred_versions: vec![ProtocolVersion::LATEST],
                            legacy_version: Some(ProtocolVersion::LATEST_WITH_INITIALIZE),
                        },
                    )
                    .await
                    .map_err(|error| McpError::Connection(error.to_string()))
            })
        });
        Self {
            start: Mutex::new(Some(start)),
            service: Mutex::new(None),
            notification_tx,
            notification_rx: Mutex::new(Some(notification_rx)),
            subscription: Mutex::new(None),
            output_validators: RwLock::new(HashMap::new()),
        }
    }
    pub async fn initialize(&self, timeout_ms: u64) -> Result<McpInitializeResult> {
        let mut service = self.service.lock().await;
        if let Some(service) = service.as_ref() {
            return service
                .peer()
                .peer_info()
                .map(|info| (*info).clone())
                .ok_or_else(|| {
                    McpError::Protocol("SDK handshake has no server information".into())
                });
        }
        let start = self
            .start
            .lock()
            .await
            .take()
            .ok_or(McpError::Disconnected)?;
        let running = tokio::time::timeout(
            Duration::from_millis(timeout_ms),
            start(BambooClientHandler {
                notifications: self.notification_tx.clone(),
            }),
        )
        .await
        .map_err(|_| McpError::Timeout("MCP initialization timed out".into()))??;
        let info = running
            .peer()
            .peer_info()
            .ok_or_else(|| McpError::Protocol("SDK handshake has no server information".into()))?;
        // Refresh and health probes must reach the server and expose failures;
        // SDK stale-on-error cache hits cannot prove runtime authority.
        running
            .peer()
            .set_response_cache_config(ClientCacheConfig::disabled())
            .await;
        let peer = running.peer().clone();
        *service = Some(running);
        drop(service);
        self.ensure_subscription(&peer, timeout_ms).await?;
        Ok((*info).clone())
    }
    async fn ensure_subscription(&self, peer: &Peer<RoleClient>, timeout_ms: u64) -> Result<()> {
        let info = peer.peer_info().ok_or(McpError::Disconnected)?;
        let mut subscription_task = self.subscription.lock().await;
        if subscription_task
            .as_ref()
            .is_some_and(|task| !task.is_finished())
        {
            return Ok(());
        }
        if info.protocol_version >= ProtocolVersion::V_2026_07_28
            && info
                .capabilities
                .tools
                .as_ref()
                .is_some_and(|tools| tools.list_changed == Some(true))
        {
            let subscription = bounded(
                timeout_ms,
                peer.listen(SubscriptionFilter::builder().tools_list_changed().build()),
            )
            .await?;
            let handler = BambooClientHandler {
                notifications: self.notification_tx.clone(),
            };
            let task = tokio::spawn(async move {
                let mut subscription = subscription;
                while let Ok(Some(notification)) = subscription.next().await {
                    handler.notify(notification);
                }
            });
            *subscription_task = Some(task);
        }
        Ok(())
    }
    async fn peer(&self) -> Result<Peer<RoleClient>> {
        self.service
            .lock()
            .await
            .as_ref()
            .map(|service| service.peer().clone())
            .filter(|peer| !peer.is_transport_closed())
            .ok_or(McpError::Disconnected)
    }
    pub async fn disconnect(&mut self) -> Result<()> {
        if let Some(task) = self.subscription.get_mut().take() {
            task.abort();
        }
        self.start.get_mut().take();
        if let Some(mut service) = self.service.get_mut().take() {
            service
                .close_with_timeout(Duration::from_secs(5))
                .await
                .map_err(|error| McpError::Connection(error.to_string()))?;
        }
        Ok(())
    }
    pub async fn list_tools(&self, timeout_ms: u64) -> Result<Vec<McpTool>> {
        let peer = self.peer().await?;
        let mut cursor = None;
        let mut seen = HashSet::new();
        let mut discovered = Vec::new();
        loop {
            let response = request(
                &peer,
                rmcp::model::ClientRequest::ListToolsRequest(
                    rmcp::model::ListToolsRequest::with_param(
                        PaginatedRequestParams::default().with_cursor(cursor),
                    ),
                ),
                timeout_ms,
            )
            .await?;
            let rmcp::model::ServerResult::ListToolsResult(page) = response else {
                return Err(McpError::Protocol(
                    "SDK returned an unexpected tools/list result".into(),
                ));
            };
            discovered.extend(page.tools);
            match page.next_cursor {
                Some(next) if seen.insert(next.clone()) => cursor = Some(next),
                Some(_) => {
                    return Err(McpError::Protocol(
                        "MCP tools/list repeated a cursor".into(),
                    ))
                }
                None => break,
            }
        }
        let mut validators = HashMap::new();
        let mut tools = Vec::with_capacity(discovered.len());
        for tool in discovered {
            let output_schema = tool
                .output_schema
                .as_ref()
                .map(|schema| Value::Object((**schema).clone()));
            if let Some(schema) = &output_schema {
                match jsonschema::validator_for(schema) {
                    Ok(validator) => {
                        validators.insert(tool.name.to_string(), Arc::new(validator));
                    }
                    Err(_) => {
                        tracing::warn!(
                            tool_id = %diagnostic_id("tool", &[tool.name.as_ref()]),
                            phase = "tools_list", error_kind = "invalid_output_schema",
                            "Ignoring MCP tool with invalid output schema"
                        );
                        continue;
                    }
                }
            }
            let parameters = Value::Object((*tool.input_schema).clone());
            if peer
                .peer_info()
                .is_some_and(|info| info.protocol_version >= ProtocolVersion::V_2026_07_28)
                && parameters.get("type").and_then(Value::as_str) != Some("object")
            {
                tracing::warn!("Ignoring MCP tool whose input schema is not an object schema");
                continue;
            }
            tools.push(McpTool {
                name: tool.name.into_owned(),
                description: tool
                    .description
                    .map(|description| description.into_owned())
                    .unwrap_or_default(),
                parameters,
                output_schema,
            });
        }
        *self.output_validators.write().await = validators;
        Ok(tools)
    }
    pub async fn call_tool(
        &self,
        name: &str,
        arguments: Value,
        timeout_ms: u64,
    ) -> Result<McpCallResult> {
        let arguments = arguments
            .as_object()
            .cloned()
            .ok_or_else(|| McpError::Protocol("MCP tool arguments must be a JSON object".into()))?;
        let peer = self.peer().await?;
        let response = request(
            &peer,
            rmcp::model::ClientRequest::CallToolRequest(rmcp::model::CallToolRequest::new(
                rmcp::model::CallToolRequestParams::new(name.to_owned()).with_arguments(arguments),
            )),
            timeout_ms,
        )
        .await?;
        let rmcp::model::ServerResult::CallToolResult(result) = response else {
            return Err(McpError::Protocol("SDK returned an unsupported tools/call result; Bamboo advertises no client input capabilities".into()));
        };
        let is_error = result.is_error.unwrap_or(false);
        if let Some(validator) = self.output_validators.read().await.get(name).cloned() {
            match &result.structured_content {
                Some(value) => validator.validate(value).map_err(|error| {
                    McpError::Protocol(format!(
                        "MCP structuredContent does not conform to outputSchema: {error}"
                    ))
                })?,
                None if !is_error => {
                    return Err(McpError::Protocol(
                        "MCP tool declared outputSchema but omitted structuredContent".into(),
                    ))
                }
                None => {}
            }
        }
        Ok(McpCallResult {
            content: serde_json::from_value(serde_json::to_value(result.content)?)?,
            is_error,
            structured_content: result
                .structured_content
                .map(|value| {
                    if value.is_null() {
                        McpStructuredContent::Null
                    } else {
                        McpStructuredContent::Value(value)
                    }
                })
                .unwrap_or(McpStructuredContent::Missing),
        })
    }
    pub async fn ping(&self, timeout_ms: u64) -> Result<()> {
        let peer = self.peer().await?;
        if peer
            .peer_info()
            .is_some_and(|info| info.protocol_version >= ProtocolVersion::V_2026_07_28)
        {
            let response = request(
                &peer,
                rmcp::model::ClientRequest::DiscoverRequest(rmcp::model::DiscoverRequest::new(
                    rmcp::model::DiscoverRequestParams::default(),
                )),
                timeout_ms,
            )
            .await?;
            if !matches!(response, rmcp::model::ServerResult::DiscoverResult(_)) {
                return Err(McpError::Protocol(
                    "SDK returned an unexpected discovery result".into(),
                ));
            }
            self.ensure_subscription(&peer, timeout_ms).await?;
        } else {
            let response = request(
                &peer,
                rmcp::model::ClientRequest::PingRequest(rmcp::model::PingRequest::default()),
                timeout_ms,
            )
            .await?;
            if !matches!(response, rmcp::model::ServerResult::EmptyResult(_)) {
                return Err(McpError::Protocol(
                    "SDK returned an unexpected ping result".into(),
                ));
            }
        }
        Ok(())
    }
    pub async fn try_receive_notification(&self) -> Option<JsonRpcNotification> {
        self.notification_rx.lock().await.as_mut()?.try_recv().ok()
    }
    pub async fn take_notification_receiver(&self) -> Option<mpsc::Receiver<JsonRpcNotification>> {
        self.notification_rx.lock().await.take()
    }
    pub async fn is_connected(&self) -> bool {
        self.peer().await.is_ok()
    }
}
impl Drop for McpProtocolClient {
    fn drop(&mut self) {
        if let Some(task) = self.subscription.get_mut().take() {
            task.abort();
        }
    }
}
async fn bounded<T>(
    timeout_ms: u64,
    future: impl std::future::Future<Output = std::result::Result<T, ServiceError>>,
) -> Result<T> {
    // The SDK subscription guard sends cancellation if establishment is abandoned.
    tokio::time::timeout(Duration::from_millis(timeout_ms), future)
        .await
        .map_err(|_| McpError::Timeout("MCP request timed out".into()))?
        .map_err(sdk_error)
}
fn sdk_error(error: ServiceError) -> McpError {
    match error {
        ServiceError::McpError(error) => McpError::RemoteProtocol {
            code: error.code.0,
            message: error.message.into_owned(),
            data: error.data,
        },
        ServiceError::TransportClosed => McpError::Disconnected,
        ServiceError::Timeout { .. } => McpError::Timeout(error.to_string()),
        error => McpError::Protocol(error.to_string()),
    }
}

// rmcp owns deadline cancellation. This guard also cancels when Bamboo's caller
// drops the operation (runtime retirement/abort) before its deadline.
struct CancelOnDrop {
    peer: Peer<RoleClient>,
    id: Option<rmcp::model::RequestId>,
}
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let (Some(id), Ok(runtime)) = (self.id.take(), tokio::runtime::Handle::try_current()) {
            let peer = self.peer.clone();
            runtime.spawn(async move {
                let _ = tokio::time::timeout(
                    Duration::from_millis(250),
                    peer.notify_cancelled(rmcp::model::CancelledNotificationParam::new(
                        Some(id),
                        Some("Bamboo caller cancelled".into()),
                    )),
                )
                .await;
            });
        }
    }
}
async fn request(
    peer: &Peer<RoleClient>,
    request: rmcp::model::ClientRequest,
    timeout_ms: u64,
) -> Result<rmcp::model::ServerResult> {
    let handle = peer
        .send_cancellable_request(
            request,
            PeerRequestOptions::with_timeout(Duration::from_millis(timeout_ms)),
        )
        .await
        .map_err(sdk_error)?;
    let mut guard = CancelOnDrop {
        peer: peer.clone(),
        id: Some(handle.id.clone()),
    };
    let result = handle.await_response().await.map_err(sdk_error);
    guard.id = None;
    result
}
