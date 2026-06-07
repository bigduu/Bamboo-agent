use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use std::str::FromStr as _;
use std::time::Duration;

use super::error::{map_jsonrpc_error, A2AClientError, A2AClientResult};
use super::jsonrpc::{methods, JsonRpcId, JsonRpcRequest, JsonRpcResponse};
use super::sse::stream_response_from_sse;
use super::sse::A2AStream;
use super::types::{
    AgentCard, CancelTaskRequest, GetTaskRequest, SendMessageRequest, SendMessageResponse, Task,
};

#[derive(Debug, Clone)]
pub struct A2AClientConfig {
    /// Stable Bamboo-side profile id, e.g. "remote-impl".
    pub profile_id: String,
    /// Agent Card discovery URL.
    pub agent_card_url: String,
    /// Optional RPC URL override. If absent, pick first JSONRPC interface from Agent Card.
    pub rpc_url_override: Option<String>,
    /// Authentication material.
    pub auth: A2AAuth,
    /// Optional tenant header/path param.
    pub tenant: Option<String>,
    /// Request timeout for non-streaming calls.
    pub request_timeout: Duration,
    /// Optional A2A-Version header. For v1.0 use "1.0".
    pub protocol_version: String,
    /// Optional required extensions to advertise via A2A-Extensions header.
    pub extensions: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum A2AAuth {
    None,
    Bearer(String),
    ApiKeyHeader { header: String, value: String },
}

#[async_trait]
pub trait A2AClient: Send + Sync {
    async fn fetch_agent_card(&self) -> A2AClientResult<AgentCard>;
    async fn send_message(
        &self,
        request: SendMessageRequest,
    ) -> A2AClientResult<SendMessageResponse>;
    async fn send_streaming_message(
        &self,
        request: SendMessageRequest,
    ) -> A2AClientResult<A2AStream>;
    async fn get_task(&self, request: GetTaskRequest) -> A2AClientResult<Task>;
    async fn cancel_task(&self, request: CancelTaskRequest) -> A2AClientResult<Task>;
}

pub struct A2AJsonRpcClient {
    http: reqwest::Client,
    config: A2AClientConfig,
    resolved_rpc_url: tokio::sync::RwLock<Option<String>>,
}

impl A2AJsonRpcClient {
    pub fn new(config: A2AClientConfig) -> A2AClientResult<Self> {
        let http = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .build()
            .map_err(A2AClientError::Http)?;
        Ok(Self {
            http,
            config,
            resolved_rpc_url: tokio::sync::RwLock::new(None),
        })
    }

    pub fn new_with_http_client(http: reqwest::Client, config: A2AClientConfig) -> Self {
        Self {
            http,
            config,
            resolved_rpc_url: tokio::sync::RwLock::new(None),
        }
    }

    fn build_headers(&self, accept_streaming: bool) -> A2AClientResult<HeaderMap> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let accept = if accept_streaming {
            "text/event-stream"
        } else {
            "application/json"
        };
        headers.insert(
            "Accept",
            HeaderValue::from_str(accept).map_err(|e| {
                A2AClientError::InvalidStreamResponse(format!("Invalid accept header: {}", e))
            })?,
        );

        match &self.config.auth {
            A2AAuth::None => {}
            A2AAuth::Bearer(token) => {
                let value = format!("Bearer {}", token);
                headers.insert(
                    AUTHORIZATION,
                    HeaderValue::from_str(&value).map_err(|e| {
                        A2AClientError::InvalidStreamResponse(format!(
                            "Invalid authorization header: {}",
                            e
                        ))
                    })?,
                );
            }
            A2AAuth::ApiKeyHeader { header, value } => {
                let name = reqwest::header::HeaderName::from_str(header).map_err(|e| {
                    A2AClientError::InvalidStreamResponse(format!(
                        "Invalid API key header name: {}",
                        e
                    ))
                })?;
                headers.insert(
                    name,
                    HeaderValue::from_str(value).map_err(|e| {
                        A2AClientError::InvalidStreamResponse(format!(
                            "Invalid API key header value: {}",
                            e
                        ))
                    })?,
                );
            }
        }

        headers.insert(
            "A2A-Version",
            HeaderValue::from_str(&self.config.protocol_version).map_err(|e| {
                A2AClientError::InvalidStreamResponse(format!("Invalid A2A-Version header: {}", e))
            })?,
        );

        if !self.config.extensions.is_empty() {
            let extensions = self.config.extensions.join(",");
            headers.insert(
                "A2A-Extensions",
                HeaderValue::from_str(&extensions).map_err(|e| {
                    A2AClientError::InvalidStreamResponse(format!(
                        "Invalid A2A-Extensions header: {}",
                        e
                    ))
                })?,
            );
        }

        Ok(headers)
    }

    async fn resolve_rpc_url(&self) -> A2AClientResult<String> {
        // Check cache first
        {
            let cache = self.resolved_rpc_url.read().await;
            if let Some(url) = cache.as_ref() {
                return Ok(url.clone());
            }
        }

        // Use override if configured
        if let Some(override_url) = &self.config.rpc_url_override {
            let mut cache = self.resolved_rpc_url.write().await;
            cache.replace(override_url.clone());
            return Ok(override_url.clone());
        }

        // Fetch Agent Card and find JSONRPC interface
        let card = self.fetch_agent_card().await?;
        let jsonrpc_interface = card
            .supported_interfaces
            .into_iter()
            .find(|iface| iface.protocol_binding.eq_ignore_ascii_case("JSONRPC"))
            .ok_or_else(|| {
                A2AClientError::InvalidAgentCard("Agent Card has no JSONRPC interface".to_string())
            })?;

        let major = jsonrpc_interface
            .protocol_version
            .split('.')
            .next()
            .and_then(|s| s.parse::<u32>().ok())
            .ok_or_else(|| {
                A2AClientError::InvalidAgentCard(format!(
                    "Invalid protocol version: {}",
                    jsonrpc_interface.protocol_version
                ))
            })?;
        if major != 1 {
            return Err(A2AClientError::VersionNotSupported(format!(
                "Protocol major version {} != 1",
                major
            )));
        }

        let mut cache = self.resolved_rpc_url.write().await;
        cache.replace(jsonrpc_interface.url.clone());
        Ok(jsonrpc_interface.url)
    }

    fn make_request_id(&self) -> JsonRpcId {
        JsonRpcId::String(uuid::Uuid::new_v4().to_string())
    }

    async fn do_jsonrpc_call<Req, Resp>(
        &self,
        method: &'static str,
        params: Req,
    ) -> A2AClientResult<Resp>
    where
        Req: serde::Serialize + Send,
        Resp: serde::de::DeserializeOwned,
    {
        let url = self.resolve_rpc_url().await?;
        let headers = self.build_headers(false)?;
        let request = JsonRpcRequest {
            jsonrpc: super::jsonrpc::JSONRPC_VERSION,
            id: self.make_request_id(),
            method,
            params: Some(params),
        };

        let body = serde_json::to_string(&request).map_err(A2AClientError::Json)?;
        let response = self
            .http
            .post(&url)
            .headers(headers)
            .body(body)
            .send()
            .await
            .map_err(A2AClientError::Http)?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(A2AClientError::Sse(format!(
                "HTTP error {}: {}",
                status, text
            )));
        }

        let body = response.bytes().await.map_err(A2AClientError::Http)?;
        let envelope: JsonRpcResponse<Resp> =
            serde_json::from_slice(&body).map_err(A2AClientError::Json)?;

        if let Some(err) = envelope.error {
            return Err(map_jsonrpc_error(err, None));
        }

        envelope.result.ok_or_else(|| {
            A2AClientError::InvalidStreamResponse(
                "missing result and error in JSON-RPC response".to_string(),
            )
        })
    }
}

#[async_trait]
impl A2AClient for A2AJsonRpcClient {
    async fn fetch_agent_card(&self) -> A2AClientResult<AgentCard> {
        let response = self
            .http
            .get(&self.config.agent_card_url)
            .send()
            .await
            .map_err(A2AClientError::Http)?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(A2AClientError::Sse(format!(
                "HTTP error {} fetching agent card: {}",
                status, text
            )));
        }

        response.json().await.map_err(A2AClientError::Http)
    }

    async fn send_message(
        &self,
        request: SendMessageRequest,
    ) -> A2AClientResult<SendMessageResponse> {
        self.do_jsonrpc_call(methods::SEND_MESSAGE, request).await
    }

    async fn send_streaming_message(
        &self,
        request: SendMessageRequest,
    ) -> A2AClientResult<A2AStream> {
        let url = self.resolve_rpc_url().await?;
        let headers = self.build_headers(true)?;
        let jsonrpc_request = JsonRpcRequest {
            jsonrpc: super::jsonrpc::JSONRPC_VERSION,
            id: self.make_request_id(),
            method: methods::SEND_STREAMING_MESSAGE,
            params: Some(request),
        };

        let body = serde_json::to_string(&jsonrpc_request).map_err(A2AClientError::Json)?;
        let response = self
            .http
            .post(&url)
            .headers(headers)
            .body(body)
            .send()
            .await
            .map_err(A2AClientError::Http)?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(A2AClientError::Sse(format!(
                "HTTP error {}: {}",
                status, text
            )));
        }

        Ok(stream_response_from_sse(response))
    }

    async fn get_task(&self, request: GetTaskRequest) -> A2AClientResult<Task> {
        self.do_jsonrpc_call(methods::GET_TASK, request).await
    }

    async fn cancel_task(&self, request: CancelTaskRequest) -> A2AClientResult<Task> {
        self.do_jsonrpc_call(methods::CANCEL_TASK, request).await
    }
}
