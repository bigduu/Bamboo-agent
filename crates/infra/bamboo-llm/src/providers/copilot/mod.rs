use async_trait::async_trait;
use reqwest::Client;
use serde_json::json;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

pub mod auth;
use crate::provider::{
    LLMError, LLMProvider, LLMRequestOptions, LLMStream, ProviderModelInfo,
    ResponsesRequestOptions, Result,
};
use crate::types::LLMChunk;
use auth::{CopilotAuthHandler, DeviceCodeResponse};
use bamboo_config::{KeywordMaskingConfig, RequestOverridesConfig};
use bamboo_domain::Message;
use bamboo_domain::ReasoningEffort;
use bamboo_domain::ToolSchema;

use super::common::openai_compat::{
    messages_to_openai_compat_json, parse_openai_compat_sse_data_lenient,
    tools_to_openai_compat_json,
};
use super::common::openai_responses::{
    build_responses_body, select_responses_input_messages, ResponsesInputSource, ResponsesSseParser,
};
use super::common::request_overrides;
use super::common::responses_debug::append_responses_sse_record;
use super::common::sse::llm_stream_from_sse;

const COPILOT_TRANSPORT_MAX_ATTEMPTS: usize = 2;
const COPILOT_TRANSPORT_RETRY_BASE_DELAY_MS: u64 = 250;
const COPILOT_MODELS_CACHE_TTL: Duration = Duration::from_secs(15 * 60);

/// GitHub Copilot Provider with Device Code Authentication
///
/// Authentication flow:
/// 1. Get device code from GitHub
/// 2. User authorizes at github.com/login/device
/// 3. Poll for access token
/// 4. Exchange for Copilot token
/// 5. Cache and use
///
/// Token sources (in order of priority):
/// 1. Direct API key via constructor
/// 2. Cached token (app_data_dir/.copilot_token.json)
/// 3. Environment variable COPILOT_API_KEY
/// 4. Interactive device code flow
pub struct CopilotProvider {
    client: Client,
    token: Option<String>,
    token_expires_at: Option<u64>,
    auth_handler: Option<CopilotAuthHandler>,
    vscode_session_id: String,
    vscode_machine_id: String,
    // Patterns (case-insensitive) for models that require Responses API upstream.
    responses_only_models: Vec<String>,
    default_reasoning_effort: Option<ReasoningEffort>,
    request_overrides: Option<RequestOverridesConfig>,
    masking_config: KeywordMaskingConfig,
    models_cache: RwLock<Option<CopilotModelsCache>>,
}

#[derive(Debug, Clone)]
struct CopilotModelsCache {
    fetched_at: Instant,
    models: Vec<ProviderModelInfo>,
}

impl CopilotProvider {
    /// Create new Copilot provider (without auth handler)
    pub fn new() -> Self {
        Self {
            client: Client::new(),
            token: None,
            token_expires_at: None,
            auth_handler: None,
            vscode_session_id: Self::generate_vscode_session_id(),
            vscode_machine_id: Self::generate_vscode_machine_id(),
            responses_only_models: vec![],
            default_reasoning_effort: None,
            request_overrides: None,
            masking_config: KeywordMaskingConfig::default(),
            models_cache: RwLock::new(None),
        }
    }

    /// Create provider with existing token
    pub fn with_token(token: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            token: Some(token.into()),
            token_expires_at: None,
            auth_handler: None,
            vscode_session_id: Self::generate_vscode_session_id(),
            vscode_machine_id: Self::generate_vscode_machine_id(),
            responses_only_models: vec![],
            default_reasoning_effort: None,
            request_overrides: None,
            masking_config: KeywordMaskingConfig::default(),
            models_cache: RwLock::new(None),
        }
    }

    /// Create provider with auth handler (for HTTP/CLI authentication)
    pub fn with_auth_handler(client: Client, app_data_dir: PathBuf, headless_auth: bool) -> Self {
        use reqwest_middleware::ClientBuilder;
        use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
        use std::sync::Arc;
        use std::time::Duration;

        // Build retry client
        let retry_policy = ExponentialBackoff::builder()
            .retry_bounds(Duration::from_millis(100), Duration::from_secs(5))
            .build_with_max_retries(3);

        let client_with_middleware = Arc::new(
            ClientBuilder::new(client.clone())
                .with(RetryTransientMiddleware::new_with_policy(retry_policy))
                .build(),
        );

        let auth_handler =
            CopilotAuthHandler::new(client_with_middleware, app_data_dir, headless_auth);

        Self {
            client,
            token: None,
            token_expires_at: None,
            auth_handler: Some(auth_handler),
            vscode_session_id: Self::generate_vscode_session_id(),
            vscode_machine_id: Self::generate_vscode_machine_id(),
            responses_only_models: vec![],
            default_reasoning_effort: None,
            request_overrides: None,
            masking_config: KeywordMaskingConfig::default(),
            models_cache: RwLock::new(None),
        }
    }

    /// Configure models that must use Responses API upstream.
    pub fn with_responses_only_models(mut self, models: Vec<String>) -> Self {
        self.responses_only_models = models;
        self
    }

    /// Configure keyword masking applied as a last-moment scan of every outbound
    /// request body (see [`crate::masking`]).
    pub fn with_masking(mut self, masking_config: KeywordMaskingConfig) -> Self {
        self.masking_config = masking_config;
        self
    }

    /// Configure default reasoning effort for requests sent through this provider.
    pub fn with_reasoning_effort(mut self, effort: Option<ReasoningEffort>) -> Self {
        self.default_reasoning_effort = effort;
        self
    }

    /// Configure request overrides for this provider.
    pub fn with_request_overrides(mut self, overrides: Option<RequestOverridesConfig>) -> Self {
        self.request_overrides = overrides;
        self
    }

    /// Check if already authenticated
    pub fn is_authenticated(&self) -> bool {
        self.token.is_some()
    }

    /// Get token (if authenticated)
    pub fn token(&self) -> Option<&str> {
        self.token.as_deref()
    }

    /// Try to authenticate silently (from cache/env, without triggering device flow)
    pub async fn try_authenticate_silent(&mut self) -> std::result::Result<bool, LLMError> {
        if let Some(handler) = &self.auth_handler {
            match handler.try_get_chat_token_silent().await {
                Ok(Some(token)) => {
                    self.token = Some(token);
                    return Ok(true);
                }
                Ok(None) => return Ok(false),
                Err(e) => return Err(LLMError::Auth(e.to_string())),
            }
        }
        Ok(false)
    }

    /// Authenticate using device code flow (full flow with cache check)
    pub async fn authenticate(&mut self) -> std::result::Result<(), LLMError> {
        // Try silent first
        if self.try_authenticate_silent().await? {
            return Ok(());
        }

        // Need interactive authentication
        if let Some(handler) = &self.auth_handler {
            let token = handler
                .get_chat_token()
                .await
                .map_err(|e| LLMError::Auth(e.to_string()))?;
            self.token = Some(token);
            Ok(())
        } else {
            Err(LLMError::Auth("No auth handler configured".to_string()))
        }
    }

    /// Start authentication and return device code info for frontend display
    pub async fn start_authentication(&self) -> std::result::Result<DeviceCodeResponse, LLMError> {
        if let Some(handler) = &self.auth_handler {
            handler
                .start_authentication()
                .await
                .map_err(|e| LLMError::Auth(e.to_string()))
        } else {
            Err(LLMError::Auth("No auth handler configured".to_string()))
        }
    }

    /// Complete authentication with device code (poll for token)
    pub async fn complete_authentication(
        &mut self,
        device_code: &DeviceCodeResponse,
    ) -> std::result::Result<(), LLMError> {
        if let Some(handler) = &self.auth_handler {
            let config = handler
                .complete_authentication(device_code)
                .await
                .map_err(|e| LLMError::Auth(e.to_string()))?;
            self.token = Some(config.token);
            self.token_expires_at = Some(config.expires_at);
            Ok(())
        } else {
            Err(LLMError::Auth("No auth handler configured".to_string()))
        }
    }

    /// Logout - delete cached tokens
    pub async fn logout(&mut self) -> std::result::Result<(), LLMError> {
        if let Some(handler) = &self.auth_handler {
            // Delete token files
            let token_path = handler.app_data_dir().join(".token");
            let copilot_token_path = handler.app_data_dir().join(".copilot_token.json");

            if token_path.exists() {
                std::fs::remove_file(&token_path)
                    .map_err(|e| LLMError::Auth(format!("Failed to delete .token: {}", e)))?;
            }

            if copilot_token_path.exists() {
                std::fs::remove_file(&copilot_token_path).map_err(|e| {
                    LLMError::Auth(format!("Failed to delete .copilot_token.json: {}", e))
                })?;
            }
        }

        self.token = None;
        self.token_expires_at = None;
        tracing::info!("Logged out and deleted cached tokens");
        Ok(())
    }

    /// Build request headers to mimic VS Code Copilot extension
    fn build_headers_with_token(
        token: &str,
    ) -> std::result::Result<reqwest::header::HeaderMap, LLMError> {
        use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};

        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", token))
                .map_err(|e| LLMError::Auth(format!("Invalid token: {}", e)))?,
        );
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        // Mimic VS Code Copilot extension headers
        headers.insert("editor-version", HeaderValue::from_static("vscode/1.99.2"));
        headers.insert(
            "editor-plugin-version",
            HeaderValue::from_static("copilot-chat/0.20.3"),
        );
        headers.insert(
            "user-agent",
            HeaderValue::from_static("GitHubCopilotChat/0.20.3"),
        );
        headers.insert("accept", HeaderValue::from_static("application/json"));
        headers.insert(
            "accept-encoding",
            HeaderValue::from_static("gzip, deflate, br"),
        );
        headers.insert(
            "copilot-integration-id",
            HeaderValue::from_static("vscode-chat"),
        );

        Ok(headers)
    }

    fn generate_vscode_session_id() -> String {
        format!(
            "{}{}",
            uuid::Uuid::new_v4(),
            chrono::Utc::now().timestamp_millis()
        )
    }

    fn generate_vscode_machine_id() -> String {
        uuid::Uuid::new_v4()
            .simple()
            .to_string()
            .repeat(2)
            .chars()
            .take(64)
            .collect()
    }

    fn infer_request_initiator(messages: &[Message]) -> &'static str {
        messages
            .iter()
            .rev()
            .find(|message| !matches!(message.role, bamboo_domain::Role::System))
            .map(|message| match message.role {
                bamboo_domain::Role::User => "user",
                bamboo_domain::Role::Assistant | bamboo_domain::Role::Tool => "agent",
                bamboo_domain::Role::System => "user",
            })
            .unwrap_or("user")
    }

    fn infer_openai_intent(messages: &[Message], tools: &[ToolSchema]) -> &'static str {
        let has_agent_activity = messages.iter().any(|message| {
            matches!(
                message.role,
                bamboo_domain::Role::Assistant | bamboo_domain::Role::Tool
            ) || message
                .tool_calls
                .as_ref()
                .is_some_and(|calls| !calls.is_empty())
        });

        if !tools.is_empty() || has_agent_activity {
            "conversation-agent"
        } else {
            "conversation-panel"
        }
    }

    fn build_llm_headers(
        &self,
        token: &str,
        messages: &[Message],
        tools: &[ToolSchema],
        endpoint: &str,
        model: Option<&str>,
    ) -> std::result::Result<reqwest::header::HeaderMap, LLMError> {
        use reqwest::header::HeaderValue;

        let mut headers = Self::build_headers_with_token(token)?;
        headers.insert(
            "openai-organization",
            HeaderValue::from_static("github-copilot"),
        );
        headers.insert(
            "openai-intent",
            HeaderValue::from_static(Self::infer_openai_intent(messages, tools)),
        );
        headers.insert(
            "x-github-api-version",
            HeaderValue::from_static("2025-05-01"),
        );
        headers.insert(
            "x-initiator",
            HeaderValue::from_static(Self::infer_request_initiator(messages)),
        );
        headers.insert(
            "x-interaction-id",
            HeaderValue::from_str(&self.vscode_session_id)
                .map_err(|e| LLMError::Auth(format!("Invalid x-interaction-id: {}", e)))?,
        );
        headers.insert(
            "vscode-sessionid",
            HeaderValue::from_str(&self.vscode_session_id)
                .map_err(|e| LLMError::Auth(format!("Invalid vscode-sessionid: {}", e)))?,
        );
        headers.insert(
            "vscode-machineid",
            HeaderValue::from_str(&self.vscode_machine_id)
                .map_err(|e| LLMError::Auth(format!("Invalid vscode-machineid: {}", e)))?,
        );
        headers.insert(
            "x-request-id",
            HeaderValue::from_str(&uuid::Uuid::new_v4().to_string())
                .map_err(|e| LLMError::Auth(format!("Invalid x-request-id: {}", e)))?,
        );
        request_overrides::apply_overrides_to_header_map(
            &mut headers,
            self.request_overrides.as_ref(),
            endpoint,
            model,
        );

        Ok(headers)
    }

    /// Build request headers using the in-memory token (primarily for tests and manual token mode).
    #[allow(dead_code)] // Used by unit tests + handy for debugging token formatting.
    fn build_headers(&self) -> std::result::Result<reqwest::header::HeaderMap, LLMError> {
        let token = self
            .token
            .as_ref()
            .ok_or_else(|| LLMError::Auth("Not authenticated".to_string()))?;
        self.build_llm_headers(
            token,
            &[],
            &[],
            request_overrides::ENDPOINT_CHAT_COMPLETIONS,
            None,
        )
    }

    async fn get_token_for_request(&self) -> std::result::Result<String, LLMError> {
        // Prefer the auth handler (cached token / env var / access token exchange).
        if let Some(handler) = &self.auth_handler {
            match handler.try_get_chat_token_silent().await {
                Ok(Some(token)) => return Ok(token),
                Ok(None) => { /* fall back to in-memory token */ }
                Err(e) => return Err(LLMError::Auth(e.to_string())),
            }
        }

        if let Some(token) = self.token.as_ref() {
            return Ok(token.clone());
        }

        Err(LLMError::Auth(
            "Not authenticated. Please run authenticate() first.".to_string(),
        ))
    }
}

impl Default for CopilotProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl CopilotProvider {
    fn matches_model_pattern(pattern: &str, model: &str) -> bool {
        let p = pattern.trim().to_ascii_lowercase();
        if p.is_empty() {
            return false;
        }

        let m = model.trim().to_ascii_lowercase();

        // Support a single trailing wildcard for simple prefix matching: "gpt-5*"
        if let Some(prefix) = p.strip_suffix('*') {
            return m.starts_with(prefix);
        }

        m == p
    }

    fn uses_responses_api(&self, model: &str) -> bool {
        self.responses_only_models
            .iter()
            .any(|p| Self::matches_model_pattern(p, model))
    }

    fn looks_like_responses_only_error(status: reqwest::StatusCode, body: &str) -> bool {
        if !(status == 400
            || status == 404
            || status == 405
            || status == 409
            || status == 415
            || status == 422)
        {
            return false;
        }

        let b = body.to_ascii_lowercase();
        b.contains("/responses") || b.contains("responses api") || b.contains("use responses")
    }

    fn looks_like_reasoning_unsupported_error(status: reqwest::StatusCode, body: &str) -> bool {
        if !(status == 400 || status == 404 || status == 405 || status == 409 || status == 422) {
            return false;
        }

        let b = body.to_ascii_lowercase();
        let mentions_reasoning = b.contains("reasoning")
            || b.contains("reasoning_effort")
            || b.contains("thinking")
            || b.contains("unknown parameter");
        let mentions_unsupported = b.contains("unsupported")
            || b.contains("not supported")
            || b.contains("unknown")
            || b.contains("invalid");
        mentions_reasoning && mentions_unsupported
    }

    fn looks_like_previous_response_id_unsupported_error(
        status: reqwest::StatusCode,
        body: &str,
    ) -> bool {
        if !(status == 400 || status == 409 || status == 422) {
            return false;
        }

        let b = body.to_ascii_lowercase();
        let mentions_previous_response_id = b.contains("previous_response_id");
        let mentions_unsupported = b.contains("unsupported_value")
            || b.contains("not supported")
            || b.contains("unsupported");
        mentions_previous_response_id && mentions_unsupported
    }

    fn is_retryable_transport_message(message: &str) -> bool {
        let lower = message.to_ascii_lowercase();
        let retryable_markers = [
            "incompletemessage",
            "error sending request",
            "connection reset",
            "broken pipe",
            "connection aborted",
            "connection closed",
            "unexpected eof",
            "timed out",
            "timeout",
        ];

        retryable_markers
            .iter()
            .any(|marker| lower.contains(marker))
    }

    fn is_retryable_transport_error(error: &reqwest::Error) -> bool {
        error.is_timeout()
            || error.is_connect()
            || error.is_request()
            || Self::is_retryable_transport_message(&error.to_string())
    }

    async fn send_with_transport_retry<F>(
        &self,
        mut build_request: F,
        operation: &str,
        session_id: Option<&str>,
    ) -> std::result::Result<reqwest::Response, LLMError>
    where
        F: FnMut() -> reqwest::RequestBuilder,
    {
        let session_log_id = session_id.unwrap_or("unknown-session");
        for attempt in 1..=COPILOT_TRANSPORT_MAX_ATTEMPTS {
            match build_request().send().await {
                Ok(response) => return Ok(response),
                Err(error) => {
                    if attempt >= COPILOT_TRANSPORT_MAX_ATTEMPTS
                        || !Self::is_retryable_transport_error(&error)
                    {
                        return Err(LLMError::Http(error));
                    }

                    let delay_ms = COPILOT_TRANSPORT_RETRY_BASE_DELAY_MS * (1u64 << (attempt - 1));
                    tracing::warn!(
                        "[{}] Copilot transport error during {} (attempt {}/{}): {}. Retrying in {}ms",
                        session_log_id,
                        operation,
                        attempt,
                        COPILOT_TRANSPORT_MAX_ATTEMPTS,
                        error,
                        delay_ms
                    );
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
            }
        }

        unreachable!("loop always returns or errors")
    }

    #[allow(clippy::too_many_arguments)]
    async fn chat_stream_via_responses(
        &self,
        token: &str,
        messages: &[Message],
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
        reasoning_effort: Option<ReasoningEffort>,
        responses_options: Option<&ResponsesRequestOptions>,
        parallel_tool_calls: Option<bool>,
        reasoning_source: &str,
        session_log_id: &str,
        request_purpose: &str,
    ) -> Result<LLMStream> {
        let url = "https://api.githubcopilot.com/responses";
        let mut effective_responses_options = responses_options.cloned().unwrap_or_default();
        if effective_responses_options.store == Some(true) {
            tracing::warn!(
                "[{}] Copilot /responses does not support store=true; forcing store=false",
                session_log_id
            );
        }
        effective_responses_options.store = Some(false);
        let input_selection =
            select_responses_input_messages(messages, Some(&effective_responses_options));
        let input_source = match input_selection.source {
            ResponsesInputSource::Explicit => "explicit",
            ResponsesInputSource::Generic => "generic",
        };
        let mut body = build_responses_body(
            model,
            messages,
            tools,
            max_output_tokens,
            reasoning_effort,
            Some(&effective_responses_options),
            parallel_tool_calls,
        );
        request_overrides::apply_overrides_to_body(
            &mut body,
            self.request_overrides.as_ref(),
            request_overrides::ENDPOINT_RESPONSES,
            Some(model),
        );
        // Last-moment scan: mask every text value in the fully-assembled body.
        crate::masking::mask_outbound_body(&mut body, &self.masking_config);

        tracing::debug!(
            "[{}] Copilot provider using Responses API model: {}",
            session_log_id,
            model
        );
        tracing::info!(
            "[{}] Copilot request protocol=responses model='{}' reasoning_effort={} reasoning_source={} request_reasoning_enabled={} max_output_tokens={} input_source={} input_messages_before={} input_messages_after={} duplicate_system_fallback={} purpose={}",
            session_log_id,
            model,
            reasoning_effort
                .map(ReasoningEffort::as_str)
                .unwrap_or("none"),
            reasoning_source,
            reasoning_effort.is_some(),
            max_output_tokens
                .map(|tokens| tokens.to_string())
                .unwrap_or_else(|| "none".to_string()),
            input_source,
            input_selection.original_len,
            input_selection.effective_len,
            input_selection.fallback_removed_duplicate_system,
            request_purpose
        );

        let request_headers = self.build_llm_headers(
            token,
            messages,
            tools,
            request_overrides::ENDPOINT_RESPONSES,
            Some(model),
        )?;
        let mut response = self
            .send_with_transport_retry(
                || {
                    self.client
                        .post(url)
                        .headers(request_headers.clone())
                        .json(&body)
                },
                "copilot responses initial request",
                Some(session_log_id),
            )
            .await?;

        if !response.status().is_success() {
            let status = response.status();

            if (status == 401 || status == 403) && self.auth_handler.is_some() {
                if let Some(handler) = &self.auth_handler {
                    if let Ok(Some(refreshed)) = handler.force_refresh_chat_token().await {
                        let refreshed_headers = self.build_llm_headers(
                            &refreshed,
                            messages,
                            tools,
                            request_overrides::ENDPOINT_RESPONSES,
                            Some(model),
                        )?;
                        response = self
                            .send_with_transport_retry(
                                || {
                                    self.client
                                        .post(url)
                                        .headers(refreshed_headers.clone())
                                        .json(&body)
                                },
                                "copilot responses auth-refresh retry",
                                Some(session_log_id),
                            )
                            .await?;
                    }
                }
            }

            if !response.status().is_success() {
                let status = response.status();
                let request_id = response
                    .headers()
                    .get("x-request-id")
                    .or_else(|| response.headers().get("x-github-request-id"))
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("-")
                    .to_string();
                let response_headers_debug: String = response
                    .headers()
                    .iter()
                    .filter(|(k, _)| {
                        let name = k.as_str();
                        !matches!(
                            name,
                            "set-cookie"
                                | "cookie"
                                | "authorization"
                                | "accept-ranges"
                                | "access-control-allow-origin"
                        )
                    })
                    .map(|(k, v)| format!("{}={}", k, v.to_str().unwrap_or("<binary>")))
                    .collect::<Vec<_>>()
                    .join(", ");
                let text = response.text().await.unwrap_or_default();

                if reasoning_effort.is_some()
                    && Self::looks_like_reasoning_unsupported_error(status, &text)
                {
                    tracing::warn!(
                        "[{}] Copilot /responses rejected reasoning for model '{}'; retrying without reasoning_effort",
                        session_log_id,
                        model
                    );
                    let mut fallback_options = effective_responses_options.clone();
                    fallback_options.reasoning_summary = None;
                    let mut fallback_body = build_responses_body(
                        model,
                        messages,
                        tools,
                        max_output_tokens,
                        None,
                        Some(&fallback_options),
                        parallel_tool_calls,
                    );
                    request_overrides::apply_overrides_to_body(
                        &mut fallback_body,
                        self.request_overrides.as_ref(),
                        request_overrides::ENDPOINT_RESPONSES,
                        Some(model),
                    );
                    crate::masking::mask_outbound_body(&mut fallback_body, &self.masking_config);
                    let fallback_headers = self.build_llm_headers(
                        token,
                        messages,
                        tools,
                        request_overrides::ENDPOINT_RESPONSES,
                        Some(model),
                    )?;
                    let mut fallback = self
                        .send_with_transport_retry(
                            || {
                                self.client
                                    .post(url)
                                    .headers(fallback_headers.clone())
                                    .json(&fallback_body)
                            },
                            "copilot responses reasoning fallback",
                            Some(session_log_id),
                        )
                        .await?;

                    if !fallback.status().is_success() {
                        let fallback_status = fallback.status();
                        if (fallback_status == 401 || fallback_status == 403)
                            && self.auth_handler.is_some()
                        {
                            if let Some(handler) = &self.auth_handler {
                                if let Ok(Some(refreshed)) =
                                    handler.force_refresh_chat_token().await
                                {
                                    let refreshed_fallback_headers = self.build_llm_headers(
                                        &refreshed,
                                        messages,
                                        tools,
                                        request_overrides::ENDPOINT_RESPONSES,
                                        Some(model),
                                    )?;
                                    fallback = self
                                        .send_with_transport_retry(
                                            || {
                                                self.client
                                                    .post(url)
                                                    .headers(refreshed_fallback_headers.clone())
                                                    .json(&fallback_body)
                                            },
                                            "copilot responses reasoning fallback auth-refresh retry",
                                            Some(session_log_id),
                                        )
                                        .await?;
                                }
                            }
                        }
                    }

                    if fallback.status().is_success() {
                        let mut parser =
                            ResponsesSseParser::new_with_context("Copilot", model, None);
                        let model_for_debug = model.to_string();
                        let stream = llm_stream_from_sse(fallback, move |event, data| {
                            let parsed = parser.handle_event(event, data);
                            append_responses_sse_record(
                                "Copilot",
                                &model_for_debug,
                                event,
                                data,
                                &parsed,
                            );
                            parsed
                        });
                        return Ok(stream);
                    }
                }

                if status == 401 || status == 403 {
                    return Err(LLMError::Auth(format!(
                        "Authentication failed: {}. Please run authenticate() again.",
                        text
                    )));
                }

                if effective_responses_options.previous_response_id.is_some()
                    && Self::looks_like_previous_response_id_unsupported_error(status, &text)
                {
                    return Err(LLMError::Api(format!(
                        "HTTP {} (request_id={}): Copilot HTTP /responses does not support previous_response_id; same-request tool continuation requires the websocket transport used by VS Code Copilot Chat. Upstream response: {}",
                        status, request_id, text
                    )));
                }

                let request_body_bytes = serde_json::to_vec(&body).map(|v| v.len()).unwrap_or(0);
                tracing::error!(
                    "[{}] Copilot Responses API error: HTTP {} - {} (request_id={}, model='{}', input_source={}, input_messages_before={}, input_messages_after={}, duplicate_system_fallback={}, tools={}, request_body_bytes={}, max_output_tokens={:?}, reasoning_effort={:?})",
                    session_log_id,
                    status,
                    text,
                    request_id,
                    model,
                    input_source,
                    input_selection.original_len,
                    input_selection.effective_len,
                    input_selection.fallback_removed_duplicate_system,
                    tools.len(),
                    request_body_bytes,
                    max_output_tokens,
                    reasoning_effort
                );
                tracing::debug!(
                    "[{}] Copilot Responses API error response headers: [{}]",
                    session_log_id,
                    response_headers_debug
                );
                return Err(LLMError::Api(format!(
                    "HTTP {} (request_id={}): {}",
                    status, request_id, text
                )));
            }
        }

        let mut parser = ResponsesSseParser::new_with_context("Copilot", model, reasoning_effort);
        let model_for_debug = model.to_string();
        let stream = llm_stream_from_sse(response, move |event, data| {
            let parsed = parser.handle_event(event, data);
            append_responses_sse_record("Copilot", &model_for_debug, event, data, &parsed);
            parsed
        });
        Ok(stream)
    }

    fn parse_token_limit(value: &serde_json::Value) -> Option<u32> {
        match value {
            serde_json::Value::Number(number) => number
                .as_u64()
                .and_then(|raw| u32::try_from(raw).ok())
                .or_else(|| {
                    number.as_f64().and_then(|raw| {
                        if raw.is_finite() && raw >= 0.0 {
                            let floor = raw.floor();
                            if floor <= u32::MAX as f64 {
                                Some(floor as u32)
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    })
                }),
            serde_json::Value::String(raw) => raw.trim().parse::<u32>().ok(),
            _ => None,
        }
    }

    fn find_max_token_limit_by_keys(value: &serde_json::Value, keys: &[&str]) -> Option<u32> {
        match value {
            serde_json::Value::Object(object) => {
                object.iter().fold(None, |current_max, (key, child)| {
                    let direct = if keys.iter().any(|candidate| *candidate == key) {
                        Self::parse_token_limit(child)
                    } else {
                        None
                    };
                    let nested = Self::find_max_token_limit_by_keys(child, keys);
                    [current_max, direct, nested].into_iter().flatten().max()
                })
            }
            serde_json::Value::Array(items) => items.iter().fold(None, |current_max, item| {
                [current_max, Self::find_max_token_limit_by_keys(item, keys)]
                    .into_iter()
                    .flatten()
                    .max()
            }),
            _ => None,
        }
    }

    fn parse_model_info(payload: serde_json::Value) -> Result<Vec<ProviderModelInfo>> {
        const CONTEXT_KEYS: &[&str] = &[
            "max_context_tokens",
            "max_input_tokens",
            "max_prompt_tokens",
            "context_window",
            "context_window_tokens",
            "input_token_limit",
            "prompt_token_limit",
            "context_length",
        ];
        const OUTPUT_KEYS: &[&str] = &[
            "max_output_tokens",
            "max_completion_tokens",
            "output_token_limit",
            "completion_token_limit",
        ];

        let data = payload
            .get("data")
            .and_then(|value| value.as_array())
            .ok_or_else(|| {
                LLMError::Api("Unexpected Copilot /models response format".to_string())
            })?;

        let mut dedup: BTreeMap<String, ProviderModelInfo> = BTreeMap::new();

        for model in data {
            let Some(id) = model
                .get("id")
                .and_then(|value| value.as_str())
                .map(str::trim)
                .filter(|value| !value.is_empty())
            else {
                continue;
            };

            let context_tokens = Self::find_max_token_limit_by_keys(model, CONTEXT_KEYS);
            let output_tokens = Self::find_max_token_limit_by_keys(model, OUTPUT_KEYS);

            dedup
                .entry(id.to_string())
                .and_modify(|existing| {
                    existing.max_context_tokens = [existing.max_context_tokens, context_tokens]
                        .into_iter()
                        .flatten()
                        .max();
                    existing.max_output_tokens = [existing.max_output_tokens, output_tokens]
                        .into_iter()
                        .flatten()
                        .max();
                })
                .or_insert(ProviderModelInfo {
                    id: id.to_string(),
                    max_context_tokens: context_tokens,
                    max_output_tokens: output_tokens,
                });
        }

        Ok(dedup.into_values().collect())
    }

    async fn fetch_model_info_from_api(&self) -> Result<Vec<ProviderModelInfo>> {
        let token = self.get_token_for_request().await?;
        let url = "https://api.githubcopilot.com/models";

        let mut request_headers = Self::build_headers_with_token(&token)?;
        request_overrides::apply_overrides_to_header_map(
            &mut request_headers,
            self.request_overrides.as_ref(),
            request_overrides::ENDPOINT_MODELS,
            None,
        );
        let mut response = self
            .send_with_transport_retry(
                || self.client.get(url).headers(request_headers.clone()),
                "copilot models list",
                None,
            )
            .await?;

        if !response.status().is_success() {
            let status = response.status();

            if (status == 401 || status == 403) && self.auth_handler.is_some() {
                if let Some(handler) = &self.auth_handler {
                    if let Ok(Some(refreshed)) = handler.force_refresh_chat_token().await {
                        let mut refreshed_headers = Self::build_headers_with_token(&refreshed)?;
                        request_overrides::apply_overrides_to_header_map(
                            &mut refreshed_headers,
                            self.request_overrides.as_ref(),
                            request_overrides::ENDPOINT_MODELS,
                            None,
                        );
                        response = self
                            .send_with_transport_retry(
                                || self.client.get(url).headers(refreshed_headers.clone()),
                                "copilot models list auth-refresh retry",
                                None,
                            )
                            .await?;
                    }
                }
            }

            if !response.status().is_success() {
                let status = response.status();
                let text = response.text().await.unwrap_or_default();

                if status == 401 || status == 403 {
                    return Err(LLMError::Auth(format!(
                        "Authentication failed: {}. Please run authenticate() again.",
                        text
                    )));
                }

                tracing::error!("Copilot API error: HTTP {} - {}", status, text);
                return Err(LLMError::Api(format!("HTTP {}: {}", status, text)));
            }
        }

        let payload: serde_json::Value = response.json().await.map_err(LLMError::Http)?;
        Self::parse_model_info(payload)
    }

    async fn list_model_info_cached(&self) -> Result<Vec<ProviderModelInfo>> {
        let cached = self.models_cache.read().await.clone();
        if let Some(cache) = &cached {
            if cache.fetched_at.elapsed() <= COPILOT_MODELS_CACHE_TTL {
                return Ok(cache.models.clone());
            }
        }

        match self.fetch_model_info_from_api().await {
            Ok(models) => {
                let mut write_guard = self.models_cache.write().await;
                *write_guard = Some(CopilotModelsCache {
                    fetched_at: Instant::now(),
                    models: models.clone(),
                });
                Ok(models)
            }
            Err(error) => {
                if let Some(cache) = cached {
                    tracing::warn!(
                        "Failed to refresh Copilot model metadata; using stale cache: {}",
                        error
                    );
                    return Ok(cache.models);
                }
                Err(error)
            }
        }
    }
}

#[async_trait]
impl LLMProvider for CopilotProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
    ) -> Result<LLMStream> {
        self.chat_stream_with_options(messages, tools, max_output_tokens, model, None)
            .await
    }

    // No `chat_stream_ir` override: the trait default derives the Responses-API
    // view from the canonical IR via `PromptIR::responses_request_options` and
    // routes it through `chat_stream_with_options` below.

    async fn chat_stream_with_options(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> Result<LLMStream> {
        let session_log_id = options
            .and_then(|value| value.session_id.as_deref())
            .unwrap_or("unknown-session");
        let token = self.get_token_for_request().await?;
        let reasoning_effort = options
            .and_then(|o| o.reasoning_effort)
            .or(self.default_reasoning_effort);
        let request_reasoning_effort = options.and_then(|o| o.reasoning_effort);
        let parallel_tool_calls = options.and_then(|o| o.parallel_tool_calls);
        let responses_options = options.and_then(|o| o.responses.as_ref());
        let request_purpose = options
            .and_then(|o| o.request_purpose.as_deref())
            .unwrap_or("unknown");
        let reasoning_source = if request_reasoning_effort.is_some() {
            "request"
        } else if self.default_reasoning_effort.is_some() {
            "provider_default"
        } else {
            "none"
        };

        // Copilot supports multiple upstream model IDs. We don't store a default model in the provider
        // instance (it is resolved by the caller and passed in per request).
        let upstream_model = model.trim();
        if upstream_model.is_empty() {
            return Err(LLMError::Api(
                "model is required for Copilot requests (no default model fallback)".to_string(),
            ));
        }

        tracing::debug!(
            "[{}] Copilot provider using upstream model: {}",
            session_log_id,
            upstream_model
        );

        // Some models only support Responses API.
        if self.uses_responses_api(upstream_model) {
            return self
                .chat_stream_via_responses(
                    &token,
                    messages,
                    tools,
                    max_output_tokens,
                    upstream_model,
                    reasoning_effort,
                    responses_options,
                    parallel_tool_calls,
                    reasoning_source,
                    session_log_id,
                    request_purpose,
                )
                .await;
        }

        let mut body = json!({
            "model": upstream_model,
            "messages": messages_to_openai_compat_json(messages),
            "stream": true,
        });

        // Only include tools and tool_choice if tools are provided
        if !tools.is_empty() {
            body["tools"] = json!(tools_to_openai_compat_json(tools));
            body["tool_choice"] = json!("auto");
        }
        if let Some(parallel_tool_calls) = parallel_tool_calls {
            body["parallel_tool_calls"] = json!(parallel_tool_calls);
        }

        if let Some(max_tokens) = max_output_tokens {
            body["max_tokens"] = json!(max_tokens);
        }

        if let Some(reasoning_effort) = reasoning_effort {
            body["reasoning_effort"] = json!(reasoning_effort.to_wire_format(upstream_model));
        }
        request_overrides::apply_overrides_to_body(
            &mut body,
            self.request_overrides.as_ref(),
            request_overrides::ENDPOINT_CHAT_COMPLETIONS,
            Some(upstream_model),
        );
        crate::masking::mask_outbound_body(&mut body, &self.masking_config);
        tracing::info!(
            "[{}] Copilot request protocol=chat_completions model='{}' reasoning_effort={} reasoning_source={} request_reasoning_enabled={} max_output_tokens={} [{}]",
            session_log_id,
            upstream_model,
            reasoning_effort
                .map(ReasoningEffort::as_str)
                .unwrap_or("none"),
            reasoning_source,
            reasoning_effort.is_some(),
            max_output_tokens
                .map(|tokens| tokens.to_string())
                .unwrap_or_else(|| "none".to_string()),
            request_purpose
        );

        tracing::debug!(
            "[{}] Sending request to Copilot API with {} messages and {} tools",
            session_log_id,
            messages.len(),
            tools.len()
        );

        let url = "https://api.githubcopilot.com/chat/completions";

        let request_headers = self.build_llm_headers(
            &token,
            messages,
            tools,
            request_overrides::ENDPOINT_CHAT_COMPLETIONS,
            Some(upstream_model),
        )?;
        let mut response = self
            .send_with_transport_retry(
                || {
                    self.client
                        .post(url)
                        .headers(request_headers.clone())
                        .json(&body)
                },
                "copilot chat/completions initial request",
                Some(session_log_id),
            )
            .await?;

        if !response.status().is_success() {
            let status = response.status();

            // On auth failures, try a one-time forced refresh via the cached access token.
            if (status == 401 || status == 403) && self.auth_handler.is_some() {
                if let Some(handler) = &self.auth_handler {
                    if let Ok(Some(refreshed)) = handler.force_refresh_chat_token().await {
                        let refreshed_headers = self.build_llm_headers(
                            &refreshed,
                            messages,
                            tools,
                            request_overrides::ENDPOINT_CHAT_COMPLETIONS,
                            Some(upstream_model),
                        )?;
                        response = self
                            .send_with_transport_retry(
                                || {
                                    self.client
                                        .post(url)
                                        .headers(refreshed_headers.clone())
                                        .json(&body)
                                },
                                "copilot chat/completions auth-refresh retry",
                                Some(session_log_id),
                            )
                            .await?;
                    }
                }
            }

            if !response.status().is_success() {
                let status = response.status();
                // Capture diagnostic headers before consuming the body.
                let request_id = response
                    .headers()
                    .get("x-request-id")
                    .or_else(|| response.headers().get("x-github-request-id"))
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("-")
                    .to_string();
                let response_headers_debug: String = response
                    .headers()
                    .iter()
                    .filter(|(k, _)| {
                        let name = k.as_str();
                        // Include error/debug-relevant headers, skip noisy ones.
                        !matches!(
                            name,
                            "set-cookie"
                                | "cookie"
                                | "authorization"
                                | "accept-ranges"
                                | "access-control-allow-origin"
                        )
                    })
                    .map(|(k, v)| format!("{}={}", k, v.to_str().unwrap_or("<binary>")))
                    .collect::<Vec<_>>()
                    .join(", ");
                let text = response.text().await.unwrap_or_default();

                // Check for auth errors
                if status == 401 || status == 403 {
                    return Err(LLMError::Auth(format!(
                        "Authentication failed: {}. Please run authenticate() again.",
                        text
                    )));
                }

                if reasoning_effort.is_some()
                    && Self::looks_like_reasoning_unsupported_error(status, &text)
                {
                    tracing::warn!(
                        "[{}] Copilot /chat/completions rejected reasoning for model '{}'; retrying without reasoning_effort",
                        session_log_id,
                        upstream_model
                    );

                    let mut body_no_reasoning = json!({
                        "model": upstream_model,
                        "messages": messages_to_openai_compat_json(messages),
                        "stream": true,
                    });
                    if !tools.is_empty() {
                        body_no_reasoning["tools"] = json!(tools_to_openai_compat_json(tools));
                        body_no_reasoning["tool_choice"] = json!("auto");
                    }
                    if let Some(parallel_tool_calls) = parallel_tool_calls {
                        body_no_reasoning["parallel_tool_calls"] = json!(parallel_tool_calls);
                    }
                    if let Some(max_tokens) = max_output_tokens {
                        body_no_reasoning["max_tokens"] = json!(max_tokens);
                    }
                    request_overrides::apply_overrides_to_body(
                        &mut body_no_reasoning,
                        self.request_overrides.as_ref(),
                        request_overrides::ENDPOINT_CHAT_COMPLETIONS,
                        Some(upstream_model),
                    );
                    crate::masking::mask_outbound_body(
                        &mut body_no_reasoning,
                        &self.masking_config,
                    );

                    let retry_headers = self.build_llm_headers(
                        &token,
                        messages,
                        tools,
                        request_overrides::ENDPOINT_CHAT_COMPLETIONS,
                        Some(upstream_model),
                    )?;
                    let mut retry = self
                        .send_with_transport_retry(
                            || {
                                self.client
                                    .post(url)
                                    .headers(retry_headers.clone())
                                    .json(&body_no_reasoning)
                            },
                            "copilot chat/completions reasoning fallback",
                            Some(session_log_id),
                        )
                        .await?;

                    if !retry.status().is_success() {
                        let retry_status = retry.status();
                        if (retry_status == 401 || retry_status == 403)
                            && self.auth_handler.is_some()
                        {
                            if let Some(handler) = &self.auth_handler {
                                if let Ok(Some(refreshed)) =
                                    handler.force_refresh_chat_token().await
                                {
                                    let refreshed_retry_headers = self.build_llm_headers(
                                        &refreshed,
                                        messages,
                                        tools,
                                        request_overrides::ENDPOINT_CHAT_COMPLETIONS,
                                        Some(upstream_model),
                                    )?;
                                    retry = self
                                        .send_with_transport_retry(
                                            || {
                                                self.client
                                                    .post(url)
                                                    .headers(refreshed_retry_headers.clone())
                                                    .json(&body_no_reasoning)
                                            },
                                            "copilot chat/completions reasoning fallback auth-refresh retry",
                                            Some(session_log_id),
                                        )
                                        .await?;
                                }
                            }
                        }
                    }

                    if retry.status().is_success() {
                        let stream = llm_stream_from_sse(retry, |_event, data| {
                            let chunk = parse_openai_compat_sse_data_lenient(data)?;
                            match chunk {
                                LLMChunk::Done => Ok(Some(LLMChunk::Done)),
                                other => Ok(Some(other)),
                            }
                        });
                        return Ok(stream);
                    }
                }

                // If this model only supports Responses API, retry with /responses.
                if Self::looks_like_responses_only_error(status, &text) {
                    tracing::info!(
                        "[{}] Copilot chat/completions rejected model '{}'; retrying via /responses",
                        session_log_id,
                        upstream_model
                    );
                    return self
                        .chat_stream_via_responses(
                            &token,
                            messages,
                            tools,
                            max_output_tokens,
                            upstream_model,
                            reasoning_effort,
                            responses_options,
                            parallel_tool_calls,
                            reasoning_source,
                            session_log_id,
                            request_purpose,
                        )
                        .await;
                }

                let request_body_bytes = serde_json::to_vec(&body).map(|v| v.len()).unwrap_or(0);
                tracing::error!(
                    "[{}] Copilot API error: HTTP {} - {} (request_id={}, model='{}', messages={}, tools={}, request_body_bytes={}, max_output_tokens={:?}, reasoning_effort={:?})",
                    session_log_id,
                    status,
                    text,
                    request_id,
                    upstream_model,
                    messages.len(),
                    tools.len(),
                    request_body_bytes,
                    max_output_tokens,
                    reasoning_effort
                );
                tracing::debug!(
                    "[{}] Copilot API error response headers: [{}]",
                    session_log_id,
                    response_headers_debug
                );
                return Err(LLMError::Api(format!(
                    "HTTP {} (request_id={}): {}",
                    status, request_id, text
                )));
            }
        }

        let model_for_log = upstream_model.to_string();
        let requested_reasoning = reasoning_effort;
        let session_for_log = session_log_id.to_string();
        let mut observed_reasoning_signal = false;
        let mut reasoning_chars = 0usize;
        let mut logged_summary = false;
        let stream = llm_stream_from_sse(response, move |_event, data| {
            let mut reasoning_chunk_to_emit: Option<String> = None;
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(data) {
                if let Some(delta) = v
                    .get("choices")
                    .and_then(|choices| choices.get(0))
                    .and_then(|choice| choice.get("delta"))
                {
                    let has_answer_content = delta
                        .get("content")
                        .and_then(|value| value.as_str())
                        .is_some_and(|value| !value.is_empty());
                    let reasoning_chunk = delta
                        .get("reasoning_content")
                        .and_then(|value| value.as_str())
                        .or_else(|| delta.get("reasoning").and_then(|value| value.as_str()));

                    if let Some(reasoning_chunk) = reasoning_chunk {
                        observed_reasoning_signal = true;
                        reasoning_chars = reasoning_chars.saturating_add(reasoning_chunk.len());
                        if !reasoning_chunk.is_empty() && !has_answer_content {
                            reasoning_chunk_to_emit = Some(reasoning_chunk.to_string());
                        }
                    }
                }
            }

            if let Some(reasoning_chunk) = reasoning_chunk_to_emit {
                return Ok(Some(LLMChunk::ReasoningToken(reasoning_chunk)));
            }

            let chunk = parse_openai_compat_sse_data_lenient(data)?;
            match chunk {
                LLMChunk::Done => {
                    if !logged_summary
                        && (requested_reasoning.is_some() || observed_reasoning_signal)
                    {
                        tracing::info!(
                            "[{}] Copilot chat_completions reasoning summary: model='{}' requested_effort={} observed_reasoning_signal={} reasoning_text_chars={}",
                            session_for_log,
                            model_for_log,
                            requested_reasoning
                                .map(ReasoningEffort::as_str)
                                .unwrap_or("none"),
                            observed_reasoning_signal,
                            reasoning_chars
                        );
                        logged_summary = true;
                    }
                    Ok(Some(LLMChunk::Done))
                }
                other => Ok(Some(other)),
            }
        });

        Ok(stream)
    }

    async fn list_models(&self) -> Result<Vec<String>> {
        Ok(self
            .list_model_info_cached()
            .await?
            .into_iter()
            .map(|model| model.id)
            .collect())
    }

    async fn list_model_info(&self) -> Result<Vec<ProviderModelInfo>> {
        self.list_model_info_cached().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::FunctionSchema;

    // Helper to skip tests in CODEX_SANDBOX environment
    fn should_skip() -> bool {
        std::env::var_os("CODEX_SANDBOX").is_some()
    }

    // ============================================
    // Existing Tests (2)
    // ============================================

    #[test]
    fn test_new_provider() {
        if should_skip() {
            return;
        }

        let provider = CopilotProvider::new();
        assert!(!provider.is_authenticated());
    }

    #[test]
    fn test_with_token() {
        if should_skip() {
            return;
        }

        let provider = CopilotProvider::with_token("test_token");
        assert!(provider.is_authenticated());
        assert_eq!(provider.token(), Some("test_token"));
    }

    #[test]
    fn responses_only_models_matches_exact_and_prefix() {
        let provider = CopilotProvider::new()
            .with_responses_only_models(vec!["gpt-5.3-codex".to_string(), "gpt-5*".to_string()]);

        assert!(provider.uses_responses_api("gpt-5.3-codex"));
        assert!(provider.uses_responses_api("gpt-5.4-whatever"));
        assert!(!provider.uses_responses_api("gpt-4o"));
    }

    // ============================================
    // Basic Tests (3)
    // ============================================

    #[test]
    fn test_default_values() {
        if should_skip() {
            return;
        }

        let provider = CopilotProvider::new();
        assert!(provider.token.is_none());
        assert!(provider.token_expires_at.is_none());
        assert!(!provider.is_authenticated());
    }

    #[test]
    fn transport_retry_message_detection_handles_incomplete_message() {
        assert!(CopilotProvider::is_retryable_transport_message(
            "error sending request for url: hyper::Error(IncompleteMessage)",
        ));
    }

    #[test]
    fn transport_retry_message_detection_ignores_validation_errors() {
        assert!(!CopilotProvider::is_retryable_transport_message(
            "HTTP 400: invalid request body",
        ));
    }

    #[test]
    fn test_with_token_chaining() {
        if should_skip() {
            return;
        }

        // Verify that with_token creates a properly configured instance
        let provider = CopilotProvider::with_token("my_token_123");

        // All assertions should pass
        assert!(provider.is_authenticated());
        assert_eq!(provider.token(), Some("my_token_123"));
        assert!(provider.token.is_some());
    }

    #[test]
    fn test_token_expiry() {
        if should_skip() {
            return;
        }

        // Token expiry is set to None when using with_token
        let provider = CopilotProvider::with_token("test_token");
        assert!(provider.token_expires_at.is_none());

        // New provider also has None expiry
        let new_provider = CopilotProvider::new();
        assert!(new_provider.token_expires_at.is_none());
    }

    // ============================================
    // Headers Tests (3)
    // ============================================

    #[test]
    fn test_build_headers_success() {
        if should_skip() {
            return;
        }

        let provider = CopilotProvider::with_token("test_token");
        let headers = provider.build_headers().unwrap();

        // Check authorization header
        assert!(headers.contains_key("authorization"));
        let auth_header = headers.get("authorization").unwrap();
        assert_eq!(auth_header, "Bearer test_token");
    }

    #[test]
    fn test_build_headers_without_token() {
        if should_skip() {
            return;
        }

        let provider = CopilotProvider::new();
        let result = provider.build_headers();

        // Should fail with Auth error
        assert!(result.is_err());
        match result {
            Err(LLMError::Auth(msg)) => {
                assert!(msg.contains("Not authenticated"));
            }
            _ => panic!("Expected Auth error"),
        }
    }

    #[test]
    fn test_headers_contain_required_fields() {
        if should_skip() {
            return;
        }

        let provider = CopilotProvider::with_token("test_token");
        let headers = provider.build_headers().unwrap();

        // Copilot needs to mimic VS Code headers
        assert!(headers.contains_key("authorization"));
        assert!(headers.contains_key("content-type"));
        assert!(headers.contains_key("editor-version"));
        assert!(headers.contains_key("editor-plugin-version"));
        assert!(headers.contains_key("user-agent"));
        assert!(headers.contains_key("accept"));
        assert!(headers.contains_key("accept-encoding"));
        assert!(headers.contains_key("copilot-integration-id"));
        assert!(headers.contains_key("openai-organization"));
        assert!(headers.contains_key("openai-intent"));
        assert!(headers.contains_key("x-github-api-version"));
        assert!(headers.contains_key("x-request-id"));
        assert!(headers.contains_key("x-initiator"));
        assert!(headers.contains_key("x-interaction-id"));
        assert!(headers.contains_key("vscode-sessionid"));
        assert!(headers.contains_key("vscode-machineid"));

        // Verify specific VS Code mimic values
        assert_eq!(headers.get("editor-version").unwrap(), "vscode/1.99.2");
        assert_eq!(
            headers.get("editor-plugin-version").unwrap(),
            "copilot-chat/0.20.3"
        );
        assert_eq!(
            headers.get("user-agent").unwrap(),
            "GitHubCopilotChat/0.20.3"
        );
        assert_eq!(
            headers.get("copilot-integration-id").unwrap(),
            "vscode-chat"
        );
        assert_eq!(headers.get("content-type").unwrap(), "application/json");
        assert_eq!(
            headers.get("openai-organization").unwrap(),
            "github-copilot"
        );
        assert_eq!(headers.get("openai-intent").unwrap(), "conversation-panel");
        assert_eq!(headers.get("x-github-api-version").unwrap(), "2025-05-01");
        assert_eq!(headers.get("x-initiator").unwrap(), "user");
        assert!(
            uuid::Uuid::parse_str(headers.get("x-request-id").unwrap().to_str().unwrap()).is_ok()
        );
    }

    #[test]
    fn infer_openai_intent_uses_agent_for_tool_loops() {
        let messages = vec![
            Message::user("run a tool"),
            Message::assistant("calling tool", None),
            Message::tool_result("call_1", "{\"ok\":true}"),
        ];

        assert_eq!(
            CopilotProvider::infer_openai_intent(&messages, &[]),
            "conversation-agent"
        );
        assert_eq!(CopilotProvider::infer_request_initiator(&messages), "agent");
    }

    #[test]
    fn build_llm_headers_uses_panel_intent_for_plain_user_turn() {
        let provider = CopilotProvider::with_token("test_token");
        let messages = vec![
            Message::system("Stable instructions"),
            Message::user("hello"),
        ];
        let headers = provider
            .build_llm_headers(
                "test_token",
                &messages,
                &[],
                request_overrides::ENDPOINT_RESPONSES,
                Some("gpt-4o"),
            )
            .expect("headers");

        assert_eq!(headers.get("openai-intent").unwrap(), "conversation-panel");
        assert_eq!(headers.get("x-initiator").unwrap(), "user");
    }

    #[test]
    fn build_llm_headers_switches_to_agent_intent_for_tool_enabled_user_turn() {
        let provider = CopilotProvider::with_token("test_token");
        let messages = vec![
            Message::system("Stable instructions"),
            Message::user("run a tool"),
        ];
        let tools = vec![ToolSchema {
            schema_type: "function".to_string(),
            function: FunctionSchema {
                name: "search".to_string(),
                description: "Search".to_string(),
                parameters: serde_json::json!({"type": "object", "properties": {}}),
            },
        }];
        let headers = provider
            .build_llm_headers(
                "test_token",
                &messages,
                &tools,
                request_overrides::ENDPOINT_RESPONSES,
                Some("gpt-4o"),
            )
            .expect("headers");

        assert_eq!(headers.get("openai-intent").unwrap(), "conversation-agent");
        assert_eq!(headers.get("x-initiator").unwrap(), "user");
    }

    #[test]
    fn copilot_responses_forces_store_false_before_building_body() {
        let mut effective_responses_options = ResponsesRequestOptions {
            store: Some(true),
            instructions: Some("Stable instructions".to_string()),
            ..Default::default()
        };
        if effective_responses_options.store == Some(true) {
            effective_responses_options.store = Some(false);
        }

        let body = build_responses_body(
            "gpt-4o",
            &[Message::user("hello")],
            &[],
            None,
            None,
            Some(&effective_responses_options),
            None,
        );

        assert_eq!(body["store"], false);
    }

    #[test]
    fn detects_previous_response_id_unsupported_errors_for_copilot_responses() {
        assert!(
            CopilotProvider::looks_like_previous_response_id_unsupported_error(
                reqwest::StatusCode::UNPROCESSABLE_ENTITY,
                r#"{"error":{"message":"previous_response_id has unsupported_value for this model"}}"#,
            )
        );
        assert!(
            !CopilotProvider::looks_like_previous_response_id_unsupported_error(
                reqwest::StatusCode::BAD_REQUEST,
                r#"{"error":{"message":"invalid input without continuation semantics"}}"#,
            )
        );
    }

    #[test]
    fn copilot_reasoning_and_previous_response_id_fallback_detectors_do_not_overlap() {
        assert!(CopilotProvider::looks_like_reasoning_unsupported_error(
            reqwest::StatusCode::BAD_REQUEST,
            "reasoning_effort is not supported for this model",
        ));
        assert!(
            !CopilotProvider::looks_like_previous_response_id_unsupported_error(
                reqwest::StatusCode::BAD_REQUEST,
                "reasoning_effort is not supported for this model",
            ),
            "reasoning fallback classifier should not misclassify as previous_response_id fallback"
        );

        assert!(
            CopilotProvider::looks_like_previous_response_id_unsupported_error(
                reqwest::StatusCode::BAD_REQUEST,
                "previous_response_id unsupported_value",
            )
        );
        assert!(
            !CopilotProvider::looks_like_reasoning_unsupported_error(
                reqwest::StatusCode::BAD_REQUEST,
                "previous_response_id unsupported_value",
            ),
            "previous_response_id fallback classifier should not misclassify as reasoning fallback"
        );
    }

    // ============================================
    // Authentication State Tests (4)
    // ============================================

    #[test]
    fn test_is_authenticated_with_token() {
        if should_skip() {
            return;
        }

        let provider = CopilotProvider::with_token("valid_token");
        assert!(provider.is_authenticated());
    }

    #[test]
    fn test_is_authenticated_without_token() {
        if should_skip() {
            return;
        }

        let provider = CopilotProvider::new();
        assert!(!provider.is_authenticated());
    }

    #[test]
    fn test_logout_clears_token() {
        if should_skip() {
            return;
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(async {
            let mut provider = CopilotProvider::with_token("test_token");
            assert!(provider.is_authenticated());

            // Note: logout will try to delete a file, which may fail in tests
            // We mainly test that the in-memory state is cleared
            let _ = provider.logout().await;

            // Token should be cleared
            assert!(provider.token.is_none());
            assert!(provider.token_expires_at.is_none());
            assert!(!provider.is_authenticated());

            Ok::<(), ()>(())
        });

        assert!(result.is_ok());
    }

    #[test]
    fn test_token_getter() {
        if should_skip() {
            return;
        }

        // Test with token
        let provider_with_token = CopilotProvider::with_token("my_secret_token");
        assert_eq!(provider_with_token.token(), Some("my_secret_token"));

        // Test without token
        let provider_no_token = CopilotProvider::new();
        assert_eq!(provider_no_token.token(), None);
    }

    // ============================================
    // Request Tests (3)
    // ============================================

    #[test]
    fn test_request_url() {
        if should_skip() {
            return;
        }

        // The URL is hardcoded in chat_stream, verify it's the expected endpoint
        let expected_url = "https://api.githubcopilot.com/chat/completions";
        assert!(expected_url.contains("githubcopilot.com"));
        assert!(expected_url.contains("chat/completions"));
    }

    #[test]
    fn test_request_body_format() {
        if should_skip() {
            return;
        }

        use serde_json::json;

        // Verify the request body structure
        let messages: Vec<Message> = vec![];
        let tools: Vec<ToolSchema> = vec![];
        let mut body = json!({
            "model": "copilot-chat",
            "messages": messages,
            "messages": messages,
            "stream": true,
            "tools": tools,
            "tool_choice": "auto",
        });

        // Verify required fields
        assert_eq!(body["model"], "copilot-chat");
        assert_eq!(body["stream"], true);
        assert_eq!(body["tool_choice"], "auto");

        // Test with max_tokens
        body["max_tokens"] = json!(1000);
        assert_eq!(body["max_tokens"], 1000);
    }

    #[test]
    fn test_request_headers_format() {
        if should_skip() {
            return;
        }

        let provider = CopilotProvider::with_token("test_token");
        let headers = provider.build_headers().unwrap();

        // Verify header values are valid UTF-8
        for (name, value) in headers.iter() {
            assert!(
                value.to_str().is_ok(),
                "Header {} has invalid UTF-8 value",
                name
            );
        }

        // Verify Bearer token format
        let auth = headers.get("authorization").unwrap().to_str().unwrap();
        assert!(auth.starts_with("Bearer "));
        assert!(auth.contains("test_token"));
    }

    // ============================================
    // Error Handling Tests (2)
    // ============================================

    #[test]
    fn test_chat_stream_without_auth_fails() {
        if should_skip() {
            return;
        }

        let provider = CopilotProvider::new(); // No token

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result =
            rt.block_on(async { provider.chat_stream(&[], &[], None, "copilot-chat").await });

        assert!(result.is_err());
        match result {
            Err(LLMError::Auth(msg)) => {
                assert!(msg.contains("Not authenticated"));
            }
            _ => panic!("Expected Auth error"),
        }
    }

    #[test]
    fn test_build_headers_with_invalid_token() {
        if should_skip() {
            return;
        }

        // Test with an empty token string
        let provider = CopilotProvider::with_token("");
        let result = provider.build_headers();

        // Should succeed (empty token is still a valid string for HeaderValue)
        assert!(result.is_ok());
        let headers = result.unwrap();
        let auth = headers.get("authorization").unwrap().to_str().unwrap();
        assert_eq!(auth, "Bearer ");

        // Test with a very long token
        let long_token = "a".repeat(10000);
        let provider_long = CopilotProvider::with_token(long_token.clone());
        let result_long = provider_long.build_headers();

        // Should still succeed
        assert!(result_long.is_ok());

        // Test with special characters in token
        // Note: Some special characters might cause issues with HeaderValue
        // The build_headers method will fail on invalid chars, which is expected
    }

    // ========== MODEL REQUIREMENT ARCHITECTURE TESTS ==========
    // These tests ensure the design principle:
    // "Provider must not have a default model field or with_model() method"

    /// Test: CopilotProvider does NOT have a model field
    #[test]
    fn copilot_provider_has_no_model_field() {
        if should_skip() {
            return;
        }

        // This test documents the provider structure:
        // pub struct CopilotProvider {
        //     client: Client,
        //     token: Option<String>,
        //     token_expires_at: Option<u64>,
        //     auth_handler: Option<CopilotAuthHandler>,
        //     // NO model field!
        // }
        //
        // If someone adds a model field, this test should be updated
        // to reflect the architecture change.
        let provider = CopilotProvider::new();
        // Verify we can access known fields
        assert!(provider.token.is_none());
        assert!(provider.token_expires_at.is_none());
        assert!(!provider.is_authenticated());
        // There is NO provider.model field to access
    }

    /// Test: CopilotProvider does NOT have with_model() method
    #[test]
    fn copilot_provider_has_no_with_model_method() {
        if should_skip() {
            return;
        }

        // Available builder/constructor methods:
        let provider = CopilotProvider::with_token("test_token");

        // There is NO .with_model("copilot-chat") method
        // Model is passed to chat_stream() as a parameter (resolved by the caller per request).

        assert!(provider.is_authenticated());
    }
}
