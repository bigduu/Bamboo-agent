//! OpenAI API provider implementation.
//!
//! This module provides integration with OpenAI's chat completion API,
//! including support for streaming responses and function calling.

use async_trait::async_trait;
use reqwest::{
    header::{HeaderMap, HeaderValue, AUTHORIZATION},
    Client,
};
use serde_json::{json, Value};

use crate::provider::{
    required_tool_from_options, LLMError, LLMProvider, LLMRequestOptions, LLMStream,
    ResponsesRequestOptions, Result,
};
use crate::types::LLMChunk;
use bamboo_config::{KeywordMaskingConfig, RequestOverridesConfig};
use bamboo_domain::Message;
use bamboo_domain::ReasoningEffort;
use bamboo_domain::ToolSchema;

use super::common::model_fetcher;
use super::common::openai_compat::{
    build_openai_compat_body, parse_openai_compat_sse_data_strict,
    parse_openai_compat_sse_data_strict_multi,
};
use super::common::openai_responses::{
    build_responses_body, select_responses_input_messages, ResponsesInputSource, ResponsesSseParser,
};
use super::common::request_overrides;
use super::common::responses_debug::append_responses_sse_record;
use super::common::sse::llm_stream_from_sse_multi;

/// OpenAI API provider for chat completions.
pub struct OpenAIProvider {
    client: Client,
    api_key: String,
    base_url: String,
    responses_only_models: Vec<String>,
    default_reasoning_effort: Option<ReasoningEffort>,
    request_overrides: Option<RequestOverridesConfig>,
    masking_config: KeywordMaskingConfig,
}

impl OpenAIProvider {
    /// Creates a new OpenAI provider with an API key.
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            api_key: api_key.into(),
            base_url: "https://api.openai.com/v1".to_string(),
            responses_only_models: vec![],
            default_reasoning_effort: None,
            request_overrides: None,
            masking_config: KeywordMaskingConfig::default(),
        }
    }

    /// Configure keyword masking applied as a last-moment scan of every outbound
    /// request body (see [`crate::masking`]).
    pub fn with_masking(mut self, masking_config: KeywordMaskingConfig) -> Self {
        self.masking_config = masking_config;
        self
    }

    /// Sets a custom base URL (e.g., for proxies or alternative endpoints).
    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into();
        self
    }

    /// Overrides the internal HTTP client (e.g., to enable a proxy).
    pub fn with_client(mut self, client: Client) -> Self {
        self.client = client;
        self
    }

    /// Configure models that must use Responses API upstream.
    pub fn with_responses_only_models(mut self, models: Vec<String>) -> Self {
        self.responses_only_models = models;
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

    fn build_headers(&self, endpoint: &str, model: Option<&str>) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", self.api_key))
                .map_err(|e| LLMError::Auth(format!("Invalid API key: {}", e)))?,
        );
        request_overrides::apply_overrides_to_header_map(
            &mut headers,
            self.request_overrides.as_ref(),
            endpoint,
            model,
        );
        Ok(headers)
    }

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
        // Shared, tightened heuristic (#237 finding 5).
        crate::providers::common::looks_like_reasoning_unsupported_error(status, body)
    }

    #[allow(clippy::too_many_arguments)]
    async fn chat_stream_via_responses(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
        reasoning_effort: Option<ReasoningEffort>,
        responses_options: Option<&ResponsesRequestOptions>,
        parallel_tool_calls: Option<bool>,
        required_tool: Option<&str>,
        reasoning_source: &str,
        request_purpose: &str,
        session_log_id: &str,
    ) -> Result<LLMStream> {
        let input_selection = select_responses_input_messages(messages, responses_options);
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
            responses_options,
            parallel_tool_calls,
        );
        request_overrides::apply_overrides_to_body(
            &mut body,
            self.request_overrides.as_ref(),
            request_overrides::ENDPOINT_RESPONSES,
            Some(model),
        );
        if let Some(name) = required_tool {
            body["tool_choice"] = json!({"type": "function", "name": name});
        }
        // Last-moment scan: mask every text value in the fully-assembled body.
        crate::masking::mask_outbound_body(&mut body, &self.masking_config);
        tracing::info!(
            "[{}] OpenAI request protocol=responses model='{}' reasoning_effort={} reasoning_source={} request_reasoning_enabled={} max_output_tokens={} input_source={} input_messages_before={} input_messages_after={} duplicate_system_fallback={} [{}]",
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

        let headers = self.build_headers(request_overrides::ENDPOINT_RESPONSES, Some(model))?;
        // Retry the initial request on transient failures (issue #18); the
        // returned body is unread, so SSE streaming below is unaffected.
        let responses_url = format!("{}/responses", self.base_url);
        let response = crate::retry::send_with_retry(crate::retry::global(), "OpenAI", || {
            self.client
                .post(&responses_url)
                .headers(headers.clone())
                .json(&body)
        })
        .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await?;

            // The upstream can no longer resolve the stateful continuation id —
            // e.g. the referenced turn was never stored (`store=false`), aged out
            // of retention, or belongs to another key/org. The full input array
            // is sent alongside the id, so retrying once WITHOUT the continuation
            // is lossless and keeps the session alive instead of hard-failing.
            let sent_previous_response_id = responses_options
                .and_then(|opts| opts.previous_response_id.as_deref())
                .map(str::trim)
                .is_some_and(|value| !value.is_empty());
            if sent_previous_response_id
                && crate::providers::common::looks_like_previous_response_not_found_error(
                    status, &text,
                )
            {
                tracing::warn!(
                    "OpenAI /responses could not find previous_response_id for model '{}'; retrying without stateful continuation. Upstream response: {}",
                    model,
                    text
                );

                let mut fallback_options = responses_options.cloned().unwrap_or_default();
                fallback_options.previous_response_id = None;
                let mut fallback_body = build_responses_body(
                    model,
                    messages,
                    tools,
                    max_output_tokens,
                    reasoning_effort,
                    Some(&fallback_options),
                    parallel_tool_calls,
                );
                request_overrides::apply_overrides_to_body(
                    &mut fallback_body,
                    self.request_overrides.as_ref(),
                    request_overrides::ENDPOINT_RESPONSES,
                    Some(model),
                );
                if let Some(name) = required_tool {
                    fallback_body["tool_choice"] = json!({"type": "function", "name": name});
                }
                crate::masking::mask_outbound_body(&mut fallback_body, &self.masking_config);
                let fallback_headers =
                    self.build_headers(request_overrides::ENDPOINT_RESPONSES, Some(model))?;
                let fallback =
                    crate::retry::send_with_retry(crate::retry::global(), "OpenAI", || {
                        self.client
                            .post(&responses_url)
                            .headers(fallback_headers.clone())
                            .json(&fallback_body)
                    })
                    .await?;

                if !fallback.status().is_success() {
                    let fallback_status = fallback.status();
                    let fallback_text = fallback.text().await?;
                    return Err(LLMError::Api(format!(
                        "HTTP {}: {}",
                        fallback_status, fallback_text
                    )));
                }

                let mut parser =
                    ResponsesSseParser::new_with_context("OpenAI", model, reasoning_effort);
                let model_for_debug = model.to_string();
                let stream = llm_stream_from_sse_multi(fallback, move |event, data| {
                    let parsed = parser.handle_event_multi(event, data);
                    append_responses_sse_record("OpenAI", &model_for_debug, event, data, &parsed);
                    parsed
                });
                return Ok(stream);
            }

            if reasoning_effort.is_some()
                && Self::looks_like_reasoning_unsupported_error(status, &text)
            {
                tracing::warn!(
                    "OpenAI /responses rejected reasoning for model '{}'; retrying without reasoning_effort",
                    model
                );

                let mut fallback_options = responses_options.cloned().unwrap_or_default();
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
                if let Some(name) = required_tool {
                    fallback_body["tool_choice"] = json!({"type": "function", "name": name});
                }
                crate::masking::mask_outbound_body(&mut fallback_body, &self.masking_config);
                let fallback_headers =
                    self.build_headers(request_overrides::ENDPOINT_RESPONSES, Some(model))?;
                let fallback =
                    crate::retry::send_with_retry(crate::retry::global(), "OpenAI", || {
                        self.client
                            .post(&responses_url)
                            .headers(fallback_headers.clone())
                            .json(&fallback_body)
                    })
                    .await?;

                if !fallback.status().is_success() {
                    let fallback_status = fallback.status();
                    let fallback_text = fallback.text().await?;
                    return Err(LLMError::Api(format!(
                        "HTTP {}: {}",
                        fallback_status, fallback_text
                    )));
                }

                let mut parser = ResponsesSseParser::new_with_context("OpenAI", model, None);
                let model_for_debug = model.to_string();
                let stream = llm_stream_from_sse_multi(fallback, move |event, data| {
                    let parsed = parser.handle_event_multi(event, data);
                    append_responses_sse_record("OpenAI", &model_for_debug, event, data, &parsed);
                    parsed
                });
                return Ok(stream);
            }

            return Err(LLMError::Api(format!("HTTP {}: {}", status, text)));
        }

        let mut parser = ResponsesSseParser::new_with_context("OpenAI", model, reasoning_effort);
        let model_for_debug = model.to_string();
        let stream = llm_stream_from_sse_multi(response, move |event, data| {
            let parsed = parser.handle_event_multi(event, data);
            append_responses_sse_record("OpenAI", &model_for_debug, event, data, &parsed);
            parsed
        });
        Ok(stream)
    }
}

#[async_trait]
impl LLMProvider for OpenAIProvider {
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
    // view (input array / instructions / previous_response_id) from the canonical
    // IR via `PromptIR::responses_request_options` and routes it through
    // `chat_stream_with_options` below — which dispatches to /responses for
    // Responses-only models and chat/completions otherwise.

    async fn chat_stream_with_options(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> Result<LLMStream> {
        tracing::debug!("OpenAI provider using model: {}", model);
        let reasoning_effort = options
            .and_then(|o| o.reasoning_effort)
            .or(self.default_reasoning_effort);
        let request_reasoning_effort = options.and_then(|o| o.reasoning_effort);
        let parallel_tool_calls = options.and_then(|o| o.parallel_tool_calls);
        let required_tool = required_tool_from_options(options, tools)?;
        let responses_options = options.and_then(|o| o.responses.as_ref());
        let request_purpose = options
            .and_then(|o| o.request_purpose.as_deref())
            .unwrap_or("unknown");
        let session_log_id = options
            .and_then(|o| o.session_id.as_deref())
            .unwrap_or("unknown-session");
        let reasoning_source = if request_reasoning_effort.is_some() {
            "request"
        } else if self.default_reasoning_effort.is_some() {
            "provider_default"
        } else {
            "none"
        };

        if self.uses_responses_api(model) {
            return self
                .chat_stream_via_responses(
                    messages,
                    tools,
                    max_output_tokens,
                    model,
                    reasoning_effort,
                    responses_options,
                    parallel_tool_calls,
                    required_tool,
                    reasoning_source,
                    request_purpose,
                    session_log_id,
                )
                .await;
        }

        let mut body = build_openai_compat_body(
            model,
            messages,
            tools,
            None,
            max_output_tokens,
            reasoning_effort,
            parallel_tool_calls,
        );
        request_overrides::apply_overrides_to_body(
            &mut body,
            self.request_overrides.as_ref(),
            request_overrides::ENDPOINT_CHAT_COMPLETIONS,
            Some(model),
        );
        if let Some(name) = required_tool {
            body["tool_choice"] = json!({"type": "function", "function": {"name": name}});
        }
        crate::masking::mask_outbound_body(&mut body, &self.masking_config);
        tracing::info!(
            "[{}] OpenAI request protocol=chat_completions model='{}' reasoning_effort={} reasoning_source={} request_reasoning_enabled={} max_output_tokens={} [{}]",
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
            request_purpose
        );

        let headers =
            self.build_headers(request_overrides::ENDPOINT_CHAT_COMPLETIONS, Some(model))?;
        // Retry the initial request on transient failures (issue #18); the
        // returned body is unread, so SSE streaming below is unaffected.
        let chat_url = format!("{}/chat/completions", self.base_url);
        let response = crate::retry::send_with_retry(crate::retry::global(), "OpenAI", || {
            self.client
                .post(&chat_url)
                .headers(headers.clone())
                .json(&body)
        })
        .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await?;

            if reasoning_effort.is_some()
                && Self::looks_like_reasoning_unsupported_error(status, &text)
            {
                tracing::warn!(
                    "OpenAI /chat/completions rejected reasoning for model '{}'; retrying without reasoning_effort",
                    model
                );

                let mut fallback_body = build_openai_compat_body(
                    model,
                    messages,
                    tools,
                    None,
                    max_output_tokens,
                    None,
                    parallel_tool_calls,
                );
                request_overrides::apply_overrides_to_body(
                    &mut fallback_body,
                    self.request_overrides.as_ref(),
                    request_overrides::ENDPOINT_CHAT_COMPLETIONS,
                    Some(model),
                );
                if let Some(name) = required_tool {
                    fallback_body["tool_choice"] =
                        json!({"type": "function", "function": {"name": name}});
                }
                crate::masking::mask_outbound_body(&mut fallback_body, &self.masking_config);
                let fallback_headers =
                    self.build_headers(request_overrides::ENDPOINT_CHAT_COMPLETIONS, Some(model))?;
                let fallback =
                    crate::retry::send_with_retry(crate::retry::global(), "OpenAI", || {
                        self.client
                            .post(&chat_url)
                            .headers(fallback_headers.clone())
                            .json(&fallback_body)
                    })
                    .await?;

                if fallback.status().is_success() {
                    let stream = llm_stream_from_sse_multi(fallback, |_event, data| {
                        if data.trim().is_empty() {
                            return Ok(Vec::new());
                        }

                        parse_openai_compat_sse_data_strict_multi(data)
                    });

                    return Ok(stream);
                }

                // The no-reasoning retry itself failed — surface ITS status/body,
                // not the stale original "reasoning unsupported" 400 (which would
                // mask the real failure, e.g. a 500/429 on the retry). (#237)
                let fallback_status = fallback.status();
                let fallback_text = fallback.text().await.unwrap_or_default();
                return Err(LLMError::Api(format!(
                    "HTTP {fallback_status}: {fallback_text} (after retrying without reasoning_effort)"
                )));
            }

            if Self::looks_like_responses_only_error(status, &text) {
                tracing::info!(
                    "OpenAI chat/completions rejected model '{}'; retrying via /responses",
                    model
                );
                return self
                    .chat_stream_via_responses(
                        messages,
                        tools,
                        max_output_tokens,
                        model,
                        reasoning_effort,
                        responses_options,
                        parallel_tool_calls,
                        required_tool,
                        reasoning_source,
                        request_purpose,
                        session_log_id,
                    )
                    .await;
            }

            return Err(LLMError::Api(format!("HTTP {}: {}", status, text)));
        }

        let model_for_log = model.to_string();
        let requested_reasoning = reasoning_effort;
        let mut observed_reasoning_signal = false;
        let mut reasoning_chars = 0usize;
        let mut logged_summary = false;
        let stream = llm_stream_from_sse_multi(response, move |_event, data| {
            if data.trim().is_empty() {
                return Ok(Vec::new());
            }

            let mut reasoning_chunk_to_emit: Option<String> = None;
            if let Ok(v) = serde_json::from_str::<Value>(data) {
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

            let mut chunks = parse_openai_compat_sse_data_strict_multi(data)?;
            if let Some(reasoning_chunk) = reasoning_chunk_to_emit {
                chunks.retain(|chunk| !matches!(chunk, LLMChunk::Token(token) if token.is_empty()));
                chunks.insert(0, LLMChunk::ReasoningToken(reasoning_chunk));
            }

            if chunks.iter().any(|chunk| matches!(chunk, LLMChunk::Done))
                && !logged_summary
                && (requested_reasoning.is_some() || observed_reasoning_signal)
            {
                tracing::info!(
                    "OpenAI chat_completions reasoning summary: model='{}' requested_effort={} observed_reasoning_signal={} reasoning_text_chars={}",
                    model_for_log,
                    requested_reasoning
                        .map(ReasoningEffort::as_str)
                        .unwrap_or("none"),
                    observed_reasoning_signal,
                    reasoning_chars
                );
                logged_summary = true;
            }
            Ok(chunks)
        });

        Ok(stream)
    }

    async fn list_models(&self) -> Result<Vec<String>> {
        let headers = self.build_headers(request_overrides::ENDPOINT_MODELS, None)?;
        let url = format!("{}/models", self.base_url.trim_end_matches('/'));
        model_fetcher::fetch_model_list(&self.client, &url, headers, "OpenAI").await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::Message;
    use bamboo_domain::{FunctionSchema, ToolSchema};

    // ===== Basic Tests (5 tests) =====

    #[test]
    fn test_new_provider() {
        let provider = OpenAIProvider::new("test_key");
        assert_eq!(provider.api_key, "test_key");
        assert_eq!(provider.base_url, "https://api.openai.com/v1");
    }

    #[test]
    fn test_with_base_url() {
        let provider =
            OpenAIProvider::new("test_key").with_base_url("https://custom.openai.com/v1");
        assert_eq!(provider.base_url, "https://custom.openai.com/v1");
    }

    #[test]
    fn test_default_values() {
        let provider = OpenAIProvider::new("test_key");
        assert_eq!(provider.base_url, "https://api.openai.com/v1");
    }

    #[test]
    fn test_chained_builders() {
        let provider =
            OpenAIProvider::new("test_key").with_base_url("https://custom.openai.com/v1");

        assert_eq!(provider.api_key, "test_key");
        assert_eq!(provider.base_url, "https://custom.openai.com/v1");
    }

    #[test]
    fn responses_only_models_matches_exact_and_prefix() {
        let provider = OpenAIProvider::new("k")
            .with_responses_only_models(vec!["gpt-5.3-codex".to_string(), "gpt-5*".to_string()]);

        assert!(provider.uses_responses_api("gpt-5.3-codex"));
        assert!(provider.uses_responses_api("gpt-5.0-any"));
        assert!(!provider.uses_responses_api("gpt-4o-mini"));
    }

    // ===== Request Building Tests (4 tests) =====

    #[test]
    fn test_authorization_header() {
        let provider = OpenAIProvider::new("sk-test-12345");

        // Verify the authorization header format
        let expected_auth = format!("Bearer {}", provider.api_key);
        assert_eq!(expected_auth, "Bearer sk-test-12345");
    }

    #[test]
    fn test_request_url_construction() {
        let provider = OpenAIProvider::new("test_key").with_base_url("https://api.custom.com/v1");

        let expected_url = format!("{}/chat/completions", provider.base_url);
        assert_eq!(expected_url, "https://api.custom.com/v1/chat/completions");
    }

    #[test]
    fn test_request_body_basic() {
        let messages = vec![Message::user("Hello")];
        let tools: Vec<ToolSchema> = vec![];

        let body =
            build_openai_compat_body("gpt-4o-mini", &messages, &tools, None, None, None, None);

        assert_eq!(body["model"], "gpt-4o-mini");
        assert_eq!(body["stream"], true);
        assert!(body["messages"].is_array());
        assert_eq!(body["messages"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_request_body_with_tools() {
        let messages = vec![Message::user("Search for weather")];
        let tools = vec![ToolSchema {
            schema_type: "function".to_string(),
            function: FunctionSchema {
                name: "search_weather".to_string(),
                description: "Search for weather information".to_string(),
                parameters: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "location": { "type": "string" }
                    }
                }),
            },
        }];

        let body =
            build_openai_compat_body("gpt-4o-mini", &messages, &tools, None, None, None, None);

        assert_eq!(body["tools"].as_array().unwrap().len(), 1);
        assert_eq!(body["tools"][0]["type"], "function");
        assert_eq!(body["tools"][0]["function"]["name"], "search_weather");
    }

    // ===== Streaming Response Tests (4 tests) =====

    #[test]
    fn test_parse_simple_token() {
        let data = r#"{"id":"chatcmpl-123","choices":[{"delta":{"content":"Hello"},"finish_reason":null}]}"#;

        let chunk = parse_openai_compat_sse_data_strict(data).unwrap();

        match chunk {
            LLMChunk::Token(text) => assert_eq!(text, "Hello"),
            _ => panic!("Expected Token chunk"),
        }
    }

    #[test]
    fn test_parse_tool_call() {
        let data = r#"{"id":"chatcmpl-123","choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_abc123","type":"function","function":{"name":"search","arguments":"{\"q\":\"test\"}"}}]},"finish_reason":null}]}"#;

        let chunk = parse_openai_compat_sse_data_strict(data).unwrap();

        // The chat-completions path emits ToolCallsIndexed carrying the index. #236.
        match chunk {
            LLMChunk::ToolCallsIndexed(calls) => {
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].0, 0);
                assert_eq!(calls[0].1.id, "call_abc123");
                assert_eq!(calls[0].1.tool_type, "function");
                assert_eq!(calls[0].1.function.name, "search");
                assert_eq!(calls[0].1.function.arguments, r#"{"q":"test"}"#);
            }
            _ => panic!("Expected ToolCallsIndexed chunk"),
        }
    }

    #[test]
    fn test_parse_done_signal() {
        let data = "[DONE]";

        let chunk = parse_openai_compat_sse_data_strict(data).unwrap();

        assert!(matches!(chunk, LLMChunk::Done));
    }

    #[test]
    fn test_parse_empty_delta() {
        let data = r#"{"id":"chatcmpl-123","choices":[{"delta":{},"finish_reason":null}]}"#;

        let chunk = parse_openai_compat_sse_data_strict(data).unwrap();

        match chunk {
            LLMChunk::Token(text) => assert!(text.is_empty()),
            _ => panic!("Expected empty Token chunk"),
        }
    }

    // ===== Error Handling Tests (2 tests) =====

    #[test]
    fn test_api_error_response() {
        // Test that we can handle API error format
        let error_response = r#"{"error":{"message":"Invalid API key","type":"invalid_request_error","code":"invalid_api_key"}}"#;

        // We can't test the full error flow without a mock server,
        // but we can verify the error format is parseable
        let parsed: serde_json::Result<serde_json::Value> = serde_json::from_str(error_response);
        assert!(parsed.is_ok());

        let error_json = parsed.unwrap();
        assert_eq!(error_json["error"]["message"], "Invalid API key");
        assert_eq!(error_json["error"]["code"], "invalid_api_key");
    }

    #[test]
    fn test_invalid_json_response() {
        let invalid_data = "{not valid json}";

        let result = parse_openai_compat_sse_data_strict(invalid_data);

        assert!(result.is_err());
    }

    // ===== Additional Edge Case Tests =====

    #[test]
    fn test_request_body_with_max_tokens() {
        let messages = vec![Message::user("Hello")];
        let tools: Vec<ToolSchema> = vec![];

        let body = build_openai_compat_body(
            "gpt-4o-mini",
            &messages,
            &tools,
            None,
            Some(4096),
            None,
            None,
        );

        assert_eq!(body["max_tokens"], 4096);
    }

    #[test]
    fn test_multiple_messages_request() {
        let messages = vec![
            Message::system("You are helpful"),
            Message::user("Hi"),
            Message::assistant("Hello!", None),
            Message::user("How are you?"),
        ];
        let tools: Vec<ToolSchema> = vec![];

        let body =
            build_openai_compat_body("gpt-4o-mini", &messages, &tools, None, None, None, None);

        assert_eq!(body["messages"].as_array().unwrap().len(), 4);
    }

    #[test]
    fn test_provider_immutability() {
        // Verify that builder methods work correctly
        let provider = OpenAIProvider::new("key1").with_base_url("https://custom.api.com");

        // Verify all settings are applied
        assert_eq!(provider.api_key, "key1");
        assert_eq!(provider.base_url, "https://custom.api.com");
    }

    #[test]
    fn test_tool_call_partial_delta() {
        // Test tool call with only name (no arguments yet)
        let data = r#"{"id":"chatcmpl-123","choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_123","type":"function","function":{"name":"search"}}]},"finish_reason":null}]}"#;

        let chunk = parse_openai_compat_sse_data_strict(data).unwrap();

        match chunk {
            LLMChunk::ToolCallsIndexed(calls) => {
                assert_eq!(calls[0].0, 0);
                assert_eq!(calls[0].1.id, "call_123");
                assert_eq!(calls[0].1.function.name, "search");
                // Arguments should be empty string when not provided
                assert_eq!(calls[0].1.function.arguments, "");
            }
            _ => panic!("Expected ToolCallsIndexed chunk"),
        }
    }

    #[test]
    fn test_multiple_tool_calls_in_single_chunk() {
        let data = r#"{"id":"chatcmpl-123","choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"search","arguments":"{}"}},{"index":1,"id":"call_2","type":"function","function":{"name":"lookup","arguments":"{}"}}]},"finish_reason":null}]}"#;

        let chunk = parse_openai_compat_sse_data_strict(data).unwrap();

        match chunk {
            LLMChunk::ToolCallsIndexed(calls) => {
                assert_eq!(calls.len(), 2);
                assert_eq!(calls[0].0, 0);
                assert_eq!(calls[0].1.function.name, "search");
                assert_eq!(calls[1].0, 1);
                assert_eq!(calls[1].1.function.name, "lookup");
            }
            _ => panic!("Expected ToolCallsIndexed chunk"),
        }
    }

    #[test]
    fn test_whitespace_in_done_signal() {
        let data = "  [DONE]  ";

        let chunk = parse_openai_compat_sse_data_strict(data).unwrap();

        assert!(matches!(chunk, LLMChunk::Done));
    }

    // ========== MODEL REQUIREMENT ARCHITECTURE TESTS ==========
    // These tests ensure the design principle:
    // "Provider must not have a default model field or with_model() method"

    /// Test: OpenAIProvider does NOT have a model field
    #[test]
    fn openai_provider_has_no_model_field() {
        // This test documents the provider structure:
        // pub struct OpenAIProvider {
        //     client: Client,
        //     api_key: String,
        //     base_url: String,
        //     // NO model field!
        // }
        //
        // If someone adds a model field, this test should be updated
        // to reflect the architecture change.
        let provider = OpenAIProvider::new("test_key");
        // Verify we can access known fields
        assert_eq!(provider.api_key, "test_key");
        assert_eq!(provider.base_url, "https://api.openai.com/v1");
        // There is NO provider.model field to access
    }

    /// Test: OpenAIProvider does NOT have with_model() method
    #[test]
    fn openai_provider_has_no_with_model_method() {
        let provider = OpenAIProvider::new("test_key");

        // Available builder method:
        let provider = provider.with_base_url("https://custom.api.com");

        // There is NO .with_model("gpt-4") method
        // Model is passed to chat_stream() as a parameter

        assert_eq!(provider.base_url, "https://custom.api.com");
    }

    // ===== /responses previous_response_not_found fallback (wiremock) =====

    use futures::StreamExt;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

    const PREVIOUS_RESPONSE_NOT_FOUND_BODY: &str = r#"{"error":{"message":"Previous response with id 'resp_stale' not found.","type":"invalid_request_error","param":"previous_response_id","code":"previous_response_not_found"}}"#;

    const RESPONSES_SSE_OK: &str = "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_new\"}}\n\n";
    const CHAT_SSE_OK: &str =
        "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n";

    fn load_skill_tool() -> ToolSchema {
        ToolSchema {
            schema_type: "function".to_string(),
            function: FunctionSchema {
                name: "load_skill".to_string(),
                description: "Load one skill".to_string(),
                parameters: serde_json::json!({"type": "object"}),
            },
        }
    }

    fn require_load_skill_options() -> LLMRequestOptions {
        LLMRequestOptions {
            required_tool: Some("load_skill".to_string()),
            parallel_tool_calls: Some(false),
            reasoning_effort: Some(bamboo_domain::ReasoningEffort::High),
            ..Default::default()
        }
    }

    /// Emulates the real OpenAI contract: a request chaining a stale (or never
    /// stored) `previous_response_id` fails with 400
    /// `previous_response_not_found`; a request without it streams normally.
    struct PreviousResponseNotFoundResponder;

    impl Respond for PreviousResponseNotFoundResponder {
        fn respond(&self, request: &Request) -> ResponseTemplate {
            let body: serde_json::Value =
                serde_json::from_slice(&request.body).expect("JSON request body");
            if body.get("previous_response_id").is_some() {
                ResponseTemplate::new(400).set_body_string(PREVIOUS_RESPONSE_NOT_FOUND_BODY)
            } else {
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(RESPONSES_SSE_OK)
            }
        }
    }

    fn responses_provider(server: &MockServer) -> OpenAIProvider {
        OpenAIProvider::new("test-key")
            .with_base_url(server.uri())
            .with_responses_only_models(vec!["gpt-5*".to_string()])
    }

    #[tokio::test]
    async fn chat_completions_forces_named_required_tool_on_wire() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(CHAT_SSE_OK),
            )
            .expect(1)
            .mount(&server)
            .await;
        let provider = OpenAIProvider::new("test-key").with_base_url(server.uri());
        let tools = vec![load_skill_tool()];
        let options = require_load_skill_options();

        let _stream = provider
            .chat_stream_with_options(
                &[Message::user("activate")],
                &tools,
                None,
                "deepseek-v4-pro",
                Some(&options),
            )
            .await
            .expect("forced chat request");

        let requests = server.received_requests().await.expect("requests recorded");
        let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(
            body["tool_choice"],
            serde_json::json!({
                "type": "function",
                "function": {"name": "load_skill"}
            })
        );
        assert_eq!(body["parallel_tool_calls"], false);
        assert_eq!(body["reasoning_effort"], "high");
    }

    #[tokio::test]
    async fn chat_completions_production_stream_preserves_same_frame_text_and_usage() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(
                        concat!(
                            "data: {\"choices\":[{\"delta\":{\"content\":\"answer\"}}],\"usage\":{\"prompt_tokens\":1000,\"completion_tokens\":120,\"prompt_tokens_details\":{\"cached_tokens\":768},\"completion_tokens_details\":{\"reasoning_tokens\":20}}}\n",
                            "\n",
                            "data: [DONE]\n",
                            "\n",
                        ),
                    ),
            )
            .expect(1)
            .mount(&server)
            .await;
        let provider = OpenAIProvider::new("test-key").with_base_url(server.uri());

        let mut stream = provider
            .chat_stream(&[Message::user("hello")], &[], None, "gpt-4o")
            .await
            .expect("chat stream");
        let mut chunks = Vec::new();
        while let Some(chunk) = stream.next().await {
            chunks.push(chunk.expect("stream chunk"));
        }

        assert_eq!(chunks.len(), 3);
        assert!(matches!(&chunks[0], LLMChunk::Token(text) if text == "answer"));
        assert!(matches!(
            chunks[1],
            LLMChunk::ProviderUsage {
                input_tokens: Some(1000),
                output_tokens: Some(120),
                reasoning_tokens: Some(20),
                cache_read_input_tokens: Some(768),
                ..
            }
        ));
        assert!(matches!(chunks[2], LLMChunk::Done));
    }

    #[tokio::test]
    async fn responses_forces_named_required_tool_on_wire() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/responses"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(RESPONSES_SSE_OK),
            )
            .expect(1)
            .mount(&server)
            .await;
        let provider = responses_provider(&server);
        let tools = vec![load_skill_tool()];
        let options = require_load_skill_options();

        let _stream = provider
            .chat_stream_with_options(
                &[Message::user("activate")],
                &tools,
                None,
                "gpt-5.2",
                Some(&options),
            )
            .await
            .expect("forced Responses request");

        let requests = server.received_requests().await.expect("requests recorded");
        let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(
            body["tool_choice"],
            serde_json::json!({"type": "function", "name": "load_skill"})
        );
        assert_eq!(body["parallel_tool_calls"], false);
        assert_eq!(body["reasoning"]["effort"], "high");
    }

    #[tokio::test]
    async fn required_tool_missing_from_schemas_fails_before_network() {
        let provider = OpenAIProvider::new("test-key");
        let result = provider
            .chat_stream_with_options(
                &[Message::user("activate")],
                &[],
                None,
                "deepseek-v4-pro",
                Some(&require_load_skill_options()),
            )
            .await;
        let error = match result {
            Ok(_) => panic!("missing required schema must fail closed"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("required tool schema 'load_skill' was not offered"));
    }

    fn options_with_previous_response_id(id: Option<&str>) -> LLMRequestOptions {
        LLMRequestOptions {
            responses: Some(ResponsesRequestOptions {
                previous_response_id: id.map(str::to_string),
                store: Some(false),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    async fn collect_tokens(mut stream: LLMStream) -> String {
        let mut text = String::new();
        while let Some(chunk) = stream.next().await {
            match chunk.expect("stream chunk") {
                LLMChunk::Token(token) => text.push_str(&token),
                LLMChunk::Done => break,
                _ => {}
            }
        }
        text
    }

    #[tokio::test]
    async fn responses_retries_without_previous_response_id_on_not_found() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/responses"))
            .respond_with(PreviousResponseNotFoundResponder)
            .expect(2)
            .mount(&server)
            .await;

        let provider = responses_provider(&server);
        let options = options_with_previous_response_id(Some("resp_stale"));
        let stream = provider
            .chat_stream_with_options(
                &[Message::user("hello")],
                &[],
                None,
                "gpt-5.2",
                Some(&options),
            )
            .await
            .expect("fallback retry without previous_response_id must succeed");

        assert_eq!(collect_tokens(stream).await, "hi");

        // First request chained the stale id; the retry dropped it but kept the
        // full input array, so no context was lost.
        let requests = server.received_requests().await.expect("requests recorded");
        assert_eq!(requests.len(), 2);
        let first: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
        let second: serde_json::Value = serde_json::from_slice(&requests[1].body).unwrap();
        assert_eq!(first["previous_response_id"], "resp_stale");
        assert!(second.get("previous_response_id").is_none());
        assert_eq!(first["input"], second["input"]);
    }

    #[tokio::test]
    async fn responses_does_not_retry_unrelated_400_errors() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/responses"))
            .respond_with(ResponseTemplate::new(400).set_body_string(
                r#"{"error":{"message":"The model `gpt-x` does not exist","type":"invalid_request_error","param":"model","code":"model_not_found"}}"#,
            ))
            .expect(1)
            .mount(&server)
            .await;

        let provider = responses_provider(&server);
        let options = options_with_previous_response_id(Some("resp_stale"));
        let error = match provider
            .chat_stream_with_options(
                &[Message::user("hello")],
                &[],
                None,
                "gpt-5.2",
                Some(&options),
            )
            .await
        {
            Ok(_) => panic!("unrelated 400 must not trigger the continuation fallback"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("model_not_found"));
    }

    #[tokio::test]
    async fn responses_does_not_retry_not_found_when_no_continuation_was_sent() {
        // A broken upstream that answers `previous_response_not_found` even though
        // no id was sent must NOT put the provider into a retry loop.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/responses"))
            .respond_with(
                ResponseTemplate::new(400).set_body_string(PREVIOUS_RESPONSE_NOT_FOUND_BODY),
            )
            .expect(1)
            .mount(&server)
            .await;

        let provider = responses_provider(&server);
        let options = options_with_previous_response_id(None);
        let error = match provider
            .chat_stream_with_options(
                &[Message::user("hello")],
                &[],
                None,
                "gpt-5.2",
                Some(&options),
            )
            .await
        {
            Ok(_) => panic!("no continuation sent → surface the upstream error as-is"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("previous_response_not_found"));
    }
}
