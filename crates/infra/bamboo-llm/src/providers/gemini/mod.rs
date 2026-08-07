//! Google Gemini provider implementation.

mod stream;

pub use stream::{parse_gemini_sse_event, GeminiStreamState};

use async_trait::async_trait;
use reqwest::{
    header::{HeaderMap, HeaderValue, CONTENT_TYPE},
    Client,
};
use serde_json::{json, Value};

use crate::protocol::gemini::GeminiRequest;
use crate::protocol::ToProvider;
use crate::provider::{LLMError, LLMProvider, LLMRequestOptions, LLMStream, Result};
use crate::providers::common::model_fetcher;
use crate::providers::common::request_overrides;
use crate::types::LLMChunk;
use bamboo_config::{KeywordMaskingConfig, RequestOverridesConfig};
use bamboo_domain::Message;
use bamboo_domain::ReasoningEffort;

pub(crate) fn apply_required_tool_choice(body: &mut Value, required_tool: Option<&str>) {
    if let Some(name) = required_tool {
        body["toolConfig"] = json!({
            "functionCallingConfig": {
                "mode": "ANY",
                "allowedFunctionNames": [name]
            }
        });
    }
}

pub(crate) fn apply_generation_config(
    request: &mut GeminiRequest,
    max_output_tokens: Option<u32>,
    reasoning_effort: Option<ReasoningEffort>,
) -> Option<u32> {
    let thinking_budget = reasoning_effort.and_then(|effort| {
        crate::providers::common::bounded_thinking_budget(effort, max_output_tokens)
    });
    if max_output_tokens.is_none() && thinking_budget.is_none() {
        return None;
    }

    let mut generation_config = serde_json::Map::new();
    if let Some(max_tokens) = max_output_tokens {
        generation_config.insert("maxOutputTokens".to_string(), json!(max_tokens));
    }
    if let Some(thinking_budget) = thinking_budget {
        generation_config.insert(
            "thinkingConfig".to_string(),
            json!({ "thinkingBudget": thinking_budget }),
        );
    }
    request.generation_config = Some(Value::Object(generation_config));
    thinking_budget
}
use bamboo_domain::ToolSchema;

/// Google Gemini API provider.
pub struct GeminiProvider {
    client: Client,
    api_key: String,
    base_url: String,
    default_reasoning_effort: Option<ReasoningEffort>,
    request_overrides: Option<RequestOverridesConfig>,
    masking_config: KeywordMaskingConfig,
}

impl GeminiProvider {
    /// Create a new Gemini provider with an API key.
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            api_key: api_key.into(),
            base_url: "https://generativelanguage.googleapis.com/v1beta".to_string(),
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

    /// Overrides the internal HTTP client (e.g., to enable a proxy).
    pub fn with_client(mut self, client: Client) -> Self {
        self.client = client;
        self
    }

    /// Set a custom base URL (e.g., for proxies or alternative endpoints).
    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into();
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
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        // Authenticate via header rather than a `?key=` URL query parameter so
        // the API key never leaks into HTTP/proxy/debug logs (issue #12).
        // A malformed/non-ASCII key surfaces as `LLMError::Auth` instead of
        // being silently dropped (which would send an unauthenticated request
        // and produce a confusing 401) — mirrors the Anthropic sibling.
        headers.insert(
            "x-goog-api-key",
            HeaderValue::from_str(&self.api_key)
                .map_err(|e| LLMError::Auth(format!("Invalid API key: {}", e)))?,
        );
        // `x-goog-api-key` is intentionally overridable: `request_overrides`
        // (applied next) may replace it for per-endpoint/operator auth overrides.
        request_overrides::apply_overrides_to_header_map(
            &mut headers,
            self.request_overrides.as_ref(),
            endpoint,
            model,
        );
        Ok(headers)
    }

    /// Build the `streamGenerateContent` URL for `model`. Auth travels via the
    /// `x-goog-api-key` header (see [`GeminiProvider::build_headers`]), so the
    /// API key never appears in this URL.
    fn stream_url(&self, model: &str) -> String {
        // Gemini returns a JSON array when `alt=sse` is omitted even though the
        // endpoint name is `streamGenerateContent`. The downstream adapter is
        // intentionally an SSE parser, so request the SSE representation
        // explicitly instead of silently producing an empty assistant turn.
        format!(
            "{}/models/{}:streamGenerateContent?alt=sse",
            self.base_url, model
        )
    }

    /// Build the `list_models` URL. Auth travels via the `x-goog-api-key`
    /// header (see [`GeminiProvider::build_headers`]), so the API key never
    /// appears in this URL.
    fn list_models_url(&self) -> String {
        format!("{}/models", self.base_url.trim_end_matches('/'))
    }

    fn thinking_budget_for_effort(
        effort: ReasoningEffort,
        max_output_tokens: Option<u32>,
    ) -> Option<u32> {
        crate::providers::common::bounded_thinking_budget(effort, max_output_tokens)
    }

    fn looks_like_reasoning_unsupported_error(status: reqwest::StatusCode, body: &str) -> bool {
        // Shared, tightened heuristic (#237 finding 5). `thinking` substring covers
        // Gemini's thinkingBudget/thinkingConfig.
        crate::providers::common::looks_like_reasoning_unsupported_error(status, body)
    }
}

#[async_trait]
impl LLMProvider for GeminiProvider {
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

    async fn chat_stream_with_options(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> Result<LLMStream> {
        tracing::debug!("Gemini provider using model: {}", model);
        let reasoning_effort = options
            .and_then(|o| o.reasoning_effort)
            .or(self.default_reasoning_effort);
        crate::providers::common::validate_max_thinking_budget(
            reasoning_effort,
            max_output_tokens,
        )?;
        let request_reasoning_effort = options.and_then(|o| o.reasoning_effort);
        let required_tool = crate::provider::required_tool_from_options(options, tools)?;
        let reasoning_source = if request_reasoning_effort.is_some() {
            "request"
        } else if self.default_reasoning_effort.is_some() {
            "provider_default"
        } else {
            "none"
        };
        let request_purpose = options
            .and_then(|o| o.request_purpose.as_deref())
            .unwrap_or("unknown");
        let session_log_id = options
            .and_then(|o| o.session_id.as_deref())
            .unwrap_or("unknown-session");
        let mut applied_reasoning_effort = reasoning_effort;
        let mut applied_thinking_budget = reasoning_effort
            .and_then(|effort| Self::thinking_budget_for_effort(effort, max_output_tokens));

        // Auth is supplied via the x-goog-api-key header (see build_headers).
        let url = self.stream_url(model);

        let build_request = |effort: Option<ReasoningEffort>| -> Result<GeminiRequest> {
            // Convert messages using the new protocol system
            let messages_vec: Vec<Message> = messages.to_vec();
            let mut request: GeminiRequest = messages_vec.to_provider()?;

            // Add tools if present
            if !tools.is_empty() {
                let tools_vec: Vec<ToolSchema> = tools.to_vec();
                request.tools = Some(tools_vec.to_provider()?);
            }

            apply_generation_config(&mut request, max_output_tokens, effort);

            Ok(request)
        };

        let request = build_request(reasoning_effort)?;
        let mut request_json = serde_json::to_value(&request).map_err(LLMError::Json)?;
        request_overrides::apply_overrides_to_body(
            &mut request_json,
            self.request_overrides.as_ref(),
            request_overrides::ENDPOINT_STREAM_GENERATE_CONTENT,
            Some(model),
        );
        apply_required_tool_choice(&mut request_json, required_tool);
        // Last-moment scan: mask every text value in the fully-assembled body.
        crate::masking::mask_outbound_body(&mut request_json, &self.masking_config);
        tracing::info!(
            "[{}] Gemini request protocol=streamGenerateContent model='{}' reasoning_effort={} reasoning_source={} request_reasoning_enabled={} thinking_budget={} max_output_tokens={} [{}]",
            session_log_id,
            model,
            reasoning_effort
                .map(ReasoningEffort::as_str)
                .unwrap_or("none"),
            reasoning_source,
            reasoning_effort.is_some(),
            applied_thinking_budget
                .map(|tokens| tokens.to_string())
                .unwrap_or_else(|| "none".to_string()),
            max_output_tokens
                .map(|tokens| tokens.to_string())
                .unwrap_or_else(|| "none".to_string()),
            request_purpose
        );
        tracing::debug!(
            "Gemini request: {}",
            serde_json::to_string_pretty(&request_json).unwrap_or_default()
        );

        let headers = self.build_headers(
            request_overrides::ENDPOINT_STREAM_GENERATE_CONTENT,
            Some(model),
        )?;
        // Retry the initial request on transient failures (issue #18); the
        // returned body is unread, so SSE streaming below is unaffected.
        let mut response = crate::retry::send_with_retry(crate::retry::global(), "Gemini", || {
            self.client
                .post(&url)
                .headers(headers.clone())
                .json(&request_json)
        })
        .await
        .map_err(LLMError::Http)?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.map_err(LLMError::Http)?;

            if reasoning_effort.is_some()
                && Self::looks_like_reasoning_unsupported_error(status, &text)
            {
                tracing::warn!(
                    "Gemini streamGenerateContent rejected reasoning for model '{}'; retrying without reasoning_effort",
                    model
                );

                let fallback_request = build_request(None)?;
                let mut fallback_request_json =
                    serde_json::to_value(&fallback_request).map_err(LLMError::Json)?;
                request_overrides::apply_overrides_to_body(
                    &mut fallback_request_json,
                    self.request_overrides.as_ref(),
                    request_overrides::ENDPOINT_STREAM_GENERATE_CONTENT,
                    Some(model),
                );
                apply_required_tool_choice(&mut fallback_request_json, required_tool);
                crate::masking::mask_outbound_body(
                    &mut fallback_request_json,
                    &self.masking_config,
                );
                applied_reasoning_effort = None;
                applied_thinking_budget = None;
                tracing::info!(
                    "Gemini request retry protocol=streamGenerateContent model='{}' reasoning_effort=none reasoning_source={} request_reasoning_enabled=false thinking_budget=none max_output_tokens={} purpose={}",
                    model,
                    reasoning_source,
                    max_output_tokens
                        .map(|tokens| tokens.to_string())
                        .unwrap_or_else(|| "none".to_string()),
                    request_purpose
                );
                let fallback_headers = self.build_headers(
                    request_overrides::ENDPOINT_STREAM_GENERATE_CONTENT,
                    Some(model),
                )?;
                response = crate::retry::send_with_retry(crate::retry::global(), "Gemini", || {
                    self.client
                        .post(&url)
                        .headers(fallback_headers.clone())
                        .json(&fallback_request_json)
                })
                .await
                .map_err(LLMError::Http)?;
            } else {
                if status == 401 || status == 403 {
                    return Err(LLMError::Auth(format!(
                        "Gemini authentication failed: {}. Please check your API key.",
                        text
                    )));
                }

                return Err(LLMError::Api(format!(
                    "Gemini API error: HTTP {}: {}",
                    status, text
                )));
            }
        }

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.map_err(LLMError::Http)?;

            if status == 401 || status == 403 {
                return Err(LLMError::Auth(format!(
                    "Gemini authentication failed: {}. Please check your API key.",
                    text
                )));
            }

            return Err(LLMError::Api(format!(
                "Gemini API error: HTTP {}: {}",
                status, text
            )));
        }

        tracing::debug!("Gemini stream started successfully");

        // Parse SSE stream with Gemini-specific parser
        // Parse SSE stream with Gemini-specific parser. Uses the multi-chunk
        // adapter: a single Gemini event can yield several chunks (a final
        // `usageMetadata` carries both a cache hit and output/thinking usage),
        // and `streamGenerateContent?alt=sse` sends no [DONE], so emitting all
        // chunks from the one event — rather than deferring — guarantees usage
        // is delivered even when the connection closes immediately afterwards.
        let mut state = GeminiStreamState::default();
        let model_for_log = model.to_string();
        let requested_reasoning_for_log = applied_reasoning_effort;
        let request_thinking_budget_for_log = applied_thinking_budget;
        let mut logged_summary = false;

        let stream = crate::providers::common::sse::llm_stream_from_sse_multi(
            response,
            move |event, data| {
                let chunks = parse_gemini_sse_event(&mut state, event, data)?;

                if !logged_summary
                    && (requested_reasoning_for_log.is_some() || state.observed_thinking_signal)
                    && chunks.iter().any(|c| matches!(c, LLMChunk::Done))
                {
                    tracing::info!(
                        "Gemini reasoning summary: model='{}' requested_effort={} request_thinking_budget={} observed_thinking_signal={} thinking_parts_count={} thinking_text_chars={}",
                        model_for_log,
                        requested_reasoning_for_log
                            .map(ReasoningEffort::as_str)
                            .unwrap_or("none"),
                        request_thinking_budget_for_log
                            .map(|tokens| tokens.to_string())
                            .unwrap_or_else(|| "none".to_string()),
                        state.observed_thinking_signal,
                        state.thinking_parts_count,
                        state.thinking_text_chars
                    );
                    logged_summary = true;
                }

                Ok(chunks)
            },
        );

        Ok(stream)
    }

    async fn list_models(&self) -> Result<Vec<String>> {
        let headers = self.build_headers(request_overrides::ENDPOINT_MODELS, None)?;
        let url = self.list_models_url();
        model_fetcher::fetch_model_list(&self.client, &url, headers, "Gemini").await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_reasoning_uses_a_distinct_larger_thinking_budget() {
        assert_eq!(
            GeminiProvider::thinking_budget_for_effort(ReasoningEffort::Xhigh, None),
            Some(8_192)
        );
        assert_eq!(
            GeminiProvider::thinking_budget_for_effort(ReasoningEffort::Max, None),
            Some(16_384)
        );
        assert_eq!(
            GeminiProvider::thinking_budget_for_effort(ReasoningEffort::Xhigh, Some(16_384)),
            Some(8_192)
        );
        assert_eq!(
            GeminiProvider::thinking_budget_for_effort(ReasoningEffort::Max, Some(16_384)),
            Some(12_288)
        );
        assert_eq!(
            GeminiProvider::thinking_budget_for_effort(ReasoningEffort::Xhigh, Some(8_320)),
            Some(4_160)
        );
        assert_eq!(
            GeminiProvider::thinking_budget_for_effort(ReasoningEffort::Max, Some(8_320)),
            Some(6_240)
        );
        assert_eq!(
            GeminiProvider::thinking_budget_for_effort(ReasoningEffort::Max, Some(1_024)),
            None
        );
    }

    #[test]
    fn max_reasoning_request_reserves_visible_output_at_the_wire() {
        let mut request: GeminiRequest = vec![Message::user("hello")]
            .to_provider()
            .expect("Gemini request");
        let applied =
            apply_generation_config(&mut request, Some(16_384), Some(ReasoningEffort::Max));
        let body = serde_json::to_value(request).expect("serialized Gemini request");

        assert_eq!(applied, Some(12_288));
        assert_eq!(body["generationConfig"]["maxOutputTokens"], 16_384);
        assert_eq!(
            body["generationConfig"]["thinkingConfig"]["thinkingBudget"],
            12_288
        );
    }

    #[test]
    fn impossible_max_reasoning_limit_omits_numeric_budget_at_the_wire() {
        let mut request: GeminiRequest = vec![Message::user("hello")]
            .to_provider()
            .expect("Gemini request");
        let applied =
            apply_generation_config(&mut request, Some(1_024), Some(ReasoningEffort::Max));
        let body = serde_json::to_value(request).expect("serialized Gemini request");

        assert_eq!(applied, None);
        assert_eq!(body["generationConfig"]["maxOutputTokens"], 1_024);
        assert!(body["generationConfig"].get("thinkingConfig").is_none());
    }

    #[test]
    fn required_tool_choice_uses_gemini_any_allowlist_shape() {
        let mut body = json!({
            "generationConfig": {
                "thinkingConfig": {"thinkingBudget": 4096}
            }
        });
        apply_required_tool_choice(&mut body, Some("load_skill"));
        assert_eq!(
            body["toolConfig"],
            json!({
                "functionCallingConfig": {
                    "mode": "ANY",
                    "allowedFunctionNames": ["load_skill"]
                }
            })
        );
        assert_eq!(
            body["generationConfig"]["thinkingConfig"]["thinkingBudget"],
            4096
        );
    }

    #[test]
    fn test_new_provider() {
        let provider = GeminiProvider::new("test_key");
        assert_eq!(provider.api_key, "test_key");
        assert_eq!(
            provider.base_url,
            "https://generativelanguage.googleapis.com/v1beta"
        );
    }

    #[test]
    fn test_with_base_url() {
        let provider =
            GeminiProvider::new("test_key").with_base_url("https://custom.googleapis.com/v1");
        assert_eq!(provider.base_url, "https://custom.googleapis.com/v1");
    }

    #[test]
    fn test_chained_builders() {
        let provider = GeminiProvider::new("test_key").with_base_url("https://custom.api.com");

        assert_eq!(provider.api_key, "test_key");
        assert_eq!(provider.base_url, "https://custom.api.com");
    }

    #[test]
    fn test_url_construction() {
        let provider =
            GeminiProvider::new("my_api_key_123").with_base_url("https://test.api.com/v1beta");

        // The API key must NOT appear in the URL; auth is sent via the
        // x-goog-api-key header (see test_build_headers_sends_api_key).
        // These assertions exercise the SAME helpers the production request
        // paths use (`stream_url` / `list_models_url`), so a regression that
        // reintroduces `?key=` in real code fails here.
        let stream_url = provider.stream_url("gemini-custom");
        assert_eq!(
            stream_url,
            "https://test.api.com/v1beta/models/gemini-custom:streamGenerateContent?alt=sse"
        );
        assert!(
            stream_url.ends_with("?alt=sse"),
            "streamGenerateContent must explicitly request SSE transport"
        );
        assert!(
            !stream_url.contains("key="),
            "API key must not be embedded in the stream URL"
        );

        let models_url = provider.list_models_url();
        assert_eq!(models_url, "https://test.api.com/v1beta/models");
        assert!(
            !models_url.contains("key="),
            "API key must not be embedded in the list_models URL"
        );
    }

    /// Header-based auth: the API key travels in `x-goog-api-key`, never in the
    /// URL query string (issue #12).
    #[test]
    fn test_build_headers_sends_api_key() {
        let provider = GeminiProvider::new("my_api_key_123");

        let stream_headers = provider
            .build_headers(request_overrides::ENDPOINT_STREAM_GENERATE_CONTENT, None)
            .expect("valid key should build headers");
        assert_eq!(
            stream_headers
                .get("x-goog-api-key")
                .expect("x-goog-api-key header should be set on streamGenerateContent requests")
                .to_str()
                .expect("valid header value"),
            "my_api_key_123"
        );

        let models_headers = provider
            .build_headers(request_overrides::ENDPOINT_MODELS, None)
            .expect("valid key should build headers");
        assert_eq!(
            models_headers
                .get("x-goog-api-key")
                .expect("x-goog-api-key header should be set on list_models requests")
                .to_str()
                .expect("valid header value"),
            "my_api_key_123"
        );
    }

    /// A malformed/non-ASCII key (e.g. one carrying a trailing newline) must
    /// surface as `LLMError::Auth` instead of being silently dropped to an
    /// unauthenticated request (which would otherwise produce a confusing 401).
    #[test]
    fn test_build_headers_rejects_invalid_api_key() {
        let provider = GeminiProvider::new("bad\nkey");
        let result =
            provider.build_headers(request_overrides::ENDPOINT_STREAM_GENERATE_CONTENT, None);
        assert!(
            matches!(result, Err(LLMError::Auth(_))),
            "expected LLMError::Auth for invalid key"
        );
    }

    // ========== MODEL REQUIREMENT ARCHITECTURE TESTS ==========
    // These tests ensure the design principle:
    // "Provider must not have a default model field or with_model() method"

    /// Test: GeminiProvider does NOT have a model field
    #[test]
    fn gemini_provider_has_no_model_field() {
        // This test documents the provider structure:
        // pub struct GeminiProvider {
        //     client: Client,
        //     api_key: String,
        //     base_url: String,
        //     // NO model field!
        // }
        //
        // If someone adds a model field, this test should be updated
        // to reflect the architecture change.
        let provider = GeminiProvider::new("test_key");
        // Verify we can access known fields
        assert_eq!(provider.api_key, "test_key");
        assert_eq!(
            provider.base_url,
            "https://generativelanguage.googleapis.com/v1beta"
        );
        // There is NO provider.model field to access
    }

    /// Test: GeminiProvider does NOT have with_model() method
    #[test]
    fn gemini_provider_has_no_with_model_method() {
        let provider = GeminiProvider::new("test_key");

        // Available builder method:
        let provider = provider.with_base_url("https://custom.api.com");

        // There is NO .with_model("gemini-pro") method
        // Model is passed to chat_stream() as a parameter

        assert_eq!(provider.base_url, "https://custom.api.com");
    }
}
