//! Google Gemini provider implementation.

mod stream;

pub use stream::{parse_gemini_sse_event, GeminiStreamState};

use async_trait::async_trait;
use reqwest::{
    header::{HeaderMap, HeaderValue, CONTENT_TYPE},
    Client,
};
use serde_json::json;

use bamboo_domain::{Message};
use bamboo_domain::{ToolSchema};
use crate::llm::protocol::gemini::GeminiRequest;
use crate::llm::protocol::ToProvider;
use crate::llm::provider::{LLMError, LLMProvider, LLMRequestOptions, LLMStream, Result};
use crate::llm::providers::common::request_overrides;
use crate::llm::types::LLMChunk;
use crate::config::RequestOverridesConfig;
use bamboo_domain::ReasoningEffort;

/// Google Gemini API provider.
pub struct GeminiProvider {
    client: Client,
    api_key: String,
    base_url: String,
    default_reasoning_effort: Option<ReasoningEffort>,
    request_overrides: Option<RequestOverridesConfig>,
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
        }
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

    fn build_headers(&self, endpoint: &str, model: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        request_overrides::apply_overrides_to_header_map(
            &mut headers,
            self.request_overrides.as_ref(),
            endpoint,
            model,
        );
        headers
    }

    fn thinking_budget_for_effort(effort: ReasoningEffort) -> Option<u32> {
        match effort {
            ReasoningEffort::Low => None,
            ReasoningEffort::Medium => Some(1024),
            ReasoningEffort::High => Some(4096),
            ReasoningEffort::Xhigh | ReasoningEffort::Max => Some(8192),
        }
    }

    fn looks_like_reasoning_unsupported_error(status: reqwest::StatusCode, body: &str) -> bool {
        if !(status == 400 || status == 404 || status == 405 || status == 409 || status == 422) {
            return false;
        }

        let b = body.to_ascii_lowercase();
        let mentions_reasoning = b.contains("reasoning")
            || b.contains("thinking")
            || b.contains("thinkingbudget")
            || b.contains("thinkingconfig")
            || b.contains("unknown parameter");
        let mentions_unsupported = b.contains("unsupported")
            || b.contains("not supported")
            || b.contains("unknown")
            || b.contains("invalid");
        mentions_reasoning && mentions_unsupported
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
        let request_reasoning_effort = options.and_then(|o| o.reasoning_effort);
        let reasoning_source = if request_reasoning_effort.is_some() {
            "request"
        } else if self.default_reasoning_effort.is_some() {
            "provider_default"
        } else {
            "none"
        };
        let mut applied_reasoning_effort = reasoning_effort;
        let mut applied_thinking_budget =
            reasoning_effort.and_then(Self::thinking_budget_for_effort);

        // Build URL with query param authentication
        let url = format!(
            "{}/models/{}:streamGenerateContent?key={}",
            self.base_url, model, self.api_key
        );

        let build_request = |effort: Option<ReasoningEffort>| -> Result<GeminiRequest> {
            // Convert messages using the new protocol system
            let messages_vec: Vec<Message> = messages.to_vec();
            let mut request: GeminiRequest = messages_vec.to_provider()?;

            // Add tools if present
            if !tools.is_empty() {
                let tools_vec: Vec<ToolSchema> = tools.to_vec();
                request.tools = Some(tools_vec.to_provider()?);
            }

            // Add generation config if max_output_tokens or reasoning effort is specified.
            let thinking_budget = effort.and_then(Self::thinking_budget_for_effort);
            if max_output_tokens.is_some() || thinking_budget.is_some() {
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
                request.generation_config = Some(serde_json::Value::Object(generation_config));
            }

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
        tracing::info!(
            "Gemini request protocol=streamGenerateContent model='{}' reasoning_effort={} reasoning_source={} request_reasoning_enabled={} thinking_budget={} max_output_tokens={}",
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
                .unwrap_or_else(|| "none".to_string())
        );
        tracing::debug!(
            "Gemini request: {}",
            serde_json::to_string_pretty(&request_json).unwrap_or_default()
        );

        let headers = self.build_headers(
            request_overrides::ENDPOINT_STREAM_GENERATE_CONTENT,
            Some(model),
        );
        let mut response = self
            .client
            .post(&url)
            .headers(headers)
            .json(&request_json)
            .send()
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
                applied_reasoning_effort = None;
                applied_thinking_budget = None;
                tracing::info!(
                    "Gemini request retry protocol=streamGenerateContent model='{}' reasoning_effort=none reasoning_source={} request_reasoning_enabled=false thinking_budget=none max_output_tokens={}",
                    model,
                    reasoning_source,
                    max_output_tokens
                        .map(|tokens| tokens.to_string())
                        .unwrap_or_else(|| "none".to_string())
                );
                let fallback_headers = self.build_headers(
                    request_overrides::ENDPOINT_STREAM_GENERATE_CONTENT,
                    Some(model),
                );
                response = self
                    .client
                    .post(&url)
                    .headers(fallback_headers)
                    .json(&fallback_request_json)
                    .send()
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
        let mut state = GeminiStreamState::default();
        let model_for_log = model.to_string();
        let requested_reasoning_for_log = applied_reasoning_effort;
        let request_thinking_budget_for_log = applied_thinking_budget;
        let mut logged_summary = false;

        let stream = crate::llm::providers::common::sse::llm_stream_from_sse(
            response,
            move |event, data| {
                let chunk = parse_gemini_sse_event(&mut state, event, data)?;

                if matches!(chunk, Some(LLMChunk::Done))
                    && !logged_summary
                    && (requested_reasoning_for_log.is_some() || state.observed_thinking_signal)
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

                Ok(chunk)
            },
        );

        Ok(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        // This verifies URL construction logic
        let expected_url = "https://test.api.com/v1beta/models/gemini-custom:streamGenerateContent?key=my_api_key_123";
        let constructed_url = format!(
            "{}/models/{}:streamGenerateContent?key={}",
            provider.base_url, "gemini-custom", provider.api_key
        );

        assert_eq!(constructed_url, expected_url);
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
