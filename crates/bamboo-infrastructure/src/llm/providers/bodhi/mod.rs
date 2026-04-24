//! Bodhi proxy provider.
//!
//! Routes LLM requests through a bodhi-server instance, which injects the real
//! API key before forwarding to the actual provider.  This keeps raw provider
//! credentials off the client.

use async_trait::async_trait;
use reqwest::{
    header::{HeaderMap, HeaderValue, AUTHORIZATION},
    Client,
};
use serde_json::json;

use crate::llm::provider::{LLMError, LLMProvider, LLMRequestOptions, LLMStream, Result};
use crate::llm::providers::common::openai_compat::{
    build_openai_compat_body, parse_openai_compat_sse_data_strict,
};
use crate::llm::providers::common::sse::llm_stream_from_sse;
use crate::llm::types::LLMChunk;
use bamboo_domain::{Message, ReasoningEffort, ToolSchema};

const DEFAULT_MAX_TOKENS: u32 = 16384;

pub struct BodhiProvider {
    client: Client,
    api_key: String,
    base_url: String,
    target_provider: String,
    default_reasoning_effort: Option<ReasoningEffort>,
}

impl BodhiProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            api_key: api_key.into(),
            base_url: "http://localhost:8080".to_string(),
            target_provider: "openai".to_string(),
            default_reasoning_effort: None,
        }
    }

    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into();
        self
    }

    pub fn with_client(mut self, client: Client) -> Self {
        self.client = client;
        self
    }

    pub fn with_target_provider(mut self, provider: impl Into<String>) -> Self {
        self.target_provider = provider.into();
        self
    }

    pub fn with_reasoning_effort(mut self, effort: Option<ReasoningEffort>) -> Self {
        self.default_reasoning_effort = effort;
        self
    }

    fn build_headers(&self) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", self.api_key))
                .map_err(|e| LLMError::Auth(format!("Invalid bodhi API key: {}", e)))?,
        );
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        Ok(headers)
    }

    fn proxy_url(&self, suffix: &str) -> String {
        let base = self.base_url.trim_end_matches('/');
        format!("{}/proxy/{}/{}", base, self.target_provider, suffix)
    }
}

#[async_trait]
impl LLMProvider for BodhiProvider {
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
        let reasoning_effort = options
            .and_then(|o| o.reasoning_effort)
            .or(self.default_reasoning_effort);
        let parallel_tool_calls = options.and_then(|o| o.parallel_tool_calls);

        tracing::info!(
            "Bodhi proxy request target={} model='{}'",
            self.target_provider,
            model
        );

        match self.target_provider.as_str() {
            "openai" => {
                self.proxy_openai(
                    messages,
                    tools,
                    max_output_tokens,
                    model,
                    reasoning_effort,
                    parallel_tool_calls,
                )
                .await
            }
            "anthropic" => {
                self.proxy_anthropic(messages, tools, max_output_tokens, model, reasoning_effort)
                    .await
            }
            "gemini" => {
                self.proxy_gemini(messages, tools, max_output_tokens, model, reasoning_effort)
                    .await
            }
            other => Err(LLMError::Auth(format!(
                "Unknown bodhi target provider: {}",
                other
            ))),
        }
    }

    async fn list_models(&self) -> Result<Vec<String>> {
        Ok(vec![])
    }

    async fn list_model_info(&self) -> Result<Vec<crate::llm::provider::ProviderModelInfo>> {
        Ok(vec![])
    }
}

impl BodhiProvider {
    async fn proxy_openai(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
        reasoning_effort: Option<ReasoningEffort>,
        parallel_tool_calls: Option<bool>,
    ) -> Result<LLMStream> {
        let body = build_openai_compat_body(
            model,
            messages,
            tools,
            None,
            max_output_tokens,
            reasoning_effort,
            parallel_tool_calls,
        );

        let headers = self.build_headers()?;
        let url = self.proxy_url("v1/chat/completions");

        let response = self
            .client
            .post(&url)
            .headers(headers)
            .json(&body)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await?;
            return Err(LLMError::Api(format!(
                "Bodhi/OpenAI proxy HTTP {}: {}",
                status, text
            )));
        }

        let stream = llm_stream_from_sse(response, |_event, data| {
            if data.trim().is_empty() {
                return Ok(None);
            }
            let chunk = parse_openai_compat_sse_data_strict(data)?;
            match chunk {
                LLMChunk::Done => Ok(Some(LLMChunk::Done)),
                other => Ok(Some(other)),
            }
        });

        Ok(stream)
    }

    async fn proxy_anthropic(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
        reasoning_effort: Option<ReasoningEffort>,
    ) -> Result<LLMStream> {
        use crate::llm::providers::anthropic::{
            build_anthropic_request, parse_anthropic_sse_event, AnthropicStreamState,
        };

        let max_tokens = max_output_tokens.unwrap_or(DEFAULT_MAX_TOKENS);

        let body = build_anthropic_request(
            messages,
            tools,
            model,
            max_tokens,
            true,
            reasoning_effort,
            None,
        );

        let headers = self.build_headers()?;
        let url = self.proxy_url("v1/messages");

        let response = self
            .client
            .post(&url)
            .headers(headers)
            .json(&body)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await?;
            return Err(LLMError::Api(format!(
                "Bodhi/Anthropic proxy HTTP {}: {}",
                status, text
            )));
        }

        let mut state = AnthropicStreamState::default();
        let stream = llm_stream_from_sse(response, move |event, data| {
            parse_anthropic_sse_event(&mut state, event, data)
        });

        Ok(stream)
    }

    async fn proxy_gemini(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
        reasoning_effort: Option<ReasoningEffort>,
    ) -> Result<LLMStream> {
        use crate::llm::protocol::gemini::GeminiRequest;
        use crate::llm::protocol::ToProvider;
        use crate::llm::providers::gemini::{parse_gemini_sse_event, GeminiStreamState};

        let messages_vec: Vec<Message> = messages.to_vec();
        let mut request: GeminiRequest = messages_vec.to_provider()?;

        if !tools.is_empty() {
            let tools_vec: Vec<ToolSchema> = tools.to_vec();
            request.tools = Some(tools_vec.to_provider()?);
        }

        if max_output_tokens.is_some()
            || reasoning_effort.and_then(Self::thinking_budget_for_effort).is_some()
        {
            let mut generation_config = serde_json::Map::new();
            if let Some(max_tokens) = max_output_tokens {
                generation_config.insert("maxOutputTokens".to_string(), json!(max_tokens));
            }
            if let Some(thinking_budget) = reasoning_effort.and_then(Self::thinking_budget_for_effort) {
                generation_config.insert(
                    "thinkingConfig".to_string(),
                    json!({ "thinkingBudget": thinking_budget }),
                );
            }
            request.generation_config = Some(serde_json::Value::Object(generation_config));
        }

        let headers = self.build_headers()?;
        let url = self.proxy_url(&format!(
            "v1beta/models/{}:streamGenerateContent",
            model
        ));

        let response = self
            .client
            .post(&url)
            .headers(headers)
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await?;
            return Err(LLMError::Api(format!(
                "Bodhi/Gemini proxy HTTP {}: {}",
                status, text
            )));
        }

        let mut state = GeminiStreamState::default();
        let stream = llm_stream_from_sse(response, move |event, data| {
            parse_gemini_sse_event(&mut state, event, data)
        });

        Ok(stream)
    }

    fn thinking_budget_for_effort(effort: ReasoningEffort) -> Option<u32> {
        match effort {
            ReasoningEffort::Low => None,
            ReasoningEffort::Medium => Some(1024),
            ReasoningEffort::High => Some(4096),
            ReasoningEffort::Xhigh | ReasoningEffort::Max => Some(8192),
        }
    }
}
