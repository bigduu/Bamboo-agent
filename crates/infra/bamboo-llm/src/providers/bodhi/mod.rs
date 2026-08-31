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
use serde_json::{json, Value};

use crate::protocol::ToProvider;
use crate::provider::{
    LLMError, LLMProvider, LLMRequestOptions, LLMStream, ProviderVisibleToolFootprint,
    ProviderVisibleToolSegment, ProviderVisibleToolSegmentKind, Result,
};
use crate::providers::common::model_fetcher;
use crate::providers::common::openai_compat::{
    build_openai_compat_body, openai_compat_chat_stream_from_sse,
    parse_openai_compat_sse_data_strict_multi, tools_to_openai_compat_json,
};
use crate::providers::common::sse::llm_stream_from_sse;
use bamboo_config::KeywordMaskingConfig;
use bamboo_domain::{Message, ReasoningEffort, ToolSchema};

const DEFAULT_MAX_TOKENS: u32 = 16384;

pub struct BodhiProvider {
    client: Client,
    api_key: String,
    base_url: String,
    target_provider: String,
    default_reasoning_effort: Option<ReasoningEffort>,
    masking_config: KeywordMaskingConfig,
}

impl BodhiProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            api_key: api_key.into(),
            base_url: "http://localhost:8080".to_string(),
            target_provider: "openai".to_string(),
            default_reasoning_effort: None,
            masking_config: KeywordMaskingConfig::default(),
        }
    }

    /// Configure keyword masking applied as a last-moment scan of every outbound
    /// request body (see [`crate::masking`]).
    pub fn with_masking(mut self, masking_config: KeywordMaskingConfig) -> Self {
        self.masking_config = masking_config;
        self
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
    async fn provider_visible_tool_footprint(
        &self,
        _ir: &crate::prompt_ir::PromptIR,
        tools: &[ToolSchema],
        _model: &str,
        _required_tool: Option<&str>,
    ) -> Result<ProviderVisibleToolFootprint> {
        let projected: Vec<Value> = match self.target_provider.as_str() {
            "openai" => tools_to_openai_compat_json(tools),
            "anthropic" => crate::providers::anthropic::tools_to_anthropic_json(
                tools,
                bamboo_domain::CapabilityLoadingMode::LegacyFullCatalog,
            ),
            "gemini" => {
                let tools: Vec<crate::protocol::gemini::GeminiTool> =
                    tools.to_vec().to_provider()?;
                tools
                    .into_iter()
                    .map(serde_json::to_value)
                    .collect::<std::result::Result<_, _>>()?
            }
            other => {
                return Err(LLMError::Auth(format!(
                    "Unknown bodhi target provider: {other}"
                )))
            }
        };
        if projected.is_empty() {
            return Ok(ProviderVisibleToolFootprint::default());
        }
        Ok(ProviderVisibleToolFootprint {
            segments: vec![ProviderVisibleToolSegment::from_serializable(
                ProviderVisibleToolSegmentKind::InitialFullDefinition,
                &projected,
            )?],
        })
    }

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
        let required_tool = crate::provider::required_tool_from_options(options, tools)?;
        let request_purpose = options
            .and_then(|o| o.request_purpose.as_deref())
            .unwrap_or("unknown");
        let session_log_id = options
            .and_then(|o| o.session_id.as_deref())
            .unwrap_or("unknown-session");

        tracing::info!(
            "[{}] Bodhi proxy request target={} model='{}' [{}]",
            session_log_id,
            self.target_provider,
            model,
            request_purpose
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
                    required_tool,
                )
                .await
            }
            "anthropic" => {
                self.proxy_anthropic(
                    messages,
                    tools,
                    max_output_tokens,
                    model,
                    reasoning_effort,
                    required_tool,
                )
                .await
            }
            "gemini" => {
                crate::providers::common::validate_max_thinking_budget(
                    reasoning_effort,
                    max_output_tokens,
                )?;
                self.proxy_gemini(
                    messages,
                    tools,
                    max_output_tokens,
                    model,
                    reasoning_effort,
                    required_tool,
                )
                .await
            }
            other => Err(LLMError::Auth(format!(
                "Unknown bodhi target provider: {}",
                other
            ))),
        }
    }

    async fn list_models(&self) -> Result<Vec<String>> {
        let url = self.proxy_url("v1/models");
        let headers = self.build_headers()?;

        // Try to fetch models through the bodhi proxy.
        // If the bodhi server doesn't support this endpoint, return empty gracefully.
        match model_fetcher::fetch_model_list(&self.client, &url, headers, "Bodhi").await {
            Ok(models) => Ok(models),
            Err(e) => {
                tracing::debug!("Bodhi proxy models endpoint not available: {}", e);
                Ok(vec![])
            }
        }
    }

    async fn list_model_info(&self) -> Result<Vec<crate::provider::ProviderModelInfo>> {
        Ok(vec![])
    }
}

impl BodhiProvider {
    #[allow(clippy::too_many_arguments)]
    async fn proxy_openai(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
        reasoning_effort: Option<ReasoningEffort>,
        parallel_tool_calls: Option<bool>,
        required_tool: Option<&str>,
    ) -> Result<LLMStream> {
        let mut body = build_openai_compat_body(
            model,
            messages,
            tools,
            required_tool.map(|name| json!({"type": "function", "function": {"name": name}})),
            max_output_tokens,
            reasoning_effort,
            parallel_tool_calls,
        );
        // Last-moment scan: mask every text value in the fully-assembled body.
        crate::masking::mask_outbound_body(&mut body, &self.masking_config);

        let headers = self.build_headers()?;
        let url = self.proxy_url("v1/chat/completions");

        // Retry the initial request on transient failures (issue #18); the
        // returned body is unread, so SSE streaming below is unaffected.
        let response = crate::retry::send_with_retry(crate::retry::global(), "Bodhi", || {
            self.client.post(&url).headers(headers.clone()).json(&body)
        })
        .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await?;
            return Err(LLMError::Api(format!(
                "Bodhi/OpenAI proxy HTTP {}: {}",
                status, text
            )));
        }

        let stream = openai_compat_chat_stream_from_sse(response, |_event, data| {
            if data.trim().is_empty() {
                return Ok(Vec::new());
            }
            parse_openai_compat_sse_data_strict_multi(data)
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
        required_tool: Option<&str>,
    ) -> Result<LLMStream> {
        use crate::providers::anthropic::{
            apply_required_tool_auto_fallback, apply_required_tool_choice, build_anthropic_request,
            looks_like_thinking_forced_tool_choice_error, parse_anthropic_sse_event,
            reasoning_effort_for_budget_validation, reasoning_effort_for_required_tool,
            AnthropicStreamState,
        };

        let max_tokens = max_output_tokens.unwrap_or(DEFAULT_MAX_TOKENS);
        let reasoning_effort = reasoning_effort_for_required_tool(reasoning_effort, required_tool);
        let budget_reasoning_effort =
            reasoning_effort_for_budget_validation(reasoning_effort, messages, false, &[]);
        crate::providers::common::validate_max_thinking_budget(
            budget_reasoning_effort,
            Some(max_tokens),
        )?;

        let mut body = build_anthropic_request(
            messages,
            tools,
            model,
            max_tokens,
            true,
            reasoning_effort,
            None,
        );
        apply_required_tool_choice(&mut body, required_tool);
        crate::masking::mask_outbound_body(&mut body, &self.masking_config);

        let headers = self.build_headers()?;
        let url = self.proxy_url("v1/messages");

        // Retry the initial request on transient failures (issue #18); the
        // returned body is unread, so SSE streaming below is unaffected.
        let mut response = crate::retry::send_with_retry(crate::retry::global(), "Bodhi", || {
            self.client.post(&url).headers(headers.clone()).json(&body)
        })
        .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await?;
            if required_tool.is_some()
                && looks_like_thinking_forced_tool_choice_error(status, &text)
            {
                tracing::warn!(
                    "Bodhi/Anthropic model '{}' rejected forced named tool_choice in thinking mode; retrying activation with tool_choice=auto and parallel tool use disabled",
                    model
                );
                let mut fallback_body = build_anthropic_request(
                    messages,
                    tools,
                    model,
                    max_tokens,
                    true,
                    None,
                    Some(false),
                );
                apply_required_tool_auto_fallback(&mut fallback_body, required_tool);
                crate::masking::mask_outbound_body(&mut fallback_body, &self.masking_config);
                response = crate::retry::send_with_retry(crate::retry::global(), "Bodhi", || {
                    self.client
                        .post(&url)
                        .headers(headers.clone())
                        .json(&fallback_body)
                })
                .await?;
                if !response.status().is_success() {
                    let fallback_status = response.status();
                    let fallback_text = response.text().await?;
                    return Err(LLMError::Api(format!(
                        "Bodhi/Anthropic proxy after tool_choice=auto activation fallback HTTP {}: {}",
                        fallback_status, fallback_text
                    )));
                }
            } else {
                return Err(LLMError::Api(format!(
                    "Bodhi/Anthropic proxy HTTP {}: {}",
                    status, text
                )));
            }
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
        required_tool: Option<&str>,
    ) -> Result<LLMStream> {
        use crate::protocol::gemini::GeminiRequest;
        use crate::protocol::ToProvider;
        use crate::providers::gemini::{
            apply_generation_config, apply_required_tool_choice, parse_gemini_sse_event,
            GeminiStreamState,
        };

        let messages_vec: Vec<Message> = messages.to_vec();
        let mut request: GeminiRequest = messages_vec.to_provider()?;

        if !tools.is_empty() {
            let tools_vec: Vec<ToolSchema> = tools.to_vec();
            request.tools = Some(tools_vec.to_provider()?);
        }

        apply_generation_config(&mut request, max_output_tokens, reasoning_effort);

        // Serialize then run the last-moment scan over the body Value before send.
        let mut request_json = serde_json::to_value(&request).map_err(LLMError::Json)?;
        apply_required_tool_choice(&mut request_json, required_tool);
        crate::masking::mask_outbound_body(&mut request_json, &self.masking_config);

        let headers = self.build_headers()?;
        let url = self.proxy_url(&format!(
            "v1beta/models/{}:streamGenerateContent?alt=sse",
            model
        ));

        // Retry the initial request on transient failures (issue #18); the
        // returned body is unread, so SSE streaming below is unaffected.
        let response = crate::retry::send_with_retry(crate::retry::global(), "Bodhi", || {
            self.client
                .post(&url)
                .headers(headers.clone())
                .json(&request_json)
        })
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
        // Multi-chunk adapter: a final Gemini `usageMetadata` carries both a
        // cache hit and output/thinking usage in one event, and the stream sends
        // no [DONE], so both must be emitted from that single event (issue #27).
        let stream = crate::providers::common::sse::llm_stream_from_sse_multi(
            response,
            move |event, data| parse_gemini_sse_event(&mut state, event, data),
        );

        Ok(stream)
    }

    #[cfg(test)]
    fn thinking_budget_for_effort(
        effort: ReasoningEffort,
        max_output_tokens: Option<u32>,
    ) -> Option<u32> {
        crate::providers::common::bounded_thinking_budget(effort, max_output_tokens)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::LLMChunk;
    use bamboo_domain::FunctionSchema;
    use futures::StreamExt;
    use serde_json::Value;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

    #[test]
    fn max_reasoning_uses_a_distinct_larger_gemini_thinking_budget() {
        assert_eq!(
            BodhiProvider::thinking_budget_for_effort(ReasoningEffort::Xhigh, None),
            Some(8_192)
        );
        assert_eq!(
            BodhiProvider::thinking_budget_for_effort(ReasoningEffort::Max, None),
            Some(16_384)
        );
        assert_eq!(
            BodhiProvider::thinking_budget_for_effort(ReasoningEffort::Xhigh, Some(16_384)),
            Some(8_192)
        );
        assert_eq!(
            BodhiProvider::thinking_budget_for_effort(ReasoningEffort::Max, Some(16_384)),
            Some(12_288)
        );
        assert_eq!(
            BodhiProvider::thinking_budget_for_effort(ReasoningEffort::Xhigh, Some(8_320)),
            Some(4_160)
        );
        assert_eq!(
            BodhiProvider::thinking_budget_for_effort(ReasoningEffort::Max, Some(8_320)),
            Some(6_240)
        );
        assert_eq!(
            BodhiProvider::thinking_budget_for_effort(ReasoningEffort::Max, Some(1_024)),
            None
        );
    }

    struct ThinkingToolChoiceResponder;

    impl Respond for ThinkingToolChoiceResponder {
        fn respond(&self, request: &Request) -> ResponseTemplate {
            let body: Value = serde_json::from_slice(&request.body).expect("JSON request body");
            if body["tool_choice"]["type"] == "tool" {
                ResponseTemplate::new(400).set_body_string(
                    r#"{"error":{"message":"Thinking mode does not support this tool_choice"}}"#,
                )
            } else {
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
            }
        }
    }

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

    #[tokio::test]
    async fn tool_footprint_matches_each_bodhi_target_lowering() {
        let tools = vec![load_skill_tool()];
        let ir = crate::prompt_ir::PromptIR::default();

        let openai = BodhiProvider::new("k")
            .provider_visible_tool_footprint(&ir, &tools, "model", None)
            .await
            .unwrap();
        assert_eq!(
            openai.segments[0].serialized,
            serde_json::to_string(&tools_to_openai_compat_json(&tools)).unwrap()
        );

        let anthropic = BodhiProvider::new("k")
            .with_target_provider("anthropic")
            .provider_visible_tool_footprint(&ir, &tools, "model", None)
            .await
            .unwrap();
        assert_eq!(
            anthropic.segments[0].serialized,
            serde_json::to_string(&crate::providers::anthropic::tools_to_anthropic_json(
                &tools,
                bamboo_domain::CapabilityLoadingMode::LegacyFullCatalog,
            ))
            .unwrap()
        );

        let gemini = BodhiProvider::new("k")
            .with_target_provider("gemini")
            .provider_visible_tool_footprint(&ir, &tools, "model", None)
            .await
            .unwrap();
        let gemini: Value = serde_json::from_str(&gemini.segments[0].serialized).unwrap();
        assert_eq!(gemini[0]["functionDeclarations"][0]["name"], "load_skill");
        assert!(gemini[0]["functionDeclarations"][0]
            .get("parametersJsonSchema")
            .is_some());

        let error = BodhiProvider::new("k")
            .with_target_provider("unknown")
            .provider_visible_tool_footprint(&ir, &tools, "model", None)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("Unknown bodhi target provider"));
    }

    fn unsigned_tool_loop_messages() -> Vec<Message> {
        vec![
            Message::user("run a tool"),
            Message::assistant_with_reasoning(
                "",
                Some(vec![bamboo_domain::ToolCall {
                    id: "call_1".to_string(),
                    tool_type: "function".to_string(),
                    function: bamboo_domain::FunctionCall {
                        name: "search".to_string(),
                        arguments: r#"{"q":"test"}"#.to_string(),
                    },
                }]),
                Some("Foreign unsigned reasoning.".to_string()),
            ),
            Message::tool_result("call_1", r#"{"ok":true}"#),
        ]
    }

    #[tokio::test]
    async fn openai_proxy_stream_preserves_same_frame_text_and_usage() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/proxy/openai/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(
                        concat!(
                            "data: {\"choices\":[{\"delta\":{\"content\":\"answer\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":1000,\"completion_tokens\":120,\"prompt_tokens_details\":{\"cached_tokens\":768}}}\n",
                            "\n",
                        ),
                    ),
            )
            .expect(1)
            .mount(&server)
            .await;
        let provider = BodhiProvider::new("test-key").with_base_url(server.uri());

        let mut stream = provider
            .chat_stream(&[Message::user("hello")], &[], None, "gpt-4o")
            .await
            .expect("Bodhi OpenAI proxy stream");
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
                cache_read_input_tokens: Some(768),
                ..
            }
        ));
        assert!(matches!(chunks[2], LLMChunk::Done));
        assert_eq!(
            chunks
                .iter()
                .filter(|chunk| matches!(chunk, LLMChunk::Done))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn anthropic_proxy_retries_exact_thinking_error_with_auto_choice() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/proxy/anthropic/v1/messages"))
            .respond_with(ThinkingToolChoiceResponder)
            .expect(2)
            .mount(&server)
            .await;
        let provider = BodhiProvider::new("test-key")
            .with_base_url(server.uri())
            .with_target_provider("anthropic")
            .with_reasoning_effort(Some(ReasoningEffort::High));
        let tools = vec![load_skill_tool()];
        let options = LLMRequestOptions {
            required_tool: Some("load_skill".to_string()),
            parallel_tool_calls: Some(false),
            ..Default::default()
        };

        let _stream = provider
            .chat_stream_with_options(
                &[Message::user("activate")],
                &tools,
                Some(8192),
                "deepseek-v4-pro",
                Some(&options),
            )
            .await
            .expect("Bodhi Anthropic proxy should retry with auto choice");

        let requests = server.received_requests().await.expect("requests recorded");
        assert_eq!(requests.len(), 2);
        let named: Value = serde_json::from_slice(&requests[0].body).unwrap();
        let fallback: Value = serde_json::from_slice(&requests[1].body).unwrap();
        assert_eq!(named["tool_choice"]["type"], "tool");
        assert_eq!(named["tool_choice"]["name"], "load_skill");
        assert_eq!(fallback["tool_choice"]["type"], "auto");
        assert_eq!(fallback["tool_choice"]["disable_parallel_tool_use"], true);
        assert_eq!(fallback["tools"].as_array().map(Vec::len), Some(1));
        assert_eq!(fallback["tools"][0]["name"], "load_skill");
    }

    #[tokio::test]
    async fn anthropic_required_tool_disables_max_before_small_budget_validation() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/proxy/anthropic/v1/messages"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"),
            )
            .expect(1)
            .mount(&server)
            .await;
        let provider = BodhiProvider::new("test-key")
            .with_base_url(server.uri())
            .with_target_provider("anthropic")
            .with_reasoning_effort(Some(ReasoningEffort::Max));
        let tools = vec![load_skill_tool()];
        let options = LLMRequestOptions {
            required_tool: Some("load_skill".to_string()),
            ..Default::default()
        };

        let _stream = provider
            .chat_stream_with_options(
                &[Message::user("activate")],
                &tools,
                Some(2_048),
                "claude-sonnet-4-5",
                Some(&options),
            )
            .await
            .expect("required tool should disable thinking before Max budget validation");

        let requests = server.received_requests().await.expect("requests recorded");
        assert_eq!(requests.len(), 1);
        let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert!(body.get("thinking").is_none());
        assert_eq!(body["max_tokens"], 2_048);
        assert_eq!(body["tool_choice"]["type"], "tool");
        assert_eq!(body["tool_choice"]["name"], "load_skill");
    }

    #[tokio::test]
    async fn anthropic_max_without_required_tool_rejects_impossible_small_budget() {
        let server = MockServer::start().await;
        let provider = BodhiProvider::new("test-key")
            .with_base_url(server.uri())
            .with_target_provider("anthropic")
            .with_reasoning_effort(Some(ReasoningEffort::Max));

        let result = provider
            .chat_stream(
                &[Message::user("hello")],
                &[],
                Some(2_048),
                "claude-sonnet-4-5",
            )
            .await;

        match result {
            Err(LLMError::Api(message)) => {
                assert!(message.contains("requires max_output_tokens of at least 2049"));
            }
            Err(other) => panic!("unexpected error: {other}"),
            Ok(_) => panic!("impossible Max budget should fail before the request"),
        }
        assert!(server
            .received_requests()
            .await
            .expect("requests recorded")
            .is_empty());
    }

    #[tokio::test]
    async fn anthropic_unsigned_tool_turn_disables_max_before_small_budget_validation() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/proxy/anthropic/v1/messages"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"),
            )
            .expect(1)
            .mount(&server)
            .await;
        let provider = BodhiProvider::new("test-key")
            .with_base_url(server.uri())
            .with_target_provider("anthropic")
            .with_reasoning_effort(Some(ReasoningEffort::Max));

        let _stream = provider
            .chat_stream(
                &unsigned_tool_loop_messages(),
                &[],
                Some(2_048),
                "claude-sonnet-4-5",
            )
            .await
            .expect("unsigned tool turn should disable thinking before Max validation");

        let requests = server.received_requests().await.expect("requests recorded");
        assert_eq!(requests.len(), 1);
        let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert!(body.get("thinking").is_none());
        assert_eq!(body["max_tokens"], 2_048);
    }
}
