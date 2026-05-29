//! Anthropic provider and request-building helpers.

pub mod api_types;
pub mod conversion;
pub mod stream;

// Re-export commonly used types
pub use api_types::*;
pub use conversion::{
    convert_complete_request, convert_complete_response, convert_messages_request,
    convert_messages_response, format_model_display_name,
};
pub use stream::{
    format_sse_data, format_sse_event, map_completion_stream_chunk, AnthropicStreamAdapter,
};

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use bamboo_domain::ToolSchema;
use bamboo_domain::{Message, MessagePart, Role};
use reqwest::{header::HeaderMap, Client};
use serde_json::{json, Value};

use crate::config::RequestOverridesConfig;
use crate::llm::provider::LLMRequestOptions;
use crate::llm::provider::{LLMError, LLMProvider, LLMStream, Result};
use crate::llm::providers::common::model_fetcher;
use crate::llm::providers::common::request_overrides;
use crate::llm::types::LLMChunk;
use bamboo_domain::ReasoningEffort;

/// Anthropic Messages API provider.
pub struct AnthropicProvider {
    client: Client,
    api_key: String,
    base_url: String,
    max_tokens: u32,
    default_reasoning_effort: Option<ReasoningEffort>,
    request_overrides: Option<RequestOverridesConfig>,
}

impl AnthropicProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            api_key: api_key.into(),
            base_url: "https://api.anthropic.com/v1".to_string(),
            max_tokens: 1024,
            default_reasoning_effort: None,
            request_overrides: None,
        }
    }

    /// Overrides the internal HTTP client (e.g., to enable a proxy).
    pub fn with_client(mut self, client: Client) -> Self {
        self.client = client;
        self
    }

    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into();
        self
    }

    pub fn with_max_tokens(mut self, max_tokens: u32) -> Self {
        self.max_tokens = max_tokens;
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
        use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-api-key",
            HeaderValue::from_str(&self.api_key)
                .map_err(|e| LLMError::Auth(format!("Invalid API key: {}", e)))?,
        );
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        request_overrides::apply_overrides_to_header_map(
            &mut headers,
            self.request_overrides.as_ref(),
            endpoint,
            model,
        );

        Ok(headers)
    }

    fn looks_like_reasoning_unsupported_error(status: reqwest::StatusCode, body: &str) -> bool {
        if !(status == 400 || status == 404 || status == 405 || status == 409 || status == 422) {
            return false;
        }

        let b = body.to_ascii_lowercase();
        let mentions_reasoning = b.contains("reasoning")
            || b.contains("thinking")
            || b.contains("budget_tokens")
            || b.contains("unknown parameter");
        let mentions_unsupported = b.contains("unsupported")
            || b.contains("not supported")
            || b.contains("unknown")
            || b.contains("invalid");
        mentions_reasoning && mentions_unsupported
    }
}

#[async_trait]
impl LLMProvider for AnthropicProvider {
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
        let max_tokens = max_output_tokens.unwrap_or(self.max_tokens);
        let reasoning_effort = options
            .and_then(|o| o.reasoning_effort)
            .or(self.default_reasoning_effort);
        let request_reasoning_effort = options.and_then(|o| o.reasoning_effort);
        let parallel_tool_calls = options.and_then(|o| o.parallel_tool_calls);
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

        tracing::debug!("Anthropic provider using model: {}", model);

        let mut body = build_anthropic_request(
            messages,
            tools,
            model,
            max_tokens,
            true,
            reasoning_effort,
            parallel_tool_calls,
        );
        request_overrides::apply_overrides_to_body(
            &mut body,
            self.request_overrides.as_ref(),
            request_overrides::ENDPOINT_MESSAGES,
            Some(model),
        );
        let mut applied_reasoning_effort = reasoning_effort;
        let mut thinking_enabled = body.get("thinking").is_some();
        let mut thinking_budget_tokens = body
            .get("thinking")
            .and_then(|thinking| thinking.get("budget_tokens"))
            .and_then(|value| value.as_u64());
        tracing::info!(
            "[{}] Anthropic request model='{}' reasoning_effort={} reasoning_source={} request_reasoning_enabled={} thinking_enabled={} thinking_budget_tokens={} max_tokens={} [{}]",
            session_log_id,
            model,
            applied_reasoning_effort
                .map(ReasoningEffort::as_str)
                .unwrap_or("none"),
            reasoning_source,
            applied_reasoning_effort.is_some(),
            thinking_enabled,
            thinking_budget_tokens
                .map(|tokens| tokens.to_string())
                .unwrap_or_else(|| "none".to_string()),
            max_tokens,
            request_purpose
        );
        let headers = self.build_headers(request_overrides::ENDPOINT_MESSAGES, Some(model))?;

        let mut response = self
            .client
            .post(format!("{}/messages", self.base_url))
            .headers(headers.clone())
            .json(&body)
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
                    "Anthropic /messages rejected reasoning for model '{}'; retrying without reasoning_effort",
                    model
                );

                let mut fallback_body = build_anthropic_request(
                    messages,
                    tools,
                    model,
                    max_tokens,
                    true,
                    None,
                    parallel_tool_calls,
                );
                request_overrides::apply_overrides_to_body(
                    &mut fallback_body,
                    self.request_overrides.as_ref(),
                    request_overrides::ENDPOINT_MESSAGES,
                    Some(model),
                );
                applied_reasoning_effort = None;
                thinking_enabled = false;
                thinking_budget_tokens = None;
                tracing::info!(
                    "[{}] Anthropic request retry model='{}' reasoning_effort=none reasoning_source={} request_reasoning_enabled=false thinking_enabled=false thinking_budget_tokens=none max_tokens={} [{}]",
                    session_log_id,
                    model,
                    reasoning_source,
                    max_tokens,
                    request_purpose
                );
                response = self
                    .client
                    .post(format!("{}/messages", self.base_url))
                    .headers(headers.clone())
                    .json(&fallback_body)
                    .send()
                    .await
                    .map_err(LLMError::Http)?;

                if !response.status().is_success() {
                    let status = response.status();
                    let text = response.text().await.map_err(LLMError::Http)?;

                    if status == 401 || status == 403 {
                        return Err(LLMError::Auth(format!(
                            "Anthropic authentication failed: {}. Please check your API key.",
                            text
                        )));
                    }

                    return Err(LLMError::Api(format!(
                        "Anthropic API error: HTTP {}: {}",
                        status, text
                    )));
                }
            } else {
                if status == 401 || status == 403 {
                    return Err(LLMError::Auth(format!(
                        "Anthropic authentication failed: {}. Please check your API key.",
                        text
                    )));
                }

                return Err(LLMError::Api(format!(
                    "Anthropic API error: HTTP {}: {}",
                    status, text
                )));
            }
        }

        // Use shared SSE adapter with Anthropic-specific parser
        let mut state = AnthropicStreamState {
            requested_reasoning_effort: applied_reasoning_effort,
            request_thinking_enabled: thinking_enabled,
            request_thinking_budget_tokens: thinking_budget_tokens,
            ..Default::default()
        };

        let stream = crate::llm::providers::common::sse::llm_stream_from_sse(
            response,
            move |event, data| parse_anthropic_sse_event(&mut state, event, data),
        );

        Ok(stream)
    }

    async fn list_models(&self) -> Result<Vec<String>> {
        let headers = self.build_headers(request_overrides::ENDPOINT_MODELS, None)?;
        let url = format!("{}/models", self.base_url.trim_end_matches('/'));
        model_fetcher::fetch_model_list(&self.client, &url, headers, "Anthropic").await
    }
}

/// Build an Anthropic Messages API request body from internal message/tool types.
///
/// This is a pure conversion helper: it does no I/O and intentionally omits internal fields
/// like message `id`/`created_at`.
pub fn build_anthropic_request(
    messages: &[Message],
    tools: &[ToolSchema],
    model: &str,
    max_tokens: u32,
    stream: bool,
    reasoning_effort: Option<ReasoningEffort>,
    parallel_tool_calls: Option<bool>,
) -> Value {
    let (system, mut anthropic_messages) = messages_to_anthropic_json(messages);

    // Add cache_control to conversation summary message blocks.
    add_cache_control_to_summary_blocks(&mut anthropic_messages);

    let mut tools_json = tools_to_anthropic_json(tools);
    // Add cache_control to the last tool definition so tool definitions
    // are cached across turns.
    if let Some(last_tool) = tools_json.last_mut() {
        last_tool
            .as_object_mut()
            .expect("tool definition is always a JSON object")
            .insert("cache_control".to_string(), json!({"type": "ephemeral"}));
    }

    let mut body = json!({
        "model": model,
        "max_tokens": max_tokens,
        "stream": stream,
        "messages": anthropic_messages,
        "tools": tools_json,
    });

    if let Some(system) = system {
        body["system"] = system;
    }

    if let Some(thinking) = anthropic_thinking_from_effort(reasoning_effort, max_tokens) {
        body["thinking"] = thinking;
    }

    if !tools.is_empty() {
        if let Some(parallel_tool_calls) = parallel_tool_calls {
            body["tool_choice"] = json!({
                "type": "auto",
                "disable_parallel_tool_use": !parallel_tool_calls,
            });
        }
    }

    body
}

/// Find messages whose text content contains the conversation summary marker and
/// add `cache_control: {"type": "ephemeral"}` to their text blocks.
fn add_cache_control_to_summary_blocks(messages: &mut [Value]) {
    for msg in messages.iter_mut() {
        let content = msg.get("content");
        let Some(content) = content else { continue };
        // Check if any text block in this message contains a summary marker.
        let has_summary = if content.is_array() {
            content.as_array().unwrap().iter().any(|block| {
                block
                    .get("text")
                    .and_then(|t| t.as_str())
                    .is_some_and(|t| t.contains("CONVERSATION_SUMMARY_START"))
            })
        } else {
            content
                .as_str()
                .is_some_and(|t| t.contains("CONVERSATION_SUMMARY_START"))
        };
        if !has_summary {
            continue;
        }
        // Add cache_control to text blocks in this message.
        if let Some(blocks) = msg.get_mut("content").and_then(|c| c.as_array_mut()) {
            for block in blocks.iter_mut() {
                if block.get("type").and_then(|t| t.as_str()) == Some("text") {
                    block
                        .as_object_mut()
                        .expect("content block is always a JSON object")
                        .insert("cache_control".to_string(), json!({"type": "ephemeral"}));
                }
            }
        }
    }
}

fn anthropic_thinking_from_effort(
    reasoning_effort: Option<ReasoningEffort>,
    max_tokens: u32,
) -> Option<Value> {
    let effort = reasoning_effort?;
    let target_budget = match effort {
        ReasoningEffort::Low => return None,
        ReasoningEffort::Medium => 1024,
        ReasoningEffort::High => 4096,
        ReasoningEffort::Xhigh | ReasoningEffort::Max => 8192,
    };

    // Keep some room for final answer tokens.
    let available_budget = max_tokens.saturating_sub(128);
    if available_budget == 0 {
        return None;
    }

    Some(json!({
        "type": "enabled",
        "budget_tokens": target_budget.min(available_budget),
    }))
}

fn messages_to_anthropic_json(messages: &[Message]) -> (Option<Value>, Vec<Value>) {
    let mut system_parts: Vec<&str> = Vec::new();
    let mut out: Vec<Value> = Vec::new();

    for m in messages {
        match m.role {
            Role::System => system_parts.push(m.content.as_str()),
            Role::User | Role::Assistant | Role::Tool => {
                let msg_json = message_to_anthropic_json(m);
                // Merge consecutive Tool messages into the preceding user
                // message so that all tool_results for a single assistant
                // tool_use turn live in the *same* user message, as required
                // by the Anthropic API.
                if matches!(m.role, Role::Tool) {
                    if let Some(last) = out.last_mut() {
                        let last_role = last.get("role").and_then(|r| r.as_str());
                        if last_role == Some("user") {
                            if let Some(last_content) =
                                last.get_mut("content").and_then(|c| c.as_array_mut())
                            {
                                let last_is_tool_result_message = !last_content.is_empty()
                                    && last_content.iter().all(|block| {
                                        block.get("type").and_then(|t| t.as_str())
                                            == Some("tool_result")
                                    });
                                if last_is_tool_result_message {
                                    if let Some(new_content) =
                                        msg_json.get("content").and_then(|c| c.as_array())
                                    {
                                        last_content.extend(new_content.iter().cloned());
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                }
                out.push(msg_json);
            }
        }
    }

    let system = if system_parts.is_empty() {
        None
    } else {
        let joined = system_parts.join("\n\n");
        let mut blocks: Vec<Value> = vec![json!({
            "type": "text",
            "text": joined,
        })];
        // Mark the last system block as a cache breakpoint so the system prompt
        // benefits from Anthropic's prompt caching.
        if let Some(last) = blocks.last_mut() {
            last.as_object_mut()
                .expect("system block is always a JSON object")
                .insert("cache_control".to_string(), json!({"type": "ephemeral"}));
        }
        Some(json!(blocks))
    };

    (system, out)
}

fn message_to_anthropic_json(message: &Message) -> Value {
    match message.role {
        Role::System => unreachable!("system messages should be extracted into top-level `system`"),
        Role::User => json!({
            "role": "user",
            "content": user_content_to_anthropic_blocks(message),
        }),
        Role::Assistant => {
            let mut blocks: Vec<Value> = Vec::new();

            // When extended thinking was enabled, Anthropic requires the
            // `thinking` content block to be present in every assistant
            // message — including tool-call turns.  Without it the API
            // returns HTTP 400:
            //   "thinking is enabled but reasoning_content is missing in
            //    assistant tool call message at index N"
            if let Some(reasoning) = &message.reasoning {
                if !reasoning.is_empty() {
                    blocks.push(json!({
                        "type": "thinking",
                        "thinking": reasoning,
                    }));
                }
            }

            if !message.content.is_empty() {
                blocks.push(json!({
                    "type": "text",
                    "text": message.content,
                }));
            }

            if let Some(tool_calls) = &message.tool_calls {
                for tc in tool_calls {
                    blocks.push(tool_call_to_tool_use_block(tc));
                }
            }

            json!({
                "role": "assistant",
                "content": blocks,
            })
        }
        Role::Tool => {
            let Some(tool_use_id) = message.tool_call_id.as_deref() else {
                tracing::warn!(
                    "Anthropic conversion received tool message without tool_call_id; emitting plain text block"
                );
                return json!({
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": message.content,
                        }
                    ],
                });
            };

            json!({
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": tool_use_id,
                        "content": message.content,
                    }
                ],
            })
        }
    }
}

fn user_content_to_anthropic_blocks(message: &Message) -> Vec<Value> {
    if let Some(parts) = message.content_parts.as_ref() {
        let mut blocks = Vec::new();
        for part in parts {
            if let Some(block) = content_part_to_anthropic_block(part) {
                blocks.push(block);
            }
        }
        if blocks.is_empty() {
            blocks.push(json!({
                "type": "text",
                "text": message.content,
            }));
        }
        return blocks;
    }

    vec![json!({
        "type": "text",
        "text": message.content,
    })]
}

fn content_part_to_anthropic_block(part: &MessagePart) -> Option<Value> {
    match part {
        MessagePart::Text { text } => Some(json!({
            "type": "text",
            "text": text,
        })),
        MessagePart::ImageUrl { image_url } => image_url_to_anthropic_block(&image_url.url),
    }
}

fn image_url_to_anthropic_block(url: &str) -> Option<Value> {
    let trimmed = url.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Some((media_type, data)) = parse_data_url_base64(trimmed) {
        return Some(json!({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": media_type,
                "data": data,
            }
        }));
    }

    Some(json!({
        "type": "image",
        "source": {
            "type": "url",
            "url": trimmed,
        }
    }))
}

fn parse_data_url_base64(url: &str) -> Option<(String, String)> {
    let rest = url.strip_prefix("data:")?;
    let (meta, data) = rest.split_once(',')?;
    let data = data.trim();
    if data.is_empty() {
        return None;
    }

    let mut media_type = "application/octet-stream";
    let mut is_base64 = false;

    for (idx, seg) in meta.split(';').enumerate() {
        let segment = seg.trim();
        if idx == 0 && !segment.is_empty() && !segment.eq_ignore_ascii_case("base64") {
            media_type = segment;
        }
        if segment.eq_ignore_ascii_case("base64") {
            is_base64 = true;
        }
    }

    if !is_base64 {
        return None;
    }

    Some((media_type.to_string(), data.to_string()))
}

fn preview_for_log(value: &str, max_chars: usize) -> String {
    let mut iter = value.chars();
    let mut preview = String::new();
    for _ in 0..max_chars {
        match iter.next() {
            Some(ch) => preview.push(ch),
            None => break,
        }
    }
    if iter.next().is_some() {
        preview.push_str("...");
    }
    preview.replace('\n', "\\n").replace('\r', "\\r")
}

fn tool_call_to_tool_use_block(tool_call: &bamboo_domain::ToolCall) -> Value {
    let raw_arguments = tool_call.function.arguments.trim();
    let input: Value = match serde_json::from_str(raw_arguments) {
        Ok(parsed) => parsed,
        Err(error) => {
            tracing::warn!(
                "Anthropic tool_use conversion fallback to string input due to invalid JSON arguments: tool_call_id={}, tool_name={}, args_len={}, args_preview=\"{}\", error={}",
                tool_call.id,
                tool_call.function.name,
                raw_arguments.len(),
                preview_for_log(raw_arguments, 180),
                error
            );
            Value::String(tool_call.function.arguments.clone())
        }
    };

    json!({
        "type": "tool_use",
        "id": tool_call.id,
        "name": tool_call.function.name,
        "input": input,
    })
}

fn tools_to_anthropic_json(tools: &[ToolSchema]) -> Vec<Value> {
    tools
        .iter()
        .map(|t| {
            json!({
                "name": t.function.name,
                "description": t.function.description,
                "input_schema": t.function.parameters,
            })
        })
        .collect()
}

/// Stateful parser for Anthropic SSE streaming events.
///
/// Tracks tool_use blocks by index so we can emit partial ToolCall chunks with correct id/name.
#[derive(Default)]
pub struct AnthropicStreamState {
    tool_uses_by_index: HashMap<usize, (String, String)>, // (id, name)
    thinking_blocks_by_index: HashSet<usize>,
    thinking_blocks_started: usize,
    thinking_chars_streamed: usize,
    saw_thinking_signal: bool,
    requested_reasoning_effort: Option<ReasoningEffort>,
    request_thinking_enabled: bool,
    request_thinking_budget_tokens: Option<u64>,
}

/// Parse a single Anthropic SSE event into an optional [`LLMChunk`].
///
/// Returns:
/// - `Ok(Some(chunk))` for content-bearing events (text deltas, tool calls, message_stop)
/// - `Ok(None)` for non-content events (message_start, pings, etc.)
/// - `Err(_)` for malformed JSON or unexpected shapes
pub fn parse_anthropic_sse_event(
    state: &mut AnthropicStreamState,
    event_type: &str,
    data: &str,
) -> Result<Option<LLMChunk>> {
    match event_type {
        "ping" => Ok(None),
        "message_start" => {
            if !data.is_empty() {
                if let Ok(v) = serde_json::from_str::<Value>(data) {
                    if let Some(usage) = v
                        .get("message")
                        .and_then(|m| m.get("usage"))
                        .or_else(|| v.get("usage"))
                        .and_then(|u| u.as_object())
                    {
                        let cache_creation = usage
                            .get("cache_creation_input_tokens")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                        let cache_read = usage
                            .get("cache_read_input_tokens")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                        if cache_creation > 0 || cache_read > 0 {
                            tracing::info!(
                                "Anthropic stream message_start cache_creation={} cache_read={}",
                                cache_creation,
                                cache_read,
                            );
                            return Ok(Some(LLMChunk::CacheUsage {
                                cache_creation_input_tokens: cache_creation,
                                cache_read_input_tokens: cache_read,
                            }));
                        }
                    }
                }
            }
            Ok(None)
        }
        "message_delta" => {
            if !data.is_empty() {
                match serde_json::from_str::<Value>(data) {
                    Ok(v) => {
                        if let Some(stop_reason) = v
                            .get("delta")
                            .and_then(|delta| delta.get("stop_reason"))
                            .and_then(|reason| reason.as_str())
                        {
                            if stop_reason == "max_tokens" {
                                tracing::warn!(
                                    "Anthropic stream stop_reason=max_tokens; response may be truncated"
                                );
                            } else {
                                tracing::debug!("Anthropic stream stop_reason={stop_reason}");
                            }
                        }

                        if let Some(usage) = v.get("usage").and_then(|u| u.as_object()) {
                            let output_tokens =
                                usage.get("output_tokens").and_then(|value| value.as_u64());
                            let thinking_tokens = usage
                                .get("thinking_tokens")
                                .and_then(|value| value.as_u64())
                                .or_else(|| {
                                    usage
                                        .get("reasoning_tokens")
                                        .and_then(|value| value.as_u64())
                                });
                            let cache_creation = usage
                                .get("cache_creation_input_tokens")
                                .and_then(|value| value.as_u64())
                                .unwrap_or(0);
                            let cache_read = usage
                                .get("cache_read_input_tokens")
                                .and_then(|value| value.as_u64())
                                .unwrap_or(0);

                            if let Some(thinking_tokens) = thinking_tokens {
                                state.saw_thinking_signal = true;
                                tracing::info!(
                                    "Anthropic stream usage output_tokens={} thinking_tokens={}",
                                    output_tokens.unwrap_or(0),
                                    thinking_tokens
                                );
                            } else if let Some(output_tokens) = output_tokens {
                                tracing::debug!(
                                    "Anthropic stream usage output_tokens={output_tokens}"
                                );
                            }

                            // Emit CacheUsage if any cache activity.
                            if cache_creation > 0 || cache_read > 0 {
                                return Ok(Some(LLMChunk::CacheUsage {
                                    cache_creation_input_tokens: cache_creation,
                                    cache_read_input_tokens: cache_read,
                                }));
                            }

                            // Emit UsageSummary with output/thinking tokens.
                            if let Some(output_tokens) = output_tokens {
                                return Ok(Some(LLMChunk::UsageSummary {
                                    output_tokens,
                                    thinking_tokens: thinking_tokens.unwrap_or(0),
                                }));
                            }
                        }
                    }
                    Err(error) => {
                        tracing::debug!(
                            "Failed to parse Anthropic message_delta payload for logging: {} (payload={})",
                            error,
                            preview_for_log(data, 120)
                        );
                    }
                }
            }
            Ok(None)
        }
        "message_stop" => {
            if state.request_thinking_enabled || state.saw_thinking_signal {
                tracing::info!(
                    "Anthropic reasoning summary: requested_effort={} request_thinking_enabled={} request_thinking_budget_tokens={} observed_thinking_signal={} thinking_blocks_started={} thinking_chars_streamed={}",
                    state
                        .requested_reasoning_effort
                        .map(ReasoningEffort::as_str)
                        .unwrap_or("none"),
                    state.request_thinking_enabled,
                    state
                        .request_thinking_budget_tokens
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "none".to_string()),
                    state.saw_thinking_signal,
                    state.thinking_blocks_started,
                    state.thinking_chars_streamed
                );
            }

            if !state.tool_uses_by_index.is_empty() {
                let open_blocks: Vec<String> = state
                    .tool_uses_by_index
                    .iter()
                    .map(|(index, (id, name))| format!("{index}:{name}:{id}"))
                    .collect();
                tracing::warn!(
                    "Anthropic message_stop received with {} open tool_use blocks (possible incomplete tool arguments): {}",
                    open_blocks.len(),
                    open_blocks.join(", ")
                );
                state.tool_uses_by_index.clear();
            }

            state.thinking_blocks_by_index.clear();
            Ok(Some(LLMChunk::Done))
        }
        "error" => Err(LLMError::Api(format!("Anthropic error event: {data}"))),
        "content_block_start" => {
            if data.is_empty() {
                return Ok(None);
            }

            let v: Value = serde_json::from_str(data)?;
            let Some(index) = v.get("index").and_then(|i| i.as_u64()) else {
                return Err(LLMError::Stream(format!(
                    "Anthropic content_block_start missing index: {data}"
                )));
            };
            let Some(content_block) = v.get("content_block") else {
                return Err(LLMError::Stream(format!(
                    "Anthropic content_block_start missing content_block: {data}"
                )));
            };

            let block_type = content_block
                .get("type")
                .and_then(|t| t.as_str())
                .unwrap_or_default();

            if block_type == "thinking" || block_type == "redacted_thinking" {
                let index = index as usize;
                state.saw_thinking_signal = true;
                state.thinking_blocks_started = state.thinking_blocks_started.saturating_add(1);
                state.thinking_blocks_by_index.insert(index);
                tracing::info!(
                    "Anthropic thinking block started: index={} type={}",
                    index,
                    block_type
                );
                return Ok(None);
            }

            if block_type != "tool_use" {
                return Ok(None);
            }

            let Some(id) = content_block.get("id").and_then(|s| s.as_str()) else {
                return Err(LLMError::Stream(format!(
                    "Anthropic tool_use content_block missing id: {data}"
                )));
            };
            let Some(name) = content_block.get("name").and_then(|s| s.as_str()) else {
                return Err(LLMError::Stream(format!(
                    "Anthropic tool_use content_block missing name: {data}"
                )));
            };

            let index = index as usize;
            state
                .tool_uses_by_index
                .insert(index, (id.to_string(), name.to_string()));
            tracing::debug!(
                "Anthropic tool_use started: index={}, tool_call_id={}, tool_name={}",
                index,
                id,
                name
            );

            Ok(Some(LLMChunk::ToolCalls(vec![bamboo_domain::ToolCall {
                id: id.to_string(),
                tool_type: "function".to_string(),
                function: bamboo_domain::FunctionCall {
                    name: name.to_string(),
                    arguments: String::new(),
                },
            }])))
        }
        "content_block_delta" => {
            if data.is_empty() {
                return Ok(None);
            }

            let v: Value = serde_json::from_str(data)?;
            let Some(delta) = v.get("delta") else {
                return Ok(None);
            };

            let delta_type = delta
                .get("type")
                .and_then(|t| t.as_str())
                .unwrap_or_default();

            match delta_type {
                "text_delta" => {
                    let text = delta
                        .get("text")
                        .and_then(|t| t.as_str())
                        .unwrap_or_default();
                    Ok(Some(LLMChunk::Token(text.to_string())))
                }
                "input_json_delta" => {
                    let Some(index) = v.get("index").and_then(|i| i.as_u64()) else {
                        return Err(LLMError::Stream(format!(
                            "Anthropic input_json_delta missing index: {data}"
                        )));
                    };
                    let partial = delta
                        .get("partial_json")
                        .and_then(|p| p.as_str())
                        .unwrap_or_default();

                    let index = index as usize;
                    let Some((id, name)) = state.tool_uses_by_index.get(&index) else {
                        return Err(LLMError::Stream(format!(
                            "Anthropic input_json_delta for unknown tool_use index {index}: {data}"
                        )));
                    };
                    tracing::trace!(
                        "Anthropic tool_use input_json_delta: index={}, tool_call_id={}, tool_name={}, chunk_len={}",
                        index,
                        id,
                        name,
                        partial.len()
                    );

                    Ok(Some(LLMChunk::ToolCalls(vec![bamboo_domain::ToolCall {
                        id: id.clone(),
                        tool_type: "function".to_string(),
                        function: bamboo_domain::FunctionCall {
                            name: name.clone(),
                            arguments: partial.to_string(),
                        },
                    }])))
                }
                "thinking_delta" => {
                    let Some(index) = v.get("index").and_then(|i| i.as_u64()) else {
                        return Ok(None);
                    };
                    let index = index as usize;

                    if state.thinking_blocks_by_index.contains(&index) {
                        state.saw_thinking_signal = true;
                        let delta_len = delta
                            .get("thinking")
                            .and_then(|value| value.as_str())
                            .map(str::len)
                            .or_else(|| {
                                delta
                                    .get("text")
                                    .and_then(|value| value.as_str())
                                    .map(str::len)
                            })
                            .unwrap_or(0);
                        state.thinking_chars_streamed =
                            state.thinking_chars_streamed.saturating_add(delta_len);
                        tracing::trace!(
                            "Anthropic thinking_delta: index={}, chunk_len={}",
                            index,
                            delta_len
                        );

                        let reasoning_chunk = delta
                            .get("thinking")
                            .and_then(|value| value.as_str())
                            .or_else(|| delta.get("text").and_then(|value| value.as_str()))
                            .unwrap_or("");
                        if !reasoning_chunk.is_empty() {
                            return Ok(Some(LLMChunk::ReasoningToken(reasoning_chunk.to_string())));
                        }
                    }
                    Ok(None)
                }
                _ => Ok(None),
            }
        }
        "content_block_stop" => {
            // Keep memory bounded: once a content block is complete, we don't need its id/name.
            if data.is_empty() {
                return Ok(None);
            }

            let v: Value = serde_json::from_str(data)?;
            if let Some(index) = v.get("index").and_then(|i| i.as_u64()) {
                let index = index as usize;
                state.tool_uses_by_index.remove(&index);
                state.thinking_blocks_by_index.remove(&index);
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod anthropic_request_building {
    use crate::models::{ContentPart, ImageUrl};
    use bamboo_domain::Message;
    use bamboo_domain::MessagePart;
    use bamboo_domain::{FunctionCall, ToolCall};
    use bamboo_domain::{FunctionSchema, ToolSchema};

    #[test]
    fn system_messages_are_extracted_into_blocks_with_cache_control() {
        let messages = vec![
            Message::system("You are helpful."),
            Message::user("Hi"),
            Message::system("Be concise."),
            Message::assistant("Hello!", None),
        ];

        let out =
            super::build_anthropic_request(&messages, &[], "claude-test", 64, false, None, None);

        let system = out["system"]
            .as_array()
            .expect("system should be an array of blocks");
        assert_eq!(system.len(), 1);
        assert_eq!(system[0]["type"], "text");
        assert_eq!(system[0]["text"], "You are helpful.\n\nBe concise.");
        assert_eq!(system[0]["cache_control"]["type"], "ephemeral");
        assert_eq!(out["messages"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn system_field_absent_when_no_system_messages() {
        let messages = vec![Message::user("Hi")];

        let out =
            super::build_anthropic_request(&messages, &[], "claude-test", 64, false, None, None);

        assert!(out.get("system").is_none());
    }

    #[test]
    fn tool_messages_become_tool_result_blocks() {
        let messages = vec![Message::tool_result("call_1", "OK")];

        let out =
            super::build_anthropic_request(&messages, &[], "claude-test", 64, false, None, None);

        assert_eq!(out["messages"].as_array().unwrap().len(), 1);
        assert_eq!(out["messages"][0]["role"], "user");
        assert_eq!(out["messages"][0]["content"][0]["type"], "tool_result");
        assert_eq!(out["messages"][0]["content"][0]["tool_use_id"], "call_1");
        assert_eq!(out["messages"][0]["content"][0]["content"], "OK");
    }

    #[test]
    fn assistant_tool_calls_become_tool_use_blocks_with_parsed_json_input() {
        let tool_call = ToolCall {
            id: "call_1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "search".to_string(),
                arguments: r#"{"q":"test"}"#.to_string(),
            },
        };

        let messages = vec![Message::assistant("", Some(vec![tool_call]))];

        let out =
            super::build_anthropic_request(&messages, &[], "claude-test", 64, false, None, None);

        assert_eq!(out["messages"].as_array().unwrap().len(), 1);
        assert_eq!(out["messages"][0]["role"], "assistant");
        assert_eq!(out["messages"][0]["content"][0]["type"], "tool_use");
        assert_eq!(out["messages"][0]["content"][0]["id"], "call_1");
        assert_eq!(out["messages"][0]["content"][0]["name"], "search");
        assert_eq!(out["messages"][0]["content"][0]["input"]["q"], "test");
    }

    #[test]
    fn user_message_with_data_url_image_becomes_anthropic_image_block() {
        let messages = vec![Message::user_with_parts(
            "describe",
            vec![
                ContentPart::Text {
                    text: "describe".to_string(),
                },
                ContentPart::ImageUrl {
                    image_url: ImageUrl {
                        url: "data:image/png;base64,AAAABBBB".to_string(),
                        detail: None,
                    },
                },
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
        )];

        let out =
            super::build_anthropic_request(&messages, &[], "claude-test", 64, false, None, None);

        assert_eq!(out["messages"][0]["content"][1]["type"], "image");
        assert_eq!(out["messages"][0]["content"][1]["source"]["type"], "base64");
        assert_eq!(
            out["messages"][0]["content"][1]["source"]["media_type"],
            "image/png"
        );
        assert_eq!(
            out["messages"][0]["content"][1]["source"]["data"],
            "AAAABBBB"
        );
    }

    #[test]
    fn user_message_with_remote_image_uses_url_source() {
        let messages = vec![Message::user_with_parts(
            "describe",
            vec![ContentPart::ImageUrl {
                image_url: ImageUrl {
                    url: "https://example.com/cat.png".to_string(),
                    detail: None,
                },
            }]
            .into_iter()
            .map(Into::into)
            .collect(),
        )];

        let out =
            super::build_anthropic_request(&messages, &[], "claude-test", 64, false, None, None);

        assert_eq!(out["messages"][0]["content"][0]["type"], "image");
        assert_eq!(out["messages"][0]["content"][0]["source"]["type"], "url");
        assert_eq!(
            out["messages"][0]["content"][0]["source"]["url"],
            "https://example.com/cat.png"
        );
    }

    fn sample_tools() -> Vec<ToolSchema> {
        vec![ToolSchema {
            schema_type: "function".to_string(),
            function: FunctionSchema {
                name: "search".to_string(),
                description: "Search".to_string(),
                parameters: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "q": {"type": "string"}
                    },
                    "required": ["q"]
                }),
            },
        }]
    }

    #[test]
    fn last_tool_definition_has_cache_control() {
        let messages = vec![Message::user("Hi")];
        let tools = vec![
            ToolSchema {
                schema_type: "function".to_string(),
                function: FunctionSchema {
                    name: "read".to_string(),
                    description: "Read a file".to_string(),
                    parameters: serde_json::json!({"type": "object", "properties": {}}),
                },
            },
            ToolSchema {
                schema_type: "function".to_string(),
                function: FunctionSchema {
                    name: "write".to_string(),
                    description: "Write a file".to_string(),
                    parameters: serde_json::json!({"type": "object", "properties": {}}),
                },
            },
        ];

        let out =
            super::build_anthropic_request(&messages, &tools, "claude-test", 64, false, None, None);

        let tools_arr = out["tools"].as_array().unwrap();
        assert_eq!(tools_arr.len(), 2);
        // First tool has no cache_control.
        assert!(
            tools_arr[0].get("cache_control").is_none(),
            "first tool should not have cache_control"
        );
        // Last tool has cache_control.
        assert_eq!(tools_arr[1]["cache_control"]["type"], "ephemeral");
    }

    #[test]
    fn conversation_summary_message_gets_cache_control() {
        let messages = vec![
            Message::user("Hi"),
            Message::user("<!-- CONVERSATION_SUMMARY_START -->\nOld context here\n<!-- CONVERSATION_SUMMARY_END -->"),
            Message::assistant("Got it", None),
        ];

        let out =
            super::build_anthropic_request(&messages, &[], "claude-test", 64, false, None, None);

        let msgs = out["messages"].as_array().unwrap();
        // First message (Hi) has no cache_control on its text block.
        let first_content = msgs[0]["content"].as_array().unwrap();
        assert!(first_content[0].get("cache_control").is_none());

        // Second message (summary) has cache_control on text blocks.
        let second_content = msgs[1]["content"].as_array().unwrap();
        assert_eq!(second_content[0]["cache_control"]["type"], "ephemeral");

        // Third message (assistant) is not affected.
        assert!(msgs[2]["content"].as_array().unwrap()[0]
            .get("cache_control")
            .is_none());
    }

    #[test]
    fn non_summary_messages_do_not_get_cache_control() {
        let messages = vec![
            Message::user("Hello"),
            Message::assistant("Hi there", None),
            Message::user("How are you?"),
        ];

        let out =
            super::build_anthropic_request(&messages, &[], "claude-test", 64, false, None, None);

        for msg in out["messages"].as_array().unwrap() {
            if let Some(blocks) = msg["content"].as_array() {
                for block in blocks {
                    assert!(
                        block.get("cache_control").is_none(),
                        "non-summary message should not have cache_control"
                    );
                }
            }
        }
    }

    #[test]
    fn consecutive_tool_results_merge_into_single_user_tool_result_message() {
        let messages = vec![
            Message::assistant(
                "calling tools",
                Some(vec![
                    ToolCall {
                        id: "call_1".to_string(),
                        tool_type: "function".to_string(),
                        function: FunctionCall {
                            name: "search".to_string(),
                            arguments: r#"{"q":"alpha"}"#.to_string(),
                        },
                    },
                    ToolCall {
                        id: "call_2".to_string(),
                        tool_type: "function".to_string(),
                        function: FunctionCall {
                            name: "read".to_string(),
                            arguments: r#"{"path":"/tmp/x"}"#.to_string(),
                        },
                    },
                ]),
            ),
            Message::tool_result("call_1", "alpha"),
            Message::tool_result("call_2", "beta"),
        ];

        let out =
            super::build_anthropic_request(&messages, &[], "claude-test", 64, false, None, None);

        let built_messages = out["messages"].as_array().expect("messages array");
        assert_eq!(built_messages.len(), 2);
        assert_eq!(built_messages[0]["role"], "assistant");
        assert_eq!(built_messages[1]["role"], "user");
        let tool_result_blocks = built_messages[1]["content"]
            .as_array()
            .expect("content array");
        assert_eq!(tool_result_blocks.len(), 2);
        assert_eq!(tool_result_blocks[0]["type"], "tool_result");
        assert_eq!(tool_result_blocks[0]["tool_use_id"], "call_1");
        assert_eq!(tool_result_blocks[1]["type"], "tool_result");
        assert_eq!(tool_result_blocks[1]["tool_use_id"], "call_2");
    }

    #[test]
    fn tool_result_does_not_merge_into_regular_user_message() {
        let messages = vec![
            Message::user("normal user text"),
            Message::tool_result("call_1", "OK"),
        ];

        let out =
            super::build_anthropic_request(&messages, &[], "claude-test", 64, false, None, None);

        let built_messages = out["messages"].as_array().expect("messages array");
        assert_eq!(
            built_messages.len(),
            2,
            "tool_result should stay in its own user message instead of merging into a regular user text message"
        );
        assert_eq!(built_messages[0]["role"], "user");
        assert_eq!(built_messages[0]["content"][0]["type"], "text");
        assert_eq!(built_messages[0]["content"][0]["text"], "normal user text");
        assert_eq!(built_messages[1]["role"], "user");
        assert_eq!(built_messages[1]["content"][0]["type"], "tool_result");
        assert_eq!(built_messages[1]["content"][0]["tool_use_id"], "call_1");
    }

    #[test]
    fn anthropic_request_preserves_non_system_message_order() {
        let messages = vec![
            Message::system("stable system"),
            Message::user("dynamic context block"),
            Message::user("conversation turn"),
            Message::assistant("calling tool", None),
            Message::tool_result("call_1", "tool output"),
            Message::user("latest user turn"),
        ];

        let out =
            super::build_anthropic_request(&messages, &[], "claude-test", 64, false, None, None);

        assert_eq!(out["system"][0]["text"], "stable system");
        let built_messages = out["messages"].as_array().expect("messages array");
        assert_eq!(built_messages.len(), 5);
        assert_eq!(built_messages[0]["role"], "user");
        assert_eq!(
            built_messages[0]["content"][0]["text"],
            "dynamic context block"
        );
        assert_eq!(built_messages[1]["role"], "user");
        assert_eq!(built_messages[1]["content"][0]["text"], "conversation turn");
        assert_eq!(built_messages[2]["role"], "assistant");
        assert_eq!(built_messages[2]["content"][0]["text"], "calling tool");
        assert_eq!(built_messages[3]["role"], "user");
        assert_eq!(built_messages[3]["content"][0]["type"], "tool_result");
        assert_eq!(built_messages[3]["content"][0]["tool_use_id"], "call_1");
        assert_eq!(built_messages[4]["role"], "user");
        assert_eq!(built_messages[4]["content"][0]["text"], "latest user turn");
    }

    #[test]
    fn parallel_tool_calls_true_enables_parallel_tool_use() {
        let messages = vec![Message::user("Hello")];
        let tools = sample_tools();

        let out = super::build_anthropic_request(
            &messages,
            &tools,
            "claude-test",
            64,
            false,
            None,
            Some(true),
        );

        assert_eq!(out["tool_choice"]["type"], "auto");
        assert_eq!(out["tool_choice"]["disable_parallel_tool_use"], false);
    }

    #[test]
    fn parallel_tool_calls_false_disables_parallel_tool_use() {
        let messages = vec![Message::user("Hello")];
        let tools = sample_tools();

        let out = super::build_anthropic_request(
            &messages,
            &tools,
            "claude-test",
            64,
            false,
            None,
            Some(false),
        );

        assert_eq!(out["tool_choice"]["type"], "auto");
        assert_eq!(out["tool_choice"]["disable_parallel_tool_use"], true);
    }
}

#[cfg(test)]
mod anthropic_stream_parse {
    use crate::llm::types::LLMChunk;

    #[test]
    fn message_start_is_ignored() {
        let mut state = super::AnthropicStreamState::default();
        let data = r#"{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","content":[]}}"#;

        let chunk = super::parse_anthropic_sse_event(&mut state, "message_start", data).unwrap();

        assert!(chunk.is_none());
    }

    #[test]
    fn message_stop_yields_done() {
        let mut state = super::AnthropicStreamState::default();
        let data = r#"{"type":"message_stop"}"#;

        let chunk = super::parse_anthropic_sse_event(&mut state, "message_stop", data)
            .unwrap()
            .expect("chunk");

        match chunk {
            LLMChunk::Done => {}
            other => panic!("expected LLMChunk::Done, got {other:?}"),
        }
    }

    #[test]
    fn text_delta_yields_token() {
        let mut state = super::AnthropicStreamState::default();
        let data = r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}"#;

        let chunk = super::parse_anthropic_sse_event(&mut state, "content_block_delta", data)
            .unwrap()
            .expect("chunk");

        match chunk {
            LLMChunk::Token(token) => assert_eq!(token, "Hello"),
            other => panic!("expected LLMChunk::Token, got {other:?}"),
        }
    }

    #[test]
    fn tool_use_start_and_input_json_delta_yield_tool_call_parts() {
        let mut state = super::AnthropicStreamState::default();

        let start = r#"{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_1","name":"search","input":{}}}"#;
        let chunk = super::parse_anthropic_sse_event(&mut state, "content_block_start", start)
            .unwrap()
            .expect("chunk");

        match chunk {
            LLMChunk::ToolCalls(calls) => {
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].id, "toolu_1");
                assert_eq!(calls[0].function.name, "search");
                assert!(calls[0].function.arguments.is_empty());
            }
            other => panic!("expected LLMChunk::ToolCalls, got {other:?}"),
        }

        let delta1 = r#"{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"q\":\"te"}}"#;
        let chunk = super::parse_anthropic_sse_event(&mut state, "content_block_delta", delta1)
            .unwrap()
            .expect("chunk");

        match chunk {
            LLMChunk::ToolCalls(calls) => {
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].id, "toolu_1");
                assert_eq!(calls[0].function.name, "search");
                assert_eq!(calls[0].function.arguments, r#"{"q":"te"#);
            }
            other => panic!("expected LLMChunk::ToolCalls, got {other:?}"),
        }

        let delta2 = r#"{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"st\"}"}}"#;
        let chunk = super::parse_anthropic_sse_event(&mut state, "content_block_delta", delta2)
            .unwrap()
            .expect("chunk");

        match chunk {
            LLMChunk::ToolCalls(calls) => {
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].id, "toolu_1");
                assert_eq!(calls[0].function.name, "search");
                assert_eq!(calls[0].function.arguments, "st\"}");
            }
            other => panic!("expected LLMChunk::ToolCalls, got {other:?}"),
        }
    }

    #[test]
    fn empty_data_returns_none() {
        let mut state = super::AnthropicStreamState::default();
        let chunk = super::parse_anthropic_sse_event(&mut state, "", "").unwrap();
        assert!(chunk.is_none());
    }

    #[test]
    fn invalid_json_returns_error() {
        let mut state = super::AnthropicStreamState::default();
        let result =
            super::parse_anthropic_sse_event(&mut state, "content_block_delta", "{invalid}");
        assert!(result.is_err());
    }

    #[test]
    fn unknown_event_type_returns_none() {
        let mut state = super::AnthropicStreamState::default();
        let data = r#"{"type":"unknown_event"}"#;
        let chunk = super::parse_anthropic_sse_event(&mut state, "unknown_event", data).unwrap();
        assert!(chunk.is_none());
    }

    #[test]
    fn text_delta_with_empty_text_returns_empty_token() {
        let mut state = super::AnthropicStreamState::default();
        let data =
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":""}}"#;

        let chunk = super::parse_anthropic_sse_event(&mut state, "content_block_delta", data)
            .unwrap()
            .expect("chunk");

        match chunk {
            LLMChunk::Token(token) => assert!(token.is_empty()),
            other => panic!("expected LLMChunk::Token, got {other:?}"),
        }
    }

    #[test]
    fn multiple_tool_uses_tracked_independently() {
        let mut state = super::AnthropicStreamState::default();

        // First tool
        let start1 = r#"{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_1","name":"search","input":{}}}"#;
        let chunk1 = super::parse_anthropic_sse_event(&mut state, "content_block_start", start1)
            .unwrap()
            .expect("chunk1");

        match chunk1 {
            LLMChunk::ToolCalls(calls) => {
                assert_eq!(calls[0].id, "toolu_1");
                assert_eq!(calls[0].function.name, "search");
            }
            other => panic!("expected LLMChunk::ToolCalls, got {other:?}"),
        }

        // Second tool
        let start2 = r#"{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_2","name":"read","input":{}}}"#;
        let chunk2 = super::parse_anthropic_sse_event(&mut state, "content_block_start", start2)
            .unwrap()
            .expect("chunk2");

        match chunk2 {
            LLMChunk::ToolCalls(calls) => {
                assert_eq!(calls[0].id, "toolu_2");
                assert_eq!(calls[0].function.name, "read");
            }
            other => panic!("expected LLMChunk::ToolCalls, got {other:?}"),
        }

        // Delta for first tool
        let delta1 = r#"{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"q\":\"test\"}"}}"#;
        let chunk3 = super::parse_anthropic_sse_event(&mut state, "content_block_delta", delta1)
            .unwrap()
            .expect("chunk3");

        match chunk3 {
            LLMChunk::ToolCalls(calls) => {
                assert_eq!(calls[0].id, "toolu_1");
                assert_eq!(calls[0].function.name, "search");
                assert_eq!(calls[0].function.arguments, r#"{"q":"test"}"#);
            }
            other => panic!("expected LLMChunk::ToolCalls, got {other:?}"),
        }

        // Delta for second tool
        let delta2 = r#"{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"file\":\"test.txt\"}"}}"#;
        let chunk4 = super::parse_anthropic_sse_event(&mut state, "content_block_delta", delta2)
            .unwrap()
            .expect("chunk4");

        match chunk4 {
            LLMChunk::ToolCalls(calls) => {
                assert_eq!(calls[0].id, "toolu_2");
                assert_eq!(calls[0].function.name, "read");
                assert_eq!(calls[0].function.arguments, r#"{"file":"test.txt"}"#);
            }
            other => panic!("expected LLMChunk::ToolCalls, got {other:?}"),
        }
    }

    #[test]
    fn content_block_start_without_tool_use_returns_none() {
        let mut state = super::AnthropicStreamState::default();
        let data = r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":"Hello"}}"#;

        let chunk =
            super::parse_anthropic_sse_event(&mut state, "content_block_start", data).unwrap();
        assert!(chunk.is_none());
    }

    #[test]
    fn input_json_delta_without_prior_tool_start_returns_error() {
        let mut state = super::AnthropicStreamState::default();
        let data = r#"{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"q\":\"test\"}"}}"#;

        let result = super::parse_anthropic_sse_event(&mut state, "content_block_delta", data);
        // Should return an error because there's no prior tool_use start for index 0
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod anthropic_request_building_edge_cases {
    use bamboo_domain::Message;

    #[test]
    fn empty_messages_list() {
        let messages: Vec<Message> = vec![];
        let out =
            super::build_anthropic_request(&messages, &[], "claude-test", 64, false, None, None);

        assert!(out["system"].is_null());
        assert_eq!(out["messages"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn only_system_messages() {
        let messages = vec![Message::system("Be helpful")];
        let out =
            super::build_anthropic_request(&messages, &[], "claude-test", 64, false, None, None);

        let system = out["system"].as_array().expect("system should be blocks");
        assert_eq!(system[0]["text"], "Be helpful");
        assert_eq!(out["messages"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn multiple_system_messages_joined() {
        let messages = vec![
            Message::system("Be helpful"),
            Message::system("Be concise"),
            Message::system("Be safe"),
        ];
        let out =
            super::build_anthropic_request(&messages, &[], "claude-test", 64, false, None, None);

        let system = out["system"].as_array().expect("system should be blocks");
        assert_eq!(system[0]["text"], "Be helpful\n\nBe concise\n\nBe safe");
    }

    #[test]
    fn assistant_message_with_both_content_and_tool_calls() {
        use bamboo_domain::{FunctionCall, ToolCall};

        let tool_call = ToolCall {
            id: "call_1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "search".to_string(),
                arguments: r#"{"q":"test"}"#.to_string(),
            },
        };

        let messages = vec![Message::assistant(
            "Let me search for that.",
            Some(vec![tool_call]),
        )];
        let out =
            super::build_anthropic_request(&messages, &[], "claude-test", 64, false, None, None);

        assert_eq!(out["messages"][0]["role"], "assistant");
        assert_eq!(out["messages"][0]["content"].as_array().unwrap().len(), 2);
        assert_eq!(out["messages"][0]["content"][0]["type"], "text");
        assert_eq!(
            out["messages"][0]["content"][0]["text"],
            "Let me search for that."
        );
        assert_eq!(out["messages"][0]["content"][1]["type"], "tool_use");
    }

    #[test]
    fn tool_call_with_invalid_json_arguments_falls_back_to_string() {
        use bamboo_domain::{FunctionCall, ToolCall};
        let tool_call = ToolCall {
            id: "call_1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "search".to_string(),
                arguments: "not valid json".to_string(),
            },
        };

        let messages = vec![Message::assistant("", Some(vec![tool_call]))];
        let out =
            super::build_anthropic_request(&messages, &[], "claude-test", 64, false, None, None);

        // Invalid JSON should be kept as a string
        assert_eq!(out["messages"][0]["content"][0]["input"], "not valid json");
    }

    #[test]
    fn stream_parameter_set_correctly() {
        let messages = vec![Message::user("Hello")];

        let out_stream_true =
            super::build_anthropic_request(&messages, &[], "claude-test", 64, true, None, None);
        assert_eq!(out_stream_true["stream"], true);

        let out_stream_false =
            super::build_anthropic_request(&messages, &[], "claude-test", 64, false, None, None);
        assert_eq!(out_stream_false["stream"], false);
    }

    #[test]
    fn max_tokens_included_in_request() {
        let messages = vec![Message::user("Hello")];
        let out =
            super::build_anthropic_request(&messages, &[], "claude-test", 2048, false, None, None);

        assert_eq!(out["max_tokens"], 2048);
    }

    #[test]
    fn assistant_reasoning_included_as_thinking_block() {
        let messages = vec![Message::assistant_with_reasoning(
            "Here is the answer.",
            None,
            Some("I thought about it.".to_string()),
        )];
        let out =
            super::build_anthropic_request(&messages, &[], "claude-test", 64, false, None, None);

        let content = out["messages"][0]["content"].as_array().unwrap();
        assert_eq!(content.len(), 2);
        // Thinking block must come first
        assert_eq!(content[0]["type"], "thinking");
        assert_eq!(content[0]["thinking"], "I thought about it.");
        // Followed by the text block
        assert_eq!(content[1]["type"], "text");
        assert_eq!(content[1]["text"], "Here is the answer.");
    }

    #[test]
    fn assistant_reasoning_included_with_tool_calls() {
        use bamboo_domain::{FunctionCall, ToolCall};
        let tool_call = ToolCall {
            id: "call_1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "search".to_string(),
                arguments: r#"{"q":"test"}"#.to_string(),
            },
        };
        let messages = vec![Message::assistant_with_reasoning(
            "",
            Some(vec![tool_call]),
            Some("Planning the search.".to_string()),
        )];
        let out =
            super::build_anthropic_request(&messages, &[], "claude-test", 64, false, None, None);

        let content = out["messages"][0]["content"].as_array().unwrap();
        assert_eq!(content.len(), 2);
        // Thinking block first
        assert_eq!(content[0]["type"], "thinking");
        assert_eq!(content[0]["thinking"], "Planning the search.");
        // Tool use block second (no text because content is empty)
        assert_eq!(content[1]["type"], "tool_use");
    }

    #[test]
    fn assistant_empty_reasoning_omits_thinking_block() {
        let messages = vec![Message::assistant_with_reasoning(
            "Hello",
            None,
            Some(String::new()),
        )];
        let out =
            super::build_anthropic_request(&messages, &[], "claude-test", 64, false, None, None);

        let content = out["messages"][0]["content"].as_array().unwrap();
        // Only text block, no thinking block for empty reasoning
        assert_eq!(content.len(), 1);
        assert_eq!(content[0]["type"], "text");
    }

    #[test]
    fn model_included_in_request() {
        let messages = vec![Message::user("Hello")];
        let out = super::build_anthropic_request(
            &messages,
            &[],
            "claude-3-opus-20240229",
            64,
            false,
            None,
            None,
        );

        assert_eq!(out["model"], "claude-3-opus-20240229");
    }
}

#[cfg(test)]
mod anthropic_provider_tests {
    use super::*;

    #[test]
    fn test_new_provider() {
        let provider = AnthropicProvider::new("test_api_key");
        assert_eq!(provider.api_key, "test_api_key");
        assert_eq!(provider.base_url, "https://api.anthropic.com/v1");
        assert_eq!(provider.max_tokens, 1024);
    }

    #[test]
    fn test_with_base_url() {
        let provider =
            AnthropicProvider::new("test_key").with_base_url("https://custom.anthropic.com");
        assert_eq!(provider.base_url, "https://custom.anthropic.com");
    }

    #[test]
    fn test_with_max_tokens() {
        let provider = AnthropicProvider::new("test_key").with_max_tokens(2048);
        assert_eq!(provider.max_tokens, 2048);
    }

    #[test]
    fn test_chained_builders() {
        let provider = AnthropicProvider::new("test_key")
            .with_base_url("https://custom.api.com")
            .with_max_tokens(4096);

        assert_eq!(provider.api_key, "test_key");
        assert_eq!(provider.base_url, "https://custom.api.com");
        assert_eq!(provider.max_tokens, 4096);
    }

    #[test]
    fn test_request_headers() {
        let provider = AnthropicProvider::new("test_key");
        let headers = provider
            .build_headers(request_overrides::ENDPOINT_MESSAGES, Some("claude-test"))
            .unwrap();

        assert!(headers.contains_key("x-api-key"));
        assert_eq!(
            headers.get("x-api-key").unwrap().to_str().unwrap(),
            "test_key"
        );

        assert!(headers.contains_key("anthropic-version"));
        assert_eq!(
            headers.get("anthropic-version").unwrap().to_str().unwrap(),
            "2023-06-01"
        );

        assert!(headers.contains_key("content-type"));
        assert_eq!(
            headers.get("content-type").unwrap().to_str().unwrap(),
            "application/json"
        );
    }

    #[test]
    fn test_headers_with_invalid_api_key() {
        // Test that headers with non-ASCII characters in API key fail
        let provider = AnthropicProvider::new("test\u{0000}key"); // null byte
        let result = provider.build_headers(request_overrides::ENDPOINT_MESSAGES, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_default_values() {
        let provider = AnthropicProvider::new("key");

        assert_eq!(provider.base_url, "https://api.anthropic.com/v1");
        assert_eq!(provider.max_tokens, 1024);
    }

    #[test]
    fn test_error_response_handling() {
        // Test error event parsing
        let mut state = AnthropicStreamState::default();
        let error_data =
            r#"{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}"#;

        let result = parse_anthropic_sse_event(&mut state, "error", error_data);
        assert!(result.is_err());

        match result {
            Err(LLMError::Api(msg)) => {
                assert!(msg.contains("Anthropic error event"));
            }
            _ => panic!("Expected LLMError::Api"),
        }
    }

    // ========== MODEL REQUIREMENT ARCHITECTURE TESTS ==========
    // These tests ensure the design principle:
    // "Provider must not have a default model field or with_model() method"

    /// Test: AnthropicProvider does NOT have a model field
    #[test]
    fn anthropic_provider_has_no_model_field() {
        // This test documents the provider structure:
        // pub struct AnthropicProvider {
        //     client: Client,
        //     api_key: String,
        //     base_url: String,
        //     max_tokens: u32,
        //     // NO model field!
        // }
        //
        // If someone adds a model field, this test should be updated
        // to reflect the architecture change.
        let provider = AnthropicProvider::new("test_key");
        // Verify we can access known fields
        assert_eq!(provider.api_key, "test_key");
        assert_eq!(provider.base_url, "https://api.anthropic.com/v1");
        assert_eq!(provider.max_tokens, 1024);
        // There is NO provider.model field to access
    }

    /// Test: AnthropicProvider does NOT have with_model() method
    #[test]
    fn anthropic_provider_has_no_with_model_method() {
        let provider = AnthropicProvider::new("test_key");

        // Available builder methods:
        let provider = provider
            .with_base_url("https://custom.api.com")
            .with_max_tokens(2048);

        // There is NO .with_model("gpt-4") method
        // Model is passed to chat_stream() as a parameter

        assert_eq!(provider.base_url, "https://custom.api.com");
        assert_eq!(provider.max_tokens, 2048);
    }
}
