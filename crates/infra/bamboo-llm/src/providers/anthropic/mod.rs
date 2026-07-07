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
use bamboo_domain::{Message, MessagePart, PromptBlock, Role};
use reqwest::{header::HeaderMap, Client};
use serde_json::{json, Value};

use crate::cache::{CacheTtl, PromptCachePlan, MAX_ANTHROPIC_CACHE_BREAKPOINTS};
use crate::prompt_ir::PromptIR;
use crate::provider::LLMRequestOptions;
use crate::provider::{LLMError, LLMProvider, LLMStream, Result};
use crate::providers::common::model_fetcher;
use crate::providers::common::request_overrides;
use crate::types::LLMChunk;
use bamboo_config::{KeywordMaskingConfig, RequestOverridesConfig};
use bamboo_domain::ReasoningEffort;

/// Anthropic Messages API provider.
pub struct AnthropicProvider {
    client: Client,
    api_key: String,
    base_url: String,
    max_tokens: u32,
    default_reasoning_effort: Option<ReasoningEffort>,
    request_overrides: Option<RequestOverridesConfig>,
    masking_config: KeywordMaskingConfig,
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
        // Shared, tightened heuristic (#237 finding 5). `budget_tokens` (Anthropic's
        // thinking-budget param) is in the shared reasoning-token set.
        crate::providers::common::looks_like_reasoning_unsupported_error(status, body)
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
        self.stream_messages_inner(messages, &[], tools, max_output_tokens, model, options)
            .await
    }

    /// Render the canonical [`PromptIR`] into the Anthropic wire: the structured
    /// system field rides `system_blocks` (per-block `cache_control`), and the
    /// body is `ir.body_chat()`. Anthropic re-sends the full message array each
    /// turn (the prompt-cache breakpoints absorb the stable prefix) and never uses
    /// `previous_response_id`, so the IR continuation is not consulted here. Falls
    /// back to `ir.flatten()` when there are no structured system blocks (the
    /// zero-tools path), byte-identical to the legacy lanes rendering.
    async fn chat_stream_ir(
        &self,
        ir: &PromptIR,
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> Result<LLMStream> {
        if ir.system_blocks.is_empty() {
            return self
                .stream_messages_inner(&ir.flatten(), &[], tools, max_output_tokens, model, options)
                .await;
        }
        let messages = ir.body_chat();
        self.stream_messages_inner(
            &messages,
            &ir.system_blocks,
            tools,
            max_output_tokens,
            model,
            options,
        )
        .await
    }

    async fn list_models(&self) -> Result<Vec<String>> {
        let headers = self.build_headers(request_overrides::ENDPOINT_MODELS, None)?;
        let url = format!("{}/models", self.base_url.trim_end_matches('/'));
        model_fetcher::fetch_model_list(&self.client, &url, headers, "Anthropic").await
    }
}

impl AnthropicProvider {
    /// Build and stream one Anthropic Messages request. `system_blocks`, when
    /// non-empty, is the canonical structured system field; otherwise the system
    /// is taken from the `System` messages in `messages`.
    #[allow(clippy::too_many_arguments)]
    async fn stream_messages_inner(
        &self,
        messages: &[Message],
        system_blocks: &[PromptBlock],
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
        let cache_plan = options.and_then(|o| o.cache.as_ref());
        let extended_cache_ttl = cache_plan
            .map(|plan| plan.ttl == CacheTtl::Extended)
            .unwrap_or(false);
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

        let mut body = build_anthropic_request_with_cache_blocks(
            messages,
            system_blocks,
            tools,
            model,
            max_tokens,
            true,
            reasoning_effort,
            parallel_tool_calls,
            cache_plan,
        );
        request_overrides::apply_overrides_to_body(
            &mut body,
            self.request_overrides.as_ref(),
            request_overrides::ENDPOINT_MESSAGES,
            Some(model),
        );
        // Last-moment scan: mask every text value in the fully-assembled body.
        crate::masking::mask_outbound_body(&mut body, &self.masking_config);
        // DIAGNOSTIC: count image blocks actually present in the OUTGOING request
        // body (top-level content blocks AND inside tool_result content arrays), so
        // we can tell with certainty whether a screenshot reaches the wire vs being
        // dropped before send. image_blocks_on_wire=0 with a screenshot in the
        // conversation means the image never left bamboo.
        let image_blocks_on_wire: usize = body
            .get("messages")
            .and_then(|m| m.as_array())
            .map(|msgs| {
                msgs.iter()
                    .filter_map(|m| m.get("content").and_then(|c| c.as_array()))
                    .flatten()
                    .map(|block| {
                        let mut n = usize::from(
                            block.get("type").and_then(|t| t.as_str()) == Some("image"),
                        );
                        if let Some(inner) = block.get("content").and_then(|c| c.as_array()) {
                            n += inner
                                .iter()
                                .filter(|b| b.get("type").and_then(|t| t.as_str()) == Some("image"))
                                .count();
                        }
                        n
                    })
                    .sum()
            })
            .unwrap_or(0);
        tracing::info!(
            "[{}] Anthropic request image_blocks_on_wire={} model='{}'",
            session_log_id,
            image_blocks_on_wire,
            model
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
        let mut headers = self.build_headers(request_overrides::ENDPOINT_MESSAGES, Some(model))?;
        if extended_cache_ttl {
            // 1-hour prompt cache TTL is gated behind a beta header.
            headers.insert(
                "anthropic-beta",
                reqwest::header::HeaderValue::from_static("extended-cache-ttl-2025-04-11"),
            );
        }

        // Retry the *initial* request establishment on transient failures
        // (429/5xx, connect/timeout). The closure rebuilds a fresh request each
        // attempt; the returned response body is unread, so streaming below is
        // unaffected (issue #18).
        let messages_url = format!("{}/messages", self.base_url);
        let mut response =
            crate::retry::send_with_retry(crate::retry::global(), "Anthropic", || {
                self.client
                    .post(&messages_url)
                    .headers(headers.clone())
                    .json(&body)
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
                    "Anthropic /messages rejected reasoning for model '{}'; retrying without reasoning_effort",
                    model
                );

                let mut fallback_body = build_anthropic_request_with_cache_blocks(
                    messages,
                    system_blocks,
                    tools,
                    model,
                    max_tokens,
                    true,
                    None,
                    parallel_tool_calls,
                    cache_plan,
                );
                request_overrides::apply_overrides_to_body(
                    &mut fallback_body,
                    self.request_overrides.as_ref(),
                    request_overrides::ENDPOINT_MESSAGES,
                    Some(model),
                );
                crate::masking::mask_outbound_body(&mut fallback_body, &self.masking_config);
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
                response =
                    crate::retry::send_with_retry(crate::retry::global(), "Anthropic", || {
                        self.client
                            .post(&messages_url)
                            .headers(headers.clone())
                            .json(&fallback_body)
                    })
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

        let stream =
            crate::providers::common::sse::llm_stream_from_sse(response, move |event, data| {
                parse_anthropic_sse_event(&mut state, event, data)
            });

        Ok(stream)
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
    build_anthropic_request_with_cache(
        messages,
        tools,
        model,
        max_tokens,
        stream,
        reasoning_effort,
        parallel_tool_calls,
        None,
    )
}

/// Build an Anthropic Messages API request body, placing prompt-cache
/// breakpoints according to a provider-agnostic [`PromptCachePlan`].
///
/// When `cache` is `None`, falls back to caching the stable system prompt and
/// tool definitions (always safe, since both are constant across a session).
/// Message-level breakpoints require the engine's knowledge of which messages
/// end a stable prefix, so they are opt-in via the plan's
/// `breakpoint_message_ids`. The total number of `cache_control` markers is
/// clamped to [`MAX_ANTHROPIC_CACHE_BREAKPOINTS`]; when there are more
/// candidates than the budget, the breakpoints nearest the end of the
/// conversation win (they cover the largest stable prefix).
/// Delegates to [`build_anthropic_request_with_cache_blocks`] with no structured
/// system blocks, so the system field is rendered from the `System` messages in
/// `messages` exactly as before.
#[allow(clippy::too_many_arguments)]
pub fn build_anthropic_request_with_cache(
    messages: &[Message],
    tools: &[ToolSchema],
    model: &str,
    max_tokens: u32,
    stream: bool,
    reasoning_effort: Option<ReasoningEffort>,
    parallel_tool_calls: Option<bool>,
    cache: Option<&PromptCachePlan>,
) -> Value {
    build_anthropic_request_with_cache_blocks(
        messages,
        &[],
        tools,
        model,
        max_tokens,
        stream,
        reasoning_effort,
        parallel_tool_calls,
        cache,
    )
}

/// Like [`build_anthropic_request_with_cache`] but renders the `system` field
/// from Bamboo's canonical structured `system_blocks` — one Anthropic system text
/// block per [`PromptBlock`] — when they are present. The single system
/// `cache_control` breakpoint still lands on the last block, so caching behavior
/// is unchanged; only the structure (N text blocks vs one joined block) differs.
/// With empty `system_blocks` this is byte-identical to the legacy path.
#[allow(clippy::too_many_arguments)]
pub fn build_anthropic_request_with_cache_blocks(
    messages: &[Message],
    system_blocks: &[PromptBlock],
    tools: &[ToolSchema],
    model: &str,
    max_tokens: u32,
    stream: bool,
    reasoning_effort: Option<ReasoningEffort>,
    parallel_tool_calls: Option<bool>,
    cache: Option<&PromptCachePlan>,
) -> Value {
    let default_plan = PromptCachePlan {
        cache_tools: true,
        cache_system: true,
        ..PromptCachePlan::default()
    };
    let plan = cache.unwrap_or(&default_plan);
    let ttl = plan.ttl;

    let (mut system, mut anthropic_messages, message_ids) =
        messages_to_anthropic_json(messages, system_blocks);

    // Anthropic honors at most MAX_ANTHROPIC_CACHE_BREAKPOINTS `cache_control`
    // markers per request. Spend the budget on the most stable regions first
    // (tools, then system), then on conversation breakpoints nearest the end.
    let mut budget = MAX_ANTHROPIC_CACHE_BREAKPOINTS;

    let mut tools_json = tools_to_anthropic_json(tools);
    if plan.cache_tools && budget > 0 {
        if let Some(last_tool) = tools_json.last_mut().and_then(|t| t.as_object_mut()) {
            last_tool.insert("cache_control".to_string(), cache_control_value(ttl));
            budget -= 1;
        }
    }

    if plan.cache_system && budget > 0 {
        if let Some(last_block) = system
            .as_mut()
            .and_then(|s| s.as_array_mut())
            .and_then(|blocks| blocks.last_mut())
            .and_then(|block| block.as_object_mut())
        {
            last_block.insert("cache_control".to_string(), cache_control_value(ttl));
            budget -= 1;
        }
    }

    if budget > 0 && !plan.breakpoint_message_ids.is_empty() {
        let mut breakpoint_indices: Vec<usize> = message_ids
            .iter()
            .enumerate()
            .filter_map(|(idx, ids)| ids.iter().any(|id| plan.is_breakpoint(id)).then_some(idx))
            .collect();
        // Keep only the breakpoints closest to the end of the conversation.
        if breakpoint_indices.len() > budget {
            breakpoint_indices = breakpoint_indices.split_off(breakpoint_indices.len() - budget);
        }
        for idx in breakpoint_indices {
            if let Some(message) = anthropic_messages.get_mut(idx) {
                add_cache_control_to_last_block(message, ttl);
            }
        }
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

/// Build a `cache_control` value, honoring an optional extended TTL.
fn cache_control_value(ttl: CacheTtl) -> Value {
    match ttl.anthropic_ttl() {
        Some(ttl) => json!({"type": "ephemeral", "ttl": ttl}),
        None => json!({"type": "ephemeral"}),
    }
}

/// Add a `cache_control` breakpoint to the last content block of an Anthropic
/// message, creating an incremental cache point at that conversation turn.
fn add_cache_control_to_last_block(message: &mut Value, ttl: CacheTtl) {
    if let Some(last_block) = message
        .get_mut("content")
        .and_then(|c| c.as_array_mut())
        .and_then(|blocks| blocks.last_mut())
        .and_then(|block| block.as_object_mut())
    {
        last_block.insert("cache_control".to_string(), cache_control_value(ttl));
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

/// Render Bamboo's canonical structured system blocks into an Anthropic `system`
/// value: an array with one `{ "type": "text", ... }` block per non-empty
/// [`PromptBlock`]. Returns `None` when there are no non-empty blocks, so callers
/// fall back to the legacy joined-text path.
fn system_blocks_to_anthropic_value(system_blocks: &[PromptBlock]) -> Option<Value> {
    let blocks: Vec<Value> = system_blocks
        .iter()
        .filter(|b| !b.text.trim().is_empty())
        .map(|b| json!({ "type": "text", "text": b.text }))
        .collect();
    (!blocks.is_empty()).then_some(Value::Array(blocks))
}

/// Convert internal messages to the Anthropic wire shape.
///
/// Returns the optional `system` block array, the message array, and a parallel
/// vector of the originating message id for each output message (so the caller
/// can place cache breakpoints by id, robust to the tool-result merging below).
///
/// When `system_blocks` is non-empty it is the canonical, structured source for
/// the system field (each block → its own text block); otherwise the system field
/// is the joined `System`-message text (legacy, byte-identical).
fn messages_to_anthropic_json(
    messages: &[Message],
    system_blocks: &[PromptBlock],
) -> (Option<Value>, Vec<Value>, Vec<Vec<String>>) {
    let mut system_parts: Vec<&str> = Vec::new();
    let mut out: Vec<Value> = Vec::new();
    // One entry per OUTPUT message: the source message id(s) folded into it. A
    // single output message can carry several ids after same-role coalescing
    // (#101); a cache breakpoint on ANY of them lands on that message.
    let mut out_ids: Vec<Vec<String>> = Vec::new();

    // Keep only the MOST RECENT tool-result image (e.g. screenshot); older ones
    // are dropped from the request to control context size, since a conversation
    // can accumulate many large images. (User-attached images are untouched.)
    let last_image_tool_idx = messages
        .iter()
        .enumerate()
        .filter(|(_, m)| matches!(m.role, Role::Tool) && message_has_image(m))
        .map(|(i, _)| i)
        .next_back();

    for (idx, m) in messages.iter().enumerate() {
        match m.role {
            Role::System => system_parts.push(m.content.as_str()),
            Role::User | Role::Assistant | Role::Tool => {
                let keep_image = Some(idx) == last_image_tool_idx;
                // `message_to_anthropic_json` returns `None` only for a stray
                // System message; skip it (rather than emit a null/empty entry)
                // so a malformed conversation never pollutes the `messages`
                // array nor crashes the call (issue #22).
                let Some(msg_json) = message_to_anthropic_json(m, keep_image) else {
                    continue;
                };
                // Coalesce consecutive SAME-ROLE messages into one. Anthropic
                // requires strict user/assistant alternation and 400s on two
                // consecutive same-role turns, so concatenating their content
                // blocks keeps alternation valid no matter what upstream produced
                // (defense-in-depth, #101). This also subsumes the original
                // tool_result merge (#29/#22): a `Tool` message serializes to a
                // `user` message, so consecutive tool_results — and a tool_result
                // following any user turn — fold into that same user message, with
                // every tool_result for one assistant tool_use turn in one message
                // as the API requires. The merged-into message keeps its original
                // id, so a cache breakpoint on that turn still maps (its id stays in
                // `out_ids`; the merged-in message's id is intentionally dropped).
                if let Some(last) = out.last_mut() {
                    let same_role = last.get("role").and_then(|r| r.as_str())
                        == msg_json.get("role").and_then(|r| r.as_str());
                    if same_role {
                        if let (Some(last_content), Some(new_content)) = (
                            last.get_mut("content").and_then(|c| c.as_array_mut()),
                            msg_json.get("content").and_then(|c| c.as_array()),
                        ) {
                            // Drop any `thinking` block from the appended content:
                            // Anthropic requires `thinking` to be the FIRST block of
                            // an assistant turn, so a merged-in turn's thinking would
                            // land in an illegal interior position. (Two consecutive
                            // assistant turns don't occur in a well-formed
                            // conversation, but coalescing must not itself produce an
                            // invalid block order. User merges carry no thinking
                            // blocks, so this is a no-op there.) #101.
                            last_content.extend(
                                new_content
                                    .iter()
                                    .filter(|block| {
                                        block.get("type").and_then(|t| t.as_str())
                                            != Some("thinking")
                                    })
                                    .cloned(),
                            );
                            // Record the merged-in id on the SAME output message so a
                            // cache breakpoint targeting it still lands here — its
                            // content now lives at the end of this message. #101.
                            if let Some(last_ids) = out_ids.last_mut() {
                                last_ids.push(m.id.clone());
                            }
                            continue;
                        }
                    }
                }
                out.push(msg_json);
                out_ids.push(vec![m.id.clone()]);
            }
        }
    }

    // The system prompt's cache breakpoint is applied by the caller based on the
    // cache plan, since whether the system prompt is stable enough to cache is a
    // policy decision, not a serialization detail.
    // Structured `system_blocks` (the canonical content-block form) supersede the
    // joined System-message text when present: each block renders as its own
    // Anthropic system text block, so the provider consumes Bamboo's block array
    // structurally. With no blocks, fall back to the legacy join (byte-identical).
    let system = system_blocks_to_anthropic_value(system_blocks).or_else(|| {
        (!system_parts.is_empty())
            .then(|| json!([{ "type": "text", "text": system_parts.join("\n\n") }]))
    });

    (system, out, out_ids)
}

/// Whether a message carries at least one image in its content parts.
fn message_has_image(message: &Message) -> bool {
    message.content_parts.as_ref().is_some_and(|parts| {
        parts
            .iter()
            .any(|p| matches!(p, MessagePart::ImageUrl { .. }))
    })
}

/// `keep_image`: when false, a tool result's images are dropped (replaced by a
/// short note) so only the most recent screenshot is sent — see
/// `messages_to_anthropic_json`.
///
/// Returns `None` for a stray `System` message (which should have been routed
/// into the top-level `system` field by the caller). Skipping it — rather than
/// panicking — keeps a malformed/legacy conversation from crashing the process
/// during an LLM call (issue #22). Callers consume this with `filter_map` /
/// `let-else` so a skipped message is omitted entirely, never turned into a
/// `null`/empty entry in the `messages` array.
fn message_to_anthropic_json(message: &Message, keep_image: bool) -> Option<Value> {
    match message.role {
        // A System message belongs in the top-level `system` field, not the
        // `messages` array; the caller (`messages_to_anthropic_json`) routes it
        // there. But a malformed/legacy session loaded from storage, or a future
        // bug in system extraction, could surface one here — so log and skip it
        // instead of bringing down the whole process mid-call (issue #22).
        Role::System => {
            tracing::warn!(
                "Anthropic conversion received a System message in the conversation array; \
                 skipping it (system messages belong in the top-level `system` field)"
            );
            None
        }
        Role::User => Some(json!({
            "role": "user",
            "content": user_content_to_anthropic_blocks(message),
        })),
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

            Some(json!({
                "role": "assistant",
                "content": blocks,
            }))
        }
        Role::Tool => {
            let Some(tool_use_id) = message.tool_call_id.as_deref() else {
                tracing::warn!(
                    "Anthropic conversion received tool message without tool_call_id; emitting plain text block"
                );
                return Some(json!({
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": message.content,
                        }
                    ],
                }));
            };

            // Tool results that carry images (e.g. an MCP `screenshot`) embed the
            // picture as blocks; Anthropic's tool_result `content` accepts an
            // array of text + image blocks. Only the most recent screenshot is
            // kept (keep_image); older ones are dropped to control context size.
            let image_blocks: Vec<Value> = if keep_image {
                message
                    .content_parts
                    .as_ref()
                    .map(|parts| {
                        parts
                            .iter()
                            .filter(|p| matches!(p, MessagePart::ImageUrl { .. }))
                            .filter_map(content_part_to_anthropic_block)
                            .collect()
                    })
                    .unwrap_or_default()
            } else {
                Vec::new()
            };

            let tool_result_content = if !image_blocks.is_empty() {
                let mut blocks = Vec::with_capacity(image_blocks.len() + 1);
                if !message.content.is_empty() {
                    blocks.push(json!({ "type": "text", "text": message.content }));
                }
                blocks.extend(image_blocks);
                json!(blocks)
            } else if !keep_image && message_has_image(message) {
                // This tool result had a screenshot we dropped — note it.
                json!(format!(
                    "{}\n[earlier screenshot omitted to save context; take a new one if needed]",
                    message.content
                ))
            } else {
                json!(message.content)
            };

            Some(json!({
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": tool_use_id,
                        "content": tool_result_content,
                    }
                ],
            }))
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

/// Convert a tool-call `arguments` string into an Anthropic `tool_use.input`.
///
/// Anthropic requires `input` to be a JSON **object**, so anything that is not
/// one maps to `{ "_raw": <original> }`: empty/whitespace (a zero-argument tool
/// call → `{}`), invalid JSON, or valid-but-non-object JSON (`"5"`, `[1,2]`,
/// `true`, `"str"`). A non-object `input` is rejected with 400
/// `invalid_request_error` and — because the `tool_use` is persisted in
/// conversation history — then poisons every subsequent request in the session.
///
/// Shared by the outbound request builder (`tool_call_to_tool_use_block`) and
/// the response conversion (`conversion::convert_messages_response`) so the two
/// sites cannot drift.
pub(super) fn tool_arguments_to_input(arguments: &str) -> Value {
    let trimmed = arguments.trim();
    if trimmed.is_empty() {
        return json!({});
    }
    match serde_json::from_str::<Value>(trimmed) {
        Ok(value) if value.is_object() => value,
        Ok(_) => {
            tracing::warn!(
                "Anthropic tool_use input fallback to _raw object: arguments are valid JSON but not an object, args_len={}, preview=\"{}\"",
                trimmed.len(),
                preview_for_log(trimmed, 180),
            );
            json!({ "_raw": arguments })
        }
        Err(error) => {
            tracing::warn!(
                "Anthropic tool_use input fallback to _raw object: invalid JSON arguments, args_len={}, preview=\"{}\", error={}",
                trimmed.len(),
                preview_for_log(trimmed, 180),
                error
            );
            json!({ "_raw": arguments })
        }
    }
}

fn tool_call_to_tool_use_block(tool_call: &bamboo_domain::ToolCall) -> Value {
    json!({
        "type": "tool_use",
        "id": tool_call.id,
        "name": tool_call.function.name,
        "input": tool_arguments_to_input(&tool_call.function.arguments),
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
                        // Non-cached fresh input — reported once, here in
                        // message_start. Disjoint from the two cache counts.
                        let input_tokens = usage
                            .get("input_tokens")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                        if cache_creation > 0 || cache_read > 0 || input_tokens > 0 {
                            tracing::info!(
                                "Anthropic stream message_start input={} cache_creation={} cache_read={}",
                                input_tokens,
                                cache_creation,
                                cache_read,
                            );
                            return Ok(Some(LLMChunk::CacheUsage {
                                cache_creation_input_tokens: cache_creation,
                                cache_read_input_tokens: cache_read,
                                input_tokens,
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
                            let input_tokens = usage
                                .get("input_tokens")
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

                            // Emit CacheUsage if any cache activity. input_tokens
                            // is normally only present in message_start; pass it
                            // through if a delta echoes it (the handler de-dups).
                            if cache_creation > 0 || cache_read > 0 {
                                return Ok(Some(LLMChunk::CacheUsage {
                                    cache_creation_input_tokens: cache_creation,
                                    cache_read_input_tokens: cache_read,
                                    input_tokens,
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
            // Tolerate benign shape deviations (common with Anthropic→OpenAI
            // aggregators): skip the malformed event instead of returning a
            // stream error, which would discard the whole already-streamed
            // assistant turn. (#237)
            let Some(index) = v.get("index").and_then(|i| i.as_u64()) else {
                tracing::warn!("Anthropic content_block_start missing index; skipping: {data}");
                return Ok(None);
            };
            let Some(content_block) = v.get("content_block") else {
                tracing::warn!(
                    "Anthropic content_block_start missing content_block; skipping: {data}"
                );
                return Ok(None);
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
                tracing::warn!("Anthropic tool_use content_block missing id; skipping: {data}");
                return Ok(None);
            };
            let Some(name) = content_block.get("name").and_then(|s| s.as_str()) else {
                tracing::warn!("Anthropic tool_use content_block missing name; skipping: {data}");
                return Ok(None);
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
                    // Skip (don't abort the turn) on benign deviations — e.g. an
                    // aggregator streaming an input_json_delta for an index it
                    // never announced via content_block_start. (#237)
                    let Some(index) = v.get("index").and_then(|i| i.as_u64()) else {
                        tracing::warn!(
                            "Anthropic input_json_delta missing index; skipping: {data}"
                        );
                        return Ok(None);
                    };
                    let partial = delta
                        .get("partial_json")
                        .and_then(|p| p.as_str())
                        .unwrap_or_default();

                    let index = index as usize;
                    let Some((id, name)) = state.tool_uses_by_index.get(&index) else {
                        tracing::warn!(
                            "Anthropic input_json_delta for unannounced tool_use index {index}; skipping: {data}"
                        );
                        return Ok(None);
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
    fn structured_system_blocks_render_as_discrete_blocks_with_terminal_cache_control() {
        use bamboo_domain::{ContextBlockType, PromptBlock};
        // The canonical structured system field: three blocks in, three Anthropic
        // system text blocks out — the provider consumes Bamboo's block array
        // structurally instead of a pre-joined string.
        let system_blocks = vec![
            PromptBlock::new("base", ContextBlockType::Base, "BASE identity"),
            PromptBlock::new(
                "core_directives",
                ContextBlockType::CoreDirectives,
                "CORE rules",
            ),
            PromptBlock::new("env", ContextBlockType::EnvSnapshot, "ENV snapshot"),
        ];
        let messages = vec![Message::user("Hi")];

        let out = super::build_anthropic_request_with_cache_blocks(
            &messages,
            &system_blocks,
            &[],
            "claude-test",
            64,
            false,
            None,
            None,
            None,
        );

        let system = out["system"].as_array().expect("system is a block array");
        assert_eq!(system.len(), 3, "one wire block per PromptBlock");
        assert_eq!(system[0]["text"], "BASE identity");
        assert_eq!(system[1]["text"], "CORE rules");
        assert_eq!(system[2]["text"], "ENV snapshot");
        assert!(system.iter().all(|b| b["type"] == "text"));
        // Exactly ONE system cache breakpoint, on the LAST block (the default plan
        // caches the system) — identical caching to the single-joined-block form.
        assert!(system[0].get("cache_control").is_none());
        assert!(system[1].get("cache_control").is_none());
        assert_eq!(system[2]["cache_control"]["type"], "ephemeral");
        // The user turn is untouched in the message array.
        assert_eq!(out["messages"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn empty_system_blocks_fall_back_to_joined_system_messages() {
        use bamboo_domain::PromptBlock;
        // With no structured blocks, the system comes from the `System` messages,
        // byte-identical to the legacy path (multiple messages join into one block).
        let messages = vec![
            Message::system("You are helpful."),
            Message::system("Be concise."),
            Message::user("Hi"),
        ];
        let no_blocks: Vec<PromptBlock> = Vec::new();

        let out = super::build_anthropic_request_with_cache_blocks(
            &messages,
            &no_blocks,
            &[],
            "claude-test",
            64,
            false,
            None,
            None,
            None,
        );
        let system = out["system"].as_array().expect("system array");
        assert_eq!(system.len(), 1);
        assert_eq!(system[0]["text"], "You are helpful.\n\nBe concise.");
    }

    #[test]
    fn tool_result_with_image_emits_text_and_image_blocks() {
        // An MCP screenshot result: text note + one image.
        let msg = Message::tool_result_with_images(
            "toolu_1",
            "screenshot 1280x536",
            true,
            vec![bamboo_domain::ToolResultImage {
                mime_type: "image/jpeg".to_string(),
                data: "AAAA".to_string(),
            }],
        );
        let v =
            super::message_to_anthropic_json(&msg, true).expect("tool message should serialize");
        let block = &v["content"][0];
        assert_eq!(block["type"], "tool_result");
        assert_eq!(block["tool_use_id"], "toolu_1");
        let arr = block["content"]
            .as_array()
            .expect("tool_result content should be an array when images are present");
        assert!(arr
            .iter()
            .any(|b| b["type"] == "text" && b["text"] == "screenshot 1280x536"));
        let img = arr
            .iter()
            .find(|b| b["type"] == "image")
            .expect("an image block");
        assert_eq!(img["source"]["type"], "base64");
        assert_eq!(img["source"]["media_type"], "image/jpeg");
        assert_eq!(img["source"]["data"], "AAAA");
    }

    #[test]
    fn tool_result_without_image_stays_plain_string() {
        // Regression: text-only tool results keep the cheap string form.
        let msg = Message::tool_result("toolu_2", "plain text");
        let v =
            super::message_to_anthropic_json(&msg, true).expect("tool message should serialize");
        assert_eq!(v["content"][0]["type"], "tool_result");
        assert_eq!(v["content"][0]["content"], "plain text");
    }

    #[test]
    fn older_tool_image_is_dropped_keeping_only_latest() {
        // With keep_image=false, an image-bearing tool result drops the image and
        // notes the omission (only the most recent screenshot is sent).
        let msg = Message::tool_result_with_images(
            "toolu_old",
            "screenshot 1",
            true,
            vec![bamboo_domain::ToolResultImage {
                mime_type: "image/jpeg".to_string(),
                data: "OLD".to_string(),
            }],
        );
        let v =
            super::message_to_anthropic_json(&msg, false).expect("tool message should serialize");
        let content = &v["content"][0]["content"];
        // No image block — content is a plain string with the omission note.
        assert!(content.is_string(), "dropped image should leave a string");
        assert!(content.as_str().unwrap().contains("omitted"));
    }

    #[test]
    fn stray_system_message_is_skipped_not_panicked() {
        // Regression for issue #22: a System message reaching the per-message
        // serializer (a malformed/legacy session loaded from storage, or a future
        // bug in system extraction) must NOT panic the process. It is logged and
        // dropped (None), and the surrounding User/Assistant messages survive
        // intact in the resulting Anthropic `messages` array.
        use serde_json::Value;

        // (a) Direct: a System message serializes to None instead of panicking.
        let system_msg = Message::system("You are a robot.");
        assert!(
            super::message_to_anthropic_json(&system_msg, true).is_none(),
            "a System message must serialize to None (skipped), not panic"
        );

        // (b) In context: the conversation array is consumed the way an
        // Option-returning serializer should be — None entries are filtered out
        // (filter_map), the rest survive. Reaching these assertions also proves
        // the System message did NOT panic.
        let conversation = [
            Message::system("You are a robot."),
            Message::user("Hello"),
            Message::assistant("Hi there!", None),
        ];
        let serialized: Vec<Value> = conversation
            .iter()
            .filter_map(|m| super::message_to_anthropic_json(m, true))
            .collect();

        // The System message is omitted; User + Assistant survive, in order.
        assert_eq!(
            serialized.len(),
            2,
            "system message must be skipped, leaving user + assistant"
        );
        assert_eq!(serialized[0]["role"], "user");
        assert_eq!(serialized[0]["content"][0]["text"], "Hello");
        assert_eq!(serialized[1]["role"], "assistant");
        // No null/placeholder entry sneaks in for the skipped system message.
        assert!(
            serialized.iter().all(|m| !m.is_null()),
            "a skipped system message must be omitted, not emitted as null"
        );
    }

    #[test]
    fn messages_to_anthropic_json_skips_stray_system_on_production_path() {
        // Regression for issue #22, but exercising the REAL production path.
        // `messages_to_anthropic_json` is the function `build_anthropic_request`
        // actually calls; unlike the synthetic `filter_map` consumer in
        // `stray_system_message_is_skipped_not_panicked`, this runs the
        // provider's own System-routing arm (→ top-level `system` field) AND
        // the let-else skip + out_ids population, so it catches regressions in
        // how the two are wired together.
        let system = Message::system("You are a robot.");
        let user = Message::user("Hello");
        let assistant = Message::assistant("Hi there!", None);

        // Capture the originating ids so we can assert out_ids tracks them
        // exactly — the System message's id must NOT appear in out_ids.
        let user_id = user.id.clone();
        let assistant_id = assistant.id.clone();

        let messages = [system, user, assistant];
        let (system_val, out, out_ids) = super::messages_to_anthropic_json(&messages, &[]);

        // (a) The System message is routed into the top-level `system` field.
        let system_value =
            system_val.expect("the System message must populate the top-level `system` field");
        let system_arr = system_value
            .as_array()
            .expect("`system` should be an array of text blocks");
        assert_eq!(system_arr.len(), 1);
        assert_eq!(system_arr[0]["type"], "text");
        assert_eq!(system_arr[0]["text"], "You are a robot.");

        // (b) The `messages` array omits the System message entirely — no null
        // entry, order preserved, User then Assistant.
        assert_eq!(
            out.len(),
            2,
            "system message must be skipped, leaving user + assistant"
        );
        assert_eq!(out[0]["role"], "user");
        assert_eq!(out[0]["content"][0]["type"], "text");
        assert_eq!(out[0]["content"][0]["text"], "Hello");
        assert_eq!(out[1]["role"], "assistant");
        assert!(
            out.iter().all(|m| !m.is_null()),
            "a skipped system message must be omitted, not emitted as null"
        );

        // (c) out_ids mirrors the surviving messages 1:1 — the System message's
        // id is absent, the User/Assistant ids are present in order. Each output
        // message carries its source id(s); here no same-role coalescing occurs so
        // each slot holds exactly one id.
        assert_eq!(out_ids.len(), 2);
        assert_eq!(out_ids[0], vec![user_id]);
        assert_eq!(out_ids[1], vec![assistant_id]);

        // (d) KEY INVARIANT: the parallel id vector stays in lockstep with the
        // messages array, so cache-breakpoint placement by id never desyncs.
        assert_eq!(
            out.len(),
            out_ids.len(),
            "out_ids must stay parallel to the messages array"
        );
    }

    #[test]
    fn messages_to_anthropic_json_skips_multiple_stray_system_messages() {
        // Every stray System message is routed to the top-level `system` field;
        // none survives into the `messages` array, no matter how many appear.
        let messages = [
            Message::system("Rule one."),
            Message::system("Rule two."),
            Message::user("Hello"),
            Message::assistant("Hi!", None),
        ];
        let (system_val, out, out_ids) = super::messages_to_anthropic_json(&messages, &[]);

        // Both system messages are joined into the system field.
        let system_value =
            system_val.expect("system messages must populate the top-level `system` field");
        let system_arr = system_value
            .as_array()
            .expect("`system` should be an array of text blocks");
        assert_eq!(system_arr.len(), 1);
        assert_eq!(system_arr[0]["text"], "Rule one.\n\nRule two.");

        // Only User + Assistant survive in the messages array.
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["role"], "user");
        assert_eq!(out[1]["role"], "assistant");
        assert!(
            out.iter().all(|m| !m.is_null()),
            "skipped system messages must be omitted, not emitted as null"
        );

        // Parity invariant holds with multiple skipped messages.
        assert_eq!(
            out.len(),
            out_ids.len(),
            "out_ids must stay parallel to the messages array"
        );
    }

    #[test]
    fn tool_arguments_to_input_always_yields_an_object() {
        use super::tool_arguments_to_input;
        // Anthropic requires `tool_use.input` to be a JSON object; a non-object
        // input is a 400 `invalid_request_error` that then poisons every
        // subsequent request in the session (the reported session-brick).

        // empty / whitespace-only (a zero-argument tool call) -> {}
        assert_eq!(tool_arguments_to_input(""), serde_json::json!({}));
        assert_eq!(tool_arguments_to_input("   "), serde_json::json!({}));

        // a real object passes through unchanged
        assert_eq!(
            tool_arguments_to_input(r#"{"q":"rust"}"#),
            serde_json::json!({ "q": "rust" })
        );

        // valid-but-non-object JSON must NOT pass through -> `_raw` object
        for non_object in ["42", "true", "null", "[1,2,3]", "\"a string\""] {
            let v = tool_arguments_to_input(non_object);
            assert!(
                v.is_object(),
                "{non_object:?} must map to an object, got {v}"
            );
            assert_eq!(v["_raw"], non_object);
        }

        // invalid JSON -> `_raw` object
        let v = tool_arguments_to_input("not json");
        assert!(v.is_object());
        assert_eq!(v["_raw"], "not json");
    }

    #[test]
    fn messages_to_anthropic_json_skips_system_between_tool_use_and_result() {
        // A stray System message wedged between an assistant tool_use and its
        // tool_result must be routed to `system` and NOT break the pairing:
        // tool_use and tool_result stay adjacent in the messages array.
        let tool_call = ToolCall {
            id: "call_1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "search".to_string(),
                arguments: r#"{"q":"rust"}"#.to_string(),
            },
        };
        let messages = [
            Message::assistant("", Some(vec![tool_call])),
            Message::system("mid-conversation system"),
            Message::tool_result("call_1", "found it"),
        ];
        let (system_val, out, out_ids) = super::messages_to_anthropic_json(&messages, &[]);

        // The wedged System message is routed to the system field, not dropped.
        assert!(
            system_val.is_some(),
            "the mid-conversation System message must populate the `system` field"
        );

        // Two messages survive, adjacent and paired: assistant tool_use then
        // user tool_result referencing the same tool_use_id.
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["role"], "assistant");
        assert_eq!(out[0]["content"][0]["type"], "tool_use");
        assert_eq!(out[0]["content"][0]["id"], "call_1");
        assert_eq!(out[1]["role"], "user");
        assert_eq!(out[1]["content"][0]["type"], "tool_result");
        assert_eq!(out[1]["content"][0]["tool_use_id"], "call_1");
        assert!(
            out.iter().all(|m| !m.is_null()),
            "skipped system message must be omitted, not emitted as null"
        );

        // Parity invariant holds even with the interleaved System message.
        assert_eq!(
            out.len(),
            out_ids.len(),
            "out_ids must stay parallel to the messages array"
        );
    }

    #[test]
    fn messages_keep_only_the_most_recent_tool_image() {
        let img = |d: &str| bamboo_domain::ToolResultImage {
            mime_type: "image/jpeg".to_string(),
            data: d.to_string(),
        };
        let messages = vec![
            Message::user("look"),
            Message::tool_result_with_images("t1", "shot1", true, vec![img("FIRST")]),
            Message::tool_result_with_images("t2", "shot2", true, vec![img("LAST")]),
        ];
        let out =
            super::build_anthropic_request(&messages, &[], "claude-test", 64, false, None, None);
        let dumped = out.to_string();
        // The most recent image survives; the older one is dropped.
        assert!(dumped.contains("LAST"), "latest screenshot must be sent");
        assert!(
            !dumped.contains("FIRST"),
            "older screenshot must be dropped"
        );
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
    fn plan_places_cache_breakpoint_on_message_by_id() {
        let flagged = Message::user("Old context here");
        let flagged_id = flagged.id.clone();
        let messages = vec![
            Message::user("Hi"),
            flagged,
            Message::assistant("Got it", None),
        ];
        let plan = crate::cache::PromptCachePlan {
            breakpoint_message_ids: vec![flagged_id],
            ..Default::default()
        };

        let out = super::build_anthropic_request_with_cache(
            &messages,
            &[],
            "claude-test",
            64,
            false,
            None,
            None,
            Some(&plan),
        );

        let msgs = out["messages"].as_array().unwrap();
        // "Hi" and the flagged "Old context here" are consecutive user turns, so
        // they coalesce into ONE user message (#101). The flagged id was folded in,
        // so its breakpoint lands on that merged message's LAST block (the flagged
        // content), not on a separate message.
        assert_eq!(msgs.len(), 2, "the two consecutive user turns coalesced");
        let merged_user = msgs[0]["content"].as_array().unwrap();
        assert!(
            merged_user[0].get("cache_control").is_none(),
            "the leading 'Hi' block is not the breakpoint"
        );
        assert_eq!(
            merged_user[1]["cache_control"]["type"], "ephemeral",
            "the breakpoint lands on the flagged content (the last block)"
        );
        assert!(msgs[1]["content"].as_array().unwrap()[0]
            .get("cache_control")
            .is_none());
    }

    #[test]
    fn stable_prefix_caches_system_and_relocated_tool_guide() {
        // Mirrors a prompt-lanes request: a static system identity, the relocated
        // tool/server guide as its own fixed prefix message, then the
        // conversation. The cacheable PREFIX (system + guide) and the rolling
        // conversation tail must each carry a `cache_control` marker; the middle
        // turns must not. This is the prefix-prompt-cache guarantee of the
        // tool-guide relocation.
        let guide = Message::user("Tool & Connected-Server Guide: nova targeting workflow");
        let guide_id = guide.id.clone();
        let tail = Message::user("the current ask");
        let tail_id = tail.id.clone();
        let messages = vec![
            Message::system("BASE_IDENTITY"),
            guide,
            Message::user("earlier turn"),
            Message::assistant("ok", None),
            tail,
        ];
        let plan = crate::cache::PromptCachePlan {
            cache_system: true,
            cache_tools: true,
            breakpoint_message_ids: vec![guide_id, tail_id],
            ..Default::default()
        };

        let out = super::build_anthropic_request_with_cache(
            &messages,
            &[],
            "claude-test",
            64,
            false,
            None,
            None,
            Some(&plan),
        );

        // Static system identity is cached (top of the stable prefix).
        let system = out["system"].as_array().expect("system blocks");
        assert_eq!(
            system.last().unwrap()["cache_control"]["type"],
            "ephemeral",
            "static system identity must be cached"
        );

        // After system extraction the messages are [guide, earlier, assistant, tail].
        let msgs = out["messages"].as_array().unwrap();
        let cc = |m: &serde_json::Value| -> bool {
            m["content"]
                .as_array()
                .and_then(|b| b.last())
                .map(|b| b.get("cache_control").is_some())
                .unwrap_or(false)
        };
        // The relocated guide closes the stable prefix and is cached.
        assert!(cc(&msgs[0]), "relocated tool guide must be cached");
        // A middle conversation turn must NOT be cached.
        assert!(!cc(&msgs[1]), "middle turn must not be cached");
        // The rolling conversation tail is cached.
        assert!(cc(msgs.last().unwrap()), "conversation tail must be cached");
    }

    #[test]
    fn breakpoint_survives_tool_result_merge_by_id() {
        // The first tool result of a turn creates a user message; subsequent
        // tool results merge into it (keeping the first result's id). A
        // breakpoint placed on that turn's id must still land on the merged
        // message even though it now holds multiple tool_result blocks.
        let assistant = Message::assistant(
            "",
            Some(vec![
                ToolCall {
                    id: "call_1".to_string(),
                    tool_type: "function".to_string(),
                    function: FunctionCall {
                        name: "f".to_string(),
                        arguments: "{}".to_string(),
                    },
                },
                ToolCall {
                    id: "call_2".to_string(),
                    tool_type: "function".to_string(),
                    function: FunctionCall {
                        name: "g".to_string(),
                        arguments: "{}".to_string(),
                    },
                },
            ]),
        );
        let first_result = Message::tool_result("call_1", "output one");
        let first_result_id = first_result.id.clone();
        let messages = vec![
            assistant,
            first_result,
            Message::tool_result("call_2", "output two"),
        ];
        let plan = crate::cache::PromptCachePlan {
            breakpoint_message_ids: vec![first_result_id],
            ..Default::default()
        };

        let out = super::build_anthropic_request_with_cache(
            &messages,
            &[],
            "claude-test",
            64,
            false,
            None,
            None,
            Some(&plan),
        );

        let msgs = out["messages"].as_array().unwrap();
        assert_eq!(
            msgs.len(),
            2,
            "both tool results merge into one user message"
        );
        let user = &msgs[1];
        assert_eq!(user["role"], "user");
        let blocks = user["content"].as_array().unwrap();
        assert_eq!(
            blocks.len(),
            2,
            "both tool results present in merged message"
        );
        assert_eq!(blocks.last().unwrap()["cache_control"]["type"], "ephemeral");
    }

    #[test]
    fn extended_ttl_emits_one_hour_cache_control() {
        let plan = crate::cache::PromptCachePlan {
            cache_system: true,
            ttl: crate::cache::CacheTtl::Extended,
            ..Default::default()
        };
        let messages = vec![Message::system("Stable prompt"), Message::user("Hi")];

        let out = super::build_anthropic_request_with_cache(
            &messages,
            &[],
            "claude-test",
            64,
            false,
            None,
            None,
            Some(&plan),
        );

        assert_eq!(out["system"][0]["cache_control"]["type"], "ephemeral");
        assert_eq!(out["system"][0]["cache_control"]["ttl"], "1h");
    }

    #[test]
    fn cache_breakpoints_are_clamped_to_provider_budget() {
        // tools + system + 6 flagged messages, but only 4 breakpoints are
        // allowed, so 2 messages (the last two) keep their markers.
        let mut messages = vec![Message::system("Stable prompt")];
        let mut flagged_ids = Vec::new();
        for i in 0..6 {
            // Alternate roles so the 6 flagged turns stay DISTINCT output messages
            // (consecutive same-role turns would coalesce, #101); the point here is
            // breakpoint CLAMPING, which needs more candidate messages than budget.
            let m = if i % 2 == 0 {
                Message::user(format!("turn {i}"))
            } else {
                Message::assistant(format!("turn {i}"), None)
            };
            flagged_ids.push(m.id.clone());
            messages.push(m);
        }
        let plan = crate::cache::PromptCachePlan {
            cache_tools: true,
            cache_system: true,
            breakpoint_message_ids: flagged_ids,
            ..Default::default()
        };

        let out = super::build_anthropic_request_with_cache(
            &messages,
            &sample_tools(),
            "claude-test",
            64,
            false,
            None,
            None,
            Some(&plan),
        );

        let tool_breaks = out["tools"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|t| t.get("cache_control").is_some())
            .count();
        let system_breaks = out["system"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|b| b.get("cache_control").is_some())
            .count();
        let message_breaks = out["messages"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|m| {
                m["content"]
                    .as_array()
                    .and_then(|blocks| blocks.last())
                    .map(|b| b.get("cache_control").is_some())
                    .unwrap_or(false)
            })
            .count();

        assert_eq!(tool_breaks, 1);
        assert_eq!(system_breaks, 1);
        assert_eq!(
            message_breaks, 2,
            "only the remaining budget of 2 message breakpoints"
        );
        assert!(
            tool_breaks + system_breaks + message_breaks <= super::MAX_ANTHROPIC_CACHE_BREAKPOINTS
        );
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
    fn tool_result_coalesces_with_adjacent_user_turn_for_alternation() {
        // A `Tool` message serializes to a `user` message, so a tool_result
        // adjacent to another user turn coalesces into ONE user message — Anthropic
        // requires strict user/assistant alternation and 400s on consecutive
        // same-role turns (#101). (This particular input is degenerate — a
        // tool_result with no preceding tool_use — but coalescing is still the
        // alternation-valid outcome, and the realistic [tool_use, tool_result,
        // user] case is what this prevents 400-ing.)
        let messages = vec![
            Message::user("normal user text"),
            Message::tool_result("call_1", "OK"),
        ];

        let out =
            super::build_anthropic_request(&messages, &[], "claude-test", 64, false, None, None);

        let built_messages = out["messages"].as_array().expect("messages array");
        assert_eq!(
            built_messages.len(),
            1,
            "consecutive user-role turns coalesce into one message"
        );
        assert_eq!(built_messages[0]["role"], "user");
        assert_eq!(built_messages[0]["content"][0]["text"], "normal user text");
        assert_eq!(built_messages[0]["content"][1]["type"], "tool_result");
        assert_eq!(built_messages[0]["content"][1]["tool_use_id"], "call_1");
    }

    #[test]
    fn consecutive_same_role_turns_coalesce_into_strict_alternation() {
        // The acceptance case (#101): [user, user, assistant, assistant] must
        // become a strictly alternating [user, assistant] with content concatenated
        // in order, so Anthropic never 400s on consecutive same-role turns.
        let messages = vec![
            Message::user("u1"),
            Message::user("u2"),
            Message::assistant("a1", None),
            Message::assistant("a2", None),
        ];

        let out =
            super::build_anthropic_request(&messages, &[], "claude-test", 64, false, None, None);
        let built = out["messages"].as_array().expect("messages array");

        assert_eq!(
            built.len(),
            2,
            "two same-role pairs collapse to two messages"
        );
        assert_eq!(built[0]["role"], "user");
        assert_eq!(built[0]["content"][0]["text"], "u1");
        assert_eq!(built[0]["content"][1]["text"], "u2");
        assert_eq!(built[1]["role"], "assistant");
        assert_eq!(built[1]["content"][0]["text"], "a1");
        assert_eq!(built[1]["content"][1]["text"], "a2");

        // Strict alternation invariant: no two adjacent messages share a role.
        for pair in built.windows(2) {
            assert_ne!(
                pair[0]["role"], pair[1]["role"],
                "adjacent messages must alternate role"
            );
        }
    }

    #[test]
    fn assistant_coalesce_drops_interior_thinking_block() {
        // Two consecutive assistant turns, each carrying reasoning. After
        // coalescing, the merged turn must keep only ONE leading `thinking` block —
        // Anthropic rejects a `thinking` block in any non-first position, so the
        // merged-in turn's thinking is dropped. (#101 invariant: coalescing never
        // produces an illegal block order, even for the assistant role.)
        let messages = vec![
            Message::assistant_with_reasoning("a1", None, Some("reason one".into())),
            Message::assistant_with_reasoning("a2", None, Some("reason two".into())),
        ];

        let out =
            super::build_anthropic_request(&messages, &[], "claude-test", 64, false, None, None);
        let built = out["messages"].as_array().expect("messages array");
        assert_eq!(built.len(), 1, "the two assistant turns coalesced");

        let content = built[0]["content"].as_array().unwrap();
        let thinking_positions: Vec<usize> = content
            .iter()
            .enumerate()
            .filter(|(_, b)| b.get("type").and_then(|t| t.as_str()) == Some("thinking"))
            .map(|(i, _)| i)
            .collect();
        assert_eq!(
            thinking_positions,
            vec![0],
            "exactly one thinking block, in the first position"
        );

        let texts: Vec<&str> = content
            .iter()
            .filter_map(|b| {
                (b.get("type").and_then(|t| t.as_str()) == Some("text"))
                    .then(|| b.get("text").and_then(|t| t.as_str()))
                    .flatten()
            })
            .collect();
        assert_eq!(
            texts,
            vec!["a1", "a2"],
            "both turns' text survives the merge"
        );
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
        // Order is preserved, but consecutive same-role turns coalesce (#101):
        // the two leading user turns merge; the tool_result (a user message) and the
        // trailing user turn merge. Result: user / assistant / user, strictly
        // alternating, with content blocks in original order.
        assert_eq!(built_messages.len(), 3);
        assert_eq!(built_messages[0]["role"], "user");
        assert_eq!(
            built_messages[0]["content"][0]["text"],
            "dynamic context block"
        );
        assert_eq!(built_messages[0]["content"][1]["text"], "conversation turn");
        assert_eq!(built_messages[1]["role"], "assistant");
        assert_eq!(built_messages[1]["content"][0]["text"], "calling tool");
        assert_eq!(built_messages[2]["role"], "user");
        assert_eq!(built_messages[2]["content"][0]["type"], "tool_result");
        assert_eq!(built_messages[2]["content"][0]["tool_use_id"], "call_1");
        assert_eq!(built_messages[2]["content"][1]["text"], "latest user turn");
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
    use crate::types::LLMChunk;

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
    fn input_json_delta_without_prior_tool_start_is_skipped_not_aborted() {
        // #237: an input_json_delta for an index never announced via
        // content_block_start (common with Anthropic→OpenAI aggregators) must be
        // SKIPPED, not turned into a stream error that discards the whole
        // already-streamed assistant turn.
        let mut state = super::AnthropicStreamState::default();
        let data = r#"{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"q\":\"test\"}"}}"#;

        let chunk = super::parse_anthropic_sse_event(&mut state, "content_block_delta", data)
            .expect("benign deviation must not abort the stream");
        assert!(chunk.is_none(), "unannounced input_json_delta is skipped");
    }

    #[test]
    fn malformed_content_block_start_events_are_skipped_not_aborted() {
        // Missing index, missing content_block, and tool_use missing id/name all
        // skip (Ok(None)) rather than erroring out the stream. (#237)
        for data in [
            r#"{"type":"content_block_start","content_block":{"type":"text"}}"#, // no index
            r#"{"type":"content_block_start","index":0}"#,                       // no content_block
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","name":"x"}}"#, // no id
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call_1"}}"#, // no name
        ] {
            let mut state = super::AnthropicStreamState::default();
            let chunk = super::parse_anthropic_sse_event(&mut state, "content_block_start", data)
                .unwrap_or_else(|e| panic!("must not abort on: {data} (got {e:?})"));
            assert!(
                chunk.is_none(),
                "malformed content_block_start skipped: {data}"
            );
        }
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
    fn tool_call_with_invalid_json_arguments_falls_back_to_object() {
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

        // Invalid JSON must fall back to an OBJECT, not a bare string: Anthropic
        // requires `tool_use.input` to be an object and 400s a string input,
        // which would then poison every subsequent request in the session.
        let input = &out["messages"][0]["content"][0]["input"];
        assert!(input.is_object(), "input must be an object, got {input}");
        assert_eq!(input["_raw"], "not valid json");
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
