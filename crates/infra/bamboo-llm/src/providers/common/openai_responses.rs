//! OpenAI Responses API request serialization + streaming parsing helpers.
//!
//! Some upstreams (notably newer "agent"/"codex" style models) only support the
//! OpenAI Responses API instead of Chat Completions. We normalize Responses SSE
//! events into [`LLMChunk`] so the rest of Bamboo can stay provider-agnostic.

use super::tool_schema::sanitize_openai_function_parameters_schema;
use crate::cache::PromptCachePlan;
use crate::provider::{LLMError, ResponsesRequestOptions, Result};
use crate::types::LLMChunk;
use bamboo_domain::MessagePart;
use bamboo_domain::ReasoningEffort;
use bamboo_domain::ToolSchema;
use bamboo_domain::{
    Message, MessagePhase, ProviderFamily, ProviderProtocol, ProviderTranscriptAuthor,
    ProviderTranscriptGroup, ProviderTranscriptItem, ProviderTranscriptOrigin, Role,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

const WIRE_TRACKER_KEY_DOMAIN: &[u8] = b"bamboo/responses-wire-tracker-key/v1\0";
const WIRE_TOP_LEVEL_DOMAIN: &[u8] = b"bamboo/responses-wire-top-level/v1\0";
const WIRE_INPUT_PREFIX_DOMAIN: &[u8] = b"bamboo/responses-wire-input-prefix/v1\0";
const MAX_WIRE_TRACKER_FAMILIES: usize = 1024;

/// Convert internal [`Message`] values to a Responses API `input` array.
///
/// The Responses API uses a heterogeneous input array containing:
/// - `{"type": "message", "role": "...", "content": "..."}` for regular messages
/// - `{"type": "function_call", "call_id": "...", "name": "...", "arguments": "..."}` for tool invocations
/// - `{"type": "function_call_output", "call_id": "...", "output": "..."}` for tool results
///
/// This function properly serializes the full tool-call chain so the model
/// maintains structured context across rounds, instead of degrading tool
/// interactions into plain-text user messages.
///
/// For assistant messages that contain `tool_calls`, we:
/// 1. Emit a `message` item for any text content the assistant produced.
/// 2. Emit a `function_call` item for each tool call.
///
/// For tool-result messages (`Role::Tool`), we emit a `function_call_output` item.
pub fn messages_to_responses_input_json(messages: &[Message]) -> Vec<Value> {
    messages_to_responses_input_json_with_cache(messages, None).0
}

fn message_supports_explicit_breakpoint(message: &Message) -> bool {
    matches!(message.role, Role::System | Role::User | Role::Tool)
}

fn explicit_breakpoint_message_ids(
    messages: &[Message],
    cache_plan: Option<&PromptCachePlan>,
) -> HashSet<String> {
    let Some(cache_plan) = cache_plan.filter(|plan| plan.is_enabled()) else {
        return HashSet::new();
    };

    let mut selected = HashSet::new();
    for requested_id in &cache_plan.breakpoint_message_ids {
        let Some(requested_index) = messages
            .iter()
            .position(|message| message.id == *requested_id)
        else {
            continue;
        };
        // Responses only permits markers on input_text/input_image/input_file.
        // When a provider-neutral plan ends on an assistant/function item,
        // select the nearest preceding cacheable input block rather than
        // emitting an invalid marker or expanding the prefix past the target.
        if let Some(message) = messages[..=requested_index]
            .iter()
            .rev()
            .find(|message| message_supports_explicit_breakpoint(message))
        {
            selected.insert(message.id.clone());
        }
    }
    selected
}

fn add_explicit_breakpoint(content: &mut Value) -> bool {
    let marker = json!({"mode": "explicit"});
    match content {
        Value::String(text) => {
            let text = std::mem::take(text);
            *content = json!([{
                "type": "input_text",
                "text": text,
                "prompt_cache_breakpoint": marker,
            }]);
            true
        }
        Value::Array(parts) => {
            let Some(part) = parts.iter_mut().rev().find(|part| {
                matches!(
                    part.get("type").and_then(Value::as_str),
                    Some("input_text" | "input_image" | "input_file")
                )
            }) else {
                return false;
            };
            part["prompt_cache_breakpoint"] = marker;
            true
        }
        _ => false,
    }
}

fn count_responses_explicit_breakpoints(value: &Value) -> usize {
    match value {
        Value::Array(values) => values
            .iter()
            .map(count_responses_explicit_breakpoints)
            .sum(),
        Value::Object(object) => {
            let this_block = (matches!(
                object.get("type").and_then(Value::as_str),
                Some("input_text" | "input_image" | "input_file")
            ) && object
                .get("prompt_cache_breakpoint")
                .and_then(|breakpoint| breakpoint.get("mode"))
                .and_then(Value::as_str)
                == Some("explicit")) as usize;
            this_block
                + object
                    .values()
                    .map(count_responses_explicit_breakpoints)
                    .sum::<usize>()
        }
        _ => 0,
    }
}

fn messages_to_responses_input_json_with_cache(
    messages: &[Message],
    cache_plan: Option<&PromptCachePlan>,
) -> (Vec<Value>, usize) {
    messages_to_responses_input_json_with_cache_and_native(messages, cache_plan, &[])
}

fn messages_to_responses_input_json_with_cache_and_native(
    messages: &[Message],
    cache_plan: Option<&PromptCachePlan>,
    native_groups: &[ProviderTranscriptGroup],
) -> (Vec<Value>, usize) {
    // If any message contains image parts, emit a "typed" content array shape so
    // multimodal inputs have a chance to reach upstream Responses implementations.
    let has_images = messages.iter().any(|m| {
        m.content_parts.as_ref().is_some_and(|parts| {
            parts
                .iter()
                .any(|p| matches!(p, MessagePart::ImageUrl { .. }))
        })
    });

    let mut out: Vec<Value> = Vec::with_capacity(messages.len());
    let breakpoint_message_ids = explicit_breakpoint_message_ids(messages, cache_plan);
    let mut rendered_breakpoints = 0usize;

    for m in messages {
        let mut anchored_groups = native_groups
            .iter()
            .filter(|group| group.anchor_message_id() == m.id)
            .collect::<Vec<_>>();
        anchored_groups.sort_by_key(|group| group.sequence());
        if !anchored_groups.is_empty() {
            out.extend(
                anchored_groups
                    .into_iter()
                    .flat_map(|group| group.items())
                    .map(|item| item.payload().clone()),
            );
            continue;
        }
        match m.role {
            Role::Assistant => {
                // Emit assistant text content as a message item (even if empty, for completeness).
                let has_text = !m.content.trim().is_empty();
                if has_text || m.tool_calls.is_none() {
                    let content = build_content_value(m, has_images, None, "output_text");
                    let mut message_item = json!({
                        "type": "message",
                        "role": "assistant",
                        "content": content,
                    });
                    if let Some(phase) = assistant_phase_for_responses_input(m) {
                        message_item["phase"] = json!(phase);
                    }
                    out.push(message_item);
                }

                // Emit each tool call as a structured function_call item.
                if let Some(calls) = m.tool_calls.as_ref() {
                    for c in calls {
                        out.push(json!({
                            "type": "function_call",
                            "call_id": c.id,
                            "name": c.function.name,
                            "arguments": c.function.arguments,
                        }));
                    }
                }
            }

            Role::Tool => {
                // Emit tool result as a structured function_call_output item.
                let call_id = m.tool_call_id.as_deref().unwrap_or("");
                if !call_id.is_empty() {
                    let mut item = json!({
                        "type": "function_call_output",
                        "call_id": call_id,
                        "output": tool_output_value(m),
                    });
                    if breakpoint_message_ids.contains(&m.id)
                        && add_explicit_breakpoint(&mut item["output"])
                    {
                        rendered_breakpoints = rendered_breakpoints.saturating_add(1);
                    }
                    out.push(item);
                } else {
                    // Fallback: no call_id available — degrade to user message with
                    // prefix. Preserve any image parts as a typed content array so
                    // they aren't dropped on this path either. (#237 finding 6)
                    let prefixed = format!("[tool_result]\n{}", m.content);
                    let content = if message_has_images(m) {
                        let mut parts = vec![json!({"type": "input_text", "text": prefixed})];
                        push_input_image_parts(m, &mut parts);
                        json!(parts)
                    } else {
                        json!(prefixed)
                    };
                    let mut item = json!({
                        "type": "message",
                        "role": "user",
                        "content": content,
                    });
                    if breakpoint_message_ids.contains(&m.id)
                        && add_explicit_breakpoint(&mut item["content"])
                    {
                        rendered_breakpoints = rendered_breakpoints.saturating_add(1);
                    }
                    out.push(item);
                }
            }

            Role::System | Role::User => {
                let role = if m.role == Role::System {
                    "system"
                } else {
                    "user"
                };
                let content = build_content_value(m, has_images, None, "input_text");
                let mut item = json!({
                    "type": "message",
                    "role": role,
                    "content": content,
                });
                if breakpoint_message_ids.contains(&m.id)
                    && add_explicit_breakpoint(&mut item["content"])
                {
                    rendered_breakpoints = rendered_breakpoints.saturating_add(1);
                }
                out.push(item);
            }
        }
    }

    (out, rendered_breakpoints)
}

fn assistant_phase_for_responses_input(message: &Message) -> Option<&'static str> {
    if !matches!(message.role, Role::Assistant) {
        return None;
    }

    if let Some(phase) = message.phase.as_ref() {
        return Some(phase.as_str());
    }

    if message
        .tool_calls
        .as_ref()
        .is_some_and(|calls| !calls.is_empty())
    {
        return Some(MessagePhase::Commentary.as_str());
    }

    if !message.content.trim().is_empty() {
        return Some(MessagePhase::FinalAnswer.as_str());
    }

    None
}

/// Whether a message carries any image content part.
fn message_has_images(m: &Message) -> bool {
    m.content_parts.as_ref().is_some_and(|parts| {
        parts
            .iter()
            .any(|p| matches!(p, MessagePart::ImageUrl { .. }))
    })
}

/// Append this message's `input_image` content parts to `parts`.
fn push_input_image_parts(m: &Message, parts: &mut Vec<Value>) {
    if let Some(content_parts) = m.content_parts.as_ref() {
        for part in content_parts {
            if let MessagePart::ImageUrl { image_url } = part {
                parts.push(json!({"type": "input_image", "image_url": image_url.url}));
            }
        }
    }
}

/// Build the `output` value for a `function_call_output` item.
///
/// When the tool result carries image parts (e.g. an MCP screenshot), the
/// Responses API accepts an array of typed content parts (`input_text` /
/// `input_image`) in place of a plain string, so the image reaches the model
/// instead of being silently dropped. Text-only results stay a plain string.
/// (#237 finding 6)
///
/// Note: for tool results the text lives in `m.content` while images live in
/// `content_parts` (see `Message::tool_result_with_images`), so we build the
/// array by hand rather than via `build_content_value` (which would emit only
/// the image parts and drop the text).
fn tool_output_value(m: &Message) -> Value {
    if message_has_images(m) {
        let mut parts = Vec::new();
        if !m.content.trim().is_empty() {
            parts.push(json!({"type": "input_text", "text": m.content}));
        }
        // Any text parts that happen to live in content_parts, then images.
        if let Some(content_parts) = m.content_parts.as_ref() {
            for part in content_parts {
                if let MessagePart::Text { text } = part {
                    parts.push(json!({"type": "input_text", "text": text}));
                }
            }
        }
        push_input_image_parts(m, &mut parts);
        json!(parts)
    } else {
        json!(m.content)
    }
}

/// Build the `content` value for a message item.
///
/// If `has_images` is true, uses typed content array.
/// `text_part_type` should be `input_text` for user/system and `output_text` for assistant.
/// Otherwise, uses a plain string value.
fn build_content_value(
    m: &Message,
    has_images: bool,
    text_override: Option<&str>,
    text_part_type: &str,
) -> Value {
    if has_images {
        let mut parts = Vec::new();
        if let Some(content_parts) = m.content_parts.as_ref() {
            for part in content_parts {
                match part {
                    MessagePart::Text { text } => {
                        parts.push(json!({"type": text_part_type, "text": text}));
                    }
                    MessagePart::ImageUrl { image_url } => {
                        parts.push(json!({"type": "input_image", "image_url": image_url.url}));
                    }
                }
            }
        } else {
            let text = text_override.unwrap_or(&m.content);
            parts.push(json!({"type": text_part_type, "text": text}));
        }
        json!(parts)
    } else {
        let text = text_override.unwrap_or(&m.content);
        json!(text)
    }
}

/// Convert internal tool schemas to a Responses API `tools` array JSON.
pub fn tools_to_responses_json(tools: &[ToolSchema]) -> Vec<Value> {
    // OpenAI Responses API expects tools in a flattened shape:
    // { "type": "function", "name": "...", "description": "...", "parameters": {..} }
    //
    // Our internal ToolSchema matches the Chat Completions shape:
    // { "type": "function", "function": { name, description, parameters } }
    tools
        .iter()
        .map(|t| {
            json!({
                "type": t.schema_type,
                "name": t.function.name,
                "description": t.function.description,
                "parameters": sanitize_openai_function_parameters_schema(&t.function.parameters),
            })
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponsesInputSource {
    Explicit,
    Generic,
}

#[derive(Debug, Clone, Copy)]
pub struct ResponsesInputSelection<'a> {
    pub input_messages: &'a [Message],
    pub source: ResponsesInputSource,
    pub fallback_removed_duplicate_system: bool,
    pub original_len: usize,
    pub effective_len: usize,
}

pub fn select_responses_input_messages<'a>(
    messages: &'a [Message],
    responses_options: Option<&'a ResponsesRequestOptions>,
) -> ResponsesInputSelection<'a> {
    let instructions = responses_options
        .and_then(|opts| opts.instructions.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let (response_input_messages, source) =
        match responses_options.and_then(|opts| opts.input_messages.as_deref()) {
            Some(input_messages) => (input_messages, ResponsesInputSource::Explicit),
            None => (messages, ResponsesInputSource::Generic),
        };

    let mut effective_messages = response_input_messages;
    let mut fallback_removed_duplicate_system = false;
    if let Some(instructions) = instructions {
        if matches!(source, ResponsesInputSource::Generic) {
            if let Some(first) = response_input_messages.first() {
                if matches!(first.role, Role::System) && first.content.trim() == instructions {
                    tracing::info!(
                        input_source = match source {
                            ResponsesInputSource::Explicit => "explicit",
                            ResponsesInputSource::Generic => "generic",
                        },
                        original_len = response_input_messages.len(),
                        "Responses input fallback removed duplicated leading system message matching top-level instructions"
                    );
                    effective_messages = &response_input_messages[1..];
                    fallback_removed_duplicate_system = true;
                }
            }
        }
    }

    ResponsesInputSelection {
        input_messages: effective_messages,
        source,
        fallback_removed_duplicate_system,
        original_len: response_input_messages.len(),
        effective_len: effective_messages.len(),
    }
}

/// Apply the final provider-family boundary before a shared Responses request
/// is lowered. OpenAI and Copilot use the same wire protocol but must never
/// share native loading/cache state.
pub fn retain_provider_transcript_family(
    options: &mut ResponsesRequestOptions,
    family: ProviderFamily,
) {
    options.provider_transcript_groups.retain(|group| {
        group.family() == family && group.protocol() == ProviderProtocol::OpenAiResponsesV1
    });
}

/// Build a standard Responses API streaming request body.
#[allow(clippy::too_many_arguments)]
pub fn build_responses_body(
    model: &str,
    messages: &[Message],
    tools: &[ToolSchema],
    max_output_tokens: Option<u32>,
    reasoning_effort: Option<ReasoningEffort>,
    responses_options: Option<&ResponsesRequestOptions>,
    parallel_tool_calls: Option<bool>,
    cache_plan: Option<&PromptCachePlan>,
) -> Value {
    let instructions = responses_options
        .and_then(|opts| opts.instructions.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let input_selection = select_responses_input_messages(messages, responses_options);
    let effective_messages = input_selection.input_messages;

    let explicit_cache_supported = supports_openai_explicit_prompt_cache(model);
    let caller_cache_input = explicit_cache_supported
        .then(|| {
            responses_options.and_then(|options| options.raw_input_with_cache_breakpoints.as_ref())
        })
        .flatten();
    let (input, rendered_breakpoints, generated_breakpoints) = if let Some(raw_input) =
        caller_cache_input
    {
        (
            raw_input.clone(),
            count_responses_explicit_breakpoints(raw_input),
            false,
        )
    } else {
        let native_groups = responses_options
            .map(|options| options.provider_transcript_groups.as_slice())
            .unwrap_or_default();
        let (input, rendered_breakpoints) = messages_to_responses_input_json_with_cache_and_native(
            effective_messages,
            explicit_cache_supported.then_some(cache_plan).flatten(),
            native_groups,
        );
        (Value::Array(input), rendered_breakpoints, true)
    };
    let mut body = json!({
        "model": model,
        "input": input,
        "stream": true,
    });

    if let Some(instructions) = instructions {
        body["instructions"] = json!(instructions);
    }

    if let Some(previous_response_id) = responses_options
        .and_then(|opts| opts.previous_response_id.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        body["previous_response_id"] = json!(previous_response_id);
    }

    if !tools.is_empty() {
        body["tools"] = json!(tools_to_responses_json(tools));
        // Best-effort default; upstreams may ignore/override.
        body["tool_choice"] = json!("auto");
    }

    if let Some(parallel_tool_calls) = parallel_tool_calls {
        body["parallel_tool_calls"] = json!(parallel_tool_calls);
    }

    if let Some(max_tokens) = max_output_tokens {
        body["max_output_tokens"] = json!(max_tokens);
    }

    let reasoning_summary = responses_options
        .and_then(|opts| opts.reasoning_summary.as_deref())
        .map(str::trim)
        .filter(|summary| !summary.is_empty());
    if reasoning_effort.is_some() || reasoning_summary.is_some() {
        let mut reasoning = serde_json::Map::new();
        if let Some(effort) = reasoning_effort {
            reasoning.insert("effort".to_string(), json!(effort.to_wire_format(model)));
        }
        if let Some(summary) = reasoning_summary {
            reasoning.insert("summary".to_string(), json!(summary));
        }
        if !reasoning.is_empty() {
            body["reasoning"] = Value::Object(reasoning);
        }
    }

    if let Some(include) = responses_options
        .and_then(|opts| opts.include.as_ref())
        .filter(|values| !values.is_empty())
    {
        body["include"] = json!(include);
    }

    if let Some(truncation) = responses_options
        .and_then(|opts| opts.truncation.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        body["truncation"] = json!(truncation);
    }

    if let Some(text_verbosity) = responses_options
        .and_then(|opts| opts.text_verbosity.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        body["text"] = json!({ "verbosity": text_verbosity });
    }

    let store = responses_options
        .and_then(|opts| opts.store)
        .unwrap_or(false);
    body["store"] = json!(store);

    if explicit_cache_supported {
        if let Some(prompt_cache_key) = responses_options
            .and_then(|opts| opts.prompt_cache_key.as_deref())
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            body["prompt_cache_key"] = json!(prompt_cache_key);
        }

        if let Some(prompt_cache_options) =
            responses_options.and_then(|opts| opts.prompt_cache_options.as_ref())
        {
            if generated_breakpoints && rendered_breakpoints > 0 {
                let mut merged =
                    serde_json::Map::from_iter([("mode".to_string(), json!("explicit"))]);
                if let Some(caller_fields) = prompt_cache_options.as_object() {
                    merged.extend(caller_fields.clone());
                    body["prompt_cache_options"] = Value::Object(merged);
                } else {
                    // Direct internal callers can construct a non-object value;
                    // preserve it so the upstream returns the authoritative
                    // validation error instead of silently changing intent.
                    body["prompt_cache_options"] = prompt_cache_options.clone();
                }
            } else {
                body["prompt_cache_options"] = prompt_cache_options.clone();
            }
        } else if generated_breakpoints && rendered_breakpoints > 0 {
            body["prompt_cache_options"] = json!({"mode": "explicit"});
        }
    }

    body
}

/// Explicit prompt-cache controls were introduced for GPT-5.6 and later GPT
/// model families. Unknown aliases fail closed because older models reject the
/// fields with HTTP 400.
pub fn supports_openai_explicit_prompt_cache(model: &str) -> bool {
    let normalized = model
        .rsplit('/')
        .next()
        .unwrap_or(model)
        .trim()
        .to_ascii_lowercase();
    let Some(version) = normalized.strip_prefix("gpt-") else {
        return false;
    };
    let numeric = version
        .split(|character: char| !(character.is_ascii_digit() || character == '.'))
        .next()
        .unwrap_or("");
    let mut components = numeric.split('.');
    let Some(major) = components
        .next()
        .and_then(|component| component.parse::<u64>().ok())
    else {
        return false;
    };
    let minor = components
        .next()
        .and_then(|component| component.parse::<u64>().ok())
        .unwrap_or(0);
    major > 5 || major == 5 && minor >= 6
}

/// Relationship between two fully-lowered, override-applied, masked Responses
/// request bodies in the same privacy-preserving cache-affinity family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponsesWirePrefixRelation {
    Baseline,
    Identical,
    ExactExtension,
    Rewrite,
    Untracked,
}

impl ResponsesWirePrefixRelation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::Identical => "identical",
            Self::ExactExtension => "exact_extension",
            Self::Rewrite => "rewrite",
            Self::Untracked => "untracked",
        }
    }
}

/// Secret-free observation of the final outbound Responses shape. No prompt
/// text, tool arguments, session id, or raw cache key is retained.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponsesWirePrefixDiagnostic {
    pub relation: ResponsesWirePrefixRelation,
    pub prefix_epoch: Option<u64>,
    pub reset_reason: Option<String>,
    pub cache_key_sha256: Option<String>,
    pub top_level_sha256: String,
    pub cumulative_item_sha256: Vec<String>,
    pub input_item_count: usize,
    pub previous_input_item_count: Option<usize>,
    pub first_divergent_item: Option<usize>,
    pub first_divergent_type: Option<String>,
}

#[derive(Debug, Clone)]
struct StoredWirePrefix {
    prefix_epoch: Option<u64>,
    top_level_sha256: String,
    cumulative_item_sha256: Vec<String>,
    item_types: Vec<String>,
}

/// Provider-instance tracker keyed by a second one-way hash of the already
/// privacy-preserving cache key. The bounded map stores hashes and structural
/// labels only.
#[derive(Clone, Default)]
pub struct ResponsesWirePrefixTracker {
    families: Arc<Mutex<HashMap<String, StoredWirePrefix>>>,
}

impl ResponsesWirePrefixTracker {
    pub fn observe_final_body(
        &self,
        body: &Value,
        prefix_epoch: Option<u64>,
        reset_reason: Option<&str>,
    ) -> ResponsesWirePrefixDiagnostic {
        let input = body
            .get("input")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let cumulative_item_sha256 = cumulative_input_hashes(input);
        let item_types = input.iter().map(responses_item_type).collect::<Vec<_>>();
        let top_level_sha256 = cache_affecting_top_level_sha256(body);
        let cache_key_sha256 = body
            .get("prompt_cache_key")
            .and_then(Value::as_str)
            .map(|key| domain_sha256(WIRE_TRACKER_KEY_DOMAIN, key.as_bytes()));

        let Some(family) = cache_key_sha256.clone() else {
            return ResponsesWirePrefixDiagnostic {
                relation: ResponsesWirePrefixRelation::Untracked,
                prefix_epoch,
                reset_reason: reset_reason.map(str::to_string),
                cache_key_sha256: None,
                top_level_sha256,
                cumulative_item_sha256,
                input_item_count: input.len(),
                previous_input_item_count: None,
                first_divergent_item: None,
                first_divergent_type: None,
            };
        };

        let mut families = self
            .families
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous = families.get(&family).cloned();
        let (relation, first_divergent_item, first_divergent_type) = previous
            .as_ref()
            .map(|prior| {
                classify_wire_prefix(
                    prior,
                    prefix_epoch,
                    &top_level_sha256,
                    &cumulative_item_sha256,
                    &item_types,
                )
            })
            .unwrap_or((ResponsesWirePrefixRelation::Baseline, None, None));
        let previous_input_item_count = previous
            .as_ref()
            .map(|prior| prior.cumulative_item_sha256.len());

        if families.len() >= MAX_WIRE_TRACKER_FAMILIES && !families.contains_key(&family) {
            if let Some(evict) = families.keys().next().cloned() {
                families.remove(&evict);
            }
        }
        families.insert(
            family,
            StoredWirePrefix {
                prefix_epoch,
                top_level_sha256: top_level_sha256.clone(),
                cumulative_item_sha256: cumulative_item_sha256.clone(),
                item_types,
            },
        );

        ResponsesWirePrefixDiagnostic {
            relation,
            prefix_epoch,
            reset_reason: reset_reason.map(str::to_string),
            cache_key_sha256,
            top_level_sha256,
            cumulative_item_sha256,
            input_item_count: input.len(),
            previous_input_item_count,
            first_divergent_item,
            first_divergent_type,
        }
    }
}

fn classify_wire_prefix(
    previous: &StoredWirePrefix,
    current_epoch: Option<u64>,
    current_top_level_sha256: &str,
    current_cumulative: &[String],
    current_types: &[String],
) -> (ResponsesWirePrefixRelation, Option<usize>, Option<String>) {
    if previous.prefix_epoch != current_epoch
        || previous.top_level_sha256 != current_top_level_sha256
    {
        return (
            ResponsesWirePrefixRelation::Rewrite,
            None,
            Some("top_level_or_epoch".to_string()),
        );
    }
    let common = previous
        .cumulative_item_sha256
        .len()
        .min(current_cumulative.len());
    if let Some(index) = (0..common)
        .find(|index| previous.cumulative_item_sha256[*index] != current_cumulative[*index])
    {
        return (
            ResponsesWirePrefixRelation::Rewrite,
            Some(index),
            current_types
                .get(index)
                .cloned()
                .or_else(|| previous.item_types.get(index).cloned()),
        );
    }
    if current_cumulative.len() < previous.cumulative_item_sha256.len() {
        let index = current_cumulative.len();
        return (
            ResponsesWirePrefixRelation::Rewrite,
            Some(index),
            previous.item_types.get(index).cloned(),
        );
    }
    if current_cumulative.len() == previous.cumulative_item_sha256.len() {
        (ResponsesWirePrefixRelation::Identical, None, None)
    } else {
        (ResponsesWirePrefixRelation::ExactExtension, None, None)
    }
}

fn cumulative_input_hashes(input: &[Value]) -> Vec<String> {
    let mut hasher = Sha256::new();
    hasher.update(WIRE_INPUT_PREFIX_DOMAIN);
    input
        .iter()
        .map(|item| {
            let bytes = serde_json::to_vec(item).expect("Responses input item is serializable");
            hasher.update((bytes.len() as u64).to_be_bytes());
            hasher.update(bytes);
            hex::encode(hasher.clone().finalize())
        })
        .collect()
}

fn cache_affecting_top_level_sha256(body: &Value) -> String {
    let top_level = body.as_object().map_or_else(
        || body.clone(),
        |object| {
            Value::Object(
                object
                    .iter()
                    .filter(|(key, _)| key.as_str() != "input")
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
            )
        },
    );
    let bytes = serde_json::to_vec(&top_level).expect("Responses body is serializable");
    domain_sha256(WIRE_TOP_LEVEL_DOMAIN, &bytes)
}

fn responses_item_type(item: &Value) -> String {
    let item_type = item
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    if item_type == "message" {
        let role = item
            .get("role")
            .and_then(Value::as_str)
            .filter(|role| {
                matches!(
                    *role,
                    "user" | "assistant" | "system" | "developer" | "tool"
                )
            })
            .unwrap_or("unknown");
        return format!("message:{role}");
    }

    // Raw-input compatibility callers can author arbitrary JSON. Log only a
    // closed set of protocol structural labels so an attacker cannot smuggle
    // prompt text or credentials into `first_divergent_type`.
    match item_type {
        "function_call"
        | "function_call_output"
        | "computer_call"
        | "computer_call_output"
        | "reasoning"
        | "item_reference"
        | "custom_tool_call"
        | "custom_tool_call_output"
        | "web_search_call"
        | "file_search_call"
        | "code_interpreter_call"
        | "local_shell_call"
        | "local_shell_call_output"
        | "apply_patch_call"
        | "apply_patch_call_output"
        | "mcp_call"
        | "mcp_approval_request"
        | "mcp_approval_response" => item_type.to_string(),
        _ => "unknown".to_string(),
    }
}

fn domain_sha256(domain: &[u8], bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

#[derive(Debug, Default)]
struct AccFnCall {
    call_id: Option<String>,
    name: Option<String>,
    arguments: String,
    /// True when `arguments` was seeded from an `output_item.added` snapshot
    /// rather than built from `function_call_arguments.delta` events. Checked
    /// when the first delta arrives: if the seed is a complete JSON object the
    /// delta stream restates it from scratch, so the seed is dropped to avoid
    /// `snapshot + deltas` duplication; if it's a partial prefix the deltas
    /// continue it and the seed is kept. (#237 finding 4)
    seeded_from_snapshot: bool,
}

/// Parser that converts Responses SSE events into [`LLMChunk`]s.
///
/// We aim for "works with common upstreams" rather than strict spec compliance.
/// Event shape varies; we primarily look at:
/// - SSE `event:` name (e.g. "response.output_text.delta")
/// - JSON `type` field (same as SSE event name in OpenAI)
///
/// Supported:
/// - `response.output_text.delta` -> `LLMChunk::Token(delta)`
/// - `response.content_part.added/done` (output_text parts) -> `LLMChunk::Token(...)`
/// - `response.output_item.added/done` message output_text -> `LLMChunk::Token(...)`
/// - `response.output_item.*` + `response.function_call_arguments.delta` -> `LLMChunk::ToolCalls`
/// - `response.completed` -> terminal output fallbacks, provider usage, then `LLMChunk::Done`
pub struct ResponsesSseParser {
    // item_id -> accumulated function call
    fn_calls: HashMap<String, AccFnCall>,
    // Text item IDs that already produced user-visible answer tokens.
    // Used to avoid duplicating final `*.done` payloads after streaming deltas.
    streamed_text_item_ids: HashSet<String>,
    // Some upstreams omit item_id on text deltas; keep a coarse flag so
    // `*.done` fallbacks can avoid obvious duplicate full-text emissions.
    saw_unkeyed_text_delta: bool,
    // Keyed text streams that have emitted deltas (or delta-like fragments).
    // Key format is derived from output/content indices when available.
    text_delta_stream_keys: HashSet<String>,
    // Keyed text streams that have already emitted terminal done snapshots.
    text_done_stream_keys: HashSet<String>,
    // Output indexes that already surfaced any user-visible text stream.
    // Used to suppress redundant message snapshots in `response.output_item.done`.
    streamed_text_output_indexes: HashSet<i64>,
    // Function-call item IDs, provider call IDs, and output indexes that already
    // produced a downstream ToolCalls chunk. A completed response can repeat the
    // authoritative output_item.done snapshot, so all three identities are
    // retained to suppress the fallback without relying on any one optional key.
    emitted_tool_item_ids: HashSet<String>,
    emitted_tool_call_ids: HashSet<String>,
    emitted_tool_output_indexes: HashSet<i64>,
    // Reasoning output item IDs that have already emitted summary text.
    streamed_reasoning_item_ids: HashSet<String>,
    // Some upstreams omit item_id on reasoning deltas; use the same done-fallback guard
    // strategy as text tokens.
    saw_unkeyed_reasoning_delta: bool,
    // Per-stream reasoning text accumulation used to normalize providers that emit
    // cumulative snapshots on `*.delta` instead of strict token deltas.
    reasoning_item_content: HashMap<String, String>,
    // Keys (summary index stream or item id stream) that already emitted reasoning deltas.
    // Used to suppress matching `*.done` snapshots.
    streamed_reasoning_stream_keys: HashSet<String>,
    // Whether this response already surfaced reasoning text via dedicated reasoning events.
    // `response.output_item.done` reasoning payloads are treated as fallback only.
    saw_reasoning_text_stream: bool,
    // Aggregate of reasoning text actually emitted to downstream consumers.
    // Used to restore paragraph boundaries when a new reasoning summary stream starts.
    emitted_reasoning_text: String,
    provider_label: String,
    model: String,
    requested_reasoning_effort: Option<ReasoningEffort>,
    request_reasoning_enabled: bool,
    observed_reasoning_signal: bool,
    reasoning_event_count: usize,
    reasoning_text_chars: usize,
    logged_summary: bool,
    emitted_response_id: Option<String>,
    retain_protocol_events: bool,
    /// Once a protocol terminal is observed, every later frame is ignored.
    ///
    /// Responses usage and `Done` are cumulative terminal data, so accepting a
    /// duplicated `response.completed` frame would double-publish both.
    terminal_seen: bool,
}

impl Default for ResponsesSseParser {
    fn default() -> Self {
        Self::new()
    }
}

impl ResponsesSseParser {
    #[allow(dead_code)] // Used in tests and retained for backward compatibility.
    pub fn new() -> Self {
        Self::new_with_context("Responses", "", None)
    }

    pub fn new_with_context(
        provider_label: &str,
        model: &str,
        requested_reasoning_effort: Option<ReasoningEffort>,
    ) -> Self {
        Self {
            fn_calls: HashMap::new(),
            streamed_text_item_ids: HashSet::new(),
            saw_unkeyed_text_delta: false,
            text_delta_stream_keys: HashSet::new(),
            text_done_stream_keys: HashSet::new(),
            streamed_text_output_indexes: HashSet::new(),
            emitted_tool_item_ids: HashSet::new(),
            emitted_tool_call_ids: HashSet::new(),
            emitted_tool_output_indexes: HashSet::new(),
            streamed_reasoning_item_ids: HashSet::new(),
            saw_unkeyed_reasoning_delta: false,
            reasoning_item_content: HashMap::new(),
            streamed_reasoning_stream_keys: HashSet::new(),
            saw_reasoning_text_stream: false,
            emitted_reasoning_text: String::new(),
            provider_label: provider_label.to_string(),
            model: model.to_string(),
            requested_reasoning_effort,
            request_reasoning_enabled: requested_reasoning_effort.is_some(),
            observed_reasoning_signal: false,
            reasoning_event_count: 0,
            reasoning_text_chars: 0,
            logged_summary: false,
            emitted_response_id: None,
            retain_protocol_events: false,
            terminal_seen: false,
        }
    }

    pub fn with_protocol_events(mut self, retain_protocol_events: bool) -> Self {
        self.retain_protocol_events = retain_protocol_events;
        self
    }

    fn event_type<'a>(&self, event: &'a str, v: &'a Value) -> &'a str {
        v.get("type").and_then(|t| t.as_str()).unwrap_or(event)
    }

    fn ensure_fn_call(&mut self, item_id: &str) -> &mut AccFnCall {
        self.fn_calls.entry(item_id.to_string()).or_default()
    }

    fn parse_fn_call_item(&mut self, item_id: &str, item: &Value) {
        let entry = self.ensure_fn_call(item_id);

        // Different upstreams use either `call_id` or overload `id`.
        if entry.call_id.is_none() {
            entry.call_id = item
                .get("call_id")
                .or_else(|| item.get("id"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }

        if entry.name.is_none() {
            entry.name = item
                .get("name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }

        // Treat `output_item.added` arguments as a snapshot/seed (not a delta).
        // Some upstreams also send `function_call_arguments.delta` and a full
        // arguments snapshot at `output_item.done`; blindly appending here can
        // duplicate JSON and break downstream tool arg parsing.
        if let Some(args) = item.get("arguments").and_then(|v| v.as_str()) {
            if !args.is_empty() && entry.arguments.is_empty() {
                entry.arguments = args.to_string();
                entry.seeded_from_snapshot = true;
            }
        }
    }

    fn apply_done_fn_call_item(&mut self, item_id: &str, item: &Value) {
        let entry = self.ensure_fn_call(item_id);

        if entry.call_id.is_none() {
            entry.call_id = item
                .get("call_id")
                .or_else(|| item.get("id"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }

        if entry.name.is_none() {
            entry.name = item
                .get("name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }

        // `output_item.done` is authoritative when it includes full arguments.
        if let Some(args) = item.get("arguments").and_then(|v| v.as_str()) {
            if !args.is_empty() {
                entry.arguments = args.to_string();
            }
        }
    }

    fn finalize_tool_call(&mut self, item_id: &str) -> Option<bamboo_domain::ToolCall> {
        let acc = self.fn_calls.remove(item_id)?;
        Some(bamboo_domain::ToolCall {
            id: acc.call_id?,
            tool_type: "function".to_string(),
            function: bamboo_domain::FunctionCall {
                name: acc.name?,
                arguments: acc.arguments,
            },
        })
    }

    fn function_call_item_key(
        &self,
        item: &Value,
        item_key_hint: Option<&str>,
        output_index: Option<i64>,
    ) -> Option<String> {
        let inner_id = item
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty());
        let item_key_hint = item_key_hint.filter(|id| !id.is_empty());
        let call_id = item
            .get("call_id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty());

        // Incremental output_item.done snapshots can be sparse. Prefer whichever
        // identity already owns the added/delta accumulator before choosing a new
        // key; otherwise a nested call_id can steal precedence from the outer
        // item_id that holds the accumulated name and arguments.
        for candidate in [inner_id, item_key_hint, call_id].into_iter().flatten() {
            if self.fn_calls.contains_key(candidate) {
                return Some(candidate.to_string());
            }
        }

        inner_id
            .or(item_key_hint)
            .or(call_id)
            .map(str::to_string)
            .or_else(|| output_index.map(|index| format!("output:{index}")))
    }

    fn tool_call_was_emitted(
        &self,
        item_key: &str,
        call_id: Option<&str>,
        output_index: Option<i64>,
    ) -> bool {
        self.emitted_tool_item_ids.contains(item_key)
            || call_id.is_some_and(|id| self.emitted_tool_call_ids.contains(id))
            || output_index.is_some_and(|index| self.emitted_tool_output_indexes.contains(&index))
    }

    fn mark_tool_call_emitted(&mut self, item_key: &str, call_id: &str, output_index: Option<i64>) {
        self.emitted_tool_item_ids.insert(item_key.to_string());
        if !call_id.is_empty() {
            self.emitted_tool_call_ids.insert(call_id.to_string());
        }
        if let Some(output_index) = output_index {
            self.emitted_tool_output_indexes.insert(output_index);
        }
    }

    fn emit_function_call_item(
        &mut self,
        item: &Value,
        item_key_hint: Option<&str>,
        output_index: Option<i64>,
    ) -> Option<LLMChunk> {
        let item_key = self.function_call_item_key(item, item_key_hint, output_index)?;
        let call_id = item
            .get("call_id")
            .or_else(|| item.get("id"))
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty());
        if self.tool_call_was_emitted(item_key.as_str(), call_id, output_index) {
            return None;
        }

        self.apply_done_fn_call_item(item_key.as_str(), item);
        let call = self.finalize_tool_call(item_key.as_str())?;
        self.mark_tool_call_emitted(item_key.as_str(), call.id.as_str(), output_index);
        Some(LLMChunk::ToolCalls(vec![call]))
    }

    fn emit_completed_message_item(&mut self, item: &Value, output_index: i64) -> Option<LLMChunk> {
        if self.streamed_text_output_indexes.contains(&output_index) {
            return None;
        }

        let item_id = item
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty());
        let text = Self::message_item_output_text(item);
        let out = self.emit_done_text(item_id, text.as_str());
        if out.is_some() {
            self.streamed_text_output_indexes.insert(output_index);
        }
        out
    }

    fn completed_output_chunks(&mut self, value: &Value) -> Vec<LLMChunk> {
        let Some(output) = value
            .get("response")
            .and_then(|response| response.get("output"))
            .or_else(|| value.get("output"))
            .and_then(Value::as_array)
        else {
            return Vec::new();
        };

        let mut chunks = Vec::new();
        for (output_index, item) in output.iter().enumerate() {
            let Ok(output_index) = i64::try_from(output_index) else {
                continue;
            };
            match item.get("type").and_then(Value::as_str).unwrap_or("") {
                "message" => {
                    if let Some(chunk) = self.emit_completed_message_item(item, output_index) {
                        chunks.push(chunk);
                    }
                }
                "function_call" => {
                    if let Some(chunk) =
                        self.emit_function_call_item(item, None, Some(output_index))
                    {
                        chunks.push(chunk);
                    }
                }
                // Unknown and malformed terminal items are intentionally ignored:
                // gateways vary, and one unsupported output must not discard valid
                // siblings from the same completed response.
                _ => {}
            }
        }
        chunks
    }

    fn completed_provider_transcript_items(&self, value: &Value) -> Vec<LLMChunk> {
        let Some(output) = value
            .get("response")
            .and_then(|response| response.get("output"))
            .or_else(|| value.get("output"))
            .and_then(Value::as_array)
        else {
            return Vec::new();
        };
        if !output.iter().any(|item| {
            matches!(
                item.get("type").and_then(Value::as_str),
                Some("tool_search_call" | "tool_search_output")
            )
        }) {
            return Vec::new();
        }

        let family = if self.provider_label.eq_ignore_ascii_case("copilot") {
            ProviderFamily::Copilot
        } else {
            ProviderFamily::OpenAi
        };
        let mut items = Vec::with_capacity(output.len());
        for payload in output {
            let author = match payload.get("type").and_then(Value::as_str) {
                Some("tool_search_output") => ProviderTranscriptAuthor::ToolResult,
                Some("message" | "reasoning" | "function_call" | "tool_search_call") => {
                    ProviderTranscriptAuthor::Model
                }
                _ => return Vec::new(),
            };
            let Ok(item) = ProviderTranscriptItem::try_from_payload(
                family,
                ProviderProtocol::OpenAiResponsesV1,
                ProviderTranscriptOrigin::Provider,
                author,
                payload.clone(),
            ) else {
                tracing::warn!(
                    "{} Responses discovery transcript failed closed during validation",
                    self.provider_label
                );
                return Vec::new();
            };
            items.push(LLMChunk::ProviderTranscriptItem(item));
        }
        items
    }

    fn text_item_id(v: &Value) -> Option<String> {
        v.get("item_id")
            .and_then(|id| id.as_str())
            .or_else(|| {
                v.get("item")
                    .and_then(|item| item.get("id"))
                    .and_then(|id| id.as_str())
            })
            .map(|id| id.to_string())
            .filter(|id| !id.is_empty())
    }

    fn message_item_output_text(item: &Value) -> String {
        let Some(content) = item.get("content").and_then(|value| value.as_array()) else {
            return String::new();
        };

        let mut out = String::new();
        for part in content {
            let part_type = part
                .get("type")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            if part_type != "output_text" {
                continue;
            }

            if let Some(text) = part.get("text").and_then(|value| value.as_str()) {
                out.push_str(text);
            }
        }
        out
    }

    fn content_part_output_text(v: &Value, prefer_delta: bool) -> String {
        let part = v
            .get("part")
            .or_else(|| v.get("content_part"))
            .unwrap_or(&Value::Null);
        let part_type = part
            .get("type")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        if part_type != "output_text" {
            return String::new();
        }

        if prefer_delta {
            return part
                .get("delta")
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .to_string();
        }

        part.get("text")
            .and_then(|value| value.as_str())
            .or_else(|| part.get("delta").and_then(|value| value.as_str()))
            .unwrap_or("")
            .to_string()
    }

    fn text_output_index(v: &Value) -> Option<i64> {
        v.get("output_index").and_then(|value| value.as_i64())
    }

    fn text_stream_key(v: &Value) -> Option<String> {
        let output_index = Self::text_output_index(v)?;
        let content_index = v
            .get("content_index")
            .and_then(|value| value.as_i64())
            .or_else(|| {
                v.get("part")
                    .and_then(|part| part.get("index"))
                    .and_then(|value| value.as_i64())
            });
        match content_index {
            Some(content_index) => Some(format!("text:{output_index}:{content_index}")),
            None => Some(format!("text:{output_index}:_")),
        }
    }

    fn emit_text_delta_with_key(&mut self, stream_key: &str, text: &str) -> Option<LLMChunk> {
        if text.is_empty() {
            return None;
        }
        self.text_delta_stream_keys.insert(stream_key.to_string());
        Some(LLMChunk::Token(text.to_string()))
    }

    fn emit_text_done_with_key(&mut self, stream_key: &str, text: &str) -> Option<LLMChunk> {
        if text.is_empty() {
            return None;
        }
        if self.text_delta_stream_keys.contains(stream_key)
            || self.text_done_stream_keys.contains(stream_key)
        {
            return None;
        }

        // Cross-channel dedupe by output_index when one channel omits content_index
        // (for example, `content_part.done` with key `text:<output>:_`).
        if let Some((prefix, _)) = stream_key.rsplit_once(':') {
            let wildcard_key = format!("{prefix}:_");
            if self.text_delta_stream_keys.contains(wildcard_key.as_str())
                || self.text_done_stream_keys.contains(wildcard_key.as_str())
            {
                return None;
            }
            if stream_key.ends_with(":_") {
                let output_prefix = format!("{prefix}:");
                if self
                    .text_delta_stream_keys
                    .iter()
                    .any(|key| key.starts_with(output_prefix.as_str()))
                    || self
                        .text_done_stream_keys
                        .iter()
                        .any(|key| key.starts_with(output_prefix.as_str()))
                {
                    return None;
                }
            }
        }

        self.text_done_stream_keys.insert(stream_key.to_string());
        Some(LLMChunk::Token(text.to_string()))
    }

    fn reasoning_item_summary_text(item: &Value) -> String {
        let mut out = String::new();

        if let Some(summary) = item.get("summary") {
            if let Some(text) = summary.as_str() {
                out.push_str(text);
            } else if let Some(parts) = summary.as_array() {
                for part in parts {
                    let maybe_text = part
                        .get("text")
                        .and_then(|value| value.as_str())
                        .or_else(|| part.as_str());
                    let Some(text) = maybe_text else {
                        continue;
                    };
                    if text.is_empty() {
                        continue;
                    }
                    if !out.is_empty() {
                        out.push_str("\n\n");
                    }
                    out.push_str(text);
                }
            }
        }

        if out.is_empty() {
            if let Some(text) = item.get("text").and_then(|value| value.as_str()) {
                out.push_str(text);
            }
        }

        out
    }

    fn reasoning_summary_part_text(v: &Value) -> String {
        let part = v
            .get("part")
            .or_else(|| v.get("summary_part"))
            .unwrap_or(&Value::Null);
        let part_type = part
            .get("type")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        if part_type != "summary_text" {
            return String::new();
        }
        part.get("text")
            .and_then(|value| value.as_str())
            .or_else(|| part.get("delta").and_then(|value| value.as_str()))
            .unwrap_or("")
            .to_string()
    }

    fn reasoning_summary_stream_key(v: &Value) -> Option<String> {
        let output_index = v.get("output_index").and_then(|value| value.as_i64());
        let summary_index = v.get("summary_index").and_then(|value| value.as_i64());
        match (output_index, summary_index) {
            (Some(output_index), Some(summary_index)) => {
                Some(format!("summary:{output_index}:{summary_index}"))
            }
            (Some(output_index), None) => Some(format!("summary:{output_index}:_")),
            (None, Some(summary_index)) => Some(format!("summary:_:{}", summary_index)),
            (None, None) => None,
        }
    }

    fn reasoning_event_stream_key(event_type: &str, v: &Value) -> Option<String> {
        if event_type.starts_with("response.reasoning_summary_") {
            return Self::reasoning_summary_stream_key(v)
                .or_else(|| Self::text_item_id(v).map(|item_id| format!("item:{item_id}")));
        }
        Self::text_item_id(v).map(|item_id| format!("item:{item_id}"))
    }

    fn reasoning_summary_stream_starts_new_block(stream_key: &str) -> bool {
        stream_key.starts_with("summary:") || stream_key.starts_with("item:")
    }

    fn with_reasoning_block_separator_if_needed(&self, stream_key: &str, chunk: &str) -> String {
        if chunk.is_empty()
            || self.emitted_reasoning_text.is_empty()
            || !Self::reasoning_summary_stream_starts_new_block(stream_key)
        {
            return chunk.to_string();
        }

        let trimmed_output = self
            .emitted_reasoning_text
            .trim_end_matches([' ', '\t', '\r']);
        let trailing_newlines = trimmed_output
            .chars()
            .rev()
            .take_while(|&c| c == '\n')
            .count();
        let leading_newlines = chunk
            .chars()
            .take_while(|&c| c == '\n' || c == '\r')
            .filter(|&c| c == '\n')
            .count();
        let missing_newlines = 2usize.saturating_sub(trailing_newlines + leading_newlines);

        if missing_newlines == 0 {
            return chunk.to_string();
        }

        format!("{}{}", "\n".repeat(missing_newlines), chunk)
    }

    fn track_emitted_reasoning_text(&mut self, chunk: &str) {
        if !chunk.is_empty() {
            self.emitted_reasoning_text.push_str(chunk);
        }
    }

    fn emit_reasoning_delta_with_key(&mut self, stream_key: &str, text: &str) -> Option<LLMChunk> {
        if text.is_empty() {
            return None;
        }
        self.saw_reasoning_text_stream = true;
        let is_new_stream = !self.streamed_reasoning_stream_keys.contains(stream_key);
        self.streamed_reasoning_stream_keys
            .insert(stream_key.to_string());
        let emitted = self.normalize_reasoning_item_chunk(stream_key, text)?;
        let emitted = if is_new_stream {
            self.with_reasoning_block_separator_if_needed(stream_key, emitted.as_str())
        } else {
            emitted
        };
        self.track_emitted_reasoning_text(emitted.as_str());
        Some(LLMChunk::ReasoningToken(emitted))
    }

    fn emit_reasoning_done_with_key(&mut self, stream_key: &str, text: &str) -> Option<LLMChunk> {
        if text.is_empty() {
            return None;
        }
        self.saw_reasoning_text_stream = true;
        let is_new_stream = !self.streamed_reasoning_stream_keys.contains(stream_key);
        if !is_new_stream {
            return None;
        }
        self.streamed_reasoning_stream_keys
            .insert(stream_key.to_string());
        let emitted = self.normalize_reasoning_item_chunk(stream_key, text)?;
        let emitted = self.with_reasoning_block_separator_if_needed(stream_key, emitted.as_str());
        self.track_emitted_reasoning_text(emitted.as_str());
        Some(LLMChunk::ReasoningToken(emitted))
    }

    fn emit_reasoning_item_text(&mut self, item_id: Option<&str>, text: &str) -> Option<LLMChunk> {
        self.emit_reasoning_delta_text(item_id, text)
    }

    fn emit_reasoning_delta_text(&mut self, item_id: Option<&str>, text: &str) -> Option<LLMChunk> {
        if text.is_empty() {
            return None;
        }
        self.saw_reasoning_text_stream = true;

        if let Some(item_id) = item_id {
            self.streamed_reasoning_item_ids.insert(item_id.to_string());
            let emitted = self.normalize_reasoning_item_chunk(item_id, text)?;
            self.track_emitted_reasoning_text(emitted.as_str());
            return Some(LLMChunk::ReasoningToken(emitted));
        }

        self.saw_unkeyed_reasoning_delta = true;
        self.track_emitted_reasoning_text(text);
        Some(LLMChunk::ReasoningToken(text.to_string()))
    }

    fn emit_reasoning_done_text(&mut self, item_id: Option<&str>, text: &str) -> Option<LLMChunk> {
        if text.is_empty() {
            return None;
        }
        self.saw_reasoning_text_stream = true;

        if let Some(item_id) = item_id {
            self.streamed_reasoning_item_ids.insert(item_id.to_string());
            let emitted = self.normalize_reasoning_item_chunk(item_id, text)?;
            self.track_emitted_reasoning_text(emitted.as_str());
            return Some(LLMChunk::ReasoningToken(emitted));
        }

        if self.saw_unkeyed_reasoning_delta {
            return None;
        }

        self.track_emitted_reasoning_text(text);
        Some(LLMChunk::ReasoningToken(text.to_string()))
    }

    fn emit_done_text(&mut self, item_id: Option<&str>, text: &str) -> Option<LLMChunk> {
        if text.is_empty() {
            return None;
        }

        if let Some(item_id) = item_id {
            if self.streamed_text_item_ids.contains(item_id) {
                return None;
            }
            self.streamed_text_item_ids.insert(item_id.to_string());
            return Some(LLMChunk::Token(text.to_string()));
        }

        if self.saw_unkeyed_text_delta {
            return None;
        }

        Some(LLMChunk::Token(text.to_string()))
    }

    fn normalize_reasoning_item_chunk(&mut self, item_id: &str, chunk: &str) -> Option<String> {
        let entry = self
            .reasoning_item_content
            .entry(item_id.to_string())
            .or_default();

        if entry.is_empty() {
            entry.push_str(chunk);
            return Some(chunk.to_string());
        }

        if chunk == entry.as_str() {
            return None;
        }

        // Snapshot mode: the new payload includes the previous text as prefix.
        if chunk.starts_with(entry.as_str()) {
            let suffix = chunk[entry.len()..].to_string();
            *entry = chunk.to_string();
            if suffix.is_empty() {
                return None;
            }
            return Some(suffix);
        }

        // Duplicate resend of an already emitted tail fragment.
        if entry.ends_with(chunk) {
            return None;
        }

        // True delta mode fallback: append fragment as-is.
        entry.push_str(chunk);
        Some(chunk.to_string())
    }

    fn log_reasoning_summary_if_needed(&mut self, usage: Option<&Value>) {
        if self.logged_summary {
            return;
        }

        if !(self.request_reasoning_enabled || self.observed_reasoning_signal) {
            return;
        }

        let reasoning_tokens = usage
            .and_then(|value| value.get("output_tokens_details"))
            .and_then(|details| details.get("reasoning_tokens"))
            .and_then(|tokens| tokens.as_u64())
            .or_else(|| {
                usage
                    .and_then(|value| value.get("reasoning_tokens"))
                    .and_then(|tokens| tokens.as_u64())
            });

        tracing::info!(
            "{} responses reasoning summary: model='{}' requested_effort={} request_reasoning_enabled={} observed_reasoning_signal={} reasoning_event_count={} reasoning_text_chars={} reasoning_tokens={}",
            self.provider_label,
            if self.model.is_empty() { "<unknown>" } else { self.model.as_str() },
            self.requested_reasoning_effort
                .map(ReasoningEffort::as_str)
                .unwrap_or("none"),
            self.request_reasoning_enabled,
            self.observed_reasoning_signal,
            self.reasoning_event_count,
            self.reasoning_text_chars,
            reasoning_tokens
                .map(|tokens| tokens.to_string())
                .unwrap_or_else(|| "none".to_string())
        );
        self.logged_summary = true;
    }

    fn response_id_from_value<'a>(&self, value: &'a Value) -> Option<&'a str> {
        value
            .get("response")
            .and_then(|response| response.get("id"))
            .and_then(|id| id.as_str())
            .or_else(|| value.get("response_id").and_then(|id| id.as_str()))
    }

    fn maybe_emit_response_id(&mut self, event_type: &str, value: &Value) -> Option<LLMChunk> {
        if !matches!(
            event_type,
            "response.created" | "response.in_progress" | "response.completed"
        ) {
            return None;
        }
        let response_id = self.response_id_from_value(value)?;
        if self.emitted_response_id.as_deref() == Some(response_id) {
            return None;
        }
        self.emitted_response_id = Some(response_id.to_string());
        Some(LLMChunk::ResponseId(response_id.to_string()))
    }

    fn terminal_error_message(event_type: &str, value: &Value) -> String {
        let response = value.get("response").unwrap_or(value);
        let error = response
            .get("error")
            .or_else(|| value.get("error"))
            .unwrap_or(response);
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .or_else(|| error.as_str())
            .or_else(|| value.get("message").and_then(Value::as_str));
        let code = error
            .get("code")
            .and_then(Value::as_str)
            .or_else(|| response.get("status").and_then(Value::as_str));
        let incomplete_reason = response
            .get("incomplete_details")
            .or_else(|| value.get("incomplete_details"))
            .and_then(|details| details.get("reason"))
            .and_then(Value::as_str);

        let detail = message
            .or(incomplete_reason)
            .or(code)
            .unwrap_or("upstream returned no error details");
        format!("OpenAI Responses {event_type}: {detail}")
    }

    fn handle_event_value(&mut self, event_type: &str, v: Value) -> Result<Option<LLMChunk>> {
        match event_type {
            // `summary_part.added` is typically a shape/placeholder signal; text is usually empty.
            "response.reasoning_summary_part.added" => {
                self.observed_reasoning_signal = true;
                self.reasoning_event_count = self.reasoning_event_count.saturating_add(1);
                return Ok(None);
            }

            "response.reasoning_summary_text.delta" => {
                self.observed_reasoning_signal = true;
                self.reasoning_event_count = self.reasoning_event_count.saturating_add(1);
                let reasoning_chunk = v
                    .get("delta")
                    .and_then(|value| value.as_str())
                    .or_else(|| v.get("text").and_then(|value| value.as_str()))
                    .or_else(|| v.get("summary").and_then(|value| value.as_str()))
                    .unwrap_or("");
                self.reasoning_text_chars = self
                    .reasoning_text_chars
                    .saturating_add(reasoning_chunk.len());
                if reasoning_chunk.is_empty() {
                    return Ok(None);
                }
                if let Some(stream_key) = Self::reasoning_event_stream_key(event_type, &v) {
                    return Ok(
                        self.emit_reasoning_delta_with_key(stream_key.as_str(), reasoning_chunk)
                    );
                }
                let item_id = Self::text_item_id(&v);
                return Ok(self.emit_reasoning_delta_text(item_id.as_deref(), reasoning_chunk));
            }

            "response.reasoning_summary_text.done" => {
                self.observed_reasoning_signal = true;
                self.reasoning_event_count = self.reasoning_event_count.saturating_add(1);
                let mut reasoning_chunk = v
                    .get("text")
                    .and_then(|value| value.as_str())
                    .or_else(|| v.get("delta").and_then(|value| value.as_str()))
                    .or_else(|| v.get("summary").and_then(|value| value.as_str()))
                    .unwrap_or("")
                    .to_string();
                if reasoning_chunk.is_empty() {
                    reasoning_chunk = Self::reasoning_item_summary_text(&v);
                }
                self.reasoning_text_chars = self
                    .reasoning_text_chars
                    .saturating_add(reasoning_chunk.len());
                if reasoning_chunk.is_empty() {
                    return Ok(None);
                }
                if let Some(stream_key) = Self::reasoning_event_stream_key(event_type, &v) {
                    return Ok(self.emit_reasoning_done_with_key(
                        stream_key.as_str(),
                        reasoning_chunk.as_str(),
                    ));
                }
                let item_id = Self::text_item_id(&v);
                return Ok(
                    self.emit_reasoning_done_text(item_id.as_deref(), reasoning_chunk.as_str())
                );
            }

            // Some providers emit only part.done with the full summary text; treat it as
            // a fallback channel and suppress when summary_text.* already streamed for this key.
            "response.reasoning_summary_part.done" => {
                self.observed_reasoning_signal = true;
                self.reasoning_event_count = self.reasoning_event_count.saturating_add(1);
                let reasoning_chunk = Self::reasoning_summary_part_text(&v);
                self.reasoning_text_chars = self
                    .reasoning_text_chars
                    .saturating_add(reasoning_chunk.len());
                if reasoning_chunk.is_empty() {
                    return Ok(None);
                }
                if let Some(stream_key) = Self::reasoning_event_stream_key(event_type, &v) {
                    return Ok(self.emit_reasoning_done_with_key(
                        stream_key.as_str(),
                        reasoning_chunk.as_str(),
                    ));
                }
                let item_id = Self::text_item_id(&v);
                return Ok(
                    self.emit_reasoning_done_text(item_id.as_deref(), reasoning_chunk.as_str())
                );
            }

            // Legacy / provider-specific reasoning streams.
            "response.reasoning.delta" | "response.reasoning_text.delta" => {
                self.observed_reasoning_signal = true;
                self.reasoning_event_count = self.reasoning_event_count.saturating_add(1);
                let mut reasoning_chunk = v
                    .get("delta")
                    .and_then(|value| value.as_str())
                    .or_else(|| v.get("text").and_then(|value| value.as_str()))
                    .or_else(|| v.get("summary").and_then(|value| value.as_str()))
                    .unwrap_or("")
                    .to_string();
                if reasoning_chunk.is_empty() {
                    reasoning_chunk = Self::reasoning_item_summary_text(&v);
                }
                self.reasoning_text_chars = self
                    .reasoning_text_chars
                    .saturating_add(reasoning_chunk.len());
                if reasoning_chunk.is_empty() {
                    return Ok(None);
                }
                let item_id = Self::text_item_id(&v);
                return Ok(
                    self.emit_reasoning_delta_text(item_id.as_deref(), reasoning_chunk.as_str())
                );
            }

            "response.reasoning.done" | "response.reasoning_text.done" => {
                self.observed_reasoning_signal = true;
                self.reasoning_event_count = self.reasoning_event_count.saturating_add(1);
                let mut reasoning_chunk = v
                    .get("text")
                    .and_then(|value| value.as_str())
                    .or_else(|| v.get("delta").and_then(|value| value.as_str()))
                    .or_else(|| v.get("summary").and_then(|value| value.as_str()))
                    .unwrap_or("")
                    .to_string();
                if reasoning_chunk.is_empty() {
                    reasoning_chunk = Self::reasoning_item_summary_text(&v);
                }
                self.reasoning_text_chars = self
                    .reasoning_text_chars
                    .saturating_add(reasoning_chunk.len());
                if reasoning_chunk.is_empty() {
                    return Ok(None);
                }
                let item_id = Self::text_item_id(&v);
                return Ok(
                    self.emit_reasoning_done_text(item_id.as_deref(), reasoning_chunk.as_str())
                );
            }

            _ => {}
        }

        match event_type {
            "response.output_text.delta" => {
                let delta = v.get("delta").and_then(|d| d.as_str()).unwrap_or("");
                if delta.is_empty() {
                    return Ok(None);
                }
                if let Some(output_index) = Self::text_output_index(&v) {
                    self.streamed_text_output_indexes.insert(output_index);
                }
                if let Some(stream_key) = Self::text_stream_key(&v) {
                    return Ok(self.emit_text_delta_with_key(stream_key.as_str(), delta));
                }
                if let Some(item_id) = Self::text_item_id(&v) {
                    self.streamed_text_item_ids.insert(item_id);
                } else {
                    self.saw_unkeyed_text_delta = true;
                }
                Ok(Some(LLMChunk::Token(delta.to_string())))
            }

            "response.output_text.done" => {
                let text = v
                    .get("text")
                    .and_then(|value| value.as_str())
                    .or_else(|| v.get("delta").and_then(|value| value.as_str()))
                    .unwrap_or("");
                let output_index = Self::text_output_index(&v);
                let out = if let Some(stream_key) = Self::text_stream_key(&v) {
                    self.emit_text_done_with_key(stream_key.as_str(), text)
                } else {
                    let item_id = Self::text_item_id(&v);
                    self.emit_done_text(item_id.as_deref(), text)
                };
                if out.is_some() {
                    self.streamed_text_output_indexes.extend(output_index);
                }
                Ok(out)
            }

            "response.content_part.added" => {
                // `content_part.added` often contains snapshot text that can overlap with
                // `output_text.delta`; prefer delta-only here to avoid duplicate emissions.
                let text = Self::content_part_output_text(&v, true);
                if text.is_empty() {
                    return Ok(None);
                }
                if let Some(output_index) = Self::text_output_index(&v) {
                    self.streamed_text_output_indexes.insert(output_index);
                }
                if let Some(stream_key) = Self::text_stream_key(&v) {
                    return Ok(self.emit_text_delta_with_key(stream_key.as_str(), text.as_str()));
                }
                if let Some(item_id) = Self::text_item_id(&v) {
                    self.streamed_text_item_ids.insert(item_id);
                } else {
                    self.saw_unkeyed_text_delta = true;
                }
                Ok(Some(LLMChunk::Token(text)))
            }

            "response.content_part.done" => {
                let text = Self::content_part_output_text(&v, false);
                let output_index = Self::text_output_index(&v);
                let out = if let Some(stream_key) = Self::text_stream_key(&v) {
                    self.emit_text_done_with_key(stream_key.as_str(), text.as_str())
                } else {
                    let item_id = Self::text_item_id(&v);
                    self.emit_done_text(item_id.as_deref(), text.as_str())
                };
                if out.is_some() {
                    self.streamed_text_output_indexes.extend(output_index);
                }
                Ok(out)
            }

            "response.output_item.added" => {
                // Best-effort: pre-register function_call metadata.
                if let Some(item) = v.get("item") {
                    let item_type = item.get("type").and_then(|t| t.as_str()).unwrap_or("");
                    if item_type == "function_call" {
                        let item_id = item
                            .get("id")
                            .and_then(|id| id.as_str())
                            .or_else(|| v.get("item_id").and_then(|id| id.as_str()))
                            .unwrap_or("");
                        if !item_id.is_empty() {
                            self.parse_fn_call_item(item_id, item);
                        }
                    }
                }
                Ok(None)
            }

            "response.function_call_arguments.delta" => {
                let item_id = v.get("item_id").and_then(|id| id.as_str()).unwrap_or("");
                let delta = v.get("delta").and_then(|d| d.as_str()).unwrap_or("");
                if item_id.is_empty() || delta.is_empty() {
                    return Ok(None);
                }
                let entry = self.ensure_fn_call(item_id);
                if entry.seeded_from_snapshot {
                    entry.seeded_from_snapshot = false;
                    // The seed came from an `output_item.added` snapshot. Two
                    // upstream shapes are indistinguishable until now:
                    //   (A) a PARTIAL prefix (e.g. `{"q":"`) that the deltas
                    //       continue — keep the seed and append; and
                    //   (B) a COMPLETE snapshot that the deltas restate from
                    //       scratch — appending would yield `snapshot + deltas`
                    //       (malformed/duplicated).
                    // A complete, parseable JSON OBJECT in the seed signals (B),
                    // so drop it before appending; otherwise keep it. Tool-call
                    // arguments are always objects, so a bare scalar/partial that
                    // merely parses (e.g. a lone number) is treated as a prefix.
                    // (#237 f.4)
                    if serde_json::from_str::<serde_json::Value>(&entry.arguments)
                        .is_ok_and(|v| v.is_object())
                    {
                        entry.arguments.clear();
                    }
                }
                entry.arguments.push_str(delta);
                Ok(None)
            }

            "response.output_item.done" => {
                // Emit tool call when the function_call item is done.
                let item_id = Self::text_item_id(&v).unwrap_or_default();
                let output_index = Self::text_output_index(&v);

                if let Some(item) = v.get("item") {
                    let item_type = item.get("type").and_then(|t| t.as_str()).unwrap_or("");
                    if item_type == "reasoning" {
                        if self.saw_reasoning_text_stream {
                            return Ok(None);
                        }
                        let text = Self::reasoning_item_summary_text(item);
                        return Ok(self.emit_reasoning_item_text(
                            if item_id.is_empty() {
                                None
                            } else {
                                Some(item_id.as_str())
                            },
                            text.as_str(),
                        ));
                    }
                    if item_type == "message" {
                        if output_index
                            .is_some_and(|index| self.streamed_text_output_indexes.contains(&index))
                        {
                            return Ok(None);
                        }
                        let text = Self::message_item_output_text(item);
                        let out = self.emit_done_text(
                            if item_id.is_empty() {
                                None
                            } else {
                                Some(item_id.as_str())
                            },
                            text.as_str(),
                        );
                        if out.is_some() {
                            if let Some(output_index) = output_index {
                                self.streamed_text_output_indexes.insert(output_index);
                            }
                        }
                        return Ok(out);
                    }
                    if item_type == "function_call" {
                        return Ok(self.emit_function_call_item(
                            item,
                            (!item_id.is_empty()).then_some(item_id.as_str()),
                            output_index,
                        ));
                    }
                }

                if item_id.is_empty() {
                    return Ok(None);
                }

                let Some(call) = self.finalize_tool_call(&item_id) else {
                    return Ok(None);
                };
                self.mark_tool_call_emitted(item_id.as_str(), call.id.as_str(), output_index);
                Ok(Some(LLMChunk::ToolCalls(vec![call])))
            }

            _ => Ok(None),
        }
    }

    /// Parse one Responses SSE event into every logical downstream chunk it carries.
    ///
    /// Most events produce zero or one chunk. `response.completed` can carry a
    /// response ID, several output items, provider usage, and terminal completion
    /// in the same frame; returning a vector keeps all of them without deferring
    /// state to a later event that may never arrive.
    pub fn handle_event_multi(&mut self, event: &str, data: &str) -> Result<Vec<LLMChunk>> {
        let Ok(v) = serde_json::from_str::<Value>(data) else {
            // Be lenient: some upstreams occasionally send non-JSON keepalives.
            return Ok(Vec::new());
        };

        let event_type = self.event_type(event, &v).to_string();
        if self.terminal_seen {
            return Ok(Vec::new());
        }

        let response_status = v
            .get("response")
            .and_then(|response| response.get("status"))
            .and_then(Value::as_str);
        let top_level_error = v.get("error").is_some_and(super::sse::sse_error_is_present);
        if top_level_error
            || matches!(response_status, Some("failed" | "incomplete" | "cancelled"))
            || matches!(
                event_type.as_str(),
                "response.failed"
                    | "response.incomplete"
                    | "response.cancelled"
                    | "response.error"
                    | "error"
            )
        {
            self.terminal_seen = true;
            let terminal_type = if event_type.is_empty() {
                "error"
            } else {
                event_type.as_str()
            };
            return Err(LLMError::Stream(Self::terminal_error_message(
                terminal_type,
                &v,
            )));
        }

        let mut chunks = Vec::new();
        if self.retain_protocol_events && event_type.starts_with("response.") {
            chunks.push(LLMChunk::ResponsesEvent {
                event_type: event_type.clone(),
                data: Box::new(v.clone()),
            });
        }
        if let Some(chunk) = self.maybe_emit_response_id(event_type.as_str(), &v) {
            chunks.push(chunk);
        }

        if event_type == "response.completed" {
            self.terminal_seen = true;
            chunks.extend(self.completed_provider_transcript_items(&v));
            chunks.extend(self.completed_output_chunks(&v));
            let usage = v
                .get("response")
                .and_then(|response| response.get("usage"))
                .or_else(|| v.get("usage"));
            self.log_reasoning_summary_if_needed(usage);
            if let Some(usage_chunk) =
                usage.and_then(crate::cache::provider_usage_from_openai_usage)
            {
                chunks.push(usage_chunk);
            }
            chunks.push(LLMChunk::Done);
            return Ok(chunks);
        }

        if let Some(chunk) = self.handle_event_value(event_type.as_str(), v)? {
            chunks.push(chunk);
        }
        Ok(chunks)
    }

    /// Backward-compatible helper for callers that only accept one chunk per event.
    ///
    /// Responses provider streams use [`Self::handle_event_multi`]. Returning an
    /// explicit error here prevents a multi-output terminal frame from being
    /// silently truncated by legacy callers.
    #[allow(dead_code)]
    pub fn handle_event(&mut self, event: &str, data: &str) -> Result<Option<LLMChunk>> {
        let mut chunks = self.handle_event_multi(event, data)?;
        match chunks.len() {
            0 => Ok(None),
            1 => Ok(chunks.pop()),
            count => Err(LLMError::Stream(format!(
                "Responses SSE event produced {count} chunks; use handle_event_multi"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{ContentPart, ImageUrl};
    use bamboo_domain::MessagePhase;
    use bamboo_domain::{FunctionCall, ToolCall};
    use bamboo_domain::{FunctionSchema, ToolSchema};

    #[test]
    fn build_responses_body_includes_input_and_stream() {
        let body =
            build_responses_body("gpt-5.3-codex", &[], &[], Some(123), None, None, None, None);
        assert_eq!(body["model"], "gpt-5.3-codex");
        assert_eq!(body["stream"], true);
        assert_eq!(body["max_output_tokens"], 123);
        assert_eq!(body["store"], false);
        assert!(body.get("input").is_some());
    }

    #[test]
    fn discovery_output_is_captured_and_replayed_at_its_message_anchor() {
        let output = json!([
            {"type":"message","id":"msg_1","role":"assistant","phase":"commentary","status":"completed","content":[{"type":"output_text","text":"Searching"}]},
            {"type":"tool_search_call","execution":"server","call_id":null,"status":"completed","arguments":{"query":"orders"}},
            {"type":"tool_search_output","execution":"server","call_id":null,"status":"completed","tools":[{"type":"function","name":"get_orders"}]},
            {"type":"function_call","call_id":"call_1","name":"get_orders","arguments":"{}"}
        ]);
        let mut parser = ResponsesSseParser::new_with_context("OpenAI", "gpt-5.6", None);
        let chunks = parser
            .handle_event_multi(
                "response.completed",
                &json!({"type":"response.completed","response":{"output":output}}).to_string(),
            )
            .unwrap();
        let items = chunks
            .into_iter()
            .filter_map(|chunk| match chunk {
                LLMChunk::ProviderTranscriptItem(item) => Some(item),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(items.len(), 4);
        assert_eq!(items[0].payload()["phase"], "commentary");

        let mut session = bamboo_domain::Session::new("native-openai", "gpt-5.6");
        session.add_message(Message::user("find orders"));
        let assistant = Message::assistant("normalized", None);
        let anchor = assistant.id.clone();
        session.add_message(assistant);
        session
            .append_provider_transcript_group(&anchor, None, items)
            .unwrap();
        let groups = session
            .provider_transcript
            .replayable_groups(ProviderFamily::OpenAi, ProviderProtocol::OpenAiResponsesV1)
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        let (input, _) = messages_to_responses_input_json_with_cache_and_native(
            &session.messages,
            None,
            &groups,
        );
        assert_eq!(input[1..], output.as_array().unwrap()[..]);
    }

    #[test]
    fn shared_responses_wire_keeps_openai_and_copilot_native_state_isolated() {
        let group = |family| {
            let mut session = bamboo_domain::Session::new("native", "model");
            let assistant = Message::assistant("", None);
            let anchor = assistant.id.clone();
            session.add_message(assistant);
            let item = ProviderTranscriptItem::try_from_payload(
                family,
                ProviderProtocol::OpenAiResponsesV1,
                ProviderTranscriptOrigin::Provider,
                ProviderTranscriptAuthor::Model,
                json!({
                    "type":"tool_search_call","execution":"server","call_id":null,
                    "status":"completed","arguments":{"query":"weather"}
                }),
            )
            .unwrap();
            session
                .append_provider_transcript_group(&anchor, None, vec![item])
                .unwrap();
            session.provider_transcript.groups()[0].clone()
        };
        let mut options = ResponsesRequestOptions {
            provider_transcript_groups: vec![
                group(ProviderFamily::OpenAi),
                group(ProviderFamily::Copilot),
            ],
            ..Default::default()
        };

        retain_provider_transcript_family(&mut options, ProviderFamily::OpenAi);
        assert_eq!(options.provider_transcript_groups.len(), 1);
        assert_eq!(
            options.provider_transcript_groups[0].family(),
            ProviderFamily::OpenAi
        );
    }

    #[test]
    fn build_responses_body_applies_responses_options() {
        let body = build_responses_body(
            "gpt-5.4",
            &[],
            &[],
            None,
            Some(ReasoningEffort::High),
            Some(&ResponsesRequestOptions {
                instructions: Some("You are helpful".to_string()),
                input_messages: None,
                provider_transcript_groups: Vec::new(),
                reasoning_summary: Some("detailed".to_string()),
                include: Some(vec!["reasoning.encrypted_content".to_string()]),
                store: Some(true),
                previous_response_id: Some("resp_123".to_string()),
                truncation: Some("auto".to_string()),
                text_verbosity: Some("high".to_string()),
                prompt_cache_key: None,
                prompt_cache_options: None,
                raw_input_with_cache_breakpoints: None,
                retain_protocol_events: false,
                prefix_epoch: None,
                prefix_reset_reason: None,
            }),
            None,
            None,
        );
        assert_eq!(body["reasoning"]["effort"], "high");
        assert_eq!(body["reasoning"]["summary"], "detailed");
        assert_eq!(body["instructions"], "You are helpful");
        assert_eq!(
            body["include"],
            serde_json::json!(["reasoning.encrypted_content"])
        );
        assert_eq!(body["store"], true);
        assert_eq!(body["previous_response_id"], "resp_123");
        assert_eq!(body["truncation"], "auto");
        assert_eq!(body["text"]["verbosity"], "high");
    }

    #[test]
    fn build_responses_body_preserves_max_reasoning_effort() {
        let body = build_responses_body(
            "gpt-5.6-sol",
            &[],
            &[],
            Some(32_000),
            Some(ReasoningEffort::Max),
            None,
            None,
            None,
        );

        assert_eq!(body["reasoning"]["effort"], "max");
        assert_eq!(body["max_output_tokens"], 32_000);
    }

    #[test]
    fn build_responses_body_keeps_legacy_models_on_xhigh() {
        let body = build_responses_body(
            "gpt-4o",
            &[],
            &[],
            Some(16_384),
            Some(ReasoningEffort::Max),
            None,
            None,
            None,
        );

        assert_eq!(body["reasoning"]["effort"], "xhigh");
        assert_eq!(body["max_output_tokens"], 16_384);
    }

    #[test]
    fn build_responses_body_with_parallel_tool_calls() {
        let body = build_responses_body("gpt-5.4", &[], &[], None, None, None, Some(false), None);
        assert_eq!(body["parallel_tool_calls"], false);
    }

    #[test]
    fn build_responses_body_deduplicates_matching_leading_system_message() {
        let messages = vec![
            Message::system("Stable instructions"),
            Message::user("Current task snapshot"),
        ];
        let body = build_responses_body(
            "gpt-5.4",
            &messages,
            &[],
            None,
            None,
            Some(&ResponsesRequestOptions {
                instructions: Some("Stable instructions".to_string()),
                ..Default::default()
            }),
            None,
            None,
        );

        assert_eq!(body["instructions"], "Stable instructions");
        let input = body["input"].as_array().expect("input array");
        assert_eq!(input.len(), 1);
        assert_eq!(input[0]["role"], "user");
        assert_eq!(input[0]["content"], "Current task snapshot");
    }

    #[test]
    fn build_responses_body_prefers_explicit_input_messages_over_generic_messages() {
        let generic_messages = vec![
            Message::system("Stable instructions"),
            Message::user("Generic conversation"),
        ];
        let explicit_input_messages = vec![Message::user("Responses-specific input")];

        let body = build_responses_body(
            "gpt-5.4",
            &generic_messages,
            &[],
            None,
            None,
            Some(&ResponsesRequestOptions {
                instructions: Some("Stable instructions".to_string()),
                input_messages: Some(explicit_input_messages),
                ..Default::default()
            }),
            None,
            None,
        );

        assert_eq!(body["instructions"], "Stable instructions");
        let input = body["input"].as_array().expect("input array");
        assert_eq!(input.len(), 1);
        assert_eq!(input[0]["role"], "user");
        assert_eq!(input[0]["content"], "Responses-specific input");
    }

    #[test]
    fn build_responses_body_continuation_keeps_explicit_input_messages_with_previous_response_id() {
        let generic_messages = vec![
            Message::system("Stable instructions"),
            Message::user("Generic conversation should not become responses input"),
        ];
        let explicit_input_messages = vec![
            Message::user("Dynamic context block"),
            Message::user("Latest continuation turn"),
        ];

        let body = build_responses_body(
            "gpt-5.4",
            &generic_messages,
            &[],
            None,
            None,
            Some(&ResponsesRequestOptions {
                instructions: Some("Stable instructions".to_string()),
                input_messages: Some(explicit_input_messages),
                previous_response_id: Some("resp_123".to_string()),
                ..Default::default()
            }),
            None,
            None,
        );

        assert_eq!(body["instructions"], "Stable instructions");
        assert_eq!(body["previous_response_id"], "resp_123");
        let input = body["input"].as_array().expect("input array");
        assert_eq!(input.len(), 2);
        assert_eq!(input[0]["role"], "user");
        assert_eq!(input[0]["content"], "Dynamic context block");
        assert_eq!(input[1]["role"], "user");
        assert_eq!(input[1]["content"], "Latest continuation turn");
        assert!(input.iter().all(|item| item["role"] != "system"));
    }

    #[test]
    fn build_responses_body_continuation_preserves_tool_loop_items_from_explicit_input_messages() {
        let generic_messages = vec![
            Message::system("Stable instructions"),
            Message::user("Generic turn should not be used"),
        ];
        let explicit_input_messages = vec![
            Message::user("Dynamic context"),
            Message::assistant(
                "calling tool",
                Some(vec![ToolCall {
                    id: "call_1".to_string(),
                    tool_type: "function".to_string(),
                    function: FunctionCall {
                        name: "search".to_string(),
                        arguments: r#"{"q":"zenith"}"#.to_string(),
                    },
                }]),
            ),
            Message::tool_result("call_1", "tool output"),
            Message::user("Continue after tool"),
        ];

        let body = build_responses_body(
            "gpt-5.4",
            &generic_messages,
            &[],
            None,
            None,
            Some(&ResponsesRequestOptions {
                instructions: Some("Stable instructions".to_string()),
                input_messages: Some(explicit_input_messages),
                previous_response_id: Some("resp_tool".to_string()),
                ..Default::default()
            }),
            None,
            None,
        );

        assert_eq!(body["previous_response_id"], "resp_tool");
        let input = body["input"].as_array().expect("input array");
        assert_eq!(input.len(), 5);
        assert_eq!(input[0]["type"], "message");
        assert_eq!(input[0]["role"], "user");
        assert_eq!(input[1]["type"], "message");
        assert_eq!(input[1]["role"], "assistant");
        assert_eq!(input[2]["type"], "function_call");
        assert_eq!(input[2]["call_id"], "call_1");
        assert_eq!(input[3]["type"], "function_call_output");
        assert_eq!(input[3]["call_id"], "call_1");
        assert_eq!(input[4]["type"], "message");
        assert_eq!(input[4]["role"], "user");
        assert_eq!(input[4]["content"], "Continue after tool");
    }

    #[test]
    fn select_responses_input_messages_only_uses_duplicate_system_fallback_for_generic_messages() {
        let generic_messages = vec![
            Message::system("Stable instructions"),
            Message::user("Generic conversation"),
        ];
        let explicit_input_messages = vec![
            Message::system("Stable instructions"),
            Message::user("Explicit responses input"),
        ];

        let generic_options = ResponsesRequestOptions {
            instructions: Some("Stable instructions".to_string()),
            ..Default::default()
        };
        let generic_selection =
            select_responses_input_messages(&generic_messages, Some(&generic_options));
        assert_eq!(generic_selection.source, ResponsesInputSource::Generic);
        assert!(generic_selection.fallback_removed_duplicate_system);
        assert_eq!(generic_selection.original_len, 2);
        assert_eq!(generic_selection.effective_len, 1);
        assert_eq!(
            generic_selection.input_messages[0].content,
            "Generic conversation"
        );

        let explicit_options = ResponsesRequestOptions {
            instructions: Some("Stable instructions".to_string()),
            input_messages: Some(explicit_input_messages),
            ..Default::default()
        };
        let explicit_selection =
            select_responses_input_messages(&generic_messages, Some(&explicit_options));
        assert_eq!(explicit_selection.source, ResponsesInputSource::Explicit);
        assert!(
            !explicit_selection.fallback_removed_duplicate_system,
            "explicit input_messages should be treated as already-curated Responses input"
        );
        assert_eq!(explicit_selection.original_len, 2);
        assert_eq!(explicit_selection.effective_len, 2);
        assert_eq!(explicit_selection.input_messages[0].role, Role::System);
    }

    #[test]
    fn tools_to_responses_json_flattens_function_schema() {
        let tools = vec![ToolSchema {
            schema_type: "function".to_string(),
            function: FunctionSchema {
                name: "search".to_string(),
                description: "Search things".to_string(),
                parameters: json!({
                    "type": "object",
                    "properties": { "q": { "type": "string" } },
                    "required": ["q"]
                }),
            },
        }];

        let out = tools_to_responses_json(&tools);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["type"], "function");
        assert_eq!(out[0]["name"], "search");
        assert_eq!(out[0]["description"], "Search things");
        assert!(out[0].get("function").is_none());
        assert!(out[0].get("parameters").is_some());
    }

    #[test]
    fn tools_to_responses_json_sanitizes_top_level_combinators() {
        let tools = vec![ToolSchema {
            schema_type: "function".to_string(),
            function: FunctionSchema {
                name: "edit".to_string(),
                description: "Edit file".to_string(),
                parameters: json!({
                    "type": "object",
                    "properties": {
                        "file_path": { "type": "string" },
                        "patch": { "type": "string" }
                    },
                    "oneOf": [
                        { "required": ["patch"] },
                        { "required": ["old_string", "new_string"] }
                    ]
                }),
            },
        }];

        let out = tools_to_responses_json(&tools);
        assert!(out[0]["parameters"]["oneOf"].is_null());
        assert_eq!(out[0]["parameters"]["type"], "object");
    }

    #[test]
    fn messages_to_responses_input_json_serializes_tool_calls_structurally() {
        let messages = vec![
            Message::assistant(
                "Calling a tool...",
                Some(vec![ToolCall {
                    id: "call_1".to_string(),
                    tool_type: "function".to_string(),
                    function: FunctionCall {
                        name: "search".to_string(),
                        arguments: r#"{"q":"x"}"#.to_string(),
                    },
                }]),
            ),
            Message::tool_result("call_1", "result payload"),
        ];

        let out = messages_to_responses_input_json(&messages);
        // Should produce 3 items: assistant message, function_call, function_call_output
        assert_eq!(out.len(), 3);

        // Item 0: assistant message with text content
        assert_eq!(out[0]["type"], "message");
        assert_eq!(out[0]["role"], "assistant");
        assert_eq!(out[0]["content"], "Calling a tool...");
        assert_eq!(out[0]["phase"], "commentary");

        // Item 1: structured function_call
        assert_eq!(out[1]["type"], "function_call");
        assert_eq!(out[1]["call_id"], "call_1");
        assert_eq!(out[1]["name"], "search");
        assert_eq!(out[1]["arguments"], r#"{"q":"x"}"#);

        // Item 2: structured function_call_output
        assert_eq!(out[2]["type"], "function_call_output");
        assert_eq!(out[2]["call_id"], "call_1");
        assert_eq!(out[2]["output"], "result payload");
    }

    #[test]
    fn messages_to_responses_input_json_assistant_with_only_tool_calls_no_text() {
        // When assistant has empty content and tool_calls, skip the message item
        let messages = vec![
            Message::assistant(
                "",
                Some(vec![ToolCall {
                    id: "call_1".to_string(),
                    tool_type: "function".to_string(),
                    function: FunctionCall {
                        name: "read_file".to_string(),
                        arguments: r#"{"path":"/tmp/test"}"#.to_string(),
                    },
                }]),
            ),
            Message::tool_result("call_1", "file contents"),
        ];

        let out = messages_to_responses_input_json(&messages);
        // Should produce 2 items: function_call, function_call_output (no empty assistant message)
        assert_eq!(out.len(), 2);

        assert_eq!(out[0]["type"], "function_call");
        assert_eq!(out[0]["call_id"], "call_1");
        assert_eq!(out[0]["name"], "read_file");

        assert_eq!(out[1]["type"], "function_call_output");
        assert_eq!(out[1]["call_id"], "call_1");
        assert_eq!(out[1]["output"], "file contents");
    }

    #[test]
    fn messages_to_responses_input_json_multiple_tool_calls_in_one_round() {
        let messages = vec![
            Message::user("Search and read"),
            Message::assistant(
                "",
                Some(vec![
                    ToolCall {
                        id: "call_1".to_string(),
                        tool_type: "function".to_string(),
                        function: FunctionCall {
                            name: "search".to_string(),
                            arguments: r#"{"q":"test"}"#.to_string(),
                        },
                    },
                    ToolCall {
                        id: "call_2".to_string(),
                        tool_type: "function".to_string(),
                        function: FunctionCall {
                            name: "read_file".to_string(),
                            arguments: r#"{"path":"/tmp"}"#.to_string(),
                        },
                    },
                ]),
            ),
            Message::tool_result("call_1", "search results"),
            Message::tool_result("call_2", "file contents"),
        ];

        let out = messages_to_responses_input_json(&messages);
        // user_msg + 2x function_call + 2x function_call_output = 5 items
        assert_eq!(out.len(), 5);

        assert_eq!(out[0]["type"], "message");
        assert_eq!(out[0]["role"], "user");

        assert_eq!(out[1]["type"], "function_call");
        assert_eq!(out[1]["call_id"], "call_1");
        assert_eq!(out[1]["name"], "search");

        assert_eq!(out[2]["type"], "function_call");
        assert_eq!(out[2]["call_id"], "call_2");
        assert_eq!(out[2]["name"], "read_file");

        assert_eq!(out[3]["type"], "function_call_output");
        assert_eq!(out[3]["call_id"], "call_1");

        assert_eq!(out[4]["type"], "function_call_output");
        assert_eq!(out[4]["call_id"], "call_2");
    }

    #[test]
    fn messages_to_responses_input_json_tool_result_without_call_id_falls_back() {
        let messages = vec![Message::tool_result("", "orphan result")];

        let out = messages_to_responses_input_json(&messages);
        assert_eq!(out.len(), 1);
        // Fallback: degrade to user message with prefix
        assert_eq!(out[0]["type"], "message");
        assert_eq!(out[0]["role"], "user");
        assert!(out[0]["content"]
            .as_str()
            .unwrap_or("")
            .contains("[tool_result]"));
    }

    /// #237 finding 6: a tool result carrying an image (e.g. an MCP screenshot)
    /// must reach the Responses API as a typed content array (`input_text` +
    /// `input_image`), not have the image silently dropped.
    #[test]
    fn messages_to_responses_input_json_tool_result_preserves_images() {
        let messages = vec![Message::tool_result_with_images(
            "call_1",
            "screenshot captured",
            true,
            vec![bamboo_domain::ToolResultImage {
                mime_type: "image/png".to_string(),
                data: "AAAA".to_string(),
            }],
        )];

        let out = messages_to_responses_input_json(&messages);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["type"], "function_call_output");
        assert_eq!(out[0]["call_id"], "call_1");

        let output = &out[0]["output"];
        let arr = output.as_array().expect("output should be a typed array");
        // The text result is preserved as an input_text part...
        assert!(
            arr.iter()
                .any(|p| p["type"] == "input_text" && p["text"] == "screenshot captured"),
            "text part missing: {output}"
        );
        // ...and the image is preserved as an input_image part.
        assert!(
            arr.iter()
                .any(|p| p["type"] == "input_image"
                    && p["image_url"] == "data:image/png;base64,AAAA"),
            "image part missing: {output}"
        );
    }

    /// A text-only tool result stays a plain string output (unchanged).
    #[test]
    fn messages_to_responses_input_json_text_only_tool_result_stays_string() {
        let messages = vec![Message::tool_result("call_9", "plain text result")];
        let out = messages_to_responses_input_json(&messages);
        assert_eq!(out[0]["type"], "function_call_output");
        assert_eq!(out[0]["output"], "plain text result");
    }

    /// A tool result with an image but no call_id still preserves the image on
    /// the degraded user-message fallback path.
    #[test]
    fn messages_to_responses_input_json_tool_result_image_fallback_preserves_image() {
        let messages = vec![Message::tool_result_with_images(
            "",
            "orphan screenshot",
            true,
            vec![bamboo_domain::ToolResultImage {
                mime_type: "image/png".to_string(),
                data: "BBBB".to_string(),
            }],
        )];

        let out = messages_to_responses_input_json(&messages);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["type"], "message");
        assert_eq!(out[0]["role"], "user");
        let arr = out[0]["content"]
            .as_array()
            .expect("fallback content should be a typed array");
        assert!(arr.iter().any(|p| p["type"] == "input_text"
            && p["text"].as_str().unwrap_or("").contains("[tool_result]")));
        assert!(arr
            .iter()
            .any(|p| p["type"] == "input_image" && p["image_url"] == "data:image/png;base64,BBBB"));
    }

    #[test]
    fn messages_to_responses_input_json_system_and_user_messages() {
        let messages = vec![Message::system("You are helpful"), Message::user("Hello")];

        let out = messages_to_responses_input_json(&messages);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["type"], "message");
        assert_eq!(out[0]["role"], "system");
        assert_eq!(out[0]["content"], "You are helpful");
        assert_eq!(out[1]["type"], "message");
        assert_eq!(out[1]["role"], "user");
        assert_eq!(out[1]["content"], "Hello");
    }

    #[test]
    fn messages_to_responses_input_json_assistant_without_tool_calls() {
        let messages = vec![Message::assistant("Just a text reply", None)];

        let out = messages_to_responses_input_json(&messages);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["type"], "message");
        assert_eq!(out[0]["role"], "assistant");
        assert_eq!(out[0]["content"], "Just a text reply");
        assert_eq!(out[0]["phase"], "final_answer");
    }

    #[test]
    fn messages_to_responses_input_json_uses_output_text_for_assistant_in_typed_content_mode() {
        let messages = vec![
            Message::user_with_parts(
                "Describe this image",
                vec![ContentPart::ImageUrl {
                    image_url: ImageUrl {
                        url: "https://example.com/image.png".to_string(),
                        detail: None,
                    },
                }]
                .into_iter()
                .map(Into::into)
                .collect(),
            ),
            Message::assistant("It is a screenshot.", None),
        ];

        let out = messages_to_responses_input_json(&messages);
        assert_eq!(out.len(), 2);
        assert_eq!(out[1]["type"], "message");
        assert_eq!(out[1]["role"], "assistant");
        assert_eq!(out[1]["content"][0]["type"], "output_text");
        assert_eq!(out[1]["content"][0]["text"], "It is a screenshot.");
    }

    #[test]
    fn messages_to_responses_input_json_honors_explicit_assistant_phase() {
        let mut assistant = Message::assistant("intermediate narration", None);
        assistant.phase = Some(MessagePhase::Commentary);

        let out = messages_to_responses_input_json(&[assistant]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["type"], "message");
        assert_eq!(out[0]["role"], "assistant");
        assert_eq!(out[0]["phase"], "commentary");
    }

    #[test]
    fn parser_emits_token_on_output_text_delta() {
        let mut p = ResponsesSseParser::new();
        let out = p
            .handle_event(
                "response.output_text.delta",
                r#"{"type":"response.output_text.delta","delta":"hi"}"#,
            )
            .unwrap();
        match out {
            Some(LLMChunk::Token(t)) => assert_eq!(t, "hi"),
            other => panic!("expected token, got {other:?}"),
        }
    }

    #[test]
    fn parser_emits_token_on_output_text_done_without_prior_delta() {
        let mut p = ResponsesSseParser::new();
        let out = p
            .handle_event(
                "response.output_text.done",
                r#"{"type":"response.output_text.done","item_id":"msg_1","text":"hello"}"#,
            )
            .unwrap();
        match out {
            Some(LLMChunk::Token(t)) => assert_eq!(t, "hello"),
            other => panic!("expected token, got {other:?}"),
        }
    }

    #[test]
    fn parser_empty_output_text_done_does_not_hide_completed_message() {
        let mut parser = ResponsesSseParser::new();
        let done = parser
            .handle_event(
                "response.output_text.done",
                r#"{"type":"response.output_text.done","item_id":"msg_terminal","output_index":0,"content_index":0,"text":""}"#,
            )
            .expect("empty text done");
        assert!(done.is_none());

        let completed = parser
            .handle_event_multi(
                "response.completed",
                r#"{"type":"response.completed","response":{"output":[{"id":"msg_terminal","type":"message","content":[{"type":"output_text","text":"real terminal text"}]}]}}"#,
            )
            .expect("completed event");

        assert_eq!(completed.len(), 2);
        assert!(matches!(
            &completed[0],
            LLMChunk::Token(text) if text == "real terminal text"
        ));
        assert!(matches!(completed[1], LLMChunk::Done));
    }

    #[test]
    fn parser_skips_output_text_done_after_streaming_delta_for_same_item() {
        let mut p = ResponsesSseParser::new();
        let _ = p
            .handle_event(
                "response.output_text.delta",
                r#"{"type":"response.output_text.delta","item_id":"msg_1","delta":"hel"}"#,
            )
            .unwrap();

        let out = p
            .handle_event(
                "response.output_text.done",
                r#"{"type":"response.output_text.done","item_id":"msg_1","text":"hello"}"#,
            )
            .unwrap();
        assert!(out.is_none());
    }

    #[test]
    fn parser_ignores_snapshot_text_on_content_part_added() {
        let mut p = ResponsesSseParser::new();
        let out = p
            .handle_event(
                "response.content_part.added",
                r#"{"type":"response.content_part.added","item_id":"msg_2","part":{"type":"output_text","text":"hello from part added"}}"#,
            )
            .unwrap();
        assert!(out.is_none());
    }

    #[test]
    fn parser_emits_token_on_content_part_done_without_prior_delta() {
        let mut p = ResponsesSseParser::new();
        let out = p
            .handle_event(
                "response.content_part.done",
                r#"{"type":"response.content_part.done","item_id":"msg_3","part":{"type":"output_text","text":"hello from part done"}}"#,
            )
            .unwrap();
        match out {
            Some(LLMChunk::Token(t)) => assert_eq!(t, "hello from part done"),
            other => panic!("expected token, got {other:?}"),
        }
    }

    #[test]
    fn parser_empty_or_non_text_content_part_done_does_not_hide_completed_message() {
        let done_payloads = [
            r#"{"type":"response.content_part.done","item_id":"msg_terminal","output_index":0,"content_index":0,"part":{"type":"output_text","text":""}}"#,
            r#"{"type":"response.content_part.done","item_id":"msg_terminal","output_index":0,"content_index":0,"part":{"type":"refusal","refusal":"not output text"}}"#,
        ];

        for done_payload in done_payloads {
            let mut parser = ResponsesSseParser::new();
            let done = parser
                .handle_event("response.content_part.done", done_payload)
                .expect("content part done");
            assert!(done.is_none());

            let completed = parser
                .handle_event_multi(
                    "response.completed",
                    r#"{"type":"response.completed","response":{"output":[{"id":"msg_terminal","type":"message","content":[{"type":"output_text","text":"real terminal text"}]}]}}"#,
                )
                .expect("completed event");

            assert_eq!(completed.len(), 2);
            assert!(matches!(
                &completed[0],
                LLMChunk::Token(text) if text == "real terminal text"
            ));
            assert!(matches!(completed[1], LLMChunk::Done));
        }
    }

    #[test]
    fn parser_skips_content_part_done_after_text_stream_for_same_item() {
        let mut p = ResponsesSseParser::new();
        let _ = p
            .handle_event(
                "response.output_text.delta",
                r#"{"type":"response.output_text.delta","item_id":"msg_4","delta":"hello"}"#,
            )
            .unwrap();

        let out = p
            .handle_event(
                "response.content_part.done",
                r#"{"type":"response.content_part.done","item_id":"msg_4","part":{"type":"output_text","text":"hello"}}"#,
            )
            .unwrap();
        assert!(out.is_none());
    }

    #[test]
    fn parser_emits_response_id_on_created_event() {
        let mut p = ResponsesSseParser::new();
        let out = p
            .handle_event(
                "response.created",
                r#"{"type":"response.created","response":{"id":"resp_123","status":"in_progress"}}"#,
            )
            .unwrap();
        match out {
            Some(LLMChunk::ResponseId(response_id)) => assert_eq!(response_id, "resp_123"),
            other => panic!("expected response id, got {other:?}"),
        }
    }

    #[test]
    fn parser_does_not_confuse_item_id_with_response_id() {
        let mut p = ResponsesSseParser::new();
        let out = p
            .handle_event(
                "response.output_item.added",
                r#"{"type":"response.output_item.added","item":{"id":"item_1","type":"function_call","call_id":"call_1","name":"search","arguments":"{}"}}"#,
            )
            .unwrap();
        assert!(out.is_none());
    }

    #[test]
    fn parser_recovers_terminal_only_message_from_completed_output() {
        let mut parser = ResponsesSseParser::new();
        let chunks = parser
            .handle_event_multi(
                "response.completed",
                r#"{"type":"response.completed","response":{"output":[{"id":"msg_terminal","type":"message","role":"assistant","content":[{"type":"output_text","text":"Hello from completion"}]}]}}"#,
            )
            .expect("completed event");

        assert_eq!(chunks.len(), 2);
        match &chunks[0] {
            LLMChunk::Token(text) => assert_eq!(text, "Hello from completion"),
            other => panic!("expected terminal token, got {other:?}"),
        }
        assert!(matches!(chunks[1], LLMChunk::Done));
    }

    #[test]
    fn parser_recovers_terminal_only_function_call_from_completed_output() {
        let mut parser = ResponsesSseParser::new();
        let chunks = parser
            .handle_event_multi(
                "response.completed",
                r#"{"type":"response.completed","response":{"output":[{"id":"fc_terminal","type":"function_call","call_id":"call_terminal","name":"search","arguments":"{\"q\":\"bamboo\"}"}]}}"#,
            )
            .expect("completed event");

        assert_eq!(chunks.len(), 2);
        match &chunks[0] {
            LLMChunk::ToolCalls(calls) => {
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].id, "call_terminal");
                assert_eq!(calls[0].function.name, "search");
                assert_eq!(calls[0].function.arguments, r#"{"q":"bamboo"}"#);
            }
            other => panic!("expected terminal tool call, got {other:?}"),
        }
        assert!(matches!(chunks[1], LLMChunk::Done));
    }

    #[test]
    fn parser_completed_fallback_does_not_repeat_normally_streamed_text_or_tool() {
        let mut parser = ResponsesSseParser::new();

        let created = parser
            .handle_event(
                "response.created",
                r#"{"type":"response.created","response":{"id":"resp_streamed"}}"#,
            )
            .expect("created event");
        assert!(matches!(
            created,
            Some(LLMChunk::ResponseId(response_id)) if response_id == "resp_streamed"
        ));

        let text = parser
            .handle_event(
                "response.output_text.delta",
                r#"{"type":"response.output_text.delta","item_id":"msg_streamed","output_index":0,"content_index":0,"delta":"already streamed"}"#,
            )
            .expect("text delta");
        assert!(matches!(
            text,
            Some(LLMChunk::Token(token)) if token == "already streamed"
        ));

        parser
            .handle_event(
                "response.output_item.added",
                r#"{"type":"response.output_item.added","output_index":1,"item":{"id":"fc_streamed","type":"function_call","call_id":"call_streamed","name":"search","arguments":""}}"#,
            )
            .expect("tool item added");
        parser
            .handle_event(
                "response.function_call_arguments.delta",
                r#"{"type":"response.function_call_arguments.delta","item_id":"fc_streamed","output_index":1,"delta":"{\"q\":\"streamed\"}"}"#,
            )
            .expect("tool arguments delta");
        let tool = parser
            .handle_event(
                "response.output_item.done",
                r#"{"type":"response.output_item.done","output_index":1,"item":{"id":"fc_streamed","type":"function_call","call_id":"call_streamed","name":"search","arguments":"{\"q\":\"streamed\"}"}}"#,
            )
            .expect("tool item done");
        assert!(matches!(tool, Some(LLMChunk::ToolCalls(calls)) if calls.len() == 1));

        let completed = parser
            .handle_event_multi(
                "response.completed",
                r#"{"type":"response.completed","response":{"id":"resp_streamed","output":[{"id":"msg_streamed","type":"message","content":[{"type":"output_text","text":"already streamed"}]},{"id":"fc_streamed","type":"function_call","call_id":"call_streamed","name":"search","arguments":"{\"q\":\"streamed\"}"}]}}"#,
            )
            .expect("completed event");

        assert_eq!(completed.len(), 1);
        assert!(matches!(completed[0], LLMChunk::Done));
    }

    #[test]
    fn parser_completed_output_preserves_multiple_mixed_items_and_ignores_unknowns() {
        let mut parser = ResponsesSseParser::new();
        let chunks = parser
            .handle_event_multi(
                "response.completed",
                r#"{"type":"response.completed","response":{"output":[{"id":"msg_1","type":"message","content":[{"type":"output_text","text":"first"},{"type":"refusal","refusal":"ignored"},{"type":"output_text","text":" message"}]},{"type":"computer_call","id":"unknown_1"},{"id":"fc_1","type":"function_call","call_id":"call_1","name":"read_file","arguments":"{\"path\":\"a\"}"},{"id":"fc_bad","type":"function_call","call_id":"call_bad","arguments":"{}"},{"id":"msg_2","type":"message","content":[{"type":"output_text","text":"second message"}]},{"id":"fc_2","type":"function_call","call_id":"call_2","name":"read_file","arguments":"{\"path\":\"b\"}"},null]}}"#,
            )
            .expect("completed event");

        assert_eq!(chunks.len(), 5);
        assert!(matches!(&chunks[0], LLMChunk::Token(text) if text == "first message"));
        assert!(
            matches!(&chunks[1], LLMChunk::ToolCalls(calls) if calls.len() == 1 && calls[0].id == "call_1")
        );
        assert!(matches!(&chunks[2], LLMChunk::Token(text) if text == "second message"));
        assert!(
            matches!(&chunks[3], LLMChunk::ToolCalls(calls) if calls.len() == 1 && calls[0].id == "call_2")
        );
        assert!(matches!(chunks[4], LLMChunk::Done));
    }

    #[test]
    fn parser_completed_only_response_id_coexists_with_output_usage_and_done() {
        let mut parser = ResponsesSseParser::new();
        let chunks = parser
            .handle_event_multi(
                "response.completed",
                r#"{"type":"response.completed","response":{"id":"resp_terminal","output":[{"id":"msg_terminal","type":"message","content":[{"type":"output_text","text":"terminal answer"}]}],"usage":{"input_tokens":21,"output_tokens":13,"input_tokens_details":{"cached_tokens":8},"output_tokens_details":{"reasoning_tokens":5}}}}"#,
            )
            .expect("completed event");

        assert_eq!(chunks.len(), 4);
        assert!(matches!(&chunks[0], LLMChunk::ResponseId(id) if id == "resp_terminal"));
        assert!(matches!(&chunks[1], LLMChunk::Token(text) if text == "terminal answer"));
        assert!(matches!(
            chunks[2],
            LLMChunk::ProviderUsage {
                input_tokens: Some(21),
                output_tokens: Some(13),
                reasoning_tokens: Some(5),
                cache_creation_input_tokens: None,
                cache_read_input_tokens: Some(8),
                ..
            }
        ));
        assert!(matches!(chunks[3], LLMChunk::Done));
        assert_eq!(
            chunks
                .iter()
                .filter(|chunk| matches!(chunk, LLMChunk::ProviderUsage { .. }))
                .count(),
            1,
            "one terminal provider event must emit usage exactly once"
        );
        assert!(
            chunks
                .iter()
                .all(|chunk| !matches!(chunk, LLMChunk::CacheUsage { .. })),
            "combined provider usage must not also emit legacy cache usage"
        );
    }

    #[test]
    fn parser_completed_usage_preserves_totals_with_zero_cache() {
        let mut parser = ResponsesSseParser::new();
        let chunks = parser
            .handle_event_multi(
                "response.completed",
                r#"{"type":"response.completed","response":{"usage":{"input_tokens":34,"output_tokens":12,"input_tokens_details":{"cached_tokens":0},"output_tokens_details":{"reasoning_tokens":3}}}}"#,
            )
            .expect("completed event");

        assert_eq!(chunks.len(), 2);
        assert!(matches!(
            chunks[0],
            LLMChunk::ProviderUsage {
                input_tokens: Some(34),
                output_tokens: Some(12),
                reasoning_tokens: Some(3),
                cache_creation_input_tokens: None,
                cache_read_input_tokens: Some(0),
                ..
            }
        ));
        assert!(matches!(chunks[1], LLMChunk::Done));
    }

    #[test]
    fn parser_completed_usage_does_not_invent_missing_totals() {
        let mut parser = ResponsesSseParser::new();
        let chunks = parser
            .handle_event_multi(
                "response.completed",
                r#"{"type":"response.completed","response":{"usage":{"total_tokens":46,"input_tokens":null,"output_tokens_details":{}}}}"#,
            )
            .expect("completed event");

        assert_eq!(chunks.len(), 2);
        assert!(matches!(
            chunks[0],
            LLMChunk::ProviderUsage {
                input_tokens: None,
                output_tokens: None,
                total_tokens: Some(46),
                reasoning_tokens: None,
                cache_creation_input_tokens: None,
                cache_read_input_tokens: None,
                cache_write_input_tokens: None,
            }
        ));
        assert!(matches!(chunks[1], LLMChunk::Done));
    }

    #[test]
    fn parser_completed_is_lenient_for_malformed_payloads() {
        let mut parser = ResponsesSseParser::new();
        assert!(parser
            .handle_event_multi("response.completed", "not-json")
            .expect("malformed keepalive")
            .is_empty());

        let chunks = parser
            .handle_event_multi(
                "response.completed",
                r#"{"type":"response.completed","response":{"output":"not-an-array"}}"#,
            )
            .expect("malformed output");
        assert_eq!(chunks.len(), 1);
        assert!(matches!(chunks[0], LLMChunk::Done));
    }

    /// #237 finding 4: an aggregator that puts a non-empty `arguments` snapshot
    /// in `output_item.added` AND also streams `function_call_arguments.delta`
    /// must not yield `snapshot + deltas`. The delta stream is authoritative, so
    /// the seed is dropped on the first delta.
    #[test]
    fn parser_snapshot_plus_deltas_does_not_duplicate_tool_args() {
        let mut p = ResponsesSseParser::new();

        // added carries a full snapshot...
        p.handle_event(
            "response.output_item.added",
            r#"{"type":"response.output_item.added","item":{"id":"item_1","type":"function_call","call_id":"call_1","name":"search","arguments":"{\"q\":\"hello\"}"}}"#,
        )
        .unwrap();

        // ...and the same args are ALSO streamed as deltas (from scratch).
        p.handle_event(
            "response.function_call_arguments.delta",
            r#"{"type":"response.function_call_arguments.delta","item_id":"item_1","delta":"{\"q\":\""}"#,
        )
        .unwrap();
        p.handle_event(
            "response.function_call_arguments.delta",
            r#"{"type":"response.function_call_arguments.delta","item_id":"item_1","delta":"hello\"}"}"#,
        )
        .unwrap();

        // done WITHOUT arguments, so it can't mask a duplication by overwriting.
        let out = p
            .handle_event(
                "response.output_item.done",
                r#"{"type":"response.output_item.done","item":{"id":"item_1","type":"function_call","call_id":"call_1","name":"search"}}"#,
            )
            .unwrap();

        match out {
            Some(LLMChunk::ToolCalls(calls)) => {
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].function.arguments, r#"{"q":"hello"}"#);
            }
            other => panic!("expected a single tool call, got {other:?}"),
        }
    }

    /// The normal delta-streaming path (empty `added` snapshot) is unaffected:
    /// deltas accumulate as before.
    #[test]
    fn parser_empty_snapshot_then_deltas_accumulates_normally() {
        let mut p = ResponsesSseParser::new();
        p.handle_event(
            "response.output_item.added",
            r#"{"type":"response.output_item.added","item":{"id":"item_2","type":"function_call","call_id":"call_2","name":"search","arguments":""}}"#,
        )
        .unwrap();
        p.handle_event(
            "response.function_call_arguments.delta",
            r#"{"type":"response.function_call_arguments.delta","item_id":"item_2","delta":"{\"a\":"}"#,
        )
        .unwrap();
        p.handle_event(
            "response.function_call_arguments.delta",
            r#"{"type":"response.function_call_arguments.delta","item_id":"item_2","delta":"1}"}"#,
        )
        .unwrap();
        let out = p
            .handle_event(
                "response.output_item.done",
                r#"{"type":"response.output_item.done","item":{"id":"item_2","type":"function_call","call_id":"call_2","name":"search"}}"#,
            )
            .unwrap();
        match out {
            Some(LLMChunk::ToolCalls(calls)) => {
                assert_eq!(calls[0].function.arguments, r#"{"a":1}"#);
            }
            other => panic!("expected a single tool call, got {other:?}"),
        }
    }

    #[test]
    fn parser_ignores_message_output_item_added_snapshot() {
        let mut p = ResponsesSseParser::new();
        let out = p
            .handle_event(
                "response.output_item.added",
                r#"{"type":"response.output_item.added","item":{"id":"msg_added_1","type":"message","content":[{"type":"output_text","text":"thinking before tool"}]}}"#,
            )
            .unwrap();
        assert!(out.is_none());
    }

    #[test]
    fn parser_emits_message_output_item_done_after_added_snapshot() {
        let mut p = ResponsesSseParser::new();
        let _ = p
            .handle_event(
                "response.output_item.added",
                r#"{"type":"response.output_item.added","item":{"id":"msg_added_2","type":"message","content":[{"type":"output_text","text":"thinking"}]}}"#,
            )
            .unwrap();

        let out = p
            .handle_event(
                "response.output_item.done",
                r#"{"type":"response.output_item.done","item":{"id":"msg_added_2","type":"message","content":[{"type":"output_text","text":"thinking"}]}}"#,
            )
            .unwrap();
        match out {
            Some(LLMChunk::Token(t)) => assert_eq!(t, "thinking"),
            other => panic!("expected token, got {other:?}"),
        }
    }

    #[test]
    fn parser_emits_reasoning_token_on_reasoning_delta() {
        let mut p = ResponsesSseParser::new();
        let out = p
            .handle_event(
                "response.reasoning.delta",
                r#"{"type":"response.reasoning.delta","delta":"think"}"#,
            )
            .unwrap();
        match out {
            Some(LLMChunk::ReasoningToken(t)) => assert_eq!(t, "think"),
            other => panic!("expected reasoning token, got {other:?}"),
        }
    }

    #[test]
    fn parser_emits_reasoning_token_from_reasoning_output_item_done() {
        let mut p = ResponsesSseParser::new();
        let out = p
            .handle_event(
                "response.output_item.done",
                r#"{"type":"response.output_item.done","item":{"id":"rs_1","type":"reasoning","summary":[{"type":"summary_text","text":"I will inspect the repo first."}]}}"#,
            )
            .unwrap();
        match out {
            Some(LLMChunk::ReasoningToken(t)) => assert_eq!(t, "I will inspect the repo first."),
            other => panic!("expected reasoning token, got {other:?}"),
        }
    }

    #[test]
    fn parser_joins_multiple_reasoning_summary_parts_with_blank_line() {
        let mut p = ResponsesSseParser::new();
        let out = p
            .handle_event(
                "response.output_item.done",
                r#"{"type":"response.output_item.done","item":{"id":"rs_multi_1","type":"reasoning","summary":[{"type":"summary_text","text":"Evaluating local changes"},{"type":"summary_text","text":"Committing release version"}]}}"#,
            )
            .unwrap();
        match out {
            Some(LLMChunk::ReasoningToken(t)) => {
                assert_eq!(t, "Evaluating local changes\n\nCommitting release version")
            }
            other => panic!("expected reasoning token, got {other:?}"),
        }
    }

    #[test]
    fn parser_skips_duplicate_reasoning_done_after_reasoning_delta_for_same_item() {
        let mut p = ResponsesSseParser::new();
        let _ = p
            .handle_event(
                "response.reasoning_summary_text.delta",
                r#"{"type":"response.reasoning_summary_text.delta","item_id":"rs_2","delta":"Planning now."}"#,
            )
            .unwrap();
        let out = p
            .handle_event(
                "response.reasoning_summary_text.done",
                r#"{"type":"response.reasoning_summary_text.done","item_id":"rs_2","text":"Planning now."}"#,
            )
            .unwrap();
        assert!(out.is_none());
    }

    #[test]
    fn parser_skips_duplicate_reasoning_output_item_done_after_reasoning_delta() {
        let mut p = ResponsesSseParser::new();
        let _ = p
            .handle_event(
                "response.reasoning_summary_text.delta",
                r#"{"type":"response.reasoning_summary_text.delta","item_id":"rs_3","delta":"Planning now."}"#,
            )
            .unwrap();
        let out = p
            .handle_event(
                "response.output_item.done",
                r#"{"type":"response.output_item.done","item":{"id":"rs_3","type":"reasoning","summary":[{"type":"summary_text","text":"Planning now."}]}}"#,
            )
            .unwrap();
        assert!(out.is_none());
    }

    #[test]
    fn parser_emits_tool_call_on_output_item_done() {
        let mut p = ResponsesSseParser::new();

        let _ = p
            .handle_event(
                "response.output_item.added",
                r#"{"type":"response.output_item.added","item":{"id":"item_1","type":"function_call","call_id":"call_1","name":"search","arguments":"{\"q\":\""}}"#,
            )
            .unwrap();

        let _ = p
            .handle_event(
                "response.function_call_arguments.delta",
                r#"{"type":"response.function_call_arguments.delta","item_id":"item_1","delta":"test\"}"}"#,
            )
            .unwrap();

        let out = p
            .handle_event(
                "response.output_item.done",
                r#"{"type":"response.output_item.done","item_id":"item_1","item":{"id":"item_1","type":"function_call"}}"#,
            )
            .unwrap();

        match out {
            Some(LLMChunk::ToolCalls(calls)) => {
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].id, "call_1");
                assert_eq!(calls[0].function.name, "search");
                assert_eq!(calls[0].function.arguments, r#"{"q":"test"}"#);
            }
            other => panic!("expected tool_calls, got {other:?}"),
        }
    }

    #[test]
    fn parser_keeps_outer_item_id_for_sparse_function_call_done_item() {
        let mut parser = ResponsesSseParser::new();
        parser
            .handle_event(
                "response.output_item.added",
                r#"{"type":"response.output_item.added","item":{"id":"fc_outer","type":"function_call","call_id":"call_outer","name":"search","arguments":""}}"#,
            )
            .expect("tool item added");
        parser
            .handle_event(
                "response.function_call_arguments.delta",
                r#"{"type":"response.function_call_arguments.delta","item_id":"fc_outer","delta":"{\"q\":\"outer\"}"}"#,
            )
            .expect("tool arguments delta");

        let chunk = parser
            .handle_event(
                "response.output_item.done",
                r#"{"type":"response.output_item.done","item_id":"fc_outer","item":{"type":"function_call"}}"#,
            )
            .expect("tool item done");

        assert!(matches!(
            chunk,
            Some(LLMChunk::ToolCalls(calls))
                if calls.len() == 1
                    && calls[0].id == "call_outer"
                    && calls[0].function.name == "search"
                    && calls[0].function.arguments == r#"{"q":"outer"}"#
        ));
    }

    #[test]
    fn parser_sparse_function_done_call_id_reuses_outer_item_accumulator() {
        let mut parser = ResponsesSseParser::new();
        parser
            .handle_event(
                "response.output_item.added",
                r#"{"type":"response.output_item.added","item_id":"fc_outer","item":{"type":"function_call","call_id":"call_outer","name":"search","arguments":""}}"#,
            )
            .expect("tool item added");
        parser
            .handle_event(
                "response.function_call_arguments.delta",
                r#"{"type":"response.function_call_arguments.delta","item_id":"fc_outer","delta":"{\"q\":\"outer\"}"}"#,
            )
            .expect("tool arguments delta");

        let chunk = parser
            .handle_event(
                "response.output_item.done",
                r#"{"type":"response.output_item.done","item_id":"fc_outer","item":{"type":"function_call","call_id":"call_outer"}}"#,
            )
            .expect("sparse tool item done");

        assert!(matches!(
            chunk,
            Some(LLMChunk::ToolCalls(calls))
                if calls.len() == 1
                    && calls[0].id == "call_outer"
                    && calls[0].function.name == "search"
                    && calls[0].function.arguments == r#"{"q":"outer"}"#
        ));
    }

    #[test]
    fn parser_prefers_done_arguments_snapshot_over_accumulated_partials() {
        let mut p = ResponsesSseParser::new();

        let _ = p
            .handle_event(
                "response.output_item.added",
                r#"{"type":"response.output_item.added","item":{"id":"item_2","type":"function_call","call_id":"call_2","name":"search","arguments":"{\"q\":\""}}"#,
            )
            .unwrap();

        let _ = p
            .handle_event(
                "response.function_call_arguments.delta",
                r#"{"type":"response.function_call_arguments.delta","item_id":"item_2","delta":"test\"}"}"#,
            )
            .unwrap();

        let out = p
            .handle_event(
                "response.output_item.done",
                r#"{"type":"response.output_item.done","item":{"id":"item_2","type":"function_call","call_id":"call_2","name":"search","arguments":"{\"q\":\"test\"}"}}"#,
            )
            .unwrap();

        match out {
            Some(LLMChunk::ToolCalls(calls)) => {
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].id, "call_2");
                assert_eq!(calls[0].function.name, "search");
                assert_eq!(calls[0].function.arguments, r#"{"q":"test"}"#);
            }
            other => panic!("expected tool_calls, got {other:?}"),
        }
    }

    #[test]
    fn parser_emits_token_from_message_output_item_done_when_no_deltas_seen() {
        let mut p = ResponsesSseParser::new();
        let out = p
            .handle_event(
                "response.output_item.done",
                r#"{"type":"response.output_item.done","item":{"id":"msg_1","type":"message","content":[{"type":"output_text","text":"hello from item"}]}}"#,
            )
            .unwrap();

        match out {
            Some(LLMChunk::Token(t)) => assert_eq!(t, "hello from item"),
            other => panic!("expected token, got {other:?}"),
        }
    }

    #[test]
    fn parser_skips_message_output_item_done_when_text_delta_already_streamed() {
        let mut p = ResponsesSseParser::new();
        let _ = p
            .handle_event(
                "response.output_text.delta",
                r#"{"type":"response.output_text.delta","item_id":"msg_1","delta":"hello"}"#,
            )
            .unwrap();

        let out = p
            .handle_event(
                "response.output_item.done",
                r#"{"type":"response.output_item.done","item":{"id":"msg_1","type":"message","content":[{"type":"output_text","text":"hello"}]}}"#,
            )
            .unwrap();
        assert!(out.is_none());
    }

    #[test]
    fn parser_avoids_duplicate_text_when_snapshot_and_delta_channels_overlap() {
        let mut p = ResponsesSseParser::new();

        let mut emitted = Vec::new();

        let out = p
            .handle_event(
                "response.output_item.added",
                r#"{"type":"response.output_item.added","item":{"id":"msg_overlap","type":"message","content":[{"type":"output_text","text":"hello"}]}}"#,
            )
            .unwrap();
        if let Some(chunk) = out {
            emitted.push(chunk);
        }

        let out = p
            .handle_event(
                "response.content_part.added",
                r#"{"type":"response.content_part.added","item_id":"msg_overlap","part":{"type":"output_text","text":"hello"}}"#,
            )
            .unwrap();
        if let Some(chunk) = out {
            emitted.push(chunk);
        }

        let out = p
            .handle_event(
                "response.output_text.delta",
                r#"{"type":"response.output_text.delta","item_id":"msg_overlap","delta":"hello"}"#,
            )
            .unwrap();
        if let Some(chunk) = out {
            emitted.push(chunk);
        }

        let out = p
            .handle_event(
                "response.output_text.done",
                r#"{"type":"response.output_text.done","item_id":"msg_overlap","text":"hello"}"#,
            )
            .unwrap();
        if let Some(chunk) = out {
            emitted.push(chunk);
        }

        let out = p
            .handle_event(
                "response.output_item.done",
                r#"{"type":"response.output_item.done","item":{"id":"msg_overlap","type":"message","content":[{"type":"output_text","text":"hello"}]}}"#,
            )
            .unwrap();
        if let Some(chunk) = out {
            emitted.push(chunk);
        }

        let tokens: Vec<String> = emitted
            .into_iter()
            .filter_map(|chunk| match chunk {
                LLMChunk::Token(text) => Some(text),
                _ => None,
            })
            .collect();

        assert_eq!(tokens, vec!["hello".to_string()]);
    }

    #[test]
    fn parser_emits_single_token_when_done_channels_repeat_same_text_with_different_item_ids() {
        let mut p = ResponsesSseParser::new();
        let mut emitted = Vec::new();

        let out = p
            .handle_event(
                "response.output_text.done",
                r#"{"type":"response.output_text.done","item_id":"msg_a","output_index":0,"content_index":0,"text":"Hi! What can I help you with?"}"#,
            )
            .unwrap();
        if let Some(chunk) = out {
            emitted.push(chunk);
        }

        let out = p
            .handle_event(
                "response.content_part.done",
                r#"{"type":"response.content_part.done","item_id":"msg_b","output_index":0,"part":{"type":"output_text","text":"Hi! What can I help you with?"}}"#,
            )
            .unwrap();
        if let Some(chunk) = out {
            emitted.push(chunk);
        }

        let out = p
            .handle_event(
                "response.output_item.done",
                r#"{"type":"response.output_item.done","output_index":0,"item":{"id":"msg_c","type":"message","content":[{"type":"output_text","text":"Hi! What can I help you with?"}]}}"#,
            )
            .unwrap();
        if let Some(chunk) = out {
            emitted.push(chunk);
        }

        let tokens: Vec<String> = emitted
            .into_iter()
            .filter_map(|chunk| match chunk {
                LLMChunk::Token(text) => Some(text),
                _ => None,
            })
            .collect();
        assert_eq!(tokens, vec!["Hi! What can I help you with?".to_string()]);
    }

    #[test]
    fn parser_handles_cumulative_reasoning_delta_for_same_item_as_suffix() {
        let mut p = ResponsesSseParser::new();
        let first = p
            .handle_event(
                "response.reasoning_summary_text.delta",
                r#"{"type":"response.reasoning_summary_text.delta","item_id":"rs_snap","delta":"Analyzing project structure"}"#,
            )
            .unwrap();
        match first {
            Some(LLMChunk::ReasoningToken(text)) => assert_eq!(text, "Analyzing project structure"),
            other => panic!("expected reasoning token, got {other:?}"),
        }

        let second = p
            .handle_event(
                "response.reasoning_summary_text.delta",
                r#"{"type":"response.reasoning_summary_text.delta","item_id":"rs_snap","delta":"Analyzing project structure and listing candidate files"}"#,
            )
            .unwrap();
        match second {
            Some(LLMChunk::ReasoningToken(text)) => {
                assert_eq!(text, " and listing candidate files")
            }
            other => panic!("expected reasoning suffix token, got {other:?}"),
        }

        let done = p
            .handle_event(
                "response.reasoning_summary_text.done",
                r#"{"type":"response.reasoning_summary_text.done","item_id":"rs_snap","text":"Analyzing project structure and listing candidate files"}"#,
            )
            .unwrap();
        assert!(done.is_none());
    }

    #[test]
    fn parser_skips_reasoning_summary_done_for_same_summary_stream_when_item_ids_differ() {
        let mut p = ResponsesSseParser::new();
        let _ = p
            .handle_event(
                "response.reasoning_summary_text.delta",
                r#"{"type":"response.reasoning_summary_text.delta","item_id":"rs_a","output_index":0,"summary_index":7,"delta":"Planning now."}"#,
            )
            .unwrap();

        let out = p
            .handle_event(
                "response.reasoning_summary_text.done",
                r#"{"type":"response.reasoning_summary_text.done","item_id":"rs_b","output_index":0,"summary_index":7,"text":"Planning now."}"#,
            )
            .unwrap();
        assert!(out.is_none());
    }

    #[test]
    fn parser_normalizes_cumulative_reasoning_delta_by_summary_stream_key() {
        let mut p = ResponsesSseParser::new();
        let first = p
            .handle_event(
                "response.reasoning_summary_text.delta",
                r#"{"type":"response.reasoning_summary_text.delta","item_id":"rs_key_1","output_index":0,"summary_index":3,"delta":"Analyzing project"}"#,
            )
            .unwrap();
        match first {
            Some(LLMChunk::ReasoningToken(text)) => assert_eq!(text, "Analyzing project"),
            other => panic!("expected reasoning token, got {other:?}"),
        }

        let second = p
            .handle_event(
                "response.reasoning_summary_text.delta",
                r#"{"type":"response.reasoning_summary_text.delta","item_id":"rs_key_2","output_index":0,"summary_index":3,"delta":"Analyzing project structure"}"#,
            )
            .unwrap();
        match second {
            Some(LLMChunk::ReasoningToken(text)) => assert_eq!(text, " structure"),
            other => panic!("expected reasoning suffix token, got {other:?}"),
        }
    }

    #[test]
    fn parser_skips_reasoning_summary_part_done_after_summary_text_done_for_same_stream() {
        let mut p = ResponsesSseParser::new();
        let first = p
            .handle_event(
                "response.reasoning_summary_text.done",
                r#"{"type":"response.reasoning_summary_text.done","item_id":"sum_1","output_index":0,"summary_index":9,"text":"Final summary"}"#,
            )
            .unwrap();
        match first {
            Some(LLMChunk::ReasoningToken(text)) => assert_eq!(text, "Final summary"),
            other => panic!("expected reasoning token, got {other:?}"),
        }

        let second = p
            .handle_event(
                "response.reasoning_summary_part.done",
                r#"{"type":"response.reasoning_summary_part.done","item_id":"sum_2","output_index":0,"summary_index":9,"part":{"type":"summary_text","text":"Final summary"}}"#,
            )
            .unwrap();
        assert!(second.is_none());
    }

    #[test]
    fn parser_inserts_blank_line_before_new_reasoning_summary_stream_delta() {
        let mut p = ResponsesSseParser::new();

        let first = p
            .handle_event(
                "response.reasoning_summary_text.delta",
                r#"{"type":"response.reasoning_summary_text.delta","item_id":"rs_block_1","output_index":0,"summary_index":0,"delta":"I've noticed some strong evidence to analyze."}"#,
            )
            .unwrap();
        match first {
            Some(LLMChunk::ReasoningToken(text)) => {
                assert_eq!(text, "I've noticed some strong evidence to analyze.")
            }
            other => panic!("expected reasoning token, got {other:?}"),
        }

        let second = p
            .handle_event(
                "response.reasoning_summary_text.delta",
                r#"{"type":"response.reasoning_summary_text.delta","item_id":"rs_block_2","output_index":0,"summary_index":1,"delta":"Analyzing dream view functionality"}"#,
            )
            .unwrap();
        match second {
            Some(LLMChunk::ReasoningToken(text)) => {
                assert_eq!(text, "\n\nAnalyzing dream view functionality")
            }
            other => panic!("expected reasoning token with separator, got {other:?}"),
        }
    }

    #[test]
    fn parser_does_not_duplicate_blank_line_when_new_reasoning_stream_already_has_separator() {
        let mut p = ResponsesSseParser::new();

        let _ = p
            .handle_event(
                "response.reasoning_summary_text.delta",
                r#"{"type":"response.reasoning_summary_text.delta","item_id":"rs_block_3","output_index":0,"summary_index":0,"delta":"First block."}"#,
            )
            .unwrap();

        let second = p
            .handle_event(
                "response.reasoning_summary_text.delta",
                r#"{"type":"response.reasoning_summary_text.delta","item_id":"rs_block_4","output_index":0,"summary_index":1,"delta":"\n\nSecond block."}"#,
            )
            .unwrap();
        match second {
            Some(LLMChunk::ReasoningToken(text)) => assert_eq!(text, "\n\nSecond block."),
            other => panic!("expected reasoning token, got {other:?}"),
        }
    }

    #[test]
    fn last_moment_scan_masks_tool_call_arguments_in_a_real_responses_body() {
        // End-to-end: the REAL Responses body builder emits an assistant tool call
        // as a `function_call` item with `arguments`. The last-moment outbound scan
        // must mask the secret inside those arguments (the gap field-by-field masking
        // missed) while leaving call_id / name intact.
        use crate::masking::mask_outbound_body;
        use bamboo_config::keyword_masking::{KeywordEntry, MatchType};
        use bamboo_config::KeywordMaskingConfig;

        let messages = vec![
            Message::user("look up hunter2 please"),
            Message::assistant(
                "calling",
                Some(vec![ToolCall {
                    id: "call_1".to_string(),
                    tool_type: "function".to_string(),
                    function: FunctionCall {
                        name: "search".to_string(),
                        arguments: r#"{"q":"hunter2"}"#.to_string(),
                    },
                }]),
            ),
        ];
        let mut body =
            build_responses_body("gpt-5.4", &messages, &[], None, None, None, None, None);
        let config = KeywordMaskingConfig {
            entries: vec![KeywordEntry {
                pattern: "hunter2".to_string(),
                match_type: MatchType::Exact,
                enabled: true,
            }],
        };
        mask_outbound_body(&mut body, &config);

        let input = body["input"].as_array().expect("input array");
        // The user message text is masked.
        assert_eq!(input[0]["content"], "look up [MASKED] please");
        // The function_call arguments are masked (the previously-missed field)...
        let function_call = input
            .iter()
            .find(|item| item["type"] == "function_call")
            .expect("function_call item");
        assert_eq!(function_call["arguments"], r#"{"q":"[MASKED]"}"#);
        // ...but the correlation id and tool name are preserved.
        assert_eq!(function_call["call_id"], "call_1");
        assert_eq!(function_call["name"], "search");
    }

    #[test]
    fn gpt_5_6_lowers_stable_plan_to_explicit_breakpoint() {
        let mut stable_user = Message::user("stable prefix");
        stable_user.id = "stable-user".to_string();
        let mut assistant = Message::assistant("stable answer", None);
        assistant.id = "stable-assistant".to_string();
        let mut volatile_user = Message::user("changing suffix");
        volatile_user.id = "volatile-user".to_string();
        let messages = vec![stable_user, assistant, volatile_user];
        let plan = PromptCachePlan {
            cache_tools: true,
            cache_system: true,
            // Assistant output blocks cannot carry Responses breakpoints, so
            // lowering must select the nearest preceding input block.
            breakpoint_message_ids: vec!["stable-assistant".to_string()],
            ..Default::default()
        };
        let options = ResponsesRequestOptions {
            prompt_cache_key: Some("tenant:stable".to_string()),
            ..Default::default()
        };

        let body = build_responses_body(
            "gpt-5.6-sol",
            &messages,
            &[],
            None,
            None,
            Some(&options),
            None,
            Some(&plan),
        );

        assert_eq!(body["prompt_cache_key"], "tenant:stable");
        assert_eq!(body["prompt_cache_options"]["mode"], "explicit");
        assert_eq!(
            body["input"][0]["content"][0]["prompt_cache_breakpoint"]["mode"],
            "explicit"
        );
        assert!(body["input"][1]["content"]
            .get("prompt_cache_breakpoint")
            .is_none());
        assert!(body["input"][2]["content"]
            .get("prompt_cache_breakpoint")
            .is_none());
    }

    #[test]
    fn caller_cache_policy_wins_and_legacy_models_are_gated() {
        let mut stable = Message::user("stable");
        stable.id = "stable".to_string();
        let plan = PromptCachePlan {
            breakpoint_message_ids: vec!["stable".to_string()],
            ..Default::default()
        };
        let options = ResponsesRequestOptions {
            prompt_cache_key: Some("cache-key".to_string()),
            prompt_cache_options: Some(json!({"mode": "implicit", "ttl": "30m"})),
            ..Default::default()
        };

        let supported = build_responses_body(
            "gpt-5.6-terra",
            std::slice::from_ref(&stable),
            &[],
            None,
            None,
            Some(&options),
            None,
            Some(&plan),
        );
        assert_eq!(
            supported["prompt_cache_options"],
            json!({"mode": "implicit", "ttl": "30m"})
        );

        let ttl_only_options = ResponsesRequestOptions {
            prompt_cache_options: Some(json!({"ttl": "30m"})),
            ..Default::default()
        };
        let ttl_only = build_responses_body(
            "gpt-5.6-terra",
            std::slice::from_ref(&stable),
            &[],
            None,
            None,
            Some(&ttl_only_options),
            None,
            Some(&plan),
        );
        assert_eq!(
            ttl_only["prompt_cache_options"],
            json!({"mode": "explicit", "ttl": "30m"})
        );

        let legacy = build_responses_body(
            "gpt-5.5",
            &[stable],
            &[],
            None,
            None,
            Some(&options),
            None,
            Some(&plan),
        );
        assert!(legacy.get("prompt_cache_key").is_none());
        assert!(legacy.get("prompt_cache_options").is_none());
        assert!(legacy["input"][0]["content"].is_string());
    }

    #[test]
    fn caller_authored_responses_breakpoints_survive_without_policy_rewrite() {
        let raw_input = json!([{
            "type": "message",
            "role": "user",
            "content": [
                {
                    "type": "input_file",
                    "file_id": "file_123",
                    "prompt_cache_breakpoint": {"mode": "explicit"}
                },
                {
                    "type": "input_text",
                    "text": "volatile question"
                }
            ]
        }]);
        let options = ResponsesRequestOptions {
            prompt_cache_key: Some("tenant:file-prefix".to_string()),
            raw_input_with_cache_breakpoints: Some(raw_input.clone()),
            ..Default::default()
        };

        let body =
            build_responses_body("gpt-5.6", &[], &[], None, None, Some(&options), None, None);
        assert_eq!(body["input"], raw_input);
        assert_eq!(body["prompt_cache_key"], "tenant:file-prefix");
        assert!(
            body.get("prompt_cache_options").is_none(),
            "the caller omitted policy, so OpenAI's implicit default must remain"
        );

        let explicit_options = ResponsesRequestOptions {
            prompt_cache_options: Some(json!({"mode": "explicit", "ttl": "30m"})),
            ..options.clone()
        };
        let explicit = build_responses_body(
            "gpt-5.6",
            &[],
            &[],
            None,
            None,
            Some(&explicit_options),
            None,
            None,
        );
        assert_eq!(
            explicit["prompt_cache_options"],
            json!({"mode": "explicit", "ttl": "30m"})
        );

        let legacy =
            build_responses_body("gpt-5.5", &[], &[], None, None, Some(&options), None, None);
        assert_eq!(legacy["input"], json!([]));
        assert!(legacy.get("prompt_cache_key").is_none());
    }

    #[test]
    fn explicit_breakpoint_keeps_same_stable_prefix_when_tail_changes() {
        let mut stable = Message::user("same stable prefix");
        stable.id = "stable".to_string();
        let mut volatile_a = Message::user("volatile A");
        volatile_a.id = "tail-a".to_string();
        let mut volatile_b = Message::user("volatile B");
        volatile_b.id = "tail-b".to_string();
        let plan = PromptCachePlan {
            breakpoint_message_ids: vec!["stable".to_string()],
            ..Default::default()
        };

        let first = build_responses_body(
            "gpt-5.6",
            &[stable.clone(), volatile_a],
            &[],
            None,
            None,
            None,
            None,
            Some(&plan),
        );
        let second = build_responses_body(
            "gpt-5.6",
            &[stable, volatile_b],
            &[],
            None,
            None,
            None,
            None,
            Some(&plan),
        );

        assert_eq!(first["input"][0], second["input"][0]);
        assert_eq!(
            first["input"][0]["content"][0]["prompt_cache_breakpoint"]["mode"],
            "explicit"
        );
        assert_ne!(first["input"][1], second["input"][1]);
    }

    #[test]
    fn explicit_breakpoint_can_end_after_typed_tool_output() {
        let mut tool_output = Message::tool_result("call_1", "stable tool output");
        tool_output.id = "tool-output".to_string();
        let plan = PromptCachePlan {
            breakpoint_message_ids: vec!["tool-output".to_string()],
            ..Default::default()
        };

        let body = build_responses_body(
            "gpt-5.6",
            &[tool_output],
            &[],
            None,
            None,
            None,
            None,
            Some(&plan),
        );

        assert_eq!(body["input"][0]["type"], "function_call_output");
        assert_eq!(
            body["input"][0]["output"][0]["prompt_cache_breakpoint"]["mode"],
            "explicit"
        );
        assert_eq!(body["input"][0]["output"][0]["text"], "stable tool output");
    }

    #[test]
    fn request_overrides_run_after_generated_cache_defaults() {
        use bamboo_config::{
            BodyPatch, BodyPatchOp, PatchValue, RequestOverridesConfig, RequestScopeOverride,
        };

        let mut stable = Message::user("stable");
        stable.id = "stable".to_string();
        let plan = PromptCachePlan {
            breakpoint_message_ids: vec!["stable".to_string()],
            ..Default::default()
        };
        let options = ResponsesRequestOptions {
            prompt_cache_key: Some("generated".to_string()),
            ..Default::default()
        };
        let mut body = build_responses_body(
            "gpt-5.6",
            &[stable],
            &[],
            None,
            None,
            Some(&options),
            None,
            Some(&plan),
        );
        let overrides = RequestOverridesConfig {
            common: RequestScopeOverride {
                headers: Default::default(),
                body_patch: vec![
                    BodyPatch {
                        path: "prompt_cache_key".to_string(),
                        op: BodyPatchOp::Set,
                        value: Some(PatchValue::Json(json!("operator-key"))),
                    },
                    BodyPatch {
                        path: "prompt_cache_options".to_string(),
                        op: BodyPatchOp::Remove,
                        value: None,
                    },
                ],
            },
            endpoints: Default::default(),
            rules: Vec::new(),
        };

        crate::providers::common::request_overrides::apply_overrides_to_body_with_env(
            &mut body,
            Some(&overrides),
            crate::providers::common::request_overrides::ENDPOINT_RESPONSES,
            Some("gpt-5.6"),
            &HashMap::new(),
        );

        assert_eq!(body["prompt_cache_key"], "operator-key");
        assert!(body.get("prompt_cache_options").is_none());
    }

    #[test]
    fn four_final_responses_bodies_preserve_exact_prefix_through_tool_loop_override_and_masking() {
        use bamboo_config::keyword_masking::{KeywordEntry, MatchType};
        use bamboo_config::{
            BodyPatch, BodyPatchOp, KeywordMaskingConfig, PatchValue, RequestOverridesConfig,
            RequestScopeOverride,
        };

        let options = ResponsesRequestOptions {
            instructions: Some("stable instructions".to_string()),
            prompt_cache_key: Some("generated-cache-key".to_string()),
            prefix_epoch: Some(0),
            ..Default::default()
        };
        let overrides = RequestOverridesConfig {
            common: RequestScopeOverride {
                headers: Default::default(),
                body_patch: vec![BodyPatch {
                    path: "prompt_cache_key".to_string(),
                    op: BodyPatchOp::Set,
                    value: Some(PatchValue::Json(json!("operator-cache-key"))),
                }],
            },
            endpoints: Default::default(),
            rules: Vec::new(),
        };
        let masking = KeywordMaskingConfig {
            entries: vec![KeywordEntry {
                pattern: "wire-secret".to_string(),
                match_type: MatchType::Exact,
                enabled: true,
            }],
        };
        let tool_call = ToolCall {
            id: "call_ledger".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "lookup".to_string(),
                arguments: r#"{"query":"wire-secret"}"#.to_string(),
            },
        };
        let rounds = [
            vec![
                Message::user("initial host context"),
                Message::user("first request with wire-secret"),
            ],
            vec![
                Message::user("initial host context"),
                Message::user("first request with wire-secret"),
                Message::assistant("checking", Some(vec![tool_call.clone()])),
                Message::tool_result("call_ledger", "tool output"),
            ],
            vec![
                Message::user("initial host context"),
                Message::user("first request with wire-secret"),
                Message::assistant("checking", Some(vec![tool_call.clone()])),
                Message::tool_result("call_ledger", "tool output"),
                Message::user("host context revision 2"),
            ],
            vec![
                Message::user("initial host context"),
                Message::user("first request with wire-secret"),
                Message::assistant("checking", Some(vec![tool_call])),
                Message::tool_result("call_ledger", "tool output"),
                Message::user("host context revision 2"),
                Message::user("continue"),
            ],
        ];
        let bodies = rounds
            .iter()
            .map(|messages| {
                let mut body = build_responses_body(
                    "gpt-5.6",
                    messages,
                    &[],
                    None,
                    None,
                    Some(&options),
                    None,
                    // Compatible-proxy / explicit_prompt_cache=false path.
                    None,
                );
                crate::providers::common::request_overrides::apply_overrides_to_body_with_env(
                    &mut body,
                    Some(&overrides),
                    crate::providers::common::request_overrides::ENDPOINT_RESPONSES,
                    Some("gpt-5.6"),
                    &HashMap::new(),
                );
                crate::masking::mask_outbound_body(&mut body, &masking);
                body
            })
            .collect::<Vec<_>>();

        for pair in bodies.windows(2) {
            let old = pair[0]["input"].as_array().unwrap();
            let new = pair[1]["input"].as_array().unwrap();
            assert_eq!(old, &new[..old.len()]);
        }
        let tool_input = bodies[1]["input"].as_array().unwrap();
        assert!(tool_input
            .iter()
            .any(|item| item["type"] == "function_call"));
        assert!(tool_input
            .iter()
            .any(|item| item["type"] == "function_call_output"));
        assert_eq!(bodies[3]["prompt_cache_key"], "operator-cache-key");
        assert!(bodies[3].get("prompt_cache_options").is_none());

        let tracker = ResponsesWirePrefixTracker::default();
        let diagnostics = bodies
            .iter()
            .map(|body| tracker.observe_final_body(body, Some(0), None))
            .collect::<Vec<_>>();
        assert_eq!(
            diagnostics[0].relation,
            ResponsesWirePrefixRelation::Baseline
        );
        assert!(diagnostics[1..]
            .iter()
            .all(|diagnostic| diagnostic.relation == ResponsesWirePrefixRelation::ExactExtension));
        let retry = tracker.observe_final_body(&bodies[3], Some(0), None);
        assert_eq!(retry.relation, ResponsesWirePrefixRelation::Identical);
        let safe = format!("{retry:?}");
        assert!(!safe.contains("wire-secret"));
        assert!(!safe.contains("operator-cache-key"));

        let mut rewritten = bodies[3].clone();
        rewritten["input"][1]["content"] = json!("rewritten old item");
        let rewrite = tracker.observe_final_body(&rewritten, Some(0), None);
        assert_eq!(rewrite.relation, ResponsesWirePrefixRelation::Rewrite);
        assert_eq!(rewrite.first_divergent_item, Some(1));
    }

    #[test]
    fn wire_diagnostic_never_logs_caller_authored_item_type_or_role() {
        let tracker = ResponsesWirePrefixTracker::default();
        let baseline = json!({
            "prompt_cache_key": "family",
            "input": [{"type": "message", "role": "user", "content": "first"}],
        });
        tracker.observe_final_body(&baseline, Some(0), None);

        let rewritten = json!({
            "prompt_cache_key": "family",
            "input": [{
                "type": "wire-secret-item-type",
                "role": "wire-secret-role",
                "content": "rewritten"
            }],
        });
        let diagnostic = tracker.observe_final_body(&rewritten, Some(0), None);

        assert_eq!(diagnostic.relation, ResponsesWirePrefixRelation::Rewrite);
        assert_eq!(diagnostic.first_divergent_item, Some(0));
        assert_eq!(diagnostic.first_divergent_type.as_deref(), Some("unknown"));
        let safe = format!("{diagnostic:?}");
        assert!(!safe.contains("wire-secret-item-type"));
        assert!(!safe.contains("wire-secret-role"));
    }

    #[test]
    fn explicit_cache_model_gate_is_fail_closed() {
        assert!(supports_openai_explicit_prompt_cache("gpt-5.6"));
        assert!(supports_openai_explicit_prompt_cache("openai/gpt-5.6-luna"));
        assert!(supports_openai_explicit_prompt_cache("gpt-6"));
        assert!(!supports_openai_explicit_prompt_cache("gpt-5.5"));
        assert!(!supports_openai_explicit_prompt_cache("chat-latest"));
    }

    #[test]
    fn failure_and_incomplete_events_are_terminal_errors() {
        for (event, payload, detail) in [
            (
                "response.failed",
                r#"{"type":"response.failed","response":{"error":{"message":"quota exhausted"}}}"#,
                "quota exhausted",
            ),
            (
                "response.incomplete",
                r#"{"type":"response.incomplete","response":{"incomplete_details":{"reason":"max_output_tokens"}}}"#,
                "max_output_tokens",
            ),
            (
                "error",
                r#"{"type":"error","error":{"message":"bad gateway"}}"#,
                "bad gateway",
            ),
            (
                "response.cancelled",
                r#"{"type":"response.cancelled","response":{"status":"cancelled","error":{"message":"request cancelled"}}}"#,
                "request cancelled",
            ),
            (
                "",
                r#"{"error":{"message":"untyped upstream error"}}"#,
                "untyped upstream error",
            ),
        ] {
            let mut parser = ResponsesSseParser::new();
            let error = parser
                .handle_event_multi(event, payload)
                .expect_err("terminal event must fail");
            assert!(error.to_string().contains(detail));
            assert!(parser
                .handle_event_multi(
                    "response.completed",
                    r#"{"type":"response.completed","response":{}}"#,
                )
                .expect("post-terminal frame is ignored")
                .is_empty());
        }
    }

    #[test]
    fn duplicate_completed_emits_usage_and_done_once() {
        let mut parser = ResponsesSseParser::new();
        let payload = r#"{"type":"response.completed","response":{"usage":{"input_tokens":8,"output_tokens":2,"total_tokens":10,"input_tokens_details":{"cached_tokens":3,"cache_write_tokens":5},"output_tokens_details":{"reasoning_tokens":1}}}}"#;
        let first = parser
            .handle_event_multi("response.completed", payload)
            .expect("first terminal");
        assert!(matches!(
            first.as_slice(),
            [
                LLMChunk::ProviderUsage {
                    input_tokens: Some(8),
                    output_tokens: Some(2),
                    total_tokens: Some(10),
                    reasoning_tokens: Some(1),
                    cache_read_input_tokens: Some(3),
                    cache_write_input_tokens: Some(5),
                    ..
                },
                LLMChunk::Done
            ]
        ));
        assert!(parser
            .handle_event_multi("response.completed", payload)
            .expect("duplicate terminal is ignored")
            .is_empty());
    }

    #[test]
    fn provider_parser_retains_raw_protocol_event_before_normalized_chunks() {
        let mut parser = ResponsesSseParser::new_with_context("OpenAI", "gpt-5.6", None)
            .with_protocol_events(true);
        let chunks = parser
            .handle_event_multi(
                "response.completed",
                r#"{"type":"response.completed","sequence_number":42,"response":{"id":"resp_raw","output":[{"id":"msg_a","type":"message","content":[{"type":"output_text","text":"a"}]},{"id":"rs_a","type":"reasoning","summary":[{"type":"summary_text","text":"why"}]},{"id":"msg_b","type":"message","content":[{"type":"output_text","text":"b"}]}],"usage":{"input_tokens":4,"output_tokens":2,"total_tokens":6}}}"#,
            )
            .expect("completed event");

        assert!(matches!(
            &chunks[0],
            LLMChunk::ResponsesEvent { event_type, data }
                if event_type == "response.completed"
                    && data["response"]["output"].as_array().is_some_and(|items| items.len() == 3)
                    && data["sequence_number"] == 42
        ));
        assert!(matches!(chunks.last(), Some(LLMChunk::Done)));
    }
}
