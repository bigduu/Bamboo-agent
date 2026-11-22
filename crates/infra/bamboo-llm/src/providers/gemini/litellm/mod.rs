pub(super) mod replay;
mod wire;

use super::super::common::tool_schema::canonicalize_json_value;
use super::{reasoning_control_for_model, GeminiReasoningControl};
use crate::protocol::gemini::{normalize_function_response, parse_data_url_base64};
use crate::protocol::ProtocolError;
use crate::provider::{optional_protocol_output_limit, LLMError, Result};
use bamboo_domain::{Message, MessagePart, ReasoningEffort, Role, ToolSchema};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{Map, Value};
use std::collections::HashMap;
use wire::{
    BlobType, ContentType, FileDataType, FunctionCall, FunctionDeclaration, FunctionResponse,
    FunctionResponsePartType, GeminiThinkingConfig, GenerationConfig, PartType, RequestBody,
    SystemInstructions, Tools,
};

pub(super) fn build_request(
    messages: &[Message],
    tools: &[ToolSchema],
    max_output_tokens: Option<u32>,
    model: &str,
    reasoning_effort: Option<ReasoningEffort>,
    options: super::LiteLlmRequestOptions<'_>,
) -> Result<Value> {
    let replay = options.replay;
    let model_name = model
        .rsplit('/')
        .next()
        .unwrap_or(model)
        .to_ascii_lowercase();
    let reasoning_control = reasoning_control_for_model(&model_name);
    let gemini3_or_newer = reasoning_control == GeminiReasoningControl::Level;
    let mut system_texts = Vec::new();
    let mut contents = Vec::<ContentType>::new();
    let mut tool_calls = HashMap::new();
    let mut responses = Vec::new();
    let mut native_parts = Vec::new();

    for message in messages {
        if message.role != Role::Tool && !responses.is_empty() {
            contents.push(take_tool_responses(&mut responses));
        }

        match message.role {
            Role::System => {
                let text = message.content.trim();
                if !text.is_empty() {
                    system_texts.push(text);
                }
            }
            Role::User => contents.push(content("user", message_parts(message))),
            Role::Assistant => {
                let calls = message.tool_calls.as_deref().unwrap_or_default();
                if let Some(native) = replay.as_ref().and_then(|replay| replay.turn(message)) {
                    for (index, (call, wire_id)) in calls.iter().zip(native.call_ids).enumerate() {
                        tool_calls.insert(
                            call.id.as_str(),
                            (index, call.function.name.as_str(), wire_id),
                        );
                    }
                    native_parts.push((contents.len(), native.parts));
                    contents.push(content("model", Vec::new()));
                    continue;
                }
                let mut parts = message_parts(message);
                for (index, call) in calls.iter().enumerate() {
                    let wire_id = gemini3_or_newer.then_some(call.id.as_str());
                    tool_calls.insert(
                        call.id.as_str(),
                        (index, call.function.name.as_str(), wire_id),
                    );
                    let args = serde_json::from_str::<Map<String, Value>>(&call.function.arguments)
                        .unwrap_or_default();
                    parts.push(PartType {
                        function_call: Some(FunctionCall {
                            id: wire_id.map(str::to_owned),
                            name: call.function.name.clone(),
                            args: Some(Value::Object(args)),
                        }),
                        // Normalized Bamboo history carries no Gemini part signatures.
                        // LiteLLM's missing-signature fallback applies to the first call.
                        thought_signature: (gemini3_or_newer && index == 0)
                            .then(|| STANDARD.encode("skip_thought_signature_validator")),
                        ..Default::default()
                    });
                }
                contents.push(content("model", parts));
            }
            Role::Tool => {
                let call_id = message
                    .tool_call_id
                    .as_deref()
                    .ok_or_else(|| ProtocolError::MissingField("tool_call_id".into()))?;
                // Older callers can submit a result whose tool_call_id is
                // already the function name, without a preceding call.
                let (index, name, wire_id) =
                    tool_calls
                        .get(call_id)
                        .copied()
                        .unwrap_or((usize::MAX, call_id, None));
                responses.push((
                    index,
                    tool_result_parts(message, wire_id, name, gemini3_or_newer),
                ));
            }
        }
    }
    if !responses.is_empty() {
        contents.push(take_tool_responses(&mut responses));
    }

    let thinking_config = match reasoning_effort
        .map(|effort| thinking_config(&model_name, reasoning_control, effort))
        .transpose()
    {
        Err(_) if options.reasoning_overridden => None,
        result => result?,
    };
    let max_output_tokens = optional_protocol_output_limit(max_output_tokens).map(i64::from);
    let mut body = serde_json::to_value(RequestBody {
        contents,
        system_instruction: (!system_texts.is_empty()).then(|| SystemInstructions {
            parts: vec![text_part(system_texts.join("\n\n"))],
        }),
        tools: (!tools.is_empty()).then(|| {
            vec![Tools {
                function_declarations: Some(
                    tools
                        .iter()
                        .map(|tool| FunctionDeclaration {
                            name: tool.function.name.clone(),
                            description: Some(tool.function.description.clone()),
                            parameters_json_schema: Some(canonicalize_json_value(
                                &tool.function.parameters,
                            )),
                        })
                        .collect(),
                ),
            }]
        }),
        generation_config: (max_output_tokens.is_some() || thinking_config.is_some()).then_some(
            GenerationConfig {
                max_output_tokens,
                thinking_config,
            },
        ),
    })?;
    for (index, parts) in native_parts {
        // A signature binds to the provider's exact part and position. Keep
        // native replay as JSON instead of projecting it through request types.
        body["contents"][index]["parts"] = parts.clone();
    }
    Ok(body)
}

fn text_part(text: String) -> PartType {
    PartType {
        text: Some(text),
        ..Default::default()
    }
}

fn content(role: &str, mut parts: Vec<PartType>) -> ContentType {
    if parts.is_empty() {
        parts.push(text_part(String::new()));
    }
    ContentType {
        role: Some(role.into()),
        parts,
    }
}

fn message_parts(message: &Message) -> Vec<PartType> {
    let mut parts: Vec<_> = message
        .content_parts
        .iter()
        .flatten()
        .filter_map(|part| match part {
            MessagePart::Text { text } => Some(text_part(text.clone())),
            MessagePart::ImageUrl { image_url } => image_part(&image_url.url),
        })
        .collect();
    if parts.is_empty() && !message.content.is_empty() {
        parts.push(text_part(message.content.clone()));
    }
    parts
}

fn image_part(url: &str) -> Option<PartType> {
    let url = url.trim();
    if url.is_empty() {
        return None;
    }
    if let Some((mime_type, data)) = parse_data_url_base64(url) {
        return Some(PartType {
            inline_data: Some(BlobType { mime_type, data }),
            ..Default::default()
        });
    }
    Some(PartType {
        file_data: Some(FileDataType {
            mime_type: None,
            file_uri: url.to_owned(),
        }),
        ..Default::default()
    })
}

fn take_tool_responses(responses: &mut Vec<(usize, Vec<PartType>)>) -> ContentType {
    // Gemini matches parallel results in call order, including when IDs are absent.
    responses.sort_by_key(|(index, _)| *index);
    content(
        "user",
        responses.drain(..).flat_map(|(_, parts)| parts).collect(),
    )
}

fn tool_result_parts(
    message: &Message,
    wire_id: Option<&str>,
    name: &str,
    gemini3_or_newer: bool,
) -> Vec<PartType> {
    let mut images = message
        .content_parts
        .iter()
        .flatten()
        .filter_map(|part| match part {
            MessagePart::ImageUrl { image_url } => image_part(&image_url.url),
            MessagePart::Text { .. } => None,
        });
    let mut response = FunctionResponse {
        id: wire_id.map(str::to_owned),
        name: name.to_owned(),
        response: Some(normalize_function_response(&message.content)),
        parts: None,
    };
    if gemini3_or_newer {
        let parts: Vec<_> = images
            .by_ref()
            .map(|image| FunctionResponsePartType {
                inline_data: image.inline_data,
                file_data: image.file_data,
            })
            .collect();
        response.parts = (!parts.is_empty()).then_some(parts);
    }
    let primary = PartType {
        function_response: Some(response),
        ..Default::default()
    };
    std::iter::once(primary).chain(images).collect()
}

// LiteLLM's VertexGeminiConfig maps OpenAI-style effort to a thinking level or
// the Gemini 2.5 budget defaults. These are the pinned Google AI Studio rules.
fn thinking_config(
    model: &str,
    control: GeminiReasoningControl,
    effort: ReasoningEffort,
) -> Result<GeminiThinkingConfig> {
    let unsupported = || {
        LLMError::Api(format!(
            "Gemini does not support reasoning effort {} for {model}",
            effort.as_str()
        ))
    };
    let mut config = GeminiThinkingConfig {
        include_thoughts: Some(effort != ReasoningEffort::Disabled),
        ..Default::default()
    };
    match control {
        GeminiReasoningControl::Level => {
            let level = match effort {
                ReasoningEffort::Disabled => {
                    if model.contains("flash") && !wire::MODELS_WITHOUT_MINIMAL.contains(&model) {
                        "minimal"
                    } else {
                        "low"
                    }
                }
                ReasoningEffort::Low | ReasoningEffort::Medium | ReasoningEffort::High => {
                    effort.as_str()
                }
                ReasoningEffort::Xhigh if wire::MODELS_WITH_XHIGH.contains(&model) => "xhigh",
                ReasoningEffort::Xhigh | ReasoningEffort::Max => return Err(unsupported()),
            };
            config.thinking_level = Some(level.into());
        }
        GeminiReasoningControl::Budget => {
            config.thinking_budget = Some(match effort {
                ReasoningEffort::Disabled if !model.starts_with("gemini-2.5-pro") => 0,
                ReasoningEffort::Low => wire::LOW_THINKING_BUDGET,
                ReasoningEffort::Medium => wire::MEDIUM_THINKING_BUDGET,
                ReasoningEffort::High => wire::HIGH_THINKING_BUDGET,
                _ => return Err(unsupported()),
            });
        }
        GeminiReasoningControl::Unsupported => return Err(unsupported()),
        GeminiReasoningControl::Unknown => {
            // A gateway alias carries no known Gemini capabilities.
            config.thinking_level = Some(effort.as_str().to_ascii_uppercase());
            config.include_thoughts = None;
        }
    }
    Ok(config)
}

#[cfg(test)]
mod tests;
