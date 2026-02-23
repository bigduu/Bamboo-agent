//! Conversion functions between Anthropic and OpenAI-compatible formats.

use super::api_types::*;
use crate::api::models::{
    ChatCompletionRequest, ChatCompletionResponse, ChatMessage, Content, ContentPart, FunctionCall,
    ImageUrl, Role, Tool, ToolCall, ToolChoice, Usage,
};
use serde_json::{json, Value};
use std::collections::HashMap;

/// HTTP status code type (avoiding actix-web dependency)
pub type HttpStatusCode = u16;

/// Error type for Anthropic conversion operations
pub struct AnthropicConversionError {
    pub status: HttpStatusCode,
    pub error_type: String,
    pub message: String,
}

impl AnthropicConversionError {
    pub fn new(status: HttpStatusCode, error_type: &str, message: String) -> Self {
        Self {
            status,
            error_type: error_type.to_string(),
            message,
        }
    }

    pub fn bad_request(error_type: &str, message: String) -> Self {
        Self::new(400, error_type, message)
    }

    pub fn bad_gateway(error_type: &str, message: String) -> Self {
        Self::new(502, error_type, message)
    }
}

/// Convert Anthropic Messages request to OpenAI-compatible request
pub fn convert_messages_request(
    request: AnthropicMessagesRequest,
) -> Result<ChatCompletionRequest, AnthropicConversionError> {
    let mut out_messages = Vec::new();

    if let Some(system) = request.system {
        let system_text = match system {
            AnthropicSystem::Text(text) => text,
            AnthropicSystem::Blocks(blocks) => blocks
                .into_iter()
                .map(|block| match block {
                    AnthropicSystemBlock::Text { text } => text,
                })
                .collect::<Vec<_>>()
                .join("\n"),
        };

        if !system_text.is_empty() {
            out_messages.push(ChatMessage {
                role: Role::System,
                content: Content::Text(system_text),
                tool_calls: None,
                tool_call_id: None,
            });
        }
    }

    for message in request.messages {
        let role = match message.role {
            AnthropicRole::User => Role::User,
            AnthropicRole::Assistant => Role::Assistant,
            AnthropicRole::System => Role::System,
        };

        match message.content {
            AnthropicContent::Text(text) => {
                out_messages.push(ChatMessage {
                    role,
                    content: Content::Text(text),
                    tool_calls: None,
                    tool_call_id: None,
                });
            }
            AnthropicContent::Blocks(blocks) => match role {
                Role::User => {
                    append_user_blocks(&mut out_messages, blocks)?;
                }
                Role::Assistant => {
                    out_messages.push(convert_assistant_blocks(blocks)?);
                }
                Role::System => {
                    let system_text = blocks
                        .into_iter()
                        .map(|block| match block {
                            AnthropicContentBlock::Text { text } => Ok(text),
                            _ => Err(AnthropicConversionError::new(
                                400,
                                "invalid_request_error",
                                "System messages only support text blocks".to_string(),
                            )),
                        })
                        .collect::<Result<Vec<_>, _>>()?
                        .join("\n");
                    out_messages.push(ChatMessage {
                        role: Role::System,
                        content: Content::Text(system_text),
                        tool_calls: None,
                        tool_call_id: None,
                    });
                }
                Role::Tool => {}
            },
        }
    }

    let mut parameters = request.extra;

    if let Some(max_tokens) = request.max_tokens {
        parameters.insert("max_tokens".to_string(), json!(max_tokens));
    }
    if let Some(temperature) = request.temperature {
        parameters.insert("temperature".to_string(), json!(temperature));
    }
    if let Some(top_p) = request.top_p {
        parameters.insert("top_p".to_string(), json!(top_p));
    }
    if let Some(top_k) = request.top_k {
        parameters.insert("top_k".to_string(), json!(top_k));
    }
    if let Some(stop_sequences) = request.stop_sequences {
        parameters.insert("stop".to_string(), json!(stop_sequences));
    }

    apply_reasoning_mapping(&mut parameters);

    let tools = request.tools.map(|tools| tools
                .into_iter()
                .map(|tool| Tool {
                    tool_type: "function".to_string(),
                    function: crate::api::models::FunctionDefinition {
                        name: tool.name,
                        description: tool.description,
                        parameters: tool.input_schema,
                    },
                })
                .collect());

    let tool_choice = match request.tool_choice {
        Some(choice) => Some(map_tool_choice(choice)?),
        None => None,
    };

    Ok(ChatCompletionRequest {
        model: request.model,
        messages: out_messages,
        tools,
        tool_choice,
        stream: request.stream,
        stream_options: None,
        parameters,
    })
}

/// Apply reasoning effort mapping from Anthropic to OpenAI format
pub fn apply_reasoning_mapping(parameters: &mut HashMap<String, Value>) {
    let reasoning = match parameters.remove("reasoning") {
        Some(value) => value,
        None => return,
    };

    if parameters.contains_key("reasoning_effort") {
        return;
    }

    let value = match reasoning {
        Value::String(value) => value,
        other => {
            parameters.insert("reasoning".to_string(), other);
            return;
        }
    };

    let normalized = value.trim().to_ascii_lowercase();
    let mapped = match normalized.as_str() {
        "low" => Some("low"),
        "mid" | "medium" => Some("medium"),
        "high" => Some("high"),
        _ => None,
    };

    match mapped {
        Some(effort) => {
            parameters.insert(
                "reasoning_effort".to_string(),
                Value::String(effort.to_string()),
            );
        }
        None => {
            parameters.insert("reasoning".to_string(), Value::String(value));
        }
    }
}

fn append_user_blocks(
    out_messages: &mut Vec<ChatMessage>,
    blocks: Vec<AnthropicContentBlock>,
) -> Result<(), AnthropicConversionError> {
    let mut text_parts = Vec::new();

    for block in blocks {
        match block {
            AnthropicContentBlock::Text { text } => {
                text_parts.push(ContentPart::Text { text });
            }
            AnthropicContentBlock::ToolResult {
                tool_use_id,
                content,
            } => {
                if !text_parts.is_empty() {
                    out_messages.push(ChatMessage {
                        role: Role::User,
                        content: Content::Parts(text_parts),
                        tool_calls: None,
                        tool_call_id: None,
                    });
                    text_parts = Vec::new();
                }

                let result_text = extract_tool_result_text(content)?;
                out_messages.push(ChatMessage {
                    role: Role::Tool,
                    content: Content::Text(result_text),
                    tool_calls: None,
                    tool_call_id: Some(tool_use_id),
                });
            }
            AnthropicContentBlock::ToolUse { .. } => {
                return Err(AnthropicConversionError::bad_request(
                    "invalid_request_error",
                    "tool_use blocks are not valid in user messages".to_string(),
                ));
            }
        }
    }

    if !text_parts.is_empty() {
        let content = if text_parts.len() == 1 {
            match text_parts.pop() {
                Some(ContentPart::Text { text }) => Content::Text(text),
                Some(ContentPart::ImageUrl { image_url }) => {
                    Content::Parts(vec![ContentPart::ImageUrl { image_url }])
                }
                None => Content::Text(String::new()),
            }
        } else {
            Content::Parts(text_parts)
        };

        out_messages.push(ChatMessage {
            role: Role::User,
            content,
            tool_calls: None,
            tool_call_id: None,
        });
    }

    Ok(())
}

fn convert_assistant_blocks(
    blocks: Vec<AnthropicContentBlock>,
) -> Result<ChatMessage, AnthropicConversionError> {
    let mut tool_calls = Vec::new();
    let mut content_parts = Vec::new();

    for block in blocks {
        match block {
            AnthropicContentBlock::Text { text } => {
                content_parts.push(ContentPart::Text { text });
            }
            AnthropicContentBlock::ToolUse { id, name, input } => {
                tool_calls.push(ToolCall {
                    id,
                    tool_type: "function".to_string(),
                    function: FunctionCall {
                        name,
                        arguments: serde_json::to_string(&input).unwrap_or_default(),
                    },
                });
            }
            AnthropicContentBlock::ToolResult { .. } => {
                return Err(AnthropicConversionError::new(
                    400,
                    "invalid_request_error",
                    "tool_result blocks are not valid in assistant messages".to_string(),
                ));
            }
        }
    }

    let content = if content_parts.is_empty() && !tool_calls.is_empty() {
        Content::Text(String::new())
    } else if content_parts.len() == 1 {
        match content_parts.into_iter().next() {
            Some(ContentPart::Text { text }) => Content::Text(text),
            Some(ContentPart::ImageUrl { image_url }) => {
                Content::Parts(vec![ContentPart::ImageUrl { image_url }])
            }
            None => Content::Text(String::new()),
        }
    } else {
        Content::Parts(content_parts)
    };

    Ok(ChatMessage {
        role: Role::Assistant,
        content,
        tool_calls: if tool_calls.is_empty() {
            None
        } else {
            Some(tool_calls)
        },
        tool_call_id: None,
    })
}

fn extract_tool_result_text(content: Value) -> Result<String, AnthropicConversionError> {
    match content {
        Value::String(text) => Ok(text),
        Value::Array(items) => {
            let mut texts = Vec::new();
            for item in items {
                let obj = item.as_object().ok_or_else(|| {
                    AnthropicConversionError::new(
                        400,
                        "invalid_request_error",
                        "tool_result content blocks must be objects".to_string(),
                    )
                })?;

                let block_type = obj
                    .get("type")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| {
                        AnthropicConversionError::new(
                            400,
                            "invalid_request_error",
                            "tool_result content blocks missing type".to_string(),
                        )
                    })?;

                if block_type != "text" {
                    return Err(AnthropicConversionError::new(
                        400,
                        "invalid_request_error",
                        "tool_result content blocks must be text".to_string(),
                    ));
                }

                let text = obj
                    .get("text")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| {
                        AnthropicConversionError::new(
                            400,
                            "invalid_request_error",
                            "tool_result content blocks missing text".to_string(),
                        )
                    })?;

                texts.push(text.to_string());
            }

            Ok(texts.join("\n"))
        }
        _ => Err(AnthropicConversionError::new(
            400,
            "invalid_request_error",
            "tool_result content must be a string or array".to_string(),
        )),
    }
}

fn map_tool_choice(choice: AnthropicToolChoice) -> Result<ToolChoice, AnthropicConversionError> {
    match choice {
        AnthropicToolChoice::String(value) => Ok(ToolChoice::String(match value.as_str() {
            "auto" => "auto".to_string(),
            "any" => "auto".to_string(),
            "none" => "none".to_string(),
            _ => {
                return Err(AnthropicConversionError::new(
                    400,
                    "invalid_request_error",
                    format!("Unsupported tool_choice value: {}", value),
                ))
            }
        })),
        AnthropicToolChoice::Tool { tool_type, name } => {
            if tool_type != "tool" {
                return Err(AnthropicConversionError::new(
                    400,
                    "invalid_request_error",
                    format!("Unsupported tool_choice type: {}", tool_type),
                ));
            }
            Ok(ToolChoice::Object {
                tool_type: "function".to_string(),
                function: crate::api::models::FunctionChoice { name },
            })
        }
    }
}

/// Convert OpenAI-compatible response to Anthropic Messages response
pub fn convert_messages_response(
    response: ChatCompletionResponse,
    response_model: &str,
) -> Result<AnthropicMessagesResponse, AnthropicConversionError> {
    let choice = response.choices.into_iter().next().ok_or_else(|| {
        AnthropicConversionError::new(
            502,
            "api_error",
            "Upstream response missing choices".to_string(),
        )
    })?;

    let mut content_blocks = Vec::new();

    match choice.message.content {
        Content::Text(text) => {
            if !text.is_empty() {
                content_blocks.push(AnthropicResponseContentBlock::Text { text });
            }
        }
        Content::Parts(parts) => {
            for part in parts {
                match part {
                    ContentPart::Text { text } => {
                        content_blocks.push(AnthropicResponseContentBlock::Text { text });
                    }
                    ContentPart::ImageUrl {
                        image_url: ImageUrl { .. },
                    } => {
                        return Err(AnthropicConversionError::new(
                            502,
                            "api_error",
                            "Image content is not supported for Anthropic responses".to_string(),
                        ));
                    }
                }
            }
        }
    }

    if let Some(tool_calls) = choice.message.tool_calls {
        for tool_call in tool_calls {
            let input = serde_json::from_str(&tool_call.function.arguments)
                .unwrap_or(Value::String(tool_call.function.arguments));
            content_blocks.push(AnthropicResponseContentBlock::ToolUse {
                id: tool_call.id,
                name: tool_call.function.name,
                input,
            });
        }
    }

    let usage = response.usage.unwrap_or(Usage {
        prompt_tokens: 0,
        completion_tokens: 0,
        total_tokens: 0,
    });

    let model = if response_model.is_empty() {
        response.model.unwrap_or_default()
    } else {
        response_model.to_string()
    };

    Ok(AnthropicMessagesResponse {
        id: response.id,
        response_type: "message".to_string(),
        role: "assistant".to_string(),
        content: content_blocks,
        model,
        stop_reason: map_stop_reason(choice.finish_reason.as_deref()),
        stop_sequence: None,
        usage: AnthropicUsage {
            input_tokens: usage.prompt_tokens,
            output_tokens: usage.completion_tokens,
        },
    })
}

/// Convert Anthropic Complete request to OpenAI-compatible request
pub fn convert_complete_request(
    request: AnthropicCompleteRequest,
) -> Result<ChatCompletionRequest, AnthropicConversionError> {
    let mut parameters = request.extra;
    parameters.insert(
        "max_tokens".to_string(),
        json!(request.max_tokens_to_sample),
    );

    if let Some(stop_sequences) = request.stop_sequences {
        parameters.insert("stop".to_string(), json!(stop_sequences));
    }
    if let Some(temperature) = request.temperature {
        parameters.insert("temperature".to_string(), json!(temperature));
    }
    if let Some(top_p) = request.top_p {
        parameters.insert("top_p".to_string(), json!(top_p));
    }
    if let Some(top_k) = request.top_k {
        parameters.insert("top_k".to_string(), json!(top_k));
    }

    apply_reasoning_mapping(&mut parameters);

    Ok(ChatCompletionRequest {
        model: request.model,
        messages: vec![ChatMessage {
            role: Role::User,
            content: Content::Text(request.prompt),
            tool_calls: None,
            tool_call_id: None,
        }],
        tools: None,
        tool_choice: None,
        stream: request.stream,
        stream_options: None,
        parameters,
    })
}

/// Convert OpenAI-compatible response to Anthropic Complete response
pub fn convert_complete_response(
    response: ChatCompletionResponse,
    response_model: &str,
) -> Result<AnthropicCompleteResponse, AnthropicConversionError> {
    let choice = response.choices.into_iter().next().ok_or_else(|| {
        AnthropicConversionError::new(
            502,
            "api_error",
            "Upstream response missing choices".to_string(),
        )
    })?;

    let completion = match choice.message.content {
        Content::Text(text) => text,
        Content::Parts(parts) => parts
            .into_iter()
            .filter_map(|part| match part {
                ContentPart::Text { text } => Some(text),
                ContentPart::ImageUrl { .. } => None,
            })
            .collect::<Vec<_>>()
            .join(""),
    };

    let model = if response_model.is_empty() {
        response.model.unwrap_or_default()
    } else {
        response_model.to_string()
    };

    Ok(AnthropicCompleteResponse {
        response_type: "completion".to_string(),
        completion,
        model,
        stop_reason: map_stop_reason_complete(choice.finish_reason.as_deref()),
    })
}

fn map_stop_reason(reason: Option<&str>) -> String {
    match reason {
        Some("stop") => "end_turn".to_string(),
        Some("length") => "max_tokens".to_string(),
        Some("tool_calls") => "tool_use".to_string(),
        Some(value) => value.to_string(),
        None => "end_turn".to_string(),
    }
}

fn map_stop_reason_complete(reason: Option<&str>) -> String {
    match reason {
        Some("length") => "max_tokens".to_string(),
        Some("stop") => "stop_sequence".to_string(),
        Some(value) => value.to_string(),
        None => "stop_sequence".to_string(),
    }
}

/// Format a model ID into a human-readable display name
pub fn format_model_display_name(model_id: &str) -> String {
    if model_id.starts_with("claude") {
        model_id
            .replace("claude-3-5-", "Claude 3.5 ")
            .replace("claude-3-", "Claude 3 ")
            .replace("-sonnet", " Sonnet")
            .replace("-haiku", " Haiku")
            .replace("-opus", " Opus")
            .replace("-latest", " (Latest)")
    } else if model_id.starts_with("gpt") {
        model_id
            .replace("gpt-4o", "GPT-4o")
            .replace("gpt-4", "GPT-4")
            .replace("gpt-3.5", "GPT-3.5")
    } else {
        model_id.to_string()
    }
}
