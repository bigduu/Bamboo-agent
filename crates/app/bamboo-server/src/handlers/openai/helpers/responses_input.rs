use crate::error::AppError;
use bamboo_llm::api::models::{
    ChatMessage, Content, ContentPart, FunctionCall, ImageUrl, Role, ToolCall,
};

pub(super) fn role_str_to_openai_role(role: &str) -> Role {
    match role {
        "system" => Role::System,
        "assistant" => Role::Assistant,
        "tool" => Role::Tool,
        _ => Role::User,
    }
}

pub(super) fn responses_input_to_chat_messages(
    input: serde_json::Value,
) -> Result<Vec<ChatMessage>, AppError> {
    if input.is_null() {
        return Ok(vec![]);
    }

    if let Some(text) = input.as_str() {
        return Ok(vec![ChatMessage {
            role: Role::User,
            content: Content::Text(text.to_string()),
            phase: None,
            tool_calls: None,
            tool_call_id: None,
        }]);
    }

    let Some(items) = input.as_array() else {
        return Err(AppError::BadRequest(
            "Invalid `input`: expected string or array".to_string(),
        ));
    };

    let mut messages = Vec::new();

    for item in items {
        if let Some(text) = item.as_str() {
            messages.push(ChatMessage {
                role: Role::User,
                content: Content::Text(text.to_string()),
                phase: None,
                tool_calls: None,
                tool_call_id: None,
            });
            continue;
        }

        let Some(obj) = item.as_object() else {
            continue;
        };

        // Check `type` field first for function_call / function_call_output items
        let item_type = obj
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("message");

        if item_type == "function_call" {
            let call_id = obj
                .get("call_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let name = obj
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let arguments = obj
                .get("arguments")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            messages.push(ChatMessage {
                role: Role::Assistant,
                content: Content::Text(String::new()),
                phase: Some("commentary".to_string()),
                tool_calls: Some(vec![ToolCall {
                    id: call_id,
                    tool_type: "function".to_string(),
                    function: FunctionCall { name, arguments },
                }]),
                tool_call_id: None,
            });
            continue;
        }

        if item_type == "function_call_output" {
            let call_id = obj
                .get("call_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            // `output` is a string OR an array of content parts
            // (`[{type:"output_text", text}]`). Flatten parts to text instead
            // of silently degrading non-string shapes to "" (#525).
            let output = match obj.get("output") {
                None | Some(serde_json::Value::Null) => String::new(),
                Some(serde_json::Value::String(text)) => text.clone(),
                Some(serde_json::Value::Array(parts)) => parts
                    .iter()
                    .filter_map(|part| part.as_object())
                    .filter_map(|part| part.get("text").and_then(|t| t.as_str()))
                    .collect::<Vec<_>>()
                    .join(""),
                // Any other shape: keep the raw JSON rather than dropping it.
                Some(other) => other.to_string(),
            };

            messages.push(ChatMessage {
                role: Role::Tool,
                content: Content::Text(output),
                phase: None,
                tool_calls: None,
                tool_call_id: Some(call_id),
            });
            continue;
        }

        // An item with an explicitly unknown `type` (Codex round-trips
        // `reasoning` items, the spec has `web_search_call`, …) must be
        // skipped: falling into the message branch fabricates an empty user
        // message that pollutes the conversation (#525). Items WITHOUT a
        // `type` key defaulted to "message" above and keep flowing.
        if item_type != "message" {
            tracing::debug!(item_type, "Skipping unsupported Responses input item type");
            continue;
        }

        // Default: treat as message item
        let role = obj
            .get("role")
            .and_then(|value| value.as_str())
            .map(role_str_to_openai_role)
            .unwrap_or(Role::User);
        let phase = if matches!(role, Role::Assistant) {
            obj.get("phase")
                .and_then(|value| value.as_str())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string)
        } else {
            None
        };

        let content_value = obj
            .get("content")
            .cloned()
            .unwrap_or(serde_json::Value::Null);

        let content = if let Some(text) = content_value.as_str() {
            Content::Text(text.to_string())
        } else if let Some(parts) = content_value.as_array() {
            let mut content_parts = Vec::new();

            for part in parts {
                let Some(part_obj) = part.as_object() else {
                    continue;
                };
                let part_type = part_obj
                    .get("type")
                    .and_then(|value| value.as_str())
                    .unwrap_or("");

                match part_type {
                    "input_text" | "output_text" | "text" => {
                        if let Some(text) = part_obj.get("text").and_then(|value| value.as_str()) {
                            content_parts.push(ContentPart::Text {
                                text: text.to_string(),
                            });
                        }
                    }
                    "refusal" => {
                        if let Some(text) = part_obj
                            .get("refusal")
                            .or_else(|| part_obj.get("text"))
                            .and_then(|value| value.as_str())
                        {
                            content_parts.push(ContentPart::Text {
                                text: text.to_string(),
                            });
                        }
                    }
                    "input_image" | "image_url" => {
                        if let Some(url) = part_obj
                            .get("image_url")
                            .and_then(|value| value.get("url").or(Some(value)))
                            .and_then(|value| value.as_str())
                        {
                            content_parts.push(ContentPart::ImageUrl {
                                image_url: ImageUrl {
                                    url: url.to_string(),
                                    detail: None,
                                },
                            });
                        }
                    }
                    _ => {}
                }
            }

            if content_parts.is_empty() {
                Content::Text(String::new())
            } else {
                Content::Parts(content_parts)
            }
        } else {
            Content::Text(String::new())
        };

        messages.push(ChatMessage {
            role,
            content,
            phase,
            tool_calls: None,
            tool_call_id: None,
        });
    }

    Ok(messages)
}
