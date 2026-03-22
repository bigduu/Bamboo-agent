use crate::agent::llm::api::models::{
    ChatMessage, Content, ContentPart, FunctionCall, ImageUrl, Role, ToolCall,
};
use crate::server::error::AppError;

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
            let output = obj
                .get("output")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            messages.push(ChatMessage {
                role: Role::Tool,
                content: Content::Text(output),
                phase: None,
                tool_calls: None,
                tool_call_id: Some(call_id),
            });
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
                    "input_text" | "text" => {
                        if let Some(text) = part_obj.get("text").and_then(|value| value.as_str()) {
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
