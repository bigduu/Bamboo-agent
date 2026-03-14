use crate::agent::llm::api::models::{ChatMessage, Content, ContentPart, ImageUrl, Role};
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
                tool_calls: None,
                tool_call_id: None,
            });
            continue;
        }

        let Some(obj) = item.as_object() else {
            continue;
        };

        let role = obj
            .get("role")
            .and_then(|value| value.as_str())
            .map(role_str_to_openai_role)
            .unwrap_or(Role::User);

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
            tool_calls: None,
            tool_call_id: None,
        });
    }

    Ok(messages)
}
