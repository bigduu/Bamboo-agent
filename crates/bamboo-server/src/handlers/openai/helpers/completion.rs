use bamboo_infrastructure_llm::api::models::{
    ChatCompletionResponse, ChatMessage, Content, ResponseChoice, Role, ToolCall, Usage,
};

pub(super) fn build_completion_response(
    content: String,
    tool_calls: Option<Vec<ToolCall>>,
    model: &str,
) -> ChatCompletionResponse {
    ChatCompletionResponse {
        id: format!("chatcmpl-{}", uuid::Uuid::new_v4()),
        object: Some("chat.completion".to_string()),
        created: Some(chrono::Utc::now().timestamp() as u64),
        model: Some(model.to_string()),
        choices: vec![ResponseChoice {
            index: 0,
            message: ChatMessage {
                role: Role::Assistant,
                content: Content::Text(content),
                phase: None,
                tool_calls,
                tool_call_id: None,
            },
            finish_reason: Some("stop".to_string()),
        }],
        usage: Some(Usage {
            prompt_tokens: 0,
            completion_tokens: 0,
            total_tokens: 0,
        }),
        system_fingerprint: None,
    }
}
