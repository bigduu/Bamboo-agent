use bamboo_agent_core::tools::ToolCall;

use super::super::types::{
    ResponsesCreateResponse, ResponsesFunctionCallOutputItem, ResponsesMessageOutputItem,
    ResponsesOutputItem, ResponsesTextContent, ResponsesUsage,
};

pub(super) fn build_output_items(
    message_id: &str,
    content: String,
    tool_calls: Vec<ToolCall>,
) -> Vec<ResponsesOutputItem> {
    let mut output: Vec<ResponsesOutputItem> = Vec::new();
    // A tool-only response must not grow a synthetic empty message item. Keep
    // one empty assistant message only for a truly empty no-tool completion so
    // legacy clients still receive a representable response.
    if !content.is_empty() || tool_calls.is_empty() {
        output.push(ResponsesOutputItem::Message(ResponsesMessageOutputItem {
            id: message_id.to_string(),
            item_type: "message".to_string(),
            role: "assistant".to_string(),
            content: vec![ResponsesTextContent {
                content_type: "output_text".to_string(),
                text: content,
                annotations: Vec::new(),
            }],
            status: Some("completed".to_string()),
        }));
    }

    for (idx, tool_call) in tool_calls.into_iter().enumerate() {
        output.push(ResponsesOutputItem::FunctionCall(
            ResponsesFunctionCallOutputItem {
                id: format!("fc_{}_{}", message_id, idx),
                item_type: "function_call".to_string(),
                call_id: tool_call.id,
                name: tool_call.function.name,
                arguments: tool_call.function.arguments,
                status: Some("completed".to_string()),
            },
        ));
    }

    output
}

pub(super) fn build_completed_response(
    response_id: String,
    created_at: u64,
    model: String,
    output: Vec<ResponsesOutputItem>,
    usage: Option<ResponsesUsage>,
) -> ResponsesCreateResponse {
    ResponsesCreateResponse {
        id: response_id,
        object: "response".to_string(),
        created_at,
        model,
        status: "completed".to_string(),
        output,
        usage,
    }
}
