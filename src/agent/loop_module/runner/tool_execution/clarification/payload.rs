use crate::agent::core::tools::{ToolCall, ToolResult};

#[derive(Debug, Clone)]
pub(super) struct UserQuestionPayload {
    pub question: String,
    pub options: Vec<String>,
    pub allow_custom: bool,
}

pub(super) fn should_handle_user_question_tool(tool_call: &ToolCall, result: &ToolResult) -> bool {
    (tool_call.function.name == "ExitPlanMode" || tool_call.function.name == "ask_user")
        && result.success
}

pub(super) fn parse_user_question_payload(result_content: &str) -> Option<UserQuestionPayload> {
    let payload = serde_json::from_str::<serde_json::Value>(result_content).ok()?;

    let question = payload["question"]
        .as_str()
        .unwrap_or("Please select:")
        .to_string();
    let options: Vec<String> = payload["options"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    let allow_custom = payload["allow_custom"].as_bool().unwrap_or(true);

    Some(UserQuestionPayload {
        question,
        options,
        allow_custom,
    })
}
