use bamboo_agent_core::tools::{ToolCall, ToolResult};

/// Names of tools that pause the agent loop to wait for user input/approval.
const PAUSE_TOOLS: [&str; 3] = [
    "ExitPlanMode",
    "conclusion_with_options",
    "request_permissions",
];

#[derive(Debug, Clone)]
pub(super) struct UserQuestionPayload {
    pub question: String,
    pub options: Vec<String>,
    pub allow_custom: bool,
}

/// Returns `true` when the tool call + result represent a user-facing question
/// that should pause the agent loop and wait for the user response.
///
/// Currently recognises:
/// - `conclusion_with_options` — general purpose question
/// - `ExitPlanMode` — plan confirmation
/// - `request_permissions` — permission approval request
/// - any tool whose result is a synthesized permission-approval prompt (the
///   permission checker gates a mutating tool and returns the same shape as
///   `request_permissions`, tagged with `display_preference = "request_permissions"`)
pub(super) fn should_handle_user_question_tool(tool_call: &ToolCall, result: &ToolResult) -> bool {
    if !result.success {
        return false;
    }
    let normalized = bamboo_tools::normalize_tool_ref(&tool_call.function.name)
        .unwrap_or_else(|| tool_call.function.name.trim().to_string());
    if PAUSE_TOOLS.contains(&normalized.as_str()) {
        return true;
    }
    // A permission gate can synthesize an approval request for any tool; it is
    // tagged with this display preference (same payload request_permissions returns).
    result.display_preference.as_deref() == Some("request_permissions")
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
