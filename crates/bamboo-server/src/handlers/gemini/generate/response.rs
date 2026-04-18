use bamboo_application_agent::ToolCall;
use bamboo_infrastructure_llm::protocol::gemini::{
    GeminiCandidate, GeminiContent, GeminiFunctionCall, GeminiPart, GeminiResponse,
};

pub(super) fn build_gemini_response(
    full_content: String,
    tool_calls: Option<Vec<ToolCall>>,
) -> GeminiResponse {
    let mut parts = vec![GeminiPart {
        text: if full_content.is_empty() && tool_calls.is_none() {
            Some(String::new())
        } else if full_content.is_empty() {
            None
        } else {
            Some(full_content)
        },
        inline_data: None,
        file_data: None,
        function_call: None,
        function_response: None,
    }];

    if let Some(calls) = tool_calls {
        if parts[0].text.as_ref().is_none_or(|text| text.is_empty()) {
            parts.clear();
        }
        for tool_call in calls {
            parts.push(tool_call_part(tool_call));
        }
    }

    GeminiResponse {
        candidates: vec![GeminiCandidate {
            content: GeminiContent {
                role: "model".to_string(),
                parts,
            },
            finish_reason: Some("STOP".to_string()),
        }],
    }
}

fn tool_call_part(tool_call: ToolCall) -> GeminiPart {
    let args = serde_json::from_str(&tool_call.function.arguments)
        .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));

    GeminiPart {
        text: None,
        inline_data: None,
        file_data: None,
        function_call: Some(GeminiFunctionCall {
            name: tool_call.function.name,
            args,
        }),
        function_response: None,
    }
}
