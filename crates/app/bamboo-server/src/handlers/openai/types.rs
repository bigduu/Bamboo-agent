use bamboo_llm::api::models::Tool;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize)]
pub(super) struct ListModelsResponse {
    pub(super) success: bool,
    pub(super) object: String,
    pub(super) data: Vec<Model>,
}

#[derive(Debug, Serialize)]
pub(super) struct Model {
    pub(super) id: String,
    pub(super) object: String,
    pub(super) created: u64,
    pub(super) owned_by: String,
    pub(super) supported_endpoint_types: Vec<String>,
}

// ============================================================================
// OpenAI Responses API (v1/responses) - Minimal compatibility layer
// ============================================================================

/// OpenAI Responses API request (subset).
///
/// We intentionally accept a subset of the spec to support common SDK usage:
/// - `model`
/// - `input` (string or array of message-like objects)
/// - `instructions` (forwarded as top-level Responses API instructions)
/// - `previous_response_id` (passed via flattened parameters for stateful continuation)
/// - `tools` (Responses-API flat tool entries; legacy nested shape tolerated)
/// - `stream`
/// - `max_output_tokens` (mapped to provider max tokens)
#[derive(Debug, Deserialize, Clone, Default)]
pub struct ResponsesCreateRequest {
    pub(super) model: String,
    #[serde(default)]
    pub(super) input: serde_json::Value,
    #[serde(default)]
    pub(super) instructions: Option<String>,
    #[serde(default)]
    pub(super) tools: Option<Vec<ResponsesToolParam>>,
    #[serde(default)]
    pub(super) stream: Option<bool>,
    #[serde(default)]
    pub(super) max_output_tokens: Option<u32>,
    #[serde(flatten)]
    pub(super) parameters: HashMap<String, serde_json::Value>,
}

/// A Responses-API function tool declaration — FLAT, per the spec:
/// `{"type":"function","name":"…","description":"…","parameters":{…},"strict":false}`.
///
/// The nested `{type, function:{name,…}}` shape belongs to the Chat Completions
/// wire format; reusing that model here made every tools-bearing /responses
/// request fail with `missing field 'function'` (#525).
#[derive(Debug, Deserialize, Clone)]
pub(super) struct ResponsesFunctionToolParam {
    #[serde(rename = "type")]
    pub(super) tool_type: String,
    pub(super) name: String,
    #[serde(default)]
    pub(super) description: Option<String>,
    #[serde(default)]
    pub(super) parameters: serde_json::Value,
    /// Accepted and ignored — bamboo does not enforce strict schemas upstream.
    /// Kept as a real field (read in tests) to document the wire shape.
    #[serde(default)]
    #[allow(dead_code)]
    pub(super) strict: Option<bool>,
}

/// A `tools[]` entry on POST /responses.
///
/// Untagged, tried in order: the flat Responses shape (spec), the nested
/// Chat-Completions shape (legacy clients that already worked against this
/// endpoint), then a raw catch-all so non-function tool types (`web_search`,
/// …) can be skipped instead of failing the whole request. #525.
#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub(super) enum ResponsesToolParam {
    Flat(ResponsesFunctionToolParam),
    Nested(Tool),
    Other(serde_json::Value),
}

#[derive(Debug, Serialize, Clone)]
pub(super) struct ResponsesUsage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) input_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) output_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) total_tokens: Option<u64>,
}

#[derive(Debug, Serialize, Clone)]
pub(super) struct ResponsesTextContent {
    #[serde(rename = "type")]
    pub(super) content_type: String, // "output_text"
    pub(super) text: String,
}

#[derive(Debug, Serialize, Clone)]
pub(super) struct ResponsesMessageOutputItem {
    pub(super) id: String,
    #[serde(rename = "type")]
    pub(super) item_type: String, // "message"
    pub(super) role: String, // "assistant"
    pub(super) content: Vec<ResponsesTextContent>,
}

#[derive(Debug, Serialize, Clone)]
pub(super) struct ResponsesFunctionCallOutputItem {
    pub(super) id: String,
    #[serde(rename = "type")]
    pub(super) item_type: String, // "function_call"
    pub(super) call_id: String,
    pub(super) name: String,
    pub(super) arguments: String,
    /// "in_progress" while streaming, "completed" in final output. Clients
    /// (Codex) read the completed item off `response.output_item.done`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) status: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(untagged)]
pub(super) enum ResponsesOutputItem {
    Message(ResponsesMessageOutputItem),
    FunctionCall(ResponsesFunctionCallOutputItem),
}

#[derive(Debug, Serialize, Clone)]
pub(super) struct ResponsesCreateResponse {
    pub(super) id: String,
    pub(super) object: String, // "response"
    pub(super) created_at: u64,
    pub(super) model: String,
    pub(super) status: String, // "completed" | "in_progress"
    pub(super) output: Vec<ResponsesOutputItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) usage: Option<ResponsesUsage>,
}

#[derive(Debug, Serialize)]
pub(super) struct ResponsesStreamEvent<T> {
    #[serde(rename = "type")]
    pub(super) event_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) response: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) response_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) item_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) output_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) content_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) delta: Option<String>,
    /// Full item object for `response.output_item.added` / `.done` (#525).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) item: Option<ResponsesOutputItem>,
    /// Complete arguments for `response.function_call_arguments.done` (#525).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) arguments: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_models_response_serialization() {
        let response = ListModelsResponse {
            success: true,
            object: "list".to_string(),
            data: vec![Model {
                id: "gpt-4".to_string(),
                object: "model".to_string(),
                created: 1234567890,
                owned_by: "openai".to_string(),
                supported_endpoint_types: vec!["chat".to_string()],
            }],
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"success\":true"));
        assert!(json.contains("\"object\":\"list\""));
        assert!(json.contains("\"gpt-4\""));
    }

    #[test]
    fn test_model_serialization() {
        let model = Model {
            id: "claude-3-opus".to_string(),
            object: "model".to_string(),
            created: 1700000000,
            owned_by: "anthropic".to_string(),
            supported_endpoint_types: vec!["chat".to_string(), "messages".to_string()],
        };

        let json = serde_json::to_string(&model).unwrap();
        assert!(json.contains("\"claude-3-opus\""));
        assert!(json.contains("\"anthropic\""));
    }

    #[test]
    fn test_responses_create_request_minimal() {
        let json = r#"{"model":"gpt-4"}"#;
        let req: ResponsesCreateRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.model, "gpt-4");
        assert!(req.input.is_null());
        assert!(req.instructions.is_none());
        assert!(req.tools.is_none());
        assert!(req.stream.is_none());
        assert!(req.max_output_tokens.is_none());
    }

    #[test]
    fn test_responses_create_request_with_input_string() {
        let json = r#"{"model":"gpt-4","input":"Hello"}"#;
        let req: ResponsesCreateRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.model, "gpt-4");
        assert_eq!(req.input.as_str().unwrap(), "Hello");
    }

    #[test]
    fn test_responses_create_request_with_instructions() {
        let json = r#"{"model":"gpt-4","instructions":"You are helpful"}"#;
        let req: ResponsesCreateRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.instructions, Some("You are helpful".to_string()));
    }

    #[test]
    fn test_responses_create_request_with_all_fields() {
        let json = r#"{
            "model":"gpt-4",
            "input":"Hello",
            "instructions":"Be helpful",
            "stream":true,
            "max_output_tokens":100
        }"#;
        let req: ResponsesCreateRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.model, "gpt-4");
        assert_eq!(req.input.as_str().unwrap(), "Hello");
        assert_eq!(req.instructions, Some("Be helpful".to_string()));
        assert_eq!(req.stream, Some(true));
        assert_eq!(req.max_output_tokens, Some(100));
    }

    #[test]
    fn test_responses_usage_serialization() {
        let usage = ResponsesUsage {
            input_tokens: Some(100),
            output_tokens: Some(50),
            total_tokens: Some(150),
        };

        let json = serde_json::to_string(&usage).unwrap();
        assert!(json.contains("\"input_tokens\":100"));
        assert!(json.contains("\"output_tokens\":50"));
    }

    #[test]
    fn test_responses_usage_minimal() {
        let usage = ResponsesUsage {
            input_tokens: None,
            output_tokens: None,
            total_tokens: None,
        };

        let json = serde_json::to_string(&usage).unwrap();
        // Should skip all null fields
        assert!(!json.contains("input_tokens"));
    }

    #[test]
    fn test_responses_text_content() {
        let content = ResponsesTextContent {
            content_type: "output_text".to_string(),
            text: "Hello world".to_string(),
        };

        let json = serde_json::to_string(&content).unwrap();
        assert!(json.contains("\"type\":\"output_text\""));
        assert!(json.contains("\"text\":\"Hello world\""));
    }

    #[test]
    fn test_responses_message_output_item() {
        let item = ResponsesMessageOutputItem {
            id: "msg-123".to_string(),
            item_type: "message".to_string(),
            role: "assistant".to_string(),
            content: vec![ResponsesTextContent {
                content_type: "output_text".to_string(),
                text: "Response text".to_string(),
            }],
        };

        let json = serde_json::to_string(&item).unwrap();
        assert!(json.contains("\"id\":\"msg-123\""));
        assert!(json.contains("\"type\":\"message\""));
        assert!(json.contains("\"role\":\"assistant\""));
    }

    #[test]
    fn test_responses_function_call_output_item() {
        let item = ResponsesFunctionCallOutputItem {
            id: "fc-456".to_string(),
            item_type: "function_call".to_string(),
            call_id: "call-789".to_string(),
            name: "get_weather".to_string(),
            arguments: r#"{"location":"NYC"}"#.to_string(),
            status: None,
        };

        let json = serde_json::to_string(&item).unwrap();
        assert!(json.contains("\"id\":\"fc-456\""));
        assert!(json.contains("\"type\":\"function_call\""));
        assert!(json.contains("\"call_id\":\"call-789\""));
        assert!(json.contains("\"name\":\"get_weather\""));
    }

    #[test]
    fn test_responses_output_item_message() {
        let msg_item = ResponsesMessageOutputItem {
            id: "msg-1".to_string(),
            item_type: "message".to_string(),
            role: "assistant".to_string(),
            content: vec![],
        };

        let output_item = ResponsesOutputItem::Message(msg_item.clone());
        let json = serde_json::to_string(&output_item).unwrap();
        assert!(json.contains("\"type\":\"message\""));
    }

    #[test]
    fn test_responses_output_item_function_call() {
        let fc_item = ResponsesFunctionCallOutputItem {
            id: "fc-1".to_string(),
            item_type: "function_call".to_string(),
            call_id: "call-1".to_string(),
            name: "test".to_string(),
            arguments: "{}".to_string(),
            status: None,
        };

        let output_item = ResponsesOutputItem::FunctionCall(fc_item.clone());
        let json = serde_json::to_string(&output_item).unwrap();
        assert!(json.contains("\"type\":\"function_call\""));
    }

    #[test]
    fn test_responses_create_response() {
        let response = ResponsesCreateResponse {
            id: "resp-123".to_string(),
            object: "response".to_string(),
            created_at: 1700000000,
            model: "gpt-4".to_string(),
            status: "completed".to_string(),
            output: vec![],
            usage: None,
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"id\":\"resp-123\""));
        assert!(json.contains("\"object\":\"response\""));
        assert!(json.contains("\"status\":\"completed\""));
    }

    #[test]
    fn test_responses_create_response_with_usage() {
        let usage = ResponsesUsage {
            input_tokens: Some(100),
            output_tokens: Some(50),
            total_tokens: Some(150),
        };

        let response = ResponsesCreateResponse {
            id: "resp-456".to_string(),
            object: "response".to_string(),
            created_at: 1700000000,
            model: "gpt-4".to_string(),
            status: "completed".to_string(),
            output: vec![],
            usage: Some(usage),
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"usage\""));
        assert!(json.contains("\"input_tokens\":100"));
    }

    #[test]
    fn test_responses_stream_event_minimal() {
        let event: ResponsesStreamEvent<String> = ResponsesStreamEvent {
            event_type: "response.created".to_string(),
            response: None,
            response_id: None,
            item_id: None,
            output_index: None,
            content_index: None,
            delta: None,
            item: None,
            arguments: None,
        };

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"type\":\"response.created\""));
    }

    #[test]
    fn test_responses_stream_event_with_delta() {
        let event: ResponsesStreamEvent<String> = ResponsesStreamEvent {
            event_type: "response.output_item.text.delta".to_string(),
            response: None,
            response_id: Some("resp-123".to_string()),
            item_id: Some("item-456".to_string()),
            output_index: Some(0),
            content_index: Some(0),
            delta: Some("Hello".to_string()),
            item: None,
            arguments: None,
        };

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"delta\":\"Hello\""));
        assert!(json.contains("\"response_id\":\"resp-123\""));
    }

    #[test]
    fn test_responses_create_request_with_extra_parameters() {
        let json =
            r#"{"model":"gpt-4","temperature":0.7,"top_p":0.9,"previous_response_id":"resp_123"}"#;
        let req: ResponsesCreateRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.model, "gpt-4");
        assert_eq!(
            req.parameters.get("temperature").unwrap(),
            &serde_json::json!(0.7)
        );
        assert_eq!(
            req.parameters.get("top_p").unwrap(),
            &serde_json::json!(0.9)
        );
        assert_eq!(
            req.parameters.get("previous_response_id").unwrap(),
            &serde_json::json!("resp_123")
        );
    }

    // #525: the Responses API sends function tools FLAT (name/parameters at the
    // top level, `strict` present). This exact shape used to 400 with
    // "missing field `function`" because the field reused the nested
    // Chat-Completions Tool model.
    #[test]
    fn test_responses_create_request_with_flat_tools() {
        let json = r#"{
            "model":"gpt-5",
            "input":"hi",
            "tools":[{
                "type":"function",
                "name":"get_weather",
                "description":"Get weather",
                "strict":false,
                "parameters":{"type":"object","properties":{"location":{"type":"string"}}}
            }]
        }"#;
        let req: ResponsesCreateRequest = serde_json::from_str(json).unwrap();
        let tools = req.tools.expect("tools parsed");
        assert_eq!(tools.len(), 1);
        match &tools[0] {
            ResponsesToolParam::Flat(flat) => {
                assert_eq!(flat.tool_type, "function");
                assert_eq!(flat.name, "get_weather");
                assert_eq!(flat.description.as_deref(), Some("Get weather"));
                assert_eq!(flat.strict, Some(false));
                assert!(flat.parameters.get("properties").is_some());
            }
            other => panic!("expected flat tool param, got {other:?}"),
        }
    }

    // Legacy clients that already sent the nested Chat-Completions shape to
    // this endpoint must keep working.
    #[test]
    fn test_responses_create_request_with_nested_tools_still_parses() {
        let json = r#"{
            "model":"gpt-5",
            "tools":[{
                "type":"function",
                "function":{"name":"get_weather","parameters":{"type":"object"}}
            }]
        }"#;
        let req: ResponsesCreateRequest = serde_json::from_str(json).unwrap();
        let tools = req.tools.expect("tools parsed");
        match &tools[0] {
            ResponsesToolParam::Nested(tool) => {
                assert_eq!(tool.function.name, "get_weather");
            }
            other => panic!("expected nested tool param, got {other:?}"),
        }
    }

    // Non-function tool types (web_search, …) must not fail the request; they
    // land in the catch-all and are skipped at conversion time.
    #[test]
    fn test_responses_create_request_tolerates_non_function_tools() {
        let json = r#"{"model":"gpt-5","tools":[{"type":"web_search"}]}"#;
        let req: ResponsesCreateRequest = serde_json::from_str(json).unwrap();
        let tools = req.tools.expect("tools parsed");
        assert!(matches!(&tools[0], ResponsesToolParam::Other(_)));
    }

    #[test]
    fn test_list_models_response_debug() {
        let response = ListModelsResponse {
            success: true,
            object: "list".to_string(),
            data: vec![],
        };

        let debug_str = format!("{:?}", response);
        assert!(debug_str.contains("ListModelsResponse"));
    }

    #[test]
    fn test_model_debug() {
        let model = Model {
            id: "test".to_string(),
            object: "model".to_string(),
            created: 12345,
            owned_by: "test-org".to_string(),
            supported_endpoint_types: vec![],
        };

        let debug_str = format!("{:?}", model);
        assert!(debug_str.contains("Model"));
        assert!(debug_str.contains("test"));
    }
}
