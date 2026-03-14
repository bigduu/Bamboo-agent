use actix_web::Error as ActixError;
use bytes::Bytes;
use serde_json::Value;

use crate::agent::{
    core::tools::ToolCall,
    llm::protocol::gemini::{
        GeminiCandidate, GeminiContent, GeminiFunctionCall, GeminiPart, GeminiResponse,
    },
};

pub(super) fn token_chunk_bytes(token: String) -> Result<Bytes, ActixError> {
    let chunk = GeminiResponse {
        candidates: vec![GeminiCandidate {
            content: GeminiContent {
                role: "model".to_string(),
                parts: vec![GeminiPart {
                    text: Some(token),
                    inline_data: None,
                    file_data: None,
                    function_call: None,
                    function_response: None,
                }],
            },
            finish_reason: None,
        }],
    };
    encode_chunk(chunk)
}

pub(super) fn tool_call_chunk_bytes(tool_call: ToolCall) -> Result<Bytes, ActixError> {
    let args = parse_tool_call_arguments(&tool_call.function.arguments);
    let chunk = GeminiResponse {
        candidates: vec![GeminiCandidate {
            content: GeminiContent {
                role: "model".to_string(),
                parts: vec![GeminiPart {
                    text: None,
                    inline_data: None,
                    file_data: None,
                    function_call: Some(GeminiFunctionCall {
                        name: tool_call.function.name,
                        args,
                    }),
                    function_response: None,
                }],
            },
            finish_reason: None,
        }],
    };
    encode_chunk(chunk)
}

pub(super) fn done_chunk_bytes() -> Bytes {
    let chunk = GeminiResponse {
        candidates: vec![GeminiCandidate {
            content: GeminiContent {
                role: "model".to_string(),
                parts: vec![],
            },
            finish_reason: Some("STOP".to_string()),
        }],
    };

    let json = serde_json::to_string(&chunk).unwrap_or_default();
    Bytes::from(format!("data: {json}\n\n"))
}

pub(super) fn stream_error(message: String) -> ActixError {
    ActixError::from(std::io::Error::other(message))
}

pub(super) fn parse_tool_call_arguments(arguments: &str) -> Value {
    serde_json::from_str(arguments).unwrap_or(Value::Object(serde_json::Map::new()))
}

fn encode_chunk(chunk: GeminiResponse) -> Result<Bytes, ActixError> {
    let json = serde_json::to_string(&chunk)
        .map_err(|error| stream_error(format!("JSON error: {error}")))?;
    Ok(Bytes::from(format!("data: {json}\n\n")))
}
