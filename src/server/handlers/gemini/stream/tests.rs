use crate::agent::core::tools::{FunctionCall, ToolCall};

use super::sse::{
    done_chunk_bytes, parse_tool_call_arguments, token_chunk_bytes, tool_call_chunk_bytes,
};

fn decode_sse_json(bytes: bytes::Bytes) -> serde_json::Value {
    let payload = std::str::from_utf8(&bytes).expect("sse bytes should be utf8");
    let json = payload
        .strip_prefix("data: ")
        .expect("sse payload should start with data prefix")
        .trim();
    serde_json::from_str(json).expect("sse payload should contain valid json")
}

#[test]
fn token_chunk_bytes_encodes_text_delta() {
    let bytes = token_chunk_bytes("hello".to_string()).expect("token chunk should serialize");
    let json = decode_sse_json(bytes);
    assert_eq!(
        json["candidates"][0]["content"]["parts"][0]["text"],
        "hello"
    );
}

#[test]
fn tool_call_chunk_bytes_falls_back_to_empty_args_for_invalid_json() {
    let tool_call = ToolCall {
        id: "call-1".to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "list_files".to_string(),
            arguments: "{invalid".to_string(),
        },
    };

    let bytes = tool_call_chunk_bytes(tool_call).expect("tool-call chunk should serialize");
    let json = decode_sse_json(bytes);
    assert_eq!(
        json["candidates"][0]["content"]["parts"][0]["functionCall"]["name"],
        "list_files"
    );
    assert_eq!(
        json["candidates"][0]["content"]["parts"][0]["functionCall"]["args"],
        serde_json::json!({})
    );
}

#[test]
fn done_chunk_bytes_sets_stop_finish_reason() {
    let json = decode_sse_json(done_chunk_bytes());
    assert_eq!(json["candidates"][0]["finish_reason"], "STOP");
}

#[test]
fn parse_tool_call_arguments_parses_valid_json() {
    let args = parse_tool_call_arguments(r#"{"path":"/tmp","recursive":false}"#);
    assert_eq!(args["path"], "/tmp");
    assert_eq!(args["recursive"], false);
}
