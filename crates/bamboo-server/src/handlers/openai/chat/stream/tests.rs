use serde_json::Value;

use super::sse::{done_marker_bytes, openai_chunk_bytes, wrap_sse_data};
use bamboo_infrastructure_llm::types::LLMChunk;

fn decode_json(bytes: bytes::Bytes) -> Value {
    serde_json::from_slice(&bytes).expect("chunk should be valid json")
}

#[test]
fn openai_chunk_bytes_serializes_token_delta() {
    let bytes = openai_chunk_bytes(LLMChunk::Token("hello".to_string()), "gpt-5")
        .expect("token chunk should serialize");
    let json = decode_json(bytes);

    assert_eq!(json["choices"][0]["delta"]["content"], "hello");
}

#[test]
fn openai_chunk_bytes_serializes_done_finish_reason() {
    let bytes = openai_chunk_bytes(LLMChunk::Done, "gpt-5").expect("done chunk should serialize");
    let json = decode_json(bytes);

    assert_eq!(json["choices"][0]["finish_reason"], "stop");
}

#[test]
fn done_marker_bytes_returns_done_sentinel() {
    let done = done_marker_bytes();
    let payload = std::str::from_utf8(&done).expect("done marker should be utf8");
    assert_eq!(payload, "[DONE]");
}

#[test]
fn wrap_sse_data_wraps_payload_with_data_prefix() {
    let wrapped = wrap_sse_data(bytes::Bytes::from_static(b"{\"x\":1}"));
    let payload = std::str::from_utf8(&wrapped).expect("wrapped bytes should be utf8");

    assert_eq!(payload, "data: {\"x\":1}\n\n");
}
