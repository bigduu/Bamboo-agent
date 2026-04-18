use futures::stream;

use bamboo_application_agent::tools::FunctionCall;
use bamboo_application_agent::ToolCall;
use bamboo_infrastructure_llm::LLMChunk;

use super::collect::collect_response_chunks;
use super::response::build_gemini_response;

#[actix_web::test]
async fn collect_response_chunks_accumulates_tokens_and_keeps_last_tool_call_batch() {
    let mut stream = stream::iter(vec![
        Ok::<LLMChunk, &'static str>(LLMChunk::Token("Hel".to_string())),
        Ok(LLMChunk::Token("lo".to_string())),
        Ok(LLMChunk::ToolCalls(vec![tool_call(
            "tool-1",
            "search",
            r#"{"q":"first"}"#,
        )])),
        Ok(LLMChunk::ToolCalls(vec![tool_call(
            "tool-2",
            "search",
            r#"{"q":"second"}"#,
        )])),
        Ok(LLMChunk::Done),
    ]);

    let collected = collect_response_chunks(&mut stream)
        .await
        .expect("collection should succeed");

    assert_eq!(collected.full_content, "Hello");
    let calls = collected.tool_calls.expect("tool calls should exist");
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].id, "tool-2");
}

#[actix_web::test]
async fn collect_response_chunks_returns_stream_error() {
    let mut stream = stream::iter(vec![
        Ok::<LLMChunk, &'static str>(LLMChunk::Token("before".to_string())),
        Err("stream-failed"),
        Ok(LLMChunk::Done),
    ]);

    let error = collect_response_chunks(&mut stream)
        .await
        .expect_err("stream should fail");

    assert_eq!(error, "stream-failed");
}

#[test]
fn build_gemini_response_keeps_empty_text_part_when_no_content_or_tools() {
    let response = build_gemini_response(String::new(), None);
    let parts = &response.candidates[0].content.parts;

    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].text.as_deref(), Some(""));
    assert!(parts[0].function_call.is_none());
}

#[test]
fn build_gemini_response_emits_tool_call_parts_without_leading_empty_text() {
    let response = build_gemini_response(
        String::new(),
        Some(vec![tool_call("tool-1", "lookup", r#"{"topic":"rust"}"#)]),
    );

    let parts = &response.candidates[0].content.parts;
    assert_eq!(parts.len(), 1);
    assert!(parts[0].text.is_none());
    assert_eq!(
        parts[0]
            .function_call
            .as_ref()
            .map(|call| call.name.as_str()),
        Some("lookup")
    );
    assert_eq!(
        parts[0]
            .function_call
            .as_ref()
            .and_then(|call| call.args.get("topic"))
            .and_then(|value| value.as_str()),
        Some("rust")
    );
}

#[test]
fn build_gemini_response_uses_empty_object_for_invalid_tool_arguments() {
    let response = build_gemini_response(
        String::new(),
        Some(vec![tool_call("tool-1", "lookup", "not-json")]),
    );

    let args = &response.candidates[0].content.parts[0]
        .function_call
        .as_ref()
        .expect("function call should exist")
        .args;
    assert_eq!(args, &serde_json::json!({}));
}

fn tool_call(id: &str, name: &str, arguments: &str) -> ToolCall {
    ToolCall {
        id: id.to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: name.to_string(),
            arguments: arguments.to_string(),
        },
    }
}
