use std::sync::{Arc, Mutex};

use actix_web::{http::StatusCode, test as actix_test, web, App};
use bamboo_agent_core::{tools::ToolSchema, Message};
use bamboo_llm::provider::{LLMError, LLMProvider, LLMStream};
use serde_json::{json, Value};

use super::helpers::convert_responses_tools;
use super::types::{ResponsesCreateRequest, ResponsesToolParam, MAX_RESPONSES_TOOLS};
use crate::{app_state::AppState, error::AppError};

const COUNT_ERROR: &str = "Responses tools[] supports at most 128 entries";
const TYPE_ERROR: &str = "Malformed tools[] entry: missing string `type` field";

fn function_tools(count: usize) -> Vec<Value> {
    (0..count)
        .map(|index| {
            let function = json!({
                "name": format!("tool_{index}"),
                "description": "fixture function",
                "parameters": {"type": "object", "properties": {"value": {"type": "string"}}}
            });
            if index % 2 == 0 {
                let mut flat = function;
                flat["type"] = json!("function");
                flat
            } else {
                // A hybrid must continue to prefer the legacy nested schema.
                json!({"type": "function", "name": "ignored_flat_name", "function": function})
            }
        })
        .collect()
}

fn convert(values: Vec<Value>) -> Result<Vec<ToolSchema>, AppError> {
    let tools: Vec<ResponsesToolParam> = serde_json::from_value(json!(values)).unwrap();
    convert_responses_tools(Some(tools))
}

fn assert_bad_request(result: Result<Vec<ToolSchema>, AppError>, expected: &str) {
    match result.expect_err("tools must be rejected") {
        AppError::BadRequest(message) => assert_eq!(message, expected),
        error => panic!("expected BadRequest, got {error}"),
    }
}

#[test]
fn responses_tool_count_rejects_before_malformed_entry() {
    // Tiny invalid entries reproduce the old reservation-before-validation
    // order without approaching the JSON body limit or stressing the allocator.
    for count in [1024, MAX_RESPONSES_TOOLS + 1] {
        let bytes = serde_json::to_vec(&json!({
            "model": "fixture-model", "input": "hello", "tools": vec![Value::Null; count]
        }))
        .unwrap();
        let request: ResponsesCreateRequest = serde_json::from_slice(&bytes).unwrap();
        assert_bad_request(convert_responses_tools(request.tools), COUNT_ERROR);
    }
}

#[test]
fn responses_tool_count_preserves_empty_and_boundary_function_schemas() {
    assert!(convert_responses_tools(None).unwrap().is_empty());
    assert!(convert(vec![]).unwrap().is_empty());
    for count in [MAX_RESPONSES_TOOLS - 1, MAX_RESPONSES_TOOLS] {
        let schemas = convert(function_tools(count)).unwrap();
        assert_eq!(schemas.len(), count);
        for (index, schema) in schemas.iter().enumerate() {
            assert_eq!(schema.schema_type, "function");
            assert_eq!(schema.function.name, format!("tool_{index}"));
            assert_eq!(schema.function.description, "fixture function");
            assert_eq!(
                schema.function.parameters["properties"]["value"]["type"],
                "string"
            );
        }
    }
    assert_bad_request(
        convert(function_tools(MAX_RESPONSES_TOOLS + 1)),
        COUNT_ERROR,
    );
}

#[test]
fn responses_tool_count_includes_skipped_entries_and_preserves_within_limit_errors() {
    for unsupported in [
        json!({"type": "web_search"}),
        json!({"type": "custom", "name": "freeform"}),
    ] {
        assert!(convert(vec![unsupported.clone(); MAX_RESPONSES_TOOLS])
            .unwrap()
            .is_empty());
        assert_bad_request(
            convert(vec![unsupported; MAX_RESPONSES_TOOLS + 1]),
            COUNT_ERROR,
        );
    }
    for malformed in [Value::Null, json!(0), json!({}), json!({"type": 7})] {
        assert_bad_request(convert(vec![malformed; MAX_RESPONSES_TOOLS]), TYPE_ERROR);
    }
    let error = convert(vec![json!({"type": "function"}); MAX_RESPONSES_TOOLS]).unwrap_err();
    assert!(
        matches!(error, AppError::BadRequest(message) if message.starts_with("Malformed function tool:"))
    );
}

#[derive(Default)]
struct ToolProbe(Mutex<Vec<Vec<ToolSchema>>>);

#[async_trait::async_trait]
impl LLMProvider for ToolProbe {
    async fn chat_stream(
        &self,
        _: &[Message],
        tools: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        self.0.lock().unwrap().push(tools.to_vec());
        Ok(Box::pin(futures::stream::iter([
            Ok(bamboo_llm::LLMChunk::Token("fixture reply".into())),
            Ok(bamboo_llm::LLMChunk::Done),
        ])))
    }
}

fn configured_routes(config: &mut web::ServiceConfig) {
    config.service(web::scope("/openai/v1").configure(super::config));
}

#[actix_web::test]
async fn responses_tool_count_both_registrations_bound_dispatch_for_streaming_and_json() {
    let home = tempfile::tempdir().unwrap();
    let probe = Arc::new(ToolProbe::default());
    let mut config = bamboo_llm::Config::from_data_dir(Some(home.path().to_path_buf()));
    config.provider = "anthropic".into();
    config.providers_mut().anthropic = Some(bamboo_config::AnthropicConfig {
        model: Some("fixture-model".into()),
        ..Default::default()
    });
    let state = web::Data::new(
        AppState::new_with_provider(home.path().to_path_buf(), config, probe.clone())
            .await
            .unwrap(),
    );

    for registration in [
        crate::routes::openai_prefixed_routes as fn(&mut web::ServiceConfig),
        configured_routes,
    ] {
        let app =
            actix_test::init_service(App::new().app_data(state.clone()).configure(registration))
                .await;
        for stream in [false, true] {
            for count in [
                MAX_RESPONSES_TOOLS - 1,
                MAX_RESPONSES_TOOLS,
                MAX_RESPONSES_TOOLS + 1,
            ] {
                let before = probe.0.lock().unwrap().len();
                let response = actix_test::call_service(
                    &app,
                    actix_test::TestRequest::post()
                        .uri("/openai/v1/responses")
                        .peer_addr("127.0.0.1:52000".parse().unwrap())
                        .set_json(json!({"model": "fixture-model", "input": "hello", "stream": stream, "tools": function_tools(count)}))
                        .to_request(),
                ).await;
                if count > MAX_RESPONSES_TOOLS {
                    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
                    let body: Value = actix_test::read_body_json(response).await;
                    assert_eq!(
                        body,
                        json!({"error": {"message": format!("Bad request: {COUNT_ERROR}"), "type": "api_error"}})
                    );
                    assert_eq!(
                        probe.0.lock().unwrap().len(),
                        before,
                        "oversized request must not dispatch"
                    );
                } else {
                    assert_eq!(response.status(), StatusCode::OK);
                    let body = actix_test::read_body(response).await;
                    assert!(std::str::from_utf8(&body)
                        .unwrap()
                        .contains("fixture reply"));
                    let calls = probe.0.lock().unwrap();
                    assert_eq!(calls.len(), before + 1);
                    let schemas = calls.last().unwrap();
                    assert_eq!(schemas.len(), count);
                    assert_eq!(schemas[1].function.name, "tool_1");
                    assert_eq!(
                        schemas[1].function.parameters["properties"]["value"]["type"],
                        "string"
                    );
                }
            }

            for (entry, count, error) in [
                (Value::Null, MAX_RESPONSES_TOOLS, Some(TYPE_ERROR)),
                (Value::Null, MAX_RESPONSES_TOOLS + 1, Some(COUNT_ERROR)),
                (json!({"type": "web_search"}), MAX_RESPONSES_TOOLS, None),
                (
                    json!({"type": "web_search"}),
                    MAX_RESPONSES_TOOLS + 1,
                    Some(COUNT_ERROR),
                ),
            ] {
                let before = probe.0.lock().unwrap().len();
                let response = actix_test::call_service(
                    &app,
                    actix_test::TestRequest::post()
                        .uri("/openai/v1/responses")
                        .peer_addr("127.0.0.1:52000".parse().unwrap())
                        .set_json(json!({"model": "fixture-model", "input": "hello", "stream": stream, "tools": vec![entry; count]}))
                        .to_request(),
                ).await;
                if let Some(error) = error {
                    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
                    let body: Value = actix_test::read_body_json(response).await;
                    assert_eq!(body["error"]["message"], format!("Bad request: {error}"));
                    assert_eq!(probe.0.lock().unwrap().len(), before);
                } else {
                    assert_eq!(response.status(), StatusCode::OK);
                    let _ = actix_test::read_body(response).await;
                    let calls = probe.0.lock().unwrap();
                    assert_eq!(calls.len(), before + 1);
                    assert!(
                        calls.last().unwrap().is_empty(),
                        "unsupported tools remain skipped"
                    );
                }
            }
        }
    }
    state.shutdown().await;
}
