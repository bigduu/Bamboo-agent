use super::{AnthropicProvider, BodhiProvider, GeminiProvider, OpenAIProvider};
use crate::prompt_ir::{PromptIR, Segment, SegmentRole};
use crate::provider::{LLMProvider, LLMRequestOptions};
use crate::resolved_model::ResolvedModel;
use bamboo_domain::{FunctionSchema, Message, ProviderModelRef, ReasoningEffort, ToolSchema};
use futures::StreamExt;
use reqwest::Client;
use serde_json::{json, Value};
use std::sync::Arc;
use wiremock::matchers::{body_partial_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const EFFORTS: [Option<ReasoningEffort>; 7] = [
    None,
    Some(ReasoningEffort::Disabled),
    Some(ReasoningEffort::Low),
    Some(ReasoningEffort::Medium),
    Some(ReasoningEffort::High),
    Some(ReasoningEffort::Xhigh),
    Some(ReasoningEffort::Max),
];

const ROUTES: [(&str, &str, &str, &str); 7] = [
    ("openai-chat", "/chat/completions", "/reasoning_effort", "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n"),
    ("openai-responses", "/responses", "/reasoning/effort", "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_test\"}}\n\n"),
    ("anthropic", "/messages", "/output_config/effort", "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"),
    ("gemini", "/models/custom-route:streamGenerateContent", "/generationConfig/thinkingConfig/thinkingLevel", "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"ok\"}]},\"finishReason\":\"STOP\"}]}\n\n"),
    ("bodhi-openai", "/proxy/openai/v1/chat/completions", "/reasoning_effort", "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n"),
    ("bodhi-anthropic", "/proxy/anthropic/v1/messages", "/output_config/effort", "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"),
    ("bodhi-gemini", "/proxy/gemini/v1beta/models/custom-route:streamGenerateContent", "/generationConfig/thinkingConfig/thinkingLevel", "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"ok\"}]},\"finishReason\":\"STOP\"}]}\n\n"),
];

fn provider(
    route: &str,
    server: &MockServer,
    default: Option<ReasoningEffort>,
) -> Box<dyn LLMProvider> {
    let client = Client::builder().no_proxy().build().unwrap();
    match route {
        "openai-chat" | "openai-responses" => Box::new(
            OpenAIProvider::new("test-key")
                .with_base_url(server.uri())
                .with_client(client)
                .with_reasoning_effort(default)
                .with_responses_only_models(if route == "openai-responses" {
                    vec!["custom-route".to_string()]
                } else {
                    vec![]
                }),
        ),
        "anthropic" => Box::new(
            AnthropicProvider::new("test-key")
                .with_base_url(server.uri())
                .with_client(client)
                .with_reasoning_effort(default),
        ),
        "gemini" => Box::new(
            GeminiProvider::new("test-key")
                .with_base_url(server.uri())
                .with_client(client)
                .with_reasoning_effort(default),
        ),
        route => Box::new(
            BodhiProvider::new("test-key")
                .with_base_url(server.uri())
                .with_client(client)
                .with_target_provider(route.strip_prefix("bodhi-").unwrap())
                .with_reasoning_effort(default),
        ),
    }
}

fn tools() -> Vec<ToolSchema> {
    vec![ToolSchema {
        schema_type: "function".to_string(),
        function: FunctionSchema {
            name: "load_skill".to_string(),
            description: "Load one skill".to_string(),
            parameters: json!({"type": "object"}),
        },
    }]
}

fn assert_effort(body: &Value, pointer: &str, effort: Option<ReasoningEffort>) {
    let wire_effort = effort
        .filter(|level| pointer != "/output_config/effort" || *level != ReasoningEffort::Disabled);
    let expected = wire_effort.map(|effort| {
        // Native Gemini's enum spelling changes case only; no level is capped
        // or replaced, including values the upstream may reject.
        if pointer.ends_with("thinkingLevel") {
            json!(effort.as_str().to_ascii_uppercase())
        } else {
            json!(effort.as_str())
        }
    });
    assert_eq!(body.pointer(pointer), expected.as_ref());
    if pointer == "/output_config/effort" {
        let activation = effort.map(|effort| {
            if effort == ReasoningEffort::Disabled {
                json!("disabled")
            } else {
                json!("adaptive")
            }
        });
        assert_eq!(body.pointer("/thinking/type"), activation.as_ref());
    }
    assert!(body.pointer("/thinking/budget_tokens").is_none());
    assert!(body
        .pointer("/generationConfig/thinkingConfig/thinkingBudget")
        .is_none());
}

#[tokio::test]
async fn explicit_disable_reaches_native_and_proxy_wires_through_a_max_role() {
    for (route, endpoint, pointer, sse) in ROUTES {
        for use_ir in [false, true] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path(endpoint))
                .respond_with(
                    ResponseTemplate::new(200)
                        .insert_header("content-type", "text/event-stream")
                        .set_body_string(sse),
                )
                .expect(1)
                .mount(&server)
                .await;
            let inner: Arc<dyn LLMProvider> =
                Arc::from(provider(route, &server, Some(ReasoningEffort::High)));
            let resolved = ResolvedModel::from_ref(
                inner,
                &ProviderModelRef::new("configured-route", "custom-route")
                    .with_reasoning_effort(ReasoningEffort::Max),
            );
            let options = LLMRequestOptions {
                reasoning_effort: Some(ReasoningEffort::Disabled),
                ..Default::default()
            };
            let ir = PromptIR {
                segments: vec![Segment::new(
                    SegmentRole::Conversation,
                    vec![Message::user("Return the final answer")],
                )],
                ..Default::default()
            };
            let mut stream = if use_ir {
                resolved
                    .provider
                    .chat_stream_ir(&ir, &[], Some(128), &resolved.model_name, Some(&options))
                    .await
                    .unwrap()
            } else {
                resolved
                    .provider
                    .chat_stream_with_options(
                        &[Message::user("Return the final answer")],
                        &[],
                        Some(128),
                        &resolved.model_name,
                        Some(&options),
                    )
                    .await
                    .unwrap()
            };
            while let Some(chunk) = stream.next().await {
                chunk.unwrap();
            }
            let requests = server.received_requests().await.unwrap();
            assert_eq!(requests.len(), 1);
            let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
            assert_effort(&body, pointer, Some(ReasoningEffort::Disabled));
            assert_eq!(requests[0].url.path(), endpoint);
        }
    }
}

#[tokio::test]
async fn every_selected_effort_reaches_each_final_native_and_proxy_request() {
    for (route, endpoint, pointer, sse) in ROUTES {
        for effort in EFFORTS {
            for provider_default in [false, true] {
                let server = MockServer::start().await;
                Mock::given(method("POST"))
                    .and(path(endpoint))
                    .respond_with(
                        ResponseTemplate::new(200)
                            .insert_header("content-type", "text/event-stream")
                            .set_body_string(sse),
                    )
                    .expect(1)
                    .mount(&server)
                    .await;
                let provider =
                    provider(route, &server, if provider_default { effort } else { None });
                let options = LLMRequestOptions {
                    reasoning_effort: if provider_default { None } else { effort },
                    required_tool: Some("load_skill".to_string()),
                    ..Default::default()
                };
                // A small output limit must not trigger a host-side budget map
                // or suppress the caller's selected effort.
                let mut stream = provider
                    .chat_stream_with_options(
                        &[Message::user("hello")],
                        &tools(),
                        Some(1),
                        "custom-route",
                        Some(&options),
                    )
                    .await
                    .unwrap_or_else(|e| panic!("{route} {effort:?}: {e}"));
                while let Some(chunk) = stream.next().await {
                    chunk.unwrap();
                }
                let requests = server.received_requests().await.unwrap();
                assert_eq!(requests.len(), 1);
                let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
                assert_effort(&body, pointer, effort);
            }
        }
    }
}

#[tokio::test]
async fn chat_compression_corrects_only_a_supported_disable_rejection() {
    for (route, endpoint, _, sse) in ROUTES
        .into_iter()
        .filter(|(route, ..)| matches!(*route, "openai-chat" | "bodhi-openai"))
    {
        let effort_error = json!({
            "error": {"param": "reasoning_effort", "code": "unsupported_value",
                      "message": "Unsupported value: 'none'."}
        });
        let unrelated = json!({"error": {"param": "messages", "code": "invalid_value",
                                        "message": "Invalid request messages"}});
        for (status, rejection, purpose, metered, corrects) in [
            (400, &effort_error, "compression", false, true),
            (400, &effort_error, "compression", true, false),
            (400, &effort_error, "agent_loop", false, false),
            (403, &effort_error, "compression", false, false),
            (400, &unrelated, "compression", false, false),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path(endpoint))
                .and(body_partial_json(json!({"enable_thinking": false})))
                .respond_with(
                    ResponseTemplate::new(200)
                        .insert_header("content-type", "text/event-stream")
                        .set_body_string(sse),
                )
                .expect(u64::from(corrects))
                .mount(&server)
                .await;
            Mock::given(method("POST"))
                .and(path(endpoint))
                .and(body_partial_json(json!({"reasoning_effort": "none"})))
                .respond_with(ResponseTemplate::new(status).set_body_json(rejection))
                .expect(1)
                .mount(&server)
                .await;

            let provider = provider(route, &server, None);
            let options = LLMRequestOptions {
                reasoning_effort: Some(ReasoningEffort::Disabled),
                request_purpose: Some(purpose.into()),
                ..Default::default()
            };
            let messages = [Message::user("Summarize the conversation")];
            let dispatch =
                provider.chat_stream_with_options(&messages, &[], Some(128), route, Some(&options));
            let response = if metered {
                crate::retry::WorkPhysicalRequest::default()
                    .scope(dispatch)
                    .await
            } else {
                dispatch.await
            };
            let requests = server.received_requests().await.unwrap();
            assert_eq!(requests.len(), 1 + usize::from(corrects));
            if corrects {
                let mut stream = response.unwrap();
                while let Some(chunk) = stream.next().await {
                    chunk.unwrap();
                }
                let first: Value = requests[0].body_json().unwrap();
                let mut expected = first.as_object().unwrap().clone();
                expected.remove("reasoning_effort");
                expected.insert("enable_thinking".into(), json!(false));
                assert_eq!(
                    requests[1].body_json::<Value>().unwrap(),
                    Value::Object(expected)
                );
            } else {
                let error = match response {
                    Ok(_) => panic!("unexpected stream from rejected request"),
                    Err(error) => error,
                };
                assert!(
                    error.to_string().contains(&format!("HTTP {status}")),
                    "{error}"
                );
            }
        }
    }
}

#[tokio::test]
async fn unsupported_selected_effort_returns_the_upstream_error_without_a_second_request() {
    for (route, endpoint, pointer, _) in ROUTES {
        for effort in EFFORTS.into_iter().flatten() {
            let server = MockServer::start().await;
            Mock::given(method("POST")).and(path(endpoint))
                .respond_with(ResponseTemplate::new(400).set_body_json(json!({"error":{"message":"Unsupported reasoning effort: preserve-upstream-error"}})))
                .expect(1).mount(&server).await;
            let provider = provider(route, &server, None);
            let options = LLMRequestOptions {
                reasoning_effort: Some(effort),
                required_tool: Some("load_skill".to_string()),
                ..Default::default()
            };
            let error = match provider
                .chat_stream_with_options(
                    &[Message::user("hello")],
                    &tools(),
                    Some(1),
                    "custom-route",
                    Some(&options),
                )
                .await
            {
                Ok(_) => panic!("{route} {effort:?} unexpectedly succeeded"),
                Err(error) => error,
            };
            assert!(
                error.to_string().contains("preserve-upstream-error"),
                "{route} {effort:?}: {error}"
            );
            let requests = server.received_requests().await.unwrap();
            assert_eq!(requests.len(), 1, "{route} {effort:?}");
            let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
            assert_effort(&body, pointer, Some(effort));
        }
    }
}
