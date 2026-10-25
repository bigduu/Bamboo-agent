use super::{AnthropicProvider, BodhiProvider, GeminiProvider, OpenAIProvider};
use crate::provider::{LLMProvider, LLMRequestOptions};
use bamboo_domain::{FunctionSchema, Message, ReasoningEffort, ToolSchema};
use futures::StreamExt;
use reqwest::Client;
use serde_json::{json, Value};
use wiremock::matchers::{method, path};
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
    let expected = effort.map(|effort| {
        // Native Gemini's enum spelling changes case only; no level is capped
        // or replaced, including values the upstream may reject.
        if pointer.ends_with("thinkingLevel") {
            json!(effort.as_str().to_ascii_uppercase())
        } else {
            json!(effort.as_str())
        }
    });
    assert_eq!(body.pointer(pointer), expected.as_ref());
    assert!(body.pointer("/thinking/budget_tokens").is_none());
    assert!(body
        .pointer("/generationConfig/thinkingConfig/thinkingBudget")
        .is_none());
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
