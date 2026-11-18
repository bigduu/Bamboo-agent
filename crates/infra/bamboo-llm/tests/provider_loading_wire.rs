//! Provider-only loopback coverage. The engine owns catalog selection and
//! validation/execution of discovery results. Here we supply its selected
//! schemas/history explicitly and verify every wire adapter preserves them.
use bamboo_config::{Config, ProviderInstanceConfig};
use bamboo_domain::{CapabilityLoadingMode, FunctionSchema, Message, ToolCall, ToolSchema};
use bamboo_llm::{
    prompt_ir::{PromptIR, Segment, SegmentRole},
    provider_factory::create_provider_from_instance,
    LLMChunk, LLMProvider,
};
use futures::StreamExt;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, Request, ResponseTemplate,
};

#[derive(Clone, Copy, Debug)]
enum Wire {
    Chat,
    Responses,
    Messages,
    Gemini,
}

fn schema(name: &str) -> ToolSchema {
    ToolSchema {
        schema_type: "function".into(),
        function: FunctionSchema {
            name: name.into(),
            description: format!("Fixture {name}"),
            parameters: json!({"type":"object","properties":{"query":{"type":"string"}}}),
        },
    }
}

fn sse(wire: Wire, name: &str) -> String {
    let args = json!({"query":"fixture"});
    let event = |kind: &str, value: Value| format!("event: {kind}\ndata: {value}\n\n");
    match wire {
        Wire::Chat => format!(
            "data: {}\n\ndata: [DONE]\n\n",
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_fixture","type":"function","function":{"name":name,"arguments":args.to_string()}}]},"finish_reason":"tool_calls"}]})
        ),
        Wire::Responses => {
            event(
                "response.output_item.done",
                json!({"type":"response.output_item.done","output_index":0,"item":{"id":"item_fixture","type":"function_call","call_id":"call_fixture","name":name,"arguments":args.to_string()}}),
            ) + &event(
                "response.completed",
                json!({"type":"response.completed","response":{"id":"resp_fixture"}}),
            )
        }
        Wire::Messages => {
            event(
                "content_block_start",
                json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call_fixture","name":name,"input":{}}}),
            ) + &event(
                "content_block_delta",
                json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":args.to_string()}}),
            ) + &event(
                "content_block_stop",
                json!({"type":"content_block_stop","index":0}),
            ) + &event(
                "message_delta",
                json!({"type":"message_delta","delta":{"stop_reason":"tool_use"}}),
            ) + &event("message_stop", json!({"type":"message_stop"}))
        }
        Wire::Gemini => format!(
            "data: {}\n\n",
            json!({"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":name,"args":args}}]},"finishReason":"STOP"}]})
        ),
    }
}

fn wire_names(body: &Value, wire: Wire) -> Vec<String> {
    let list: Vec<&Value> = match wire {
        Wire::Gemini => body["tools"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|tool| tool["functionDeclarations"].as_array().unwrap())
            .collect(),
        _ => body["tools"].as_array().unwrap().iter().collect(),
    };
    list.into_iter()
        .map(|tool| match wire {
            Wire::Chat => tool["function"]["name"].as_str().unwrap().to_string(),
            _ => tool["name"].as_str().unwrap().to_string(),
        })
        .collect()
}

async fn calls(
    provider: &dyn LLMProvider,
    messages: &[Message],
    tools: &[ToolSchema],
) -> Vec<ToolCall> {
    // Match the engine entry point: the legacy chat_stream compatibility API
    // intentionally does not select the runtime capability-loading mode.
    let ir = PromptIR {
        segments: vec![Segment::new(SegmentRole::Conversation, messages.to_vec())],
        ..Default::default()
    };
    let mut stream = provider
        .chat_stream_ir(&ir, tools, Some(2048), "fixture-model", None)
        .await
        .unwrap();
    let mut calls = Vec::new();
    while let Some(chunk) = stream.next().await {
        match chunk.unwrap() {
            LLMChunk::ToolCalls(items) => calls.extend(items),
            LLMChunk::ToolCallsIndexed(items) => {
                calls.extend(items.into_iter().map(|(_, call)| call))
            }
            _ => {}
        }
    }
    calls
}

#[tokio::test]
async fn factory_fallback_routes_preserve_small_catalog_and_discovery_history_on_wire() {
    for (kind, target, wire, endpoint) in [
        ("openai", "", Wire::Chat, "/chat/completions"),
        ("openai", "", Wire::Responses, "/responses"),
        ("anthropic", "", Wire::Messages, "/messages"),
        (
            "gemini",
            "",
            Wire::Gemini,
            "/models/fixture-model:streamGenerateContent",
        ),
        (
            "bodhi",
            "openai",
            Wire::Chat,
            "/proxy/openai/v1/chat/completions",
        ),
        (
            "bodhi",
            "anthropic",
            Wire::Messages,
            "/proxy/anthropic/v1/messages",
        ),
        (
            "bodhi",
            "gemini",
            Wire::Gemini,
            "/proxy/gemini/v1beta/models/fixture-model:streamGenerateContent",
        ),
    ] {
        let server = MockServer::start().await;
        let count = Arc::new(AtomicUsize::new(0));
        Mock::given(method("POST"))
            .and(path(endpoint))
            .respond_with(move |_: &Request| {
                let name = if count.fetch_add(1, Ordering::SeqCst) == 0 {
                    "discover_capabilities"
                } else {
                    "fixture_lookup"
                };
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(sse(wire, name))
            })
            .expect(3)
            .mount(&server)
            .await;
        let mut config_json = json!({"provider_type":kind,"api_key":"fixture-key","base_url":server.uri(),"enabled":true,"target_provider":target});
        if matches!(wire, Wire::Responses) {
            config_json["responses_only_models"] = json!(["fixture-model"]);
        }
        let instance: ProviderInstanceConfig = serde_json::from_value(config_json.clone()).unwrap();
        let provider =
            create_provider_from_instance(&Config::default(), &instance, std::env::temp_dir())
                .await
                .unwrap();
        assert_eq!(
            provider
                .capability_loading_mode("fixture-model", None)
                .await,
            CapabilityLoadingMode::StickyFallbackForRootChat,
            "{kind}/{target}/{wire:?}"
        );
        let selected = vec![schema("core_fixture"), schema("discover_capabilities")];
        let mut messages = vec![Message::user("Use fixture lookup after discovering it")];
        let initial_calls = calls(provider.as_ref(), &messages, &selected).await;
        assert!(
            initial_calls
                .iter()
                .any(|call| call.function.name == "discover_capabilities"),
            "{wire:?}"
        );
        let discovery_call = initial_calls
            .into_iter()
            .find(|call| call.function.name == "discover_capabilities")
            .unwrap();
        let discovery_id = discovery_call.id.clone();
        messages.push(Message::assistant("", Some(vec![discovery_call])));
        // This mimics a validated engine discovery result; it is not an engine
        // integration test or a claim that arbitrary history grants authority.
        messages.push(Message::tool_result(
            discovery_id,
            json!({"tools":[schema("fixture_lookup")]}).to_string(),
        ));
        let loaded_calls = calls(provider.as_ref(), &messages, &selected).await;
        assert!(
            loaded_calls
                .iter()
                .any(|call| call.function.name == "fixture_lookup"),
            "{kind}/{target}/{wire:?}"
        );

        // Explicit false keeps the complete supplied catalog on the wire.
        config_json["sticky_tool_loading"] = json!(false);
        let disabled: ProviderInstanceConfig = serde_json::from_value(config_json).unwrap();
        let disabled =
            create_provider_from_instance(&Config::default(), &disabled, std::env::temp_dir())
                .await
                .unwrap();
        assert_eq!(
            disabled
                .capability_loading_mode("fixture-model", None)
                .await,
            CapabilityLoadingMode::LegacyFullCatalog
        );
        let mut full = selected.clone();
        full.push(schema("fixture_lookup"));
        calls(disabled.as_ref(), &[Message::user("lookup")], &full).await;

        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 3);
        let bodies: Vec<Value> = requests
            .iter()
            .map(|request| serde_json::from_slice(&request.body).unwrap())
            .collect();
        for body in &bodies[..2] {
            assert_eq!(
                wire_names(body, wire),
                vec!["core_fixture", "discover_capabilities"],
                "{kind}/{target}/{wire:?}"
            );
            assert!(!body.to_string().contains("defer_loading"));
            assert!(!body.to_string().contains("tool_search_tool"));
        }
        assert!(!bodies[0].to_string().contains("fixture_lookup"));
        assert!(bodies[1].to_string().contains("fixture_lookup"));
        assert_eq!(
            wire_names(&bodies[2], wire),
            vec!["core_fixture", "discover_capabilities", "fixture_lookup"]
        );
    }
}
