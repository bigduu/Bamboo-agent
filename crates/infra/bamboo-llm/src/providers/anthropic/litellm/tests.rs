use bamboo_config::RequestOverridesConfig;
use bamboo_domain::{
    CapabilityLoadingMode, ContextBlockType, FunctionCall, FunctionSchema, Message, PromptBlock,
    ProviderFamily, ProviderProtocol, ProviderTranscriptAuthor, ProviderTranscriptItem,
    ProviderTranscriptOrigin, Session, ToolCall, ToolSchema,
};
use futures::TryStreamExt;
use serde_json::{json, Value};
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, ResponseTemplate,
};

use super::super::{
    prepare_anthropic_content, serialize_anthropic_request, AnthropicProvider,
    PreparedAnthropicTurn, ANTHROPIC_TOOL_SEARCH_TYPE, EXTENDED_CACHE_TTL_BETA,
};
use super::{clamp_thinking_budget, messages_request, PreparedAnthropicContent, ReasoningEffort};
use crate::cache::{CacheTtl, PromptCachePlan};
use crate::prompt_ir::{PromptIR, Segment, SegmentRole};
use crate::provider::{
    scope_completion_diagnostics, CompletionDiagnostics, LLMError, LLMProvider, LLMRequestOptions,
};
use crate::types::LLMChunk;

const MODEL: &str = "claude-opus-4-8";
const LEGACY_MODEL: &str = "claude-sonnet-4-5";
const RESPONSE: &str = concat!(include_str!("messages.sse"), "\n");

fn local_provider(server: &MockServer) -> AnthropicProvider {
    AnthropicProvider::new("fixture-key")
        .with_base_url(server.uri())
        .with_client(reqwest::Client::builder().no_proxy().build().unwrap())
}

fn configured_override(path: &str, value: Value) -> RequestOverridesConfig {
    serde_json::from_value(json!({"endpoints":{"messages":{"body_patch":[
        {"op":"set","path":path,"value":value}
    ]}}}))
    .unwrap()
}

fn tool_schema(name: &str) -> ToolSchema {
    ToolSchema {
        schema_type: "function".into(),
        function: FunctionSchema {
            name: name.into(),
            description: "Fetch the weather for a city".into(),
            parameters: json!({"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}),
        },
    }
}

fn forecast_call(id: &str) -> ToolCall {
    ToolCall {
        id: id.into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "get_weather".into(),
            arguments: json!({"city": id}).to_string(),
        },
    }
}

fn semantic_chunks(chunks: &[LLMChunk]) -> Vec<String> {
    chunks
        .iter()
        .filter(|chunk| !matches!(chunk, LLMChunk::TransportActivity))
        .map(|chunk| format!("{chunk:?}"))
        .collect()
}

#[tokio::test]
async fn native_messages_matches_tools_cache_and_stream_usage() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(RESPONSE, "text/event-stream"))
        .expect(2)
        .mount(&server)
        .await;

    let user = Message::user("Check the forecast for Shanghai.");
    let cache = PromptCachePlan {
        cache_tools: true,
        cache_system: true,
        breakpoint_message_ids: vec![user.id.clone()],
        ttl: CacheTtl::Extended,
        ..Default::default()
    };
    let previous_tool = ToolCall {
        id: "toolu_previous".into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "get_weather".into(),
            arguments: r#"{"city":"Shanghai"}"#.into(),
        },
    };
    let assistant = Message::assistant_with_reasoning(
        "",
        Some(vec![previous_tool]),
        Some("Look it up.".into()),
    )
    .with_reasoning_signature(Some("fixture_history_signature".into()));
    let ir = PromptIR {
        system_blocks: vec![PromptBlock::new(
            "base",
            ContextBlockType::Base,
            "Use the weather tool.",
        )],
        segments: vec![Segment::new(
            SegmentRole::Conversation,
            vec![
                user,
                assistant,
                Message::tool_result("toolu_previous", "Cloudy"),
            ],
        )],
        cache: cache.clone(),
        ..Default::default()
    };
    let options = LLMRequestOptions {
        reasoning_effort: Some(ReasoningEffort::Xhigh),
        parallel_tool_calls: Some(false),
        cache: Some(cache),
        ..Default::default()
    };
    let tools = [tool_schema("get_weather")];
    let baseline: Vec<_> = local_provider(&server)
        .chat_stream_ir(&ir, &tools, Some(4096), MODEL, Some(&options))
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let candidate: Vec<_> = local_provider(&server)
        .with_litellm_messages()
        .chat_stream_ir(&ir, &tools, Some(4096), MODEL, Some(&options))
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    let requests = server.received_requests().await.unwrap();
    let [baseline_request, candidate_request] = requests.as_slice() else {
        panic!("expected one POST per path");
    };
    for name in ["anthropic-version", "anthropic-beta", "x-api-key"] {
        assert_eq!(
            baseline_request.headers.get(name),
            candidate_request.headers.get(name)
        );
    }
    assert_eq!(
        candidate_request.headers["anthropic-beta"],
        EXTENDED_CACHE_TTL_BETA
    );
    let mut expected = baseline_request.body_json::<Value>().unwrap();
    let actual = candidate_request.body_json::<Value>().unwrap();
    assert_eq!(expected["thinking"], json!({"type":"adaptive"}));
    expected["thinking"]["display"] = json!("summarized");
    assert_eq!(actual, expected);
    assert_eq!(actual["stream"], true);
    assert_eq!(actual["output_config"]["effort"], "xhigh");
    assert_eq!(actual["tool_choice"]["disable_parallel_tool_use"], true);
    assert_eq!(
        actual["messages"][1]["content"][0]["signature"],
        "fixture_history_signature"
    );
    assert_eq!(actual["messages"][1]["content"][1]["id"], "toolu_previous");
    assert_eq!(
        actual["messages"][2]["content"][0]["tool_use_id"],
        "toolu_previous"
    );
    for path in ["/tools/0", "/system/0", "/messages/0/content/0"] {
        assert_eq!(
            actual.pointer(&format!("{path}/cache_control")),
            Some(&json!({"type":"ephemeral","ttl":"1h"}))
        );
    }

    assert_eq!(semantic_chunks(&candidate), semantic_chunks(&baseline));
    assert!(candidate.iter().any(|chunk| matches!(chunk,
        LLMChunk::ReasoningSignature(signature) if signature == "fixture_stream_signature"
    )));
    let calls: Vec<_> = candidate
        .iter()
        .filter_map(|chunk| match chunk {
            LLMChunk::ToolCalls(calls) => Some(calls),
            _ => None,
        })
        .flatten()
        .collect();
    assert_eq!(calls.len(), 3);
    assert!(calls
        .iter()
        .all(|call| call.id == "toolu_new" && call.function.name == "get_weather"));
    let arguments: String = calls
        .iter()
        .map(|call| call.function.arguments.as_str())
        .collect();
    assert_eq!(
        serde_json::from_str::<Value>(&arguments).unwrap(),
        json!({"city":"Shanghai"})
    );
    assert!(matches!(candidate.last(), Some(LLMChunk::Done)));
    let usage: Vec<_> = candidate
        .iter()
        .filter(|chunk| matches!(chunk, LLMChunk::ProviderUsage { .. }))
        .collect();
    assert!(matches!(
        usage.as_slice(),
        [LLMChunk::ProviderUsage {
            input_tokens: Some(187),
            output_tokens: Some(33),
            reasoning_tokens: Some(9),
            cache_creation_input_tokens: Some(120),
            cache_read_input_tokens: Some(50),
            ..
        }]
    ));
}

#[test]
fn claude_reasoning_matches_pinned_messages_transform() {
    use ReasoningEffort::{Disabled, High, Max, Xhigh};

    // Outputs observed from the pinned Rust transform with model capabilities
    // from its bundled catalog and the default thinking budgets.
    let cases = [
        (
            MODEL,
            8192,
            Some(Xhigh),
            json!({"thinking":{"type":"adaptive","display":"summarized"},"output_config":{"effort":"xhigh"}}),
        ),
        (
            "claude-opus-4-6",
            8192,
            Some(Max),
            json!({"thinking":{"type":"adaptive","display":"summarized"},"output_config":{"effort":"max"}}),
        ),
        (
            LEGACY_MODEL,
            8192,
            Some(High),
            json!({"thinking":{"type":"enabled","budget_tokens":4096}}),
        ),
        (
            "claude-sonnet-4-5-20250929",
            2048,
            Some(High),
            json!({"thinking":{"type":"enabled","budget_tokens":2047}}),
        ),
        (
            LEGACY_MODEL,
            1025,
            Some(High),
            json!({"thinking":{"type":"enabled","budget_tokens":1024}}),
        ),
        (LEGACY_MODEL, 8192, Some(Disabled), json!({})),
        (MODEL, 8192, Some(Disabled), json!({})),
        (
            "claude-opus-5",
            8192,
            Some(Disabled),
            json!({"thinking":{"type":"disabled"}}),
        ),
        ("claude-opus-5-5", 8192, Some(Disabled), json!({})),
        ("claude-opus-5", 8192, None, json!({})),
    ];
    for (model, max_tokens, effort, expected_controls) in cases {
        let content = || {
            prepare_anthropic_content(
                &[Message::user("Hello")],
                &[],
                &[],
                false,
                None,
                None,
                false,
                &[],
                CapabilityLoadingMode::LegacyFullCatalog,
                false,
            )
        };
        let mut expected = serialize_anthropic_request(content(), model, max_tokens, true, None);
        expected
            .as_object_mut()
            .unwrap()
            .extend(expected_controls.as_object().unwrap().clone());
        let request = messages_request(content(), model, max_tokens, true, effort).unwrap();
        let mut body = serde_json::to_value(request).unwrap();
        if let Some(budget) = body
            .pointer("/thinking/budget_tokens")
            .and_then(Value::as_u64)
        {
            clamp_thinking_budget(&mut body, budget).unwrap();
        }
        assert_eq!(body, expected, "{model} {effort:?} at {max_tokens}");
    }
}

#[tokio::test]
async fn legacy_budget_reaches_wire_only_through_the_opted_in_path() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(RESPONSE, "text/event-stream"))
        .expect(5)
        .mount(&server)
        .await;
    let options = LLMRequestOptions {
        reasoning_effort: Some(ReasoningEffort::High),
        ..Default::default()
    };
    let providers = [
        local_provider(&server),
        local_provider(&server).with_litellm_messages(),
        local_provider(&server)
            .with_litellm_messages()
            .with_request_overrides(Some(configured_override("/max_tokens", json!(2048)))),
        local_provider(&server)
            .with_litellm_messages()
            .with_request_overrides(Some(configured_override(
                "/thinking/budget_tokens",
                json!(3072),
            ))),
    ];
    let send = |provider: AnthropicProvider| {
        let options = &options;
        async move {
            let _: Vec<_> = provider
                .chat_stream_with_options(
                    &[Message::user("Hello")],
                    &[],
                    Some(8192),
                    LEGACY_MODEL,
                    Some(options),
                )
                .await
                .unwrap()
                .try_collect()
                .await
                .unwrap();
        }
    };
    for provider in providers {
        send(provider).await;
    }
    let diagnostics =
        CompletionDiagnostics::for_agent_round(None).with_model_window_limit(Some(1536));
    scope_completion_diagnostics(
        diagnostics,
        send(local_provider(&server).with_litellm_messages()),
    )
    .await;
    let bodies: Vec<Value> = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .map(|request| request.body_json().unwrap())
        .collect();
    let [native, typed, clamped, overridden, agent_window] = bodies.as_slice() else {
        panic!("expected all five request bodies");
    };
    assert_eq!(native["thinking"], json!({"type":"adaptive"}));
    assert_eq!(native["output_config"]["effort"], "high");
    assert_eq!(
        typed["thinking"],
        json!({"type":"enabled","budget_tokens":4096})
    );
    assert!(typed.get("output_config").is_none());
    assert_eq!(
        clamped["thinking"],
        json!({"type":"enabled","budget_tokens":2047})
    );
    assert_eq!(clamped["max_tokens"], 2048);
    assert!(clamped.get("output_config").is_none());
    assert_eq!(overridden, native);
    assert_eq!(agent_window["max_tokens"], 1536);
    assert_eq!(
        agent_window["thinking"],
        json!({"type":"enabled","budget_tokens":1535})
    );
}

#[tokio::test]
async fn claude_reasoning_rejects_unsupported_controls_before_sending() {
    let server = MockServer::start().await;
    let provider = local_provider(&server).with_litellm_messages();
    for (model, max_tokens, effort, reason) in [
        (
            "claude-opus-4-6",
            8192,
            ReasoningEffort::Xhigh,
            "does not support xhigh",
        ),
        (
            "anthropic/claude-opus-4-6-20260205",
            8192,
            ReasoningEffort::Xhigh,
            "does not support xhigh",
        ),
        (
            LEGACY_MODEL,
            1024,
            ReasoningEffort::High,
            "requires max_tokens above 1024",
        ),
    ] {
        let options = LLMRequestOptions {
            reasoning_effort: Some(effort),
            ..Default::default()
        };
        let result = provider
            .chat_stream_with_options(
                &[Message::user("Hello")],
                &[],
                Some(max_tokens),
                model,
                Some(&options),
            )
            .await;
        assert!(matches!(result, Err(LLMError::Api(message)) if message.contains(reason)));
    }
    let diagnostics =
        CompletionDiagnostics::for_agent_round(None).with_model_window_limit(Some(1024));
    let options = LLMRequestOptions {
        reasoning_effort: Some(ReasoningEffort::High),
        ..Default::default()
    };
    let result = scope_completion_diagnostics(
        diagnostics,
        provider.chat_stream_with_options(
            &[Message::user("Hello")],
            &[],
            Some(8192),
            LEGACY_MODEL,
            Some(&options),
        ),
    )
    .await;
    assert!(
        matches!(result, Err(LLMError::Api(message)) if message.contains("requires max_tokens above 1024"))
    );
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn effective_overrides_take_precedence_over_initial_effort_and_cap() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(RESPONSE, "text/event-stream"))
        .expect(3)
        .mount(&server)
        .await;

    for (model, max_tokens, effort, patch, value) in [
        (
            "claude-opus-4-6",
            8192,
            ReasoningEffort::Xhigh,
            "/output_config/effort",
            json!("high"),
        ),
        (
            LEGACY_MODEL,
            1024,
            ReasoningEffort::High,
            "/max_tokens",
            json!(2048),
        ),
        (
            LEGACY_MODEL,
            8192,
            ReasoningEffort::High,
            "/tool_choice",
            json!({"type":"auto"}),
        ),
    ] {
        let provider = local_provider(&server)
            .with_litellm_messages()
            .with_request_overrides(Some(configured_override(patch, value)));
        let options = LLMRequestOptions {
            reasoning_effort: Some(effort),
            ..Default::default()
        };
        let _: Vec<_> = provider
            .chat_stream_with_options(
                &[Message::user("Hello")],
                &[tool_schema("get_weather")],
                Some(max_tokens),
                model,
                Some(&options),
            )
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
    }
    let requests = server.received_requests().await.unwrap();
    let reduced_effort: Value = requests[0].body_json().unwrap();
    assert_eq!(reduced_effort["output_config"]["effort"], "high");
    let raised_cap: Value = requests[1].body_json().unwrap();
    assert_eq!(raised_cap["max_tokens"], 2048);
    assert_eq!(
        raised_cap["thinking"],
        json!({"type":"enabled","budget_tokens":2047})
    );
    let tool_choice: Value = requests[2].body_json().unwrap();
    assert_eq!(tool_choice["tool_choice"], json!({"type":"auto"}));
    assert_eq!(
        tool_choice["thinking"],
        json!({"type":"enabled","budget_tokens":4096})
    );
}

#[tokio::test]
async fn default_and_required_thinking_replay_signed_tool_history() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(RESPONSE, "text/event-stream"))
        .expect(3)
        .mount(&server)
        .await;
    let provider = local_provider(&server).with_litellm_messages();
    let signature = "fixture_history_signature";
    let messages = [
        Message::user("Check the forecast."),
        Message::assistant_with_reasoning(
            "",
            Some(vec![forecast_call("toolu_previous")]),
            Some("Look it up.".into()),
        )
        .with_reasoning_signature(Some(signature.into())),
        Message::tool_result("toolu_previous", "Sunny"),
    ];
    let expected = [
        ("claude-opus-5", None, true),
        ("claude-opus-5-5", Some(ReasoningEffort::Disabled), true),
        ("claude-opus-4-6", Some(ReasoningEffort::Disabled), false),
    ];
    for (model, effort, _) in expected {
        let options = LLMRequestOptions {
            reasoning_effort: effort,
            ..Default::default()
        };
        let _: Vec<_> = provider
            .chat_stream_with_options(
                &messages,
                &[tool_schema("get_weather")],
                Some(4096),
                model,
                Some(&options),
            )
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
    }
    let requests = server.received_requests().await.unwrap();
    for (request, (_, _, replay)) in requests.iter().zip(expected) {
        let body: Value = request.body_json().unwrap();
        let assistant = body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .find(|turn| turn["role"] == "assistant")
            .unwrap();
        assert_eq!(
            assistant["content"]
                .as_array()
                .unwrap()
                .iter()
                .any(|block| block["signature"] == signature),
            replay
        );
    }
}

#[test]
fn native_tool_search_declarations_preserve_bamboo_wire_shape() {
    let content = || {
        prepare_anthropic_content(
            &[Message::user("Find a tool")],
            &[],
            &[tool_schema("Glob")],
            false,
            None,
            None,
            false,
            &[],
            CapabilityLoadingMode::Progressive,
            false,
        )
    };
    let body = serde_json::to_value(messages_request(content(), MODEL, 4096, true, None).unwrap())
        .unwrap();
    assert_eq!(
        body,
        serialize_anthropic_request(content(), MODEL, 4096, true, None)
    );
    assert_eq!(body["tools"][0]["defer_loading"], true);
    assert_eq!(body["tools"][1]["type"], ANTHROPIC_TOOL_SEARCH_TYPE);
    assert_eq!(body["tools"][1]["cache_control"]["type"], "ephemeral");
}

#[test]
fn messages_request_preserves_native_extensions_and_all_bamboo_efforts() {
    let native = json!({
        "type": "server_tool_use",
        "id": "srvtoolu_fixture",
        "name": "web_search",
        "input": {"query": "Shanghai"},
        "cache_control": null,
        "extension": [null, {"value": 7}]
    });
    let content = || PreparedAnthropicContent {
        system: None,
        messages: vec![PreparedAnthropicTurn {
            role: "assistant",
            content: vec![native.clone()],
        }],
        tools: Vec::new(),
        tool_choice: None,
    };
    for effort in [
        None,
        Some(ReasoningEffort::Disabled),
        Some(ReasoningEffort::Low),
        Some(ReasoningEffort::Medium),
        Some(ReasoningEffort::High),
        Some(ReasoningEffort::Xhigh),
        Some(ReasoningEffort::Max),
    ] {
        let request = messages_request(content(), "deepseek-flash", 4096, true, effort).unwrap();
        let body = serde_json::to_value(request).unwrap();
        assert_eq!(
            body,
            serialize_anthropic_request(content(), "deepseek-flash", 4096, true, effort)
        );
        assert_eq!(body["messages"][0]["content"][0], native);
    }
}

#[tokio::test]
async fn parallel_client_tool_ids_match_the_pinned_transform() {
    // These pairs come from the pinned sanitizer run on parallel tool blocks.
    let raw_ids = [
        "functions.lookup:0",
        "call_second__thought__CiIB+/a==",
        "toolu_ok-A",
    ];
    let expected_ids = ["functions_lookup_0", "call_second", "toolu_ok-A"];
    let messages = vec![
        Message::user("Compare the forecasts. The literal tag is functions.lookup:0."),
        Message::assistant("", Some(raw_ids.into_iter().map(forecast_call).collect())),
        Message::tool_result(raw_ids[2], "Third result"),
        Message::tool_result(raw_ids[0], "First result"),
        Message::tool_result(raw_ids[1], "Second result"),
    ];
    let tools = [tool_schema("get_weather")];
    let options = LLMRequestOptions {
        parallel_tool_calls: Some(true),
        ..Default::default()
    };
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(RESPONSE, "text/event-stream"))
        .expect(4)
        .mount(&server)
        .await;
    for (provider, model) in [
        (local_provider(&server), MODEL),
        (local_provider(&server).with_litellm_messages(), MODEL),
        (local_provider(&server).with_litellm_messages(), MODEL),
        (
            local_provider(&server).with_litellm_messages(),
            "deepseek-flash",
        ),
    ] {
        let _: Vec<_> = provider
            .chat_stream_with_options(&messages, &tools, Some(4096), model, Some(&options))
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
    }

    let received = server.received_requests().await.unwrap();
    let [baseline, candidate, repeated, deepseek] = received.as_slice() else {
        panic!("expected four Messages requests");
    };
    let baseline = baseline.body_json::<Value>().unwrap();
    let mut expected = baseline.clone();
    for (index, id) in expected_ids.into_iter().enumerate() {
        assert_eq!(
            baseline["messages"][1]["content"][index]["id"],
            raw_ids[index]
        );
        expected["messages"][1]["content"][index]["id"] = id.into();
    }
    for (position, index) in [2, 0, 1].into_iter().enumerate() {
        assert_eq!(
            baseline["messages"][2]["content"][position]["tool_use_id"],
            raw_ids[index],
        );
        expected["messages"][2]["content"][position]["tool_use_id"] = expected_ids[index].into();
    }
    assert_eq!(candidate.body_json::<Value>().unwrap(), expected);
    assert_eq!(repeated.body_json::<Value>().unwrap(), expected);
    let mut expected_compatible = baseline;
    expected_compatible["model"] = "deepseek-flash".into();
    assert_eq!(deepseek.body_json::<Value>().unwrap(), expected_compatible);
}

#[tokio::test]
async fn replayed_client_tools_keep_distinct_ids_and_matching_results() {
    const BOUNDARY: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let raw_ids = ["history.lookup", "history:lookup", "history_lookup"];
    let expected_ids = ["history_lookup", "history_lookup-1", "history_lookup-2"];
    let mut session = Session::new("Claude tool replay fixture", MODEL);
    session.add_message(Message::user("Replay three forecasts."));
    let assistant = Message::assistant(
        "Normalized assistant fallback",
        Some(raw_ids.into_iter().map(forecast_call).collect()),
    );
    let assistant_anchor = assistant.id.clone();
    session.add_message(assistant);
    for index in [2, 1, 0] {
        session.add_message(Message::tool_result(
            raw_ids[index],
            format!("Result {index}"),
        ));
    }
    let result_anchor = session.messages[3].id.clone();
    session
        .activate_provider_transcript_route(
            ProviderFamily::Anthropic,
            ProviderProtocol::AnthropicMessages2023_06_01,
            BOUNDARY,
        )
        .unwrap();
    let native_item = |origin, author, payload| {
        ProviderTranscriptItem::try_from_payload(
            ProviderFamily::Anthropic,
            ProviderProtocol::AnthropicMessages2023_06_01,
            origin,
            author,
            payload,
        )
        .unwrap()
    };
    let mut native_turn = vec![
        native_item(
            ProviderTranscriptOrigin::Provider,
            ProviderTranscriptAuthor::Model,
            json!({"type":"server_tool_use","id":"srvtoolu_search","name":"tool_search_tool_regex","input":{"pattern":"weather"}}),
        ),
        native_item(
            ProviderTranscriptOrigin::Provider,
            ProviderTranscriptAuthor::ToolResult,
            json!({"type":"tool_search_tool_result","tool_use_id":"srvtoolu_search",
                "content":{"type":"tool_search_tool_search_result","tool_references":[{"type":"tool_reference","tool_name":"get_weather"}]}}),
        ),
    ];
    native_turn.extend(raw_ids.into_iter().map(|id| {
        native_item(ProviderTranscriptOrigin::Provider, ProviderTranscriptAuthor::Model,
            json!({"type":"tool_use","id":id,"name":"get_weather","input":{"city":format!("Native {id}")}}))
    }));
    session
        .append_provider_transcript_group(&assistant_anchor, None, native_turn)
        .unwrap();
    session
        .append_provider_transcript_group(
            &result_anchor,
            None,
            vec![native_item(
                ProviderTranscriptOrigin::HostToolSearch,
                ProviderTranscriptAuthor::ToolResult,
                json!({"type":"tool_result","tool_use_id":raw_ids[1],
                "content":[{"type":"tool_reference","tool_name":"get_weather"}]}),
            )],
        )
        .unwrap();
    let stored_transcript = serde_json::to_value(&session.provider_transcript).unwrap();
    let groups: Vec<_> = session
        .provider_transcript
        .replayable_groups(
            ProviderFamily::Anthropic,
            ProviderProtocol::AnthropicMessages2023_06_01,
            BOUNDARY,
        )
        .into_iter()
        .cloned()
        .collect();
    assert_eq!(groups.len(), 2);

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(RESPONSE, "text/event-stream"))
        .expect(2)
        .mount(&server)
        .await;
    let tools = [tool_schema("get_weather")];
    for provider in [
        local_provider(&server),
        local_provider(&server).with_litellm_messages(),
    ] {
        // Production selects Progressive for the Claude discovery route. Use
        // that lane explicitly so the requests can run against a loopback mock.
        let _: Vec<_> = provider
            .stream_messages_inner(
                &session.messages,
                &[],
                &groups,
                &tools,
                Some(4096),
                MODEL,
                None,
                CapabilityLoadingMode::Progressive,
            )
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
    }

    let received = server.received_requests().await.unwrap();
    let [baseline, candidate] = received.as_slice() else {
        panic!("expected two native replay requests");
    };
    let mut expected = baseline.body_json::<Value>().unwrap();
    assert_eq!(
        expected["messages"][1]["content"][0]["id"],
        "srvtoolu_search"
    );
    assert_eq!(
        expected["messages"][1]["content"][1]["tool_use_id"],
        "srvtoolu_search"
    );
    assert_eq!(
        expected["messages"][1]["content"][2]["input"]["city"],
        "Native history.lookup"
    );
    assert_eq!(
        expected["messages"][2]["content"][1]["content"][0]["type"],
        "tool_reference"
    );
    for (index, id) in expected_ids.into_iter().enumerate() {
        assert_eq!(
            expected["messages"][1]["content"][index + 2]["id"],
            raw_ids[index]
        );
        assert_eq!(
            expected["messages"][2]["content"][2 - index]["tool_use_id"],
            raw_ids[index]
        );
        expected["messages"][1]["content"][index + 2]["id"] = id.into();
        expected["messages"][2]["content"][2 - index]["tool_use_id"] = id.into();
    }
    assert_eq!(candidate.body_json::<Value>().unwrap(), expected);
    assert_eq!(
        serde_json::to_value(&session.provider_transcript).unwrap(),
        stored_transcript
    );
}
