use bamboo_domain::{
    FunctionCall, FunctionSchema, Message, ReasoningEffort, ToolCall, ToolResultImage, ToolSchema,
};
use futures::TryStreamExt;
use reqwest::Client;
use serde_json::{json, Value};
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, ResponseTemplate,
};

use super::OpenAIProvider;
use crate::providers::BodhiProvider;
use crate::{
    provider::{LLMProvider, LLMRequestOptions, ResponsesRequestOptions},
    types::LLMChunk,
};

#[tokio::test]
async fn responses_wire_types_preserve_calls_reasoning_cache_and_unknown_events() {
    const MODEL: &str = "fixture-responses";
    const SSE: &str = concat!(
        r#"data: {"type":"response.created","response":{"id":"resp_fixture","model":"fixture-responses","output":[]}}"#,
        "\n\n",
        r#"data: {"type":"response.vendor_future","detail":{"kept":[1,null,"future"]}}"#,
        "\n\n",
        "event: response.reasoning_summary_text.delta\n",
        r#"data: {"item_id":"reason-1","output_index":0,"summary_index":0,"delta":"Checking both cities."}"#,
        "\n\n",
        r#"data: {"type":"response.output_item.added","output_index":1,"item":{"type":"function_call","id":"item-a","call_id":"call-a","name":"lookup","arguments":""}}"#,
        "\n\n",
        r#"data: {"type":"response.output_item.added","output_index":2,"item":{"type":"function_call","id":"item-b","call_id":"call-b","name":"lookup","arguments":""}}"#,
        "\n\n",
        r#"data: {"type":"response.function_call_arguments.delta","item_id":"item-a","delta":"{\"city\":\"Shanghai\"}"}"#,
        "\n\n",
        r#"data: {"type":"response.output_item.done","output_index":1,"item":{"type":"function_call","id":"item-a","call_id":"call-a"}}"#,
        "\n\n",
        r#"data: {"type":"response.completed","response":{"id":"resp_fixture","model":"fixture-responses","output":[{"type":"reasoning","id":"reason-1","summary":[]},{"type":"function_call","id":"item-a","call_id":"call-a","name":"lookup","arguments":"{\"city\":\"Shanghai\"}"},{"type":"function_call","id":"item-b","call_id":"call-b","name":"lookup","arguments":"{\"city\":\"Suzhou\"}"}],"usage":{"input_tokens":80,"output_tokens":20,"total_tokens":100,"input_tokens_details":{"cached_tokens":40},"output_tokens_details":{"reasoning_tokens":6}}}}"#,
        "\n\n",
    );
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/responses"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(SSE, "text/event-stream"))
        .expect(2)
        .mount(&server)
        .await;
    let provider = || {
        OpenAIProvider::new("fixture-key")
            .with_client(Client::builder().no_proxy().build().unwrap())
            .with_base_url(server.uri())
            .with_responses_only_models(vec![MODEL.into()])
    };
    let options = LLMRequestOptions {
        reasoning_effort: Some(ReasoningEffort::Medium),
        responses: Some(ResponsesRequestOptions {
            retain_protocol_events: true,
            ..Default::default()
        }),
        ..Default::default()
    };
    let messages = [Message::user("Check both cities.")];
    let tools = [ToolSchema {
        schema_type: "function".into(),
        function: FunctionSchema {
            name: "lookup".into(),
            description: "Look up a city.".into(),
            parameters: json!({"type":"object","properties":{"city":{"type":"string"}}}),
        },
    }];
    let mut streams = Vec::new();
    for provider in [provider(), provider().with_litellm_responses_events()] {
        let chunks: Vec<_> = provider
            .chat_stream_with_options(&messages, &tools, Some(400), MODEL, Some(&options))
            .await
            .unwrap()
            .try_filter(|chunk| {
                futures::future::ready(!matches!(chunk, LLMChunk::TransportActivity))
            })
            .try_collect()
            .await
            .unwrap();
        streams.push(chunks);
    }
    let typed = &streams[1];
    assert_eq!(format!("{typed:#?}"), format!("{:#?}", streams[0]));
    assert!(typed.iter().any(|chunk| matches!(
        chunk, LLMChunk::ResponsesEvent { event_type, data }
            if event_type == "response.vendor_future"
                && **data == json!({"type":"response.vendor_future","detail":{"kept":[1,null,"future"]}})
    )));
    assert!(typed.iter().any(|chunk| matches!(
        chunk, LLMChunk::ReasoningToken(text) if text == "Checking both cities."
    )));
    let calls = typed
        .iter()
        .filter_map(|chunk| match chunk {
            LLMChunk::ToolCalls(calls) => Some(calls.iter()),
            _ => None,
        })
        .flatten()
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        calls,
        [
            lookup_call("call-a", r#"{"city":"Shanghai"}"#),
            lookup_call("call-b", r#"{"city":"Suzhou"}"#),
        ]
    );
    assert!(typed.iter().any(|chunk| matches!(
        chunk,
        LLMChunk::ProviderUsage {
            input_tokens: Some(80),
            output_tokens: Some(20),
            reasoning_tokens: Some(6),
            cache_read_input_tokens: Some(40),
            total_tokens: Some(100),
            ..
        }
    )));
    assert!(matches!(typed.last(), Some(LLMChunk::Done)));
    let requests = server.received_requests().await.unwrap();
    let original: Value = requests[0].body_json().unwrap();
    assert_eq!(requests[1].body_json::<Value>().unwrap(), original);
    assert_eq!(original["reasoning"]["effort"], "medium");
}

const CHAT_TOOL_STREAM: &str = concat!(
    r#"data: {"choices":[{"delta":{"reasoning_content":"Check Shanghai."}}]}"#,
    "\n\n",
    r#"data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_new","type":"function","function":{"name":"lookup","arguments":"{\"city\":"}}]}}]}"#,
    "\n\n",
    r#"data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"Shanghai\"}"}}]}}]}"#,
    "\n\n",
    r#"data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}"#,
    "\n\n",
    r#"data: {"choices":[],"usage":{"prompt_tokens":128,"completion_tokens":32,"total_tokens":160,"prompt_tokens_details":{"cached_tokens":64},"completion_tokens_details":{"reasoning_tokens":8}}}"#,
    "\n\ndata: [DONE]\n\n",
);

fn lookup_call(id: &str, arguments: &str) -> ToolCall {
    ToolCall {
        id: id.into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "lookup".into(),
            arguments: arguments.into(),
        },
    }
}

fn assert_chat_tool_stream(chunks: Vec<LLMChunk>, direct: bool) {
    let chunks: Vec<_> = chunks
        .into_iter()
        .filter(|chunk| !matches!(chunk, LLMChunk::TransportActivity))
        .collect();
    let [first, LLMChunk::ToolCallsIndexed(start), LLMChunk::ToolCallsIndexed(continuation), LLMChunk::ProviderUsage {
        input_tokens: Some(128),
        output_tokens: Some(32),
        total_tokens: Some(160),
        reasoning_tokens: Some(8),
        cache_read_input_tokens: Some(64),
        ..
    }, LLMChunk::Done] = chunks.as_slice()
    else {
        panic!("unexpected Chat Completions chunks: {chunks:?}");
    };
    if direct {
        assert!(
            matches!(first, LLMChunk::ReasoningToken(reasoning) if reasoning == "Check Shanghai.")
        );
    } else {
        assert!(matches!(first, LLMChunk::Token(text) if text.is_empty()));
    }
    assert_eq!(start, &[(0, lookup_call("call_new", r#"{"city":"#))]);
    assert_eq!(continuation.len(), 1);
    assert_eq!(continuation[0].0, 0);
    assert!(continuation[0].1.id.is_empty());
    let arguments = format!(
        "{}{}",
        start[0].1.function.arguments, continuation[0].1.function.arguments
    );
    assert_eq!(
        serde_json::from_str::<Value>(&arguments).unwrap(),
        json!({"city":"Shanghai"})
    );
}

#[tokio::test]
async fn chat_messages_match_default_for_tool_calls_and_all_reasoning_efforts() {
    let efforts = [
        None,
        Some(ReasoningEffort::Disabled),
        Some(ReasoningEffort::Low),
        Some(ReasoningEffort::Medium),
        Some(ReasoningEffort::High),
        Some(ReasoningEffort::Xhigh),
        Some(ReasoningEffort::Max),
    ];
    let server = MockServer::start().await;
    for route in ["/chat/completions", "/proxy/openai/v1/chat/completions"] {
        Mock::given(method("POST"))
            .and(path(route))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(CHAT_TOOL_STREAM, "text/event-stream"),
            )
            .expect(2 * efforts.len() as u64)
            .mount(&server)
            .await;
    }

    let client = Client::builder().no_proxy().build().unwrap();
    let provider = || {
        OpenAIProvider::new("fixture-key")
            .with_base_url(server.uri())
            .with_client(client.clone())
    };
    let baseline = provider();
    assert!(!baseline.litellm_chat_messages);
    let candidate = provider().with_litellm_chat_messages();
    let proxy = || {
        BodhiProvider::new("fixture-key")
            .with_base_url(server.uri())
            .with_client(client.clone())
    };
    let proxy_baseline = proxy();
    let proxy_candidate = proxy().with_litellm_wire_types();
    let providers: [&dyn LLMProvider; 4] =
        [&baseline, &candidate, &proxy_baseline, &proxy_candidate];
    let previous_calls = vec![
        lookup_call("call_first", r#"{"city":"Shanghai"}"#),
        lookup_call("call_second", r#"{"city":"Hangzhou"}"#),
    ];
    let image = |data: &str| ToolResultImage {
        mime_type: "image/png".into(),
        data: data.into(),
    };
    let messages = [
        Message::system("Use the lookup tool."),
        Message::user("Compare the cities."),
        Message::assistant_with_reasoning(
            "",
            Some(previous_calls.clone()),
            Some("Local history.".into()),
        ),
        Message::tool_result_with_images("call_first", "Cloudy", true, vec![image("AAAA")]),
        Message::tool_result_with_images("call_second", "Sunny", true, vec![image("BBBB")]),
        Message::user("Refresh Shanghai."),
    ];
    let tools = [ToolSchema {
        schema_type: "function".into(),
        function: FunctionSchema {
            name: "lookup".into(),
            description: "Get the forecast.".into(),
            parameters: json!({"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}),
        },
    }];

    for effort in efforts {
        let options = LLMRequestOptions {
            reasoning_effort: effort,
            parallel_tool_calls: Some(true),
            required_tool: Some("lookup".into()),
            ..Default::default()
        };
        for (index, provider) in providers.into_iter().enumerate() {
            let chunks = provider
                .chat_stream_with_options(
                    &messages,
                    &tools,
                    Some(4096),
                    "fixture-chat",
                    Some(&options),
                )
                .await
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            assert_chat_tool_stream(chunks, index < 2);
        }
    }

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 4 * efforts.len());
    let (requests_by_effort, _) = requests.as_chunks::<4>();
    for (effort, [baseline_request, candidate_request, proxy_baseline, proxy_candidate]) in
        efforts.into_iter().zip(requests_by_effort)
    {
        let native = baseline_request.body_json::<Value>().unwrap();
        let typed = candidate_request.body_json::<Value>().unwrap();
        assert_eq!(typed, native, "{effort:?}");
        assert_eq!(
            proxy_candidate.body_json::<Value>().unwrap(),
            proxy_baseline.body_json::<Value>().unwrap()
        );
        assert_eq!(proxy_candidate.body_json::<Value>().unwrap(), typed);
        assert_eq!(
            typed.get("reasoning_effort").and_then(Value::as_str),
            effort.map(ReasoningEffort::as_str)
        );
        let (limit, absent) = if effort.is_some() {
            ("max_completion_tokens", "max_tokens")
        } else {
            ("max_tokens", "max_completion_tokens")
        };
        assert_eq!(typed[limit], 4096);
        assert!(typed.get(absent).is_none());
        assert_eq!(typed["tools"][0]["function"]["name"], "lookup");
        assert_eq!(
            typed["tool_choice"],
            json!({"type":"function","function":{"name":"lookup"}})
        );
        assert_eq!(typed["parallel_tool_calls"], true);
        assert_eq!(typed["stream_options"]["include_usage"], true);

        let wire = typed["messages"].as_array().unwrap();
        assert_eq!(wire.len(), 7);
        assert_eq!(
            wire[2],
            json!({"role":"assistant","content":"","tool_calls":previous_calls})
        );
        assert_eq!(
            wire[3],
            json!({"role":"tool","tool_call_id":"call_first","content":"Cloudy"})
        );
        assert_eq!(
            wire[4],
            json!({"role":"tool","tool_call_id":"call_second","content":"Sunny"})
        );
        assert_eq!(wire[5]["role"], "user");
        assert_eq!(
            wire[5]["content"][1]["image_url"]["url"],
            "data:image/png;base64,AAAA"
        );
        assert_eq!(
            wire[5]["content"][2]["image_url"]["url"],
            "data:image/png;base64,BBBB"
        );
        assert_eq!(
            wire[6],
            json!({"role":"user","content":"Refresh Shanghai."})
        );
    }
}
