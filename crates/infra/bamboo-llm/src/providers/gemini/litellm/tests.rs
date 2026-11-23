use super::*;
use crate::provider::{LLMProvider, LLMRequestOptions};
use crate::providers::GeminiProvider;
use crate::types::LLMChunk;
use bamboo_domain::{FunctionSchema, ToolResultImage};
use futures::TryStreamExt;
use serde_json::json;
use wiremock::{
    matchers::{header, method, path, query_param},
    Mock, MockServer, ResponseTemplate,
};

const GEMINI_STREAM: &str = concat!(
    r#"data: {"candidates":[{"content":{"role":"model","parts":[{"thought":true,"text":"I should look up both cities."}]}}]}"#,
    "\n\n",
    r#"data: {"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"id":"native-a","name":"lookup","args":{"city":"Shanghai"}}},{"functionCall":{"id":"native-b","name":"lookup","args":{"city":"Suzhou"}}}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":128,"cachedContentTokenCount":64,"candidatesTokenCount":12,"thoughtsTokenCount":8,"totalTokenCount":148}}"#,
    "\n\n",
);

fn lookup_tool() -> ToolSchema {
    ToolSchema {
        schema_type: "function".into(),
        function: FunctionSchema {
            name: "lookup".into(),
            description: "Look up a city".into(),
            parameters: json!({
                "type": "object",
                "properties": { "city": { "$ref": "#/$defs/city" } },
                "$defs": { "city": { "type": "string" } },
                "additionalProperties": false,
                "required": ["city"]
            }),
        },
    }
}

#[tokio::test]
async fn typed_gemini_provider_replays_parallel_calls_and_preserves_usage() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(
            "/v1beta/models/gemini-3.5-flash:streamGenerateContent",
        ))
        .and(query_param("alt", "sse"))
        .and(header("x-goog-api-key", "fixture-key"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(GEMINI_STREAM, "text/event-stream"))
        .expect(3)
        .mount(&server)
        .await;
    let provider = || {
        GeminiProvider::new("fixture-key")
            .with_base_url(format!("{}/v1beta", server.uri()))
            .with_client(reqwest::Client::builder().no_proxy().build().unwrap())
    };
    let baseline = provider();
    assert!(!baseline.litellm_wire_types);
    let candidate = provider().with_litellm_wire_types();
    let tools = [lookup_tool()];
    let options = LLMRequestOptions {
        reasoning_effort: Some(ReasoningEffort::Low),
        required_tool: Some("lookup".into()),
        ..Default::default()
    };
    let mut messages = vec![
        Message::system("Two cities."),
        Message::user("Look up Shanghai and Suzhou."),
    ];
    let mut candidate_calls = None;
    for provider in [&baseline, &candidate] {
        let chunks: Vec<_> = provider
            .chat_stream_with_options(
                &messages,
                &tools,
                Some(400),
                "gemini-3.5-flash",
                Some(&options),
            )
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let chunks: Vec<_> = chunks
            .into_iter()
            .filter(|chunk| !matches!(chunk, LLMChunk::TransportActivity | LLMChunk::Done))
            .collect();
        let [LLMChunk::ReasoningToken(reasoning), LLMChunk::ToolCalls(calls), LLMChunk::CacheUsage {
            cache_read_input_tokens: 64,
            ..
        }, LLMChunk::UsageSummary {
            output_tokens: 12,
            thinking_tokens: 8,
        }, LLMChunk::ProviderUsageReplace(usage), LLMChunk::ProviderUsage {
            total_tokens: Some(148),
            ..
        }] = chunks.as_slice()
        else {
            panic!("unexpected Gemini chunks: {chunks:?}");
        };
        assert_eq!(reasoning, "I should look up both cities.");
        assert_eq!(usage.cache_read_input_tokens, Some(64));
        assert_eq!(usage.reasoning_tokens, Some(8));
        assert_eq!(calls.len(), 2);
        let ids = if provider.litellm_wire_types {
            candidate_calls = Some(calls.clone());
            ["native-a", "native-b"]
        } else {
            ["gemini_0", "gemini_1"]
        };
        assert_eq!(calls[0].id, ids[0]);
        assert_eq!(calls[1].id, ids[1]);
    }

    messages.extend([
        Message::assistant("", candidate_calls),
        Message::tool_result("native-b", "sunny"),
        Message::tool_result_with_images(
            "native-a",
            r#"{"weather":"cloudy"}"#,
            true,
            vec![ToolResultImage {
                mime_type: "image/png".into(),
                data: "AAAA".into(),
            }],
        ),
    ]);
    candidate
        .chat_stream(&messages, &tools, Some(400), "gemini-3.5-flash")
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    let requests = server.received_requests().await.unwrap();
    let body = |index: usize| requests[index].body_json::<Value>().unwrap();
    let original = body(0);
    let typed = body(1);
    assert_eq!(typed["contents"], original["contents"]);
    assert_eq!(typed["tools"], original["tools"]);
    assert_eq!(typed["toolConfig"], original["toolConfig"]);
    assert_eq!(
        typed["generationConfig"],
        json!({
            "maxOutputTokens": 400,
            "thinkingConfig": { "thinkingLevel": "low", "includeThoughts": true }
        })
    );
    assert_eq!(
        typed["systemInstruction"],
        json!({ "parts": [{ "text": "Two cities." }] })
    );

    let replay = body(2);
    assert_eq!(
        replay["contents"][1],
        json!({"role":"model","parts":[
            {"functionCall":{"id":"native-a","name":"lookup","args":{"city":"Shanghai"}},
                "thoughtSignature": STANDARD.encode("skip_thought_signature_validator")},
            {"functionCall":{"id":"native-b","name":"lookup","args":{"city":"Suzhou"}}}
        ]})
    );
    assert_eq!(
        replay["contents"][2],
        json!({"role":"user","parts":[
            {"functionResponse":{"id":"native-a","name":"lookup","response":{"weather":"cloudy"},
                "parts":[{"inlineData":{"mimeType":"image/png","data":"AAAA"}}]}},
            {"functionResponse":{"id":"native-b","name":"lookup","response":{"result":"sunny"}}}
        ]})
    );

    let legacy = build_request(
        &messages,
        &tools,
        None,
        "gemini-2.5-flash",
        None,
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        legacy["contents"][1]["parts"][0],
        json!({"functionCall":{"name":"lookup","args":{"city":"Shanghai"}}})
    );
    assert_eq!(
        legacy["contents"][2]["parts"][0],
        json!({"functionResponse":{"name":"lookup","response":{"weather":"cloudy"}}})
    );
    let robotics = build_request(
        &messages,
        &tools,
        None,
        "gemini-robotics-er-2-preview",
        None,
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        robotics["contents"][1]["parts"][0]["functionCall"]["id"],
        "native-a"
    );
    assert_eq!(
        robotics["contents"][2]["parts"][0]["functionResponse"]["id"],
        "native-a"
    );
}

#[test]
fn typed_gemini_maps_effort_to_levels_and_budgets() {
    use ReasoningEffort::*;

    let thinking = |model, effort| {
        build_request(
            &[Message::user("hello")],
            &[],
            None,
            model,
            Some(effort),
            Default::default(),
        )
        .map(|body| body["generationConfig"]["thinkingConfig"].clone())
    };
    for (effort, name, budget) in [
        (Low, "low", 1024),
        (Medium, "medium", 2048),
        (High, "high", 4096),
    ] {
        assert_eq!(
            thinking("gemini-3.5-flash", effort).unwrap(),
            json!({ "thinkingLevel": name, "includeThoughts": true }),
        );
        assert_eq!(
            thinking("gemini-2.5-flash", effort).unwrap(),
            json!({ "thinkingBudget": budget, "includeThoughts": true }),
        );
    }
    assert_eq!(
        thinking("gemini-3-flash-preview", Disabled).unwrap(),
        json!({ "thinkingLevel": "minimal", "includeThoughts": false })
    );
    assert_eq!(
        thinking("gemini-3.8-flash", Disabled).unwrap(),
        json!({ "thinkingLevel": "low", "includeThoughts": false })
    );
    assert_eq!(
        thinking("gemini-2.5-flash", Disabled).unwrap(),
        json!({ "thinkingBudget": 0, "includeThoughts": false })
    );
    assert_eq!(
        thinking("gemini-4-hydrogen", Xhigh).unwrap(),
        json!({ "thinkingLevel": "xhigh", "includeThoughts": true })
    );
    assert_eq!(
        thinking("gateway-model-alias", Xhigh).unwrap(),
        json!({ "thinkingLevel": "XHIGH" })
    );
    assert_eq!(
        thinking("gemini-robotics-er-2-preview", Low).unwrap(),
        json!({ "thinkingLevel": "low", "includeThoughts": true })
    );
    for (model, effort) in [
        ("gemini-2.0-flash", Low),
        ("gemini-2.5-pro", Disabled),
        ("gemini-2.5-flash", Xhigh),
        ("gemini-3.5-flash", Max),
    ] {
        assert!(matches!(thinking(model, effort), Err(LLMError::Api(_))));
    }
}

#[test]
fn typed_gemini_accepts_legacy_tool_results_keyed_by_function_name() {
    let body = build_request(
        &[Message::tool_result("lookup", "sunny")],
        &[],
        None,
        "gemini-3.5-flash",
        None,
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        body["contents"][0]["parts"][0],
        json!({"functionResponse":{"name":"lookup","response":{"result":"sunny"}}})
    );
}

#[tokio::test]
async fn configured_thinking_override_resolves_an_unsupported_effort() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(GEMINI_STREAM, "text/event-stream"))
        .expect(1)
        .mount(&server)
        .await;
    let thinking = json!({"thinkingLevel":"low","includeThoughts":true});
    let overrides = serde_json::from_value(json!({"common":{"body_patch":[{
        "op":"set","path":"/generationConfig/thinkingConfig","value":thinking.clone()
    }]}}))
    .unwrap();
    let provider = GeminiProvider::new("fixture-key")
        .with_base_url(server.uri())
        .with_client(reqwest::Client::builder().no_proxy().build().unwrap())
        .with_litellm_wire_types()
        .with_request_overrides(Some(overrides));
    let model = "gemini-3.5-flash";
    let options = LLMRequestOptions {
        reasoning_effort: Some(ReasoningEffort::Max),
        ..Default::default()
    };
    let _: Vec<_> = provider
        .chat_stream_with_options(
            &[Message::user("hello")],
            &[],
            Some(4096),
            model,
            Some(&options),
        )
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests[0].body_json::<Value>().unwrap()["generationConfig"]["thinkingConfig"],
        thinking
    );

    let replaced = serde_json::from_value(json!({"common":{"body_patch":[{
        "op":"set","path":"/","value":{"contents":[]}
    }]}}))
    .unwrap();
    assert!(provider
        .with_request_overrides(Some(replaced))
        .native_transcript_protocol(model)
        .is_none());
}
