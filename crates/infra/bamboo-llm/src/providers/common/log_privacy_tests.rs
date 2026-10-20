//! Capture provider events while exercising real request builders and parsers.
use std::sync::{Arc, Mutex};

use bamboo_domain::{FunctionSchema, Message, ReasoningEffort, ToolSchema};
use futures::StreamExt;
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

use crate::provider::{LLMProvider, LLMRequestOptions, ResponsesRequestOptions};
use crate::types::LLMChunk;

#[derive(Clone, Default)]
pub(crate) struct EventCapture(Arc<Mutex<Vec<String>>>);

impl EventCapture {
    pub(crate) fn text(&self) -> String {
        self.0.lock().unwrap().join("\n")
    }

    pub(crate) fn assert_private(&self, phase: &str, sentinels: &[&str]) {
        let text = self.text();
        assert!(
            text.contains(phase),
            "provider phase was not captured: {phase}"
        );
        for sentinel in sentinels {
            assert!(
                !text.contains(sentinel),
                "operational log leaked sentinel {sentinel}"
            );
        }
    }
}

impl tracing::Subscriber for EventCapture {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.target().starts_with("bamboo_llm")
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Visitor(String);
        impl tracing::field::Visit for Visitor {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                use std::fmt::Write;
                let _ = write!(&mut self.0, "{}={value:?};", field.name());
            }
        }
        let mut visitor = Visitor(String::new());
        event.record(&mut visitor);
        self.0.lock().unwrap().push(visitor.0);
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

const PROMPT: &str = "private-prompt-999-sentinel";
const WORKSPACE: &str = "/private/workspace-999-sentinel/project";
const CREDENTIAL: &str = "credential-999-sentinel";
const TOOL: &str = "private_tool_999_sentinel";
const BODY: &str = "private-body-999-sentinel";
const MODEL: &str = "private-model-999-sentinel";
const SESSION: &str = "private-session-999-sentinel";
const PURPOSE: &str = "private-purpose-999-sentinel";

fn options() -> LLMRequestOptions {
    LLMRequestOptions {
        session_id: Some(SESSION.into()),
        request_purpose: Some(PURPOSE.into()),
        ..Default::default()
    }
}

#[tokio::test(flavor = "current_thread")]
async fn provider_operational_logs_gemini_keep_wire_and_stream_private() {
    let server = MockServer::start().await;
    let response = format!(
        "data: {}\n\n",
        json!({"candidates":[{"content":{"parts":[{"text":BODY}],"role":"model"},"finishReason":"STOP"}]})
    );
    Mock::given(method("POST"))
        .and(path(format!("/models/{MODEL}:streamGenerateContent")))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(response),
        )
        .mount(&server)
        .await;
    let provider =
        crate::providers::gemini::GeminiProvider::new(CREDENTIAL).with_base_url(server.uri());
    let messages = vec![
        Message::system(format!("Workspace: {WORKSPACE}")),
        Message::user(format!("{PROMPT} {CREDENTIAL}")),
    ];
    let tools = vec![ToolSchema {
        schema_type: "function".into(),
        function: FunctionSchema {
            name: TOOL.into(),
            description: BODY.into(),
            parameters: json!({"type":"object","properties":{"path":{"type":"string","description":WORKSPACE}}}),
        },
    }];
    let capture = EventCapture::default();
    let _guard = tracing::subscriber::set_default(capture.clone());
    tracing::callsite::rebuild_interest_cache();
    let mut stream = provider
        .chat_stream_with_options(&messages, &tools, Some(2048), MODEL, Some(&options()))
        .await
        .unwrap();
    let mut output = String::new();
    while let Some(chunk) = stream.next().await {
        if let LLMChunk::Token(token) = chunk.unwrap() {
            output.push_str(&token);
        }
    }
    assert_eq!(output, BODY);
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    let wire = String::from_utf8(requests[0].body.clone()).unwrap();
    for sentinel in [PROMPT, WORKSPACE, CREDENTIAL, TOOL, BODY] {
        assert!(wire.contains(sentinel));
    }
    assert_eq!(
        requests[0].headers.get("x-goog-api-key").unwrap(),
        CREDENTIAL
    );
    capture.assert_private(
        "Gemini request",
        &[
            PROMPT, WORKSPACE, CREDENTIAL, TOOL, BODY, MODEL, SESSION, PURPOSE,
        ],
    );
}

struct MissingPreviousResponse;
impl Respond for MissingPreviousResponse {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
        if body.get("previous_response_id").is_some() {
            ResponseTemplate::new(400).set_body_json(json!({"error":{"code":"previous_response_not_found","message":format!("{BODY} {CREDENTIAL} {WORKSPACE}")}}))
        } else {
            ResponseTemplate::new(200).insert_header("content-type", "text/event-stream").set_body_string(format!("event: response.output_text.delta\ndata: {}\n\nevent: response.completed\ndata: {{\"type\":\"response.completed\",\"response\":{{\"id\":\"resp_new\"}}}}\n\n", json!({"type":"response.output_text.delta","delta":BODY})))
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn provider_operational_logs_openai_fallback_keeps_error_body_private() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/responses"))
        .respond_with(MissingPreviousResponse)
        .mount(&server)
        .await;
    let provider = crate::providers::openai::OpenAIProvider::new(CREDENTIAL)
        .with_base_url(server.uri())
        .with_responses_only_models(vec![MODEL.into()]);
    let mut controls = options();
    controls.responses = Some(ResponsesRequestOptions {
        previous_response_id: Some("resp_stale".into()),
        ..Default::default()
    });
    let capture = EventCapture::default();
    let _guard = tracing::subscriber::set_default(capture.clone());
    tracing::callsite::rebuild_interest_cache();
    let mut stream = provider
        .chat_stream_with_options(
            &[Message::user(PROMPT)],
            &[],
            Some(2048),
            MODEL,
            Some(&controls),
        )
        .await
        .unwrap();
    let mut output = String::new();
    while let Some(chunk) = stream.next().await {
        if let LLMChunk::Token(token) = chunk.unwrap() {
            output.push_str(&token);
        }
    }
    assert_eq!(output, BODY);
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    let bodies: Vec<serde_json::Value> = requests
        .iter()
        .map(|r| serde_json::from_slice(&r.body).unwrap())
        .collect();
    assert_eq!(bodies[0]["previous_response_id"], "resp_stale");
    assert!(bodies[1].get("previous_response_id").is_none());
    assert!(bodies[1].to_string().contains(PROMPT));
    assert_eq!(
        requests[0].headers.get("authorization").unwrap(),
        &format!("Bearer {CREDENTIAL}")
    );
    capture.assert_private(
        "previous_response_id",
        &[BODY, CREDENTIAL, WORKSPACE, PROMPT, MODEL, SESSION, PURPOSE],
    );
}

#[test]
fn provider_operational_logs_anthropic_tool_events_keep_identifiers_private() {
    use crate::providers::anthropic::{parse_anthropic_sse_event, AnthropicStreamState};
    let mut state = AnthropicStreamState::default();
    let capture = EventCapture::default();
    let _guard = tracing::subscriber::set_default(capture.clone());
    tracing::callsite::rebuild_interest_cache();
    let start = json!({"index":0,"content_block":{"type":"tool_use","id":CREDENTIAL,"name":TOOL,"input":{}}}).to_string();
    let chunk = parse_anthropic_sse_event(&mut state, "content_block_start", &start)
        .unwrap()
        .unwrap();
    assert!(
        matches!(chunk, LLMChunk::ToolCalls(calls) if calls[0].id == CREDENTIAL && calls[0].function.name == TOOL)
    );
    let partial = json!({"index":0,"delta":{"type":"input_json_delta","partial_json":format!("{{\"path\":\"{WORKSPACE}\"}}")}}).to_string();
    let chunk = parse_anthropic_sse_event(&mut state, "content_block_delta", &partial)
        .unwrap()
        .unwrap();
    assert!(
        matches!(chunk, LLMChunk::ToolCalls(calls) if calls[0].function.arguments.contains(WORKSPACE))
    );
    let delta = json!({"delta":{"stop_reason":BODY}}).to_string();
    parse_anthropic_sse_event(&mut state, "message_delta", &delta).unwrap();
    assert!(matches!(
        parse_anthropic_sse_event(&mut state, "message_stop", "{}").unwrap(),
        Some(LLMChunk::Done)
    ));
    capture.assert_private("tool_use", &[TOOL, CREDENTIAL, WORKSPACE, BODY]);
}

#[test]
fn provider_operational_logs_responses_summary_keeps_model_private() {
    let mut parser = super::openai_responses::ResponsesSseParser::new_with_context(
        "OpenAI",
        MODEL,
        Some(ReasoningEffort::High),
    );
    let capture = EventCapture::default();
    let _guard = tracing::subscriber::set_default(capture.clone());
    tracing::callsite::rebuild_interest_cache();
    let chunks = parser.handle_event_multi("response.completed", r#"{"type":"response.completed","response":{"usage":{"output_tokens":4,"output_tokens_details":{"reasoning_tokens":2}}}}"#).unwrap();
    assert!(chunks.iter().any(|c| matches!(c, LLMChunk::Done)));
    capture.assert_private("reasoning summary", &[MODEL]);
}

#[test]
fn provider_operational_logs_overrides_keep_header_names_and_patch_paths_private() {
    use bamboo_config::{BodyPatch, BodyPatchOp, RequestOverridesConfig, TemplateExpr};
    let mut overrides = RequestOverridesConfig::default();
    overrides.common.headers.insert(
        format!("invalid header {CREDENTIAL}"),
        TemplateExpr::Literal("value".into()),
    );
    overrides
        .common
        .headers
        .insert("x-valid".into(), TemplateExpr::Literal(CREDENTIAL.into()));
    overrides.common.body_patch.push(BodyPatch {
        path: WORKSPACE.into(),
        op: BodyPatchOp::Set,
        value: None,
    });
    let capture = EventCapture::default();
    let _guard = tracing::subscriber::set_default(capture.clone());
    tracing::callsite::rebuild_interest_cache();
    let mut headers = reqwest::header::HeaderMap::new();
    let env = std::collections::HashMap::new();
    super::request_overrides::apply_overrides_to_header_map_with_env(
        &mut headers,
        Some(&overrides),
        "responses",
        None,
        &env,
    );
    assert_eq!(headers.get("x-valid").unwrap(), CREDENTIAL);
    assert_eq!(headers.len(), 1);
    let mut body = json!({"input":PROMPT});
    super::request_overrides::apply_overrides_to_body_with_env(
        &mut body,
        Some(&overrides),
        "responses",
        None,
        &env,
    );
    assert_eq!(body, json!({"input":PROMPT}));
    capture.assert_private("Skipping", &[CREDENTIAL, WORKSPACE, PROMPT]);
}

#[tokio::test(flavor = "current_thread")]
async fn provider_operational_logs_shared_retry_keep_url_private_and_error_intact() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let url = format!("http://{address}/{WORKSPACE}?key={CREDENTIAL}");
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let config = crate::retry::RetryConfig {
        max_attempts: 2,
        base_delay: std::time::Duration::ZERO,
        max_delay: std::time::Duration::ZERO,
    };
    let capture = EventCapture::default();
    let _guard = tracing::subscriber::set_default(capture.clone());
    tracing::callsite::rebuild_interest_cache();
    let error = crate::retry::send_with_retry(&config, "OpenAI", || client.get(&url))
        .await
        .unwrap_err();
    assert_eq!(error.url().unwrap().as_str(), url);
    capture.assert_private("transport error", &[WORKSPACE, CREDENTIAL]);
}

#[tokio::test(flavor = "current_thread")]
async fn provider_operational_logs_bodhi_model_error_keeps_body_private() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/proxy/openai/v1/models"))
        .respond_with(
            ResponseTemplate::new(400).set_body_string(format!("{BODY} {WORKSPACE} {CREDENTIAL}")),
        )
        .mount(&server)
        .await;
    let provider =
        crate::providers::bodhi::BodhiProvider::new(CREDENTIAL).with_base_url(server.uri());
    let capture = EventCapture::default();
    let _guard = tracing::subscriber::set_default(capture.clone());
    tracing::callsite::rebuild_interest_cache();
    assert!(provider.list_models().await.unwrap().is_empty());
    capture.assert_private("models endpoint", &[BODY, WORKSPACE, CREDENTIAL]);
}

#[test]
fn provider_operational_logs_anthropic_converter_keep_invalid_arguments_private() {
    use crate::protocol::ToProvider;
    use crate::providers::anthropic::{AnthropicContent, AnthropicContentBlock, AnthropicMessage};
    let arguments = format!("not-json {CREDENTIAL} {WORKSPACE} {BODY}");
    let message = Message::assistant(
        "",
        Some(vec![bamboo_domain::ToolCall {
            id: CREDENTIAL.into(),
            tool_type: "function".into(),
            function: bamboo_domain::FunctionCall {
                name: TOOL.into(),
                arguments: arguments.clone(),
            },
        }]),
    );
    let capture = EventCapture::default();
    let _guard = tracing::subscriber::set_default(capture.clone());
    tracing::callsite::rebuild_interest_cache();
    let converted: AnthropicMessage = message.to_provider().unwrap();
    let AnthropicContent::Blocks(blocks) = converted.content else {
        panic!("converter must preserve the tool-use block");
    };
    assert_eq!(blocks.len(), 1);
    let AnthropicContentBlock::ToolUse { id, name, input } = &blocks[0] else {
        panic!("converter must preserve tool-use fields");
    };
    assert_eq!(id, CREDENTIAL);
    assert_eq!(name, TOOL);
    assert_eq!(input, &serde_json::Value::String(arguments));
    capture.assert_private(
        "protocol conversion fallback",
        &[CREDENTIAL, WORKSPACE, BODY, TOOL],
    );
}

#[tokio::test(flavor = "current_thread")]
async fn provider_operational_logs_registry_keep_instance_identifiers_private() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = bamboo_config::Config::default();
    config.default_provider_instance = Some(SESSION.into());
    config.provider_instances.insert(
        SESSION.into(),
        serde_json::from_value(json!({"provider_type":"openai","api_key":"","enabled":true}))
            .unwrap(),
    );
    let capture = EventCapture::default();
    let _guard = tracing::subscriber::set_default(capture.clone());
    tracing::callsite::rebuild_interest_cache();
    let registry =
        crate::provider_registry::ProviderRegistry::from_config(&config, directory.path().into())
            .await
            .unwrap();
    assert!(registry.get(SESSION).is_none());
    assert!(registry.get_default().is_none());
    capture.assert_private("failed to initialize", &[SESSION]);
}
