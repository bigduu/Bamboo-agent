use std::sync::Arc;

use bamboo_agent_core::tools::{FunctionCall, FunctionSchema, ToolCall, ToolSchema};
use bamboo_agent_core::{Message, Session};
use bamboo_config::RequestOverridesConfig;
use bamboo_domain::{DISCOVERY_CONTROL_FALLBACK_TOOL_NAME, SESSION_COORDINATION_TOOL_NAME};
use bamboo_llm::providers::BodhiProvider;
use bamboo_llm::{AnthropicProvider, GeminiProvider, LLMProvider, OpenAIProvider};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::runtime::config::AgentLoopConfig;

const MODEL: &str = "fixture-model";
const DENIED_SCHEMA_MARKER: &str = "PRIVATE_TOOL_DEFINITION_TEST_MARKER";
const CHILD_GUIDANCE_TITLE: &str = "Child Tool Availability";
const COORDINATION_GUIDANCE_TITLE: &str = "On-demand Session coordination";

fn schema(name: &str) -> ToolSchema {
    ToolSchema {
        schema_type: "function".into(),
        function: FunctionSchema {
            name: name.into(),
            description: if name == "load_skill" {
                DENIED_SCHEMA_MARKER.into()
            } else {
                format!("Use {name}")
            },
            parameters: json!({"type":"object","properties":{},"additionalProperties":false}),
        },
    }
}

fn record_discovery(session: &mut Session, definitions: &[ToolSchema]) {
    let mut assistant = Message::assistant(
        "",
        Some(vec![ToolCall {
            id: "discovery".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: DISCOVERY_CONTROL_FALLBACK_TOOL_NAME.into(),
                arguments: r#"{"query":"files"}"#.into(),
            },
        }]),
    );
    assistant.never_compress = true;
    assistant.metadata = Some(json!({
        "runtime_kind":"sticky_capability_discovery",
        "version":1,
    }));
    session.add_message(assistant);
    let mut result = Message::tool_result_with_status(
        "discovery",
        format!(
            "<loaded_tools>{}</loaded_tools>",
            json!({"tools": definitions})
        ),
        true,
    );
    result.never_compress = true;
    result.metadata = Some(json!({
        "runtime_kind":"sticky_capability_discovery",
        "version":1,
        "canonical_new_names":definitions.iter().map(|tool| &tool.function.name).collect::<Vec<_>>(),
    }));
    session.add_message(result);
}

async fn send(session: &mut Session, provider: &Arc<dyn LLMProvider>, catalog: &[ToolSchema]) {
    assert!(run_round(session, provider, catalog, None).await.is_err());
}

async fn run_round(
    session: &mut Session,
    provider: &Arc<dyn LLMProvider>,
    catalog: &[ToolSchema],
    output_limit_policy: Option<super::RoundRecoveryPolicy<'_>>,
) -> Result<super::RoundLlmExecutionOutput, super::AgentError> {
    run_round_for_model(session, provider, catalog, output_limit_policy, MODEL, None).await
}

async fn run_round_for_model(
    session: &mut Session,
    provider: &Arc<dyn LLMProvider>,
    catalog: &[ToolSchema],
    output_limit_policy: Option<super::RoundRecoveryPolicy<'_>>,
    model: &str,
    provider_type: Option<&str>,
) -> Result<super::RoundLlmExecutionOutput, super::AgentError> {
    let config = AgentLoopConfig {
        model_name: Some(model.into()),
        provider_type: provider_type.map(str::to_owned),
        reasoning_effort: session.reasoning_effort,
        ..Default::default()
    };
    let (events, _receiver) = tokio::sync::mpsc::channel(128);
    let session_id = session.id.clone();
    super::execute_llm_round(
        session,
        &config,
        provider,
        &events,
        &CancellationToken::new(),
        &session_id,
        model,
        catalog,
        None,
        None,
        output_limit_policy,
    )
    .await
}

#[tokio::test]
async fn ultra_bodhi_gemini_sends_supported_reasoning_for_dotted_models() {
    use wiremock::matchers::path;

    let server = MockServer::start().await;
    let provider: Arc<dyn LLMProvider> = Arc::new(
        BodhiProvider::new("fixture-key")
            .with_base_url(server.uri())
            .with_client(reqwest::Client::builder().no_proxy().build().unwrap())
            .with_target_provider("gemini")
            .with_litellm_wire_types(),
    );

    for model in ["gemini-3.1-pro", "gemini-3.5-flash"] {
        let endpoint = format!("/proxy/gemini/v1beta/models/{model}:streamGenerateContent");
        Mock::given(method("POST"))
            .and(path(endpoint))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                concat!(
                    r#"data: {"candidates":[{"content":{"role":"model","parts":[{"text":"Ready."}]},"finishReason":"STOP"}]}"#,
                    "\n\n",
                ),
                "text/event-stream",
            ))
            .expect(1)
            .mount(&server)
            .await;

        let mut session = Session::new(format!("ultra-{model}"), model);
        session.add_message(Message::user("Check the route."));
        session.set_root_orchestration_only(true).unwrap();
        let round = run_round_for_model(&mut session, &provider, &[], None, model, Some("bodhi"))
            .await
            .unwrap();
        assert_eq!(round.stream_output.content, "Ready.");
    }

    let requests = capture(&server).await;
    assert_eq!(requests.len(), 2);
    for body in requests {
        assert_eq!(
            body["generationConfig"]["thinkingConfig"],
            json!({"thinkingLevel":"high","includeThoughts":true})
        );
    }
}

#[tokio::test]
async fn gemini_replays_signed_parallel_tool_turn_after_session_reload() {
    use bamboo_agent_core::storage::Storage;
    use bamboo_domain::{ProviderFamily, ProviderProtocol};
    use bamboo_storage::SessionStoreV2;

    const GEMINI_MODEL: &str = "gemini-3.5-flash";
    let native_parts = json!([
        {"thought": true, "text": "I need both results."},
        {"functionCall":{"id":"gemini_1","name":"lookup","args":{}},
            "thoughtSignature":"bW9kZWwtb3duZWQtc2lnbmF0dXJl"},
        {"functionCall":{"name":"lookup","args":{}}}
    ]);
    let frames = [
        json!({"candidates":[{"content":{"role":"model","parts":[native_parts[0]]}}]}),
        json!({"candidates":[{"content":{"role":"model","parts":[native_parts[1],native_parts[2]]}}]}),
        json!({"candidates":[{"finishReason":"STOP"}]}),
    ];
    let tool_stream: String = frames
        .iter()
        .map(|frame| format!("data: {frame}\n\n"))
        .collect();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(move |request: &wiremock::Request| {
            let body: Value = request.body_json().unwrap();
            let replay = body["contents"].as_array().unwrap().iter().any(|content| {
                content["parts"].as_array().unwrap().iter().any(|part| part.get("functionResponse").is_some())
            });
            let stream = if replay {
                "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"Both looked up.\"}]},\"finishReason\":\"STOP\"}]}\n\n"
            } else {
                &tool_stream
            };
            ResponseTemplate::new(200).set_body_raw(stream, "text/event-stream")
        })
        .expect(3)
        .mount(&server)
        .await;
    struct SnapshottingProvider(Arc<dyn LLMProvider>);

    #[async_trait::async_trait]
    impl LLMProvider for SnapshottingProvider {
        fn queued_provider_snapshot(
            &self,
        ) -> bamboo_llm::provider::Result<Option<Arc<dyn LLMProvider>>> {
            bamboo_llm::provider::snapshot_forwarded_provider(Arc::clone(&self.0)).map(Some)
        }

        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> bamboo_llm::provider::Result<bamboo_llm::LLMStream> {
            panic!("signed Gemini replay must dispatch via the selected provider snapshot")
        }
    }

    let provider = |key: &str| -> Arc<dyn LLMProvider> {
        Arc::new(SnapshottingProvider(Arc::new(
            GeminiProvider::new(key)
                .with_base_url(server.uri())
                .with_client(reqwest::Client::builder().no_proxy().build().unwrap())
                .with_litellm_wire_types(),
        )))
    };
    let mut session = Session::new("gemini-replay", GEMINI_MODEL);
    session.add_message(Message::user("Look up both records."));
    let catalog = [schema("lookup")];
    let first = run_round_for_model(
        &mut session,
        &provider("original-key"),
        &catalog,
        None,
        GEMINI_MODEL,
        None,
    )
    .await
    .unwrap()
    .stream_output;
    assert_eq!(first.tool_calls.len(), 2);
    assert_eq!(first.tool_calls[0].id, "gemini_1");
    assert_ne!(first.tool_calls[0].id, first.tool_calls[1].id);
    let assistant = Message::assistant_with_reasoning(
        first.content,
        Some(first.tool_calls.clone()),
        Some(first.reasoning_content),
    );
    let anchor = assistant.id.clone();
    session.add_message(assistant);
    session
        .append_provider_transcript_group(&anchor, None, first.provider_transcript_items)
        .unwrap();
    assert_eq!(
        session.provider_transcript.active_family(),
        Some(ProviderFamily::Gemini)
    );
    assert_eq!(
        session.provider_transcript.active_protocol(),
        Some(ProviderProtocol::GeminiGenerateContentV1Beta)
    );
    for call in first.tool_calls.iter().rev() {
        session.add_message(Message::tool_result(&call.id, "looked up"));
    }
    let data_dir = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(data_dir.path().to_path_buf())
        .await
        .unwrap();
    store.save_session(&session).await.unwrap();
    drop(store);
    let restarted = SessionStoreV2::new(data_dir.path().to_path_buf())
        .await
        .unwrap();
    for key in ["original-key", "different-key"] {
        let mut restored = restarted.load_session(&session.id).await.unwrap().unwrap();
        assert_eq!(
            run_round_for_model(
                &mut restored,
                &provider(key),
                &catalog,
                None,
                GEMINI_MODEL,
                None
            )
            .await
            .unwrap()
            .stream_output
            .content,
            "Both looked up."
        );
    }
    let requests = capture(&server).await;
    let parts = |body_index: usize, key: &str| {
        requests[body_index]["contents"]
            .as_array()
            .unwrap()
            .iter()
            .find_map(|content| {
                let parts = content["parts"].as_array().unwrap();
                parts
                    .iter()
                    .any(|part| part.get(key).is_some())
                    .then_some(&content["parts"])
            })
            .unwrap()
    };
    assert_eq!(parts(1, "functionCall"), &native_parts);
    let results = parts(1, "functionResponse").as_array().unwrap();
    assert_eq!(results[0]["functionResponse"]["id"], "gemini_1");
    assert!(results[1]["functionResponse"].get("id").is_none());
    assert_ne!(parts(2, "functionCall"), &native_parts);
}

async fn capture(server: &MockServer) -> Vec<Value> {
    server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|request| request.method.as_str() == "POST")
        .map(|request| request.body_json().unwrap())
        .collect()
}

fn tool_names(body: &Value) -> Vec<&str> {
    body["tools"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|entry| {
            if let Some(declarations) = entry["functionDeclarations"].as_array() {
                declarations
                    .iter()
                    .map(|definition| definition["name"].as_str().unwrap())
                    .collect()
            } else {
                vec![entry
                    .get("name")
                    .unwrap_or(&entry["function"]["name"])
                    .as_str()
                    .unwrap()]
            }
        })
        .collect()
}

fn body_override(path: &str, value: Value) -> RequestOverridesConfig {
    serde_json::from_value(json!({"common":{"body_patch":[{"path":path,"value":value}]}})).unwrap()
}

fn coordination_schema() -> ToolSchema {
    let mut coordination = schema(SESSION_COORDINATION_TOOL_NAME);
    coordination.function.parameters = json!({
        "type":"object","properties":{"action":{"type":"string","enum":["list_running","send"]}},
        "required":["action"],"additionalProperties":false,
    });
    coordination
}

#[tokio::test]
async fn running_coordination_guidance_precedes_the_user_without_disclosing_the_schema() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(400).set_body_string("fixture request captured"))
        .expect(3)
        .mount(&server)
        .await;
    let provider: Arc<dyn LLMProvider> = Arc::new(
        OpenAIProvider::new("fixture")
            .with_base_url(server.uri())
            .with_client(reqwest::Client::builder().no_proxy().build().unwrap()),
    );
    let catalog = [schema("Read"), coordination_schema()];
    let mut session = Session::new("coordination-wire", MODEL);
    const USER_REQUEST: &str = "Check for overlapping implementation before editing.";
    session.add_message(Message::user(USER_REQUEST));

    send(&mut session, &provider, &catalog).await;
    record_discovery(&mut session, &catalog[1..]);
    send(&mut session, &provider, &catalog).await;
    send(&mut session, &provider, &catalog[..1]).await;

    let bodies = capture(&server).await;
    assert_eq!(bodies.len(), 3);
    let first = &bodies[0];
    assert_eq!(
        tool_names(first),
        ["Read", DISCOVERY_CONTROL_FALLBACK_TOOL_NAME]
    );
    assert!(!first["tools"].to_string().contains("list_running"));
    let messages = first["messages"].as_array().unwrap();
    let guide_index = messages
        .iter()
        .position(|message| {
            message["content"]
                .to_string()
                .contains(COORDINATION_GUIDANCE_TITLE)
        })
        .expect("on-demand tool hint in the provider request");
    let user_index = messages
        .iter()
        .position(|message| message["content"].to_string().contains(USER_REQUEST))
        .expect("user request in the provider request");
    assert!(guide_index < user_index);
    assert!(messages[guide_index]["content"]
        .to_string()
        .contains("Call `discover_capabilities` by itself"));

    assert!(tool_names(&bodies[1]).contains(&SESSION_COORDINATION_TOOL_NAME));
    assert!(bodies[1].to_string().contains("list_running"));
    assert_eq!(tool_names(&bodies[2]), ["Read"]);
    assert!(!bodies[2].to_string().contains(COORDINATION_GUIDANCE_TITLE));
    assert!(!bodies[2].to_string().contains("list_running"));
    assert!(!session
        .messages
        .iter()
        .any(|message| message.content.contains(COORDINATION_GUIDANCE_TITLE)));
}

#[tokio::test]
async fn native_search_accounts_for_running_coordination_as_deferred() {
    use bamboo_domain::CapabilityLoadingMode;
    use bamboo_llm::providers::common::openai_responses::ResponsesToolSearchExecution;
    use bamboo_llm::{PromptIR, ProviderVisibleToolSegmentKind};

    let openai = |execution| {
        OpenAIProvider::new("fixture")
            .with_responses_only_models(vec![MODEL.into()])
            .with_tool_search_execution(execution)
    };
    let providers: [(Box<dyn LLMProvider>, &str); 3] = [
        (
            Box::new(openai(ResponsesToolSearchExecution::Client)),
            MODEL,
        ),
        (
            Box::new(openai(ResponsesToolSearchExecution::Server)),
            MODEL,
        ),
        (
            Box::new(AnthropicProvider::new("fixture")),
            "claude-sonnet-4-6",
        ),
    ];
    let catalog = [schema("Read"), coordination_schema()];
    for (provider, model) in providers {
        let mode = provider.capability_loading_mode(model, None).await;
        assert_eq!(mode, CapabilityLoadingMode::Progressive);
        let footprint = provider
            .provider_visible_tool_footprint(&PromptIR::default(), &catalog, model, None)
            .await
            .unwrap();
        let initial = footprint
            .segments
            .iter()
            .find(|segment| segment.kind == ProviderVisibleToolSegmentKind::InitialFullDefinition)
            .unwrap();
        assert!(initial.serialized.contains("Read"));
        assert!(!initial.serialized.contains(SESSION_COORDINATION_TOOL_NAME));
        assert!(footprint
            .segments
            .iter()
            .any(|segment| { segment.kind == ProviderVisibleToolSegmentKind::ProviderLateBound }));
    }
}

#[tokio::test]
async fn roles_use_on_demand_declarations_on_every_compatibility_wire() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(400).set_body_string("fixture request captured"))
        .mount(&server)
        .await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let providers: Vec<Arc<dyn LLMProvider>> = vec![
        Arc::new(
            OpenAIProvider::new("fixture")
                .with_base_url(server.uri())
                .with_client(client.clone())
                .with_sticky_tool_loading(false),
        ),
        Arc::new(
            OpenAIProvider::new("fixture")
                .with_base_url(server.uri())
                .with_client(client.clone())
                .with_responses_only_models(vec![MODEL.into()]),
        ),
        Arc::new(
            AnthropicProvider::new("fixture")
                .with_base_url(server.uri())
                .with_client(client.clone()),
        ),
        Arc::new(
            GeminiProvider::new("fixture")
                .with_base_url(server.uri())
                .with_client(client.clone()),
        ),
        Arc::new(
            BodhiProvider::new("fixture")
                .with_base_url(server.uri())
                .with_client(client.clone()),
        ),
        Arc::new(
            BodhiProvider::new("fixture")
                .with_base_url(server.uri())
                .with_client(client.clone())
                .with_target_provider("anthropic"),
        ),
        Arc::new(
            BodhiProvider::new("fixture")
                .with_base_url(server.uri())
                .with_client(client)
                .with_target_provider("gemini"),
        ),
    ];
    let catalog = [
        "Read",
        "Glob",
        "GetFileInfo",
        "load_skill",
        "exec_command",
        "write_stdin",
    ]
    .map(schema);
    for (index, provider) in providers.iter().enumerate() {
        let mut session = Session::new(&format!("wire-role-{index}"), MODEL);
        session.add_message(Message::user("Find a file"));
        session.set_root_orchestration_only(true).unwrap();
        send(&mut session, provider, &catalog).await;
        record_discovery(&mut session, &[catalog[1].clone(), catalog[3].clone()]);
        send(&mut session, provider, &catalog).await;

        let mut child = Session::new(&format!("wire-child-{index}"), MODEL);
        child.kind = bamboo_domain::SessionKind::Child;
        child.add_message(Message::user("Review the assigned change"));
        send(&mut child, provider, &catalog[..2]).await;
    }
    let bodies = capture(&server).await;
    assert_eq!(bodies.len(), providers.len() * 3);
    for rounds in bodies.chunks_exact(3) {
        assert_eq!(
            tool_names(&rounds[0]),
            [
                "Read",
                "exec_command",
                "write_stdin",
                "discover_capabilities"
            ]
        );
        assert_eq!(
            tool_names(&rounds[1]),
            [
                "Read",
                "Glob",
                "exec_command",
                "write_stdin",
                "discover_capabilities"
            ]
        );
        for round in &rounds[..2] {
            assert!(round
                .to_string()
                .contains("Use exec_command for authorized shell commands"));
        }
        assert_eq!(tool_names(&rounds[2]), ["Read", "discover_capabilities"]);
        assert!(rounds[2].to_string().contains(CHILD_GUIDANCE_TITLE));
        assert!(rounds
            .iter()
            .all(|body| !body.to_string().contains(DENIED_SCHEMA_MARKER)));
    }
}

const OLD_SESSION_CONTROL_DESCRIPTION: &str = "An obsolete Session Control schema";
const SESSION_CONTROL_QUERY: &str = r#"{"query":"session_control","limit":1}"#;

struct SupervisorDiscoveryWireRun {
    session: Session,
    requests: Vec<Value>,
    calls: Vec<ToolCall>,
    outcome: crate::runtime::runner::Result<()>,
}

fn wire_tool_call(index: usize, id: &str, name: &str, arguments: &str) -> Value {
    json!({"index":index,"id":id,"type":"function","function":{"name":name,"arguments":arguments}})
}

fn scope_schema() -> ToolSchema {
    let mut control = schema("session_control");
    control.function.parameters = json!({
        "type":"object", "properties":{"action":{"const":"scope"}}, "required":["action"],
        "additionalProperties":false,
    });
    control
}

async fn run_supervisor_discovery_wire(
    max_rounds: u32,
    subsequent_replies: Vec<Value>,
) -> SupervisorDiscoveryWireRun {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use bamboo_agent_core::storage::Storage;
    use bamboo_agent_core::tools::{ToolError, ToolExecutionContext, ToolExecutor, ToolResult};
    use bamboo_compression::{
        apply_compression_plan, build_message_summary_candidate_plan_with_fixed_tokens,
        finalize_compression_candidate_plan, TokenBudget,
    };
    use bamboo_domain::{
        AgentRuntimeState, CompressionEventKind, CompressionTriggerType, SessionPermissionMode,
        SupervisorReference, DEFAULT_SUPERVISOR_SESSION_ID,
    };
    use wiremock::matchers::path;

    struct RecordingExecutor {
        catalog: Vec<ToolSchema>,
        calls: Mutex<Vec<ToolCall>>,
        supervisor: SupervisorReference,
    }

    #[async_trait::async_trait]
    impl ToolExecutor for RecordingExecutor {
        async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
            self.calls.lock().unwrap().push(call.clone());
            Ok(ToolResult::text(true, "Fixture tool completed."))
        }

        async fn execute_with_context(
            &self,
            call: &ToolCall,
            ctx: ToolExecutionContext<'_>,
        ) -> Result<ToolResult, ToolError> {
            let observed = ctx
                .to_tool_ctx()
                .executing_supervisor_for(DEFAULT_SUPERVISOR_SESSION_ID)
                .expect("tool must enter with the original Supervisor authority");
            assert_eq!(observed.supervisor_reference(), self.supervisor);
            self.execute(call).await
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            self.catalog.clone()
        }
    }

    let mut old_control = scope_schema();
    old_control.function.description = OLD_SESSION_CONTROL_DESCRIPTION.into();
    let monitor = schema("supervisor_log_monitor");

    let data_dir = tempfile::tempdir().unwrap();
    let store = bamboo_storage::SessionStoreV2::new(data_dir.path().to_path_buf())
        .await
        .unwrap();
    let identity = store.get_or_create_default_supervisor(MODEL).await.unwrap();
    let mut session = store
        .load_session(&identity.session_id)
        .await
        .unwrap()
        .unwrap();
    session.model = MODEL.into();
    session.set_root_orchestration_only(true).unwrap();
    session
        .agent_runtime_state
        .get_or_insert_with(AgentRuntimeState::default)
        .set_permission_mode(SessionPermissionMode::Default);
    session.add_message(Message::user("Earlier request"));
    let archived = session.messages.last().unwrap().id.clone();
    record_discovery(&mut session, &[old_control, monitor.clone()]);
    session.add_message(Message::assistant("Previous answer.", None));
    session.add_message(Message::user("Keep this request active."));
    let mut budget = TokenBudget::for_model(16_000);
    budget.compression_target_percent = 40;
    budget.working_reserve_tokens = 0;
    let candidate = build_message_summary_candidate_plan_with_fixed_tokens(
        &session,
        MODEL,
        Some(&budget),
        0.20,
        CompressionTriggerType::Auto,
        0,
    )
    .unwrap();
    let plan =
        finalize_compression_candidate_plan(&session, candidate, "Earlier review.".into()).unwrap();
    assert!(apply_compression_plan(&mut session, plan) > 0);
    store.save_session(&session).await.unwrap();
    drop(store);

    let store = Arc::new(
        bamboo_storage::SessionStoreV2::new(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );
    let mut session = store
        .load_session(&identity.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        session.supervisor_incarnation_id(),
        Some(identity.incarnation_id)
    );
    assert!(session.root_orchestration_only_enabled());
    assert_eq!(
        session.compression_events.last().unwrap().kind,
        CompressionEventKind::MessageSummary
    );
    assert!(session
        .messages
        .iter()
        .any(|row| row.id == archived && row.compressed));
    assert!(session.messages.iter().any(|row| {
        row.tool_call_id.as_deref() == Some("discovery")
            && !row.compressed
            && row.content.contains(OLD_SESSION_CONTROL_DESCRIPTION)
    }));

    let mixed = json!({"tool_calls":[
        wire_tool_call(0, "mixed-discovery", DISCOVERY_CONTROL_FALLBACK_TOOL_NAME, SESSION_CONTROL_QUERY),
        wire_tool_call(1, "mixed-task", "Task", r#"{"tasks":[]}"#),
    ]});
    let replies = std::iter::once(mixed)
        .chain(subsequent_replies)
        .collect::<Vec<_>>();
    let expected_requests = replies.len() as u64;
    let requests = AtomicUsize::new(0);
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(move |_: &wiremock::Request| {
            let round = requests.fetch_add(1, Ordering::SeqCst);
            let Some(delta) = replies.get(round) else {
                return ResponseTemplate::new(400).set_body_string("unexpected fixture request");
            };
            let finish_reason = if delta.get("tool_calls").is_some() {
                "tool_calls"
            } else {
                "stop"
            };
            let chunk = json!({
                "id":format!("discovery-wire-{round}"), "object":"chat.completion.chunk",
                "choices":[{"index":0,"delta":delta,"finish_reason":finish_reason}]
            });
            ResponseTemplate::new(200).set_body_raw(
                format!("data: {chunk}\n\ndata: [DONE]\n\n"),
                "text/event-stream",
            )
        })
        .expect(expected_requests)
        .mount(&server)
        .await;
    let provider = Arc::new(
        OpenAIProvider::new("fixture")
            .with_base_url(server.uri())
            .with_client(reqwest::Client::builder().no_proxy().build().unwrap()),
    );
    let executor = Arc::new(RecordingExecutor {
        catalog: vec![scope_schema(), schema("Task"), monitor],
        calls: Mutex::new(Vec::new()),
        supervisor: SupervisorReference::from(&identity),
    });
    let (events, _receiver) = tokio::sync::mpsc::channel(256);
    let outcome = crate::runtime::runner::run_agent_loop_with_config(
        &mut session,
        "Read the scope and update the checklist.".into(),
        events,
        provider,
        executor.clone(),
        CancellationToken::new(),
        AgentLoopConfig {
            storage: Some(store),
            model_name: Some(MODEL.into()),
            provider_type: Some("openai".into()),
            run_budget: bamboo_config::RunBudgetConfig {
                max_rounds: Some(max_rounds),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await;
    let requests = capture(&server).await;
    let calls = std::mem::take(&mut *executor.calls.lock().unwrap());
    SupervisorDiscoveryWireRun {
        session,
        requests,
        calls,
        outcome,
    }
}

fn assert_discovery_correction_request(request: &Value) {
    assert_eq!(tool_names(request), [DISCOVERY_CONTROL_FALLBACK_TOOL_NAME]);
    assert_eq!(request["parallel_tool_calls"], false);
    let wire = request.to_string();
    assert!(wire.contains("This request is for tool discovery only."));
    assert!(!wire.contains("Loaded tool definitions are in the tools for this request."));
    assert!(!wire.contains("Current delegation route observation:"));
}

fn assert_discovery_wire_tool_result(session: &Session, id: &str, success: bool) {
    let mut results = session
        .messages
        .iter()
        .filter(|row| row.tool_call_id.as_deref() == Some(id));
    assert_eq!(
        results.next().and_then(|row| row.tool_success),
        Some(success),
        "missing result: {id}"
    );
    assert!(results.next().is_none(), "duplicate result: {id}");
}

#[tokio::test]
async fn supervisor_resumes_live_session_control_and_recovers_parallel_discovery_on_wire() {
    let SupervisorDiscoveryWireRun { session, requests, calls, outcome } = run_supervisor_discovery_wire(6, vec![
        json!({"tool_calls":[
            wire_tool_call(0, "lookup", DISCOVERY_CONTROL_FALLBACK_TOOL_NAME, SESSION_CONTROL_QUERY),
        ]}),
        json!({"tool_calls":[
            wire_tool_call(0, "scope", "session_control", r#"{"action":"scope"}"#),
            wire_tool_call(1, "task", "Task", r#"{"tasks":[]}"#),
        ]}),
        json!({"content":"Checked the Session scope and updated the checklist."}),
    ]).await;
    outcome.unwrap();

    assert_eq!(requests.len(), 4);
    assert!(!tool_names(&requests[0]).contains(&"session_control"));
    assert!(tool_names(&requests[0]).contains(&"supervisor_log_monitor"));
    assert!(!requests[0]
        .to_string()
        .contains(OLD_SESSION_CONTROL_DESCRIPTION));
    let old_discovery = requests[0]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| message["tool_call_id"] == "discovery")
        .and_then(|message| message["content"].as_str())
        .expect("the reopened discovery result is retained on the wire");
    assert!(old_discovery.contains("Call discover_capabilities by itself"));
    assert!(old_discovery.contains(r#""query":"session_control""#));
    assert_eq!(requests[0]["parallel_tool_calls"], true);
    assert_discovery_correction_request(&requests[1]);
    assert_eq!(
        tool_names(&requests[2]),
        [
            "Task",
            "session_control",
            "supervisor_log_monitor",
            DISCOVERY_CONTROL_FALLBACK_TOOL_NAME
        ]
    );
    let current_control = requests[2]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["function"]["name"] == "session_control")
        .unwrap();
    assert_eq!(
        current_control["function"]["parameters"],
        scope_schema().function.parameters
    );
    assert_eq!(requests[2]["parallel_tool_calls"], true);

    for (id, success) in [
        ("mixed-discovery", false),
        ("mixed-task", false),
        ("lookup", true),
        ("scope", true),
        ("task", true),
    ] {
        assert_discovery_wire_tool_result(&session, id, success);
    }
    assert_eq!(calls.len(), 2);
    let scope = calls.iter().find(|call| call.id == "scope").unwrap();
    assert_eq!(scope.function.name, "session_control");
    assert_eq!(
        serde_json::from_str::<Value>(&scope.function.arguments).unwrap(),
        json!({"action":"scope"})
    );
    assert!(calls
        .iter()
        .any(|call| call.id == "task" && call.function.name == "Task"));
}

#[tokio::test]
async fn supervisor_discovery_correction_stops_if_wire_returns_other_tools() {
    for mixes_discovery in [false, true] {
        let mut ignored_calls = vec![wire_tool_call(0, "ignored-task", "Task", r#"{"tasks":[]}"#)];
        if mixes_discovery {
            ignored_calls.push(wire_tool_call(
                1,
                "ignored-discovery",
                DISCOVERY_CONTROL_FALLBACK_TOOL_NAME,
                SESSION_CONTROL_QUERY,
            ));
        }
        let run = run_supervisor_discovery_wire(6, vec![json!({"tool_calls":ignored_calls})]).await;
        assert!(matches!(
            run.outcome,
            Err(super::AgentError::Tool(error)) if error.contains("correction round")
        ));
        assert_eq!(run.requests.len(), 2, "only one correction is allowed");
        assert_discovery_correction_request(&run.requests[1]);
        assert!(
            run.calls.is_empty(),
            "no tool from either batch may execute"
        );
        assert_discovery_wire_tool_result(&run.session, "mixed-task", false);
        assert_discovery_wire_tool_result(&run.session, "ignored-task", false);
        if mixes_discovery {
            assert_discovery_wire_tool_result(&run.session, "ignored-discovery", false);
        }
    }
}

#[tokio::test]
async fn supervisor_discovery_correction_does_not_replace_round_limit_summary() {
    let summary = "Stopped at the round limit before checking Session scope.";
    let unexpected = wire_tool_call(0, "summary-tool", "Task", r#"{"tasks":[]}"#);
    let run = run_supervisor_discovery_wire(
        1,
        vec![json!({"content":summary,"tool_calls":[unexpected]})],
    )
    .await;
    run.outcome.unwrap();
    assert_eq!(run.requests.len(), 2);
    let summary_request = &run.requests[1];
    assert!(summary_request
        .to_string()
        .contains("Summarize the progress, verified outputs, and remaining work in text only"));
    assert!(!summary_request
        .to_string()
        .contains("This request is for tool discovery only."));
    assert!(tool_names(summary_request).is_empty());
    assert!(summary_request.get("parallel_tool_calls").is_none());
    assert_eq!(
        run.session
            .metadata
            .get("runtime.completion_reason")
            .map(String::as_str),
        Some("max_rounds_reached")
    );
    assert!(run
        .session
        .messages
        .iter()
        .any(|row| row.content == summary));
    assert!(run.calls.is_empty());
    assert_discovery_wire_tool_result(&run.session, "mixed-task", false);
    assert!(run.session.messages.iter().all(|message| {
        message.tool_call_id.as_deref() != Some("summary-tool")
            && message
                .tool_calls
                .as_ref()
                .is_none_or(|calls| calls.iter().all(|call| call.id != "summary-tool"))
    }));
}

#[tokio::test]
async fn explicit_round_cap_summary_has_no_tools_even_with_a_forced_provider_choice() {
    let summary = "Saved the partial artifact; verification remains.";
    check_round_cap_summary_wire("tool_calls", summary).await;
}

#[tokio::test]
async fn explicit_round_cap_summary_publishes_only_host_fallback_for_truncated_text() {
    let fallback = "The round limit was reached before a complete progress summary was available. \
                    Work completed so far remains available.";
    check_round_cap_summary_wire("length", fallback).await;
}

async fn check_round_cap_summary_wire(summary_finish_reason: &'static str, expected_summary: &str) {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bamboo_agent_core::tools::{ToolError, ToolExecutor, ToolResult};
    use wiremock::matchers::path;

    struct Writer(std::path::PathBuf);

    #[async_trait::async_trait]
    impl ToolExecutor for Writer {
        async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
            std::fs::write(&self.0, &call.id).unwrap();
            Ok(ToolResult::text(true, "Partial artifact saved."))
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            vec![schema("Write")]
        }
    }

    let summary = "Saved the partial artifact; verification remains.";
    let data = tempfile::tempdir().unwrap();
    let artifact = data.path().join("partial.txt");
    let write_arguments = json!({"file_path":artifact,"content":"fixture"}).to_string();
    let requests = AtomicUsize::new(0);
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(move |_: &wiremock::Request| {
            let round = requests.fetch_add(1, Ordering::SeqCst);
            let delta = match round {
                0 => json!({"tool_calls":[wire_tool_call(0, "work-write", "Write", &write_arguments)]}),
                1 => json!({
                    "content":summary,
                    "tool_calls":[wire_tool_call(0, "over-cap-write", "Write", &write_arguments)],
                }),
                _ => return ResponseTemplate::new(400),
            };
            let finish_reason = if round == 0 {
                "tool_calls"
            } else {
                summary_finish_reason
            };
            let chunk = json!({
                "id":format!("round-cap-{round}"), "object":"chat.completion.chunk",
                "choices":[{"index":0,"delta":delta,"finish_reason":finish_reason}]
            });
            ResponseTemplate::new(200).set_body_raw(
                format!("data: {chunk}\n\ndata: [DONE]\n\n"),
                "text/event-stream",
            )
        })
        .expect(2)
        .mount(&server)
        .await;

    let provider = Arc::new(
        OpenAIProvider::new("fixture")
            .with_base_url(server.uri())
            .with_client(reqwest::Client::builder().no_proxy().build().unwrap())
            .with_request_overrides(Some(body_override(
                "tool_choice",
                json!({"type":"function","function":{"name":"Write"}}),
            ))),
    );
    let mut session = Session::new("round-cap", MODEL);
    let (events, mut receiver) = tokio::sync::mpsc::channel(128);
    crate::runtime::runner::run_agent_loop_with_config(
        &mut session,
        "Write the artifact and verify it.".into(),
        events,
        provider,
        Arc::new(Writer(artifact.clone())),
        CancellationToken::new(),
        AgentLoopConfig {
            model_name: Some(MODEL.into()),
            provider_type: Some("openai".into()),
            run_budget: bamboo_config::RunBudgetConfig {
                max_rounds: Some(1),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let requests = capture(&server).await;
    assert_eq!(requests.len(), 2);
    assert_eq!(tool_names(&requests[0]), ["Write"]);
    assert_eq!(requests[0]["tool_choice"]["function"]["name"], "Write");
    assert!(tool_names(&requests[1]).is_empty());
    assert!(requests[1].get("tool_choice").is_none());
    assert!(requests[1].to_string().contains("Partial artifact saved."));
    assert_eq!(std::fs::read_to_string(artifact).unwrap(), "work-write");
    assert_eq!(
        session.metadata["runtime.completion_reason"],
        "max_rounds_reached"
    );
    let answer = session.messages.last().unwrap();
    assert_eq!(answer.role, bamboo_domain::Role::Assistant);
    assert_eq!(answer.content, expected_summary);
    assert!(answer.tool_calls.is_none());
    let transcript = serde_json::to_string(&session.messages).unwrap();
    assert!(!transcript.contains("over-cap-write"));
    if summary_finish_reason == "length" {
        assert!(!transcript.contains(summary));
        let mut visible_text = String::new();
        while let Ok(event) = receiver.try_recv() {
            if let bamboo_agent_core::AgentEvent::Token { content } = event {
                visible_text.push_str(&content);
            }
        }
        assert!(visible_text.contains(expected_summary));
        assert!(!visible_text.contains(summary));
    }
}

#[tokio::test]
async fn rejected_round_cap_summary_clears_responses_id_before_resume() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let requests = AtomicUsize::new(0);
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(wiremock::matchers::path("/responses"))
        .respond_with(move |_: &wiremock::Request| {
            let round = requests.fetch_add(1, Ordering::SeqCst);
            let frames = match round {
                0 => vec![
                    json!({"type":"response.output_text.delta","item_id":"msg_summary","delta":"Discard this partial summary."}),
                    json!({"type":"response.incomplete","response":{
                        "id":"resp_rejected","status":"incomplete","output":[],
                        "incomplete_details":{"reason":"max_output_tokens"}
                    }}),
                ],
                1 => vec![
                    json!({"type":"response.output_text.delta","item_id":"msg_resumed","delta":"Resumed."}),
                    json!({"type":"response.completed","response":{
                        "id":"resp_resumed","status":"completed","output":[]
                    }}),
                ],
                _ => return ResponseTemplate::new(400),
            };
            let mut body = String::new();
            for event in frames {
                let kind = event["type"].as_str().unwrap();
                body.push_str(&format!("event: {kind}\ndata: {event}\n\n"));
            }
            ResponseTemplate::new(200).set_body_raw(body, "text/event-stream")
        })
        .expect(2)
        .mount(&server)
        .await;
    let provider: Arc<dyn LLMProvider> = Arc::new(
        OpenAIProvider::new("fixture")
            .with_base_url(server.uri())
            .with_client(reqwest::Client::builder().no_proxy().build().unwrap())
            .with_responses_only_models(vec![MODEL.into()]),
    );
    let mut session = Session::new("round-cap-resume", MODEL);
    session.add_message(Message::user("Prior request"));
    session.add_message(Message::assistant("Work performed before the cap.", None));
    session.add_message(Message::user("Summarize progress"));
    let key = super::stream_execution::SESSION_RESPONSES_PREVIOUS_RESPONSE_ID_KEY;
    session.metadata.insert(key.into(), "resp_old_work".into());

    let catalog = [schema("Read")];
    let summary = run_round(
        &mut session,
        &provider,
        &catalog,
        Some(super::RoundRecoveryPolicy {
            retry: None,
            remaining_run_tokens: None,
            previous_prompt_tokens: 0,
            task_split: None,
            standalone_discovery: false,
            round_cap_summary: true,
        }),
    )
    .await
    .unwrap();
    assert!(matches!(
        summary.terminal_validation_error,
        Some(super::AgentError::OutputTokenLimit { .. })
    ));
    assert!(!session.metadata.contains_key(key));

    session.add_message(Message::assistant(
        "A complete summary was not available.",
        None,
    ));
    session.add_message(Message::user("Resume from saved work."));
    let resumed = run_round(&mut session, &provider, &catalog, None)
        .await
        .unwrap();
    assert_eq!(resumed.stream_output.content, "Resumed.");
    let bodies = capture(&server).await;
    assert!(bodies
        .iter()
        .all(|body| body.get("previous_response_id").is_none()));
    let resumed_input = bodies[1]["input"].to_string();
    assert!(resumed_input.contains("Work performed before the cap."));
    assert!(resumed_input.contains("Resume from saved work."));
    assert!(!resumed_input.contains("Discard this partial summary."));
}

#[tokio::test]
async fn child_tool_guidance_matches_current_request_and_stays_out_of_history() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(400).set_body_string("fixture request captured"))
        .mount(&server)
        .await;
    let provider: Arc<dyn LLMProvider> = Arc::new(
        OpenAIProvider::new("fixture")
            .with_base_url(server.uri())
            .with_client(reqwest::Client::builder().no_proxy().build().unwrap()),
    );
    let mut session = Session::new("child-tool-guidance", MODEL);
    session.kind = bamboo_domain::SessionKind::Child;
    session.reasoning_effort = Some(bamboo_domain::ReasoningEffort::Max);
    session.add_message(Message::user("Review the assigned change"));
    let read_only = ["Read", "Glob"].map(schema);
    send(&mut session, &provider, &read_only).await;
    record_discovery(&mut session, &read_only[1..]);
    send(&mut session, &provider, &read_only).await;
    let with_shell = ["Read", "exec_command", "write_stdin"].map(schema);
    send(&mut session, &provider, &with_shell).await;
    send(&mut session, &provider, &[]).await;
    let mut root = Session::new("root-tool-guidance-control", MODEL);
    root.add_message(Message::user("Inspect the change"));
    send(&mut root, &provider, &read_only).await;

    let requests = capture(&server).await;
    let [restricted, discovered, restored, no_tools, root] = requests.as_slice() else {
        panic!("expected five provider requests, got {}", requests.len());
    };
    assert_eq!(tool_names(restricted), ["Read", "discover_capabilities"]);
    assert_eq!(
        tool_names(discovered),
        ["Read", "Glob", "discover_capabilities"]
    );
    for request in [restricted, discovered] {
        let wire = request.to_string();
        assert_eq!(wire.matches(CHILD_GUIDANCE_TITLE).count(), 1);
        assert!(wire.contains("built-in shell command tool is unavailable on this request"));
        assert!(wire.contains("tell your parent the exact action and evidence needed"));
    }
    assert_eq!(
        tool_names(restored),
        ["Read", "exec_command", "write_stdin"]
    );
    for request in [restricted, discovered, restored, no_tools] {
        assert_eq!(request["reasoning_effort"], "max");
    }
    assert!(!restored.to_string().contains(CHILD_GUIDANCE_TITLE));
    assert!(tool_names(no_tools).is_empty());
    assert!(no_tools
        .to_string()
        .contains("This child request has no tools"));
    assert!(!root.to_string().contains(CHILD_GUIDANCE_TITLE));
    assert!(!serde_json::to_string(&session)
        .unwrap()
        .contains(CHILD_GUIDANCE_TITLE));
}

#[tokio::test]
async fn output_limit_split_does_not_advertise_discovery_on_gemini_wires() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(400).set_body_string("fixture request captured"))
        .mount(&server)
        .await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let providers: [Arc<dyn LLMProvider>; 2] = [
        Arc::new(
            GeminiProvider::new("fixture")
                .with_base_url(server.uri())
                .with_client(client.clone()),
        ),
        Arc::new(
            BodhiProvider::new("fixture")
                .with_base_url(server.uri())
                .with_client(client)
                .with_target_provider("gemini"),
        ),
    ];
    let catalog = ["Read", "Glob"].map(schema);
    for provider in &providers {
        let mut session = Session::new("split-wire", MODEL);
        session.add_message(Message::user("Read a file"));
        let result = run_round(
            &mut session,
            provider,
            &catalog,
            Some(super::RoundRecoveryPolicy {
                retry: None,
                remaining_run_tokens: None,
                previous_prompt_tokens: 0,
                task_split: Some(super::TaskSplitPrompt {
                    advisory: "Complete a smaller part of the request",
                    private_plan: false,
                }),
                round_cap_summary: false,
                standalone_discovery: false,
            }),
        )
        .await;
        assert!(result.is_err(), "the fixture rejects the captured request");
    }
    let bodies = capture(&server).await;
    assert_eq!(bodies.len(), providers.len());
    for body in &bodies {
        assert_eq!(tool_names(body), ["Read"]);
    }
}

#[tokio::test]
async fn forced_openai_function_rejects_multiple_calls_before_dispatch() {
    let named = json!({"type":"function","function":{"name":"Read"}});
    let allowlist = json!({"type":"allowed_tools","allowed_tools":{"mode":"required","tools":[
        {"type":"function","function":{"name":"Read"}}
    ]}});
    let catalog = ["Read", "Glob"].map(schema);
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    for (choice, call_count, should_reject) in [
        (named.clone(), 2, true),
        (named, 1, false),
        (allowlist, 2, false),
    ] {
        let tool_calls: Vec<_> = (0..call_count)
            .map(|index| {
                json!({
                    "index":index, "id":format!("read-{index}"), "type":"function",
                    "function":{"name":"Read","arguments":"{}"}
                })
            })
            .collect();
        let chunk = json!({
            "id":"forced-tool-response", "object":"chat.completion.chunk",
            "choices":[{"index":0,"delta":{"tool_calls":tool_calls},"finish_reason":"tool_calls"}]
        });
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(format!("data: {chunk}\n\ndata: [DONE]\n\n")),
            )
            .expect(1)
            .mount(&server)
            .await;
        let provider: Arc<dyn LLMProvider> = Arc::new(
            OpenAIProvider::new("fixture")
                .with_base_url(server.uri())
                .with_client(client.clone())
                .with_request_overrides(Some(body_override("tool_choice", choice))),
        );
        let mut session = Session::new("force-count-wire", MODEL);
        session.add_message(Message::user("Read a file"));
        let output = run_round(&mut session, &provider, &catalog, None)
            .await
            .expect("the fixture returns a completed generation");
        assert_eq!(output.stream_output.tool_calls.len(), call_count);
        if should_reject {
            assert!(matches!(
                output.terminal_validation_error,
                Some(super::AgentError::LLMNonRetryable(message)) if message.contains("exactly one")
            ));
        } else {
            assert!(output.terminal_validation_error.is_none());
        }
        let bodies = capture(&server).await;
        assert_eq!(tool_names(&bodies[0]), ["Read"]);
    }
}

#[tokio::test]
async fn named_and_allowlist_choices_reach_the_wire_only_with_eligible_declarations() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(400).set_body_string("fixture request captured"))
        .mount(&server)
        .await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let catalog = ["Read", "Glob", "GetFileInfo", "load_skill"].map(schema);
    let named = json!({"type":"function","function":{"name":"Glob"}});
    let allowed = json!({"type":"allowed_tools","mode":"required","tools":[
        {"type":"function","name":"Glob"}, {"type":"function","name":"GetFileInfo"}
    ]});
    let anth = json!({"type":"tool","name":"Glob"});
    let gemini =
        json!({"functionCallingConfig":{"mode":"ANY","allowedFunctionNames":["GetFileInfo"]}});
    let providers: Vec<(Arc<dyn LLMProvider>, &str, Value, Vec<&str>)> = vec![
        (
            Arc::new(
                OpenAIProvider::new("fixture")
                    .with_base_url(server.uri())
                    .with_client(client.clone())
                    .with_request_overrides(Some(body_override("tool_choice", named.clone()))),
            ),
            "tool_choice",
            named,
            vec!["Glob"],
        ),
        (
            Arc::new(
                OpenAIProvider::new("fixture")
                    .with_base_url(server.uri())
                    .with_client(client.clone())
                    .with_responses_only_models(vec![MODEL.into()])
                    .with_request_overrides(Some(body_override("tool_choice", allowed.clone()))),
            ),
            "tool_choice",
            allowed,
            vec!["Glob", "GetFileInfo"],
        ),
        (
            Arc::new(
                AnthropicProvider::new("fixture")
                    .with_base_url(server.uri())
                    .with_client(client.clone())
                    .with_request_overrides(Some(body_override("tool_choice", anth.clone()))),
            ),
            "tool_choice",
            anth,
            vec!["Glob"],
        ),
        (
            Arc::new(
                GeminiProvider::new("fixture")
                    .with_base_url(server.uri())
                    .with_client(client.clone())
                    .with_request_overrides(Some(body_override("toolConfig", gemini.clone()))),
            ),
            "toolConfig",
            gemini,
            vec!["GetFileInfo"],
        ),
    ];
    for (provider, _, _, _) in &providers {
        let mut session = Session::new("constrained-wire", MODEL);
        session.add_message(Message::user("Find a file"));
        session.set_root_orchestration_only(true).unwrap();
        send(&mut session, provider, &catalog).await;
    }
    let mut forced = Session::new("forced-load-skill", MODEL);
    forced.add_message(Message::user("Run the selected skill"));
    forced.metadata.insert(
        bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTION_SOURCE_KEY.into(),
        "explicit".into(),
    );
    forced.metadata.insert(
        bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.into(),
        r#"["fixture-skill"]"#.into(),
    );
    assert_eq!(
        super::required_tool_for_session(&forced),
        Some("load_skill")
    );
    send(&mut forced, &providers[0].0, &catalog).await;

    let denied: Arc<dyn LLMProvider> = Arc::new(
        OpenAIProvider::new("fixture")
            .with_base_url(server.uri())
            .with_client(client)
            .with_request_overrides(Some(body_override(
                "tool_choice",
                json!({
                    "type":"allowed_tools","allowed_tools":{"mode":"required","tools":[
                        {"type":"function","function":{"name":"Glob"}},
                        {"type":"function","function":{"name":"load_skill"}}
                    ]}
                }),
            ))),
    );
    let mut ultra = Session::new("denied-wire", MODEL);
    ultra.set_root_orchestration_only(true).unwrap();
    ultra.add_message(Message::user("Find a file"));
    send(&mut ultra, &denied, &catalog).await;

    let bodies = capture(&server).await;
    assert_eq!(
        bodies.len(),
        providers.len(),
        "the denied choice must fail before HTTP"
    );
    for (body, (_, key, choice, names)) in bodies.iter().zip(providers) {
        assert_eq!(body[key], choice);
        assert_eq!(tool_names(body), names);
    }
}
