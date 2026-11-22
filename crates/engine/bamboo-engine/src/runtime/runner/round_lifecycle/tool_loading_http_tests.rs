use std::sync::Arc;

use bamboo_agent_core::tools::{FunctionCall, FunctionSchema, ToolCall, ToolSchema};
use bamboo_agent_core::{Message, Session};
use bamboo_config::RequestOverridesConfig;
use bamboo_domain::DISCOVERY_CONTROL_FALLBACK_TOOL_NAME;
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
    session.add_message(Message::assistant(
        "",
        Some(vec![ToolCall {
            id: "discovery".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: DISCOVERY_CONTROL_FALLBACK_TOOL_NAME.into(),
                arguments: r#"{"query":"files"}"#.into(),
            },
        }]),
    ));
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
    output_limit_policy: Option<super::RoundOutputLimitPolicy<'_>>,
) -> Result<super::RoundLlmExecutionOutput, super::AgentError> {
    run_round_for_model(session, provider, catalog, output_limit_policy, MODEL).await
}

async fn run_round_for_model(
    session: &mut Session,
    provider: &Arc<dyn LLMProvider>,
    catalog: &[ToolSchema],
    output_limit_policy: Option<super::RoundOutputLimitPolicy<'_>>,
    model: &str,
) -> Result<super::RoundLlmExecutionOutput, super::AgentError> {
    let config = AgentLoopConfig {
        model_name: Some(model.into()),
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

#[cfg(feature = "litellm-wire-types")]
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
        async fn native_transcript_dispatch_snapshot(
            &self,
            _model: &str,
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
            run_round_for_model(&mut restored, &provider(key), &catalog, None, GEMINI_MODEL)
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
            Some(super::RoundOutputLimitPolicy {
                retry: None,
                remaining_run_tokens: None,
                previous_prompt_tokens: 0,
                task_split: Some(super::TaskSplitPrompt {
                    advisory: "Complete a smaller part of the request",
                    private_plan: false,
                }),
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
