use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bamboo_agent_core::tools::{ToolCall, ToolError, ToolExecutor, ToolResult, ToolSchema};
use bamboo_agent_core::{AgentError, AgentEvent, Message, Role, Session};
use bamboo_domain::session::runtime_state::RoundRuntimeState;
use bamboo_domain::ReasoningEffort;
use bamboo_llm::LLMProvider;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

use super::tests::{
    canonical_usage_pipeline_config, e2e_loop_state, mock_completion_sse, mock_messages_completion,
    OutputLimitExecutor,
};

#[derive(Default)]
struct SplitExecutor(AtomicUsize);

impl SplitExecutor {
    fn call_count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl ToolExecutor for SplitExecutor {
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(ToolResult {
            success: true,
            result: format!("confirmed result for {}", call.id),
            display_preference: None,
            images: Vec::new(),
        })
    }

    fn list_tools(&self) -> Vec<ToolSchema> {
        OutputLimitExecutor::default().list_tools()
    }
}

const CAP: u64 = 4096;
const REPEATED_REASONING: &str = "Rechecking the same assumption. ";

fn limit() -> ResponseTemplate {
    mock_messages_completion(
        json!({"type":"thinking","thinking":"","signature":""}),
        json!({"type":"thinking_delta","thinking":"Incomplete reasoning only."}),
        "max_tokens",
        Some(CAP),
    )
}

fn text(content: &str) -> ResponseTemplate {
    message_text(content, "end_turn", Some(7))
}

fn message_text(content: &str, finish: &str, tokens: Option<u64>) -> ResponseTemplate {
    mock_messages_completion(
        json!({"type":"text","text":""}),
        json!({"type":"text_delta","text":content}),
        finish,
        tokens,
    )
}

fn tool(id: &str, tokens: u64) -> ResponseTemplate {
    mock_messages_completion(
        json!({"type":"tool_use","id":id,"name":"fixture_tool","input":{}}),
        json!({"type":"input_json_delta","partial_json":"{}"}),
        "tool_use",
        Some(tokens),
    )
}

struct Run<T = SplitExecutor> {
    result: Result<bool, AgentError>,
    requests: Vec<Value>,
    session: Session,
    usage: RoundRuntimeState,
    tools: Arc<T>,
    events: Vec<AgentEvent>,
}

#[derive(Clone, Copy)]
enum Protocol {
    Messages { compatible: bool },
    Chat,
    Responses,
    Gemini,
    BodhiMessages,
    BodhiGemini,
}

impl Protocol {
    fn provider(self, server: &MockServer) -> (&'static str, Arc<dyn LLMProvider>) {
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        match self {
            Protocol::Messages { compatible } => {
                let overrides = serde_json::from_value(json!({"common":{"body_patch":[
                    {"op":"set","path":"/max_tokens","value":CAP}
                ]}}))
                .unwrap();
                (
                    "/messages",
                    Arc::new(
                        bamboo_llm::AnthropicProvider::new("test-key")
                            .with_base_url(server.uri())
                            .with_client(client)
                            .with_thinking_replay_always(compatible)
                            .with_request_overrides(Some(overrides)),
                    ),
                )
            }
            Protocol::Chat | Protocol::Responses => {
                let provider = bamboo_llm::OpenAIProvider::new("test-key")
                    .with_base_url(server.uri())
                    .with_client(client);
                if matches!(self, Protocol::Responses) {
                    (
                        "/responses",
                        Arc::new(provider.with_responses_only_models(vec!["model".into()])),
                    )
                } else {
                    ("/chat/completions", Arc::new(provider))
                }
            }
            Protocol::Gemini => (
                "/models/model:streamGenerateContent",
                Arc::new(
                    bamboo_llm::GeminiProvider::new("test-key")
                        .with_base_url(server.uri())
                        .with_client(client),
                ),
            ),
            Protocol::BodhiMessages => (
                "/proxy/anthropic/v1/messages",
                Arc::new(
                    bamboo_llm::providers::BodhiProvider::new("test-key")
                        .with_base_url(server.uri())
                        .with_target_provider("anthropic")
                        .with_client(client),
                ),
            ),
            Protocol::BodhiGemini => (
                "/proxy/gemini/v1beta/models/model:streamGenerateContent",
                Arc::new(
                    bamboo_llm::providers::BodhiProvider::new("test-key")
                        .with_base_url(server.uri())
                        .with_target_provider("gemini")
                        .with_client(client),
                ),
            ),
        }
    }
}

async fn mount_replies(server: &MockServer, route: &str, replies: Vec<ResponseTemplate>) {
    let count = replies.len();
    let index = AtomicUsize::new(0);
    Mock::given(method("POST"))
        .and(path(route))
        .respond_with(move |_: &Request| {
            let n = index.fetch_add(1, Ordering::SeqCst);
            replies
                .get(n)
                .cloned()
                .unwrap_or_else(|| ResponseTemplate::new(400))
        })
        .expect(count as u64)
        .mount(server)
        .await;
}

async fn run(
    protocol: Protocol,
    effort: Option<ReasoningEffort>,
    replies: Vec<ResponseTemplate>,
    budget: bamboo_config::RunBudgetConfig,
) -> Run {
    run_with_executor(
        protocol,
        effort,
        replies,
        budget,
        Arc::new(SplitExecutor::default()),
    )
    .await
}

async fn run_with_executor<T: ToolExecutor + 'static>(
    protocol: Protocol,
    effort: Option<ReasoningEffort>,
    replies: Vec<ResponseTemplate>,
    budget: bamboo_config::RunBudgetConfig,
    tools: Arc<T>,
) -> Run<T> {
    let count = replies.len();
    let server = MockServer::start().await;
    let (route, provider) = protocol.provider(&server);
    mount_replies(&server, route, replies).await;
    let mut config = canonical_usage_pipeline_config();
    let dir = tempfile::tempdir().unwrap();
    config.app_data_dir = Some(dir.path().into());
    config.reasoning_effort = effort;
    config.upstream_retry = bamboo_llm::retry::UpstreamRetry::UntilCancelled;
    config.run_budget = budget;
    let mut session = Session::new("task-split", "model");
    if matches!(protocol, Protocol::Responses) {
        session
            .metadata
            .insert("responses.previous_response_id".into(), "resp_prior".into());
    }
    session.add_message(Message::user(
        "Inspect the fixture and verify the reported behavior.",
    ));
    session.add_message(Message::assistant(
        "",
        Some(vec![ToolCall {
            id: "call_prior".into(),
            tool_type: "function".into(),
            function: bamboo_domain::FunctionCall {
                name: "fixture_tool".into(),
                arguments: "{}".into(),
            },
        }]),
    ));
    session.add_message(Message::tool_result("call_prior", "Verified prerequisite."));
    let mut state = e2e_loop_state("task-split");
    let (tx, mut rx) = tokio::sync::mpsc::channel(256);
    let cancel = CancellationToken::new();
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        super::run_pipeline(
            &mut session,
            &tx,
            provider,
            tools.clone(),
            &cancel,
            &config,
            &mut state,
        ),
    )
    .await
    .expect("the output-limit recovery must terminate");
    let requests: Vec<Value> = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .map(|request| request.body_json().unwrap())
        .collect();
    assert_eq!(requests.len(), count, "{result:?}");
    let events = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    Run {
        result,
        requests,
        session,
        usage: state.runtime_state.round,
        tools,
        events,
    }
}

async fn run_compatible(
    replies: Vec<ResponseTemplate>,
    budget: bamboo_config::RunBudgetConfig,
) -> Run {
    run(
        Protocol::Messages { compatible: true },
        Some(ReasoningEffort::Medium),
        replies,
        budget,
    )
    .await
}

#[cfg(unix)]
#[tokio::test]
async fn pipeline_output_limit_split_uses_foreground_bash_contract() {
    use bamboo_tools::{
        tools::foreground_bash::ForegroundBashTool, BuiltinToolExecutor, ToolRegistry,
    };

    for (command, success) in [
        ("printf 'foreground evidence'", true),
        ("printf 'foreground evidence'; exit 7", false),
    ] {
        let registry = ToolRegistry::new();
        registry.register(ForegroundBashTool).unwrap();
        let mut replies = vec![
            limit(),
            text(r#"["Run the shell command","Use its output"]"#),
            mock_messages_completion(
                json!({"type":"tool_use","id":"call_bash","name":"bash","input":{}}),
                json!({"type":"input_json_delta","partial_json":json!({"command":command}).to_string()}),
                "tool_use",
                Some(9),
            ),
        ];
        if success {
            replies.extend([
                text("CHECKPOINT: Foreground output was received."),
                text("Final foreground report."),
            ]);
        }
        let run = run_with_executor(
            Protocol::Messages { compatible: true },
            Some(ReasoningEffort::Medium),
            replies,
            Default::default(),
            Arc::new(BuiltinToolExecutor::with_registry(registry)),
        )
        .await;
        let result = run
            .session
            .messages
            .iter()
            .find(|message| {
                message.role == Role::Tool && message.tool_call_id.as_deref() == Some("call_bash")
            })
            .expect("the foreground executor must return a result for the call");
        assert_eq!(result.tool_success, Some(success));
        assert!(result.content.contains("foreground evidence"));
        if success {
            assert_eq!(result.content, "foreground evidence");
            assert!(run.result.unwrap());
            assert_eq!(
                run.session.messages.last().unwrap().content,
                "Final foreground report."
            );
        } else {
            assert!(result.content.contains("Command exited with code 7"));
            assert!(matches!(run.result, Err(AgentError::Tool(ref error))
                if error.contains("unconfirmed step")));
        }
    }
}

#[tokio::test]
async fn pipeline_output_limit_split_reuses_results_and_keeps_attempts_bounded() {
    for (compatible, effort, wire_effort) in [
        (true, ReasoningEffort::Medium, Some("medium")),
        (true, ReasoningEffort::Low, Some("low")),
        (false, ReasoningEffort::Medium, Some("medium")),
        (false, ReasoningEffort::Disabled, None),
    ] {
        let plan = r#"["Inspect one fixture","Verify one observation","Record the evidence"]"#;
        let replies = vec![
            limit(),
            text(plan),
            tool("step_a", 8),
            tool("step_b", 9),
            text("CHECKPOINT: The tool results confirm the fixture and observation."),
            text("Final verified report."),
        ];
        let run = run(
            Protocol::Messages { compatible },
            Some(effort),
            replies,
            Default::default(),
        )
        .await;
        assert!(run.result.unwrap());
        assert_eq!(run.tools.call_count(), 2);
        let stages = &run.requests[1..];
        for (i, body) in stages.iter().enumerate() {
            assert_eq!(body["max_tokens"], CAP);
            assert_eq!(
                body.pointer("/output_config/effort")
                    .and_then(Value::as_str),
                wire_effort
            );
            let messages = body["messages"].to_string();
            assert!(messages.contains("Verified prerequisite."));
            assert!(!messages.contains("Incomplete reasoning only."));
            assert_eq!(
                body.get("tools")
                    .and_then(Value::as_array)
                    .is_none_or(Vec::is_empty),
                i == 0 || i == 4,
            );
        }
        let second = stages[2]["messages"].to_string();
        assert!(second.contains("Verify one observation"));
        assert!(second.contains("confirmed result for step_a"));
        assert!(stages[4]["messages"]
            .to_string()
            .contains("confirmed result for step_b"));
        assert!(run
            .session
            .messages
            .iter()
            .all(|message| message.content != plan));
        assert!(!run
            .events
            .iter()
            .any(|event| matches!(event, AgentEvent::Token {content} if content.contains(plan))));
        assert_eq!(
            run.session.messages.last().unwrap().content,
            "Final verified report."
        );
        assert_eq!(
            run.usage.total_prompt_tokens,
            31 * run.requests.len() as u64
        );
        assert_eq!(run.usage.total_completion_tokens, CAP + 38);
    }
}

#[tokio::test]
async fn dynamic_model_window_shrinks_the_wire_output_when_the_prompt_grows() {
    use crate::runtime::runner::round_lifecycle::execute_llm_round;

    for (protocol, output_field, short_output) in [
        (Protocol::Messages { compatible: false }, "/max_tokens", CAP),
        (Protocol::Chat, "/max_tokens", 6000),
        (Protocol::Responses, "/max_output_tokens", 6000),
        (Protocol::Gemini, "/generationConfig/maxOutputTokens", 6000),
    ] {
        let server = MockServer::start().await;
        let (route, provider) = protocol.provider(&server);
        let model = match protocol {
            Protocol::Gemini => json!({"models":[{"name":"models/model",
                "max_context_tokens":10000,"inputTokenLimit":9000,"outputTokenLimit":6000}]}),
            Protocol::Messages { .. } => json!({"data":[{"id":"model",
                "max_context_tokens":10000,"max_input_tokens":9000,"max_tokens":6000}]}),
            _ => json!({"data":[{"id":"model","max_context_tokens":10000,
                "max_input_tokens":9000,"max_output_tokens":6000}]}),
        };
        Mock::given(method("GET"))
            .and(path("/models"))
            .respond_with(ResponseTemplate::new(200).set_body_json(model))
            .expect(1)
            .mount(&server)
            .await;
        let response = match protocol {
            Protocol::Messages { .. } => text("Verified."),
            Protocol::Chat => mock_completion_sse(&[json!({"type":"chunk","id":"resp",
                "choices":[{"index":0,"delta":{"content":"Verified."},"finish_reason":"stop"}],
                "usage":{"prompt_tokens":31,"completion_tokens":7}})]),
            Protocol::Responses => mock_completion_sse(&[json!({"type":"response.completed",
                "response":{"id":"resp_window","status":"completed","usage":{"input_tokens":31,"output_tokens":7},
                    "output":[{"id":"msg_window","type":"message",
                               "content":[{"type":"output_text","text":"Verified."}]}]}})]),
            Protocol::Gemini => mock_completion_sse(&[json!({"type":"chunk","candidates":[
                {"content":{"parts":[{"text":"Verified."}]},"finishReason":"STOP"}],
                "usageMetadata":{"promptTokenCount":31,"candidatesTokenCount":7}})]),
            _ => unreachable!(),
        };
        mount_replies(&server, route, vec![response.clone(), response]).await;
        let dir = tempfile::tempdir().unwrap();
        let mut config = canonical_usage_pipeline_config();
        config.provider_name = Some("configured-route".into());
        config.app_data_dir = Some(dir.path().into());
        let long_prompt = " decision".repeat(4800);
        for prompt in ["Check the result.", long_prompt.as_str()] {
            let mut session = Session::new("dynamic-window", "model");
            session.add_message(Message::user(prompt));
            let (events, _receiver) = tokio::sync::mpsc::channel(128);
            let output = execute_llm_round(
                &mut session,
                &config,
                &provider,
                &events,
                &CancellationToken::new(),
                "dynamic-window",
                "model",
                &[],
                None,
                None,
                None,
            )
            .await
            .unwrap();
            assert!(output.terminal_validation_error.is_none());
            assert_eq!(output.stream_output.content, "Verified.");
        }
        let requests = server.received_requests().await.unwrap();
        let bodies: Vec<Value> = requests
            .iter()
            .filter(|request| request.url.path() == route)
            .map(|request| request.body_json().unwrap())
            .collect();
        assert_eq!(bodies.len(), 2);
        assert_eq!(bodies[0].pointer(output_field), Some(&json!(short_output)));
        let long_output = bodies[1]
            .pointer(output_field)
            .and_then(Value::as_u64)
            .unwrap();
        assert!(
            long_output > 0 && long_output < short_output,
            "{output_field}"
        );
        assert!(bodies[1].to_string().contains(long_prompt.trim()));
    }
}

fn repeated_reasoning(protocol: Protocol) -> ResponseTemplate {
    let mut frames = Vec::new();
    if matches!(
        protocol,
        Protocol::Messages { .. } | Protocol::BodhiMessages
    ) {
        frames.push(
            json!({"type":"message_start","message":{"usage":{"input_tokens":31,
            "cache_creation_input_tokens":5,"cache_read_input_tokens":7,"output_tokens":0}}}),
        );
        frames.push(json!({"type":"content_block_start","index":0,
            "content_block":{"type":"thinking","thinking":""}}));
    }
    let reasoning = REPEATED_REASONING.repeat(96);
    for chunk in reasoning.as_bytes().chunks(137) {
        let chunk = std::str::from_utf8(chunk).unwrap();
        frames.push(match protocol {
            Protocol::Messages { .. } | Protocol::BodhiMessages => {
                json!({"type":"content_block_delta","index":0,
                "delta":{"type":"thinking_delta","thinking":chunk}})
            }
            Protocol::Chat => json!({"type":"chunk","id":"response","choices":[
                {"index":0,"delta":{"reasoning_content":chunk},"finish_reason":null}]}),
            Protocol::Responses => json!({"type":"response.reasoning_summary_text.delta",
                "item_id":"reasoning","delta":chunk}),
            Protocol::Gemini | Protocol::BodhiGemini => json!({"type":"chunk","candidates":[
                {"content":{"parts":[{"thought":true,"text":chunk}]}}]}),
        });
    }
    frames.push(json!({"type":"error","error":{"message":"stream was read past the cutoff"}}));
    mock_completion_sse(&frames)
}

#[tokio::test]
async fn pipeline_closes_repeated_reasoning_without_explicit_thinking_controls() {
    let protocol = Protocol::Messages { compatible: false };
    let run = run(
        protocol,
        None,
        vec![
            repeated_reasoning(protocol),
            text("[]"),
            text("Completed from the earlier result."),
        ],
        Default::default(),
    )
    .await;
    assert!(run.result.unwrap());
    assert_eq!(run.usage.total_prompt_tokens, 43 + 31 * 2);
    assert_eq!(run.usage.total_completion_tokens, CAP + 14);
    for body in &run.requests {
        assert!(body.get("thinking").is_none());
        let messages = body["messages"].to_string();
        assert!(messages.contains("Verified prerequisite."));
        assert!(!messages.contains(REPEATED_REASONING));
    }
}

#[tokio::test]
async fn pipeline_detects_reasoning_repetition_on_each_normalized_stream() {
    for protocol in [
        Protocol::Messages { compatible: false },
        Protocol::Chat,
        Protocol::Responses,
        Protocol::Gemini,
        Protocol::BodhiMessages,
    ] {
        let run = run(
            protocol,
            Some(ReasoningEffort::Medium),
            vec![repeated_reasoning(protocol)],
            bamboo_config::RunBudgetConfig {
                max_rounds: Some(1),
                ..Default::default()
            },
        )
        .await;
        assert!(
            matches!(run.result, Err(AgentError::ReasoningRepetition { .. })),
            "{:?}",
            run.result
        );
        let request = &run.requests[0];
        let cap = [
            "/max_tokens",
            "/max_completion_tokens",
            "/max_output_tokens",
            "/generationConfig/maxOutputTokens",
        ]
        .into_iter()
        .find_map(|path| request.pointer(path).and_then(Value::as_u64))
        .expect("the dispatched request has a wire cap");
        assert_eq!(run.usage.total_completion_tokens, cap);
        if matches!(
            protocol,
            Protocol::Messages { .. } | Protocol::BodhiMessages
        ) {
            assert_eq!(run.usage.total_prompt_tokens, 43);
        }
        assert_eq!(run.tools.call_count(), 0);
        assert_eq!(run.session.messages.len(), 3);
    }
}

#[tokio::test]
async fn pipeline_retains_usage_from_the_frame_where_reasoning_is_stopped() {
    const KNOWN_INPUT: u64 = 9321;
    let reasoning = REPEATED_REASONING.repeat(96);
    let gemini_frame = json!({"type":"chunk","candidates":[{"content":{"parts":[
        {"thought":true,"text":reasoning} ]}}],
        "usageMetadata":{"promptTokenCount":KNOWN_INPUT,"candidatesTokenCount":0,
        "thoughtsTokenCount":9}});
    for (protocol, frame) in [
        (
            Protocol::Chat,
            json!({"type":"chunk","id":"response","choices":[
            {"index":0,"delta":{"reasoning_content":reasoning},"finish_reason":null}],
            "usage":{"prompt_tokens":KNOWN_INPUT,"completion_tokens":9}}),
        ),
        (Protocol::Gemini, gemini_frame.clone()),
        (Protocol::BodhiGemini, gemini_frame),
    ] {
        let run = run(
            protocol,
            None,
            vec![mock_completion_sse(&[frame])],
            bamboo_config::RunBudgetConfig {
                max_rounds: Some(1),
                ..Default::default()
            },
        )
        .await;
        assert!(matches!(
            run.result,
            Err(AgentError::ReasoningRepetition { .. })
        ));
        assert_eq!(run.usage.total_prompt_tokens, KNOWN_INPUT);
        assert!(run.usage.total_completion_tokens > 9);
    }
}

#[tokio::test]
async fn pipeline_keeps_visible_or_tool_output_in_the_same_frame_as_repeated_reasoning() {
    let completed = |content| {
        mock_completion_sse(&[json!({"type":"chunk","candidates":[
            {"content":{"parts":[{"text":content}]},"finishReason":"STOP"}],
            "usageMetadata":{"promptTokenCount":31,"candidatesTokenCount":7,"thoughtsTokenCount":0}
        })])
    };
    for (output, expected_tool_calls) in [
        (json!({"text":"Visible progress."}), 0),
        (json!({"functionCall":{"name":"fixture_tool","args":{}}}), 1),
    ] {
        let mixed = json!({"type":"chunk","candidates":[{"content":{"parts":[
            {"thought":true,"text":REPEATED_REASONING.repeat(96)}, output
        ]}}]});
        // The mixed frame has no finish reason. Its whole contents must
        // be considered before a reasoning delta can stop the stream.
        let finish = json!({"type":"chunk","candidates":[{"finishReason":"STOP"}],
            "usageMetadata":{"promptTokenCount":31,"candidatesTokenCount":7,"thoughtsTokenCount":90}
        });
        let mut replies = vec![mock_completion_sse(&[mixed, finish])];
        if expected_tool_calls == 1 {
            replies.push(completed("Using the confirmed tool result."));
        }
        let run = run(Protocol::Gemini, None, replies, Default::default()).await;
        assert!(run.result.unwrap());
        assert_eq!(run.tools.call_count(), expected_tool_calls);
        let last = run.session.messages.last().unwrap();
        if expected_tool_calls == 0 {
            assert_eq!(last.content, "Visible progress.");
            assert_eq!(run.usage.total_completion_tokens, 97);
        } else {
            assert_eq!(last.content, "Using the confirmed tool result.");
            assert!(run.requests[1].to_string().contains("confirmed result for"));
        }
    }
}

#[tokio::test]
async fn pipeline_keeps_signed_visible_text_outside_reasoning_detection() {
    let answer = REPEATED_REASONING.trim_end().repeat(96);
    let reply = mock_completion_sse(&[
        json!({"type":"chunk","candidates":[{"content":{"parts":[
            {"text":answer,"thoughtSignature":"signature"}
        ]}}]}),
        json!({"type":"chunk","candidates":[{"finishReason":"STOP"}],
            "usageMetadata":{"promptTokenCount":31,"candidatesTokenCount":97,"thoughtsTokenCount":0}
        }),
    ]);
    let run = run(Protocol::Gemini, None, vec![reply], Default::default()).await;
    assert!(run.result.unwrap());
    let message = run.session.messages.last().unwrap();
    assert_eq!(message.content, answer);
    assert!(message.reasoning.is_none());
}

#[tokio::test]
async fn pipeline_keeps_a_provider_tool_stream_open_until_it_settles() {
    let reply = mock_completion_sse(&[
        json!({"type":"response.output_item.added",
            "item":{"type":"tool_search_call","execution":"server","id":"search","status":"in_progress"}}),
        json!({"type":"response.reasoning_summary_text.delta",
            "item_id":"reasoning","delta":REPEATED_REASONING.repeat(96)}),
        json!({"type":"response.output_text.delta","item_id":"answer","delta":"Provider work settled."}),
        json!({"type":"response.completed","response":{"id":"response","status":"completed",
            "output":[],"usage":{"input_tokens":31,"output_tokens":97}}}),
    ]);
    let run = run(Protocol::Responses, None, vec![reply], Default::default()).await;
    assert!(run.result.unwrap());
    assert_eq!(
        run.session.messages.last().unwrap().content,
        "Provider work settled."
    );
    assert_eq!(run.tools.call_count(), 0);
    assert_eq!(run.usage.total_completion_tokens, 97);
}

#[tokio::test]
async fn pipeline_accepts_long_distinct_reasoning_before_a_normal_answer() {
    use bamboo_compression::{TiktokenTokenCounter, TokenCounter};

    let reasoning: String = (0..1100)
        .map(|index| format!("Checking distinct observation {index}. "))
        .collect();
    assert!(u64::from(TiktokenTokenCounter::default().count_text(&reasoning)) > CAP * 7 / 8);
    let reply = mock_completion_sse(&[
        json!({"type":"content_block_start","index":0,
            "content_block":{"type":"thinking","thinking":""}}),
        json!({"type":"content_block_delta","index":0,
            "delta":{"type":"thinking_delta","thinking":reasoning}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}),
        json!({"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"Verified."}}),
        json!({"type":"content_block_stop","index":1}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":CAP - 1}}),
        json!({"type":"message_stop"}),
    ]);
    let run = run_compatible(vec![reply], Default::default()).await;
    assert!(run.result.unwrap());
    assert_eq!(run.session.messages.last().unwrap().content, "Verified.");
    assert_eq!(run.usage.total_completion_tokens, CAP - 1);
}

#[tokio::test]
async fn pipeline_stops_when_a_recovery_step_repeats_its_reasoning() {
    let protocol = Protocol::Messages { compatible: true };
    let run = run_compatible(
        vec![
            repeated_reasoning(protocol),
            text(r#"["Read the fixture","Verify the result"]"#),
            tool("confirmed_step", 8),
            repeated_reasoning(protocol),
        ],
        Default::default(),
    )
    .await;
    assert!(matches!(
        run.result,
        Err(AgentError::ReasoningRepetition { .. })
    ));
    assert_eq!(run.usage.total_completion_tokens, CAP * 2 + 15);
    assert_eq!(run.tools.call_count(), 1);
    let last_request = run.requests.last().unwrap()["messages"].to_string();
    assert!(last_request.contains("Verified prerequisite."));
    assert!(last_request.contains("confirmed result for confirmed_step"));
    assert!(!last_request.contains(REPEATED_REASONING));
    assert_eq!(
        run.session.messages.last().unwrap().content,
        "confirmed result for confirmed_step"
    );
}

#[tokio::test]
async fn pipeline_stops_repetition_even_when_recovery_cannot_fit_the_budget() {
    let protocol = Protocol::Messages { compatible: true };
    let run = run_compatible(
        vec![repeated_reasoning(protocol)],
        bamboo_config::RunBudgetConfig {
            max_total_tokens: Some(2 * CAP),
            ..Default::default()
        },
    )
    .await;
    assert!(matches!(
        run.result,
        Err(AgentError::ReasoningRepetition { .. })
    ));
    assert_eq!(run.usage.total_completion_tokens, CAP);
    assert_eq!(run.tools.call_count(), 0);
}

#[tokio::test]
async fn pipeline_splits_a_capped_messages_turn_without_reasoning_or_a_complete_tool() {
    const CAPPED_TOOL_ID: &str = "capped_tool_must_not_replay_2757";
    let opaque = mock_completion_sse(&[
        json!({"type":"message_delta","delta":{"stop_reason":"max_tokens"},"usage":{"output_tokens":CAP}}),
        json!({"type":"message_stop"}),
    ]);
    let provisional = mock_messages_completion(
        json!({"type":"tool_use","id":CAPPED_TOOL_ID,"name":"fixture_tool","input":{}}),
        json!({"type":"input_json_delta","partial_json":"{}"}),
        "max_tokens",
        Some(CAP),
    );
    for reply in [opaque, provisional] {
        let run = run(
            Protocol::Messages { compatible: false },
            None,
            vec![
                reply,
                text(r#"["Check the fixture once"]"#),
                tool("complete", 8),
                text("Completed from the confirmed results."),
            ],
            Default::default(),
        )
        .await;
        assert!(run.result.unwrap());
        assert_eq!(run.tools.call_count(), 1);
        let report = run.requests[3]["messages"].to_string();
        assert!(report.contains("Verified prerequisite."));
        assert!(report.contains("confirmed result for complete"));
        assert!(!report.contains(CAPPED_TOOL_ID));
    }
}

#[tokio::test]
async fn pipeline_output_limit_split_uses_only_an_explicit_total_token_budget() {
    let first_tool = mock_completion_sse(&[
        json!({"type":"message_start","message":{"usage":{
            "input_tokens":31,"cache_creation_input_tokens":0,"cache_read_input_tokens":250_000
        }}}),
        json!({"type":"content_block_start","index":0,"content_block":{
            "type":"tool_use","id":"prior_work","name":"fixture_tool","input":{}
        }}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{}"}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":8}}),
        json!({"type":"message_stop"}),
    ]);
    let mut replies = vec![first_tool, limit()];
    let explicit_limit = 256_000;
    let bounded = run_compatible(
        replies.clone(),
        bamboo_config::RunBudgetConfig {
            max_total_tokens: Some(explicit_limit),
            ..Default::default()
        },
    )
    .await;
    assert!(matches!(
        bounded.result,
        Err(AgentError::OutputTokenLimit { .. })
    ));
    assert_eq!(bounded.usage.total_prompt_tokens, 250_062);
    assert_eq!(bounded.tools.call_count(), 1);
    assert_eq!(
        bounded.session.messages.last().unwrap().content,
        "confirmed result for prior_work"
    );

    replies.extend([
        text(r#"["Verify one observation"]"#),
        tool("verification", 3000),
        text("Completed verification."),
    ]);
    let run = run_compatible(replies, Default::default()).await;
    assert!(run.result.unwrap());
    assert_eq!(run.tools.call_count(), 2);
    assert!(run.usage.total_prompt_tokens + run.usage.total_completion_tokens > explicit_limit);
    let report_history = run.requests[4]["messages"].to_string();
    assert!(report_history.contains("confirmed result for prior_work"));
    assert!(report_history.contains("confirmed result for verification"));
    assert_eq!(
        run.session.messages.last().unwrap().content,
        "Completed verification."
    );
}

#[tokio::test]
async fn pipeline_output_limit_split_stops_at_token_and_round_limits() {
    for budget in [
        bamboo_config::RunBudgetConfig {
            max_total_tokens: Some(12_500),
            ..Default::default()
        },
        bamboo_config::RunBudgetConfig {
            max_rounds: Some(3),
            ..Default::default()
        },
        bamboo_config::RunBudgetConfig {
            max_tool_calls: Some(1),
            ..Default::default()
        },
    ] {
        let run = run_compatible(
            vec![
                limit(),
                text(r#"["Inspect one fixture","Verify one observation"]"#),
                tool("step_a", CAP),
            ],
            budget,
        )
        .await;
        assert!(
            matches!(run.result, Err(AgentError::Budget(_))),
            "{:?}",
            run.result
        );
        assert_eq!(run.tools.call_count(), 1);
        assert_eq!(run.usage.total_completion_tokens, CAP * 2 + 7);
        assert!(run.session.messages.iter().any(|message| {
            message.role == Role::Tool && message.content.contains("confirmed result for step_a")
        }));
    }
}

#[tokio::test]
async fn pipeline_output_limit_split_rejects_unbounded_or_failed_plans() {
    for (reply, reason) in [
        (text(r#"["one","two","three","four"]"#), "plan was invalid"),
        (
            tool("plan_must_not_execute", 8),
            "tool outside the current step",
        ),
        (ResponseTemplate::new(429), "429"),
    ] {
        let run = run_compatible(vec![limit(), reply], Default::default()).await;
        assert!(
            run.result.unwrap_err().to_string().contains(reason),
            "{reason}"
        );
        assert_eq!(run.tools.call_count(), 0);
        assert_eq!(run.session.messages.len(), 3);
    }
}

#[tokio::test]
async fn pipeline_uses_one_confirmed_action_after_a_capped_recovery_plan() {
    for step_completes in [true, false] {
        let mut replies = vec![
            limit(),
            message_text(r#"["Discard this partial plan"#, "max_tokens", Some(CAP)),
        ];
        if step_completes {
            replies.extend([
                tool("step_retained", 8),
                text("Completed from the confirmed tools."),
            ]);
        } else {
            replies.push(limit());
        }
        let run = run_compatible(replies, Default::default()).await;
        let work_request = run.requests[2]["messages"].to_string();
        assert!(work_request.contains("next useful unfinished action"));
        assert!(work_request.contains("Verified prerequisite."));
        assert!(!work_request.contains("Discard this partial plan"));
        if step_completes {
            assert!(run.result.unwrap());
            assert_eq!(run.tools.call_count(), 1);
            assert!(run.requests[3]["messages"]
                .to_string()
                .contains("confirmed result for step_retained"));
            assert_eq!(run.usage.total_completion_tokens, 2 * CAP + 15);
        } else {
            assert!(matches!(
                run.result,
                Err(AgentError::OutputTokenLimit { .. })
            ));
            assert_eq!(run.tools.call_count(), 0);
            assert_eq!(run.usage.total_completion_tokens, 3 * CAP);
            assert!(run
                .session
                .messages
                .iter()
                .any(|message| message.content == "Verified prerequisite."));
        }
    }
}

#[tokio::test]
async fn pipeline_stops_if_a_capped_plan_contains_provider_work_or_leaves_too_little_budget() {
    let unexpected_provider_work = mock_completion_sse(&[
        json!({"type":"content_block_start","index":0,"content_block":{
            "type":"server_tool_use","id":"srv_plan","name":"web_search","input":{}
        }}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"message_delta","delta":{"stop_reason":"max_tokens"},"usage":{"output_tokens":CAP}}),
        json!({"type":"message_stop"}),
    ]);
    let capped_plan = mock_completion_sse(&[
        json!({"type":"message_start","message":{"usage":{"input_tokens":10_000,
            "cache_creation_input_tokens":0,"cache_read_input_tokens":0}}}),
        json!({"type":"message_delta","delta":{"stop_reason":"max_tokens"},"usage":{"output_tokens":CAP}}),
        json!({"type":"message_stop"}),
    ]);
    for (plan, max_total_tokens, expected_error) in [
        (unexpected_provider_work, None, "output token limit"),
        (capped_plan, Some(26_000), "budget"),
    ] {
        let run = run_compatible(
            vec![limit(), plan],
            bamboo_config::RunBudgetConfig {
                max_total_tokens,
                ..Default::default()
            },
        )
        .await;
        assert!(run
            .result
            .unwrap_err()
            .to_string()
            .to_lowercase()
            .contains(expected_error));
        assert_eq!(run.tools.call_count(), 0);
        assert_eq!(
            run.session.messages.last().unwrap().content,
            "Verified prerequisite."
        );
        if max_total_tokens.is_some() {
            assert_eq!(run.usage.total_prompt_tokens, 10_031);
            assert_eq!(run.usage.total_completion_tokens, CAP * 2);
        }
    }
}

#[tokio::test]
async fn pipeline_output_token_limit_recovery_never_retries_after_a_failed_http_send() {
    for (effort, response, status) in [
        (ReasoningEffort::Medium, ResponseTemplate::new(429), "429"),
        (
            ReasoningEffort::Low,
            ResponseTemplate::new(307).insert_header("location", "/messages"),
            "307",
        ),
    ] {
        let run = run(
            Protocol::Messages { compatible: false },
            Some(effort),
            vec![limit(), response],
            Default::default(),
        )
        .await;
        assert!(run.result.unwrap_err().to_string().contains(status));
        assert_eq!(run.tools.call_count(), 0);
        assert_eq!(run.requests[1]["output_config"]["effort"], effort.as_str());
    }
}

#[tokio::test]
async fn pipeline_output_limit_split_rejects_a_tool_before_messages_stop() {
    let incomplete = mock_completion_sse(&[
        json!({"type":"content_block_start","index":0,"content_block":{
            "type":"tool_use","id":"partial_tool","name":"fixture_tool","input":{}
        }}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{}"}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":8}}),
    ]);
    let run = run_compatible(
        vec![limit(), text(r#"["Check the fixture"]"#), incomplete],
        Default::default(),
    )
    .await;
    assert!(run
        .result
        .unwrap_err()
        .to_string()
        .contains("without a successful provider completion"));
    assert_eq!(run.tools.call_count(), 0);
    assert_eq!(run.session.messages.len(), 3);
}

#[tokio::test]
async fn pipeline_output_limit_split_reserves_confirmed_cache_and_refuses_provider_work() {
    for server_tool in [false, true] {
        let mut frames = vec![
            json!({"type":"message_start","message":{"usage":{"input_tokens":31,"cache_read_input_tokens":9_000}}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"Still reasoning"}}),
            json!({"type":"content_block_stop","index":0}),
        ];
        if server_tool {
            frames.extend([
                json!({"type":"content_block_start","index":1,"content_block":{"type":"server_tool_use","id":"srv_1","name":"web_search","input":{}}}),
                json!({"type":"content_block_stop","index":1}),
            ]);
        }
        frames.extend([
            json!({"type":"message_delta","delta":{"stop_reason":"max_tokens"},"usage":{"output_tokens":CAP}}),
            json!({"type":"message_stop"}),
        ]);
        let run = run_compatible(
            vec![mock_completion_sse(&frames)],
            bamboo_config::RunBudgetConfig {
                max_total_tokens: (!server_tool).then_some(20_000),
                ..Default::default()
            },
        )
        .await;
        assert!(
            matches!(run.result, Err(AgentError::OutputTokenLimit { reasoning_only, .. }) if reasoning_only != server_tool)
        );
        assert!(run.usage.total_prompt_tokens >= 9_031);
    }
}

#[test]
fn output_limit_split_restricts_delegation_aliases_and_requires_terminal_shell_results() {
    use super::super::output_limit_split::{confirms_tool_result, forbidden_tool};
    use bamboo_tools::tools::foreground_bash::FOREGROUND_BASH_EXECUTION_CONTRACT;
    for name in ["default::SubAgent", "default::spawnSession", "deploy_agent"] {
        assert!(forbidden_tool(name), "{name}");
    }
    assert!(!forbidden_tool("fixture_tool"));
    for (result, success) in [
        (json!({"status":"running","exit_code":null}), false),
        (json!({"status":"exited","exit_code":1}), false),
        (
            json!({"status":"error","exit_code":0,"error":"read failed"}),
            false,
        ),
        (json!({"status":"exited","exit_code":0}), true),
    ] {
        let message = Message::tool_result("command", result.to_string());
        assert_eq!(
            confirms_tool_result("default::exec_command", "command", None, &message),
            success
        );
    }
    assert!(confirms_tool_result(
        "Bash",
        "command",
        None,
        &Message::tool_result("command", r#"{"exit_code":0}"#),
    ));

    let output = Message::tool_result("command", "plain foreground output");
    let contract = Some(FOREGROUND_BASH_EXECUTION_CONTRACT);
    assert!(!confirms_tool_result("bash", "command", None, &output));
    assert!(!confirms_tool_result("bash", "other", contract, &output));
    assert!(confirms_tool_result("bash", "command", contract, &output));
}

#[tokio::test]
async fn pipeline_output_limit_split_counts_separate_gemini_proxy_reasoning() {
    let run = run(
        Protocol::BodhiGemini,
        Some(ReasoningEffort::Low),
        vec![mock_completion_sse(&[json!({
            "type":"chunk","candidates":[{"content":{"parts":[{"text":"Thinking","thought":true}]},"finishReason":"MAX_TOKENS"}],
            "usageMetadata":{"promptTokenCount":31,"candidatesTokenCount":7,"thoughtsTokenCount":8000}
        })])],
        bamboo_config::RunBudgetConfig { max_total_tokens: Some(8000), ..Default::default() },
    ).await;
    assert!(matches!(
        run.result,
        Err(AgentError::OutputTokenLimit { .. })
    ));
    assert_eq!(run.usage.total_completion_tokens, 8007);
}

#[tokio::test]
async fn pipeline_output_limit_split_stops_on_blocked_or_truncated_work() {
    let blocked = "BLOCKED: The second fixture is missing.";
    let clipped = "CHECKPOINT: The fixture might";
    for (reply, reason, retained) in [
        (text(blocked), "unconfirmed step", Some(blocked)),
        (
            message_text(clipped, "max_tokens", Some(CAP)),
            "output token limit",
            None,
        ),
    ] {
        let run = run_compatible(
            vec![
                limit(),
                text(r#"["Check fixture one","Check fixture two"]"#),
                reply,
            ],
            Default::default(),
        )
        .await;
        assert!(run.result.unwrap_err().to_string().contains(reason));
        assert_eq!(run.tools.call_count(), 0);
        assert_eq!(
            run.session
                .messages
                .get(3)
                .map(|message| message.content.as_str()),
            retained
        );
    }
}

#[tokio::test]
async fn pipeline_output_limit_split_reserves_missing_gemini_candidate_count() {
    let reasoning = mock_completion_sse(&[json!({
        "type":"chunk","candidates":[{"content":{"parts":[{"text":"Thinking","thought":true}]},"finishReason":"MAX_TOKENS"}],
        "usageMetadata":{"promptTokenCount":31,"candidatesTokenCount":0,"thoughtsTokenCount":7}
    })]);
    let answer = mock_completion_sse(&[json!({
        "type":"chunk","candidates":[{"content":{"parts":[{"text":"Recovered"}]},"finishReason":"STOP"}],
        "usageMetadata":{"promptTokenCount":31,"thoughtsTokenCount":7}
    })]);
    let run = run(
        Protocol::Gemini,
        Some(ReasoningEffort::Low),
        vec![reasoning, answer],
        Default::default(),
    )
    .await;
    assert!(run.result.unwrap());
    let wire_cap = run.requests[1]["generationConfig"]["maxOutputTokens"]
        .as_u64()
        .unwrap();
    assert_eq!(run.usage.total_completion_tokens, wire_cap + 7);
}

#[tokio::test]
async fn output_limit_step_failure_ends_execution_and_next_turn_uses_saved_results() {
    use crate::runtime::{tests::build_direct_execute_agent, ExecuteRequestBuilder};
    use bamboo_domain::AgentStatusState;

    let server = MockServer::start().await;
    let (route, provider) = Protocol::Messages { compatible: true }.provider(&server);
    Mock::given(method("GET"))
        .and(path("/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data":[{"id":"model"}]})))
        .expect(1)
        .mount(&server)
        .await;
    mount_replies(
        &server,
        route,
        vec![
            limit(),
            text(r#"["Read the fixture","Check the result"]"#),
            tool("step_retained", 8),
            limit(),
            text("Finished using the saved result."),
        ],
    )
    .await;
    let tools = Arc::new(SplitExecutor::default());
    let (dir, agent, storage) =
        build_direct_execute_agent(provider, None, Some(tools.clone())).await;
    let mut session = Session::new("output-limit-reentry", "model");
    storage.save_session(&session).await.unwrap();
    let run_budget = bamboo_config::RunBudgetConfig {
        max_rounds: Some(4),
        max_total_tokens: Some(20_000),
        ..Default::default()
    };
    let request = |message, tx| {
        ExecuteRequestBuilder::new(message, tx, CancellationToken::new())
            .model("model")
            .provider_type("anthropic")
            .reasoning_effort(ReasoningEffort::Medium)
            .app_data_dir(dir.path().to_path_buf())
            .run_budget(run_budget)
            .build()
    };
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(256);
    let first = tokio::time::timeout(
        Duration::from_secs(10),
        agent.execute(&mut session, request("Inspect the fixture.", event_tx)),
    )
    .await
    .expect("the exhausted run must release its execution");
    assert!(
        matches!(
            first,
            Err(AgentError::OutputTokenLimit {
                reasoning_only: true,
                ..
            })
        ),
        "{first:?}"
    );
    let sends = server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|request| request.method.as_str() == "POST");
    assert_eq!(sends.count(), 4);
    tokio::time::timeout(Duration::from_secs(1), async {
        while let Some(event) = event_rx.recv().await {
            assert!(!matches!(event, AgentEvent::Complete { .. }));
        }
    })
    .await
    .expect("failed run event stream must close");

    let mut saved = storage.load_session(&session.id).await.unwrap().unwrap();
    let failed_run = saved.agent_runtime_state.as_ref().unwrap();
    assert_eq!(failed_run.status, AgentStatusState::Failed);
    assert!(saved.messages.iter().any(|message| {
        message.tool_success == Some(true)
            && message.content == "confirmed result for step_retained"
    }));

    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(256);
    tokio::time::timeout(
        Duration::from_secs(10),
        agent.execute(
            &mut saved,
            request("Continue from the saved result.", event_tx),
        ),
    )
    .await
    .expect("a new turn must be admitted")
    .unwrap();
    let completed = storage.load_session(&session.id).await.unwrap().unwrap();
    let completed_run = completed.agent_runtime_state.as_ref().unwrap();
    assert_eq!(completed_run.status, AgentStatusState::Completed);
    assert_eq!(completed_run.round.total_completion_tokens, 7);
    let requests = server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|request| request.method.as_str() == "POST")
        .collect::<Vec<_>>();
    assert_eq!(requests.len(), 5);
    let body: Value = requests[4].body_json().unwrap();
    assert!(body["messages"]
        .to_string()
        .contains("confirmed result for step_retained"));
    assert_eq!(tools.0.load(Ordering::SeqCst), 1);
    assert!(std::iter::from_fn(|| event_rx.try_recv().ok())
        .any(|event| matches!(event, AgentEvent::Complete { .. })));
}

#[tokio::test]
async fn pipeline_output_limit_split_rejects_tool_batches_before_dispatch() {
    let mut events = vec![json!({"type":"message_start","message":{"usage":{
        "input_tokens":31,"cache_creation_input_tokens":0,"cache_read_input_tokens":0
    }}})];
    for index in 0..2 {
        events.extend([
            json!({"type":"content_block_start","index":index,"content_block":{
                "type":"tool_use","id":format!("forbidden_{index}"),"name":"fixture_tool","input":{}
            }}),
            json!({"type":"content_block_stop","index":index}),
        ]);
    }
    events.extend([
        json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":8}}),
        json!({"type":"message_stop"}),
    ]);
    let run = run_compatible(
        vec![
            limit(),
            text(r#"["Check the fixture"]"#),
            mock_completion_sse(&events),
        ],
        Default::default(),
    )
    .await;
    let error = run.result.unwrap_err().to_string();
    assert!(error.contains("tool outside the current step"), "{error}");
    assert_eq!(run.tools.call_count(), 0);
    assert_eq!(run.session.messages.len(), 3);
}

#[tokio::test]
async fn pipeline_output_limit_split_reserves_full_output_when_usage_is_missing() {
    let run = run_compatible(
        vec![
            limit(),
            message_text(r#"["Check the fixture"]"#, "end_turn", None),
        ],
        bamboo_config::RunBudgetConfig {
            max_total_tokens: Some(3 * (CAP + 31)),
            ..Default::default()
        },
    )
    .await;
    assert!(matches!(run.result, Err(AgentError::Budget(_))));
    assert_eq!(run.usage.total_completion_tokens, 2 * CAP);
    assert_eq!(run.tools.call_count(), 0);
}

#[tokio::test]
async fn pipeline_output_limit_split_rejects_tools_when_reported_usage_exceeds_budget() {
    let run = run_compatible(
        vec![
            limit(),
            text(r#"["Check the fixture"]"#),
            tool("over_budget", CAP * 4),
        ],
        bamboo_config::RunBudgetConfig {
            max_total_tokens: Some(12_500),
            ..Default::default()
        },
    )
    .await;
    assert!(matches!(run.result, Err(AgentError::Budget(_))));
    assert_eq!(run.usage.total_completion_tokens, 5 * CAP + 7);
    assert_eq!(run.tools.call_count(), 0);
}

#[tokio::test]
async fn pipeline_output_limit_split_runs_after_convergence_on_other_protocols() {
    for (protocol, prompt, cap, effort_path, wire_effort) in [
        (
            Protocol::Chat,
            "messages",
            "/max_completion_tokens",
            "/reasoning_effort",
            "low",
        ),
        (
            Protocol::Responses,
            "input",
            "/max_output_tokens",
            "/reasoning/effort",
            "low",
        ),
        (
            Protocol::Gemini,
            "contents",
            "/generationConfig/maxOutputTokens",
            "/generationConfig/thinkingConfig/thinkingLevel",
            "LOW",
        ),
    ] {
        let reply = |id: &str, content: Option<&str>| {
            let event = match protocol {
                Protocol::Chat => {
                    let (delta, reason) = content.map_or(
                        (
                            json!({"reasoning_content":"Unfinished reasoning"}),
                            "length",
                        ),
                        |text| (json!({"content":text}), "stop"),
                    );
                    json!({"type":"chunk","choices":[{"delta":delta,"finish_reason":reason}],
                        "usage":{"prompt_tokens":31,"completion_tokens":7}})
                }
                Protocol::Responses => {
                    if let Some(text) = content {
                        json!({"type":"response.completed","response":{"id":format!("resp_{id}"),
                            "status":"completed","usage":{"input_tokens":31,"output_tokens":7},
                            "output":[{"id":format!("msg_{id}"),"type":"message",
                                    "content":[{"type":"output_text","text":text}]}]}})
                    } else {
                        json!({"type":"response.incomplete","response":{"id":format!("resp_{id}"),
                            "status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},
                            "usage":{"input_tokens":31,"output_tokens":7,"output_tokens_details":{"reasoning_tokens":7}},
                            "output":[{"id":format!("rs_{id}"),"type":"reasoning","summary":[]}]}})
                    }
                }
                Protocol::Gemini | Protocol::BodhiGemini => {
                    let (part, reason) = content.map_or(
                        (
                            json!({"text":"Unfinished reasoning","thought":true}),
                            "MAX_TOKENS",
                        ),
                        |text| (json!({"text":text}), "STOP"),
                    );
                    json!({"type":"chunk","candidates":[{"content":{"parts":[part]},"finishReason":reason}],
                        "usageMetadata":{"promptTokenCount":31,"candidatesTokenCount":if content.is_some() {7} else {0},
                            "thoughtsTokenCount":if content.is_some() {0} else {7}}})
                }
                Protocol::Messages { .. } | Protocol::BodhiMessages => unreachable!(),
            };
            mock_completion_sse(&[event])
        };
        let responses = matches!(protocol, Protocol::Responses);
        let mut replies = vec![reply("initial", None)];
        if responses {
            replies.push(reply("lower", None));
        }
        let convergence = replies.len();
        replies.extend([
            reply("convergence", None),
            reply("plan", Some(r#"["Check the verified prerequisite"]"#)),
            reply(
                "work",
                Some("CHECKPOINT: The prior tool result verified the prerequisite."),
            ),
            reply("report", Some("The prerequisite is verified.")),
        ]);
        let run = run(
            protocol,
            Some(if responses {
                ReasoningEffort::Max
            } else {
                ReasoningEffort::Low
            }),
            replies,
            Default::default(),
        )
        .await;
        assert!(run.result.unwrap());
        assert_eq!(
            run.session.messages.last().unwrap().content,
            "The prerequisite is verified."
        );
        assert_eq!(
            run.usage.total_prompt_tokens,
            31 * run.requests.len() as u64
        );
        assert_eq!(
            run.usage.total_completion_tokens,
            7 * run.requests.len() as u64
        );
        if responses {
            let mut before = run.requests[0].clone();
            before["reasoning"]["effort"] = json!("low");
            assert_eq!(run.requests[1], before);
        }
        assert!(run.requests[convergence][prompt]
            .to_string()
            .contains(bamboo_llm::provider::OUTPUT_LIMIT_CONVERGENCE_HINT));
        let original_cap = run.requests[0].pointer(cap).unwrap();
        for body in &run.requests[convergence..] {
            assert_eq!(body.pointer(cap), Some(original_cap));
            assert_eq!(body.pointer(effort_path), Some(&json!(wire_effort)));
            assert!(body.get("previous_response_id").is_none());
            let input = body[prompt].to_string();
            assert!(input.contains("Verified prerequisite."));
            assert!(!input.contains("Unfinished reasoning"));
            assert!(!input.contains("rs_initial") && !input.contains("rs_lower"));
        }
    }
}
