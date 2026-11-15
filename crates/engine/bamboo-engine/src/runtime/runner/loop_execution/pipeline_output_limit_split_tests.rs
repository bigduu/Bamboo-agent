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

struct Run {
    result: Result<bool, AgentError>,
    requests: Vec<Value>,
    session: Session,
    usage: RoundRuntimeState,
    tool_count: usize,
    events: Vec<AgentEvent>,
}

#[derive(Clone, Copy)]
enum Protocol {
    Messages { compatible: bool },
    Chat,
    Responses,
    Gemini,
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
    effort: ReasoningEffort,
    replies: Vec<ResponseTemplate>,
    budget: bamboo_config::RunBudgetConfig,
) -> Run {
    let count = replies.len();
    let server = MockServer::start().await;
    let (route, provider) = protocol.provider(&server);
    mount_replies(&server, route, replies).await;
    let mut config = canonical_usage_pipeline_config();
    let dir = tempfile::tempdir().unwrap();
    config.app_data_dir = Some(dir.path().into());
    config.reasoning_effort = Some(effort);
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
    let tools = Arc::new(SplitExecutor::default());
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
        tool_count: tools.0.load(Ordering::SeqCst),
        events,
    }
}

async fn run_compatible(
    replies: Vec<ResponseTemplate>,
    budget: bamboo_config::RunBudgetConfig,
) -> Run {
    run(
        Protocol::Messages { compatible: true },
        ReasoningEffort::Medium,
        replies,
        budget,
    )
    .await
}

#[tokio::test]
async fn pipeline_output_limit_split_reuses_results_and_keeps_attempts_bounded() {
    for (compatible, effort, plan_index, wire_effort) in [
        (true, ReasoningEffort::Medium, 1, "medium"),
        (true, ReasoningEffort::Low, 1, "low"),
        (false, ReasoningEffort::Medium, 3, "low"),
    ] {
        let mut replies = vec![limit(); plan_index];
        let plan = r#"["Inspect one fixture","Verify one observation","Record the evidence"]"#;
        replies.extend([
            text(plan),
            tool("step_a", 8),
            tool("step_b", 9),
            text("CHECKPOINT: The tool results confirm the fixture and observation."),
            text("Final verified report."),
        ]);
        let run = run(
            Protocol::Messages { compatible },
            effort,
            replies,
            Default::default(),
        )
        .await;
        assert!(run.result.unwrap());
        assert_eq!(run.tool_count, 2);
        let stages = &run.requests[plan_index..];
        for (i, body) in stages.iter().enumerate() {
            assert_eq!(body["max_tokens"], CAP);
            assert_eq!(body["output_config"]["effort"], wire_effort);
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
        assert_eq!(
            run.usage.total_completion_tokens,
            CAP * plan_index as u64 + 38
        );
    }
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
        assert_eq!(run.tool_count, 1);
        assert_eq!(run.usage.total_completion_tokens, CAP * 2 + 7);
        assert!(run.session.messages.iter().any(|message| {
            message.role == Role::Tool && message.content.contains("confirmed result for step_a")
        }));
    }
}

#[tokio::test]
async fn pipeline_output_limit_split_rejects_unbounded_or_failed_plans() {
    for (reply, reason) in [
        (limit(), "output token limit"),
        (
            message_text("[]", "max_tokens", Some(CAP)),
            "output token limit",
        ),
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
        assert_eq!(run.tool_count, 0);
        assert_eq!(run.session.messages.len(), 3);
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
            effort,
            vec![limit(), response],
            Default::default(),
        )
        .await;
        assert!(run.result.unwrap_err().to_string().contains(status));
        assert_eq!(run.tool_count, 0);
        assert_eq!(run.requests[1]["output_config"]["effort"], "low");
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
    assert_eq!(run.tool_count, 0);
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
            confirms_tool_result("default::exec_command", "command", &message),
            success
        );
    }
    assert!(confirms_tool_result(
        "Bash",
        "command",
        &Message::tool_result("command", r#"{"exit_code":0}"#),
    ));
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
        assert_eq!(run.tool_count, 0);
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
async fn output_limit_step_failure_ends_execution_and_next_turn_uses_saved_results() {
    use crate::runtime::{tests::build_direct_execute_agent, ExecuteRequestBuilder};
    use bamboo_domain::AgentStatusState;

    let server = MockServer::start().await;
    let (route, provider) = Protocol::Messages { compatible: true }.provider(&server);
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
    assert_eq!(server.received_requests().await.unwrap().len(), 4);
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
    let requests = server.received_requests().await.unwrap();
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
    assert_eq!(run.tool_count, 0);
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
    assert_eq!(run.tool_count, 0);
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
    assert_eq!(run.tool_count, 0);
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
                Protocol::Gemini => {
                    let (part, reason) = content.map_or(
                        (
                            json!({"text":"Unfinished reasoning","thought":true}),
                            "MAX_TOKENS",
                        ),
                        |text| (json!({"text":text}), "STOP"),
                    );
                    json!({"type":"chunk","candidates":[{"content":{"parts":[part]},"finishReason":reason}],
                        "usageMetadata":{"promptTokenCount":31,"candidatesTokenCount":7}})
                }
                Protocol::Messages { .. } => unreachable!(),
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
            if responses {
                ReasoningEffort::Max
            } else {
                ReasoningEffort::Low
            },
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
