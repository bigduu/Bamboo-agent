use super::*;
use bamboo_agent_core::AgentError;
use bamboo_llm::providers::anthropic::AnthropicProvider;
use futures::StreamExt;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

#[tokio::test]
async fn actual_provider_failure_receipt_reaches_the_session_and_survives_cold_storage() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(recovery())
        .mount(&server)
        .await;
    let provider = Arc::new(AnthropicProvider::new("fixture-key").with_base_url(server.uri()));
    let (_temp, agent, storage) = build_direct_execute_agent(provider, None, None).await;
    let mut session = Session::new("persistent-provider-diagnostic", "test-model");
    session.add_message(Message::user("one synthetic request"));
    storage.save_session(&session).await.unwrap();
    let (tx, _rx) = mpsc::channel(128);
    let error = agent
        .execute(&mut session, direct_request(tx, CancellationToken::new()))
        .await
        .unwrap_err();
    assert!(matches!(error, AgentError::LLMNonRetryable(_)));
    let captured = bamboo_domain::LlmRequestDiagnostic::from_json(
        session
            .metadata
            .get(bamboo_domain::LAST_RUN_LLM_DIAGNOSTIC_KEY)
            .expect("the exact bootstrap observer must reach the executing Session"),
    )
    .unwrap();
    assert!(captured.matches_error_message(&error.to_string()));
    // The hosted terminal owner already performs these existing operations;
    // this component test exercises the same durable Session representation.
    session.set_last_run_error(error.to_string());
    storage.save_session(&session).await.unwrap();
    let cold = storage.load_session(&session.id).await.unwrap().unwrap();
    let persisted = cold.last_run_diagnostic().unwrap();
    assert_eq!(persisted.diagnostic_id(), captured.diagnostic_id());
    assert!(persisted.matches_error_message(&cold.last_run_error().unwrap()));
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        bamboo_llm::retry::global().max_attempts as usize
    );
}

const RECOVERY: &str = "Authentication Error, Error in connector: Error querying the database: FATAL: the database system is not yet accepting connections\nDETAIL: Consistent recovery state has not been yet reached.";

fn recovery() -> ResponseTemplate {
    ResponseTemplate::new(401)
        .insert_header("Retry-After", "0")
        .set_body_json(serde_json::json!({"error":{"type":"auth_error","message":RECOVERY}}))
}

struct CountingTool(AtomicUsize);

#[async_trait]
impl ToolExecutor for CountingTool {
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        SuccessfulReadToolExecutor.execute(call).await
    }
    fn list_tools(&self) -> Vec<ToolSchema> {
        SuccessfulReadToolExecutor.list_tools()
    }
}

struct ToolThenAnthropic {
    calls: AtomicUsize,
    provider: AnthropicProvider,
}

#[async_trait]
impl LLMProvider for ToolThenAnthropic {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        match self.calls.fetch_add(1, Ordering::SeqCst) {
            0 => Ok(Box::pin(stream::iter(vec![
                Ok(LLMChunk::ToolCalls(vec![make_tool_call(
                    "mid-loop-call",
                    "Read",
                    "{}",
                )])),
                Ok(LLMChunk::Done),
            ]))),
            1 => {
                assert!(messages
                    .iter()
                    .any(|m| m.tool_call_id.as_deref() == Some("mid-loop-call")));
                self.provider.chat_stream(messages, tools, max, model).await
            }
            call => panic!("exhausted request budget must not replay turn: call {call}"),
        }
    }
}

#[tokio::test]
async fn connector_exhaustion_preserves_context_and_never_replays_successful_tools_or_user_input() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(recovery())
        .mount(&server)
        .await;
    let provider = Arc::new(ToolThenAnthropic {
        calls: AtomicUsize::new(0),
        provider: AnthropicProvider::new("fixture-key").with_base_url(server.uri()),
    });
    let tools = Arc::new(CountingTool(AtomicUsize::new(0)));
    let (_temp, agent, storage) =
        build_direct_execute_agent(provider.clone(), None, Some(tools.clone())).await;
    let mut session = Session::new("connector-exhaustion", "test-model");
    session.add_message(Message::user("one durable user message"));
    storage.save_session(&session).await.unwrap();
    let (tx, mut rx) = mpsc::channel(128);
    let error = agent
        .execute(&mut session, direct_request(tx, CancellationToken::new()))
        .await
        .unwrap_err();
    assert!(matches!(error, AgentError::LLMNonRetryable(_)));
    assert!(error.to_string().contains("temporarily recovering"));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    assert_eq!(tools.0.load(Ordering::SeqCst), 1);
    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        bamboo_llm::retry::global().max_attempts as usize
    );
    assert!(requests.iter().all(|r| r.body == requests[0].body));
    assert!(String::from_utf8_lossy(&requests[0].body).contains("mid-loop tool result"));
    let saved = storage.load_session(&session.id).await.unwrap().unwrap();
    assert_eq!(
        saved
            .messages
            .iter()
            .filter(|m| m.content == "one durable user message")
            .count(),
        1
    );
    assert_eq!(
        saved
            .messages
            .iter()
            .filter(|m| m.tool_call_id.as_deref() == Some("mid-loop-call"))
            .count(),
        1
    );
    assert!(!saved
        .agent_runtime_state
        .as_ref()
        .is_some_and(|s| matches!(s.status, bamboo_domain::AgentStatusState::Completed)));
    let mut errors = 0;
    while let Ok(event) = rx.try_recv() {
        assert!(!matches!(event, AgentEvent::Complete { .. }));
        if matches!(event, AgentEvent::Error { .. }) {
            errors += 1;
        }
    }
    assert_eq!(
        errors, 0,
        "direct execution must not emit intermediate request failures"
    );
    let event =
        crate::runtime::execution::agent_spawn::terminal_error_event_for_result(&Err(error))
            .unwrap();
    match event {
        AgentEvent::Error { message } => {
            assert!(message.contains("temporarily recovering"));
            assert!(!message.contains("Please check your API key"));
        }
        other => panic!("expected the final error event, got {other:?}"),
    }
}

struct RecoverThenStreamFailure(AtomicUsize);
impl Respond for RecoverThenStreamFailure {
    fn respond(&self, _request: &wiremock::Request) -> ResponseTemplate {
        if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            recovery()
        } else {
            ResponseTemplate::new(200).insert_header("content-type", "text/event-stream").set_body_string("event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"temporary stream failure\"}}\n\n")
        }
    }
}

#[tokio::test]
async fn connector_recovery_then_zero_semantic_stream_failure_does_not_reset_http_budget() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(RecoverThenStreamFailure(AtomicUsize::new(0)))
        .mount(&server)
        .await;
    let provider = Arc::new(AnthropicProvider::new("fixture-key").with_base_url(server.uri()));
    let (_temp, agent, storage) = build_direct_execute_agent(provider, None, None).await;
    let mut session = Session::new("connector-zero-semantic", "test-model");
    session.add_message(Message::user("only once"));
    storage.save_session(&session).await.unwrap();
    let (tx, _rx) = mpsc::channel(128);
    let error = agent
        .execute(&mut session, direct_request(tx, CancellationToken::new()))
        .await
        .unwrap_err();
    assert!(matches!(error, AgentError::LLMNonRetryable(_)));
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
    let saved = storage.load_session(&session.id).await.unwrap().unwrap();
    assert_eq!(
        saved
            .messages
            .iter()
            .filter(|m| m.content == "only once")
            .count(),
        1
    );
}

struct PartialTransientProvider {
    calls: AtomicUsize,
    kind: u8,
}
#[async_trait]
impl LLMProvider for PartialTransientProvider {
    async fn chat_stream(
        &self,
        _: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        assert_eq!(
            self.calls.fetch_add(1, Ordering::SeqCst),
            0,
            "semantic output must never be replayed"
        );
        let chunk = match self.kind {
            0 => LLMChunk::Token("partial text".into()),
            1 => LLMChunk::ReasoningToken("partial reasoning".into()),
            _ => LLMChunk::ToolCallsIndexed(vec![(0, make_tool_call("", "", "{\"partial"))]),
        };
        Ok(Box::pin(stream::iter(vec![
            Ok(chunk),
            Err(LLMError::Stream("temporary body failure".into())),
        ])))
    }
}

struct PartialThenStalledProvider(AtomicUsize);

#[async_trait]
impl LLMProvider for PartialThenStalledProvider {
    async fn chat_stream(
        &self,
        _: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        assert_eq!(
            self.0.fetch_add(1, Ordering::SeqCst),
            0,
            "partial output must not replay"
        );
        Ok(Box::pin(
            stream::once(async {
                Ok(LLMChunk::ReasoningToken("private partial reasoning".into()))
            })
            .chain(stream::pending()),
        ))
    }
}

#[tokio::test]
async fn terminal_stream_timeout_receipt_reaches_live_events_and_the_existing_checkpoint() {
    let provider = Arc::new(PartialThenStalledProvider(AtomicUsize::new(0)));
    let mut config = bamboo_llm::Config::default();
    config.stream_timeout.transport_idle_timeout_secs = 1;
    let (_temp, agent, storage) =
        build_direct_execute_agent_with_config(provider.clone(), None, None, config).await;
    let mut session = Session::new(
        &format!("timeout-diagnostic-{}", uuid::Uuid::new_v4()),
        "test-model",
    );
    session.add_message(Message::user("private user prompt"));
    storage.save_session(&session).await.unwrap();
    let (tx, mut rx) = mpsc::channel(128);
    let error = agent
        .execute(&mut session, direct_request(tx, CancellationToken::new()))
        .await
        .unwrap_err();
    assert!(matches!(&error, AgentError::StreamTimeout(timeout) if !timeout.retry_safe()));
    assert_eq!(provider.0.load(Ordering::SeqCst), 1);
    let live =
        bamboo_llm::retry::public_request_diagnostic(&session.id, &error.to_string()).unwrap();
    assert!(
        bamboo_llm::retry::public_request_diagnostic("foreign-session", &error.to_string())
            .is_none()
    );
    let mut saved = storage.load_session(&session.id).await.unwrap().unwrap();
    let captured = bamboo_domain::LlmRequestDiagnostic::from_json(
        saved
            .metadata
            .get(bamboo_domain::LAST_RUN_LLM_DIAGNOSTIC_KEY)
            .unwrap(),
    )
    .unwrap();
    assert_eq!(live, captured);
    assert!(captured.matches_error_message(&error.to_string()));
    let wire = serde_json::to_value(&captured).unwrap();
    assert_eq!(wire["phase"], "transport_idle");
    assert_eq!(wire["deadline_ms"], 1_000);
    assert_eq!(wire["semantic_output_started"], true);
    assert_eq!(wire["retry_safe"], false);
    assert!(!wire.to_string().contains("private user prompt"));
    assert!(!wire.to_string().contains("private partial reasoning"));
    assert!(saved.messages.iter().any(|message| message
        .metadata
        .as_ref()
        .and_then(|value| value.get("interrupted"))
        .and_then(|value| value.as_bool())
        == Some(true)));
    while let Ok(event) = rx.try_recv() {
        assert!(!matches!(
            event,
            AgentEvent::Complete { .. } | AgentEvent::Error { .. }
        ));
    }
    // Existing terminal owners set the canonical error after direct execution;
    // no new checkpoint or status lifecycle is introduced for diagnostics.
    saved.set_last_run_error(error.to_string());
    storage.save_session(&saved).await.unwrap();
    let cold = storage.load_session(&session.id).await.unwrap().unwrap();
    assert_eq!(cold.last_run_diagnostic(), Some(captured));
}

#[tokio::test]
async fn partial_text_reasoning_and_tool_fragments_remain_terminal_and_durable() {
    for kind in 0..3 {
        let provider = Arc::new(PartialTransientProvider {
            calls: AtomicUsize::new(0),
            kind,
        });
        let (_temp, agent, storage) =
            build_direct_execute_agent(provider.clone(), None, None).await;
        let mut session = Session::new(&format!("partial-transient-{kind}"), "test-model");
        session.add_message(Message::user("once"));
        storage.save_session(&session).await.unwrap();
        let (tx, _rx) = mpsc::channel(128);
        let error = agent
            .execute(&mut session, direct_request(tx, CancellationToken::new()))
            .await
            .unwrap_err();
        assert!(matches!(error, AgentError::LLMNonRetryable(_)));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        let saved = storage.load_session(&session.id).await.unwrap().unwrap();
        let interrupted = saved
            .messages
            .iter()
            .find(|m| {
                m.metadata
                    .as_ref()
                    .and_then(|v| v.get("interrupted"))
                    .and_then(|v| v.as_bool())
                    == Some(true)
            })
            .unwrap();
        assert!(interrupted.tool_calls.is_none());
        assert_eq!(
            interrupted.metadata.as_ref().unwrap()["interruption_kind"],
            "llm_error"
        );
    }
}

#[tokio::test]
async fn cancelling_anthropic_backoff_stops_without_another_model_request() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(recovery().insert_header("Retry-After", "5"))
        .mount(&server)
        .await;
    let provider = Arc::new(AnthropicProvider::new("fixture-key").with_base_url(server.uri()));
    let (_temp, agent, storage) = build_direct_execute_agent(provider, None, None).await;
    let mut session = Session::new("cancel-connector-backoff", "test-model");
    session.add_message(Message::user("only once"));
    storage.save_session(&session).await.unwrap();
    let (tx, _rx) = mpsc::channel(128);
    let cancel = CancellationToken::new();
    let trigger = cancel.clone();
    let cancellation = async {
        while server.received_requests().await.unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
        trigger.cancel();
    };
    let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        tokio::join!(
            agent.execute(&mut session, direct_request(tx, cancel)),
            cancellation
        )
    })
    .await
    .unwrap();
    assert!(matches!(result, Err(AgentError::Cancelled)));
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

const RECOVERIES_BEFORE_REJECTION: usize = 3;

struct RecoveryThenRejected(AtomicUsize);
impl Respond for RecoveryThenRejected {
    fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
        if self.0.fetch_add(1, Ordering::SeqCst) < RECOVERIES_BEFORE_REJECTION {
            recovery()
        } else {
            ResponseTemplate::new(422).set_body_string("unprocessable model request")
        }
    }
}

#[tokio::test]
async fn connector_recovery_then_terminal_422_does_not_reset_the_request_budget() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(RecoveryThenRejected(AtomicUsize::new(0)))
        .mount(&server)
        .await;
    let provider = Arc::new(AnthropicProvider::new("fixture-key").with_base_url(server.uri()));
    let (_temp, agent, storage) = build_direct_execute_agent(provider, None, None).await;
    let mut session = Session::new("connector-terminal-422", "test-model");
    session.add_message(Message::user("once"));
    storage.save_session(&session).await.unwrap();
    let (tx, _rx) = mpsc::channel(128);
    let error = agent
        .execute(&mut session, direct_request(tx, CancellationToken::new()))
        .await
        .unwrap_err();
    assert!(matches!(error, AgentError::LLMNonRetryable(_)));
    assert!(error.to_string().contains("422"));
    // A larger ceiling does not authorize retrying the first terminal response.
    assert!(bamboo_llm::retry::global().max_attempts as usize > RECOVERIES_BEFORE_REJECTION + 1);
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), RECOVERIES_BEFORE_REJECTION + 1);
    assert!(requests
        .iter()
        .all(|request| request.body == requests[0].body));
    let cold = storage.load_session(&session.id).await.unwrap().unwrap();
    assert_eq!(
        cold.messages
            .iter()
            .filter(|message| message.role == bamboo_agent_core::Role::User
                && message.content == "once")
            .count(),
        1
    );
}

struct ManagedResponseThenHang {
    provider: AnthropicProvider,
    calls: AtomicUsize,
}

#[async_trait]
impl LLMProvider for ManagedResponseThenHang {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        assert_eq!(self.calls.fetch_add(1, Ordering::SeqCst), 0);
        // The managed send must finish before this deliberately unresponsive
        // provider wrapper becomes the outer watchdog's responsibility.
        let _stream = self
            .provider
            .chat_stream(messages, tools, max, model)
            .await?;
        std::future::pending().await
    }
}

#[tokio::test]
async fn anthropic_bootstrap_watchdog_does_not_restart_a_managed_budget() {
    let server = MockServer::start().await;
    let hits = Arc::new(AtomicUsize::new(0));
    let count = hits.clone();
    let request_times = Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed_times = request_times.clone();
    Mock::given(method("POST"))
        .respond_with(move |_: &wiremock::Request| {
            observed_times
                .lock()
                .unwrap()
                .push(std::time::Instant::now());
            if count.fetch_add(1, Ordering::SeqCst) == 0 {
                recovery().insert_header("Retry-After", "2")
            } else {
                ResponseTemplate::new(200)
                    .insert_header("Content-Type", "text/event-stream")
                    .set_body_string("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
            }
        })
        .expect(2)
        .mount(&server)
        .await;
    let provider = Arc::new(ManagedResponseThenHang {
        provider: AnthropicProvider::new("fixture-key").with_base_url(server.uri()),
        calls: AtomicUsize::new(0),
    });
    let mut config = bamboo_llm::Config::default();
    config.stream_timeout.transport_idle_timeout_secs = 1;
    let (_temp, agent, storage) =
        build_direct_execute_agent_with_config(provider.clone(), None, None, config).await;
    let mut session = Session::new("connector-bootstrap-watchdog", "test-model");
    session.add_message(Message::user("once"));
    storage.save_session(&session).await.unwrap();
    let (tx, _rx) = mpsc::channel(128);
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        agent.execute(&mut session, direct_request(tx, CancellationToken::new())),
    )
    .await
    .expect("the provider hang after managed headers must remain bounded")
    .unwrap_err();
    let receipt =
        bamboo_llm::retry::public_request_diagnostic(&session.id, &error.to_string()).unwrap();
    let wire = serde_json::to_value(&receipt).unwrap();
    assert_eq!(wire["phase"], "bootstrap");
    assert_eq!(wire["last_http_status"], 401);
    assert_eq!(wire["retry_delay_ms"], 2_000);
    assert_eq!(wire["semantic_output_started"], false);
    assert_eq!(wire["retry_safe"], false);
    match error {
        AgentError::StreamTimeout(timeout) => {
            assert!(!timeout.retry_safe());
            assert_eq!(timeout.last_http_retry().unwrap().0, 401);
        }
        other => panic!("expected bounded post-headers provider hang, got {other:?}"),
    }
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(hits.load(Ordering::SeqCst), 2);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(requests[0].body, requests[1].body);
    {
        let times = request_times.lock().unwrap();
        assert_eq!(times.len(), 2);
        assert!(times[1].duration_since(times[0]) >= std::time::Duration::from_secs(2));
    }
    let cold = storage.load_session(&session.id).await.unwrap().unwrap();
    assert_eq!(
        cold.messages
            .iter()
            .filter(|message| message.role == bamboo_agent_core::Role::User
                && message.content == "once")
            .count(),
        1
    );
}

#[tokio::test]
async fn anthropic_zero_semantic_stream_timeout_does_not_restart_a_managed_budget() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        for attempt in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0; 4096];
            loop {
                let read = socket.read(&mut buffer).await.unwrap();
                assert!(read > 0);
                request.extend_from_slice(&buffer[..read]);
                if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&request[..end]).to_ascii_lowercase();
                    let length = headers
                        .lines()
                        .find_map(|l| l.strip_prefix("content-length:"))
                        .unwrap()
                        .trim()
                        .parse::<usize>()
                        .unwrap();
                    if request.len() >= end + 4 + length {
                        break;
                    }
                }
            }
            if attempt == 0 {
                let body = serde_json::json!({"error":{"type":"auth_error","message":RECOVERY}})
                    .to_string();
                socket.write_all(format!("HTTP/1.1 401 Error\r\nContent-Length: {}\r\nRetry-After: 0\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            } else {
                socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 65536\r\n\r\n: ping\n\n").await.unwrap();
                while socket.read(&mut buffer).await.unwrap_or(0) != 0 {}
            }
        }
    });
    let provider = Arc::new(AnthropicProvider::new("fixture-key").with_base_url(url));
    let mut config = bamboo_llm::Config::default();
    config.stream_timeout.transport_idle_timeout_secs = 1;
    let (_temp, agent, storage) =
        build_direct_execute_agent_with_config(provider, None, None, config).await;
    let mut session = Session::new("connector-stream-watchdog", "test-model");
    session.add_message(Message::user("once"));
    storage.save_session(&session).await.unwrap();
    let (tx, _rx) = mpsc::channel(128);
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        agent.execute(&mut session, direct_request(tx, CancellationToken::new())),
    )
    .await
    .unwrap()
    .unwrap_err();
    match error {
        AgentError::StreamTimeout(timeout) => assert!(!timeout.retry_safe()),
        other => panic!("expected managed stream timeout, got {other:?}"),
    }
    tokio::time::timeout(std::time::Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn credential_and_permission_denials_with_overflow_text_remain_terminal() {
    for status in [401, 403] {
        let server = MockServer::start().await;
        Mock::given(method("POST")).respond_with(ResponseTemplate::new(status).set_body_json(serde_json::json!({"error":{"type":"auth_error","message":"invalid API key; input is too long"}}))).mount(&server).await;
        let provider = Arc::new(AnthropicProvider::new("fixture-key").with_base_url(server.uri()));
        let (_temp, agent, storage) = build_direct_execute_agent(provider, None, None).await;
        let mut session = Session::new(&format!("auth-overflow-{status}"), "test-model");
        session.add_message(Message::user("preserve context"));
        storage.save_session(&session).await.unwrap();
        let (tx, _rx) = mpsc::channel(128);
        let error = agent
            .execute(&mut session, direct_request(tx, CancellationToken::new()))
            .await
            .unwrap_err();
        assert!(matches!(error, AgentError::LLMNonRetryable(_)), "{error:?}");
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
        let saved = storage.load_session(&session.id).await.unwrap().unwrap();
        assert!(saved
            .messages
            .iter()
            .any(|m| m.content == "preserve context"));
        assert!(saved.compression_events.is_empty());
    }
}

struct InitialRetryTool(AtomicUsize);

#[async_trait]
impl ToolExecutor for InitialRetryTool {
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
        assert_eq!(call.id, "retry-probe-tool");
        assert_eq!(call.function.name, "Read");
        assert_eq!(call.function.arguments, "{}");
        assert_eq!(self.0.fetch_add(1, Ordering::SeqCst), 0);
        Ok(ToolResult {
            success: true,
            result: "controlled retry tool result".to_string(),
            display_preference: None,
            images: Vec::new(),
        })
    }

    fn list_tools(&self) -> Vec<ToolSchema> {
        SuccessfulReadToolExecutor.list_tools()
    }
}

struct InitialRetryProbe {
    url: String,
    tool_once: bool,
    calls: AtomicUsize,
}

#[async_trait]
impl LLMProvider for InitialRetryProbe {
    async fn chat_stream(
        &self,
        messages: &[Message],
        _tools: &[ToolSchema],
        _max: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.tool_once && call == 0 {
            return Ok(Box::pin(stream::iter(vec![
                Ok(LLMChunk::ToolCalls(vec![make_tool_call(
                    "retry-probe-tool",
                    "Read",
                    "{}",
                )])),
                Ok(LLMChunk::Done),
            ])));
        }
        if self.tool_once {
            assert!(messages
                .iter()
                .any(|m| m.tool_call_id.as_deref() == Some("retry-probe-tool")));
        }
        let config = bamboo_llm::retry::RetryConfig {
            max_attempts: 1,
            base_delay: std::time::Duration::from_millis(1),
            max_delay: std::time::Duration::from_millis(5),
        };
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let response =
            bamboo_llm::retry::send_provider_request(&config, "controlled-consumer", || {
                client.post(&self.url).json(messages)
            })
            .await?;
        if !response.status().is_success() {
            return Err(LLMError::RequestFailed(
                "controlled initial request failed".into(),
            ));
        }
        Ok(Box::pin(stream::iter(vec![
            Ok(LLMChunk::Token("controlled completed".into())),
            Ok(LLMChunk::Done),
        ])))
    }
}

#[tokio::test]
async fn execute_opt_in_retries_initial_http_without_replaying_tools_or_user_message() {
    for continuous in [false, true] {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        let count = hits.clone();
        Mock::given(method("POST"))
            .respond_with(move |_: &wiremock::Request| {
                ResponseTemplate::new(if count.fetch_add(1, Ordering::SeqCst) < 3 {
                    503
                } else {
                    200
                })
            })
            .mount(&server)
            .await;
        let provider = Arc::new(InitialRetryProbe {
            url: server.uri(),
            tool_once: true,
            calls: AtomicUsize::new(0),
        });
        let tools = Arc::new(InitialRetryTool(AtomicUsize::new(0)));
        let (_home, agent, storage) =
            build_direct_execute_agent(provider.clone(), None, Some(tools.clone())).await;
        let mut session = Session::new(&format!("retry-consumer-{continuous}"), "test-model");
        session.add_message(Message::user("one retained user message"));
        storage.save_session(&session).await.unwrap();
        let (tx, _rx) = mpsc::channel(128);
        let request = direct_request(tx, CancellationToken::new());
        let result = agent
            .execute_with_upstream_retry(
                &mut session,
                request,
                if continuous {
                    bamboo_llm::retry::UpstreamRetry::UntilCancelled
                } else {
                    bamboo_llm::retry::UpstreamRetry::Bounded
                },
            )
            .await;
        assert_eq!(result.is_ok(), continuous);
        assert_eq!(hits.load(Ordering::SeqCst), if continuous { 4 } else { 1 });
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
        assert_eq!(tools.0.load(Ordering::SeqCst), 1);
        let requests = server.received_requests().await.unwrap();
        assert!(requests.iter().all(|r| r.body == requests[0].body));
        let cold = storage.load_session(&session.id).await.unwrap().unwrap();
        assert_eq!(
            cold.messages
                .iter()
                .filter(|m| m.content == "one retained user message")
                .count(),
            1
        );
        assert_eq!(
            cold.messages
                .iter()
                .filter(|m| m.tool_call_id.as_deref() == Some("retry-probe-tool"))
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn continuous_initial_retry_never_replays_partial_text_reasoning_or_tool_fragments() {
    for kind in 0..3 {
        let provider = Arc::new(PartialTransientProvider {
            calls: AtomicUsize::new(0),
            kind,
        });
        let (_home, agent, storage) =
            build_direct_execute_agent(provider.clone(), None, None).await;
        let mut session = Session::new(&format!("continuous-partial-{kind}"), "test-model");
        session.add_message(Message::user("retained once"));
        storage.save_session(&session).await.unwrap();
        let (tx, _rx) = mpsc::channel(128);
        let request = crate::runtime::ExecuteRequestBuilder::new("", tx, CancellationToken::new())
            .model("test-model")
            .build();
        let error = agent
            .execute_with_upstream_retry(
                &mut session,
                request,
                bamboo_llm::retry::UpstreamRetry::UntilCancelled,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, AgentError::LLMNonRetryable(_)));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        let cold = storage.load_session(&session.id).await.unwrap().unwrap();
        assert_eq!(
            cold.messages
                .iter()
                .filter(|m| m.content == "retained once")
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn actual_openai_bounded_consumer_stops_when_retry_after_cannot_fit_without_turn_replay() {
    use bamboo_llm::providers::openai::OpenAIProvider;
    for status in [429, 503] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(status)
                    .insert_header("Retry-After", "3600")
                    .set_body_json(
                        serde_json::json!({"error":{"message":"synthetic temporary failure"}}),
                    ),
            )
            .expect(1)
            .mount(&server)
            .await;
        let provider = Arc::new(
            OpenAIProvider::new("synthetic-key")
                .with_base_url(server.uri())
                .with_client(reqwest::Client::builder().no_proxy().build().unwrap()),
        );
        let (_home, agent, storage) = build_direct_execute_agent(provider, None, None).await;
        let mut session = Session::new(&format!("bounded-openai-hint-{status}"), "test-model");
        session.add_message(Message::user("one synthetic user identity"));
        storage.save_session(&session).await.unwrap();
        let (tx, _rx) = mpsc::channel(128);
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            agent.execute(&mut session, direct_request(tx, CancellationToken::new())),
        )
        .await
        .expect("long hint must terminate without resetting a turn")
        .unwrap_err();
        assert!(matches!(error, AgentError::LLMNonRetryable(_)));
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
        let cold = storage.load_session(&session.id).await.unwrap().unwrap();
        assert_eq!(
            cold.messages
                .iter()
                .filter(|m| m.content == "one synthetic user identity")
                .count(),
            1
        );
    }
}

struct LegacyLongHintProvider {
    url: String,
}
#[async_trait]
impl LLMProvider for LegacyLongHintProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        _max: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let config = bamboo_llm::retry::RetryConfig {
            max_attempts: 5,
            base_delay: std::time::Duration::from_millis(1),
            max_delay: std::time::Duration::from_millis(5),
        };
        let response =
            bamboo_llm::retry::send_with_retry(&config, "legacy-owned", || client.post(&self.url))
                .await?;
        Err(LLMError::RequestFailed(format!(
            "HTTP {} synthetic temporary failure",
            response.status()
        )))
    }
}

#[tokio::test]
async fn legacy_public_http_policy_cannot_be_reset_by_an_engine_turn_retry() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "3600"))
        .expect(1)
        .mount(&server)
        .await;
    let provider = Arc::new(LegacyLongHintProvider { url: server.uri() });
    let (_home, agent, storage) = build_direct_execute_agent(provider, None, None).await;
    let mut session = Session::new("legacy-long-hint", "test-model");
    session.add_message(Message::user("one legacy synthetic message"));
    storage.save_session(&session).await.unwrap();
    let (tx, _rx) = mpsc::channel(128);
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        agent.execute(&mut session, direct_request(tx, CancellationToken::new())),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(matches!(error, AgentError::LLMNonRetryable(_)));
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}
