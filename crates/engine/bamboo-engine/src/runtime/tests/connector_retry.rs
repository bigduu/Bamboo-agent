use super::*;
use bamboo_agent_core::AgentError;
use bamboo_llm::providers::anthropic::AnthropicProvider;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

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
                    "already-done",
                    "Read",
                    "{}",
                )])),
                Ok(LLMChunk::Done),
            ]))),
            1 => {
                assert!(messages
                    .iter()
                    .any(|m| m.tool_call_id.as_deref() == Some("already-done")));
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
    assert_eq!(requests.len(), 4);
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
            .filter(|m| m.tool_call_id.as_deref() == Some("already-done"))
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
    assert_eq!(errors, 1, "only the final exhausted error is published");
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

struct RecoveryThenRejected(AtomicUsize);
impl Respond for RecoveryThenRejected {
    fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
        if self.0.fetch_add(1, Ordering::SeqCst) < 3 {
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
    assert_eq!(server.received_requests().await.unwrap().len(), 4);
}

#[tokio::test]
async fn anthropic_bootstrap_watchdog_does_not_restart_a_managed_budget() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(recovery().insert_header("Retry-After", "5"))
        .mount(&server)
        .await;
    let provider = Arc::new(AnthropicProvider::new("fixture-key").with_base_url(server.uri()));
    let mut config = bamboo_llm::Config::default();
    config.stream_timeout.transport_idle_timeout_secs = 1;
    let (_temp, agent, storage) =
        build_direct_execute_agent_with_config(provider, None, None, config).await;
    let mut session = Session::new("connector-bootstrap-watchdog", "test-model");
    session.add_message(Message::user("once"));
    storage.save_session(&session).await.unwrap();
    let (tx, _rx) = mpsc::channel(128);
    let error = agent
        .execute(&mut session, direct_request(tx, CancellationToken::new()))
        .await
        .unwrap_err();
    match error {
        AgentError::StreamTimeout(timeout) => {
            assert!(!timeout.retry_safe());
            assert_eq!(timeout.last_http_retry().unwrap().0, 401);
        }
        other => panic!("expected managed bootstrap timeout, got {other:?}"),
    }
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
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
