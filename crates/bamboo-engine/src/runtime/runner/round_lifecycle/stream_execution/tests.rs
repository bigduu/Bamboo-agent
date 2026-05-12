use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::stream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::execute_llm_stream;
use bamboo_agent_core::{AgentEvent, Message, Role, Session};
use bamboo_compression::{PreparedContext, TokenUsageBreakdown};
use bamboo_infrastructure::{LLMChunk, LLMProvider, LLMRequestOptions, LLMStream};

struct MockLlmProvider {
    chunks: Vec<LLMChunk>,
    requested_messages: Mutex<Vec<Message>>,
    requested_session_id: Mutex<Option<String>>,
    requested_previous_response_id: Mutex<Option<String>>,
    requested_reasoning_summary: Mutex<Option<String>>,
    requested_store: Mutex<Option<bool>>,
    requested_include: Mutex<Option<Vec<String>>>,
    requested_text_verbosity: Mutex<Option<String>>,
}

#[async_trait]
impl LLMProvider for MockLlmProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[bamboo_agent_core::tools::ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> bamboo_infrastructure::provider::Result<LLMStream> {
        panic!("chat_stream should not be called directly in this test");
    }

    async fn chat_stream_with_options(
        &self,
        messages: &[Message],
        _tools: &[bamboo_agent_core::tools::ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> bamboo_infrastructure::provider::Result<LLMStream> {
        *self.requested_messages.lock().expect("messages lock") = messages.to_vec();
        *self.requested_session_id.lock().expect("session_id lock") =
            options.and_then(|value| value.session_id.clone());
        *self
            .requested_previous_response_id
            .lock()
            .expect("previous_response_id lock") = options
            .and_then(|value| value.responses.as_ref())
            .and_then(|value| value.previous_response_id.clone());
        *self
            .requested_reasoning_summary
            .lock()
            .expect("reasoning_summary lock") = options
            .and_then(|value| value.responses.as_ref())
            .and_then(|value| value.reasoning_summary.clone());
        *self.requested_store.lock().expect("store lock") = options
            .and_then(|value| value.responses.as_ref())
            .and_then(|value| value.store);
        *self.requested_include.lock().expect("include lock") = options
            .and_then(|value| value.responses.as_ref())
            .and_then(|value| value.include.clone());
        *self
            .requested_text_verbosity
            .lock()
            .expect("text_verbosity lock") = options
            .and_then(|value| value.responses.as_ref())
            .and_then(|value| value.text_verbosity.clone());

        let items = self
            .chunks
            .clone()
            .into_iter()
            .map(Ok::<LLMChunk, bamboo_infrastructure::provider::LLMError>);
        Ok(Box::pin(stream::iter(items)))
    }
}

#[tokio::test]
async fn execute_llm_stream_sets_session_usage_and_emits_budget_event() {
    let mut session = Session::new("session-stream-1", "test-model");
    let (event_tx, mut event_rx) = mpsc::channel::<AgentEvent>(16);

    let prepared_context = PreparedContext {
        messages: vec![Message::system("system")],
        token_usage: TokenUsageBreakdown {
            system_tokens: 10,
            summary_tokens: 0,
            window_tokens: 12,
            total_tokens: 22,
            budget_limit: 100,
        },
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };

    let llm = Arc::new(MockLlmProvider {
        chunks: vec![LLMChunk::Token("hi".to_string()), LLMChunk::Done],
        requested_messages: Mutex::new(Vec::new()),
        requested_session_id: Mutex::new(None),
        requested_previous_response_id: Mutex::new(None),
        requested_reasoning_summary: Mutex::new(None),
        requested_store: Mutex::new(None),
        requested_include: Mutex::new(None),
        requested_text_verbosity: Mutex::new(None),
    });
    let llm_dyn: Arc<dyn LLMProvider> = llm.clone();

    let (stream_output, _duration) = execute_llm_stream(
        &mut session,
        &llm_dyn,
        &event_tx,
        &CancellationToken::new(),
        &prepared_context,
        400_000,
        &[],
        128,
        "test-model",
        None,
        None,
        "session-stream-1",
    )
    .await
    .expect("execute llm stream");

    assert!(stream_output.response_id.is_none());
    assert_eq!(stream_output.content, "hi");
    assert!(stream_output.reasoning_content.is_empty());
    assert!(session.token_usage.is_some());
    assert_eq!(
        session
            .token_usage
            .as_ref()
            .map(|usage| usage.max_context_tokens),
        Some(400_000)
    );

    let first = event_rx.recv().await.expect("budget event expected");
    assert!(matches!(first, AgentEvent::TokenBudgetUpdated { .. }));

    let second = event_rx.recv().await.expect("token event expected");
    assert!(matches!(second, AgentEvent::Token { .. }));
    assert_eq!(
        llm.requested_text_verbosity
            .lock()
            .expect("text_verbosity lock")
            .as_deref(),
        Some("high")
    );
    assert_eq!(
        llm.requested_include.lock().expect("include lock").clone(),
        Some(vec!["reasoning.encrypted_content".to_string()])
    );
    assert_eq!(
        llm.requested_reasoning_summary
            .lock()
            .expect("reasoning_summary lock")
            .as_deref(),
        Some("auto")
    );
    assert_eq!(
        llm.requested_session_id
            .lock()
            .expect("session_id lock")
            .as_deref(),
        Some("session-stream-1")
    );
}

#[tokio::test]
async fn execute_llm_stream_continues_responses_turn_with_delta_messages() {
    let mut session = Session::new("session-stream-2", "test-model");
    session.metadata.insert(
        "responses.previous_response_id".to_string(),
        "resp_prev".to_string(),
    );

    let (event_tx, _event_rx) = mpsc::channel::<AgentEvent>(16);
    let prepared_context = PreparedContext {
        messages: vec![
            Message::system("system"),
            Message::user("run a tool"),
            Message::assistant("calling tool", None),
            Message::tool_result("call_1", "{\"ok\":true}"),
        ],
        token_usage: TokenUsageBreakdown {
            system_tokens: 10,
            summary_tokens: 0,
            window_tokens: 12,
            total_tokens: 22,
            budget_limit: 100,
        },
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };

    let llm = Arc::new(MockLlmProvider {
        chunks: vec![
            LLMChunk::ResponseId("resp_next".to_string()),
            LLMChunk::Token("done".to_string()),
            LLMChunk::Done,
        ],
        requested_messages: Mutex::new(Vec::new()),
        requested_session_id: Mutex::new(None),
        requested_previous_response_id: Mutex::new(None),
        requested_reasoning_summary: Mutex::new(None),
        requested_store: Mutex::new(None),
        requested_include: Mutex::new(None),
        requested_text_verbosity: Mutex::new(None),
    });
    let llm_dyn: Arc<dyn LLMProvider> = llm.clone();

    let (stream_output, _duration) = execute_llm_stream(
        &mut session,
        &llm_dyn,
        &event_tx,
        &CancellationToken::new(),
        &prepared_context,
        400_000,
        &[],
        128,
        "test-model",
        Some("openai"),
        None,
        "session-stream-2",
    )
    .await
    .expect("execute llm stream");

    let requested_messages = llm
        .requested_messages
        .lock()
        .expect("messages lock")
        .clone();
    assert_eq!(requested_messages.len(), 1);
    assert!(matches!(requested_messages[0].role, Role::Tool));
    assert_eq!(
        llm.requested_previous_response_id
            .lock()
            .expect("previous_response_id lock")
            .as_deref(),
        Some("resp_prev")
    );
    assert_eq!(
        *llm.requested_store.lock().expect("store lock"),
        Some(false)
    );
    assert_eq!(
        llm.requested_text_verbosity
            .lock()
            .expect("text_verbosity lock")
            .as_deref(),
        Some("high")
    );
    assert_eq!(
        llm.requested_include.lock().expect("include lock").clone(),
        Some(vec!["reasoning.encrypted_content".to_string()])
    );
    assert_eq!(
        llm.requested_reasoning_summary
            .lock()
            .expect("reasoning_summary lock")
            .as_deref(),
        Some("auto")
    );
    assert_eq!(
        llm.requested_session_id
            .lock()
            .expect("session_id lock")
            .as_deref(),
        Some("session-stream-2")
    );
    assert_eq!(stream_output.response_id.as_deref(), Some("resp_next"));
    assert_eq!(
        session
            .metadata
            .get("responses.previous_response_id")
            .map(String::as_str),
        Some("resp_next")
    );
}

#[tokio::test]
async fn execute_llm_stream_keeps_previous_response_id_when_local_summary_or_compression_is_active()
{
    let mut session = Session::new("session-stream-2b", "test-model");
    session.metadata.insert(
        "responses.previous_response_id".to_string(),
        "resp_prev".to_string(),
    );
    session.conversation_summary = Some(bamboo_agent_core::agent::types::ConversationSummary::new(
        "Older work has been summarized locally.",
        6,
        42,
    ));

    let (event_tx, _event_rx) = mpsc::channel::<AgentEvent>(16);
    let prepared_context = PreparedContext {
        messages: vec![
            Message::system("system"),
            Message::user("previous work was compressed"),
            Message::assistant("here is the local summary context", None),
            Message::user("continue from the compressed state"),
            Message::tool_result("call_1", "{\"ok\":true}"),
        ],
        token_usage: TokenUsageBreakdown {
            system_tokens: 10,
            summary_tokens: 18,
            window_tokens: 12,
            total_tokens: 40,
            budget_limit: 100,
        },
        truncation_occurred: false,
        segments_removed: 1,
        compressed_message_ids: vec!["msg_old_1".to_string(), "msg_old_2".to_string()],
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };

    let llm = Arc::new(MockLlmProvider {
        chunks: vec![
            LLMChunk::ResponseId("resp_next".to_string()),
            LLMChunk::Token("done".to_string()),
            LLMChunk::Done,
        ],
        requested_messages: Mutex::new(Vec::new()),
        requested_session_id: Mutex::new(None),
        requested_previous_response_id: Mutex::new(None),
        requested_reasoning_summary: Mutex::new(None),
        requested_store: Mutex::new(None),
        requested_include: Mutex::new(None),
        requested_text_verbosity: Mutex::new(None),
    });
    let llm_dyn: Arc<dyn LLMProvider> = llm.clone();

    let (_stream_output, _duration) = execute_llm_stream(
        &mut session,
        &llm_dyn,
        &event_tx,
        &CancellationToken::new(),
        &prepared_context,
        400_000,
        &[],
        128,
        "test-model",
        Some("openai"),
        None,
        "session-stream-2b",
    )
    .await
    .expect("execute llm stream");

    let requested_messages = llm
        .requested_messages
        .lock()
        .expect("messages lock")
        .clone();
    assert_eq!(requested_messages.len(), 2);
    assert!(matches!(requested_messages[0].role, Role::User));
    assert!(matches!(requested_messages[1].role, Role::Tool));
    assert_eq!(
        llm.requested_previous_response_id
            .lock()
            .expect("previous_response_id lock")
            .as_deref(),
        Some("resp_prev")
    );
    assert_eq!(
        llm.requested_session_id
            .lock()
            .expect("session_id lock")
            .as_deref(),
        Some("session-stream-2b")
    );
}

#[tokio::test]
async fn execute_llm_stream_disables_previous_response_id_for_copilot() {
    let mut session = Session::new("session-stream-3", "test-model");
    session.metadata.insert(
        "responses.previous_response_id".to_string(),
        "resp_prev".to_string(),
    );

    let (event_tx, _event_rx) = mpsc::channel::<AgentEvent>(16);
    let prepared_context = PreparedContext {
        messages: vec![
            Message::system("system"),
            Message::user("run a tool"),
            Message::assistant("calling tool", None),
            Message::tool_result("call_1", "{\"ok\":true}"),
        ],
        token_usage: TokenUsageBreakdown {
            system_tokens: 10,
            summary_tokens: 0,
            window_tokens: 12,
            total_tokens: 22,
            budget_limit: 100,
        },
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };

    let llm = Arc::new(MockLlmProvider {
        chunks: vec![
            LLMChunk::ResponseId("resp_next".to_string()),
            LLMChunk::Token("done".to_string()),
            LLMChunk::Done,
        ],
        requested_messages: Mutex::new(Vec::new()),
        requested_session_id: Mutex::new(None),
        requested_previous_response_id: Mutex::new(None),
        requested_reasoning_summary: Mutex::new(None),
        requested_store: Mutex::new(None),
        requested_include: Mutex::new(None),
        requested_text_verbosity: Mutex::new(None),
    });
    let llm_dyn: Arc<dyn LLMProvider> = llm.clone();

    let (_stream_output, _duration) = execute_llm_stream(
        &mut session,
        &llm_dyn,
        &event_tx,
        &CancellationToken::new(),
        &prepared_context,
        400_000,
        &[],
        128,
        "test-model",
        Some("copilot"),
        None,
        "session-stream-3",
    )
    .await
    .expect("execute llm stream");

    let requested_messages = llm
        .requested_messages
        .lock()
        .expect("messages lock")
        .clone();
    assert_eq!(requested_messages.len(), prepared_context.messages.len());
    assert_eq!(
        llm.requested_previous_response_id
            .lock()
            .expect("previous_response_id lock")
            .as_deref(),
        None
    );
    assert_eq!(
        *llm.requested_store.lock().expect("store lock"),
        Some(false)
    );
    assert_eq!(
        llm.requested_text_verbosity
            .lock()
            .expect("text_verbosity lock")
            .as_deref(),
        Some("high")
    );
    assert_eq!(
        llm.requested_include.lock().expect("include lock").clone(),
        Some(vec!["reasoning.encrypted_content".to_string()])
    );
    assert_eq!(
        llm.requested_reasoning_summary
            .lock()
            .expect("reasoning_summary lock")
            .as_deref(),
        Some("auto")
    );
    assert_eq!(
        llm.requested_session_id
            .lock()
            .expect("session_id lock")
            .as_deref(),
        Some("session-stream-3")
    );
    assert!(!session
        .metadata
        .contains_key("responses.previous_response_id"));
}

#[test]
fn overflow_error_detection_matches_common_provider_messages() {
    assert!(super::is_llm_overflow_error("prompt too long"));
    assert!(super::is_llm_overflow_error(
        "API error: maximum context length exceeded"
    ));
    assert!(super::is_llm_overflow_error(
        "Request too large for model context window"
    ));
    assert!(!super::is_llm_overflow_error("timeout while connecting"));
    assert!(!super::is_llm_overflow_error(
        "authentication error: invalid api key"
    ));
}
