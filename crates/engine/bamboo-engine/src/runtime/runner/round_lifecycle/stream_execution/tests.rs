// Test env-lock is a std Mutex intentionally held across .await to serialize env access.
#![allow(clippy::await_holding_lock)]

use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use futures::stream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::execute_llm_stream;
use super::LlmStreamFrame;
use bamboo_agent_core::agent::types::{ConversationSummary, TaskItem, TaskItemStatus, TaskList};
use bamboo_agent_core::{AgentEvent, Message, Role, Session};
use bamboo_compression::{PreparedContext, TokenUsageBreakdown};
use bamboo_llm::{Config, LLMChunk, LLMProvider, LLMRequestOptions, LLMStream};
use chrono::Utc;

fn isolate_prompt_safe_env_cache() -> MutexGuard<'static, ()> {
    let guard = crate::runtime::tests::env_cache_lock_acquire();
    let empty_data_dir = tempfile::tempdir().expect("temp dir for empty config");
    let _ = Config::from_data_dir(Some(empty_data_dir.path().to_path_buf()));
    guard
}

struct MockLlmProvider {
    chunks: Vec<LLMChunk>,
    requested_messages: Mutex<Vec<Message>>,
    requested_session_id: Mutex<Option<String>>,
    requested_previous_response_id: Mutex<Option<String>>,
    requested_reasoning_summary: Mutex<Option<String>>,
    requested_store: Mutex<Option<bool>>,
    requested_include: Mutex<Option<Vec<String>>>,
    requested_text_verbosity: Mutex<Option<String>>,
    requested_instructions: Mutex<Option<String>>,
    /// Set when the engine routed this request through the canonical
    /// `chat_stream_lanes` entry point (the normal, non-continuation path).
    lanes_invoked: Mutex<bool>,
}

#[async_trait]
impl LLMProvider for MockLlmProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[bamboo_agent_core::tools::ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        panic!("chat_stream should not be called directly in this test");
    }

    async fn chat_stream_lanes(
        &self,
        lanes: &bamboo_llm::PromptLanes,
        tools: &[bamboo_agent_core::tools::ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        *self.lanes_invoked.lock().expect("lanes_invoked lock") = true;
        // Mirror the default behavior so `requested_messages` is still captured.
        self.chat_stream_with_options(&lanes.flatten(), tools, max_output_tokens, model, options)
            .await
    }

    async fn chat_stream_with_options(
        &self,
        messages: &[Message],
        _tools: &[bamboo_agent_core::tools::ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> bamboo_llm::provider::Result<LLMStream> {
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
        *self
            .requested_instructions
            .lock()
            .expect("instructions lock") = options
            .and_then(|value| value.responses.as_ref())
            .and_then(|value| value.instructions.clone());

        let items = self
            .chunks
            .clone()
            .into_iter()
            .map(Ok::<LLMChunk, bamboo_llm::provider::LLMError>);
        Ok(Box::pin(stream::iter(items)))
    }
}

fn mock_llm(chunks: Vec<LLMChunk>) -> Arc<MockLlmProvider> {
    Arc::new(MockLlmProvider {
        chunks,
        requested_messages: Mutex::new(Vec::new()),
        requested_session_id: Mutex::new(None),
        requested_previous_response_id: Mutex::new(None),
        requested_reasoning_summary: Mutex::new(None),
        requested_store: Mutex::new(None),
        requested_include: Mutex::new(None),
        requested_text_verbosity: Mutex::new(None),
        requested_instructions: Mutex::new(None),
        lanes_invoked: Mutex::new(false),
    })
}

fn test_config(system_prompt: &str) -> crate::runtime::config::AgentLoopConfig {
    crate::runtime::config::AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        system_prompt: Some(system_prompt.to_string()),
        ..Default::default()
    }
}

fn usage(summary_tokens: u32, total_tokens: u32) -> TokenUsageBreakdown {
    TokenUsageBreakdown {
        system_tokens: 10,
        summary_tokens,
        window_tokens: total_tokens
            .saturating_sub(10)
            .saturating_sub(summary_tokens),
        total_tokens,
        budget_limit: 100,
    }
}

#[tokio::test]
async fn execute_llm_stream_sets_session_usage_and_emits_budget_event() {
    let _env_lock = isolate_prompt_safe_env_cache();
    let mut session = Session::new("session-stream-1", "test-model");
    let (event_tx, mut event_rx) = mpsc::channel::<AgentEvent>(16);
    let config = test_config("system");

    let prepared_context = PreparedContext {
        messages: vec![Message::system("system")],
        token_usage: usage(0, 22),
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };

    let llm = mock_llm(vec![LLMChunk::Token("hi".to_string()), LLMChunk::Done]);
    let llm_dyn: Arc<dyn LLMProvider> = llm.clone();

    let (stream_output, _duration) = execute_llm_stream(
        &mut session,
        &config,
        &llm_dyn,
        &prepared_context,
        &[],
        &LlmStreamFrame {
            event_tx: &event_tx,
            cancel_token: &CancellationToken::new(),
            session_id: "session-stream-1",
            model: "test-model",
            provider_name: None,
            provider_type: None,
            reasoning_effort: None,
            max_context_tokens: 400_000,
            max_output_tokens: 128,
        },
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

    let requested_messages = llm
        .requested_messages
        .lock()
        .expect("messages lock")
        .clone();
    assert_eq!(requested_messages.len(), 1);
    assert!(matches!(requested_messages[0].role, Role::System));
    assert_eq!(requested_messages[0].content, "system");
    assert_eq!(
        llm.requested_instructions
            .lock()
            .expect("instructions lock")
            .as_deref(),
        Some("system")
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
async fn execute_llm_stream_emits_final_budget_event_with_stream_usage() {
    let _env_lock = isolate_prompt_safe_env_cache();
    let mut session = Session::new("session-stream-final-budget", "test-model");
    let (event_tx, mut event_rx) = mpsc::channel::<AgentEvent>(16);
    let config = test_config("system");

    let prepared_context = PreparedContext {
        messages: vec![Message::system("system")],
        token_usage: usage(0, 22),
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };

    let llm = mock_llm(vec![
        LLMChunk::CacheUsage {
            cache_creation_input_tokens: 21,
            cache_read_input_tokens: 34,
        },
        LLMChunk::UsageSummary {
            output_tokens: 56,
            thinking_tokens: 78,
        },
        LLMChunk::Done,
    ]);
    let llm_dyn: Arc<dyn LLMProvider> = llm.clone();

    let (stream_output, _duration) = execute_llm_stream(
        &mut session,
        &config,
        &llm_dyn,
        &prepared_context,
        &[],
        &LlmStreamFrame {
            event_tx: &event_tx,
            cancel_token: &CancellationToken::new(),
            session_id: "session-stream-final-budget",
            model: "test-model",
            provider_name: None,
            provider_type: None,
            reasoning_effort: None,
            max_context_tokens: 400_000,
            max_output_tokens: 128,
        },
    )
    .await
    .expect("execute llm stream");

    match event_rx
        .recv()
        .await
        .expect("initial budget event expected")
    {
        AgentEvent::TokenBudgetUpdated { usage } => {
            assert_eq!(usage.thinking_tokens, 0);
            assert_eq!(usage.cache_read_input_tokens, 0);
        }
        other => panic!("unexpected first event: {other:?}"),
    }

    match event_rx.recv().await.expect("final budget event expected") {
        AgentEvent::TokenBudgetUpdated { usage } => {
            assert_eq!(usage.thinking_tokens, 78);
            assert_eq!(usage.cache_read_input_tokens, 34);
        }
        other => panic!("unexpected second event: {other:?}"),
    }

    assert_eq!(stream_output.output_tokens, 56);
    assert_eq!(stream_output.thinking_tokens, 78);
    assert_eq!(stream_output.cache_read_input_tokens, 34);
    assert_eq!(
        session
            .token_usage
            .as_ref()
            .map(|usage| (usage.thinking_tokens, usage.cache_read_input_tokens)),
        Some((78, 34))
    );
}

#[tokio::test]
async fn execute_llm_stream_includes_task_block_in_full_request() {
    let _env_lock = isolate_prompt_safe_env_cache();
    let mut session = Session::new("session-stream-task", "test-model");
    session.task_list = Some(TaskList {
        session_id: session.id.clone(),
        title: "Agent Tasks".to_string(),
        items: vec![TaskItem {
            id: "task-1".to_string(),
            description: "Implement task block wiring".to_string(),
            status: TaskItemStatus::InProgress,
            ..TaskItem::default()
        }],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    });

    let (event_tx, _event_rx) = mpsc::channel::<AgentEvent>(16);
    let config = test_config("system");
    let prepared_context = PreparedContext {
        messages: vec![Message::system("system"), Message::user("continue")],
        token_usage: usage(0, 24),
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };

    let llm = mock_llm(vec![LLMChunk::Token("ok".to_string()), LLMChunk::Done]);
    let llm_dyn: Arc<dyn LLMProvider> = llm.clone();

    let (_stream_output, _duration) = execute_llm_stream(
        &mut session,
        &config,
        &llm_dyn,
        &prepared_context,
        &[],
        &LlmStreamFrame {
            event_tx: &event_tx,
            cancel_token: &CancellationToken::new(),
            session_id: "session-stream-task",
            model: "test-model",
            provider_name: Some("openai"),
            provider_type: Some("openai"),
            reasoning_effort: None,
            max_context_tokens: 400_000,
            max_output_tokens: 128,
        },
    )
    .await
    .expect("execute llm stream");

    let requested_messages = llm
        .requested_messages
        .lock()
        .expect("messages lock")
        .clone();
    assert_eq!(requested_messages.len(), 3);
    assert!(matches!(requested_messages[0].role, Role::System));
    // Conversation history precedes the volatile context, which is appended at
    // the tail so it stays out of the cacheable prefix.
    assert!(matches!(requested_messages[1].role, Role::User));
    assert_eq!(requested_messages[1].content, "continue");
    assert!(matches!(requested_messages[2].role, Role::User));
    assert!(requested_messages[2]
        .content
        .contains("context_type: task_snapshot"));
    assert!(requested_messages[2]
        .content
        .contains("Implement task block wiring"));
    assert_eq!(
        llm.requested_instructions
            .lock()
            .expect("instructions lock")
            .as_deref(),
        Some("system")
    );
}

#[test]
fn build_request_envelope_tails_volatile_context_and_sets_cache_breakpoints() {
    let _env_lock = isolate_prompt_safe_env_cache();
    let mut session = Session::new("session-cache-plan", "test-model");
    session.task_list = Some(TaskList {
        session_id: session.id.clone(),
        title: "Agent Tasks".to_string(),
        items: vec![TaskItem {
            id: "task-1".to_string(),
            description: "Cacheable task".to_string(),
            status: TaskItemStatus::InProgress,
            ..TaskItem::default()
        }],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    });
    let config = test_config("system");
    let last_user = Message::user("continue");
    let last_user_id = last_user.id.clone();
    let prepared_context = PreparedContext {
        messages: vec![Message::system("system"), last_user],
        token_usage: usage(0, 24),
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };

    let envelope = super::build_request_envelope(&session, &prepared_context, &config, &[]);

    // Volatile task context is rendered and placed at the very tail, out of the
    // cacheable prefix.
    assert_eq!(envelope.volatile_context_messages.len(), 1);
    let last = envelope
        .chat_messages
        .last()
        .expect("chat messages present");
    assert!(last.content.contains("context_type: task_snapshot"));

    // Plan caches system + tools and puts a rolling breakpoint on the last
    // conversation message (just before the volatile tail).
    assert!(envelope.cache_plan.cache_system);
    assert!(envelope.cache_plan.cache_tools);
    assert!(envelope.cache_plan.is_breakpoint(&last_user_id));
}

#[test]
fn stable_prefix_is_byte_stable_across_rounds() {
    // The whole point of relocating the tool guide out of the system prompt is a
    // PREFIX that actually caches: across rounds where only the conversation
    // grows, the cacheable prefix (static system + the relocated guide message
    // content) must be byte-identical and its drift hash unchanged, so the
    // provider cache hits instead of re-reading the prefix each turn.
    let _env_lock = isolate_prompt_safe_env_cache();
    let session = Session::new("session-prefix-stable", "test-model");
    let mut config = test_config("BASE_IDENTITY");
    config.mcp_tool_guidance = Some("NOVA_GUIDANCE_MARKER stable guidance".to_string());

    let ctx = |msgs: Vec<Message>| PreparedContext {
        messages: msgs,
        token_usage: usage(0, 24),
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };

    // Round 1: a short conversation.
    let round1 = ctx(vec![Message::system("BASE_IDENTITY"), Message::user("first")]);
    // Round 2: the same session, conversation has grown.
    let round2 = ctx(vec![
        Message::system("BASE_IDENTITY"),
        Message::user("first"),
        Message::assistant("answer", None),
        Message::user("second"),
    ]);

    let e1 = super::build_request_envelope(&session, &round1, &config, &[]);
    let e2 = super::build_request_envelope(&session, &round2, &config, &[]);

    // The static system identity is unchanged across rounds.
    assert_eq!(
        e1.lanes.stable_instructions, e2.lanes.stable_instructions,
        "system prompt must stay byte-stable across rounds"
    );

    // The relocated guide message content is unchanged (its id may differ — only
    // content is what the provider caches).
    let guide = |e: &super::PreparedRequestEnvelope| -> String {
        e.lanes
            .stable_prefix_messages
            .iter()
            .find(|m| m.content.contains("NOVA_GUIDANCE_MARKER"))
            .map(|m| m.content.clone())
            .expect("relocated guide present")
    };
    assert_eq!(guide(&e1), guide(&e2), "guide prefix must stay byte-stable");

    // The cacheable-prefix drift hash is unchanged, so the cache hits.
    assert!(e1.envelope_observability.stable_prefix_hash.is_some());
    assert_eq!(
        e1.envelope_observability.stable_prefix_hash,
        e2.envelope_observability.stable_prefix_hash,
        "stable prefix must not drift between rounds"
    );
}

#[tokio::test]
async fn execute_llm_stream_routes_normal_request_through_lanes_with_relocated_guide() {
    let _env_lock = isolate_prompt_safe_env_cache();
    let mut session = Session::new("session-stream-lanes", "test-model");
    let (event_tx, _event_rx) = mpsc::channel::<AgentEvent>(16);
    let mut config = test_config("BASE_IDENTITY");
    config.mcp_tool_guidance = Some("NOVA_GUIDANCE_MARKER targeting workflow".to_string());

    let prepared_context = PreparedContext {
        messages: vec![Message::system("BASE_IDENTITY"), Message::user("go")],
        token_usage: usage(0, 24),
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };

    let llm = mock_llm(vec![LLMChunk::Token("ok".to_string()), LLMChunk::Done]);
    let llm_dyn: Arc<dyn LLMProvider> = llm.clone();
    execute_llm_stream(
        &mut session,
        &config,
        &llm_dyn,
        &prepared_context,
        &[],
        &LlmStreamFrame {
            event_tx: &event_tx,
            cancel_token: &CancellationToken::new(),
            session_id: "session-stream-lanes",
            model: "test-model",
            provider_name: None,
            provider_type: None,
            reasoning_effort: None,
            max_context_tokens: 400_000,
            max_output_tokens: 128,
        },
    )
    .await
    .expect("execute llm stream");

    // Normal (non-continuation) requests go through the canonical lanes entry.
    assert!(
        *llm.lanes_invoked.lock().expect("lanes_invoked lock"),
        "normal request must route through chat_stream_lanes"
    );

    // What the provider actually received: the leading system message keeps the
    // static identity but NOT the tool/server guide, which arrives as its own
    // message instead.
    let messages = llm.requested_messages.lock().expect("messages lock").clone();
    assert!(matches!(messages[0].role, Role::System));
    assert!(messages[0].content.contains("BASE_IDENTITY"));
    assert!(
        !messages[0].content.contains("NOVA_GUIDANCE_MARKER"),
        "guide must not be in the system message"
    );
    assert!(
        messages
            .iter()
            .any(|m| matches!(m.role, Role::User) && m.content.contains("NOVA_GUIDANCE_MARKER")),
        "guide arrives as a dedicated message"
    );
}

#[test]
fn build_request_envelope_relocates_tool_guide_into_stable_prefix_lane() {
    let _env_lock = isolate_prompt_safe_env_cache();
    let session = Session::new("session-toolguide", "test-model");
    let mut config = test_config("BASE_SYSTEM_IDENTITY");
    // A connected MCP server's `initialize` guidance (e.g. nova's targeting
    // workflow) flows into the tool guide; mark it so we can locate it.
    config.mcp_tool_guidance = Some("NOVA_GUIDANCE_MARKER targeting workflow".to_string());
    let prepared_context = PreparedContext {
        messages: vec![
            Message::system("BASE_SYSTEM_IDENTITY"),
            Message::user("go"),
        ],
        token_usage: usage(0, 24),
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };

    let envelope = super::build_request_envelope(&session, &prepared_context, &config, &[]);

    // The lane's system prompt keeps the static identity but NO LONGER carries the
    // tool/connected-server guide — that is the "system stays static" property.
    assert!(
        envelope
            .lanes
            .stable_instructions
            .contains("BASE_SYSTEM_IDENTITY"),
        "lane system keeps the static base identity"
    );
    assert!(
        !envelope
            .lanes
            .stable_instructions
            .contains("NOVA_GUIDANCE_MARKER"),
        "tool/server guide must be removed from the lane system prompt"
    );

    // It rides as a fixed stable-prefix MESSAGE (a typed, never-compressed context
    // block at a known position) instead.
    let guide = envelope
        .lanes
        .stable_prefix_messages
        .iter()
        .find(|m| m.content.contains("NOVA_GUIDANCE_MARKER"))
        .expect("tool guide relocated into a stable-prefix message");
    assert_eq!(guide.role, bamboo_agent_core::Role::User);
    assert!(guide.never_compress);

    // And that relocated, session-stable message earns its own cache breakpoint.
    assert!(envelope.cache_plan.is_breakpoint(&guide.id));

    // The Responses-API view mirrors the relocation: the guide leaves
    // `instructions` and rides at the FRONT of the input messages — so every
    // provider family gets the same static-system structure.
    assert!(
        !envelope
            .instructions
            .as_deref()
            .unwrap_or_default()
            .contains("NOVA_GUIDANCE_MARKER"),
        "tool/server guide must be removed from Responses instructions"
    );
    assert!(
        envelope
            .responses_input_messages
            .first()
            .is_some_and(|m| m.content.contains("NOVA_GUIDANCE_MARKER")),
        "tool/server guide leads the Responses input messages"
    );
}

#[tokio::test]
async fn execute_llm_stream_continues_responses_turn_with_delta_messages() {
    let _env_lock = isolate_prompt_safe_env_cache();
    let mut session = Session::new("session-stream-2", "test-model");
    session.metadata.insert(
        "responses.previous_response_id".to_string(),
        "resp_prev".to_string(),
    );

    let (event_tx, _event_rx) = mpsc::channel::<AgentEvent>(16);
    let config = test_config("system");
    let prepared_context = PreparedContext {
        messages: vec![
            Message::system("system"),
            Message::user("run a tool"),
            Message::assistant("calling tool", None),
            Message::tool_result("call_1", "{\"ok\":true}"),
        ],
        token_usage: usage(0, 22),
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };

    let llm = mock_llm(vec![
        LLMChunk::ResponseId("resp_next".to_string()),
        LLMChunk::Token("done".to_string()),
        LLMChunk::Done,
    ]);
    let llm_dyn: Arc<dyn LLMProvider> = llm.clone();

    let (stream_output, _duration) = execute_llm_stream(
        &mut session,
        &config,
        &llm_dyn,
        &prepared_context,
        &[],
        &LlmStreamFrame {
            event_tx: &event_tx,
            cancel_token: &CancellationToken::new(),
            session_id: "session-stream-2",
            model: "test-model",
            provider_name: Some("openai"),
            provider_type: Some("openai"),
            reasoning_effort: None,
            max_context_tokens: 400_000,
            max_output_tokens: 128,
        },
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
    assert!(requested_messages
        .iter()
        .all(|message| !matches!(message.role, Role::System)));
    assert_eq!(
        llm.requested_previous_response_id
            .lock()
            .expect("previous_response_id lock")
            .as_deref(),
        Some("resp_prev")
    );
    assert_eq!(
        llm.requested_instructions
            .lock()
            .expect("instructions lock")
            .as_deref(),
        Some("system")
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
async fn execute_llm_stream_continuation_includes_external_memory_dynamic_block() {
    let _env_lock = isolate_prompt_safe_env_cache();
    let mut session = Session::new("session-stream-2a", "test-model");
    session.metadata.insert(
        "responses.previous_response_id".to_string(),
        "resp_prev".to_string(),
    );

    let (event_tx, _event_rx) = mpsc::channel::<AgentEvent>(16);
    let config = test_config(
        "system\n\n<!-- BAMBOO_EXTERNAL_MEMORY_START -->\n## External Memory (Persistent)\n\nSession note body\n<!-- BAMBOO_EXTERNAL_MEMORY_END -->",
    );
    let prepared_context = PreparedContext {
        messages: vec![
            Message::system(
                "system\n\n<!-- BAMBOO_EXTERNAL_MEMORY_START -->\n## External Memory (Persistent)\n\nSession note body\n<!-- BAMBOO_EXTERNAL_MEMORY_END -->",
            ),
            Message::user("run a tool"),
            Message::assistant("calling tool", None),
            Message::tool_result("call_1", "{\"ok\":true}"),
        ],
        token_usage: usage(0, 22),
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };

    let llm = mock_llm(vec![
        LLMChunk::ResponseId("resp_next".to_string()),
        LLMChunk::Token("done".to_string()),
        LLMChunk::Done,
    ]);
    let llm_dyn: Arc<dyn LLMProvider> = llm.clone();

    let (_stream_output, _duration) = execute_llm_stream(
        &mut session,
        &config,
        &llm_dyn,
        &prepared_context,
        &[],
        &LlmStreamFrame {
            event_tx: &event_tx,
            cancel_token: &CancellationToken::new(),
            session_id: "session-stream-2a",
            model: "test-model",
            provider_name: Some("openai"),
            provider_type: Some("openai"),
            reasoning_effort: None,
            max_context_tokens: 400_000,
            max_output_tokens: 128,
        },
    )
    .await
    .expect("execute llm stream");

    let requested_messages = llm
        .requested_messages
        .lock()
        .expect("messages lock")
        .clone();
    // Volatile context (external memory) is re-sent in the delta but now at the
    // tail, after the conversation continuation.
    assert_eq!(requested_messages.len(), 2);
    assert!(matches!(requested_messages[0].role, Role::Tool));
    assert!(matches!(requested_messages[1].role, Role::User));
    assert!(requested_messages[1]
        .content
        .contains("context_type: external_memory"));
    assert!(requested_messages[1].content.contains("Session note body"));
}

#[tokio::test]
async fn execute_llm_stream_continuation_includes_plan_mode_and_runtime_dynamic_blocks() {
    let _env_lock = isolate_prompt_safe_env_cache();
    let mut session = Session::new("session-stream-2plan", "test-model");
    session.metadata.insert(
        "responses.previous_response_id".to_string(),
        "resp_prev".to_string(),
    );

    let (event_tx, _event_rx) = mpsc::channel::<AgentEvent>(16);
    let system_prompt = "system\n\n<!-- BAMBOO_PLAN_RUNTIME_CONTEXT_START -->\n=== DURABLE PLAN EXECUTION CONTEXT ===\n\nResume rule\n<!-- BAMBOO_PLAN_RUNTIME_CONTEXT_END -->\n\n<!-- BAMBOO_PLAN_MODE_START -->\n=== PLAN MODE ACTIVE ===\n\nEXPLORE\n<!-- BAMBOO_PLAN_MODE_END -->";
    let config = test_config(system_prompt);
    let prepared_context = PreparedContext {
        messages: vec![
            Message::system(system_prompt),
            Message::user("run a tool"),
            Message::assistant("calling tool", None),
            Message::tool_result("call_1", "{\"ok\":true}"),
        ],
        token_usage: usage(0, 22),
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };

    let llm = mock_llm(vec![
        LLMChunk::ResponseId("resp_next".to_string()),
        LLMChunk::Token("done".to_string()),
        LLMChunk::Done,
    ]);
    let llm_dyn: Arc<dyn LLMProvider> = llm.clone();

    let (_stream_output, _duration) = execute_llm_stream(
        &mut session,
        &config,
        &llm_dyn,
        &prepared_context,
        &[],
        &LlmStreamFrame {
            event_tx: &event_tx,
            cancel_token: &CancellationToken::new(),
            session_id: "session-stream-2plan",
            model: "test-model",
            provider_name: Some("openai"),
            provider_type: Some("openai"),
            reasoning_effort: None,
            max_context_tokens: 400_000,
            max_output_tokens: 128,
        },
    )
    .await
    .expect("execute llm stream");

    let requested_messages = llm
        .requested_messages
        .lock()
        .expect("messages lock")
        .clone();
    // Conversation continuation first, then the volatile plan blocks at the tail
    // (in push order: plan_runtime, then plan_mode).
    assert_eq!(requested_messages.len(), 3);
    assert!(matches!(requested_messages[0].role, Role::Tool));
    assert!(matches!(requested_messages[1].role, Role::User));
    assert!(requested_messages[1]
        .content
        .contains("context_type: plan_runtime_state"));
    assert!(requested_messages[1]
        .content
        .contains("DURABLE PLAN EXECUTION CONTEXT"));
    assert!(matches!(requested_messages[2].role, Role::User));
    assert!(requested_messages[2]
        .content
        .contains("context_type: plan_mode_state"));
    assert!(requested_messages[2].content.contains("PLAN MODE ACTIVE"));
}

#[tokio::test]
async fn execute_llm_stream_keeps_previous_response_id_when_local_summary_or_compression_is_active()
{
    let _env_lock = isolate_prompt_safe_env_cache();
    let mut session = Session::new("session-stream-2b", "test-model");
    session.metadata.insert(
        "responses.previous_response_id".to_string(),
        "resp_prev".to_string(),
    );
    session.conversation_summary = Some(ConversationSummary::new(
        "Older work has been summarized locally.",
        6,
        42,
    ));

    let (event_tx, _event_rx) = mpsc::channel::<AgentEvent>(16);
    let config = test_config("system");
    let prepared_context = PreparedContext {
        messages: vec![
            Message::system("system"),
            Message::user("previous work was compressed"),
            Message::assistant("here is the local summary context", None),
            Message::user("continue from the compressed state"),
            Message::tool_result("call_1", "{\"ok\":true}"),
        ],
        token_usage: usage(18, 40),
        truncation_occurred: false,
        segments_removed: 1,
        compressed_message_ids: vec!["msg_old_1".to_string(), "msg_old_2".to_string()],
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };

    let llm = mock_llm(vec![
        LLMChunk::ResponseId("resp_next".to_string()),
        LLMChunk::Token("done".to_string()),
        LLMChunk::Done,
    ]);
    let llm_dyn: Arc<dyn LLMProvider> = llm.clone();

    let (_stream_output, _duration) = execute_llm_stream(
        &mut session,
        &config,
        &llm_dyn,
        &prepared_context,
        &[],
        &LlmStreamFrame {
            event_tx: &event_tx,
            cancel_token: &CancellationToken::new(),
            session_id: "session-stream-2b",
            model: "test-model",
            provider_name: Some("openai"),
            provider_type: Some("openai"),
            reasoning_effort: None,
            max_context_tokens: 400_000,
            max_output_tokens: 128,
        },
    )
    .await
    .expect("execute llm stream");

    let requested_messages = llm
        .requested_messages
        .lock()
        .expect("messages lock")
        .clone();
    assert_eq!(requested_messages.len(), 3);
    assert!(matches!(requested_messages[0].role, Role::User));
    assert!(requested_messages[0]
        .content
        .contains("context_type: conversation_summary"));
    assert!(requested_messages[0]
        .content
        .contains("Older work has been summarized locally."));
    assert!(matches!(requested_messages[1].role, Role::User));
    assert_eq!(
        requested_messages[1].content,
        "continue from the compressed state"
    );
    assert!(matches!(requested_messages[2].role, Role::Tool));
    assert_eq!(
        llm.requested_previous_response_id
            .lock()
            .expect("previous_response_id lock")
            .as_deref(),
        Some("resp_prev")
    );
    assert_eq!(
        llm.requested_instructions
            .lock()
            .expect("instructions lock")
            .as_deref(),
        Some("system")
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
    let _env_lock = isolate_prompt_safe_env_cache();
    let mut session = Session::new("session-stream-3", "test-model");
    session.metadata.insert(
        "responses.previous_response_id".to_string(),
        "resp_prev".to_string(),
    );

    let (event_tx, _event_rx) = mpsc::channel::<AgentEvent>(16);
    let config = test_config("system");
    let prepared_context = PreparedContext {
        messages: vec![
            Message::system("system"),
            Message::user("run a tool"),
            Message::assistant("calling tool", None),
            Message::tool_result("call_1", "{\"ok\":true}"),
        ],
        token_usage: usage(0, 22),
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };

    let llm = mock_llm(vec![
        LLMChunk::ResponseId("resp_next".to_string()),
        LLMChunk::Token("done".to_string()),
        LLMChunk::Done,
    ]);
    let llm_dyn: Arc<dyn LLMProvider> = llm.clone();

    let (_stream_output, _duration) = execute_llm_stream(
        &mut session,
        &config,
        &llm_dyn,
        &prepared_context,
        &[],
        &LlmStreamFrame {
            event_tx: &event_tx,
            cancel_token: &CancellationToken::new(),
            session_id: "session-stream-3",
            model: "test-model",
            provider_name: Some("copilot"),
            provider_type: Some("copilot"),
            reasoning_effort: None,
            max_context_tokens: 400_000,
            max_output_tokens: 128,
        },
    )
    .await
    .expect("execute llm stream");

    let requested_messages = llm
        .requested_messages
        .lock()
        .expect("messages lock")
        .clone();
    assert_eq!(requested_messages.len(), 4);
    assert!(matches!(requested_messages[0].role, Role::System));
    assert_eq!(
        llm.requested_previous_response_id
            .lock()
            .expect("previous_response_id lock")
            .as_deref(),
        None
    );
    assert_eq!(
        llm.requested_instructions
            .lock()
            .expect("instructions lock")
            .as_deref(),
        Some("system")
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

#[tokio::test]
async fn execute_llm_stream_disables_previous_response_id_for_copilot_instance_provider_type() {
    let _env_lock = isolate_prompt_safe_env_cache();
    let mut session = Session::new("session-stream-4", "test-model");
    session.metadata.insert(
        "responses.previous_response_id".to_string(),
        "resp_prev".to_string(),
    );

    let (event_tx, _event_rx) = mpsc::channel::<AgentEvent>(16);
    let config = test_config("system");
    let prepared_context = PreparedContext {
        messages: vec![
            Message::system("system"),
            Message::user("run a tool"),
            Message::assistant("calling tool", None),
            Message::tool_result("call_1", "{\"ok\":true}"),
        ],
        token_usage: usage(0, 22),
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };

    let llm = mock_llm(vec![
        LLMChunk::ResponseId("resp_next".to_string()),
        LLMChunk::Token("done".to_string()),
        LLMChunk::Done,
    ]);
    let llm_dyn: Arc<dyn LLMProvider> = llm.clone();

    let (_stream_output, _duration) = execute_llm_stream(
        &mut session,
        &config,
        &llm_dyn,
        &prepared_context,
        &[],
        &LlmStreamFrame {
            event_tx: &event_tx,
            cancel_token: &CancellationToken::new(),
            session_id: "session-stream-4",
            model: "test-model",
            provider_name: Some("copilot-instance"),
            provider_type: Some("copilot"),
            reasoning_effort: None,
            max_context_tokens: 400_000,
            max_output_tokens: 128,
        },
    )
    .await
    .expect("execute llm stream");

    assert_eq!(
        llm.requested_previous_response_id
            .lock()
            .expect("previous_response_id lock")
            .as_deref(),
        None
    );
    assert_eq!(
        llm.requested_instructions
            .lock()
            .expect("instructions lock")
            .as_deref(),
        Some("system")
    );
    assert!(!session
        .metadata
        .contains_key("responses.previous_response_id"));
}

#[test]
fn overflow_error_detection_matches_common_provider_messages() {
    let _env_lock = isolate_prompt_safe_env_cache();
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
