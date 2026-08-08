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
    requested_required_tool: Mutex<Option<String>>,
    requested_parallel_tool_calls: Mutex<Option<bool>>,
    requested_tool_names: Mutex<Vec<String>>,
    /// Set when the engine routed this request through the canonical
    /// `chat_stream_ir` entry point.
    ir_invoked: Mutex<bool>,
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

    async fn chat_stream_ir(
        &self,
        ir: &bamboo_llm::PromptIR,
        tools: &[bamboo_agent_core::tools::ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        *self.ir_invoked.lock().expect("ir_invoked lock") = true;
        // Faithful Responses-provider adapter: derive the flat messages AND the
        // Responses wire options (instructions / input_messages / previous_response_id)
        // from the IR, exactly like the OpenAI/Copilot overrides — so the captured
        // request reflects what a real adapter sends, not an engine pre-bake.
        let messages = if ir.continuation.is_some() {
            ir.continuation_delta()
        } else {
            ir.flatten()
        };
        let mut effective_options = options.cloned().unwrap_or_default();
        effective_options.responses =
            Some(ir.responses_request_options(effective_options.responses.as_ref()));
        self.chat_stream_with_options(
            &messages,
            tools,
            max_output_tokens,
            model,
            Some(&effective_options),
        )
        .await
    }

    async fn chat_stream_with_options(
        &self,
        messages: &[Message],
        tools: &[bamboo_agent_core::tools::ToolSchema],
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
        *self
            .requested_required_tool
            .lock()
            .expect("required_tool lock") = options.and_then(|value| value.required_tool.clone());
        *self
            .requested_parallel_tool_calls
            .lock()
            .expect("parallel_tool_calls lock") =
            options.and_then(|value| value.parallel_tool_calls);
        *self.requested_tool_names.lock().expect("tool names lock") = tools
            .iter()
            .map(|schema| schema.function.name.clone())
            .collect();

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
        requested_required_tool: Mutex::new(None),
        requested_parallel_tool_calls: Mutex::new(None),
        requested_tool_names: Mutex::new(Vec::new()),
        ir_invoked: Mutex::new(false),
    })
}

fn test_config(system_prompt: &str) -> crate::runtime::config::AgentLoopConfig {
    crate::runtime::config::AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        system_prompt: Some(system_prompt.to_string()),
        ..Default::default()
    }
}

/// The system field that a base prompt resolves to once the framework-invariant
/// agent directives are folded in — mirrors what `build_stable_prompt_frame_*`
/// produces for a bare base (no workspace/env/skill/tool-guide contexts).
fn expected_system_field(base: &str) -> String {
    crate::runtime::runner::prompt_context::append_core_agent_directives(
        base,
        crate::runtime::context::CORE_AGENT_DIRECTIVES,
    )
}

#[test]
fn system_remainder_none_when_persisted_base_absorbed_by_directive_system() {
    // Persisted System message holds the base WITHOUT directives; the assembled
    // system field is that same base WITH the framework directives appended. The
    // base is fully absorbed by the system field, so no duplicate system message
    // is re-emitted into the conversation.
    let persisted = bamboo_agent_core::Message::system("You are Bodhi. Do good work.");
    let stable = expected_system_field("You are Bodhi. Do good work.");
    assert!(super::derive_system_remainder_message(&persisted, &stable).is_none());
}

#[test]
fn system_remainder_none_when_bare_base_is_prefix_of_context_bearing_system() {
    // Production shape: the persisted System message is the bare configured base
    // (contexts are injected only at request time), while the assembled system
    // field appends framework contexts (workspace/skill/tool-guide/env). The bare
    // base is fully contained at the head, so it is absorbed — no redundant
    // re-emission of the base into the conversation. (This would FAIL before the
    // subset-prefix fix, which re-emitted the whole base every round.)
    let persisted = bamboo_agent_core::Message::system("You are Bodhi. Do good work.");
    let stable = "You are Bodhi. Do good work.\n\nWorkspace path: /tmp/x\n\n## Skill\nuse it";
    assert!(super::derive_system_remainder_message(&persisted, stable).is_none());
}

#[test]
fn system_remainder_keeps_genuinely_extra_persisted_content() {
    // The persisted base carries trailing content not present in the system field.
    // Stripping directives from both sides must NOT swallow that genuine extra —
    // it is re-emitted as a remainder message.
    let persisted = bamboo_agent_core::Message::system("Base.\n\nExtra operator note.");
    let stable = expected_system_field("Base.");
    let remainder = super::derive_system_remainder_message(&persisted, &stable)
        .expect("genuinely-extra persisted content must be re-emitted");
    assert!(remainder.content.contains("Extra operator note"));
}

#[test]
fn system_remainder_strips_legacy_goal_block() {
    // The goal now rides the volatile tail (built from the active goal). A legacy
    // persisted System message still carrying a `<!-- BAMBOO_GOAL_START -->` block
    // must NOT resurface it in the SystemRemainder run (it would duplicate the
    // active volatile-tail goal). Once stripped, only the bare base remains, which
    // matches the system field → no remainder.
    let goal_block = "<!-- BAMBOO_GOAL_START -->\nSHIP THE RELEASE\n<!-- BAMBOO_GOAL_END -->";
    let persisted = bamboo_agent_core::Message::system(format!("Base.\n\n{goal_block}"));
    let stable = expected_system_field("Base.");
    assert!(
        super::derive_system_remainder_message(&persisted, &stable).is_none(),
        "legacy goal block must be stripped from the remainder (goal rides the volatile tail)"
    );
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
    assert_eq!(
        requested_messages[0].content,
        expected_system_field("system")
    );
    assert_eq!(
        llm.requested_instructions
            .lock()
            .expect("instructions lock")
            .as_deref(),
        Some(expected_system_field("system").as_str())
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
async fn ledger_checkpoint_failure_stops_before_provider_dispatch() {
    struct FailOncePersistence(std::sync::atomic::AtomicUsize);

    #[async_trait]
    impl bamboo_domain::RuntimeSessionPersistence for FailOncePersistence {
        async fn save_runtime_session(&self, _session: &mut Session) -> std::io::Result<()> {
            let attempt = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if attempt == 0 {
                Err(std::io::Error::other("injected ledger checkpoint failure"))
            } else {
                Ok(())
            }
        }
    }

    let _env_lock = isolate_prompt_safe_env_cache();
    let mut session = Session::new("session-ledger-checkpoint-failure", "test-model");
    let (event_tx, _event_rx) = mpsc::channel::<AgentEvent>(16);
    let mut config = test_config("system");
    let persistence = Arc::new(FailOncePersistence(std::sync::atomic::AtomicUsize::new(0)));
    config.persistence = Some(persistence.clone());
    let prepared_context = PreparedContext {
        messages: vec![Message::system("system"), Message::user("continue")],
        token_usage: usage(0, 22),
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };
    let llm = mock_llm(vec![LLMChunk::Done]);
    let llm_dyn: Arc<dyn LLMProvider> = llm.clone();

    let result = execute_llm_stream(
        &mut session,
        &config,
        &llm_dyn,
        &prepared_context,
        &[],
        &LlmStreamFrame {
            event_tx: &event_tx,
            cancel_token: &CancellationToken::new(),
            session_id: "session-ledger-checkpoint-failure",
            model: "test-model",
            provider_name: Some("openai"),
            provider_type: Some("openai"),
            reasoning_effort: None,
            max_context_tokens: 400_000,
            max_output_tokens: 128,
        },
    )
    .await;
    let Err(error) = result else {
        panic!("ledger state must be durable before provider dispatch");
    };

    assert!(error
        .to_string()
        .contains("model-context ledger checkpoint failed before provider dispatch"));
    assert!(!*llm.ir_invoked.lock().expect("ir_invoked lock"));
    assert!(llm
        .requested_messages
        .lock()
        .expect("messages lock")
        .is_empty());
    assert!(
        session.model_context_state.is_none(),
        "failed checkpoint must roll the in-memory ledger transaction back"
    );

    execute_llm_stream(
        &mut session,
        &config,
        &llm_dyn,
        &prepared_context,
        &[],
        &LlmStreamFrame {
            event_tx: &event_tx,
            cancel_token: &CancellationToken::new(),
            session_id: "session-ledger-checkpoint-failure",
            model: "test-model",
            provider_name: Some("openai"),
            provider_type: Some("openai"),
            reasoning_effort: None,
            max_context_tokens: 400_000,
            max_output_tokens: 128,
        },
    )
    .await
    .expect("retry checkpoints the same deterministic ledger before dispatch");
    assert_eq!(
        persistence.0.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "retry must not bypass the failed durable checkpoint"
    );
    assert!(*llm.ir_invoked.lock().expect("ir_invoked lock"));
}

#[tokio::test]
async fn mock_responses_provider_captures_four_exact_prefix_final_bodies() {
    use bamboo_agent_core::tools::{FunctionCall, FunctionSchema, ToolCall, ToolSchema};
    use bamboo_config::keyword_masking::{KeywordEntry, MatchType};
    use bamboo_config::{
        BodyPatch, BodyPatchOp, KeywordMaskingConfig, PatchValue, RequestOverridesConfig,
        RequestScopeOverride,
    };
    use bamboo_llm::providers::common::{openai_responses, request_overrides};
    use serde_json::{json, Value};

    struct CapturingResponsesProvider {
        bodies: Mutex<Vec<Value>>,
        overrides: RequestOverridesConfig,
        masking: KeywordMaskingConfig,
    }

    #[async_trait]
    impl LLMProvider for CapturingResponsesProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> bamboo_llm::provider::Result<LLMStream> {
            panic!("engine must dispatch the canonical PromptIR path")
        }

        async fn chat_stream_ir(
            &self,
            ir: &bamboo_llm::PromptIR,
            tools: &[ToolSchema],
            max_output_tokens: Option<u32>,
            model: &str,
            options: Option<&LLMRequestOptions>,
        ) -> bamboo_llm::provider::Result<LLMStream> {
            let generic_messages = ir.flatten();
            let responses = ir
                .responses_request_options(options.and_then(|options| options.responses.as_ref()));
            // `None` is intentional: this is the compatible-proxy
            // explicit_prompt_cache=false lowering used by #781.
            let mut body = openai_responses::build_responses_body(
                model,
                &generic_messages,
                tools,
                max_output_tokens,
                options.and_then(|options| options.reasoning_effort),
                Some(&responses),
                options.and_then(|options| options.parallel_tool_calls),
                None,
            );
            request_overrides::apply_overrides_to_body_with_env(
                &mut body,
                Some(&self.overrides),
                request_overrides::ENDPOINT_RESPONSES,
                Some(model),
                &std::collections::HashMap::new(),
            );
            if let Some(name) = options.and_then(|options| options.required_tool.as_deref()) {
                body["tool_choice"] = json!({"type": "function", "name": name});
            }
            bamboo_llm::masking::mask_outbound_body(&mut body, &self.masking);
            self.bodies.lock().expect("bodies lock").push(body);
            Ok(Box::pin(stream::iter(vec![Ok(LLMChunk::Done)])))
        }
    }

    let _env_lock = isolate_prompt_safe_env_cache();
    let provider = Arc::new(CapturingResponsesProvider {
        bodies: Mutex::new(Vec::new()),
        overrides: RequestOverridesConfig {
            common: RequestScopeOverride {
                headers: Default::default(),
                body_patch: vec![BodyPatch {
                    path: "prompt_cache_key".to_string(),
                    op: BodyPatchOp::Set,
                    value: Some(PatchValue::Json(json!("operator-cache-key"))),
                }],
            },
            endpoints: Default::default(),
            rules: Vec::new(),
        },
        masking: KeywordMaskingConfig {
            entries: vec![KeywordEntry {
                pattern: "wire-secret".to_string(),
                match_type: MatchType::Exact,
                enabled: true,
            }],
        },
    });
    let llm: Arc<dyn LLMProvider> = provider.clone();
    let mut session = Session::new("serialized-prefix-session", "test-model");
    session.add_message(Message::system("BASE_IDENTITY"));
    session.add_message(Message::user("wire-secret"));
    session.task_list = Some(TaskList {
        session_id: session.id.clone(),
        title: "Prefix work".to_string(),
        items: vec![TaskItem {
            id: "task-1".to_string(),
            description: "context-v1".to_string(),
            status: TaskItemStatus::InProgress,
            ..TaskItem::default()
        }],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    });
    let config = test_config("BASE_IDENTITY");
    let tools = vec![ToolSchema {
        schema_type: "function".to_string(),
        function: FunctionSchema {
            name: "lookup".to_string(),
            description: "look up a value".to_string(),
            parameters: json!({"type": "object"}),
        },
    }];
    let (event_tx, _event_rx) = mpsc::channel::<AgentEvent>(32);

    macro_rules! capture_round {
        () => {{
            let prepared_context = PreparedContext {
                messages: session.messages.clone(),
                token_usage: usage(0, 22),
                truncation_occurred: false,
                segments_removed: 0,
                compressed_message_ids: Vec::new(),
                prompt_cached_tool_outputs: 0,
                prompt_cached_tool_tokens_saved: 0,
            };
            execute_llm_stream(
                &mut session,
                &config,
                &llm,
                &prepared_context,
                &tools,
                &LlmStreamFrame {
                    event_tx: &event_tx,
                    cancel_token: &CancellationToken::new(),
                    session_id: "serialized-prefix-session",
                    model: "test-model",
                    provider_name: Some("compatible-proxy"),
                    provider_type: Some("openai"),
                    reasoning_effort: None,
                    max_context_tokens: 400_000,
                    max_output_tokens: 128,
                },
            )
            .await
            .expect("captured Responses round");
        }};
    }

    capture_round!();
    session.add_message(Message::assistant(
        "checking",
        Some(vec![ToolCall {
            id: "call-prefix".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "lookup".to_string(),
                arguments: r#"{"query":"wire-secret"}"#.to_string(),
            },
        }]),
    ));
    session.add_message(Message::tool_result("call-prefix", "tool output"));
    capture_round!();
    session.task_list.as_mut().unwrap().items[0].description = "context-v2".to_string();
    session.task_list.as_mut().unwrap().updated_at = Utc::now();
    capture_round!();
    session.add_message(Message::user("continue"));
    capture_round!();

    let bodies = provider.bodies.lock().expect("bodies lock").clone();
    assert_eq!(bodies.len(), 4);
    for pair in bodies.windows(2) {
        let previous = pair[0]["input"].as_array().expect("previous input");
        let current = pair[1]["input"].as_array().expect("current input");
        assert_eq!(
            previous,
            &current[..previous.len()],
            "the complete final serialized input must be the next input's leading slice"
        );
    }
    let tool_round = bodies[1]["input"].as_array().unwrap();
    assert!(tool_round
        .iter()
        .any(|item| item["type"] == "function_call"));
    assert!(tool_round
        .iter()
        .any(|item| item["type"] == "function_call_output"));
    assert!(bodies.iter().all(|body| {
        body["prompt_cache_key"] == "operator-cache-key"
            && body.get("prompt_cache_options").is_none()
    }));
    let final_wire = serde_json::to_string(&bodies).unwrap();
    assert!(!final_wire.contains("wire-secret"));
    assert!(session
        .messages
        .iter()
        .all(|message| !message.content.contains("BAMBOO_MODEL_CONTEXT_EVENT_START")));
}

#[tokio::test]
async fn explicit_activation_pending_suppresses_answer_tokens() {
    let _env_lock = isolate_prompt_safe_env_cache();
    let mut session = Session::new("session-explicit-guard", "test-model");
    session.metadata.insert(
        bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTION_SOURCE_KEY.to_string(),
        "explicit".to_string(),
    );
    session.metadata.insert(
        bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.to_string(),
        "[\"review\"]".to_string(),
    );
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
        LLMChunk::Token("answer must remain hidden".to_string()),
        LLMChunk::Done,
    ]);
    let llm_dyn: Arc<dyn LLMProvider> = llm.clone();
    let tool_schemas = ["load_skill", "Read"]
        .into_iter()
        .map(|name| bamboo_agent_core::tools::ToolSchema {
            schema_type: "function".to_string(),
            function: bamboo_agent_core::tools::FunctionSchema {
                name: name.to_string(),
                description: String::new(),
                parameters: serde_json::json!({"type": "object"}),
            },
        })
        .collect::<Vec<_>>();

    let (stream_output, _) = execute_llm_stream(
        &mut session,
        &config,
        &llm_dyn,
        &prepared_context,
        &tool_schemas,
        &LlmStreamFrame {
            event_tx: &event_tx,
            cancel_token: &CancellationToken::new(),
            session_id: "session-explicit-guard",
            model: "test-model",
            provider_name: None,
            provider_type: None,
            reasoning_effort: None,
            max_context_tokens: 400_000,
            max_output_tokens: 128,
        },
    )
    .await
    .expect("execute guarded stream");

    assert_eq!(stream_output.content, "answer must remain hidden");
    assert_eq!(
        llm.requested_required_tool
            .lock()
            .expect("required_tool lock")
            .as_deref(),
        Some("load_skill")
    );
    assert_eq!(
        *llm.requested_parallel_tool_calls
            .lock()
            .expect("parallel_tool_calls lock"),
        Some(false)
    );
    assert_eq!(
        *llm.requested_tool_names.lock().expect("tool names lock"),
        vec!["load_skill".to_string()]
    );
    drop(event_tx);
    let events = std::iter::from_fn(|| event_rx.try_recv().ok()).collect::<Vec<_>>();
    assert!(events
        .iter()
        .all(|event| matches!(event, AgentEvent::TokenBudgetUpdated { .. })));
}

#[tokio::test]
async fn execute_llm_stream_emits_final_budget_event_with_provider_usage() {
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
        LLMChunk::ProviderUsage {
            input_tokens: Some(100),
            output_tokens: Some(80),
            total_tokens: Some(180),
            reasoning_tokens: Some(24),
            cache_creation_input_tokens: None,
            cache_read_input_tokens: Some(34),
            cache_write_input_tokens: None,
        },
        // Later legacy summaries/cache frames must not overwrite authoritative
        // provider fields or double the cache badge.
        LLMChunk::UsageSummary {
            output_tokens: 56,
            thinking_tokens: 78,
        },
        LLMChunk::CacheUsage {
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 34,
            input_tokens: 66,
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
            assert_eq!(usage.thinking_tokens, 24);
            assert_eq!(usage.cache_read_input_tokens, 34);
        }
        other => panic!("unexpected second event: {other:?}"),
    }

    assert_eq!(stream_output.input_tokens, 66);
    assert_eq!(stream_output.output_tokens, 80);
    assert_eq!(stream_output.thinking_tokens, 24);
    assert_eq!(stream_output.cache_read_input_tokens, 34);
    assert_eq!(
        stream_output
            .provider_usage
            .and_then(|usage| usage.input_tokens),
        Some(100)
    );
    assert_eq!(
        session
            .token_usage
            .as_ref()
            .map(|usage| (usage.thinking_tokens, usage.cache_read_input_tokens)),
        Some((24, 34))
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
    // On the first ledger epoch the full task snapshot is seeded before the
    // real transcript. It is provider-visible but never added to Session.messages.
    assert!(matches!(requested_messages[1].role, Role::User));
    assert!(requested_messages[1]
        .content
        .contains("context_type: task_snapshot"));
    assert!(requested_messages[1]
        .content
        .contains("Implement task block wiring"));
    assert!(matches!(requested_messages[2].role, Role::User));
    assert_eq!(requested_messages[2].content, "continue");
    assert!(session
        .messages
        .iter()
        .all(|message| !message.content.contains("BAMBOO_MODEL_CONTEXT_EVENT_START")));
    assert_eq!(
        llm.requested_instructions
            .lock()
            .expect("instructions lock")
            .as_deref(),
        Some(expected_system_field("system").as_str())
    );
}

#[test]
fn build_request_envelope_seeds_ledger_without_rewriting_old_breakpoints() {
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

    // The first epoch seeds typed context ahead of the real transcript. Future
    // updates append after the prior transcript boundary instead of moving it.
    assert_eq!(
        envelope
            .ir
            .run(bamboo_llm::SegmentRole::ModelTranscript)
            .len(),
        2
    );
    let transcript = envelope.ir.run(bamboo_llm::SegmentRole::ModelTranscript);
    assert!(transcript[0]
        .content
        .contains("context_type: task_snapshot"));
    assert_eq!(transcript[1].content, "continue");

    // A rolling breakpoint would remove an annotation from the former last item
    // on the next round and violate byte-prefix identity. Only stable fixed
    // boundaries (for example a relocated tool guide) may be generated.
    assert!(envelope.ir.cache.cache_system);
    assert!(envelope.ir.cache.cache_tools);
    assert!(!envelope.ir.cache.is_breakpoint(&last_user_id));
    assert!(envelope.ir.cache.breakpoint_message_ids.is_empty());
    // The stable prefix uses the 1-hour extended TTL so the cache survives
    // pauses longer than the 5-minute default and big tool results keep hitting.
    assert_eq!(envelope.ir.cache.ttl, bamboo_llm::CacheTtl::Extended);
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
    let round1 = ctx(vec![
        Message::system("BASE_IDENTITY"),
        Message::user("first"),
    ]);
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
        e1.ir.system_text, e2.ir.system_text,
        "system prompt must stay byte-stable across rounds"
    );

    // The relocated guide message content is unchanged (its id may differ — only
    // content is what the provider caches).
    let guide = |e: &super::PreparedRequestEnvelope| -> String {
        e.ir.run(bamboo_llm::SegmentRole::StablePrefix)
            .iter()
            .find(|m| m.content.contains("NOVA_GUIDANCE_MARKER"))
            .map(|m| m.content.clone())
            .expect("relocated guide present")
    };
    assert_eq!(guide(&e1), guide(&e2), "guide prefix must stay byte-stable");

    // The cacheable-prefix sections are byte-identical across rounds, so the cache
    // hits. Compare the (name, content) of every section directly — the strongest
    // form of the old drift-hash check.
    let sections = |e: &super::PreparedRequestEnvelope| -> Vec<(String, String)> {
        e.stable_prefix_sections
            .iter()
            .map(|s| (s.name.to_string(), s.content.clone()))
            .collect()
    };
    assert!(!e1.stable_prefix_sections.is_empty());
    assert_eq!(
        sections(&e1),
        sections(&e2),
        "stable prefix sections must not drift between rounds"
    );
}

#[test]
fn execution_model_change_starts_one_cache_scope_epoch() {
    let _env_lock = isolate_prompt_safe_env_cache();
    let mut session = Session::new("session-model-scope", "recorded-model");
    let config = test_config("BASE_IDENTITY");
    let prepared_context = PreparedContext {
        messages: vec![Message::system("BASE_IDENTITY"), Message::user("go")],
        token_usage: usage(0, 22),
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };

    let first = super::build_request_envelope_reconciled(
        &mut session,
        &prepared_context,
        &config,
        &[],
        "model-a",
    );
    assert_eq!(first.prefix_epoch, 0);
    assert!(first.prefix_reset_reason.is_none());

    let changed = super::build_request_envelope_reconciled(
        &mut session,
        &prepared_context,
        &config,
        &[],
        "model-b",
    );
    assert_eq!(changed.prefix_epoch, 1);
    assert_eq!(
        changed.prefix_reset_reason,
        Some(bamboo_domain::ModelContextResetReason::CacheScopeChanged)
    );

    let next = super::build_request_envelope_reconciled(
        &mut session,
        &prepared_context,
        &config,
        &[],
        "model-b",
    );
    assert_eq!(next.prefix_epoch, changed.prefix_epoch);
    assert!(next.prefix_reset_reason.is_none());
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

    // Requests go through the single canonical IR entry point.
    assert!(
        *llm.ir_invoked.lock().expect("ir_invoked lock"),
        "request must route through chat_stream_ir"
    );

    // What the provider actually received: the leading system message keeps the
    // static identity but NOT the tool/server guide, which arrives as its own
    // message instead.
    let messages = llm
        .requested_messages
        .lock()
        .expect("messages lock")
        .clone();
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
        messages: vec![Message::system("BASE_SYSTEM_IDENTITY"), Message::user("go")],
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
        envelope.ir.system_text.contains("BASE_SYSTEM_IDENTITY"),
        "lane system keeps the static base identity"
    );
    assert!(
        !envelope.ir.system_text.contains("NOVA_GUIDANCE_MARKER"),
        "tool/server guide must be removed from the lane system prompt"
    );

    // It rides as a fixed stable-prefix MESSAGE (a typed, never-compressed context
    // block at a known position) instead.
    let guide = envelope
        .ir
        .run(bamboo_llm::SegmentRole::StablePrefix)
        .iter()
        .find(|m| m.content.contains("NOVA_GUIDANCE_MARKER"))
        .expect("tool guide relocated into a stable-prefix message");
    assert_eq!(guide.role, bamboo_agent_core::Role::User);
    assert!(guide.never_compress);

    // And that relocated, session-stable message earns its own cache breakpoint.
    assert!(envelope.ir.cache.is_breakpoint(&guide.id));

    // The Responses-API view (derived by the adapter from the IR) mirrors the
    // relocation: the guide leaves `instructions` (the system field) and rides at
    // the FRONT of the input messages — so every provider family gets the same
    // static-system structure.
    let responses = envelope.ir.responses_request_options(None);
    assert!(
        !responses
            .instructions
            .as_deref()
            .unwrap_or_default()
            .contains("NOVA_GUIDANCE_MARKER"),
        "tool/server guide must be removed from Responses instructions"
    );
    assert!(
        responses
            .input_messages
            .as_deref()
            .and_then(|m| m.first())
            .is_some_and(|m| m.content.contains("NOVA_GUIDANCE_MARKER")),
        "tool/server guide leads the Responses input messages"
    );
}

#[test]
fn build_request_envelope_relocates_session_context_after_tool_guide() {
    // Session-variable context (here: loaded-skill context) must leave the static
    // system field and ride as a typed context-block message positioned AFTER the
    // large invariant tool guide — so it never shifts the cached head.
    let _env_lock = isolate_prompt_safe_env_cache();
    let mut session = Session::new("session-sessionctx", "test-model");
    session.metadata.insert(
        "skill.context".to_string(),
        "SKILL_CONTEXT_MARKER loaded skill body".to_string(),
    );
    let mut config = test_config("BASE_SYSTEM_IDENTITY");
    config.mcp_tool_guidance = Some("NOVA_GUIDANCE_MARKER targeting workflow".to_string());
    let prepared_context = PreparedContext {
        messages: vec![Message::system("BASE_SYSTEM_IDENTITY"), Message::user("go")],
        token_usage: usage(0, 24),
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };

    let envelope = super::build_request_envelope(&session, &prepared_context, &config, &[]);

    // Not in the cacheable, invariant system field.
    assert!(
        !envelope.ir.system_text.contains("SKILL_CONTEXT_MARKER"),
        "session-variable skill context must leave the system prompt"
    );

    // The immutable guide remains the stable prefix; mutable skill state is a
    // typed event in the chronological ledger.
    let stable = envelope.ir.run(bamboo_llm::SegmentRole::StablePrefix);
    let transcript = envelope.ir.run(bamboo_llm::SegmentRole::ModelTranscript);
    let skill_pos = transcript
        .iter()
        .position(|m| m.content.contains("SKILL_CONTEXT_MARKER"))
        .expect("skill context reconciled into the model transcript");
    assert!(transcript[skill_pos]
        .content
        .contains("context_type: skill_context"));
    assert!(!transcript[skill_pos].never_compress);

    // Provider lowering places the stable guide before the ledger event.
    let guide_pos = stable
        .iter()
        .position(|m| m.content.contains("NOVA_GUIDANCE_MARKER"))
        .expect("tool guide present");
    let responses = envelope.ir.responses_input();
    let response_guide = responses
        .iter()
        .position(|m| m.content.contains("NOVA_GUIDANCE_MARKER"))
        .unwrap();
    let response_skill = responses
        .iter()
        .position(|m| m.content.contains("SKILL_CONTEXT_MARKER"))
        .unwrap();
    assert_eq!(guide_pos, 0);
    assert!(response_guide < response_skill);
    assert!(session
        .messages
        .iter()
        .all(|message| !message.content.contains("BAMBOO_MODEL_CONTEXT_EVENT_START")));
}

#[test]
fn lane_system_is_invariant_to_session_variable_context() {
    // The cache win: two sessions that differ ONLY in session-variable context
    // (here loaded skills) must produce a byte-identical system field, so the big
    // invariant prefix (system + tool guide) can share an automatic prefix cache
    // across sessions instead of diverging at the first variable byte. (Fails on
    // the old layout, which merged the skill block into the system prompt.)
    let _env_lock = isolate_prompt_safe_env_cache();
    let mut config = test_config("BASE_SYSTEM_IDENTITY");
    config.mcp_tool_guidance = Some("NOVA_GUIDANCE_MARKER targeting workflow".to_string());
    let ctx = || PreparedContext {
        messages: vec![Message::system("BASE_SYSTEM_IDENTITY"), Message::user("go")],
        token_usage: usage(0, 24),
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };

    let mut with_skill = Session::new("sess-a", "test-model");
    with_skill
        .metadata
        .insert("skill.context".to_string(), "SKILL_A body".to_string());
    let without_skill = Session::new("sess-b", "test-model");

    let e_with = super::build_request_envelope(&with_skill, &ctx(), &config, &[]);
    let e_without = super::build_request_envelope(&without_skill, &ctx(), &config, &[]);

    assert_eq!(
        e_with.ir.system_text, e_without.ir.system_text,
        "system field must be invariant to session-variable context"
    );
    // The relocated invariant guide block is identical too (same cached block).
    let guide = |e: &super::PreparedRequestEnvelope| {
        e.ir.run(bamboo_llm::SegmentRole::StablePrefix)
            .iter()
            .find(|m| m.content.contains("NOVA_GUIDANCE_MARKER"))
            .map(|m| m.content.clone())
            .expect("guide present")
    };
    assert_eq!(guide(&e_with), guide(&e_without));
}

#[test]
fn system_field_is_assembled_as_discrete_base_core_env_blocks() {
    // Step 4 invariant: the cross-session-invariant system field is carried as a
    // STRUCTURED ARRAY of typed PromptBlocks (identity `base`, framework
    // `core_directives`, globally-stable `env`) — not one glued string. A
    // block-native provider can render one wire block per entry; the single
    // system cache breakpoint anchors on the LAST block.
    let _env_lock = isolate_prompt_safe_env_cache();
    let session = Session::new("session-sysblocks", "test-model");
    let mut config = test_config("BASE_SYSTEM_IDENTITY");
    // A non-empty tool/server guide triggers the static-system relocation under
    // which the system field is emitted as discrete blocks.
    config.mcp_tool_guidance = Some("NOVA_GUIDANCE_MARKER targeting workflow".to_string());
    let prepared_context = PreparedContext {
        messages: vec![Message::system("BASE_SYSTEM_IDENTITY"), Message::user("go")],
        token_usage: usage(0, 24),
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };

    let envelope = super::build_request_envelope(&session, &prepared_context, &config, &[]);
    let blocks = &envelope.ir.system_blocks;

    // Discrete, structured, and non-empty.
    assert!(!blocks.is_empty(), "system field must be structured blocks");
    // Identity `base` leads and carries the configured base identity.
    assert_eq!(blocks[0].id, "base");
    assert_eq!(blocks[0].kind, bamboo_domain::ContextBlockType::Base);
    assert!(blocks[0].text.contains("BASE_SYSTEM_IDENTITY"));
    // Framework `core_directives` is its own block (always present, non-empty).
    let core = blocks
        .iter()
        .find(|b| b.id == "core_directives")
        .expect("core_directives is a discrete block");
    assert_eq!(core.kind, bamboo_domain::ContextBlockType::CoreDirectives);
    assert!(!core.text.trim().is_empty());
    // Every system block is marked Stable (it forms the cacheable head).
    assert!(blocks
        .iter()
        .all(|b| b.stability == bamboo_domain::ContextBlockStability::Stable));
    // Exactly the LAST block is the cache anchor (one system breakpoint).
    assert!(blocks.last().expect("non-empty").cache_anchor);
    assert_eq!(
        blocks.iter().filter(|b| b.cache_anchor).count(),
        1,
        "exactly one system cache breakpoint, on the last block"
    );
    // The tool guide is NOT a system block — it rides as a relocated message.
    assert!(blocks
        .iter()
        .all(|b| !b.text.contains("NOVA_GUIDANCE_MARKER")));
}

#[test]
fn system_blocks_join_is_byte_identical_to_lane_system_string() {
    // The structured `system_blocks` and the legacy joined `stable_instructions`
    // string are two views of the SAME bytes: their `"\n\n"` join must reproduce
    // the string wire exactly, so flipping a provider to the structured form
    // changes nothing on the wire.
    let _env_lock = isolate_prompt_safe_env_cache();
    let session = Session::new("session-sysblocks-join", "test-model");
    let mut config = test_config("BASE_SYSTEM_IDENTITY");
    config.mcp_tool_guidance = Some("NOVA_GUIDANCE_MARKER targeting workflow".to_string());
    let prepared_context = PreparedContext {
        messages: vec![Message::system("BASE_SYSTEM_IDENTITY"), Message::user("go")],
        token_usage: usage(0, 24),
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };

    let envelope = super::build_request_envelope(&session, &prepared_context, &config, &[]);
    let joined = envelope
        .ir
        .system_blocks
        .iter()
        .map(|b| b.text.as_str())
        .collect::<Vec<_>>()
        .join("\n\n");
    assert_eq!(
        envelope.ir.system_text, joined,
        "joined system blocks must reproduce the string system field byte-for-byte"
    );
    // `system_text()` is the provider-facing accessor; it returns the same bytes.
    assert_eq!(envelope.ir.system_field(), joined);
}

#[test]
fn goal_rides_model_context_ledger_and_never_leaks_into_system_blocks() {
    // Goal-leak fix (step 6a): a per-session goal is built as a dedicated volatile
    // GoalState block in the tail — NOT injected into the cached system prefix —
    // so changing the goal never invalidates the cached system head.
    let _env_lock = isolate_prompt_safe_env_cache();
    let session = Session::new("session-goal-block", "test-model");
    let mut config = test_config("BASE_SYSTEM_IDENTITY");
    config.mcp_tool_guidance = Some("NOVA_GUIDANCE_MARKER targeting workflow".to_string());
    config.gold_config = Some(crate::runtime::config::GoldConfig {
        enabled: true,
        goal: Some("SHIP_THE_RELEASE_GOAL".to_string()),
        ..Default::default()
    });
    assert_eq!(config.active_goal(), Some("SHIP_THE_RELEASE_GOAL"));
    let prepared_context = PreparedContext {
        messages: vec![Message::system("BASE_SYSTEM_IDENTITY"), Message::user("go")],
        token_usage: usage(0, 24),
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };

    let envelope = super::build_request_envelope(&session, &prepared_context, &config, &[]);

    // Goal does NOT leak into any system block, nor the joined system string.
    assert!(envelope
        .ir
        .system_blocks
        .iter()
        .all(|b| !b.text.contains("SHIP_THE_RELEASE_GOAL")));
    assert!(!envelope.ir.system_text.contains("SHIP_THE_RELEASE_GOAL"));

    // It rides the append-only model transcript as a typed GoalState snapshot.
    let goal_msg = envelope
        .ir
        .run(bamboo_llm::SegmentRole::ModelTranscript)
        .iter()
        .find(|m| m.content.contains("context_type: goal_state"))
        .expect("goal rides the model-context ledger as a goal_state block");
    assert!(goal_msg.content.contains("SHIP_THE_RELEASE_GOAL"));
}

#[test]
fn zero_tools_fallback_keeps_merged_system_string_and_no_blocks() {
    // When there is no tool/server guide (no tools, no MCP guidance), the
    // invariant system field stays a byte-authoritative string and
    // `system_blocks` is empty. Mutable context is carried by the ledger.
    let _env_lock = isolate_prompt_safe_env_cache();
    let session = Session::new("session-zero-tools", "test-model");
    let config = test_config("BASE_SYSTEM_IDENTITY");
    let prepared_context = PreparedContext {
        messages: vec![Message::system("BASE_SYSTEM_IDENTITY"), Message::user("go")],
        token_usage: usage(0, 24),
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };

    let envelope = super::build_request_envelope(&session, &prepared_context, &config, &[]);
    assert!(
        envelope.ir.system_blocks.is_empty(),
        "no relocation → no structured system blocks"
    );
    assert!(envelope.ir.system_text.contains("BASE_SYSTEM_IDENTITY"));
}

#[test]
fn agent_loop_prompt_cache_key_is_stable_isolated_and_private() {
    let raw_session_id = "session-secret-alpha";
    let first = super::agent_loop_prompt_cache_key(Some(raw_session_id), Some("agent_loop"))
        .expect("agent-loop session gets an affinity key");
    let resumed = super::agent_loop_prompt_cache_key(Some(raw_session_id), Some("agent_loop"))
        .expect("resumed session gets the same affinity key");
    let other_session =
        super::agent_loop_prompt_cache_key(Some("session-secret-beta"), Some("agent_loop"))
            .expect("different session gets an affinity key");

    assert_eq!(first, resumed);
    assert_eq!(
        first,
        "29665ec5c5f0fc1c3f89fb420c578cf217757f6a3d4f52b57ca711c94f15e599"
    );
    assert_eq!(first.len(), 64);
    assert!(first
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
    assert_ne!(first, other_session);
    assert!(!first.contains(raw_session_id));
    assert!(super::agent_loop_prompt_cache_key(None, Some("agent_loop")).is_none());
    assert!(super::agent_loop_prompt_cache_key(Some(""), Some("agent_loop")).is_none());
    assert!(super::agent_loop_prompt_cache_key(Some("   "), Some("agent_loop")).is_none());
    assert!(super::agent_loop_prompt_cache_key(Some(raw_session_id), None).is_none());
    assert!(
        super::agent_loop_prompt_cache_key(Some(raw_session_id), Some("title_generation"))
            .is_none()
    );
}

#[test]
fn plan_llm_request_model_transcript_path_records_observability() {
    // The single request-planning seam: a normal request takes the canonical
    // lanes path and the render descriptor captures the system shape + cache plan.
    let _env_lock = isolate_prompt_safe_env_cache();
    let session = Session::new("session-plan", "test-model");
    let mut config = test_config("BASE_IDENTITY");
    config.mcp_tool_guidance = Some("NOVA_GUIDANCE_MARKER guide".to_string());
    let prepared_context = PreparedContext {
        messages: vec![Message::system("BASE_IDENTITY"), Message::user("go")],
        token_usage: usage(0, 24),
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };
    let envelope = super::build_request_envelope(&session, &prepared_context, &config, &[]);

    let planned = super::plan_llm_request(&envelope, "session-plan", None, 3, None);

    // No continuation set on the IR → the canonical model-transcript wire.
    assert!(envelope.ir.continuation.is_none());
    assert_eq!(planned.render.wire, "model_transcript");
    assert!(planned.render.model_transcript_messages >= 1);
    // System rendered as structured blocks (tool guide present → relocation on).
    assert!(planned.render.system_block_count >= 1);
    assert_eq!(planned.render.tool_count, 3);
    // Cache plan surfaced into the observability + carried in the request options.
    assert!(planned.render.cache_system);
    assert_eq!(planned.render.cache_ttl, "1h");
    assert!(planned.request_options.cache.is_some());
    let responses = planned
        .request_options
        .responses
        .as_ref()
        .expect("agent loop carries Responses policy");
    let cache_key = responses
        .prompt_cache_key
        .as_deref()
        .expect("agent loop carries a session-scoped cache-affinity key");
    assert_eq!(cache_key.len(), 64);
    assert!(!cache_key.contains("session-plan"));
}

#[test]
fn plan_llm_request_continuation_path_builds_delta() {
    // A Responses-API continuation takes the flat delta path: only the messages
    // after the last assistant turn are sent, and the render reflects that.
    let _env_lock = isolate_prompt_safe_env_cache();
    let session = Session::new("session-plan-cont", "test-model");
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
    let envelope = with_ir_continuation(
        super::build_request_envelope(&session, &prepared_context, &config, &[]),
        "resp_prev",
    );

    let planned = super::plan_llm_request(&envelope, "session-plan-cont", None, 0, None);

    assert_eq!(planned.render.wire, "responses_continuation");
    // Delta is the tool result after the last assistant turn — NOT the full convo.
    let delta = envelope.ir.continuation_delta();
    assert!(!delta.is_empty());
    assert_eq!(planned.render.request_message_count, delta.len());
    // The provider derives previous_response_id from the IR continuation (the engine
    // no longer pre-bakes it into the request options).
    let responses = envelope
        .ir
        .responses_request_options(planned.request_options.responses.as_ref());
    assert_eq!(responses.previous_response_id.as_deref(), Some("resp_prev"));
}

fn message_shape(messages: &[bamboo_agent_core::Message]) -> Vec<(Role, String)> {
    messages
        .iter()
        .map(|m| (m.role.clone(), m.content.clone()))
        .collect()
}

/// Mirror the engine dispatch: set the stateful Responses continuation on the IR
/// (boundary = the last assistant turn in the ModelTranscript run), as
/// `execute_llm_stream` does before planning/dispatch.
fn with_ir_continuation(
    mut envelope: super::PreparedRequestEnvelope,
    previous_response_id: &str,
) -> super::PreparedRequestEnvelope {
    let last_committed_assistant_id = envelope
        .ir
        .run(bamboo_llm::SegmentRole::ModelTranscript)
        .iter()
        .rev()
        .find(|m| matches!(m.role, Role::Assistant))
        .map(|m| m.id.clone());
    envelope.ir.continuation = Some(bamboo_llm::Continuation {
        previous_response_id: previous_response_id.to_string(),
        last_committed_assistant_id,
    });
    envelope
}

#[test]
fn envelope_ir_flatten_orders_stable_prefix_and_model_transcript() {
    // The normal engine path lowers one chronological ModelTranscript after the
    // immutable guide. Initial typed snapshots precede real history.
    let _env_lock = isolate_prompt_safe_env_cache();
    let mut session = Session::new("session-ir-golden", "test-model");
    // A persisted System message that diverges from the assembled system field
    // becomes a SystemRemainder run — the case that must stay byte-stable.
    session.add_message(Message::system("PERSISTED OPERATOR NOTE"));
    session.task_list = Some(TaskList {
        session_id: session.id.clone(),
        title: "Tasks".to_string(),
        items: vec![TaskItem {
            id: "t1".to_string(),
            // A distinctive marker: a plain phrase like "do it" collides with the
            // system directives (which contain the substring "do it"), so
            // `pos("do it")` would match the leading System run, not this tail.
            description: "VOLATILE_TAIL_TASK".to_string(),
            status: TaskItemStatus::InProgress,
            ..TaskItem::default()
        }],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    });
    let mut config = test_config("BASE_IDENTITY");
    config.mcp_tool_guidance = Some("NOVA_GUIDANCE_MARKER guide".to_string());
    let prepared_context = PreparedContext {
        messages: vec![
            Message::system("PERSISTED OPERATOR NOTE"),
            Message::user("u1"),
            Message::assistant("a1", None),
            Message::user("u2"),
        ],
        token_usage: usage(0, 24),
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };

    let envelope = super::build_request_envelope(&session, &prepared_context, &config, &[]);

    assert!(envelope
        .ir
        .run(bamboo_llm::SegmentRole::SystemRemainder)
        .is_empty());
    assert!(envelope
        .ir
        .run(bamboo_llm::SegmentRole::VolatileTail)
        .is_empty());
    assert!(!envelope
        .ir
        .run(bamboo_llm::SegmentRole::ModelTranscript)
        .is_empty());
    // Flat order: system, stable guide, initial typed snapshot, then real history.
    let flat = message_shape(&envelope.ir.flatten());
    assert!(matches!(flat[0].0, Role::System));
    assert!(flat[0].1.contains("BASE_IDENTITY"), "system field leads");
    let pos = |needle: &str| flat.iter().position(|(_, c)| c.contains(needle));
    let guide = pos("NOVA_GUIDANCE_MARKER").expect("relocated tool guide present");
    let task = pos("VOLATILE_TAIL_TASK").expect("task snapshot present");
    let remainder = pos("PERSISTED OPERATOR NOTE").expect("system remainder present");
    let conversation = pos("u1").expect("conversation present");
    assert!(
        0 < guide && guide < task,
        "stable prefix (guide) precedes the ledger"
    );
    assert!(
        task < remainder && remainder < conversation,
        "initial snapshot precedes the unmodified real transcript"
    );
    // system_field() returns the byte-authoritative system string.
    assert_eq!(envelope.ir.system_field(), envelope.ir.system_text);
}

#[test]
fn engine_model_transcript_continuation_delta_starts_after_assistant_boundary() {
    // The engine's normal request no longer reconstructs four mutable lanes.
    // Continuation slicing operates over the single chronological transcript,
    // retaining only provider-visible items after the committed assistant.
    let _env_lock = isolate_prompt_safe_env_cache();
    let mut session = Session::new("session-ir-delta-golden", "test-model");
    session.add_message(Message::system("PERSISTED OPERATOR NOTE"));
    // DynamicContext run: a conversation summary (rides the front/dynamic context).
    session.conversation_summary = Some(ConversationSummary::new("DELTA_SUMMARY_MARKER", 2, 50));
    // VolatileTail run: a task list.
    session.task_list = Some(TaskList {
        session_id: session.id.clone(),
        title: "Tasks".to_string(),
        items: vec![TaskItem {
            id: "t1".to_string(),
            description: "DELTA_TASK_MARKER".to_string(),
            status: TaskItemStatus::InProgress,
            ..TaskItem::default()
        }],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    });
    let config = test_config("BASE_IDENTITY");
    let prepared_context = PreparedContext {
        messages: vec![
            Message::system("PERSISTED OPERATOR NOTE"),
            Message::user("run a tool"),
            Message::assistant("calling tool", None),
            Message::tool_result("call_1", "{\"ok\":true}"),
        ],
        token_usage: usage(2, 40),
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };

    let envelope = with_ir_continuation(
        super::build_request_envelope(&session, &prepared_context, &config, &[]),
        "resp_prev",
    );

    let delta = envelope.ir.continuation_delta();
    assert_eq!(delta.len(), 1);
    assert!(matches!(delta[0].role, Role::Tool));
    assert_eq!(delta[0].content, "{\"ok\":true}");
    assert!(delta.iter().all(|message| {
        !message.content.contains("DELTA_SUMMARY_MARKER")
            && !message.content.contains("DELTA_TASK_MARKER")
            && !message.content.contains("PERSISTED OPERATOR NOTE")
    }));
}

#[test]
fn responses_continuation_uses_full_input_not_the_delta() {
    // Locks review finding #1: on a Responses continuation the request sends the
    // FULL input view (== ir.responses_input()) via input_messages +
    // previous_response_id. select_responses_input_messages prefers the Explicit
    // input_messages over the delta `messages` arg, so the continuation_delta is
    // NOT what rides the Responses wire. Byte-faithful to legacy — this test makes
    // that intentional rather than accidental, so a future "real delta" change
    // fails loudly.
    let _env_lock = isolate_prompt_safe_env_cache();
    let mut session = Session::new("session-resp-cont", "test-model");
    session.add_message(Message::system("PERSISTED OPERATOR NOTE"));
    let config = test_config("BASE_IDENTITY");
    let prepared_context = PreparedContext {
        messages: vec![
            Message::system("PERSISTED OPERATOR NOTE"),
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

    let envelope = with_ir_continuation(
        super::build_request_envelope(&session, &prepared_context, &config, &[]),
        "resp_prev",
    );
    let planned = super::plan_llm_request(&envelope, "session-resp-cont", None, 0, None);

    // The adapter derives the Responses wire view from the IR + the engine's request
    // POLICY (planned.request_options.responses).
    let responses = envelope
        .ir
        .responses_request_options(planned.request_options.responses.as_ref());

    let input = responses
        .input_messages
        .as_ref()
        .expect("input_messages present");
    assert_eq!(
        message_shape(input),
        message_shape(&envelope.ir.responses_input()),
        "Responses continuation sends the FULL input view, not the delta"
    );
    assert_eq!(responses.previous_response_id.as_deref(), Some("resp_prev"));
    assert!(
        input.len() > envelope.ir.continuation_delta().len(),
        "FULL Responses input must exceed the smaller continuation delta"
    );
}

#[test]
fn ir_cache_matches_request_options_cache() {
    // Locks review finding #4: the cache plan lives in both ir.cache and
    // options.cache (the provider reads options.cache; the IR carries its own copy
    // for observability). They must stay identical so the two can never diverge.
    let _env_lock = isolate_prompt_safe_env_cache();
    let session = Session::new("session-cache-dup", "test-model");
    let config = test_config("BASE_IDENTITY");
    let prepared_context = PreparedContext {
        messages: vec![Message::system("BASE_IDENTITY"), Message::user("go")],
        token_usage: usage(0, 24),
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };

    let envelope = super::build_request_envelope(&session, &prepared_context, &config, &[]);
    let planned = super::plan_llm_request(&envelope, "session-cache-dup", None, 0, None);
    let options_cache = planned
        .request_options
        .cache
        .as_ref()
        .expect("cache plan present");

    assert_eq!(envelope.ir.cache.cache_system, options_cache.cache_system);
    assert_eq!(envelope.ir.cache.cache_tools, options_cache.cache_tools);
    assert_eq!(envelope.ir.cache.ttl, options_cache.ttl);
    assert_eq!(
        envelope.ir.cache.breakpoint_message_ids,
        options_cache.breakpoint_message_ids
    );
}

#[test]
fn cache_anchor_marks_only_the_last_system_block() {
    // Review finding #3: the engine marks `cache_anchor` on the LAST system block,
    // and the Anthropic builder hardcodes `cache_control` onto the last system
    // block (without reading `cache_anchor`). They agree only because the anchor is
    // last — lock that so moving the anchor (or adding a second) is caught here
    // instead of silently diverging from the Anthropic breakpoint.
    let _env_lock = isolate_prompt_safe_env_cache();
    let session = Session::new("session-anchor", "test-model");
    let mut config = test_config("BASE_IDENTITY");
    config.mcp_tool_guidance = Some("NOVA_GUIDANCE_MARKER guide".to_string());
    let prepared_context = PreparedContext {
        messages: vec![Message::system("BASE_IDENTITY"), Message::user("go")],
        token_usage: usage(0, 24),
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };

    let envelope = super::build_request_envelope(&session, &prepared_context, &config, &[]);
    let blocks = &envelope.ir.system_blocks;
    assert!(
        !blocks.is_empty(),
        "tool guide present → structured system blocks"
    );
    let anchor_indices: Vec<usize> = blocks
        .iter()
        .enumerate()
        .filter(|(_, block)| block.cache_anchor)
        .map(|(index, _)| index)
        .collect();
    assert_eq!(
        anchor_indices,
        vec![blocks.len() - 1],
        "exactly one cache_anchor, on the LAST system block (where Anthropic places cache_control)"
    );
}

#[test]
fn ir_responses_view_orders_guide_skill_conversation_and_lifts_system_to_instructions() {
    // GOLDEN: the adapter-derived Responses view (`PromptIR::responses_request_options`)
    // is the canonical Responses wire — the tool guide leads the input array, the
    // relocated skill context follows, the conversation comes after, and the stable
    // system field is lifted to top-level `instructions` (NOT a leading system
    // message in the array).
    let _env_lock = isolate_prompt_safe_env_cache();
    let mut session = Session::new("session-ir-resp", "test-model");
    session.add_message(Message::system("PERSISTED OPERATOR NOTE"));
    session.metadata.insert(
        "skill.context".to_string(),
        "SKILL_CONTEXT_MARKER body".to_string(),
    );
    let mut config = test_config("BASE_IDENTITY");
    config.mcp_tool_guidance = Some("NOVA_GUIDANCE_MARKER guide".to_string());
    let prepared_context = PreparedContext {
        messages: vec![
            Message::system("PERSISTED OPERATOR NOTE"),
            Message::user("u1"),
            Message::assistant("a1", None),
            Message::user("u2"),
        ],
        token_usage: usage(0, 24),
        truncation_occurred: false,
        segments_removed: 0,
        compressed_message_ids: Vec::new(),
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
    };

    let envelope = super::build_request_envelope(&session, &prepared_context, &config, &[]);
    // responses_input() == body_chat() (system rides instructions, not the array).
    let input = message_shape(&envelope.ir.responses_input());
    let pos = |needle: &str| input.iter().position(|(_, c)| c.contains(needle));
    let guide = pos("NOVA_GUIDANCE_MARKER").expect("tool guide present");
    let skill = pos("SKILL_CONTEXT_MARKER").expect("relocated skill context present");
    let conversation = pos("u1").expect("conversation present");
    assert!(
        guide < skill && skill < conversation,
        "guide leads, then relocated skill context, then the conversation"
    );
    assert!(
        !matches!(input.first(), Some((Role::System, _))),
        "the stable system rides instructions, so the input array does not LEAD with a system message"
    );

    // The adapter lifts the stable system to top-level instructions. Asserted by
    // CONTENT (not by re-deriving from system_field, which would be tautological):
    // instructions carries the base identity but NOT the relocated guide/skill —
    // proving the guide/skill left the system and ride the input array instead.
    let responses = envelope.ir.responses_request_options(None);
    let instructions = responses.instructions.expect("instructions lifted");
    assert!(
        instructions.contains("BASE_IDENTITY"),
        "instructions carries the stable base identity"
    );
    assert!(
        !instructions.contains("NOVA_GUIDANCE_MARKER")
            && !instructions.contains("SKILL_CONTEXT_MARKER"),
        "the relocated guide + skill are NOT in instructions (they ride the input array)"
    );
    assert!(
        instructions == instructions.trim(),
        "instructions are trimmed (byte-faithful to build_responses_body)"
    );
    // The adapter wires the responses_input view as the Responses input array.
    let wired = responses
        .input_messages
        .expect("input_messages derived")
        .iter()
        .any(|m| m.content.contains("NOVA_GUIDANCE_MARKER"));
    assert!(
        wired,
        "input_messages carries the relocated guide (the responses_input view)"
    );
}

#[tokio::test]
async fn execute_llm_stream_ignores_previous_response_id_under_stateless_store_policy() {
    // The engine's Responses policy is stateless (`store=false`): the upstream
    // never persists a turn, so a chained `previous_response_id` is guaranteed to
    // fail with `previous_response_not_found`. A session that still carries a
    // stale id (persisted by a pre-fix build) must be sent as a FULL request with
    // NO continuation, the stale id must be scrubbed from metadata, and the new
    // response id must NOT be persisted for the next round.
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
    // FULL request (no continuation delta): system field + whole conversation.
    assert_eq!(requested_messages.len(), 4);
    assert!(matches!(requested_messages[0].role, Role::System));
    assert!(matches!(requested_messages[3].role, Role::Tool));
    // The stale id is NOT sent to the provider.
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
        Some(expected_system_field("system").as_str())
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
    // The stream still surfaces the upstream response id, but under a stateless
    // store policy the engine neither keeps the stale id nor persists the new
    // one — the metadata key is scrubbed entirely.
    assert_eq!(stream_output.response_id.as_deref(), Some("resp_next"));
    assert!(!session
        .metadata
        .contains_key("responses.previous_response_id"));
}

#[test]
fn responses_continuation_enabled_requires_store_true_and_supported_provider() {
    use bamboo_llm::provider::ResponsesRequestOptions;

    // The engine's shipped policy is stateless — this is the invariant that
    // guarantees no `previous_response_id` ever rides a `store=false` turn
    // (OpenAI rejects that with 400 `previous_response_not_found`).
    let shipped = super::engine_responses_policy();
    assert_eq!(shipped.store, Some(false));
    assert!(!super::responses_continuation_enabled(
        &shipped,
        Some("openai")
    ));

    // A stateful (store=true) policy re-enables chaining for providers that
    // support the parameter…
    let stateful = ResponsesRequestOptions {
        store: Some(true),
        ..Default::default()
    };
    assert!(super::responses_continuation_enabled(
        &stateful,
        Some("openai")
    ));
    assert!(super::responses_continuation_enabled(&stateful, None));
    // …but never for Copilot, whose HTTP /responses endpoint rejects it.
    assert!(!super::responses_continuation_enabled(
        &stateful,
        Some("copilot")
    ));

    // store unset defaults to the stateless wire value (false) → disabled.
    let unset = ResponsesRequestOptions::default();
    assert!(!super::responses_continuation_enabled(
        &unset,
        Some("openai")
    ));
}

#[tokio::test]
async fn execute_llm_stream_includes_external_memory_volatile_block() {
    let _env_lock = isolate_prompt_safe_env_cache();
    let mut session = Session::new("session-stream-2a", "test-model");
    // A stale continuation id may still be present (pre-fix persistence); under
    // the stateless store policy it is ignored and the request is sent in full.
    session.metadata.insert(
        "responses.previous_response_id".to_string(),
        "resp_prev".to_string(),
    );
    // External memory rides a session field now (the async refresh populates it),
    // not a system-message marker.
    session.metadata.insert(
        crate::runtime::runner::prompt_context::EXTERNAL_MEMORY_RENDERED_KEY.to_string(),
        "## External Memory (Persistent)\n\nSession note body".to_string(),
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
    // First epoch: external memory is seeded before the real transcript. Later
    // revisions append after the then-current transcript boundary.
    assert_eq!(requested_messages.len(), 5);
    assert!(matches!(requested_messages[0].role, Role::System));
    assert!(matches!(requested_messages[1].role, Role::User));
    assert!(requested_messages[1]
        .content
        .contains("context_type: external_memory"));
    assert!(requested_messages[1].content.contains("Session note body"));
    assert!(matches!(requested_messages[4].role, Role::Tool));
    // The stale continuation id was ignored, not forwarded.
    assert_eq!(
        llm.requested_previous_response_id
            .lock()
            .expect("previous_response_id lock")
            .as_deref(),
        None
    );
}

#[tokio::test]
async fn execute_llm_stream_includes_plan_mode_and_runtime_volatile_blocks() {
    let _env_lock = isolate_prompt_safe_env_cache();
    let mut session = Session::new("session-stream-2plan", "test-model");
    // Stale continuation id: ignored under the stateless store policy.
    session.metadata.insert(
        "responses.previous_response_id".to_string(),
        "resp_prev".to_string(),
    );
    // Plan mode active in session STATE — the plan_runtime/plan_mode volatile
    // blocks are now built directly from this, not reparsed from system markers.
    {
        use bamboo_domain::session::runtime_state::{
            AgentRuntimeState, PlanModeState, PlanModeStatus,
        };
        session.agent_runtime_state = Some(AgentRuntimeState::new("run-1"));
        session.agent_runtime_state.as_mut().unwrap().plan_mode = Some(PlanModeState {
            entered_at: chrono::Utc::now(),
            pre_permission_mode: "default".to_string(),
            plan_file_path: None,
            status: PlanModeStatus::Exploring,
        });
    }

    let (event_tx, _event_rx) = mpsc::channel::<AgentEvent>(16);
    let system_prompt = "system";
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
    // Initial snapshots are ordered deterministically by typed context kind,
    // followed by the unchanged real transcript.
    assert_eq!(requested_messages.len(), 6);
    assert!(matches!(requested_messages[0].role, Role::System));
    assert!(matches!(requested_messages[1].role, Role::User));
    assert!(requested_messages[1]
        .content
        .contains("context_type: plan_mode_state"));
    assert!(requested_messages[1].content.contains("PLAN MODE ACTIVE"));
    assert!(matches!(requested_messages[2].role, Role::User));
    assert!(requested_messages[2]
        .content
        .contains("context_type: plan_runtime_state"));
    assert!(requested_messages[2]
        .content
        .contains("DURABLE PLAN EXECUTION CONTEXT"));
    assert!(matches!(requested_messages[5].role, Role::Tool));
}

#[tokio::test]
async fn execute_llm_stream_sends_full_request_with_summary_when_compression_is_active() {
    let _env_lock = isolate_prompt_safe_env_cache();
    let mut session = Session::new("session-stream-2b", "test-model");
    // Stale continuation id: ignored under the stateless store policy — the
    // request rides the full lanes wire with the summary as dynamic context.
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
    // Full request: system field, the summary as dynamic context, then the whole
    // (compressed) conversation window.
    assert_eq!(requested_messages.len(), 6);
    assert!(matches!(requested_messages[0].role, Role::System));
    assert!(matches!(requested_messages[1].role, Role::User));
    assert!(requested_messages[1]
        .content
        .contains("context_type: conversation_summary"));
    assert!(requested_messages[1]
        .content
        .contains("Older work has been summarized locally."));
    assert!(matches!(requested_messages[2].role, Role::User));
    assert_eq!(
        requested_messages[2].content,
        "previous work was compressed"
    );
    assert!(matches!(requested_messages[5].role, Role::Tool));
    // The stale continuation id was ignored, not forwarded.
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
        Some(expected_system_field("system").as_str())
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
        Some(expected_system_field("system").as_str())
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
        Some(expected_system_field("system").as_str())
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
