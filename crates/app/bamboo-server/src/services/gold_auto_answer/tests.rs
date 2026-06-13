//! Server-side full-loop integration tests for Gold auto-answer.
//!
//! These construct a real `AppState` plus the `AppStateResumeRef` resume
//! adapter, so they live in the server crate. Pure decision-helper unit tests
//! live alongside the service in `bamboo_engine::gold_auto_answer`.

use crate::app_state::resume_adapter::AppStateResumeRef;
use crate::app_state::{AgentStatus, AppState};
use actix_web::web::Data;
use async_trait::async_trait;
use bamboo_agent_core::Session;
use bamboo_agent_core::{
    AgentEvent, FunctionCall, Message, PendingQuestionSource, ToolCall, ToolSchema,
};
use bamboo_engine::config::GoldConfig;
use bamboo_engine::gold_auto_answer::{maybe_auto_answer_pending_question, GoldAutoAnswerOutcome};
use bamboo_engine::session_app::execute::has_pending_clarification_resume;
use bamboo_engine::session_app::types::ResumeOutcome;
use bamboo_llm::{
    Config, LLMChunk, LLMError, LLMProvider, LLMRequestOptions, LLMStream, ProviderModelRouter,
    ProviderRegistry,
};
use futures::stream;
use serde_json::json;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, Semaphore};
use tokio::time::{sleep, timeout, Duration};

fn auto_answer_call(arguments: serde_json::Value) -> ToolCall {
    ToolCall {
        id: "call-1".to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "report_gold_auto_answer".to_string(),
            arguments: arguments.to_string(),
        },
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordedRequest {
    purpose: String,
    model: String,
}

struct ScriptedProvider {
    auto_answer: String,
    requests: Mutex<Vec<RecordedRequest>>,
    agent_loop_gate: Arc<Semaphore>,
}

impl ScriptedProvider {
    fn new(auto_answer: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            auto_answer: auto_answer.into(),
            requests: Mutex::new(Vec::new()),
            agent_loop_gate: Arc::new(Semaphore::new(0)),
        })
    }

    fn request_purposes(&self) -> Vec<String> {
        self.requests
            .lock()
            .expect("requests lock")
            .iter()
            .map(|request| request.purpose.clone())
            .collect()
    }

    fn release_agent_loop(&self) {
        self.agent_loop_gate.add_permits(1);
    }
}

#[async_trait]
impl LLMProvider for ScriptedProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        panic!("chat_stream should not be called directly in this test")
    }

    async fn chat_stream_with_options(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> Result<LLMStream, LLMError> {
        let purpose = options
            .and_then(|value| value.request_purpose.clone())
            .unwrap_or_else(|| "unknown".to_string());
        self.requests
            .lock()
            .expect("requests lock")
            .push(RecordedRequest {
                purpose: purpose.clone(),
                model: model.to_string(),
            });

        match purpose.as_str() {
            "gold_evaluation" => Ok(Box::pin(stream::iter(vec![
                Ok(LLMChunk::ToolCalls(vec![gold_evaluation_call(json!({
                    "decision": "continue",
                    "confidence": "high",
                    "reasoning": "The clarification can be answered safely and execution should continue."
                }))])),
                Ok(LLMChunk::Done),
            ]))),
            "gold_auto_answer" => Ok(Box::pin(stream::iter(vec![
                Ok(LLMChunk::ToolCalls(vec![auto_answer_call(json!({
                    "apply": true,
                    "answer": self.auto_answer,
                    "confidence": "high",
                    "reasoning": "The answer is an exact low-risk option already supported by the session context."
                }))])),
                Ok(LLMChunk::Done),
            ]))),
            "agent_loop" => {
                let gate = self.agent_loop_gate.clone();
                Ok(Box::pin(async_stream::stream! {
                    let _permit = gate.acquire().await.expect("agent loop gate should stay open");
                    yield Ok::<LLMChunk, LLMError>(LLMChunk::Token("done".to_string()));
                    yield Ok::<LLMChunk, LLMError>(LLMChunk::Done);
                }))
            }
            other => Err(LLMError::Api(format!(
                "unexpected request_purpose in ScriptedProvider: {other}"
            ))),
        }
    }
}

fn gold_evaluation_call(arguments: serde_json::Value) -> ToolCall {
    ToolCall {
        id: "gold-evaluation-call-1".to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "report_gold_evaluation".to_string(),
            arguments: arguments.to_string(),
        },
    }
}

fn test_gold_config() -> GoldConfig {
    GoldConfig {
        enabled: true,
        auto_answer_enabled: true,
        auto_continue_enabled: false,
        model_name: Some("test-model".to_string()),
        max_output_tokens: 256,
        max_auto_continuations: 3,
        ..GoldConfig::default()
    }
}

fn awaiting_clarification_state(
    run_id: &str,
) -> bamboo_domain::session::runtime_state::AgentRuntimeState {
    use bamboo_domain::session::runtime_state::{
        AgentRuntimeState, AgentStatusState, SuspensionState,
    };
    let mut runtime_state = AgentRuntimeState::new(run_id.to_string());
    runtime_state.status = AgentStatusState::Suspended;
    runtime_state.round.current_round = 3;
    runtime_state.round.last_round_id = Some("round-3".to_string());
    runtime_state.suspension = Some(SuspensionState {
        reason: "awaiting_clarification".to_string(),
        suspended_at: chrono::Utc::now(),
        resumable: true,
        hook_point: Some("AfterToolExecution".to_string()),
    });
    runtime_state
}

fn build_pending_session(
    session_id: &str,
    tool_call_id: &str,
    tool_name: &str,
    question: &str,
    options: &[&str],
    allow_custom: bool,
    tool_result_payload: serde_json::Value,
) -> Session {
    let mut session = Session::new(session_id, "test-model");
    session.add_message(Message::user(
        "Please continue once the clarification has been resolved.",
    ));
    session.add_message(Message::assistant(
        "I need a clarification before I can continue.",
        None,
    ));
    session.add_message(Message::tool_result_with_status(
        tool_call_id.to_string(),
        tool_result_payload.to_string(),
        true,
    ));
    session.set_pending_question_with_source(
        tool_call_id.to_string(),
        tool_name.to_string(),
        question.to_string(),
        options.iter().map(|option| option.to_string()).collect(),
        allow_custom,
        PendingQuestionSource::PauseTool,
    );
    session.metadata.insert(
        "runtime.suspend_reason".to_string(),
        "awaiting_clarification".to_string(),
    );
    session.agent_runtime_state = Some(awaiting_clarification_state("run-awaiting-clarification"));
    session
}

#[tokio::test]
async fn gold_auto_answer_conclusion_with_options_full_loop() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let mut config = Config::from_data_dir(Some(temp_dir.path().to_path_buf()));
    config.provider = String::new();
    config.features.provider_model_ref = true;
    let provider = ScriptedProvider::new("ok.");
    let provider_trait: Arc<dyn LLMProvider> = provider.clone();
    let mut app_state =
        AppState::new_with_provider(temp_dir.path().to_path_buf(), config, provider_trait)
            .await
            .expect("app state");
    app_state.provider_registry = Arc::new(ProviderRegistry::new(HashMap::new(), String::new()));
    app_state.provider_router = Arc::new(ProviderModelRouter::new(
        app_state.provider_registry.clone(),
    ));
    let state = Data::new(app_state);

    let session_id = "gold-auto-answer-conclusion-full-loop";
    let tool_call_id = "call-conclusion";
    let mut session = build_pending_session(
        session_id,
        tool_call_id,
        "conclusion_with_options",
        "Any other requests before I finish?",
        &["OK", "Need changes"],
        true,
        json!({
            "summary": "Core validation is complete and release is ready."
        }),
    );
    state.save_and_cache_session(&mut session).await;

    let resume_port = AppStateResumeRef(state.clone());
    let outcome = maybe_auto_answer_pending_question(
        state.get_ref(),
        &resume_port,
        session_id,
        Some(test_gold_config()),
    )
    .await;

    let run_id = match &outcome {
        GoldAutoAnswerOutcome::Applied {
            answer,
            resume_outcome,
        } => {
            assert_eq!(answer, "OK");
            match resume_outcome {
                ResumeOutcome::Started { run_id } => run_id.clone(),
                other => panic!("expected resume to start, got {other:?}"),
            }
        }
        other => panic!("expected Gold auto-answer to apply, got {other:?}"),
    };

    let resumed_status = wait_for_resume_activity(state.as_ref(), session_id).await;
    wait_for_provider_purpose(provider.as_ref(), "agent_loop").await;
    assert!(matches!(
        resumed_status,
        AgentStatus::Running | AgentStatus::Completed
    ));

    let after_respond = state
        .load_session_merged(session_id)
        .await
        .expect("session should exist after respond");
    assert!(after_respond.pending_question.is_none());
    assert!(after_respond.messages.iter().any(|message| {
        message.tool_call_id.as_deref() == Some(tool_call_id)
            && message.content == "Auto-selected response (gold): OK"
            && message.tool_success == Some(true)
    }));
    assert!(!has_pending_clarification_resume(&after_respond));

    {
        let runners = state.agent_runners.read().await;
        let runner = runners
            .get(session_id)
            .expect("runner should exist after auto-resume start");
        assert_eq!(runner.run_id, run_id);
        assert!(matches!(
            runner.status,
            AgentStatus::Running | AgentStatus::Completed
        ));
    }

    assert_eq!(
        provider.request_purposes(),
        vec!["gold_evaluation", "gold_auto_answer", "agent_loop"]
    );

    provider.release_agent_loop();
    sleep(Duration::from_millis(50)).await;
}

#[tokio::test]
async fn gold_auto_answer_exit_plan_mode_full_loop() {
    use bamboo_domain::session::runtime_state::{PlanModeState, PlanModeStatus};

    let temp_dir = tempfile::tempdir().expect("temp dir");
    let mut config = Config::from_data_dir(Some(temp_dir.path().to_path_buf()));
    config.provider = String::new();
    config.features.provider_model_ref = true;
    let provider = ScriptedProvider::new("Approve (Default mode)");
    let provider_trait: Arc<dyn LLMProvider> = provider.clone();
    let mut app_state =
        AppState::new_with_provider(temp_dir.path().to_path_buf(), config, provider_trait)
            .await
            .expect("app state");
    app_state.provider_registry = Arc::new(ProviderRegistry::new(HashMap::new(), String::new()));
    app_state.provider_router = Arc::new(ProviderModelRouter::new(
        app_state.provider_registry.clone(),
    ));
    let state = Data::new(app_state);

    let session_id = "gold-auto-answer-exit-plan-mode-full-loop";
    let tool_call_id = "call-exit-plan-mode";
    let mut session = build_pending_session(
        session_id,
        tool_call_id,
        "ExitPlanMode",
        "Approve leaving plan mode?",
        &["Approve (Default mode)", "Stay in plan mode"],
        false,
        json!({ "plan": "1. Inspect the codebase\n2. Write the implementation plan" }),
    );
    if let Some(runtime_state) = session.agent_runtime_state.as_mut() {
        runtime_state.plan_mode = Some(PlanModeState {
            entered_at: chrono::Utc::now(),
            pre_permission_mode: "default".to_string(),
            plan_file_path: Some("/tmp/test-plan.md".to_string()),
            status: PlanModeStatus::AwaitingApproval,
        });
    }
    state.save_and_cache_session(&mut session).await;
    let mut event_rx = state.get_session_event_sender(session_id).await.subscribe();

    let resume_port = AppStateResumeRef(state.clone());
    let outcome = maybe_auto_answer_pending_question(
        state.get_ref(),
        &resume_port,
        session_id,
        Some(test_gold_config()),
    )
    .await;
    let run_id = match &outcome {
        GoldAutoAnswerOutcome::Applied {
            answer,
            resume_outcome,
        } => {
            assert_eq!(answer, "Approve (Default mode)");
            match resume_outcome {
                ResumeOutcome::Started { run_id } => run_id.clone(),
                other => panic!("expected resume to start, got {other:?}"),
            }
        }
        other => panic!("expected Gold auto-answer to apply, got {other:?}"),
    };

    let resumed_status = wait_for_resume_activity(state.as_ref(), session_id).await;
    wait_for_provider_purpose(provider.as_ref(), "agent_loop").await;
    assert!(matches!(
        resumed_status,
        AgentStatus::Running | AgentStatus::Completed
    ));

    timeout(Duration::from_secs(5), async {
        loop {
            match event_rx.recv().await {
                Ok(AgentEvent::PlanModeExited {
                    session_id: event_session_id,
                    approved,
                    restored_mode,
                    plan,
                }) if event_session_id == session_id => {
                    assert!(approved);
                    assert_eq!(restored_mode, "default");
                    assert_eq!(
                        plan.as_deref(),
                        Some("1. Inspect the codebase\n2. Write the implementation plan")
                    );
                    break;
                }
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(error) => panic!("failed to receive session event: {error}"),
            }
        }
    })
    .await
    .expect("expected PlanModeExited event");

    let after_respond = state
        .load_session_merged(session_id)
        .await
        .expect("session should exist after respond");
    assert!(after_respond.pending_question.is_none());
    assert!(after_respond.messages.iter().any(|message| {
        message.tool_call_id.as_deref() == Some(tool_call_id)
            && message.content == "Auto-selected response (gold): Approve (Default mode)"
            && message.tool_success == Some(true)
    }));
    assert!(!has_pending_clarification_resume(&after_respond));
    assert!(after_respond
        .agent_runtime_state
        .as_ref()
        .and_then(|runtime_state| runtime_state.plan_mode.as_ref())
        .is_none());

    {
        let runners = state.agent_runners.read().await;
        let runner = runners
            .get(session_id)
            .expect("runner should exist after auto-resume start");
        assert_eq!(runner.run_id, run_id);
        assert!(matches!(
            runner.status,
            AgentStatus::Running | AgentStatus::Completed
        ));
    }

    assert_eq!(
        provider.request_purposes(),
        vec!["gold_evaluation", "gold_auto_answer", "agent_loop"]
    );
    provider.release_agent_loop();
    sleep(Duration::from_millis(50)).await;
}

async fn wait_for_resume_activity(state: &AppState, session_id: &str) -> AgentStatus {
    timeout(Duration::from_secs(5), async {
        loop {
            let marker_consumed = state
                .load_session_merged(session_id)
                .await
                .is_some_and(|session| !has_pending_clarification_resume(&session));
            let runner_status = {
                let runners = state.agent_runners.read().await;
                runners.get(session_id).map(|runner| runner.status.clone())
            };
            if marker_consumed {
                if let Some(status @ AgentStatus::Running) = runner_status.clone() {
                    return status;
                }
                if let Some(status @ AgentStatus::Completed) = runner_status {
                    return status;
                }
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("resume activity should appear")
}

async fn wait_for_provider_purpose(provider: &ScriptedProvider, purpose: &str) {
    timeout(Duration::from_secs(5), async {
        loop {
            if provider
                .request_purposes()
                .iter()
                .any(|value| value == purpose)
            {
                return;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("provider purpose should appear");
}
