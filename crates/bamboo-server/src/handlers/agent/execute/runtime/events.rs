use tokio::sync::mpsc;
use tracing::Instrument;

use crate::app_state::AppState;
use crate::services::gold_auto_answer::{
    maybe_auto_answer_pending_question, GoldAutoAnswerOutcome,
};
use bamboo_agent_core::AgentEvent;
use bamboo_engine::config::GoldConfig;

/// Returns true for events that carry critical state a late subscriber must see.
///
/// These are cached on the runner and replayed when an SSE client connects
/// after the live stream has already started.
fn is_critical_event(event: &AgentEvent) -> bool {
    matches!(
        event,
        AgentEvent::TaskListUpdated { .. }
            | AgentEvent::TaskListCompleted { .. }
            | AgentEvent::SubAgentStarted { .. }
            | AgentEvent::SubAgentCompleted { .. }
            | AgentEvent::SessionTitleUpdated { .. }
            | AgentEvent::SessionPinnedUpdated { .. }
            | AgentEvent::PlanModeEntered { .. }
            | AgentEvent::PlanModeExited { .. }
    )
}

pub(crate) fn spawn_event_forwarder(
    state: actix_web::web::Data<AppState>,
    session_id: String,
    mut mpsc_rx: mpsc::Receiver<AgentEvent>,
    session_tx: tokio::sync::broadcast::Sender<AgentEvent>,
    gold_config: Option<GoldConfig>,
) {
    let span_session_id = session_id.clone();
    let session_span = tracing::info_span!("event_forwarder", session_id = %span_session_id);

    tokio::spawn(
        async move {
            while let Some(event) = mpsc_rx.recv().await {
                // Cache critical events for late-subscriber replay.
                if is_critical_event(&event) {
                    let mut runners = state.agent_runners.write().await;
                    if let Some(runner) = runners.get_mut(&session_id) {
                        runner.push_critical_event(event.clone());
                        tracing::trace!(
                            "[{}] Cached critical event for late subscribers",
                            session_id
                        );
                    }
                }

                // Store budget events for late subscribers.
                if matches!(&event, AgentEvent::TokenBudgetUpdated { .. }) {
                    let mut runners = state.agent_runners.write().await;
                    if let Some(runner) = runners.get_mut(&session_id) {
                        runner.last_budget_event = Some(event.clone());
                        // Fires once per agent round — far too hot for debug.
                        tracing::trace!(
                            "[{}] Stored budget event for late subscribers",
                            session_id
                        );
                    }
                }

                // Mirror durable change events onto the account-wide feed
                // (sequenced + journaled) for resumable multi-client sync. The
                // sink filters ephemeral events (tokens, etc.) internally, so
                // this is near-free for the hot path. `session_id` is passed
                // explicitly so terminal events (which carry no id) still route.
                state.account_sink.record(Some(&session_id), &event);

                // Always forward to the broadcast channel regardless of subscriber count.
                // The broadcast channel's internal capacity (1000 slots) buffers events so
                // that late or temporarily-disconnected subscribers can catch up when they
                // resubscribe.  Critical events are *also* cached on the runner above, so
                // even if the broadcast ring wraps, a fresh subscriber will still receive
                // the latest state via the replay path.
                let _ = session_tx.send(event);
            }

            // Gold auto-answer responds to a pending clarification that genuinely
            // paused the run (the session is Suspended, not Completed), so it stays
            // here in the post-stop path. Autonomous goal continuation, by contrast,
            // now happens INSIDE the runner loop's terminal gate (see
            // bamboo-engine `evaluate_gold_terminal`) so the run emits a single
            // terminal `Complete` and the frontend never gets stuck mid-settle.
            let resume_port =
                crate::app_state::resume_adapter::AppStateResumeRef(state.clone());
            let auto_answer_outcome = maybe_auto_answer_pending_question(
                state.get_ref(),
                &resume_port,
                &session_id,
                gold_config.clone(),
            )
            .await;
            match auto_answer_outcome {
                GoldAutoAnswerOutcome::Skipped { reason } => {
                    tracing::debug!(
                        session_id = %session_id,
                        reason = %reason,
                        "Gold auto-answer skipped after event forwarder completion"
                    );
                }
                GoldAutoAnswerOutcome::Applied {
                    answer,
                    resume_outcome,
                } => {
                    tracing::info!(
                        session_id = %session_id,
                        answer = %answer,
                        resume_status = %resume_outcome.status_str(),
                        "Gold auto-answer applied after event forwarder completion"
                    );
                }
            }

            tracing::debug!("[{}] Event forwarder finished", session_id);
        }
        .instrument(session_span),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use bamboo_agent_core::AgentEvent;
    use bamboo_domain::session::runtime_state::{
        AgentRuntimeState, AgentStatusState, SuspensionState,
    };
    use bamboo_domain::{TaskItem, TaskItemStatus, TaskList};
    use bamboo_infrastructure::{
        Config, LLMChunk, LLMError, LLMProvider, LLMRequestOptions, LLMStream, ProviderModelRouter,
        ProviderRegistry,
    };
    use chrono::Utc;
    use futures::stream;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tokio::sync::Semaphore;
    use tokio::time::{sleep, timeout, Duration};

    use crate::app_state::AgentStatus;
    use crate::session_app::execute::has_pending_clarification_resume;

    /// Helper: create a minimal `TaskListUpdated` event for testing.
    fn task_list_updated() -> AgentEvent {
        AgentEvent::TaskListUpdated {
            task_list: TaskList {
                session_id: "test-session".to_string(),
                title: "Test".to_string(),
                items: vec![TaskItem {
                    id: "t1".into(),
                    description: "Do something".into(),
                    status: TaskItemStatus::InProgress,
                    ..TaskItem::default()
                }],
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
        }
    }

    fn sub_agent_started() -> AgentEvent {
        AgentEvent::SubAgentStarted {
            parent_session_id: "parent".into(),
            child_session_id: "child-1".into(),
            title: Some("child work".into()),
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
            _messages: &[bamboo_agent_core::Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            panic!("chat_stream should not be called directly in this test")
        }

        async fn chat_stream_with_options(
            &self,
            _messages: &[bamboo_agent_core::Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
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

    fn auto_answer_call(arguments: serde_json::Value) -> bamboo_agent_core::ToolCall {
        bamboo_agent_core::ToolCall {
            id: "call-1".to_string(),
            tool_type: "function".to_string(),
            function: bamboo_agent_core::FunctionCall {
                name: "report_gold_auto_answer".to_string(),
                arguments: arguments.to_string(),
            },
        }
    }

    fn gold_evaluation_call(arguments: serde_json::Value) -> bamboo_agent_core::ToolCall {
        bamboo_agent_core::ToolCall {
            id: "gold-evaluation-call-1".to_string(),
            tool_type: "function".to_string(),
            function: bamboo_agent_core::FunctionCall {
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

    fn awaiting_clarification_state(run_id: &str) -> AgentRuntimeState {
        let mut runtime_state = AgentRuntimeState::new(run_id.to_string());
        runtime_state.status = AgentStatusState::Suspended;
        runtime_state.round.current_round = 3;
        runtime_state.round.last_round_id = Some("round-3".to_string());
        runtime_state.suspension = Some(SuspensionState {
            reason: "awaiting_clarification".to_string(),
            suspended_at: Utc::now(),
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
    ) -> bamboo_agent_core::Session {
        let mut session = bamboo_agent_core::Session::new(session_id, "test-model");
        session.add_message(bamboo_agent_core::Message::user(
            "Please continue once the clarification has been resolved.",
        ));
        session.add_message(bamboo_agent_core::Message::assistant(
            "I need a clarification before I can continue.",
            None,
        ));
        session.add_message(bamboo_agent_core::Message::tool_result_with_status(
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
            bamboo_agent_core::PendingQuestionSource::PauseTool,
        );
        session.metadata.insert(
            "runtime.suspend_reason".to_string(),
            "awaiting_clarification".to_string(),
        );
        session.agent_runtime_state =
            Some(awaiting_clarification_state("run-awaiting-clarification"));
        session
    }

    async fn wait_for_resume_activity(state: &AppState, session_id: &str) -> AgentStatus {
        // Generous timeout: this polls for an async gold resume that races
        // through several LLM round-trips. Under the full parallel test suite
        // (16 threads), a tight 5s budget can starve and flake; 30s only
        // affects the genuine-hang detection ceiling, not the happy path.
        timeout(Duration::from_secs(30), async {
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
        // See `wait_for_resume_activity`: generous ceiling to avoid flaking
        // under full-suite scheduling pressure.
        timeout(Duration::from_secs(30), async {
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

    // ── is_critical_event ──────────────────────────────────────────────

    #[test]
    fn critical_event_identifies_task_list_updated() {
        assert!(is_critical_event(&task_list_updated()));
    }

    #[test]
    fn critical_event_identifies_task_list_completed() {
        let event = AgentEvent::TaskListCompleted {
            session_id: "s1".into(),
            completed_at: Utc::now(),
            total_rounds: 3,
            total_tool_calls: 10,
        };
        assert!(is_critical_event(&event));
    }

    #[test]
    fn critical_event_identifies_sub_agent_started() {
        assert!(is_critical_event(&sub_agent_started()));
    }

    #[test]
    fn critical_event_identifies_sub_agent_completed() {
        let event = AgentEvent::SubAgentCompleted {
            parent_session_id: "parent".into(),
            child_session_id: "child-1".into(),
            status: "completed".into(),
            error: None,
        };
        assert!(is_critical_event(&event));
    }

    #[test]
    fn critical_event_includes_session_title_updated() {
        use bamboo_agent_core::TitleSource;
        use chrono::Utc;
        let event = AgentEvent::SessionTitleUpdated {
            session_id: "s".to_string(),
            title: "t".to_string(),
            title_version: 1,
            source: TitleSource::Manual,
            updated_at: Utc::now(),
        };
        assert!(is_critical_event(&event));
    }

    #[test]
    fn critical_event_includes_session_pinned_updated() {
        let event = AgentEvent::SessionPinnedUpdated {
            session_id: "s".to_string(),
            pinned: true,
            updated_at: Utc::now(),
        };
        assert!(is_critical_event(&event));
    }

    #[test]
    fn non_critical_events_are_not_flagged() {
        // Token events are NOT critical.
        assert!(!is_critical_event(&AgentEvent::Token {
            content: "hello".into(),
        }));
        // ToolStart is NOT critical.
        assert!(!is_critical_event(&AgentEvent::ToolStart {
            tool_call_id: "tc-1".into(),
            tool_name: "Bash".into(),
            arguments: serde_json::json!(null),
        }));
        // Complete is NOT critical (terminal event is handled separately).
        assert!(!is_critical_event(&AgentEvent::Complete {
            usage: bamboo_domain::TokenUsage {
                prompt_tokens: 0,
                completion_tokens: 0,
                total_tokens: 0,
            },
        }));
    }

    // ── Event forwarder integration ────────────────────────────────────

    #[tokio::test]
    async fn event_forwarder_sends_events_even_with_zero_subscribers() {
        let temp_dir = tempfile::tempdir().unwrap();
        let state = AppState::new(temp_dir.path().to_path_buf())
            .await
            .expect("app state should initialize");
        let state = actix_web::web::Data::new(state);

        let session_id = "test-no-subs";

        // Register a runner so the forwarder can cache critical events.
        {
            use bamboo_engine::runtime::execution::runner_state::AgentRunner;
            let mut runner = AgentRunner::new();
            runner.status = bamboo_engine::runtime::execution::runner_state::AgentStatus::Running;
            state
                .agent_runners
                .write()
                .await
                .insert(session_id.to_string(), runner);
        }

        let (mpsc_tx, mpsc_rx) = mpsc::channel::<AgentEvent>(64);
        let (session_tx, _session_rx) = tokio::sync::broadcast::channel::<AgentEvent>(1000);
        // NOTE: _session_rx is dropped immediately — zero subscribers.

        spawn_event_forwarder(
            state.clone(),
            session_id.to_string(),
            mpsc_rx,
            session_tx.clone(),
            None,
        );

        // Send a critical event while there are zero broadcast subscribers.
        mpsc_tx.send(task_list_updated()).await.unwrap();
        mpsc_tx
            .send(AgentEvent::Token {
                content: "hi".into(),
            })
            .await
            .unwrap();

        // Give the forwarder task a chance to process.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Now subscribe to the broadcast channel and verify events are available.
        // Because we always send (no subscriber-count gate), the events went into
        // the broadcast buffer. A late subscriber won't see them via broadcast
        // (broadcast only replays to active receivers), but the critical event
        // cache should be populated.
        let runners = state.agent_runners.read().await;
        let runner = runners.get(session_id).expect("runner should exist");
        assert_eq!(
            runner.last_critical_events.len(),
            1,
            "should have cached exactly one critical event"
        );
        assert!(
            matches!(
                runner.last_critical_events[0],
                AgentEvent::TaskListUpdated { .. }
            ),
            "cached event should be TaskListUpdated"
        );
    }

    #[tokio::test]
    async fn event_forwarder_mirrors_durable_events_to_account_feed() {
        let temp_dir = tempfile::tempdir().unwrap();
        let state = AppState::new(temp_dir.path().to_path_buf())
            .await
            .expect("app state should initialize");
        let state = actix_web::web::Data::new(state);

        let session_id = "test-account-feed";
        {
            use bamboo_engine::runtime::execution::runner_state::AgentRunner;
            let mut runner = AgentRunner::new();
            runner.status = bamboo_engine::runtime::execution::runner_state::AgentStatus::Running;
            state
                .agent_runners
                .write()
                .await
                .insert(session_id.to_string(), runner);
        }

        let (mpsc_tx, mpsc_rx) = mpsc::channel::<AgentEvent>(64);
        let (session_tx, _) = tokio::sync::broadcast::channel::<AgentEvent>(1000);
        spawn_event_forwarder(state.clone(), session_id.to_string(), mpsc_rx, session_tx, None);

        // A durable change event...
        mpsc_tx.send(task_list_updated()).await.unwrap();
        // ...and an ephemeral one that must NOT be journaled.
        mpsc_tx
            .send(AgentEvent::Token {
                content: "noise".into(),
            })
            .await
            .unwrap();
        // A terminal event (carries no session_id of its own) must still route.
        mpsc_tx
            .send(AgentEvent::Complete {
                usage: Default::default(),
            })
            .await
            .unwrap();
        drop(mpsc_tx);

        tokio::time::sleep(std::time::Duration::from_millis(80)).await;

        let journaled =
            crate::events::journal::read_since(state.account_sink.events_dir(), 0).unwrap();
        // Only the two durable events (TaskListUpdated, Complete) were journaled.
        assert_eq!(journaled.len(), 2, "ephemeral Token must be excluded");
        assert!(matches!(
            journaled[0].event,
            AgentEvent::TaskListUpdated { .. }
        ));
        assert!(matches!(journaled[1].event, AgentEvent::Complete { .. }));
        // The terminal event routed to the right session via caller context.
        assert_eq!(journaled[1].session_id.as_deref(), Some(session_id));
        // Sequence numbers are monotonic and 1-based.
        assert_eq!(journaled[0].seq, 1);
        assert_eq!(journaled[1].seq, 2);
    }

    #[tokio::test]
    async fn event_forwarder_caches_multiple_critical_events_in_order() {
        let temp_dir = tempfile::tempdir().unwrap();
        let state = AppState::new(temp_dir.path().to_path_buf())
            .await
            .expect("app state should initialize");
        let state = actix_web::web::Data::new(state);

        let session_id = "test-critical-order";

        {
            use bamboo_engine::runtime::execution::runner_state::AgentRunner;
            let mut runner = AgentRunner::new();
            runner.status = bamboo_engine::runtime::execution::runner_state::AgentStatus::Running;
            state
                .agent_runners
                .write()
                .await
                .insert(session_id.to_string(), runner);
        }

        let (mpsc_tx, mpsc_rx) = mpsc::channel::<AgentEvent>(64);
        let (session_tx, _) = tokio::sync::broadcast::channel::<AgentEvent>(1000);

        spawn_event_forwarder(
            state.clone(),
            session_id.to_string(),
            mpsc_rx,
            session_tx,
            None,
        );

        // Send a critical event, then a non-critical event, then another critical event.
        mpsc_tx.send(task_list_updated()).await.unwrap();
        mpsc_tx
            .send(AgentEvent::Token {
                content: "thinking".into(),
            })
            .await
            .unwrap();
        mpsc_tx.send(sub_agent_started()).await.unwrap();

        // Drop sender to terminate forwarder.
        drop(mpsc_tx);

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let runners = state.agent_runners.read().await;
        let runner = runners.get(session_id).expect("runner should exist");
        assert_eq!(runner.last_critical_events.len(), 2);
        assert!(matches!(
            runner.last_critical_events[0],
            AgentEvent::TaskListUpdated { .. }
        ));
        assert!(matches!(
            runner.last_critical_events[1],
            AgentEvent::SubAgentStarted { .. }
        ));
    }

    #[tokio::test]
    async fn late_subscriber_receives_events_from_broadcast() {
        // Verify that with the new always-send behavior, a subscriber who joins
        // after events were sent can still receive new events live.
        let temp_dir = tempfile::tempdir().unwrap();
        let state = AppState::new(temp_dir.path().to_path_buf())
            .await
            .expect("app state should initialize");
        let state = actix_web::web::Data::new(state);

        let session_id = "test-late-sub";

        {
            use bamboo_engine::runtime::execution::runner_state::AgentRunner;
            let mut runner = AgentRunner::new();
            runner.status = bamboo_engine::runtime::execution::runner_state::AgentStatus::Running;
            state
                .agent_runners
                .write()
                .await
                .insert(session_id.to_string(), runner);
        }

        let (mpsc_tx, mpsc_rx) = mpsc::channel::<AgentEvent>(64);
        let (session_tx, _) = tokio::sync::broadcast::channel::<AgentEvent>(1000);

        spawn_event_forwarder(
            state.clone(),
            session_id.to_string(),
            mpsc_rx,
            session_tx.clone(),
            None,
        );

        // Send events with no subscriber.
        mpsc_tx
            .send(AgentEvent::Token {
                content: "before-sub".into(),
            })
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Now subscribe.
        let mut late_rx = session_tx.subscribe();

        // Send more events.
        mpsc_tx
            .send(AgentEvent::Token {
                content: "after-sub".into(),
            })
            .await
            .unwrap();

        // Late subscriber should receive the event sent after subscription.
        let received = tokio::time::timeout(std::time::Duration::from_millis(200), late_rx.recv())
            .await
            .expect("should receive event before timeout")
            .expect("should not get channel closed");

        match received {
            AgentEvent::Token { content } => assert_eq!(content, "after-sub"),
            other => panic!("unexpected event: {other:?}"),
        }

        // Drop to clean up.
        drop(mpsc_tx);
    }

    #[tokio::test]
    async fn event_forwarder_triggers_gold_auto_answer_when_mpsc_closes() {
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
        app_state.provider_registry =
            Arc::new(ProviderRegistry::new(HashMap::new(), String::new()));
        app_state.provider_router = Arc::new(ProviderModelRouter::new(
            app_state.provider_registry.clone(),
        ));
        let state = actix_web::web::Data::new(app_state);

        let session_id = "test-forwarder-gold-auto-answer";
        let tool_call_id = "call-forwarder-conclusion";
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

        let (mpsc_tx, mpsc_rx) = mpsc::channel::<AgentEvent>(64);
        let (session_tx, _session_rx) = tokio::sync::broadcast::channel::<AgentEvent>(1000);

        spawn_event_forwarder(
            state.clone(),
            session_id.to_string(),
            mpsc_rx,
            session_tx,
            Some(test_gold_config()),
        );

        mpsc_tx
            .send(AgentEvent::Token {
                content: "before-close".into(),
            })
            .await
            .expect("should send token before close");
        drop(mpsc_tx);

        let resumed_status = wait_for_resume_activity(state.as_ref(), session_id).await;
        wait_for_provider_purpose(provider.as_ref(), "agent_loop").await;
        assert!(matches!(
            resumed_status,
            AgentStatus::Running | AgentStatus::Completed
        ));

        let after = state
            .load_session_merged(session_id)
            .await
            .expect("session should exist after forwarder close hook");
        assert!(after.pending_question.is_none());
        assert!(after.messages.iter().any(|message| {
            message.tool_call_id.as_deref() == Some(tool_call_id)
                && message.content == "Auto-selected response (gold): OK"
                && message.tool_success == Some(true)
        }));
        assert!(!has_pending_clarification_resume(&after));
        assert_eq!(
            provider.request_purposes(),
            vec!["gold_evaluation", "gold_auto_answer", "agent_loop"]
        );

        {
            let runners = state.agent_runners.read().await;
            let runner = runners
                .get(session_id)
                .expect("runner should exist after auto-resume start");
            assert!(matches!(
                runner.status,
                AgentStatus::Running | AgentStatus::Completed
            ));
        }

        provider.release_agent_loop();
        sleep(Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn event_forwarder_does_not_auto_continue_after_auto_answer_applies() {
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
        app_state.provider_registry =
            Arc::new(ProviderRegistry::new(HashMap::new(), String::new()));
        app_state.provider_router = Arc::new(ProviderModelRouter::new(
            app_state.provider_registry.clone(),
        ));
        let state = actix_web::web::Data::new(app_state);

        let session_id = "test-forwarder-gold-auto-answer-no-auto-continue";
        let tool_call_id = "call-forwarder-conclusion-no-auto-continue";
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

        let (mpsc_tx, mpsc_rx) = mpsc::channel::<AgentEvent>(64);
        let (session_tx, _session_rx) = tokio::sync::broadcast::channel::<AgentEvent>(1000);

        let mut gold_config = test_gold_config();
        gold_config.auto_answer_enabled = true;
        gold_config.auto_continue_enabled = true;
        gold_config.max_auto_continuations = 3;

        spawn_event_forwarder(
            state.clone(),
            session_id.to_string(),
            mpsc_rx,
            session_tx,
            Some(gold_config),
        );

        mpsc_tx
            .send(AgentEvent::Token {
                content: "before-close".into(),
            })
            .await
            .expect("should send token before close");
        drop(mpsc_tx);

        let resumed_status = wait_for_resume_activity(state.as_ref(), session_id).await;
        wait_for_provider_purpose(provider.as_ref(), "agent_loop").await;
        assert!(matches!(
            resumed_status,
            AgentStatus::Running | AgentStatus::Completed
        ));

        let after = state
            .load_session_merged(session_id)
            .await
            .expect("session should exist after forwarder close hook");
        assert!(after.pending_question.is_none());
        assert!(after.messages.iter().any(|message| {
            message.tool_call_id.as_deref() == Some(tool_call_id)
                && message.content == "Auto-selected response (gold): OK"
                && message.tool_success == Some(true)
        }));
        assert!(!after.metadata.contains_key("gold.auto_continue_count"));
        assert!(!after.messages.iter().any(|message| {
            message
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("runtime_kind"))
                .and_then(|value| value.as_str())
                == Some("gold_continue_resume")
        }));
        assert_eq!(
            provider.request_purposes(),
            vec!["gold_evaluation", "gold_auto_answer", "agent_loop"]
        );

        provider.release_agent_loop();
        sleep(Duration::from_millis(50)).await;
    }
}
