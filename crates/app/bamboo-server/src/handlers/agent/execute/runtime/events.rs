use std::collections::HashSet;
use tokio::sync::mpsc;
use tracing::Instrument;

use crate::app_state::AppState;
use bamboo_agent_core::AgentEvent;
use bamboo_engine::config::GoldConfig;
use bamboo_engine::gold_auto_answer::{maybe_auto_answer_pending_question, GoldAutoAnswerOutcome};

/// Returns true for events that carry critical state a late subscriber must see.
///
/// These are cached on the runner and replayed when an SSE client connects
/// after the live stream has already started.
fn is_critical_event(event: &AgentEvent) -> bool {
    event.is_replayable_session_state()
}

pub(crate) fn spawn_event_forwarder(
    state: actix_web::web::Data<AppState>,
    session_id: String,
    run_id: String,
    mut mpsc_rx: mpsc::Receiver<AgentEvent>,
    session_tx: tokio::sync::broadcast::Sender<AgentEvent>,
    gold_config: Option<GoldConfig>,
) {
    // Always-on relay: previously the notification relay only started when an
    // SSE/WS client subscribed, so a run that finishes (or hits a
    // clarification/approval gate) before any client ever connects — a race
    // on the interactive path, or a session opened later — silently never
    // classified events into notifications. This is called at every
    // execution start site (paired 1:1 with `spawn_agent_execution` here and
    // in `resume_adapter`), so the relay is guaranteed running the moment a
    // run begins, not just when someone happens to be watching.
    // Idempotent (`try_begin_relay`), so this and a client's own
    // `ensure_notification_relay` call race harmlessly — whichever runs
    // first wins.
    state.ensure_notification_relay(&session_id, session_tx.clone());

    let span_session_id = session_id.clone();
    let session_span = tracing::info_span!("event_forwarder", session_id = %span_session_id);

    tokio::spawn(
        async move {
            // Capture the reservation generation at the call site. This task
            // may start only after a clarification answer has replaced the
            // shared runner entry; reading that mutable registry here would
            // tag the old activation's terminal with the successor run id.
            let started_event = AgentEvent::ExecutionStarted {
                run_id: run_id.clone(),
                session_id: session_id.clone(),
                started_at: chrono::Utc::now().to_rfc3339(),
            };
            let publication = {
                let runners = state.agent_runners.read().await;
                let Some(runner) = runners
                    .get(&session_id)
                    .filter(|runner| runner.run_id == run_id)
                else {
                    return;
                };
                state.account_sink.record(Some(&session_id), &started_event);
                let _ = session_tx.send(started_event);
                runner.event_publication.clone()
            };
            let mut forwarded_lifecycle_ids = HashSet::new();
            while let Some(event) = mpsc_rx.recv().await {
                let lifecycle_id = match &event {
                    AgentEvent::WorkflowActivated { event_id, .. }
                    | AgentEvent::WorkflowDeactivated { event_id, .. } => Some(event_id),
                    _ => None,
                };
                if lifecycle_id
                    .is_some_and(|event_id| !forwarded_lifecycle_ids.insert(event_id.clone()))
                {
                    tracing::debug!(
                        session_id = %session_id,
                        "duplicate workflow lifecycle event suppressed before forwarding"
                    );
                    continue;
                }
                let needs_runner_update = is_critical_event(&event)
                    || matches!(&event, AgentEvent::TokenBudgetUpdated { .. });
                if needs_runner_update {
                    let mut runners = state.agent_runners.write().await;
                    let Some(runner) = runners
                        .get_mut(&session_id)
                        .filter(|runner| runner.run_id == run_id)
                    else {
                        return;
                    };
                    if is_critical_event(&event) {
                        runner.push_critical_event(event.clone());
                        tracing::trace!(
                            "[{}] Cached critical event for late subscribers",
                            session_id
                        );
                    }
                    if matches!(&event, AgentEvent::TokenBudgetUpdated { .. }) {
                        runner.last_budget_event = Some(event.clone());
                        // Fires once per agent round — far too hot for debug.
                        tracing::trace!(
                            "[{}] Stored budget event for late subscribers",
                            session_id
                        );
                    }
                    // Hold exact generation ownership through the synchronous
                    // account/broadcast publication. A successor reservation
                    // needs this write lock and therefore cannot interleave a
                    // new Started before this old frame.
                    let route_session_id = event.session_id().unwrap_or(&session_id);
                    state.account_sink.record(Some(route_session_id), &event);
                    let _ = session_tx.send(event);
                } else {
                    if !publication.publish(|| {
                        let route_session_id = event.session_id().unwrap_or(&session_id);
                        state.account_sink.record(Some(route_session_id), &event);
                        let _ = session_tx.send(event);
                    }) {
                        return;
                    }
                }
            }

            // Gold auto-answer responds to a pending clarification that genuinely
            // paused the run (the session is Suspended, not Completed), so it stays
            // here in the post-stop path. Autonomous goal continuation, by contrast,
            // now happens INSIDE the runner loop's terminal gate (see
            // bamboo-engine `evaluate_gold_terminal`) so the run emits a single
            // terminal `Complete` and the frontend never gets stuck mid-settle.
            let superseded = state
                .agent_runners
                .read()
                .await
                .get(&session_id)
                .is_some_and(|runner| runner.run_id != run_id);
            let auto_answer_outcome = if superseded {
                GoldAutoAnswerOutcome::Skipped {
                    reason: format!("run {run_id} was superseded before post-stop auto-answer"),
                }
            } else {
                let resume_port =
                    crate::app_state::resume_adapter::AppStateResumeRef(state.clone());
                maybe_auto_answer_pending_question(
                    state.get_ref(),
                    &resume_port,
                    &session_id,
                    gold_config.clone(),
                )
                .await
            };
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
    use bamboo_llm::{
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
    use bamboo_engine::session_app::execute::has_pending_clarification_resume;

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
            version: Some(1),
        }
    }

    fn sub_agent_started() -> AgentEvent {
        AgentEvent::SubAgentStarted {
            parent_session_id: "parent".into(),
            child_session_id: "child-1".into(),
            title: Some("child work".into()),
        }
    }

    async fn current_run_id(state: &actix_web::web::Data<AppState>, session_id: &str) -> String {
        state.agent_runners.read().await[session_id].run_id.clone()
    }

    #[tokio::test]
    async fn server_tokens_progress_without_the_shared_runner_registry() {
        let directory = tempfile::tempdir().unwrap();
        let state =
            actix_web::web::Data::new(AppState::new(directory.path().to_path_buf()).await.unwrap());
        let id = "server-independent-tokens";
        let sender = state.get_session_event_sender(id).await;
        let mut receiver = sender.subscribe();
        bamboo_engine::execution::reserve_runner_core(
            &state.agent_runners,
            &state.session_event_senders,
            id,
            &sender,
        )
        .await;
        let run_id = current_run_id(&state, id).await;
        let (input, events) = mpsc::channel(8);
        spawn_event_forwarder(state.clone(), id.into(), run_id, events, sender, None);
        assert!(matches!(
            receiver.recv().await.unwrap(),
            AgentEvent::ExecutionStarted { .. }
        ));
        let registry = state.agent_runners.write().await;
        input
            .send(AgentEvent::Token {
                content: "independent".into(),
            })
            .await
            .unwrap();
        timeout(Duration::from_secs(5), async {
            loop {
                if let AgentEvent::Token { content } = receiver.recv().await.unwrap() {
                    assert_eq!(content, "independent");
                    break;
                }
            }
        })
        .await
        .expect("the server token path must not wait for runner registry ownership");
        assert!(registry[id].last_activity_at().is_some());
        drop(registry);
        drop(input);
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
                // Read the marker from the authoritative in-memory cache rather
                // than `load_session_merged`: the merge has a write side-effect
                // (on a prefer-storage decision it inserts the storage copy back
                // into the cache), so polling it here can repeatedly clobber the
                // freshly-answered session with the stale storage version and
                // starve convergence under load.
                let marker_consumed =
                    bamboo_engine::read_cached_session(&state.sessions, session_id)
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
            version: Some(2),
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
            title_generated: true,
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

    #[test]
    fn clarification_is_replayed_to_late_subscribers() {
        assert!(is_critical_event(&AgentEvent::NeedClarification {
            question: "Choose".to_string(),
            options: Some(vec!["A".to_string()]),
            tool_call_id: Some("call-1".to_string()),
            tool_name: Some("ConclusionWithOptions".to_string()),
            allow_custom: false,
            source: Some(bamboo_agent_core::PendingQuestionSource::PauseTool),
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
            current_run_id(&state, session_id).await,
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
    async fn delayed_server_forwarder_cannot_publish_after_successor_reservation() {
        let temp_dir = tempfile::tempdir().unwrap();
        let state = actix_web::web::Data::new(
            AppState::new(temp_dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        let session_id = "server-generation-race";
        let (session_tx, mut session_rx) = tokio::sync::broadcast::channel(16);
        let mut successor = bamboo_engine::runtime::execution::runner_state::AgentRunner::new();
        successor.run_id = "run-new".to_string();
        successor.status = bamboo_engine::runtime::execution::runner_state::AgentStatus::Running;
        successor.event_sender = session_tx.clone();
        state
            .agent_runners
            .write()
            .await
            .insert(session_id.to_string(), successor);

        session_tx
            .send(AgentEvent::ExecutionStarted {
                run_id: "run-new".to_string(),
                session_id: session_id.to_string(),
                started_at: Utc::now().to_rfc3339(),
            })
            .unwrap();
        let (old_tx, old_rx) = mpsc::channel(4);
        spawn_event_forwarder(
            state,
            session_id.to_string(),
            "run-old".to_string(),
            old_rx,
            session_tx,
            None,
        );
        let _ = old_tx.send(task_list_updated()).await;
        drop(old_tx);

        assert!(matches!(
            session_rx.recv().await.unwrap(),
            AgentEvent::ExecutionStarted { ref run_id, .. } if run_id == "run-new"
        ));
        assert!(
            timeout(Duration::from_millis(100), session_rx.recv())
                .await
                .is_err(),
            "superseded server forwarder must not publish Started or stale state"
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
        spawn_event_forwarder(
            state.clone(),
            session_id.to_string(),
            current_run_id(&state, session_id).await,
            mpsc_rx,
            session_tx,
            None,
        );

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
        mpsc_tx
            .send(AgentEvent::ChildApprovalChanged {
                parent_session_id: "parent-session".into(),
                child_session_id: session_id.into(),
                child_attempt: 1,
                request_id: "req-1".into(),
                version: 2,
                status: "approved".into(),
                reason: None,
                tool_name: "Bash".into(),
                permission: "execute".into(),
                resource: "/tmp/x".into(),
                created_at: "2026-01-01T00:00:00Z".into(),
                resolved_at: Some("2026-01-01T00:00:01Z".into()),
            })
            .await
            .unwrap();
        drop(mpsc_tx);

        tokio::time::sleep(std::time::Duration::from_millis(80)).await;

        let journaled =
            bamboo_engine::events::journal::read_since(state.account_sink.events_dir(), 0).unwrap();
        assert_eq!(journaled.len(), 4, "ephemeral Token must be excluded");
        assert!(matches!(
            journaled[0].event,
            AgentEvent::ExecutionStarted { .. }
        ));
        assert!(matches!(
            journaled[1].event,
            AgentEvent::TaskListUpdated { .. }
        ));
        assert!(matches!(journaled[2].event, AgentEvent::Complete { .. }));
        // The terminal event routed to the right session via caller context.
        assert_eq!(journaled[2].session_id.as_deref(), Some(session_id));
        assert_eq!(journaled[3].session_id.as_deref(), Some("parent-session"));
        assert!(matches!(
            journaled[3].event,
            AgentEvent::ChildApprovalChanged { .. }
        ));
        // Sequence numbers are monotonic and 1-based.
        assert_eq!(journaled[0].seq, 1);
        assert_eq!(journaled[1].seq, 2);
        assert_eq!(journaled[2].seq, 3);
        assert_eq!(journaled[3].seq, 4);
    }

    #[tokio::test]
    async fn event_forwarder_suppresses_duplicate_lifecycle_before_all_observers() {
        let temp_dir = tempfile::tempdir().unwrap();
        let state = AppState::new(temp_dir.path().to_path_buf())
            .await
            .expect("app state");
        let state = actix_web::web::Data::new(state);
        let session_id = "lifecycle-forwarder-dedupe";
        {
            let mut runner = bamboo_engine::runtime::execution::runner_state::AgentRunner::new();
            runner.status = bamboo_engine::runtime::execution::runner_state::AgentStatus::Running;
            state
                .agent_runners
                .write()
                .await
                .insert(session_id.to_string(), runner);
        }
        let (mpsc_tx, mpsc_rx) = mpsc::channel(8);
        let (session_tx, mut session_rx) = tokio::sync::broadcast::channel(8);
        let mut account_rx = state.account_sink.subscribe();
        spawn_event_forwarder(
            state.clone(),
            session_id.to_string(),
            current_run_id(&state, session_id).await,
            mpsc_rx,
            session_tx,
            None,
        );
        let event = AgentEvent::WorkflowActivated {
            event_id: "stable-forwarder-id".to_string(),
            session_id: session_id.to_string(),
            workflow_id: "review".to_string(),
            revision: 7,
            invoked_by: "model".to_string(),
        };
        mpsc_tx.send(event.clone()).await.unwrap();
        mpsc_tx.send(event).await.unwrap();
        drop(mpsc_tx);

        let started = timeout(Duration::from_secs(1), session_rx.recv())
            .await
            .expect("session start event")
            .expect("broadcast");
        assert!(matches!(started, AgentEvent::ExecutionStarted { .. }));
        let session_event = timeout(Duration::from_secs(1), session_rx.recv())
            .await
            .expect("session lifecycle event")
            .expect("broadcast");
        assert!(matches!(
            session_event,
            AgentEvent::WorkflowActivated { .. }
        ));
        let mut session_lifecycle_count = 1;
        while let Ok(Ok(event)) = timeout(Duration::from_millis(30), session_rx.recv()).await {
            if matches!(event, AgentEvent::WorkflowActivated { ref event_id, .. } if event_id == "stable-forwarder-id")
            {
                session_lifecycle_count += 1;
            }
        }
        assert_eq!(session_lifecycle_count, 1);
        let account_started = timeout(Duration::from_secs(1), account_rx.recv())
            .await
            .expect("account start event")
            .expect("account broadcast");
        assert!(matches!(
            account_started.event,
            AgentEvent::ExecutionStarted { .. }
        ));
        let account_event = timeout(Duration::from_secs(1), account_rx.recv())
            .await
            .expect("account lifecycle event")
            .expect("account broadcast");
        assert!(matches!(
            account_event.event,
            AgentEvent::WorkflowActivated { .. }
        ));
        let mut account_lifecycle_count = 1;
        while let Ok(Ok(change)) = timeout(Duration::from_millis(30), account_rx.recv()).await {
            if matches!(change.event, AgentEvent::WorkflowActivated { ref event_id, .. } if event_id == "stable-forwarder-id")
            {
                account_lifecycle_count += 1;
            }
        }
        assert_eq!(account_lifecycle_count, 1);
        let runner = state.agent_runners.read().await;
        assert_eq!(runner[session_id].last_critical_events.len(), 1);
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
            current_run_id(&state, session_id).await,
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
            current_run_id(&state, session_id).await,
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
        config.provider = "test-provider".to_string();
        config.features.provider_model_ref = true;
        let provider = ScriptedProvider::new("ok.");
        let provider_trait: Arc<dyn LLMProvider> = provider.clone();
        let mut app_state = AppState::new_with_provider(
            temp_dir.path().to_path_buf(),
            config,
            provider_trait.clone(),
        )
        .await
        .expect("app state");
        app_state.provider_registry = Arc::new(ProviderRegistry::new(
            HashMap::from([("test-provider".to_string(), provider_trait)]),
            "test-provider".to_string(),
        ));
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
        let mut runner = bamboo_engine::runtime::execution::runner_state::AgentRunner::new();
        runner.run_id = "gold-run".to_string();
        runner.status = AgentStatus::Running;
        runner.event_sender = session_tx.clone();
        state
            .agent_runners
            .write()
            .await
            .insert(session_id.to_string(), runner);

        spawn_event_forwarder(
            state.clone(),
            session_id.to_string(),
            "gold-run".to_string(),
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
        state
            .agent_runners
            .write()
            .await
            .get_mut(session_id)
            .unwrap()
            .status = AgentStatus::Completed;
        drop(mpsc_tx);

        let resumed_status = wait_for_resume_activity(state.as_ref(), session_id).await;
        wait_for_provider_purpose(provider.as_ref(), "agent_loop").await;
        assert!(matches!(
            resumed_status,
            AgentStatus::Running | AgentStatus::Completed
        ));

        // The gold answer (pending_question cleared + auto-answer message)
        // lands in the memory cache first; `load_session_merged` can still
        // surface the stale storage copy until persistence catches up, because
        // its prefer-storage heuristic favours a storage session that still
        // carries the pending question (`memory.pending=None && storage.pending=Some`).
        // Poll until the merged view has settled so the assertions are
        // deterministic rather than racing the memory↔storage convergence.
        let after = timeout(Duration::from_secs(30), async {
            loop {
                if let Some(session) =
                    bamboo_engine::read_cached_session(&state.sessions, session_id)
                {
                    let answered = session.pending_question.is_none()
                        && session.messages.iter().any(|message| {
                            message.tool_call_id.as_deref() == Some(tool_call_id)
                                && message.content == "Auto-selected response (gold): OK"
                                && message.tool_success == Some(true)
                        });
                    if answered {
                        return session;
                    }
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("gold auto-answer should be applied to the session");
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
        config.provider = "test-provider".to_string();
        config.features.provider_model_ref = true;
        let provider = ScriptedProvider::new("ok.");
        let provider_trait: Arc<dyn LLMProvider> = provider.clone();
        let mut app_state = AppState::new_with_provider(
            temp_dir.path().to_path_buf(),
            config,
            provider_trait.clone(),
        )
        .await
        .expect("app state");
        app_state.provider_registry = Arc::new(ProviderRegistry::new(
            HashMap::from([("test-provider".to_string(), provider_trait)]),
            "test-provider".to_string(),
        ));
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
        let mut runner = bamboo_engine::runtime::execution::runner_state::AgentRunner::new();
        runner.run_id = "gold-run".to_string();
        runner.status = AgentStatus::Running;
        runner.event_sender = session_tx.clone();
        state
            .agent_runners
            .write()
            .await
            .insert(session_id.to_string(), runner);

        let mut gold_config = test_gold_config();
        gold_config.auto_answer_enabled = true;
        gold_config.auto_continue_enabled = true;
        gold_config.max_auto_continuations = 3;

        spawn_event_forwarder(
            state.clone(),
            session_id.to_string(),
            "gold-run".to_string(),
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
        state
            .agent_runners
            .write()
            .await
            .get_mut(session_id)
            .unwrap()
            .status = AgentStatus::Completed;
        drop(mpsc_tx);

        let resumed_status = wait_for_resume_activity(state.as_ref(), session_id).await;
        wait_for_provider_purpose(provider.as_ref(), "agent_loop").await;
        assert!(matches!(
            resumed_status,
            AgentStatus::Running | AgentStatus::Completed
        ));

        // See the sibling test: poll until the merged view has settled (the
        // gold answer is visible) so the assertions don't race the
        // memory↔storage convergence in `load_session_merged`.
        let after = timeout(Duration::from_secs(30), async {
            loop {
                if let Some(session) =
                    bamboo_engine::read_cached_session(&state.sessions, session_id)
                {
                    let answered = session.pending_question.is_none()
                        && session.messages.iter().any(|message| {
                            message.tool_call_id.as_deref() == Some(tool_call_id)
                                && message.content == "Auto-selected response (gold): OK"
                                && message.tool_success == Some(true)
                        });
                    if answered {
                        return session;
                    }
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("gold auto-answer should be applied to the session");
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
