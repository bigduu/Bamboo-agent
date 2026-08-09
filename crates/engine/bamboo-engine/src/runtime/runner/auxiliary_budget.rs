//! Process-wide low-priority concurrency budget for auxiliary LLM requests.
//!
//! The key uses the provider allocation identity plus model. Provider registry
//! clones retain the same `Arc` allocation across sessions, while a provider
//! reload receives a fresh identity. Weak semaphore entries disappear once no
//! request is waiting/running, avoiding an unbounded per-reload registry.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};

use bamboo_llm::LLMProvider;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AuxiliaryBudgetKey {
    provider_identity: usize,
    model_name: String,
}

struct AuxiliaryBudgetEntry {
    configured_limit: usize,
    semaphore: Weak<Semaphore>,
}

static AUXILIARY_BUDGETS: OnceLock<Mutex<HashMap<AuxiliaryBudgetKey, AuxiliaryBudgetEntry>>> =
    OnceLock::new();

fn provider_identity(provider: &Arc<dyn LLMProvider>) -> usize {
    Arc::as_ptr(provider) as *const () as usize
}

fn semaphore(
    provider: &Arc<dyn LLMProvider>,
    model_name: &str,
    configured_limit: usize,
) -> Arc<Semaphore> {
    let configured_limit = configured_limit.clamp(
        1,
        crate::runtime::config::MAX_AUXILIARY_EVALUATION_MAX_CONCURRENCY,
    );
    let key = AuxiliaryBudgetKey {
        provider_identity: provider_identity(provider),
        model_name: model_name.to_string(),
    };
    let registry = AUXILIARY_BUDGETS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut registry = registry
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    registry.retain(|_, entry| entry.semaphore.strong_count() > 0);

    if let Some(entry) = registry.get(&key) {
        if let Some(semaphore) = entry.semaphore.upgrade() {
            if entry.configured_limit != configured_limit {
                tracing::warn!(
                    provider_identity = key.provider_identity,
                    model = %key.model_name,
                    active_limit = entry.configured_limit,
                    requested_limit = configured_limit,
                    "auxiliary evaluation budget already active with a different limit; retaining the active process-wide limit"
                );
            }
            return semaphore;
        }
    }

    let semaphore = Arc::new(Semaphore::new(configured_limit));
    registry.insert(
        key,
        AuxiliaryBudgetEntry {
            configured_limit,
            semaphore: Arc::downgrade(&semaphore),
        },
    );
    semaphore
}

/// Wait for one low-priority slot for this exact provider allocation/model.
/// Foreground request paths never call this function.
pub(crate) async fn acquire(
    provider: &Arc<dyn LLMProvider>,
    model_name: &str,
    configured_limit: usize,
) -> OwnedSemaphorePermit {
    semaphore(provider, model_name, configured_limit)
        .acquire_owned()
        .await
        .expect("auxiliary evaluation semaphore is never closed")
}

#[cfg(test)]
mod tests {
    use super::acquire;
    use bamboo_agent_core::{
        FunctionCall, GoldCheckpoint, GoldConfidence, GoldDecision, Message, PendingQuestion,
        PendingQuestionSource, Session, ToolCall, ToolSchema,
    };
    use bamboo_domain::{TaskItem, TaskItemStatus, TaskList};
    use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMRequestOptions, LLMStream};
    use chrono::Utc;
    use futures::stream;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    struct ImmediateProvider;

    #[async_trait::async_trait]
    impl LLMProvider for ImmediateProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            Ok(Box::pin(stream::iter(vec![Ok(LLMChunk::Done)])))
        }
    }

    struct BlockingAuxiliaryProvider {
        active: AtomicUsize,
        peak: AtomicUsize,
        entered: AtomicUsize,
        release: tokio::sync::Semaphore,
    }

    impl BlockingAuxiliaryProvider {
        fn new() -> Self {
            Self {
                active: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                entered: AtomicUsize::new(0),
                release: tokio::sync::Semaphore::new(0),
            }
        }

        async fn wait_for_entered(&self, expected: usize) {
            tokio::time::timeout(Duration::from_secs(2), async {
                while self.entered.load(Ordering::SeqCst) < expected {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("expected auxiliary provider dispatches should enter");
        }

        fn response_for(purpose: &str) -> ToolCall {
            let (name, arguments) = match purpose {
                "task_evaluation" => (
                    "update_task_item",
                    r#"{"item_id":"task-1","status":"completed","notes":"done"}"#,
                ),
                "gold_auto_answer" => (
                    "report_gold_auto_answer",
                    r#"{"apply":true,"answer":"OK","confidence":"high","reasoning":"supported"}"#,
                ),
                _ => (
                    "report_gold_evaluation",
                    r#"{"decision":"continue","confidence":"high","reasoning":"continue"}"#,
                ),
            };
            ToolCall {
                id: format!("{purpose}-call"),
                tool_type: "function".to_string(),
                function: FunctionCall {
                    name: name.to_string(),
                    arguments: arguments.to_string(),
                },
            }
        }
    }

    #[async_trait::async_trait]
    impl LLMProvider for BlockingAuxiliaryProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            // Foreground calls use this entry point in the regression test and
            // never acquire the opt-in auxiliary budget.
            Ok(Box::pin(stream::iter(vec![Ok(LLMChunk::Done)])))
        }

        async fn chat_stream_with_options(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
            options: Option<&LLMRequestOptions>,
        ) -> Result<LLMStream, LLMError> {
            let purpose = options
                .and_then(|options| options.request_purpose.as_deref())
                .unwrap_or("unknown")
                .to_string();
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(active, Ordering::SeqCst);
            self.entered.fetch_add(1, Ordering::SeqCst);
            let release = self
                .release
                .acquire()
                .await
                .expect("release semaphore stays open");
            release.forget();
            self.active.fetch_sub(1, Ordering::SeqCst);

            Ok(Box::pin(stream::iter(vec![
                Ok(LLMChunk::ToolCalls(vec![Self::response_for(&purpose)])),
                Ok(LLMChunk::Done),
            ])))
        }
    }

    fn task_session_and_context() -> (Session, crate::runtime::task_context::TaskLoopContext) {
        let mut session = Session::new("task-session", "shared-fast-model");
        session.set_task_list(TaskList {
            session_id: session.id.clone(),
            title: "Tasks".to_string(),
            items: vec![TaskItem {
                id: "task-1".to_string(),
                description: "Finish the work".to_string(),
                status: TaskItemStatus::InProgress,
                ..TaskItem::default()
            }],
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });
        session.set_task_list_version_meta("1");
        let context = crate::runtime::task_context::TaskLoopContext::from_session(&session)
            .expect("task context");
        (session, context)
    }

    fn gold_config() -> crate::runtime::config::GoldConfig {
        crate::runtime::config::GoldConfig {
            enabled: true,
            auto_answer_enabled: true,
            model_name: Some("shared-fast-model".to_string()),
            ..Default::default()
        }
    }

    fn gold_target(provider: Arc<dyn LLMProvider>) -> crate::gold_auto_answer::GoldAuxiliaryTarget {
        crate::gold_auto_answer::GoldAuxiliaryTarget {
            provider,
            model: "shared-fast-model".to_string(),
            timeout_context: crate::runtime::stream::handler::StreamTimeoutContext::default(),
            configured_limit: 2,
        }
    }

    #[tokio::test]
    async fn task_and_gold_sessions_share_provider_model_limit_without_blocking_foreground() {
        let provider: Arc<dyn LLMProvider> = Arc::new(ImmediateProvider);
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let mut handles = Vec::new();

        // Alternate Task and Gold labels across distinct logical sessions. Both
        // production call paths acquire this exact primitive.
        for index in 0..6 {
            let provider = provider.clone();
            let active = active.clone();
            let peak = peak.clone();
            let entered = entered.clone();
            let release = release.clone();
            handles.push(tokio::spawn(async move {
                let _kind = if index % 2 == 0 { "task" } else { "gold" };
                let _session_id = format!("session-{index}");
                let _permit = acquire(&provider, "shared-fast-model", 2).await;
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                entered.fetch_add(1, Ordering::SeqCst);
                let release_permit = release.acquire().await.expect("release gate stays open");
                release_permit.forget();
                active.fetch_sub(1, Ordering::SeqCst);
            }));
        }

        tokio::time::timeout(Duration::from_secs(1), async {
            while entered.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("two auxiliary evaluations should enter");
        assert_eq!(peak.load(Ordering::SeqCst), 2);

        // The budget is opt-in at auxiliary dispatch sites, not a wrapper on
        // the provider itself. A foreground call therefore completes while all
        // low-priority slots are occupied.
        let _foreground_stream = tokio::time::timeout(
            Duration::from_millis(100),
            provider.chat_stream(&[], &[], None, "shared-fast-model"),
        )
        .await
        .expect("foreground provider call must not wait for auxiliary permits")
        .expect("foreground provider call succeeds");

        for expected_entered in [4, 6] {
            release.add_permits(2);
            tokio::time::timeout(Duration::from_secs(1), async {
                while entered.load(Ordering::SeqCst) < expected_entered {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("next auxiliary batch should enter");
            assert!(active.load(Ordering::SeqCst) <= 2);
        }
        release.add_permits(2);
        for handle in handles {
            handle.await.expect("auxiliary session joins");
        }
        assert_eq!(peak.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn all_production_auxiliary_paths_share_one_provider_model_budget() {
        use crate::runtime::gold_evaluation::{
            execute_async_gold_evaluation, AsyncGoldEvaluationRequest, GoldEvaluationResult,
        };
        use crate::runtime::runner::task_lifecycle::{
            execute_async_task_evaluation, AsyncTaskEvaluationRequest,
        };

        let concrete = Arc::new(BlockingAuxiliaryProvider::new());
        let provider: Arc<dyn LLMProvider> = concrete.clone();
        let (event_tx, _event_rx) = tokio::sync::mpsc::channel(32);
        let gold_config = gold_config();

        let (task_session, task_context) = task_session_and_context();
        let task_handle = {
            let provider = provider.clone();
            let event_tx = event_tx.clone();
            tokio::spawn(async move {
                execute_async_task_evaluation(
                    AsyncTaskEvaluationRequest {
                        evaluation_id: "task-eval".to_string(),
                        metrics_round_id: "task-metrics".to_string(),
                        session_id: task_session.id.clone(),
                        shared_session_id: task_session.id.clone(),
                        round_number: 1,
                        based_on_task_context_version: task_context.version,
                        based_on_task_list: task_session
                            .task_list
                            .clone()
                            .expect("task session list"),
                        task_list_title: Some("Tasks".to_string()),
                        model_name: "shared-fast-model".to_string(),
                        timeout_context:
                            crate::runtime::stream::handler::StreamTimeoutContext::default(),
                        reasoning_effort: None,
                        task_context_snapshot: task_context,
                        session_snapshot: task_session,
                    },
                    provider,
                    event_tx,
                    2,
                    None,
                    Arc::new(AtomicBool::new(false)),
                    Arc::new(AtomicBool::new(false)),
                )
                .await
            })
        };

        let gold_handle = {
            let provider = provider.clone();
            let event_tx = event_tx.clone();
            let gold_config = gold_config.clone();
            tokio::spawn(async move {
                execute_async_gold_evaluation(
                    AsyncGoldEvaluationRequest {
                        session_id: "gold-session".to_string(),
                        round_number: 1,
                        model_name: "shared-fast-model".to_string(),
                        timeout_context:
                            crate::runtime::stream::handler::StreamTimeoutContext::default(),
                        reasoning_effort: None,
                        checkpoint: GoldCheckpoint::PostRound,
                        session_snapshot: Session::new("gold-session", "shared-fast-model"),
                        task_context_snapshot: None,
                        gold_config,
                    },
                    provider,
                    event_tx,
                    2,
                )
                .await
            })
        };

        let auto_state_handle = {
            let provider = provider.clone();
            let event_tx = event_tx.clone();
            let gold_config = gold_config.clone();
            tokio::spawn(async move {
                crate::gold_auto_answer::evaluate_gold_state_with_target(
                    "auto-state-session",
                    &Session::new("auto-state-session", "shared-fast-model"),
                    &gold_config,
                    gold_target(provider),
                    &event_tx,
                    1,
                )
                .await
            })
        };

        let auto_answer_handle = {
            let provider = provider.clone();
            let gold_config = gold_config.clone();
            tokio::spawn(async move {
                let mut session = Session::new("auto-answer-session", "shared-fast-model");
                session.pending_question = Some(PendingQuestion {
                    tool_call_id: "pending-call".to_string(),
                    tool_name: "conclusion_with_options".to_string(),
                    question: "Choose".to_string(),
                    options: vec!["OK".to_string()],
                    allow_custom: false,
                    source: PendingQuestionSource::PauseTool,
                });
                crate::gold_auto_answer::evaluate_gold_auto_answer_question_with_target(
                    "auto-answer-session",
                    &session,
                    &gold_config,
                    &GoldEvaluationResult {
                        checkpoint: GoldCheckpoint::Terminal,
                        iteration: 1,
                        decision: GoldDecision::Continue,
                        confidence: GoldConfidence::High,
                        reasoning: "continue".to_string(),
                        missing_information: Vec::new(),
                        next_action: None,
                        prompt_tokens: 1,
                        completion_tokens: 1,
                    },
                    gold_target(provider),
                )
                .await
            })
        };

        concrete.wait_for_entered(2).await;
        assert_eq!(concrete.peak.load(Ordering::SeqCst), 2);

        let foreground = tokio::time::timeout(
            Duration::from_millis(100),
            provider.chat_stream(&[], &[], None, "shared-fast-model"),
        )
        .await
        .expect("foreground dispatch must bypass the auxiliary budget")
        .expect("foreground provider succeeds");
        drop(foreground);

        concrete.release.add_permits(2);
        concrete.wait_for_entered(4).await;
        assert!(concrete.active.load(Ordering::SeqCst) <= 2);
        concrete.release.add_permits(2);

        task_handle.await.expect("Task path joins");
        gold_handle.await.expect("Gold path joins");
        auto_state_handle
            .await
            .expect("Gold auto-answer state path joins")
            .expect("Gold auto-answer state evaluation succeeds");
        auto_answer_handle
            .await
            .expect("Gold auto-answer decision path joins")
            .expect("Gold auto-answer decision succeeds");
        assert_eq!(concrete.peak.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn auxiliary_permit_wait_does_not_consume_stream_request_timeout() {
        use crate::runtime::gold_evaluation::{
            execute_async_gold_evaluation, AsyncGoldEvaluationRequest, GoldEvaluationResult,
        };
        use crate::runtime::runner::task_lifecycle::{
            execute_async_task_evaluation, AsyncTaskEvaluationRequest,
        };

        let concrete = Arc::new(BlockingAuxiliaryProvider::new());
        let provider: Arc<dyn LLMProvider> = concrete.clone();
        let model = "queued-fast-model";
        let held_permit = acquire(&provider, model, 1).await;
        let (event_tx, _event_rx) = tokio::sync::mpsc::channel(32);
        let timeout_context = || {
            crate::runtime::stream::handler::StreamTimeoutContext::new(
                bamboo_config::StreamTimeoutConfig {
                    transport_idle_timeout_secs: 1,
                    first_semantic_timeout_secs: 1,
                    semantic_idle_timeout_secs: 1,
                },
                Some("queued-provider"),
                Some(model),
            )
        };
        let (task_session, task_context) = task_session_and_context();
        let task_dispatched = Arc::new(AtomicBool::new(false));
        let task_handle = {
            let provider = provider.clone();
            let event_tx = event_tx.clone();
            let metrics_started = task_dispatched.clone();
            tokio::spawn(async move {
                execute_async_task_evaluation(
                    AsyncTaskEvaluationRequest {
                        evaluation_id: "queued-task".to_string(),
                        metrics_round_id: "queued-task-metrics".to_string(),
                        session_id: task_session.id.clone(),
                        shared_session_id: task_session.id.clone(),
                        round_number: 1,
                        based_on_task_context_version: task_context.version,
                        based_on_task_list: task_session
                            .task_list
                            .clone()
                            .expect("task session list"),
                        task_list_title: Some("Tasks".to_string()),
                        model_name: model.to_string(),
                        timeout_context: timeout_context(),
                        reasoning_effort: None,
                        task_context_snapshot: task_context,
                        session_snapshot: task_session,
                    },
                    provider,
                    event_tx,
                    1,
                    None,
                    metrics_started,
                    Arc::new(AtomicBool::new(false)),
                )
                .await
            })
        };

        let gold_config = gold_config();
        let gold_handle = {
            let provider = provider.clone();
            let event_tx = event_tx.clone();
            let gold_config = gold_config.clone();
            tokio::spawn(async move {
                execute_async_gold_evaluation(
                    AsyncGoldEvaluationRequest {
                        session_id: "queued-gold".to_string(),
                        round_number: 1,
                        model_name: model.to_string(),
                        timeout_context: timeout_context(),
                        reasoning_effort: None,
                        checkpoint: GoldCheckpoint::PostRound,
                        session_snapshot: Session::new("queued-gold", model),
                        task_context_snapshot: None,
                        gold_config,
                    },
                    provider,
                    event_tx,
                    1,
                )
                .await
            })
        };

        let auto_answer_handle = {
            let provider = provider.clone();
            let gold_config = gold_config.clone();
            tokio::spawn(async move {
                let mut session = Session::new("queued-auto-answer", model);
                session.pending_question = Some(PendingQuestion {
                    tool_call_id: "queued-pending".to_string(),
                    tool_name: "conclusion_with_options".to_string(),
                    question: "Choose".to_string(),
                    options: vec!["OK".to_string()],
                    allow_custom: false,
                    source: PendingQuestionSource::PauseTool,
                });
                crate::gold_auto_answer::evaluate_gold_auto_answer_question_with_target(
                    "queued-auto-answer",
                    &session,
                    &gold_config,
                    &GoldEvaluationResult {
                        checkpoint: GoldCheckpoint::Terminal,
                        iteration: 1,
                        decision: GoldDecision::Continue,
                        confidence: GoldConfidence::High,
                        reasoning: "continue".to_string(),
                        missing_information: Vec::new(),
                        next_action: None,
                        prompt_tokens: 1,
                        completion_tokens: 1,
                    },
                    crate::gold_auto_answer::GoldAuxiliaryTarget {
                        provider,
                        model: model.to_string(),
                        timeout_context: timeout_context(),
                        configured_limit: 1,
                    },
                )
                .await
            })
        };

        tokio::time::sleep(Duration::from_millis(1_200)).await;
        assert_eq!(
            concrete.entered.load(Ordering::SeqCst),
            0,
            "no provider request may dispatch while the permit is held"
        );
        assert!(
            !task_dispatched.load(Ordering::Acquire),
            "Task dispatch metrics must start at the same post-permit boundary"
        );

        drop(held_permit);
        for expected in 1..=3 {
            concrete.wait_for_entered(expected).await;
            concrete.release.add_permits(1);
        }

        let task = task_handle.await.expect("queued Task joins");
        assert_eq!(
            task.error, None,
            "Task request should retain a fresh timeout"
        );
        let gold = gold_handle.await.expect("queued Gold joins");
        assert!(
            !gold.evaluation_result.reasoning.contains("failed"),
            "Gold request should retain a fresh timeout: {}",
            gold.evaluation_result.reasoning
        );
        auto_answer_handle
            .await
            .expect("queued auto-answer joins")
            .expect("auto-answer request should retain a fresh timeout");
    }
}
