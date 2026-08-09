use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::sync::mpsc;

use crate::runtime::task_context::TaskLoopContext;
use crate::runtime::task_evaluation::{evaluate_task_progress_with_dispatch, TaskEvaluationResult};
use bamboo_agent_core::{AgentError, AgentEvent, Session, SessionKind};
use bamboo_domain::task::{TaskBlocker, TaskBlockerKind, TaskEvidence, TaskEvidenceKind};
use bamboo_domain::{ReasoningEffort, TaskItemStatus, TaskList};
use bamboo_llm::LLMProvider;
use bamboo_metrics::{MetricsCollector, TokenUsage as MetricsTokenUsage};

fn task_lists_match(current: &TaskList, expected: &TaskList) -> bool {
    match (
        serde_json::to_value(current),
        serde_json::to_value(expected),
    ) {
        (Ok(current), Ok(expected)) => current == expected,
        _ => false,
    }
}

fn normalize_criterion(value: &str) -> Option<String> {
    let normalized = value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_lowercase();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn parse_criterion_ref(value: &str) -> Option<usize> {
    let trimmed = value.trim().to_ascii_lowercase();
    let as_c_ref = trimmed
        .strip_prefix("criterion_")
        .or_else(|| trimmed.strip_prefix("criterion-"))
        .or_else(|| trimmed.strip_prefix('c'));
    if let Some(raw_index) = as_c_ref {
        return raw_index.parse::<usize>().ok().filter(|index| *index > 0);
    }
    None
}

fn missing_completion_criteria(required: &[String], criteria_met: &[String]) -> Vec<String> {
    let mut required_lookup = std::collections::HashMap::new();
    for (index, criterion) in required.iter().enumerate() {
        if let Some(normalized) = normalize_criterion(criterion) {
            required_lookup.insert(normalized, index + 1);
        }
    }

    let mut met_refs: HashSet<usize> = HashSet::new();
    for criterion in criteria_met {
        if let Some(index) = parse_criterion_ref(criterion) {
            met_refs.insert(index);
            continue;
        }
        if let Some(normalized) = normalize_criterion(criterion) {
            if let Some(index) = required_lookup.get(&normalized).copied() {
                met_refs.insert(index);
            }
        }
    }

    required
        .iter()
        .enumerate()
        .filter_map(|(index, criterion)| {
            if met_refs.contains(&(index + 1)) {
                return None;
            }
            Some(criterion.trim().to_string())
        })
        .collect()
}

fn append_text(existing: Option<String>, addition: &str) -> Option<String> {
    let addition = addition.trim();
    if addition.is_empty() {
        return existing;
    }
    match existing {
        Some(mut value) if !value.trim().is_empty() => {
            value.push('\n');
            value.push_str(addition);
            Some(value)
        }
        _ => Some(addition.to_string()),
    }
}

#[derive(Debug, Clone)]
pub(in crate::runtime::runner) struct TaskEvaluationApplyOutcome {
    pub(in crate::runtime::runner) usage: MetricsTokenUsage,
    pub(in crate::runtime::runner) applied_updates: usize,
    pub(in crate::runtime::runner) stale: bool,
}

impl TaskEvaluationApplyOutcome {
    pub(in crate::runtime::runner) fn stale(usage: MetricsTokenUsage) -> Self {
        Self {
            usage,
            applied_updates: 0,
            stale: true,
        }
    }
}

#[derive(Debug, Clone)]
pub(in crate::runtime::runner) struct AsyncTaskEvaluationRequest {
    pub(in crate::runtime::runner) evaluation_id: String,
    pub(in crate::runtime::runner) metrics_round_id: String,
    pub(in crate::runtime::runner) session_id: String,
    pub(in crate::runtime::runner) shared_session_id: String,
    pub(in crate::runtime::runner) round_number: usize,
    pub(in crate::runtime::runner) based_on_task_context_version: u64,
    pub(in crate::runtime::runner) based_on_task_list: TaskList,
    pub(in crate::runtime::runner) task_list_title: Option<String>,
    pub(in crate::runtime::runner) model_name: String,
    pub(in crate::runtime::runner) timeout_context:
        crate::runtime::stream::handler::StreamTimeoutContext,
    pub(in crate::runtime::runner) reasoning_effort: Option<ReasoningEffort>,
    pub(in crate::runtime::runner) task_context_snapshot: TaskLoopContext,
    pub(in crate::runtime::runner) session_snapshot: Session,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::runner) struct AsyncTaskEvaluationResult {
    pub(in crate::runtime::runner) metrics_round_id: String,
    pub(in crate::runtime::runner) shared_session_id: String,
    pub(in crate::runtime::runner) round_number: usize,
    pub(in crate::runtime::runner) based_on_task_context_version: u64,
    pub(in crate::runtime::runner) based_on_task_list: TaskList,
    pub(in crate::runtime::runner) task_list_title: Option<String>,
    pub(in crate::runtime::runner) evaluation_result: TaskEvaluationResult,
    pub(in crate::runtime::runner) finished_at: DateTime<Utc>,
    pub(in crate::runtime::runner) error: Option<String>,
    pub(in crate::runtime::runner) metrics_started: Arc<AtomicBool>,
    pub(in crate::runtime::runner) metrics_terminal: Arc<AtomicBool>,
}

pub(in crate::runtime::runner) fn build_async_task_evaluation_request(
    task_context: &Option<TaskLoopContext>,
    session: &Session,
    session_id: &str,
    round_number: usize,
    model_name: Option<&str>,
    reasoning_effort: Option<ReasoningEffort>,
    timeout_context: crate::runtime::stream::handler::StreamTimeoutContext,
) -> Result<Option<AsyncTaskEvaluationRequest>, AgentError> {
    let Some(task_context_snapshot) = task_context.clone() else {
        return Ok(None);
    };
    let Some(based_on_task_list) = session.task_list.clone() else {
        return Ok(None);
    };

    let model_name = model_name
        .ok_or_else(|| AgentError::LLM("model_name is required in AgentLoopConfig".to_string()))?;

    let shared_session_id = match session.kind {
        SessionKind::Child => session.root_session_id.clone(),
        SessionKind::Root => session.id.clone(),
    };

    let evaluation_id = crate::runtime::runner::round_prelude::new_execution_id();
    let metrics_round_id = format!("{session_id}-task-evaluation-{evaluation_id}");

    Ok(Some(AsyncTaskEvaluationRequest {
        evaluation_id,
        metrics_round_id,
        session_id: session_id.to_string(),
        shared_session_id,
        round_number,
        based_on_task_context_version: task_context_snapshot.version,
        based_on_task_list,
        task_list_title: session
            .task_list
            .as_ref()
            .map(|task_list| task_list.title.clone()),
        model_name: model_name.to_string(),
        timeout_context,
        reasoning_effort,
        task_context_snapshot,
        session_snapshot: session.clone(),
    }))
}

pub(in crate::runtime::runner) async fn execute_async_task_evaluation(
    request: AsyncTaskEvaluationRequest,
    llm: Arc<dyn LLMProvider>,
    event_tx: mpsc::Sender<AgentEvent>,
    configured_limit: usize,
    metrics_collector: Option<MetricsCollector>,
    metrics_started: Arc<AtomicBool>,
    metrics_terminal: Arc<AtomicBool>,
) -> AsyncTaskEvaluationResult {
    let budget_provider = llm.clone();
    let budget_model = request.model_name.clone();
    let acquire_dispatch_guard = async move {
        crate::runtime::runner::auxiliary_budget::acquire(
            &budget_provider,
            &budget_model,
            configured_limit,
        )
        .await
    };
    let metrics_for_dispatch = metrics_collector.clone();
    let metrics_round_id = request.metrics_round_id.clone();
    let metrics_session_id = request.session_id.clone();
    let metrics_model_name = request.model_name.clone();
    let metrics_started_for_dispatch = metrics_started.clone();
    let evaluation = evaluate_task_progress_with_dispatch(
        &request.task_context_snapshot,
        &request.session_snapshot,
        llm,
        &crate::runtime::task_evaluation::TaskEvaluationFrame {
            event_tx: &event_tx,
            session_id: &request.session_id,
            model: &request.model_name,
            reasoning_effort: request.reasoning_effort,
            timeout_context: &request.timeout_context,
        },
        acquire_dispatch_guard,
        move || {
            if let Some(metrics) = metrics_for_dispatch.as_ref() {
                metrics.round_started(
                    metrics_round_id,
                    metrics_session_id,
                    metrics_model_name,
                    Utc::now(),
                );
            }
            metrics_started_for_dispatch.store(true, Ordering::Release);
        },
    )
    .await;
    let finished_at = Utc::now();
    let (evaluation_result, error) = match evaluation {
        Ok(result) => {
            let error = result
                .reasoning
                .strip_prefix("Evaluation failed:")
                .map(|_| result.reasoning.clone());
            (result, error)
        }
        Err(error) => (
            TaskEvaluationResult {
                needs_evaluation: false,
                updates: Vec::new(),
                reasoning: format!("Evaluation failed: {error}"),
                prompt_tokens: 0,
                completion_tokens: 0,
            },
            Some(error.to_string()),
        ),
    };

    AsyncTaskEvaluationResult {
        metrics_round_id: request.metrics_round_id,
        shared_session_id: request.shared_session_id,
        round_number: request.round_number,
        based_on_task_context_version: request.based_on_task_context_version,
        based_on_task_list: request.based_on_task_list,
        task_list_title: request.task_list_title,
        evaluation_result,
        finished_at,
        error,
        metrics_started,
        metrics_terminal,
    }
}

pub(in crate::runtime::runner) fn apply_task_evaluation_result(
    task_context: &mut Option<TaskLoopContext>,
    session: &mut Session,
    session_id: &str,
    evaluation: AsyncTaskEvaluationResult,
) -> TaskEvaluationApplyOutcome {
    fn append_structured_fields_to_session_task(
        session: &mut Session,
        item_id: &str,
        evidence: Option<&str>,
        blocker: Option<&str>,
    ) -> bool {
        let Some(task_list) = session.task_list.as_mut() else {
            return false;
        };
        let Some(item) = task_list.items.iter_mut().find(|item| item.id == item_id) else {
            return false;
        };

        let mut changed = false;
        if let Some(summary) = evidence
            .map(str::trim)
            .filter(|summary| !summary.is_empty())
        {
            let previous_len = item.evidence.len();
            item.push_evidence(TaskEvidence {
                kind: TaskEvidenceKind::Observation,
                summary: summary.to_string(),
                reference: None,
                tool_name: None,
                tool_call_id: None,
                round: None,
                success: None,
            });
            changed |= item.evidence.len() != previous_len;
        }
        if let Some(summary) = blocker.map(str::trim).filter(|summary| !summary.is_empty()) {
            let previous_len = item.blockers.len();
            item.add_blocker(TaskBlocker {
                kind: TaskBlockerKind::Unknown,
                summary: summary.to_string(),
                waiting_on: None,
            });
            changed |= item.blockers.len() != previous_len;
        }
        if changed {
            task_list.updated_at = chrono::Utc::now();
            session.updated_at = chrono::Utc::now();
        }
        changed
    }

    let mut usage = MetricsTokenUsage::default();
    usage.prompt_tokens = evaluation.evaluation_result.prompt_tokens;
    usage.completion_tokens = evaluation.evaluation_result.completion_tokens;
    usage.total_tokens = usage.prompt_tokens.saturating_add(usage.completion_tokens);

    let Some(ctx) = task_context.as_mut() else {
        tracing::debug!(
            "[{}] Dropping async task evaluation for round {} because task context no longer exists",
            session_id,
            evaluation.round_number
        );
        return TaskEvaluationApplyOutcome::stale(usage);
    };

    let session_matches_snapshot = session
        .task_list
        .as_ref()
        .is_some_and(|task_list| task_lists_match(task_list, &evaluation.based_on_task_list));
    let context_matches_snapshot = task_lists_match(
        &ctx.to_task_list_with_title(evaluation.based_on_task_list.title.clone()),
        &evaluation.based_on_task_list,
    );
    if ctx.version != evaluation.based_on_task_context_version
        || !session_matches_snapshot
        || !context_matches_snapshot
    {
        tracing::debug!(
            "[{}] Dropping stale async task evaluation for round {} (snapshot version={}, current version={}, session_snapshot_match={}, context_snapshot_match={})",
            session_id,
            evaluation.round_number,
            evaluation.based_on_task_context_version,
            ctx.version,
            session_matches_snapshot,
            context_matches_snapshot,
        );
        return TaskEvaluationApplyOutcome::stale(usage);
    }

    if evaluation.evaluation_result.needs_evaluation
        && !evaluation.evaluation_result.updates.is_empty()
    {
        tracing::info!(
            "[{}] Applying async task evaluation from round {} with {} update(s)",
            session_id,
            evaluation.round_number,
            evaluation.evaluation_result.updates.len()
        );
    }

    let mut applied_updates = 0usize;
    for update in evaluation.evaluation_result.updates {
        let mut status = update.status.clone();
        let mut notes = update.notes.clone();
        let mut evidence = update.evidence.clone();
        let blocker = update.blocker.clone();
        if matches!(status, TaskItemStatus::Completed) {
            let required_criteria = session
                .task_list
                .as_ref()
                .and_then(|task_list| {
                    task_list
                        .items
                        .iter()
                        .find(|item| item.id == update.item_id)
                        .map(|item| item.completion_criteria.clone())
                })
                .unwrap_or_default();
            if !required_criteria.is_empty() {
                let criteria_met = update.criteria_met.clone().unwrap_or_default();
                let missing = missing_completion_criteria(&required_criteria, &criteria_met);
                if !missing.is_empty() {
                    status = TaskItemStatus::InProgress;
                    let gate_note = format!(
                        "Completion criteria not fully met; keeping task in_progress. Missing: {}",
                        missing.join(" | ")
                    );
                    notes = append_text(notes, &gate_note);
                    let gate_evidence = if criteria_met.is_empty() {
                        format!(
                            "No criteria were reported as met. Missing: {}",
                            missing.join(" | ")
                        )
                    } else {
                        format!(
                            "Criteria met: {} | Missing: {}",
                            criteria_met.join(" | "),
                            missing.join(" | ")
                        )
                    };
                    evidence = append_text(evidence, &gate_evidence);
                    tracing::debug!(
                        "[{}] Kept task {} in_progress due to unmet completion criteria",
                        session_id,
                        update.item_id
                    );
                }
            }
        }

        let ctx_changed = ctx.apply_evaluated_update(
            &update.item_id,
            status.clone(),
            notes.as_deref(),
            evidence.as_deref(),
            blocker.as_deref(),
        );
        let previous_session_updated_at = session.updated_at;
        let _ = session.update_task_item(
            &update.item_id,
            status,
            notes.as_deref(),
            update.criteria_met.as_deref(),
        );
        let session_update_changed = session.updated_at != previous_session_updated_at;
        let structured_changed = append_structured_fields_to_session_task(
            session,
            &update.item_id,
            evidence.as_deref(),
            blocker.as_deref(),
        );

        if ctx_changed || session_update_changed || structured_changed {
            applied_updates += 1;
        }
    }

    TaskEvaluationApplyOutcome {
        usage,
        applied_updates,
        stale: false,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        apply_task_evaluation_result, build_async_task_evaluation_request,
        execute_async_task_evaluation, missing_completion_criteria, AsyncTaskEvaluationResult,
    };
    use crate::runtime::task_context::TaskLoopContext;
    use bamboo_agent_core::{AgentEvent, Message, Session, ToolSchema};
    use bamboo_domain::task::{TaskPhase, TaskPriority};
    use bamboo_domain::{TaskItem, TaskItemStatus, TaskList};
    use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMStream};
    use chrono::Utc;
    use futures::stream;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    fn session_with_single_in_progress_task() -> (Session, TaskLoopContext) {
        let mut session = Session::new("task-eval-session", "model");
        session.set_task_list(TaskList {
            session_id: "task-eval-session".to_string(),
            title: "Eval Tasks".to_string(),
            items: vec![TaskItem {
                id: "task-1".to_string(),
                description: "Implement async task evaluation".to_string(),
                status: TaskItemStatus::InProgress,
                phase: TaskPhase::Execution,
                priority: TaskPriority::High,
                ..TaskItem::default()
            }],
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });
        session
            .metadata
            .insert("task_list_version".to_string(), "3".to_string());
        let context = TaskLoopContext::from_session(&session).expect("task context should exist");
        (session, context)
    }

    struct BlockingEvaluationProvider {
        active: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
        entered: Arc<AtomicUsize>,
        release: Arc<tokio::sync::Semaphore>,
    }

    #[async_trait::async_trait]
    impl LLMProvider for BlockingEvaluationProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(active, Ordering::SeqCst);
            self.entered.fetch_add(1, Ordering::SeqCst);
            let permit = self
                .release
                .acquire()
                .await
                .expect("provider release gate stays open");
            permit.forget();
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(Box::pin(stream::iter(vec![Ok(LLMChunk::Done)])))
        }
    }

    #[tokio::test]
    async fn task_evaluations_from_multiple_sessions_obey_shared_provider_model_limit() {
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let provider: Arc<dyn LLMProvider> = Arc::new(BlockingEvaluationProvider {
            active: active.clone(),
            peak: peak.clone(),
            entered: entered.clone(),
            release: release.clone(),
        });
        let (event_tx, _event_rx) = tokio::sync::mpsc::channel::<AgentEvent>(32);
        let mut handles = Vec::new();

        for index in 0..6 {
            let session_id = format!("evaluation-session-{index}");
            let mut session = Session::new(&session_id, "main-model");
            let now = Utc::now();
            session.set_task_list(TaskList {
                session_id: session_id.clone(),
                title: "Shared-budget task".to_string(),
                items: vec![TaskItem {
                    id: "task-1".to_string(),
                    description: "evaluate me".to_string(),
                    status: TaskItemStatus::InProgress,
                    ..TaskItem::default()
                }],
                created_at: now,
                updated_at: now,
            });
            session.set_task_list_version_meta("1");
            let context = TaskLoopContext::from_session(&session);
            let request = build_async_task_evaluation_request(
                &context,
                &session,
                &session_id,
                1,
                Some("shared-fast-model"),
                None,
                crate::runtime::stream::handler::StreamTimeoutContext::default(),
            )
            .expect("request builds")
            .expect("context exists");
            let provider = provider.clone();
            let event_tx = event_tx.clone();
            handles.push(tokio::spawn(async move {
                execute_async_task_evaluation(
                    request,
                    provider,
                    event_tx,
                    2,
                    None,
                    Arc::new(AtomicBool::new(false)),
                    Arc::new(AtomicBool::new(false)),
                )
                .await
            }));
        }

        for expected_entered in [2, 4, 6] {
            tokio::time::timeout(Duration::from_secs(1), async {
                while entered.load(Ordering::SeqCst) < expected_entered {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("next cross-session evaluation batch dispatches");
            assert!(active.load(Ordering::SeqCst) <= 2);
            assert_eq!(peak.load(Ordering::SeqCst), 2);
            release.add_permits(2);
        }

        let mut metric_ids = std::collections::HashSet::new();
        for handle in handles {
            let result = handle.await.expect("evaluation session joins");
            assert!(result.metrics_started.load(Ordering::Acquire));
            assert!(metric_ids.insert(result.metrics_round_id));
        }
        assert_eq!(peak.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn apply_task_evaluation_result_rejects_stale_results() {
        let (mut session, mut context) = session_with_single_in_progress_task();
        context.version = 5;
        let based_on_task_list = session.task_list.clone().expect("task list");

        let outcome = apply_task_evaluation_result(
            &mut Some(context.clone()),
            &mut session,
            "task-eval-session",
            AsyncTaskEvaluationResult {
                metrics_round_id: "stale-evaluation".to_string(),
                shared_session_id: "task-eval-session".to_string(),
                round_number: 2,
                based_on_task_context_version: 4,
                based_on_task_list,
                task_list_title: Some("Eval Tasks".to_string()),
                evaluation_result: crate::runtime::task_evaluation::TaskEvaluationResult {
                    needs_evaluation: true,
                    updates: vec![crate::runtime::task_evaluation::TaskItemUpdate {
                        item_id: "task-1".to_string(),
                        status: TaskItemStatus::Completed,
                        notes: Some("done".to_string()),
                        evidence: Some("tests passed".to_string()),
                        blocker: None,
                        criteria_met: None,
                    }],
                    reasoning: "complete".to_string(),
                    prompt_tokens: 12,
                    completion_tokens: 6,
                },
                finished_at: Utc::now(),
                error: None,
                metrics_started: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
                metrics_terminal: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            },
        );

        assert!(outcome.stale);
        assert_eq!(outcome.applied_updates, 0);
        assert_eq!(outcome.usage.prompt_tokens, 12);
        assert_eq!(
            session.task_list.as_ref().unwrap().items[0].status,
            TaskItemStatus::InProgress
        );
    }

    #[test]
    fn apply_task_evaluation_result_rejects_same_version_divergent_snapshot() {
        let (mut session, original_context) = session_with_single_in_progress_task();
        let based_on_task_list = session.task_list.clone().expect("task list");
        session.task_list.as_mut().expect("live task list").items[0].description =
            "Competing same-generation winner".to_string();
        let winner_context =
            TaskLoopContext::from_session(&session).expect("winner context should exist");
        assert_eq!(winner_context.version, original_context.version);
        let mut maybe_context = Some(winner_context);

        let outcome = apply_task_evaluation_result(
            &mut maybe_context,
            &mut session,
            "task-eval-session",
            AsyncTaskEvaluationResult {
                metrics_round_id: "same-version-stale-evaluation".to_string(),
                shared_session_id: "task-eval-session".to_string(),
                round_number: 2,
                based_on_task_context_version: original_context.version,
                based_on_task_list,
                task_list_title: Some("Eval Tasks".to_string()),
                evaluation_result: crate::runtime::task_evaluation::TaskEvaluationResult {
                    needs_evaluation: true,
                    updates: vec![crate::runtime::task_evaluation::TaskItemUpdate {
                        item_id: "task-1".to_string(),
                        status: TaskItemStatus::Completed,
                        notes: Some("stale result must not apply".to_string()),
                        evidence: Some("stale evidence".to_string()),
                        blocker: None,
                        criteria_met: None,
                    }],
                    reasoning: "stale".to_string(),
                    prompt_tokens: 7,
                    completion_tokens: 3,
                },
                finished_at: Utc::now(),
                error: None,
                metrics_started: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
                metrics_terminal: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            },
        );

        assert!(outcome.stale);
        assert_eq!(outcome.applied_updates, 0);
        let live_item = &session.task_list.as_ref().expect("live task list").items[0];
        assert_eq!(live_item.status, TaskItemStatus::InProgress);
        assert_eq!(live_item.description, "Competing same-generation winner");
    }

    #[test]
    fn apply_task_evaluation_result_applies_matching_results() {
        let (mut session, context) = session_with_single_in_progress_task();
        let mut maybe_context = Some(context.clone());
        let based_on_task_list = session.task_list.clone().expect("task list");

        let outcome = apply_task_evaluation_result(
            &mut maybe_context,
            &mut session,
            "task-eval-session",
            AsyncTaskEvaluationResult {
                metrics_round_id: "matching-evaluation".to_string(),
                shared_session_id: "task-eval-session".to_string(),
                round_number: 2,
                based_on_task_context_version: context.version,
                based_on_task_list,
                task_list_title: Some("Eval Tasks".to_string()),
                evaluation_result: crate::runtime::task_evaluation::TaskEvaluationResult {
                    needs_evaluation: true,
                    updates: vec![crate::runtime::task_evaluation::TaskItemUpdate {
                        item_id: "task-1".to_string(),
                        status: TaskItemStatus::Completed,
                        notes: Some("done".to_string()),
                        evidence: Some("tests passed".to_string()),
                        blocker: None,
                        criteria_met: None,
                    }],
                    reasoning: "complete".to_string(),
                    prompt_tokens: 12,
                    completion_tokens: 6,
                },
                finished_at: Utc::now(),
                error: None,
                metrics_started: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
                metrics_terminal: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            },
        );

        assert!(!outcome.stale);
        assert_eq!(outcome.applied_updates, 1);
        assert_eq!(outcome.usage.total_tokens, 18);
        assert_eq!(
            session.task_list.as_ref().unwrap().items[0].status,
            TaskItemStatus::Completed
        );
        let updated_context = maybe_context.expect("context should still exist");
        assert!(updated_context.version > context.version);
        assert!(updated_context.items[0]
            .evidence
            .iter()
            .any(|entry| entry.summary.contains("tests passed")));
    }

    #[test]
    fn missing_completion_criteria_matches_case_and_whitespace_insensitively() {
        let required = vec![
            "All integration tests pass".to_string(),
            "No clippy warnings".to_string(),
        ];
        let met = vec![
            "  all   integration tests   pass  ".to_string(),
            "NO CLIPPY WARNINGS".to_string(),
        ];

        let missing = missing_completion_criteria(&required, &met);
        assert!(missing.is_empty());
    }

    #[test]
    fn missing_completion_criteria_reports_unmet_items() {
        let required = vec![
            "All integration tests pass".to_string(),
            "No clippy warnings".to_string(),
        ];
        let met = vec!["All integration tests pass".to_string()];

        let missing = missing_completion_criteria(&required, &met);
        assert_eq!(missing, vec!["No clippy warnings".to_string()]);
    }

    #[test]
    fn missing_completion_criteria_accepts_criterion_refs() {
        let required = vec![
            "All integration tests pass".to_string(),
            "No clippy warnings".to_string(),
        ];
        let met = vec!["c1".to_string(), "criterion_2".to_string()];

        let missing = missing_completion_criteria(&required, &met);
        assert!(missing.is_empty());
    }

    #[test]
    fn missing_completion_criteria_accepts_dash_style_criterion_ref() {
        let required = vec![
            "All integration tests pass".to_string(),
            "No clippy warnings".to_string(),
        ];
        let met = vec!["criterion-1".to_string(), "c2".to_string()];

        let missing = missing_completion_criteria(&required, &met);
        assert!(missing.is_empty());
    }
}
