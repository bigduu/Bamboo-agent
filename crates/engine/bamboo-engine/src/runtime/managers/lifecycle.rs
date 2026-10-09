use async_trait::async_trait;
use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{AgentError, AgentEvent, Session};
use bamboo_domain::AgentRuntimeState;
use bamboo_llm::LLMProvider;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::runtime::config::AgentLoopConfig;
use crate::runtime::task_context::TaskLoopContext;

/// Manages agent run state machine and round transitions.
#[async_trait]
pub trait LifecycleManager: Send + Sync {
    /// Initialize runtime state for a new agent run.
    fn initialize_run(&self, session: &Session, config: &AgentLoopConfig) -> AgentRuntimeState;

    /// Prepare for a new round. Returns the round ID.
    ///
    /// `max_rounds` is the run's round cap; `None` = unlimited.
    #[allow(clippy::too_many_arguments)]
    async fn prepare_round(
        &self,
        session: &mut Session,
        task_context: &mut Option<TaskLoopContext>,
        runtime_state: &mut AgentRuntimeState,
        round: usize,
        max_rounds: Option<usize>,
        config: &AgentLoopConfig,
        cancel_token: &CancellationToken,
        metrics_collector: Option<&bamboo_metrics::MetricsCollector>,
        session_id: &str,
        model_name: &str,
        tools: &dyn ToolExecutor,
        llm: &dyn LLMProvider,
    ) -> Result<String, AgentError>;

    /// Optional data-only companion. Older implementations execute exactly once.
    async fn prepare_round_with_observation(
        &self,
        session: &mut Session,
        task_context: &mut Option<TaskLoopContext>,
        runtime_state: &mut AgentRuntimeState,
        round: usize,
        max_rounds: Option<usize>,
        config: &AgentLoopConfig,
        cancel_token: &CancellationToken,
        metrics_collector: Option<&bamboo_metrics::MetricsCollector>,
        session_id: &str,
        model_name: &str,
        tools: &dyn ToolExecutor,
        llm: &dyn LLMProvider,
    ) -> Result<ObservedRoundPreparation, AgentError> {
        let round_id = self
            .prepare_round(
                session,
                task_context,
                runtime_state,
                round,
                max_rounds,
                config,
                cancel_token,
                metrics_collector,
                session_id,
                model_name,
                tools,
                llm,
            )
            .await?;
        Ok(ObservedRoundPreparation {
            round_id,
            observation: InputObservation::default(),
        })
    }

    /// Handle post-round processing and determine next action.
    /// Returns `true` if the agent run should break out of the round loop.
    async fn handle_round_outcome(
        &self,
        session: &mut Session,
        runtime_state: &mut AgentRuntimeState,
        task_context: &mut Option<TaskLoopContext>,
        round: usize,
        should_break: bool,
    ) -> Result<bool, AgentError>;

    /// Finalize the agent run.
    #[allow(clippy::too_many_arguments)]
    async fn finalize_run(
        &self,
        session: &mut Session,
        runtime_state: &mut AgentRuntimeState,
        event_tx: &mpsc::Sender<AgentEvent>,
        session_id: &str,
        config: &AgentLoopConfig,
        metrics_collector: Option<&bamboo_metrics::MetricsCollector>,
        task_context: Option<TaskLoopContext>,
    );
}

pub use crate::runtime::runner::state_bridge::{
    project_input_request_batch, BorrowedInputRequestRecord,
};

/// Original round result plus opaque, untrusted execution-local input data.
#[derive(Debug)]
pub struct ObservedRoundPreparation {
    pub round_id: String,
    pub observation: InputObservation,
}

/// Only checked admission creates positive data; this is never a Skill grant.
/// The caller owns finite current data and supplies its existing run identity.
#[derive(Debug, Default)]
pub struct InputObservation(ObservationState);

#[derive(Debug, Default)]
enum ObservationState {
    #[default]
    Unavailable,
    SuccessfulNoNewInput(BoundedInputRequestBatch),
    New(BoundedInputRequestBatch),
}

impl InputObservation {
    pub(crate) fn projected(projection: InputRequestProjection) -> Self {
        Self(match projection {
            InputRequestProjection::Available(batch) if batch.records.is_empty() => {
                ObservationState::SuccessfulNoNewInput(batch)
            }
            InputRequestProjection::Available(batch) => ObservationState::New(batch),
            InputRequestProjection::Unavailable(_) => ObservationState::Unavailable,
        })
    }

    pub fn new_inputs(&self) -> Option<&BoundedInputRequestBatch> {
        match &self.0 {
            ObservationState::New(batch) => Some(batch),
            _ => None,
        }
    }

    pub fn is_unavailable(&self) -> bool {
        matches!(self.0, ObservationState::Unavailable)
    }

    pub fn is_successful_no_new_input(&self) -> bool {
        matches!(self.0, ObservationState::SuccessfulNoNewInput(_))
    }

    pub(crate) fn matches_session(&self, session_id: &str) -> bool {
        match &self.0 {
            ObservationState::New(batch) | ObservationState::SuccessfulNoNewInput(batch) => {
                batch.session_id == session_id
            }
            ObservationState::Unavailable => false,
        }
    }

    /// Move a whole new batch, retain only same-run NoNew, or clear on unknown.
    /// A new input with no request replaces the old IDs and selections too.
    pub fn update_current(
        self,
        current: &mut Option<BoundedInputRequestBatch>,
        session_id: &str,
        execution_id: &str,
    ) {
        let matches = |batch: &BoundedInputRequestBatch| {
            batch.session_id == session_id && batch.execution_id == execution_id
        };
        match self.0 {
            ObservationState::New(batch) if matches(&batch) => *current = Some(batch),
            ObservationState::SuccessfulNoNewInput(batch)
                if matches(&batch) && current.as_ref().is_none_or(matches) => {}
            _ => *current = None,
        }
    }
}

/// Untrusted request data only; neither current-input evidence nor permission.
#[derive(Debug, PartialEq)]
pub struct ProjectedInputRequest {
    pub input_id: String,
    pub source: bamboo_domain::SessionMessageSource,
    pub kind: bamboo_domain::SessionMessageKind,
    pub wrapper: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub request: Option<bamboo_domain::SessionSkillRequest>,
}

/// One execution-private data value. No live consumer is connected here.
/// Compact bytes bound the measurement, not total AST allocation or RSS.
///
/// ```
/// use bamboo_engine::runtime::managers::lifecycle::{project_input_request_batch, InputRequestProjection};
/// let InputRequestProjection::Available(batch) = project_input_request_batch("s", "e", &[]) else { panic!() };
/// assert!(batch.records().is_empty());
/// ```
///
/// ```compile_fail
/// use bamboo_engine::runtime::managers::lifecycle::BoundedInputRequestBatch;
/// fn requires_clone<T: Clone>() {}
/// requires_clone::<BoundedInputRequestBatch>();
/// ```
///
/// ```compile_fail
/// use bamboo_engine::runtime::managers::lifecycle::BoundedInputRequestBatch;
/// fn wire<T: serde::Serialize>() {}
/// wire::<BoundedInputRequestBatch>();
/// ```
#[derive(Debug, PartialEq)]
pub struct BoundedInputRequestBatch {
    pub(crate) session_id: String,
    pub(crate) execution_id: String,
    pub(crate) records: Box<[ProjectedInputRequest]>,
    pub(crate) compact_bytes: usize,
}

impl BoundedInputRequestBatch {
    pub fn session_id(&self) -> &str {
        &self.session_id
    }
    pub fn execution_id(&self) -> &str {
        &self.execution_id
    }
    pub fn records(&self) -> &[ProjectedInputRequest] {
        &self.records
    }
    pub fn compact_bytes(&self) -> usize {
        self.compact_bytes
    }
}

tokio::task_local! {
    static INPUT_REQUEST_DATA: Option<BoundedInputRequestBatch>;
}

/// Lend one existing, uniquely owned input-data batch during a future's polls.
///
/// The optional batch is moved, never cloned or persisted. A synchronous reader
/// can borrow it only in this scope. Normal completion, including an error
/// result, returns the original owner. Dropping or unwinding the future drops
/// that owner and restores the surrounding scope; it cannot resume the cancelled
/// work. `None` shadows an outer value. Detached tasks do not inherit the scope;
/// explicitly moving the whole scoped future carries only its own value.
///
/// This is unwired data transport, not an admission seal, caller identity, Source
/// lease or Skill permission. The host must apply `InputObservation::update_current`
/// between rounds and independently validate current authority before each use.
/// No `Send` bound is imposed on inline futures.
///
/// ```
/// use bamboo_engine::runtime::managers::lifecycle::{
///     project_input_request_batch, scope_input_request_data,
///     with_scoped_input_request_data, InputRequestProjection,
/// };
/// # async fn example() {
/// let InputRequestProjection::Available(batch) = project_input_request_batch("s", "e", &[]) else { panic!() };
/// let (count, returned) = scope_input_request_data(Some(batch), async {
///     with_scoped_input_request_data("s", "e", |batch| batch.records().len())
/// }).await;
/// assert_eq!(count, Some(0));
/// assert!(returned.is_some());
/// # }
/// ```
pub async fn scope_input_request_data<F: std::future::Future>(
    batch: Option<BoundedInputRequestBatch>,
    future: F,
) -> (F::Output, Option<BoundedInputRequestBatch>) {
    let mut scoped = Box::pin(INPUT_REQUEST_DATA.scope(batch, future));
    let output = scoped.as_mut().await;
    let batch = scoped
        .as_mut()
        .take_value()
        .expect("completed input-data scope retains its unique owner");
    (output, batch)
}

/// Synchronously borrow data for exactly the supplied Session and execution.
///
/// Absent data, empty IDs or either mismatch returns `None` without calling the
/// reader. Matching IDs identify data only; they authenticate no caller and do
/// not turn projected/unsealed data into New input. Return an independently
/// owned result; do not poll another scoped future while holding this borrow.
///
/// ```compile_fail
/// use bamboo_engine::runtime::managers::lifecycle::{
///     with_scoped_input_request_data, BoundedInputRequestBatch,
/// };
/// fn escape() -> &'static BoundedInputRequestBatch {
///     with_scoped_input_request_data("s", "e", |batch| batch).unwrap()
/// }
/// ```
///
/// ```compile_fail
/// use bamboo_engine::runtime::managers::lifecycle::with_scoped_input_request_data;
/// async fn escape_into_future() {
///     let future = with_scoped_input_request_data("s", "e", |batch| async move {
///         tokio::task::yield_now().await;
///         batch.records().len()
///     }).unwrap();
///     future.await;
/// }
/// ```
pub fn with_scoped_input_request_data<R>(
    session_id: &str,
    execution_id: &str,
    reader: impl FnOnce(&BoundedInputRequestBatch) -> R,
) -> Option<R> {
    if session_id.is_empty() || execution_id.is_empty() {
        return None;
    }
    INPUT_REQUEST_DATA
        .try_with(|batch| {
            batch
                .as_ref()
                .filter(|batch| {
                    batch.session_id == session_id && batch.execution_id == execution_id
                })
                .map(reader)
        })
        .ok()
        .flatten()
}

/// Embedded record/selection headers counted once, separate from dynamic data.
pub const INPUT_REQUEST_FIXED_SLOT_BYTES: usize = 128
    * std::mem::size_of::<ProjectedInputRequest>()
    + 4096 * std::mem::size_of::<bamboo_domain::SessionSkillSelection>()
    + std::mem::size_of::<BoundedInputRequestBatch>();

#[derive(Debug, PartialEq, Eq)]
pub enum InputRequestUnavailable {
    RecordLimit { count: usize, limit: usize },
    InvalidData,
    CompactLimit { limit: usize },
}

#[derive(Debug, PartialEq)]
pub enum InputRequestProjection {
    Available(BoundedInputRequestBatch),
    Unavailable(InputRequestUnavailable),
}

#[cfg(test)]
mod input_request_batch_tests {
    fn ql_observation(session: &str, execution: &str, id: Option<&str>) -> InputObservation {
        let source = bamboo_domain::SessionMessageSource::User;
        let records = id.map(|id| BorrowedInputRequestRecord {
            input_id: id,
            source: &source,
            kind: bamboo_domain::SessionMessageKind::UserInput,
            wrapper: None,
            created_at: chrono::DateTime::from_timestamp(1, 0).unwrap(),
            request: None,
        });
        InputObservation::projected(project_input_request_batch(
            session,
            execution,
            records.as_slice(),
        ))
    }

    #[test]
    fn ql_finite_owner_retains_nonew_only_for_exact_session_and_execution() {
        let mut current = None;
        ql_observation("s", "e", Some("N")).update_current(&mut current, "s", "e");
        let pointer = current.as_ref().unwrap().records().as_ptr();
        ql_observation("s", "e", None).update_current(&mut current, "s", "e");
        assert_eq!(current.as_ref().unwrap().records().as_ptr(), pointer);
        ql_observation("s", "old-execution", None).update_current(&mut current, "s", "e");
        assert!(
            current.is_none(),
            "foreign NoNew cannot preserve this owner's data"
        );
        ql_observation("foreign-session", "e", Some("wrong")).update_current(
            &mut current,
            "s",
            "e",
        );
        assert!(current.is_none());
        ql_observation("s", "e", Some("N")).update_current(&mut current, "s", "e");
        ql_observation("s", "e", None).update_current(&mut current, "successor", "e");
        assert!(current.is_none());
        ql_observation("s", "e", Some("N")).update_current(&mut current, "s", "e");
        InputObservation::default().update_current(&mut current, "s", "e");
        ql_observation("s", "e", None).update_current(&mut current, "s", "e");
        assert!(
            current.is_none(),
            "NoNew after Unavailable never resurrects N"
        );
    }

    use super::*;
    use crate::runtime::runner::state_bridge::{
        project_input_request_batch, BorrowedInputRequestRecord,
    };

    #[test]
    fn input_request_batch_compiled_fixed_slots_and_moves_preserve_one_owned_value() {
        let request = bamboo_domain::SessionSkillRequest {
            mode: None,
            selections: (0..32)
                .map(|i| bamboo_domain::SessionSkillSelection {
                    id: format!("{i:x}"),
                    source: "user".into(),
                    revision: 1,
                    args: serde_json::Value::Null,
                })
                .collect(),
        };
        let records = [BorrowedInputRequestRecord {
            input_id: "u",
            source: &bamboo_domain::SessionMessageSource::User,
            kind: bamboo_domain::SessionMessageKind::UserInput,
            wrapper: None,
            created_at: chrono::DateTime::from_timestamp(1, 0).unwrap(),
            request: Some(&request),
        }; 128];
        let make = |execution| match project_input_request_batch("s", execution, &records) {
            InputRequestProjection::Available(batch) => batch,
            _ => panic!("full slot fixture must actually fit"),
        };
        let mut owner = Some(make("old"));
        let old_pointer = owner.as_ref().unwrap().records().as_ptr();
        let prospective = make("new");
        let new_pointer = prospective.records().as_ptr();
        assert_ne!(old_pointer, new_pointer);
        let old = owner.replace(prospective).unwrap();
        assert_eq!(old.records().as_ptr(), old_pointer);
        assert_eq!(owner.as_ref().unwrap().records().as_ptr(), new_pointer);
        drop(old);
        let batch = owner.take().unwrap();
        assert!(owner.is_none());
        assert_eq!(batch.execution_id(), "new");
        assert_eq!(batch.records().len(), 128);
        assert_eq!(
            batch
                .records
                .iter()
                .map(|r| r.request.as_ref().unwrap().selections.capacity())
                .sum::<usize>(),
            4096
        );
        assert_eq!(
            INPUT_REQUEST_FIXED_SLOT_BYTES,
            128 * std::mem::size_of::<ProjectedInputRequest>()
                + 4096 * std::mem::size_of::<bamboo_domain::SessionSkillSelection>()
                + std::mem::size_of::<BoundedInputRequestBatch>()
        );
        println!(
            "compiled fixed slots={} Record={} Selection={} Batch={}",
            INPUT_REQUEST_FIXED_SLOT_BYTES,
            std::mem::size_of::<ProjectedInputRequest>(),
            std::mem::size_of::<bamboo_domain::SessionSkillSelection>(),
            std::mem::size_of::<BoundedInputRequestBatch>()
        );
    }
}

#[cfg(test)]
mod input_request_scope_tests {
    use super::*;
    use bamboo_agent_core::tools::{
        observed_tool_output_cap, scope_tool_output_cap, FunctionCall, ToolCall, ToolCtx,
        ToolError, ToolResult, ToolSchema,
    };
    use futures::{future::join_all, FutureExt};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    fn batch(session: &str, execution: &str, id: &str) -> BoundedInputRequestBatch {
        let request = bamboo_domain::SessionSkillRequest {
            mode: Some("plan".into()),
            selections: vec![bamboo_domain::SessionSkillSelection {
                id: "untrusted-selection".into(),
                source: "user".into(),
                revision: 7,
                args: serde_json::json!({"text": "bounded arguments"}),
            }],
        };
        let records = [BorrowedInputRequestRecord {
            input_id: id,
            source: &bamboo_domain::SessionMessageSource::User,
            kind: bamboo_domain::SessionMessageKind::UserInput,
            wrapper: None,
            created_at: chrono::DateTime::from_timestamp(1, 0).unwrap(),
            request: Some(&request),
        }];
        let InputRequestProjection::Available(batch) =
            project_input_request_batch(session, execution, &records)
        else {
            panic!("bounded fixture must fit")
        };
        batch
    }

    fn pointer(session: &str, execution: &str) -> Option<usize> {
        with_scoped_input_request_data(session, execution, |batch| {
            batch.records().as_ptr() as usize
        })
    }

    #[tokio::test]
    async fn owned_scope_pending_and_ready_return_the_same_batch_and_args_allocations() {
        let original = batch("s", "e", "N");
        let records = original.records().as_ptr() as usize;
        let args = original.records()[0].request.as_ref().unwrap().selections[0].args["text"]
            .as_str()
            .unwrap()
            .as_ptr() as usize;
        let bytes = original.compact_bytes();
        let mut future = Box::pin(scope_input_request_data(Some(original), async {
            assert_eq!(pointer("s", "e"), Some(records));
            tokio::task::yield_now().await;
            with_scoped_input_request_data("s", "e", |batch| {
                assert_eq!(batch.compact_bytes(), bytes);
                assert_eq!(batch.records()[0].input_id, "N");
                let request = batch.records()[0].request.as_ref().unwrap();
                assert_eq!(request.mode.as_deref(), Some("plan"));
                assert_eq!(
                    request.selections[0].args["text"]
                        .as_str()
                        .unwrap()
                        .as_ptr() as usize,
                    args
                );
                43
            })
        }));
        assert!(futures::poll!(future.as_mut()).is_pending());
        assert_eq!(pointer("s", "e"), None, "restored between polls");
        let (result, returned) = future.await;
        assert_eq!(result, Some(43));
        assert_eq!(returned.unwrap().records().as_ptr() as usize, records);
        assert_eq!(pointer("s", "e"), None);
    }

    #[tokio::test]
    async fn owned_scope_absent_and_mismatched_ids_never_call_the_reader() {
        let calls = AtomicUsize::new(0);
        assert_eq!(
            with_scoped_input_request_data("s", "e", |_| calls.fetch_add(1, Ordering::SeqCst)),
            None
        );
        scope_input_request_data(Some(batch("s", "e", "N")), async {
            for (session, execution) in [
                ("", "e"),
                ("s", ""),
                ("other", "e"),
                ("s", "other"),
                (" s", "e"),
            ] {
                assert_eq!(
                    with_scoped_input_request_data(session, execution, |_| calls
                        .fetch_add(1, Ordering::SeqCst)),
                    None
                );
            }
            assert!(pointer("s", "e").is_some());
        })
        .await;
        scope_input_request_data(Some(batch("", "", "N")), async {
            assert_eq!(pointer("", ""), None);
        })
        .await;
        scope_input_request_data(None, async {
            assert_eq!(pointer("s", "e"), None);
        })
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn owned_scope_nested_values_and_explicit_absence_restore_the_outer_value() {
        let original = batch("s", "e", "outer");
        let outer = original.records().as_ptr() as usize;
        let (_, returned) = scope_input_request_data(Some(original), async {
            let inner = batch("s", "inner", "inner");
            let inner_pointer = inner.records().as_ptr() as usize;
            let (_, inner) = scope_input_request_data(Some(inner), async {
                assert_eq!(pointer("s", "e"), None);
                assert_eq!(pointer("s", "inner"), Some(inner_pointer));
                tokio::task::yield_now().await;
                assert_eq!(pointer("s", "inner"), Some(inner_pointer));
            })
            .await;
            assert_eq!(inner.unwrap().records().as_ptr() as usize, inner_pointer);
            assert_eq!(pointer("s", "e"), Some(outer));
            let (_, absent) = scope_input_request_data(None, async {
                assert_eq!(pointer("s", "e"), None);
                tokio::task::yield_now().await;
                assert_eq!(pointer("s", "e"), None);
            })
            .await;
            assert!(absent.is_none());
            assert_eq!(pointer("s", "e"), Some(outer));
        })
        .await;
        assert_eq!(returned.unwrap().records().as_ptr() as usize, outer);
    }

    struct DataExecutor {
        entered: tokio::sync::Barrier,
    }

    #[async_trait]
    impl ToolExecutor for DataExecutor {
        async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
            let mut ctx = ToolCtx::none(call.id.clone());
            ctx.session_id = Some("s".into());
            let before = pointer("s", "e").expect("real executor future sees the owned batch");
            let cap = observed_tool_output_cap(&ctx).unwrap();
            self.entered.wait().await;
            tokio::task::yield_now().await;
            assert_eq!(pointer("s", "e"), Some(before));
            assert_eq!(observed_tool_output_cap(&ctx), Some(cap));
            Ok(ToolResult {
                success: true,
                result: serde_json::json!({"pointer": before, "cap": cap}).to_string(),
                ..Default::default()
            })
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            Vec::new()
        }
    }

    #[tokio::test]
    async fn owned_scope_real_inline_executor_futures_share_data_with_independent_dispatch_caps() {
        let executor = DataExecutor {
            entered: tokio::sync::Barrier::new(2),
        };
        let original = batch("s", "e", "N");
        let records = original.records().as_ptr() as usize;
        let calls = [7u32, 19].map(|cap| ToolCall {
            id: cap.to_string(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "data-probe".into(),
                arguments: "{}".into(),
            },
        });
        let (results, returned) = scope_input_request_data(Some(original), async {
            join_all(calls.iter().zip([7, 19]).map(|(call, cap)| {
                scope_tool_output_cap("s", &call.id, Some(cap), executor.execute(call))
            }))
            .await
        })
        .await;
        for (result, cap) in results.into_iter().zip([7, 19]) {
            let value: serde_json::Value = serde_json::from_str(&result.unwrap().result).unwrap();
            assert_eq!(value["pointer"], records);
            assert_eq!(value["cap"], cap);
        }
        assert_eq!(returned.unwrap().records().as_ptr() as usize, records);
        assert_eq!(pointer("s", "e"), None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn owned_scope_whole_send_futures_with_identical_ids_stay_task_local() {
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let mut tasks = Vec::new();
        for id in ["A", "B"] {
            let barrier = barrier.clone();
            let original = batch("s", "e", id);
            let records = original.records().as_ptr() as usize;
            tasks.push(tokio::spawn(scope_input_request_data(
                Some(original),
                async move {
                    barrier.wait().await;
                    for _ in 0..10 {
                        tokio::task::yield_now().await;
                        assert_eq!(pointer("s", "e"), Some(records));
                        assert_eq!(
                            with_scoped_input_request_data("s", "e", |batch| batch.records()[0]
                                .input_id
                                .clone())
                            .as_deref(),
                            Some(id)
                        );
                    }
                    records
                },
            )));
        }
        for task in tasks {
            let (records, returned) = task.await.unwrap();
            assert_eq!(returned.unwrap().records().as_ptr() as usize, records);
        }
        assert_eq!(pointer("s", "e"), None);
    }

    #[tokio::test]
    async fn owned_scope_unwrapped_spawn_and_spawn_blocking_do_not_inherit_data() {
        scope_input_request_data(Some(batch("s", "e", "N")), async {
            let records = pointer("s", "e");
            assert!(records.is_some());
            assert_eq!(
                tokio::spawn(async { pointer("s", "e") }).await.unwrap(),
                None
            );
            assert_eq!(
                tokio::task::spawn_blocking(|| pointer("s", "e"))
                    .await
                    .unwrap(),
                None
            );
            assert_eq!(pointer("s", "e"), records);
        })
        .await;
    }

    #[tokio::test]
    async fn owned_scope_inline_non_send_future_is_supported() {
        let local = std::rc::Rc::new(41);
        let (result, returned) = scope_input_request_data(Some(batch("s", "e", "N")), async {
            let local = local.clone();
            tokio::task::yield_now().await;
            assert!(pointer("s", "e").is_some());
            *local
        })
        .await;
        assert_eq!(result, 41);
        assert!(returned.is_some());
        assert_eq!(pointer("s", "e"), None);
    }

    struct DropProbe(Arc<AtomicUsize>);
    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn owned_scope_cancellation_drops_work_once_and_restores_outer_data() {
        let drops = Arc::new(AtomicUsize::new(0));
        let probe = DropProbe(drops.clone());
        let (_, returned) = scope_input_request_data(Some(batch("s", "outer", "outer")), async {
            let outer = pointer("s", "outer");
            let mut cancelled = Box::pin(scope_input_request_data(
                Some(batch("s", "e", "cancelled")),
                async move {
                    let _probe = probe;
                    assert!(pointer("s", "e").is_some());
                    assert_eq!(pointer("s", "outer"), None);
                    std::future::pending::<()>().await;
                },
            ));
            assert!(futures::poll!(cancelled.as_mut()).is_pending());
            assert_eq!(pointer("s", "e"), None);
            assert_eq!(pointer("s", "outer"), outer);
            drop(cancelled);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert_eq!(pointer("s", "e"), None);
            assert_eq!(pointer("s", "outer"), outer);
        })
        .await;
        assert!(returned.is_some());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(pointer("s", "outer"), None);
    }

    #[tokio::test]
    async fn owned_scope_unwind_drops_work_once_and_restores_outer_data() {
        let drops = Arc::new(AtomicUsize::new(0));
        let probe = DropProbe(drops.clone());
        scope_input_request_data(Some(batch("s", "outer", "outer")), async {
            let outer = pointer("s", "outer");
            let result = std::panic::AssertUnwindSafe(scope_input_request_data(
                Some(batch("s", "e", "panicked")),
                async move {
                    let _probe = probe;
                    tokio::task::yield_now().await;
                    assert!(pointer("s", "e").is_some());
                    panic!("intentional scoped-work panic");
                },
            ))
            .catch_unwind()
            .await;
            assert!(result.is_err());
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert_eq!(pointer("s", "e"), None);
            assert_eq!(pointer("s", "outer"), outer);
        })
        .await;
        assert_eq!(pointer("s", "outer"), None);
    }

    #[tokio::test]
    async fn owned_scope_error_result_returns_ownership_and_never_retains_a_scope() {
        let original = batch("s", "e", "N");
        let records = original.records().as_ptr() as usize;
        let (result, returned) = scope_input_request_data(Some(original), async {
            tokio::task::yield_now().await;
            Err::<(), _>("real error result")
        })
        .await;
        assert_eq!(result, Err("real error result"));
        assert_eq!(returned.unwrap().records().as_ptr() as usize, records);
        assert_eq!(pointer("s", "e"), None);
    }

    #[tokio::test]
    async fn owned_scope_round_updates_retain_nonew_replace_new_and_clear_unavailable() {
        let mut current = None;
        InputObservation::projected(InputRequestProjection::Available(batch("s", "e", "N")))
            .update_current(&mut current, "s", "e");
        let original = current.as_ref().unwrap().records().as_ptr() as usize;
        let (_, returned) = scope_input_request_data(current.take(), async {
            assert_eq!(pointer("s", "e"), Some(original));
        })
        .await;
        current = returned;
        InputObservation::projected(project_input_request_batch("s", "e", &[])).update_current(
            &mut current,
            "s",
            "e",
        );
        assert_eq!(
            current.as_ref().unwrap().records().as_ptr() as usize,
            original
        );
        let mut newer = batch("s", "e", "N2");
        newer.records[0].request = None;
        InputObservation::projected(InputRequestProjection::Available(newer)).update_current(
            &mut current,
            "s",
            "e",
        );
        let (_, returned) = scope_input_request_data(current.take(), async {
            with_scoped_input_request_data("s", "e", |batch| {
                assert_eq!(batch.records()[0].input_id, "N2");
                assert!(batch.records()[0].request.is_none());
            })
            .unwrap();
        })
        .await;
        current = returned;
        InputObservation::default().update_current(&mut current, "s", "e");
        InputObservation::projected(project_input_request_batch("s", "e", &[])).update_current(
            &mut current,
            "s",
            "e",
        );
        let (_, returned) = scope_input_request_data(current, async {
            assert_eq!(
                pointer("s", "e"),
                None,
                "NoNew cannot resurrect cleared data"
            );
        })
        .await;
        assert!(returned.is_none());
    }

    #[tokio::test]
    async fn owned_scope_aborting_a_real_task_drops_work_without_inheriting_data() {
        let drops = Arc::new(AtomicUsize::new(0));
        let probe = DropProbe(drops.clone());
        let entered = Arc::new(tokio::sync::Notify::new());
        let task_entered = entered.clone();
        let task = tokio::spawn(scope_input_request_data(
            Some(batch("s", "e", "aborted")),
            async move {
                let _probe = probe;
                assert!(pointer("s", "e").is_some());
                task_entered.notify_one();
                std::future::pending::<()>().await;
            },
        ));
        entered.notified().await;
        assert_eq!(pointer("s", "e"), None);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(pointer("s", "e"), None);
    }

    #[tokio::test]
    async fn owned_scope_maximum_record_batch_remains_one_allocation() {
        let ids = (0..128).map(|i| format!("input-{i}")).collect::<Vec<_>>();
        let source = bamboo_domain::SessionMessageSource::User;
        let records = ids
            .iter()
            .map(|id| BorrowedInputRequestRecord {
                input_id: id,
                source: &source,
                kind: bamboo_domain::SessionMessageKind::UserInput,
                wrapper: None,
                created_at: chrono::DateTime::from_timestamp(1, 0).unwrap(),
                request: None,
            })
            .collect::<Vec<_>>();
        let InputRequestProjection::Available(original) =
            project_input_request_batch("s", "e", &records)
        else {
            panic!("maximum slots must fit")
        };
        let address = original.records().as_ptr() as usize;
        let (_, returned) = scope_input_request_data(Some(original), async {
            tokio::task::yield_now().await;
            with_scoped_input_request_data("s", "e", |batch| {
                assert_eq!(batch.records().len(), 128);
                assert_eq!(batch.records().as_ptr() as usize, address);
                assert_eq!(batch.records()[127].input_id, "input-127");
            })
            .unwrap();
        })
        .await;
        assert_eq!(returned.unwrap().records().as_ptr() as usize, address);
    }
}
