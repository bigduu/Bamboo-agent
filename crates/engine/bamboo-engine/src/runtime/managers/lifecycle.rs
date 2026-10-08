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
        assert!(INPUT_REQUEST_FIXED_SLOT_BYTES > 0);
        println!(
            "compiled fixed slots={} Record={} Selection={} Batch={}",
            INPUT_REQUEST_FIXED_SLOT_BYTES,
            std::mem::size_of::<ProjectedInputRequest>(),
            std::mem::size_of::<bamboo_domain::SessionSkillSelection>(),
            std::mem::size_of::<BoundedInputRequestBatch>()
        );
    }
}
