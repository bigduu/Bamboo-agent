//! Versioned, durable orchestration workflow domain types.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A catalog-pinned orchestration definition. `revision` is immutable for a run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowRunDefinition {
    /// Unambiguous format discriminator. Phase 1 accepts only `1`.
    pub workflow_schema: u32,
    pub id: String,
    pub revision: u64,
    #[serde(default = "default_object_schema")]
    pub input_schema: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<Value>,
    pub steps: Vec<WorkflowStepDefinition>,
    pub plan: WorkflowPlan,
    #[serde(default)]
    pub budgets: WorkflowBudgets,
}

/// Immutable set of every definition reachable from one catalog publication.
/// A run persists this value before execution and never resolves the live
/// catalog again, eliminating definition TOCTOU across nested dispatch/restart.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowDefinitionBundle {
    pub publication_revision: u64,
    pub root_id: String,
    pub root_revision: u64,
    /// Invocation authority captured from the catalog publication that was
    /// pinned for this run. Restarts must never consult a newer live policy.
    #[serde(default = "default_invocation_policy")]
    pub root_invocation_policy: Value,
    pub definitions: BTreeMap<String, WorkflowRunDefinition>,
}

impl WorkflowDefinitionBundle {
    pub fn key(id: &str, revision: u64) -> String {
        format!("{id}@{revision}")
    }

    pub fn root(&self) -> Option<&WorkflowRunDefinition> {
        self.get(&self.root_id, self.root_revision)
    }

    pub fn get(&self, id: &str, revision: u64) -> Option<&WorkflowRunDefinition> {
        self.definitions.get(&Self::key(id, revision))
    }
}

fn default_object_schema() -> Value {
    serde_json::json!({"type": "object", "additionalProperties": true})
}

fn default_invocation_policy() -> Value {
    serde_json::json!({"explicit": true, "automatic": false})
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowStepDefinition {
    pub id: String,
    #[serde(flatten)]
    pub kind: WorkflowStepKind,
    #[serde(default)]
    pub failure: FailurePolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkflowStepKind {
    Tool {
        tool: String,
        #[serde(default)]
        args: Value,
        #[serde(default)]
        capabilities: Vec<String>,
    },
    Agent {
        /// Named agent resolved through the injected #563-compatible port.
        agent: String,
        prompt: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        effort: Option<String>,
        #[serde(default)]
        capabilities: Vec<String>,
        #[serde(default = "default_structured_attempts")]
        structured_output_attempts: u32,
    },
    Workflow {
        workflow_id: String,
        revision: u64,
        #[serde(default)]
        args: Value,
    },
}

fn default_structured_attempts() -> u32 {
    2
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkflowPlan {
    Step {
        step: String,
    },
    Sequence {
        nodes: Vec<WorkflowPlan>,
    },
    Parallel {
        nodes: Vec<WorkflowPlan>,
    },
    Map {
        source: ValueRef,
        item: String,
        body: Box<WorkflowPlan>,
    },
    Retry {
        node: Box<WorkflowPlan>,
        max_attempts: u32,
        #[serde(default)]
        delay_ms: u64,
    },
}

/// Safe data binding: no interpolation, shell expansion, or expression evaluator.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "from", rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum ValueRef {
    Args {
        #[serde(default)]
        pointer: String,
    },
    Step {
        step: String,
        #[serde(default)]
        pointer: String,
    },
    Item {
        name: String,
        #[serde(default)]
        pointer: String,
    },
    Literal {
        value: Value,
    },
}

/// The only serializable representation of secret input. The handle is safe to
/// persist; secret material is resolved ephemerally at tool dispatch.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkflowSecretHandle {
    #[serde(rename = "$secret")]
    pub capability: String,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FailurePolicy {
    #[default]
    FailFast,
    ContinueWithError,
    SkipDependents,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowBudgets {
    pub max_concurrency: usize,
    pub max_agents: u32,
    pub max_steps: u32,
    pub max_retries: u32,
    pub max_nesting_depth: u32,
    pub wall_time_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_cost_micros: Option<u64>,
}

impl Default for WorkflowBudgets {
    fn default() -> Self {
        Self {
            max_concurrency: 4,
            max_agents: 8,
            max_steps: 256,
            max_retries: 8,
            max_nesting_depth: 4,
            wall_time_ms: 15 * 60 * 1000,
            max_tokens: None,
            max_cost_micros: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowRunStatus {
    Queued,
    Running,
    Suspended,
    Succeeded,
    Failed,
    Cancelled,
}

impl WorkflowRunStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStepStatus {
    Queued,
    Running,
    Suspended,
    Succeeded,
    Failed,
    Cancelled,
    Skipped,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowBudgetUsage {
    pub steps: u32,
    pub retries: u32,
    pub agents: u32,
    pub tokens: u64,
    pub cost_micros: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowFailure {
    pub code: WorkflowFailureCode,
    pub message: String,
    #[serde(default)]
    pub retryable: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowFailureCode {
    InvalidDefinition,
    InvalidInput,
    InvalidOutput,
    UnknownReference,
    PermissionDenied,
    UntrustedWorkspace,
    BudgetExceeded,
    RetryExhausted,
    ExecutionFailed,
    Cancelled,
    RecoverySuspended,
    Suspended,
    DependencySkipped,
    Storage,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowStepSnapshot {
    pub id: String,
    pub status: WorkflowStepStatus,
    pub input_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<WorkflowFailure>,
    pub attempts: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowRunSnapshot {
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_step_id: Option<String>,
    pub session_id: String,
    pub definition: WorkflowRunDefinition,
    pub definition_bundle: WorkflowDefinitionBundle,
    pub definition_bundle_hash: String,
    pub validated_args: Value,
    pub status: WorkflowRunStatus,
    pub steps: BTreeMap<String, WorkflowStepSnapshot>,
    pub usage: WorkflowBudgetUsage,
    pub last_sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<WorkflowFailure>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suspension: Option<WorkflowSuspensionContext>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Durable, non-secret context explaining why a run cannot be blindly replayed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkflowSuspensionContext {
    ToolApproval {
        step_id: String,
        tool: String,
        tool_call_id: String,
    },
    ToolRunning {
        step_id: String,
        tool: String,
        tool_call_id: String,
        killed: bool,
    },
    Recovery {
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowRunEvent {
    pub run_id: String,
    pub sequence: u64,
    pub at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_id: Option<String>,
    #[serde(flatten)]
    pub kind: WorkflowRunEventKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkflowRunEventKind {
    RunQueued,
    RunStarted,
    Phase { name: String },
    StepQueued,
    StepStarted,
    StepSuspended { reason: String },
    StepCompleted { output: Value },
    StepFailed { failure: WorkflowFailure },
    StepCancelled,
    StepSkipped { reason: String },
    RunSuspended { reason: String },
    RunSucceeded { output: Value },
    RunFailed { failure: WorkflowFailure },
    RunCancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StartWorkflowRun {
    pub definition: WorkflowRunDefinition,
    #[serde(default = "default_empty_object")]
    pub args: Value,
    pub session_id: String,
    #[serde(default)]
    pub workspace_trusted: bool,
    #[serde(default)]
    pub allowed_capabilities: Vec<String>,
}

fn default_empty_object() -> Value {
    serde_json::json!({})
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowProgress {
    pub snapshot: WorkflowRunSnapshot,
    pub events: Vec<WorkflowRunEvent>,
}
