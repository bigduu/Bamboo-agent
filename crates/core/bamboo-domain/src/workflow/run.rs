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
    Choice {
        condition: ValueRef,
        then_branch: Box<WorkflowPlan>,
        else_branch: Box<WorkflowPlan>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowBudgetUsage {
    pub steps: u32,
    pub retries: u32,
    pub agents: u32,
    pub tokens: u64,
    /// None means the execution has no monetary measurement.
    pub cost_micros: Option<u64>,
}

impl Default for WorkflowBudgetUsage {
    fn default() -> Self {
        Self {
            steps: 0,
            retries: 0,
            agents: 0,
            tokens: 0,
            cost_micros: Some(0),
        }
    }
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

/// Stable metadata locating one runtime step within its pinned definition.
/// `plan_node_path` starts at the root (`[]`); Sequence/Parallel use child
/// ordinals, Choice uses then=0/else=1, and Map.body/Retry.node use 0.
/// Map indices are outermost first, never item values or names. Unexpanded
/// skipped/timeout placeholders retain only the already-entered outer prefix.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowStepTopology {
    pub planned_step_id: String,
    pub plan_node_path: Vec<usize>,
    pub map_index_path: Vec<usize>,
}

impl WorkflowPlan {
    /// Locate exactly one leaf, refusing unknown or repeated step references
    /// and impossible Map depths instead of guessing from runtime IDs.
    pub fn step_topology(
        &self,
        step_id: &str,
        map_index_path: Vec<usize>,
    ) -> Option<WorkflowStepTopology> {
        fn locate(
            plan: &WorkflowPlan,
            step_id: &str,
            path: &mut Vec<usize>,
            map_depth: usize,
            found: &mut Vec<(Vec<usize>, usize)>,
        ) {
            let mut child = |node: &WorkflowPlan, index, depth| {
                path.push(index);
                locate(node, step_id, path, depth, found);
                path.pop();
            };
            match plan {
                WorkflowPlan::Step { step } => {
                    if step == step_id {
                        found.push((path.clone(), map_depth));
                    }
                }
                WorkflowPlan::Sequence { nodes } | WorkflowPlan::Parallel { nodes } => {
                    for (index, node) in nodes.iter().enumerate() {
                        child(node, index, map_depth);
                    }
                }
                WorkflowPlan::Choice {
                    then_branch,
                    else_branch,
                    ..
                } => {
                    child(then_branch, 0, map_depth);
                    child(else_branch, 1, map_depth);
                }
                WorkflowPlan::Map { body, .. } => child(body, 0, map_depth + 1),
                WorkflowPlan::Retry { node, .. } => child(node, 0, map_depth),
            }
        }
        let mut found = Vec::new();
        locate(self, step_id, &mut Vec::new(), 0, &mut found);
        if found.len() != 1 {
            return None;
        }
        let (plan_node_path, map_depth) = found.pop()?;
        (map_index_path.len() <= map_depth).then(|| WorkflowStepTopology {
            planned_step_id: step_id.to_owned(),
            plan_node_path,
            map_index_path,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowStepSnapshot {
    pub id: String,
    /// Absent in historical snapshots; clients must treat those as unmapped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topology: Option<WorkflowStepTopology>,
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

#[cfg(test)]
mod usage_compatibility_tests {
    use super::*;
    #[test]
    fn workflow_agent_unknown_cost_is_null_and_old_numeric_usage_still_loads() {
        let mut usage = WorkflowBudgetUsage::default();
        assert_eq!(serde_json::to_value(&usage).unwrap()["cost_micros"], 0);
        usage.cost_micros = None;
        let unknown = serde_json::to_value(&usage).unwrap();
        assert!(unknown["cost_micros"].is_null());
        assert_eq!(
            serde_json::from_value::<WorkflowBudgetUsage>(unknown).unwrap(),
            usage
        );
        let old = serde_json::json!({"steps":1,"retries":0,"agents":1,"tokens":12,"cost_micros":2});
        assert_eq!(
            serde_json::from_value::<WorkflowBudgetUsage>(old)
                .unwrap()
                .cost_micros,
            Some(2)
        );
    }
}

#[cfg(test)]
mod topology_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn workflow_topology_rejects_ambiguous_refs_and_impossible_map_depths() {
        let leaf = WorkflowPlan::Step {
            step: "opaque@root[9]/leaf".into(),
        };
        assert_eq!(
            leaf.step_topology("opaque@root[9]/leaf", vec![])
                .unwrap()
                .plan_node_path,
            Vec::<usize>::new()
        );
        assert!(leaf.step_topology("missing", vec![]).is_none());
        assert!(leaf.step_topology("opaque@root[9]/leaf", vec![0]).is_none());
        let duplicate = WorkflowPlan::Sequence {
            nodes: vec![leaf.clone(), leaf],
        };
        assert!(duplicate
            .step_topology("opaque@root[9]/leaf", vec![])
            .is_none());
    }

    #[test]
    fn workflow_topology_loads_historical_steps_without_inventing_mapping() {
        let historical = json!({"id":"step@root[9]", "status":"succeeded", "input_hash":"private", "attempts":1});
        let step: WorkflowStepSnapshot = serde_json::from_value(historical.clone()).unwrap();
        assert_eq!(step.topology, None);
        assert_eq!(serde_json::to_value(step).unwrap(), historical);
    }
}
