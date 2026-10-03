use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeBinding {
    pub scope_id: String,
    pub supervisor_session_id: String,
    pub binding_revision: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TicketKind {
    Goal,
    Work,
    Step,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkState {
    Draft,
    Ready,
    Active,
    Submitted,
    Accepted,
    Blocked,
    Cancelled,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Contract {
    pub title: String,
    pub objective: String,
    pub constraints: Vec<String>,
    pub acceptance: Vec<String>,
    pub user_acceptance_required: bool,
    pub allowed_tools: BTreeSet<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Ticket {
    pub id: String,
    pub scope_id: String,
    pub kind: TicketKind,
    pub parent: Option<String>,
    pub depends_on: BTreeSet<String>,
    pub record_revision: u64,
    pub contract_revision: u64,
    pub generation: u64,
    pub contract: Contract,
    pub state: WorkState,
    pub blocked: Option<BlockReason>,
    /// Explicit user pause survives stop confirmation until an explicit resume.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub paused: bool,
    pub archived: bool,
    pub active_assignment: Option<String>,
    pub current_submission: Option<String>,
    pub accepted_submission: Option<String>,
    pub updated_seq: u64,
    pub import_source: Option<ImportSource>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlockReason {
    pub reason: String,
    pub resume_state: WorkState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssignmentState {
    Planned,
    DispatchPending,
    Admitted,
    Running,
    Submitted,
    Blocked,
    Failed,
    Cancelling,
    Cancelled,
    OutcomeUnknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DependencyInput {
    pub work_id: String,
    pub submission_id: String,
    pub contract_revision: u64,
    pub artifact_hashes: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LocalPlan {
    pub plan_revision: u64,
    pub steps: Vec<LocalStep>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalStep {
    pub id: String,
    pub parent: Option<String>,
    pub title: String,
    pub completed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<StepStatus>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    Pending,
    InProgress,
    Completed,
    Blocked,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionWorkspace {
    pub repo: String,
    pub base_commit: String,
    pub branch: String,
    pub worktree: String,
    pub write_roots: Vec<String>,
    /// Canonical resource keys; capacity leases do not release these claims.
    pub claims: BTreeSet<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Assignment {
    pub id: String,
    pub work_id: String,
    pub generation: u64,
    pub contract_revision: u64,
    pub authority_epoch: u64,
    pub record_revision: u64,
    pub state: AssignmentState,
    pub dispatch_key: String,
    pub runtime: Option<RuntimeReceipt>,
    /// Host-confirmed owned-process termination, independent of business state.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub process_stopped: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub awaiting_request: Option<String>,
    pub dependency_inputs: Vec<DependencyInput>,
    pub plan: LocalPlan,
    pub workspace: Option<ExecutionWorkspace>,
    pub allowed_tools: BTreeSet<String>,
    pub effects: BTreeMap<String, Effect>,
    pub updated_seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeReceipt {
    pub dispatch_key: String,
    pub spec_hash: String,
    pub run_id: String,
    pub session_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectState {
    Planned,
    Started,
    Succeeded,
    FailedBeforeEffect,
    OutcomeUnknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Effect {
    pub action_fingerprint: String,
    pub state: EffectState,
    pub provider_receipt: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Artifact {
    pub uri: String,
    pub sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Submission {
    pub id: String,
    pub work_id: String,
    pub assignment_id: String,
    pub generation: u64,
    pub contract_revision: u64,
    pub runtime: RuntimeReceipt,
    pub dependency_inputs: Vec<DependencyInput>,
    pub artifacts: Vec<Artifact>,
    pub evidence: Vec<String>,
    pub effects: BTreeMap<String, Effect>,
    pub stale: bool,
    pub updated_seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Action {
    pub kind: String,
    pub target: String,
    pub data_hash: String,
    pub amount: Option<String>,
    pub permissions: BTreeSet<String>,
    pub risk: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RequestKind {
    Question,
    Approval { action: Action, fingerprint: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestStatus {
    Open,
    Answered,
    Approved,
    Denied,
    Superseded,
    Expired,
    Consumed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PendingRequest {
    pub id: String,
    pub work_id: String,
    pub assignment_id: Option<String>,
    pub generation: u64,
    pub contract_revision: u64,
    pub prompt_revision: u64,
    pub kind: RequestKind,
    pub prompt: String,
    pub status: RequestStatus,
    pub answer: Option<String>,
    pub consumed_attempt: Option<String>,
    /// Host-derived context hash for replaying an exact yielded Task callback.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_packet_hash: Option<String>,
    pub updated_seq: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DispatchIntent {
    pub assignment_id: String,
    pub dispatch_key: String,
    pub immutable_spec: DispatchSpec,
    pub spec_hash: String,
    pub receipt: Option<RuntimeReceipt>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DispatchSpec {
    pub binding: ScopeBinding,
    pub work_id: String,
    pub assignment_id: String,
    pub generation: u64,
    pub contract_revision: u64,
    pub authority_epoch: u64,
    pub contract: Contract,
    pub dependency_inputs: Vec<DependencyInput>,
    pub workspace: Option<ExecutionWorkspace>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationReceipt {
    pub operation_id: String,
    pub principal: String,
    pub request_hash: String,
    /// Immutable canonical request for host-generated retries after restart.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub canonical_request: String,
    pub committed_seq: u64,
    pub ids: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessageResolution {
    pub message_id: String,
    pub ingress_seq: u64,
    pub message_hash: String,
    pub groups: Vec<ResolutionGroup>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingress: Option<crate::HumanIngressRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub basis: Option<crate::ResolutionBasis>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposal: Option<crate::MessageProposal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposal_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_seq: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResolutionGroup {
    pub operation_id: String,
    pub status: ResolutionStatus,
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub item_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt: Option<OperationReceipt>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolutionStatus {
    Proposed,
    Committed,
    NeedsClarification,
    Rejected,
    Stale,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ImportSource {
    pub session_id: String,
    pub task_id: String,
    pub snapshot_hash: String,
    pub original_state: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub schema: u32,
    pub binding: ScopeBinding,
    pub seq: u64,
    pub graph_revision: u64,
    pub authority_epoch: u64,
    pub tickets: BTreeMap<String, Ticket>,
    pub assignments: BTreeMap<String, Assignment>,
    pub requests: BTreeMap<String, PendingRequest>,
    pub submissions: BTreeMap<String, Submission>,
    pub receipts: BTreeMap<String, OperationReceipt>,
    pub intents: BTreeMap<String, DispatchIntent>,
    pub resolutions: BTreeMap<String, MessageResolution>,
}

impl Snapshot {
    pub fn empty(binding: ScopeBinding) -> Self {
        Self {
            schema: 1,
            binding,
            seq: 0,
            graph_revision: 0,
            authority_epoch: 0,
            tickets: BTreeMap::new(),
            assignments: BTreeMap::new(),
            requests: BTreeMap::new(),
            submissions: BTreeMap::new(),
            receipts: BTreeMap::new(),
            intents: BTreeMap::new(),
            resolutions: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Command {
    pub operation_id: String,
    pub binding: ScopeBinding,
    pub expected_seq: u64,
    pub expected_epoch: u64,
    pub operations: Vec<Operation>,
    /// Original adapter input for generated IDs. It confers no authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<CommandSource>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "adapter", rename_all = "snake_case", deny_unknown_fields)]
pub enum CommandSource {
    WorkerTask { arguments: serde_json::Value },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Operation {
    Create {
        temp_id: String,
        kind: TicketKind,
        parent: Option<String>,
        contract: Contract,
        depends_on: BTreeSet<String>,
    },
    Ready {
        work_id: String,
    },
    UpdateContract {
        work_id: String,
        contract: Contract,
    },
    SetDependencies {
        work_id: String,
        depends_on: BTreeSet<String>,
    },
    Start {
        work_id: String,
        temp_id: String,
        workspace: Option<ExecutionWorkspace>,
    },
    Admitted {
        assignment_id: String,
        receipt: RuntimeReceipt,
    },
    Running {
        assignment_id: String,
    },
    ReplacePlan {
        assignment_id: String,
        expected_plan_revision: u64,
        steps: Vec<LocalStep>,
    },
    Ask {
        work_id: String,
        temp_id: String,
        prompt: String,
        action: Option<Action>,
    },
    YieldForInput {
        assignment_id: String,
        request_id: String,
        packet_hash: String,
    },
    Answer {
        request_id: String,
        prompt_revision: u64,
        answer: String,
    },
    DecideApproval {
        request_id: String,
        prompt_revision: u64,
        fingerprint: String,
        approve: bool,
    },
    ConsumeApproval {
        request_id: String,
        fingerprint: String,
        attempt_id: String,
    },
    RecordEffect {
        assignment_id: String,
        attempt_id: String,
        effect: Effect,
    },
    Submit {
        assignment_id: String,
        temp_id: String,
        artifacts: Vec<Artifact>,
        evidence: Vec<String>,
    },
    Accept {
        work_id: String,
        submission_id: String,
        evidence: Vec<String>,
    },
    AcceptGoal {
        goal_id: String,
        evidence: Vec<String>,
    },
    Reject {
        work_id: String,
        submission_id: String,
        reason: String,
    },
    Cancel {
        work_id: String,
    },
    Pause {
        work_id: String,
        reason: String,
    },
    ConfirmStopped {
        assignment_id: String,
        effects_reconciled: bool,
    },
    RuntimeStopped {
        assignment_id: String,
        receipt: RuntimeReceipt,
        completed: bool,
    },
    ReconcileCompleted {
        assignment_id: String,
        receipt: RuntimeReceipt,
    },
    OutcomeUnknown {
        assignment_id: String,
        reason: String,
    },
    Reopen {
        work_id: String,
    },
    Archive {
        ticket_id: String,
        archived: bool,
    },
    ResolveMessage {
        resolution: MessageResolution,
    },
    Import {
        temp_id: String,
        contract: Contract,
        source: ImportSource,
    },
}
