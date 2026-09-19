//! Shared wire types for Bamboo client front-ends.
//!
//! `bamboo-tui` (and other HTTP/SSE front-ends) talk to the Bamboo server over
//! REST + Server-Sent Events. Rather than re-declaring the same
//! request/response/event structs per client, this crate is the single source
//! of truth for those wire shapes. It depends only on `serde` (no
//! workspace-internal crates) so the clients stay decoupled from the server's
//! internal types.

use serde::{Deserialize, Serialize};

fn default_allow_custom() -> bool {
    true
}

/// Provider-agnostic reasoning levels accepted by Bamboo's session and
/// execute contracts. Keeping this wire enum in the shared client crate makes
/// every front-end serialize the same canonical lowercase values.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ReasoningEffort {
    Low,
    Medium,
    High,
    Xhigh,
    Max,
}

impl ReasoningEffort {
    pub const ALL: [Self; 5] = [Self::Low, Self::Medium, Self::High, Self::Xhigh, Self::Max];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Xhigh => "xhigh",
            Self::Max => "max",
        }
    }
}

// ── Chat ──

#[derive(Serialize, Clone, Debug)]
pub struct ChatRequest {
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Stable Project membership for a new root session. Existing sessions may
    /// repeat the same id but cannot use chat to reassign membership.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    /// Optional model override. Omitted from the request body when `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Provider paired with `model`. Supplying both preserves the catalog's
    /// exact provider/model identity instead of falling back to whichever
    /// provider was previously active for a same-named model.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    /// Optional execution-profile override. Omission delegates to the
    /// provider/session default instead of inventing a client-side level.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ChatResponse {
    pub session_id: String,
    pub stream_url: String,
    pub status: String,
}

#[cfg(test)]
mod chat_request_tests {
    use super::ChatRequest;

    #[test]
    fn chat_request_serializes_project_identity_for_new_root_session() {
        let value = serde_json::to_value(ChatRequest {
            message: "hello".to_string(),
            session_id: None,
            project_id: Some("project-client".to_string()),
            model: Some("gpt-5".to_string()),
            provider: Some("openai".to_string()),
            reasoning_effort: Some(super::ReasoningEffort::High),
        })
        .unwrap();
        assert_eq!(value["project_id"], "project-client");
        assert_eq!(value["provider"], "openai");
        assert_eq!(value["reasoning_effort"], "high");
        assert!(value.get("session_id").is_none());
    }
}

// ── Execute ──

#[derive(Serialize, Clone, Debug)]
pub struct ExecuteRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,
}

#[cfg(test)]
mod execute_request_tests {
    use super::ExecuteRequest;

    #[test]
    fn execute_request_keeps_provider_paired_with_model() {
        let value = serde_json::to_value(ExecuteRequest {
            model: Some("shared".to_string()),
            provider: Some("provider-b".to_string()),
            reasoning_effort: Some(super::ReasoningEffort::Xhigh),
        })
        .unwrap();
        assert_eq!(value["model"], "shared");
        assert_eq!(value["provider"], "provider-b");
        assert_eq!(value["reasoning_effort"], "xhigh");
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct ExecuteResponse {
    pub session_id: String,
    pub status: String,
    pub events_url: String,
}

// ── SSE events ──

/// Canonical task lifecycle status shared by HTTP snapshots and SSE deltas.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskItemStatus {
    #[default]
    Pending,
    InProgress,
    Completed,
    Blocked,
    #[serde(other)]
    Unknown,
}

#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskPhase {
    Planning,
    #[default]
    Execution,
    Verification,
    Handoff,
    #[serde(other)]
    Unknown,
}

#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskPriority {
    Low,
    #[default]
    Medium,
    High,
    Critical,
    #[serde(other)]
    Unknown,
}

#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskEvidenceKind {
    #[default]
    Note,
    ToolCall,
    File,
    Command,
    Test,
    Observation,
    #[serde(other)]
    Unknown,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct TaskEvidence {
    #[serde(default)]
    pub kind: TaskEvidenceKind,
    #[serde(default)]
    pub summary: String,
    #[serde(default)]
    pub reference: Option<String>,
    #[serde(default)]
    pub tool_name: Option<String>,
    #[serde(default)]
    pub tool_call_id: Option<String>,
    #[serde(default)]
    pub round: Option<u32>,
    #[serde(default)]
    pub success: Option<bool>,
}

#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskBlockerKind {
    UserInput,
    Dependency,
    ToolFailure,
    External,
    #[default]
    #[serde(other)]
    Unknown,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct TaskBlocker {
    #[serde(default)]
    pub kind: TaskBlockerKind,
    #[serde(default)]
    pub summary: String,
    #[serde(default)]
    pub waiting_on: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct TaskTransition {
    #[serde(default)]
    pub from_status: TaskItemStatus,
    #[serde(default)]
    pub to_status: TaskItemStatus,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub round: Option<u32>,
    #[serde(default)]
    pub changed_at: String,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct TaskItem {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub status: TaskItemStatus,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub notes: String,
    #[serde(default, alias = "activeForm")]
    pub active_form: Option<String>,
    #[serde(default)]
    pub parent_id: Option<String>,
    #[serde(default)]
    pub phase: TaskPhase,
    #[serde(default)]
    pub priority: TaskPriority,
    #[serde(default)]
    pub completion_criteria: Vec<String>,
    #[serde(default)]
    pub evidence: Vec<TaskEvidence>,
    #[serde(default)]
    pub blockers: Vec<TaskBlocker>,
    #[serde(default)]
    pub transitions: Vec<TaskTransition>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct TaskList {
    #[serde(default)]
    pub session_id: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub items: Vec<TaskItem>,
    #[serde(default)]
    pub created_at: String,
    #[serde(default)]
    pub updated_at: String,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct TaskProgress {
    #[serde(default)]
    pub completed: usize,
    #[serde(default)]
    pub total: usize,
    #[serde(default)]
    pub percentage: u8,
}

/// Read-only response from `GET /api/v1/sessions/{id}/task`.
#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct TaskListResponse {
    #[serde(default)]
    pub session_id: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub items: Vec<TaskItem>,
    #[serde(default)]
    pub progress: TaskProgress,
    #[serde(default)]
    pub version: u64,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PlanModeStatus {
    #[default]
    Exploring,
    Designing,
    Reviewing,
    Finalizing,
    AwaitingApproval,
    #[serde(other)]
    Unknown,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
pub struct PlanModeState {
    pub entered_at: String,
    pub pre_permission_mode: String,
    #[serde(default)]
    pub plan_file_path: Option<String>,
    #[serde(default)]
    pub status: PlanModeStatus,
}

/// Server-Sent Event payload streamed during an agent run.
///
/// Tagged by a `type` field, snake_cased. This is the superset of variants
/// emitted by the server; individual front-ends may only render a subset.
#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentEvent {
    ExecutionStarted {
        run_id: String,
        session_id: String,
        started_at: String,
    },
    Token {
        content: String,
    },
    ReasoningToken {
        content: String,
    },
    ToolToken {
        tool_call_id: String,
        content: String,
    },
    ToolStart {
        tool_call_id: String,
        tool_name: String,
        arguments: serde_json::Value,
    },
    ToolComplete {
        tool_call_id: String,
        result: ToolResult,
    },
    ToolError {
        tool_call_id: String,
        error: String,
    },
    NeedClarification {
        question: String,
        options: Option<Vec<String>>,
        #[serde(default)]
        tool_call_id: Option<String>,
        #[serde(default)]
        tool_name: Option<String>,
        #[serde(default = "default_allow_custom")]
        allow_custom: bool,
        #[serde(default)]
        source: Option<String>,
    },
    TaskListUpdated {
        task_list: TaskList,
        /// Monotonic persisted task-list generation. Older servers omit it;
        /// clients then fall back to the snapshot timestamp.
        #[serde(default)]
        version: Option<u64>,
    },
    TaskListItemProgress {
        session_id: String,
        item_id: String,
        status: TaskItemStatus,
        tool_calls_count: usize,
        version: u64,
        /// Rich item projection for blocker/evidence/transition rendering.
        /// Optional so critical events persisted by older servers still replay.
        #[serde(default)]
        item: Option<TaskItem>,
    },
    TaskListCompleted {
        session_id: String,
        completed_at: String,
        total_rounds: u32,
        total_tool_calls: usize,
        #[serde(default)]
        version: Option<u64>,
    },
    TaskEvaluationStarted {
        session_id: String,
        items_count: usize,
        #[serde(default)]
        generation: Option<u64>,
    },
    TaskEvaluationCompleted {
        session_id: String,
        updates_count: usize,
        reasoning: String,
        #[serde(default)]
        generation: Option<u64>,
    },
    TaskEvaluationCancelled {
        session_id: String,
        reason: String,
        #[serde(default)]
        generation: Option<u64>,
    },
    /// A parent-session tool was stopped at Bamboo's typed permission gate.
    /// The authoritative request contract is fetched from the pending endpoint;
    /// this event is an early, argument-bearing signal used to trigger that
    /// reconciliation without interpreting clarification display text.
    ToolApprovalRequested {
        tool_call_id: String,
        tool_name: String,
        #[serde(default)]
        parameters: serde_json::Value,
    },
    /// An out-of-process child agent is waiting for a checked one-shot human
    /// decision. The child/session/request tuple is the protocol identity.
    ChildApprovalRequested {
        child_session_id: String,
        request_id: String,
        tool_name: String,
        permission: String,
        resource: String,
    },
    /// Durable lifecycle update for a child approval. Optional/default fields
    /// keep older persisted critical-event frames replayable.
    ChildApprovalChanged {
        parent_session_id: String,
        child_session_id: String,
        #[serde(default)]
        child_attempt: u32,
        request_id: String,
        #[serde(default)]
        version: u64,
        status: String,
        #[serde(default)]
        reason: Option<String>,
        #[serde(default)]
        tool_name: String,
        #[serde(default)]
        permission: String,
        #[serde(default)]
        resource: String,
        #[serde(default)]
        created_at: String,
        #[serde(default)]
        resolved_at: Option<String>,
    },
    ToolLifecycle {
        tool_call_id: String,
        tool_name: String,
        phase: String,
        elapsed_ms: Option<u64>,
        is_mutating: bool,
        auto_approved: bool,
        summary: Option<String>,
        error: Option<String>,
    },
    ContextCompressionStatus {
        phase: String,
        status: String,
    },
    PlanModeEntered {
        session_id: String,
        #[serde(default)]
        reason: Option<String>,
        #[serde(default)]
        pre_permission_mode: Option<String>,
        #[serde(default)]
        entered_at: Option<String>,
        #[serde(default)]
        status: Option<PlanModeStatus>,
        #[serde(default)]
        plan_file_path: Option<String>,
    },
    PlanModeExited {
        session_id: String,
        approved: bool,
        #[serde(default)]
        plan: Option<String>,
        #[serde(default)]
        restored_mode: Option<String>,
    },
    PlanFileUpdated {
        session_id: String,
        file_path: String,
        #[serde(default)]
        content_summary: Option<String>,
        #[serde(default)]
        status: Option<PlanModeStatus>,
    },
    /// Current round for a session. Legacy parent streams may carry this inside
    /// a `SubAgentEvent`; current clients subscribe to the child's own session
    /// stream for full-fidelity progress.
    RunnerProgress {
        session_id: String,
        round_count: u32,
    },
    /// A per-run token/tool-call/subagent budget tripped and the run was
    /// gracefully stopped (issue #221).
    BudgetExceeded {
        /// Which budget tripped: `"max_total_tokens"` | `"max_tool_calls"` |
        /// `"max_subagents"`.
        kind: String,
        limit: u64,
        actual: u64,
    },
    Complete {
        usage: TokenUsage,
    },
    Cancelled {
        #[serde(default)]
        message: Option<String>,
    },
    Error {
        message: String,
    },

    // ── Sub-agent lifecycle (forwarded from the parent session's stream) ──
    // Typed projections of the server's SubAgent* events. Extra envelope fields
    // such as parent_session_id and timestamp are ignored on decode.
    SubAgentStarted {
        child_session_id: String,
        #[serde(default)]
        title: Option<String>,
    },
    /// Legacy full-fidelity child projection on the parent session stream.
    /// Boxing keeps the recursively-shaped compatibility contract finite.
    SubAgentEvent {
        child_session_id: String,
        event: Box<AgentEvent>,
    },
    SubAgentHeartbeat {
        child_session_id: String,
    },
    SubAgentCompleted {
        child_session_id: String,
        /// "completed" | "cancelled" | "error" | "skipped"
        status: String,
        #[serde(default)]
        error: Option<String>,
    },
}

#[derive(Deserialize, Debug, Clone)]
pub struct ToolResult {
    pub success: bool,
    pub result: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TokenUsage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
}

#[cfg(test)]
mod clarification_event_tests {
    use super::AgentEvent;

    #[test]
    fn typed_clarification_preserves_all_contract_fields() {
        let event: AgentEvent = serde_json::from_value(serde_json::json!({
            "type": "need_clarification",
            "question": "Choose",
            "options": ["same", "same"],
            "tool_call_id": "call-1",
            "tool_name": "ConclusionWithOptions",
            "allow_custom": false,
            "source": "pause_tool"
        }))
        .unwrap();

        assert!(matches!(
            event,
            AgentEvent::NeedClarification {
                question,
                options: Some(options),
                tool_call_id: Some(tool_call_id),
                tool_name: Some(tool_name),
                allow_custom: false,
                source: Some(source),
            } if question == "Choose"
                && options == ["same", "same"]
                && tool_call_id == "call-1"
                && tool_name == "ConclusionWithOptions"
                && source == "pause_tool"
        ));
    }

    #[test]
    fn legacy_clarification_defaults_to_open_custom_mode() {
        let event: AgentEvent = serde_json::from_value(serde_json::json!({
            "type": "need_clarification",
            "question": "Explain",
            "options": null
        }))
        .unwrap();

        assert!(matches!(
            event,
            AgentEvent::NeedClarification {
                allow_custom: true,
                tool_call_id: None,
                tool_name: None,
                source: None,
                ..
            }
        ));
    }

    #[test]
    fn approval_events_preserve_protocol_identity_and_payload() {
        let parent: AgentEvent = serde_json::from_value(serde_json::json!({
            "type": "tool_approval_requested",
            "tool_call_id": "call-parent",
            "tool_name": "Bash",
            "parameters": {"command": "git push"}
        }))
        .unwrap();
        assert!(matches!(
            parent,
            AgentEvent::ToolApprovalRequested {
                tool_call_id,
                tool_name,
                parameters,
            } if tool_call_id == "call-parent"
                && tool_name == "Bash"
                && parameters["command"] == "git push"
        ));

        let child: AgentEvent = serde_json::from_value(serde_json::json!({
            "type": "child_approval_requested",
            "child_session_id": "child-1",
            "request_id": "request-7",
            "tool_name": "Write",
            "permission": "write_file",
            "resource": "/tmp/result.txt"
        }))
        .unwrap();
        assert!(matches!(
            child,
            AgentEvent::ChildApprovalRequested {
                child_session_id,
                request_id,
                resource,
                ..
            } if child_session_id == "child-1"
                && request_id == "request-7"
                && resource == "/tmp/result.txt"
        ));
    }

    #[test]
    fn forwarded_child_progress_preserves_child_and_session_identity() {
        let event: AgentEvent = serde_json::from_value(serde_json::json!({
            "type": "sub_agent_event",
            "parent_session_id": "parent-1",
            "child_session_id": "child-2",
            "event": {
                "type": "runner_progress",
                "session_id": "child-2",
                "round_count": 7
            }
        }))
        .unwrap();

        assert!(matches!(
            event,
            AgentEvent::SubAgentEvent { child_session_id, event }
                if child_session_id == "child-2"
                    && matches!(
                        event.as_ref(),
                        AgentEvent::RunnerProgress { session_id, round_count: 7 }
                            if session_id == "child-2"
                    )
        ));
    }

    #[test]
    fn older_child_approval_change_defaults_new_fields() {
        let event: AgentEvent = serde_json::from_value(serde_json::json!({
            "type": "child_approval_changed",
            "parent_session_id": "parent-1",
            "child_session_id": "child-1",
            "request_id": "request-7",
            "status": "denied"
        }))
        .unwrap();
        assert!(matches!(
            event,
            AgentEvent::ChildApprovalChanged {
                child_attempt: 0,
                version: 0,
                reason: None,
                resolved_at: None,
                ..
            }
        ));
    }
}

#[cfg(test)]
mod task_plan_event_tests {
    use super::{AgentEvent, PlanModeStatus, TaskItemStatus};

    #[test]
    fn legacy_task_events_default_new_projection_fields() {
        let updated: AgentEvent = serde_json::from_value(serde_json::json!({
            "type": "task_list_updated",
            "task_list": {
                "session_id": "root-1",
                "title": "Tasks",
                "items": [],
                "created_at": "2026-08-16T00:00:00Z",
                "updated_at": "2026-08-16T00:00:01Z"
            }
        }))
        .unwrap();
        assert!(matches!(
            updated,
            AgentEvent::TaskListUpdated { version: None, .. }
        ));

        let progress: AgentEvent = serde_json::from_value(serde_json::json!({
            "type": "task_list_item_progress",
            "session_id": "root-1",
            "item_id": "task-1",
            "status": "blocked",
            "tool_calls_count": 2,
            "version": 4
        }))
        .unwrap();
        assert!(matches!(
            progress,
            AgentEvent::TaskListItemProgress {
                status: TaskItemStatus::Blocked,
                item: None,
                version: 4,
                ..
            }
        ));
    }

    #[test]
    fn rich_task_and_plan_events_preserve_live_details() {
        let progress: AgentEvent = serde_json::from_value(serde_json::json!({
            "type": "task_list_item_progress",
            "session_id": "root-1",
            "item_id": "task-1",
            "status": "blocked",
            "tool_calls_count": 2,
            "version": 5,
            "item": {
                "id": "task-1",
                "description": "Deploy",
                "status": "blocked",
                "blockers": [{
                    "kind": "external",
                    "summary": "release gate",
                    "waiting_on": "human approval"
                }]
            }
        }))
        .unwrap();
        assert!(matches!(
            progress,
            AgentEvent::TaskListItemProgress { item: Some(item), .. }
                if item.blockers[0].waiting_on.as_deref() == Some("human approval")
        ));

        let plan: AgentEvent = serde_json::from_value(serde_json::json!({
            "type": "plan_file_updated",
            "session_id": "root-1",
            "file_path": "/tmp/plan.md",
            "content_summary": "Ready for review",
            "status": "awaiting_approval"
        }))
        .unwrap();
        assert!(matches!(
            plan,
            AgentEvent::PlanFileUpdated {
                status: Some(PlanModeStatus::AwaitingApproval),
                ..
            }
        ));
    }
}
