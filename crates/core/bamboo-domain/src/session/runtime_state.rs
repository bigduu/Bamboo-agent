//! Structured agent runtime state — the "control plane" persisted alongside a Session.
//!
//! Where `Session.messages` records the *factual* conversation history and
//! `Session.metadata` stores lightweight key-value annotations, `AgentRuntimeState`
//! captures the structured lifecycle state of an agent execution: current phase,
//! round progress, suspension info, child session tracking, and hook checkpoints.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Schema version for forward-compatible deserialization.
pub const AGENT_RUNTIME_STATE_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/// Top-level status of an agent run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AgentStatusState {
    #[default]
    Idle,
    Initializing,
    Running,
    Suspended,
    Finalizing,
    Completed,
    Cancelled,
    Failed,
}

// ---------------------------------------------------------------------------
// Sub-states
// ---------------------------------------------------------------------------

/// Round-level progress within an agent run.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct RoundRuntimeState {
    pub current_round: u32,
    pub max_rounds: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_round_id: Option<String>,
    /// Cumulative tool calls issued across every round of this run so far.
    /// Consulted by the per-run `max_tool_calls` budget guardrail (issue #221).
    pub total_tool_calls: u32,
    /// Cumulative actual (provider-reported) prompt tokens across every round
    /// of this run so far. Consulted by the per-run `max_total_tokens` budget
    /// guardrail (issue #221).
    pub total_prompt_tokens: u64,
    /// Cumulative actual (provider-reported) completion tokens across every
    /// round of this run so far. Consulted by the per-run `max_total_tokens`
    /// budget guardrail (issue #221).
    pub total_completion_tokens: u64,
    /// Cumulative `SubAgent` create calls issued across every round of this
    /// run so far. Consulted by the per-run `max_subagents` budget guardrail
    /// (issue #221).
    #[serde(default)]
    pub total_subagents_spawned: u32,
}

/// Prompt assembly tracking.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct PromptRuntimeState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub composer_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub section_flags: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub section_lengths: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub section_layout: Option<String>,
}

/// Tool surface tracking.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct ToolRuntimeState {
    pub schemas_resolved: bool,
    pub disabled_tool_count: u32,
    pub active_tool_calls: u32,
}

/// Memory and compression tracking.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct MemoryRuntimeState {
    pub external_memory_injected: bool,
    pub compression_events_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_compression_at: Option<DateTime<Utc>>,
    pub overflow_recovery_total: u32,
    pub overflow_recovery_consecutive: u32,
}

/// LLM provider tracking.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct LlmRuntimeState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub background_model_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fast_model_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub responses_previous_id: Option<String>,
}

/// Child session tracking for spawn/session-tree support.
///
/// These fields are a **derived, in-memory-only** view. The authoritative
/// parent→child relationship and each child's run status already live in the
/// global session index (`parent_session_id` + `last_run_status`), so persisting
/// them here would denormalize that data and force a full parent rewrite on every
/// child state change. They are `#[serde(skip)]`: never written to disk and never
/// read back — callers that need them reconstruct from the index (see
/// `Storage::list_child_run_statuses`). The durable parent-side state is
/// [`WaitingForChildrenState`], which is NOT derivable and IS persisted.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct ChildSessionRuntimeState {
    #[serde(skip)]
    pub active_children: u32,
    #[serde(skip)]
    pub completed_children: u32,
    #[serde(skip)]
    pub active_ids: Vec<String>,
    #[serde(skip)]
    pub completed_ids: Vec<String>,
}

/// Parent wait policy for child-session orchestration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ChildWaitPolicy {
    /// Resume the parent when every tracked child reaches a terminal state.
    #[default]
    All,
    /// Resume the parent when any tracked child reaches a terminal state.
    Any,
    /// Resume the parent immediately on error/timeout/cancelled, otherwise wait
    /// for all children to complete successfully.
    FirstError,
}

impl ChildWaitPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Any => "any",
            Self::FirstError => "first_error",
        }
    }
}

/// Durable parent-side wait state for event-driven child-session orchestration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WaitingForChildrenState {
    /// Child sessions this parent is currently waiting on.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub child_session_ids: Vec<String>,
    /// Wait completion policy.
    #[serde(default)]
    pub wait_for: ChildWaitPolicy,
    /// When this wait state was registered.
    pub registered_at: DateTime<Utc>,
    /// Optional parent wait lease. Expiry does not kill children; child runners
    /// own child liveness/timeout decisions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_at: Option<DateTime<Utc>>,
    /// Tool call that registered the wait, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registered_by_tool_call_id: Option<String>,
}

impl WaitingForChildrenState {
    /// Construct a parent wait over `child_session_ids` with the default
    /// 6-hour wait lease (`timeout_at = now + 6h`) and no registering tool
    /// call. Callers needing a specific lease or originating tool-call id set
    /// those fields on the returned value afterward.
    ///
    /// Centralizes the wait-record defaults so every runner-initiated suspend
    /// (the orphaned-children safety net, the guardian review gate, ...) and the
    /// adapter's merge default-branch build the record identically.
    pub fn for_children(
        child_session_ids: Vec<String>,
        wait_for: ChildWaitPolicy,
        now: DateTime<Utc>,
    ) -> Self {
        Self {
            child_session_ids,
            wait_for,
            registered_at: now,
            timeout_at: Some(now + chrono::Duration::hours(6)),
            registered_by_tool_call_id: None,
        }
    }
}

/// Durable wait state for event-driven background-Bash orchestration (issue #84).
///
/// The agent loop opt-in is implicit: a background shell only lands in the
/// session-aware registry (and therefore in `bash_runtime::running_shells_for_session`)
/// when the agent launched it with `run_in_background: true`. Foreground Bash
/// calls are awaited inline and never appear here, so this state never engages
/// for the default foreground path. The wait policy is fixed — "all tracked bash
/// ids must finish" — so, unlike children, no policy enum is needed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WaitingForBashState {
    /// Background shell ids this session is currently waiting on.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bash_ids: Vec<String>,
    /// When this wait state was registered.
    pub registered_at: DateTime<Utc>,
    /// Optional wait lease. Expiry does not kill shells; the bash coordinator
    /// (Phase 2c) owns liveness/timeout decisions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_at: Option<DateTime<Utc>>,
}

impl WaitingForBashState {
    /// Construct a wait over `bash_ids` with the default 6-hour wait lease
    /// (`timeout_at = now + 6h`), mirroring [`WaitingForChildrenState::for_children`]
    /// so every runner-initiated bash suspend (the end-of-turn safety net) builds
    /// the record identically.
    pub fn for_bash(bash_ids: Vec<String>, now: DateTime<Utc>) -> Self {
        Self {
            bash_ids,
            registered_at: now,
            timeout_at: Some(now + chrono::Duration::hours(6)),
        }
    }
}

/// Suspension reason and context for resumable runs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SuspensionState {
    pub reason: String,
    pub suspended_at: DateTime<Utc>,
    pub resumable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hook_point: Option<String>,
}

/// Record of a hook execution for observability and debugging.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HookCheckpoint {
    pub hook_point: String,
    pub timestamp: DateTime<Utc>,
    pub result: String,
    pub duration_ms: u64,
}

// ---------------------------------------------------------------------------
// Plan mode state
// ---------------------------------------------------------------------------

/// Status within a plan mode session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PlanModeStatus {
    /// Exploring the codebase with read-only tools.
    #[default]
    Exploring,
    /// Designing the implementation approach.
    Designing,
    /// Reviewing the plan before finalizing.
    Reviewing,
    /// Writing the plan to the plan file.
    Finalizing,
    /// Awaiting user approval to exit plan mode.
    AwaitingApproval,
}

/// Structured state for an active plan mode session.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PlanModeState {
    /// When plan mode was entered.
    pub entered_at: DateTime<Utc>,
    /// The permission mode to restore when exiting plan mode.
    pub pre_permission_mode: String,
    /// Path to the persisted plan file, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_file_path: Option<String>,
    /// Current phase within plan mode.
    #[serde(default)]
    pub status: PlanModeStatus,
}

// ---------------------------------------------------------------------------
// Top-level state
// ---------------------------------------------------------------------------

/// Structured runtime state persisted alongside a [`Session`](super::Session).
///
/// This is the "control plane" of an agent execution — distinct from the
/// "data plane" (`messages`) and the "annotation plane" (`metadata`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentRuntimeState {
    pub version: u32,
    pub run_id: String,
    #[serde(default)]
    pub status: AgentStatusState,
    #[serde(default)]
    pub round: RoundRuntimeState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suspension: Option<SuspensionState>,
    #[serde(default)]
    pub prompt: PromptRuntimeState,
    #[serde(default)]
    pub tools: ToolRuntimeState,
    #[serde(default)]
    pub memory: MemoryRuntimeState,
    #[serde(default)]
    pub llm: LlmRuntimeState,
    #[serde(default)]
    pub children: ChildSessionRuntimeState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiting_for_children: Option<WaitingForChildrenState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiting_for_bash: Option<WaitingForBashState>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub checkpoints: Vec<HookCheckpoint>,
    /// Volatile context injected by lifecycle hooks for this run. Request
    /// assembly renders it after conversation history, never into the cached
    /// system prefix.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hook_contexts: Vec<String>,
    /// Number of extra rounds forced by a blocking `Stop` hook in this run.
    #[serde(default)]
    pub stop_hook_forced_continuations: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_mode: Option<PlanModeState>,
    /// When `true`, this session skips all tool permission checks (the
    /// "bypass permissions" mode). Scoped to a single session so concurrent
    /// sessions are unaffected; persisted in `runtime.json`.
    #[serde(default)]
    pub bypass_permissions: bool,
    /// When `true`, this run has NO interactive human approver — a headless
    /// `-p` run, a scheduled job, or a deployed broker-agent. #73: child
    /// sub-agents inherit it (alongside `bypass_permissions`), and a sub-agent's
    /// gated action is then decided by the off-loop model-reviewer locally
    /// instead of escalating to a human who will never answer. Interactive
    /// sessions leave it `false` so approvals reach the human as usual.
    #[serde(default)]
    pub no_human_approver: bool,
}

impl AgentRuntimeState {
    pub fn new(run_id: impl Into<String>) -> Self {
        Self {
            version: AGENT_RUNTIME_STATE_VERSION,
            run_id: run_id.into(),
            status: AgentStatusState::Idle,
            round: RoundRuntimeState::default(),
            suspension: None,
            prompt: PromptRuntimeState::default(),
            tools: ToolRuntimeState::default(),
            memory: MemoryRuntimeState::default(),
            llm: LlmRuntimeState::default(),
            children: ChildSessionRuntimeState::default(),
            waiting_for_children: None,
            waiting_for_bash: None,
            checkpoints: Vec::new(),
            hook_contexts: Vec::new(),
            stop_hook_forced_continuations: 0,
            plan_mode: None,
            bypass_permissions: false,
            no_human_approver: false,
        }
    }
}

impl Default for AgentRuntimeState {
    fn default() -> Self {
        Self {
            version: AGENT_RUNTIME_STATE_VERSION,
            run_id: String::new(),
            status: AgentStatusState::Idle,
            round: RoundRuntimeState::default(),
            suspension: None,
            prompt: PromptRuntimeState::default(),
            tools: ToolRuntimeState::default(),
            memory: MemoryRuntimeState::default(),
            llm: LlmRuntimeState::default(),
            children: ChildSessionRuntimeState::default(),
            waiting_for_children: None,
            waiting_for_bash: None,
            checkpoints: Vec::new(),
            hook_contexts: Vec::new(),
            stop_hook_forced_continuations: 0,
            plan_mode: None,
            bypass_permissions: false,
            no_human_approver: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_state_has_correct_defaults() {
        let state = AgentRuntimeState::new("run-001");
        assert_eq!(state.version, AGENT_RUNTIME_STATE_VERSION);
        assert_eq!(state.run_id, "run-001");
        assert_eq!(state.status, AgentStatusState::Idle);
        assert!(state.suspension.is_none());
        assert!(state.waiting_for_children.is_none());
        assert_eq!(state.round.current_round, 0);
        assert!(state.checkpoints.is_empty());
    }

    #[test]
    fn default_impl_matches_new_empty_id() {
        let default_state = AgentRuntimeState::default();
        let new_state = AgentRuntimeState::new("");
        assert_eq!(default_state, new_state);
    }

    #[test]
    fn serialize_deserialize_round_trip() {
        let mut state = AgentRuntimeState::new("run-abc");
        state.status = AgentStatusState::Running;
        state.round.current_round = 3;
        state.round.max_rounds = 200;
        state.llm.model_name = Some("gpt-4o".to_string());
        state.memory.overflow_recovery_total = 1;
        state.suspension = Some(SuspensionState {
            reason: "awaiting_user".to_string(),
            suspended_at: Utc::now(),
            resumable: true,
            hook_point: Some("AfterToolExecution".to_string()),
        });
        state.checkpoints.push(HookCheckpoint {
            hook_point: "BeforeLlmCall".to_string(),
            timestamp: Utc::now(),
            result: "Continue".to_string(),
            duration_ms: 42,
        });

        let json = serde_json::to_string(&state).unwrap();
        let restored: AgentRuntimeState = serde_json::from_str(&json).unwrap();
        assert_eq!(state, restored);
    }

    #[test]
    fn old_json_without_new_fields_deserializes() {
        // Simulate JSON that only has version and run_id
        let json = r#"{"version":1,"run_id":"old-run","status":"idle"}"#;
        let state: AgentRuntimeState = serde_json::from_str(json).unwrap();
        assert_eq!(state.version, 1);
        assert_eq!(state.run_id, "old-run");
        assert_eq!(state.status, AgentStatusState::Idle);
        assert!(state.suspension.is_none());
        assert_eq!(state.round.current_round, 0);
    }

    #[test]
    fn all_status_variants_serialize_correctly() {
        let variants = [
            AgentStatusState::Idle,
            AgentStatusState::Initializing,
            AgentStatusState::Running,
            AgentStatusState::Suspended,
            AgentStatusState::Finalizing,
            AgentStatusState::Completed,
            AgentStatusState::Cancelled,
            AgentStatusState::Failed,
        ];
        for variant in &variants {
            let json = serde_json::to_string(variant).unwrap();
            let restored: AgentStatusState = serde_json::from_str(&json).unwrap();
            assert_eq!(variant, &restored, "round-trip failed for {:?}", variant);
        }
    }

    #[test]
    fn plan_mode_state_serialize_deserialize_round_trip() {
        let state = PlanModeState {
            entered_at: Utc::now(),
            pre_permission_mode: "default".to_string(),
            plan_file_path: Some("/tmp/plans/test-plan.md".to_string()),
            status: PlanModeStatus::Designing,
        };
        let json = serde_json::to_string(&state).unwrap();
        let restored: PlanModeState = serde_json::from_str(&json).unwrap();
        assert_eq!(state, restored);
    }

    #[test]
    fn old_json_without_plan_mode_field_deserializes() {
        // Simulate JSON from before plan_mode was added
        let json = r#"{"version":1,"run_id":"old-run","status":"idle","checkpoints":[]}"#;
        let state: AgentRuntimeState = serde_json::from_str(json).unwrap();
        assert_eq!(state.version, 1);
        assert_eq!(state.run_id, "old-run");
        assert!(state.plan_mode.is_none());
    }

    #[test]
    fn children_view_is_not_persisted() {
        // The derived child-tracking view must never be serialized: it is
        // reconstructed from the session index, not the parent file.
        let mut state = AgentRuntimeState::new("run-children");
        state.children.active_children = 3;
        state.children.completed_children = 1;
        state.children.active_ids = vec!["c-1".to_string(), "c-2".to_string(), "c-3".to_string()];
        state.children.completed_ids = vec!["c-0".to_string()];

        let json = serde_json::to_string(&state).unwrap();
        assert!(
            !json.contains("active_ids") && !json.contains("completed_ids"),
            "children id vectors must not be serialized: {json}"
        );
        assert!(
            !json.contains("active_children") && !json.contains("completed_children"),
            "children counts must not be serialized: {json}"
        );

        // And they come back as the empty default after a round trip.
        let restored: AgentRuntimeState = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.children, ChildSessionRuntimeState::default());
    }

    #[test]
    fn waiting_for_children_round_trip() {
        let mut state = AgentRuntimeState::new("run-wait");
        state.waiting_for_children = Some(WaitingForChildrenState {
            child_session_ids: vec!["child-1".to_string(), "child-2".to_string()],
            wait_for: ChildWaitPolicy::All,
            registered_at: Utc::now(),
            timeout_at: None,
            registered_by_tool_call_id: Some("tool-1".to_string()),
        });

        let serialized = serde_json::to_string(&state).expect("serialize");
        let deserialized: AgentRuntimeState =
            serde_json::from_str(&serialized).expect("deserialize");

        let wait = deserialized
            .waiting_for_children
            .expect("wait state should round-trip");
        assert_eq!(wait.child_session_ids.len(), 2);
        assert_eq!(wait.wait_for, ChildWaitPolicy::All);
        assert_eq!(wait.registered_by_tool_call_id.as_deref(), Some("tool-1"));
    }

    #[test]
    fn waiting_for_bash_state_round_trip() {
        let wait = WaitingForBashState {
            bash_ids: vec!["shell-1".to_string(), "shell-2".to_string()],
            registered_at: Utc::now(),
            timeout_at: Some(Utc::now()),
        };
        let serialized = serde_json::to_string(&wait).expect("serialize");
        let restored: WaitingForBashState = serde_json::from_str(&serialized).expect("deserialize");
        assert_eq!(wait, restored);
        assert_eq!(restored.bash_ids.len(), 2);
    }

    #[test]
    fn agent_runtime_state_with_waiting_for_bash_round_trip() {
        let mut state = AgentRuntimeState::new("run-bash");
        state.waiting_for_bash = Some(WaitingForBashState {
            bash_ids: vec!["bg-1".to_string(), "bg-2".to_string()],
            registered_at: Utc::now(),
            timeout_at: None,
        });

        let serialized = serde_json::to_string(&state).expect("serialize");
        let deserialized: AgentRuntimeState =
            serde_json::from_str(&serialized).expect("deserialize");
        assert_eq!(state, deserialized);

        let wait = deserialized
            .waiting_for_bash
            .expect("bash wait state should round-trip");
        assert_eq!(wait.bash_ids, vec!["bg-1".to_string(), "bg-2".to_string()]);
    }

    #[test]
    fn waiting_for_bash_absent_by_default_and_not_serialized() {
        // A fresh state has no bash wait, and it must not be serialized when absent.
        let state = AgentRuntimeState::new("run-empty");
        assert!(state.waiting_for_bash.is_none());

        let json = serde_json::to_string(&state).unwrap();
        assert!(
            !json.contains("waiting_for_bash"),
            "absent bash wait must be skipped in serialization: {json}"
        );

        // Old JSON without the field still deserializes to None.
        let old = r#"{"version":1,"run_id":"old","status":"idle"}"#;
        let restored: AgentRuntimeState = serde_json::from_str(old).unwrap();
        assert!(restored.waiting_for_bash.is_none());
    }

    #[test]
    fn agent_runtime_state_with_plan_mode_round_trip() {
        let mut state = AgentRuntimeState::new("run-plan");
        state.plan_mode = Some(PlanModeState {
            entered_at: Utc::now(),
            pre_permission_mode: "accept_edits".to_string(),
            plan_file_path: None,
            status: PlanModeStatus::Exploring,
        });

        let json = serde_json::to_string(&state).unwrap();
        let restored: AgentRuntimeState = serde_json::from_str(&json).unwrap();
        assert_eq!(state, restored);
        assert!(restored.plan_mode.is_some());
        let plan = restored.plan_mode.unwrap();
        assert_eq!(plan.pre_permission_mode, "accept_edits");
        assert_eq!(plan.status, PlanModeStatus::Exploring);
    }

    #[test]
    fn all_plan_mode_status_variants_serialize_correctly() {
        let variants = [
            PlanModeStatus::Exploring,
            PlanModeStatus::Designing,
            PlanModeStatus::Reviewing,
            PlanModeStatus::Finalizing,
            PlanModeStatus::AwaitingApproval,
        ];
        for variant in &variants {
            let json = serde_json::to_string(variant).unwrap();
            let restored: PlanModeStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(variant, &restored, "round-trip failed for {:?}", variant);
        }
    }
}
