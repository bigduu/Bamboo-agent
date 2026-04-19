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
    pub total_tool_calls: u32,
    pub total_prompt_tokens: u64,
    pub total_completion_tokens: u64,
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
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct ChildSessionRuntimeState {
    pub active_children: u32,
    pub completed_children: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub active_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub completed_ids: Vec<String>,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub checkpoints: Vec<HookCheckpoint>,
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
            checkpoints: Vec::new(),
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
            checkpoints: Vec::new(),
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
        state.children.active_children = 2;
        state.children.active_ids = vec!["child-1".to_string(), "child-2".to_string()];
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
}
