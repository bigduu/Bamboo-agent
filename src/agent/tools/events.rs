//! Structured event tracking for tool executions.
//!
//! Inspired by Codex's `ToolEmitter` pattern, this module provides a way to
//! trace tool calls through their lifecycle: begin → execute → finish.
//!
//! Events are emitted via the existing `AgentEvent` channel so they integrate
//! seamlessly with the agent loop's event pipeline.

use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::agent::core::AgentEvent;

/// Phase of a tool execution lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolEventPhase {
    Begin,
    Executing,
    Finished,
    Error,
    Cancelled,
}

/// A structured event emitted during tool execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolEvent {
    /// Tool call ID (matches `ToolCall.id`)
    pub call_id: String,
    /// Canonical tool name
    pub tool_name: String,
    /// Current lifecycle phase
    pub phase: ToolEventPhase,
    /// Wall-clock duration since the call began (None for Begin phase)
    pub elapsed_ms: Option<u64>,
    /// Whether the tool is mutating (writes files, runs commands, etc.)
    pub is_mutating: bool,
    /// Whether the execution was auto-approved (no user prompt)
    pub auto_approved: bool,
    /// Human-readable summary (e.g. file paths affected, command preview)
    pub summary: Option<String>,
    /// Error message if phase == Error
    pub error: Option<String>,
}

impl ToolEvent {
    /// Convert this event into an [`AgentEvent::ToolLifecycle`] for streaming
    /// through the agent event channel to the UI.
    pub fn into_agent_event(self) -> AgentEvent {
        let phase_str = match self.phase {
            ToolEventPhase::Begin => "begin",
            ToolEventPhase::Executing => "executing",
            ToolEventPhase::Finished => "finished",
            ToolEventPhase::Error => "error",
            ToolEventPhase::Cancelled => "cancelled",
        };
        AgentEvent::ToolLifecycle {
            tool_call_id: self.call_id,
            tool_name: self.tool_name,
            phase: phase_str.to_string(),
            elapsed_ms: self.elapsed_ms,
            is_mutating: self.is_mutating,
            auto_approved: self.auto_approved,
            summary: self.summary,
            error: self.error,
        }
    }
}

/// Emitter that tracks a single tool call through its lifecycle.
///
/// Usage pattern:
/// ```ignore
/// let emitter = ToolEmitter::new("call_123", "Edit", true);
/// emitter.begin();
/// // ... do work ...
/// emitter.finish(Some("Edited 2 files"));
/// ```
#[derive(Debug)]
pub struct ToolEmitter {
    call_id: String,
    tool_name: String,
    is_mutating: bool,
    auto_approved: bool,
    started_at: Instant,
    events: Vec<ToolEvent>,
}

impl ToolEmitter {
    /// Create a new emitter for a tool call.
    pub fn new(call_id: &str, tool_name: &str, is_mutating: bool) -> Self {
        Self {
            call_id: call_id.to_string(),
            tool_name: tool_name.to_string(),
            is_mutating,
            auto_approved: false,
            started_at: Instant::now(),
            events: Vec::new(),
        }
    }

    /// Mark the call as auto-approved (no user approval needed).
    pub fn set_auto_approved(&mut self, auto_approved: bool) {
        self.auto_approved = auto_approved;
    }

    /// Emit a "begin" event.
    pub fn begin(&mut self) -> &ToolEvent {
        let event = ToolEvent {
            call_id: self.call_id.clone(),
            tool_name: self.tool_name.clone(),
            phase: ToolEventPhase::Begin,
            elapsed_ms: None,
            is_mutating: self.is_mutating,
            auto_approved: self.auto_approved,
            summary: None,
            error: None,
        };
        self.events.push(event);
        self.events.last().unwrap()
    }

    /// Emit a "finished" event with an optional summary.
    pub fn finish(&mut self, summary: Option<String>) -> &ToolEvent {
        let elapsed = self.started_at.elapsed();
        let event = ToolEvent {
            call_id: self.call_id.clone(),
            tool_name: self.tool_name.clone(),
            phase: ToolEventPhase::Finished,
            elapsed_ms: Some(elapsed.as_millis() as u64),
            is_mutating: self.is_mutating,
            auto_approved: self.auto_approved,
            summary,
            error: None,
        };
        self.events.push(event);
        self.events.last().unwrap()
    }

    /// Emit an "error" event.
    pub fn error(&mut self, error: String) -> &ToolEvent {
        let elapsed = self.started_at.elapsed();
        let event = ToolEvent {
            call_id: self.call_id.clone(),
            tool_name: self.tool_name.clone(),
            phase: ToolEventPhase::Error,
            elapsed_ms: Some(elapsed.as_millis() as u64),
            is_mutating: self.is_mutating,
            auto_approved: self.auto_approved,
            summary: None,
            error: Some(error),
        };
        self.events.push(event);
        self.events.last().unwrap()
    }

    /// Emit a "cancelled" event (e.g. user denied approval).
    pub fn cancelled(&mut self, reason: Option<String>) -> &ToolEvent {
        let elapsed = self.started_at.elapsed();
        let event = ToolEvent {
            call_id: self.call_id.clone(),
            tool_name: self.tool_name.clone(),
            phase: ToolEventPhase::Cancelled,
            elapsed_ms: Some(elapsed.as_millis() as u64),
            is_mutating: self.is_mutating,
            auto_approved: self.auto_approved,
            summary: reason,
            error: None,
        };
        self.events.push(event);
        self.events.last().unwrap()
    }

    /// Get the elapsed time since the emitter was created.
    pub fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }

    /// Get all recorded events.
    pub fn events(&self) -> &[ToolEvent] {
        &self.events
    }

    /// Get the tool call ID.
    pub fn call_id(&self) -> &str {
        &self.call_id
    }

    /// Get the tool name.
    pub fn tool_name(&self) -> &str {
        &self.tool_name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_emitter_lifecycle() {
        let mut emitter = ToolEmitter::new("call_1", "Edit", true);
        emitter.set_auto_approved(true);

        let begin = emitter.begin();
        assert_eq!(begin.phase, ToolEventPhase::Begin);
        assert!(begin.is_mutating);
        assert!(begin.auto_approved);
        assert!(begin.elapsed_ms.is_none());

        let finish = emitter.finish(Some("Updated 2 files".to_string()));
        assert_eq!(finish.phase, ToolEventPhase::Finished);
        assert!(finish.elapsed_ms.is_some());
        assert_eq!(finish.summary.as_deref(), Some("Updated 2 files"));

        assert_eq!(emitter.events().len(), 2);
    }

    #[test]
    fn test_emitter_error() {
        let mut emitter = ToolEmitter::new("call_2", "Bash", true);
        emitter.begin();
        let err = emitter.error("Permission denied".to_string());
        assert_eq!(err.phase, ToolEventPhase::Error);
        assert_eq!(err.error.as_deref(), Some("Permission denied"));
        assert_eq!(emitter.events().len(), 2);
    }

    #[test]
    fn test_emitter_cancelled() {
        let mut emitter = ToolEmitter::new("call_3", "Write", true);
        emitter.begin();
        let cancel = emitter.cancelled(Some("User denied".to_string()));
        assert_eq!(cancel.phase, ToolEventPhase::Cancelled);
        assert_eq!(cancel.summary.as_deref(), Some("User denied"));
    }

    #[test]
    fn test_non_mutating_tool() {
        let mut emitter = ToolEmitter::new("call_4", "Read", false);
        let begin = emitter.begin();
        assert!(!begin.is_mutating);
        assert!(!begin.auto_approved);
    }

    #[test]
    fn test_serialization() {
        let event = ToolEvent {
            call_id: "c1".to_string(),
            tool_name: "Bash".to_string(),
            phase: ToolEventPhase::Finished,
            elapsed_ms: Some(150),
            is_mutating: true,
            auto_approved: false,
            summary: Some("Ran command".to_string()),
            error: None,
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"phase\":\"finished\""));
        assert!(json.contains("\"elapsed_ms\":150"));
        let deserialized: ToolEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.phase, ToolEventPhase::Finished);
    }

    #[test]
    fn test_into_agent_event_begin() {
        let mut emitter = ToolEmitter::new("call_99", "Read", false);
        let event = emitter.begin().clone();
        let agent_event = event.into_agent_event();

        match agent_event {
            AgentEvent::ToolLifecycle {
                tool_call_id,
                tool_name,
                phase,
                elapsed_ms,
                is_mutating,
                auto_approved,
                ..
            } => {
                assert_eq!(tool_call_id, "call_99");
                assert_eq!(tool_name, "Read");
                assert_eq!(phase, "begin");
                assert!(elapsed_ms.is_none());
                assert!(!is_mutating);
                assert!(!auto_approved);
            }
            _ => panic!("Expected ToolLifecycle variant"),
        }
    }

    #[test]
    fn test_into_agent_event_finished() {
        let mut emitter = ToolEmitter::new("call_100", "Bash", true);
        emitter.set_auto_approved(false);
        emitter.begin();
        std::thread::sleep(std::time::Duration::from_millis(5));
        let event = emitter.finish(Some("done".to_string())).clone();
        let agent_event = event.into_agent_event();

        match agent_event {
            AgentEvent::ToolLifecycle {
                phase,
                elapsed_ms,
                is_mutating,
                summary,
                ..
            } => {
                assert_eq!(phase, "finished");
                assert!(elapsed_ms.unwrap() >= 5);
                assert!(is_mutating);
                assert_eq!(summary.as_deref(), Some("done"));
            }
            _ => panic!("Expected ToolLifecycle variant"),
        }
    }

    #[test]
    fn test_into_agent_event_error() {
        let mut emitter = ToolEmitter::new("call_101", "Write", true);
        emitter.begin();
        let event = emitter.error("Permission denied".to_string()).clone();
        let agent_event = event.into_agent_event();

        match agent_event {
            AgentEvent::ToolLifecycle { phase, error, .. } => {
                assert_eq!(phase, "error");
                assert_eq!(error.as_deref(), Some("Permission denied"));
            }
            _ => panic!("Expected ToolLifecycle variant"),
        }
    }

    #[test]
    fn test_into_agent_event_cancelled() {
        let mut emitter = ToolEmitter::new("call_102", "Edit", true);
        emitter.begin();
        let event = emitter.cancelled(Some("User denied".to_string())).clone();
        let agent_event = event.into_agent_event();

        match agent_event {
            AgentEvent::ToolLifecycle { phase, summary, .. } => {
                assert_eq!(phase, "cancelled");
                assert_eq!(summary.as_deref(), Some("User denied"));
            }
            _ => panic!("Expected ToolLifecycle variant"),
        }
    }

    #[test]
    fn test_agent_event_serialization_roundtrip() {
        let event = ToolEvent {
            call_id: "c1".to_string(),
            tool_name: "Bash".to_string(),
            phase: ToolEventPhase::Finished,
            elapsed_ms: Some(42),
            is_mutating: true,
            auto_approved: false,
            summary: Some("ok".to_string()),
            error: None,
        };
        let agent_event = event.into_agent_event();
        let json = serde_json::to_string(&agent_event).unwrap();
        assert!(json.contains("\"type\":\"tool_lifecycle\""));
        assert!(json.contains("\"phase\":\"finished\""));
        assert!(json.contains("\"elapsed_ms\":42"));
    }
}
