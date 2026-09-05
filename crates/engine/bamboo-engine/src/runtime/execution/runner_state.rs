//! Runner state types for background agent execution.
//!
//! Provides the `AgentRunner` and `AgentStatus` types that track the lifecycle
//! of an in-progress agent execution. These are used by the execution
//! orchestration layer across all background paths (HTTP execute, spawn, schedule).

use chrono::{DateTime, Utc};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use bamboo_agent_core::AgentEvent;

fn subagent_lifecycle_child_id(event: &AgentEvent) -> Option<&str> {
    match event {
        AgentEvent::SubAgentStarted {
            child_session_id, ..
        }
        | AgentEvent::SubAgentCompleted {
            child_session_id, ..
        } => Some(child_session_id),
        _ => None,
    }
}

/// Status of an agent execution runner.
///
/// Represents the lifecycle state of an agent run from initialization
/// through completion or error.
#[derive(Debug, Clone)]
pub enum AgentStatus {
    /// Agent is initialized but not yet running.
    Pending,

    /// Agent is currently executing.
    Running,

    /// Agent completed successfully.
    Completed,

    /// Agent execution was cancelled by user.
    Cancelled,

    /// Agent execution failed with an error message.
    Error(String),
}

/// Runner that manages agent execution for a session.
///
/// Each active agent run has an associated `AgentRunner` that coordinates
/// event broadcasting, cancellation, and status tracking.
///
/// # Event Broadcasting
///
/// Uses a broadcast channel to support multiple subscribers watching
/// the same agent run simultaneously.
///
/// # Cancellation
///
/// Provides a cancellation token that can be used to gracefully stop
/// an in-progress agent execution.
#[derive(Debug, Clone)]
pub struct AgentRunner {
    /// Generation fence and activity clock shared only by this run's producers.
    pub event_publication: std::sync::Arc<super::event_publication::EventPublication>,
    /// Broadcast sender for agent events.
    ///
    /// Allows multiple clients to subscribe to agent events
    /// via `event_sender.subscribe()`.
    pub event_sender: broadcast::Sender<AgentEvent>,

    /// Cancellation token for graceful shutdown.
    ///
    /// When triggered, the agent should stop execution at the
    /// next safe point.
    pub cancel_token: CancellationToken,

    /// Current status of the agent run.
    pub status: AgentStatus,

    /// Timestamp when the run was started.
    pub started_at: DateTime<Utc>,

    /// Timestamp when the run completed (if finished).
    pub completed_at: Option<DateTime<Utc>>,

    /// Last token budget event to replay for new subscribers.
    ///
    /// When a new client subscribes to an ongoing run, this
    /// allows them to receive the most recent token usage info.
    pub last_budget_event: Option<AgentEvent>,

    /// Small ring of critical state events (TaskListUpdated, SubAgent*, etc.)
    /// cached for replay to late/reconnecting subscribers.
    ///
    /// Bounded to [`CRITICAL_EVENTS_CAPACITY`] entries; oldest are evicted.
    pub last_critical_events: Vec<AgentEvent>,

    /// Name of the most recently executed tool (if any).
    /// Updated live during execution for diagnostic visibility.
    pub last_tool_name: Option<String>,

    /// Phase of the most recently executed tool: "begin", "finished", or "error".
    /// Updated live during execution for diagnostic visibility.
    pub last_tool_phase: Option<String>,

    /// Timestamp of the last event received during this run.
    /// Updated live during execution for liveness checks.
    pub last_event_at: Option<DateTime<Utc>>,

    /// Number of completed rounds (turns) so far.
    /// Updated live during execution for progress tracking.
    pub round_count: u32,

    /// Unique identifier for this execution run.
    /// Generated fresh for every `try_reserve_runner` call so that
    /// frontend SSE events can be matched to the correct run even
    /// across reconnects.
    pub run_id: String,
}

impl Default for AgentRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl AgentRunner {
    /// Includes lock-free event traffic as well as legacy diagnostic updates.
    pub fn last_activity_at(&self) -> Option<DateTime<Utc>> {
        self.last_event_at
            .max(self.event_publication.last_event_at())
    }
    /// Broadcast channel capacity for agent events.
    pub const EVENT_CHANNEL_CAPACITY: usize = 1000;

    /// Maximum number of critical events cached for late-subscriber replay.
    pub const CRITICAL_EVENTS_CAPACITY: usize = 32;

    /// Create a new agent runner with default settings.
    ///
    /// Initializes a broadcast channel, a fresh cancellation token,
    /// and `Pending` status.
    pub fn new() -> Self {
        let (event_sender, _) = broadcast::channel(Self::EVENT_CHANNEL_CAPACITY);
        Self {
            event_sender,
            event_publication: std::sync::Arc::default(),
            cancel_token: CancellationToken::new(),
            status: AgentStatus::Pending,
            started_at: Utc::now(),
            completed_at: None,
            last_budget_event: None,
            last_critical_events: Vec::new(),
            last_tool_name: None,
            last_tool_phase: None,
            last_event_at: None,
            round_count: 0,
            run_id: Uuid::new_v4().to_string(),
        }
    }

    /// Push a critical state event into the bounded replay cache.
    ///
    /// Sub-agent lifecycle entries are snapshots keyed by stable child id, not
    /// an event log: a rerunnable/resident child can produce many generations,
    /// and replaying older Start/Complete pairs makes the current generation
    /// ambiguous to reconnecting clients. Keep only the newest lifecycle state
    /// for each child while live subscribers still receive every event.
    ///
    /// If the remaining cache is full, the oldest entry is evicted.
    pub fn push_critical_event(&mut self, event: AgentEvent) {
        let lifecycle_id = match &event {
            AgentEvent::WorkflowActivated { event_id, .. }
            | AgentEvent::WorkflowDeactivated { event_id, .. } => Some(event_id),
            _ => None,
        };
        if lifecycle_id.is_some_and(|event_id| {
            self.last_critical_events
                .iter()
                .any(|existing| match existing {
                    AgentEvent::WorkflowActivated {
                        event_id: existing_id,
                        ..
                    }
                    | AgentEvent::WorkflowDeactivated {
                        event_id: existing_id,
                        ..
                    } => existing_id == event_id,
                    _ => false,
                })
        }) {
            return;
        }
        if let Some(child_session_id) = subagent_lifecycle_child_id(&event) {
            self.last_critical_events
                .retain(|existing| subagent_lifecycle_child_id(existing) != Some(child_session_id));
        }
        if self.last_critical_events.len() >= Self::CRITICAL_EVENTS_CAPACITY {
            self.last_critical_events.remove(0);
        }
        self.last_critical_events.push(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn started(child_session_id: &str, title: &str) -> AgentEvent {
        AgentEvent::SubAgentStarted {
            parent_session_id: "parent".to_string(),
            child_session_id: child_session_id.to_string(),
            title: Some(title.to_string()),
        }
    }

    fn completed(child_session_id: &str, status: &str) -> AgentEvent {
        AgentEvent::SubAgentCompleted {
            parent_session_id: "parent".to_string(),
            child_session_id: child_session_id.to_string(),
            status: status.to_string(),
            error: None,
        }
    }

    #[test]
    fn critical_replay_keeps_only_latest_generation_state_per_child() {
        let mut runner = AgentRunner::new();

        // A stable resident child may be reused repeatedly inside one parent
        // runner. Reconnect must see S3 only, never the ambiguous historical
        // sequence S1,C1,S2,C2,S3.
        for event in [
            started("resident", "generation-1"),
            completed("resident", "completed"),
            started("resident", "generation-2"),
            completed("resident", "completed"),
            started("resident", "generation-3"),
        ] {
            runner.push_critical_event(event);
        }

        assert_eq!(runner.last_critical_events.len(), 1);
        assert!(matches!(
            &runner.last_critical_events[0],
            AgentEvent::SubAgentStarted {
                child_session_id,
                title: Some(title),
                ..
            } if child_session_id == "resident" && title == "generation-3"
        ));

        runner.push_critical_event(completed("resident", "completed"));
        assert_eq!(runner.last_critical_events.len(), 1);
        assert!(matches!(
            &runner.last_critical_events[0],
            AgentEvent::SubAgentCompleted {
                child_session_id,
                status,
                ..
            } if child_session_id == "resident" && status == "completed"
        ));
    }

    #[test]
    fn subagent_coalescing_preserves_other_children_and_recency() {
        let mut runner = AgentRunner::new();
        runner.push_critical_event(started("child-a", "a1"));
        runner.push_critical_event(started("child-b", "b1"));
        runner.push_critical_event(completed("child-a", "completed"));

        assert_eq!(runner.last_critical_events.len(), 2);
        assert!(matches!(
            &runner.last_critical_events[0],
            AgentEvent::SubAgentStarted {
                child_session_id, ..
            } if child_session_id == "child-b"
        ));
        assert!(matches!(
            &runner.last_critical_events[1],
            AgentEvent::SubAgentCompleted {
                child_session_id, ..
            } if child_session_id == "child-a"
        ));
    }
}
