//! Runner state types for background agent execution.
//!
//! Provides the `AgentRunner` and `AgentStatus` types that track the lifecycle
//! of an in-progress agent execution. These are used by the execution
//! orchestration layer across all background paths (HTTP execute, spawn, schedule).

use chrono::{DateTime, Utc};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use bamboo_application_agent::AgentEvent;

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
}

impl Default for AgentRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl AgentRunner {
    /// Broadcast channel capacity for agent events.
    pub const EVENT_CHANNEL_CAPACITY: usize = 1000;

    /// Create a new agent runner with default settings.
    ///
    /// Initializes a broadcast channel, a fresh cancellation token,
    /// and `Pending` status.
    pub fn new() -> Self {
        let (event_sender, _) = broadcast::channel(Self::EVENT_CHANNEL_CAPACITY);
        Self {
            event_sender,
            cancel_token: CancellationToken::new(),
            status: AgentStatus::Pending,
            started_at: Utc::now(),
            completed_at: None,
            last_budget_event: None,
        }
    }
}
