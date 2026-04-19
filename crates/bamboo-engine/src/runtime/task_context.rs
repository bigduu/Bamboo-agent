//! TaskList context for Agent Loop integration
//!
//! This module provides TaskLoopContext which integrates TaskList
//! as a first-class citizen in the Agent Loop, similar to Token Budget.

use bamboo_domain::task::{
    TaskBlocker, TaskEvidence, TaskPhase, TaskPriority, TaskTransition,
};
use bamboo_domain::TaskItemStatus;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

mod auto_status;
mod conversion;
mod prompt;
mod tracking;

/// TaskList context for Agent Loop
///
/// Acts as a first-class citizen in the agent loop, tracking
/// task progress throughout the entire conversation lifecycle.
#[derive(Debug, Clone)]
pub struct TaskLoopContext {
    /// Session ID
    pub session_id: String,

    /// Task items with execution tracking
    pub items: Vec<TaskLoopItem>,

    /// Currently active task item ID
    pub active_item_id: Option<String>,

    /// Current round number
    pub current_round: u32,

    /// Maximum rounds allowed
    pub max_rounds: u32,

    /// Creation timestamp
    pub created_at: DateTime<Utc>,

    /// Last update timestamp
    pub updated_at: DateTime<Utc>,

    /// Version number for conflict detection
    pub version: u64,
}

/// Task item with execution tracking
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TaskLoopItem {
    /// Item ID
    pub id: String,

    /// Item description
    pub description: String,

    /// Item status
    pub status: TaskItemStatus,

    /// IDs of other items this item depends on.
    pub depends_on: Vec<String>,

    /// Additional notes or context.
    pub notes: String,

    /// Present-progress phrasing for the active task.
    pub active_form: Option<String>,

    /// Optional parent task ID when this item is part of a larger task tree.
    pub parent_id: Option<String>,

    /// Phase of work for the task.
    pub phase: TaskPhase,

    /// Relative priority of the task.
    pub priority: TaskPriority,

    /// Explicit completion criteria for the task.
    pub completion_criteria: Vec<String>,

    /// Structured evidence gathered while working on the task.
    pub evidence: Vec<TaskEvidence>,

    /// Structured blocker information.
    pub blockers: Vec<TaskBlocker>,

    /// Transition history for this task item.
    pub transitions: Vec<TaskTransition>,

    /// Tool call history (tracks execution process)
    pub tool_calls: Vec<ToolCallRecord>,

    /// Round when item was started
    pub started_at_round: Option<u32>,

    /// Round when item was completed
    pub completed_at_round: Option<u32>,
}

/// Record of a tool call execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallRecord {
    /// Round number
    pub round: u32,

    /// Tool name
    pub tool_name: String,

    /// Whether the call succeeded
    pub success: bool,

    /// Timestamp
    pub timestamp: DateTime<Utc>,
}

impl TaskLoopContext {
    /// Check if all items are completed
    pub fn is_all_completed(&self) -> bool {
        !self.items.is_empty()
            && self
                .items
                .iter()
                .all(|item| matches!(item.status, TaskItemStatus::Completed))
    }

    fn completed_item_ids(&self) -> HashSet<String> {
        self.items
            .iter()
            .filter(|item| matches!(item.status, TaskItemStatus::Completed))
            .map(|item| item.id.clone())
            .collect()
    }

    fn unresolved_dependencies(&self, depends_on: &[String]) -> Vec<String> {
        let completed = self.completed_item_ids();
        depends_on
            .iter()
            .filter(|dependency| !completed.contains(*dependency))
            .cloned()
            .collect()
    }

    fn append_item_notes(item: &mut TaskLoopItem, note: &str) {
        let note = note.trim();
        if note.is_empty() {
            return;
        }
        if !item.notes.is_empty() {
            item.notes.push('\n');
        }
        item.notes.push_str(note);
    }

    fn transition_item(
        item: &mut TaskLoopItem,
        status: TaskItemStatus,
        reason: Option<&str>,
        round: Option<u32>,
    ) -> bool {
        let reason = reason.map(str::trim).filter(|value| !value.is_empty());
        if item.status == status {
            if let Some(reason) = reason {
                Self::append_item_notes(item, reason);
            }
            return false;
        }

        let transition = TaskTransition {
            from_status: item.status.clone(),
            to_status: status.clone(),
            reason: reason.map(ToOwned::to_owned),
            round,
            changed_at: Utc::now(),
        };
        item.status = status.clone();

        if matches!(status, TaskItemStatus::InProgress) && item.started_at_round.is_none() {
            item.started_at_round = round;
        }
        if matches!(status, TaskItemStatus::Completed) {
            item.completed_at_round = round;
        }
        if let Some(reason) = transition.reason.as_deref() {
            Self::append_item_notes(item, reason);
        }

        item.transitions.push(transition);
        true
    }

    fn add_item_blocker(item: &mut TaskLoopItem, blocker: TaskBlocker) {
        if blocker.summary.trim().is_empty() {
            return;
        }
        if item.blockers.iter().any(|existing| {
            existing.kind == blocker.kind
                && existing.summary == blocker.summary
                && existing.waiting_on == blocker.waiting_on
        }) {
            return;
        }
        item.blockers.push(blocker);
    }

    fn push_item_evidence(item: &mut TaskLoopItem, evidence: TaskEvidence) {
        if evidence.summary.trim().is_empty() {
            return;
        }
        item.evidence.push(evidence);
    }
}

#[cfg(test)]
mod tests;
