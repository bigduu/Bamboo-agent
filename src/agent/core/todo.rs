//! Task list types for task tracking in sessions.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Task item status.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum TaskItemStatus {
    #[serde(rename = "pending")]
    #[default]
    Pending,
    #[serde(rename = "in_progress")]
    InProgress,
    #[serde(rename = "completed")]
    Completed,
    #[serde(rename = "blocked")]
    Blocked,
}

/// High-level phase of a task item.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TaskPhase {
    Planning,
    #[default]
    Execution,
    Verification,
    Handoff,
}

/// Relative importance of a task item.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TaskPriority {
    Low,
    #[default]
    Medium,
    High,
    Critical,
}

/// Evidence kind attached to a task item.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TaskEvidenceKind {
    #[default]
    Note,
    ToolCall,
    File,
    Command,
    Test,
    Observation,
}

/// Structured evidence supporting task status.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct TaskEvidence {
    #[serde(default)]
    pub kind: TaskEvidenceKind,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none", alias = "ref")]
    pub reference: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub round: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub success: Option<bool>,
}

/// Blocker kind attached to a task item.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TaskBlockerKind {
    UserInput,
    Dependency,
    ToolFailure,
    External,
    #[default]
    Unknown,
}

/// Structured blocker details for a task item.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct TaskBlocker {
    #[serde(default)]
    pub kind: TaskBlockerKind,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiting_on: Option<String>,
}

/// History entry for task state changes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskTransition {
    pub from_status: TaskItemStatus,
    pub to_status: TaskItemStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub round: Option<u32>,
    pub changed_at: DateTime<Utc>,
}

/// Task item for task tracking.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TaskItem {
    /// Unique identifier for the task item.
    pub id: String,
    /// Human-readable description of the task.
    pub description: String,
    /// Current status of the item.
    pub status: TaskItemStatus,
    /// IDs of other items this item depends on.
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Additional notes or context.
    #[serde(default)]
    pub notes: String,
    /// Present-progress phrasing for the active task.
    #[serde(default, skip_serializing_if = "Option::is_none", alias = "activeForm")]
    pub active_form: Option<String>,
    /// Optional parent task ID when this item is part of a larger task tree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    /// Phase of work for the task.
    #[serde(default)]
    pub phase: TaskPhase,
    /// Relative priority of the task.
    #[serde(default)]
    pub priority: TaskPriority,
    /// Explicit completion criteria for the task.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub completion_criteria: Vec<String>,
    /// Structured evidence gathered while working on the task.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence: Vec<TaskEvidence>,
    /// Structured blocker information.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blockers: Vec<TaskBlocker>,
    /// Transition history for this task item.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transitions: Vec<TaskTransition>,
}

impl TaskItem {
    /// Append notes while preserving previous content.
    pub fn append_notes(&mut self, note: &str) {
        let note = note.trim();
        if note.is_empty() {
            return;
        }
        if !self.notes.is_empty() {
            self.notes.push('\n');
        }
        self.notes.push_str(note);
    }

    /// Return the most useful active-form display for the task.
    pub fn effective_active_form(&self) -> Option<&str> {
        self.active_form.as_deref().or_else(|| {
            let notes = self.notes.trim();
            if matches!(self.status, TaskItemStatus::InProgress) && !notes.is_empty() {
                Some(notes)
            } else {
                None
            }
        })
    }

    /// Add structured evidence if it contains useful content.
    pub fn push_evidence(&mut self, evidence: TaskEvidence) {
        if evidence.summary.trim().is_empty() {
            return;
        }
        self.evidence.push(evidence);
    }

    /// Add a blocker if it is not empty and not already recorded.
    pub fn add_blocker(&mut self, blocker: TaskBlocker) {
        if blocker.summary.trim().is_empty() {
            return;
        }
        if self.blockers.iter().any(|existing| {
            existing.kind == blocker.kind
                && existing.summary == blocker.summary
                && existing.waiting_on == blocker.waiting_on
        }) {
            return;
        }
        self.blockers.push(blocker);
    }

    /// Transition to a new status and record the change.
    pub fn transition_to(
        &mut self,
        status: TaskItemStatus,
        reason: Option<&str>,
        round: Option<u32>,
    ) -> bool {
        let reason = reason.map(str::trim).filter(|value| !value.is_empty());

        if self.status == status {
            if let Some(reason) = reason {
                self.append_notes(reason);
            }
            return false;
        }

        let transition = TaskTransition {
            from_status: self.status.clone(),
            to_status: status.clone(),
            reason: reason.map(ToOwned::to_owned),
            round,
            changed_at: Utc::now(),
        };
        self.status = status;
        if let Some(reason) = transition.reason.as_deref() {
            self.append_notes(reason);
        }
        self.transitions.push(transition);
        true
    }

    /// Whether all declared dependencies are already completed.
    pub fn dependencies_ready(&self, completed_ids: &HashSet<String>) -> bool {
        self.depends_on
            .iter()
            .all(|dependency_id| completed_ids.contains(dependency_id))
    }
}

/// Task list for a session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskList {
    /// Session ID this task list belongs to.
    pub session_id: String,
    /// Title of the task list.
    pub title: String,
    /// List of task items.
    pub items: Vec<TaskItem>,
    /// When the list was created.
    pub created_at: DateTime<Utc>,
    /// When the list was last updated.
    pub updated_at: DateTime<Utc>,
}

pub fn task_status_label(status: &TaskItemStatus) -> &'static str {
    match status {
        TaskItemStatus::Pending => "pending",
        TaskItemStatus::InProgress => "in_progress",
        TaskItemStatus::Completed => "completed",
        TaskItemStatus::Blocked => "blocked",
    }
}

pub fn task_status_icon(status: &TaskItemStatus) -> &'static str {
    match status {
        TaskItemStatus::Pending => "[ ]",
        TaskItemStatus::InProgress => "[/]",
        TaskItemStatus::Completed => "[x]",
        TaskItemStatus::Blocked => "[!]",
    }
}

pub fn task_phase_label(phase: &TaskPhase) -> &'static str {
    match phase {
        TaskPhase::Planning => "planning",
        TaskPhase::Execution => "execution",
        TaskPhase::Verification => "verification",
        TaskPhase::Handoff => "handoff",
    }
}

pub fn task_priority_label(priority: &TaskPriority) -> &'static str {
    match priority {
        TaskPriority::Low => "low",
        TaskPriority::Medium => "medium",
        TaskPriority::High => "high",
        TaskPriority::Critical => "critical",
    }
}

pub fn task_evidence_kind_label(kind: &TaskEvidenceKind) -> &'static str {
    match kind {
        TaskEvidenceKind::Note => "note",
        TaskEvidenceKind::ToolCall => "tool_call",
        TaskEvidenceKind::File => "file",
        TaskEvidenceKind::Command => "command",
        TaskEvidenceKind::Test => "test",
        TaskEvidenceKind::Observation => "observation",
    }
}

pub fn task_blocker_kind_label(kind: &TaskBlockerKind) -> &'static str {
    match kind {
        TaskBlockerKind::UserInput => "user_input",
        TaskBlockerKind::Dependency => "dependency",
        TaskBlockerKind::ToolFailure => "tool_failure",
        TaskBlockerKind::External => "external",
        TaskBlockerKind::Unknown => "unknown",
    }
}

impl TaskList {
    /// Format task list for display in system prompt.
    pub fn format_for_prompt(&self) -> String {
        if self.items.is_empty() {
            return String::new();
        }

        let mut output = format!("\n\n## Current Task List: {}\n", self.title);

        for item in &self.items {
            output.push_str(&format!(
                "\n{} {}: {}",
                task_status_icon(&item.status),
                item.id,
                item.description
            ));

            let mut tags = Vec::new();
            if item.phase != TaskPhase::Execution {
                tags.push(format!("phase={}", task_phase_label(&item.phase)));
            }
            if item.priority != TaskPriority::Medium {
                tags.push(format!("priority={}", task_priority_label(&item.priority)));
            }
            if let Some(parent_id) = item.parent_id.as_deref().filter(|value| !value.is_empty()) {
                tags.push(format!("parent={parent_id}"));
            }
            if !item.depends_on.is_empty() {
                tags.push(format!("depends_on={}", item.depends_on.join(", ")));
            }
            if !tags.is_empty() {
                output.push_str(&format!(" [{}]", tags.join(" | ")));
            }

            if matches!(item.status, TaskItemStatus::InProgress) {
                if let Some(active_form) = item.effective_active_form() {
                    output.push_str(&format!(
                        "\n    Active: {}",
                        truncate_for_prompt(active_form, 160)
                    ));
                }
            }

            if !item.completion_criteria.is_empty() {
                let criteria = item
                    .completion_criteria
                    .iter()
                    .take(3)
                    .map(|criterion| truncate_for_prompt(criterion, 60))
                    .collect::<Vec<_>>()
                    .join(" | ");
                output.push_str(&format!("\n    Criteria: {criteria}"));
                if item.completion_criteria.len() > 3 {
                    output.push_str(&format!(" | +{} more", item.completion_criteria.len() - 3));
                }
            }

            if let Some(blocker) = item.blockers.last() {
                let mut blocker_line = truncate_for_prompt(&blocker.summary, 140);
                if let Some(waiting_on) = blocker
                    .waiting_on
                    .as_deref()
                    .filter(|value| !value.is_empty())
                {
                    blocker_line.push_str(&format!(
                        " (waiting_on: {})",
                        truncate_for_prompt(waiting_on, 60)
                    ));
                }
                output.push_str(&format!("\n    Blocked by: {blocker_line}"));
            }

            if let Some(evidence) = item.evidence.last() {
                let mut evidence_line = truncate_for_prompt(&evidence.summary, 140);
                if let Some(reference) = evidence
                    .reference
                    .as_deref()
                    .filter(|value| !value.is_empty())
                {
                    evidence_line
                        .push_str(&format!(" [ref: {}]", truncate_for_prompt(reference, 60)));
                }
                output.push_str(&format!(
                    "\n    Latest evidence [{}]: {}",
                    task_evidence_kind_label(&evidence.kind),
                    evidence_line
                ));
            }

            let notes = item.notes.trim();
            let active_form = item.active_form.as_deref().map(str::trim);
            if !notes.is_empty() && Some(notes) != active_form {
                output.push_str(&format!("\n    Notes: {}", truncate_for_prompt(notes, 160)));
            }
        }

        let completed = self
            .items
            .iter()
            .filter(|item| item.status == TaskItemStatus::Completed)
            .count();
        let total = self.items.len();
        output.push_str(&format!(
            "\n\nProgress: {}/{} tasks completed",
            completed, total
        ));

        output
    }

    /// Update a task item status.
    pub fn update_item(
        &mut self,
        item_id: &str,
        status: TaskItemStatus,
        notes: Option<&str>,
    ) -> Result<String, String> {
        if let Some(item) = self.items.iter_mut().find(|item| item.id == item_id) {
            item.transition_to(status, notes, None);
            self.updated_at = Utc::now();
            Ok(format!("Updated item '{}'", item_id))
        } else {
            Err(format!("Task item '{}' not found", item_id))
        }
    }
}

fn truncate_for_prompt(value: &str, max_chars: usize) -> String {
    let trimmed = value.trim().replace('\n', " ");
    let char_count = trimmed.chars().count();
    if char_count <= max_chars {
        return trimmed;
    }

    let truncated: String = trimmed.chars().take(max_chars).collect();
    format!("{}…", truncated.trim_end())
}
