use bamboo_domain::{TaskItem, TaskItemStatus, TaskList};

use super::{TaskLoopContext, TaskLoopItem};

fn clone_loop_item_into_task_item(loop_item: &TaskLoopItem) -> TaskItem {
    TaskItem {
        id: loop_item.id.clone(),
        description: loop_item.description.clone(),
        status: loop_item.status.clone(),
        depends_on: loop_item.depends_on.clone(),
        notes: loop_item.notes.clone(),
        active_form: loop_item.active_form.clone(),
        parent_id: loop_item.parent_id.clone(),
        phase: loop_item.phase.clone(),
        priority: loop_item.priority.clone(),
        completion_criteria: loop_item.completion_criteria.clone(),
        evidence: loop_item.evidence.clone(),
        blockers: loop_item.blockers.clone(),
        transitions: loop_item.transitions.clone(),
    }
}

fn into_task_item(loop_item: TaskLoopItem) -> TaskItem {
    TaskItem {
        id: loop_item.id,
        description: loop_item.description,
        status: loop_item.status,
        depends_on: loop_item.depends_on,
        notes: loop_item.notes,
        active_form: loop_item.active_form,
        parent_id: loop_item.parent_id,
        phase: loop_item.phase,
        priority: loop_item.priority,
        completion_criteria: loop_item.completion_criteria,
        evidence: loop_item.evidence,
        blockers: loop_item.blockers,
        transitions: loop_item.transitions,
    }
}

impl TaskLoopContext {
    /// Create `TaskLoopContext` from the session's task list.
    pub fn from_session(session: &bamboo_agent_core::Session) -> Option<Self> {
        session.task_list.as_ref().map(|task_list| {
            // Preserve version from existing task_list metadata if available.
            // This prevents version reset across multiple executions.
            let existing_version = session
                .task_list_version_meta()
                .or_else(|| session.todo_list_version_meta())
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);

            let items: Vec<TaskLoopItem> = task_list
                .items
                .iter()
                .map(|item| TaskLoopItem {
                    id: item.id.clone(),
                    description: item.description.clone(),
                    status: item.status.clone(),
                    depends_on: item.depends_on.clone(),
                    notes: item.notes.clone(),
                    active_form: item.active_form.clone(),
                    parent_id: item.parent_id.clone(),
                    phase: item.phase.clone(),
                    priority: item.priority.clone(),
                    completion_criteria: item.completion_criteria.clone(),
                    evidence: item.evidence.clone(),
                    blockers: item.blockers.clone(),
                    transitions: item.transitions.clone(),
                    tool_calls: Vec::new(),
                    started_at_round: item
                        .transitions
                        .iter()
                        .find(|transition| {
                            matches!(transition.to_status, TaskItemStatus::InProgress)
                        })
                        .and_then(|transition| transition.round),
                    completed_at_round: item
                        .transitions
                        .iter()
                        .rev()
                        .find(|transition| {
                            matches!(transition.to_status, TaskItemStatus::Completed)
                        })
                        .and_then(|transition| transition.round),
                })
                .collect();

            Self {
                session_id: task_list.session_id.clone(),
                active_item_id: items
                    .iter()
                    .find(|item| matches!(item.status, TaskItemStatus::InProgress))
                    .map(|item| item.id.clone()),
                items,
                current_round: 0,
                max_rounds: 200,
                created_at: task_list.created_at,
                updated_at: task_list.updated_at,
                version: existing_version,
                task_list_dirty: false,
            }
        })
    }

    /// Clone into a `TaskList`, preserving the provided title.
    pub fn to_task_list_with_title(&self, title: impl Into<String>) -> TaskList {
        TaskList {
            session_id: self.session_id.clone(),
            title: title.into(),
            items: self
                .items
                .iter()
                .map(clone_loop_item_into_task_item)
                .collect(),
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }

    /// Convert back to `TaskList` for persistence.
    pub fn into_task_list(self) -> TaskList {
        self.into_task_list_with_title("Agent Tasks")
    }

    /// Convert back to `TaskList` for persistence with an explicit title.
    pub fn into_task_list_with_title(self, title: impl Into<String>) -> TaskList {
        TaskList {
            session_id: self.session_id,
            title: title.into(),
            items: self.items.into_iter().map(into_task_item).collect(),
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}
