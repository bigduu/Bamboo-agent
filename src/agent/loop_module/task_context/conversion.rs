use crate::agent::core::{TaskItem, TaskList};

use super::{TaskLoopContext, TaskLoopItem};

impl TaskLoopContext {
    /// Create `TaskLoopContext` from the session's task list.
    pub fn from_session(session: &crate::agent::core::Session) -> Option<Self> {
        session.task_list.as_ref().map(|task_list| {
            // Preserve version from existing task_list metadata if available.
            // This prevents version reset across multiple executions.
            let existing_version = session
                .metadata
                .get("task_list_version")
                .or_else(|| session.metadata.get("todo_list_version"))
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);

            Self {
                session_id: task_list.session_id.clone(),
                items: task_list
                    .items
                    .iter()
                    .map(|item| TaskLoopItem {
                        id: item.id.clone(),
                        description: item.description.clone(),
                        status: item.status.clone(),
                        tool_calls: Vec::new(),
                        started_at_round: None,
                        completed_at_round: None,
                    })
                    .collect(),
                active_item_id: None,
                current_round: 0,
                max_rounds: 200,
                created_at: task_list.created_at,
                updated_at: task_list.updated_at,
                version: existing_version,
            }
        })
    }

    /// Convert back to `TaskList` for persistence.
    pub fn into_task_list(self) -> TaskList {
        TaskList {
            session_id: self.session_id,
            title: "Agent Tasks".to_string(),
            items: self
                .items
                .into_iter()
                .map(|loop_item| TaskItem {
                    id: loop_item.id,
                    description: loop_item.description,
                    status: loop_item.status,
                    depends_on: Vec::new(),
                    notes: String::new(),
                })
                .collect(),
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}
