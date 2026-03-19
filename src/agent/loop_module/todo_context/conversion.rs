use crate::agent::core::todo::{TodoItem, TodoList};

use super::{TodoLoopContext, TodoLoopItem};

impl TodoLoopContext {
    /// Create TodoLoopContext from Session's TodoList
    pub fn from_session(session: &crate::agent::core::Session) -> Option<Self> {
        session.todo_list.as_ref().map(|todo_list| {
            // Preserve version from existing todo_list metadata if available.
            // This prevents version reset across multiple executions.
            let existing_version = session
                .metadata
                .get("todo_list_version")
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);

            Self {
                session_id: todo_list.session_id.clone(),
                items: todo_list
                    .items
                    .iter()
                    .map(|item| TodoLoopItem {
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
                created_at: todo_list.created_at,
                updated_at: todo_list.updated_at,
                version: existing_version,
            }
        })
    }

    /// Convert back to TodoList for persistence
    pub fn into_todo_list(self) -> TodoList {
        TodoList {
            session_id: self.session_id,
            title: "Agent Tasks".to_string(),
            items: self
                .items
                .into_iter()
                .map(|loop_item| TodoItem {
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
