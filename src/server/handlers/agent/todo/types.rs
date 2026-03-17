use serde::Serialize;

use crate::agent::core::{TodoItem, TodoItemStatus, TodoList};

/// Todo item response for frontend
#[derive(Serialize)]
pub struct TodoItemResponse {
    pub id: String,
    pub description: String,
    pub status: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub notes: String,
}

/// Todo list response for frontend
#[derive(Serialize)]
pub struct TodoListResponse {
    pub session_id: String,
    pub title: String,
    pub items: Vec<TodoItemResponse>,
    pub progress: TodoProgress,
}

/// Progress information
#[derive(Serialize)]
pub struct TodoProgress {
    pub completed: usize,
    pub total: usize,
    pub percentage: u8,
}

pub(super) fn to_todo_list_response(todo_list: &TodoList) -> TodoListResponse {
    let items = todo_list
        .items
        .iter()
        .map(TodoItemResponse::from_item)
        .collect();
    let completed = todo_list
        .items
        .iter()
        .filter(|item| matches!(item.status, TodoItemStatus::Completed))
        .count();
    let total = todo_list.items.len();

    TodoListResponse {
        session_id: todo_list.session_id.clone(),
        title: todo_list.title.clone(),
        items,
        progress: TodoProgress {
            completed,
            total,
            percentage: completion_percentage(completed, total),
        },
    }
}

pub(super) fn completion_percentage(completed: usize, total: usize) -> u8 {
    if total == 0 {
        return 0;
    }
    ((completed as f32 / total as f32) * 100.0) as u8
}

pub(super) fn todo_status_label(status: &TodoItemStatus) -> &'static str {
    match status {
        TodoItemStatus::Pending => "pending",
        TodoItemStatus::InProgress => "in_progress",
        TodoItemStatus::Completed => "completed",
        TodoItemStatus::Blocked => "blocked",
    }
}

impl TodoItemResponse {
    fn from_item(item: &TodoItem) -> Self {
        Self {
            id: item.id.clone(),
            description: item.description.clone(),
            status: todo_status_label(&item.status).to_string(),
            depends_on: item.depends_on.clone(),
            notes: item.notes.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::core::TodoItemStatus;

    #[test]
    fn test_todo_item_response_serialization() {
        let item = TodoItemResponse {
            id: "todo-1".to_string(),
            description: "Test task".to_string(),
            status: "pending".to_string(),
            depends_on: vec![],
            notes: String::new(),
        };

        let json = serde_json::to_string(&item).unwrap();
        assert!(json.contains("\"id\":\"todo-1\""));
        assert!(json.contains("\"Test task\""));
    }

    #[test]
    fn test_todo_item_response_with_dependencies() {
        let item = TodoItemResponse {
            id: "todo-2".to_string(),
            description: "Dependent task".to_string(),
            status: "pending".to_string(),
            depends_on: vec!["todo-1".to_string()],
            notes: String::new(),
        };

        let json = serde_json::to_string(&item).unwrap();
        assert!(json.contains("\"depends_on\":[\"todo-1\"]"));
    }

    #[test]
    fn test_todo_item_response_with_notes() {
        let item = TodoItemResponse {
            id: "todo-3".to_string(),
            description: "Task with notes".to_string(),
            status: "in_progress".to_string(),
            depends_on: vec![],
            notes: "This is a note".to_string(),
        };

        let json = serde_json::to_string(&item).unwrap();
        assert!(json.contains("\"This is a note\""));
    }

    #[test]
    fn test_todo_item_response_skips_empty_fields() {
        let item = TodoItemResponse {
            id: "todo-4".to_string(),
            description: "Clean task".to_string(),
            status: "completed".to_string(),
            depends_on: vec![],
            notes: String::new(),
        };

        let json = serde_json::to_string(&item).unwrap();
        // Should not serialize empty depends_on or notes
        assert!(!json.contains("\"depends_on\""));
        assert!(!json.contains("\"notes\""));
    }

    #[test]
    fn test_todo_list_response_serialization() {
        let item = TodoItemResponse {
            id: "item-1".to_string(),
            description: "Task 1".to_string(),
            status: "completed".to_string(),
            depends_on: vec![],
            notes: String::new(),
        };

        let response = TodoListResponse {
            session_id: "session-1".to_string(),
            title: "My Todo List".to_string(),
            items: vec![item],
            progress: TodoProgress {
                completed: 1,
                total: 1,
                percentage: 100,
            },
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"session_id\":\"session-1\""));
        assert!(json.contains("\"My Todo List\""));
        assert!(json.contains("\"percentage\":100"));
    }

    #[test]
    fn test_todo_progress_calculation() {
        let progress = TodoProgress {
            completed: 3,
            total: 5,
            percentage: 60,
        };

        assert_eq!(progress.completed, 3);
        assert_eq!(progress.total, 5);
        assert_eq!(progress.percentage, 60);
    }

    #[test]
    fn test_completion_percentage_zero_total() {
        let percentage = completion_percentage(0, 0);
        assert_eq!(percentage, 0);
    }

    #[test]
    fn test_completion_percentage_half() {
        let percentage = completion_percentage(1, 2);
        assert_eq!(percentage, 50);
    }

    #[test]
    fn test_completion_percentage_full() {
        let percentage = completion_percentage(5, 5);
        assert_eq!(percentage, 100);
    }

    #[test]
    fn test_completion_percentage_rounding() {
        let percentage = completion_percentage(1, 3);
        assert_eq!(percentage, 33);
    }

    #[test]
    fn test_todo_status_label_pending() {
        let label = todo_status_label(&TodoItemStatus::Pending);
        assert_eq!(label, "pending");
    }

    #[test]
    fn test_todo_status_label_in_progress() {
        let label = todo_status_label(&TodoItemStatus::InProgress);
        assert_eq!(label, "in_progress");
    }

    #[test]
    fn test_todo_status_label_completed() {
        let label = todo_status_label(&TodoItemStatus::Completed);
        assert_eq!(label, "completed");
    }

    #[test]
    fn test_todo_status_label_blocked() {
        let label = todo_status_label(&TodoItemStatus::Blocked);
        assert_eq!(label, "blocked");
    }
}
