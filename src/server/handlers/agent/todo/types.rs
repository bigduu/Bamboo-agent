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
