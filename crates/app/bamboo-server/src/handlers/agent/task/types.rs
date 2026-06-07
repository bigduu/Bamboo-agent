use serde::Serialize;

use bamboo_domain::{TaskItem, TaskItemStatus, TaskList};

/// Task item response for frontend
#[derive(Serialize)]
pub struct TaskItemResponse {
    pub id: String,
    pub description: String,
    pub status: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub notes: String,
}

/// Task list response for frontend
#[derive(Serialize)]
pub struct TaskListResponse {
    pub session_id: String,
    pub title: String,
    pub items: Vec<TaskItemResponse>,
    pub progress: TaskProgress,
}

/// Progress information
#[derive(Serialize)]
pub struct TaskProgress {
    pub completed: usize,
    pub total: usize,
    pub percentage: u8,
}

pub(super) fn to_task_list_response(task_list: &TaskList) -> TaskListResponse {
    let items = task_list
        .items
        .iter()
        .map(TaskItemResponse::from_item)
        .collect();
    let completed = task_list
        .items
        .iter()
        .filter(|item| matches!(item.status, TaskItemStatus::Completed))
        .count();
    let total = task_list.items.len();

    TaskListResponse {
        session_id: task_list.session_id.clone(),
        title: task_list.title.clone(),
        items,
        progress: TaskProgress {
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

pub(super) fn task_status_label(status: &TaskItemStatus) -> &'static str {
    match status {
        TaskItemStatus::Pending => "pending",
        TaskItemStatus::InProgress => "in_progress",
        TaskItemStatus::Completed => "completed",
        TaskItemStatus::Blocked => "blocked",
    }
}

impl TaskItemResponse {
    fn from_item(item: &TaskItem) -> Self {
        Self {
            id: item.id.clone(),
            description: item.description.clone(),
            status: task_status_label(&item.status).to_string(),
            depends_on: item.depends_on.clone(),
            notes: item.notes.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::TaskItemStatus;

    #[test]
    fn test_task_item_response_serialization() {
        let item = TaskItemResponse {
            id: "task-1".to_string(),
            description: "Test task".to_string(),
            status: "pending".to_string(),
            depends_on: vec![],
            notes: String::new(),
        };

        let json = serde_json::to_string(&item).unwrap();
        assert!(json.contains("\"id\":\"task-1\""));
        assert!(json.contains("\"Test task\""));
    }

    #[test]
    fn test_task_item_response_with_dependencies() {
        let item = TaskItemResponse {
            id: "task-2".to_string(),
            description: "Dependent task".to_string(),
            status: "pending".to_string(),
            depends_on: vec!["task-1".to_string()],
            notes: String::new(),
        };

        let json = serde_json::to_string(&item).unwrap();
        assert!(json.contains("\"depends_on\":[\"task-1\"]"));
    }

    #[test]
    fn test_task_item_response_with_notes() {
        let item = TaskItemResponse {
            id: "task-3".to_string(),
            description: "Task with notes".to_string(),
            status: "in_progress".to_string(),
            depends_on: vec![],
            notes: "This is a note".to_string(),
        };

        let json = serde_json::to_string(&item).unwrap();
        assert!(json.contains("\"This is a note\""));
    }

    #[test]
    fn test_task_item_response_skips_empty_fields() {
        let item = TaskItemResponse {
            id: "task-4".to_string(),
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
    fn test_task_list_response_serialization() {
        let item = TaskItemResponse {
            id: "item-1".to_string(),
            description: "Task 1".to_string(),
            status: "completed".to_string(),
            depends_on: vec![],
            notes: String::new(),
        };

        let response = TaskListResponse {
            session_id: "session-1".to_string(),
            title: "My Task List".to_string(),
            items: vec![item],
            progress: TaskProgress {
                completed: 1,
                total: 1,
                percentage: 100,
            },
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"session_id\":\"session-1\""));
        assert!(json.contains("\"My Task List\""));
        assert!(json.contains("\"percentage\":100"));
    }

    #[test]
    fn test_task_progress_calculation() {
        let progress = TaskProgress {
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
    fn test_task_status_label_pending() {
        let label = task_status_label(&TaskItemStatus::Pending);
        assert_eq!(label, "pending");
    }

    #[test]
    fn test_task_status_label_in_progress() {
        let label = task_status_label(&TaskItemStatus::InProgress);
        assert_eq!(label, "in_progress");
    }

    #[test]
    fn test_task_status_label_completed() {
        let label = task_status_label(&TaskItemStatus::Completed);
        assert_eq!(label, "completed");
    }

    #[test]
    fn test_task_status_label_blocked() {
        let label = task_status_label(&TaskItemStatus::Blocked);
        assert_eq!(label, "blocked");
    }
}
