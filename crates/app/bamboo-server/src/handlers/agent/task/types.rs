use chrono::{DateTime, Utc};
use serde::Serialize;

use bamboo_domain::{TaskItem, TaskItemStatus, TaskList};

/// Task list response for frontend
#[derive(Serialize)]
pub struct TaskListResponse {
    pub session_id: String,
    pub title: Option<String>,
    pub items: Vec<TaskItem>,
    pub progress: TaskProgress,
    pub version: u64,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

/// Progress information
#[derive(Serialize)]
pub struct TaskProgress {
    pub completed: usize,
    pub total: usize,
    pub percentage: u8,
}

pub(super) fn to_task_list_response(task_list: &TaskList, version: u64) -> TaskListResponse {
    let completed = task_list
        .items
        .iter()
        .filter(|item| matches!(item.status, TaskItemStatus::Completed))
        .count();
    let total = task_list.items.len();

    TaskListResponse {
        session_id: task_list.session_id.clone(),
        title: Some(task_list.title.clone()),
        items: task_list.items.clone(),
        progress: TaskProgress {
            completed,
            total,
            percentage: completion_percentage(completed, total),
        },
        version,
        created_at: Some(task_list.created_at),
        updated_at: Some(task_list.updated_at),
    }
}

pub(super) fn completion_percentage(completed: usize, total: usize) -> u8 {
    if total == 0 {
        return 0;
    }
    ((completed as f32 / total as f32) * 100.0) as u8
}

#[cfg(test)]
pub(super) fn task_status_label(status: &TaskItemStatus) -> &'static str {
    match status {
        TaskItemStatus::Pending => "pending",
        TaskItemStatus::InProgress => "in_progress",
        TaskItemStatus::Completed => "completed",
        TaskItemStatus::Blocked => "blocked",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::{TaskBlocker, TaskBlockerKind, TaskItemStatus};

    #[test]
    fn test_task_item_response_serialization() {
        let item = TaskItem {
            id: "task-1".to_string(),
            description: "Test task".to_string(),
            status: TaskItemStatus::Pending,
            ..TaskItem::default()
        };

        let json = serde_json::to_string(&item).unwrap();
        assert!(json.contains("\"id\":\"task-1\""));
        assert!(json.contains("\"Test task\""));
    }

    #[test]
    fn test_task_item_response_with_dependencies() {
        let item = TaskItem {
            id: "task-2".to_string(),
            description: "Dependent task".to_string(),
            status: TaskItemStatus::Pending,
            depends_on: vec!["task-1".to_string()],
            ..TaskItem::default()
        };

        let json = serde_json::to_string(&item).unwrap();
        assert!(json.contains("\"depends_on\":[\"task-1\"]"));
    }

    #[test]
    fn test_task_item_response_with_notes() {
        let item = TaskItem {
            id: "task-3".to_string(),
            description: "Task with notes".to_string(),
            status: TaskItemStatus::InProgress,
            notes: "This is a note".to_string(),
            blockers: vec![TaskBlocker {
                kind: TaskBlockerKind::External,
                summary: "Waiting for CI".to_string(),
                waiting_on: Some("build #42".to_string()),
            }],
            ..TaskItem::default()
        };

        let json = serde_json::to_string(&item).unwrap();
        assert!(json.contains("\"This is a note\""));
        assert!(json.contains("\"waiting_on\":\"build #42\""));
    }

    #[test]
    fn test_task_item_response_skips_empty_fields() {
        let item = TaskItem {
            id: "task-4".to_string(),
            description: "Clean task".to_string(),
            status: TaskItemStatus::Completed,
            ..TaskItem::default()
        };

        let json = serde_json::to_string(&item).unwrap();
        assert!(json.contains("\"status\":\"completed\""));
    }

    #[test]
    fn test_task_list_response_serialization() {
        let item = TaskItem {
            id: "item-1".to_string(),
            description: "Task 1".to_string(),
            status: TaskItemStatus::Completed,
            ..TaskItem::default()
        };

        let response = TaskListResponse {
            session_id: "session-1".to_string(),
            title: Some("My Task List".to_string()),
            items: vec![item],
            progress: TaskProgress {
                completed: 1,
                total: 1,
                percentage: 100,
            },
            version: 3,
            created_at: None,
            updated_at: None,
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"session_id\":\"session-1\""));
        assert!(json.contains("\"My Task List\""));
        assert!(json.contains("\"percentage\":100"));
        assert!(json.contains("\"version\":3"));
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
}
