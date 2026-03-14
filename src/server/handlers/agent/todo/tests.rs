use super::types::{completion_percentage, todo_status_label};
use crate::agent::core::TodoItemStatus;

#[test]
fn completion_percentage_returns_zero_for_empty_list() {
    assert_eq!(completion_percentage(0, 0), 0);
}

#[test]
fn completion_percentage_truncates_fractional_values() {
    assert_eq!(completion_percentage(1, 3), 33);
}

#[test]
fn todo_status_label_maps_all_variants() {
    assert_eq!(todo_status_label(&TodoItemStatus::Pending), "pending");
    assert_eq!(
        todo_status_label(&TodoItemStatus::InProgress),
        "in_progress"
    );
    assert_eq!(todo_status_label(&TodoItemStatus::Completed), "completed");
    assert_eq!(todo_status_label(&TodoItemStatus::Blocked), "blocked");
}
