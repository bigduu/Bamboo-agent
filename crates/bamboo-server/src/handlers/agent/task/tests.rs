use super::types::{completion_percentage, task_status_label};
use bamboo_domain::TaskItemStatus;

#[test]
fn completion_percentage_returns_zero_for_empty_list() {
    assert_eq!(completion_percentage(0, 0), 0);
}

#[test]
fn completion_percentage_truncates_fractional_values() {
    assert_eq!(completion_percentage(1, 3), 33);
}

#[test]
fn task_status_label_maps_all_variants() {
    assert_eq!(task_status_label(&TaskItemStatus::Pending), "pending");
    assert_eq!(
        task_status_label(&TaskItemStatus::InProgress),
        "in_progress"
    );
    assert_eq!(task_status_label(&TaskItemStatus::Completed), "completed");
    assert_eq!(task_status_label(&TaskItemStatus::Blocked), "blocked");
}
