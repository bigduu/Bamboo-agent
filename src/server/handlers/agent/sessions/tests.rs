use super::SessionSummary;

use crate::agent::core::storage::SessionIndexEntry;
use crate::agent::core::SessionKind;
use crate::core::ReasoningEffort;
use chrono::Utc;

#[test]
fn session_summary_from_entry_includes_last_run_fields() {
    let entry = SessionIndexEntry {
        id: "child-1".to_string(),
        kind: SessionKind::Child,
        rel_path: "sessions/root/children/child-1".to_string(),
        title: "Child Session".to_string(),
        pinned: false,
        parent_session_id: Some("root".to_string()),
        root_session_id: "root".to_string(),
        spawn_depth: 1,
        model: "gpt-4o".to_string(),
        reasoning_effort: Some(ReasoningEffort::High),
        created_by_schedule_id: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        last_activity_at: Utc::now(),
        message_count: 5,
        has_attachments: false,
        last_run_status: Some("completed".to_string()),
        last_run_error: None,
        token_usage: None,
    };

    let summary = SessionSummary::from_entry(entry, false);

    assert_eq!(summary.last_run_status.as_deref(), Some("completed"));
    assert_eq!(summary.last_run_error, None);
}
