use super::SessionSummary;

use bamboo_agent_core::SessionKind;
use bamboo_domain::reasoning::ReasoningEffort;
use bamboo_storage::SessionIndexEntry;
use chrono::Utc;

#[test]
fn session_summary_from_entry_includes_last_run_fields() {
    let entry = SessionIndexEntry {
        id: "child-1".to_string(),
        bypass_permissions: false,
        kind: SessionKind::Child,
        rel_path: "sessions/root/children/child-1".to_string(),
        title: "Child Session".to_string(),
        title_version: 0,
        pinned: false,
        parent_session_id: Some("root".to_string()),
        root_session_id: "root".to_string(),
        spawn_depth: 1,
        model: "gpt-4o".to_string(),
        model_ref: None,
        reasoning_effort: Some(ReasoningEffort::High),
        workspace_path: Some("/workspaces/zenith".to_string()),
        gold_config_json: None,
        created_by_schedule_id: None,
        schedule_run_id: Some("run-123".to_string()),
        created_at: Utc::now(),
        updated_at: Utc::now(),
        last_activity_at: Utc::now(),
        message_count: 5,
        has_attachments: false,
        has_pending_question: true,
        plan_mode: None,
        last_run_status: Some("completed".to_string()),
        last_run_error: None,
        token_usage: None,
        subagent_type: None,
        lifecycle: None,
        resident_name: None,
        placement: None,
    };

    let summary = SessionSummary::from_entry(entry, false);

    assert_eq!(summary.last_run_status.as_deref(), Some("completed"));
    assert_eq!(summary.last_run_error, None);
    assert_eq!(summary.schedule_run_id.as_deref(), Some("run-123"));
    assert_eq!(
        summary.workspace_path.as_deref(),
        Some("/workspaces/zenith")
    );
    assert_eq!(summary.subagent_type, None);
    assert!(summary.has_pending_question);
    assert_eq!(summary.running_child_count, 0);
}

#[test]
fn session_summary_from_entry_propagates_subagent_type() {
    let entry = SessionIndexEntry {
        id: "child-2".to_string(),
        bypass_permissions: false,
        kind: SessionKind::Child,
        rel_path: "sessions/root/children/child-2".to_string(),
        title: "Plan Child".to_string(),
        title_version: 0,
        pinned: false,
        parent_session_id: Some("root".to_string()),
        root_session_id: "root".to_string(),
        spawn_depth: 1,
        model: "gpt-4o".to_string(),
        model_ref: None,
        reasoning_effort: None,
        workspace_path: None,
        gold_config_json: None,
        created_by_schedule_id: None,
        schedule_run_id: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        last_activity_at: Utc::now(),
        message_count: 0,
        has_attachments: false,
        has_pending_question: false,
        plan_mode: None,
        last_run_status: None,
        last_run_error: None,
        token_usage: None,
        subagent_type: Some("plan".to_string()),
        lifecycle: None,
        resident_name: None,
        placement: None,
    };

    let summary = SessionSummary::from_entry(entry, false);
    assert_eq!(summary.subagent_type.as_deref(), Some("plan"));
    assert!(!summary.has_pending_question);
}
