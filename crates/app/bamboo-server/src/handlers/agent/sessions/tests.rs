use super::SessionSummary;

use bamboo_agent_core::SessionKind;
use bamboo_domain::reasoning::ReasoningEffort;
use bamboo_storage::SessionIndexEntry;
use chrono::Utc;

#[test]
fn session_summary_from_entry_includes_last_run_fields() {
    let entry = SessionIndexEntry {
        id: "child-1".to_string(),
        // Simulate a pre-#770 index row: the compatibility boolean is present
        // but the newly added typed field deserializes to its default.
        bypass_permissions: true,
        permission_mode: bamboo_domain::SessionPermissionMode::Default,
        kind: SessionKind::Child,
        rel_path: "sessions/root/children/child-1".to_string(),
        title: "Child Session".to_string(),
        title_version: 0,
        title_generated: true,
        pinned: false,
        parent_session_id: Some("root".to_string()),
        root_session_id: "root".to_string(),
        spawn_depth: 1,
        model: "gpt-4o".to_string(),
        model_ref: None,
        reasoning_effort: Some(ReasoningEffort::High),
        workspace_path: Some("/workspaces/zenith".to_string()),
        project_id: Some("project-1".to_string()),
        gold_config_json: None,
        created_by_schedule_id: None,
        schedule_run_id: Some("run-123".to_string()),
        created_at: Utc::now(),
        updated_at: Utc::now(),
        last_activity_at: Utc::now(),
        message_count: 5,
        has_attachments: false,
        has_pending_question: true,
        waiting_for_children: false,
        waiting_for_bash: false,
        last_human_input: None,
        session_origin: None,
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
    assert!(summary.title_generated);
    assert_eq!(summary.last_run_error, None);
    assert_eq!(summary.schedule_run_id.as_deref(), Some("run-123"));
    assert_eq!(
        summary.workspace_path.as_deref(),
        Some("/workspaces/zenith")
    );
    assert_eq!(
        summary.project_id.as_ref().map(ToString::to_string),
        Some("project-1".to_string())
    );
    assert_eq!(summary.subagent_type, None);
    assert!(summary.has_pending_question);
    assert_eq!(summary.running_child_count, 0);
    assert!(summary.bypass_permissions);
    assert_eq!(
        summary.permission_mode,
        bamboo_domain::SessionPermissionMode::Bypass
    );
}

#[test]
fn session_summary_from_entry_propagates_subagent_type() {
    let entry = SessionIndexEntry {
        id: "child-2".to_string(),
        bypass_permissions: false,
        permission_mode: bamboo_domain::SessionPermissionMode::Default,
        kind: SessionKind::Child,
        rel_path: "sessions/root/children/child-2".to_string(),
        title: "Plan Child".to_string(),
        title_version: 0,
        title_generated: true,
        pinned: false,
        parent_session_id: Some("root".to_string()),
        root_session_id: "root".to_string(),
        spawn_depth: 1,
        model: "gpt-4o".to_string(),
        model_ref: None,
        reasoning_effort: None,
        workspace_path: None,
        project_id: Some("../malformed".to_string()),
        gold_config_json: None,
        created_by_schedule_id: None,
        schedule_run_id: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        last_activity_at: Utc::now(),
        message_count: 0,
        has_attachments: false,
        has_pending_question: false,
        waiting_for_children: false,
        waiting_for_bash: false,
        last_human_input: None,
        session_origin: None,
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
    assert_eq!(summary.project_id, None);
    assert!(!summary.has_pending_question);
}

/// `is_running` alone reads `false` for a Root that yielded to its children, so
/// the label has to come from the wait mirrors and the running-child count. These
/// cases pin the precedence, which is what keeps a finished session from looking
/// busy and a waiting one from looking hung.
mod activity {
    use super::super::SessionActivity;

    #[test]
    fn a_live_runner_outranks_every_wait_mirror() {
        assert_eq!(
            SessionActivity::derive(true, true, Some("completed"), 3, true, true),
            SessionActivity::Running
        );
    }

    #[test]
    fn a_pending_question_outranks_the_delegation_mirrors() {
        // A Root can yield and also hold a question; the user must break it.
        assert_eq!(
            SessionActivity::derive(false, true, None, 2, true, false),
            SessionActivity::WaitingUser
        );
    }

    #[test]
    fn a_terminal_verdict_outranks_a_surviving_wait() {
        // The exact false-positive this field exists to prevent: a wait row that
        // outlived its run must not keep a finished session looking busy.
        for status in ["completed", "error", "cancelled"] {
            assert_eq!(
                SessionActivity::derive(false, false, Some(status), 1, true, true),
                SessionActivity::Terminal,
                "{status} must settle the row"
            );
        }
    }

    #[test]
    fn a_yielded_root_reports_waiting_even_without_a_counted_child() {
        // The gap after the last child stops and before this Root resumes: no
        // running child to count, but the persisted wait still says what it is.
        assert_eq!(
            SessionActivity::derive(false, false, None, 0, true, false),
            SessionActivity::WaitingChildren
        );
        assert_eq!(
            SessionActivity::derive(false, false, Some("suspended"), 0, true, false),
            SessionActivity::WaitingChildren,
            "suspended is a wait, never a terminal verdict"
        );
    }

    #[test]
    fn a_counted_child_reports_waiting_without_the_mirror() {
        assert_eq!(
            SessionActivity::derive(false, false, None, 1, false, false),
            SessionActivity::WaitingChildren
        );
    }

    #[test]
    fn a_background_shell_wait_is_live_work() {
        assert_eq!(
            SessionActivity::derive(false, false, None, 0, false, true),
            SessionActivity::WaitingBash
        );
        // A shell wait does not outrank a real delegation wait.
        assert_eq!(
            SessionActivity::derive(false, false, None, 0, true, true),
            SessionActivity::WaitingChildren
        );
    }

    #[test]
    fn nothing_in_flight_is_idle() {
        assert_eq!(
            SessionActivity::derive(false, false, None, 0, false, false),
            SessionActivity::Idle
        );
        assert_eq!(
            SessionActivity::derive(false, false, Some("running"), 0, false, false),
            SessionActivity::Idle,
            "a stale non-terminal status is not evidence of work"
        );
    }
}
