//! Authoritative account-level sub-agent recovery snapshot.
//!
//! The account change feed is an incremental log. A persisted browser cursor
//! can legitimately be newer than the event that first created an unresolved
//! approval, so replay alone cannot rebuild the current UI after a full reload.
//! This endpoint supplies the replacement snapshot and a feed watermark; the
//! client buffers the live feed while fetching it, replaces local state, then
//! replays only events newer than `snapshot_seq`.

use std::collections::HashMap;

use actix_web::{web, HttpResponse, Result};
use bamboo_engine::external_agents::approval_registry::{ApprovalState, DurableApproval};
use serde::Serialize;

use crate::app_state::{AgentStatus, AppState};

const SNAPSHOT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Serialize)]
pub struct SubagentSnapshotResponse {
    pub schema_version: u32,
    /// Account-feed coordinate captured before reading the replacement state.
    /// State transitions are persisted before their durable event is published,
    /// so replaying events strictly newer than this watermark cannot miss a
    /// transition. A concurrent later transition may appear in both the
    /// snapshot and replay; approval versions and replacement semantics make
    /// that overlap idempotent.
    pub snapshot_seq: u64,
    pub approvals_revision: u64,
    pub generated_at: String,
    pub approvals: Vec<DurableApproval>,
    pub children: Vec<SubagentLifecycleSnapshot>,
}

#[derive(Debug, Serialize)]
pub struct SubagentLifecycleSnapshot {
    pub parent_session_id: String,
    pub child_session_id: String,
    pub root_session_id: String,
    pub child_attempt: u32,
    pub title: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub last_seen_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subagent_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resident_name: Option<String>,
    pub approval_request_ids: Vec<String>,
}

/// `GET /api/v1/subagents/snapshot`
pub async fn handler(state: web::Data<AppState>) -> Result<HttpResponse> {
    // Capture the watermark first. Every producer updates its durable/session
    // authority before publishing the corresponding account event. Reading
    // replacement state afterwards therefore cannot omit an event at or below
    // this coordinate; concurrent newer events are replayed by the client.
    let snapshot_seq = state.account_sink.latest_seq();
    let approval_snapshot = state
        .approval_registry
        .lock()
        .map_err(|_| crate::error::json_internal_server_error("approval registry lock poisoned"))?
        .unresolved_snapshot();

    let unresolved_by_child = unresolved_by_child(&approval_snapshot.approvals);
    let live_runners = state
        .agent_runners
        .read()
        .await
        .iter()
        .filter(|(_, runner)| matches!(runner.status, AgentStatus::Running))
        .map(|(session_id, runner)| {
            (
                session_id.clone(),
                runner.last_activity_at().unwrap_or(runner.started_at),
            )
        })
        .collect::<HashMap<_, _>>();
    let mut entries = state
        .session_store
        .list_index_entries()
        .await
        .into_iter()
        .filter(|entry| entry.parent_session_id.is_some())
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| {
        (
            &left.root_session_id,
            &left.parent_session_id,
            left.created_at,
            &left.id,
        )
            .cmp(&(
                &right.root_session_id,
                &right.parent_session_id,
                right.created_at,
                &right.id,
            ))
    });

    let mut children = Vec::with_capacity(entries.len());
    for entry in entries {
        let parent_session_id = entry
            .parent_session_id
            .clone()
            .expect("child entries were filtered above");
        let approvals = unresolved_by_child
            .get(&(parent_session_id.clone(), entry.id.clone()))
            .cloned()
            .unwrap_or_default();
        let has_unresolved_approval = !approvals.is_empty();
        let waiting_for_children =
            if !has_unresolved_approval && entry.last_run_status.as_deref() == Some("suspended") {
                state
                    .storage
                    .load_runtime_control_plane(&entry.id)
                    .await
                    .map_err(|error| crate::error::json_internal_server_error(error.to_string()))?
                    .is_some_and(|session| {
                        session
                            .agent_runtime_state
                            .as_ref()
                            .is_some_and(|runtime| runtime.waiting_for_children.is_some())
                    })
            } else {
                false
            };
        let child_attempt = approvals
            .iter()
            .map(|approval| approval.child_attempt)
            .max()
            .unwrap_or(0);
        let approval_request_ids = approvals
            .iter()
            .map(|approval| approval.request_id.clone())
            .collect();
        // Heartbeats/events only advance live runner liveness; they never
        // rewrite the child's business status. Surface that fresher coordinate
        // while retaining the durable index timestamp for restart recovery.
        let last_seen_at = live_runners
            .get(&entry.id)
            .copied()
            .map_or(entry.last_activity_at, |live| {
                entry.last_activity_at.max(live)
            });

        children.push(SubagentLifecycleSnapshot {
            parent_session_id,
            child_session_id: entry.id.clone(),
            root_session_id: entry.root_session_id,
            child_attempt,
            title: entry.title,
            status: normalize_status(
                entry.last_run_status.as_deref(),
                live_runners.contains_key(&entry.id),
                has_unresolved_approval,
                waiting_for_children,
            ),
            error: entry.last_run_error,
            created_at: entry.created_at.to_rfc3339(),
            updated_at: entry.updated_at.to_rfc3339(),
            last_seen_at: last_seen_at.to_rfc3339(),
            subagent_type: entry.subagent_type,
            lifecycle: entry.lifecycle,
            resident_name: entry.resident_name,
            approval_request_ids,
        });
    }

    Ok(HttpResponse::Ok().json(SubagentSnapshotResponse {
        schema_version: SNAPSHOT_SCHEMA_VERSION,
        snapshot_seq,
        approvals_revision: approval_snapshot.revision,
        generated_at: chrono::Utc::now().to_rfc3339(),
        approvals: approval_snapshot.approvals,
        children,
    }))
}

fn unresolved_by_child(
    approvals: &[DurableApproval],
) -> HashMap<(String, String), Vec<&DurableApproval>> {
    let mut by_child: HashMap<(String, String), Vec<&DurableApproval>> = HashMap::new();
    for approval in approvals {
        if matches!(
            approval.state,
            ApprovalState::Pending | ApprovalState::DecisionRecorded
        ) {
            by_child
                .entry((
                    approval.parent_session_id.clone(),
                    approval.child_session_id.clone(),
                ))
                .or_default()
                .push(approval);
        }
    }
    by_child
}

fn normalize_status(
    persisted: Option<&str>,
    runner_is_live: bool,
    has_unresolved_approval: bool,
    waiting_for_children: bool,
) -> String {
    if has_unresolved_approval {
        return "awaiting_permission".to_string();
    }
    if waiting_for_children {
        return "waiting_for_children".to_string();
    }
    if runner_is_live {
        return "running".to_string();
    }
    match persisted
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "" => "created",
        "created" | "pending" | "queued" => "queued",
        // A persisted running marker without a runner in this process is an
        // orphan left by a crash/restart. The child-wait watchdog will also
        // synthesize the canonical terminal completion, but the recovery API
        // must not advertise the stale marker as live in the meantime.
        "started" | "already_running" | "running" => "lost",
        "success" | "done" | "completed" | "skipped" => "completed",
        "error" | "failed" => "failed",
        "canceled" | "cancelled" => "cancelled",
        "timeout" | "timed_out" => "timed_out",
        "lost" => "lost",
        "suspended" => "suspended",
        other => other,
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use actix_web::{test, web, App};
    use bamboo_agent_core::{AgentEvent, Session};
    use bamboo_engine::external_agents::approval_registry::{ApprovalState, DurableApproval};

    use super::*;

    fn pending_approval() -> DurableApproval {
        DurableApproval {
            parent_session_id: "parent-1".into(),
            child_session_id: "child-1".into(),
            child_attempt: 2,
            request_id: "request-1".into(),
            tool_name: "Bash".into(),
            permission: "execute".into(),
            resource: "cargo test".into(),
            created_at: "2026-07-22T00:00:00Z".into(),
            updated_at: "2026-07-22T00:00:00Z".into(),
            version: 1,
            state: ApprovalState::Pending,
            approved: None,
            reason: None,
        }
    }

    #[actix_web::test]
    async fn lifecycle_status_prefers_semantic_blockers_and_normalizes_terminals() {
        assert_eq!(
            normalize_status(Some("running"), true, true, false),
            "awaiting_permission"
        );
        assert_eq!(
            normalize_status(Some("suspended"), false, false, true),
            "waiting_for_children"
        );
        assert_eq!(
            normalize_status(Some("error"), false, false, false),
            "failed"
        );
        assert_eq!(
            normalize_status(Some("timeout"), false, false, false),
            "timed_out"
        );
        assert_eq!(
            normalize_status(Some("running"), false, false, false),
            "lost"
        );
        assert_eq!(
            normalize_status(Some("running"), true, false, false),
            "running"
        );
    }

    #[actix_web::test]
    async fn snapshot_returns_feed_watermark_pending_approval_and_child_tree() {
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());

        let parent = Session::new("parent-1", "gpt-5");
        state.storage.save_session(&parent).await.unwrap();
        let mut child = Session::new_child("child-1", "parent-1", "gpt-5", "Reviewer");
        child.set_last_run_status("running");
        state.storage.save_session(&child).await.unwrap();
        state
            .approval_registry
            .lock()
            .unwrap()
            .register(pending_approval())
            .unwrap();
        state.account_sink.record(
            Some("parent-1"),
            &AgentEvent::SessionDeleted {
                session_id: "old-child".into(),
            },
        );
        for _ in 0..100 {
            if state.account_sink.latest_seq() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let app = test::init_service(
            App::new()
                .app_data(state)
                .route("/api/v1/subagents/snapshot", web::get().to(handler)),
        )
        .await;
        let request = test::TestRequest::get()
            .uri("/api/v1/subagents/snapshot")
            .to_request();
        let response: serde_json::Value = test::call_and_read_body_json(&app, request).await;

        assert_eq!(response["schema_version"], 1);
        assert_eq!(response["snapshot_seq"], 1);
        assert_eq!(response["approvals_revision"], 1);
        assert_eq!(response["approvals"][0]["request_id"], "request-1");
        assert_eq!(response["children"][0]["parent_session_id"], "parent-1");
        assert_eq!(response["children"][0]["child_session_id"], "child-1");
        assert_eq!(response["children"][0]["child_attempt"], 2);
        assert_eq!(response["children"][0]["status"], "awaiting_permission");
        assert_eq!(
            response["children"][0]["approval_request_ids"][0],
            "request-1"
        );
    }
}
