use std::collections::HashSet;

use actix_web::{web, HttpResponse, Result};

use crate::app_state::{AgentStatus, AppState};
use crate::handlers::agent::sessions::types::{RunningSessionEntry, RunningSessionsResponse};

/// `GET /api/v1/runs/active`
///
/// Returns a snapshot of all currently-running sessions so the frontend
/// can replay their state on boot or after a transport reconnect.
pub async fn running_sessions_snapshot(state: web::Data<AppState>) -> Result<HttpResponse> {
    let runners = state.agent_runners.read().await;

    // Collect running runner IDs first so we can compute child relationships.
    let running_ids: HashSet<String> = runners
        .iter()
        .filter_map(|(session_id, runner)| {
            if matches!(runner.status, AgentStatus::Running) {
                Some(session_id.clone())
            } else {
                None
            }
        })
        .collect();

    // Also fetch index entries to determine parent/child relationships.
    let entries = state.session_store.list_index_entries().await;
    let parent_to_children: std::collections::HashMap<String, Vec<String>> = entries
        .iter()
        .filter(|e| {
            e.parent_session_id
                .as_ref()
                .map(|p| running_ids.contains(&e.id) && running_ids.contains(p))
                .unwrap_or(false)
        })
        .fold(std::collections::HashMap::new(), |mut map, entry| {
            if let Some(parent_id) = &entry.parent_session_id {
                map.entry(parent_id.clone())
                    .or_default()
                    .push(entry.id.clone());
            }
            map
        });

    let sessions: Vec<RunningSessionEntry> = runners
        .iter()
        .filter_map(|(session_id, runner)| {
            if !matches!(runner.status, AgentStatus::Running) {
                return None;
            }

            let running_child_session_ids = parent_to_children
                .get(session_id)
                .cloned()
                .unwrap_or_default();

            Some(RunningSessionEntry {
                session_id: session_id.clone(),
                run_id: runner.run_id.clone(),
                started_at: runner.started_at.to_rfc3339(),
                round_count: runner.round_count,
                last_tool_name: runner.last_tool_name.clone(),
                last_tool_phase: runner.last_tool_phase.clone(),
                last_event_at: runner.last_activity_at().map(|t| t.to_rfc3339()),
                last_critical_events: runner.last_critical_events.clone(),
                running_child_session_ids,
            })
        })
        .collect();

    Ok(HttpResponse::Ok().json(RunningSessionsResponse { sessions }))
}
