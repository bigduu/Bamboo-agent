//! HTTP wire types for the TUI client.
//!
//! These structs mirror the Bamboo server's REST responses. Some fields are
//! deserialized for contract fidelity (and so the TUI is robust to responses
//! that include them) but are not yet surfaced in the UI — hence the
//! module-scoped `dead_code` allow. New *logic* dead code elsewhere in the
//! crate still warns, since the blanket crate-level allow was removed.
#![allow(dead_code)]

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// Shared HTTP/SSE wire types live in bamboo-client-core (single source of truth
// across the CLI and TUI front-ends); re-exported here so the rest of the TUI
// can keep referring to `crate::api::types::{AgentEvent, ChatRequest, …}`.
pub use bamboo_client_core::{
    AgentEvent, ChatRequest, ChatResponse, ExecuteRequest, ExecuteResponse, TokenUsage,
};

// ── Sessions ──

/// Subset of the server's `SessionSummary` (see
/// `bamboo-server/src/handlers/agent/sessions/types.rs`) the TUI renders.
/// Every field carries `#[serde(default)]` so server-side additions/removals
/// degrade gracefully instead of breaking deserialization (there is
/// deliberately no flat `status` string on the server — see `last_run_status`
/// / `is_running` / `has_pending_question` below).
#[derive(Deserialize, Debug, Clone)]
pub struct SessionSummary {
    pub id: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub model: String,
    #[serde(default)]
    pub is_running: bool,
    #[serde(default)]
    pub has_pending_question: bool,
    #[serde(default)]
    pub last_run_status: Option<String>,
    #[serde(default)]
    pub updated_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub message_count: usize,
    #[serde(default)]
    pub pinned: bool,
}

/// Wire shape of `GET /api/v1/sessions` — the server wraps the page in an
/// envelope (`ListSessionsResponse`, #421/#252) so the list can be paginated
/// instead of growing without bound as session count grows forever.
#[derive(Deserialize, Debug, Clone, Default)]
pub struct ListSessionsEnvelope {
    #[serde(default)]
    pub sessions: Vec<SessionSummary>,
    #[serde(default)]
    pub total: usize,
    #[serde(default)]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
    #[serde(default)]
    pub next_offset: Option<usize>,
}

#[derive(Serialize)]
pub struct RespondRequest {
    pub response: String,
}

// ── MCP ──

#[derive(Deserialize, Debug, Clone)]
pub struct McpServer {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    pub enabled: bool,
    #[serde(default)]
    pub transport: serde_json::Value,
    #[serde(default)]
    pub connected: Option<bool>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ToolInfo {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
}

// ── Schedules ──

/// Flat, display-oriented schedule used by the Schedules tab. Built from the
/// server's richer `ScheduleView` via [`Schedule::from_view`] — the server has
/// no flat `cron`/`prompt` fields, so we project its `trigger`/`state` into the
/// handful of fields the list actually renders.
#[derive(Debug, Clone)]
pub struct Schedule {
    pub id: String,
    pub name: Option<String>,
    pub cron: Option<String>,
    pub enabled: Option<bool>,
    pub prompt: Option<String>,
    pub last_run: Option<DateTime<Utc>>,
    pub next_run: Option<DateTime<Utc>>,
}

impl Schedule {
    pub fn from_view(v: ScheduleView) -> Self {
        Self {
            cron: Some(trigger_display(&v.trigger)),
            name: Some(v.name),
            enabled: Some(v.enabled),
            prompt: v.run_config.task_message,
            last_run: v.state.last_finished_at,
            next_run: v.state.next_fire_at,
            id: v.id,
        }
    }
}

/// Human-readable one-liner for a schedule trigger. Cron triggers show their
/// expression; other trigger kinds show their `type` tag (e.g. `interval`).
fn trigger_display(trigger: &serde_json::Value) -> String {
    match trigger.get("type").and_then(|t| t.as_str()) {
        Some("cron") => trigger
            .get("expr")
            .and_then(|e| e.as_str())
            .unwrap_or("cron")
            .to_string(),
        Some(kind) => kind.to_string(),
        None => "-".to_string(),
    }
}

/// Wire shape of `GET /api/v1/schedules` — the server wraps the list in a
/// `{ "schedules": [...] }` envelope (`ListSchedulesResponse`).
#[derive(Deserialize, Debug, Clone)]
pub struct ListSchedulesResponse {
    #[serde(default)]
    pub schedules: Vec<ScheduleView>,
}

/// Subset of the server's `ScheduleView` the TUI consumes. The full trigger
/// enum is kept as an opaque `Value` (see [`trigger_display`]) so new trigger
/// kinds don't break deserialization.
#[derive(Deserialize, Debug, Clone)]
pub struct ScheduleView {
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub trigger: serde_json::Value,
    #[serde(default)]
    pub state: ScheduleStateView,
    #[serde(default)]
    pub run_config: ScheduleRunConfigView,
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct ScheduleStateView {
    #[serde(default)]
    pub next_fire_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_finished_at: Option<DateTime<Utc>>,
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct ScheduleRunConfigView {
    #[serde(default)]
    pub task_message: Option<String>,
}

/// Body of `POST /api/v1/schedules`. Must match the server's
/// `CreateScheduleRequest`: a tagged `trigger` and a `run_config` carrying the
/// prompt (there is no flat `cron`/`prompt` on the server side).
#[derive(Serialize)]
pub struct CreateScheduleRequest {
    pub name: String,
    pub trigger: ScheduleTriggerReq,
    pub enabled: bool,
    pub run_config: ScheduleRunConfigReq,
}

/// Serialize-side mirror of the server's internally-tagged `ScheduleTrigger`.
/// The TUI form only authors cron triggers today.
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ScheduleTriggerReq {
    Cron { expr: String },
}

#[derive(Serialize, Default)]
pub struct ScheduleRunConfigReq {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_message: Option<String>,
    /// Run the authored prompt when the schedule fires (only meaningful with a
    /// `task_message`).
    pub auto_execute: bool,
}

// ── Skills ──

#[derive(Deserialize, Debug, Clone)]
pub struct Skill {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub enabled: Option<bool>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SkillDetail {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub prompt: Option<String>,
    #[serde(default)]
    pub tools: Option<Vec<String>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fixture mirroring the real `ListSessionsResponse` shape (#421), including
    /// fields the TUI doesn't model (`kind`, `placement`, …) — those must be
    /// ignored rather than break deserialization.
    const ENVELOPE_JSON: &str = r#"{
        "sessions": [
            {
                "id": "s1",
                "kind": "root",
                "title": "Fix the bug",
                "title_version": 3,
                "pinned": true,
                "root_session_id": "s1",
                "spawn_depth": 0,
                "model": "claude-sonnet-5",
                "created_at": "2026-07-01T00:00:00Z",
                "updated_at": "2026-07-09T12:34:00Z",
                "last_activity_at": "2026-07-09T12:34:00Z",
                "message_count": 12,
                "has_attachments": false,
                "is_running": true,
                "has_pending_question": false,
                "running_child_count": 0,
                "placement": {"kind": "local", "host": "box"}
            },
            {
                "id": "s2",
                "kind": "root",
                "title": "",
                "title_version": 0,
                "pinned": false,
                "root_session_id": "s2",
                "spawn_depth": 0,
                "model": "gpt-5",
                "created_at": "2026-07-01T00:00:00Z",
                "updated_at": "2026-07-08T08:00:00Z",
                "last_activity_at": "2026-07-08T08:00:00Z",
                "message_count": 0,
                "has_attachments": false,
                "is_running": false,
                "has_pending_question": true,
                "last_run_status": "error",
                "running_child_count": 0,
                "placement": {"kind": "local", "host": "box"}
            }
        ],
        "total": 5,
        "limit": 2,
        "offset": 0,
        "next_offset": 2
    }"#;

    #[test]
    fn envelope_deserializes_with_pagination_metadata() {
        let envelope: ListSessionsEnvelope = serde_json::from_str(ENVELOPE_JSON).unwrap();
        assert_eq!(envelope.total, 5);
        assert_eq!(envelope.limit, 2);
        assert_eq!(envelope.offset, 0);
        assert_eq!(envelope.next_offset, Some(2));
        assert_eq!(envelope.sessions.len(), 2);

        let s1 = &envelope.sessions[0];
        assert_eq!(s1.id, "s1");
        assert_eq!(s1.title, "Fix the bug");
        assert_eq!(s1.model, "claude-sonnet-5");
        assert!(s1.is_running);
        assert!(!s1.has_pending_question);
        assert_eq!(s1.message_count, 12);
        assert!(s1.pinned);
        assert!(s1.last_run_status.is_none());

        let s2 = &envelope.sessions[1];
        assert!(s2.title.is_empty());
        assert!(!s2.is_running);
        assert!(s2.has_pending_question);
        assert_eq!(s2.last_run_status.as_deref(), Some("error"));
    }

    /// A last page (`next_offset` omitted entirely) must deserialize to `None`,
    /// and unknown top-level fields must not break parsing.
    #[test]
    fn envelope_tolerates_missing_next_offset_and_unknown_fields() {
        let json = r#"{
            "sessions": [],
            "total": 0,
            "limit": 200,
            "offset": 0,
            "some_future_field": "ignored"
        }"#;
        let envelope: ListSessionsEnvelope = serde_json::from_str(json).unwrap();
        assert!(envelope.sessions.is_empty());
        assert_eq!(envelope.next_offset, None);
    }

    /// A minimal (nearly-empty) session object must still deserialize thanks to
    /// `#[serde(default)]` on every non-id field — the lenient-degrade contract.
    #[test]
    fn session_summary_defaults_missing_fields() {
        let json = r#"{"id": "bare"}"#;
        let s: SessionSummary = serde_json::from_str(json).unwrap();
        assert_eq!(s.id, "bare");
        assert_eq!(s.title, "");
        assert_eq!(s.model, "");
        assert!(!s.is_running);
        assert!(!s.has_pending_question);
        assert!(s.last_run_status.is_none());
        assert!(s.updated_at.is_none());
        assert_eq!(s.message_count, 0);
        assert!(!s.pinned);
    }
}
