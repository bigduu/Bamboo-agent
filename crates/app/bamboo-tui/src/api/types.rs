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

#[derive(Deserialize, Debug, Clone)]
pub struct SessionSummary {
    pub id: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub created_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub updated_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub message_count: Option<u32>,
    #[serde(default)]
    pub status: Option<String>,
}

#[derive(Serialize, Clone)]
pub struct CreateSessionRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_path: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct PendingQuestion {
    pub has_pending_question: bool,
    #[serde(default)]
    pub question: Option<String>,
    #[serde(default)]
    pub options: Option<Vec<String>>,
    #[serde(default)]
    pub allow_custom: Option<bool>,
    #[serde(default)]
    pub tool_call_id: Option<String>,
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

// ── Config ──

#[derive(Deserialize, Debug, Clone)]
pub struct ModelInfo {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
}
