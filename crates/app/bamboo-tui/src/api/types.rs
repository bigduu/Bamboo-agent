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

fn default_title_generated() -> bool {
    true
}

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
    /// Stable Project membership used when authoring a schedule from the
    /// currently selected session.
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub title: String,
    #[serde(default = "default_title_generated")]
    pub title_generated: bool,
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

/// Wire shape of `GET /api/v1/sessions/{session_id}` — the server wraps the
/// single summary in a `{ "session": ... }` envelope rather than a bare
/// object.
#[derive(Deserialize, Debug, Clone)]
pub struct GetSessionEnvelope {
    pub session: SessionSummary,
}

// ── Session resume (history + pending question) ──

/// `function` payload of a [`HistoryToolCall`] — mirrors the server's
/// `FunctionCall` (`bamboo-domain::session::tool_types::FunctionCall`).
#[derive(Deserialize, Debug, Clone, Default)]
pub struct HistoryFunctionCall {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub arguments: String,
}

/// One tool call attached to an assistant [`HistoryMessage`] — mirrors the
/// server's `ToolCall`.
#[derive(Deserialize, Debug, Clone, Default)]
pub struct HistoryToolCall {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub function: HistoryFunctionCall,
}

/// One entry in `GET /api/v1/history/{session_id}`'s `messages` array — a
/// lenient subset of the server's `Message`
/// (`bamboo-domain::session::types::Message`). Only the fields
/// `history::map_history` needs are modeled; everything else (content_parts,
/// image_ocr, phase, compression fields, metadata) is ignored on decode
/// rather than breaking deserialization.
#[derive(Deserialize, Debug, Clone, Default)]
pub struct HistoryMessage {
    #[serde(default)]
    pub id: String,
    /// "system" | "user" | "assistant" | "tool", lowercase.
    #[serde(default)]
    pub role: String,
    #[serde(default)]
    pub content: String,
    #[serde(default)]
    pub reasoning: Option<String>,
    #[serde(default)]
    pub tool_calls: Option<Vec<HistoryToolCall>>,
    #[serde(default)]
    pub tool_call_id: Option<String>,
    #[serde(default)]
    pub tool_success: Option<bool>,
    #[serde(default)]
    pub created_at: Option<DateTime<Utc>>,
}

/// Wire shape of `GET /api/v1/history/{session_id}` (see the route's ACTUAL
/// registration in `routes/agent.rs` — the doc comment on the handler itself
/// names a different, stale path).
#[derive(Deserialize, Debug, Clone, Default)]
pub struct HistoryResponse {
    #[serde(default)]
    pub session_id: String,
    #[serde(default)]
    pub messages: Vec<HistoryMessage>,
    #[serde(default)]
    pub is_delta: bool,
    /// Whether the server dropped older messages to stay under its cold-fetch
    /// cap — surfaced to the operator as "showing last N of M messages".
    #[serde(default)]
    pub truncated: bool,
    #[serde(default)]
    pub total_message_count: usize,
}

/// Wire shape of `GET /api/v1/respond/{session_id}/pending`.
#[derive(Deserialize, Debug, Clone, Default)]
pub struct PendingQuestion {
    #[serde(default)]
    pub has_pending_question: bool,
    #[serde(default)]
    pub question: String,
    #[serde(default)]
    pub options: Option<Vec<String>>,
    #[serde(default)]
    pub allow_custom: bool,
    #[serde(default)]
    pub tool_call_id: Option<String>,
    #[serde(default)]
    pub tool_name: Option<String>,
    #[serde(default)]
    pub source: Option<String>,
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
    pub project_id: Option<String>,
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
    pub project_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_message: Option<String>,
    /// Run the authored prompt when the schedule fires (only meaningful with a
    /// `task_message`).
    pub auto_execute: bool,
}

// ── Provider catalog (model picker, Ctrl+O) ──

/// Mirrors the server's `ProviderModelRef` (`crates/core/bamboo-domain`) on
/// the wire: `{"provider": "...", "model": "..."}`. `model` alone (NOT
/// `provider/model`) is the string form `ChatRequest.model` /
/// `ExecuteRequest.model` / `PatchSessionRequest.model` actually resolve —
/// see `apply_model` in `app.rs`.
#[derive(Deserialize, Debug, Clone, Default)]
pub struct CatalogModelRef {
    #[serde(default)]
    pub provider: String,
    #[serde(default)]
    pub model: String,
}

/// One entry in `GET /v1/bamboo/provider-catalog`'s `models` array — a
/// lenient subset of the server's `ProviderModelDescriptor`
/// (`bamboo-domain::provider_catalog`). `capabilities`/`source`/
/// `discovered_at` aren't rendered by the picker, so they're ignored on
/// decode rather than modeled.
#[derive(Deserialize, Debug, Clone, Default)]
pub struct CatalogModel {
    #[serde(default)]
    pub reference: CatalogModelRef,
    #[serde(default)]
    pub display_name: String,
    #[serde(default)]
    pub provider_display_name: String,
}

/// Wire shape of `GET /v1/bamboo/provider-catalog`. `providers` isn't
/// rendered by the picker (each model entry already carries
/// `provider_display_name`), so only `models` is modeled.
#[derive(Deserialize, Debug, Clone, Default)]
pub struct ProviderCatalog {
    #[serde(default)]
    pub models: Vec<CatalogModel>,
}

/// Body of `PATCH /api/v1/sessions/{id}` for the model-picker's
/// fire-and-forget session update. Must match the server's
/// `PatchSessionRequest.model` field name.
#[derive(Serialize)]
pub struct PatchSessionModelRequest {
    pub model: String,
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

    /// Fixture mirroring the real `GET /api/v1/history/{id}` response,
    /// including server fields the TUI doesn't model (`compression_events`,
    /// `gold_config`, `goal_state`) — those must be ignored, not break
    /// deserialization.
    #[test]
    fn history_response_deserializes_lenient_message_shapes() {
        let json = r#"{
            "session_id": "s1",
            "messages": [
                {"id": "m1", "role": "system", "content": "you are helpful", "created_at": "2026-07-01T00:00:00Z"},
                {"id": "m2", "role": "user", "content": "read foo.txt", "created_at": "2026-07-01T00:00:01Z"},
                {
                    "id": "m3",
                    "role": "assistant",
                    "content": "",
                    "reasoning": "let me check",
                    "tool_calls": [
                        {"id": "t1", "type": "function", "function": {"name": "Read", "arguments": "{\"path\":\"foo.txt\"}"}}
                    ],
                    "created_at": "2026-07-01T00:00:02Z"
                },
                {"id": "m4", "role": "tool", "content": "file body", "tool_call_id": "t1", "tool_success": true, "created_at": "2026-07-01T00:00:03Z"}
            ],
            "is_delta": false,
            "truncated": true,
            "total_message_count": 4,
            "compression_events": [],
            "gold_config": {"anything": "opaque"},
            "goal_state": {"status": "in_progress"}
        }"#;
        let resp: HistoryResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.session_id, "s1");
        assert_eq!(resp.messages.len(), 4);
        assert!(resp.truncated);
        assert_eq!(resp.total_message_count, 4);

        let asst = &resp.messages[2];
        assert_eq!(asst.role, "assistant");
        assert_eq!(asst.reasoning.as_deref(), Some("let me check"));
        let tool_calls = asst.tool_calls.as_ref().unwrap();
        assert_eq!(tool_calls[0].id, "t1");
        assert_eq!(tool_calls[0].function.name, "Read");

        let tool_msg = &resp.messages[3];
        assert_eq!(tool_msg.tool_call_id.as_deref(), Some("t1"));
        assert_eq!(tool_msg.tool_success, Some(true));
    }

    /// A near-empty message (only `role`/`content`) must still deserialize —
    /// the lenient-degrade contract that lets the TUI survive server-side
    /// field additions/removals.
    #[test]
    fn history_message_defaults_missing_fields() {
        let json = r#"{"role": "user", "content": "hi"}"#;
        let m: HistoryMessage = serde_json::from_str(json).unwrap();
        assert_eq!(m.role, "user");
        assert_eq!(m.content, "hi");
        assert!(m.id.is_empty());
        assert!(m.reasoning.is_none());
        assert!(m.tool_calls.is_none());
        assert!(m.tool_call_id.is_none());
        assert!(m.tool_success.is_none());
    }

    /// `has_pending_question: false` responses omit every other field.
    #[test]
    fn pending_question_deserializes_both_shapes() {
        let none: PendingQuestion =
            serde_json::from_str(r#"{"has_pending_question": false}"#).unwrap();
        assert!(!none.has_pending_question);
        assert!(none.question.is_empty());

        let some: PendingQuestion = serde_json::from_str(
            r#"{
                "has_pending_question": true,
                "question": "Run rm -rf?",
                "options": ["Yes", "No"],
                "allow_custom": true,
                "tool_call_id": "t1",
                "tool_name": "Bash",
                "source": "permission"
            }"#,
        )
        .unwrap();
        assert!(some.has_pending_question);
        assert_eq!(some.question, "Run rm -rf?");
        assert_eq!(
            some.options.as_deref(),
            Some(&["Yes".to_string(), "No".to_string()][..])
        );
        assert!(some.allow_custom);
    }

    /// `GET /api/v1/sessions/{id}` wraps the summary in a `{ "session": ... }`
    /// envelope.
    #[test]
    fn get_session_envelope_unwraps() {
        let json = r#"{"session": {"id": "s1", "model": "claude-sonnet-5"}}"#;
        let envelope: GetSessionEnvelope = serde_json::from_str(json).unwrap();
        assert_eq!(envelope.session.id, "s1");
        assert_eq!(envelope.session.model, "claude-sonnet-5");
    }

    /// Fixture mirroring the real `GET /v1/bamboo/provider-catalog` response
    /// (`bamboo-domain::ProviderCatalog`), including fields the picker doesn't
    /// model (`providers`, `capabilities`, `source`, `updated_at`) — those must
    /// be ignored, not break deserialization.
    #[test]
    fn provider_catalog_deserializes_lenient_model_shapes() {
        let json = r#"{
            "providers": [
                {"id": "openai", "display_name": "OpenAI", "enabled": true, "authenticated": true}
            ],
            "models": [
                {
                    "reference": {"provider": "openai", "model": "gpt-4.1"},
                    "display_name": "GPT-4.1",
                    "provider_display_name": "OpenAI",
                    "capabilities": {"supports_tools": true, "supports_vision": true},
                    "source": "upstream",
                    "discovered_at": "2026-07-01T00:00:00Z"
                },
                {
                    "reference": {"provider": "anthropic", "model": "claude-sonnet-5"},
                    "display_name": "Claude Sonnet 5",
                    "provider_display_name": "Anthropic"
                }
            ],
            "updated_at": "2026-07-09T00:00:00Z"
        }"#;
        let catalog: ProviderCatalog = serde_json::from_str(json).unwrap();
        assert_eq!(catalog.models.len(), 2);
        let m0 = &catalog.models[0];
        assert_eq!(m0.reference.provider, "openai");
        assert_eq!(m0.reference.model, "gpt-4.1");
        assert_eq!(m0.display_name, "GPT-4.1");
        assert_eq!(m0.provider_display_name, "OpenAI");
        let m1 = &catalog.models[1];
        assert_eq!(m1.reference.model, "claude-sonnet-5");
    }

    /// An empty catalog (no providers configured) must still deserialize —
    /// the model picker keys "nothing to show" off `models.is_empty()`.
    #[test]
    fn provider_catalog_tolerates_empty_models() {
        let json = r#"{"providers": [], "models": []}"#;
        let catalog: ProviderCatalog = serde_json::from_str(json).unwrap();
        assert!(catalog.models.is_empty());
    }

    #[test]
    fn patch_session_model_request_serializes_model_field() {
        let req = PatchSessionModelRequest {
            model: "gpt-4.1".to_string(),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(json, r#"{"model":"gpt-4.1"}"#);
    }
}
