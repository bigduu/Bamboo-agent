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

#[derive(Deserialize, Debug, Clone)]
pub struct Schedule {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub cron: Option<String>,
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub prompt: Option<String>,
    #[serde(default)]
    pub created_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_run: Option<DateTime<Utc>>,
    #[serde(default)]
    pub next_run: Option<DateTime<Utc>>,
}

#[derive(Serialize)]
pub struct CreateScheduleRequest {
    pub name: String,
    pub cron: String,
    pub prompt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_path: Option<String>,
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
