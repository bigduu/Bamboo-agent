#![allow(dead_code)]

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ── Chat ──

#[derive(Serialize, Clone)]
pub struct ChatRequest {
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub model: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ChatResponse {
    pub session_id: String,
    pub stream_url: String,
    pub status: String,
}

#[derive(Serialize, Clone)]
pub struct ExecuteRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ExecuteResponse {
    pub session_id: String,
    pub status: String,
    pub events_url: String,
}

// ── SSE Events ──

#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentEvent {
    Token {
        content: String,
    },
    ReasoningToken {
        content: String,
    },
    ToolToken {
        tool_call_id: String,
        content: String,
    },
    ToolStart {
        tool_call_id: String,
        tool_name: String,
        arguments: serde_json::Value,
    },
    ToolComplete {
        tool_call_id: String,
        result: ToolResult,
    },
    ToolError {
        tool_call_id: String,
        error: String,
    },
    NeedClarification {
        question: String,
        options: Option<Vec<String>>,
    },
    ToolLifecycle {
        tool_call_id: String,
        tool_name: String,
        phase: String,
        elapsed_ms: Option<u64>,
        is_mutating: bool,
        auto_approved: bool,
        summary: Option<String>,
        error: Option<String>,
    },
    ContextCompressionStatus {
        phase: String,
        status: String,
    },
    PlanModeEntered {
        session_id: String,
        #[serde(default)]
        reason: Option<String>,
        #[serde(default)]
        pre_permission_mode: Option<String>,
        #[serde(default)]
        entered_at: Option<String>,
        #[serde(default)]
        status: Option<String>,
        #[serde(default)]
        plan_file_path: Option<String>,
    },
    PlanModeExited {
        session_id: String,
        approved: bool,
        #[serde(default)]
        plan: Option<String>,
        #[serde(default)]
        restored_mode: Option<String>,
    },
    PlanFileUpdated {
        session_id: String,
        file_path: String,
        #[serde(default)]
        content_summary: Option<String>,
    },
    Complete {
        usage: TokenUsage,
    },
    Cancelled {
        #[serde(default)]
        message: Option<String>,
    },
    Error {
        message: String,
    },
}

#[derive(Deserialize, Debug, Clone)]
pub struct ToolResult {
    pub success: bool,
    pub result: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TokenUsage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
}

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
