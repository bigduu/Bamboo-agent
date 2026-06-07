//! Shared wire types for Bamboo client front-ends.
//!
//! Both `bamboo-cli` and `bamboo-tui` are standalone HTTP/SSE clients that talk
//! to the Bamboo server over REST + Server-Sent Events. They previously each
//! re-declared the same request/response/event structs; this crate is the
//! single source of truth for those wire shapes. It depends only on `serde`
//! (no workspace-internal crates) so the clients stay decoupled from the
//! server's internal types.

use serde::{Deserialize, Serialize};

// ── Chat ──

#[derive(Serialize, Clone, Debug)]
pub struct ChatRequest {
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Optional model override. Omitted from the request body when `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ChatResponse {
    pub session_id: String,
    pub stream_url: String,
    pub status: String,
}

// ── Execute ──

#[derive(Serialize, Clone, Debug)]
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

// ── SSE events ──

/// Server-Sent Event payload streamed during an agent run.
///
/// Tagged by a `type` field, snake_cased. This is the superset of variants
/// emitted by the server; individual front-ends may only render a subset.
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
