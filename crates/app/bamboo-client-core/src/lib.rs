//! Shared wire types for Bamboo client front-ends.
//!
//! `bamboo-tui` (and other HTTP/SSE front-ends) talk to the Bamboo server over
//! REST + Server-Sent Events. Rather than re-declaring the same
//! request/response/event structs per client, this crate is the single source
//! of truth for those wire shapes. It depends only on `serde` (no
//! workspace-internal crates) so the clients stay decoupled from the server's
//! internal types.

use serde::{Deserialize, Serialize};

fn default_allow_custom() -> bool {
    true
}

// ── Chat ──

#[derive(Serialize, Clone, Debug)]
pub struct ChatRequest {
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Stable Project membership for a new root session. Existing sessions may
    /// repeat the same id but cannot use chat to reassign membership.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
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

#[cfg(test)]
mod chat_request_tests {
    use super::ChatRequest;

    #[test]
    fn chat_request_serializes_project_identity_for_new_root_session() {
        let value = serde_json::to_value(ChatRequest {
            message: "hello".to_string(),
            session_id: None,
            project_id: Some("project-client".to_string()),
            model: Some("gpt-5".to_string()),
        })
        .unwrap();
        assert_eq!(value["project_id"], "project-client");
        assert!(value.get("session_id").is_none());
    }
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
    ExecutionStarted {
        run_id: String,
        session_id: String,
        started_at: String,
    },
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
        #[serde(default)]
        tool_call_id: Option<String>,
        #[serde(default)]
        tool_name: Option<String>,
        #[serde(default = "default_allow_custom")]
        allow_custom: bool,
        #[serde(default)]
        source: Option<String>,
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
    /// A per-run token/tool-call/subagent budget tripped and the run was
    /// gracefully stopped (issue #221).
    BudgetExceeded {
        /// Which budget tripped: `"max_total_tokens"` | `"max_tool_calls"` |
        /// `"max_subagents"`.
        kind: String,
        limit: u64,
        actual: u64,
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

    // ── Sub-agent lifecycle (forwarded from the parent session's stream) ──
    // Minimal projections of the server's SubAgent* events; extra server fields
    // (parent_session_id, timestamp, the nested `event`) are ignored on decode.
    SubAgentStarted {
        child_session_id: String,
        #[serde(default)]
        title: Option<String>,
    },
    SubAgentHeartbeat {
        child_session_id: String,
    },
    SubAgentCompleted {
        child_session_id: String,
        /// "completed" | "cancelled" | "error" | "skipped"
        status: String,
        #[serde(default)]
        error: Option<String>,
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

#[cfg(test)]
mod clarification_event_tests {
    use super::AgentEvent;

    #[test]
    fn typed_clarification_preserves_all_contract_fields() {
        let event: AgentEvent = serde_json::from_value(serde_json::json!({
            "type": "need_clarification",
            "question": "Choose",
            "options": ["same", "same"],
            "tool_call_id": "call-1",
            "tool_name": "ConclusionWithOptions",
            "allow_custom": false,
            "source": "pause_tool"
        }))
        .unwrap();

        assert!(matches!(
            event,
            AgentEvent::NeedClarification {
                question,
                options: Some(options),
                tool_call_id: Some(tool_call_id),
                tool_name: Some(tool_name),
                allow_custom: false,
                source: Some(source),
            } if question == "Choose"
                && options == ["same", "same"]
                && tool_call_id == "call-1"
                && tool_name == "ConclusionWithOptions"
                && source == "pause_tool"
        ));
    }

    #[test]
    fn legacy_clarification_defaults_to_open_custom_mode() {
        let event: AgentEvent = serde_json::from_value(serde_json::json!({
            "type": "need_clarification",
            "question": "Explain",
            "options": null
        }))
        .unwrap();

        assert!(matches!(
            event,
            AgentEvent::NeedClarification {
                allow_custom: true,
                tool_call_id: None,
                tool_name: None,
                source: None,
                ..
            }
        ));
    }
}
