//! Hook system types — lifecycle phases and hook results.
//!
//! These types define the contract between the agent loop and its
//! extension points.  The trait itself (`AgentHook`) lives in
//! `bamboo-engine` because it depends on `Session`.

use serde::{Deserialize, Serialize};

/// Point-specific data supplied to an [`AgentHookPoint`] callback.
///
/// Payloads own their data deliberately.  Hooks are asynchronous and may be
/// shared by concurrent runs, so keeping the public contract free of borrowed
/// engine-only types makes it serializable, easy to test, and preserves the
/// domain/engine dependency boundary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum HookPayload {
    /// A point currently has no additional structured data.
    #[default]
    None,
    /// Session initialization completed for this user turn.
    SessionSetup { initial_message: String },
    /// A round is about to start or has just completed.
    Round { round: u32 },
    /// A prompt-oriented hook payload reserved for prompt/LLM seams.
    Prompt { prompt: String },
    /// A parsed tool call immediately before dispatch.
    ToolExecution {
        tool_name: String,
        tool_call_id: String,
        parsed_args: serde_json::Value,
    },
    /// A tool outcome immediately before it is applied to the session.
    ToolResult {
        tool_name: String,
        tool_call_id: String,
        outcome: HookToolOutcome,
    },
    /// Context compression is about to begin.
    Compression {
        estimated_tokens: u32,
        usage_percent: f64,
        phase: String,
    },
    /// The run is about to emit its terminal completion event.
    Finalize,
}

/// Engine-independent view of a tool execution outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookToolOutcome {
    pub success: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default)]
    pub needs_human: bool,
    #[serde(default)]
    pub duration_ms: u64,
}

/// Lifecycle phases where hooks can be attached.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentHookPoint {
    // Session-level
    BeforeSessionSetup,
    AfterSessionSetup,
    BeforeFinalize,

    // Round-level
    BeforeRound,
    AfterRound,

    // Prompt assembly
    BeforePromptAssembly,
    AfterPromptAssembly,

    // LLM call
    BeforeLlmCall,
    AfterLlmCall,

    // Tool execution
    BeforeToolExecution,
    AfterToolExecution,

    // Memory
    BeforeMemoryRecall,
    AfterMemoryRecall,

    // Context compression
    BeforeCompression,
    AfterCompression,
}

/// Result of running a hook.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum HookResult {
    /// Continue with normal flow (no modifications).
    #[default]
    Continue,
    /// State was mutated; downstream should re-read.
    Mutated,
    /// Explicitly allow the operation represented by the payload.
    Allow,
    /// Deny the operation represented by the payload.
    Deny { reason: String },
    /// Ask the owning parent agent to review the operation.
    ///
    /// The engine never turns this into an unowned/manual approval.  Tool
    /// seams route it through a parent approval delegate/proxy and fail closed
    /// when no such route exists.
    Ask,
    /// Add durable context for the remaining run.
    InjectContext { text: String },
    /// Pause execution; set suspension state.
    Suspend { reason: String },
    /// Abort the agent run.
    Abort { reason: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hook_point_serialization_round_trip() {
        let points = [
            AgentHookPoint::BeforeSessionSetup,
            AgentHookPoint::AfterSessionSetup,
            AgentHookPoint::BeforeFinalize,
            AgentHookPoint::BeforeRound,
            AgentHookPoint::AfterRound,
            AgentHookPoint::BeforePromptAssembly,
            AgentHookPoint::AfterPromptAssembly,
            AgentHookPoint::BeforeLlmCall,
            AgentHookPoint::AfterLlmCall,
            AgentHookPoint::BeforeToolExecution,
            AgentHookPoint::AfterToolExecution,
            AgentHookPoint::BeforeMemoryRecall,
            AgentHookPoint::AfterMemoryRecall,
            AgentHookPoint::BeforeCompression,
            AgentHookPoint::AfterCompression,
        ];
        for point in &points {
            let json = serde_json::to_string(point).unwrap();
            let restored: AgentHookPoint = serde_json::from_str(&json).unwrap();
            assert_eq!(point, &restored);
        }
    }

    #[test]
    fn hook_result_default_is_continue() {
        assert_eq!(HookResult::default(), HookResult::Continue);
    }

    #[test]
    fn hook_result_variants_serialize() {
        let variants = [
            HookResult::Continue,
            HookResult::Mutated,
            HookResult::Allow,
            HookResult::Deny {
                reason: "blocked".to_string(),
            },
            HookResult::Ask,
            HookResult::InjectContext {
                text: "extra context".to_string(),
            },
            HookResult::Suspend {
                reason: "waiting".to_string(),
            },
            HookResult::Abort {
                reason: "error".to_string(),
            },
        ];
        for variant in &variants {
            let json = serde_json::to_string(variant).unwrap();
            let restored: HookResult = serde_json::from_str(&json).unwrap();
            assert_eq!(variant, &restored);
        }
    }

    #[test]
    fn hook_payload_round_trips_structured_tool_data() {
        let payload = HookPayload::ToolExecution {
            tool_name: "Bash".to_string(),
            tool_call_id: "call-1".to_string(),
            parsed_args: serde_json::json!({"command": "pwd"}),
        };
        let json = serde_json::to_string(&payload).unwrap();
        let restored: HookPayload = serde_json::from_str(&json).unwrap();
        assert_eq!(restored, payload);
    }
}
