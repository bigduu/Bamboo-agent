//! Hook system types — lifecycle phases and hook results.
//!
//! These types define the contract between the agent loop and its
//! extension points.  The trait itself (`AgentHook`) lives in
//! `bamboo-application-agent` because it depends on `Session`.

use serde::{Deserialize, Serialize};

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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum HookResult {
    /// Continue with normal flow (no modifications).
    Continue,
    /// State was mutated; downstream should re-read.
    Mutated,
    /// Pause execution; set suspension state.
    Suspend { reason: String },
    /// Abort the agent run.
    Abort { reason: String },
}

impl Default for HookResult {
    fn default() -> Self {
        Self::Continue
    }
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
}
