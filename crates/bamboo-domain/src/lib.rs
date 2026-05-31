//! Bamboo domain types — kernel types, session, schedule, workflow, storage models.

// From bamboo-shared-types
pub mod reasoning;
pub mod token_usage;

// From bamboo-application-tool-types
pub mod mcp_config;
pub mod tool_names;
pub mod tool_types;

// Provider/Model first-class types
pub mod provider_catalog;
pub mod provider_model_ref;

// From bamboo-domain-session (was root-level modules, now under session/)
pub mod session;

// From bamboo-domain-schedule
pub mod schedule;

// From bamboo-domain-workflow
pub mod workflow;

// Storage port definitions (moved from application-agent)
pub mod storage;

// Subagent profile definitions (additive — not yet wired into runtime).
pub mod subagent;

// Flat re-exports for backward-compatible access
pub use mcp_config::*;
pub use provider_catalog::*;
pub use provider_model_ref::ProviderModelRef;
pub use reasoning::{ReasoningEffort, DEFAULT_REASONING_EFFORT};
pub use schedule::*;
pub use session::*;
pub use storage::*;
pub use token_usage::TokenUsage;
pub use tool_names::*;
pub use tool_types::*;
pub use workflow::*;
