//! Bamboo domain types — kernel types, session, schedule, workflow, storage models.

// From bamboo-shared-types
pub mod bounded_dedup;
pub mod poison;
pub mod reasoning;
pub mod token_usage;

// From bamboo-application-tool-types
pub mod mcp_config;
pub mod tool_names;
pub mod tool_types;

// Provider/Model first-class types
pub mod project;
pub mod provider_catalog;
pub mod provider_model_ref;

// From bamboo-domain-session (was root-level modules, now under session/)
pub mod session;

// From bamboo-domain-schedule
pub mod schedule;

// Ledger: prospective-memory records (todos, events, reminders, habits)
pub mod ledger;

// From bamboo-domain-workflow
pub mod workflow;

// Storage port definitions (moved from application-agent)
pub mod storage;

// Shared prompt markers (deduped from server/engine).
pub mod prompt_markers;

// Flat re-exports for backward-compatible access
pub use ledger::*;
pub use mcp_config::*;
pub use project::*;
pub use prompt_markers::{LEGACY_TODO_LIST_END_MARKER, LEGACY_TODO_LIST_START_MARKER};
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
