//! SubagentProfile — role definitions for child sessions.
//!
//! Each profile bundles the per-role `system_prompt`, tool policy, model hint,
//! and UI hint. This module only defines types and the registry; runtime
//! consumption (system prompt injection, tool surface filtering, model
//! resolution) lives in higher layers and is wired up in subsequent PRs.
//!
//! # Compatibility
//!
//! - Unknown / missing `subagent_type` strings resolve to the registry's
//!   fallback profile (`general-purpose`), which mirrors today's default
//!   behaviour exactly.
//! - This module is purely additive: nothing else in the codebase depends on
//!   it yet, so adding it is a no-op for existing flows.

mod model;
mod registry;

pub use model::{disabled_tools_for_profile, ModelHint, SubagentProfile, ToolPolicy, UiHint};
pub use registry::{
    SubagentProfileFile, SubagentProfileRegistry, SubagentProfileRegistryBuilder,
    SubagentProfileRegistryError, DEFAULT_FALLBACK_PROFILE_ID,
};
