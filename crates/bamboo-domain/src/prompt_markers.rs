//! Shared prompt markers.
//!
//! These HTML-comment markers delimit the legacy TODO-list block injected into
//! system prompts. They were previously redefined in three different crates
//! (server `session_app`, engine `workspace_context` and `prompt_context`);
//! defining them once here keeps the two copies from drifting apart.

/// Opening marker for the legacy injected TODO-list block.
pub const LEGACY_TODO_LIST_START_MARKER: &str = "<!-- BAMBOO_TODO_LIST_START -->";

/// Closing marker for the legacy injected TODO-list block.
pub const LEGACY_TODO_LIST_END_MARKER: &str = "<!-- BAMBOO_TODO_LIST_END -->";
