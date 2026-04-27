//! Subagent profile composition for `bamboo-server`.
//!
//! - [`builtin`] supplies the six default profiles
//!   (`general-purpose` / `plan` / `researcher` / `coder` /
//!   `reviewer` / `tester`).
//! - [`loader`] composes builtin + user-global + project-level + env-pointed
//!   override files into a single [`SubagentProfileRegistry`].
//!
//! This module is **standalone**: nothing in the runtime imports it yet (the
//! registry will be plumbed into `AppState` in the same PR but no consumer
//! reads from it). Subsequent PRs add the runtime wiring.

pub mod builtin;
pub mod loader;

pub use loader::{load_registry, LoaderError};
