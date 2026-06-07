//! Subagent profile composition (built-ins + layered loader).
//!
//! - [`builtin`] supplies the six default profiles
//!   (`general-purpose` / `plan` / `researcher` / `coder` /
//!   `reviewer` / `tester`).
//! - [`loader`] composes builtin + user-global + project-level + env-pointed
//!   override files into a single
//!   [`bamboo_domain::subagent::SubagentProfileRegistry`].
//!
//! This module owns the canonical profile system. `bamboo-server` re-exports
//! it through a thin shim (`crate::subagent_profiles`) for back-compat.

pub mod builtin;
pub mod loader;

pub use builtin::builtin_profiles;
pub use loader::{load_registry, LoaderError};
