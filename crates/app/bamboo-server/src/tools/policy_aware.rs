//! Re-export shim for [`PolicyAwareToolExecutor`].
//!
//! The implementation moved to `bamboo_tools::policy_aware` (it depends only on
//! `bamboo-agent-core` + `bamboo-domain`, so it belongs in the tools layer).
//! This shim preserves the historical `crate::tools::PolicyAwareToolExecutor`
//! path used by `tools/mod.rs` and the child-session builder.
pub use bamboo_tools::PolicyAwareToolExecutor;
