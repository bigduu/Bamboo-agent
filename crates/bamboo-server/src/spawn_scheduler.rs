//! Sub-session spawn scheduler — re-exported from bamboo-application-runtime.
//!
//! The implementation lives in `bamboo_application_runtime::execution::spawn`.
//! This module provides backward-compatible re-exports for callers in `src/`.

pub use bamboo_application_runtime::execution::spawn::{SpawnContext, SpawnJob, SpawnScheduler};
