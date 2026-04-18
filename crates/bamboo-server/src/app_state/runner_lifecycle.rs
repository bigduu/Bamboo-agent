//! Runner lifecycle helpers — re-exported from bamboo-application-runtime.
//!
//! The implementation lives in `bamboo_application_runtime::execution::`.
//! This module provides backward-compatible re-exports for callers in `src/`.

pub use bamboo_application_runtime::execution::event_forwarder::create_event_forwarder;
pub use bamboo_application_runtime::execution::runner_lifecycle::{
    finalize_runner, status_from_execution_result, try_reserve_runner,
};
