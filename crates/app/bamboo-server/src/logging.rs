//! Logging initialization.
//!
//! The implementation lives in `bamboo-infrastructure` so the server binary, the
//! CLI/TUI, and embedded hosts share daily-rotating log files with date-based
//! retention. The standalone server has a stable `info` default while embedding
//! APIs can opt into build-profile-derived levels.
//! These re-exports preserve the historical `bamboo_agent::server::logging::*`
//! call paths.
pub use bamboo_infrastructure::logging::{
    init_logging, init_logging_with_home, init_logging_with_options, init_server_logging_with_home,
    LogOptions, DEFAULT_MAX_LOG_FILES,
};
