//! Logging initialization.
//!
//! The implementation lives in `bamboo-infrastructure` so the server binary, the
//! CLI/TUI, and embedded hosts (e.g. the Bodhi Tauri app) share one policy:
//! daily-rotating log files with date-based retention and build-profile levels.
//! These re-exports preserve the historical `bamboo_agent::server::logging::*`
//! call paths.
pub use bamboo_infrastructure::logging::{
    init_logging, init_logging_with_home, init_logging_with_options, LogOptions,
    DEFAULT_MAX_LOG_FILES,
};
