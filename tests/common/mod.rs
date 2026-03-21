//! Common test utilities and fixtures

use std::sync::Once;
use tempfile::TempDir;

static INIT: Once = Once::new();

/// Initialize test environment
pub fn init_test_env() {
    INIT.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_test_writer()
            .try_init();
    });
}

/// Create a temporary directory for test data
pub fn create_temp_dir() -> TempDir {
    tempfile::TempDir::new().expect("Failed to create temp dir")
}
