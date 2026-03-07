//! Common test utilities and fixtures

use std::sync::Once;
use tempfile::TempDir;

static INIT: Once = Once::new();

/// Initialize test environment
pub fn init_test_env() {
    INIT.call_once(|| {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Debug)
            .try_init();
    });
}

/// Create a temporary directory for test data
pub fn create_temp_dir() -> TempDir {
    tempfile::TempDir::new().expect("Failed to create temp dir")
}

/// Find an available port for testing
pub fn find_available_port() -> u16 {
    use std::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind to random port");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}
