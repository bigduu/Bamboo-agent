//! Common utilities and helpers for e2e tests

use bamboo_agent::agent::server::state::AppState;
use std::path::PathBuf;

/// Test application configuration
pub struct TestApp {
    pub port: u16,
    pub base_url: String,
    pub temp_dir: PathBuf,
}

impl TestApp {
    /// Create a new test application instance
    pub async fn new() -> Self {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir").keep();

        Self {
            port: 0, // Let OS assign a random port
            base_url: "http://localhost".to_string(),
            temp_dir,
        }
    }
}

/// Create a test app with AppState
pub async fn create_test_app() -> actix_web::web::Data<AppState> {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir").keep();

    actix_web::web::Data::new(
        AppState::new_with_config(
            "openai",
            "http://localhost:12123".to_string(),
            "test-model".to_string(),
            "test-api-key".to_string(),
            Some(temp_dir),
            false,
        )
        .await,
    )
}

/// Create a test session ID
pub fn test_session_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Wait for a condition to be true with timeout
pub async fn wait_for<F, Fut>(mut condition: F, timeout_ms: u64)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_millis(timeout_ms);

    while start.elapsed() < timeout {
        if condition().await {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    panic!("Timeout waiting for condition");
}
