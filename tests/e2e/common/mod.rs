//! Common utilities and helpers for e2e tests

use bamboo_agent::server::AppState;
use std::path::PathBuf;
use std::sync::OnceLock;

static TEST_HOME_DIR: OnceLock<PathBuf> = OnceLock::new();
static CLAUDE_FS_LOCK: OnceLock<std::sync::Mutex<()>> = OnceLock::new();

/// Ensure test process has a writable HOME directory.
///
/// Some endpoints (Claude Code integration) use `dirs::home_dir()` and write to `~/.claude`.
/// In our sandboxed test environment the real home directory may not be writable, so we
/// redirect HOME to a temp directory once per test process.
pub fn ensure_test_home_dir() -> PathBuf {
    TEST_HOME_DIR
        .get_or_init(|| {
            let dir = tempfile::tempdir()
                .expect("Failed to create test HOME temp dir")
                .keep();
            std::env::set_var("HOME", &dir);
            dir
        })
        .clone()
}

/// Serialize tests that touch `~/.claude/*` (which is shared process-wide).
pub fn claude_fs_lock() -> std::sync::MutexGuard<'static, ()> {
    CLAUDE_FS_LOCK
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .expect("claude fs lock poisoned")
}

/// Serialize tests that mutate shared agent data directories.
pub fn data_dir_lock() -> std::sync::MutexGuard<'static, ()> {
    claude_fs_lock()
}

/// Create a test app with AppState
pub async fn create_test_app() -> actix_web::web::Data<AppState> {
    ensure_test_home_dir();
    let temp_dir = tempfile::tempdir()
        .expect("Failed to create temp dir")
        .keep();

    actix_web::web::Data::new(
        AppState::new(temp_dir.clone())
            .await
            .expect("Failed to create AppState for e2e test"),
    )
}
