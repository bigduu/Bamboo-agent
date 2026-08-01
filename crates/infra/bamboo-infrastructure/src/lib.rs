//! Bamboo infrastructure — process management, logging, and metrics.
//!
//! This crate provides infrastructure-level utilities that are tightly coupled
//! to the process environment (shell discovery, command building, window hiding).
//!
//! Higher-level crates (LLM, config, storage, A2A) each have their own dedicated
//! crate (`bamboo-llm`, `bamboo-config`, `bamboo-storage`, `bamboo-a2a`) and should
//! be imported directly rather than through this crate.

pub mod logging;
pub mod metrics;
pub mod process;

// Flat re-exports for process utilities (commonly used by bamboo-tools, bamboo-mcp)
pub use process::process_utils::{
    build_command_environment, decode_process_line_lossy, hide_window_for_std_command,
    hide_window_for_tokio_command, preferred_bash_shell, render_command_line,
    trace_windows_command, windows_command_trace_enabled, CommandEnvironmentDiagnostics,
    CommandEnvironmentSource, PreparedCommandEnvironment, PythonDiscoveryDiagnostics, ShellCommand,
};
pub use process::{
    ProcessHandle, ProcessInfo, ProcessRegistrationConfig, ProcessRegistry, ProcessType,
};

// Legacy test-utils exports retained for downstream source compatibility.
// New tests should use `test_support::override_command_environment`.
#[cfg(any(test, feature = "test-utils"))]
pub use process::process_utils::{
    clear_command_environment_cache_for_tests, prime_command_environment_cache_for_tests,
};

// Process sub-module re-exports for backward compat
pub mod process_utils {
    pub use crate::process::process_utils::*;
}
pub mod registry {
    pub use crate::process::registry::*;
}

#[cfg(any(test, feature = "test-utils"))]
pub mod test_support {
    use std::collections::HashMap;
    use std::sync::{Mutex, MutexGuard, OnceLock};

    use crate::process::process_utils;
    use crate::CommandEnvironmentDiagnostics;

    /// Same-thread scope for a deterministic command environment in tests.
    ///
    /// This wrapper is deliberately non-`Send`: keep it alive while polling the
    /// code under test on the same thread. The default `#[tokio::test]` runtime
    /// is current-thread and satisfies that contract. The override never clears
    /// or primes the process-global login-shell cache, so other parallel tests
    /// and in-flight production-cache refreshes remain isolated.
    #[must_use = "keep the guard alive for the full command-environment test scope"]
    pub struct CommandEnvironmentOverrideGuard {
        _inner: process_utils::CommandEnvironmentOverrideGuard,
    }

    /// Install a deterministic command environment for the current test thread.
    ///
    /// Dropping the returned guard restores the previous override, including
    /// when scopes are nested.
    pub fn override_command_environment(
        env: HashMap<String, String>,
        diagnostics: CommandEnvironmentDiagnostics,
    ) -> CommandEnvironmentOverrideGuard {
        CommandEnvironmentOverrideGuard {
            _inner: process_utils::override_command_environment_for_tests(env, diagnostics),
        }
    }

    /// The single crate-wide lock guarding all tests that mutate process-global
    /// state — environment variables (`BAMBOO_DATA_DIR`, `HOME`, `BAMBOO_*`, the
    /// encryption-key var) and the published env-vars static snapshot. Every
    /// such test across `config`, `encryption`, and `paths` acquires this so
    /// they serialize against one another; per-module locks let them race on
    /// the same globals and flake under parallel test execution.
    pub fn env_cache_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    pub fn env_cache_lock_acquire() -> MutexGuard<'static, ()> {
        env_cache_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
