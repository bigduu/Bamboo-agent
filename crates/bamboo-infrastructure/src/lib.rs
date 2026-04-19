//! Bamboo infrastructure — config, LLM, storage, process.

pub mod config;
pub mod llm;
pub mod storage;
pub mod process;

// Flat re-exports for backward compatibility
pub use config::*;

// Re-export LLM sub-modules so `bamboo_infrastructure::models::ContentPart` works
pub mod models {
    pub use crate::llm::models::*;
}
pub mod provider {
    pub use crate::llm::provider::*;
}
pub mod types {
    pub use crate::llm::types::*;
}
pub mod http_client {
    pub use crate::llm::http_client::*;
}
pub mod provider_factory {
    pub use crate::llm::provider_factory::*;
}
pub mod protocol {
    pub use crate::llm::protocol::*;
}
pub mod providers {
    pub use crate::llm::providers::*;
}
pub mod error {
    pub use crate::llm::error::*;
}

pub use llm::api;
pub use llm::{LLMProvider, LLMError, LLMRequestOptions, LLMStream, LLMChunk};
pub use llm::{
    AnthropicProtocol, FromProvider, GeminiProtocol, OpenAIProtocol, ProtocolError, ProtocolResult,
    ToProvider, ProxyAuthRequiredError,
};
pub use llm::{
    create_provider, create_provider_with_dir, validate_provider_config, AVAILABLE_PROVIDERS,
};
pub use llm::{AnthropicProvider, CopilotProvider, GeminiProvider, OpenAIProvider};
pub use storage::{JsonlStorage, SessionSearchIndex, SessionSearchMatch};
pub use storage::{CleanupMode, CleanupResult, SessionIndexEntry, SessionStoreV2, SessionsIndex};
pub use process::{
    ProcessHandle, ProcessInfo, ProcessRegistrationConfig, ProcessRegistry, ProcessType,
};
pub use process::process_utils::{
    ShellCommand, CommandEnvironmentSource, PythonDiscoveryDiagnostics,
    CommandEnvironmentDiagnostics, PreparedCommandEnvironment,
    build_command_environment, render_command_line, windows_command_trace_enabled,
    trace_windows_command, decode_process_line_lossy, preferred_bash_shell,
    hide_window_for_std_command, hide_window_for_tokio_command,
};

#[cfg(any(test, feature = "test-utils"))]
pub use process::process_utils::{
    clear_command_environment_cache_for_tests, prime_command_environment_cache_for_tests,
};

// Process/storage sub-module re-exports for backward compat
pub mod process_utils {
    pub use crate::process::process_utils::*;
}
pub mod registry {
    pub use crate::process::registry::*;
}
pub mod jsonl {
    pub use crate::storage::jsonl::*;
}
pub mod search_index {
    pub use crate::storage::search_index::*;
}
pub mod v2 {
    pub use crate::storage::v2::*;
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::{Mutex, MutexGuard, OnceLock};

    pub(crate) fn env_cache_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    pub(crate) fn env_cache_lock_acquire() -> MutexGuard<'static, ()> {
        env_cache_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
