//! Bamboo infrastructure — config, LLM, storage, process.

// The A2A protocol client now lives in the `bamboo-a2a` crate; re-exported
// here so the historical `bamboo_infrastructure::a2a::…` paths keep resolving
// unchanged.
pub use bamboo_a2a as a2a;
pub mod logging;
pub mod metrics;
pub mod process;

// The session storage layer now lives in the `bamboo-storage` crate; re-exported
// here so the historical `bamboo_infrastructure::storage::…` paths (and
// engine/server callers) keep resolving unchanged.
pub use bamboo_storage as storage;

// Configuration now lives in the `bamboo-config` crate; re-exported here so the
// historical `bamboo_infrastructure::config::…` / flat `bamboo_infrastructure::Config`
// paths (and engine/server/tools/llm callers) keep resolving unchanged.
pub use bamboo_config as config;

// The LLM provider layer now lives in the `bamboo-llm` crate; aliased here so
// the facade modules below (`crate::llm::…`) and every
// `bamboo_infrastructure::{LLMProvider, ProviderRegistry, …}` consumer keep
// resolving unchanged.
pub use bamboo_llm as llm;

// Flat re-exports for backward compatibility
pub use bamboo_config::*;

// Re-export LLM sub-modules so `bamboo_infrastructure::models::ContentPart` works
pub mod models {
    pub use crate::llm::models::*;
}
pub mod provider {
    pub use crate::llm::provider::*;
}
pub mod provider_registry {
    pub use crate::llm::provider_registry::*;
}
pub mod router {
    pub use crate::llm::router::*;
}
pub mod model_catalog {
    pub use crate::llm::model_catalog::*;
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
pub use llm::ModelCatalogService;
pub use llm::ProviderModelRouter;
pub use llm::ProviderRegistry;
pub use llm::ResolvedModel;
pub use llm::{
    create_provider, create_provider_with_dir, validate_provider_config, AVAILABLE_PROVIDERS,
};
pub use llm::{
    AnthropicProtocol, FromProvider, GeminiProtocol, OpenAIProtocol, ProtocolError, ProtocolResult,
    ProxyAuthRequiredError, ToProvider,
};
pub use llm::{AnthropicProvider, CopilotProvider, GeminiProvider, OpenAIProvider};
pub use llm::{CacheTtl, PromptCachePlan};
pub use llm::{LLMChunk, LLMError, LLMProvider, LLMRequestOptions, LLMStream};
pub use process::process_utils::{
    build_command_environment, decode_process_line_lossy, hide_window_for_std_command,
    hide_window_for_tokio_command, preferred_bash_shell, render_command_line,
    trace_windows_command, windows_command_trace_enabled, CommandEnvironmentDiagnostics,
    CommandEnvironmentSource, PreparedCommandEnvironment, PythonDiscoveryDiagnostics, ShellCommand,
};
pub use process::{
    ProcessHandle, ProcessInfo, ProcessRegistrationConfig, ProcessRegistry, ProcessType,
};
pub use storage::{
    merge_save_session, JsonlStorage, LockedSessionStore, SessionSearchIndex, SessionSearchMatch,
};
pub use storage::{CleanupMode, CleanupResult, SessionIndexEntry, SessionStoreV2, SessionsIndex};

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

#[cfg(any(test, feature = "test-utils"))]
pub mod test_support {
    use std::sync::{Mutex, MutexGuard, OnceLock};

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
