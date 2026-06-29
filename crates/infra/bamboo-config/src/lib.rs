//! Runtime/platform configuration for Bamboo agent.
//!
//! **Not a business domain model.** This crate carries:
//! - Provider / model mapping configuration
//! - Proxy authentication
//! - Environment variable hydration and encryption
//! - Keyword masking
//! - XDG-compliant path utilities
//!
//! Name: kept as `domain-config` for backward compatibility, but semantically
//! this is infrastructure/runtime config rather than a stable business domain.

pub mod cluster_fabric;
#[allow(clippy::module_inception)]
pub mod config;
pub mod config_crypto;
pub mod encryption;
pub mod keyword_masking;
pub mod model_mapping;
pub mod patch;
pub mod paths;
pub mod provider_instance;
pub mod settings;
pub mod settings_loader;

pub use cluster_fabric::*;
pub use config::*;
pub use encryption::*;
pub use keyword_masking::*;
pub use model_mapping::*;
pub use paths::*;
pub use provider_instance::synthesize_legacy_instances;
pub use settings::PermissionMode;

/// Test-only synchronization for tests that mutate process-global state
/// (env vars, the encryption-key var, the env-vars snapshot). Tests across
/// `config`, `encryption` and `paths` acquire this single lock so they
/// serialize instead of racing under parallel execution.
#[cfg(test)]
pub mod test_support {
    use std::sync::{Mutex, MutexGuard, OnceLock};

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
