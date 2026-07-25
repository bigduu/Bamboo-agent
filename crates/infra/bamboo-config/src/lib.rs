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
pub mod config_module;
pub mod config_store;
pub mod credential_migration;
pub mod credential_store;
pub mod dot_path;
pub mod encryption;
pub mod keyword_masking;
pub mod memory_config;
pub mod model_mapping;
pub mod patch;
pub mod paths;
pub mod provider_configs;
pub mod provider_instance;
pub mod section_facade;
pub mod settings;
pub mod settings_loader;
pub mod subagents_config;

pub use cluster_fabric::*;
pub use config::*;
pub use config_module::*;
pub use config_store::*;
pub use credential_migration::*;
pub use credential_store::*;
pub use encryption::*;
pub use keyword_masking::*;
pub use memory_config::MemoryConfigModule;
pub use model_mapping::*;
pub use paths::*;
pub use provider_configs::ProviderConfigsModule;
pub use provider_instance::synthesize_legacy_instances;
pub use section_facade::*;
pub use settings::PermissionMode;
pub use subagents_config::SubagentsConfigModule;

/// Test-only synchronization for tests that mutate process-global state
/// (env vars, the encryption-key var, the env-vars snapshot). Tests across
/// `config`, `encryption` and `paths` acquire this single lock so they
/// serialize instead of racing under parallel execution.
#[cfg(any(test, feature = "test-utils"))]
pub mod test_support {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::marker::PhantomData;
    use std::rc::Rc;
    use std::sync::{Mutex, MutexGuard, OnceLock};

    use crate::PromptSafeEnvVarEntry;

    #[derive(Clone)]
    pub(crate) struct EnvVarsCacheSnapshot {
        env_vars: HashMap<String, String>,
        prompt_safe_env_vars: Vec<PromptSafeEnvVarEntry>,
    }

    thread_local! {
        /// Test-only env snapshots are deliberately separate from the
        /// process-global runtime cache. Rust's test harness runs tests on
        /// parallel OS threads, so each scoped test can publish and inspect its
        /// own snapshot without racing unrelated server `AppState` publishers.
        static ENV_VARS_CACHE_OVERRIDE_FOR_TESTS:
            RefCell<Option<EnvVarsCacheSnapshot>> = const { RefCell::new(None) };
    }

    /// Restores the previous test-only env snapshot when its scope ends.
    ///
    /// The `Rc` marker deliberately makes this guard `!Send` and `!Sync`:
    /// callers must poll code under test on the same thread that installed the
    /// isolation scope. Default Tokio and Actix test runtimes are
    /// current-thread, so in-scope `publish_env_vars` writes remain observable
    /// while unrelated test threads continue to use the process-global cache.
    #[must_use = "keep the guard alive for the full env-cache test scope"]
    pub struct EnvVarsCacheIsolationGuard {
        previous: Option<EnvVarsCacheSnapshot>,
        _same_thread: PhantomData<Rc<()>>,
    }

    impl Drop for EnvVarsCacheIsolationGuard {
        fn drop(&mut self) {
            let previous = self.previous.take();
            ENV_VARS_CACHE_OVERRIDE_FOR_TESTS.with(|slot| {
                slot.replace(previous);
            });
        }
    }

    /// Isolate `Config` env-cache reads and publications to the current thread.
    ///
    /// The isolated cache starts with the currently visible snapshot. Both
    /// `Config::publish_env_vars` and the two snapshot readers honor it, so a
    /// mutation performed by code under test on this thread is still detected;
    /// only publishers from unrelated parallel test threads are excluded.
    pub fn isolate_env_vars_cache() -> EnvVarsCacheIsolationGuard {
        let snapshot = EnvVarsCacheSnapshot {
            env_vars: crate::Config::current_env_vars(),
            prompt_safe_env_vars: crate::Config::current_prompt_safe_env_vars(),
        };
        let previous = ENV_VARS_CACHE_OVERRIDE_FOR_TESTS.with(|slot| slot.replace(Some(snapshot)));
        EnvVarsCacheIsolationGuard {
            previous,
            _same_thread: PhantomData,
        }
    }

    pub(crate) fn env_vars_cache_override_is_active() -> bool {
        ENV_VARS_CACHE_OVERRIDE_FOR_TESTS.with(|slot| slot.borrow().is_some())
    }

    pub(crate) fn publish_env_vars_to_override(
        env_vars: HashMap<String, String>,
        prompt_safe_env_vars: Vec<PromptSafeEnvVarEntry>,
    ) {
        ENV_VARS_CACHE_OVERRIDE_FOR_TESTS.with(|slot| {
            let mut slot = slot.borrow_mut();
            let snapshot = slot
                .as_mut()
                .expect("env-cache override must be active before publication");
            snapshot.env_vars = env_vars;
            snapshot.prompt_safe_env_vars = prompt_safe_env_vars;
        });
    }

    pub(crate) fn current_env_vars_override() -> Option<HashMap<String, String>> {
        ENV_VARS_CACHE_OVERRIDE_FOR_TESTS.with(|slot| {
            slot.borrow()
                .as_ref()
                .map(|snapshot| snapshot.env_vars.clone())
        })
    }

    pub(crate) fn current_prompt_safe_env_vars_override() -> Option<Vec<PromptSafeEnvVarEntry>> {
        ENV_VARS_CACHE_OVERRIDE_FOR_TESTS.with(|slot| {
            slot.borrow()
                .as_ref()
                .map(|snapshot| snapshot.prompt_safe_env_vars.clone())
        })
    }

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
