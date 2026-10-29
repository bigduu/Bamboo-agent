//! Agent execution API handler.
//!
//! This module provides the HTTP endpoint for triggering AI agent execution
//! on a previously created chat session.

pub(crate) mod defaults;
mod handler;
pub(crate) mod image_fallback;
pub(crate) mod runtime;
mod types;

pub use defaults::handler as defaults_handler;
pub use handler::{handle_execute, handler, retry_waiting_handler};
#[cfg(test)]
pub(crate) use runtime::spawn_agent_execution;
pub use types::{
    ExecuteClientSync, ExecuteRequest, ExecuteResponse, ExecuteSyncInfo, ExecuteSyncReason,
};

#[cfg(test)]
pub(crate) mod scheduled_startup_test_hooks {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, OnceLock};
    use tokio::sync::Notify;

    pub(crate) struct Hook {
        pub(crate) reached: Notify,
        pub(crate) resume: Notify,
    }

    fn hooks() -> &'static Mutex<HashMap<String, Arc<Hook>>> {
        static HOOKS: OnceLock<Mutex<HashMap<String, Arc<Hook>>>> = OnceLock::new();
        HOOKS.get_or_init(|| Mutex::new(HashMap::new()))
    }

    pub(crate) fn install(id: &str) -> Arc<Hook> {
        let hook = Arc::new(Hook { reached: Notify::new(), resume: Notify::new() });
        hooks().lock().unwrap().insert(id.to_string(), hook.clone());
        hook
    }

    pub(crate) async fn pause(id: &str) {
        let hook = hooks().lock().unwrap().remove(id);
        if let Some(hook) = hook {
            hook.reached.notify_one();
            hook.resume.notified().await;
        }
    }
}

#[cfg(test)]
mod tests;
