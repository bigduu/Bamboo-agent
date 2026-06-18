//! Per-run nested-spawn proxy (Phase 6: nested execution).
//!
//! A subagent worker runs out-of-process and cannot spawn its own sub-agents
//! directly. When the worker's `SubAgent`-proxy tool runs, it forwards the call
//! to the host (parent orchestrator) over the actor protocol, which performs the
//! real spawn (parenting a grandchild under the requesting child) and returns
//! the tool result.
//!
//! This module is the task-local seam — symmetric to [`crate::approval`]: the
//! worker installs a [`NestedSpawnProxy`] for the duration of a run (task-local,
//! so concurrent runs on a shared worker can't see each other's proxy); the
//! `SubAgent`-proxy tool reads it via [`current_nested_spawn_proxy`]. Unset on
//! every non-worker path (the in-process orchestrator runs the real `SubAgent`
//! tool, not the proxy), so default behavior is unchanged.

use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;

/// Forwards a `SubAgent` tool call from an out-of-process worker to its host.
#[async_trait]
pub trait NestedSpawnProxy: Send + Sync {
    /// Forward the `SubAgent` tool-call arguments to the host and return the
    /// tool-result JSON. A transport failure should surface as `Err`.
    async fn spawn(&self, args: serde_json::Value) -> Result<serde_json::Value, String>;
}

tokio::task_local! {
    static NESTED_SPAWN_PROXY: Option<Arc<dyn NestedSpawnProxy>>;
}

/// Run `fut` with `proxy` installed as the ambient nested-spawn proxy for the
/// duration of the future. The worker scopes this around a single child run.
pub async fn with_nested_spawn_proxy<F, T>(proxy: Option<Arc<dyn NestedSpawnProxy>>, fut: F) -> T
where
    F: Future<Output = T>,
{
    NESTED_SPAWN_PROXY.scope(proxy, fut).await
}

/// The ambient nested-spawn proxy for the current task, if one is installed.
/// `None` outside any [`with_nested_spawn_proxy`] scope (every non-worker path).
pub fn current_nested_spawn_proxy() -> Option<Arc<dyn NestedSpawnProxy>> {
    NESTED_SPAWN_PROXY.try_with(|p| p.clone()).ok().flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct EchoProxy;

    #[async_trait]
    impl NestedSpawnProxy for EchoProxy {
        async fn spawn(&self, args: serde_json::Value) -> Result<serde_json::Value, String> {
            Ok(serde_json::json!({ "echoed": args }))
        }
    }

    #[tokio::test]
    async fn current_proxy_is_none_outside_scope() {
        assert!(current_nested_spawn_proxy().is_none());
    }

    #[tokio::test]
    async fn scope_installs_and_clears_proxy() {
        let proxy: Arc<dyn NestedSpawnProxy> = Arc::new(EchoProxy);
        with_nested_spawn_proxy(Some(proxy), async {
            let got = current_nested_spawn_proxy().expect("proxy installed in scope");
            let out = got
                .spawn(serde_json::json!({"action": "create"}))
                .await
                .unwrap();
            assert_eq!(out["echoed"]["action"], serde_json::json!("create"));
        })
        .await;
        assert!(current_nested_spawn_proxy().is_none());
    }
}
