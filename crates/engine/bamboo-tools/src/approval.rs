//! Cross-process approval proxy (Phase 2: child → parent approval delegation).
//!
//! When a tool's permission check returns [`ConfirmationRequired`], the executor
//! normally either pauses an interactive session (synthesizing an
//! `awaiting_permission_approval` result that the engine turns into a
//! clarification) or — with no event sink — fails closed.
//!
//! A **subagent worker** runs out-of-process and cannot surface a prompt to the
//! human itself. Instead it installs an [`ApprovalProxy`] for the duration of a
//! child run (task-local, so concurrent runs on a shared worker can't see each
//! other's proxy). When a gated tool hits `ConfirmationRequired`, the executor
//! forwards the decision to the worker's parent ("host") over the
//! `bamboo-subagent` WebSocket protocol and blocks *inline* for the reply —
//! approve lets the tool proceed, deny fails it closed. No suspend/resume is
//! involved: the tool's future simply parks on the host round-trip.
//!
//! The proxy is **unset on every non-worker path**, so default behavior
//! (interactive pause / fail-closed) is byte-for-byte unchanged.
//!
//! [`ConfirmationRequired`]: crate::permission::PermissionError::ConfirmationRequired

use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;

/// A pending gated-tool approval to forward to the host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalAsk {
    /// The tool requesting the gated action (e.g. `Write`, `Bash`).
    pub tool_name: String,
    /// Human-readable description of the permission being requested.
    pub permission: String,
    /// The concrete resource the action targets (path, command, …).
    pub resource: String,
}

/// Forwards a gated-tool approval decision to an out-of-process host (parent).
///
/// Implementations live in the worker and bridge to the host over the subagent
/// protocol. A transport failure MUST resolve to `false` (fail closed) rather
/// than letting an unapproved action proceed.
#[async_trait]
pub trait ApprovalProxy: Send + Sync {
    /// Ask the host to approve `ask`. Return `true` to proceed, `false` to deny.
    async fn request_approval(&self, ask: ApprovalAsk) -> bool;
}

tokio::task_local! {
    static APPROVAL_PROXY: Option<Arc<dyn ApprovalProxy>>;
}

/// Run `fut` with `proxy` installed as the ambient approval proxy for the
/// duration of the future (and everything it awaits). The worker scopes this
/// around a single child run so the proxy is bound per-run, not per-process.
pub async fn with_approval_proxy<F, T>(proxy: Option<Arc<dyn ApprovalProxy>>, fut: F) -> T
where
    F: Future<Output = T>,
{
    APPROVAL_PROXY.scope(proxy, fut).await
}

/// The ambient approval proxy for the current task, if one is installed.
///
/// Returns `None` outside any [`with_approval_proxy`] scope (i.e. on every
/// non-worker path), which makes the executor fall back to its existing
/// interactive-pause / fail-closed behavior.
pub fn current_approval_proxy() -> Option<Arc<dyn ApprovalProxy>> {
    APPROVAL_PROXY.try_with(|p| p.clone()).ok().flatten()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Recorder {
        approve: bool,
        seen: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl ApprovalProxy for Recorder {
        async fn request_approval(&self, _ask: ApprovalAsk) -> bool {
            self.seen.fetch_add(1, Ordering::SeqCst);
            self.approve
        }
    }

    #[tokio::test]
    async fn current_proxy_is_none_outside_scope() {
        assert!(current_approval_proxy().is_none());
    }

    #[tokio::test]
    async fn scope_installs_and_clears_proxy() {
        let seen = Arc::new(AtomicUsize::new(0));
        let proxy: Arc<dyn ApprovalProxy> = Arc::new(Recorder {
            approve: true,
            seen: seen.clone(),
        });

        with_approval_proxy(Some(proxy), async {
            let got = current_approval_proxy().expect("proxy installed in scope");
            assert!(
                got.request_approval(ApprovalAsk {
                    tool_name: "Write".into(),
                    permission: "write".into(),
                    resource: "/tmp/x".into(),
                })
                .await
            );
        })
        .await;

        assert_eq!(seen.load(Ordering::SeqCst), 1);
        // Proxy is cleared once the scope ends.
        assert!(current_approval_proxy().is_none());
    }

    #[tokio::test]
    async fn none_scope_keeps_proxy_unset() {
        with_approval_proxy(None, async {
            assert!(current_approval_proxy().is_none());
        })
        .await;
    }
}
