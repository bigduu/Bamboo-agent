//! Host-only, invocation-scoped denial of interactive/detached suspension.
//! No Session metadata, model argument or permission flag can enable suspension
//! inside this scope. It never grants a permission or changes an existing gate.

use std::future::Future;

tokio::task_local! {
    static SUSPENSION_DISABLED: ();
}

/// Run an owned tool-dispatch future without allowing a new suspension.
/// The engine enters this only for a fresh, actually active managed generation.
/// Task locals are not inherited by tokio::spawn: side-effect owners must reject
/// before spawning, and a host that dispatches in another task must scope it.
pub async fn with_suspension_disabled<F: Future>(future: F) -> F::Output {
    SUSPENSION_DISABLED.scope((), future).await
}

/// Ordinary execution remains unchanged outside an explicit host scope.
pub fn suspension_allowed() -> bool {
    SUSPENSION_DISABLED.try_with(|_| ()).is_err()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn host_only_suspension_scope_is_nested_task_local_and_not_session_metadata() {
        assert!(suspension_allowed());
        with_suspension_disabled(async {
            let mut forged = crate::Session::new("suspension-probe", "synthetic-only");
            forged
                .metadata
                .insert("suspension_allowed".into(), "true".into());
            assert!(!suspension_allowed());
            with_suspension_disabled(async {
                assert!(!suspension_allowed());
            })
            .await;
            assert!(!suspension_allowed());
            // A new task needs its own host scope. Side-effect owners must
            // reject before they spawn; no model-controlled flag restores it.
            assert!(tokio::spawn(async { suspension_allowed() }).await.unwrap());
            assert!(!suspension_allowed());
        })
        .await;
        assert!(suspension_allowed());
    }

    #[tokio::test]
    async fn dropping_a_scoped_future_restores_the_callers_default() {
        let mut pending = Box::pin(with_suspension_disabled(std::future::pending::<()>()));
        tokio::select! {
            biased;
            _ = &mut pending => panic!("pending future completed"),
            _ = std::future::ready(()) => {}
        }
        drop(pending);
        assert!(suspension_allowed());
    }
}
