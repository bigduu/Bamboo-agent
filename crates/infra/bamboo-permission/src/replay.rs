//! Exact task-local identity for an approved permission re-execution.
//!
//! Providers may reuse a tool-call id in a later model round. The server scopes
//! this identity only around the parked operation's explicit replay so a
//! generation-bound one-shot grant cannot authorize an ordinary later call.

use std::future::Future;

tokio::task_local! {
    static PERMISSION_REPLAY: Option<PermissionReplayIdentity>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PermissionReplayIdentity {
    session_id: String,
    request_id: String,
    request_generation: String,
}

/// Run one tool dispatch with its exact approved request identity installed.
pub async fn with_permission_replay_generation<F, T>(
    session_id: &str,
    request_id: &str,
    request_generation: Option<&str>,
    future: F,
) -> T
where
    F: Future<Output = T>,
{
    let identity = request_generation
        .filter(|generation| !generation.trim().is_empty())
        .map(|generation| PermissionReplayIdentity {
            session_id: session_id.to_string(),
            request_id: request_id.to_string(),
            request_generation: generation.to_string(),
        });
    PERMISSION_REPLAY.scope(identity, future).await
}

/// Return the generation only for the exact replay currently being dispatched.
pub fn current_permission_replay_generation(session_id: &str, request_id: &str) -> Option<String> {
    PERMISSION_REPLAY
        .try_with(|identity| {
            identity.as_ref().and_then(|identity| {
                (identity.session_id == session_id && identity.request_id == request_id)
                    .then(|| identity.request_generation.clone())
            })
        })
        .ok()
        .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn replay_generation_is_scoped_to_exact_operation() {
        assert_eq!(
            current_permission_replay_generation("session-1", "request-1"),
            None
        );
        with_permission_replay_generation("session-1", "request-1", Some("generation-1"), async {
            assert_eq!(
                current_permission_replay_generation("session-1", "request-1").as_deref(),
                Some("generation-1")
            );
            assert_eq!(
                current_permission_replay_generation("session-1", "request-2"),
                None
            );
            assert_eq!(
                current_permission_replay_generation("session-2", "request-1"),
                None
            );
        })
        .await;
        assert_eq!(
            current_permission_replay_generation("session-1", "request-1"),
            None
        );
    }
}
