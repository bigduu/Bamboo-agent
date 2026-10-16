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
    request_generation: Option<String>,
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
    // Retain exact replay presence even for legacy approvals with no typed
    // generation. Absence of a generation still cannot install a typed grant.
    let identity = Some(PermissionReplayIdentity {
        session_id: session_id.to_string(),
        request_id: request_id.to_string(),
        request_generation: request_generation
            .filter(|generation| !generation.trim().is_empty())
            .map(str::to_owned),
    });
    PERMISSION_REPLAY.scope(identity, future).await
}

/// True only for the exact host-selected replay, including legacy replay
/// without a generation. This observation never grants a permission.
pub fn permission_replay_active(session_id: &str, tool_call_id: &str) -> bool {
    PERMISSION_REPLAY
        .try_with(|identity| {
            identity.as_ref().is_some_and(|identity| {
                identity.session_id == session_id && identity.request_id == tool_call_id
            })
        })
        .unwrap_or(false)
}

/// Return the generation only for the exact replay currently being dispatched.
pub fn current_permission_replay_generation(session_id: &str, request_id: &str) -> Option<String> {
    PERMISSION_REPLAY
        .try_with(|identity| {
            identity.as_ref().and_then(|identity| {
                if identity.session_id == session_id && identity.request_id == request_id {
                    identity.request_generation.clone()
                } else {
                    None
                }
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

    #[tokio::test]
    async fn replay_presence_includes_legacy_none_without_changing_generation_or_scope() {
        for generation in [Some("generation-1"), None, Some("")] {
            assert!(!permission_replay_active("session-1", "request-1"));
            with_permission_replay_generation("session-1", "request-1", generation, async {
                assert!(permission_replay_active("session-1", "request-1"));
                assert!(!permission_replay_active("session-2", "request-1"));
                assert!(!permission_replay_active("session-1", "request-2"));
                assert_eq!(
                    current_permission_replay_generation("session-1", "request-1").as_deref(),
                    generation.filter(|value| !value.trim().is_empty()),
                );
                with_permission_replay_generation("session-2", "request-2", None, async {
                    assert!(permission_replay_active("session-2", "request-2"));
                    assert!(!permission_replay_active("session-1", "request-1"));
                    assert_eq!(
                        current_permission_replay_generation("session-2", "request-2"),
                        None
                    );
                })
                .await;
                assert!(permission_replay_active("session-1", "request-1"));
                assert_eq!(
                    current_permission_replay_generation("session-1", "request-1").as_deref(),
                    generation.filter(|value| !value.trim().is_empty()),
                );
            })
            .await;
            assert!(!permission_replay_active("session-1", "request-1"));
            assert_eq!(
                current_permission_replay_generation("session-1", "request-1"),
                None
            );
        }
    }
}
