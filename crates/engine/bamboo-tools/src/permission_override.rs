//! Per-call permission overrides issued by trusted lifecycle hooks.
//!
//! The scope is task-local so concurrent tool calls and sessions cannot see
//! each other's decisions. The engine installs it only around the exact tool
//! dispatch whose `PreToolUse` hook returned `allow`.

use std::future::Future;

/// Permission-gate effects a trusted `PreToolUse` hook may request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookPermissionOverride {
    /// Skip ordinary permission asks for this call. Explicit denies and Bamboo's
    /// non-configurable hard-dangerous backstop remain authoritative.
    Allow,
}

tokio::task_local! {
    static HOOK_PERMISSION_OVERRIDE: Option<ScopedHookPermissionOverride>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ScopedHookPermissionOverride {
    tool_call_id: String,
    decision: HookPermissionOverride,
}

/// Run one tool dispatch with its hook-issued permission override installed.
pub async fn with_hook_permission_override<F, T>(
    permission_override: Option<HookPermissionOverride>,
    tool_call_id: &str,
    future: F,
) -> T
where
    F: Future<Output = T>,
{
    let scoped_override = permission_override.map(|decision| ScopedHookPermissionOverride {
        tool_call_id: tool_call_id.to_string(),
        decision,
    });
    HOOK_PERMISSION_OVERRIDE
        .scope(scoped_override, future)
        .await
}

/// Return the hook-issued override for the current tool dispatch, if any.
pub fn current_hook_permission_override(tool_call_id: &str) -> Option<HookPermissionOverride> {
    HOOK_PERMISSION_OVERRIDE
        .try_with(|value| {
            value
                .as_ref()
                .and_then(|scoped| (scoped.tool_call_id == tool_call_id).then_some(scoped.decision))
        })
        .ok()
        .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn override_is_scoped_to_one_future() {
        assert_eq!(current_hook_permission_override("call-1"), None);
        with_hook_permission_override(Some(HookPermissionOverride::Allow), "call-1", async {
            assert_eq!(
                current_hook_permission_override("call-1"),
                Some(HookPermissionOverride::Allow)
            );
            assert_eq!(
                current_hook_permission_override("nested-call"),
                None,
                "a nested or unrelated call must not inherit the override"
            );
        })
        .await;
        assert_eq!(current_hook_permission_override("call-1"), None);
    }
}
