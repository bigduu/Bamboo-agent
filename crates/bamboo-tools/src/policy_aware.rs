//! `PolicyAwareToolExecutor`: enforces a subagent profile's [`ToolPolicy`]
//! at tool-call time.
//!
//! This module lives in `bamboo-tools` (it depends only on `bamboo-agent-core`
//! and `bamboo-domain`); `bamboo-server` re-exports it through a thin shim
//! (`crate::tools::PolicyAwareToolExecutor`) for back-compat.
//!
//! This is the runtime half of the SubagentProfile feature. Profile loading
//! and prompt injection live in `bamboo_engine::profiles` and
//! `bamboo_engine::session_app::child_session`; here we wrap the child-session
//! [`ToolExecutor`] so that tool calls are filtered against the calling
//! child's `subagent_type` metadata.
//!
//! ## Why a wrapper instead of changing `SpawnContext`
//!
//! `bamboo-engine`'s `SpawnContext` carries a single `Arc<dyn ToolExecutor>`
//! shared by every child session. Different child sessions can have
//! different `subagent_type`s and therefore different
//! [`ToolPolicy`]s. Rather than changing the engine to dispatch executors
//! per session (which would ripple into `SpawnContext`, `SpawnScheduler`
//! and `ScheduleManager`), this wrapper sits in front of the existing
//! executor and resolves the policy on the fly using
//! [`ToolExecutionContext::session_id`] and the in-memory sessions cache
//! that the server already maintains.
//!
//! ## Behaviour summary
//!
//! - Tool _execution_ is filtered:
//!   - [`ToolPolicy::Inherit`] → forwarded unchanged.
//!   - [`ToolPolicy::Allowlist`] → forwarded only if the tool name is on
//!     the allow list; otherwise rejected with a clear
//!     `ToolError::Execution` message.
//!   - [`ToolPolicy::Denylist`] → rejected if the tool name is on the
//!     deny list; otherwise forwarded.
//! - Tool _discovery_ (`list_tools`) is **not** filtered here. The
//!   advertised tool surface is filtered upstream via the engine's
//!   `disabled_tools` mechanism, which includes the subagent profile
//!   policy (see `ChildSessionAdapter::enqueue_child_run`). This wrapper
//!   remains as a safety net for execution-time enforcement.
//! - When the wrapper cannot determine a policy (no `session_id`, the
//!   session is not in cache, no `subagent_type` metadata, or the
//!   profile is unknown), it forwards unchanged. This keeps the change
//!   strictly additive: any existing call path that has not yet adopted
//!   subagent profiles continues to behave exactly as before.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bamboo_agent_core::tools::{
    ToolCall, ToolError, ToolExecutionContext, ToolExecutor, ToolResult, ToolSchema,
};
use bamboo_agent_core::Session;
use bamboo_domain::subagent::{SubagentProfileRegistry, ToolPolicy};
use tokio::sync::RwLock;

/// Tool executor that enforces a subagent profile's [`ToolPolicy`] when
/// executing tool calls from a child session.
pub struct PolicyAwareToolExecutor {
    inner: Arc<dyn ToolExecutor>,
    profiles: Arc<SubagentProfileRegistry>,
    sessions: Arc<RwLock<HashMap<String, Session>>>,
}

impl PolicyAwareToolExecutor {
    pub fn new(
        inner: Arc<dyn ToolExecutor>,
        profiles: Arc<SubagentProfileRegistry>,
        sessions: Arc<RwLock<HashMap<String, Session>>>,
    ) -> Self {
        Self {
            inner,
            profiles,
            sessions,
        }
    }

    /// Look up the `subagent_type` metadata for a session id from the
    /// in-memory cache. Returns `None` when the session is not cached or
    /// the metadata key is missing / blank.
    async fn subagent_type_for_session(&self, session_id: &str) -> Option<String> {
        let sessions = self.sessions.read().await;
        let value = sessions
            .get(session_id)?
            .metadata
            .get("subagent_type")?
            .trim();
        if value.is_empty() {
            None
        } else {
            Some(value.to_string())
        }
    }

    /// Check whether a tool call is permitted under the given policy.
    /// Returns `Ok(())` when allowed, `Err(reason)` when blocked.
    fn check_policy(
        policy: &ToolPolicy,
        tool_name: &str,
        subagent_type: &str,
    ) -> Result<(), String> {
        match policy {
            ToolPolicy::Inherit => Ok(()),
            ToolPolicy::Allowlist { allow } => {
                if allow.iter().any(|t| t == tool_name) {
                    Ok(())
                } else {
                    Err(format!(
                        "tool '{tool_name}' is not permitted for subagent_type \
                         '{subagent_type}' (allowlist policy: {allow:?})"
                    ))
                }
            }
            ToolPolicy::Denylist { deny } => {
                if deny.iter().any(|t| t == tool_name) {
                    Err(format!(
                        "tool '{tool_name}' is denied for subagent_type \
                         '{subagent_type}' (denylist policy: {deny:?})"
                    ))
                } else {
                    Ok(())
                }
            }
        }
    }

    /// Resolve the policy for a tool call: returns `Ok(())` when the call
    /// should proceed, or an `Err` payload to be surfaced as a
    /// `ToolError::Execution`. When no policy can be resolved the call is
    /// allowed (legacy / inherit behaviour).
    async fn evaluate(&self, call: &ToolCall, session_id: Option<&str>) -> Result<(), String> {
        let Some(session_id) = session_id else {
            return Ok(());
        };
        let Some(subagent_type) = self.subagent_type_for_session(session_id).await else {
            return Ok(());
        };
        let profile = self.profiles.resolve(&subagent_type);
        Self::check_policy(&profile.tools, call.function.name.trim(), &subagent_type)
    }
}

#[async_trait]
impl ToolExecutor for PolicyAwareToolExecutor {
    async fn execute(&self, call: &ToolCall) -> std::result::Result<ToolResult, ToolError> {
        // No session context available via plain `execute` — fall through.
        // The agent loop calls `execute_with_context`; this branch is only
        // hit by direct callers that don't carry session metadata, in
        // which case we mirror legacy behaviour exactly.
        self.inner.execute(call).await
    }

    async fn execute_with_context(
        &self,
        call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> std::result::Result<ToolResult, ToolError> {
        if let Err(reason) = self.evaluate(call, ctx.session_id).await {
            return Err(ToolError::Execution(reason));
        }
        self.inner.execute_with_context(call, ctx).await
    }

    fn list_tools(&self) -> Vec<ToolSchema> {
        // Discovery is intentionally not filtered here; see module docs.
        self.inner.list_tools()
    }

    fn tool_mutability(&self, tool_name: &str) -> bamboo_agent_core::tools::ToolMutability {
        self.inner.tool_mutability(tool_name)
    }

    fn call_mutability(&self, call: &ToolCall) -> bamboo_agent_core::tools::ToolMutability {
        self.inner.call_mutability(call)
    }

    fn tool_concurrency_safe(&self, tool_name: &str) -> bool {
        self.inner.tool_concurrency_safe(tool_name)
    }

    fn call_concurrency_safe(&self, call: &ToolCall) -> bool {
        self.inner.call_concurrency_safe(call)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::tools::{FunctionCall, ToolMutability};
    use bamboo_domain::subagent::SubagentProfile;

    /// Tiny stand-in executor that records every name it was asked to
    /// execute and always succeeds with a fixed payload. We use it to
    /// assert "forwarded vs blocked" without spinning up the full builtin
    /// tool surface.
    struct RecordingExecutor {
        executed: Arc<RwLock<Vec<String>>>,
    }

    impl RecordingExecutor {
        fn new() -> (Arc<Self>, Arc<RwLock<Vec<String>>>) {
            let executed = Arc::new(RwLock::new(Vec::new()));
            let exec = Arc::new(Self {
                executed: executed.clone(),
            });
            (exec, executed)
        }
    }

    #[async_trait]
    impl ToolExecutor for RecordingExecutor {
        async fn execute(&self, call: &ToolCall) -> std::result::Result<ToolResult, ToolError> {
            self.executed.write().await.push(call.function.name.clone());
            Ok(ToolResult {
                success: true,
                result: "ok".to_string(),
                display_preference: None,
            })
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            Vec::new()
        }

        fn tool_mutability(&self, _tool_name: &str) -> ToolMutability {
            ToolMutability::ReadOnly
        }
    }

    fn make_call(name: &str) -> ToolCall {
        ToolCall {
            id: "call_1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: name.to_string(),
                arguments: "{}".to_string(),
            },
        }
    }

    fn registry_with(profile: SubagentProfile) -> Arc<SubagentProfileRegistry> {
        // The registry requires the fallback id to exist in the profile
        // set. Use the profile's own id so each test can declare just one
        // profile and still build a valid registry.
        let id = profile.id.clone();
        Arc::new(
            SubagentProfileRegistry::builder()
                .extend(vec![profile])
                .fallback_id(id)
                .build()
                .expect("registry build"),
        )
    }

    fn profile(id: &str, tools: ToolPolicy) -> SubagentProfile {
        SubagentProfile {
            id: id.to_string(),
            display_name: id.to_string(),
            description: String::new(),
            system_prompt: "p".to_string(),
            tools,
            model_hint: None,
            default_responsibility: None,
            ui: Default::default(),
        }
    }

    async fn sessions_with(
        session_id: &str,
        subagent_type: Option<&str>,
    ) -> Arc<RwLock<HashMap<String, Session>>> {
        let mut map = HashMap::new();
        let mut session = Session::new_child(session_id, "root", "test-model", "Child");
        if let Some(t) = subagent_type {
            session
                .metadata
                .insert("subagent_type".to_string(), t.to_string());
        }
        map.insert(session_id.to_string(), session);
        Arc::new(RwLock::new(map))
    }

    #[tokio::test]
    async fn inherit_policy_forwards_all_calls() {
        let (inner, executed) = RecordingExecutor::new();
        let registry = registry_with(profile("test", ToolPolicy::Inherit));
        let sessions = sessions_with("s1", Some("test")).await;
        let exec = PolicyAwareToolExecutor::new(inner, registry, sessions);

        let call = make_call("Read");
        let ctx = ToolExecutionContext {
            session_id: Some("s1"),
            tool_call_id: "call_1",
            event_tx: None,
            available_tool_schemas: None,
        };
        exec.execute_with_context(&call, ctx).await.unwrap();
        assert_eq!(executed.read().await.as_slice(), &["Read".to_string()]);
    }

    #[tokio::test]
    async fn allowlist_permits_listed_tool() {
        let (inner, executed) = RecordingExecutor::new();
        let registry = registry_with(profile(
            "researcher",
            ToolPolicy::Allowlist {
                allow: vec!["Read".to_string(), "Grep".to_string()],
            },
        ));
        let sessions = sessions_with("s1", Some("researcher")).await;
        let exec = PolicyAwareToolExecutor::new(inner, registry, sessions);

        let ctx = ToolExecutionContext {
            session_id: Some("s1"),
            tool_call_id: "call_1",
            event_tx: None,
            available_tool_schemas: None,
        };
        exec.execute_with_context(&make_call("Read"), ctx)
            .await
            .unwrap();
        assert_eq!(executed.read().await.as_slice(), &["Read".to_string()]);
    }

    #[tokio::test]
    async fn allowlist_blocks_unlisted_tool() {
        let (inner, executed) = RecordingExecutor::new();
        let registry = registry_with(profile(
            "researcher",
            ToolPolicy::Allowlist {
                allow: vec!["Read".to_string()],
            },
        ));
        let sessions = sessions_with("s1", Some("researcher")).await;
        let exec = PolicyAwareToolExecutor::new(inner, registry, sessions);

        let ctx = ToolExecutionContext {
            session_id: Some("s1"),
            tool_call_id: "call_1",
            event_tx: None,
            available_tool_schemas: None,
        };
        let err = exec
            .execute_with_context(&make_call("Edit"), ctx)
            .await
            .unwrap_err();
        match err {
            ToolError::Execution(msg) => {
                assert!(msg.contains("Edit"), "msg should name tool: {msg}");
                assert!(
                    msg.contains("researcher"),
                    "msg should name subagent_type: {msg}"
                );
                assert!(msg.contains("allowlist"), "msg should name mode: {msg}");
            }
            other => panic!("expected ToolError::Execution, got {other:?}"),
        }
        assert!(executed.read().await.is_empty());
    }

    #[tokio::test]
    async fn denylist_blocks_listed_tool() {
        let (inner, executed) = RecordingExecutor::new();
        let registry = registry_with(profile(
            "coder",
            ToolPolicy::Denylist {
                deny: vec!["SubAgent".to_string()],
            },
        ));
        let sessions = sessions_with("s1", Some("coder")).await;
        let exec = PolicyAwareToolExecutor::new(inner, registry, sessions);

        let ctx = ToolExecutionContext {
            session_id: Some("s1"),
            tool_call_id: "call_1",
            event_tx: None,
            available_tool_schemas: None,
        };
        let err = exec
            .execute_with_context(&make_call("SubAgent"), ctx)
            .await
            .unwrap_err();
        match err {
            ToolError::Execution(msg) => {
                assert!(msg.contains("SubAgent"));
                assert!(msg.contains("denylist"));
            }
            other => panic!("expected ToolError::Execution, got {other:?}"),
        }
        assert!(executed.read().await.is_empty());
    }

    #[tokio::test]
    async fn denylist_permits_unlisted_tool() {
        let (inner, executed) = RecordingExecutor::new();
        let registry = registry_with(profile(
            "coder",
            ToolPolicy::Denylist {
                deny: vec!["SubAgent".to_string()],
            },
        ));
        let sessions = sessions_with("s1", Some("coder")).await;
        let exec = PolicyAwareToolExecutor::new(inner, registry, sessions);

        let ctx = ToolExecutionContext {
            session_id: Some("s1"),
            tool_call_id: "call_1",
            event_tx: None,
            available_tool_schemas: None,
        };
        exec.execute_with_context(&make_call("Read"), ctx)
            .await
            .unwrap();
        assert_eq!(executed.read().await.as_slice(), &["Read".to_string()]);
    }

    #[tokio::test]
    async fn missing_session_id_falls_through() {
        // No session_id in context → wrapper must not reject. This is the
        // "legacy / direct caller" path and must keep working.
        let (inner, executed) = RecordingExecutor::new();
        let registry = registry_with(profile(
            "researcher",
            ToolPolicy::Allowlist {
                allow: vec!["Read".to_string()],
            },
        ));
        let sessions = sessions_with("s1", Some("researcher")).await;
        let exec = PolicyAwareToolExecutor::new(inner, registry, sessions);

        let ctx = ToolExecutionContext::none("call_1");
        exec.execute_with_context(&make_call("Edit"), ctx)
            .await
            .unwrap();
        assert_eq!(executed.read().await.as_slice(), &["Edit".to_string()]);
    }

    #[tokio::test]
    async fn unknown_session_falls_through() {
        let (inner, executed) = RecordingExecutor::new();
        let registry = registry_with(profile(
            "researcher",
            ToolPolicy::Allowlist {
                allow: vec!["Read".to_string()],
            },
        ));
        // Cache contains a different session id than the one referenced.
        let sessions = sessions_with("other", Some("researcher")).await;
        let exec = PolicyAwareToolExecutor::new(inner, registry, sessions);

        let ctx = ToolExecutionContext {
            session_id: Some("missing"),
            tool_call_id: "call_1",
            event_tx: None,
            available_tool_schemas: None,
        };
        exec.execute_with_context(&make_call("Edit"), ctx)
            .await
            .unwrap();
        assert_eq!(executed.read().await.as_slice(), &["Edit".to_string()]);
    }

    #[tokio::test]
    async fn missing_subagent_type_metadata_falls_through() {
        let (inner, executed) = RecordingExecutor::new();
        let registry = registry_with(profile(
            "researcher",
            ToolPolicy::Allowlist {
                allow: vec!["Read".to_string()],
            },
        ));
        // Session is cached but has no subagent_type metadata.
        let sessions = sessions_with("s1", None).await;
        let exec = PolicyAwareToolExecutor::new(inner, registry, sessions);

        let ctx = ToolExecutionContext {
            session_id: Some("s1"),
            tool_call_id: "call_1",
            event_tx: None,
            available_tool_schemas: None,
        };
        exec.execute_with_context(&make_call("Edit"), ctx)
            .await
            .unwrap();
        assert_eq!(executed.read().await.as_slice(), &["Edit".to_string()]);
    }

    #[tokio::test]
    async fn execute_without_context_forwards() {
        // The plain `execute` path has no session context. We document this
        // by forwarding unconditionally.
        let (inner, executed) = RecordingExecutor::new();
        let registry = registry_with(profile(
            "researcher",
            ToolPolicy::Allowlist {
                allow: vec!["Read".to_string()],
            },
        ));
        let sessions = sessions_with("s1", Some("researcher")).await;
        let exec = PolicyAwareToolExecutor::new(inner, registry, sessions);

        exec.execute(&make_call("Edit")).await.unwrap();
        assert_eq!(executed.read().await.as_slice(), &["Edit".to_string()]);
    }
}
