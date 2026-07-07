use async_trait::async_trait;

use bamboo_agent_core::tools::{
    normalize_tool_name, parse_tool_args_best_effort, Tool, ToolCall, ToolError,
    ToolExecutionContext, ToolExecutor, ToolOutcome, ToolResult, ToolSchema,
};
use bamboo_tools::normalize_tool_ref;

/// Tool executor that overlays a single tool on top of an existing executor.
///
/// This is used to add server-only tools (like `SubAgent`) without mutating the
/// underlying built-in/MCP executor.
pub struct OverlayToolExecutor {
    base: std::sync::Arc<dyn ToolExecutor>,
    overlay: std::sync::Arc<dyn Tool>,
}

impl OverlayToolExecutor {
    pub fn new(base: std::sync::Arc<dyn ToolExecutor>, overlay: std::sync::Arc<dyn Tool>) -> Self {
        Self { base, overlay }
    }
}

#[async_trait]
impl ToolExecutor for OverlayToolExecutor {
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
        self.execute_with_context(call, ToolExecutionContext::none(&call.id))
            .await
    }

    async fn execute_with_context(
        &self,
        call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let name = normalize_tool_name(&call.function.name);
        let is_overlay_call = name == self.overlay.name()
            || normalize_tool_ref(name)
                .as_deref()
                .is_some_and(|normalized| normalized == self.overlay.name());
        if is_overlay_call {
            // Gate the overlay tool through the base executor's real permission
            // check BEFORE invoking it (issue #341). The permission checker lives
            // in the base (built-in) executor, so overlay tools (`memory`,
            // `scheduler`, `SubAgent`, …) used to bypass it entirely. `Some`
            // is the interactive approval pause; `Err` is deny / fail-closed.
            if let Some(outcome) = self.base.check_permissions_for(call, &ctx).await? {
                return Ok(outcome.into_tool_result());
            }
            let args_raw = call.function.arguments.trim();
            let (args, parse_warning) = parse_tool_args_best_effort(&call.function.arguments);
            if let Some(warning) = parse_warning {
                tracing::warn!(
                    "Overlay tool argument parsing fallback applied: tool_call_id={}, tool_name={}, args_len={}, warning={}",
                    call.id,
                    call.function.name,
                    args_raw.len(),
                    warning
                );
            }
            return self
                .overlay
                .invoke(args, ctx.to_tool_ctx())
                .await
                .map(|outcome| outcome.into_tool_result());
        }
        self.base.execute_with_context(call, ctx).await
    }

    async fn execute_with_context_outcome(
        &self,
        call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolOutcome, ToolError> {
        let name = normalize_tool_name(&call.function.name);
        let is_overlay_call = name == self.overlay.name()
            || normalize_tool_ref(name)
                .as_deref()
                .is_some_and(|normalized| normalized == self.overlay.name());
        if is_overlay_call {
            // Same permission gate as `execute_with_context` (issue #341): the
            // overlay tool is checked by the base executor before it runs.
            if let Some(outcome) = self.base.check_permissions_for(call, &ctx).await? {
                return Ok(outcome);
            }
            let (args, _) = parse_tool_args_best_effort(&call.function.arguments);
            return self.overlay.invoke(args, ctx.to_tool_ctx()).await;
        }
        self.base.execute_with_context_outcome(call, ctx).await
    }

    /// Delegate the permission gate to the base executor so stacked overlays
    /// chain down to the built-in executor's real check (issue #341). A wrapper
    /// must never silently answer "allowed" — it defers to whatever it wraps.
    async fn check_permissions_for(
        &self,
        call: &ToolCall,
        ctx: &ToolExecutionContext<'_>,
    ) -> Result<Option<ToolOutcome>, ToolError> {
        self.base.check_permissions_for(call, ctx).await
    }

    fn list_tools(&self) -> Vec<ToolSchema> {
        let mut tools = self.base.list_tools();

        // Ensure overlay tool is present exactly once.
        let overlay_schema = self.overlay.to_schema();
        let overlay_name = overlay_schema.function.name.clone();
        tools.retain(|t| t.function.name != overlay_name);
        tools.push(overlay_schema);

        tools.sort_by_key(|t| t.function.name.clone());
        tools
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    use bamboo_agent_core::tools::{FunctionCall, ToolCtx};

    struct BaseExecutor;

    #[async_trait]
    impl ToolExecutor for BaseExecutor {
        async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
            Err(ToolError::Execution(format!(
                "base executor called for {}",
                call.function.name
            )))
        }

        async fn execute_with_context(
            &self,
            call: &ToolCall,
            _ctx: ToolExecutionContext<'_>,
        ) -> Result<ToolResult, ToolError> {
            self.execute(call).await
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            Vec::new()
        }
    }

    struct SubAgentOverlayTool;

    #[async_trait]
    impl Tool for SubAgentOverlayTool {
        fn name(&self) -> &str {
            "SubAgent"
        }

        fn description(&self) -> &str {
            "overlay sub agent"
        }

        fn parameters_schema(&self) -> serde_json::Value {
            json!({"type":"object","properties":{}})
        }

        async fn invoke(
            &self,
            _args: serde_json::Value,
            _ctx: ToolCtx,
        ) -> Result<ToolOutcome, ToolError> {
            Ok(ToolOutcome::Completed(ToolResult {
                success: true,
                result: "overlay".to_string(),
                display_preference: None,
                images: Vec::new(),
            }))
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

    #[tokio::test]
    async fn overlay_executor_routes_spawn_alias_to_overlay_tool() {
        let overlay = OverlayToolExecutor::new(
            std::sync::Arc::new(BaseExecutor),
            std::sync::Arc::new(SubAgentOverlayTool),
        );

        let result = overlay
            .execute(&make_call("sub_task"))
            .await
            .expect("spawn alias should route to overlay");

        assert!(result.success);
        assert_eq!(result.result, "overlay");
    }

    #[tokio::test]
    async fn overlay_executor_keeps_non_overlay_calls_on_base_executor() {
        let overlay = OverlayToolExecutor::new(
            std::sync::Arc::new(BaseExecutor),
            std::sync::Arc::new(SubAgentOverlayTool),
        );

        let err = overlay
            .execute(&make_call("Read"))
            .await
            .expect_err("non-overlay call should stay on base executor");

        assert!(
            matches!(err, ToolError::Execution(msg) if msg.contains("base executor called for Read"))
        );
    }

    // ---- issue #341: overlay tools must hit the base permission gate ---------

    use std::sync::atomic::{AtomicBool, Ordering};

    fn make_call_with_args(name: &str, args: &str) -> ToolCall {
        ToolCall {
            id: "call_1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: name.to_string(),
                arguments: args.to_string(),
            },
        }
    }

    /// An overlay tool named `memory` that records whether it was actually
    /// invoked, so a test can prove the permission gate blocked it BEFORE it ran.
    struct RecordingMemoryOverlayTool {
        invoked: std::sync::Arc<AtomicBool>,
    }

    #[async_trait]
    impl Tool for RecordingMemoryOverlayTool {
        fn name(&self) -> &str {
            "memory"
        }

        fn description(&self) -> &str {
            "overlay memory tool"
        }

        fn parameters_schema(&self) -> serde_json::Value {
            json!({"type":"object","properties":{"action":{"type":"string"}}})
        }

        async fn invoke(
            &self,
            _args: serde_json::Value,
            _ctx: ToolCtx,
        ) -> Result<ToolOutcome, ToolError> {
            self.invoked.store(true, Ordering::SeqCst);
            Ok(ToolOutcome::Completed(ToolResult {
                success: true,
                result: "purged".to_string(),
                display_preference: None,
                images: Vec::new(),
            }))
        }
    }

    #[tokio::test]
    async fn overlay_memory_purge_is_denied_by_base_permission_gate() {
        // Full composition: an OverlayToolExecutor over a real BuiltinToolExecutor
        // that carries a permission checker (deny-dangerous). A destructive
        // `memory purge` overlay call must now route through the base's permission
        // check (which classifies it as a durable WriteFile) and be DENIED instead
        // of silently invoked — the exact hole issue #341 fixed.
        let invoked = std::sync::Arc::new(AtomicBool::new(false));
        let base = bamboo_tools::BuiltinToolExecutor::new_with_permissions(std::sync::Arc::new(
            bamboo_tools::permission::DenyDangerousPermissionChecker,
        ));
        let overlay = OverlayToolExecutor::new(
            std::sync::Arc::new(base),
            std::sync::Arc::new(RecordingMemoryOverlayTool {
                invoked: invoked.clone(),
            }),
        );

        let call = make_call_with_args("memory", r#"{"action":"purge"}"#);
        let result = overlay.execute(&call).await;

        assert!(
            matches!(result, Err(ToolError::Execution(_))),
            "gated overlay call must be denied, got: {result:?}"
        );
        assert!(
            !invoked.load(Ordering::SeqCst),
            "overlay tool must NOT run when the permission gate denies it"
        );
    }

    #[tokio::test]
    async fn overlay_read_only_memory_action_passes_gate_and_runs() {
        // Control: a read-only `memory query` is NOT classified as a write, so the
        // gate is a no-op and the overlay tool still runs — proving the gate only
        // blocks the actions it should, not every overlay call.
        let invoked = std::sync::Arc::new(AtomicBool::new(false));
        let base = bamboo_tools::BuiltinToolExecutor::new_with_permissions(std::sync::Arc::new(
            bamboo_tools::permission::DenyDangerousPermissionChecker,
        ));
        let overlay = OverlayToolExecutor::new(
            std::sync::Arc::new(base),
            std::sync::Arc::new(RecordingMemoryOverlayTool {
                invoked: invoked.clone(),
            }),
        );

        let call = make_call_with_args("memory", r#"{"action":"query"}"#);
        let result = overlay
            .execute(&call)
            .await
            .expect("read-only overlay action should pass the gate");

        assert!(result.success);
        assert_eq!(result.result, "purged");
        assert!(
            invoked.load(Ordering::SeqCst),
            "read-only overlay action must actually run"
        );
    }

    /// A base executor whose permission gate always denies, and whose execute
    /// paths would panic if reached — proves the overlay consults the base's
    /// `check_permissions_for` and short-circuits on `Err` before invoking.
    struct GateDenyingBaseExecutor;

    #[async_trait]
    impl ToolExecutor for GateDenyingBaseExecutor {
        async fn execute(&self, _call: &ToolCall) -> Result<ToolResult, ToolError> {
            panic!("base execute must not be reached for a denied overlay call");
        }

        async fn execute_with_context(
            &self,
            _call: &ToolCall,
            _ctx: ToolExecutionContext<'_>,
        ) -> Result<ToolResult, ToolError> {
            panic!("base execute_with_context must not be reached for a denied overlay call");
        }

        async fn check_permissions_for(
            &self,
            _call: &ToolCall,
            _ctx: &ToolExecutionContext<'_>,
        ) -> Result<Option<ToolOutcome>, ToolError> {
            Err(ToolError::Execution("denied-by-base-gate".to_string()))
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            Vec::new()
        }
    }

    #[tokio::test]
    async fn overlay_call_short_circuits_on_base_gate_error_before_invoke() {
        // Minimal proof of routing: the overlay call reaches the base's
        // `check_permissions_for`, and its `Err` is returned before the overlay
        // tool's `invoke` runs (the overlay tool records if it ran).
        let invoked = std::sync::Arc::new(AtomicBool::new(false));
        let overlay = OverlayToolExecutor::new(
            std::sync::Arc::new(GateDenyingBaseExecutor),
            std::sync::Arc::new(RecordingMemoryOverlayTool {
                invoked: invoked.clone(),
            }),
        );

        let call = make_call_with_args("memory", r#"{"action":"purge"}"#);
        let err = overlay
            .execute(&call)
            .await
            .expect_err("base gate error must short-circuit the overlay call");

        assert!(
            matches!(err, ToolError::Execution(msg) if msg.contains("denied-by-base-gate")),
            "overlay must return the base gate's error verbatim"
        );
        assert!(
            !invoked.load(Ordering::SeqCst),
            "overlay tool must NOT run when the base gate errors"
        );
    }

    #[tokio::test]
    async fn overlay_check_permissions_for_delegates_to_base() {
        // The overlay's own `check_permissions_for` delegates to the base, so a
        // wrapper stacked over this overlay chains down to the real check.
        let overlay = OverlayToolExecutor::new(
            std::sync::Arc::new(GateDenyingBaseExecutor),
            std::sync::Arc::new(RecordingMemoryOverlayTool {
                invoked: std::sync::Arc::new(AtomicBool::new(false)),
            }),
        );

        let call = make_call_with_args("memory", r#"{"action":"purge"}"#);
        let ctx = ToolExecutionContext::none(&call.id);
        let result = overlay.check_permissions_for(&call, &ctx).await;

        assert!(
            matches!(result, Err(ToolError::Execution(ref msg)) if msg.contains("denied-by-base-gate")),
            "overlay check_permissions_for must return the base's decision, got: {result:?}"
        );
    }
}
