use async_trait::async_trait;

use bamboo_agent_core::tools::{
    parse_tool_args_best_effort, Tool, ToolCall, ToolError, ToolExecutionContext, ToolExecutor,
    ToolOutcome, ToolResult, ToolSchema,
};
use bamboo_domain::resolve_tool_reference_name;

#[derive(Clone, Debug, PartialEq, Eq)]
enum OverlayRoute {
    Overlay,
    BaseExact(String),
    BaseFallback,
}

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

    /// Resolve the args to hand the overlay tool, parsing the raw JSON at most
    /// once (issue #106). When the dispatch loop already parsed them (threaded via
    /// `ctx.pre_parsed_args`), reuse that value — the malformed-args fallback
    /// `warn!` was already emitted (or not) at the dispatch site, so it is never
    /// re-emitted here. Otherwise parse leniently, warning once on fallback,
    /// preserving the original single-parse-per-consumer behavior.
    fn resolve_args(&self, call: &ToolCall, ctx: &ToolExecutionContext<'_>) -> serde_json::Value {
        if let Some(pre_parsed) = ctx.pre_parsed_args {
            return pre_parsed.clone();
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
        args
    }

    /// Resolve against the complete overlay + base catalog. Exact base
    /// identities are considered before any compatibility alias can select the
    /// overlay; an intentional same-name overlay replacement retains precedence.
    fn route(&self, reference: &str) -> OverlayRoute {
        let resolved = resolve_tool_reference_name(reference, |candidate| {
            candidate == self.overlay.name() || self.base.owns_exact_tool(candidate)
        });
        match resolved {
            Some(execution_name) if execution_name == self.overlay.name() => OverlayRoute::Overlay,
            Some(execution_name) => OverlayRoute::BaseExact(execution_name),
            None => OverlayRoute::BaseFallback,
        }
    }

    async fn execute_overlay_outcome(
        &self,
        call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolOutcome, ToolError> {
        let args = self.resolve_args(call, &ctx);
        if let Some(outcome) = self
            .base
            .check_permissions_for_resolved(call, self.overlay.name(), &args, &ctx)
            .await?
        {
            return Ok(outcome);
        }
        self.overlay.invoke(args, ctx.to_tool_ctx()).await
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
        self.execute_with_context_outcome(call, ctx)
            .await
            .map(ToolOutcome::into_tool_result)
    }

    async fn execute_with_context_outcome(
        &self,
        call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolOutcome, ToolError> {
        match self.route(&call.function.name) {
            OverlayRoute::Overlay => self.execute_overlay_outcome(call, ctx).await,
            OverlayRoute::BaseExact(execution_name) => {
                self.base
                    .execute_exact_with_context_outcome(call, &execution_name, ctx)
                    .await
            }
            OverlayRoute::BaseFallback => self.base.execute_with_context_outcome(call, ctx).await,
        }
    }

    async fn execute_exact_with_context_outcome(
        &self,
        call: &ToolCall,
        execution_name: &str,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolOutcome, ToolError> {
        if execution_name == self.overlay.name() {
            self.execute_overlay_outcome(call, ctx).await
        } else if self.base.owns_exact_tool(execution_name) {
            self.base
                .execute_exact_with_context_outcome(call, execution_name, ctx)
                .await
        } else {
            Err(ToolError::NotFound(format!(
                "Tool '{}' not found",
                execution_name
            )))
        }
    }

    /// Delegate the permission gate to the base executor so stacked overlays
    /// chain down to the built-in executor's real check (issue #341). A wrapper
    /// must never silently answer "allowed" — it defers to whatever it wraps.
    async fn check_permissions_for(
        &self,
        call: &ToolCall,
        ctx: &ToolExecutionContext<'_>,
    ) -> Result<Option<ToolOutcome>, ToolError> {
        match self.route(&call.function.name) {
            OverlayRoute::Overlay => {
                let args = ctx
                    .pre_parsed_args
                    .cloned()
                    .unwrap_or_else(|| parse_tool_args_best_effort(&call.function.arguments).0);
                self.base
                    .check_permissions_for_resolved(call, self.overlay.name(), &args, ctx)
                    .await
            }
            OverlayRoute::BaseExact(execution_name) => {
                self.base
                    .check_permissions_for_exact(call, &execution_name, ctx)
                    .await
            }
            OverlayRoute::BaseFallback => self.base.check_permissions_for(call, ctx).await,
        }
    }

    async fn check_permissions_for_exact(
        &self,
        call: &ToolCall,
        execution_name: &str,
        ctx: &ToolExecutionContext<'_>,
    ) -> Result<Option<ToolOutcome>, ToolError> {
        if execution_name == self.overlay.name() {
            let args = ctx
                .pre_parsed_args
                .cloned()
                .unwrap_or_else(|| parse_tool_args_best_effort(&call.function.arguments).0);
            self.base
                .check_permissions_for_resolved(call, execution_name, &args, ctx)
                .await
        } else if self.base.owns_exact_tool(execution_name) {
            self.base
                .check_permissions_for_exact(call, execution_name, ctx)
                .await
        } else {
            Err(ToolError::NotFound(format!(
                "Tool '{}' not found",
                execution_name
            )))
        }
    }

    async fn check_permissions_for_resolved(
        &self,
        call: &ToolCall,
        execution_name: &str,
        args: &serde_json::Value,
        ctx: &ToolExecutionContext<'_>,
    ) -> Result<Option<ToolOutcome>, ToolError> {
        self.base
            .check_permissions_for_resolved(call, execution_name, args, ctx)
            .await
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

    fn owns_exact_tool(&self, tool_name: &str) -> bool {
        tool_name == self.overlay.name() || self.base.owns_exact_tool(tool_name)
    }

    fn tool_mutability(&self, tool_name: &str) -> bamboo_agent_core::ToolMutability {
        match self.route(tool_name) {
            OverlayRoute::Overlay => self.overlay.classify(&serde_json::Value::Null).mutability,
            OverlayRoute::BaseExact(execution_name) => self.base.tool_mutability(&execution_name),
            OverlayRoute::BaseFallback => self.base.tool_mutability(tool_name),
        }
    }

    fn call_mutability(&self, call: &ToolCall) -> bamboo_agent_core::ToolMutability {
        self.call_parallel_classification(call).0
    }

    fn tool_concurrency_safe(&self, tool_name: &str) -> bool {
        match self.route(tool_name) {
            OverlayRoute::Overlay => {
                self.overlay
                    .classify(&serde_json::Value::Null)
                    .parallel_safe
            }
            OverlayRoute::BaseExact(execution_name) => {
                self.base.tool_concurrency_safe(&execution_name)
            }
            OverlayRoute::BaseFallback => self.base.tool_concurrency_safe(tool_name),
        }
    }

    fn call_concurrency_safe(&self, call: &ToolCall) -> bool {
        self.call_parallel_classification(call).1
    }

    fn call_parallel_classification(
        &self,
        call: &ToolCall,
    ) -> (bamboo_agent_core::ToolMutability, bool) {
        match self.route(&call.function.name) {
            OverlayRoute::Overlay => {
                let args = parse_tool_args_best_effort(&call.function.arguments).0;
                let class = self.overlay.classify(&args);
                (class.mutability, class.parallel_safe)
            }
            OverlayRoute::BaseExact(execution_name) => {
                let mut exact_call = call.clone();
                exact_call.function.name = execution_name;
                self.base.call_parallel_classification(&exact_call)
            }
            OverlayRoute::BaseFallback => self.base.call_parallel_classification(call),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    use bamboo_agent_core::tools::{FunctionCall, FunctionSchema, ToolCtx};

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

        fn classify(&self, _args: &serde_json::Value) -> bamboo_agent_core::ToolClass {
            bamboo_agent_core::ToolClass::READONLY_PARALLEL
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

    struct ExactBaseExecutor {
        name: &'static str,
        result: Result<&'static str, ToolError>,
    }

    #[async_trait]
    impl ToolExecutor for ExactBaseExecutor {
        async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
            if call.function.name != self.name {
                return Err(ToolError::NotFound(call.function.name.clone()));
            }
            let result = self.result.clone()?;
            Ok(ToolResult {
                success: true,
                result: result.to_string(),
                display_preference: None,
                images: Vec::new(),
            })
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            vec![ToolSchema {
                schema_type: "function".to_string(),
                function: FunctionSchema {
                    name: self.name.to_string(),
                    description: "exact base tool".to_string(),
                    parameters: json!({"type":"object"}),
                },
            }]
        }

        fn owns_exact_tool(&self, tool_name: &str) -> bool {
            tool_name == self.name
        }

        fn call_parallel_classification(
            &self,
            call: &ToolCall,
        ) -> (bamboo_agent_core::ToolMutability, bool) {
            assert_eq!(call.function.name, self.name);
            (bamboo_agent_core::ToolMutability::Mutating, false)
        }
    }

    struct NamedOverlayTool(&'static str);

    #[async_trait]
    impl Tool for NamedOverlayTool {
        fn name(&self) -> &str {
            self.0
        }

        fn description(&self) -> &str {
            "named overlay"
        }

        fn parameters_schema(&self) -> serde_json::Value {
            json!({"type":"object"})
        }

        async fn invoke(
            &self,
            _args: serde_json::Value,
            _ctx: ToolCtx,
        ) -> Result<ToolOutcome, ToolError> {
            Ok(ToolOutcome::Completed(ToolResult {
                success: true,
                result: format!("overlay:{}", self.0),
                display_preference: None,
                images: Vec::new(),
            }))
        }
    }

    #[tokio::test]
    async fn exact_base_spawn_session_beats_overlay_alias_through_stacked_overlays() {
        let base: std::sync::Arc<dyn ToolExecutor> = std::sync::Arc::new(ExactBaseExecutor {
            name: "spawn_session",
            result: Ok("exact-base"),
        });
        let subagent: std::sync::Arc<dyn ToolExecutor> = std::sync::Arc::new(
            OverlayToolExecutor::new(base, std::sync::Arc::new(SubAgentOverlayTool)),
        );
        let stacked =
            OverlayToolExecutor::new(subagent, std::sync::Arc::new(NamedOverlayTool("memory")));

        assert!(stacked.owns_exact_tool("spawn_session"));
        let exact = stacked
            .execute(&make_call("spawn_session"))
            .await
            .expect("base exact owner must win");
        assert_eq!(exact.result, "exact-base");

        let alias = stacked
            .execute(&make_call("sub_task"))
            .await
            .expect("unshadowed alias must reach SubAgent overlay");
        assert_eq!(alias.result, "overlay");
    }

    struct ReResolvingPermissionBase {
        executed: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        permission_checked: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        classified: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl ToolExecutor for ReResolvingPermissionBase {
        async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
            if call.function.name != "spawn_session" {
                return Err(ToolError::NotFound(call.function.name.clone()));
            }
            self.executed
                .lock()
                .unwrap()
                .push(call.function.name.clone());
            Ok(ToolResult {
                success: true,
                result: "exact-base".to_string(),
                display_preference: None,
                images: Vec::new(),
            })
        }

        async fn check_permissions_for(
            &self,
            call: &ToolCall,
            _ctx: &ToolExecutionContext<'_>,
        ) -> Result<Option<ToolOutcome>, ToolError> {
            let permission_name = if call.function.name == "default::spawn_session" {
                "SubAgent"
            } else {
                &call.function.name
            };
            self.permission_checked
                .lock()
                .unwrap()
                .push(permission_name.to_string());
            Ok(None)
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            vec![ToolSchema {
                schema_type: "function".to_string(),
                function: FunctionSchema {
                    name: "spawn_session".to_string(),
                    description: "exact base tool".to_string(),
                    parameters: json!({"type":"object"}),
                },
            }]
        }

        fn owns_exact_tool(&self, tool_name: &str) -> bool {
            tool_name == "spawn_session"
        }

        fn call_parallel_classification(
            &self,
            call: &ToolCall,
        ) -> (bamboo_agent_core::ToolMutability, bool) {
            self.classified
                .lock()
                .unwrap()
                .push(call.function.name.clone());
            (bamboo_agent_core::ToolMutability::Mutating, false)
        }
    }

    #[tokio::test]
    async fn stacked_overlays_keep_base_exact_owner_for_permission_and_classification() {
        let executed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let permission_checked = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let classified = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let base = std::sync::Arc::new(ReResolvingPermissionBase {
            executed: executed.clone(),
            permission_checked: permission_checked.clone(),
            classified: classified.clone(),
        });
        let call = make_call("default::spawn_session");
        let ctx = ToolExecutionContext::none(&call.id);

        base.check_permissions_for(&call, &ctx)
            .await
            .expect("raw permission mismatch repro");
        assert_eq!(permission_checked.lock().unwrap().as_slice(), ["SubAgent"]);
        permission_checked.lock().unwrap().clear();

        let subagent: std::sync::Arc<dyn ToolExecutor> = std::sync::Arc::new(
            OverlayToolExecutor::new(base, std::sync::Arc::new(SubAgentOverlayTool)),
        );
        let stacked =
            OverlayToolExecutor::new(subagent, std::sync::Arc::new(NamedOverlayTool("memory")));

        let result = stacked
            .execute(&call)
            .await
            .expect("stacked exact base owner executes");
        assert_eq!(result.result, "exact-base");
        stacked
            .check_permissions_for(&call, &ctx)
            .await
            .expect("stacked exact base owner permission check");
        assert_eq!(
            stacked.call_parallel_classification(&call),
            (bamboo_agent_core::ToolMutability::Mutating, false)
        );

        assert_eq!(executed.lock().unwrap().as_slice(), ["spawn_session"]);
        assert_eq!(
            permission_checked.lock().unwrap().as_slice(),
            ["spawn_session"]
        );
        assert_eq!(classified.lock().unwrap().as_slice(), ["spawn_session"]);
    }

    #[tokio::test]
    async fn exact_base_not_found_is_not_reinterpreted_as_overlay_alias() {
        let overlay = OverlayToolExecutor::new(
            std::sync::Arc::new(ExactBaseExecutor {
                name: "spawn_session",
                result: Err(ToolError::NotFound("exact owner failed".to_string())),
            }),
            std::sync::Arc::new(SubAgentOverlayTool),
        );

        let error = overlay
            .execute(&make_call("spawn_session"))
            .await
            .expect_err("exact owner error must propagate");
        assert!(matches!(error, ToolError::NotFound(message) if message == "exact owner failed"));
    }

    #[tokio::test]
    async fn namespace_fallback_dispatches_the_resolved_exact_base_identity() {
        let overlay = OverlayToolExecutor::new(
            std::sync::Arc::new(ExactBaseExecutor {
                name: "custom_tool",
                result: Ok("base-custom"),
            }),
            std::sync::Arc::new(SubAgentOverlayTool),
        );
        let call = make_call("default::custom_tool");

        let result = overlay
            .execute(&call)
            .await
            .expect("namespace fallback must execute base exact identity");
        assert_eq!(result.result, "base-custom");
        assert_eq!(
            overlay.call_parallel_classification(&call),
            (bamboo_agent_core::ToolMutability::Mutating, false)
        );
    }

    #[tokio::test]
    async fn same_name_overlay_replacement_keeps_overlay_precedence() {
        let overlay = OverlayToolExecutor::new(
            std::sync::Arc::new(ExactBaseExecutor {
                name: "SubAgent",
                result: Ok("base-subagent"),
            }),
            std::sync::Arc::new(SubAgentOverlayTool),
        );

        let result = overlay
            .execute(&make_call("SubAgent"))
            .await
            .expect("same-name overlay must replace base");
        assert_eq!(result.result, "overlay");
        assert_eq!(
            overlay
                .list_tools()
                .iter()
                .filter(|schema| schema.function.name == "SubAgent")
                .count(),
            1
        );
    }

    #[test]
    fn alias_classification_uses_the_selected_overlay_identity() {
        let overlay = OverlayToolExecutor::new(
            std::sync::Arc::new(BaseExecutor),
            std::sync::Arc::new(SubAgentOverlayTool),
        );

        assert_eq!(
            overlay.call_parallel_classification(&make_call("sub_task")),
            (bamboo_agent_core::ToolMutability::ReadOnly, true)
        );
    }

    struct ResolvedPermissionBase {
        seen: std::sync::Arc<std::sync::Mutex<Option<(String, serde_json::Value)>>>,
    }

    #[async_trait]
    impl ToolExecutor for ResolvedPermissionBase {
        async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
            Err(ToolError::NotFound(call.function.name.clone()))
        }

        async fn check_permissions_for(
            &self,
            _call: &ToolCall,
            _ctx: &ToolExecutionContext<'_>,
        ) -> Result<Option<ToolOutcome>, ToolError> {
            panic!("overlay must use the resolved permission seam")
        }

        async fn check_permissions_for_resolved(
            &self,
            _call: &ToolCall,
            execution_name: &str,
            args: &serde_json::Value,
            _ctx: &ToolExecutionContext<'_>,
        ) -> Result<Option<ToolOutcome>, ToolError> {
            *self.seen.lock().unwrap() = Some((execution_name.to_string(), args.clone()));
            Ok(None)
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            Vec::new()
        }
    }

    #[tokio::test]
    async fn alias_permission_gate_receives_overlay_identity_and_effective_args() {
        let seen = std::sync::Arc::new(std::sync::Mutex::new(None));
        let overlay = OverlayToolExecutor::new(
            std::sync::Arc::new(ResolvedPermissionBase { seen: seen.clone() }),
            std::sync::Arc::new(SubAgentOverlayTool),
        );
        let call = make_call_with_args("sub_task", r#"{"prompt":"inspect"}"#);

        overlay.execute(&call).await.expect("execute overlay alias");

        let recorded = seen.lock().unwrap().clone().expect("permission record");
        assert_eq!(recorded.0, "SubAgent");
        assert_eq!(recorded.1, json!({"prompt": "inspect"}));
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

    // ---- issue #106: overlay reuses pre-parsed args (parse-once, no re-warn) ---

    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex as StdMutex;

    /// An overlay tool that records the exact args `Value` it was invoked with, so
    /// a test can prove WHICH value the overlay passed through: the threaded
    /// pre-parsed value, or a re-parse of the raw string.
    struct ArgsRecordingOverlayTool {
        seen: std::sync::Arc<StdMutex<Option<serde_json::Value>>>,
    }

    #[async_trait]
    impl Tool for ArgsRecordingOverlayTool {
        fn name(&self) -> &str {
            "memory"
        }

        fn description(&self) -> &str {
            "records the args it was invoked with"
        }

        fn parameters_schema(&self) -> serde_json::Value {
            json!({"type":"object","properties":{}})
        }

        async fn invoke(
            &self,
            args: serde_json::Value,
            _ctx: ToolCtx,
        ) -> Result<ToolOutcome, ToolError> {
            *self.seen.lock().unwrap() = Some(args);
            Ok(ToolOutcome::Completed(ToolResult {
                success: true,
                result: "ok".to_string(),
                display_preference: None,
                images: Vec::new(),
            }))
        }
    }

    /// Minimal tracing subscriber that counts WARN-level events on the current
    /// thread — lets a test assert whether the malformed-args fallback `warn!`
    /// fired, without pulling in `tracing-subscriber` as a dev-dependency.
    #[derive(Clone, Default)]
    struct WarnCounter {
        warns: std::sync::Arc<AtomicUsize>,
    }

    impl tracing::Subscriber for WarnCounter {
        fn enabled(&self, _m: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _a: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _s: &tracing::span::Id, _v: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _s: &tracing::span::Id, _f: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            if *event.metadata().level() == tracing::Level::WARN {
                self.warns.fetch_add(1, Ordering::SeqCst);
            }
        }
        fn enter(&self, _s: &tracing::span::Id) {}
        fn exit(&self, _s: &tracing::span::Id) {}
    }

    #[tokio::test]
    async fn overlay_reuses_pre_parsed_args_and_skips_refallback_warn() {
        // The dispatch loop already parsed the args once and threaded them via
        // `ctx.pre_parsed_args`. The overlay must reuse that value rather than
        // re-parse `call.function.arguments` — so even MALFORMED raw args do NOT
        // trigger a second parse (nor its malformed-args fallback `warn!`).
        let seen = std::sync::Arc::new(StdMutex::new(None));
        let overlay = OverlayToolExecutor::new(
            std::sync::Arc::new(BaseExecutor),
            std::sync::Arc::new(ArgsRecordingOverlayTool { seen: seen.clone() }),
        );

        // Distinctive parsed value; raw args are deliberately broken so a re-parse
        // would fall back to `{}` AND warn — neither must happen.
        let pre_parsed = json!({"action": "query", "threaded": true});
        let call = make_call_with_args("memory", "{ this is not valid json");
        let mut ctx = ToolExecutionContext::none(&call.id);
        ctx.pre_parsed_args = Some(&pre_parsed);

        let counter = WarnCounter::default();
        let warns = counter.warns.clone();
        {
            let _guard = tracing::subscriber::set_default(counter);
            overlay
                .execute_with_context(&call, ctx)
                .await
                .expect("overlay call should succeed");
        }

        assert_eq!(
            seen.lock().unwrap().clone(),
            Some(pre_parsed),
            "overlay must invoke with the threaded pre-parsed args, not a re-parse of the raw string"
        );
        assert_eq!(
            warns.load(Ordering::SeqCst),
            0,
            "reusing pre-parsed args must NOT re-emit the malformed-args fallback warning"
        );
    }

    #[tokio::test]
    async fn overlay_without_pre_parsed_reparses_and_warns_on_malformed_args() {
        // Control proving the WarnCounter observes the fallback warn and that the
        // reuse path above is what suppresses it: with NO pre-parsed args, the
        // overlay re-parses the malformed raw string, falls back to `{}`, warns once.
        let seen = std::sync::Arc::new(StdMutex::new(None));
        let overlay = OverlayToolExecutor::new(
            std::sync::Arc::new(BaseExecutor),
            std::sync::Arc::new(ArgsRecordingOverlayTool { seen: seen.clone() }),
        );

        let call = make_call_with_args("memory", "{ this is not valid json");
        let ctx = ToolExecutionContext::none(&call.id); // pre_parsed_args = None

        let counter = WarnCounter::default();
        let warns = counter.warns.clone();
        {
            let _guard = tracing::subscriber::set_default(counter);
            overlay
                .execute_with_context(&call, ctx)
                .await
                .expect("overlay call should succeed");
        }

        assert_eq!(
            seen.lock().unwrap().clone(),
            Some(json!({})),
            "no pre-parsed args → the malformed raw string parses to the empty-object fallback"
        );
        assert_eq!(
            warns.load(Ordering::SeqCst),
            1,
            "the malformed-args fallback must warn exactly once when it actually parses"
        );
    }
}
