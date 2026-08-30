//! Tool execution infrastructure
//!
//! This module provides the trait and utilities for executing tool calls,
//! with support for both direct tool execution and composition-based workflows.

use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;

use crate::composition::{CompositionExecutor, ExecutionContext, ToolExpr};
use crate::tools::{ToolCall, ToolOutcome, ToolResult, ToolSchema};

use super::result_handler::parse_tool_args_best_effort;
use super::ToolExecutionContext;

/// Errors that can occur during tool execution
#[derive(Error, Debug, Clone)]
pub enum ToolError {
    /// The requested tool was not found in the registry
    #[error("Tool not found: {0}")]
    NotFound(String),

    /// Tool execution failed
    #[error("Execution failed: {0}")]
    Execution(String),

    /// Invalid arguments provided to the tool
    #[error("Invalid arguments: {0}")]
    InvalidArguments(String),
}

/// Convenient result type for tool execution operations
pub type Result<T> = std::result::Result<T, ToolError>;

/// Trait for tool execution backends
///
/// This trait defines the interface for executing tool calls and listing
/// available tools. Implementations can wrap tool registries, provide
/// mock tools for testing, or implement custom execution logic.
///
/// # Example
///
/// ```ignore
/// use bamboo_agent::agent::core::tools::executor::ToolExecutor;
///
/// struct MyExecutor {
///     tools: HashMap<String, Box<dyn Tool>>,
/// }
///
/// #[async_trait]
/// impl ToolExecutor for MyExecutor {
///     async fn execute(&self, call: &ToolCall) -> Result<ToolResult> {
///         let tool = self.tools.get(&call.function.name)
///             .ok_or_else(|| ToolError::NotFound(call.function.name.clone()))?;
///         let args = parse_tool_args(&call.function.arguments)?;
///         tool.execute(args).await
///     }
///
///     fn list_tools(&self) -> Vec<ToolSchema> {
///         self.tools.values().map(|t| t.schema()).collect()
///     }
/// }
/// ```
#[async_trait]
pub trait ToolExecutor: Send + Sync {
    /// Executes a tool call
    ///
    /// # Arguments
    ///
    /// * `call` - The tool call to execute (contains tool name and arguments)
    ///
    /// # Returns
    ///
    /// The tool execution result or an error
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult>;

    /// Executes a tool call with streaming-capable context.
    ///
    /// Default implementation falls back to `execute()` for executors that don't
    /// support streaming (e.g. remote MCP tools).
    async fn execute_with_context(
        &self,
        call: &ToolCall,
        _ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult> {
        self.execute(call).await
    }

    /// Outcome-aware dispatch: returns the tool's [`ToolOutcome`] rather than the
    /// collapsed [`ToolResult`], so the agent loop can branch on
    /// `Completed`/`Running`/`NeedsHuman` directly instead of sniffing markers on
    /// a result. The default collapses via the `execute_with_context` path
    /// (always `Completed`), so executors that never surface `Running`/`NeedsHuman`
    /// (composition, MCP, tests) need no override; the built-in + overlay
    /// executors override this to return the real outcome.
    async fn execute_with_context_outcome(
        &self,
        call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolOutcome> {
        self.execute_with_context(call, ctx)
            .await
            .map(ToolOutcome::Completed)
    }

    /// Execute a call through an already-selected exact owner.
    ///
    /// `execution_name` is the complete, case-sensitive identity returned by
    /// the shared reference resolver. The original `call` is retained so a
    /// concrete executor can apply compatibility argument handling based on the
    /// caller's spelling without re-running ownership selection. The default
    /// rewrites only the function name and delegates to the outcome-aware path.
    async fn execute_exact_with_context_outcome(
        &self,
        call: &ToolCall,
        execution_name: &str,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolOutcome> {
        let mut exact_call = call.clone();
        exact_call.function.name = execution_name.to_string();
        self.execute_with_context_outcome(&exact_call, ctx).await
    }

    /// Permission gate for a tool call, exposed on the executor trait so the
    /// check is part of the executor *chain*.
    ///
    /// An overlay/wrapping executor runs its own tool directly (it does NOT route
    /// that call through the inner executor's `execute` path), so before this
    /// method existed the inner executor's permission check was silently skipped
    /// for those tools (issue #341). By putting the check on the trait, a wrapper
    /// can call the inner executor's real check before invoking its own tool.
    ///
    /// Returns:
    /// - `Ok(None)` — the call is permitted; the caller proceeds to run the tool.
    /// - `Ok(Some(outcome))` — the permission layer intercepts the call and THIS
    ///   [`ToolOutcome`] is the tool call's result *without* the tool running. It
    ///   carries the interactive "awaiting approval" pause the built-in executor
    ///   synthesizes for a human event sink (a `Completed` result tagged
    ///   `display_preference = "request_permissions"` that the engine turns into a
    ///   clarification pause). Returning it here — rather than collapsing it to an
    ///   `Err` — is what preserves the built-in path's exact pause-and-ask
    ///   behavior when the check is routed through this method.
    /// - `Err(_)` — the call is denied / fails closed.
    ///
    /// The default returns `Ok(None)` (no gate), so executors that enforce no
    /// permissions are unaffected. The built-in executor overrides this with the
    /// real check; overlay/wrapping executors delegate to their inner executor's
    /// implementation so the gate can't be dropped by stacking a wrapper.
    async fn check_permissions_for(
        &self,
        _call: &ToolCall,
        _ctx: &ToolExecutionContext<'_>,
    ) -> Result<Option<ToolOutcome>> {
        Ok(None)
    }

    /// Permission gate for a call whose exact execution identity and effective
    /// arguments have already been resolved by a wrapping executor.
    ///
    /// Routing wrappers use this seam after selecting an exact owner so the
    /// permission layer observes the same identity and arguments that the tool
    /// will receive. The default rebuilds both the call and its pre-parsed-args
    /// context before delegating to
    /// [`check_permissions_for`](Self::check_permissions_for), preserving source
    /// compatibility for executors without a specialized permission layer.
    async fn check_permissions_for_resolved(
        &self,
        call: &ToolCall,
        execution_name: &str,
        args: &serde_json::Value,
        ctx: &ToolExecutionContext<'_>,
    ) -> Result<Option<ToolOutcome>> {
        let mut resolved_call = call.clone();
        resolved_call.function.name = execution_name.to_string();
        resolved_call.function.arguments = args.to_string();
        let resolved_ctx = ToolExecutionContext {
            pre_parsed_args: Some(args),
            ..*ctx
        };
        self.check_permissions_for(&resolved_call, &resolved_ctx)
            .await
    }

    /// Lists all available tools and their schemas
    ///
    /// Returns schemas for all tools that can be executed via this executor
    fn list_tools(&self) -> Vec<ToolSchema>;

    /// Returns whether this executor owns `tool_name` as a complete, exact,
    /// case-sensitive execution identity.
    ///
    /// This deliberately performs no namespace stripping or alias resolution.
    /// Wrappers use it to select an owner before applying compatibility
    /// fallbacks. Registry/index-backed production executors should override the
    /// source-compatible default to avoid cloning parameter schemas per query.
    fn owns_exact_tool(&self, tool_name: &str) -> bool {
        self.list_tools()
            .iter()
            .any(|schema| schema.function.name == tool_name)
    }

    /// Server-level usage guidance to surface in the system prompt for whatever
    /// this executor currently exposes — e.g. the `instructions` an MCP server
    /// returns from `initialize`. Because it is derived from the live executor,
    /// the text is naturally scoped to what is actually connected/loaded for the
    /// run (a disconnected server contributes nothing).
    ///
    /// Returns `None` when there is no guidance to add (the default for executors
    /// that have none). The string, when present, is appended to the tool-guide
    /// section of the prompt.
    fn tool_guidance(&self) -> Option<String> {
        None
    }

    /// Returns mutability metadata for a tool name when available.
    /// Executors that can inspect concrete tools should override this.
    fn tool_mutability(&self, tool_name: &str) -> crate::tools::ToolMutability {
        crate::tools::classify_tool(tool_name)
    }

    /// Returns mutability metadata for a specific tool call when available.
    /// Defaults to name-based classification.
    fn call_mutability(&self, call: &ToolCall) -> crate::tools::ToolMutability {
        self.tool_mutability(call.function.name.trim())
    }

    /// Returns whether a tool can safely execute in parallel with other
    /// read-only tools. Executors that can inspect concrete tools should
    /// override this. Fallback keeps current behavior for known read-only tools.
    fn tool_concurrency_safe(&self, tool_name: &str) -> bool {
        self.tool_mutability(tool_name) == crate::tools::ToolMutability::ReadOnly
    }

    /// Returns whether a specific tool call can safely run in parallel.
    /// Defaults to the tool-name level classification.
    fn call_concurrency_safe(&self, call: &ToolCall) -> bool {
        self.tool_concurrency_safe(call.function.name.trim())
    }

    /// Classify a tool call's mutability AND concurrency-safety together,
    /// returning `(mutability, concurrency_safe)`.
    ///
    /// The default delegates to [`call_mutability`](Self::call_mutability) and
    /// [`call_concurrency_safe`](Self::call_concurrency_safe) — i.e. the exact
    /// prior behavior, so every executor that doesn't override this is
    /// unchanged. Concrete executors that parse the call's arguments inside BOTH
    /// of those methods (e.g. `BuiltinToolExecutor`) override this to parse the
    /// arguments a single time while returning the identical pair. The combined
    /// result lets callers that need both (parallel scheduling) avoid a
    /// redundant argument parse per tool call.
    fn call_parallel_classification(
        &self,
        call: &ToolCall,
    ) -> (crate::tools::ToolMutability, bool) {
        let mutability = self.call_mutability(call);
        let concurrency_safe = self.call_concurrency_safe(call);
        (mutability, concurrency_safe)
    }
}

/// Executes a tool call with composition support
///
/// This function provides a unified interface for tool execution that supports
/// both composition-based workflows and direct tool execution.
///
/// # Execution Strategy
///
/// 1. If a `composition_executor` is provided, attempts to execute as a composition
/// 2. If composition execution fails with `NotFound`, falls back to direct execution
/// 3. Other composition errors are propagated immediately
///
/// # Arguments
///
/// * `tool_call` - The tool call to execute
/// * `tools` - Direct tool executor (fallback)
/// * `composition_executor` - Optional composition-based executor
///
/// # Returns
///
/// The tool execution result or an error
///
/// # Example
///
/// ```ignore
/// use bamboo_agent::agent::core::tools::executor::execute_tool_call;
///
/// let result = execute_tool_call(
///     &tool_call,
///     &registry,
///     Some(composition_executor),
/// ).await?;
/// ```
pub async fn execute_tool_call(
    tool_call: &ToolCall,
    tools: &dyn ToolExecutor,
    composition_executor: Option<Arc<CompositionExecutor>>,
) -> Result<ToolResult> {
    execute_tool_call_with_context(
        tool_call,
        tools,
        composition_executor,
        ToolExecutionContext::none(&tool_call.id),
    )
    .await
}

/// Like [`execute_tool_call`], but provides a context to support streaming tools.
pub async fn execute_tool_call_with_context(
    tool_call: &ToolCall,
    tools: &dyn ToolExecutor,
    composition_executor: Option<Arc<CompositionExecutor>>,
    ctx: ToolExecutionContext<'_>,
) -> Result<ToolResult> {
    if let Some(executor) = composition_executor {
        // Reuse the args the dispatching loop already parsed (threaded via the
        // context) instead of re-parsing the raw string here (issue #106). When
        // absent, parse exactly as before — including the fallback warning.
        let args = if let Some(pre_parsed) = ctx.pre_parsed_args {
            pre_parsed.clone()
        } else {
            let args_raw = tool_call.function.arguments.trim();
            let (parsed, parse_warning) =
                parse_tool_args_best_effort(&tool_call.function.arguments);
            if let Some(warning) = parse_warning {
                tracing::warn!(
                    "Composition executor tool args fallback applied: tool_call_id={}, tool_name={}, args_len={}, warning={}",
                    tool_call.id,
                    tool_call.function.name,
                    args_raw.len(),
                    warning
                );
            }
            parsed
        };
        let expr = ToolExpr::call(tool_call.function.name.clone(), args);
        let mut exec_ctx = ExecutionContext::new();

        match executor.execute(&expr, &mut exec_ctx).await {
            Ok(result) => return Ok(result),
            Err(ToolError::NotFound(_)) => {}
            Err(error) => return Err(error),
        }
    }

    tools.execute_with_context(tool_call, ctx).await
}

/// Outcome-aware variant of [`execute_tool_call_with_context`]: the composition
/// path is always `Completed`; the direct path returns the executor's real
/// [`ToolOutcome`] so the loop can branch on `NeedsHuman` / `Running` without
/// sniffing markers on a `ToolResult`.
pub async fn execute_tool_call_with_context_outcome(
    tool_call: &ToolCall,
    tools: &dyn ToolExecutor,
    composition_executor: Option<Arc<CompositionExecutor>>,
    ctx: ToolExecutionContext<'_>,
) -> Result<ToolOutcome> {
    if let Some(executor) = composition_executor {
        let args = if let Some(pre_parsed) = ctx.pre_parsed_args {
            pre_parsed.clone()
        } else {
            parse_tool_args_best_effort(&tool_call.function.arguments).0
        };
        let expr = ToolExpr::call(tool_call.function.name.clone(), args);
        let mut exec_ctx = ExecutionContext::new();
        match executor.execute(&expr, &mut exec_ctx).await {
            Ok(result) => return Ok(ToolOutcome::Completed(result)),
            Err(ToolError::NotFound(_)) => {}
            Err(error) => return Err(error),
        }
    }

    tools.execute_with_context_outcome(tool_call, ctx).await
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use async_trait::async_trait;
    use serde_json::json;

    use crate::tools::{FunctionCall, Tool, ToolCtx, ToolOutcome, ToolRegistry};

    use super::*;

    struct StaticExecutor {
        results: HashMap<String, ToolResult>,
    }

    #[async_trait]
    impl ToolExecutor for StaticExecutor {
        async fn execute(&self, call: &ToolCall) -> Result<ToolResult> {
            self.results
                .get(&call.function.name)
                .cloned()
                .ok_or_else(|| ToolError::NotFound(call.function.name.clone()))
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            Vec::new()
        }
    }

    struct RegistryTool;

    #[async_trait]
    impl Tool for RegistryTool {
        fn name(&self) -> &str {
            "registry_tool"
        }

        fn description(&self) -> &str {
            "registry tool"
        }

        fn parameters_schema(&self) -> serde_json::Value {
            json!({
                "type": "object",
                "properties": {}
            })
        }

        async fn invoke(
            &self,
            _args: serde_json::Value,
            _ctx: ToolCtx,
        ) -> std::result::Result<ToolOutcome, ToolError> {
            Ok(ToolOutcome::Completed(ToolResult {
                success: true,
                result: "from-composition".to_string(),
                display_preference: None,
                images: Vec::new(),
            }))
        }
    }

    fn make_tool_call(name: &str) -> ToolCall {
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
    async fn execute_tool_call_falls_back_when_composition_misses_tool() {
        let mut results = HashMap::new();
        results.insert(
            "fallback_tool".to_string(),
            ToolResult {
                success: true,
                result: "from-fallback".to_string(),
                display_preference: None,
                images: Vec::new(),
            },
        );

        let tools = StaticExecutor { results };
        let composition_executor =
            Arc::new(CompositionExecutor::new(Arc::new(ToolRegistry::new())));
        let tool_call = make_tool_call("fallback_tool");

        let result = execute_tool_call(&tool_call, &tools, Some(composition_executor))
            .await
            .expect("fallback execution should succeed");

        assert_eq!(result.result, "from-fallback");
    }

    #[tokio::test]
    async fn execute_tool_call_uses_composition_when_available() {
        let registry = Arc::new(ToolRegistry::new());
        registry.register(RegistryTool).expect("register tool");

        let tools = StaticExecutor {
            results: HashMap::new(),
        };
        let composition_executor = Arc::new(CompositionExecutor::new(registry));
        let tool_call = make_tool_call("registry_tool");

        let result = execute_tool_call(&tool_call, &tools, Some(composition_executor))
            .await
            .expect("composition execution should succeed");

        assert_eq!(result.result, "from-composition");
    }

    struct PermissionArgsExecutor {
        seen: Arc<std::sync::Mutex<Option<(String, serde_json::Value, serde_json::Value)>>>,
    }

    #[async_trait]
    impl ToolExecutor for PermissionArgsExecutor {
        async fn execute(&self, call: &ToolCall) -> Result<ToolResult> {
            Err(ToolError::NotFound(call.function.name.clone()))
        }

        async fn check_permissions_for(
            &self,
            call: &ToolCall,
            ctx: &ToolExecutionContext<'_>,
        ) -> Result<Option<ToolOutcome>> {
            let call_args = serde_json::from_str(&call.function.arguments).expect("call args JSON");
            let pre_parsed_args = ctx
                .pre_parsed_args
                .cloned()
                .expect("resolved args must be threaded through context");
            *self.seen.lock().unwrap() =
                Some((call.function.name.clone(), call_args, pre_parsed_args));
            Ok(None)
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            Vec::new()
        }
    }

    #[tokio::test]
    async fn default_resolved_permission_seam_replaces_call_and_pre_parsed_args() {
        let seen = Arc::new(std::sync::Mutex::new(None));
        let executor: Arc<dyn ToolExecutor> =
            Arc::new(PermissionArgsExecutor { seen: seen.clone() });
        let mut call = make_tool_call("default::apply_patch");
        call.function.arguments = json!({"path": "original"}).to_string();
        let original_args = json!({"path": "original"});
        let effective_args = json!({"file_path": "effective"});
        let ctx = ToolExecutionContext {
            pre_parsed_args: Some(&original_args),
            ..ToolExecutionContext::none(&call.id)
        };

        executor
            .check_permissions_for_resolved(&call, "Edit", &effective_args, &ctx)
            .await
            .expect("resolved permission check");

        let recorded = seen.lock().unwrap().clone().expect("permission record");
        assert_eq!(recorded.0, "Edit");
        assert_eq!(recorded.1, effective_args);
        assert_eq!(recorded.2, effective_args);
        assert_eq!(call.function.name, "default::apply_patch");
        assert_eq!(call.function.arguments, original_args.to_string());
    }
}
