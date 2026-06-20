//! Tool execution infrastructure
//!
//! This module provides the trait and utilities for executing tool calls,
//! with support for both direct tool execution and composition-based workflows.

use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;

use crate::composition::{CompositionExecutor, ExecutionContext, ToolExpr};
use crate::tools::{ToolCall, ToolResult, ToolSchema};

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

    /// Lists all available tools and their schemas
    ///
    /// Returns schemas for all tools that can be executed via this executor
    fn list_tools(&self) -> Vec<ToolSchema>;

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
        let args_raw = tool_call.function.arguments.trim();
        let (args, parse_warning) = parse_tool_args_best_effort(&tool_call.function.arguments);
        if let Some(warning) = parse_warning {
            tracing::warn!(
                "Composition executor tool args fallback applied: tool_call_id={}, tool_name={}, args_len={}, warning={}",
                tool_call.id,
                tool_call.function.name,
                args_raw.len(),
                warning
            );
        }
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use async_trait::async_trait;
    use serde_json::json;

    use crate::tools::{FunctionCall, Tool, ToolRegistry};

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

        async fn execute(
            &self,
            _args: serde_json::Value,
        ) -> std::result::Result<ToolResult, ToolError> {
            Ok(ToolResult {
                success: true,
                result: "from-composition".to_string(),
                display_preference: None,
                images: Vec::new(),
            })
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
}
