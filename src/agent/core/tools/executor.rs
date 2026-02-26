//! Tool execution infrastructure
//!
//! This module provides the trait and utilities for executing tool calls,
//! with support for both direct tool execution and composition-based workflows.

use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;

use crate::agent::core::composition::{CompositionExecutor, ExecutionContext, ToolExpr};
use crate::agent::core::tools::{ToolCall, ToolResult, ToolSchema};

use super::result_handler::parse_tool_args;
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
        let args = parse_tool_args(&tool_call.function.arguments)?;
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

    use crate::agent::core::tools::{FunctionCall, Tool, ToolRegistry};

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
