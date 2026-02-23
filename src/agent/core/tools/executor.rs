use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;

use crate::agent::core::composition::{CompositionExecutor, ExecutionContext, ToolExpr};
use crate::agent::core::tools::{ToolCall, ToolResult, ToolSchema};

use super::result_handler::parse_tool_args;

#[derive(Error, Debug, Clone)]
pub enum ToolError {
    #[error("Tool not found: {0}")]
    NotFound(String),

    #[error("Execution failed: {0}")]
    Execution(String),

    #[error("Invalid arguments: {0}")]
    InvalidArguments(String),
}

pub type Result<T> = std::result::Result<T, ToolError>;

#[async_trait]
pub trait ToolExecutor: Send + Sync {
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult>;
    fn list_tools(&self) -> Vec<ToolSchema>;
}

pub async fn execute_tool_call(
    tool_call: &ToolCall,
    tools: &dyn ToolExecutor,
    composition_executor: Option<Arc<CompositionExecutor>>,
) -> Result<ToolResult> {
    if let Some(executor) = composition_executor {
        let args = parse_tool_args(&tool_call.function.arguments)?;
        let expr = ToolExpr::call(tool_call.function.name.clone(), args);
        let mut ctx = ExecutionContext::new();

        match executor.execute(&expr, &mut ctx).await {
            Ok(result) => return Ok(result),
            Err(ToolError::NotFound(_)) => {}
            Err(error) => return Err(error),
        }
    }

    tools.execute(tool_call).await
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
