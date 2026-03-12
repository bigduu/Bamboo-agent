use async_trait::async_trait;

use crate::agent::core::tools::{
    normalize_tool_name, parse_tool_args, Tool, ToolCall, ToolError, ToolExecutionContext,
    ToolExecutor, ToolResult, ToolSchema,
};
use crate::agent::tools::normalize_tool_ref;

/// Tool executor that overlays a single tool on top of an existing executor.
///
/// This is used to add server-only tools (like `spawn_session`) without mutating the
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
            let args = parse_tool_args(&call.function.arguments)?;
            return self.overlay.execute_with_context(args, ctx).await;
        }
        self.base.execute_with_context(call, ctx).await
    }

    fn list_tools(&self) -> Vec<ToolSchema> {
        let mut tools = self.base.list_tools();

        // Ensure overlay tool is present exactly once.
        let overlay_schema = self.overlay.to_schema();
        let overlay_name = overlay_schema.function.name.clone();
        tools.retain(|t| t.function.name != overlay_name);
        tools.push(overlay_schema);

        tools.sort_by(|a, b| a.function.name.cmp(&b.function.name));
        tools
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    use crate::agent::core::tools::FunctionCall;

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

    struct TaskOverlayTool;

    #[async_trait]
    impl Tool for TaskOverlayTool {
        fn name(&self) -> &str {
            "Task"
        }

        fn description(&self) -> &str {
            "overlay task"
        }

        fn parameters_schema(&self) -> serde_json::Value {
            json!({"type":"object","properties":{}})
        }

        async fn execute(&self, _args: serde_json::Value) -> Result<ToolResult, ToolError> {
            Ok(ToolResult {
                success: true,
                result: "overlay".to_string(),
                display_preference: None,
            })
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
    async fn overlay_executor_routes_task_alias_to_overlay_tool() {
        let overlay = OverlayToolExecutor::new(
            std::sync::Arc::new(BaseExecutor),
            std::sync::Arc::new(TaskOverlayTool),
        );

        let result = overlay
            .execute(&make_call("sub_task"))
            .await
            .expect("task alias should route to overlay");

        assert!(result.success);
        assert_eq!(result.result, "overlay");
    }

    #[tokio::test]
    async fn overlay_executor_keeps_non_overlay_calls_on_base_executor() {
        let overlay = OverlayToolExecutor::new(
            std::sync::Arc::new(BaseExecutor),
            std::sync::Arc::new(TaskOverlayTool),
        );

        let err = overlay
            .execute(&make_call("Read"))
            .await
            .expect_err("non-overlay call should stay on base executor");

        assert!(
            matches!(err, ToolError::Execution(msg) if msg.contains("base executor called for Read"))
        );
    }
}
