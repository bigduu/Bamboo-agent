use async_trait::async_trait;

use crate::agent::core::tools::{
    normalize_tool_name, parse_tool_args, Tool, ToolCall, ToolError, ToolExecutionContext,
    ToolExecutor, ToolResult, ToolSchema,
};

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
        if name == self.overlay.name() {
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

