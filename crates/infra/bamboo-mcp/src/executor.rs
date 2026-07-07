use async_trait::async_trait;
use bamboo_agent_core::{
    parse_tool_args_best_effort, ToolCall, ToolError, ToolExecutionContext, ToolExecutor,
    ToolOutcome, ToolResult, ToolResultImage, ToolSchema,
};
use std::sync::Arc;
use tracing::{debug, error, warn};

use crate::error::McpError;
use crate::manager::McpServerManager;
use crate::tool_index::ToolIndex;
use crate::types::McpContentItem;

/// MCP tool executor that delegates to the MCP server manager
pub struct McpToolExecutor {
    manager: Arc<McpServerManager>,
    index: Arc<ToolIndex>,
}

impl McpToolExecutor {
    pub fn new(manager: Arc<McpServerManager>, index: Arc<ToolIndex>) -> Self {
        Self { manager, index }
    }

    fn preview_for_log(value: &str, max_chars: usize) -> String {
        let mut iter = value.chars();
        let mut preview = String::new();
        for _ in 0..max_chars {
            match iter.next() {
                Some(ch) => preview.push(ch),
                None => break,
            }
        }
        if iter.next().is_some() {
            preview.push_str("...");
        }
        preview.replace('\n', "\\n").replace('\r', "\\r")
    }

    /// Convert MCP result content into a text string plus any returned images.
    ///
    /// Text and resource items are joined into the result string as before;
    /// image items are collected separately (with a short textual marker left in
    /// the string) so they can be forwarded to vision-capable models instead of
    /// being flattened to a `[Image: …]` placeholder.
    fn format_result_content(content: &[McpContentItem]) -> (String, Vec<ToolResultImage>) {
        let mut parts = Vec::new();
        let mut images = Vec::new();
        for item in content {
            match item {
                McpContentItem::Text { text } => parts.push(text.clone()),
                McpContentItem::Image { data, mime_type } => {
                    images.push(ToolResultImage {
                        mime_type: mime_type.clone(),
                        data: data.clone(),
                    });
                    parts.push(format!("[image returned: {mime_type}]"));
                }
                McpContentItem::Resource { resource } => {
                    if let Some(text) = &resource.text {
                        parts.push(format!("[Resource {}]: {}", resource.uri, text));
                    } else {
                        parts.push(format!("[Resource {}]", resource.uri));
                    }
                }
            }
        }
        (parts.join("\n"), images)
    }
}

#[async_trait]
impl ToolExecutor for McpToolExecutor {
    async fn execute(&self, call: &ToolCall) -> std::result::Result<ToolResult, ToolError> {
        let tool_name = &call.function.name;

        // Lookup the tool alias
        let alias = match self.index.lookup(tool_name) {
            Some(alias) => alias,
            None => {
                return Err(ToolError::NotFound(format!(
                    "MCP tool '{}' not found",
                    tool_name
                )));
            }
        };

        debug!(
            "Executing MCP tool: {} (server: {}, original: {})",
            tool_name, alias.server_id, alias.original_name
        );

        // Parse arguments
        let args_raw = call.function.arguments.trim();
        let (args, parse_warning) = parse_tool_args_best_effort(&call.function.arguments);
        if let Some(warning) = parse_warning {
            warn!(
                "MCP tool argument parsing fallback applied: tool_call_id={}, tool_name={}, server_id={}, args_len={}, args_preview=\"{}\", warning={}",
                call.id,
                tool_name,
                alias.server_id,
                args_raw.len(),
                Self::preview_for_log(args_raw, 180),
                warning
            );
        }

        // Execute via manager
        match self
            .manager
            .call_tool(&alias.server_id, &alias.original_name, args)
            .await
        {
            Ok(result) => {
                let (text, images) = Self::format_result_content(&result.content);
                if result.is_error {
                    // Errors are textual; don't forward images.
                    Ok(ToolResult {
                        success: false,
                        result: text,
                        display_preference: None,
                        images: Vec::new(),
                    })
                } else {
                    Ok(ToolResult {
                        success: true,
                        result: text,
                        display_preference: None,
                        images,
                    })
                }
            }
            Err(McpError::ServerNotFound(id)) => Err(ToolError::NotFound(format!(
                "MCP server '{}' not found",
                id
            ))),
            Err(McpError::ToolNotFound(name)) => {
                Err(ToolError::NotFound(format!("Tool '{}' not found", name)))
            }
            Err(e) => {
                error!("MCP tool execution failed: {}", e);
                Err(ToolError::Execution(format!("MCP error: {}", e)))
            }
        }
    }

    fn list_tools(&self) -> Vec<ToolSchema> {
        self.index
            .all_aliases()
            .into_iter()
            .filter_map(|alias| {
                // Get tool info from manager
                self.manager
                    .get_tool_info(&alias.server_id, &alias.original_name)
                    .map(|tool| ToolSchema {
                        schema_type: "function".to_string(),
                        function: bamboo_agent_core::FunctionSchema {
                            name: alias.alias,
                            description: tool.description,
                            parameters: tool.parameters,
                        },
                    })
            })
            .collect()
    }

    /// Each connected MCP server's `instructions`, rendered as a labeled block.
    /// Only ready servers contribute, so this guidance is automatically scoped to
    /// whatever is loaded for the run.
    fn tool_guidance(&self) -> Option<String> {
        let servers = self.manager.connected_server_instructions();
        if servers.is_empty() {
            return None;
        }
        let blocks: Vec<String> = servers
            .into_iter()
            .map(|(server_id, instructions)| {
                format!("### MCP server `{server_id}`\n\n{instructions}")
            })
            .collect();
        Some(format!(
            "## Connected MCP server instructions\n\n{}",
            blocks.join("\n\n")
        ))
    }
}

/// Composite tool executor that tries built-in tools first, then MCP
pub struct CompositeToolExecutor {
    builtin: Arc<dyn ToolExecutor>,
    mcp: Arc<dyn ToolExecutor>,
}

impl CompositeToolExecutor {
    pub fn new(builtin: Arc<dyn ToolExecutor>, mcp: Arc<dyn ToolExecutor>) -> Self {
        Self { builtin, mcp }
    }
}

#[async_trait]
impl ToolExecutor for CompositeToolExecutor {
    async fn execute(&self, call: &ToolCall) -> std::result::Result<ToolResult, ToolError> {
        // Try built-in first
        match self.builtin.execute(call).await {
            Ok(result) => return Ok(result),
            Err(ToolError::NotFound(_)) => {
                // Fall through to MCP
            }
            Err(e) => return Err(e),
        }

        // Try MCP
        self.mcp.execute(call).await
    }

    async fn execute_with_context(
        &self,
        call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> std::result::Result<ToolResult, ToolError> {
        // Try built-in first (preserve context for streaming tools).
        match self.builtin.execute_with_context(call, ctx).await {
            Ok(result) => return Ok(result),
            Err(ToolError::NotFound(_)) => {
                // Fall through to MCP
            }
            Err(e) => return Err(e),
        }

        // Try MCP (context ignored by default).
        self.mcp.execute_with_context(call, ctx).await
    }

    /// Outcome-aware dispatch. MUST be overridden here (not left to the trait
    /// default) so a builtin tool's `ToolOutcome::NeedsHuman`/`Running` survives to
    /// the engine's outcome-aware loop. The default would call
    /// `execute_with_context(...).map(Completed)`, and this composite's
    /// `execute_with_context` collapses the builtin outcome via `into_tool_result`
    /// (dropping the `PendingQuestion`) — so an interactive tool like
    /// `conclusion_with_options` would never suspend on the live overlay→composite
    /// stack. Mirror `execute_with_context`: try built-in first, fall through to MCP
    /// on `NotFound`.
    async fn execute_with_context_outcome(
        &self,
        call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> std::result::Result<ToolOutcome, ToolError> {
        match self.builtin.execute_with_context_outcome(call, ctx).await {
            Ok(outcome) => return Ok(outcome),
            Err(ToolError::NotFound(_)) => {
                // Fall through to MCP.
            }
            Err(e) => return Err(e),
        }

        self.mcp.execute_with_context_outcome(call, ctx).await
    }

    /// Delegate the permission gate to the built-in executor, which is where the
    /// real `PermissionChecker` lives (issue #341). This MUST be overridden (not
    /// left to the trait default) because in the live server the overlay chain
    /// (`memory`, `SubAgent`, `scheduler`, …) is stacked ON TOP OF this composite:
    /// each overlay calls `base.check_permissions_for` before invoking its tool,
    /// and that chain bottoms out here. Falling back to the default `Ok(None)`
    /// would silently skip the gate for every overlay tool in production. MCP
    /// tools carry no gate, and the built-in check returns `Ok(None)` for any
    /// name it doesn't classify, so delegating solely to `builtin` is correct for
    /// both surfaces.
    async fn check_permissions_for(
        &self,
        call: &ToolCall,
        ctx: &ToolExecutionContext<'_>,
    ) -> std::result::Result<Option<ToolOutcome>, ToolError> {
        self.builtin.check_permissions_for(call, ctx).await
    }

    fn list_tools(&self) -> Vec<ToolSchema> {
        let mut tools = self.builtin.list_tools();
        tools.extend(self.mcp.list_tools());
        tools
    }

    fn tool_guidance(&self) -> Option<String> {
        let parts: Vec<String> = [self.builtin.tool_guidance(), self.mcp.tool_guidance()]
            .into_iter()
            .flatten()
            .collect();
        if parts.is_empty() {
            None
        } else {
            Some(parts.join("\n\n"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::McpContentItem;
    use bamboo_agent_core::{FunctionCall, FunctionSchema};
    use mockall::mock;
    use mockall::predicate::*;

    // Mock McpTransport for testing
    mock! {
        pub ToolExecutor {}

        #[async_trait]
        impl ToolExecutor for ToolExecutor {
            async fn execute(&self, call: &ToolCall) -> std::result::Result<ToolResult, ToolError>;
            fn list_tools(&self) -> Vec<ToolSchema>;
        }
    }

    /// Minimal stub executor with a fixed `tool_guidance`, for composite tests.
    struct GuidanceStub(Option<&'static str>);

    #[async_trait]
    impl ToolExecutor for GuidanceStub {
        async fn execute(&self, _call: &ToolCall) -> std::result::Result<ToolResult, ToolError> {
            Err(ToolError::NotFound("stub".into()))
        }
        fn list_tools(&self) -> Vec<ToolSchema> {
            Vec::new()
        }
        fn tool_guidance(&self) -> Option<String> {
            self.0.map(str::to_string)
        }
    }

    #[test]
    fn composite_tool_guidance_merges_builtin_and_mcp() {
        // Both present → joined with a blank line, builtin first.
        let both = CompositeToolExecutor::new(
            Arc::new(GuidanceStub(Some("BUILTIN"))),
            Arc::new(GuidanceStub(Some("MCP"))),
        );
        assert_eq!(both.tool_guidance().as_deref(), Some("BUILTIN\n\nMCP"));

        // Only MCP present → just that.
        let mcp_only = CompositeToolExecutor::new(
            Arc::new(GuidanceStub(None)),
            Arc::new(GuidanceStub(Some("MCP"))),
        );
        assert_eq!(mcp_only.tool_guidance().as_deref(), Some("MCP"));

        // Neither present → None (no empty section leaks into the prompt).
        let neither =
            CompositeToolExecutor::new(Arc::new(GuidanceStub(None)), Arc::new(GuidanceStub(None)));
        assert!(neither.tool_guidance().is_none());
    }

    #[test]
    fn default_tool_guidance_is_none() {
        // The trait default contributes nothing unless an executor opts in.
        assert!(GuidanceStub(None).tool_guidance().is_none());
    }

    /// A builtin stub that returns an interactive `NeedsHuman` outcome (mirrors
    /// `conclusion_with_options`), used to prove the composite forwards it.
    struct NeedsHumanBuiltin;

    #[async_trait]
    impl ToolExecutor for NeedsHumanBuiltin {
        async fn execute(&self, _call: &ToolCall) -> std::result::Result<ToolResult, ToolError> {
            Err(ToolError::NotFound("stub".into()))
        }
        async fn execute_with_context_outcome(
            &self,
            _call: &ToolCall,
            _ctx: ToolExecutionContext<'_>,
        ) -> std::result::Result<ToolOutcome, ToolError> {
            Ok(ToolOutcome::NeedsHuman {
                question: bamboo_agent_core::PendingQuestion {
                    tool_call_id: "test-id".into(),
                    tool_name: "conclusion_with_options".into(),
                    question: "Pick one".into(),
                    options: vec!["A".into(), "B".into()],
                    allow_custom: false,
                    source: bamboo_agent_core::PendingQuestionSource::default(),
                },
                result: ToolResult {
                    success: true,
                    result: "{}".into(),
                    display_preference: Some("conclusion_with_options".into()),
                    images: Vec::new(),
                },
            })
        }
        fn list_tools(&self) -> Vec<ToolSchema> {
            Vec::new()
        }
    }

    /// Regression (PR #211 review): the composite MUST override
    /// `execute_with_context_outcome` and forward the inner outcome. The trait
    /// default would call `execute_with_context(...).map(Completed)`, and the
    /// composite's `execute_with_context` collapses the builtin outcome via
    /// `into_tool_result` (dropping the `PendingQuestion`) — so an interactive tool
    /// would never suspend on the live overlay→composite→builtin stack.
    #[tokio::test]
    async fn composite_preserves_builtin_needs_human_outcome() {
        let composite =
            CompositeToolExecutor::new(Arc::new(NeedsHumanBuiltin), Arc::new(GuidanceStub(None)));
        let call = create_test_tool_call("conclusion_with_options", "{}");
        let outcome = composite
            .execute_with_context_outcome(&call, ToolExecutionContext::none("test-id"))
            .await
            .expect("outcome ok");
        assert!(
            matches!(outcome, ToolOutcome::NeedsHuman { .. }),
            "composite must preserve the builtin NeedsHuman outcome, got {outcome:?}"
        );
    }

    /// A builtin stub whose permission gate always denies, to prove the composite
    /// delegates `check_permissions_for` to its builtin instead of the trait
    /// default (`Ok(None)`) — the default would silently skip the gate for the
    /// overlay chain stacked over the composite in production (issue #341).
    struct GateDenyingBuiltin;

    #[async_trait]
    impl ToolExecutor for GateDenyingBuiltin {
        async fn execute(&self, _call: &ToolCall) -> std::result::Result<ToolResult, ToolError> {
            Err(ToolError::NotFound("stub".into()))
        }
        async fn check_permissions_for(
            &self,
            _call: &ToolCall,
            _ctx: &ToolExecutionContext<'_>,
        ) -> std::result::Result<Option<ToolOutcome>, ToolError> {
            Err(ToolError::Execution("denied-by-builtin-gate".into()))
        }
        fn list_tools(&self) -> Vec<ToolSchema> {
            Vec::new()
        }
    }

    #[tokio::test]
    async fn composite_delegates_permission_gate_to_builtin() {
        let composite =
            CompositeToolExecutor::new(Arc::new(GateDenyingBuiltin), Arc::new(GuidanceStub(None)));
        let call = create_test_tool_call("memory", r#"{"action":"purge"}"#);
        let ctx = ToolExecutionContext::none("test-id");
        let result = composite.check_permissions_for(&call, &ctx).await;
        assert!(
            matches!(result, Err(ToolError::Execution(ref m)) if m.contains("denied-by-builtin-gate")),
            "composite must delegate the gate to its builtin, got: {result:?}"
        );
    }

    #[test]
    fn format_result_content_collects_images_and_keeps_text() {
        let content = vec![
            McpContentItem::Text {
                text: "screenshot 1280x536".to_string(),
            },
            McpContentItem::Image {
                data: "abc123".to_string(),
                mime_type: "image/jpeg".to_string(),
            },
        ];
        let (text, images) = McpToolExecutor::format_result_content(&content);
        assert!(text.contains("screenshot 1280x536"));
        assert_eq!(images.len(), 1);
        assert_eq!(images[0].mime_type, "image/jpeg");
        assert_eq!(images[0].data, "abc123");
    }

    fn create_test_tool_call(name: &str, args: &str) -> ToolCall {
        ToolCall {
            id: "test-id".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: name.to_string(),
                arguments: args.to_string(),
            },
        }
    }

    #[test]
    fn test_format_result_text() {
        let content = vec![
            McpContentItem::Text {
                text: "Hello".to_string(),
            },
            McpContentItem::Text {
                text: "World".to_string(),
            },
        ];
        let (text, images) = McpToolExecutor::format_result_content(&content);
        assert_eq!(text, "Hello\nWorld");
        assert!(images.is_empty());
    }

    #[test]
    fn test_format_result_image() {
        let content = vec![McpContentItem::Image {
            data: "base64imagedata".to_string(),
            mime_type: "image/png".to_string(),
        }];
        // The image is no longer flattened to a placeholder; it is collected
        // separately, with a short marker left in the text.
        let (text, images) = McpToolExecutor::format_result_content(&content);
        assert_eq!(text, "[image returned: image/png]");
        assert_eq!(images.len(), 1);
        assert_eq!(images[0].mime_type, "image/png");
        assert_eq!(images[0].data, "base64imagedata");
    }

    #[test]
    fn test_format_result_resource_with_text() {
        let content = vec![McpContentItem::Resource {
            resource: crate::types::McpResource {
                uri: "file:///test.txt".to_string(),
                mime_type: Some("text/plain".to_string()),
                text: Some("File content".to_string()),
                blob: None,
            },
        }];
        let (text, _images) = McpToolExecutor::format_result_content(&content);
        assert_eq!(text, "[Resource file:///test.txt]: File content");
    }

    #[test]
    fn test_format_result_resource_without_text() {
        let content = vec![McpContentItem::Resource {
            resource: crate::types::McpResource {
                uri: "file:///test.bin".to_string(),
                mime_type: None,
                text: None,
                blob: Some("base64data".to_string()),
            },
        }];
        let (text, _images) = McpToolExecutor::format_result_content(&content);
        assert_eq!(text, "[Resource file:///test.bin]");
    }

    #[test]
    fn test_format_result_mixed() {
        let content = vec![
            McpContentItem::Text {
                text: "Result:".to_string(),
            },
            McpContentItem::Image {
                data: "img".to_string(),
                mime_type: "image/png".to_string(),
            },
        ];
        let (text, images) = McpToolExecutor::format_result_content(&content);
        assert!(text.contains("Result:"));
        assert!(text.contains("[image returned:"));
        assert_eq!(images.len(), 1);
    }

    #[tokio::test]
    async fn test_composite_executor_fallback() {
        let mut mock_builtin = MockToolExecutor::new();
        let mut mock_mcp = MockToolExecutor::new();

        // Built-in returns NotFound, so it should fall through to MCP
        mock_builtin
            .expect_execute()
            .returning(|_| Err(ToolError::NotFound("not found".to_string())));

        mock_mcp.expect_execute().returning(|_| {
            Ok(ToolResult {
                success: true,
                result: "MCP result".to_string(),
                display_preference: None,
                images: Vec::new(),
            })
        });

        mock_builtin
            .expect_list_tools()
            .returning(std::vec::Vec::new);
        mock_mcp.expect_list_tools().returning(std::vec::Vec::new);

        let composite = CompositeToolExecutor::new(Arc::new(mock_builtin), Arc::new(mock_mcp));

        let call = create_test_tool_call("test_tool", "{}");
        let result = composite.execute(&call).await.unwrap();
        assert!(result.success);
        assert_eq!(result.result, "MCP result");
    }

    #[tokio::test]
    async fn test_composite_executor_builtin_success() {
        let mut mock_builtin = MockToolExecutor::new();
        let mock_mcp = MockToolExecutor::new();

        // Built-in succeeds, MCP should not be called
        mock_builtin.expect_execute().returning(|_| {
            Ok(ToolResult {
                success: true,
                result: "Built-in result".to_string(),
                display_preference: None,
                images: Vec::new(),
            })
        });

        mock_builtin.expect_list_tools().returning(|| {
            vec![ToolSchema {
                schema_type: "function".to_string(),
                function: FunctionSchema {
                    name: "builtin_tool".to_string(),
                    description: "A built-in tool".to_string(),
                    parameters: serde_json::json!({}),
                },
            }]
        });

        let composite = CompositeToolExecutor::new(Arc::new(mock_builtin), Arc::new(mock_mcp));

        let call = create_test_tool_call("test_tool", "{}");
        let result = composite.execute(&call).await.unwrap();
        assert!(result.success);
        assert_eq!(result.result, "Built-in result");
    }

    #[tokio::test]
    async fn test_composite_executor_builtin_error() {
        let mut mock_builtin = MockToolExecutor::new();
        let mock_mcp = MockToolExecutor::new();

        // Built-in returns error (not NotFound), should propagate
        mock_builtin
            .expect_execute()
            .returning(|_| Err(ToolError::Execution("Built-in error".to_string())));

        mock_builtin.expect_list_tools().returning(|| {
            vec![ToolSchema {
                schema_type: "function".to_string(),
                function: FunctionSchema {
                    name: "builtin_tool".to_string(),
                    description: "A built-in tool".to_string(),
                    parameters: serde_json::json!({}),
                },
            }]
        });

        let composite = CompositeToolExecutor::new(Arc::new(mock_builtin), Arc::new(mock_mcp));

        let call = create_test_tool_call("test_tool", "{}");
        let result = composite.execute(&call).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            ToolError::Execution(msg) => assert_eq!(msg, "Built-in error"),
            _ => panic!("Expected Execution error"),
        }
    }

    #[test]
    fn test_composite_list_tools() {
        let mut mock_builtin = MockToolExecutor::new();
        let mut mock_mcp = MockToolExecutor::new();

        mock_builtin.expect_list_tools().returning(|| {
            vec![ToolSchema {
                schema_type: "function".to_string(),
                function: FunctionSchema {
                    name: "builtin_tool".to_string(),
                    description: "Built-in tool".to_string(),
                    parameters: serde_json::json!({}),
                },
            }]
        });

        mock_mcp.expect_list_tools().returning(|| {
            vec![ToolSchema {
                schema_type: "function".to_string(),
                function: FunctionSchema {
                    name: "mcp_tool".to_string(),
                    description: "MCP tool".to_string(),
                    parameters: serde_json::json!({}),
                },
            }]
        });

        let composite = CompositeToolExecutor::new(Arc::new(mock_builtin), Arc::new(mock_mcp));

        let tools = composite.list_tools();
        assert_eq!(tools.len(), 2);
        assert_eq!(tools[0].function.name, "builtin_tool");
        assert_eq!(tools[1].function.name, "mcp_tool");
    }
}
