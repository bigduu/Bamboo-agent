use agent_core::tools::{ToolCall, ToolError, ToolExecutor, ToolResult, ToolSchema};
use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, error};

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

    /// Convert MCP result to string representation
    fn format_result_content(content: &[ McpContentItem]) -> String {
        content
            .iter()
            .map(|item| match item {
                McpContentItem::Text { text } => text.clone(),
                McpContentItem::Image { data, mime_type } => {
                    format!("[Image: {} ({} bytes)]", mime_type, data.len())
                }
                McpContentItem::Resource { resource } => {
                    if let Some(text) = &resource.text {
                        format!("[Resource {}]: {}", resource.uri, text)
                    } else {
                        format!("[Resource {}]", resource.uri)
                    }
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

#[async_trait]
impl ToolExecutor for McpToolExecutor {
    async fn execute(&self,
        call: &ToolCall,
    ) -> std::result::Result<ToolResult, ToolError> {
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
        let args: serde_json::Value =
            serde_json::from_str(&call.function.arguments).map_err(|e| {
                ToolError::InvalidArguments(format!("Invalid JSON: {}", e))
            })?;

        // Execute via manager
        match self
            .manager
            .call_tool(&alias.server_id, &alias.original_name, args)
            .await
        {
            Ok(result) => {
                if result.is_error {
                    let error_text = Self::format_result_content(&result.content);
                    Ok(ToolResult {
                        success: false,
                        result: error_text,
                        display_preference: None,
                    })
                } else {
                    let content = Self::format_result_content(&result.content);
                    Ok(ToolResult {
                        success: true,
                        result: content,
                        display_preference: None,
                    })
                }
            }
            Err(McpError::ServerNotFound(id)) => {
                Err(ToolError::NotFound(format!("MCP server '{}' not found", id)))
            }
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
                        function: agent_core::tools::FunctionSchema {
                            name: alias.alias,
                            description: tool.description,
                            parameters: tool.parameters,
                        },
                    })
            })
            .collect()
    }
}

/// Composite tool executor that tries built-in tools first, then MCP
pub struct CompositeToolExecutor {
    builtin: Arc<dyn ToolExecutor>,
    mcp: Arc<dyn ToolExecutor>,
}

impl CompositeToolExecutor {
    pub fn new(
        builtin: Arc<dyn ToolExecutor>,
        mcp: Arc<dyn ToolExecutor>,
    ) -> Self {
        Self { builtin, mcp }
    }
}

#[async_trait]
impl ToolExecutor for CompositeToolExecutor {
    async fn execute(&self,
        call: &ToolCall,
    ) -> std::result::Result<ToolResult, ToolError> {
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

    fn list_tools(&self) -> Vec<ToolSchema> {
        let mut tools = self.builtin.list_tools();
        tools.extend(self.mcp.list_tools());
        tools
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::McpContentItem;
    use agent_core::tools::{FunctionCall, FunctionSchema};
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
        let result = McpToolExecutor::format_result_content(&content);
        assert_eq!(result, "Hello\nWorld");
    }

    #[test]
    fn test_format_result_image() {
        let content = vec![McpContentItem::Image {
            data: "base64imagedata".to_string(),
            mime_type: "image/png".to_string(),
        }];
        let result = McpToolExecutor::format_result_content(&content);
        assert_eq!(result, "[Image: image/png (15 bytes)]");
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
        let result = McpToolExecutor::format_result_content(&content);
        assert_eq!(result, "[Resource file:///test.txt]: File content");
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
        let result = McpToolExecutor::format_result_content(&content);
        assert_eq!(result, "[Resource file:///test.bin]");
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
        let result = McpToolExecutor::format_result_content(&content);
        assert!(result.contains("Result:"));
        assert!(result.contains("[Image:"));
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
            })
        });

        mock_builtin.expect_list_tools().returning(|| vec![]);
        mock_mcp.expect_list_tools().returning(|| vec![]);

        let composite = CompositeToolExecutor::new(
            Arc::new(mock_builtin),
            Arc::new(mock_mcp),
        );

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

        let composite = CompositeToolExecutor::new(
            Arc::new(mock_builtin),
            Arc::new(mock_mcp),
        );

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
        mock_builtin.expect_execute().returning(|_| {
            Err(ToolError::Execution("Built-in error".to_string()))
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

        let composite = CompositeToolExecutor::new(
            Arc::new(mock_builtin),
            Arc::new(mock_mcp),
        );

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

        let composite = CompositeToolExecutor::new(
            Arc::new(mock_builtin),
            Arc::new(mock_mcp),
        );

        let tools = composite.list_tools();
        assert_eq!(tools.len(), 2);
        assert_eq!(tools[0].function.name, "builtin_tool");
        assert_eq!(tools[1].function.name, "mcp_tool");
    }
}
