use std::sync::Arc;

use crate::agent::core::tools::{
    normalize_tool_name, Tool, ToolCall, ToolError, ToolExecutionContext, ToolExecutor, ToolResult,
    ToolSchema,
};
use async_trait::async_trait;
use serde_json::json;

use crate::agent::tools::guide::{context::GuideBuildContext, EnhancedPromptBuilder, ToolGuide};
use crate::agent::tools::permission::{check_permissions, PermissionChecker, PermissionError};
use crate::agent::tools::tools::{
    ApplyPatchTool, AskUserTool, CreateTodoListTool, ExecuteCommandTool, FileExistsTool,
    GetCurrentDirTool, GetFileInfoTool, GitDiffTool, GitStatusTool, GitWriteTool, GlobSearchTool,
    HttpRequestTool, ListDirectoryTool, MemoryNoteTool, ReadFileRangeTool, ReadFileTool,
    SearchInFileTool, SearchInProjectTool, SetWorkspaceTool, SleepTool, TerminalSessionTool,
    ToolRegistry, UpdateTodoItemTool, WriteFileTool,
};
use crate::core::Config;
use tokio::sync::RwLock;

/// List of all built-in tool names.
///
/// This list intentionally includes only tools that are always registered by
/// `BuiltinToolExecutor::new()`. Optional tools (for example integrations that
/// depend on host binaries) should NOT be added here.
pub const BUILTIN_TOOL_NAMES: [&str; 23] = [
    "read_file",
    "write_file",
    "list_directory",
    "file_exists",
    "get_file_info",
    "execute_command",
    "ask_user",
    "get_current_dir",
    "set_workspace",
    "read_file_range",
    "search_in_file",
    "apply_patch",
    "search_in_project",
    "git_status",
    "git_diff",
    "git_write",
    "create_todo_list",
    "update_todo_item",
    "glob_search",
    "http_request",
    "sleep",
    "terminal_session",
    "memory_note",
];

/// Normalizes a tool reference to a standard tool name
///
/// Handles legacy aliases like "run_command" -> "execute_command"
/// Returns None if the tool name is not recognized
pub fn normalize_tool_ref(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    let raw_tool_name = trimmed.split("::").last().unwrap_or(trimmed);
    let tool_name = match raw_tool_name {
        "run_command" => "execute_command",
        _ => raw_tool_name,
    };
    // `claude_code` is an optional integration tool that may be registered at runtime.
    if BUILTIN_TOOL_NAMES.iter().any(|name| name == &tool_name) || tool_name == "claude_code" {
        Some(tool_name.to_string())
    } else {
        None
    }
}

/// Checks if a tool reference is a built-in tool
pub fn is_builtin_tool(value: &str) -> bool {
    normalize_tool_ref(value).is_some()
}

/// Built-in tool executor that uses ToolRegistry for dynamic dispatch
pub struct BuiltinToolExecutor {
    registry: ToolRegistry,
    permission_checker: Option<Arc<dyn PermissionChecker>>,
}

impl BuiltinToolExecutor {
    /// Creates a new executor with all built-in tools registered
    pub fn new() -> Self {
        let registry = ToolRegistry::new();
        Self::register_builtin_tools(&registry, None);
        Self {
            registry,
            permission_checker: None,
        }
    }

    /// Creates a new executor with a permission checker
    pub fn new_with_permissions(permission_checker: Arc<dyn PermissionChecker>) -> Self {
        let registry = ToolRegistry::new();
        Self::register_builtin_tools(&registry, None);
        Self {
            registry,
            permission_checker: Some(permission_checker),
        }
    }

    /// Creates a new executor that can read the shared, hot-reloadable config.
    ///
    /// Use this when running inside the Bamboo server so tools (notably
    /// `http_request`) honor proxy settings from `config.json`.
    pub fn new_with_config(config: Arc<RwLock<Config>>) -> Self {
        let registry = ToolRegistry::new();
        Self::register_builtin_tools(&registry, Some(config));
        Self {
            registry,
            permission_checker: None,
        }
    }

    /// Creates a new executor with both shared config and a permission checker.
    pub fn new_with_config_and_permissions(
        config: Arc<RwLock<Config>>,
        permission_checker: Arc<dyn PermissionChecker>,
    ) -> Self {
        let registry = ToolRegistry::new();
        Self::register_builtin_tools(&registry, Some(config));
        Self {
            registry,
            permission_checker: Some(permission_checker),
        }
    }

    /// Creates a new executor from an existing registry
    pub fn with_registry(registry: ToolRegistry) -> Self {
        Self {
            registry,
            permission_checker: None,
        }
    }

    /// Returns a reference to the internal registry
    pub fn registry(&self) -> &ToolRegistry {
        &self.registry
    }

    /// Registers all built-in tools to the given registry
    fn register_builtin_tools(registry: &ToolRegistry, config: Option<Arc<RwLock<Config>>>) {
        // Register filesystem tools
        let _ = registry.register(ReadFileTool::new());
        let _ = registry.register(WriteFileTool::new());
        let _ = registry.register(ListDirectoryTool::new());
        let _ = registry.register(FileExistsTool::new());
        let _ = registry.register(GetFileInfoTool::new());

        // Register command tools
        let _ = registry.register(ExecuteCommandTool::new());
        let _ = registry.register(AskUserTool::new());
        let _ = registry.register(GetCurrentDirTool::new());

        // Register workspace tools
        let _ = registry.register(SetWorkspaceTool::new());

        // Register advanced file tools
        let _ = registry.register(ReadFileRangeTool::new());
        let _ = registry.register(SearchInFileTool::new());
        let _ = registry.register(ApplyPatchTool::new());

        // Register project-wide tools
        let _ = registry.register(SearchInProjectTool::new());

        // Register git tools
        let _ = registry.register(GitStatusTool::new());
        let _ = registry.register(GitDiffTool::new());
        let _ = registry.register(GitWriteTool::new());

        // Register todo list tools
        let _ = registry.register(CreateTodoListTool::new());
        let _ = registry.register(UpdateTodoItemTool::new());

        // Register new utility tools
        let _ = registry.register(GlobSearchTool::new());
        let _ = match config {
            Some(config) => registry.register(HttpRequestTool::new_with_config(config)),
            None => registry.register(HttpRequestTool::new()),
        };
        let _ = registry.register(SleepTool::new());
        let _ = registry.register(TerminalSessionTool::new());

        // Persistent external memory note (per session).
        let _ = registry.register(MemoryNoteTool::new());
    }

    /// Returns all built-in tool schemas
    pub fn tool_schemas() -> Vec<ToolSchema> {
        let registry = ToolRegistry::new();
        Self::register_builtin_tools(&registry, None);
        registry.list_tools()
    }

    /// Registers a custom tool to this executor
    pub fn register_tool<T: Tool + 'static>(&self, tool: T) -> Result<(), ToolError> {
        self.registry
            .register(tool)
            .map_err(|e| ToolError::Execution(e.to_string()))
    }

    /// Register a tool with its guide
    pub fn register_tool_with_guide<T, G>(&self, tool: T, guide: G) -> Result<(), ToolError>
    where
        T: Tool + 'static,
        G: ToolGuide + 'static,
    {
        self.registry
            .register_with_guide(tool, guide)
            .map_err(|e| ToolError::Execution(e.to_string()))
    }

    /// Get guide for a tool
    pub fn get_guide(&self, tool_name: &str) -> Option<Arc<dyn ToolGuide>> {
        self.registry.get_guide(tool_name)
    }

    /// Build enhanced prompt for all registered tools
    pub fn build_enhanced_prompt(&self, context: GuideBuildContext) -> String {
        EnhancedPromptBuilder::build(Some(&self.registry), &self.registry.list_tools(), &context)
    }
}

fn permission_error_to_tool_error(error: PermissionError) -> ToolError {
    match error {
        PermissionError::CheckFailed(_) => ToolError::InvalidArguments(error.to_string()),
        _ => ToolError::Execution(error.to_string()),
    }
}

impl Default for BuiltinToolExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ToolExecutor for BuiltinToolExecutor {
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
        self.execute_with_context(call, ToolExecutionContext::none(&call.id))
            .await
    }

    async fn execute_with_context(
        &self,
        call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let args_raw = call.function.arguments.trim();
        let args: serde_json::Value = if args_raw.is_empty() {
            json!({})
        } else {
            serde_json::from_str(args_raw).map_err(|e| {
                ToolError::InvalidArguments(format!("Invalid JSON arguments: {}", e))
            })?
        };

        let tool_name = normalize_tool_name(&call.function.name);

        // Look up the tool in the registry
        let tool = self
            .registry
            .get(tool_name)
            .ok_or_else(|| ToolError::NotFound(format!("Tool '{}' not found", tool_name)))?;

        if let Some(permission_checker) = &self.permission_checker {
            if let Some(contexts) =
                check_permissions(tool_name, &args).map_err(permission_error_to_tool_error)?
            {
                for context in contexts {
                    let resource = context.resource.clone();
                    let allowed = permission_checker
                        .check_or_request(context)
                        .await
                        .map_err(permission_error_to_tool_error)?;
                    if !allowed {
                        return Err(ToolError::Execution(format!(
                            "Permission denied for: {}",
                            resource
                        )));
                    }
                }
            }
        }

        tool.execute_with_context(args, ctx).await
    }

    fn list_tools(&self) -> Vec<ToolSchema> {
        self.registry.list_tools()
    }
}

/// Builder for constructing a BuiltinToolExecutor with custom tool configurations
pub struct BuiltinToolExecutorBuilder {
    registry: ToolRegistry,
    permission_checker: Option<Arc<dyn PermissionChecker>>,
}

impl BuiltinToolExecutorBuilder {
    /// Creates a new builder with no tools registered
    pub fn new() -> Self {
        Self {
            registry: ToolRegistry::new(),
            permission_checker: None,
        }
    }

    /// Registers all default built-in tools
    pub fn with_default_tools(self) -> Self {
        BuiltinToolExecutor::register_builtin_tools(&self.registry, None);
        self
    }

    /// Registers a specific filesystem tool by name
    pub fn with_filesystem_tool(self, name: &str) -> Result<Self, ToolError> {
        match name {
            "read_file" => self.registry.register(ReadFileTool::new()),
            "write_file" => self.registry.register(WriteFileTool::new()),
            "list_directory" => self.registry.register(ListDirectoryTool::new()),
            "file_exists" => self.registry.register(FileExistsTool::new()),
            "get_file_info" => self.registry.register(GetFileInfoTool::new()),
            _ => return Err(ToolError::NotFound(format!("Unknown tool: {}", name))),
        }
        .map_err(|e| ToolError::Execution(e.to_string()))?;
        Ok(self)
    }

    /// Registers a specific command tool by name
    pub fn with_command_tool(self, name: &str) -> Result<Self, ToolError> {
        match name {
            "execute_command" => self.registry.register(ExecuteCommandTool::new()),
            "get_current_dir" => self.registry.register(GetCurrentDirTool::new()),
            _ => return Err(ToolError::NotFound(format!("Unknown tool: {}", name))),
        }
        .map_err(|e| ToolError::Execution(e.to_string()))?;
        Ok(self)
    }

    /// Registers a custom tool
    pub fn with_tool<T: Tool + 'static>(self, tool: T) -> Result<Self, ToolError> {
        self.registry
            .register(tool)
            .map_err(|e| ToolError::Execution(e.to_string()))?;
        Ok(self)
    }

    /// Sets a permission checker for this executor
    pub fn with_permission_checker(mut self, checker: Arc<dyn PermissionChecker>) -> Self {
        self.permission_checker = Some(checker);
        self
    }

    /// Builds the executor
    pub fn build(self) -> BuiltinToolExecutor {
        BuiltinToolExecutor {
            registry: self.registry,
            permission_checker: self.permission_checker,
        }
    }
}

impl Default for BuiltinToolExecutorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::core::tools::FunctionCall;
    use crate::agent::core::tools::ToolExecutionContext;
    use crate::agent::core::AgentEvent;
    use serde_json::json;
    use std::sync::Arc;
    use tokio::fs;
    use tokio::sync::mpsc;

    use crate::agent::tools::tools::WriteFileTool;

    fn make_tool_call(name: &str, args: serde_json::Value) -> ToolCall {
        ToolCall {
            id: "call_1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: name.to_string(),
                arguments: args.to_string(),
            },
        }
    }

    fn make_executor(
        permission_checker: Option<Arc<dyn PermissionChecker>>,
    ) -> BuiltinToolExecutor {
        let builder = BuiltinToolExecutorBuilder::new()
            .with_tool(WriteFileTool::new())
            .expect("register write_file tool");

        let builder = match permission_checker {
            Some(checker) => builder.with_permission_checker(checker),
            None => builder,
        };

        builder.build()
    }

    #[test]
    fn test_normalize_tool_ref_supports_legacy_run_command_alias() {
        assert_eq!(
            normalize_tool_ref("default::run_command"),
            Some("execute_command".to_string())
        );
    }

    #[test]
    fn test_normalize_tool_ref_rejects_unknown_tool() {
        assert_eq!(normalize_tool_ref("default::search"), None);
    }

    #[test]
    fn test_executor_has_all_builtin_tools() {
        let executor = BuiltinToolExecutor::new();
        let tools = executor.list_tools();

        assert_eq!(tools.len(), BUILTIN_TOOL_NAMES.len());

        let tool_names: Vec<String> = tools.iter().map(|t| t.function.name.clone()).collect();
        for tool_name in BUILTIN_TOOL_NAMES {
            assert!(tool_names.contains(&tool_name.to_string()));
        }
    }

    #[test]
    fn test_executor_builds_enhanced_prompt() {
        let executor = BuiltinToolExecutor::new();
        let prompt = executor.build_enhanced_prompt(GuideBuildContext::default());
        assert!(prompt.contains("## Tool Usage Guidelines"));
        assert!(prompt.contains("**read_file**"));
    }

    #[test]
    fn test_executor_builder_empty() {
        let executor = BuiltinToolExecutorBuilder::new().build();
        assert!(executor.list_tools().is_empty());
    }

    #[test]
    fn test_executor_builder_with_default_tools() {
        let executor = BuiltinToolExecutorBuilder::new()
            .with_default_tools()
            .build();
        assert_eq!(executor.list_tools().len(), BUILTIN_TOOL_NAMES.len());
    }

    #[test]
    fn test_executor_builder_with_specific_tool() {
        let executor = BuiltinToolExecutorBuilder::new()
            .with_filesystem_tool("read_file")
            .unwrap()
            .build();

        let tools = executor.list_tools();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].function.name, "read_file");
    }

    #[tokio::test]
    async fn test_executor_skips_permission_checks_without_checker() {
        let executor = make_executor(None);
        let path = "/tmp/executor_permission_none.txt";
        let _ = fs::remove_file(path).await;

        let call = make_tool_call("write_file", json!({"path": path, "content": "ok"}));
        let result = executor.execute(&call).await.expect("execute tool");

        assert!(result.success);
        let _ = fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn test_executor_with_permission_checker_enforces_checks() {
        let checker = Arc::new(crate::agent::tools::permission::DenyDangerousPermissionChecker);
        let executor = make_executor(Some(checker));
        let path = "/tmp/executor_permission_denied.txt";
        let _ = fs::remove_file(path).await;

        let call = make_tool_call("write_file", json!({"path": path, "content": "nope"}));
        let result = executor.execute(&call).await;

        assert!(matches!(result, Err(ToolError::Execution(_))));
        assert!(fs::metadata(path).await.is_err());
    }

    #[tokio::test]
    async fn tool_can_stream_events_via_execute_with_context() {
        struct StreamingTool;

        #[async_trait]
        impl Tool for StreamingTool {
            fn name(&self) -> &str {
                "streaming_tool"
            }

            fn description(&self) -> &str {
                "streams one token"
            }

            fn parameters_schema(&self) -> serde_json::Value {
                json!({"type":"object","properties":{}})
            }

            async fn execute(&self, _args: serde_json::Value) -> Result<ToolResult, ToolError> {
                Ok(ToolResult {
                    success: true,
                    result: "ok".to_string(),
                    display_preference: None,
                })
            }

            async fn execute_with_context(
                &self,
                args: serde_json::Value,
                ctx: ToolExecutionContext<'_>,
            ) -> Result<ToolResult, ToolError> {
                ctx.emit(AgentEvent::Token {
                    content: "stream".to_string(),
                })
                .await;
                self.execute(args).await
            }
        }

        let executor = BuiltinToolExecutor::new();
        executor
            .register_tool(StreamingTool)
            .expect("register streaming tool");

        let (tx, mut rx) = mpsc::channel(8);
        let call = make_tool_call("streaming_tool", json!({}));

        let result = executor
            .execute_with_context(
                &call,
                ToolExecutionContext {
                    session_id: Some("s1"),
                    tool_call_id: &call.id,
                    event_tx: Some(&tx),
                },
            )
            .await
            .expect("execute tool");

        assert!(result.success);
        assert_eq!(result.result, "ok");

        let ev = rx.recv().await.expect("expected streamed event");
        assert!(
            matches!(ev, AgentEvent::ToolToken { tool_call_id, content } if tool_call_id == "call_1" && content == "stream")
        );
    }
}
