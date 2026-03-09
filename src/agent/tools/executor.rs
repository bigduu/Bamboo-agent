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
    AskUserTool, BashOutputTool, BashTool, EditTool, ExitPlanModeTool, FileExistsTool,
    GetCurrentDirTool, GetFileInfoTool, GlobTool, GrepTool, KillShellTool, MemoryNoteTool,
    NotebookEditTool, ReadTool, SetWorkspaceTool, SlashCommandTool, SleepTool, TaskTool,
    TodoWriteTool, ToolRegistry, WebFetchTool, WebSearchTool, WriteTool,
};
use crate::core::Config;
use tokio::sync::RwLock;

/// List of all built-in tool names.
///
/// This list intentionally includes only tools that are always registered by
/// `BuiltinToolExecutor::new()`. Optional tools (for example integrations that
/// depend on host binaries) should NOT be added here.
pub const BUILTIN_TOOL_NAMES: [&str; 22] = [
    "ask_user",
    "Bash",
    "BashOutput",
    "Edit",
    "ExitPlanMode",
    "FileExists",
    "Glob",
    "GetCurrentDir",
    "GetFileInfo",
    "Grep",
    "KillShell",
    "memory_note",
    "NotebookEdit",
    "Read",
    "SetWorkspace",
    "Sleep",
    "SlashCommand",
    "Task",
    "TodoWrite",
    "WebFetch",
    "WebSearch",
    "Write",
];

/// Normalizes a tool reference to a standard tool name
///
/// Returns None if the tool name is not recognized.
/// Returns None if the tool name is not recognized
pub fn normalize_tool_ref(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    let raw_tool_name = trimmed.split("::").last().unwrap_or(trimmed);
    let normalized = normalize_builtin_alias(raw_tool_name);
    if BUILTIN_TOOL_NAMES.iter().any(|name| name == &normalized) {
        Some(normalized.to_string())
    } else {
        None
    }
}

fn normalize_builtin_alias(name: &str) -> &str {
    match name {
        // Backward compatibility for earlier camelCase variant names.
        "fileExists" => "FileExists",
        "getCurrentDir" => "GetCurrentDir",
        "getFileInfo" => "GetFileInfo",
        "setWorkspace" => "SetWorkspace",
        "sleep" => "Sleep",
        _ => name,
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
        let _ = config;
        let _ = registry.register(AskUserTool::new());
        let _ = registry.register(BashTool::new());
        let _ = registry.register(BashOutputTool::new());
        let _ = registry.register(EditTool::new());
        let _ = registry.register(ExitPlanModeTool::new());
        let _ = registry.register(FileExistsTool::new());
        let _ = registry.register(GlobTool::new());
        let _ = registry.register(GetCurrentDirTool::new());
        let _ = registry.register(GetFileInfoTool::new());
        let _ = registry.register(GrepTool::new());
        let _ = registry.register(KillShellTool::new());
        let _ = registry.register(MemoryNoteTool::new());
        let _ = registry.register(NotebookEditTool::new());
        let _ = registry.register(ReadTool::new());
        let _ = registry.register(SetWorkspaceTool::new());
        let _ = registry.register(SlashCommandTool::new());
        let _ = registry.register(SleepTool::new());
        let _ = registry.register(TaskTool::new());
        let _ = registry.register(TodoWriteTool::new());
        let _ = registry.register(WebFetchTool::new());
        let _ = registry.register(WebSearchTool::new());
        let _ = registry.register(WriteTool::new());
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

        let tool_name = normalize_builtin_alias(normalize_tool_name(&call.function.name));

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
            "Read" => self.registry.register(ReadTool::new()),
            "Write" => self.registry.register(WriteTool::new()),
            "Edit" => self.registry.register(EditTool::new()),
            "NotebookEdit" => self.registry.register(NotebookEditTool::new()),
            _ => return Err(ToolError::NotFound(format!("Unknown tool: {}", name))),
        }
        .map_err(|e| ToolError::Execution(e.to_string()))?;
        Ok(self)
    }

    /// Registers a specific command tool by name
    pub fn with_command_tool(self, name: &str) -> Result<Self, ToolError> {
        match name {
            "Bash" => self.registry.register(BashTool::new()),
            "BashOutput" => self.registry.register(BashOutputTool::new()),
            "KillShell" => self.registry.register(KillShellTool::new()),
            "Task" => self.registry.register(TaskTool::new()),
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

    use crate::agent::tools::tools::WriteTool;

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
            .with_tool(WriteTool::new())
            .expect("register Write tool");

        let builder = match permission_checker {
            Some(checker) => builder.with_permission_checker(checker),
            None => builder,
        };

        builder.build()
    }

    #[test]
    fn test_normalize_tool_ref_accepts_claude_style_names() {
        assert_eq!(
            normalize_tool_ref("default::Bash"),
            Some("Bash".to_string())
        );
    }

    #[test]
    fn test_normalize_tool_ref_accepts_legacy_camel_aliases() {
        assert_eq!(
            normalize_tool_ref("default::fileExists"),
            Some("FileExists".to_string())
        );
        assert_eq!(
            normalize_tool_ref("default::getCurrentDir"),
            Some("GetCurrentDir".to_string())
        );
        assert_eq!(
            normalize_tool_ref("default::getFileInfo"),
            Some("GetFileInfo".to_string())
        );
        assert_eq!(
            normalize_tool_ref("default::setWorkspace"),
            Some("SetWorkspace".to_string())
        );
        assert_eq!(
            normalize_tool_ref("default::sleep"),
            Some("Sleep".to_string())
        );
    }

    #[test]
    fn test_normalize_tool_ref_rejects_unknown_tool() {
        assert_eq!(normalize_tool_ref("default::search"), None);
    }

    #[test]
    fn test_executor_does_not_expose_legacy_tools() {
        let executor = BuiltinToolExecutor::new();
        let tool_names: Vec<String> = executor
            .list_tools()
            .into_iter()
            .map(|schema| schema.function.name)
            .collect();

        for legacy in [
            "claude_code",
            "search_in_file",
            "search_in_project",
            "apply_patch",
        ] {
            assert!(!tool_names.iter().any(|name| name == legacy));
        }
    }

    #[test]
    fn test_critical_tool_schemas_match_claude_shapes() {
        let executor = BuiltinToolExecutor::new();
        let tools = executor.list_tools();

        let get_params = |name: &str| {
            tools
                .iter()
                .find(|tool| tool.function.name == name)
                .unwrap()
                .function
                .parameters
                .clone()
        };

        let grep = get_params("Grep");
        assert_eq!(grep["required"], json!(["pattern"]));
        assert_eq!(
            grep["properties"]["output_mode"]["enum"],
            json!(["content", "files_with_matches", "count"])
        );
        assert!(grep["properties"]["-A"].is_object());
        assert!(grep["properties"]["-B"].is_object());
        assert!(grep["properties"]["-C"].is_object());
        assert!(grep["properties"]["-n"].is_object());
        assert!(grep["properties"]["-i"].is_object());

        let edit = get_params("Edit");
        assert_eq!(
            edit["required"],
            json!(["file_path", "old_string", "new_string"])
        );
        assert_eq!(edit["properties"]["replace_all"]["type"], "boolean");

        let bash = get_params("Bash");
        assert_eq!(bash["required"], json!(["command"]));
        assert_eq!(bash["properties"]["run_in_background"]["type"], "boolean");

        let bash_output = get_params("BashOutput");
        assert_eq!(bash_output["required"], json!(["bash_id"]));
        assert_eq!(bash_output["properties"]["filter"]["type"], "string");
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
        assert!(prompt.contains("**Read**"));
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
            .with_filesystem_tool("Read")
            .unwrap()
            .build();

        let tools = executor.list_tools();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].function.name, "Read");
    }

    #[tokio::test]
    async fn test_executor_skips_permission_checks_without_checker() {
        let executor = make_executor(None);
        let path = "/tmp/executor_permission_none.txt";
        let _ = fs::remove_file(path).await;

        let call = make_tool_call("Write", json!({"file_path": path, "content": "ok"}));
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

        let call = make_tool_call("Write", json!({"file_path": path, "content": "nope"}));
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

    #[tokio::test]
    async fn removed_legacy_tools_return_not_found() {
        let executor = BuiltinToolExecutor::new();

        for legacy in [
            "claude_code",
            "search_in_file",
            "search_in_project",
            "apply_patch",
        ] {
            let call = make_tool_call(legacy, json!({}));
            let result = executor.execute(&call).await;
            assert!(matches!(result, Err(ToolError::NotFound(_))));
        }
    }
}
