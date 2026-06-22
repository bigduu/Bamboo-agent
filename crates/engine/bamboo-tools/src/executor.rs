use std::sync::Arc;

use async_trait::async_trait;
use bamboo_agent_core::{
    normalize_tool_name, parse_tool_args_best_effort, Tool, ToolCall, ToolError,
    ToolExecutionContext, ToolExecutor, ToolResult, ToolSchema,
};
use bamboo_domain::tool_names::{normalize_builtin_alias, resolve_alias};

use crate::guide::{context::GuideBuildContext, EnhancedPromptBuilder, ToolGuide};
use crate::permission::{check_permissions, PermissionChecker, PermissionError};
use crate::tools::{
    BashInputTool, BashOutputTool, BashTool, ConclusionWithOptionsTool, EditTool,
    EnterPlanModeTool, ExitPlanModeTool, GetFileInfoTool, GlobTool, GrepTool, JsReplTool,
    KillShellTool, NotebookEditTool, ReadTool, RequestPermissionsTool, SessionNoteTool, SleepTool,
    TaskTool, ToolRegistry, UpdateGoalTool, WebFetchTool, WebSearchTool, WorkspaceTool, WriteTool,
};
use bamboo_llm::Config;
use tokio::sync::RwLock;

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

fn copy_legacy_arg_if_missing(
    args: &mut serde_json::Map<String, serde_json::Value>,
    from: &str,
    to: &str,
) {
    if args.contains_key(to) {
        return;
    }
    if let Some(value) = args.get(from).cloned() {
        args.insert(to.to_string(), value);
    }
}

fn normalize_legacy_builtin_args(
    raw_tool_name: &str,
    args: &mut serde_json::Map<String, serde_json::Value>,
) {
    match raw_tool_name {
        "read_file" | "write_file" | "Read" | "Write" | "apply_patch" => {
            copy_legacy_arg_if_missing(args, "path", "file_path");
        }
        "execute_command" | "Bash" => {
            copy_legacy_arg_if_missing(args, "cmd", "command");
        }
        "list_directory" | "Glob" => {
            let should_default_pattern = raw_tool_name == "list_directory"
                || args.contains_key("path")
                || args.contains_key("recursive");
            if should_default_pattern && !args.contains_key("pattern") {
                let recursive = args
                    .get("recursive")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false);
                let pattern = if recursive { "**/*" } else { "*" };
                args.insert(
                    "pattern".to_string(),
                    serde_json::Value::String(pattern.to_string()),
                );
            }
            args.remove("recursive");
        }
        _ => {}
    }
}

fn resolve_registered_tool_name(registry: &ToolRegistry, raw_tool_name: &str) -> String {
    if registry.get(raw_tool_name).is_some() {
        return raw_tool_name.to_string();
    }

    let aliased = normalize_builtin_alias(raw_tool_name);
    if registry.get(aliased).is_some() {
        return aliased.to_string();
    }

    resolve_alias(aliased).unwrap_or(aliased).to_string()
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
        // NOTE: apply_patch is now an alias for Edit – no separate registration.
        let _ = registry.register(ConclusionWithOptionsTool::new());
        let _ = registry.register(BashTool::new());
        let _ = registry.register(BashInputTool::new());
        let _ = registry.register(BashOutputTool::new());
        let _ = registry.register(EditTool::new());
        let _ = registry.register(EnterPlanModeTool::new());
        let _ = registry.register(ExitPlanModeTool::new());
        // NOTE: FileExists is now an alias for GetFileInfo – no separate registration.
        let _ = registry.register(GetFileInfoTool::new());
        let _ = registry.register(GlobTool::new());
        let _ = registry.register(GrepTool::new());
        let _ = registry.register(UpdateGoalTool::new());
        let _ = registry.register(JsReplTool::new());
        let _ = registry.register(KillShellTool::new());
        let _ = registry.register(SessionNoteTool::new());
        let _ = registry.register(NotebookEditTool::new());
        let _ = registry.register(ReadTool::new());
        let _ = registry.register(RequestPermissionsTool::new());
        let _ = registry.register(SleepTool::new());
        let _ = registry.register(TaskTool::new());
        let _ = registry.register(WebFetchTool::new());
        let _ = registry.register(WebSearchTool::new());
        // NOTE: GetCurrentDir + SetWorkspace are now aliases for Workspace.
        let _ = registry.register(WorkspaceTool::new());
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
        // Reuse the args the dispatching agent loop already parsed (for the
        // `ToolStart` event) when it threaded them through the context, instead
        // of re-parsing the raw JSON string here (issue #106, deferred B1 from
        // #17). The pre-parsed value is the exact output of
        // `parse_tool_args_best_effort` on the same input, and that loop already
        // logged any fallback warning at parse time, so skipping the re-parse is
        // behavior-preserving. When absent (the `execute` entry point, tests, or
        // a loop that parsed with a different/stricter parser), fall back to
        // parsing here exactly as before — including the fallback-warning log.
        let mut args = if let Some(pre_parsed) = ctx.pre_parsed_args {
            pre_parsed.clone()
        } else {
            let args_raw = call.function.arguments.trim();
            let (parsed, parse_warning) = parse_tool_args_best_effort(&call.function.arguments);
            if let Some(warning) = parse_warning {
                tracing::warn!(
                    "Builtin tool argument parsing fallback applied: session_id={:?}, tool_call_id={}, tool_name={}, args_len={}, args_preview=\"{}\", warning={}",
                    ctx.session_id,
                    call.id,
                    call.function.name,
                    args_raw.len(),
                    preview_for_log(args_raw, 180),
                    warning
                );
            }
            parsed
        };

        let raw_tool_name = normalize_tool_name(&call.function.name);
        if let Some(args_obj) = args.as_object_mut() {
            normalize_legacy_builtin_args(raw_tool_name, args_obj);
        }

        let tool_name = resolve_registered_tool_name(&self.registry, raw_tool_name);

        // Look up the tool in the registry
        let tool = self
            .registry
            .get(&tool_name)
            .ok_or_else(|| ToolError::NotFound(format!("Tool '{}' not found", tool_name)))?;

        if let Some(permission_checker) = &self.permission_checker {
            if let Some(contexts) =
                check_permissions(&tool_name, &args).map_err(permission_error_to_tool_error)?
            {
                // "Always ask" rules (configured patterns + built-in dangerous
                // commands) force a confirmation even under bypass. Everything
                // else is skipped when this session is in "bypass permissions"
                // mode (scoped per-session via its runtime state).
                let force_ask = permission_checker.requires_forced_confirmation(&tool_name, &args);
                for context in contexts {
                    if ctx.bypass_permissions && !force_ask {
                        continue;
                    }
                    let resource = context.resource.clone();
                    // Forced confirmations route through `check_or_request_forced`
                    // so the active mode/bypass cannot suppress the prompt.
                    let decision = if force_ask {
                        permission_checker.check_or_request_forced(context).await
                    } else {
                        permission_checker.check_or_request(context).await
                    };
                    match decision {
                        Ok(true) => {}
                        Ok(false) => {
                            return Err(ToolError::Execution(format!(
                                "Permission denied for: {}",
                                resource
                            )));
                        }
                        Err(PermissionError::ConfirmationRequired {
                            permission_type,
                            resource: _,
                        }) => {
                            // Phase 2 (cross-process): a subagent worker installs a
                            // task-local `ApprovalProxy` for the duration of its run.
                            // When present, forward the decision to the worker's host
                            // (parent) and block this tool inline for the reply —
                            // approve proceeds (treated like a granted context), deny
                            // fails closed. Checked BEFORE the interactive sink below
                            // so a worker (which also has an `event_tx`) proxies to its
                            // parent instead of trying to prompt a human itself. The
                            // proxy is unset on every non-worker path, so the behavior
                            // there is unchanged.
                            if let Some(proxy) = crate::approval::current_approval_proxy() {
                                let approved = proxy
                                    .request_approval(crate::approval::ApprovalAsk {
                                        tool_name: tool_name.clone(),
                                        permission: permission_type.description().to_string(),
                                        resource: resource.clone(),
                                    })
                                    .await;
                                if approved {
                                    // Treat as a granted context: check any remaining
                                    // contexts, then fall through to execution.
                                    continue;
                                }
                                return Err(ToolError::Execution(format!(
                                    "Permission denied by host for: {}",
                                    resource
                                )));
                            }

                            // Interactive sessions pause for approval by reusing the
                            // same pending-question pipeline as `request_permissions`:
                            // synthesize an "awaiting_permission_approval" result that
                            // the engine recognizes (via display_preference) and turns
                            // into a NeedClarification pause. On approval the respond
                            // handler records a session grant so the re-attempt passes.
                            if let Some(tx) = ctx.event_tx {
                                // Keep emitting the structured approval event for observers.
                                let _ = tx
                                    .send(bamboo_agent_core::AgentEvent::ToolApprovalRequested {
                                        tool_call_id: call.id.clone(),
                                        tool_name: tool_name.clone(),
                                        parameters: args.clone(),
                                    })
                                    .await;

                                let question = format!(
                                    "**Permission required**\n\nThe `{}` tool needs approval to {} on:\n\n`{}`",
                                    tool_name,
                                    permission_type.description(),
                                    resource
                                );
                                let payload = serde_json::json!({
                                    "status": "awaiting_permission_approval",
                                    "question": question,
                                    "permission_type": permission_type,
                                    "resource": resource,
                                    "options": ["Approve", "Deny"],
                                    "allow_custom": false,
                                });
                                return Ok(ToolResult {
                                    success: true,
                                    result: payload.to_string(),
                                    display_preference: Some("request_permissions".to_string()),
                                    images: Vec::new(),
                                });
                            }

                            // Non-interactive (no event sink to surface the prompt):
                            // fail closed rather than silently proceeding.
                            return Err(ToolError::Execution(format!(
                                "Permission approval required for: {}",
                                resource
                            )));
                        }
                        Err(other) => {
                            return Err(permission_error_to_tool_error(other));
                        }
                    }
                }
            }
        }

        tool.execute_with_context(args, ctx).await
    }

    fn list_tools(&self) -> Vec<ToolSchema> {
        self.registry.list_tools()
    }

    fn tool_mutability(&self, tool_name: &str) -> crate::ToolMutability {
        self.registry
            .get(tool_name)
            .map(|tool| tool.mutability())
            .unwrap_or_else(|| crate::classify_tool(tool_name))
    }

    fn call_mutability(&self, call: &ToolCall) -> crate::ToolMutability {
        let canonical = resolve_registered_tool_name(&self.registry, call.function.name.trim());
        let args = bamboo_agent_core::parse_tool_args_best_effort(&call.function.arguments).0;
        self.registry
            .get(&canonical)
            .map(|tool| tool.call_mutability(&args))
            .unwrap_or_else(|| self.tool_mutability(&canonical))
    }

    fn tool_concurrency_safe(&self, tool_name: &str) -> bool {
        let canonical = resolve_registered_tool_name(&self.registry, tool_name);
        self.registry
            .get(&canonical)
            .map(|tool| tool.concurrency_safe())
            .unwrap_or_else(|| self.tool_mutability(&canonical) == crate::ToolMutability::ReadOnly)
    }

    fn call_concurrency_safe(&self, call: &ToolCall) -> bool {
        let canonical = resolve_registered_tool_name(&self.registry, call.function.name.trim());
        let args = bamboo_agent_core::parse_tool_args_best_effort(&call.function.arguments).0;
        self.registry
            .get(&canonical)
            .map(|tool| tool.call_concurrency_safe(&args))
            .unwrap_or_else(|| self.tool_concurrency_safe(&canonical))
    }

    fn call_parallel_classification(&self, call: &ToolCall) -> (crate::ToolMutability, bool) {
        // `call_mutability` and `call_concurrency_safe` each independently resolve
        // the canonical tool name and parse the call's arguments. Computing both
        // together here lets us do that work a single time, while returning the
        // exact same `(mutability, concurrency_safe)` pair the two methods
        // produce. Equivalent to calling both, but with one (not two) arg parses.
        let canonical = resolve_registered_tool_name(&self.registry, call.function.name.trim());
        let args = bamboo_agent_core::parse_tool_args_best_effort(&call.function.arguments).0;
        match self.registry.get(&canonical) {
            Some(tool) => (
                tool.call_mutability(&args),
                tool.call_concurrency_safe(&args),
            ),
            None => (
                self.tool_mutability(&canonical),
                self.tool_concurrency_safe(&canonical),
            ),
        }
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
            // apply_patch is now an alias for Edit
            "Edit" | "apply_patch" => self.registry.register(EditTool::new()),
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
    use bamboo_agent_core::AgentEvent;
    use bamboo_agent_core::FunctionCall;
    use bamboo_agent_core::ToolExecutionContext;
    use bamboo_domain::tool_names::{normalize_tool_ref, BUILTIN_TOOL_NAMES};
    use serde_json::json;
    use std::sync::Arc;
    use tokio::fs;
    use tokio::sync::mpsc;

    use crate::tools::WriteTool;

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

    fn make_tool_call_with_raw_args(name: &str, raw_args: &str) -> ToolCall {
        ToolCall {
            id: "call_1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: name.to_string(),
                arguments: raw_args.to_string(),
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
    fn test_normalize_tool_ref_accepts_legacy_snake_case_aliases() {
        assert_eq!(
            normalize_tool_ref("default::execute_command"),
            Some("Bash".to_string())
        );
        assert_eq!(
            normalize_tool_ref("default::file_exists"),
            Some("FileExists".to_string())
        );
        assert_eq!(
            normalize_tool_ref("default::get_current_dir"),
            Some("GetCurrentDir".to_string())
        );
        assert_eq!(
            normalize_tool_ref("default::get_file_info"),
            Some("GetFileInfo".to_string())
        );
        assert_eq!(
            normalize_tool_ref("default::list_directory"),
            Some("Glob".to_string())
        );
        assert_eq!(
            normalize_tool_ref("default::memory_note"),
            Some("memory_note".to_string())
        );
        assert_eq!(
            normalize_tool_ref("default::read_file"),
            Some("Read".to_string())
        );
        assert_eq!(
            normalize_tool_ref("default::set_workspace"),
            Some("SetWorkspace".to_string())
        );
        assert_eq!(
            normalize_tool_ref("default::write_file"),
            Some("Write".to_string())
        );
    }

    #[test]
    fn test_normalize_tool_ref_accepts_spawn_task_aliases() {
        for alias in [
            "default::spawn_session",
            "default::sub_session",
            "default::sub_task",
            "default::team_agent",
            "default::child_session",
        ] {
            assert_eq!(normalize_tool_ref(alias), Some("SubAgent".to_string()));
        }
    }

    #[test]
    fn test_normalize_tool_ref_accepts_server_overlay_tools() {
        assert_eq!(normalize_tool_ref("compress_context"), None);
        assert_eq!(
            normalize_tool_ref("default::read_skill_resource"),
            Some("read_skill_resource".to_string())
        );
    }

    #[tokio::test]
    async fn test_executor_accepts_legacy_read_file_path_argument() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("legacy-read.txt");
        fs::write(&file_path, "legacy read content").await.unwrap();

        let executor = BuiltinToolExecutor::new();
        let call = make_tool_call("read_file", json!({"path": file_path}));

        let result = executor.execute(&call).await.unwrap();
        assert!(result.success);
        assert!(result.result.contains("legacy read content"));
    }

    #[tokio::test]
    async fn test_executor_accepts_legacy_list_directory_without_pattern() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("legacy-list.txt");
        fs::write(&file_path, "legacy list content").await.unwrap();

        let executor = BuiltinToolExecutor::new();
        let call = make_tool_call("list_directory", json!({"path": dir.path()}));

        let result = executor.execute(&call).await.unwrap();
        assert!(result.success);
        assert!(result.result.contains("legacy-list.txt"));
    }

    #[tokio::test]
    async fn test_executor_accepts_canonical_read_with_path_argument() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("canonical-read.txt");
        fs::write(&file_path, "canonical read content")
            .await
            .unwrap();

        let executor = BuiltinToolExecutor::new();
        let call = make_tool_call("Read", json!({"path": file_path}));

        let result = executor.execute(&call).await.unwrap();
        assert!(result.success);
        assert!(result.result.contains("canonical read content"));
    }

    #[tokio::test]
    async fn test_executor_accepts_canonical_glob_without_pattern_when_path_present() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("canonical-list.txt");
        fs::write(&file_path, "canonical list content")
            .await
            .unwrap();

        let executor = BuiltinToolExecutor::new();
        let call = make_tool_call("Glob", json!({"path": dir.path()}));

        let result = executor.execute(&call).await.unwrap();
        assert!(result.success);
        assert!(result.result.contains("canonical-list.txt"));
    }

    #[test]
    fn test_executor_workspace_mutability_depends_on_path_argument() {
        let executor = BuiltinToolExecutor::new();
        let get_call = make_tool_call("Workspace", json!({}));
        let set_call = make_tool_call("Workspace", json!({"path": "/tmp"}));

        assert_eq!(
            executor.call_mutability(&get_call),
            crate::ToolMutability::ReadOnly
        );
        assert!(executor.call_concurrency_safe(&get_call));

        assert_eq!(
            executor.call_mutability(&set_call),
            crate::ToolMutability::Mutating
        );
        assert!(!executor.call_concurrency_safe(&set_call));
    }

    #[test]
    fn call_parallel_classification_matches_individual_methods() {
        // Regression guard for the issue #17 perf refactor: the combined
        // `call_parallel_classification` (which parses args once) must return the
        // exact same (mutability, concurrency_safe) pair as calling
        // `call_mutability` and `call_concurrency_safe` separately (which each
        // parse args). Covers a read-only tool, mutating tools, and an
        // args-aware tool (Workspace get vs set) so every branch of the
        // single-parse override is exercised.
        let executor = BuiltinToolExecutor::new();
        let cases: &[(&str, serde_json::Value)] = &[
            ("Read", json!({})),
            ("Grep", json!({"pattern": "x"})),
            (
                "Write",
                json!({"file_path": "/tmp/par_cls.txt", "content": "y"}),
            ),
            ("Bash", json!({"command": "echo hi"})),
            ("Workspace", json!({})),
            ("Workspace", json!({"path": "/tmp"})),
        ];

        for (name, args) in cases {
            let call = make_tool_call(name, args.clone());
            let expected_mutability = executor.call_mutability(&call);
            let expected_concurrency = executor.call_concurrency_safe(&call);
            let (mutability, concurrency) = executor.call_parallel_classification(&call);
            assert_eq!(
                mutability, expected_mutability,
                "mutability mismatch for {name} ({args})"
            );
            assert_eq!(
                concurrency, expected_concurrency,
                "concurrency mismatch for {name} ({args})"
            );
        }
    }

    #[test]
    fn list_tools_snapshot_is_stable_across_calls() {
        // The per-round schema cache (issue #17 Part A) assumes the executor's
        // `list_tools()` is stable within a round: a snapshot taken once must
        // equal a fresh call. Guards that invariant so caching the set for the
        // duration of a round can't serve a stale or filtered view.
        let executor = BuiltinToolExecutor::new();
        let first: Vec<String> = executor
            .list_tools()
            .into_iter()
            .map(|s| s.function.name)
            .collect();
        let second: Vec<String> = executor
            .list_tools()
            .into_iter()
            .map(|s| s.function.name)
            .collect();
        assert!(!first.is_empty(), "builtin executor should expose tools");
        assert_eq!(
            first, second,
            "list_tools() must be deterministic per round"
        );
    }

    #[tokio::test]
    async fn test_executor_recovers_truncated_json_arguments() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("recovered-write.txt");

        // Missing closing brace simulates EOF while parsing an object.
        let malformed_args = format!(
            r#"{{"file_path":"{}","content":"recovered content""#,
            path.display()
        );

        let executor = BuiltinToolExecutor::new();
        let call = make_tool_call_with_raw_args("Write", &malformed_args);

        let result = executor
            .execute(&call)
            .await
            .expect("truncated JSON should be auto-repaired");
        assert!(result.success);

        let written = fs::read_to_string(&path)
            .await
            .expect("file should be written");
        assert_eq!(written, "recovered content");
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

        for legacy in ["claude_code", "search_in_file", "search_in_project"] {
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
        assert_eq!(edit["required"], json!(["file_path"]));
        assert_eq!(edit["properties"]["old_string"]["type"], "string");
        assert_eq!(edit["properties"]["new_string"]["type"], "string");
        assert_eq!(edit["properties"]["patch"]["type"], "string");
        assert_eq!(edit["properties"]["replace_all"]["type"], "boolean");
        assert!(edit.get("oneOf").is_none());

        // apply_patch is now an alias for Edit – its schema is the Edit
        // schema, so we just verify that Edit includes the patch property.
        assert_eq!(edit["properties"]["patch"]["type"], "string");
        assert_eq!(edit["properties"]["line_number"]["type"], "integer");

        let bash = get_params("Bash");
        assert_eq!(bash["required"], json!(["command"]));
        assert_eq!(bash["properties"]["run_in_background"]["type"], "boolean");
        assert_eq!(bash["properties"]["workdir"]["type"], "string");

        let bash_output = get_params("BashOutput");
        assert_eq!(bash_output["required"], json!(["bash_id"]));
        assert_eq!(bash_output["properties"]["filter"]["type"], "string");
    }

    #[test]
    fn test_tool_schemas_avoid_openai_forbidden_top_level_keywords() {
        let executor = BuiltinToolExecutor::new();
        let tools = executor.list_tools();
        let forbidden = ["oneOf", "anyOf", "allOf", "not", "enum"];

        for tool in tools {
            let params = &tool.function.parameters;
            assert_eq!(
                params["type"], "object",
                "tool '{}' parameters must be a top-level object schema",
                tool.function.name
            );
            for key in forbidden {
                assert!(
                    params.get(key).is_none(),
                    "tool '{}' parameters contains forbidden top-level keyword '{}'",
                    tool.function.name,
                    key
                );
            }
        }
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
        let checker = Arc::new(crate::permission::DenyDangerousPermissionChecker);
        let executor = make_executor(Some(checker));
        let path = "/tmp/executor_permission_denied.txt";
        let _ = fs::remove_file(path).await;

        let call = make_tool_call("Write", json!({"file_path": path, "content": "nope"}));
        let result = executor.execute(&call).await;

        assert!(matches!(result, Err(ToolError::Execution(_))));
        assert!(fs::metadata(path).await.is_err());
    }

    #[tokio::test]
    async fn test_bypass_permissions_skips_checker() {
        // Even with a deny-all checker, a context flagged `bypass_permissions`
        // must skip the permission check entirely and let the write through.
        let checker = Arc::new(crate::permission::DenyDangerousPermissionChecker);
        let executor = make_executor(Some(checker));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bypass_allows_write.txt");
        let path_str = path.to_str().unwrap();

        let call = make_tool_call("Write", json!({"file_path": path_str, "content": "ok"}));
        let ctx = ToolExecutionContext {
            session_id: Some("s-bypass"),
            tool_call_id: &call.id,
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: true,
            can_async_resume: false,
            pre_parsed_args: None,
        };
        let result = executor.execute_with_context(&call, ctx).await;

        assert!(result.is_ok(), "bypass should allow the write: {result:?}");
        assert_eq!(fs::read_to_string(&path).await.unwrap(), "ok");
    }

    #[tokio::test]
    async fn test_forced_ask_rule_overrides_bypass() {
        // A configured "always ask" rule must force a confirmation even when the
        // session is in bypass mode. With no event sink the executor fails closed
        // (approval required) rather than silently writing.
        let config = Arc::new(crate::permission::PermissionConfig::new());
        config.set_ask_rules(["Write(/etc/**)".to_string()]);
        let checker = Arc::new(crate::permission::ConfigPermissionChecker::new(config));
        let executor = make_executor(Some(checker));

        let call = make_tool_call(
            "Write",
            json!({"file_path": "/etc/forced.conf", "content": "x"}),
        );
        let ctx = ToolExecutionContext {
            session_id: Some("s-forced"),
            tool_call_id: &call.id,
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: true,
            can_async_resume: false,
            pre_parsed_args: None,
        };
        let result = executor.execute_with_context(&call, ctx).await;

        assert!(
            matches!(result, Err(ToolError::Execution(ref m)) if m.contains("approval required")),
            "forced ask rule should block under bypass: {result:?}"
        );
        assert!(fs::metadata("/etc/forced.conf").await.is_err());
    }

    // ---- Phase 2: cross-process approval proxy ----------------------------

    struct HostStub {
        approve: bool,
    }

    #[async_trait]
    impl crate::approval::ApprovalProxy for HostStub {
        async fn request_approval(&self, _ask: crate::approval::ApprovalAsk) -> bool {
            self.approve
        }
    }

    #[tokio::test]
    async fn approval_proxy_grant_lets_gated_tool_proceed() {
        // A subagent worker installs an ApprovalProxy for its run. A forced-ask
        // rule with NO event sink would otherwise fail closed; with the host
        // proxy granting, the executor treats the context as approved and the
        // tool proceeds inline (no suspend, no synthetic pause).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("approved.txt");
        let path_str = path.to_str().unwrap().to_string();
        let config = Arc::new(crate::permission::PermissionConfig::new());
        config.set_ask_rules([format!("Write({}/**)", dir.path().to_str().unwrap())]);
        let checker = Arc::new(crate::permission::ConfigPermissionChecker::new(config));
        let executor = make_executor(Some(checker));

        let call = make_tool_call("Write", json!({"file_path": path_str, "content": "ok"}));
        let ctx = ToolExecutionContext {
            session_id: Some("s-worker"),
            tool_call_id: &call.id,
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            can_async_resume: false,
            pre_parsed_args: None,
        };

        let proxy: Arc<dyn crate::approval::ApprovalProxy> = Arc::new(HostStub { approve: true });
        let result = crate::approval::with_approval_proxy(
            Some(proxy),
            executor.execute_with_context(&call, ctx),
        )
        .await;

        assert!(
            result.is_ok(),
            "host grant should let the write through: {result:?}"
        );
        assert_eq!(fs::read_to_string(&path).await.unwrap(), "ok");
    }

    #[tokio::test]
    async fn approval_proxy_deny_fails_gated_tool_closed() {
        // With the host proxy denying, the gated tool fails closed and the side
        // effect never happens.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("denied.txt");
        let path_str = path.to_str().unwrap().to_string();
        let config = Arc::new(crate::permission::PermissionConfig::new());
        config.set_ask_rules([format!("Write({}/**)", dir.path().to_str().unwrap())]);
        let checker = Arc::new(crate::permission::ConfigPermissionChecker::new(config));
        let executor = make_executor(Some(checker));

        let call = make_tool_call("Write", json!({"file_path": path_str, "content": "nope"}));
        let ctx = ToolExecutionContext {
            session_id: Some("s-worker"),
            tool_call_id: &call.id,
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            can_async_resume: false,
            pre_parsed_args: None,
        };

        let proxy: Arc<dyn crate::approval::ApprovalProxy> = Arc::new(HostStub { approve: false });
        let result = crate::approval::with_approval_proxy(
            Some(proxy),
            executor.execute_with_context(&call, ctx),
        )
        .await;

        assert!(
            matches!(result, Err(ToolError::Execution(ref m)) if m.contains("denied by host")),
            "host deny should fail the tool closed: {result:?}"
        );
        assert!(fs::metadata(&path).await.is_err());
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
                    images: Vec::new(),
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
                    available_tool_schemas: None,
                    bypass_permissions: false,
                    can_async_resume: false,
                    pre_parsed_args: None,
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

        for legacy in ["claude_code", "search_in_file", "search_in_project"] {
            let call = make_tool_call(legacy, json!({}));
            let result = executor.execute(&call).await;
            assert!(matches!(result, Err(ToolError::NotFound(_))));
        }
    }

    #[tokio::test]
    async fn executor_prefers_exact_tool_name_before_builtin_alias() {
        struct CustomSpawnSessionTool;

        #[async_trait]
        impl Tool for CustomSpawnSessionTool {
            fn name(&self) -> &str {
                "spawn_session"
            }

            fn description(&self) -> &str {
                "custom tool for regression coverage"
            }

            fn parameters_schema(&self) -> serde_json::Value {
                json!({"type":"object","properties":{}})
            }

            async fn execute(&self, _args: serde_json::Value) -> Result<ToolResult, ToolError> {
                Ok(ToolResult {
                    success: true,
                    result: "custom-spawn-session".to_string(),
                    display_preference: None,
                    images: Vec::new(),
                })
            }
        }

        let executor = BuiltinToolExecutorBuilder::new()
            .with_tool(CustomSpawnSessionTool)
            .expect("register custom spawn_session tool")
            .build();

        let call = make_tool_call("spawn_session", json!({}));
        let result = executor.execute(&call).await.expect("execute custom tool");
        assert!(result.success);
        assert_eq!(result.result, "custom-spawn-session");
    }

    // ---- issue #106: parse tool args once on the execute path -------------

    /// A tool that echoes back the `v` field of the args it was invoked with, so
    /// a test can observe *which* parsed value reached the tool.
    struct EchoArgsTool;

    #[async_trait]
    impl Tool for EchoArgsTool {
        fn name(&self) -> &str {
            "echo_args"
        }
        fn description(&self) -> &str {
            "echoes the `v` arg"
        }
        fn parameters_schema(&self) -> serde_json::Value {
            json!({"type":"object","properties":{"v":{"type":"string"}}})
        }
        async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
            let v = args
                .get("v")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("<none>")
                .to_string();
            Ok(ToolResult {
                success: true,
                result: v,
                display_preference: None,
                images: Vec::new(),
            })
        }
    }

    fn ctx_with_pre_parsed<'a>(
        call_id: &'a str,
        pre_parsed: Option<&'a serde_json::Value>,
    ) -> ToolExecutionContext<'a> {
        ToolExecutionContext {
            session_id: Some("s-106"),
            tool_call_id: call_id,
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            can_async_resume: false,
            pre_parsed_args: pre_parsed,
        }
    }

    #[tokio::test]
    async fn execute_with_context_reuses_pre_parsed_args_without_reparsing() {
        // The raw `arguments` string and the threaded `pre_parsed_args` Value
        // deliberately disagree. If the executor honored the contract (parse
        // once at the dispatch site, reuse downstream), the tool sees the
        // pre-parsed value; if it re-parsed the raw string it would see "raw".
        // This is the load-bearing proof that the second parse was eliminated.
        let executor = BuiltinToolExecutor::new();
        executor.register_tool(EchoArgsTool).expect("register echo");

        let call = make_tool_call("echo_args", json!({"v": "raw"}));
        let pre_parsed = json!({"v": "preparsed"});
        let ctx = ctx_with_pre_parsed(&call.id, Some(&pre_parsed));

        let result = executor
            .execute_with_context(&call, ctx)
            .await
            .expect("execute echo tool");
        assert_eq!(
            result.result, "preparsed",
            "executor must reuse pre_parsed_args, not re-parse the raw string"
        );
    }

    #[tokio::test]
    async fn execute_with_context_parses_raw_when_no_pre_parsed_args() {
        // Without a threaded value (the `execute` entry point / tests / a loop
        // that parsed with a different parser), the executor falls back to
        // parsing the raw string exactly as before — behavior preserved.
        let executor = BuiltinToolExecutor::new();
        executor.register_tool(EchoArgsTool).expect("register echo");

        let call = make_tool_call("echo_args", json!({"v": "raw"}));
        let ctx = ctx_with_pre_parsed(&call.id, None);

        let result = executor
            .execute_with_context(&call, ctx)
            .await
            .expect("execute echo tool");
        assert_eq!(
            result.result, "raw",
            "without pre_parsed_args the executor parses the raw string as before"
        );
    }

    #[tokio::test]
    async fn execute_with_context_malformed_args_repair_unchanged_without_pre_parsed() {
        // Malformed (truncated) JSON must still be auto-repaired by the
        // fallback parse when no pre-parsed value is threaded — the existing
        // error/leniency behavior is untouched by the dedup.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("recovered-no-preparsed.txt");
        let malformed_args = format!(
            r#"{{"file_path":"{}","content":"recovered content""#,
            path.display()
        );

        let executor = BuiltinToolExecutor::new();
        let call = make_tool_call_with_raw_args("Write", &malformed_args);
        let ctx = ctx_with_pre_parsed(&call.id, None);

        let result = executor
            .execute_with_context(&call, ctx)
            .await
            .expect("truncated JSON should be auto-repaired");
        assert!(result.success);
        let written = fs::read_to_string(&path).await.expect("file written");
        assert_eq!(written, "recovered content");
    }
}
