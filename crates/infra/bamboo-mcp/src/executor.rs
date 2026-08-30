use async_trait::async_trait;
use bamboo_agent_core::{
    parse_tool_args_best_effort, ToolCall, ToolError, ToolExecutionContext, ToolExecutor,
    ToolOutcome, ToolResult, ToolResultImage, ToolSchema,
};
use bamboo_domain::resolve_tool_reference_name;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{debug, error, warn};

use crate::error::McpError;
use crate::manager::McpServerManager;
use crate::tool_index::ToolIndex;
use crate::types::{McpContentItem, McpContentMetadata, McpStructuredContent};

/// MCP tool executor that delegates to the MCP server manager
pub struct McpToolExecutor {
    manager: Arc<McpServerManager>,
    authority_matches: bool,
}

impl McpToolExecutor {
    pub fn new(manager: Arc<McpServerManager>, index: Arc<ToolIndex>) -> Self {
        let authority_matches = manager.has_same_authority(&index);
        Self {
            manager,
            authority_matches,
        }
    }

    pub fn from_manager(manager: Arc<McpServerManager>) -> Self {
        Self {
            manager,
            authority_matches: true,
        }
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
                McpContentItem::Text { text, metadata } => {
                    parts.push(Self::append_content_metadata(text.clone(), metadata));
                }
                McpContentItem::Image {
                    data,
                    mime_type,
                    metadata,
                } => {
                    images.push(ToolResultImage {
                        mime_type: mime_type.clone(),
                        data: data.clone(),
                    });
                    parts.push(Self::append_content_metadata(
                        format!("[image returned: {mime_type}]"),
                        metadata,
                    ));
                }
                McpContentItem::Audio { .. } => {
                    parts.push(Self::serialized_content_block("audio content", item));
                }
                McpContentItem::ResourceLink { .. } => {
                    parts.push(Self::serialized_content_block("resource link", item));
                }
                McpContentItem::Resource { resource, metadata } => {
                    if let Some(text) = &resource.text {
                        let rendered = format!("[Resource {}]: {}", resource.uri, text);
                        if resource.meta.is_some()
                            || metadata.annotations.is_some()
                            || metadata.meta.is_some()
                        {
                            parts.push(format!(
                                "{rendered}\n{}",
                                Self::serialized_content_block("embedded resource metadata", item)
                            ));
                        } else {
                            parts.push(rendered);
                        }
                    } else {
                        // Binary embedded resources have no native ToolResult
                        // carrier. Preserve the base64 payload and all metadata
                        // in an explicit JSON content block for the model.
                        parts.push(Self::serialized_content_block("embedded resource", item));
                    }
                }
            }
        }
        (parts.join("\n"), images)
    }

    fn append_content_metadata(mut rendered: String, metadata: &McpContentMetadata) -> String {
        if metadata.annotations.is_none() && metadata.meta.is_none() {
            return rendered;
        }
        let encoded = serde_json::to_string(metadata)
            .unwrap_or_else(|error| format!("{{\"serializationError\":\"{error}\"}}"));
        rendered.push_str("\n[MCP content metadata]: ");
        rendered.push_str(&encoded);
        rendered
    }

    fn serialized_content_block(label: &str, item: &McpContentItem) -> String {
        let encoded = serde_json::to_string(item)
            .unwrap_or_else(|error| format!("{{\"serializationError\":\"{error}\"}}"));
        format!("[MCP {label}]: {encoded}")
    }

    fn append_structured_content(
        mut text: String,
        structured_content: &McpStructuredContent,
    ) -> String {
        let Some(value) = structured_content.to_json_value() else {
            return text;
        };
        let encoded = serde_json::to_string(&value)
            .unwrap_or_else(|error| format!("[unserializable structured content: {error}]"));
        if text.trim() == encoded {
            return text;
        }
        if !text.is_empty() {
            text.push('\n');
            text.push_str("[structured content]: ");
        }
        text.push_str(&encoded);
        text
    }
}

#[async_trait]
impl ToolExecutor for McpToolExecutor {
    async fn execute(&self, call: &ToolCall) -> std::result::Result<ToolResult, ToolError> {
        let tool_name = &call.function.name;
        if !self.authority_matches {
            return Err(ToolError::NotFound(
                "MCP executor uses a detached tool-index authority".to_string(),
            ));
        }
        let snapshot = self.manager.snapshot();
        let resolved = match snapshot.resolve_call(tool_name) {
            Some(resolved) => resolved,
            None => {
                return Err(ToolError::NotFound(format!(
                    "MCP tool '{}' not found",
                    tool_name
                )));
            }
        };

        debug!(
            "Executing MCP tool: {} (server: {}, original: {})",
            tool_name,
            resolved.server_id(),
            resolved.original_name()
        );

        // Parse arguments
        let args_raw = call.function.arguments.trim();
        let (args, parse_warning) = parse_tool_args_best_effort(&call.function.arguments);
        if let Some(warning) = parse_warning {
            warn!(
                "MCP tool argument parsing fallback applied: tool_call_id={}, tool_name={}, server_id={}, args_len={}, args_preview=\"{}\", warning={}",
                call.id,
                tool_name,
                resolved.server_id(),
                args_raw.len(),
                Self::preview_for_log(args_raw, 180),
                warning
            );
        }

        // Execute via manager
        match self.manager.call_resolved_tool(&resolved, args).await {
            Ok(result) => {
                let (text, images) = Self::format_result_content(&result.content);
                let text = Self::append_structured_content(text, &result.structured_content);
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
        if self.authority_matches {
            self.manager.snapshot().list_tools()
        } else {
            Vec::new()
        }
    }

    fn owns_exact_tool(&self, tool_name: &str) -> bool {
        self.authority_matches && self.manager.snapshot().contains_exact_alias(tool_name)
    }

    /// Each connected MCP server's `instructions`, rendered as a labeled block.
    /// Only ready servers contribute, so this guidance is automatically scoped to
    /// whatever is loaded for the run.
    fn tool_guidance(&self) -> Option<String> {
        if !self.authority_matches {
            return None;
        }
        let servers = self.manager.snapshot().connected_server_instructions();
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

#[derive(Clone, Debug, PartialEq, Eq)]
enum CompositeRoute {
    Builtin { execution_name: String },
    Secondary { execution_name: String },
    LegacyFallback,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CompositeOwner {
    Builtin,
    Secondary,
    None,
}

impl CompositeToolExecutor {
    pub fn new(builtin: Arc<dyn ToolExecutor>, mcp: Arc<dyn ToolExecutor>) -> Self {
        Self { builtin, mcp }
    }

    /// Resolve once against both children. Builtin wins duplicate exact names;
    /// the secondary wins an exact shadow before builtin alias fallback.
    fn route(&self, reference: &str) -> CompositeRoute {
        let mut owners = HashMap::<String, CompositeOwner>::new();
        let mut selected_owner = CompositeOwner::None;
        let resolved = resolve_tool_reference_name(reference, |candidate| {
            let owner = *owners.entry(candidate.to_string()).or_insert_with(|| {
                if self.builtin.owns_exact_tool(candidate) {
                    CompositeOwner::Builtin
                } else if self.mcp.owns_exact_tool(candidate) {
                    CompositeOwner::Secondary
                } else {
                    CompositeOwner::None
                }
            });
            if owner != CompositeOwner::None {
                selected_owner = owner;
                true
            } else {
                false
            }
        });
        let Some(execution_name) = resolved else {
            return CompositeRoute::LegacyFallback;
        };
        match selected_owner {
            CompositeOwner::Builtin => CompositeRoute::Builtin { execution_name },
            CompositeOwner::Secondary => CompositeRoute::Secondary { execution_name },
            CompositeOwner::None => CompositeRoute::LegacyFallback,
        }
    }

    fn call_with_execution_name(call: &ToolCall, execution_name: &str) -> ToolCall {
        let mut resolved_call = call.clone();
        resolved_call.function.name = execution_name.to_string();
        resolved_call
    }
}

#[async_trait]
impl ToolExecutor for CompositeToolExecutor {
    async fn execute(&self, call: &ToolCall) -> std::result::Result<ToolResult, ToolError> {
        self.execute_with_context_outcome(call, ToolExecutionContext::none(&call.id))
            .await
            .map(ToolOutcome::into_tool_result)
    }

    async fn execute_with_context(
        &self,
        call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> std::result::Result<ToolResult, ToolError> {
        self.execute_with_context_outcome(call, ctx)
            .await
            .map(ToolOutcome::into_tool_result)
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
        match self.route(&call.function.name) {
            CompositeRoute::Builtin { execution_name } => {
                self.builtin
                    .execute_exact_with_context_outcome(call, &execution_name, ctx)
                    .await
            }
            CompositeRoute::Secondary { execution_name } => {
                self.mcp
                    .execute_exact_with_context_outcome(call, &execution_name, ctx)
                    .await
            }
            CompositeRoute::LegacyFallback => {
                match self.builtin.execute_with_context_outcome(call, ctx).await {
                    Ok(outcome) => return Ok(outcome),
                    Err(ToolError::NotFound(_)) => {}
                    Err(error) => return Err(error),
                }
                self.mcp.execute_with_context_outcome(call, ctx).await
            }
        }
    }

    async fn execute_exact_with_context_outcome(
        &self,
        call: &ToolCall,
        execution_name: &str,
        ctx: ToolExecutionContext<'_>,
    ) -> std::result::Result<ToolOutcome, ToolError> {
        if self.builtin.owns_exact_tool(execution_name) {
            self.builtin
                .execute_exact_with_context_outcome(call, execution_name, ctx)
                .await
        } else if self.mcp.owns_exact_tool(execution_name) {
            self.mcp
                .execute_exact_with_context_outcome(call, execution_name, ctx)
                .await
        } else {
            Err(ToolError::NotFound(format!(
                "Tool '{}' not found",
                execution_name
            )))
        }
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
        match self.route(&call.function.name) {
            CompositeRoute::Builtin { execution_name } => {
                self.builtin
                    .check_permissions_for_exact(call, &execution_name, ctx)
                    .await
            }
            CompositeRoute::Secondary { execution_name } => {
                let args = ctx
                    .pre_parsed_args
                    .cloned()
                    .unwrap_or_else(|| parse_tool_args_best_effort(&call.function.arguments).0);
                self.builtin
                    .check_permissions_for_resolved(call, &execution_name, &args, ctx)
                    .await
            }
            CompositeRoute::LegacyFallback => self.builtin.check_permissions_for(call, ctx).await,
        }
    }

    async fn check_permissions_for_exact(
        &self,
        call: &ToolCall,
        execution_name: &str,
        ctx: &ToolExecutionContext<'_>,
    ) -> std::result::Result<Option<ToolOutcome>, ToolError> {
        if self.builtin.owns_exact_tool(execution_name) {
            self.builtin
                .check_permissions_for_exact(call, execution_name, ctx)
                .await
        } else if self.mcp.owns_exact_tool(execution_name) {
            let args = ctx
                .pre_parsed_args
                .cloned()
                .unwrap_or_else(|| parse_tool_args_best_effort(&call.function.arguments).0);
            self.builtin
                .check_permissions_for_resolved(call, execution_name, &args, ctx)
                .await
        } else {
            Err(ToolError::NotFound(format!(
                "Tool '{}' not found",
                execution_name
            )))
        }
    }

    async fn check_permissions_for_resolved(
        &self,
        call: &ToolCall,
        execution_name: &str,
        args: &serde_json::Value,
        ctx: &ToolExecutionContext<'_>,
    ) -> std::result::Result<Option<ToolOutcome>, ToolError> {
        // The builtin executor hosts the real permission policy for every live
        // surface, including MCP and the server overlays stacked above this
        // composite. Preserve that architecture while forwarding the exact
        // identity and effective arguments selected by the routing layer.
        self.builtin
            .check_permissions_for_resolved(call, execution_name, args, ctx)
            .await
    }

    fn list_tools(&self) -> Vec<ToolSchema> {
        let mut tools = self.builtin.list_tools();
        let mut names: HashSet<String> = tools
            .iter()
            .map(|schema| schema.function.name.clone())
            .collect();
        tools.extend(
            self.mcp
                .list_tools()
                .into_iter()
                .filter(|schema| names.insert(schema.function.name.clone())),
        );
        tools
    }

    fn owns_exact_tool(&self, tool_name: &str) -> bool {
        self.builtin.owns_exact_tool(tool_name) || self.mcp.owns_exact_tool(tool_name)
    }

    fn tool_mutability(&self, tool_name: &str) -> bamboo_agent_core::ToolMutability {
        match self.route(tool_name) {
            CompositeRoute::Builtin { execution_name } => {
                self.builtin.tool_mutability(&execution_name)
            }
            CompositeRoute::Secondary { execution_name } => {
                self.mcp.tool_mutability(&execution_name)
            }
            CompositeRoute::LegacyFallback => bamboo_agent_core::classify_tool(tool_name),
        }
    }

    fn call_mutability(&self, call: &ToolCall) -> bamboo_agent_core::ToolMutability {
        self.call_parallel_classification(call).0
    }

    fn tool_concurrency_safe(&self, tool_name: &str) -> bool {
        match self.route(tool_name) {
            CompositeRoute::Builtin { execution_name } => {
                self.builtin.tool_concurrency_safe(&execution_name)
            }
            CompositeRoute::Secondary { execution_name } => {
                self.mcp.tool_concurrency_safe(&execution_name)
            }
            CompositeRoute::LegacyFallback => {
                bamboo_agent_core::classify_tool(tool_name)
                    == bamboo_agent_core::ToolMutability::ReadOnly
            }
        }
    }

    fn call_concurrency_safe(&self, call: &ToolCall) -> bool {
        self.call_parallel_classification(call).1
    }

    fn call_parallel_classification(
        &self,
        call: &ToolCall,
    ) -> (bamboo_agent_core::ToolMutability, bool) {
        match self.route(&call.function.name) {
            CompositeRoute::Builtin { execution_name } => self
                .builtin
                .call_parallel_classification(&Self::call_with_execution_name(
                    call,
                    &execution_name,
                )),
            CompositeRoute::Secondary { execution_name } => {
                self.mcp
                    .call_parallel_classification(&Self::call_with_execution_name(
                        call,
                        &execution_name,
                    ))
            }
            CompositeRoute::LegacyFallback => {
                let mutability = bamboo_agent_core::classify_tool(&call.function.name);
                (
                    mutability,
                    mutability == bamboo_agent_core::ToolMutability::ReadOnly,
                )
            }
        }
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

    struct RoutingStub {
        exact_names: Vec<&'static str>,
        label: &'static str,
        apply_patch_alias: bool,
        error: Option<ToolError>,
        classification: (bamboo_agent_core::ToolMutability, bool),
        executed: Arc<std::sync::Mutex<Vec<String>>>,
        permission_checked: Arc<std::sync::Mutex<Vec<(String, serde_json::Value)>>>,
        permission_reresolution: Option<(&'static str, &'static str)>,
        classified: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl RoutingStub {
        fn new(exact_names: Vec<&'static str>, label: &'static str) -> Self {
            Self {
                exact_names,
                label,
                apply_patch_alias: false,
                error: None,
                classification: (bamboo_agent_core::ToolMutability::Mutating, false),
                executed: Arc::new(std::sync::Mutex::new(Vec::new())),
                permission_checked: Arc::new(std::sync::Mutex::new(Vec::new())),
                permission_reresolution: None,
                classified: Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }

        fn schema(name: &str) -> ToolSchema {
            ToolSchema {
                schema_type: "function".to_string(),
                function: FunctionSchema {
                    name: name.to_string(),
                    description: "routing stub".to_string(),
                    parameters: serde_json::json!({"type":"object"}),
                },
            }
        }
    }

    #[async_trait]
    impl ToolExecutor for RoutingStub {
        async fn execute(&self, call: &ToolCall) -> std::result::Result<ToolResult, ToolError> {
            let exact = self.owns_exact_tool(&call.function.name);
            let accepted_alias = self.apply_patch_alias
                && call.function.name == "apply_patch"
                && self.owns_exact_tool("Edit");
            if !exact && !accepted_alias {
                return Err(ToolError::NotFound(call.function.name.clone()));
            }
            self.executed
                .lock()
                .unwrap()
                .push(call.function.name.clone());
            if let Some(error) = &self.error {
                return Err(error.clone());
            }
            Ok(ToolResult {
                success: true,
                result: self.label.to_string(),
                display_preference: None,
                images: Vec::new(),
            })
        }

        async fn check_permissions_for(
            &self,
            call: &ToolCall,
            _ctx: &ToolExecutionContext<'_>,
        ) -> std::result::Result<Option<ToolOutcome>, ToolError> {
            let args = parse_tool_args_best_effort(&call.function.arguments).0;
            let permission_name = self
                .permission_reresolution
                .filter(|(reference, _)| *reference == call.function.name)
                .map(|(_, alias)| alias)
                .unwrap_or(&call.function.name);
            self.permission_checked
                .lock()
                .unwrap()
                .push((permission_name.to_string(), args));
            Ok(None)
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            self.exact_names
                .iter()
                .map(|name| Self::schema(name))
                .collect()
        }

        fn owns_exact_tool(&self, tool_name: &str) -> bool {
            self.exact_names.contains(&tool_name)
        }

        fn call_parallel_classification(
            &self,
            call: &ToolCall,
        ) -> (bamboo_agent_core::ToolMutability, bool) {
            self.classified
                .lock()
                .unwrap()
                .push(call.function.name.clone());
            self.classification
        }
    }

    #[tokio::test]
    async fn secondary_exact_apply_patch_beats_builtin_alias_across_all_routes() {
        let mut builtin = RoutingStub::new(vec!["Edit"], "builtin-edit");
        builtin.apply_patch_alias = true;
        let builtin_executed = builtin.executed.clone();
        let builtin_permissions = builtin.permission_checked.clone();

        let mut secondary = RoutingStub::new(vec!["apply_patch"], "secondary-exact");
        secondary.classification = (bamboo_agent_core::ToolMutability::ReadOnly, true);
        let secondary_executed = secondary.executed.clone();
        let secondary_permissions = secondary.permission_checked.clone();

        let composite = CompositeToolExecutor::new(Arc::new(builtin), Arc::new(secondary));
        let call = create_test_tool_call("apply_patch", r#"{"path":"custom"}"#);

        let result = composite
            .execute(&call)
            .await
            .expect("secondary exact owner must execute");
        assert_eq!(result.result, "secondary-exact");
        assert!(builtin_executed.lock().unwrap().is_empty());
        assert_eq!(
            secondary_executed.lock().unwrap().as_slice(),
            ["apply_patch"]
        );
        assert_eq!(
            composite.call_parallel_classification(&call),
            (bamboo_agent_core::ToolMutability::ReadOnly, true)
        );

        composite
            .check_permissions_for(&call, &ToolExecutionContext::none(&call.id))
            .await
            .expect("secondary permission check");
        assert_eq!(
            builtin_permissions.lock().unwrap().as_slice(),
            [(
                "apply_patch".to_string(),
                serde_json::json!({"path": "custom"})
            )]
        );
        assert!(secondary_permissions.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn stacked_composites_keep_the_selected_builtin_exact_identity() {
        let mut builtin = RoutingStub::new(vec!["custom_tool"], "builtin-custom");
        builtin.permission_reresolution = Some(("default::custom_tool", "legacy_alias_owner"));
        let builtin = Arc::new(builtin);
        let builtin_executed = builtin.executed.clone();
        let builtin_permissions = builtin.permission_checked.clone();
        let builtin_classified = builtin.classified.clone();

        let mut secondary = RoutingStub::new(vec!["legacy_alias_owner"], "secondary-alias");
        secondary.classification = (bamboo_agent_core::ToolMutability::ReadOnly, true);
        let secondary_executed = secondary.executed.clone();
        let secondary_permissions = secondary.permission_checked.clone();
        let secondary_classified = secondary.classified.clone();
        let call = create_test_tool_call("default::custom_tool", r#"{"path":"selected-owner"}"#);
        let ctx = ToolExecutionContext::none(&call.id);

        builtin
            .check_permissions_for(&call, &ctx)
            .await
            .expect("raw permission repro");
        assert_eq!(
            builtin_permissions.lock().unwrap().as_slice(),
            [(
                "legacy_alias_owner".to_string(),
                serde_json::json!({"path": "selected-owner"})
            )],
            "the generic raw gate demonstrates the re-resolution mismatch"
        );
        builtin_permissions.lock().unwrap().clear();

        let inner = Arc::new(CompositeToolExecutor::new(builtin, Arc::new(secondary)));
        let composite = CompositeToolExecutor::new(inner, Arc::new(GuidanceStub(None)));
        let result = composite
            .execute(&call)
            .await
            .expect("selected builtin exact owner executes");
        assert_eq!(result.result, "builtin-custom");
        composite
            .check_permissions_for(&call, &ctx)
            .await
            .expect("selected builtin exact owner permission check");
        assert_eq!(
            composite.call_parallel_classification(&call),
            (bamboo_agent_core::ToolMutability::Mutating, false)
        );

        assert_eq!(builtin_executed.lock().unwrap().as_slice(), ["custom_tool"]);
        assert_eq!(
            builtin_permissions.lock().unwrap().as_slice(),
            [(
                "custom_tool".to_string(),
                serde_json::json!({"path": "selected-owner"})
            )]
        );
        assert_eq!(
            builtin_classified.lock().unwrap().as_slice(),
            ["custom_tool"]
        );
        assert!(secondary_executed.lock().unwrap().is_empty());
        assert!(secondary_permissions.lock().unwrap().is_empty());
        assert!(secondary_classified.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn unshadowed_apply_patch_preserves_builtin_alias_fallback() {
        let mut builtin = RoutingStub::new(vec!["Edit"], "builtin-edit");
        builtin.apply_patch_alias = true;
        let composite = CompositeToolExecutor::new(
            Arc::new(builtin),
            Arc::new(RoutingStub::new(Vec::new(), "secondary")),
        );

        let result = composite
            .execute(&create_test_tool_call("apply_patch", "{}"))
            .await
            .expect("unshadowed alias must retain builtin behavior");
        assert_eq!(result.result, "builtin-edit");
    }

    #[tokio::test]
    async fn namespace_fallback_executes_the_selected_secondary_exact_identity() {
        let secondary = RoutingStub::new(vec!["custom_tool"], "secondary-custom");
        let composite = CompositeToolExecutor::new(
            Arc::new(RoutingStub::new(Vec::new(), "builtin")),
            Arc::new(secondary),
        );

        let result = composite
            .execute(&create_test_tool_call("default::custom_tool", "{}"))
            .await
            .expect("namespace fallback must execute secondary exact identity");
        assert_eq!(result.result, "secondary-custom");
    }

    #[tokio::test]
    async fn selected_exact_owner_errors_never_fall_through() {
        for error in [
            ToolError::NotFound("secondary exact missing".to_string()),
            ToolError::Execution("secondary exact denied".to_string()),
        ] {
            let mut builtin = RoutingStub::new(vec!["Edit"], "builtin-fallback");
            builtin.apply_patch_alias = true;
            let builtin_executed = builtin.executed.clone();
            let mut secondary = RoutingStub::new(vec!["apply_patch"], "secondary");
            secondary.error = Some(error.clone());
            let composite = CompositeToolExecutor::new(Arc::new(builtin), Arc::new(secondary));

            let actual = composite
                .execute(&create_test_tool_call("apply_patch", "{}"))
                .await
                .expect_err("selected exact owner error must propagate");
            assert_eq!(actual.to_string(), error.to_string());
            assert!(builtin_executed.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn duplicate_exact_names_are_builtin_first_and_listed_once() {
        let builtin = RoutingStub::new(vec!["duplicate"], "builtin");
        let secondary = RoutingStub::new(vec!["duplicate"], "secondary");
        let secondary_executed = secondary.executed.clone();
        let composite = CompositeToolExecutor::new(Arc::new(builtin), Arc::new(secondary));

        let result = composite
            .execute(&create_test_tool_call("duplicate", "{}"))
            .await
            .expect("builtin duplicate owner must win");
        assert_eq!(result.result, "builtin");
        assert!(secondary_executed.lock().unwrap().is_empty());
        assert_eq!(
            composite
                .list_tools()
                .iter()
                .filter(|schema| schema.function.name == "duplicate")
                .count(),
            1
        );
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
                metadata: McpContentMetadata::default(),
            },
            McpContentItem::Image {
                data: "abc123".to_string(),
                mime_type: "image/jpeg".to_string(),
                metadata: McpContentMetadata::default(),
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
                metadata: McpContentMetadata::default(),
            },
            McpContentItem::Text {
                text: "World".to_string(),
                metadata: McpContentMetadata::default(),
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
            metadata: McpContentMetadata::default(),
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
                meta: None,
            },
            metadata: McpContentMetadata::default(),
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
                meta: None,
            },
            metadata: McpContentMetadata::default(),
        }];
        let (text, _images) = McpToolExecutor::format_result_content(&content);
        assert!(text.starts_with("[MCP embedded resource]: "));
        assert!(text.contains("\"blob\":\"base64data\""));
    }

    #[test]
    fn non_native_content_and_metadata_reach_the_model_without_data_loss() {
        let audio: McpContentItem = serde_json::from_value(serde_json::json!({
            "type": "audio",
            "data": "UklGRg==",
            "mimeType": "audio/wav",
            "annotations": {"audience": ["assistant"]},
            "_meta": {"example.com/trace": "trace-1"}
        }))
        .expect("audio block");
        let link: McpContentItem = serde_json::from_value(serde_json::json!({
            "type": "resource_link",
            "uri": "file:///report.json",
            "name": "report",
            "icons": [{"src": "data:image/png;base64,AA=="}],
            "_meta": {"example.com/link": true}
        }))
        .expect("resource link block");
        let blob: McpContentItem = serde_json::from_value(serde_json::json!({
            "type": "resource",
            "resource": {
                "uri": "file:///payload.bin",
                "blob": "AAEC",
                "_meta": {"example.com/checksum": "abc"}
            },
            "annotations": {"priority": 1.0}
        }))
        .expect("embedded blob block");

        let (text, images) = McpToolExecutor::format_result_content(&[audio, link, blob]);
        assert!(images.is_empty());
        for preserved in [
            "UklGRg==",
            "audio/wav",
            "example.com/trace",
            "data:image/png;base64,AA==",
            "example.com/link",
            "AAEC",
            "example.com/checksum",
            "\"priority\":1.0",
        ] {
            assert!(
                text.contains(preserved),
                "formatted content omitted {preserved}: {text}"
            );
        }
    }

    #[test]
    fn test_format_result_mixed() {
        let content = vec![
            McpContentItem::Text {
                text: "Result:".to_string(),
                metadata: McpContentMetadata::default(),
            },
            McpContentItem::Image {
                data: "img".to_string(),
                mime_type: "image/png".to_string(),
                metadata: McpContentMetadata::default(),
            },
        ];
        let (text, images) = McpToolExecutor::format_result_content(&content);
        assert!(text.contains("Result:"));
        assert!(text.contains("[image returned:"));
        assert_eq!(images.len(), 1);
    }

    #[test]
    fn structured_content_is_preserved_without_duplicating_text_fallback() {
        let structured = McpStructuredContent::Value(serde_json::json!({
            "temperature": 22.5,
            "condition": "clear"
        }));
        let encoded = r#"{"condition":"clear","temperature":22.5}"#;
        assert_eq!(
            McpToolExecutor::append_structured_content(String::new(), &structured),
            encoded
        );
        assert_eq!(
            McpToolExecutor::append_structured_content(encoded.to_string(), &structured),
            encoded
        );
        assert_eq!(
            McpToolExecutor::append_structured_content(
                "Weather result".to_string(),
                &McpStructuredContent::Null,
            ),
            "Weather result\n[structured content]: null"
        );
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
        let mut mock_mcp = MockToolExecutor::new();

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
        mock_mcp.expect_list_tools().returning(Vec::new);

        let composite = CompositeToolExecutor::new(Arc::new(mock_builtin), Arc::new(mock_mcp));

        let call = create_test_tool_call("test_tool", "{}");
        let result = composite.execute(&call).await.unwrap();
        assert!(result.success);
        assert_eq!(result.result, "Built-in result");
    }

    #[tokio::test]
    async fn test_composite_executor_builtin_error() {
        let mut mock_builtin = MockToolExecutor::new();
        let mut mock_mcp = MockToolExecutor::new();

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
        mock_mcp.expect_list_tools().returning(Vec::new);

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
