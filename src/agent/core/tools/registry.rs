//! Tool registry for managing and executing tools.
//!
//! This module provides a thread-safe registry for tool management,
//! including registration, lookup, and execution of tools.
//!
//! # Key Types
//!
//! - [`Tool`] - Trait for implementing executable tools
//! - [`ToolRegistry`] - Thread-safe tool registry
//! - [`RegistryError`] - Registration errors
//! - [`SharedTool`] - Reference-counted tool pointer
//!
//! # Usage
//!
//! ```rust,ignore
//! use bamboo_agent::agent::core::tools::registry::*;
//!
//! // Create a registry
//! let registry = ToolRegistry::new();
//!
//! // Register a tool
//! registry.register(MyTool::new())?;
//!
//! // Get tool schema for LLM
//! let schemas = registry.list_tools();
//!
//! // Execute a tool
//! let tool = registry.get("my_tool").unwrap();
//! let result = tool.execute(args).await?;
//! ```
//!
//! # Global Registry
//!
//! For convenience, a global singleton registry is available:
//!
//! ```rust,ignore
//! let registry = global_registry();
//! registry.register(my_tool)?;
//! ```

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use dashmap::{mapref::entry::Entry, DashMap};
use thiserror::Error;

use crate::agent::core::tools::{
    FunctionSchema, ToolError, ToolExecutionContext, ToolResult, ToolSchema,
};

/// Trait for implementing executable tools.
///
/// All tools must implement this trait to be registered with the tool registry.
///
/// # Required Methods
///
/// - `name()` - Unique tool identifier
/// - `description()` - Human-readable tool description
/// - `parameters_schema()` - JSON Schema for tool parameters
/// - `execute()` - Async tool execution logic
///
/// # Provided Methods
///
/// - `to_schema()` - Convert tool to LLM-compatible schema
///
/// # Example
///
/// ```rust,ignore
/// struct ReadFileTool;
///
/// #[async_trait]
/// impl Tool for ReadFileTool {
///     fn name(&self) -> &str {
///         "read_file"
///     }
///
///     fn description(&self) -> &str {
///         "Read file contents from disk"
///     }
///
///     fn parameters_schema(&self) -> serde_json::Value {
///         json!({
///             "type": "object",
///             "properties": {
///                 "path": {"type": "string"}
///             },
///             "required": ["path"]
///         })
///     }
///
///     async fn execute(&self, args: Value) -> Result<ToolResult, ToolError> {
///         let path = args["path"].as_str().unwrap();
///         let content = tokio::fs::read_to_string(path).await?;
///         Ok(ToolResult {
///             success: true,
///             result: content,
///             display_preference: None,
///         })
///     }
/// }
/// ```
#[async_trait]
pub trait Tool: Send + Sync {
    fn name(&self) -> &str;
    /// Human-readable tool description for LLM.
    fn description(&self) -> &str;
    /// JSON Schema for tool parameters.
    fn parameters_schema(&self) -> serde_json::Value;
    /// Execute the tool with given arguments.
    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError>;

    /// Execute the tool with a streaming-capable context.
    ///
    /// Default implementation falls back to `execute()` for tools that don't
    /// need streaming.
    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        _ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        self.execute(args).await
    }

    /// Convert tool to LLM-compatible schema.
    ///
    /// Creates a [`ToolSchema`] suitable for LLM function calling.
    fn to_schema(&self) -> ToolSchema {
        ToolSchema {
            schema_type: "function".to_string(),
            function: FunctionSchema {
                name: self.name().to_string(),
                description: self.description().to_string(),
                parameters: self.parameters_schema(),
            },
        }
    }
}

/// Reference-counted pointer to a tool.
pub type SharedTool = Arc<dyn Tool>;

/// Errors that can occur during tool registration.
///
/// # Variants
///
/// * `DuplicateTool` - Tool with same name already registered
/// * `InvalidTool` - Tool validation failed (e.g., empty name)
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RegistryError {
    /// Tool with same name already exists in registry.
    #[error("tool with name '{0}' already registered")]
    DuplicateTool(String),

    /// Tool validation failed.
    #[error("invalid tool: {0}")]
    InvalidTool(String),
}

/// Thread-safe tool registry.
///
/// Manages a collection of tools with concurrent access support.
/// Uses a `DashMap` for lock-free concurrent operations.
///
/// # Features
///
/// - Thread-safe registration and lookup
/// - Tool schema generation for LLM
/// - Global singleton registry support
///
/// # Example
///
/// ```rust,ignore
/// let registry = ToolRegistry::new();
///
/// // Register tools
/// registry.register(ReadFileTool::new())?;
/// registry.register(WriteFileTool::new())?;
///
/// // List all tool schemas
/// let schemas = registry.list_tools();
///
/// // Get and execute a tool
/// if let Some(tool) = registry.get("read_file") {
///     let result = tool.execute(json!({"path": "test.txt"})).await?;
/// }
/// ```
pub struct ToolRegistry {
    tools: DashMap<String, SharedTool>,
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ToolRegistry {
    /// Create a new empty tool registry.
    pub fn new() -> Self {
        Self {
            tools: DashMap::new(),
        }
    }

    /// Register a tool in the registry.
    ///
    /// # Arguments
    ///
    /// * `tool` - Tool to register
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::DuplicateTool`] if tool name already exists.
    /// Returns [`RegistryError::InvalidTool`] if tool name is empty.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// registry.register(MyTool::new())?;
    /// ```
    pub fn register<T>(&self, tool: T) -> Result<(), RegistryError>
    where
        T: Tool + 'static,
    {
        self.register_shared(Arc::new(tool))
    }

    /// Register a shared tool reference.
    ///
    /// # Arguments
    ///
    /// * `tool` - Shared tool reference
    ///
    /// # Errors
    ///
    /// Same as [`register`](Self::register).
    pub fn register_shared(&self, tool: SharedTool) -> Result<(), RegistryError> {
        let name = tool.name().trim();

        if name.is_empty() {
            return Err(RegistryError::InvalidTool(
                "tool name cannot be empty".to_string(),
            ));
        }

        match self.tools.entry(name.to_string()) {
            Entry::Occupied(_) => Err(RegistryError::DuplicateTool(name.to_string())),
            Entry::Vacant(entry) => {
                entry.insert(tool);
                Ok(())
            }
        }
    }

    /// Get a tool by name.
    ///
    /// # Arguments
    ///
    /// * `name` - Tool name
    ///
    /// # Returns
    ///
    /// Shared tool reference if found, `None` otherwise.
    pub fn get(&self, name: &str) -> Option<SharedTool> {
        self.tools.get(name).map(|entry| Arc::clone(entry.value()))
    }

    /// Check if a tool exists in the registry.
    pub fn contains(&self, name: &str) -> bool {
        self.tools.contains_key(name)
    }

    /// List all tool schemas.
    ///
    /// Returns schemas sorted alphabetically by tool name.
    pub fn list_tools(&self) -> Vec<ToolSchema> {
        let mut tools: Vec<ToolSchema> = self
            .tools
            .iter()
            .map(|entry| entry.value().to_schema())
            .collect();
        tools.sort_by(|left, right| left.function.name.cmp(&right.function.name));
        tools
    }

    /// List all tool names.
    ///
    /// Returns names sorted alphabetically.
    pub fn list_tool_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.tools.iter().map(|entry| entry.key().clone()).collect();
        names.sort();
        names
    }

    /// Remove a tool from the registry.
    ///
    /// # Returns
    ///
    /// `true` if tool was removed, `false` if not found.
    pub fn unregister(&self, name: &str) -> bool {
        self.tools.remove(name).is_some()
    }

    /// Get the number of registered tools.
    pub fn len(&self) -> usize {
        self.tools.len()
    }

    /// Check if registry is empty.
    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }

    /// Remove all tools from the registry.
    pub fn clear(&self) {
        self.tools.clear();
    }
}

/// Global tool registry singleton.
static GLOBAL_REGISTRY: OnceLock<ToolRegistry> = OnceLock::new();

/// Get the global tool registry.
///
/// The global registry is a singleton that persists for the lifetime
/// of the application. Useful for sharing tools across components.
///
/// # Example
///
/// ```rust,ignore
/// let registry = global_registry();
/// registry.register(my_tool)?;
/// ```
pub fn global_registry() -> &'static ToolRegistry {
    GLOBAL_REGISTRY.get_or_init(ToolRegistry::new)
}

/// Normalize a tool name by removing namespace prefix.
///
/// # Arguments
///
/// * `name` - Tool name (may include `::` namespace separator)
///
/// # Returns
///
/// Tool name after the last `::`, or the original name if no separator.
///
/// # Example
///
/// ```rust,ignore
/// assert_eq!(normalize_tool_name("bamboo::read_file"), "read_file");
/// assert_eq!(normalize_tool_name("read_file"), "read_file");
/// ```
pub fn normalize_tool_name(name: &str) -> &str {
    name.split("::").last().unwrap_or(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    struct TestTool {
        name: &'static str,
        description: &'static str,
    }

    #[async_trait]
    impl Tool for TestTool {
        fn name(&self) -> &str {
            self.name
        }

        fn description(&self) -> &str {
            self.description
        }

        fn parameters_schema(&self) -> serde_json::Value {
            json!({
                "type": "object",
                "properties": {}
            })
        }

        async fn execute(&self, _args: serde_json::Value) -> Result<ToolResult, ToolError> {
            Ok(ToolResult {
                success: true,
                result: "ok".to_string(),
                display_preference: None,
            })
        }
    }

    #[test]
    fn register_and_get() {
        let registry = ToolRegistry::new();
        let tool = TestTool {
            name: "test_tool",
            description: "test tool",
        };

        assert!(registry.register(tool).is_ok());
        assert!(registry.get("test_tool").is_some());
        assert!(registry.get("unknown").is_none());
    }

    #[test]
    fn duplicate_tool_registration() {
        let registry = ToolRegistry::new();

        registry
            .register(TestTool {
                name: "dup",
                description: "first",
            })
            .unwrap();

        let duplicate = registry.register(TestTool {
            name: "dup",
            description: "second",
        });

        assert!(matches!(duplicate, Err(RegistryError::DuplicateTool(name)) if name == "dup"));
    }

    #[test]
    fn list_tools_returns_registered_tools() {
        let registry = ToolRegistry::new();

        registry
            .register(TestTool {
                name: "tool_a",
                description: "tool a",
            })
            .unwrap();
        registry
            .register(TestTool {
                name: "tool_b",
                description: "tool b",
            })
            .unwrap();

        let tools = registry.list_tools();

        assert_eq!(tools.len(), 2);
        assert_eq!(tools[0].function.name, "tool_a");
        assert_eq!(tools[1].function.name, "tool_b");
    }

    #[test]
    fn register_rejects_empty_tool_name() {
        let registry = ToolRegistry::new();

        let result = registry.register(TestTool {
            name: "",
            description: "invalid",
        });

        assert!(
            matches!(result, Err(RegistryError::InvalidTool(reason)) if reason == "tool name cannot be empty")
        );
    }

    #[test]
    fn normalize_tool_name_handles_namespaced_inputs() {
        assert_eq!(normalize_tool_name("read_file"), "read_file");
        assert_eq!(normalize_tool_name("default::read_file"), "read_file");
        assert_eq!(normalize_tool_name("a::b::c::read_file"), "read_file");
    }
}
