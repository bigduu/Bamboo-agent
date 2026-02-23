use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use dashmap::{mapref::entry::Entry, DashMap};
use thiserror::Error;

use crate::agent::core::tools::{FunctionSchema, ToolError, ToolResult, ToolSchema};

#[async_trait]
pub trait Tool: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn parameters_schema(&self) -> serde_json::Value;
    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError>;

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

pub type SharedTool = Arc<dyn Tool>;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RegistryError {
    #[error("tool with name '{0}' already registered")]
    DuplicateTool(String),

    #[error("invalid tool: {0}")]
    InvalidTool(String),
}

pub struct ToolRegistry {
    tools: DashMap<String, SharedTool>,
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self {
            tools: DashMap::new(),
        }
    }

    pub fn register<T>(&self, tool: T) -> Result<(), RegistryError>
    where
        T: Tool + 'static,
    {
        self.register_shared(Arc::new(tool))
    }

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

    pub fn get(&self, name: &str) -> Option<SharedTool> {
        self.tools.get(name).map(|entry| Arc::clone(entry.value()))
    }

    pub fn contains(&self, name: &str) -> bool {
        self.tools.contains_key(name)
    }

    pub fn list_tools(&self) -> Vec<ToolSchema> {
        let mut tools: Vec<ToolSchema> = self
            .tools
            .iter()
            .map(|entry| entry.value().to_schema())
            .collect();
        tools.sort_by(|left, right| left.function.name.cmp(&right.function.name));
        tools
    }

    pub fn list_tool_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.tools.iter().map(|entry| entry.key().clone()).collect();
        names.sort();
        names
    }

    pub fn unregister(&self, name: &str) -> bool {
        self.tools.remove(name).is_some()
    }

    pub fn len(&self) -> usize {
        self.tools.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }

    pub fn clear(&self) {
        self.tools.clear();
    }
}

static GLOBAL_REGISTRY: OnceLock<ToolRegistry> = OnceLock::new();

pub fn global_registry() -> &'static ToolRegistry {
    GLOBAL_REGISTRY.get_or_init(ToolRegistry::new)
}

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
