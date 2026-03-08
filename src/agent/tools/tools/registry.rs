//! Tool registry with guide support for enhanced prompting.

use std::sync::Arc;

use crate::agent::core::tools::{
    registry::{RegistryError, SharedTool},
    Tool, ToolSchema,
};
use dashmap::DashMap;

use crate::agent::tools::guide::{ToolGuide, ToolGuideSpec};

/// Tool registry with guide support for enhanced prompting.
///
/// This registry extends the core ToolRegistry with support for tool guides,
/// which provide enhanced documentation and usage examples for LLM prompting.
///
/// # Features
///
/// - Register tools with or without guides
/// - Load guides from JSON or YAML specifications
/// - Query tools and their associated guides
/// - Thread-safe concurrent access
///
/// # Example
///
/// ```no_run
/// use bamboo_agent::tools::{ToolRegistry, ReadTool};
///
/// let registry = ToolRegistry::new();
/// registry.register(ReadTool::new()).unwrap();
///
/// // Later, add a guide
/// let guide_spec = r#"{...}"#;
/// registry.register_guide_from_json("Read", guide_spec).unwrap();
/// ```
pub struct ToolRegistry {
    tools: crate::agent::core::tools::ToolRegistry,
    guides: DashMap<String, Arc<dyn ToolGuide>>,
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ToolRegistry {
    /// Create a new empty ToolRegistry instance.
    ///
    /// The registry starts with no tools or guides registered.
    /// Use [`register`](Self::register) or [`register_with_guide`](Self::register_with_guide)
    /// to add tools.
    pub fn new() -> Self {
        Self {
            tools: crate::agent::core::tools::ToolRegistry::new(),
            guides: DashMap::new(),
        }
    }

    /// Register a tool without guide
    pub fn register<T>(&self, tool: T) -> Result<(), RegistryError>
    where
        T: Tool + 'static,
    {
        self.tools.register(tool)
    }

    /// Register a tool with its guide
    pub fn register_with_guide<T, G>(&self, tool: T, guide: G) -> Result<(), RegistryError>
    where
        T: Tool + 'static,
        G: ToolGuide + 'static,
    {
        let name = tool.name().to_string();
        self.tools.register(tool)?;
        self.guides.insert(name, Arc::new(guide));
        Ok(())
    }

    /// Register a guide for an already-registered tool
    pub fn register_guide<G>(&self, tool_name: &str, guide: G) -> Result<(), RegistryError>
    where
        G: ToolGuide + 'static,
    {
        if !self.tools.contains(tool_name) {
            return Err(RegistryError::InvalidTool(format!(
                "tool '{}' not found, register tool before adding guide",
                tool_name
            )));
        }
        self.guides.insert(tool_name.to_string(), Arc::new(guide));
        Ok(())
    }

    /// Register guide from JSON spec
    pub fn register_guide_from_json(
        &self,
        tool_name: &str,
        json_spec: &str,
    ) -> Result<(), RegistryError> {
        let spec = ToolGuideSpec::from_json_str(json_spec)
            .map_err(|e| RegistryError::InvalidTool(format!("invalid guide JSON: {}", e)))?;
        self.register_guide(tool_name, spec)
    }

    /// Register guide from YAML spec
    pub fn register_guide_from_yaml(
        &self,
        tool_name: &str,
        yaml_spec: &str,
    ) -> Result<(), RegistryError> {
        let spec = ToolGuideSpec::from_yaml_str(yaml_spec)
            .map_err(|e| RegistryError::InvalidTool(format!("invalid guide YAML: {}", e)))?;
        self.register_guide(tool_name, spec)
    }

    /// Get a tool by name
    pub fn get(&self, name: &str) -> Option<SharedTool> {
        self.tools.get(name)
    }

    /// Get a tool's guide by name
    pub fn get_guide(&self, name: &str) -> Option<Arc<dyn ToolGuide>> {
        self.guides.get(name).map(|entry| Arc::clone(&entry))
    }

    /// Check if tool exists
    pub fn contains(&self, name: &str) -> bool {
        self.tools.contains(name)
    }

    /// List all tool schemas
    pub fn list_tools(&self) -> Vec<ToolSchema> {
        self.tools.list_tools()
    }

    /// List all tool names
    pub fn list_tool_names(&self) -> Vec<String> {
        self.tools.list_tool_names()
    }

    /// Unregister a tool (also removes guide)
    pub fn unregister(&self, name: &str) -> bool {
        self.guides.remove(name);
        self.tools.unregister(name)
    }

    pub fn len(&self) -> usize {
        self.tools.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }

    pub fn clear(&self) {
        self.guides.clear();
        self.tools.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::tools::tools::ReadTool;

    struct MockGuide;

    impl ToolGuide for MockGuide {
        fn tool_name(&self) -> &str {
            "mock_tool"
        }

        fn when_to_use(&self) -> &str {
            "when you need to mock"
        }

        fn when_not_to_use(&self) -> &str {
            "in production"
        }

        fn examples(&self) -> Vec<crate::agent::tools::guide::ToolExample> {
            vec![]
        }

        fn related_tools(&self) -> Vec<&str> {
            vec![]
        }

        fn category(&self) -> crate::agent::tools::guide::ToolCategory {
            crate::agent::tools::guide::ToolCategory::FileReading
        }
    }

    #[test]
    fn register_tool_without_guide() {
        let registry = ToolRegistry::new();
        registry.register(ReadTool::new()).unwrap();

        assert!(registry.contains("Read"));
        assert!(registry.get_guide("Read").is_none());
    }

    #[test]
    fn register_tool_with_guide() {
        let registry = ToolRegistry::new();
        registry
            .register_with_guide(ReadTool::new(), MockGuide)
            .unwrap();

        assert!(registry.contains("Read"));
        assert!(registry.get_guide("Read").is_some());
    }

    #[test]
    fn register_guide_from_json() {
        let registry = ToolRegistry::new();
        registry.register(ReadTool::new()).unwrap();

        let json_spec = r#"{
            "tool_name": "Read",
            "when_to_use": "Read small files",
            "when_not_to_use": "Don't read large files",
            "examples": [],
            "related_tools": [],
            "category": "FileReading"
        }"#;

        registry
            .register_guide_from_json("Read", json_spec)
            .unwrap();

        let guide = registry.get_guide("Read").unwrap();
        assert_eq!(guide.when_to_use(), "Read small files");
    }

    #[test]
    fn guide_removed_when_tool_unregistered() {
        let registry = ToolRegistry::new();
        registry
            .register_with_guide(ReadTool::new(), MockGuide)
            .unwrap();

        registry.unregister("Read");

        assert!(!registry.contains("Read"));
        assert!(registry.get_guide("Read").is_none());
    }
}
