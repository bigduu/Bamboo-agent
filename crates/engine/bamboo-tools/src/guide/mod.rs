//! Tool guide system for enhanced LLM prompts
//!
//! This module provides a comprehensive system for generating enhanced tool usage
//! guidelines that help LLMs understand when and how to use different tools effectively.
//!
//! # Components
//!
//! - `ToolGuide` trait: Interface for defining tool usage guides
//! - `ToolGuideSpec`: Serializable guide specification
//! - `ToolExample`: Usage examples for tools
//! - `ToolCategory`: Categorization system for tools
//! - `EnhancedPromptBuilder`: Generates formatted prompt sections
//!
//! # Example
//!
//! ```rust,ignore
//! use bamboo_agent::tools::guide::context::GuideBuildContext;
//! use bamboo_agent::tools::guide::EnhancedPromptBuilder;
//! use bamboo_agent::tools::ToolRegistry;
//!
//! let registry = ToolRegistry::new();
//! let schemas = registry.list_tools();
//! let context = GuideBuildContext::default();
//!
//! let prompt = EnhancedPromptBuilder::build(Some(&registry), &schemas, &context);
//! println!("{}", prompt);
//! ```

use std::collections::BTreeSet;

use crate::exposure::{canonical_tool_name, has_full_tool_guide};
use bamboo_agent_core::ToolSchema;
use serde::{Deserialize, Serialize};

pub mod context;

use context::GuideBuildContext;

use crate::tools::ToolRegistry;

/// Represents a usage example for a tool
///
/// Each example demonstrates a specific scenario with parameters
/// and an explanation of the expected outcome.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolExample {
    /// Description of the scenario/use case
    pub scenario: String,

    /// Example parameters in JSON format
    pub parameters: serde_json::Value,

    /// Explanation of what this example does and why
    pub explanation: String,
}

impl ToolExample {
    /// Creates a new tool example
    ///
    /// # Arguments
    ///
    /// * `scenario` - Description of the use case
    /// * `parameters` - JSON parameters for the example
    /// * `explanation` - What this example accomplishes
    pub fn new(
        scenario: impl Into<String>,
        parameters: serde_json::Value,
        explanation: impl Into<String>,
    ) -> Self {
        Self {
            scenario: scenario.into(),
            parameters,
            explanation: explanation.into(),
        }
    }
}

/// Categories for organizing tools
///
/// Tools are grouped into logical categories to help LLMs understand
/// their purpose and choose the right tool for each task.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ToolCategory {
    /// Tools for reading files and understanding code
    FileReading,

    /// Tools for creating and modifying files
    FileWriting,

    /// Tools for searching code and text
    CodeSearch,

    /// Tools for running shell commands
    CommandExecution,

    /// Tools for Git operations
    GitOperations,

    /// Tools for managing tasks and workflows
    TaskManagement,

    /// Tools for interacting with the user
    UserInteraction,
}

impl ToolCategory {
    /// Order for presenting categories in prompts
    const ORDER: [ToolCategory; 7] = [
        ToolCategory::FileReading,
        ToolCategory::FileWriting,
        ToolCategory::CodeSearch,
        ToolCategory::CommandExecution,
        ToolCategory::GitOperations,
        ToolCategory::TaskManagement,
        ToolCategory::UserInteraction,
    ];

    /// Returns categories in their standard presentation order
    pub fn ordered() -> &'static [ToolCategory] {
        &Self::ORDER
    }
}

/// Trait for defining tool usage guides
///
/// Implement this trait to provide contextual guidance for tools,
/// helping LLMs understand when and how to use them effectively.
///
/// # Required Methods
///
/// - `tool_name`: Unique identifier for the tool
/// - `when_to_use`: Guidance on appropriate use cases
/// - `when_not_to_use`: Scenarios where the tool should be avoided
/// - `examples`: Concrete usage examples
/// - `related_tools`: Other tools that work well with this one
/// - `category`: Logical grouping for the tool
pub trait ToolGuide: Send + Sync {
    /// Returns the tool's unique name
    fn tool_name(&self) -> &str;

    /// Returns guidance on when this tool should be used
    fn when_to_use(&self) -> &str;

    /// Returns guidance on when this tool should NOT be used
    fn when_not_to_use(&self) -> &str;

    /// Returns usage examples for this tool
    fn examples(&self) -> Vec<ToolExample>;

    /// Returns names of related tools that complement this one
    fn related_tools(&self) -> Vec<&str>;

    /// Returns the category this tool belongs to
    fn category(&self) -> ToolCategory;
}

/// Serializable specification for a tool guide
///
/// This struct can be loaded from JSON or YAML files and implements
/// the `ToolGuide` trait for use in the prompt building system.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolGuideSpec {
    /// Unique tool identifier
    pub tool_name: String,

    /// When to use this tool
    pub when_to_use: String,

    /// When NOT to use this tool
    pub when_not_to_use: String,

    /// Usage examples
    pub examples: Vec<ToolExample>,

    /// Related tool names
    pub related_tools: Vec<String>,

    /// Tool category
    pub category: ToolCategory,
}

impl ToolGuideSpec {
    /// Creates a spec from a `ToolGuide` implementation
    ///
    /// # Arguments
    ///
    /// * `guide` - Reference to any type implementing `ToolGuide`
    pub fn from_guide(guide: &dyn ToolGuide) -> Self {
        Self {
            tool_name: guide.tool_name().to_string(),
            when_to_use: guide.when_to_use().to_string(),
            when_not_to_use: guide.when_not_to_use().to_string(),
            examples: guide.examples(),
            related_tools: guide
                .related_tools()
                .into_iter()
                .map(str::to_string)
                .collect(),
            category: guide.category(),
        }
    }

    /// Parses a guide spec from a JSON string
    ///
    /// # Errors
    ///
    /// Returns an error if the JSON is malformed or missing required fields
    pub fn from_json_str(raw: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(raw)
    }

    /// Parses a guide spec from a YAML string
    ///
    /// # Errors
    ///
    /// Returns an error if the YAML is malformed or missing required fields
    pub fn from_yaml_str(raw: &str) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(raw)
    }
}

impl ToolGuide for ToolGuideSpec {
    fn tool_name(&self) -> &str {
        &self.tool_name
    }

    fn when_to_use(&self) -> &str {
        &self.when_to_use
    }

    fn when_not_to_use(&self) -> &str {
        &self.when_not_to_use
    }

    fn examples(&self) -> Vec<ToolExample> {
        self.examples.clone()
    }

    fn related_tools(&self) -> Vec<&str> {
        self.related_tools.iter().map(String::as_str).collect()
    }

    fn category(&self) -> ToolCategory {
        self.category
    }
}

/// Supplements callable schemas with explicitly registered notes and shared rules.
pub struct EnhancedPromptBuilder;

impl EnhancedPromptBuilder {
    pub fn build(
        registry: Option<&ToolRegistry>,
        available_schemas: &[ToolSchema],
        context: &GuideBuildContext,
    ) -> String {
        let tool_names = available_schemas
            .iter()
            .map(|schema| schema.function.name.clone())
            .collect::<Vec<_>>();
        Self::build_for_tools(registry, &tool_names, available_schemas, context)
    }

    /// Render registered notes for the selected tools. Schema content travels
    /// through the provider's tool definitions.
    pub fn build_for_tools(
        registry: Option<&ToolRegistry>,
        tool_names: &[String],
        _fallback_schemas: &[ToolSchema],
        context: &GuideBuildContext,
    ) -> String {
        let names = tool_names
            .iter()
            .map(|name| name.as_str())
            .collect::<BTreeSet<_>>();
        let mut notes = String::new();
        for name in &names {
            let Some(guide) = registry.and_then(|registry| registry.get_guide(name)) else {
                continue;
            };
            if has_full_tool_guide(name)
                || context
                    .activated_discoverable_tools
                    .contains(&canonical_tool_name(name))
            {
                Self::render_full_guide(
                    &mut notes,
                    &ToolGuideSpec::from_guide(guide.as_ref()),
                    context,
                );
            }
        }
        if !names.is_empty() && context.include_best_practices {
            for rule in context.best_practices() {
                notes.push_str("- ");
                notes.push_str(rule);
                notes.push('\n');
            }
        }
        if notes.is_empty() {
            return notes;
        }
        format!("## Tool Guidance\n{}", notes.trim_end())
    }

    fn render_full_guide(output: &mut String, guide: &ToolGuideSpec, context: &GuideBuildContext) {
        output.push_str(&format!("\n**{}**\n", guide.tool_name));
        output.push_str("When to use\n");
        output.push_str(&guide.when_to_use);
        output.push('\n');
        if !guide.when_not_to_use.is_empty() {
            output.push_str("When NOT to use\n");
            output.push_str(&guide.when_not_to_use);
            output.push('\n');
        }
        for example in guide.examples.iter().take(context.max_examples_per_tool) {
            let params =
                serde_json::to_string(&example.parameters).unwrap_or_else(|_| "{}".to_string());
            output.push_str(&format!("- `{params}`\n  {}\n", example.explanation));
        }
        if !guide.related_tools.is_empty() {
            output.push_str(&format!(
                "Related tools include {}.\n",
                guide.related_tools.join(", ")
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{tools::SleepTool, BuiltinToolExecutor, ReadTool};
    use bamboo_agent_core::ToolExecutor;

    #[test]
    fn default_guide_supplements_schemas_with_only_shared_rules() {
        let executor = BuiltinToolExecutor::new();
        let schemas = executor.list_tools();
        let context = GuideBuildContext::default();
        let prompt = EnhancedPromptBuilder::build(None, &schemas, &context);
        assert!(prompt.len() < 640);
        for schema in schemas {
            assert!(!prompt.contains(&format!("**{}**", schema.function.name)));
            assert!(!prompt.contains(&schema.function.description));
        }
        assert!(!prompt.contains("Discoverable"));
        assert!(!prompt.contains("Example"));
        assert!(EnhancedPromptBuilder::build(None, &[], &context).is_empty());
    }

    #[test]
    fn custom_guides_follow_exposure_and_example_limits() {
        let registry = ToolRegistry::new();
        registry.register(SleepTool::new()).unwrap();
        let spec = ToolGuideSpec {
            tool_name: "Sleep".into(),
            when_to_use: "CUSTOM_SLEEP_NOTES".into(),
            when_not_to_use: String::new(),
            examples: vec![
                ToolExample::new("first", serde_json::json!({"seconds":1}), "FIRST_EXAMPLE"),
                ToolExample::new("second", serde_json::json!({"seconds":2}), "SECOND_EXAMPLE"),
            ],
            related_tools: vec![],
            category: ToolCategory::CommandExecution,
        };
        registry.register_guide("Sleep", spec).unwrap();
        let mut context = GuideBuildContext {
            include_best_practices: false,
            max_examples_per_tool: 1,
            ..Default::default()
        };
        let schemas = registry.list_tools();
        assert!(EnhancedPromptBuilder::build(Some(&registry), &schemas, &context).is_empty());
        context.activated_discoverable_tools.insert("Sleep".into());
        let prompt = EnhancedPromptBuilder::build(Some(&registry), &schemas, &context);
        assert!(prompt.contains("CUSTOM_SLEEP_NOTES"));
        assert!(prompt.contains("FIRST_EXAMPLE"));
        assert!(!prompt.contains("SECOND_EXAMPLE"));
        assert!(EnhancedPromptBuilder::build(Some(&registry), &[], &context).is_empty());
    }

    #[test]
    fn custom_resident_guides_are_available_without_activation() {
        let registry = ToolRegistry::new();
        registry.register(ReadTool::new()).unwrap();
        registry
            .register_guide(
                "Read",
                ToolGuideSpec {
                    tool_name: "Read".into(),
                    when_to_use: "CUSTOM_READ_NOTES".into(),
                    when_not_to_use: "CUSTOM_READ_AVOID".into(),
                    examples: vec![],
                    related_tools: vec![],
                    category: ToolCategory::FileReading,
                },
            )
            .unwrap();
        let prompt = EnhancedPromptBuilder::build(
            Some(&registry),
            &registry.list_tools(),
            &GuideBuildContext::default(),
        );
        assert!(prompt.contains("When to use\nCUSTOM_READ_NOTES"));
        assert!(prompt.contains("When NOT to use\nCUSTOM_READ_AVOID"));
        assert_eq!(prompt.matches("**Read**").count(), 1);
    }
}
