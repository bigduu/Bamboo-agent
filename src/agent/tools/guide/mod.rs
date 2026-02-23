use std::collections::{BTreeMap, BTreeSet};

use agent_core::tools::ToolSchema;
use serde::{Deserialize, Serialize};

pub mod builtin_guides;
pub mod context;

use builtin_guides::builtin_tool_guide;
use context::{GuideBuildContext, GuideLanguage};

use crate::tools::ToolRegistry;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolExample {
    pub scenario: String,
    pub parameters: serde_json::Value,
    pub explanation: String,
}

impl ToolExample {
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ToolCategory {
    FileReading,
    FileWriting,
    CodeSearch,
    CommandExecution,
    GitOperations,
    TaskManagement,
    UserInteraction,
}

impl ToolCategory {
    const ORDER: [ToolCategory; 7] = [
        ToolCategory::FileReading,
        ToolCategory::FileWriting,
        ToolCategory::CodeSearch,
        ToolCategory::CommandExecution,
        ToolCategory::GitOperations,
        ToolCategory::TaskManagement,
        ToolCategory::UserInteraction,
    ];

    pub fn ordered() -> &'static [ToolCategory] {
        &Self::ORDER
    }

    fn title(self, language: GuideLanguage) -> &'static str {
        match (self, language) {
            (ToolCategory::FileReading, GuideLanguage::Chinese) => "File Reading Tools",
            (ToolCategory::FileWriting, GuideLanguage::Chinese) => "File Writing Tools",
            (ToolCategory::CodeSearch, GuideLanguage::Chinese) => "Code Search Tools",
            (ToolCategory::CommandExecution, GuideLanguage::Chinese) => "Command Execution Tools",
            (ToolCategory::GitOperations, GuideLanguage::Chinese) => "Git Tools",
            (ToolCategory::TaskManagement, GuideLanguage::Chinese) => "Task Management Tools",
            (ToolCategory::UserInteraction, GuideLanguage::Chinese) => "User Interaction Tools",
            (ToolCategory::FileReading, GuideLanguage::English) => "File Reading Tools",
            (ToolCategory::FileWriting, GuideLanguage::English) => "File Writing Tools",
            (ToolCategory::CodeSearch, GuideLanguage::English) => "Code Search Tools",
            (ToolCategory::CommandExecution, GuideLanguage::English) => "Command Tools",
            (ToolCategory::GitOperations, GuideLanguage::English) => "Git Tools",
            (ToolCategory::TaskManagement, GuideLanguage::English) => "Task Management Tools",
            (ToolCategory::UserInteraction, GuideLanguage::English) => "User Interaction Tools",
        }
    }

    fn description(self, language: GuideLanguage) -> &'static str {
        match (self, language) {
            (ToolCategory::FileReading, GuideLanguage::Chinese) => {
                "Use these to understand existing files, directory structure, and metadata."
            }
            (ToolCategory::FileWriting, GuideLanguage::Chinese) => {
                "Use these to create files or make content modifications."
            }
            (ToolCategory::CodeSearch, GuideLanguage::Chinese) => {
                "Use these to locate definitions, references, and key text."
            }
            (ToolCategory::CommandExecution, GuideLanguage::Chinese) => {
                "Use these to run commands, confirm or switch working directories."
            }
            (ToolCategory::GitOperations, GuideLanguage::Chinese) => {
                "Use these to view repository status and code differences."
            }
            (ToolCategory::TaskManagement, GuideLanguage::Chinese) => {
                "Use these to break down tasks and track execution progress."
            }
            (ToolCategory::UserInteraction, GuideLanguage::Chinese) => {
                "Use this to confirm uncertain matters with the user."
            }
            (ToolCategory::FileReading, GuideLanguage::English) => {
                "Use these to inspect existing files and structure."
            }
            (ToolCategory::FileWriting, GuideLanguage::English) => {
                "Use these to create files and apply edits."
            }
            (ToolCategory::CodeSearch, GuideLanguage::English) => {
                "Use these to find symbols, references, and patterns."
            }
            (ToolCategory::CommandExecution, GuideLanguage::English) => {
                "Use these for shell commands and workspace context."
            }
            (ToolCategory::GitOperations, GuideLanguage::English) => {
                "Use these to inspect repository status and diffs."
            }
            (ToolCategory::TaskManagement, GuideLanguage::English) => {
                "Use these for planning and progress tracking."
            }
            (ToolCategory::UserInteraction, GuideLanguage::English) => {
                "Use this when user clarification is required."
            }
        }
    }
}

pub trait ToolGuide: Send + Sync {
    fn tool_name(&self) -> &str;
    fn when_to_use(&self) -> &str;
    fn when_not_to_use(&self) -> &str;
    fn examples(&self) -> Vec<ToolExample>;
    fn related_tools(&self) -> Vec<&str>;
    fn category(&self) -> ToolCategory;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolGuideSpec {
    pub tool_name: String,
    pub when_to_use: String,
    pub when_not_to_use: String,
    pub examples: Vec<ToolExample>,
    pub related_tools: Vec<String>,
    pub category: ToolCategory,
}

impl ToolGuideSpec {
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

    pub fn from_json_str(raw: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(raw)
    }

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

pub struct EnhancedPromptBuilder;

impl EnhancedPromptBuilder {
    pub fn build(
        registry: Option<&ToolRegistry>,
        available_schemas: &[ToolSchema],
        context: &GuideBuildContext,
    ) -> String {
        let mut tool_names: Vec<String> = available_schemas
            .iter()
            .map(|schema| schema.function.name.clone())
            .collect();
        tool_names.sort();
        tool_names.dedup();

        Self::build_for_tools(registry, &tool_names, available_schemas, context)
    }

    pub fn build_for_tools(
        registry: Option<&ToolRegistry>,
        tool_names: &[String],
        fallback_schemas: &[ToolSchema],
        context: &GuideBuildContext,
    ) -> String {
        let guides = Self::collect_guides(registry, tool_names);

        if guides.is_empty() {
            return Self::render_schema_only_section(fallback_schemas, context, true);
        }

        let mut output = String::from("## Tool Usage Guidelines\n");
        let mut grouped: BTreeMap<ToolCategory, Vec<&ToolGuideSpec>> = BTreeMap::new();

        for guide in &guides {
            grouped.entry(guide.category).or_default().push(guide);
        }

        for guides in grouped.values_mut() {
            guides.sort_by(|left, right| left.tool_name.cmp(&right.tool_name));
        }

        for category in ToolCategory::ordered() {
            let Some(category_guides) = grouped.get(category) else {
                continue;
            };

            output.push_str(&format!("\n### {}\n", category.title(context.language)));
            output.push_str(category.description(context.language));
            output.push('\n');

            for guide in category_guides {
                output.push_str(&format!("\n**{}**\n", guide.tool_name));
                output.push_str(&format!(
                    "- {}: {}\n",
                    when_to_use_label(context.language),
                    guide.when_to_use
                ));
                output.push_str(&format!(
                    "- {}: {}\n",
                    when_not_to_use_label(context.language),
                    guide.when_not_to_use
                ));

                for example in guide.examples.iter().take(context.max_examples_per_tool) {
                    let params = serde_json::to_string(&example.parameters)
                        .unwrap_or_else(|_| "{}".to_string());
                    output.push_str(&format!(
                        "- {}: {}\n  -> {}\n",
                        example_label(context.language),
                        params,
                        example.explanation
                    ));
                }

                if !guide.related_tools.is_empty() {
                    output.push_str(&format!(
                        "- {}: {}\n",
                        related_tools_label(context.language),
                        guide.related_tools.join(", ")
                    ));
                }
            }
        }

        let guided_names: BTreeSet<&str> = guides
            .iter()
            .map(|guide| guide.tool_name.as_str())
            .collect();
        let unguided_schemas: Vec<ToolSchema> = fallback_schemas
            .iter()
            .filter(|schema| !guided_names.contains(schema.function.name.as_str()))
            .cloned()
            .collect();

        if !unguided_schemas.is_empty() {
            output.push('\n');
            output.push_str(&Self::render_schema_only_section(
                &unguided_schemas,
                context,
                false,
            ));
        }

        if context.include_best_practices {
            output.push_str(&format!(
                "\n### {}\n",
                best_practices_title(context.language)
            ));
            for (index, rule) in context.best_practices().iter().enumerate() {
                output.push_str(&format!("{}. {}\n", index + 1, rule));
            }
        }

        output
    }

    fn collect_guides(registry: Option<&ToolRegistry>, tool_names: &[String]) -> Vec<ToolGuideSpec> {
        let mut seen = BTreeSet::new();
        let mut guides = Vec::new();

        for raw_name in tool_names {
            let name = raw_name.trim();
            if name.is_empty() || !seen.insert(name.to_string()) {
                continue;
            }

            let guide = registry
                .and_then(|registry| registry.get_guide(name))
                .or_else(|| builtin_tool_guide(name));

            if let Some(guide) = guide {
                guides.push(ToolGuideSpec::from_guide(guide.as_ref()));
            }
        }

        guides.sort_by(|left, right| left.tool_name.cmp(&right.tool_name));
        guides
    }

    fn render_schema_only_section(
        schemas: &[ToolSchema],
        context: &GuideBuildContext,
        include_header: bool,
    ) -> String {
        if schemas.is_empty() {
            return String::new();
        }

        let mut output = String::new();
        if include_header {
            output.push_str("## Tool Usage Guidelines\n");
        }

        output.push_str(&format!(
            "\n### {}\n",
            schema_only_title(context.language)
        ));
        output.push_str(schema_only_description(context.language));
        output.push('\n');

        let mut sorted = schemas.to_vec();
        sorted.sort_by(|left, right| left.function.name.cmp(&right.function.name));

        for schema in sorted {
            output.push_str(&format!(
                "- `{}`: {}\n",
                schema.function.name, schema.function.description
            ));
        }

        output
    }
}

fn when_to_use_label(language: GuideLanguage) -> &'static str {
    match language {
        GuideLanguage::Chinese => "When to use",
        GuideLanguage::English => "When to use",
    }
}

fn when_not_to_use_label(language: GuideLanguage) -> &'static str {
    match language {
        GuideLanguage::Chinese => "When NOT to use",
        GuideLanguage::English => "When NOT to use",
    }
}

fn example_label(language: GuideLanguage) -> &'static str {
    match language {
        GuideLanguage::Chinese => "Example",
        GuideLanguage::English => "Example",
    }
}

fn related_tools_label(language: GuideLanguage) -> &'static str {
    match language {
        GuideLanguage::Chinese => "Related tools",
        GuideLanguage::English => "Related tools",
    }
}

fn best_practices_title(language: GuideLanguage) -> &'static str {
    match language {
        GuideLanguage::Chinese => "Best Practices",
        GuideLanguage::English => "Best Practices",
    }
}

fn schema_only_title(language: GuideLanguage) -> &'static str {
    match language {
        GuideLanguage::Chinese => "Additional Tools (Schema Only)",
        GuideLanguage::English => "Additional Tools (Schema Only)",
    }
}

fn schema_only_description(language: GuideLanguage) -> &'static str {
    match language {
        GuideLanguage::Chinese => "No detailed guide is available for these tools; rely on schema.",
        GuideLanguage::English => "No detailed guide is available for these tools; rely on schema.",
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use agent_core::tools::{FunctionSchema, ToolSchema};

    use crate::tools::{ReadFileTool, ToolRegistry};

    use super::{context::GuideBuildContext, context::GuideLanguage, EnhancedPromptBuilder};

    #[test]
    fn build_renders_builtin_guides() {
        let registry = ToolRegistry::new();
        registry.register(ReadFileTool::new()).unwrap();

        let schemas = registry.list_tools();
        let prompt = EnhancedPromptBuilder::build(Some(&registry), &schemas, &GuideBuildContext::default());

        assert!(prompt.contains("## Tool Usage Guidelines"));
        assert!(prompt.contains("**read_file**"));
    }

    #[test]
    fn build_falls_back_to_schema_without_guides() {
        let schema = ToolSchema {
            schema_type: "function".to_string(),
            function: FunctionSchema {
                name: "dynamic_tool".to_string(),
                description: "A runtime tool".to_string(),
                parameters: json!({ "type": "object", "properties": {} }),
            },
        };
        let context = GuideBuildContext {
            language: GuideLanguage::English,
            ..GuideBuildContext::default()
        };

        let prompt = EnhancedPromptBuilder::build(None, &[schema], &context);

        assert!(prompt.contains("Additional Tools (Schema Only)"));
        assert!(prompt.contains("dynamic_tool"));
    }
}
