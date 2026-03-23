use async_trait::async_trait;

use super::resolve_available_tool_schemas;
use crate::agent::core::tools::{FunctionSchema, ToolCall, ToolExecutor, ToolResult, ToolSchema};

struct StaticToolExecutor {
    schemas: Vec<ToolSchema>,
}

#[async_trait]
impl ToolExecutor for StaticToolExecutor {
    async fn execute(
        &self,
        _call: &ToolCall,
    ) -> crate::agent::core::tools::executor::Result<ToolResult> {
        Ok(ToolResult {
            success: true,
            result: "ok".to_string(),
            display_preference: None,
        })
    }

    fn list_tools(&self) -> Vec<ToolSchema> {
        self.schemas.clone()
    }
}

fn schema(name: &str) -> ToolSchema {
    ToolSchema {
        schema_type: "function".to_string(),
        function: FunctionSchema {
            name: name.to_string(),
            description: format!("{name} tool"),
            parameters: serde_json::json!({ "type": "object", "properties": {} }),
        },
    }
}

#[test]
fn resolve_available_tool_schemas_uses_executor_when_registry_empty() {
    let config = crate::agent::loop_module::config::AgentLoopConfig::default();
    let tools = StaticToolExecutor {
        schemas: vec![schema("z_tool"), schema("a_tool")],
    };

    let resolved = resolve_available_tool_schemas(&config, &tools);
    let names: Vec<&str> = resolved
        .iter()
        .map(|item| item.function.name.as_str())
        .collect();

    assert_eq!(names, vec!["a_tool", "z_tool"]);
}

#[test]
fn resolve_available_tool_schemas_dedupes_and_merges_additional_entries() {
    let config = crate::agent::loop_module::config::AgentLoopConfig {
        additional_tool_schemas: vec![schema("b_tool"), schema("a_tool")],
        ..Default::default()
    };
    let tools = StaticToolExecutor {
        schemas: vec![schema("a_tool"), schema("z_tool")],
    };

    let resolved = resolve_available_tool_schemas(&config, &tools);
    let names: Vec<&str> = resolved
        .iter()
        .map(|item| item.function.name.as_str())
        .collect();

    assert_eq!(names, vec!["a_tool", "b_tool", "z_tool"]);
}

#[test]
fn resolve_available_tool_schemas_excludes_disabled_tools() {
    let config = crate::agent::loop_module::config::AgentLoopConfig {
        additional_tool_schemas: vec![schema("b_tool")],
        disabled_tools: ["a_tool".to_string(), "b_tool".to_string()]
            .into_iter()
            .collect(),
        ..Default::default()
    };
    let tools = StaticToolExecutor {
        schemas: vec![schema("a_tool"), schema("z_tool")],
    };

    let resolved = resolve_available_tool_schemas(&config, &tools);
    let names: Vec<&str> = resolved
        .iter()
        .map(|item| item.function.name.as_str())
        .collect();

    assert_eq!(names, vec!["z_tool"]);
}

#[test]
fn resolve_available_tool_schemas_excludes_canonicalized_disabled_tool_aliases() {
    let config = crate::agent::loop_module::config::AgentLoopConfig {
        disabled_tools: ["Bash".to_string(), "Read".to_string()]
            .into_iter()
            .collect(),
        ..Default::default()
    };
    let tools = StaticToolExecutor {
        schemas: vec![schema("Bash"), schema("Read"), schema("Write")],
    };

    let resolved = resolve_available_tool_schemas(&config, &tools);
    let names: Vec<&str> = resolved
        .iter()
        .map(|item| item.function.name.as_str())
        .collect();

    assert_eq!(names, vec!["Write"]);
}
