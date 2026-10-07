//! Parameter schemas derived from the same argument types used at execution.

pub(super) fn for_arguments<T: schemars::JsonSchema>() -> serde_json::Value {
    let mut schema = schemars::schema_for!(T);
    // Function parameters previously carried neither document metadata nor
    // the Rust struct name. Keep the provider-visible contract unchanged.
    schema.remove("$schema");
    schema.remove("title");
    schema.to_value()
}

#[cfg(test)]
mod tests {
    use bamboo_agent_core::Tool;
    use bamboo_llm::providers::common::tool_schema::sanitize_openai_function_parameters_schema;
    use serde_json::json;

    use crate::tools::{ViewImageTool, WriteTool};

    fn assert_unchanged(tool: &dyn Tool, previous: serde_json::Value) {
        let generated = tool.parameters_schema();
        assert_eq!(generated, previous);
        assert_eq!(
            sanitize_openai_function_parameters_schema(&generated).to_string(),
            sanitize_openai_function_parameters_schema(&previous).to_string()
        );
    }

    #[test]
    fn write_schema_preserves_existing_parameter_contract() {
        assert_unchanged(
            &WriteTool::new(),
            json!({
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "The absolute path to the file to write"
                    },
                    "content": {
                        "type": "string",
                        "description": "The content to write to the file"
                    }
                },
                "required": ["file_path", "content"],
                "additionalProperties": false
            }),
        );
    }

    #[test]
    fn view_image_schema_preserves_existing_parameter_contract() {
        assert_unchanged(
            &ViewImageTool::new(),
            json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Absolute path to a local PNG, JPEG, GIF, or WebP image"
                    }
                },
                "required": ["path"],
                "additionalProperties": false
            }),
        );
    }
}
