//! Parameter schemas derived from the same argument types used at execution.

pub(super) fn for_arguments<T: schemars::JsonSchema>() -> serde_json::Value {
    let mut schema = schemars::schema_for!(T);
    // Function parameters previously carried neither document metadata nor
    // the Rust struct name. Keep the provider-visible contract unchanged.
    schema.remove("$schema");
    schema.remove("title");
    schema.to_value()
}

// Read/Glob/Grep historically advertise a plain number while serde parses usize.
// Keep that provider contract without integer bounds or a float format.
pub(super) fn number(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({"type": "number"})
}

#[cfg(test)]
mod tests {
    use bamboo_agent_core::{Message, Tool};
    use bamboo_llm::providers::anthropic::build_anthropic_request_with_cache_blocks;
    use bamboo_llm::providers::common::tool_schema::sanitize_openai_function_parameters_schema;
    use serde_json::json;

    use crate::tools::{GlobTool, GrepTool, ReadTool, ViewImageTool, WriteTool};

    fn anthropic_input_schema(
        tool: &dyn Tool,
        parameters: &serde_json::Value,
    ) -> serde_json::Value {
        let mut schema = tool.to_schema();
        schema.function.parameters = parameters.clone();
        let request = build_anthropic_request_with_cache_blocks(
            &[Message::user("Inspect the workspace")],
            &[],
            &[schema],
            "claude-test",
            64,
            false,
            None,
            None,
            None,
            false,
        );
        let tools = request["tools"].as_array().expect("wire tools");
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["name"], tool.name());
        assert_eq!(tools[0]["description"], tool.description());
        serde_json::Value::Object(
            tools[0]["input_schema"]
                .as_object()
                .expect("wire input_schema")
                .clone(),
        )
    }

    fn assert_unchanged(tool: &dyn Tool, previous: serde_json::Value) {
        let generated = tool.parameters_schema();
        assert_eq!(generated, previous);
        assert_eq!(
            sanitize_openai_function_parameters_schema(&generated).to_string(),
            sanitize_openai_function_parameters_schema(&previous).to_string()
        );
        assert_eq!(
            anthropic_input_schema(tool, &generated).to_string(),
            anthropic_input_schema(tool, &previous).to_string()
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

    #[test]
    fn read_schema_preserves_existing_parameter_contract() {
        assert_unchanged(
            &ReadTool::new(),
            json!({
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "The absolute path to the file or directory to read"
                    },
                    "offset": {
                        "type": "number",
                        "description": "The line offset to start reading from. Omit when you want the full file or directory listing."
                    },
                    "limit": {
                        "type": "number",
                        "description": "The maximum number of lines or directory entries to read. Omit for the full result when safe."
                    }
                },
                "required": ["file_path"],
                "additionalProperties": false
            }),
        );
    }

    #[test]
    fn glob_schema_preserves_existing_parameter_contract() {
        assert_unchanged(
            &GlobTool::new(),
            json!({
                "type": "object",
                "properties": {
                    "pattern": {
                        "type": "string",
                        "description": "The glob pattern to match files against (for example **/*.rs or src/**/*.ts)"
                    },
                    "path": {
                        "type": "string",
                        "description": "The directory to search in. Omit to use the current workspace root."
                    },
                    "limit": {
                        "type": "number",
                        "description": "Maximum number of returned matches (default 100, hard cap 200). Use a smaller limit for broad searches."
                    },
                    "include_ignored": {
                        "type": "boolean",
                        "default": false,
                        "description": "Include gitignored files. Requires an explicit path; scan/result limits and fixed directory exclusions still apply."
                    }
                },
                "required": ["pattern"],
                "additionalProperties": false
            }),
        );
    }

    #[test]
    fn grep_schema_preserves_existing_parameter_contract() {
        assert_unchanged(
            &GrepTool::new(),
            json!({
                "type": "object",
                "properties": {
                    "pattern": { "type": "string", "description": "Regex pattern" },
                    "path": { "type": "string", "description": "File or directory to search. An explicit file bypasses ignore rules. Narrow this for expensive or multiline searches." },
                    "glob": { "type": "string", "description": "Glob file filter used to limit candidate files" },
                    "output_mode": {
                        "type": "string",
                        "enum": ["content", "files_with_matches", "count"],
                        "description": "Output mode. Prefer files_with_matches for broad discovery, then refine with Read or content mode."
                    },
                    "-B": { "type": "number", "description": "Lines before match" },
                    "-A": { "type": "number", "description": "Lines after match" },
                    "-C": { "type": "number", "description": "Lines before and after match" },
                    "-n": { "type": "boolean", "description": "Show line numbers" },
                    "-i": { "type": "boolean", "description": "Case insensitive" },
                    "type": { "type": "string", "description": "File type filter (for example rust, js, ts, py)" },
                    "head_limit": { "type": "number", "description": "Limit output entries. Keep this small for broad queries." },
                    "multiline": { "type": "boolean", "description": "Enable multiline regex. Requires a narrowed path." },
                    "include_ignored": { "type": "boolean", "default": false, "description": "Include gitignored files. Requires an explicit path; scan/result limits and fixed directory exclusions still apply." }
                },
                "required": ["pattern"],
                "additionalProperties": false
            }),
        );
    }
}
