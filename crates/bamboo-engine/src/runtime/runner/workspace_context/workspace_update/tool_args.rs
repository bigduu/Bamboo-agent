use bamboo_agent_core::tools::ToolCall;

pub(super) fn normalized_tool_name(tool_call: &ToolCall) -> Option<String> {
    bamboo_tools::normalize_tool_ref(&tool_call.function.name)
}

pub(super) fn is_workspace_update_tool(tool_call: &ToolCall) -> Option<bool> {
    let normalized_tool_name = normalized_tool_name(tool_call)?;
    Some(
        normalized_tool_name == "SetWorkspace"
            || normalized_tool_name == "Workspace"
            || normalized_tool_name == "Write"
            || normalized_tool_name == "Edit"
            || normalized_tool_name == "NotebookEdit",
    )
}

pub(super) fn is_write_or_edit_tool_name(tool_name: String) -> bool {
    tool_name == "Write" || tool_name == "Edit" || tool_name == "NotebookEdit"
}

pub(super) fn extract_target_file_path_from_tool_call(tool_call: &ToolCall) -> Option<String> {
    let normalized_tool_name = normalized_tool_name(tool_call)?;
    let argument_key = match normalized_tool_name.as_str() {
        "Write" | "Edit" => "file_path",
        "NotebookEdit" => "notebook_path",
        _ => return None,
    };

    let args: serde_json::Value = serde_json::from_str(&tool_call.function.arguments).ok()?;
    args.get(argument_key)
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::tools::{FunctionCall, ToolCall};

    fn make_tool_call(name: &str, args: &str) -> ToolCall {
        ToolCall {
            id: "call-123".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: name.to_string(),
                arguments: args.to_string(),
            },
        }
    }

    #[test]
    fn test_is_write_or_edit_tool_name_write() {
        assert!(is_write_or_edit_tool_name("Write".to_string()));
    }

    #[test]
    fn test_is_write_or_edit_tool_name_edit() {
        assert!(is_write_or_edit_tool_name("Edit".to_string()));
    }

    #[test]
    fn test_is_write_or_edit_tool_name_notebook() {
        assert!(is_write_or_edit_tool_name("NotebookEdit".to_string()));
    }

    #[test]
    fn test_is_write_or_edit_tool_name_other() {
        assert!(!is_write_or_edit_tool_name("Bash".to_string()));
        assert!(!is_write_or_edit_tool_name("Read".to_string()));
    }

    #[test]
    fn test_extract_target_file_path_write() {
        let tool_call = make_tool_call("Write", r#"{"file_path":"/test.txt","content":"hello"}"#);
        let path = extract_target_file_path_from_tool_call(&tool_call);
        assert_eq!(path, Some("/test.txt".to_string()));
    }

    #[test]
    fn test_extract_target_file_path_edit() {
        let tool_call = make_tool_call(
            "Edit",
            r#"{"file_path":"/src/main.rs","old":"a","new":"b"}"#,
        );
        let path = extract_target_file_path_from_tool_call(&tool_call);
        assert_eq!(path, Some("/src/main.rs".to_string()));
    }

    #[test]
    fn test_extract_target_file_path_notebook() {
        let tool_call = make_tool_call(
            "NotebookEdit",
            r#"{"notebook_path":"/test.ipynb","cell_number":0}"#,
        );
        let path = extract_target_file_path_from_tool_call(&tool_call);
        assert_eq!(path, Some("/test.ipynb".to_string()));
    }

    #[test]
    fn test_extract_target_file_path_other_tool() {
        let tool_call = make_tool_call("Bash", r#"{"command":"ls"}"#);
        let path = extract_target_file_path_from_tool_call(&tool_call);
        assert!(path.is_none());
    }

    #[test]
    fn test_extract_target_file_path_empty() {
        let tool_call = make_tool_call("Write", r#"{"file_path":"   ","content":"x"}"#);
        let path = extract_target_file_path_from_tool_call(&tool_call);
        assert!(path.is_none()); // trimmed empty string filtered out
    }

    #[test]
    fn test_extract_target_file_path_invalid_json() {
        let tool_call = make_tool_call("Write", r#"invalid json"#);
        let path = extract_target_file_path_from_tool_call(&tool_call);
        assert!(path.is_none());
    }

    #[test]
    fn test_extract_target_file_path_missing_key() {
        let tool_call = make_tool_call("Write", r#"{"content":"test"}"#);
        let path = extract_target_file_path_from_tool_call(&tool_call);
        assert!(path.is_none());
    }

    #[test]
    fn test_extract_target_file_path_trims_whitespace() {
        let tool_call = make_tool_call(
            "Write",
            r#"{"file_path":"  /path/to/file  ","content":"x"}"#,
        );
        let path = extract_target_file_path_from_tool_call(&tool_call);
        assert_eq!(path, Some("/path/to/file".to_string()));
    }
}
