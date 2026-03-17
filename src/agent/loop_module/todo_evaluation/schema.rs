use crate::agent::core::tools::{FunctionSchema, ToolSchema};
use serde_json::json;

/// 获取 TodoList 评估的 tool schemas
pub fn get_todo_evaluation_tools() -> Vec<ToolSchema> {
    vec![ToolSchema {
        schema_type: "function".to_string(),
        function: FunctionSchema {
            name: "update_todo_item".to_string(),
            description: "Update the status of a todo item based on evaluation".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "item_id": {
                        "type": "string",
                        "description": "The ID of the todo item to update"
                    },
                    "status": {
                        "type": "string",
                        "enum": ["completed", "blocked"],
                        "description": "New status for the item"
                    },
                    "notes": {
                        "type": "string",
                        "description": "Brief explanation of why the status changed"
                    }
                },
                "required": ["item_id", "status"]
            }),
        },
    }]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_todo_evaluation_tools_returns_tool() {
        let tools = get_todo_evaluation_tools();
        assert_eq!(tools.len(), 1);
    }

    #[test]
    fn test_get_todo_evaluation_tools_schema_type() {
        let tools = get_todo_evaluation_tools();
        assert_eq!(tools[0].schema_type, "function");
    }

    #[test]
    fn test_get_todo_evaluation_tools_function_name() {
        let tools = get_todo_evaluation_tools();
        assert_eq!(tools[0].function.name, "update_todo_item");
    }

    #[test]
    fn test_get_todo_evaluation_tools_function_description() {
        let tools = get_todo_evaluation_tools();
        assert!(tools[0].function.description.contains("Update"));
        assert!(tools[0].function.description.contains("todo item"));
    }

    #[test]
    fn test_get_todo_evaluation_tools_parameters_structure() {
        let tools = get_todo_evaluation_tools();
        let params = &tools[0].function.parameters;

        assert_eq!(params["type"], "object");
        assert!(params["properties"].is_object());
    }

    #[test]
    fn test_get_todo_evaluation_tools_item_id_parameter() {
        let tools = get_todo_evaluation_tools();
        let item_id = &tools[0].function.parameters["properties"]["item_id"];

        assert_eq!(item_id["type"], "string");
        assert!(item_id["description"].is_string());
    }

    #[test]
    fn test_get_todo_evaluation_tools_status_parameter() {
        let tools = get_todo_evaluation_tools();
        let status = &tools[0].function.parameters["properties"]["status"];

        assert_eq!(status["type"], "string");
        assert!(status["enum"].is_array());

        let enum_values = status["enum"].as_array().unwrap();
        assert!(enum_values.contains(&json!("completed")));
        assert!(enum_values.contains(&json!("blocked")));
    }

    #[test]
    fn test_get_todo_evaluation_tools_notes_parameter() {
        let tools = get_todo_evaluation_tools();
        let notes = &tools[0].function.parameters["properties"]["notes"];

        assert_eq!(notes["type"], "string");
        assert!(notes["description"].is_string());
    }

    #[test]
    fn test_get_todo_evaluation_tools_required_fields() {
        let tools = get_todo_evaluation_tools();
        let required = tools[0].function.parameters["required"].as_array().unwrap();

        assert_eq!(required.len(), 2);
        assert!(required.contains(&json!("item_id")));
        assert!(required.contains(&json!("status")));
        assert!(!required.contains(&json!("notes"))); // notes is optional
    }

    #[test]
    fn test_get_todo_evaluation_tools_all_properties() {
        let tools = get_todo_evaluation_tools();
        let props = tools[0].function.parameters["properties"]
            .as_object()
            .unwrap();

        assert_eq!(props.len(), 3);
        assert!(props.contains_key("item_id"));
        assert!(props.contains_key("status"));
        assert!(props.contains_key("notes"));
    }
}
