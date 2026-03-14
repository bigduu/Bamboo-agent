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
