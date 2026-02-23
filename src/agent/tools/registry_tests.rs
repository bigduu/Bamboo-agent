//! Integration tests for Tool Registry

#[cfg(test)]
mod tests {
    use agent_core::tools::{RegistryError, Tool, ToolError, ToolRegistry, ToolResult};
    use async_trait::async_trait;
    use serde_json::json;

    // Test tool implementations
    struct TestTool {
        name: String,
        description: String,
        should_fail: bool,
    }

    impl TestTool {
        fn new(name: &str, description: &str) -> Self {
            Self {
                name: name.to_string(),
                description: description.to_string(),
                should_fail: false,
            }
        }

        fn with_failure(name: &str, description: &str) -> Self {
            Self {
                name: name.to_string(),
                description: description.to_string(),
                should_fail: true,
            }
        }
    }

    #[async_trait]
    impl Tool for TestTool {
        fn name(&self) -> &str {
            &self.name
        }

        fn description(&self) -> &str {
            &self.description
        }

        fn parameters_schema(&self) -> serde_json::Value {
            json!({
                "type": "object",
                "properties": {
                    "input": {
                        "type": "string",
                        "description": "Input parameter"
                    }
                },
                "required": ["input"]
            })
        }

        async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
            if self.should_fail {
                return Err(ToolError::Execution("Intentional failure".to_string()));
            }

            let input = args["input"].as_str().unwrap_or("default");
            Ok(ToolResult {
                success: true,
                result: format!("Processed: {}", input),
                display_preference: Some("text".to_string()),
            })
        }
    }

    // Another test tool for variety
    struct CalculatorTool;

    #[async_trait]
    impl Tool for CalculatorTool {
        fn name(&self) -> &str {
            "calculator"
        }

        fn description(&self) -> &str {
            "Performs basic arithmetic operations"
        }

        fn parameters_schema(&self) -> serde_json::Value {
            json!({
                "type": "object",
                "properties": {
                    "a": { "type": "number", "description": "First operand" },
                    "b": { "type": "number", "description": "Second operand" },
                    "operation": {
                        "type": "string",
                        "enum": ["add", "subtract", "multiply", "divide"],
                        "description": "Operation to perform"
                    }
                },
                "required": ["a", "b", "operation"]
            })
        }

        async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
            let a = args["a"]
                .as_f64()
                .ok_or_else(|| ToolError::InvalidArguments("Missing or invalid 'a'".to_string()))?;
            let b = args["b"]
                .as_f64()
                .ok_or_else(|| ToolError::InvalidArguments("Missing or invalid 'b'".to_string()))?;
            let op = args["operation"]
                .as_str()
                .ok_or_else(|| ToolError::InvalidArguments("Missing 'operation'".to_string()))?;

            let result = match op {
                "add" => a + b,
                "subtract" => a - b,
                "multiply" => a * b,
                "divide" => {
                    if b == 0.0 {
                        return Err(ToolError::Execution("Division by zero".to_string()));
                    }
                    a / b
                }
                _ => {
                    return Err(ToolError::InvalidArguments(format!(
                        "Unknown operation: {}",
                        op
                    )))
                }
            };

            Ok(ToolResult {
                success: true,
                result: result.to_string(),
                display_preference: None,
            })
        }
    }

    mod tool_registration {
        use super::*;

        #[tokio::test]
        async fn test_tool_registration() {
            let registry = ToolRegistry::new();
            let tool = TestTool::new("test_tool", "A test tool");

            // Test registration succeeds
            let result = registry.register(tool);
            assert!(result.is_ok(), "Tool registration should succeed");

            // Test tool is retrievable
            let retrieved = registry.get("test_tool");
            assert!(retrieved.is_some(), "Registered tool should be retrievable");
            assert_eq!(retrieved.unwrap().name(), "test_tool");
        }

        #[tokio::test]
        async fn test_duplicate_tool_registration() {
            let registry = ToolRegistry::new();
            let tool1 = TestTool::new("unique_tool", "First tool");
            let tool2 = TestTool::new("unique_tool", "Duplicate tool");

            registry.register(tool1).unwrap();

            // Duplicate registration should fail
            let result = registry.register(tool2);
            assert!(
                matches!(result, Err(RegistryError::DuplicateTool(name)) if name == "unique_tool")
            );
        }

        #[tokio::test]
        async fn test_multiple_tools_registration() {
            let registry = ToolRegistry::new();

            registry
                .register(TestTool::new("tool_a", "Tool A"))
                .unwrap();
            registry
                .register(TestTool::new("tool_b", "Tool B"))
                .unwrap();
            registry.register(CalculatorTool).unwrap();

            assert_eq!(registry.len(), 3);
            assert!(registry.contains("tool_a"));
            assert!(registry.contains("tool_b"));
            assert!(registry.contains("calculator"));
        }

        #[tokio::test]
        async fn test_tool_unregistration() {
            let registry = ToolRegistry::new();
            registry
                .register(TestTool::new("temp_tool", "Temporary"))
                .unwrap();

            assert!(registry.contains("temp_tool"));

            let removed = registry.unregister("temp_tool");
            assert!(removed, "Unregister should return true for existing tool");
            assert!(!registry.contains("temp_tool"));

            // Unregistering non-existent tool returns false
            let removed_again = registry.unregister("temp_tool");
            assert!(!removed_again);
        }

        #[tokio::test]
        async fn test_registry_clear() {
            let registry = ToolRegistry::new();
            registry.register(TestTool::new("tool1", "Tool 1")).unwrap();
            registry.register(TestTool::new("tool2", "Tool 2")).unwrap();

            assert_eq!(registry.len(), 2);

            registry.clear();

            assert_eq!(registry.len(), 0);
            assert!(registry.is_empty());
        }
    }

    mod tool_execution {
        use super::*;

        #[tokio::test]
        async fn test_tool_execution() {
            let registry = ToolRegistry::new();
            registry
                .register(TestTool::new("processor", "Data processor"))
                .unwrap();

            let tool = registry.get("processor").unwrap();
            let result = tool.execute(json!({"input": "hello"})).await;

            assert!(result.is_ok());
            let tool_result = result.unwrap();
            assert!(tool_result.success);
            assert_eq!(tool_result.result, "Processed: hello");
        }

        #[tokio::test]
        async fn test_tool_execution_failure() {
            let registry = ToolRegistry::new();
            registry
                .register(TestTool::with_failure("failing_tool", "Always fails"))
                .unwrap();

            let tool = registry.get("failing_tool").unwrap();
            let result = tool.execute(json!({})).await;

            assert!(result.is_err());
            assert!(matches!(result, Err(ToolError::Execution(_))));
        }

        #[tokio::test]
        async fn test_calculator_add() {
            let registry = ToolRegistry::new();
            registry.register(CalculatorTool).unwrap();

            let tool = registry.get("calculator").unwrap();
            let result = tool
                .execute(json!({
                    "a": 10.0,
                    "b": 5.0,
                    "operation": "add"
                }))
                .await
                .unwrap();

            assert!(result.success);
            assert_eq!(result.result, "15");
        }

        #[tokio::test]
        async fn test_calculator_divide_by_zero() {
            let registry = ToolRegistry::new();
            registry.register(CalculatorTool).unwrap();

            let tool = registry.get("calculator").unwrap();
            let result = tool
                .execute(json!({
                    "a": 10.0,
                    "b": 0.0,
                    "operation": "divide"
                }))
                .await;

            assert!(result.is_err());
        }

        #[tokio::test]
        async fn test_invalid_arguments() {
            let registry = ToolRegistry::new();
            registry.register(CalculatorTool).unwrap();

            let tool = registry.get("calculator").unwrap();
            let result = tool
                .execute(json!({
                    "a": "not a number",
                    "b": 5.0,
                    "operation": "add"
                }))
                .await;

            assert!(result.is_err());
            assert!(matches!(result, Err(ToolError::InvalidArguments(_))));
        }
    }

    mod tool_schema_generation {
        use super::*;

        #[tokio::test]
        async fn test_tool_schema_generation() {
            let registry = ToolRegistry::new();
            registry
                .register(TestTool::new("schema_test", "Schema test tool"))
                .unwrap();

            let tool = registry.get("schema_test").unwrap();
            let schema = tool.to_schema();

            assert_eq!(schema.schema_type, "function");
            assert_eq!(schema.function.name, "schema_test");
            assert_eq!(schema.function.description, "Schema test tool");
            assert!(schema.function.parameters.get("properties").is_some());
        }

        #[tokio::test]
        async fn test_list_tools() {
            let registry = ToolRegistry::new();
            registry
                .register(TestTool::new("tool1", "First tool"))
                .unwrap();
            registry
                .register(TestTool::new("tool2", "Second tool"))
                .unwrap();

            let tools = registry.list_tools();
            assert_eq!(tools.len(), 2);

            let names: Vec<String> = tools.iter().map(|s| s.function.name.clone()).collect();
            assert!(names.contains(&"tool1".to_string()));
            assert!(names.contains(&"tool2".to_string()));
        }

        #[tokio::test]
        async fn test_list_tool_names() {
            let registry = ToolRegistry::new();
            registry
                .register(TestTool::new("alpha", "Alpha tool"))
                .unwrap();
            registry
                .register(TestTool::new("beta", "Beta tool"))
                .unwrap();

            let names = registry.list_tool_names();
            assert_eq!(names.len(), 2);
            assert!(names.contains(&"alpha".to_string()));
            assert!(names.contains(&"beta".to_string()));
        }

        #[tokio::test]
        async fn test_calculator_schema() {
            let registry = ToolRegistry::new();
            registry.register(CalculatorTool).unwrap();

            let tool = registry.get("calculator").unwrap();
            let schema = tool.to_schema();

            assert_eq!(schema.function.name, "calculator");

            let params = &schema.function.parameters;
            let properties = params.get("properties").unwrap();
            assert!(properties.get("a").is_some());
            assert!(properties.get("b").is_some());
            assert!(properties.get("operation").is_some());

            let operation = properties.get("operation").unwrap();
            let enum_values = operation.get("enum").unwrap().as_array().unwrap();
            assert_eq!(enum_values.len(), 4);
        }
    }

    mod registry_edge_cases {
        use super::*;
        use agent_core::tools::normalize_tool_name;

        #[tokio::test]
        async fn test_empty_registry() {
            let registry = ToolRegistry::new();
            assert!(registry.is_empty());
            assert_eq!(registry.len(), 0);
            assert!(registry.get("anything").is_none());
        }

        #[tokio::test]
        async fn test_get_nonexistent_tool() {
            let registry = ToolRegistry::new();
            registry
                .register(TestTool::new("exists", "Exists"))
                .unwrap();

            assert!(registry.get("exists").is_some());
            assert!(registry.get("does_not_exist").is_none());
        }

        #[tokio::test]
        async fn test_normalize_tool_name() {
            assert_eq!(normalize_tool_name("simple_tool"), "simple_tool");
            assert_eq!(normalize_tool_name("default::tool_name"), "tool_name");
            assert_eq!(normalize_tool_name("a::b::c::tool"), "tool");
            assert_eq!(normalize_tool_name(""), "");
        }

        #[tokio::test]
        async fn test_concurrent_registration() {
            let registry = std::sync::Arc::new(ToolRegistry::new());
            let mut handles = vec![];

            for i in 0..10 {
                let reg = registry.clone();
                let handle = tokio::spawn(async move {
                    let tool = TestTool::new(&format!("concurrent_tool_{}", i), "Concurrent tool");
                    reg.register(tool)
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.await.unwrap().unwrap();
            }

            assert_eq!(registry.len(), 10);
        }
    }
}
