use bamboo_application_agent::tools::ToolResult;
use crate::error::AppError;

use super::models::{ToolExecutionResponse, ToolExecutionResultPayload};

pub(super) fn build_execution_response(
    requested_tool_name: String,
    result: ToolResult,
) -> Result<ToolExecutionResponse, AppError> {
    let display_preference = result
        .display_preference
        .unwrap_or_else(|| "Default".to_string());
    let payload = ToolExecutionResultPayload {
        tool_name: requested_tool_name,
        success: result.success,
        result: result.result,
        display_preference,
    };

    Ok(ToolExecutionResponse {
        result: serde_json::to_string(&payload).map_err(AppError::SerializationError)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_application_agent::tools::ToolResult;

    fn create_tool_result(result: &str, display_preference: Option<&str>) -> ToolResult {
        ToolResult {
            success: true,
            result: result.to_string(),
            display_preference: display_preference.map(|s| s.to_string()),
        }
    }

    #[test]
    fn build_execution_response_with_display_preference() {
        let tool_result = create_tool_result("Test result", Some("custom"));
        let response = build_execution_response("TestTool".to_string(), tool_result);

        assert!(response.is_ok());
        let response = response.unwrap();

        let payload: ToolExecutionResultPayload =
            serde_json::from_str(&response.result).expect("Should deserialize");

        assert_eq!(payload.tool_name, "TestTool");
        assert_eq!(payload.result, "Test result");
        assert_eq!(payload.display_preference, "custom");
    }

    #[test]
    fn build_execution_response_without_display_preference_uses_default() {
        let tool_result = create_tool_result("Result", None);
        let response = build_execution_response("MyTool".to_string(), tool_result);

        assert!(response.is_ok());
        let response = response.unwrap();

        let payload: ToolExecutionResultPayload =
            serde_json::from_str(&response.result).expect("Should deserialize");

        assert_eq!(payload.display_preference, "Default");
    }

    #[test]
    fn build_execution_response_preserves_tool_name() {
        let tool_result = create_tool_result("result", None);
        let response = build_execution_response("SpecialTool123".to_string(), tool_result);

        assert!(response.is_ok());
        let response = response.unwrap();

        let payload: ToolExecutionResultPayload =
            serde_json::from_str(&response.result).expect("Should deserialize");

        assert_eq!(payload.tool_name, "SpecialTool123");
    }

    #[test]
    fn build_execution_response_preserves_result_content() {
        let content = "This is a complex result\nwith multiple lines\nand special chars: !@#$%";
        let tool_result = create_tool_result(content, None);
        let response = build_execution_response("Tool".to_string(), tool_result);

        assert!(response.is_ok());
        let response = response.unwrap();

        let payload: ToolExecutionResultPayload =
            serde_json::from_str(&response.result).expect("Should deserialize");

        assert_eq!(payload.result, content);
    }

    #[test]
    fn build_execution_response_returns_json_string() {
        let tool_result = create_tool_result("test", None);
        let response = build_execution_response("Tool".to_string(), tool_result);

        assert!(response.is_ok());
        let response = response.unwrap();

        // The result should be a valid JSON string
        assert!(response.result.starts_with("{"));
        assert!(response.result.ends_with("}"));
        assert!(response.result.contains("tool_name"));
        assert!(response.result.contains("result"));
        assert!(response.result.contains("display_preference"));
    }

    #[test]
    fn build_execution_response_handles_empty_result() {
        let tool_result = create_tool_result("", None);
        let response = build_execution_response("Tool".to_string(), tool_result);

        assert!(response.is_ok());
        let response = response.unwrap();

        let payload: ToolExecutionResultPayload =
            serde_json::from_str(&response.result).expect("Should deserialize");

        assert_eq!(payload.result, "");
    }

    #[test]
    fn build_execution_response_handles_unicode() {
        let tool_result = create_tool_result("结果 🎯", Some("显示"));
        let response = build_execution_response("工具".to_string(), tool_result);

        assert!(response.is_ok());
        let response = response.unwrap();

        let payload: ToolExecutionResultPayload =
            serde_json::from_str(&response.result).expect("Should deserialize");

        assert_eq!(payload.tool_name, "工具");
        assert_eq!(payload.result, "结果 🎯");
        assert_eq!(payload.display_preference, "显示");
    }

    #[test]
    fn build_execution_response_handles_long_result() {
        let long_result = "Result line\n".repeat(100);
        let tool_result = create_tool_result(&long_result, None);
        let response = build_execution_response("Tool".to_string(), tool_result);

        assert!(response.is_ok());
        let response = response.unwrap();

        let payload: ToolExecutionResultPayload =
            serde_json::from_str(&response.result).expect("Should deserialize");

        assert_eq!(payload.result, long_result);
    }

    #[test]
    fn build_execution_response_handles_special_characters_in_tool_name() {
        let tool_result = create_tool_result("result", None);
        let response = build_execution_response("Tool-Name_123.test".to_string(), tool_result);

        assert!(response.is_ok());
        let response = response.unwrap();

        let payload: ToolExecutionResultPayload =
            serde_json::from_str(&response.result).expect("Should deserialize");

        assert_eq!(payload.tool_name, "Tool-Name_123.test");
    }

    #[test]
    fn build_execution_response_handles_json_in_result() {
        // Result containing JSON should be properly escaped in the output
        let json_result = r#"{"nested": "json", "number": 42}"#;
        let tool_result = create_tool_result(json_result, None);
        let response = build_execution_response("Tool".to_string(), tool_result);

        assert!(response.is_ok());
        let response = response.unwrap();

        let payload: ToolExecutionResultPayload =
            serde_json::from_str(&response.result).expect("Should deserialize");

        assert_eq!(payload.result, json_result);
    }

    #[test]
    fn build_execution_response_preserves_display_preference_exactly() {
        let custom_preference = "Custom-Preference_123";
        let tool_result = create_tool_result("result", Some(custom_preference));
        let response = build_execution_response("Tool".to_string(), tool_result);

        assert!(response.is_ok());
        let response = response.unwrap();

        let payload: ToolExecutionResultPayload =
            serde_json::from_str(&response.result).expect("Should deserialize");

        assert_eq!(payload.display_preference, custom_preference);
    }

    #[test]
    fn build_execution_response_can_be_deserialized_back() {
        let tool_result = create_tool_result("test result", Some("test_display"));
        let response = build_execution_response("TestTool".to_string(), tool_result).unwrap();

        // Parse the response.result string back into a payload
        let payload: Result<ToolExecutionResultPayload, _> = serde_json::from_str(&response.result);

        assert!(payload.is_ok());
        let payload = payload.unwrap();

        assert_eq!(payload.tool_name, "TestTool");
        assert_eq!(payload.result, "test result");
        assert_eq!(payload.display_preference, "test_display");
    }
}
