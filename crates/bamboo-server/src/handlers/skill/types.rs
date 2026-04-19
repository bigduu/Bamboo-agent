use bamboo_engine::SkillDefinition;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize)]
pub(super) struct SkillListResponse {
    pub(super) skills: Vec<SkillDefinition>,
    pub(super) total: usize,
}

#[derive(Deserialize)]
pub struct ListSkillsQuery {
    pub(super) search: Option<String>,
    pub(super) refresh: Option<bool>,
    pub(super) include_disabled: Option<bool>,
}

#[derive(Serialize)]
pub(super) struct AvailableToolsResponse {
    pub(super) tools: Vec<String>,
}

#[derive(Serialize)]
pub(super) struct FilteredToolsResponse {
    pub(super) tools: Vec<OpenAiTool>,
}

#[derive(Serialize)]
pub(super) struct OpenAiTool {
    #[serde(rename = "type")]
    pub(super) tool_type: String,
    pub(super) function: OpenAiFunction,
}

#[derive(Serialize)]
pub(super) struct OpenAiFunction {
    pub(super) name: String,
    pub(super) description: String,
    pub(super) parameters: Value,
}

#[derive(Serialize)]
pub(super) struct AvailableWorkflowsResponse {
    pub(super) workflows: Vec<String>,
}

#[derive(Deserialize)]
pub struct FilteredToolsQuery {
    pub(super) session_id: Option<String>,
    pub(super) chat_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_skill_list_response_serialization() {
        let response = SkillListResponse {
            skills: vec![],
            total: 0,
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"skills\":[]"));
        assert!(json.contains("\"total\":0"));
    }

    #[test]
    fn test_list_skills_query_deserialization() {
        let json = r#"{"search":"test"}"#;
        let query: ListSkillsQuery = serde_json::from_str(json).unwrap();

        assert_eq!(query.search, Some("test".to_string()));
        assert_eq!(query.refresh, None);
    }

    #[test]
    fn test_list_skills_query_with_refresh() {
        let json = r#"{"search":"workflow","refresh":true}"#;
        let query: ListSkillsQuery = serde_json::from_str(json).unwrap();

        assert_eq!(query.search, Some("workflow".to_string()));
        assert_eq!(query.refresh, Some(true));
        assert_eq!(query.include_disabled, None);
    }

    #[test]
    fn test_list_skills_query_with_include_disabled() {
        let json = r#"{"search":"workflow","refresh":true,"include_disabled":true}"#;
        let query: ListSkillsQuery = serde_json::from_str(json).unwrap();

        assert_eq!(query.search, Some("workflow".to_string()));
        assert_eq!(query.refresh, Some(true));
        assert_eq!(query.include_disabled, Some(true));
    }

    #[test]
    fn test_list_skills_query_empty() {
        let json = r#"{}"#;
        let query: ListSkillsQuery = serde_json::from_str(json).unwrap();

        assert_eq!(query.search, None);
        assert_eq!(query.refresh, None);
    }

    #[test]
    fn test_available_tools_response_serialization() {
        let response = AvailableToolsResponse {
            tools: vec!["bash".to_string(), "read".to_string()],
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("bash"));
        assert!(json.contains("read"));
    }

    #[test]
    fn test_available_tools_response_empty() {
        let response = AvailableToolsResponse { tools: vec![] };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"tools\":[]"));
    }

    #[test]
    fn test_open_ai_tool_serialization() {
        let tool = OpenAiTool {
            tool_type: "function".to_string(),
            function: OpenAiFunction {
                name: "read_file".to_string(),
                description: "Read file contents".to_string(),
                parameters: serde_json::json!({"type":"object"}),
            },
        };

        let json = serde_json::to_string(&tool).unwrap();
        assert!(json.contains("\"type\":\"function\""));
        assert!(json.contains("read_file"));
        assert!(json.contains("Read file contents"));
    }

    #[test]
    fn test_open_ai_function_serialization() {
        let func = OpenAiFunction {
            name: "bash".to_string(),
            description: "Execute bash command".to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "command": {"type": "string"}
                }
            }),
        };

        let json = serde_json::to_string(&func).unwrap();
        assert!(json.contains("bash"));
        assert!(json.contains("command"));
    }

    #[test]
    fn test_available_workflows_response() {
        let response = AvailableWorkflowsResponse {
            workflows: vec!["deploy".to_string(), "test".to_string()],
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("deploy"));
        assert!(json.contains("test"));
    }

    #[test]
    fn test_filtered_tools_query_with_session() {
        let json = r#"{"session_id":"sess-123"}"#;
        let query: FilteredToolsQuery = serde_json::from_str(json).unwrap();

        assert_eq!(query.session_id, Some("sess-123".to_string()));
        assert_eq!(query.chat_id, None);
    }

    #[test]
    fn test_filtered_tools_query_with_chat() {
        let json = r#"{"chat_id":"chat-456"}"#;
        let query: FilteredToolsQuery = serde_json::from_str(json).unwrap();

        assert_eq!(query.session_id, None);
        assert_eq!(query.chat_id, Some("chat-456".to_string()));
    }

    #[test]
    fn test_filtered_tools_query_both() {
        let json = r#"{"session_id":"sess-123","chat_id":"chat-456"}"#;
        let query: FilteredToolsQuery = serde_json::from_str(json).unwrap();

        assert_eq!(query.session_id, Some("sess-123".to_string()));
        assert_eq!(query.chat_id, Some("chat-456".to_string()));
    }

    #[test]
    fn test_filtered_tools_query_empty() {
        let json = r#"{}"#;
        let query: FilteredToolsQuery = serde_json::from_str(json).unwrap();

        assert_eq!(query.session_id, None);
        assert_eq!(query.chat_id, None);
    }

    #[test]
    fn test_filtered_tools_response_serialization() {
        let response = FilteredToolsResponse { tools: vec![] };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"tools\":[]"));
    }
}
