use serde::{Deserialize, Serialize};

/// Command type enumeration for categorizing different command sources.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum CommandType {
    /// Markdown commands and stored prompt presets.
    Prompt,
    /// Workflow commands from markdown files.
    Workflow,
    /// Skill commands defined in the skill system.
    Skill,
    /// MCP (Model Context Protocol) tool commands.
    Mcp,
}

/// Represents a unified command item from workflows, skills, and MCP tools.
#[derive(Debug, Serialize, Clone)]
pub struct CommandItem {
    pub id: String,
    pub name: String,
    pub display_name: String,
    pub description: String,
    #[serde(rename = "type")]
    pub command_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Default, Deserialize)]
pub struct ListCommandsQuery {
    #[serde(default)]
    pub workspace_path: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct GetCommandQuery {
    #[serde(default)]
    pub workspace_path: Option<String>,
    #[serde(default)]
    pub arguments: Option<String>,
}

/// Response structure for listing all available commands.
#[derive(Debug, Serialize)]
pub struct CommandListResponse {
    pub commands: Vec<CommandItem>,
    pub total: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_type_serialization() {
        let workflow = CommandType::Workflow;
        assert_eq!(serde_json::to_string(&workflow).unwrap(), "\"workflow\"");

        let prompt = CommandType::Prompt;
        assert_eq!(serde_json::to_string(&prompt).unwrap(), "\"prompt\"");

        let skill = CommandType::Skill;
        assert_eq!(serde_json::to_string(&skill).unwrap(), "\"skill\"");

        let mcp = CommandType::Mcp;
        assert_eq!(serde_json::to_string(&mcp).unwrap(), "\"mcp\"");
    }

    #[test]
    fn test_command_type_deserialization() {
        let workflow: CommandType = serde_json::from_str("\"workflow\"").unwrap();
        assert!(matches!(workflow, CommandType::Workflow));

        let skill: CommandType = serde_json::from_str("\"skill\"").unwrap();
        assert!(matches!(skill, CommandType::Skill));

        let mcp: CommandType = serde_json::from_str("\"mcp\"").unwrap();
        assert!(matches!(mcp, CommandType::Mcp));
    }

    #[test]
    fn test_command_type_clone() {
        let cmd = CommandType::Workflow;
        let cloned = cmd.clone();
        assert!(matches!(cloned, CommandType::Workflow));
    }

    #[test]
    fn test_command_type_debug() {
        let cmd = CommandType::Skill;
        let debug_str = format!("{:?}", cmd);
        assert!(debug_str.contains("Skill"));
    }

    #[test]
    fn test_command_item_serialization() {
        let item = CommandItem {
            id: "cmd-1".to_string(),
            name: "test_command".to_string(),
            display_name: "Test Command".to_string(),
            description: "A test".to_string(),
            command_type: "workflow".to_string(),
            category: Some("general".to_string()),
            tags: Some(vec!["test".to_string()]),
            metadata: serde_json::json!({"key": "value"}),
        };

        let json = serde_json::to_string(&item).unwrap();
        assert!(json.contains("cmd-1"));
        assert!(json.contains("test_command"));
        assert!(json.contains("Test Command"));
        assert!(json.contains("workflow"));
        assert!(json.contains("category"));
        assert!(json.contains("tags"));
    }

    #[test]
    fn test_command_item_skip_none_fields() {
        let item = CommandItem {
            id: "cmd-2".to_string(),
            name: "test".to_string(),
            display_name: "Test".to_string(),
            description: "Desc".to_string(),
            command_type: "skill".to_string(),
            category: None,
            tags: None,
            metadata: serde_json::json!(null),
        };

        let json = serde_json::to_string(&item).unwrap();
        assert!(!json.contains("category"));
        assert!(!json.contains("tags"));
    }

    #[test]
    fn test_command_item_debug() {
        let item = CommandItem {
            id: "id".to_string(),
            name: "name".to_string(),
            display_name: "Display".to_string(),
            description: "desc".to_string(),
            command_type: "mcp".to_string(),
            category: None,
            tags: None,
            metadata: serde_json::json!({}),
        };

        let debug_str = format!("{:?}", item);
        assert!(debug_str.contains("CommandItem"));
    }

    #[test]
    fn test_command_list_response_serialization() {
        let response = CommandListResponse {
            commands: vec![],
            total: 0,
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"commands\":[]"));
        assert!(json.contains("\"total\":0"));
    }

    #[test]
    fn test_command_list_response_with_commands() {
        let item = CommandItem {
            id: "1".to_string(),
            name: "cmd".to_string(),
            display_name: "Command".to_string(),
            description: "Test".to_string(),
            command_type: "workflow".to_string(),
            category: None,
            tags: None,
            metadata: serde_json::json!({}),
        };

        let response = CommandListResponse {
            commands: vec![item],
            total: 1,
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"total\":1"));
    }

    #[test]
    fn test_command_list_response_debug() {
        let response = CommandListResponse {
            commands: vec![],
            total: 0,
        };

        let debug_str = format!("{:?}", response);
        assert!(debug_str.contains("CommandListResponse"));
    }
}
