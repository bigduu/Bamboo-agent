use serde::{Deserialize, Serialize};

/// Command type enumeration for categorizing different command sources.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum CommandType {
    /// Workflow commands from markdown files.
    Workflow,
    /// Skill commands defined in the skill system.
    Skill,
    /// MCP (Model Context Protocol) tool commands.
    Mcp,
}

/// Represents a unified command item from workflows, skills, and MCP tools.
#[derive(Debug, Serialize)]
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

/// Response structure for listing all available commands.
#[derive(Debug, Serialize)]
pub struct CommandListResponse {
    pub commands: Vec<CommandItem>,
    pub total: usize,
}
