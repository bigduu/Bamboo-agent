use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

/// MCP tool metadata from server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpTool {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
}

/// Result of calling an MCP tool
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpCallResult {
    pub content: Vec<McpContentItem>,
    #[serde(default)]
    pub is_error: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum McpContentItem {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "image")]
    Image { data: String, mime_type: String },
    #[serde(rename = "resource")]
    Resource { resource: McpResource },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpResource {
    pub uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob: Option<String>,
}

/// Server runtime status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServerStatus {
    Connecting,
    Ready,
    Degraded,
    Stopped,
    Error,
}

impl std::fmt::Display for ServerStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerStatus::Connecting => write!(f, "connecting"),
            ServerStatus::Ready => write!(f, "ready"),
            ServerStatus::Degraded => write!(f, "degraded"),
            ServerStatus::Stopped => write!(f, "stopped"),
            ServerStatus::Error => write!(f, "error"),
        }
    }
}

/// Runtime information for an MCP server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeInfo {
    pub status: ServerStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connected_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disconnected_at: Option<DateTime<Utc>>,
    pub tool_count: usize,
    pub restart_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_ping_at: Option<DateTime<Utc>>,
}

impl Default for RuntimeInfo {
    fn default() -> Self {
        Self {
            status: ServerStatus::Stopped,
            last_error: None,
            connected_at: None,
            disconnected_at: None,
            tool_count: 0,
            restart_count: 0,
            last_ping_at: None,
        }
    }
}

/// Tool alias mapping
#[derive(Debug, Clone)]
pub struct ToolAlias {
    pub alias: String,
    pub server_id: String,
    pub original_name: String,
}

/// Event emitted by MCP manager
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum McpEvent {
    ServerStatusChanged {
        server_id: String,
        status: ServerStatus,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    ToolsChanged {
        server_id: String,
        tools: Vec<String>,
    },
    ToolExecuted {
        server_id: String,
        tool_name: String,
        success: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mcp_tool() {
        let tool = McpTool {
            name: "read_file".to_string(),
            description: "Read a file".to_string(),
            parameters: serde_json::json!({"type": "object"}),
        };
        assert_eq!(tool.name, "read_file");
        assert_eq!(tool.description, "Read a file");
    }

    #[test]
    fn test_mcp_call_result() {
        let result = McpCallResult {
            content: vec![McpContentItem::Text {
                text: "success".to_string(),
            }],
            is_error: false,
        };
        assert!(!result.is_error);
        assert_eq!(result.content.len(), 1);
    }

    #[test]
    fn test_mcp_call_result_error() {
        let result = McpCallResult {
            content: vec![McpContentItem::Text {
                text: "error occurred".to_string(),
            }],
            is_error: true,
        };
        assert!(result.is_error);
    }

    #[test]
    fn test_mcp_content_item_text() {
        let item = McpContentItem::Text {
            text: "hello".to_string(),
        };
        match item {
            McpContentItem::Text { text } => assert_eq!(text, "hello"),
            _ => panic!("Expected Text variant"),
        }
    }

    #[test]
    fn test_mcp_content_item_image() {
        let item = McpContentItem::Image {
            data: "base64data".to_string(),
            mime_type: "image/png".to_string(),
        };
        match item {
            McpContentItem::Image { data, mime_type } => {
                assert_eq!(data, "base64data");
                assert_eq!(mime_type, "image/png");
            }
            _ => panic!("Expected Image variant"),
        }
    }

    #[test]
    fn test_mcp_content_item_resource() {
        let item = McpContentItem::Resource {
            resource: McpResource {
                uri: "file:///test.txt".to_string(),
                mime_type: Some("text/plain".to_string()),
                text: Some("content".to_string()),
                blob: None,
            },
        };
        match item {
            McpContentItem::Resource { resource } => {
                assert_eq!(resource.uri, "file:///test.txt");
            }
            _ => panic!("Expected Resource variant"),
        }
    }

    #[test]
    fn test_mcp_resource() {
        let resource = McpResource {
            uri: "file:///test.txt".to_string(),
            mime_type: Some("text/plain".to_string()),
            text: Some("file content".to_string()),
            blob: None,
        };
        assert_eq!(resource.uri, "file:///test.txt");
        assert_eq!(resource.mime_type, Some("text/plain".to_string()));
        assert_eq!(resource.text, Some("file content".to_string()));
    }

    #[test]
    fn test_server_status_variants() {
        assert_eq!(ServerStatus::Connecting, ServerStatus::Connecting);
        assert_eq!(ServerStatus::Ready, ServerStatus::Ready);
        assert_eq!(ServerStatus::Degraded, ServerStatus::Degraded);
        assert_eq!(ServerStatus::Stopped, ServerStatus::Stopped);
        assert_eq!(ServerStatus::Error, ServerStatus::Error);
    }

    #[test]
    fn test_server_status_display() {
        assert_eq!(format!("{}", ServerStatus::Connecting), "connecting");
        assert_eq!(format!("{}", ServerStatus::Ready), "ready");
        assert_eq!(format!("{}", ServerStatus::Degraded), "degraded");
        assert_eq!(format!("{}", ServerStatus::Stopped), "stopped");
        assert_eq!(format!("{}", ServerStatus::Error), "error");
    }

    #[test]
    fn test_runtime_info_default() {
        let info = RuntimeInfo::default();
        assert_eq!(info.status, ServerStatus::Stopped);
        assert!(info.last_error.is_none());
        assert!(info.connected_at.is_none());
        assert!(info.disconnected_at.is_none());
        assert_eq!(info.tool_count, 0);
        assert_eq!(info.restart_count, 0);
        assert!(info.last_ping_at.is_none());
    }

    #[test]
    fn test_runtime_info_custom() {
        let info = RuntimeInfo {
            status: ServerStatus::Ready,
            last_error: None,
            connected_at: Some(Utc::now()),
            disconnected_at: None,
            tool_count: 5,
            restart_count: 0,
            last_ping_at: Some(Utc::now()),
        };
        assert_eq!(info.status, ServerStatus::Ready);
        assert_eq!(info.tool_count, 5);
    }

    #[test]
    fn test_tool_alias() {
        let alias = ToolAlias {
            alias: "mcp__server__tool".to_string(),
            server_id: "server".to_string(),
            original_name: "tool".to_string(),
        };
        assert_eq!(alias.alias, "mcp__server__tool");
        assert_eq!(alias.server_id, "server");
        assert_eq!(alias.original_name, "tool");
    }

    #[test]
    fn test_mcp_event_server_status_changed() {
        let event = McpEvent::ServerStatusChanged {
            server_id: "test-server".to_string(),
            status: ServerStatus::Ready,
            error: None,
        };
        match event {
            McpEvent::ServerStatusChanged { server_id, status, error } => {
                assert_eq!(server_id, "test-server");
                assert_eq!(status, ServerStatus::Ready);
                assert!(error.is_none());
            }
            _ => panic!("Expected ServerStatusChanged variant"),
        }
    }

    #[test]
    fn test_mcp_event_tools_changed() {
        let event = McpEvent::ToolsChanged {
            server_id: "test-server".to_string(),
            tools: vec!["tool1".to_string(), "tool2".to_string()],
        };
        match event {
            McpEvent::ToolsChanged { server_id, tools } => {
                assert_eq!(server_id, "test-server");
                assert_eq!(tools.len(), 2);
            }
            _ => panic!("Expected ToolsChanged variant"),
        }
    }

    #[test]
    fn test_mcp_event_tool_executed() {
        let event = McpEvent::ToolExecuted {
            server_id: "test-server".to_string(),
            tool_name: "test-tool".to_string(),
            success: true,
        };
        match event {
            McpEvent::ToolExecuted { server_id, tool_name, success } => {
                assert_eq!(server_id, "test-server");
                assert_eq!(tool_name, "test-tool");
                assert!(success);
            }
            _ => panic!("Expected ToolExecuted variant"),
        }
    }

    #[test]
    fn test_mcp_event_serialization() {
        let event = McpEvent::ServerStatusChanged {
            server_id: "test".to_string(),
            status: ServerStatus::Ready,
            error: None,
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("ServerStatusChanged"));
        assert!(json.contains("test"));
        assert!(json.contains("ready"));
    }

    #[test]
    fn test_server_status_serialization() {
        let status = ServerStatus::Ready;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"ready\"");
    }

    #[test]
    fn test_runtime_info_serialization() {
        let info = RuntimeInfo::default();
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("stopped"));
        assert!(json.contains("tool_count"));
    }
}
