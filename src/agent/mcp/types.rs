//! MCP Types and Data Structures
//!
//! This module defines the core types used throughout the MCP (Model Context Protocol)
//! implementation, including tool definitions, execution results, server status tracking,
//! and event notifications.
//!
//! # Core Types
//!
//! - [`McpTool`]: Tool metadata received from MCP servers
//! - [`McpCallResult`]: Result of tool execution
//! - [`McpContentItem`]: Content returned by tools (text, images, resources)
//! - [`ServerStatus`]: Runtime status of MCP servers
//! - [`RuntimeInfo`]: Detailed server health and statistics
//! - [`McpEvent`]: Events for server state changes
//!
//! # Architecture
//!
//! The MCP client manages multiple server connections, each providing tools.
//! Tools can be invoked and return structured content. The manager tracks
//! server health and emits events for monitoring.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// MCP tool metadata received from a server.
///
/// Represents a tool discovered from an MCP server during tool listing.
/// Contains the tool's identity, description, and parameter schema.
///
/// # Fields
///
/// * `name` - Unique tool identifier within the server
/// * `description` - Human-readable description of the tool's purpose
/// * `parameters` - JSON Schema describing expected input parameters
///
/// # Example
///
/// ```ignore
/// let tool = McpTool {
///     name: "read_file".to_string(),
///     description: "Read file contents from the filesystem".to_string(),
///     parameters: json!({
///         "type": "object",
///         "properties": {
///             "path": {"type": "string", "description": "File path"}
///         },
///         "required": ["path"]
///     }),
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpTool {
    /// Unique tool identifier within the server
    pub name: String,
    /// Human-readable description of what the tool does
    pub description: String,
    /// JSON Schema describing the tool's input parameters
    pub parameters: serde_json::Value,
}

/// Result of calling an MCP tool.
///
/// Contains the content returned by the tool execution and a flag
/// indicating whether an error occurred.
///
/// # Fields
///
/// * `content` - List of content items (text, images, or resources)
/// * `is_error` - Whether the tool execution failed
///
/// # Error Handling
///
/// When `is_error` is true, the content typically contains error messages
/// or diagnostic information. Clients should check this flag before
/// processing the content.
///
/// # Example
///
/// ```ignore
/// let result = McpCallResult {
///     content: vec![McpContentItem::Text {
///         text: "File contents here".to_string(),
///     }],
///     is_error: false,
/// };
///
/// if !result.is_error {
///     for item in result.content {
///         // Process content
///     }
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpCallResult {
    /// Content items returned by the tool
    pub content: Vec<McpContentItem>,
    /// Whether the tool execution encountered an error
    #[serde(default)]
    pub is_error: bool,
}

/// Content item returned by MCP tools.
///
/// Tools can return different types of content: text, images, or resources.
/// This enum provides a tagged union for all content types.
///
/// # Variants
///
/// * `Text` - Plain text content
/// * `Image` - Image data with MIME type (base64-encoded)
/// * `Resource` - Reference to a resource (file, URL, etc.)
///
/// # Example
///
/// ```ignore
/// // Text content
/// let text = McpContentItem::Text {
///     text: "Hello, world!".to_string(),
/// };
///
/// // Image content
/// let image = McpContentItem::Image {
///     data: base64_encoded_data,
///     mime_type: "image/png".to_string(),
/// };
///
/// // Resource reference
/// let resource = McpContentItem::Resource {
///     resource: McpResource {
///         uri: "file:///path/to/file.txt".to_string(),
///         mime_type: Some("text/plain".to_string()),
///         text: Some("file contents".to_string()),
///         blob: None,
///     },
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum McpContentItem {
    /// Plain text content
    #[serde(rename = "text")]
    Text {
        /// The text content
        text: String,
    },
    /// Image content (base64-encoded)
    #[serde(rename = "image")]
    Image {
        /// Base64-encoded image data
        data: String,
        /// MIME type of the image (e.g., "image/png")
        mime_type: String,
    },
    /// Resource reference
    #[serde(rename = "resource")]
    Resource {
        /// The resource being referenced
        resource: McpResource,
    },
}

/// Resource reference in MCP.
///
/// Represents a resource that can be accessed through MCP, such as a file,
/// URL, or other data source. Resources can contain either text or binary data.
///
/// # Fields
///
/// * `uri` - Unique identifier for the resource (e.g., "file:///path/to/file")
/// * `mime_type` - Optional MIME type of the resource content
/// * `text` - Text content (if the resource is text-based)
/// * `blob` - Binary content as base64 (if the resource is binary)
///
/// # Note
///
/// Either `text` or `blob` should be present, but not both.
///
/// # Example
///
/// ```ignore
/// // Text file resource
/// let text_resource = McpResource {
///     uri: "file:///docs/readme.txt".to_string(),
///     mime_type: Some("text/plain".to_string()),
///     text: Some("File contents here".to_string()),
///     blob: None,
/// };
///
/// // Binary file resource
/// let binary_resource = McpResource {
///     uri: "file:///images/photo.png".to_string(),
///     mime_type: Some("image/png".to_string()),
///     text: None,
///     blob: Some(base64_encoded_data),
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpResource {
    /// Unique resource identifier (URI format)
    pub uri: String,
    /// MIME type of the resource content
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    /// Text content (for text-based resources)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// Binary content as base64 (for binary resources)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob: Option<String>,
}

/// Server runtime status indicator.
///
/// Represents the current operational state of an MCP server connection.
/// Used for health monitoring and connection management.
///
/// # States
///
/// * `Connecting` - Server is being initialized
/// * `Ready` - Server is operational and accepting requests
/// * `Degraded` - Server is running but with limited functionality
/// * `Stopped` - Server has been shut down
/// * `Error` - Server encountered a critical error
///
/// # Example
///
/// ```ignore
/// match server_status {
///     ServerStatus::Ready => println!("Server is healthy"),
///     ServerStatus::Error => eprintln!("Server failed"),
///     _ => println!("Server is transitioning"),
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServerStatus {
    /// Server is being initialized
    Connecting,
    /// Server is operational and ready for requests
    Ready,
    /// Server is running but with degraded functionality
    Degraded,
    /// Server has been shut down
    Stopped,
    /// Server encountered a critical error
    Error,
}

impl std::fmt::Display for ServerStatus {
    /// Formats the status as a lowercase string.
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

/// Runtime information for an MCP server.
///
/// Contains comprehensive health and statistics data for a server connection,
/// including status, timestamps, tool counts, and error information.
///
/// # Fields
///
/// * `status` - Current operational status of the server
/// * `last_error` - Most recent error message (if any)
/// * `connected_at` - Timestamp when the server connected
/// * `disconnected_at` - Timestamp when the server disconnected
/// * `tool_count` - Number of tools provided by this server
/// * `restart_count` - Number of times the server has been restarted
/// * `last_ping_at` - Timestamp of the last successful ping
///
/// # Monitoring
///
/// This structure is used for health monitoring and diagnostics,
/// providing visibility into server connection lifecycle and performance.
///
/// # Example
///
/// ```ignore
/// let info = RuntimeInfo {
///     status: ServerStatus::Ready,
///     connected_at: Some(Utc::now()),
///     tool_count: 5,
///     ..Default::default()
/// };
///
/// if info.status == ServerStatus::Ready {
///     println!("Server has {} tools available", info.tool_count);
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeInfo {
    /// Current operational status
    pub status: ServerStatus,
    /// Most recent error message (if any)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Timestamp when the server connected
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connected_at: Option<DateTime<Utc>>,
    /// Timestamp when the server disconnected
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disconnected_at: Option<DateTime<Utc>>,
    /// Number of tools provided by this server
    pub tool_count: usize,
    /// Number of times the server has been restarted
    pub restart_count: u32,
    /// Timestamp of the last successful ping
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_ping_at: Option<DateTime<Utc>>,
}

impl Default for RuntimeInfo {
    /// Creates default runtime info for a stopped server.
    ///
    /// Initializes with `Stopped` status and zero counters.
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

/// Tool alias mapping for namespaced tool access.
///
/// Maps a fully-qualified tool name (with server prefix) to the original
/// tool name on a specific server. This enables multiple servers to provide
/// tools with the same name without conflicts.
///
/// # Format
///
/// The alias format is: `mcp__<server_id>__<original_name>`
///
/// # Fields
///
/// * `alias` - Fully-qualified tool name with server prefix
/// * `server_id` - Identifier of the server providing this tool
/// * `original_name` - Original tool name on the server
///
/// # Example
///
/// ```ignore
/// let alias = ToolAlias {
///     alias: "mcp__filesystem__read_file".to_string(),
///     server_id: "filesystem".to_string(),
///     original_name: "read_file".to_string(),
/// };
///
/// // When the user calls "mcp__filesystem__read_file",
/// // it maps to the "read_file" tool on the "filesystem" server
/// ```
#[derive(Debug, Clone)]
pub struct ToolAlias {
    /// Fully-qualified tool name (mcp__<server>__<tool>)
    pub alias: String,
    /// Server providing this tool
    pub server_id: String,
    /// Original tool name on the server
    pub original_name: String,
}

/// Event emitted by the MCP manager.
///
/// Represents state changes and operations in the MCP system.
/// Events are used for monitoring, logging, and reactive updates.
///
/// # Variants
///
/// * `ServerStatusChanged` - Server connection status changed
/// * `ToolsChanged` - Server's tool list was updated
/// * `ToolExecuted` - A tool was invoked (success or failure)
///
/// # Monitoring
///
/// Subscribe to these events to monitor server health, track tool usage,
/// or implement reactive UI updates.
///
/// # Example
///
/// ```ignore
/// match event {
///     McpEvent::ServerStatusChanged { server_id, status, error } => {
///         println!("Server {} is now {:?}", server_id, status);
///     }
///     McpEvent::ToolExecuted { server_id, tool_name, success } => {
///         println!("Tool {} on {} - success: {}", tool_name, server_id, success);
///     }
///     _ => {}
/// }
/// ```
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum McpEvent {
    /// Server connection status changed
    ServerStatusChanged {
        /// Server identifier
        server_id: String,
        /// New status
        status: ServerStatus,
        /// Error message (if status is Error)
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    /// Server's available tools changed
    ToolsChanged {
        /// Server identifier
        server_id: String,
        /// List of available tool names
        tools: Vec<String>,
    },
    /// Tool was executed
    ToolExecuted {
        /// Server identifier
        server_id: String,
        /// Name of the executed tool
        tool_name: String,
        /// Whether execution succeeded
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
            McpEvent::ServerStatusChanged {
                server_id,
                status,
                error,
            } => {
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
            McpEvent::ToolExecuted {
                server_id,
                tool_name,
                success,
            } => {
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
