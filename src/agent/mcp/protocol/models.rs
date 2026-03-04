//! MCP Protocol Models
//!
//! This module contains data structures for the Model Context Protocol (MCP),
//! which enables communication between AI agents and external tools/services.
//!
//! MCP follows a client-server architecture where:
//! - The client (agent) sends requests to discover and invoke tools
//! - The server provides tools, resources, and prompts
//! - Communication is based on JSON-RPC 2.0
//!
//! # Protocol Flow
//!
//! 1. Client sends `McpInitializeRequest` to establish connection
//! 2. Server responds with `McpInitializeResult` and capabilities
//! 3. Client discovers available tools via `McpToolListRequest`
//! 4. Client invokes tools using `McpToolCallRequest`
//!
//! # Example
//!
//! ```ignore
//! use bamboo_agent::agent::mcp::protocol::models::*;
//!
//! // Create initialization request
//! let init_request = McpInitializeRequest::default();
//!
//! // Call a tool
//! let tool_call = McpToolCallRequest {
//!     name: "read_file".to_string(),
//!     arguments: Some(serde_json::json!({"path": "/test.txt"})),
//! };
//! ```

use serde::{Deserialize, Serialize};
use serde_json::Value;

// JSON-RPC 2.0 base types

/// A JSON-RPC 2.0 request message.
///
/// Represents a request sent from client to server, containing a method name
/// and optional parameters. All MCP requests are wrapped in this structure.
///
/// # Fields
///
/// * `jsonrpc` - Protocol version, always "2.0"
/// * `id` - Unique request identifier for matching responses
/// * `method` - The method name to invoke (e.g., "tools/list")
/// * `params` - Optional parameters for the method
///
/// # Example
///
/// ```ignore
/// let request = JsonRpcRequest::new(1, "tools/list", None);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    /// JSON-RPC protocol version (always "2.0")
    pub jsonrpc: String,
    /// Unique identifier for this request
    pub id: u64,
    /// Name of the method to invoke
    pub method: String,
    /// Optional parameters for the method call
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

impl JsonRpcRequest {
    /// Creates a new JSON-RPC request with the specified parameters.
    ///
    /// # Arguments
    ///
    /// * `id` - Unique identifier for this request
    /// * `method` - The method name to invoke
    /// * `params` - Optional parameters (can be None)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let request = JsonRpcRequest::new(1, "tools/call", Some(json!({"name": "test"})));
    /// ```
    pub fn new(id: u64, method: impl Into<String>, params: Option<Value>) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id,
            method: method.into(),
            params,
        }
    }
}

/// A JSON-RPC 2.0 response message.
///
/// Represents a response from server to client, containing either a result
/// or an error. The `id` field matches the corresponding request.
///
/// # Fields
///
/// * `jsonrpc` - Protocol version, always "2.0"
/// * `id` - Request identifier this response corresponds to
/// * `result` - Successful result (mutually exclusive with error)
/// * `error` - Error information if the request failed
///
/// # Note
///
/// Either `result` or `error` will be present, never both.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    /// JSON-RPC protocol version (always "2.0")
    pub jsonrpc: String,
    /// Request identifier this response corresponds to
    pub id: u64,
    /// Successful result data (present on success)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    /// Error information (present on failure)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

/// A JSON-RPC 2.0 error object.
///
/// Contains error details when a request fails, including an error code,
/// human-readable message, and optional additional data.
///
/// # Standard Error Codes
///
/// - `-32700`: Parse error
/// - `-32600`: Invalid request
/// - `-32601`: Method not found
/// - `-32602`: Invalid params
/// - `-32603`: Internal error
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcError {
    /// Error code indicating the type of error
    pub code: i32,
    /// Human-readable error message
    pub message: String,
    /// Additional error data (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

/// A JSON-RPC 2.0 notification message.
///
/// Represents a one-way message that doesn't expect a response.
/// Used for server-initiated events like tool list changes.
///
/// # Fields
///
/// * `jsonrpc` - Protocol version, always "2.0"
/// * `method` - The notification method name
/// * `params` - Optional notification parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcNotification {
    /// JSON-RPC protocol version (always "2.0")
    pub jsonrpc: String,
    /// Name of the notification method
    pub method: String,
    /// Optional parameters for the notification
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

// MCP Protocol types

/// MCP initialization request sent by the client.
///
/// This is the first message sent to establish an MCP connection.
/// The client declares its protocol version, capabilities, and identity.
///
/// # Fields
///
/// * `protocol_version` - MCP protocol version (e.g., "2024-11-05")
/// * `capabilities` - Features the client supports
/// * `client_info` - Client implementation details (name and version)
///
/// # Example
///
/// ```ignore
/// let request = McpInitializeRequest::default();
/// // Uses bamboo-agent as client name and current package version
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpInitializeRequest {
    /// MCP protocol version being used
    pub protocol_version: String,
    /// Capabilities the client supports
    pub capabilities: ClientCapabilities,
    /// Information about the client implementation
    pub client_info: Implementation,
}

impl Default for McpInitializeRequest {
    /// Creates a default initialization request for the bamboo agent.
    ///
    /// Uses protocol version "2024-11-05" and the current package version.
    fn default() -> Self {
        Self {
            protocol_version: "2024-11-05".to_string(),
            capabilities: ClientCapabilities::default(),
            client_info: Implementation {
                name: "bamboo-agent".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
        }
    }
}

/// MCP initialization result returned by the server.
///
/// Sent in response to an initialization request, declaring the server's
/// protocol version, capabilities, and identity.
///
/// # Fields
///
/// * `protocol_version` - MCP protocol version the server is using
/// * `capabilities` - Features the server supports (tools, resources, prompts)
/// * `server_info` - Server implementation details (name and version)
/// * `instructions` - Optional usage instructions for the client
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpInitializeResult {
    /// MCP protocol version being used by the server
    pub protocol_version: String,
    /// Capabilities the server supports
    pub capabilities: ServerCapabilities,
    /// Information about the server implementation
    pub server_info: Implementation,
    /// Optional instructions for using this server
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
}

/// Client capabilities declaration.
///
/// Informs the server about which optional features the client supports.
/// Currently minimal, as most MCP features are server-side.
///
/// # Fields
///
/// * `experimental` - Experimental capabilities (future use)
/// * `sampling` - Support for LLM sampling requests (future use)
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClientCapabilities {
    /// Experimental capabilities (reserved for future use)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub experimental: Option<Value>,
    /// Support for LLM sampling requests (reserved for future use)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sampling: Option<Value>,
}

/// Server capabilities declaration.
///
/// Informs the client about which features the server provides.
/// The client can then use this information to discover and use
/// available tools, resources, and prompts.
///
/// # Fields
///
/// * `experimental` - Experimental capabilities (future use)
/// * `logging` - Support for log message notifications
/// * `prompts` - Support for prompt templates
/// * `resources` - Support for resource access
/// * `tools` - Support for tool invocation
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ServerCapabilities {
    /// Experimental capabilities (reserved for future use)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub experimental: Option<Value>,
    /// Support for logging notifications
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logging: Option<Value>,
    /// Support for prompt templates
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompts: Option<PromptsCapability>,
    /// Support for resource access
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourcesCapability>,
    /// Support for tool invocation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<ToolsCapability>,
}

/// Prompts capability configuration.
///
/// Indicates support for prompt templates and whether the server
/// will notify clients when the prompt list changes.
///
/// # Fields
///
/// * `list_changed` - Whether the server sends notifications when prompts change
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptsCapability {
    /// If true, server sends "prompts/list_changed" notifications
    ///
    /// Some MCP servers omit this field; treat missing as `false`.
    #[serde(default, alias = "list_changed")]
    pub list_changed: bool,
}

/// Resources capability configuration.
///
/// Indicates support for resource access, subscriptions, and change notifications.
/// Resources allow servers to expose files, data, or other content.
///
/// # Fields
///
/// * `subscribe` - Whether clients can subscribe to resource updates
/// * `list_changed` - Whether the server sends notifications when resources change
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourcesCapability {
    /// If true, clients can subscribe to resource change notifications
    ///
    /// Some MCP servers omit this field; treat missing as `false`.
    #[serde(default)]
    pub subscribe: bool,
    /// If true, server sends "resources/list_changed" notifications
    ///
    /// Some MCP servers omit this field; treat missing as `false`.
    #[serde(default, alias = "list_changed")]
    pub list_changed: bool,
}

/// Tools capability configuration.
///
/// Indicates support for tool invocation and whether the server
/// will notify clients when the tool list changes.
///
/// # Fields
///
/// * `list_changed` - Whether the server sends notifications when tools change
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolsCapability {
    /// If true, server sends "tools/list_changed" notifications
    ///
    /// Some MCP servers omit this field; treat missing as `false`.
    #[serde(default, alias = "list_changed")]
    pub list_changed: bool,
}

/// Implementation information for client or server.
///
/// Identifies the software implementation on either side of the connection.
///
/// # Fields
///
/// * `name` - Human-readable name of the implementation
/// * `version` - Version string (e.g., "1.0.0")
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Implementation {
    /// Name of the implementation
    pub name: String,
    /// Version of the implementation
    pub version: String,
}

/// Tool list request (empty parameters).
///
/// Sent by the client to discover all available tools on the server.
/// The server responds with a `McpToolListResult` containing tool metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpToolListRequest {}

/// Tool list result containing available tools.
///
/// Returned by the server in response to a tool list request.
/// Contains metadata for each available tool including name, description,
/// and input schema.
///
/// # Fields
///
/// * `tools` - List of available tools with their metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpToolListResult {
    /// List of available tools
    pub tools: Vec<McpToolInfo>,
}

/// Tool metadata and schema information.
///
/// Describes a tool's name, purpose, and expected input parameters.
/// Used in tool discovery to help clients understand how to invoke tools.
///
/// # Fields
///
/// * `name` - Unique identifier for the tool
/// * `description` - Human-readable description of what the tool does
/// * `input_schema` - JSON Schema describing expected parameters
///
/// # Example
///
/// ```ignore
/// let tool = McpToolInfo {
///     name: "read_file".to_string(),
///     description: "Read contents of a file".to_string(),
///     input_schema: Some(json!({
///         "type": "object",
///         "properties": {
///             "path": {"type": "string"}
///         },
///         "required": ["path"]
///     })),
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpToolInfo {
    /// Unique tool identifier
    pub name: String,
    /// Human-readable tool description
    pub description: String,
    /// JSON Schema for tool input parameters
    #[serde(
        rename = "inputSchema",
        alias = "input_schema",
        skip_serializing_if = "Option::is_none"
    )]
    pub input_schema: Option<Value>,
}

/// Tool call request to invoke a tool.
///
/// Sent by the client to execute a specific tool with the provided arguments.
///
/// # Fields
///
/// * `name` - Name of the tool to invoke
/// * `arguments` - Tool-specific parameters matching the input schema
///
/// # Example
///
/// ```ignore
/// let request = McpToolCallRequest {
///     name: "read_file".to_string(),
///     arguments: Some(json!({"path": "/test.txt"})),
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpToolCallRequest {
    /// Name of the tool to invoke
    pub name: String,
    /// Tool arguments matching its input schema
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arguments: Option<Value>,
}

/// Tool call result containing execution output.
///
/// Returned by the server after tool execution, containing content items
/// (text, images, or resources) and an error flag.
///
/// # Fields
///
/// * `content` - List of content items returned by the tool
/// * `is_error` - Whether the tool execution failed
///
/// # Note
///
/// Even when `is_error` is true, the content may contain error messages
/// or diagnostic information.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpToolCallResult {
    /// Content items returned by the tool
    pub content: Vec<crate::agent::mcp::types::McpContentItem>,
    /// Whether the tool execution encountered an error
    #[serde(default)]
    pub is_error: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_rpc_request() {
        let request = JsonRpcRequest::new(1, "test", Some(serde_json::json!({"key": "value"})));
        assert_eq!(request.jsonrpc, "2.0");
        assert_eq!(request.id, 1);
        assert_eq!(request.method, "test");
        assert!(request.params.is_some());
    }

    #[test]
    fn test_json_rpc_request_serialization() {
        let request = JsonRpcRequest::new(1, "test", None);
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"jsonrpc\":\"2.0\""));
        assert!(json.contains("\"id\":1"));
        assert!(json.contains("\"method\":\"test\""));
    }

    #[test]
    fn test_json_rpc_response_success() {
        let json = r#"{"jsonrpc":"2.0","id":1,"result":{"status":"ok"}}"#;
        let response: JsonRpcResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.jsonrpc, "2.0");
        assert_eq!(response.id, 1);
        assert!(response.result.is_some());
        assert!(response.error.is_none());
    }

    #[test]
    fn test_json_rpc_response_error() {
        let json =
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"Invalid Request"}}"#;
        let response: JsonRpcResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.jsonrpc, "2.0");
        assert_eq!(response.id, 1);
        assert!(response.result.is_none());
        assert!(response.error.is_some());
        let error = response.error.unwrap();
        assert_eq!(error.code, -32600);
        assert_eq!(error.message, "Invalid Request");
    }

    #[test]
    fn test_json_rpc_notification() {
        let json = r#"{"jsonrpc":"2.0","method":"update","params":{"count":1}}"#;
        let notification: JsonRpcNotification = serde_json::from_str(json).unwrap();
        assert_eq!(notification.jsonrpc, "2.0");
        assert_eq!(notification.method, "update");
        assert!(notification.params.is_some());
    }

    #[test]
    fn test_mcp_initialize_request_default() {
        let request = McpInitializeRequest::default();
        assert_eq!(request.protocol_version, "2024-11-05");
        assert_eq!(request.client_info.name, "bamboo-agent");
    }

    #[test]
    fn test_mcp_initialize_request_serialization() {
        let request = McpInitializeRequest::default();
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("protocolVersion"));
        assert!(json.contains("bamboo-agent"));
    }

    #[test]
    fn test_mcp_initialize_result() {
        let json = r#"{
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "serverInfo": {
                "name": "test-server",
                "version": "1.0.0"
            }
        }"#;
        let result: McpInitializeResult = serde_json::from_str(json).unwrap();
        assert_eq!(result.protocol_version, "2024-11-05");
        assert_eq!(result.server_info.name, "test-server");
        assert_eq!(result.server_info.version, "1.0.0");
    }

    #[test]
    fn test_client_capabilities_default() {
        let caps = ClientCapabilities::default();
        assert!(caps.experimental.is_none());
        assert!(caps.sampling.is_none());
    }

    #[test]
    fn test_server_capabilities_default() {
        let caps = ServerCapabilities::default();
        assert!(caps.experimental.is_none());
        assert!(caps.tools.is_none());
    }

    #[test]
    fn test_tools_capability() {
        let caps = ToolsCapability { list_changed: true };
        assert!(caps.list_changed);
    }

    #[test]
    fn test_prompts_capability() {
        let caps = PromptsCapability {
            list_changed: false,
        };
        assert!(!caps.list_changed);
    }

    #[test]
    fn test_resources_capability() {
        let caps = ResourcesCapability {
            subscribe: true,
            list_changed: false,
        };
        assert!(caps.subscribe);
        assert!(!caps.list_changed);
    }

    #[test]
    fn test_tools_capability_missing_list_changed_defaults_false() {
        let json = r#"{"tools": {}}"#;
        let caps: ServerCapabilities = serde_json::from_str(json).unwrap();
        assert!(caps.tools.is_some());
        assert!(!caps.tools.unwrap().list_changed);
    }

    #[test]
    fn test_tools_capability_accepts_snake_case_list_changed() {
        let json = r#"{"tools": {"list_changed": true}}"#;
        let caps: ServerCapabilities = serde_json::from_str(json).unwrap();
        assert!(caps.tools.is_some());
        assert!(caps.tools.unwrap().list_changed);
    }

    #[test]
    fn test_resources_capability_missing_fields_defaults_false() {
        let json = r#"{"resources": {}}"#;
        let caps: ServerCapabilities = serde_json::from_str(json).unwrap();
        let resources = caps.resources.unwrap();
        assert!(!resources.subscribe);
        assert!(!resources.list_changed);
    }

    #[test]
    fn test_prompts_capability_missing_list_changed_defaults_false() {
        let json = r#"{"prompts": {}}"#;
        let caps: ServerCapabilities = serde_json::from_str(json).unwrap();
        assert!(caps.prompts.is_some());
        assert!(!caps.prompts.unwrap().list_changed);
    }

    #[test]
    fn test_implementation() {
        let impl_info = Implementation {
            name: "test".to_string(),
            version: "1.0.0".to_string(),
        };
        assert_eq!(impl_info.name, "test");
        assert_eq!(impl_info.version, "1.0.0");
    }

    #[test]
    fn test_mcp_tool_list_result() {
        let json = r#"{
            "tools": [
                {
                    "name": "test_tool",
                    "description": "A test tool",
                    "inputSchema": {"type": "object"}
                }
            ]
        }"#;
        let result: McpToolListResult = serde_json::from_str(json).unwrap();
        assert_eq!(result.tools.len(), 1);
        assert_eq!(result.tools[0].name, "test_tool");
        assert_eq!(result.tools[0].description, "A test tool");
    }

    #[test]
    fn test_mcp_tool_list_result_accepts_snake_case_input_schema() {
        let json = r#"{
            "tools": [
                {
                    "name": "test_tool",
                    "description": "A test tool",
                    "input_schema": {"type": "object"}
                }
            ]
        }"#;
        let result: McpToolListResult = serde_json::from_str(json).unwrap();
        assert_eq!(result.tools.len(), 1);
        assert_eq!(result.tools[0].name, "test_tool");
        assert_eq!(result.tools[0].description, "A test tool");
        assert!(result.tools[0].input_schema.is_some());
    }

    #[test]
    fn test_mcp_tool_info() {
        let tool = McpToolInfo {
            name: "read_file".to_string(),
            description: "Read a file".to_string(),
            input_schema: Some(serde_json::json!({"type": "object"})),
        };
        assert_eq!(tool.name, "read_file");
        assert_eq!(tool.description, "Read a file");
        assert!(tool.input_schema.is_some());
    }

    #[test]
    fn test_mcp_tool_call_request() {
        let request = McpToolCallRequest {
            name: "test_tool".to_string(),
            arguments: Some(serde_json::json!({"path": "/test"})),
        };
        assert_eq!(request.name, "test_tool");
        assert!(request.arguments.is_some());
    }

    #[test]
    fn test_mcp_tool_call_request_serialization() {
        let request = McpToolCallRequest {
            name: "test".to_string(),
            arguments: Some(serde_json::json!({"key": "value"})),
        };
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"name\":\"test\""));
        assert!(json.contains("\"arguments\""));
    }

    #[test]
    fn test_mcp_tool_call_result() {
        let result = McpToolCallResult {
            content: vec![],
            is_error: false,
        };
        assert!(!result.is_error);
        assert!(result.content.is_empty());
    }

    #[test]
    fn test_json_rpc_error() {
        let error = JsonRpcError {
            code: -32600,
            message: "Invalid Request".to_string(),
            data: Some(serde_json::json!({"details": "test"})),
        };
        assert_eq!(error.code, -32600);
        assert_eq!(error.message, "Invalid Request");
        assert!(error.data.is_some());
    }
}
