use serde::{Deserialize, Serialize};
use serde_json::Value;

// JSON-RPC 2.0 base types

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub id: u64,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

impl JsonRpcRequest {
    pub fn new(id: u64, method: impl Into<String>, params: Option<Value>) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id,
            method: method.into(),
            params,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcNotification {
    pub jsonrpc: String,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

// MCP Protocol types

/// Initialize request sent by client
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpInitializeRequest {
    pub protocol_version: String,
    pub capabilities: ClientCapabilities,
    pub client_info: Implementation,
}

impl Default for McpInitializeRequest {
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

/// Initialize result from server
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpInitializeResult {
    pub protocol_version: String,
    pub capabilities: ServerCapabilities,
    pub server_info: Implementation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClientCapabilities {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub experimental: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sampling: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ServerCapabilities {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub experimental: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logging: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompts: Option<PromptsCapability>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourcesCapability>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<ToolsCapability>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptsCapability {
    pub list_changed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourcesCapability {
    pub subscribe: bool,
    pub list_changed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolsCapability {
    pub list_changed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Implementation {
    pub name: String,
    pub version: String,
}

/// Tool list request/result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpToolListRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpToolListResult {
    pub tools: Vec<McpToolInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpToolInfo {
    pub name: String,
    pub description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_schema: Option<Value>,
}

/// Tool call request/result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpToolCallRequest {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arguments: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpToolCallResult {
    pub content: Vec<crate::types::McpContentItem>,
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
        let json = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"Invalid Request"}}"#;
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
        let caps = ToolsCapability {
            list_changed: true,
        };
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
