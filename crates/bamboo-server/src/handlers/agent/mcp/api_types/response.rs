use serde::Serialize;
use std::collections::HashMap;

/// Response for listing MCP servers
#[derive(Debug, Serialize)]
pub struct ServerListResponse {
    /// List of server information
    pub servers: Vec<McpServerApiRecord>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum TransportConfigApi {
    Stdio {
        command: String,
        #[serde(default)]
        args: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        cwd: Option<String>,
        #[serde(default)]
        env: HashMap<String, String>,
        startup_timeout_ms: u64,
    },
    Sse {
        url: String,
        #[serde(default)]
        headers: Vec<HeaderConfigApi>,
        connect_timeout_ms: u64,
    },
    StreamableHttp {
        url: String,
        #[serde(default)]
        headers: Vec<HeaderConfigApi>,
        connect_timeout_ms: u64,
    },
}

#[derive(Debug, Serialize)]
pub struct HeaderConfigApi {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Serialize)]
pub struct McpServerConfigApi {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub enabled: bool,
    pub transport: TransportConfigApi,
    pub request_timeout_ms: u64,
    pub healthcheck_interval_ms: u64,
    pub reconnect: bamboo_engine::ReconnectConfig,
    #[serde(default)]
    pub allowed_tools: Vec<String>,
    #[serde(default)]
    pub denied_tools: Vec<String>,
}

/// API record for an MCP server (matches Bodhi frontend expectations).
#[derive(Debug, Serialize)]
pub struct McpServerApiRecord {
    /// Server identifier
    pub id: String,
    /// Server display name (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Whether the server is enabled
    pub enabled: bool,
    /// Server connection status
    pub status: String,
    /// Number of tools provided by this server
    pub tool_count: usize,
    /// Last error message (if any)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Number of restart attempts
    pub restart_count: u32,
    /// Persisted server config (secrets are encrypted / redacted by serde attrs)
    pub config: McpServerConfigApi,
    /// Runtime info (status, timestamps, tool_count, etc.)
    pub runtime: bamboo_engine::RuntimeInfo,
}

/// Response for listing MCP tools
#[derive(Debug, Serialize)]
pub struct ToolListResponse {
    /// List of tool information
    pub tools: Vec<ToolInfo>,
}

/// Information about an MCP tool
#[derive(Debug, Serialize)]
pub struct ToolInfo {
    /// Alias name for the tool (unique identifier)
    pub alias: String,
    /// ID of the server providing this tool
    pub server_id: String,
    /// Original tool name from the server
    pub original_name: String,
    /// Tool description
    pub description: String,
    /// Tool input parameters schema (JSON Schema).
    ///
    /// This is surfaced so the UI can display expected arguments and help users
    /// debug tool calls. It does not contain secrets.
    pub parameters: serde_json::Value,
}

#[derive(Debug, Serialize)]
pub struct ImportServersResponse {
    pub message: String,
    pub mode: String,
    pub added: usize,
    pub updated: usize,
    pub removed: usize,
    pub server_ids: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub start_errors: Vec<ImportStartError>,
}

#[derive(Debug, Serialize)]
pub struct ImportStartError {
    pub server_id: String,
    pub error: String,
}
