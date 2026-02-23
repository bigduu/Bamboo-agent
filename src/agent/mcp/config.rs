use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Root MCP configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpConfig {
    #[serde(default = "default_version")]
    pub version: u32,
    #[serde(default)]
    pub servers: Vec<McpServerConfig>,
}

fn default_version() -> u32 {
    1
}

impl Default for McpConfig {
    fn default() -> Self {
        Self {
            version: 1,
            servers: Vec::new(),
        }
    }
}

/// Single MCP server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpServerConfig {
    /// Unique identifier for this server
    pub id: String,
    /// Human-readable name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Whether this server is enabled
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Transport configuration
    pub transport: TransportConfig,
    /// Request timeout in milliseconds
    #[serde(default = "default_request_timeout")]
    pub request_timeout_ms: u64,
    /// Health check interval in milliseconds
    #[serde(default = "default_healthcheck_interval")]
    pub healthcheck_interval_ms: u64,
    /// Reconnection configuration
    #[serde(default)]
    pub reconnect: ReconnectConfig,
    /// List of allowed tools (empty = all allowed)
    #[serde(default)]
    pub allowed_tools: Vec<String>,
    /// List of denied tools
    #[serde(default)]
    pub denied_tools: Vec<String>,
}

fn default_true() -> bool {
    true
}

fn default_request_timeout() -> u64 {
    60000 // 60 seconds
}

fn default_healthcheck_interval() -> u64 {
    30000 // 30 seconds
}

/// Transport configuration variants
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum TransportConfig {
    Stdio(StdioConfig),
    Sse(SseConfig),
}

/// Stdio transport configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StdioConfig {
    /// Command to execute
    pub command: String,
    /// Arguments for the command
    #[serde(default)]
    pub args: Vec<String>,
    /// Working directory
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// Environment variables
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// Startup timeout in milliseconds
    #[serde(default = "default_startup_timeout")]
    pub startup_timeout_ms: u64,
}

fn default_startup_timeout() -> u64 {
    20000 // 20 seconds
}

/// SSE transport configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SseConfig {
    /// SSE endpoint URL
    pub url: String,
    /// Additional headers
    #[serde(default)]
    pub headers: Vec<HeaderConfig>,
    /// Connection timeout in milliseconds
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_ms: u64,
}

fn default_connect_timeout() -> u64 {
    10000 // 10 seconds
}

/// HTTP header configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeaderConfig {
    pub name: String,
    pub value: String,
}

/// Reconnection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconnectConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Initial backoff in milliseconds
    #[serde(default = "default_initial_backoff")]
    pub initial_backoff_ms: u64,
    /// Maximum backoff in milliseconds
    #[serde(default = "default_max_backoff")]
    pub max_backoff_ms: u64,
    /// Maximum reconnection attempts (0 = unlimited)
    #[serde(default)]
    pub max_attempts: u32,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            initial_backoff_ms: 1000,
            max_backoff_ms: 30000,
            max_attempts: 0,
        }
    }
}

fn default_initial_backoff() -> u64 {
    1000
}

fn default_max_backoff() -> u64 {
    30000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mcp_config_default() {
        let config = McpConfig::default();
        assert_eq!(config.version, 1);
        assert!(config.servers.is_empty());
    }

    #[test]
    fn test_mcp_config_deserialization() {
        let json = r#"{"version": 2, "servers": []}"#;
        let config: McpConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.version, 2);
        assert!(config.servers.is_empty());
    }

    #[test]
    fn test_mcp_config_default_version() {
        let json = r#"{"servers": []}"#;
        let config: McpConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.version, 1);
    }

    #[test]
    fn test_mcp_server_config_minimal() {
        let json = r#"{
            "id": "test-server",
            "transport": {
                "type": "stdio",
                "command": "node"
            }
        }"#;
        let config: McpServerConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.id, "test-server");
        assert!(config.enabled); // default
        assert_eq!(config.request_timeout_ms, 60000); // default
        assert_eq!(config.healthcheck_interval_ms, 30000); // default
        assert!(config.reconnect.enabled); // default
        assert!(config.allowed_tools.is_empty());
        assert!(config.denied_tools.is_empty());
    }

    #[test]
    fn test_mcp_server_config_full() {
        let json = r#"{
            "id": "test-server",
            "name": "Test Server",
            "enabled": false,
            "transport": {
                "type": "stdio",
                "command": "node",
                "args": ["server.js"],
                "cwd": "/app",
                "env": {"NODE_ENV": "production"},
                "startup_timeout_ms": 30000
            },
            "request_timeout_ms": 120000,
            "healthcheck_interval_ms": 60000,
            "reconnect": {
                "enabled": true,
                "initial_backoff_ms": 2000,
                "max_backoff_ms": 60000,
                "max_attempts": 5
            },
            "allowed_tools": ["tool1", "tool2"],
            "denied_tools": ["tool3"]
        }"#;
        let config: McpServerConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.id, "test-server");
        assert_eq!(config.name, Some("Test Server".to_string()));
        assert!(!config.enabled);
        assert_eq!(config.request_timeout_ms, 120000);
        assert_eq!(config.healthcheck_interval_ms, 60000);
        assert!(config.reconnect.enabled);
        assert_eq!(config.reconnect.initial_backoff_ms, 2000);
        assert_eq!(config.reconnect.max_backoff_ms, 60000);
        assert_eq!(config.reconnect.max_attempts, 5);
        assert_eq!(config.allowed_tools, vec!["tool1", "tool2"]);
        assert_eq!(config.denied_tools, vec!["tool3"]);
    }

    #[test]
    fn test_stdio_config() {
        let json = r#"{
            "type": "stdio",
            "command": "python",
            "args": ["-m", "server"],
            "cwd": "/home/user",
            "env": {"DEBUG": "1"},
            "startup_timeout_ms": 15000
        }"#;
        let config: TransportConfig = serde_json::from_str(json).unwrap();
        match config {
            TransportConfig::Stdio(stdio) => {
                assert_eq!(stdio.command, "python");
                assert_eq!(stdio.args, vec!["-m", "server"]);
                assert_eq!(stdio.cwd, Some("/home/user".to_string()));
                assert_eq!(stdio.env.get("DEBUG"), Some(&"1".to_string()));
                assert_eq!(stdio.startup_timeout_ms, 15000);
            }
            _ => panic!("Expected Stdio transport"),
        }
    }

    #[test]
    fn test_stdio_config_minimal() {
        let json = r#"{
            "type": "stdio",
            "command": "node"
        }"#;
        let config: TransportConfig = serde_json::from_str(json).unwrap();
        match config {
            TransportConfig::Stdio(stdio) => {
                assert_eq!(stdio.command, "node");
                assert!(stdio.args.is_empty());
                assert!(stdio.cwd.is_none());
                assert!(stdio.env.is_empty());
                assert_eq!(stdio.startup_timeout_ms, 20000); // default
            }
            _ => panic!("Expected Stdio transport"),
        }
    }

    #[test]
    fn test_sse_config() {
        let json = r#"{
            "type": "sse",
            "url": "http://localhost:8080/sse",
            "headers": [
                {"name": "Authorization", "value": "Bearer token123"}
            ],
            "connect_timeout_ms": 5000
        }"#;
        let config: TransportConfig = serde_json::from_str(json).unwrap();
        match config {
            TransportConfig::Sse(sse) => {
                assert_eq!(sse.url, "http://localhost:8080/sse");
                assert_eq!(sse.headers.len(), 1);
                assert_eq!(sse.headers[0].name, "Authorization");
                assert_eq!(sse.headers[0].value, "Bearer token123");
                assert_eq!(sse.connect_timeout_ms, 5000);
            }
            _ => panic!("Expected SSE transport"),
        }
    }

    #[test]
    fn test_sse_config_minimal() {
        let json = r#"{
            "type": "sse",
            "url": "http://localhost:8080/sse"
        }"#;
        let config: TransportConfig = serde_json::from_str(json).unwrap();
        match config {
            TransportConfig::Sse(sse) => {
                assert_eq!(sse.url, "http://localhost:8080/sse");
                assert!(sse.headers.is_empty());
                assert_eq!(sse.connect_timeout_ms, 10000); // default
            }
            _ => panic!("Expected SSE transport"),
        }
    }

    #[test]
    fn test_reconnect_config_default() {
        let config = ReconnectConfig::default();
        assert!(config.enabled);
        assert_eq!(config.initial_backoff_ms, 1000);
        assert_eq!(config.max_backoff_ms, 30000);
        assert_eq!(config.max_attempts, 0); // unlimited
    }

    #[test]
    fn test_reconnect_config_unlimited_attempts() {
        let json = r#"{
            "enabled": true,
            "initial_backoff_ms": 500,
            "max_backoff_ms": 10000
        }"#;
        let config: ReconnectConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
        assert_eq!(config.initial_backoff_ms, 500);
        assert_eq!(config.max_backoff_ms, 10000);
        assert_eq!(config.max_attempts, 0);
    }

    #[test]
    fn test_reconnect_config_disabled() {
        let json = r#"{"enabled": false}"#;
        let config: ReconnectConfig = serde_json::from_str(json).unwrap();
        assert!(!config.enabled);
    }

    #[test]
    fn test_header_config() {
        let header = HeaderConfig {
            name: "Content-Type".to_string(),
            value: "application/json".to_string(),
        };
        assert_eq!(header.name, "Content-Type");
        assert_eq!(header.value, "application/json");
    }

    #[test]
    fn test_full_mcp_config() {
        let json = r#"{
            "version": 1,
            "servers": [
                {
                    "id": "fs-server",
                    "transport": {
                        "type": "stdio",
                        "command": "mcp-server-filesystem"
                    }
                },
                {
                    "id": "web-server",
                    "transport": {
                        "type": "sse",
                        "url": "http://localhost:3000/sse"
                    }
                }
            ]
        }"#;
        let config: McpConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.servers.len(), 2);
        assert_eq!(config.servers[0].id, "fs-server");
        assert_eq!(config.servers[1].id, "web-server");
    }

    #[test]
    fn test_server_config_disabled() {
        let json = r#"{
            "id": "disabled-server",
            "enabled": false,
            "transport": {"type": "stdio", "command": "node"}
        }"#;
        let config: McpServerConfig = serde_json::from_str(json).unwrap();
        assert!(!config.enabled);
    }
}
