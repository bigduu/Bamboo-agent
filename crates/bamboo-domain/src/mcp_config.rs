use serde::de;
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::collections::HashMap;

/// Root MCP configuration
#[derive(Debug, Clone)]
pub struct McpConfig {
    pub version: u32,
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

// -----------------------------------------------------------------------------
// On-disk format compatibility
// -----------------------------------------------------------------------------
//
// The MCP ecosystem "mainstream" config format (e.g. Claude Desktop) uses:
//
//   "mcpServers": {
//     "filesystem": { "command": "...", "args": [...], "env": {...} }
//   }
//
// Our internal runtime format historically used:
//
//   { "version": 1, "servers": [ { "id": "...", "transport": {...} } ] }
//
// We support reading BOTH formats, and we serialize to the mainstream map format
// (the Config layer maps this struct under the `mcpServers` key).

#[derive(Debug, Clone, Deserialize)]
struct McpConfigLegacyDisk {
    #[serde(default = "default_version")]
    version: u32,
    #[serde(default)]
    servers: Vec<McpServerConfig>,
}

#[derive(Debug, Clone, Deserialize)]
struct McpServerConfigFlatDisk {
    id: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    disabled: bool,

    // stdio (mainstream) shape
    #[serde(default)]
    command: Option<String>,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    env: HashMap<String, String>,
    #[serde(default)]
    env_encrypted: HashMap<String, String>,
    #[serde(default)]
    startup_timeout_ms: Option<u64>,

    // sse shape
    #[serde(default)]
    url: Option<String>,
    #[serde(default, deserialize_with = "deserialize_headers")]
    headers: Vec<HeaderConfig>,
    #[serde(default)]
    connect_timeout_ms: Option<u64>,

    // Bamboo extras (optional)
    #[serde(default)]
    request_timeout_ms: Option<u64>,
    #[serde(default)]
    healthcheck_interval_ms: Option<u64>,
    #[serde(default)]
    reconnect: Option<ReconnectConfig>,
    #[serde(default)]
    allowed_tools: Vec<String>,
    #[serde(default)]
    denied_tools: Vec<String>,
}

fn deserialize_headers<'de, D>(deserializer: D) -> Result<Vec<HeaderConfig>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;

    if value.is_null() {
        return Ok(Vec::new());
    }

    // Mainstream/Claude Desktop style: "headers": { "Authorization": "Bearer ..." }
    if let Some(map) = value.as_object() {
        let mut headers = Vec::with_capacity(map.len());
        for (name, raw) in map.iter() {
            let value = raw.as_str().unwrap_or("").to_string();
            headers.push(HeaderConfig {
                name: name.clone(),
                value,
                value_encrypted: None,
            });
        }
        return Ok(headers);
    }

    // Alternate style: "headers": [{ "name": "...", "value": "..." }, ...]
    if value.is_array() {
        return serde_json::from_value::<Vec<HeaderConfig>>(value).map_err(de::Error::custom);
    }

    Err(de::Error::custom(
        "MCP SSE headers must be an object map or an array",
    ))
}

impl McpServerConfigFlatDisk {
    fn into_internal(self) -> Result<McpServerConfig, String> {
        let enabled = self.enabled.unwrap_or(!self.disabled);

        let request_timeout_ms = self
            .request_timeout_ms
            .unwrap_or_else(default_request_timeout);
        let healthcheck_interval_ms = self
            .healthcheck_interval_ms
            .unwrap_or_else(default_healthcheck_interval);
        let reconnect = self.reconnect.unwrap_or_default();

        let transport = match (self.command, self.url) {
            (Some(command), None) => TransportConfig::Stdio(StdioConfig {
                command,
                args: self.args,
                cwd: self.cwd,
                env: self.env,
                env_encrypted: self.env_encrypted,
                startup_timeout_ms: self
                    .startup_timeout_ms
                    .unwrap_or_else(default_startup_timeout),
            }),
            (None, Some(url)) => TransportConfig::Sse(SseConfig {
                url,
                headers: self.headers,
                connect_timeout_ms: self
                    .connect_timeout_ms
                    .unwrap_or_else(default_connect_timeout),
            }),
            (Some(_), Some(_)) => {
                return Err("MCP server config cannot contain both 'command' and 'url'".to_string())
            }
            (None, None) => {
                return Err(
                    "MCP server config must contain either 'command' (stdio) or 'url' (sse)"
                        .to_string(),
                )
            }
        };

        Ok(McpServerConfig {
            id: self.id,
            name: self.name,
            enabled,
            transport,
            request_timeout_ms,
            healthcheck_interval_ms,
            reconnect,
            allowed_tools: self.allowed_tools,
            denied_tools: self.denied_tools,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
struct McpServerDiskOut {
    // Claude Desktop / mainstream MCP config convention:
    // - The server id is the map key under `mcpServers`.
    // - A server is disabled by setting `"disabled": true`.
    #[serde(default, skip_serializing_if = "is_false")]
    disabled: bool,

    // stdio transport shape
    #[serde(skip_serializing_if = "Option::is_none")]
    command: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    args: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cwd: Option<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    env: HashMap<String, String>,

    // sse transport shape
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    headers: HashMap<String, String>,

    // Full transport config for StreamableHttp (and future non-SSE/non-stdio transports).
    // Stdio and SSE are serialized inline (command/url) for Claude Desktop compatibility.
    #[serde(skip_serializing_if = "Option::is_none")]
    transport: Option<TransportConfig>,
}

fn is_false(value: &bool) -> bool {
    !*value
}

impl From<&McpServerConfig> for McpServerDiskOut {
    fn from(server: &McpServerConfig) -> Self {
        let mut out = Self {
            disabled: !server.enabled,
            command: None,
            args: Vec::new(),
            cwd: None,
            env: HashMap::new(),
            url: None,
            headers: HashMap::new(),
            transport: None,
        };

        match &server.transport {
            TransportConfig::Stdio(stdio) => {
                out.command = Some(stdio.command.clone());
                out.args = stdio.args.clone();
                out.cwd = stdio.cwd.clone();
                out.env = stdio.env.clone();
            }
            TransportConfig::Sse(sse) => {
                out.url = Some(sse.url.clone());
                out.headers = sse
                    .headers
                    .iter()
                    .filter(|h| !h.name.trim().is_empty())
                    .map(|h| (h.name.clone(), h.value.clone()))
                    .collect();
            }
            TransportConfig::StreamableHttp(config) => {
                out.transport = Some(TransportConfig::StreamableHttp(config.clone()));
            }
        }

        out
    }
}

impl Serialize for McpConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.servers.len()))?;
        for server in &self.servers {
            let entry = McpServerDiskOut::from(server);
            map.serialize_entry(&server.id, &entry)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for McpConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;

        // Legacy format: { "version": 1, "servers": [ ... ] }
        if value.get("servers").is_some() {
            let legacy: McpConfigLegacyDisk =
                serde_json::from_value(value).map_err(de::Error::custom)?;
            return Ok(Self {
                version: legacy.version,
                servers: legacy.servers,
            });
        }

        // Mainstream map format: { "<server_id>": { ... }, ... }
        let Some(obj) = value.as_object() else {
            return Err(de::Error::custom(
                "MCP config must be an object (legacy {version,servers} or a server map)",
            ));
        };

        let mut servers = Vec::with_capacity(obj.len());
        for (id, raw_entry) in obj.iter() {
            let mut entry = raw_entry.clone();
            let entry_obj = entry
                .as_object_mut()
                .ok_or_else(|| de::Error::custom("MCP server entry must be an object"))?;
            // Inject id from the map key.
            entry_obj.insert("id".to_string(), Value::String(id.clone()));

            // Accept either our internal full shape or the mainstream flattened shape.
            if let Ok(server) = serde_json::from_value::<McpServerConfig>(entry.clone()) {
                servers.push(server);
                continue;
            }

            let flat: McpServerConfigFlatDisk =
                serde_json::from_value(entry).map_err(de::Error::custom)?;
            let server = flat.into_internal().map_err(de::Error::custom)?;
            servers.push(server);
        }

        Ok(Self {
            version: default_version(),
            servers,
        })
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

pub fn default_request_timeout() -> u64 {
    60000 // 60 seconds
}

pub fn default_healthcheck_interval() -> u64 {
    30000 // 30 seconds
}

/// Transport configuration variants
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum TransportConfig {
    Stdio(StdioConfig),
    Sse(SseConfig),
    #[serde(rename = "streamable_http")]
    StreamableHttp(StreamableHttpConfig),
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
    /// Environment variables (plaintext, in-memory only).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub env: HashMap<String, String>,
    /// Encrypted environment variables values (nonce:ciphertext), keyed by env var name.
    ///
    /// Legacy/back-compat only: older Bamboo builds stored secrets encrypted-at-rest.
    /// We still accept these values so existing configs keep working, but we no longer
    /// persist them (standard MCP config stores plaintext env vars).
    #[serde(default, skip_serializing)]
    pub env_encrypted: HashMap<String, String>,
    /// Startup timeout in milliseconds
    #[serde(default = "default_startup_timeout")]
    pub startup_timeout_ms: u64,
}

pub fn default_startup_timeout() -> u64 {
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

pub fn default_connect_timeout() -> u64 {
    10000 // 10 seconds
}

/// MCP Streamable HTTP transport configuration (MCP 2.0, 2025-03-26).
///
/// Uses a single HTTP endpoint for both sending and receiving JSON-RPC messages.
/// The server URL should point to the MCP endpoint (e.g. `http://localhost:3000/mcp`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamableHttpConfig {
    /// MCP endpoint URL
    pub url: String,
    /// Additional headers
    #[serde(default)]
    pub headers: Vec<HeaderConfig>,
    /// Connection timeout in milliseconds
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_ms: u64,
}

/// HTTP header configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeaderConfig {
    pub name: String,
    /// Header value (plaintext).
    #[serde(default)]
    pub value: String,
    /// Encrypted header value (nonce:ciphertext).
    ///
    /// Legacy/back-compat only: older Bamboo builds stored secrets encrypted-at-rest.
    /// We still accept these values so existing configs keep working, but we no longer
    /// persist them (standard MCP config stores plaintext headers).
    #[serde(default, skip_serializing)]
    pub value_encrypted: Option<String>,
}

/// Reconnection configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
    fn test_streamable_http_config() {
        let json = r#"{
            "type": "streamable_http",
            "url": "http://localhost:3000/mcp",
            "headers": [
                {"name": "Authorization", "value": "Bearer token123"}
            ],
            "connect_timeout_ms": 5000
        }"#;
        let config: TransportConfig = serde_json::from_str(json).unwrap();
        match config {
            TransportConfig::StreamableHttp(cfg) => {
                assert_eq!(cfg.url, "http://localhost:3000/mcp");
                assert_eq!(cfg.headers.len(), 1);
                assert_eq!(cfg.headers[0].name, "Authorization");
                assert_eq!(cfg.connect_timeout_ms, 5000);
            }
            _ => panic!("Expected StreamableHttp transport"),
        }
    }

    #[test]
    fn test_streamable_http_config_minimal() {
        let json = r#"{
            "type": "streamable_http",
            "url": "http://localhost:3000/mcp"
        }"#;
        let config: TransportConfig = serde_json::from_str(json).unwrap();
        match config {
            TransportConfig::StreamableHttp(cfg) => {
                assert_eq!(cfg.url, "http://localhost:3000/mcp");
                assert!(cfg.headers.is_empty());
                assert_eq!(cfg.connect_timeout_ms, 10000); // default
            }
            _ => panic!("Expected StreamableHttp transport"),
        }
    }

    #[test]
    fn test_streamable_http_round_trip() {
        let cfg = McpConfig {
            version: 1,
            servers: vec![McpServerConfig {
                id: "test-sh".to_string(),
                name: None,
                enabled: true,
                transport: TransportConfig::StreamableHttp(StreamableHttpConfig {
                    url: "http://localhost:3000/mcp".to_string(),
                    headers: vec![HeaderConfig {
                        name: "Authorization".to_string(),
                        value: "Bearer token".to_string(),
                        value_encrypted: None,
                    }],
                    connect_timeout_ms: 5000,
                }),
                request_timeout_ms: default_request_timeout(),
                healthcheck_interval_ms: default_healthcheck_interval(),
                reconnect: ReconnectConfig::default(),
                allowed_tools: vec![],
                denied_tools: vec![],
            }],
        };

        let json = serde_json::to_string(&cfg).unwrap();
        let parsed: McpConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.servers.len(), 1);
        match &parsed.servers[0].transport {
            TransportConfig::StreamableHttp(sh) => {
                assert_eq!(sh.url, "http://localhost:3000/mcp");
                assert_eq!(sh.connect_timeout_ms, 5000);
            }
            _ => panic!("Expected StreamableHttp transport"),
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
            value_encrypted: None,
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
    fn test_mcp_config_deserialization_mainstream_map_stdio() {
        let json = r#"{
            "filesystem": {
                "command": "node",
                "args": ["server.js"],
                "env": {"MCP_ROOT": "/tmp"}
            }
        }"#;

        let config: McpConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.version, 1);
        assert_eq!(config.servers.len(), 1);
        assert_eq!(config.servers[0].id, "filesystem");

        match &config.servers[0].transport {
            TransportConfig::Stdio(stdio) => {
                assert_eq!(stdio.command, "node");
                assert_eq!(stdio.args, vec!["server.js"]);
                assert_eq!(stdio.env.get("MCP_ROOT").map(|s| s.as_str()), Some("/tmp"));
            }
            _ => panic!("Expected stdio transport"),
        }
    }

    #[test]
    fn test_mcp_config_serialization_is_map() {
        let mut env_encrypted = HashMap::new();
        env_encrypted.insert("TOKEN".to_string(), "nonce:ciphertext".to_string());

        let cfg = McpConfig {
            version: 1,
            servers: vec![McpServerConfig {
                id: "demo".to_string(),
                name: Some("Demo".to_string()),
                enabled: true,
                transport: TransportConfig::Stdio(StdioConfig {
                    command: "node".to_string(),
                    args: vec!["server.js".to_string()],
                    cwd: None,
                    env: HashMap::new(),
                    env_encrypted,
                    startup_timeout_ms: default_startup_timeout(),
                }),
                request_timeout_ms: default_request_timeout(),
                healthcheck_interval_ms: default_healthcheck_interval(),
                reconnect: ReconnectConfig::default(),
                allowed_tools: vec![],
                denied_tools: vec![],
            }],
        };

        let value = serde_json::to_value(&cfg).unwrap();
        assert!(value.get("servers").is_none());
        assert!(value.get("demo").is_some());
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
