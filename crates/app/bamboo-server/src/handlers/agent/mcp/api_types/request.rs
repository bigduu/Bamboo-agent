use serde::Deserialize;
use std::collections::HashMap;

/// Add/update request body: accept both "Bamboo internal" and "mainstream" MCP shapes.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum ServerRequest {
    Internal(bamboo_mcp::McpServerConfig),
    Mainstream(MainstreamServerRequest),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MainstreamServerRequest {
    /// Server id (required for POST; ignored for PUT where path param wins)
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,

    /// Mainstream config often uses `disabled`; we also accept `enabled`.
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub disabled: bool,

    // stdio transport
    #[serde(default)]
    pub command: Option<String>,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default)]
    pub env_encrypted: HashMap<String, String>,
    #[serde(default)]
    pub startup_timeout_ms: Option<u64>,

    // Streamable HTTP transport
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub headers: Vec<bamboo_mcp::HeaderConfig>,
    #[serde(default)]
    pub connect_timeout_ms: Option<u64>,

    #[serde(default)]
    pub transport_kind: Option<String>,

    // Bamboo extras
    #[serde(default)]
    pub request_timeout_ms: Option<u64>,
    #[serde(default)]
    pub healthcheck_interval_ms: Option<u64>,
    #[serde(default)]
    pub reconnect: Option<bamboo_mcp::ReconnectConfig>,
    #[serde(default)]
    pub allowed_tools: Vec<String>,
    #[serde(default)]
    pub denied_tools: Vec<String>,
}

impl MainstreamServerRequest {
    pub fn into_internal(
        mut self,
        id_override: Option<String>,
    ) -> Result<bamboo_mcp::McpServerConfig, String> {
        if let Some(id) = id_override {
            self.id = id;
        }

        if self
            .transport_kind
            .as_deref()
            .is_some_and(|kind| kind != "streamable_http")
            || (self.command.is_some() && self.transport_kind.is_some())
        {
            return Err("Unsupported MCP transport_kind".to_string());
        }

        let enabled = self.enabled.unwrap_or(!self.disabled);
        let request_timeout_ms = self
            .request_timeout_ms
            .unwrap_or(bamboo_mcp::config::default_request_timeout());
        let healthcheck_interval_ms = self
            .healthcheck_interval_ms
            .unwrap_or(bamboo_mcp::config::default_healthcheck_interval());
        let reconnect = self.reconnect.unwrap_or_default();

        let transport = match (self.command, self.url) {
            (Some(command), None) => bamboo_mcp::TransportConfig::Stdio(bamboo_mcp::StdioConfig {
                command,
                args: self.args,
                cwd: self.cwd,
                env: self.env,
                env_encrypted: self.env_encrypted,
                env_credential_refs: std::collections::HashMap::new(),
                startup_timeout_ms: self
                    .startup_timeout_ms
                    .unwrap_or(bamboo_mcp::config::default_startup_timeout()),
            }),
            (None, Some(url)) => {
                bamboo_mcp::TransportConfig::StreamableHttp(bamboo_mcp::StreamableHttpConfig {
                    url,
                    headers: self.headers,
                    connect_timeout_ms: self
                        .connect_timeout_ms
                        .unwrap_or(bamboo_mcp::config::default_connect_timeout()),
                })
            }
            (Some(_), Some(_)) => {
                return Err("MCP server config cannot contain both 'command' and 'url'".to_string());
            }
            (None, None) => {
                return Err(
                    "MCP server config must contain either 'command' (stdio) or 'url' (streamable_http)"
                        .to_string(),
                );
            }
        };

        Ok(bamboo_mcp::McpServerConfig {
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

#[derive(Debug, Deserialize)]
pub struct ImportServersRequest {
    #[serde(rename = "mcpServers")]
    pub mcp_servers: bamboo_mcp::config::McpConfig,
    /// Import mode: "merge" (default) or "replace"
    #[serde(default)]
    pub mode: Option<String>,
}
