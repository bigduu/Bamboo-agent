//! Configuration adapters for SDK transports. No MCP wire implementation lives here.
use crate::config::TransportConfig;
use crate::error::{McpError, Result};
use crate::protocol::McpProtocolClient;
use bamboo_infrastructure::process::{hide_window_for_tokio_command, trace_windows_command};
use reqwest::header::{HeaderName, HeaderValue};
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::transport::{StreamableHttpClientTransport, TokioChildProcess};
use std::collections::HashMap;
use std::time::Duration;

pub(crate) fn build_client(
    config: &TransportConfig,
    http_client: Option<reqwest::Client>,
) -> Result<McpProtocolClient> {
    match config {
        TransportConfig::Stdio(config) => {
            trace_windows_command(
                "agent.mcp.stdio.connect",
                &config.command,
                config.args.iter().map(String::as_str),
            );
            let mut command = tokio::process::Command::new(&config.command);
            hide_window_for_tokio_command(&mut command);
            command
                .args(&config.args)
                .envs(&config.env)
                .kill_on_drop(true);
            if let Some(cwd) = &config.cwd {
                command.current_dir(cwd);
            }
            Ok(McpProtocolClient::new(TokioChildProcess::new(command)?))
        }
        TransportConfig::StreamableHttp(config) => {
            let mut headers = HashMap::new();
            for header in &config.headers {
                let name = HeaderName::from_bytes(header.name.as_bytes())
                    .map_err(|_| McpError::InvalidConfig("Invalid MCP HTTP header name".into()))?;
                let mut value = HeaderValue::from_str(&header.value)
                    .map_err(|_| McpError::InvalidConfig("Invalid MCP HTTP header value".into()))?;
                value.set_sensitive(true);
                headers.insert(name, value);
            }
            let client = match http_client {
                Some(client) => client,
                None => reqwest::Client::builder()
                    .connect_timeout(Duration::from_millis(config.connect_timeout_ms))
                    .build()
                    .map_err(McpError::from)?,
            };
            let transport = StreamableHttpClientTransport::with_client(
                client,
                StreamableHttpClientTransportConfig::with_uri(config.url.clone())
                    .custom_headers(headers),
            );
            Ok(McpProtocolClient::new(transport))
        }
    }
}
