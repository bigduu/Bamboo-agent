use actix_web::{web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::server::app_state::{AppState, ConfigUpdateEffects};

fn persist_config_error(message: impl Into<String>) -> HttpResponse {
    HttpResponse::InternalServerError().json(serde_json::json!({
        "error": message.into()
    }))
}

// ============================================================================
// Response Types
// ============================================================================

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
    pub reconnect: crate::agent::mcp::ReconnectConfig,
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
    pub runtime: crate::agent::mcp::RuntimeInfo,
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

// ============================================================================
// Request Types
// ============================================================================

/// Add/update request body: accept both "Bamboo internal" and "mainstream" MCP shapes.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum ServerRequest {
    Internal(crate::agent::mcp::McpServerConfig),
    Mainstream(MainstreamServerRequest),
}

#[derive(Debug, Deserialize)]
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

    // sse transport
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub headers: Vec<crate::agent::mcp::HeaderConfig>,
    #[serde(default)]
    pub connect_timeout_ms: Option<u64>,

    // Bamboo extras
    #[serde(default)]
    pub request_timeout_ms: Option<u64>,
    #[serde(default)]
    pub healthcheck_interval_ms: Option<u64>,
    #[serde(default)]
    pub reconnect: Option<crate::agent::mcp::ReconnectConfig>,
    #[serde(default)]
    pub allowed_tools: Vec<String>,
    #[serde(default)]
    pub denied_tools: Vec<String>,
}

impl MainstreamServerRequest {
    fn into_internal(
        mut self,
        id_override: Option<String>,
    ) -> Result<crate::agent::mcp::McpServerConfig, String> {
        if let Some(id) = id_override {
            self.id = id;
        }

        let enabled = self.enabled.unwrap_or(!self.disabled);
        let request_timeout_ms = self
            .request_timeout_ms
            .unwrap_or(crate::agent::mcp::config::default_request_timeout());
        let healthcheck_interval_ms = self
            .healthcheck_interval_ms
            .unwrap_or(crate::agent::mcp::config::default_healthcheck_interval());
        let reconnect = self.reconnect.unwrap_or_default();

        let transport = match (self.command, self.url) {
            (Some(command), None) => {
                crate::agent::mcp::TransportConfig::Stdio(crate::agent::mcp::StdioConfig {
                    command,
                    args: self.args,
                    cwd: self.cwd,
                    env: self.env,
                    env_encrypted: self.env_encrypted,
                    startup_timeout_ms: self
                        .startup_timeout_ms
                        .unwrap_or(crate::agent::mcp::config::default_startup_timeout()),
                })
            }
            (None, Some(url)) => {
                crate::agent::mcp::TransportConfig::Sse(crate::agent::mcp::SseConfig {
                    url,
                    headers: self.headers,
                    connect_timeout_ms: self
                        .connect_timeout_ms
                        .unwrap_or(crate::agent::mcp::config::default_connect_timeout()),
                })
            }
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

        Ok(crate::agent::mcp::McpServerConfig {
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

fn mask() -> String {
    "****...****".to_string()
}

fn to_api_config(server: &crate::agent::mcp::McpServerConfig) -> McpServerConfigApi {
    let transport = match &server.transport {
        crate::agent::mcp::TransportConfig::Stdio(stdio) => {
            // Never return plaintext; only return keys so users can see which env vars exist.
            let mut keys: Vec<String> = stdio.env_encrypted.keys().cloned().collect();
            keys.extend(stdio.env.keys().cloned());
            keys.sort();
            keys.dedup();

            let env = keys.into_iter().map(|k| (k, mask())).collect();

            TransportConfigApi::Stdio {
                command: stdio.command.clone(),
                args: stdio.args.clone(),
                cwd: stdio.cwd.clone(),
                env,
                startup_timeout_ms: stdio.startup_timeout_ms,
            }
        }
        crate::agent::mcp::TransportConfig::Sse(sse) => TransportConfigApi::Sse {
            url: sse.url.clone(),
            headers: sse
                .headers
                .iter()
                .map(|h| HeaderConfigApi {
                    name: h.name.clone(),
                    value: mask(),
                })
                .collect(),
            connect_timeout_ms: sse.connect_timeout_ms,
        },
    };

    McpServerConfigApi {
        id: server.id.clone(),
        name: server.name.clone(),
        enabled: server.enabled,
        transport,
        request_timeout_ms: server.request_timeout_ms,
        healthcheck_interval_ms: server.healthcheck_interval_ms,
        reconnect: server.reconnect.clone(),
        allowed_tools: server.allowed_tools.clone(),
        denied_tools: server.denied_tools.clone(),
    }
}

// ============================================================================
// HTTP Handlers
// ============================================================================

/// Lists all MCP servers and their status
///
/// # HTTP Route
/// `GET /mcp/servers`
///
/// # Response Format
/// Returns a [`ServerListResponse`] with server information:
/// ```json
/// {
///   "servers": [
///     {
///       "id": "filesystem",
///       "name": "filesystem",
///       "enabled": true,
///       "status": "connected",
///       "tool_count": 5,
///       "last_error": null,
///       "restart_count": 0
///     }
///   ]
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Successfully retrieved server list
///
/// # Example
/// ```bash
/// curl http://localhost:3000/mcp/servers
/// ```
pub async fn list_servers(state: web::Data<AppState>) -> impl Responder {
    let config = state.config.read().await.clone();
    let servers: Vec<McpServerApiRecord> = config
        .mcp
        .servers
        .into_iter()
        .map(|server_cfg| {
            let runtime = state
                .mcp_manager
                .get_server_info(&server_cfg.id)
                .unwrap_or_default();
            McpServerApiRecord {
                id: server_cfg.id.clone(),
                name: server_cfg.name.clone(),
                enabled: server_cfg.enabled,
                status: runtime.status.to_string(),
                tool_count: runtime.tool_count,
                last_error: runtime.last_error.clone(),
                restart_count: runtime.restart_count,
                config: to_api_config(&server_cfg),
                runtime,
            }
        })
        .collect();

    HttpResponse::Ok().json(ServerListResponse { servers })
}

/// Gets details of a specific MCP server
///
/// # HTTP Route
/// `GET /mcp/servers/{server_id}`
///
/// # Path Parameters
/// - `server_id`: Server identifier
///
/// # Response Format
/// Returns [`ServerInfo`] on success:
/// ```json
/// {
///   "id": "filesystem",
///   "name": "filesystem",
///   "enabled": true,
///   "status": "connected",
///   "tool_count": 5,
///   "last_error": null,
///   "restart_count": 0
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Server found
/// - `404 Not Found`: Server not found
///
/// # Example
/// ```bash
/// curl http://localhost:3000/mcp/servers/filesystem
/// ```
pub async fn get_server(state: web::Data<AppState>, path: web::Path<String>) -> impl Responder {
    let server_id = path.into_inner();

    let config = state.config.read().await.clone();
    let Some(server_cfg) = config.mcp.servers.iter().find(|s| s.id == server_id) else {
        return HttpResponse::NotFound().json(serde_json::json!({
            "error": format!("Server '{}' not found", server_id)
        }));
    };

    let runtime = state
        .mcp_manager
        .get_server_info(&server_id)
        .unwrap_or_default();
    let server_info = McpServerApiRecord {
        id: server_cfg.id.clone(),
        name: server_cfg.name.clone(),
        enabled: server_cfg.enabled,
        status: runtime.status.to_string(),
        tool_count: runtime.tool_count,
        last_error: runtime.last_error.clone(),
        restart_count: runtime.restart_count,
        config: to_api_config(server_cfg),
        runtime,
    };

    HttpResponse::Ok().json(server_info)
}

#[derive(Debug, Deserialize)]
pub struct ImportServersRequest {
    #[serde(rename = "mcpServers")]
    pub mcp_servers: crate::agent::mcp::McpConfig,
    /// Import mode: "merge" (default) or "replace"
    #[serde(default)]
    pub mode: Option<String>,
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

/// Bulk import MCP servers from a Claude Desktop-style config chunk.
///
/// # HTTP Route
/// `POST /mcp/servers/import`
///
/// # Request Body
/// ```json
/// {
///   "mcpServers": {
///     "filesystem": { "command": "npx", "args": ["-y", "..."], "env": {"MCP_ROOT": "/tmp"} },
///     "my-sse": { "url": "http://127.0.0.1:3000/sse", "headers": [{"name":"Authorization","value":"Bearer ..."}] }
///   },
///   "mode": "merge"
/// }
/// ```
pub async fn import_servers(
    state: web::Data<AppState>,
    req: web::Json<ImportServersRequest>,
) -> impl Responder {
    let incoming = req.into_inner();
    let mode = incoming.mode.unwrap_or_else(|| "merge".to_string());
    let replace = mode.trim().eq_ignore_ascii_case("replace");
    let mode = if replace { "replace" } else { "merge" }.to_string();

    // Deduplicate by id (last one wins).
    let mut incoming_by_id: HashMap<String, crate::agent::mcp::McpServerConfig> = HashMap::new();
    for server in incoming.mcp_servers.servers {
        incoming_by_id.insert(server.id.clone(), server);
    }

    if incoming_by_id.is_empty() {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "No servers found under 'mcpServers'"
        }));
    }

    let server_ids: Vec<String> = {
        let mut ids = incoming_by_id.keys().cloned().collect::<Vec<_>>();
        ids.sort();
        ids
    };

    let mut added = 0usize;
    let mut updated = 0usize;
    let mut removed = 0usize;

    // Unified: update memory -> persist config.json. Then apply runtime updates.
    let mut removed_ids: Vec<String> = Vec::new();
    if let Err(e) = state
        .update_config(
            |root| {
                let existing_ids: std::collections::HashSet<String> =
                    root.mcp.servers.iter().map(|s| s.id.clone()).collect();

                if replace {
                    let incoming_ids: std::collections::HashSet<String> =
                        incoming_by_id.keys().cloned().collect();
                    let to_remove: Vec<String> =
                        existing_ids.difference(&incoming_ids).cloned().collect();
                    removed = to_remove.len();
                    removed_ids = to_remove;

                    root.mcp.servers.retain(|s| !incoming_ids.contains(&s.id));
                }

                for (id, server) in incoming_by_id.iter() {
                    let slot = root.mcp.servers.iter_mut().find(|s| s.id == *id);
                    if let Some(existing) = slot {
                        *existing = server.clone();
                        updated += 1;
                    } else {
                        root.mcp.servers.push(server.clone());
                        added += 1;
                    }
                }

                Ok(())
            },
            ConfigUpdateEffects::default(),
        )
        .await
    {
        return persist_config_error(format!("Failed to save config: {e}"));
    }

    // Apply runtime changes best-effort (do not fail the import if some servers can't start).
    for server_id in removed_ids.iter() {
        let _ = state.mcp_manager.stop_server(server_id).await;
    }

    let mut start_errors = Vec::new();
    for id in server_ids.iter() {
        let Some(server_cfg) = incoming_by_id.get(id).cloned() else {
            continue;
        };

        // In merge mode we only touch imported servers; in replace mode we also already stopped
        // removed servers.
        let _ = state.mcp_manager.stop_server(id).await;
        if server_cfg.enabled {
            if let Err(e) = state.mcp_manager.start_server(server_cfg).await {
                start_errors.push(ImportStartError {
                    server_id: id.clone(),
                    error: e.to_string(),
                });
            }
        }
    }

    HttpResponse::Ok().json(ImportServersResponse {
        message: "MCP servers imported".to_string(),
        mode,
        added,
        updated,
        removed,
        server_ids,
        start_errors,
    })
}

/// Adds a new MCP server
///
/// # HTTP Route
/// `POST /mcp/servers`
///
/// # Request Body
/// Server configuration (fields depend on server type):
/// ```json
/// {
///   "id": "my-server",
///   "command": "node",
///   "args": ["server.js"],
///   "env": {}
/// }
/// ```
///
/// # Response Format
/// ```json
/// {
///   "message": "Server started",
///   "server_id": "my-server"
/// }
/// ```
///
/// # Response Status
/// - `201 Created`: Server started successfully
/// - `500 Internal Server Error`: Failed to start server
///
/// # Example
/// ```bash
/// curl -X POST http://localhost:3000/mcp/servers \
///   -H "Content-Type: application/json" \
///   -d '{"id": "my-server", "command": "node", "args": ["server.js"]}'
/// ```
pub async fn add_server(
    state: web::Data<AppState>,
    req: web::Json<ServerRequest>,
) -> impl Responder {
    let config = match req.into_inner() {
        ServerRequest::Internal(config) => config,
        ServerRequest::Mainstream(flat) => match flat.into_internal(None) {
            Ok(config) => config,
            Err(error) => {
                return HttpResponse::BadRequest().json(serde_json::json!({ "error": error }))
            }
        },
    };
    let server_id = config.id.clone();

    if let Err(e) = state
        .update_config(
            |root| {
                let existing = root.mcp.servers.iter_mut().find(|s| s.id == server_id);
                if let Some(slot) = existing {
                    *slot = config.clone();
                } else {
                    root.mcp.servers.push(config.clone());
                }
                Ok(())
            },
            ConfigUpdateEffects::default(),
        )
        .await
    {
        return persist_config_error(format!("Failed to save config: {e}"));
    }

    if config.enabled {
        if let Err(e) = state.mcp_manager.start_server(config).await {
            return HttpResponse::InternalServerError().json(serde_json::json!({
                "error": format!("Failed to start server: {}", e)
            }));
        }
    }

    HttpResponse::Created().json(serde_json::json!({
        "message": "Server saved",
        "server_id": server_id
    }))
}

/// Updates an existing MCP server configuration
///
/// # HTTP Route
/// `PUT /mcp/servers/{server_id}`
///
/// # Path Parameters
/// - `server_id`: Server identifier to update
///
/// # Request Body
/// Updated server configuration:
/// ```json
/// {
///   "id": "my-server",
///   "command": "node",
///   "args": ["server.js", "--verbose"],
///   "env": {"DEBUG": "true"}
/// }
/// ```
///
/// # Response Format
/// ```json
/// {
///   "message": "Server updated",
///   "server_id": "my-server"
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Server updated successfully
/// - `500 Internal Server Error`: Failed to update server
///
/// # Example
/// ```bash
/// curl -X PUT http://localhost:3000/mcp/servers/my-server \
///   -H "Content-Type: application/json" \
///   -d '{"id": "my-server", "command": "node", "args": ["server.js"]}'
/// ```
pub async fn update_server(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: web::Json<ServerRequest>,
) -> impl Responder {
    let server_id = path.into_inner();
    let config = match req.into_inner() {
        ServerRequest::Internal(mut config) => {
            config.id = server_id.clone();
            config
        }
        ServerRequest::Mainstream(flat) => match flat.into_internal(Some(server_id.clone())) {
            Ok(config) => config,
            Err(error) => {
                return HttpResponse::BadRequest().json(serde_json::json!({ "error": error }))
            }
        },
    };

    if let Err(e) = state
        .update_config(
            |root| {
                let existing = root.mcp.servers.iter_mut().find(|s| s.id == server_id);
                if let Some(slot) = existing {
                    *slot = config.clone();
                } else {
                    root.mcp.servers.push(config.clone());
                }
                Ok(())
            },
            ConfigUpdateEffects::default(),
        )
        .await
    {
        return persist_config_error(format!("Failed to save config: {e}"));
    }

    // Apply runtime: stop existing server if running, then (re)start if enabled.
    let _ = state.mcp_manager.stop_server(&server_id).await;
    if config.enabled {
        if let Err(e) = state.mcp_manager.start_server(config).await {
            return HttpResponse::InternalServerError().json(serde_json::json!({
                "error": format!("Failed to update server: {}", e)
            }));
        }
    }

    HttpResponse::Ok().json(serde_json::json!({
        "message": "Server updated",
        "server_id": server_id
    }))
}

/// Deletes an MCP server (stops and removes it)
///
/// # HTTP Route
/// `DELETE /mcp/servers/{server_id}`
///
/// # Path Parameters
/// - `server_id`: Server identifier to delete
///
/// # Response Format
/// ```json
/// {
///   "message": "Server stopped and removed",
///   "server_id": "my-server"
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Server stopped successfully
/// - `500 Internal Server Error`: Failed to stop server
///
/// # Example
/// ```bash
/// curl -X DELETE http://localhost:3000/mcp/servers/my-server
/// ```
pub async fn delete_server(state: web::Data<AppState>, path: web::Path<String>) -> impl Responder {
    let server_id = path.into_inner();

    if let Err(e) = state
        .update_config(
            |root| {
                root.mcp.servers.retain(|s| s.id != server_id);
                Ok(())
            },
            ConfigUpdateEffects::default(),
        )
        .await
    {
        return persist_config_error(format!("Failed to save config: {e}"));
    }

    let _ = state.mcp_manager.stop_server(&server_id).await;
    HttpResponse::Ok().json(serde_json::json!({
        "message": "Server removed",
        "server_id": server_id
    }))
}

/// Connects/reconnects to an MCP server
///
/// # HTTP Route
/// `POST /mcp/servers/{server_id}/connect`
///
/// # Path Parameters
/// - `server_id`: Server identifier to connect
///
/// # Response Format
/// ```json
/// {
///   "message": "Connect not fully implemented",
///   "server_id": "my-server"
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Connection request acknowledged
///
/// # Note
/// This endpoint is not fully implemented yet.
///
/// # Example
/// ```bash
/// curl -X POST http://localhost:3000/mcp/servers/my-server/connect
/// ```
pub async fn connect_server(state: web::Data<AppState>, path: web::Path<String>) -> impl Responder {
    let server_id = path.into_inner();

    // Enable + start using the stored config.
    let mut server_cfg: Option<crate::agent::mcp::McpServerConfig> = None;
    if let Err(e) = state
        .update_config(
            |root| {
                let Some(cfg) = root.mcp.servers.iter_mut().find(|s| s.id == server_id) else {
                    return Err(crate::server::error::AppError::NotFound(format!(
                        "Server '{}'",
                        server_id
                    )));
                };
                cfg.enabled = true;
                server_cfg = Some(cfg.clone());
                Ok(())
            },
            ConfigUpdateEffects::default(),
        )
        .await
    {
        // Preserve the previous endpoint error shape.
        return match e {
            crate::server::error::AppError::NotFound(_) => {
                HttpResponse::NotFound().json(serde_json::json!({
                    "error": format!("Server '{}' not found", server_id)
                }))
            }
            other => persist_config_error(format!("Failed to save config: {other}")),
        };
    }
    let Some(server_cfg) = server_cfg else {
        return persist_config_error("Missing server config after connect".to_string());
    };

    let _ = state.mcp_manager.stop_server(&server_id).await;
    match state.mcp_manager.start_server(server_cfg).await {
        Ok(_) => HttpResponse::Ok().json(serde_json::json!({
            "message": "Server connected",
            "server_id": server_id
        })),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Failed to start server: {}", e)
        })),
    }
}

/// Disconnects an MCP server
///
/// # HTTP Route
/// `POST /mcp/servers/{server_id}/disconnect`
///
/// # Path Parameters
/// - `server_id`: Server identifier to disconnect
///
/// # Response Format
/// ```json
/// {
///   "message": "Server disconnected",
///   "server_id": "my-server"
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Server disconnected successfully
/// - `500 Internal Server Error`: Failed to disconnect
///
/// # Example
/// ```bash
/// curl -X POST http://localhost:3000/mcp/servers/my-server/disconnect
/// ```
pub async fn disconnect_server(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> impl Responder {
    let server_id = path.into_inner();

    if let Err(e) = state
        .update_config(
            |root| {
                let Some(cfg) = root.mcp.servers.iter_mut().find(|s| s.id == server_id) else {
                    return Err(crate::server::error::AppError::NotFound(format!(
                        "Server '{}'",
                        server_id
                    )));
                };
                cfg.enabled = false;
                Ok(())
            },
            ConfigUpdateEffects::default(),
        )
        .await
    {
        return match e {
            crate::server::error::AppError::NotFound(_) => {
                HttpResponse::NotFound().json(serde_json::json!({
                    "error": format!("Server '{}' not found", server_id)
                }))
            }
            other => persist_config_error(format!("Failed to save config: {other}")),
        };
    }

    match state.mcp_manager.stop_server(&server_id).await {
        Ok(_) => HttpResponse::Ok().json(serde_json::json!({
            "message": "Server disconnected",
            "server_id": server_id
        })),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Failed to disconnect server: {}", e)
        })),
    }
}

/// Refreshes tools from an MCP server
///
/// # HTTP Route
/// `POST /mcp/servers/{server_id}/refresh`
///
/// # Path Parameters
/// - `server_id`: Server identifier to refresh
///
/// # Response Format
/// ```json
/// {
///   "message": "Tools refreshed",
///   "server_id": "my-server",
///   "tool_count": 5
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Tools refreshed successfully
/// - `500 Internal Server Error`: Failed to refresh tools
///
/// # Example
/// ```bash
/// curl -X POST http://localhost:3000/mcp/servers/my-server/refresh
/// ```
pub async fn refresh_tools(state: web::Data<AppState>, path: web::Path<String>) -> impl Responder {
    let server_id = path.into_inner();

    match state.mcp_manager.refresh_tools(&server_id).await {
        Ok(_) => {
            let tool_count = state
                .mcp_manager
                .get_server_info(&server_id)
                .map(|info| info.tool_count)
                .unwrap_or(0);

            HttpResponse::Ok().json(serde_json::json!({
                "message": "Tools refreshed",
                "server_id": server_id,
                "tool_count": tool_count
            }))
        }
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Failed to refresh tools: {}", e)
        })),
    }
}

/// Lists all MCP tools from all servers
///
/// # HTTP Route
/// `GET /mcp/tools`
///
/// # Response Format
/// Returns a [`ToolListResponse`] with tool information:
/// ```json
/// {
///   "tools": [
///     {
///       "alias": "read_file",
///       "server_id": "filesystem",
///       "original_name": "read_file",
///       "description": "Read file contents"
///     }
///   ]
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Successfully retrieved tool list
///
/// # Example
/// ```bash
/// curl http://localhost:3000/mcp/tools
/// ```
pub async fn list_tools(state: web::Data<AppState>) -> impl Responder {
    let aliases = state.mcp_manager.tool_index().all_aliases();

    let tools: Vec<ToolInfo> = aliases
        .into_iter()
        .filter_map(|alias| {
            state
                .mcp_manager
                .get_tool_info(&alias.server_id, &alias.original_name)
                .map(|tool| ToolInfo {
                    alias: alias.alias,
                    server_id: alias.server_id,
                    original_name: alias.original_name,
                    description: tool.description,
                    parameters: tool.parameters,
                })
        })
        .collect();

    HttpResponse::Ok().json(ToolListResponse { tools })
}

/// Lists tools for a specific MCP server
///
/// # HTTP Route
/// `GET /mcp/servers/{server_id}/tools`
///
/// # Path Parameters
/// - `server_id`: Server identifier
///
/// # Response Format
/// Returns a [`ToolListResponse`] with tools from the specified server:
/// ```json
/// {
///   "tools": [
///     {
///       "alias": "read_file",
///       "server_id": "filesystem",
///       "original_name": "read_file",
///       "description": "Read file contents"
///     }
///   ]
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Successfully retrieved tools
/// - `404 Not Found`: Server not found
///
/// # Example
/// ```bash
/// curl http://localhost:3000/mcp/servers/filesystem/tools
/// ```
pub async fn get_server_tools(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> impl Responder {
    let server_id = path.into_inner();

    match state.mcp_manager.get_server_info(&server_id) {
        Some(_) => {
            let tools: Vec<ToolInfo> = state
                .mcp_manager
                .tool_index()
                .all_aliases()
                .into_iter()
                .filter(|alias| alias.server_id == server_id)
                .filter_map(|alias| {
                    state
                        .mcp_manager
                        .get_tool_info(&alias.server_id, &alias.original_name)
                        .map(|tool| ToolInfo {
                            alias: alias.alias,
                            server_id: alias.server_id,
                            original_name: alias.original_name,
                            description: tool.description,
                            parameters: tool.parameters,
                        })
                })
                .collect();

            HttpResponse::Ok().json(ToolListResponse { tools })
        }
        None => HttpResponse::NotFound().json(serde_json::json!({
            "error": format!("Server '{}' not found", server_id)
        })),
    }
}
