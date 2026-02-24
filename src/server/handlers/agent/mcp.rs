use actix_web::{web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};

use crate::server::app_state::AppState;

// ============================================================================
// Response Types
// ============================================================================

/// Response for listing MCP servers
#[derive(Debug, Serialize)]
pub struct ServerListResponse {
    /// List of server information
    pub servers: Vec<ServerInfo>,
}

/// Information about an MCP server
#[derive(Debug, Serialize)]
pub struct ServerInfo {
    /// Server identifier
    pub id: String,
    /// Server display name
    pub name: String,
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
}

// ============================================================================
// Request Types
// ============================================================================

/// Request for adding or updating an MCP server
#[derive(Debug, Deserialize)]
pub struct ServerRequest {
    /// Server configuration (flattened)
    #[serde(flatten)]
    pub config: crate::agent::mcp::McpServerConfig,
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
    let server_ids = state.mcp_manager.list_servers();

    let servers: Vec<ServerInfo> = server_ids
        .into_iter()
        .filter_map(|id| {
            state.mcp_manager.get_server_info(&id).map(|info| {
                let _config = state
                    .mcp_manager
                    .list_servers()
                    .into_iter()
                    .find(|s| s == &id)
                    .map(|_| id.clone());

                ServerInfo {
                    id: id.clone(),
                    name: id.clone(), // TODO: get from config
                    enabled: true,    // TODO: get from config
                    status: info.status.to_string(),
                    tool_count: info.tool_count,
                    last_error: info.last_error,
                    restart_count: info.restart_count,
                }
            })
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

    match state.mcp_manager.get_server_info(&server_id) {
        Some(info) => {
            let server_info = ServerInfo {
                id: server_id.clone(),
                name: server_id.clone(),
                enabled: true,
                status: info.status.to_string(),
                tool_count: info.tool_count,
                last_error: info.last_error,
                restart_count: info.restart_count,
            };
            HttpResponse::Ok().json(server_info)
        }
        None => HttpResponse::NotFound().json(serde_json::json!({
            "error": format!("Server '{}' not found", server_id)
        })),
    }
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
    let config = req.into_inner().config;
    let server_id = config.id.clone();

    match state.mcp_manager.start_server(config).await {
        Ok(_) => HttpResponse::Created().json(serde_json::json!({
            "message": "Server started",
            "server_id": server_id
        })),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Failed to start server: {}", e)
        })),
    }
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
    let mut config = req.into_inner().config;
    config.id = server_id.clone();

    // Stop existing server if running
    let _ = state.mcp_manager.stop_server(&server_id).await;

    // Start with new config
    match state.mcp_manager.start_server(config).await {
        Ok(_) => HttpResponse::Ok().json(serde_json::json!({
            "message": "Server updated",
            "server_id": server_id
        })),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Failed to update server: {}", e)
        })),
    }
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

    match state.mcp_manager.stop_server(&server_id).await {
        Ok(_) => HttpResponse::Ok().json(serde_json::json!({
            "message": "Server stopped and removed",
            "server_id": server_id
        })),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Failed to stop server: {}", e)
        })),
    }
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
pub async fn connect_server(
    _state: web::Data<AppState>,
    path: web::Path<String>,
) -> impl Responder {
    let server_id = path.into_inner();

    // TODO: Implement reconnect using stored config
    HttpResponse::Ok().json(serde_json::json!({
        "message": "Connect not fully implemented",
        "server_id": server_id
    }))
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
