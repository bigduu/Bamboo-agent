use actix_web::{web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};

use crate::state::AppState;

/// Response for server list
#[derive(Debug, Serialize)]
pub struct ServerListResponse {
    pub servers: Vec<ServerInfo>,
}

#[derive(Debug, Serialize)]
pub struct ServerInfo {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub status: String,
    pub tool_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub restart_count: u32,
}

/// Response for tool list
#[derive(Debug, Serialize)]
pub struct ToolListResponse {
    pub tools: Vec<ToolInfo>,
}

#[derive(Debug, Serialize)]
pub struct ToolInfo {
    pub alias: String,
    pub server_id: String,
    pub original_name: String,
    pub description: String,
}

/// Add/update server request
#[derive(Debug, Deserialize)]
pub struct ServerRequest {
    #[serde(flatten)]
    pub config: agent_mcp::McpServerConfig,
}

/// List all MCP servers and their status
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
                    .find(|s| s == &id).map(|_| id.clone());

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

/// Get MCP server details by ID
pub async fn get_server(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> impl Responder {
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

/// Add a new MCP server
pub async fn add_server(
    state: web::Data<AppState>,
    req: web::Json<ServerRequest>,
) -> impl Responder {
    let config = req.into_inner().config;
    let server_id = config.id.clone();

    match state.mcp_manager.start_server(config).await {
        Ok(_) => {
            HttpResponse::Created().json(serde_json::json!({
                "message": "Server started",
                "server_id": server_id
            }))
        }
        Err(e) => {
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": format!("Failed to start server: {}", e)
            }))
        }
    }
}

/// Update MCP server configuration
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
        Ok(_) => {
            HttpResponse::Ok().json(serde_json::json!({
                "message": "Server updated",
                "server_id": server_id
            }))
        }
        Err(e) => {
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": format!("Failed to update server: {}", e)
            }))
        }
    }
}

/// Delete MCP server
pub async fn delete_server(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> impl Responder {
    let server_id = path.into_inner();

    match state.mcp_manager.stop_server(&server_id).await {
        Ok(_) => {
            HttpResponse::Ok().json(serde_json::json!({
                "message": "Server stopped and removed",
                "server_id": server_id
            }))
        }
        Err(e) => {
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": format!("Failed to stop server: {}", e)
            }))
        }
    }
}

/// Connect/reconnect to MCP server
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

/// Disconnect MCP server
pub async fn disconnect_server(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> impl Responder {
    let server_id = path.into_inner();

    match state.mcp_manager.stop_server(&server_id).await {
        Ok(_) => {
            HttpResponse::Ok().json(serde_json::json!({
                "message": "Server disconnected",
                "server_id": server_id
            }))
        }
        Err(e) => {
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": format!("Failed to disconnect server: {}", e)
            }))
        }
    }
}

/// Refresh tools from MCP server
pub async fn refresh_tools(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> impl Responder {
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
        Err(e) => {
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": format!("Failed to refresh tools: {}", e)
            }))
        }
    }
}

/// List all MCP tools (flattened from all servers)
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

/// Get tools for a specific server
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
