use actix_web::{web, HttpResponse, Responder};

use crate::app_state::AppState;

use super::super::api_types::{to_api_config, McpServerApiRecord, ServerListResponse};

/// Lists all MCP servers and their status
///
/// # HTTP Route
/// `GET /mcp/servers`
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
pub async fn get_server(state: web::Data<AppState>, path: web::Path<String>) -> impl Responder {
    let server_id = path.into_inner();

    let config = state.config.read().await.clone();
    let Some(server_cfg) = config
        .mcp
        .servers
        .iter()
        .find(|server| server.id == server_id)
    else {
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
