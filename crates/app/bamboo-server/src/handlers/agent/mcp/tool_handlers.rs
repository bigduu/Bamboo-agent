use actix_web::{web, HttpResponse, Responder};

use super::api_types::{ToolInfo, ToolListResponse};
use crate::app_state::AppState;

/// Lists all MCP tools from all servers
///
/// # HTTP Route
/// `GET /mcp/tools`
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
