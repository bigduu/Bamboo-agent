use actix_web::{web, HttpResponse, Responder};

use crate::app_state::AppState;

use super::super::mutation_error_response;

/// Connects/reconnects to an MCP server
///
/// # HTTP Route
/// `POST /mcp/servers/{server_id}/connect`
pub async fn connect_server(state: web::Data<AppState>, path: web::Path<String>) -> impl Responder {
    let server_id = path.into_inner();

    let response_server_id = server_id.clone();
    let force_restart = std::collections::BTreeSet::from([server_id.clone()]);
    if let Err(error) = state
        .update_legacy_mcp_config(force_restart, move |mcp| {
            let Some(cfg) = mcp.servers.iter_mut().find(|server| server.id == server_id) else {
                return Err(crate::error::AppError::NotFound(format!(
                    "Server '{}'",
                    server_id
                )));
            };
            cfg.enabled = true;
            Ok(())
        })
        .await
    {
        return mutation_error_response(error);
    }

    HttpResponse::Ok().json(serde_json::json!({
        "message": "Server connected",
        "server_id": response_server_id
    }))
}

/// Disconnects an MCP server
///
/// # HTTP Route
/// `POST /mcp/servers/{server_id}/disconnect`
pub async fn disconnect_server(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> impl Responder {
    let server_id = path.into_inner();

    let response_server_id = server_id.clone();
    if let Err(error) = state
        .update_legacy_mcp_config(std::collections::BTreeSet::new(), move |mcp| {
            let Some(cfg) = mcp.servers.iter_mut().find(|server| server.id == server_id) else {
                return Err(crate::error::AppError::NotFound(format!(
                    "Server '{}'",
                    server_id
                )));
            };
            cfg.enabled = false;
            Ok(())
        })
        .await
    {
        return mutation_error_response(error);
    }

    HttpResponse::Ok().json(serde_json::json!({
        "message": "Server disconnected",
        "server_id": response_server_id
    }))
}

/// Refreshes tools from an MCP server
///
/// # HTTP Route
/// `POST /mcp/servers/{server_id}/refresh`
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
            "error": crate::error::error_value(format!("Failed to refresh tools: {}", e))
        })),
    }
}
