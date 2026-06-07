use actix_web::{web, HttpResponse, Responder};

use crate::app_state::{AppState, ConfigUpdateEffects};

use super::super::persist_config_error;

/// Connects/reconnects to an MCP server
///
/// # HTTP Route
/// `POST /mcp/servers/{server_id}/connect`
pub async fn connect_server(state: web::Data<AppState>, path: web::Path<String>) -> impl Responder {
    let server_id = path.into_inner();

    // Enable + start using the stored config.
    let mut server_cfg: Option<bamboo_mcp::McpServerConfig> = None;
    if let Err(e) = state
        .update_config(
            |root| {
                let Some(cfg) = root
                    .mcp
                    .servers
                    .iter_mut()
                    .find(|server| server.id == server_id)
                else {
                    return Err(crate::error::AppError::NotFound(format!(
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
            crate::error::AppError::NotFound(_) => {
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
pub async fn disconnect_server(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> impl Responder {
    let server_id = path.into_inner();

    if let Err(e) = state
        .update_config(
            |root| {
                let Some(cfg) = root
                    .mcp
                    .servers
                    .iter_mut()
                    .find(|server| server.id == server_id)
                else {
                    return Err(crate::error::AppError::NotFound(format!(
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
            crate::error::AppError::NotFound(_) => {
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
