use actix_web::{web, HttpResponse, Responder};

use crate::app_state::{AppState, ConfigUpdateEffects};

use super::super::api_types::ServerRequest;
use super::super::persist_config_error;

/// Adds a new MCP server
///
/// # HTTP Route
/// `POST /mcp/servers`
pub async fn add_server(
    state: web::Data<AppState>,
    req: web::Json<ServerRequest>,
) -> impl Responder {
    let config = match req.into_inner() {
        ServerRequest::Internal(config) => config,
        ServerRequest::Mainstream(flat) => match flat.into_internal(None) {
            Ok(config) => config,
            Err(error) => {
                return HttpResponse::BadRequest().json(serde_json::json!({ "error": error }));
            }
        },
    };
    let server_id = config.id.clone();

    if let Err(e) = state
        .update_config(
            |root| {
                let existing = root
                    .mcp
                    .servers
                    .iter_mut()
                    .find(|server| server.id == server_id);
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
                return HttpResponse::BadRequest().json(serde_json::json!({ "error": error }));
            }
        },
    };

    if let Err(e) = state
        .update_config(
            |root| {
                let existing = root
                    .mcp
                    .servers
                    .iter_mut()
                    .find(|server| server.id == server_id);
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
pub async fn delete_server(state: web::Data<AppState>, path: web::Path<String>) -> impl Responder {
    let server_id = path.into_inner();

    if let Err(e) = state
        .update_config(
            |root| {
                root.mcp.servers.retain(|server| server.id != server_id);
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
