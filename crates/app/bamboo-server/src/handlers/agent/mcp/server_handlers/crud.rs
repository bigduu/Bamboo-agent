use actix_web::{web, HttpResponse, Responder};

use crate::app_state::AppState;

use super::super::api_types::ServerRequest;
use super::super::mutation_error_response;

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
                return HttpResponse::BadRequest()
                    .json(serde_json::json!({ "error": crate::error::error_value(error) }));
            }
        },
    };
    let server_id = config.id.clone();
    let response_server_id = server_id.clone();

    let force_restart = std::collections::BTreeSet::from([server_id.clone()]);
    if let Err(error) = state
        .update_legacy_mcp_config(force_restart, move |mcp| {
            let existing = mcp.servers.iter_mut().find(|server| server.id == server_id);
            if let Some(slot) = existing {
                *slot = config.clone();
            } else {
                mcp.servers.push(config.clone());
            }
            Ok(())
        })
        .await
    {
        return mutation_error_response(error);
    }

    HttpResponse::Created().json(serde_json::json!({
        "message": "Server saved",
        "server_id": response_server_id
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
                return HttpResponse::BadRequest()
                    .json(serde_json::json!({ "error": crate::error::error_value(error) }));
            }
        },
    };

    let response_server_id = server_id.clone();
    let force_restart = std::collections::BTreeSet::from([server_id.clone()]);
    if let Err(error) = state
        .update_legacy_mcp_config(force_restart, move |mcp| {
            let existing = mcp.servers.iter_mut().find(|server| server.id == server_id);
            if let Some(slot) = existing {
                *slot = config.clone();
            } else {
                mcp.servers.push(config.clone());
            }
            Ok(())
        })
        .await
    {
        return mutation_error_response(error);
    }

    HttpResponse::Ok().json(serde_json::json!({
        "message": "Server updated",
        "server_id": response_server_id
    }))
}

/// Deletes an MCP server (stops and removes it)
///
/// # HTTP Route
/// `DELETE /mcp/servers/{server_id}`
pub async fn delete_server(state: web::Data<AppState>, path: web::Path<String>) -> impl Responder {
    let server_id = path.into_inner();

    let response_server_id = server_id.clone();
    if let Err(error) = state
        .update_legacy_mcp_config(std::collections::BTreeSet::new(), move |mcp| {
            mcp.servers.retain(|server| server.id != server_id);
            Ok(())
        })
        .await
    {
        return mutation_error_response(error);
    }

    HttpResponse::Ok().json(serde_json::json!({
        "message": "Server removed",
        "server_id": response_server_id
    }))
}
