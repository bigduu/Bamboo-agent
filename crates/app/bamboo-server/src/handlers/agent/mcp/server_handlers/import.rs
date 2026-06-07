use actix_web::{web, HttpResponse, Responder};
use std::collections::HashMap;

use crate::app_state::{AppState, ConfigUpdateEffects};

use super::super::api_types::{ImportServersRequest, ImportServersResponse, ImportStartError};
use super::super::persist_config_error;

/// Bulk import MCP servers from a Claude Desktop-style config chunk.
///
/// # HTTP Route
/// `POST /mcp/servers/import`
pub async fn import_servers(
    state: web::Data<AppState>,
    req: web::Json<ImportServersRequest>,
) -> impl Responder {
    let incoming = req.into_inner();
    let mode = incoming.mode.unwrap_or_else(|| "merge".to_string());
    let replace = mode.trim().eq_ignore_ascii_case("replace");
    let mode = if replace { "replace" } else { "merge" }.to_string();

    // Deduplicate by id (last one wins).
    let mut incoming_by_id: HashMap<String, bamboo_mcp::McpServerConfig> = HashMap::new();
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
                let existing_ids: std::collections::HashSet<String> = root
                    .mcp
                    .servers
                    .iter()
                    .map(|server| server.id.clone())
                    .collect();

                if replace {
                    let incoming_ids: std::collections::HashSet<String> =
                        incoming_by_id.keys().cloned().collect();
                    let to_remove: Vec<String> =
                        existing_ids.difference(&incoming_ids).cloned().collect();
                    removed = to_remove.len();
                    removed_ids = to_remove;

                    root.mcp
                        .servers
                        .retain(|server| !incoming_ids.contains(&server.id));
                }

                for (id, server) in incoming_by_id.iter() {
                    let slot = root.mcp.servers.iter_mut().find(|item| item.id == *id);
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
    for server_id in &removed_ids {
        let _ = state.mcp_manager.stop_server(server_id).await;
    }

    let mut start_errors = Vec::new();
    for id in &server_ids {
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
