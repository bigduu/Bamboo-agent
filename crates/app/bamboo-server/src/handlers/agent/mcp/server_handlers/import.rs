use actix_web::{web, HttpResponse, Responder};
use std::collections::HashMap;

use crate::app_state::AppState;

use super::super::api_types::{ImportServersRequest, ImportServersResponse};
use super::super::mutation_error_response;

/// Merge one MCP server into a config's server list BY ID: replace an existing
/// entry with the same id, or append a new one. Returns `true` if the server
/// was newly ADDED, `false` if it REPLACED an existing entry.
///
/// The single by-id merge implementation shared by [`import_servers`] here and
/// the plugin installer's MCP registration
/// (`crate::plugin_installer::ServerPluginInstaller::register_mcp`) so the two
/// can't drift (see PLUGIN_PLAN.md § "MCP registration reuses the existing
/// merge logic"). NOTE: this is a raw last-writer-wins merge with NO ownership
/// check — the plugin installer runs `reconcile_exclusive` FIRST and only ever
/// passes ids it is allowed to (re)register.
pub(crate) fn upsert_server_by_id(
    servers: &mut Vec<bamboo_mcp::McpServerConfig>,
    server: bamboo_mcp::McpServerConfig,
) -> bool {
    if let Some(slot) = servers.iter_mut().find(|item| item.id == server.id) {
        *slot = server;
        false
    } else {
        servers.push(server);
        true
    }
}

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
            "error": crate::error::error_value("No servers found under 'mcpServers'")
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

    // Stage every enabled incoming runtime before committing the exact MCP
    // metadata/credential generation. A failed candidate leaves the previous
    // durable/live/runtime generation untouched, so a successful import has
    // no deferred per-server start errors.
    let force_restart = server_ids.iter().cloned().collect();
    if let Err(error) = state
        .update_legacy_mcp_config(force_restart, |mcp| {
            let existing_ids: std::collections::HashSet<String> =
                mcp.servers.iter().map(|server| server.id.clone()).collect();

            if replace {
                let incoming_ids: std::collections::HashSet<String> =
                    incoming_by_id.keys().cloned().collect();
                let to_remove: Vec<String> =
                    existing_ids.difference(&incoming_ids).cloned().collect();
                removed = to_remove.len();

                mcp.servers
                    .retain(|server| incoming_ids.contains(&server.id));
            }

            for server in incoming_by_id.values() {
                if upsert_server_by_id(&mut mcp.servers, server.clone()) {
                    added += 1;
                } else {
                    updated += 1;
                }
            }

            Ok(())
        })
        .await
    {
        return mutation_error_response(error);
    }

    HttpResponse::Ok().json(ImportServersResponse {
        message: "MCP servers imported".to_string(),
        mode,
        added,
        updated,
        removed,
        server_ids,
        start_errors: Vec::new(),
    })
}
