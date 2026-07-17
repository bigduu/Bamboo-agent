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
            "error": crate::error::error_value(format!("Server '{}' not found", server_id))
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

#[cfg(test)]
mod tests {
    use actix_web::{http::StatusCode, test, web, App};
    use serde_json::Value;
    use tempfile::tempdir;

    use crate::routes::configure_routes;
    use crate::AppState;

    /// `GET /api/v1/mcp/servers/{id}` for an unknown server must use the
    /// canonical nested error envelope (`{"error": {"message", "type"}}`),
    /// not the old flat `{"error": "<string>"}` shape. #251/#507.
    #[actix_web::test]
    async fn get_server_not_found_uses_canonical_error_envelope() {
        let temp_dir = tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let state = web::Data::new(
            AppState::new(temp_dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let resp = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/mcp/servers/does-not-exist")
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["error"]["type"], "api_error");
        assert_eq!(
            body["error"]["message"],
            "Server 'does-not-exist' not found"
        );
    }
}
