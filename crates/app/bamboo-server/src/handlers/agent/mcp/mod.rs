use actix_web::HttpResponse;

mod api_types;
mod server_handlers;
mod tool_handlers;

pub use api_types::{
    HeaderConfigApi, ImportServersRequest, ImportServersResponse, ImportStartError,
    MainstreamServerRequest, McpServerApiRecord, McpServerConfigApi, ServerListResponse,
    ServerRequest, ToolInfo, ToolListResponse, TransportConfigApi,
};
pub use server_handlers::{
    add_server, connect_server, delete_server, disconnect_server, get_server, import_servers,
    list_servers, refresh_tools, update_server,
};
pub use tool_handlers::{get_server_tools, list_tools};

fn persist_config_error(message: impl Into<String>) -> HttpResponse {
    HttpResponse::InternalServerError().json(serde_json::json!({
        "error": message.into()
    }))
}
