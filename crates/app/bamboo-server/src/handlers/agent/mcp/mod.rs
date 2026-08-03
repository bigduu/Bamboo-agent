use actix_web::{HttpResponse, ResponseError};

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
// Shared by-id MCP merge helper, re-exported for `crate::plugin_installer`.
pub(crate) use server_handlers::upsert_server_by_id;
pub use tool_handlers::{get_server_tools, list_tools};

fn mutation_error_response(error: crate::error::AppError) -> HttpResponse {
    error.error_response()
}
