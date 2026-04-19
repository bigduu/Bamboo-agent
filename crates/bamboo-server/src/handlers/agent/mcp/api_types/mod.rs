mod config_map;
mod request;
mod response;
#[cfg(test)]
mod tests;

pub use request::{ImportServersRequest, MainstreamServerRequest, ServerRequest};
pub use response::{
    HeaderConfigApi, ImportServersResponse, ImportStartError, McpServerApiRecord,
    McpServerConfigApi, ServerListResponse, ToolInfo, ToolListResponse, TransportConfigApi,
};

pub(super) fn to_api_config(server: &bamboo_engine::McpServerConfig) -> McpServerConfigApi {
    config_map::to_api_config(server)
}
