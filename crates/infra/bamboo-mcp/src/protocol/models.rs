//! Application notification projection. Wire models belong to `rmcp`.
pub use rmcp::model::ServerPeerInfo as McpInitializeResult;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct JsonRpcNotification {
    #[serde(default = "jsonrpc_version")]
    pub jsonrpc: String,
    pub method: String,
    pub params: Option<serde_json::Value>,
}

fn jsonrpc_version() -> String {
    "2.0".into()
}
