mod connections;
mod crud;
mod import;
mod query;

pub use connections::{connect_server, disconnect_server, refresh_tools};
pub use crud::{add_server, delete_server, update_server};
pub use import::import_servers;
// Shared by-id MCP merge helper, re-exported for the plugin installer's MCP
// registration (see `import::upsert_server_by_id`).
pub(crate) use import::upsert_server_by_id;
pub use query::{get_server, list_servers};
