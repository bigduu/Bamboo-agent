//! `/api/v1/plugins` — install / update / list / remove HTTP routes
//! (Wave 2 § HTTP agent, `PLUGIN_PLAN.md`).
//!
//! Thin: each handler constructs `ServerPluginInstaller::new(state.clone())`
//! per request and either calls it directly or drives a two-phase prepared
//! source through one serialized ownership/swap/install transaction.
//! All real install/uninstall/list logic (ownership pre-checks, upgrade
//! drop-diff, rollback, source staging) lives in `crate::plugin_installer` /
//! `crate::plugin_source`; this module is wiring + wire-format + HTTP status
//! mapping only.
//!
//! Routes are registered in `crate::routes::agent::plugin_scope`, inside the
//! `/api/v1` scope — which is wrapped by
//! `handlers::settings::enforce_access_password_middleware` (see that
//! function's registration in `routes/agent.rs::agent_routes`). No new auth
//! was added for this module: it inherits the exact same gate every other
//! mutating `/api/v1/*` endpoint (mcp servers, sessions, schedules, ...) sits
//! behind. This matters here more than most: `install`'s body can point at
//! an arbitrary local filesystem path/archive OR a URL whose per-platform
//! artifact gets downloaded, sha256-checked, and later *executed* as an MCP
//! server binary — i.e. this endpoint is an intentional remote-code-install
//! surface, gated exactly like every other config-mutating route and no more
//! loosely.

mod api_types;
mod errors;
mod handlers;
#[cfg(test)]
mod tests;

pub use api_types::{InstallPluginRequest, InstalledPluginView, PluginListResponse};
pub use handlers::{install_plugin, list_plugins, remove_plugin, update_plugin};
