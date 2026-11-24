//! Unified HTTP server entry points
//!
//! Consolidates run(), run_with_bind(), WebService from web_service/server.rs
//! Eliminates the proxy pattern by using unified AppState

mod entrypoints;
mod h1;
mod listeners;
mod managed_shutdown;
mod supervisor_host_protocol;
pub(crate) mod tailscale;
mod tls;
mod web_service;

pub use entrypoints::{
    run, run_with_bind, run_with_bind_and_static, run_with_bind_and_static_tls, run_with_bind_tls,
    run_with_managed_shutdown, run_with_native_scope_confirmation, run_with_tls,
};
pub use managed_shutdown::MANAGED_SHUTDOWN_DIR_ENV;
pub use supervisor_host_protocol::native_scope_confirmation_helper_entry;
pub use tailscale::helper_entry as tailscale_serve_helper_entry;
pub use web_service::WebService;

#[cfg(test)]
mod tests;
