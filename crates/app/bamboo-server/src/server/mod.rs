//! Unified HTTP server entry points
//!
//! Consolidates run(), run_with_bind(), WebService from web_service/server.rs
//! Eliminates the proxy pattern by using unified AppState

mod entrypoints;
mod h1;
mod listeners;
mod supervisor_host_protocol;
mod tls;
mod web_service;

pub use entrypoints::{
    run, run_with_bind, run_with_bind_and_static, run_with_bind_and_static_tls, run_with_bind_tls,
    run_with_native_scope_confirmation, run_with_tls,
};
pub use supervisor_host_protocol::native_scope_confirmation_helper_entry;
pub use web_service::WebService;

#[cfg(test)]
mod tests;
