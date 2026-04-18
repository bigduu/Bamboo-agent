//! Unified HTTP server entry points
//!
//! Consolidates run(), run_with_bind(), WebService from web_service/server.rs
//! Eliminates the proxy pattern by using unified AppState

mod entrypoints;
mod listeners;
mod web_service;

pub use entrypoints::{run, run_with_bind, run_with_bind_and_static};
pub use web_service::WebService;

#[cfg(test)]
mod tests;
