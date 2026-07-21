//! End-to-end flow tests for the unified config system.
//!
//! These tests exercise multiple endpoints in sequence to catch regressions where:
//! - config patches clobber other sections (lost updates)
//! - permissive config endpoint incorrectly enforces provider validation
//! - strict provider endpoint returns proper HTTP codes and messages
//! - config patch sanitization prevents encrypted material injection

use actix_web::{test, App};
use bamboo_agent::server::configure_routes;
use serde_json::json;

mod flows;
mod validation;

fn read_config_json(path: &std::path::Path) -> serde_json::Value {
    let raw = std::fs::read_to_string(path).expect("config.json should be readable");
    serde_json::from_str(&raw).expect("config.json should be valid JSON")
}

/// Accept both legacy raw sidecars and revisioned #597 envelopes while a
/// watcher-driven migration may race the first read in an end-to-end flow.
fn config_document_data(document: &serde_json::Value) -> &serde_json::Value {
    document.get("data").unwrap_or(document)
}
