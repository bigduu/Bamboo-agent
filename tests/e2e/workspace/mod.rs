//! E2E tests for `/v1/workspace/*` endpoints.

use actix_web::{test, web, App};
use bamboo_agent::server::handlers::workspace;
use serde_json::json;
use tempfile::TempDir;

mod browse_folder;
mod files;
mod integration;
mod recent;
mod suggestions;
mod validate;
