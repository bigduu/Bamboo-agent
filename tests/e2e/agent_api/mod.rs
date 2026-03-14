//! E2E tests for Claude Code integration endpoints (`/v1/agent/*`).

use std::path::PathBuf;

use actix_web::{test, web, App};
use bamboo_agent::server::app_state::{AgentRunner, AgentStatus};
use bamboo_agent::server::handlers::agent_api;
use serde_json::{json, Value};

mod integration;
mod projects;
mod sessions;
mod settings;
mod system_prompt;

/// Creates a temporary project directory for testing.
fn create_temp_project() -> PathBuf {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    temp_dir.keep()
}
