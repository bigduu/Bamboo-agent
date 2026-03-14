//! E2E tests for /v1/commands/* endpoints.

use actix_web::{test, web, App};
use bamboo_agent::server::handlers::command;
use serde_json::Value;

mod get;
mod list;
