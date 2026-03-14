//! E2E tests for /api/v1/mcp/* endpoints.

use actix_web::{test, web, App};
use bamboo_agent::server::handlers;
use serde_json::json;

mod connections;
mod servers;
mod tools;
