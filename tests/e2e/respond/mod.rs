//! E2E tests for /api/v1/respond/* endpoints.

use actix_web::{test, web, App};
use bamboo_agent::server::handlers;
use serde_json::json;

mod pending;
mod submit;
