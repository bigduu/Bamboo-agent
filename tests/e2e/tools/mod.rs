//! E2E tests for `/v1/tools/execute` endpoint.

use actix_web::{test, web, App};
use bamboo_agent::server::handlers::tools;
use serde_json::{json, Value};

mod basic;
mod compact;
mod edit_flow;
mod filesystem;
