//! E2E tests for /v1/bamboo/copilot/* endpoints.

use actix_web::{test, web, App};
use bamboo_agent::server::handlers::copilot_auth;
use serde_json::json;

mod authenticate;
mod complete;
mod integration;
mod logout;
mod start;
mod status;
