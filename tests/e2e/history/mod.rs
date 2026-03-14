//! E2E tests for /api/v1/history/{session_id} endpoint.

use actix_web::{test, web, App};
use bamboo_agent::server::handlers;

mod endpoint;
mod sessions;
