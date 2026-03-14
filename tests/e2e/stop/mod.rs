//! E2E tests for /api/v1/stop/{session_id} endpoint.

use actix_web::{test, web, App};
use bamboo_agent::server::handlers;

mod endpoint;
mod sessions;
