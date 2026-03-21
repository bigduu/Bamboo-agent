//! E2E tests for /api/v1/task/* endpoints.

use actix_web::{test, web, App};
use bamboo_agent::server::handlers;

mod endpoint;
mod sessions;
