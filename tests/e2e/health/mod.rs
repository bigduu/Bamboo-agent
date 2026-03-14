//! E2E tests for /api/v1/health endpoint.

use actix_web::{test, web, App};
use bamboo_agent::server::handlers;

mod body;
mod endpoint;
