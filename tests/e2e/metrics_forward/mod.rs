//! E2E tests for /api/v1/metrics/forward/* endpoints.

use actix_web::{test, web, App};
use bamboo_agent::server::handlers;

mod by_endpoint;
mod requests;
mod summary;
