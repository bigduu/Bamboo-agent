//! E2E tests for /api/v1/metrics/* endpoints.

use actix_web::{test, web, App};
use bamboo_agent::server::handlers;

mod core;
mod v2;
