//! E2E tests for /v1/skills endpoints.

use actix_web::{test, web, App};
use bamboo_agent::server::handlers::skill;

mod integration;
mod list;
mod tools;
mod workflows;
