//! E2E tests for `/v1/bamboo/*` settings endpoints.

use actix_web::{test, web, App};
use bamboo_agent::server::handlers::settings;
use serde_json::json;

mod config;
mod invalid_inputs;
mod keyword_masking;
mod provider;
mod setup;
mod workflows;
