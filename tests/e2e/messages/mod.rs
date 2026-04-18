//! E2E tests for message mutation endpoints (delete/truncate/restore).

use actix_web::{test, web, App};
use bamboo_agent::agent::{Message, Session};
use bamboo_agent::server::handlers::agent::messages;
use serde_json::json;

mod delete;
mod patch;
mod restore;
mod truncate;
