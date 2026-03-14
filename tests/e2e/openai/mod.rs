//! E2E tests for OpenAI-compatible endpoints (/v1/chat/completions and /v1/models)

use actix_web::{test, web, App};
use bamboo_agent::server::handlers::openai;
use serde_json::json;

mod chat_completions;
mod models;
