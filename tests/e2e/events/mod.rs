//! E2E tests for /api/v1/events/{session_id} endpoint (SSE streaming).

use actix_web::{test, web, App};
use bamboo_agent::agent::{Message, Session};
use bamboo_agent::server::app_state::{AgentRunner, AgentStatus};
use bamboo_agent::server::handlers;

mod basic;
mod terminal;
