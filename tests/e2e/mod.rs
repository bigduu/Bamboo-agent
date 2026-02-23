//! E2E tests for all Bamboo Agent API endpoints
//!
//! This module contains comprehensive end-to-end tests for all HTTP endpoints
//! exposed by the Bamboo Agent server.

mod common;
mod chat;
mod execute;
mod events;
mod stream;
mod history;
mod todo;
mod respond;
mod stop;
mod delete;
mod metrics;
mod metrics_forward;
mod health;
mod mcp;
mod integration_tests;
