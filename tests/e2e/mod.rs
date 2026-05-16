//! E2E tests for all Bamboo Agent API endpoints
//!
//! This module contains comprehensive end-to-end tests for all HTTP endpoints
//! exposed by the Bamboo Agent server.

#[path = "agent_api/schedules.rs"]
mod agent_api_schedules;
mod anthropic;
mod chat;
mod commands;
mod common;
mod config_flow;
mod copilot_auth;
mod delete;
mod events;
mod execute;
mod gemini;
mod health;
mod history;
mod integration_tests;
mod mcp;
mod messages;
mod metrics;
mod metrics_forward;
mod openai;
mod respond;
mod sessions;
mod settings;
mod skills;
mod stop;
mod task;
mod tools;
mod wiring;
mod workspace;
