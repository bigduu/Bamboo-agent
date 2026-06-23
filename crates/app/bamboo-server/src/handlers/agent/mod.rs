//! Core agent API handlers
//!
//! These handlers provide the core agent functionality including
//! chat, execution, event streaming, session management, and MCP.

pub mod chat;
pub mod child_approval;
pub mod delete;
pub mod dev;
pub mod events;
pub mod execute;
pub mod health;
pub mod history;
pub mod mcp;
pub mod messages;
pub mod metrics;
pub mod notifications;
pub mod prompt_presets;
pub mod respond;
pub mod schedules;
pub mod sessions;
pub mod stop;
pub mod stream;
pub mod task;
pub mod ws_v2;
