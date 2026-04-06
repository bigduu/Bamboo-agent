//! HTTP API handlers for the Bamboo agent server.
//!
//! This module contains all HTTP request handlers for the Bamboo API,
//! providing unified endpoint handling for all API types.
//!
//! # Handler Organization
//!
//! - **agent/** - Core agent API handlers (chat, execute, events, etc.)
//! - **command.rs** - Command execution endpoints
//! - **openai/** - OpenAI-compatible API endpoints
//! - **anthropic/** - Anthropic Claude API endpoints
//! - **gemini.rs** - Google Gemini API endpoints
//! - **settings/** - Configuration management
//! - **skill.rs** - Skill management endpoints
//! - **tools.rs** - Tool execution endpoints
//! - **workspace.rs** - Workspace management
//! - **copilot_auth/** - GitHub Copilot authentication
//!
//! # API Architecture
//!
//! Bamboo uses a two-step execution model for better reliability:
//!
//! 1. **Create Chat**: `POST /api/v1/chat` creates a session and adds messages
//! 2. **Execute**: `POST /api/v1/execute/{session_id}` starts agent execution
//! 3. **Subscribe**: `GET /api/v1/events/{session_id}` receives real-time events

// Agent API handlers (core functionality)
pub mod agent;

// Re-export core agent handlers at the top level for backward compatibility.
// Historically these lived at `crate::server::handlers::{chat, execute, ...}`.
pub use agent::{
    chat, delete, events, execute, health, history, mcp, metrics, respond, stop, task,
};

// Multi-provider API handlers
pub mod anthropic;
pub mod command;
pub mod copilot_auth;
pub mod gemini;
pub mod openai;
pub mod settings;
pub mod skill;
pub mod tools;
pub mod workspace;
