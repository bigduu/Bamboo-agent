//! HTTP API handlers for the Bamboo agent server.
//!
//! This module contains all HTTP request handlers for the Bamboo API,
//! providing endpoints for chat, agent execution, event streaming,
//! session management, and tool execution.
//!
//! # API Architecture
//!
//! Bamboo uses a two-step execution model for better reliability:
//!
//! 1. **Create Chat**: `POST /api/v1/chat` creates a session and adds messages
//! 2. **Execute**: `POST /api/v1/execute/{session_id}` starts agent execution
//! 3. **Subscribe**: `GET /api/v1/events/{session_id}` receives real-time events
//!
//! # Modules
//!
//! - [`chat`] - Create chat sessions and messages
//! - [`execute`] - Trigger agent execution
//! - [`events`] - Subscribe to agent events via SSE
//! - [`stream`] - Legacy streaming endpoint (deprecated)
//! - [`delete`] - Delete sessions
//! - [`history`] - Retrieve session history
//! - [`health`] - Health check endpoint
//! - [`stop`] - Stop running executions
//! - [`respond`] - Handle interactive agent questions
//! - [`todo`] - Todo list management
//! - [`mcp`] - MCP (Model Context Protocol) endpoints
//! - [`metrics`] - Agent metrics and monitoring
//!
//! # Example Usage
//!
//! ```bash
//! # Create a chat
//! curl -X POST http://localhost:8080/api/v1/chat \
//!   -H "Content-Type: application/json" \
//!   -d '{"message": "Hello", "model": "gpt-4o-mini"}'
//!
//! # Execute the agent
//! curl -X POST http://localhost:8080/api/v1/execute/{session_id} \
//!   -H "Content-Type: application/json" \
//!   -d '{"model": "gpt-4o-mini"}'
//!
//! # Subscribe to events (JavaScript)
//! const events = new EventSource('/api/v1/events/{session_id}');
//! events.onmessage = (e) => console.log(JSON.parse(e.data));
//! ```

pub mod chat;
pub mod delete;
pub mod events;
pub mod execute;
pub mod health;
pub mod history;
pub mod mcp;
pub mod metrics;
pub mod respond;
pub mod stop;
pub mod stream;
pub mod todo;
