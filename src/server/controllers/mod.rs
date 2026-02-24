//! HTTP request controllers (handlers)
//!
//! This module organizes HTTP request handlers by domain/feature area.
//! Each controller handles a specific set of related endpoints.

pub mod agent_controller;
pub mod anthropic;
pub use anthropic::*;
pub mod command_controller;
pub mod copilot_auth_controller;
pub mod gemini_controller;
pub mod openai_controller;
pub mod settings_controller;
pub mod skill_controller;
pub mod tools_controller;
pub mod workspace_controller;
