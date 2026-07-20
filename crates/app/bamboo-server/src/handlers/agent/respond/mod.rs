//! User response API handler for interactive agent questions.
//!
//! This module provides HTTP endpoints for submitting user responses
//! when the agent asks questions via the `conclusion_with_options` tool.

mod handlers;
mod session;
mod types;

pub use handlers::{get_pending_question, submit_permission_decision, submit_response};
pub use types::RespondRequest;
