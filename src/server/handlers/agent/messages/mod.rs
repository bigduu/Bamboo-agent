//! Message management endpoints (delete/truncate).
//!
//! These endpoints mutate a session's persisted message history.

mod delete;
mod restore;
mod shared;
mod truncate;
mod types;

pub use delete::delete_message;
pub use restore::restore_session_state;
pub use truncate::truncate_messages;
pub use types::{RestoreSessionRequest, TruncateRequest};
