//! Message management endpoints (delete/truncate/patch).
//!
//! These endpoints mutate a session's persisted message history.

mod delete;
mod patch;
mod restore;
mod shared;
mod truncate;
mod types;

pub use delete::delete_message;
pub use patch::patch_message;
pub use restore::restore_session_state;
pub use truncate::truncate_messages;
pub use types::{PatchMessageRequest, RestoreSessionRequest, TruncateRequest};
