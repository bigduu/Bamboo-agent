//! Session storage backends for Bamboo agents.
//!
//! Provides persistent storage implementations:
//! - **JsonlStorage**: Simple JSON file per session
//! - **SessionStoreV2**: Folder-per-session layout with SQLite search index
//! - **SessionSearchIndex**: Full-text search for session content
//! - **merge_save_session**: Merge-aware session save with concurrent edit protection

pub mod jsonl;
pub mod search_index;
pub mod session_inbox;
pub mod session_merge;
pub mod v2;

pub use jsonl::JsonlStorage;
pub use search_index::{SessionSearchIndex, SessionSearchMatch};
pub use session_inbox::FileSessionInbox;
pub use session_merge::{merge_save_session, LockedSessionStore};
pub use v2::{
    CleanupMode, CleanupResult, DurationMetricsSnapshot, SavePersistenceMetricsSnapshot,
    SessionCopyProjectionGuard, SessionIndexEntry, SessionPersistenceMetricsSnapshot,
    SessionPlacement, SessionStoreV2, SessionsIndex,
};
