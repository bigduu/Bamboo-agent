//! Concrete storage implementations for Bamboo agent sessions.
//!
//! Provides persistent storage backends:
//! - **JsonlStorage**: Simple JSON file per session
//! - **SessionStoreV2**: Folder-per-session layout with SQLite search index
//! - **SessionSearchIndex**: Full-text search for session content

pub mod jsonl;
pub mod search_index;
pub mod session_merge;
pub mod v2;

pub use jsonl::JsonlStorage;
pub use search_index::{SessionSearchIndex, SessionSearchMatch};
pub use session_merge::{merge_save_session, LockedSessionStore};
pub use v2::{CleanupMode, CleanupResult, SessionIndexEntry, SessionStoreV2, SessionsIndex};
