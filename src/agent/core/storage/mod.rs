//! Persistent storage backends
//!
//! This module provides storage implementations for persisting agent data,
//! such as conversation history, session state, and other artifacts.
//!
//! # Available Backends
//!
//! - **JsonlStorage**: JSON Lines format for append-only logs
//!
//! # Usage
//!
//! ```rust,ignore
//! use bamboo_agent::agent::core::storage::{Storage, JsonlStorage};
//!
//! let storage = JsonlStorage::new("sessions.jsonl")?;
//! storage.append(session_id, &data).await?;
//! let data = storage.read(session_id).await?;
//! ```

pub mod jsonl;
pub mod v2;

pub use jsonl::{JsonlStorage, Storage};
pub use v2::{CleanupMode, CleanupResult, SessionIndexEntry, SessionStoreV2, SessionsIndex};

/// Attachment reader for `bamboo-attachment://<session_id>/<attachment_id>` references.
///
/// This is used to keep session storage free of base64 while still allowing the
/// agent loop to send data URLs upstream (most providers expect either HTTP(S)
/// URLs or `data:` URLs for images).
#[async_trait::async_trait]
pub trait AttachmentReader: Send + Sync {
    async fn read_attachment(
        &self,
        session_id: &str,
        attachment_id: &str,
    ) -> std::io::Result<Option<(Vec<u8>, String)>>;
}
