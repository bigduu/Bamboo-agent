//! Child-session completion notification primitives.
//!
//! The engine owns child runner lifecycle, but parent resume policy lives in the
//! server/application layer.  This module defines the small callback boundary
//! between them so child completion is event-driven without making the engine
//! depend on `AppState`.

use async_trait::async_trait;
use chrono::{DateTime, Utc};

/// Terminal child-session completion recorded by the child runner lifecycle.
#[derive(Debug, Clone)]
pub struct ChildCompletion {
    pub parent_session_id: String,
    pub child_session_id: String,
    /// One of: completed | cancelled | error | skipped | timeout.
    pub status: String,
    pub error: Option<String>,
    pub completed_at: DateTime<Utc>,
}

/// Application-layer callback invoked when a child session reaches a terminal
/// state. Implementations must be idempotent: duplicate completion events for
/// the same child can occur when watchdog timeout races normal runner teardown.
#[async_trait]
pub trait ChildCompletionHandler: Send + Sync {
    async fn on_child_completed(&self, completion: ChildCompletion);
}
