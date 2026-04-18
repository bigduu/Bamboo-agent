//! Session-scoped event sender management.
//!
//! Provides long-lived broadcast senders for session event streams.
//! Unlike runner-scoped senders (which exist only during agent execution),
//! these persist for the lifetime of the session.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{broadcast, RwLock};

use bamboo_application_agent::AgentEvent;

/// Default broadcast channel capacity for session event senders.
pub const SESSION_EVENT_CHANNEL_CAPACITY: usize = 1000;

/// Get or create a broadcast sender for the given session.
///
/// If a sender already exists in the map, returns a clone of it.
/// Otherwise creates a new one with [`SESSION_EVENT_CHANNEL_CAPACITY`]
/// capacity and inserts it.
pub async fn get_or_create_event_sender(
    senders: &Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
    session_id: &str,
) -> broadcast::Sender<AgentEvent> {
    {
        let read = senders.read().await;
        if let Some(sender) = read.get(session_id) {
            return sender.clone();
        }
    }

    let mut write = senders.write().await;
    // Double-check after acquiring write lock.
    if let Some(sender) = write.get(session_id) {
        return sender.clone();
    }

    let (sender, _) = broadcast::channel(SESSION_EVENT_CHANNEL_CAPACITY);
    write.insert(session_id.to_string(), sender.clone());
    sender
}
