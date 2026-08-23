//! Session-scoped event sender management.
//!
//! Provides long-lived broadcast senders for session event streams.
//! Unlike runner-scoped senders (which exist only during agent execution),
//! these persist for the lifetime of the session.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{broadcast, RwLock};

use bamboo_agent_core::AgentEvent;

use super::event_forwarder::AccountFeedInbox;
use super::runner_state::AgentRunner;

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

/// Atomic cache + account-feed + broadcast publisher for replayable session
/// events produced outside an agent event forwarder.
///
/// Subscribers establish their receiver and clone `last_critical_events` while
/// holding the matching runner read lock. Keeping cache mutation and broadcast
/// inside the write lock means each publication appears either in that replay
/// snapshot or in the live receiver, never both and never neither.
#[derive(Clone)]
pub(crate) struct ReplayableSessionEventPublisher {
    runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
    account_feed_inbox: Option<AccountFeedInbox>,
}

impl ReplayableSessionEventPublisher {
    pub(crate) fn new(
        runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
        senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
        account_feed_inbox: Option<AccountFeedInbox>,
    ) -> Self {
        Self {
            runners,
            senders,
            account_feed_inbox,
        }
    }

    pub(crate) async fn publish(&self, session_id: &str, event: AgentEvent) {
        debug_assert!(event.is_replayable_session_state());
        // Resolve the sender before the runner lock. The SSE/WS subscription
        // boundary uses the same sender-then-runner lock order.
        let sender = get_or_create_event_sender(&self.senders, session_id).await;
        let mut runners = self.runners.write().await;
        if let Some(runner) = runners.get_mut(session_id) {
            runner.push_critical_event(event.clone());
        }
        if let Some(inbox) = &self.account_feed_inbox {
            if event.is_durable_change() {
                let route_session_id = event.session_id().unwrap_or(session_id);
                let _ = inbox.try_send((Some(route_session_id.to_string()), event.clone()));
            }
        }
        let _ = sender.send(event);
    }
}
