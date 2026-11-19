//! Session-scoped event sender management.
//!
//! Provides long-lived broadcast senders for session event streams.
//! Unlike runner-scoped senders (which exist only during agent execution),
//! these persist for the lifetime of the session.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{broadcast, RwLock};

use bamboo_agent_core::AgentEvent;

use super::event_forwarder::SessionAccountFeed;
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
    account_feed: SessionAccountFeed,
}

impl ReplayableSessionEventPublisher {
    pub(crate) fn new(
        runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
        senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
        account_feed: SessionAccountFeed,
    ) -> Self {
        Self {
            runners,
            senders,
            account_feed,
        }
    }

    pub(crate) async fn publish(&self, session_id: &str, event: AgentEvent) {
        self.publish_with_completion_id(session_id, event, None)
            .await;
    }

    pub(crate) async fn publish_with_completion_id(
        &self,
        session_id: &str,
        event: AgentEvent,
        completion_id: Option<String>,
    ) {
        debug_assert!(event.is_replayable_session_state());
        let account = self
            .account_feed
            .reserve(session_id, &event, completion_id)
            .await
            .unwrap_or_else(|error| {
                tracing::warn!(%error, "session event could not enter the account change feed");
                None
            });
        // Resolve the sender before the runner lock. The SSE/WS subscription
        // boundary uses the same sender-then-runner lock order.
        let sender = get_or_create_event_sender(&self.senders, session_id).await;
        let mut runners = self.runners.write().await;
        if let Some(runner) = runners.get_mut(session_id) {
            runner.push_critical_event(event.clone());
        }
        if let Some(account) = account {
            account.enqueue(&event);
        }
        let _ = sender.send(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn durable_session_publication_waits_for_a_full_account_inbox() {
        let runners = Arc::new(RwLock::new(HashMap::new()));
        let senders = Arc::new(RwLock::new(HashMap::new()));
        let mut session_events = get_or_create_event_sender(&senders, "parent")
            .await
            .subscribe();
        let (account, mut account_events) = mpsc::channel(1);
        account
            .try_send((
                Some("queued".into()),
                AgentEvent::SessionHistoryCommitted {
                    session_id: "queued".into(),
                },
            ))
            .unwrap();
        let publisher = ReplayableSessionEventPublisher::new(
            runners.clone(),
            senders,
            SessionAccountFeed::Legacy(Some(account)),
        );
        let publication = publisher.publish(
            "parent",
            AgentEvent::SubAgentStarted {
                parent_session_id: "parent".into(),
                child_session_id: "child".into(),
                title: None,
            },
        );
        tokio::pin!(publication);
        assert!(futures::poll!(&mut publication).is_pending());
        assert!(runners.try_write().is_ok(), "no runner lock while waiting");
        assert!(matches!(
            session_events.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));

        let (first_route, first_event) = account_events.recv().await.unwrap();
        assert_eq!(first_route.as_deref(), Some("queued"));
        assert!(matches!(
            first_event,
            AgentEvent::SessionHistoryCommitted { .. }
        ));
        publication.await;
        let (second_route, second_event) = account_events.try_recv().unwrap();
        assert_eq!(second_route.as_deref(), Some("parent"));
        assert!(matches!(second_event, AgentEvent::SubAgentStarted { .. }));
        assert!(matches!(
            session_events.try_recv(),
            Ok(AgentEvent::SubAgentStarted { .. })
        ));
    }
}
