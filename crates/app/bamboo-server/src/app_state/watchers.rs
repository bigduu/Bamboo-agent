//! Live client-subscriber counting per session.
//!
//! Deliberately **not** `broadcast::Sender::receiver_count()` — that count is
//! inflated by the always-on notification relay's own subscription (see
//! [`super::session_events::ensure_notification_relay`]), so it can never
//! distinguish "just the relay is listening" from "a real client is
//! watching". This tracks real client connect/disconnect explicitly: the SSE
//! handler (`handlers::agent::events::handler`) and the WS `ws_v2`
//! agent-channel subscribe each hold a [`WatcherGuard`] for the lifetime of
//! their subscription.
//!
//! Used to suppress a redundant desktop popup for categories the UI already
//! surfaces while a client is actively watching a session (see
//! `notify_sinks::desktop::suppressed_by_watcher`).

use std::sync::Arc;

use bamboo_agent_core::AgentEvent;
use dashmap::DashMap;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

/// Per-session count of live SSE/WS client subscribers.
#[derive(Default)]
pub struct SessionWatchers {
    counts: DashMap<String, usize>,
    /// Internal observers are tracked by exact channel, independently of UI
    /// watchers. A relay cannot keep its own terminal session alive forever.
    relays: DashMap<String, RelayRegistration>,
}

struct RelayRegistration {
    channel: broadcast::WeakSender<AgentEvent>,
    generation: Arc<()>,
    cancel: CancellationToken,
    started_at: chrono::DateTime<chrono::Utc>,
}

/// Owns both the internal-observer registration and its actual receiver.
/// Drop unregisters before Rust drops the receiver, including task abort and
/// unwind, so eviction can never subtract an observer that has already left.
pub(crate) struct NotificationRelaySubscription {
    watchers: Arc<SessionWatchers>,
    service: Arc<bamboo_notification::NotificationService>,
    session_id: String,
    generation: Arc<()>,
    cancel: CancellationToken,
    receiver: broadcast::Receiver<AgentEvent>,
}

impl NotificationRelaySubscription {
    pub(crate) async fn recv(&mut self) -> Result<AgentEvent, broadcast::error::RecvError> {
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => Err(broadcast::error::RecvError::Closed),
            event = self.receiver.recv() => event,
        }
    }
}

impl Drop for NotificationRelaySubscription {
    fn drop(&mut self) {
        if let dashmap::mapref::entry::Entry::Occupied(entry) =
            self.watchers.relays.entry(self.session_id.clone())
        {
            if Arc::ptr_eq(&entry.get().generation, &self.generation) {
                // Clear the compatibility marker while still holding the entry
                // lock; a new generation cannot race between clear and remove.
                self.service.end_relay(&self.session_id);
                entry.remove();
            }
        }
    }
}

impl SessionWatchers {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Registers one live watcher for `session_id`. Private: callers must go
    /// through [`WatcherGuard::new`] so the decrement can never be forgotten.
    fn watch(&self, session_id: &str) {
        *self.counts.entry(session_id.to_string()).or_insert(0) += 1;
    }

    /// Removes one live watcher for `session_id`, dropping the entry once the
    /// count reaches zero so the map doesn't grow unboundedly over the
    /// server's lifetime.
    fn unwatch(&self, session_id: &str) {
        let mut drop_entry = false;
        if let Some(mut count) = self.counts.get_mut(session_id) {
            *count = count.saturating_sub(1);
            drop_entry = *count == 0;
        }
        if drop_entry {
            self.counts.remove(session_id);
        }
    }

    /// Whether `session_id` currently has ≥1 live watcher.
    pub fn has_watcher(&self, session_id: &str) -> bool {
        self.counts.get(session_id).is_some_and(|count| *count > 0)
    }

    pub(crate) fn begin_notification_relay(
        self: &Arc<Self>,
        service: Arc<bamboo_notification::NotificationService>,
        session_id: &str,
        sender: &broadcast::Sender<AgentEvent>,
    ) -> Option<NotificationRelaySubscription> {
        let entry = self.relays.entry(session_id.to_string());
        if let dashmap::mapref::entry::Entry::Occupied(occupied) = &entry {
            if occupied
                .get()
                .channel
                .upgrade()
                .is_some_and(|channel| channel.same_channel(sender))
            {
                return None;
            }
            // A channel recreated after eviction needs a new relay immediately.
            // The old task may still be draining; it cannot clear this successor.
            occupied.get().cancel.cancel();
            service.end_relay(session_id);
        }
        let receiver = sender.subscribe();
        let generation = Arc::new(());
        let cancel = CancellationToken::new();
        let registration = RelayRegistration {
            channel: sender.downgrade(),
            generation: generation.clone(),
            cancel: cancel.clone(),
            started_at: chrono::Utc::now(),
        };
        service.try_begin_relay(session_id);
        entry.insert(registration);
        Some(NotificationRelaySubscription {
            watchers: self.clone(),
            service,
            session_id: session_id.to_string(),
            generation,
            cancel,
            receiver,
        })
    }

    /// Count every real receiver, excluding only our live relay on this exact
    /// channel. Holding the entry guard across receiver_count prevents its
    /// RAII owner from unregistering/dropping between those two observations.
    pub(crate) fn has_external_receivers(
        &self,
        session_id: &str,
        sender: &broadcast::Sender<AgentEvent>,
    ) -> bool {
        let relay = self.relays.get(session_id);
        let internal = usize::from(relay.as_ref().is_some_and(|relay| {
            relay
                .channel
                .upgrade()
                .is_some_and(|channel| channel.same_channel(sender))
        }));
        sender.receiver_count() > internal
    }

    pub(crate) fn relay_started_at(
        &self,
        session_id: &str,
        sender: &broadcast::Sender<AgentEvent>,
    ) -> Option<chrono::DateTime<chrono::Utc>> {
        let relay = self.relays.get(session_id)?;
        relay
            .channel
            .upgrade()
            .filter(|channel| channel.same_channel(sender))
            .map(|_| relay.started_at)
    }
}

/// RAII guard: increments the watcher count for `session_id` on creation,
/// decrements it on drop.
///
/// Covers both graceful stream completion and a forced
/// `tokio::task::JoinHandle::abort()` — Tokio runs an aborted task's local
/// drop glue at its next cancellation point, so a guard held as a local
/// inside a spawned SSE/WS forwarder task decrements correctly either way.
pub struct WatcherGuard {
    watchers: Arc<SessionWatchers>,
    session_id: String,
}

impl WatcherGuard {
    pub fn new(watchers: Arc<SessionWatchers>, session_id: &str) -> Self {
        watchers.watch(session_id);
        Self {
            watchers,
            session_id: session_id.to_string(),
        }
    }
}

impl Drop for WatcherGuard {
    fn drop(&mut self) {
        self.watchers.unwatch(&self.session_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn notification_service() -> (
        Arc<bamboo_notification::NotificationService>,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().unwrap();
        (
            Arc::new(bamboo_notification::NotificationService::new(
                dir.path().join("prefs.json"),
            )),
            dir,
        )
    }

    #[tokio::test]
    async fn relay_generation_does_not_hide_replacement_channel_subscribers() {
        let watchers = SessionWatchers::new();
        let (service, _dir) = notification_service();
        let (old, _) = broadcast::channel(8);
        let old_relay = watchers
            .begin_notification_relay(service.clone(), "s", &old)
            .unwrap();
        assert!(!watchers.has_external_receivers("s", &old));
        let (replacement, _ui_receiver) = broadcast::channel(8);
        assert!(watchers.has_external_receivers("s", &replacement));
        let new_relay = watchers
            .begin_notification_relay(service.clone(), "s", &replacement)
            .unwrap();
        drop(old_relay);
        assert!(
            !service.try_begin_relay("s"),
            "old Drop cannot clear the new registration"
        );
        assert!(watchers.has_external_receivers("s", &replacement));
        drop(new_relay);
        assert!(service.try_begin_relay("s"));
        service.end_relay("s");
    }

    #[tokio::test]
    async fn aborted_and_dropped_relay_tasks_release_receiver_and_registration() {
        let watchers = SessionWatchers::new();
        let (service, _dir) = notification_service();
        let (sender, _) = broadcast::channel(8);
        let subscription = watchers
            .begin_notification_relay(service.clone(), "aborted", &sender)
            .unwrap();
        let task = tokio::spawn(async move {
            let _subscription = subscription;
            std::future::pending::<()>().await;
        });
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(sender.receiver_count(), 0);
        assert!(service.try_begin_relay("aborted"));
        service.end_relay("aborted");

        let subscription = watchers
            .begin_notification_relay(service.clone(), "failed-setup", &sender)
            .unwrap();
        drop(subscription);
        assert_eq!(sender.receiver_count(), 0);
        assert!(service.try_begin_relay("failed-setup"));
        service.end_relay("failed-setup");
    }

    #[test]
    fn watch_and_unwatch_tracks_presence() {
        let watchers = SessionWatchers::new();
        assert!(!watchers.has_watcher("s1"));

        let guard_one = WatcherGuard::new(watchers.clone(), "s1");
        assert!(watchers.has_watcher("s1"));

        let guard_two = WatcherGuard::new(watchers.clone(), "s1");
        assert!(watchers.has_watcher("s1"));

        drop(guard_one);
        assert!(watchers.has_watcher("s1"), "one watcher remains");

        drop(guard_two);
        assert!(!watchers.has_watcher("s1"));
    }

    #[test]
    fn distinct_sessions_are_tracked_independently() {
        let watchers = SessionWatchers::new();
        let _guard = WatcherGuard::new(watchers.clone(), "s1");
        assert!(watchers.has_watcher("s1"));
        assert!(!watchers.has_watcher("s2"));
    }

    #[test]
    fn repeated_watch_unwatch_never_underflows_or_panics() {
        let watchers = SessionWatchers::new();
        for _ in 0..5 {
            let guard = WatcherGuard::new(watchers.clone(), "s3");
            drop(guard);
        }
        assert!(!watchers.has_watcher("s3"));
    }
}
