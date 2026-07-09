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

use dashmap::DashMap;

/// Per-session count of live SSE/WS client subscribers.
#[derive(Default)]
pub struct SessionWatchers {
    counts: DashMap<String, usize>,
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
