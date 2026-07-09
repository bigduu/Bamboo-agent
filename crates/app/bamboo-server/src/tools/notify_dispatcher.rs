//! Server adapter implementing `bamboo_server_tools::NotificationDispatcher`
//! for the agent-invocable `notify` tool.
//!
//! Bridges the tool's fire-and-forget port to real delivery:
//! - mints via [`bamboo_notification::NotificationService::mint_custom`] so
//!   the master enabled switch and the shared dedup window apply exactly
//!   like every other notification (WP1's passthrough classification);
//! - re-broadcasts the resulting `AgentEvent::Notification` on the session's
//!   event channel so connected SSE/WS clients see it, same as the always-on
//!   relay (`app_state::session_events::ensure_notification_relay`); and
//! - fans it out to configured delivery sinks (desktop/ntfy/bark) via
//!   [`crate::notify_sinks::dispatch_to_sinks`] (WP3), reading the CURRENT
//!   config so a hot-reloaded topic/token/toggle takes effect immediately.
//!
//! Mirrors [`super::child_session_adapter::ChildSessionAdapter`]'s role: a
//! server-side struct that satisfies a port declared in the
//! framework-agnostic `bamboo-server-tools` crate, so that crate never needs
//! to depend on `AppState` (see its crate doc).

use std::collections::HashMap;
use std::sync::Arc;

use bamboo_agent_core::AgentEvent;
use bamboo_notification::NotificationPriority;
use tokio::sync::{broadcast, RwLock};

use bamboo_server_tools::NotificationDispatcher;

use crate::app_state::watchers::SessionWatchers;

/// Server-side adapter satisfying `NotificationDispatcher` for `NotifyTool`.
pub struct ServerNotificationDispatcher {
    pub notification_service: Arc<bamboo_notification::NotificationService>,
    pub session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
    pub session_watchers: Arc<SessionWatchers>,
    pub config: Arc<RwLock<bamboo_llm::Config>>,
}

impl ServerNotificationDispatcher {
    pub fn new(
        notification_service: Arc<bamboo_notification::NotificationService>,
        session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
        session_watchers: Arc<SessionWatchers>,
        config: Arc<RwLock<bamboo_llm::Config>>,
    ) -> Self {
        Self {
            notification_service,
            session_event_senders,
            session_watchers,
            config,
        }
    }
}

/// Maps the tool's validated `"low" | "normal" | "high"` string to the
/// policy enum. Any other value (there shouldn't be one — `NotifyTool`
/// validates before calling `dispatch`) falls back to `Normal` rather than
/// panicking; a delivery path must never crash a run over a cosmetic field.
fn parse_priority(priority: &str) -> NotificationPriority {
    match priority {
        "high" => NotificationPriority::High,
        "low" => NotificationPriority::Low,
        _ => NotificationPriority::Normal,
    }
}

impl NotificationDispatcher for ServerNotificationDispatcher {
    fn dispatch(
        &self,
        session_id: &str,
        title: &str,
        body: &str,
        priority: &str,
        url: Option<&str>,
    ) {
        let notification_service = self.notification_service.clone();
        let session_event_senders = self.session_event_senders.clone();
        let session_watchers = self.session_watchers.clone();
        let config = self.config.clone();
        let session_id = session_id.to_string();
        let title = title.to_string();
        let body = body.to_string();
        let priority = parse_priority(priority);
        let url = url.map(str::to_string);

        // Non-blocking: the tool call already returned a confirmation to the
        // model. The real classify/broadcast/sink-dispatch work happens here,
        // off that call, so a slow/unreachable sink can never stall the loop
        // (same posture as `NotificationSink::deliver` and the always-on
        // relay in `app_state::session_events`).
        tokio::spawn(async move {
            let Some(event) = notification_service.mint_custom(&session_id, title, body, priority)
            else {
                // Disabled (master switch off) or deduped within the window —
                // not an error, just nothing to deliver.
                return;
            };

            // Build the sink payload before `event` is moved into the
            // broadcast send below (mirrors `ensure_notification_relay`),
            // attaching the tool-supplied click-through URL — the only
            // producer that currently populates `SinkNotification::click_url`.
            let sink_notification =
                crate::notify_sinks::SinkNotification::from_event(&event).map(|mut n| {
                    n.click_url = url;
                    n
                });

            let tx = session_event_senders.read().await.get(&session_id).cloned();
            if let Some(tx) = tx {
                let _ = tx.send(event);
            }

            if let Some(sink_notification) = sink_notification {
                let has_watcher = session_watchers.has_watcher(&session_id);
                let config_snapshot = config.read().await.clone();
                crate::notify_sinks::dispatch_to_sinks(
                    &config_snapshot,
                    has_watcher,
                    &sink_notification,
                );
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn dispatcher() -> (ServerNotificationDispatcher, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let notification_service = Arc::new(bamboo_notification::NotificationService::new(
            dir.path().join("prefs.json"),
        ));
        let dispatcher = ServerNotificationDispatcher::new(
            notification_service,
            Arc::new(RwLock::new(HashMap::new())),
            SessionWatchers::new(),
            Arc::new(RwLock::new(bamboo_llm::Config::default())),
        );
        (dispatcher, dir)
    }

    #[test]
    fn parse_priority_maps_known_values_and_falls_back_to_normal() {
        assert_eq!(parse_priority("high"), NotificationPriority::High);
        assert_eq!(parse_priority("low"), NotificationPriority::Low);
        assert_eq!(parse_priority("normal"), NotificationPriority::Normal);
        assert_eq!(parse_priority("unexpected"), NotificationPriority::Normal);
    }

    #[tokio::test]
    async fn dispatch_broadcasts_a_notification_event_on_the_session_channel() {
        let (dispatcher, _dir) = dispatcher();
        let (tx, mut rx) = broadcast::channel(16);
        dispatcher
            .session_event_senders
            .write()
            .await
            .insert("sess-1".to_string(), tx);

        dispatcher.dispatch("sess-1", "Reminder", "Stand up", "high", None);

        let event = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("dispatch should broadcast within the timeout")
            .unwrap();
        match event {
            AgentEvent::Notification {
                session_id,
                category,
                priority,
                title,
                body,
                ..
            } => {
                assert_eq!(session_id, "sess-1");
                assert_eq!(category, "custom");
                assert_eq!(priority, "high");
                assert_eq!(title, "Reminder");
                assert_eq!(body, "Stand up");
            }
            other => panic!("expected Notification, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn dispatch_is_a_no_op_when_notifications_are_disabled() {
        let (dispatcher, _dir) = dispatcher();
        dispatcher
            .notification_service
            .set_preferences(bamboo_notification::NotificationPreferences {
                enabled: false,
                ..bamboo_notification::NotificationPreferences::default()
            })
            .unwrap();
        let (tx, mut rx) = broadcast::channel(16);
        dispatcher
            .session_event_senders
            .write()
            .await
            .insert("sess-1".to_string(), tx);

        dispatcher.dispatch("sess-1", "Reminder", "Stand up", "normal", None);

        // Give the spawned task a chance to run; it must not send anything.
        let outcome = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await;
        assert!(
            outcome.is_err(),
            "disabled notifications must not broadcast an event"
        );
    }

    #[tokio::test]
    async fn dispatch_with_no_subscriber_does_not_panic() {
        // No entry in `session_event_senders` for this session — dispatch
        // must degrade gracefully (still mints/dedups/sinks, just nothing to
        // broadcast onto), not panic.
        let (dispatcher, _dir) = dispatcher();
        dispatcher.dispatch("no-such-session", "Title", "Body", "low", None);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
