//! Delivery sinks for classified notifications.
//!
//! `bamboo_notification::NotificationService::notify` (backend policy) and
//! [`crate::app_state::session_events`] already decide WHETHER an event is
//! notification-worthy and materialize it as an `AgentEvent::Notification`
//! for SSE/WS clients. This module is the next stage: WHERE else to deliver
//! that notification — an OS-native popup ([`desktop`]) and/or push-relay
//! services ([`ntfy`], [`bark`]).
//!
//! Sinks are constructed fresh from the CURRENT config on every dispatch (see
//! [`dispatch_to_sinks`]) rather than once at startup — config is
//! hot-reloadable, and a sink built once would silently keep serving a stale
//! topic/token/toggle forever. Every sink's `deliver` is non-blocking and
//! swallows its own errors (logged via `tracing::warn`/`debug`): a failed
//! notification must never break an agent run.

mod bark;
mod desktop;
mod ntfy;

pub use desktop::{is_sidecar_mode, set_sidecar_mode};

use bamboo_llm::Config;

/// An owned, sink-agnostic notification payload.
///
/// Deliberately decoupled from `AgentEvent::Notification` (whose fields are
/// wire-shaped for the SSE/WS client, e.g. carrying a dedup key and a
/// timestamp sinks have no use for) — see [`SinkNotification::from_event`].
#[derive(Debug, Clone, PartialEq)]
pub struct SinkNotification {
    pub title: String,
    pub body: String,
    /// Stable wire category, e.g. `"needs_approval"`, `"run_completed"` (see
    /// `bamboo_notification::policy::NotificationCategory::as_str`).
    pub category: String,
    /// `"high"` | `"normal"` | `"low"` (see
    /// `bamboo_notification::policy::NotificationPriority::as_str`).
    pub priority: String,
    pub session_id: String,
    /// Deep link to surface in a click-through push notification. Not yet
    /// populated by any producer — reserved for a future frontend route.
    pub click_url: Option<String>,
}

impl SinkNotification {
    /// Builds a sink-facing payload from an already-classified
    /// `AgentEvent::Notification`. Returns `None` for any other event
    /// variant (defensive; callers should only ever pass a `Notification`).
    pub fn from_event(event: &bamboo_agent_core::AgentEvent) -> Option<Self> {
        match event {
            bamboo_agent_core::AgentEvent::Notification {
                session_id,
                category,
                priority,
                title,
                body,
                ..
            } => Some(Self {
                title: title.clone(),
                body: body.clone(),
                category: category.clone(),
                priority: priority.clone(),
                session_id: session_id.clone(),
                click_url: None,
            }),
            _ => None,
        }
    }
}

/// A delivery target for a classified notification.
///
/// `deliver` is deliberately **not** `async`: implementations spawn their own
/// tokio task so a slow/unreachable sink (a hung ntfy server, a wedged D-Bus
/// session) can never stall the caller — the always-on notification relay
/// loop that classifies every session's events.
pub trait NotificationSink: Send + Sync {
    /// Short identifier for logging (e.g. `"desktop"`, `"ntfy"`, `"bark"`).
    #[allow(dead_code)] // surfaced for future structured logging / metrics.
    fn name(&self) -> &'static str;

    /// Delivers `n`. Non-blocking: returns immediately, doing the actual I/O
    /// (D-Bus/mac call, HTTP POST) on a spawned task.
    fn deliver(&self, n: &SinkNotification);
}

/// Fans a classified notification out to every enabled/gated sink, reading
/// `config` (a snapshot the caller just took — see
/// `app_state::session_events::ensure_notification_relay`) so a hot-reloaded
/// topic/token/toggle takes effect on the very next notification.
///
/// `has_watcher` is whether the session currently has ≥1 live SSE/WS client
/// (`app_state::watchers::SessionWatchers`, NOT `broadcast::receiver_count` —
/// that count is inflated by the relay's own subscription). It only
/// suppresses the desktop popup for categories the UI already surfaces
/// (`run_completed`, `needs_*`) — push sinks (ntfy/bark) are never suppressed
/// by a watcher: a phone push is still useful while the desktop UI is open.
pub fn dispatch_to_sinks(config: &Config, has_watcher: bool, notification: &SinkNotification) {
    if desktop::desktop_enabled(
        config.notifications.desktop.enabled,
        desktop::is_sidecar_mode(),
    ) && !desktop::suppressed_by_watcher(&notification.category, has_watcher)
    {
        desktop::DesktopSink.deliver(notification);
    }

    if config.notifications.ntfy.enabled {
        ntfy::NtfySink::from_config(&config.notifications.ntfy).deliver(notification);
    }

    if config.notifications.bark.enabled {
        bark::BarkSink::from_config(&config.notifications.bark).deliver(notification);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> bamboo_agent_core::AgentEvent {
        bamboo_agent_core::AgentEvent::Notification {
            id: "n1".to_string(),
            session_id: "sess-1".to_string(),
            category: "needs_approval".to_string(),
            priority: "high".to_string(),
            title: "Approval needed".to_string(),
            body: "run `rm -rf`?".to_string(),
            dedup_key: Some("dk".to_string()),
            created_at: "2026-01-01T00:00:00Z".to_string(),
        }
    }

    #[test]
    fn from_event_extracts_sink_fields() {
        let sink = SinkNotification::from_event(&sample()).unwrap();
        assert_eq!(sink.title, "Approval needed");
        assert_eq!(sink.body, "run `rm -rf`?");
        assert_eq!(sink.category, "needs_approval");
        assert_eq!(sink.priority, "high");
        assert_eq!(sink.session_id, "sess-1");
        assert_eq!(sink.click_url, None);
    }

    #[test]
    fn from_event_rejects_non_notification_variants() {
        let event = bamboo_agent_core::AgentEvent::Token {
            content: "hi".to_string(),
        };
        assert!(SinkNotification::from_event(&event).is_none());
    }

    #[test]
    fn dispatch_to_sinks_does_not_panic_with_everything_disabled() {
        // No enabled channels (desktop explicitly off, ntfy/bark default off)
        // — must be a pure no-op, not a panic or a hang.
        let mut config = Config::default();
        config.notifications.desktop.enabled = Some(false);
        let notification = SinkNotification::from_event(&sample()).unwrap();
        dispatch_to_sinks(&config, false, &notification);
    }
}
