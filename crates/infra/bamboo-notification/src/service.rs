//! Stateful notification service wrapping the pure [`crate::policy`].
//!
//! [`NotificationService`] is the object the server holds (behind an `Arc`).
//! It adds three things to bare classification:
//!
//! - a **dedup window** so a repeated event (same `dedup_key`) does not spam the
//!   user within [`NotificationService::DEDUP_WINDOW`];
//! - **preference persistence** so updates survive a restart; and
//! - **per-session relay idempotency** so the server spawns at most one
//!   notification relay per session.
//!
//! All methods take `&self`; interior mutability is provided by `RwLock`,
//! `Mutex`, and a `DashSet`. The type is intentionally **not** `Clone` — share
//! it via `Arc`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant};

use bamboo_agent_core::AgentEvent;
use dashmap::DashSet;

use crate::policy;
use crate::preferences::NotificationPreferences;

/// Notification policy service: classification + dedup + preference persistence.
///
/// Construct with [`NotificationService::new`] and share via `Arc`.
pub struct NotificationService {
    /// Current user preferences (hot-swappable via [`Self::set_preferences`]).
    preferences: RwLock<NotificationPreferences>,
    /// Last-emitted instant per `dedup_key`, used to suppress rapid repeats.
    dedup: Mutex<HashMap<String, Instant>>,
    /// Session ids that currently have a running relay (server idempotency).
    active_relays: DashSet<String>,
    /// Path the preferences are persisted to.
    prefs_path: PathBuf,
}

impl NotificationService {
    /// Window within which a repeated `dedup_key` is coalesced (suppressed).
    pub const DEDUP_WINDOW: Duration = Duration::from_secs(30);

    /// Creates a service, loading preferences from `prefs_path`.
    ///
    /// A missing or unparsable file falls back to
    /// [`NotificationPreferences::default`] (see [`NotificationPreferences::load`]).
    /// The dedup map and relay set start empty.
    pub fn new(prefs_path: PathBuf) -> Self {
        let preferences = NotificationPreferences::load(&prefs_path);
        Self {
            preferences: RwLock::new(preferences),
            dedup: Mutex::new(HashMap::new()),
            active_relays: DashSet::new(),
            prefs_path,
        }
    }

    /// Classifies `event` and, if notification-worthy and not a recent
    /// duplicate, materialises an [`AgentEvent::Notification`].
    ///
    /// Returns `None` when the policy declines the event (disabled, gated, or
    /// not notification-worthy) or when an identical `dedup_key` fired within
    /// [`Self::DEDUP_WINDOW`]. On a successful (non-deduped) emission the dedup
    /// map is opportunistically pruned of entries older than the window.
    pub fn notify(&self, session_id: &str, event: &AgentEvent) -> Option<AgentEvent> {
        let classified = {
            let prefs = self
                .preferences
                .read()
                .expect("notification preferences lock poisoned");
            policy::classify(session_id, event, &prefs)?
        };

        {
            let mut dedup = self.dedup.lock().expect("notification dedup lock poisoned");
            if let Some(last) = dedup.get(&classified.dedup_key) {
                if last.elapsed() < Self::DEDUP_WINDOW {
                    return None;
                }
            }
            let now = Instant::now();
            dedup.insert(classified.dedup_key.clone(), now);
            // Opportunistically drop entries that can no longer suppress anything.
            dedup.retain(|_, &mut last| last.elapsed() < Self::DEDUP_WINDOW);
        }

        Some(AgentEvent::Notification {
            id: uuid::Uuid::new_v4().to_string(),
            session_id: session_id.to_string(),
            category: classified.category.as_str().to_string(),
            priority: classified.priority.as_str().to_string(),
            title: classified.title,
            body: classified.body,
            dedup_key: Some(classified.dedup_key),
            created_at: chrono::Utc::now().to_rfc3339(),
        })
    }

    /// Returns a snapshot clone of the current preferences.
    pub fn preferences(&self) -> NotificationPreferences {
        self.preferences
            .read()
            .expect("notification preferences lock poisoned")
            .clone()
    }

    /// Replaces the current preferences and persists them to disk.
    ///
    /// The in-memory value is updated first, then written to the configured
    /// path; a write error is returned but the in-memory update still stands.
    pub fn set_preferences(&self, prefs: NotificationPreferences) -> std::io::Result<()> {
        {
            let mut guard = self
                .preferences
                .write()
                .expect("notification preferences lock poisoned");
            *guard = prefs.clone();
        }
        prefs.save(&self.prefs_path)
    }

    /// Marks `session_id` as having a running relay.
    ///
    /// Returns `true` when the session was newly inserted — i.e. the caller is
    /// the one that should spawn the relay. Returns `false` if a relay is
    /// already active for this session.
    pub fn try_begin_relay(&self, session_id: &str) -> bool {
        self.active_relays.insert(session_id.to_string())
    }

    /// Clears the running-relay marker for `session_id`.
    pub fn end_relay(&self, session_id: &str) {
        self.active_relays.remove(session_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clarification(question: &str, tool_call_id: &str) -> AgentEvent {
        AgentEvent::NeedClarification {
            question: question.to_string(),
            options: None,
            tool_call_id: Some(tool_call_id.to_string()),
            tool_name: None,
            allow_custom: true,
        }
    }

    fn service() -> (NotificationService, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prefs.json");
        (NotificationService::new(path), dir)
    }

    #[test]
    fn notify_dedups_within_window_and_refires_other_keys() {
        let (svc, _dir) = service();
        let first = svc.notify("sess", &clarification("question", "tc-1"));
        assert!(first.is_some());

        // Same dedup_key again within the window → suppressed.
        let second = svc.notify("sess", &clarification("question", "tc-1"));
        assert!(second.is_none());

        // A different dedup_key (different tool_call_id) still fires.
        let third = svc.notify("sess", &clarification("question", "tc-2"));
        assert!(third.is_some());
    }

    #[test]
    fn notify_builds_notification_variant_with_expected_fields() {
        let (svc, _dir) = service();
        let event = svc
            .notify("sess-42", &clarification("Need input", "tc-9"))
            .unwrap();
        match event {
            AgentEvent::Notification {
                session_id,
                category,
                priority,
                title,
                dedup_key,
                id,
                created_at,
                ..
            } => {
                assert_eq!(session_id, "sess-42");
                assert_eq!(category, "needs_clarification");
                assert_eq!(priority, "high");
                assert_eq!(title, "Your input needed");
                assert_eq!(dedup_key.as_deref(), Some("clarification:sess-42:tc-9"));
                assert!(!id.is_empty());
                assert!(!created_at.is_empty());
            }
            other => panic!("expected Notification, got {other:?}"),
        }
    }

    #[test]
    fn set_preferences_persists_and_reloads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prefs.json");
        let svc = NotificationService::new(path.clone());

        let updated = NotificationPreferences {
            enabled: true,
            on_clarification: false,
            on_tool_approval: false,
            on_context_pressure: true,
            on_subagent_complete: false,
        };
        svc.set_preferences(updated.clone()).unwrap();
        assert_eq!(svc.preferences(), updated);

        // A fresh service over the same path reads the persisted value.
        let reloaded = NotificationService::new(path);
        assert_eq!(reloaded.preferences(), updated);
    }

    #[test]
    fn try_begin_relay_is_idempotent_per_session() {
        let (svc, _dir) = service();
        assert!(svc.try_begin_relay("sess"));
        assert!(!svc.try_begin_relay("sess"));

        svc.end_relay("sess");
        // After ending, the session can begin a relay again.
        assert!(svc.try_begin_relay("sess"));
    }

    #[test]
    fn notify_returns_none_when_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prefs.json");
        let svc = NotificationService::new(path);
        svc.set_preferences(NotificationPreferences {
            enabled: false,
            ..NotificationPreferences::default()
        })
        .unwrap();
        assert!(svc
            .notify("sess", &clarification("question", "tc-1"))
            .is_none());
    }
}
