//! Session event sender management + the always-on notification relay.
//!
//! Re-exports the shared `get_or_create_event_sender` function from the
//! runtime crate. The `impl AppState` methods delegate to it and to the
//! free [`ensure_notification_relay`] below.

use std::collections::HashMap;
use std::sync::Arc;

use bamboo_agent_core::AgentEvent;
use tokio::sync::{broadcast, RwLock};

use super::watchers::SessionWatchers;

pub use bamboo_engine::execution::session_events::get_or_create_event_sender;

/// Dependencies the always-on notification relay needs, independent of a
/// full `AppState`.
///
/// Two call sites start this relay: [`super::AppState::ensure_notification_relay`]
/// (interactive/resume execution, and SSE/WS client subscribe) and the
/// schedule manager (`schedule_app::manager`, headless/scheduled runs — which
/// hold their own trimmed `ScheduleContext` rather than a full `AppState`,
/// since scheduled runs use a minimal tool/session surface). Bundling the
/// four `Arc`/cheap-clone fields the relay task actually touches lets both
/// callers share one implementation instead of two copies drifting apart.
#[derive(Clone)]
pub struct NotificationRelayDeps {
    pub notification_service: Arc<bamboo_notification::NotificationService>,
    pub session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
    pub session_watchers: Arc<SessionWatchers>,
    pub config: Arc<tokio::sync::RwLock<bamboo_llm::Config>>,
}

/// Server-owned observer setup injected into the engine's canonical child
/// scheduler. Both queued tool launches and reserved SessionInbox activations
/// therefore start the same always-on relay before child execution.
pub struct NotificationRelayLaunchHook {
    deps: NotificationRelayDeps,
}

impl NotificationRelayLaunchHook {
    pub fn new(deps: NotificationRelayDeps) -> Self {
        Self { deps }
    }
}

impl bamboo_engine::execution::ChildRunLaunchHook for NotificationRelayLaunchHook {
    fn before_child_launch(
        &self,
        job: &bamboo_engine::execution::SpawnJob,
        child_events: broadcast::Sender<AgentEvent>,
    ) {
        ensure_notification_relay(&self.deps, &job.child_session_id, child_events);
    }
}

/// Ensure a notification relay task is running for `session_id`.
///
/// The relay subscribes to the session's event broadcast, runs the backend
/// notification policy on each event, and:
/// - re-broadcasts any resulting `AgentEvent::Notification` onto the same
///   channel so connected SSE/WS clients receive it, and
/// - fans it out to the configured delivery sinks (command/desktop/ntfy/bark — see
///   [`crate::notify_sinks::dispatch_to_sinks`]), reading the CURRENT config
///   on every notification so a hot-reloaded topic/token/toggle takes effect
///   immediately.
///
/// Idempotent: at most one relay per session (tracked by the notification
/// service via `try_begin_relay`). The relay does not hold a `Sender` clone,
/// so the channel still closes naturally (`RecvError::Closed`) when the
/// session is torn down, and this task exits cleanly with it — nothing leaks.
pub fn ensure_notification_relay(
    deps: &NotificationRelayDeps,
    session_id: &str,
    sender: broadcast::Sender<AgentEvent>,
) {
    if !deps.notification_service.try_begin_relay(session_id) {
        return;
    }
    let service = deps.notification_service.clone();
    let senders = deps.session_event_senders.clone();
    let watchers = deps.session_watchers.clone();
    let config = deps.config.clone();
    let sid = session_id.to_string();
    let mut rx = sender.subscribe();
    drop(sender);
    tokio::spawn(async move {
        use tokio::sync::broadcast::error::RecvError;
        loop {
            match rx.recv().await {
                Ok(event) => {
                    if let Some(notification) = service.notify(&sid, &event) {
                        // Build the sink payload before `notification` is moved
                        // into `tx.send` below.
                        let sink_notification =
                            crate::notify_sinks::SinkNotification::from_event(&notification);

                        let tx = senders.read().await.get(&sid).cloned();
                        if let Some(tx) = tx {
                            let _ = tx.send(notification);
                        }

                        if let Some(sink_notification) = sink_notification {
                            let has_watcher = watchers.has_watcher(&sid);
                            let config_snapshot = config.read().await.clone();
                            super::AppState::dispatch_to_sinks(
                                &config_snapshot,
                                has_watcher,
                                &sink_notification,
                            );
                        }
                    }
                }
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => break,
            }
        }
        service.end_relay(&sid);
    });
}

impl super::AppState {
    /// Get (or create) a long-lived session event sender for a session id.
    ///
    /// This stream is intended for UI consumption and background activity; it should remain
    /// available even when no agent execution is running.
    pub async fn get_session_event_sender(
        &self,
        session_id: &str,
    ) -> tokio::sync::broadcast::Sender<bamboo_agent_core::AgentEvent> {
        get_or_create_event_sender(&self.session_event_senders, session_id).await
    }

    /// Bundles this `AppState`'s notification-relay dependencies (see
    /// [`NotificationRelayDeps`]).
    pub fn notification_relay_deps(&self) -> NotificationRelayDeps {
        NotificationRelayDeps {
            notification_service: self.notification_service.clone(),
            session_event_senders: self.session_event_senders.clone(),
            session_watchers: self.session_watchers.clone(),
            config: self.config.clone(),
        }
    }

    /// Ensure a notification relay task is running for `session_id`. See
    /// [`ensure_notification_relay`].
    pub fn ensure_notification_relay(
        &self,
        session_id: &str,
        sender: tokio::sync::broadcast::Sender<bamboo_agent_core::AgentEvent>,
    ) {
        ensure_notification_relay(&self.notification_relay_deps(), session_id, sender);
    }

    /// Fans a classified notification out to configured delivery sinks
    /// (command/desktop/ntfy/bark). Free of `&self` — the relay task started by
    /// [`ensure_notification_relay`] only holds cloned `Arc`s (not a full
    /// `AppState`), so this is a plain associated function over the values
    /// it needs; see [`crate::notify_sinks::dispatch_to_sinks`].
    pub fn dispatch_to_sinks(
        config: &bamboo_llm::Config,
        has_watcher: bool,
        notification: &crate::notify_sinks::SinkNotification,
    ) {
        crate::notify_sinks::dispatch_to_sinks(config, has_watcher, notification);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn test_deps() -> (NotificationRelayDeps, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let notification_service = Arc::new(bamboo_notification::NotificationService::new(
            dir.path().join("prefs.json"),
        ));
        let deps = NotificationRelayDeps {
            notification_service,
            session_event_senders: Arc::new(RwLock::new(HashMap::new())),
            session_watchers: SessionWatchers::new(),
            config: Arc::new(tokio::sync::RwLock::new(bamboo_llm::Config::default())),
        };
        (deps, dir)
    }

    #[tokio::test]
    async fn ensure_notification_relay_is_idempotent_and_classifies_events() {
        let (deps, _dir) = test_deps();
        let (tx, mut rx) = broadcast::channel(16);
        deps.session_event_senders
            .write()
            .await
            .insert("sess-1".to_string(), tx.clone());

        ensure_notification_relay(&deps, "sess-1", tx.clone());
        // A second call for the same session must be a no-op: `try_begin_relay`
        // only returns `true` once per session while a relay is active.
        assert!(!deps.notification_service.try_begin_relay("sess-1"));

        tx.send(AgentEvent::NeedClarification {
            question: "Which file?".to_string(),
            options: None,
            tool_call_id: Some("tc-1".to_string()),
            tool_name: None,
            allow_custom: true,
        })
        .unwrap();

        // The relay re-broadcasts the classified `Notification` onto the same
        // channel, after the raw event this subscriber also echoes back to
        // itself — loop past that echo (bounded by the outer timeout).
        let category = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let AgentEvent::Notification { category, .. } = rx.recv().await.unwrap() {
                    return category;
                }
            }
        })
        .await
        .expect("relay should classify and re-broadcast within the timeout");
        assert_eq!(category, "needs_clarification");
    }

    #[tokio::test]
    async fn ensure_notification_relay_exits_when_broadcast_channel_closes() {
        let (deps, _dir) = test_deps();
        let (tx, rx) = broadcast::channel::<AgentEvent>(16);

        ensure_notification_relay(&deps, "sess-closed", tx.clone());
        // Drop every `Sender` clone (the relay's own clone is dropped
        // internally right after subscribing) so the channel actually closes.
        drop(tx);
        drop(rx);

        // On `RecvError::Closed` the relay breaks its loop and calls
        // `end_relay`, which frees the session for `try_begin_relay` again.
        // Poll (bounded) instead of asserting immediately — the task's exit
        // is asynchronous from this test's perspective.
        let relay_ended = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if deps.notification_service.try_begin_relay("sess-closed") {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(
            relay_ended.is_ok(),
            "relay task did not exit after its broadcast channel closed"
        );
    }
}
