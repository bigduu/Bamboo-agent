//! Config-driven command sink for classified notifications.
//!
//! This is invoked only from the shared post-classification sink fan-out, so
//! commands never see policy-rejected or deduplicated notification attempts.

use bamboo_agent_core::Session;
use bamboo_config::LifecycleHooksConfig;
use bamboo_domain::{AgentHookPoint, AgentRuntimeState, HookPayload};
use bamboo_engine::HookRunner;

use super::{NotificationSink, SinkNotification};

pub(super) struct CommandSink {
    hooks: LifecycleHooksConfig,
    fallback_cwd: Option<std::path::PathBuf>,
}

impl CommandSink {
    pub(super) fn from_config(config: &bamboo_llm::Config) -> Self {
        Self {
            hooks: config.lifecycle_hooks.clone(),
            fallback_cwd: config.get_default_work_area_path(),
        }
    }
}

impl NotificationSink for CommandSink {
    fn name(&self) -> &'static str {
        "command"
    }

    fn deliver(&self, notification: &SinkNotification) {
        let hooks = self.hooks.clone();
        let fallback_cwd = self.fallback_cwd.clone();
        let notification = notification.clone();
        tokio::spawn(async move {
            let runner = HookRunner::new().with_lifecycle_config(&hooks, fallback_cwd);
            if !runner.has_hooks_for(AgentHookPoint::AfterNotification) {
                return;
            }

            let session = Session::new(&notification.session_id, "");
            let mut runtime_state = AgentRuntimeState::new(&notification.session_id);
            let payload = HookPayload::Notification {
                id: notification.id,
                category: notification.category,
                priority: notification.priority,
                title: notification.title,
                body: notification.body,
                dedup_key: notification.dedup_key,
                created_at: notification.created_at,
                click_url: notification.click_url,
            };

            // Notifications are observers: decision-shaped stdout and exit 2
            // are recorded but cannot undo an already-delivered notification or
            // prevent later configured commands from running.
            let _ = runner
                .run_observer_hooks(
                    AgentHookPoint::AfterNotification,
                    &payload,
                    &session,
                    &mut runtime_state,
                    None,
                )
                .await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_config::{
        LifecycleHookGroup, LifecycleHookHandler, DEFAULT_LIFECYCLE_HOOK_TIMEOUT_MS,
    };

    fn sample_notification() -> SinkNotification {
        SinkNotification {
            id: Some("notification-1".to_string()),
            title: "Build finished".to_string(),
            body: "All checks passed".to_string(),
            category: "background_task_completed".to_string(),
            priority: "normal".to_string(),
            session_id: "session-1".to_string(),
            dedup_key: Some("background:session-1:build".to_string()),
            created_at: Some("2026-07-22T09:00:00Z".to_string()),
            click_url: Some("bamboo://session/session-1".to_string()),
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn command_sink_receives_the_classified_notification_payload() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("notification.json");
        let command = format!("cat > '{}'", output.display());
        let hooks = LifecycleHooksConfig {
            enabled: true,
            notification: vec![LifecycleHookGroup {
                enabled: true,
                matcher: None,
                hooks: vec![LifecycleHookHandler::command(
                    command,
                    DEFAULT_LIFECYCLE_HOOK_TIMEOUT_MS,
                )],
            }],
            ..Default::default()
        };

        let mut config = bamboo_llm::Config::default();
        config.lifecycle_hooks = hooks;
        CommandSink::from_config(&config).deliver(&sample_notification());

        let envelope = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if let Ok(contents) = std::fs::read_to_string(&output) {
                    // The shell creates the redirection target before `cat`
                    // writes the hook payload. Do not treat that transient
                    // empty/partial file as a completed delivery.
                    if let Ok(envelope) = serde_json::from_str::<serde_json::Value>(&contents) {
                        break envelope;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("notification hook should complete within its test deadline");

        assert_eq!(envelope["hook_event_name"], "Notification");
        assert_eq!(envelope["session_id"], "session-1");
        assert_eq!(envelope["payload"]["type"], "notification");
        assert_eq!(envelope["payload"]["id"], "notification-1");
        assert_eq!(envelope["payload"]["category"], "background_task_completed");
        assert_eq!(envelope["payload"]["priority"], "normal");
        assert_eq!(envelope["payload"]["title"], "Build finished");
        assert_eq!(envelope["payload"]["body"], "All checks passed");
        assert_eq!(
            envelope["payload"]["dedup_key"],
            "background:session-1:build"
        );
        assert_eq!(envelope["payload"]["created_at"], "2026-07-22T09:00:00Z");
        assert_eq!(
            envelope["payload"]["click_url"],
            "bamboo://session/session-1"
        );
    }
}
