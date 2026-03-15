use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use tokio::sync::{broadcast, RwLock};
use tokio_util::sync::CancellationToken;

use crate::agent::core::AgentEvent;
use crate::server::app_state::{AgentRunner, AgentStatus};

type ClaudeRunners = Arc<RwLock<HashMap<String, AgentRunner>>>;

pub(super) async fn insert_running_runner(
    runners: ClaudeRunners,
    session_id: &str,
) -> (broadcast::Sender<AgentEvent>, CancellationToken) {
    let mut runner = AgentRunner::new();
    runner.status = AgentStatus::Running;

    let event_sender = runner.event_sender.clone();
    let cancel_token = runner.cancel_token.clone();

    {
        let mut guard = runners.write().await;
        guard.insert(session_id.to_string(), runner);
    }

    (event_sender, cancel_token)
}

pub(super) fn spawn_runner_status_watcher(
    runners: ClaudeRunners,
    session_id: String,
    event_sender: broadcast::Sender<AgentEvent>,
) {
    let mut rx = event_sender.subscribe();
    tokio::spawn(async move {
        while let Ok(event) = rx.recv().await {
            let terminal = match &event {
                AgentEvent::Complete { .. } => Some(AgentStatus::Completed),
                AgentEvent::Error { message } => Some(AgentStatus::Error(message.clone())),
                _ => None,
            };

            if let Some(status) = terminal {
                let mut guard = runners.write().await;
                if let Some(runner) = guard.get_mut(&session_id) {
                    runner.status = status;
                    runner.completed_at = Some(Utc::now());
                }
                break;
            }
        }
    });
}
