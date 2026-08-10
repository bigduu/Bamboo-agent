use bamboo_agent_core::AgentEvent;
use tokio::sync::broadcast;

use crate::app_state::{AgentRunner, AppState};

/// Install a live receiver and capture its replay snapshot at one runner-lock
/// boundary.
///
/// Replayable publishers cache and broadcast while holding the runner write
/// lock. Taking the read lock across both `subscribe` and the cache clone
/// therefore gives the client exactly one side of every publication:
///
/// - a publication completed before this boundary is present in the snapshot;
/// - a publication completed after it is buffered by the receiver.
///
/// Without the shared boundary, an event can be cached, then subscribed, then
/// broadcast, causing snapshot/live duplication and lifecycle reordering.
pub(crate) async fn subscribe_with_runner_snapshot(
    state: &AppState,
    session_id: &str,
) -> (
    broadcast::Sender<AgentEvent>,
    broadcast::Receiver<AgentEvent>,
    Option<AgentRunner>,
) {
    // Event senders live behind a separate lock. Resolve it before taking the
    // runner lock so publishers and subscribers use one consistent lock order.
    let sender = state.get_session_event_sender(session_id).await;
    let runners = state.agent_runners.read().await;
    let receiver = sender.subscribe();
    let runner_snapshot = runners.get(session_id).cloned();
    drop(runners);
    (sender, receiver, runner_snapshot)
}
