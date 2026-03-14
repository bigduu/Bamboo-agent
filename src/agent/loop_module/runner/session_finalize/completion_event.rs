use tokio::sync::mpsc;

use crate::agent::core::agent::events::TokenUsage;
use crate::agent::core::AgentEvent;

pub(super) async fn send_complete_event_if_needed(
    event_tx: &mpsc::Sender<AgentEvent>,
    sent_complete: bool,
) {
    if sent_complete {
        return;
    }

    let _ = event_tx
        .send(AgentEvent::Complete {
            usage: TokenUsage {
                prompt_tokens: 0,
                completion_tokens: 0,
                total_tokens: 0,
            },
        })
        .await;
}
