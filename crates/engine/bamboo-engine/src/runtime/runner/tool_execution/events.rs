use tokio::sync::mpsc;

use bamboo_agent_core::AgentEvent;
use bamboo_metrics::MetricsCollector;

pub(super) async fn send_event_with_metrics(
    event_tx: &mpsc::Sender<AgentEvent>,
    metrics_collector: Option<&MetricsCollector>,
    session_id: &str,
    round_id: &str,
    event: AgentEvent,
) {
    if let Some(metrics) = metrics_collector {
        metrics.record_agent_event(session_id, round_id, &event);
    }

    let _ = event_tx.send(event).await;
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;

    use super::send_event_with_metrics;
    use bamboo_agent_core::AgentEvent;

    #[tokio::test]
    async fn send_event_with_metrics_forwards_event_without_collector() {
        let (tx, mut rx) = mpsc::channel(4);
        send_event_with_metrics(
            &tx,
            None,
            "session-1",
            "round-1",
            AgentEvent::Token {
                content: "hello".to_string(),
            },
        )
        .await;

        let event = rx.recv().await.expect("event should be sent");
        match event {
            AgentEvent::Token { content } => assert_eq!(content, "hello"),
            other => panic!("unexpected event: {other:?}"),
        }
    }
}
