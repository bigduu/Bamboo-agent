use tokio::sync::mpsc;

use crate::agent::core::{AgentEvent, Message, Session};
use crate::agent::metrics::{
    MetricsCollector, RoundStatus as MetricsRoundStatus, TokenUsage as MetricsTokenUsage,
};

use super::super::{metrics_lifecycle, to_event_token_usage};
use super::RoundFlowOutcome;

pub(super) async fn handle_no_tool_calls(
    content: String,
    reasoning: Option<String>,
    prompt_tokens: u64,
    completion_tokens: u64,
    round_usage: MetricsTokenUsage,
    session: &mut Session,
    event_tx: &mpsc::Sender<AgentEvent>,
    metrics_collector: Option<&MetricsCollector>,
    round_id: &str,
    session_id: &str,
) -> RoundFlowOutcome {
    session.add_message(Message::assistant_with_reasoning(content, None, reasoning));

    let _ = event_tx
        .send(AgentEvent::Complete {
            usage: to_event_token_usage(prompt_tokens, completion_tokens),
        })
        .await;

    metrics_lifecycle::record_round_completed(
        metrics_collector,
        round_id,
        session_id,
        session.messages.len() as u32,
        MetricsRoundStatus::Success,
        round_usage,
        None,
    );

    RoundFlowOutcome {
        should_break: true,
        sent_complete: true,
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;

    use super::handle_no_tool_calls;
    use crate::agent::core::{AgentEvent, Role, Session};
    use crate::agent::metrics::TokenUsage as MetricsTokenUsage;

    #[tokio::test]
    async fn handle_no_tool_calls_emits_complete_and_appends_assistant_message() {
        let mut session = Session::new("session-1", "model");
        let (tx, mut rx) = mpsc::channel(4);

        let outcome = handle_no_tool_calls(
            "final answer".to_string(),
            Some("reasoning trace".to_string()),
            11,
            7,
            MetricsTokenUsage {
                prompt_tokens: 11,
                completion_tokens: 7,
                total_tokens: 18,
            },
            &mut session,
            &tx,
            None,
            "round-1",
            "session-1",
        )
        .await;

        assert!(outcome.should_break);
        assert!(outcome.sent_complete);
        assert_eq!(session.messages.len(), 1);
        assert!(matches!(session.messages[0].role, Role::Assistant));
        assert_eq!(session.messages[0].content, "final answer");
        assert_eq!(
            session.messages[0].reasoning.as_deref(),
            Some("reasoning trace")
        );

        let event = rx.recv().await.expect("complete event should be sent");
        match event {
            AgentEvent::Complete { usage } => {
                assert_eq!(usage.prompt_tokens, 11);
                assert_eq!(usage.completion_tokens, 7);
                assert_eq!(usage.total_tokens, 18);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }
}
