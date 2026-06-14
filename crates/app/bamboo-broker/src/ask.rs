//! Orchestrator-side ask: deliver an `Ask` to a target agent over the broker and
//! await its correlated `Reply`. This is the primitive the `SubAgent` "ask"
//! action (and tests) build on — `me` asks `target` a question and judges the
//! answer, regardless of where `target` physically runs.

use std::time::Duration;

use bamboo_subagent::{AgentRef, AskBody, AskMode, InboxKind, InboxMessage, MsgId, ReplyBody};
use chrono::Utc;

use crate::client::BrokerClient;
use crate::error::{BrokerError, BrokerResult};

/// Connect as `me`, ask `target` a `question` in `mode`, and return the answer.
/// Convenience wrapper over [`ask_over`] for a one-shot ask.
pub async fn ask_agent(
    endpoint: &str,
    me: AgentRef,
    token: &str,
    target: &str,
    question: &str,
    mode: AskMode,
    timeout: Duration,
) -> BrokerResult<String> {
    let mut client = BrokerClient::connect(endpoint, me.clone(), token).await?;
    client.subscribe().await?;
    ask_over(&mut client, &me, target, question, mode, timeout).await
}

/// Ask over an already-connected, already-subscribed client — so an orchestrator
/// can reuse one connection for many asks. Delivers the `Ask` and waits for the
/// `Reply` whose `correlation_id` matches (skipping any unrelated messages), up
/// to `timeout`.
pub async fn ask_over(
    client: &mut BrokerClient,
    me: &AgentRef,
    target: &str,
    question: &str,
    mode: AskMode,
    timeout: Duration,
) -> BrokerResult<String> {
    let msg = InboxMessage {
        id: MsgId::new(),
        from: me.clone(),
        kind: InboxKind::Ask,
        body: serde_json::to_value(AskBody {
            question: question.to_string(),
            mode,
        })
        .expect("AskBody serializes"),
        created_at: Utc::now(),
        correlation_id: None,
    };
    let qid = msg.id.clone();
    client.deliver(target, msg).await?;

    loop {
        match tokio::time::timeout(timeout, client.next_message()).await {
            Ok(Some(reply)) if reply.correlation_id.as_ref() == Some(&qid) => {
                let body: ReplyBody = serde_json::from_value(reply.body)
                    .map_err(|e| BrokerError::Protocol(format!("bad reply body: {e}")))?;
                return Ok(body.answer);
            }
            Ok(Some(_)) => continue, // unrelated message; keep waiting for ours
            Ok(None) => {
                return Err(BrokerError::Transport(
                    "connection closed before reply".into(),
                ))
            }
            Err(_) => {
                return Err(BrokerError::Transport(format!(
                    "ask to '{target}' timed out after {timeout:?}"
                )))
            }
        }
    }
}

/// Generic correlated request/reply over an existing connected + subscribed
/// client: deliver a message of `kind` carrying `body` to `target`, then wait
/// for the reply whose `correlation_id` matches and return its body. Up to
/// `timeout`. (The MCP proxy and `ask` both build on this.)
pub async fn request_over(
    client: &mut BrokerClient,
    me: &AgentRef,
    target: &str,
    kind: InboxKind,
    body: serde_json::Value,
    timeout: Duration,
) -> BrokerResult<serde_json::Value> {
    let msg = InboxMessage {
        id: MsgId::new(),
        from: me.clone(),
        kind,
        body,
        created_at: Utc::now(),
        correlation_id: None,
    };
    let qid = msg.id.clone();
    client.deliver(target, msg).await?;

    loop {
        match tokio::time::timeout(timeout, client.next_message()).await {
            Ok(Some(reply)) if reply.correlation_id.as_ref() == Some(&qid) => return Ok(reply.body),
            Ok(Some(_)) => continue,
            Ok(None) => {
                return Err(BrokerError::Transport(
                    "connection closed before reply".into(),
                ))
            }
            Err(_) => {
                return Err(BrokerError::Transport(format!(
                    "request to '{target}' timed out after {timeout:?}"
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::BrokerCore;
    use crate::serve::serve_executor;
    use crate::server::BrokerServer;
    use std::sync::Arc;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn ask_agent_round_trip_against_echo_executor() {
        let dir = tempfile::tempdir().unwrap();
        let core = Arc::new(BrokerCore::new(dir.path()));
        let server = Arc::new(BrokerServer::new(core, "t"));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        let endpoint = format!("ws://{addr}");

        // Echo-executor agent on the bus.
        let ep = endpoint.clone();
        tokio::spawn(async move {
            let _ = serve_executor(
                &ep,
                AgentRef {
                    session_id: "w".into(),
                    role: None,
                },
                "t",
                Arc::new(bamboo_subagent::EchoExecutor),
            )
            .await;
        });

        let answer = ask_agent(
            &endpoint,
            AgentRef {
                session_id: "orch".into(),
                role: None,
            },
            "t",
            "w",
            "ping pong",
            AskMode::Query,
            Duration::from_secs(5),
        )
        .await
        .expect("ask returns an answer");
        assert_eq!(answer, "echo: ping pong");
    }
}
