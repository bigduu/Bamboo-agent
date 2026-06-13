//! Mailbox serving loop: the worker side of the bus.
//!
//! An agent (wherever it runs — local subprocess, Docker, SSH/remote) connects
//! to the broker as itself, subscribes to its own mailbox, and for each inbound
//! message runs a `handler` and — if the handler produces an answer — delivers a
//! correlated [`InboxKind::Reply`] back to the sender. This is the generic
//! plumbing; the real agent execution (query vs steer) lives in the handler the
//! caller supplies.

use std::future::Future;
use std::sync::Arc;

use bamboo_subagent::{AgentRef, InboxKind, InboxMessage, MsgId, ReplyBody};
use chrono::Utc;

use crate::client::BrokerClient;
use crate::error::BrokerResult;

/// What a handler decides to do with one inbound message.
pub enum Handled {
    /// Produced an answer; the loop delivers it as a `Reply` to the sender and
    /// acks the original.
    Reply(String),
    /// Processed with no reply (e.g. a fire-and-forget task); just ack.
    Ack,
    /// Leave the message unacked (it will be redelivered on the next subscribe).
    Leave,
}

/// Connect as `me`, subscribe, and serve inbound messages with `handler` until
/// the connection closes. The handler receives each [`InboxMessage`] and returns
/// a [`Handled`]; the loop owns reply addressing (to `msg.from`, correlated to
/// `msg.id`) and ack bookkeeping, so handlers stay pure "answer this" logic.
pub async fn serve_mailbox<H, Fut>(
    endpoint: &str,
    me: AgentRef,
    token: &str,
    handler: H,
) -> BrokerResult<()>
where
    H: Fn(InboxMessage) -> Fut + Send + Sync,
    Fut: Future<Output = Handled> + Send,
{
    let mut client = BrokerClient::connect(endpoint, me.clone(), token).await?;
    client.subscribe().await?;
    serve_loop(&mut client, &me, handler).await
}

/// The serve loop against an already-connected, already-subscribed client.
/// Separated so tests can drive it over an in-process client.
pub async fn serve_loop<H, Fut>(
    client: &mut BrokerClient,
    me: &AgentRef,
    handler: H,
) -> BrokerResult<()>
where
    H: Fn(InboxMessage) -> Fut + Send + Sync,
    Fut: Future<Output = Handled> + Send,
{
    while let Some(msg) = client.next_message().await {
        let id = msg.id.clone();
        let reply_to = msg.from.session_id.clone();
        match handler(msg).await {
            Handled::Reply(answer) => {
                let reply = InboxMessage {
                    id: MsgId::new(),
                    from: me.clone(),
                    kind: InboxKind::Reply,
                    body: serde_json::to_value(ReplyBody { answer }).expect("ReplyBody serializes"),
                    created_at: Utc::now(),
                    correlation_id: Some(id.clone()),
                };
                client.deliver(&reply_to, reply).await?;
                client.ack(id).await?;
            }
            Handled::Ack => client.ack(id).await?,
            Handled::Leave => {}
        }
    }
    Ok(())
}

/// Convenience wrapper for `serve_mailbox` whose `Arc`-shared handler answers
/// every message with a string (the common ask/reply agent case).
pub async fn serve_with<F, Fut>(
    endpoint: &str,
    me: AgentRef,
    token: &str,
    answer: Arc<F>,
) -> BrokerResult<()>
where
    F: Fn(InboxMessage) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = String> + Send,
{
    serve_mailbox(endpoint, me, token, move |msg| {
        let answer = Arc::clone(&answer);
        async move { Handled::Reply(answer(msg).await) }
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::BrokerCore;
    use crate::server::BrokerServer;
    use bamboo_subagent::{AskBody, AskMode};
    use std::time::Duration;
    use tokio::net::TcpListener;

    const TOKEN: &str = "t";

    async fn start() -> (String, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let core = Arc::new(BrokerCore::new(dir.path()));
        let server = Arc::new(BrokerServer::new(core, TOKEN));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        (format!("ws://{addr}"), dir)
    }

    fn ask(from: &str, q: &str) -> InboxMessage {
        InboxMessage {
            id: MsgId::new(),
            from: AgentRef {
                session_id: from.into(),
                role: None,
            },
            kind: InboxKind::Ask,
            body: serde_json::to_value(AskBody {
                question: q.into(),
                mode: AskMode::Query,
            })
            .unwrap(),
            created_at: Utc::now(),
            correlation_id: None,
        }
    }

    #[tokio::test]
    async fn serve_mailbox_answers_and_correlates() {
        let (endpoint, _dir) = start().await;

        // A worker that echoes the question back as the answer.
        let worker_ep = endpoint.clone();
        tokio::spawn(async move {
            let _ = serve_with(
                &worker_ep,
                AgentRef {
                    session_id: "worker".into(),
                    role: Some("echo".into()),
                },
                TOKEN,
                Arc::new(|msg: InboxMessage| async move {
                    let body: AskBody = serde_json::from_value(msg.body).unwrap();
                    format!("echo: {}", body.question)
                }),
            )
            .await;
        });

        // Orchestrator asks the worker and awaits the correlated reply.
        let mut orch = BrokerClient::connect(
            &endpoint,
            AgentRef {
                session_id: "orch".into(),
                role: None,
            },
            TOKEN,
        )
        .await
        .unwrap();
        orch.subscribe().await.unwrap();

        let q = ask("orch", "are you up?");
        let qid = q.id.clone();
        orch.deliver("worker", q).await.unwrap();

        let reply = tokio::time::timeout(Duration::from_secs(5), orch.next_message())
            .await
            .expect("reply within timeout")
            .expect("reply present");
        assert_eq!(reply.kind, InboxKind::Reply);
        assert_eq!(reply.correlation_id, Some(qid));
        let body: ReplyBody = serde_json::from_value(reply.body).unwrap();
        assert_eq!(body.answer, "echo: are you up?");
    }
}
