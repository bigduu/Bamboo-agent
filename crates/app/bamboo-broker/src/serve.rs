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

/// Serve an agent backed by any [`ChildExecutor`] over the broker. This is the
/// production worker: for each inbound `Ask`, it runs the executor with the
/// question over the agent's accumulated context and replies with the result.
///
/// The two ask modes (per `docs/ask-agent-design.md`):
/// - [`AskMode::Query`] — summarize/extract: runs over a *copy* of the current
///   context; the exchange is NOT persisted, so the agent's ongoing state is
///   untouched.
/// - [`AskMode::Steer`] — insert into the conversation / redirect the goal: the
///   question + answer are appended to the agent's context, changing what it
///   carries forward.
///
/// A `Task` is treated as a steer (it advances the agent's work). Works with the
/// real `BambooRuntime` executor in production and with `EchoExecutor` (no LLM)
/// for deterministic tests.
pub async fn serve_executor<E>(
    endpoint: &str,
    me: AgentRef,
    token: &str,
    executor: Arc<E>,
) -> BrokerResult<()>
where
    E: bamboo_subagent::ChildExecutor,
{
    let context: Arc<tokio::sync::Mutex<Vec<serde_json::Value>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::new()));
    serve_mailbox(endpoint, me, token, move |msg| {
        let executor = Arc::clone(&executor);
        let context = Arc::clone(&context);
        async move { handle_with_executor(executor.as_ref(), &context, msg).await }
    })
    .await
}

/// Answer one inbound message by running `executor`, applying query/steer
/// context semantics. Pulled out so the policy is unit-testable.
async fn handle_with_executor<E>(
    executor: &E,
    context: &tokio::sync::Mutex<Vec<serde_json::Value>>,
    msg: InboxMessage,
) -> Handled
where
    E: bamboo_subagent::ChildExecutor,
{
    use bamboo_subagent::{AskBody, AskMode, EventSink, RunSpec, SteerInbox};

    // Resolve (question, persist?) from the message kind.
    let (question, persist) = match msg.kind {
        InboxKind::Ask => match serde_json::from_value::<AskBody>(msg.body) {
            Ok(b) => (b.question, matches!(b.mode, AskMode::Steer)),
            Err(_) => return Handled::Ack, // malformed Ask: drop without reply
        },
        InboxKind::Task => (
            msg.body
                .get("assignment")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            true,
        ),
        // Replies / handoffs are not answered by this loop.
        _ => return Handled::Ack,
    };

    let prior = context.lock().await.clone();
    let (sink, _discard) = EventSink::channel();
    let outcome = executor
        .run(
            RunSpec {
                assignment: question.clone(),
                reasoning_effort: None,
                messages: prior,
            },
            sink,
            SteerInbox::disconnected(),
            tokio_util::sync::CancellationToken::new(),
        )
        .await;
    let answer = outcome
        .result
        .or(outcome.error)
        .unwrap_or_else(|| "(no result)".to_string());

    if persist {
        let mut ctx = context.lock().await;
        ctx.push(serde_json::json!({ "role": "user", "content": question }));
        ctx.push(serde_json::json!({ "role": "assistant", "content": answer }));
    }
    Handled::Reply(answer)
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

    /// Executor that reports how many prior context messages it received — lets
    /// us prove query (read-only) vs steer (persist) deterministically, no LLM.
    struct ContextReporter;
    #[async_trait::async_trait]
    impl bamboo_subagent::ChildExecutor for ContextReporter {
        async fn run(
            &self,
            spec: bamboo_subagent::RunSpec,
            _events: bamboo_subagent::EventSink,
            _steer: bamboo_subagent::SteerInbox,
            _cancel: tokio_util::sync::CancellationToken,
        ) -> bamboo_subagent::ChildOutcome {
            bamboo_subagent::ChildOutcome::completed(format!("ctx={}", spec.messages.len()))
        }
    }

    async fn ask_mode(orch: &mut BrokerClient, to: &str, q: &str, mode: AskMode) -> String {
        let msg = InboxMessage {
            id: MsgId::new(),
            from: AgentRef {
                session_id: "orch2".into(),
                role: None,
            },
            kind: InboxKind::Ask,
            body: serde_json::to_value(AskBody {
                question: q.into(),
                mode,
            })
            .unwrap(),
            created_at: Utc::now(),
            correlation_id: None,
        };
        let qid = msg.id.clone();
        orch.deliver(to, msg).await.unwrap();
        loop {
            let r = tokio::time::timeout(Duration::from_secs(5), orch.next_message())
                .await
                .expect("reply within timeout")
                .expect("reply present");
            if r.correlation_id == Some(qid.clone()) {
                return serde_json::from_value::<ReplyBody>(r.body).unwrap().answer;
            }
        }
    }

    #[tokio::test]
    async fn query_is_read_only_steer_persists_context() {
        let (endpoint, _dir) = start().await;

        // A real serve_executor agent backed by the deterministic ContextReporter.
        let worker_ep = endpoint.clone();
        tokio::spawn(async move {
            let _ = serve_executor(
                &worker_ep,
                AgentRef {
                    session_id: "agent".into(),
                    role: None,
                },
                TOKEN,
                Arc::new(ContextReporter),
            )
            .await;
        });

        let mut orch = BrokerClient::connect(
            &endpoint,
            AgentRef {
                session_id: "orch2".into(),
                role: None,
            },
            TOKEN,
        )
        .await
        .unwrap();
        orch.subscribe().await.unwrap();

        // query never persists: context stays empty across queries.
        assert_eq!(
            ask_mode(&mut orch, "agent", "q1", AskMode::Query).await,
            "ctx=0"
        );
        assert_eq!(
            ask_mode(&mut orch, "agent", "q2", AskMode::Query).await,
            "ctx=0"
        );
        // steer runs over the (still empty) context, then persists user+assistant.
        assert_eq!(
            ask_mode(&mut orch, "agent", "s1", AskMode::Steer).await,
            "ctx=0"
        );
        // a later query now sees the 2 persisted messages.
        assert_eq!(
            ask_mode(&mut orch, "agent", "q3", AskMode::Query).await,
            "ctx=2"
        );
        // a second steer sees 2 then persists 2 more; the next query sees 4.
        assert_eq!(
            ask_mode(&mut orch, "agent", "s2", AskMode::Steer).await,
            "ctx=2"
        );
        assert_eq!(
            ask_mode(&mut orch, "agent", "q4", AskMode::Query).await,
            "ctx=4"
        );
    }
}
