//! Mailbox serving loop: the worker side of the bus.
//!
//! An agent (wherever it runs — local subprocess, Docker, SSH/remote) connects
//! to the broker as itself, subscribes to its own mailbox, and for each inbound
//! message runs a `handler` and — if the handler produces an answer — delivers a
//! correlated [`InboxKind::Reply`] back to the sender. This is the generic
//! plumbing; the real agent execution (query vs steer) lives in the handler the
//! caller supplies.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use bamboo_subagent::{AgentRef, InboxKind, InboxMessage, MsgId, ReplyBody};
use chrono::Utc;
use tokio_util::sync::CancellationToken;

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
    H: Fn(InboxMessage, CancellationToken) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Handled> + Send + 'static,
{
    let mut client = BrokerClient::connect(endpoint, me.clone(), token).await?;
    client.subscribe().await?;
    serve_loop(&mut client, &me, handler).await
}

/// One finished handler routed back to the single client owner for delivery+ack.
/// Carries everything the owner needs so the spawned task touches no client state.
struct Completion {
    /// Correlation id of the original inbound message (the run's id).
    id: MsgId,
    /// `session_id` of the sender, i.e. where a `Reply` is delivered.
    reply_to: String,
    /// What the handler decided (reply / bare ack / leave unacked).
    handled: Handled,
}

/// The serve loop against an already-connected, already-subscribed client.
/// Separated so tests can drive it over an in-process client.
///
/// Each inbound message's handler runs in its OWN spawned task, so N concurrent
/// Asks to one worker overlap their (expensive, agent-execution) work instead of
/// serializing behind a single `handler(msg).await`. The single client owner —
/// this loop — still does ALL the connection I/O: it routes out-of-band cancels
/// to the matching in-flight run's token, and delivers+acks each finished
/// handler's reply as it arrives over the completion channel. So the wire side
/// stays single-owner (no concurrent `deliver`/`ack`) while the work side is
/// parallel. The original #50 cancel + persist + ack semantics are preserved per
/// run: each run still gets its own token (now tracked in a live map so a cancel
/// can find it after we've moved on to the next message), and ack still happens
/// only AFTER the reply is delivered. #45.
pub async fn serve_loop<H, Fut>(
    client: &mut BrokerClient,
    me: &AgentRef,
    handler: H,
) -> BrokerResult<()>
where
    H: Fn(InboxMessage, CancellationToken) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Handled> + Send + 'static,
{
    let handler = Arc::new(handler);

    // Live cancel tokens for runs still in flight, keyed by the run (message) id.
    // A cancel naming an id present here trips its token (the executor honors it
    // mid-LLM-call); a cancel for an unknown id (already finished / never started)
    // is a no-op, exactly as before. An entry is removed when its run completes.
    let mut inflight: HashMap<MsgId, CancellationToken> = HashMap::new();

    // Finished handlers flow back here to the single owner for delivery+ack.
    // KEEP-ALIVE: this original `done_tx` stays in scope for the whole loop (each
    // spawn clones it), so `done_rx.recv()` only returns `None` once the loop is
    // tearing down — never spuriously while runs are in flight. Mirrors the
    // `reply_tx` keep-alive in `serve_mcp_proxy`. #144/#45.
    let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel::<Completion>();

    let mut messages_open = true;
    loop {
        tokio::select! {
            // `biased`: drain finished handlers (arm A) ahead of pulling new
            // work/cancels (arm B) so completed replies are delivered+acked and
            // their in-flight entries cleared promptly, bounding memory under load.
            // (#144's serve_mcp_proxy is unbiased; here arm B internally biases the
            // cancel lane, so cancel latency stays prompt — completions are gated by
            // real agent work, so arm B is always reached between them.)
            biased;
            // A. A finished handler: deliver its reply (if any) then ack — the ack
            //    still strictly follows a delivered reply, as before. Done on the
            //    owner so there is never a concurrent `deliver`/`ack` on the client.
            //    Biased first so completions (which let us exit on teardown) and
            //    their acks don't starve behind a steady inbound stream.
            Some(done) = done_rx.recv() => {
                inflight.remove(&done.id);
                let Completion { id, reply_to, handled } = done;
                match handled {
                    Handled::Reply(answer) => {
                        let reply = InboxMessage {
                            id: MsgId::new(),
                            from: me.clone(),
                            kind: InboxKind::Reply,
                            body: serde_json::to_value(ReplyBody { answer })
                                .unwrap_or_else(|_| serde_json::json!({})),
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
            // B. The next inbound message OR out-of-band cancel (demuxed over one
            //    `&mut client` borrow). A cancel trips the matching in-flight run's
            //    token (#50); a new message registers a fresh token and spawns the
            //    handler on its own task — so concurrent Asks overlap their work and
            //    only the (cheap) wire I/O stays serialized through this owner. #45.
            event = client.next_message_or_cancel(), if messages_open => match event {
                crate::client::ServeEvent::Cancel(Some(cid)) => {
                    if let Some(tok) = inflight.get(&cid) {
                        tok.cancel();
                    }
                }
                // Cancel lane closed (reader gone). The message lane is fed by the
                // same reader, so treat it as connection teardown: stop pulling and
                // drain the in-flight handlers through arm A.
                crate::client::ServeEvent::Cancel(None) => messages_open = false,
                crate::client::ServeEvent::Message(Some(msg)) => {
                    let id = msg.id.clone();
                    let reply_to = msg.from.session_id.clone();
                    let token = CancellationToken::new();
                    inflight.insert(id.clone(), token.clone());

                    let handler = Arc::clone(&handler);
                    let done_tx = done_tx.clone();
                    tokio::spawn(async move {
                        let handled = handler(msg, token).await;
                        // Receiver gone == owner loop exited (conn dropped) -> drop.
                        let _ = done_tx.send(Completion { id, reply_to, handled });
                    });
                }
                // Connection closed: stop pulling new messages and let the remaining
                // in-flight handlers drain through arm A before we exit.
                crate::client::ServeEvent::Message(None) => messages_open = false,
            },
        }

        // Once the message stream is closed, exit as soon as every in-flight run
        // has drained — replies for them have been delivered+acked above.
        if !messages_open && inflight.is_empty() {
            break;
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
    serve_mailbox(endpoint, me, token, move |msg, _cancel| {
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
///
/// CONCURRENCY (#45): inbound Asks are now handled concurrently (see [`serve_loop`]).
/// All Asks to this worker share one `context` vector. For `Query` (the common,
/// non-persisting mode) that is fully safe — pure overlap, no mutation. For two
/// *concurrent* `Steer`s the per-push critical section is atomic (no corruption),
/// but their persisted ordering is non-deterministic and a steer started mid-run
/// sees the pre-push context — i.e. concurrent steers to ONE worker interleave by
/// design. If strict steer ordering is ever required, hold the context lock across
/// read→run→push for steers (leaving queries concurrent).
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
    serve_mailbox(endpoint, me, token, move |msg, cancel| {
        let executor = Arc::clone(&executor);
        let context = Arc::clone(&context);
        async move { handle_with_executor(executor.as_ref(), &context, msg, cancel).await }
    })
    .await
}

/// Answer one inbound message by running `executor`, applying query/steer
/// context semantics. Pulled out so the policy is unit-testable.
async fn handle_with_executor<E>(
    executor: &E,
    context: &tokio::sync::Mutex<Vec<serde_json::Value>>,
    msg: InboxMessage,
    cancel: CancellationToken,
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
            cancel,
        )
        .await;
    let result = outcome.result;
    let answer = result
        .clone()
        .or(outcome.error)
        .unwrap_or_else(|| "(no result)".to_string());

    // Persist into the ongoing context ONLY for a steer/task that actually
    // produced a result. A cancelled or errored run (`result == None`) must NOT
    // push a synthetic "(no result)" assistant turn, which would pollute every
    // later query/steer with a bogus exchange. #50.
    if persist {
        if let Some(result) = result {
            let mut ctx = context.lock().await;
            ctx.push(serde_json::json!({ "role": "user", "content": question }));
            ctx.push(serde_json::json!({ "role": "assistant", "content": result }));
        }
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

    #[tokio::test]
    async fn cancel_aborts_in_flight_run_and_loop_keeps_serving() {
        use bamboo_subagent::{ChildExecutor, ChildOutcome, EventSink, RunSpec, SteerInbox};

        // Parks on its cancel token for a "park" ask; echoes anything else.
        struct ParkOrEcho;
        #[async_trait::async_trait]
        impl ChildExecutor for ParkOrEcho {
            async fn run(
                &self,
                spec: RunSpec,
                _events: EventSink,
                _steer: SteerInbox,
                cancel: CancellationToken,
            ) -> ChildOutcome {
                if spec.assignment.contains("park") {
                    cancel.cancelled().await;
                    ChildOutcome::cancelled()
                } else {
                    ChildOutcome::completed(format!("echo: {}", spec.assignment))
                }
            }
        }

        let (endpoint, _dir) = start().await;
        let worker_ep = endpoint.clone();
        tokio::spawn(async move {
            let _ = serve_executor(
                &worker_ep,
                AgentRef {
                    session_id: "worker".into(),
                    role: None,
                },
                TOKEN,
                Arc::new(ParkOrEcho),
            )
            .await;
        });

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

        // Probe round-trip first: confirms the worker is subscribed (an out-of-band
        // cancel is dropped if the target isn't), so the test can't race the
        // worker's Subscribe registration.
        let probe = ask("orch", "ping");
        let probe_id = probe.id.clone();
        orch.deliver("worker", probe).await.unwrap();
        let r0 = tokio::time::timeout(Duration::from_secs(5), orch.next_message())
            .await
            .expect("probe reply")
            .expect("present");
        assert_eq!(r0.correlation_id, Some(probe_id));

        // Ask 1 parks the worker's run; a cancel for it aborts the run mid-flight,
        // and the loop still delivers the (cancelled) reply — i.e. it isn't wedged.
        let q1 = ask("orch", "please park");
        let qid1 = q1.id.clone();
        orch.deliver("worker", q1).await.unwrap();
        orch.cancel("worker", &qid1).await.unwrap();
        let reply1 = tokio::time::timeout(Duration::from_secs(5), orch.next_message())
            .await
            .expect("cancelled run still replies — loop not wedged")
            .expect("present");
        assert_eq!(reply1.correlation_id, Some(qid1));

        // Ask 2: the worker is still serving, and a normal ask completes correctly
        // — proving the cancel didn't break the loop or its context.
        let q2 = ask("orch", "hello");
        let qid2 = q2.id.clone();
        orch.deliver("worker", q2).await.unwrap();
        let reply2 = tokio::time::timeout(Duration::from_secs(5), orch.next_message())
            .await
            .expect("loop keeps serving after a cancel")
            .expect("present");
        assert_eq!(reply2.correlation_id, Some(qid2));
        let body: ReplyBody = serde_json::from_value(reply2.body).unwrap();
        assert_eq!(body.answer, "echo: hello");
    }

    #[tokio::test]
    async fn concurrent_asks_to_one_worker_overlap() {
        use bamboo_subagent::{ChildExecutor, ChildOutcome, EventSink, RunSpec, SteerInbox};
        use std::time::Instant;

        // An executor where each run takes 200ms. Serial handling of N asks to ONE
        // worker would take ~N*200ms; concurrent (per-ask spawn) overlaps to ~200ms.
        struct SlowEcho;
        #[async_trait::async_trait]
        impl ChildExecutor for SlowEcho {
            async fn run(
                &self,
                spec: RunSpec,
                _events: EventSink,
                _steer: SteerInbox,
                _cancel: CancellationToken,
            ) -> ChildOutcome {
                tokio::time::sleep(Duration::from_millis(200)).await;
                ChildOutcome::completed(format!("done: {}", spec.assignment))
            }
        }

        let (endpoint, _dir) = start().await;
        let worker_ep = endpoint.clone();
        tokio::spawn(async move {
            let _ = serve_executor(
                &worker_ep,
                AgentRef {
                    session_id: "worker".into(),
                    role: None,
                },
                TOKEN,
                Arc::new(SlowEcho),
            )
            .await;
        });

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

        // Probe round-trip first so the worker is provably subscribed before we
        // fire the concurrent batch (else early Asks could queue as durable backlog
        // and not actually overlap).
        let probe = ask("orch", "ping");
        let probe_id = probe.id.clone();
        orch.deliver("worker", probe).await.unwrap();
        loop {
            let r = tokio::time::timeout(Duration::from_secs(5), orch.next_message())
                .await
                .expect("probe reply")
                .expect("present");
            if r.correlation_id == Some(probe_id.clone()) {
                break;
            }
        }

        // Fire N concurrent (Query) Asks to the SAME worker, then await all N
        // correlated replies. Serial would be ~N*200ms; concurrent ~200ms.
        const N: usize = 4;
        let mut want: std::collections::HashSet<MsgId> = std::collections::HashSet::new();
        let start = Instant::now();
        for i in 0..N {
            let q = ask("orch", &format!("q{i}"));
            want.insert(q.id.clone());
            orch.deliver("worker", q).await.unwrap();
        }
        while !want.is_empty() {
            let r = tokio::time::timeout(Duration::from_secs(5), orch.next_message())
                .await
                .expect("a reply arrives")
                .expect("present");
            if let Some(cid) = &r.correlation_id {
                want.remove(cid);
            }
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(500),
            "{N} concurrent 200ms Asks to ONE worker must OVERLAP \
             (serial would be ~{}ms); took {elapsed:?}",
            N * 200
        );
    }

    #[tokio::test]
    async fn cancelled_steer_does_not_pollute_context() {
        use bamboo_subagent::{ChildExecutor, ChildOutcome, EventSink, RunSpec, SteerInbox};

        // Parks (-> cancelled) on a "park" assignment; otherwise reports how many
        // prior context messages it was given.
        struct ParkOrReportCtx;
        #[async_trait::async_trait]
        impl ChildExecutor for ParkOrReportCtx {
            async fn run(
                &self,
                spec: RunSpec,
                _events: EventSink,
                _steer: SteerInbox,
                cancel: CancellationToken,
            ) -> ChildOutcome {
                if spec.assignment.contains("park") {
                    cancel.cancelled().await;
                    ChildOutcome::cancelled()
                } else {
                    ChildOutcome::completed(format!("ctx={}", spec.messages.len()))
                }
            }
        }

        let (endpoint, _dir) = start().await;
        let worker_ep = endpoint.clone();
        tokio::spawn(async move {
            let _ = serve_executor(
                &worker_ep,
                AgentRef {
                    session_id: "w".into(),
                    role: None,
                },
                TOKEN,
                Arc::new(ParkOrReportCtx),
            )
            .await;
        });

        // `ask_mode` hardcodes `from = "orch2"`, so connect as that to receive replies.
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

        // Probe (query): context starts empty + confirms subscription.
        assert_eq!(
            ask_mode(&mut orch, "w", "ping", AskMode::Query).await,
            "ctx=0"
        );

        // A STEER (which DOES persist) that gets cancelled — built manually to
        // capture its id for the cancel.
        let steer = InboxMessage {
            id: MsgId::new(),
            from: AgentRef {
                session_id: "orch2".into(),
                role: None,
            },
            kind: InboxKind::Ask,
            body: serde_json::to_value(AskBody {
                question: "park this steer".into(),
                mode: AskMode::Steer,
            })
            .unwrap(),
            created_at: Utc::now(),
            correlation_id: None,
        };
        let sid = steer.id.clone();
        orch.deliver("w", steer).await.unwrap();
        orch.cancel("w", &sid).await.unwrap();
        loop {
            let m = tokio::time::timeout(Duration::from_secs(5), orch.next_message())
                .await
                .expect("cancelled steer replies")
                .expect("present");
            if m.correlation_id == Some(sid.clone()) {
                break;
            }
        }

        // The cancelled steer must NOT have persisted a synthetic turn — the next
        // query still sees an EMPTY context (ctx=0), not ctx=2.
        assert_eq!(
            ask_mode(&mut orch, "w", "again", AskMode::Query).await,
            "ctx=0"
        );
    }
}
