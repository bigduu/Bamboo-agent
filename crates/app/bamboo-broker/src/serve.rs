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
    // A fresh, never-cancelled token: identical behavior to before graceful
    // shutdown existed — no external caller of this entry point can trip it.
    serve_mailbox_with_shutdown(endpoint, me, token, handler, CancellationToken::new()).await
}

/// Like [`serve_mailbox`], but stops pulling NEW inbound messages once
/// `shutdown` is cancelled — any handlers already in flight still run to
/// completion and their replies are still delivered + acked (via the same
/// drain [`serve_loop`] performs on connection close) before this returns.
/// This is the hook a process-level signal handler (SIGTERM/ctrl_c) wires
/// into, so stopping a worker doesn't have to hard-kill it mid-Ask. #49.
pub async fn serve_mailbox_with_shutdown<H, Fut>(
    endpoint: &str,
    me: AgentRef,
    token: &str,
    handler: H,
    shutdown: CancellationToken,
) -> BrokerResult<()>
where
    H: Fn(InboxMessage, CancellationToken) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Handled> + Send + 'static,
{
    serve_mailbox_full(endpoint, me, token, handler, shutdown, None).await
}

/// Like [`serve_mailbox_with_shutdown`], plus an optional rustls
/// `ClientConfig` for `wss://` (e.g. [`crate::client::client_config_trusting_cert`]
/// to trust a self-signed broker cert without touching the OS trust store).
/// `None` behaves exactly like [`serve_mailbox_with_shutdown`] — the OS
/// native root store. #48.
pub async fn serve_mailbox_full<H, Fut>(
    endpoint: &str,
    me: AgentRef,
    token: &str,
    handler: H,
    shutdown: CancellationToken,
    tls_config: Option<Arc<rustls::ClientConfig>>,
) -> BrokerResult<()>
where
    H: Fn(InboxMessage, CancellationToken) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Handled> + Send + 'static,
{
    let mut client =
        BrokerClient::connect_with_tls(endpoint, me.clone(), token, clone_tls_config(&tls_config))
            .await?;
    client.subscribe().await?;
    serve_loop(&mut client, &me, handler, shutdown).await
}

/// [`BrokerClient::connect_with_tls`] takes an owned `ClientConfig` (it hands
/// it straight to `Connector::Rustls(Arc::new(cfg))`), but the serve chain
/// here shares ONE config across the worker's own connection plus every
/// per-run reconnect ([`handle_run`]'s forward/approval deliver
/// connections) — so it's held as an `Arc` and cloned out (rustls
/// `ClientConfig::clone` is cheap: its fields are themselves `Arc`-backed)
/// at each call site instead of rebuilding it. #48.
fn clone_tls_config(cfg: &Option<Arc<rustls::ClientConfig>>) -> Option<rustls::ClientConfig> {
    cfg.as_deref().cloned()
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
///
/// `shutdown`: once cancelled, the loop stops pulling new inbound messages
/// (same as a closed connection) but keeps draining in-flight handlers through
/// arm A until they've all completed, THEN returns — so a graceful stop lets
/// the current Ask finish and its reply get delivered+acked instead of being
/// abandoned. #49.
pub async fn serve_loop<H, Fut>(
    client: &mut BrokerClient,
    me: &AgentRef,
    handler: H,
    shutdown: CancellationToken,
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
            // `biased`: drain finished handlers (arm A) ahead of a graceful-stop
            // signal (arm B) ahead of pulling new work/cancels (arm C), so
            // completed replies are delivered+acked and their in-flight entries
            // cleared promptly (bounding memory under load), and a shutdown
            // request is noticed before another inbound message is pulled.
            // (#144's serve_mcp_proxy is unbiased; here arm C internally biases
            // the cancel lane, so cancel latency stays prompt — completions are
            // gated by real agent work, so arm C is always reached between them.)
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
            // B. Graceful stop requested (#49): stop pulling new work — mirrors
            //    the `Message(None)` connection-closed case below — but leave
            //    the connection open so arm A can keep delivering+acking
            //    replies for handlers already in flight. Guarded on
            //    `messages_open` so a signal that fires more than once (or
            //    after we've already stopped pulling) doesn't re-trigger.
            _ = shutdown.cancelled(), if messages_open => {
                tracing::info!("broker worker: graceful shutdown requested — draining in-flight work");
                messages_open = false;
            }
            // C. The next inbound message OR out-of-band cancel (demuxed over one
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
    E: bamboo_subagent::ChildExecutor + ?Sized,
{
    // A fresh, never-cancelled token: identical behavior to before graceful
    // shutdown existed — no external caller of this entry point can trip it.
    serve_executor_with_shutdown(endpoint, me, token, executor, CancellationToken::new()).await
}

/// Like [`serve_executor`], but stops accepting new Ask/Task/Run work once
/// `shutdown` is cancelled while letting whatever is already in flight finish
/// and reply normally (see [`serve_mailbox_with_shutdown`]). Wire this to a
/// process-level SIGTERM/ctrl_c handler so `deploy_agent action=stop` (or an
/// orchestrator exit) doesn't abandon an in-progress Ask. #49.
pub async fn serve_executor_with_shutdown<E>(
    endpoint: &str,
    me: AgentRef,
    token: &str,
    executor: Arc<E>,
    shutdown: CancellationToken,
) -> BrokerResult<()>
where
    E: bamboo_subagent::ChildExecutor + ?Sized,
{
    serve_executor_full(endpoint, me, token, executor, shutdown, None).await
}

/// Like [`serve_executor_with_shutdown`], plus an optional rustls
/// `ClientConfig` for `wss://` (see [`serve_mailbox_full`]) — threaded into
/// BOTH the worker's own connection and [`handle_run`]'s per-run
/// forward/approval reconnects, so a `Run`'s event/approval traffic uses the
/// same self-signed-cert trust as the worker's primary connection. `None`
/// behaves exactly like [`serve_executor_with_shutdown`]. #48.
pub async fn serve_executor_full<E>(
    endpoint: &str,
    me: AgentRef,
    token: &str,
    executor: Arc<E>,
    shutdown: CancellationToken,
    tls_config: Option<Arc<rustls::ClientConfig>>,
) -> BrokerResult<()>
where
    E: bamboo_subagent::ChildExecutor + ?Sized,
{
    let context: Arc<tokio::sync::Mutex<Vec<serde_json::Value>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::new()));
    // Per-run coordination so a SEPARATE Steer / ApprovalReply mailbox message can
    // reach the channels of the Run it belongs to (the Run + its control messages
    // arrive as independent messages handled by independent tasks).
    let coords: RunCoords = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
    let waiters: ApprovalWaiters = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
    let endpoint_owned = endpoint.to_string();
    let token_owned = token.to_string();
    let me_owned = me.clone();
    let tls_owned = tls_config.clone();
    serve_mailbox_full(
        endpoint,
        me,
        token,
        move |msg, cancel| {
            let executor = Arc::clone(&executor);
            let context = Arc::clone(&context);
            let coords = Arc::clone(&coords);
            let waiters = Arc::clone(&waiters);
            let endpoint = endpoint_owned.clone();
            let token = token_owned.clone();
            let me = me_owned.clone();
            let tls_config = tls_owned.clone();
            async move {
                match msg.kind {
                    // A full child session over the bus (the actor-over-mailbox path):
                    // stream events back to the parent live, then the terminal outcome.
                    InboxKind::Run => {
                        handle_run(
                            executor.as_ref(),
                            &endpoint,
                            &token,
                            &me,
                            msg,
                            cancel,
                            &coords,
                            &waiters,
                            tls_config,
                        )
                        .await
                    }
                    // In-band steer for a running Run: route to its steer inbox.
                    InboxKind::Steer => {
                        if let Some(run_id) = &msg.correlation_id {
                            let text = msg
                                .body
                                .get("text")
                                .and_then(|v| v.as_str())
                                .unwrap_or_default()
                                .to_string();
                            if let Some(coord) = coords.lock().await.get(run_id) {
                                let _ = coord.steer_tx.send(text);
                            }
                        }
                        Handled::Ack
                    }
                    // Approval decision for a gated tool a Run proxied up: wake the
                    // waiting tool call, keyed by the approval-request id in the body.
                    InboxKind::ApprovalReply => {
                        let id = msg
                            .body
                            .get("id")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                            .to_string();
                        let approved = msg
                            .body
                            .get("approved")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);
                        if let Some(tx) = waiters.lock().await.remove(&id) {
                            let _ = tx.send(approved);
                        }
                        Handled::Ack
                    }
                    // Ask/Task: the conversational query/steer path (unchanged).
                    _ => handle_with_executor(executor.as_ref(), &context, msg, cancel).await,
                }
            }
        },
        shutdown,
        tls_config,
    )
    .await
}

/// Live steer channel for a running [`InboxKind::Run`], keyed by run id so an
/// out-of-band [`InboxKind::Steer`] message can be pushed into the run's inbox.
struct RunCoord {
    steer_tx: tokio::sync::mpsc::UnboundedSender<String>,
}
type RunCoords = Arc<tokio::sync::Mutex<HashMap<MsgId, RunCoord>>>;
/// Pending gated-tool approvals a Run proxied up, keyed by approval-request id;
/// an [`InboxKind::ApprovalReply`] fulfils the matching one.
type ApprovalWaiters = Arc<tokio::sync::Mutex<HashMap<String, tokio::sync::oneshot::Sender<bool>>>>;

/// Drive a full child session ([`InboxKind::Run`]) over the bus: parse the
/// `RunSpec`, run the executor, and forward its streamed events + terminal
/// outcome to the parent's mailbox over a dedicated deliver connection (the same
/// pattern [`crate::mcp::serve_mcp_proxy`] uses for worker→orchestrator I/O).
///
/// The serve loop only `Ack`s the run; the real result flows as `Event`s and a
/// final `Outcome`, both correlated to the run id, so the parent can stream them
/// exactly like it would over a direct WS connection.
#[allow(clippy::too_many_arguments)]
async fn handle_run<E>(
    executor: &E,
    endpoint: &str,
    token: &str,
    me: &AgentRef,
    msg: InboxMessage,
    cancel: CancellationToken,
    coords: &RunCoords,
    waiters: &ApprovalWaiters,
    tls_config: Option<Arc<rustls::ClientConfig>>,
) -> Handled
where
    E: bamboo_subagent::ChildExecutor + ?Sized,
{
    use bamboo_subagent::{EventSink, HostBridge, RunSpec, SteerInbox};

    let spec: RunSpec = match serde_json::from_value(msg.body) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("run {:?}: malformed RunSpec, dropping: {e}", msg.id);
            return Handled::Ack;
        }
    };
    let run_id = msg.id.clone();
    let parent = msg.from.session_id.clone();

    let (sink, mut events) = EventSink::channel();
    // Steer: register this run's steer inbox so out-of-band Steer messages route in.
    let (steer_tx, steer_inbox) = SteerInbox::channel();
    coords
        .lock()
        .await
        .insert(run_id.clone(), RunCoord { steer_tx });
    // Approval: a host bridge on the sink; its requests are pumped to the parent.
    let (host_bridge, mut host_rx) = HostBridge::channel();
    let sink = sink.with_host_bridge(host_bridge);
    let (outcome_tx, outcome_rx) = tokio::sync::oneshot::channel();

    let endpoint = endpoint.to_string();
    let token = token.to_string();
    let me = me.clone();

    // Forward task: own one deliver connection; stream Events live, then the
    // Outcome once the run finishes (sink dropped ⇒ `events` closes).
    let run_id_fwd = run_id.clone();
    let me_fwd = me.clone();
    let parent_fwd = parent.clone();
    let (ep_fwd, tok_fwd) = (endpoint.clone(), token.clone());
    let tls_fwd = tls_config.clone();
    let forward = tokio::spawn(async move {
        let mut deliver = match BrokerClient::connect_with_tls(
            &ep_fwd,
            me_fwd.clone(),
            &tok_fwd,
            tls_fwd.as_deref().cloned(),
        )
        .await
        {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("run {run_id_fwd:?}: event deliver connect failed: {e}");
                return;
            }
        };
        let emit = |kind, body| InboxMessage {
            id: MsgId::new(),
            from: me_fwd.clone(),
            kind,
            body,
            created_at: Utc::now(),
            correlation_id: Some(run_id_fwd.clone()),
        };
        while let Some(event) = events.recv().await {
            if deliver
                .deliver(&parent_fwd, emit(InboxKind::Event, event))
                .await
                .is_err()
            {
                return; // parent/connection gone — stop forwarding.
            }
        }
        if let Ok(outcome) = outcome_rx.await {
            let body = serde_json::to_value(&outcome).unwrap_or_else(|_| serde_json::json!({}));
            let _ = deliver
                .deliver(&parent_fwd, emit(InboxKind::Outcome, body))
                .await;
        }
    });

    // Approval drain: each gated-tool approval the executor raises is delivered to
    // the parent as an ApprovalRequest (correlated to the run); the matching
    // ApprovalReply wakes the registered waiter, whose decision answers the tool.
    // Ends when the run drops the sink ⇒ the host bridge ⇒ `host_rx` closes.
    let waiters_drain = Arc::clone(waiters);
    let run_id_appr = run_id.clone();
    let approval = tokio::spawn(async move {
        let mut deliver = match BrokerClient::connect_with_tls(
            &endpoint,
            me.clone(),
            &token,
            tls_config.as_deref().cloned(),
        )
        .await
        {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("run {run_id_appr:?}: approval deliver connect failed: {e}");
                // Drain + deny so the worker's permission flow never hangs.
                while let Some(req) = host_rx.recv().await {
                    let _ = req.reply.send(serde_json::json!({ "approved": false }));
                }
                return;
            }
        };
        while let Some(req) = host_rx.recv().await {
            let approval_id = MsgId::new();
            let approval_id_str = format!("{approval_id:?}");
            let (atx, arx) = tokio::sync::oneshot::channel::<bool>();
            waiters_drain
                .lock()
                .await
                .insert(approval_id_str.clone(), atx);
            let m = InboxMessage {
                id: MsgId::new(),
                from: me.clone(),
                kind: InboxKind::ApprovalRequest,
                body: serde_json::json!({ "id": approval_id_str, "request": req.body }),
                created_at: Utc::now(),
                correlation_id: Some(run_id_appr.clone()),
            };
            if deliver.deliver(&parent, m).await.is_err() {
                let _ = req.reply.send(serde_json::json!({ "approved": false }));
                continue;
            }
            // Await the parent's decision; a dropped reply (link gone) fail-closes.
            let approved = arx.await.unwrap_or(false);
            let _ = req.reply.send(serde_json::json!({ "approved": approved }));
        }
    });

    // Run to completion (events stream into `sink`); dropping `sink` closes the
    // forward loop's `events` (→ outcome) and the approval drain's `host_rx`.
    let outcome = executor.run(spec, sink, steer_inbox, cancel).await;
    coords.lock().await.remove(&run_id);
    let _ = outcome_tx.send(outcome);
    let _ = forward.await;
    let _ = approval.await;
    Handled::Ack
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
    E: bamboo_subagent::ChildExecutor + ?Sized,
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
                permission_policy: None,
                messages: prior,
                secrets: Default::default(),
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
        use std::sync::atomic::{AtomicU32, Ordering};

        // N concurrent asks to ONE worker. Prove overlap DIRECTLY (issue #486)
        // instead of inferring it from wall-clock duration: this originally
        // asserted `elapsed < 500ms` against a `sleep(200ms)` backend, which
        // raced CI load — a loaded runner can push even genuinely-concurrent
        // asks past any fixed real-ms bound, producing a one-off failure with
        // no code regression.
        //
        // Fix: `SlowEcho` tracks its own instantaneous concurrency
        // (`in_flight` / `max_in_flight` high-water mark via `fetch_max`) AND
        // forces the rendezvous instead of hoping the scheduler produces it:
        // each of the N batch runs blocks on a `Barrier` sized for N until
        // all N have arrived, so `max_in_flight == N` is deterministic
        // regardless of host load. If per-ask spawn ever regresses to serial
        // handling, only 1 of the N barrier parties will ever arrive and the
        // wait deadlocks — bounded by the outer `tokio::time::timeout` below,
        // turning that regression into a clear, fast failure instead of a
        // hang. The preceding subscription probe ("ping") is exempted from
        // the barrier — it runs alone, before the batch, specifically to
        // confirm subscription, and would otherwise deadlock waiting for N-1
        // batch calls that haven't been sent yet.
        const N: usize = 4;
        struct SlowEcho {
            in_flight: AtomicU32,
            max_in_flight: AtomicU32,
            rendezvous: tokio::sync::Barrier,
        }
        #[async_trait::async_trait]
        impl ChildExecutor for SlowEcho {
            async fn run(
                &self,
                spec: RunSpec,
                _events: EventSink,
                _steer: SteerInbox,
                _cancel: CancellationToken,
            ) -> ChildOutcome {
                if spec.assignment == "ping" {
                    return ChildOutcome::completed(format!("done: {}", spec.assignment));
                }
                let now_in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_in_flight
                    .fetch_max(now_in_flight, Ordering::SeqCst);
                // Forced rendezvous: block until all N concurrent batch asks
                // have arrived here.
                self.rendezvous.wait().await;
                tokio::time::sleep(Duration::from_millis(50)).await;
                self.in_flight.fetch_sub(1, Ordering::SeqCst);
                ChildOutcome::completed(format!("done: {}", spec.assignment))
            }
        }
        let slow_echo = Arc::new(SlowEcho {
            in_flight: AtomicU32::new(0),
            max_in_flight: AtomicU32::new(0),
            rendezvous: tokio::sync::Barrier::new(N),
        });

        let (endpoint, _dir) = start().await;
        let worker_ep = endpoint.clone();
        let slow_echo_for_worker = slow_echo.clone();
        tokio::spawn(async move {
            let _ = serve_executor(
                &worker_ep,
                AgentRef {
                    session_id: "worker".into(),
                    role: None,
                },
                TOKEN,
                slow_echo_for_worker,
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
        // correlated replies. Each batch run is forced to rendezvous inside
        // `SlowEcho` (see above), so genuine overlap is deterministic rather
        // than inferred from wall-clock duration.
        let mut want: std::collections::HashSet<MsgId> = std::collections::HashSet::new();
        for i in 0..N {
            let q = ask("orch", &format!("q{i}"));
            want.insert(q.id.clone());
            orch.deliver("worker", q).await.unwrap();
        }
        tokio::time::timeout(Duration::from_secs(20), async {
            while !want.is_empty() {
                let r = tokio::time::timeout(Duration::from_secs(5), orch.next_message())
                    .await
                    .expect("a reply arrives")
                    .expect("present");
                if let Some(cid) = &r.correlation_id {
                    want.remove(cid);
                }
            }
        })
        .await
        .expect(
            "timed out waiting for the N concurrent Asks to complete — this means the \
             per-ask spawn is serializing them (only some of the N ever reached the \
             rendezvous barrier), which is the regression this test guards against",
        );
        let max_in_flight = slow_echo.max_in_flight.load(Ordering::SeqCst);
        assert_eq!(
            max_in_flight, N as u32,
            "{N} concurrent Asks to ONE worker must OVERLAP (serial handling could never \
             observe more than 1 in flight at once); observed max_in_flight = {max_in_flight}"
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

    /// A full child session over the bus: deliver a `Run`, and the worker streams
    /// `Event`s then a terminal `Outcome` to the parent — the actor-over-mailbox
    /// path (P1.3). Proves the broker carries run/events/outcome with no wire
    /// change, exactly mirroring a direct-WS child run.
    #[tokio::test]
    async fn run_streams_events_then_outcome_to_parent() {
        use bamboo_subagent::{EchoExecutor, RunSpec};

        let (endpoint, _dir) = start().await;

        // Echo worker on the bus (serve_executor now also handles Run).
        let worker_ep = endpoint.clone();
        tokio::spawn(async move {
            let _ = serve_executor(
                &worker_ep,
                AgentRef {
                    session_id: "w".into(),
                    role: None,
                },
                TOKEN,
                Arc::new(EchoExecutor),
            )
            .await;
        });

        // Parent subscribes, then delivers a Run to the worker.
        let mut parent = BrokerClient::connect(
            &endpoint,
            AgentRef {
                session_id: "orch".into(),
                role: None,
            },
            TOKEN,
        )
        .await
        .unwrap();
        parent.subscribe().await.unwrap();

        let spec = RunSpec {
            assignment: "ping pong".into(),
            reasoning_effort: None,
            permission_policy: None,
            messages: vec![],
            secrets: Default::default(),
        };
        let run = InboxMessage {
            id: MsgId::new(),
            from: AgentRef {
                session_id: "orch".into(),
                role: None,
            },
            kind: InboxKind::Run,
            body: serde_json::to_value(&spec).unwrap(),
            created_at: Utc::now(),
            correlation_id: None,
        };
        let run_id = run.id.clone();
        parent.deliver("w", run).await.unwrap();

        // Collect streamed Events until the terminal Outcome (all correlated).
        let mut events = 0usize;
        let outcome = loop {
            let msg = tokio::time::timeout(Duration::from_secs(5), parent.next_message())
                .await
                .expect("a run message arrives")
                .expect("stream open");
            assert_eq!(
                msg.correlation_id.as_ref(),
                Some(&run_id),
                "run messages must correlate to the run id"
            );
            match msg.kind {
                InboxKind::Event => {
                    events += 1;
                    parent.ack(msg.id).await.ok();
                }
                InboxKind::Outcome => break msg,
                other => panic!("unexpected kind during run: {other:?}"),
            }
        };

        assert!(events >= 1, "expected streamed events, got {events}");
        let oc: bamboo_subagent::ChildOutcome = serde_json::from_value(outcome.body).unwrap();
        assert_eq!(oc.result.as_deref(), Some("echo: ping pong"));
    }

    /// Graceful shutdown (#49): tripping the shutdown token while an Ask is in
    /// flight must NOT abandon it — the worker finishes the run, delivers the
    /// reply (delivered + acked), and only THEN does the serve future return.
    #[tokio::test]
    async fn graceful_shutdown_drains_in_flight_ask_then_exits() {
        use bamboo_subagent::{ChildExecutor, ChildOutcome, EventSink, RunSpec, SteerInbox};

        // An executor slow enough that the shutdown signal provably lands while
        // the run is still in flight.
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
                tokio::time::sleep(Duration::from_millis(300)).await;
                ChildOutcome::completed(format!("echo: {}", spec.assignment))
            }
        }

        let (endpoint, _dir) = start().await;
        let shutdown = CancellationToken::new();
        let worker_ep = endpoint.clone();
        let worker_shutdown = shutdown.clone();
        let worker = tokio::spawn(async move {
            serve_executor_with_shutdown(
                &worker_ep,
                AgentRef {
                    session_id: "worker".into(),
                    role: None,
                },
                TOKEN,
                Arc::new(SlowEcho),
                worker_shutdown,
            )
            .await
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

        // Probe round-trip so the worker is provably subscribed before the
        // in-flight ask + shutdown race begins.
        let probe = ask("orch", "ping");
        let probe_id = probe.id.clone();
        orch.deliver("worker", probe).await.unwrap();
        let r0 = tokio::time::timeout(Duration::from_secs(5), orch.next_message())
            .await
            .expect("probe reply")
            .expect("present");
        assert_eq!(r0.correlation_id, Some(probe_id));

        // Fire the slow ask, then request graceful shutdown while it's running.
        let q = ask("orch", "slow one");
        let qid = q.id.clone();
        orch.deliver("worker", q).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await; // let the run start
        shutdown.cancel();

        // The in-flight ask still completes and its reply is delivered — a
        // graceful stop is a drain, not an abandonment.
        let reply = tokio::time::timeout(Duration::from_secs(5), orch.next_message())
            .await
            .expect("in-flight ask must be drained, not lost")
            .expect("present");
        assert_eq!(reply.correlation_id, Some(qid));
        let body: ReplyBody = serde_json::from_value(reply.body).unwrap();
        assert_eq!(body.answer, "echo: slow one");

        // And the serve future itself returns (cleanly) once drained.
        let served = tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .expect("serve_executor_with_shutdown returns after the drain")
            .expect("worker task not panicked");
        assert!(served.is_ok(), "graceful shutdown exits Ok: {served:?}");
    }

    /// Graceful shutdown with an idle worker: no in-flight work means the serve
    /// future returns promptly on cancel (no wedge waiting for work that will
    /// never arrive). #49.
    #[tokio::test]
    async fn graceful_shutdown_idle_worker_exits_promptly() {
        let (endpoint, _dir) = start().await;
        let shutdown = CancellationToken::new();
        let worker_shutdown = shutdown.clone();
        let worker_ep = endpoint.clone();
        let worker = tokio::spawn(async move {
            serve_executor_with_shutdown(
                &worker_ep,
                AgentRef {
                    session_id: "idle".into(),
                    role: None,
                },
                TOKEN,
                Arc::new(bamboo_subagent::EchoExecutor),
                worker_shutdown,
            )
            .await
        });

        // Prove it's up (subscribed) with a probe round-trip.
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
        let probe = ask("orch", "ping");
        orch.deliver("idle", probe).await.unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), orch.next_message())
            .await
            .expect("probe reply")
            .expect("present");

        shutdown.cancel();
        let served = tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .expect("idle worker exits promptly on graceful shutdown")
            .expect("worker task not panicked");
        assert!(served.is_ok(), "graceful shutdown exits Ok: {served:?}");
    }

    /// The bus answers "who's connected serving role X" over the WS protocol — the
    /// Phase 3 presence query the schedulable cutover uses instead of an HTTP
    /// registry. Subscribing with a role makes a connection discoverable.
    #[tokio::test]
    async fn list_connected_finds_subscribed_actors_by_role() {
        let (endpoint, _dir) = start().await;

        async fn join(endpoint: &str, id: &str, role: &str) -> BrokerClient {
            let mut c = BrokerClient::connect(
                endpoint,
                AgentRef {
                    session_id: id.into(),
                    role: Some(role.into()),
                },
                TOKEN,
            )
            .await
            .unwrap();
            c.subscribe().await.unwrap();
            c
        }
        let _w1 = join(&endpoint, "w1", "gpu-pool").await;
        let _w2 = join(&endpoint, "w2", "gpu-pool").await;
        let _w3 = join(&endpoint, "w3", "cpu-pool").await;

        let mut q = BrokerClient::connect(
            &endpoint,
            AgentRef {
                session_id: "orch".into(),
                role: None,
            },
            TOKEN,
        )
        .await
        .unwrap();

        let mut gpu = q.list_connected("gpu-pool").await.unwrap();
        gpu.sort();
        assert_eq!(gpu, vec!["w1".to_string(), "w2".to_string()]);
        assert_eq!(
            q.list_connected("cpu-pool").await.unwrap(),
            vec!["w3".to_string()]
        );
        assert!(q.list_connected("none").await.unwrap().is_empty());
    }
}
