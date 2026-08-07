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
use std::time::Duration;

use bamboo_subagent::{AdmittedSet, AgentRef, InboxKind, InboxMessage, MsgId, ReplyBody};
use chrono::Utc;
use tokio_util::sync::CancellationToken;

use crate::client::BrokerClient;
use crate::error::{BrokerError, BrokerResult};

/// Connection loss is not a graceful shutdown: the owner can no longer receive
/// results, so cancel admitted work and give cancellation-aware handlers a short
/// bounded window to clean up before force-aborting them.
const DEFAULT_CONNECTION_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// `JoinHandle::abort` is cooperative: a future doing synchronous work cannot
/// observe it until that poll returns. Keep the library bounded anyway; the
/// dedicated `subagent-worker` process hard-exits on this returned error.
const DEFAULT_ABORT_JOIN_TIMEOUT: Duration = Duration::from_secs(1);

/// Approval is intentionally human-scale while the owner is alive. Owner loss
/// cancels this wait immediately; this deadline is the fail-closed backstop for
/// a live but non-responsive owner.
const DEFAULT_APPROVAL_TIMEOUT: Duration = Duration::from_secs(15 * 60);

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

/// Why a mailbox worker stopped serving normally.
///
/// Transport errors still return [`crate::BrokerError`]. These reasons cover
/// clean lifecycle exits and are intentionally separate from durable child
/// business state (#592): callers use them for process/pool observability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServeExitReason {
    ShutdownRequested,
    ConnectionClosed,
    IdleTimeout,
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
/// completion and their replies are still delivered + acked before this
/// returns. This graceful path is intentionally different from unexpected
/// connection loss, which cancels admitted handlers and bounds their cleanup
/// because no reply or ack can reach the owner. This is the hook a process-level
/// signal handler (SIGTERM/ctrl_c) wires into. #49/#742.
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
    serve_mailbox_full_with_lifecycle(endpoint, me, token, handler, shutdown, tls_config, None)
        .await
        .map(|_| ())
}

async fn serve_mailbox_full_with_lifecycle<H, Fut>(
    endpoint: &str,
    me: AgentRef,
    token: &str,
    handler: H,
    shutdown: CancellationToken,
    tls_config: Option<Arc<rustls::ClientConfig>>,
    idle_timeout: Option<Duration>,
) -> BrokerResult<ServeExitReason>
where
    H: Fn(InboxMessage, CancellationToken) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Handled> + Send + 'static,
{
    let mut client =
        BrokerClient::connect_with_tls(endpoint, me.clone(), token, clone_tls_config(&tls_config))
            .await?;
    client.subscribe().await?;
    serve_loop_with_idle_timeout(&mut client, &me, handler, shutdown, idle_timeout).await
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

struct InflightHandler {
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
    /// Number of mailbox deliveries with this id observed while the one active
    /// admission runs. None is acked until that admission completes successfully.
    deliveries: usize,
}

/// A spawned helper owned by its parent future. Tokio detaches a task when a
/// plain `JoinHandle` is dropped; this wrapper instead aborts it, so aborting a
/// stuck top-level handler cannot orphan its forwarding/approval descendants.
struct AbortOnDropTask<T>(Option<tokio::task::JoinHandle<T>>);

impl<T> AbortOnDropTask<T> {
    fn new(task: tokio::task::JoinHandle<T>) -> Self {
        Self(Some(task))
    }

    async fn join(&mut self) -> Result<T, tokio::task::JoinError> {
        let result = self.0.as_mut().expect("task handle present").await;
        self.0.take();
        result
    }
}

impl<T> Drop for AbortOnDropTask<T> {
    fn drop(&mut self) {
        if let Some(task) = &self.0 {
            task.abort();
        }
    }
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
/// `shutdown`: once cancelled, the loop stops pulling new inbound messages but
/// leaves admitted handlers uncancelled until they finish, then delivers and
/// acks their results. Unexpected connection loss is a separate cancel-only,
/// bounded drain: handlers are cancelled immediately, completions are joined
/// without dead-socket I/O, and stuck work is aborted at the deadline. #49/#742.
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
    serve_loop_with_idle_timeout(client, me, handler, shutdown, None)
        .await
        .map(|_| ())
}

/// Serve with an optional true-idle deadline and report the clean exit reason.
/// The deadline is disabled while any handler is in flight and restarts only
/// after the final completion is delivered and acked.
pub async fn serve_loop_with_idle_timeout<H, Fut>(
    client: &mut BrokerClient,
    me: &AgentRef,
    handler: H,
    shutdown: CancellationToken,
    idle_timeout: Option<Duration>,
) -> BrokerResult<ServeExitReason>
where
    H: Fn(InboxMessage, CancellationToken) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Handled> + Send + 'static,
{
    serve_loop_with_timeouts(
        client,
        me,
        handler,
        shutdown,
        idle_timeout,
        DEFAULT_CONNECTION_DRAIN_TIMEOUT,
        DEFAULT_ABORT_JOIN_TIMEOUT,
    )
    .await
}

async fn serve_loop_with_timeouts<H, Fut>(
    client: &mut BrokerClient,
    me: &AgentRef,
    handler: H,
    shutdown: CancellationToken,
    idle_timeout: Option<Duration>,
    connection_drain_timeout: Duration,
    abort_join_timeout: Duration,
) -> BrokerResult<ServeExitReason>
where
    H: Fn(InboxMessage, CancellationToken) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Handled> + Send + 'static,
{
    let handler = Arc::new(handler);

    // Live cancel tokens for runs still in flight, keyed by the run (message) id.
    // A cancel naming an id present here trips its token (the executor honors it
    // mid-LLM-call); a cancel for an unknown id (already finished / never started)
    // is a no-op, exactly as before. An entry is removed when its run completes.
    let mut inflight: HashMap<MsgId, InflightHandler> = HashMap::new();
    // Connection-local successful admissions. A late duplicate can be acked
    // without re-running the handler, while Leave/crash remains redeliverable.
    // AdmittedSet bounds this memory for long-lived reusable workers.
    let mut completed_admissions = AdmittedSet::default();

    // Finished handlers flow back here to the single owner for delivery+ack.
    // KEEP-ALIVE: this original `done_tx` stays in scope for the whole loop (each
    // spawn clones it), so `done_rx.recv()` only returns `None` once the loop is
    // tearing down — never spuriously while runs are in flight. Mirrors the
    // `reply_tx` keep-alive in `serve_mcp_proxy`. #144/#45.
    let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel::<Completion>();

    let mut messages_open = true;
    let mut exit_reason = ServeExitReason::ConnectionClosed;
    let idle_sleep =
        tokio::time::sleep(idle_timeout.unwrap_or_else(|| Duration::from_secs(365 * 24 * 60 * 60)));
    tokio::pin!(idle_sleep);
    let connection_drain_sleep = tokio::time::sleep(Duration::from_secs(365 * 24 * 60 * 60));
    tokio::pin!(connection_drain_sleep);
    let mut connection_lost = false;
    let mut connection_failure = None;
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
                let Completion { id, reply_to, handled } = done;
                let Some(completed) = inflight.remove(&id) else {
                    // A completion must belong to the one active admission for
                    // this id. Never let a stale/duplicate completion ack a
                    // later generation that happens to reuse the same MsgId.
                    tracing::warn!(message_id = %id.as_str(), "ignoring completion without matching in-flight admission");
                    continue;
                };
                let delivery_count = completed.deliveries;
                let remember_completion = !matches!(handled, Handled::Leave);
                // The reader marks death before closing its event lanes. This
                // closes the completion-vs-close select race: even if this
                // biased arm wins first, it must not write to the dead socket
                // or return early while other handlers remain detached.
                if !connection_lost && !client.reader_alive() {
                    messages_open = false;
                    exit_reason = ServeExitReason::ConnectionClosed;
                    connection_lost = true;
                    for handler in inflight.values() {
                        handler.cancel.cancel();
                    }
                    connection_drain_sleep.as_mut().reset(
                        tokio::time::Instant::now() + connection_drain_timeout,
                    );
                }
                // Once the connection is known dead, no reply or ack can reach
                // the broker. Still consume completions so cooperative handlers
                // are joined and the bounded drain can finish cleanly.
                let wire_result = if !connection_lost {
                    async {
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
                            for _ in 0..delivery_count {
                                client.ack(id.clone()).await?;
                            }
                        }
                        Handled::Ack => {
                            for _ in 0..delivery_count {
                                client.ack(id.clone()).await?;
                            }
                        }
                        Handled::Leave => {}
                        }
                        Ok::<(), BrokerError>(())
                    }
                    .await
                } else {
                    Ok(())
                };
                let _ = completed.task.await;
                if let Err(error) = wire_result {
                    tracing::warn!(%error, "broker worker completion delivery failed; cancelling remaining handlers");
                    messages_open = false;
                    connection_lost = true;
                    connection_failure = Some(error);
                    for handler in inflight.values() {
                        handler.cancel.cancel();
                    }
                    connection_drain_sleep.as_mut().reset(
                        tokio::time::Instant::now() + connection_drain_timeout,
                    );
                } else if !connection_lost && remember_completion {
                    completed_admissions.insert(id);
                }
                if let Some(timeout) = idle_timeout {
                    idle_sleep
                        .as_mut()
                        .reset(tokio::time::Instant::now() + timeout);
                }
            }
            // B. Graceful stop requested (#49): stop pulling new work but keep
            //    admitted handlers uncancelled and the connection open so arm A
            //    can keep delivering+acking their replies. Unlike the disconnect
            //    arms below, this path has no forced drain deadline. Guarded on
            //    `messages_open` so a signal that fires more than once (or
            //    after we've already stopped pulling) doesn't re-trigger.
            _ = shutdown.cancelled(), if messages_open => {
                tracing::info!("broker worker: graceful shutdown requested — draining in-flight work");
                messages_open = false;
                exit_reason = ServeExitReason::ShutdownRequested;
            }
            // C. The next inbound message OR out-of-band cancel (demuxed over one
            //    `&mut client` borrow). A cancel trips the matching in-flight run's
            //    token (#50); a new message registers a fresh token and spawns the
            //    handler on its own task — so concurrent Asks overlap their work and
            //    only the (cheap) wire I/O stays serialized through this owner. #45.
            // Keep observing the transport while a graceful shutdown drains.
            // Admission is closed below, but a subsequent real disconnect must
            // still upgrade the drain to cancel+deadline semantics.
            event = client.next_message_or_cancel(), if !connection_lost => match event {
                crate::client::ServeEvent::Cancel(Some(cid)) => {
                    if let Some(timeout) = idle_timeout {
                        idle_sleep
                            .as_mut()
                            .reset(tokio::time::Instant::now() + timeout);
                    }
                    if let Some(handler) = inflight.get(&cid) {
                        handler.cancel.cancel();
                    }
                }
                // Cancel lane closed (reader gone). The message lane is fed by the
                // same reader, so cancel every admitted handler and join cooperative
                // completions without dead-socket I/O, bounded by the drain timer.
                crate::client::ServeEvent::Cancel(None) => {
                    messages_open = false;
                    exit_reason = ServeExitReason::ConnectionClosed;
                    connection_lost = true;
                    for handler in inflight.values() {
                        handler.cancel.cancel();
                    }
                    connection_drain_sleep.as_mut().reset(
                        tokio::time::Instant::now() + connection_drain_timeout,
                    );
                }
                crate::client::ServeEvent::Message(Some(msg)) if messages_open => {
                    if let Some(timeout) = idle_timeout {
                        idle_sleep
                            .as_mut()
                            .reset(tokio::time::Instant::now() + timeout);
                    }
                    let id = msg.id.clone();
                    if completed_admissions.contains(&id) {
                        // This id already completed successfully on this
                        // connection. Ack this newly observed durable copy, but
                        // never run the handler or emit a duplicate reply.
                        if let Err(error) = client.ack(id).await {
                            tracing::warn!(%error, "broker worker duplicate ack failed; cancelling remaining handlers");
                            messages_open = false;
                            connection_lost = true;
                            connection_failure = Some(error);
                            for handler in inflight.values() {
                                handler.cancel.cancel();
                            }
                            connection_drain_sleep.as_mut().reset(
                                tokio::time::Instant::now() + connection_drain_timeout,
                            );
                        }
                    } else if let Some(active) = inflight.get_mut(&id) {
                        // Coalesce an at-least-once duplicate into the active
                        // admission. It remains unacked until that handler
                        // succeeds, so a crash/Leave cannot lose the message.
                        active.deliveries = active.deliveries.saturating_add(1);
                    } else {
                        let reply_to = msg.from.session_id.clone();
                        let token = CancellationToken::new();
                        let inflight_id = id.clone();
                        let inflight_cancel = token.clone();
                        let handler = Arc::clone(&handler);
                        let done_tx = done_tx.clone();
                        let task = tokio::spawn(async move {
                            let handled = handler(msg, token).await;
                            // Receiver gone == owner loop exited (conn dropped) -> drop.
                            let _ = done_tx.send(Completion { id, reply_to, handled });
                        });
                        inflight.insert(
                            inflight_id,
                            InflightHandler {
                                cancel: inflight_cancel,
                                task,
                                deliveries: 1,
                            },
                        );
                    }
                }
                // A message already in the reader queue when graceful shutdown
                // closed admission stays unacked for a future worker; do not
                // start new work while we merely observe the transport.
                crate::client::ServeEvent::Message(Some(_)) => {}
                // Connection closed: cancel every admitted handler and join
                // cooperative completions, bounded by the drain timer.
                crate::client::ServeEvent::Message(None) => {
                    messages_open = false;
                    exit_reason = ServeExitReason::ConnectionClosed;
                    connection_lost = true;
                    for handler in inflight.values() {
                        handler.cancel.cancel();
                    }
                    connection_drain_sleep.as_mut().reset(
                        tokio::time::Instant::now() + connection_drain_timeout,
                    );
                }
            },
            _ = &mut idle_sleep,
                if messages_open && inflight.is_empty() && idle_timeout.is_some() =>
            {
                tracing::info!(
                    idle_timeout_ms = idle_timeout.expect("guarded").as_millis() as u64,
                    shutdown_reason = "idle_timeout",
                    "broker worker reached its true-idle deadline"
                );
                messages_open = false;
                exit_reason = ServeExitReason::IdleTimeout;
            }
            _ = &mut connection_drain_sleep,
                if connection_lost && !inflight.is_empty() =>
            {
                let mut stuck_ids = inflight
                    .keys()
                    .map(|id| id.as_str().to_string())
                    .collect::<Vec<_>>();
                stuck_ids.sort();
                tracing::error!(
                    drain_timeout_ms = connection_drain_timeout.as_millis() as u64,
                    stuck_ids = ?stuck_ids,
                    "broker worker connection-loss drain timed out; aborting stuck handlers"
                );
                let stuck = std::mem::take(&mut inflight);
                for handler in stuck.values() {
                    handler.cancel.cancel();
                    handler.task.abort();
                }
                let join_aborted = async move {
                    for (_, handler) in stuck {
                        let _ = handler.task.await;
                    }
                };
                let abort_join_timed_out =
                    tokio::time::timeout(abort_join_timeout, join_aborted)
                        .await
                        .is_err();
                if abort_join_timed_out {
                    tracing::error!(
                        abort_join_timeout_ms = abort_join_timeout.as_millis() as u64,
                        "aborted broker handlers did not yield within the bounded join window; dedicated worker must hard-exit"
                    );
                }
                return Err(BrokerError::ConnectionDrainTimeout {
                    timeout_ms: connection_drain_timeout.as_millis() as u64,
                    stuck_ids,
                    abort_join_timeout_ms: abort_join_timeout.as_millis() as u64,
                    abort_join_timed_out,
                });
            }
        }

        // Graceful shutdown reaches here after normal delivered+acked completion;
        // disconnect reaches here after cancellation and join without wire I/O.
        if !messages_open && inflight.is_empty() {
            if let Some(error) = connection_failure {
                return Err(error);
            }
            break;
        }
    }
    Ok(exit_reason)
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
    serve_executor_full_with_lifecycle(endpoint, me, token, executor, shutdown, tls_config, None)
        .await
        .map(|_| ())
}

/// Serve an executor with bounded true-idle lifetime and return a structured
/// clean shutdown reason. In-flight work disables the idle deadline; explicit
/// shutdown keeps the existing graceful-drain behavior.
pub async fn serve_executor_with_lifecycle<E>(
    endpoint: &str,
    me: AgentRef,
    token: &str,
    executor: Arc<E>,
    shutdown: CancellationToken,
    idle_timeout: Option<Duration>,
) -> BrokerResult<ServeExitReason>
where
    E: bamboo_subagent::ChildExecutor + ?Sized,
{
    serve_executor_full_with_lifecycle(endpoint, me, token, executor, shutdown, None, idle_timeout)
        .await
}

#[allow(clippy::too_many_arguments)]
async fn serve_executor_full_with_lifecycle<E>(
    endpoint: &str,
    me: AgentRef,
    token: &str,
    executor: Arc<E>,
    shutdown: CancellationToken,
    tls_config: Option<Arc<rustls::ClientConfig>>,
    idle_timeout: Option<Duration>,
) -> BrokerResult<ServeExitReason>
where
    E: bamboo_subagent::ChildExecutor + ?Sized,
{
    let context: Arc<tokio::sync::Mutex<Vec<serde_json::Value>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::new()));
    // Per-run coordination so a SEPARATE Steer / ApprovalReply mailbox message can
    // reach the channels of the Run it belongs to (the Run + its control messages
    // arrive as independent messages handled by independent tasks).
    let coords: RunCoords = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
    let waiters: ApprovalWaiters = Arc::new(std::sync::Mutex::new(HashMap::new()));
    let endpoint_owned = endpoint.to_string();
    let token_owned = token.to_string();
    let me_owned = me.clone();
    let tls_owned = tls_config.clone();
    let approval_timeout = DEFAULT_APPROVAL_TIMEOUT;
    serve_mailbox_full_with_lifecycle(
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
                            approval_timeout,
                        )
                        .await
                    }
                    // In-band steer for a running Run: route to its steer inbox.
                    InboxKind::Steer => {
                        if let Some(run_id) = &msg.correlation_id {
                            let steer = decode_steer_body(&msg.body, Some(msg.id.as_str()));
                            if let Some(steer) = steer {
                                let coords = coords.lock().await;
                                if let Some(coord) = coords.get(run_id) {
                                    let _ = coord.steer_tx.send(steer);
                                }
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
                        if let Some(tx) = approval_waiters_lock(&waiters).remove(&id) {
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
        idle_timeout,
    )
    .await
}

/// Live steer channel for a running [`InboxKind::Run`], keyed by run id so an
/// out-of-band [`InboxKind::Steer`] message can be pushed into the run's inbox.
struct RunCoord {
    steer_tx: tokio::sync::mpsc::UnboundedSender<bamboo_subagent::SteerMessage>,
}
type RunCoords = Arc<tokio::sync::Mutex<HashMap<MsgId, RunCoord>>>;
/// Pending gated-tool approvals a Run proxied up, keyed by approval-request id;
/// an [`InboxKind::ApprovalReply`] fulfils the matching one.
type ApprovalWaiters = Arc<std::sync::Mutex<HashMap<String, tokio::sync::oneshot::Sender<bool>>>>;

fn approval_waiters_lock(
    waiters: &ApprovalWaiters,
) -> std::sync::MutexGuard<'_, HashMap<String, tokio::sync::oneshot::Sender<bool>>> {
    waiters
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Synchronous drop cleanup is important here: if the approval task itself is
/// aborted while awaiting a decision, its future never gets another async poll
/// in which to remove the registered sender.
struct ApprovalWaiterRegistration {
    waiters: ApprovalWaiters,
    approval_id: String,
}

impl ApprovalWaiterRegistration {
    fn new(waiters: &ApprovalWaiters, approval_id: &str) -> Self {
        Self {
            waiters: Arc::clone(waiters),
            approval_id: approval_id.to_string(),
        }
    }
}

impl Drop for ApprovalWaiterRegistration {
    fn drop(&mut self) {
        approval_waiters_lock(&self.waiters).remove(&self.approval_id);
    }
}

async fn deliver_approval_request(
    deliver: &mut BrokerClient,
    parent: &str,
    message: InboxMessage,
    waiters: &ApprovalWaiters,
    approval_id: &str,
    owner_cancel: &CancellationToken,
) -> bool {
    let result = tokio::select! {
        biased;
        _ = owner_cancel.cancelled() => None,
        result = deliver.deliver(parent, message) => Some(result),
    };
    match result {
        Some(Ok(_)) => true,
        Some(Err(error)) => {
            tracing::warn!(approval_id, %error, "approval request delivery failed; denying fail-closed");
            approval_waiters_lock(waiters).remove(approval_id);
            false
        }
        None => {
            approval_waiters_lock(waiters).remove(approval_id);
            false
        }
    }
}

/// Wait for one approval decision without retaining the sender forever. Every
/// terminal path removes the map entry: normal reply may have removed it first,
/// while timeout, owner cancellation, and sender loss clean it up here.
async fn await_approval_decision(
    waiters: &ApprovalWaiters,
    approval_id: &str,
    receiver: tokio::sync::oneshot::Receiver<bool>,
    owner_cancel: &CancellationToken,
    timeout: Duration,
) -> bool {
    let _registration = ApprovalWaiterRegistration::new(waiters, approval_id);
    let approved = tokio::select! {
        biased;
        _ = owner_cancel.cancelled() => {
            tracing::warn!(approval_id, "approval denied because the owner connection was lost");
            false
        }
        decision = receiver => decision.unwrap_or(false),
        _ = tokio::time::sleep(timeout) => {
            tracing::warn!(
                approval_id,
                approval_timeout_ms = timeout.as_millis() as u64,
                "approval timed out; denying fail-closed"
            );
            false
        }
    };
    approved
}

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
    approval_timeout: Duration,
) -> Handled
where
    E: bamboo_subagent::ChildExecutor + ?Sized,
{
    use bamboo_subagent::{EventSink, ExecutorControl, HostBridge, RunSpec, SteerInbox};

    let spec: RunSpec = match serde_json::from_value(msg.body) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("run {:?}: malformed RunSpec, dropping: {e}", msg.id);
            return Handled::Ack;
        }
    };
    let run_id = msg.id.clone();
    let parent = msg.from.session_id.clone();

    let (sink, mut events, mut controls) = EventSink::channel_with_control();
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
    let forward_cancel = cancel.clone();
    let approval_cancel = cancel.clone();

    // Forward task: own one deliver connection; stream Events live, then the
    // Outcome once the run finishes (sink dropped ⇒ `events` closes).
    let run_id_fwd = run_id.clone();
    let me_fwd = me.clone();
    let parent_fwd = parent.clone();
    let (ep_fwd, tok_fwd) = (endpoint.clone(), token.clone());
    let tls_fwd = tls_config.clone();
    let mut forward = AbortOnDropTask::new(tokio::spawn(async move {
        let connect = BrokerClient::connect_with_tls(
            &ep_fwd,
            me_fwd.clone(),
            &tok_fwd,
            tls_fwd.as_deref().cloned(),
        );
        let mut deliver = match tokio::select! {
            biased;
            _ = forward_cancel.cancelled() => return,
            result = connect => result,
        } {
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
        let mut events_open = true;
        let mut controls_open = true;
        while events_open || controls_open {
            tokio::select! {
                biased;
                _ = forward_cancel.cancelled() => return,
                event = events.recv(), if events_open => match event {
                    Some(event) => {
                        if deliver
                            .deliver(&parent_fwd, emit(InboxKind::Event, event))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    None => events_open = false,
                },
                control = controls.recv(), if controls_open => match control {
                    Some(ExecutorControl::SessionMessageAdmitted(confirmation)) => {
                        let body = serde_json::to_value(confirmation)
                            .unwrap_or_else(|_| serde_json::json!({}));
                        if deliver
                            .deliver(
                                &parent_fwd,
                                emit(InboxKind::SessionMessageAdmitted, body),
                            )
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    None => controls_open = false,
                },
            }
        }
        if let Ok(outcome) = outcome_rx.await {
            let body = serde_json::to_value(&outcome).unwrap_or_else(|_| serde_json::json!({}));
            let _ = deliver
                .deliver(&parent_fwd, emit(InboxKind::Outcome, body))
                .await;
        }
    }));

    // Approval drain: each gated-tool approval the executor raises is delivered to
    // the parent as an ApprovalRequest (correlated to the run); the matching
    // ApprovalReply wakes the registered waiter, whose decision answers the tool.
    // Ends when the run drops the sink ⇒ the host bridge ⇒ `host_rx` closes.
    let waiters_drain = Arc::clone(waiters);
    let run_id_appr = run_id.clone();
    let mut approval = AbortOnDropTask::new(tokio::spawn(async move {
        let connect = BrokerClient::connect_with_tls(
            &endpoint,
            me.clone(),
            &token,
            tls_config.as_deref().cloned(),
        );
        let mut deliver = match tokio::select! {
            biased;
            _ = approval_cancel.cancelled() => return,
            result = connect => result,
        } {
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
        loop {
            let req = tokio::select! {
                biased;
                _ = approval_cancel.cancelled() => break,
                req = host_rx.recv() => match req {
                    Some(req) => req,
                    None => break,
                },
            };
            let approval_id = MsgId::new();
            let approval_id_str = format!("{approval_id:?}");
            let (atx, arx) = tokio::sync::oneshot::channel::<bool>();
            approval_waiters_lock(&waiters_drain).insert(approval_id_str.clone(), atx);
            // Own the registration before the first fallible/awaiting delivery
            // operation. If this whole approval task is aborted while delivery
            // is waiting for its broker receipt, Drop still removes the sender.
            let _registration = ApprovalWaiterRegistration::new(&waiters_drain, &approval_id_str);
            let m = InboxMessage {
                id: MsgId::new(),
                from: me.clone(),
                kind: InboxKind::ApprovalRequest,
                body: serde_json::json!({ "id": approval_id_str, "request": req.body }),
                created_at: Utc::now(),
                correlation_id: Some(run_id_appr.clone()),
            };
            let delivered = deliver_approval_request(
                &mut deliver,
                &parent,
                m,
                &waiters_drain,
                &approval_id_str,
                &approval_cancel,
            )
            .await;
            if !delivered {
                let _ = req.reply.send(serde_json::json!({ "approved": false }));
                continue;
            }
            let approved = await_approval_decision(
                &waiters_drain,
                &approval_id_str,
                arx,
                &approval_cancel,
                approval_timeout,
            )
            .await;
            let _ = req.reply.send(serde_json::json!({ "approved": approved }));
        }
    }));

    // Run to completion (events stream into `sink`); dropping `sink` closes the
    // forward loop's `events` (→ outcome) and the approval drain's `host_rx`.
    let outcome = executor.run(spec, sink, steer_inbox, cancel).await;
    coords.lock().await.remove(&run_id);
    let _ = outcome_tx.send(outcome);
    let _ = forward.join().await;
    let _ = approval.join().await;
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
                logical_session: None,
                project_id: None,
                reasoning_effort: None,
                permission_policy: None,
                messages: prior,
                activation_run_id: None,
                initial_session_messages: Vec::new(),
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

fn decode_steer_body(
    body: &serde_json::Value,
    durable_message_id: Option<&str>,
) -> Option<bamboo_subagent::SteerMessage> {
    // Any typed-protocol marker makes this a typed frame. A partial/malformed
    // typed frame must fail closed; it may never fall through to a coincidental
    // `text` field and become an uncorrelated legacy steer.
    let typed = [
        "target_session_id",
        "envelope",
        "canonical_claim_generation",
        "activation_run_id",
    ]
    .iter()
    .any(|key| body.get(key).is_some());
    if typed {
        return match serde_json::from_value(body.clone()) {
            Ok(delivery) => Some(bamboo_subagent::SteerMessage::SessionMessage(Box::new(
                delivery,
            ))),
            Err(error) => {
                tracing::warn!(
                    %error,
                    "dropping malformed typed SessionInbox steer frame"
                );
                None
            }
        };
    }

    match body.get("text").and_then(|value| value.as_str()) {
        Some(text) => {
            tracing::info!(
                telemetry_event = "session_inbox.legacy_broker_steer_ingress",
                "observed legacy broker steer ingress"
            );
            Some(match durable_message_id {
                Some(message_id) => bamboo_subagent::SteerMessage::DurableText {
                    message_id: message_id.to_string(),
                    text: text.to_string(),
                },
                None => bamboo_subagent::SteerMessage::Text(text.to_string()),
            })
        }
        None => {
            tracing::warn!("dropping malformed legacy steer frame");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{BrokerCore, PushItem};
    use crate::proto::{BrokerFrame, ClientFrame};
    use crate::server::BrokerServer;
    use bamboo_subagent::{AskBody, AskMode};
    use futures_util::{SinkExt, StreamExt};
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio_tungstenite::tungstenite::Message as WsMessage;

    const TOKEN: &str = "t";

    struct DropFlag(Arc<std::sync::atomic::AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

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

    async fn start_single_connection() -> (
        String,
        tempfile::TempDir,
        Arc<BrokerCore>,
        tokio::task::JoinHandle<()>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let core = Arc::new(BrokerCore::new(dir.path()));
        let server = Arc::new(BrokerServer::new(core.clone(), TOKEN));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let connection = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("worker connection");
            let _ = server.handle_conn(stream).await;
        });
        (format!("ws://{addr}"), dir, core, connection)
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

    fn duplicate_ask_pair(from: &str, question: &str) -> (InboxMessage, InboxMessage) {
        let first = ask(from, question);
        let mut second = first.clone();
        second.created_at = first.created_at + chrono::Duration::microseconds(1);
        (first, second)
    }

    async fn mailbox_pending_files(dir: &tempfile::TempDir, session_id: &str) -> usize {
        let mut total = 0;
        for lane in ["new", "cur"] {
            let path = dir.path().join("mailboxes").join(session_id).join(lane);
            let Ok(mut entries) = tokio::fs::read_dir(path).await else {
                continue;
            };
            while let Ok(Some(entry)) = entries.next_entry().await {
                if entry.file_name().to_string_lossy().ends_with(".json") {
                    total += 1;
                }
            }
        }
        total
    }

    async fn wait_for_empty_mailbox(dir: &tempfile::TempDir, session_id: &str) {
        wait_for_mailbox_count(dir, session_id, 0).await;
    }

    async fn wait_for_mailbox_count(dir: &tempfile::TempDir, session_id: &str, expected: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while mailbox_pending_files(dir, session_id).await != expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("mailbox reaches {expected} pending file(s)"));
    }

    #[test]
    fn malformed_typed_steer_never_falls_back_to_legacy_text() {
        assert!(decode_steer_body(
            &serde_json::json!({
                "target_session_id": "logical-session",
                "text": "must not be injected as legacy"
            }),
            Some("broker-id"),
        )
        .is_none());
        assert!(decode_steer_body(
            &serde_json::json!({
                "envelope": {},
                "text": "must not be injected as legacy"
            }),
            Some("broker-id"),
        )
        .is_none());
        assert!(matches!(
            decode_steer_body(
                &serde_json::json!({"text": "legacy-compatible"}),
                Some("broker-id"),
            ),
            Some(bamboo_subagent::SteerMessage::DurableText { message_id, text })
                if message_id == "broker-id" && text == "legacy-compatible"
        ));
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
            logical_session: None,
            project_id: None,
            reasoning_effort: None,
            permission_policy: None,
            messages: vec![],
            activation_run_id: None,
            initial_session_messages: Vec::new(),
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

    #[tokio::test]
    async fn lifecycle_reports_idle_timeout_for_unused_worker() {
        let (endpoint, _dir) = start().await;
        let reason = tokio::time::timeout(
            Duration::from_secs(2),
            serve_executor_with_lifecycle(
                &endpoint,
                AgentRef {
                    session_id: "idle-timeout-worker".into(),
                    role: None,
                },
                TOKEN,
                Arc::new(bamboo_subagent::EchoExecutor),
                CancellationToken::new(),
                Some(Duration::from_millis(100)),
            ),
        )
        .await
        .expect("idle worker exits within the bound")
        .expect("clean lifecycle exit");
        assert_eq!(reason, ServeExitReason::IdleTimeout);
    }

    #[tokio::test]
    async fn true_idle_timeout_never_fires_while_a_run_is_in_flight() {
        use bamboo_subagent::{ChildExecutor, ChildOutcome, EventSink, RunSpec, SteerInbox};

        struct BlockingEcho {
            started: Arc<tokio::sync::Notify>,
            release: Arc<tokio::sync::Notify>,
        }

        #[async_trait::async_trait]
        impl ChildExecutor for BlockingEcho {
            async fn run(
                &self,
                spec: RunSpec,
                _events: EventSink,
                _steer: SteerInbox,
                _cancel: CancellationToken,
            ) -> ChildOutcome {
                self.started.notify_one();
                self.release.notified().await;
                ChildOutcome::completed(format!("echo: {}", spec.assignment))
            }
        }

        let (endpoint, _dir) = start().await;
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let worker_endpoint = endpoint.clone();
        let worker = tokio::spawn({
            let started = started.clone();
            let release = release.clone();
            async move {
                serve_executor_with_lifecycle(
                    &worker_endpoint,
                    AgentRef {
                        session_id: "busy-worker".into(),
                        role: None,
                    },
                    TOKEN,
                    Arc::new(BlockingEcho { started, release }),
                    CancellationToken::new(),
                    Some(Duration::from_millis(100)),
                )
                .await
            }
        });
        let mut parent = BrokerClient::connect(
            &endpoint,
            AgentRef {
                session_id: "busy-parent".into(),
                role: None,
            },
            TOKEN,
        )
        .await
        .unwrap();
        parent.subscribe().await.unwrap();
        let request = ask("busy-parent", "held open");
        let request_id = request.id.clone();
        parent.deliver("busy-worker", request).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), started.notified())
            .await
            .expect("run starts");

        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(
            !worker.is_finished(),
            "true-idle must be disabled while a handler is in flight"
        );
        release.notify_one();
        let reply = tokio::time::timeout(Duration::from_secs(2), parent.next_message())
            .await
            .expect("reply arrives")
            .expect("reply present");
        assert_eq!(reply.correlation_id, Some(request_id));

        let reason = tokio::time::timeout(Duration::from_secs(2), worker)
            .await
            .expect("worker exits after becoming idle")
            .expect("worker task")
            .expect("clean lifecycle exit");
        assert_eq!(reason, ServeExitReason::IdleTimeout);
    }

    #[tokio::test]
    async fn lifecycle_reports_connection_closed_separately_from_idle() {
        let (endpoint, _dir, core, connection) = start_single_connection().await;
        let worker_endpoint = endpoint.clone();
        let worker = tokio::spawn(async move {
            serve_executor_with_lifecycle(
                &worker_endpoint,
                AgentRef {
                    session_id: "disconnect-worker".into(),
                    role: Some("disconnect-test".into()),
                },
                TOKEN,
                Arc::new(bamboo_subagent::EchoExecutor),
                CancellationToken::new(),
                Some(Duration::from_secs(30)),
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if core
                    .connected_by_role("disconnect-test")
                    .await
                    .iter()
                    .any(|id| id == "disconnect-worker")
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("worker subscribes");

        connection.abort();
        let reason = tokio::time::timeout(Duration::from_secs(2), worker)
            .await
            .expect("worker observes connection loss")
            .expect("worker task")
            .expect("clean connection-close lifecycle exit");
        assert_eq!(reason, ServeExitReason::ConnectionClosed);
    }

    #[tokio::test]
    async fn graceful_shutdown_does_not_cancel_but_subsequent_connection_loss_does() {
        let (endpoint, _dir, core, connection) = start_single_connection().await;
        let request = ask("gone-parent", "wait for cancellation");
        core.deliver("cancel-worker", &request).await.unwrap();

        let me = AgentRef {
            session_id: "cancel-worker".into(),
            role: Some("disconnect-cancel-test".into()),
        };
        let mut client = BrokerClient::connect(&endpoint, me.clone(), TOKEN)
            .await
            .unwrap();
        client.subscribe().await.unwrap();
        let started = Arc::new(tokio::sync::Notify::new());
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let shutdown = CancellationToken::new();
        let worker = tokio::spawn({
            let started = started.clone();
            let cancelled = cancelled.clone();
            let worker_shutdown = shutdown.clone();
            async move {
                serve_loop_with_timeouts(
                    &mut client,
                    &me,
                    move |_msg, cancel| {
                        let started = started.clone();
                        let cancelled = cancelled.clone();
                        async move {
                            started.notify_one();
                            cancel.cancelled().await;
                            cancelled.store(true, std::sync::atomic::Ordering::SeqCst);
                            Handled::Leave
                        }
                    },
                    worker_shutdown,
                    None,
                    Duration::from_millis(500),
                    Duration::from_millis(500),
                )
                .await
            }
        });
        tokio::time::timeout(Duration::from_secs(2), started.notified())
            .await
            .expect("handler starts");

        shutdown.cancel();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(
            !cancelled.load(std::sync::atomic::Ordering::SeqCst),
            "explicit graceful shutdown must leave admitted work uncancelled"
        );
        assert!(!worker.is_finished(), "graceful drain still awaits handler");

        connection.abort();
        let _ = connection.await;
        let result = tokio::time::timeout(Duration::from_secs(2), worker)
            .await
            .expect("connection-loss drain is bounded")
            .expect("serve task does not panic")
            .expect("cooperative handler drains cleanly");
        assert_eq!(result, ServeExitReason::ConnectionClosed);
        assert!(cancelled.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn connection_loss_aborts_and_joins_handler_that_ignores_cancellation() {
        let (endpoint, _dir, core, connection) = start_single_connection().await;
        let request = ask("gone-parent", "ignore cancellation");
        let request_id = request.id.clone();
        core.deliver("stuck-worker", &request).await.unwrap();

        let me = AgentRef {
            session_id: "stuck-worker".into(),
            role: Some("disconnect-stuck-test".into()),
        };
        let mut client = BrokerClient::connect(&endpoint, me.clone(), TOKEN)
            .await
            .unwrap();
        client.subscribe().await.unwrap();
        let started = Arc::new(tokio::sync::Notify::new());
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worker = tokio::spawn({
            let started = started.clone();
            let dropped = dropped.clone();
            async move {
                serve_loop_with_timeouts(
                    &mut client,
                    &me,
                    move |_msg, _cancel| {
                        let started = started.clone();
                        let dropped = dropped.clone();
                        async move {
                            let _drop_flag = DropFlag(dropped);
                            started.notify_one();
                            std::future::pending::<Handled>().await
                        }
                    },
                    CancellationToken::new(),
                    None,
                    Duration::from_millis(50),
                    Duration::from_millis(500),
                )
                .await
            }
        });
        tokio::time::timeout(Duration::from_secs(2), started.notified())
            .await
            .expect("handler starts");

        connection.abort();
        let _ = connection.await;
        let error = tokio::time::timeout(Duration::from_secs(2), worker)
            .await
            .expect("stuck handler is bounded")
            .expect("serve task does not panic")
            .expect_err("stuck drain returns non-success");
        match error {
            BrokerError::ConnectionDrainTimeout {
                timeout_ms,
                stuck_ids,
                ..
            } => {
                assert_eq!(timeout_ms, 50);
                assert_eq!(stuck_ids, vec![request_id.as_str().to_string()]);
            }
            other => panic!("unexpected error: {other}"),
        }
        assert!(
            dropped.load(std::sync::atomic::Ordering::SeqCst),
            "timed-out handler must be aborted and joined before return"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn synchronous_non_yielding_handler_cannot_unbound_abort_join() {
        let (endpoint, _dir, core, connection) = start_single_connection().await;
        let request = ask("gone-parent", "block a runtime thread");
        let request_id = request.id.clone();
        core.deliver("sync-block-worker", &request).await.unwrap();

        let me = AgentRef {
            session_id: "sync-block-worker".into(),
            role: None,
        };
        let mut client = BrokerClient::connect(&endpoint, me.clone(), TOKEN)
            .await
            .unwrap();
        client.subscribe().await.unwrap();
        let started = Arc::new(tokio::sync::Notify::new());
        let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worker = tokio::spawn({
            let started = started.clone();
            let finished = finished.clone();
            async move {
                serve_loop_with_timeouts(
                    &mut client,
                    &me,
                    move |_msg, _cancel| {
                        let started = started.clone();
                        let finished = finished.clone();
                        async move {
                            started.notify_one();
                            std::thread::sleep(Duration::from_millis(300));
                            finished.store(true, std::sync::atomic::Ordering::SeqCst);
                            Handled::Leave
                        }
                    },
                    CancellationToken::new(),
                    None,
                    Duration::from_millis(20),
                    Duration::from_millis(20),
                )
                .await
            }
        });
        tokio::time::timeout(Duration::from_secs(2), started.notified())
            .await
            .expect("synchronous handler starts");

        let disconnect_started = tokio::time::Instant::now();
        connection.abort();
        let _ = connection.await;
        let error = tokio::time::timeout(Duration::from_millis(180), worker)
            .await
            .expect("serve returns before synchronous work yields")
            .expect("serve task")
            .expect_err("stuck synchronous handler is non-successful");
        assert!(
            disconnect_started.elapsed() < Duration::from_millis(180),
            "disconnect + abort join must remain bounded"
        );
        match error {
            BrokerError::ConnectionDrainTimeout {
                timeout_ms,
                stuck_ids,
                abort_join_timeout_ms,
                abort_join_timed_out,
            } => {
                assert_eq!(timeout_ms, 20);
                assert_eq!(stuck_ids, vec![request_id.as_str().to_string()]);
                assert_eq!(abort_join_timeout_ms, 20);
                assert!(abort_join_timed_out);
            }
            other => panic!("unexpected error: {other}"),
        }
        assert!(
            !finished.load(std::sync::atomic::Ordering::SeqCst),
            "library returned while the non-yielding task was still running"
        );

        // Let the synthetic blocker leave its synchronous section so the test
        // runtime itself can shut down; a real subagent-worker process exits(1).
        tokio::time::timeout(Duration::from_secs(1), async {
            while !finished.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("synthetic synchronous blocker eventually returns");
    }

    #[tokio::test]
    async fn duplicate_id_runs_once_then_acks_active_and_late_durable_copies() {
        let (endpoint, dir, core, connection) = start_single_connection().await;
        let (first, second) = duplicate_ask_pair("duplicate-parent", "dedupe me");
        core.deliver("dedupe-worker", &first).await.unwrap();
        core.deliver("dedupe-worker", &second).await.unwrap();

        let me = AgentRef {
            session_id: "dedupe-worker".into(),
            role: None,
        };
        let mut client = BrokerClient::connect(&endpoint, me.clone(), TOKEN)
            .await
            .unwrap();
        client.subscribe().await.unwrap();
        let invocations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let shutdown = CancellationToken::new();
        let worker = tokio::spawn({
            let invocations = invocations.clone();
            let started = started.clone();
            let release = release.clone();
            let worker_shutdown = shutdown.clone();
            async move {
                serve_loop_with_timeouts(
                    &mut client,
                    &me,
                    move |_msg, _cancel| {
                        let invocations = invocations.clone();
                        let started = started.clone();
                        let release = release.clone();
                        async move {
                            invocations.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            started.notify_one();
                            release.notified().await;
                            Handled::Ack
                        }
                    },
                    worker_shutdown,
                    None,
                    Duration::from_millis(500),
                    Duration::from_millis(500),
                )
                .await
            }
        });
        tokio::time::timeout(Duration::from_secs(2), started.notified())
            .await
            .expect("first duplicate starts");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            invocations.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "an active duplicate must coalesce into the original admission"
        );
        release.notify_waiters();
        wait_for_empty_mailbox(&dir, "dedupe-worker").await;

        let mut late = second;
        late.created_at += chrono::Duration::microseconds(1);
        core.deliver("dedupe-worker", &late).await.unwrap();
        wait_for_empty_mailbox(&dir, "dedupe-worker").await;
        assert_eq!(
            invocations.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "a late duplicate after successful completion must only be acked"
        );

        shutdown.cancel();
        let reason = tokio::time::timeout(Duration::from_secs(2), worker)
            .await
            .expect("dedupe worker exits")
            .expect("serve task")
            .expect("clean shutdown");
        assert_eq!(reason, ServeExitReason::ShutdownRequested);
        let _ = tokio::time::timeout(Duration::from_secs(1), connection).await;
    }

    #[tokio::test]
    async fn duplicate_id_stays_unacked_and_has_no_detached_task_on_disconnect() {
        let (endpoint, _dir, core, connection) = start_single_connection().await;
        let (first, second) = duplicate_ask_pair("duplicate-parent", "disconnect me");
        let duplicate_id = first.id.clone();
        core.deliver("duplicate-disconnect-worker", &first)
            .await
            .unwrap();
        core.deliver("duplicate-disconnect-worker", &second)
            .await
            .unwrap();

        let me = AgentRef {
            session_id: "duplicate-disconnect-worker".into(),
            role: None,
        };
        let mut client = BrokerClient::connect(&endpoint, me.clone(), TOKEN)
            .await
            .unwrap();
        client.subscribe().await.unwrap();
        let invocations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worker = tokio::spawn({
            let invocations = invocations.clone();
            let started = started.clone();
            let dropped = dropped.clone();
            async move {
                serve_loop_with_timeouts(
                    &mut client,
                    &me,
                    move |_msg, cancel| {
                        let invocations = invocations.clone();
                        let started = started.clone();
                        let dropped = dropped.clone();
                        async move {
                            let _drop_flag = DropFlag(dropped);
                            invocations.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            started.notify_one();
                            cancel.cancelled().await;
                            Handled::Leave
                        }
                    },
                    CancellationToken::new(),
                    None,
                    Duration::from_millis(500),
                    Duration::from_millis(500),
                )
                .await
            }
        });
        tokio::time::timeout(Duration::from_secs(2), started.notified())
            .await
            .expect("duplicate admission starts");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(invocations.load(std::sync::atomic::Ordering::SeqCst), 1);

        connection.abort();
        let _ = connection.await;
        let reason = tokio::time::timeout(Duration::from_secs(2), worker)
            .await
            .expect("disconnect drain is bounded")
            .expect("serve task")
            .expect("cooperative cancellation drains");
        assert_eq!(reason, ServeExitReason::ConnectionClosed);
        assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));

        core.unsubscribe("duplicate-disconnect-worker").await;
        let (mut replay, _lease) = core
            .subscribe_with_lease("duplicate-disconnect-worker", None)
            .await
            .unwrap();
        for _ in 0..2 {
            let PushItem::Message(message) = replay.try_recv().expect("duplicate remains durable")
            else {
                panic!("expected durable duplicate message");
            };
            assert_eq!(message.id, duplicate_id);
        }
    }

    #[tokio::test]
    async fn late_duplicate_ack_failure_cancels_and_joins_other_inflight_handlers() {
        let (endpoint, dir, core, connection) = start_single_connection().await;
        let original = ask("duplicate-parent", "complete first");
        let blocker = ask("duplicate-parent", "wait for cancellation");
        core.deliver("duplicate-ack-failure-worker", &original)
            .await
            .unwrap();
        core.deliver("duplicate-ack-failure-worker", &blocker)
            .await
            .unwrap();

        let me = AgentRef {
            session_id: "duplicate-ack-failure-worker".into(),
            role: None,
        };
        let mut client = BrokerClient::connect(&endpoint, me.clone(), TOKEN)
            .await
            .unwrap();
        client.subscribe().await.unwrap();
        let fail_next_ack = client.fail_next_ack_handle();
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
        let release = Arc::new(tokio::sync::Notify::new());
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let invocations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let worker = tokio::spawn({
            let release = release.clone();
            let cancelled = cancelled.clone();
            let invocations = invocations.clone();
            async move {
                serve_loop_with_timeouts(
                    &mut client,
                    &me,
                    move |msg, cancel| {
                        let started_tx = started_tx.clone();
                        let release = release.clone();
                        let cancelled = cancelled.clone();
                        let invocations = invocations.clone();
                        async move {
                            let body: AskBody = serde_json::from_value(msg.body).unwrap();
                            started_tx.send(body.question.clone()).unwrap();
                            if body.question == "complete first" {
                                invocations.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                release.notified().await;
                                Handled::Ack
                            } else {
                                cancel.cancelled().await;
                                cancelled.store(true, std::sync::atomic::Ordering::SeqCst);
                                Handled::Leave
                            }
                        }
                    },
                    CancellationToken::new(),
                    None,
                    Duration::from_millis(500),
                    Duration::from_millis(500),
                )
                .await
            }
        });

        let mut started = std::collections::HashSet::new();
        for _ in 0..2 {
            started.insert(
                tokio::time::timeout(Duration::from_secs(2), started_rx.recv())
                    .await
                    .expect("both handlers start")
                    .expect("start signal"),
            );
        }
        assert!(started.contains("complete first"));
        assert!(started.contains("wait for cancellation"));
        release.notify_one();
        wait_for_mailbox_count(&dir, "duplicate-ack-failure-worker", 1).await;

        fail_next_ack.store(true, std::sync::atomic::Ordering::SeqCst);
        let mut late = original;
        late.created_at += chrono::Duration::microseconds(1);
        core.deliver("duplicate-ack-failure-worker", &late)
            .await
            .unwrap();

        let error = tokio::time::timeout(Duration::from_secs(2), worker)
            .await
            .expect("duplicate ack failure teardown is bounded")
            .expect("serve task does not panic")
            .expect_err("duplicate ack failure remains non-successful");
        assert!(
            matches!(error, BrokerError::Transport(ref message) if message == "injected broker ack failure"),
            "{error}"
        );
        assert!(cancelled.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(
            invocations.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the late duplicate must never execute after its original completed"
        );
        assert_eq!(
            mailbox_pending_files(&dir, "duplicate-ack-failure-worker").await,
            2,
            "failed duplicate ack and cancelled work must remain durable"
        );
        let _ = tokio::time::timeout(Duration::from_secs(1), connection).await;
    }

    #[tokio::test]
    async fn leave_does_not_permanently_dedupe_same_id() {
        let (endpoint, _dir, core, connection) = start_single_connection().await;
        let (first, second) = duplicate_ask_pair("leave-parent", "leave twice");
        core.deliver("leave-worker", &first).await.unwrap();

        let me = AgentRef {
            session_id: "leave-worker".into(),
            role: None,
        };
        let mut client = BrokerClient::connect(&endpoint, me.clone(), TOKEN)
            .await
            .unwrap();
        client.subscribe().await.unwrap();
        let (invoked_tx, mut invoked_rx) = tokio::sync::mpsc::unbounded_channel();
        let shutdown = CancellationToken::new();
        let worker = tokio::spawn({
            let worker_shutdown = shutdown.clone();
            async move {
                serve_loop_with_timeouts(
                    &mut client,
                    &me,
                    move |_msg, _cancel| {
                        let invoked_tx = invoked_tx.clone();
                        async move {
                            invoked_tx.send(()).unwrap();
                            Handled::Leave
                        }
                    },
                    worker_shutdown,
                    None,
                    Duration::from_millis(500),
                    Duration::from_millis(500),
                )
                .await
            }
        });
        tokio::time::timeout(Duration::from_secs(2), invoked_rx.recv())
            .await
            .expect("first Leave runs")
            .expect("first invocation");
        tokio::time::sleep(Duration::from_millis(20)).await;

        core.deliver("leave-worker", &second).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), invoked_rx.recv())
            .await
            .expect("same id runs again after Leave")
            .expect("second invocation");

        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(2), worker)
            .await
            .expect("Leave worker exits")
            .expect("serve task")
            .expect("clean shutdown");
        let _ = tokio::time::timeout(Duration::from_secs(1), connection).await;
    }

    #[tokio::test]
    async fn completion_wire_failure_cancels_and_joins_other_inflight_handlers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let complete = ask("gone-parent", "complete first");
        let waiting = ask("gone-parent", "wait for cancellation");
        let fake_broker = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("worker connection");
            let mut ws = tokio_tungstenite::accept_async(stream)
                .await
                .expect("websocket upgrade");
            let hello = ws.next().await.expect("hello frame").expect("hello read");
            assert!(matches!(
                ClientFrame::from_text(hello.to_text().unwrap()).unwrap(),
                ClientFrame::Hello { .. }
            ));
            ws.send(WsMessage::text(BrokerFrame::Welcome.to_text()))
                .await
                .unwrap();
            let subscribe = ws
                .next()
                .await
                .expect("subscribe frame")
                .expect("subscribe read");
            assert_eq!(
                ClientFrame::from_text(subscribe.to_text().unwrap()).unwrap(),
                ClientFrame::Subscribe
            );
            ws.send(WsMessage::text(
                BrokerFrame::Message { message: complete }.to_text(),
            ))
            .await
            .unwrap();
            ws.send(WsMessage::text(
                BrokerFrame::Message { message: waiting }.to_text(),
            ))
            .await
            .unwrap();

            loop {
                let frame = ws
                    .next()
                    .await
                    .expect("completion writes a reply")
                    .expect("completion frame");
                let frame = ClientFrame::from_text(frame.to_text().unwrap()).unwrap();
                if matches!(frame, ClientFrame::Deliver { .. }) {
                    // Drop the connection without a delivery receipt. The
                    // completion arm is already awaiting this exact receipt,
                    // so its wire-error path deterministically wins before the
                    // reader-close event can drive the outer select.
                    break;
                }
            }
        });

        let me = AgentRef {
            session_id: "wire-failure-worker".into(),
            role: None,
        };
        let mut client = BrokerClient::connect(&endpoint, me.clone(), TOKEN)
            .await
            .unwrap();
        client.subscribe().await.unwrap();
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
        let release = Arc::new(tokio::sync::Notify::new());
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worker = tokio::spawn({
            let release = release.clone();
            let cancelled = cancelled.clone();
            async move {
                serve_loop_with_timeouts(
                    &mut client,
                    &me,
                    move |msg, cancel| {
                        let started_tx = started_tx.clone();
                        let release = release.clone();
                        let cancelled = cancelled.clone();
                        async move {
                            let body: AskBody = serde_json::from_value(msg.body).unwrap();
                            started_tx.send(()).unwrap();
                            if body.question == "complete first" {
                                release.notified().await;
                                Handled::Reply("done".into())
                            } else {
                                cancel.cancelled().await;
                                cancelled.store(true, std::sync::atomic::Ordering::SeqCst);
                                Handled::Leave
                            }
                        }
                    },
                    CancellationToken::new(),
                    None,
                    Duration::from_millis(500),
                    Duration::from_millis(500),
                )
                .await
            }
        });
        for _ in 0..2 {
            tokio::time::timeout(Duration::from_secs(2), started_rx.recv())
                .await
                .expect("both handlers start")
                .expect("start signal");
        }
        release.notify_one();

        let error = tokio::time::timeout(Duration::from_secs(2), worker)
            .await
            .expect("wire failure teardown is bounded")
            .expect("serve task does not panic")
            .expect_err("delivery failure remains non-successful");
        assert!(matches!(error, BrokerError::Transport(_)), "{error}");
        assert!(cancelled.load(std::sync::atomic::Ordering::SeqCst));
        fake_broker.await.unwrap();
    }

    #[tokio::test]
    async fn approval_timeout_and_owner_loss_fail_closed_without_waiter_leaks() {
        let waiters: ApprovalWaiters = Arc::new(std::sync::Mutex::new(HashMap::new()));

        let (timeout_tx, timeout_rx) = tokio::sync::oneshot::channel();
        approval_waiters_lock(&waiters).insert("timeout".to_string(), timeout_tx);
        assert!(
            !await_approval_decision(
                &waiters,
                "timeout",
                timeout_rx,
                &CancellationToken::new(),
                Duration::from_millis(20),
            )
            .await
        );
        assert!(approval_waiters_lock(&waiters).is_empty());

        let owner_cancel = CancellationToken::new();
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
        approval_waiters_lock(&waiters).insert("owner-lost".to_string(), cancel_tx);
        owner_cancel.cancel();
        assert!(
            !await_approval_decision(
                &waiters,
                "owner-lost",
                cancel_rx,
                &owner_cancel,
                Duration::from_secs(30),
            )
            .await
        );
        assert!(approval_waiters_lock(&waiters).is_empty());

        let (abort_tx, abort_rx) = tokio::sync::oneshot::channel();
        approval_waiters_lock(&waiters).insert("task-aborted".to_string(), abort_tx);
        let aborted_wait = tokio::spawn({
            let waiters = Arc::clone(&waiters);
            async move {
                await_approval_decision(
                    &waiters,
                    "task-aborted",
                    abort_rx,
                    &CancellationToken::new(),
                    Duration::from_secs(30),
                )
                .await
            }
        });
        tokio::task::yield_now().await;
        aborted_wait.abort();
        let _ = aborted_wait.await;
        assert!(
            approval_waiters_lock(&waiters).is_empty(),
            "aborting the approval future must synchronously drop its registration"
        );
    }

    #[tokio::test]
    async fn approval_reply_and_sender_loss_both_cleanup_waiters() {
        let waiters: ApprovalWaiters = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let owner_cancel = CancellationToken::new();

        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        approval_waiters_lock(&waiters).insert("reply".to_string(), reply_tx);
        approval_waiters_lock(&waiters)
            .remove("reply")
            .expect("registered reply waiter")
            .send(true)
            .unwrap();
        assert!(
            await_approval_decision(
                &waiters,
                "reply",
                reply_rx,
                &owner_cancel,
                Duration::from_secs(1),
            )
            .await
        );
        assert!(approval_waiters_lock(&waiters).is_empty());

        let (lost_tx, lost_rx) = tokio::sync::oneshot::channel();
        approval_waiters_lock(&waiters).insert("sender-lost".to_string(), lost_tx);
        drop(
            approval_waiters_lock(&waiters)
                .remove("sender-lost")
                .expect("registered sender-loss waiter"),
        );
        assert!(
            !await_approval_decision(
                &waiters,
                "sender-lost",
                lost_rx,
                &owner_cancel,
                Duration::from_secs(1),
            )
            .await
        );
        assert!(approval_waiters_lock(&waiters).is_empty());
    }

    #[tokio::test]
    async fn approval_delivery_rejection_removes_registered_waiter() {
        let dir = tempfile::tempdir().unwrap();
        let core = Arc::new(BrokerCore::new(dir.path()).with_max_pending_per_mailbox(0));
        let server = Arc::new(BrokerServer::new(core, TOKEN));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });

        let me = AgentRef {
            session_id: "approval-worker".into(),
            role: None,
        };
        let mut deliver = BrokerClient::connect(&endpoint, me.clone(), TOKEN)
            .await
            .unwrap();
        let waiters: ApprovalWaiters = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let (waiter_tx, waiter_rx) = tokio::sync::oneshot::channel();
        approval_waiters_lock(&waiters).insert("rejected".to_string(), waiter_tx);
        let message = InboxMessage {
            id: MsgId::new(),
            from: me,
            kind: InboxKind::ApprovalRequest,
            body: serde_json::json!({ "id": "rejected", "request": {} }),
            created_at: Utc::now(),
            correlation_id: None,
        };
        assert!(
            !deliver_approval_request(
                &mut deliver,
                "full-parent",
                message,
                &waiters,
                "rejected",
                &CancellationToken::new(),
            )
            .await
        );
        assert!(approval_waiters_lock(&waiters).is_empty());
        assert!(waiter_rx.await.is_err(), "removed sender must be dropped");
    }

    #[tokio::test]
    async fn abort_during_approval_delivery_wait_removes_registered_waiter() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let (deliver_seen_tx, deliver_seen_rx) = tokio::sync::oneshot::channel();
        let fake_broker = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("approval connection");
            let mut ws = tokio_tungstenite::accept_async(stream)
                .await
                .expect("websocket upgrade");
            let hello = ws.next().await.expect("hello frame").expect("hello read");
            assert!(matches!(
                ClientFrame::from_text(hello.to_text().unwrap()).unwrap(),
                ClientFrame::Hello { .. }
            ));
            ws.send(WsMessage::text(BrokerFrame::Welcome.to_text()))
                .await
                .unwrap();
            loop {
                let frame = ws
                    .next()
                    .await
                    .expect("approval delivery")
                    .expect("approval frame");
                if matches!(
                    ClientFrame::from_text(frame.to_text().unwrap()).unwrap(),
                    ClientFrame::Deliver { .. }
                ) {
                    let _ = deliver_seen_tx.send(());
                    break;
                }
            }
            std::future::pending::<()>().await;
        });

        let me = AgentRef {
            session_id: "approval-abort-worker".into(),
            role: None,
        };
        let deliver = BrokerClient::connect(&endpoint, me.clone(), TOKEN)
            .await
            .unwrap();
        let waiters: ApprovalWaiters = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let (waiter_tx, _waiter_rx) = tokio::sync::oneshot::channel();
        approval_waiters_lock(&waiters).insert("delivery-wait".to_string(), waiter_tx);
        let delivery = tokio::spawn({
            let waiters = Arc::clone(&waiters);
            async move {
                let mut deliver = deliver;
                let _registration = ApprovalWaiterRegistration::new(&waiters, "delivery-wait");
                let message = InboxMessage {
                    id: MsgId::new(),
                    from: me,
                    kind: InboxKind::ApprovalRequest,
                    body: serde_json::json!({ "id": "delivery-wait", "request": {} }),
                    created_at: Utc::now(),
                    correlation_id: None,
                };
                deliver_approval_request(
                    &mut deliver,
                    "silent-parent",
                    message,
                    &waiters,
                    "delivery-wait",
                    &CancellationToken::new(),
                )
                .await
            }
        });
        tokio::time::timeout(Duration::from_secs(1), deliver_seen_rx)
            .await
            .expect("deliver reaches broker")
            .expect("delivery signal");
        assert!(approval_waiters_lock(&waiters).contains_key("delivery-wait"));
        delivery.abort();
        let _ = delivery.await;
        assert!(
            approval_waiters_lock(&waiters).is_empty(),
            "aborting during delivery receipt wait must drop registration"
        );
        fake_broker.abort();
        let _ = fake_broker.await;
    }

    #[tokio::test]
    async fn aborting_parent_during_join_does_not_detach_child() {
        let started = Arc::new(tokio::sync::Notify::new());
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let parent = tokio::spawn({
            let started = started.clone();
            let dropped = dropped.clone();
            async move {
                let mut task = AbortOnDropTask::new(tokio::spawn(async move {
                    let _drop_flag = DropFlag(dropped);
                    started.notify_one();
                    std::future::pending::<()>().await;
                }));
                let _ = task.join().await;
            }
        });
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("child starts");
        parent.abort();
        let _ = parent.await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while !dropped.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropping owner aborts child");
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
