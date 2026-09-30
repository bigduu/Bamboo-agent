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

use bamboo_subagent::{
    ActorEventBatch, ActorEventQos, AdmittedSet, AgentRef, InboxKind, InboxMessage, MsgId,
    ReplyBody,
};
use chrono::Utc;
use tokio_util::sync::CancellationToken;

use crate::client::BrokerClient;
use crate::error::{BrokerError, BrokerResult};
use crate::proto::FencedRunEnvelope;

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

/// One Cluster worker owns a fixed pair of outbound broker connections no
/// matter how many runs it executes concurrently. Durable controls/results and
/// actor events have independent queues and sockets, so a burst of 200 child
/// streams cannot head-of-line block an approval or terminal outcome.
const ACTOR_UPLINK_CONTROL_QUEUE_CAPACITY: usize = 256;
const ACTOR_UPLINK_EVENT_QUEUE_CAPACITY: usize = 1024;

/// What a handler decides to do with one inbound message.
pub enum Handled {
    /// Produced an answer; the loop delivers it as a `Reply` to the sender and
    /// acks the original.
    Reply(String),
    /// Processed with no reply (e.g. a fire-and-forget task); just ack.
    Ack,
    /// Leave the message unacked (it will be redelivered on the next subscribe).
    Leave,
    /// Leave the message unacked and retire this subscription. A critical
    /// actor uplink failed, so this worker cannot accept another Run safely.
    LeaveAndDisconnect,
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
    serve_mailbox_full_with_lifecycle_and_owner_loss(
        endpoint,
        me,
        token,
        handler,
        shutdown,
        tls_config,
        idle_timeout,
        CancellationToken::new(),
        false,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn serve_mailbox_full_with_lifecycle_and_owner_loss<H, Fut>(
    endpoint: &str,
    me: AgentRef,
    token: &str,
    handler: H,
    shutdown: CancellationToken,
    tls_config: Option<Arc<rustls::ClientConfig>>,
    idle_timeout: Option<Duration>,
    owner_loss: CancellationToken,
    environment_lease_v1: bool,
) -> BrokerResult<ServeExitReason>
where
    H: Fn(InboxMessage, CancellationToken) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Handled> + Send + 'static,
{
    let mut client =
        BrokerClient::connect_with_tls(endpoint, me.clone(), token, clone_tls_config(&tls_config))
            .await?;
    if environment_lease_v1 {
        client.subscribe_environment_lease_v1().await?;
    } else {
        client.subscribe().await?;
    }
    serve_loop_with_timeouts_and_owner_loss(
        &mut client,
        &me,
        handler,
        shutdown,
        idle_timeout,
        DEFAULT_CONNECTION_DRAIN_TIMEOUT,
        DEFAULT_ABORT_JOIN_TIMEOUT,
        owner_loss,
    )
    .await
}

/// [`BrokerClient::connect_with_tls`] takes an owned `ClientConfig`, while one
/// executor worker shares the same trust configuration across its inbound
/// subscription and fixed control/event uplinks. Keep it in an `Arc` and clone
/// the cheap rustls configuration for each of those three connections. #48.
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
    task: AbortOnDropTask<()>,
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

    fn abort(&self) {
        if let Some(task) = &self.0 {
            task.abort();
        }
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

enum ActorControlCommand {
    Deliver {
        to: String,
        message: InboxMessage,
        result: tokio::sync::oneshot::Sender<BrokerResult<MsgId>>,
    },
    ObserveSelf {
        mailbox: String,
        role: String,
        result:
            tokio::sync::oneshot::Sender<BrokerResult<Option<crate::proto::WorkerHostObservation>>>,
    },
}

enum ActorEventCommand {
    Durable {
        to: String,
        message: InboxMessage,
        result: tokio::sync::oneshot::Sender<BrokerResult<MsgId>>,
    },
    Live {
        to: String,
        correlation_id: MsgId,
        batch: ActorEventBatch,
    },
}

/// Cloneable per-run handle backed by worker-owned, bounded outbound lanes.
/// The two `BrokerClient`s themselves stay inside their single-owner tasks;
/// callers never contend on a connection mutex.
#[derive(Clone)]
struct ActorBrokerUplink {
    control: tokio::sync::mpsc::Sender<ActorControlCommand>,
    events: tokio::sync::mpsc::Sender<ActorEventCommand>,
    source: AgentRef,
}

/// Dropping the serving worker aborts both connection owners even if a broker
/// receipt is stalled. Normal graceful shutdown first drains every run, so the
/// queues are empty before this guard reaches Drop.
struct ActorBrokerUplinkOwners {
    _control: AbortOnDropTask<()>,
    _events: AbortOnDropTask<()>,
}

impl ActorBrokerUplink {
    async fn connect(
        endpoint: &str,
        me: &AgentRef,
        token: &str,
        tls_config: &Option<Arc<rustls::ClientConfig>>,
    ) -> BrokerResult<(Self, ActorBrokerUplinkOwners)> {
        let control_connect = BrokerClient::connect_with_tls(
            endpoint,
            me.clone(),
            token,
            clone_tls_config(tls_config),
        );
        let event_connect = BrokerClient::connect_with_tls(
            endpoint,
            me.clone(),
            token,
            clone_tls_config(tls_config),
        );
        let (control_client, event_client) = tokio::try_join!(control_connect, event_connect)?;
        let (control_tx, control_rx) =
            tokio::sync::mpsc::channel(ACTOR_UPLINK_CONTROL_QUEUE_CAPACITY);
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(ACTOR_UPLINK_EVENT_QUEUE_CAPACITY);
        let owners = ActorBrokerUplinkOwners {
            _control: AbortOnDropTask::new(tokio::spawn(actor_control_uplink_loop(
                control_client,
                control_rx,
            ))),
            _events: AbortOnDropTask::new(tokio::spawn(actor_event_uplink_loop(
                event_client,
                event_rx,
            ))),
        };
        Ok((
            Self {
                control: control_tx,
                events: event_tx,
                source: me.clone(),
            },
            owners,
        ))
    }

    async fn deliver_control(&self, to: &str, message: InboxMessage) -> BrokerResult<MsgId> {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        self.control
            .send(ActorControlCommand::Deliver {
                to: to.to_string(),
                message,
                result: result_tx,
            })
            .await
            .map_err(|_| BrokerError::Transport("actor control uplink closed".into()))?;
        result_rx
            .await
            .map_err(|_| BrokerError::Transport("actor control uplink closed".into()))?
    }

    /// Check a queued Run again after its execution slot opens. The inbound
    /// connection may have lost ownership while the handler waited in memory.
    async fn fenced_run_is_current(
        &self,
        me: &AgentRef,
        envelope: &FencedRunEnvelope,
    ) -> BrokerResult<bool> {
        let role = me
            .role
            .as_deref()
            .ok_or_else(|| BrokerError::Protocol("FencedRun worker role missing".into()))?;
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        self.control
            .send(ActorControlCommand::ObserveSelf {
                mailbox: me.session_id.clone(),
                role: role.to_owned(),
                result: result_tx,
            })
            .await
            .map_err(|_| BrokerError::Transport("actor control uplink closed".into()))?;
        let observation = result_rx
            .await
            .map_err(|_| BrokerError::Transport("actor control uplink closed".into()))??;
        Ok(observation
            .as_ref()
            .is_some_and(|current| envelope.matches_observation(&me.session_id, current)))
    }

    /// Send an ordered durable boundary on the actor-event connection. Outcome
    /// uses this path so every preceding live batch has reached the broker
    /// before the terminal frame is enqueued, even though ordinary controls use
    /// the independent priority uplink.
    async fn deliver_ordered(&self, to: &str, message: InboxMessage) -> BrokerResult<MsgId> {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        self.events
            .send(ActorEventCommand::Durable {
                to: to.to_string(),
                message,
                result: result_tx,
            })
            .await
            .map_err(|_| BrokerError::Transport("actor event uplink closed".into()))?;
        result_rx
            .await
            .map_err(|_| BrokerError::Transport("actor event uplink closed".into()))?
    }

    /// Durable batches are backpressured and receipt-confirmed. Snapshot and
    /// ephemeral batches never wait: overflow is an intentional drop exposed
    /// by the next sequence gap and repaired from the session snapshot.
    async fn send_event_batch(
        &self,
        to: &str,
        correlation_id: &MsgId,
        batch: ActorEventBatch,
    ) -> bool {
        if batch.validate().is_err() {
            return false;
        }
        if batch.qos != ActorEventQos::Durable {
            let first_seq = batch.first_seq;
            let last_seq = batch.last_seq;
            return match self.events.try_send(ActorEventCommand::Live {
                to: to.to_string(),
                correlation_id: correlation_id.clone(),
                batch,
            }) {
                Ok(()) => true,
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    tracing::debug!(
                        first_seq,
                        last_seq,
                        "dropping actor live batch at saturated worker uplink"
                    );
                    true
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => false,
            };
        }

        let body = match serde_json::to_value(batch) {
            Ok(body) => body,
            Err(_) => return false,
        };
        let message = InboxMessage {
            id: MsgId::new(),
            from: self.source.clone(),
            kind: InboxKind::Event,
            body,
            created_at: Utc::now(),
            correlation_id: Some(correlation_id.clone()),
        };
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        if self
            .events
            .send(ActorEventCommand::Durable {
                to: to.to_string(),
                message,
                result: result_tx,
            })
            .await
            .is_err()
        {
            return false;
        }
        matches!(result_rx.await, Ok(Ok(_)))
    }
}

async fn actor_control_uplink_loop(
    mut client: BrokerClient,
    mut commands: tokio::sync::mpsc::Receiver<ActorControlCommand>,
) {
    while let Some(command) = commands.recv().await {
        let transport_failed = match command {
            ActorControlCommand::Deliver {
                to,
                message,
                result,
            } => {
                let delivery = client.deliver(&to, message).await;
                let failed = delivery.is_err() && !client.reader_alive();
                let _ = result.send(delivery);
                failed
            }
            ActorControlCommand::ObserveSelf {
                mailbox,
                role,
                result,
            } => {
                let observation = client.observe_host(&mailbox, &role).await;
                let failed = observation.is_err() && !client.reader_alive();
                let _ = result.send(observation);
                failed
            }
        };
        if transport_failed {
            break;
        }
    }
}

async fn actor_event_uplink_loop(
    mut client: BrokerClient,
    mut commands: tokio::sync::mpsc::Receiver<ActorEventCommand>,
) {
    while let Some(command) = commands.recv().await {
        match command {
            ActorEventCommand::Durable {
                to,
                message,
                result,
            } => {
                let delivery = client.deliver(&to, message).await;
                let transport_failed = delivery.is_err() && !client.reader_alive();
                let _ = result.send(delivery);
                if transport_failed {
                    break;
                }
            }
            ActorEventCommand::Live {
                to,
                correlation_id,
                batch,
            } => {
                if let Err(error) = client
                    .publish_event_batch(&to, &correlation_id, batch)
                    .await
                {
                    tracing::warn!(%error, "actor live event uplink failed");
                    break;
                }
            }
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
    serve_loop_with_timeouts_and_owner_loss(
        client,
        me,
        handler,
        shutdown,
        idle_timeout,
        connection_drain_timeout,
        abort_join_timeout,
        CancellationToken::new(),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn serve_loop_with_timeouts_and_owner_loss<H, Fut>(
    client: &mut BrokerClient,
    me: &AgentRef,
    handler: H,
    shutdown: CancellationToken,
    idle_timeout: Option<Duration>,
    connection_drain_timeout: Duration,
    abort_join_timeout: Duration,
    owner_loss: CancellationToken,
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
                let Some(mut completed) = inflight.remove(&id) else {
                    // A completion must belong to the one active admission for
                    // this id. Never let a stale/duplicate completion ack a
                    // later generation that happens to reuse the same MsgId.
                    tracing::warn!(message_id = %id.as_str(), "ignoring completion without matching in-flight admission");
                    continue;
                };
                let delivery_count = completed.deliveries;
                let retire = matches!(handled, Handled::LeaveAndDisconnect);
                let remember_completion = matches!(handled, Handled::Reply(_) | Handled::Ack);
                // The reader marks death before closing its event lanes. This
                // closes the completion-vs-close select race: even if this
                // biased arm wins first, it must not write to the dead socket
                // or return early while other handlers remain detached.
                if !connection_lost && !client.reader_alive() {
                    messages_open = false;
                    exit_reason = ServeExitReason::ConnectionClosed;
                    connection_lost = true;
                    owner_loss.cancel();
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
                        Handled::Leave | Handled::LeaveAndDisconnect => {}
                        }
                        Ok::<(), BrokerError>(())
                    }
                    .await
                } else {
                    Ok(())
                };
                let _ = completed.task.join().await;
                if let Err(error) = wire_result {
                    tracing::warn!(%error, "broker worker completion delivery failed; cancelling remaining handlers");
                    messages_open = false;
                    connection_lost = true;
                    owner_loss.cancel();
                    connection_failure = Some(error);
                    for handler in inflight.values() {
                        handler.cancel.cancel();
                    }
                    connection_drain_sleep.as_mut().reset(
                        tokio::time::Instant::now() + connection_drain_timeout,
                    );
                } else if retire && !connection_lost {
                    tracing::warn!(message_id = %id.as_str(), "worker lost Run ownership or critical uplink; retiring without ACK");
                    messages_open = false;
                    connection_lost = true;
                    owner_loss.cancel();
                    connection_failure = Some(BrokerError::Transport(
                        "worker lost Run ownership or critical uplink; Run remains retryable".into(),
                    ));
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
                    owner_loss.cancel();
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
                    // A Run may have reached this connection's reader queue
                    // before another subscriber replaced it. The broker's
                    // enqueue/replay fence alone cannot retract such a frame.
                    // Ask the broker for the current authenticated generation
                    // on this same connection immediately before admission.
                    if msg.kind == InboxKind::FencedRun {
                        let current = match (
                            me.role.as_deref(),
                            serde_json::from_value::<FencedRunEnvelope>(msg.body.clone()),
                        ) {
                            (Some(role), Ok(envelope)) => client
                                .observe_host(&me.session_id, role)
                                .await
                                .ok()
                                .flatten()
                                .is_some_and(|observation| {
                                    envelope.matches_observation(&me.session_id, &observation)
                                }),
                            _ => false,
                        };
                        if !current {
                            tracing::warn!(run_id = %msg.id.as_str(), "queued FencedRun no longer owns this WorkerHost connection; retaining it for broker recovery");
                            messages_open = false;
                            exit_reason = ServeExitReason::ConnectionClosed;
                            connection_lost = true;
                            owner_loss.cancel();
                            for handler in inflight.values() {
                                handler.cancel.cancel();
                            }
                            connection_drain_sleep.as_mut().reset(
                                tokio::time::Instant::now() + connection_drain_timeout,
                            );
                            continue;
                        }
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
                            owner_loss.cancel();
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
                            use futures_util::FutureExt;
                            let handled = std::panic::AssertUnwindSafe(async { handler(msg, token).await })
                                .catch_unwind()
                                .await
                                .unwrap_or_else(|_| {
                                    tracing::error!(message_id = %id.as_str(), "broker handler panicked; preserving unacknowledged delivery");
                                    Handled::Leave
                                });
                            // Receiver gone == owner loop exited (conn dropped) -> drop.
                            let _ = done_tx.send(Completion { id, reply_to, handled });
                        });
                        inflight.insert(
                            inflight_id,
                            InflightHandler {
                                cancel: inflight_cancel,
                                task: AbortOnDropTask::new(task),
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
                    owner_loss.cancel();
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
                    for (_, mut handler) in stuck {
                        let _ = handler.task.join().await;
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
/// `ClientConfig` for `wss://` (see [`serve_mailbox_full`]) — shared by the
/// worker's inbound connection and its fixed control/event uplinks. `None`
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
    let (uplink, _uplink_owners) =
        ActorBrokerUplink::connect(endpoint, &me, token, &tls_config).await?;
    let context: Arc<tokio::sync::Mutex<Vec<serde_json::Value>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::new()));
    // `serve_loop` intentionally overlaps mailbox handlers. Executor-backed
    // Run/Ask/Task work needs an additional isolation boundary: production
    // executors commonly carry mutable permission and nested-child routing
    // state, while stateless executors may explicitly advertise more slots.
    let execution_slots = Arc::new(tokio::sync::Semaphore::new(
        executor.max_parallel_executions().max(1),
    ));
    // Per-run coordination so a SEPARATE Steer / ApprovalReply mailbox message can
    // reach the channels of the Run it belongs to (the Run + its control messages
    // arrive as independent messages handled by independent tasks).
    let coords: RunCoords = Arc::new(std::sync::Mutex::new(HashMap::new()));
    let waiters: ApprovalWaiters = Arc::new(std::sync::Mutex::new(HashMap::new()));
    let tree_waiters: TreeWaiters = Arc::new(std::sync::Mutex::new(HashMap::new()));
    let me_owned = me.clone();
    let approval_timeout = DEFAULT_APPROVAL_TIMEOUT;
    let readiness = shutdown.clone();
    let fatal_uplink = CancellationToken::new();
    let owner_loss = CancellationToken::new();
    let handler_owner_loss = owner_loss.clone();
    let environment_lease_v1 = executor.supports_environment_lease_v1();
    serve_mailbox_full_with_lifecycle_and_owner_loss(
        endpoint,
        me,
        token,
        move |msg, cancel| {
            let executor = Arc::clone(&executor);
            let context = Arc::clone(&context);
            let coords = Arc::clone(&coords);
            let waiters = Arc::clone(&waiters);
            let tree_waiters = Arc::clone(&tree_waiters);
            let execution_slots = Arc::clone(&execution_slots);
            let me = me_owned.clone();
            let uplink = uplink.clone();
            let readiness = readiness.clone();
            let fatal_uplink = fatal_uplink.clone();
            let owner_loss = handler_owner_loss.clone();
            async move {
                let _execution_slot = if matches!(
                    msg.kind,
                    InboxKind::Run
                        | InboxKind::LeasedRun
                        | InboxKind::FencedRun
                        | InboxKind::Ask
                        | InboxKind::Task
                ) {
                    Some(tokio::select! {
                        biased;
                        _ = fatal_uplink.cancelled() => return Handled::Leave,
                        slot = execution_slots.acquire_owned() => slot
                            .expect("executor execution-slot semaphore is never closed"),
                    })
                } else {
                    None
                };
                if fatal_uplink.is_cancelled() {
                    return Handled::Leave;
                }
                match msg.kind {
                    // A full child session over the bus (the actor-over-mailbox path):
                    // stream events back to the parent live, then the terminal outcome.
                    InboxKind::Run | InboxKind::LeasedRun | InboxKind::FencedRun => {
                        handle_run(
                            executor.as_ref(),
                            &me,
                            msg,
                            cancel,
                            &coords,
                            &waiters,
                            &tree_waiters,
                            &uplink,
                            approval_timeout,
                            readiness,
                            fatal_uplink,
                            owner_loss,
                            environment_lease_v1,
                        )
                        .await
                    }
                    // In-band steer for a running Run: route to its steer inbox.
                    InboxKind::Steer => {
                        if let Some(run_id) = &msg.correlation_id {
                            let steer = decode_steer_body(&msg.body, Some(msg.id.as_str()));
                            if let Some(steer) = steer {
                                let coords =
                                    coords.lock().unwrap_or_else(|error| error.into_inner());
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
                    InboxKind::OwnedTreeReply => {
                        let id = msg
                            .body
                            .get("id")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default();
                        let mut pending = tree_waiters_lock(&tree_waiters);
                        if pending.get(id).is_some_and(|waiter| {
                            waiter.parent == msg.from
                                && msg.correlation_id.as_ref() == Some(&waiter.run_id)
                        }) {
                            if let Some(waiter) = pending.remove(id) {
                                let page = msg
                                    .body
                                    .get("page")
                                    .filter(|page| {
                                        serde_json::to_vec(page)
                                            .is_ok_and(|bytes| bytes.len() <= 8192)
                                    })
                                    .cloned();
                                let _ = waiter.reply.send(page);
                            }
                        }
                        Handled::Ack
                    }
                    InboxKind::SubAgentReply => {
                        let id = msg
                            .body
                            .get("id")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or_default();
                        let mut pending = tree_waiters_lock(&tree_waiters);
                        if pending.get(id).is_some_and(|waiter| {
                            waiter.parent == msg.from
                                && msg.correlation_id.as_ref() == Some(&waiter.run_id)
                        }) {
                            if let Some(waiter) = pending.remove(id) {
                                let result = msg
                                    .body
                                    .get("result")
                                    .filter(|value| {
                                        serde_json::to_vec(value)
                                            .is_ok_and(|bytes| bytes.len() <= 16 * 1024)
                                    })
                                    .cloned();
                                let _ = waiter.reply.send(result);
                            }
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
        owner_loss,
        environment_lease_v1,
    )
    .await
}

/// Live steer channel for a running [`InboxKind::Run`], keyed by run id so an
/// out-of-band [`InboxKind::Steer`] message can be pushed into the run's inbox.
struct RunCoord {
    steer_tx: tokio::sync::mpsc::UnboundedSender<bamboo_subagent::SteerMessage>,
}
type RunCoords = Arc<std::sync::Mutex<HashMap<MsgId, RunCoord>>>;

struct RunCoordRegistration {
    coords: RunCoords,
    run_id: MsgId,
}

impl Drop for RunCoordRegistration {
    fn drop(&mut self) {
        self.coords
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&self.run_id);
    }
}
/// Pending gated-tool approvals a Run proxied up, keyed by approval-request id;
/// an [`InboxKind::ApprovalReply`] fulfils the matching one.
type ApprovalWaiters = Arc<std::sync::Mutex<HashMap<String, tokio::sync::oneshot::Sender<bool>>>>;

struct TreeReplyWaiter {
    parent: AgentRef,
    run_id: MsgId,
    reply: tokio::sync::oneshot::Sender<Option<serde_json::Value>>,
}
type TreeWaiters = Arc<std::sync::Mutex<HashMap<String, TreeReplyWaiter>>>;

fn tree_waiters_lock(
    waiters: &TreeWaiters,
) -> std::sync::MutexGuard<'_, HashMap<String, TreeReplyWaiter>> {
    waiters
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

struct TreeWaiterRegistration {
    waiters: TreeWaiters,
    id: String,
}

impl Drop for TreeWaiterRegistration {
    fn drop(&mut self) {
        tree_waiters_lock(&self.waiters).remove(&self.id);
    }
}

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
    delivery: impl Future<Output = BrokerResult<MsgId>>,
    waiters: &ApprovalWaiters,
    approval_id: &str,
    owner_cancel: &CancellationToken,
) -> BrokerResult<bool> {
    let result = tokio::select! {
        biased;
        _ = owner_cancel.cancelled() => None,
        result = delivery => Some(result),
    };
    match result {
        Some(Ok(_)) => Ok(true),
        Some(Err(error)) => {
            tracing::warn!(approval_id, %error, "approval request delivery failed; denying fail-closed");
            approval_waiters_lock(waiters).remove(approval_id);
            Err(error)
        }
        None => {
            approval_waiters_lock(waiters).remove(approval_id);
            Ok(false)
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
/// outcome through the worker's fixed, bounded control/event uplinks.
///
/// The serve loop only `Ack`s the run; the real result flows as `Event`s and a
/// final `Outcome`, both correlated to the run id, so the parent can stream them
/// exactly like it would over a direct WS connection.
#[allow(clippy::too_many_arguments)]
async fn handle_run<E>(
    executor: &E,
    me: &AgentRef,
    msg: InboxMessage,
    cancel: CancellationToken,
    coords: &RunCoords,
    waiters: &ApprovalWaiters,
    tree_waiters: &TreeWaiters,
    uplink: &ActorBrokerUplink,
    approval_timeout: Duration,
    readiness: CancellationToken,
    fatal_uplink: CancellationToken,
    owner_loss: CancellationToken,
    environment_lease_v1: bool,
) -> Handled
where
    E: bamboo_subagent::ChildExecutor + ?Sized,
{
    use bamboo_subagent::executor::HostRequestKind;
    use bamboo_subagent::{
        ActorEventBatcher, EventSink, ExecutorControl, HostBridge, RunSpec, SteerInbox,
    };

    if fatal_uplink.is_cancelled() {
        return Handled::Leave;
    }
    if msg.kind == InboxKind::LeasedRun {
        return reject_leased_run(me, &msg, uplink, "remote_unfenced_run_rejected").await;
    }
    let spec: RunSpec = if msg.kind == InboxKind::FencedRun {
        let envelope: FencedRunEnvelope = match serde_json::from_value(msg.body.clone()) {
            Ok(envelope) => envelope,
            Err(_) => {
                return reject_leased_run(me, &msg, uplink, "remote_fenced_runspec_invalid").await;
            }
        };
        if envelope.version != FencedRunEnvelope::VERSION
            || envelope.recipient_host_ref.is_empty()
            || envelope.recipient_mailbox != me.session_id
            || envelope.recipient_connection_generation.is_empty()
            || envelope.recipient_role != me.role.as_deref().unwrap_or_default()
        {
            return reject_leased_run(me, &msg, uplink, "remote_fenced_runspec_invalid").await;
        }
        // The message can wait for an executor slot after the inbound reader
        // checked it. A replacement during that wait invalidates the old
        // connection even though its reader already queued this Run.
        if !uplink
            .fenced_run_is_current(me, &envelope)
            .await
            .unwrap_or(false)
        {
            tracing::warn!(run_id = %msg.id.as_str(), "FencedRun lost WorkerHost ownership before executor admission");
            return Handled::LeaveAndDisconnect;
        }
        envelope.run
    } else {
        match serde_json::from_value(msg.body.clone()) {
            Ok(spec) => spec,
            Err(error) => {
                tracing::warn!("run {:?}: malformed RunSpec: {error}", msg.id);
                return Handled::Ack;
            }
        }
    };
    if msg.kind == InboxKind::Run
        && spec
            .permission_policy
            .as_ref()
            .and_then(|policy| policy.environment_lease.as_ref())
            .is_some()
    {
        return reject_leased_run(me, &msg, uplink, "remote_unfenced_run_rejected").await;
    }
    if msg.kind == InboxKind::FencedRun {
        if !environment_lease_v1 {
            tracing::warn!(run_id = %msg.id.as_str(), "leased Run reached an unsupported executor; retaining it for a capable worker");
            return Handled::LeaveAndDisconnect;
        }
        let error = if !spec.permission_policy.as_ref().is_some_and(|policy| {
            policy.workspace_path.is_none() && policy.environment_lease.is_some()
        }) {
            Some("remote_environment_lease_missing_or_host_path_present")
        } else {
            None
        };
        if let Some(error) = error {
            return reject_leased_run(me, &msg, uplink, error).await;
        }
    }
    let run_id = msg.id.clone();
    let parent_actor = msg.from.clone();
    let parent = msg.from.session_id.clone();
    let legacy_event_wire = spec.execution_epoch == 0;
    let mut event_batcher = ActorEventBatcher::for_run(&spec, None, Some(me.session_id.clone()));

    let (sink, mut events, mut controls) = EventSink::channel_with_control();
    // Steer: register this run's steer inbox so out-of-band Steer messages route in.
    let (steer_tx, steer_inbox) = SteerInbox::channel();
    coords
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .insert(run_id.clone(), RunCoord { steer_tx });
    let registration = RunCoordRegistration {
        coords: coords.clone(),
        run_id: run_id.clone(),
    };
    // Approval: a host bridge on the sink; its requests are pumped to the parent.
    let (host_bridge, mut host_rx) = HostBridge::channel();
    let sink = sink.with_host_bridge(host_bridge);
    let (outcome_tx, outcome_rx) = tokio::sync::oneshot::channel();

    let me = me.clone();
    let forward_cancel = cancel.clone();
    let approval_cancel = cancel.clone();
    let critical_uplink_failed = CancellationToken::new();

    // Forward task: enqueue Events onto the worker-owned ordered data uplink,
    // then append Outcome on that same lane once the run finishes.
    let run_id_fwd = run_id.clone();
    let me_fwd = me.clone();
    let parent_fwd = parent.clone();
    let uplink_fwd = uplink.clone();
    let failure_fwd = critical_uplink_failed.clone();
    let readiness_fwd = readiness.clone();
    let fatal_fwd = fatal_uplink.clone();
    let owner_loss_fwd = owner_loss.clone();
    let mut forward = AbortOnDropTask::new(tokio::spawn(async move {
        let critical_failure = || {
            failure_fwd.cancel();
            fatal_fwd.cancel();
            readiness_fwd.cancel();
            forward_cancel.cancel();
            false
        };
        let emit = |kind, body| InboxMessage {
            id: MsgId::new(),
            from: me_fwd.clone(),
            kind,
            body,
            created_at: Utc::now(),
            correlation_id: Some(run_id_fwd.clone()),
        };
        let mut flush = tokio::time::interval(Duration::from_millis(20));
        flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        flush.tick().await;
        let mut events_open = true;
        let mut controls_open = true;
        let mut explicit_cancel_seen = false;
        while events_open || controls_open {
            tokio::select! {
                biased;
                _ = owner_loss_fwd.cancelled() => return false,
                _ = fatal_fwd.cancelled() => return false,
                // An explicit parent Cancel still needs a durable Cancelled
                // Outcome before the Run may be acknowledged.
                _ = forward_cancel.cancelled(), if !explicit_cancel_seen => {
                    explicit_cancel_seen = true;
                },
                control = controls.recv(), if controls_open => match control {
                    Some(ExecutorControl::InitialInputReleaseRequest(request)) => {
                        // The worker enqueued its actual permission audit before
                        // this control. Drain/flush it on the ordered event lane
                        // before asking the Host for provider permission.
                        while let Ok(event) = events.try_recv() {
                            for batch in event_batcher.push(event) {
                                if !forward_actor_event_batch(&uplink_fwd, &parent_fwd, &run_id_fwd, batch).await { return critical_failure(); }
                            }
                        }
                        if let Some(batch) = event_batcher.flush() {
                            if !forward_actor_event_batch(&uplink_fwd, &parent_fwd, &run_id_fwd, batch).await { return critical_failure(); }
                        }
                        let Ok(body) = serde_json::to_value(bamboo_subagent::proto::InitialInputControl::Request { request }) else { return critical_failure(); };
                        if uplink_fwd.deliver_ordered(&parent_fwd, emit(InboxKind::SessionMessageAdmitted, body)).await.is_err() { return critical_failure(); }
                    }
                    Some(ExecutorControl::SessionMessageAdmitted(confirmation)) => {
                        let body = serde_json::to_value(confirmation)
                            .unwrap_or_else(|_| serde_json::json!({}));
                        if uplink_fwd
                            .deliver_control(
                                &parent_fwd,
                                emit(InboxKind::SessionMessageAdmitted, body),
                            )
                            .await
                            .is_err()
                        {
                            return critical_failure();
                        }
                    }
                    None => controls_open = false,
                },
                _ = flush.tick(), if !legacy_event_wire && event_batcher.has_pending() => {
                    if let Some(batch) = event_batcher.flush() {
                        if !forward_actor_event_batch(
                            &uplink_fwd,
                            &parent_fwd,
                            &run_id_fwd,
                            batch,
                        )
                        .await
                        {
                            return critical_failure();
                        }
                    }
                },
                event = events.recv(), if events_open => match event {
                    Some(event) => {
                        if legacy_event_wire {
                            if uplink_fwd
                                .deliver_ordered(
                                    &parent_fwd,
                                    emit(InboxKind::Event, event),
                                )
                                .await
                                .is_err()
                            {
                                return critical_failure();
                            }
                            continue;
                        }
                        for batch in event_batcher.push(event) {
                            if !forward_actor_event_batch(
                                &uplink_fwd,
                                &parent_fwd,
                                &run_id_fwd,
                                batch,
                            )
                            .await
                            {
                                return critical_failure();
                            }
                        }
                    }
                    None => events_open = false,
                },
            }
        }
        if !legacy_event_wire {
            if let Some(batch) = event_batcher.flush() {
                if !forward_actor_event_batch(&uplink_fwd, &parent_fwd, &run_id_fwd, batch).await {
                    return critical_failure();
                }
            }
        }
        let outcome = tokio::select! {
            biased;
            _ = owner_loss_fwd.cancelled() => return false,
            _ = fatal_fwd.cancelled() => return false,
            _ = failure_fwd.cancelled() => return false,
            outcome = outcome_rx => outcome,
        };
        let Ok(outcome) = outcome else {
            return critical_failure();
        };
        let body = serde_json::to_value(&outcome).unwrap_or_else(|_| serde_json::json!({}));
        if failure_fwd.is_cancelled() || fatal_fwd.is_cancelled() || owner_loss_fwd.is_cancelled() {
            return false;
        }
        // Once submitted, the receipt is authoritative: a concurrent control
        // failure must not discard a known-successful Outcome and replay it.
        let delivered = uplink_fwd
            .deliver_ordered(&parent_fwd, emit(InboxKind::Outcome, body))
            .await;
        if delivered.is_err() {
            return critical_failure();
        }
        true
    }));

    // Approval drain: each gated-tool approval the executor raises is delivered to
    // the parent as an ApprovalRequest (correlated to the run); the matching
    // ApprovalReply wakes the registered waiter, whose decision answers the tool.
    // Ends when the run drops the sink ⇒ the host bridge ⇒ `host_rx` closes.
    let waiters_drain = Arc::clone(waiters);
    let tree_waiters_drain = Arc::clone(tree_waiters);
    let run_id_appr = run_id.clone();
    let parent_actor_appr = parent_actor.clone();
    let uplink_appr = uplink.clone();
    let failure_appr = critical_uplink_failed.clone();
    let readiness_appr = readiness.clone();
    let fatal_appr = fatal_uplink.clone();
    let mut approval = AbortOnDropTask::new(tokio::spawn(async move {
        loop {
            let req = tokio::select! {
                biased;
                _ = approval_cancel.cancelled() => break,
                req = host_rx.recv() => match req {
                    Some(req) => req,
                    None => break,
                },
            };
            if matches!(
                req.kind,
                HostRequestKind::OwnedTree | HostRequestKind::SubAgent
            ) {
                let request_id = MsgId::new();
                let request_id_str = format!("{request_id:?}");
                let (tx, rx) = tokio::sync::oneshot::channel();
                tree_waiters_lock(&tree_waiters_drain).insert(
                    request_id_str.clone(),
                    TreeReplyWaiter {
                        parent: parent_actor_appr.clone(),
                        run_id: run_id_appr.clone(),
                        reply: tx,
                    },
                );
                let _registration = TreeWaiterRegistration {
                    waiters: tree_waiters_drain.clone(),
                    id: request_id_str.clone(),
                };
                let (kind, body, timeout) = match req.kind {
                    HostRequestKind::OwnedTree => (
                        InboxKind::OwnedTreeRequest,
                        serde_json::json!({
                            "id": request_id_str,
                            "cursor": req.body.get("cursor").cloned().unwrap_or_default(),
                        }),
                        Duration::from_secs(8),
                    ),
                    HostRequestKind::SubAgent => (
                        InboxKind::SubAgentRequest,
                        serde_json::json!({
                            "id": request_id_str,
                            "tool_call_id": req.body.get("tool_call_id"),
                            "args": req.body.get("args"),
                        }),
                        Duration::from_secs(30),
                    ),
                    HostRequestKind::Approval => unreachable!(),
                };
                let message = InboxMessage {
                    id: MsgId::new(),
                    from: me.clone(),
                    kind,
                    body,
                    created_at: Utc::now(),
                    correlation_id: Some(run_id_appr.clone()),
                };
                let delivered = tokio::select! {
                    _ = approval_cancel.cancelled() => false,
                    result = tokio::time::timeout(timeout,
                        uplink_appr.deliver_control(&parent, message)) => result.is_ok_and(|result| result.is_ok()),
                };
                let result = if delivered {
                    tokio::select! {
                        _ = approval_cancel.cancelled() => None,
                        result = tokio::time::timeout(timeout, rx) => result.ok().and_then(Result::ok).flatten(),
                    }
                } else {
                    None
                };
                let reply = if req.kind == HostRequestKind::OwnedTree {
                    serde_json::json!({ "page": result })
                } else {
                    result.unwrap_or_else(|| {
                        serde_json::json!({
                            "error": "canonical SubAgent operation unavailable"
                        })
                    })
                };
                let _ = req.reply.send(reply);
                continue;
            }
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
                uplink_appr.deliver_control(&parent, m),
                &waiters_drain,
                &approval_id_str,
                &approval_cancel,
            )
            .await;
            match delivered {
                Ok(true) => {}
                Ok(false) => {
                    let _ = req.reply.send(serde_json::json!({ "approved": false }));
                    continue;
                }
                Err(_) => {
                    failure_appr.cancel();
                    fatal_appr.cancel();
                    readiness_appr.cancel();
                    approval_cancel.cancel();
                    let _ = req.reply.send(serde_json::json!({ "approved": false }));
                    break;
                }
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
    use futures_util::FutureExt;
    let outcome = tokio::select! {
        biased;
        _ = owner_loss.cancelled() => return Handled::Leave,
        _ = fatal_uplink.cancelled() => return Handled::LeaveAndDisconnect,
        _ = critical_uplink_failed.cancelled() => return Handled::LeaveAndDisconnect,
        outcome = std::panic::AssertUnwindSafe(executor.run(spec, sink, steer_inbox, cancel)).catch_unwind() => {
            outcome.unwrap_or_else(|_| bamboo_subagent::ChildOutcome::error("actor executor panicked"))
        }
    };
    drop(registration);
    let _ = outcome_tx.send(outcome);
    let critical_uplink_ok = matches!(forward.join().await, Ok(true));
    let _ = approval.join().await;
    // Once Outcome is durable, a subsequently observed control failure retires
    // this worker but must not replay the same successful terminal result.
    if critical_uplink_ok {
        Handled::Ack
    } else {
        Handled::LeaveAndDisconnect
    }
}

async fn reject_leased_run(
    me: &AgentRef,
    msg: &InboxMessage,
    uplink: &ActorBrokerUplink,
    reason: &'static str,
) -> Handled {
    use sha2::{Digest, Sha256};
    use std::fmt::Write as _;

    let outcome = bamboo_subagent::ChildOutcome::error(reason);
    let body = serde_json::to_value(outcome).expect("ChildOutcome is serializable");
    // Maildir names include both created_at and MsgId. Keep both stable across
    // a Run replay so a lost Run ACK cannot create a fresh terminal identity.
    let mut stable_id = String::from("lease-reject-");
    for byte in Sha256::digest(format!("{}:{}", me.session_id, msg.id.as_str()).as_bytes()) {
        write!(&mut stable_id, "{byte:02x}").expect("write to String");
    }
    let outcome_id = MsgId(stable_id);
    let terminal = InboxMessage {
        id: outcome_id,
        from: me.clone(),
        kind: InboxKind::Outcome,
        body,
        created_at: msg.created_at,
        correlation_id: Some(msg.id.clone()),
    };
    if uplink
        .deliver_ordered(&msg.from.session_id, terminal)
        .await
        .is_ok()
    {
        Handled::Ack
    } else {
        Handled::LeaveAndDisconnect
    }
}

async fn forward_actor_event_batch(
    uplink: &ActorBrokerUplink,
    parent: &str,
    run_id: &MsgId,
    batch: ActorEventBatch,
) -> bool {
    uplink.send_event_batch(parent, run_id, batch).await
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
    let (sink, discard) = EventSink::channel();
    // Ask/Task return only the final reply. Close the intentionally unused
    // stream so bounded sends cannot wait forever for a nonexistent consumer.
    drop(discard);
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
                execution_epoch: 0,
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
    if ["initial_input_control", "request", "release"]
        .iter()
        .any(|key| body.get(key).is_some())
    {
        return match bamboo_subagent::proto::InitialInputControl::decode(body.clone()) {
            Ok(bamboo_subagent::proto::InitialInputControl::Release { release }) => {
                Some(bamboo_subagent::SteerMessage::InitialInputRelease(release))
            }
            _ => None, // Unknown tags/wrong direction never fall back to Text.
        };
    }
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

    #[tokio::test]
    async fn reply_only_ask_and_task_do_not_block_on_discarded_event_capacity() {
        let question = std::iter::repeat_n("word", bamboo_subagent::EventSink::EVENT_CAPACITY + 1)
            .collect::<Vec<_>>()
            .join(" ");
        for kind in [InboxKind::Ask, InboxKind::Task] {
            let mut message = ask("parent", &question);
            if kind == InboxKind::Task {
                message.kind = InboxKind::Task;
                message.body = serde_json::json!({ "assignment": question });
            }
            let context = tokio::sync::Mutex::new(Vec::new());
            let handled = tokio::time::timeout(
                Duration::from_secs(2),
                handle_with_executor(
                    &bamboo_subagent::EchoExecutor,
                    &context,
                    message,
                    CancellationToken::new(),
                ),
            )
            .await
            .expect("reply-only execution must not await an unused stream consumer");
            assert!(
                matches!(handled, Handled::Reply(answer) if answer == format!("echo: {question}"))
            );
            assert_eq!(
                context.lock().await.len(),
                if kind == InboxKind::Task { 2 } else { 0 }
            );
        }
    }

    #[tokio::test]
    async fn panicked_handler_releases_inflight_slot_and_preserves_unacked_delivery() {
        let (endpoint, dir, core, connection) = start_single_connection().await;
        core.deliver("panic-worker", &ask("parent", "panic"))
            .await
            .unwrap();
        let me = AgentRef {
            session_id: "panic-worker".into(),
            role: None,
        };
        let mut client = BrokerClient::connect(&endpoint, me.clone(), TOKEN)
            .await
            .unwrap();
        client.subscribe().await.unwrap();
        let started = Arc::new(tokio::sync::Notify::new());
        let shutdown = CancellationToken::new();
        let task = tokio::spawn({
            let started = started.clone();
            let shutdown = shutdown.clone();
            async move {
                serve_loop(
                    &mut client,
                    &me,
                    move |_, _| {
                        let started = started.clone();
                        async move {
                            started.notify_one();
                            panic!("intentional mailbox handler panic");
                            #[allow(unreachable_code)]
                            Handled::Leave
                        }
                    },
                    shutdown,
                )
                .await
            }
        });
        tokio::time::timeout(Duration::from_secs(2), started.notified())
            .await
            .unwrap();
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("panicked admission must not block graceful drain")
            .unwrap()
            .unwrap();
        assert_eq!(mailbox_pending_files(&dir, "panic-worker").await, 1);
        connection.abort();
    }

    #[tokio::test]
    async fn aborting_mailbox_owner_aborts_its_inflight_execution_and_coord_registration() {
        let (endpoint, _dir, core, connection) = start_single_connection().await;
        core.deliver("aborted-worker", &ask("parent", "hang"))
            .await
            .unwrap();
        let me = AgentRef {
            session_id: "aborted-worker".into(),
            role: None,
        };
        let mut client = BrokerClient::connect(&endpoint, me.clone(), TOKEN)
            .await
            .unwrap();
        client.subscribe().await.unwrap();
        let started = Arc::new(tokio::sync::Notify::new());
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let coords: RunCoords = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let task = tokio::spawn({
            let started = started.clone();
            let dropped = dropped.clone();
            let coords = coords.clone();
            async move {
                serve_loop(
                    &mut client,
                    &me,
                    move |message, _| {
                        let started = started.clone();
                        let dropped = dropped.clone();
                        let coords = coords.clone();
                        async move {
                            let _dropped = DropFlag(dropped);
                            let (steer_tx, _inbox) = bamboo_subagent::SteerInbox::channel();
                            coords
                                .lock()
                                .unwrap()
                                .insert(message.id.clone(), RunCoord { steer_tx });
                            let _registration = RunCoordRegistration {
                                coords,
                                run_id: message.id,
                            };
                            started.notify_one();
                            std::future::pending::<Handled>().await
                        }
                    },
                    CancellationToken::new(),
                )
                .await
            }
        });
        tokio::time::timeout(Duration::from_secs(2), started.notified())
            .await
            .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(2), async {
            while !dropped.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("owner drop must abort rather than detach handler");
        assert!(coords.lock().unwrap().is_empty());
        connection.abort();
    }

    #[tokio::test]
    async fn shared_actor_uplink_accepts_200_parallel_live_runs() {
        let (control, _control_rx) =
            tokio::sync::mpsc::channel(ACTOR_UPLINK_CONTROL_QUEUE_CAPACITY);
        let (events, mut event_rx) = tokio::sync::mpsc::channel(ACTOR_UPLINK_EVENT_QUEUE_CAPACITY);
        let uplink = ActorBrokerUplink {
            control,
            events,
            source: AgentRef {
                session_id: "cluster-worker".into(),
                role: Some("pool".into()),
            },
        };
        let sends = (0..200).map(|run| {
            let uplink = uplink.clone();
            async move {
                let correlation_id = MsgId::new();
                uplink
                    .send_event_batch(
                        "parent",
                        &correlation_id,
                        ActorEventBatch {
                            logical_session: None,
                            activation_id: Some(format!("activation-{run}")),
                            execution_epoch: run + 1,
                            source_node_id: Some("node-a".into()),
                            source_actor_id: Some("cluster-worker".into()),
                            first_seq: 1,
                            last_seq: 1,
                            qos: ActorEventQos::Ephemeral,
                            events: vec![serde_json::json!({
                                "type":"token",
                                "content":run.to_string(),
                            })],
                        },
                    )
                    .await
            }
        });
        let accepted = futures_util::future::join_all(sends).await;
        assert!(accepted.into_iter().all(|accepted| accepted));

        let mut queued = 0usize;
        while event_rx.try_recv().is_ok() {
            queued += 1;
        }
        assert_eq!(queued, 200);
    }

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

    async fn start_executor_connections() -> (
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
        let connections = tokio::spawn(async move {
            let mut handlers = tokio::task::JoinSet::new();
            // One inbound subscription plus the worker-level control and event
            // uplinks. Dropping the JoinSet aborts every accepted connection.
            for _ in 0..3 {
                let (stream, _) = listener.accept().await.expect("worker connection");
                let server = Arc::clone(&server);
                handlers.spawn(async move {
                    let _ = server.handle_conn(stream).await;
                });
            }
            while handlers.join_next().await.is_some() {}
        });
        (format!("ws://{addr}"), dir, core, connections)
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

    #[derive(Clone, Copy, Debug)]
    enum RunUplinkFault {
        Event,
        Control,
        Approval,
        Outcome,
    }

    struct RetryRunExecutor {
        fault: RunUplinkFault,
        calls: std::sync::atomic::AtomicUsize,
    }

    struct WaitForRunCancel {
        started: Arc<tokio::sync::Notify>,
        calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl bamboo_subagent::ChildExecutor for WaitForRunCancel {
        async fn run(
            &self,
            _spec: bamboo_subagent::RunSpec,
            _events: bamboo_subagent::EventSink,
            _steer: bamboo_subagent::SteerInbox,
            cancel: CancellationToken,
        ) -> bamboo_subagent::ChildOutcome {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.started.notify_one();
            cancel.cancelled().await;
            bamboo_subagent::ChildOutcome::cancelled()
        }
    }

    #[async_trait::async_trait]
    impl bamboo_subagent::ChildExecutor for RetryRunExecutor {
        async fn run(
            &self,
            _spec: bamboo_subagent::RunSpec,
            events: bamboo_subagent::EventSink,
            _steer: bamboo_subagent::SteerInbox,
            _cancel: CancellationToken,
        ) -> bamboo_subagent::ChildOutcome {
            use std::sync::atomic::Ordering;
            if self.calls.fetch_add(1, Ordering::SeqCst) != 0 {
                return bamboo_subagent::ChildOutcome::completed("recovered");
            }
            match self.fault {
                RunUplinkFault::Event => {
                    events
                        .emit(serde_json::json!({"type":"task_list_item_progress"}))
                        .await;
                }
                RunUplinkFault::Control => {
                    events
                        .confirm_session_message(
                            bamboo_subagent::SessionMessageAdmissionConfirmation {
                                target_session_id: "retry-worker".into(),
                                envelope_id: "owned-input".into(),
                                canonical_claim_generation: 1,
                                activation_run_id: "same-activation".into(),
                            },
                        )
                        .await;
                }
                RunUplinkFault::Approval => {
                    let _ = events
                        .host()
                        .expect("Run has a host bridge")
                        .approval_call(serde_json::json!({"resource":"blocked"}))
                        .await;
                }
                RunUplinkFault::Outcome => {
                    return bamboo_subagent::ChildOutcome::completed("unconfirmed");
                }
            }
            // The worker must retire even if an executor ignores cancellation.
            std::future::pending().await
        }
    }

    fn test_run_uplink(
        source: &AgentRef,
        core: &Arc<BrokerCore>,
        fault: Option<RunUplinkFault>,
        outcomes: &Arc<std::sync::atomic::AtomicUsize>,
    ) -> ActorBrokerUplink {
        let (control, mut control_rx) = tokio::sync::mpsc::channel::<ActorControlCommand>(4);
        let (events, mut event_rx) = tokio::sync::mpsc::channel::<ActorEventCommand>(4);
        if matches!(
            fault,
            Some(RunUplinkFault::Control | RunUplinkFault::Approval)
        ) {
            drop(control_rx);
        } else {
            let core = Arc::clone(core);
            tokio::spawn(async move {
                while let Some(command) = control_rx.recv().await {
                    match command {
                        ActorControlCommand::Deliver {
                            to,
                            message,
                            result,
                        } => {
                            let _ = result.send(core.deliver(&to, &message).await);
                        }
                        ActorControlCommand::ObserveSelf {
                            mailbox,
                            role,
                            result,
                        } => {
                            let _ = result
                                .send(Ok(core.current_host_observation(&mailbox, &role).await));
                        }
                    }
                }
            });
        }
        if matches!(fault, Some(RunUplinkFault::Event | RunUplinkFault::Outcome)) {
            drop(event_rx);
        } else {
            let core = Arc::clone(core);
            let outcomes = Arc::clone(outcomes);
            tokio::spawn(async move {
                while let Some(command) = event_rx.recv().await {
                    if let ActorEventCommand::Durable {
                        to,
                        message,
                        result,
                    } = command
                    {
                        let terminal = matches!(message.kind, InboxKind::Outcome);
                        let delivered = core.deliver(&to, &message).await;
                        if terminal && delivered.is_ok() {
                            outcomes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        }
                        let _ = result.send(delivered);
                    }
                }
            });
        }
        ActorBrokerUplink {
            control,
            events,
            source: source.clone(),
        }
    }

    #[tokio::test]
    async fn queued_fenced_run_fails_worker_admission_after_slot_wait_and_replacement() {
        use crate::core::AuthenticatedHost;
        use chrono::Utc;

        let dir = tempfile::tempdir().unwrap();
        let core = Arc::new(BrokerCore::new(dir.path()));
        let me = AgentRef {
            session_id: "worker".into(),
            role: Some("worker".into()),
        };
        let host = || AuthenticatedHost {
            host_ref: "worker-host".into(),
            credential_expires_at: Utc::now() + chrono::Duration::minutes(5),
            host_capabilities: None,
            max_slots: None,
        };
        let (_first_streams, _first_lease) = core
            .subscribe_scoped_environment_lease_v1("worker", Some("worker"), host())
            .await
            .unwrap();
        let first = core
            .current_host_observation("worker", "worker")
            .await
            .unwrap();
        let run: bamboo_subagent::RunSpec = serde_json::from_value(serde_json::json!({
            "assignment":"queued work",
            "logical_session":{
                "session_id":"logical-child","parent_session_id":"logical-parent",
                "root_session_id":"logical-root",
                "creation":{"created_at":Utc::now(),"spawn_depth":1}
            },
            "activation_run_id":"activation-1","execution_epoch":1,
            "permission_policy":{
                "revision":1,"bypass_permissions":false,
                "session_id":"logical-child","policy":{},
                "environment_lease":{
                    "version":1,"actor_id":"logical-child",
                    "activation_run_id":"activation-1","execution_epoch":1,
                    "admit_before":Utc::now()+chrono::Duration::minutes(2),
                    "git_commit":"a".repeat(40),"content_sha256":"b".repeat(64),
                    "workspace_relpath":"."
                }
            }
        }))
        .unwrap();
        let envelope = FencedRunEnvelope::for_observation(run, &first).unwrap();
        let outcomes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let uplink = test_run_uplink(&me, &core, None, &outcomes);
        assert!(uplink.fenced_run_is_current(&me, &envelope).await.unwrap());

        // The worker's handler can sit behind its execution-slot semaphore.
        // When it eventually admits the queued Run, the old generation fails.
        let (_successor_streams, _successor_lease) = core
            .subscribe_scoped_environment_lease_v1("worker", Some("worker"), host())
            .await
            .unwrap();
        assert!(!uplink.fenced_run_is_current(&me, &envelope).await.unwrap());

        struct Probe(Arc<std::sync::atomic::AtomicUsize>);
        #[async_trait::async_trait]
        impl bamboo_subagent::ChildExecutor for Probe {
            async fn run(
                &self,
                _spec: bamboo_subagent::RunSpec,
                _events: bamboo_subagent::EventSink,
                _steer: bamboo_subagent::SteerInbox,
                _cancel: CancellationToken,
            ) -> bamboo_subagent::ChildOutcome {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                bamboo_subagent::ChildOutcome::completed("should not execute")
            }
        }
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let message = InboxMessage {
            id: MsgId::new(),
            from: AgentRef {
                session_id: "parent".into(),
                role: Some("host".into()),
            },
            kind: InboxKind::FencedRun,
            body: serde_json::to_value(envelope).unwrap(),
            created_at: Utc::now(),
            correlation_id: None,
        };
        let handled = handle_run(
            &Probe(Arc::clone(&calls)),
            &me,
            message,
            CancellationToken::new(),
            &Arc::new(std::sync::Mutex::new(HashMap::new())),
            &Arc::new(std::sync::Mutex::new(HashMap::new())),
            &Arc::new(std::sync::Mutex::new(HashMap::new())),
            &uplink,
            Duration::from_secs(1),
            CancellationToken::new(),
            CancellationToken::new(),
            CancellationToken::new(),
            true,
        )
        .await;
        assert!(matches!(handled, Handled::LeaveAndDisconnect));
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    async fn serve_retry_run<E>(
        endpoint: &str,
        worker: AgentRef,
        executor: Arc<E>,
        uplink: ActorBrokerUplink,
    ) -> BrokerResult<ServeExitReason>
    where
        E: bamboo_subagent::ChildExecutor + ?Sized,
    {
        let mut client = BrokerClient::connect(endpoint, worker.clone(), TOKEN).await?;
        client.subscribe().await?;
        let coords: RunCoords = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let waiters: ApprovalWaiters = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let tree_waiters: TreeWaiters = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let readiness = CancellationToken::new();
        let loop_readiness = readiness.clone();
        let fatal = CancellationToken::new();
        let owner_loss = CancellationToken::new();
        let loop_owner_loss = owner_loss.clone();
        let handler_worker = worker.clone();
        serve_loop_with_timeouts_and_owner_loss(
            &mut client,
            &worker,
            move |message, cancel| {
                let executor = Arc::clone(&executor);
                let coords = Arc::clone(&coords);
                let waiters = Arc::clone(&waiters);
                let tree_waiters = Arc::clone(&tree_waiters);
                let uplink = uplink.clone();
                let worker = handler_worker.clone();
                let readiness = readiness.clone();
                let fatal = fatal.clone();
                let owner_loss = owner_loss.clone();
                async move {
                    handle_run(
                        executor.as_ref(),
                        &worker,
                        message,
                        cancel,
                        &coords,
                        &waiters,
                        &tree_waiters,
                        &uplink,
                        Duration::from_secs(1),
                        readiness,
                        fatal,
                        owner_loss,
                        false,
                    )
                    .await
                }
            },
            loop_readiness,
            Some(Duration::from_millis(500)),
            Duration::from_secs(1),
            Duration::from_secs(1),
            loop_owner_loss,
        )
        .await
    }

    #[tokio::test]
    async fn critical_run_uplink_failures_retry_same_run_without_duplicate_outcome() {
        use std::sync::atomic::Ordering;
        for fault in [
            RunUplinkFault::Event,
            RunUplinkFault::Control,
            RunUplinkFault::Approval,
            RunUplinkFault::Outcome,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let core = Arc::new(BrokerCore::new(dir.path()));
            let server = Arc::new(BrokerServer::new(Arc::clone(&core), TOKEN));
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("ws://{}", listener.local_addr().unwrap());
            let server_task = tokio::spawn(async move {
                let _ = server.serve(listener).await;
            });
            let worker = AgentRef {
                session_id: "retry-worker".into(),
                role: None,
            };
            let run_id = MsgId::new();
            let run = InboxMessage {
                id: run_id.clone(),
                from: AgentRef {
                    session_id: "retry-parent".into(),
                    role: None,
                },
                kind: InboxKind::Run,
                body: serde_json::to_value(bamboo_subagent::RunSpec {
                    assignment: "same-generation".into(),
                    logical_session: None,
                    project_id: None,
                    reasoning_effort: None,
                    permission_policy: None,
                    messages: vec![],
                    activation_run_id: Some("same-activation".into()),
                    execution_epoch: 1,
                    initial_session_messages: Vec::new(),
                    secrets: Default::default(),
                })
                .unwrap(),
                created_at: Utc::now(),
                correlation_id: None,
            };
            core.deliver(&worker.session_id, &run).await.unwrap();
            let outcomes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let executor = Arc::new(RetryRunExecutor {
                fault,
                calls: std::sync::atomic::AtomicUsize::new(0),
            });
            let bad_uplink = test_run_uplink(&worker, &core, Some(fault), &outcomes);
            let failed = tokio::time::timeout(
                Duration::from_secs(5),
                serve_retry_run(&endpoint, worker.clone(), Arc::clone(&executor), bad_uplink),
            )
            .await
            .unwrap_or_else(|_| panic!("{fault:?}: failed worker must retire promptly"));
            assert!(
                failed.is_err(),
                "{fault:?}: worker stays ready after uplink loss"
            );
            tokio::time::timeout(Duration::from_secs(2), async {
                while core.is_subscribed(&worker.session_id).await {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("{fault:?}: failed worker remains subscribed"));
            wait_for_mailbox_count(&dir, &worker.session_id, 1).await;
            assert_eq!(outcomes.load(Ordering::SeqCst), 0, "{fault:?}");

            let healthy = test_run_uplink(&worker, &core, None, &outcomes);
            let recovered = tokio::time::timeout(
                Duration::from_secs(5),
                serve_retry_run(&endpoint, worker.clone(), Arc::clone(&executor), healthy),
            )
            .await
            .unwrap_or_else(|_| panic!("{fault:?}: replacement must finish"))
            .unwrap();
            assert_eq!(recovered, ServeExitReason::IdleTimeout, "{fault:?}");
            wait_for_empty_mailbox(&dir, &worker.session_id).await;
            assert_eq!(executor.calls.load(Ordering::SeqCst), 2, "{fault:?}");
            assert_eq!(outcomes.load(Ordering::SeqCst), 1, "{fault:?}");
            assert_eq!(mailbox_pending_files(&dir, "retry-parent").await, 1);
            server_task.abort();
        }
    }

    #[tokio::test]
    async fn inbound_connection_loss_does_not_publish_cancelled_outcome_for_unacked_run() {
        let (endpoint, dir, core, connection) = start_single_connection().await;
        let worker = AgentRef {
            session_id: "inbound-loss-worker".into(),
            role: None,
        };
        let run = InboxMessage {
            id: MsgId::new(),
            from: AgentRef {
                session_id: "inbound-loss-parent".into(),
                role: None,
            },
            kind: InboxKind::Run,
            body: serde_json::to_value(bamboo_subagent::RunSpec {
                assignment: "inbound-loss".into(),
                logical_session: None,
                project_id: None,
                reasoning_effort: None,
                permission_policy: None,
                messages: vec![],
                activation_run_id: Some("inbound-loss-activation".into()),
                execution_epoch: 1,
                initial_session_messages: Vec::new(),
                secrets: Default::default(),
            })
            .unwrap(),
            created_at: Utc::now(),
            correlation_id: None,
        };
        core.deliver(&worker.session_id, &run).await.unwrap();
        let outcomes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let uplink = test_run_uplink(&worker, &core, None, &outcomes);
        let started = Arc::new(tokio::sync::Notify::new());
        let executor = Arc::new(WaitForRunCancel {
            started: Arc::clone(&started),
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let worker_task =
            tokio::spawn(async move { serve_retry_run(&endpoint, worker, executor, uplink).await });
        tokio::time::timeout(Duration::from_secs(2), started.notified())
            .await
            .expect("Run starts before inbound disconnect");
        connection.abort();
        let _ = connection.await;
        tokio::time::timeout(Duration::from_secs(5), worker_task)
            .await
            .expect("worker stops after inbound loss")
            .expect("worker task does not panic")
            .expect("inbound loss uses the existing clean connection-close path");
        wait_for_mailbox_count(&dir, "inbound-loss-worker", 1).await;
        assert_eq!(outcomes.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(mailbox_pending_files(&dir, "inbound-loss-parent").await, 0);
    }

    #[tokio::test]
    async fn explicit_parent_cancel_publishes_terminal_before_ack_without_replaying_run() {
        let dir = tempfile::tempdir().unwrap();
        let core = Arc::new(BrokerCore::new(dir.path()));
        let server = Arc::new(BrokerServer::new(Arc::clone(&core), TOKEN));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server_task = tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        let worker = AgentRef {
            session_id: "cancel-worker".into(),
            role: None,
        };
        let run = InboxMessage {
            id: MsgId::new(),
            from: AgentRef {
                session_id: "cancel-parent".into(),
                role: None,
            },
            kind: InboxKind::Run,
            body: serde_json::to_value(bamboo_subagent::RunSpec {
                assignment: "explicit-parent-cancel".into(),
                logical_session: None,
                project_id: None,
                reasoning_effort: None,
                permission_policy: None,
                messages: vec![],
                activation_run_id: Some("cancel-activation".into()),
                execution_epoch: 1,
                initial_session_messages: Vec::new(),
                secrets: Default::default(),
            })
            .unwrap(),
            created_at: Utc::now(),
            correlation_id: None,
        };
        core.deliver(&worker.session_id, &run).await.unwrap();
        let outcomes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let executor = Arc::new(WaitForRunCancel {
            started: Arc::clone(&started),
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let worker_task = tokio::spawn({
            let endpoint = endpoint.clone();
            let worker = worker.clone();
            let executor = Arc::clone(&executor);
            let uplink = test_run_uplink(&worker, &core, None, &outcomes);
            async move { serve_retry_run(&endpoint, worker, executor, uplink).await }
        });
        tokio::time::timeout(Duration::from_secs(2), started.notified())
            .await
            .expect("Run starts before explicit parent cancel");
        assert!(core.cancel(&worker.session_id, &run.id).await);
        let exit = tokio::time::timeout(Duration::from_secs(5), worker_task)
            .await
            .expect("worker finishes cancelled Run")
            .expect("worker task does not panic")
            .expect("healthy uplink keeps worker serving until idle");
        assert_eq!(exit, ServeExitReason::IdleTimeout);
        wait_for_empty_mailbox(&dir, &worker.session_id).await;
        assert_eq!(outcomes.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(mailbox_pending_files(&dir, "cancel-parent").await, 1);
        let mut parent = core.subscribe("cancel-parent", None).await.unwrap();
        let PushItem::Message(terminal) = parent.recv().await.expect("durable parent Outcome")
        else {
            panic!("parent receives an Outcome message");
        };
        assert_eq!(terminal.kind, InboxKind::Outcome);
        assert_eq!(terminal.correlation_id, Some(run.id));
        let outcome: bamboo_subagent::ChildOutcome = serde_json::from_value(terminal.body).unwrap();
        assert_eq!(outcome.status, bamboo_subagent::TerminalStatus::Cancelled);

        let replacement = test_run_uplink(&worker, &core, None, &outcomes);
        let replay = tokio::time::timeout(
            Duration::from_secs(2),
            serve_retry_run(&endpoint, worker, Arc::clone(&executor), replacement),
        )
        .await
        .expect("replacement remains idle")
        .expect("replacement can subscribe");
        assert_eq!(replay, ServeExitReason::IdleTimeout);
        assert_eq!(executor.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(outcomes.load(std::sync::atomic::Ordering::SeqCst), 1);
        server_task.abort();
    }

    #[test]
    fn malformed_initial_release_never_becomes_text_steering() {
        for value in [
            serde_json::json!({"release":{}, "text":"unsafe"}),
            serde_json::json!({"request":{}, "text":"unsafe"}),
            serde_json::json!({"initial_input_control":"unknown", "text":"unsafe"}),
            serde_json::json!({"initial_input_control":"release", "release":{}, "text":"unsafe"}),
            serde_json::json!({"initial_input_control":"request", "request":{}, "text":"unsafe"}),
        ] {
            assert!(decode_steer_body(&value, Some("transport-id")).is_none());
        }
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
            fn max_parallel_executions(&self) -> usize {
                N
            }

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
            execution_epoch: 0,
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
    async fn canonical_subagent_rpc_rejects_wrong_run_reply_over_real_broker() {
        struct NestedCall;

        #[async_trait::async_trait]
        impl bamboo_subagent::ChildExecutor for NestedCall {
            async fn run(
                &self,
                _spec: bamboo_subagent::RunSpec,
                events: bamboo_subagent::EventSink,
                _steer: bamboo_subagent::SteerInbox,
                _cancel: CancellationToken,
            ) -> bamboo_subagent::ChildOutcome {
                let result = events
                    .host()
                    .expect("active Run bridge")
                    .subagent_call(
                        serde_json::json!({"intent":"inspect","target":"owned-child"}),
                        "nested-call",
                    )
                    .await
                    .expect("canonical Host reply");
                bamboo_subagent::ChildOutcome::completed(
                    result["result"].as_str().expect("canonical result text"),
                )
            }
        }

        let (endpoint, _dir) = start().await;
        let worker_endpoint = endpoint.clone();
        tokio::spawn(async move {
            let _ = serve_executor(
                &worker_endpoint,
                AgentRef {
                    session_id: "nested-worker".into(),
                    role: None,
                },
                TOKEN,
                Arc::new(NestedCall),
            )
            .await;
        });
        let mut parent = BrokerClient::connect(
            &endpoint,
            AgentRef {
                session_id: "nested-parent".into(),
                role: None,
            },
            TOKEN,
        )
        .await
        .unwrap();
        parent.subscribe().await.unwrap();
        let run = InboxMessage {
            id: MsgId::new(),
            from: AgentRef {
                session_id: "nested-parent".into(),
                role: None,
            },
            kind: InboxKind::Run,
            body: serde_json::to_value(bamboo_subagent::RunSpec {
                assignment: "inspect owned child".into(),
                logical_session: None,
                project_id: None,
                reasoning_effort: None,
                permission_policy: None,
                messages: vec![],
                activation_run_id: Some("nested-activation".into()),
                execution_epoch: 1,
                initial_session_messages: Vec::new(),
                secrets: Default::default(),
            })
            .unwrap(),
            created_at: Utc::now(),
            correlation_id: None,
        };
        let run_id = run.id.clone();
        parent.deliver("nested-worker", run).await.unwrap();
        let request = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let message = parent.next_message().await.unwrap();
                if message.kind == InboxKind::SubAgentRequest {
                    break message;
                }
            }
        })
        .await
        .expect("worker SubAgent RPC");
        assert_eq!(request.correlation_id.as_ref(), Some(&run_id));
        assert_eq!(request.body["tool_call_id"], "nested-call");
        assert_eq!(request.body["args"]["target"], "owned-child");
        parent.ack(request.id.clone()).await.ok();
        let id = request.body["id"].as_str().unwrap();
        let result = serde_json::json!({"result":{
            "success":true,"result":"canonical-result","display_preference":null
        }});
        let reply = |correlation_id| InboxMessage {
            id: MsgId::new(),
            from: AgentRef {
                session_id: "nested-parent".into(),
                role: None,
            },
            kind: InboxKind::SubAgentReply,
            body: serde_json::json!({"id":id,"result":result.clone()}),
            created_at: Utc::now(),
            correlation_id: Some(correlation_id),
        };
        parent
            .deliver("nested-worker", reply(MsgId::new()))
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(150), parent.next_message())
                .await
                .is_err(),
            "wrong Run reply must not wake the caller"
        );
        parent
            .deliver("nested-worker", reply(run_id.clone()))
            .await
            .unwrap();
        let terminal = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let message = parent.next_message().await.unwrap();
                if message.kind == InboxKind::Outcome {
                    break message;
                }
            }
        })
        .await
        .expect("correlated canonical reply completes Run");
        assert_eq!(terminal.correlation_id.as_ref(), Some(&run_id));
        let outcome: bamboo_subagent::ChildOutcome = serde_json::from_value(terminal.body).unwrap();
        assert_eq!(outcome.status, bamboo_subagent::TerminalStatus::Completed);
        assert_eq!(outcome.result.as_deref(), Some("canonical-result"));
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

        // Preload the durable mailbox before the worker's deliberately short
        // idle deadline is armed. The delivery receipt is the readiness
        // boundary: startup scheduling can no longer consume the idle window
        // while the parent is still connecting and enqueueing the run.
        parent.deliver("busy-worker", request).await.unwrap();

        let idle_timeout = Duration::from_millis(100);
        let worker_endpoint = endpoint.clone();
        let mut worker = tokio::spawn({
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
                    Some(idle_timeout),
                )
                .await
            }
        });

        // Handler entry is the admission boundary for the behavior under test.
        tokio::time::timeout(Duration::from_secs(2), started.notified())
            .await
            .expect("run starts");

        assert!(
            tokio::time::timeout(idle_timeout * 2, &mut worker)
                .await
                .is_err(),
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
        let (endpoint, _dir, core, connections) = start_executor_connections().await;
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

        connections.abort();
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
            let PushItem::Message(message) = replay
                .control
                .try_recv()
                .expect("duplicate remains durable")
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
        assert!(deliver_approval_request(
            deliver.deliver("full-parent", message),
            &waiters,
            "rejected",
            &CancellationToken::new(),
        )
        .await
        .is_err());
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
                    deliver.deliver("silent-parent", message),
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
