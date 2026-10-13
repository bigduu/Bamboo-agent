//! Broker routing core: per-session durable mailboxes + push subscriptions.
//!
//! Transport-agnostic (no WebSocket here) so it is unit-testable in-process. The
//! WS server is a thin shell over this: a connection's `Deliver` calls
//! [`BrokerCore::deliver`], `Subscribe` calls [`BrokerCore::subscribe`] and
//! forwards the returned stream as `Message` frames, `Ack` calls
//! [`BrokerCore::ack`].
//!
//! Durability + delivery semantics come straight from the underlying
//! [`Mailbox`] (maildir, atomic, crash-safe, at-least-once): `deliver` persists
//! before returning; `subscribe` first re-pushes crash leftovers (`recover`),
//! then claims pending (`drain`); each subsequent `deliver` claims-and-pushes the
//! new message. A pushed-but-unacked message stays in `cur/` and is re-pushed on
//! the next `subscribe` — consumers dedupe by [`MsgId`].

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bamboo_subagent::{
    ActorEventBatch, ActorEventQos, Delivered, InboxKind, InboxMessage, Mailbox, MsgId,
};
use chrono::{DateTime, Utc};
use tokio::sync::{mpsc, Mutex, OnceCell, OwnedSemaphorePermit, RwLock, Semaphore};

use crate::error::{BrokerError, BrokerResult};
use crate::proto::{FencedRunEnvelope, WorkerHostObservation};

fn fenced_run_for_observation(
    message: &InboxMessage,
    to: &str,
    observation: Option<&WorkerHostObservation>,
) -> bool {
    if message.kind != InboxKind::FencedRun {
        return true;
    }
    serde_json::from_value::<FencedRunEnvelope>(message.body.clone())
        .ok()
        .zip(observation)
        .is_some_and(|(envelope, observation)| envelope.matches_observation(to, observation))
}

fn stale_fenced_run() -> BrokerError {
    BrokerError::Protocol("fenced Run target connection changed or is unavailable".into())
}

/// A replacement authenticated connection proves the old generation can no
/// longer receive a Run. Validate the full old envelope before retiring it;
/// malformed or current-generation messages remain untouched for diagnosis.
fn confirmed_stale_fenced_run_generation(
    message: &InboxMessage,
    to: &str,
    current: &WorkerHostObservation,
) -> Option<String> {
    if message.kind != InboxKind::FencedRun
        || current.mailbox != to
        || current.connection_generation.is_empty()
    {
        return None;
    }
    let envelope = serde_json::from_value::<FencedRunEnvelope>(message.body.clone()).ok()?;
    if envelope.recipient_connection_generation == current.connection_generation {
        return None;
    }
    // Check the old envelope's own destination and Run lease constraints with
    // the current credential's validity. Only the new generation is trusted
    // as live; the old host and role here are validation inputs, not authority.
    let old_target = WorkerHostObservation {
        host_ref: envelope.recipient_host_ref.clone(),
        mailbox: to.to_owned(),
        role: Some(envelope.recipient_role.clone()),
        credential_expires_at: current.credential_expires_at,
        connection_generation: envelope.recipient_connection_generation.clone(),
        host_capabilities: None,
        max_slots: None,
        environment_lease_v1: true,
    };
    envelope
        .matches_observation(to, &old_target)
        .then_some(envelope.recipient_connection_generation)
}

fn is_ordered_actor_message(message: &InboxMessage) -> bool {
    matches!(
        message.kind,
        bamboo_subagent::InboxKind::Event | bamboo_subagent::InboxKind::Outcome
    )
}

/// Default cap on a single mailbox's pending (undelivered-or-unacked) message
/// count (#53). Generous enough for durable actor boundaries and ordinary
/// mailbox bursts; live token/snapshot batches no longer touch Maildir. This
/// bounds worst-case disk use for offline sessions rather than throttling live
/// streams. Override via [`BrokerCore::with_max_pending_per_mailbox`].
pub const DEFAULT_MAX_PENDING_PER_MAILBOX: usize = 50_000;

/// Maximum live actor-event batches buffered for one subscriber. With the
/// protocol's 64-event batch cap this bounds each actor link independently;
/// overload drops a batch and is detected through its sequence range.
pub const DEFAULT_EVENT_QUEUE_CAPACITY: usize = 64;

/// An item pushed to a live subscriber's control sink. Both variants bypass the
/// bounded actor-data lane, so cancellation and ordinary mailbox traffic cannot
/// be starved by token streams. A `Cancel` never touches the mailbox. #50.
#[derive(Debug)]
pub enum PushItem {
    /// A durable message claimed from the subscriber's mailbox.
    Message(InboxMessage),
    /// Out-of-band cancel for the in-flight run correlated to this id.
    Cancel(MsgId),
}

/// One ordered actor event item. Durable mailbox Events and lossy live batches
/// share this queue so a later durable boundary cannot overtake earlier tokens.
/// Only `Live` holds a semaphore permit, bounding high-volume data without
/// weakening durable delivery.
#[derive(Debug)]
pub(crate) enum EventPush {
    Durable(InboxMessage),
    Live {
        correlation_id: MsgId,
        batch: ActorEventBatch,
        _permit: OwnedSemaphorePermit,
    },
}

/// Result of publishing onto the bounded live event lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventPublishOutcome {
    Published,
    Offline,
    Dropped,
    Rejected,
}

/// A live subscriber: its push sink plus the role it announced at handshake.
/// The subscriber table doubles as the live-actor registry — presence here is
/// connection-truth (subscribed == reachable now), fresher than any lease.
struct Subscriber {
    control_sink: mpsc::UnboundedSender<PushItem>,
    event_sink: mpsc::UnboundedSender<EventPush>,
    live_event_capacity: Arc<Semaphore>,
    ordered_actor_events: bool,
    /// Role announced in the `Hello` (`subagent_type`), if any — lets the bus
    /// answer "which connected actors serve role X" without a separate registry.
    role: Option<String>,
    /// Only set after a scoped Subscribe has completed its backlog preload.
    host_observation: Option<WorkerHostObservation>,
}

/// The host identity comes only from an authenticated operator PeerPolicy.
pub(crate) struct AuthenticatedHost {
    pub host_ref: String,
    pub credential_expires_at: DateTime<Utc>,
    pub host_capabilities: Option<bamboo_domain::WorkerHostCapabilities>,
    pub max_slots: Option<u16>,
}

/// Opaque proof that one server connection installed the current subscriber.
/// Cleanup compares the underlying channel identity so an older connection
/// cannot unregister a newer replacement for the same session. #788.
pub(crate) struct SubscriptionLease {
    control_sink: mpsc::UnboundedSender<PushItem>,
}

pub(crate) struct SubscriptionStreams {
    pub control: mpsc::UnboundedReceiver<PushItem>,
    pub events: mpsc::UnboundedReceiver<EventPush>,
}

/// In-process routing engine: owns the mailbox root and the live subscriber table.
pub struct BrokerCore {
    root: PathBuf,
    /// Exactly one durable identity publication/read per server process.
    identity: OnceCell<String>,
    /// session_id -> live subscriber. Present only while a client is subscribed.
    subscribers: RwLock<HashMap<String, Subscriber>>,
    /// Per-mailbox pending-message cap (#53); see
    /// [`DEFAULT_MAX_PENDING_PER_MAILBOX`].
    max_pending_per_mailbox: usize,
    /// Live per-mailbox pending-message counter, seeded from a single
    /// directory scan the first time a session is touched by THIS
    /// `BrokerCore` (via [`pending_count_for`](Self::pending_count_for)),
    /// then kept current in-memory on every `deliver` (+1) / `ack` that
    /// actually removes a message (-1) — instead of rescanning `new/` +
    /// `cur/` on every single `deliver` call. The scan-per-call design was
    /// O(backlog) work on every write, so a legitimate burst against a
    /// lagging subscriber paid ~O(backlog²) total directory-scan work
    /// climbing toward the cap (review finding #2 on #491/#53).
    pending_counts: Mutex<HashMap<String, usize>>,
    event_queue_capacity: usize,
    dropped_event_batches: AtomicU64,
    retired_fenced_runs: AtomicU64,
}

impl BrokerCore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            identity: OnceCell::new(),
            subscribers: RwLock::new(HashMap::new()),
            max_pending_per_mailbox: DEFAULT_MAX_PENDING_PER_MAILBOX,
            pending_counts: Mutex::new(HashMap::new()),
            event_queue_capacity: DEFAULT_EVENT_QUEUE_CAPACITY,
            dropped_event_batches: AtomicU64::new(0),
            retired_fenced_runs: AtomicU64::new(0),
        }
    }

    /// Stable identity of this Maildir namespace. Publish a fully synced
    /// temporary file with a no-replace hard link: concurrent handshakes must
    /// never observe an empty identity between create_new and write_all.
    pub async fn broker_identity(&self) -> BrokerResult<String> {
        self.identity
            .get_or_try_init(|| async {
                let root = self.root.clone();
                tokio::task::spawn_blocking(move || -> BrokerResult<String> {
                    use std::io::Write;
                    std::fs::create_dir_all(&root).map_err(|error| {
                        BrokerError::Transport(format!("broker identity directory: {error}"))
                    })?;
                    let path = root.join(".broker-maildir-identity-v1");
                    if !path.exists() {
                        let candidate = uuid::Uuid::new_v4().to_string();
                        let temporary = root.join(format!(
                            ".broker-maildir-identity-v1.{}.tmp",
                            uuid::Uuid::new_v4()
                        ));
                        let mut file = std::fs::OpenOptions::new()
                            .write(true)
                            .create_new(true)
                            .open(&temporary)
                            .map_err(|error| {
                                BrokerError::Transport(format!(
                                    "broker identity temp create: {error}"
                                ))
                            })?;
                        let write_result = file
                            .write_all(candidate.as_bytes())
                            .and_then(|_| file.sync_all());
                        drop(file);
                        if let Err(error) = write_result {
                            let _ = std::fs::remove_file(&temporary);
                            return Err(BrokerError::Transport(format!(
                                "broker identity temp write: {error}"
                            )));
                        }
                        let published = std::fs::hard_link(&temporary, &path);
                        let _ = std::fs::remove_file(&temporary);
                        match published {
                            Ok(()) => {}
                            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                            Err(error) => {
                                return Err(BrokerError::Transport(format!(
                                    "broker identity publish: {error}"
                                )));
                            }
                        }
                    }
                    // Also sync on the losing side of a concurrent publish. Neither
                    // connection may advertise a root identity before its directory
                    // entry is durable.
                    #[cfg(unix)]
                    std::fs::File::open(&root)
                        .and_then(|directory| directory.sync_all())
                        .map_err(|error| {
                            BrokerError::Transport(format!("broker identity sync: {error}"))
                        })?;
                    let raw = std::fs::read_to_string(&path).map_err(|error| {
                        BrokerError::Transport(format!("broker identity read: {error}"))
                    })?;
                    let parsed = uuid::Uuid::parse_str(&raw).map_err(|_| {
                        BrokerError::Protocol("invalid persistent broker identity".into())
                    })?;
                    Ok(parsed.to_string())
                })
                .await
                .map_err(|error| BrokerError::Transport(format!("broker identity task: {error}")))?
            })
            .await
            .cloned()
    }

    /// Disjoint transport namespace; no legacy backlog adoption or migration.
    pub fn new_scoped(root: impl Into<PathBuf>) -> Self {
        Self::new(root.into().join("scoped-peers-v1"))
    }

    /// Override the per-mailbox pending-message cap (#53) from
    /// [`DEFAULT_MAX_PENDING_PER_MAILBOX`]. Builder-style — chain onto
    /// [`Self::new`] before wrapping in `Arc`.
    pub fn with_max_pending_per_mailbox(mut self, max: usize) -> Self {
        self.max_pending_per_mailbox = max;
        self
    }

    pub fn with_event_queue_capacity(mut self, capacity: usize) -> Self {
        self.event_queue_capacity = capacity.max(1);
        self
    }

    /// Mailbox for one session: `<root>/mailboxes/<session_id>`.
    fn mailbox(&self, session_id: &str) -> Mailbox {
        Mailbox::at(self.root.join("mailboxes").join(session_id))
    }

    /// Durably enqueue `msg` into `to`'s mailbox, then — if `to` is currently
    /// subscribed — claim and push it immediately. Returns the stored [`MsgId`].
    ///
    /// Rejects with [`BrokerError::MailboxFull`] once `to`'s mailbox already
    /// holds `max_pending_per_mailbox` pending messages (#53) — a backlog cap
    /// against a flood aimed at an offline/never-draining mailbox. The check
    /// races benignly with concurrent delivers (worst case a few messages
    /// over the cap land before the count is next observed); it is a
    /// best-effort bound, not a hard invariant.
    pub async fn deliver(&self, to: &str, msg: &InboxMessage) -> BrokerResult<MsgId> {
        if msg.kind == InboxKind::LeasedRun {
            return Err(BrokerError::Protocol(
                "legacy leased Run lacks a WorkerHost connection fence".into(),
            ));
        }
        if msg.kind == InboxKind::Run
            && serde_json::from_value::<bamboo_subagent::RunSpec>(msg.body.clone())
                .ok()
                .and_then(|run| run.permission_policy)
                .and_then(|policy| policy.environment_lease)
                .is_some()
        {
            return Err(BrokerError::Protocol(
                "remote EnvironmentLease Run requires a WorkerHost connection fence".into(),
            ));
        }
        if msg.kind == InboxKind::FencedRun {
            let subscribers = self.subscribers.read().await;
            let observation = subscribers
                .get(to)
                .and_then(|subscriber| subscriber.host_observation.as_ref());
            if !fenced_run_for_observation(msg, to, observation) {
                return Err(stale_fenced_run());
            }
        }
        let pending = self.pending_count_for(to).await?;
        if pending >= self.max_pending_per_mailbox {
            return Err(BrokerError::MailboxFull {
                session: to.to_string(),
                limit: self.max_pending_per_mailbox,
            });
        }
        let id = self.mailbox(to).deliver(msg).await?;
        *self
            .pending_counts
            .lock()
            .await
            .entry(to.to_string())
            .or_insert(0) += 1;
        self.push_new(to).await?;
        Ok(id)
    }

    /// Current pending count for `session_id`'s mailbox — an in-memory
    /// HashMap lookup after the first call for that session, not a directory
    /// scan (see [`Self::pending_counts`]). The first call for a given
    /// session performs the ONE directory scan (via
    /// [`Mailbox::pending_count`]) that seeds its baseline, deliberately done
    /// OFF the map lock (it's the only await here that touches disk) so a
    /// slow scan for one mailbox never stalls lookups for others.
    ///
    /// Concurrent first-touches of the same never-before-seen mailbox can
    /// race this seed — both scan, both see the same pre-write disk state,
    /// and `or_insert` keeps whichever wins, while EVERY caller still applies
    /// its own subsequent `+1`/`-1` on top — so the final count stays
    /// correct regardless of which scan wins. No less precise than the
    /// scan-per-call design's own already-documented benign races.
    async fn pending_count_for(&self, session_id: &str) -> BrokerResult<usize> {
        if let Some(&c) = self.pending_counts.lock().await.get(session_id) {
            return Ok(c);
        }
        let scanned = self.mailbox(session_id).pending_count().await?;
        let mut counts = self.pending_counts.lock().await;
        Ok(*counts.entry(session_id.to_string()).or_insert(scanned))
    }

    /// Route a FencedRun using the current subscriber, with validation and
    /// enqueue under one read lock. A replacement cannot take ownership
    /// between the generation check and the send. Preload supplies its own
    /// sink and candidate observation while its public observation is hidden;
    /// a superseded preload may neither send nor retire a message.
    async fn route_fenced_run(
        &self,
        session_id: &str,
        delivered: &Delivered,
        preload_sink: Option<&mpsc::UnboundedSender<PushItem>>,
        preload_observation: Option<&WorkerHostObservation>,
    ) -> BrokerResult<()> {
        debug_assert!(preload_observation.is_none() || preload_sink.is_some());
        let subscribers = self.subscribers.read().await;
        let Some(current) = subscribers.get(session_id) else {
            return Ok(());
        };
        if preload_sink.is_some_and(|sink| !current.control_sink.same_channel(sink)) {
            return Ok(());
        }
        let observation = current.host_observation.as_ref().or(preload_observation);
        if fenced_run_for_observation(&delivered.msg, session_id, observation) {
            let _ = current
                .control_sink
                .send(PushItem::Message(delivered.msg.clone()));
            return Ok(());
        }
        let Some(observation) = observation else {
            return Ok(());
        };
        let Some(old_generation) =
            confirmed_stale_fenced_run_generation(&delivered.msg, session_id, observation)
        else {
            return Ok(());
        };
        // Seed before removal, just as ACK does, then decrement exactly once
        // only if this particular cur/ entry was actually deleted.
        self.pending_count_for(session_id).await?;
        if self
            .mailbox(session_id)
            .ack_delivered_if_present(delivered)
            .await?
        {
            if let Some(count) = self.pending_counts.lock().await.get_mut(session_id) {
                *count = count.saturating_sub(1);
            }
            let total = self.retired_fenced_runs.fetch_add(1, Ordering::Relaxed) + 1;
            tracing::info!(
                mailbox = session_id,
                message_id = %delivered.msg.id.0,
                old_generation = %old_generation,
                current_generation = %observation.connection_generation,
                retired_total = total,
                "Retired FencedRun for superseded WorkerHost connection"
            );
        }
        Ok(())
    }

    /// Register a subscriber for `session_id` and return the stream of pushed
    /// messages. Immediately re-pushes crash leftovers (`recover`) then any
    /// pending backlog (`drain`). A prior subscriber for the same id is replaced.
    pub async fn subscribe(
        &self,
        session_id: &str,
        role: Option<&str>,
    ) -> BrokerResult<mpsc::UnboundedReceiver<PushItem>> {
        self.subscribe_streams(session_id, role, false, None, false)
            .await
            .map(|(streams, _lease)| streams.control)
    }

    /// Server-facing subscription API that returns connection-scoped
    /// ownership. The lease must be presented to [`unsubscribe_if_owner`] so a
    /// stale/replaced connection cannot delete the current subscriber.
    pub(crate) async fn subscribe_with_lease(
        &self,
        session_id: &str,
        role: Option<&str>,
    ) -> BrokerResult<(SubscriptionStreams, SubscriptionLease)> {
        self.subscribe_streams(session_id, role, true, None, false)
            .await
    }

    pub(crate) async fn subscribe_scoped_with_lease(
        &self,
        session_id: &str,
        role: Option<&str>,
        host: AuthenticatedHost,
    ) -> BrokerResult<(SubscriptionStreams, SubscriptionLease)> {
        self.subscribe_streams(session_id, role, true, Some(host), false)
            .await
    }

    pub(crate) async fn subscribe_scoped_environment_lease_v1(
        &self,
        session_id: &str,
        role: Option<&str>,
        host: AuthenticatedHost,
    ) -> BrokerResult<(SubscriptionStreams, SubscriptionLease)> {
        self.subscribe_streams(session_id, role, true, Some(host), true)
            .await
    }

    async fn subscribe_streams(
        &self,
        session_id: &str,
        role: Option<&str>,
        ordered_actor_events: bool,
        host: Option<AuthenticatedHost>,
        environment_lease_v1: bool,
    ) -> BrokerResult<(SubscriptionStreams, SubscriptionLease)> {
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let live_event_capacity = Arc::new(Semaphore::new(self.event_queue_capacity));
        let lease = SubscriptionLease {
            control_sink: control_tx.clone(),
        };
        // Generate the trusted connection identity before replay. Do not make
        // it observable to senders until backlog preload completes.
        let host_observation = host.map(|host| WorkerHostObservation {
            host_ref: host.host_ref,
            mailbox: session_id.to_owned(),
            role: role.map(str::to_string),
            credential_expires_at: host.credential_expires_at,
            connection_generation: MsgId::new().0,
            host_capabilities: host.host_capabilities,
            max_slots: host.max_slots,
            environment_lease_v1,
        });
        self.subscribers.write().await.insert(
            session_id.to_string(),
            Subscriber {
                control_sink: control_tx.clone(),
                event_sink: event_tx.clone(),
                live_event_capacity,
                ordered_actor_events,
                role: role.map(str::to_string),
                host_observation: None,
            },
        );

        let mb = self.mailbox(session_id);
        // Crash leftovers first (claimed-but-unacked from a previous connection),
        // then newly delivered, all in time order.
        let preload: BrokerResult<()> = async {
            for d in mb.recover().await? {
                if d.msg.kind == InboxKind::LeasedRun {
                    continue;
                }
                if d.msg.kind == InboxKind::FencedRun {
                    self.route_fenced_run(
                        session_id,
                        &d,
                        Some(&control_tx),
                        host_observation.as_ref(),
                    )
                    .await?;
                    continue;
                }
                if ordered_actor_events && is_ordered_actor_message(&d.msg) {
                    let _ = event_tx.send(EventPush::Durable(d.msg));
                } else {
                    let _ = control_tx.send(PushItem::Message(d.msg));
                }
            }
            for d in mb.drain().await? {
                if d.msg.kind == InboxKind::LeasedRun {
                    continue;
                }
                if d.msg.kind == InboxKind::FencedRun {
                    self.route_fenced_run(
                        session_id,
                        &d,
                        Some(&control_tx),
                        host_observation.as_ref(),
                    )
                    .await?;
                    continue;
                }
                if ordered_actor_events && is_ordered_actor_message(&d.msg) {
                    let _ = event_tx.send(EventPush::Durable(d.msg));
                } else {
                    let _ = control_tx.send(PushItem::Message(d.msg));
                }
            }
            Ok(())
        }
        .await;
        if let Err(error) = preload {
            self.unsubscribe_if_owner(session_id, &lease).await;
            return Err(error);
        }
        if let Some(host_observation) = host_observation {
            let mut subscribers = self.subscribers.write().await;
            if let Some(current) = subscribers.get_mut(session_id) {
                if current.control_sink.same_channel(&lease.control_sink) {
                    current.host_observation = Some(host_observation);
                }
            }
        }
        Ok((
            SubscriptionStreams {
                control: control_rx,
                events: event_rx,
            },
            lease,
        ))
    }

    /// Publish a non-durable actor event batch to the target's bounded live
    /// lane. No filesystem operation occurs here. Full queues drop the current
    /// batch and increment a metric; the next delivered `first_seq` exposes the
    /// gap so the host/UI can reload authoritative state.
    pub async fn publish_event_batch(
        &self,
        to: &str,
        correlation_id: &MsgId,
        batch: ActorEventBatch,
    ) -> EventPublishOutcome {
        if batch.validate().is_err() || batch.qos == ActorEventQos::Durable {
            return EventPublishOutcome::Rejected;
        }
        let (sink, capacity) = {
            let subscribers = self.subscribers.read().await;
            let Some(subscriber) = subscribers.get(to) else {
                return EventPublishOutcome::Offline;
            };
            (
                subscriber.event_sink.clone(),
                Arc::clone(&subscriber.live_event_capacity),
            )
        };
        let permit = match capacity.try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                self.dropped_event_batches.fetch_add(1, Ordering::Relaxed);
                return EventPublishOutcome::Dropped;
            }
        };
        match sink.send(EventPush::Live {
            correlation_id: correlation_id.clone(),
            batch,
            _permit: permit,
        }) {
            Ok(()) => EventPublishOutcome::Published,
            Err(_) => EventPublishOutcome::Offline,
        }
    }

    pub fn dropped_event_batches(&self) -> u64 {
        self.dropped_event_batches.load(Ordering::Relaxed)
    }

    /// Number of obsolete FencedRuns physically removed by this broker process.
    /// Each removal is also logged with the mailbox and message identity.
    pub fn retired_fenced_runs(&self) -> u64 {
        self.retired_fenced_runs.load(Ordering::Relaxed)
    }

    /// Out-of-band cancel: if `to` is currently subscribed, push an ephemeral
    /// [`PushItem::Cancel`] to its live sink. Does NOT touch the mailbox (not
    /// durable, never claimed/acked/recovered), so a cancel can never queue
    /// behind the very work it cancels. Returns true iff a live subscriber
    /// received it (a cancel for an offline session is a meaningless no-op — the
    /// run isn't happening). #50.
    pub async fn cancel(&self, to: &str, correlation_id: &MsgId) -> bool {
        let subs = self.subscribers.read().await;
        match subs.get(to) {
            Some(sub) => sub
                .control_sink
                .send(PushItem::Cancel(correlation_id.clone()))
                .is_ok(),
            None => false,
        }
    }

    /// Drop the subscriber for `session_id` (connection closed). Unacked messages
    /// remain in `cur/` for redelivery on the next subscribe.
    pub async fn unsubscribe(&self, session_id: &str) {
        self.subscribers.write().await.remove(session_id);
    }

    /// Remove `session_id` only if `lease` still owns its subscriber entry.
    /// Returns whether this call removed the entry. This is the connection-safe
    /// counterpart to unconditional [`unsubscribe`](Self::unsubscribe).
    pub(crate) async fn unsubscribe_if_owner(
        &self,
        session_id: &str,
        lease: &SubscriptionLease,
    ) -> bool {
        let mut subscribers = self.subscribers.write().await;
        let owns_current = subscribers
            .get(session_id)
            .is_some_and(|subscriber| subscriber.control_sink.same_channel(&lease.control_sink));
        if owns_current {
            subscribers.remove(session_id);
        }
        owns_current
    }

    /// Acknowledge a processed message: delete it from `session_id`'s mailbox,
    /// and — if it was actually removed — decrement the live pending counter
    /// (#53 follow-up; see [`Self::pending_counts`]).
    pub async fn ack(&self, session_id: &str, id: &MsgId) -> BrokerResult<()> {
        // Seed the counter from a scan BEFORE the delete below if this
        // session hasn't been touched by this `BrokerCore` yet — the
        // baseline must include the message we're about to remove, or the
        // decrement would double-count it (seeding AFTER the delete would
        // scan a count that already excludes it).
        let _ = self.pending_count_for(session_id).await;
        let removed = self.mailbox(session_id).ack(id).await?;
        if removed {
            if let Some(c) = self.pending_counts.lock().await.get_mut(session_id) {
                *c = c.saturating_sub(1);
            }
        }
        Ok(())
    }

    /// Authorize the current connection at admission only. An admitted remove
    /// may finish after subscriber replacement; this is not a filesystem lease.
    pub(crate) async fn ack_current(
        &self,
        session_id: &str,
        id: &MsgId,
        lease: &SubscriptionLease,
    ) -> BrokerResult<()> {
        let owns = self
            .subscribers
            .read()
            .await
            .get(session_id)
            .is_some_and(|s| s.control_sink.same_channel(&lease.control_sink));
        if !owns {
            return Err(BrokerError::Auth("scoped peer admission denied".into()));
        }
        self.ack(session_id, id).await
    }

    /// True if a client is currently subscribed to `session_id`.
    pub async fn is_subscribed(&self, session_id: &str) -> bool {
        self.subscribers.read().await.contains_key(session_id)
    }

    /// Reclaim orphan mailbox dirs: delete every mailbox that is EMPTY and has NO
    /// live subscriber. Each child run leaves a one-shot parent-link mailbox
    /// (`p-<child>`) behind, and a killed pool worker's mailbox lingers — all
    /// empty after their acks. An empty, unsubscribed mailbox holds no work and
    /// is re-created on the next deliver/subscribe, so deleting it is lossless.
    /// Returns the count purged.
    ///
    /// The subscriber lock is held only BRIEFLY (to snapshot ids, and to re-check
    /// each candidate before removal) — never across the filesystem sweep. Every
    /// deliver/subscribe/cancel also takes that lock, so holding it across the
    /// blocking `read_dir`/`is_fully_empty`/`remove_dir_all` would stall all bus
    /// routing for the whole sweep; the fs work also runs on `spawn_blocking` so
    /// it never blocks a tokio worker thread. See #344.
    pub async fn gc_empty_mailboxes(&self) -> usize {
        let root = self.root.join("mailboxes");

        // Snapshot the currently-subscribed ids under a brief lock, then release.
        let subscribed: std::collections::HashSet<String> =
            self.subscribers.read().await.keys().cloned().collect();

        // Phase 1 (off-lock, off-worker): find empty, unsubscribed candidate dirs.
        let scan_root = root.clone();
        let candidates: Vec<std::path::PathBuf> = tokio::task::spawn_blocking(move || {
            let mut out = Vec::new();
            let Ok(entries) = std::fs::read_dir(&scan_root) else {
                return out;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if !path.is_dir() {
                    continue;
                }
                let id = entry.file_name().to_string_lossy().into_owned();
                if subscribed.contains(&id) {
                    continue; // a live subscriber owns it — keep.
                }
                if Mailbox::at(&path).is_fully_empty() {
                    out.push(path);
                }
            }
            out
        })
        .await
        .unwrap_or_default();

        // Phase 2: re-check subscription (brief lock, closing the
        // delete-vs-subscribe race) AND emptiness (re-checked atomically with the
        // remove, closing the deliver-vs-delete race), then remove off-lock.
        let mut purged = 0;
        for path in candidates {
            let id = path
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default();
            if self.subscribers.read().await.contains_key(&id) {
                continue; // subscribed since the snapshot — keep.
            }
            // Re-check emptiness and remove back-to-back in one blocking call (no
            // await between them), so a message delivered to this mailbox since
            // the Phase-1 scan is never silently deleted — matching the original
            // synchronous `is_fully_empty` → `remove_dir_all`.
            let removed = tokio::task::spawn_blocking(move || {
                Mailbox::at(&path).is_fully_empty() && std::fs::remove_dir_all(&path).is_ok()
            })
            .await
            .unwrap_or(false);
            if removed {
                // Drop the stale counter entry too, so `pending_counts` doesn't
                // grow unbounded in lockstep with the mailbox dirs it just
                // stopped tracking (#53 follow-up). The dir was confirmed
                // empty immediately before removal, so its live count (if
                // seeded at all) is 0 — nothing here relies on that, but it
                // means dropping the entry loses no information; the next
                // deliver/ack for this id re-seeds fresh from disk.
                self.pending_counts.lock().await.remove(&id);
                purged += 1;
            }
        }
        purged
    }

    /// Spawn a background sweep that reclaims empty, unsubscribed mailbox dirs
    /// every `interval` (see [`gc_empty_mailboxes`](Self::gc_empty_mailboxes)).
    /// Returns the task handle — abort it to stop the sweep.
    pub fn spawn_mailbox_gc(self: Arc<Self>, interval: Duration) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.tick().await; // consume the immediate first tick
            loop {
                tick.tick().await;
                let n = self.gc_empty_mailboxes().await;
                if n > 0 {
                    tracing::debug!("broker mailbox GC reclaimed {n} empty mailbox(es)");
                }
            }
        })
    }

    /// Every currently-connected actor as `(mailbox_id, role)` — the bus IS the
    /// live-actor registry (presence is connection-truth, no leases). Replaces
    /// "list workers" reads against the file/HTTP registries.
    pub async fn connected(&self) -> Vec<(String, Option<String>)> {
        self.subscribers
            .read()
            .await
            .iter()
            .map(|(id, sub)| (id.clone(), sub.role.clone()))
            .collect()
    }

    /// Mailbox ids of every connected actor announcing `role` — the bus-native
    /// answer to "find a live actor of role X" (replaces the registry discover +
    /// lease-liveness + connect-fail-failover dance for schedulable selection).
    pub async fn connected_by_role(&self, role: &str) -> Vec<String> {
        self.subscribers
            .read()
            .await
            .iter()
            .filter(|(_, sub)| sub.role.as_deref() == Some(role))
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Current trusted scoped connection only. Ordinary presence never grants
    /// a host identity, and an expired credential is unavailable immediately.
    pub(crate) async fn current_host_observation(
        &self,
        mailbox: &str,
        role: &str,
    ) -> Option<WorkerHostObservation> {
        let subscribers = self.subscribers.read().await;
        subscribers
            .get(mailbox)
            .and_then(|subscriber| subscriber.host_observation.as_ref())
            .filter(|observation| {
                observation.role.as_deref() == Some(role)
                    && observation.credential_expires_at > Utc::now()
            })
            .cloned()
    }

    /// Claim newly-delivered messages for `session_id` and push to its live
    /// subscriber. No-op when no one is subscribed (the message stays durably in
    /// `new/` until someone subscribes). Does NOT `recover` — in-flight `cur/`
    /// messages are only re-pushed on a fresh `subscribe`, so a live subscriber
    /// is not spammed with not-yet-acked duplicates.
    async fn push_new(&self, session_id: &str) -> BrokerResult<()> {
        let (control_tx, event_tx, ordered_actor_events) = {
            let subs = self.subscribers.read().await;
            match subs.get(session_id) {
                Some(sub) => (
                    sub.control_sink.clone(),
                    sub.event_sink.clone(),
                    sub.ordered_actor_events,
                ),
                None => return Ok(()),
            }
        };
        for d in self.mailbox(session_id).drain().await? {
            if d.msg.kind == InboxKind::LeasedRun {
                continue;
            }
            if d.msg.kind == InboxKind::FencedRun {
                self.route_fenced_run(session_id, &d, None, None).await?;
                continue;
            }
            if ordered_actor_events && is_ordered_actor_message(&d.msg) {
                let _ = event_tx.send(EventPush::Durable(d.msg));
            } else {
                let _ = control_tx.send(PushItem::Message(d.msg));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod broker_identity_tests {
    use super::*;

    #[tokio::test]
    async fn identity_survives_core_restart_and_changes_with_maildir() {
        let same_root = tempfile::tempdir().unwrap();
        let first = BrokerCore::new(same_root.path());
        let id = first.broker_identity().await.unwrap();
        assert_eq!(
            BrokerCore::new(same_root.path())
                .broker_identity()
                .await
                .unwrap(),
            id
        );
        let other_root = tempfile::tempdir().unwrap();
        assert_ne!(
            BrokerCore::new(other_root.path())
                .broker_identity()
                .await
                .unwrap(),
            id
        );
    }

    #[tokio::test]
    async fn concurrent_cores_publish_one_complete_identity() {
        let root = tempfile::tempdir().unwrap();
        let mut tasks = Vec::new();
        for _ in 0..32 {
            let path = root.path().to_path_buf();
            tasks.push(tokio::spawn(async move {
                BrokerCore::new(path).broker_identity().await.unwrap()
            }));
        }
        let first = tasks.remove(0).await.unwrap();
        for task in tasks {
            assert_eq!(task.await.unwrap(), first);
        }
        assert_eq!(
            std::fs::read_to_string(root.path().join(".broker-maildir-identity-v1")).unwrap(),
            first
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_subagent::{AgentRef, InboxKind};
    use chrono::Utc;
    use tempfile::TempDir;

    fn msg(seq: u32) -> InboxMessage {
        InboxMessage {
            id: MsgId::new(),
            from: AgentRef {
                session_id: "from".into(),
                role: None,
            },
            kind: InboxKind::Ask,
            body: serde_json::json!({ "seq": seq }),
            created_at: Utc::now(),
            correlation_id: None,
        }
    }

    fn core() -> (TempDir, BrokerCore) {
        let d = TempDir::new().unwrap();
        let c = BrokerCore::new(d.path());
        (d, c)
    }

    fn event_batch(seq: u64, qos: ActorEventQos) -> ActorEventBatch {
        let event = match qos {
            ActorEventQos::Ephemeral => {
                serde_json::json!({"type":"token","content":seq.to_string()})
            }
            ActorEventQos::Snapshot => {
                serde_json::json!({"type":"runner_progress","session_id":"child","round_count":seq})
            }
            ActorEventQos::Durable => {
                serde_json::json!({"type":"tool_start","tool_call_id":"t"})
            }
        };
        ActorEventBatch {
            logical_session: None,
            activation_id: Some("activation".into()),
            execution_epoch: 1,
            source_node_id: None,
            source_actor_id: Some("worker".into()),
            first_seq: seq,
            last_seq: seq,
            qos,
            events: vec![event],
        }
    }

    fn expect_message(item: PushItem) -> InboxMessage {
        match item {
            PushItem::Message(m) => m,
            PushItem::Cancel(c) => panic!("expected a message, got Cancel({c:?})"),
        }
    }

    #[tokio::test]
    async fn deliver_then_subscribe_drains_backlog() {
        let (_d, c) = core();
        let m = msg(1);
        c.deliver("child", &m).await.unwrap();
        // not subscribed yet -> message waits durably
        assert!(!c.is_subscribed("child").await);

        let mut rx = c.subscribe("child", None).await.unwrap();
        let got = expect_message(rx.try_recv().expect("backlog delivered on subscribe"));
        assert_eq!(got.id, m.id);
    }

    #[tokio::test]
    async fn subscribe_then_deliver_pushes_live() {
        let (_d, c) = core();
        let mut rx = c.subscribe("child", None).await.unwrap();
        assert!(rx.try_recv().is_err()); // empty initially

        let m = msg(2);
        c.deliver("child", &m).await.unwrap();
        let got = expect_message(rx.recv().await.expect("live push"));
        assert_eq!(got.id, m.id);
    }

    #[tokio::test]
    async fn live_event_batch_bypasses_mailbox() {
        let (_d, c) = core();
        let (mut streams, _lease) = c.subscribe_with_lease("parent", None).await.unwrap();
        let correlation_id = MsgId::new();
        assert_eq!(
            c.publish_event_batch(
                "parent",
                &correlation_id,
                event_batch(1, ActorEventQos::Ephemeral),
            )
            .await,
            EventPublishOutcome::Published
        );
        let pushed = streams.events.recv().await.expect("live event batch");
        let EventPush::Live {
            correlation_id: pushed_correlation,
            batch,
            ..
        } = pushed
        else {
            panic!("expected live event batch");
        };
        assert_eq!(pushed_correlation, correlation_id);
        assert_eq!(batch.first_seq, 1);
        assert_eq!(c.mailbox("parent").pending_count().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn live_and_durable_actor_events_keep_source_order() {
        let (_d, c) = core();
        let (mut streams, _lease) = c.subscribe_with_lease("parent", None).await.unwrap();
        let correlation_id = MsgId::new();
        assert_eq!(
            c.publish_event_batch(
                "parent",
                &correlation_id,
                event_batch(1, ActorEventQos::Ephemeral),
            )
            .await,
            EventPublishOutcome::Published
        );
        let durable_batch = event_batch(2, ActorEventQos::Durable);
        let durable_message = InboxMessage {
            id: MsgId::new(),
            from: AgentRef {
                session_id: "worker".into(),
                role: None,
            },
            kind: InboxKind::Event,
            body: serde_json::to_value(durable_batch).unwrap(),
            created_at: Utc::now(),
            correlation_id: Some(correlation_id),
        };
        c.deliver("parent", &durable_message).await.unwrap();

        assert!(matches!(
            streams.events.recv().await,
            Some(EventPush::Live { batch, .. }) if batch.first_seq == 1
        ));
        assert!(matches!(
            streams.events.recv().await,
            Some(EventPush::Durable(message)) if message.id == durable_message.id
        ));
    }

    #[tokio::test]
    async fn saturated_event_lane_drops_data_but_not_control() {
        let dir = TempDir::new().unwrap();
        let c = BrokerCore::new(dir.path()).with_event_queue_capacity(1);
        let (mut streams, _lease) = c.subscribe_with_lease("parent", None).await.unwrap();
        let correlation_id = MsgId::new();
        assert_eq!(
            c.publish_event_batch(
                "parent",
                &correlation_id,
                event_batch(1, ActorEventQos::Ephemeral),
            )
            .await,
            EventPublishOutcome::Published
        );
        assert_eq!(
            c.publish_event_batch(
                "parent",
                &correlation_id,
                event_batch(2, ActorEventQos::Ephemeral),
            )
            .await,
            EventPublishOutcome::Dropped
        );
        assert_eq!(c.dropped_event_batches(), 1);

        assert!(c.cancel("parent", &correlation_id).await);
        assert!(matches!(
            streams.control.recv().await,
            Some(PushItem::Cancel(id)) if id == correlation_id
        ));
        assert_eq!(
            c.publish_event_batch(
                "parent",
                &correlation_id,
                event_batch(3, ActorEventQos::Durable),
            )
            .await,
            EventPublishOutcome::Rejected
        );
    }

    #[tokio::test]
    async fn ack_removes_so_resubscribe_does_not_redeliver() {
        let (_d, c) = core();
        let m = msg(3);
        c.deliver("child", &m).await.unwrap();
        let mut rx = c.subscribe("child", None).await.unwrap();
        let got = expect_message(rx.recv().await.unwrap());
        assert_eq!(got.id, m.id);

        // ack + drop subscription, then resubscribe: nothing redelivered.
        c.ack("child", &got.id).await.unwrap();
        c.unsubscribe("child").await;
        let mut rx2 = c.subscribe("child", None).await.unwrap();
        assert!(rx2.try_recv().is_err(), "acked message must not redeliver");
    }

    #[tokio::test]
    async fn unacked_message_redelivers_on_resubscribe() {
        let (_d, c) = core();
        let m = msg(4);
        c.deliver("child", &m).await.unwrap();
        let mut rx = c.subscribe("child", None).await.unwrap();
        let got = expect_message(rx.recv().await.unwrap()); // pushed, NOT acked
        assert_eq!(got.id, m.id);

        // connection drops without ack -> message stays in cur/ -> re-pushed.
        c.unsubscribe("child").await;
        let mut rx2 = c.subscribe("child", None).await.unwrap();
        let again = expect_message(rx2.try_recv().expect("unacked message redelivers"));
        assert_eq!(again.id, m.id);
    }

    #[tokio::test]
    async fn deliver_to_unsubscribed_is_durable_and_isolated_per_session() {
        let (_d, c) = core();
        c.deliver("a", &msg(1)).await.unwrap();
        c.deliver("b", &msg(2)).await.unwrap();
        // subscriber for "a" sees only a's mailbox.
        let mut rx_a = c.subscribe("a", None).await.unwrap();
        assert!(rx_a.try_recv().is_ok());
        assert!(rx_a.try_recv().is_err());
    }

    #[tokio::test]
    async fn cancel_pushes_control_item_without_touching_mailbox() {
        let (_d, c) = core();
        let cid = MsgId::new();

        // No live subscriber -> cancel is a meaningless no-op.
        assert!(!c.cancel("worker", &cid).await);

        // Subscribed -> the live subscriber receives a Cancel control item.
        let mut rx = c.subscribe("worker", None).await.unwrap();
        assert!(
            c.cancel("worker", &cid).await,
            "a live subscriber received the cancel"
        );
        match rx.try_recv().expect("cancel was pushed") {
            PushItem::Cancel(got) => assert_eq!(got, cid),
            PushItem::Message(_) => panic!("expected a Cancel, got a Message"),
        }

        // Out-of-band: the cancel left NO durable mailbox trace, so a fresh
        // subscribe re-pushes nothing (no new/ or cur/ entry was created).
        c.unsubscribe("worker").await;
        let mut rx2 = c.subscribe("worker", None).await.unwrap();
        assert!(
            rx2.try_recv().is_err(),
            "cancel must not persist anything to the mailbox"
        );
    }

    #[tokio::test]
    async fn subscriber_table_is_the_live_actor_registry() {
        let (_d, c) = core();
        let _a = c.subscribe("w1", Some("explorer")).await.unwrap();
        let _b = c.subscribe("w2", Some("explorer")).await.unwrap();
        let _r = c.subscribe("w3", Some("reviewer")).await.unwrap();
        let _n = c.subscribe("w4", None).await.unwrap();

        let mut explorers = c.connected_by_role("explorer").await;
        explorers.sort();
        assert_eq!(explorers, vec!["w1".to_string(), "w2".to_string()]);
        assert_eq!(
            c.connected_by_role("reviewer").await,
            vec!["w3".to_string()]
        );
        assert!(c.connected_by_role("missing").await.is_empty());
        assert_eq!(c.connected().await.len(), 4, "all four are live");

        // Presence is connection-truth: unsubscribing drops it from the registry.
        c.unsubscribe("w1").await;
        assert_eq!(
            c.connected_by_role("explorer").await,
            vec!["w2".to_string()]
        );
    }

    #[tokio::test]
    async fn trusted_host_observation_tracks_only_current_scoped_subscription() {
        let (dir, core) = core();
        let deadline = Utc::now() + chrono::Duration::minutes(1);
        let host = |name: &str, expiry| AuthenticatedHost {
            host_ref: name.into(),
            credential_expires_at: expiry,
            host_capabilities: None,
            max_slots: None,
        };
        let (_first_streams, first_lease) = core
            .subscribe_scoped_with_lease("worker", Some("gpu"), host("host-a", deadline))
            .await
            .unwrap();
        let first = core
            .current_host_observation("worker", "gpu")
            .await
            .unwrap();
        assert_eq!(first.host_ref, "host-a");
        assert_eq!(first.mailbox, "worker");
        assert_eq!(first.role.as_deref(), Some("gpu"));
        assert_eq!(first.credential_expires_at, deadline);
        assert!(!first.connection_generation.is_empty());
        assert!(core
            .current_host_observation("worker", "other")
            .await
            .is_none());

        let (_replacement_streams, replacement_lease) = core
            .subscribe_scoped_with_lease("worker", Some("gpu"), host("host-b", deadline))
            .await
            .unwrap();
        let replacement = core
            .current_host_observation("worker", "gpu")
            .await
            .unwrap();
        assert_eq!(replacement.host_ref, "host-b");
        assert_ne!(
            replacement.connection_generation,
            first.connection_generation
        );
        assert!(!core.unsubscribe_if_owner("worker", &first_lease).await);
        assert_eq!(
            core.current_host_observation("worker", "gpu").await,
            Some(replacement)
        );
        assert!(
            core.unsubscribe_if_owner("worker", &replacement_lease)
                .await
        );
        assert!(core
            .current_host_observation("worker", "gpu")
            .await
            .is_none());

        let (_lease_streams, lease_owner) = core
            .subscribe_scoped_environment_lease_v1(
                "worker",
                Some("gpu"),
                host("lease-host", deadline),
            )
            .await
            .unwrap();
        assert!(
            core.current_host_observation("worker", "gpu")
                .await
                .unwrap()
                .environment_lease_v1
        );
        assert!(core.unsubscribe_if_owner("worker", &lease_owner).await);

        let (_legacy, legacy_lease) = core
            .subscribe_with_lease("worker", Some("gpu"))
            .await
            .unwrap();
        assert!(core
            .current_host_observation("worker", "gpu")
            .await
            .is_none());
        assert!(core.unsubscribe_if_owner("worker", &legacy_lease).await);
        core.subscribe_scoped_with_lease(
            "worker",
            Some("gpu"),
            host("expired", Utc::now() - chrono::Duration::seconds(1)),
        )
        .await
        .unwrap();
        assert!(core
            .current_host_observation("worker", "gpu")
            .await
            .is_none());
        assert!(BrokerCore::new(dir.path())
            .current_host_observation("worker", "gpu")
            .await
            .is_none());
    }

    fn test_host() -> AuthenticatedHost {
        AuthenticatedHost {
            host_ref: "same-host".into(),
            credential_expires_at: Utc::now() + chrono::Duration::minutes(5),
            host_capabilities: None,
            max_slots: None,
        }
    }

    fn test_fenced_run(observation: &WorkerHostObservation) -> InboxMessage {
        let run: bamboo_subagent::RunSpec = serde_json::from_value(serde_json::json!({
            "assignment":"bounded work",
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
        InboxMessage {
            id: MsgId::new(),
            from: AgentRef {
                session_id: "parent".into(),
                role: Some("host".into()),
            },
            kind: InboxKind::FencedRun,
            body: serde_json::to_value(
                FencedRunEnvelope::for_observation(run, observation).unwrap(),
            )
            .unwrap(),
            created_at: Utc::now(),
            correlation_id: None,
        }
    }

    #[tokio::test]
    async fn broker_restart_retires_unacked_fenced_run_from_old_generation() {
        let (dir, original) = core();
        let (mut first_streams, _first_lease) = original
            .subscribe_scoped_environment_lease_v1("worker", Some("worker"), test_host())
            .await
            .unwrap();
        let first = original
            .current_host_observation("worker", "worker")
            .await
            .unwrap();
        let old_run = test_fenced_run(&first);
        original.deliver("worker", &old_run).await.unwrap();
        assert_eq!(
            expect_message(first_streams.control.recv().await.unwrap()).id,
            old_run.id
        );
        // The Run is durably claimed but has no ACK when the broker restarts.
        drop(first_streams);
        drop(original);

        let restarted = BrokerCore::new(dir.path());
        let (mut successor_streams, _successor_lease) = restarted
            .subscribe_scoped_environment_lease_v1("worker", Some("worker"), test_host())
            .await
            .unwrap();
        let successor = restarted
            .current_host_observation("worker", "worker")
            .await
            .unwrap();
        assert_eq!(successor.host_ref, first.host_ref);
        assert_ne!(successor.connection_generation, first.connection_generation);
        assert!(successor_streams.control.try_recv().is_err());
        assert_eq!(restarted.pending_count_for("worker").await.unwrap(), 0);
        assert_eq!(
            restarted.mailbox("worker").pending_count().await.unwrap(),
            0
        );
        assert_eq!(restarted.retired_fenced_runs(), 1);
    }

    #[tokio::test]
    async fn push_new_retires_only_old_generation_and_frees_mailbox_capacity() {
        let (dir, original) = core();
        original
            .subscribe_scoped_environment_lease_v1("worker", Some("worker"), test_host())
            .await
            .unwrap();
        let old = original
            .current_host_observation("worker", "worker")
            .await
            .unwrap();
        drop(original);

        let current = BrokerCore::new(dir.path()).with_max_pending_per_mailbox(1);
        let (mut streams, _lease) = current
            .subscribe_scoped_environment_lease_v1("worker", Some("worker"), test_host())
            .await
            .unwrap();
        let observation = current
            .current_host_observation("worker", "worker")
            .await
            .unwrap();
        assert_ne!(old.connection_generation, observation.connection_generation);

        // Model a Run published before replacement but first claimed by the
        // new subscriber's push path. It must not consume the only quota slot.
        current
            .mailbox("worker")
            .deliver(&test_fenced_run(&old))
            .await
            .unwrap();
        current.push_new("worker").await.unwrap();
        assert_eq!(current.pending_count_for("worker").await.unwrap(), 0);
        assert_eq!(current.mailbox("worker").pending_count().await.unwrap(), 0);
        assert_eq!(current.retired_fenced_runs(), 1);
        assert!(streams.control.try_recv().is_err());

        let active = test_fenced_run(&observation);
        current.deliver("worker", &active).await.unwrap();
        assert_eq!(
            expect_message(streams.control.recv().await.unwrap()).id,
            active.id
        );
        assert_eq!(current.pending_count_for("worker").await.unwrap(), 1);
        assert_eq!(current.mailbox("worker").pending_count().await.unwrap(), 1);
        assert_eq!(current.retired_fenced_runs(), 1);
    }

    #[tokio::test]
    async fn replaced_preload_cannot_send_or_retire_current_fenced_run() {
        let (_dir, core) = core();
        // Keep a completed successor subscription aside so the replacement
        // can be interleaved precisely after the first connection claims a Run.
        let (mut successor_streams, _successor_lease) = core
            .subscribe_scoped_environment_lease_v1("worker", Some("worker"), test_host())
            .await
            .unwrap();
        let successor = core
            .current_host_observation("worker", "worker")
            .await
            .unwrap();
        let successor_subscriber = core.subscribers.write().await.remove("worker").unwrap();

        let (mut first_streams, first_lease) = core
            .subscribe_scoped_environment_lease_v1("worker", Some("worker"), test_host())
            .await
            .unwrap();
        let first = core
            .current_host_observation("worker", "worker")
            .await
            .unwrap();
        let current_run = test_fenced_run(&successor);
        core.mailbox("worker").deliver(&current_run).await.unwrap();
        let claimed = core.mailbox("worker").drain().await.unwrap().pop().unwrap();

        core.subscribers
            .write()
            .await
            .insert("worker".into(), successor_subscriber);
        // The first preload holds its old candidate observation, but its sink
        // no longer owns the mailbox. It cannot send or retire this Run.
        core.route_fenced_run(
            "worker",
            &claimed,
            Some(&first_lease.control_sink),
            Some(&first),
        )
        .await
        .unwrap();
        assert!(first_streams.control.try_recv().is_err());
        assert!(successor_streams.control.try_recv().is_err());
        assert_eq!(core.retired_fenced_runs(), 0);
        assert_eq!(core.mailbox("worker").pending_count().await.unwrap(), 1);

        // A push that claimed this message before replacement now consults
        // the current subscriber, so the successor receives its own Run.
        core.route_fenced_run("worker", &claimed, None, None)
            .await
            .unwrap();
        assert_eq!(
            expect_message(successor_streams.control.recv().await.unwrap()).id,
            current_run.id
        );
        assert!(first_streams.control.try_recv().is_err());
        assert_eq!(core.mailbox("worker").pending_count().await.unwrap(), 1);

        let obsolete_run = test_fenced_run(&first);
        core.mailbox("worker").deliver(&obsolete_run).await.unwrap();
        core.push_new("worker").await.unwrap();
        assert!(first_streams.control.try_recv().is_err());
        assert!(successor_streams.control.try_recv().is_err());
        assert_eq!(core.retired_fenced_runs(), 1);
        assert_eq!(core.mailbox("worker").pending_count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn malformed_fenced_run_is_never_retired_as_a_stale_generation() {
        let (dir, core) = core();
        let mut unknown = msg(1);
        unknown.kind = InboxKind::FencedRun;
        unknown.body = serde_json::json!({ "version": 1 });
        core.mailbox("worker").deliver(&unknown).await.unwrap();

        let (mut streams, _lease) = core
            .subscribe_scoped_environment_lease_v1("worker", Some("worker"), test_host())
            .await
            .unwrap();
        assert!(streams.control.try_recv().is_err());
        assert_eq!(core.retired_fenced_runs(), 0);
        assert_eq!(core.pending_count_for("worker").await.unwrap(), 1);
        assert_eq!(core.mailbox("worker").recover().await.unwrap().len(), 1);
        assert_eq!(BrokerCore::new(dir.path()).retired_fenced_runs(), 0);
    }

    #[tokio::test]
    async fn gc_purges_empty_unsubscribed_mailboxes_only() {
        let (_d, c) = core();

        // "done": subscribe → deliver (claimed) → ack ⇒ empty dir; then unsubscribe.
        {
            let mut rx = c.subscribe("done", None).await.unwrap();
            let id = c.deliver("done", &msg(1)).await.unwrap();
            let _ = expect_message(rx.recv().await.unwrap());
            c.ack("done", &id).await.unwrap();
        }
        c.unsubscribe("done").await;

        // "pending": a delivered-but-unacked message ⇒ NON-empty ⇒ kept.
        let _ = c.deliver("pending", &msg(2)).await.unwrap();
        // "live": currently subscribed ⇒ kept even though empty.
        let _live = c.subscribe("live", None).await.unwrap();

        // Only the empty + unsubscribed mailbox is reclaimed.
        assert_eq!(c.gc_empty_mailboxes().await, 1);
        // pending (non-empty) + live (subscribed) survive a second sweep.
        assert_eq!(c.gc_empty_mailboxes().await, 0);
    }

    /// Mailbox-flood DoS defense (#53): once a session's mailbox holds
    /// `max_pending_per_mailbox` pending messages, further `deliver`s are
    /// rejected with [`BrokerError::MailboxFull`] rather than accepted
    /// unboundedly — the concrete vector is a client delivering to an
    /// offline/never-draining session to fill disk.
    #[tokio::test]
    async fn deliver_rejects_once_pending_cap_reached() {
        let d = TempDir::new().unwrap();
        let c = BrokerCore::new(d.path()).with_max_pending_per_mailbox(2);

        // No subscriber for "hoard" -> messages accumulate in new/, uncapped
        // until the cap check kicks in.
        c.deliver("hoard", &msg(1)).await.expect("1st under cap");
        c.deliver("hoard", &msg(2)).await.expect("2nd reaches cap");

        let err = c.deliver("hoard", &msg(3)).await;
        assert!(
            matches!(
                err,
                Err(BrokerError::MailboxFull {
                    ref session,
                    limit: 2
                }) if session == "hoard"
            ),
            "delivery beyond the cap must be rejected: {err:?}"
        );

        // A different session's mailbox is unaffected — the cap is per-session.
        c.deliver("other", &msg(4))
            .await
            .expect("cap is per-mailbox, not global");
    }

    /// The cap tracks the LIVE pending count, not a one-shot budget (#53):
    /// once a message is claimed+acked (freeing a slot), `deliver` succeeds
    /// again.
    #[tokio::test]
    async fn deliver_succeeds_again_after_ack_frees_a_slot() {
        let d = TempDir::new().unwrap();
        let c = BrokerCore::new(d.path()).with_max_pending_per_mailbox(1);

        let m1 = msg(1);
        c.deliver("hoard", &m1).await.expect("1st reaches cap");
        assert!(
            c.deliver("hoard", &msg(2)).await.is_err(),
            "2nd delivery is over the cap"
        );

        // Claim + ack the pending message, freeing its slot.
        let mut rx = c.subscribe("hoard", None).await.unwrap();
        let got = expect_message(rx.recv().await.unwrap());
        assert_eq!(got.id, m1.id);
        c.ack("hoard", &got.id).await.unwrap();

        c.deliver("hoard", &msg(3))
            .await
            .expect("delivery succeeds again once a slot is freed");
    }

    /// The pending counter's baseline for a session is SCANNED from disk on
    /// first touch (#53 follow-up: an in-memory counter replacing the old
    /// per-call directory rescan), not assumed to start at 0 — so a mailbox
    /// that already has a backlog when this `BrokerCore` starts (e.g. after a
    /// broker restart) is correctly capped from the very first `deliver`
    /// call, not just after enough in-process deliveries accumulate.
    #[tokio::test]
    async fn pending_count_seeds_from_preexisting_disk_backlog_on_first_touch() {
        let d = TempDir::new().unwrap();
        // Simulate pre-existing backlog: deliver 2 messages directly via a raw
        // `Mailbox` handle, bypassing `BrokerCore` entirely — as if a PRIOR
        // broker process wrote them before this `BrokerCore` ever started.
        let raw = Mailbox::at(d.path().join("mailboxes").join("hoard"));
        raw.deliver(&msg(1)).await.unwrap();
        raw.deliver(&msg(2)).await.unwrap();

        let c = BrokerCore::new(d.path()).with_max_pending_per_mailbox(2);
        // First-ever `deliver` call from THIS `BrokerCore` must already see
        // the 2 pre-existing messages and reject — not treat its in-memory
        // count as starting fresh at 0.
        let err = c.deliver("hoard", &msg(3)).await;
        assert!(
            matches!(err, Err(BrokerError::MailboxFull { limit: 2, .. })),
            "the cap must account for backlog that predates this BrokerCore, got {err:?}"
        );
    }

    /// The live in-memory counter (#53 follow-up, replacing a per-call
    /// directory rescan) must stay a FAITHFUL count under concurrent
    /// deliveries — not drift from the on-disk truth, and not corrupt itself
    /// (lost updates / double counts) under concurrent map access. Fires
    /// many concurrent `deliver`s at the same never-subscribed (so nothing
    /// drains it) mailbox and checks the in-memory count agrees with a
    /// fresh, direct on-disk scan afterward.
    ///
    /// NOTE: this deliberately does NOT assert on how many of the 100 calls
    /// succeeded vs. were `MailboxFull`-rejected. The cap's check-then-write
    /// is, and always has been, a documented BEST-EFFORT race, not a hard
    /// invariant (see `deliver`'s doc comment) — under enough concurrency,
    /// many callers can observe the same under-cap count before any of
    /// their writes lands, so "how many landed over the cap" is inherently
    /// a race outcome, not a fixed number this test can pin down. What
    /// MUST hold unconditionally is that the counter tracking whatever DID
    /// land is accurate.
    #[tokio::test]
    async fn pending_count_stays_consistent_with_disk_under_concurrent_delivers() {
        let d = TempDir::new().unwrap();
        let c = Arc::new(BrokerCore::new(d.path()).with_max_pending_per_mailbox(20));

        let mut handles = Vec::new();
        for i in 0..100u32 {
            let c = c.clone();
            handles.push(tokio::spawn(
                async move { c.deliver("hoard", &msg(i)).await },
            ));
        }
        let mut succeeded = 0usize;
        for h in handles {
            if h.await.unwrap().is_ok() {
                succeeded += 1;
            }
        }
        assert!(succeeded > 0, "at least some concurrent deliveries land");

        // The in-memory count must match reality: a direct on-disk scan (the
        // OLD, ground-truth mechanism) agrees with what the new in-memory
        // counter believes — no lost updates, no double counts.
        let on_disk = Mailbox::at(d.path().join("mailboxes").join("hoard"))
            .pending_count()
            .await
            .unwrap();
        assert_eq!(
            on_disk, succeeded,
            "the in-memory counter must not drift from the on-disk message count"
        );
    }
}
