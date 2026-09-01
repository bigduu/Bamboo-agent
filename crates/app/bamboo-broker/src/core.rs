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

use bamboo_subagent::{ActorEventBatch, ActorEventQos, InboxMessage, Mailbox, MsgId};
use tokio::sync::{mpsc, Mutex, OwnedSemaphorePermit, RwLock, Semaphore};

use crate::error::{BrokerError, BrokerResult};

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
}

impl BrokerCore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            subscribers: RwLock::new(HashMap::new()),
            max_pending_per_mailbox: DEFAULT_MAX_PENDING_PER_MAILBOX,
            pending_counts: Mutex::new(HashMap::new()),
            event_queue_capacity: DEFAULT_EVENT_QUEUE_CAPACITY,
            dropped_event_batches: AtomicU64::new(0),
        }
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

    /// Register a subscriber for `session_id` and return the stream of pushed
    /// messages. Immediately re-pushes crash leftovers (`recover`) then any
    /// pending backlog (`drain`). A prior subscriber for the same id is replaced.
    pub async fn subscribe(
        &self,
        session_id: &str,
        role: Option<&str>,
    ) -> BrokerResult<mpsc::UnboundedReceiver<PushItem>> {
        self.subscribe_streams(session_id, role, false)
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
        self.subscribe_streams(session_id, role, true).await
    }

    async fn subscribe_streams(
        &self,
        session_id: &str,
        role: Option<&str>,
        ordered_actor_events: bool,
    ) -> BrokerResult<(SubscriptionStreams, SubscriptionLease)> {
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let live_event_capacity = Arc::new(Semaphore::new(self.event_queue_capacity));
        let lease = SubscriptionLease {
            control_sink: control_tx.clone(),
        };
        self.subscribers.write().await.insert(
            session_id.to_string(),
            Subscriber {
                control_sink: control_tx.clone(),
                event_sink: event_tx.clone(),
                live_event_capacity,
                ordered_actor_events,
                role: role.map(str::to_string),
            },
        );

        let mb = self.mailbox(session_id);
        // Crash leftovers first (claimed-but-unacked from a previous connection),
        // then newly delivered, all in time order.
        let preload: BrokerResult<()> = async {
            for d in mb.recover().await? {
                if ordered_actor_events && is_ordered_actor_message(&d.msg) {
                    let _ = event_tx.send(EventPush::Durable(d.msg));
                } else {
                    let _ = control_tx.send(PushItem::Message(d.msg));
                }
            }
            for d in mb.drain().await? {
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
