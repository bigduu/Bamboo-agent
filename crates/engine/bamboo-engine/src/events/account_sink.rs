//! Account-wide durable change-feed sink.
//!
//! This is the single choke point the feed is built around. The three existing
//! per-session event paths (the interactive execute forwarder, the synchronous
//! `publish_replayable_session_event` helper, and the engine forwarder) each
//! also hand their events to this sink via [`AccountEventSink::record`] (or
//! [`AccountEventSink::inbox`] for the dependency-free engine path).
//!
//! A **single writer task** owns sequence allocation, journal append, and the
//! live broadcast — in that order, per event — so "journal order == broadcast
//! order == seq order" holds trivially without scattering the invariant across
//! concurrent callers.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bamboo_agent_core::AgentEvent;
use chrono::Utc;
use tokio::sync::{broadcast, mpsc};

use super::change_feed::ChangeEvent;
use super::journal::EventJournal;

/// Bounded inbox to the writer task. Change events are low-volume, so this
/// should never fill; a full inbox drops with a warning rather than blocking a
/// forwarder.
const INBOX_CAPACITY: usize = 1024;

/// Live-tail ring capacity. Larger than the per-session channel because the
/// account feed multiplexes all sessions and a bigger ring reduces `Lagged`
/// for resuming clients (the journal is the durable backstop regardless).
const BROADCAST_CAPACITY: usize = 4096;

/// Boot-time retention: keep at most this many journal files (each ~8 MiB), so
/// disk stays bounded (~512 MiB) without semantic compaction. Clients whose
/// cursor falls below the retained window full-resync via `feed_reset`.
const RETAINED_FILES: usize = 64;

/// An event accepted by the sink before seq/ts assignment: `(session_id,
/// event)`. The session id is the caller's known routing context.
pub type PendingEvent = (Option<String>, AgentEvent);

/// Account-wide durable change-feed sink. Construct once per [`AppState`].
pub struct AccountEventSink {
    /// Last-assigned seq. The writer task owns assignment; this is exposed only
    /// for diagnostics ([`Self::latest_seq`]).
    seq: Arc<AtomicU64>,
    /// Inbox to the writer task.
    tx: mpsc::Sender<PendingEvent>,
    /// Live account tail. `Arc` keeps fan-out to many subscribers cheap.
    broadcast: broadcast::Sender<Arc<ChangeEvent>>,
    /// Journal directory, for stateless replay reads on the `/stream` path.
    events_dir: PathBuf,
    /// Count of events dropped due to a full inbox (should remain 0).
    dropped: Arc<AtomicU64>,
}

impl AccountEventSink {
    /// Open the journal (recovering the max seq) and spawn the writer task.
    pub fn new(events_dir: PathBuf) -> std::io::Result<Arc<Self>> {
        // Boot-time retention pruning before opening the writer.
        if let Err(e) = super::journal::prune(&events_dir, RETAINED_FILES) {
            tracing::warn!("change-feed journal prune failed: {e}");
        }
        let delivered_lifecycle_ids = super::journal::read_since(&events_dir, 0)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|change| lifecycle_event_id(&change.event).map(str::to_string))
            .collect::<HashSet<_>>();
        let (journal, max_seq) = EventJournal::open(events_dir.clone())?;
        let seq = Arc::new(AtomicU64::new(max_seq));
        let (tx, rx) = mpsc::channel(INBOX_CAPACITY);
        let (btx, _brx) = broadcast::channel(BROADCAST_CAPACITY);

        let sink = Arc::new(Self {
            seq: seq.clone(),
            tx,
            broadcast: btx.clone(),
            events_dir,
            dropped: Arc::new(AtomicU64::new(0)),
        });

        tokio::spawn(writer_loop(rx, journal, seq, btx, delivered_lifecycle_ids));
        Ok(sink)
    }

    /// Record an event onto the change feed, if it is durable.
    ///
    /// `session_id` is the caller's known session context (forwarders are
    /// per-session) and wins over [`AgentEvent::session_id`] so terminal events
    /// (`Complete`/`Cancelled`/`Error`, which carry no id) still route to the
    /// right session. Never blocks: ephemeral events are filtered out before
    /// any clone, then the durable event is `try_send`-ed.
    pub fn record(&self, session_id: Option<&str>, event: &AgentEvent) {
        if !event.is_durable_change() {
            return;
        }
        let sid = session_id
            .map(|s| s.to_string())
            .or_else(|| event.session_id().map(|s| s.to_string()));
        if self.tx.try_send((sid, event.clone())).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            tracing::warn!("account change-feed inbox full or closed; event dropped");
        }
    }

    /// Clone of the writer inbox, for dependency-free callers (the engine
    /// forwarder) that cannot reference `AppState`/this type's `record`. Such
    /// callers must filter with [`AgentEvent::is_durable_change`] before
    /// sending so ephemeral token traffic never crosses the channel.
    pub fn inbox(&self) -> mpsc::Sender<PendingEvent> {
        self.tx.clone()
    }

    /// Subscribe to the live account tail. Subscribe *before* reading the
    /// journal on the `/stream` path so the replay→live handoff cannot gap.
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<ChangeEvent>> {
        self.broadcast.subscribe()
    }

    /// Journal directory, for stateless replay reads.
    pub fn events_dir(&self) -> &std::path::Path {
        &self.events_dir
    }

    /// Last-assigned seq (0 if none yet).
    pub fn latest_seq(&self) -> u64 {
        self.seq.load(Ordering::SeqCst)
    }

    /// Number of events dropped due to a full inbox (diagnostics).
    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

fn lifecycle_event_id(event: &AgentEvent) -> Option<&str> {
    match event {
        AgentEvent::WorkflowActivated { event_id, .. }
        | AgentEvent::WorkflowDeactivated { event_id, .. } => Some(event_id),
        _ => None,
    }
}

async fn writer_loop(
    mut rx: mpsc::Receiver<PendingEvent>,
    mut journal: EventJournal,
    seq: Arc<AtomicU64>,
    broadcast: broadcast::Sender<Arc<ChangeEvent>>,
    mut delivered_lifecycle_ids: HashSet<String>,
) {
    while let Some((session_id, event)) = rx.recv().await {
        if lifecycle_event_id(&event)
            .is_some_and(|event_id| !delivered_lifecycle_ids.insert(event_id.to_string()))
        {
            tracing::debug!("duplicate workflow lifecycle event suppressed");
            continue;
        }
        // Single writer → fetch_add is the seq allocator. 1-based.
        let next = seq.fetch_add(1, Ordering::SeqCst) + 1;
        let ce = ChangeEvent {
            seq: next,
            ts: Utc::now(),
            session_id,
            event,
        };
        if let Err(e) = journal.append(&ce) {
            tracing::error!("failed to append change event {} to journal: {e}", ce.seq);
        }
        // Lossy by design: with no live subscribers, or if all lag, the durable
        // journal remains the source of truth for resume.
        let _ = broadcast.send(Arc::new(ce));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::journal;

    fn deletion(id: &str) -> AgentEvent {
        AgentEvent::SessionDeleted {
            session_id: id.to_string(),
        }
    }

    fn activation(event_id: &str) -> AgentEvent {
        AgentEvent::WorkflowActivated {
            event_id: event_id.to_string(),
            session_id: "session".to_string(),
            workflow_id: "review".to_string(),
            revision: 7,
            invoked_by: "model".to_string(),
        }
    }

    #[tokio::test]
    async fn lifecycle_ids_are_deduped_live_and_after_sink_restart() {
        let dir = tempfile::tempdir().unwrap();
        let sink = AccountEventSink::new(dir.path().to_path_buf()).unwrap();
        let mut rx = sink.subscribe();
        let event = activation("stable-activation-id");
        sink.record(Some("session"), &event);
        sink.record(Some("session"), &event);
        let first = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
            .await
            .expect("first event")
            .expect("broadcast");
        assert_eq!(
            lifecycle_event_id(&first.event),
            Some("stable-activation-id")
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), rx.recv())
                .await
                .is_err()
        );
        drop(rx);
        drop(sink);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let restarted = AccountEventSink::new(dir.path().to_path_buf()).unwrap();
        let mut restarted_rx = restarted.subscribe();
        restarted.record(Some("session"), &event);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), restarted_rx.recv())
                .await
                .is_err()
        );
        assert_eq!(restarted.latest_seq(), 1);
    }

    #[tokio::test]
    async fn assigns_monotonic_seq_and_journals() {
        let dir = tempfile::tempdir().unwrap();
        let sink = AccountEventSink::new(dir.path().to_path_buf()).unwrap();

        let mut rx = sink.subscribe();
        for i in 0..3 {
            sink.record(None, &deletion(&format!("s{i}")));
        }

        // Live tail delivers seq 1,2,3 in order.
        for expected in 1..=3u64 {
            let ce = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
                .await
                .expect("not timed out")
                .expect("not closed");
            assert_eq!(ce.seq, expected);
        }

        // Journal has the same three, in order.
        let journaled = journal::read_since(sink.events_dir(), 0).unwrap();
        assert_eq!(
            journaled.iter().map(|e| e.seq).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(sink.latest_seq(), 3);
    }

    #[tokio::test]
    async fn ephemeral_events_are_dropped_from_feed() {
        let dir = tempfile::tempdir().unwrap();
        let sink = AccountEventSink::new(dir.path().to_path_buf()).unwrap();

        sink.record(
            Some("s1"),
            &AgentEvent::Token {
                content: "hi".into(),
            },
        );
        sink.record(Some("s1"), &deletion("s1"));

        // Give the writer a moment.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let journaled = journal::read_since(sink.events_dir(), 0).unwrap();
        assert_eq!(journaled.len(), 1, "only the durable event is journaled");
        assert_eq!(journaled[0].seq, 1);
    }

    #[tokio::test]
    async fn terminal_event_routes_via_caller_session_id() {
        let dir = tempfile::tempdir().unwrap();
        let sink = AccountEventSink::new(dir.path().to_path_buf()).unwrap();

        sink.record(
            Some("sess-7"),
            &AgentEvent::Complete {
                usage: Default::default(),
            },
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let journaled = journal::read_since(sink.events_dir(), 0).unwrap();
        assert_eq!(journaled.len(), 1);
        assert_eq!(journaled[0].session_id.as_deref(), Some("sess-7"));
    }
}
