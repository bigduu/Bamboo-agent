//! Account-wide durable change-feed sink.
//!
//! This is the single choke point the feed is built around. The three existing
//! per-session event paths (the interactive execute forwarder, the synchronous
//! `publish_replayable_session_event` helper, and the engine forwarder) each
//! also hand their events to this sink via [`AccountEventSink::record`] (or
//! [`AccountEventSink::inbox`] for the dependency-free engine path).
//!
//! A **single writer task per sink** owns its live broadcast ordering. Durable
//! sequence allocation and append are additionally serialized by one fixed
//! account-journal file claim, so rolling Bamboo processes cannot allocate the
//! same sequence or duplicate a confirmed transition.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bamboo_agent_core::AgentEvent;
use chrono::Utc;
use fs2::FileExt;
use tokio::sync::oneshot;
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
const JOURNAL_LOCK_FILE: &str = ".account-journal.lock";
const CONFIRMATION_TIMEOUT: Duration = Duration::from_secs(30);

/// An event accepted by the sink before seq/ts assignment: `(session_id,
/// event)`. The session id is the caller's known routing context.
pub type PendingEvent = (Option<String>, AgentEvent);

type ConfirmationWaiter = (u64, oneshot::Sender<bool>);
type ConfirmationWaiters = Arc<Mutex<HashMap<String, Vec<ConfirmationWaiter>>>>;
type LatestConfigStates = Arc<Mutex<HashMap<String, ConfigEventKind>>>;

struct WriterDedupState {
    delivered_lifecycle_ids: HashSet<String>,
    latest_config_states: LatestConfigStates,
    delivered_changed_ids: HashSet<String>,
    confirmation_waiters: ConfirmationWaiters,
}

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
    confirmation_waiters: ConfirmationWaiters,
    latest_config_states: LatestConfigStates,
    next_waiter_id: AtomicU64,
}

impl AccountEventSink {
    /// Open the journal (recovering the max seq) and spawn the writer task.
    pub fn new(events_dir: PathBuf) -> std::io::Result<Arc<Self>> {
        // Build the boot snapshot under the same fixed claim used by appends.
        // Otherwise a rolling process could observe the new max sequence but
        // miss the corresponding confirmation id and later duplicate it.
        let (durable_events, max_seq) = load_durable_snapshot(&events_dir)?;
        let delivered_lifecycle_ids = durable_events
            .iter()
            .filter_map(|change| lifecycle_event_id(&change.event))
            .collect::<HashSet<_>>();
        let mut durable_config_states = HashMap::new();
        for change in &durable_events {
            if let Some((key, kind)) = config_event_state(&change.event) {
                durable_config_states.insert(key, kind);
            }
        }
        let delivered_changed_ids = durable_events
            .iter()
            .filter_map(|change| config_event_id(&change.event))
            .collect::<HashSet<_>>();
        let seq = Arc::new(AtomicU64::new(max_seq));
        let (tx, rx) = mpsc::channel(INBOX_CAPACITY);
        let (btx, _brx) = broadcast::channel(BROADCAST_CAPACITY);
        let confirmation_waiters = Arc::new(Mutex::new(HashMap::new()));
        let latest_config_states = Arc::new(Mutex::new(durable_config_states));

        let writer_events_dir = events_dir.clone();
        let sink = Arc::new(Self {
            seq: seq.clone(),
            tx,
            broadcast: btx.clone(),
            events_dir,
            dropped: Arc::new(AtomicU64::new(0)),
            confirmation_waiters: Arc::clone(&confirmation_waiters),
            latest_config_states: Arc::clone(&latest_config_states),
            next_waiter_id: AtomicU64::new(1),
        });

        tokio::spawn(writer_loop(
            rx,
            writer_events_dir,
            seq,
            btx,
            WriterDedupState {
                delivered_lifecycle_ids,
                latest_config_states,
                delivered_changed_ids,
                confirmation_waiters,
            },
        ));
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

    /// Enqueue one event and wait until the single writer has either appended
    /// it durably or confirmed that the same idempotent lifecycle/configuration
    /// transition already exists in the journal. `false` is returned on
    /// queue/append failure, so callers must retain their durable outbox entry
    /// for a later retry.
    pub async fn record_confirmed(&self, session_id: Option<&str>, event: &AgentEvent) -> bool {
        self.record_confirmed_with_timeout(session_id, event, CONFIRMATION_TIMEOUT)
            .await
    }

    async fn record_confirmed_with_timeout(
        &self,
        session_id: Option<&str>,
        event: &AgentEvent,
        timeout: Duration,
    ) -> bool {
        let Some(confirmation_id) = event_confirmation_id(event) else {
            return false;
        };
        let sid = session_id
            .map(str::to_string)
            .or_else(|| event.session_id().map(str::to_string));
        let (receipt, confirmation) = oneshot::channel();
        let waiter_id = self.next_waiter_id.fetch_add(1, Ordering::Relaxed);
        self.confirmation_waiters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entry(confirmation_id.clone())
            .or_default()
            .push((waiter_id, receipt));
        // One deadline bounds both queue admission and journal confirmation.
        // A full inbox must not hold a session-create idempotency claim forever.
        let durable = tokio::time::timeout(timeout, async {
            if self.tx.send((sid, event.clone())).await.is_err() {
                return false;
            }
            confirmation.await.unwrap_or(false)
        })
        .await
        .unwrap_or(false);
        // Success normally removed the whole confirmation group in the writer;
        // send failure/timeout removes this waiter explicitly. If the event was
        // enqueued just before timeout, a late append remains safe and recovery
        // confirms it through durable dedupe.
        remove_confirmation_waiter(&self.confirmation_waiters, &confirmation_id, waiter_id);
        durable
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

    /// Whether the durable journal's latest transition for this exact config
    /// generation is Invalid. This state is rebuilt before the writer starts
    /// and updated immediately after every successful append.
    pub fn latest_config_transition_is_invalid(&self, section: &str, revision: u64) -> bool {
        self.latest_config_states
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&format!("{section}:{revision}"))
            == Some(&ConfigEventKind::Invalid)
    }
}

fn lifecycle_event_id(event: &AgentEvent) -> Option<String> {
    match event {
        AgentEvent::WorkflowActivated { event_id, .. }
        | AgentEvent::WorkflowDeactivated { event_id, .. } => Some(format!("workflow:{event_id}")),
        AgentEvent::SessionCreated { session_id, .. } => {
            Some(format!("session-created:{session_id}"))
        }
        _ => None,
    }
}

fn config_event_id(event: &AgentEvent) -> Option<String> {
    let (section, revision) = match event {
        AgentEvent::ConfigChanged { section, revision } => (section, revision),
        _ => return None,
    };
    Some(format!("{section}:{revision}"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConfigEventKind {
    Changed,
    Invalid,
    Recovered,
}

fn config_event_state(event: &AgentEvent) -> Option<(String, ConfigEventKind)> {
    let (section, revision, kind) = match event {
        AgentEvent::ConfigChanged { section, revision } => {
            (section, revision, ConfigEventKind::Changed)
        }
        AgentEvent::ConfigInvalid { section, revision } => {
            (section, revision, ConfigEventKind::Invalid)
        }
        AgentEvent::ConfigRecovered { section, revision } => {
            (section, revision, ConfigEventKind::Recovered)
        }
        _ => return None,
    };
    Some((format!("{section}:{revision}"), kind))
}

fn config_confirmation_id(event: &AgentEvent) -> Option<String> {
    let (key, kind) = config_event_state(event)?;
    let kind = match kind {
        ConfigEventKind::Changed => "changed",
        ConfigEventKind::Invalid => "invalid",
        ConfigEventKind::Recovered => "recovered",
    };
    Some(format!("{kind}:{key}"))
}

fn event_confirmation_id(event: &AgentEvent) -> Option<String> {
    config_confirmation_id(event).or_else(|| lifecycle_event_id(event))
}

fn complete_confirmation_waiters(waiters: &ConfirmationWaiters, config_id: &str, durable: bool) {
    let pending = waiters
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(config_id)
        .unwrap_or_default();
    for (_, waiter) in pending {
        let _ = waiter.send(durable);
    }
}

fn remove_confirmation_waiter(waiters: &ConfirmationWaiters, config_id: &str, waiter_id: u64) {
    let mut waiters = waiters
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut remove_entry = false;
    if let Some(pending) = waiters.get_mut(config_id) {
        pending.retain(|(id, _)| *id != waiter_id);
        remove_entry = pending.is_empty();
    }
    if remove_entry {
        waiters.remove(config_id);
    }
}

enum GlobalAppend {
    Existing {
        max_seq: u64,
        observed_events: Vec<ChangeEvent>,
    },
    Appended {
        event: Box<ChangeEvent>,
        observed_events: Vec<ChangeEvent>,
    },
}

fn exact_once_event_id(event: &AgentEvent) -> Option<String> {
    lifecycle_event_id(event).or_else(|| config_event_id(event))
}

fn duplicate_after_observed_delta(
    event: &AgentEvent,
    known_config_state: Option<ConfigEventKind>,
    observed_events: &[ChangeEvent],
) -> bool {
    if let Some(exact_id) = exact_once_event_id(event) {
        return observed_events.iter().any(|change| {
            exact_once_event_id(&change.event).as_deref() == Some(exact_id.as_str())
        });
    }
    let Some((key, desired_kind)) = config_event_state(event) else {
        return false;
    };
    observed_events
        .iter()
        .filter_map(|change| config_event_state(&change.event))
        .filter_map(|(observed_key, kind)| (observed_key == key).then_some(kind))
        .next_back()
        .or(known_config_state)
        == Some(desired_kind)
}

fn publish_observed_events(
    observed_events: Vec<ChangeEvent>,
    seq: &AtomicU64,
    broadcast: &broadcast::Sender<Arc<ChangeEvent>>,
    delivered_lifecycle_ids: &mut HashSet<String>,
    latest_config_states: &LatestConfigStates,
    delivered_changed_ids: &mut HashSet<String>,
) {
    let mut config_states = latest_config_states
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    for change in observed_events {
        if let Some(id) = lifecycle_event_id(&change.event) {
            delivered_lifecycle_ids.insert(id);
        }
        if let Some((key, kind)) = config_event_state(&change.event) {
            config_states.insert(key, kind);
        }
        if let Some(id) = config_event_id(&change.event) {
            delivered_changed_ids.insert(id);
        }
        // A rolling sibling may have appended this event since this sink's
        // last known sequence. Forward that durable delta into the local live
        // tail before publishing the event owned by this sink. This preserves
        // per-sink monotonic live ordering and lets a stale AppState observe an
        // exact transition that global dedupe found in the shared journal.
        seq.fetch_max(change.seq, Ordering::SeqCst);
        let _ = broadcast.send(Arc::new(change));
    }
}

fn load_durable_snapshot(events_dir: &std::path::Path) -> std::io::Result<(Vec<ChangeEvent>, u64)> {
    std::fs::create_dir_all(events_dir)?;
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(events_dir.join(JOURNAL_LOCK_FILE))?;
    FileExt::lock_exclusive(&lock)?;
    let result = (|| {
        if let Err(error) = super::journal::prune(events_dir, RETAINED_FILES) {
            tracing::warn!("change-feed journal prune failed: {error}");
        }
        // Writable recovery truncates a torn/invalid UTF-8 crash tail before
        // the snapshot reader walks complete durable lines.
        let (_, max_seq) = EventJournal::open_for_locked_append(events_dir.to_path_buf())?;
        let durable_events = super::journal::read_since(events_dir, 0)?;
        Ok((durable_events, max_seq))
    })();
    let _ = FileExt::unlock(&lock);
    result
}

/// Serialize durable journal publication across rolling Bamboo processes.
/// Confirmation-bearing events are re-checked from disk while the claim is
/// held, then a fresh disk-derived sequence is appended, flushed, and synced.
/// Events without a confirmation id share the append/sequence boundary but do
/// not gain semantic deduplication.
async fn append_with_global_claim(
    events_dir: PathBuf,
    session_id: Option<String>,
    event: AgentEvent,
    known_max_seq: u64,
    known_config_state: Option<ConfigEventKind>,
) -> std::io::Result<GlobalAppend> {
    tokio::task::spawn_blocking(move || {
        std::fs::create_dir_all(&events_dir)?;
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(events_dir.join(JOURNAL_LOCK_FILE))?;
        FileExt::lock_exclusive(&lock)?;

        let result = (|| {
            let (mut journal, max_seq) = EventJournal::open_for_locked_append(events_dir.clone())?;
            let observed_events = if max_seq > known_max_seq {
                super::journal::read_since(&events_dir, known_max_seq)?
            } else {
                Vec::new()
            };
            if duplicate_after_observed_delta(&event, known_config_state, &observed_events) {
                return Ok(GlobalAppend::Existing {
                    max_seq,
                    observed_events,
                });
            }

            let ce = ChangeEvent {
                seq: max_seq
                    .checked_add(1)
                    .ok_or_else(|| std::io::Error::other("account journal sequence exhausted"))?,
                ts: Utc::now(),
                session_id,
                event,
            };
            journal.append_synced(&ce)?;
            Ok(GlobalAppend::Appended {
                event: Box::new(ce),
                observed_events,
            })
        })();
        let _ = FileExt::unlock(&lock);
        result
    })
    .await
    .map_err(|error| std::io::Error::other(format!("join account journal task: {error}")))?
}

async fn writer_loop(
    mut rx: mpsc::Receiver<PendingEvent>,
    events_dir: PathBuf,
    seq: Arc<AtomicU64>,
    broadcast: broadcast::Sender<Arc<ChangeEvent>>,
    state: WriterDedupState,
) {
    let WriterDedupState {
        mut delivered_lifecycle_ids,
        latest_config_states,
        mut delivered_changed_ids,
        confirmation_waiters,
    } = state;
    while let Some((session_id, event)) = rx.recv().await {
        let lifecycle_id = lifecycle_event_id(&event);
        let config_id = config_event_id(&event);
        let config_state = config_event_state(&event);
        let confirmation_id = event_confirmation_id(&event);
        if lifecycle_id
            .as_ref()
            .is_some_and(|id| delivered_lifecycle_ids.contains(id))
            || config_id
                .as_ref()
                .is_some_and(|id| delivered_changed_ids.contains(id))
        {
            tracing::debug!("duplicate durable change event suppressed");
            if let Some(confirmation_id) = confirmation_id.as_deref() {
                complete_confirmation_waiters(&confirmation_waiters, confirmation_id, true);
            }
            continue;
        }
        let append = append_with_global_claim(
            events_dir.clone(),
            session_id,
            event,
            seq.load(Ordering::SeqCst),
            config_state.as_ref().and_then(|(key, _)| {
                latest_config_states
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .get(key)
                    .copied()
            }),
        )
        .await;
        let ce = match append {
            Ok(GlobalAppend::Existing {
                max_seq,
                observed_events,
            }) => {
                publish_observed_events(
                    observed_events,
                    seq.as_ref(),
                    &broadcast,
                    &mut delivered_lifecycle_ids,
                    &latest_config_states,
                    &mut delivered_changed_ids,
                );
                seq.fetch_max(max_seq, Ordering::SeqCst);
                if let Some(confirmation_id) = confirmation_id.as_deref() {
                    complete_confirmation_waiters(&confirmation_waiters, confirmation_id, true);
                }
                continue;
            }
            Ok(GlobalAppend::Appended {
                event,
                observed_events,
            }) => {
                publish_observed_events(
                    observed_events,
                    seq.as_ref(),
                    &broadcast,
                    &mut delivered_lifecycle_ids,
                    &latest_config_states,
                    &mut delivered_changed_ids,
                );
                seq.fetch_max(event.seq, Ordering::SeqCst);
                *event
            }
            Err(e) => {
                tracing::error!("failed to append durable change event to journal: {e}");
                if let Some(confirmation_id) = confirmation_id.as_deref() {
                    complete_confirmation_waiters(&confirmation_waiters, confirmation_id, false);
                }
                continue;
            }
        };
        if let Some(id) = lifecycle_id {
            delivered_lifecycle_ids.insert(id);
        }
        if let Some((key, kind)) = config_state {
            latest_config_states
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(key, kind);
        }
        if let Some(id) = config_id.as_ref() {
            delivered_changed_ids.insert(id.clone());
        }
        // Lossy by design: with no live subscribers, or if all lag, the durable
        // journal remains the source of truth for resume.
        let _ = broadcast.send(Arc::new(ce));
        if let Some(confirmation_id) = confirmation_id.as_deref() {
            complete_confirmation_waiters(&confirmation_waiters, confirmation_id, true);
        }
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

    fn session_created(session_id: &str) -> AgentEvent {
        AgentEvent::SessionCreated {
            session_id: session_id.to_string(),
            project_id: None,
            title: "New Session".to_string(),
            kind: bamboo_agent_core::SessionKind::Root,
            created_at: Utc::now(),
        }
    }

    fn config_changed(section: &str, revision: u64) -> AgentEvent {
        AgentEvent::ConfigChanged {
            section: section.to_string(),
            revision,
        }
    }

    fn config_invalid(section: &str, revision: u64) -> AgentEvent {
        AgentEvent::ConfigInvalid {
            section: section.to_string(),
            revision,
        }
    }

    fn config_recovered(section: &str, revision: u64) -> AgentEvent {
        AgentEvent::ConfigRecovered {
            section: section.to_string(),
            revision,
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
            Some("workflow:stable-activation-id".to_string())
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
    async fn confirmed_config_publication_is_deduped_after_journal_restart() {
        let dir = tempfile::tempdir().unwrap();
        let event = config_changed("core", 7);
        let sink = AccountEventSink::new(dir.path().to_path_buf()).unwrap();
        assert!(sink.record_confirmed(None, &event).await);
        assert_eq!(journal::read_since(sink.events_dir(), 0).unwrap().len(), 1);
        drop(sink);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let restarted = AccountEventSink::new(dir.path().to_path_buf()).unwrap();
        assert!(restarted.record_confirmed(None, &event).await);
        assert_eq!(
            journal::read_since(restarted.events_dir(), 0)
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn confirmed_session_created_is_deduped_by_session_id_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let event = session_created("stable-session-id");
        let sink = AccountEventSink::new(dir.path().to_path_buf()).unwrap();
        assert!(
            sink.record_confirmed(Some("stable-session-id"), &event)
                .await
        );
        assert!(
            sink.record_confirmed(Some("stable-session-id"), &event)
                .await
        );
        assert_eq!(journal::read_since(sink.events_dir(), 0).unwrap().len(), 1);
        drop(sink);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let restarted = AccountEventSink::new(dir.path().to_path_buf()).unwrap();
        assert!(
            restarted
                .record_confirmed(Some("stable-session-id"), &event)
                .await
        );
        assert_eq!(
            journal::read_since(restarted.events_dir(), 0)
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn concurrently_confirmed_stale_sinks_share_dedupe_and_sequence_boundary() {
        let dir = tempfile::tempdir().unwrap();
        // Both snapshots are intentionally constructed before either append.
        let first = AccountEventSink::new(dir.path().to_path_buf()).unwrap();
        let second = AccountEventSink::new(dir.path().to_path_buf()).unwrap();
        let same = session_created("rolling-stable-session");
        let (first_ok, second_ok) = tokio::join!(
            first.record_confirmed(Some("rolling-stable-session"), &same),
            second.record_confirmed(Some("rolling-stable-session"), &same),
        );
        assert!(first_ok && second_ok);
        let events = journal::read_since(dir.path(), 0).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].seq, 1);

        // Different confirmed events from those same stale sinks still derive
        // unique sequences from disk while holding the global append claim.
        let left = session_created("rolling-left");
        let right = session_created("rolling-right");
        let (left_ok, right_ok) = tokio::join!(
            first.record_confirmed(Some("rolling-left"), &left),
            second.record_confirmed(Some("rolling-right"), &right),
        );
        assert!(left_ok && right_ok);
        let events = journal::read_since(dir.path(), 0).unwrap();
        assert_eq!(events.len(), 3);
        let mut sequences = events.iter().map(|event| event.seq).collect::<Vec<_>>();
        sequences.sort_unstable();
        assert_eq!(sequences, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn stale_sink_forwards_an_existing_remote_event_to_its_local_live_tail() {
        let dir = tempfile::tempdir().unwrap();
        let remote = AccountEventSink::new(dir.path().to_path_buf()).unwrap();
        let stale = AccountEventSink::new(dir.path().to_path_buf()).unwrap();
        let event = session_created("rolling-existing-session");
        let mut live = stale.subscribe();

        assert!(
            remote
                .record_confirmed(Some("rolling-existing-session"), &event)
                .await
        );
        assert!(
            stale
                .record_confirmed(Some("rolling-existing-session"), &event)
                .await
        );

        let observed = tokio::time::timeout(Duration::from_secs(1), live.recv())
            .await
            .expect("stale sink forwards the durable remote delta")
            .expect("local live tail remains open");
        assert_eq!(observed.seq, 1);
        assert!(matches!(
            &observed.event,
            AgentEvent::SessionCreated { session_id, .. }
                if session_id == "rolling-existing-session"
        ));
        assert_eq!(journal::read_since(dir.path(), 0).unwrap().len(), 1);
        assert!(tokio::time::timeout(Duration::from_millis(50), live.recv())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn stale_sink_forwards_remote_delta_before_its_own_appended_event() {
        let dir = tempfile::tempdir().unwrap();
        let remote = AccountEventSink::new(dir.path().to_path_buf()).unwrap();
        let stale = AccountEventSink::new(dir.path().to_path_buf()).unwrap();
        let mut live = stale.subscribe();

        assert!(
            remote
                .record_confirmed(
                    Some("rolling-remote-session"),
                    &session_created("rolling-remote-session"),
                )
                .await
        );
        assert!(
            stale
                .record_confirmed(
                    Some("rolling-local-session"),
                    &session_created("rolling-local-session"),
                )
                .await
        );

        let first = tokio::time::timeout(Duration::from_secs(1), live.recv())
            .await
            .expect("remote delta")
            .expect("local live tail remains open");
        let second = tokio::time::timeout(Duration::from_secs(1), live.recv())
            .await
            .expect("locally appended event")
            .expect("local live tail remains open");
        assert_eq!((first.seq, second.seq), (1, 2));
        assert!(matches!(
            &first.event,
            AgentEvent::SessionCreated { session_id, .. }
                if session_id == "rolling-remote-session"
        ));
        assert!(matches!(
            &second.event,
            AgentEvent::SessionCreated { session_id, .. }
                if session_id == "rolling-local-session"
        ));
        assert_eq!(
            journal::read_since(dir.path(), 0)
                .unwrap()
                .iter()
                .map(|event| event.seq)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[tokio::test]
    async fn stale_sinks_fold_config_health_deltas_before_consecutive_dedupe() {
        let dir = tempfile::tempdir().unwrap();
        let first = AccountEventSink::new(dir.path().to_path_buf()).unwrap();
        let second = AccountEventSink::new(dir.path().to_path_buf()).unwrap();
        let invalid = config_invalid("mcp", 19);
        let recovered = config_recovered("mcp", 19);

        assert!(first.record_confirmed(None, &invalid).await);
        assert!(second.record_confirmed(None, &recovered).await);
        // `first` still remembers Invalid locally. It must observe the other
        // process's Recovered transition under the journal claim and append the
        // new Invalid rather than pre-suppressing from stale state.
        assert!(first.record_confirmed(None, &invalid).await);

        let events = journal::read_since(dir.path(), 0).unwrap();
        assert_eq!(events.len(), 3);
        assert!(matches!(events[0].event, AgentEvent::ConfigInvalid { .. }));
        assert!(matches!(
            events[1].event,
            AgentEvent::ConfigRecovered { .. }
        ));
        assert!(matches!(events[2].event, AgentEvent::ConfigInvalid { .. }));
        assert_eq!(
            events.iter().map(|event| event.seq).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    #[tokio::test]
    async fn locked_append_reuses_rotating_files_and_restart_retains_more_than_64_events() {
        let dir = tempfile::tempdir().unwrap();
        let sink = AccountEventSink::new(dir.path().to_path_buf()).unwrap();
        for index in 0..70 {
            let session_id = format!("retained-session-{index}");
            assert!(
                sink.record_confirmed(Some(&session_id), &session_created(&session_id))
                    .await
            );
        }
        assert_eq!(journal::read_since(dir.path(), 0).unwrap().len(), 70);
        drop(sink);

        let restarted = AccountEventSink::new(dir.path().to_path_buf()).unwrap();
        let retained = journal::read_since(restarted.events_dir(), 0).unwrap();
        assert_eq!(retained.len(), 70);
        let files = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("events-"))
            .count();
        assert!(files < 64, "70 small events must not become 70 files");
    }

    #[tokio::test]
    async fn restart_truncates_invalid_utf8_tail_and_preserves_monotonic_sequence() {
        use std::io::Write as _;

        let dir = tempfile::tempdir().unwrap();
        let first = AccountEventSink::new(dir.path().to_path_buf()).unwrap();
        assert!(
            first
                .record_confirmed(Some("utf8-first"), &session_created("utf8-first"))
                .await
        );
        drop(first);
        let journal_path = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| {
                path.file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with("events-"))
            })
            .unwrap();
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&journal_path)
            .unwrap();
        file.write_all(&[0xf0, 0x9f, 0x92]).unwrap();
        file.sync_data().unwrap();
        drop(file);

        let restarted = AccountEventSink::new(dir.path().to_path_buf()).unwrap();
        assert_eq!(journal::read_since(dir.path(), 0).unwrap().len(), 1);
        assert!(
            restarted
                .record_confirmed(Some("utf8-second"), &session_created("utf8-second"))
                .await
        );
        let events = journal::read_since(dir.path(), 0).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].seq, 1);
        assert_eq!(events[1].seq, 2);
    }

    #[tokio::test]
    async fn confirmed_config_publications_preserve_fifo_revision_order() {
        let dir = tempfile::tempdir().unwrap();
        let sink = AccountEventSink::new(dir.path().to_path_buf()).unwrap();
        let first = config_changed("core", 1);
        let second = config_changed("core", 2);
        let (first_confirmed, second_confirmed) = tokio::join!(
            sink.record_confirmed(None, &first),
            sink.record_confirmed(None, &second)
        );
        assert!(first_confirmed && second_confirmed);
        let events = journal::read_since(sink.events_dir(), 0).unwrap();
        assert!(matches!(
            &events[0].event,
            AgentEvent::ConfigChanged { revision: 1, .. }
        ));
        assert!(matches!(
            &events[1].event,
            AgentEvent::ConfigChanged { revision: 2, .. }
        ));
    }

    #[tokio::test]
    async fn config_health_state_dedupes_only_consecutive_same_kind_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        let sink = AccountEventSink::new(dir.path().to_path_buf()).unwrap();
        let invalid = config_invalid("mcp", 7);
        sink.record(None, &invalid);
        sink.record(None, &invalid);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(journal::read_since(sink.events_dir(), 0).unwrap().len(), 1);
        drop(sink);
        tokio::time::sleep(Duration::from_millis(20)).await;

        let restarted = AccountEventSink::new(dir.path().to_path_buf()).unwrap();
        restarted.record(None, &invalid);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            journal::read_since(restarted.events_dir(), 0)
                .unwrap()
                .len(),
            1,
            "same failure stays deduped after watcher/sink restart"
        );

        restarted.record(None, &config_recovered("mcp", 7));
        restarted.record(None, &invalid);
        restarted.record(None, &config_changed("mcp", 7));
        tokio::time::sleep(Duration::from_millis(50)).await;
        let events = journal::read_since(restarted.events_dir(), 0).unwrap();
        assert_eq!(events.len(), 4);
        assert!(matches!(
            events[1].event,
            AgentEvent::ConfigRecovered { .. }
        ));
        assert!(matches!(events[2].event, AgentEvent::ConfigInvalid { .. }));
        assert!(matches!(events[3].event, AgentEvent::ConfigChanged { .. }));
    }

    #[tokio::test]
    async fn confirmed_changed_is_exactly_once_even_after_same_revision_invalid_transition() {
        let dir = tempfile::tempdir().unwrap();
        let sink = AccountEventSink::new(dir.path().to_path_buf()).unwrap();
        let changed = config_changed("core", 11);
        assert!(sink.record_confirmed(None, &changed).await);
        sink.record(None, &config_invalid("core", 11));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(sink.record_confirmed(None, &changed).await);
        let events = journal::read_since(sink.events_dir(), 0).unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event.event,
                    AgentEvent::ConfigChanged { revision: 11, .. }
                ))
                .count(),
            1
        );
        assert_eq!(events.len(), 2);
    }

    #[tokio::test]
    async fn confirmed_invalid_is_kind_qualified_from_same_revision_changed() {
        let dir = tempfile::tempdir().unwrap();
        let sink = AccountEventSink::new(dir.path().to_path_buf()).unwrap();
        let changed = config_changed("core", 1);
        let invalid = config_invalid("core", 1);

        assert!(sink.record_confirmed(None, &changed).await);
        assert!(sink.record_confirmed(None, &invalid).await);
        assert!(sink.record_confirmed(None, &invalid).await);

        let events = journal::read_since(sink.events_dir(), 0).unwrap();
        assert_eq!(events.len(), 2);
        assert!(matches!(
            events[0].event,
            AgentEvent::ConfigChanged { revision: 1, .. }
        ));
        assert!(matches!(
            events[1].event,
            AgentEvent::ConfigInvalid { revision: 1, .. }
        ));
    }

    #[test]
    fn removing_a_closed_confirmation_waiter_clears_its_map_entry() {
        let waiters: ConfirmationWaiters = Arc::new(Mutex::new(HashMap::new()));
        let (sender, receiver) = oneshot::channel();
        drop(receiver);
        waiters
            .lock()
            .unwrap()
            .insert("core:1".to_string(), vec![(9, sender)]);
        remove_confirmation_waiter(&waiters, "core:1", 9);
        assert!(waiters.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn confirmed_record_bounds_full_inbox_wait_and_cleans_waiter() {
        let dir = tempfile::tempdir().unwrap();
        let (tx, _paused_rx) = mpsc::channel(1);
        tx.try_send((None, session_created("inbox-filler")))
            .unwrap();
        let (broadcast, _receiver) = broadcast::channel(1);
        let confirmation_waiters = Arc::new(Mutex::new(HashMap::new()));
        let sink = AccountEventSink {
            seq: Arc::new(AtomicU64::new(0)),
            tx,
            broadcast,
            events_dir: dir.path().to_path_buf(),
            dropped: Arc::new(AtomicU64::new(0)),
            confirmation_waiters: Arc::clone(&confirmation_waiters),
            latest_config_states: Arc::new(Mutex::new(HashMap::new())),
            next_waiter_id: AtomicU64::new(1),
        };

        let started = std::time::Instant::now();
        assert!(
            !sink
                .record_confirmed_with_timeout(
                    Some("blocked-confirmation"),
                    &session_created("blocked-confirmation"),
                    Duration::from_millis(20),
                )
                .await
        );
        assert!(started.elapsed() < Duration::from_millis(250));
        assert!(confirmation_waiters.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn normal_inbox_drains_after_confirmed_sender_closes() {
        let dir = tempfile::tempdir().unwrap();
        let sink = AccountEventSink::new(dir.path().to_path_buf()).unwrap();
        let events_dir = sink.events_dir().to_path_buf();
        let inbox = sink.inbox();
        drop(sink);
        inbox
            .send((None, deletion("after-confirmed-close")))
            .await
            .unwrap();
        drop(inbox);

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if journal::read_since(&events_dir, 0).is_ok_and(|events| events.len() == 1) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("normal inbox remains serviced");
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
