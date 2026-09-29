//! Lazy, process-local delivery of redacted canonical Actor change markers.
//! Only actors with a live gateway subscriber have a sender or replay window.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use bamboo_engine::external_agents::actor_event_stream::{
    ActorEventObserver, ActorEventSourceOrder, PublicActorEvent, PublicActorEventClass,
};
use serde::Serialize;
use tokio::sync::broadcast;

const REPLAY_EVENTS: usize = 64;
const REPLAY_BYTES: usize = 64 * 1024;
const RING_EVENTS: usize = 64;
const MAX_CHANNELS: usize = 256;
// The outer WS seq is a JavaScript-safe integer. Its high bits distinguish a
// new channel after all observers leave within one process. The generation
// restarts with the process and is not a cross-process resume credential.
const COUNTER_BITS: u32 = 32;
const MAX_GENERATION: u64 = (1 << 21) - 1;

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ActorChange {
    #[serde(rename = "type")]
    kind: &'static str,
    #[serde(flatten)]
    pub event: PublicActorEvent,
}

impl ActorChange {
    fn new(event: PublicActorEvent) -> Self {
        Self {
            kind: "actor_changed",
            event,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SequencedActorChange {
    pub cursor: u64,
    pub change: ActorChange,
}

#[derive(Clone, Debug)]
pub(crate) enum ActorHubMessage {
    Change(SequencedActorChange),
    Reset(u64),
}

pub(crate) enum ActorReplay {
    Events(Vec<SequencedActorChange>),
    SnapshotRequired { cursor: u64, reason: &'static str },
}

struct ChannelEntry {
    generation: u64,
    counter: u32,
    refs: usize,
    replay_floor: u64,
    replay_bytes: usize,
    replay: VecDeque<(SequencedActorChange, usize)>,
    last_source: Option<AcceptedSource>,
    sender: broadcast::Sender<ActorHubMessage>,
}

struct AcceptedSource {
    attempt: u64,
    activation_id: String,
    order: ActorEventSourceOrder,
}

impl AcceptedSource {
    fn admits(&self, event: &PublicActorEvent) -> bool {
        if event.attempt != self.attempt {
            return event.attempt > self.attempt && event.class == PublicActorEventClass::Lifecycle;
        }
        if event.activation_id != self.activation_id
            || event.source_order.lease_epoch != self.order.lease_epoch
        {
            return false;
        }
        if event.source_order.execution_epoch != self.order.execution_epoch {
            return event.source_order.execution_epoch > self.order.execution_epoch
                && event.class == PublicActorEventClass::Lifecycle;
        }
        event.source_order.sequence > self.order.sequence
    }
}

impl ChannelEntry {
    fn cursor(&self) -> u64 {
        (self.generation << COUNTER_BITS) | u64::from(self.counter)
    }

    fn replay_after(&self, since: Option<u64>) -> ActorReplay {
        let cursor = self.cursor();
        let Some(since) = since else {
            return ActorReplay::SnapshotRequired {
                cursor,
                reason: "initial",
            };
        };
        if (since >> COUNTER_BITS) != self.generation || since < self.replay_floor || since > cursor
        {
            return ActorReplay::SnapshotRequired {
                cursor,
                reason: "gap",
            };
        }
        ActorReplay::Events(
            self.replay
                .iter()
                .filter(|(event, _)| event.cursor > since)
                .map(|(event, _)| event.clone())
                .collect(),
        )
    }
}

#[derive(Default)]
struct HubState {
    next_generation: u64,
    channels: HashMap<String, ChannelEntry>,
}

#[derive(Default)]
pub(crate) struct ActorEventHub {
    state: Mutex<HubState>,
}

pub(crate) struct ActorSubscription {
    hub: Arc<ActorEventHub>,
    actor_id: String,
    pub receiver: broadcast::Receiver<ActorHubMessage>,
    pub initial: ActorReplay,
}

impl ActorSubscription {
    pub fn replay_after(&self, since: u64) -> ActorReplay {
        let state = self.hub.state.lock().unwrap_or_else(|e| e.into_inner());
        state
            .channels
            .get(&self.actor_id)
            .map(|entry| entry.replay_after(Some(since)))
            .unwrap_or(ActorReplay::SnapshotRequired {
                cursor: since,
                reason: "gap",
            })
    }
}

impl Drop for ActorSubscription {
    fn drop(&mut self) {
        let mut state = self.hub.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = state.channels.get_mut(&self.actor_id) {
            entry.refs -= 1;
            if entry.refs == 0 {
                state.channels.remove(&self.actor_id);
            }
        }
    }
}

impl ActorEventHub {
    pub fn subscribe(
        self: &Arc<Self>,
        actor_id: &str,
        since: Option<u64>,
    ) -> Option<ActorSubscription> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if !state.channels.contains_key(actor_id) {
            if state.channels.len() >= MAX_CHANNELS || state.next_generation >= MAX_GENERATION {
                return None;
            }
            state.next_generation += 1;
            let generation = state.next_generation;
            let (sender, _) = broadcast::channel(RING_EVENTS);
            state.channels.insert(
                actor_id.to_owned(),
                ChannelEntry {
                    generation,
                    counter: 0,
                    refs: 0,
                    replay_floor: generation << COUNTER_BITS,
                    replay_bytes: 0,
                    replay: VecDeque::new(),
                    last_source: None,
                    sender,
                },
            );
        }
        let entry = state.channels.get_mut(actor_id)?;
        entry.refs += 1;
        // Receiver creation and replay planning are serialized with publish.
        let receiver = entry.sender.subscribe();
        // A numeric client cursor has no boot identity. Even if a replay window
        // is active, a previous process can have issued the same generation and
        // counter. Never claim a reconnect is continuous from `since`; require
        // an authorized snapshot. This subscription's own replay_after remains
        // valid for live lag recovery because it cannot outlive this hub.
        let initial = if since.is_some() {
            ActorReplay::SnapshotRequired {
                cursor: entry.cursor(),
                reason: "gap",
            }
        } else {
            entry.replay_after(since)
        };
        Some(ActorSubscription {
            hub: self.clone(),
            actor_id: actor_id.to_owned(),
            receiver,
            initial,
        })
    }

    #[cfg(test)]
    fn active_channels(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .channels
            .len()
    }
}

impl ActorEventObserver for ActorEventHub {
    fn publish(&self, event: PublicActorEvent) {
        // Ephemeral content (tokens and other non-durable progress) has no
        // authoritative snapshot meaning and can arrive at token rate. Do not
        // let it consume a public cursor or evict bounded lifecycle/semantic
        // replay; legacy agent channels retain their separate content stream.
        if event.class == PublicActorEventClass::Ephemeral {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let Some(entry) = state.channels.get(&event.actor_id) else {
            return;
        };
        // Several activation pumps may finish their async publication paths
        // out of order. Once this channel has observed an activation, a new
        // attempt or execution epoch must publish its lifecycle first. A new
        // channel may begin mid-activation: its required initial snapshot
        // supplies that context, so its first event need not be lifecycle.
        if entry
            .last_source
            .as_ref()
            .is_some_and(|source| !source.admits(&event))
        {
            return;
        }
        let rollover = entry.counter == u32::MAX;
        if rollover {
            // An extremely long-lived channel gets a new cursor epoch and a
            // reset signal. Existing receivers must refetch before trusting it.
            if state.next_generation >= MAX_GENERATION {
                return;
            }
            state.next_generation += 1;
            let generation = state.next_generation;
            let entry = state.channels.get_mut(&event.actor_id).expect("present");
            entry.generation = generation;
            entry.counter = 0;
            entry.replay.clear();
            entry.replay_bytes = 0;
            entry.replay_floor = entry.cursor();
            let _ = entry.sender.send(ActorHubMessage::Reset(entry.cursor()));
        }
        let entry = state.channels.get_mut(&event.actor_id).expect("present");
        entry.counter += 1;
        entry.last_source = Some(AcceptedSource {
            attempt: event.attempt,
            activation_id: event.activation_id.clone(),
            order: event.source_order,
        });
        let change = ActorChange::new(event);
        let bytes = serde_json::to_vec(&change).map_or(REPLAY_BYTES + 1, |value| value.len());
        let item = SequencedActorChange {
            cursor: entry.cursor(),
            change,
        };
        if bytes <= REPLAY_BYTES {
            entry.replay_bytes += bytes;
            entry.replay.push_back((item.clone(), bytes));
            while entry.replay.len() > REPLAY_EVENTS || entry.replay_bytes > REPLAY_BYTES {
                if let Some((removed, size)) = entry.replay.pop_front() {
                    entry.replay_bytes -= size;
                    entry.replay_floor = removed.cursor;
                }
            }
        } else {
            entry.replay.clear();
            entry.replay_bytes = 0;
            entry.replay_floor = item.cursor;
        }
        let _ = entry.sender.send(ActorHubMessage::Change(item));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(actor_id: &str, n: usize) -> PublicActorEvent {
        PublicActorEvent {
            actor_id: actor_id.into(),
            root_actor_id: "root".into(),
            parent_actor_id: Some("root".into()),
            activation_id: "safe-activation".into(),
            attempt: 1,
            event_id: format!("ae1-{n:064x}"),
            class: PublicActorEventClass::Semantic,
            source_order: ActorEventSourceOrder {
                lease_epoch: 1,
                execution_epoch: 1,
                sequence: n as u64,
            },
        }
    }

    #[test]
    fn only_visible_actors_have_channels_and_last_drop_retires_cursor() {
        let hub = Arc::new(ActorEventHub::default());
        for id in 0..134 {
            hub.publish(event(&format!("actor-{id}"), 1));
        }
        assert_eq!(hub.active_channels(), 0);
        let selected = hub.subscribe("actor-42", None).unwrap();
        let peer = hub.subscribe("actor-42", None).unwrap();
        assert_eq!(hub.active_channels(), 1);
        let first_cursor = match &selected.initial {
            ActorReplay::SnapshotRequired {
                cursor,
                reason: "initial",
            } => *cursor,
            _ => panic!("fresh subscription needs snapshot"),
        };
        assert!(first_cursor > 0 && first_cursor < (1 << 53));
        drop(selected);
        assert_eq!(hub.active_channels(), 1);
        drop(peer);
        assert_eq!(hub.active_channels(), 0);
        let resumed = hub.subscribe("actor-42", Some(first_cursor)).unwrap();
        assert!(matches!(
            &resumed.initial,
            ActorReplay::SnapshotRequired { reason: "gap", .. }
        ));
    }

    #[test]
    fn process_restart_cannot_accept_a_colliding_old_cursor() {
        let before_restart = Arc::new(ActorEventHub::default());
        let old = before_restart.subscribe("child", None).unwrap();
        let base = match &old.initial {
            ActorReplay::SnapshotRequired { cursor, .. } => *cursor,
            _ => panic!("first subscription needs a snapshot"),
        };
        before_restart.publish(event("child", 1));
        before_restart.publish(event("child", 2));
        let old_cursor = base + 2;
        drop(old);

        // A fresh process has the same initial generation. Another subscriber
        // can open its channel and advance to the same numeric cursor before
        // this client reconnects, so merely checking for a new channel fails.
        let after_restart = Arc::new(ActorEventHub::default());
        let current = after_restart.subscribe("child", None).unwrap();
        after_restart.publish(event("child", 3));
        after_restart.publish(event("child", 4));
        let resumed = after_restart.subscribe("child", Some(old_cursor)).unwrap();
        assert!(matches!(
            &resumed.initial,
            ActorReplay::SnapshotRequired {
                cursor,
                reason: "gap"
            } if *cursor == old_cursor
        ));

        // After a fresh snapshot, this live channel can still replay events
        // published while the client fetched that snapshot.
        after_restart.publish(event("child", 5));
        let ActorReplay::Events(events) = resumed.replay_after(old_cursor) else {
            panic!("the current channel must retain its live tail");
        };
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].cursor, old_cursor + 1);
        drop(current);
    }

    #[test]
    fn event_between_snapshot_read_and_subscription_requires_a_new_snapshot() {
        let hub = Arc::new(ActorEventHub::default());
        // The independent REST snapshot can finish before WS subscribes. With
        // no listener, this change has no replay window and must not be silently
        // treated as covered by that earlier snapshot.
        hub.publish(event("child", 1));
        let mut subscribed = hub.subscribe("child", None).unwrap();
        let anchor = match &subscribed.initial {
            ActorReplay::SnapshotRequired {
                cursor,
                reason: "initial",
            } => *cursor,
            _ => panic!("subscribe must require a post-subscription snapshot"),
        };
        hub.publish(event("child", 2));
        let ActorReplay::Events(events) = subscribed.replay_after(anchor) else {
            panic!("events after subscription must remain recoverable");
        };
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].change.event.event_id, event("child", 2).event_id);
        assert!(matches!(
            subscribed.receiver.try_recv(),
            Ok(ActorHubMessage::Change(item)) if item.cursor == anchor + 1
        ));
    }

    #[test]
    fn replay_is_bounded_and_redacted() {
        let hub = Arc::new(ActorEventHub::default());
        let mut first = hub.subscribe("child", None).unwrap();
        let cursor = match &first.initial {
            ActorReplay::SnapshotRequired { cursor, .. } => *cursor,
            _ => unreachable!(),
        };
        for n in 0..80 {
            hub.publish(event("child", n));
        }
        let ActorReplay::Events(replay) = first.replay_after(cursor + 75) else {
            panic!("recent cursor must replay");
        };
        assert_eq!(replay.len(), 5);
        assert_eq!(replay[0].cursor, cursor + 76);
        assert!(matches!(
            first.replay_after(cursor),
            ActorReplay::SnapshotRequired { reason: "gap", .. }
        ));
        let wire = serde_json::to_value(&replay[0].change).unwrap();
        assert_eq!(wire["type"], "actor_changed");
        assert!(wire.get("payload").is_none());
        assert!(wire.get("project_id").is_none());
        assert!(wire.get("lease_epoch").is_none());
        assert!(wire.get("worker_endpoint").is_none());
        assert_eq!(wire.as_object().unwrap().len(), 8);
        // A slow receiver overruns; the retained critical window still covers
        // recent cursors, while an older cursor requires a snapshot.
        assert!(matches!(
            first.receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Lagged(_))
        ));
    }

    #[test]
    fn ephemeral_burst_does_not_displace_critical_replay_or_advance_cursor() {
        let hub = Arc::new(ActorEventHub::default());
        let mut live = hub.subscribe("child", None).unwrap();
        let anchor = match &live.initial {
            ActorReplay::SnapshotRequired { cursor, .. } => *cursor,
            _ => unreachable!(),
        };
        let mut lifecycle = event("child", 1);
        lifecycle.class = PublicActorEventClass::Lifecycle;
        hub.publish(lifecycle);
        for n in 0..1000 {
            let mut ephemeral = event("child", n);
            ephemeral.class = PublicActorEventClass::Ephemeral;
            hub.publish(ephemeral);
        }
        let mut snapshot = event("child", 2);
        snapshot.class = PublicActorEventClass::Snapshot;
        hub.publish(snapshot);

        let ActorReplay::Events(replay) = live.replay_after(anchor) else {
            panic!("critical events must remain replayable");
        };
        assert_eq!(replay.len(), 2);
        assert_eq!(replay[0].cursor, anchor + 1);
        assert_eq!(replay[1].cursor, anchor + 2);
        assert_eq!(
            replay[0].change.event.class,
            PublicActorEventClass::Lifecycle
        );
        assert_eq!(
            replay[1].change.event.class,
            PublicActorEventClass::Snapshot
        );
        assert!(matches!(
            live.receiver.try_recv(),
            Ok(ActorHubMessage::Change(_))
        ));
        assert!(matches!(
            live.receiver.try_recv(),
            Ok(ActorHubMessage::Change(_))
        ));
        assert!(matches!(
            live.receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn delayed_or_replayed_sources_cannot_rewind_a_live_actor_channel() {
        let hub = Arc::new(ActorEventHub::default());
        let live = hub.subscribe("child", None).unwrap();
        let anchor = match live.initial {
            ActorReplay::SnapshotRequired { cursor, .. } => cursor,
            _ => unreachable!(),
        };

        let mut first = event("child", 1);
        first.class = PublicActorEventClass::Lifecycle;
        hub.publish(first.clone());
        hub.publish(event("child", 2));
        hub.publish(event("child", 2)); // exact replay
        hub.publish(first); // older sequence
        hub.publish(event("child", 4)); // source may skip an ephemeral event
        hub.publish(event("child", 3)); // delayed event cannot rewind it

        let mut early_epoch_content = event("child", 2);
        early_epoch_content.source_order.execution_epoch = 2;
        hub.publish(early_epoch_content); // cannot precede the new lifecycle
        let mut next_epoch = event("child", 1);
        next_epoch.source_order.execution_epoch = 2;
        next_epoch.event_id = "ae1-next-epoch".into();
        next_epoch.class = PublicActorEventClass::Lifecycle;
        hub.publish(next_epoch);
        hub.publish(event("child", 99)); // delayed prior execution epoch

        let mut early_attempt_content = event("child", 2);
        early_attempt_content.attempt = 2;
        early_attempt_content.activation_id = "replacement-activation".into();
        early_attempt_content.source_order.lease_epoch = 2;
        hub.publish(early_attempt_content); // cannot precede the replacement lifecycle
        let mut next_attempt = event("child", 1);
        next_attempt.attempt = 2;
        next_attempt.activation_id = "replacement-activation".into();
        next_attempt.source_order.lease_epoch = 2;
        next_attempt.event_id = "ae1-next-attempt".into();
        next_attempt.class = PublicActorEventClass::Lifecycle;
        hub.publish(next_attempt.clone());
        let mut old_attempt = event("child", 100);
        old_attempt.source_order.execution_epoch = 3;
        hub.publish(old_attempt);
        let mut conflicting = next_attempt;
        conflicting.activation_id = "different-activation".into();
        conflicting.source_order.sequence = 2;
        hub.publish(conflicting);

        let ActorReplay::Events(replay) = live.replay_after(anchor) else {
            panic!("accepted source events must remain replayable");
        };
        assert_eq!(replay.len(), 5);
        assert_eq!(replay.last().unwrap().cursor, anchor + 5);
        assert_eq!(replay[3].change.event.event_id, "ae1-next-epoch");
        assert_eq!(replay[4].change.event.event_id, "ae1-next-attempt");
        assert_eq!(replay[4].change.event.attempt, 2);
    }
}
