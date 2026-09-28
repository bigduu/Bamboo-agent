//! The deliberately small public projection of a Directory-fenced Actor event.
//! A browser only learns that its authorized Actor view may have changed. It
//! never receives a worker frame, an AgentEvent body, or placement authority.

use serde::Serialize;
use sha2::{Digest, Sha256};

use super::actor_event_router::{ActorEventClass, ActorEventEnvelope};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicActorEventClass {
    Lifecycle,
    Semantic,
    Snapshot,
    Ephemeral,
}

/// Safe metadata copied from the host-admitted envelope. The opaque event ID
/// preserves duplicate identity without exposing internal lease/epoch fields.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PublicActorEvent {
    pub actor_id: String,
    pub root_actor_id: String,
    pub parent_actor_id: Option<String>,
    pub activation_id: String,
    pub attempt: u64,
    pub event_id: String,
    pub class: PublicActorEventClass,
}

impl From<&ActorEventEnvelope> for PublicActorEvent {
    fn from(envelope: &ActorEventEnvelope) -> Self {
        let digest = Sha256::digest(envelope.event_id.as_bytes());
        let class = match envelope.class {
            ActorEventClass::Lifecycle => PublicActorEventClass::Lifecycle,
            ActorEventClass::Semantic => PublicActorEventClass::Semantic,
            ActorEventClass::Snapshot => PublicActorEventClass::Snapshot,
            ActorEventClass::Ephemeral => PublicActorEventClass::Ephemeral,
        };
        Self {
            actor_id: envelope.actor_id.clone(),
            root_actor_id: envelope.root_actor_id.clone(),
            parent_actor_id: envelope.parent_actor_id.clone(),
            activation_id: envelope.activation_id.clone(),
            attempt: envelope.attempt,
            event_id: format!("ae1-{digest:x}"),
            class,
        }
    }
}

/// Synchronous and nonblocking: the actor frame pump must never wait for a
/// browser. Implementations may drop an event; consumers recover via snapshot.
pub trait ActorEventObserver: Send + Sync {
    fn publish(&self, event: PublicActorEvent);
}
