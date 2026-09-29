//! The deliberately small public projection of a Directory-fenced Actor event.
//! A browser only learns that its authorized Actor view may have changed. It
//! never receives a worker frame, an AgentEvent body, or placement authority.

use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fmt::Write;

use super::actor_event_router::{ActorEventClass, ActorEventEnvelope};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicActorEventClass {
    Lifecycle,
    Semantic,
    Snapshot,
    Ephemeral,
}

/// Host-admitted order within an activation. This is only for the process-local
/// change hub; the browser receives an opaque event ID instead of these lease
/// and execution coordinates.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ActorEventSourceOrder {
    pub lease_epoch: u64,
    pub execution_epoch: u64,
    pub sequence: u64,
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
    #[serde(skip)]
    pub source_order: ActorEventSourceOrder,
}

impl From<&ActorEventEnvelope> for PublicActorEvent {
    fn from(envelope: &ActorEventEnvelope) -> Self {
        let digest = Sha256::digest(envelope.event_id.as_bytes());
        let mut event_id = String::with_capacity(68);
        event_id.push_str("ae1-");
        for byte in digest {
            write!(&mut event_id, "{byte:02x}").expect("String formatting cannot fail");
        }
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
            event_id,
            class,
            source_order: ActorEventSourceOrder {
                lease_epoch: envelope.lease_epoch,
                execution_epoch: envelope.execution_epoch,
                sequence: envelope.sequence,
            },
        }
    }
}

/// Synchronous and nonblocking: the actor frame pump must never wait for a
/// browser. Implementations may drop an event; consumers recover via snapshot.
pub trait ActorEventObserver: Send + Sync {
    fn publish(&self, event: PublicActorEvent);
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::AgentEvent;

    #[test]
    fn public_projection_hashes_internal_identity_and_excludes_content() {
        let envelope = ActorEventEnvelope {
            actor_id: "child".into(),
            parent_actor_id: Some("root".into()),
            root_actor_id: "root".into(),
            project_id: Some("private-project".into()),
            activation_id: "safe-activation".into(),
            attempt: 1,
            lease_epoch: 7,
            execution_epoch: 9,
            sequence: 3,
            event_id: "safe-activation:7:9:3".into(),
            class: ActorEventClass::Semantic,
            payload: AgentEvent::Token {
                content: "private-token".into(),
            },
        };
        let public = PublicActorEvent::from(&envelope);
        assert_eq!(public.event_id.len(), 68);
        assert!(public.event_id.starts_with("ae1-"));
        assert_eq!(public.source_order.sequence, 3);
        assert!(public.event_id[4..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
        let json = serde_json::to_string(&public).unwrap();
        for private in [
            "private-token",
            "private-project",
            "lease_epoch",
            "execution_epoch",
            "source_order",
        ] {
            assert!(!json.contains(private));
        }
    }
}
