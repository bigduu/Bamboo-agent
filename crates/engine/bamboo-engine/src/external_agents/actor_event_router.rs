//! Host-owned routing for Directory-fenced actor events. The wire batch is an
//! untrusted transport frame; only an admitted envelope may reach a consumer.

use std::collections::VecDeque;

use bamboo_agent_core::AgentEvent;
use bamboo_domain::{ActorActivationFence, ActorActivationStatus, ActorDirectoryEntry, ActorId};

const MAX_REPLAY_EVENTS: usize = 64;
const MAX_REPLAY_BYTES: usize = 64 * 1024;
const MAX_REPLAY_EVENT_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ActorEventClass {
    Lifecycle,
    Semantic,
    Snapshot,
    Ephemeral,
}

/// Identity and placement are copied exclusively from the authoritative
/// Directory entry. An event id is stable for a broker redelivery of the same
/// `(activation, epoch, sequence)` and is never supplied by a worker.
#[derive(Debug, Clone)]
pub(super) struct ActorEventEnvelope {
    pub actor_id: ActorId,
    pub parent_actor_id: Option<ActorId>,
    pub root_actor_id: ActorId,
    pub project_id: Option<String>,
    pub activation_id: String,
    pub attempt: u64,
    pub lease_epoch: u64,
    pub execution_epoch: u64,
    pub sequence: u64,
    pub event_id: String,
    pub class: ActorEventClass,
    pub payload: AgentEvent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ActorEventRouteError {
    StaleAuthority,
    ForgedSession,
    LifecycleMissing,
    ConflictingReplay,
    SequenceGap { expected: u64, received: u64 },
    SequenceExhausted,
}

#[derive(Debug)]
pub(super) enum ActorEventRoute {
    Publish(ActorEventEnvelope),
    Duplicate,
    Suppressed,
}

/// A bounded, in-memory replay window for this activation and execution epoch.
/// It does not claim durable or lossless token replay. An evicted or suppressed
/// sequence returns `SequenceGap`, telling a caller to load a Session snapshot.
pub(super) struct ActorEventRouter {
    identity: ActorEventEnvelopeIdentity,
    next_sequence: u64,
    lifecycle_seen: bool,
    require_lifecycle: bool,
    replay_floor: u64,
    replay_bytes: usize,
    replay: VecDeque<ActorEventEnvelope>,
}

struct ActorEventEnvelopeIdentity {
    actor_id: ActorId,
    parent_actor_id: Option<ActorId>,
    root_actor_id: ActorId,
    project_id: Option<String>,
    activation_id: String,
    attempt: u64,
    lease_epoch: u64,
    execution_epoch: u64,
}

impl ActorEventRouter {
    pub(super) fn new(
        entry: &ActorDirectoryEntry,
        fence: &ActorActivationFence,
        execution_epoch: u64,
        require_lifecycle: bool,
    ) -> Result<Self, ActorEventRouteError> {
        let activation = entry
            .activation
            .as_ref()
            .filter(|activation| {
                activation.matches_fence(fence)
                    && activation.status == ActorActivationStatus::Running
                    && entry.actor.actor_id == fence.actor_id
            })
            .ok_or(ActorEventRouteError::StaleAuthority)?;
        Ok(Self {
            identity: ActorEventEnvelopeIdentity {
                actor_id: entry.actor.actor_id.clone(),
                parent_actor_id: entry.actor.parent_actor_id.clone(),
                root_actor_id: entry.actor.root_actor_id.clone(),
                project_id: entry.actor.project_id.clone(),
                activation_id: activation.activation_id.clone(),
                attempt: activation.attempt,
                lease_epoch: activation.lease_epoch,
                execution_epoch,
            },
            next_sequence: 1,
            lifecycle_seen: false,
            require_lifecycle,
            replay_floor: 0,
            replay_bytes: 0,
            replay: VecDeque::new(),
        })
    }

    pub(super) fn route(
        &mut self,
        entry: &ActorDirectoryEntry,
        fence: &ActorActivationFence,
        execution_epoch: u64,
        sequence: u64,
        payload: Option<AgentEvent>,
    ) -> Result<ActorEventRoute, ActorEventRouteError> {
        let authority = entry.activation.as_ref().is_some_and(|activation| {
            activation.matches_fence(fence)
                && activation.status == ActorActivationStatus::Running
                && activation.activation_id == self.identity.activation_id
                && activation.attempt == self.identity.attempt
                && activation.lease_epoch == self.identity.lease_epoch
        }) && entry.actor.actor_id == self.identity.actor_id
            && entry.actor.parent_actor_id == self.identity.parent_actor_id
            && entry.actor.root_actor_id == self.identity.root_actor_id
            && entry.actor.project_id == self.identity.project_id
            && execution_epoch == self.identity.execution_epoch;
        if !authority {
            return Err(ActorEventRouteError::StaleAuthority);
        }
        if sequence < self.next_sequence {
            // A broker can redeliver an acknowledged event after reconnect.
            // Compare it with the bounded published window when available;
            // a reused sequence with different content is never a valid replay.
            if let Some(candidate) = self
                .replay_after(sequence.saturating_sub(1))?
                .into_iter()
                .find(|event| event.sequence == sequence)
            {
                if payload.as_ref().is_some_and(|payload| {
                    serde_json::to_value(payload).ok()
                        != serde_json::to_value(&candidate.payload).ok()
                }) {
                    return Err(ActorEventRouteError::ConflictingReplay);
                }
            }
            return Ok(ActorEventRoute::Duplicate);
        }
        if sequence > self.next_sequence {
            return Err(ActorEventRouteError::SequenceGap {
                expected: self.next_sequence,
                received: sequence,
            });
        }
        let next_sequence = sequence
            .checked_add(1)
            .ok_or(ActorEventRouteError::SequenceExhausted)?;
        let Some(payload) = payload else {
            self.next_sequence = next_sequence;
            self.forget_through(sequence);
            return Ok(ActorEventRoute::Suppressed);
        };
        if payload
            .session_id()
            .is_some_and(|session_id| session_id != self.identity.actor_id)
        {
            return Err(ActorEventRouteError::ForgedSession);
        }
        let class = match &payload {
            AgentEvent::PermissionPostureActivated { .. } => ActorEventClass::Lifecycle,
            AgentEvent::RunnerProgress { .. } => ActorEventClass::Snapshot,
            event if event.is_durable_change() => ActorEventClass::Semantic,
            _ => ActorEventClass::Ephemeral,
        };
        if self.require_lifecycle && !self.lifecycle_seen && class != ActorEventClass::Lifecycle {
            return Err(ActorEventRouteError::LifecycleMissing);
        }
        if class == ActorEventClass::Lifecycle {
            if self.lifecycle_seen {
                return Err(ActorEventRouteError::StaleAuthority);
            }
            self.lifecycle_seen = true;
        }
        let envelope = ActorEventEnvelope {
            actor_id: self.identity.actor_id.clone(),
            parent_actor_id: self.identity.parent_actor_id.clone(),
            root_actor_id: self.identity.root_actor_id.clone(),
            project_id: self.identity.project_id.clone(),
            activation_id: self.identity.activation_id.clone(),
            attempt: self.identity.attempt,
            lease_epoch: self.identity.lease_epoch,
            execution_epoch,
            sequence,
            event_id: format!(
                "{}:{}:{}:{}",
                self.identity.activation_id, self.identity.lease_epoch, execution_epoch, sequence
            ),
            class,
            payload,
        };
        self.next_sequence = next_sequence;
        self.remember(&envelope);
        Ok(ActorEventRoute::Publish(envelope))
    }

    pub(super) fn replay_after(
        &self,
        sequence: u64,
    ) -> Result<Vec<ActorEventEnvelope>, ActorEventRouteError> {
        if sequence < self.replay_floor {
            return Err(ActorEventRouteError::SequenceGap {
                expected: sequence.saturating_add(1),
                received: self.replay_floor.saturating_add(1),
            });
        }
        Ok(self
            .replay
            .iter()
            .filter(|event| event.sequence > sequence)
            .cloned()
            .collect())
    }

    fn remember(&mut self, envelope: &ActorEventEnvelope) {
        let bytes = serde_json::to_vec(&envelope.payload)
            .map_or(MAX_REPLAY_EVENT_BYTES + 1, |value| value.len());
        if bytes > MAX_REPLAY_EVENT_BYTES {
            self.forget_through(envelope.sequence);
            return;
        }
        self.replay_bytes += bytes;
        self.replay.push_back(envelope.clone());
        while self.replay.len() > MAX_REPLAY_EVENTS || self.replay_bytes > MAX_REPLAY_BYTES {
            if let Some(old) = self.replay.pop_front() {
                self.replay_bytes -=
                    serde_json::to_vec(&old.payload).map_or(0, |value| value.len());
                self.replay_floor = old.sequence;
            }
        }
    }

    fn forget_through(&mut self, sequence: u64) {
        self.replay.clear();
        self.replay_bytes = 0;
        self.replay_floor = sequence;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::{
        ActorActivation, ActorLogicalState, ActorSession, Session, ACTOR_DIRECTORY_SCHEMA_VERSION,
    };
    use chrono::{Duration, Utc};

    fn running() -> (ActorDirectoryEntry, ActorActivationFence) {
        let now = Utc::now();
        let session = Session::new("event-root", "model");
        let mut entry = ActorDirectoryEntry::new(ActorSession::from_session(&session).unwrap());
        entry.actor.state = ActorLogicalState::Active;
        entry.actor.current_attempt = 1;
        entry.activation = Some(ActorActivation {
            schema_version: ACTOR_DIRECTORY_SCHEMA_VERSION,
            actor_id: session.id,
            activation_id: "activation-one".into(),
            attempt: 1,
            run_id: "run-one".into(),
            lease_owner: "host".into(),
            lease_epoch: 1,
            lease_expires_at: now + Duration::minutes(1),
            project_id: None,
            inbox_generation: 0,
            placement_ref: None,
            status: ActorActivationStatus::Running,
            checkpoint_revision: 0,
            started_at: Some(now),
            finished_at: None,
        });
        let fence = entry.activation.as_ref().unwrap().fence();
        (entry, fence)
    }

    fn lifecycle() -> AgentEvent {
        AgentEvent::PermissionPostureActivated {
            session_id: "event-root".into(),
            policy_revision: 1,
            requested_mode: "default".into(),
            effective_mode: "default".into(),
            executor_mapping: "test".into(),
        }
    }

    #[test]
    fn publishes_directory_identity_before_content_and_replays_bounded_window() {
        let (entry, fence) = running();
        let mut router = ActorEventRouter::new(&entry, &fence, 7, true).unwrap();
        assert!(matches!(
            router.route(
                &entry,
                &fence,
                7,
                1,
                Some(AgentEvent::Token {
                    content: "early".into()
                })
            ),
            Err(ActorEventRouteError::LifecycleMissing)
        ));
        let ActorEventRoute::Publish(first) = router
            .route(&entry, &fence, 7, 1, Some(lifecycle()))
            .unwrap()
        else {
            panic!("lifecycle must publish");
        };
        assert_eq!(first.actor_id, "event-root");
        assert_eq!(first.root_actor_id, "event-root");
        assert_eq!(first.parent_actor_id, None);
        assert_eq!(first.project_id, None);
        assert_eq!(first.activation_id, "activation-one");
        assert_eq!(first.attempt, 1);
        assert_eq!(first.lease_epoch, 1);
        assert_eq!(first.execution_epoch, 7);
        assert_eq!(first.class, ActorEventClass::Lifecycle);
        assert_eq!(first.event_id, "activation-one:1:7:1");
        let token = AgentEvent::Token {
            content: "live".into(),
        };
        assert!(matches!(
            router
                .route(&entry, &fence, 7, 2, Some(token.clone()))
                .unwrap(),
            ActorEventRoute::Publish(_)
        ));
        assert_eq!(router.replay_after(0).unwrap().len(), 2);
        assert!(matches!(
            router.route(&entry, &fence, 7, 2, Some(token)).unwrap(),
            ActorEventRoute::Duplicate
        ));
        assert!(matches!(
            router.route(
                &entry,
                &fence,
                7,
                2,
                Some(AgentEvent::Token {
                    content: "forged".into()
                })
            ),
            Err(ActorEventRouteError::ConflictingReplay)
        ));
        assert!(matches!(
            router.route(
                &entry,
                &fence,
                7,
                4,
                Some(AgentEvent::Token {
                    content: "gap".into()
                })
            ),
            Err(ActorEventRouteError::SequenceGap {
                expected: 3,
                received: 4
            })
        ));
        assert!(matches!(
            router.route(&entry, &fence, 7, 3, None).unwrap(),
            ActorEventRoute::Suppressed
        ));
        assert!(router.replay_after(0).is_err());
        assert!(matches!(
            router.route(
                &entry,
                &fence,
                7,
                2,
                Some(AgentEvent::Token {
                    content: "live".into()
                })
            ),
            Err(ActorEventRouteError::SequenceGap {
                expected: 2,
                received: 4
            })
        ));
    }

    #[test]
    fn replacement_or_forged_payload_cannot_enter_replay() {
        let (mut entry, fence) = running();
        let mut router = ActorEventRouter::new(&entry, &fence, 1, true).unwrap();
        let forged = AgentEvent::PermissionPostureActivated {
            session_id: "other".into(),
            policy_revision: 1,
            requested_mode: "default".into(),
            effective_mode: "default".into(),
            executor_mapping: "test".into(),
        };
        assert!(matches!(
            router.route(&entry, &fence, 1, 1, Some(forged)),
            Err(ActorEventRouteError::ForgedSession)
        ));
        assert!(router.replay_after(0).unwrap().is_empty());
        entry.activation.as_mut().unwrap().attempt = 2;
        assert!(matches!(
            router.route(&entry, &fence, 1, 1, Some(lifecycle())),
            Err(ActorEventRouteError::StaleAuthority)
        ));
    }
}
