//! Broker wire protocol: client ↔ broker frames, JSON over WebSocket text.
//!
//! Message payloads reuse `bamboo-subagent`'s [`InboxMessage`] / [`MsgId`] /
//! [`AgentRef`] verbatim. The broker treats ordinary messages as opaque; the
//! canonical remote [`FencedRunEnvelope`] is the exception because safe replay
//! requires checking its destination against the current authenticated peer.

use bamboo_domain::WorkerHostCapabilities;
use bamboo_subagent::{ActorEventBatch, AgentRef, InboxMessage, MsgId, RunSpec};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Authenticated, connection-scoped WorkerHost observation. This is broker
/// health evidence, not an ActorActivation or a placement lease.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerHostObservation {
    pub host_ref: String,
    pub mailbox: String,
    pub role: Option<String>,
    pub credential_expires_at: DateTime<Utc>,
    pub connection_generation: String,
    /// Operator policy attestation captured by the broker for this exact
    /// authenticated connection. Missing on legacy peers: no scheduling grant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_capabilities: Option<WorkerHostCapabilities>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_slots: Option<u16>,
    /// Claimed by this authenticated subscriber for the current connection.
    /// The Host still sends a versioned Run and the Worker validates its lease.
    #[serde(default)]
    pub environment_lease_v1: bool,
}

/// Durable destination binding for a canonical remote Run. The broker creates
/// the connection generation from an authenticated scoped subscription; the
/// sender copies that observation here, and the broker checks it again both at
/// enqueue and at every delivery/replay. An old Run can therefore remain in
/// Maildir without being executed by a replacement subscriber at the same
/// mailbox (even when its checkout is byte-for-byte identical).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FencedRunEnvelope {
    pub version: u32,
    pub recipient_host_ref: String,
    pub recipient_mailbox: String,
    pub recipient_connection_generation: String,
    pub recipient_role: String,
    pub run: RunSpec,
}

impl FencedRunEnvelope {
    pub const VERSION: u32 = 1;

    pub fn for_observation(run: RunSpec, observation: &WorkerHostObservation) -> Option<Self> {
        let envelope = Self {
            version: Self::VERSION,
            recipient_host_ref: observation.host_ref.clone(),
            recipient_mailbox: observation.mailbox.clone(),
            recipient_connection_generation: observation.connection_generation.clone(),
            recipient_role: observation.role.clone()?,
            run,
        };
        envelope
            .matches_observation(&observation.mailbox, observation)
            .then_some(envelope)
    }

    pub fn matches_observation(&self, to: &str, observation: &WorkerHostObservation) -> bool {
        let run_id = self.run.activation_run_id.as_deref().unwrap_or_default();
        let actor_id = self
            .run
            .logical_session
            .as_ref()
            .filter(|identity| identity.creation.is_some())
            .map(|identity| identity.session_id.as_str())
            .unwrap_or_default();
        self.version == Self::VERSION
            && !self.recipient_host_ref.is_empty()
            && !self.recipient_mailbox.is_empty()
            && !self.recipient_connection_generation.is_empty()
            && !self.recipient_role.is_empty()
            && !run_id.is_empty()
            && !actor_id.is_empty()
            && self.run.execution_epoch != 0
            && self.run.permission_policy.as_ref().is_some_and(|policy| {
                policy.workspace_path.is_none()
                    && policy.environment_lease.as_ref().is_some_and(|lease| {
                        lease.actor_id == actor_id
                            && lease.activation_run_id == run_id
                            && lease.execution_epoch == self.run.execution_epoch
                    })
            })
            && observation.mailbox == to
            && observation.mailbox == self.recipient_mailbox
            && observation.host_ref == self.recipient_host_ref
            && observation.connection_generation == self.recipient_connection_generation
            && observation.role.as_deref() == Some(self.recipient_role.as_str())
            && observation.environment_lease_v1
            && observation.credential_expires_at > Utc::now()
    }
}

/// Client → broker.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ClientFrame {
    /// First frame on a connection: authenticate (`token`) and bind this
    /// connection to a mailbox key (`agent.session_id`).
    Hello { agent: AgentRef, token: String },
    /// Durably enqueue `message` into the mailbox of session `to`.
    Deliver { to: String, message: InboxMessage },
    /// Publish a sequenced snapshot/ephemeral actor event batch to a live
    /// subscriber without touching Maildir. This lane is deliberately lossy
    /// and bounded; durable event batches must use [`ClientFrame::Deliver`].
    PublishEventBatch {
        to: String,
        correlation_id: MsgId,
        batch: ActorEventBatch,
    },
    /// Start receiving this client's own mailbox (push). Backlog (incl. crash
    /// leftovers) is delivered first, then new messages as they arrive.
    Subscribe,
    /// Worker opts in to fixed-remote EnvironmentLease runs on this authenticated
    /// subscription. Old brokers reject this unknown frame, failing closed.
    SubscribeEnvironmentLeaseV1,
    /// Acknowledge a processed message so the broker deletes it (at-least-once;
    /// an unacked message is re-pushed on the next subscribe).
    Ack { id: MsgId },
    /// ACK with a correlated deletion receipt. Used when the caller must
    /// prove that the broker processed the ACK before releasing a Host receipt.
    AckWithReceipt { id: MsgId, request_id: MsgId },
    /// Out-of-band cancel: ask the broker to signal session `to` to abort the
    /// in-flight run correlated to `correlation_id` (the timed-out ask's id).
    /// Ephemeral and fire-and-forget — NOT durable, never enters a mailbox, never
    /// acked. A control signal, deliberately off the at-least-once work path so a
    /// cancel can never queue behind the very work it cancels. #50.
    Cancel { to: String, correlation_id: MsgId },
    /// Ask the broker which actors are currently connected serving `role` — the
    /// bus IS the live-actor registry (presence is connection-truth). The broker
    /// answers with [`BrokerFrame::Connected`]. Replaces the HTTP `/v1/agents`
    /// registry discover for schedulable worker selection (Phase 3).
    ListConnected { role: String },
    /// Internal exact-target query; scoped policy must allow the role's
    /// presence and Run delivery to this mailbox.
    ObserveHost {
        request_id: MsgId,
        mailbox: String,
        role: String,
        /// Opt in to the operator-policy capacity extension. Old peers omit
        /// this field and receive the original observation shape.
        #[serde(default, skip_serializing_if = "is_false")]
        include_capacity: bool,
    },
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// Broker → client.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BrokerFrame {
    /// Handshake accepted.
    Welcome,
    /// Stable identity of the durable mailbox namespace behind this broker.
    /// A Host receipt may only be replay-ACKed against this exact namespace.
    BrokerIdentity { id: String },
    /// Handshake or request rejected; the broker closes the connection after
    /// an auth error.
    ///
    /// `id` correlates the rejection back to the [`ClientFrame::Deliver`]
    /// that caused it (e.g. `MailboxFull`) — [`Some`] the message's own
    /// [`MsgId`] when the broker rejected a specific `Deliver`, `None` for
    /// rejections with nothing to correlate to (bad handshake, a malformed
    /// frame). Lets `BrokerClient::deliver` route the rejection back to the
    /// waiting caller instead of the caller only ever seeing a generic
    /// receipt timeout (review finding on #491/#53).
    Error {
        reason: String,
        #[serde(default)]
        id: Option<MsgId>,
    },
    /// A message pushed from the subscriber's mailbox.
    Message { message: InboxMessage },
    /// A live actor event batch. No ack is required: sequence gaps trigger
    /// snapshot reconciliation at the consumer.
    EventBatch {
        correlation_id: MsgId,
        batch: ActorEventBatch,
    },
    /// Receipt that a [`ClientFrame::Deliver`] was durably enqueued.
    Delivered { id: MsgId },
    /// Result of an [`ClientFrame::AckWithReceipt`]. A retry with the same
    /// message id remains safe after a lost response because ACK is idempotent.
    AckResult {
        id: MsgId,
        request_id: MsgId,
        accepted: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    /// Out-of-band cancel pushed to a live subscriber: abort the in-flight run
    /// correlated to `correlation_id`. Ephemeral — never persisted/acked (#50).
    Cancel { correlation_id: MsgId },
    /// Answer to [`ClientFrame::ListConnected`]: the mailbox ids of every actor
    /// currently connected serving the requested role.
    Connected { ids: Vec<String> },
    /// Current trusted observation, or none if the target is not live.
    HostObservation {
        request_id: MsgId,
        observation: Option<WorkerHostObservation>,
    },
}

impl ClientFrame {
    pub fn to_text(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
    }
    pub fn from_text(s: &str) -> serde_json::Result<Self> {
        serde_json::from_str(s)
    }
}

impl BrokerFrame {
    pub fn to_text(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
    }
    pub fn from_text(s: &str) -> serde_json::Result<Self> {
        serde_json::from_str(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_subagent::{AskBody, AskMode, InboxKind};
    use chrono::Utc;

    fn ask_msg() -> InboxMessage {
        InboxMessage {
            id: MsgId::new(),
            from: AgentRef {
                session_id: "parent".into(),
                role: None,
            },
            kind: InboxKind::Ask,
            body: serde_json::to_value(AskBody {
                question: "status?".into(),
                mode: AskMode::Query,
            })
            .unwrap(),
            created_at: Utc::now(),
            correlation_id: None,
        }
    }

    #[test]
    fn client_frames_round_trip() {
        let frames = [
            ClientFrame::Hello {
                agent: AgentRef {
                    session_id: "p".into(),
                    role: Some("root".into()),
                },
                token: "t".into(),
            },
            ClientFrame::Deliver {
                to: "child".into(),
                message: ask_msg(),
            },
            ClientFrame::PublishEventBatch {
                to: "parent".into(),
                correlation_id: MsgId::new(),
                batch: event_batch(),
            },
            ClientFrame::Subscribe,
            ClientFrame::SubscribeEnvironmentLeaseV1,
            ClientFrame::Ack { id: MsgId::new() },
            ClientFrame::AckWithReceipt {
                id: MsgId::new(),
                request_id: MsgId::new(),
            },
            ClientFrame::Cancel {
                to: "child".into(),
                correlation_id: MsgId::new(),
            },
            ClientFrame::ListConnected {
                role: "gpu-pool".into(),
            },
            ClientFrame::ObserveHost {
                request_id: MsgId::new(),
                mailbox: "worker".into(),
                role: "gpu-pool".into(),
                include_capacity: false,
            },
        ];
        for f in frames {
            assert_eq!(ClientFrame::from_text(&f.to_text()).unwrap(), f);
        }
        // tag stability
        let v: serde_json::Value = serde_json::from_str(&ClientFrame::Subscribe.to_text()).unwrap();
        assert_eq!(v["kind"], "subscribe");
        let c: serde_json::Value = serde_json::from_str(
            &ClientFrame::Cancel {
                to: "child".into(),
                correlation_id: MsgId::new(),
            }
            .to_text(),
        )
        .unwrap();
        assert_eq!(c["kind"], "cancel");
    }

    #[test]
    fn ordinary_host_observation_request_keeps_legacy_wire_shape() {
        let frame = ClientFrame::ObserveHost {
            request_id: MsgId::new(),
            mailbox: "worker".into(),
            role: "worker".into(),
            include_capacity: false,
        };
        let encoded = serde_json::to_value(&frame).unwrap();
        assert!(encoded.get("include_capacity").is_none());
        assert_eq!(
            serde_json::from_value::<ClientFrame>(encoded).unwrap(),
            frame
        );
    }

    #[test]
    fn broker_frames_round_trip() {
        let frames = [
            BrokerFrame::Welcome,
            BrokerFrame::BrokerIdentity {
                id: uuid::Uuid::new_v4().to_string(),
            },
            BrokerFrame::Error {
                reason: "bad token".into(),
                id: None,
            },
            BrokerFrame::Error {
                reason: "mailbox 'child' is full (2 pending messages)".into(),
                id: Some(MsgId::new()),
            },
            BrokerFrame::Message { message: ask_msg() },
            BrokerFrame::EventBatch {
                correlation_id: MsgId::new(),
                batch: event_batch(),
            },
            BrokerFrame::Delivered { id: MsgId::new() },
            BrokerFrame::AckResult {
                id: MsgId::new(),
                request_id: MsgId::new(),
                accepted: true,
                reason: None,
            },
            BrokerFrame::Cancel {
                correlation_id: MsgId::new(),
            },
            BrokerFrame::Connected {
                ids: vec!["w-1".into(), "w-2".into()],
            },
            BrokerFrame::HostObservation {
                request_id: MsgId::new(),
                observation: None,
            },
            BrokerFrame::HostObservation {
                request_id: MsgId::new(),
                observation: Some(WorkerHostObservation {
                    host_ref: "trusted-host".into(),
                    mailbox: "worker".into(),
                    role: Some("gpu-pool".into()),
                    credential_expires_at: Utc::now(),
                    connection_generation: MsgId::new().0,
                    host_capabilities: None,
                    max_slots: None,
                    environment_lease_v1: true,
                }),
            },
        ];
        for f in frames {
            assert_eq!(BrokerFrame::from_text(&f.to_text()).unwrap(), f);
        }
    }

    /// A legacy `Error` frame serialized without the `id` field (as every
    /// `Error` was before this correlation id existed) still parses, with
    /// `id` defaulting to `None` — so an older broker (or a captured/replayed
    /// frame) never fails a newer client's deserialization.
    #[test]
    fn error_frame_without_id_defaults_to_none() {
        let legacy = serde_json::json!({ "kind": "error", "reason": "bad token" });
        let parsed: BrokerFrame = serde_json::from_value(legacy).unwrap();
        assert_eq!(
            parsed,
            BrokerFrame::Error {
                reason: "bad token".into(),
                id: None,
            }
        );
    }

    #[test]
    fn old_host_observation_does_not_claim_environment_lease() {
        let legacy = serde_json::json!({
            "host_ref": "old-worker",
            "mailbox": "worker",
            "role": "gpu",
            "credential_expires_at": Utc::now(),
            "connection_generation": "old-generation"
        });
        let observation: WorkerHostObservation = serde_json::from_value(legacy).unwrap();
        assert!(!observation.environment_lease_v1);
        assert!(observation.host_capabilities.is_none());
        assert!(observation.max_slots.is_none());
    }

    fn event_batch() -> ActorEventBatch {
        ActorEventBatch {
            logical_session: None,
            activation_id: Some("run-1".into()),
            execution_epoch: 1,
            source_node_id: Some("node-a".into()),
            source_actor_id: Some("worker-a".into()),
            first_seq: 1,
            last_seq: 1,
            qos: bamboo_subagent::ActorEventQos::Ephemeral,
            events: vec![serde_json::json!({"type":"token","content":"x"})],
        }
    }
}
