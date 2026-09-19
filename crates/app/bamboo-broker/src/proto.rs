//! Broker wire protocol: client ↔ broker frames, JSON over WebSocket text.
//!
//! Message payloads reuse `bamboo-subagent`'s [`InboxMessage`] / [`MsgId`] /
//! [`AgentRef`] verbatim — the broker is a transport for those, it does not
//! reinterpret them.

use bamboo_subagent::{ActorEventBatch, AgentRef, InboxMessage, MsgId};
use serde::{Deserialize, Serialize};

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
    /// Acknowledge a processed message so the broker deletes it (at-least-once;
    /// an unacked message is re-pushed on the next subscribe).
    Ack { id: MsgId },
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
}

/// Broker → client.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BrokerFrame {
    /// Handshake accepted.
    Welcome,
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
    /// Out-of-band cancel pushed to a live subscriber: abort the in-flight run
    /// correlated to `correlation_id`. Ephemeral — never persisted/acked (#50).
    Cancel { correlation_id: MsgId },
    /// Answer to [`ClientFrame::ListConnected`]: the mailbox ids of every actor
    /// currently connected serving the requested role.
    Connected { ids: Vec<String> },
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
            ClientFrame::Ack { id: MsgId::new() },
            ClientFrame::Cancel {
                to: "child".into(),
                correlation_id: MsgId::new(),
            },
            ClientFrame::ListConnected {
                role: "gpu-pool".into(),
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
    fn broker_frames_round_trip() {
        let frames = [
            BrokerFrame::Welcome,
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
            BrokerFrame::Cancel {
                correlation_id: MsgId::new(),
            },
            BrokerFrame::Connected {
                ids: vec!["w-1".into(), "w-2".into()],
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
