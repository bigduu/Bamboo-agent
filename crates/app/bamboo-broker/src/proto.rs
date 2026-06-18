//! Broker wire protocol: client ↔ broker frames, JSON over WebSocket text.
//!
//! Message payloads reuse `bamboo-subagent`'s [`InboxMessage`] / [`MsgId`] /
//! [`AgentRef`] verbatim — the broker is a transport for those, it does not
//! reinterpret them.

use bamboo_subagent::{AgentRef, InboxMessage, MsgId};
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
    /// Start receiving this client's own mailbox (push). Backlog (incl. crash
    /// leftovers) is delivered first, then new messages as they arrive.
    Subscribe,
    /// Acknowledge a processed message so the broker deletes it (at-least-once;
    /// an unacked message is re-pushed on the next subscribe).
    Ack { id: MsgId },
}

/// Broker → client.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BrokerFrame {
    /// Handshake accepted.
    Welcome,
    /// Handshake or request rejected; the broker closes the connection after
    /// an auth error.
    Error { reason: String },
    /// A message pushed from the subscriber's mailbox.
    Message { message: InboxMessage },
    /// Receipt that a [`ClientFrame::Deliver`] was durably enqueued.
    Delivered { id: MsgId },
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
            ClientFrame::Subscribe,
            ClientFrame::Ack { id: MsgId::new() },
        ];
        for f in frames {
            assert_eq!(ClientFrame::from_text(&f.to_text()).unwrap(), f);
        }
        // tag stability
        let v: serde_json::Value = serde_json::from_str(&ClientFrame::Subscribe.to_text()).unwrap();
        assert_eq!(v["kind"], "subscribe");
    }

    #[test]
    fn broker_frames_round_trip() {
        let frames = [
            BrokerFrame::Welcome,
            BrokerFrame::Error {
                reason: "bad token".into(),
            },
            BrokerFrame::Message { message: ask_msg() },
            BrokerFrame::Delivered { id: MsgId::new() },
        ];
        for f in frames {
            assert_eq!(BrokerFrame::from_text(&f.to_text()).unwrap(), f);
        }
    }
}
