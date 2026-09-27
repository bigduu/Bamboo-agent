//! Explicit startup policy for the opt-in scoped broker listener.
use bamboo_subagent::{ActorEventBatch, ActorEventQos, AgentRef, InboxKind};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::{collections::HashSet, sync::Arc};
use tokio::time::Instant;

use crate::{BrokerError, BrokerResult, ClientFrame};

pub const MAX_PEER_POLICY_BYTES: usize = 64 * 1024;
const DENIED: &str = "scoped peer admission denied";

/// Constructed only by bounded, closed parsing. Credentials have no Debug or
/// serialization implementation and never leave this private lookup table.
pub struct PeerPolicy(Vec<Arc<Peer>>);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyInput {
    peers: Vec<Peer>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Peer {
    credential: String,
    host: String,
    mailbox: String,
    #[serde(default)]
    role: Option<String>,
    expires_at: DateTime<Utc>,
    destinations: Vec<Destination>,
    #[serde(default)]
    cancel: Vec<String>,
    #[serde(default)]
    presence: Vec<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Destination {
    mailbox: String,
    kinds: Vec<InboxKind>,
}

fn identifier(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 256
        && s != "."
        && s != ".."
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-.".contains(&b))
}
fn mailbox_identifier(s: &str) -> bool {
    identifier(s) && !s.bytes().any(|b| b.is_ascii_uppercase())
}
fn identifiers(values: &[String]) -> bool {
    values.len() <= 16
        && values.iter().all(|s| identifier(s))
        && values.iter().collect::<HashSet<_>>().len() == values.len()
}
fn denied() -> BrokerError {
    BrokerError::Auth(DENIED.into())
}

impl PeerPolicy {
    pub fn from_json(bytes: &[u8]) -> BrokerResult<Self> {
        let input: PolicyInput = if bytes.len() <= MAX_PEER_POLICY_BYTES {
            serde_json::from_slice(bytes).map_err(|_| denied())?
        } else {
            return Err(denied());
        };
        let mut credentials = HashSet::new();
        let mut mailboxes = HashSet::new();
        if input.peers.is_empty() || input.peers.len() > 64 {
            return Err(denied());
        }
        for peer in &input.peers {
            let mut targets = HashSet::new();
            if !(32..=256).contains(&peer.credential.len())
                || !peer.credential.bytes().all(|b| b.is_ascii_graphic())
                || !credentials.insert(&peer.credential)
                || !mailboxes.insert(&peer.mailbox)
                || !identifier(&peer.host)
                || !mailbox_identifier(&peer.mailbox)
                || peer.role.as_deref().is_some_and(|s| !identifier(s))
                || !identifiers(&peer.cancel)
                || !peer.cancel.iter().all(|s| mailbox_identifier(s))
                || !identifiers(&peer.presence)
                || peer.destinations.len() > 16
                || peer.destinations.iter().any(|d| {
                    !mailbox_identifier(&d.mailbox)
                        || !targets.insert(&d.mailbox)
                        || d.kinds.is_empty()
                        || d.kinds
                            .iter()
                            .enumerate()
                            .any(|(i, k)| d.kinds[..i].contains(k))
                })
            {
                return Err(denied());
            }
        }
        Ok(Self(input.peers.into_iter().map(Arc::new).collect()))
    }

    pub(crate) fn capture(&self, agent: &AgentRef, token: &str) -> BrokerResult<CapturedPeer> {
        let peer = self
            .0
            .iter()
            .find(|p| p.credential == token)
            .ok_or_else(denied)?;
        if agent.session_id != peer.mailbox || agent.role != peer.role {
            return Err(denied());
        }
        let remaining = (peer.expires_at - Utc::now())
            .to_std()
            .map_err(|_| denied())?;
        let deadline = Instant::now().checked_add(remaining).ok_or_else(denied)?;
        let captured = CapturedPeer {
            peer: Arc::clone(peer),
            deadline,
        };
        captured.live()?;
        Ok(captured)
    }
}

pub(crate) struct CapturedPeer {
    peer: Arc<Peer>,
    pub(crate) deadline: Instant,
}
impl CapturedPeer {
    pub(crate) fn live(&self) -> BrokerResult<()> {
        if Utc::now() >= self.peer.expires_at || Instant::now() >= self.deadline {
            Err(denied())
        } else {
            Ok(())
        }
    }
    fn destination(&self, to: &str, kind: InboxKind) -> bool {
        mailbox_identifier(to)
            && self
                .peer
                .destinations
                .iter()
                .any(|d| d.mailbox == to && d.kinds.contains(&kind))
    }
    fn batch(&self, batch: &ActorEventBatch) -> bool {
        batch.validate().is_ok()
            && batch.source_actor_id.as_deref() == Some(self.peer.mailbox.as_str())
            && batch
                .source_node_id
                .as_deref()
                .is_none_or(|s| s == self.peer.host)
            && batch.activation_id.as_deref().is_none_or(identifier)
            && batch.logical_session.as_ref().is_none_or(|s| {
                identifier(&s.session_id)
                    && identifier(&s.root_session_id)
                    && s.parent_session_id.as_deref().is_none_or(identifier)
            })
    }
    pub(crate) fn admit(&self, frame: &ClientFrame) -> BrokerResult<()> {
        self.live()?;
        let allowed = match frame {
            ClientFrame::Hello { .. } => false,
            ClientFrame::Subscribe => true,
            ClientFrame::Ack { id } => identifier(id.as_str()),
            ClientFrame::Cancel { to, correlation_id } => {
                mailbox_identifier(to)
                    && identifier(correlation_id.as_str())
                    && self.peer.cancel.contains(to)
            }
            ClientFrame::ListConnected { role } => {
                identifier(role) && self.peer.presence.contains(role)
            }
            ClientFrame::Deliver { to, message } => {
                self.destination(to, message.kind)
                    && identifier(message.id.as_str())
                    && message
                        .correlation_id
                        .as_ref()
                        .is_none_or(|id| identifier(id.as_str()))
                    && message.from.session_id == self.peer.mailbox
                    && message.from.role == self.peer.role
                    && (message.kind != InboxKind::Event
                        || serde_json::from_value::<ActorEventBatch>(message.body.clone())
                            .is_ok_and(|b| b.qos == ActorEventQos::Durable && self.batch(&b)))
            }
            ClientFrame::PublishEventBatch {
                to,
                correlation_id,
                batch,
            } => {
                self.destination(to, InboxKind::Event)
                    && identifier(correlation_id.as_str())
                    && batch.qos != ActorEventQos::Durable
                    && self.batch(batch)
            }
        };
        if allowed {
            Ok(())
        } else {
            Err(denied())
        }
    }
}
