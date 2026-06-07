//! The sequenced change-feed event type.
//!
//! A [`ChangeEvent`] is an [`AgentEvent`] stamped with a global monotonic
//! sequence number and routing metadata. It is the unit carried on the account
//! `GET /api/v1/stream` feed and persisted, one JSON line each, in the durable
//! journal (see [`super::journal`]).
//!
//! Classification of which events are durable lives on the event type itself:
//! [`AgentEvent::is_durable_change`]. Keeping it in `bamboo-agent-core` lets
//! both the server and the engine forwarder filter before cloning onto the
//! feed.

use bamboo_agent_core::AgentEvent;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// An [`AgentEvent`] stamped with its global sequence number for the account
/// change feed.
///
/// The `seq` of a [`AgentEvent::MessageAppended`] event is also the message's
/// feed coordinate, used by `GET /history/{id}?since={seq}` to compute deltas.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    /// Global monotonic sequence number (account-wide, 1-based, strictly
    /// increasing, stable across restarts).
    pub seq: u64,
    /// When the writer task stamped this event.
    pub ts: DateTime<Utc>,
    /// Session this event pertains to, if any (client-side routing key).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// The underlying agent event.
    pub event: AgentEvent,
}
