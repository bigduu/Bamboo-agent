//! Wire types for the v2 unified WebSocket multiplex (`GET /v2/stream`).
//!
//! The envelope is a thin shell around the **existing** event schemas — the
//! inner `event` is a byte-for-byte `AgentEvent` (for `agent.{sid}`) or a whole
//! `ChangeEvent` (for `feed`). v2 only changes transport + framing, never the
//! business event payload (see `docs/api-v2-transport.md` §5.3).

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A server→client envelope.
///
/// Serializes as one of two shapes sharing `{ch, seq}`:
///
/// ```jsonc
/// { "ch": "agent.sess_abc", "seq": 42, "event": { "type": "token", "content": "Hi" } }
/// { "ch": "agent.sess_abc", "seq": 43, "control": { "type": "terminal", "reason": "complete" } }
/// ```
///
/// The `event` / `control` keys are mutually exclusive and flattened in, so the
/// wire object is exactly `{ch, seq, event}` or `{ch, seq, control}` — no extra
/// nesting.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct ServerEnvelope {
    /// Channel id: `feed` or `agent.{session_id}`.
    pub ch: String,
    /// Per-channel monotonic sequence number.
    ///
    /// For `feed` this is `ChangeEvent.seq` — a durable, cross-connection cursor
    /// the client passes back as `subscribe.since` to resume losslessly.
    ///
    /// For `agent.{sid}` it is a server-maintained counter that is **per
    /// subscription**, not a durable resume cursor: it restarts at 1 on each
    /// (re)subscribe. Agent resume is replay-cache-only (RFC §10-Q2 — a long
    /// disconnect re-fetches session detail via REST), so `agent` `since` is not
    /// honored as a lossless cursor and this counter is for ordering/dedup within
    /// one subscription only.
    pub seq: u64,
    /// The carried payload: either a journaled/agent `event` or a transport
    /// `control` signal.
    #[serde(flatten)]
    pub body: EnvelopeBody,
}

/// The mutually-exclusive payload of a [`ServerEnvelope`].
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(untagged)]
pub(crate) enum EnvelopeBody {
    /// A business event, reusing the existing `AgentEvent` / `ChangeEvent` JSON
    /// verbatim. We carry it as an opaque [`Value`] so the envelope never
    /// re-encodes (and so cannot drift from) the underlying schema.
    Event {
        /// The inner event JSON, byte-for-byte the existing schema.
        event: Value,
    },
    /// A transport control / terminal marker (e.g. channel `terminal`,
    /// `feed_reset`).
    Control {
        /// The control payload JSON.
        control: Value,
    },
}

impl ServerEnvelope {
    /// Build an `{ch, seq, event}` envelope wrapping an already-serialized inner
    /// event value.
    pub(crate) fn event(ch: impl Into<String>, seq: u64, event: Value) -> Self {
        Self {
            ch: ch.into(),
            seq,
            body: EnvelopeBody::Event { event },
        }
    }

    /// Build an `{ch, seq, control}` envelope.
    pub(crate) fn control(ch: impl Into<String>, seq: u64, control: Value) -> Self {
        Self {
            ch: ch.into(),
            seq,
            body: EnvelopeBody::Control { control },
        }
    }

    /// Serialize to a JSON text frame.
    pub(crate) fn to_text(&self) -> Option<String> {
        serde_json::to_string(self).ok()
    }
}

/// A terminal control payload for an `agent.{sid}` channel: the agent run
/// finished. `reason` mirrors the v1 terminal event class.
pub(crate) fn terminal_control(reason: &str) -> Value {
    serde_json::json!({ "type": "terminal", "reason": reason })
}

/// A feed reset control payload: the client's cursor predated the retained
/// window, so it must drop local state and full-resync. Mirrors the v1 SSE
/// `feed_reset` frame shape.
pub(crate) fn feed_reset_control(from_seq: u64) -> Value {
    serde_json::json!({ "type": "feed_reset", "from_seq": from_seq })
}

/// A client→server frame, tagged by `type`.
///
/// Unknown / malformed frames deserialize to [`ClientFrame::Unknown`] (via the
/// serde `other` fallback for the tag, or a parse error the driver catches) so a
/// bad frame logs-and-continues instead of tearing down the connection.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ClientFrame {
    /// First frame (auth in v2-P2). Accepted and ignored in P1 — auth is the
    /// scope middleware only.
    Hello {
        #[serde(default)]
        device_id: Option<String>,
        #[serde(default)]
        token: Option<String>,
    },
    /// Subscribe to a channel, optionally resuming from a cursor.
    Subscribe {
        ch: String,
        #[serde(default)]
        since: Option<u64>,
    },
    /// Unsubscribe from a channel.
    Unsubscribe { ch: String },
    /// Cancel a running session (the only `control` uplink in P1).
    Stop { session_id: String },
    /// Any frame whose `type` is not recognized. The driver logs and ignores it
    /// rather than disconnecting.
    #[serde(other)]
    Unknown,
}

/// The kind of channel a `ch` string names.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Channel {
    /// The account-wide change feed.
    Feed,
    /// A per-session agent event stream.
    Agent(String),
}

impl Channel {
    /// Parse a `ch` wire string into a [`Channel`], or `None` if unrecognized.
    pub(crate) fn parse(ch: &str) -> Option<Channel> {
        if ch == "feed" {
            Some(Channel::Feed)
        } else if let Some(sid) = ch.strip_prefix("agent.") {
            if sid.is_empty() {
                None
            } else {
                Some(Channel::Agent(sid.to_string()))
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn server_envelope_event_shape() {
        let env = ServerEnvelope::event(
            "agent.sess_abc",
            42,
            json!({ "type": "token", "content": "Hello" }),
        );
        let v = serde_json::to_value(&env).unwrap();
        assert_eq!(
            v,
            json!({
                "ch": "agent.sess_abc",
                "seq": 42,
                "event": { "type": "token", "content": "Hello" }
            })
        );
        // Exactly three keys, no nesting under a wrapper.
        assert_eq!(v.as_object().unwrap().len(), 3);
    }

    #[test]
    fn server_envelope_control_shape() {
        let env = ServerEnvelope::control("agent.sess_abc", 43, terminal_control("complete"));
        let v = serde_json::to_value(&env).unwrap();
        assert_eq!(
            v,
            json!({
                "ch": "agent.sess_abc",
                "seq": 43,
                "control": { "type": "terminal", "reason": "complete" }
            })
        );
    }

    #[test]
    fn feed_reset_control_shape() {
        let env = ServerEnvelope::control("feed", 0, feed_reset_control(1006));
        let v = serde_json::to_value(&env).unwrap();
        assert_eq!(
            v["control"],
            json!({ "type": "feed_reset", "from_seq": 1006 })
        );
    }

    #[test]
    fn client_frame_hello_parses() {
        let f: ClientFrame =
            serde_json::from_str(r#"{"type":"hello","device_id":"d1","token":"bd1_x"}"#).unwrap();
        assert_eq!(
            f,
            ClientFrame::Hello {
                device_id: Some("d1".to_string()),
                token: Some("bd1_x".to_string())
            }
        );
        // Hello with no fields still parses (auth ignored in P1).
        let f: ClientFrame = serde_json::from_str(r#"{"type":"hello"}"#).unwrap();
        assert_eq!(
            f,
            ClientFrame::Hello {
                device_id: None,
                token: None
            }
        );
    }

    #[test]
    fn client_frame_subscribe_parses_with_and_without_cursor() {
        let f: ClientFrame =
            serde_json::from_str(r#"{"type":"subscribe","ch":"feed","since":1006}"#).unwrap();
        assert_eq!(
            f,
            ClientFrame::Subscribe {
                ch: "feed".to_string(),
                since: Some(1006)
            }
        );
        let f: ClientFrame =
            serde_json::from_str(r#"{"type":"subscribe","ch":"agent.s1"}"#).unwrap();
        assert_eq!(
            f,
            ClientFrame::Subscribe {
                ch: "agent.s1".to_string(),
                since: None
            }
        );
    }

    #[test]
    fn client_frame_unsubscribe_and_stop_parse() {
        let f: ClientFrame =
            serde_json::from_str(r#"{"type":"unsubscribe","ch":"agent.s1"}"#).unwrap();
        assert_eq!(
            f,
            ClientFrame::Unsubscribe {
                ch: "agent.s1".to_string()
            }
        );
        let f: ClientFrame = serde_json::from_str(r#"{"type":"stop","session_id":"s1"}"#).unwrap();
        assert_eq!(
            f,
            ClientFrame::Stop {
                session_id: "s1".to_string()
            }
        );
    }

    #[test]
    fn unknown_frame_type_maps_to_unknown_not_error() {
        // An unrecognized `type` must NOT fail the parse — it maps to Unknown so
        // the driver logs + continues instead of disconnecting.
        let f: ClientFrame =
            serde_json::from_str(r#"{"type":"execute","session_id":"s1","message":"hi"}"#).unwrap();
        assert_eq!(f, ClientFrame::Unknown);
    }

    #[test]
    fn malformed_json_is_a_parse_error_caught_by_driver() {
        // Not valid JSON at all → Err. The driver treats this the same as
        // Unknown: log + ignore, no disconnect.
        let r: Result<ClientFrame, _> = serde_json::from_str("not json{");
        assert!(r.is_err());
        // A JSON object missing the `type` tag → also an error (no default tag).
        let r: Result<ClientFrame, _> = serde_json::from_str(r#"{"ch":"feed"}"#);
        assert!(r.is_err());
    }

    #[test]
    fn channel_parse() {
        assert_eq!(Channel::parse("feed"), Some(Channel::Feed));
        assert_eq!(
            Channel::parse("agent.sess_abc"),
            Some(Channel::Agent("sess_abc".to_string()))
        );
        assert_eq!(Channel::parse("agent."), None);
        assert_eq!(Channel::parse("bogus"), None);
    }
}
