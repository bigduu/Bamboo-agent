//! Wire types for the v2 unified WebSocket multiplex (`GET /v2/stream`).
//!
//! The envelope is a thin shell around the **existing** event schemas — the
//! inner `event` is a byte-for-byte `AgentEvent` (for `agent.{sid}`) or a whole
//! `ChangeEvent` (for `feed`). v2 only changes transport + framing, never the
//! business event payload (see `docs/api-v2-transport.md` §5.3).

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The wire encoding negotiated for a `/v2/stream` connection (v2-P3, §7.2).
///
/// Selected ONCE at the upgrade from the offered `Sec-WebSocket-Protocol`
/// subprotocols and carried for the connection's lifetime. JSON is the default
/// (desktop / debuggability); `bamboo.v2.msgpack` switches the SAME envelope
/// schema to binary MessagePack. The logical schema is byte-for-byte identical —
/// only the serialization + WS frame type (Text vs Binary) differs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Encoding {
    /// JSON text frames (`bamboo.v2`, the default). Today's behavior, unchanged.
    Json,
    /// MessagePack binary frames (`bamboo.v2.msgpack`).
    Msgpack,
}

/// The subprotocol token a `bamboo.v2.msgpack` client offers / the server echoes.
pub(crate) const SUBPROTOCOL_MSGPACK: &str = "bamboo.v2.msgpack";
/// The subprotocol token for the default JSON encoding.
pub(crate) const SUBPROTOCOL_JSON: &str = "bamboo.v2";

/// An already-encoded outbound frame, tagged by WS frame type.
///
/// The forwarders encode a [`ServerEnvelope`] per the connection's [`Encoding`]
/// up front and push one of these onto the per-channel queue; the driver writes
/// `Text` via `session.text` and `Binary` via `session.binary`. This keeps the
/// final encode out of the driver's hot select loop and makes the encoding
/// per-connection rather than per-write.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum OutFrame {
    /// A JSON text frame (`Encoding::Json`).
    Text(String),
    /// A MessagePack binary frame (`Encoding::Msgpack`).
    Binary(Vec<u8>),
}

/// Encode a top-level server control frame in the connection's negotiated
/// representation. These frames deliberately do not use a channel envelope.
fn top_level_type_frame(encoding: Encoding, frame_type: &'static str) -> Option<OutFrame> {
    #[derive(Serialize)]
    struct TypeFrame {
        r#type: &'static str,
    }

    let frame = TypeFrame { r#type: frame_type };
    match encoding {
        Encoding::Json => serde_json::to_string(&frame).ok().map(OutFrame::Text),
        Encoding::Msgpack => rmp_serde::to_vec_named(&frame).ok().map(OutFrame::Binary),
    }
}

/// Encode the acknowledgement for an authorized client `hello`.
///
/// The exact logical shape is `{"type":"welcome"}` in both encodings. It
/// intentionally carries no identity, credential, server configuration, or
/// channel data. The caller must write this frame directly to the socket so an
/// acknowledgement cannot be dropped by a best-effort control queue.
pub(crate) fn welcome_frame(encoding: Encoding) -> Option<OutFrame> {
    top_level_type_frame(encoding, "welcome")
}

/// Encode the application-level heartbeat acknowledgement.
///
/// This deliberately is a top-level frame (`{"type":"pong"}`), not a
/// channel envelope: clients use it to prove that a frame made a complete
/// round trip through the application read loop.
pub(crate) fn pong_frame(encoding: Encoding) -> Option<OutFrame> {
    top_level_type_frame(encoding, "pong")
}

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

    /// Encode to an [`OutFrame`] per the connection's [`Encoding`].
    ///
    /// JSON → a text frame (today's behavior, byte-for-byte). Msgpack →
    /// `rmp_serde::to_vec_named`, which serializes structs as MAPS (named
    /// fields). This is REQUIRED: `ServerEnvelope` uses `#[serde(flatten)]` over
    /// an `#[serde(untagged)]` body, and rmp-serde's default `to_vec` writes
    /// structs as positional ARRAYS, which breaks both flatten and untagged
    /// resolution. With `to_vec_named` the logical wire schema is identical to
    /// the JSON form (`{ch, seq, event}` / `{ch, seq, control}`), just msgpack.
    ///
    /// A serialization failure yields `None` — the caller skips the frame and
    /// keeps the forwarder alive (matches the v1 SSE `to_string(...).ok()`
    /// discipline).
    pub(crate) fn encode(&self, encoding: Encoding) -> Option<OutFrame> {
        match encoding {
            Encoding::Json => self.to_text().map(OutFrame::Text),
            Encoding::Msgpack => rmp_serde::to_vec_named(self).ok().map(OutFrame::Binary),
        }
    }
}

/// Decode an inbound [`ClientFrame`] per the connection's [`Encoding`].
///
/// JSON mode parses a Text frame's UTF-8 with serde_json (today's behavior).
/// Msgpack mode parses a Binary frame with rmp-serde. Both honor the
/// `#[serde(other)] Unknown` fallback, so an unrecognized `type` tag decodes to
/// [`ClientFrame::Unknown`] rather than erroring; a truly malformed body is an
/// `Err` the driver logs-and-ignores (it never tears down the connection).
pub(crate) fn decode_client_frame(encoding: Encoding, bytes: &[u8]) -> Result<ClientFrame, String> {
    match encoding {
        Encoding::Json => serde_json::from_slice(bytes).map_err(|e| e.to_string()),
        Encoding::Msgpack => rmp_serde::from_slice(bytes).map_err(|e| e.to_string()),
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

/// A gap control payload for an `agent.{sid}` channel (#543): the broadcast
/// ring overran while this connection was slow and `skipped` events were lost
/// beyond recovery (agent events have no durable journal, unlike the feed).
/// The client must reconcile the session's authoritative state via REST — the
/// transcript it derived from the live stream may be missing tool results or
/// whole turns.
pub(crate) fn gap_control(skipped: u64) -> Value {
    serde_json::json!({ "type": "gap", "skipped": skipped })
}

/// The app-level keepalive envelope sent on every ping tick (#533):
/// `{ch:"sys", seq:0, control:{type:"keepalive"}}`.
///
/// Browsers never expose protocol-level ping frames to JS, so this DATA frame
/// is the client's only observable liveness signal — the lotus watchdog forces
/// a reconnect when it stops arriving (a half-open socket after sleep/wake or
/// NAT idle eviction never fires `onclose` on its own). `seq` is fixed at 0:
/// the `sys` channel carries no ordered stream to resume.
pub(crate) fn sys_keepalive_envelope() -> ServerEnvelope {
    ServerEnvelope::control("sys", 0, serde_json::json!({ "type": "keepalive" }))
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
    /// Application-level heartbeat probe. Unlike WebSocket protocol Ping/Pong,
    /// this is visible to browser JavaScript and receives a top-level `pong`.
    Ping,
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
    fn client_ping_and_json_pong_wire_shapes() {
        let ping: ClientFrame = serde_json::from_str(r#"{"type":"ping"}"#).unwrap();
        assert_eq!(ping, ClientFrame::Ping);
        assert_eq!(
            pong_frame(Encoding::Json),
            Some(OutFrame::Text(r#"{"type":"pong"}"#.to_string()))
        );
    }

    #[test]
    fn msgpack_pong_is_a_top_level_named_map() {
        let OutFrame::Binary(bytes) = pong_frame(Encoding::Msgpack).expect("encodes") else {
            panic!("msgpack pong must be binary");
        };
        let value: Value = rmp_serde::from_slice(&bytes).expect("decodes");
        assert_eq!(value, json!({ "type": "pong" }));
    }

    #[test]
    fn welcome_is_exact_top_level_shape_in_json_and_msgpack_and_secret_free() {
        let json_frame = welcome_frame(Encoding::Json).expect("JSON welcome encodes");
        assert_eq!(
            json_frame,
            OutFrame::Text(r#"{"type":"welcome"}"#.to_string())
        );

        let OutFrame::Binary(msgpack_bytes) =
            welcome_frame(Encoding::Msgpack).expect("msgpack welcome encodes")
        else {
            panic!("msgpack welcome must be binary");
        };
        let msgpack_value: Value =
            rmp_serde::from_slice(&msgpack_bytes).expect("msgpack welcome decodes");
        assert_eq!(msgpack_value, json!({ "type": "welcome" }));

        let serialized = match json_frame {
            OutFrame::Text(text) => text,
            OutFrame::Binary(_) => unreachable!("JSON welcome is text"),
        };
        for forbidden in [
            "token",
            "device",
            "credential",
            "config",
            "password",
            "secret",
            "server",
        ] {
            assert!(
                !serialized.contains(forbidden),
                "welcome leaked forbidden metadata: {forbidden}"
            );
        }
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

    // ── msgpack round-trips (v2-P3, #181) ─────────────────────────────────────
    //
    // The CRITICAL gotcha: `ServerEnvelope` has `#[serde(flatten)]` over an
    // untagged body, and rmp-serde's default `to_vec` writes structs as positional
    // ARRAYS, which breaks both. These tests pin that `to_vec_named` (structs as
    // MAPS) round-trips the SAME logical schema as JSON. We decode back to a
    // `serde_json::Value` (msgpack maps → JSON objects) so the assertions are on
    // the exact field names/values the JSON form carries.

    #[test]
    fn server_envelope_event_msgpack_roundtrips_to_same_schema() {
        let env = ServerEnvelope::event(
            "agent.sess_abc",
            42,
            json!({ "type": "token", "content": "Hello" }),
        );
        let frame = env.encode(Encoding::Msgpack).expect("msgpack encode");
        let OutFrame::Binary(bytes) = frame else {
            panic!("msgpack encoding must yield a Binary frame");
        };
        // Decode the msgpack bytes back to a JSON Value: maps → objects, so the
        // logical shape must equal the JSON form exactly.
        let v: Value = rmp_serde::from_slice(&bytes).expect("msgpack decodes to Value");
        assert_eq!(
            v,
            json!({
                "ch": "agent.sess_abc",
                "seq": 42,
                "event": { "type": "token", "content": "Hello" }
            }),
            "to_vec_named must preserve flatten + untagged as a {{ch,seq,event}} map"
        );
        assert_eq!(v.as_object().unwrap().len(), 3, "no wrapper nesting");
    }

    #[test]
    fn server_envelope_control_msgpack_roundtrips_to_same_schema() {
        let env = ServerEnvelope::control("agent.sess_abc", 43, terminal_control("complete"));
        let frame = env.encode(Encoding::Msgpack).expect("msgpack encode");
        let OutFrame::Binary(bytes) = frame else {
            panic!("msgpack encoding must yield a Binary frame");
        };
        let v: Value = rmp_serde::from_slice(&bytes).expect("msgpack decodes to Value");
        assert_eq!(
            v,
            json!({
                "ch": "agent.sess_abc",
                "seq": 43,
                "control": { "type": "terminal", "reason": "complete" }
            }),
            "the untagged Control arm must round-trip as {{ch,seq,control}}"
        );
    }

    #[test]
    fn server_envelope_json_encode_is_unchanged_text() {
        // The JSON encoding path is byte-for-byte the existing `to_text`.
        let env = ServerEnvelope::event("feed", 7, json!({ "type": "x" }));
        let frame = env.encode(Encoding::Json).expect("json encode");
        assert_eq!(frame, OutFrame::Text(env.to_text().unwrap()));
    }

    #[test]
    fn client_frame_all_variants_msgpack_roundtrip() {
        // Each variant encodes (as a tagged map) and decodes back identically.
        for original in [
            ClientFrame::Hello {
                device_id: Some("d1".into()),
                token: Some("bd1_x".into()),
            },
            ClientFrame::Hello {
                device_id: None,
                token: None,
            },
            ClientFrame::Subscribe {
                ch: "feed".into(),
                since: Some(1006),
            },
            ClientFrame::Subscribe {
                ch: "agent.s1".into(),
                since: None,
            },
            ClientFrame::Unsubscribe {
                ch: "agent.s1".into(),
            },
            ClientFrame::Stop {
                session_id: "s1".into(),
            },
            ClientFrame::Ping,
        ] {
            // ClientFrame is Deserialize-only; encode the equivalent JSON Value to
            // msgpack (the same bytes a client would send) and decode it back.
            let as_json = match &original {
                ClientFrame::Hello { device_id, token } => {
                    json!({ "type": "hello", "device_id": device_id, "token": token })
                }
                ClientFrame::Subscribe { ch, since } => {
                    json!({ "type": "subscribe", "ch": ch, "since": since })
                }
                ClientFrame::Unsubscribe { ch } => json!({ "type": "unsubscribe", "ch": ch }),
                ClientFrame::Stop { session_id } => {
                    json!({ "type": "stop", "session_id": session_id })
                }
                ClientFrame::Ping => json!({ "type": "ping" }),
                ClientFrame::Unknown => unreachable!(),
            };
            let bytes = rmp_serde::to_vec_named(&as_json).expect("encode client frame as msgpack");
            let decoded = decode_client_frame(Encoding::Msgpack, &bytes).expect("decode");
            assert_eq!(decoded, original, "msgpack client frame must round-trip");
        }
    }

    #[test]
    fn client_frame_unknown_tag_msgpack_maps_to_unknown_not_error() {
        // An unrecognized `type` over msgpack must map to Unknown (serde `other`),
        // NOT an Err that would drop the connection — parity with the JSON path.
        let bytes =
            rmp_serde::to_vec_named(&json!({ "type": "execute", "session_id": "s1" })).unwrap();
        let decoded =
            decode_client_frame(Encoding::Msgpack, &bytes).expect("unknown tag is not Err");
        assert_eq!(decoded, ClientFrame::Unknown);
    }

    #[test]
    fn client_frame_malformed_msgpack_is_err_not_panic() {
        // Random bytes that are not a valid msgpack map → Err, which the driver
        // logs + ignores (no disconnect).
        let r = decode_client_frame(Encoding::Msgpack, &[0xc1, 0x00, 0xff, 0x10]);
        assert!(r.is_err());
        // A valid msgpack value missing the `type` tag → also Err (no default tag),
        // same as the JSON path.
        let bytes = rmp_serde::to_vec_named(&json!({ "ch": "feed" })).unwrap();
        assert!(decode_client_frame(Encoding::Msgpack, &bytes).is_err());
    }

    #[test]
    fn decode_client_frame_json_matches_serde_json() {
        // The JSON decode path is unchanged: same result as direct serde_json.
        let text = r#"{"type":"subscribe","ch":"feed","since":5}"#;
        let decoded = decode_client_frame(Encoding::Json, text.as_bytes()).unwrap();
        assert_eq!(
            decoded,
            ClientFrame::Subscribe {
                ch: "feed".into(),
                since: Some(5)
            }
        );
    }

    #[test]
    fn channel_parse() {
        assert_eq!(Channel::parse("feed"), Some(Channel::Feed));
        assert_eq!(
            Channel::parse("agent.sess_abc"),
            Some(Channel::Agent("sess_abc".to_string()))
        );
        assert_eq!(Channel::parse("agent."), None);
        assert_eq!(Channel::parse("sys"), None, "sys is connection-reserved");
        assert_eq!(Channel::parse("bogus"), None);
    }

    /// #533: the sys keepalive wire shape is a CONTRACT with the lotus
    /// watchdog (`ch === "sys"` + `control.type === "keepalive"`) — lock the
    /// exact JSON so a refactor can't silently break client liveness.
    #[test]
    fn sys_keepalive_wire_shape() {
        let text = sys_keepalive_envelope().to_text().expect("serializes");
        assert_eq!(
            text,
            r#"{"ch":"sys","seq":0,"control":{"type":"keepalive"}}"#
        );
    }
}
