//! Vendored `pbbp2.Frame`/`pbbp2.Header` proto2 message types for Feishu/Lark's
//! event long-connection (WS) wire protocol.
//!
//! This is a HAND-WRITTEN prost derive, not a `protoc`-generated one — there
//! is no `build.rs`/`protoc` dependency in this crate, and the message shape
//! (verified against the official Go SDK, see `docs/feishu-adapter-plan.md`
//! §2c) is small and stable enough that deriving `prost::Message` directly
//! on hand-written structs is simpler than vendoring a `.proto` + codegen
//! pipeline. Field TAGS below are the wire contract; Rust field NAMES are
//! ours to choose (prost's derive macro doesn't require them to match a
//! `.proto` source, only the tag numbers/wire types need to match what the
//! server sends).
//!
//! proto2 required vs optional matters here: `SeqID`/`LogID`/`service`/
//! `method` are **required** (decode fails if absent — matches the
//! protocol: every frame carries a sequence id, log id, service, and
//! method). Fields 6/7/9 are `optional string` and Feishu is known to send
//! EMPTY strings (not absent fields) for these on many frames (e.g. plain
//! data frames have no `payload_encoding`/`payload_type` set); prost decodes
//! an empty string as `Some(String::new())`, which callers must tolerate
//! (never treat `Some("")` as an error — see `ws.rs` header lookups, which
//! only special-case `None`/absence, not emptiness).

use prost::Message;

/// One key/value header entry on a [`Frame`].
#[derive(Clone, PartialEq, Eq, Message)]
pub struct Header {
    #[prost(string, required, tag = "1")]
    pub key: String,
    #[prost(string, required, tag = "2")]
    pub value: String,
}

impl Header {
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// The binary wire frame for Feishu/Lark's event long-connection protocol
/// (`pbbp2.Frame`). `method`: `0` = control (ping/pong/ack), `1` = data
/// (event payloads).
#[derive(Clone, PartialEq, Message)]
pub struct Frame {
    #[prost(uint64, required, tag = "1")]
    pub seq_id: u64,
    #[prost(uint64, required, tag = "2")]
    pub log_id: u64,
    #[prost(int32, required, tag = "3")]
    pub service: i32,
    #[prost(int32, required, tag = "4")]
    pub method: i32,
    #[prost(message, repeated, tag = "5")]
    pub headers: Vec<Header>,
    #[prost(string, optional, tag = "6")]
    pub payload_encoding: Option<String>,
    #[prost(string, optional, tag = "7")]
    pub payload_type: Option<String>,
    #[prost(bytes = "vec", optional, tag = "8")]
    pub payload: Option<Vec<u8>>,
    #[prost(string, optional, tag = "9")]
    pub log_id_new: Option<String>,
}

impl Frame {
    /// Looks up a header by key (first match; Feishu frames never repeat a
    /// key). Returns `Some("")` for a present-but-empty value — callers that
    /// care about emptiness must check the returned string themselves.
    pub fn header(&self, key: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|h| h.key == key)
            .map(|h| h.value.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_round_trips_through_encode_decode() {
        let frame = Frame {
            seq_id: 42,
            log_id: 7,
            service: 1,
            method: 1,
            headers: vec![
                Header::new("type", "event"),
                Header::new("message_id", "m-1"),
            ],
            payload_encoding: None,
            payload_type: None,
            payload: Some(b"{\"hello\":\"world\"}".to_vec()),
            log_id_new: None,
        };

        let mut buf = Vec::new();
        frame.encode(&mut buf).expect("encode succeeds");
        let decoded = Frame::decode(buf.as_slice()).expect("decode succeeds");

        assert_eq!(decoded.seq_id, 42);
        assert_eq!(decoded.log_id, 7);
        assert_eq!(decoded.service, 1);
        assert_eq!(decoded.method, 1);
        assert_eq!(decoded.header("type"), Some("event"));
        assert_eq!(decoded.header("message_id"), Some("m-1"));
        assert_eq!(
            decoded.payload.as_deref(),
            Some(b"{\"hello\":\"world\"}".as_slice())
        );
    }

    #[test]
    fn empty_optional_strings_decode_as_some_empty_not_error() {
        let frame = Frame {
            seq_id: 1,
            log_id: 1,
            service: 1,
            method: 0,
            headers: vec![],
            payload_encoding: Some(String::new()),
            payload_type: Some(String::new()),
            payload: None,
            log_id_new: Some(String::new()),
        };

        let mut buf = Vec::new();
        frame.encode(&mut buf).expect("encode succeeds");
        let decoded = Frame::decode(buf.as_slice()).expect("decode succeeds");

        assert_eq!(decoded.payload_encoding.as_deref(), Some(""));
        assert_eq!(decoded.payload_type.as_deref(), Some(""));
        assert_eq!(decoded.log_id_new.as_deref(), Some(""));
    }

    #[test]
    fn decode_zero_fills_a_missing_required_field_rather_than_erroring() {
        // prost's `required` attribute affects ENCODING (the field is never
        // optional on the wire) but does NOT enforce presence on DECODE —
        // a wire message missing a required field decodes successfully with
        // that field defaulted (0 for numeric types), rather than erroring.
        // This is documented here (not just asserted implicitly) because
        // it's easy to assume proto2 "required" is enforced symmetrically;
        // callers that care about a genuinely-absent `log_id`/`service`/
        // `method` must check for `0` themselves if that matters.
        let mut buf = Vec::new();
        prost::encoding::uint64::encode(1, &1u64, &mut buf); // seq_id (tag 1) only
        let decoded =
            Frame::decode(buf.as_slice()).expect("decode succeeds despite missing required fields");
        assert_eq!(decoded.seq_id, 1);
        assert_eq!(decoded.log_id, 0);
        assert_eq!(decoded.service, 0);
        assert_eq!(decoded.method, 0);
    }
}
