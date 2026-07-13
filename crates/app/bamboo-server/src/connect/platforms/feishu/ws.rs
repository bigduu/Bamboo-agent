//! Feishu/Lark event long-connection (WS) client: bootstrap, ping, frame
//! ack, multi-part reassembly, reconnect. No public IP / webhook needed —
//! this is the sole inbound transport (see `docs/feishu-adapter-plan.md`
//! §2c, protocol verified against the official Go SDK).
//!
//! Protocol logic (frame building/parsing, bootstrap-response
//! classification, multi-part reassembly, reconnect backoff) is factored
//! into small pure/near-pure functions so it's unit-testable without a live
//! socket — the actual `tokio-tungstenite` connection driver
//! ([`run_with_reconnect`]) is thin glue over them, per the task brief's
//! "prefer unit-testing frame handling functions directly" guidance.

use std::sync::Arc;
use std::time::Duration;

use futures::{Sink, SinkExt, StreamExt};
use prost::Message as _;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message as WsMessage;

use super::super::super::platform::{PlatformError, PlatformResult};
use super::api::{http_client, sanitize_reqwest_error};
use super::pbbp2::{Frame, Header};

/// Hard deadline (server-imposed) to ack a data frame — after this the
/// server re-pushes the same event. [`ACK_SOFT_DEADLINE`] is what
/// `mod.rs`'s pending-ack map self-imposes (auto-resolve with a default
/// toast) so this transport-level wrapper always has headroom left to
/// actually get the ack frame back onto the wire.
const ACK_HARD_DEADLINE: Duration = Duration::from_millis(2900);
/// How often to check for read-idle disconnects (frames NOT received for
/// longer than `ping_interval * IDLE_TIMEOUT_MULTIPLIER`).
const IDLE_CHECK_INTERVAL: Duration = Duration::from_secs(10);
const IDLE_TIMEOUT_MULTIPLIER: u32 = 3;
const MIN_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// Multi-part reassembly TTL (5s per spec — a part that's been waiting on
/// its siblings longer than this is dropped).
const REASSEMBLY_TTL: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------

/// Server-provided long-connection tuning, seconds-denominated (PascalCase
/// on the wire: `PingInterval`/`ReconnectInterval`/`ReconnectNonce`/
/// `ReconnectCount`). Defaults match the documented Feishu defaults, used
/// when a bootstrap response omits `ClientConfig` entirely.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ClientConfig {
    pub ping_interval: Duration,
    pub reconnect_interval: Duration,
    pub reconnect_nonce: Duration,
    /// `-1` = infinite. This adapter always retries infinitely regardless
    /// of this value (per task spec) — kept for completeness/logging only.
    pub reconnect_count: i64,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            ping_interval: Duration::from_secs(120),
            reconnect_interval: Duration::from_secs(120),
            reconnect_nonce: Duration::from_secs(30),
            reconnect_count: -1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BootstrapReady {
    pub url: String,
    pub client_config: ClientConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum BootstrapOutcome {
    Ready(BootstrapReady),
    /// Transient bootstrap failure (code `1` or `1000040343`) — retry.
    Retryable,
    /// Unrecoverable — stop reconnecting entirely.
    Fatal(String),
}

/// PascalCase bootstrap request body: `POST {base}/callback/ws/endpoint`.
pub(super) fn build_bootstrap_body(app_id: &str, app_secret: &str) -> serde_json::Value {
    serde_json::json!({
        "AppID": app_id,
        "AppSecret": app_secret,
        "ClientAssertion": "",
    })
}

#[derive(Debug, serde::Deserialize)]
struct RawBootstrapResponse {
    code: i64,
    #[serde(default)]
    msg: Option<String>,
    #[serde(default)]
    data: Option<RawBootstrapData>,
}

#[derive(Debug, serde::Deserialize)]
struct RawBootstrapData {
    #[serde(rename = "URL")]
    url: String,
    #[serde(rename = "ClientConfig", default)]
    client_config: Option<RawClientConfig>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct RawClientConfig {
    #[serde(rename = "PingInterval", default)]
    ping_interval: Option<u64>,
    #[serde(rename = "ReconnectInterval", default)]
    reconnect_interval: Option<u64>,
    #[serde(rename = "ReconnectNonce", default)]
    reconnect_nonce: Option<u64>,
    #[serde(rename = "ReconnectCount", default)]
    reconnect_count: Option<i64>,
}

/// Classifies a bootstrap HTTP response body per the plan's verified
/// contract: `code == 0` is success; `1` and `1000040343` are transient
/// (retry); anything else is fatal (stop reconnecting entirely).
pub(super) fn parse_bootstrap_response(body: &str) -> PlatformResult<BootstrapOutcome> {
    let parsed: RawBootstrapResponse = serde_json::from_str(body).map_err(|error| {
        PlatformError::other(format!("bootstrap response parse failed: {error}"))
    })?;

    match parsed.code {
        0 => {
            let data = parsed
                .data
                .ok_or_else(|| PlatformError::other("bootstrap response missing data"))?;
            let defaults = ClientConfig::default();
            let raw = data.client_config.unwrap_or_default();
            let client_config = ClientConfig {
                ping_interval: raw
                    .ping_interval
                    .map(Duration::from_secs)
                    .unwrap_or(defaults.ping_interval),
                reconnect_interval: raw
                    .reconnect_interval
                    .map(Duration::from_secs)
                    .unwrap_or(defaults.reconnect_interval),
                reconnect_nonce: raw
                    .reconnect_nonce
                    .map(Duration::from_secs)
                    .unwrap_or(defaults.reconnect_nonce),
                reconnect_count: raw.reconnect_count.unwrap_or(defaults.reconnect_count),
            };
            Ok(BootstrapOutcome::Ready(BootstrapReady {
                url: data.url,
                client_config,
            }))
        }
        1 | 1_000_040_343 => Ok(BootstrapOutcome::Retryable),
        other => Ok(BootstrapOutcome::Fatal(format!(
            "bootstrap failed (code={other}): {}",
            parsed.msg.unwrap_or_default()
        ))),
    }
}

async fn bootstrap_endpoint(
    app_id: &str,
    app_secret: &str,
    base_url: &str,
) -> PlatformResult<BootstrapOutcome> {
    let url = format!("{}/callback/ws/endpoint", base_url.trim_end_matches('/'));
    let body = build_bootstrap_body(app_id, app_secret);
    let response = http_client()
        .post(url)
        .header("locale", "zh")
        .json(&body)
        .send()
        .await
        .map_err(|error| {
            PlatformError::other(format!(
                "WS bootstrap request failed: {}",
                sanitize_reqwest_error(error, &[app_secret])
            ))
        })?;
    let text = response.text().await.map_err(|error| {
        PlatformError::other(format!(
            "WS bootstrap response read failed: {}",
            sanitize_reqwest_error(error, &[app_secret])
        ))
    })?;
    parse_bootstrap_response(&text)
}

/// Parses the `service_id` query parameter off the bootstrap-issued `wss://`
/// URL — echoed back in every ping frame's `service` field.
pub(super) fn parse_service_id(ws_url: &str) -> Option<i32> {
    let parsed = url::Url::parse(ws_url).ok()?;
    parsed
        .query_pairs()
        .find(|(key, _)| key == "service_id")
        .and_then(|(_, value)| value.parse::<i32>().ok())
}

// ---------------------------------------------------------------------
// Frame helpers
// ---------------------------------------------------------------------

pub(super) fn build_ping_frame(seq_id: u64, service_id: i32) -> Frame {
    Frame {
        seq_id,
        log_id: seq_id,
        service: service_id,
        method: 0,
        headers: vec![Header::new("type", "ping")],
        payload_encoding: None,
        payload_type: None,
        payload: None,
        log_id_new: None,
    }
}

pub(super) fn is_event_frame(frame: &Frame) -> bool {
    frame.header("type") == Some("event")
}

pub(super) fn is_pong_frame(frame: &Frame) -> bool {
    frame.header("type") == Some("pong")
}

/// Builds the ack payload body: `{"code":200,"headers":null,"data":<base64
/// or null>}`. `callback_response`, when `Some`, is the JSON value to
/// base64-encode into `data` (used for `card.action.trigger` — the toast/
/// updated-card response); `None` (a plain message event, or an unresolved
/// card callback past its soft deadline) yields `data: null`.
pub(super) fn build_ack_payload(callback_response: Option<&serde_json::Value>) -> Vec<u8> {
    use base64::Engine;
    let data = callback_response
        .map(|value| base64::engine::general_purpose::STANDARD.encode(value.to_string()));
    serde_json::json!({
        "code": 200,
        "headers": null,
        "data": data,
    })
    .to_string()
    .into_bytes()
}

/// Echoes `original` back with its payload replaced by the ack body — same
/// `seq_id`/`log_id`/`service`/`method`/`headers`, per the protocol's "ack =
/// echo the same frame" contract.
pub(super) fn build_ack_frame(original: &Frame, ack_payload: Vec<u8>) -> Frame {
    let mut frame = original.clone();
    frame.payload = Some(ack_payload);
    frame
}

// ---------------------------------------------------------------------
// Multi-part reassembly
// ---------------------------------------------------------------------

struct PendingParts {
    total: usize,
    received: std::collections::HashMap<usize, Vec<u8>>,
    first_seen: Instant,
}

/// Reassembles multi-part data frames keyed by their `message_id` header,
/// using the `sum`/`seq` headers to know the total part count and this
/// part's index. A frame with no `sum` header (or `sum <= 1`) is a
/// single-part message and passes through immediately. Incomplete groups
/// older than [`REASSEMBLY_TTL`] are dropped (never delivered, never acked
/// as a group — an isolated stray part is harmless to lose).
pub(super) struct Reassembler {
    pending: std::collections::HashMap<String, PendingParts>,
    ttl: Duration,
}

impl Reassembler {
    pub fn new(ttl: Duration) -> Self {
        Self {
            pending: std::collections::HashMap::new(),
            ttl,
        }
    }

    /// Feeds one data frame in `now` (threaded explicitly so tests can drive
    /// TTL expiry deterministically). Returns the complete payload once
    /// every part of a multi-part message has arrived.
    pub fn feed(&mut self, frame: &Frame, now: Instant) -> Option<Vec<u8>> {
        self.pending
            .retain(|_, parts| now.duration_since(parts.first_seen) < self.ttl);

        let payload = frame.payload.clone().unwrap_or_default();
        let sum: usize = frame
            .header("sum")
            .and_then(|value| value.parse().ok())
            .unwrap_or(1);
        if sum <= 1 {
            return Some(payload);
        }

        let seq: usize = frame
            .header("seq")
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);
        let message_id = frame.header("message_id").unwrap_or_default().to_string();

        let entry = self
            .pending
            .entry(message_id.clone())
            .or_insert_with(|| PendingParts {
                total: sum,
                received: std::collections::HashMap::new(),
                first_seen: now,
            });
        entry.received.insert(seq, payload);

        if entry.received.len() >= entry.total {
            let mut parts = self
                .pending
                .remove(&message_id)
                .expect("just inserted above");
            let mut full = Vec::new();
            for index in 0..parts.total {
                if let Some(chunk) = parts.received.remove(&index) {
                    full.extend_from_slice(&chunk);
                }
            }
            Some(full)
        } else {
            None
        }
    }
}

// ---------------------------------------------------------------------
// Reconnect backoff
// ---------------------------------------------------------------------

/// `attempt` is the count of consecutive failed reconnect tries SINCE the
/// last successful connection (0 = the first try right after a disconnect).
/// Per spec: the first attempt waits a one-time random jitter in
/// `[0, nonce]`; every attempt after that waits the fixed `interval`.
pub(super) fn reconnect_delay(attempt: u32, nonce: Duration, interval: Duration) -> Duration {
    if attempt == 0 {
        let millis = nonce.as_millis() as u64;
        if millis == 0 {
            Duration::ZERO
        } else {
            use rand::RngExt;
            Duration::from_millis(rand::rng().random_range(0..=millis))
        }
    } else {
        interval
    }
}

/// Fatal handshake conditions per the plan: a `403` `Handshake-Status`
/// header, or auth error code `1000040350` (>50 connections for this app)
/// in `Handshake-Autherrcode`. Either stops reconnecting entirely — NOT a
/// retryable condition.
pub(super) fn fatal_handshake_reason(
    headers: &tokio_tungstenite::tungstenite::http::HeaderMap,
) -> Option<String> {
    let status = headers
        .get("Handshake-Status")
        .and_then(|value| value.to_str().ok());
    if status == Some("403") {
        return Some("handshake rejected: Handshake-Status=403".to_string());
    }
    let autherrcode = headers
        .get("Handshake-Autherrcode")
        .and_then(|value| value.to_str().ok());
    if autherrcode == Some("1000040350") {
        return Some(
            "handshake rejected: too many connections for this app (Handshake-Autherrcode=1000040350)"
                .to_string(),
        );
    }
    None
}

// ---------------------------------------------------------------------
// Connection driver
// ---------------------------------------------------------------------

/// Handles ONE fully-reassembled event-data frame's plaintext schema-2.0 JSON
/// payload. Implemented by `FeishuPlatform` (see `mod.rs`) — kept as a trait
/// here so `ws.rs` stays ignorant of `im.message.receive_v1`/
/// `card.action.trigger` semantics (that mapping, and the card-callback
/// pending-ack map, are `mod.rs`'s job per the module split in the task
/// brief). Returns the value to base64-encode into the frame ack's `data`
/// field (`None` for a plain message event, or an unresolved card callback
/// past ITS OWN soft deadline).
#[async_trait::async_trait]
pub(super) trait EventSink: Send + Sync {
    async fn handle_event(&self, payload: Vec<u8>) -> Option<serde_json::Value>;
}

pub(super) enum ConnectionEnded {
    /// Transient — reconnect (re-bootstrap from scratch).
    Retry,
    /// Unrecoverable — stop the platform's WS loop entirely.
    Fatal(String),
}

struct RunOnceResult {
    ended: ConnectionEnded,
    /// Whether a WS connection was actually established this round (used to
    /// reset the reconnect-attempt counter for jitter purposes even if the
    /// connection died quickly afterward).
    connected: bool,
    /// The server's tuning for this round, when we got far enough to learn
    /// it (used to compute the next reconnect delay with the real values
    /// instead of hardcoded defaults).
    client_config: Option<ClientConfig>,
}

async fn run_once(
    app_id: &str,
    app_secret: &str,
    base_url: &str,
    sink: &Arc<dyn EventSink>,
) -> RunOnceResult {
    let bootstrap = match bootstrap_endpoint(app_id, app_secret, base_url).await {
        Ok(BootstrapOutcome::Ready(ready)) => ready,
        Ok(BootstrapOutcome::Retryable) => {
            tracing::warn!("connect: feishu WS bootstrap transient failure, retrying");
            return RunOnceResult {
                ended: ConnectionEnded::Retry,
                connected: false,
                client_config: None,
            };
        }
        Ok(BootstrapOutcome::Fatal(reason)) => {
            return RunOnceResult {
                ended: ConnectionEnded::Fatal(reason),
                connected: false,
                client_config: None,
            };
        }
        Err(error) => {
            tracing::warn!("connect: feishu WS bootstrap failed, retrying: {error}");
            return RunOnceResult {
                ended: ConnectionEnded::Retry,
                connected: false,
                client_config: None,
            };
        }
    };

    let service_id = parse_service_id(&bootstrap.url).unwrap_or(0);
    let client_config = bootstrap.client_config;

    let (ws_stream, response) = match tokio_tungstenite::connect_async(&bootstrap.url).await {
        Ok(pair) => pair,
        Err(error) => {
            tracing::warn!("connect: feishu WS connect failed, retrying: {error}");
            return RunOnceResult {
                ended: ConnectionEnded::Retry,
                connected: false,
                client_config: Some(client_config),
            };
        }
    };

    if let Some(reason) = fatal_handshake_reason(response.headers()) {
        return RunOnceResult {
            ended: ConnectionEnded::Fatal(reason),
            connected: true,
            client_config: Some(client_config),
        };
    }

    tracing::info!("connect: feishu WS connected (service_id={service_id})");
    let (mut write, mut read) = ws_stream.split();
    let (ack_tx, mut ack_rx) = mpsc::channel::<Frame>(32);
    let mut reassembler = Reassembler::new(REASSEMBLY_TTL);
    let mut seq: u64 = 1;

    let ping_interval = client_config.ping_interval.max(Duration::from_secs(1));
    let idle_timeout = (ping_interval * IDLE_TIMEOUT_MULTIPLIER).max(MIN_IDLE_TIMEOUT);
    let mut last_activity = Instant::now();

    let mut ping_ticker = tokio::time::interval(ping_interval);
    ping_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ping_ticker.tick().await; // consume the immediate first tick
    let mut idle_check = tokio::time::interval(IDLE_CHECK_INTERVAL);
    idle_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let ended = loop {
        tokio::select! {
            _ = ping_ticker.tick() => {
                let frame = build_ping_frame(seq, service_id);
                seq += 1;
                if send_frame(&mut write, &frame).await.is_err() {
                    break ConnectionEnded::Retry;
                }
            }
            _ = idle_check.tick() => {
                if last_activity.elapsed() > idle_timeout {
                    tracing::warn!("connect: feishu WS idle for {:?}, reconnecting", last_activity.elapsed());
                    break ConnectionEnded::Retry;
                }
            }
            ack_frame = ack_rx.recv() => {
                if let Some(ack_frame) = ack_frame {
                    if send_frame(&mut write, &ack_frame).await.is_err() {
                        break ConnectionEnded::Retry;
                    }
                }
            }
            incoming = read.next() => {
                match incoming {
                    Some(Ok(WsMessage::Binary(bytes))) => {
                        last_activity = Instant::now();
                        match Frame::decode(bytes.as_ref()) {
                            Ok(frame) => {
                                if is_pong_frame(&frame) {
                                    continue;
                                }
                                if frame.method != 1 || !is_event_frame(&frame) {
                                    continue;
                                }
                                if let Some(full_payload) = reassembler.feed(&frame, Instant::now()) {
                                    let sink = sink.clone();
                                    let ack_tx = ack_tx.clone();
                                    let frame_for_ack = frame.clone();
                                    tokio::spawn(async move {
                                        let response = tokio::time::timeout(
                                            ACK_HARD_DEADLINE,
                                            sink.handle_event(full_payload),
                                        )
                                        .await
                                        .unwrap_or(None);
                                        let payload = build_ack_payload(response.as_ref());
                                        let ack_frame = build_ack_frame(&frame_for_ack, payload);
                                        let _ = ack_tx.send(ack_frame).await;
                                    });
                                }
                            }
                            Err(error) => {
                                tracing::warn!("connect: feishu WS frame decode failed: {error}");
                            }
                        }
                    }
                    Some(Ok(WsMessage::Close(_))) | None => break ConnectionEnded::Retry,
                    Some(Ok(_)) => {
                        last_activity = Instant::now();
                    }
                    Some(Err(error)) => {
                        tracing::warn!("connect: feishu WS read error, reconnecting: {error}");
                        break ConnectionEnded::Retry;
                    }
                }
            }
        }
    };

    RunOnceResult {
        ended,
        connected: true,
        client_config: Some(client_config),
    }
}

async fn send_frame(
    write: &mut (impl Sink<WsMessage, Error = tokio_tungstenite::tungstenite::Error> + Unpin),
    frame: &Frame,
) -> Result<(), ()> {
    let mut buf = Vec::new();
    if frame.encode(&mut buf).is_err() {
        return Ok(()); // malformed outgoing frame — nothing to send, not a connection error
    }
    write
        .send(WsMessage::Binary(buf.into()))
        .await
        .map_err(|_| ())
}

/// Runs the WS client for the adapter's lifetime: bootstrap → connect →
/// drive frames → on disconnect, wait (jitter-then-fixed backoff) and
/// re-bootstrap, forever — UNLESS a fatal condition is hit (bad
/// credentials, handshake rejection, >50 connections for this app), which
/// stops reconnecting and returns `Err`. Never runs two connections at
/// once: each loop iteration fully awaits [`run_once`] (which owns the
/// entire connection's lifetime) before starting the next one.
pub(super) async fn run_with_reconnect(
    app_id: String,
    app_secret: String,
    base_url: String,
    sink: Arc<dyn EventSink>,
) -> PlatformResult<()> {
    let mut attempt: u32 = 0;
    loop {
        let result = run_once(&app_id, &app_secret, &base_url, &sink).await;
        if result.connected {
            attempt = 0;
        }
        match result.ended {
            ConnectionEnded::Fatal(reason) => {
                tracing::error!("connect: feishu WS stopped (fatal): {reason}");
                return Err(PlatformError::other(format!("feishu WS fatal: {reason}")));
            }
            ConnectionEnded::Retry => {
                let config = result.client_config.unwrap_or_default();
                let delay =
                    reconnect_delay(attempt, config.reconnect_nonce, config.reconnect_interval);
                tracing::warn!("connect: feishu WS reconnecting in {delay:?} (attempt {attempt})");
                tokio::time::sleep(delay).await;
                attempt = attempt.saturating_add(1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_body_uses_pascal_case_keys() {
        let body = build_bootstrap_body("cli_x", "secret_y");
        assert_eq!(body["AppID"], serde_json::json!("cli_x"));
        assert_eq!(body["AppSecret"], serde_json::json!("secret_y"));
        assert_eq!(body["ClientAssertion"], serde_json::json!(""));
    }

    #[test]
    fn bootstrap_response_code_zero_is_ready_with_defaults_when_client_config_absent() {
        let body = serde_json::json!({
            "code": 0,
            "msg": "success",
            "data": { "URL": "wss://example/?service_id=42" }
        })
        .to_string();
        let outcome = parse_bootstrap_response(&body).expect("parses");
        match outcome {
            BootstrapOutcome::Ready(ready) => {
                assert_eq!(ready.url, "wss://example/?service_id=42");
                assert_eq!(ready.client_config, ClientConfig::default());
            }
            other => panic!("expected Ready, got {other:?}"),
        }
    }

    #[test]
    fn bootstrap_response_honors_explicit_client_config() {
        let body = serde_json::json!({
            "code": 0,
            "data": {
                "URL": "wss://example/?service_id=7",
                "ClientConfig": {
                    "PingInterval": 60,
                    "ReconnectInterval": 45,
                    "ReconnectNonce": 10,
                    "ReconnectCount": -1
                }
            }
        })
        .to_string();
        let outcome = parse_bootstrap_response(&body).expect("parses");
        match outcome {
            BootstrapOutcome::Ready(ready) => {
                assert_eq!(ready.client_config.ping_interval, Duration::from_secs(60));
                assert_eq!(
                    ready.client_config.reconnect_interval,
                    Duration::from_secs(45)
                );
                assert_eq!(ready.client_config.reconnect_nonce, Duration::from_secs(10));
            }
            other => panic!("expected Ready, got {other:?}"),
        }
    }

    #[test]
    fn bootstrap_response_codes_1_and_1000040343_are_retryable() {
        for code in [1, 1_000_040_343] {
            let body = serde_json::json!({ "code": code, "msg": "transient" }).to_string();
            let outcome = parse_bootstrap_response(&body).expect("parses");
            assert!(
                matches!(outcome, BootstrapOutcome::Retryable),
                "code {code} should be retryable"
            );
        }
    }

    #[test]
    fn bootstrap_response_other_codes_are_fatal() {
        let body = serde_json::json!({ "code": 10003, "msg": "invalid app" }).to_string();
        let outcome = parse_bootstrap_response(&body).expect("parses");
        assert!(matches!(outcome, BootstrapOutcome::Fatal(_)));
    }

    #[test]
    fn parse_service_id_extracts_the_query_param() {
        assert_eq!(
            parse_service_id("wss://open.feishu.cn/ws?device_id=1&service_id=99&other=x"),
            Some(99)
        );
        assert_eq!(
            parse_service_id("wss://open.feishu.cn/ws?device_id=1"),
            None
        );
        assert_eq!(parse_service_id("not a url"), None);
    }

    #[test]
    fn ping_frame_has_method_zero_and_type_ping_header() {
        let frame = build_ping_frame(5, 42);
        assert_eq!(frame.method, 0);
        assert_eq!(frame.service, 42);
        assert_eq!(frame.header("type"), Some("ping"));
    }

    #[test]
    fn ack_payload_encodes_callback_response_as_base64_data() {
        let response = serde_json::json!({ "toast": { "type": "info", "content": "done" } });
        let payload = build_ack_payload(Some(&response));
        let value: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(value["code"], serde_json::json!(200));
        assert!(value["headers"].is_null());
        let data = value["data"].as_str().expect("data is a base64 string");
        use base64::Engine;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(data)
            .unwrap();
        let decoded_json: serde_json::Value = serde_json::from_slice(&decoded).unwrap();
        assert_eq!(decoded_json, response);
    }

    #[test]
    fn ack_payload_with_no_response_has_null_data() {
        let payload = build_ack_payload(None);
        let value: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(value["code"], serde_json::json!(200));
        assert!(value["data"].is_null());
    }

    #[test]
    fn ack_frame_echoes_seq_log_service_method_headers_with_new_payload() {
        let original = Frame {
            seq_id: 9,
            log_id: 9,
            service: 3,
            method: 1,
            headers: vec![
                Header::new("type", "event"),
                Header::new("message_id", "m-1"),
            ],
            payload_encoding: None,
            payload_type: None,
            payload: Some(b"original".to_vec()),
            log_id_new: None,
        };
        let ack = build_ack_frame(&original, b"{\"code\":200}".to_vec());
        assert_eq!(ack.seq_id, original.seq_id);
        assert_eq!(ack.log_id, original.log_id);
        assert_eq!(ack.service, original.service);
        assert_eq!(ack.method, original.method);
        assert_eq!(ack.headers, original.headers);
        assert_eq!(ack.payload, Some(b"{\"code\":200}".to_vec()));
    }

    fn data_frame(
        message_id: &str,
        sum: Option<usize>,
        seq: Option<usize>,
        payload: &[u8],
    ) -> Frame {
        let mut headers = vec![
            Header::new("type", "event"),
            Header::new("message_id", message_id),
        ];
        if let Some(sum) = sum {
            headers.push(Header::new("sum", sum.to_string()));
        }
        if let Some(seq) = seq {
            headers.push(Header::new("seq", seq.to_string()));
        }
        Frame {
            seq_id: 1,
            log_id: 1,
            service: 1,
            method: 1,
            headers,
            payload_encoding: None,
            payload_type: None,
            payload: Some(payload.to_vec()),
            log_id_new: None,
        }
    }

    #[test]
    fn reassembler_passes_through_single_part_frames_immediately() {
        let mut reassembler = Reassembler::new(REASSEMBLY_TTL);
        let now = Instant::now();
        let frame = data_frame("m-1", None, None, b"hello");
        assert_eq!(reassembler.feed(&frame, now), Some(b"hello".to_vec()));
    }

    #[test]
    fn reassembler_reassembles_multi_part_frames_in_order() {
        let mut reassembler = Reassembler::new(REASSEMBLY_TTL);
        let now = Instant::now();
        let part0 = data_frame("m-2", Some(2), Some(0), b"hel");
        let part1 = data_frame("m-2", Some(2), Some(1), b"lo");

        assert_eq!(
            reassembler.feed(&part0, now),
            None,
            "incomplete group yields nothing yet"
        );
        let full = reassembler.feed(&part1, now).expect("group completes");
        assert_eq!(full, b"hello".to_vec());
    }

    #[test]
    fn reassembler_reorders_out_of_order_parts() {
        let mut reassembler = Reassembler::new(REASSEMBLY_TTL);
        let now = Instant::now();
        let part1 = data_frame("m-3", Some(2), Some(1), b"lo");
        let part0 = data_frame("m-3", Some(2), Some(0), b"hel");

        assert_eq!(reassembler.feed(&part1, now), None);
        let full = reassembler.feed(&part0, now).expect("group completes");
        assert_eq!(full, b"hello".to_vec());
    }

    #[test]
    fn reassembler_drops_groups_older_than_the_ttl() {
        let mut reassembler = Reassembler::new(Duration::from_secs(5));
        let start = Instant::now();
        let part0 = data_frame("m-4", Some(2), Some(0), b"hel");
        assert_eq!(reassembler.feed(&part0, start), None);

        // Feed an unrelated frame long after the TTL to trigger pruning, then
        // the second part of the ORIGINAL group must not complete it.
        let later = start + Duration::from_secs(10);
        let part1 = data_frame("m-4", Some(2), Some(1), b"lo");
        // The stale part was pruned, so this now looks like the FIRST part of
        // a fresh group — still incomplete.
        assert_eq!(reassembler.feed(&part1, later), None);
    }

    #[test]
    fn reconnect_delay_first_attempt_is_bounded_jitter() {
        let nonce = Duration::from_secs(30);
        let interval = Duration::from_secs(120);
        for _ in 0..20 {
            let delay = reconnect_delay(0, nonce, interval);
            assert!(
                delay <= nonce,
                "jitter {delay:?} must be <= nonce {nonce:?}"
            );
        }
    }

    #[test]
    fn reconnect_delay_subsequent_attempts_are_the_fixed_interval() {
        let nonce = Duration::from_secs(30);
        let interval = Duration::from_secs(120);
        assert_eq!(reconnect_delay(1, nonce, interval), interval);
        assert_eq!(reconnect_delay(5, nonce, interval), interval);
    }

    #[test]
    fn fatal_handshake_reason_detects_403_status_header() {
        let mut headers = tokio_tungstenite::tungstenite::http::HeaderMap::new();
        headers.insert("Handshake-Status", "403".parse().unwrap());
        assert!(fatal_handshake_reason(&headers).is_some());
    }

    #[test]
    fn fatal_handshake_reason_detects_too_many_connections_autherrcode() {
        let mut headers = tokio_tungstenite::tungstenite::http::HeaderMap::new();
        headers.insert("Handshake-Autherrcode", "1000040350".parse().unwrap());
        assert!(fatal_handshake_reason(&headers).is_some());
    }

    #[test]
    fn fatal_handshake_reason_is_none_for_ordinary_headers() {
        let mut headers = tokio_tungstenite::tungstenite::http::HeaderMap::new();
        headers.insert("Handshake-Status", "200".parse().unwrap());
        assert!(fatal_handshake_reason(&headers).is_none());
    }
}
