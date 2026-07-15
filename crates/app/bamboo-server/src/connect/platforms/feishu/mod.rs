//! Feishu/Lark event long-connection platform adapter (epic #447 phase 3).
//!
//! No public IP / webhook: inbound events arrive over a WS long-connection
//! (`ws.rs`); outbound sends and the `tenant_access_token` cache go over
//! plain REST (`api.rs`); the wire frame shape is `pbbp2.rs`. See
//! `docs/feishu-adapter-plan.md` §2c for the full protocol writeup this
//! module implements.
//!
//! Module split:
//! - `pbbp2.rs` — vendored `Frame`/`Header` proto2 wire types.
//! - `ws.rs` — the long-connection client (bootstrap/ping/ack/reassembly/
//!   reconnect), ignorant of Feishu's EVENT semantics (that's this file's
//!   job, via the [`ws::EventSink`] trait).
//! - `api.rs` — REST: token cache, send/update message, bot self-info, the
//!   per-chat rate limiter.
//! - `mod.rs` (this file) — [`FeishuPlatform`]: the `Platform` impl, inbound
//!   event → `InboundMessage`/`CallbackQuery` mapping, outbound card
//!   building, and the card-callback pending-ack map.

mod api;
mod pbbp2;
mod ws;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex};

use super::super::platform::{
    Button, CallbackQuery, Capabilities, Inbound, InboundMessage, MessageRef, OutboundMessage,
    Platform, PlatformError, PlatformResult, ReplyCtx,
};
use super::super::render::{chunk_message, MAX_MESSAGE_CHARS};

/// Default outgoing rate limit: 1 message/second per chat (same
/// conservative default as `telegram.rs`; Feishu's actual documented limits
/// are more generous — 5 QPS/user in a p2p chat — but 1/s is a safe,
/// simple default and matches the existing adapter's precedent).
const DEFAULT_RATE_LIMIT_INTERVAL: Duration = Duration::from_secs(1);
/// How long a `card.action.trigger`'s pending-ack waits for the bridge to
/// call `answer_callback` before auto-acking with a bare `{"code":200}`
/// (empty `data`). Kept comfortably under `ws.rs::ACK_HARD_DEADLINE`
/// (2.9s) so the WS transport always has headroom to get the frame back.
const ACK_SOFT_DEADLINE: Duration = Duration::from_millis(2500);
/// The fixed `element_id` on every card's markdown body element (so
/// `edit()`'s PATCH always targets the same element — not load-bearing for
/// Feishu's API, which replaces the whole `content` string, but documents
/// the stable shape).
const MAIN_TEXT_ELEMENT_ID: &str = "main_text";

pub struct FeishuPlatform {
    app_id: String,
    app_secret: String,
    base_url: String,
    tokens: api::TokenCache,
    rate_limiter: api::RateLimiter,
    /// This app's own `open_id`, fetched once in `start()` via
    /// `GET /open-apis/bot/v3/info` — used for @mention detection in group
    /// chats. `None` until fetched (or forever, if the fetch failed — group
    /// messages are then treated as unmentioned/dropped, per plan).
    bot_open_id: Arc<AsyncMutex<Option<String>>>,
    /// `card.action.trigger` events parked here (keyed by `event_id`,
    /// Feishu's `callback_query_id`) while `start()`'s WS event handler
    /// waits for the bridge to call `answer_callback`.
    pending_acks: Arc<AsyncMutex<HashMap<String, oneshot::Sender<serde_json::Value>>>>,
    /// Test seam: skip the real WS client entirely (`start()` returns
    /// `Ok(())` immediately) so REST-only tests (reply/edit/rate-limit)
    /// never touch the network for the long-connection.
    ws_disabled: bool,
}

impl FeishuPlatform {
    /// Production constructor. `base_url` is the ALREADY-RESOLVED REST/WS
    /// bootstrap base (e.g. `https://open.feishu.cn`) — the
    /// `feishu`/`lark`/custom-URL `domain` config field is resolved by the
    /// server's registration arm (`connect/mod.rs`, owned by another agent
    /// for this epic), not here; this constructor only ever sees a plain
    /// string.
    pub fn new(app_id: String, app_secret: String, base_url: String) -> Self {
        Self::with_options(
            app_id,
            app_secret,
            base_url,
            DEFAULT_RATE_LIMIT_INTERVAL,
            false,
        )
    }

    /// Test/advanced constructor: override the rate-limit interval (kept
    /// tiny in tests) and optionally disable the WS client entirely
    /// (`ws_disabled`) so `start()` is a no-op — REST-only tests construct
    /// the platform this way and never call `start()`'s WS half.
    pub fn with_options(
        app_id: String,
        app_secret: String,
        base_url: String,
        rate_limit_interval: Duration,
        ws_disabled: bool,
    ) -> Self {
        Self {
            app_id,
            app_secret,
            base_url,
            tokens: api::TokenCache::new(),
            rate_limiter: api::RateLimiter::new(rate_limit_interval),
            bot_open_id: Arc::new(AsyncMutex::new(None)),
            pending_acks: Arc::new(AsyncMutex::new(HashMap::new())),
            ws_disabled,
        }
    }

    fn extract_chat_id(ctx: &serde_json::Value) -> PlatformResult<String> {
        ctx.get("chat_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| PlatformError::other("reply_ctx is missing chat_id"))
    }

    fn extract_message_id(msg_ref: &serde_json::Value) -> PlatformResult<String> {
        msg_ref
            .get("message_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| PlatformError::other("message_ref is missing message_id"))
    }
}

#[async_trait::async_trait]
impl Platform for FeishuPlatform {
    fn name(&self) -> &str {
        "feishu"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            buttons: true,
            edit_message: true,
            images: false,
            files: false,
        }
    }

    async fn start(&self, inbound: mpsc::Sender<Inbound>) -> PlatformResult<()> {
        if self.ws_disabled {
            return Ok(());
        }

        match api::fetch_bot_open_id(&self.tokens, &self.base_url, &self.app_id, &self.app_secret)
            .await
        {
            Ok(open_id) => {
                *self.bot_open_id.lock().await = Some(open_id);
            }
            Err(error) => {
                tracing::warn!(
                    "connect: feishu bot/v3/info failed ({error}); group messages will be \
                     treated as unmentioned (dropped) until this succeeds"
                );
            }
        }

        let sink: Arc<dyn ws::EventSink> = Arc::new(FeishuEventSink {
            bot_open_id: self.bot_open_id.clone(),
            pending_acks: self.pending_acks.clone(),
            inbound,
            ack_soft_deadline: ACK_SOFT_DEADLINE,
        });

        ws::run_with_reconnect(
            self.app_id.clone(),
            self.app_secret.clone(),
            self.base_url.clone(),
            sink,
        )
        .await
    }

    async fn reply(&self, ctx: &ReplyCtx, msg: OutboundMessage) -> PlatformResult<MessageRef> {
        let chat_id = Self::extract_chat_id(&ctx.0)?;
        let chunks = chunk_message(&msg.text, MAX_MESSAGE_CHARS);
        let chunk_count = chunks.len();
        let mut last_message_id = String::new();

        for (index, chunk) in chunks.into_iter().enumerate() {
            self.rate_limiter.wait(&format!("chat:{chat_id}")).await;

            // ALWAYS an interactive card (never plain "text"): render.rs
            // calls `reply()` for the initial streaming status message too,
            // and `edit()` can only PATCH a card — never a text message (the
            // text PUT has a 20-edit cap). Sending a uniform card shape for
            // every reply means this adapter never has to guess which call
            // site it's serving. See `docs/feishu-adapter-plan.md` §2c
            // outbound decision 4.
            let buttons = if index + 1 == chunk_count {
                msg.buttons.as_deref()
            } else {
                None
            };
            let card = build_card(&chunk, buttons);
            let content = card.to_string();

            last_message_id = api::send_message(
                &self.tokens,
                &self.base_url,
                &self.app_id,
                &self.app_secret,
                &chat_id,
                "interactive",
                &content,
            )
            .await?;
        }

        Ok(MessageRef(serde_json::json!({
            "chat_id": chat_id,
            "message_id": last_message_id,
        })))
    }

    async fn edit(&self, msg_ref: &MessageRef, new: OutboundMessage) -> PlatformResult<()> {
        let message_id = Self::extract_message_id(&msg_ref.0)?;
        self.rate_limiter.wait(&format!("msg:{message_id}")).await;

        let card = build_card(&new.text, new.buttons.as_deref());
        api::update_card(
            &self.tokens,
            &self.base_url,
            &self.app_id,
            &self.app_secret,
            &message_id,
            &card.to_string(),
        )
        .await
    }

    async fn answer_callback(
        &self,
        callback_query_id: &str,
        text: Option<&str>,
    ) -> PlatformResult<()> {
        // NOT rate-limited: this resolves a parked WS frame ack, not a REST
        // call — see `ws.rs::EventSink`/`FeishuEventSink::handle_card_action_event`.
        let sender = self.pending_acks.lock().await.remove(callback_query_id);
        if let Some(sender) = sender {
            let value = match text {
                Some(text) => serde_json::json!({ "toast": { "type": "info", "content": text } }),
                None => serde_json::json!({}),
            };
            // A send error just means the WS event handler already gave up
            // waiting (past its own soft deadline) — nothing left to do.
            let _ = sender.send(value);
        }
        Ok(())
    }

    async fn stop(&self) -> PlatformResult<()> {
        // Best-effort no-op, matching `telegram.rs`'s precedent: real
        // cancellation happens via `ConnectManager`'s `Drop`, which aborts
        // the JoinHandle running `start()` — that unwinds the whole WS
        // connection (and its ping/ack machinery) at once.
        Ok(())
    }
}

// ---------------------------------------------------------------------
// Outbound: card building
// ---------------------------------------------------------------------

/// Escapes characters Feishu's card markdown element treats specially, so
/// arbitrary agent output (which render.rs treats as PLAIN TEXT — see
/// `platform.rs`'s `OutboundMessage` doc, there is no markdown flag) never
/// gets reinterpreted as markdown syntax or a `<at>`/`<a>` tag. This is a
/// deliberately simple backslash-escape of the documented Feishu markdown
/// special characters — NOT a general HTML/markdown sanitizer, and not
/// exhaustive of every card-markdown edge case, but sufficient for MVP text
/// (tool output, assistant replies) which is prose, not hand-crafted markup.
fn escape_feishu_markdown(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for ch in text.chars() {
        if matches!(
            ch,
            '\\' | '`'
                | '*'
                | '_'
                | '~'
                | '#'
                | '+'
                | '-'
                | '.'
                | '!'
                | '['
                | ']'
                | '('
                | ')'
                | '<'
                | '>'
                | '|'
        ) {
            escaped.push('\\');
        }
        escaped.push(ch);
    }
    escaped
}

/// Builds a schema-2.0 interactive card: `config.update_multi:true`, a
/// markdown body element (fixed `element_id:"main_text"`, escaped text), and
/// — when present — one `"action"` element per button row, per
/// `docs/feishu-adapter-plan.md` §2c outbound decision 2. The exact button
/// element wrapper (`tag:"action"` with a nested `"actions"` array) is this
/// adapter's own choice: the plan pins ONLY the `behaviors`/`value.cb`
/// shape, not the surrounding container, so this is the one place protocol
/// shape was inferred rather than verified — flagged in the task report.
fn build_card(text: &str, buttons: Option<&[Vec<Button>]>) -> serde_json::Value {
    let mut elements = vec![serde_json::json!({
        "tag": "markdown",
        "element_id": MAIN_TEXT_ELEMENT_ID,
        "content": escape_feishu_markdown(text),
    })];

    if let Some(rows) = buttons {
        for row in rows {
            let actions: Vec<serde_json::Value> = row
                .iter()
                .map(|button| {
                    serde_json::json!({
                        "tag": "button",
                        "text": { "tag": "plain_text", "content": button.label },
                        "type": "default",
                        "behaviors": [
                            { "type": "callback", "value": { "cb": button.callback_data } }
                        ],
                    })
                })
                .collect();
            elements.push(serde_json::json!({ "tag": "action", "actions": actions }));
        }
    }

    serde_json::json!({
        "schema": "2.0",
        "config": { "update_multi": true },
        // Schema 2.0 nests `elements` under `body` — a top-level `elements`
        // key is the v1 location and the real API rejects the card with
        // `200621 parse card json err: unknown property "elements"` (caught
        // in Magpie's 2026-07-15 real-device e2e; same builder, same bug).
        "body": { "elements": elements },
    })
}

// ---------------------------------------------------------------------
// Inbound: event envelope + mapping
// ---------------------------------------------------------------------

#[derive(Debug, serde::Deserialize)]
struct EventEnvelope {
    #[serde(default)]
    #[allow(dead_code)]
    // documents the wire shape; not branched on (only "2.0" is ever sent on this transport)
    schema: Option<String>,
    header: EventHeader,
    event: serde_json::Value,
}

#[derive(Debug, serde::Deserialize)]
struct EventHeader {
    event_id: String,
    event_type: String,
}

#[derive(Debug, serde::Deserialize)]
struct MessageReceiveEvent {
    sender: EventSender,
    message: EventMessage,
}

#[derive(Debug, serde::Deserialize)]
struct EventSender {
    sender_id: EventSenderId,
    #[serde(default)]
    sender_type: String,
}

#[derive(Debug, serde::Deserialize)]
struct EventSenderId {
    open_id: String,
}

#[derive(Debug, serde::Deserialize)]
struct EventMessage {
    message_id: String,
    chat_id: String,
    #[serde(default)]
    chat_type: String,
    message_type: String,
    content: String,
    create_time: String,
    #[serde(default)]
    mentions: Vec<EventMention>,
}

#[derive(Debug, serde::Deserialize)]
struct EventMention {
    key: String,
    id: EventMentionId,
    #[serde(default)]
    name: String,
}

#[derive(Debug, serde::Deserialize)]
struct EventMentionId {
    open_id: String,
}

#[derive(Debug, serde::Deserialize)]
struct CardActionEvent {
    operator: CardOperator,
    context: CardContext,
    action: CardAction,
}

#[derive(Debug, serde::Deserialize)]
struct CardOperator {
    open_id: String,
}

#[derive(Debug, serde::Deserialize)]
struct CardContext {
    open_chat_id: String,
}

#[derive(Debug, serde::Deserialize)]
struct CardAction {
    value: serde_json::Value,
}

/// Replaces `@_user_N` placeholders in `text` per `mentions[].key`: the
/// bot's OWN mention (`id.open_id == bot_open_id`) is dropped entirely, any
/// other mention becomes `@{name}`. Leftover whitespace from a dropped
/// placeholder is normalized (runs of whitespace collapsed to one space,
/// trimmed) — a deliberate simplification (documented deviation: this does
/// not preserve exact original whitespace/newline structure around a
/// stripped mention, since MVP text is prose, not formatted markup).
fn strip_mentions(text: &str, mentions: &[EventMention], bot_open_id: Option<&str>) -> String {
    let mut result = text.to_string();
    for mention in mentions {
        let replacement = if Some(mention.id.open_id.as_str()) == bot_open_id {
            String::new()
        } else {
            format!("@{}", mention.name)
        };
        result = result.replace(&mention.key, &replacement);
    }
    result.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Group-chat gating: p2p always passes; a group message passes only when
/// it @-mentions this bot (found in `mentions[].id.open_id`) or is an
/// `@所有人`/`@_all` broadcast (`mentions` is empty for those — a literal
/// substring check on the raw text is the documented way to detect it).
/// KNOWN LIMITATION: the wire format makes a real `@所有人` mention
/// indistinguishable from a member literally typing the characters `@_all`
/// (both arrive as `{"text":"…@_all…"}` with zero mention entries), so the
/// latter also passes this gate. Accepted: it only widens "processed vs
/// ignored", never authorization — `allow_from` still gates the sender's
/// identity downstream in the bridge.
/// `bot_open_id: None` (the startup `bot/v3/info` fetch failed) means every
/// group message is treated as unmentioned and dropped.
fn passes_group_gate(
    chat_type: &str,
    raw_text: &str,
    mentions: &[EventMention],
    bot_open_id: Option<&str>,
) -> bool {
    if chat_type != "group" {
        return true;
    }
    if raw_text.contains("@_all") {
        return true;
    }
    match bot_open_id {
        Some(id) => mentions.iter().any(|m| m.id.open_id == id),
        None => false,
    }
}

/// Maps one already-parsed `im.message.receive_v1` event body to an
/// [`InboundMessage`], or `None` for anything this MVP doesn't forward:
/// a bot sender, a non-text message, or a group message that fails the
/// @mention gate. Pure/sync so it's directly unit-testable.
fn map_message_event(
    event: &MessageReceiveEvent,
    bot_open_id: Option<&str>,
) -> Option<InboundMessage> {
    if event.sender.sender_type == "bot" {
        return None;
    }
    if event.message.message_type != "text" {
        return None;
    }

    let raw_text: String = serde_json::from_str::<serde_json::Value>(&event.message.content)
        .ok()
        .and_then(|v| {
            v.get("text")
                .and_then(|t| t.as_str())
                .map(|s| s.to_string())
        })
        .unwrap_or_default();

    if !passes_group_gate(
        &event.message.chat_type,
        &raw_text,
        &event.message.mentions,
        bot_open_id,
    ) {
        return None;
    }

    let text = strip_mentions(&raw_text, &event.message.mentions, bot_open_id);

    let sent_at = event
        .message
        .create_time
        .parse::<i64>()
        .ok()
        .and_then(chrono::DateTime::<chrono::Utc>::from_timestamp_millis)
        .unwrap_or_else(chrono::Utc::now);

    Some(InboundMessage {
        platform: "feishu".to_string(),
        chat_id: event.message.chat_id.clone(),
        user_id: event.sender.sender_id.open_id.clone(),
        message_id: event.message.message_id.clone(),
        sent_at,
        text,
        reply_ctx: ReplyCtx(serde_json::json!({
            "chat_id": event.message.chat_id,
            "message_id": event.message.message_id,
        })),
    })
}

/// Maps one already-parsed `card.action.trigger` event body to a
/// [`CallbackQuery`]. Returns `None` when `action.value` doesn't carry our
/// `"cb"` key (not a button this adapter produced — nothing to route).
fn map_card_action_event(event_id: &str, event: &CardActionEvent) -> Option<CallbackQuery> {
    let data = event
        .action
        .value
        .get("cb")
        .and_then(|v| v.as_str())?
        .to_string();
    Some(CallbackQuery {
        platform: "feishu".to_string(),
        chat_id: event.context.open_chat_id.clone(),
        user_id: event.operator.open_id.clone(),
        callback_query_id: event_id.to_string(),
        data,
        reply_ctx: ReplyCtx(serde_json::json!({ "chat_id": event.context.open_chat_id })),
    })
}

/// Bridges `ws.rs`'s transport-only [`ws::EventSink`] to Feishu event
/// semantics. Holds only OWNED/`Arc`-cloned state (never `&FeishuPlatform`)
/// so it satisfies `EventSink`'s `'static` bound — `start()` constructs one
/// per WS connection lifetime from `self`'s `Arc`-wrapped fields.
struct FeishuEventSink {
    bot_open_id: Arc<AsyncMutex<Option<String>>>,
    pending_acks: Arc<AsyncMutex<HashMap<String, oneshot::Sender<serde_json::Value>>>>,
    inbound: mpsc::Sender<Inbound>,
    ack_soft_deadline: Duration,
}

impl FeishuEventSink {
    async fn handle_message_event(&self, event: serde_json::Value) {
        let parsed: MessageReceiveEvent = match serde_json::from_value(event) {
            Ok(parsed) => parsed,
            Err(error) => {
                tracing::warn!("connect: feishu im.message.receive_v1 parse failed: {error}");
                return;
            }
        };
        let bot_open_id = self.bot_open_id.lock().await.clone();
        let Some(message) = map_message_event(&parsed, bot_open_id.as_deref()) else {
            return;
        };
        // Per task spec: a send failure here means the bridge/manager is
        // shutting down (the receiving end of `dispatch_loop`'s channel was
        // dropped) — nothing more to do for this event; real cleanup of the
        // WS connection itself happens via `ConnectManager`'s `Drop`
        // (JoinHandle abort), not by this handler propagating an error.
        let _ = self.inbound.send(Inbound::Message(message)).await;
    }

    /// Returns the ack payload to echo back over the WS frame: the
    /// bridge-supplied toast (or `{}`) if `answer_callback` resolved the
    /// pending-ack before `ack_soft_deadline`, otherwise `None` (a bare
    /// `{"code":200}` ack with null `data`).
    async fn handle_card_action_event(
        &self,
        event_id: String,
        event: serde_json::Value,
    ) -> Option<serde_json::Value> {
        let parsed: CardActionEvent = match serde_json::from_value(event) {
            Ok(parsed) => parsed,
            Err(error) => {
                tracing::warn!("connect: feishu card.action.trigger parse failed: {error}");
                return None;
            }
        };
        let Some(callback) = map_card_action_event(&event_id, &parsed) else {
            tracing::debug!("connect: feishu card action missing our 'cb' value; ignoring");
            return None;
        };

        let (tx, rx) = oneshot::channel();
        self.pending_acks.lock().await.insert(event_id.clone(), tx);

        if self
            .inbound
            .send(Inbound::Callback(callback))
            .await
            .is_err()
        {
            self.pending_acks.lock().await.remove(&event_id);
            return None;
        }

        match tokio::time::timeout(self.ack_soft_deadline, rx).await {
            Ok(Ok(value)) => Some(value),
            Ok(Err(_)) => None, // sender dropped without resolving
            Err(_elapsed) => {
                self.pending_acks.lock().await.remove(&event_id);
                None
            }
        }
    }
}

#[async_trait::async_trait]
impl ws::EventSink for FeishuEventSink {
    async fn handle_event(&self, payload: Vec<u8>) -> Option<serde_json::Value> {
        let envelope: EventEnvelope = match serde_json::from_slice(&payload) {
            Ok(envelope) => envelope,
            Err(error) => {
                tracing::warn!("connect: feishu event envelope parse failed: {error}");
                return None;
            }
        };
        match envelope.header.event_type.as_str() {
            "im.message.receive_v1" => {
                self.handle_message_event(envelope.event).await;
                None
            }
            "card.action.trigger" => {
                self.handle_card_action_event(envelope.header.event_id, envelope.event)
                    .await
            }
            other => {
                tracing::debug!("connect: feishu event type '{other}' not handled (MVP)");
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn platform_with_stub(base_url: String) -> FeishuPlatform {
        FeishuPlatform::with_options(
            "cli_test_app".to_string(),
            "test-app-secret".to_string(),
            base_url,
            Duration::from_millis(50),
            true,
        )
    }

    async fn wait_for_requests(
        server: &wiremock::MockServer,
        expected: usize,
    ) -> Vec<wiremock::Request> {
        for _ in 0..200 {
            if let Some(requests) = server.received_requests().await {
                if requests.len() >= expected {
                    return requests;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        server.received_requests().await.unwrap_or_default()
    }

    fn mount_token(server: &wiremock::MockServer) -> impl std::future::Future<Output = ()> + '_ {
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path(
                "/open-apis/auth/v3/tenant_access_token/internal",
            ))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "code": 0,
                    "msg": "ok",
                    "tenant_access_token": "tok-abc",
                    "expire": 7200
                })),
            )
            .mount(server)
    }

    #[tokio::test]
    async fn capabilities_advertise_buttons_and_edit_message_only() {
        let platform = platform_with_stub("http://localhost:0".to_string());
        let caps = platform.capabilities();
        assert!(caps.buttons);
        assert!(caps.edit_message);
        assert!(!caps.images);
        assert!(!caps.files);
    }

    #[tokio::test]
    async fn reply_sends_an_interactive_card_with_escaped_markdown_content() {
        let server = wiremock::MockServer::start().await;
        mount_token(&server).await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/open-apis/im/v1/messages"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "code": 0,
                    "data": { "message_id": "om_1" }
                })),
            )
            .mount(&server)
            .await;

        let platform = platform_with_stub(server.uri());
        let ctx = ReplyCtx(serde_json::json!({ "chat_id": "oc_1" }));
        let msg_ref = platform
            .reply(&ctx, OutboundMessage::text("hello *world*"))
            .await
            .expect("reply succeeds");
        assert_eq!(
            msg_ref.0.get("message_id").and_then(|v| v.as_str()),
            Some("om_1")
        );

        let requests = wait_for_requests(&server, 1).await;
        let send_request = requests
            .iter()
            .find(|r| r.url.path() == "/open-apis/im/v1/messages")
            .expect("send request present");
        let body: serde_json::Value = serde_json::from_slice(&send_request.body).unwrap();
        assert_eq!(body["msg_type"], serde_json::json!("interactive"));
        assert_eq!(body["receive_id"], serde_json::json!("oc_1"));
        let content: serde_json::Value =
            serde_json::from_str(body["content"].as_str().expect("content is a JSON string"))
                .unwrap();
        assert_eq!(content["schema"], serde_json::json!("2.0"));
        assert_eq!(content["config"]["update_multi"], serde_json::json!(true));
        let markdown_content = content["body"]["elements"][0]["content"].as_str().unwrap();
        assert!(
            markdown_content.contains("\\*world\\*"),
            "got: {markdown_content}"
        );
    }

    #[tokio::test]
    async fn reply_with_buttons_puts_callback_value_under_cb_key() {
        let server = wiremock::MockServer::start().await;
        mount_token(&server).await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/open-apis/im/v1/messages"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "code": 0,
                    "data": { "message_id": "om_2" }
                })),
            )
            .mount(&server)
            .await;

        let platform = platform_with_stub(server.uri());
        let ctx = ReplyCtx(serde_json::json!({ "chat_id": "oc_1" }));
        let outbound = OutboundMessage::text("Approve?")
            .with_buttons(vec![vec![Button::new("Approve", "n1:0")]]);
        platform
            .reply(&ctx, outbound)
            .await
            .expect("reply succeeds");

        let requests = wait_for_requests(&server, 2).await;
        let send_request = requests
            .iter()
            .find(|r| r.url.path() == "/open-apis/im/v1/messages")
            .expect("send request present");
        let body: serde_json::Value = serde_json::from_slice(&send_request.body).unwrap();
        let content: serde_json::Value =
            serde_json::from_str(body["content"].as_str().unwrap()).unwrap();
        let action = &content["body"]["elements"][1];
        assert_eq!(action["tag"], serde_json::json!("action"));
        assert_eq!(
            action["actions"][0]["behaviors"][0]["value"]["cb"],
            serde_json::json!("n1:0")
        );
    }

    #[tokio::test]
    async fn reply_chunks_long_text_into_multiple_card_sends() {
        let server = wiremock::MockServer::start().await;
        mount_token(&server).await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/open-apis/im/v1/messages"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "code": 0,
                    "data": { "message_id": "om_3" }
                })),
            )
            .mount(&server)
            .await;

        let platform = platform_with_stub(server.uri());
        let ctx = ReplyCtx(serde_json::json!({ "chat_id": "oc_1" }));
        let long_text = "a".repeat(9000); // -> 3 chunks at MAX_MESSAGE_CHARS=4096

        platform
            .reply(&ctx, OutboundMessage::text(long_text))
            .await
            .expect("reply succeeds");

        let requests = wait_for_requests(&server, 3).await;
        let send_requests: Vec<_> = requests
            .iter()
            .filter(|r| r.url.path() == "/open-apis/im/v1/messages")
            .collect();
        assert_eq!(send_requests.len(), 3, "expected exactly 3 card sends");
    }

    #[tokio::test]
    async fn edit_patches_the_card_at_the_message_id() {
        let server = wiremock::MockServer::start().await;
        mount_token(&server).await;
        wiremock::Mock::given(wiremock::matchers::method("PATCH"))
            .and(wiremock::matchers::path("/open-apis/im/v1/messages/om_9"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "code": 0,
                    "data": {}
                })),
            )
            .mount(&server)
            .await;

        let platform = platform_with_stub(server.uri());
        let msg_ref = MessageRef(serde_json::json!({ "chat_id": "oc_1", "message_id": "om_9" }));
        platform
            .edit(&msg_ref, OutboundMessage::text("updated"))
            .await
            .expect("edit succeeds");

        let requests = wait_for_requests(&server, 1).await;
        let request = requests
            .iter()
            .find(|r| r.url.path() == "/open-apis/im/v1/messages/om_9")
            .expect("patch request present");
        let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
        let content: serde_json::Value =
            serde_json::from_str(body["content"].as_str().unwrap()).unwrap();
        assert_eq!(
            content["body"]["elements"][0]["content"],
            serde_json::json!("updated")
        );
    }

    #[tokio::test]
    async fn edit_returns_err_on_api_error_instead_of_panicking() {
        let server = wiremock::MockServer::start().await;
        mount_token(&server).await;
        wiremock::Mock::given(wiremock::matchers::method("PATCH"))
            .and(wiremock::matchers::path("/open-apis/im/v1/messages/om_9"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "code": 230001,
                    "msg": "card not found"
                })),
            )
            .mount(&server)
            .await;

        let platform = platform_with_stub(server.uri());
        let msg_ref = MessageRef(serde_json::json!({ "chat_id": "oc_1", "message_id": "om_9" }));
        let result = platform
            .edit(&msg_ref, OutboundMessage::text("updated"))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn token_is_cached_across_two_sends_then_force_refreshed_on_99991663() {
        let server = wiremock::MockServer::start().await;
        // First token call returns tok-1; a SECOND call (post-invalidate)
        // returns tok-2 — wiremock matches requests in mount order and
        // dispatches the first that still has un-exhausted expectations, so
        // an explicit `up_to_n_times` differentiates the two responses.
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path(
                "/open-apis/auth/v3/tenant_access_token/internal",
            ))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "code": 0, "tenant_access_token": "tok-1", "expire": 7200
                })),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path(
                "/open-apis/auth/v3/tenant_access_token/internal",
            ))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "code": 0, "tenant_access_token": "tok-2", "expire": 7200
                })),
            )
            .mount(&server)
            .await;

        // First send: 99991663 (stale token) -> the adapter must invalidate
        // and retry once, succeeding on the retry.
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/open-apis/im/v1/messages"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "code": 99991663, "msg": "invalid access token"
                })),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/open-apis/im/v1/messages"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "code": 0, "data": { "message_id": "om_1" }
                })),
            )
            .mount(&server)
            .await;

        let platform = platform_with_stub(server.uri());
        let ctx = ReplyCtx(serde_json::json!({ "chat_id": "oc_1" }));

        platform
            .reply(&ctx, OutboundMessage::text("first"))
            .await
            .expect("reply succeeds after one token-invalid retry");

        let token_requests = wait_for_requests(&server, 2).await;
        let token_calls = token_requests
            .iter()
            .filter(|r| r.url.path() == "/open-apis/auth/v3/tenant_access_token/internal")
            .count();
        assert_eq!(
            token_calls, 2,
            "expected an initial fetch + one forced refresh"
        );
    }

    #[tokio::test]
    async fn transport_errors_never_leak_the_app_secret() {
        let secret = "SECRET-APP-SECRET-MUST-NOT-LEAK";
        let platform = FeishuPlatform::with_options(
            "cli_test_app".to_string(),
            secret.to_string(),
            "http://127.0.0.1:1".to_string(),
            Duration::from_millis(1),
            true,
        );
        let ctx = ReplyCtx(serde_json::json!({ "chat_id": "oc_1" }));
        let err = platform
            .reply(&ctx, OutboundMessage::text("hi"))
            .await
            .expect_err("port 1 must refuse the connection");
        let text = format!("{err}");
        assert!(
            !text.contains(secret) && !text.contains("SECRET-APP-SECRET"),
            "app_secret leaked into error text: {text}"
        );
    }

    // -- inbound mapping -------------------------------------------------

    fn message_event(
        chat_type: &str,
        text_json: &str,
        sender_type: &str,
        mentions: serde_json::Value,
    ) -> MessageReceiveEvent {
        let value = serde_json::json!({
            "sender": { "sender_id": { "open_id": "ou_sender" }, "sender_type": sender_type },
            "message": {
                "message_id": "om_1",
                "chat_id": "oc_1",
                "chat_type": chat_type,
                "message_type": "text",
                "content": text_json,
                "create_time": "1700000000000",
                "mentions": mentions,
            }
        });
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn map_message_event_maps_a_p2p_text_message() {
        let event = message_event("p2p", "{\"text\":\"hello\"}", "user", serde_json::json!([]));
        let message = map_message_event(&event, None).expect("maps");
        assert_eq!(message.platform, "feishu");
        assert_eq!(message.chat_id, "oc_1");
        assert_eq!(message.user_id, "ou_sender");
        assert_eq!(message.message_id, "om_1");
        assert_eq!(message.text, "hello");
        assert_eq!(
            message.reply_ctx.0,
            serde_json::json!({ "chat_id": "oc_1", "message_id": "om_1" })
        );
    }

    #[test]
    fn map_message_event_drops_bot_senders() {
        let event = message_event("p2p", "{\"text\":\"hi\"}", "bot", serde_json::json!([]));
        assert!(map_message_event(&event, None).is_none());
    }

    #[test]
    fn map_message_event_strips_other_mentions_and_drops_own_bot_mention() {
        let event = message_event(
            "group",
            "{\"text\":\"@_user_1 @_user_2 please review\"}",
            "user",
            serde_json::json!([
                { "key": "@_user_1", "id": { "open_id": "ou_bot" }, "name": "MyBot" },
                { "key": "@_user_2", "id": { "open_id": "ou_alice" }, "name": "Alice" }
            ]),
        );
        let message =
            map_message_event(&event, Some("ou_bot")).expect("group message w/ bot mention passes");
        assert_eq!(message.text, "@Alice please review");
    }

    #[test]
    fn map_message_event_group_without_mention_or_bot_open_id_is_dropped() {
        let event = message_event(
            "group",
            "{\"text\":\"hello\"}",
            "user",
            serde_json::json!([]),
        );
        assert!(map_message_event(&event, Some("ou_bot")).is_none());
        assert!(map_message_event(&event, None).is_none());
    }

    #[test]
    fn map_message_event_group_at_all_passes_even_with_empty_mentions() {
        let event = message_event(
            "group",
            "{\"text\":\"@_all please review\"}",
            "user",
            serde_json::json!([]),
        );
        let message =
            map_message_event(&event, Some("ou_bot")).expect("@_all passes the group gate");
        assert!(message.text.contains("please review"));
    }

    #[test]
    fn map_message_event_drops_non_text_message_types() {
        let value = serde_json::json!({
            "sender": { "sender_id": { "open_id": "ou_sender" }, "sender_type": "user" },
            "message": {
                "message_id": "om_1", "chat_id": "oc_1", "chat_type": "p2p",
                "message_type": "image", "content": "{}", "create_time": "1700000000000",
                "mentions": []
            }
        });
        let event: MessageReceiveEvent = serde_json::from_value(value).unwrap();
        assert!(map_message_event(&event, None).is_none());
    }

    #[test]
    fn map_card_action_event_extracts_cb_value_and_context() {
        let value = serde_json::json!({
            "operator": { "open_id": "ou_op" },
            "context": { "open_chat_id": "oc_5" },
            "action": { "value": { "cb": "nonce123:1" } }
        });
        let event: CardActionEvent = serde_json::from_value(value).unwrap();
        let callback = map_card_action_event("ev_1", &event).expect("maps");
        assert_eq!(callback.platform, "feishu");
        assert_eq!(callback.chat_id, "oc_5");
        assert_eq!(callback.user_id, "ou_op");
        assert_eq!(callback.callback_query_id, "ev_1");
        assert_eq!(callback.data, "nonce123:1");
        assert_eq!(
            callback.reply_ctx.0,
            serde_json::json!({ "chat_id": "oc_5" })
        );
    }

    #[test]
    fn map_card_action_event_returns_none_without_cb_key() {
        let value = serde_json::json!({
            "operator": { "open_id": "ou_op" },
            "context": { "open_chat_id": "oc_5" },
            "action": { "value": { "other": "x" } }
        });
        let event: CardActionEvent = serde_json::from_value(value).unwrap();
        assert!(map_card_action_event("ev_1", &event).is_none());
    }

    // -- pending-ack ------------------------------------------------------

    fn card_action_payload(event_id: &str) -> Vec<u8> {
        serde_json::json!({
            "schema": "2.0",
            "header": { "event_id": event_id, "event_type": "card.action.trigger" },
            "event": {
                "operator": { "open_id": "ou_op" },
                "context": { "open_chat_id": "oc_5" },
                "action": { "value": { "cb": "nonce1:0" } }
            }
        })
        .to_string()
        .into_bytes()
    }

    #[tokio::test]
    async fn card_callback_resolves_pending_ack_with_toast_when_answered() {
        let (tx, mut rx) = mpsc::channel(4);
        let sink = FeishuEventSink {
            bot_open_id: Arc::new(AsyncMutex::new(None)),
            pending_acks: Arc::new(AsyncMutex::new(HashMap::new())),
            inbound: tx,
            ack_soft_deadline: Duration::from_secs(5),
        };
        let pending_acks = sink.pending_acks.clone();

        let handle = tokio::spawn(async move {
            use ws::EventSink;
            sink.handle_event(card_action_payload("ev_1")).await
        });

        // Wait until the CallbackQuery is on the inbound channel (which
        // happens strictly AFTER the pending-ack oneshot is registered).
        let inbound = rx.recv().await.expect("callback delivered");
        match inbound {
            Inbound::Callback(callback) => assert_eq!(callback.callback_query_id, "ev_1"),
            Inbound::Message(_) => panic!("expected a callback"),
        }

        // Resolve it, mirroring `FeishuPlatform::answer_callback`.
        let sender = pending_acks
            .lock()
            .await
            .remove("ev_1")
            .expect("pending ack present");
        sender
            .send(serde_json::json!({ "toast": { "type": "info", "content": "Approved" } }))
            .expect("resolve succeeds");

        let ack_response = handle.await.expect("task completes");
        assert_eq!(
            ack_response,
            Some(serde_json::json!({ "toast": { "type": "info", "content": "Approved" } }))
        );
    }

    #[tokio::test]
    async fn card_callback_auto_acks_after_the_soft_deadline_when_unanswered() {
        let (tx, mut rx) = mpsc::channel(4);
        let sink = FeishuEventSink {
            bot_open_id: Arc::new(AsyncMutex::new(None)),
            pending_acks: Arc::new(AsyncMutex::new(HashMap::new())),
            inbound: tx,
            ack_soft_deadline: Duration::from_millis(20),
        };

        let handle = tokio::spawn(async move {
            use ws::EventSink;
            sink.handle_event(card_action_payload("ev_2")).await
        });

        let _ = rx.recv().await.expect("callback delivered");
        // Never call answer_callback — the soft deadline (20ms) must fire.
        let ack_response = handle.await.expect("task completes");
        assert_eq!(
            ack_response, None,
            "unanswered callback auto-acks with no toast data"
        );
    }

    #[tokio::test]
    async fn im_message_receive_event_reaches_the_inbound_channel() {
        let (tx, mut rx) = mpsc::channel(4);
        let sink = FeishuEventSink {
            bot_open_id: Arc::new(AsyncMutex::new(None)),
            pending_acks: Arc::new(AsyncMutex::new(HashMap::new())),
            inbound: tx,
            ack_soft_deadline: Duration::from_secs(5),
        };
        let payload = serde_json::json!({
            "schema": "2.0",
            "header": { "event_id": "ev_3", "event_type": "im.message.receive_v1" },
            "event": {
                "sender": { "sender_id": { "open_id": "ou_1" }, "sender_type": "user" },
                "message": {
                    "message_id": "om_5", "chat_id": "oc_1", "chat_type": "p2p",
                    "message_type": "text", "content": "{\"text\":\"hi there\"}",
                    "create_time": "1700000000000", "mentions": []
                }
            }
        })
        .to_string()
        .into_bytes();

        use ws::EventSink;
        let ack = sink.handle_event(payload).await;
        assert_eq!(ack, None, "a plain message event acks with no toast data");

        let inbound = rx.recv().await.expect("message delivered");
        match inbound {
            Inbound::Message(message) => assert_eq!(message.text, "hi there"),
            Inbound::Callback(_) => panic!("expected a message"),
        }
    }
}
