//! Telegram long-poll platform adapter (issue #452 phase 1 MVP; buttons +
//! `editMessageText` + `callback_query` added in issue #458 phase 2): no
//! public IP, no webhook, no WS — just `getUpdates` over plain HTTPS.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

use tokio::sync::{mpsc, Mutex as AsyncMutex};

use super::super::platform::{
    Button, CallbackQuery, Capabilities, Inbound, InboundMessage, MessageRef, OutboundMessage,
    Platform, PlatformError, PlatformResult, ReplyCtx,
};
use super::super::render::{chunk_message, MAX_MESSAGE_CHARS};

const DEFAULT_BASE_URL: &str = "https://api.telegram.org";
/// Telegram's own long-poll timeout — the server holds the `getUpdates`
/// connection open for up to this many seconds waiting for a new update.
const LONG_POLL_TIMEOUT_SECS: u64 = 30;
/// Backoff between `getUpdates` retries after a transport/parse failure.
const RETRY_BACKOFF: Duration = Duration::from_secs(5);
/// Default outgoing rate limit: 1 message/second per chat (telegram-safe;
/// Telegram's own documented soft limit is ~1 msg/sec per chat).
const DEFAULT_RATE_LIMIT_INTERVAL: Duration = Duration::from_secs(1);

/// One shared `reqwest::Client`. Reuses the workspace's pinned (native-tls)
/// `reqwest` — never construct a second client/connector for this adapter
/// (mirrors `notify_sinks::ntfy::http_client`).
fn http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

#[derive(Debug, serde::Deserialize)]
struct TelegramResponse<T> {
    ok: bool,
    #[serde(default)]
    result: Option<T>,
    #[serde(default)]
    description: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct TelegramUpdate {
    update_id: i64,
    #[serde(default)]
    message: Option<TelegramMessage>,
    #[serde(default)]
    callback_query: Option<TelegramCallbackQuery>,
}

/// An inline-button press (issue #458). `message` is the message the button
/// was attached to — its `chat` gives us the chat to route the resolution
/// against; a callback with no `message` (can happen for very old/inline
/// messages) is dropped by [`TelegramPlatform::to_inbound_callback`], same
/// treatment as a text update this MVP doesn't handle.
#[derive(Debug, Clone, serde::Deserialize)]
struct TelegramCallbackQuery {
    id: String,
    #[serde(default)]
    from: Option<TelegramUser>,
    #[serde(default)]
    message: Option<TelegramMessage>,
    #[serde(default)]
    data: Option<String>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
struct TelegramMessage {
    #[serde(default)]
    message_id: i64,
    /// Unix timestamp (seconds) — Telegram's own message send time.
    #[serde(default)]
    date: i64,
    #[serde(default)]
    chat: TelegramChat,
    #[serde(default)]
    from: Option<TelegramUser>,
    #[serde(default)]
    text: Option<String>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
struct TelegramChat {
    #[serde(default)]
    id: i64,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct TelegramUser {
    id: i64,
}

/// Per-chat outgoing token bucket: blocks (never drops) until at least
/// `min_interval` has elapsed since the last send to that chat. Reserves the
/// next allowed slot atomically under a short-held lock, then sleeps
/// OUTSIDE the lock — so a chat waiting on its slot never blocks a send to a
/// different chat.
struct RateLimiter {
    next_allowed: AsyncMutex<HashMap<String, tokio::time::Instant>>,
    min_interval: Duration,
}

impl RateLimiter {
    fn new(min_interval: Duration) -> Self {
        Self {
            next_allowed: AsyncMutex::new(HashMap::new()),
            min_interval,
        }
    }

    async fn wait(&self, key: &str) {
        let now = tokio::time::Instant::now();
        let scheduled = {
            let mut guard = self.next_allowed.lock().await;
            let earliest = guard.get(key).copied().unwrap_or(now);
            let scheduled = earliest.max(now);
            guard.insert(key.to_string(), scheduled + self.min_interval);
            // Bound growth (issue #454 follow-up): without this, the map
            // gains one entry per distinct chat id for the life of the
            // process, even after a chat goes permanently idle. Sweep out
            // every entry whose reserved slot has ALREADY elapsed — safe
            // because a swept key's next `wait()` falls back to `now` via
            // `unwrap_or(now)` above, i.e. exactly the value we're
            // discarding, so this never changes scheduling behavior. The
            // entry this call just inserted is always `> now` (it's
            // `scheduled + min_interval` with `scheduled >= now`), so it
            // always survives its own sweep.
            guard.retain(|_, next_allowed| *next_allowed > now);
            scheduled
        };
        if scheduled > now {
            tokio::time::sleep(scheduled - now).await;
        }
    }

    #[cfg(test)]
    async fn tracked_chat_count(&self) -> usize {
        self.next_allowed.lock().await.len()
    }
}

pub struct TelegramPlatform {
    token: String,
    base_url: String,
    offset: AtomicI64,
    rate_limiter: RateLimiter,
}

impl TelegramPlatform {
    /// Production constructor: official Telegram API base URL, 1 msg/s
    /// per-chat rate limit.
    pub fn new(token: String) -> Self {
        Self::with_options(
            token,
            DEFAULT_BASE_URL.to_string(),
            DEFAULT_RATE_LIMIT_INTERVAL,
        )
    }

    /// Test/advanced constructor: override the base URL (a local HTTP stub)
    /// and/or the rate-limit interval (kept tiny in tests so a
    /// rate-limit-blocks assertion doesn't need a real second-plus sleep).
    pub fn with_options(token: String, base_url: String, rate_limit_interval: Duration) -> Self {
        Self {
            token,
            base_url,
            offset: AtomicI64::new(0),
            rate_limiter: RateLimiter::new(rate_limit_interval),
        }
    }

    fn api_url(&self, method: &str) -> String {
        format!(
            "{}/bot{}/{method}",
            self.base_url.trim_end_matches('/'),
            self.token
        )
    }

    /// Format a `reqwest::Error` for logs/errors WITHOUT the bot token.
    ///
    /// Telegram puts the token in the URL path (`/bot<token>/<method>`), and
    /// `reqwest::Error`'s `Display` includes the request URL when one is
    /// attached ("error sending request for url (…)"), so a naive
    /// `format!("{error}")` on any transient network failure would print the
    /// live token into ordinary server logs. Strip the URL from the error
    /// (`without_url`) and, belt-and-braces, redact any literal token
    /// occurrence in whatever text remains (e.g. proxy errors that embed the
    /// target URL themselves).
    fn sanitize_error(&self, error: reqwest::Error) -> String {
        let text = error.without_url().to_string();
        if self.token.is_empty() {
            return text;
        }
        text.replace(&self.token, "[REDACTED]")
    }

    async fn get_updates(
        &self,
        offset: i64,
        timeout_secs: u64,
    ) -> PlatformResult<Vec<TelegramUpdate>> {
        // Issue #458: request `callback_query` updates alongside `message` —
        // without `allowed_updates`, a bot that has never called `setWebhook`
        // already receives both by default, but being explicit keeps this
        // adapter correct even if that default ever changes upstream.
        let allowed_updates =
            serde_json::to_string(&["message", "callback_query"]).unwrap_or_default();
        let response = http_client()
            .get(self.api_url("getUpdates"))
            .query(&[
                ("offset", offset.to_string()),
                ("timeout", timeout_secs.to_string()),
                ("allowed_updates", allowed_updates),
            ])
            // Generous margin over Telegram's own long-poll timeout so the
            // HTTP client doesn't time out the connection out from under a
            // legitimately-long-held poll.
            .timeout(Duration::from_secs(timeout_secs + 15))
            .send()
            .await
            .map_err(|error| {
                PlatformError::other(format!(
                    "getUpdates request failed: {}",
                    self.sanitize_error(error)
                ))
            })?;

        let parsed: TelegramResponse<Vec<TelegramUpdate>> =
            response.json().await.map_err(|error| {
                PlatformError::other(format!(
                    "getUpdates response parse failed: {}",
                    self.sanitize_error(error)
                ))
            })?;

        if !parsed.ok {
            return Err(PlatformError::other(
                parsed
                    .description
                    .unwrap_or_else(|| "getUpdates returned ok=false".to_string()),
            ));
        }
        Ok(parsed.result.unwrap_or_default())
    }

    /// Converts a raw update into a bridge-facing [`InboundMessage`].
    /// Returns `None` for updates this MVP doesn't handle (no `message`, no
    /// text, no sender) — the caller still advances the offset for these so
    /// Telegram never re-delivers them.
    fn to_inbound_message(update: &TelegramUpdate) -> Option<InboundMessage> {
        let message = update.message.as_ref()?;
        let text = message.text.clone()?;
        let from = message.from.as_ref()?;
        let sent_at = chrono::DateTime::<chrono::Utc>::from_timestamp(message.date, 0)
            .unwrap_or_else(chrono::Utc::now);
        Some(InboundMessage {
            platform: "telegram".to_string(),
            chat_id: message.chat.id.to_string(),
            user_id: from.id.to_string(),
            message_id: update.update_id.to_string(),
            sent_at,
            text,
            reply_ctx: ReplyCtx(serde_json::json!({ "chat_id": message.chat.id })),
        })
    }

    /// Converts a raw update's `callback_query` into a bridge-facing
    /// [`Inbound::Callback`]. Returns `None` when there's no
    /// `callback_query`, or it's missing `from`/`message`/`data` (nothing to
    /// route a resolution against) — the caller still advances the offset
    /// for these.
    fn to_inbound_callback(update: &TelegramUpdate) -> Option<Inbound> {
        let callback_query = update.callback_query.as_ref()?;
        let from = callback_query.from.as_ref()?;
        let message = callback_query.message.as_ref()?;
        let data = callback_query.data.clone()?;
        Some(Inbound::Callback(CallbackQuery {
            platform: "telegram".to_string(),
            chat_id: message.chat.id.to_string(),
            user_id: from.id.to_string(),
            callback_query_id: callback_query.id.clone(),
            data,
            reply_ctx: ReplyCtx(serde_json::json!({ "chat_id": message.chat.id })),
        }))
    }

    /// One `getUpdates(offset, timeout_secs)` cycle: fetches, advances
    /// `self.offset` past EVERY returned update (so Telegram never
    /// re-delivers one this MVP skips), and returns the subset that convert
    /// to an [`Inbound`] event (a text message or a button-press callback).
    /// Used both for `start()`'s drain-on-start pass (`timeout_secs = 0`,
    /// result discarded) and its main long-poll loop — factored out so tests
    /// can drive a single cycle deterministically against a local HTTP stub
    /// without looping forever.
    async fn poll_once(&self, timeout_secs: u64) -> PlatformResult<Vec<Inbound>> {
        let offset = self.offset.load(Ordering::SeqCst);
        let updates = self.get_updates(offset, timeout_secs).await?;

        let mut events = Vec::with_capacity(updates.len());
        for update in &updates {
            // Always advance past this update_id, whether or not we forward
            // it — an un-forwarded update (no text, no sender, …) would
            // otherwise be redelivered by Telegram forever.
            self.offset.store(update.update_id + 1, Ordering::SeqCst);
            if let Some(callback) = Self::to_inbound_callback(update) {
                events.push(callback);
            } else if let Some(message) = Self::to_inbound_message(update) {
                events.push(Inbound::Message(message));
            }
        }
        Ok(events)
    }

    fn extract_chat_id(ctx: &ReplyCtx) -> PlatformResult<String> {
        Self::extract_chat_id_value(&ctx.0)
    }

    fn extract_chat_id_value(value: &serde_json::Value) -> PlatformResult<String> {
        value
            .get("chat_id")
            .and_then(|v| {
                v.as_i64()
                    .map(|n| n.to_string())
                    .or_else(|| v.as_str().map(|s| s.to_string()))
            })
            .ok_or_else(|| PlatformError::other("reply_ctx is missing chat_id"))
    }

    /// Telegram's `reply_markup` inline-keyboard shape: `{"inline_keyboard":
    /// [[{"text": ..., "callback_data": ...}], ...]}`, JSON-encoded as a
    /// single form field (issue #458).
    fn build_reply_markup(buttons: &[Vec<Button>]) -> String {
        let rows: Vec<Vec<serde_json::Value>> = buttons
            .iter()
            .map(|row| {
                row.iter()
                    .map(|button| {
                        serde_json::json!({
                            "text": button.label,
                            "callback_data": button.callback_data,
                        })
                    })
                    .collect()
            })
            .collect();
        serde_json::json!({ "inline_keyboard": rows }).to_string()
    }
}

#[async_trait::async_trait]
impl Platform for TelegramPlatform {
    fn name(&self) -> &str {
        "telegram"
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
        // Drain-on-start: fetch (and silently discard) any backlog that
        // accumulated while the bot was offline, so a restart never replays
        // a burst of stale prompts. A non-blocking (`timeout=0`) call.
        match self.poll_once(0).await {
            Ok(drained) if !drained.is_empty() => {
                tracing::info!(
                    "connect: telegram drained {} stale update(s) on start",
                    drained.len()
                );
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!("connect: telegram drain-on-start failed (continuing): {error}");
            }
        }

        loop {
            match self.poll_once(LONG_POLL_TIMEOUT_SECS).await {
                Ok(events) => {
                    for event in events {
                        if inbound.send(event).await.is_err() {
                            // Receiver dropped: the manager is shutting down.
                            return Ok(());
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!("connect: telegram getUpdates failed, retrying: {error}");
                    tokio::time::sleep(RETRY_BACKOFF).await;
                }
            }
        }
    }

    async fn reply(&self, ctx: &ReplyCtx, msg: OutboundMessage) -> PlatformResult<MessageRef> {
        let chat_id = Self::extract_chat_id(ctx)?;
        let mut last_message_id = None;
        let reply_markup = msg.buttons.as_deref().map(Self::build_reply_markup);
        let chunks = chunk_message(&msg.text, MAX_MESSAGE_CHARS);
        let chunk_count = chunks.len();

        for (index, chunk) in chunks.into_iter().enumerate() {
            self.rate_limiter.wait(&chat_id).await;

            let mut form: Vec<(&str, String)> = vec![("chat_id", chat_id.clone()), ("text", chunk)];
            // Attach the keyboard only to the LAST chunk — buttons make sense
            // on one message, not on earlier text-overflow spillover.
            if index + 1 == chunk_count {
                if let Some(markup) = &reply_markup {
                    form.push(("reply_markup", markup.clone()));
                }
            }

            let response = http_client()
                .post(self.api_url("sendMessage"))
                .form(&form)
                .send()
                .await
                .map_err(|error| {
                    PlatformError::other(format!(
                        "sendMessage request failed: {}",
                        self.sanitize_error(error)
                    ))
                })?;

            let parsed: TelegramResponse<TelegramMessage> =
                response.json().await.map_err(|error| {
                    PlatformError::other(format!(
                        "sendMessage response parse failed: {}",
                        self.sanitize_error(error)
                    ))
                })?;

            if !parsed.ok {
                return Err(PlatformError::other(
                    parsed
                        .description
                        .unwrap_or_else(|| "sendMessage returned ok=false".to_string()),
                ));
            }
            last_message_id = parsed.result.map(|m| m.message_id);
        }

        Ok(MessageRef(serde_json::json!({
            "chat_id": chat_id,
            "message_id": last_message_id,
        })))
    }

    async fn edit(&self, msg_ref: &MessageRef, new: OutboundMessage) -> PlatformResult<()> {
        let chat_id = Self::extract_chat_id_value(&msg_ref.0)?;
        let message_id = msg_ref
            .0
            .get("message_id")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| PlatformError::other("message_ref is missing message_id"))?;

        self.rate_limiter.wait(&chat_id).await;

        let mut form: Vec<(&str, String)> = vec![
            ("chat_id", chat_id),
            ("message_id", message_id.to_string()),
            ("text", new.text),
        ];
        if let Some(buttons) = &new.buttons {
            form.push(("reply_markup", Self::build_reply_markup(buttons)));
        }

        let response = http_client()
            .post(self.api_url("editMessageText"))
            .form(&form)
            .send()
            .await
            .map_err(|error| {
                PlatformError::other(format!(
                    "editMessageText request failed: {}",
                    self.sanitize_error(error)
                ))
            })?;

        let parsed: TelegramResponse<TelegramMessage> = response.json().await.map_err(|error| {
            PlatformError::other(format!(
                "editMessageText response parse failed: {}",
                self.sanitize_error(error)
            ))
        })?;

        if !parsed.ok {
            // The caller (`connect::render::StreamingRenderer`) degrades to a
            // fresh `reply()` on any edit error — a stale/too-old message, an
            // unchanged-content 400, or anything else Telegram rejects.
            return Err(PlatformError::other(parsed.description.unwrap_or_else(
                || "editMessageText returned ok=false".to_string(),
            )));
        }
        Ok(())
    }

    async fn answer_callback(
        &self,
        callback_query_id: &str,
        text: Option<&str>,
    ) -> PlatformResult<()> {
        // Deliberately NOT rate-limited: `answerCallbackQuery` isn't a chat
        // message (it dismisses the client's loading spinner) and Telegram
        // expects it promptly — sharing the per-chat send throttle here would
        // only risk the ack timing out.
        let mut form: Vec<(&str, String)> =
            vec![("callback_query_id", callback_query_id.to_string())];
        if let Some(text) = text {
            form.push(("text", text.to_string()));
        }

        let response = http_client()
            .post(self.api_url("answerCallbackQuery"))
            .form(&form)
            .send()
            .await
            .map_err(|error| {
                PlatformError::other(format!(
                    "answerCallbackQuery request failed: {}",
                    self.sanitize_error(error)
                ))
            })?;

        let parsed: TelegramResponse<bool> = response.json().await.map_err(|error| {
            PlatformError::other(format!(
                "answerCallbackQuery response parse failed: {}",
                self.sanitize_error(error)
            ))
        })?;

        if !parsed.ok {
            return Err(PlatformError::other(parsed.description.unwrap_or_else(
                || "answerCallbackQuery returned ok=false".to_string(),
            )));
        }
        Ok(())
    }

    async fn stop(&self) -> PlatformResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn platform_with_stub(base_url: String) -> TelegramPlatform {
        TelegramPlatform::with_options(
            "test-token".to_string(),
            base_url,
            Duration::from_millis(50),
        )
    }

    async fn wait_for_requests(
        server: &wiremock::MockServer,
        expected: usize,
    ) -> Vec<wiremock::Request> {
        for _ in 0..100 {
            if let Some(requests) = server.received_requests().await {
                if requests.len() >= expected {
                    return requests;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        server.received_requests().await.unwrap_or_default()
    }

    #[tokio::test]
    async fn poll_once_advances_offset_past_every_returned_update() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/bottest-token/getUpdates"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "ok": true,
                    "result": [
                        {
                            "update_id": 100,
                            "message": {
                                "message_id": 1,
                                "date": 1_700_000_000,
                                "chat": { "id": 42 },
                                "from": { "id": 7 },
                                "text": "hello"
                            }
                        },
                        {
                            "update_id": 101
                            // no "message" -- must still advance the offset past it.
                        }
                    ]
                })),
            )
            .mount(&server)
            .await;

        let platform = platform_with_stub(server.uri());
        let events = platform.poll_once(30).await.expect("poll_once succeeds");

        assert_eq!(events.len(), 1);
        match &events[0] {
            Inbound::Message(message) => {
                assert_eq!(message.chat_id, "42");
                assert_eq!(message.user_id, "7");
                assert_eq!(message.message_id, "100");
                assert_eq!(message.text, "hello");
            }
            Inbound::Callback(_) => panic!("expected a message event"),
        }
        assert_eq!(platform.offset.load(Ordering::SeqCst), 102);
    }

    #[tokio::test]
    async fn poll_once_next_call_requests_the_advanced_offset() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/bottest-token/getUpdates"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ok": true,
                "result": [
                    {
                        "update_id": 5,
                        "message": {
                            "message_id": 1, "date": 1, "chat": {"id": 1}, "from": {"id": 1}, "text": "hi"
                        }
                    }
                ]
            })))
            .mount(&server)
            .await;

        let platform = platform_with_stub(server.uri());
        platform.poll_once(30).await.unwrap();
        assert_eq!(platform.offset.load(Ordering::SeqCst), 6);

        let requests = wait_for_requests(&server, 1).await;
        let query: HashMap<String, String> = requests[0]
            .url
            .query_pairs()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert_eq!(query.get("offset"), Some(&"0".to_string()));

        // A second poll must request the ADVANCED offset.
        let _ = platform.poll_once(30).await;
        let requests = wait_for_requests(&server, 2).await;
        let query: HashMap<String, String> = requests[1]
            .url
            .query_pairs()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert_eq!(query.get("offset"), Some(&"6".to_string()));
    }

    #[tokio::test]
    async fn reply_chunks_long_text_into_multiple_send_message_calls() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/bottest-token/sendMessage"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "ok": true,
                    "result": { "message_id": 1, "date": 1, "chat": { "id": 1 } }
                })),
            )
            .mount(&server)
            .await;

        let platform = platform_with_stub(server.uri());
        let ctx = ReplyCtx(serde_json::json!({ "chat_id": 1 }));
        let long_text = "a".repeat(9000); // -> 3 chunks at 4096

        platform
            .reply(&ctx, OutboundMessage::text(long_text))
            .await
            .expect("reply succeeds");

        let requests = wait_for_requests(&server, 3).await;
        assert_eq!(requests.len(), 3, "expected exactly 3 sendMessage calls");
    }

    #[tokio::test]
    async fn reply_short_text_sends_exactly_one_message() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/bottest-token/sendMessage"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "ok": true,
                    "result": { "message_id": 1, "date": 1, "chat": { "id": 1 } }
                })),
            )
            .mount(&server)
            .await;

        let platform = platform_with_stub(server.uri());
        let ctx = ReplyCtx(serde_json::json!({ "chat_id": 1 }));

        platform
            .reply(&ctx, OutboundMessage::text("hello"))
            .await
            .expect("reply succeeds");

        let requests = wait_for_requests(&server, 1).await;
        assert_eq!(requests.len(), 1);
    }

    #[tokio::test]
    async fn reply_rate_limits_consecutive_sends_to_the_same_chat() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/bottest-token/sendMessage"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "ok": true,
                    "result": { "message_id": 1, "date": 1, "chat": { "id": 1 } }
                })),
            )
            .mount(&server)
            .await;

        // 50ms rate-limit interval (see `platform_with_stub`) keeps the test fast.
        let platform = platform_with_stub(server.uri());
        let ctx = ReplyCtx(serde_json::json!({ "chat_id": 1 }));

        let start = tokio::time::Instant::now();
        platform
            .reply(&ctx, OutboundMessage::text("first"))
            .await
            .unwrap();
        platform
            .reply(&ctx, OutboundMessage::text("second"))
            .await
            .unwrap();
        let elapsed = start.elapsed();

        assert!(
            elapsed >= Duration::from_millis(50),
            "second send to the same chat must block for the rate-limit interval, elapsed={elapsed:?}"
        );
    }

    #[tokio::test]
    async fn reply_does_not_rate_limit_different_chats() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/bottest-token/sendMessage"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "ok": true,
                    "result": { "message_id": 1, "date": 1, "chat": { "id": 1 } }
                })),
            )
            .mount(&server)
            .await;

        let platform = platform_with_stub(server.uri());
        let ctx_a = ReplyCtx(serde_json::json!({ "chat_id": 1 }));
        let ctx_b = ReplyCtx(serde_json::json!({ "chat_id": 2 }));

        let start = tokio::time::Instant::now();
        platform
            .reply(&ctx_a, OutboundMessage::text("first"))
            .await
            .unwrap();
        platform
            .reply(&ctx_b, OutboundMessage::text("second"))
            .await
            .unwrap();
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_millis(50),
            "sends to DIFFERENT chats must not share a rate-limit slot, elapsed={elapsed:?}"
        );
    }

    /// Issue #454 follow-up: `RateLimiter::next_allowed` must not grow one
    /// entry per distinct chat for the life of the process — a chat's entry
    /// is swept once its reserved slot has elapsed, rather than lingering
    /// forever.
    #[tokio::test]
    async fn rate_limiter_sweeps_stale_entries_instead_of_growing_unbounded() {
        let limiter = RateLimiter::new(Duration::from_millis(20));

        limiter.wait("chat-1").await;
        assert_eq!(limiter.tracked_chat_count().await, 1);

        // Let chat-1's reserved slot fully elapse before any other chat is
        // seen.
        tokio::time::sleep(Duration::from_millis(40)).await;

        // A wait for a DIFFERENT chat must sweep chat-1's now-stale entry
        // out rather than accumulate it forever.
        limiter.wait("chat-2").await;
        assert_eq!(
            limiter.tracked_chat_count().await,
            1,
            "stale chat-1 entry must have been swept when chat-2 was scheduled"
        );

        // Simulate many distinct, one-shot chats spaced far enough apart
        // that each previous entry is stale by the time the next arrives —
        // the tracked count must stay bounded to the currently-relevant
        // entry/entries, not grow to the number of chats ever seen.
        for i in 0..50 {
            tokio::time::sleep(Duration::from_millis(25)).await;
            limiter.wait(&format!("burst-chat-{i}")).await;
            assert!(
                limiter.tracked_chat_count().await <= 2,
                "map grew unbounded after {i} one-shot chats"
            );
        }
    }

    #[tokio::test]
    async fn capabilities_advertise_buttons_and_edit_message() {
        let platform = platform_with_stub("http://localhost:0".to_string());
        let caps = platform.capabilities();
        assert!(caps.buttons);
        assert!(caps.edit_message);
        assert!(!caps.images);
        assert!(!caps.files);
    }

    #[tokio::test]
    async fn get_updates_requests_message_and_callback_query_updates() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/bottest-token/getUpdates"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({ "ok": true, "result": [] })),
            )
            .mount(&server)
            .await;

        let platform = platform_with_stub(server.uri());
        platform.poll_once(30).await.unwrap();

        let requests = wait_for_requests(&server, 1).await;
        let query: HashMap<String, String> = requests[0]
            .url
            .query_pairs()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let allowed: Vec<String> = serde_json::from_str(
            query
                .get("allowed_updates")
                .expect("allowed_updates present"),
        )
        .unwrap();
        assert_eq!(
            allowed,
            vec!["message".to_string(), "callback_query".to_string()]
        );
    }

    #[tokio::test]
    async fn poll_once_converts_callback_query_updates_to_inbound_callbacks() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/bottest-token/getUpdates"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "ok": true,
                    "result": [
                        {
                            "update_id": 200,
                            "callback_query": {
                                "id": "cbq-1",
                                "from": { "id": 7 },
                                "message": {
                                    "message_id": 1,
                                    "date": 1_700_000_000,
                                    "chat": { "id": 42 }
                                },
                                "data": "nonce123:1"
                            }
                        }
                    ]
                })),
            )
            .mount(&server)
            .await;

        let platform = platform_with_stub(server.uri());
        let events = platform.poll_once(30).await.expect("poll_once succeeds");

        assert_eq!(events.len(), 1);
        match &events[0] {
            Inbound::Callback(callback) => {
                assert_eq!(callback.platform, "telegram");
                assert_eq!(callback.chat_id, "42");
                assert_eq!(callback.user_id, "7");
                assert_eq!(callback.callback_query_id, "cbq-1");
                assert_eq!(callback.data, "nonce123:1");
            }
            Inbound::Message(_) => panic!("expected a callback event"),
        }
        assert_eq!(platform.offset.load(Ordering::SeqCst), 201);
    }

    #[tokio::test]
    async fn reply_with_buttons_sends_inline_keyboard_reply_markup() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/bottest-token/sendMessage"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "ok": true,
                    "result": { "message_id": 9, "date": 1, "chat": { "id": 1 } }
                })),
            )
            .mount(&server)
            .await;

        let platform = platform_with_stub(server.uri());
        let ctx = ReplyCtx(serde_json::json!({ "chat_id": 1 }));
        let outbound = OutboundMessage::text("Approve?").with_buttons(vec![
            vec![Button::new("Approve", "n1:0")],
            vec![Button::new("Deny", "n1:1")],
        ]);

        let msg_ref = platform
            .reply(&ctx, outbound)
            .await
            .expect("reply succeeds");
        assert_eq!(
            msg_ref.0.get("message_id").and_then(|v| v.as_i64()),
            Some(9)
        );

        let requests = wait_for_requests(&server, 1).await;
        let body = String::from_utf8(requests[0].body.clone()).unwrap();
        let form: HashMap<String, String> = url::form_urlencoded::parse(body.as_bytes())
            .into_owned()
            .collect();
        let markup: serde_json::Value =
            serde_json::from_str(form.get("reply_markup").expect("reply_markup present")).unwrap();
        assert_eq!(
            markup["inline_keyboard"][0][0]["callback_data"],
            serde_json::json!("n1:0")
        );
        assert_eq!(
            markup["inline_keyboard"][1][0]["text"],
            serde_json::json!("Deny")
        );
    }

    #[tokio::test]
    async fn edit_sends_edit_message_text_with_the_stored_message_id() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/bottest-token/editMessageText"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "ok": true,
                    "result": { "message_id": 9, "date": 1, "chat": { "id": 1 } }
                })),
            )
            .mount(&server)
            .await;

        let platform = platform_with_stub(server.uri());
        let msg_ref = MessageRef(serde_json::json!({ "chat_id": 1, "message_id": 9 }));

        platform
            .edit(&msg_ref, OutboundMessage::text("updated"))
            .await
            .expect("edit succeeds");

        let requests = wait_for_requests(&server, 1).await;
        let body = String::from_utf8(requests[0].body.clone()).unwrap();
        let form: HashMap<String, String> = url::form_urlencoded::parse(body.as_bytes())
            .into_owned()
            .collect();
        assert_eq!(form.get("message_id").map(String::as_str), Some("9"));
        assert_eq!(form.get("text").map(String::as_str), Some("updated"));
    }

    #[tokio::test]
    async fn edit_returns_an_error_on_a_400_response_instead_of_panicking() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/bottest-token/editMessageText"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "ok": false,
                    "description": "Bad Request: message is not modified"
                })),
            )
            .mount(&server)
            .await;

        let platform = platform_with_stub(server.uri());
        let msg_ref = MessageRef(serde_json::json!({ "chat_id": 1, "message_id": 9 }));

        let result = platform
            .edit(&msg_ref, OutboundMessage::text("same text"))
            .await;
        // The caller (`connect::render`) is responsible for degrading to a
        // fresh send on this Err — verified in `render.rs`'s
        // `streaming_mode_edit_failure_degrades_to_a_fresh_send` test.
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn answer_callback_posts_callback_query_id_and_optional_text() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path(
                "/bottest-token/answerCallbackQuery",
            ))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({ "ok": true, "result": true })),
            )
            .mount(&server)
            .await;

        let platform = platform_with_stub(server.uri());
        platform
            .answer_callback("cbq-1", Some("This action has expired."))
            .await
            .expect("answer_callback succeeds");

        let requests = wait_for_requests(&server, 1).await;
        let body = String::from_utf8(requests[0].body.clone()).unwrap();
        let form: HashMap<String, String> = url::form_urlencoded::parse(body.as_bytes())
            .into_owned()
            .collect();
        assert_eq!(
            form.get("callback_query_id").map(String::as_str),
            Some("cbq-1")
        );
        assert_eq!(
            form.get("text").map(String::as_str),
            Some("This action has expired.")
        );
    }

    #[tokio::test]
    async fn answer_callback_round_trips_without_text() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path(
                "/bottest-token/answerCallbackQuery",
            ))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({ "ok": true, "result": true })),
            )
            .mount(&server)
            .await;

        let platform = platform_with_stub(server.uri());
        platform
            .answer_callback("cbq-2", None)
            .await
            .expect("answer_callback succeeds");

        let requests = wait_for_requests(&server, 1).await;
        let body = String::from_utf8(requests[0].body.clone()).unwrap();
        let form: HashMap<String, String> = url::form_urlencoded::parse(body.as_bytes())
            .into_owned()
            .collect();
        assert!(!form.contains_key("text"));
    }

    /// The bot token lives in the request URL path; `reqwest::Error`'s
    /// `Display` would print it on any transport failure. Force a real
    /// connection error (port 1 is unroutable/refused) and assert the token
    /// never appears in the surfaced error text.
    #[tokio::test]
    async fn transport_errors_never_leak_the_bot_token() {
        let token = "123456:SECRET-TOKEN-MUST-NOT-LEAK";
        let platform = TelegramPlatform::with_options(
            token.to_string(),
            "http://127.0.0.1:1".to_string(),
            Duration::from_millis(1),
        );
        let err = platform
            .get_updates(0, 0)
            .await
            .expect_err("port 1 must refuse the connection");
        let text = format!("{err}");
        assert!(
            !text.contains(token) && !text.contains("SECRET-TOKEN"),
            "token leaked into error text: {text}"
        );
        assert!(
            text.contains("getUpdates request failed"),
            "unexpected error shape: {text}"
        );
    }
}
