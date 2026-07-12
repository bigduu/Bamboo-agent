//! Telegram long-poll platform adapter (issue #452, epic #447's first
//! platform: no public IP, no webhook, no WS — just `getUpdates` over plain
//! HTTPS).
//!
//! Buttons / `editMessageText` are NOT in this phase — [`Capabilities`]
//! advertises them `false` (see epic #447's phase list).

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

use tokio::sync::{mpsc, Mutex as AsyncMutex};

use super::super::platform::{
    Capabilities, InboundMessage, MessageRef, OutboundMessage, Platform, PlatformError,
    PlatformResult, ReplyCtx,
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
            scheduled
        };
        if scheduled > now {
            tokio::time::sleep(scheduled - now).await;
        }
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

    async fn get_updates(
        &self,
        offset: i64,
        timeout_secs: u64,
    ) -> PlatformResult<Vec<TelegramUpdate>> {
        let response = http_client()
            .get(self.api_url("getUpdates"))
            .query(&[
                ("offset", offset.to_string()),
                ("timeout", timeout_secs.to_string()),
            ])
            // Generous margin over Telegram's own long-poll timeout so the
            // HTTP client doesn't time out the connection out from under a
            // legitimately-long-held poll.
            .timeout(Duration::from_secs(timeout_secs + 15))
            .send()
            .await
            .map_err(|error| PlatformError::other(format!("getUpdates request failed: {error}")))?;

        let parsed: TelegramResponse<Vec<TelegramUpdate>> =
            response.json().await.map_err(|error| {
                PlatformError::other(format!("getUpdates response parse failed: {error}"))
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

    /// One `getUpdates(offset, timeout_secs)` cycle: fetches, advances
    /// `self.offset` past EVERY returned update (so Telegram never
    /// re-delivers one this MVP skips), and returns the subset that convert
    /// to an [`InboundMessage`]. Used both for `start()`'s drain-on-start
    /// pass (`timeout_secs = 0`, result discarded) and its main long-poll
    /// loop — factored out so tests can drive a single cycle deterministically
    /// against a local HTTP stub without looping forever.
    async fn poll_once(&self, timeout_secs: u64) -> PlatformResult<Vec<InboundMessage>> {
        let offset = self.offset.load(Ordering::SeqCst);
        let updates = self.get_updates(offset, timeout_secs).await?;

        let mut messages = Vec::with_capacity(updates.len());
        for update in &updates {
            // Always advance past this update_id, whether or not we forward
            // it — an un-forwarded update (no text, no sender, …) would
            // otherwise be redelivered by Telegram forever.
            self.offset.store(update.update_id + 1, Ordering::SeqCst);
            if let Some(message) = Self::to_inbound_message(update) {
                messages.push(message);
            }
        }
        Ok(messages)
    }

    fn extract_chat_id(ctx: &ReplyCtx) -> PlatformResult<String> {
        ctx.0
            .get("chat_id")
            .and_then(|v| {
                v.as_i64()
                    .map(|n| n.to_string())
                    .or_else(|| v.as_str().map(|s| s.to_string()))
            })
            .ok_or_else(|| PlatformError::other("reply_ctx is missing chat_id"))
    }
}

#[async_trait::async_trait]
impl Platform for TelegramPlatform {
    fn name(&self) -> &str {
        "telegram"
    }

    fn capabilities(&self) -> Capabilities {
        // Buttons/edit-message/attachments are a later phase of epic #447.
        Capabilities::default()
    }

    async fn start(&self, inbound: mpsc::Sender<InboundMessage>) -> PlatformResult<()> {
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
                Ok(messages) => {
                    for message in messages {
                        if inbound.send(message).await.is_err() {
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

        for chunk in chunk_message(&msg.text, MAX_MESSAGE_CHARS) {
            self.rate_limiter.wait(&chat_id).await;

            let response = http_client()
                .post(self.api_url("sendMessage"))
                .form(&[("chat_id", chat_id.as_str()), ("text", chunk.as_str())])
                .send()
                .await
                .map_err(|error| {
                    PlatformError::other(format!("sendMessage request failed: {error}"))
                })?;

            let parsed: TelegramResponse<TelegramMessage> =
                response.json().await.map_err(|error| {
                    PlatformError::other(format!("sendMessage response parse failed: {error}"))
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

    async fn edit(&self, _msg_ref: &MessageRef, _new: OutboundMessage) -> PlatformResult<()> {
        Err(PlatformError::other(
            "telegram adapter does not support edit_message in this phase (#452)",
        ))
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
        let messages = platform.poll_once(30).await.expect("poll_once succeeds");

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].chat_id, "42");
        assert_eq!(messages[0].user_id, "7");
        assert_eq!(messages[0].message_id, "100");
        assert_eq!(messages[0].text, "hello");
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

    #[tokio::test]
    async fn edit_is_unsupported_in_this_phase() {
        let platform = platform_with_stub("http://localhost:0".to_string());
        let msg_ref = MessageRef(serde_json::json!({}));
        let result = platform.edit(&msg_ref, OutboundMessage::text("x")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn capabilities_advertise_nothing_in_this_phase() {
        let platform = platform_with_stub("http://localhost:0".to_string());
        let caps = platform.capabilities();
        assert!(!caps.buttons);
        assert!(!caps.edit_message);
        assert!(!caps.images);
        assert!(!caps.files);
    }
}
