//! Feishu/Lark REST calls: `tenant_access_token` cache, send/update message,
//! bot self-info, and the per-chat outgoing rate limiter — the counterpart
//! to `ws.rs`'s event long-connection.

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;

use tokio::sync::Mutex as AsyncMutex;
use tokio::time::Instant;

use super::super::super::platform::{PlatformError, PlatformResult};

/// One shared `reqwest::Client` for every Feishu REST call (bootstrap POST
/// included — `ws.rs` reuses this too). Mirrors `telegram.rs`'s
/// `http_client()` — the workspace's pinned (native-tls) `reqwest`, never a
/// second connector.
pub(super) fn http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

/// Formats a `reqwest::Error` for logs/errors WITHOUT `app_secret` or the
/// current `tenant_access_token`. Both live in a request body/header (never
/// the URL), so a plain `error.without_url()` is already secret-free for
/// THIS adapter's error paths — but redact any literal occurrence anyway,
/// belt-and-braces, the same way `telegram.rs::sanitize_error` does for the
/// bot token (e.g. a misbehaving proxy that echoes the request body into its
/// own error text).
pub(super) fn sanitize_reqwest_error(error: reqwest::Error, secrets: &[&str]) -> String {
    let mut text = error.without_url().to_string();
    for secret in secrets {
        if !secret.is_empty() {
            text = text.replace(secret, "[REDACTED]");
        }
    }
    text
}

const TOKEN_REFRESH_MARGIN: Duration = Duration::from_secs(30 * 60);
/// Feishu-documented invalid/expired tenant_access_token error codes — force
/// a cache-busting refresh and retry exactly once.
const TOKEN_INVALID_CODES: [i64; 2] = [99991663, 99991661];
/// Rate-limit / quota error codes that must be waited out, never dropped.
const RATE_LIMIT_CODES: [i64; 2] = [99991400, 230020];
const RATE_LIMIT_HEADER: &str = "x-ogw-ratelimit-reset";
/// Upper bound on retry-after-rate-limit attempts for one logical send, so a
/// misbehaving/absent reset header can't hang a caller forever. Feishu's own
/// documented limits reset in low single-digit seconds, so this is generous.
const MAX_RATE_LIMIT_RETRIES: u32 = 5;
const DEFAULT_RATE_LIMIT_WAIT: Duration = Duration::from_secs(1);

#[derive(Debug, Default)]
struct TokenState {
    token: String,
    /// `None` until the first successful fetch — always treated as expired.
    expires_at: Option<Instant>,
}

/// Caches the `tenant_access_token`, refreshing when fewer than
/// [`TOKEN_REFRESH_MARGIN`] remain (or on-demand via [`TokenCache::invalidate`]
/// after a `99991663`/`99991661` response). The whole refresh happens under
/// the single lock, which gives single-flight behavior for free — concurrent
/// callers simply queue on the mutex rather than firing parallel refreshes.
pub(super) struct TokenCache {
    state: AsyncMutex<TokenState>,
}

impl TokenCache {
    pub fn new() -> Self {
        Self {
            state: AsyncMutex::new(TokenState::default()),
        }
    }

    /// Returns a valid token, refreshing first if necessary.
    pub async fn get(
        &self,
        base_url: &str,
        app_id: &str,
        app_secret: &str,
    ) -> PlatformResult<String> {
        let mut guard = self.state.lock().await;
        let needs_refresh = match guard.expires_at {
            Some(expires_at) => Instant::now() + TOKEN_REFRESH_MARGIN >= expires_at,
            None => true,
        };
        if needs_refresh {
            refresh_locked(&mut guard, base_url, app_id, app_secret).await?;
        }
        Ok(guard.token.clone())
    }

    /// Forces the NEXT [`TokenCache::get`] call to refresh, regardless of the
    /// cached expiry — used after a `99991663`/`99991661` "invalid token"
    /// response so a stale cached value is never retried a second time.
    pub async fn invalidate(&self) {
        let mut guard = self.state.lock().await;
        guard.expires_at = None;
    }
}

async fn refresh_locked(
    state: &mut TokenState,
    base_url: &str,
    app_id: &str,
    app_secret: &str,
) -> PlatformResult<()> {
    #[derive(serde::Deserialize)]
    struct TokenResponse {
        code: i64,
        #[serde(default)]
        msg: Option<String>,
        #[serde(default)]
        tenant_access_token: Option<String>,
        #[serde(default)]
        expire: Option<u64>,
    }

    let url = format!(
        "{}/open-apis/auth/v3/tenant_access_token/internal",
        base_url.trim_end_matches('/')
    );
    let response = http_client()
        .post(url)
        .json(&serde_json::json!({ "app_id": app_id, "app_secret": app_secret }))
        .send()
        .await
        .map_err(|error| {
            PlatformError::other(format!(
                "tenant_access_token request failed: {}",
                sanitize_reqwest_error(error, &[app_secret])
            ))
        })?;

    let parsed: TokenResponse = response.json().await.map_err(|error| {
        PlatformError::other(format!(
            "tenant_access_token response parse failed: {}",
            sanitize_reqwest_error(error, &[app_secret])
        ))
    })?;

    if parsed.code != 0 {
        return Err(PlatformError::other(format!(
            "tenant_access_token refresh failed (code={}): {}",
            parsed.code,
            parsed.msg.unwrap_or_default()
        )));
    }
    let token = parsed
        .tenant_access_token
        .ok_or_else(|| PlatformError::other("tenant_access_token response missing token"))?;
    let expire = Duration::from_secs(parsed.expire.unwrap_or(7200));

    state.token = token;
    state.expires_at = Some(Instant::now() + expire);
    Ok(())
}

/// Per-chat outgoing token bucket — identical shape/semantics to
/// `telegram.rs::RateLimiter` (blocks, never drops; a waiting chat never
/// blocks a send to a different chat).
pub(super) struct RateLimiter {
    next_allowed: AsyncMutex<HashMap<String, Instant>>,
    min_interval: Duration,
}

impl RateLimiter {
    pub fn new(min_interval: Duration) -> Self {
        Self {
            next_allowed: AsyncMutex::new(HashMap::new()),
            min_interval,
        }
    }

    pub async fn wait(&self, key: &str) {
        let now = Instant::now();
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

#[derive(Debug, serde::Deserialize)]
struct FeishuEnvelope<T> {
    code: i64,
    #[serde(default)]
    msg: Option<String>,
    #[serde(default)]
    data: Option<T>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct SendMessageData {
    #[serde(default)]
    message_id: Option<String>,
}

/// `x-ogw-ratelimit-reset` is documented as the number of seconds until the
/// limit resets. Parsed defensively — an absent/unparseable header falls
/// back to [`DEFAULT_RATE_LIMIT_WAIT`] rather than failing the send.
fn rate_limit_wait_from_headers(headers: &reqwest::header::HeaderMap) -> Duration {
    headers
        .get(RATE_LIMIT_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_RATE_LIMIT_WAIT)
}

/// `POST /open-apis/im/v1/messages?receive_id_type=chat_id`. `content` is
/// caller-supplied already-JSON-encoded-as-a-string (text: `{"text":"..."}`;
/// interactive: the card JSON itself, string-encoded) per the API contract.
/// Retries once on a stale token (`99991663`/`99991661`) and waits out (never
/// drops) a rate-limit response, up to [`MAX_RATE_LIMIT_RETRIES`].
pub(super) async fn send_message(
    tokens: &TokenCache,
    base_url: &str,
    app_id: &str,
    app_secret: &str,
    chat_id: &str,
    msg_type: &str,
    content: &str,
) -> PlatformResult<String> {
    let url = format!(
        "{}/open-apis/im/v1/messages?receive_id_type=chat_id",
        base_url.trim_end_matches('/')
    );
    let body = serde_json::json!({
        "receive_id": chat_id,
        "msg_type": msg_type,
        "content": content,
    });

    let mut token_retried = false;
    let mut rate_limit_attempts = 0u32;
    loop {
        let token = tokens.get(base_url, app_id, app_secret).await?;
        let response = http_client()
            .post(&url)
            .bearer_auth(&token)
            .json(&body)
            .send()
            .await
            .map_err(|error| {
                PlatformError::other(format!(
                    "im/v1/messages send failed: {}",
                    sanitize_reqwest_error(error, &[app_secret, &token])
                ))
            })?;

        let status = response.status();
        let headers = response.headers().clone();
        let parsed: FeishuEnvelope<SendMessageData> = response.json().await.map_err(|error| {
            PlatformError::other(format!(
                "im/v1/messages response parse failed: {}",
                sanitize_reqwest_error(error, &[app_secret, &token])
            ))
        })?;

        if parsed.code == 0 {
            return parsed
                .data
                .and_then(|d| d.message_id)
                .ok_or_else(|| PlatformError::other("im/v1/messages response missing message_id"));
        }

        if !token_retried && TOKEN_INVALID_CODES.contains(&parsed.code) {
            token_retried = true;
            tokens.invalidate().await;
            continue;
        }

        if (status.as_u16() == 429 || RATE_LIMIT_CODES.contains(&parsed.code))
            && rate_limit_attempts < MAX_RATE_LIMIT_RETRIES
        {
            rate_limit_attempts += 1;
            let wait = rate_limit_wait_from_headers(&headers);
            tracing::warn!(
                "connect: feishu send rate-limited (code={}), waiting {wait:?} before retry {rate_limit_attempts}/{MAX_RATE_LIMIT_RETRIES}",
                parsed.code
            );
            tokio::time::sleep(wait).await;
            continue;
        }

        return Err(PlatformError::other(format!(
            "im/v1/messages send failed (code={}): {}",
            parsed.code,
            parsed.msg.unwrap_or_default()
        )));
    }
}

/// `PATCH /open-apis/im/v1/messages/:message_id` — updates an interactive
/// card's content in place. Same token-retry/rate-limit-wait treatment as
/// [`send_message`].
pub(super) async fn update_card(
    tokens: &TokenCache,
    base_url: &str,
    app_id: &str,
    app_secret: &str,
    message_id: &str,
    content: &str,
) -> PlatformResult<()> {
    let url = format!(
        "{}/open-apis/im/v1/messages/{message_id}",
        base_url.trim_end_matches('/')
    );
    let body = serde_json::json!({ "content": content });

    let mut token_retried = false;
    let mut rate_limit_attempts = 0u32;
    loop {
        let token = tokens.get(base_url, app_id, app_secret).await?;
        let response = http_client()
            .patch(&url)
            .bearer_auth(&token)
            .json(&body)
            .send()
            .await
            .map_err(|error| {
                PlatformError::other(format!(
                    "im/v1/messages patch failed: {}",
                    sanitize_reqwest_error(error, &[app_secret, &token])
                ))
            })?;

        let status = response.status();
        let headers = response.headers().clone();
        let parsed: FeishuEnvelope<serde_json::Value> = response.json().await.map_err(|error| {
            PlatformError::other(format!(
                "im/v1/messages patch response parse failed: {}",
                sanitize_reqwest_error(error, &[app_secret, &token])
            ))
        })?;

        if parsed.code == 0 {
            return Ok(());
        }

        if !token_retried && TOKEN_INVALID_CODES.contains(&parsed.code) {
            token_retried = true;
            tokens.invalidate().await;
            continue;
        }

        if (status.as_u16() == 429 || RATE_LIMIT_CODES.contains(&parsed.code))
            && rate_limit_attempts < MAX_RATE_LIMIT_RETRIES
        {
            rate_limit_attempts += 1;
            let wait = rate_limit_wait_from_headers(&headers);
            tokio::time::sleep(wait).await;
            continue;
        }

        return Err(PlatformError::other(format!(
            "im/v1/messages patch failed (code={}): {}",
            parsed.code,
            parsed.msg.unwrap_or_default()
        )));
    }
}

/// `GET /open-apis/bot/v3/info` — the bot's own `open_id`, fetched once at
/// startup for @mention detection in group chats (`mod.rs`'s group-gating).
pub(super) async fn fetch_bot_open_id(
    tokens: &TokenCache,
    base_url: &str,
    app_id: &str,
    app_secret: &str,
) -> PlatformResult<String> {
    #[derive(Default, serde::Deserialize)]
    struct BotInfoData {
        #[serde(default)]
        open_id: Option<String>,
    }

    let url = format!("{}/open-apis/bot/v3/info", base_url.trim_end_matches('/'));
    let token = tokens.get(base_url, app_id, app_secret).await?;
    let response = http_client()
        .get(url)
        .bearer_auth(&token)
        .send()
        .await
        .map_err(|error| {
            PlatformError::other(format!(
                "bot/v3/info request failed: {}",
                sanitize_reqwest_error(error, &[app_secret, &token])
            ))
        })?;

    let parsed: FeishuEnvelope<BotInfoData> = response.json().await.map_err(|error| {
        PlatformError::other(format!(
            "bot/v3/info response parse failed: {}",
            sanitize_reqwest_error(error, &[app_secret, &token])
        ))
    })?;

    if parsed.code != 0 {
        return Err(PlatformError::other(format!(
            "bot/v3/info failed (code={}): {}",
            parsed.code,
            parsed.msg.unwrap_or_default()
        )));
    }
    parsed
        .data
        .and_then(|d| d.open_id)
        .ok_or_else(|| PlatformError::other("bot/v3/info response missing open_id"))
}
