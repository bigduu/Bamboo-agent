//! Retry / exponential-backoff for the *initial* outbound LLM HTTP request.
//!
//! All providers send a single HTTP request to the upstream LLM API with no
//! retry logic, so a transient 429 / 5xx / connection blip immediately
//! surfaces as a hard error (issue #18). This module adds a shared,
//! provider-agnostic retry around establishing that request.
//!
//! Scope — IMPORTANT for streaming safety:
//! - We retry only the act of *establishing* the response: sending the request
//!   and inspecting the response status code (the body has NOT been read yet).
//! - We never retry mid-stream. Once a successful (or non-retryable) response is
//!   returned, the caller reads the SSE body exactly as before; a stream that
//!   dies partway through is not re-issued (that would replay partial output).
//!
//! What counts as transient (and thus retryable):
//! - HTTP 429 (Too Many Requests) — `Retry-After` header is respected when present.
//! - HTTP 500 / 502 / 503 / 504.
//! - Transient `reqwest` errors: connect failures and timeouts.
//!
//! Everything else (other 4xx, JSON errors, success) is returned to the caller
//! immediately and unchanged.

use std::sync::OnceLock;
use std::time::Duration;

use reqwest::{RequestBuilder, Response, StatusCode};

/// Default number of *retries* after the first attempt (issue #18: "max 3
/// retries"). Total default attempts = `DEFAULT_MAX_RETRIES + 1`.
const DEFAULT_MAX_RETRIES: u32 = 3;
/// Default maximum number of *attempts* (1 initial + [`DEFAULT_MAX_RETRIES`]).
const DEFAULT_MAX_ATTEMPTS: u32 = DEFAULT_MAX_RETRIES + 1;
/// Default base delay for the first backoff step.
const DEFAULT_BASE_DELAY_MS: u64 = 500;
/// Default cap on any single *computed* backoff sleep.
const DEFAULT_MAX_DELAY_MS: u64 = 30_000;
/// Absolute ceiling applied to a server-provided `Retry-After`. The server is
/// authoritative about how long to wait, so we honor it even past `max_delay`,
/// but still bound it to avoid an unbounded sleep from a hostile/buggy header.
const RETRY_AFTER_CEILING: Duration = Duration::from_secs(60);

/// Tunables for the transient-failure retry loop.
///
/// Values are read from environment variables via [`RetryConfig::from_env`]
/// with the documented defaults:
/// - `BAMBOO_LLM_MAX_RETRIES` — number of *retries* after the first attempt, so
///   total attempts = retries + 1 (default `3` retries → up to 4 attempts; `0`
///   disables retrying).
/// - `BAMBOO_LLM_RETRY_BASE_DELAY_MS` — base backoff delay in ms (default 500).
/// - `BAMBOO_LLM_RETRY_MAX_DELAY_MS` — per-sleep cap in ms (default 30000).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryConfig {
    /// Total number of attempts, including the first. `1` disables retrying.
    pub max_attempts: u32,
    /// Base delay used for exponential backoff (`base * 2^attempt`).
    pub base_delay: Duration,
    /// Upper bound applied to every individual backoff sleep.
    pub max_delay: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            base_delay: Duration::from_millis(DEFAULT_BASE_DELAY_MS),
            max_delay: Duration::from_millis(DEFAULT_MAX_DELAY_MS),
        }
    }
}

impl RetryConfig {
    /// Build a [`RetryConfig`] from environment variables, falling back to the
    /// documented defaults for any unset / unparseable value.
    ///
    /// `BAMBOO_LLM_MAX_RETRIES` is interpreted as the number of *retries* on top
    /// of the initial attempt, matching the issue wording ("max 3 retries").
    /// A value of `0` disables retrying entirely (a single attempt).
    pub fn from_env() -> Self {
        let default = Self::default();

        let max_attempts = std::env::var("BAMBOO_LLM_MAX_RETRIES")
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
            // retries -> attempts (initial + retries), saturating to avoid overflow.
            .map(|retries| retries.saturating_add(1))
            .unwrap_or(default.max_attempts);

        let base_delay = std::env::var("BAMBOO_LLM_RETRY_BASE_DELAY_MS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or(default.base_delay);

        let max_delay = std::env::var("BAMBOO_LLM_RETRY_MAX_DELAY_MS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or(default.max_delay);

        Self {
            max_attempts: max_attempts.max(1),
            base_delay,
            max_delay,
        }
    }
}

/// Process-wide retry config, resolved from the environment on first use.
///
/// Providers don't carry a `RetryConfig` field (keeps their structs and the
/// many constructors untouched); they call [`global`] at the send site instead.
/// The env vars are read exactly once.
pub fn global() -> &'static RetryConfig {
    static CONFIG: OnceLock<RetryConfig> = OnceLock::new();
    CONFIG.get_or_init(RetryConfig::from_env)
}

/// Whether an HTTP status code represents a *transient* upstream failure that is
/// worth retrying.
fn is_retryable_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS
        || matches!(
            status,
            StatusCode::INTERNAL_SERVER_ERROR
                | StatusCode::BAD_GATEWAY
                | StatusCode::SERVICE_UNAVAILABLE
                | StatusCode::GATEWAY_TIMEOUT
        )
}

/// Whether a `reqwest` transport error is transient (connect / timeout). Body or
/// decode errors are not retried because they are not raised here (the body is
/// untouched at this stage).
fn is_retryable_reqwest_error(err: &reqwest::Error) -> bool {
    err.is_timeout() || err.is_connect()
}

/// Parse a `Retry-After` header into a delay. Supports the delay-seconds form
/// (e.g. `Retry-After: 2`). The HTTP-date form is intentionally ignored (rare
/// for LLM APIs) and falls back to computed backoff.
fn parse_retry_after(response: &Response) -> Option<Duration> {
    let value = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .to_string();
    value.parse::<u64>().ok().map(Duration::from_secs)
}

/// Compute the exponential backoff delay for a given zero-based attempt index,
/// with full jitter, capped at `max_delay`.
fn backoff_delay(config: &RetryConfig, attempt: u32) -> Duration {
    // base * 2^attempt, saturating, then capped.
    let factor = 1u64.checked_shl(attempt).unwrap_or(u64::MAX);
    let raw_ms = (config.base_delay.as_millis() as u64).saturating_mul(factor);
    let capped_ms = raw_ms.min(config.max_delay.as_millis() as u64);
    Duration::from_millis(jitter_ms(capped_ms))
}

/// Apply "full jitter": a random value in `[ceil(ms/2), ms]`. Keeps a sensible
/// floor so backoff still grows while spreading out concurrent retries.
fn jitter_ms(ms: u64) -> u64 {
    if ms == 0 {
        return 0;
    }
    // Cheap, dependency-free jitter source derived from the wall clock.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    let half = ms / 2;
    let span = ms - half; // = ceil(ms/2)
    half + (nanos % (span + 1))
}

/// Send a request with transient-failure retry + exponential backoff.
///
/// `build` is invoked once per attempt to produce a *fresh* [`RequestBuilder`]
/// (headers + body), so this works even though `RequestBuilder` is not reusable
/// after `send`. The returned [`Response`] has an **unread** body, so the caller
/// can stream it exactly as before — retries apply only to establishing the
/// response, never to re-reading a partial stream.
///
/// On the final attempt the response (or error) is returned as-is regardless of
/// whether it is retryable, so existing error-handling (reasoning fallback,
/// Responses-API migration, `HTTP {status}: {body}` surfacing) is preserved.
pub async fn send_with_retry<F>(
    config: &RetryConfig,
    provider: &str,
    build: F,
) -> reqwest::Result<Response>
where
    F: Fn() -> RequestBuilder,
{
    let max_attempts = config.max_attempts.max(1);
    let mut attempt: u32 = 0;

    loop {
        let result = build().send().await;
        let is_last = attempt + 1 >= max_attempts;

        match result {
            Ok(response) => {
                let status = response.status();
                if is_last || !is_retryable_status(status) {
                    return Ok(response);
                }

                // Prefer server-provided Retry-After (429s), else computed
                // backoff. Retry-After is honored beyond `max_delay` (server is
                // authoritative) but bounded by `RETRY_AFTER_CEILING`.
                let delay = parse_retry_after(&response)
                    .map(|d| d.min(RETRY_AFTER_CEILING))
                    .unwrap_or_else(|| backoff_delay(config, attempt));

                tracing::warn!(
                    "[{provider}] transient HTTP {} on attempt {}/{}; retrying in {}ms",
                    status.as_u16(),
                    attempt + 1,
                    max_attempts,
                    delay.as_millis()
                );

                // Drop the response (and its connection) before retrying.
                drop(response);
                tokio::time::sleep(delay).await;
            }
            Err(err) => {
                if is_last || !is_retryable_reqwest_error(&err) {
                    return Err(err);
                }

                let delay = backoff_delay(config, attempt);
                tracing::warn!(
                    "[{provider}] transient transport error on attempt {}/{} ({}); retrying in {}ms",
                    attempt + 1,
                    max_attempts,
                    err,
                    delay.as_millis()
                );
                tokio::time::sleep(delay).await;
            }
        }

        attempt += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

    /// Fast retry config so tests stay deterministic and quick.
    fn fast_config(max_attempts: u32) -> RetryConfig {
        RetryConfig {
            max_attempts,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(5),
        }
    }

    /// A responder that returns `first_status` for the first `fail_count`
    /// requests, then `200 OK`, counting how many times it was hit.
    struct FlakyResponder {
        fail_count: usize,
        first_status: u16,
        hits: Arc<AtomicUsize>,
    }

    impl Respond for FlakyResponder {
        fn respond(&self, _req: &wiremock::Request) -> ResponseTemplate {
            let n = self.hits.fetch_add(1, Ordering::SeqCst);
            if n < self.fail_count {
                ResponseTemplate::new(self.first_status)
            } else {
                ResponseTemplate::new(200).set_body_string("ok")
            }
        }
    }

    async fn run(config: &RetryConfig, server: &MockServer) -> reqwest::Result<reqwest::Response> {
        let client = reqwest::Client::new();
        let url = format!("{}/v1/test", server.uri());
        send_with_retry(config, "test", || {
            client.post(&url).json(&serde_json::json!({"a": 1}))
        })
        .await
    }

    #[tokio::test]
    async fn retries_503_then_succeeds() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(method("POST"))
            .respond_with(FlakyResponder {
                fail_count: 1,
                first_status: 503,
                hits: hits.clone(),
            })
            .mount(&server)
            .await;

        let resp = run(&fast_config(3), &server).await.unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(hits.load(Ordering::SeqCst), 2, "one 503 then one success");
        // The success response's body survives the retry wrapper intact (the
        // headline streaming-safety guarantee: retry inspects status only, never
        // consumes the body, and returns the final Response whole).
        assert_eq!(resp.text().await.unwrap(), "ok");
    }

    #[tokio::test]
    async fn retries_429_then_succeeds() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(method("POST"))
            .respond_with(FlakyResponder {
                fail_count: 2,
                first_status: 429,
                hits: hits.clone(),
            })
            .mount(&server)
            .await;

        let resp = run(&fast_config(3), &server).await.unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(hits.load(Ordering::SeqCst), 3, "two 429s then one success");
    }

    #[tokio::test]
    async fn does_not_retry_400() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(method("POST"))
            .respond_with(FlakyResponder {
                fail_count: 99,
                first_status: 400,
                hits: hits.clone(),
            })
            .mount(&server)
            .await;

        let resp = run(&fast_config(3), &server).await.unwrap();
        assert_eq!(resp.status(), 400);
        assert_eq!(hits.load(Ordering::SeqCst), 1, "400 is not retried");
    }

    #[tokio::test]
    async fn does_not_retry_401() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(method("POST"))
            .respond_with(FlakyResponder {
                fail_count: 99,
                first_status: 401,
                hits: hits.clone(),
            })
            .mount(&server)
            .await;

        let resp = run(&fast_config(3), &server).await.unwrap();
        assert_eq!(resp.status(), 401);
        assert_eq!(hits.load(Ordering::SeqCst), 1, "401 is not retried");
    }

    #[tokio::test]
    async fn bounded_gives_up_after_max_attempts() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(method("POST"))
            .respond_with(FlakyResponder {
                fail_count: 99,
                first_status: 503,
                hits: hits.clone(),
            })
            .mount(&server)
            .await;

        // 3 attempts total => 3 hits, last one still 503 and returned to caller.
        let resp = run(&fast_config(3), &server).await.unwrap();
        assert_eq!(resp.status(), 503);
        assert_eq!(hits.load(Ordering::SeqCst), 3, "exactly max_attempts hits");
    }

    #[tokio::test]
    async fn single_attempt_when_retries_disabled() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(method("POST"))
            .respond_with(FlakyResponder {
                fail_count: 99,
                first_status: 503,
                hits: hits.clone(),
            })
            .mount(&server)
            .await;

        let resp = run(&fast_config(1), &server).await.unwrap();
        assert_eq!(resp.status(), 503);
        assert_eq!(hits.load(Ordering::SeqCst), 1, "max_attempts=1 => no retry");
    }

    #[tokio::test]
    async fn respects_retry_after_header() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));

        // First response: 429 + Retry-After: 1 (second). Then 200.
        struct RetryAfterResponder {
            hits: Arc<AtomicUsize>,
        }
        impl Respond for RetryAfterResponder {
            fn respond(&self, _req: &wiremock::Request) -> ResponseTemplate {
                let n = self.hits.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    ResponseTemplate::new(429).insert_header("Retry-After", "1")
                } else {
                    ResponseTemplate::new(200).set_body_string("ok")
                }
            }
        }

        Mock::given(method("POST"))
            .respond_with(RetryAfterResponder { hits: hits.clone() })
            .mount(&server)
            .await;

        // base/max delay are tiny (1-5ms), so a >=1s wait proves Retry-After won.
        let started = std::time::Instant::now();
        let resp = run(&fast_config(3), &server).await.unwrap();
        let elapsed = started.elapsed();

        assert_eq!(resp.status(), 200);
        assert_eq!(hits.load(Ordering::SeqCst), 2);
        assert!(
            elapsed >= Duration::from_millis(900),
            "Retry-After: 1s should dominate the tiny backoff (elapsed={elapsed:?})"
        );
    }

    #[test]
    fn from_env_defaults_and_overrides() {
        // Default when unset.
        std::env::remove_var("BAMBOO_LLM_MAX_RETRIES");
        let cfg = RetryConfig::from_env();
        assert_eq!(cfg.max_attempts, DEFAULT_MAX_ATTEMPTS);
        assert_eq!(cfg.max_attempts, DEFAULT_MAX_RETRIES + 1);

        // retries -> attempts (initial + retries).
        std::env::set_var("BAMBOO_LLM_MAX_RETRIES", "5");
        let cfg = RetryConfig::from_env();
        assert_eq!(cfg.max_attempts, 6);

        // 0 retries => single attempt.
        std::env::set_var("BAMBOO_LLM_MAX_RETRIES", "0");
        let cfg = RetryConfig::from_env();
        assert_eq!(cfg.max_attempts, 1);

        std::env::remove_var("BAMBOO_LLM_MAX_RETRIES");
    }

    #[test]
    fn status_classification() {
        for s in [429u16, 500, 502, 503, 504] {
            assert!(
                is_retryable_status(StatusCode::from_u16(s).unwrap()),
                "{s} retryable"
            );
        }
        for s in [400u16, 401, 403, 404, 422, 200] {
            assert!(
                !is_retryable_status(StatusCode::from_u16(s).unwrap()),
                "{s} not retryable"
            );
        }
    }
}
