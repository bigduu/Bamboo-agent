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
//!
//! Anthropic additionally opts into the bounded policy below: only an explicit
//! connector database recovery diagnostic can make its 401 transient. That
//! policy shares this retry driver and never resets its budget in the agent loop.

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use reqwest::{RequestBuilder, Response, StatusCode};
use tokio::time::Instant;

use crate::provider::LLMError;

mod diagnostics;
mod policy;
pub use diagnostics::{
    public_request_diagnostic, recent_initial_request_diagnostics,
    record_stream_timeout_diagnostic, InitialRequestDiagnostic, RequestStopReason,
};
use diagnostics::{AttemptObservation, CancellationObservation};
use policy::continuous_request_retries;
pub use policy::{
    initial_request_limits, upstream_retry_defaults, with_upstream_retry, InitialRequestLimits,
    UpstreamRetry, UpstreamRetryDefaults,
};

/// Secret-free evidence from the most recent retryable HTTP response while
/// establishing one provider stream. A fresh observer is scoped to each
/// bootstrap request so concurrent sessions cannot exchange diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InitialHttpRetry {
    pub status: u16,
    pub delay: Duration,
}

#[derive(Debug, Default)]
pub struct InitialHttpRetryObserver {
    last: Mutex<Option<InitialHttpRetry>>,
    budget_managed: AtomicBool,
    active_request_policies: AtomicU32,
    terminal_diagnostic: Mutex<Option<bamboo_domain::LlmRequestDiagnostic>>,
}

impl InitialHttpRetryObserver {
    /// Whether the provider owns a bounded request policy for this bootstrap.
    pub fn request_budget_managed(&self) -> bool {
        self.budget_managed.load(Ordering::Relaxed)
    }

    /// Only the actual send/retry scope may extend the outer bootstrap wait.
    /// Error parsing or other provider work after that scope stays bounded.
    pub fn request_policy_active(&self) -> bool {
        self.active_request_policies.load(Ordering::Relaxed) > 0
    }

    /// Evidence captured by this exact provider bootstrap, independent of the
    /// bounded process log buffer and concurrent sessions.
    pub fn terminal_diagnostic(&self) -> Option<bamboo_domain::LlmRequestDiagnostic> {
        self.terminal_diagnostic.lock().ok().and_then(|d| d.clone())
    }

    pub fn last(&self) -> Option<InitialHttpRetry> {
        *self
            .last
            .lock()
            .expect("initial HTTP retry observer poisoned")
    }

    fn record(&self, status: StatusCode, delay: Duration) {
        *self
            .last
            .lock()
            .expect("initial HTTP retry observer poisoned") = Some(InitialHttpRetry {
            status: status.as_u16(),
            delay,
        });
    }
}

tokio::task_local! {
    static INITIAL_HTTP_RETRY_OBSERVER: Arc<InitialHttpRetryObserver>;
    static WORK_PHYSICAL_REQUEST: Arc<AtomicBool>;
}

struct ManagedRequestScope(Option<Arc<InitialHttpRetryObserver>>);

impl ManagedRequestScope {
    fn enter() -> Self {
        Self(
            INITIAL_HTTP_RETRY_OBSERVER
                .try_with(|observer| {
                    observer.budget_managed.store(true, Ordering::Relaxed);
                    observer
                        .active_request_policies
                        .fetch_add(1, Ordering::Relaxed);
                    observer.clone()
                })
                .ok(),
        )
    }
}

impl Drop for ManagedRequestScope {
    fn drop(&mut self) {
        if let Some(observer) = &self.0 {
            observer
                .active_request_policies
                .fetch_sub(1, Ordering::Relaxed);
        }
    }
}

/// One durable Work reservation permits one physical model request. The same
/// handle follows bootstrap and stream polls; fallbacks share its spent bit.
/// A retry must return through the Host for another durable reservation.
#[derive(Clone, Default)]
pub struct WorkPhysicalRequest(Arc<AtomicBool>);

impl WorkPhysicalRequest {
    pub async fn scope<F: Future>(&self, future: F) -> F::Output {
        WORK_PHYSICAL_REQUEST.scope(self.0.clone(), future).await
    }
}

pub(crate) fn work_request_budget_active() -> bool {
    WORK_PHYSICAL_REQUEST.try_with(|_| ()).is_ok()
}

pub(crate) fn claim_work_physical_request() -> Result<(), LLMError> {
    WORK_PHYSICAL_REQUEST.try_with(|spent| {
        spent.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .map(|_| ())
            .map_err(|_| LLMError::RequestFailed("Work token reservation permits one physical model send; a new Host reservation is required".into()))
    }).unwrap_or(Ok(()))
}

/// Establish one managed provider HTTP response, leaving its successful body
/// unread. The owner must retain cancellation around this future. Only this
/// managed entry opts into the execution's continuous policy; the legacy
/// public `send_with_retry` helper keeps its caller-owned bootstrap contract.
pub async fn send_provider_request<F>(
    config: &RetryConfig,
    provider: &str,
    build: F,
) -> Result<Response, LLMError>
where
    F: Fn() -> RequestBuilder,
{
    claim_work_physical_request()?;
    send_managed_request(config, provider, *initial_request_limits(), build).await
}

// The timeout owns only the headers future, never the successful SSE body.
// Anthropic separately shares one budget across its compatibility fallbacks.
async fn send_managed_request<F>(
    config: &RetryConfig,
    provider: &str,
    limits: InitialRequestLimits,
    build: F,
) -> Result<Response, LLMError>
where
    F: Fn() -> RequestBuilder,
{
    let continuous = continuous_request_retries();
    let deadline = (!continuous).then(|| Instant::now() + limits.total_budget);
    let _managed_request = ManagedRequestScope::enter();
    retry_initial_request(
        config,
        provider,
        continuous,
        || async {
            let started = Instant::now();
            if deadline.is_some_and(|deadline| started >= deadline) {
                return RetryAttempt::final_result(Err(LLMError::RequestFailed(
                    "initial upstream request time budget exhausted".into(),
                )));
            }
            let attempt_deadline = deadline
                .map(|deadline| deadline.min(started + limits.attempt_timeout))
                .unwrap_or(started + limits.attempt_timeout);
            match tokio::time::timeout_at(attempt_deadline, build().send()).await {
                Ok(Ok(response)) => RetryAttempt {
                    retry: is_retryable_status(response.status()),
                    status: Some(response.status()),
                    retry_after: parse_retry_after_with_date(&response),
                    deadline,
                    observation: None,
                    result: Ok(response),
                },
                Ok(Err(error)) => RetryAttempt {
                    retry: is_retryable_reqwest_error(&error),
                    status: None,
                    retry_after: None,
                    deadline,
                    observation: None,
                    result: Err(LLMError::Http(error)),
                },
                Err(_) => RetryAttempt {
                    retry: true,
                    status: None,
                    retry_after: None,
                    deadline,
                    observation: None,
                    result: Err(LLMError::RequestFailed(
                        "initial response headers timed out".into(),
                    )),
                },
            }
        },
        |_, _| {},
    )
    .await
}

pub async fn observe_initial_http_retries<F: Future>(
    observer: Arc<InitialHttpRetryObserver>,
    future: F,
) -> F::Output {
    INITIAL_HTTP_RETRY_OBSERVER.scope(observer, future).await
}

/// Default number of retries after the first attempt. Six total attempts keep
/// the interactive default finite while tolerating a longer gateway outage.
const DEFAULT_MAX_RETRIES: u32 = 5;
/// Default maximum number of *attempts* (1 initial + [`DEFAULT_MAX_RETRIES`]).
const DEFAULT_MAX_ATTEMPTS: u32 = DEFAULT_MAX_RETRIES + 1;
/// Default base delay for the first backoff step.
const DEFAULT_BASE_DELAY_MS: u64 = 500;
/// Default cap on any single *computed* backoff sleep.
const DEFAULT_MAX_DELAY_MS: u64 = 30_000;

/// Tunables for the transient-failure retry loop.
///
/// Values are read from environment variables via [`RetryConfig::from_env`]
/// with the documented defaults:
/// - `BAMBOO_LLM_MAX_RETRIES` — number of *retries* after the first attempt, so
///   total attempts = retries + 1 (default `5` retries → up to 6 attempts; `0`
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

/// Retry transient transport failures while establishing headers. Never infer
/// retries from the outer "error sending request" text: invalid URLs, headers
/// and serialization failures must remain terminal.
fn is_retryable_reqwest_error(err: &reqwest::Error) -> bool {
    use std::error::Error;
    if err.is_builder() || err.is_decode() {
        return false;
    }
    // Native TLS can erase the distinction between certificate validation
    // failures and network errors, while reqwest labels both as connect
    // failures. Stop when TLS hides the cause rather than retrying a permanent
    // trust or hostname failure forever. Ordinary typed network errors and
    // the separate headers timeout remain eligible for retry below.
    let mut tls_source = err.source();
    let mut opaque_connect_io = false;
    while let Some(cause) = tls_source {
        if cause.downcast_ref::<native_tls::Error>().is_some() {
            return false;
        }
        if let Some(io) = cause.downcast_ref::<std::io::Error>() {
            opaque_connect_io |= matches!(
                io.kind(),
                std::io::ErrorKind::Other
                    | std::io::ErrorKind::InvalidData
                    | std::io::ErrorKind::InvalidInput
                    | std::io::ErrorKind::PermissionDenied
            );
        }
        tls_source = cause.source();
    }
    if err.is_timeout() {
        return true;
    }
    if err.is_connect() {
        // Rustls certificate errors can arrive as opaque io::Error::Other.
        // Unknown or invalid connect errors are not evidence of a transient
        // network fault and must not cause an unbounded retry.
        return !opaque_connect_io;
    }
    let mut source = err.source();
    while let Some(cause) = source {
        if let Some(io) = cause.downcast_ref::<std::io::Error>() {
            if matches!(
                io.kind(),
                std::io::ErrorKind::ConnectionRefused
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::NotConnected
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::TimedOut
                    | std::io::ErrorKind::Interrupted
                    | std::io::ErrorKind::WouldBlock
            ) {
                return true;
            }
        }
        // Hyper can represent EOF-before-headers without an io::Error source.
        // Inspect this narrow cause only; it is never copied into public logs.
        let text = cause.to_string().to_ascii_lowercase();
        if [
            "connection closed before message completed",
            "connection reset",
            "connection aborted",
            "broken pipe",
            "unexpected eof",
        ]
        .iter()
        .any(|marker| text.contains(marker))
        {
            return true;
        }
        source = cause.source();
    }
    false
}

/// Parse delay-seconds without turning numeric overflow into a short backoff.
/// The date parser below handles the HTTP-date and legacy RFC2822 forms.
fn parse_retry_after(response: &Response) -> Option<Duration> {
    let value = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .to_string();
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    // A syntactically numeric value larger than u64 cannot be safely waited;
    // saturating preserves a stop decision instead of ignoring the server hint.
    Some(Duration::from_secs(
        value.parse::<u64>().unwrap_or(u64::MAX),
    ))
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
    // Legacy/custom callers keep their finite policy even when invoked from
    // a main execution that opted into managed continuous requests. The owner
    // still bounds headers, but must not reset this retry/hint policy by
    // replaying a whole turn. No active marker extends the outer watchdog.
    let _ = INITIAL_HTTP_RETRY_OBSERVER.try_with(|observer| {
        observer.budget_managed.store(true, Ordering::Relaxed);
    });
    let deadline = Instant::now() + initial_request_limits().total_budget;
    retry_initial_request(
        config,
        provider,
        false,
        || async {
            match build().send().await {
                Ok(response) => {
                    let status = response.status();
                    let retry_after = parse_retry_after_with_date(&response);
                    RetryAttempt {
                        retry: is_retryable_status(status),
                        status: Some(status),
                        retry_after,
                        deadline: Some(deadline),
                        observation: None,
                        result: Ok(response),
                    }
                }
                Err(error) => RetryAttempt {
                    retry: is_retryable_reqwest_error(&error),
                    status: None,
                    retry_after: None,
                    deadline: Some(deadline),
                    observation: None,
                    result: Err(error),
                },
            }
        },
        |_, _| {},
    )
    .await
}

struct RetryAttempt<T> {
    result: T,
    retry: bool,
    status: Option<StatusCode>,
    retry_after: Option<Duration>,
    deadline: Option<Instant>,
    observation: Option<AttemptObservation>,
}

impl<T> RetryAttempt<T> {
    fn final_result(result: T) -> Self {
        Self {
            result,
            retry: false,
            status: None,
            retry_after: None,
            deadline: None,
            observation: None,
        }
    }
}

#[derive(Clone, Copy)]
enum RetryDecision {
    Return,
    Retry(Duration),
    AttemptsExhausted,
    BudgetExhausted,
    DelayExceedsBudget,
    DelayTooLarge,
}

// One inline retry driver: legacy callers keep their unread Response contract;
// the Anthropic policy below supplies only its classification and shared budget.
async fn retry_initial_request<T, F, Fut, D>(
    config: &RetryConfig,
    provider: &str,
    allow_continuous: bool,
    attempt: F,
    decide: D,
) -> T
where
    F: Fn() -> Fut,
    Fut: Future<Output = RetryAttempt<T>>,
    D: Fn(&mut RetryAttempt<T>, RetryDecision),
{
    let max_attempts = if work_request_budget_active() {
        1
    } else {
        config.max_attempts.max(1)
    };
    let continuous = allow_continuous && !work_request_budget_active();
    let mut index: u32 = 0;
    loop {
        let mut outcome = attempt().await;
        let decision = if !outcome.retry {
            RetryDecision::Return
        } else if outcome
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            RetryDecision::BudgetExhausted
        } else if !continuous && index.saturating_add(1) >= max_attempts {
            RetryDecision::AttemptsExhausted
        } else {
            let backoff = if continuous {
                backoff_delay(config, index).max(Duration::from_millis(1))
            } else {
                backoff_delay(config, index)
            };
            let delay = outcome
                .retry_after
                .map(|hint| hint.max(backoff))
                .unwrap_or(backoff);
            if Instant::now().checked_add(delay).is_none() {
                RetryDecision::DelayTooLarge
            } else if outcome.deadline.is_some_and(|deadline| {
                Instant::now()
                    .checked_add(delay)
                    .is_none_or(|next| next >= deadline)
            }) {
                // Never shorten a server hint to squeeze another request into a budget.
                RetryDecision::DelayExceedsBudget
            } else {
                RetryDecision::Retry(delay)
            }
        };
        decide(&mut outcome, decision);
        let RetryDecision::Retry(delay) = decision else {
            return outcome.result;
        };
        if let Some(status) = outcome.status {
            let _ = INITIAL_HTTP_RETRY_OBSERVER.try_with(|observer| observer.record(status, delay));
            tracing::warn!(
                "[{provider}] transient HTTP {} on attempt {} (policy limit {:?}); retrying in {}ms",
                status.as_u16(),
                index.saturating_add(1),
                (!continuous).then_some(max_attempts),
                delay.as_millis()
            );
        } else {
            tracing::warn!(
                "[{provider}] transient transport error on attempt {} (policy limit {:?}); retrying in {}ms",
                index.saturating_add(1),
                (!continuous).then_some(max_attempts),
                delay.as_millis()
            );
        }
        drop(outcome);
        tokio::time::sleep(delay).await;
        index = index.saturating_add(1);
    }
}

const CONNECTOR_BODY_LIMIT: usize = 16 * 1024;

/// One Anthropic call, including reasoning/tool-choice compatibility fallbacks.
/// Atomic counting keeps borrowed policy futures Send without spawning work.
pub(crate) struct InitialRequestBudget {
    attempts: AtomicU32,
    max_attempts: u32,
    continuous: bool,
    started: Instant,
    deadline: Instant,
    attempt_timeout: Duration,
    diagnostic_id: uuid::Uuid,
    session_hash: Option<String>,
    latest: Mutex<Option<AttemptObservation>>,
}

impl InitialRequestBudget {
    pub(crate) fn new(config: &RetryConfig) -> Self {
        let started = Instant::now();
        Self {
            attempts: AtomicU32::new(0),
            max_attempts: if work_request_budget_active() {
                1
            } else {
                config.max_attempts.max(1)
            },
            continuous: continuous_request_retries(),
            started,
            deadline: started + initial_request_limits().total_budget,
            attempt_timeout: initial_request_limits().attempt_timeout,
            diagnostic_id: uuid::Uuid::new_v4(),
            session_hash: None,
            latest: Mutex::new(None),
        }
    }

    fn failure(
        &self,
        observation: AttemptObservation,
        stop: Option<RequestStopReason>,
        retry_after: Option<Duration>,
    ) -> RetryAttempt<Result<InitialResponse, LLMError>> {
        let mut observation = observation;
        let stop = stop.or_else(|| {
            if !self.continuous && Instant::now() >= self.deadline {
                Some(RequestStopReason::TimeBudgetExhausted)
            } else if !self.continuous && self.attempts.load(Ordering::Relaxed) >= self.max_attempts
            {
                Some(RequestStopReason::AttemptsExhausted)
            } else {
                None
            }
        });
        observation.diagnostic.stop_reason = stop;
        // The shared driver resolves the actual stopping condition before this
        // placeholder can escape; scheduled retries never report exhaustion.
        RetryAttempt {
            result: Err(LLMError::RequestFailed(String::new())),
            retry: stop.is_none(),
            status: observation
                .diagnostic
                .status
                .and_then(|s| StatusCode::from_u16(s).ok()),
            retry_after,
            deadline: (!self.continuous).then_some(self.deadline),
            observation: Some(observation),
        }
    }
}

/// Non-transient error bodies are bounded for auth classification and existing
/// compatibility fallbacks. Successful SSE bodies are never read by this policy.
#[derive(Debug)]
pub(crate) struct InitialResponse {
    response: Response,
    error_body: Option<String>,
}

impl InitialResponse {
    pub(crate) fn status(&self) -> StatusCode {
        self.response.status()
    }

    pub(crate) async fn text(self) -> reqwest::Result<String> {
        match self.error_body {
            Some(body) => Ok(body),
            None => self.response.text().await,
        }
    }

    pub(crate) fn into_response(self) -> Response {
        self.response
    }
}

/// Fail closed: this is the reported connector recovery diagnostic, not a
/// general auth-error or database-error allowlist. All 403s stay terminal.
pub(crate) fn is_connector_recovery(status: StatusCode, body: &str) -> bool {
    if status != StatusCode::UNAUTHORIZED || body.len() > CONNECTOR_BODY_LIMIT {
        return false;
    }
    let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
        return false;
    };
    let error = value.get("error").unwrap_or(&value);
    // A conflicting gateway/credential code outranks a recovery-looking message.
    // Unknown codes fail closed until an observed contract establishes them.
    if [&value, error]
        .iter()
        .any(|value| value.get("code").is_some_and(|code| !code.is_null()))
    {
        return false;
    }
    if !matches!(
        error.get("type").and_then(|v| v.as_str()),
        Some("auth_error" | "authentication_error")
    ) {
        return false;
    }
    let Some(message) = error.get("message").and_then(|v| v.as_str()) else {
        return false;
    };
    let normalized = message
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    let diagnostic = normalized
        .strip_prefix("authentication error, ")
        .unwrap_or(&normalized);
    diagnostic == "error in connector: error querying the database: fatal: the database system is not yet accepting connections detail: consistent recovery state has not been yet reached."
}

async fn read_connector_body(response: &mut Response) -> reqwest::Result<String> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if body.len().saturating_add(chunk.len()) > CONNECTOR_BODY_LIMIT {
            return Ok("[upstream error response exceeded the diagnostic size limit]".to_string());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(String::from_utf8_lossy(&body).into_owned())
}

fn parse_retry_after_with_date(response: &Response) -> Option<Duration> {
    parse_retry_after(response).or_else(|| {
        let value = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)?
            .to_str()
            .ok()?;
        httpdate::parse_http_date(value)
            .map(|date| {
                date.duration_since(std::time::SystemTime::now())
                    .unwrap_or_default()
            })
            // Preserve the previously accepted numeric-zone RFC2822 form.
            .or_else(|_| {
                chrono::DateTime::parse_from_rfc2822(value).map(|date| {
                    (date.with_timezone(&chrono::Utc) - chrono::Utc::now())
                        .to_std()
                        .unwrap_or_default()
                })
            })
            .ok()
    })
}

pub(crate) async fn send_anthropic_with_retry<F>(
    config: &RetryConfig,
    budget: &InitialRequestBudget,
    build: F,
) -> Result<InitialResponse, LLMError>
where
    F: Fn() -> RequestBuilder,
{
    claim_work_physical_request()?;
    let _managed_request = ManagedRequestScope::enter();
    let mut cancellation = CancellationObservation {
        budget,
        completed: false,
    };
    let result = retry_initial_request(
        config,
        "Anthropic",
        budget.continuous,
        || async {
            let attempt_started = Instant::now();
            if !budget.continuous && attempt_started >= budget.deadline {
                return budget.failure(
                    budget.observation(
                        attempt_started,
                        "send_headers",
                        "request deadline exceeded",
                        None,
                    ),
                    Some(RequestStopReason::TimeBudgetExhausted),
                    None,
                );
            }
            if budget
                .attempts
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |attempts| {
                    (budget.continuous || attempts < budget.max_attempts)
                        .then_some(attempts.saturating_add(1))
                })
                .is_err()
            {
                return budget.failure(
                    budget.observation(
                        attempt_started,
                        "send_headers",
                        "request attempt limit reached",
                        None,
                    ),
                    Some(RequestStopReason::AttemptsExhausted),
                    None,
                );
            }
            budget.record(budget.observation(attempt_started, "send_headers", "none", None));
            let attempt_deadline = if budget.continuous {
                attempt_started + budget.attempt_timeout
            } else {
                budget
                    .deadline
                    .min(attempt_started + budget.attempt_timeout)
            };
            let sent = tokio::time::timeout_at(attempt_deadline, build().send()).await;
            let mut response = match sent {
                Err(_) => {
                    return budget.failure(
                        budget.observation(
                            attempt_started,
                            "send_headers",
                            "initial response headers timed out",
                            None,
                        ),
                        None,
                        None,
                    );
                }
                Ok(Err(error)) => {
                    let retry = is_retryable_reqwest_error(&error);
                    let kind = crate::providers::common::http_error_kind(&error);
                    let mut outcome = budget.failure(
                        budget.observation(attempt_started, "send_headers", kind, None),
                        (!retry).then_some(RequestStopReason::NonRetryableTransport),
                        None,
                    );
                    if !retry {
                        outcome.result = Err(LLMError::Http(error));
                    }
                    return outcome;
                }
                Ok(Ok(response)) => response,
            };
            let status = response.status();
            let retry_after = parse_retry_after_with_date(&response);
            let error_body = if !status.is_success() && !is_retryable_status(status) {
                let mut observation =
                    budget.observation(attempt_started, "error_body", "none", Some(status));
                observation.diagnostic.event = "classify_error_body";
                budget.record(observation);
                match tokio::time::timeout_at(
                    if budget.continuous {
                        attempt_deadline
                    } else {
                        budget.deadline
                    },
                    read_connector_body(&mut response),
                )
                .await
                {
                    Ok(Ok(body)) => Some(body),
                    result => {
                        let (failure, stop) = if result.is_err() {
                            (
                                "upstream error body deadline exceeded",
                                if budget.continuous {
                                    RequestStopReason::UnclassifiedErrorBody
                                } else {
                                    RequestStopReason::TimeBudgetExhausted
                                },
                            )
                        } else {
                            (
                                "upstream error body read failed",
                                RequestStopReason::UnclassifiedErrorBody,
                            )
                        };
                        return budget.failure(
                            budget.observation(
                                attempt_started,
                                "error_body",
                                failure,
                                Some(status),
                            ),
                            Some(stop),
                            None,
                        );
                    }
                }
            } else {
                None
            };
            let connector_recovery = error_body
                .as_deref()
                .is_some_and(|body| is_connector_recovery(status, body));
            if connector_recovery || is_retryable_status(status) {
                return budget.failure(
                    budget.observation(
                        attempt_started,
                        "response_status",
                        if connector_recovery {
                            "connector database is temporarily recovering"
                        } else {
                            "temporary upstream failure"
                        },
                        Some(status),
                    ),
                    // Fallback invocations share this absolute attempt count.
                    (!budget.continuous
                        && budget.attempts.load(Ordering::Relaxed) >= budget.max_attempts)
                        .then_some(RequestStopReason::AttemptsExhausted),
                    retry_after,
                );
            }
            let mut observation =
                budget.observation(attempt_started, "response_status", "none", Some(status));
            observation.diagnostic.event = "response_returned";
            observation.diagnostic.upstream_outcome = "response_received";
            let mut outcome = RetryAttempt::final_result(Ok(InitialResponse {
                response,
                error_body,
            }));
            outcome.observation = Some(observation);
            outcome
        },
        |outcome, decision| budget.decide(outcome, decision),
    )
    .await;
    cancellation.completed = true;
    result
}

#[cfg(test)]
#[path = "retry/continuous_tests.rs"]
mod continuous_tests;

#[cfg(test)]
#[path = "retry/connector_tests.rs"]
mod connector_tests;

#[cfg(test)]
#[path = "retry/tls_terminal_tests.rs"]
mod tls_terminal_tests;

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
    async fn work_budget_permits_one_physical_send_across_retry_and_fallback() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let client = reqwest::Client::new();
        let config = fast_config(4);
        let scope = WorkPhysicalRequest::default();
        let response = scope
            .scope(send_provider_request(&config, "local-budget", || {
                client.post(server.uri())
            }))
            .await
            .unwrap();
        assert_eq!(response.status(), 503);
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
        let retry = scope
            .scope(send_provider_request(
                &config,
                "local-budget-fallback",
                || client.post(server.uri()),
            ))
            .await;
        assert!(matches!(retry, Err(LLMError::RequestFailed(_))));
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn work_budget_unknown_headers_timeout_never_resends() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(200)))
            .mount(&server)
            .await;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(30))
            .build()
            .unwrap();
        let scope = WorkPhysicalRequest::default();
        let response = scope
            .scope(send_provider_request(
                &fast_config(4),
                "local-budget-timeout",
                || client.post(server.uri()),
            ))
            .await;
        assert!(matches!(response, Err(LLMError::Http(error)) if error.is_timeout()));
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
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
    async fn initial_retry_observer_records_only_status_and_delay() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(method("POST"))
            .respond_with(FlakyResponder {
                fail_count: 1,
                first_status: 429,
                hits,
            })
            .mount(&server)
            .await;
        let observer = Arc::new(InitialHttpRetryObserver::default());

        let response =
            observe_initial_http_retries(observer.clone(), run(&fast_config(2), &server))
                .await
                .unwrap();
        assert_eq!(response.status(), 200);
        let retry = observer.last().expect("429 must be observed before retry");
        assert_eq!(retry.status, 429);
        assert!(retry.delay <= Duration::from_millis(5));
    }

    #[tokio::test]
    async fn concurrent_initial_retry_observers_are_isolated() {
        let first = Arc::new(InitialHttpRetryObserver::default());
        let second = Arc::new(InitialHttpRetryObserver::default());
        let ((), ()) = tokio::join!(
            observe_initial_http_retries(first.clone(), async {
                INITIAL_HTTP_RETRY_OBSERVER.with(|observer| {
                    observer.record(StatusCode::TOO_MANY_REQUESTS, Duration::from_secs(60))
                });
                tokio::task::yield_now().await;
                assert_eq!(
                    INITIAL_HTTP_RETRY_OBSERVER
                        .with(|observer| observer.last())
                        .unwrap()
                        .status,
                    429
                );
            }),
            observe_initial_http_retries(second.clone(), async {
                INITIAL_HTTP_RETRY_OBSERVER.with(|observer| {
                    observer.record(StatusCode::SERVICE_UNAVAILABLE, Duration::from_secs(1))
                });
                tokio::task::yield_now().await;
                assert_eq!(
                    INITIAL_HTTP_RETRY_OBSERVER
                        .with(|observer| observer.last())
                        .unwrap()
                        .status,
                    503
                );
            }),
        );
        assert_eq!(first.last().unwrap().status, 429);
        assert_eq!(second.last().unwrap().status, 503);
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
