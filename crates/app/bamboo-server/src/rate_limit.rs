//! Thin actix-web rate-limiting middleware over the MIT/Apache-2.0-licensed
//! `governor` crate.
//!
//! Replaces `actix-governor` (GPL-3.0-or-later — license-incompatible with
//! this MIT-licensed crate, #504) with an in-repo `Transform`/`Service` pair
//! that reproduces exactly the slice of actix-governor 0.10's behavior
//! `config.rs` relied on:
//!
//! - per-key token-bucket throttling, backed by `governor`'s keyed rate
//!   limiter (same GCRA algorithm, same `Quota::with_period(..).allow_burst(..)`
//!   construction actix-governor's `GovernorConfigBuilder::finish()` used);
//! - request-driven, single-flight eviction of idle keyed state via governor's
//!   `retain_recent`, with no per-request or per-worker background tasks;
//! - on throttle: `429 Too Many Requests` with `retry-after` and
//!   `x-ratelimit-after` headers (seconds until the bucket admits again) and
//!   Bamboo's canonical nested JSON error envelope;
//! - on key-extraction failure: whatever response the extractor's error
//!   produces (bamboo's `ClientIpKeyExtractor` returns `500` with the same
//!   canonical nested JSON error envelope).
//!
//! actix-governor features bamboo never configured — per-method filtering,
//! `permissive` mode, whitelisted keys, and the `x-ratelimit-limit` /
//! `-remaining` / `-whitelisted` headers only added by its
//! `StateInformationMiddleware` — are intentionally NOT reproduced; adding
//! them back is a small extension to [`KeyExtractor`]/[`RateLimitMiddleware`]
//! if ever needed, not a rewrite.

use std::future::{ready, Ready};
use std::hash::Hash;
use std::num::{NonZeroU32, NonZeroU64};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use actix_web::body::{EitherBody, MessageBody};
use actix_web::dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::http::StatusCode;
use actix_web::{Error, HttpResponse, ResponseError};
use futures::future::LocalBoxFuture;
use governor::clock::{Clock, DefaultClock};
use governor::middleware::NoOpMiddleware;
use governor::state::keyed::DefaultKeyedStateStore;
use governor::{Quota, RateLimiter as GovernorRateLimiter};

/// Extracts the rate-limiting key from an incoming request.
///
/// Mirrors the one piece of `actix_governor::KeyExtractor`'s shape bamboo
/// actually used, so `config::ClientIpKeyExtractor` ports over with no
/// change to its extraction logic — only its error type moves to
/// [`SimpleKeyExtractionError`] below.
pub trait KeyExtractor: Clone + 'static {
    /// The extracted key type — must be usable as a `governor` keyed-limiter key.
    type Key: Clone + Eq + std::hash::Hash + Send + Sync + 'static;
    /// The error returned when extraction fails; converts to the response a
    /// caller sees (via `actix_web::Error`'s blanket `From<ResponseError>`).
    type KeyExtractionError: ResponseError + 'static;

    /// Extract the key, or fail with a response-producing error.
    fn extract(&self, req: &ServiceRequest) -> Result<Self::Key, Self::KeyExtractionError>;
}

/// A minimal extraction-failure error with Bamboo's canonical JSON body.
#[derive(Debug, Clone, Copy)]
pub struct SimpleKeyExtractionError(pub &'static str);

impl SimpleKeyExtractionError {
    pub const fn new(body: &'static str) -> Self {
        Self(body)
    }
}

impl std::fmt::Display for SimpleKeyExtractionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SimpleKeyExtractionError")
    }
}

impl std::error::Error for SimpleKeyExtractionError {}

impl ResponseError for SimpleKeyExtractionError {
    fn status_code(&self) -> StatusCode {
        StatusCode::INTERNAL_SERVER_ERROR
    }

    fn error_response(&self) -> HttpResponse {
        crate::error::json_error(self.status_code(), self.0)
    }
}

type KeyedLimiter<Key, C = DefaultClock> =
    GovernorRateLimiter<Key, DefaultKeyedStateStore<Key>, C, NoOpMiddleware<<C as Clock>::Instant>>;

/// Bound the number of newly observed request keys that can accumulate between
/// governor state-store sweeps. Cleanup is request-driven: an idle server does
/// no work, and traffic cannot create one background task per request/worker.
const KEYED_STATE_CLEANUP_EVERY_REQUESTS: NonZeroU64 =
    NonZeroU64::new(4_096).expect("cleanup interval is non-zero");

struct KeyCleanupSchedule {
    every_requests: NonZeroU64,
    requests_since_cleanup: AtomicU64,
    cleanup_in_progress: AtomicBool,
    #[cfg(test)]
    completed_cleanups: AtomicU64,
}

impl KeyCleanupSchedule {
    fn new(every_requests: NonZeroU64) -> Self {
        Self {
            every_requests,
            requests_since_cleanup: AtomicU64::new(0),
            cleanup_in_progress: AtomicBool::new(false),
            #[cfg(test)]
            completed_cleanups: AtomicU64::new(0),
        }
    }

    fn add_pending_requests(&self, additional: u64) -> u64 {
        self.requests_since_cleanup
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |pending| {
                Some(pending.saturating_add(additional))
            })
            .expect("saturating counter updates never fail")
            .saturating_add(additional)
    }

    fn after_request(&self, cleanup: impl FnOnce()) {
        let pending = self.add_pending_requests(1);
        if pending < self.every_requests.get()
            || self
                .cleanup_in_progress
                .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
        {
            return;
        }

        let _cleanup_guard = CleanupInProgressGuard(&self.cleanup_in_progress);
        let pending = self.requests_since_cleanup.swap(0, Ordering::AcqRel);
        if pending < self.every_requests.get() {
            // A concurrent sweep may have reset the counter after this request
            // observed it as due. Preserve requests recorded since that sweep.
            self.add_pending_requests(pending);
            return;
        }

        cleanup();
        #[cfg(test)]
        self.completed_cleanups.fetch_add(1, Ordering::Relaxed);
    }
}

struct CleanupInProgressGuard<'a>(&'a AtomicBool);

impl Drop for CleanupInProgressGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Shared keyed governor plus bounded, single-flight state maintenance.
///
/// `governor::retain_recent` only evicts a key once its GCRA state is
/// indistinguishable from a fresh bucket, so a recent or still-throttled key is
/// never reset by cleanup. The scan runs synchronously on at most one request
/// after each request-count interval; it does not spawn or retain a task.
struct ManagedKeyedLimiter<Key, C = DefaultClock>
where
    Key: Clone + Eq + Hash,
    C: Clock,
{
    limiter: KeyedLimiter<Key, C>,
    cleanup: KeyCleanupSchedule,
}

impl<Key, C> ManagedKeyedLimiter<Key, C>
where
    Key: Clone + Eq + Hash,
    C: Clock,
{
    fn new(limiter: KeyedLimiter<Key, C>, cleanup_every: NonZeroU64) -> Self {
        Self {
            limiter,
            cleanup: KeyCleanupSchedule::new(cleanup_every),
        }
    }

    fn check_key(&self, key: &Key) -> Result<(), governor::NotUntil<C::Instant>> {
        let decision = self.limiter.check_key(key);
        self.cleanup.after_request(|| {
            let before = self.limiter.len();
            self.limiter.retain_recent();
            if self.limiter.len() < before {
                self.limiter.shrink_to_fit();
            }
        });
        decision
    }

    fn now(&self) -> C::Instant {
        self.limiter.clock().now()
    }

    #[cfg(test)]
    fn key_count(&self) -> usize {
        self.limiter.len()
    }

    #[cfg(test)]
    fn requests_since_cleanup(&self) -> u64 {
        self.cleanup.requests_since_cleanup.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    fn completed_cleanup_count(&self) -> u64 {
        self.cleanup.completed_cleanups.load(Ordering::Relaxed)
    }
}

/// Rate-limiter configuration: a shared quota + key extractor, cheap to
/// clone (the limiter is behind an `Arc`) so the SAME bucket state is reused
/// across every `App` factory invocation (actix spins up one `App` per
/// worker thread) — mirrors `actix_governor::GovernorConfig`.
pub struct RateLimiterConfig<K: KeyExtractor> {
    limiter: Arc<ManagedKeyedLimiter<K::Key>>,
    key_extractor: K,
}

impl<K: KeyExtractor> Clone for RateLimiterConfig<K> {
    fn clone(&self) -> Self {
        Self {
            limiter: self.limiter.clone(),
            key_extractor: self.key_extractor.clone(),
        }
    }
}

impl<K: KeyExtractor> RateLimiterConfig<K> {
    /// Build a config allowing bursts up to `burst_size`, replenishing one
    /// quota element every `period`. Mirrors
    /// `GovernorConfigBuilder::{milliseconds_per_request,burst_size}.finish()`
    /// (same `Quota::with_period(..).allow_burst(..)` construction).
    ///
    /// Panics if `period` is zero or `burst_size` is zero, same as
    /// `finish()` returning `None` did — callers clamp beforehand (see
    /// `config::rate_limiter_config`).
    pub fn new(period: Duration, burst_size: u32, key_extractor: K) -> Self {
        let burst = NonZeroU32::new(burst_size).expect("burst_size must be non-zero");
        let quota = Quota::with_period(period)
            .expect("period must be non-zero")
            .allow_burst(burst);
        Self {
            limiter: Arc::new(ManagedKeyedLimiter::new(
                GovernorRateLimiter::keyed(quota),
                KEYED_STATE_CLEANUP_EVERY_REQUESTS,
            )),
            key_extractor,
        }
    }
}

/// Rate-limiting middleware factory — mirrors `actix_governor::Governor`.
pub struct RateLimit<K: KeyExtractor> {
    limiter: Arc<ManagedKeyedLimiter<K::Key>>,
    key_extractor: K,
}

impl<K: KeyExtractor> RateLimit<K> {
    /// Create a new middleware factory from a shared [`RateLimiterConfig`].
    pub fn new(config: &RateLimiterConfig<K>) -> Self {
        Self {
            limiter: config.limiter.clone(),
            key_extractor: config.key_extractor.clone(),
        }
    }
}

impl<S, B, K> Transform<S, ServiceRequest> for RateLimit<K>
where
    K: KeyExtractor,
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    B: MessageBody + 'static,
{
    type Response = ServiceResponse<EitherBody<B>>;
    type Error = Error;
    type Transform = RateLimitMiddleware<S, K>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(RateLimitMiddleware {
            service: Rc::new(service),
            limiter: self.limiter.clone(),
            key_extractor: self.key_extractor.clone(),
        }))
    }
}

pub struct RateLimitMiddleware<S, K: KeyExtractor> {
    service: Rc<S>,
    limiter: Arc<ManagedKeyedLimiter<K::Key>>,
    key_extractor: K,
}

impl<S, B, K> Service<ServiceRequest> for RateLimitMiddleware<S, K>
where
    K: KeyExtractor,
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    B: MessageBody + 'static,
{
    type Response = ServiceResponse<EitherBody<B>>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let key = match self.key_extractor.extract(&req) {
            Ok(key) => key,
            Err(err) => {
                // Matches actix-governor's non-permissive extraction-failure
                // path: fail the request with the extractor's own error
                // response; the inner service is never reached.
                return Box::pin(async move { Err(err.into()) });
            }
        };

        match self.limiter.check_key(&key) {
            Ok(_) => {
                let fut = self.service.call(req);
                Box::pin(async move { fut.await.map(|res| res.map_into_left_body()) })
            }
            Err(negative) => {
                let wait_time = negative.wait_time_from(self.limiter.now()).as_secs();
                let mut response = crate::error::json_error(
                    StatusCode::TOO_MANY_REQUESTS,
                    format!("Too many requests, retry in {wait_time}s"),
                );
                response.headers_mut().insert(
                    actix_web::http::header::RETRY_AFTER,
                    actix_web::http::header::HeaderValue::from(wait_time),
                );
                response.headers_mut().insert(
                    actix_web::http::header::HeaderName::from_static("x-ratelimit-after"),
                    actix_web::http::header::HeaderValue::from(wait_time),
                );
                let response = req.into_response(response).map_into_right_body();
                Box::pin(async move { Ok(response) })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::http::StatusCode as HttpStatusCode;
    use actix_web::{test, web, App, HttpResponse as Resp};
    use governor::clock::FakeRelativeClock;
    use std::net::{IpAddr, Ipv4Addr};
    use std::num::NonZeroU64;
    use std::sync::atomic::AtomicUsize;
    use std::sync::mpsc;
    use std::thread;

    /// A trivial always-succeeds key extractor keyed on a fixed IP, used to
    /// unit-test the burst/refill behavior of [`RateLimitMiddleware`]
    /// directly (independent of `config::ClientIpKeyExtractor`, which has
    /// its own dedicated tests in `config.rs`).
    #[derive(Clone)]
    struct FixedKeyExtractor(IpAddr);

    impl KeyExtractor for FixedKeyExtractor {
        type Key = IpAddr;
        type KeyExtractionError = SimpleKeyExtractionError;

        fn extract(&self, _req: &ServiceRequest) -> Result<Self::Key, Self::KeyExtractionError> {
            Ok(self.0)
        }
    }

    #[actix_web::test]
    async fn request_driven_cleanup_reclaims_stale_keys_and_keeps_recent_keys() {
        let clock = FakeRelativeClock::default();
        let quota = Quota::with_period(Duration::from_millis(100))
            .expect("non-zero period")
            .allow_burst(NonZeroU32::new(1).expect("non-zero burst"));
        let limiter: KeyedLimiter<IpAddr, FakeRelativeClock> = GovernorRateLimiter::new(
            quota,
            DefaultKeyedStateStore::<IpAddr>::default(),
            clock.clone(),
        );
        let limiter = ManagedKeyedLimiter::new(
            limiter,
            NonZeroU64::new(3).expect("non-zero cleanup interval"),
        );
        let stale = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let recent = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
        let trigger = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3));

        assert!(limiter.check_key(&stale).is_ok());
        clock.advance(Duration::from_millis(300));
        assert!(limiter.check_key(&recent).is_ok());
        assert_eq!(limiter.key_count(), 2);
        assert_eq!(limiter.completed_cleanup_count(), 0);

        assert!(limiter.check_key(&trigger).is_ok());
        assert_eq!(limiter.completed_cleanup_count(), 1);
        assert_eq!(
            limiter.key_count(),
            2,
            "the stale key is evicted while the recent and triggering keys remain"
        );
        assert!(
            limiter.check_key(&recent).is_err(),
            "a recent key must retain its exhausted bucket instead of becoming fresh"
        );
    }

    #[actix_web::test]
    async fn cleanup_frequency_is_request_bounded_and_shared_by_config_clones() {
        let key = IpAddr::V4(Ipv4Addr::new(10, 0, 1, 1));
        let config = RateLimiterConfig::new(Duration::from_secs(60), 1, FixedKeyExtractor(key));
        let cloned = config.clone();
        let middleware = RateLimit::new(&cloned);

        assert!(Arc::ptr_eq(&config.limiter, &cloned.limiter));
        assert!(Arc::ptr_eq(&config.limiter, &middleware.limiter));
        assert!(config.limiter.check_key(&key).is_ok());
        assert!(
            cloned.limiter.check_key(&key).is_err(),
            "clones must share the same per-key bucket"
        );
        assert_eq!(middleware.limiter.key_count(), 1);
        assert_eq!(
            middleware.limiter.requests_since_cleanup(),
            2,
            "the cleanup request counter must be shared with the limiter state"
        );
        assert_eq!(middleware.limiter.completed_cleanup_count(), 0);

        let clock = FakeRelativeClock::default();
        let quota = Quota::with_period(Duration::from_millis(100))
            .expect("non-zero period")
            .allow_burst(NonZeroU32::new(1).expect("non-zero burst"));
        let limiter: KeyedLimiter<u32, FakeRelativeClock> =
            GovernorRateLimiter::new(quota, DefaultKeyedStateStore::<u32>::default(), clock);
        let limiter = ManagedKeyedLimiter::new(
            limiter,
            NonZeroU64::new(3).expect("non-zero cleanup interval"),
        );

        for key in 1..=2 {
            assert!(limiter.check_key(&key).is_ok());
        }
        assert_eq!(limiter.completed_cleanup_count(), 0);
        assert!(limiter.check_key(&3).is_ok());
        assert_eq!(limiter.completed_cleanup_count(), 1);
        for key in 4..=5 {
            assert!(limiter.check_key(&key).is_ok());
        }
        assert_eq!(limiter.completed_cleanup_count(), 1);
        assert!(limiter.check_key(&6).is_ok());
        assert_eq!(limiter.completed_cleanup_count(), 2);

        limiter
            .cleanup
            .requests_since_cleanup
            .store(u64::MAX, Ordering::Relaxed);
        assert!(limiter.check_key(&7).is_ok());
        assert_eq!(limiter.completed_cleanup_count(), 3);
        assert_eq!(
            limiter.requests_since_cleanup(),
            0,
            "the long-lived request counter must saturate and clean up instead of wrapping"
        );
    }

    #[actix_web::test]
    async fn high_cardinality_cleanup_keeps_only_the_recent_request_window() {
        const CLEANUP_INTERVAL: u64 = 8;

        let clock = FakeRelativeClock::default();
        let quota = Quota::with_period(Duration::from_millis(100))
            .expect("non-zero period")
            .allow_burst(NonZeroU32::new(1).expect("non-zero burst"));
        let limiter: KeyedLimiter<u32, FakeRelativeClock> = GovernorRateLimiter::new(
            quota,
            DefaultKeyedStateStore::<u32>::default(),
            clock.clone(),
        );
        let limiter = ManagedKeyedLimiter::new(
            limiter,
            NonZeroU64::new(CLEANUP_INTERVAL).expect("non-zero cleanup interval"),
        );

        for round in 0..3 {
            clock.advance(Duration::from_millis(300));
            let first_key = round * CLEANUP_INTERVAL;
            for key in first_key..first_key + CLEANUP_INTERVAL {
                assert!(limiter.check_key(&(key as u32)).is_ok());
            }

            assert_eq!(limiter.completed_cleanup_count(), round + 1);
            assert_eq!(
                limiter.key_count(),
                CLEANUP_INTERVAL as usize,
                "each sweep must reclaim the previous stale high-cardinality window"
            );
        }
    }

    #[actix_web::test]
    async fn cleanup_is_single_flight_under_concurrent_requests() {
        const CONCURRENT_REQUESTS: usize = 8;

        let schedule = Arc::new(KeyCleanupSchedule::new(
            NonZeroU64::new(1).expect("non-zero cleanup interval"),
        ));
        let cleanup_calls = Arc::new(AtomicUsize::new(0));
        let (entered_tx, entered_rx) = mpsc::sync_channel(0);
        let (release_tx, release_rx) = mpsc::sync_channel(0);

        let first_cleanup = thread::spawn({
            let schedule = Arc::clone(&schedule);
            let cleanup_calls = Arc::clone(&cleanup_calls);
            move || {
                schedule.after_request(|| {
                    cleanup_calls.fetch_add(1, Ordering::Relaxed);
                    entered_tx.send(()).expect("test receiver remains alive");
                    release_rx.recv().expect("test sender remains alive");
                });
            }
        });
        entered_rx.recv().expect("cleanup must start");

        let concurrent_requests = (0..CONCURRENT_REQUESTS)
            .map(|_| {
                let schedule = Arc::clone(&schedule);
                let cleanup_calls = Arc::clone(&cleanup_calls);
                thread::spawn(move || {
                    schedule.after_request(|| {
                        cleanup_calls.fetch_add(1, Ordering::Relaxed);
                    });
                })
            })
            .collect::<Vec<_>>();
        for request in concurrent_requests {
            request.join().expect("concurrent request must finish");
        }

        assert_eq!(cleanup_calls.load(Ordering::Relaxed), 1);
        assert_eq!(schedule.completed_cleanups.load(Ordering::Relaxed), 0);
        assert_eq!(
            schedule.requests_since_cleanup.load(Ordering::Relaxed),
            CONCURRENT_REQUESTS as u64,
            "requests arriving during cleanup must be retained for the next sweep"
        );

        release_tx.send(()).expect("cleanup thread remains alive");
        first_cleanup.join().expect("cleanup must finish");
        assert_eq!(schedule.completed_cleanups.load(Ordering::Relaxed), 1);

        schedule.after_request(|| {
            cleanup_calls.fetch_add(1, Ordering::Relaxed);
        });
        assert_eq!(cleanup_calls.load(Ordering::Relaxed), 2);
        assert_eq!(schedule.completed_cleanups.load(Ordering::Relaxed), 2);
        assert_eq!(schedule.requests_since_cleanup.load(Ordering::Relaxed), 0);
    }

    #[actix_web::test]
    async fn burst_then_429_with_retry_after_header() {
        let key = IpAddr::V4(Ipv4Addr::new(10, 1, 1, 1));
        let conf = RateLimiterConfig::new(Duration::from_millis(100), 3, FixedKeyExtractor(key));
        let app = test::init_service(
            App::new()
                .wrap(RateLimit::new(&conf))
                .route("/", web::get().to(|| async { Resp::Ok().finish() })),
        )
        .await;

        // The first 3 requests (burst_size) pass.
        for _ in 0..3 {
            let res =
                test::call_service(&app, test::TestRequest::get().uri("/").to_request()).await;
            assert_eq!(res.status(), HttpStatusCode::OK);
        }

        // The 4th is throttled, with a Retry-After header.
        let res = test::call_service(&app, test::TestRequest::get().uri("/").to_request()).await;
        assert_eq!(res.status(), HttpStatusCode::TOO_MANY_REQUESTS);
        assert!(
            res.headers().contains_key("retry-after"),
            "a 429 must carry Retry-After"
        );
        assert!(
            res.headers().contains_key("x-ratelimit-after"),
            "a 429 must carry x-ratelimit-after"
        );
        let body: serde_json::Value = test::read_body_json(res).await;
        assert_eq!(body["error"]["type"], "api_error");
        assert!(body["error"]["message"]
            .as_str()
            .is_some_and(|message| message.starts_with("Too many requests, retry in ")));
    }

    #[actix_web::test]
    async fn refills_after_period_elapses() {
        let key = IpAddr::V4(Ipv4Addr::new(10, 1, 1, 2));
        // burst=1, one element every 20ms — small enough to sleep past in a test.
        let conf = RateLimiterConfig::new(Duration::from_millis(20), 1, FixedKeyExtractor(key));
        let app = test::init_service(
            App::new()
                .wrap(RateLimit::new(&conf))
                .route("/", web::get().to(|| async { Resp::Ok().finish() })),
        )
        .await;

        let res = test::call_service(&app, test::TestRequest::get().uri("/").to_request()).await;
        assert_eq!(res.status(), HttpStatusCode::OK);

        let res = test::call_service(&app, test::TestRequest::get().uri("/").to_request()).await;
        assert_eq!(
            res.status(),
            HttpStatusCode::TOO_MANY_REQUESTS,
            "bucket must be empty immediately after the burst"
        );

        tokio::time::sleep(Duration::from_millis(60)).await;

        let res = test::call_service(&app, test::TestRequest::get().uri("/").to_request()).await;
        assert_eq!(
            res.status(),
            HttpStatusCode::OK,
            "the bucket must have refilled after the period elapses"
        );
    }

    #[actix_web::test]
    async fn extraction_failure_returns_extractors_error_response() {
        #[derive(Clone)]
        struct AlwaysFailsExtractor;

        impl KeyExtractor for AlwaysFailsExtractor {
            type Key = ();
            type KeyExtractionError = SimpleKeyExtractionError;

            fn extract(
                &self,
                _req: &ServiceRequest,
            ) -> Result<Self::Key, Self::KeyExtractionError> {
                Err(SimpleKeyExtractionError::new("no key for you"))
            }
        }

        let conf = RateLimiterConfig::new(Duration::from_secs(1), 1, AlwaysFailsExtractor);
        let app = test::init_service(
            App::new()
                .wrap(RateLimit::new(&conf))
                .route("/", web::get().to(|| async { Resp::Ok().finish() })),
        )
        .await;

        // The middleware returns `Err(err.into())` on extraction failure (matching
        // actix-governor's non-permissive path) rather than an `Ok(response)` —
        // real HTTP serving converts that via `ResponseError` further down the
        // stack (in the H1/H2 dispatcher), so exercise that conversion directly
        // here via `try_call_service` + `ResponseError::error_response()` instead
        // of `call_service` (which panics on `Err`, since it bypasses that layer).
        let err = test::try_call_service(&app, test::TestRequest::get().uri("/").to_request())
            .await
            .expect_err("extraction failure must reach the caller as an Err");
        let response = err.error_response();
        assert_eq!(response.status(), HttpStatusCode::INTERNAL_SERVER_ERROR);
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["message"], "no key for you");
        assert_eq!(body["error"]["type"], "api_error");
    }
}
