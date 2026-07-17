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
//! - on throttle: `429 Too Many Requests` with `retry-after` and
//!   `x-ratelimit-after` headers (seconds until the bucket admits again) and
//!   a `text/plain` body — byte-for-byte what actix-governor's `NoOpMiddleware`
//!   path produced;
//! - on key-extraction failure: whatever response the extractor's error
//!   produces (bamboo's `ClientIpKeyExtractor` mirrors actix-governor's
//!   `SimpleKeyExtractionError` default: `500`, `text/plain`).
//!
//! actix-governor features bamboo never configured — per-method filtering,
//! `permissive` mode, whitelisted keys, and the `x-ratelimit-limit` /
//! `-remaining` / `-whitelisted` headers only added by its
//! `StateInformationMiddleware` — are intentionally NOT reproduced; adding
//! them back is a small extension to [`KeyExtractor`]/[`RateLimitMiddleware`]
//! if ever needed, not a rewrite.

use std::future::{ready, Ready};
use std::num::NonZeroU32;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use actix_web::body::{EitherBody, MessageBody};
use actix_web::dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::http::StatusCode;
use actix_web::{Error, HttpResponse, HttpResponseBuilder, ResponseError};
use futures::future::LocalBoxFuture;
use governor::clock::{Clock, DefaultClock};
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

/// A minimal extraction-failure error: fixed status + `text/plain` body.
///
/// Matches `actix_governor::SimpleKeyExtractionError`'s default (status
/// `500`, content-type `text/plain`) — the only shape bamboo ever used (it
/// never called that type's `set_status_code`/`set_content_type`).
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
        HttpResponseBuilder::new(self.status_code())
            .content_type("text/plain; charset=utf-8")
            .body(self.0)
    }
}

type KeyedLimiter<Key> = GovernorRateLimiter<Key, DefaultKeyedStateStore<Key>, DefaultClock>;

/// Rate-limiter configuration: a shared quota + key extractor, cheap to
/// clone (the limiter is behind an `Arc`) so the SAME bucket state is reused
/// across every `App` factory invocation (actix spins up one `App` per
/// worker thread) — mirrors `actix_governor::GovernorConfig`.
pub struct RateLimiterConfig<K: KeyExtractor> {
    limiter: Arc<KeyedLimiter<K::Key>>,
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
            limiter: Arc::new(GovernorRateLimiter::keyed(quota)),
            key_extractor,
        }
    }
}

/// Rate-limiting middleware factory — mirrors `actix_governor::Governor`.
pub struct RateLimit<K: KeyExtractor> {
    limiter: Arc<KeyedLimiter<K::Key>>,
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
    limiter: Arc<KeyedLimiter<K::Key>>,
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
                let wait_time = negative
                    .wait_time_from(DefaultClock::default().now())
                    .as_secs();
                let response = HttpResponse::build(StatusCode::TOO_MANY_REQUESTS)
                    .insert_header(("retry-after", wait_time))
                    .insert_header(("x-ratelimit-after", wait_time))
                    .content_type("text/plain; charset=utf-8")
                    .body(format!("Too many requests, retry in {wait_time}s"));
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
    use std::net::{IpAddr, Ipv4Addr};

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
    }
}
