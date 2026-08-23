//! Process-local idempotency for short-lived mutating HTTP requests.
//!
//! `POST /chat` and `POST /execute` have durable side effects but return small,
//! self-contained JSON responses. A client can therefore safely retry an
//! ambiguous request when it supplies an `Idempotency-Key`: the first request
//! owns the key, concurrent duplicates wait for it, and completed responses
//! are replayed for a bounded window. Only digests are retained; raw keys and
//! request payloads never enter the cache or logs.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use actix_web::body::to_bytes;
use actix_web::http::header::{HeaderMap, CONTENT_LENGTH, TRANSFER_ENCODING};
use actix_web::http::StatusCode;
use actix_web::{HttpRequest, HttpResponse};
use dashmap::DashMap;
use serde::Serialize;
use tokio::sync::{Mutex, OwnedMutexGuard};

use super::session_create_operations::{key_digest, payload_fingerprint, validate_key};

pub(crate) const IDEMPOTENCY_KEY_HEADER: &str = "Idempotency-Key";
const DEFAULT_TTL: Duration = Duration::from_secs(10 * 60);
const DEFAULT_CAPACITY: usize = 1_024;
const MAX_CACHED_RESPONSE_BYTES: usize = 256 * 1_024;

#[derive(Clone)]
pub(crate) struct PreparedMutationIdempotency {
    key_digest: String,
    payload_fingerprint: String,
}

#[derive(Serialize)]
struct FingerprintEnvelope<'a, T: ?Sized> {
    target: &'a str,
    payload: &'a T,
}

/// Parse and fingerprint an optional idempotency request.
///
/// `scope` separates independent mutation families (for example, chat and
/// execute). `target` is included in the payload fingerprint, so reusing one
/// execute key for another session fails closed instead of starting work.
pub(crate) fn prepare<T: Serialize + ?Sized>(
    request: &HttpRequest,
    scope: &str,
    target: &str,
    payload: &T,
) -> Result<Option<PreparedMutationIdempotency>, HttpResponse> {
    let Some(value) = request.headers().get(IDEMPOTENCY_KEY_HEADER) else {
        return Ok(None);
    };
    let raw_key = value.to_str().map_err(|_| {
        invalid_key_response("Idempotency-Key must contain only visible ASCII characters")
    })?;
    validate_key(raw_key).map_err(invalid_key_response)?;

    // Domain-separate mutation families while retaining the existing key
    // validation and SHA-256 implementation shared with POST /sessions.
    let digest = key_digest(&format!("mutation-idempotency:v1:{scope}\0{raw_key}"));
    let fingerprint =
        payload_fingerprint(&FingerprintEnvelope { target, payload }).map_err(|error| {
            tracing::error!(
                target: "bamboo.mutation_idempotency",
                phase = "fingerprint",
                error = %error,
                "failed to fingerprint idempotent mutation request"
            );
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": crate::error::error_value(
                    "Failed to fingerprint idempotent mutation request"
                )
            }))
        })?;

    Ok(Some(PreparedMutationIdempotency {
        key_digest: digest,
        payload_fingerprint: fingerprint,
    }))
}

fn invalid_key_response(message: &str) -> HttpResponse {
    HttpResponse::BadRequest().json(serde_json::json!({
        "error": {
            "type": "api_error",
            "code": "invalid_idempotency_key",
            "message": message,
        }
    }))
}

fn conflict_response() -> HttpResponse {
    HttpResponse::Conflict().json(serde_json::json!({
        "error": {
            "type": "api_error",
            "code": "idempotency_key_conflict",
            "message": "Idempotency-Key was already used with a different request payload",
        }
    }))
}

#[derive(Clone)]
struct CachedHttpResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: actix_web::web::Bytes,
}

impl CachedHttpResponse {
    async fn capture(response: HttpResponse) -> Result<Self, String> {
        let status = response.status();
        let headers = response.headers().clone();
        let body = to_bytes(response.into_body())
            .await
            .map_err(|error| format!("failed to buffer idempotent response: {error}"))?;
        Ok(Self {
            status,
            headers,
            body,
        })
    }

    fn to_response(&self) -> HttpResponse {
        let mut builder = HttpResponse::build(self.status);
        for (name, value) in &self.headers {
            if name != CONTENT_LENGTH && name != TRANSFER_ENCODING {
                builder.append_header((name.clone(), value.clone()));
            }
        }
        builder.body(self.body.clone())
    }
}

struct CachedEntry {
    payload_fingerprint: String,
    response: CachedHttpResponse,
    expires_at: Instant,
    sequence: u64,
}

#[derive(Default)]
struct CacheState {
    entries: HashMap<String, CachedEntry>,
    next_sequence: u64,
}

impl CacheState {
    fn prune_expired(&mut self, now: Instant) {
        self.entries.retain(|_, entry| entry.expires_at > now);
    }

    fn insert_bounded(
        &mut self,
        capacity: usize,
        key_digest: String,
        payload_fingerprint: String,
        response: CachedHttpResponse,
        expires_at: Instant,
    ) {
        while self.entries.len() >= capacity {
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.sequence)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            self.entries.remove(&oldest);
        }
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.wrapping_add(1);
        self.entries.insert(
            key_digest,
            CachedEntry {
                payload_fingerprint,
                response,
                expires_at,
                sequence,
            },
        );
    }
}

/// Bounded, process-ephemeral response receipts for chat/execute mutations.
pub(crate) struct MutationIdempotencyStore {
    ttl: Duration,
    capacity: usize,
    entries: Mutex<CacheState>,
    /// Strong entries exist only while at least one request owns or waits for
    /// the key. [`KeyLockGuard`] removes the last idle lock on release.
    key_locks: DashMap<String, Arc<Mutex<()>>>,
}

impl Default for MutationIdempotencyStore {
    fn default() -> Self {
        Self::new(DEFAULT_TTL, DEFAULT_CAPACITY)
    }
}

impl MutationIdempotencyStore {
    fn new(ttl: Duration, capacity: usize) -> Self {
        Self {
            ttl,
            capacity: capacity.max(1),
            entries: Mutex::new(CacheState::default()),
            key_locks: DashMap::new(),
        }
    }

    async fn lock_key(self: &Arc<Self>, key_digest: &str) -> KeyLockGuard {
        let lock = self
            .key_locks
            .entry(key_digest.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let guard = lock.lock_owned().await;
        KeyLockGuard {
            store: self.clone(),
            key_digest: key_digest.to_string(),
            guard: Some(guard),
        }
    }

    /// Run an idempotent mutation or replay the first completed response.
    ///
    /// The per-key mutex remains held through the operation and cache commit.
    /// Cancellation drops the guard without writing a receipt, allowing the
    /// next retry to become the owner instead of waiting forever.
    pub(crate) async fn execute<F, Fut>(
        self: &Arc<Self>,
        prepared: PreparedMutationIdempotency,
        operation: F,
    ) -> HttpResponse
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = HttpResponse>,
    {
        let _key_guard = self.lock_key(&prepared.key_digest).await;
        let now = Instant::now();
        {
            let mut cache = self.entries.lock().await;
            cache.prune_expired(now);
            if let Some(entry) = cache.entries.get(&prepared.key_digest) {
                if entry.payload_fingerprint != prepared.payload_fingerprint {
                    return conflict_response();
                }
                tracing::debug!(
                    target: "bamboo.mutation_idempotency",
                    correlation_id = %correlation_id(&prepared.key_digest),
                    outcome = "replayed",
                    "replayed idempotent mutation response"
                );
                return entry.response.to_response();
            }
        }

        let response = operation().await;
        let captured = match CachedHttpResponse::capture(response).await {
            Ok(captured) => captured,
            Err(error) => {
                tracing::error!(
                    target: "bamboo.mutation_idempotency",
                    correlation_id = %correlation_id(&prepared.key_digest),
                    error = %error,
                    "failed to capture idempotent mutation response"
                );
                return HttpResponse::InternalServerError().json(serde_json::json!({
                    "error": crate::error::error_value(
                        "Failed to capture idempotent mutation response"
                    )
                }));
            }
        };

        if captured.body.len() <= MAX_CACHED_RESPONSE_BYTES {
            let mut cache = self.entries.lock().await;
            cache.prune_expired(Instant::now());
            cache.insert_bounded(
                self.capacity,
                prepared.key_digest.clone(),
                prepared.payload_fingerprint,
                captured.clone(),
                Instant::now() + self.ttl,
            );
            tracing::debug!(
                target: "bamboo.mutation_idempotency",
                correlation_id = %correlation_id(&prepared.key_digest),
                outcome = "stored",
                "stored idempotent mutation response"
            );
        } else {
            tracing::warn!(
                target: "bamboo.mutation_idempotency",
                correlation_id = %correlation_id(&prepared.key_digest),
                response_bytes = captured.body.len(),
                max_response_bytes = MAX_CACHED_RESPONSE_BYTES,
                outcome = "too_large",
                "idempotent mutation response exceeded the cache limit"
            );
        }

        captured.to_response()
    }

    #[cfg(test)]
    async fn entry_count(&self) -> usize {
        self.entries.lock().await.entries.len()
    }
}

fn correlation_id(key_digest: &str) -> &str {
    key_digest.get(..16).unwrap_or(key_digest)
}

struct KeyLockGuard {
    store: Arc<MutationIdempotencyStore>,
    key_digest: String,
    guard: Option<OwnedMutexGuard<()>>,
}

impl Drop for KeyLockGuard {
    fn drop(&mut self) {
        // Release ownership before checking the map's strong count. If no
        // waiter captured the lock concurrently, only the map entry remains.
        self.guard.take();
        self.store
            .key_locks
            .remove_if(&self.key_digest, |_, lock| Arc::strong_count(lock) == 1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;

    fn prepared(key: &str, fingerprint: &str) -> PreparedMutationIdempotency {
        PreparedMutationIdempotency {
            key_digest: key_digest(key),
            payload_fingerprint: fingerprint.to_string(),
        }
    }

    async fn body(response: HttpResponse) -> (StatusCode, actix_web::web::Bytes) {
        let status = response.status();
        let body = to_bytes(response.into_body()).await.expect("response body");
        (status, body)
    }

    #[actix_web::test]
    async fn completed_response_is_replayed_without_running_again() {
        let store = Arc::new(MutationIdempotencyStore::default());
        let calls = Arc::new(AtomicUsize::new(0));

        let first_calls = calls.clone();
        let first = store
            .execute(prepared("same-key", "same-payload"), || async move {
                first_calls.fetch_add(1, Ordering::SeqCst);
                HttpResponse::Created().json(serde_json::json!({"result": "first"}))
            })
            .await;
        let replay_calls = calls.clone();
        let replay = store
            .execute(prepared("same-key", "same-payload"), || async move {
                replay_calls.fetch_add(1, Ordering::SeqCst);
                HttpResponse::Ok().finish()
            })
            .await;

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(body(first).await, body(replay).await);
        assert_eq!(store.entry_count().await, 1);
        assert!(store.key_locks.is_empty(), "idle key locks must self-clean");
    }

    #[actix_web::test]
    async fn same_key_with_different_payload_fails_closed() {
        let store = Arc::new(MutationIdempotencyStore::default());
        let _ = store
            .execute(prepared("same-key", "payload-a"), || async {
                HttpResponse::Created().finish()
            })
            .await;

        let conflict = store
            .execute(prepared("same-key", "payload-b"), || async {
                panic!("conflicting request must not execute")
            })
            .await;
        let (status, bytes) = body(conflict).await;
        assert_eq!(status, StatusCode::CONFLICT);
        let value: serde_json::Value = serde_json::from_slice(&bytes).expect("json response");
        assert_eq!(value["error"]["code"], "idempotency_key_conflict");
    }

    #[actix_web::test]
    async fn concurrent_duplicate_waits_and_executes_only_once() {
        let store = Arc::new(MutationIdempotencyStore::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());

        let first_store = store.clone();
        let first_calls = calls.clone();
        let first_started = started.clone();
        let first = async move {
            first_store
                .execute(prepared("racing-key", "payload"), || async move {
                    first_calls.fetch_add(1, Ordering::SeqCst);
                    first_started.notify_one();
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    HttpResponse::Accepted().json(serde_json::json!({"run_id": "one"}))
                })
                .await
        };

        let second_store = store.clone();
        let second_calls = calls.clone();
        let second = async move {
            started.notified().await;
            second_store
                .execute(prepared("racing-key", "payload"), || async move {
                    second_calls.fetch_add(1, Ordering::SeqCst);
                    HttpResponse::Ok().finish()
                })
                .await
        };

        let (first, second) = tokio::join!(first, second);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(body(first).await, body(second).await);
        assert!(store.key_locks.is_empty(), "idle key locks must self-clean");
    }

    #[actix_web::test]
    async fn ttl_and_capacity_bound_replay_state() {
        let store = Arc::new(MutationIdempotencyStore::new(Duration::from_millis(20), 2));
        for key in ["one", "two", "three"] {
            let _ = store
                .execute(prepared(key, key), || async { HttpResponse::Ok().finish() })
                .await;
        }
        assert_eq!(store.entry_count().await, 2);

        tokio::time::sleep(Duration::from_millis(30)).await;
        let calls = Arc::new(AtomicUsize::new(0));
        let rerun_calls = calls.clone();
        let _ = store
            .execute(prepared("three", "three"), || async move {
                rerun_calls.fetch_add(1, Ordering::SeqCst);
                HttpResponse::Ok().finish()
            })
            .await;
        assert_eq!(calls.load(Ordering::SeqCst), 1, "expired entry must rerun");
        assert_eq!(store.entry_count().await, 1);
    }
}
