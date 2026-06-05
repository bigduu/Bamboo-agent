//! Canonical session coordinator owned by the framework.
//!
//! [`SessionRepository`] bundles the three tiers a Bamboo session lives in — the
//! in-memory [`SessionCache`], the durable [`Storage`], and the
//! merge-on-write [`LockedSessionStore`] — and provides the one canonical
//! load/save coordination (cache → storage → backfill, and dual-write).
//!
//! This is a *framework* capability, not a server one: previously the
//! coordination lived only as inherent methods on the server's `AppState`,
//! which meant anything outside the HTTP server (the SDK, in-process embedders)
//! could not load or persist sessions consistently. `SessionRepository` lets any
//! caller that holds the three tiers share the exact same behaviour; the
//! server's `AppState` now delegates to it.

use std::sync::Arc;

use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::Session;
use bamboo_infrastructure::LockedSessionStore;

use crate::{read_cached_session, SessionCache};

/// Framework-owned coordinator over a session's cache / storage / persistence
/// tiers. Cheap to clone (all fields are `Arc`).
#[derive(Clone)]
pub struct SessionRepository {
    cache: SessionCache,
    storage: Arc<dyn Storage>,
    persistence: Arc<LockedSessionStore>,
}

impl SessionRepository {
    pub fn new(
        cache: SessionCache,
        storage: Arc<dyn Storage>,
        persistence: Arc<LockedSessionStore>,
    ) -> Self {
        Self {
            cache,
            storage,
            persistence,
        }
    }

    pub fn cache(&self) -> &SessionCache {
        &self.cache
    }

    pub fn storage(&self) -> &Arc<dyn Storage> {
        &self.storage
    }

    pub fn persistence(&self) -> &Arc<LockedSessionStore> {
        &self.persistence
    }

    /// Load a session from the memory cache, falling back to durable storage
    /// (and back-filling the cache on a storage hit). `None` if absent in both.
    pub async fn load(&self, session_id: &str) -> Option<Session> {
        if let Some(session) = read_cached_session(&self.cache, session_id) {
            return Some(session);
        }

        match self.storage.load_session(session_id).await {
            Ok(Some(session)) => {
                self.cache.insert(
                    session_id.to_string(),
                    Arc::new(parking_lot::RwLock::new(session.clone())),
                );
                Some(session)
            }
            _ => None,
        }
    }

    /// Like [`load`](Self::load), but surfaces storage errors instead of
    /// swallowing them to `None`. Cache hit short-circuits; a storage hit
    /// back-fills the cache.
    pub async fn try_load(&self, session_id: &str) -> std::io::Result<Option<Session>> {
        if let Some(session) = read_cached_session(&self.cache, session_id) {
            return Ok(Some(session));
        }
        let loaded = self.storage.load_session(session_id).await?;
        if let Some(ref session) = loaded {
            self.cache.insert(
                session_id.to_string(),
                Arc::new(parking_lot::RwLock::new(session.clone())),
            );
        }
        Ok(loaded)
    }

    /// Persist the session (merge-on-write) and refresh the cache, surfacing
    /// storage errors. Use [`save_and_cache`](Self::save_and_cache) for the
    /// fire-and-forget variant that logs and continues on failure.
    pub async fn save(&self, session: &mut Session) -> std::io::Result<()> {
        self.persistence.merge_save_runtime(session).await?;
        self.cache.insert(
            session.id.clone(),
            Arc::new(parking_lot::RwLock::new(session.clone())),
        );
        Ok(())
    }

    /// Load a session, creating a fresh `Session::new(id, model)` if absent.
    pub async fn load_or_create(&self, session_id: &str, model: &str) -> Session {
        if let Some(session) = self.load(session_id).await {
            return session;
        }
        Session::new(session_id.to_string(), model.to_string())
    }

    /// Load a session, reconciling the memory and storage copies via a
    /// preference heuristic: storage wins when it is strictly newer, or when it
    /// is the same age but still carries a pending question memory lost. Storage
    /// is **never** preferred when it is strictly older than memory.
    ///
    /// The cache is refreshed cache-aside but with a no-regression guarantee:
    /// `load_merged` never overwrites a newer cached session with an older
    /// storage copy, so it is safe to call from hot read paths.
    pub async fn load_merged(&self, session_id: &str) -> Option<Session> {
        let memory_session = read_cached_session(&self.cache, session_id);
        let storage_session = self
            .storage
            .load_session(session_id)
            .await
            .unwrap_or_default();

        match (memory_session, storage_session) {
            (Some(memory), Some(storage)) => {
                let prefer_storage = should_prefer_storage(&memory, &storage);
                let diverged = prefer_storage || memory.messages.len() != storage.messages.len();
                let chosen_len = if prefer_storage {
                    storage.messages.len()
                } else {
                    memory.messages.len()
                };
                macro_rules! merged_log {
                    ($level:ident) => {
                        tracing::$level!(
                            "[{}] load_session_merged: memory={} msgs (updated_at={}), storage={} msgs (updated_at={}), prefer_storage={} -> chose {} msgs",
                            session_id,
                            memory.messages.len(),
                            memory.updated_at,
                            storage.messages.len(),
                            storage.updated_at,
                            prefer_storage,
                            chosen_len,
                        )
                    };
                }
                if diverged {
                    merged_log!(debug);
                } else {
                    merged_log!(trace);
                }
                let memory_updated_at = memory.updated_at;
                let chosen = if prefer_storage { storage } else { memory };
                // Cache-aside refresh with a hard no-regression invariant: only
                // write back when we actually reconciled *to storage* (a memory
                // win is already the cached copy; re-inserting it would needlessly
                // replace a possibly-live Arc) AND the reconciled copy is not
                // older than what memory already holds. This is what makes
                // `load_merged` safe on hot read paths — it can never clobber a
                // freshly-updated session with a stale storage copy.
                if prefer_storage && chosen.updated_at >= memory_updated_at {
                    self.cache.insert(
                        session_id.to_string(),
                        Arc::new(parking_lot::RwLock::new(chosen.clone())),
                    );
                }
                Some(chosen)
            }
            (Some(memory), None) => Some(memory),
            (None, Some(storage)) => {
                self.cache.insert(
                    session_id.to_string(),
                    Arc::new(parking_lot::RwLock::new(storage.clone())),
                );
                Some(storage)
            }
            (None, None) => None,
        }
    }

    /// Persist the session (merge-on-write, preserving concurrent UI edits to
    /// the authoritative metadata group) and refresh the in-memory cache.
    pub async fn save_and_cache(&self, session: &mut Session) {
        if let Err(error) = self.persistence.merge_save_runtime(session).await {
            tracing::warn!("[{}] Failed to save session: {}", session.id, error);
        }
        self.cache.insert(
            session.id.clone(),
            Arc::new(parking_lot::RwLock::new(session.clone())),
        );
    }
}

fn should_prefer_storage(memory_session: &Session, storage_session: &Session) -> bool {
    // Never reconcile *backwards* to a strictly-older storage copy: if memory is
    // newer it is authoritative (e.g. it just answered and cleared a pending
    // question while storage still holds the stale one). Respecting `updated_at`
    // here is what stops `load_merged` from returning — and caching — stale data.
    if storage_session.updated_at < memory_session.updated_at {
        return false;
    }
    // Storage is same-age or newer: prefer it when strictly newer, or when it
    // still carries a pending question that the (same-age) memory copy lost, so
    // a genuine clarification is never dropped.
    storage_session.updated_at > memory_session.updated_at
        || (memory_session.pending_question.is_none()
            && storage_session.pending_question.is_some())
}

/// `SessionRepository` is the canonical `RuntimeSessionPersistence`: the runtime
/// can persist a session through the same coordinator (merge-on-write + cache
/// refresh) instead of a bespoke adapter.
#[async_trait::async_trait]
impl bamboo_domain::RuntimeSessionPersistence for SessionRepository {
    async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        self.save(session).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::storage::Storage;
    use chrono::Utc;
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Default)]
    struct MapStorage {
        sessions: Mutex<HashMap<String, Session>>,
    }

    #[async_trait::async_trait]
    impl Storage for MapStorage {
        async fn save_session(&self, session: &Session) -> std::io::Result<()> {
            self.sessions
                .lock()
                .unwrap()
                .insert(session.id.clone(), session.clone());
            Ok(())
        }
        async fn load_session(&self, session_id: &str) -> std::io::Result<Option<Session>> {
            Ok(self.sessions.lock().unwrap().get(session_id).cloned())
        }
        async fn delete_session(&self, session_id: &str) -> std::io::Result<bool> {
            Ok(self.sessions.lock().unwrap().remove(session_id).is_some())
        }
    }

    fn test_repo(storage: Arc<dyn Storage>) -> SessionRepository {
        let cache: SessionCache = Arc::new(dashmap::DashMap::new());
        let persistence = Arc::new(LockedSessionStore::new(storage.clone()));
        SessionRepository::new(cache, storage, persistence)
    }

    fn cache_put(repo: &SessionRepository, session: &Session) {
        repo.cache().insert(
            session.id.clone(),
            Arc::new(parking_lot::RwLock::new(session.clone())),
        );
    }

    /// Regression guard: a strictly-newer in-memory session (e.g. one that just
    /// answered and cleared its pending question) must win over a strictly-older
    /// storage copy that still carries the pending question — both in the value
    /// returned AND in the cache (no clobber).
    #[tokio::test]
    async fn load_merged_does_not_regress_to_older_storage() {
        let storage: Arc<dyn Storage> = Arc::new(MapStorage::default());
        let repo = test_repo(storage.clone());
        let id = "s1";

        let mut stale = Session::new(id.to_string(), "m");
        stale.set_pending_question(
            "tc1".into(),
            "kind".into(),
            "q?".into(),
            vec!["OK".into()],
            true,
        );
        stale.updated_at = Utc::now() - chrono::Duration::seconds(10);
        storage.save_session(&stale).await.unwrap();

        let mut fresh = Session::new(id.to_string(), "m");
        fresh.updated_at = Utc::now();
        cache_put(&repo, &fresh);

        let merged = repo.load_merged(id).await.expect("session exists");
        assert!(
            merged.pending_question.is_none(),
            "must return the newer answered memory copy, not the stale storage one"
        );
        let cached = read_cached_session(repo.cache(), id).expect("cached");
        assert!(
            cached.pending_question.is_none(),
            "load_merged must never regress the cache to a stale storage copy"
        );
    }

    /// The pending-question recovery still works when storage is the same age:
    /// if memory lost a pending question that same-age storage retains, prefer
    /// storage so a genuine clarification is not dropped.
    #[tokio::test]
    async fn load_merged_recovers_pending_question_from_same_age_storage() {
        let storage: Arc<dyn Storage> = Arc::new(MapStorage::default());
        let repo = test_repo(storage.clone());
        let id = "s2";
        let ts = Utc::now();

        let mut with_pending = Session::new(id.to_string(), "m");
        with_pending.set_pending_question(
            "tc".into(),
            "k".into(),
            "q".into(),
            vec!["OK".into()],
            true,
        );
        with_pending.updated_at = ts;
        storage.save_session(&with_pending).await.unwrap();

        let mut lost = with_pending.clone();
        lost.clear_pending_question();
        lost.updated_at = ts;
        cache_put(&repo, &lost);

        let merged = repo.load_merged(id).await.expect("session exists");
        assert!(
            merged.pending_question.is_some(),
            "same-age storage carrying a pending question must still be recovered"
        );
    }
}
