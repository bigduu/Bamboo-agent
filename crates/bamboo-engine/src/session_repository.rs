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

    /// Load a session, creating a fresh `Session::new(id, model)` if absent.
    pub async fn load_or_create(&self, session_id: &str, model: &str) -> Session {
        if let Some(session) = self.load(session_id).await {
            return session;
        }
        Session::new(session_id.to_string(), model.to_string())
    }

    /// Load a session, reconciling the memory and storage copies via a
    /// preference heuristic (storage wins when it carries a pending question
    /// memory lacks, or has a newer `updated_at`). Back-fills the cache.
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
                let chosen = if prefer_storage { storage } else { memory };
                self.cache.insert(
                    session_id.to_string(),
                    Arc::new(parking_lot::RwLock::new(chosen.clone())),
                );
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
    if memory_session.pending_question.is_none() && storage_session.pending_question.is_some() {
        return true;
    }
    storage_session.updated_at > memory_session.updated_at
}
