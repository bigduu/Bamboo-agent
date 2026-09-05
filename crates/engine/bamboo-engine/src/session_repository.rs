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
use bamboo_storage::LockedSessionStore;

use crate::{read_cached_session, SessionCache};

#[cfg(test)]
type PostDurableHook = Arc<dyn Fn(&str, &str) + Send + Sync>;

/// Framework-owned coordinator over a session's cache / storage / persistence
/// tiers. Cheap to clone (all fields are `Arc`).
#[derive(Clone)]
pub struct SessionRepository {
    cache: SessionCache,
    storage: Arc<dyn Storage>,
    persistence: Arc<LockedSessionStore>,
    #[cfg(test)]
    post_durable_hook: Option<PostDurableHook>,
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
            #[cfg(test)]
            post_durable_hook: None,
        }
    }

    #[cfg(test)]
    fn with_post_durable_hook(mut self, hook: PostDurableHook) -> Self {
        self.post_durable_hook = Some(hook);
        self
    }

    #[cfg(test)]
    fn run_post_durable_hook(&self, operation: &str, marker: &str) {
        if let Some(hook) = self.post_durable_hook.as_ref() {
            hook(operation, marker);
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

        let _guard = self.persistence.acquire_lock(session_id).await;
        if let Some(session) = read_cached_session(&self.cache, session_id) {
            return Some(session);
        }

        let loaded = self.storage.load_session(session_id).await;
        #[cfg(test)]
        self.run_post_durable_hook("load", session_id);
        match loaded {
            Ok(Some(session)) => {
                self.cache.insert(
                    session_id.to_string(),
                    Arc::new(crate::SessionSnapshot::new(session.clone())),
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

        let _guard = self.persistence.acquire_lock(session_id).await;
        if let Some(session) = read_cached_session(&self.cache, session_id) {
            return Ok(Some(session));
        }

        let loaded = self.storage.load_session(session_id).await?;
        #[cfg(test)]
        self.run_post_durable_hook("try_load", session_id);
        if let Some(ref session) = loaded {
            self.cache.insert(
                session_id.to_string(),
                Arc::new(crate::SessionSnapshot::new(session.clone())),
            );
        }
        Ok(loaded)
    }

    /// Persist the session (merge-on-write) and refresh the cache, surfacing
    /// storage errors. Use [`save_and_cache`](Self::save_and_cache) for the
    /// fire-and-forget variant that logs and continues on failure.
    pub async fn save(&self, session: &mut Session) -> std::io::Result<()> {
        self.persistence
            .merge_save_runtime_and_publish(session, |saved, committed| {
                if committed {
                    #[cfg(test)]
                    self.run_post_durable_hook("save_full", &saved.id);
                    self.cache.insert(
                        saved.id.clone(),
                        Arc::new(crate::SessionSnapshot::new(saved.clone())),
                    );
                }
            })
            .await
    }

    /// Atomically mutate the latest durable runtime session and refresh the
    /// cache with the saved value. This is the safe path for narrow metadata
    /// indexes that can be updated concurrently with runner message writes.
    pub async fn update_runtime_session<F>(
        &self,
        session_id: &str,
        metadata_keys: &[&str],
        mutate: F,
    ) -> std::io::Result<Option<Session>>
    where
        F: FnOnce(&mut Session),
    {
        self.persistence
            .update_runtime_config_and_publish(session_id, mutate, |saved| {
                if let Some(cached) = self.cache.get(session_id) {
                    cached.update(|cached| {
                        for key in metadata_keys {
                            if let Some(value) = saved.metadata.get(*key) {
                                cached.metadata.insert((*key).to_string(), value.clone());
                            } else {
                                cached.metadata.remove(*key);
                            }
                        }
                    });
                }
            })
            .await
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
    pub async fn load_merged_checked(&self, session_id: &str) -> std::io::Result<Option<Session>> {
        let _guard = self.persistence.acquire_lock(session_id).await;
        let memory_session = read_cached_session(&self.cache, session_id);
        let storage_session = self.storage.load_session(session_id).await?;
        #[cfg(test)]
        self.run_post_durable_hook("load_merged", session_id);

        Ok(match (memory_session, storage_session) {
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
                        Arc::new(crate::SessionSnapshot::new(chosen.clone())),
                    );
                }
                Some(chosen)
            }
            (Some(memory), None) => Some(memory),
            (None, Some(storage)) => {
                self.cache.insert(
                    session_id.to_string(),
                    Arc::new(crate::SessionSnapshot::new(storage.clone())),
                );
                Some(storage)
            }
            (None, None) => None,
        })
    }

    /// Compatibility wrapper for read paths where the historical contract
    /// treated a storage failure like absence. Mutating/recovery paths should
    /// use [`Self::load_merged_checked`] so they can preserve retry state.
    pub async fn load_merged(&self, session_id: &str) -> Option<Session> {
        match self.load_merged_checked(session_id).await {
            Ok(session) => session,
            Err(error) => {
                tracing::warn!(
                    "[{}] Failed to load merged session from storage: {}",
                    session_id,
                    error
                );
                read_cached_session(&self.cache, session_id)
            }
        }
    }

    /// Persist the session (merge-on-write, preserving concurrent UI edits to
    /// the authoritative metadata group) and refresh the in-memory cache.
    pub async fn save_and_cache(&self, session: &mut Session) {
        let result = self
            .persistence
            .merge_save_runtime_and_publish(session, |saved, _| {
                #[cfg(test)]
                self.run_post_durable_hook("save_and_cache", &saved.id);
                self.cache.insert(
                    saved.id.clone(),
                    Arc::new(crate::SessionSnapshot::new(saved.clone())),
                );
            })
            .await;
        if let Err(error) = result {
            tracing::warn!("[{}] Failed to save session: {}", session.id, error);
        }
    }

    async fn refresh_cached_task_control_plane(&self, session_id: &str) {
        match self.storage.load_runtime_control_plane(session_id).await {
            Ok(Some(durable)) => {
                if let Some(cached) = self.cache.get(session_id) {
                    cached.update(|cached| adopt_task_control_plane(cached, &durable));
                }
            }
            Ok(None) => {}
            Err(error) => tracing::warn!(
                "[{}] Failed to refresh Task control-plane after write conflict: {}",
                session_id,
                error
            ),
        }
    }
}

fn adopt_task_control_plane(target: &mut Session, durable: &Session) {
    target.task_list = durable.task_list.clone();
    target
        .metadata
        .remove(bamboo_domain::session::runtime_metadata::keys::TASK_LIST_VERSION);
    if let Some(runtime_metadata) = target.runtime_metadata.as_mut() {
        runtime_metadata.task_list_version = None;
    }
    if target
        .runtime_metadata
        .as_ref()
        .is_some_and(bamboo_domain::session::SessionRuntimeMetadata::is_empty)
    {
        target.runtime_metadata = None;
    }
    if let Some(version) = durable.task_list_version_meta() {
        target.set_task_list_version_meta(version);
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
        || (memory_session.pending_question.is_none() && storage_session.pending_question.is_some())
}

/// `SessionRepository` is the canonical `RuntimeSessionPersistence`: the runtime
/// can persist a session through the same coordinator (merge-on-write + cache
/// refresh) instead of a bespoke adapter.
#[async_trait::async_trait]
impl bamboo_domain::RuntimeSessionPersistence for SessionRepository {
    async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        // Runtime authorization reads through this same cache. Refresh it even
        // when durable storage fails so a current activation can never observe
        // a previous run's skill allowlist. The error is still returned to the
        // caller and durable state remains unchanged.
        self.persistence
            .merge_save_runtime_and_publish(session, |saved, _| {
                #[cfg(test)]
                self.run_post_durable_hook("save_runtime_session", &saved.id);
                self.cache.insert(
                    saved.id.clone(),
                    Arc::new(crate::SessionSnapshot::new(saved.clone())),
                );
            })
            .await
    }

    async fn seed_runtime_activation(&self, session: &mut Session) -> std::io::Result<()> {
        self.persistence
            .seed_runtime_activation_and_publish(session, |saved, committed| {
                #[cfg(test)]
                self.run_post_durable_hook("seed_runtime_activation", &saved.id);
                if committed {
                    self.cache.insert(
                        saved.id.clone(),
                        Arc::new(crate::SessionSnapshot::new(saved.clone())),
                    );
                }
            })
            .await
    }

    async fn record_permission_posture_activation(
        &self,
        session_id: &str,
        expected_audit_revision: Option<u64>,
        seed: &bamboo_domain::PermissionAuditSeed,
    ) -> std::io::Result<Option<Session>> {
        self.persistence
            .record_permission_posture_activation_and_publish(
                session_id,
                expected_audit_revision,
                seed,
                |saved| {
                    #[cfg(test)]
                    self.run_post_durable_hook("permission_posture_activation", &saved.id);
                    self.cache.insert(
                        saved.id.clone(),
                        Arc::new(crate::SessionSnapshot::new(saved.clone())),
                    );
                },
            )
            .await
    }

    async fn save_runtime_control_plane(&self, session: &mut Session) -> std::io::Result<()> {
        self.persistence
            .save_runtime_only_and_publish(session, |saved| {
                #[cfg(test)]
                self.run_post_durable_hook("save", &saved.id);

                // A control-plane snapshot may intentionally carry no messages
                // (for example, child Task synchronization loads the root's
                // runtime sidecar). Publish its fresh runtime fields without
                // replacing a cache-resident transcript with that empty
                // snapshot. SessionInbox admission is coupled to transcript
                // persistence and is therefore preserved alongside the cached
                // messages, matching the V2 sidecar overlay contract.
                if let Some(cached) = self.cache.get(&saved.id) {
                    cached.update(|cached| {
                        let messages = cached.messages.clone();
                        let provider_transcript = cached.provider_transcript.clone();
                        let admission = cached
                            .runtime_metadata
                            .as_ref()
                            .and_then(|metadata| metadata.session_inbox_admission.clone());
                        let mut refreshed = saved.clone();
                        refreshed.messages = messages;
                        refreshed.provider_transcript = provider_transcript;
                        if let Some(admission) = admission {
                            refreshed
                                .runtime_metadata
                                .get_or_insert_with(Default::default)
                                .session_inbox_admission = Some(admission);
                        } else if let Some(metadata) = refreshed.runtime_metadata.as_mut() {
                            metadata.session_inbox_admission = None;
                        }
                        *cached = refreshed;
                    });
                }
            })
            .await
    }

    async fn load_runtime_control_plane(
        &self,
        session_id: &str,
    ) -> std::io::Result<Option<Session>> {
        bamboo_domain::RuntimeSessionPersistence::load_runtime_control_plane(
            self.persistence.as_ref(),
            session_id,
        )
        .await
    }

    async fn update_task_list_control_plane(
        &self,
        session_id: &str,
        task_list: &bamboo_domain::TaskList,
        version: &str,
    ) -> std::io::Result<bool> {
        let result = self
            .persistence
            .update_task_list_control_plane_and_publish(session_id, task_list, version, |_| {
                #[cfg(test)]
                self.run_post_durable_hook("task", version);

                // The durable transaction changed only Task-owned fields.
                // Mirror that same narrow patch into the cache so a
                // concurrent round/status/child transition already present
                // in memory cannot be replaced by a stale whole-control-
                // plane snapshot.
                if let Some(cached) = self.cache.get(session_id) {
                    cached.update(|cached| {
                        cached.set_task_list(task_list.clone());
                        cached.set_task_list_version_meta(version.to_string());
                    });
                }
            })
            .await;
        if result
            .as_ref()
            .is_err_and(|error| error.kind() == std::io::ErrorKind::WouldBlock)
        {
            // The storage final-CAS rejected this unconditional patch because
            // an independent writer won. Refresh the winner into the cache but
            // preserve the conflict result so Taskwrite never mistakes it for
            // the Ok(false) legacy-persistence fallback signal.
            self.refresh_cached_task_control_plane(session_id).await;
        }
        result
    }

    async fn update_task_list_control_plane_if_version(
        &self,
        session_id: &str,
        expected_version: &str,
        expected_task_list: &bamboo_domain::TaskList,
        task_list: &bamboo_domain::TaskList,
        version: &str,
    ) -> std::io::Result<bool> {
        self.persistence
            .update_task_list_control_plane_if_version_and_publish(
                session_id,
                expected_version,
                expected_task_list,
                task_list,
                version,
                |_| {
                    if let Some(cached) = self.cache.get(session_id) {
                        cached.update(|cached| {
                            cached.set_task_list(task_list.clone());
                            cached.set_task_list_version_meta(version.to_string());
                        });
                    }
                },
            )
            .await
    }

    async fn update_task_list_control_planes_if_version(
        &self,
        session_id: &str,
        shared_session_id: &str,
        expected_version: &str,
        expected_task_list: &bamboo_domain::TaskList,
        task_list: &bamboo_domain::TaskList,
        version: &str,
    ) -> std::io::Result<bool> {
        self.persistence
            .update_task_list_control_planes_if_version_and_publish(
                session_id,
                shared_session_id,
                expected_version,
                expected_task_list,
                task_list,
                version,
                |_, _| {
                    for id in [session_id, shared_session_id] {
                        if let Some(cached) = self.cache.get(id) {
                            cached.update(|cached| {
                                cached.set_task_list(task_list.clone());
                                cached.set_task_list_version_meta(version.to_string());
                            });
                        }
                    }
                },
            )
            .await
    }

    async fn checkpoint_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        // The execute-boundary checkpoint uses LockedSessionStore's atomic
        // append-safe transcript merge, then publishes that reconciled snapshot
        // to the runtime cache.  On failure, leave the existing cache alone: the
        // checkpoint may have failed to load the latest durable transcript, and
        // replacing a fresher cache entry with the stale runner snapshot would
        // set up a later SHRINK write.
        self.persistence
            .checkpoint_runtime_session_and_publish(session, |saved, committed| {
                #[cfg(test)]
                self.run_post_durable_hook("checkpoint", &saved.id);
                if committed {
                    self.cache.insert(
                        saved.id.clone(),
                        Arc::new(crate::SessionSnapshot::new(saved.clone())),
                    );
                }
            })
            .await
    }

    async fn load_runtime_session(&self, session_id: &str) -> std::io::Result<Option<Session>> {
        self.try_load(session_id).await
    }

    async fn clear_legacy_pending_messages(
        &self,
        session_id: &str,
        expected: &[serde_json::Value],
    ) -> std::io::Result<bool> {
        // Do not use the trait default here: `load_runtime_session` is
        // cache-first, so a stale cache could erase a message concurrently
        // appended to the durable legacy queue. Delegate the compare-and-clear
        // to LockedSessionStore's single per-session critical section.
        self.persistence
            .clear_legacy_pending_messages_and_publish(session_id, expected, |latest| {
                #[cfg(test)]
                self.run_post_durable_hook("clear_legacy", session_id);
                self.cache.insert(
                    session_id.to_string(),
                    Arc::new(crate::SessionSnapshot::new(latest.clone())),
                );
            })
            .await
    }

    async fn append_token_usage_record(
        &self,
        session_id: &str,
        json_line: &str,
    ) -> std::io::Result<()> {
        self.storage
            .append_token_usage_record(session_id, json_line)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::storage::Storage;
    use chrono::Utc;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Condvar, Mutex};
    use std::time::Duration;

    #[derive(Default)]
    struct MapStorage {
        sessions: Mutex<HashMap<String, Session>>,
        fail_pair_commit: AtomicBool,
    }

    struct FailingSaveStorage {
        persisted: Mutex<Option<Session>>,
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

        async fn save_task_control_plane_if_matches(
            &self,
            original: &Session,
            updated: &Session,
        ) -> std::io::Result<bool> {
            let mut sessions = self.sessions.lock().unwrap();
            let matches_original = sessions.get(&original.id).is_some_and(|current| {
                current.task_list_version_meta() == original.task_list_version_meta()
                    && serde_json::to_value(&current.task_list).ok()
                        == serde_json::to_value(&original.task_list).ok()
            });
            if !matches_original {
                return Ok(false);
            }
            let Some(current) = sessions.get(&original.id).cloned() else {
                return Ok(false);
            };
            let mut committed = current;
            committed.task_list = updated.task_list.clone();
            if let Some(version) = updated.task_list_version_meta() {
                committed.set_task_list_version_meta(version);
            }
            sessions.insert(committed.id.clone(), committed);
            Ok(true)
        }

        async fn save_task_control_planes_atomically(
            &self,
            first_original: &Session,
            first_updated: &Session,
            second_original: &Session,
            second_updated: &Session,
        ) -> std::io::Result<bool> {
            if self.fail_pair_commit.swap(false, Ordering::SeqCst) {
                return Err(std::io::Error::other(
                    "injected paired Task transaction failure",
                ));
            }
            let mut sessions = self.sessions.lock().unwrap();
            let matches_original = |current: Option<&Session>, original: &Session| {
                current.is_some_and(|current| {
                    current.task_list_version_meta() == original.task_list_version_meta()
                        && serde_json::to_value(&current.task_list).ok()
                            == serde_json::to_value(&original.task_list).ok()
                })
            };
            if !matches_original(sessions.get(&first_original.id), first_original)
                || !matches_original(sessions.get(&second_original.id), second_original)
            {
                return Ok(false);
            }
            sessions.insert(first_updated.id.clone(), first_updated.clone());
            sessions.insert(second_updated.id.clone(), second_updated.clone());
            Ok(true)
        }
    }

    #[async_trait::async_trait]
    impl Storage for FailingSaveStorage {
        async fn save_session(&self, _session: &Session) -> std::io::Result<()> {
            Err(std::io::Error::other("injected save failure"))
        }

        async fn load_session(&self, _session_id: &str) -> std::io::Result<Option<Session>> {
            Ok(self.persisted.lock().unwrap().clone())
        }

        async fn delete_session(&self, _session_id: &str) -> std::io::Result<bool> {
            Ok(false)
        }
    }

    fn test_repo(storage: Arc<dyn Storage>) -> SessionRepository {
        let cache: SessionCache = Arc::default();
        let persistence = Arc::new(LockedSessionStore::new(storage.clone()));
        SessionRepository::new(cache, storage, persistence)
    }

    fn cache_put(repo: &SessionRepository, session: &Session) {
        repo.cache().insert(
            session.id.clone(),
            Arc::new(crate::SessionSnapshot::new(session.clone())),
        );
    }

    fn task_list(session_id: &str, title: &str) -> bamboo_domain::TaskList {
        let now = Utc::now();
        bamboo_domain::TaskList {
            session_id: session_id.to_string(),
            title: title.to_string(),
            items: Vec::new(),
            created_at: now,
            updated_at: now,
        }
    }

    #[tokio::test]
    async fn paired_task_cas_narrowly_updates_child_and_root_cache() {
        let storage: Arc<dyn Storage> = Arc::new(MapStorage::default());
        let repo = test_repo(storage.clone());
        let root_id = "paired-cache-root";
        let child_id = "paired-cache-child";
        let expected_task_list = task_list(root_id, "old shared");

        let mut root = Session::new(root_id, "model");
        root.add_message(bamboo_agent_core::Message::user("root transcript"));
        root.metadata
            .insert("unrelated.root".to_string(), "keep".to_string());
        root.set_task_list(expected_task_list.clone());
        root.set_task_list_version_meta("1");
        storage.save_session(&root).await.unwrap();
        cache_put(&repo, &root);

        let mut child = Session::new_child(child_id, root_id, "model", "child");
        child.add_message(bamboo_agent_core::Message::user("child transcript"));
        child
            .metadata
            .insert("unrelated.child".to_string(), "keep".to_string());
        child.set_task_list(expected_task_list.clone());
        child.set_task_list_version_meta("1");
        storage.save_session(&child).await.unwrap();
        cache_put(&repo, &child);

        assert!(
            bamboo_domain::RuntimeSessionPersistence::update_task_list_control_planes_if_version(
                &repo,
                child_id,
                root_id,
                "1",
                &expected_task_list,
                &task_list(root_id, "evaluated"),
                "2",
            )
            .await
            .expect("paired cache CAS succeeds")
        );

        let cached_root = read_cached_session(repo.cache(), root_id).expect("cached root");
        let cached_child = read_cached_session(repo.cache(), child_id).expect("cached child");
        for (session, transcript, metadata_key) in [
            (&cached_root, "root transcript", "unrelated.root"),
            (&cached_child, "child transcript", "unrelated.child"),
        ] {
            assert_eq!(session.task_list_version_meta().as_deref(), Some("2"));
            assert_eq!(
                session.task_list.as_ref().map(|list| list.title.as_str()),
                Some("evaluated")
            );
            assert_eq!(session.messages.len(), 1);
            assert_eq!(session.messages[0].content, transcript);
            assert_eq!(
                session.metadata.get(metadata_key).map(String::as_str),
                Some("keep")
            );
        }
    }

    #[tokio::test]
    async fn failed_paired_task_transaction_does_not_publish_child_or_root_cache() {
        let concrete = Arc::new(MapStorage::default());
        let storage: Arc<dyn Storage> = concrete.clone();
        let repo = test_repo(storage.clone());
        let root_id = "paired-cache-failure-root";
        let child_id = "paired-cache-failure-child";
        let expected_task_list = task_list(root_id, "old shared");

        let mut root = Session::new(root_id, "model");
        root.add_message(bamboo_agent_core::Message::user("root transcript"));
        root.metadata
            .insert("unrelated.root".to_string(), "keep".to_string());
        root.set_task_list(expected_task_list.clone());
        root.set_task_list_version_meta("1");
        storage.save_session(&root).await.unwrap();
        cache_put(&repo, &root);

        let mut child = Session::new_child(child_id, root_id, "model", "child");
        child.add_message(bamboo_agent_core::Message::user("child transcript"));
        child
            .metadata
            .insert("unrelated.child".to_string(), "keep".to_string());
        child.set_task_list(expected_task_list.clone());
        child.set_task_list_version_meta("1");
        storage.save_session(&child).await.unwrap();
        cache_put(&repo, &child);

        concrete.fail_pair_commit.store(true, Ordering::SeqCst);
        let error =
            bamboo_domain::RuntimeSessionPersistence::update_task_list_control_planes_if_version(
                &repo,
                child_id,
                root_id,
                "1",
                &expected_task_list,
                &task_list(root_id, "must not publish"),
                "2",
            )
            .await
            .expect_err("paired durable transaction fails");
        assert!(error.to_string().contains("injected paired"));

        for (id, expected_title, transcript, metadata_key) in [
            (root_id, "old shared", "root transcript", "unrelated.root"),
            (
                child_id,
                "old shared",
                "child transcript",
                "unrelated.child",
            ),
        ] {
            let cached = read_cached_session(repo.cache(), id).expect("cached session remains");
            assert_eq!(cached.task_list_version_meta().as_deref(), Some("1"));
            assert_eq!(
                cached.task_list.as_ref().map(|list| list.title.as_str()),
                Some(expected_title)
            );
            assert_eq!(cached.messages[0].content, transcript);
            assert_eq!(
                cached.metadata.get(metadata_key).map(String::as_str),
                Some("keep")
            );

            let durable = storage.load_session(id).await.unwrap().unwrap();
            assert_eq!(durable.task_list_version_meta().as_deref(), Some("1"));
            assert_eq!(
                durable.task_list.as_ref().map(|list| list.title.as_str()),
                Some(expected_title)
            );
        }
    }

    fn durable_cache_fence(
        operation: impl Into<String>,
        marker: impl Into<String>,
    ) -> (
        PostDurableHook,
        tokio::sync::oneshot::Receiver<()>,
        Arc<(Mutex<bool>, Condvar)>,
    ) {
        let operation = operation.into();
        let marker = marker.into();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let started_tx = Arc::new(Mutex::new(Some(started_tx)));
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let hook_release = release.clone();
        let hook: PostDurableHook = Arc::new(move |actual_operation, actual_marker| {
            if actual_operation != operation || actual_marker != marker {
                return;
            }
            if let Some(started_tx) = started_tx.lock().unwrap().take() {
                started_tx.send(()).expect("fence observer still present");
            }
            let (released, wake) = &*hook_release;
            let mut released = released.lock().unwrap();
            while !*released {
                released = wake.wait(released).unwrap();
            }
        });
        (hook, started_rx, release)
    }

    fn release_fence(release: &Arc<(Mutex<bool>, Condvar)>) {
        let (released, wake) = &**release;
        *released.lock().unwrap() = true;
        wake.notify_all();
    }

    async fn assert_second_write_waits_for_cache_publish<T>(
        first: tokio::task::JoinHandle<std::io::Result<T>>,
        mut second: tokio::task::JoinHandle<std::io::Result<T>>,
        release: Arc<(Mutex<bool>, Condvar)>,
    ) -> (T, T) {
        let second_before_release =
            tokio::time::timeout(Duration::from_millis(100), &mut second).await;
        let completed_before_release = second_before_release.is_ok();
        release_fence(&release);

        let first = first
            .await
            .expect("first writer joins")
            .expect("first writer succeeds");
        let second = match second_before_release {
            Ok(joined) => joined
                .expect("second writer joins")
                .expect("second writer succeeds"),
            Err(_) => second
                .await
                .expect("second writer joins")
                .expect("second writer succeeds"),
        };
        assert!(
            !completed_before_release,
            "the second write must remain behind the first write's durable-to-cache fence"
        );
        (first, second)
    }

    #[derive(Clone, Copy, Debug)]
    enum FullSaveRoute {
        InherentSave,
        SaveAndCache,
        RuntimePersistence,
    }

    impl FullSaveRoute {
        fn operation(self) -> &'static str {
            match self {
                Self::InherentSave => "save_full",
                Self::SaveAndCache => "save_and_cache",
                Self::RuntimePersistence => "save_runtime_session",
            }
        }

        fn name(self) -> &'static str {
            match self {
                Self::InherentSave => "inherent",
                Self::SaveAndCache => "save-and-cache",
                Self::RuntimePersistence => "runtime-persistence",
            }
        }
    }

    async fn assert_full_save_route_serializes_cache_publish(route: FullSaveRoute) {
        let temp = tempfile::tempdir().unwrap();
        let concrete_storage = Arc::new(
            bamboo_storage::SessionStoreV2::new(temp.path().to_path_buf())
                .await
                .expect("SessionStoreV2"),
        );
        let storage: Arc<dyn Storage> = concrete_storage;
        let id = format!("root-full-cache-order-{}", route.name());
        let (hook, first_durable, release) = durable_cache_fence(route.operation(), id.clone());
        let repo = Arc::new(test_repo(storage.clone()).with_post_durable_hook(hook));

        let mut initial = Session::new(&id, "model");
        initial.add_message(bamboo_agent_core::Message::user("durable transcript"));
        initial.set_task_list(task_list(&id, "initial"));
        initial.set_task_list_version_meta("0");
        initial
            .metadata
            .insert("unrelated.runtime".to_string(), "keep".to_string());
        storage.save_session(&initial).await.unwrap();
        cache_put(&repo, &initial);

        let first_repo = repo.clone();
        let mut root_snapshot = initial.clone();
        root_snapshot.add_message(bamboo_agent_core::Message::assistant(
            "full-save transcript suffix",
            None,
        ));
        root_snapshot.set_task_list(task_list(&id, "root"));
        root_snapshot.set_task_list_version_meta("1");
        let first = tokio::spawn(async move {
            match route {
                FullSaveRoute::InherentSave => first_repo.save(&mut root_snapshot).await,
                FullSaveRoute::SaveAndCache => {
                    first_repo.save_and_cache(&mut root_snapshot).await;
                    Ok(())
                }
                FullSaveRoute::RuntimePersistence => {
                    bamboo_domain::RuntimeSessionPersistence::save_runtime_session(
                        first_repo.as_ref(),
                        &mut root_snapshot,
                    )
                    .await
                }
            }
        });
        first_durable
            .await
            .expect("root full durable write reached");

        let second_repo = repo.clone();
        let second_id = id.clone();
        let child_task_list = task_list(&id, "child");
        let second = tokio::spawn(async move {
            bamboo_domain::RuntimeSessionPersistence::update_task_list_control_plane(
                second_repo.as_ref(),
                &second_id,
                &child_task_list,
                "2",
            )
            .await
            .map(|updated| assert!(updated, "root must exist"))
        });
        assert_second_write_waits_for_cache_publish(first, second, release).await;

        let durable = storage.load_session(&id).await.unwrap().unwrap();
        let cached = read_cached_session(repo.cache(), &id).expect("cached root");
        for (tier, session) in [("durable", durable), ("cache", cached)] {
            assert_eq!(
                session.task_list_version_meta().as_deref(),
                Some("2"),
                "{route:?} {tier} must retain the child transaction"
            );
            assert_eq!(
                session.task_list.as_ref().map(|list| list.title.as_str()),
                Some("child"),
                "{route:?} {tier} must retain the child transaction"
            );
            assert_eq!(
                session
                    .metadata
                    .get("unrelated.runtime")
                    .map(String::as_str),
                Some("keep"),
                "{route:?} {tier} must preserve unrelated runtime state"
            );
            assert_eq!(
                session.messages.len(),
                2,
                "{route:?} {tier} must preserve the full-save transcript"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn root_full_saves_and_child_task_patch_share_publish_order() {
        for route in [
            FullSaveRoute::InherentSave,
            FullSaveRoute::SaveAndCache,
            FullSaveRoute::RuntimePersistence,
        ] {
            assert_full_save_route_serializes_cache_publish(route).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn checkpoint_and_child_task_patch_share_publish_order() {
        let temp = tempfile::tempdir().unwrap();
        let concrete_storage = Arc::new(
            bamboo_storage::SessionStoreV2::new(temp.path().to_path_buf())
                .await
                .expect("SessionStoreV2"),
        );
        let storage: Arc<dyn Storage> = concrete_storage;
        let id = "checkpoint-cache-order";
        let (hook, checkpoint_durable, release) = durable_cache_fence("checkpoint", id);
        let repo = Arc::new(test_repo(storage.clone()).with_post_durable_hook(hook));

        let mut initial = Session::new(id, "model");
        initial.add_message(bamboo_agent_core::Message::user("durable transcript"));
        initial.set_task_list(task_list(id, "initial"));
        initial.set_task_list_version_meta("0");
        initial
            .metadata
            .insert("unrelated.runtime".to_string(), "keep".to_string());
        storage.save_session(&initial).await.unwrap();
        cache_put(&repo, &initial);

        let checkpoint_repo = repo.clone();
        let mut checkpoint_snapshot = initial.clone();
        checkpoint_snapshot.add_message(bamboo_agent_core::Message::assistant(
            "checkpoint transcript suffix",
            None,
        ));
        checkpoint_snapshot.set_task_list(task_list(id, "checkpoint"));
        checkpoint_snapshot.set_task_list_version_meta("1");
        let checkpoint = tokio::spawn(async move {
            bamboo_domain::RuntimeSessionPersistence::checkpoint_runtime_session(
                checkpoint_repo.as_ref(),
                &mut checkpoint_snapshot,
            )
            .await
        });
        checkpoint_durable
            .await
            .expect("checkpoint durable write reached");

        let child_repo = repo.clone();
        let child_task_list = task_list(id, "child");
        let child_patch = tokio::spawn(async move {
            bamboo_domain::RuntimeSessionPersistence::update_task_list_control_plane(
                child_repo.as_ref(),
                id,
                &child_task_list,
                "2",
            )
            .await
            .map(|updated| assert!(updated, "root must exist"))
        });
        assert_second_write_waits_for_cache_publish(checkpoint, child_patch, release).await;

        let durable = storage.load_session(id).await.unwrap().unwrap();
        let cached = read_cached_session(repo.cache(), id).expect("cached root");
        for (tier, session) in [("durable", durable), ("cache", cached)] {
            assert_eq!(
                session.task_list_version_meta().as_deref(),
                Some("2"),
                "{tier} must retain the child transaction"
            );
            assert_eq!(
                session.task_list.as_ref().map(|list| list.title.as_str()),
                Some("child"),
                "{tier} must retain the child transaction"
            );
            assert_eq!(
                session
                    .metadata
                    .get("unrelated.runtime")
                    .map(String::as_str),
                Some("keep"),
                "{tier} must preserve unrelated runtime state"
            );
            assert_eq!(
                session.messages.len(),
                2,
                "{tier} must preserve the checkpoint transcript"
            );
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum CacheBackfillRoute {
        Load,
        TryLoad,
    }

    impl CacheBackfillRoute {
        fn operation(self) -> &'static str {
            match self {
                Self::Load => "load",
                Self::TryLoad => "try_load",
            }
        }

        fn name(self) -> &'static str {
            match self {
                Self::Load => "load",
                Self::TryLoad => "try-load",
            }
        }
    }

    async fn assert_cache_backfill_serializes_with_task_patch(route: CacheBackfillRoute) {
        let temp = tempfile::tempdir().unwrap();
        let concrete_storage = Arc::new(
            bamboo_storage::SessionStoreV2::new(temp.path().to_path_buf())
                .await
                .expect("SessionStoreV2"),
        );
        let storage: Arc<dyn Storage> = concrete_storage;
        let id = format!("cache-backfill-order-{}", route.name());
        let (hook, loaded_old_durable, release) =
            durable_cache_fence(route.operation(), id.clone());
        let repo = Arc::new(test_repo(storage.clone()).with_post_durable_hook(hook));

        let mut initial = Session::new(&id, "model");
        initial.set_task_list(task_list(&id, "initial"));
        initial.set_task_list_version_meta("0");
        storage.save_session(&initial).await.unwrap();
        assert!(
            read_cached_session(repo.cache(), &id).is_none(),
            "the race requires a genuine cache miss"
        );

        let load_repo = repo.clone();
        let load_id = id.clone();
        let load = tokio::spawn(async move {
            let loaded = match route {
                CacheBackfillRoute::Load => load_repo.load(&load_id).await,
                CacheBackfillRoute::TryLoad => {
                    load_repo.try_load(&load_id).await.expect("storage load")
                }
            };
            assert!(loaded.is_some(), "seeded session must load");
            Ok(())
        });
        loaded_old_durable
            .await
            .expect("old durable snapshot loaded");

        let patch_repo = repo.clone();
        let patch_id = id.clone();
        let child_task_list = task_list(&id, "child");
        let patch = tokio::spawn(async move {
            bamboo_domain::RuntimeSessionPersistence::update_task_list_control_plane(
                patch_repo.as_ref(),
                &patch_id,
                &child_task_list,
                "1",
            )
            .await
            .map(|updated| assert!(updated, "root must exist"))
        });
        assert_second_write_waits_for_cache_publish(load, patch, release).await;

        let durable = storage.load_session(&id).await.unwrap().unwrap();
        let cached = read_cached_session(repo.cache(), &id).expect("backfilled cache");
        for (tier, session) in [("durable", durable), ("cache", cached)] {
            assert_eq!(
                session.task_list_version_meta().as_deref(),
                Some("1"),
                "{route:?} {tier} must retain the child transaction"
            );
            assert_eq!(
                session.task_list.as_ref().map(|list| list.title.as_str()),
                Some("child"),
                "{route:?} {tier} must retain the child transaction"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cache_miss_backfills_and_child_task_patch_share_publish_order() {
        for route in [CacheBackfillRoute::Load, CacheBackfillRoute::TryLoad] {
            assert_cache_backfill_serializes_with_task_patch(route).await;
        }
    }

    #[tokio::test]
    async fn cache_hits_do_not_wait_for_the_persistence_lock() {
        let storage: Arc<dyn Storage> = Arc::new(MapStorage::default());
        let repo = test_repo(storage);
        let id = "cache-hit-lock-free";
        let cached = Session::new(id, "cached-model");
        cache_put(&repo, &cached);
        let persistence_guard = repo.persistence().acquire_lock(id).await;

        let loaded = tokio::time::timeout(Duration::from_millis(100), repo.load(id)).await;
        let try_loaded = tokio::time::timeout(Duration::from_millis(100), repo.try_load(id)).await;
        drop(persistence_guard);

        assert_eq!(
            loaded
                .expect("cache hit must not wait for the persistence lock")
                .expect("cached session")
                .model,
            "cached-model"
        );
        assert_eq!(
            try_loaded
                .expect("fallible cache hit must not wait for the persistence lock")
                .expect("cache read succeeds")
                .expect("cached session")
                .model,
            "cached-model"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn load_merged_storage_refresh_and_child_task_patch_share_publish_order() {
        let temp = tempfile::tempdir().unwrap();
        let concrete_storage = Arc::new(
            bamboo_storage::SessionStoreV2::new(temp.path().to_path_buf())
                .await
                .expect("SessionStoreV2"),
        );
        let storage: Arc<dyn Storage> = concrete_storage;
        let id = "load-merged-cache-order";
        let (hook, loaded_old_durable, release) = durable_cache_fence("load_merged", id);
        let repo = Arc::new(test_repo(storage.clone()).with_post_durable_hook(hook));

        let mut durable = Session::new(id, "model");
        durable.updated_at = Utc::now();
        durable.set_task_list(task_list(id, "initial"));
        durable.set_task_list_version_meta("0");
        storage.save_session(&durable).await.unwrap();

        let mut memory = durable.clone();
        memory.updated_at = durable.updated_at - chrono::Duration::seconds(1);
        memory.set_task_list(task_list(id, "memory"));
        cache_put(&repo, &memory);

        let load_repo = repo.clone();
        let load = tokio::spawn(async move {
            assert!(
                load_repo.load_merged(id).await.is_some(),
                "seeded session must load"
            );
            Ok(())
        });
        loaded_old_durable
            .await
            .expect("old durable snapshot loaded");

        let patch_repo = repo.clone();
        let child_task_list = task_list(id, "child");
        let patch = tokio::spawn(async move {
            bamboo_domain::RuntimeSessionPersistence::update_task_list_control_plane(
                patch_repo.as_ref(),
                id,
                &child_task_list,
                "1",
            )
            .await
            .map(|updated| assert!(updated, "root must exist"))
        });
        assert_second_write_waits_for_cache_publish(load, patch, release).await;

        let durable = storage.load_session(id).await.unwrap().unwrap();
        let cached = read_cached_session(repo.cache(), id).expect("refreshed cache");
        for (tier, session) in [("durable", durable), ("cache", cached)] {
            assert_eq!(
                session.task_list_version_meta().as_deref(),
                Some("1"),
                "{tier} must retain the child transaction"
            );
            assert_eq!(
                session.task_list.as_ref().map(|list| list.title.as_str()),
                Some("child"),
                "{tier} must retain the child transaction"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn legacy_clear_refresh_and_child_task_patch_share_publish_order() {
        let temp = tempfile::tempdir().unwrap();
        let concrete_storage = Arc::new(
            bamboo_storage::SessionStoreV2::new(temp.path().to_path_buf())
                .await
                .expect("SessionStoreV2"),
        );
        let storage: Arc<dyn Storage> = concrete_storage;
        let id = "legacy-clear-cache-order";
        let expected = vec![serde_json::json!({"content": "legacy"})];
        let (hook, loaded_post_cas_snapshot, release) = durable_cache_fence("clear_legacy", id);
        let repo = Arc::new(test_repo(storage.clone()).with_post_durable_hook(hook));

        let mut initial = Session::new(id, "model");
        initial.set_pending_injected_messages(expected.clone());
        initial.set_task_list(task_list(id, "initial"));
        initial.set_task_list_version_meta("0");
        storage.save_session(&initial).await.unwrap();
        cache_put(&repo, &initial);

        let clear_repo = repo.clone();
        let clear_expected = expected.clone();
        let clear = tokio::spawn(async move {
            bamboo_domain::RuntimeSessionPersistence::clear_legacy_pending_messages(
                clear_repo.as_ref(),
                id,
                &clear_expected,
            )
            .await
            .map(|cleared| assert!(cleared, "legacy queue must match"))
        });
        loaded_post_cas_snapshot
            .await
            .expect("post-CAS snapshot loaded");

        let patch_repo = repo.clone();
        let child_task_list = task_list(id, "child");
        let patch = tokio::spawn(async move {
            bamboo_domain::RuntimeSessionPersistence::update_task_list_control_plane(
                patch_repo.as_ref(),
                id,
                &child_task_list,
                "1",
            )
            .await
            .map(|updated| assert!(updated, "root must exist"))
        });
        assert_second_write_waits_for_cache_publish(clear, patch, release).await;

        let durable = storage.load_session(id).await.unwrap().unwrap();
        let cached = read_cached_session(repo.cache(), id).expect("refreshed cache");
        for (tier, session) in [("durable", durable), ("cache", cached)] {
            assert_eq!(
                session.task_list_version_meta().as_deref(),
                Some("1"),
                "{tier} must retain the child transaction"
            );
            assert_eq!(
                session.task_list.as_ref().map(|list| list.title.as_str()),
                Some("child"),
                "{tier} must retain the child transaction"
            );
            assert!(
                !session.has_pending_injected_messages(),
                "{tier} must retain the successful legacy clear"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_task_patches_publish_cache_in_durable_order() {
        let storage: Arc<dyn Storage> = Arc::new(MapStorage::default());
        let (hook, first_durable, release) = durable_cache_fence("task", "1");
        let repo = Arc::new(test_repo(storage.clone()).with_post_durable_hook(hook));
        let id = "concurrent-task-cache-order";
        let mut initial = Session::new(id, "model");
        initial.set_task_list(task_list(id, "initial"));
        initial.set_task_list_version_meta("0");
        storage.save_session(&initial).await.unwrap();
        cache_put(&repo, &initial);

        let first_repo = repo.clone();
        let first_task_list = task_list(id, "first");
        let first = tokio::spawn(async move {
            bamboo_domain::RuntimeSessionPersistence::update_task_list_control_plane(
                first_repo.as_ref(),
                id,
                &first_task_list,
                "1",
            )
            .await
        });
        first_durable.await.expect("first durable write reached");

        let second_repo = repo.clone();
        let second_task_list = task_list(id, "second");
        let second = tokio::spawn(async move {
            bamboo_domain::RuntimeSessionPersistence::update_task_list_control_plane(
                second_repo.as_ref(),
                id,
                &second_task_list,
                "2",
            )
            .await
        });
        let (first_updated, second_updated) =
            assert_second_write_waits_for_cache_publish(first, second, release).await;
        assert!(first_updated && second_updated);

        let durable = storage.load_session(id).await.unwrap().unwrap();
        let cached = read_cached_session(repo.cache(), id).expect("cached root");
        for (tier, session) in [("durable", durable), ("cache", cached)] {
            assert_eq!(
                session.task_list_version_meta().as_deref(),
                Some("2"),
                "{tier} must retain the second transaction"
            );
            assert_eq!(
                session.task_list.as_ref().map(|list| list.title.as_str()),
                Some("second"),
                "{tier} must retain the second transaction"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn root_control_plane_save_and_child_task_patch_share_publish_order() {
        let storage: Arc<dyn Storage> = Arc::new(MapStorage::default());
        let (hook, root_durable, release) = durable_cache_fence("save", "root-control-plane-order");
        let repo = Arc::new(test_repo(storage.clone()).with_post_durable_hook(hook));
        let id = "root-control-plane-order";

        let mut initial = Session::new(id, "model");
        initial.add_message(bamboo_agent_core::Message::user("durable transcript"));
        initial.set_task_list(task_list(id, "initial"));
        initial.set_task_list_version_meta("0");
        initial
            .metadata
            .insert("unrelated.runtime".to_string(), "keep".to_string());
        storage.save_session(&initial).await.unwrap();
        cache_put(&repo, &initial);

        let root_repo = repo.clone();
        let mut root_snapshot = initial.clone();
        root_snapshot.set_task_list(task_list(id, "root"));
        root_snapshot.set_task_list_version_meta("1");
        let root_save = tokio::spawn(async move {
            bamboo_domain::RuntimeSessionPersistence::save_runtime_control_plane(
                root_repo.as_ref(),
                &mut root_snapshot,
            )
            .await
        });
        root_durable.await.expect("root durable write reached");

        let child_repo = repo.clone();
        let child_task_list = task_list(id, "child");
        let child_patch = tokio::spawn(async move {
            bamboo_domain::RuntimeSessionPersistence::update_task_list_control_plane(
                child_repo.as_ref(),
                id,
                &child_task_list,
                "2",
            )
            .await
            .map(|updated| {
                assert!(updated, "root must exist");
            })
        });
        assert_second_write_waits_for_cache_publish(root_save, child_patch, release).await;

        let durable = storage.load_session(id).await.unwrap().unwrap();
        let cached = read_cached_session(repo.cache(), id).expect("cached root");
        for (tier, session) in [("durable", durable), ("cache", cached)] {
            assert_eq!(
                session.task_list_version_meta().as_deref(),
                Some("2"),
                "{tier} must retain the child transaction"
            );
            assert_eq!(
                session.task_list.as_ref().map(|list| list.title.as_str()),
                Some("child"),
                "{tier} must retain the child transaction"
            );
            assert_eq!(
                session
                    .metadata
                    .get("unrelated.runtime")
                    .map(String::as_str),
                Some("keep"),
                "{tier} must preserve unrelated runtime state"
            );
            assert_eq!(
                session.messages.len(),
                1,
                "{tier} must preserve the transcript"
            );
        }
    }

    #[tokio::test]
    async fn stale_control_plane_save_keeps_checkpointed_ledger_in_durable_and_cache() {
        let temp = tempfile::tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(
            bamboo_storage::SessionStoreV2::new(temp.path().to_path_buf())
                .await
                .expect("SessionStoreV2"),
        );
        let repo = test_repo(storage.clone());
        let id = "control-plane-ledger-cache";
        let mut initial = Session::new(id, "model");
        initial.add_message(bamboo_agent_core::Message::user("durable transcript"));
        let assistant = bamboo_agent_core::Message::assistant("normalized", None);
        let anchor = assistant.id.clone();
        initial.add_message(assistant);
        let native_item = bamboo_domain::ProviderTranscriptItem::try_from_payload(
            bamboo_domain::ProviderFamily::OpenAi,
            bamboo_domain::ProviderProtocol::OpenAiResponsesV1,
            bamboo_domain::ProviderTranscriptOrigin::Provider,
            bamboo_domain::ProviderTranscriptAuthor::Model,
            serde_json::json!({
                "type":"tool_search_call","id":"tsc_cache_native","execution":"client","call_id":"cache_native",
                "status":"completed","arguments":{"query":"CACHE_NATIVE_PAYLOAD_SENTINEL"}
            }),
        )
        .unwrap();
        initial
            .append_provider_transcript_group(&anchor, None, vec![native_item])
            .unwrap();
        storage.save_session(&initial).await.unwrap();
        cache_put(&repo, &initial);
        let mut stale = initial.clone();

        let mut runner = initial;
        runner.model_context_state = Some(bamboo_domain::ModelContextState {
            state_revision: 1,
            prefix_epoch: 1,
            cache_scope_sha256: Some("scope".to_string()),
            transcript_item_sha256: vec!["runner-l1".to_string()],
            ..bamboo_domain::ModelContextState::default()
        });
        bamboo_domain::RuntimeSessionPersistence::checkpoint_runtime_session(&repo, &mut runner)
            .await
            .unwrap();

        stale
            .metadata
            .insert("runtime.suspend_reason".to_string(), "waiting".to_string());
        bamboo_domain::RuntimeSessionPersistence::save_runtime_control_plane(&repo, &mut stale)
            .await
            .unwrap();

        let expected = runner.model_context_state;
        let expected_native = runner.provider_transcript;
        let durable = storage.load_session(id).await.unwrap().unwrap();
        let cached = read_cached_session(repo.cache(), id).expect("cached session");
        for (tier, session) in [("durable", durable), ("cache", cached.clone())] {
            assert_eq!(session.model_context_state, expected, "tier={tier}");
            assert_eq!(
                session.provider_transcript, expected_native,
                "tier={tier} must retain the message-anchored native transcript"
            );
            assert_eq!(
                session
                    .metadata
                    .get("runtime.suspend_reason")
                    .map(String::as_str),
                Some("waiting"),
                "tier={tier}"
            );
            assert_eq!(session.messages.len(), 2, "tier={tier}");
        }

        let runtime_json =
            std::fs::read_to_string(temp.path().join("sessions").join(id).join("runtime.json"))
                .unwrap();
        assert!(!runtime_json.contains("CACHE_NATIVE_PAYLOAD_SENTINEL"));

        // Prove the cache projection cannot turn a runtime-only update into a
        // later durable loss when that cached value becomes a full checkpoint.
        let mut cache_writer = cached;
        bamboo_domain::RuntimeSessionPersistence::checkpoint_runtime_session(
            &repo,
            &mut cache_writer,
        )
        .await
        .unwrap();
        let restarted = storage.load_session(id).await.unwrap().unwrap();
        assert_eq!(restarted.provider_transcript, expected_native);
    }

    #[tokio::test]
    async fn narrow_runtime_metadata_transaction_preserves_live_and_durable_non_owned_state() {
        let storage: Arc<dyn Storage> = Arc::new(MapStorage::default());
        let repo = test_repo(storage.clone());
        let id = "narrow-metadata";
        let mut durable = Session::new(id, "durable-model");
        durable.add_message(bamboo_agent_core::Message::user("durable user turn"));
        durable
            .metadata
            .insert("external.durable".to_string(), "keep".to_string());
        storage.save_session(&durable).await.expect("seed durable");

        let mut live = durable.clone();
        live.add_message(bamboo_agent_core::Message::assistant(
            "in-flight assistant tool call",
            None,
        ));
        live.model = "live-model".to_string();
        live.metadata
            .insert("external.live".to_string(), "keep".to_string());
        cache_put(&repo, &live);

        repo.update_runtime_session(id, &["workflow.owned"], |latest| {
            latest
                .metadata
                .insert("workflow.owned".to_string(), "active".to_string());
        })
        .await
        .expect("transaction")
        .expect("session exists");

        let saved = storage
            .load_session(id)
            .await
            .expect("load durable")
            .expect("durable exists");
        assert_eq!(
            saved.messages.len(),
            1,
            "transaction never writes stale live messages"
        );
        assert_eq!(
            saved.metadata.get("external.durable").map(String::as_str),
            Some("keep")
        );
        assert_eq!(
            saved.metadata.get("workflow.owned").map(String::as_str),
            Some("active")
        );

        let cached = read_cached_session(repo.cache(), id).expect("live cache");
        assert_eq!(
            cached.messages.len(),
            2,
            "cache live tool call is not replaced"
        );
        assert_eq!(cached.model, "live-model");
        assert_eq!(
            cached.metadata.get("external.live").map(String::as_str),
            Some("keep")
        );
        assert_eq!(
            cached.metadata.get("workflow.owned").map(String::as_str),
            Some("active")
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

    #[tokio::test]
    async fn runtime_publish_refreshes_cache_even_when_storage_fails() {
        let id = "runtime-selection";
        let mut previous = Session::new(id.to_string(), "m");
        previous.metadata.insert(
            "skill_runtime_selected_skill_ids".to_string(),
            "[\"plan\"]".to_string(),
        );
        let storage: Arc<dyn Storage> = Arc::new(FailingSaveStorage {
            persisted: Mutex::new(Some(previous.clone())),
        });
        let repo = test_repo(storage.clone());
        cache_put(&repo, &previous);

        let mut current = previous.clone();
        current.metadata.insert(
            "skill_runtime_selected_skill_ids".to_string(),
            "[\"review\"]".to_string(),
        );
        current.updated_at = Utc::now();

        let result =
            bamboo_domain::RuntimeSessionPersistence::save_runtime_session(&repo, &mut current)
                .await;
        assert!(result.is_err(), "durable failure must still be surfaced");

        let cached = repo.load(id).await.expect("cached current session");
        assert_eq!(
            cached
                .metadata
                .get("skill_runtime_selected_skill_ids")
                .map(String::as_str),
            Some("[\"review\"]")
        );
        let allowlist = bamboo_skills::access_control::extract_skill_allowlist(&cached.metadata)
            .expect("runtime authorization allowlist");
        assert!(allowlist.contains("review"));
        assert!(!allowlist.contains("plan"));
        let durable = storage
            .load_session(id)
            .await
            .expect("load durable state")
            .expect("previous durable session");
        assert_eq!(
            durable
                .metadata
                .get("skill_runtime_selected_skill_ids")
                .map(String::as_str),
            Some("[\"plan\"]")
        );
    }

    #[tokio::test]
    async fn inherent_save_leaves_existing_cache_untouched_when_storage_fails() {
        let id = "inherent-save-failure";
        let previous = Session::new(id, "previous");
        let storage: Arc<dyn Storage> = Arc::new(FailingSaveStorage {
            persisted: Mutex::new(Some(previous.clone())),
        });
        let repo = test_repo(storage);
        cache_put(&repo, &previous);

        let mut current = previous.clone();
        current.model = "current".to_string();
        assert!(repo.save(&mut current).await.is_err());
        assert_eq!(
            read_cached_session(repo.cache(), id)
                .expect("existing cache")
                .model,
            "previous",
            "fallible inherent save must publish only after a durable commit"
        );
    }

    #[tokio::test]
    async fn save_and_cache_still_refreshes_cache_when_storage_fails() {
        let id = "save-and-cache-failure";
        let previous = Session::new(id, "previous");
        let storage: Arc<dyn Storage> = Arc::new(FailingSaveStorage {
            persisted: Mutex::new(Some(previous.clone())),
        });
        let repo = test_repo(storage);
        cache_put(&repo, &previous);

        let mut current = previous;
        current.model = "current".to_string();
        repo.save_and_cache(&mut current).await;
        assert_eq!(
            read_cached_session(repo.cache(), id)
                .expect("refreshed cache")
                .model,
            "current",
            "fire-and-forget save must retain its existing cache-on-failure behavior"
        );
    }

    #[tokio::test]
    async fn checkpoint_leaves_existing_cache_untouched_when_storage_fails() {
        let id = "checkpoint-failure";
        let previous = Session::new(id, "previous");
        let storage: Arc<dyn Storage> = Arc::new(FailingSaveStorage {
            persisted: Mutex::new(Some(previous.clone())),
        });
        let repo = test_repo(storage);
        cache_put(&repo, &previous);

        let mut current = previous.clone();
        current.model = "current".to_string();
        let result = bamboo_domain::RuntimeSessionPersistence::checkpoint_runtime_session(
            &repo,
            &mut current,
        )
        .await;
        assert!(result.is_err());
        assert_eq!(
            read_cached_session(repo.cache(), id)
                .expect("existing cache")
                .model,
            "previous",
            "checkpoint must publish only after a durable commit"
        );
    }

    #[tokio::test]
    async fn legacy_clear_uses_durable_cas_and_never_erases_concurrent_append_from_stale_cache() {
        let storage: Arc<dyn Storage> = Arc::new(MapStorage::default());
        let repo = test_repo(storage.clone());
        let id = "legacy-cas-race";
        let expected = vec![serde_json::json!({"content": "first"})];

        let mut stale_cache = Session::new(id, "m");
        stale_cache.set_pending_injected_messages(expected.clone());
        cache_put(&repo, &stale_cache);

        let mut durable = stale_cache.clone();
        durable.set_pending_injected_messages(vec![
            serde_json::json!({"content": "first"}),
            serde_json::json!({"content": "concurrent"}),
        ]);
        storage.save_session(&durable).await.unwrap();

        let cleared = bamboo_domain::RuntimeSessionPersistence::clear_legacy_pending_messages(
            &repo, id, &expected,
        )
        .await
        .unwrap();
        assert!(!cleared, "the durable compare-and-clear must reject drift");
        assert_eq!(
            storage
                .load_session(id)
                .await
                .unwrap()
                .unwrap()
                .pending_injected_messages()
                .unwrap(),
            durable.pending_injected_messages().unwrap(),
            "the concurrent durable append must remain intact"
        );
        assert_eq!(
            read_cached_session(repo.cache(), id)
                .unwrap()
                .pending_injected_messages()
                .unwrap(),
            expected,
            "a failed CAS must not mutate the existing cache"
        );
    }

    #[tokio::test]
    async fn successful_legacy_clear_refreshes_stale_cache_from_durable_state() {
        let storage: Arc<dyn Storage> = Arc::new(MapStorage::default());
        let repo = test_repo(storage.clone());
        let id = "legacy-cas-success";
        let expected = vec![serde_json::json!({"content": "first"})];

        let mut stale_cache = Session::new(id, "stale-model");
        stale_cache.set_pending_injected_messages(expected.clone());
        cache_put(&repo, &stale_cache);

        let mut durable = Session::new(id, "durable-model");
        durable.set_pending_injected_messages(expected.clone());
        durable
            .metadata
            .insert("durable-only".to_string(), "keep".to_string());
        storage.save_session(&durable).await.unwrap();

        assert!(
            bamboo_domain::RuntimeSessionPersistence::clear_legacy_pending_messages(
                &repo, id, &expected,
            )
            .await
            .unwrap()
        );
        let cached = read_cached_session(repo.cache(), id).unwrap();
        assert!(!cached.has_pending_injected_messages());
        assert_eq!(cached.model, "durable-model");
        assert_eq!(
            cached.metadata.get("durable-only").map(String::as_str),
            Some("keep")
        );
    }
}
