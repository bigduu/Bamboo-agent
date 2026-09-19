//! Lock-free session snapshots. Durable read/modify/write transactions remain
//! the responsibility of SessionRepository; readers never join their queue.

use std::ops::Deref;
use std::sync::Arc;

use arc_swap::ArcSwap;
use bamboo_agent_core::Session;
use crossbeam_skiplist::SkipMap;

pub type SessionCache = Arc<SessionCacheMap>;

/// An immutable version can remain in use while a writer publishes its successor.
pub struct SessionSnapshot(ArcSwap<Session>);

impl SessionSnapshot {
    pub fn new(session: Session) -> Self {
        Self(ArcSwap::from_pointee(session))
    }

    pub fn read(&self) -> SessionRead {
        SessionRead(self.0.load_full())
    }

    /// Apply a narrow patch to the latest version. The closure may be retried
    /// after a concurrent publication, so it must have no external side effects.
    pub fn update(&self, mutate: impl Fn(&mut Session)) {
        self.0.rcu(|current| {
            let mut next = (**current).clone();
            mutate(&mut next);
            next
        });
    }
}

/// Deliberately does not implement Clone: `read().clone()` clones the Session,
/// preserving the existing detached-snapshot read contract.
pub struct SessionRead(Arc<Session>);

impl Deref for SessionRead {
    type Target = Session;
    fn deref(&self) -> &Session {
        &self.0
    }
}

/// Both the index and the values use atomic publication. Unlike a DashMap
/// guard, a returned entry does not hold a shard lock while cloning a transcript.
#[derive(Default)]
pub struct SessionCacheMap {
    entries: SkipMap<String, Arc<SessionSnapshot>>,
}

pub struct SessionCacheEntry {
    key: String,
    value: Arc<SessionSnapshot>,
}

impl SessionCacheEntry {
    pub fn key(&self) -> &String {
        &self.key
    }
    pub fn value(&self) -> &Arc<SessionSnapshot> {
        &self.value
    }
}

impl Deref for SessionCacheEntry {
    type Target = Arc<SessionSnapshot>;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl SessionCacheMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, id: String, snapshot: Arc<SessionSnapshot>) {
        // Keep the slot stable until eviction. A concurrent narrow update must
        // retry against this publication, not succeed on an orphaned old Arc.
        let entry = self.entries.get_or_insert(id, snapshot.clone());
        if !Arc::ptr_eq(entry.value(), &snapshot) {
            entry.value().0.store(snapshot.0.load_full());
        }
    }

    pub fn get(&self, id: &str) -> Option<SessionCacheEntry> {
        self.entries.get(id).map(|entry| SessionCacheEntry {
            key: entry.key().clone(),
            value: entry.value().clone(),
        })
    }

    pub fn remove(&self, id: &str) -> Option<(String, Arc<SessionSnapshot>)> {
        self.entries
            .remove(id)
            .map(|entry| (entry.key().clone(), entry.value().clone()))
    }

    pub fn iter(&self) -> impl Iterator<Item = SessionCacheEntry> + '_ {
        self.entries.iter().map(|entry| SessionCacheEntry {
            key: entry.key().clone(),
            value: entry.value().clone(),
        })
    }

    pub fn contains_key(&self, id: &str) -> bool {
        self.entries.contains_key(id)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn clear(&self) {
        self.entries.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_reader_does_not_block_publication_or_change_its_version() {
        let snapshot = SessionSnapshot::new(Session::new("s", "model"));
        let previous = snapshot.read();
        snapshot.update(|session| {
            session.metadata.insert("committed".into(), "yes".into());
        });
        assert!(!previous.metadata.contains_key("committed"));
        assert_eq!(snapshot.read().metadata["committed"], "yes");
    }

    #[test]
    fn concurrent_narrow_updates_preserve_every_writer() {
        let snapshot = SessionSnapshot::new(Session::new("shared-root", "model"));
        std::thread::scope(|scope| {
            for worker in 0..16 {
                let snapshot = &snapshot;
                scope.spawn(move || {
                    for item in 0..32 {
                        snapshot.update(|session| {
                            session
                                .metadata
                                .insert(format!("{worker}/{item}"), "yes".into());
                        });
                    }
                });
            }
        });
        assert_eq!(snapshot.read().metadata.len(), 512);
    }

    #[test]
    fn narrow_update_retries_when_full_snapshot_is_published_to_same_slot() {
        use std::sync::{
            atomic::{AtomicBool, Ordering},
            Barrier,
        };
        let cache = SessionCacheMap::new();
        cache.insert(
            "root".into(),
            Arc::new(SessionSnapshot::new(Session::new("root", "old"))),
        );
        let entered = Barrier::new(2);
        let release = Barrier::new(2);
        std::thread::scope(|scope| {
            let updater = scope.spawn(|| {
                let first = AtomicBool::new(true);
                cache.get("root").unwrap().update(|session| {
                    // Deterministic test seam: stop only the first CAS attempt.
                    if first.swap(false, Ordering::SeqCst) {
                        entered.wait();
                        release.wait();
                    }
                    session
                        .metadata
                        .insert("narrow-patch".into(), "kept".into());
                });
            });
            entered.wait();
            cache.insert(
                "root".into(),
                Arc::new(SessionSnapshot::new(Session::new("root", "new"))),
            );
            release.wait();
            updater.join().unwrap();
        });
        let actual = cache.get("root").unwrap().read();
        assert_eq!(actual.model, "new");
        assert_eq!(actual.metadata["narrow-patch"], "kept");
    }

    #[test]
    fn hundreds_of_independent_sessions_publish_and_remove_with_retained_reads() {
        let cache = SessionCacheMap::new();
        std::thread::scope(|scope| {
            for worker in 0..16 {
                let cache = &cache;
                scope.spawn(move || {
                    for item in 0..32 {
                        let id = format!("child-{worker}-{item}");
                        cache.insert(
                            id.clone(),
                            Arc::new(SessionSnapshot::new(Session::new(&id, "m"))),
                        );
                        let held = cache.get(&id).unwrap();
                        let old = held.read();
                        held.update(|session| {
                            session.metadata.insert("done".into(), "yes".into());
                        });
                        assert_eq!(cache.get(&id).unwrap().read().metadata["done"], "yes");
                        cache.remove(&id).unwrap();
                        assert!(cache.get(&id).is_none());
                        assert!(!old.metadata.contains_key("done"));
                    }
                });
            }
        });
        assert!(cache.is_empty());
    }
}
