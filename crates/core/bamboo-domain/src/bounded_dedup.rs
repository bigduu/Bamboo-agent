//! Dependency-light, bounded fingerprint de-duplication.
//!
//! The set stores only fixed-size hashes rather than raw keys. Callers can use
//! it for process-local diagnostics without retaining paths, provider errors,
//! or other potentially sensitive values indefinitely.

use std::collections::{hash_map::DefaultHasher, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::Mutex;

/// Default capacity for small process-local diagnostic fingerprint sets.
pub const DEFAULT_BOUNDED_FINGERPRINT_CAPACITY: usize = 1024;

#[derive(Debug, Default)]
struct State {
    seen: HashSet<u64>,
    insertion_order: VecDeque<u64>,
}

/// A thread-safe FIFO-bounded set of hashed `(key, value)` pairs.
///
/// [`Self::insert_if_new`] returns `true` for a pair's first retained
/// observation and `false` for repeats. Once capacity is reached, inserting a
/// new pair evicts the oldest fingerprint. A zero-capacity set retains nothing
/// and therefore always reports an insertion as new.
#[derive(Debug)]
pub struct BoundedFingerprintSet {
    capacity: usize,
    state: Mutex<State>,
}

impl BoundedFingerprintSet {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            state: Mutex::new(State::default()),
        }
    }

    /// Insert the fingerprint of `(key, value)`, returning whether it was new.
    pub fn insert_if_new<K, V>(&self, key: &K, value: &V) -> bool
    where
        K: Hash + ?Sized,
        V: Hash + ?Sized,
    {
        if self.capacity == 0 {
            return true;
        }

        let fingerprint = fingerprint(key, value);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.seen.contains(&fingerprint) {
            return false;
        }

        if state.seen.len() == self.capacity {
            if let Some(evicted) = state.insertion_order.pop_front() {
                state.seen.remove(&evicted);
            }
        }
        state.seen.insert(fingerprint);
        state.insertion_order.push_back(fingerprint);
        true
    }
}

fn fingerprint<K, V>(key: &K, value: &V) -> u64
where
    K: Hash + ?Sized,
    V: Hash + ?Sized,
{
    let mut hasher = DefaultHasher::new();
    // Domain separators avoid ambiguous concatenations across key/value types.
    0x6b65_792d_7631_u64.hash(&mut hasher);
    key.hash(&mut hasher);
    0x7661_6c2d_7631_u64.hash(&mut hasher);
    value.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};

    #[test]
    fn tracks_duplicate_changed_key_changed_value_and_fifo_eviction() {
        let deduper = BoundedFingerprintSet::new(2);
        assert!(deduper.insert_if_new("skill-a", "parse-error-a"));
        assert!(!deduper.insert_if_new("skill-a", "parse-error-a"));
        assert!(deduper.insert_if_new("skill-b", "parse-error-a"));
        assert!(deduper.insert_if_new("skill-a", "parse-error-b"));
        // The original pair was the oldest and was evicted to keep capacity 2.
        assert!(deduper.insert_if_new("skill-a", "parse-error-a"));
    }

    #[test]
    fn zero_capacity_never_suppresses_an_observation() {
        let deduper = BoundedFingerprintSet::new(0);
        assert!(deduper.insert_if_new("key", "value"));
        assert!(deduper.insert_if_new("key", "value"));
    }

    #[test]
    fn allows_exactly_one_concurrent_first_observation() {
        const THREADS: usize = 16;
        let deduper = Arc::new(BoundedFingerprintSet::new(8));
        let barrier = Arc::new(Barrier::new(THREADS));
        let first_count = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();

        for _ in 0..THREADS {
            let deduper = deduper.clone();
            let barrier = barrier.clone();
            let first_count = first_count.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                if deduper.insert_if_new("same-key", "same-value") {
                    first_count.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for handle in handles {
            handle.join().expect("dedup worker");
        }

        assert_eq!(first_count.load(Ordering::Relaxed), 1);
    }
}
