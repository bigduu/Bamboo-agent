//! Recovery for poisoned `std::sync` locks.
//!
//! When a thread panics while holding a `std::sync::Mutex`/`RwLock`, the lock is
//! *poisoned*: every later `.lock()/.read()/.write()` returns `Err(PoisonError)`.
//! Propagating that with `.unwrap()`/`.expect()` turns one thread's panic into a
//! cascade that bricks the whole subsystem (every future caller panics too).
//!
//! [`PoisonRecover::recover_poison`] instead takes the still-usable guard via
//! [`PoisonError::into_inner`] and carries on with the last-good data. Because
//! `into_inner` does **not** clear the poison, a naive log in the recovery path
//! would fire on *every* subsequent access and flood the hot path — so the first
//! recovery is logged exactly once per process and the rest are silent.
//!
//! Follow-up to #21; see #92.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::PoisonError;

/// Whether the process has already logged a poison recovery.
static WARNED: AtomicBool = AtomicBool::new(false);

fn warn_once() {
    if !WARNED.swap(true, Ordering::Relaxed) {
        tracing::warn!(
            target: "bamboo::lock_poison",
            "recovered from a poisoned std lock (a thread panicked while holding it); \
             continuing with the last-good data. Further recoveries this process are silenced."
        );
    }
}

/// Extension on `std::sync` lock results that recovers from poisoning instead of
/// panicking. Use it in place of `.unwrap()`, `.expect(..)`, or
/// `.unwrap_or_else(|e| e.into_inner())` on a `Mutex`/`RwLock` guard.
pub trait PoisonRecover<G> {
    /// Return the guard, recovering (and logging once) if the lock was poisoned.
    fn recover_poison(self) -> G;
}

impl<G> PoisonRecover<G> for Result<G, PoisonError<G>> {
    fn recover_poison(self) -> G {
        match self {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn_once();
                poisoned.into_inner()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::{Mutex, RwLock};

    #[test]
    fn recover_poison_returns_data_after_a_mutex_is_poisoned() {
        let lock = Mutex::new(vec![1, 2, 3]);
        // Poison the mutex by panicking while a guard is held.
        let poisoned = catch_unwind(AssertUnwindSafe(|| {
            let _guard = lock.lock().unwrap();
            panic!("intentional poison for test");
        }));
        assert!(poisoned.is_err(), "setup panic should have been caught");

        // A subsequent lock is Err(poisoned), but recover_poison yields the data.
        assert!(lock.lock().is_err(), "the mutex should now be poisoned");
        assert_eq!(&*lock.lock().recover_poison(), &[1, 2, 3]);
    }

    #[test]
    fn recover_poison_returns_data_after_an_rwlock_is_poisoned() {
        let lock = RwLock::new(7u32);
        let poisoned = catch_unwind(AssertUnwindSafe(|| {
            let _guard = lock.write().unwrap();
            panic!("intentional poison for test");
        }));
        assert!(poisoned.is_err());

        assert_eq!(*lock.read().recover_poison(), 7);
        *lock.write().recover_poison() = 9;
        assert_eq!(*lock.read().recover_poison(), 9);
    }
}
