//! Local poison-recovery for this crate's `std::sync` locks.
//!
//! A mirror of `bamboo_domain::poison`, kept local because `bamboo-subagent` is
//! a deliberately dependency-light leaf crate (the sub-process worker) — it has
//! no internal crate deps and no `tracing`. When a thread panics while holding a
//! `std::sync` lock, the lock is poisoned and every later `.lock()` returns
//! `Err`; propagating that with `.unwrap()`/`.expect()` would cascade one
//! thread's panic into the worker's whole reply-routing path. Recovering via
//! [`PoisonError::into_inner`] keeps it alive with the last-good data. The first
//! recovery is reported once to stderr (captured by the parent) without flooding
//! the hot path — `into_inner` never clears the poison. See #92.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::PoisonError;

/// Whether the process has already reported a poison recovery.
static WARNED: AtomicBool = AtomicBool::new(false);

fn warn_once() {
    if !WARNED.swap(true, Ordering::Relaxed) {
        eprintln!(
            "bamboo-subagent: recovered from a poisoned std lock (a thread panicked \
             while holding it); continuing with the last-good data. Further recoveries \
             this process are silenced."
        );
    }
}

/// Extension on `std::sync` lock results that recovers from poisoning instead of
/// panicking. Local mirror of `bamboo_domain::poison::PoisonRecover`.
pub(crate) trait PoisonRecover<G> {
    /// Return the guard, recovering (and reporting once) if the lock was poisoned.
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
    use std::collections::HashMap;
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::Mutex;

    #[test]
    fn recover_poison_returns_data_after_the_mutex_is_poisoned() {
        // Shaped like the transport pending-replies map (a HashMap behind a std
        // Mutex): poisoning it must not brick subsequent reply delivery.
        let pending: Mutex<HashMap<String, u32>> =
            Mutex::new(HashMap::from([("req-1".to_string(), 42)]));

        let poisoned = catch_unwind(AssertUnwindSafe(|| {
            let _guard = pending.lock().unwrap();
            panic!("intentional poison for test");
        }));
        assert!(poisoned.is_err(), "setup panic should have been caught");
        assert!(pending.lock().is_err(), "the mutex should now be poisoned");

        // A subsequent op returns the data instead of panicking.
        assert_eq!(pending.lock().recover_poison().remove("req-1"), Some(42));
    }
}
