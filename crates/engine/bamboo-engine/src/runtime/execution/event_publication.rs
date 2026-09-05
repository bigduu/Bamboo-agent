//! Per-run atomic publication fence. Token traffic never acquires the shared
//! runner registry. Replacement closes the old fence and drains synchronous
//! publications before the successor can emit Started on the same channel.

use chrono::{DateTime, Utc};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

const CLOSED: usize = 1 << (usize::BITS - 1);

#[derive(Debug, Default)]
pub struct EventPublication {
    state: AtomicUsize,
    last_event_millis: AtomicI64,
}

struct PublicationGuard<'a>(&'a AtomicUsize);
impl Drop for PublicationGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Release);
    }
}

impl EventPublication {
    /// Only synchronous sends belong in this closure. An async producer cannot
    /// keep a publication permit across suspension and delay a successor.
    pub fn publish(&self, send: impl FnOnce()) -> bool {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            if state & CLOSED != 0 {
                return false;
            }
            match self.state.compare_exchange_weak(
                state,
                state + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(current) => state = current,
            }
        }
        let _permit = PublicationGuard(&self.state);
        self.touch();
        send();
        true
    }

    pub fn touch(&self) {
        self.last_event_millis
            .fetch_max(Utc::now().timestamp_millis(), Ordering::Relaxed);
    }

    pub fn last_event_at(&self) -> Option<DateTime<Utc>> {
        let millis = self.last_event_millis.load(Ordering::Relaxed);
        (millis != 0)
            .then(|| DateTime::from_timestamp_millis(millis))
            .flatten()
    }

    pub async fn retire(&self) {
        self.state.fetch_or(CLOSED, Ordering::AcqRel);
        while self.state.load(Ordering::Acquire) & !CLOSED != 0 {
            tokio::task::yield_now().await;
        }
    }

    /// Maintenance removes an entry only once admitted publications have exited.
    pub fn retire_if_idle(&self) -> bool {
        self.state.fetch_or(CLOSED, Ordering::AcqRel) & !CLOSED == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn replacement_drains_admitted_frame_and_rejects_late_frames() {
        let gate = Arc::new(EventPublication::default());
        let admitted = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let publisher = {
            let gate = gate.clone();
            let admitted = admitted.clone();
            let release = release.clone();
            std::thread::spawn(move || {
                gate.publish(|| {
                    admitted.wait();
                    release.wait();
                })
            })
        };
        admitted.wait();
        assert!(!gate.retire_if_idle());
        assert!(!gate.publish(|| panic!("old generation published")));
        let retiring = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.retire().await })
        };
        tokio::task::yield_now().await;
        assert!(!retiring.is_finished());
        release.wait();
        assert!(publisher.join().unwrap());
        retiring.await.unwrap();
        assert!(gate.retire_if_idle());
    }

    #[test]
    fn panic_releases_publication_permit() {
        let gate = EventPublication::default();
        let _ = std::panic::catch_unwind(|| gate.publish(|| panic!("sink panic")));
        assert!(gate.retire_if_idle());
    }
}
