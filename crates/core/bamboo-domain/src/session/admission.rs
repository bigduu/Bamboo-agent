//! A synchronous cancellation fence for a single child delivery.
//!
//! Preparation may await freely. The actual queue send or filesystem rename
//! runs inside `commit`, under the same lock used by the caller's cancellation
//! guard. A cancelled caller therefore cannot race a late delivery commit.

use std::future::poll_fn;
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Pending,
    Cancelled,
    Committed,
}

#[derive(Debug, PartialEq, Eq)]
pub enum AdmissionCommit<T> {
    Committed(T),
    AlreadyCommitted,
    Cancelled,
}

#[derive(Debug, Default)]
struct AdmissionState {
    state: State,
    cancellation_waiters: Vec<Waker>,
}

#[derive(Debug, Clone, Default)]
pub struct AdmissionGate(Arc<Mutex<AdmissionState>>);

impl Default for State {
    fn default() -> Self {
        Self::Pending
    }
}

impl AdmissionGate {
    pub fn cancel_if_pending(&self) {
        let mut inner = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if inner.state == State::Pending {
            inner.state = State::Cancelled;
            let waiters = std::mem::take(&mut inner.cancellation_waiters);
            drop(inner);
            for waiter in waiters {
                waiter.wake();
            }
        }
    }

    /// Wake a pending admission owner as soon as cancellation wins. The
    /// registration and state check share the commit/cancel mutex, so there is
    /// no lost wakeup between a caller dropping and an owner parking.
    pub async fn cancelled(&self) {
        poll_fn(|cx| {
            let mut inner = self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if inner.state == State::Cancelled {
                Poll::Ready(())
            } else {
                if !inner
                    .cancellation_waiters
                    .iter()
                    .any(|waiter| waiter.will_wake(cx.waker()))
                {
                    inner.cancellation_waiters.push(cx.waker().clone());
                }
                Poll::Pending
            }
        })
        .await
    }

    pub fn is_cancelled(&self) -> bool {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .state
            == State::Cancelled
    }

    pub fn is_committed(&self) -> bool {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .state
            == State::Committed
    }

    /// Runs a synchronous, non-yielding admission commit. An error leaves the
    /// gate pending so cancellation can still win a subsequent retry.
    pub fn commit<T, E>(
        &self,
        commit: impl FnOnce() -> Result<T, E>,
    ) -> Result<AdmissionCommit<T>, E> {
        let mut inner = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match inner.state {
            State::Cancelled => Ok(AdmissionCommit::Cancelled),
            State::Committed => Ok(AdmissionCommit::AlreadyCommitted),
            State::Pending => {
                let value = commit()?;
                inner.state = State::Committed;
                inner.cancellation_waiters.clear();
                Ok(AdmissionCommit::Committed(value))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_and_commit_are_linearized_and_idempotent() {
        let gate = AdmissionGate::default();
        gate.cancel_if_pending();
        assert_eq!(
            gate.commit(|| Ok::<_, ()>(1)),
            Ok(AdmissionCommit::Cancelled)
        );

        let gate = AdmissionGate::default();
        assert_eq!(
            gate.commit(|| Ok::<_, ()>(1)),
            Ok(AdmissionCommit::Committed(1))
        );
        gate.cancel_if_pending();
        assert_eq!(
            gate.commit(|| Ok::<_, ()>(2)),
            Ok(AdmissionCommit::AlreadyCommitted)
        );
    }

    #[test]
    fn failed_commit_remains_cancellable() {
        let gate = AdmissionGate::default();
        assert_eq!(gate.commit(|| Err::<(), _>("failed")), Err("failed"));
        gate.cancel_if_pending();
        assert!(gate.is_cancelled());
    }
}
