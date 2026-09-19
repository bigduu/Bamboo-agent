use std::collections::VecDeque;
use std::sync::{Mutex, TryLockError};

use thiserror::Error;

use crate::{ToolEventBuildError, ToolEventV1};

/// Non-blocking injection seam owned by one Bamboo runtime/AppState.
///
/// Implementations MUST return immediately and MUST NOT perform process I/O.
/// Runtime routing and queues are intentionally outside this protocol slice.
pub trait ToolEventPublisher: Send + Sync {
    /// Fast capability hint so the default no-op path performs no event DTO
    /// allocation. Implementations should keep the default `true`.
    fn is_enabled(&self) -> bool {
        true
    }

    fn try_publish(&self, event: ToolEventV1) -> Result<(), ToolEventPublishError>;
}

/// Default publisher used by the SDK, server, and tests unless explicitly
/// injected. It preserves historical behavior and allocations at the sink.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopToolEventPublisher;

impl ToolEventPublisher for NoopToolEventPublisher {
    fn is_enabled(&self) -> bool {
        false
    }

    fn try_publish(&self, _event: ToolEventV1) -> Result<(), ToolEventPublishError> {
        Ok(())
    }
}

/// Bounded in-memory recorder for tests and embedders.
///
/// Both publication and observation use `Mutex::try_lock`; contention is a
/// drop/error signal and can never park the executing tool thread.
#[derive(Debug)]
pub struct InMemoryToolEventRecorder {
    capacity: usize,
    events: Mutex<VecDeque<ToolEventV1>>,
}

impl InMemoryToolEventRecorder {
    pub fn new(capacity: usize) -> Result<Self, ToolEventPublishError> {
        if capacity == 0 {
            return Err(ToolEventPublishError::InvalidCapacity);
        }
        Ok(Self {
            capacity,
            // Capacity is a logical bound. Avoid eagerly allocating a caller-
            // supplied capacity before the first bounded event arrives.
            events: Mutex::new(VecDeque::new()),
        })
    }

    pub fn try_snapshot(&self) -> Result<Vec<ToolEventV1>, ToolEventPublishError> {
        let events = self.try_lock()?;
        Ok(events.iter().cloned().collect())
    }

    pub fn try_drain(&self) -> Result<Vec<ToolEventV1>, ToolEventPublishError> {
        let mut events = self.try_lock()?;
        Ok(events.drain(..).collect())
    }

    fn try_lock(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, VecDeque<ToolEventV1>>, ToolEventPublishError> {
        match self.events.try_lock() {
            Ok(events) => Ok(events),
            Err(TryLockError::WouldBlock) => Err(ToolEventPublishError::Busy),
            Err(TryLockError::Poisoned(_)) => Err(ToolEventPublishError::Poisoned),
        }
    }
}

impl ToolEventPublisher for InMemoryToolEventRecorder {
    fn try_publish(&self, event: ToolEventV1) -> Result<(), ToolEventPublishError> {
        event
            .validate_bounds()
            .map_err(ToolEventPublishError::InvalidEvent)?;
        let mut events = self.try_lock()?;
        if events.len() >= self.capacity {
            return Err(ToolEventPublishError::Full {
                capacity: self.capacity,
            });
        }
        events.push_back(event);
        Ok(())
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ToolEventPublishError {
    #[error("tool event recorder capacity must be greater than zero")]
    InvalidCapacity,
    #[error("tool event publisher is busy")]
    Busy,
    #[error("tool event publisher is full (capacity {capacity})")]
    Full { capacity: usize },
    #[error("tool event publisher state is poisoned")]
    Poisoned,
    #[error("invalid tool event: {0}")]
    InvalidEvent(ToolEventBuildError),
    #[error("tool event publisher failed: {0}")]
    Failed(String),
}

#[cfg(test)]
mod tests {
    use crate::{FileChangedV1, ToolEventContextV1};

    use super::*;

    fn event(call_id: &str) -> ToolEventV1 {
        ToolEventV1::file_changed(
            ToolEventContextV1::bounded("session", "root-session", "Write", call_id).unwrap(),
            FileChangedV1::bounded("/root/file.rs").unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn recorder_is_bounded() {
        let recorder = InMemoryToolEventRecorder::new(1).unwrap();
        recorder.try_publish(event("one")).unwrap();
        assert_eq!(
            recorder.try_publish(event("two")),
            Err(ToolEventPublishError::Full { capacity: 1 })
        );
        assert_eq!(recorder.try_snapshot().unwrap().len(), 1);
    }

    #[test]
    fn contended_recorder_returns_without_waiting() {
        let recorder = InMemoryToolEventRecorder::new(1).unwrap();
        let _guard = recorder.events.lock().unwrap();
        assert_eq!(
            recorder.try_publish(event("busy")),
            Err(ToolEventPublishError::Busy)
        );
    }

    #[test]
    fn poisoned_recorder_returns_an_explicit_error() {
        let recorder = std::sync::Arc::new(InMemoryToolEventRecorder::new(1).unwrap());
        let poison_target = recorder.clone();
        let poisoned = std::thread::spawn(move || {
            let _guard = poison_target.events.lock().unwrap();
            panic!("poison recorder for deterministic coverage");
        });
        assert!(poisoned.join().is_err());

        assert_eq!(
            recorder.try_publish(event("poisoned")),
            Err(ToolEventPublishError::Poisoned)
        );
    }
}
