use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Instant;

use bamboo_config::ContextManagementStrategy;
use bamboo_domain::Session;
use parking_lot::Mutex;
use serde::Serialize;

#[derive(Clone, Copy, Serialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub(crate) enum CompactionPhase {
    Preparing,
    Summarizing { part: usize, total: usize },
    Merging { part: usize, total: usize },
    Validating,
    Saving,
}

#[derive(Serialize)]
pub struct ContextCompactionSnapshot {
    pub id: String,
    pub strategy: Option<ContextManagementStrategy>,
    pub elapsed_ms: u64,
    #[serde(flatten)]
    phase: CompactionPhase,
    #[serde(skip)]
    created_at: chrono::DateTime<chrono::Utc>,
    #[serde(skip)]
    conversation_revision: u64,
}

impl ContextCompactionSnapshot {
    /// The disk read may have observed a reset or a commit after the live read.
    pub fn is_active_for(&self, session: &Session) -> bool {
        self.created_at == session.created_at
            && self.conversation_revision == session.conversation_revision()
            && !session
                .compression_events
                .iter()
                .any(|event| event.id == self.id)
    }
}

// This observes work owned by the normal execution/compaction writer. It never
// authorizes a write. An attempt lives as long as its owning future.
#[derive(Clone, Default)]
pub(crate) struct ContextCompactions(Arc<Mutex<HashMap<String, Weak<ContextCompaction>>>>);

impl ContextCompactions {
    pub(crate) fn begin(&self, session: &Session) -> Arc<ContextCompaction> {
        let attempt = Arc::new(ContextCompaction {
            id: uuid::Uuid::new_v4().to_string(),
            session_id: session.id.clone(),
            created_at: session.created_at,
            conversation_revision: session.conversation_revision(),
            started: Instant::now(),
            state: Mutex::new((None, CompactionPhase::Preparing)),
            registry: self.clone(),
        });
        self.0
            .lock()
            .insert(session.id.clone(), Arc::downgrade(&attempt));
        attempt
    }

    pub(crate) fn current(&self, id: &str) -> Option<Arc<ContextCompaction>> {
        self.0.lock().get(id).and_then(Weak::upgrade)
    }

    pub(crate) fn snapshot(&self, id: &str) -> Option<ContextCompactionSnapshot> {
        let attempt = self.current(id)?;
        let (strategy, phase) = *attempt.state.lock();
        Some(ContextCompactionSnapshot {
            id: attempt.id.clone(),
            strategy,
            elapsed_ms: attempt.started.elapsed().as_millis().min(u64::MAX as u128) as u64,
            phase,
            created_at: attempt.created_at,
            conversation_revision: attempt.conversation_revision,
        })
    }
}

pub struct ContextCompaction {
    pub(crate) id: String,
    session_id: String,
    created_at: chrono::DateTime<chrono::Utc>,
    conversation_revision: u64,
    started: Instant,
    state: Mutex<(Option<ContextManagementStrategy>, CompactionPhase)>,
    registry: ContextCompactions,
}

impl ContextCompaction {
    pub(crate) fn select_strategy(&self, strategy: ContextManagementStrategy) {
        self.state.lock().0 = Some(strategy);
    }

    pub(crate) fn set_phase(&self, phase: CompactionPhase) {
        self.state.lock().1 = phase;
    }

    pub fn saving(&self) {
        self.set_phase(CompactionPhase::Saving);
    }
}

impl Drop for ContextCompaction {
    fn drop(&mut self) {
        let mut active = self.registry.0.lock();
        if active
            .get(&self.session_id)
            .is_some_and(|current| std::ptr::eq(current.as_ptr(), self))
        {
            active.remove(&self.session_id);
        }
    }
}
