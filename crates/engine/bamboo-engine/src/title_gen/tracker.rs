use std::collections::{HashMap, VecDeque};

use bamboo_agent_core::Session;
use parking_lot::Mutex;

use crate::session_app::metadata::TitleGenerationRevision;

const MAX_RECENT_ATTEMPTS: usize = 1024;

/// Host-local deduplication for pending titles. Only evidence fingerprints are
/// retained, and restarting the host permits a new attempt.
#[derive(Default)]
pub struct TitleGenerationTracker {
    state: Mutex<TrackerState>,
}

#[derive(Default)]
struct TrackerState {
    in_flight: HashMap<String, bool>,
    recent_attempts: VecDeque<Attempt>,
}

struct Attempt {
    session_id: String,
    revision: TitleGenerationRevision,
    fingerprint: u64,
}

impl TitleGenerationTracker {
    #[doc(hidden)]
    pub fn acquire(&self, session_id: &str, queue_if_busy: bool) -> bool {
        let mut state = self.state.lock();
        if let Some(queued) = state.in_flight.get_mut(session_id) {
            *queued |= queue_if_busy;
            return false;
        }

        state.in_flight.insert(session_id.to_owned(), false);
        true
    }

    /// Hold the slot across a coalesced trigger. A later trigger can acquire
    /// the slot as soon as the final attempt releases it.
    pub(super) fn finish(&self, session_id: &str) -> bool {
        let mut state = self.state.lock();
        if let Some(queued) = state.in_flight.get_mut(session_id) {
            if std::mem::take(queued) {
                return true;
            }
        }

        state.in_flight.remove(session_id);
        false
    }

    #[doc(hidden)]
    pub fn release(&self, session_id: &str) {
        self.state.lock().in_flight.remove(session_id);
    }

    pub(super) fn record_attempt(&self, session: &Session, fingerprint: u64) -> bool {
        let mut state = self.state.lock();
        if let Some(index) = state
            .recent_attempts
            .iter()
            .position(|attempt| attempt.session_id == session.id)
        {
            let last = &state.recent_attempts[index];
            if last.fingerprint == fingerprint && last.revision.matches(session) {
                return false;
            }
            state.recent_attempts.remove(index);
        }

        if state.recent_attempts.len() == MAX_RECENT_ATTEMPTS {
            state.recent_attempts.pop_front();
        }
        state.recent_attempts.push_back(Attempt {
            session_id: session.id.clone(),
            revision: TitleGenerationRevision::capture(session),
            fingerprint,
        });
        true
    }
}
