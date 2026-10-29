//! Private-origin, one-shot continuation of an active provider request.
//!
//! This is a process-local capability. It never represents a finished run,
//! never carries request bytes or credentials and cannot be deserialized.
use std::io;
use std::time::Duration;

use bamboo_domain::{AdmissionGate, LiveBootstrapProof};
use tokio::sync::oneshot;
use tokio::time::Instant;

use super::{backoff_delay, RetryConfig};

// Bound the time for a person to decide. Once accepted, the normal continuous
// retry policy remains active until the exact execution is cancelled.
pub const LIVE_BOOTSTRAP_OFFER_TTL: Duration = Duration::from_secs(10 * 60);

/// Created only by the active Anthropic managed-send path. The sender can be
/// consumed once and is bound to the same gate used by the actor checkpoint.
pub struct LiveBootstrapOffer {
    gate: AdmissionGate,
    deadline: Instant,
    decision: Option<oneshot::Sender<AdmissionGate>>,
}

impl std::fmt::Debug for LiveBootstrapOffer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LiveBootstrapOffer")
            .finish_non_exhaustive()
    }
}

impl LiveBootstrapOffer {
    /// The gate is used only to bind the exact actor checkpoint.
    pub fn gate(&self) -> AdmissionGate {
        self.gate.clone()
    }

    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    pub fn matches_proof(&self, proof: &LiveBootstrapProof) -> bool {
        proof.matches_gate(&self.gate)
    }

    pub fn is_live(&self) -> bool {
        Instant::now() < self.deadline
            && !self.gate.is_cancelled()
            && self
                .decision
                .as_ref()
                .is_some_and(|sender| !sender.is_closed())
    }

    /// Call only as the final action inside the actor's gate commit callback.
    /// Do not lock the gate here: the callback already holds it. The receiver
    /// checks the committed gate before it can send the next HTTP request.
    pub fn approve(mut self, proof: LiveBootstrapProof) -> io::Result<()> {
        let sender = self
            .decision
            .take()
            .ok_or_else(|| io::Error::other("live bootstrap offer was consumed"))?;
        if !proof.matches_gate(&self.gate) || Instant::now() >= self.deadline {
            return Err(io::Error::other("live bootstrap offer expired or changed"));
        }
        // The receiver only needs the gate identity and committed state. Do
        // not retain the Session-bearing proof during Retry-After/backoff.
        sender
            .send(self.gate.clone())
            .map_err(|_| io::Error::other("live bootstrap producer is no longer waiting"))
    }
}

impl Drop for LiveBootstrapOffer {
    fn drop(&mut self) {
        if self.decision.is_some() {
            self.gate.cancel_if_pending();
        }
    }
}

/// Only a positively classified transient observation creates this value.
#[derive(Clone, Copy)]
pub(super) struct Candidate {
    pub(super) not_before: Instant,
}

/// Park the original request future until the Host confirms the exact actor
/// and inbox proof, then preserve the upstream hint and exponential backoff.
pub(super) async fn offer_and_wait(
    sender: oneshot::Sender<LiveBootstrapOffer>,
    candidate: Candidate,
    attempts: u32,
    config: &RetryConfig,
    observed_gate: &std::sync::Mutex<Option<AdmissionGate>>,
) -> bool {
    let Some(deadline) = Instant::now().checked_add(LIVE_BOOTSTRAP_OFFER_TTL) else {
        return false;
    };
    let gate = AdmissionGate::with_deadline(deadline.into_std());
    if let Ok(mut observed) = observed_gate.lock() {
        *observed = Some(gate.clone());
    }
    let (decision, received) = oneshot::channel();
    let offer = LiveBootstrapOffer {
        gate: gate.clone(),
        deadline,
        decision: Some(decision),
    };
    if sender.send(offer).is_err() {
        return false;
    }
    let Ok(Ok(committed_gate)) = tokio::time::timeout_at(deadline, received).await else {
        gate.cancel_if_pending();
        return false;
    };
    // If a callback sent the proof but subsequently failed, commit never won;
    // that signal cannot authorize the still-live provider future.
    if !committed_gate.same_instance(&gate) || !committed_gate.is_committed() {
        return false;
    }
    let delay = backoff_delay(config, attempts.saturating_sub(1)).max(Duration::from_millis(1));
    let Some(backoff) = Instant::now().checked_add(delay) else {
        return false;
    };
    tokio::time::sleep_until(backoff.max(candidate.not_before)).await;
    true
}
