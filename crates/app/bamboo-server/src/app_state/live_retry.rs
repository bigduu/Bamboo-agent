//! Live, process-local continuation of one prepared provider bootstrap.
//!
//! HTTP can select an existing offer, but cannot create the producer offer,
//! Root writer, proof, or execution identity. Restart drops all offers.

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use bamboo_domain::{
    LiveBootstrapProof, RootActorRuntimeWrite, RuntimeSessionPersistence, SessionInboxPort,
};
use bamboo_engine::execution::SessionExecutionReservation;
use bamboo_engine::runtime::config::LiveBootstrapHost;
use bamboo_llm::retry::LiveBootstrapOffer;
use tokio::sync::{watch, RwLock};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::{AgentRunner, AgentStatus, AppState};

type Runners = Arc<RwLock<HashMap<String, AgentRunner>>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ContinueResult {
    Continuing,
    NotPrepared,
    OutcomeUncertain,
}

pub(crate) struct WaitingSnapshot {
    pub run_id: String,
    pub expires_in_ms: u64,
    pub confirming: bool,
}

struct Entry {
    session_id: String,
    run_id: String,
    deadline: Instant,
    token: CancellationToken,
    gate: bamboo_domain::AdmissionGate,
    offer: Mutex<Option<LiveBootstrapOffer>>,
    resources: Mutex<Option<OfferResources>>,
    claimed: AtomicBool,
    resolution: watch::Sender<Option<ContinueResult>>,
}

struct OfferResources {
    proof: LiveBootstrapProof,
    persistence: Arc<dyn RuntimeSessionPersistence>,
    inbox: Arc<dyn SessionInboxPort>,
    lease: Arc<PayloadLease>,
}

struct PayloadLease(Arc<AtomicUsize>);

impl Drop for PayloadLease {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Entry {
    // Preserve the first result, even when there are no subscribers yet.
    fn resolve(&self, result: ContinueResult) -> ContinueResult {
        self.resolution.send_if_modified(|current| {
            if current.is_none() {
                *current = Some(result);
                true
            } else {
                false
            }
        });
        (*self.resolution.borrow()).unwrap_or(ContinueResult::OutcomeUncertain)
    }

    async fn wait_for_resolution(&self) -> ContinueResult {
        let mut receiver = self.resolution.subscribe();
        let wait_until = self.deadline.min(Instant::now() + Duration::from_secs(5));
        loop {
            let current = *receiver.borrow_and_update();
            if let Some(result) = current {
                return result;
            }
            tokio::select! {
                _ = self.token.cancelled() => return ContinueResult::OutcomeUncertain,
                _ = tokio::time::sleep_until(wait_until) => return ContinueResult::OutcomeUncertain,
                result = receiver.changed() => {
                    if result.is_err() {
                        return ContinueResult::OutcomeUncertain;
                    }
                }
            }
        }
    }

    fn retire_offer(&self, result: ContinueResult) {
        self.gate.cancel_if_pending();
        self.offer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        self.resources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        self.resolve(result);
    }
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct ConfirmationTestGate {
    pub claimed: tokio::sync::Notify,
    pub follower: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct RegistrationTestGate {
    pub reached: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
}

#[derive(Default)]
pub(crate) struct LiveRetryBridge {
    offers: Mutex<HashMap<String, Arc<Entry>>>,
    active_payloads: Arc<AtomicUsize>,
    #[cfg(test)]
    confirmation_test_gate: Mutex<Option<Arc<ConfirmationTestGate>>>,
    #[cfg(test)]
    registration_test_gate: Mutex<Option<Arc<RegistrationTestGate>>>,
    #[cfg(test)]
    reject_next_callback: AtomicBool,
}

impl LiveRetryBridge {
    fn offers(&self) -> std::sync::MutexGuard<'_, HashMap<String, Arc<Entry>>> {
        self.offers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn remove_locked(map: &mut HashMap<String, Arc<Entry>>, entry: &Arc<Entry>) {
        if map
            .get(&entry.session_id)
            .is_some_and(|current| Arc::ptr_eq(current, entry))
        {
            entry.retire_offer(if entry.claimed.load(Ordering::SeqCst) {
                ContinueResult::OutcomeUncertain
            } else {
                ContinueResult::NotPrepared
            });
            map.remove(&entry.session_id);
        }
    }

    #[cfg(test)]
    pub(crate) fn pause_registration_for_test(&self) -> Arc<RegistrationTestGate> {
        let gate = Arc::new(RegistrationTestGate::default());
        *self.registration_test_gate.lock().unwrap() = Some(gate.clone());
        gate
    }

    #[cfg(test)]
    fn registration_test_gate(&self) -> Option<Arc<RegistrationTestGate>> {
        self.registration_test_gate.lock().unwrap().clone()
    }

    #[cfg(test)]
    pub(crate) fn pause_confirmations_for_test(&self) -> Arc<ConfirmationTestGate> {
        let gate = Arc::new(ConfirmationTestGate::default());
        *self.confirmation_test_gate.lock().unwrap() = Some(gate.clone());
        gate
    }

    #[cfg(test)]
    fn confirmation_test_gate(&self) -> Option<Arc<ConfirmationTestGate>> {
        self.confirmation_test_gate.lock().unwrap().clone()
    }

    #[cfg(test)]
    pub(crate) fn active_payloads_for_test(&self) -> usize {
        self.active_payloads.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(crate) fn clear_receipt_for_test(&self, session: &str, run: &str) {
        let mut map = self.offers();
        if let Some(entry) = map
            .get(session)
            .filter(|entry| entry.run_id == run)
            .cloned()
        {
            Self::remove_locked(&mut map, &entry);
        }
    }

    #[cfg(test)]
    pub(crate) fn cancel_claimed_gate_for_test(&self, session: &str, run: &str) -> bool {
        let map = self.offers();
        let Some(entry) = map.get(session).filter(|entry| entry.run_id == run) else {
            return false;
        };
        if !entry.claimed.load(Ordering::SeqCst) || entry.resolution.borrow().is_some() {
            return false;
        }
        entry.gate.cancel_if_pending();
        true
    }

    #[cfg(test)]
    pub(crate) fn reject_next_callback_for_test(&self) {
        self.reject_next_callback.store(true, Ordering::SeqCst);
    }

    async fn current_result(
        &self,
        runners: &Runners,
        entry: &Arc<Entry>,
        result: ContinueResult,
    ) -> ContinueResult {
        if result != ContinueResult::Continuing {
            return result;
        }
        let runners = runners.read().await;
        let valid_runner = runners.get(&entry.session_id).is_some_and(|runner| {
            runner.run_id == entry.run_id
                && matches!(runner.status, AgentStatus::Running)
                && !runner.cancel_token.is_cancelled()
        });
        let map = self.offers();
        if valid_runner
            && !entry.token.is_cancelled()
            && Instant::now() < entry.deadline
            && map
                .get(&entry.session_id)
                .is_some_and(|current| Arc::ptr_eq(current, entry))
        {
            ContinueResult::Continuing
        } else {
            ContinueResult::OutcomeUncertain
        }
    }

    /// Called while holding the installed runner's registry read guard.
    pub(crate) fn cancel_and_token(&self, session: &str, run: &str, token: &CancellationToken) {
        let mut map = self.offers();
        if let Some(entry) = map
            .get(session)
            .filter(|entry| entry.run_id == run)
            .cloned()
        {
            entry.gate.cancel_if_pending();
            token.cancel();
            entry.retire_offer(if entry.claimed.load(Ordering::SeqCst) {
                ContinueResult::OutcomeUncertain
            } else {
                ContinueResult::NotPrepared
            });
        } else {
            token.cancel();
        }
    }

    pub(crate) async fn waiting(
        &self,
        runners: &Runners,
        session: &str,
    ) -> Option<WaitingSnapshot> {
        let runners = runners.read().await;
        let runner = runners.get(session)?;
        if !matches!(runner.status, AgentStatus::Running) || runner.cancel_token.is_cancelled() {
            return None;
        }
        let map = self.offers();
        let entry = map.get(session)?;
        if entry.run_id != runner.run_id
            || entry.deadline <= Instant::now()
            || entry.gate.is_cancelled()
            || entry.token.is_cancelled()
            || entry.resolution.borrow().is_some()
        {
            return None;
        }
        Some(WaitingSnapshot {
            run_id: entry.run_id.clone(),
            expires_in_ms: entry
                .deadline
                .saturating_duration_since(Instant::now())
                .as_millis()
                .min(u64::MAX as u128) as u64,
            confirming: entry.claimed.load(Ordering::SeqCst),
        })
    }

    /// Select only the existing offer for this exact live execution.
    pub(crate) async fn confirm(
        &self,
        runners: &Runners,
        session: &str,
        run: &str,
    ) -> ContinueResult {
        let (entry, winner, resources) = {
            let runners = runners.read().await;
            let map = self.offers();
            let Some(entry) = map
                .get(session)
                .filter(|entry| entry.run_id == run)
                .cloned()
            else {
                return ContinueResult::OutcomeUncertain;
            };
            let running = runners.get(session).is_some_and(|runner| {
                runner.run_id == run
                    && matches!(runner.status, AgentStatus::Running)
                    && !runner.cancel_token.is_cancelled()
            });
            if !running || entry.deadline <= Instant::now() || entry.token.is_cancelled() {
                return if entry.claimed.load(Ordering::SeqCst) {
                    ContinueResult::OutcomeUncertain
                } else {
                    ContinueResult::NotPrepared
                };
            }
            if let Some(result) = *entry.resolution.borrow() {
                return result;
            }
            if entry.gate.is_cancelled() {
                return if entry.claimed.load(Ordering::SeqCst) {
                    ContinueResult::OutcomeUncertain
                } else {
                    ContinueResult::NotPrepared
                };
            }
            let winner = !entry.claimed.swap(true, Ordering::SeqCst);
            let resources = if winner {
                entry
                    .resources
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
            } else {
                None
            };
            (entry, winner, resources)
        };
        if !winner {
            #[cfg(test)]
            if let Some(gate) = self.confirmation_test_gate() {
                gate.follower.notify_one();
            }
            let result = entry.wait_for_resolution().await;
            return self.current_result(runners, &entry, result).await;
        }
        let Some(OfferResources {
            proof,
            persistence,
            inbox,
            lease,
        }) = resources
        else {
            return entry.resolve(ContinueResult::OutcomeUncertain);
        };
        // A dropped winning HTTP future cancels the exact pending gate and
        // wakes all competitors with an honest uncertain outcome.
        let _attempt = Attempt {
            entry: entry.clone(),
        };
        #[cfg(test)]
        if let Some(gate) = self.confirmation_test_gate() {
            gate.claimed.notify_one();
            tokio::select! {
                _ = gate.release.notified() => {},
                _ = entry.token.cancelled() => return ContinueResult::OutcomeUncertain,
            }
        }
        let sent = Arc::new(AtomicBool::new(false));
        let sent_callback = sent.clone();
        let callback_entry = entry.clone();
        let callback_proof = proof.clone();
        let callback_lease = lease.clone();
        #[cfg(test)]
        let reject_callback = self.reject_next_callback.swap(false, Ordering::SeqCst);
        let callback: bamboo_domain::RootActorRuntimeEventPublisher = Box::new(move |check| {
            let _lease = callback_lease;
            #[cfg(test)]
            if reject_callback {
                return Err(io::Error::other("synthetic callback refusal"));
            }
            check()?;
            if callback_entry.token.is_cancelled() || callback_entry.deadline <= Instant::now() {
                return Err(io::Error::other("run cancelled or offer expired"));
            }
            let offer = callback_entry
                .offer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
                .ok_or_else(|| io::Error::other("producer offer already consumed"))?;
            offer.approve(callback_proof)?;
            sent_callback.store(true, Ordering::SeqCst);
            Ok(())
        });
        let decision_deadline = entry.deadline.min(Instant::now() + Duration::from_secs(15));
        let result = tokio::select! {
            biased;
            _ = entry.token.cancelled() => None,
            _ = tokio::time::sleep_until(decision_deadline) => None,
            result = persistence.confirm_live_bootstrap(&proof, inbox, callback) => Some(result),
        };
        let committed = entry.gate.is_committed();
        drop(proof);
        drop(lease);
        let result = match (result, sent.load(Ordering::SeqCst), committed) {
            (Some(Ok(())), true, true) => ContinueResult::Continuing,
            (None, _, _) | (_, true, _) => ContinueResult::OutcomeUncertain,
            _ => ContinueResult::NotPrepared,
        };
        let result = entry.resolve(result);
        self.current_result(runners, &entry, result).await
    }
}

/// Constructed only from the real server reservation before it moves into the
/// engine. It fixes the owner, cancellation token, persister, and Inbox.
struct BoundLiveBootstrapHost {
    bridge: Arc<LiveRetryBridge>,
    runners: Runners,
    owner: RootActorRuntimeWrite,
    token: CancellationToken,
    persistence: Arc<dyn RuntimeSessionPersistence>,
    inbox: Arc<dyn SessionInboxPort>,
}

pub(crate) fn bind(
    state: &AppState,
    reservation: &SessionExecutionReservation,
    session: &bamboo_domain::Session,
) -> Option<Arc<dyn LiveBootstrapHost>> {
    let owner = reservation.root_actor_writer()?;
    let persistence = reservation.execution_persistence()?;
    if session
        .agent_runtime_state
        .as_ref()
        .is_none_or(|runtime| runtime.no_human_approver)
        || owner.fence.actor_id != session.id
        || owner.fence.run_id != reservation.run_id()
        || owner.created_at != session.created_at
        || reservation.cancel_token().is_cancelled()
    {
        return None;
    }
    Some(Arc::new(BoundLiveBootstrapHost {
        bridge: state.live_retry.clone(),
        runners: state.agent_runners.clone(),
        owner,
        token: reservation.cancel_token().clone(),
        persistence,
        inbox: state.session_inbox.clone(),
    }))
}

#[async_trait::async_trait]
impl LiveBootstrapHost for BoundLiveBootstrapHost {
    async fn register(
        &self,
        owner: RootActorRuntimeWrite,
        proof: LiveBootstrapProof,
        persistence: Arc<dyn RuntimeSessionPersistence>,
        inbox: Arc<dyn SessionInboxPort>,
        offer: LiveBootstrapOffer,
    ) -> io::Result<Box<dyn Send>> {
        if owner.fence != self.owner.fence
            || owner.created_at != self.owner.created_at
            || owner.conversation_generation != self.owner.conversation_generation
            || !Arc::ptr_eq(&persistence, &self.persistence)
            || !Arc::ptr_eq(&inbox, &self.inbox)
            || !offer.matches_proof(&proof)
            || !offer.is_live()
            || proof.is_cancelled()
            || self.token.is_cancelled()
        {
            return Err(io::Error::other("live retry binding changed"));
        }
        #[cfg(test)]
        if let Some(gate) = self.bridge.registration_test_gate() {
            gate.reached.notify_one();
            tokio::select! {
                _ = gate.release.notified() => {},
                _ = self.token.cancelled() => return Err(io::Error::other("run cancelled")),
            }
        }
        let session_id = owner.fence.actor_id.clone();
        let run_id = owner.fence.run_id.clone();
        let deadline = offer.deadline();
        let runners = self.runners.read().await;
        let runner = runners
            .get(&session_id)
            .filter(|runner| {
                runner.run_id == run_id
                    && matches!(runner.status, AgentStatus::Running)
                    && !runner.cancel_token.is_cancelled()
            })
            .ok_or_else(|| io::Error::other("live retry runner unavailable"))?;
        let mut map = self.bridge.offers();
        if runner.cancel_token.is_cancelled()
            || self.token.is_cancelled()
            || proof.is_cancelled()
            || !offer.is_live()
            || deadline <= Instant::now()
        {
            return Err(io::Error::other("live retry offer unavailable"));
        }
        if let Some(previous) = map.get(&session_id).cloned() {
            if previous.run_id == run_id {
                return Err(io::Error::other(
                    "live retry offer already used by this run",
                ));
            }
            LiveRetryBridge::remove_locked(&mut map, &previous);
        }
        // These limits cover Host-registered runs and offer operations;
        // they do not govern pre-registration snapshots in the engine.
        const MAX_TRACKED_RUNS: usize = 1024;
        const MAX_PENDING_OFFERS: usize = 32;
        if map.len() >= MAX_TRACKED_RUNS
            || self.bridge.active_payloads.load(Ordering::SeqCst) >= MAX_PENDING_OFFERS
        {
            return Err(io::Error::other("live retry capacity exhausted"));
        }
        self.bridge.active_payloads.fetch_add(1, Ordering::SeqCst);
        let lease = Arc::new(PayloadLease(self.bridge.active_payloads.clone()));
        let (resolution, _) = watch::channel(None);
        let entry = Arc::new(Entry {
            session_id: session_id.clone(),
            run_id,
            deadline,
            token: self.token.clone(),
            gate: proof.storage_gate(),
            offer: Mutex::new(Some(offer)),
            resources: Mutex::new(Some(OfferResources {
                proof,
                persistence: self.persistence.clone(),
                inbox: self.inbox.clone(),
                lease,
            })),
            claimed: AtomicBool::new(false),
            resolution,
        });
        map.insert(session_id, entry.clone());
        let bridge = Arc::downgrade(&self.bridge);
        let weak_entry = Arc::downgrade(&entry);
        let weak_runners = Arc::downgrade(&self.runners);
        tokio::spawn(async move {
            let mut check_at = deadline.min(Instant::now() + Duration::from_secs(30));
            loop {
                tokio::time::sleep_until(check_at).await;
                let (Some(bridge), Some(entry), Some(runners)) = (
                    bridge.upgrade(),
                    weak_entry.upgrade(),
                    weak_runners.upgrade(),
                ) else {
                    break;
                };
                let keep = {
                    let runners = runners.read().await;
                    let active = runners.get(&entry.session_id).is_some_and(|runner| {
                        runner.run_id == entry.run_id
                            && matches!(runner.status, AgentStatus::Running)
                            && !runner.cancel_token.is_cancelled()
                    });
                    let mut map = bridge.offers();
                    let present = map
                        .get(&entry.session_id)
                        .is_some_and(|current| Arc::ptr_eq(current, &entry));
                    if !present {
                        false
                    } else {
                        let unresolved = entry.resolution.borrow().is_none();
                        if unresolved && Instant::now() >= entry.deadline {
                            entry.retire_offer(if entry.claimed.load(Ordering::SeqCst) {
                                ContinueResult::OutcomeUncertain
                            } else {
                                ContinueResult::NotPrepared
                            });
                        }
                        if !active {
                            LiveRetryBridge::remove_locked(&mut map, &entry);
                        }
                        active
                    }
                };
                if !keep {
                    break;
                }
                // A run has only one v1 offer: retain a small tombstone while
                // it remains active so an old confirmation cannot approve a
                // later bootstrap for the same run.
                check_at = Instant::now() + Duration::from_secs(30);
            }
        });
        Ok(Box::new(OfferLifetime {
            bridge: Arc::downgrade(&self.bridge),
            entry: Arc::downgrade(&entry),
        }))
    }
}

struct Attempt {
    entry: Arc<Entry>,
}
impl Drop for Attempt {
    fn drop(&mut self) {
        self.entry.retire_offer(ContinueResult::OutcomeUncertain);
    }
}

struct OfferLifetime {
    bridge: Weak<LiveRetryBridge>,
    entry: Weak<Entry>,
}
impl Drop for OfferLifetime {
    fn drop(&mut self) {
        if let (Some(bridge), Some(entry)) = (self.bridge.upgrade(), self.entry.upgrade()) {
            let map = bridge.offers();
            if map
                .get(&entry.session_id)
                .is_some_and(|current| Arc::ptr_eq(current, &entry))
                && !entry.claimed.load(Ordering::SeqCst)
            {
                entry.retire_offer(ContinueResult::NotPrepared);
            }
        }
    }
}
