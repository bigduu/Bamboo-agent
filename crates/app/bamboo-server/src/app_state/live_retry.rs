//! Live, process-local continuation of one prepared provider bootstrap.
//!
//! HTTP can select an existing offer, but cannot create the producer offer,
//! Root writer, proof, or execution identity. Restart drops all offers.

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use bamboo_domain::{
    LiveBootstrapProof, RootActorRuntimeWrite, RuntimeSessionPersistence, SessionInboxPort,
};
use bamboo_engine::execution::SessionExecutionReservation;
use bamboo_engine::runtime::config::LiveBootstrapHost;
use bamboo_llm::retry::LiveBootstrapOffer;
use tokio::sync::RwLock;
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
    proof: LiveBootstrapProof,
    persistence: Arc<dyn RuntimeSessionPersistence>,
    inbox: Arc<dyn SessionInboxPort>,
    offer: Mutex<Option<LiveBootstrapOffer>>,
    claimed: AtomicBool,
}

#[derive(Default)]
pub(crate) struct LiveRetryBridge {
    offers: Mutex<HashMap<String, Arc<Entry>>>,
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
            entry.proof.cancel_if_pending();
            entry
                .offer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            map.remove(&entry.session_id);
        }
    }

    fn remove_exact(&self, entry: &Arc<Entry>) {
        Self::remove_locked(&mut self.offers(), entry);
    }

    /// Called while holding the installed runner's registry read guard.
    pub(crate) fn cancel_and_token(&self, session: &str, run: &str, token: &CancellationToken) {
        let mut map = self.offers();
        if let Some(entry) = map
            .get(session)
            .filter(|entry| entry.run_id == run)
            .cloned()
        {
            entry.proof.cancel_if_pending();
            token.cancel();
            entry
                .offer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            map.remove(session);
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
            || entry.proof.is_cancelled()
            || entry.token.is_cancelled()
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
        let entry = {
            let runners = runners.read().await;
            let Some(runner) = runners.get(session) else {
                return ContinueResult::NotPrepared;
            };
            if runner.run_id != run
                || !matches!(runner.status, AgentStatus::Running)
                || runner.cancel_token.is_cancelled()
            {
                return ContinueResult::NotPrepared;
            }
            let mut map = self.offers();
            let Some(entry) = map
                .get(session)
                .filter(|entry| entry.run_id == run)
                .cloned()
            else {
                return ContinueResult::NotPrepared;
            };
            if entry.deadline <= Instant::now()
                || entry.proof.is_cancelled()
                || entry.token.is_cancelled()
            {
                Self::remove_locked(&mut map, &entry);
                return ContinueResult::NotPrepared;
            }
            if entry.claimed.swap(true, Ordering::SeqCst) {
                return ContinueResult::NotPrepared;
            }
            entry
        };
        // Cancellation of this HTTP future cancels the same gate even if an
        // already-started storage job outlives the awaiting task.
        let _attempt = Attempt {
            bridge: self,
            entry: entry.clone(),
        };
        let sent = Arc::new(AtomicBool::new(false));
        let sent_callback = sent.clone();
        let callback_entry = entry.clone();
        let callback: bamboo_domain::RootActorRuntimeEventPublisher = Box::new(move |check| {
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
            // This is the final effect inside the fenced gate callback.
            offer.approve(callback_entry.proof.clone())?;
            sent_callback.store(true, Ordering::SeqCst);
            Ok(())
        });
        let result = entry
            .persistence
            .confirm_live_bootstrap(&entry.proof, entry.inbox.clone(), callback)
            .await;
        match (result.is_ok(), sent.load(Ordering::SeqCst)) {
            (true, true) => ContinueResult::Continuing,
            (_, true) => ContinueResult::OutcomeUncertain,
            _ => ContinueResult::NotPrepared,
        }
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
            || map.contains_key(&session_id)
        {
            return Err(io::Error::other("live retry offer unavailable"));
        }
        let entry = Arc::new(Entry {
            session_id: session_id.clone(),
            run_id,
            deadline,
            token: self.token.clone(),
            proof,
            persistence: self.persistence.clone(),
            inbox: self.inbox.clone(),
            offer: Mutex::new(Some(offer)),
            claimed: AtomicBool::new(false),
        });
        map.insert(session_id, entry.clone());
        let bridge = Arc::downgrade(&self.bridge);
        let weak_entry = Arc::downgrade(&entry);
        tokio::spawn(async move {
            tokio::time::sleep_until(deadline).await;
            if let (Some(bridge), Some(entry)) = (bridge.upgrade(), weak_entry.upgrade()) {
                bridge.remove_exact(&entry);
            }
        });
        Ok(Box::new(OfferLifetime {
            bridge: Arc::downgrade(&self.bridge),
            entry: Arc::downgrade(&entry),
        }))
    }
}

struct Attempt<'a> {
    bridge: &'a LiveRetryBridge,
    entry: Arc<Entry>,
}
impl Drop for Attempt<'_> {
    fn drop(&mut self) {
        self.bridge.remove_exact(&self.entry);
    }
}

struct OfferLifetime {
    bridge: Weak<LiveRetryBridge>,
    entry: Weak<Entry>,
}
impl Drop for OfferLifetime {
    fn drop(&mut self) {
        if let (Some(bridge), Some(entry)) = (self.bridge.upgrade(), self.entry.upgrade()) {
            bridge.remove_exact(&entry);
        }
    }
}
