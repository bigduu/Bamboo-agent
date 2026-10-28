//! A bounded, process-local handoff for an acknowledged new input whose first
//! Execute request returned `need_sync` before any execution began.
//!
//! The retained reservation is the authority. History, ACK tombstones and
//! client-provided IDs never recreate this capability after loss or restart.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use bamboo_agent_core::{Message, Role, Session};
use bamboo_domain::{
    AdmissionGate, RootActorRuntimeWrite, SessionInboxBacklog, SessionMessageEnvelope,
};
use bamboo_engine::config::UntrustedExecutionInputs;
use bamboo_engine::execution::SessionExecutionReservation;
use tokio::time::Instant;
use uuid::Uuid;

use crate::app_state::{AgentStatus, AppState};

const MAX_PENDING_SYNC: usize = 64;
const PENDING_SYNC_LIFETIME: Duration = Duration::from_secs(60);

/// Created only at the actual newly checkpointed-and-ACKed ingress boundary.
/// Private fields prevent HTTP request data from manufacturing a witness.
pub(crate) struct FreshSyncWitness {
    session_id: String,
    input_id: String,
    owner: RootActorRuntimeWrite,
}

impl FreshSyncWitness {
    pub(crate) fn from_newly_admitted(
        session_id: &str,
        committed: &[Message],
        inputs: Option<&UntrustedExecutionInputs>,
        reservation: Option<&SessionExecutionReservation>,
    ) -> Option<Self> {
        let message = committed.last()?;
        if message.role != Role::User {
            return None;
        }
        let value = message.metadata.as_ref()?.get("session_message")?;
        let source = value.get("source")?;
        let kind = value.get("kind")?.as_str()?;
        let is_user = source.get("type")?.as_str()? == "user" && kind == "user_input";
        let is_root_chat = source.get("type")?.as_str()? == "runtime"
            && source.get("subsystem")?.as_str()? == "chat"
            && kind == "runtime_instruction"
            && value.get("body")?.get("instruction")?.as_str()? == "root_chat_turn_v1";
        if (!is_user && !is_root_chat) || value.get("target_session_id")?.as_str()? != session_id {
            return None;
        }
        let envelope = <SessionMessageEnvelope as serde::Deserialize>::deserialize(value).ok()?;
        if !bamboo_domain::is_matching_session_message(message, &envelope)
            || inputs.is_some_and(|inputs| {
                !inputs
                    .observations()
                    .iter()
                    .any(|item| item.input_id() == message.id.as_str())
            })
        {
            return None;
        }
        let reservation = reservation?;
        let owner = reservation.root_actor_writer()?;
        if reservation.session_id() != session_id
            || owner.fence.actor_id != session_id
            || owner.fence.run_id != reservation.run_id()
            || reservation.cancel_token().is_cancelled()
        {
            return None;
        }
        Some(Self {
            session_id: session_id.to_owned(),
            input_id: message.id.clone(),
            owner,
        })
    }

    fn matches_reservation(&self, reservation: &SessionExecutionReservation) -> bool {
        let Some(owner) = reservation.root_actor_writer() else {
            return false;
        };
        self.session_id == reservation.session_id()
            && self.owner.fence == owner.fence
            && self.owner.created_at == owner.created_at
            && self.owner.conversation_generation == owner.conversation_generation
            && owner.fence.run_id == reservation.run_id()
            && !reservation.cancel_token().is_cancelled()
    }

    fn matches_snapshot(&self, session: &Session) -> bool {
        session.id == self.session_id
            && session.created_at == self.owner.created_at
            && session
                .messages
                .last()
                .is_some_and(|message| message.role == Role::User && message.id == self.input_id)
            && crate::handlers::agent::events::startup_work_id(session).as_deref()
                == Some(self.input_id.as_str())
    }
}

/// Returned only by removing one actual, retained owner from the registry.
pub(crate) struct FreshSyncHandoff {
    pub reservation: SessionExecutionReservation,
    pub inputs: Option<UntrustedExecutionInputs>,
    pub witness: Option<FreshSyncWitness>,
    pub generate_title: bool,
    pub native_main: bool,
    pub live_bootstrap_resume: bool,
}

struct PendingFreshSync {
    nonce: Uuid,
    deadline: Instant,
    reservation: SessionExecutionReservation,
    inputs: Option<UntrustedExecutionInputs>,
    witness: FreshSyncWitness,
    snapshot: Session,
    backlog: SessionInboxBacklog,
    proof: bamboo_domain::LiveBootstrapProof,
    generate_title: bool,
    native_main: bool,
    live_bootstrap_resume: bool,
}

enum FreshSyncEntry {
    Parked(PendingFreshSync),
    // Retains the exact owner while take revalidates or abandonment finishes.
    InFlight { nonce: Uuid, run_id: String },
}

/// No durable or public reconstruction exists: restart loses every handoff.
#[derive(Default)]
pub(crate) struct FreshSyncRegistry {
    entries: Mutex<HashMap<String, FreshSyncEntry>>,
}

/// Owns the pending reservation during asynchronous revalidation and cleanup.
/// Cancellation of the HTTP future drops the reservation (and cancels its
/// token) before removing this marker. A successful transition removes the
/// marker while holding the runner write lock.
struct TakenFreshSync {
    registry: Weak<FreshSyncRegistry>,
    session_id: String,
    nonce: Uuid,
    pending: Option<PendingFreshSync>,
}

impl TakenFreshSync {
    fn pending(&self) -> &PendingFreshSync {
        self.pending.as_ref().expect("taken owner is present")
    }

    fn into_handoff(mut self) -> FreshSyncHandoff {
        let pending = self.pending.take().expect("taken owner is present");
        pending.proof.cancel_if_pending();
        FreshSyncHandoff {
            reservation: pending.reservation,
            inputs: pending.inputs,
            witness: Some(pending.witness),
            generate_title: pending.generate_title,
            native_main: pending.native_main,
            live_bootstrap_resume: pending.live_bootstrap_resume,
        }
    }

    async fn abandon(self) {
        // Keep exact cleanup alive even if the HTTP waiter is dropped.
        let _ = tokio::spawn(async move {
            let mut taken = self;
            if let Some(pending) = taken.pending.take() {
                pending.proof.cancel_if_pending();
                pending.reservation.abandon().await;
            }
        })
        .await;
    }
}

impl Drop for TakenFreshSync {
    fn drop(&mut self) {
        if let Some(pending) = self.pending.take() {
            pending.proof.cancel_if_pending();
            drop(pending);
        }
        let Some(registry) = self.registry.upgrade() else {
            return;
        };
        if let Ok(mut entries) = registry.entries.lock() {
            if matches!(entries.get(&self.session_id), Some(FreshSyncEntry::InFlight { nonce, .. }) if *nonce == self.nonce)
            {
                entries.remove(&self.session_id);
            }
        };
    }
}

fn empty_stable_backlog(backlog: &SessionInboxBacklog) -> bool {
    backlog.pending == 0 && backlog.claimed == 0 && backlog.generation < u64::MAX
}

impl FreshSyncRegistry {
    /// Caller holds the runner read lock, preserving runner -> registry lock
    /// order. A parked or revalidating reservation is the same Pending owner.
    pub(crate) fn is_current_owner(&self, session_id: &str, run_id: &str) -> bool {
        let Ok(entries) = self.entries.lock() else {
            return false;
        };
        match entries.get(session_id) {
            Some(FreshSyncEntry::Parked(entry)) => entry.reservation.run_id() == run_id,
            Some(FreshSyncEntry::InFlight {
                run_id: current, ..
            }) => current == run_id,
            None => false,
        }
    }

    fn claim(
        self: &Arc<Self>,
        session_id: &str,
        expected_nonce: Option<Uuid>,
    ) -> Option<TakenFreshSync> {
        let mut entries = self.entries.lock().ok()?;
        let entry = entries.get_mut(session_id)?;
        let FreshSyncEntry::Parked(pending) = entry else {
            return None;
        };
        if expected_nonce.is_some_and(|nonce| nonce != pending.nonce) {
            return None;
        }
        let nonce = pending.nonce;
        let run_id = pending.reservation.run_id().to_owned();
        let FreshSyncEntry::Parked(pending) =
            std::mem::replace(entry, FreshSyncEntry::InFlight { nonce, run_id })
        else {
            unreachable!("entry was parked while the registry lock was held")
        };
        Some(TakenFreshSync {
            registry: Arc::downgrade(self),
            session_id: session_id.to_owned(),
            nonce,
            pending: Some(pending),
        })
    }

    /// Called only from `SyncMismatch`, before any hook, title or provider
    /// startup. Failure leaves the reservation and input with the caller.
    pub(crate) async fn park(
        self: &Arc<Self>,
        state: &AppState,
        reservation: &mut Option<SessionExecutionReservation>,
        inputs: &mut Option<UntrustedExecutionInputs>,
        witness: &mut Option<FreshSyncWitness>,
        generate_title: bool,
        native_main: bool,
        live_bootstrap_resume: bool,
    ) -> bool {
        let (Some(owner), Some(witness_ref)) = (reservation.as_ref(), witness.as_ref()) else {
            #[cfg(test)]
            eprintln!("fresh sync park: missing owner or witness");
            return false;
        };
        if !witness_ref.matches_reservation(owner)
            || inputs.as_ref().is_some_and(|inputs| {
                !inputs
                    .observations()
                    .iter()
                    .any(|item| item.input_id() == witness_ref.input_id.as_str())
            })
        {
            #[cfg(test)]
            eprintln!("fresh sync park: owner or input observation changed");
            return false;
        }
        let id = witness_ref.session_id.clone();
        let snapshot = match state.storage.load_session(&id).await {
            Ok(Some(session)) if witness_ref.matches_snapshot(&session) => session,
            _ => {
                #[cfg(test)]
                eprintln!("fresh sync park: Session snapshot mismatch");
                return false;
            }
        };
        let Some(persistence) = owner.execution_persistence() else {
            #[cfg(test)]
            eprintln!("fresh sync park: no bound persistence");
            return false;
        };
        let backlog = match state.session_inbox.inspect(&id).await {
            Ok(backlog) if empty_stable_backlog(&backlog) => backlog,
            _ => {
                #[cfg(test)]
                eprintln!("fresh sync park: Inbox is not empty and stable");
                return false;
            }
        };
        let proof = match persistence
            .checkpoint_live_bootstrap(
                &snapshot,
                state.session_inbox.clone(),
                AdmissionGate::default(),
            )
            .await
        {
            Ok(proof) => proof,
            Err(error) => {
                #[cfg(test)]
                eprintln!("fresh sync park: actor checkpoint failed: {error}");
                #[cfg(not(test))]
                let _ = error;
                return false;
            }
        };
        if !matches!(state.session_inbox.inspect(&id).await, Ok(after) if after == backlog)
            || !witness_ref.matches_reservation(owner)
        {
            #[cfg(test)]
            eprintln!("fresh sync park: Inbox or owner changed after checkpoint");
            proof.cancel_if_pending();
            return false;
        }

        let run_id = owner.run_id().to_owned();
        let token = owner.cancel_token().clone();
        let nonce = Uuid::new_v4();
        let deadline = Instant::now() + PENDING_SYNC_LIFETIME;
        {
            // No await while both guards are held. Stop and take serialize on
            // the same runner slot; the map stores the non-clonable owner.
            let mut runners = state.agent_runners.write().await;
            let Some(runner) = runners.get_mut(&id) else {
                #[cfg(test)]
                eprintln!("fresh sync park: runner missing");
                proof.cancel_if_pending();
                return false;
            };
            let Ok(mut entries) = self.entries.lock() else {
                #[cfg(test)]
                eprintln!("fresh sync park: registry lock poisoned");
                proof.cancel_if_pending();
                return false;
            };
            if entries.len() >= MAX_PENDING_SYNC
                || entries.contains_key(&id)
                || runner.run_id != run_id
                || !matches!(runner.status, AgentStatus::Running)
                || token.is_cancelled()
            {
                #[cfg(test)]
                eprintln!("fresh sync park: registry/runner/token admission failed");
                proof.cancel_if_pending();
                return false;
            }
            let entry = PendingFreshSync {
                nonce,
                deadline,
                reservation: reservation.take().expect("checked reservation"),
                inputs: inputs.take(),
                witness: witness.take().expect("checked witness"),
                snapshot,
                backlog,
                proof,
                generate_title,
                native_main,
                live_bootstrap_resume,
            };
            // A waiting resync is not a running generation. The logical router
            // and Root actor remain owned by this exact retained reservation.
            runner.status = AgentStatus::Pending;
            entries.insert(id.clone(), FreshSyncEntry::Parked(entry));
        }

        let registry = Arc::downgrade(self);
        tokio::spawn(async move {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => {},
                _ = token.cancelled() => {},
            }
            let Some(registry) = registry.upgrade() else {
                return;
            };
            if let Some(taken) = registry.claim(&id, Some(nonce)) {
                taken.abandon().await;
            }
        });
        true
    }

    /// Atomically removes the one-shot owner before any new reservation. An
    /// invalid or changed Session/Inbox consumes it and fails closed.
    pub(crate) async fn take(
        self: &Arc<Self>,
        state: &AppState,
        id: &str,
    ) -> Option<FreshSyncHandoff> {
        let taken = self.claim(id, None)?;
        let deadline = taken.pending().deadline;
        let token = taken.pending().reservation.cancel_token().clone();
        let valid = tokio::select! {
            valid = self.revalidate(state, taken.pending()) => valid,
            _ = token.cancelled() => false,
            _ = tokio::time::sleep_until(deadline) => false,
        };
        if !valid {
            taken.abandon().await;
            return None;
        }
        let handoff = {
            let mut runners = state.agent_runners.write().await;
            let Some(runner) = runners.get_mut(id) else {
                drop(runners);
                taken.abandon().await;
                return None;
            };
            if runner.run_id != taken.pending().reservation.run_id()
                || !matches!(runner.status, AgentStatus::Pending)
                || taken.pending().reservation.cancel_token().is_cancelled()
                || Instant::now() >= taken.pending().deadline
            {
                drop(runners);
                taken.abandon().await;
                return None;
            }
            runner.status = AgentStatus::Running;
            taken.into_handoff()
        };
        Some(handoff)
    }

    async fn revalidate(&self, state: &AppState, pending: &PendingFreshSync) -> bool {
        if Instant::now() >= pending.deadline
            || !pending.witness.matches_reservation(&pending.reservation)
            || pending.proof.is_cancelled()
        {
            return false;
        }
        let id = &pending.witness.session_id;
        if !matches!(state.session_inbox.inspect(id).await, Ok(before) if before == pending.backlog)
        {
            return false;
        }
        let Some(persistence) = pending.reservation.execution_persistence() else {
            return false;
        };
        let result = persistence
            .checkpoint_live_bootstrap(
                &pending.snapshot,
                state.session_inbox.clone(),
                pending.proof.storage_gate(),
            )
            .await;
        if result.is_err() || Instant::now() >= pending.deadline {
            return false;
        }
        matches!(state.session_inbox.inspect(id).await, Ok(after) if after == pending.backlog)
            && pending.witness.matches_reservation(&pending.reservation)
    }
}
