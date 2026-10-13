//! Durable WorkerHost observations and atomic slot reservations.
//!
//! The broker supplies connection identity; this file is control-plane state,
//! not a broker mailbox. All readers and writers take one physical file lock.

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};

use bamboo_domain::{
    select_worker_host, HostPlacementRequest, HostRegistryError, RegisteredWorkerHost,
    WorkerHostRegistration, WorkerRunReservation, WorkerRunReservationCheck, WorkerSlotLease,
    HOST_REGISTRY_SCHEMA_VERSION, MAX_REGISTERED_HOSTS,
};
use chrono::{DateTime, Utc};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::durable_atomic_write_blocking;

const RECORD: &str = "worker-host-registry.json";
const MARKER: &str = "worker-host-registry.initialized";
const LOCK: &str = "worker-host-registry.lock";
const MARKER_BYTES: &[u8] = b"worker-host-registry-v1\n";

fn storage(error: io::Error) -> HostRegistryError {
    HostRegistryError::Storage(error.to_string())
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegistryState {
    schema_version: u32,
    revision: u64,
    hosts: BTreeMap<String, RegisteredWorkerHost>,
}

impl Default for RegistryState {
    fn default() -> Self {
        Self {
            schema_version: HOST_REGISTRY_SCHEMA_VERSION,
            revision: 0,
            hosts: BTreeMap::new(),
        }
    }
}

impl RegistryState {
    fn validate(&self) -> Result<(), HostRegistryError> {
        if self.schema_version != HOST_REGISTRY_SCHEMA_VERSION
            || self.revision == 0
            || self.hosts.len() > MAX_REGISTERED_HOSTS
            || self
                .hosts
                .iter()
                .any(|(key, host)| key != &host.host_ref || host.validate().is_err())
        {
            return Err(HostRegistryError::Corrupt);
        }
        Ok(())
    }
}

fn read_state(home: &Path) -> Result<RegistryState, HostRegistryError> {
    let marker = std::fs::read(home.join(MARKER));
    let record = std::fs::read(home.join(RECORD));
    match (marker, record) {
        (Err(marker), Err(record))
            if marker.kind() == io::ErrorKind::NotFound
                && record.kind() == io::ErrorKind::NotFound =>
        {
            Ok(RegistryState::default())
        }
        (Ok(marker), Ok(record)) if marker == MARKER_BYTES => {
            let state: RegistryState =
                serde_json::from_slice(&record).map_err(|_| HostRegistryError::Corrupt)?;
            state.validate()?;
            Ok(state)
        }
        (Err(error), _) if error.kind() != io::ErrorKind::NotFound => Err(storage(error)),
        (_, Err(error)) if error.kind() != io::ErrorKind::NotFound => Err(storage(error)),
        _ => Err(HostRegistryError::Corrupt),
    }
}

fn publish_state(home: &Path, state: &RegistryState) -> Result<(), HostRegistryError> {
    let marker = home.join(MARKER);
    if !marker.exists() {
        // Publish the immutable witness first. Failure before the registry
        // record then fails closed on restart rather than resetting epochs.
        durable_atomic_write_blocking(&marker, MARKER_BYTES, |_| Ok(())).map_err(storage)?;
    }
    let encoded = serde_json::to_vec(state).map_err(|_| HostRegistryError::Corrupt)?;
    durable_atomic_write_blocking(&home.join(RECORD), &encoded, |_| Ok(())).map_err(storage)
}

/// A shared-home registry. This is safe across independent processes using the
/// same physical filesystem locking semantics; it is not a distributed DB.
#[derive(Debug, Clone)]
pub struct FileHostRegistry {
    home: PathBuf,
}

impl FileHostRegistry {
    pub async fn new(home: PathBuf) -> io::Result<Self> {
        if !home.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "WorkerHost registry home must be absolute",
            ));
        }
        tokio::fs::create_dir_all(&home).await?;
        Ok(Self { home })
    }

    async fn transact<T, F>(&self, operation: F) -> Result<T, HostRegistryError>
    where
        T: Send + 'static,
        F: FnOnce(&mut RegistryState) -> Result<(T, bool), HostRegistryError> + Send + 'static,
    {
        let home = self.home.clone();
        tokio::task::spawn_blocking(move || {
            let lock = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(home.join(LOCK))
                .map_err(storage)?;
            lock.lock_exclusive().map_err(storage)?;
            let mut state = read_state(&home)?;
            let (result, changed) = operation(&mut state)?;
            if changed {
                state.revision = state
                    .revision
                    .checked_add(1)
                    .ok_or(HostRegistryError::Corrupt)?;
                state.validate()?;
                publish_state(&home, &state)?;
            }
            Ok(result)
        })
        .await
        .map_err(|error| {
            HostRegistryError::Storage(format!("join registry transaction: {error}"))
        })?
    }

    pub async fn inspect_host(
        &self,
        host_ref: &str,
    ) -> Result<Option<RegisteredWorkerHost>, HostRegistryError> {
        let host_ref = host_ref.to_owned();
        self.transact(move |state| Ok((state.hosts.get(&host_ref).cloned(), false)))
            .await
    }

    pub async fn observe_host(
        &self,
        request: WorkerHostRegistration,
    ) -> Result<RegisteredWorkerHost, HostRegistryError> {
        request.validate()?;
        self.transact(move |state| {
            let existing = state.hosts.get(&request.host_ref);
            if existing.map(|host| host.connection_generation.as_str())
                != request.expected_connection_generation.as_deref()
            {
                return Err(HostRegistryError::StaleGeneration);
            }
            if existing.is_none() && state.hosts.len() >= MAX_REGISTERED_HOSTS {
                return Err(HostRegistryError::Busy);
            }
            let mut slots = BTreeMap::new();
            let mut next_slot_epoch = 0;
            if let Some(old) = existing {
                next_slot_epoch = old.next_slot_epoch;
                if old.connection_generation == request.connection_generation {
                    if request.observed_at < old.observed_at
                        || old.mailbox != request.mailbox
                        || old.role != request.role
                    {
                        return Err(HostRegistryError::StaleGeneration);
                    }
                    slots = old.slots.clone();
                    slots.retain(|_, lease| lease.expires_at > request.observed_at);
                    if !slots.is_empty()
                        && (old.capabilities != request.capabilities
                            || slots.keys().any(|slot| *slot >= request.max_slots)
                            || slots
                                .values()
                                .any(|lease| lease.expires_at > request.lease_expires_at))
                    {
                        return Err(HostRegistryError::Busy);
                    }
                }
                // A different authenticated connection fences every slot of
                // the predecessor. Its epoch counter remains monotonic.
            }
            let host = RegisteredWorkerHost {
                host_ref: request.host_ref.clone(),
                mailbox: request.mailbox,
                role: request.role,
                connection_generation: request.connection_generation,
                credential_expires_at: request.credential_expires_at,
                observed_at: request.observed_at,
                lease_expires_at: request.lease_expires_at,
                max_slots: request.max_slots,
                capabilities: request.capabilities,
                next_slot_epoch,
                slots,
            };
            state.hosts.insert(host.host_ref.clone(), host.clone());
            Ok((host, true))
        })
        .await
    }

    /// Select and reserve under one file lock. No caller can claim capacity
    /// merely by observing a broker connection or reading this registry.
    pub async fn reserve_slot(
        &self,
        request: HostPlacementRequest,
    ) -> Result<WorkerSlotLease, HostRegistryError> {
        request.validate()?;
        self.transact(move |state| {
            // ActorSession identity may not occupy two WorkerHosts or slots at
            // once, including a retry with a different Run ID. This check and
            // slot selection share the same cross-process file lock.
            if state.hosts.values().any(|host| {
                host.is_live(request.now)
                    && host.slots.values().any(|lease| {
                        lease.actor_id == request.actor_id && lease.expires_at > request.now
                    })
            }) {
                return Err(HostRegistryError::Busy);
            }
            let selected = select_worker_host(&state.hosts, &request)?;
            let host_ref = selected.host_ref.clone();
            let slot = selected
                .first_free_slot(request.now)
                .ok_or(HostRegistryError::NoEligibleHost)?;
            let host = state
                .hosts
                .get_mut(&host_ref)
                .ok_or(HostRegistryError::Corrupt)?;
            let epoch = host
                .next_slot_epoch
                .checked_add(1)
                .ok_or(HostRegistryError::Corrupt)?;
            let expires_at = request
                .lease_expires_at
                .min(host.lease_expires_at)
                .min(host.credential_expires_at);
            if expires_at <= request.now {
                return Err(HostRegistryError::NoEligibleHost);
            }
            let lease = WorkerSlotLease {
                lease_id: Uuid::new_v4().to_string(),
                host_ref,
                connection_generation: host.connection_generation.clone(),
                slot,
                epoch,
                actor_id: request.actor_id,
                run_id: request.run_id,
                expires_at,
            };
            host.next_slot_epoch = epoch;
            host.slots.insert(slot, lease.clone());
            Ok((lease, true))
        })
        .await
    }

    pub async fn validate_slot(
        &self,
        lease: &WorkerSlotLease,
        now: DateTime<Utc>,
    ) -> Result<(), HostRegistryError> {
        let lease = lease.clone();
        self.transact(move |state| {
            let host = state
                .hosts
                .get(&lease.host_ref)
                .ok_or(HostRegistryError::StaleLease)?;
            if !host.is_live(now)
                || host.connection_generation != lease.connection_generation
                || host.slots.get(&lease.slot) != Some(&lease)
                || lease.expires_at <= now
            {
                return Err(HostRegistryError::StaleLease);
            }
            Ok(((), false))
        })
        .await
    }

    /// Resolve the opaque ActorActivation placement ID to the exact live slot
    /// and delivery identity under one cross-process registry lock. The
    /// `observed_*` fields must come from a fresh authenticated broker query;
    /// this is a preflight only, not an atomic broker-delivery fence.
    pub async fn validate_run_reservation(
        &self,
        check: WorkerRunReservationCheck,
    ) -> Result<WorkerRunReservation, HostRegistryError> {
        check.validate()?;
        self.transact(move |state| {
            let host = state
                .hosts
                .get(&check.observed_host_ref)
                .ok_or(HostRegistryError::StaleLease)?;
            if !host.is_live(check.now)
                || host.connection_generation != check.observed_connection_generation
                || host.capabilities.placement_class != check.placement_ref.class
            {
                return Err(HostRegistryError::StaleLease);
            }
            let mut matching = host
                .slots
                .values()
                .filter(|lease| lease.lease_id == check.placement_ref.lease_id);
            let lease = matching.next().ok_or(HostRegistryError::StaleLease)?;
            if matching.next().is_some() {
                return Err(HostRegistryError::Corrupt);
            }
            if lease.actor_id != check.actor_id
                || lease.run_id != check.run_id
                || Some(lease.epoch) != check.placement_ref.slot_epoch
                || lease.connection_generation != check.observed_connection_generation
                || lease.expires_at <= check.now
            {
                return Err(HostRegistryError::StaleLease);
            }
            Ok((
                WorkerRunReservation {
                    lease: lease.clone(),
                    mailbox: host.mailbox.clone(),
                    role: host.role.clone(),
                },
                false,
            ))
        })
        .await
    }

    /// Recover a lease by the opaque ID persisted in ActorActivation's
    /// placement_ref. Callers must also validate the ActorDirectory fence.
    pub async fn inspect_slot_by_lease_id(
        &self,
        lease_id: &str,
        actor_id: &str,
        run_id: &str,
        now: DateTime<Utc>,
    ) -> Result<WorkerSlotLease, HostRegistryError> {
        let lease_id = lease_id.to_owned();
        let actor_id = actor_id.to_owned();
        let run_id = run_id.to_owned();
        self.transact(move |state| {
            let lease = state
                .hosts
                .values()
                .filter(|host| host.is_live(now))
                .flat_map(|host| host.slots.values())
                .find(|lease| lease.lease_id == lease_id)
                .filter(|lease| {
                    lease.actor_id == actor_id && lease.run_id == run_id && lease.expires_at > now
                })
                .cloned()
                .ok_or(HostRegistryError::StaleLease)?;
            Ok((lease, false))
        })
        .await
    }

    pub async fn renew_slot(
        &self,
        lease: &WorkerSlotLease,
        now: DateTime<Utc>,
        new_expires_at: DateTime<Utc>,
    ) -> Result<WorkerSlotLease, HostRegistryError> {
        let lease = lease.clone();
        self.transact(move |state| {
            let host = state
                .hosts
                .get_mut(&lease.host_ref)
                .ok_or(HostRegistryError::StaleLease)?;
            if !host.is_live(now)
                || host.connection_generation != lease.connection_generation
                || host.slots.get(&lease.slot) != Some(&lease)
                || lease.expires_at <= now
            {
                return Err(HostRegistryError::StaleLease);
            }
            if new_expires_at <= lease.expires_at
                || new_expires_at - now > chrono::Duration::minutes(5)
                || new_expires_at > host.lease_expires_at
                || new_expires_at > host.credential_expires_at
            {
                return Err(HostRegistryError::Invalid);
            }
            let mut renewed = lease;
            renewed.expires_at = new_expires_at;
            host.slots.insert(renewed.slot, renewed.clone());
            Ok((renewed, true))
        })
        .await
    }

    pub async fn release_slot(&self, lease: &WorkerSlotLease) -> Result<(), HostRegistryError> {
        let lease = lease.clone();
        self.transact(move |state| {
            let host = state
                .hosts
                .get_mut(&lease.host_ref)
                .ok_or(HostRegistryError::StaleLease)?;
            if host.connection_generation != lease.connection_generation
                || host.slots.get(&lease.slot) != Some(&lease)
            {
                return Err(HostRegistryError::StaleLease);
            }
            host.slots.remove(&lease.slot);
            Ok(((), true))
        })
        .await
    }

    /// Immediate disconnect or policy revocation. An old generation cannot
    /// revoke a successor's slots.
    pub async fn revoke_host(
        &self,
        host_ref: &str,
        connection_generation: &str,
        now: DateTime<Utc>,
    ) -> Result<(), HostRegistryError> {
        let host_ref = host_ref.to_owned();
        let connection_generation = connection_generation.to_owned();
        self.transact(move |state| {
            let host = state
                .hosts
                .get_mut(&host_ref)
                .ok_or(HostRegistryError::StaleGeneration)?;
            if host.connection_generation != connection_generation {
                return Err(HostRegistryError::StaleGeneration);
            }
            host.lease_expires_at = now.min(host.credential_expires_at);
            host.slots.clear();
            Ok(((), true))
        })
        .await
    }
}
