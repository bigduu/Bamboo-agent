//! Trusted WorkerHost capacity and placement contracts.
//!
//! A host is replaceable execution capacity. These types never define an
//! ActorSession identity or grant a Run; the ActorDirectory remains authoritative.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{ActorPlacementClass, ActorPlacementRef, ProjectId};

pub const HOST_REGISTRY_SCHEMA_VERSION: u32 = 1;
pub const MAX_WORKER_HOST_SLOTS: u16 = 64;
pub const MAX_REGISTERED_HOSTS: usize = 256;

fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn identifiers(values: &BTreeSet<String>) -> bool {
    values.len() <= 256 && values.iter().all(|value| identifier(value))
}

/// Capabilities come from trusted Host configuration, never from a worker Run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerHostCapabilities {
    pub placement_class: ActorPlacementClass,
    pub project_ids: BTreeSet<ProjectId>,
    pub allow_unscoped_project: bool,
    pub trust_zone: String,
    /// Opaque workspace identities. A matching label does not replace the
    /// per-Run EnvironmentLease snapshot check.
    pub workspace_labels: BTreeSet<String>,
    pub executors: BTreeSet<String>,
    pub tools: BTreeSet<String>,
    pub network_zones: BTreeSet<String>,
    /// The Host can enforce a no-network Run boundary.
    pub network_isolation: bool,
}

impl WorkerHostCapabilities {
    pub fn validate(&self) -> Result<(), HostRegistryError> {
        if self.project_ids.len() > 256
            || !identifier(&self.trust_zone)
            || !identifiers(&self.workspace_labels)
            || !identifiers(&self.executors)
            || !identifiers(&self.tools)
            || !identifiers(&self.network_zones)
            || self.executors.is_empty()
        {
            return Err(HostRegistryError::Invalid);
        }
        Ok(())
    }
}

/// One authenticated broker observation plus trusted capability policy.
/// `expected_connection_generation` is a CAS against the last observation;
/// an older concurrent query cannot restore an already replaced connection.
#[derive(Debug, Clone)]
pub struct WorkerHostRegistration {
    pub host_ref: String,
    pub mailbox: String,
    pub role: Option<String>,
    pub connection_generation: String,
    pub credential_expires_at: DateTime<Utc>,
    pub observed_at: DateTime<Utc>,
    pub lease_expires_at: DateTime<Utc>,
    pub expected_connection_generation: Option<String>,
    pub max_slots: u16,
    pub capabilities: WorkerHostCapabilities,
}

impl WorkerHostRegistration {
    pub fn validate(&self) -> Result<(), HostRegistryError> {
        self.capabilities.validate()?;
        if !identifier(&self.host_ref)
            || !identifier(&self.mailbox)
            || self.role.as_deref().is_some_and(|role| !identifier(role))
            || !identifier(&self.connection_generation)
            || self
                .expected_connection_generation
                .as_deref()
                .is_some_and(|generation| !identifier(generation))
            || self.max_slots == 0
            || self.max_slots > MAX_WORKER_HOST_SLOTS
            || self.lease_expires_at <= self.observed_at
            || self.lease_expires_at - self.observed_at > chrono::Duration::minutes(2)
            || self.lease_expires_at > self.credential_expires_at
        {
            return Err(HostRegistryError::Invalid);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerSlotLease {
    pub lease_id: String,
    pub host_ref: String,
    pub connection_generation: String,
    pub slot: u16,
    pub epoch: u64,
    pub actor_id: String,
    pub run_id: String,
    pub expires_at: DateTime<Utc>,
}

/// Preflight selectors for an ActorActivation's persisted placement reference.
/// The caller must obtain the observed host and connection generation from a
/// fresh, authenticated broker observation and separately verify the exact
/// ActorDirectory activation fence. Worker-provided values have no authority.
#[derive(Debug, Clone)]
pub struct WorkerRunReservationCheck {
    pub placement_ref: ActorPlacementRef,
    pub actor_id: String,
    pub run_id: String,
    pub observed_host_ref: String,
    pub observed_connection_generation: String,
    pub now: DateTime<Utc>,
}

impl WorkerRunReservationCheck {
    pub fn validate(&self) -> Result<(), HostRegistryError> {
        if !identifier(&self.placement_ref.lease_id)
            || !matches!(self.placement_ref.slot_epoch, Some(epoch) if epoch > 0)
            || !identifier(&self.actor_id)
            || !identifier(&self.run_id)
            || !identifier(&self.observed_host_ref)
            || !identifier(&self.observed_connection_generation)
        {
            return Err(HostRegistryError::Invalid);
        }
        Ok(())
    }
}

/// One file-locked registry snapshot for a Run delivery preflight. It does not
/// grant execution: the broker must still condition delivery and durable
/// replay on this connection generation, and the worker must check its current
/// ActorDirectory fence before executing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerRunReservation {
    pub lease: WorkerSlotLease,
    pub mailbox: String,
    pub role: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisteredWorkerHost {
    pub host_ref: String,
    pub mailbox: String,
    pub role: Option<String>,
    pub connection_generation: String,
    pub credential_expires_at: DateTime<Utc>,
    pub observed_at: DateTime<Utc>,
    pub lease_expires_at: DateTime<Utc>,
    pub max_slots: u16,
    pub capabilities: WorkerHostCapabilities,
    pub next_slot_epoch: u64,
    pub slots: BTreeMap<u16, WorkerSlotLease>,
}

impl RegisteredWorkerHost {
    pub fn is_live(&self, now: DateTime<Utc>) -> bool {
        self.lease_expires_at > now && self.credential_expires_at > now
    }

    pub fn active_slots(&self, now: DateTime<Utc>) -> usize {
        self.slots
            .values()
            .filter(|lease| lease.expires_at > now)
            .count()
    }

    pub fn first_free_slot(&self, now: DateTime<Utc>) -> Option<u16> {
        (0..self.max_slots).find(|slot| {
            self.slots
                .get(slot)
                .is_none_or(|lease| lease.expires_at <= now)
        })
    }

    pub fn validate(&self) -> Result<(), HostRegistryError> {
        self.capabilities.validate()?;
        if !identifier(&self.host_ref)
            || !identifier(&self.mailbox)
            || self.role.as_deref().is_some_and(|role| !identifier(role))
            || !identifier(&self.connection_generation)
            || self.max_slots == 0
            || self.max_slots > MAX_WORKER_HOST_SLOTS
            || self.lease_expires_at > self.credential_expires_at
            || self.slots.iter().any(|(slot, lease)| {
                *slot >= self.max_slots
                    || lease.slot != *slot
                    || lease.host_ref != self.host_ref
                    || lease.connection_generation != self.connection_generation
                    || lease.epoch == 0
                    || lease.epoch > self.next_slot_epoch
                    || !identifier(&lease.lease_id)
                    || !identifier(&lease.actor_id)
                    || !identifier(&lease.run_id)
            })
        {
            return Err(HostRegistryError::Corrupt);
        }
        Ok(())
    }
}

/// Internal runtime selection mode; no host field is exposed to the model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostPlacementIntent {
    Auto,
    Local { host_ref: String },
    Pool { pool_id: String },
    PinnedHost { host_ref: String },
}

#[derive(Debug, Clone)]
pub struct HostPlacementRequest {
    pub intent: HostPlacementIntent,
    pub actor_id: String,
    pub run_id: String,
    pub project_id: Option<ProjectId>,
    pub trust_zone: String,
    pub workspace_label: Option<String>,
    pub executor: String,
    pub required_tools: BTreeSet<String>,
    pub network_zone: Option<String>,
    pub require_network_isolation: bool,
    /// Soft preference only; all hard constraints still apply.
    pub preferred_host_ref: Option<String>,
    pub now: DateTime<Utc>,
    pub lease_expires_at: DateTime<Utc>,
}

impl HostPlacementRequest {
    pub fn validate(&self) -> Result<(), HostRegistryError> {
        let intent_valid = match &self.intent {
            HostPlacementIntent::Auto => true,
            HostPlacementIntent::Local { host_ref }
            | HostPlacementIntent::PinnedHost { host_ref } => identifier(host_ref),
            HostPlacementIntent::Pool { pool_id } => identifier(pool_id),
        };
        if !intent_valid
            || !identifier(&self.actor_id)
            || !identifier(&self.run_id)
            || !identifier(&self.trust_zone)
            || !identifier(&self.executor)
            || self
                .workspace_label
                .as_deref()
                .is_some_and(|v| !identifier(v))
            || self.network_zone.as_deref().is_some_and(|v| !identifier(v))
            || self
                .preferred_host_ref
                .as_deref()
                .is_some_and(|v| !identifier(v))
            || !identifiers(&self.required_tools)
            || self.lease_expires_at <= self.now
            || self.lease_expires_at - self.now > chrono::Duration::minutes(5)
        {
            return Err(HostRegistryError::Invalid);
        }
        Ok(())
    }

    pub fn eligible(&self, host: &RegisteredWorkerHost) -> bool {
        if !host.is_live(self.now)
            || host.first_free_slot(self.now).is_none()
            || host.capabilities.trust_zone != self.trust_zone
            || !host.capabilities.executors.contains(&self.executor)
            || !self.required_tools.is_subset(&host.capabilities.tools)
            || !self
                .project_id
                .as_ref()
                .map_or(host.capabilities.allow_unscoped_project, |id| {
                    host.capabilities.project_ids.contains(id)
                })
            || self
                .workspace_label
                .as_ref()
                .is_some_and(|label| !host.capabilities.workspace_labels.contains(label))
            || self
                .network_zone
                .as_ref()
                .is_some_and(|zone| !host.capabilities.network_zones.contains(zone))
            || (self.require_network_isolation && !host.capabilities.network_isolation)
        {
            return false;
        }
        match &self.intent {
            HostPlacementIntent::Auto => true,
            HostPlacementIntent::Local { host_ref } => {
                host.host_ref == *host_ref
                    && host.capabilities.placement_class == ActorPlacementClass::Local
            }
            HostPlacementIntent::PinnedHost { host_ref } => host.host_ref == *host_ref,
            HostPlacementIntent::Pool { pool_id } => host.role.as_deref() == Some(pool_id),
        }
    }
}

/// Pure, deterministic hard-filter-then-score selection. The storage layer
/// calls this while holding the registry's cross-process transaction lock.
pub fn select_worker_host<'a>(
    hosts: &'a BTreeMap<String, RegisteredWorkerHost>,
    request: &HostPlacementRequest,
) -> Result<&'a RegisteredWorkerHost, HostRegistryError> {
    request.validate()?;
    let selected = hosts
        .values()
        .filter(|host| request.eligible(host))
        .min_by(|a, b| {
            let load = (a.active_slots(request.now) as u64 * b.max_slots as u64)
                .cmp(&(b.active_slots(request.now) as u64 * a.max_slots as u64));
            load.then_with(|| {
                let a_preferred =
                    request.preferred_host_ref.as_deref() == Some(a.host_ref.as_str());
                let b_preferred =
                    request.preferred_host_ref.as_deref() == Some(b.host_ref.as_str());
                b_preferred.cmp(&a_preferred)
            })
            .then_with(|| a.host_ref.cmp(&b.host_ref))
        });
    selected.ok_or_else(|| match &request.intent {
        HostPlacementIntent::Local { .. } | HostPlacementIntent::PinnedHost { .. } => {
            HostRegistryError::PinnedUnavailable
        }
        _ => HostRegistryError::NoEligibleHost,
    })
}

#[derive(Debug, Error)]
pub enum HostRegistryError {
    #[error("invalid WorkerHost registry request")]
    Invalid,
    #[error("WorkerHost registry is corrupt or its initialized record is missing")]
    Corrupt,
    #[error("WorkerHost connection generation changed")]
    StaleGeneration,
    #[error("WorkerHost slot lease is stale")]
    StaleLease,
    #[error("WorkerHost capacity is busy")]
    Busy,
    #[error("pinned WorkerHost is unavailable or violates a hard constraint")]
    PinnedUnavailable,
    #[error("no eligible WorkerHost capacity")]
    NoEligibleHost,
    #[error("WorkerHost registry storage failure: {0}")]
    Storage(String),
}
