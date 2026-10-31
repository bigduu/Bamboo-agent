//! Bounded producer checkpoint for the canonical Supervisor relationship.
//!
//! This is not a delivery queue or an authority grant. Only the integrated
//! canonical writer may persist it after checking the current Observer epoch;
//! only a real SessionInbox enqueue receipt permits advancing the producer
//! cursor. Consumer admission/ACK stays exclusively in the existing Inbox.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::session::inbox::{canonical_json_bytes, SessionInboxReceipt, SessionMessageId};

pub const OBSERVER_SUBSCRIPTION_VERSION: u32 = 1;
pub const MAX_OBSERVER_CHANGE_HISTORY: usize = 16;
pub const MAX_OBSERVER_WINDOW_TARGETS: usize = 8;
pub const MAX_OBSERVER_CHANGE_BYTES: usize = 1024;
pub const MAX_OBSERVER_WINDOW_BYTES: usize = 24 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObserverChangeKind {
    Status,
    NeedsInput,
    Tasks,
}

pub fn observer_filtered_fingerprint(
    filter: &BTreeSet<ObserverChangeKind>,
    values: &BTreeMap<ObserverChangeKind, String>,
) -> Result<String, &'static str> {
    if filter.is_empty() {
        return Err("empty Observer filter");
    }
    let selected: BTreeMap<_, _> = filter
        .iter()
        .map(|kind| {
            let value = values
                .get(kind)
                .filter(|value| digest_shape(value))
                .ok_or("missing Observer fingerprint")?;
            Ok((kind, value))
        })
        .collect::<Result<_, &'static str>>()?;
    Ok(hex::encode(Sha256::digest(canonical_json_bytes(
        &serde_json::to_value(selected).map_err(|_| "fingerprint encoding")?,
    ))))
}

/// The view is already published. These selectors confer no file/read right.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverRootOutcomeCursor {
    pub root_created_at: DateTime<Utc>,
    pub sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverViewChange {
    pub scope_epoch: u64,
    pub sequence: u64,
    pub target_session_id: String,
    pub view_revision: String,
    pub source_fingerprint: String,
    pub observed_kinds: BTreeSet<ObserverChangeKind>,
    pub observed_at: DateTime<Utc>,
    pub event_id: SessionMessageId,
    /// Optional historical Root result, not a new grant or consumer ACK.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_outcome: Option<ObserverRootOutcomeCursor>,
}

/// Transient values from the locked canonical registration, never another
/// serialized identity or a caller-provided authorization capability.
pub struct ObserverSubscriptionOwner<'a> {
    pub supervisor_incarnation: Uuid,
    pub supervisor_created_at: DateTime<Utc>,
    pub observer_session_id: &'a str,
    pub observer_created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverSubscriptionWindow {
    pub version: u32,
    /// Exact registration epoch. Relation/cursor writes do not advance it.
    pub scope_epoch: u64,
    pub enqueued_sequence: u64,
    pub enqueued_fingerprints: BTreeMap<String, String>,
    /// Source position advances only with this pending view's Inbox receipt.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub root_outcome_cursors: BTreeMap<String, ObserverRootOutcomeCursor>,
    pub pending: Option<ObserverViewChange>,
    /// Bounded enqueue references, never a consumer/model watermark. Canonical
    /// Inbox remains consumer ACK truth.
    pub history: Vec<ObserverViewChange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObserverStageOutcome {
    Unchanged,
    ExistingPending(ObserverViewChange),
    Staged(ObserverViewChange),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ObserverChangePage {
    pub scope_epoch: u64,
    pub after_sequence: u64,
    pub enqueued_sequence: u64,
    pub gap: bool,
    pub changes: Vec<ObserverViewChange>,
}

/// A bounded derived read, never an Inbox admission/ACK log. On a gap the
/// reader must request fresh scoped snapshots for the named assigned targets.
#[derive(Debug, Clone, Serialize)]
pub struct ObserverChangeRead {
    pub page: ObserverChangePage,
    pub jsonl_shards: Vec<String>,
    pub snapshot_targets: Vec<String>,
    pub shard_revision: String,
    pub shard_directory: std::path::PathBuf,
}

fn digest_shape(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

impl ObserverViewChange {
    fn validate(&self) -> Result<(), &'static str> {
        crate::validate_supervisor_target_id(&self.target_session_id)?;
        if self.scope_epoch == 0
            || self.sequence == 0
            || self.observed_kinds.is_empty()
            || self
                .root_outcome
                .as_ref()
                .is_some_and(|position| position.sequence == 0)
            || !digest_shape(&self.view_revision)
            || !digest_shape(&self.source_fingerprint)
            || canonical_json_bytes(&serde_json::to_value(self).map_err(|_| "change encoding")?)
                .len()
                > MAX_OBSERVER_CHANGE_BYTES
        {
            return Err("invalid or oversized Observer change");
        }
        Ok(())
    }

    pub fn expected_event_id(&self, owner: &ObserverSubscriptionOwner<'_>) -> SessionMessageId {
        let mut identity = serde_json::json!({
            "supervisor_incarnation":owner.supervisor_incarnation,
            "supervisor_created_at":owner.supervisor_created_at,
            "observer_session_id":owner.observer_session_id,
            "observer_created_at":owner.observer_created_at,
            "scope_epoch":self.scope_epoch, "sequence":self.sequence,
            "target_session_id":self.target_session_id,
            "view_revision":self.view_revision,
            "source_fingerprint":self.source_fingerprint,
            "observed_kinds":self.observed_kinds, "observed_at":self.observed_at,
        });
        if let Some(outcome) = &self.root_outcome {
            identity["root_outcome"] = serde_json::json!(outcome);
        }
        SessionMessageId::stable("supervisor_observation_v1", &identity)
    }
}

impl ObserverSubscriptionWindow {
    pub fn new(scope_epoch: u64) -> Result<Self, &'static str> {
        let window = Self {
            version: OBSERVER_SUBSCRIPTION_VERSION,
            scope_epoch,
            enqueued_sequence: 0,
            enqueued_fingerprints: BTreeMap::new(),
            root_outcome_cursors: BTreeMap::new(),
            pending: None,
            history: Vec::new(),
        };
        window.validate()?;
        Ok(window)
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        if self.version != OBSERVER_SUBSCRIPTION_VERSION
            || self.scope_epoch == 0
            || self.enqueued_fingerprints.len() > MAX_OBSERVER_WINDOW_TARGETS
            || self.root_outcome_cursors.len() > MAX_OBSERVER_WINDOW_TARGETS
            || self.history.len() > MAX_OBSERVER_CHANGE_HISTORY
            || serde_json::to_vec(self)
                .map_err(|_| "window encoding")?
                .len()
                > MAX_OBSERVER_WINDOW_BYTES
        {
            return Err("invalid or oversized Observer subscription window");
        }
        for (target, fingerprint) in &self.enqueued_fingerprints {
            crate::validate_supervisor_target_id(target)?;
            if !digest_shape(fingerprint) {
                return Err("invalid Observer source fingerprint");
            }
        }
        for target in self.root_outcome_cursors.keys() {
            crate::validate_supervisor_target_id(target)?;
        }
        let first = self
            .enqueued_sequence
            .checked_sub(self.history.len() as u64)
            .ok_or("invalid Observer history length")?;
        for (index, change) in self.history.iter().enumerate() {
            change.validate()?;
            if change.scope_epoch != self.scope_epoch || change.sequence != first + index as u64 + 1
            {
                return Err("noncontiguous Observer history");
            }
        }
        if (self.enqueued_sequence == 0) != self.history.is_empty() {
            return Err("missing Observer enqueue history");
        }
        if let Some(pending) = &self.pending {
            pending.validate()?;
            if let Some(outcome) = &pending.root_outcome {
                if outcome.sequence == 0
                    || self
                        .root_outcome_cursors
                        .get(&pending.target_session_id)
                        .is_some_and(|current| {
                            current.root_created_at != outcome.root_created_at
                                || current.sequence >= outcome.sequence
                        })
                {
                    return Err("invalid pending Observer Root outcome position");
                }
            }
            if pending.scope_epoch != self.scope_epoch
                || Some(pending.sequence) != self.enqueued_sequence.checked_add(1)
                || (!self
                    .enqueued_fingerprints
                    .contains_key(&pending.target_session_id)
                    && self.enqueued_fingerprints.len() >= MAX_OBSERVER_WINDOW_TARGETS)
            {
                return Err("invalid Observer pending sequence or epoch");
            }
        }
        Ok(())
    }

    pub fn validate_for_owner(
        &self,
        owner: &ObserverSubscriptionOwner<'_>,
    ) -> Result<(), &'static str> {
        self.validate()?;
        crate::validate_supervisor_target_id(owner.observer_session_id)?;
        if owner.supervisor_incarnation.is_nil() {
            return Err("invalid Observer owner incarnation");
        }
        for change in self.history.iter().chain(self.pending.iter()) {
            if change.event_id != change.expected_event_id(owner) {
                return Err("Observer event identity changed");
            }
        }
        Ok(())
    }

    /// Preserve a published pending reference even when the live source changes.
    /// The caller must persist Staged before attempting physical Inbox delivery.
    pub fn stage(
        &mut self,
        owner: &ObserverSubscriptionOwner<'_>,
        target: &str,
        view_revision: &str,
        source_fingerprint: &str,
        observed_kinds: BTreeSet<ObserverChangeKind>,
        observed_at: DateTime<Utc>,
    ) -> Result<ObserverStageOutcome, &'static str> {
        self.stage_with_root_outcome(
            owner,
            target,
            view_revision,
            source_fingerprint,
            observed_kinds,
            observed_at,
            None,
        )
    }

    /// A historical Root selector is frozen with the existing pending view.
    /// Its position is not advanced until the matching Inbox receipt commits.
    pub fn stage_with_root_outcome(
        &mut self,
        owner: &ObserverSubscriptionOwner<'_>,
        target: &str,
        view_revision: &str,
        source_fingerprint: &str,
        observed_kinds: BTreeSet<ObserverChangeKind>,
        observed_at: DateTime<Utc>,
        root_outcome: Option<ObserverRootOutcomeCursor>,
    ) -> Result<ObserverStageOutcome, &'static str> {
        self.validate_for_owner(owner)?;
        if let Some(pending) = &self.pending {
            return Ok(ObserverStageOutcome::ExistingPending(pending.clone()));
        }
        if root_outcome.is_none()
            && self.enqueued_fingerprints.get(target).map(String::as_str)
                == Some(source_fingerprint)
        {
            return Ok(ObserverStageOutcome::Unchanged);
        }
        if !self.enqueued_fingerprints.contains_key(target)
            && self.enqueued_fingerprints.len() >= MAX_OBSERVER_WINDOW_TARGETS
        {
            return Err("Observer target capacity exceeded");
        }
        let mut change = ObserverViewChange {
            scope_epoch: self.scope_epoch,
            sequence: self
                .enqueued_sequence
                .checked_add(1)
                .ok_or("Observer sequence exhausted")?,
            target_session_id: target.into(),
            view_revision: view_revision.into(),
            source_fingerprint: source_fingerprint.into(),
            observed_kinds,
            observed_at,
            event_id: SessionMessageId::new(),
            root_outcome,
        };
        change.event_id = change.expected_event_id(owner);
        change.validate()?;
        let mut candidate = self.clone();
        candidate.pending = Some(change.clone());
        candidate.validate_for_owner(owner)?;
        *self = candidate;
        Ok(ObserverStageOutcome::Staged(change))
    }

    /// A matching real Inbox receipt is required by the integrated writer.
    /// This pure data transition cannot inspect or manufacture that receipt.
    pub fn commit_enqueued(
        &mut self,
        owner: &ObserverSubscriptionOwner<'_>,
        receipt: &SessionInboxReceipt,
    ) -> Result<bool, &'static str> {
        self.validate_for_owner(owner)?;
        if receipt.generation == 0 {
            return Err("Observer receipt lacks a delivery generation");
        }
        let event_id = &receipt.id;
        let Some(pending) = self.pending.as_ref() else {
            return if self
                .history
                .last()
                .is_some_and(|last| &last.event_id == event_id)
            {
                Ok(false)
            } else {
                Err("Observer receipt has no matching pending source")
            };
        };
        if &pending.event_id != event_id {
            return Err("Observer receipt belongs to another event");
        }
        let mut candidate = self.clone();
        let pending = candidate.pending.take().expect("matching pending checked");
        candidate.enqueued_sequence = pending.sequence;
        if let Some(position) = pending.root_outcome.clone() {
            candidate
                .root_outcome_cursors
                .insert(pending.target_session_id.clone(), position);
        }
        candidate.enqueued_fingerprints.insert(
            pending.target_session_id.clone(),
            pending.source_fingerprint.clone(),
        );
        candidate.history.push(pending);
        if candidate.history.len() > MAX_OBSERVER_CHANGE_HISTORY {
            candidate.history.remove(0);
        }
        candidate.validate_for_owner(owner)?;
        *self = candidate;
        Ok(true)
    }

    pub fn changes_after(
        &self,
        scope_epoch: u64,
        sequence: u64,
    ) -> Result<ObserverChangePage, &'static str> {
        self.validate()?;
        if scope_epoch != self.scope_epoch {
            return Err("Observer cursor belongs to another registration epoch");
        }
        if sequence > self.enqueued_sequence {
            return Err("Observer cursor is ahead of the enqueue sequence");
        }
        let oldest = self.history.first().map_or(1, |change| change.sequence);
        Ok(ObserverChangePage {
            scope_epoch: self.scope_epoch,
            after_sequence: sequence,
            enqueued_sequence: self.enqueued_sequence,
            gap: sequence.saturating_add(1) < oldest,
            changes: self
                .history
                .iter()
                .filter(|change| change.sequence > sequence)
                .cloned()
                .collect(),
        })
    }

    /// Bounded derived shard bytes. They confer no consumer ACK or authority.
    pub fn jsonl_changes_after(
        &self,
        scope_epoch: u64,
        sequence: u64,
    ) -> Result<Vec<u8>, &'static str> {
        let page = self.changes_after(scope_epoch, sequence)?;
        let mut bytes = Vec::new();
        for change in page.changes {
            bytes.extend(canonical_json_bytes(
                &serde_json::to_value(change).map_err(|_| "change encoding")?,
            ));
            bytes.push(b'\n');
        }
        Ok(bytes)
    }
}

/// One bounded producer policy, persisted with its canonical registration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverSubscription {
    pub filter: BTreeSet<ObserverChangeKind>,
    pub min_interval_ms: u32,
    pub window: ObserverSubscriptionWindow,
    pub failures: u8,
    pub retry_at: Option<DateTime<Utc>>,
    pub last_error: Option<ObserverSubscriptionFailure>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_fault: Option<ObserverTerminalFault>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObserverSubscriptionFailure {
    PublicationUnavailable,
    InboxUnavailable,
    AuthorityUnavailable,
}

/// An administrative diagnostic about this subscription, never source content
/// or a ChildOutcome. Payload and identity freeze at the first terminal failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverFaultPayload {
    pub supervisor_incarnation: Uuid,
    pub supervisor_created_at: DateTime<Utc>,
    pub observer_session_id: String,
    pub observer_created_at: DateTime<Utc>,
    pub scope_epoch: u64,
    pub code: ObserverSubscriptionFailure,
    pub failures: u8,
    pub exhausted_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverTerminalFault {
    pub payload: ObserverFaultPayload,
    pub event_id: SessionMessageId,
    /// Producer receipt only. Consumer ACK remains exclusively in Inbox.
    pub reported: bool,
    pub delivery_failures: u8,
    pub retry_at: Option<DateTime<Utc>>,
}

impl ObserverTerminalFault {
    pub fn expected_event_id(&self) -> SessionMessageId {
        SessionMessageId::stable(
            "observer_subscription_fault_v1",
            &serde_json::to_value(&self.payload).expect("typed diagnostic JSON"),
        )
    }
    pub fn ready(&self, now: DateTime<Utc>) -> bool {
        !self.reported && self.delivery_failures < 8 && self.retry_at.is_none_or(|at| at <= now)
    }
    pub fn delivery_failed(&mut self, now: DateTime<Utc>) {
        self.delivery_failures = self.delivery_failures.saturating_add(1).min(8);
        self.retry_at = (self.delivery_failures < 8)
            .then(|| now + chrono::Duration::seconds(1_i64 << self.delivery_failures));
    }
    pub fn validate_for_owner(
        &self,
        owner: &ObserverSubscriptionOwner<'_>,
        epoch: u64,
    ) -> Result<(), &'static str> {
        if self.payload.supervisor_incarnation != owner.supervisor_incarnation
            || self.payload.supervisor_created_at != owner.supervisor_created_at
            || self.payload.observer_session_id != owner.observer_session_id
            || self.payload.observer_created_at != owner.observer_created_at
            || self.payload.scope_epoch != epoch
            || self.payload.failures != 8
            || self.event_id != self.expected_event_id()
            || self.delivery_failures > 8
            || (!self.reported
                && self.delivery_failures > 0
                && self.delivery_failures < 8
                && self.retry_at.is_none())
            || ((self.reported || self.delivery_failures == 8) && self.retry_at.is_some())
        {
            return Err("invalid Observer terminal diagnostic");
        }
        Ok(())
    }
}

impl ObserverSubscription {
    pub fn new(
        epoch: u64,
        filter: BTreeSet<ObserverChangeKind>,
        min_interval_ms: u32,
    ) -> Result<Self, &'static str> {
        let value = Self {
            filter,
            min_interval_ms,
            window: ObserverSubscriptionWindow::new(epoch)?,
            failures: 0,
            retry_at: None,
            last_error: None,
            terminal_fault: None,
        };
        value.validate()?;
        Ok(value)
    }
    pub fn validate(&self) -> Result<(), &'static str> {
        self.window.validate()?;
        if self.filter.is_empty()
            || !(1_000..=3_600_000).contains(&self.min_interval_ms)
            || self.failures > 8
            || ((self.failures > 0) != self.last_error.is_some())
            || (self.failures == 8 && self.retry_at.is_some())
            || self
                .window
                .history
                .iter()
                .chain(self.window.pending.iter())
                .any(|change| !change.observed_kinds.is_subset(&self.filter))
        {
            return Err("invalid Observer subscription policy");
        }
        if let Some(fault) = &self.terminal_fault {
            if self.failures != 8 || Some(fault.payload.code) != self.last_error {
                return Err("terminal diagnostic differs from original failure");
            }
        }
        Ok(())
    }
    pub fn capture_terminal_fault(
        &mut self,
        owner: &ObserverSubscriptionOwner<'_>,
        now: DateTime<Utc>,
    ) -> Result<bool, &'static str> {
        if self.failures != 8 {
            return Err("Observer retry cap has not been reached");
        }
        if let Some(fault) = &self.terminal_fault {
            fault.validate_for_owner(owner, self.window.scope_epoch)?;
            return Ok(false);
        }
        let payload = ObserverFaultPayload {
            supervisor_incarnation: owner.supervisor_incarnation,
            supervisor_created_at: owner.supervisor_created_at,
            observer_session_id: owner.observer_session_id.into(),
            observer_created_at: owner.observer_created_at,
            scope_epoch: self.window.scope_epoch,
            code: self.last_error.ok_or("terminal failure code missing")?,
            failures: 8,
            exhausted_at: now,
        };
        let mut fault = ObserverTerminalFault {
            payload,
            event_id: SessionMessageId::new(),
            reported: false,
            delivery_failures: 0,
            retry_at: None,
        };
        fault.event_id = fault.expected_event_id();
        self.terminal_fault = Some(fault);
        Ok(true)
    }
    pub fn ready(&self, now: DateTime<Utc>) -> bool {
        self.terminal_fault.is_none()
            && self.failures < 8
            && self.retry_at.is_none_or(|at| at <= now)
    }
    pub fn record_failure(&mut self, now: DateTime<Utc>, code: ObserverSubscriptionFailure) {
        if self.failures >= 8 {
            return;
        }
        self.failures = self.failures.saturating_add(1).min(8);
        self.last_error = Some(code);
        self.retry_at = (self.failures < 8)
            .then(|| now + chrono::Duration::seconds((1_i64 << self.failures).min(300)));
    }
    pub fn delivered(&mut self, now: DateTime<Utc>) {
        self.failures = 0;
        self.last_error = None;
        self.retry_at = Some(now + chrono::Duration::milliseconds(i64::from(self.min_interval_ms)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn owner() -> ObserverSubscriptionOwner<'static> {
        ObserverSubscriptionOwner {
            supervisor_incarnation: Uuid::from_u128(1),
            supervisor_created_at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            observer_session_id: "observer-a",
            observer_created_at: Utc.timestamp_opt(1_700_000_001, 0).unwrap(),
        }
    }

    fn stage(window: &mut ObserverSubscriptionWindow, n: u64) -> ObserverViewChange {
        let ObserverStageOutcome::Staged(change) = window
            .stage(
                &owner(),
                "root-a",
                &format!("{:064x}", n + 1),
                &format!("{n:064x}"),
                BTreeSet::from([ObserverChangeKind::Status]),
                Utc.timestamp_opt(1_700_000_002, 0).unwrap(),
            )
            .unwrap()
        else {
            panic!("expected a new producer checkpoint");
        };
        change
    }

    fn receipt(change: &ObserverViewChange) -> SessionInboxReceipt {
        // Pure state-machine input, not actual Inbox/runtime acceptance proof.
        SessionInboxReceipt {
            id: change.event_id.clone(),
            generation: change.sequence,
        }
    }

    #[test]
    fn pending_checkpoint_survives_reopen_and_preserves_source_before_delivery() {
        let mut window = ObserverSubscriptionWindow::new(7).unwrap();
        let pending = stage(&mut window, 1);
        let persisted = serde_json::to_vec(&window).unwrap();
        let mut reopened: ObserverSubscriptionWindow = serde_json::from_slice(&persisted).unwrap();
        let outcome = reopened
            .stage(
                &owner(),
                "root-b",
                &"b".repeat(64),
                &"c".repeat(64),
                BTreeSet::from([ObserverChangeKind::NeedsInput]),
                Utc::now(),
            )
            .unwrap();
        assert_eq!(outcome, ObserverStageOutcome::ExistingPending(pending));
        assert_eq!(serde_json::to_vec(&reopened).unwrap(), persisted);
        assert_eq!(reopened.enqueued_sequence, 0);
        assert!(reopened.changes_after(7, 0).unwrap().changes.is_empty());
    }

    #[test]
    fn mismatched_or_zero_generation_receipt_never_advances_cursor() {
        let mut window = ObserverSubscriptionWindow::new(7).unwrap();
        let pending = stage(&mut window, 1);
        let before = window.clone();
        let mut other = receipt(&pending);
        other.id = SessionMessageId::new();
        assert!(window.commit_enqueued(&owner(), &other).is_err());
        other = receipt(&pending);
        other.generation = 0;
        assert!(window.commit_enqueued(&owner(), &other).is_err());
        assert_eq!(window, before);
    }

    #[test]
    fn root_outcome_position_advances_only_with_its_exact_inbox_receipt() {
        let mut window = ObserverSubscriptionWindow::new(7).unwrap();
        let position = ObserverRootOutcomeCursor {
            root_created_at: Utc.timestamp_opt(1_700_000_003, 0).unwrap(),
            sequence: 1,
        };
        let ObserverStageOutcome::Staged(pending) = window
            .stage_with_root_outcome(
                &owner(),
                "root-a",
                &"a".repeat(64),
                &"b".repeat(64),
                BTreeSet::from([ObserverChangeKind::Status]),
                Utc.timestamp_opt(1_700_000_004, 0).unwrap(),
                Some(position.clone()),
            )
            .unwrap()
        else {
            panic!("expected historical pending outcome");
        };
        assert!(window.root_outcome_cursors.is_empty());
        let bytes = serde_json::to_vec(&window).unwrap();
        let mut reopened: ObserverSubscriptionWindow = serde_json::from_slice(&bytes).unwrap();
        let mut wrong = receipt(&pending);
        wrong.id = SessionMessageId::new();
        assert!(reopened.commit_enqueued(&owner(), &wrong).is_err());
        assert_eq!(serde_json::to_vec(&reopened).unwrap(), bytes);
        assert!(reopened
            .commit_enqueued(&owner(), &receipt(&pending))
            .unwrap());
        assert_eq!(reopened.root_outcome_cursors["root-a"], position);
        assert!(!reopened
            .commit_enqueued(&owner(), &receipt(&pending))
            .unwrap());

        // An ordinary snapshot fingerprint must not short-circuit a distinct
        // historical position, even when the filtered status bytes match.
        let next = ObserverRootOutcomeCursor {
            sequence: 2,
            ..position
        };
        assert!(matches!(
            reopened.stage_with_root_outcome(
                &owner(),
                "root-a",
                &"c".repeat(64),
                &"b".repeat(64),
                BTreeSet::from([ObserverChangeKind::Status]),
                Utc.timestamp_opt(1_700_000_005, 0).unwrap(),
                Some(next),
            ),
            Ok(ObserverStageOutcome::Staged(_))
        ));
    }

    #[test]
    fn matching_receipt_commit_is_replay_safe_and_unchanged_source_coalesces() {
        let mut window = ObserverSubscriptionWindow::new(7).unwrap();
        let pending = stage(&mut window, 1);
        assert!(window
            .commit_enqueued(&owner(), &receipt(&pending))
            .unwrap());
        assert!(!window
            .commit_enqueued(&owner(), &receipt(&pending))
            .unwrap());
        assert_eq!(window.enqueued_sequence, 1);
        let outcome = window
            .stage(
                &owner(),
                "root-a",
                &"a".repeat(64),
                &pending.source_fingerprint,
                BTreeSet::from([ObserverChangeKind::Status]),
                Utc::now(),
            )
            .unwrap();
        assert_eq!(outcome, ObserverStageOutcome::Unchanged);
        assert!(window.pending.is_none());
    }

    #[test]
    fn rotated_history_exposes_a_gap_without_reusing_sequence_or_fingerprint_as_cursor() {
        let mut window = ObserverSubscriptionWindow::new(7).unwrap();
        for n in 1..=20 {
            let pending = stage(&mut window, n);
            window
                .commit_enqueued(&owner(), &receipt(&pending))
                .unwrap();
        }
        let page = window.changes_after(7, 0).unwrap();
        assert!(page.gap);
        assert_eq!(page.enqueued_sequence, 20);
        assert_eq!(page.changes.len(), MAX_OBSERVER_CHANGE_HISTORY);
        assert_eq!(page.changes[0].sequence, 5);
        assert!(!window.changes_after(7, 4).unwrap().gap);
        assert!(window.changes_after(7, 21).is_err());
        assert!(window.changes_after(8, 0).is_err());
        let shard = window.jsonl_changes_after(7, 0).unwrap();
        assert!(shard.len() <= MAX_OBSERVER_CHANGE_HISTORY * (MAX_OBSERVER_CHANGE_BYTES + 1));
        let decoded: Vec<ObserverViewChange> = std::str::from_utf8(&shard)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(decoded, page.changes);
    }

    #[test]
    fn regrant_epoch_or_observer_birth_cannot_reuse_an_event_identity() {
        let mut first = ObserverSubscriptionWindow::new(7).unwrap();
        let original = stage(&mut first, 1);
        let mut next = ObserverSubscriptionWindow::new(8).unwrap();
        assert_ne!(original.event_id, stage(&mut next, 1).event_id);
        let mut changed_owner = owner();
        changed_owner.observer_created_at += chrono::Duration::seconds(1);
        assert!(first.validate_for_owner(&changed_owner).is_err());
        assert!(first.validate_for_owner(&owner()).is_ok());
    }

    #[test]
    fn malformed_serialized_window_or_tampered_pending_is_not_admitted() {
        let mut window = ObserverSubscriptionWindow::new(7).unwrap();
        let pending = stage(&mut window, 1);
        let mut encoded = serde_json::to_value(&window).unwrap();
        encoded["unknown"] = serde_json::json!(true);
        assert!(serde_json::from_value::<ObserverSubscriptionWindow>(encoded).is_err());
        window.pending.as_mut().unwrap().event_id = SessionMessageId::new();
        let before = window.clone();
        assert!(window
            .commit_enqueued(&owner(), &receipt(&pending))
            .is_err());
        assert_eq!(window, before);
        window.pending.as_mut().unwrap().scope_epoch = 8;
        assert!(window.validate().is_err());
    }
}
