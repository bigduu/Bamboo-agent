//! Bounded producer checkpoint for the canonical Supervisor relationship.
//!
//! This is not a delivery queue or an authority grant. Only the integrated
//! canonical writer may persist it after checking the current Observer epoch;
//! only a real SessionInbox receipt permits committing a pending observation.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
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

/// The view is already published. These selectors confer no file/read right.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverViewChange {
    pub scope_epoch: u64,
    pub sequence: u64,
    pub target_session_id: String,
    pub view_revision: String,
    pub source_fingerprint: String,
    pub kind: ObserverChangeKind,
    pub observed_at: DateTime<Utc>,
    pub event_id: SessionMessageId,
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
    pub delivered_sequence: u64,
    pub delivered_fingerprints: BTreeMap<String, String>,
    pub pending: Option<ObserverViewChange>,
    /// Bounded derived change references; canonical Inbox remains ACK truth.
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
    pub delivered_sequence: u64,
    pub gap: bool,
    pub changes: Vec<ObserverViewChange>,
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
        SessionMessageId::stable(
            "supervisor_observation_v1",
            &serde_json::json!({
                "supervisor_incarnation":owner.supervisor_incarnation,
                "supervisor_created_at":owner.supervisor_created_at,
                "observer_session_id":owner.observer_session_id,
                "observer_created_at":owner.observer_created_at,
                "scope_epoch":self.scope_epoch, "sequence":self.sequence,
                "target_session_id":self.target_session_id,
                "view_revision":self.view_revision,
                "source_fingerprint":self.source_fingerprint,
                "kind":self.kind, "observed_at":self.observed_at,
            }),
        )
    }
}

impl ObserverSubscriptionWindow {
    pub fn new(scope_epoch: u64) -> Result<Self, &'static str> {
        let window = Self {
            version: OBSERVER_SUBSCRIPTION_VERSION,
            scope_epoch,
            delivered_sequence: 0,
            delivered_fingerprints: BTreeMap::new(),
            pending: None,
            history: Vec::new(),
        };
        window.validate()?;
        Ok(window)
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        if self.version != OBSERVER_SUBSCRIPTION_VERSION
            || self.scope_epoch == 0
            || self.delivered_fingerprints.len() > MAX_OBSERVER_WINDOW_TARGETS
            || self.history.len() > MAX_OBSERVER_CHANGE_HISTORY
            || serde_json::to_vec(self)
                .map_err(|_| "window encoding")?
                .len()
                > MAX_OBSERVER_WINDOW_BYTES
        {
            return Err("invalid or oversized Observer subscription window");
        }
        for (target, fingerprint) in &self.delivered_fingerprints {
            crate::validate_supervisor_target_id(target)?;
            if !digest_shape(fingerprint) {
                return Err("invalid Observer source fingerprint");
            }
        }
        let first = self
            .delivered_sequence
            .checked_sub(self.history.len() as u64)
            .ok_or("invalid Observer history length")?;
        for (index, change) in self.history.iter().enumerate() {
            change.validate()?;
            if change.scope_epoch != self.scope_epoch || change.sequence != first + index as u64 + 1
            {
                return Err("noncontiguous Observer history");
            }
        }
        if (self.delivered_sequence == 0) != self.history.is_empty() {
            return Err("missing Observer delivered history");
        }
        if let Some(pending) = &self.pending {
            pending.validate()?;
            if pending.scope_epoch != self.scope_epoch
                || Some(pending.sequence) != self.delivered_sequence.checked_add(1)
                || (!self
                    .delivered_fingerprints
                    .contains_key(&pending.target_session_id)
                    && self.delivered_fingerprints.len() >= MAX_OBSERVER_WINDOW_TARGETS)
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
        kind: ObserverChangeKind,
        observed_at: DateTime<Utc>,
    ) -> Result<ObserverStageOutcome, &'static str> {
        self.validate_for_owner(owner)?;
        if let Some(pending) = &self.pending {
            return Ok(ObserverStageOutcome::ExistingPending(pending.clone()));
        }
        if self.delivered_fingerprints.get(target).map(String::as_str) == Some(source_fingerprint) {
            return Ok(ObserverStageOutcome::Unchanged);
        }
        if !self.delivered_fingerprints.contains_key(target)
            && self.delivered_fingerprints.len() >= MAX_OBSERVER_WINDOW_TARGETS
        {
            return Err("Observer target capacity exceeded");
        }
        let mut change = ObserverViewChange {
            scope_epoch: self.scope_epoch,
            sequence: self
                .delivered_sequence
                .checked_add(1)
                .ok_or("Observer sequence exhausted")?,
            target_session_id: target.into(),
            view_revision: view_revision.into(),
            source_fingerprint: source_fingerprint.into(),
            kind,
            observed_at,
            event_id: SessionMessageId::new(),
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
    pub fn commit_admitted(
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
        candidate.delivered_sequence = pending.sequence;
        candidate.delivered_fingerprints.insert(
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
        if sequence > self.delivered_sequence {
            return Err("Observer cursor is ahead of the delivered sequence");
        }
        let oldest = self.history.first().map_or(1, |change| change.sequence);
        Ok(ObserverChangePage {
            scope_epoch: self.scope_epoch,
            after_sequence: sequence,
            delivered_sequence: self.delivered_sequence,
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
                ObserverChangeKind::Status,
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
                ObserverChangeKind::NeedsInput,
                Utc::now(),
            )
            .unwrap();
        assert_eq!(outcome, ObserverStageOutcome::ExistingPending(pending));
        assert_eq!(serde_json::to_vec(&reopened).unwrap(), persisted);
        assert_eq!(reopened.delivered_sequence, 0);
        assert!(reopened.changes_after(7, 0).unwrap().changes.is_empty());
    }

    #[test]
    fn mismatched_or_zero_generation_receipt_never_advances_cursor() {
        let mut window = ObserverSubscriptionWindow::new(7).unwrap();
        let pending = stage(&mut window, 1);
        let before = window.clone();
        let mut other = receipt(&pending);
        other.id = SessionMessageId::new();
        assert!(window.commit_admitted(&owner(), &other).is_err());
        other = receipt(&pending);
        other.generation = 0;
        assert!(window.commit_admitted(&owner(), &other).is_err());
        assert_eq!(window, before);
    }

    #[test]
    fn matching_receipt_commit_is_replay_safe_and_unchanged_source_coalesces() {
        let mut window = ObserverSubscriptionWindow::new(7).unwrap();
        let pending = stage(&mut window, 1);
        assert!(window
            .commit_admitted(&owner(), &receipt(&pending))
            .unwrap());
        assert!(!window
            .commit_admitted(&owner(), &receipt(&pending))
            .unwrap());
        assert_eq!(window.delivered_sequence, 1);
        let outcome = window
            .stage(
                &owner(),
                "root-a",
                &"a".repeat(64),
                &pending.source_fingerprint,
                ObserverChangeKind::Status,
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
                .commit_admitted(&owner(), &receipt(&pending))
                .unwrap();
        }
        let page = window.changes_after(7, 0).unwrap();
        assert!(page.gap);
        assert_eq!(page.delivered_sequence, 20);
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
            .commit_admitted(&owner(), &receipt(&pending))
            .is_err());
        assert_eq!(window, before);
        window.pending.as_mut().unwrap().scope_epoch = 8;
        assert!(window.validate().is_err());
    }
}
