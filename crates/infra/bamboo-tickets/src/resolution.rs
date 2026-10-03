//! Durable zero-to-many Human resolution over the same HEAD/full manifest.
//! Models propose outside this service. Registration, proposal freeze and each
//! indivisible semantic group are short file-authority transactions.
mod model;
mod validation;
use crate::service::{apply_operations, validate_snapshot};
use crate::*;
pub use model::*;
use std::collections::BTreeSet;
pub(crate) use validation::validate_history;
use validation::{group_operation_id, group_status, user, validate_group, validate_shape};

impl TicketService {
    pub fn register_user_ingress(
        &self,
        authority: &Authority,
        ingress: &VerifiedUserIngress,
    ) -> Result<MessageResolution> {
        let mut store = self.inner.lock().expect("store mutex");
        let (_, current) = store
            .published
            .as_ref()
            .ok_or_else(|| Error::AuthorityUnavailable("no verified snapshot".into()))?;
        user(authority, current, &ingress.record)?;
        let hash = content_hash(&canonical_bytes(&ingress.record)?);
        if let Some(saved) = current.resolutions.get(&ingress.message_id) {
            let record = saved.ingress.as_ref().ok_or_else(|| {
                Error::ScopeDenied("legacy resolution cannot assert Human provenance".into())
            })?;
            user(authority, current, record)?;
            return if saved.message_hash == hash {
                Ok(saved.clone())
            } else {
                Err(Error::IdempotencyConflict)
            };
        }
        if current
            .resolutions
            .values()
            .filter_map(|r| r.ingress.as_ref())
            .map(|r| r.source_ingress_seq)
            .max()
            .unwrap_or(0)
            >= ingress.record.source_ingress_seq
        {
            return Err(Error::InvalidTransition(
                "canonical ingress is out of durable sequence".into(),
            ));
        }
        let record = MessageResolution {
            message_id: ingress.message_id.clone(),
            ingress_seq: current
                .resolutions
                .values()
                .map(|r| r.ingress_seq)
                .max()
                .unwrap_or(0)
                + 1,
            message_hash: hash,
            groups: vec![],
            ingress: Some(ingress.record.clone()),
            basis: None,
            proposal: None,
            proposal_hash: None,
            updated_seq: Some(current.seq + 1),
        };
        let mut next = current.clone();
        next.seq += 1;
        next.resolutions
            .insert(record.message_id.clone(), record.clone());
        validate_snapshot(&next)?;
        store.publish(next)?;
        Ok(record)
    }

    // Admission can run ahead of resolution. Pin only when processing starts,
    // so a queued message sees changes committed by its preceding Human turn.
    fn pin_message_basis(
        &self,
        authority: &Authority,
        message_id: &str,
    ) -> Result<MessageResolution> {
        let mut store = self.inner.lock().expect("store mutex");
        let (commit, current) = store
            .published
            .as_ref()
            .ok_or_else(|| Error::AuthorityUnavailable("no verified snapshot".into()))?;
        let mut saved = current
            .resolutions
            .get(message_id)
            .cloned()
            .ok_or_else(|| Error::InvalidTransition("message missing".into()))?;
        user(
            authority,
            current,
            saved
                .ingress
                .as_ref()
                .ok_or_else(|| Error::ScopeDenied("canonical Human provenance missing".into()))?,
        )?;
        if saved.basis.is_some() {
            return Ok(saved);
        }
        saved.basis = Some(ResolutionBasis {
            commit: commit.clone(),
            seq: current.seq,
            graph_revision: current.graph_revision,
            authority_epoch: current.authority_epoch,
        });
        let mut next = current.clone();
        next.seq += 1;
        saved.updated_seq = Some(next.seq);
        next.resolutions.insert(message_id.into(), saved.clone());
        validate_snapshot(&next)?;
        store.publish(next)?;
        Ok(saved)
    }

    pub fn message_resolution(
        &self,
        authority: &Authority,
        message_id: &str,
    ) -> Result<MessageResolution> {
        let (_, snapshot) = self.published()?;
        let record = snapshot
            .resolutions
            .get(message_id)
            .ok_or_else(|| Error::InvalidTransition("message resolution missing".into()))?;
        user(
            authority,
            &snapshot,
            record
                .ingress
                .as_ref()
                .ok_or_else(|| Error::ScopeDenied("canonical Human provenance missing".into()))?,
        )?;
        Ok(record.clone())
    }

    /// Complete contracts/questions from one immutable snapshot. If the fixed
    /// candidate page is incomplete, coverage says so; absence is not proof.
    pub fn resolution_input(
        &self,
        authority: &Authority,
        message_id: &str,
        limit: usize,
        budget: usize,
    ) -> Result<ResolutionInput> {
        if !(1..=100).contains(&limit) || !(128..=65536).contains(&budget) {
            return Err(Error::ContextBudgetExceeded);
        }
        let saved = self.pin_message_basis(authority, message_id)?;
        let human = saved.ingress.expect("validated Human record");
        let basis = saved.basis.expect("validated fixed basis");
        let snapshot = self.fixed_snapshot(&basis.commit)?;
        let works: Vec<_> = snapshot
            .tickets
            .values()
            .filter(|t| t.kind != TicketKind::Step && !t.archived)
            .collect();
        let candidates = works
            .iter()
            .take(limit)
            .map(|t| ResolutionCandidate {
                target: TicketReference::from_ticket(t),
                kind: t.kind,
                state: t.state,
                contract: t.contract.clone(),
                current_submission: t.current_submission.clone(),
                requests: snapshot
                    .requests
                    .values()
                    .filter(|q| q.work_id == t.id && q.status == RequestStatus::Open)
                    .cloned()
                    .collect(),
            })
            .collect();
        let input = ResolutionInput {
            message_id: saved.message_id,
            ingress_seq: saved.ingress_seq,
            human,
            basis,
            candidates,
            truncated: works.len() > limit,
            omitted_count: works.len().saturating_sub(limit),
            coverage: if works.len() > limit {
                "partial"
            } else {
                "complete"
            }
            .into(),
        };
        if canonical_bytes(&input)?.len() > budget {
            return Err(Error::ContextBudgetExceeded);
        }
        Ok(input)
    }

    /// Freeze the entire zero-to-many proposal before any group can mutate
    /// Tickets or publish a dispatch intent. Changed retries conflict.
    pub fn save_message_proposal(
        &self,
        authority: &Authority,
        message_id: &str,
        proposal: &MessageProposal,
    ) -> Result<MessageResolution> {
        validate_shape(proposal)?;
        self.pin_message_basis(authority, message_id)?;
        let mut store = self.inner.lock().expect("store mutex");
        let current = &store
            .published
            .as_ref()
            .ok_or_else(|| Error::AuthorityUnavailable("no verified snapshot".into()))?
            .1;
        let mut saved = current
            .resolutions
            .get(message_id)
            .cloned()
            .ok_or_else(|| {
                Error::InvalidTransition("register canonical Human input first".into())
            })?;
        let human = saved
            .ingress
            .as_ref()
            .ok_or_else(|| Error::ScopeDenied("canonical Human provenance missing".into()))?;
        user(authority, current, human)?;
        let hash = content_hash(&canonical_bytes(proposal)?);
        if let Some(old) = &saved.proposal_hash {
            return if old == &hash {
                Ok(saved)
            } else {
                Err(Error::IdempotencyConflict)
            };
        }
        let basis = saved.basis.as_ref().expect("validated fixed basis");
        let fixed = store.load_snapshot(&basis.commit)?;
        saved.groups = proposal
            .groups
            .iter()
            .map(|group| {
                let result = validate_group(&fixed, human, group);
                ResolutionGroup {
                    operation_id: group_operation_id(message_id, &group.group_id),
                    status: result
                        .as_ref()
                        .err()
                        .map(group_status)
                        .unwrap_or(ResolutionStatus::Proposed),
                    reason: result.err().map(|e| e.to_string()),
                    item_ids: group.item_ids.clone(),
                    receipt: None,
                }
            })
            .collect();
        saved.proposal = Some(proposal.clone());
        saved.proposal_hash = Some(hash);
        let mut next = current.clone();
        next.seq += 1;
        saved.updated_seq = Some(next.seq);
        next.resolutions.insert(message_id.into(), saved.clone());
        validate_snapshot(&next)?;
        store.publish(next)?;
        Ok(saved)
    }

    /// A transaction advances exactly one pending group. All its typed
    /// mutations, temporary IDs, receipt and item outcomes publish together.
    pub fn settle_message_group(
        &self,
        authority: &Authority,
        message_id: &str,
        group_id: &str,
    ) -> Result<MessageResolution> {
        let mut store = self.inner.lock().expect("store mutex");
        let current = &store
            .published
            .as_ref()
            .ok_or_else(|| Error::AuthorityUnavailable("no verified snapshot".into()))?
            .1;
        let mut saved = current
            .resolutions
            .get(message_id)
            .cloned()
            .ok_or_else(|| Error::InvalidTransition("message missing".into()))?;
        let human = saved
            .ingress
            .as_ref()
            .ok_or_else(|| Error::ScopeDenied("canonical Human provenance missing".into()))?;
        user(authority, current, human)?;
        let proposal = saved
            .proposal
            .as_ref()
            .ok_or_else(|| Error::InvalidTransition("persist proposal before effects".into()))?;
        let index = proposal
            .groups
            .iter()
            .position(|g| g.group_id == group_id)
            .ok_or_else(|| Error::InvalidTransition("group missing".into()))?;
        if saved.groups[index].status != ResolutionStatus::Proposed {
            return Ok(saved);
        }
        // Later ingress is durable, but cannot overtake an unresolved prefix.
        if current.resolutions.values().any(|r| {
            r.ingress_seq < saved.ingress_seq
                && r.ingress.is_some()
                && (r.proposal.is_none()
                    || r.groups
                        .iter()
                        .any(|g| g.status == ResolutionStatus::Proposed))
        }) {
            return Err(Error::ResourceBlocked(
                "earlier Human ingress is unresolved".into(),
            ));
        }
        let group = &proposal.groups[index];
        let ops: Vec<_> = group.operations.iter().map(operation).collect();
        let command = Command {
            operation_id: saved.groups[index].operation_id.clone(),
            binding: current.binding.clone(),
            expected_seq: current.seq,
            expected_epoch: current.authority_epoch,
            operations: ops,
            source: None,
        };
        let mut next = current.clone();
        next.seq += 1;
        let attempted = if current.receipts.contains_key(&command.operation_id) {
            // A caller-selected legacy operation ID must not be overwritten by
            // a derived message/group ID. Committed group replay returned above.
            Err(Error::IdempotencyConflict)
        } else if saved
            .basis
            .as_ref()
            .expect("validated basis")
            .authority_epoch
            != current.authority_epoch
        {
            Err(Error::RevisionConflict)
        } else {
            validate_group(current, human, group).and_then(|()| {
                apply_operations(&mut next, authority, &command.operations, store.root())
            })
        };
        match attempted.and_then(|ids| {
            validate_snapshot(&next)?;
            Ok(ids)
        }) {
            Ok(ids) => {
                let receipt = OperationReceipt {
                    operation_id: command.operation_id.clone(),
                    principal: authority.identity(),
                    request_hash: content_hash(&canonical_bytes(&command)?),
                    canonical_request: String::from_utf8(canonical_bytes(&command)?)
                        .expect("canonical UTF-8"),
                    committed_seq: next.seq,
                    ids,
                };
                next.receipts.insert(command.operation_id, receipt.clone());
                saved.groups[index].status = ResolutionStatus::Committed;
                saved.groups[index].receipt = Some(receipt);
            }
            Err(error) => {
                next = current.clone();
                next.seq += 1;
                saved.groups[index].status = group_status(&error);
                saved.groups[index].reason = Some(error.to_string());
            }
        }
        saved.updated_seq = Some(next.seq);
        next.resolutions.insert(message_id.into(), saved.clone());
        validate_snapshot(&next)?;
        store.publish(next)?;
        Ok(saved)
    }

    pub fn settle_message(
        &self,
        authority: &Authority,
        message_id: &str,
    ) -> Result<MessageResolution> {
        let saved = self.message_resolution(authority, message_id)?;
        let proposal = saved
            .proposal
            .ok_or_else(|| Error::InvalidTransition("persist proposal before effects".into()))?;
        for group in proposal.groups {
            self.settle_message_group(authority, message_id, &group.group_id)?;
        }
        self.message_resolution(authority, message_id)
    }
}
