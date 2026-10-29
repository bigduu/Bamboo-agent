//! Canonical producer state. Inbox admission and ACK remain in FileSessionInbox.
use super::*;
use bamboo_domain::{
    observer_filtered_fingerprint, ObserverChangeKind, ObserverStageOutcome, ObserverSubscription,
    ObserverSubscriptionFailure, ObserverSubscriptionOwner, SessionCallerReference,
    SessionInboxReceipt, SessionMessageBody, SessionMessageContent, SessionMessageEnvelope,
    SessionMessageKind, SessionMessageSource, SessionRuntimeInstruction, SupervisorObserverContext,
    SupervisorObserverRegistration, SUPERVISOR_SUBSCRIPTION_SCHEMA_VERSION,
};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

fn digest_hex(value: impl std::fmt::LowerHex) -> String {
    format!("{value:x}")
}

struct LockedObserver {
    guards: Arc<SupervisorManagementGuards>,
    parent: Session,
    child: Session,
    registration: SupervisorObserverRegistration,
    state: SupervisorManagementState,
}

/// Administrative failure state is bound to the original parent registration.
/// It never supplies Child/target read authority or an Inbox admission token.
struct LockedObserverFailure {
    guards: Arc<SupervisorManagementGuards>,
    parent: Session,
    state: SupervisorManagementState,
}

/// Constructed only from current canonical registration and pending state. The
/// same full guard is retained by every blocking Maildir job and cursor write.
pub(crate) struct ObserverNoticeAuthority {
    locked: LockedObserver,
    pub(crate) envelope: SessionMessageEnvelope,
    pub(crate) inbox_dir: PathBuf,
    pub(crate) supervisor_receipts: Vec<SessionMessageEnvelope>,
    pub(crate) supervisor_inbox_dir: PathBuf,
    fault: bool,
}

/// Retained canonical Observer/target guards for reading Root Main and its
/// original Inbox ACKs. This is not an execution or sending grant.
pub(crate) struct ObserverRootOutcomeAuthority {
    pub(crate) _guards: Arc<SupervisorManagementGuards>,
    pub(crate) parent: Session,
    pub(crate) target: Session,
    pub(crate) main: Session,
    pub(crate) cursor: Option<bamboo_domain::ObserverRootOutcomeCursor>,
    pub(crate) pending: Option<bamboo_domain::ObserverViewChange>,
    pub(crate) inbox_dir: PathBuf,
}

fn owner<'a>(reference: &SupervisorReference, child: &'a Session) -> ObserverSubscriptionOwner<'a> {
    ObserverSubscriptionOwner {
        supervisor_incarnation: reference.incarnation_id,
        supervisor_created_at: child
            .parent_created_at
            .expect("strict Observer parent birth"),
        observer_session_id: &child.id,
        observer_created_at: child.created_at,
    }
}

impl SessionStoreV2 {
    pub(crate) async fn observer_root_outcome_authority(
        &self,
        caller: &SessionCallerReference,
        reference: &SupervisorReference,
        target_id: &str,
    ) -> io::Result<Arc<ObserverRootOutcomeAuthority>> {
        let locked = self
            .subscription_authority(reference, &caller.session_id, true)
            .await?;
        if !caller.matches_session(&locked.child) {
            return Err(denied("Observer original caller changed"));
        }
        let subscription = locked
            .state
            .subscriptions
            .get(&caller.session_id)
            .ok_or_else(|| denied("Observer has no subscription"))?;
        if subscription.filter != BTreeSet::from([ObserverChangeKind::Status])
            || !locked.registration.target_links.contains_key(target_id)
        {
            return Err(denied(
                "Observer outcome requires its assigned status subscription",
            ));
        }
        let cursor = subscription
            .window
            .root_outcome_cursors
            .get(target_id)
            .cloned();
        let pending = subscription.window.pending.clone();
        let target = self
            .load_root_authority_unchecked(target_id)
            .await?
            .ok_or_else(|| denied("Observer target is missing"))?;
        let main = self
            .load_execution_admission_unchecked(target_id)
            .await?
            .ok_or_else(|| denied("Observer Root Main is missing"))?;
        if main.id != target.id
            || main.created_at != target.created_at
            || main.kind != SessionKind::Root
            || main.authority_identity != target.authority_identity
            || main.project_id_meta() != target.project_id_meta()
            || main.conversation_reset != target.conversation_reset
        {
            return Err(denied("Observer Root Main lifetime changed"));
        }
        Ok(Arc::new(ObserverRootOutcomeAuthority {
            _guards: locked.guards,
            parent: locked.parent,
            target,
            main,
            cursor,
            pending,
            inbox_dir: self.sessions_dir.join(target_id).join("inbox"),
        }))
    }

    async fn subscription_authority(
        &self,
        reference: &SupervisorReference,
        child_id: &str,
        require_targets: bool,
    ) -> io::Result<LockedObserver> {
        reference_is_valid(reference)?;
        target_id_is_valid(child_id)?;
        // Never resurrect durable fallback after an installed/revoked Host
        // namespace. Acquire this publication fence before lifecycle/Task locks.
        let absence = self
            .host_scope_registry
            .retain_never_installed(child_id)
            .await?;
        let preview = Storage::load_root_authority(self, &reference.session_id)
            .await?
            .ok_or_else(|| denied("Observer Supervisor is missing"))?;
        let registration = preview
            .supervisor_management
            .as_ref()
            .and_then(|state| state.observers.get(child_id))
            .ok_or_else(|| denied("Observer is not registered"))?;
        let targets: Vec<_> = if require_targets {
            registration.target_links.keys().cloned().collect()
        } else {
            Vec::new()
        };
        let guards = self
            .observer_guards(child_id, targets.clone(), Some(absence), false)
            .await?;
        let parent = self.management_supervisor_locked(reference).await?;
        self.check_management_session_compatibility(&parent).await?;
        let state = parent
            .supervisor_management
            .clone()
            .ok_or_else(|| denied("Observer state is missing"))?;
        let child = self
            .strict_observer_child(&parent, child_id)
            .await?
            .ok_or_else(|| denied("Observer Child is missing"))?;
        let registration = state
            .observers
            .get(child_id)
            .cloned()
            .ok_or_else(|| denied("Observer registration disappeared"))?;
        if !registration.enabled
            || !observer_native_child(&child)
            || !state.classifies_observer(&parent, &child)
        {
            return Err(denied("Observer registration was revoked or replaced"));
        }
        if require_targets {
            if registration
                .target_links
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                != targets
            {
                return Err(denied("Observer target set changed before locking"));
            }
            for (id, link) in &registration.target_links {
                let target = self
                    .load_root_authority_unchecked(id)
                    .await?
                    .ok_or_else(|| denied("Observer target is missing"))?;
                if !observer_link_authorizes_target(&state, &registration, id, link, &target) {
                    return Err(denied("Observer target scope changed"));
                }
            }
        }
        Ok(LockedObserver {
            guards,
            parent,
            child,
            registration,
            state,
        })
    }

    /// Register producer configuration after the original Creator/register-only
    /// operation. An exact retry never clears a pending event or exhausted retry.
    pub async fn subscribe_supervisor_observer(
        &self,
        reference: &SupervisorReference,
        expected_revision: u64,
        child_id: &str,
        filter: BTreeSet<ObserverChangeKind>,
        min_interval_ms: u32,
    ) -> io::Result<ObserverSubscription> {
        let mut locked = self
            .subscription_authority(reference, child_id, true)
            .await?;
        let mut candidate =
            ObserverSubscription::new(locked.registration.revision, filter, min_interval_ms)
                .map_err(invalid)?;
        if let Some(old) = locked.state.subscriptions.get(child_id) {
            if old.filter != candidate.filter
                || old.min_interval_ms != candidate.min_interval_ms
                || old.window.scope_epoch != candidate.window.scope_epoch
            {
                return Err(denied(
                    "Observer subscription retry conflicts with original configuration",
                ));
            }
            return Ok(old.clone());
        }
        if locked.state.revision != expected_revision {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "Supervisor changed before subscription",
            ));
        }
        if candidate.filter == BTreeSet::from([ObserverChangeKind::Status]) {
            for (target_id, link) in &locked.registration.target_links {
                candidate.window.root_outcome_cursors.insert(
                    target_id.clone(),
                    bamboo_domain::ObserverRootOutcomeCursor {
                        root_created_at: link.target_created_at,
                        sequence: 0,
                    },
                );
            }
            candidate.validate().map_err(invalid)?;
            locked.state.schema_version = locked
                .state
                .schema_version
                .max(bamboo_domain::SUPERVISOR_OBSERVER_ROOT_OUTCOME_SCHEMA_VERSION);
        }
        locked.state.schema_version = locked
            .state
            .schema_version
            .max(SUPERVISOR_SUBSCRIPTION_SCHEMA_VERSION);
        locked.state.revision = next_revision(locked.state.revision)?;
        locked
            .state
            .subscriptions
            .insert(child_id.into(), candidate.clone());
        self.publish_management_state(&mut locked.parent, &locked.state, &locked.guards)
            .await?;
        Ok(candidate)
    }

    /// Upgrade an existing status-only subscription after its legacy pending
    /// item has finished. Zero means no verified Root outcome receipt; an old
    /// snapshot fingerprint is never promoted into a source cursor.
    pub async fn initialize_observer_root_outcome_lane(
        &self,
        reference: &SupervisorReference,
        child_id: &str,
    ) -> io::Result<()> {
        let mut locked = self
            .subscription_authority(reference, child_id, true)
            .await?;
        let subscription = locked
            .state
            .subscriptions
            .get_mut(child_id)
            .ok_or_else(|| denied("Observer has no subscription"))?;
        if subscription.filter != BTreeSet::from([ObserverChangeKind::Status])
            || subscription.window.pending.is_some()
        {
            return Ok(());
        }
        let mut changed = false;
        for (target, link) in &locked.registration.target_links {
            if !subscription
                .window
                .root_outcome_cursors
                .contains_key(target)
            {
                subscription.window.root_outcome_cursors.insert(
                    target.clone(),
                    bamboo_domain::ObserverRootOutcomeCursor {
                        root_created_at: link.target_created_at,
                        sequence: 0,
                    },
                );
                changed = true;
            }
        }
        if changed {
            subscription.validate().map_err(invalid)?;
            locked.state.schema_version = locked
                .state
                .schema_version
                .max(bamboo_domain::SUPERVISOR_OBSERVER_ROOT_OUTCOME_SCHEMA_VERSION);
            locked.state.revision = next_revision(locked.state.revision)?;
            self.publish_management_state(&mut locked.parent, &locked.state, &locked.guards)
                .await?;
        }
        Ok(())
    }

    /// A trusted Host supplies only a selector. Verify the actually published,
    /// bounded bundle and its current canonical provenance before recording it.
    pub async fn stage_supervisor_observation(
        &self,
        reference: &SupervisorReference,
        child_id: &str,
        target_id: &str,
        revision: &str,
        observed_at: DateTime<Utc>,
    ) -> io::Result<ObserverStageOutcome> {
        self.stage_supervisor_observation_with_root_outcome(
            reference,
            child_id,
            target_id,
            revision,
            None,
            observed_at,
        )
        .await
    }

    /// The optional position is a selector only. The locked Root Main and
    /// current receipt-committed cursor determine whether it is the next fact.
    pub async fn stage_supervisor_observation_with_root_outcome(
        &self,
        reference: &SupervisorReference,
        child_id: &str,
        target_id: &str,
        revision: &str,
        root_outcome: Option<&bamboo_domain::ObserverRootOutcomeCursor>,
        observed_at: DateTime<Utc>,
    ) -> io::Result<ObserverStageOutcome> {
        let mut locked = self
            .subscription_authority(reference, child_id, true)
            .await?;
        let subscription = locked
            .state
            .subscriptions
            .get(child_id)
            .ok_or_else(|| denied("Observer has no subscription"))?;
        if subscription.window.scope_epoch != locked.registration.revision {
            return Err(denied("Observer subscription epoch changed"));
        }
        if let Some(pending) = &subscription.window.pending {
            return Ok(ObserverStageOutcome::ExistingPending(pending.clone()));
        }
        if !subscription.ready(observed_at) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "Observer retry or interval is not due",
            ));
        }
        if !locked.registration.target_links.contains_key(target_id) {
            return Err(denied("Observer target is not assigned"));
        }
        let target = self
            .load_root_authority_unchecked(target_id)
            .await?
            .ok_or_else(|| denied("Observer target is missing"))?;
        let files = crate::context_view::read_session_context_snapshot_retaining(
            &self.bamboo_home_dir,
            child_id,
            revision,
            Arc::clone(&locked.guards),
        )
        .await?;
        let main = if root_outcome.is_some() {
            Some(
                self.load_execution_admission_unchecked(target_id)
                    .await?
                    .ok_or_else(|| denied("Observer Root Main is missing"))?,
            )
        } else {
            None
        };
        let fingerprint = verify_view(
            &files,
            &locked,
            &target,
            revision,
            root_outcome,
            main.as_ref(),
        )?;
        let filter = subscription.filter.clone();
        let outcome = locked
            .state
            .subscriptions
            .get_mut(child_id)
            .expect("subscription checked")
            .window
            .stage_with_root_outcome(
                &owner(reference, &locked.child),
                target_id,
                revision,
                &fingerprint,
                filter,
                observed_at,
                root_outcome.cloned(),
            )
            .map_err(invalid)?;
        if matches!(outcome, ObserverStageOutcome::Staged(_)) {
            if root_outcome.is_some() {
                locked.state.schema_version = locked
                    .state
                    .schema_version
                    .max(bamboo_domain::SUPERVISOR_OBSERVER_ROOT_OUTCOME_SCHEMA_VERSION);
            }
            locked.state.revision = next_revision(locked.state.revision)?;
            self.publish_management_state(&mut locked.parent, &locked.state, &locked.guards)
                .await?;
        }
        Ok(outcome)
    }

    /// Derived shards rotate with the bounded canonical history. A gap names
    /// fixed targets requiring fresh snapshots; no derived bytes become ACK truth.
    pub async fn read_supervisor_observer_changes(
        &self,
        reference: &SupervisorReference,
        child_id: &str,
        scope_epoch: u64,
        after: u64,
    ) -> io::Result<bamboo_domain::ObserverChangeRead> {
        let locked = self
            .subscription_authority(reference, child_id, true)
            .await?;
        let subscription = locked
            .state
            .subscriptions
            .get(child_id)
            .ok_or_else(|| denied("Observer has no subscription"))?;
        let page = subscription
            .window
            .changes_after(scope_epoch, after)
            .map_err(invalid)?;
        let mut jsonl_shards = Vec::new();
        for chunk in page.changes.chunks(4) {
            let mut shard = String::new();
            for change in chunk {
                shard.push_str(
                    &serde_json::to_string(change).map_err(|error| invalid(error.to_string()))?,
                );
                shard.push('\n');
            }
            jsonl_shards.push(shard);
        }
        let snapshot_targets = if page.gap {
            locked.registration.target_links.keys().cloned().collect()
        } else {
            Vec::new()
        };
        let (shard_revision, shard_directory) = self
            .publish_observer_changes(&locked, &subscription.window)
            .await?;
        Ok(bamboo_domain::ObserverChangeRead {
            page,
            jsonl_shards,
            snapshot_targets,
            shard_revision,
            shard_directory,
        })
    }

    // Reconciliation must remain bounded even when the original Observer has
    // disappeared or lost its sidecar. Only the parent's immutable registration
    // is needed to record a diagnostic; fresh Child authority is still required
    // by every observation and physical Inbox admission path.
    async fn observer_failure_state(
        &self,
        reference: &SupervisorReference,
        child_id: &str,
    ) -> io::Result<LockedObserverFailure> {
        reference_is_valid(reference)?;
        target_id_is_valid(child_id)?;
        let lifecycle = self.lock_session_lifecycle_shared().await?;
        let task = self.lock_runtime_task_sidecar_shared().await?;
        let sessions = self.management_session_locks(None).await?;
        let guards = Arc::new(SupervisorManagementGuards {
            sessions,
            _task: task,
            _lifecycle: super::LifecycleGuard::Shared { _guard: lifecycle },
            _host_namespace: None,
        });
        let parent = self.management_supervisor_locked(reference).await?;
        self.check_management_session_compatibility(&parent).await?;
        let state = parent
            .supervisor_management
            .clone()
            .ok_or_else(|| denied("Observer state missing"))?;
        let registration = state
            .observers
            .get(child_id)
            .ok_or_else(|| denied("Observer registration missing"))?;
        let subscription = state
            .subscriptions
            .get(child_id)
            .ok_or_else(|| denied("Observer has no subscription"))?;
        if !registration.enabled
            || registration.parent_created_at != parent.created_at
            || subscription.window.scope_epoch != registration.revision
        {
            return Err(denied(
                "Observer failure registration was revoked or changed",
            ));
        }
        Ok(LockedObserverFailure {
            guards,
            parent,
            state,
        })
    }

    pub async fn record_observer_subscription_failure(
        &self,
        reference: &SupervisorReference,
        child_id: &str,
        now: DateTime<Utc>,
        failure: ObserverSubscriptionFailure,
    ) -> io::Result<()> {
        let mut locked = self.observer_failure_state(reference, child_id).await?;
        let registration = locked
            .state
            .observers
            .get(child_id)
            .expect("verified registration");
        let failure_owner = ObserverSubscriptionOwner {
            supervisor_incarnation: reference.incarnation_id,
            supervisor_created_at: registration.parent_created_at,
            observer_session_id: child_id,
            observer_created_at: registration.child_created_at,
        };
        let subscription = locked
            .state
            .subscriptions
            .get_mut(child_id)
            .ok_or_else(|| denied("Observer has no subscription"))?;
        subscription.record_failure(now, failure);
        if subscription.failures == 8 {
            subscription
                .capture_terminal_fault(&failure_owner, now)
                .map_err(invalid)?;
            locked.state.schema_version = locked
                .state
                .schema_version
                .max(bamboo_domain::SUPERVISOR_OBSERVER_FAULT_SCHEMA_VERSION);
        }
        locked.state.revision = next_revision(locked.state.revision)?;
        self.publish_management_state(&mut locked.parent, &locked.state, &locked.guards)
            .await
    }

    pub(crate) async fn begin_observer_notice(
        &self,
        reference: &SupervisorReference,
        child_id: &str,
        now: DateTime<Utc>,
    ) -> io::Result<Arc<ObserverNoticeAuthority>> {
        let locked = self
            .subscription_authority(reference, child_id, true)
            .await?;
        let subscription = locked
            .state
            .subscriptions
            .get(child_id)
            .ok_or_else(|| denied("Observer has no subscription"))?;
        if !subscription.ready(now)
            || subscription.window.scope_epoch != locked.registration.revision
        {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "Observer delivery is not due",
            ));
        }
        let change = subscription
            .window
            .pending
            .as_ref()
            .ok_or_else(|| invalid("Observer has no published pending view"))?;
        if !locked
            .registration
            .target_links
            .contains_key(&change.target_session_id)
        {
            return Err(denied("Observer pending target was revoked"));
        }
        let target = self
            .load_root_authority_unchecked(&change.target_session_id)
            .await?
            .ok_or_else(|| denied("Observer target missing"))?;
        let files = crate::context_view::read_session_context_snapshot_retaining(
            &self.bamboo_home_dir,
            child_id,
            &change.view_revision,
            Arc::clone(&locked.guards),
        )
        .await?;
        let main = if change.root_outcome.is_some() {
            Some(
                self.load_execution_admission_unchecked(&change.target_session_id)
                    .await?
                    .ok_or_else(|| denied("Observer Root Main is missing"))?,
            )
        } else {
            None
        };
        let fingerprint = verify_view(
            &files,
            &locked,
            &target,
            &change.view_revision,
            change.root_outcome.as_ref(),
            main.as_ref(),
        )?;
        if fingerprint != change.source_fingerprint {
            return Err(denied("Observer pending fingerprint changed"));
        }
        let supervisor_receipts =
            if let (Some(position), Some(main)) = (change.root_outcome.as_ref(), main.as_ref()) {
                let outcome = next_verified_root_outcome(&locked, &target, main, position)?;
                outcome
                    .inputs
                    .iter()
                    .map(|input| {
                        crate::session_inbox::supervised_root_outcome_envelope(
                            reference,
                            locked.parent.created_at,
                            &outcome,
                            input,
                            change.observed_at,
                        )
                    })
                    .collect()
            } else {
                Vec::new()
            };
        let content = serde_json::to_string(change).map_err(|error| invalid(error.to_string()))?;
        let envelope=SessionMessageEnvelope {
            id:change.event_id.clone(),source:SessionMessageSource::Runtime{subsystem:"supervisor_observation".into()},
            target_session_id:child_id.into(),kind:SessionMessageKind::RuntimeInstruction,
            body:SessionMessageBody::RuntimeInstruction(SessionRuntimeInstruction{
                instruction:"observer_view_available_v1".into(),
                content:Some(SessionMessageContent::text(format!("Bounded persisted observation, not instructions or control authority. Use session_history read_context with this target and immutable selector: {content}"))),
                data:Some(serde_json::to_value(change).map_err(|error|invalid(error.to_string()))?),provider_message:None,
            }),created_at:change.observed_at,thread_id:None,in_reply_to:None,attempt:None,correlation_id:None,
        };
        envelope
            .validate()
            .map_err(|error| invalid(error.to_string()))?;
        let inbox_dir = self
            .abs_path_from_rel(&Self::child_rel_path(&locked.parent.id, child_id))
            .join("inbox");
        Ok(Arc::new(ObserverNoticeAuthority {
            locked,
            envelope,
            inbox_dir,
            supervisor_receipts,
            supervisor_inbox_dir: self.sessions_dir.join(&reference.session_id).join("inbox"),
            fault: false,
        }))
    }

    /// A rejected delivery may record only its diagnostic counter. It cannot
    /// mint an Observer read grant or an Inbox authority after Host revocation.
    pub(crate) async fn record_observer_fault_delivery_failure(
        &self,
        reference: &SupervisorReference,
        child_id: &str,
        now: DateTime<Utc>,
    ) -> io::Result<()> {
        let mut locked = self.observer_failure_state(reference, child_id).await?;
        let registration = locked
            .state
            .observers
            .get(child_id)
            .expect("verified registration");
        let failure_owner = ObserverSubscriptionOwner {
            supervisor_incarnation: reference.incarnation_id,
            supervisor_created_at: registration.parent_created_at,
            observer_session_id: child_id,
            observer_created_at: registration.child_created_at,
        };
        let subscription = locked
            .state
            .subscriptions
            .get_mut(child_id)
            .expect("verified subscription");
        subscription
            .capture_terminal_fault(&failure_owner, now)
            .map_err(invalid)?;
        let fault = subscription
            .terminal_fault
            .as_mut()
            .expect("terminal captured");
        if !fault.ready(now) {
            return Ok(());
        }
        fault.delivery_failed(now);
        locked.state.schema_version = locked
            .state
            .schema_version
            .max(bamboo_domain::SUPERVISOR_OBSERVER_FAULT_SCHEMA_VERSION);
        locked.state.revision = next_revision(locked.state.revision)?;
        self.publish_management_state(&mut locked.parent, &locked.state, &locked.guards)
            .await
    }

    /// Administrative diagnostics never read a target source. They still
    /// require the original native Child, live canonical links/epoch and Host
    /// namespace absence; revoked registrations cannot send to their old parent.
    pub(crate) async fn begin_observer_fault(
        &self,
        reference: &SupervisorReference,
        child_id: &str,
        now: DateTime<Utc>,
    ) -> io::Result<Arc<ObserverNoticeAuthority>> {
        let mut locked = self
            .subscription_authority(reference, child_id, false)
            .await?;
        let subscription = locked
            .state
            .subscriptions
            .get_mut(child_id)
            .ok_or_else(|| denied("Observer has no subscription"))?;
        if subscription.window.scope_epoch != locked.registration.revision {
            return Err(denied("Observer diagnostic epoch changed"));
        }
        let created = subscription
            .capture_terminal_fault(&owner(reference, &locked.child), now)
            .map_err(invalid)?;
        let fault = subscription
            .terminal_fault
            .as_ref()
            .expect("terminal captured");
        if !fault.ready(now) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "Observer diagnostic is reported or delivery is blocked",
            ));
        }
        let envelope=SessionMessageEnvelope{
            id:fault.event_id.clone(),source:SessionMessageSource::Runtime{subsystem:"supervisor_observation".into()},
            target_session_id:reference.session_id.clone(),kind:SessionMessageKind::RuntimeInstruction,
            body:SessionMessageBody::RuntimeInstruction(SessionRuntimeInstruction{
                instruction:"observer_subscription_fault_v1".into(),
                content:Some(SessionMessageContent::text(format!("Observer subscription exhausted its bounded retries. This diagnostic grants no control and announces no readable source file: {}",serde_json::to_string(&fault.payload).map_err(|error|invalid(error.to_string()))?))),
                data:Some(serde_json::to_value(&fault.payload).map_err(|error|invalid(error.to_string()))?),provider_message:None,
            }),created_at:fault.payload.exhausted_at,thread_id:None,in_reply_to:None,attempt:None,correlation_id:None,
        };
        if created {
            locked.state.schema_version = locked
                .state
                .schema_version
                .max(bamboo_domain::SUPERVISOR_OBSERVER_FAULT_SCHEMA_VERSION);
            locked.state.revision = next_revision(locked.state.revision)?;
            self.publish_management_state(&mut locked.parent, &locked.state, &locked.guards)
                .await?;
        }
        envelope
            .validate()
            .map_err(|error| invalid(error.to_string()))?;
        let inbox_dir = self.sessions_dir.join(&reference.session_id).join("inbox");
        Ok(Arc::new(ObserverNoticeAuthority {
            locked,
            envelope,
            inbox_dir,
            supervisor_receipts: Vec::new(),
            supervisor_inbox_dir: PathBuf::new(),
            fault: true,
        }))
    }

    /// Private integrated completion; a receipt-shaped public DTO has no port
    /// to mark a diagnostic reported. Delivery failure never replaces payload.
    pub(crate) async fn finish_observer_fault(
        &self,
        authority: &Arc<ObserverNoticeAuthority>,
        receipt: Option<&SessionInboxReceipt>,
        now: DateTime<Utc>,
    ) -> io::Result<()> {
        if !authority.fault {
            return Err(denied("view is not a diagnostic delivery"));
        }
        let locked = &authority.locked;
        let mut state = locked.state.clone();
        let fault = state
            .subscriptions
            .get_mut(&locked.child.id)
            .and_then(|subscription| subscription.terminal_fault.as_mut())
            .ok_or_else(|| denied("terminal diagnostic disappeared"))?;
        if let Some(receipt) = receipt {
            if receipt.id != authority.envelope.id
                || receipt.generation == 0
                || authority.envelope.target_session_id != locked.parent.id
            {
                return Err(denied(
                    "diagnostic receipt has a different event or mailbox",
                ));
            }
            fault.reported = true;
            fault.retry_at = None;
        } else {
            fault.delivery_failed(now);
        }
        state.revision = next_revision(state.revision)?;
        let mut parent = locked.parent.clone();
        self.publish_management_state(&mut parent, &state, &locked.guards)
            .await
    }

    async fn publish_observer_changes(
        &self,
        locked: &LockedObserver,
        window: &bamboo_domain::ObserverSubscriptionWindow,
    ) -> io::Result<(String, PathBuf)> {
        let mut shards = Vec::new();
        for chunk in window.history.chunks(4) {
            let mut shard = String::new();
            for change in chunk {
                shard.push_str(
                    &serde_json::to_string(change).map_err(|error| invalid(error.to_string()))?,
                );
                shard.push('\n');
            }
            shards.push(shard);
        }
        let files:Vec<_>=shards.iter().enumerate().map(|(index,bytes)|json!({"file":format!("{index}.jsonl"),"bytes":bytes.len(),"sha256":digest_hex(Sha256::digest(bytes.as_bytes()))})).collect();
        let mut manifest = json!({"schema":"observer-change-shards.v1","observer_session_id":locked.child.id,
            "observer_created_at":locked.child.created_at,"supervisor_created_at":locked.parent.created_at,
            "supervisor_incarnation":locked.state.incarnation_id,"scope_epoch":window.scope_epoch,
            "oldest_sequence":window.history.first().map(|change|change.sequence),"enqueued_sequence":window.enqueued_sequence,
            "files":files,"truth":"derived enqueue references; consumer ACK remains canonical Inbox"});
        let revision = digest_hex(Sha256::digest(
            bamboo_domain::session::inbox::canonical_json_bytes(&manifest),
        ));
        manifest["revision"] = json!(revision);
        let manifest =
            serde_json::to_vec_pretty(&manifest).map_err(|error| invalid(error.to_string()))?;
        let home = self.bamboo_home_dir.clone();
        let child_id = locked.child.id.clone();
        let selector = revision.clone();
        let guards = Arc::clone(&locked.guards);
        let directory = tokio::task::spawn_blocking(move || {
            let _guards = guards;
            crate::context_view::publish_observer_change_snapshot(
                &home, &child_id, &selector, &manifest, &shards,
            )
        })
        .await
        .map_err(|error| other_io_error(format!("join change publication: {error}")))??;
        Ok((revision, directory))
    }

    /// Private to FileSessionInbox, after its actual Maildir transaction returns.
    /// No public storage API accepts a caller-supplied receipt as cursor truth.
    pub(crate) async fn finish_observer_notice(
        &self,
        authority: &Arc<ObserverNoticeAuthority>,
        receipt: &SessionInboxReceipt,
        now: DateTime<Utc>,
    ) -> io::Result<()> {
        if authority.fault {
            return Err(denied("fault is not a view delivery"));
        }
        let locked = &authority.locked;
        let reference = observer_parent_reference(&locked.parent)
            .ok_or_else(|| denied("Observer Supervisor changed"))?;
        let mut state = locked.state.clone();
        let subscription = state
            .subscriptions
            .get_mut(&locked.child.id)
            .ok_or_else(|| denied("Observer subscription missing"))?;
        if subscription
            .window
            .commit_enqueued(&owner(&reference, &locked.child), receipt)
            .map_err(invalid)?
        {
            subscription.delivered(now);
            self.publish_observer_changes(locked, &subscription.window)
                .await?;
            state.revision = next_revision(state.revision)?;
            let mut parent = locked.parent.clone();
            self.publish_management_state(&mut parent, &state, &locked.guards)
                .await?;
        }
        Ok(())
    }
}

// Reconstruct a bounded selector from canonical Main under the same physical
// guards as the Observer relationship. A snapshot is not authority to choose
// an arbitrary fact, even if its manifest and file hashes are self-consistent.
fn next_verified_root_outcome(
    locked: &LockedObserver,
    target: &Session,
    main: &Session,
    position: &bamboo_domain::ObserverRootOutcomeCursor,
) -> io::Result<bamboo_domain::ObserverSupervisedRootOutcome> {
    let subscription = locked
        .state
        .subscriptions
        .get(&locked.child.id)
        .ok_or_else(|| denied("Observer subscription missing"))?;
    if subscription.filter != BTreeSet::from([ObserverChangeKind::Status])
        || main.id != target.id
        || main.created_at != target.created_at
        || main.kind != SessionKind::Root
        || main.authority_identity != target.authority_identity
        || main.project_id_meta() != target.project_id_meta()
        || main.conversation_reset != target.conversation_reset
        || position.root_created_at != main.created_at
    {
        return Err(denied("Observer Root outcome source changed"));
    }
    let reference = observer_parent_reference(&locked.parent)
        .ok_or_else(|| denied("Observer Supervisor changed"))?;
    let outcomes = main
        .runtime_metadata
        .as_ref()
        .and_then(|metadata| metadata.supervised_root_outcomes.as_ref())
        .ok_or_else(|| denied("Root outcome source is unavailable"))?;
    outcomes.validate().map_err(invalid)?;
    if outcomes.root_session_id != main.id || outcomes.root_created_at != main.created_at {
        return Err(denied("Root outcome source lifetime changed"));
    }
    let belongs = |input: &bamboo_domain::SupervisedRootOutcomeInput| {
        input.supervisor_origin.as_ref().is_some_and(|origin| {
            origin.original_caller().is_some()
                && origin.requires_host_scope().is_some()
                && origin.supervisor() == &reference
                && origin.supervisor_created_at() == locked.parent.created_at
                && origin.matches_receipt(&main.id, main.created_at, &input.receipt_id)
        })
    };
    let cursor = subscription
        .window
        .root_outcome_cursors
        .get(&target.id)
        .ok_or_else(|| denied("Observer Root outcome baseline is unavailable"))?;
    if cursor.root_created_at != target.created_at {
        return Err(denied("Observer Root outcome cursor lifetime changed"));
    }
    if cursor.sequence != 0
        && !outcomes
            .facts
            .iter()
            .any(|fact| fact.sequence == cursor.sequence && fact.result.is_some())
    {
        return Err(denied("Observer Root outcome cursor is unavailable"));
    }
    let fact = outcomes
        .facts
        .iter()
        .find(|fact| {
            fact.sequence > cursor.sequence
                && fact.result.is_some()
                && fact.inputs.iter().any(&belongs)
        })
        .ok_or_else(|| denied("Observer has no next Root outcome"))?;
    if position.sequence != fact.sequence {
        return Err(denied(
            "Observer Root outcome is not the next canonical fact",
        ));
    }
    let inputs = fact
        .inputs
        .iter()
        .filter(|input| belongs(input))
        .map(|input| bamboo_domain::ObserverSupervisedRootInput {
            receipt_id: input.receipt_id.clone(),
            generation: input.generation,
            operation_id: input
                .supervisor_origin
                .as_ref()
                .expect("filtered origin")
                .operation_id()
                .clone(),
        })
        .collect();
    Ok(bamboo_domain::ObserverSupervisedRootOutcome {
        target_session_id: main.id.clone(),
        target_created_at: main.created_at,
        sequence: fact.sequence,
        run_id: fact.run_id.clone(),
        status: fact.result.clone().expect("filtered result"),
        inputs,
    })
}

fn verify_view(
    files: &crate::context_view::ContextSnapshotFiles,
    locked: &LockedObserver,
    target: &Session,
    revision: &str,
    root_outcome: Option<&bamboo_domain::ObserverRootOutcomeCursor>,
    main: Option<&Session>,
) -> io::Result<String> {
    let child = &locked.child;
    let registration = &locked.registration;
    let reference = observer_parent_reference(&locked.parent)
        .ok_or_else(|| denied("Observer Supervisor changed"))?;
    let filter = &locked
        .state
        .subscriptions
        .get(&child.id)
        .ok_or_else(|| denied("Observer subscription missing"))?
        .filter;
    let link = registration
        .target_links
        .get(&target.id)
        .ok_or_else(|| denied("Observer target is not assigned"))?;
    let mut manifest: Value = serde_json::from_slice(&files.manifest)
        .map_err(|_| invalid("invalid Observer view manifest"))?;
    if serde_json::to_vec_pretty(&manifest).map_err(|error| invalid(error.to_string()))?
        != files.manifest
    {
        return Err(invalid("noncanonical Observer view manifest"));
    }
    if manifest["schema_version"] != "session-context-view.scoped.v1"
        || manifest["revision"] != revision
    {
        return Err(denied("Observer view selector or schema changed"));
    }
    let caller = SessionCallerReference::from(child);
    let caller_sha256 = digest_hex(Sha256::digest(
        serde_json::to_vec(&caller).map_err(|error| invalid(error.to_string()))?,
    ));
    let expected = json!({"caller_session_id":child.id,"root_session_id":reference.session_id,"target_session_id":target.id,
        "project_id":link.target_project_id.to_string(),"authority":{"caller_sha256":caller_sha256,
        "route":{"route":"observer","registration":SupervisorObserverContext{
            supervisor:reference.clone(),parent_created_at:registration.parent_created_at,child_created_at:registration.child_created_at,
            operation_id:registration.operation_id,registration_revision:registration.revision,target_link:link.clone()}},
        "target_created_at":target.created_at,"target_metadata_version":target.metadata_version,"target_project_id":target.project_id_meta()}});
    if manifest["scope"] != expected
        || manifest["observation"] != "last_persisted"
        || manifest["files"]
            .as_object()
            .is_none_or(|files| files.len() != 2)
        || manifest["filenames"] != json!({"status":"status.md","brief":"brief.md"})
    {
        return Err(denied("Observer view current provenance changed"));
    }
    let verified_outcome = match (root_outcome, main, manifest.get("root_outcome")) {
        (None, None, None) => None,
        (Some(position), Some(main), Some(value)) => {
            let observed = next_verified_root_outcome(locked, target, main, position)?;
            let digest = digest_hex(Sha256::digest(bamboo_domain::canonical_json_bytes(
                &serde_json::to_value(&observed).map_err(|error| invalid(error.to_string()))?,
            )));
            if *value
                != json!({"root_created_at":position.root_created_at,
                "sequence":position.sequence,"sha256":digest})
            {
                return Err(denied("Observer Root outcome commitment differs"));
            }
            Some(observed)
        }
        _ => return Err(denied("Observer Root outcome selector differs")),
    };
    for (key, bytes) in [("status", &files.status), ("brief", &files.brief)] {
        let content =
            std::str::from_utf8(bytes).map_err(|_| invalid("Observer view is not UTF-8"))?;
        let expected = json!({"bytes":bytes.len(),"lines":content.lines().count(),"sha256":digest_hex(Sha256::digest(bytes))});
        let max_lines = if key == "status" { 40 } else { 120 };
        if content.lines().count() > max_lines
            || content.lines().any(|line| line.len() > 512)
            || manifest["files"][key] != expected
        {
            return Err(denied("Observer view file commitment changed"));
        }
    }
    let fingerprints: BTreeMap<ObserverChangeKind, String> =
        serde_json::from_value(manifest["fingerprints"].clone())
            .map_err(|_| invalid("Observer view fingerprints missing"))?;
    if let Some(outcome) = verified_outcome {
        if files.status != bamboo_domain::observer_root_outcome_status(&outcome).as_bytes()
            || files.brief != bamboo_domain::OBSERVER_ROOT_OUTCOME_BRIEF.as_bytes()
        {
            return Err(denied(
                "Observer Root outcome view differs from canonical source",
            ));
        }
        let expected = digest_hex(Sha256::digest(bamboo_domain::canonical_json_bytes(
            &json!({
                "status":outcome.status.observer_status(),"run_id":outcome.run_id
            }),
        )));
        if fingerprints.get(&ObserverChangeKind::Status) != Some(&expected) {
            return Err(denied("Observer Root outcome fingerprint differs"));
        }
    }
    manifest
        .as_object_mut()
        .ok_or_else(|| invalid("Observer manifest object missing"))?
        .remove("revision");
    if digest_hex(Sha256::digest(
        serde_json::to_vec(&manifest).map_err(|error| invalid(error.to_string()))?,
    )) != revision
    {
        return Err(denied("Observer immutable view commitment changed"));
    }
    observer_filtered_fingerprint(filter, &fingerprints).map_err(invalid)
}
