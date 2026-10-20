//! Live Supervisor control reuses the canonical full writer and Inbox lease.
//! No new scheduler, recovery permission or second input transaction is created.
use super::root_actor_runtime::{conflict, RootActorWriteProof};
use super::*;
use crate::session_inbox::{FileSessionInbox, InboxAuthority, OwnedFilesystem};
use bamboo_domain::{
    ScopedLoopControlBinding, ScopedLoopControlCheckpoint, ScopedLoopControlClaim,
    SessionActivationPort,
};

#[derive(Clone)]
pub(super) struct ScopedLoopWriteProof {
    binding: ScopedLoopControlBinding,
    activation: Arc<dyn SessionActivationPort>,
    native_census: Option<super::scoped_transport_expiry::ScopedTransportCensus>,
}
impl ScopedLoopWriteProof {
    pub(super) fn validate_retained(
        &self,
        guard: &Arc<dyn bamboo_domain::SessionRunDeliveryGuard>,
    ) -> io::Result<()> {
        if self
            .native_census
            .as_ref()
            .is_none_or(|census| census.check_current_live(&self.binding).is_ok())
            && guard.matches(
                &self.binding.target_session_id,
                self.binding.target_created_at,
                &self.binding.owner_run_id,
            )
        {
            Ok(())
        } else {
            Err(conflict("scoped Loop owner stopped before publication"))
        }
    }

    pub(super) fn retain(&self) -> io::Result<Arc<dyn bamboo_domain::SessionRunDeliveryGuard>> {
        let guard = self
            .activation
            .retain_supervisor_loop_blocking(&self.binding)
            .map_err(conflict)?;
        let guard = match self.native_census.as_ref() {
            Some(census) => census.wrap_live_guard(&self.binding, guard)?,
            None => guard,
        };
        if !guard.matches(
            &self.binding.target_session_id,
            self.binding.target_created_at,
            &self.binding.owner_run_id,
        ) {
            return Err(conflict("scoped Loop retained owner identity mismatch"));
        }
        Ok(guard)
    }
}

#[derive(Clone)]
pub(super) struct ScopedLoopInputProof {
    inbox: FileSessionInbox,
    directory: PathBuf,
    claim: ScopedLoopControlClaim,
    root: Option<RootActorWriteProof>,
    terminal_consumption: Option<bamboo_domain::ScopedLoopControlConsumption>,
}
impl ScopedLoopInputProof {
    pub(super) fn validate(&self, allow_terminal: bool) -> io::Result<()> {
        let allow_terminal = allow_terminal || self.terminal_consumption.is_some();
        match &self.claim {
            ScopedLoopControlClaim::Owned(claim) => {
                if let Some(root) = &self.root {
                    root.validate_deadline(claim.lease.expires_at)?;
                }
                self.inbox
                    .locked_owned_claim(
                        &self.directory,
                        &claim.claim.envelope.target_session_id,
                        claim,
                        Utc::now(),
                        allow_terminal,
                    )
                    .map(|_| ())
                    .map_err(conflict)
            }
            ScopedLoopControlClaim::Legacy(claim) => self
                .inbox
                .locked_scoped_legacy_claim(&self.directory, claim, allow_terminal)
                .map_err(conflict),
        }
    }

    fn validate_consumption(&self, session: &Session) -> io::Result<()> {
        if !bamboo_domain::has_scoped_loop_consumption(session, self.claim.envelope())
            || !session.session_inbox_admission().is_some_and(|cursor| {
                cursor.contains(&self.claim.envelope().id)
                    && cursor.last_admitted_sequence >= self.claim.generation()
            })
        {
            return Err(conflict(
                "scoped Loop checkpoint lacks typed consumption and cursor",
            ));
        }
        if let Some(prior) = &self.terminal_consumption {
            if !bamboo_domain::scoped_loop_consumptions(session)
                .map_err(conflict)?
                .contains(prior)
            {
                return Err(conflict(
                    "immutable scoped Loop consumption changed on ACK retry",
                ));
            }
        }
        Ok(())
    }
}

fn read_committed(directory: &Path) -> io::Result<Session> {
    let main_bytes =
        actor_transcript::regular_bytes(&directory.join("session.json")).map_err(conflict)?;
    compact_main::validate_full_main(&main_bytes)?;
    let main: Session = actor_transcript::decode(&main_bytes).map_err(conflict)?;
    let side: Session = actor_transcript::decode(
        &actor_transcript::regular_bytes(&directory.join(RUNTIME_SIDECAR_FILE))
            .map_err(conflict)?,
    )
    .map_err(conflict)?;
    supervisor::validate_overlay(&main, Some(&side))?;
    if main.id != side.id
        || main.created_at != side.created_at
        || main.kind != SessionKind::Root
        || main.parent_session_id.is_some()
        || main.spawn_depth != 0
        || !main.authority_identity.is_ordinary()
    {
        return Err(conflict(
            "scoped Loop canonical checkpoint identity changed",
        ));
    }
    Ok(overlay_runtime_sidecar(main, Some(side)))
}

impl SessionStoreV2 {
    /// A narrow projection of the existing canonical Loop state, not an
    /// authority source or a persisted closed-run registry. Callers retain
    /// their existing canonical and exact live-owner proofs separately.
    pub(crate) fn scoped_loop_execution_stopped(
        session: &Session,
        binding: &ScopedLoopControlBinding,
    ) -> io::Result<bool> {
        let Some(raw) = session.metadata.get("runtime.counted_loop") else {
            return Ok(false);
        };
        if raw.len() > 128 * 1024 {
            return Err(conflict("scoped Loop state exceeds byte bound"));
        }
        let state: serde_json::Value = serde_json::from_str(raw).map_err(conflict)?;
        let owner = state["owner_execution_id"]
            .as_str()
            .ok_or_else(|| conflict("scoped Loop execution identity is malformed"))?;
        if owner != binding.execution_id.as_str() {
            return Ok(false);
        }
        let Some(control) = state.get("control").filter(|control| !control.is_null()) else {
            return Ok(false);
        };
        control["stop_requested"]
            .as_bool()
            .ok_or_else(|| conflict("scoped Loop stop proof is malformed"))
    }

    pub(super) async fn load_scoped_loop_session_impl(
        &self,
        binding: &ScopedLoopControlBinding,
        activation: Arc<dyn SessionActivationPort>,
    ) -> io::Result<Option<Session>> {
        let authority = self.lock_scoped_loop_authority(binding).await?;
        let session = self
            .load_execution_admission_unchecked(&binding.target_session_id)
            .await?;
        let proof = ScopedLoopWriteProof {
            binding: binding.clone(),
            activation,
            native_census: authority.scoped_native(),
        };
        // Final owner/grant observation is linearized in an owned blocking job.
        // Even caller cancellation cannot release canonical locks mid-check.
        tokio::task::spawn_blocking(move || {
            let _authority = authority;
            let _runtime = proof.retain()?;
            Ok(session)
        })
        .await
        .map_err(io::Error::other)?
    }

    pub(super) async fn checkpoint_scoped_loop_impl(
        &self,
        request: ScopedLoopControlCheckpoint<'_>,
    ) -> io::Result<()> {
        if request
            .session
            .metadata
            .contains_key(bamboo_domain::SCOPED_LOOP_UNCERTAIN_KEY)
        {
            return Err(conflict("scoped Loop checkpoint outcome is uncertain"));
        }
        let binding = request
            .bindings
            .first()
            .ok_or_else(|| conflict("scoped Loop binding set is empty"))?;
        // An owner has one installed grant. Multiple origins may retain that
        // same grant; no caller can combine observations from different grants.
        if request.bindings.iter().any(|other| other != binding) {
            return Err(conflict("scoped Loop binding set is not one current grant"));
        }
        if bamboo_domain::scoped_loop_objective_origins(request.session)
            .map_err(conflict)?
            .iter()
            .any(|origin| !request.bindings.contains(origin))
        {
            return Err(conflict("scoped Loop objective origin is not retained"));
        }
        let authority = self.lock_scoped_loop_authority(binding).await?;
        let root = request.owner.map(|owner| {
            RootActorWriteProof::new(
                self.sessions_dir.join(&binding.target_session_id),
                owner.clone(),
            )
        });
        if request.owner.is_some_and(|owner| {
            owner.fence.actor_id != binding.target_session_id
                || owner.fence.run_id != binding.owner_run_id
                || owner.created_at != binding.target_created_at
        }) {
            return Err(conflict("scoped Loop Root capability identity changed"));
        }
        let proof = ScopedLoopWriteProof {
            binding: binding.clone(),
            activation: request.activation,
            native_census: authority.scoped_native(),
        };
        let mut guards = DefaultWriterGuards::scoped_loop(authority, root.clone(), proof.clone());
        let input_fs: Option<(
            FileSessionInbox,
            PathBuf,
            OwnedFilesystem,
            ScopedLoopControlClaim,
        )> = if let Some((inbox, claim)) = request.input {
            let input = bamboo_domain::scoped_loop_control_input(claim.envelope())
                .map_err(conflict)?
                .ok_or_else(|| conflict("scoped Loop input required"))?;
            if input.binding != *binding {
                return Err(conflict("scoped Loop input binding changed"));
            }
            let inbox = inbox
                .as_any()
                .and_then(|any| any.downcast_ref::<FileSessionInbox>())
                .ok_or_else(|| conflict("scoped Loop requires the canonical concrete Inbox"))?
                .bind_scoped_checkpoint(request.owner, &self.sessions_dir)?;
            let (directory, filesystem) = inbox
                .filesystem_with_authority(
                    &binding.target_session_id,
                    InboxAuthority::Root {
                        _guard: guards.physical.clone(),
                    },
                )
                .await
                .map_err(conflict)?;
            let canonical = self
                .load_execution_admission_unchecked(&binding.target_session_id)
                .await?
                .ok_or_else(|| conflict("Loop input target disappeared"))?;
            let terminal_consumption =
                if bamboo_domain::has_scoped_loop_consumption(&canonical, claim.envelope())
                    && canonical.session_inbox_admission().is_some_and(|cursor| {
                        cursor.contains(&claim.envelope().id)
                            && cursor.last_admitted_sequence >= claim.generation()
                    })
                {
                    bamboo_domain::scoped_loop_consumptions(&canonical)
                        .map_err(conflict)?
                        .into_iter()
                        .find(|record| record.input_id == claim.envelope().id)
                } else {
                    None
                };
            let input = ScopedLoopInputProof {
                terminal_consumption,
                inbox: inbox.clone(),
                directory: directory.clone(),
                claim: claim.clone(),
                root: root.clone(),
            };
            let validate = input.clone();
            let target_directory = self.sessions_dir.join(&binding.target_session_id);
            let root_check = root.clone();
            let retain = proof.clone();
            let filesystem = filesystem
                .with_runtime_retainer(Arc::new(move || retain.retain()), binding.clone())
                .with_check(Arc::new(move || {
                    validate.validate(true)?;
                    let durable = match &root_check {
                        Some(root) => root.validate()?,
                        None => read_committed(&target_directory)?,
                    };
                    validate.validate_consumption(&durable)
                }));
            let fresh = Arc::get_mut(&mut guards).expect("new scoped writer has no borrowers");
            fresh.scoped_input = Some(input);
            fresh._input_filesystem = Some(filesystem.clone());
            Some((inbox, directory, filesystem, claim))
        } else {
            None
        };
        let target = binding.target_session_id.clone();
        let result = async {
            let canonical = self
                .load_execution_admission_unchecked(&target)
                .await?
                .ok_or_else(|| conflict("scoped Loop target disappeared"))?;
            if request.session.id != canonical.id
                || request.session.created_at != canonical.created_at
                || request.session.metadata_version != canonical.metadata_version
                || request.session.authority_identity != canonical.authority_identity
                || request.session.kind != SessionKind::Root
                || request.session.parent_session_id.is_some()
                || request.session.spawn_depth != 0
                || request.session.project_id_meta() != canonical.project_id_meta()
                || request.session.root_tool_authority_revision
                    != canonical.root_tool_authority_revision
                || request.session.root_orchestration_only != canonical.root_orchestration_only
                || request
                    .session
                    .agent_runtime_state
                    .as_ref()
                    .and_then(|runtime| runtime.execution_id.as_deref())
                    != Some(binding.execution_id.as_str())
            {
                return Err(conflict(
                    "scoped Loop candidate is not the current canonical lifetime/execution",
                ));
            }
            // Keep append-only history and admission facts from concurrent
            // canonical writers; this port owns no new provider input.
            bamboo_domain::append_missing_runtime_messages(request.session, &canonical);
            bamboo_domain::merge_session_inbox_admission(request.session, &canonical);
            if let Some(input) = &guards.scoped_input {
                input.validate(false)?;
                input.validate_consumption(request.session)?;
            }
            self.save_session_after_lock(request.session, Instant::now(), &guards, None)
                .await?;
            let committed = self
                .load_execution_admission_unchecked(&target)
                .await?
                .ok_or_else(|| conflict("scoped Loop committed target disappeared"))?;
            let committed_for_publish = committed.clone();
            let root_check = root.clone();
            let input_check = guards.scoped_input.clone();
            let publish = request.publish;
            Self::default_writer_job(&guards, move || {
                if let Some(root) = root_check {
                    root.validate()?;
                }
                if let Some(input) = input_check {
                    input.validate(false)?;
                    input.validate_consumption(&committed_for_publish)?;
                }
                publish(&committed_for_publish);
                Ok(())
            })
            .await?;
            *request.session = committed;
            if let Some((inbox, directory, filesystem, claim)) = input_fs {
                let input = bamboo_domain::scoped_loop_control_input(claim.envelope())
                    .map_err(conflict)?
                    .ok_or_else(|| conflict("scoped Loop input disappeared before ACK"))?;
                let configured = bamboo_domain::scoped_loop_consumptions(request.session)
                    .map_err(conflict)?
                    .iter()
                    .any(|record| record.input_id == claim.envelope().id && record.configured);
                if matches!(
                    input.command,
                    bamboo_domain::LoopControlRequest::Stop { .. }
                ) && configured
                    && Self::scoped_loop_execution_stopped(request.session, binding)?
                {
                    // Main's typed Stop consumption is real; keep its original
                    // cur/lease proof live until all non-admitted tail inputs
                    // are tombstoned. Every rename still checks the same real
                    // writer and owner; only then ACK the actual Stop input.
                    inbox
                        .retire_scoped_loop_tail_with_filesystem(
                            &directory,
                            binding,
                            &claim,
                            request.session,
                            &filesystem,
                        )
                        .await
                        .map_err(conflict)?;
                }
                match claim {
                    ScopedLoopControlClaim::Owned(claim) => inbox
                        .ack_owned_with_filesystem(
                            &directory,
                            &target,
                            &claim,
                            Utc::now(),
                            filesystem,
                        )
                        .await
                        .map_err(conflict)?,
                    ScopedLoopControlClaim::Legacy(claim) => inbox
                        .ack_scoped_legacy_with_filesystem(&directory, &target, &claim, filesystem)
                        .await
                        .map_err(conflict)?,
                }
            }
            Ok(())
        }
        .await;
        if result.is_err() {
            // A failed write or ACK is not rollback. Preserve the latest real
            // Main/sidecar facts, including commit-then-error, while still holding
            // this exact canonical lock set. Unknown reads poison only this
            // caller snapshot; the marker itself is never a durable mutation.
            match self.load_execution_admission_unchecked(&target).await {
                Ok(Some(canonical)) => *request.session = canonical,
                _ => {
                    request.session.metadata.insert(
                        bamboo_domain::SCOPED_LOOP_UNCERTAIN_KEY.into(),
                        "true".into(),
                    );
                }
            }
        }
        result
    }
}

impl SessionStoreV2 {
    /// Canonical target locks precede Inbox; the exact closed-run guard is
    /// acquired only in physical jobs, after Inbox. Never re-enter a writer.
    pub(super) async fn retire_scoped_loop_transport_impl(
        &self,
        request: bamboo_domain::storage::ScopedLoopTransportRetirement<'_>,
    ) -> io::Result<()> {
        let binding = request.binding;
        validate_session_id(&binding.target_session_id)?;
        let lifecycle = self.lock_default_writer_lifecycle().await?;
        let task = self.lock_runtime_task_sidecar_shared().await?;
        let session = self
            .acquire_session_write_lock(&binding.target_session_id, SaveKind::Runtime)
            .await?;
        let directory = self.sessions_dir.join(&binding.target_session_id);
        let root = request
            .owner
            .map(|owner| RootActorWriteProof::new(directory.clone(), owner.clone()));
        if request.owner.is_some_and(|owner| {
            owner.fence.actor_id != binding.target_session_id
                || owner.created_at != binding.target_created_at
                || owner.fence.run_id != binding.owner_run_id
        }) {
            return Err(conflict("target retirement Root identity changed"));
        }
        let guards =
            DefaultWriterGuards::shared_with_root_actor(lifecycle, task, session, root.clone());
        let census = super::scoped_transport_expiry::ScopedTransportCensus::read(&directory)?
            .ok_or_else(|| conflict("target retirement canonical census missing"))?;
        let committed = census.session.clone();
        let native_inert = census.native_inert(binding)?;
        if committed.id != binding.target_session_id
            || committed.created_at != binding.target_created_at
        {
            return Err(conflict("target retirement canonical lifetime changed"));
        }
        if !native_inert {
            if committed
                .agent_runtime_state
                .as_ref()
                .and_then(|state| state.execution_id.as_deref())
                != Some(binding.execution_id.as_str())
            {
                return Err(conflict("target retirement canonical execution changed"));
            }
            if let Some(root) = &root {
                root.validate()?;
            } else {
                self.check_default_actor_clear(&directory).await?;
            }
        }
        let inbox = request
            .inbox
            .as_any()
            .and_then(|value| value.downcast_ref::<FileSessionInbox>())
            .ok_or_else(|| conflict("target retirement requires its concrete Inbox"))?;
        let inbox = inbox.bind_scoped_checkpoint(request.owner, &self.sessions_dir)?;
        let (dir, filesystem) = inbox
            .filesystem_with_authority(
                &binding.target_session_id,
                InboxAuthority::Root {
                    _guard: guards.physical.clone(),
                },
            )
            .await
            .map_err(conflict)?;
        let filesystem = filesystem.with_runtime_retainer(request.retain, binding.clone());
        let filesystem = if native_inert {
            let binding = binding.clone();
            filesystem.with_check(Arc::new(move || {
                let current = census.require_same_snapshot()?;
                if current.native_inert(&binding)? {
                    Ok(())
                } else {
                    Err(conflict("target transport expiry no longer proved"))
                }
            }))
        } else if let Some(root) = root {
            filesystem.with_check(Arc::new(move || root.validate().map(|_| ())))
        } else {
            filesystem
        };
        inbox
            .retire_closed_loop_with_filesystem(&dir, binding, &committed, &filesystem)
            .await
            .map_err(conflict)
    }
}
