//! Ordinary Root input uses the existing full writer and owned Maildir lease.
use super::root_actor_runtime::{conflict, RootActorWriteProof};
use super::*;
use crate::session_inbox::{FileSessionInbox, InboxAuthority, OwnedFilesystem};
use bamboo_domain::{
    RootActorRuntimePublisher, RootActorRuntimeWrite, SessionInboxOwnedClaim, SessionInboxPort,
    SessionMessageEnvelope,
};

/// A detached physical job can outlive the async waiter. Dropping the waiter
/// cancels the same gate that the final synchronous effect must commit.
struct CancelPendingOnDrop {
    gate: bamboo_domain::AdmissionGate,
    armed: bool,
}

impl CancelPendingOnDrop {
    fn new(gate: bamboo_domain::AdmissionGate) -> Self {
        Self { gate, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CancelPendingOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.gate.cancel_if_pending();
        }
    }
}

/// Nonserializable, process-local observation. It is not provider authority.
struct LiveBootstrapCapture {
    owner: RootActorRuntimeWrite,
    committed: serde_json::Value,
    inbox_directory: PathBuf,
    inbox_generation: u64,
    consumed: AtomicBool,
}

impl LiveBootstrapCapture {
    fn matches_owner(&self, owner: &RootActorRuntimeWrite) -> bool {
        self.owner.fence == owner.fence
            && self.owner.created_at == owner.created_at
            && self.owner.conversation_generation == owner.conversation_generation
    }
}

fn ensure_live_bootstrap_canonical_file_sizes(directory: &Path) -> io::Result<()> {
    for name in [
        "session.json",
        RUNTIME_SIDECAR_FILE,
        "actor-authority.json",
        "actor-authority.initialized.json",
    ] {
        let meta = std::fs::symlink_metadata(directory.join(name))?;
        if !meta.file_type().is_file()
            || meta.len() > crate::session_merge::LIVE_BOOTSTRAP_SESSION_JSON_LIMIT as u64
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "live bootstrap canonical file exceeds limit or is invalid",
            ));
        }
    }
    Ok(())
}

#[derive(Clone)]
pub(super) struct RootActorInputProof {
    root: RootActorWriteProof,
    inbox: FileSessionInbox,
    directory: PathBuf,
    claim: SessionInboxOwnedClaim,
}

impl RootActorInputProof {
    pub(super) fn validate(&self, allow_terminal: bool) -> io::Result<()> {
        self.root.validate_deadline(self.claim.lease.expires_at)?;
        self.inbox
            .locked_owned_claim(
                &self.directory,
                &self.claim.claim.envelope.target_session_id,
                &self.claim,
                Utc::now(),
                allow_terminal,
            )
            .map(|_| ())
            .map_err(conflict)
    }

    pub(super) fn validate_transcript(&self, session: &Session) -> io::Result<()> {
        if !session.messages.iter().any(|message| {
            bamboo_domain::is_matching_session_message(message, &self.claim.claim.envelope)
        }) || !session.session_inbox_admission().is_some_and(|cursor| {
            cursor.contains(&self.claim.claim.envelope.id)
                && cursor.last_admitted_sequence >= self.claim.claim.generation
        }) {
            return Err(conflict(
                "Root input checkpoint lacks typed message and admission proof",
            ));
        }
        Ok(())
    }

    /// Derive the Main-owned facts only from this live Actor and the exact
    /// retained Inbox lease. An ordinary Root message has no Child-style
    /// checkpoint marker; its original intent comes from the locked transport.
    pub(super) fn outcomes_after_input(
        &self,
        durable: &Session,
    ) -> io::Result<Option<bamboo_domain::SupervisedRootOutcomes>> {
        self.root.validate_deadline(self.claim.lease.expires_at)?;
        let (envelope, intent) = self
            .inbox
            .locked_owned_claim(
                &self.directory,
                &self.claim.claim.envelope.target_session_id,
                &self.claim,
                Utc::now(),
                false,
            )
            .map_err(conflict)?;
        let origin = intent
            .as_ref()
            .and_then(bamboo_domain::SessionInboxActivationIntent::supervised_root_origin)
            .cloned();
        if origin
            .as_ref()
            .is_some_and(|origin| !origin.matches_input_envelope(&envelope))
        {
            return Err(conflict("Root input origin differs from its owned claim"));
        }
        let previous = durable
            .runtime_metadata
            .as_ref()
            .and_then(|metadata| metadata.supervised_root_outcomes.clone());
        if previous.is_none() && origin.is_none() {
            return Ok(None);
        }
        let owner = self.root.owner();
        let mut outcomes = match previous {
            Some(outcomes) => outcomes,
            None => bamboo_domain::SupervisedRootOutcomes::new(&durable.id, durable.created_at)
                .map_err(conflict)?,
        };
        let reset = durable.conversation_reset_sha256().map_err(conflict)?;
        // Unrelated later Human/peer runs must not allocate a supervised fact.
        // Inputs admitted during an already tracked run remain associated with
        // that run, even when their own Supervisor origin is unknown.
        if origin.is_none()
            && outcomes
                .facts
                .last()
                .is_none_or(|fact| fact.run_id != owner.fence.run_id || fact.result.is_some())
        {
            if outcomes
                .facts
                .last()
                .is_some_and(|fact| fact.result.is_none() && fact.run_id != owner.fence.run_id)
            {
                outcomes
                    .mark_predecessor_interrupted(&owner.fence, reset.as_deref())
                    .map_err(conflict)?;
            }
            return Ok(Some(outcomes));
        }
        if outcomes
            .facts
            .last()
            .is_some_and(|fact| fact.result.is_none() && fact.run_id != owner.fence.run_id)
        {
            // The current Root proof establishes this newer live Actor fence;
            // unresolved predecessor evidence is marked unknown, never success.
            outcomes
                .recover_predecessor_and_reserve(&owner.fence, reset.as_deref())
                .map_err(conflict)?;
        } else {
            outcomes
                .reserve(&owner.fence, reset.as_deref())
                .map_err(conflict)?;
        }
        outcomes
            .record_input(
                &owner.fence,
                bamboo_domain::SupervisedRootOutcomeInput {
                    receipt_id: envelope.id,
                    generation: self.claim.claim.generation,
                    supervisor_origin: origin,
                },
            )
            .map_err(conflict)?;
        Ok(Some(outcomes))
    }
}

impl SessionStoreV2 {
    pub(crate) fn root_sessions_directory(&self) -> &Path {
        &self.sessions_dir
    }

    pub(super) fn bound_root_inbox(
        &self,
        owner: &RootActorRuntimeWrite,
        inbox: &Arc<dyn SessionInboxPort>,
    ) -> io::Result<FileSessionInbox> {
        inbox
            .as_any()
            .and_then(|any| any.downcast_ref::<FileSessionInbox>())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Unsupported,
                    "Root admission requires the concrete owned Inbox",
                )
            })?
            .bind_root_owner(owner, &self.sessions_dir)
    }

    async fn root_input_guards(
        &self,
        target: &str,
        owner: &RootActorRuntimeWrite,
    ) -> io::Result<Arc<DefaultWriterGuards>> {
        self.root_input_guards_inner(target, owner, false).await
    }

    async fn root_input_guards_inner(
        &self,
        target: &str,
        owner: &RootActorRuntimeWrite,
        limit_recovery_files: bool,
    ) -> io::Result<Arc<DefaultWriterGuards>> {
        if target != owner.fence.actor_id {
            return Err(conflict("Root Inbox target changed"));
        }
        validate_session_id(target)?;
        let lifecycle = self.lock_default_writer_lifecycle().await?;
        let task = self.lock_runtime_task_sidecar_shared().await?;
        let session = self
            .acquire_session_write_lock(target, SaveKind::Full)
            .await?;
        let directory = self.sessions_dir.join(target);
        let proof = RootActorWriteProof::new(directory.clone(), owner.clone());
        let guards = DefaultWriterGuards::shared_with_root_actor(
            lifecycle,
            task,
            session,
            Some(proof.clone()),
        );
        Self::default_writer_job(&guards, move || {
            if limit_recovery_files {
                ensure_live_bootstrap_canonical_file_sizes(&directory)?;
            }
            proof.validate().map(|_| ())
        })
        .await?;
        Ok(guards)
    }

    pub(crate) async fn root_owned_inbox_filesystem(
        &self,
        inbox: &FileSessionInbox,
        target: &str,
        owner: &RootActorRuntimeWrite,
    ) -> io::Result<(PathBuf, OwnedFilesystem)> {
        let guards = self.root_input_guards(target, owner).await?;
        let proof = guards.root_actor.clone().expect("bound Root proof");
        let (directory, filesystem) = inbox
            .filesystem_with_authority(
                target,
                InboxAuthority::Root {
                    _guard: guards.physical.clone(),
                },
            )
            .await
            .map_err(conflict)?;
        Ok((
            directory,
            filesystem.with_check(Arc::new(move || proof.validate().map(|_| ()))),
        ))
    }

    pub(crate) fn root_inbox_lease_check(
        &self,
        owner: &RootActorRuntimeWrite,
        deadline: DateTime<Utc>,
    ) -> Arc<dyn Fn() -> io::Result<()> + Send + Sync> {
        let proof =
            RootActorWriteProof::new(self.sessions_dir.join(&owner.fence.actor_id), owner.clone());
        Arc::new(move || proof.validate_deadline(deadline))
    }

    pub(crate) fn root_inbox_terminal_check(
        &self,
        owner: &RootActorRuntimeWrite,
        envelope: &SessionMessageEnvelope,
    ) -> Arc<dyn Fn() -> io::Result<()> + Send + Sync> {
        let proof =
            RootActorWriteProof::new(self.sessions_dir.join(&owner.fence.actor_id), owner.clone());
        let envelope = envelope.clone();
        Arc::new(move || {
            let durable = proof.validate()?;
            if (bamboo_domain::is_scoped_loop_control(&envelope)
                && bamboo_domain::has_scoped_loop_consumption(&durable, &envelope)
                && durable
                    .session_inbox_admission()
                    .is_some_and(|cursor| cursor.contains(&envelope.id)))
                || (!bamboo_domain::is_scoped_loop_control(&envelope)
                    && durable.messages.iter().any(|message| {
                        bamboo_domain::is_matching_session_message(message, &envelope)
                    }))
            {
                Ok(())
            } else {
                Err(conflict(
                    "terminal Inbox receipt lacks typed canonical proof",
                ))
            }
        })
    }

    pub(super) async fn checkpoint_root_live_bootstrap_impl(
        &self,
        owner: &RootActorRuntimeWrite,
        session: &Session,
        inbox: Arc<dyn SessionInboxPort>,
        gate: bamboo_domain::AdmissionGate,
        publish: RootActorRuntimePublisher,
    ) -> io::Result<bamboo_domain::LiveBootstrapProof> {
        let mut cancel_on_drop = CancelPendingOnDrop::new(gate.clone());
        if gate.is_cancelled() || gate.is_committed() {
            return Err(conflict("live bootstrap gate unavailable"));
        }
        let started = Instant::now();
        let inbox = self.bound_root_inbox(owner, &inbox)?;
        let mut guards = self
            .root_input_guards_inner(&session.id, owner, true)
            .await?;
        let root = guards.root_actor.clone().expect("bound Root proof");
        let (directory, filesystem) = inbox
            .filesystem_with_authority(
                &session.id,
                InboxAuthority::Root {
                    _guard: guards.physical.clone(),
                },
            )
            .await
            .map_err(conflict)?;
        if directory != self.sessions_dir.join(&session.id).join("inbox") {
            return Err(conflict("Root Inbox directory changed"));
        }
        // The started physical writer retains Inbox as well as Session/Actor
        // even if its awaiting task is cancelled during a blocking write.
        Arc::get_mut(&mut guards)
            .expect("new Root guards are private")
            ._input_filesystem = Some(filesystem.clone());
        let expected = crate::session_merge::bounded_live_bootstrap_value(session)?;
        let candidate = session.clone();
        let check_root = root.clone();
        let check_session_directory = self.sessions_dir.join(&session.id);
        let check_dir = directory.clone();
        let check_filesystem = filesystem.clone();
        let check_gate = gate.clone();
        let before = Self::default_writer_job(&guards, move || {
            if check_gate.is_cancelled() || check_gate.is_committed() {
                return Err(conflict("live bootstrap gate unavailable"));
            }
            ensure_live_bootstrap_canonical_file_sizes(&check_session_directory)?;
            check_root.validate_candidate(&candidate)?;
            let current = check_root.validate()?;
            crate::session_merge::validate_live_bootstrap_candidate(&candidate, &current)?;
            crate::session_inbox::empty_live_bootstrap_generation_locked(
                &check_dir,
                &check_filesystem,
            )
            .map_err(conflict)
        })
        .await?;
        self.save_session_after_lock(session, started, &guards, None)
            .await?;
        let check_root = root.clone();
        let check_session_directory = self.sessions_dir.join(&session.id);
        let check_dir = directory.clone();
        let check_filesystem = filesystem.clone();
        let check_expected = expected.clone();
        let check_gate = gate.clone();
        Self::default_writer_job(&guards, move || {
            if check_gate.is_cancelled() || check_gate.is_committed() {
                return Err(conflict("live bootstrap gate unavailable"));
            }
            ensure_live_bootstrap_canonical_file_sizes(&check_session_directory)?;
            let saved = check_root.validate()?;
            if crate::session_merge::bounded_live_bootstrap_value(&saved)? != check_expected {
                return Err(conflict("live bootstrap Session readback changed"));
            }
            let after = crate::session_inbox::empty_live_bootstrap_generation_locked(
                &check_dir,
                &check_filesystem,
            )
            .map_err(conflict)?;
            if after != before {
                return Err(conflict("live bootstrap Inbox changed"));
            }
            Ok(())
        })
        .await?;
        // Reuse the existing guarded cache publication, not an event or grant.
        self.publish_root_actor_runtime(&guards, publish).await?;
        if gate.is_cancelled() || gate.is_committed() {
            return Err(conflict("live bootstrap gate unavailable"));
        }
        let proof = bamboo_domain::LiveBootstrapProof::from_storage(
            LiveBootstrapCapture {
                owner: owner.clone(),
                committed: expected,
                inbox_directory: directory,
                inbox_generation: before,
                consumed: AtomicBool::new(false),
            },
            gate,
        );
        cancel_on_drop.disarm();
        Ok(proof)
    }

    pub(super) async fn confirm_root_live_bootstrap_impl(
        &self,
        owner: &RootActorRuntimeWrite,
        proof: &bamboo_domain::LiveBootstrapProof,
        inbox: Arc<dyn SessionInboxPort>,
        publish: bamboo_domain::RootActorRuntimeEventPublisher,
    ) -> io::Result<()> {
        let captured = proof
            .storage_ref::<LiveBootstrapCapture>()
            .ok_or_else(|| conflict("wrong live bootstrap proof"))?;
        let mut cancel_on_drop = CancelPendingOnDrop::new(proof.storage_gate());
        // Storage validates the actor and commits a supplied synchronous
        // effect once; it cannot create a provider continuation. The private
        // Host must supply that effect only for a live provider-created offer
        // whose gate matches this proof.
        if !captured.matches_owner(owner) {
            return Err(conflict("live bootstrap owner unavailable"));
        }
        let inbox = self.bound_root_inbox(owner, &inbox)?;
        let mut guards = self
            .root_input_guards_inner(&owner.fence.actor_id, owner, true)
            .await?;
        let root = guards.root_actor.clone().expect("bound Root proof");
        let (directory, filesystem) = inbox
            .filesystem_with_authority(
                &owner.fence.actor_id,
                InboxAuthority::Root {
                    _guard: guards.physical.clone(),
                },
            )
            .await
            .map_err(conflict)?;
        if directory != self.sessions_dir.join(&owner.fence.actor_id).join("inbox")
            || directory != captured.inbox_directory
        {
            return Err(conflict("Root Inbox directory changed"));
        }
        Arc::get_mut(&mut guards)
            .expect("new Root guards are private")
            ._input_filesystem = Some(filesystem.clone());
        let held = proof.clone();
        let session_directory = self.sessions_dir.join(&owner.fence.actor_id);
        let result = Self::default_writer_job(&guards, move || {
            let captured = held
                .storage_ref::<LiveBootstrapCapture>()
                .ok_or_else(|| conflict("wrong live bootstrap proof"))?;
            let check = || -> io::Result<()> {
                ensure_live_bootstrap_canonical_file_sizes(&session_directory)?;
                let current = root.validate()?;
                if crate::session_merge::bounded_live_bootstrap_value(&current)?
                    != captured.committed
                {
                    return Err(conflict("live bootstrap Session changed"));
                }
                let generation = crate::session_inbox::empty_live_bootstrap_generation_locked(
                    &directory,
                    &filesystem,
                )
                .map_err(conflict)?;
                if generation != captured.inbox_generation {
                    return Err(conflict("live bootstrap Inbox changed"));
                }
                Ok(())
            };
            check()?;
            match held.storage_gate().commit(|| {
                check()?;
                // Burn before invoking the callback: even a lost ACK after an
                // effect must not let a retry invoke the decision twice.
                if captured.consumed.swap(true, Ordering::AcqRel) {
                    return Err(conflict("live bootstrap proof already consumed"));
                }
                publish(&check)
            })? {
                bamboo_domain::AdmissionCommit::Committed(()) => Ok(()),
                bamboo_domain::AdmissionCommit::Cancelled => {
                    Err(conflict("live bootstrap cancelled"))
                }
                bamboo_domain::AdmissionCommit::AlreadyCommitted => {
                    Err(conflict("live bootstrap already committed"))
                }
            }
        })
        .await;
        if result.is_ok() {
            cancel_on_drop.disarm();
        }
        result
    }

    pub(super) async fn save_root_actor_input_impl(
        &self,
        owner: &RootActorRuntimeWrite,
        session: &mut Session,
        inbox: Arc<dyn SessionInboxPort>,
        claim: &SessionInboxOwnedClaim,
        publish: RootActorRuntimePublisher,
        inherited: Option<&bamboo_domain::InheritedChildWait>,
    ) -> io::Result<()> {
        let total_started = Instant::now();
        let inbox = self.bound_root_inbox(owner, &inbox)?;
        let mut guards = self.root_input_guards(&session.id, owner).await?;
        let root_proof = guards.root_actor.clone().expect("bound Root proof");
        let (directory, filesystem) = inbox
            .filesystem_with_authority(
                &session.id,
                InboxAuthority::Root {
                    _guard: guards.physical.clone(),
                },
            )
            .await
            .map_err(conflict)?;
        let input = RootActorInputProof {
            root: root_proof.clone(),
            inbox: inbox.clone(),
            directory: directory.clone(),
            claim: claim.clone(),
        };
        let ack_input = input.clone();
        let filesystem = filesystem.with_check(Arc::new(move || {
            let durable = root_proof.validate()?;
            ack_input.validate(true)?;
            ack_input.validate_transcript(&durable)
        }));
        let mutable = Arc::get_mut(&mut guards).expect("new Root writer has no borrowers");
        mutable.input = Some(input);
        mutable._input_filesystem = Some(filesystem.clone());
        let reconciled = match inherited {
            Some(inherited) => Some(
                self.reconcile_inherited_runtime_snapshot(session, inherited, false)
                    .await?,
            ),
            None => None,
        };
        self.save_session_after_lock(
            reconciled.as_ref().unwrap_or(session),
            total_started,
            &guards,
            None,
        )
        .await?;
        self.publish_root_actor_runtime(&guards, publish).await?;
        if let Some(reconciled) = reconciled {
            *session = reconciled;
        }
        // No provider boundary or new lease driver between Main and ACK.
        inbox
            .ack_owned_with_filesystem(&directory, &session.id, claim, Utc::now(), filesystem)
            .await
            .map_err(conflict)
    }
}
