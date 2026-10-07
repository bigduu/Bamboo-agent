//! Ordinary Root input uses the existing full writer and owned Maildir lease.
use super::root_actor_runtime::{conflict, RootActorWriteProof};
use super::*;
use crate::session_inbox::{FileSessionInbox, InboxAuthority, OwnedFilesystem};
use bamboo_domain::{
    RootActorRuntimePublisher, RootActorRuntimeWrite, SessionInboxOwnedClaim, SessionInboxPort,
    SessionMessageEnvelope,
};

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
        if target != owner.fence.actor_id {
            return Err(conflict("Root Inbox target changed"));
        }
        validate_session_id(target)?;
        let lifecycle = self.lock_default_writer_lifecycle().await?;
        let task = self.lock_runtime_task_sidecar_shared().await?;
        let session = self
            .acquire_session_write_lock(target, SaveKind::Full)
            .await?;
        let proof = RootActorWriteProof::new(self.sessions_dir.join(target), owner.clone());
        let guards = DefaultWriterGuards::shared_with_root_actor(
            lifecycle,
            task,
            session,
            Some(proof.clone()),
        );
        Self::default_writer_job(&guards, move || proof.validate().map(|_| ())).await?;
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
            if durable
                .messages
                .iter()
                .any(|message| bamboo_domain::is_matching_session_message(message, &envelope))
            {
                Ok(())
            } else {
                Err(conflict(
                    "terminal Inbox receipt lacks typed canonical proof",
                ))
            }
        })
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
