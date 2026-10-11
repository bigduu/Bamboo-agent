//! Dedicated same-process Child execution writer. Root capabilities remain Root-only.
use super::*;
use crate::session_inbox::{FileSessionInbox, InboxAuthority, OwnedFilesystem};
use bamboo_domain::{
    ActorActivationStatus, ActorDirectoryEntry, ChildActorRuntimeSave, ChildActorRuntimeWrite,
    ParentQuestion, ParentQuestionResolution, PendingQuestionSource,
    RootActorRuntimeEventPublisher, RootActorRuntimePublisher, SessionAuthorityConflict,
    SessionInboxOwnedClaim, SessionInboxPort, SessionMessageEnvelope, PARENT_QUESTION_REQUEST_KEY,
    PARENT_QUESTION_RESOLUTION_KEY,
};

fn conflict(reason: impl std::fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        SessionAuthorityConflict(format!("Child Actor runtime authority rejected: {reason}")),
    )
}

#[derive(Clone)]
pub(super) struct ChildActorWriteProof {
    sessions: PathBuf,
    owner: ChildActorRuntimeWrite,
}

impl ChildActorWriteProof {
    fn new(sessions: PathBuf, owner: ChildActorRuntimeWrite) -> io::Result<Self> {
        for id in [
            &owner.fence.actor_id,
            &owner.root_session_id,
            &owner.parent_session_id,
        ] {
            validate_session_id(id)?;
        }
        if owner.spawn_depth == 0 || owner.root_session_id == owner.fence.actor_id {
            return Err(conflict("invalid Child ancestry"));
        }
        Ok(Self { sessions, owner })
    }

    fn directory(&self) -> PathBuf {
        self.sessions
            .join(&self.owner.root_session_id)
            .join("children")
            .join(&self.owner.fence.actor_id)
    }

    fn pair(&self, directory: &Path) -> io::Result<(Session, Session)> {
        let main =
            actor_transcript::regular_bytes(&directory.join("session.json")).map_err(conflict)?;
        compact_main::validate_full_main(&main).map_err(conflict)?;
        let mut main: Session = actor_transcript::decode(&main).map_err(conflict)?;
        let side = actor_transcript::regular_bytes(&directory.join(RUNTIME_SIDECAR_FILE))
            .map_err(conflict)?;
        let mut side: Session = actor_transcript::decode(&side).map_err(conflict)?;
        supervisor::validate_overlay(&main, Some(&side)).map_err(conflict)?;
        for s in [&mut main, &mut side] {
            if s.kind == SessionKind::Root && s.root_session_id.is_empty() {
                s.root_session_id = s.id.clone();
            }
        }
        if main.id != side.id
            || main.kind != side.kind
            || main.created_at != side.created_at
            || main.parent_session_id != side.parent_session_id
            || main.parent_created_at != side.parent_created_at
            || main.root_session_id != side.root_session_id
            || main.spawn_depth != side.spawn_depth
            || main.authority_identity != side.authority_identity
            || main.project_id_meta() != side.project_id_meta()
        {
            return Err(conflict("canonical identity pair changed"));
        }
        Ok((main, side))
    }

    fn identity(&self, session: &Session) -> io::Result<()> {
        if session.id != self.owner.fence.actor_id
            || session.kind != SessionKind::Child
            || !session.authority_identity.is_ordinary()
            || session.created_at != self.owner.created_at
            || session.root_session_id != self.owner.root_session_id
            || session.parent_session_id.as_deref() != Some(self.owner.parent_session_id.as_str())
            || session.parent_created_at != Some(self.owner.parent_created_at)
            || session.spawn_depth != self.owner.spawn_depth
            || session.project_id_meta() != self.owner.project_id
        {
            return Err(conflict("Child birth, Project or ancestry changed"));
        }
        Ok(())
    }

    pub(super) fn validate(&self) -> io::Result<Session> {
        let directory = self.directory();
        let (main, side) = self.pair(&directory)?;
        self.identity(&main)?;
        self.identity(&side)?;
        let record = actor_transcript::regular_bytes(&directory.join("actor-authority.json"))
            .map_err(conflict)?;
        let marker =
            actor_transcript::regular_bytes(&directory.join("actor-authority.initialized.json"))
                .map_err(conflict)?;
        let project = self
            .owner
            .project_id
            .as_deref()
            .map(bamboo_domain::ProjectId::parse)
            .transpose()
            .map_err(conflict)?;
        actor_directory::validate_census_witnesses(&record, &marker, &main, project.as_ref())
            .map_err(conflict)?;
        actor_directory::validate_census_witnesses(&record, &marker, &side, project.as_ref())
            .map_err(conflict)?;
        let entry: ActorDirectoryEntry = actor_transcript::decode(&record).map_err(conflict)?;
        let activation = actor_directory::current_live(&entry, &self.owner.fence, Utc::now())
            .map_err(conflict)?;
        if activation.status != ActorActivationStatus::Running {
            return Err(conflict("Child is not Running"));
        }
        // Pure canonical tree reads, never an index hint or a reconstructed owner.
        let mut current = main.clone();
        for ancestor in &entry.actor.ancestor_observations {
            if current.parent_session_id.as_deref() != Some(ancestor.actor_id.as_str())
                || current.parent_created_at != Some(ancestor.session_created_at)
            {
                return Err(conflict("Child ancestor birth changed"));
            }
            validate_session_id(&ancestor.actor_id)?;
            let is_root = ancestor.actor_id == self.owner.root_session_id;
            let directory = if is_root {
                self.sessions.join(&ancestor.actor_id)
            } else {
                self.sessions
                    .join(&self.owner.root_session_id)
                    .join("children")
                    .join(&ancestor.actor_id)
            };
            let (parent, side) = self.pair(&directory)?;
            if parent.id != ancestor.actor_id
                || parent.created_at != ancestor.session_created_at
                || parent.kind
                    != if is_root {
                        SessionKind::Root
                    } else {
                        SessionKind::Child
                    }
                || parent.root_session_id != self.owner.root_session_id
                || parent.spawn_depth.checked_add(1) != Some(current.spawn_depth)
                || parent.project_id_meta() != self.owner.project_id
                || parent.created_at > current.created_at
            {
                return Err(conflict("canonical ancestor identity changed"));
            }
            current = overlay_runtime_sidecar(parent, Some(side));
        }
        if current.id != self.owner.root_session_id
            || current.kind != SessionKind::Root
            || current.spawn_depth != 0
            || current.parent_session_id.is_some()
        {
            return Err(conflict("incomplete Child ancestor chain"));
        }
        Ok(overlay_runtime_sidecar(main, Some(side)))
    }

    pub(super) fn validate_candidate(&self, incoming: &Session) -> io::Result<()> {
        self.identity(incoming)?;
        self.validate().map(|_| ())
    }

    /// Normalize only this live Child execution's own clarification checkpoint.
    /// The existing direct-parent request/resolution protocol remains the source
    /// of replay authority; this does not grant the Child an answer-write permit.
    fn normalize_parent_question(
        &self,
        candidate: &mut Session,
        runtime_only: bool,
    ) -> io::Result<()> {
        let Some(pending) = candidate.pending_question.as_ref() else {
            return Ok(());
        };
        if !matches!(
            pending.source,
            PendingQuestionSource::AgenticClarification | PendingQuestionSource::DirectParent
        ) {
            return Ok(());
        }
        self.identity(candidate)?;
        if candidate
            .agent_runtime_state
            .as_ref()
            .is_none_or(|state| state.run_id != self.owner.fence.run_id)
        {
            return Err(conflict("Child clarification Run changed"));
        }
        let durable = self.validate()?;
        let parent_dir = if self.owner.parent_session_id == self.owner.root_session_id {
            self.sessions.join(&self.owner.root_session_id)
        } else {
            self.sessions
                .join(&self.owner.root_session_id)
                .join("children")
                .join(&self.owner.parent_session_id)
        };
        let (parent, side) = self.pair(&parent_dir)?;
        let parent = overlay_runtime_sidecar(parent, Some(side));
        if parent.id != self.owner.parent_session_id
            || parent.created_at != self.owner.parent_created_at
        {
            return Err(conflict("Child clarification parent birth changed"));
        }
        let mut normalized = candidate.clone();
        normalized
            .pending_question
            .as_mut()
            .expect("pending")
            .source = PendingQuestionSource::DirectParent;
        if let Some(raw) = durable.metadata.get(PARENT_QUESTION_REQUEST_KEY) {
            let request: ParentQuestion = serde_json::from_str(raw).map_err(conflict)?;
            let resolution = ParentQuestionResolution::from_child(&parent, &durable, &request.id);
            if ParentQuestion::for_pending(&parent, &durable).as_ref() != Some(&request)
                && resolution
                    .as_ref()
                    .is_none_or(|value| value.request != request)
            {
                return Err(conflict("canonical Child clarification proof changed"));
            }
            // Match the original paired occurrence, not merely a reused tool id
            // or copied question text. An already answered exact snapshot also
            // remains a valid replay of that same independently recorded fact.
            let original = ParentQuestion::issue_at(&parent, &normalized, request.issued_at);
            let answered = ParentQuestionResolution::from_child(&parent, &normalized, &request.id);
            if original.as_ref() != Some(&request)
                && !answered.as_ref().is_some_and(|value| {
                    value.request == request && resolution.as_ref() == Some(value)
                })
            {
                return Err(conflict("Child clarification occurrence changed"));
            }
            candidate.metadata.insert(
                "runtime.actor_parent_question_handoff".into(),
                "true".into(),
            );
            crate::session_merge::adopt_durable_actor_parent_question_handoff(candidate, &durable)?;
            candidate.metadata_version = candidate.metadata_version.max(durable.metadata_version);
            return Ok(());
        }
        if pending.source != PendingQuestionSource::AgenticClarification
            || candidate.metadata.contains_key(PARENT_QUESTION_REQUEST_KEY)
            || candidate
                .metadata
                .contains_key(PARENT_QUESTION_RESOLUTION_KEY)
            || durable
                .metadata
                .contains_key(PARENT_QUESTION_RESOLUTION_KEY)
            || durable.pending_question.as_ref().is_some_and(|previous| {
                serde_json::to_value(previous).ok() != serde_json::to_value(pending).ok()
            })
        {
            return Err(conflict(
                "new Child clarification is not an exact fresh pending ask",
            ));
        }
        let issued_at = Utc::now();
        let request = ParentQuestion::issue_at(&parent, &normalized, issued_at)
            .ok_or_else(|| conflict("Child clarification lacks its exact paired Tool result"))?;
        if normalized
            .messages
            .iter()
            .filter(|message| message.id == request.tool_result_message_id)
            .count()
            != 1
        {
            return Err(conflict("Child clarification Tool result is duplicated"));
        }
        if runtime_only {
            // A sidecar cannot publish a request whose paired history is still
            // only in memory. Full Agent clarification checkpoints take the
            // ordinary full-save branch and commit the pair with the request.
            let mut canonical_pair = normalized.clone();
            canonical_pair.messages.clone_from(&durable.messages);
            if ParentQuestion::issue_at(&parent, &canonical_pair, issued_at).as_ref()
                != Some(&request)
            {
                return Err(conflict(
                    "runtime-only Child clarification history is not canonical",
                ));
            }
        }
        normalized.metadata.insert(
            PARENT_QUESTION_REQUEST_KEY.into(),
            serde_json::to_string(&request).map_err(conflict)?,
        );
        normalized.metadata_version = normalized
            .metadata_version
            .max(durable.metadata_version)
            .checked_add(1)
            .ok_or_else(|| conflict("Child clarification metadata revision overflow"))?;
        normalized.updated_at = normalized.updated_at.max(issued_at);
        *candidate = normalized;
        Ok(())
    }

    fn validate_deadline(&self, deadline: DateTime<Utc>) -> io::Result<()> {
        self.validate()?;
        let bytes = actor_transcript::regular_bytes(&self.directory().join("actor-authority.json"))
            .map_err(conflict)?;
        let entry: ActorDirectoryEntry = actor_transcript::decode(&bytes).map_err(conflict)?;
        let now = Utc::now();
        let live =
            actor_directory::current_live(&entry, &self.owner.fence, now).map_err(conflict)?;
        if deadline <= now || deadline > live.lease_expires_at {
            return Err(conflict("Inbox lease exceeds Child activation"));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(super) struct ChildActorInputProof {
    child: ChildActorWriteProof,
    inbox: FileSessionInbox,
    directory: PathBuf,
    claim: SessionInboxOwnedClaim,
}
impl ChildActorInputProof {
    pub(super) fn validate(&self, terminal: bool) -> io::Result<()> {
        self.child.validate_deadline(self.claim.lease.expires_at)?;
        self.inbox
            .locked_owned_claim(
                &self.directory,
                &self.child.owner.fence.actor_id,
                &self.claim,
                Utc::now(),
                terminal,
            )
            .map(|_| ())
            .map_err(conflict)
    }
    pub(super) fn validate_transcript(&self, session: &Session) -> io::Result<()> {
        if !session
            .messages
            .iter()
            .any(|m| bamboo_domain::is_matching_session_message(m, &self.claim.claim.envelope))
            || !session.session_inbox_admission().is_some_and(|cursor| {
                cursor.contains(&self.claim.claim.envelope.id)
                    && cursor.last_admitted_sequence >= self.claim.claim.generation
            })
        {
            return Err(conflict(
                "Child input checkpoint lacks typed message and cursor",
            ));
        }
        Ok(())
    }
}

impl SessionStoreV2 {
    async fn child_runtime_guards(
        &self,
        owner: &ChildActorRuntimeWrite,
        kind: SaveKind,
    ) -> io::Result<Arc<DefaultWriterGuards>> {
        let proof = ChildActorWriteProof::new(self.sessions_dir.clone(), owner.clone())?;
        let lifecycle = self.lock_default_writer_lifecycle().await?;
        let task = self.lock_runtime_task_sidecar_shared().await?;
        let session = self
            .acquire_session_write_lock(&owner.fence.actor_id, kind)
            .await?;
        let mut guards = DefaultWriterGuards::shared(lifecycle, task, session);
        Arc::get_mut(&mut guards)
            .expect("fresh Child guards")
            .child_actor = Some(proof.clone());
        Self::default_writer_job(&guards, move || proof.validate().map(|_| ())).await?;
        Ok(guards)
    }

    pub(super) async fn publish_child_actor_runtime(
        &self,
        guards: &Arc<DefaultWriterGuards>,
        publish: RootActorRuntimePublisher,
    ) -> io::Result<()> {
        let proof = guards
            .child_actor
            .clone()
            .ok_or_else(|| conflict("Child publisher missing owner"))?;
        let input = guards.child_input.clone();
        Self::default_writer_job(guards, move || {
            let saved = proof.validate()?;
            if let Some(input) = input {
                input.validate(false)?;
                input.validate_transcript(&saved)?;
            }
            publish(&saved);
            Ok(())
        })
        .await
    }

    pub(super) async fn publish_child_actor_runtime_event_impl(
        &self,
        owner: &ChildActorRuntimeWrite,
        publish: RootActorRuntimeEventPublisher,
    ) -> io::Result<()> {
        let guards = self.child_runtime_guards(owner, SaveKind::Runtime).await?;
        guards.hold_tree(
            self.acquire_actor_tree_write_guard(&owner.root_session_id)
                .await?,
        );
        let proof = guards.child_actor.clone().expect("bound Child proof");
        Self::default_writer_job(&guards, move || {
            proof.validate()?;
            publish(&|| proof.validate().map(|_| ()))
        })
        .await
    }

    pub(super) fn bound_child_inbox(
        &self,
        owner: &ChildActorRuntimeWrite,
        inbox: &Arc<dyn SessionInboxPort>,
    ) -> io::Result<FileSessionInbox> {
        ChildActorWriteProof::new(self.sessions_dir.clone(), owner.clone())?;
        inbox
            .as_any()
            .and_then(|any| any.downcast_ref::<FileSessionInbox>())
            .ok_or_else(|| conflict("Child admission requires the concrete owned Inbox"))?
            .bind_child_owner(owner, &self.sessions_dir)
    }

    pub(crate) async fn child_owned_inbox_filesystem(
        &self,
        inbox: &FileSessionInbox,
        target: &str,
        owner: &ChildActorRuntimeWrite,
    ) -> io::Result<(PathBuf, OwnedFilesystem)> {
        if target != owner.fence.actor_id {
            return Err(conflict("Child Inbox target changed"));
        }
        let guards = self.child_runtime_guards(owner, SaveKind::Full).await?;
        guards.hold_tree(
            self.acquire_actor_tree_write_guard(&owner.root_session_id)
                .await?,
        );
        let proof = guards.child_actor.clone().expect("bound Child proof");
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

    pub(crate) fn child_inbox_lease_check(
        &self,
        owner: &ChildActorRuntimeWrite,
        deadline: DateTime<Utc>,
    ) -> Arc<dyn Fn() -> io::Result<()> + Send + Sync> {
        let proof = ChildActorWriteProof::new(self.sessions_dir.clone(), owner.clone());
        Arc::new(move || {
            proof
                .as_ref()
                .map_err(conflict)?
                .validate_deadline(deadline)
        })
    }

    pub(crate) fn child_inbox_terminal_check(
        &self,
        owner: &ChildActorRuntimeWrite,
        envelope: &SessionMessageEnvelope,
    ) -> Arc<dyn Fn() -> io::Result<()> + Send + Sync> {
        let proof = ChildActorWriteProof::new(self.sessions_dir.clone(), owner.clone());
        let envelope = envelope.clone();
        Arc::new(move || {
            let saved = proof.as_ref().map_err(conflict)?.validate()?;
            if saved
                .messages
                .iter()
                .any(|m| bamboo_domain::is_matching_session_message(m, &envelope))
            {
                Ok(())
            } else {
                Err(conflict(
                    "Child terminal Inbox receipt lacks canonical typed proof",
                ))
            }
        })
    }

    pub(super) async fn save_child_actor_runtime_impl(
        &self,
        request: ChildActorRuntimeSave<'_>,
    ) -> io::Result<()> {
        let ChildActorRuntimeSave {
            owner,
            session,
            runtime_only,
            inherited,
            finalized_wait,
            input,
            publish,
        } = request;
        if runtime_only && (input.is_some() || finalized_wait.is_some()) {
            return Err(conflict(
                "Child input/finalized wait requires a full checkpoint",
            ));
        }
        let started = Instant::now();
        let mut guards = self
            .child_runtime_guards(
                owner,
                if runtime_only {
                    SaveKind::Runtime
                } else {
                    SaveKind::Full
                },
            )
            .await?;
        let mut pending_ack = None;
        if let Some((inbox, claim)) = input {
            let inbox = self.bound_child_inbox(owner, &inbox)?;
            let proof = guards.child_actor.clone().expect("bound Child proof");
            let (directory, filesystem) = inbox
                .filesystem_with_authority(
                    &session.id,
                    InboxAuthority::Root {
                        _guard: guards.physical.clone(),
                    },
                )
                .await
                .map_err(conflict)?;
            let input = ChildActorInputProof {
                child: proof.clone(),
                inbox: inbox.clone(),
                directory: directory.clone(),
                claim: claim.clone(),
            };
            let ack = input.clone();
            let filesystem = filesystem.with_check(Arc::new(move || {
                let saved = proof.validate()?;
                ack.validate(true)?;
                ack.validate_transcript(&saved)
            }));
            let unique = Arc::get_mut(&mut guards).expect("fresh Child input guards");
            unique.child_input = Some(input);
            unique._input_filesystem = Some(filesystem.clone());
            pending_ack = Some((inbox, directory, claim, filesystem));
        }
        let mut candidate = if let Some(inherited) = inherited {
            self.reconcile_inherited_runtime_snapshot(session, inherited, runtime_only)
                .await?
        } else {
            session.clone()
        };
        if let Some(wait) = finalized_wait {
            let latest = self
                .load_session_unlocked(&session.id)
                .await?
                .ok_or_else(|| conflict("Child disappeared at finalization"))?;
            if candidate
                .agent_runtime_state
                .as_ref()
                .and_then(|s| s.waiting_for_children.as_ref())
                == Some(wait)
            {
                crate::session_merge::adopt_finalized_child_wait(&mut candidate, &latest, true);
            }
        }
        let proof = guards.child_actor.clone().expect("bound Child proof");
        candidate = Self::default_writer_job(&guards, move || {
            proof.normalize_parent_question(&mut candidate, runtime_only)?;
            Ok(candidate)
        })
        .await?;
        if runtime_only {
            let rel = Self::child_rel_path(&owner.root_session_id, &owner.fence.actor_id);
            self.save_runtime_state_after_lock(
                &candidate,
                &rel,
                started,
                &guards,
                Some(publish),
                None,
            )
            .await?;
        } else {
            self.save_session_after_lock(&candidate, started, &guards, None)
                .await?;
            self.publish_child_actor_runtime(&guards, publish).await?;
        }
        if let Some((inbox, directory, claim, filesystem)) = pending_ack {
            inbox
                .ack_owned_with_filesystem(&directory, &session.id, &claim, Utc::now(), filesystem)
                .await
                .map_err(conflict)?;
        }
        *session = candidate;
        Ok(())
    }
}
