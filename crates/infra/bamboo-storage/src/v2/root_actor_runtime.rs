//! Ordinary Root runtime writes reuse V2's canonical publication protocol.
//! The capability belongs to one execution, never the default snapshot writer.

use super::*;
use bamboo_domain::storage::{RootActorRuntimePublisher, RootActorRuntimeWrite};
use bamboo_domain::{ActorActivationStatus, ActorDirectoryEntry, SessionAuthorityConflict};

pub(super) fn conflict(reason: impl std::fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        SessionAuthorityConflict(format!("Root Actor runtime authority rejected: {reason}")),
    )
}

#[derive(Clone)]
pub(super) struct RootActorWriteProof {
    directory: PathBuf,
    owner: RootActorRuntimeWrite,
}

impl RootActorWriteProof {
    pub(super) fn new(directory: PathBuf, owner: RootActorRuntimeWrite) -> Self {
        Self { directory, owner }
    }

    pub(super) fn owner(&self) -> &RootActorRuntimeWrite {
        &self.owner
    }

    // Pure reads at the final boundary; never ensure/repair/claim authority.
    pub(super) fn validate(&self) -> io::Result<Session> {
        let id = &self.owner.fence.actor_id;
        let regular = |name: &str| {
            actor_transcript::regular_bytes(&self.directory.join(name)).map_err(conflict)
        };
        let main_bytes = regular("session.json")?;
        let side_bytes = regular(RUNTIME_SIDECAR_FILE)?;
        compact_main::validate_full_main(&main_bytes).map_err(conflict)?;
        let mut main: Session = actor_transcript::decode(&main_bytes).map_err(conflict)?;
        let mut side: Session = actor_transcript::decode(&side_bytes).map_err(conflict)?;
        supervisor::validate_overlay(&main, Some(&side)).map_err(conflict)?;
        main.check_conversation_generation(&side)
            .map_err(conflict)?;
        self.owner
            .conversation_generation
            .check_canonical(&side)
            .map_err(conflict)?;
        // Normalize the ordinary Root serializer's legacy empty root id. This
        // port retains the existing full/runtime validators and does not import
        // the bounded transcript append port's separate metadata/payload limits.
        for session in [&mut main, &mut side] {
            if session.root_session_id.is_empty() {
                session.root_session_id = id.clone();
            }
            if session.id != *id
                || session.kind != SessionKind::Root
                || !session.authority_identity.is_ordinary()
            {
                return Err(conflict("canonical Root identity changed"));
            }
        }
        let record = regular("actor-authority.json")?;
        let marker = regular("actor-authority.initialized.json")?;
        let project = side
            .project_id_meta()
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
        if activation.status != ActorActivationStatus::Running
            || main.created_at != self.owner.created_at
            || side.created_at != main.created_at
            || main.project_id_meta() != side.project_id_meta()
        {
            return Err(conflict("execution birth or running owner changed"));
        }
        Ok(overlay_runtime_sidecar(main, Some(side)))
    }

    pub(super) fn validate_candidate(&self, incoming: &Session) -> io::Result<()> {
        let durable = self.validate()?;
        incoming
            .check_conversation_generation(&durable)
            .map_err(conflict)?;
        if incoming.id != durable.id
            || incoming.kind != SessionKind::Root
            || incoming.created_at != durable.created_at
            || incoming.parent_session_id != durable.parent_session_id
            || incoming.spawn_depth != 0
            || (!incoming.root_session_id.is_empty() && incoming.root_session_id != durable.id)
            || incoming.authority_identity != durable.authority_identity
            || incoming.project_id_meta() != durable.project_id_meta()
        {
            return Err(conflict("candidate Root identity or Project changed"));
        }
        Ok(())
    }

    pub(super) fn validate_deadline(&self, deadline: DateTime<Utc>) -> io::Result<()> {
        self.validate()?;
        let bytes = actor_transcript::regular_bytes(&self.directory.join("actor-authority.json"))
            .map_err(conflict)?;
        let entry: ActorDirectoryEntry = actor_transcript::decode(&bytes).map_err(conflict)?;
        let now = Utc::now();
        let current =
            actor_directory::current_live(&entry, &self.owner.fence, now).map_err(conflict)?;
        if deadline <= now || deadline > current.lease_expires_at {
            return Err(conflict("Inbox lease exceeds current Root lease"));
        }
        Ok(())
    }
}

impl SessionStoreV2 {
    pub(super) async fn publish_root_actor_runtime_event_impl(
        &self,
        owner: &RootActorRuntimeWrite,
        publish: bamboo_domain::storage::RootActorRuntimeEventPublisher,
    ) -> io::Result<()> {
        validate_session_id(&owner.fence.actor_id)?;
        let lifecycle = self.lock_default_writer_lifecycle().await?;
        let task = self.lock_runtime_task_sidecar_shared().await?;
        let session = self
            .acquire_session_write_lock(&owner.fence.actor_id, SaveKind::Runtime)
            .await?;
        let proof =
            RootActorWriteProof::new(self.sessions_dir.join(&owner.fence.actor_id), owner.clone());
        let guards = DefaultWriterGuards::shared_with_root_actor(
            lifecycle,
            task,
            session,
            Some(proof.clone()),
        );
        Self::default_writer_job(&guards, move || {
            proof.validate()?;
            publish(&|| proof.validate().map(|_| ()))
        })
        .await
    }

    pub(super) async fn check_default_or_root_actor_context(
        &self,
        incoming: &Session,
        directory: &Path,
        full: bool,
        guards: &Arc<DefaultWriterGuards>,
    ) -> io::Result<()> {
        if let Some(proof) = guards.root_mode_context.as_ref() {
            let incoming = incoming.clone();
            let proof = proof.clone();
            Self::default_writer_job(guards, move || proof.validate_candidate(&incoming, full))
                .await
        } else if let Some(proof) = guards.root_actor.as_ref() {
            let incoming = incoming.clone();
            let proof = proof.clone();
            let input = guards.input.clone();
            Self::default_writer_job(guards, move || {
                proof.validate_candidate(&incoming)?;
                if let Some(input) = input {
                    input.validate(false)?;
                    input.validate_transcript(&incoming)?;
                }
                Ok(())
            })
            .await
        } else if let Some(proof) = guards.child_actor.as_ref() {
            let incoming = incoming.clone();
            let proof = proof.clone();
            let input = guards.child_input.clone();
            Self::default_writer_job(guards, move || {
                proof.validate_candidate(&incoming)?;
                if let Some(input) = input {
                    input.validate(false)?;
                    input.validate_transcript(&incoming)?;
                }
                Ok(())
            })
            .await
        } else if let Some(proof) = guards.parent_question_answer.as_ref() {
            let incoming = incoming.clone();
            let proof = proof.clone();
            Self::default_writer_job(guards, move || proof.validate_candidate(&incoming, full))
                .await
        } else {
            self.check_default_actor_context(incoming, directory, full)
                .await
        }
    }

    pub(super) async fn publish_root_actor_runtime(
        &self,
        guards: &Arc<DefaultWriterGuards>,
        publish: RootActorRuntimePublisher,
    ) -> io::Result<()> {
        if guards.child_actor.is_some() {
            return self.publish_child_actor_runtime(guards, publish).await;
        }
        let proof = guards
            .root_actor
            .clone()
            .ok_or_else(|| conflict("cache publication has no bound owner"))?;
        let input = guards.input.clone();
        Self::default_writer_job(guards, move || {
            let committed = proof.validate()?;
            if let Some(input) = input {
                input.validate(false)?;
                input.validate_transcript(&committed)?;
            }
            publish(&committed);
            Ok(())
        })
        .await
    }

    pub(super) async fn save_root_actor_bound_startup_impl(
        &self,
        owner: &RootActorRuntimeWrite,
        session: &mut Session,
        expected: &Session,
        inherited: Option<&bamboo_domain::InheritedChildWait>,
        publish: RootActorRuntimePublisher,
    ) -> io::Result<()> {
        let total_started = Instant::now();
        validate_session_id(&session.id)?;
        if let Some(wait) = inherited {
            wait.validate_session(session)?;
        }
        let lifecycle = self.lock_default_writer_lifecycle().await?;
        let task = self.lock_runtime_task_sidecar_shared().await?;
        let session_write = self
            .acquire_session_write_lock(&session.id, SaveKind::Full)
            .await?;
        let proof = RootActorWriteProof::new(self.sessions_dir.join(&session.id), owner.clone());
        let guards = DefaultWriterGuards::shared_with_root_actor(
            lifecycle,
            task,
            session_write,
            Some(proof.clone()),
        );
        let observed = expected.clone();
        let proposed = session.clone();
        Self::default_writer_job(&guards, move || {
            let current = proof.validate()?;
            observed
                .check_conversation_generation(&current)
                .map_err(conflict)?;
            let schedule = observed.metadata.get("created_by_schedule_id");
            let connect = observed.metadata.get("created_by_connect_key");
            let pending_result = |session: &Session| -> io::Result<Option<serde_json::Value>> {
                let Some(question) = observed.pending_question.as_ref() else {
                    return Ok(None);
                };
                session
                    .messages
                    .iter()
                    .rev()
                    .find(|message| {
                        message.tool_call_id.as_deref() == Some(question.tool_call_id.as_str())
                    })
                    .map(serde_json::to_value)
                    .transpose()
                    .map_err(conflict)
            };
            if observed.pending_question != current.pending_question
                || pending_result(&observed)? != pending_result(&current)?
                || observed
                    .metadata
                    .get(bamboo_domain::CONSUMED_CLARIFICATION_IDS_KEY)
                    != current
                        .metadata
                        .get(bamboo_domain::CONSUMED_CLARIFICATION_IDS_KEY)
                || observed
                    .metadata
                    .get(bamboo_domain::CONSUMED_RESPONSE_OCCURRENCES_KEY)
                    != current
                        .metadata
                        .get(bamboo_domain::CONSUMED_RESPONSE_OCCURRENCES_KEY)
                || schedule != current.metadata.get("created_by_schedule_id")
                || schedule != proposed.metadata.get("created_by_schedule_id")
                || connect != current.metadata.get("created_by_connect_key")
                || connect != proposed.metadata.get("created_by_connect_key")
                || observed.id != current.id
                || observed.created_at != current.created_at
                || observed.kind != current.kind
                || observed.authority_identity != current.authority_identity
                || observed.project_id_meta() != current.project_id_meta()
                || observed.model != current.model
                || observed.model_ref != current.model_ref
                || observed.provider_name() != current.provider_name()
                || observed.reasoning_effort != current.reasoning_effort
                || observed.metadata_version != current.metadata_version
            {
                return Err(conflict("bound startup execution profile changed"));
            }
            proof.validate_candidate(&proposed)
        })
        .await?;
        let reconciled = if let Some(wait) = inherited {
            self.reconcile_inherited_runtime_snapshot(session, wait, false)
                .await?
        } else {
            session.clone()
        };
        self.save_session_after_lock(&reconciled, total_started, &guards, None)
            .await?;
        self.publish_root_actor_runtime(&guards, publish).await?;
        *session = reconciled;
        Ok(())
    }

    pub(super) async fn save_root_actor_runtime_impl(
        &self,
        owner: &RootActorRuntimeWrite,
        session: &Session,
        runtime_only: bool,
        publish: RootActorRuntimePublisher,
    ) -> io::Result<()> {
        if runtime_only {
            return self
                .save_runtime_state_with_owner(session, Some(owner), Some(publish))
                .await;
        }
        let total_started = Instant::now();
        validate_session_id(&session.id)?;
        let lifecycle = self.lock_default_writer_lifecycle().await?;
        let task = self.lock_runtime_task_sidecar_shared().await?;
        let session_write = self
            .acquire_session_write_lock(&session.id, SaveKind::Full)
            .await?;
        let proof = RootActorWriteProof::new(self.sessions_dir.join(&session.id), owner.clone());
        let guards = DefaultWriterGuards::shared_with_root_actor(
            lifecycle,
            task,
            session_write,
            Some(proof),
        );
        self.save_session_after_lock(session, total_started, &guards, None)
            .await?;
        self.publish_root_actor_runtime(&guards, publish).await
    }

    pub(super) async fn save_finalized_root_actor_runtime_impl(
        &self,
        owner: &RootActorRuntimeWrite,
        session: &mut Session,
        inherited: Option<&bamboo_domain::InheritedChildWait>,
        publish: RootActorRuntimePublisher,
    ) -> io::Result<()> {
        let started = Instant::now();
        validate_session_id(&session.id)?;
        if let Some(inherited) = inherited {
            inherited.validate_session(session)?;
        }
        let lifecycle = self.lock_default_writer_lifecycle().await?;
        let task = self.lock_runtime_task_sidecar_shared().await?;
        let session_write = self
            .acquire_session_write_lock(&session.id, SaveKind::Full)
            .await?;
        let proof = RootActorWriteProof::new(self.sessions_dir.join(&session.id), owner.clone());
        let mut guards = DefaultWriterGuards::shared_with_root_actor(
            lifecycle,
            task,
            session_write,
            Some(proof.clone()),
        );
        Arc::get_mut(&mut guards)
            .expect("new Root final writer has no borrowers")
            .root_outcome_finalized = true;
        let mut reconciled = match inherited {
            Some(inherited) => {
                self.reconcile_inherited_runtime_snapshot(session, inherited, false)
                    .await?
            }
            None => session.clone(),
        };
        // Another Store may complete a tagged Child after the outer merge's
        // read. Reconcile again while this physical Root lock is retained.
        let latest = self
            .load_session_unlocked(&session.id)
            .await?
            .filter(|latest| latest.created_at == session.created_at)
            .ok_or_else(|| conflict("Root finalization source changed"))?;
        if let Some(registered_at) =
            crate::session_merge::adopt_finalized_child_wait(&mut reconciled, &latest, false)
        {
            crate::session_merge::preserve_finalized_hidden_child_resumes(
                &mut reconciled,
                &latest,
                registered_at,
            );
        }
        if let Some(registered_at) =
            crate::session_merge::adopt_durable_tagged_child_wait(&mut reconciled, &latest)
        {
            crate::session_merge::preserve_finalized_hidden_child_resumes(
                &mut reconciled,
                &latest,
                registered_at,
            );
        }
        self.save_session_after_lock(&reconciled, started, &guards, None)
            .await?;
        let committed = Self::default_writer_job(&guards, move || proof.validate()).await?;
        self.publish_root_actor_runtime(&guards, publish).await?;
        // Same #1053 constraint as session_merge::adopt_root_publication: the
        // persisted proof copy never carries `resolved_token_budget` or
        // `force_manual_compression`, so adopting it would erase the live
        // round's values. `observed_runtime_session_version` may be re-stamped
        // by the sidecar overlay; keeping the caller's observation is still
        // correct because this path already verified the finalized source.
        let mut adopted = committed;
        adopted.resolved_token_budget = session.resolved_token_budget.clone();
        adopted.observed_runtime_session_version = session.observed_runtime_session_version;
        adopted.force_manual_compression = session.force_manual_compression.clone();
        *session = adopted;
        Ok(())
    }
}
