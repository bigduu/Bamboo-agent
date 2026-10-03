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
        if let Some(proof) = guards.root_actor.as_ref() {
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
}
