//! Read-only transport expiry. This never creates a Root runtime write proof.
use super::*;
use bamboo_domain::{ActorDirectoryEntry, ScopedLoopControlBinding};
mod native;
use native::{scoped_native_transport_disposition, ScopedNativeTransportDisposition};

#[derive(Clone)]
pub(crate) struct ScopedTransportCensus {
    pub(crate) session: Session,
    directory: PathBuf,
    entry: Option<ActorDirectoryEntry>,
}

fn regular_optional(path: &Path) -> io::Result<Option<Vec<u8>>> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_file() => std::fs::read(path).map(Some),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "transport census witness is not regular",
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

impl ScopedTransportCensus {
    /// Caller holds lifecycle/Task/target canonical locks. No repair, index
    /// publication, Actor claim, or reconstruction from incomplete sidecars.
    pub(crate) fn read(directory: &Path) -> io::Result<Option<Self>> {
        let Some(main_bytes) = regular_optional(&directory.join("session.json"))? else {
            return Ok(None);
        };
        compact_main::validate_full_main(&main_bytes)?;
        let mut main: Session = serde_json::from_slice(&main_bytes)?;
        if main.kind != SessionKind::Root || !main.authority_identity.is_ordinary() {
            return Ok(None);
        }
        let side_bytes =
            regular_optional(&directory.join(RUNTIME_SIDECAR_FILE))?.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "transport census runtime sidecar missing",
                )
            })?;
        let mut side: Session = serde_json::from_slice(&side_bytes)?;
        for session in [&mut main, &mut side] {
            if session.root_session_id.is_empty() {
                session.root_session_id = session.id.clone();
            }
        }
        supervisor::validate_overlay(&main, Some(&side))?;
        if main.id != side.id
            || main.created_at != side.created_at
            || main.kind != side.kind
            || main.parent_session_id.is_some()
            || side.parent_session_id.is_some()
            || main.spawn_depth != 0
            || side.spawn_depth != 0
            || !side.authority_identity.is_ordinary()
            || main.root_session_id != main.id
            || side.root_session_id != main.id
            || main.project_id_meta() != side.project_id_meta()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "transport census Root identity changed",
            ));
        }
        let record = regular_optional(&directory.join("actor-authority.json"))?;
        let marker = regular_optional(&directory.join("actor-authority.initialized.json"))?;
        let entry = match (record, marker) {
            (None, None) => None,
            (Some(record), Some(marker)) => {
                let project = side
                    .project_id_meta()
                    .map(ProjectId::parse)
                    .transpose()
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                actor_directory::validate_census_witnesses(
                    &record,
                    &marker,
                    &main,
                    project.as_ref(),
                )?;
                actor_directory::validate_census_witnesses(
                    &record,
                    &marker,
                    &side,
                    project.as_ref(),
                )?;
                Some(serde_json::from_slice::<ActorDirectoryEntry>(&record)?)
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "transport census witness pair incomplete",
                ))
            }
        };
        Ok(Some(Self {
            session: overlay_runtime_sidecar(main, Some(side)),
            entry,
            directory: directory.to_owned(),
        }))
    }

    pub(crate) fn native_inert(&self, binding: &ScopedLoopControlBinding) -> io::Result<bool> {
        let Some(entry) = &self.entry else {
            return Ok(false);
        };
        Ok(matches!(
            scoped_native_transport_disposition(binding, &self.session, entry, Utc::now())?,
            ScopedNativeTransportDisposition::Inert(_)
        ))
    }

    /// Existing logical grant cannot outlive an independently expired native
    /// execution. Legacy absence preserves its original live-router contract.
    pub(super) fn require_live_binding(
        &self,
        binding: &ScopedLoopControlBinding,
    ) -> io::Result<()> {
        if self.session.id != binding.target_session_id
            || self.session.created_at != binding.target_created_at
            || self
                .session
                .agent_runtime_state
                .as_ref()
                .and_then(|state| state.execution_id.as_deref())
                != Some(binding.execution_id.as_str())
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "Loop canonical execution is not live",
            ));
        }
        if let Some(entry) = &self.entry {
            scoped_native_transport_disposition(binding, &self.session, entry, Utc::now())?;
            let live = entry.activation.as_ref().is_some_and(|activation| {
                activation.status == bamboo_domain::ActorActivationStatus::Running
                    && activation.lease_expires_at > Utc::now()
                    && activation.run_id == binding.owner_run_id
            });
            if !live
                || self.session.created_at != binding.target_created_at
                || self
                    .session
                    .agent_runtime_state
                    .as_ref()
                    .and_then(|state| state.execution_id.as_deref())
                    != Some(binding.execution_id.as_str())
            {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "native Loop owner is not currently live",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn check_current_live(&self, binding: &ScopedLoopControlBinding) -> io::Result<()> {
        Self::read(&self.directory)?
            .ok_or_else(|| io::Error::other("Loop current census disappeared"))?
            .require_live_binding(binding)
    }

    pub(crate) fn wrap_live_guard(
        &self,
        binding: &ScopedLoopControlBinding,
        inner: Arc<dyn bamboo_domain::SessionRunDeliveryGuard>,
    ) -> io::Result<Arc<dyn bamboo_domain::SessionRunDeliveryGuard>> {
        self.check_current_live(binding)?;
        Ok(Arc::new(ScopedNativeDeliveryGuard {
            census: self.clone(),
            binding: binding.clone(),
            inner,
        }))
    }

    pub(crate) fn require_same_snapshot(&self) -> io::Result<Self> {
        let current = Self::read(&self.directory)?
            .ok_or_else(|| io::Error::other("transport census disappeared"))?;
        let projection = |session: &Session| {
            serde_json::to_value((
                &session.id,
                session.created_at,
                session
                    .agent_runtime_state
                    .as_ref()
                    .and_then(|state| state.execution_id.as_deref()),
                session
                    .metadata
                    .get(bamboo_domain::SCOPED_LOOP_CONSUMPTION_KEY),
                session.session_inbox_admission(),
            ))
        };
        if projection(&current.session)? != projection(&self.session)? {
            return Err(io::Error::other(
                "transport canonical checkpoint changed before disposal",
            ));
        }
        Ok(current)
    }
}

struct ScopedNativeDeliveryGuard {
    census: ScopedTransportCensus,
    binding: ScopedLoopControlBinding,
    inner: Arc<dyn bamboo_domain::SessionRunDeliveryGuard>,
}
impl bamboo_domain::SessionRunDeliveryGuard for ScopedNativeDeliveryGuard {
    fn matches(&self, target: &str, birth: DateTime<Utc>, run: &str) -> bool {
        self.census.check_current_live(&self.binding).is_ok()
            && self.inner.matches(target, birth, run)
    }
    fn notify_committed_delivery(&self, generation: u64) -> bool {
        self.census.check_current_live(&self.binding).is_ok()
            && self.inner.notify_committed_delivery(generation)
    }
}

impl SessionStoreV2 {
    /// Upgrade the already-held lifecycle guard exactly once. Reacquiring it
    /// while a lifecycle writer waits would deadlock Tokio's fair RwLock.
    pub(crate) async fn scoped_transport_delivery_guards(
        &self,
        lifecycle: SessionLifecycleReadGuard,
        target: &str,
    ) -> io::Result<Arc<DefaultWriterPhysicalGuards>> {
        let task = self.lock_runtime_task_sidecar_shared().await?;
        let session = self
            .acquire_session_write_lock(target, SaveKind::Runtime)
            .await?;
        Ok(DefaultWriterGuards::shared(lifecycle, task, session)
            .physical
            .clone())
    }

    pub(crate) fn scoped_transport_census_locked(
        &self,
        target: &str,
    ) -> io::Result<Option<ScopedTransportCensus>> {
        validate_session_id(target)?;
        ScopedTransportCensus::read(&self.sessions_dir.join(target))
    }
}
