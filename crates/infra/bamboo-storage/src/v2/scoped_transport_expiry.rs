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
        reject_conversation_generation_change(&main, &side)?;
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
        let current = Self::read(&self.directory)?
            .ok_or_else(|| io::Error::other("Loop current census disappeared"))?;
        reject_conversation_generation_change(&self.session, &current.session)?;
        current.require_live_binding(binding)
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
        reject_conversation_generation_change(&self.session, &current.session)?;
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

#[cfg(test)]
pub(super) mod generation_tests {
    use super::*;
    use bamboo_domain::{AgentRuntimeState, LoopControlAction, SupervisorReference};
    use std::collections::BTreeSet;

    pub(in crate::v2) fn next_conversation(source: &Session) -> Session {
        let mut current = source.clone();
        current.conversation_reset = Some(bamboo_domain::ConversationResetState {
            schema_version: 1,
            conversation_revision: 1,
            receipts: vec![bamboo_domain::ConversationResetReceipt {
                operation_id: format!("0:{}", Uuid::new_v4()),
                birth_token: source.root_mode_birth_token(),
                expected_snapshot_sha256: "a".repeat(64),
                previous_revision: 0,
                conversation_revision: 1,
                retained_system_messages: 0,
                reset_at: source.updated_at,
            }],
        });
        current
    }

    pub(in crate::v2) fn source() -> Session {
        let mut session = Session::new("scoped-generation-target", "model");
        session.set_project_id_meta("project-scoped-generation");
        session.agent_runtime_state = Some(AgentRuntimeState {
            execution_id: Some(Uuid::new_v4().to_string()),
            ..Default::default()
        });
        session
    }

    /// Selector data only; no live Supervisor grant or permission is installed.
    pub(in crate::v2) fn binding(session: &Session) -> ScopedLoopControlBinding {
        let binding = ScopedLoopControlBinding {
            grant_id: Uuid::new_v4(),
            supervisor: SupervisorReference {
                session_id: DEFAULT_SUPERVISOR_SESSION_ID.into(),
                incarnation_id: Uuid::new_v4(),
            },
            supervisor_tool_authority_revision: 1,
            target_session_id: session.id.clone(),
            target_created_at: session.created_at,
            target_metadata_version: session.metadata_version,
            target_project_id: session.project_id_meta().unwrap().parse().unwrap(),
            link_revision: 1,
            owner_run_id: "scoped-generation-run".into(),
            execution_id: session
                .agent_runtime_state
                .as_ref()
                .unwrap()
                .execution_id
                .clone()
                .unwrap(),
            allowed_actions: BTreeSet::from([LoopControlAction::Start]),
            max_iterations: 2,
            max_total_rounds: 4,
        };
        binding.validate().unwrap();
        binding
    }

    fn pair(directory: &Path, main: &Session, side: &Session) {
        std::fs::write(
            directory.join("session.json"),
            compact_main::serialize_main(main).unwrap(),
        )
        .unwrap();
        std::fs::write(
            directory.join(RUNTIME_SIDECAR_FILE),
            serde_json::to_vec(&runtime_sidecar_snapshot(side)).unwrap(),
        )
        .unwrap();
    }

    fn conflict(error: io::Error) {
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        assert!(error
            .get_ref()
            .is_some_and(|cause| cause.is::<bamboo_domain::SessionAuthorityConflict>()));
    }

    #[test]
    fn census_pair_rejects_different_or_invalid_conversation_before_overlay() {
        let home = tempfile::tempdir().unwrap();
        let original = source();
        let current = next_conversation(&original);
        pair(home.path(), &original, &current);
        conflict(
            ScopedTransportCensus::read(home.path())
                .err()
                .expect("mixed pair"),
        );
        let mut invalid = original.clone();
        invalid.conversation_reset = Some(Default::default());
        pair(home.path(), &original, &invalid);
        conflict(
            ScopedTransportCensus::read(home.path())
                .err()
                .expect("invalid pair"),
        );
        pair(home.path(), &current, &current);
        assert!(ScopedTransportCensus::read(home.path()).unwrap().is_some());
    }

    #[test]
    fn retained_census_never_refreshes_original_generation_from_current_pair() {
        for same_revision in [false, true] {
            let home = tempfile::tempdir().unwrap();
            let zero = source();
            let original = if same_revision {
                next_conversation(&zero)
            } else {
                zero.clone()
            };
            let selector = binding(&original);
            pair(home.path(), &original, &original);
            let retained = ScopedTransportCensus::read(home.path()).unwrap().unwrap();
            let mut current = if same_revision {
                original.clone()
            } else {
                next_conversation(&zero)
            };
            if same_revision {
                current.conversation_reset.as_mut().unwrap().receipts[0].expected_snapshot_sha256 =
                    "b".repeat(64);
            }
            pair(home.path(), &current, &current);
            conflict(retained.check_current_live(&selector).unwrap_err());
            conflict(
                retained
                    .require_same_snapshot()
                    .err()
                    .expect("old snapshot"),
            );
            assert_eq!(
                retained.session.conversation_reset,
                original.conversation_reset
            );
        }
    }

    #[derive(Default)]
    struct DeliveryProbe {
        matches: AtomicUsize,
        notifications: AtomicUsize,
    }
    impl bamboo_domain::SessionRunDeliveryGuard for DeliveryProbe {
        fn matches(&self, _: &str, _: DateTime<Utc>, _: &str) -> bool {
            self.matches.fetch_add(1, Ordering::SeqCst);
            true
        }
        fn notify_committed_delivery(&self, _: u64) -> bool {
            self.notifications.fetch_add(1, Ordering::SeqCst);
            true
        }
    }

    #[test]
    fn retained_delivery_does_not_delegate_or_notify_after_conversation_change() {
        let home = tempfile::tempdir().unwrap();
        let original = source();
        let selector = binding(&original);
        pair(home.path(), &original, &original);
        let retained = ScopedTransportCensus::read(home.path()).unwrap().unwrap();
        let probe = Arc::new(DeliveryProbe::default());
        let guard = retained.wrap_live_guard(&selector, probe.clone()).unwrap();
        let current = next_conversation(&original);
        pair(home.path(), &current, &current);
        assert!(!guard.matches(
            &selector.target_session_id,
            selector.target_created_at,
            &selector.owner_run_id
        ));
        assert!(!guard.notify_committed_delivery(1));
        assert_eq!(probe.matches.load(Ordering::SeqCst), 0);
        assert_eq!(probe.notifications.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn current_generation_keeps_unrelated_append_and_existing_delivery_contract() {
        let home = tempfile::tempdir().unwrap();
        let original = next_conversation(&source());
        let selector = binding(&original);
        pair(home.path(), &original, &original);
        let retained = ScopedTransportCensus::read(home.path()).unwrap().unwrap();
        let mut current = original.clone();
        current.add_message(Message::user("same conversation append"));
        current.pinned = true;
        pair(home.path(), &current, &current);
        retained.check_current_live(&selector).unwrap();
        assert_eq!(
            retained
                .require_same_snapshot()
                .unwrap()
                .session
                .messages
                .len(),
            1
        );
        let probe = Arc::new(DeliveryProbe::default());
        let guard = retained.wrap_live_guard(&selector, probe.clone()).unwrap();
        assert!(guard.matches(
            &selector.target_session_id,
            selector.target_created_at,
            &selector.owner_run_id
        ));
        assert!(guard.notify_committed_delivery(1));
        assert_eq!(probe.matches.load(Ordering::SeqCst), 1);
        assert_eq!(probe.notifications.load(Ordering::SeqCst), 1);
    }
}
