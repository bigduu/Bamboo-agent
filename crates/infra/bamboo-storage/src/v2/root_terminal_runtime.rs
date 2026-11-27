//! Root logical terminal repair. No lease or cache observation proves Worker stop.
use super::*;
use bamboo_domain::{
    is_root_compaction_lease_owner, root_planned_update_predecessor_sha256, AgentRuntimeState,
    AgentStatusState, LlmRequestDiagnostic, RootActorRuntimeWrite, SessionKind,
    SupervisedRootOutcomeResult,
};
use sha2::{Digest, Sha256};

pub(super) const ROOT_OBSERVATION: &str = "runtime.root_writer_observation_v1";
const PLANNED_UPDATE_COMPLETED: &str = "runtime.planned_update_completed_v1";
#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RootObservation {
    schema_version: u32,
    created_at: DateTime<Utc>,
    fence_sha256: String,
    conversation_reset_sha256: String,
    project_sha256: String,
    metadata_version: u64,
}
fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "invalid Root runtime observation",
    )
}
fn observation(session: &Session, fence: &ActorActivationFence) -> io::Result<RootObservation> {
    if session.id != fence.actor_id {
        return Err(invalid());
    }
    observation_for_fence_sha256(
        session,
        format!("{:x}", Sha256::digest(serde_json::to_vec(fence)?)),
    )
}
fn observation_for_fence_sha256(
    session: &Session,
    fence_sha256: String,
) -> io::Result<RootObservation> {
    if session.kind != SessionKind::Root || !session.authority_identity.is_ordinary() {
        return Err(invalid());
    }
    Ok(RootObservation {
        schema_version: 1,
        created_at: session.created_at,
        project_sha256: format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&session.project_id_meta())?)
        ),
        metadata_version: session.metadata_version,
        fence_sha256,
        conversation_reset_sha256: session
            .conversation_reset_sha256()
            .map_err(|_| invalid())?
            .unwrap_or_else(|| format!("{:x}", Sha256::digest(b"bamboo.root-runtime.no-reset.v1"))),
    })
}
fn matches_observation(session: &Session, fence: &ActorActivationFence) -> io::Result<bool> {
    let Some(recorded) = read_observation(session)? else {
        return Ok(false);
    };
    Ok(compatible_observation(
        &recorded,
        &observation(session, fence)?,
    ))
}
fn read_observation(session: &Session) -> io::Result<Option<RootObservation>> {
    let Some(raw) = session.metadata.get(ROOT_OBSERVATION) else {
        return Ok(None);
    };
    if raw.len() > 2048 {
        return Err(invalid());
    }
    serde_json::from_str(raw).map(Some).map_err(|_| invalid())
}
fn parked_update_predecessor_sha256<'a>(
    session: &Session,
    activation: &'a ActorActivation,
) -> io::Result<Option<&'a str>> {
    if activation.checkpoint_revision != 0
        || session.last_run_status().as_deref() != Some("suspended")
        || !session.is_parked_for_planned_update()
    {
        return Ok(None);
    }
    let Some(digest) = root_planned_update_predecessor_sha256(&activation.lease_owner) else {
        return Ok(None);
    };
    let Some(recorded) = read_observation(session)? else {
        return Ok(None);
    };
    Ok(compatible_observation(
        &recorded,
        &observation_for_fence_sha256(session, digest.to_owned())?,
    )
    .then_some(digest))
}
fn compatible_observation(recorded: &RootObservation, current: &RootObservation) -> bool {
    recorded.schema_version == 1
        && recorded.created_at == current.created_at
        && recorded.fence_sha256 == current.fence_sha256
        && recorded.conversation_reset_sha256 == current.conversation_reset_sha256
        && recorded.project_sha256 == current.project_sha256
        && current
            .metadata_version
            .checked_sub(recorded.metadata_version)
            .is_some_and(|delta| delta <= 1)
}
pub(super) fn stamp_owned_observation(
    session: &mut Session,
    owner: &RootActorRuntimeWrite,
    canonical: &Session,
) -> io::Result<()> {
    owner
        .conversation_generation
        .check_canonical(canonical)
        .map_err(|_| invalid())?;
    session
        .check_conversation_generation(canonical)
        .map_err(|_| invalid())?;
    if owner.created_at != canonical.created_at
        || session.created_at != canonical.created_at
        || session.id != canonical.id
        || session.project_id_meta() != canonical.project_id_meta()
    {
        return Err(invalid());
    }
    let current = observation(canonical, &owner.fence)?;
    if let Some(raw) = canonical.metadata.get(ROOT_OBSERVATION) {
        if raw.len() > 2048 {
            return Err(invalid());
        }
        let recorded: RootObservation = serde_json::from_str(raw).map_err(|_| invalid())?;
        if recorded.schema_version != 1
            || (recorded.fence_sha256 == current.fence_sha256
                && !compatible_observation(&recorded, &current))
        {
            return Err(invalid());
        }
    }
    session
        .metadata
        .insert(ROOT_OBSERVATION.into(), serde_json::to_string(&current)?);
    session
        .metadata
        .remove("runtime.reconciled_activation_fence");
    preserve_completed_update(session, Some(canonical));
    Ok(())
}
fn preserve_completed_update(incoming: &mut Session, canonical: Option<&Session>) {
    incoming.metadata.remove(PLANNED_UPDATE_COMPLETED);
    if let Some(digest) = canonical.and_then(|s| s.metadata.get(PLANNED_UPDATE_COMPLETED)) {
        incoming
            .metadata
            .insert(PLANNED_UPDATE_COMPLETED.into(), digest.clone());
    }
}
pub(super) fn preserve_observation(incoming: &mut Session, canonical: Option<&Session>) {
    incoming.metadata.remove(ROOT_OBSERVATION);
    if let Some(raw) = canonical.and_then(|s| s.metadata.get(ROOT_OBSERVATION)) {
        incoming
            .metadata
            .insert(ROOT_OBSERVATION.into(), raw.clone());
    }
    preserve_completed_update(incoming, canonical);
}
pub(super) fn validate_projection_identity(
    entry: &ActorDirectoryEntry,
    session: &Session,
) -> Result<(), ActorDirectoryError> {
    let delta = session
        .metadata_version
        .checked_sub(entry.actor.observed_metadata_version);
    if session.kind != SessionKind::Root
        || !session.authority_identity.is_ordinary()
        || !entry.actor.matches_session(session)
        || entry.actor.project_id.as_deref() != session.project_id_meta().as_deref()
        || !delta.is_some_and(|delta| delta <= 1)
    {
        return Err(ActorDirectoryError::InvalidIdentity);
    }
    Ok(())
}
fn terminal_finish(status: ActorActivationStatus) -> Option<ActorActivationFinish> {
    match status {
        ActorActivationStatus::Succeeded => Some(ActorActivationFinish::Succeeded),
        ActorActivationStatus::Cancelled => Some(ActorActivationFinish::Cancelled),
        ActorActivationStatus::Failed => Some(ActorActivationFinish::Failed),
        _ => None,
    }
}
fn finish_status(finish: ActorActivationFinish) -> ActorActivationStatus {
    match finish {
        ActorActivationFinish::Succeeded => ActorActivationStatus::Succeeded,
        ActorActivationFinish::Cancelled => ActorActivationStatus::Cancelled,
        ActorActivationFinish::Failed => ActorActivationStatus::Failed,
    }
}
impl SessionStoreV2 {
    pub(in crate::v2) fn same_root_runtime_observation(left: &Session, right: &Session) -> bool {
        left.metadata.get(ROOT_OBSERVATION) == right.metadata.get(ROOT_OBSERVATION)
    }

    /// Verify that the persisted Root runtime was written under this Actor.
    pub fn root_runtime_matches_activation(
        session: &Session,
        fence: &ActorActivationFence,
    ) -> io::Result<bool> {
        matches_observation(session, fence)
    }

    /// A planned update has saved its successor projection, but no execution
    /// effects are authorized until this exact Actor consumes revision zero.
    pub fn root_has_unstarted_planned_update_projection(
        session: &Session,
        activation: &ActorActivation,
    ) -> io::Result<bool> {
        Ok(Self::root_unstarted_planned_update_projection_sha256(session, activation)?.is_some())
    }

    /// A completed replacement can no longer be the current Actor after an
    /// ordinary Inbox activation. Its last exact handoff remains observable.
    pub fn root_completed_planned_update(
        session: &Session,
        predecessor: &ActorActivationFence,
    ) -> bool {
        session
            .metadata
            .get(PLANNED_UPDATE_COMPLETED)
            .is_some_and(|digest| {
                observation(session, predecessor)
                    .is_ok_and(|predecessor| predecessor.fence_sha256 == *digest)
            })
    }

    pub(super) fn root_unstarted_planned_update_projection_sha256(
        session: &Session,
        activation: &ActorActivation,
    ) -> io::Result<Option<String>> {
        if activation.checkpoint_revision != 0
            || root_planned_update_predecessor_sha256(&activation.lease_owner).is_none()
            || !session.is_initializing_planned_update_resume()
        {
            return Ok(None);
        }
        let Some(recorded) = read_observation(session)? else {
            return Ok(None);
        };
        if compatible_observation(&recorded, &observation(session, &activation.fence())?) {
            return Ok(Some(recorded.fence_sha256));
        }
        let Some(prior) =
            bamboo_domain::root_planned_update_prior_projection_sha256(&activation.lease_owner)
        else {
            return Ok(None);
        };
        Ok(compatible_observation(
            &recorded,
            &observation_for_fence_sha256(session, prior.into())?,
        )
        .then_some(recorded.fence_sha256))
    }

    /// Attribute the provider diagnostic to an already authorized failed run.
    /// The saved writer observation binds the pipeline state to the Root Actor.
    pub(crate) async fn managed_root_failure_diagnostic(
        &self,
        authority: &crate::v2::CurrentUserTargetGuard,
        root: &Session,
        run_id: &str,
    ) -> Option<LlmRequestDiagnostic> {
        let diagnostic = root.last_run_diagnostic()?;
        if !authority.reference.matches(root) || root.last_run_status().as_deref() != Some("error")
        {
            return None;
        }
        let path = authority.inbox_dir.with_file_name(ACTOR_AUTHORITY_FILE);
        let entry = self.observed_runtime_actor(&path, root).await.ok()??;
        let fence = entry.activation?.fence();
        if fence.run_id != run_id || !matches_observation(root, &fence).ok()? {
            return None;
        }
        let outcomes = root
            .runtime_metadata
            .as_ref()?
            .supervised_root_outcomes
            .as_ref()?;
        let result = outcomes.finalized_for_fence(root, &fence).ok()??;
        (result == SupervisedRootOutcomeResult::Failed).then_some(diagnostic)
    }

    /// Only the fully validated title/pinned transaction calls this.
    /// Advance a matching observation; never create one or change its fence.
    pub(in crate::v2) fn advance_root_presentation_observation(
        previous: &Session,
        incoming: &mut Session,
        entry: &ActorDirectoryEntry,
    ) -> io::Result<()> {
        if previous.kind != SessionKind::Root || !previous.authority_identity.is_ordinary() {
            return Ok(());
        }
        let Some(activation) = entry.activation.as_ref() else {
            return Ok(());
        };
        let updated = if matches_observation(previous, &activation.fence()).unwrap_or(false) {
            observation(incoming, &activation.fence())?
        } else {
            let digest =
                Self::root_unstarted_planned_update_projection_sha256(previous, activation)
                    .unwrap_or(None)
                    .or_else(|| {
                        parked_update_predecessor_sha256(previous, activation)
                            .unwrap_or(None)
                            .map(str::to_owned)
                    });
            let Some(digest) = digest else {
                return Ok(());
            };
            observation_for_fence_sha256(incoming, digest)?
        };
        incoming
            .metadata
            .insert(ROOT_OBSERVATION.into(), serde_json::to_string(&updated)?);
        Ok(())
    }

    async fn confirmed_root_main_outcome(
        &self,
        entry: &ActorDirectoryEntry,
        rel: &str,
        fence: &ActorActivationFence,
        guards: &Arc<ActorAuthorityGuards>,
    ) -> Result<Option<SupervisedRootOutcomeResult>, ActorDirectoryError> {
        let activation = entry
            .activation
            .as_ref()
            .filter(|activation| activation.matches_fence(fence))
            .ok_or(ActorDirectoryError::StaleFence)?;
        if entry.actor.parent_actor_id.is_some() {
            return Err(ActorDirectoryError::StaleFence);
        }
        let path = self.abs_path_from_rel(rel).join("session.json");
        if !Self::regular_actor_file(&path).await? {
            return Err(ActorDirectoryError::Corrupt);
        }
        let bytes = super::super::cold_main::read(&path)
            .await
            .map_err(storage)?;
        super::super::compact_main::validate_full_main(&bytes).map_err(storage)?;
        let main: Session =
            serde_json::from_slice(&bytes).map_err(|_| ActorDirectoryError::Corrupt)?;
        super::super::reject_incompatible_session_write(&main).map_err(storage)?;
        let mut outcome = main
            .runtime_metadata
            .as_ref()
            .and_then(|m| m.supervised_root_outcomes.as_ref())
            .map(|outcomes| outcomes.finalized_for_fence(&main, fence))
            .transpose()
            .map_err(|reason| {
                tracing::warn!(%reason, "Root Main terminal fact failed validation");
                ActorDirectoryError::Corrupt
            })?
            .flatten();
        if outcome == Some(SupervisedRootOutcomeResult::InterruptedUnknown) {
            return Ok(None);
        }
        if outcome.is_none()
            && activation.checkpoint_revision > 0
            && root_planned_update_predecessor_sha256(&activation.lease_owner).is_some()
        {
            // Ordinary Roots do not have a Supervisor ledger. Their actual
            // final save records the exact writer and terminal status in Main.
            // A leading Runtime write alone cannot attest completion.
            let status = match main.last_run_status().as_deref() {
                Some("completed" | "skipped") => Some(SupervisedRootOutcomeResult::Completed),
                Some("suspended") => Some(SupervisedRootOutcomeResult::Suspended),
                Some("cancelled") => Some(SupervisedRootOutcomeResult::Cancelled),
                Some("error" | "failed" | "timeout") => Some(SupervisedRootOutcomeResult::Failed),
                _ => None,
            };
            if status.is_some() && matches_observation(&main, fence).map_err(storage)? {
                outcome = status;
            }
        }
        if outcome.is_none() {
            return Ok(None);
        }
        if main.kind != SessionKind::Root
            || !main.authority_identity.is_ordinary()
            || !entry.actor.matches_session(&main)
            || entry.actor.project_id.as_deref() != main.project_id_meta().as_deref()
        {
            return Err(ActorDirectoryError::InvalidIdentity);
        }
        let current = self
            .projection_target(&fence.actor_id, rel)
            .await
            .map_err(storage)?
            .ok_or(ActorDirectoryError::InvalidIdentity)?;
        validate_projection_identity(entry, &current)?;
        if main.project_id_meta() != current.project_id_meta()
            || !current
                .metadata_version
                .checked_sub(main.metadata_version)
                .is_some_and(|delta| delta <= 1)
        {
            return Err(ActorDirectoryError::InvalidIdentity);
        }
        main.check_conversation_generation(&current)
            .map_err(|_| ActorDirectoryError::InvalidIdentity)?;
        let record = fs::read(self.abs_path_from_rel(rel).join(ACTOR_AUTHORITY_FILE))
            .await
            .map_err(storage)?;
        let marker = fs::read(self.abs_path_from_rel(rel).join(ACTOR_INITIALIZED_FILE))
            .await
            .map_err(storage)?;
        let project = main
            .project_id_meta()
            .map(ProjectId::parse)
            .transpose()
            .map_err(|_| ActorDirectoryError::InvalidIdentity)?;
        validate_census_witnesses(&record, &marker, &main, project.as_ref()).map_err(storage)?;
        if outcome.is_some() {
            // A readable AfterReplace result is not a commit acknowledgement.
            // Retain the exact Actor/Session/tree guards through the real sync.
            self.confirm_actor_file_durable(&path, guards).await?;
        }
        Ok(outcome)
    }

    async fn settle_root_entry(
        &self,
        entry: &mut ActorDirectoryEntry,
        rel: &str,
        path: &Path,
        fence: &ActorActivationFence,
        requested: Option<ActorActivationFinish>,
        now: DateTime<Utc>,
        guards: &Arc<ActorAuthorityGuards>,
    ) -> Result<
        (
            ActorActivation,
            Option<SupervisedRootOutcomeResult>,
            Option<String>,
        ),
        ActorDirectoryError,
    > {
        let activation = entry
            .activation
            .as_ref()
            .filter(|a| a.matches_fence(fence))
            .ok_or(ActorDirectoryError::StaleFence)?;
        let main = if is_root_compaction_lease_owner(&activation.lease_owner) {
            None
        } else {
            self.confirmed_root_main_outcome(entry, rel, fence, guards)
                .await?
        };
        let selected = match main.as_ref() {
            Some(
                SupervisedRootOutcomeResult::Completed | SupervisedRootOutcomeResult::Suspended,
            ) => ActorActivationFinish::Succeeded,
            Some(SupervisedRootOutcomeResult::Cancelled) => ActorActivationFinish::Cancelled,
            Some(SupervisedRootOutcomeResult::Failed) => ActorActivationFinish::Failed,
            Some(SupervisedRootOutcomeResult::InterruptedUnknown) => {
                return Err(ActorDirectoryError::InvalidTransition)
            }
            None => requested
                .or_else(|| terminal_finish(activation.status))
                .ok_or(ActorDirectoryError::InvalidTransition)?,
        };
        let selected_status = finish_status(selected);
        let repeated = activation.status == selected_status;
        if !repeated && !activation.status.is_live() && main.is_none() {
            return Err(ActorDirectoryError::InvalidTransition);
        }
        if activation.status.is_live() {
            if entry.actor.state != ActorLogicalState::Active
                || (selected == ActorActivationFinish::Succeeded
                    && activation.status != ActorActivationStatus::Running)
            {
                return Err(ActorDirectoryError::InvalidTransition);
            }
            // Only the actual owner or an expired exact durable Main outcome
            // closes this fence. Recovery never gets here for an unexpired live owner.
            if requested.is_none() && (activation.lease_expires_at > now || main.is_none()) {
                return Err(ActorDirectoryError::InvalidTransition);
            }
        }
        let completed_update = if main.is_some() {
            self.record_planned_update_completion(entry, rel, activation, guards)
                .await?
        } else {
            None
        };
        if repeated {
            self.confirm_actor_file_durable(path, guards).await?;
            return Ok((activation.clone(), main, completed_update));
        }
        let activation = entry
            .activation
            .as_mut()
            .ok_or(ActorDirectoryError::StaleFence)?;
        activation.status = selected_status;
        activation.finished_at.get_or_insert(now);
        let result = activation.clone();
        if entry.actor.state != ActorLogicalState::Retired {
            entry.actor.state = if selected == ActorActivationFinish::Failed {
                ActorLogicalState::Failed
            } else {
                ActorLogicalState::Cold
            };
        }
        entry.revision = checked_next(entry.revision)?;
        self.write_actor_entry(path, entry, guards).await?;
        Ok((result, main, completed_update))
    }

    async fn record_planned_update_completion(
        &self,
        entry: &ActorDirectoryEntry,
        rel: &str,
        activation: &ActorActivation,
        guards: &Arc<ActorAuthorityGuards>,
    ) -> Result<Option<String>, ActorDirectoryError> {
        let Some(digest) = root_planned_update_predecessor_sha256(&activation.lease_owner)
            .filter(|_| activation.checkpoint_revision > 0)
        else {
            return Ok(None);
        };
        let current = self
            .projection_target(&activation.actor_id, rel)
            .await
            .map_err(storage)?
            .ok_or(ActorDirectoryError::InvalidIdentity)?;
        validate_projection_identity(entry, &current)?;
        let mut completed = current.clone();
        completed
            .metadata
            .insert(PLANNED_UPDATE_COMPLETED.into(), digest.into());
        // Seal the prior handoff before the Actor becomes replaceable by Inbox.
        self.commit_runtime_projection(&current, &completed, rel, guards)
            .await?;
        Ok(Some(digest.into()))
    }

    async fn project_root_terminal(
        &self,
        before: &Session,
        outcome: &str,
        rel: &str,
        guards: &Arc<ActorAuthorityGuards>,
        publish: Option<&bamboo_domain::storage::RootActorRuntimePublisher>,
    ) -> Result<Session, ActorDirectoryError> {
        let mut after = before.clone();
        let same = before
            .last_run_status()
            .as_deref()
            .is_some_and(|s| terminal_runtime::same_terminal_class(s, outcome));
        // Preserve the Host's more specific timeout/skipped spelling and its
        // diagnostic, but never upgrade a typed state to authority or a receipt.
        if !same {
            after.set_last_run_status(outcome);
        }
        match outcome {
            "completed" => after.clear_last_run_error(),
            "cancelled" if !same || after.last_run_error().is_none() => {
                after.set_last_run_error("Root execution cancelled")
            }
            "error" if !same || after.last_run_error().is_none() => {
                after.set_last_run_error("Root activation failed before final-save was confirmed")
            }
            _ => {}
        }
        if !(same && terminal_runtime::idle_control_state(before)) {
            after.synchronize_terminal_runtime_status();
        }
        self.commit_runtime_projection(before, &after, rel, guards)
            .await?;
        if let Some(publish) = publish {
            publish(&after);
        }
        Ok(after)
    }

    async fn commit_root_observation(
        &self,
        before: &Session,
        after: &Session,
        rel: &str,
        guards: &Arc<ActorAuthorityGuards>,
    ) -> Result<(), ActorDirectoryError> {
        let path = self
            .abs_path_from_rel(rel)
            .join(super::super::RUNTIME_SIDECAR_FILE);
        let old = super::super::runtime_sidecar_snapshot(before);
        let new = super::super::runtime_sidecar_snapshot(after);
        if serde_json::to_value(&old).map_err(|_| ActorDirectoryError::Corrupt)?
            != serde_json::to_value(&new).map_err(|_| ActorDirectoryError::Corrupt)?
        {
            self.write_actor_bytes(
                &path,
                serde_json::to_vec_pretty(&new).map_err(|_| ActorDirectoryError::Corrupt)?,
                guards,
            )
            .await?;
        } else {
            self.confirm_actor_file_durable(&path, guards).await?;
        }
        // This is only an attribution checkpoint. Do not publish a leading
        // sidecar's unconfirmed terminal status to the index or cache.
        Ok(())
    }

    pub(super) async fn finish_root_activation_impl(
        &self,
        owner: &RootActorRuntimeWrite,
        now: DateTime<Utc>,
        outcome: ActorActivationFinish,
        publish: Option<&bamboo_domain::storage::RootActorRuntimePublisher>,
    ) -> Result<ActorActivation, ActorDirectoryError> {
        let fence = &owner.fence;
        let (rel, path, guards) = self.lock_actor_runtime(&fence.actor_id).await?;
        if !Self::regular_actor_file(
            &self
                .abs_path_from_rel(&rel)
                .join(super::super::RUNTIME_SIDECAR_FILE),
        )
        .await?
        {
            return Err(ActorDirectoryError::Corrupt);
        }
        let initial = self
            .projection_target(&fence.actor_id, &rel)
            .await
            .map_err(storage)?
            .ok_or(ActorDirectoryError::InvalidIdentity)?;
        owner
            .conversation_generation
            .check_canonical(&initial)
            .map_err(|_| ActorDirectoryError::InvalidIdentity)?;
        if initial.created_at != owner.created_at {
            return Err(ActorDirectoryError::InvalidIdentity);
        }
        let mut entry = self
            .observed_runtime_actor(&path, &initial)
            .await?
            .ok_or(ActorDirectoryError::StaleFence)?;
        validate_projection_identity(&entry, &initial)?;
        if is_root_compaction_lease_owner(&fence.lease_owner) {
            // Compaction owns this exact fence and can close it even if its
            // HTTP waiter was dropped. It has no chat outcome to project.
            let (finished, ..) = self
                .settle_root_entry(&mut entry, &rel, &path, fence, Some(outcome), now, &guards)
                .await?;
            return Ok(finished);
        }
        let active = entry
            .activation
            .as_ref()
            .filter(|a| a.matches_fence(fence))
            .ok_or(ActorDirectoryError::StaleFence)?;
        // Reject a conflicting repeat before changing even the observation.
        // Main may correct a diagnostic, but an ordinary repeated finish may not.
        if !active.status.is_live() {
            let main = self
                .confirmed_root_main_outcome(&entry, &rel, fence, &guards)
                .await?;
            if main.is_none() && terminal_finish(active.status) != Some(outcome) {
                return Err(ActorDirectoryError::InvalidTransition);
            }
        }
        if matches!(
            outcome,
            ActorActivationFinish::Cancelled | ActorActivationFinish::Failed
        ) && (Self::root_has_unstarted_planned_update_projection(&initial, active)
            .map_err(storage)?
            || parked_update_predecessor_sha256(&initial, active)
                .map_err(storage)?
                .is_some())
        {
            // This replacement never crossed the execution checkpoint. Close
            // its Actor attempt without consuming the exact retry projection.
            let (finished, ..) = self
                .settle_root_entry(&mut entry, &rel, &path, fence, Some(outcome), now, &guards)
                .await?;
            return Ok(finished);
        }
        let mut marked = initial.clone();
        let was_attributed = matches_observation(&initial, fence).unwrap_or(false);
        stamp_owned_observation(&mut marked, owner, &initial).map_err(storage)?;
        if !was_attributed
            && (active.status.is_live() || initial.metadata.contains_key(ROOT_OBSERVATION))
            && !initial.has_runtime_wait()
            && initial.last_run_status().as_deref() != Some("pending")
        {
            // First-write failure must not relabel A's error/Idle gate as B.
            // Record B's real context before committing its Actor terminal.
            marked.set_last_run_status("running");
            marked.clear_last_run_error();
            let state = marked
                .agent_runtime_state
                .get_or_insert_with(|| AgentRuntimeState::new(&marked.id));
            state.status = AgentStatusState::Initializing;
            marked.metadata.insert(
                "agent.runtime.state".into(),
                serde_json::to_string(state).map_err(|_| ActorDirectoryError::Corrupt)?,
            );
        }
        self.commit_root_observation(&initial, &marked, &rel, &guards)
            .await?;
        let (finished, main, completed_update) = self
            .settle_root_entry(&mut entry, &rel, &path, fence, Some(outcome), now, &guards)
            .await?;
        if let Some(digest) = completed_update {
            marked
                .metadata
                .insert(PLANNED_UPDATE_COMPLETED.into(), digest);
        }
        if main != Some(SupervisedRootOutcomeResult::Suspended)
            && !marked.has_runtime_wait()
            && marked.last_run_status().as_deref() != Some("pending")
        {
            self.project_root_terminal(
                &marked,
                terminal_runtime::terminal_status(finished.status)
                    .ok_or(ActorDirectoryError::InvalidTransition)?,
                &rel,
                &guards,
                publish,
            )
            .await?;
        } else {
            self.commit_runtime_projection(&marked, &marked, &rel, &guards)
                .await?;
            if let Some(publish) = publish {
                publish(&marked);
            }
        }
        Ok(finished)
    }

    /// A still-marked-running Root can be resumed only after its exact activation
    /// expires and storage has no confirmed terminal checkpoint for that owner.
    pub async fn has_expired_interrupted_root(&self, expected: &Session) -> io::Result<bool> {
        let (rel, path, guards) = self
            .lock_actor_runtime(&expected.id)
            .await
            .map_err(io::Error::other)?;
        let Some(current) = self.projection_target(&expected.id, &rel).await? else {
            return Ok(false);
        };
        if current.last_run_status().as_deref() != Some("running")
            || current.has_runtime_wait()
            || !Self::same_root_runtime_observation(expected, &current)
        {
            return Ok(false);
        }
        expected
            .check_conversation_generation(&current)
            .map_err(io::Error::other)?;
        let Some(entry) = self
            .observed_runtime_actor(&path, &current)
            .await
            .map_err(io::Error::other)?
        else {
            return Ok(false);
        };
        validate_projection_identity(&entry, &current).map_err(io::Error::other)?;
        let Some(activation) = entry.activation.as_ref() else {
            return Ok(false);
        };
        let fence = activation.fence();
        if !activation.status.is_live()
            || activation.lease_expires_at > Utc::now()
            || is_root_compaction_lease_owner(&activation.lease_owner)
            || root_planned_update_predecessor_sha256(&activation.lease_owner).is_some()
            || !matches_observation(&current, &fence)?
        {
            return Ok(false);
        }
        Ok(self
            .confirmed_root_main_outcome(&entry, &rel, &fence, &guards)
            .await
            .map_err(io::Error::other)?
            .is_none())
    }

    pub(crate) async fn reconcile_root_runtime_impl(
        &self,
        expected: &Session,
        cutoff: DateTime<Utc>,
        publish: &bamboo_domain::storage::RootActorRuntimePublisher,
    ) -> io::Result<Option<Session>> {
        let (rel, path, guards) = self
            .lock_actor_runtime(&expected.id)
            .await
            .map_err(io::Error::other)?;
        if !Self::regular_actor_file(
            &self
                .abs_path_from_rel(&rel)
                .join(super::super::RUNTIME_SIDECAR_FILE),
        )
        .await
        .map_err(io::Error::other)?
        {
            return Err(invalid());
        }
        let Some(mut latest) = self.projection_target(&expected.id, &rel).await? else {
            return Ok(None);
        };
        super::super::reject_incompatible_session_write(&latest)?;
        super::super::reject_incompatible_session_write(expected)?;
        super::super::child_wait_control_plane::validate_incarnation(expected, &latest)?;
        expected
            .check_conversation_generation(&latest)
            .map_err(io::Error::other)?;
        if latest.kind != SessionKind::Root
            || !latest.authority_identity.is_ordinary()
            || serde_json::to_value(expected)? != serde_json::to_value(&latest)?
        {
            return Ok(None);
        }
        let outcome = if let Some(mut entry) = self
            .observed_runtime_actor(&path, &latest)
            .await
            .map_err(io::Error::other)?
        {
            validate_projection_identity(&entry, &latest).map_err(io::Error::other)?;
            let Some(activation) = entry.activation.as_ref() else {
                return Ok(None);
            };
            if is_root_compaction_lease_owner(&activation.lease_owner) {
                // A previous compaction, including one interrupted by a crash,
                // supplies no evidence about the preceding chat run.
                return Ok(None);
            }
            if Self::root_has_unstarted_planned_update_projection(&latest, activation)? {
                // The desktop handoff can still retry this unused attempt.
                return Ok(None);
            }
            let fence = activation.fence();
            let attributed = matches_observation(&latest, &fence)?;
            if (!attributed
                && (latest.metadata.contains_key(ROOT_OBSERVATION)
                    || latest.conversation_reset.is_some()))
                || (!attributed
                    && latest
                        .host_run_id()
                        .is_some_and(|id| id != activation.run_id))
            {
                return Ok(None);
            }
            if activation.status.is_live()
                && (latest.updated_at >= cutoff || activation.lease_expires_at > Utc::now())
            {
                return Ok(None);
            }
            if activation.status.is_live()
                && self
                    .confirmed_root_main_outcome(&entry, &rel, &fence, &guards)
                    .await
                    .map_err(io::Error::other)?
                    .is_none()
            {
                return Ok(None);
            }
            let (settled, main, completed_update) = self
                .settle_root_entry(&mut entry, &rel, &path, &fence, None, Utc::now(), &guards)
                .await
                .map_err(io::Error::other)?;
            if let Some(digest) = completed_update {
                latest
                    .metadata
                    .insert(PLANNED_UPDATE_COMPLETED.into(), digest);
            }
            if main == Some(SupervisedRootOutcomeResult::Suspended) {
                self.commit_runtime_projection(&latest, &latest, &rel, &guards)
                    .await
                    .map_err(io::Error::other)?;
                publish(&latest);
                return Ok(Some(latest));
            }
            terminal_runtime::terminal_status(settled.status).map(str::to_string)
        } else {
            // An unactivated legacy Root has no exact current Actor/run proof.
            // Neither leading Runtime nor historical Main can create it here.
            return Ok(None);
        };
        let Some(outcome) = outcome else {
            return Ok(None);
        };
        if latest.has_runtime_wait() || latest.last_run_status().as_deref() == Some("pending") {
            self.commit_runtime_projection(&latest, &latest, &rel, &guards)
                .await
                .map_err(io::Error::other)?;
            publish(&latest);
            return Ok(Some(latest));
        }
        self.project_root_terminal(&latest, &outcome, &rel, &guards, Some(publish))
            .await
            .map(Some)
            .map_err(io::Error::other)
    }
}

#[cfg(test)]
#[path = "root_terminal_runtime_tests.rs"]
mod tests;
