//! Root logical terminal repair. No lease or cache observation proves Worker stop.
use super::*;
use bamboo_domain::{
    AgentRuntimeState, AgentStatusState, RootActorRuntimeWrite, SessionKind,
    SupervisedRootOutcomeResult,
};
use sha2::{Digest, Sha256};

pub(super) const ROOT_OBSERVATION: &str = "runtime.root_writer_observation_v1";
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
    if session.kind != SessionKind::Root
        || !session.authority_identity.is_ordinary()
        || session.id != fence.actor_id
    {
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
        fence_sha256: format!("{:x}", Sha256::digest(serde_json::to_vec(fence)?)),
        conversation_reset_sha256: session
            .conversation_reset_sha256()
            .map_err(|_| invalid())?
            .unwrap_or_else(|| format!("{:x}", Sha256::digest(b"bamboo.root-runtime.no-reset.v1"))),
    })
}
fn matches_observation(session: &Session, fence: &ActorActivationFence) -> io::Result<bool> {
    let Some(raw) = session.metadata.get(ROOT_OBSERVATION) else {
        return Ok(false);
    };
    if raw.len() > 2048 {
        return Err(invalid());
    }
    let recorded: RootObservation = serde_json::from_str(raw).map_err(|_| invalid())?;
    Ok(compatible_observation(
        &recorded,
        &observation(session, fence)?,
    ))
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
    Ok(())
}
pub(super) fn preserve_observation(incoming: &mut Session, canonical: Option<&Session>) {
    incoming.metadata.remove(ROOT_OBSERVATION);
    if let Some(raw) = canonical.and_then(|s| s.metadata.get(ROOT_OBSERVATION)) {
        incoming
            .metadata
            .insert(ROOT_OBSERVATION.into(), raw.clone());
    }
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
    async fn confirmed_root_main_outcome(
        &self,
        entry: &ActorDirectoryEntry,
        rel: &str,
        fence: &ActorActivationFence,
        guards: &Arc<ActorAuthorityGuards>,
    ) -> Result<Option<SupervisedRootOutcomeResult>, ActorDirectoryError> {
        if entry.actor.parent_actor_id.is_some()
            || !entry
                .activation
                .as_ref()
                .is_some_and(|a| a.matches_fence(fence))
        {
            return Err(ActorDirectoryError::StaleFence);
        }
        let path = self.abs_path_from_rel(rel).join("session.json");
        if !Self::regular_actor_file(&path).await? {
            return Err(ActorDirectoryError::Corrupt);
        }
        let bytes = fs::read(&path).await.map_err(storage)?;
        super::super::compact_main::validate_full_main(&bytes).map_err(storage)?;
        let main: Session =
            serde_json::from_slice(&bytes).map_err(|_| ActorDirectoryError::Corrupt)?;
        super::super::reject_incompatible_session_write(&main).map_err(storage)?;
        let Some(outcomes) = main
            .runtime_metadata
            .as_ref()
            .and_then(|m| m.supervised_root_outcomes.as_ref())
        else {
            return Ok(None);
        };
        let outcome = outcomes
            .finalized_for_fence(&main, fence)
            .map_err(|reason| {
                tracing::warn!(%reason, "Root Main terminal fact failed validation");
                ActorDirectoryError::Corrupt
            })?;
        let outcome =
            outcome.filter(|result| *result != SupervisedRootOutcomeResult::InterruptedUnknown);
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
    ) -> Result<(ActorActivation, Option<SupervisedRootOutcomeResult>), ActorDirectoryError> {
        let activation = entry
            .activation
            .as_ref()
            .filter(|a| a.matches_fence(fence))
            .ok_or(ActorDirectoryError::StaleFence)?;
        let main = self
            .confirmed_root_main_outcome(entry, rel, fence, guards)
            .await?;
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
        if activation.status == selected_status {
            self.confirm_actor_file_durable(path, guards).await?;
            return Ok((activation.clone(), main));
        }
        if !activation.status.is_live() && main.is_none() {
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
        Ok((result, main))
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
        let (finished, main) = self
            .settle_root_entry(&mut entry, &rel, &path, fence, Some(outcome), now, &guards)
            .await?;
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
        let Some(latest) = self.projection_target(&expected.id, &rel).await? else {
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
            let (settled, main) = self
                .settle_root_entry(&mut entry, &rel, &path, &fence, None, Utc::now(), &guards)
                .await
                .map_err(io::Error::other)?;
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
