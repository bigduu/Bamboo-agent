//! Terminal projection and restart repair share the Actor's physical locks.
//! Authority is committed first; a failed projection is replayable by the same
//! fence or by startup reconciliation, without a version migration.

use super::*;
use bamboo_domain::{AgentRuntimeState, AgentStatusState, SessionKind};

fn conflict(message: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        bamboo_domain::SessionAuthorityConflict(message.into()),
    )
}

fn terminal_status(status: ActorActivationStatus) -> Option<&'static str> {
    match status {
        ActorActivationStatus::Succeeded => Some("completed"),
        ActorActivationStatus::Cancelled => Some("cancelled"),
        ActorActivationStatus::Failed => Some("error"),
        _ => None,
    }
}

fn pending_delivery(session: &Session) -> bool {
    session
        .metadata
        .get("runtime.child_completion_source_v1")
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
        .is_some_and(|source| source.get("pending_confirmation").is_some())
}

fn same_terminal_class(left: &str, right: &str) -> bool {
    let class = |status| match status {
        "completed" | "skipped" => Some(0),
        "cancelled" => Some(1),
        "error" | "failed" | "timeout" => Some(2),
        _ => None,
    };
    class(left).is_some() && class(left) == class(right)
}

impl SessionStoreV2 {
    async fn projection_target(&self, id: &str, rel: &str) -> io::Result<Option<Session>> {
        let (kind, root) = Self::copy_source_identity_from_rel(id, rel)?;
        Self::regular_actor_file(
            &self
                .abs_path_from_rel(rel)
                .join(super::super::RUNTIME_SIDECAR_FILE),
        )
        .await
        .map_err(io::Error::other)?;
        self.load_session_from_dir_strict(&self.abs_path_from_rel(rel), id, kind, &root)
            .await
    }

    async fn observed_runtime_actor(
        &self,
        path: &Path,
        session: &Session,
    ) -> Result<Option<ActorDirectoryEntry>, ActorDirectoryError> {
        if !Self::regular_actor_file(path).await? {
            if Self::regular_actor_file(&path.with_file_name(ACTOR_INITIALIZED_FILE)).await? {
                return Err(ActorDirectoryError::Corrupt);
            }
            return Ok(None);
        }
        let record = fs::read(path).await.map_err(storage)?;
        if !Self::regular_actor_file(&path.with_file_name(ACTOR_INITIALIZED_FILE)).await? {
            return Err(ActorDirectoryError::Corrupt);
        }
        if !Self::regular_actor_file(&path.with_file_name(super::super::RUNTIME_SIDECAR_FILE))
            .await?
        {
            return Err(ActorDirectoryError::Corrupt);
        }
        let marker = fs::read(path.with_file_name(ACTOR_INITIALIZED_FILE))
            .await
            .map_err(storage)?;
        let project = session
            .project_id_meta()
            .map(ProjectId::parse)
            .transpose()
            .map_err(|_| ActorDirectoryError::InvalidIdentity)?;
        validate_census_witnesses(&record, &marker, session, project.as_ref()).map_err(storage)?;
        let entry = serde_json::from_slice(&record).map_err(|_| ActorDirectoryError::Corrupt)?;
        Ok(Some(entry))
    }

    async fn commit_runtime_projection(
        &self,
        before: &Session,
        after: &Session,
        rel: &str,
        guards: &Arc<ActorAuthorityGuards>,
    ) -> Result<(), ActorDirectoryError> {
        let old = super::super::runtime_sidecar_snapshot(before);
        let new = super::super::runtime_sidecar_snapshot(after);
        if serde_json::to_value(&old).map_err(|_| ActorDirectoryError::Corrupt)?
            != serde_json::to_value(&new).map_err(|_| ActorDirectoryError::Corrupt)?
        {
            self.write_actor_bytes(
                &self
                    .abs_path_from_rel(rel)
                    .join(super::super::RUNTIME_SIDECAR_FILE),
                serde_json::to_vec_pretty(&new).map_err(|_| ActorDirectoryError::Corrupt)?,
                guards,
            )
            .await?;
        }
        // Replay an interrupted index publication without rewriting settled
        // records on every watchdog sweep.
        if self.get_index_entry(&after.id).await.is_none_or(|entry| {
            entry.last_run_status != after.last_run_status()
                || entry.last_run_error != after.last_run_error()
                || entry.updated_at != after.updated_at
        }) {
            self.upsert_index_from_session(after, rel.into())
                .await
                .map_err(storage)?;
        }
        Ok(())
    }

    pub(super) async fn project_actor_terminal(
        &self,
        entry: &ActorDirectoryEntry,
        rel: &str,
        guards: &Arc<ActorAuthorityGuards>,
    ) -> Result<(), ActorDirectoryError> {
        let Some(activation) = entry.activation.as_ref() else {
            return Ok(());
        };
        let Some(status) = terminal_status(activation.status) else {
            return Ok(());
        };
        let (kind, root) =
            Self::copy_source_identity_from_rel(&entry.actor.actor_id, rel).map_err(storage)?;
        if !Self::regular_actor_file(
            &self
                .abs_path_from_rel(rel)
                .join(super::super::RUNTIME_SIDECAR_FILE),
        )
        .await?
        {
            return Err(ActorDirectoryError::Corrupt);
        }
        let Some(before) = self
            .load_session_from_dir_strict(
                &self.abs_path_from_rel(rel),
                &entry.actor.actor_id,
                kind,
                &root,
            )
            .await
            .map_err(storage)?
        else {
            return Err(ActorDirectoryError::InvalidIdentity);
        };
        if !entry.actor.matches_session(&before) {
            return Err(ActorDirectoryError::InvalidIdentity);
        }
        super::super::reject_incompatible_session_write(&before).map_err(storage)?;
        if before.has_runtime_wait()
            || pending_delivery(&before)
            || !before.host_run_matches_launch()
            || before.last_run_status().as_deref() == Some("pending")
            || (before.host_run_id().is_some()
                && !super::super::child_wait_control_plane::terminal(
                    before.last_run_status().as_deref().unwrap_or(""),
                ))
            || before
                .host_run_id()
                .is_some_and(|id| id != activation.run_id)
        {
            return Ok(());
        }
        let mut after = before.clone();
        let prior = before.last_run_status();
        if prior
            .as_deref()
            .is_some_and(super::super::child_wait_control_plane::terminal)
        {
            if !prior
                .as_deref()
                .is_some_and(|prior| same_terminal_class(prior, status))
            {
                return Err(ActorDirectoryError::InvalidTransition);
            }
        } else {
            after.set_last_run_status(status);
        }
        if status == "completed" {
            after.clear_last_run_error();
        }
        after.synchronize_terminal_runtime_status();
        self.commit_runtime_projection(&before, &after, rel, guards)
            .await
    }

    pub(crate) async fn begin_host_run_impl(
        &self,
        expected: &Session,
        run_id: &str,
    ) -> io::Result<()> {
        if run_id.trim().is_empty() {
            return Err(conflict("empty Host run identity"));
        }
        let (rel, path, guards) = self
            .lock_actor_runtime(&expected.id)
            .await
            .map_err(io::Error::other)?;
        let latest = self
            .projection_target(&expected.id, &rel)
            .await?
            .ok_or_else(|| conflict("Host run Session disappeared"))?;
        super::super::reject_incompatible_session_write(&latest)?;
        super::super::reject_incompatible_session_write(expected)?;
        super::super::child_wait_control_plane::validate_incarnation(expected, &latest)?;
        if expected.parent_created_at != latest.parent_created_at {
            return Err(conflict("Host run parent incarnation changed"));
        }
        if expected.child_launch_generation() != latest.child_launch_generation() {
            return Err(conflict("Host startup launch generation was superseded"));
        }
        if latest.host_run_id() == Some(run_id) {
            return Ok(());
        }
        if latest.host_run_id() != expected.host_run_id() {
            return Err(conflict("Host run was superseded before startup"));
        }
        if self
            .observed_runtime_actor(&path, &latest)
            .await
            .map_err(io::Error::other)?
            .and_then(|entry| entry.activation)
            .is_some_and(|activation| activation.status.is_live() && activation.run_id != run_id)
        {
            return Err(conflict("another Actor attempt owns Host startup"));
        }
        let mut after = latest.clone();
        after.set_host_run_id(run_id);
        after.metadata.remove("runtime.reconciled_activation_fence");
        after.set_last_run_status("running");
        after.clear_last_run_error();
        if !after.has_runtime_wait() {
            let state = after
                .agent_runtime_state
                .get_or_insert_with(|| AgentRuntimeState::new(&after.id));
            state.status = AgentStatusState::Initializing;
            after
                .metadata
                .insert("agent.runtime.state".into(), serde_json::to_string(state)?);
        }
        after.updated_at = Utc::now();
        self.commit_runtime_projection(&latest, &after, &rel, &guards)
            .await
            .map_err(io::Error::other)
    }

    pub(crate) async fn reconcile_child_runtime_impl(
        &self,
        expected: &Session,
        before: DateTime<Utc>,
        status: &str,
        error: Option<&str>,
    ) -> io::Result<Option<Session>> {
        let (rel, path, guards) = self
            .lock_actor_runtime(&expected.id)
            .await
            .map_err(io::Error::other)?;
        let Some(latest) = self.projection_target(&expected.id, &rel).await? else {
            return Ok(None);
        };
        super::super::reject_incompatible_session_write(&latest)?;
        super::super::reject_incompatible_session_write(expected)?;
        super::super::child_wait_control_plane::validate_incarnation(expected, &latest)?;
        // This is a control-plane CAS, including Host run UUID, pipeline UUID,
        // launch generation, Inbox admission and timestamp, not just a wait CAS.
        if latest.kind != SessionKind::Child
            || latest.updated_at >= before
            || latest.has_runtime_wait()
            || pending_delivery(&latest)
            || !latest.host_run_matches_launch()
            || latest.last_run_status().as_deref() == Some("pending")
            || latest.session_inbox_admission() != expected.session_inbox_admission()
            || serde_json::to_value(super::super::runtime_sidecar_snapshot(expected))?
                != serde_json::to_value(super::super::runtime_sidecar_snapshot(&latest))?
        {
            return Ok(None);
        }
        let mut chosen_status = latest
            .last_run_status()
            .filter(|s| super::super::child_wait_control_plane::terminal(s));
        let mut expired = false;
        let mut recovered_fence = None;
        if let Some(mut entry) = self
            .observed_runtime_actor(&path, &latest)
            .await
            .map_err(io::Error::other)?
        {
            if let Some(activation) = entry.activation.as_mut() {
                if latest
                    .host_run_id()
                    .is_some_and(|id| id != activation.run_id)
                {
                    return Ok(None);
                }
                if activation.status.is_live() {
                    if activation.lease_expires_at > Utc::now() {
                        return Ok(None);
                    }
                    // Expiry revokes logical authority; it does not certify that
                    // a remote Worker stopped or authorize unfenced side effects.
                    activation.status = ActorActivationStatus::Failed;
                    expired = true;
                    activation.finished_at = Some(Utc::now());
                    entry.actor.state = ActorLogicalState::Failed;
                    entry.revision = checked_next(entry.revision).map_err(io::Error::other)?;
                    self.write_actor_entry(&path, &entry, &guards)
                        .await
                        .map_err(io::Error::other)?;
                }
                chosen_status = entry
                    .activation
                    .as_ref()
                    .and_then(|a| terminal_status(a.status))
                    .map(str::to_string);
                recovered_fence = entry.activation.as_ref().map(ActorActivation::fence);
            }
        }
        if chosen_status.is_none()
            && !matches!(
                latest.last_run_status().as_deref(),
                Some("running" | "initializing")
            )
        {
            return Ok(None);
        }
        let mut after = latest.clone();
        if let Some(fence) = recovered_fence {
            after.metadata.insert(
                "runtime.reconciled_activation_fence".into(),
                serde_json::to_string(&fence)?,
            );
        }
        after.set_last_run_status(chosen_status.as_deref().unwrap_or(status));
        if chosen_status.is_none() || expired {
            if let Some(error) = error {
                after.set_last_run_error(error)
            }
        }
        after.synchronize_terminal_runtime_status();
        self.commit_runtime_projection(&latest, &after, &rel, &guards)
            .await
            .map_err(io::Error::other)?;
        Ok(Some(after))
    }

    /// Called only after the ordinary physical Session writer locks are held.
    pub(crate) async fn normalize_terminal_runtime_write(
        &self,
        incoming: &Session,
        rel: &str,
    ) -> io::Result<Session> {
        let path = self.abs_path_from_rel(rel);
        if let Some(latest) = Self::read_runtime_sidecar_at(
            &path.join(super::super::RUNTIME_SIDECAR_FILE),
            &incoming.id,
        )
        .await?
        {
            if incoming.child_launch_generation() < latest.child_launch_generation() {
                return Err(conflict(
                    "Host runtime writer belongs to an older launch generation",
                ));
            }
            if latest.host_run_id() != incoming.host_run_id()
                && (latest.host_run_id().is_some() || incoming.host_run_id().is_some())
            {
                return Err(conflict("Host runtime writer belongs to a different run"));
            }
            if latest.host_run_id().is_some()
                && super::super::child_wait_control_plane::terminal(
                    latest.last_run_status().as_deref().unwrap_or(""),
                )
                && !super::super::child_wait_control_plane::terminal(
                    incoming.last_run_status().as_deref().unwrap_or(""),
                )
                && !(incoming.last_run_status().as_deref() == Some("pending")
                    && incoming.child_launch_generation() > latest.child_launch_generation())
                && !pending_delivery(incoming)
            {
                return Err(conflict("Host runtime writer would reopen a terminal run"));
            }
        }
        let mut candidate = incoming.clone();
        if !candidate.has_runtime_wait() && !pending_delivery(&candidate) {
            if let Some(entry) = if candidate.host_run_id().is_some() {
                self.observed_runtime_actor(&path.join(ACTOR_AUTHORITY_FILE), &candidate)
                    .await
                    .map_err(io::Error::other)?
            } else {
                None
            } {
                if let Some(activation) = entry.activation {
                    if candidate
                        .host_run_id()
                        .is_none_or(|id| id == activation.run_id)
                    {
                        if candidate.host_run_matches_launch()
                            && candidate.last_run_status().as_deref() != Some("pending")
                        {
                            if let Some(status) = terminal_status(activation.status) {
                                let matches = candidate.last_run_status().as_deref().is_some_and(
                                    |candidate| same_terminal_class(candidate, status),
                                );
                                if !matches {
                                    return Err(conflict(
                                        "Host outcome conflicts with committed activation",
                                    ));
                                }
                            }
                        }
                    }
                }
            }
            candidate.synchronize_terminal_runtime_status();
        }
        Ok(candidate)
    }
}

#[cfg(test)]
#[path = "terminal_runtime_tests.rs"]
mod tests;
