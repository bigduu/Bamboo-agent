//! Child-wait mutations share the canonical physical Session writer boundary.
//! They own wait/control fields, never transcript or Actor execution authority.

use super::*;
use bamboo_domain::{
    AgentRuntimeState, ChildWaitPolicy, RootActorRuntimePublisher, WaitingForChildrenState,
};

pub(crate) fn runtime(session: &Session) -> AgentRuntimeState {
    session
        .agent_runtime_state
        .clone()
        .or_else(|| {
            session
                .metadata
                .get("agent.runtime.state")
                .and_then(|raw| serde_json::from_str(raw).ok())
        })
        .unwrap_or_else(|| AgentRuntimeState::new(format!("{}-child-wait", session.id)))
}

pub(crate) fn write_runtime(session: &mut Session, state: AgentRuntimeState) -> io::Result<()> {
    session.metadata.insert(
        "agent.runtime.state".into(),
        serde_json::to_string(&state).map_err(|error| other_io_error(error.to_string()))?,
    );
    session.agent_runtime_state = Some(state);
    Ok(())
}

pub(crate) fn validate_incarnation(expected: &Session, latest: &Session) -> io::Result<()> {
    reject_conversation_generation_change(expected, latest)?;
    if expected.id != latest.id
        || expected.created_at != latest.created_at
        || expected.kind != latest.kind
        || expected.parent_session_id != latest.parent_session_id
        || expected.root_session_id != latest.root_session_id
        || expected.authority_identity != latest.authority_identity
        || expected.spawn_depth != latest.spawn_depth
        || expected.project_id_meta() != latest.project_id_meta()
    {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            bamboo_domain::SessionAuthorityConflict(
                "child wait Session incarnation changed".into(),
            ),
        ));
    }
    Ok(())
}

pub(crate) fn terminal(status: &str) -> bool {
    matches!(
        status,
        "completed" | "error" | "timeout" | "cancelled" | "skipped"
    )
}

pub(crate) fn is_observation(expected: &Session, updated: &Session) -> bool {
    runtime(expected) == runtime(updated)
        && ["runtime.suspend_reason", "guardian.state"]
            .iter()
            .all(|key| expected.metadata.get(*key) == updated.metadata.get(*key))
        && expected
            .messages
            .iter()
            .map(|message| &message.id)
            .eq(updated.messages.iter().map(|message| &message.id))
}

pub(crate) fn register_pending(
    latest: &mut Session,
    pending: Vec<(String, Option<String>)>,
    policy: ChildWaitPolicy,
) -> io::Result<()> {
    let mut state = runtime(latest);
    let mut wait = state
        .waiting_for_children
        .take()
        .unwrap_or_else(|| WaitingForChildrenState::for_children(Vec::new(), policy, Utc::now()));
    wait.wait_for = policy;
    for (id, tag) in pending {
        wait.child_session_ids.push(id);
        if wait.registered_by_tool_call_id.is_none() {
            wait.registered_by_tool_call_id = tag;
        }
    }
    wait.child_session_ids.sort();
    wait.child_session_ids.dedup();
    state.waiting_for_children = Some(wait);
    write_runtime(latest, state)?;
    latest.metadata.insert(
        "runtime.suspend_reason".into(),
        "waiting_for_children".into(),
    );
    latest.updated_at = Utc::now();
    Ok(())
}

pub(crate) fn apply_transition(
    latest: &mut Session,
    expected: &Session,
    updated: &Session,
    runtime_only: bool,
) -> io::Result<()> {
    let mut state = runtime(latest);
    let before = runtime(expected);
    let desired = runtime(updated);
    state.waiting_for_children = desired.waiting_for_children;
    // A partial Guardian outcome can update only its verdict while another
    // activation opens the parent's reasoning gate. Apply the coordinator's
    // actual changes, rather than restoring its older runner snapshot.
    if desired.status != before.status {
        state.status = desired.status;
    }
    if desired.suspension != before.suspension {
        state.suspension = desired.suspension;
    }
    write_runtime(latest, state)?;
    for key in ["runtime.suspend_reason", "guardian.state"] {
        if updated.metadata.get(key) != expected.metadata.get(key) {
            match updated.metadata.get(key) {
                Some(value) => {
                    latest.metadata.insert(key.into(), value.clone());
                }
                None => {
                    latest.metadata.remove(key);
                }
            }
        }
    }
    latest.updated_at = Utc::now();
    if !runtime_only {
        for message in &updated.messages {
            if !expected.messages.iter().any(|old| old.id == message.id)
                && !latest.messages.iter().any(|old| old.id == message.id)
            {
                latest.messages.push(message.clone());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod incremental_wait_tests {
    use super::*;
    use bamboo_domain::{AgentStatusState, SuspensionState};

    #[tokio::test]
    async fn runtime_only_child_wait_registration_and_clear_update_the_index() -> io::Result<()> {
        let temp = tempfile::tempdir()?;
        let first = SessionStoreV2::new(temp.path().into()).await?;
        let parent = Session::new("runtime-only-parent", "model");
        first.save_session(&parent).await?;
        let main_path = first
            .sessions_root_dir()
            .join(&parent.id)
            .join("session.json");
        let original_main = fs::read(&main_path).await?;
        let second = SessionStoreV2::new(temp.path().into()).await?;

        let (registered, count) = first
            .register_child_wait(
                &parent,
                &[("child".into(), Some("wait-call".into()))],
                ChildWaitPolicy::All,
                false,
                Arc::new(|_| {}),
            )
            .await?;
        assert_eq!(count, 1);
        assert_eq!(
            runtime(&first.load_session(&parent.id).await?.unwrap())
                .waiting_for_children
                .unwrap()
                .child_session_ids,
            ["child"]
        );
        assert!(
            first
                .get_index_entry(&parent.id)
                .await
                .unwrap()
                .waiting_for_children
        );
        // This writer opened before registration and its cached row still says no wait.
        assert!(
            !second
                .get_index_entry(&parent.id)
                .await
                .unwrap()
                .waiting_for_children
        );

        let mut updated = registered.clone();
        let mut state = runtime(&updated);
        state.waiting_for_children = None;
        write_runtime(&mut updated, state)?;
        assert!(
            second
                .compare_exchange_child_wait(&registered, &mut updated, true, Arc::new(|_| {}))
                .await?
        );
        assert!(runtime(&second.load_session(&parent.id).await?.unwrap())
            .waiting_for_children
            .is_none());
        let reopened = SessionStoreV2::new(temp.path().into()).await?;
        assert!(
            !reopened
                .get_index_entry(&parent.id)
                .await
                .unwrap()
                .waiting_for_children
        );
        assert_eq!(fs::read(main_path).await?, original_main);
        Ok(())
    }

    #[tokio::test]
    async fn partial_guardian_verdict_preserves_concurrently_activated_parent() {
        let temp = tempfile::tempdir().unwrap();
        let store = SessionStoreV2::new(temp.path().into()).await.unwrap();
        let mut expected = Session::new("partial-guardian-parent", "model");
        let mut state = AgentRuntimeState::new("waiting-run");
        state.status = AgentStatusState::Suspended;
        let original = WaitingForChildrenState::for_children(
            vec!["guardian".into(), "slow-child".into()],
            ChildWaitPolicy::All,
            Utc::now(),
        );
        state.waiting_for_children = Some(original.clone());
        state.suspension = Some(SuspensionState {
            reason: "waiting_for_children".into(),
            suspended_at: Utc::now(),
            resumable: true,
            hook_point: Some("ChildCompletion".into()),
        });
        write_runtime(&mut expected, state).unwrap();
        store.save_session(&expected).await.unwrap();

        let mut activated = expected.clone();
        let mut running = runtime(&activated);
        running.run_id = "new-parent-owner".into();
        running.status = AgentStatusState::Running;
        running.suspension = None;
        write_runtime(&mut activated, running).unwrap();
        store.save_session(&activated).await.unwrap();

        let mut verdict = expected.clone();
        verdict
            .metadata
            .insert("guardian.state".into(), "approved".into());
        assert!(store
            .compare_exchange_child_wait(&expected, &mut verdict, true, Arc::new(|_| {}),)
            .await
            .unwrap());
        let saved = store.load_session(&expected.id).await.unwrap().unwrap();
        let state = runtime(&saved);
        assert_eq!(state.run_id, "new-parent-owner");
        assert_eq!(state.status, AgentStatusState::Running);
        assert!(state.suspension.is_none());
        assert_eq!(state.waiting_for_children, Some(original));
        assert_eq!(
            saved.metadata.get("guardian.state").map(String::as_str),
            Some("approved")
        );
    }
}

impl SessionStoreV2 {
    /// Test-only barrier after fresh status reads while registration retains
    /// the physical parent writer. No production build exposes this hook.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn pause_child_wait_registration_after_status_read(
        &self,
    ) -> (Arc<tokio::sync::Barrier>, Arc<tokio::sync::Barrier>) {
        let entered = Arc::new(tokio::sync::Barrier::new(2));
        let release = Arc::new(tokio::sync::Barrier::new(2));
        *self.child_wait_registration_pause.lock().unwrap() =
            Some((entered.clone(), release.clone()));
        (entered, release)
    }

    async fn child_wait_guards(&self, id: &str) -> io::Result<Arc<DefaultWriterGuards>> {
        validate_session_id(id)?;
        let lifecycle = self.lock_default_writer_lifecycle().await?;
        let task = self.lock_runtime_task_sidecar_shared().await?;
        let session = self
            .acquire_session_write_lock(id, SaveKind::Runtime)
            .await?;
        Ok(DefaultWriterGuards::shared(lifecycle, task, session))
    }

    /// Read the canonical owned child path without an instance index. Check
    /// durable linkage before loading any full transcript.
    pub(super) async fn load_child_wait_session(
        &self,
        parent: &Session,
        id: &str,
        full: bool,
    ) -> io::Result<Option<Session>> {
        validate_session_id(id)?;
        let root = if parent.kind == SessionKind::Root {
            &parent.id
        } else {
            &parent.root_session_id
        };
        validate_session_id(&parent.id)?;
        validate_session_id(root)?;
        let directory = self.abs_path_from_rel(&Self::child_rel_path(root, id));
        let side = Self::read_runtime_sidecar_at(&directory.join(RUNTIME_SIDECAR_FILE), id).await?;
        let Some(side) = side else {
            // Ordinary compatibility: legacy Child Main remains authoritative
            // when its optional sidecar is missing or corrupt.
            let bytes = match cold_main::read(directory.join("session.json")).await {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(error),
            };
            compact_main::validate_full_main(&bytes)?;
            let main: Session = serde_json::from_slice(&bytes)
                .map_err(|error| other_io_error(error.to_string()))?;
            supervisor::validate_identity(&main)?;
            self.validate_root_tool_authority_overlay(id, &main, None)
                .await?;
            if main.id != id
                || main.kind != SessionKind::Child
                || main.parent_session_id.as_deref() != Some(&parent.id)
                || main.root_session_id != *root
                || !self.session_lifetime_is_live(&main).await?
            {
                return Ok(None);
            }
            return Ok(Some(if full {
                main
            } else {
                runtime_sidecar_snapshot(&main)
            }));
        };
        if side.id != id
            || side.kind != SessionKind::Child
            || side.parent_session_id.as_deref() != Some(&parent.id)
            || side.root_session_id != *root
            || !self.session_lifetime_is_live(&side).await?
        {
            return Ok(None);
        }
        if !full {
            return Ok(Some(side));
        }
        let bytes = match cold_main::read(directory.join("session.json")).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        compact_main::validate_full_main(&bytes)?;
        let main: Session =
            serde_json::from_slice(&bytes).map_err(|error| other_io_error(error.to_string()))?;
        if main.id != side.id
            || main.created_at != side.created_at
            || main.parent_session_id != side.parent_session_id
            || main.root_session_id != side.root_session_id
            || main.kind != side.kind
            || main.spawn_depth != side.spawn_depth
        {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                bamboo_domain::SessionAuthorityConflict("child wait canonical pair changed".into()),
            ));
        }
        supervisor::validate_overlay(&main, Some(&side))?;
        self.validate_root_tool_authority_overlay(id, &main, Some(&side))
            .await?;
        if !self.session_lifetime_is_live(&main).await? {
            return Ok(None);
        }
        Ok(Some(overlay_runtime_sidecar(main, Some(side))))
    }

    async fn child_wait_terminal_status(
        &self,
        parent: &Session,
        id: &str,
    ) -> io::Result<Option<String>> {
        Ok(self
            .load_child_wait_session(parent, id, false)
            .await?
            .and_then(|child| child.last_run_status())
            .filter(|status| terminal(status)))
    }

    async fn publish_child_wait(
        &self,
        guards: &Arc<DefaultWriterGuards>,
        committed: &Session,
        publish: RootActorRuntimePublisher,
    ) -> io::Result<()> {
        let snapshot = committed.clone();
        Self::default_writer_job(guards, move || {
            publish(&snapshot);
            Ok(())
        })
        .await
    }

    pub(super) async fn register_child_wait(
        &self,
        expected: &Session,
        batch: &[(String, Option<String>)],
        policy: ChildWaitPolicy,
        check_terminal: bool,
        publish: RootActorRuntimePublisher,
    ) -> io::Result<(Session, usize)> {
        let started = Instant::now();
        let guards = self.child_wait_guards(&expected.id).await?;
        let mut latest = self
            .load_runtime_control_plane_unchecked(&expected.id)
            .await?
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "child wait parent disappeared")
            })?;
        validate_incarnation(expected, &latest)?;
        let mut pending = Vec::new();
        let mut satisfied = false;
        for entry in batch {
            if let Some(state) = latest.supervisor_management.as_ref() {
                if let Some(child) = self
                    .load_child_wait_session(&latest, &entry.0, false)
                    .await?
                {
                    if state.classifies_observer(&latest, &child) {
                        return Err(io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            "an Observer cannot become a foreground child wait",
                        ));
                    }
                }
            }
            if !check_terminal {
                // A synchronous launch owns a new run that has not reached
                // enqueue yet. Its predecessor's terminal status is irrelevant.
                pending.push(entry.clone());
                continue;
            }
            match self.child_wait_terminal_status(&latest, &entry.0).await? {
                Some(status) => {
                    satisfied |= policy == ChildWaitPolicy::Any
                        || (policy == ChildWaitPolicy::FirstError
                            && matches!(status.as_str(), "error" | "timeout" | "cancelled"));
                }
                None => pending.push(entry.clone()),
            }
        }
        #[cfg(any(test, feature = "test-utils"))]
        {
            let pause = self.child_wait_registration_pause.lock().unwrap().take();
            if let Some((entered, release)) = pause {
                entered.wait().await;
                release.wait().await;
            }
        }
        if satisfied || pending.is_empty() {
            self.publish_child_wait(&guards, &latest, publish).await?;
            return Ok((latest, 0));
        }
        let count = pending.len();
        register_pending(&mut latest, pending, policy)?;
        let rel = Self::default_writer_rel_path(&latest)?;
        self.save_runtime_state_after_lock(&latest, &rel, started, &guards, None, None)
            .await?;
        self.publish_child_wait(&guards, &latest, publish).await?;
        Ok((latest, count))
    }

    pub(super) async fn expire_root_child_wait(
        &self,
        expected: &Session,
        committed: &bamboo_domain::CommittedChildWaitExpiry,
        inbox: &dyn bamboo_domain::SessionInboxPort,
        now: DateTime<Utc>,
        publish: RootActorRuntimePublisher,
    ) -> io::Result<bool> {
        use crate::session_inbox::FileSessionInbox;

        let inbox = inbox
            .as_any()
            .and_then(|any| any.downcast_ref::<FileSessionInbox>())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Unsupported,
                    "Root child wait expiry requires the concrete Inbox",
                )
            })?;
        let started = Instant::now();
        let mut guards = self.child_wait_guards(&expected.id).await?;
        let Some(mut latest) = self.load_session_unlocked(&expected.id).await? else {
            return Ok(false);
        };
        validate_incarnation(expected, &latest)?;
        let expiry = &committed.expiry;
        let receipt = committed.receipt();
        let mut state = runtime(&latest);
        if latest.kind != SessionKind::Root
            || !latest.authority_identity.is_ordinary()
            || latest.metadata.contains_key("ticket.runtime.dispatch.v1")
            || latest.has_pending_question()
            || latest
                .metadata
                .contains_key(bamboo_domain::ONCE_RESPONSE_HANDOFF_KEY)
            || latest.last_run_status().as_deref() != Some("suspended")
            || state.waiting_for_bash.is_some()
            || latest
                .metadata
                .get("runtime.suspend_reason")
                .is_some_and(|reason| reason != "waiting_for_children")
            || state
                .suspension
                .as_ref()
                .is_some_and(|suspension| suspension.reason != "waiting_for_children")
            || state
                .plan_mode
                .as_ref()
                .is_some_and(|plan| plan.status == bamboo_domain::PlanModeStatus::AwaitingApproval)
            || state.run_id != expiry.run_id
            || state.waiting_for_children.as_ref() != Some(&expiry.wait)
            || !expiry.matches_lifetime(&latest)
            || !expiry.is_due(now)
            || self
                .child_wait_expiry_activation_id(&latest, &state.run_id)
                .await?
                .as_deref()
                != Some(expiry.actor_activation_id.as_str())
        {
            return Ok(false);
        }
        let envelope = expiry.envelope(&latest.id)?;
        let Some(filesystem) = inbox
            .retain_staged_child_wait_expiry(
                &self.sessions_dir,
                &envelope,
                &receipt,
                guards.physical.clone(),
            )
            .await
            .map_err(io::Error::other)?
        else {
            return Ok(false);
        };
        // Keep the staged receipt protected through the blocking runtime write.
        Arc::get_mut(&mut guards)
            .expect("new child wait guards are private")
            ._input_filesystem = Some(filesystem);

        state.waiting_for_children = None;
        state.status = bamboo_domain::AgentStatusState::Idle;
        state.suspension = None;
        write_runtime(&mut latest, state)?;
        latest.metadata.remove("runtime.suspend_reason");
        latest.metadata.insert(
            bamboo_domain::CHILD_WAIT_EXPIRY_RECEIPT_KEY.into(),
            serde_json::to_string(committed)?,
        );
        latest.updated_at = now;
        let rel = Self::default_writer_rel_path(&latest)?;
        self.save_runtime_state_after_lock(&latest, &rel, started, &guards, None, None)
            .await?;
        self.publish_child_wait(&guards, &latest, publish).await?;
        Ok(true)
    }

    pub(super) async fn compare_exchange_child_wait(
        &self,
        expected: &Session,
        updated: &mut Session,
        runtime_only: bool,
        publish: RootActorRuntimePublisher,
    ) -> io::Result<bool> {
        let started = Instant::now();
        let guards = self.child_wait_guards(&expected.id).await?;
        let latest = if runtime_only {
            self.load_runtime_control_plane_unchecked(&expected.id)
                .await?
        } else {
            self.load_session_unlocked(&expected.id).await?
        }
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "child wait parent disappeared"))?;
        validate_incarnation(expected, &latest)?;
        validate_incarnation(expected, updated)?;
        let state = runtime(&latest);
        if state.waiting_for_children != runtime(expected).waiting_for_children {
            return Ok(false);
        }
        if is_observation(expected, updated) {
            *updated = latest;
            return Ok(true);
        }
        let mut committed = latest;
        apply_transition(&mut committed, expected, updated, runtime_only)?;
        if runtime_only {
            let rel = Self::default_writer_rel_path(&committed)?;
            self.save_runtime_state_after_lock(&committed, &rel, started, &guards, None, None)
                .await?;
        } else {
            self.save_session_after_lock(&committed, started, &guards, None)
                .await?;
        }
        self.publish_child_wait(&guards, &committed, publish)
            .await?;
        *updated = committed;
        Ok(true)
    }
}
