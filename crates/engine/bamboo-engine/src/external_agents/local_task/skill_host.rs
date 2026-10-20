//! Empty Skills authority for an already owned strict native Child execution.
//! Current input observations are data; neither history nor this host grants Source access.
use super::*;
use crate::runtime::config::SkillExecutionHost;
use crate::runtime::managers::lifecycle::BoundedInputRequestBatch;

pub(super) fn strict_child(session: &Session) -> Result<bool, AgentError> {
    Ok(bamboo_domain::ChildContextBinding::from_session(session)
        .map_err(failure)?
        .is_some()
        || crate::session_app::child_session::named_profile::has_named_profile(session)
        || session
            .metadata
            .contains_key(crate::ticket_worker_plan::TICKET_LOCAL_PLAN_KEY))
}

pub(super) fn skill_interface(name: &str) -> bool {
    matches!(
        name,
        "load_skill" | "read_skill_resource" | "skills_list" | "skills_read"
    )
}

pub(super) struct StrictChildSkills {
    store: Arc<bamboo_storage::SessionStoreV2>,
    owner: ChildActorRuntimeWrite,
    cancel: CancellationToken,
    live: AtomicBool,
    execution_id: Mutex<Option<String>>,
    #[cfg(test)]
    observations: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    batches: std::sync::atomic::AtomicUsize,
}
impl StrictChildSkills {
    // No public constructor or transport/metadata owner. Only the actual
    // prepared Execution supplies this writer and private cancellation token.
    pub(super) fn bind(
        store: Arc<bamboo_storage::SessionStoreV2>,
        execution: &Execution,
    ) -> Arc<Self> {
        let host = Arc::new(Self {
            store,
            owner: execution.owner.clone(),
            cancel: execution.cancel.clone(),
            live: AtomicBool::new(true),
            execution_id: Mutex::new(None),
            #[cfg(test)]
            observations: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            batches: std::sync::atomic::AtomicUsize::new(0),
        });
        #[cfg(test)]
        super::tests::skills::observe_bound(&host);
        host
    }
    #[cfg(test)]
    pub(super) fn session_id(&self) -> &str {
        &self.owner.fence.actor_id
    }
    #[cfg(test)]
    pub(super) fn observations(&self) -> (usize, usize, Option<String>) {
        (
            self.observations.load(Ordering::Relaxed),
            self.batches.load(Ordering::Relaxed),
            lock(&self.execution_id).clone(),
        )
    }
    fn check_live(&self) -> Result<(), AgentError> {
        if !self.live.load(Ordering::Acquire) || self.cancel.is_cancelled() {
            return Err(failure("strict Child Skills execution is no longer live"));
        }
        Ok(())
    }
    fn check_session(&self, session: &Session) -> Result<(), AgentError> {
        if session.kind != SessionKind::Child
            || !session.authority_identity.is_ordinary()
            || session.id != self.owner.fence.actor_id
            || session.created_at != self.owner.created_at
            || session.root_session_id != self.owner.root_session_id
            || session.parent_session_id.as_deref() != Some(self.owner.parent_session_id.as_str())
            || session.parent_created_at != Some(self.owner.parent_created_at)
            || session.spawn_depth != self.owner.spawn_depth
            || session.project_id_meta() != self.owner.project_id
            || !strict_child(session)?
        {
            return Err(failure(
                "strict Child Skills identity or restriction changed",
            ));
        }
        Ok(())
    }
}
#[async_trait]
impl SkillExecutionHost for StrictChildSkills {
    fn observe_current_inputs(
        &self,
        session_id: &str,
        execution_id: &str,
        batch: Option<&BoundedInputRequestBatch>,
    ) -> Result<(), AgentError> {
        self.check_live()?;
        if session_id != self.owner.fence.actor_id
            || execution_id.is_empty()
            || batch.is_some_and(|batch| {
                batch.session_id() != session_id || batch.execution_id() != execution_id
            })
        {
            self.live.store(false, Ordering::Release);
            return Err(failure(
                "strict Child Skills current-input identity changed",
            ));
        }
        let mut current = lock(&self.execution_id);
        if current.as_deref().is_some_and(|id| id != execution_id) {
            self.live.store(false, Ordering::Release);
            return Err(failure("strict Child Skills execution changed"));
        }
        *current = Some(execution_id.to_owned());
        #[cfg(test)]
        {
            self.observations.fetch_add(1, Ordering::Relaxed);
            if batch.is_some() {
                self.batches.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(())
    }
    async fn render_skill_prompt(
        &self,
        session: &Session,
        execution_id: &str,
    ) -> Result<String, AgentError> {
        self.check_live()?;
        self.check_session(session)?;
        if lock(&self.execution_id).as_deref() != Some(execution_id) {
            return Err(failure("strict Child Skills metadata execution changed"));
        }
        // Check the real current Actor fence/birth/ancestor chain, including
        // NoNew rounds. This existing guarded operation publishes no event.
        self.store
            .publish_child_actor_runtime_event(&self.owner, Box::new(|validate| validate()))
            .await
            .map_err(failure)?;
        self.check_live()?;
        Ok(String::new())
    }
    fn finish(&self, _: &str, _: &str) {
        self.live.store(false, Ordering::Release);
    }
}
