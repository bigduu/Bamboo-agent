//! App-facing binding for an already owned same-process ordinary Child.
use super::*;
use crate::runtime::config::SkillExecutionHost;

/// Execution proof supplied only by the actual local runner after admission.
/// It is neither serialized nor constructible from an input/Session snapshot.
pub struct LocalChildSkillAuthority {
    store: Arc<bamboo_storage::SessionStoreV2>,
    owner: ChildActorRuntimeWrite,
    cancel: CancellationToken,
    reader_callable: AtomicBool,
}
impl LocalChildSkillAuthority {
    pub(super) fn bind(
        store: Arc<bamboo_storage::SessionStoreV2>,
        execution: &Execution,
    ) -> Arc<Self> {
        Arc::new(Self {
            store,
            owner: execution.owner.clone(),
            cancel: execution.cancel.clone(),
            reader_callable: AtomicBool::new(false),
        })
    }
    // Set only by the real runner after its final positive/disabled/read-only
    // tool boundary is installed. The app cannot promote this observation.
    pub(super) fn finalize_tools(&self, tools: &dyn ToolExecutor) {
        self.reader_callable
            .store(tools.owns_exact_tool("skills_read"), Ordering::Release);
    }
    pub fn reader_callable(&self) -> bool {
        self.reader_callable.load(Ordering::Acquire)
    }
    pub fn session_id(&self) -> &str {
        &self.owner.fence.actor_id
    }
    pub fn cancel_token(&self) -> &CancellationToken {
        &self.cancel
    }
    pub fn check_session(&self, session: &Session) -> Result<(), AgentError> {
        if self.cancel.is_cancelled() {
            return Err(AgentError::Cancelled);
        }
        if session.kind != SessionKind::Child
            || !session.authority_identity.is_ordinary()
            || session.id != self.owner.fence.actor_id
            || session.created_at != self.owner.created_at
            || session.root_session_id != self.owner.root_session_id
            || session.parent_session_id.as_deref() != Some(self.owner.parent_session_id.as_str())
            || session.parent_created_at != Some(self.owner.parent_created_at)
            || session.spawn_depth != self.owner.spawn_depth
            || session.project_id_meta() != self.owner.project_id
            || skill_host::strict_child(session)?
        {
            return Err(failure("ordinary local Child Skills identity changed"));
        }
        Ok(())
    }
    /// Fresh canonical scope plus the same live Actor fence/ancestor check used
    /// by local tools. This guarded check publishes no event or checkpoint.
    pub async fn current_session(&self) -> Result<Session, AgentError> {
        let session = self
            .store
            .load_session(self.session_id())
            .await
            .map_err(failure)?
            .ok_or_else(|| failure("ordinary local Child disappeared"))?;
        self.check_session(&session)?;
        self.store
            .publish_child_actor_runtime_event(&self.owner, Box::new(|check| check()))
            .await
            .map_err(failure)?;
        self.check_session(&session)?;
        Ok(session)
    }
}

/// The app supplies its existing catalog/read services; Engine retains the
/// real admission, writer, task lifetime and outer local permission ceiling.
pub struct LocalChildSkillBinding {
    pub host: Arc<dyn SkillExecutionHost>,
    pub tools: Arc<dyn ToolExecutor>,
}
#[async_trait]
pub trait LocalChildSkillFactory: Send + Sync {
    async fn bind(
        &self,
        authority: Arc<LocalChildSkillAuthority>,
        session: &Session,
        tools: Arc<dyn ToolExecutor>,
    ) -> Result<LocalChildSkillBinding, AgentError>;
}
