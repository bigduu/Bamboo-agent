use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use bamboo_llm::Config;
use bamboo_skills::access_control::{SkillAccessError, SkillSessionPort};
use bamboo_skills::runtime_metadata::validate_pinned_activation_metadata;
use bamboo_skills::{SkillManager, SkillStore};

use bamboo_agent_core::tools::ToolError;
use bamboo_agent_core::Session;

mod load_skill;
mod read_resource;

#[cfg(test)]
mod tests;

pub use load_skill::LoadSkillTool;
pub use read_resource::ReadSkillResourceTool;

pub(super) const MAX_RESOURCE_CONTENT_CHARS: usize = 50_000;

#[derive(Clone)]
pub(super) struct SkillToolAccess {
    pub(super) skill_manager: Arc<SkillManager>,
    config: Arc<RwLock<Config>>,
    pub(super) session_repo: bamboo_engine::SessionRepository,
}

impl SkillToolAccess {
    pub(super) fn new(
        skill_manager: Arc<SkillManager>,
        config: Arc<RwLock<Config>>,
        session_repo: bamboo_engine::SessionRepository,
    ) -> Self {
        Self {
            skill_manager,
            config,
            session_repo,
        }
    }

    pub(super) async fn session_for_context(&self, session_id: Option<&str>) -> Option<Session> {
        self.session_repo.load(session_id?).await
    }

    pub(super) async fn skill_store(
        &self,
        session_id: Option<&str>,
    ) -> Result<Arc<SkillStore>, ToolError> {
        let session_id = session_id.ok_or_else(|| {
            ToolError::Execution(
                "Skill runtime tools require a session_id in tool context".to_string(),
            )
        })?;
        let session = self
            .session_for_context(Some(session_id))
            .await
            .ok_or_else(|| {
                ToolError::Execution(format!(
                    "Session '{session_id}' not found while resolving skill workspace"
                ))
            })?;
        let workspace = session.workspace_path_meta().map(PathBuf::from);
        self.skill_manager
            .store_for_workspace(workspace.as_deref())
            .await
            .map_err(|error| {
                ToolError::Execution(format!("Failed to resolve session skill store: {error}"))
            })
    }
}

/// Validate Bamboo runner-owned immutable activation metadata. Returns `false`
/// only for legacy/direct-tool callers that have no generation marker and may
/// establish their pin lazily.
pub(super) async fn validate_runtime_activation(
    access: &SkillToolAccess,
    store: &SkillStore,
    session_id: &str,
    skill_id: &str,
) -> Result<bool, ToolError> {
    let session = access
        .session_for_context(Some(session_id))
        .await
        .ok_or_else(|| ToolError::Execution(format!("Session '{session_id}' not found")))?;
    let descriptor = store.activation_descriptor(session_id).await;
    let validated =
        validate_pinned_activation_metadata(&session.metadata, descriptor.as_ref(), Some(skill_id))
            .map_err(ToolError::Execution)?;
    if validated {
        bamboo_skills::access_control::ensure_skill_enabled(access, skill_id)
            .await
            .map_err(skill_access_error_to_tool_error)?;
    }
    Ok(validated)
}

pub(super) async fn validate_runtime_activation_descriptor(
    access: &SkillToolAccess,
    descriptor: &bamboo_skills::SkillActivationDescriptor,
    session_id: &str,
    skill_id: &str,
) -> Result<bool, ToolError> {
    let session = access
        .session_for_context(Some(session_id))
        .await
        .ok_or_else(|| ToolError::Execution(format!("Session '{session_id}' not found")))?;
    validate_pinned_activation_metadata(&session.metadata, Some(descriptor), Some(skill_id))
        .map_err(ToolError::Execution)
}

#[async_trait]
impl SkillSessionPort for SkillToolAccess {
    async fn load_session_metadata(&self, session_id: &str) -> Option<HashMap<String, String>> {
        self.session_for_context(Some(session_id))
            .await
            .map(|session| session.metadata.clone())
    }

    async fn save_metadata_updates(
        &self,
        session_id: &str,
        updates: &[(String, Option<String>)],
    ) -> Result<(), String> {
        let mut session = self
            .session_repo
            .try_load(session_id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("Session '{session_id}' not found"))?;

        for (key, value) in updates {
            if let Some(val) = value {
                session.metadata.insert(key.clone(), val.clone());
            } else {
                session.metadata.remove(key);
            }
        }

        self.session_repo
            .save(&mut session)
            .await
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    async fn disabled_skill_ids(&self) -> HashSet<String> {
        let config = self.config.read().await;
        config.disabled_skill_ids().into_iter().collect()
    }
}

pub(super) fn skill_access_error_to_tool_error(error: SkillAccessError) -> ToolError {
    match error {
        SkillAccessError::NotAllowed(msg)
        | SkillAccessError::NotLoaded(msg)
        | SkillAccessError::SessionRequired(msg)
        | SkillAccessError::SessionNotFound(msg)
        | SkillAccessError::PersistenceError(msg) => ToolError::Execution(msg),
    }
}
