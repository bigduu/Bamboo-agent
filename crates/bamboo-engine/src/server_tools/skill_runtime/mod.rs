use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::access_control::{SkillAccessError, SkillSessionPort};
use crate::SkillManager;
use bamboo_infrastructure::Config;

use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::ToolError;
use bamboo_agent_core::Session;
use bamboo_infrastructure::LockedSessionStore;

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
    pub(super) sessions: crate::SessionCache,
    storage: Arc<dyn Storage>,
    pub(super) persistence: Arc<LockedSessionStore>,
}

impl SkillToolAccess {
    pub(super) fn new(
        skill_manager: Arc<SkillManager>,
        config: Arc<RwLock<Config>>,
        sessions: crate::SessionCache,
        storage: Arc<dyn Storage>,
        persistence: Arc<LockedSessionStore>,
    ) -> Self {
        Self {
            skill_manager,
            config,
            sessions,
            storage,
            persistence,
        }
    }

    pub(super) async fn session_for_context(&self, session_id: Option<&str>) -> Option<Session> {
        let session_id = session_id?;

        let in_memory = {
            let arc = self.sessions.get(session_id).map(|e| e.value().clone());
            arc.map(|a| a.read().clone())
        };

        match in_memory {
            Some(session) => Some(session),
            None => self.storage.load_session(session_id).await.ok().flatten(),
        }
    }

    pub(super) async fn skill_root(
        &self,
        skill_id: &str,
        skill_mode: Option<&str>,
    ) -> Result<PathBuf, ToolError> {
        self.skill_manager
            .store()
            .get_skill_root_for_mode(skill_id, skill_mode)
            .await
            .map_err(|err| ToolError::Execution(format!("Failed to resolve skill root: {err}")))
    }
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
        let mut session = {
            let arc = self.sessions.get(session_id).map(|e| e.value().clone());
            arc.map(|a| a.read().clone())
        };

        if session.is_none() {
            session = self
                .storage
                .load_session(session_id)
                .await
                .map_err(|e| e.to_string())?;
        }

        let mut session = session.ok_or_else(|| format!("Session '{session_id}' not found"))?;

        for (key, value) in updates {
            if let Some(val) = value {
                session.metadata.insert(key.clone(), val.clone());
            } else {
                session.metadata.remove(key);
            }
        }

        self.persistence
            .merge_save_runtime(&mut session)
            .await
            .map_err(|e| e.to_string())?;

        self.sessions.insert(
            session_id.to_string(),
            Arc::new(parking_lot::RwLock::new(session)),
        );

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
