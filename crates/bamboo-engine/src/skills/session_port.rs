use std::collections::{HashMap, HashSet};

/// Port trait for skill access control — abstracts session metadata access
/// and global disabled-skill configuration.
#[async_trait::async_trait]
pub trait SkillSessionPort: Send + Sync {
    async fn load_session_metadata(&self, session_id: &str) -> Option<HashMap<String, String>>;
    async fn save_metadata_updates(
        &self,
        session_id: &str,
        updates: &[(String, Option<String>)],
    ) -> Result<(), String>;
    async fn disabled_skill_ids(&self) -> HashSet<String>;
}
