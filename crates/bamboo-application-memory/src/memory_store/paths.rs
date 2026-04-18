use std::path::{Path, PathBuf};

use bamboo_infrastructure_config::paths::bamboo_dir;

use super::types::MemoryScope;

pub const MEMORY_ROOT_DIR: &str = "memory";
pub const MEMORY_VERSION_DIR: &str = "v1";
pub const SESSIONS_DIR: &str = "sessions";
pub const SCOPES_DIR: &str = "scopes";
pub const GLOBAL_DIR: &str = "global";
pub const PROJECTS_DIR: &str = "projects";
pub const NOTE_DIR: &str = "note";
pub const STATE_DIR: &str = "state";
pub const INDEXES_DIR: &str = "indexes";
pub const VIEWS_DIR: &str = "views";
pub const LOGS_DIR: &str = "logs";
pub const LOCKS_DIR: &str = "locks";
pub const TOPICS_DIR: &str = "topics";

#[derive(Debug, Clone)]
pub struct MemoryPathResolver {
    data_dir: PathBuf,
    root: PathBuf,
}

impl Default for MemoryPathResolver {
    fn default() -> Self {
        Self::from_data_dir(bamboo_dir())
    }
}

impl MemoryPathResolver {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        let data_dir = infer_data_dir_from_root(&root);
        Self { data_dir, root }
    }

    pub fn from_data_dir(data_dir: impl Into<PathBuf>) -> Self {
        let data_dir = data_dir.into();
        let root = data_dir.join(MEMORY_ROOT_DIR).join(MEMORY_VERSION_DIR);
        Self { data_dir, root }
    }

    pub fn data_dir(&self) -> PathBuf {
        self.data_dir.clone()
    }

    pub fn root(&self) -> PathBuf {
        self.root.clone()
    }

    pub fn sessions_root(&self) -> PathBuf {
        self.root.join(SESSIONS_DIR)
    }

    pub fn session_root(&self, session_id: &str) -> PathBuf {
        self.sessions_root().join(session_id)
    }

    pub fn session_note_dir(&self, session_id: &str) -> PathBuf {
        self.session_root(session_id).join(NOTE_DIR)
    }

    pub fn session_topic_path(&self, session_id: &str, topic: &str) -> PathBuf {
        self.session_note_dir(session_id)
            .join(format!("{}.md", topic))
    }

    pub fn session_state_path(&self, session_id: &str) -> PathBuf {
        self.session_root(session_id).join("state.json")
    }

    pub fn scopes_root(&self) -> PathBuf {
        self.root.join(SCOPES_DIR)
    }

    pub fn global_root(&self) -> PathBuf {
        self.scopes_root().join(GLOBAL_DIR)
    }

    pub fn project_root(&self, project_key: &str) -> PathBuf {
        self.scopes_root().join(PROJECTS_DIR).join(project_key)
    }

    pub fn scope_root(&self, scope: MemoryScope, project_key: Option<&str>) -> PathBuf {
        match scope {
            MemoryScope::Global => self.global_root(),
            MemoryScope::Project => self.project_root(project_key.unwrap_or("unknown")),
            MemoryScope::Session => self.sessions_root(),
        }
    }

    pub fn topic_dir(&self, scope: MemoryScope, project_key: Option<&str>) -> PathBuf {
        match scope {
            MemoryScope::Global => self.global_root().join(TOPICS_DIR),
            MemoryScope::Project => self
                .project_root(project_key.unwrap_or("unknown"))
                .join(TOPICS_DIR),
            MemoryScope::Session => self.sessions_root(),
        }
    }

    pub fn topic_path(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
        memory_id: &str,
    ) -> PathBuf {
        self.topic_dir(scope, project_key)
            .join(format!("{}.md", memory_id))
    }

    pub fn indexes_dir(&self, scope: MemoryScope, project_key: Option<&str>) -> PathBuf {
        match scope {
            MemoryScope::Global => self.global_root().join(INDEXES_DIR),
            MemoryScope::Project => self
                .project_root(project_key.unwrap_or("unknown"))
                .join(INDEXES_DIR),
            MemoryScope::Session => self.sessions_root(),
        }
    }

    pub fn views_dir(&self, scope: MemoryScope, project_key: Option<&str>) -> PathBuf {
        match scope {
            MemoryScope::Global => self.global_root().join(VIEWS_DIR),
            MemoryScope::Project => self
                .project_root(project_key.unwrap_or("unknown"))
                .join(VIEWS_DIR),
            MemoryScope::Session => self.sessions_root(),
        }
    }

    pub fn logs_dir(&self, scope: MemoryScope, project_key: Option<&str>) -> PathBuf {
        match scope {
            MemoryScope::Global => self.global_root().join(LOGS_DIR),
            MemoryScope::Project => self
                .project_root(project_key.unwrap_or("unknown"))
                .join(LOGS_DIR),
            MemoryScope::Session => self.sessions_root(),
        }
    }

    pub fn state_dir(&self, scope: MemoryScope, project_key: Option<&str>) -> PathBuf {
        match scope {
            MemoryScope::Global => self.global_root().join(STATE_DIR),
            MemoryScope::Project => self
                .project_root(project_key.unwrap_or("unknown"))
                .join(STATE_DIR),
            MemoryScope::Session => self.sessions_root(),
        }
    }

    pub fn locks_dir(&self, scope: MemoryScope, project_key: Option<&str>) -> PathBuf {
        self.state_dir(scope, project_key).join(LOCKS_DIR)
    }

    pub fn legacy_notes_root(&self) -> PathBuf {
        self.data_dir.join("notes")
    }

    pub fn legacy_session_file(&self, session_id: &str) -> PathBuf {
        self.legacy_notes_root().join(format!("{}.md", session_id))
    }

    pub fn legacy_session_topic_dir(&self, session_id: &str) -> PathBuf {
        self.legacy_notes_root().join(session_id)
    }
}

pub fn memory_root() -> PathBuf {
    MemoryPathResolver::from_data_dir(bamboo_dir()).root()
}

fn infer_data_dir_from_root(root: &Path) -> PathBuf {
    if root
        .file_name()
        .and_then(|value| value.to_str())
        .is_some_and(|value| value == MEMORY_VERSION_DIR)
    {
        if let Some(memory_dir) = root.parent() {
            if memory_dir
                .file_name()
                .and_then(|value| value.to_str())
                .is_some_and(|value| value == MEMORY_ROOT_DIR)
            {
                if let Some(data_dir) = memory_dir.parent() {
                    return data_dir.to_path_buf();
                }
            }
        }
    }
    root.to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_follow_v1_layout() {
        let resolver = MemoryPathResolver::new("/tmp/memory/v1");
        assert_eq!(
            resolver.session_topic_path("session-1", "default"),
            PathBuf::from("/tmp/memory/v1/sessions/session-1/note/default.md")
        );
        assert_eq!(
            resolver.topic_path(MemoryScope::Project, Some("proj-1"), "mem_1"),
            PathBuf::from("/tmp/memory/v1/scopes/projects/proj-1/topics/mem_1.md")
        );
    }
}
