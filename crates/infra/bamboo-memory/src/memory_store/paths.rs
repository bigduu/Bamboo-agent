use std::path::{Path, PathBuf};

use bamboo_config::paths::bamboo_dir;
use bamboo_domain::ProjectId;

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
pub const TOPICS_DIR: &str = "topics";

#[derive(Debug, Clone)]
pub struct MemoryPathResolver {
    data_dir: PathBuf,
    root: PathBuf,
    project_id: Option<ProjectId>,
    legacy_project_read_roots: Vec<LegacyProjectMemoryReadRoot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyProjectMemoryReadRoot {
    pub project_key: String,
    pub root: PathBuf,
}

/// Explicit first-class Project memory layout.
///
/// This resolver is intentionally separate from the legacy path-hash
/// `MemoryPathResolver::project_root(&str)`: a safe legacy key and a ProjectId
/// can have the same character shape, so guessing by string format would move
/// old scopes silently. Assigned-session writes must use this typed resolver;
/// legacy roots remain read-only migration aliases.
#[derive(Debug, Clone)]
pub struct ProjectMemoryPathResolver {
    data_dir: PathBuf,
}

impl ProjectMemoryPathResolver {
    pub fn from_data_dir(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
        }
    }

    pub fn project_home(&self, project_id: &ProjectId) -> PathBuf {
        self.data_dir.join(PROJECTS_DIR).join(project_id.as_str())
    }

    pub fn memory_root(&self, project_id: &ProjectId) -> PathBuf {
        self.project_home(project_id)
            .join(MEMORY_ROOT_DIR)
            .join(MEMORY_VERSION_DIR)
    }

    pub fn legacy_read_root(&self, legacy_project_key: &str) -> std::io::Result<PathBuf> {
        let legacy_project_key = legacy_project_key.trim();
        if legacy_project_key.is_empty()
            || legacy_project_key.contains('/')
            || legacy_project_key.contains('\\')
            || legacy_project_key == "."
            || legacy_project_key == ".."
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "legacy project key is not a safe path component",
            ));
        }
        Ok(self
            .data_dir
            .join(MEMORY_ROOT_DIR)
            .join(MEMORY_VERSION_DIR)
            .join(SCOPES_DIR)
            .join(PROJECTS_DIR)
            .join(legacy_project_key))
    }
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
        Self {
            data_dir,
            root,
            project_id: None,
            legacy_project_read_roots: Vec::new(),
        }
    }

    pub fn from_data_dir(data_dir: impl Into<PathBuf>) -> Self {
        let data_dir = data_dir.into();
        let root = data_dir.join(MEMORY_ROOT_DIR).join(MEMORY_VERSION_DIR);
        Self {
            data_dir,
            root,
            project_id: None,
            legacy_project_read_roots: Vec::new(),
        }
    }

    /// Bind Project-scope paths to one validated first-class Project id.
    ///
    /// This is explicit rather than inferred from an arbitrary string key, so
    /// legacy path-hash keys keep their original layout.
    pub fn for_project(&self, project_id: &ProjectId) -> Self {
        Self {
            data_dir: self.data_dir.clone(),
            root: self.root.clone(),
            project_id: Some(project_id.clone()),
            legacy_project_read_roots: Vec::new(),
        }
    }

    pub fn for_project_with_legacy_read_roots(
        &self,
        project_id: &ProjectId,
        legacy_project_read_roots: Vec<LegacyProjectMemoryReadRoot>,
    ) -> Self {
        Self {
            data_dir: self.data_dir.clone(),
            root: self.root.clone(),
            project_id: Some(project_id.clone()),
            legacy_project_read_roots,
        }
    }

    pub fn project_id(&self) -> Option<&ProjectId> {
        self.project_id.as_ref()
    }

    pub fn has_legacy_project_read_roots(&self) -> bool {
        !self.legacy_project_read_roots.is_empty()
    }

    pub fn project_read_roots(&self, project_key: &str) -> Vec<PathBuf> {
        if self
            .project_id
            .as_ref()
            .is_some_and(|project_id| project_id.as_str() == project_key)
        {
            let mut roots = vec![self.project_root(project_key)];
            roots.extend(
                self.legacy_project_read_roots
                    .iter()
                    .map(|legacy| legacy.root.clone()),
            );
            return roots;
        }
        vec![self.project_root(project_key)]
    }

    pub fn is_legacy_project_read_path(&self, path: &Path) -> bool {
        self.legacy_project_read_roots
            .iter()
            .any(|legacy| path.starts_with(&legacy.root))
    }

    pub fn scope_read_roots(&self, scope: MemoryScope, project_key: Option<&str>) -> Vec<PathBuf> {
        match scope {
            MemoryScope::Project => self.project_read_roots(project_key.unwrap_or("unknown")),
            _ => vec![self.scope_root(scope, project_key)],
        }
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
        if self
            .project_id
            .as_ref()
            .is_some_and(|project_id| project_id.as_str() == project_key)
        {
            return ProjectMemoryPathResolver::from_data_dir(self.data_dir.clone())
                .memory_root(self.project_id.as_ref().expect("checked Project id"));
        }
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

    #[test]
    fn typed_project_memory_uses_project_home_without_guessing_legacy_keys() {
        let resolver = ProjectMemoryPathResolver::from_data_dir("/tmp/bamboo");
        let project_id = ProjectId::parse("01JABCDEF0123456789ABCDEFG").expect("id");
        assert_eq!(
            resolver.memory_root(&project_id),
            PathBuf::from("/tmp/bamboo/projects/01JABCDEF0123456789ABCDEFG/memory/v1")
        );
        let scoped = MemoryPathResolver::from_data_dir("/tmp/bamboo").for_project(&project_id);
        assert_eq!(
            scoped.topic_dir(MemoryScope::Project, Some(project_id.as_str())),
            PathBuf::from("/tmp/bamboo/projects/01JABCDEF0123456789ABCDEFG/memory/v1/topics")
        );
        assert_eq!(
            resolver
                .legacy_read_root("zenith-deadbeef")
                .expect("legacy"),
            PathBuf::from("/tmp/bamboo/memory/v1/scopes/projects/zenith-deadbeef")
        );
        assert!(resolver.legacy_read_root("../escape").is_err());
    }
}
