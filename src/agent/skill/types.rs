//! Skill types and shared data structures.

use serde::{Deserialize, Serialize};

/// Unique identifier for a skill (kebab-case)
pub type SkillId = String;

/// Complete definition of a skill
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillDefinition {
    /// Unique identifier (kebab-case)
    pub id: SkillId,

    /// Display name
    pub name: String,

    /// Human-readable description
    pub description: String,

    /// Optional license information from SKILL.md frontmatter
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,

    /// Optional compatibility notes from SKILL.md frontmatter
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compatibility: Option<String>,

    /// Optional arbitrary metadata from SKILL.md frontmatter
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,

    /// Prompt fragment injected into system prompt
    pub prompt: String,

    /// Built-in tool references (format: "tool")
    #[serde(default)]
    pub tool_refs: Vec<String>,
}

impl SkillDefinition {
    /// Create a new skill definition.
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        description: impl Into<String>,
        prompt: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            description: description.into(),
            license: None,
            compatibility: None,
            metadata: None,
            prompt: prompt.into(),
            tool_refs: Vec::new(),
        }
    }

    /// Add a tool reference
    pub fn with_tool_ref(mut self, tool_ref: impl Into<String>) -> Self {
        self.tool_refs.push(tool_ref.into());
        self
    }

    /// Check if this is a built-in skill (based on id prefix).
    pub fn is_builtin(&self) -> bool {
        self.id.starts_with("builtin-") || self.id.starts_with("system-")
    }
}

/// Configuration for skill store persistence
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillStoreConfig {
    pub skills_dir: std::path::PathBuf,
}

impl Default for SkillStoreConfig {
    fn default() -> Self {
        Self {
            // Keep runtime path resolution consistent across the codebase:
            // use BAMBOO_DATA_DIR (or `${HOME}/.bamboo`) as the single storage root.
            skills_dir: crate::core::paths::bamboo_dir().join("skills"),
        }
    }
}

/// Filter options for listing skills
#[derive(Debug, Clone, Default)]
pub struct SkillFilter {
    /// Search in name and description
    pub search: Option<String>,
}

impl SkillFilter {
    /// Create a new empty filter
    pub fn new() -> Self {
        Self::default()
    }

    /// Set search query
    pub fn with_search(mut self, search: impl Into<String>) -> Self {
        self.search = Some(search.into());
        self
    }

    /// Check if a skill matches this filter
    pub fn matches(&self, skill: &SkillDefinition) -> bool {
        if let Some(ref search) = self.search {
            let search_lower = search.to_lowercase();
            if !skill.name.to_lowercase().contains(&search_lower)
                && !skill.description.to_lowercase().contains(&search_lower)
            {
                return false;
            }
        }

        true
    }
}

/// Error types for skill operations
#[derive(Debug, thiserror::Error)]
pub enum SkillError {
    #[error("Skill not found: {0}")]
    NotFound(SkillId),

    #[error("Skill already exists: {0}")]
    AlreadyExists(SkillId),

    #[error("Invalid skill ID: {0}")]
    InvalidId(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Read-only: {0}")]
    ReadOnly(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("YAML error: {0}")]
    Yaml(#[from] serde_yaml::Error),
}

/// Result type for skill operations
pub type SkillResult<T> = Result<T, SkillError>;
