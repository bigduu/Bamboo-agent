//! Skill types and shared data structures.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Unique identifier for a skill (kebab-case)
pub type SkillId = String;

/// Visibility level for a skill
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum SkillVisibility {
    /// Visible to all users
    #[default]
    Public,
    /// Private to the creator
    Private,
}


/// Complete definition of a skill
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillDefinition {
    /// Unique identifier (kebab-case)
    pub id: SkillId,

    /// Display name
    pub name: String,

    /// Human-readable description
    pub description: String,

    /// Category for grouping
    pub category: String,

    /// Searchable tags
    #[serde(default)]
    pub tags: Vec<String>,

    /// Prompt fragment injected into system prompt
    pub prompt: String,

    /// Built-in tool references (format: "tool")
    #[serde(default)]
    pub tool_refs: Vec<String>,

    /// Associated workflow names
    #[serde(default)]
    pub workflow_refs: Vec<String>,

    /// Visibility level
    #[serde(default)]
    pub visibility: SkillVisibility,

    /// Semantic version
    #[serde(default = "default_version")]
    pub version: String,

    /// Creation timestamp
    pub created_at: DateTime<Utc>,

    /// Last update timestamp
    pub updated_at: DateTime<Utc>,
}

fn default_version() -> String {
    "1.0.0".to_string()
}

impl SkillDefinition {
    /// Create a new skill definition with generated timestamp
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        description: impl Into<String>,
        category: impl Into<String>,
        prompt: impl Into<String>,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: id.into(),
            name: name.into(),
            description: description.into(),
            category: category.into(),
            tags: Vec::new(),
            prompt: prompt.into(),
            tool_refs: Vec::new(),
            workflow_refs: Vec::new(),
            visibility: SkillVisibility::default(),
            version: default_version(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Add a tool reference
    pub fn with_tool_ref(mut self, tool_ref: impl Into<String>) -> Self {
        self.tool_refs.push(tool_ref.into());
        self
    }

    /// Add a workflow reference
    pub fn with_workflow_ref(mut self, workflow_ref: impl Into<String>) -> Self {
        self.workflow_refs.push(workflow_ref.into());
        self
    }

    /// Add a tag
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Set visibility
    pub fn with_visibility(mut self, visibility: SkillVisibility) -> Self {
        self.visibility = visibility;
        self
    }

    /// Update the timestamp
    pub fn touch(&mut self) {
        self.updated_at = Utc::now();
    }

    /// Check if this is a built-in skill (based on id prefix or version)
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
        let home = dirs::home_dir().unwrap_or_else(|| std::path::PathBuf::from("."));
        Self {
            skills_dir: home.join(".bamboo").join("skills"),
        }
    }
}

/// Filter options for listing skills
#[derive(Debug, Clone, Default)]
pub struct SkillFilter {
    /// Filter by category
    pub category: Option<String>,

    /// Filter by tags (any match)
    pub tags: Vec<String>,

    /// Search in name and description
    pub search: Option<String>,

    /// Filter by visibility
    pub visibility: Option<SkillVisibility>,
}

impl SkillFilter {
    /// Create a new empty filter
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter by category
    pub fn with_category(mut self, category: impl Into<String>) -> Self {
        self.category = Some(category.into());
        self
    }

    /// Add a tag filter
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Set search query
    pub fn with_search(mut self, search: impl Into<String>) -> Self {
        self.search = Some(search.into());
        self
    }

    /// Check if a skill matches this filter
    pub fn matches(&self, skill: &SkillDefinition) -> bool {
        if let Some(ref category) = self.category {
            if skill.category != *category {
                return false;
            }
        }

        if !self.tags.is_empty() {
            let has_matching_tag = self.tags.iter().any(|tag| skill.tags.contains(tag));
            if !has_matching_tag {
                return false;
            }
        }

        if let Some(ref search) = self.search {
            let search_lower = search.to_lowercase();
            if !skill.name.to_lowercase().contains(&search_lower)
                && !skill.description.to_lowercase().contains(&search_lower)
            {
                return false;
            }
        }

        if let Some(ref visibility) = self.visibility {
            if skill.visibility != *visibility {
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
