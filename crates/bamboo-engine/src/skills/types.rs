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
    /// Global skills directory (for example: `${BAMBOO_DATA_DIR}/skills`).
    pub skills_dir: std::path::PathBuf,
    /// Optional workspace root used for project-local skills discovery.
    ///
    /// When set, Bamboo also discovers skills from:
    /// - `<project_dir>/.bamboo/skills`
    /// - `<project_dir>/.bamboo/skills-<active_mode>` (when `active_mode` is set)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_dir: Option<std::path::PathBuf>,
    /// Optional active mode slug for mode-specific skill overrides.
    ///
    /// When set, Bamboo also discovers:
    /// - `${BAMBOO_DATA_DIR}/skills-<active_mode>`
    /// - `<project_dir>/.bamboo/skills-<active_mode>` (if project_dir is set)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_mode: Option<String>,
}

impl Default for SkillStoreConfig {
    fn default() -> Self {
        Self {
            // Keep runtime path resolution consistent across the codebase:
            // use BAMBOO_DATA_DIR (or `${HOME}/.bamboo`) as the single storage root.
            skills_dir: bamboo_infrastructure::paths::bamboo_dir().join("skills"),
            project_dir: None,
            active_mode: None,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_skill_definition_new() {
        let skill = SkillDefinition::new(
            "test-skill",
            "Test Skill",
            "A test skill description",
            "Test prompt",
        );

        assert_eq!(skill.id, "test-skill");
        assert_eq!(skill.name, "Test Skill");
        assert_eq!(skill.description, "A test skill description");
        assert_eq!(skill.prompt, "Test prompt");
        assert!(skill.license.is_none());
        assert!(skill.compatibility.is_none());
        assert!(skill.metadata.is_none());
        assert!(skill.tool_refs.is_empty());
    }

    #[test]
    fn test_skill_definition_with_tool_ref() {
        let skill = SkillDefinition::new("skill-1", "Skill", "Description", "Prompt")
            .with_tool_ref("tool-1")
            .with_tool_ref("tool-2");

        assert_eq!(skill.tool_refs.len(), 2);
        assert_eq!(skill.tool_refs[0], "tool-1");
        assert_eq!(skill.tool_refs[1], "tool-2");
    }

    #[test]
    fn test_skill_definition_is_builtin() {
        let builtin1 = SkillDefinition::new("builtin-test", "", "", "");
        assert!(builtin1.is_builtin());

        let builtin2 = SkillDefinition::new("system-test", "", "", "");
        assert!(builtin2.is_builtin());

        let custom = SkillDefinition::new("custom-test", "", "", "");
        assert!(!custom.is_builtin());
    }

    #[test]
    fn test_skill_definition_serialization() {
        let skill = SkillDefinition {
            id: "test-id".to_string(),
            name: "Test".to_string(),
            description: "Desc".to_string(),
            license: Some("MIT".to_string()),
            compatibility: None,
            metadata: Some(serde_json::json!({"key": "value"})),
            prompt: "Prompt".to_string(),
            tool_refs: vec!["tool-1".to_string()],
        };

        let json = serde_json::to_string(&skill).unwrap();
        assert!(json.contains("\"id\":\"test-id\""));
        assert!(json.contains("\"license\":\"MIT\""));
        assert!(json.contains("\"tool_refs\":[\"tool-1\"]"));
    }

    #[test]
    fn test_skill_definition_deserialization() {
        let json = r#"{
            "id": "skill-1",
            "name": "Test Skill",
            "description": "A test",
            "prompt": "Test prompt",
            "tool_refs": ["bash"]
        }"#;

        let skill: SkillDefinition = serde_json::from_str(json).unwrap();
        assert_eq!(skill.id, "skill-1");
        assert_eq!(skill.tool_refs.len(), 1);
    }

    #[test]
    fn test_skill_definition_debug() {
        let skill = SkillDefinition::new("test", "", "", "");
        let debug_str = format!("{:?}", skill);
        assert!(debug_str.contains("SkillDefinition"));
        assert!(debug_str.contains("test"));
    }

    #[test]
    fn test_skill_definition_clone() {
        let skill1 = SkillDefinition::new("test", "Name", "Desc", "Prompt");
        let skill2 = skill1.clone();
        assert_eq!(skill1.id, skill2.id);
        assert_eq!(skill1.name, skill2.name);
    }

    #[test]
    fn test_skill_store_config_default() {
        let config = SkillStoreConfig::default();
        assert!(config.skills_dir.to_str().unwrap().contains("skills"));
    }

    #[test]
    fn test_skill_store_config_debug() {
        let config = SkillStoreConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("SkillStoreConfig"));
    }

    #[test]
    fn test_skill_store_config_clone() {
        let config1 = SkillStoreConfig::default();
        let config2 = config1.clone();
        assert_eq!(config1.skills_dir, config2.skills_dir);
    }

    #[test]
    fn test_skill_filter_new() {
        let filter = SkillFilter::new();
        assert!(filter.search.is_none());
    }

    #[test]
    fn test_skill_filter_with_search() {
        let filter = SkillFilter::new().with_search("test");
        assert_eq!(filter.search, Some("test".to_string()));
    }

    #[test]
    fn test_skill_filter_matches_no_search() {
        let filter = SkillFilter::new();
        let skill = SkillDefinition::new("test", "", "", "");
        assert!(filter.matches(&skill));
    }

    #[test]
    fn test_skill_filter_matches_by_name() {
        let filter = SkillFilter::new().with_search("test");
        let skill = SkillDefinition::new("id", "Test Skill", "Description", "Prompt");
        assert!(filter.matches(&skill));
    }

    #[test]
    fn test_skill_filter_matches_by_description() {
        let filter = SkillFilter::new().with_search("search");
        let skill = SkillDefinition::new("id", "Name", "Search here", "Prompt");
        assert!(filter.matches(&skill));
    }

    #[test]
    fn test_skill_filter_no_match() {
        let filter = SkillFilter::new().with_search("xyz");
        let skill = SkillDefinition::new("id", "Name", "Description", "Prompt");
        assert!(!filter.matches(&skill));
    }

    #[test]
    fn test_skill_filter_case_insensitive() {
        let filter = SkillFilter::new().with_search("TEST");
        let skill = SkillDefinition::new("id", "test skill", "Desc", "Prompt");
        assert!(filter.matches(&skill));
    }

    #[test]
    fn test_skill_error_not_found() {
        let err = SkillError::NotFound("skill-123".to_string());
        let msg = err.to_string();
        assert!(msg.contains("Skill not found"));
        assert!(msg.contains("skill-123"));
    }

    #[test]
    fn test_skill_error_already_exists() {
        let err = SkillError::AlreadyExists("skill-456".to_string());
        let msg = err.to_string();
        assert!(msg.contains("Skill already exists"));
    }

    #[test]
    fn test_skill_error_invalid_id() {
        let err = SkillError::InvalidId("bad id".to_string());
        let msg = err.to_string();
        assert!(msg.contains("Invalid skill ID"));
    }

    #[test]
    fn test_skill_error_validation() {
        let err = SkillError::Validation("Missing field".to_string());
        let msg = err.to_string();
        assert!(msg.contains("Validation error"));
    }

    #[test]
    fn test_skill_error_storage() {
        let err = SkillError::Storage("Disk full".to_string());
        let msg = err.to_string();
        assert!(msg.contains("Storage error"));
    }

    #[test]
    fn test_skill_error_read_only() {
        let err = SkillError::ReadOnly("Cannot delete".to_string());
        let msg = err.to_string();
        assert!(msg.contains("Read-only"));
    }

    #[test]
    fn test_skill_error_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err = SkillError::Io(io_err);
        let msg = err.to_string();
        assert!(msg.contains("IO error"));
    }

    #[test]
    fn test_skill_error_debug() {
        let err = SkillError::NotFound("test".to_string());
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("NotFound"));
    }
}
