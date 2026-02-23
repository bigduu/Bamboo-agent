use std::path::Path;

use agent_tools::normalize_tool_ref;
use chrono::{DateTime, Utc};
use log::warn;
use serde::{Deserialize, Serialize};

use crate::types::{SkillDefinition, SkillError, SkillResult, SkillVisibility};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SkillFrontmatter {
    id: String,
    name: String,
    description: String,
    category: String,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    tool_refs: Vec<String>,
    #[serde(default)]
    workflow_refs: Vec<String>,
    visibility: SkillVisibility,
    version: String,
    created_at: String,
    updated_at: String,
}

pub fn parse_markdown_skill(path: &Path, content: &str) -> SkillResult<SkillDefinition> {
    let (frontmatter_raw, body) = split_frontmatter(content)?;
    let frontmatter: SkillFrontmatter = serde_yaml::from_str(&frontmatter_raw)?;

    // Validate that parent directory name matches skill ID
    let dir_name = path
        .parent()
        .and_then(|parent| parent.file_name())
        .and_then(|segment| segment.to_str())
        .unwrap_or_default();
    if dir_name != frontmatter.id {
        return Err(SkillError::Validation(format!(
            "Skill id {} does not match directory name {}",
            frontmatter.id, dir_name
        )));
    }

    if !is_valid_skill_id(&frontmatter.id) {
        return Err(SkillError::InvalidId(format!(
            "Invalid skill ID: {}. Use kebab-case (e.g., my-skill-name)",
            frontmatter.id
        )));
    }

    let created_at = parse_timestamp(&frontmatter.created_at)?;
    let updated_at = parse_timestamp(&frontmatter.updated_at)?;

    let mut tool_refs = Vec::new();
    for tool_ref in frontmatter.tool_refs {
        match normalize_tool_ref(&tool_ref) {
            Some(normalized) => tool_refs.push(normalized),
            None => {
                warn!(
                    "Skipping unsupported tool reference {} in {:?}",
                    tool_ref, path
                );
            }
        }
    }

    Ok(SkillDefinition {
        id: frontmatter.id,
        name: frontmatter.name,
        description: frontmatter.description,
        category: frontmatter.category,
        tags: frontmatter.tags,
        prompt: body.trim().to_string(),
        tool_refs,
        workflow_refs: frontmatter.workflow_refs,
        visibility: frontmatter.visibility,
        version: frontmatter.version,
        created_at,
        updated_at,
    })
}

pub fn split_frontmatter(content: &str) -> SkillResult<(String, String)> {
    let mut lines = content.lines();
    match lines.next() {
        Some("---") => {}
        _ => {
            return Err(SkillError::Validation(
                "Missing YAML frontmatter".to_string(),
            ))
        }
    }

    let mut frontmatter_lines = Vec::new();
    for line in lines.by_ref() {
        if line == "---" {
            break;
        }
        frontmatter_lines.push(line);
    }

    let frontmatter = frontmatter_lines.join("\n");
    let body = lines.collect::<Vec<_>>().join("\n");
    Ok((frontmatter, body))
}

pub fn parse_timestamp(value: &str) -> SkillResult<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|date_time| date_time.with_timezone(&Utc))
        .map_err(|error| SkillError::Validation(format!("Invalid timestamp {}: {}", value, error)))
}

pub fn render_skill_markdown(skill: &SkillDefinition) -> SkillResult<String> {
    let frontmatter = SkillFrontmatter {
        id: skill.id.clone(),
        name: skill.name.clone(),
        description: skill.description.clone(),
        category: skill.category.clone(),
        tags: skill.tags.clone(),
        tool_refs: skill.tool_refs.clone(),
        workflow_refs: skill.workflow_refs.clone(),
        visibility: skill.visibility.clone(),
        version: skill.version.clone(),
        created_at: skill.created_at.to_rfc3339(),
        updated_at: skill.updated_at.to_rfc3339(),
    };

    let yaml = serde_yaml::to_string(&frontmatter)?;
    let body = skill.prompt.trim();

    Ok(format!("---\n{}---\n\n{}\n", yaml, body))
}

pub(crate) fn is_valid_skill_id(id: &str) -> bool {
    if id.is_empty() {
        return false;
    }

    if !id
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_lowercase())
    {
        return false;
    }

    id.chars().all(|character| {
        character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
    })
}

#[cfg(test)]
mod tests {
    use super::is_valid_skill_id;

    #[test]
    fn valid_skill_ids() {
        assert!(is_valid_skill_id("my-skill"));
        assert!(is_valid_skill_id("skill123"));
        assert!(is_valid_skill_id("a-b-c"));
        assert!(is_valid_skill_id("builtin-file-analysis"));
    }

    #[test]
    fn invalid_skill_ids() {
        assert!(!is_valid_skill_id(""));
        assert!(!is_valid_skill_id("MySkill"));
        assert!(!is_valid_skill_id("123-skill"));
        assert!(!is_valid_skill_id("my_skill"));
        assert!(!is_valid_skill_id("my skill"));
    }
}
