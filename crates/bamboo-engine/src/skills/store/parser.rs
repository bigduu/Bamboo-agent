use std::path::Path;

use bamboo_domain::normalize_tool_ref;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::skills::types::{SkillDefinition, SkillError, SkillResult};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillFrontmatter {
    name: String,
    description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    license: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    compatibility: Option<String>,
    #[serde(default)]
    #[serde(rename = "allowed-tools", skip_serializing_if = "Vec::is_empty")]
    allowed_tools: Vec<String>,
    #[serde(
        default,
        rename = "argument-hint",
        alias = "argument_hint",
        skip_serializing_if = "Option::is_none"
    )]
    argument_hint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    metadata: Option<serde_json::Value>,
}

pub fn parse_markdown_skill(path: &Path, content: &str) -> SkillResult<SkillDefinition> {
    let (frontmatter_raw, body) = split_frontmatter(content)?;
    let frontmatter: SkillFrontmatter = serde_yaml::from_str(&frontmatter_raw)?;
    let SkillFrontmatter {
        name,
        description,
        license,
        compatibility,
        allowed_tools,
        argument_hint: _argument_hint,
        metadata,
    } = frontmatter;

    // Skill ID comes from directory name.
    let dir_name = path
        .parent()
        .and_then(|parent| parent.file_name())
        .and_then(|segment| segment.to_str())
        .unwrap_or_default();
    if !is_valid_skill_id(dir_name) {
        return Err(SkillError::InvalidId(format!(
            "Invalid skill ID: {}. Use kebab-case (e.g., my-skill-name)",
            dir_name
        )));
    }

    let name = name.trim();
    if name.is_empty() {
        return Err(SkillError::Validation(
            "Skill name cannot be empty".to_string(),
        ));
    }
    validate_skill_name(name)?;
    if !matches_skill_name_directory(name, dir_name) {
        return Err(SkillError::Validation(format!(
            "Skill name '{}' must match directory name '{}' or '<namespace>:{}'",
            name, dir_name, dir_name
        )));
    }

    let description = description.trim();
    if description.is_empty() {
        return Err(SkillError::Validation(
            "Skill description cannot be empty".to_string(),
        ));
    }
    validate_skill_description(description)?;

    let compatibility = compatibility
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    if let Some(value) = compatibility.as_deref() {
        validate_compatibility(value)?;
    }

    let license = license
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    let mut tool_refs = Vec::new();
    for tool_ref in allowed_tools {
        let trimmed = tool_ref.trim();
        if trimmed.is_empty() {
            continue;
        }

        match normalize_tool_ref(trimmed) {
            Some(normalized) => tool_refs.push(normalized),
            None => {
                warn!(
                    "Unrecognized allowed-tool '{}' in {:?}; preserving raw value",
                    trimmed, path
                );
                tool_refs.push(trimmed.to_string());
            }
        }
    }

    Ok(SkillDefinition {
        id: dir_name.to_string(),
        name: name.to_string(),
        description: description.to_string(),
        license,
        compatibility,
        metadata,
        prompt: body.trim().to_string(),
        tool_refs,
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
    let mut found_closing = false;
    for line in lines.by_ref() {
        if line == "---" {
            found_closing = true;
            break;
        }
        frontmatter_lines.push(line);
    }
    if !found_closing {
        return Err(SkillError::Validation(
            "Invalid frontmatter format".to_string(),
        ));
    }

    let frontmatter = frontmatter_lines.join("\n");
    let body = lines.collect::<Vec<_>>().join("\n");
    Ok((frontmatter, body))
}

fn validate_skill_name(name: &str) -> SkillResult<()> {
    if name.chars().any(char::is_whitespace) {
        return Err(SkillError::Validation(format!(
            "Name '{}' cannot contain whitespace",
            name
        )));
    }
    if name.len() > 128 {
        return Err(SkillError::Validation(format!(
            "Name is too long ({} characters). Maximum is 128 characters.",
            name.len()
        )));
    }

    let mut segments = name.split(':');
    let primary = segments.next().unwrap_or_default();
    let secondary = segments.next();
    let extra = segments.next();

    if extra.is_some() {
        return Err(SkillError::Validation(format!(
            "Name '{}' supports at most one namespace separator ':'",
            name
        )));
    }

    if !is_valid_skill_id(primary) {
        return Err(SkillError::Validation(format!(
            "Name '{}' must be kebab-case or '<namespace>:kebab-case'",
            name
        )));
    }

    if let Some(suffix) = secondary {
        if !is_valid_skill_id(suffix) {
            return Err(SkillError::Validation(format!(
                "Name '{}' must be kebab-case or '<namespace>:kebab-case'",
                name
            )));
        }
    }

    Ok(())
}

fn matches_skill_name_directory(name: &str, dir_name: &str) -> bool {
    name == dir_name
        || name
            .rsplit_once(':')
            .is_some_and(|(_, suffix)| suffix == dir_name)
}

fn validate_skill_description(description: &str) -> SkillResult<()> {
    if description.contains('<') || description.contains('>') {
        return Err(SkillError::Validation(
            "Description cannot contain angle brackets (< or >)".to_string(),
        ));
    }
    if description.len() > 1024 {
        return Err(SkillError::Validation(format!(
            "Description is too long ({} characters). Maximum is 1024 characters.",
            description.len()
        )));
    }
    Ok(())
}

fn validate_compatibility(compatibility: &str) -> SkillResult<()> {
    if compatibility.len() > 500 {
        return Err(SkillError::Validation(format!(
            "Compatibility is too long ({} characters). Maximum is 500 characters.",
            compatibility.len()
        )));
    }
    Ok(())
}

pub fn render_skill_markdown(skill: &SkillDefinition) -> SkillResult<String> {
    let frontmatter = SkillFrontmatter {
        name: skill.name.clone(),
        description: skill.description.clone(),
        license: skill.license.clone(),
        compatibility: skill.compatibility.clone(),
        allowed_tools: skill.tool_refs.clone(),
        argument_hint: None,
        metadata: skill.metadata.clone(),
    };

    let yaml = serde_yaml::to_string(&frontmatter)?;
    let body = skill.prompt.trim();

    Ok(format!("---\n{}---\n\n{}\n", yaml, body))
}

pub(crate) fn is_valid_skill_id(id: &str) -> bool {
    if id.is_empty() {
        return false;
    }

    // Kilo-compatible rule: ^[a-z0-9]+(?:-[a-z0-9]+)*$
    // This forbids leading/trailing hyphens and consecutive hyphens.
    if id.starts_with('-') || id.ends_with('-') || id.contains("--") {
        return false;
    }

    id.split('-').all(|segment| {
        !segment.is_empty()
            && segment
                .chars()
                .all(|character| character.is_ascii_lowercase() || character.is_ascii_digit())
    })
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{is_valid_skill_id, parse_markdown_skill};

    #[test]
    fn valid_skill_ids() {
        assert!(is_valid_skill_id("my-skill"));
        assert!(is_valid_skill_id("skill123"));
        assert!(is_valid_skill_id("a-b-c"));
        assert!(is_valid_skill_id("skill-creator"));
        assert!(is_valid_skill_id("123-skill"));
    }

    #[test]
    fn invalid_skill_ids() {
        assert!(!is_valid_skill_id(""));
        assert!(!is_valid_skill_id("MySkill"));
        assert!(!is_valid_skill_id("my_skill"));
        assert!(!is_valid_skill_id("my skill"));
        assert!(!is_valid_skill_id("-skill"));
        assert!(!is_valid_skill_id("skill-"));
        assert!(!is_valid_skill_id("my--skill"));
    }

    #[test]
    fn parse_skill_without_id_uses_directory_name() {
        let content = r#"---
name: skill-creator
description: Helps create and improve skills.
---
Use this skill when users want to create skills.
"#;

        let parsed = parse_markdown_skill(Path::new("skill-creator/SKILL.md"), content)
            .expect("parse minimal frontmatter");
        assert_eq!(parsed.id, "skill-creator");
        assert_eq!(parsed.name, "skill-creator");
        assert_eq!(parsed.description, "Helps create and improve skills.");
        assert!(parsed.tool_refs.is_empty());
    }

    #[test]
    fn parse_skill_accepts_namespaced_name_and_argument_hint() {
        let content = r#"---
name: ckm:design
description: Design workflows.
argument-hint: "[type]"
---
Use this skill when users need design support.
"#;

        let parsed = parse_markdown_skill(Path::new("design/SKILL.md"), content)
            .expect("namespaced skill name should parse");
        assert_eq!(parsed.id, "design");
        assert_eq!(parsed.name, "ckm:design");
        assert_eq!(parsed.description, "Design workflows.");
    }

    #[test]
    fn parse_skill_rejects_unexpected_id_field() {
        let content = r#"---
id: skill-creator
name: skill-creator
description: Helps create and improve skills.
---
Use this skill when users want to create skills.
"#;

        let error = parse_markdown_skill(Path::new("skill-creator/SKILL.md"), content)
            .expect_err("id should be rejected by strict schema");
        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn parse_skill_rejects_name_directory_mismatch() {
        let content = r#"---
name: ckm:another-name
description: Helps create and improve skills.
---
Use this skill when users want to create skills.
"#;

        let error = parse_markdown_skill(Path::new("skill-creator/SKILL.md"), content)
            .expect_err("name mismatch should be rejected");
        assert!(error
            .to_string()
            .contains("must match directory name 'skill-creator'"));
    }
}
