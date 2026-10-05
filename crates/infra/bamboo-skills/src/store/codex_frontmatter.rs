// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Adapted from Codex 7f892275e31002f0422477c6219189284560e689.
// Bamboo shares extraction/repair with its host extras and preserves raw metadata/body.

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;

const MAX_NAME_LEN: usize = 64;

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct SkillFrontmatter {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// Validated metadata parsed from a `SKILL.md` frontmatter block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedSkillFrontmatter {
    pub name: String,
    pub description: String,
    pub short_description: Option<String>,
}

/// Error produced while parsing or validating `SKILL.md` metadata.
#[derive(Debug, Error)]
pub enum SkillParseError {
    #[error("missing YAML frontmatter delimited by ---")]
    MissingFrontmatter,
    #[error("invalid YAML: {0}")]
    InvalidYaml(#[source] serde_yaml::Error),
    #[error("missing field `{0}`")]
    MissingField(&'static str),
    #[error("invalid {field}: {reason}")]
    InvalidField { field: &'static str, reason: String },
}

/// Parses and validates the metadata frontmatter from `SKILL.md` contents.
#[cfg(test)]
pub fn parse_skill_frontmatter_metadata(
    contents: &str,
    default_name: impl FnOnce() -> String,
) -> Result<ParsedSkillFrontmatter, SkillParseError> {
    let (frontmatter, _) = split_frontmatter(contents)?;
    let parsed: SkillFrontmatter = parse_frontmatter(&frontmatter, &[])?;
    normalize_metadata(&parsed, default_name)
}

pub(super) fn parse_frontmatter<T: DeserializeOwned>(
    frontmatter: &str,
    protected_keys: &[&str],
) -> Result<T, SkillParseError> {
    let parsed: T = match serde_yaml::from_str(frontmatter) {
        Ok(parsed) => Ok(parsed),
        Err(original_error) => {
            match repair_frontmatter_scalar_fields(frontmatter, protected_keys) {
                // Some third-party skills use prose like `description: Build for AWS: ECS`
                // or `argument-hint: <duration: e.g. 7d>`. Keep the repair line-oriented
                // so unrelated invalid YAML still surfaces.
                Some(repaired_frontmatter) => {
                    serde_yaml::from_str(&repaired_frontmatter).map_err(|_| original_error)
                }
                None => Err(original_error),
            }
        }
    }
    .map_err(SkillParseError::InvalidYaml)?;

    Ok(parsed)
}

pub(super) fn normalize_metadata(
    parsed: &SkillFrontmatter,
    default_name: impl FnOnce() -> String,
) -> Result<ParsedSkillFrontmatter, SkillParseError> {
    let name = parsed
        .name
        .as_deref()
        .map(sanitize_single_line)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(default_name);
    let description = parsed
        .description
        .as_deref()
        .map(sanitize_single_line)
        .unwrap_or_default();
    let short_description = parsed
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("short-description"))
        .filter(|value| !value.is_null())
        .map(|value| {
            value.as_str().ok_or_else(|| SkillParseError::InvalidField {
                field: "metadata.short-description",
                reason: "must be a string".to_string(),
            })
        })
        .transpose()?
        .map(sanitize_single_line)
        .filter(|value| !value.is_empty());

    validate_len(&name, MAX_NAME_LEN, "name")?;
    if description.is_empty() {
        return Err(SkillParseError::MissingField("description"));
    }

    Ok(ParsedSkillFrontmatter {
        name,
        description,
        short_description,
    })
}

pub(super) fn sanitize_single_line(raw: &str) -> String {
    raw.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn repair_frontmatter_scalar_fields(frontmatter: &str, protected_keys: &[&str]) -> Option<String> {
    let mut changed = false;
    let mut block_scalar_indent: Option<usize> = None;
    let mut repaired_lines = Vec::new();
    for line in frontmatter.lines() {
        let indent = line
            .chars()
            .take_while(|character| *character == ' ')
            .count();
        if let Some(block_indent) = block_scalar_indent {
            if line.trim().is_empty() || indent > block_indent {
                repaired_lines.push(line.to_string());
                continue;
            }
            block_scalar_indent = None;
        }

        let Some((key, value)) = line.split_once(':') else {
            repaired_lines.push(line.to_string());
            continue;
        };
        // Quoting, escaping and YAML tags do not change a host key's identity.
        // An alias cannot be resolved in isolation; leave its value untouched
        // when host controls are protected instead of treating it as prose.
        let protected_key = !protected_keys.is_empty()
            && serde_yaml::from_str::<String>(key.trim())
                .map(|key| protected_keys.contains(&key.as_str()))
                .unwrap_or(true);
        if key.trim().is_empty()
            || protected_key
            || !value.chars().next().is_none_or(char::is_whitespace)
        {
            repaired_lines.push(line.to_string());
            continue;
        }

        let trimmed_start = value.trim_start();
        let leading_whitespace = &value[..value.len() - trimmed_start.len()];
        let mut scalar = trimmed_start;
        let mut comment = "";
        for (index, character) in trimmed_start.char_indices() {
            if character == '#'
                && (index == 0
                    || trimmed_start[..index]
                        .chars()
                        .next_back()
                        .is_some_and(char::is_whitespace))
            {
                let comment_start = trimmed_start[..index].trim_end().len();
                scalar = &trimmed_start[..comment_start];
                comment = &trimmed_start[comment_start..];
                break;
            }
        }

        let scalar = scalar.trim_end();
        let Some(first_char) = scalar.chars().next() else {
            repaired_lines.push(line.to_string());
            continue;
        };
        if matches!(first_char, '|' | '>') {
            block_scalar_indent = Some(indent);
            repaired_lines.push(line.to_string());
            continue;
        }
        if matches!(first_char, '\'' | '"') {
            repaired_lines.push(line.to_string());
            continue;
        }
        let mut has_colon_separator = false;
        let mut chars = scalar.chars().peekable();
        while let Some(character) = chars.next() {
            if character == ':'
                && matches!(chars.peek(), Some(next_character) if next_character.is_whitespace())
            {
                has_colon_separator = true;
                break;
            }
        }
        let invalid_flow_like_scalar = matches!(first_char, '[' | '{' | '@' | '`')
            && serde_yaml::from_str::<serde_yaml::Value>(scalar).is_err();
        if !has_colon_separator && !invalid_flow_like_scalar {
            repaired_lines.push(line.to_string());
            continue;
        }

        let quoted_scalar = format!("'{}'", scalar.replace('\'', "''"));
        repaired_lines.push(format!(
            "{key}:{leading_whitespace}{quoted_scalar}{comment}"
        ));
        changed = true;
    }
    changed.then(|| repaired_lines.join("\n"))
}

fn validate_len(
    value: &str,
    max_len: usize,
    field_name: &'static str,
) -> Result<(), SkillParseError> {
    if value.is_empty() {
        return Err(SkillParseError::MissingField(field_name));
    }
    if value.chars().count() > max_len {
        return Err(SkillParseError::InvalidField {
            field: field_name,
            reason: format!("exceeds maximum length of {max_len} characters"),
        });
    }
    Ok(())
}

/// Uses the pinned delimiter rules while retaining the untouched body slice.
pub(super) fn split_frontmatter(contents: &str) -> Result<(String, &str), SkillParseError> {
    let mut lines = contents.split_inclusive('\n');
    let first = lines.next().ok_or(SkillParseError::MissingFrontmatter)?;
    if first.trim() != "---" {
        return Err(SkillParseError::MissingFrontmatter);
    }
    let mut offset = first.len();
    let mut frontmatter_lines = Vec::new();
    for line in lines {
        offset += line.len();
        if line.trim() == "---" {
            if frontmatter_lines.is_empty() {
                return Err(SkillParseError::MissingFrontmatter);
            }
            return Ok((frontmatter_lines.join("\n"), &contents[offset..]));
        }
        frontmatter_lines.push(line.trim_end_matches('\n').trim_end_matches('\r'));
    }
    Err(SkillParseError::MissingFrontmatter)
}

#[cfg(test)]
#[path = "codex_frontmatter_tests.rs"]
mod tests;
