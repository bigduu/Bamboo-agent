use std::path::Path;
use std::sync::LazyLock;

use bamboo_domain::bounded_dedup::{BoundedFingerprintSet, DEFAULT_BOUNDED_FINGERPRINT_CAPACITY};
use bamboo_domain::normalize_tool_ref;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::types::{SkillDefinition, SkillError, SkillResult};

use super::codex_frontmatter::{self, SkillFrontmatter as CodexFrontmatter};

static STATIC_WARNINGS: LazyLock<BoundedFingerprintSet> =
    LazyLock::new(|| BoundedFingerprintSet::new(DEFAULT_BOUNDED_FINGERPRINT_CAPACITY));

const LEGACY_FLAGS: [&str; 4] = [
    "legacy_manual_only",
    "legacy_adapter",
    "legacy_migration",
    "legacy_import",
];
const INFERRED_DESCRIPTION_LIMIT: usize = 256;

#[derive(Debug, Serialize, Deserialize)]
struct SkillFrontmatter {
    #[serde(flatten)]
    core: CodexFrontmatter,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(deserialize_with = "optional_text")]
    license: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(deserialize_with = "optional_text")]
    compatibility: Option<String>,
    #[serde(default)]
    #[serde(
        rename = "allowed-tools",
        alias = "allowed_tools",
        skip_serializing_if = "AllowedTools::is_empty"
    )]
    allowed_tools: AllowedTools,
    #[serde(
        default,
        rename = "argument-hint",
        alias = "argument_hint",
        skip_serializing_if = "Option::is_none"
    )]
    argument_hint: Option<serde_json::Value>,
}

fn optional_text<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<String>, D::Error> {
    let value = Option::<serde_yaml::Value>::deserialize(deserializer)?;
    Ok(value.and_then(|value| value.as_str().map(str::to_string)))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
enum AllowedTools {
    Sequence(Vec<String>),
    Scalar(String),
}

impl Default for AllowedTools {
    fn default() -> Self {
        Self::Sequence(Vec::new())
    }
}

impl AllowedTools {
    fn is_empty(&self) -> bool {
        match self {
            Self::Sequence(tools) => tools.is_empty(),
            Self::Scalar(tools) => tools.is_empty(),
        }
    }

    fn into_refs(self) -> Vec<String> {
        match self {
            Self::Sequence(tools) => tools,
            Self::Scalar(tools) => tools
                .split(|c: char| c.is_whitespace() || c == ',')
                .filter(|tool| !tool.is_empty())
                .map(str::to_string)
                .collect(),
        }
    }
}

pub fn parse_markdown_skill(path: &Path, content: &str) -> SkillResult<SkillDefinition> {
    let (raw, body) = codex_frontmatter::split_frontmatter(content).map_err(validation_error)?;
    build_skill(path, body, parse_frontmatter(&raw)?, false)
}

/// Parses an ordinary instruction from a captured SkillStore source.
pub(super) fn parse_discovered_instruction_skill(
    path: &Path,
    content: &str,
) -> SkillResult<SkillDefinition> {
    let (raw, body) =
        codex_frontmatter::split_discovery_frontmatter(content).map_err(validation_error)?;
    let raw = if raw
        .lines()
        .all(|line| line.trim().is_empty() || line.trim_start().starts_with('#'))
    {
        "{}"
    } else {
        &raw
    };
    let frontmatter = parse_frontmatter(raw)?;
    if frontmatter.core.metadata.as_ref().is_some_and(|metadata| {
        metadata.get("workflow_definition_authoring").is_some()
            || LEGACY_FLAGS
                .iter()
                .any(|key| metadata.get(*key).and_then(serde_json::Value::as_bool) == Some(true))
    }) {
        return parse_markdown_skill(path, content);
    }
    build_skill(path, body, frontmatter, true)
}

fn validation_error(error: codex_frontmatter::SkillParseError) -> SkillError {
    SkillError::Validation(error.to_string())
}

fn parse_frontmatter(raw: &str) -> SkillResult<SkillFrontmatter> {
    codex_frontmatter::parse_frontmatter(
        raw,
        &[
            "allowed-tools",
            "allowed_tools",
            "metadata",
            "legacy_manual_only",
            "legacy_adapter",
            "legacy_migration",
            "legacy_import",
        ],
    )
    .map_err(validation_error)
}

fn build_skill(
    path: &Path,
    body: &str,
    frontmatter: SkillFrontmatter,
    infer_display_fields: bool,
) -> SkillResult<SkillDefinition> {
    let SkillFrontmatter {
        mut core,
        license,
        compatibility,
        allowed_tools,
        argument_hint: _argument_hint,
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

    if core.metadata.as_ref().is_some_and(|metadata| {
        LEGACY_FLAGS
            .iter()
            .any(|key| metadata.get(*key).is_some_and(|value| !value.is_boolean()))
    }) {
        return Err(SkillError::Validation(
            "Invalid host control flag in metadata".to_string(),
        ));
    }

    if infer_display_fields {
        if body.trim().is_empty() {
            return Err(SkillError::Validation("missing instruction text".into()));
        }
        infer_discovery_display_fields(&mut core, dir_name, body);
    }
    let parsed = codex_frontmatter::normalize_metadata(&core, || dir_name.to_string())
        .map_err(validation_error)?;

    let compatibility = compatibility
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    let license = license
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    let mut tool_refs = Vec::new();
    for tool_ref in allowed_tools.into_refs() {
        let trimmed = tool_ref.trim();
        if trimmed.is_empty() {
            continue;
        }

        match normalize_tool_ref(trimmed) {
            Some(normalized) => tool_refs.push(normalized),
            None => {
                let key = ("unrecognized-allowed-tool", path);
                if STATIC_WARNINGS.insert_if_new(&key, trimmed) {
                    warn!(
                        "Unrecognized allowed-tool '{}' in {:?}; preserving raw value",
                        trimmed, path
                    );
                } else {
                    debug!(
                        "Unrecognized allowed-tool '{}' in {:?}; preserving raw value",
                        trimmed, path
                    );
                }
                tool_refs.push(trimmed.to_string());
            }
        }
    }

    Ok(SkillDefinition {
        id: dir_name.to_string(),
        name: parsed.name,
        description: parsed.description,
        short_description: parsed.short_description,
        license,
        compatibility,
        metadata: core.metadata,
        prompt: body.trim().to_string(),
        tool_refs,
    })
}

fn infer_discovery_display_fields(core: &mut CodexFrontmatter, id: &str, body: &str) {
    let missing_name = core
        .name
        .as_deref()
        .is_none_or(|name| name.trim().is_empty());
    let missing_description = core
        .description
        .as_deref()
        .is_none_or(|description| description.trim().is_empty());
    if !missing_name && !missing_description {
        return;
    }
    let (title, summary) = markdown_display_fields(body);
    if missing_name {
        core.name = Some(display_excerpt(
            title.unwrap_or(id).split_whitespace(),
            codex_frontmatter::MAX_NAME_LEN,
        ));
    }
    if missing_description {
        core.description = if summary.is_empty() {
            core.name.clone()
        } else {
            Some(summary)
        };
    }
}

fn markdown_display_fields(body: &str) -> (Option<&str>, String) {
    let mut lines = body
        .split_inclusive('\n')
        .scan(0, |offset, line| {
            let start = *offset;
            *offset += line.len();
            Some((start, line))
        })
        .peekable();
    let mut title = None;
    while let Some((start, line)) = lines.next() {
        let text = line.trim();
        if let Some((marker, length)) = markdown_fence(line) {
            let _ = lines.by_ref().find(|(_, line)| {
                let text = line.trim();
                !indented_code(line)
                    && text.len() >= length
                    && text.chars().all(|character| character == marker)
            });
        } else if text.starts_with("<!--") {
            if !text.contains("-->") {
                let _ = lines.by_ref().find(|(_, line)| line.contains("-->"));
            }
        } else if let Some(heading) = markdown_heading(line) {
            if !heading.is_empty() {
                title.get_or_insert(heading);
            }
        } else if !thematic_break(text) && list_or_quote(text) {
            // Keep wrapped list and quote text out of the display excerpt.
            while lines
                .next_if(|(_, line)| {
                    let text = line.trim();
                    !text.is_empty()
                        && !text.starts_with("<!--")
                        && !thematic_break(text)
                        && markdown_heading(line).is_none()
                        && markdown_fence(line).is_none()
                })
                .is_some()
            {}
        } else if prose_line(line) {
            let mut end = start + line.len();
            while let Some((offset, line)) =
                lines.next_if(|(_, line)| !setext_underline(line) && prose_line(line))
            {
                end = offset + line.len();
            }
            let block = &body[start..end];
            if lines.next_if(|(_, line)| setext_underline(line)).is_some() {
                title.get_or_insert(block);
                continue;
            }
            let summary = display_excerpt(block.split_whitespace(), INFERRED_DESCRIPTION_LIMIT);
            return (title, summary);
        }
    }
    (title, String::new())
}

fn markdown_fence(line: &str) -> Option<(char, usize)> {
    if indented_code(line) {
        return None;
    }
    let text = line.trim_start();
    let marker = text
        .chars()
        .next()
        .filter(|marker| matches!(marker, '`' | '~'))?;
    let length = text
        .chars()
        .take_while(|character| *character == marker)
        .count();
    (length >= 3).then_some((marker, length))
}

fn setext_underline(line: &str) -> bool {
    if indented_code(line) {
        return false;
    }
    let text = line.trim();
    text.bytes().next().is_some_and(|marker| {
        matches!(marker, b'=' | b'-') && text.bytes().all(|byte| byte == marker)
    })
}

fn markdown_heading(line: &str) -> Option<&str> {
    if indented_code(line) {
        return None;
    }
    let line = line.trim();
    let title = line.trim_start_matches('#');
    let level = line.len() - title.len();
    if !(1..=6).contains(&level) || (!title.is_empty() && !title.starts_with(char::is_whitespace)) {
        return None;
    }
    let without_closing = title.trim_end_matches('#');
    if without_closing.ends_with(char::is_whitespace) {
        return Some(without_closing.trim());
    }
    Some(title.trim())
}

fn prose_line(line: &str) -> bool {
    let text = line.trim();
    let block_prefixes = ["```", "~~~", "<!--", "![", "[!["];
    !text.is_empty()
        && !indented_code(line)
        && markdown_heading(line).is_none()
        && !list_or_quote(text)
        && !thematic_break(text)
        && !block_prefixes.iter().any(|prefix| text.starts_with(prefix))
}

fn list_or_quote(text: &str) -> bool {
    text.starts_with('>')
        || text.split_whitespace().next().is_some_and(|prefix| {
            matches!(prefix, "-" | "+" | "*")
                || prefix.strip_suffix(['.', ')']).is_some_and(|digits| {
                    !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
                })
        })
}

fn thematic_break(text: &str) -> bool {
    let mut markers = text.bytes().filter(|byte| !byte.is_ascii_whitespace());
    let Some(marker @ (b'-' | b'*' | b'_')) = markers.next() else {
        return false;
    };
    markers.clone().take(2).count() == 2 && markers.all(|byte| byte == marker)
}

fn indented_code(line: &str) -> bool {
    line.starts_with("    ") || line.trim_start_matches(' ').starts_with('\t')
}

fn display_excerpt<'a>(words: impl Iterator<Item = &'a str>, limit: usize) -> String {
    words
        .flat_map(|word| word.chars().chain(std::iter::once(' ')))
        .take(limit)
        .collect::<String>()
        .trim_end()
        .to_string()
}

pub fn split_frontmatter(content: &str) -> SkillResult<(String, String)> {
    codex_frontmatter::split_frontmatter(content)
        .map(|(frontmatter, body)| (frontmatter, body.to_string()))
        .map_err(validation_error)
}

pub fn render_skill_markdown(skill: &SkillDefinition) -> SkillResult<String> {
    let frontmatter = SkillFrontmatter {
        core: CodexFrontmatter {
            name: Some(skill.name.clone()),
            description: Some(skill.description.clone()),
            metadata: skill.metadata.clone(),
        },
        license: skill.license.clone(),
        compatibility: skill.compatibility.clone(),
        allowed_tools: AllowedTools::Sequence(skill.tool_refs.clone()),
        argument_hint: None,
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
    use std::sync::{Arc, Mutex};

    use super::{is_valid_skill_id, parse_discovered_instruction_skill, parse_markdown_skill};
    use tracing::span::{Attributes, Id, Record};
    use tracing::{Event, Level, Metadata, Subscriber};

    #[test]
    fn ordinary_discovery_infers_unicode_display_fields_from_plain_markdown() {
        let path = Path::new("review-skill/SKILL.md");
        let prompt = "## 本地 审阅 ##\r\n\r\n先读取 <计划>。\r\n再运行检查。\r\n\r\n后续步骤。";
        let raw = format!("\u{feff}\r\n\r\n{prompt}\r\n");
        let skill = parse_discovered_instruction_skill(path, &raw).unwrap();
        assert_eq!(skill.id, "review-skill");
        assert_eq!(skill.name, "本地 审阅");
        assert_eq!(skill.description, "先读取 <计划>。 再运行检查。");
        assert_eq!(skill.prompt, prompt);
        assert!(parse_markdown_skill(path, &raw).is_err());
    }

    #[test]
    fn ordinary_discovery_finds_prose_after_leading_markdown_blocks() {
        let path = Path::new("review-skill/SKILL.md");
        let raw = concat!(
            "发布检查\n使用指南\n=====\n\n",
            "[![Status](badge.svg)](status.md)\n\n",
            "1. Introductory list\n",
            "<!--\n# Comment text\n-->\n\n",
            "- Opening checklist\n  Wrapped item\n",
            "~~~~ example\n# Fenced heading\n~~~\nExample text.\n~~~~\n\n",
            "- Final list item\n- - -\n",
            "Review the changed files.\nCheck the release notes.\n\nLater paragraph."
        );
        let skill = parse_discovered_instruction_skill(path, raw).unwrap();
        assert_eq!(skill.name, "发布检查 使用指南");
        assert_eq!(
            skill.description,
            "Review the changed files. Check the release notes."
        );
        assert_eq!(skill.prompt, raw);
    }

    #[test]
    fn ordinary_discovery_fills_only_blank_display_fields() {
        let path = Path::new("review-skill/SKILL.md");
        let description = format!("{} <详细>", "说明".repeat(180));
        let raw = format!(
            "\u{feff}\n \n---\nname: '  '\ndescription: {description}\nextra-field: accepted\n---\n# 标题\n\nUnchanged body.\n"
        );
        let skill = parse_discovered_instruction_skill(path, &raw).unwrap();
        assert_eq!(skill.name, "标题");
        assert_eq!(skill.description, description);
        assert_eq!(skill.prompt, "# 标题\n\nUnchanged body.");

        let named = "---\nname: Explicit name\ndescription: ''\n---\n# Ignored title\n\nOpening paragraph.\n";
        let skill = parse_discovered_instruction_skill(path, named).unwrap();
        assert_eq!(skill.name, "Explicit name");
        assert_eq!(skill.description, "Opening paragraph.");

        for raw in [
            "---\n---\nOpening paragraph.",
            "---\n# comment\n---\nOpening paragraph.",
        ] {
            let skill = parse_discovered_instruction_skill(path, raw).unwrap();
            assert_eq!(skill.name, "review-skill");
            assert_eq!(skill.description, "Opening paragraph.");
        }
    }

    #[test]
    fn ordinary_discovery_bounds_display_text_and_skips_code_blocks() {
        let id = "x".repeat(70);
        let path = std::path::PathBuf::from(&id).join("SKILL.md");
        let raw = format!("# {}\n\n{}", "名".repeat(70), "描".repeat(300));
        let skill = parse_discovered_instruction_skill(&path, &raw).unwrap();
        assert_eq!(skill.id, id);
        assert_eq!(skill.name, "名".repeat(64));
        assert_eq!(skill.description, "描".repeat(256));
        assert_eq!(skill.prompt, raw);

        for raw in ["```text\n# Example heading\n```", "    # Example heading"] {
            let skill = parse_discovered_instruction_skill(&path, raw).unwrap();
            assert_eq!(skill.name, "x".repeat(64));
            assert_eq!(skill.description, skill.name);
        }
    }

    #[test]
    fn ordinary_discovery_reports_invalid_headers_and_empty_bodies() {
        let path = Path::new("review-skill/SKILL.md");
        for raw in [
            "\u{feff} \n---\nname: Unclosed\n\nInstruction text.",
            "---\nallowed-tools: [\n---\nInstruction text.",
            "---\nmetadata:\n  legacy_manual_only: wrong\n---\nInstruction text.",
            "---\nmetadata:\n  legacy_manual_only: true\n---\nInstruction text.",
            "---\nmetadata:\n  workflow_definition_authoring: 1\n---\nInstruction text.",
            "\u{feff}\n\t",
            "---\ndescription: A description\n---\n  \n",
        ] {
            assert!(
                parse_discovered_instruction_skill(path, raw).is_err(),
                "{raw:?}"
            );
        }
    }

    struct LevelSubscriber(Arc<Mutex<Vec<Level>>>);

    impl Subscriber for LevelSubscriber {
        fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _span: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }

        fn record(&self, _span: &Id, _values: &Record<'_>) {}

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

        fn event(&self, event: &Event<'_>) {
            self.0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(*event.metadata().level());
        }

        fn enter(&self, _span: &Id) {}

        fn exit(&self, _span: &Id) {}
    }

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
    fn repeated_static_warning_for_same_key_and_error_downgrades_to_debug() {
        const CHILD_ENV: &str = "BAMBOO_SKILLS_PARSER_WARN_DEDUP_ISOLATED_CHILD_1751";
        if std::env::var(CHILD_ENV).as_deref() != Ok("1") {
            let output = std::process::Command::new(std::env::current_exe().expect("test binary"))
                .args([
                    "--exact",
                    "store::parser::tests::repeated_static_warning_for_same_key_and_error_downgrades_to_debug",
                    "--test-threads=1",
                    "--nocapture",
                ])
                .env_remove(CHILD_ENV)
                .env(CHILD_ENV, "1")
                .output()
                .expect("run isolated static warning fixture");
            assert!(
                output.status.success()
                    && String::from_utf8_lossy(&output.stdout)
                        .contains("test result: ok. 1 passed; 0 failed; 0 ignored;"),
                "isolated static warning fixture failed or selected no test ({}):\nstdout:\n{}\nstderr:\n{}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        let levels = Arc::new(Mutex::new(Vec::new()));
        let subscriber = LevelSubscriber(levels.clone());
        let content = r#"---
name: issue-741-warn-dedup
description: Exercises static warning de-duplication.
allowed-tools:
  - default::search
---
Test body.
"#;

        tracing::subscriber::with_default(subscriber, || {
            for _ in 0..2 {
                parse_markdown_skill(Path::new("issue-741-warn-dedup/SKILL.md"), content)
                    .expect("unknown tool is preserved, not rejected");
            }
        });

        assert_eq!(
            *levels
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            [Level::WARN, Level::DEBUG]
        );
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
    fn parse_skill_accepts_mixed_case_name_matching_directory() {
        let content = r#"---
name: Surge
description: Runs deployment workflows.
---
Deploy the application safely.
"#;

        let parsed = parse_markdown_skill(Path::new("surge/SKILL.md"), content)
            .expect("mixed-case display name should match its directory");
        assert_eq!(parsed.id, "surge");
        assert_eq!(parsed.name, "Surge");
    }

    #[test]
    fn parse_skill_accepts_mixed_case_namespaced_name() {
        let content = r#"---
name: OpenAI:Documents
description: Creates and edits documents.
---
Use this skill for document work.
"#;

        let parsed = parse_markdown_skill(Path::new("documents/SKILL.md"), content)
            .expect("mixed-case namespaced display name should parse");
        assert_eq!(parsed.id, "documents");
        assert_eq!(parsed.name, "OpenAI:Documents");
    }

    #[test]
    fn parse_skill_accepts_display_name_independent_of_safe_id() {
        let content = r#"---
name: Skill_Creator
description: Helps create and improve skills.
---
Use this skill when users want to create skills.
"#;

        let skill = parse_markdown_skill(Path::new("skill-creator/SKILL.md"), content).unwrap();
        assert_eq!(skill.name, "Skill_Creator");
        assert_eq!(skill.id, "skill-creator");
    }

    #[test]
    fn parse_skill_ignores_external_id_field() {
        let content = r#"---
id: ../../unsafe
name: skill-creator
description: Helps create and improve skills.
---
Use this skill when users want to create skills.
"#;

        let skill = parse_markdown_skill(Path::new("skill-creator/SKILL.md"), content).unwrap();
        assert_eq!(skill.id, "skill-creator");
    }

    #[test]
    fn parse_skill_accepts_name_directory_mismatch() {
        let content = r#"---
name: ckm:another-name
description: Helps create and improve skills.
---
Use this skill when users want to create skills.
"#;

        let skill = parse_markdown_skill(Path::new("skill-creator/SKILL.md"), content).unwrap();
        assert_eq!(skill.name, "ckm:another-name");
        assert_eq!(skill.id, "skill-creator");
    }
    #[test]
    fn codex_adapter_preserves_body_bytes_and_arbitrary_metadata() {
        let content = "  ---  \r\nname: 文件  助手\r\ndescription: >-\r\n  Process <files>\r\n  safely\r\nlicense: MIT\r\ncompatibility: Any host\r\nmetadata:\r\n  short-description:  File   work\r\n  custom: [a, b]\r\nallowed-tools: Bash, Read mcp__test__lookup\r\nunknown: ignored\r\n --- \r\n\r\nFirst line  \r\n\tCode  stays\r\nLast line\r\n";
        let skill = parse_markdown_skill(Path::new("safe-id/SKILL.md"), content).unwrap();
        assert_eq!(skill.id, "safe-id");
        assert_eq!(skill.name, "文件 助手");
        assert_eq!(skill.description, "Process <files> safely");
        assert_eq!(skill.short_description.as_deref(), Some("File work"));
        assert_eq!(
            skill.metadata.as_ref().unwrap()["short-description"],
            "File   work"
        );
        assert_eq!(
            skill.metadata.as_ref().unwrap()["custom"],
            serde_json::json!(["a", "b"])
        );
        assert_eq!(skill.prompt, "First line  \r\n\tCode  stays\r\nLast line");
        assert_eq!(skill.license.as_deref(), Some("MIT"));
        assert_eq!(skill.compatibility.as_deref(), Some("Any host"));
        assert_eq!(skill.tool_refs, ["Bash", "Read", "mcp__test__lookup"]);
    }

    #[test]
    fn codex_adapter_supports_fallback_unicode_limits_and_long_prose() {
        for name in [None, Some("'  '"), Some("'分析 专家'")] {
            let name_line = name
                .map(|name| format!("name: {name}\n"))
                .unwrap_or_default();
            let content = format!(
                "---\n{name_line}description: {}\ncompatibility: {}\n---\nBody",
                "💡<>".repeat(1100),
                "x".repeat(600)
            );
            let skill = parse_markdown_skill(Path::new("safe-id/SKILL.md"), &content).unwrap();
            assert_eq!(
                skill.name,
                if name == Some("'分析 专家'") {
                    "分析 专家"
                } else {
                    "safe-id"
                }
            );
            assert_eq!(skill.description.chars().count(), 3300);
            assert_eq!(skill.compatibility.unwrap().len(), 600);
        }
        for (length, accepted) in [(64, true), (65, false)] {
            let content = format!(
                "---\nname: {}\ndescription: Description\n---\n",
                "文".repeat(length)
            );
            assert_eq!(
                parse_markdown_skill(Path::new("safe-id/SKILL.md"), &content).is_ok(),
                accepted
            );
        }
        let valid = "---\ndescription: Description\n---\nBody";
        assert!(parse_markdown_skill(Path::new("unsafe_id/SKILL.md"), valid).is_err());
        assert!(parse_markdown_skill(
            Path::new("safe-id/SKILL.md"),
            "---\nname: Display\n---\nBody"
        )
        .is_err());
    }

    #[test]
    fn codex_adapter_tolerates_descriptive_extra_shapes() {
        let skill = parse_markdown_skill(Path::new("portable/SKILL.md"), "---\ndescription: Demo\nlicense: [optional]\ncompatibility: {host: optional}\nargument-hint: [one, two]\nmetadata: [arbitrary, data]\n---\nBody").unwrap();
        assert!(skill.license.is_none());
        assert!(skill.compatibility.is_none());
        assert_eq!(
            skill.metadata,
            Some(serde_json::json!(["arbitrary", "data"]))
        );
    }

    #[test]
    fn codex_adapter_repairs_once_without_dropping_tool_restrictions() {
        let content = "---\ndescription: Deploy to AWS: ECS\nallowed-tools:\n  - Bash\n  - default::private\nmetadata:\n  legacy_manual_only: true\nargument-hint: <duration: 7d>\n---\nUnchanged: body\n";
        let skill = parse_markdown_skill(Path::new("deploy/SKILL.md"), content).unwrap();
        assert_eq!(skill.description, "Deploy to AWS: ECS");
        assert_eq!(skill.tool_refs, ["Bash", "default::private"]);
        assert_eq!(skill.metadata.unwrap()["legacy_manual_only"], true);
        assert_eq!(skill.prompt, "Unchanged: body");
        for bad in [
            "allowed_tools: [",
            "metadata: {legacy_manual_only: true,",
            "metadata: {legacy_manual_only: wrong}",
            "allowed-tools: 7",
            "allowed-tools: [Read, 7]",
            "allowed-tools: [",
            "metadata: [",
        ] {
            let content = format!("---\ndescription: Deploy\n{bad}\n---\nBody");
            assert!(
                parse_markdown_skill(Path::new("deploy/SKILL.md"), &content).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn codex_adapter_quoted_host_controls_cannot_be_repaired_into_prose() {
        for key in [
            "'metadata'",
            "\"metadata\"",
            "\"\\u006detadata\"",
            "!!str metadata",
            "&host_key metadata",
        ] {
            let malformed =
                format!("---\ndescription: Demo\n{key}: {{legacy_manual_only: true,\n---\nBody");
            assert!(
                parse_markdown_skill(Path::new("safe-id/SKILL.md"), &malformed).is_err(),
                "malformed host container was accepted for {key}"
            );
            let valid = format!(
                "---\ndescription: Deploy to AWS: ECS\n{key}:\n  legacy_manual_only: true\n---\nBody"
            );
            let skill = parse_markdown_skill(Path::new("safe-id/SKILL.md"), &valid).unwrap();
            assert_eq!(skill.description, "Deploy to AWS: ECS");
            assert_eq!(skill.metadata.unwrap()["legacy_manual_only"], true);
        }
        for key in [
            "'allowed-tools'",
            "\"allowed_tools\"",
            "\"allowed\\u002dtools\"",
        ] {
            let malformed = format!("---\ndescription: Demo\n{key}: [\n---\nBody");
            assert!(
                parse_markdown_skill(Path::new("safe-id/SKILL.md"), &malformed).is_err(),
                "malformed tool restriction was accepted for {key}"
            );
        }
        let valid_alias = "---\ndescription: Demo\nkey_name: &host_key metadata\n*host_key:\n  legacy_manual_only: true\n---\nBody";
        let skill = parse_markdown_skill(Path::new("safe-id/SKILL.md"), valid_alias).unwrap();
        assert_eq!(skill.metadata.unwrap()["legacy_manual_only"], true);
        let malformed_alias = "---\ndescription: Demo\nkey_name: &host_key metadata\n*host_key: {legacy_manual_only: true,\n---\nBody";
        assert!(parse_markdown_skill(Path::new("safe-id/SKILL.md"), malformed_alias).is_err());
    }
}
