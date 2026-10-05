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
    let (frontmatter_raw, body) = split_frontmatter(content)?;
    let frontmatter: SkillFrontmatter = codex_frontmatter::parse_frontmatter(
        &frontmatter_raw,
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
    .map_err(|error| SkillError::Validation(error.to_string()))?;
    let SkillFrontmatter {
        core,
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
        [
            "legacy_manual_only",
            "legacy_adapter",
            "legacy_migration",
            "legacy_import",
        ]
        .iter()
        .any(|flag| metadata.get(*flag).is_some_and(|value| !value.is_boolean()))
    }) {
        return Err(SkillError::Validation(
            "Invalid host control flag in metadata".to_string(),
        ));
    }

    let parsed = codex_frontmatter::normalize_metadata(&core, || dir_name.to_string())
        .map_err(|error| SkillError::Validation(error.to_string()))?;

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

pub fn split_frontmatter(content: &str) -> SkillResult<(String, String)> {
    codex_frontmatter::split_frontmatter(content)
        .map(|(frontmatter, body)| (frontmatter, body.to_string()))
        .map_err(|error| SkillError::Validation(error.to_string()))
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

    use super::{is_valid_skill_id, parse_markdown_skill};
    use tracing::span::{Attributes, Id, Record};
    use tracing::{Event, Level, Metadata, Subscriber};

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
