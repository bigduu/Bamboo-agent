use std::borrow::Cow;

use serde::Serialize;

use crate::resource_helpers::{display_relative_path, normalize_relative_resource_path};
use crate::types::SkillDefinition;

use super::parser::{
    indented_code, list_or_quote, markdown_heading, prose_line, setext_underline,
    skip_literal_markdown_block, thematic_break,
};
use super::SkillResourceSnapshot;

const SCORE_VERSION: u8 = 1;
const MAX_INSTRUCTION_CHARS: usize = 64 * 1024;

/// Advisory metadata for a management row in a published Skill catalog.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SkillQuality {
    score_version: u8,
    state: QualityState,
    score: Option<u8>,
    suggestions: Vec<Suggestion>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum QualityState {
    Rated,
    Unavailable,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct Suggestion {
    code: &'static str,
    message: &'static str,
    location: &'static str,
}

impl SkillQuality {
    pub fn unavailable() -> Self {
        Self {
            score_version: SCORE_VERSION,
            state: QualityState::Unavailable,
            score: None,
            suggestions: Vec::new(),
        }
    }
}

#[derive(Default)]
struct Evidence {
    paragraphs: usize,
    steps: usize,
    headings: usize,
    verification: bool,
    missing_reference: bool,
}

pub(super) fn assess(skill: &SkillDefinition, resources: &SkillResourceSnapshot) -> SkillQuality {
    if skill.prompt.chars().nth(MAX_INSTRUCTION_CHARS).is_some() || skill.prompt.trim().is_empty() {
        return SkillQuality::unavailable();
    }
    let evidence = inspect(&skill.prompt, resources);
    let title = skill.name.trim();
    let has_context = [
        Some(skill.description.as_str()),
        skill.short_description.as_deref(),
    ]
    .into_iter()
    .flatten()
    .map(str::trim)
    .any(|description| !description.is_empty() && description != title);
    let multiple_parts = evidence.paragraphs > 1 || evidence.steps > 1;
    let rules = [
        (
            !has_context,
            20,
            Suggestion {
                code: "clarify_applicability",
                message: "Describe when to use this Skill and what it helps accomplish.",
                location: "description",
            },
        ),
        (
            evidence.paragraphs == 0 && evidence.steps == 0,
            25,
            Suggestion {
                code: "describe_procedure",
                message: "Add a prose instruction or checklist for the steps to follow.",
                location: "instructions",
            },
        ),
        (
            evidence.paragraphs > 1 && evidence.steps == 0 && evidence.headings < 2,
            15,
            Suggestion {
                code: "organize_procedure",
                message: "Use section headings or a checklist to make the instruction order clear.",
                location: "instructions",
            },
        ),
        (
            multiple_parts && !evidence.verification,
            20,
            Suggestion {
                code: "describe_verification",
                message: "Describe the result to check once the steps are complete.",
                location: "instructions",
            },
        ),
        (
            evidence.missing_reference,
            15,
            Suggestion {
                code: "review_references",
                message:
                    "Check links to reference material against the files included with this Skill.",
                location: "instructions",
            },
        ),
    ];
    let mut score = 100u8;
    let mut suggestions = Vec::new();
    for (applies, deduction, suggestion) in rules {
        if applies {
            score = score.saturating_sub(deduction);
            suggestions.push(suggestion);
        }
    }
    SkillQuality {
        score_version: SCORE_VERSION,
        state: QualityState::Rated,
        score: Some(score),
        suggestions,
    }
}

fn inspect(prompt: &str, resources: &SkillResourceSnapshot) -> Evidence {
    let mut evidence = Evidence::default();
    let mut lines = prompt.lines().peekable();
    let mut in_paragraph = false;
    while let Some(line) = lines.next() {
        let text = line.trim();
        if skip_literal_markdown_block(line, &mut lines)
            || indented_code(line)
            || text.starts_with('>')
        {
            in_paragraph = false;
            continue;
        }
        let prose_candidate = prose_line(line);
        let heading = markdown_heading(line).is_some()
            || (prose_candidate && lines.next_if(|line| setext_underline(line)).is_some());
        let step = list_or_quote(text)
            && !thematic_break(text)
            && text.split_once(char::is_whitespace).is_some();
        let prose = prose_candidate && !heading;
        if heading {
            evidence.headings += 1;
        } else if step {
            evidence.steps += 1;
        } else if prose && !in_paragraph {
            evidence.paragraphs += 1;
        }
        if heading || step || prose {
            inspect_inline(text, resources, &mut evidence);
        }
        in_paragraph = prose;
    }
    evidence
}

fn mentions_verification(text: &str) -> bool {
    const ENGLISH: [&str; 13] = [
        "acceptance",
        "assert",
        "check",
        "checks",
        "confirm",
        "expect",
        "expected",
        "pass",
        "passes",
        "success",
        "validate",
        "verification",
        "verify",
    ];
    const CHINESE: [&str; 8] = [
        "检查", "核对", "确认", "验证", "验收", "预期", "成功", "通过",
    ];
    text.split(|character: char| !character.is_ascii_alphabetic())
        .any(|word| ENGLISH.iter().any(|known| word.eq_ignore_ascii_case(known)))
        || CHINESE.iter().any(|known| text.contains(known))
}

fn inspect_inline(text: &str, resources: &SkillResourceSnapshot, evidence: &mut Evidence) {
    let text = without_inline_code(text);
    let bytes = text.as_bytes();
    let mut open_labels = 0usize;
    let mut visible_start = 0;
    let mut cursor = 0;
    while cursor < bytes.len() {
        match bytes[cursor] {
            b'\\' if bytes.get(cursor + 1).is_some_and(u8::is_ascii_punctuation) => {
                cursor += 2;
                continue;
            }
            b'[' => open_labels += 1,
            b']' if open_labels > 0 => {
                open_labels -= 1;
                if bytes.get(cursor + 1) == Some(&b'(') {
                    let Some((target, tail)) = inline_destination(&text[cursor + 2..]) else {
                        break;
                    };
                    evidence.verification |=
                        mentions_verification(&text[visible_start..cursor + 1]);
                    if let Some(path) = reference_target(target) {
                        evidence.missing_reference |= normalize_relative_resource_path(path)
                            .ok()
                            .is_none_or(|path| {
                                !resources.contains_key(&display_relative_path(&path))
                            });
                    }
                    cursor = text.len() - tail.len();
                    visible_start = cursor;
                    continue;
                }
            }
            _ => {}
        }
        cursor += 1;
    }
    evidence.verification |= mentions_verification(&text[visible_start..]);
}

fn inline_destination(part: &str) -> Option<(&str, &str)> {
    let part = part.trim_start();
    let delimiter_input = if part.starts_with('<') {
        part.split_once('>')?.1
    } else {
        part
    };
    let mut depth = 0;
    let mut escaped = false;
    let mut title_quote = None;
    for (offset, byte) in delimiter_input.bytes().enumerate() {
        if escaped {
            escaped = false;
            continue;
        }
        if byte == b'\\' {
            escaped = true;
            continue;
        }
        if let Some(quote) = title_quote {
            if byte == quote {
                title_quote = None;
            }
            continue;
        }
        match byte {
            b'\'' | b'"'
                if depth == 0
                    && offset > 0
                    && delimiter_input.as_bytes()[offset - 1].is_ascii_whitespace() =>
            {
                title_quote = Some(byte);
            }
            b'(' => depth += 1,
            b')' if depth == 0 => {
                let end = part.len() - delimiter_input.len() + offset;
                return Some((&part[..end], &part[end + 1..]));
            }
            b')' => depth -= 1,
            _ => {}
        }
    }
    None
}

fn without_inline_code(text: &str) -> Cow<'_, str> {
    let mut ticks = backtick_runs(text);
    let mut visible = None;
    let mut remaining_start = 0;
    while let Some((start, end)) = ticks.next() {
        if text.as_bytes()[..start]
            .iter()
            .rev()
            .take_while(|&&byte| byte == b'\\')
            .count()
            % 2
            == 1
        {
            continue;
        }
        let mut remaining_ticks = ticks.clone();
        if let Some((_, closing_end)) = remaining_ticks
            .find(|(closing_start, closing_end)| closing_end - closing_start == end - start)
        {
            let buffer = visible.get_or_insert_with(String::new);
            buffer.push_str(&text[remaining_start..start]);
            buffer.push(' ');
            remaining_start = closing_end;
            ticks = remaining_ticks;
        }
    }
    if let Some(mut visible) = visible {
        visible.push_str(&text[remaining_start..]);
        Cow::Owned(visible)
    } else {
        Cow::Borrowed(text)
    }
}

fn backtick_runs(text: &str) -> impl Iterator<Item = (usize, usize)> + Clone + '_ {
    let mut offset = 0;
    std::iter::from_fn(move || {
        let start = offset + text[offset..].find('`')?;
        offset = start
            + text[start..]
                .bytes()
                .take_while(|byte| *byte == b'`')
                .count();
        Some((start, offset))
    })
}

fn reference_target(target: &str) -> Option<&str> {
    let link = if let Some(bracketed) = target.strip_prefix('<') {
        bracketed.split_once('>')?.0
    } else {
        target.split_whitespace().next()?
    };
    let path = link.split(['#', '?']).next()?;
    let relative = path.strip_prefix("./").unwrap_or(path);
    let (directory, file) = relative.split_once('/')?;
    (matches!(directory, "references" | "assets" | "scripts")
        && !file.is_empty()
        && !file.ends_with('/')
        && !file.contains(['%', '\\']))
    .then_some(relative)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;

    #[test]
    fn equivalent_chinese_and_english_steps_receive_the_same_score() {
        let resources = SkillResourceSnapshot::default();
        let english = SkillDefinition::new(
            "review",
            "Review",
            "Prepare a change summary before publication.",
            "1. Read the change notes.\n2. Confirm the summary covers the intended release.",
        );
        let mut chinese = SkillDefinition::new(
            "review",
            "审阅",
            "发布前整理变更摘要。",
            "1. 阅读变更说明。\n2. 确认摘要涵盖本次发布。",
        );
        chinese.metadata = Some(serde_json::json!({"other_ecosystem": "advisory"}));
        let quality = assess(&english, &resources);
        assert_eq!(quality.score, Some(100));
        assert_eq!(quality, assess(&chinese, &resources));

        let short = SkillDefinition::new(
            "review",
            "审阅",
            "发布前整理变更摘要。",
            "列出三项用户可见的变更。",
        );
        assert_eq!(assess(&short, &resources), quality);

        let english_checklist = SkillDefinition::new(
            "review",
            "Review",
            "Prepare a change summary before publication.",
            "## Checklist\n\n1. Read the change notes.\n2. Write the summary.",
        );
        let chinese_checklist = SkillDefinition::new(
            "review",
            "审阅",
            "发布前整理变更摘要。",
            "## 清单\n\n1. 阅读变更说明。\n2. 写出摘要。",
        );
        let quality = assess(&english_checklist, &resources);
        assert_eq!(quality.score, Some(80));
        assert_eq!(quality, assess(&chinese_checklist, &resources));
    }

    #[test]
    fn suggestions_use_visible_instructions_and_captured_resources() {
        let skill = SkillDefinition::new(
            "review",
            "Review",
            "Review",
            concat!(
                "# Review\n\n",
                "Collect the [source](<./references/project notes.md#scope>).\n\n",
                "Save the summary.\n\n",
                "<!-- Confirm the expected outcome. -->\n",
                "~~~text\nVerify success.\n[Example](assets/discarded-example.txt)\n~~~",
            ),
        );
        let empty = SkillResourceSnapshot::default();
        let quality = assess(&skill, &empty);
        let codes: Vec<_> = quality.suggestions.iter().map(|item| item.code).collect();
        assert_eq!(
            codes,
            [
                "clarify_applicability",
                "organize_procedure",
                "describe_verification",
                "review_references"
            ]
        );
        assert_eq!(quality.score, Some(30));
        assert_eq!(quality, assess(&skill, &empty));
        let json = serde_json::to_string(&quality).unwrap();
        assert!(!json.contains("project notes"));
        assert!(!json.contains("discarded-example"));

        let resources = Arc::new(HashMap::from([(
            "references/project notes.md".into(),
            Arc::new(Vec::new()),
        )]));
        let updated = assess(&skill, &resources);
        assert_eq!(updated.score, Some(45));
        assert_eq!(updated.suggestions, quality.suggestions[..3]);
    }

    #[test]
    fn reference_evidence_distinguishes_inline_code_and_balanced_link_destinations() {
        let skill = SkillDefinition::new(
            "review",
            "Review",
            "Review changes before publication.",
            concat!(
                "1. Read the [notes](references/Verification_(notes).md).\n",
                "2. Save the summary. Example syntax is ``[Guide](references/example.md) `verify success` ``."
            ),
        );
        let resources = Arc::new(HashMap::from([(
            "references/Verification_(notes).md".into(),
            Arc::new(Vec::new()),
        )]));
        let quality = assess(&skill, &resources);
        assert_eq!(quality.score, Some(80));
        assert_eq!(quality.suggestions.len(), 1);
        assert_eq!(quality.suggestions[0].code, "describe_verification");
        let absent = assess(&skill, &SkillResourceSnapshot::default());
        assert_eq!(absent.score, Some(65));
        assert_eq!(absent.suggestions[1].code, "review_references");
    }

    #[test]
    fn reference_evidence_requires_unescaped_label_brackets() {
        let mut skill = SkillDefinition::new(
            "review",
            "Review",
            "Review changes before publication.",
            r#"Write \[notes](references/literal.md) as literal Markdown."#,
        );
        let resources = SkillResourceSnapshot::default();
        assert_eq!(assess(&skill, &resources).score, Some(100));

        skill
            .prompt
            .push_str(r#" Use the [guide [draft]](references/guide.md "Title with ](")."#);
        let quality = assess(&skill, &resources);
        assert_eq!(quality.score, Some(85));
        assert_eq!(quality.suggestions[0].code, "review_references");
    }

    #[test]
    fn missing_or_oversized_instruction_evidence_is_unavailable() {
        let resources = SkillResourceSnapshot::default();
        for prompt in [String::new(), "字".repeat(MAX_INSTRUCTION_CHARS + 1)] {
            let skill = SkillDefinition::new("review", "Review", "Review changes.", prompt);
            let quality = assess(&skill, &resources);
            assert_eq!(
                serde_json::to_value(&quality).unwrap(),
                serde_json::json!({
                    "score_version": 1,
                    "state": "unavailable",
                    "score": null,
                    "suggestions": []
                })
            );
        }

        for character in ["a", "字"] {
            let skill = SkillDefinition::new(
                "review",
                "Review",
                "Review changes.",
                character.repeat(MAX_INSTRUCTION_CHARS),
            );
            assert_eq!(assess(&skill, &resources).state, QualityState::Rated);
        }
    }
}
