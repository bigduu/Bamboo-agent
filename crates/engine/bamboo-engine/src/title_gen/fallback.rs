//! Heuristic fallback title generator (no LLM call).

const MAX_FALLBACK_LEN: usize = 60;

/// Titles of the in-conversation failure banner, which users paste verbatim when
/// reporting a problem. Keep in sync with the front-end copy in
/// `lotus-next/src/shared/i18n/resources/ui-en-US.ts` and `ui-zh-CN.ts`
/// (`message_sent_but_generation_was_interrupted_62a4d83d`).
const BANNER_TITLE_PREFIXES: &[&str] = &[
    "message sent, but generation was interrupted",
    "消息已发送，但生成中断",
];

/// The banner's `<summary>` label (`technical_details_c26fb419`). It renders next
/// to the banner title, so a text selection can join both into one line.
const BANNER_DETAILS_LABEL_PREFIXES: &[&str] = &["technical details", "技术详情"];

/// Derive a short title from the first user message using simple heuristics.
///
/// - Strips fenced code blocks (```...```).
/// - Drops a pasted failure banner so its copy does not become the title.
/// - Collapses whitespace.
/// - Takes the first sentence (up to `.`/`!`/`?`/newline).
/// - Caps at `MAX_FALLBACK_LEN` characters without cutting a word in half.
pub fn heuristic_title(first_user_text: &str) -> String {
    let cleaned = strip_code_fences(first_user_text);
    let stripped = strip_pasted_failure_banner(&cleaned);
    // A message that is nothing but banner copy keeps its original text: a
    // banner-derived title is still better than no title at all.
    let content = if stripped.trim().is_empty() {
        cleaned.as_str()
    } else {
        stripped.trim()
    };
    let collapsed: String = content.split_whitespace().collect::<Vec<_>>().join(" ");
    let first_sentence = collapsed.split(['.', '!', '?', '\n']).next().unwrap_or("");
    truncate_at_word_boundary(first_sentence, MAX_FALLBACK_LEN)
        .trim_end_matches([' ', ',', ';', ':', '-'])
        .trim()
        .to_string()
}

/// Cap `text` at `max_chars` characters without leaving a partial trailing word.
///
/// Text with no whitespace before the cut (a URL, a CJK sentence) keeps the hard
/// cut: retreating to a "word" boundary there would discard the whole title.
fn truncate_at_word_boundary(text: &str, max_chars: usize) -> &str {
    let Some((hard_cut, _)) = text.char_indices().nth(max_chars) else {
        return text;
    };
    let head = &text[..hard_cut];
    match head.rfind(char::is_whitespace) {
        Some(word_cut) if word_cut > 0 => &head[..word_cut],
        _ => head,
    }
}

/// Remove a leading pasted failure banner, returning the user's own text.
///
/// A selection dragged over the banner can join its title and `<summary>` label
/// into one line, or keep them on separate lines; both shapes are handled. When
/// the banner title shares a line with the details label, the error body that
/// follows belongs to the banner too, so it is skipped up to the next blank line.
/// Nothing is removed unless the first non-blank line really starts a banner.
fn strip_pasted_failure_banner(text: &str) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let Some(start) = lines.iter().position(|line| !line.trim().is_empty()) else {
        return String::new();
    };
    let has_prefix = |line: &str, prefixes: &[&str]| {
        let normalized = line.trim().to_lowercase();
        prefixes.iter().any(|prefix| normalized.starts_with(prefix))
    };
    if !has_prefix(lines[start], BANNER_TITLE_PREFIXES) {
        return text.to_string();
    }

    // The banner title's own line is always copy. When a selection joined the
    // details label onto it, the error body below belongs to the banner too.
    let banner_line = lines[start].trim().to_lowercase();
    let joined_details = BANNER_DETAILS_LABEL_PREFIXES
        .iter()
        .any(|label| banner_line.contains(label));
    let mut skip_body = joined_details;
    let mut index = start + 1;
    while index < lines.len() {
        let line = lines[index];
        if line.trim().is_empty() {
            // The banner's own block ended; user content starts after the gap.
            index += 1;
            break;
        }
        if !skip_body && has_prefix(line, BANNER_DETAILS_LABEL_PREFIXES) {
            // A separate details line: skip its label and the error body under it.
            skip_body = true;
            index += 1;
            continue;
        }
        if !skip_body {
            break;
        }
        index += 1;
    }

    lines[index..].join("\n")
}

fn strip_code_fences(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let mut in_fence = false;
    for line in text.lines() {
        if line.trim_start().starts_with("```") {
            in_fence = !in_fence;
            continue;
        }
        if !in_fence {
            result.push_str(line);
            result.push('\n');
        }
    }
    result
}
