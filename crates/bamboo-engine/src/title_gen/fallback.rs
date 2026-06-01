//! Heuristic fallback title generator (no LLM call).

const MAX_FALLBACK_LEN: usize = 60;

/// Derive a short title from the first user message using simple heuristics.
///
/// - Strips fenced code blocks (```...```).
/// - Collapses whitespace.
/// - Takes the first sentence (up to `.`/`!`/`?`/newline).
/// - Caps at `MAX_FALLBACK_LEN` characters.
pub fn heuristic_title(first_user_text: &str) -> String {
    let cleaned = strip_code_fences(first_user_text);
    let collapsed: String = cleaned.split_whitespace().collect::<Vec<_>>().join(" ");
    let first_sentence = collapsed.split(['.', '!', '?', '\n']).next().unwrap_or("");
    let limit = first_sentence
        .char_indices()
        .nth(MAX_FALLBACK_LEN)
        .map(|(i, _)| i)
        .unwrap_or(first_sentence.len());
    first_sentence[..limit]
        .trim_end_matches([' ', ',', ';', ':', '-'])
        .trim()
        .to_string()
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
