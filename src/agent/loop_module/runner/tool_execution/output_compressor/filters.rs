//! Low-level text filters shared by all scenario compressors.

use regex::Regex;

// ── ANSI Strip ──────────────────────────────────────────────────────────────

lazy_static::lazy_static! {
    /// Matches ANSI escape sequences: CSI (ESC [ … m) and OSC (ESC ] … ST).
    static ref ANSI_RE: Regex = Regex::new(
        r"(\x1b\[[0-9;]*[A-Za-z]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)|\x1b[()][A-B012])"
    ).expect("ANSI regex must compile");
}

/// Strip all ANSI escape sequences from the input string.
pub(crate) fn strip_ansi(input: &str) -> String {
    ANSI_RE.replace_all(input, "").into_owned()
}

// ── Line Capping ────────────────────────────────────────────────────────────

/// Default maximum number of output lines before truncation.
pub(crate) const DEFAULT_MAX_LINES: usize = 200;

/// Cap the number of lines, keeping the first `head` and last `tail` lines
/// when the total exceeds `max_lines`.
///
/// Returns `(capped_text, was_capped)`.
pub(crate) fn cap_lines(input: &str, max_lines: usize) -> (String, bool) {
    let lines: Vec<&str> = input.lines().collect();
    if lines.len() <= max_lines {
        return (input.to_string(), false);
    }

    let head = max_lines / 2;
    let tail = max_lines - head;
    let omitted = lines.len() - head - tail;

    let mut result = String::with_capacity(input.len() / 2);
    for line in &lines[..head] {
        result.push_str(line);
        result.push('\n');
    }
    result.push_str(&format!("\n... ({omitted} lines omitted) ...\n\n"));
    for (i, line) in lines[lines.len() - tail..].iter().enumerate() {
        result.push_str(line);
        if i < tail - 1 {
            result.push('\n');
        }
    }

    (result, true)
}

// ── Byte Capping ────────────────────────────────────────────────────────────

/// Default maximum byte size before truncation.
pub(crate) const DEFAULT_MAX_BYTES: usize = 64 * 1024; // 64 KB

/// Cap the byte length of the output, keeping a head portion and a tail
/// portion with an omission marker in between.
///
/// Returns `(capped_text, was_capped)`.
pub(crate) fn cap_bytes(input: &str, max_bytes: usize) -> (String, bool) {
    if input.len() <= max_bytes {
        return (input.to_string(), false);
    }

    let marker = "\n... (output truncated) ...\n";
    let usable = max_bytes.saturating_sub(marker.len());
    let head_bytes = usable / 2;
    let tail_bytes = usable - head_bytes;

    // Find char-boundary-safe split points.
    let head_end = char_boundary_floor(input, head_bytes);
    let tail_start = char_boundary_ceil(input, input.len() - tail_bytes);

    let mut result = String::with_capacity(max_bytes + 64);
    result.push_str(&input[..head_end]);
    result.push_str(marker);
    result.push_str(&input[tail_start..]);

    (result, true)
}

/// Find the largest index ≤ `pos` that is a valid char boundary.
fn char_boundary_floor(s: &str, pos: usize) -> usize {
    let mut idx = pos.min(s.len());
    while idx > 0 && !s.is_char_boundary(idx) {
        idx -= 1;
    }
    idx
}

/// Find the smallest index ≥ `pos` that is a valid char boundary.
fn char_boundary_ceil(s: &str, pos: usize) -> usize {
    let mut idx = pos.min(s.len());
    while idx < s.len() && !s.is_char_boundary(idx) {
        idx += 1;
    }
    idx
}

// ── Consecutive Blank Line Collapse ─────────────────────────────────────────

/// Collapse runs of 3+ consecutive blank lines into a single blank line.
pub(crate) fn collapse_blank_lines(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut blank_run = 0u32;

    for line in input.lines() {
        if line.trim().is_empty() {
            blank_run += 1;
            if blank_run <= 2 {
                result.push('\n');
            }
        } else {
            blank_run = 0;
            result.push_str(line);
            result.push('\n');
        }
    }

    // Remove trailing whitespace
    let trimmed = result.trim_end().len();
    result.truncate(trimmed);
    if !result.is_empty() {
        result.push('\n');
    }

    result
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── strip_ansi ──

    #[test]
    fn strip_ansi_removes_colors() {
        let input = "\x1b[31mERROR\x1b[0m: something failed";
        assert_eq!(strip_ansi(input), "ERROR: something failed");
    }

    #[test]
    fn strip_ansi_removes_bold() {
        let input = "\x1b[1m\x1b[32mPASSED\x1b[0m";
        assert_eq!(strip_ansi(input), "PASSED");
    }

    #[test]
    fn strip_ansi_no_sequences() {
        let input = "plain text with no escapes";
        assert_eq!(strip_ansi(input), input);
    }

    #[test]
    fn strip_ansi_complex_sequence() {
        let input = "\x1b[38;5;196mred\x1b[0m \x1b[48;2;0;128;0mgreen bg\x1b[0m";
        assert_eq!(strip_ansi(input), "red green bg");
    }

    // ── cap_lines ──

    #[test]
    fn cap_lines_under_limit() {
        let input = "line1\nline2\nline3\n";
        let (result, capped) = cap_lines(input, 10);
        assert!(!capped);
        assert_eq!(result, input);
    }

    #[test]
    fn cap_lines_over_limit() {
        let lines: Vec<String> = (0..20).map(|i| format!("line {i}")).collect();
        let input = lines.join("\n");
        let (result, capped) = cap_lines(&input, 6);
        assert!(capped);
        assert!(result.contains("line 0"));
        assert!(result.contains("line 1"));
        assert!(result.contains("line 2"));
        assert!(result.contains("14 lines omitted"));
        assert!(result.contains("line 17"));
        assert!(result.contains("line 18"));
        assert!(result.contains("line 19"));
    }

    #[test]
    fn cap_lines_exact_limit() {
        let lines: Vec<String> = (0..5).map(|i| format!("line {i}")).collect();
        let input = lines.join("\n");
        let (_, capped) = cap_lines(&input, 5);
        assert!(!capped);
    }

    // ── cap_bytes ──

    #[test]
    fn cap_bytes_under_limit() {
        let input = "short text";
        let (result, capped) = cap_bytes(input, 1024);
        assert!(!capped);
        assert_eq!(result, input);
    }

    #[test]
    fn cap_bytes_over_limit() {
        let input = "a".repeat(1000);
        let (result, capped) = cap_bytes(&input, 200);
        assert!(capped);
        assert!(result.len() <= 300); // 200 + marker overhead
        assert!(result.contains("output truncated"));
    }

    #[test]
    fn cap_bytes_multibyte_chars() {
        // Ensure we don't split a multi-byte UTF-8 character
        let input = "🔥".repeat(100); // Each emoji is 4 bytes
        let (result, capped) = cap_bytes(&input, 100);
        assert!(capped);
        // Result should be valid UTF-8
        assert!(result.is_char_boundary(result.len()));
    }

    // ── collapse_blank_lines ──

    #[test]
    fn collapse_blank_lines_basic() {
        let input = "a\n\n\n\n\nb\n";
        let result = collapse_blank_lines(input);
        assert_eq!(result, "a\n\n\nb\n");
    }

    #[test]
    fn collapse_blank_lines_keeps_single() {
        let input = "a\n\nb\n";
        let result = collapse_blank_lines(input);
        assert_eq!(result, "a\n\nb\n");
    }

    #[test]
    fn collapse_blank_lines_no_blanks() {
        let input = "a\nb\nc\n";
        let result = collapse_blank_lines(input);
        assert_eq!(result, "a\nb\nc\n");
    }
}
