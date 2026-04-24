//! Compression for `Read` tool output.
//!
//! **Strategy:**
//! - For very large file reads, collapse consecutive blank lines and long
//!   comment blocks to reduce context usage.
//! - Line numbers are preserved to maintain file position reference.
//! - Only triggers when output exceeds a size threshold.

use regex::Regex;

use crate::runtime::runner::tool_execution::output_compressor::filters;
use crate::runtime::runner::tool_execution::output_compressor::CompressionResult;
use crate::runtime::runner::tool_execution::output_compressor::CompressionTier;

/// Minimum result length (chars) before compression kicks in.
const MIN_COMPRESS_LEN: usize = 8_000;

/// Maximum lines in compressed output.
const MAX_LINES: usize = 400;

/// Maximum consecutive comment lines before collapsing.
const MAX_COMMENT_BLOCK: usize = 15;

lazy_static::lazy_static! {
    /// Matches common comment-only lines across languages:
    /// `//`, `#`, `--`, `/* ... */`, ` * `, `///`, `/** ... */`
    static ref COMMENT_LINE_RE: Regex = Regex::new(
        r"(?m)^\s*(?:\d+\s*[\|│]?\s*)?(?://[!/]?|#[!]?|\*|/\*|\*/|--\s|<!--|-->|;;\s|%\s)"
    ).expect("comment line regex");

    /// Matches blank lines (with optional line numbers): `  42 |`
    static ref BLANK_WITH_NUMBER_RE: Regex = Regex::new(
        r"(?m)^\s*\d*\s*[\|│]?\s*$"
    ).expect("blank line regex");

    /// Extracts line number from Read output format: `  42 | content` or `42│content`
    static ref READ_LINE_NUM_RE: Regex = Regex::new(
        r"^\s*(\d+)\s*[\|│]"
    ).expect("read line number regex");
}

// ── Public Entry Point ─────────────────────────────────────────────────────

pub(crate) fn compress(raw_result: &str, tier: CompressionTier) -> CompressionResult {
    let _ = tier;
    if raw_result.len() < MIN_COMPRESS_LEN {
        return CompressionResult {
            compressed: raw_result.to_string(),
            was_compressed: false,
        };
    }

    let cleaned = filters::collapse_blank_lines(raw_result);
    let collapsed = collapse_long_comment_blocks(&cleaned);
    let (capped, was_capped) = filters::cap_lines(&collapsed, MAX_LINES);

    let was_compressed =
        was_capped || collapsed.len() < cleaned.len() || cleaned.len() < raw_result.len();

    if !was_compressed {
        return CompressionResult {
            compressed: raw_result.to_string(),
            was_compressed: false,
        };
    }

    CompressionResult {
        compressed: capped,
        was_compressed: true,
    }
}

// ── Comment Block Collapsing ─────────────────────────────────────────────────

/// Collapse long comment blocks (>MAX_COMMENT_BLOCK consecutive lines) into
/// a summary line: `  ... (lines 42-78: 37 comment lines collapsed)`
fn collapse_long_comment_blocks(text: &str) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let mut result = String::with_capacity(text.len());
    let mut comment_start: Option<usize> = None;
    let mut i = 0;

    while i < lines.len() {
        if is_comment_line(lines[i]) {
            if comment_start.is_none() {
                comment_start = Some(i);
            }
            i += 1;
            continue;
        }

        // End of a comment block
        if let Some(start) = comment_start {
            let block_len = i - start;
            if block_len > MAX_COMMENT_BLOCK {
                // Keep first 3 and last 2 lines of the block
                for line in &lines[start..start + 3] {
                    result.push_str(line);
                    result.push('\n');
                }
                let range_hint = format_line_range(lines[start], lines[i - 1]);
                result.push_str(&format!(
                    "  ... ({}: {} comment lines collapsed)\n",
                    range_hint,
                    block_len - 5
                ));
                for line in &lines[i - 2..i] {
                    result.push_str(line);
                    result.push('\n');
                }
            } else {
                for line in &lines[start..i] {
                    result.push_str(line);
                    result.push('\n');
                }
            }
            comment_start = None;
        }

        result.push_str(lines[i]);
        result.push('\n');
        i += 1;
    }

    // Handle trailing comment block
    if let Some(start) = comment_start {
        let block_len = lines.len() - start;
        if block_len > MAX_COMMENT_BLOCK {
            for line in &lines[start..start + 3] {
                result.push_str(line);
                result.push('\n');
            }
            let range_hint = format_line_range(lines[start], lines[lines.len() - 1]);
            result.push_str(&format!(
                "  ... ({}: {} comment lines collapsed)\n",
                range_hint,
                block_len - 5
            ));
            for line in &lines[lines.len() - 2..] {
                result.push_str(line);
                result.push('\n');
            }
        } else {
            for line in &lines[start..] {
                result.push_str(line);
                result.push('\n');
            }
        }
    }

    result
}

fn format_line_range(first_line: &str, last_line: &str) -> String {
    let first_num = READ_LINE_NUM_RE
        .captures(first_line)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string());
    let last_num = READ_LINE_NUM_RE
        .captures(last_line)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string());
    match (first_num, last_num) {
        (Some(f), Some(l)) if f == l => format!("line {}", f),
        (Some(f), Some(l)) => format!("lines {}-{}", f, l),
        (Some(f), None) => format!("line {}", f),
        _ => String::new(),
    }
}

fn is_comment_line(line: &str) -> bool {
    COMMENT_LINE_RE.is_match(line)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn pad(s: &str) -> String {
        let needed = MIN_COMPRESS_LEN.saturating_sub(s.len()) + 100;
        format!("{}{}", s, "\n".repeat(needed))
    }

    #[test]
    fn short_output_not_compressed() {
        let input = "line 1\nline 2\nline 3\n";
        let result = compress(input, CompressionTier::Standard);
        assert!(!result.was_compressed);
    }

    #[test]
    fn blank_lines_collapsed() {
        let mut input = String::new();
        input.push_str("code line 1\n");
        for _ in 0..20 {
            input.push('\n');
        }
        input.push_str("code line 2\n");
        let padded = pad(&input);
        let result = compress(&padded, CompressionTier::Standard);
        assert!(result.was_compressed);
        // Should not have 20 consecutive blank lines anymore
        assert!(!result.compressed.contains("\n\n\n\n\n"));
    }

    #[test]
    fn long_comment_block_collapsed() {
        let mut input = String::new();
        input.push_str("fn main() {\n");
        for i in 0..30 {
            input.push_str(&format!("// comment line {}\n", i));
        }
        input.push_str("    let x = 1;\n");
        let padded = pad(&input);
        let result = compress(&padded, CompressionTier::Standard);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("comment lines collapsed"));
        // Should still have the first few comments
        assert!(result.compressed.contains("// comment line 0"));
    }

    #[test]
    fn comment_block_with_line_numbers() {
        let mut input = String::new();
        input.push_str("   1 | fn main() {\n");
        for i in 2..25 {
            input.push_str(&format!("{:>4} | // comment about thing {}\n", i, i));
        }
        input.push_str("  25 |     let x = 1;\n");
        let padded = pad(&input);
        let result = compress(&padded, CompressionTier::Standard);
        assert!(result.was_compressed);
        // Should show line range in the collapsed notice
        assert!(result.compressed.contains("lines 2-24"));
    }

    #[test]
    fn short_comment_block_preserved() {
        let mut input = String::new();
        input.push_str("fn main() {\n");
        for i in 0..5 {
            input.push_str(&format!("// comment {}\n", i));
        }
        input.push_str("    let x = 1;\n");
        let padded = pad(&input);
        let result = compress(&padded, CompressionTier::Standard);
        // Short comment blocks should be kept
        assert!(result.compressed.contains("// comment 0"));
        assert!(result.compressed.contains("// comment 4"));
    }

    #[test]
    fn large_file_capped() {
        let input: String = (0..600)
            .map(|i| format!("{:>4} | fn func_{}() {{}}\n", i, i))
            .collect();
        let result = compress(&input, CompressionTier::Standard);
        assert!(result.was_compressed);
        let line_count = result.compressed.lines().count();
        assert!(line_count <= MAX_LINES + 5); // cap + truncation notice
    }

    // ── Edge cases ──

    #[test]
    fn empty_input_not_compressed() {
        let result = compress("", CompressionTier::Standard);
        assert!(!result.was_compressed);
    }

    #[test]
    fn code_only_no_comments_not_compressed() {
        let mut input = String::new();
        for i in 0..100 {
            input.push_str(&format!("{:>4} | let x = {};\n", i, i));
        }
        // Below MIN_COMPRESS_LEN threshold
        if input.len() < 8000 {
            let result = compress(&input, CompressionTier::Standard);
            assert!(!result.was_compressed);
        }
    }

    #[test]
    fn comment_block_exactly_at_threshold() {
        // Exactly MAX_COMMENT_BLOCK lines → not collapsed
        let mut input = String::new();
        input.push_str("   1 | fn main() {\n");
        for i in 2..(2 + MAX_COMMENT_BLOCK) {
            input.push_str(&format!("{:>4} | // comment {}\n", i, i));
        }
        input.push_str("  18 | }\n");
        let padded = pad(&input);
        let result = compress(&padded, CompressionTier::Standard);
        // Should NOT collapse — exactly at threshold
        assert!(!result.compressed.contains("comment lines collapsed"));
    }

    #[test]
    fn comment_block_one_over_threshold() {
        // MAX_COMMENT_BLOCK + 1 lines → should collapse
        let mut input = String::new();
        input.push_str("   1 | fn main() {\n");
        for i in 2..(2 + MAX_COMMENT_BLOCK + 1) {
            input.push_str(&format!("{:>4} | // comment {}\n", i, i));
        }
        input.push_str("  19 | }\n");
        let padded = pad(&input);
        let result = compress(&padded, CompressionTier::Standard);
        assert!(result.compressed.contains("comment lines collapsed"));
    }

    #[test]
    fn multiple_comment_blocks() {
        let mut input = String::new();
        input.push_str("   1 | fn a() {\n");
        for i in 2..20 {
            input.push_str(&format!("{:>4} | // block 1 comment {}\n", i, i));
        }
        input.push_str("  20 | }\n");
        input.push_str("  21 | fn b() {\n");
        for i in 22..40 {
            input.push_str(&format!("{:>4} | // block 2 comment {}\n", i, i));
        }
        input.push_str("  40 | }\n");
        let padded = pad(&input);
        let result = compress(&padded, CompressionTier::Standard);
        assert!(result.was_compressed);
        // Both blocks should be collapsed
        let collapse_count = result.compressed.matches("comment lines collapsed").count();
        assert_eq!(collapse_count, 2);
    }

    #[test]
    fn mixed_comment_styles() {
        let mut input = String::new();
        input.push_str("   1 | fn main() {\n");
        input.push_str("   2 | // rust line comment\n");
        input.push_str("   3 | # shell comment\n");
        input.push_str("   4 | /* block */\n");
        input.push_str("   5 | * continued\n");
        input.push_str("   6 | -- sql comment\n");
        for i in 7..25 {
            input.push_str(&format!("{:>4} | // more {}\n", i, i));
        }
        input.push_str("  25 | }\n");
        let padded = pad(&input);
        let result = compress(&padded, CompressionTier::Standard);
        assert!(result.compressed.contains("comment lines collapsed"));
    }

    #[test]
    fn line_number_range_single_line() {
        let mut input = String::new();
        input.push_str("   1 | code\n");
        input.push_str("   2 | // only one comment\n");
        // Need enough comments to trigger collapse
        for i in 3..20 {
            input.push_str(&format!("{:>4} | // comment {}\n", i, i));
        }
        input.push_str("  20 | more code\n");
        let padded = pad(&input);
        let result = compress(&padded, CompressionTier::Standard);
        // Should show line range
        assert!(result.compressed.contains("lines 2-19"));
    }

    #[test]
    fn format_line_range_no_numbers() {
        // Lines without number prefix → empty range hint
        let result = format_line_range("// comment", "// comment");
        assert!(result.is_empty());
    }

    #[test]
    fn format_line_range_same_number() {
        let result = format_line_range("  42 | // comment", "  42 | // comment");
        assert_eq!(result, "line 42");
    }
}
