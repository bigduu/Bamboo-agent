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
}

// ── Public Entry Point ─────────────────────────────────────────────────────

pub(crate) fn compress(raw_result: &str) -> CompressionResult {
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
/// a summary line: `  ... (23 comment lines collapsed)`
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
                result.push_str(&format!(
                    "  ... ({} comment lines collapsed)\n",
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
            result.push_str(&format!(
                "  ... ({} comment lines collapsed)\n",
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
        let result = compress(input);
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
        let result = compress(&padded);
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
        let result = compress(&padded);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("comment lines collapsed"));
        // Should still have the first few comments
        assert!(result.compressed.contains("// comment line 0"));
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
        let result = compress(&padded);
        // Short comment blocks should be kept
        assert!(result.compressed.contains("// comment 0"));
        assert!(result.compressed.contains("// comment 4"));
    }

    #[test]
    fn large_file_capped() {
        let input: String = (0..600)
            .map(|i| format!("{:>4} | fn func_{}() {{}}\n", i, i))
            .collect();
        let result = compress(&input);
        assert!(result.was_compressed);
        let line_count = result.compressed.lines().count();
        assert!(line_count <= MAX_LINES + 5); // cap + truncation notice
    }
}
