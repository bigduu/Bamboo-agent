//! Compression for `WebFetch` tool output.
//!
//! **Strategy:**
//! - Strip navigation elements, footers, cookie banners, and other boilerplate.
//! - Collapse long sequences of blank lines.
//! - Cap total lines to avoid huge HTML pages consuming context.
//! - Strip repetitive link-list sections.

use regex::Regex;
use std::sync::LazyLock;

use crate::runtime::runner::tool_execution::output_compressor::filters;
use crate::runtime::runner::tool_execution::output_compressor::CompressionResult;
use crate::runtime::runner::tool_execution::output_compressor::CompressionTier;

/// Minimum result length (chars) before compression kicks in.
const MIN_COMPRESS_LEN: usize = 6_000;

/// Maximum lines in compressed output.
const MAX_LINES: usize = 300;

/// Maximum consecutive very-short lines (nav/menu noise) before collapsing.
const MAX_SHORT_LINE_RUN: usize = 10;

/// Lines shorter than this in a short-line-run are considered nav noise.
const SHORT_LINE_THRESHOLD: usize = 40;

/// Matches common web noise patterns (nav, footer, cookie, etc.)
static WEB_NOISE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
    r"(?i)(?:cookie(?:s?\s+(?:policy|settings|preferences|notice))|privacy\s+policy|terms\s+(?:of\s+(?:use|service))|all\s+rights?\s+reserved|©\s*\d{4}|powered\s+by|sign\s+(?:in|up)\s+(?:with|to)|(?:follow|share|tweet|like)\s+(?:us|on)|loading\.{3}|skip\s+to\s+(?:main\s+)?content)"
).expect("web noise regex")
});

/// Matches breadcrumb-style navigation: `Home > Products > Widgets`
static BREADCRUMB_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?m)^(?:\w[\w\s]+\s*(?:>|›|»|/)\s*){2,}").expect("breadcrumb regex")
});

// ── Public Entry Point ─────────────────────────────────────────────────────

pub(crate) fn compress(raw_result: &str, tier: CompressionTier) -> CompressionResult {
    let _ = tier;
    if raw_result.len() < MIN_COMPRESS_LEN {
        return CompressionResult {
            compressed: raw_result.to_string(),
            was_compressed: false,
        };
    }

    // Stage 1: Strip web noise lines
    let stripped = strip_noise_lines(raw_result);

    // Stage 2: Collapse consecutive blank lines
    let collapsed = filters::collapse_blank_lines(&stripped);

    // Stage 3: Collapse long runs of short lines (nav menus)
    let nav_cleaned = collapse_short_line_runs(&collapsed);

    // Stage 4: Cap total lines
    let (capped, was_capped) = filters::cap_lines(&nav_cleaned, MAX_LINES);

    let was_compressed = was_capped
        || nav_cleaned.len() < collapsed.len()
        || collapsed.len() < stripped.len()
        || stripped.len() < raw_result.len();

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

// ── Noise Stripping ──────────────────────────────────────────────────────────

fn strip_noise_lines(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let mut stripped_count = 0;

    for line in text.lines() {
        let trimmed = line.trim();

        // Skip empty lines (will be handled by blank line collapse)
        if trimmed.is_empty() {
            result.push('\n');
            continue;
        }

        // Skip common web noise
        if WEB_NOISE_RE.is_match(trimmed) {
            stripped_count += 1;
            continue;
        }

        // Skip pure breadcrumbs
        if BREADCRUMB_RE.is_match(trimmed) {
            stripped_count += 1;
            continue;
        }

        result.push_str(line);
        result.push('\n');
    }

    if stripped_count > 0 {
        result.push_str(&format!(
            "\n[{} web noise/navigation lines stripped]\n",
            stripped_count
        ));
    }

    result
}

// ── Short Line Run Collapse ──────────────────────────────────────────────────

/// Collapse long runs of very short lines (typical of navigation menus)
/// into a single summary line.
fn collapse_short_line_runs(text: &str) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let mut result = String::with_capacity(text.len());
    let mut short_run_start: Option<usize> = None;
    let mut i = 0;

    while i < lines.len() {
        let trimmed = lines[i].trim();
        let is_short = !trimmed.is_empty() && trimmed.len() < SHORT_LINE_THRESHOLD;

        if is_short {
            if short_run_start.is_none() {
                short_run_start = Some(i);
            }
            i += 1;
            continue;
        }

        // End of short run
        if let Some(start) = short_run_start {
            let run_len = i - start;
            if run_len > MAX_SHORT_LINE_RUN {
                // Keep first 3 lines
                for line in &lines[start..start.saturating_add(3).min(i)] {
                    result.push_str(line);
                    result.push('\n');
                }
                result.push_str(&format!(
                    "  ... ({} short navigation/menu lines collapsed)\n",
                    run_len - 3
                ));
            } else {
                for line in &lines[start..i] {
                    result.push_str(line);
                    result.push('\n');
                }
            }
            short_run_start = None;
        }

        result.push_str(lines[i]);
        result.push('\n');
        i += 1;
    }

    // Handle trailing short run
    if let Some(start) = short_run_start {
        let run_len = lines.len() - start;
        if run_len > MAX_SHORT_LINE_RUN {
            for line in &lines[start..start.saturating_add(3).min(lines.len())] {
                result.push_str(line);
                result.push('\n');
            }
            result.push_str(&format!(
                "  ... ({} short navigation/menu lines collapsed)\n",
                run_len - 3
            ));
        } else {
            for line in &lines[start..] {
                result.push_str(line);
                result.push('\n');
            }
        }
    }

    result
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
        let input = "Hello World\nSome content\n";
        let result = compress(input, CompressionTier::Standard);
        assert!(!result.was_compressed);
    }

    #[test]
    fn noise_lines_stripped() {
        let input = pad(&format!(
            "{}\n{}\n{}\n{}\n{}",
            "Welcome to our website",
            "This is the main content about Rust programming.",
            "Cookie Policy - We use cookies to improve your experience.",
            "© 2024 All Rights Reserved.",
            "More useful content here about async/await in Rust."
        ));
        let result = compress(&input, CompressionTier::Standard);
        assert!(result.was_compressed);
        assert!(!result.compressed.contains("Cookie Policy"));
        assert!(!result.compressed.contains("All Rights Reserved"));
        assert!(result.compressed.contains("Rust programming"));
    }

    #[test]
    fn navigation_menu_collapsed() {
        let mut input = String::new();
        input.push_str("Welcome to the documentation\n\n");
        // Simulate a long nav menu (short lines)
        for i in 0..20 {
            input.push_str(&format!("Nav Item {}\n", i));
        }
        input.push_str(
            "\nThis is the actual important documentation content that spans a much longer line.\n",
        );
        let padded = pad(&input);
        let result = compress(&padded, CompressionTier::Standard);
        assert!(result.was_compressed);
        assert!(result
            .compressed
            .contains("navigation/menu lines collapsed"));
        // Should keep first few nav items
        assert!(result.compressed.contains("Nav Item 0"));
    }

    #[test]
    fn blank_lines_collapsed() {
        let mut input = String::new();
        input.push_str("Content block 1\n");
        for _ in 0..20 {
            input.push('\n');
        }
        input.push_str("Content block 2\n");
        let padded = pad(&input);
        let result = compress(&padded, CompressionTier::Standard);
        assert!(result.was_compressed);
        // Should not have 20 blank lines anymore
        assert!(!result.compressed.contains("\n\n\n\n\n"));
    }

    #[test]
    fn large_page_capped() {
        let input: String = (0..500)
            .map(|i| {
                format!(
                    "This is paragraph {} of the very long documentation page with sufficient length.\n",
                    i
                )
            })
            .collect();
        let result = compress(&input, CompressionTier::Standard);
        assert!(result.was_compressed);
        let line_count = result.compressed.lines().count();
        assert!(line_count <= MAX_LINES + 5);
    }

    #[test]
    fn breadcrumbs_stripped() {
        let input = pad(&format!(
            "{}\n{}\n{}",
            "Home > Documentation > API Reference > Authentication",
            "This is the actual authentication documentation with details.",
            "Products › Security › OAuth2 Setup Guide"
        ));
        let result = compress(&input, CompressionTier::Standard);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("authentication documentation"));
        assert!(result.compressed.contains("web noise"));
    }

    #[test]
    fn mixed_content_preserved() {
        let mut input = String::new();
        input.push_str("# API Documentation\n\n");
        input.push_str("## Authentication\n\n");
        input.push_str("Use Bearer tokens for authentication.\n\n");
        input.push_str(
            "```bash\ncurl -H \"Authorization: Bearer TOKEN\" https://api.example.com\n```\n\n",
        );
        input.push_str("## Response Format\n\n");
        input.push_str("All responses are JSON.\n");
        let padded = pad(&input);
        let result = compress(&padded, CompressionTier::Standard);
        // Good content should be preserved
        assert!(result.compressed.contains("API Documentation"));
        assert!(result.compressed.contains("Bearer tokens"));
        assert!(result.compressed.contains("curl"));
    }

    // ── Edge cases ──

    #[test]
    fn empty_input_not_compressed() {
        let result = compress("", CompressionTier::Standard);
        assert!(!result.was_compressed);
    }

    #[test]
    fn short_content_not_compressed() {
        let input = "Some short web page content\n";
        let result = compress(input, CompressionTier::Standard);
        assert!(!result.was_compressed);
    }

    #[test]
    fn only_noise_lines() {
        let mut input = String::new();
        for _ in 0..200 {
            input.push_str("Cookie Policy - We use cookies\n");
        }
        let padded = pad(&input);
        let result = compress(&padded, CompressionTier::Standard);
        assert!(result.was_compressed);
        assert!(!result.compressed.contains("Cookie Policy"));
        assert!(result.compressed.contains("web noise"));
    }

    #[test]
    fn short_line_run_below_threshold() {
        // 5 short lines → below MAX_SHORT_LINE_RUN → not collapsed
        let mut input = String::new();
        input.push_str("Header content\n\n");
        for i in 0..5 {
            input.push_str(&format!("Item {}\n", i));
        }
        input.push_str("Footer content\n");
        let padded = pad(&input);
        let result = compress(&padded, CompressionTier::Standard);
        // Short lines should be preserved
        assert!(result.compressed.contains("Item 0"));
        assert!(result.compressed.contains("Item 4"));
    }

    #[test]
    fn mixed_noise_and_content_preserves_content() {
        let input = pad(&format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            "Important API endpoint documentation",
            "Cookie Policy settings",
            "Critical error handling guide",
            "© 2024 All Rights Reserved",
            "Terms of Service agreement",
            "Actual useful troubleshooting content"
        ));
        let result = compress(&input, CompressionTier::Standard);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("API endpoint"));
        assert!(result.compressed.contains("error handling"));
        assert!(result.compressed.contains("troubleshooting"));
        assert!(!result.compressed.contains("Cookie Policy"));
        assert!(!result.compressed.contains("All Rights Reserved"));
    }

    #[test]
    fn unicode_content_preserved() {
        let input = pad("日本語のコンテンツ\n中文内容\n한국어 콘텐츠\nEmoji: 🎉🚀");
        let result = compress(&input, CompressionTier::Standard);
        assert!(result.compressed.contains("日本語"));
        assert!(result.compressed.contains("中文"));
        assert!(result.compressed.contains("한국어"));
    }
}
