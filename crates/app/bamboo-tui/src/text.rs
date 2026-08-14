//! Terminal-cell-aware text helpers.
//!
//! All clipping operates on grapheme clusters, never scalar values. This
//! keeps combining marks and ZWJ emoji attached to their base glyph while
//! still measuring the terminal cells they occupy.

use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

pub fn display_width(value: &str) -> usize {
    UnicodeWidthStr::width(value)
}

pub fn clip_cells(value: &str, max_width: usize) -> String {
    if max_width == 0 {
        return String::new();
    }
    if display_width(value) <= max_width && !value.contains('\n') {
        return value.to_string();
    }

    let target = max_width.saturating_sub(1);
    let mut output = String::new();
    let mut used = 0;
    for grapheme in value.graphemes(true) {
        if grapheme.contains('\n') {
            break;
        }
        let width = display_width(grapheme);
        if used + width > target {
            break;
        }
        output.push_str(grapheme);
        used += width;
    }
    output.push('…');
    output
}

pub fn clip_tail_cells(value: &str, max_width: usize) -> String {
    if max_width == 0 {
        return String::new();
    }
    if display_width(value) <= max_width && !value.contains('\n') {
        return value.to_string();
    }

    let target = max_width.saturating_sub(1);
    let mut suffix = Vec::new();
    let mut used = 0;
    for grapheme in value.graphemes(true).rev() {
        if grapheme.contains('\n') {
            break;
        }
        let width = display_width(grapheme);
        if used + width > target {
            break;
        }
        suffix.push(grapheme);
        used += width;
    }
    suffix.reverse();
    format!("…{}", suffix.concat())
}

pub fn truncate_cells(value: &str, max_width: usize) -> String {
    let mut output = clip_cells(value, max_width);
    let width = display_width(&output);
    if width < max_width {
        output.extend(std::iter::repeat_n(' ', max_width - width));
    }
    output
}

pub fn hard_wrap(value: &str, width: usize) -> Vec<String> {
    let width = width.max(1);
    let mut output = Vec::new();
    for logical in value.split('\n') {
        let mut current = String::new();
        let mut used = 0;
        for grapheme in logical.graphemes(true) {
            let grapheme_width = display_width(grapheme);
            if !current.is_empty() && used + grapheme_width > width {
                output.push(std::mem::take(&mut current));
                used = 0;
            }
            current.push_str(grapheme);
            used += grapheme_width;
        }
        output.push(current);
    }
    if output.is_empty() {
        output.push(String::new());
    }
    output
}

pub fn wrapped_lines(value: &str, width: usize) -> Vec<String> {
    hard_wrap(value, width)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clipping_never_splits_combining_or_zwj_graphemes() {
        assert_eq!(clip_cells("Ae\u{301}B", 2), "A…");
        assert_eq!(clip_cells("A👨‍👩‍👧‍👦B", 3), "A…");
        assert_eq!(clip_tail_cells("Ae\u{301}B", 2), "…B");
        assert_eq!(clip_tail_cells("A👨‍👩‍👧‍👦B", 3), "…B");
    }

    #[test]
    fn wrapping_preserves_exact_grapheme_content() {
        let input = "e\u{301}👨‍👩‍👧‍👦界suffix";
        let lines = hard_wrap(input, 3);
        assert_eq!(lines.concat(), input);
        assert!(lines.iter().all(|line| display_width(line) <= 3));
    }

    #[test]
    fn wrapping_never_splits_graphemes_at_cell_boundaries() {
        let input = "ab👨‍👩‍👧‍👦e\u{301}界tail";
        let lines = hard_wrap(input, 4);
        assert_eq!(lines.concat(), input);
        assert!(lines.iter().all(|line| display_width(line) <= 4));
        assert!(lines.iter().any(|line| line.contains("👨‍👩‍👧‍👦")));
        assert!(lines.iter().any(|line| line.contains("e\u{301}")));
    }
}
