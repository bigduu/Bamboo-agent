//! Terminal-cell-aware text helpers.
//!
//! All clipping operates on grapheme clusters, never scalar values. This
//! keeps combining marks and ZWJ emoji attached to their base glyph while
//! still measuring the terminal cells they occupy.

use std::borrow::Cow;

use ratatui::buffer::CellWidth;
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

pub fn display_width(value: &str) -> usize {
    UnicodeWidthStr::width(value)
}

/// Normalize one source row for display while retaining the original for copy.
pub(crate) fn code_source_line(line: &str) -> Cow<'_, str> {
    if !line.chars().any(char::is_control) {
        return Cow::Borrowed(line);
    }
    let mut display = String::with_capacity(line.len());
    for character in line.chars() {
        match character {
            '\t' => display.push_str("    "),
            '\r' => display.push('␍'),
            '\u{1b}' => display.push('␛'),
            character if character.is_control() => display.push('�'),
            character => display.push(character),
        }
    }
    Cow::Owned(display)
}

/// Measure columns using the same grapheme widths as [`slice_cells`].
pub(crate) fn grapheme_cell_width(value: &str) -> usize {
    value
        .graphemes(true)
        .map(|grapheme| usize::from(grapheme.cell_width()))
        .fold(0, usize::saturating_add)
}

/// Show a window of terminal cells from an already normalized source row.
/// Keep the columns after a partially visible left-edge grapheme aligned.
pub(crate) fn slice_cells(value: &str, first: usize, width: usize) -> String {
    let end = first.saturating_add(width);
    let mut output = String::new();
    if end == first {
        return output;
    }

    let mut column = 0usize;
    for grapheme in value.graphemes(true) {
        if column >= end {
            break;
        }
        let next = column.saturating_add(usize::from(grapheme.cell_width()));
        if column < first {
            if next > first {
                output.extend(std::iter::repeat_n(' ', next.min(end) - first));
            }
        } else if next <= end {
            output.push_str(grapheme);
        } else {
            break;
        }
        column = next;
    }
    output
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
    fn code_source_display_borrows_clean_text_and_expands_tabs() {
        let clean = "  界e\u{301}👨‍👩‍👧‍👦  ";
        assert!(matches!(code_source_line(clean), Cow::Borrowed(value) if value == clean));
        let source = "\t界\te\u{301}👨‍👩‍👧‍👦\u{1b}\r\u{7f}";
        let display = code_source_line(source);
        assert!(matches!(display, Cow::Owned(_)));
        assert_eq!(display, "    界    e\u{301}👨‍👩‍👧‍👦␛␍�");
        assert_eq!(slice_cells(&display, 5, 5), "     ");
    }

    #[test]
    fn cell_windows_keep_complete_graphemes_and_pad_only_the_left_overlap() {
        let source = "A界e\u{301}👨‍👩‍👧‍👦Z";
        for (first, width, expected) in [
            (0, 2, "A"),
            (1, 2, "界"),
            (2, 1, " "),
            (2, 4, " e\u{301}👨‍👩‍👧‍👦"),
            (3, 2, "e\u{301}"),
            (4, 1, ""),
            (4, 2, "👨‍👩‍👧‍👦"),
            (5, 2, " Z"),
            (6, 4, "Z"),
        ] {
            let result = slice_cells(source, first, width);
            assert_eq!(result, expected, "first={first}, width={width}");
            assert!(display_width(&result) <= width);
        }
    }

    #[test]
    fn grapheme_cell_width_reaches_the_suffix_after_arabic_ligatures() {
        let source = "لا".repeat(56) + "SUFFIX";
        let viewport_width = 56;
        let whole_width = display_width(&source);
        let window_width = grapheme_cell_width(&source);
        assert_eq!((whole_width, window_width), (62, 118));
        assert!(
            !slice_cells(&source, whole_width - viewport_width, viewport_width).contains("SUFFIX")
        );
        assert!(
            slice_cells(&source, window_width - viewport_width, viewport_width).ends_with("SUFFIX")
        );
    }

    #[test]
    fn cell_windows_match_ratatui_width_for_halfwidth_dakuten() {
        use ratatui::{buffer::Buffer, layout::Rect, style::Style};

        let source = "AｶﾞZ";
        let mut buffer = Buffer::empty(Rect::new(0, 0, 10, 1));
        let (right, _) = buffer.set_stringn(0, 0, source, 10, Style::default());
        assert_eq!((display_width(source), right), (4, 5));
        assert_eq!(grapheme_cell_width(source), usize::from(right));
        assert_eq!(slice_cells(source, 0, 3), "A");
        assert_eq!(slice_cells(source, 1, 3), "ｶﾞ");
        assert_eq!(slice_cells(source, 2, 3), "  Z");
        assert_eq!(slice_cells(source, grapheme_cell_width(source) - 1, 1), "Z");
    }

    #[test]
    fn cell_windows_handle_empty_and_saturating_bounds() {
        assert_eq!(slice_cells("", 0, 4), "");
        assert_eq!(slice_cells("界", 0, 0), "");
        assert_eq!(slice_cells("A界", 3, 3), "");
        assert_eq!(slice_cells("A界", 1, usize::MAX), "界");
        assert_eq!(slice_cells("A界", usize::MAX - 1, 3), "");
        assert_eq!(slice_cells("A界", usize::MAX, usize::MAX), "");
    }

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
