use std::iter::Peekable;

use ratatui::buffer::CellWidth;
use ratatui::layout::Alignment;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use unicode_segmentation::UnicodeSegmentation;

use crate::text::clip_cells;
use crate::theme::colors;

use super::{fence::SourceLine, heading, ordered_item, parse_inline};

const COLUMN_GAP: &str = "   ";
const MAX_COLUMNS: usize = 12;
const MAX_BODY_ROWS: usize = 32;
const MAX_CONTENT_LINES: usize = 64;
const CLIPPED_LABEL: &str = "… Table preview clipped";
type Row = Vec<Line<'static>>;

pub(super) fn render_if_table<'a>(
    header: &str,
    input: &mut Peekable<impl Iterator<Item = SourceLine<'a>>>,
    width: usize,
    opens_heading: impl Fn(SourceLine<'a>) -> bool,
) -> Option<Vec<Line<'static>>> {
    let delimiter = input.peek()?.text;
    if !delimiter.contains('|') || ends_table(header) {
        return None;
    }
    // Validate the whole header and delimiter, including undisplayed columns.
    let mut alignments = split_row(delimiter, usize::MAX)
        .iter()
        .map(|cell| column_alignment(cell))
        .collect::<Option<Vec<_>>>()?;
    let mut header = split_row(header, usize::MAX);
    if header.len() != alignments.len() {
        return None;
    }
    input.next();

    let mut clipped = alignments.len() > MAX_COLUMNS;
    alignments.truncate(MAX_COLUMNS);
    let columns = alignments.len();
    header.truncate(columns);
    let mut rows = vec![styled_row(header, columns, false)];
    while let Some(line) = input.next_if(|line| !ends_table(line.text) && !opens_heading(*line)) {
        if rows.len() > MAX_BODY_ROWS {
            clipped = true;
            continue;
        }
        // A marker-only row still inside the table is an unfinished rule.
        let literal = super::rule_markers(line.text).is_some();
        rows.push(styled_row(split_row(line.text, columns), columns, literal));
    }

    let (mut lines, lines_clipped) = match column_widths(&rows, columns, width) {
        Some(widths) => render_aligned(&rows, &widths, &alignments),
        None => render_stacked(&rows, width),
    };
    if clipped || lines_clipped {
        lines.push(Line::styled(
            clip_cells(CLIPPED_LABEL, width),
            Style::default().fg(colors::subtle()),
        ));
    }
    Some(lines)
}

fn ends_table(line: &str) -> bool {
    let trimmed = line.trim();
    trimmed.is_empty()
        || trimmed.starts_with('>')
        || ["```", "~~~", "- ", "* ", "+ "]
            .iter()
            .any(|&prefix| trimmed.starts_with(prefix))
        || heading(trimmed).is_some()
        || ordered_item(trimmed).is_some()
        || super::list_anchor(line).is_some_and(|anchor| anchor.task().is_some())
        || super::rule_markers(line).is_some_and(|markers| markers.forms_break())
        || trimmed.starts_with("---") && trimmed.bytes().all(|byte| byte == b'-')
}

fn split_row(line: &str, limit: usize) -> Vec<String> {
    let line = line.trim();
    let mut chars = line.strip_prefix('|').unwrap_or(line).chars().peekable();
    let mut cells = Vec::new();
    let mut cell = String::new();
    let mut escaped = false;
    while let Some(character) = chars.next() {
        match character {
            '|' if escaped => {
                cell.pop();
                cell.push('|');
            }
            '|' => {
                cells.push(std::mem::take(&mut cell).trim().to_string());
                if cells.len() == limit || chars.peek().is_none() {
                    return cells;
                }
            }
            _ => cell.push(character),
        }
        escaped = character == '\\' && !escaped;
    }
    cells.push(cell.trim().to_string());
    cells
}

fn column_alignment(cell: &str) -> Option<Alignment> {
    let left = cell.starts_with(':');
    let right = cell.ends_with(':');
    let dashes = cell.strip_prefix(':').unwrap_or(cell);
    let dashes = dashes.strip_suffix(':').unwrap_or(dashes);
    if dashes.is_empty() || !dashes.bytes().all(|byte| byte == b'-') {
        return None;
    }
    Some(match (left, right) {
        (true, true) => Alignment::Center,
        (false, true) => Alignment::Right,
        _ => Alignment::Left,
    })
}

fn styled_row(cells: Vec<String>, columns: usize, literal: bool) -> Row {
    let mut row = cells
        .into_iter()
        .map(|cell| {
            let text = cell.replace('\t', "  ");
            if literal {
                Line::raw(text)
            } else {
                Line::from(parse_inline(&text))
            }
        })
        .collect::<Row>();
    row.resize_with(columns, Line::default);
    row
}

fn terminal_cell_widths<'a>(line: &'a Line<'a>) -> impl Iterator<Item = usize> + 'a {
    line.styled_graphemes(Style::default())
        .map(|grapheme| usize::from(grapheme.symbol.cell_width()))
}

fn terminal_line_width(line: &Line<'_>) -> usize {
    terminal_cell_widths(line).sum()
}

fn column_widths(rows: &[Row], columns: usize, width: usize) -> Option<Vec<usize>> {
    let available = width.checked_sub(COLUMN_GAP.len() * columns.saturating_sub(1))?;
    let natural = (0..columns)
        .map(|column| {
            rows.iter()
                .map(|row| terminal_line_width(&row[column]))
                .max()
                .unwrap_or(1)
                .max(1)
        })
        .collect::<Vec<_>>();
    if natural.iter().sum::<usize>() <= available {
        return Some(natural);
    }

    // Reserve room for one complete grapheme in each column, then share
    // the remaining cells among columns with content left to wrap.
    let mut widths = (0..columns)
        .map(|column| {
            rows.iter()
                .flat_map(|row| terminal_cell_widths(&row[column]))
                .max()
                .unwrap_or(1)
                .max(1)
        })
        .collect::<Vec<_>>();
    let mut remaining = available.checked_sub(widths.iter().sum())?;
    while remaining > 0 {
        let before = remaining;
        for (column, &natural) in widths.iter_mut().zip(&natural) {
            if *column < natural {
                *column += 1;
                remaining -= 1;
            }
            if remaining == 0 {
                break;
            }
        }
        if remaining == before {
            break;
        }
    }
    Some(widths)
}

fn render_aligned(
    rows: &[Row],
    widths: &[usize],
    alignments: &[Alignment],
) -> (Vec<Line<'static>>, bool) {
    let mut lines = Vec::new();
    if render_row(&mut lines, &rows[0], widths, alignments, true)
        || lines.len() == MAX_CONTENT_LINES
    {
        // The rule after the header doesn't fit.
        return (lines, true);
    }
    let rule = widths
        .iter()
        .map(|&width| "─".repeat(width))
        .collect::<Vec<_>>()
        .join(COLUMN_GAP);
    lines.push(Line::styled(rule, Style::default().fg(colors::inactive())));
    for row in &rows[1..] {
        if render_row(&mut lines, row, widths, alignments, false) {
            return (lines, true);
        }
    }
    (lines, false)
}

fn render_row(
    lines: &mut Vec<Line<'static>>,
    row: &Row,
    widths: &[usize],
    alignments: &[Alignment],
    header: bool,
) -> bool {
    let remaining = MAX_CONTENT_LINES - lines.len();
    if remaining == 0 {
        return true;
    }
    let mut wrapped = row
        .iter()
        .zip(widths)
        .map(|(cell, &width)| wrap_cell(cell, width, remaining + 1).into_iter())
        .collect::<Vec<_>>();
    let height = wrapped
        .iter()
        .map(ExactSizeIterator::len)
        .max()
        .unwrap_or(0);
    for _ in 0..height.min(remaining) {
        let mut spans = Vec::new();
        for (column, ((cell, &width), &alignment)) in
            wrapped.iter_mut().zip(widths).zip(alignments).enumerate()
        {
            if column > 0 {
                spans.push(Span::raw(COLUMN_GAP));
            }
            let content = cell.next();
            let padding = width.saturating_sub(content.as_ref().map_or(0, terminal_line_width));
            let leading = match alignment {
                Alignment::Left => 0,
                Alignment::Center => padding / 2,
                Alignment::Right => padding,
            };
            spans.push(Span::raw(" ".repeat(leading)));
            if let Some(content) = content {
                spans.extend(content.spans);
            }
            spans.push(Span::raw(" ".repeat(padding - leading)));
        }
        let mut line = Line::from(spans);
        if header {
            line.style = line.style.add_modifier(Modifier::BOLD);
        }
        lines.push(line);
    }
    height > remaining
}

/// Keep at most the caller's available lines plus its overflow witness.
fn wrap_cell(cell: &Line<'static>, width: usize, limit: usize) -> Vec<Line<'static>> {
    if terminal_line_width(cell) <= width {
        return vec![cell.clone()];
    }

    let mut lines = Vec::new();
    let mut spans = Vec::new();
    let mut used = 0;
    for span in &cell.spans {
        let mut start = 0;
        for (offset, grapheme) in span.content.grapheme_indices(true) {
            // Paragraph's styled graphemes omit controls. Keep byte offsets
            // for preserving spans, but measure only what the widget draws.
            if grapheme.contains(char::is_control) {
                continue;
            }
            let size = usize::from(grapheme.cell_width());
            if used > 0 && used + size > width {
                if offset > start {
                    spans.push(Span::styled(
                        span.content[start..offset].to_string(),
                        span.style,
                    ));
                }
                lines.push(Line::from(std::mem::take(&mut spans)));
                if lines.len() == limit {
                    return lines;
                }
                start = offset;
                used = 0;
            }
            used += size;
        }
        if start < span.content.len() {
            spans.push(Span::styled(span.content[start..].to_string(), span.style));
        }
    }
    lines.push(Line::from(spans));
    lines
}

fn append_wrapped_cell(lines: &mut Vec<Line<'static>>, cell: &Line<'static>, width: usize) -> bool {
    let remaining = MAX_CONTENT_LINES - lines.len();
    if remaining == 0 {
        return true;
    }
    let wrapped = wrap_cell(cell, width, remaining + 1);
    let clipped = wrapped.len() > remaining;
    lines.extend(wrapped.into_iter().take(remaining));
    clipped
}

fn render_stacked(rows: &[Row], width: usize) -> (Vec<Line<'static>>, bool) {
    let (header, data) = rows.split_first().expect("a table has a header");
    let mut lines = Vec::new();
    if data.is_empty() {
        for cell in header {
            if append_wrapped_cell(&mut lines, cell, width) {
                return (lines, true);
            }
        }
        return (lines, false);
    }
    for (index, row) in data.iter().enumerate() {
        if index > 0 {
            if lines.len() == MAX_CONTENT_LINES {
                return (lines, true);
            }
            lines.push(Line::default());
        }
        for (label, cell) in header.iter().zip(row) {
            if lines.len() == MAX_CONTENT_LINES {
                return (lines, true);
            }
            let mut spans = label
                .spans
                .iter()
                .map(|span| {
                    Span::styled(
                        span.content.clone(),
                        span.style.add_modifier(Modifier::BOLD),
                    )
                })
                .collect::<Vec<_>>();
            spans.push(Span::raw(": "));
            spans.extend(cell.spans.iter().cloned());
            if append_wrapped_cell(&mut lines, &Line::from(spans), width) {
                return (lines, true);
            }
        }
    }
    (lines, false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::components::markdown::{render_markdown, render_streaming_markdown};

    const RELEASE_FIELDS: [&str; 13] = [
        "Job",
        "Build",
        "Tests",
        "Docs",
        "Review",
        "Linux",
        "macOS",
        "Windows",
        "ARM",
        "Owner",
        "ETA",
        "Status",
        "Follow-up",
    ];

    fn text_of(line: &Line<'_>) -> String {
        line.spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect()
    }

    fn grid_head(fields: &[&str], separators: &[&str]) -> String {
        format!(
            "| {} |\n| {} |\n",
            fields.join(" | "),
            separators.join(" | ")
        )
    }

    #[test]
    fn preview_limits_columns_without_reinterpreting_extra_body_cells() {
        let within = grid_head(&RELEASE_FIELDS[..MAX_COLUMNS], &["---"; MAX_COLUMNS]);
        let wider = grid_head(&RELEASE_FIELDS, &["---"; MAX_COLUMNS + 1]);
        let row = concat!(
            "| Ship | go | pass | yes | OK | x64 | arm64 | x64 | yes | Mei | Fri | Ready",
            " | hide from preview |\n",
        );
        for width in [60, 80, 120] {
            let expected = render_markdown(&format!("{within}{row}"), width);
            assert!(!expected.iter().any(|line| text_of(line) == CLIPPED_LABEL));
            assert!(expected
                .iter()
                .all(|line| line.width() <= usize::from(width)));

            let clipped = render_markdown(&format!("{wider}{row}"), width);
            assert_eq!(&clipped[..expected.len()], expected.as_slice());
            assert_eq!(clipped.len(), expected.len() + 1);
            assert_eq!(text_of(clipped.last().unwrap()), CLIPPED_LABEL);
        }
    }

    #[test]
    fn preview_checks_delimiter_cells_and_counts_after_the_column_limit() {
        let late_invalid = grid_head(
            &RELEASE_FIELDS,
            &[&["---"; MAX_COLUMNS][..], &["pending"]].concat(),
        );
        let late_mismatch = grid_head(&RELEASE_FIELDS, &["---"; MAX_COLUMNS + 2]);
        for malformed in [late_invalid, late_mismatch] {
            let source =
                format!("{malformed}\n| Outcome | Owner |\n| --- | --- |\n| Ready | Mei |");
            let rows = render_markdown(&source, 80);
            assert_eq!(text_of(&rows[0]), malformed.lines().next().unwrap());
            assert!(!rows.iter().any(|line| text_of(line) == CLIPPED_LABEL));
            assert_eq!(
                text_of(rows.last().unwrap())
                    .split_whitespace()
                    .collect::<Vec<_>>(),
                ["Ready", "Mei"]
            );
        }
    }

    #[test]
    fn preview_limits_body_rows_and_keeps_the_following_streamed_block() {
        let mut table = String::from("Job | Status\n--- | ---\n");
        for index in 1..=MAX_BODY_ROWS {
            table.push_str(&format!("Check {index:02} | Ready\n"));
        }
        let exact = render_streaming_markdown(&table, 60);
        assert_eq!(exact.len(), MAX_BODY_ROWS + 2);
        assert!(text_of(exact.last().unwrap()).contains("Check 32"));

        let clipped = render_streaming_markdown(
            &format!(
                "{table}Overflow check | Ready\nSecond extra check | Ready\n##\tFollow-up ##\r\n"
            ),
            60,
        );
        assert_eq!(&clipped[..exact.len()], exact.as_slice());
        assert_eq!(clipped.len(), exact.len() + 2);
        assert_eq!(text_of(&clipped[exact.len()]), CLIPPED_LABEL);
        assert_eq!(text_of(clipped.last().unwrap()), "Follow-up");
    }

    #[test]
    fn aligned_preview_counts_header_rule_and_wrapped_lines() {
        let header = "Job | Notes\n--- | ---\n";
        let note = "Raised the threshold during the review. Verify the report once more.";
        let leading = format!("Run | {note}\n").repeat(30);
        let exact = render_markdown(&format!("{header}{leading}Run | {note}"), 60);
        assert_eq!(exact.len(), MAX_CONTENT_LINES);
        assert!(!exact.iter().any(|line| text_of(line) == CLIPPED_LABEL));

        let clipped = render_markdown(
            &format!("{header}{leading}Run | {note} {note}\n\n## Next section"),
            60,
        );
        assert_eq!(
            &clipped[..MAX_CONTENT_LINES - 2],
            &exact[..MAX_CONTENT_LINES - 2]
        );
        assert_eq!(clipped.len(), MAX_CONTENT_LINES + 3);
        assert_eq!(text_of(&clipped[MAX_CONTENT_LINES]), CLIPPED_LABEL);
        assert_eq!(text_of(clipped.last().unwrap()), "Next section");
    }

    #[test]
    fn multilingual_table_preview_limits_actual_transcript_rows() {
        use ratatui::backend::TestBackend;
        use ratatui::widgets::{Paragraph, Wrap};
        use ratatui::Terminal;

        const WIDTH: u16 = 60;
        let note = "لا توجد عوائق للإطلاق، ولا توجد أي إجراءات متابعة اليوم.";
        let mut source = String::from("Job | Notes\n--- | ---\n");
        for index in 1..=MAX_BODY_ROWS {
            source.push_str(&format!("{index:02} | {note}\n"));
        }
        source.push_str("## Follow-up");
        let lines = render_markdown(&source, WIDTH);
        let paragraph = Paragraph::new(lines).wrap(Wrap { trim: false });
        // The marker and following heading each occupy one additional line.
        assert_eq!(paragraph.line_count(WIDTH), MAX_CONTENT_LINES + 2);
        let mut terminal =
            Terminal::new(TestBackend::new(WIDTH, (MAX_CONTENT_LINES + 2) as u16)).unwrap();
        terminal
            .draw(|frame| frame.render_widget(paragraph, frame.area()))
            .unwrap();
        let buffer = terminal.backend().buffer();
        let row = |index: usize| {
            buffer.content()[index * usize::from(WIDTH)..(index + 1) * usize::from(WIDTH)]
                .iter()
                .map(|cell| cell.symbol())
                .collect::<String>()
        };
        assert_eq!(row(MAX_CONTENT_LINES).trim_end(), CLIPPED_LABEL);
        assert_eq!(row(MAX_CONTENT_LINES + 1).trim_end(), "Follow-up");
    }

    #[test]
    fn stacked_preview_counts_wrapping_and_row_separators() {
        let header = "A | B | C\n--- | --- | ---\n";
        let leading = "one | two | three\n".repeat(15);
        let exact = render_markdown(&format!("{header}{leading}one | two | Six items"), 8);
        assert_eq!(exact.len(), MAX_CONTENT_LINES);

        let clipped = render_markdown(
            &format!("{header}{leading}one | two | Six items and seven notes"),
            8,
        );
        assert_eq!(
            &clipped[..MAX_CONTENT_LINES - 2],
            &exact[..MAX_CONTENT_LINES - 2]
        );
        assert_eq!(clipped.len(), MAX_CONTENT_LINES + 1);
        assert_eq!(
            text_of(clipped.last().unwrap()),
            clip_cells(CLIPPED_LABEL, 8)
        );
        assert!(clipped.iter().all(|row| row.width() <= 8));
    }
}
