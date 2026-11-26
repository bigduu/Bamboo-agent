use std::iter::Peekable;

use ratatui::layout::Alignment;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use unicode_segmentation::UnicodeSegmentation;

use crate::text::display_width;
use crate::theme::colors;

use super::{fence::SourceLine, heading, ordered_item, parse_inline};

const COLUMN_GAP: &str = "   ";
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
    let alignments = split_row(delimiter)
        .iter()
        .map(|cell| column_alignment(cell))
        .collect::<Option<Vec<_>>>()?;
    let header = split_row(header);
    let columns = alignments.len();
    if header.len() != columns {
        return None;
    }
    input.next();

    let mut rows = vec![styled_row(header, columns, false)];
    while let Some(line) = input.next_if(|line| !ends_table(line.text) && !opens_heading(*line)) {
        // A marker-only row still inside the table is an unfinished rule.
        let literal = super::rule_markers(line.text).is_some();
        rows.push(styled_row(split_row(line.text), columns, literal));
    }

    let Some(widths) = column_widths(&rows, columns, width) else {
        return Some(render_stacked(&rows, width));
    };
    let mut lines = Vec::new();
    render_row(&mut lines, &rows[0], &widths, &alignments, true);
    let rule = widths
        .iter()
        .map(|&width| "─".repeat(width))
        .collect::<Vec<_>>()
        .join(COLUMN_GAP);
    lines.push(Line::styled(rule, Style::default().fg(colors::inactive())));
    for row in &rows[1..] {
        render_row(&mut lines, row, &widths, &alignments, false);
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

fn split_row(line: &str) -> Vec<String> {
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
                if chars.peek().is_none() {
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
        .take(columns)
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

fn column_widths(rows: &[Row], columns: usize, width: usize) -> Option<Vec<usize>> {
    let available = width.checked_sub(COLUMN_GAP.len() * columns.saturating_sub(1))?;
    let natural = (0..columns)
        .map(|column| {
            rows.iter()
                .map(|row| row[column].width())
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
                .flat_map(|row| &row[column].spans)
                .flat_map(|span| span.content.graphemes(true))
                .map(display_width)
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

fn render_row(
    lines: &mut Vec<Line<'static>>,
    row: &Row,
    widths: &[usize],
    alignments: &[Alignment],
    header: bool,
) {
    let mut wrapped = row
        .iter()
        .zip(widths)
        .map(|(cell, &width)| wrap_cell(cell, width).into_iter())
        .collect::<Vec<_>>();
    let height = wrapped
        .iter()
        .map(ExactSizeIterator::len)
        .max()
        .unwrap_or(0);
    for _ in 0..height {
        let mut spans = Vec::new();
        for (column, ((cell, &width), &alignment)) in
            wrapped.iter_mut().zip(widths).zip(alignments).enumerate()
        {
            if column > 0 {
                spans.push(Span::raw(COLUMN_GAP));
            }
            let content = cell.next();
            let padding = width.saturating_sub(content.as_ref().map_or(0, Line::width));
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
}

fn wrap_cell(cell: &Line<'static>, width: usize) -> Vec<Line<'static>> {
    if cell.width() <= width {
        return vec![cell.clone()];
    }

    let mut lines = Vec::new();
    let mut spans = Vec::new();
    let mut used = 0;
    for span in &cell.spans {
        let mut start = 0;
        for (offset, grapheme) in span.content.grapheme_indices(true) {
            let size = display_width(grapheme);
            if used > 0 && used + size > width {
                if offset > start {
                    spans.push(Span::styled(
                        span.content[start..offset].to_string(),
                        span.style,
                    ));
                }
                lines.push(Line::from(std::mem::take(&mut spans)));
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

fn render_stacked(rows: &[Row], width: usize) -> Vec<Line<'static>> {
    let (header, data) = rows.split_first().expect("a table has a header");
    if data.is_empty() {
        return header
            .iter()
            .flat_map(|cell| wrap_cell(cell, width))
            .collect();
    }
    let mut lines = Vec::new();
    for (index, row) in data.iter().enumerate() {
        if index > 0 {
            lines.push(Line::default());
        }
        for (label, cell) in header.iter().zip(row) {
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
            lines.extend(wrap_cell(&Line::from(spans), width));
        }
    }
    lines
}
