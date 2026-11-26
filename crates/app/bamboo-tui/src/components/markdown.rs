use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};

use crate::text::{clip_cells, display_width, truncate_cells};
use crate::theme::colors;

mod fence;
mod table;

use fence::{FenceKind, FencedBlock};

const MAX_PREVIEW_BYTES: usize = 128 * 1024;
const MAX_PREVIEW_SOURCE_LINES: usize = 2_048;
const MAX_FENCE_PREVIEW_ROWS: usize = 64;
const MAX_FENCE_LABEL_CELLS: usize = 24;

/// Render markdown text into a list of ratatui Lines with styling.
/// Handles: backtick and tilde fences, inline `code`, **bold**, _italic_,
/// [links](url), `#`..`######` headings, `-`/`*` and ordered (`1.`) lists with
/// nesting, `>` blockquotes, and pipe tables.
pub fn render_markdown(text: &str, width: u16) -> Vec<Line<'static>> {
    let width = width as usize;
    if width == 0 {
        return Vec::new();
    }
    let source = preview_source(text);
    let mut lines: Vec<Line> = Vec::new();

    let mut input = fence::source_lines(source).peekable();
    while let Some(line) = input.next() {
        if let Some(block) = fence::take_fence(source, line, &mut input) {
            render_code_block(&mut lines, block, width);
            continue;
        }
        let raw_line = line.text;

        if let Some(rows) = table::render_if_table(raw_line, &mut input, width) {
            lines.extend(rows);
            continue;
        }

        let indent_len = raw_line.len() - raw_line.trim_start().len();
        let indent: String = raw_line[..indent_len].replace('\t', "  ");
        let body = &raw_line[indent_len..];

        // Headings: 1–6 leading '#'.
        if let Some((level, content)) = heading(body) {
            let style = if level <= 1 {
                Style::default()
                    .fg(colors::brand())
                    .add_modifier(Modifier::BOLD)
            } else if level == 2 {
                Style::default().add_modifier(Modifier::BOLD)
            } else {
                Style::default()
                    .fg(colors::inactive())
                    .add_modifier(Modifier::BOLD)
            };
            let mut spans = vec![Span::styled(content.to_string(), style)];
            // Keep any inline emphasis inside a heading readable too.
            if content.contains('`') || content.contains('*') {
                spans = parse_inline(content);
            }
            lines.push(Line::from(spans));
            continue;
        }

        // Blockquote.
        if let Some(content) = body
            .strip_prefix("> ")
            .or_else(|| body.eq("> ").then_some(""))
        {
            let mut spans = vec![Span::styled(
                format!("{indent}│ "),
                Style::default().fg(colors::thinking()),
            )];
            spans.extend(parse_inline(content));
            lines.push(Line::from(spans));
            continue;
        }

        // Ordered list: "N. text".
        if let Some((num, content)) = ordered_item(body) {
            let mut spans = vec![Span::styled(
                format!("{indent}  {num}. "),
                Style::default().fg(colors::subtle()),
            )];
            spans.extend(parse_inline(content));
            lines.push(Line::from(spans));
            continue;
        }

        // Unordered list: "- text" / "* text".
        if let Some(content) = body.strip_prefix("- ").or_else(|| body.strip_prefix("* ")) {
            let mut spans = vec![Span::styled(
                format!("{indent}  • "),
                Style::default().fg(colors::subtle()),
            )];
            spans.extend(parse_inline(content));
            lines.push(Line::from(spans));
            continue;
        }

        if raw_line.is_empty() {
            lines.push(Line::raw(""));
        } else {
            let mut spans = Vec::new();
            if !indent.is_empty() {
                spans.push(Span::raw(indent));
            }
            spans.extend(parse_inline(body));
            lines.push(Line::from(spans));
        }
    }

    if source.len() < text.len() {
        lines.push(Line::styled(
            clip_cells("… Markdown preview clipped", width),
            Style::default().fg(colors::subtle()),
        ));
    }

    lines
}

fn preview_source(text: &str) -> &str {
    let bytes = text.as_bytes();
    // End on a line boundary when cutting a byte-limited preview. A partial
    // line at the budget boundary could otherwise look like a closing fence.
    let byte_end = if bytes.len() <= MAX_PREVIEW_BYTES {
        bytes.len()
    } else {
        bytes[..MAX_PREVIEW_BYTES]
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |position| position + 1)
    };
    let line_end = bytes[..byte_end]
        .iter()
        .enumerate()
        .filter(|(_, byte)| **byte == b'\n')
        .nth(MAX_PREVIEW_SOURCE_LINES - 1)
        .map_or(byte_end, |(position, _)| position + 1);
    &text[..line_end]
}

/// If `body` is an ATX heading, return `(level, content)`.
fn heading(body: &str) -> Option<(usize, &str)> {
    let hashes = body.chars().take_while(|&c| c == '#').count();
    if (1..=6).contains(&hashes) {
        let rest = &body[hashes..];
        if let Some(content) = rest.strip_prefix(' ') {
            return Some((hashes, content));
        }
    }
    None
}

/// If `body` starts with an ordered-list marker (`N. `), return `(number, rest)`.
fn ordered_item(body: &str) -> Option<(&str, &str)> {
    let digits = body.chars().take_while(|c| c.is_ascii_digit()).count();
    if digits == 0 {
        return None;
    }
    let rest = &body[digits..];
    if let Some(content) = rest.strip_prefix(". ") {
        return Some((&body[..digits], content));
    }
    None
}

fn render_code_block(lines: &mut Vec<Line>, block: FencedBlock<'_>, width: usize) {
    let framed = width >= 4;
    let inner_width = if framed { width - 4 } else { width };
    let border_style = Style::default().fg(colors::code_border());
    if framed {
        let language = match block.kind {
            FenceKind::Code { language } => language.unwrap_or_default(),
            FenceKind::Mermaid { language } => language,
        };
        let label = clip_cells(language, inner_width.min(MAX_FENCE_LABEL_CELLS));
        let label = if label.is_empty() {
            label
        } else {
            format!(" {label} ")
        };
        let rule = "─".repeat(width - 2 - display_width(&label));
        lines.push(Line::styled(format!("┌{label}{rule}┐"), border_style));
    }

    let row = |code: &str, style| {
        let code = code.replace('\t', "    ");
        let cells = if framed {
            format!("│ {} │", truncate_cells(&code, inner_width))
        } else {
            clip_cells(&code, inner_width)
        };
        Line::styled(cells, style)
    };
    let mut code = block.source.lines();
    let code_style = Style::default().fg(colors::inactive());
    lines.extend(
        code.by_ref()
            .take(MAX_FENCE_PREVIEW_ROWS)
            .map(|line| row(line, code_style)),
    );
    if code.next().is_some() {
        lines.push(row("… more code", Style::default().fg(colors::subtle())));
    }

    if framed {
        let rule = "─".repeat(width - 2);
        lines.push(Line::styled(format!("└{rule}┘"), border_style));
    }
}

/// Parse inline formatting: `code`, **bold**, _italic_ / *italic*,
/// [links](url), and plain text into Spans.
fn parse_inline(text: &str) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    let mut chars = text.chars().peekable();
    let mut current = String::new();

    macro_rules! flush {
        () => {
            if !current.is_empty() {
                spans.push(Span::raw(std::mem::take(&mut current)));
            }
        };
    }

    while let Some(ch) = chars.next() {
        // Inline code.
        if ch == '`' {
            flush!();
            let code = take_until(&mut chars, |c| c == '`');
            spans.push(Span::styled(
                code,
                Style::default()
                    .fg(colors::inactive())
                    .add_modifier(Modifier::REVERSED),
            ));
            continue;
        }

        // Link: [text](url) — show the text (underlined) then the url dimmed.
        if ch == '[' {
            if let Some((label, url)) = parse_link(&mut chars) {
                flush!();
                spans.push(Span::styled(
                    label,
                    Style::default()
                        .fg(colors::brand())
                        .add_modifier(Modifier::UNDERLINED),
                ));
                if !url.is_empty() {
                    spans.push(Span::styled(
                        format!(" ({url})"),
                        Style::default().fg(colors::subtle()),
                    ));
                }
                continue;
            }
            // Not a link — treat '[' as literal (the rest re-parses normally).
            current.push('[');
            continue;
        }

        // Bold (**).
        if ch == '*' && chars.peek() == Some(&'*') {
            chars.next();
            flush!();
            let bold = take_until_pair(&mut chars, '*');
            spans.push(Span::styled(
                bold,
                Style::default().add_modifier(Modifier::BOLD),
            ));
            continue;
        }

        // Italic (single * or _).
        if ch == '*' || ch == '_' {
            flush!();
            let italic = take_until(&mut chars, |c| c == ch);
            spans.push(Span::styled(
                italic,
                Style::default().add_modifier(Modifier::ITALIC),
            ));
            continue;
        }

        current.push(ch);
    }

    if !current.is_empty() {
        spans.push(Span::raw(current));
    }

    spans
}

/// Consume chars until `stop` returns true (consuming the terminator).
fn take_until(
    chars: &mut std::iter::Peekable<std::str::Chars>,
    stop: impl Fn(char) -> bool,
) -> String {
    let mut out = String::new();
    while let Some(&c) = chars.peek() {
        chars.next();
        if stop(c) {
            break;
        }
        out.push(c);
    }
    out
}

/// Consume until a doubled `marker` (`**`).
fn take_until_pair(chars: &mut std::iter::Peekable<std::str::Chars>, marker: char) -> String {
    let mut out = String::new();
    while let Some(c) = chars.next() {
        if c == marker && chars.peek() == Some(&marker) {
            chars.next();
            break;
        }
        out.push(c);
    }
    out
}

/// Parse the remainder of a link after the opening `[`: `text](url)`. Probes on
/// a clone and only advances `chars` on a full match, so a bare `[` is left
/// intact for literal rendering.
fn parse_link(chars: &mut std::iter::Peekable<std::str::Chars>) -> Option<(String, String)> {
    let mut probe = chars.clone();
    let mut label = String::new();
    let mut closed = false;
    for c in probe.by_ref() {
        if c == ']' {
            closed = true;
            break;
        }
        label.push(c);
    }
    if !closed || probe.peek() != Some(&'(') {
        return None;
    }
    probe.next(); // consume '('
    let mut url = String::new();
    let mut url_closed = false;
    for c in probe.by_ref() {
        if c == ')' {
            url_closed = true;
            break;
        }
        url.push(c);
    }
    if !url_closed {
        return None;
    }
    *chars = probe; // commit only on success
    Some((label, url))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text_of(line: &Line) -> String {
        line.spans.iter().map(|s| s.content.as_ref()).collect()
    }

    fn render(md: &str) -> Vec<String> {
        render_markdown(md, 80).iter().map(text_of).collect()
    }

    #[test]
    fn headings_all_levels() {
        let out = render("# H1\n## H2\n### H3\n###### H6");
        assert_eq!(out, vec!["H1", "H2", "H3", "H6"]);
        // 7 hashes is not a heading.
        assert_eq!(render("####### nope"), vec!["####### nope"]);
    }

    #[test]
    fn ordered_and_nested_lists() {
        let out = render("1. first\n2. second\n  - nested");
        assert_eq!(out[0], "  1. first");
        assert_eq!(out[1], "  2. second");
        assert!(out[2].contains("• nested"));
        assert!(out[2].starts_with("  "), "nested indent preserved");
    }

    #[test]
    fn links_render_label_and_url() {
        let out = render("see [docs](https://x.io) now");
        assert_eq!(out[0], "see docs (https://x.io) now");
        // A bare bracket is left literal.
        assert_eq!(render("[not a link"), vec!["[not a link"]);
    }

    #[test]
    fn blockquote_and_inline() {
        let out = render("> quoted **bold**");
        assert!(out[0].contains("│ "));
        assert!(out[0].contains("quoted"));
        assert!(out[0].contains("bold"));
    }

    #[test]
    fn italic_and_bold_distinct() {
        let out = render("_em_ and **strong** and `code`");
        assert_eq!(out[0], "em and strong and code");
    }

    #[test]
    fn table_aligns_columns_and_preserves_inline_styles() {
        let rows = render_markdown(
            "Check | Runs | Score\n:---- | :--: | ----:\n**Unit** | x | `2`\nFormatting | full | 100",
            80,
        );
        assert_eq!(text_of(&rows[0]), "Check        Runs   Score");
        assert_eq!(text_of(&rows[1]), "──────────   ────   ─────");
        assert_eq!(text_of(&rows[2]), "Unit          x         2");
        assert!(rows[0].style.add_modifier.contains(Modifier::BOLD));
        assert!(rows[2].spans.iter().any(|span| {
            span.content == "Unit" && span.style.add_modifier.contains(Modifier::BOLD)
        }));
        assert!(rows[2].spans.iter().any(|span| {
            span.content == "2" && span.style.add_modifier.contains(Modifier::REVERSED)
        }));
    }

    #[test]
    fn table_keeps_escaped_pipes_and_handles_short_and_long_rows() {
        let rows = render(
            "| Option | Value |\n| --- | --- |\n| `a\\|b` | enabled |\nsolo\n| more | yes | ignored |\n> outside",
        );
        assert!(rows[2].contains("a|b"));
        assert_eq!(rows[3].trim(), "solo");
        assert_eq!(
            rows[4].split_whitespace().collect::<Vec<_>>(),
            ["more", "yes"]
        );
        assert_eq!(rows[5], "│ outside");
    }

    #[test]
    fn table_appears_once_the_stream_has_a_matching_delimiter() {
        let header = "| Check | Result |";
        assert_eq!(render(header), [header]);
        assert_eq!(render("| Check | Result |\n| --- |"), [header, "| --- |"]);
        assert!(render("| Check | Result |\n| --- | --- |")[1].contains('─'));
        assert!(
            render("| Check | Result |\n| --- | --- |\n| Format | Pass")[2]
                .contains("Format   Pass")
        );
        let fenced = render("```md\n| Check | Result |\n| --- | --- |\n```");
        assert!(fenced[2].contains("| --- | --- |"));
    }

    #[test]
    fn table_wraps_wide_cells_with_styles_intact() {
        let rows = render_markdown(
            "| 項目 | Value |\n| --- | --- |\n| **四海八方** | abcdefgh |",
            14,
        );
        assert_eq!(rows.len(), 4);
        assert!(rows.iter().all(|row| row.width() <= 14));
        assert!(rows[2].spans.iter().any(|span| {
            span.content == "四海八" && span.style.add_modifier.contains(Modifier::BOLD)
        }));
        assert!(rows[3].spans.iter().any(|span| {
            span.content == "方" && span.style.add_modifier.contains(Modifier::BOLD)
        }));
        assert!(text_of(&rows[3]).contains("fgh"));
    }

    #[test]
    fn table_stacks_labeled_cells_when_the_columns_cannot_fit() {
        let rows = render_markdown("A | B | C\n--- | --- | ---\none | two | three", 8);
        assert_eq!(
            rows.iter().map(text_of).collect::<Vec<_>>(),
            ["A: one", "B: two", "C: three"]
        );
    }

    #[test]
    fn unicode_code_block_borders_share_one_cell_width() {
        let rendered = render_markdown("```\n界e\u{301}👨‍👩‍👧‍👦\n```", 18);
        let widths = rendered
            .iter()
            .map(|line| crate::text::display_width(&text_of(line)))
            .collect::<Vec<_>>();
        assert!(widths.iter().all(|width| *width == widths[0]), "{widths:?}");
        assert_eq!(widths[0], 18);
    }

    #[test]
    fn long_fence_emits_a_fixed_preview_and_reports_both_limits() {
        let source = format!(
            "```rust\n{}```\nAfter the preview.",
            "println!(\"still streaming\");\n".repeat(MAX_PREVIEW_SOURCE_LINES),
        );

        let rows = render(&source);

        assert_eq!(rows.len(), MAX_FENCE_PREVIEW_ROWS + 4);
        assert!(rows[0].contains(" rust "));
        assert!(rows[1..=MAX_FENCE_PREVIEW_ROWS]
            .iter()
            .all(|row| row.contains("still streaming")));
        assert!(rows[MAX_FENCE_PREVIEW_ROWS + 1].contains("… more code"));
        assert!(rows[MAX_FENCE_PREVIEW_ROWS + 2].starts_with('└'));
        assert_eq!(rows.last().unwrap(), "… Markdown preview clipped");
    }

    #[test]
    fn byte_budget_ends_before_a_partial_unicode_source_line() {
        let source = format!(
            "~~~rust\r\nretained\r\n{}\r\n~~~\r\nAfter the preview.",
            "界".repeat(MAX_PREVIEW_BYTES / 3),
        );

        assert_eq!(preview_source(&source), "~~~rust\r\nretained\r\n");
        let rows = render(&source);
        assert!(rows[1].contains("retained"));
        assert!(rows[2].starts_with('└'));
        assert_eq!(rows[3], "… Markdown preview clipped");
    }

    #[test]
    fn code_preview_fits_tiny_areas_and_clips_wide_graphemes() {
        let source = "```language\n界e\u{301}👨‍👩‍👧‍👦more text\n```";
        assert!(render_markdown(source, 0).is_empty());
        for width in 1..=12 {
            let rows = render_markdown(source, width);
            assert!(!rows.is_empty());
            assert!(rows.iter().all(|row| row.width() <= usize::from(width)));
        }
        let rows = render_markdown(source, 9);
        assert!(text_of(&rows[1]).contains("界e\u{301}…"));
        assert!(!text_of(&rows[1]).contains("👨"));
    }
}
