use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};

use crate::text::{clip_cells, display_width, truncate_cells};
use crate::theme::colors;

mod fence;
mod table;

use fence::FencedBlock;

const MAX_PREVIEW_BYTES: usize = 128 * 1024;
const MAX_PREVIEW_SOURCE_LINES: usize = 2_048;
const MAX_FENCE_PREVIEW_ROWS: usize = 64;
const MAX_FENCE_LABEL_CELLS: usize = 24;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CopyableFence<'a> {
    pub language: Option<&'a str>,
    pub source: &'a str,
}

/// Return the bounded preview source and the exact bodies of its closed fences.
pub(crate) fn copyable_fences(text: &str) -> (&str, Vec<CopyableFence<'_>>) {
    let source = preview_source(text);
    let mut input = fence::source_lines(source);
    let mut fences = Vec::new();
    while let Some(line) = input.next() {
        if let Some(block) =
            fence::take_fence(source, line, &mut input).filter(|block| block.closed)
        {
            fences.push(CopyableFence {
                language: block.kind.language(),
                source: block.source,
            });
        }
    }
    (source, fences)
}

/// Render markdown text into a list of ratatui Lines with styling.
/// Handles: backtick and tilde fences, inline `code`, **bold**, _italic_,
/// [links](url), `#`..`######` headings, nested bullet and ordered lists,
/// task-list checkboxes, `>` blockquotes, and pipe tables.
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

        if let Some(item) = render_list_item(raw_line, &indent) {
            lines.push(item);
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

struct ListAnchor<'a> {
    indent: &'a str,
    marker: &'a str,
    rest: &'a str,
}

impl<'a> ListAnchor<'a> {
    fn task(&self) -> Option<(char, &'a str)> {
        let content = self.rest.trim_start_matches([' ', '\t']);
        let task = task_item(content)?;
        let padding = &self.rest[..self.rest.len() - content.len()];
        // Apply the 1–4 column marker padding within this line. Container
        // nesting is handled by the existing line-oriented renderer.
        let marker_end = column_after(0, self.indent) + self.marker.len();
        (column_after(marker_end, padding) - marker_end <= 4).then_some(task)
    }
}

fn column_after(start: usize, text: &str) -> usize {
    text.chars().fold(start, |column, character| {
        if character == '\t' {
            column + (4 - column % 4)
        } else {
            column + 1
        }
    })
}

fn list_anchor(line: &str) -> Option<ListAnchor<'_>> {
    let body = line.trim_start();
    let marker_len = if body.starts_with(['-', '*', '+']) {
        1
    } else {
        let digits = body.bytes().take_while(u8::is_ascii_digit).count();
        if digits == 0 || !matches!(body.as_bytes().get(digits), Some(b'.' | b')')) {
            return None;
        }
        digits + 1
    };
    let (marker, rest) = body.split_at(marker_len);
    rest.starts_with([' ', '\t']).then_some(ListAnchor {
        indent: &line[..line.len() - body.len()],
        marker,
        rest,
    })
}

/// The ordinary ordered-list syntax already supported by the renderer.
fn ordered_item(line: &str) -> Option<(&str, &str)> {
    let anchor = list_anchor(line)?;
    Some((
        anchor.marker.strip_suffix('.')?,
        anchor.rest.strip_prefix(' ')?,
    ))
}

fn render_list_item(line: &str, indent: &str) -> Option<Line<'static>> {
    let anchor = list_anchor(line)?;
    let (prefix, content) = match (anchor.marker, anchor.task()) {
        (marker, Some((box_mark, text))) if marker.len() > 1 => {
            (format!("{indent}  {marker} {box_mark} "), text)
        }
        (_, Some((box_mark, text))) => (format!("{indent}  {box_mark} "), text),
        (marker, None) if marker.ends_with('.') => (
            format!("{indent}  {marker} "),
            anchor.rest.strip_prefix(' ')?,
        ),
        ("-" | "*", None) => (format!("{indent}  • "), anchor.rest.strip_prefix(' ')?),
        _ => return None,
    };
    let mut spans = vec![Span::styled(prefix, Style::default().fg(colors::subtle()))];
    spans.extend(parse_inline(content));
    Some(Line::from(spans))
}

fn task_item(content: &str) -> Option<(char, &str)> {
    let box_mark = match content.as_bytes() {
        [b'[', b' ', b']', ..] => '☐',
        [b'[', b'x' | b'X', b']', ..] => '☑',
        _ => return None,
    };
    let rest = &content[3..];
    (rest.is_empty() || rest.starts_with(char::is_whitespace))
        .then_some((box_mark, rest.trim_start()))
}

fn render_code_block(lines: &mut Vec<Line>, block: FencedBlock<'_>, width: usize) {
    let framed = width >= 4;
    let inner_width = if framed { width - 4 } else { width };
    let border_style = Style::default().fg(colors::code_border());
    if framed {
        let language = block.kind.language().unwrap_or_default();
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
    fn task_lists_preserve_nesting_numbering_and_inline_styles() {
        let rows = render_markdown(
            "- [ ] **准备** 🧭\n  * [x] `done`\n\t7. [X] _完成_\n+ [ ]\n1. ordinary\n+ ordinary",
            80,
        );
        assert_eq!(
            rows.iter().map(text_of).collect::<Vec<_>>(),
            [
                "  ☐ 准备 🧭",
                "    ☑ done",
                "    7. ☑ 完成",
                "  ☐ ",
                "  1. ordinary",
                "+ ordinary",
            ]
        );
        for (row, text, modifier) in [
            (&rows[0], "准备", Modifier::BOLD),
            (&rows[1], "done", Modifier::REVERSED),
            (&rows[2], "完成", Modifier::ITALIC),
        ] {
            assert!(row
                .spans
                .iter()
                .any(|span| span.content == text && span.style.add_modifier.contains(modifier)));
        }
    }

    #[test]
    fn task_markers_require_an_exact_checkbox_and_a_separator() {
        assert_eq!(
            render(concat!(
                "- [\n- [x\n- [] empty\n- [o] wrong\n- [x]suffix\n",
                "- [x](https://example.test)\n- [X]\tTabbed\n[x] Plain prose",
            )),
            [
                "  • [",
                "  • [x",
                "  • [] empty",
                "  • [o] wrong",
                "  • [x]suffix",
                "  • x (https://example.test)",
                "  ☑ Tabbed",
                "[x] Plain prose",
            ]
        );
    }

    #[test]
    fn checklist_anchors_accept_spaces_tabs_and_both_ordered_delimiters() {
        assert_eq!(
            render(concat!(
                "-   [ ] spacing\n*\t[x] tab\n+\t[X] plus\n09.\t[x] period\n",
                "7)  [X] parenthesis\n   7)\t[X] nested\n-\t [x] four columns",
            )),
            [
                "  ☐ spacing",
                "  ☑ tab",
                "  ☑ plus",
                "  09. ☑ period",
                "  7) ☑ parenthesis",
                "     7) ☑ nested",
                "  ☑ four columns",
            ]
        );
    }

    #[test]
    fn checklist_anchors_keep_missing_or_excessive_padding_literal() {
        assert_eq!(
            render(concat!(
                "-[x] no separator\n7)[X] no separator\n1.[ ] no separator\n",
                "-     [x] five spaces\n-\t  [x] five columns\n7)\t[x\n",
                "3.  ordinary\n7) ordinary\n*\tordinary",
            )),
            [
                "-[x] no separator",
                "7)[X] no separator",
                "1.[ ] no separator",
                "  •     [x] five spaces",
                "-\t  [x] five columns",
                "7)\t[x",
                "  3.  ordinary",
                "7) ordinary",
                "\tordinary",
            ]
        );
    }

    #[test]
    fn checklist_anchors_end_a_preceding_table() {
        for (item, expected) in [
            ("-\t[X] tab", "  ☑ tab"),
            ("7)  [ ] ordered", "  7) ☐ ordered"),
            ("   +\t[x] nested", "     ☑ nested"),
        ] {
            let rows = render(&format!("| Item |\n| --- |\n| table |\n{item}"));
            assert_eq!(rows.len(), 4);
            assert_eq!(rows[2].trim_end(), "table");
            assert_eq!(rows[3], expected);
        }
    }

    #[test]
    fn task_marker_text_stays_literal_in_code_and_table_cells() {
        let rows = render("~~~md\n- [x] code\n~~~\n| Item |\n| --- |\n| [ ] cell |");
        assert!(rows[1].contains("- [x] code"));
        assert_eq!(rows[5].trim_end(), "[ ] cell");
        assert!(rows.iter().all(|row| !row.contains(['☐', '☑'])));
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
    fn copyable_fences_keep_original_bodies_in_source_order() {
        let source = concat!(
            "Leading prose.\r\n",
            "````rust options\r\n",
            "\tfirst 界👩‍💻\r\n",
            "```\r\n",
            "~~~~\r\n",
            "```` trailing\r\n",
            "\r\n",
            "````` \t\r\n",
            "Between fences.\n",
            "~~~ Mermaid\n",
            "\tA --> B\n",
            "~~~\n",
            "```\n",
            "still open",
        );
        let (prefix, fences) = copyable_fences(source);
        assert_eq!(prefix, source);
        assert_eq!(
            fences,
            [
                CopyableFence {
                    language: Some("rust"),
                    source: "\tfirst 界👩‍💻\r\n```\r\n~~~~\r\n```` trailing\r\n\r\n",
                },
                CopyableFence {
                    language: Some("Mermaid"),
                    source: "\tA --> B\n",
                },
            ]
        );

        let (_, at_eof) = copyable_fences("```\nunlabelled\n```");
        assert_eq!(
            at_eof,
            [CopyableFence {
                language: None,
                source: "unlabelled\n",
            }]
        );
    }

    #[test]
    fn copyable_fences_keep_rows_beyond_the_rendered_preview() {
        let body = "\tfirst\r\n".repeat(MAX_FENCE_PREVIEW_ROWS) + "\tlast 👩‍💻\r\n";
        let source = format!("~~~sh\r\n{body}~~~\r\n");
        let (_, fences) = copyable_fences(&source);
        assert_eq!(fences.len(), 1);
        assert_eq!(fences[0].source, body);
        let rows = render(&source);
        assert!(rows.iter().any(|row| row.contains("… more code")));
        assert!(rows.iter().all(|row| !row.contains("last")));
    }

    #[test]
    fn copyable_fences_do_not_treat_a_truncated_physical_line_as_a_closer() {
        let early = "```rust\r\n\tfirst\r\n```\r\n~~~sh\n";
        let fill = "x".repeat(MAX_PREVIEW_BYTES - early.len() - 4);
        let source = format!("{early}{fill}\n~~~ still code\n~~~\n");
        assert_eq!(&source[MAX_PREVIEW_BYTES - 3..MAX_PREVIEW_BYTES], "~~~");
        let (prefix, fences) = copyable_fences(&source);
        assert_eq!(prefix.len(), MAX_PREVIEW_BYTES - 3);
        assert_eq!(prefix, preview_source(&source));
        assert_eq!(
            fences,
            [CopyableFence {
                language: Some("rust"),
                source: "\tfirst\r\n",
            }]
        );
    }

    #[test]
    fn copyable_fences_require_closure_within_the_source_line_budget() {
        let early = "```rust\nfirst\n```\n~~~sh\n";
        let source = format!("{early}{}~~~\n", "more\n".repeat(MAX_PREVIEW_SOURCE_LINES));
        let (prefix, fences) = copyable_fences(&source);
        assert_eq!(prefix.lines().count(), MAX_PREVIEW_SOURCE_LINES);
        assert_eq!(prefix, preview_source(&source));
        assert_eq!(
            fences,
            [CopyableFence {
                language: Some("rust"),
                source: "first\n",
            }]
        );
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
