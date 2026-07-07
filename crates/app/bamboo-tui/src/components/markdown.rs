use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};

use crate::theme::colors;

/// Render markdown text into a list of ratatui Lines with styling.
/// Handles: fenced code blocks (```), inline `code`, **bold**, _italic_,
/// [links](url), `#`..`######` headings, `-`/`*` and ordered (`1.`) lists with
/// nesting, and `>` blockquotes.
pub fn render_markdown(text: &str, width: u16) -> Vec<Line<'static>> {
    let width = width as usize;
    let mut lines: Vec<Line> = Vec::new();
    let mut in_code_block = false;
    let mut code_lines: Vec<String> = Vec::new();

    for raw_line in text.lines() {
        if raw_line.trim_start().starts_with("```") {
            if in_code_block {
                render_code_block(&mut lines, &code_lines, width);
                code_lines.clear();
                in_code_block = false;
            } else {
                in_code_block = true;
            }
            continue;
        }

        if in_code_block {
            code_lines.push(raw_line.to_string());
            continue;
        }

        let indent_len = raw_line.len() - raw_line.trim_start().len();
        let indent: String = raw_line[..indent_len].replace('\t', "  ");
        let body = &raw_line[indent_len..];

        // Headings: 1–6 leading '#'.
        if let Some((level, content)) = heading(body) {
            let style = if level <= 1 {
                Style::default()
                    .fg(colors::BRAND)
                    .add_modifier(Modifier::BOLD)
            } else if level == 2 {
                Style::default().add_modifier(Modifier::BOLD)
            } else {
                Style::default()
                    .fg(colors::INACTIVE)
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
                Style::default().fg(colors::THINKING),
            )];
            spans.extend(parse_inline(content));
            lines.push(Line::from(spans));
            continue;
        }

        // Ordered list: "N. text".
        if let Some((num, content)) = ordered_item(body) {
            let mut spans = vec![Span::styled(
                format!("{indent}  {num}. "),
                Style::default().fg(colors::SUBTLE),
            )];
            spans.extend(parse_inline(content));
            lines.push(Line::from(spans));
            continue;
        }

        // Unordered list: "- text" / "* text".
        if let Some(content) = body.strip_prefix("- ").or_else(|| body.strip_prefix("* ")) {
            let mut spans = vec![Span::styled(
                format!("{indent}  • "),
                Style::default().fg(colors::SUBTLE),
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

    if in_code_block && !code_lines.is_empty() {
        render_code_block(&mut lines, &code_lines, width);
    }

    lines
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

fn render_code_block(lines: &mut Vec<Line>, code_lines: &[String], width: usize) {
    let inner_width = width.saturating_sub(4);
    let top_border = format!("┌{}┐", "─".repeat(inner_width));
    let bottom_border = format!("└{}┘", "─".repeat(inner_width));

    lines.push(Line::from(Span::styled(
        top_border,
        Style::default().fg(colors::CODE_BORDER),
    )));

    for code_line in code_lines {
        let padded = format!("│ {:<width$} │", code_line, width = inner_width);
        lines.push(Line::from(Span::styled(
            padded,
            Style::default().fg(colors::INACTIVE),
        )));
    }

    lines.push(Line::from(Span::styled(
        bottom_border,
        Style::default().fg(colors::CODE_BORDER),
    )));
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
                    .fg(colors::INACTIVE)
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
                        .fg(colors::BRAND)
                        .add_modifier(Modifier::UNDERLINED),
                ));
                if !url.is_empty() {
                    spans.push(Span::styled(
                        format!(" ({url})"),
                        Style::default().fg(colors::SUBTLE),
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
}
