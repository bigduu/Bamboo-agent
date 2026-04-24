use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};

use crate::theme::colors;

/// Render markdown text into a list of ratatui Lines with styling.
/// Handles: code blocks (```), inline code (`), bold (**), headings (#), bullets (-).
pub fn render_markdown(text: &str, width: u16) -> Vec<Line<'static>> {
    let width = width as usize;
    let mut lines: Vec<Line> = Vec::new();
    let mut in_code_block = false;
    let mut code_lines: Vec<String> = Vec::new();

    for raw_line in text.lines() {
        if raw_line.starts_with("```") {
            if in_code_block {
                // End code block — render with border.
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

        // Heading
        if let Some(content) = raw_line.strip_prefix("# ") {
            lines.push(Line::from(Span::styled(
                content.to_string(),
                Style::default()
                    .fg(colors::BRAND)
                    .add_modifier(Modifier::BOLD),
            )));
            continue;
        }
        if let Some(content) = raw_line.strip_prefix("## ") {
            lines.push(Line::from(Span::styled(
                content.to_string(),
                Style::default().add_modifier(Modifier::BOLD),
            )));
            continue;
        }

        // Bullet
        let bullet_content = raw_line
            .strip_prefix("- ")
            .or_else(|| raw_line.strip_prefix("* "));
        if let Some(content) = bullet_content {
            let mut spans = vec![Span::raw("  • ")];
            spans.extend(parse_inline(content));
            lines.push(Line::from(spans));
            continue;
        }

        // Regular line with inline formatting
        if raw_line.is_empty() {
            lines.push(Line::raw(""));
        } else {
            lines.push(Line::from(parse_inline(raw_line)));
        }
    }

    // Handle unclosed code block.
    if in_code_block && !code_lines.is_empty() {
        render_code_block(&mut lines, &code_lines, width);
    }

    lines
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

/// Parse inline formatting: `code`, **bold**, and plain text into Spans.
fn parse_inline(text: &str) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    let mut chars = text.chars().peekable();
    let mut current = String::new();

    while let Some(ch) = chars.next() {
        // Inline code
        if ch == '`' {
            if !current.is_empty() {
                spans.push(Span::raw(std::mem::take(&mut current)));
            }
            let mut code = String::new();
            while let Some(&c) = chars.peek() {
                if c == '`' {
                    chars.next();
                    break;
                }
                code.push(chars.next().unwrap());
            }
            spans.push(Span::styled(
                code,
                Style::default()
                    .fg(colors::INACTIVE)
                    .add_modifier(Modifier::REVERSED),
            ));
            continue;
        }

        // Bold (**)
        if ch == '*' && chars.peek() == Some(&'*') {
            chars.next(); // consume second *
            if !current.is_empty() {
                spans.push(Span::raw(std::mem::take(&mut current)));
            }
            let mut bold = String::new();
            while let Some(c) = chars.next() {
                if c == '*' && chars.peek() == Some(&'*') {
                    chars.next(); // consume second *
                    break;
                }
                bold.push(c);
            }
            spans.push(Span::styled(
                bold,
                Style::default().add_modifier(Modifier::BOLD),
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
