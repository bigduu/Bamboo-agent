use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Wrap};
use ratatui::Frame;

use crate::app::{App, MessageRole};
use crate::components::markdown;
use crate::theme::{self, colors};

pub fn render(f: &mut Frame, content: Rect, input: Rect, app: &App) {
    let lines = build_message_lines(app, content.width);
    let total_lines = lines.len() as u16;

    let visible_height = content.height;
    let scroll_offset = if app.chat.auto_scroll {
        total_lines.saturating_sub(visible_height)
    } else {
        app.chat
            .scroll_offset
            .min(total_lines.saturating_sub(visible_height))
    };

    let messages = Paragraph::new(lines)
        .wrap(Wrap { trim: false })
        .scroll((scroll_offset, 0));

    f.render_widget(messages, content);

    // Input area
    if app.chat.streaming {
        let blocked = Paragraph::new(Line::from(vec![
            Span::styled("❯ ", Style::default().fg(colors::BRAND)),
            Span::styled(
                "Waiting for response... (Ctrl+S to stop, j/k to scroll)",
                Style::default().fg(colors::INACTIVE),
            ),
        ]));
        f.render_widget(blocked, input);
    } else {
        f.render_widget(&app.chat.textarea, input);
    }
}

fn build_message_lines(app: &App, width: u16) -> Vec<Line<'static>> {
    let mut lines: Vec<Line> = Vec::new();

    for msg in &app.chat.messages {
        match msg.role {
            MessageRole::User => {
                // User message: "> text" prefix style
                lines.push(Line::from(Span::styled(
                    ">",
                    Style::default().fg(colors::USER_PREFIX),
                )));
                for line in msg.content.lines() {
                    lines.push(Line::from(Span::styled(
                        line.to_string(),
                        Style::default().fg(colors::USER_PREFIX),
                    )));
                }
                lines.push(Line::raw(""));
            }
            MessageRole::Assistant => {
                // Assistant message: markdown rendered content
                let rendered = markdown::render_markdown(&msg.content, width);
                lines.extend(rendered);

                // Tool calls
                for tc in &msg.tool_calls {
                    let (icon, style) = match tc.phase.as_str() {
                        "complete" => ("✓", Style::default().fg(colors::TOOL_DONE)),
                        "error" => ("✗", Style::default().fg(colors::TOOL_ERROR)),
                        _ => ("●", Style::default().fg(colors::TOOL_RUNNING)),
                    };
                    lines.push(Line::from(Span::styled(
                        format!(" {} {}", icon, tc.tool_name),
                        style,
                    )));
                    if let Some(result) = &tc.result {
                        for rline in result.lines().take(3) {
                            lines.push(Line::from(Span::styled(
                                format!("   {}", rline),
                                Style::default().fg(colors::INACTIVE),
                            )));
                        }
                        let total = result.lines().count();
                        if total > 3 {
                            lines.push(Line::from(Span::styled(
                                format!("   ... ({} more lines)", total - 3),
                                Style::default().fg(colors::SUBTLE),
                            )));
                        }
                    }
                    if let Some(err) = &tc.error {
                        lines.push(Line::from(Span::styled(
                            format!("   Error: {}", err),
                            Style::default().fg(colors::TOOL_ERROR),
                        )));
                    }
                }

                // Reasoning / thinking block
                if let Some(reasoning) = &msg.reasoning {
                    let sep_len: usize = 40;
                    lines.push(Line::from(Span::styled(
                        format!("── thinking {}", "─".repeat(sep_len.saturating_sub(14))),
                        Style::default().fg(colors::THINKING),
                    )));
                    for rline in reasoning.lines().take(5) {
                        lines.push(Line::from(Span::styled(
                            format!(" {}", rline),
                            Style::default().fg(colors::SUBTLE),
                        )));
                    }
                    let total = reasoning.lines().count();
                    if total > 5 {
                        lines.push(Line::from(Span::styled(
                            format!(" ... ({} more lines)", total - 5),
                            Style::default().fg(colors::SUBTLE),
                        )));
                    }
                    lines.push(Line::from(Span::styled(
                        "─".repeat(sep_len),
                        Style::default().fg(colors::THINKING),
                    )));
                }

                lines.push(Line::raw(""));
            }
        }
    }

    // Current streaming response
    if app.chat.streaming {
        if !app.chat.current_response.is_empty() {
            let rendered = markdown::render_markdown(&app.chat.current_response, width);
            lines.extend(rendered);
        } else {
            lines.push(Line::from(Span::styled(
                "...",
                Style::default().fg(colors::INACTIVE),
            )));
        }

        // Streaming tool calls
        for tc in &app.chat.current_tool_calls {
            let tick = app.spinner_tick % theme::BRAILLE_SPINNER.len();
            let (icon, style) = match tc.phase.as_str() {
                "complete" => ("✓", Style::default().fg(colors::TOOL_DONE)),
                "error" => ("✗", Style::default().fg(colors::TOOL_ERROR)),
                _ => (
                    theme::BRAILLE_SPINNER[tick],
                    Style::default().fg(colors::TOOL_RUNNING),
                ),
            };
            lines.push(Line::from(Span::styled(
                format!(" {} {}", icon, tc.tool_name),
                style,
            )));
        }

        // Streaming thinking
        if !app.chat.current_reasoning.is_empty() {
            let sep_len: usize = 40;
            lines.push(Line::from(Span::styled(
                format!("── thinking {}", "─".repeat(sep_len.saturating_sub(14))),
                Style::default().fg(colors::THINKING),
            )));
            for rline in app.chat.current_reasoning.lines().take(3) {
                lines.push(Line::from(Span::styled(
                    format!(" {}", rline),
                    Style::default().fg(colors::SUBTLE),
                )));
            }
        }
    }

    // Empty state
    if lines.is_empty() {
        lines.push(Line::from(Span::styled(
            "  No messages yet. Type a message below to start.",
            Style::default().fg(colors::INACTIVE),
        )));
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "  Tip: Use --model <name> to set the model.",
            Style::default().fg(colors::SUBTLE),
        )));
    }

    lines
}
