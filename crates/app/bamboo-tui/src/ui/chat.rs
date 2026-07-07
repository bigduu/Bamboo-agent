use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Wrap};
use ratatui::Frame;

use crate::app::{App, MessageRole, ToolCallDisplay};
use crate::components::markdown;
use crate::theme::{self, colors};

/// Render a tool call's arguments, result, and error under its name line.
/// Truncated by default (args → one preview line, result → 3 lines); `expand`
/// (Ctrl+X on the Chat tab) shows everything.
fn push_tool_detail(lines: &mut Vec<Line<'static>>, tc: &ToolCallDisplay, expand: bool) {
    // Arguments.
    let args = tc.arguments.trim();
    if !args.is_empty() && args != "null" && args != "{}" {
        if expand {
            for aline in args.lines() {
                lines.push(Line::from(Span::styled(
                    format!("   args: {aline}"),
                    Style::default().fg(colors::SUBTLE),
                )));
            }
        } else {
            let preview: String = args.chars().take(88).collect();
            let ellipsis = if args.chars().count() > 88 { "…" } else { "" };
            lines.push(Line::from(Span::styled(
                format!("   args: {preview}{ellipsis}"),
                Style::default().fg(colors::SUBTLE),
            )));
        }
    }

    // Result.
    if let Some(result) = &tc.result {
        let max = if expand { usize::MAX } else { 3 };
        for rline in result.lines().take(max) {
            lines.push(Line::from(Span::styled(
                format!("   {rline}"),
                Style::default().fg(colors::INACTIVE),
            )));
        }
        let total = result.lines().count();
        if !expand && total > 3 {
            lines.push(Line::from(Span::styled(
                format!("   … ({} more — Ctrl+X to expand)", total - 3),
                Style::default().fg(colors::SUBTLE),
            )));
        }
    }

    // Error.
    if let Some(err) = &tc.error {
        lines.push(Line::from(Span::styled(
            format!("   Error: {err}"),
            Style::default().fg(colors::TOOL_ERROR),
        )));
    }
}

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
                    push_tool_detail(&mut lines, tc, app.chat.expand_tools);
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
            push_tool_detail(&mut lines, tc, app.chat.expand_tools);
        }

        // Sub-agents spawned by this run.
        if !app.chat.sub_agents.is_empty() {
            for sa in &app.chat.sub_agents {
                let (icon, style) = match sa.status.as_str() {
                    "running" => {
                        let tick = app.spinner_tick % theme::BRAILLE_SPINNER.len();
                        (
                            theme::BRAILLE_SPINNER[tick],
                            Style::default().fg(colors::TOOL_RUNNING),
                        )
                    }
                    "completed" => ("✓", Style::default().fg(colors::TOOL_DONE)),
                    "error" | "cancelled" => ("✗", Style::default().fg(colors::TOOL_ERROR)),
                    _ => ("·", Style::default().fg(colors::INACTIVE)),
                };
                let label = sa.title.clone().unwrap_or_else(|| {
                    let short: String = sa.child_session_id.chars().take(8).collect();
                    format!("sub-agent {short}")
                });
                lines.push(Line::from(Span::styled(
                    format!(" {icon} ▸ {label} ({})", sa.status),
                    style,
                )));
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn tc(args: &str, result: Option<&str>) -> ToolCallDisplay {
        ToolCallDisplay {
            tool_name: "Read".into(),
            arguments: args.into(),
            result: result.map(String::from),
            error: None,
            phase: "complete".into(),
        }
    }

    #[test]
    fn tool_detail_truncates_then_expands() {
        let t = tc("{\"path\":\"x\"}", Some("l1\nl2\nl3\nl4\nl5"));

        let mut lines: Vec<Line> = Vec::new();
        push_tool_detail(&mut lines, &t, false);
        // args(1) + 3 result lines + "N more" marker(1) = 5
        assert_eq!(lines.len(), 5);

        let mut lines: Vec<Line> = Vec::new();
        push_tool_detail(&mut lines, &t, true);
        // args(1) + all 5 result lines = 6
        assert_eq!(lines.len(), 6);
    }

    #[test]
    fn empty_args_and_no_result_render_nothing() {
        let mut lines: Vec<Line> = Vec::new();
        push_tool_detail(&mut lines, &tc("{}", None), false);
        assert!(lines.is_empty());
    }
}
