use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Wrap};
use ratatui::Frame;

use crate::app::{
    App, ConversationBlockKind, ConversationBlockLineRange, ConversationBlockUiState,
    ToolCallDisplay, CONVERSATION_DETAIL_VIEWPORT,
};
use crate::components::markdown;
use crate::theme::{self, colors};

const COLLAPSED_DETAIL_LINES: usize = 3;

/// Render one tool block's bounded inspector. Expansion is per block, and an
/// expanded block remains bounded; when focused, j/k or PgUp/PgDn changes its
/// independent `scroll` offset instead of flooding the whole transcript.
fn push_tool_detail(
    lines: &mut Vec<Line<'static>>,
    app: &App,
    block_id: &str,
    tc: &ToolCallDisplay,
    state: &ConversationBlockUiState,
    focused: bool,
    width: u16,
) {
    // Arguments.
    let args = tc.arguments.trim();
    if !args.is_empty() && args != "null" && args != "{}" {
        let (argument_count, argument_lines) = app.chat.inspector_slice(
            &format!("{block_id}:args"),
            args,
            width.saturating_sub(9) as usize,
            0,
            3,
        );
        if state.expanded {
            for aline in &argument_lines {
                lines.push(Line::from(Span::styled(
                    format!("   args: {aline}"),
                    Style::default().fg(colors::subtle()),
                )));
            }
            let extra = argument_count.saturating_sub(3);
            if extra > 0 {
                lines.push(Line::from(Span::styled(
                    format!("   … {extra} more argument lines"),
                    Style::default().fg(colors::subtle()),
                )));
            }
        } else {
            let preview = argument_lines
                .first()
                .map(String::as_str)
                .unwrap_or_default();
            let ellipsis = if argument_count > 1 { "…" } else { "" };
            lines.push(Line::from(Span::styled(
                format!("   args: {preview}{ellipsis}"),
                Style::default().fg(colors::subtle()),
            )));
        }
    }

    // Result.
    let output = tc.display_output();
    if !output.is_empty() {
        let limit = if state.expanded {
            CONVERSATION_DETAIL_VIEWPORT
        } else {
            COLLAPSED_DETAIL_LINES
        };
        let output_key = format!("{block_id}:output");
        let (output_count, _) =
            app.chat
                .inspector_slice(&output_key, output, width.saturating_sub(3) as usize, 0, 0);
        let max_start = output_count.saturating_sub(limit);
        let start = if state.expanded {
            state.scroll.min(max_start)
        } else {
            0
        };
        let (_, output_lines) = app.chat.inspector_slice(
            &output_key,
            output,
            width.saturating_sub(3) as usize,
            start,
            limit,
        );
        if start > 0 {
            lines.push(Line::from(Span::styled(
                format!("   ↑ {start} earlier lines"),
                Style::default().fg(colors::subtle()),
            )));
        }
        for rline in &output_lines {
            lines.push(Line::from(Span::styled(
                format!("   {rline}"),
                Style::default().fg(colors::inactive()),
            )));
        }
        let hidden_after = output_count.saturating_sub(start + limit);
        if hidden_after > 0 {
            lines.push(Line::from(Span::styled(
                if state.expanded {
                    format!("   ↓ {hidden_after} later lines")
                } else {
                    format!("   … {hidden_after} more — focus then Enter to inspect")
                },
                Style::default().fg(colors::subtle()),
            )));
        }
    }

    // Error.
    if let Some(err) = &tc.error {
        let (error_count, error_lines) = app.chat.inspector_slice(
            &format!("{block_id}:error"),
            err,
            width.saturating_sub(10) as usize,
            0,
            3,
        );
        for (index, line) in error_lines.iter().enumerate() {
            lines.push(Line::from(Span::styled(
                if index == 0 {
                    format!("   Error: {line}")
                } else {
                    format!("          {line}")
                },
                Style::default().fg(colors::tool_error()),
            )));
        }
        let extra = error_count.saturating_sub(3);
        if extra > 0 {
            lines.push(Line::from(Span::styled(
                format!("   … {extra} more error lines · y copies exact text"),
                Style::default().fg(colors::tool_error()),
            )));
        }
    }
    if focused {
        lines.push(Line::from(Span::styled(
            if state.expanded {
                "   Enter collapse · j/k scroll · y copy"
            } else {
                "   Enter expand · y copy"
            },
            Style::default().fg(colors::subtle()),
        )));
    }
}

pub fn render(f: &mut Frame, content: Rect, input: Rect, app: &App) {
    let rendered = build_conversation_lines(app, content.width);
    let total_lines = rendered.visual_line_count;

    let visible_height = content.height;
    let max_scroll = total_lines.saturating_sub(visible_height);
    // Recorded every frame so key/mouse handlers (which only see `&mut App`,
    // not this frame's layout) can clamp `scroll_offset` — see
    // `ChatState::max_scroll`'s doc comment.
    app.chat.max_scroll.set(max_scroll);
    app.chat.content_height.set(visible_height);
    app.chat.content_width.set(content.width);
    *app.chat.block_line_ranges.borrow_mut() = rendered.ranges;
    let scroll_offset = if app.chat.auto_scroll {
        max_scroll
    } else {
        app.chat.scroll_offset.min(max_scroll)
    };

    let messages = Paragraph::new(rendered.lines)
        .wrap(Wrap { trim: false })
        .scroll((scroll_offset, 0));

    f.render_widget(messages, content);

    // The composer remains the stable editing surface while a run is active.
    // Enter is gated in `handle_chat_key`; editing here never implies mid-run
    // steering or a new server-side queue.
    f.render_widget(&app.chat.textarea, input);
}

struct RenderedConversation {
    lines: Vec<Line<'static>>,
    ranges: Vec<ConversationBlockLineRange>,
    visual_line_count: u16,
}

fn build_conversation_lines(app: &App, width: u16) -> RenderedConversation {
    let mut lines: Vec<Line> = Vec::new();
    let mut ranges = Vec::new();
    let mut visual_line_count = 0usize;

    for block in app.conversation_blocks() {
        let first_logical_line = lines.len();
        let start = visual_line_count;
        let focused = app.chat.focused_block.as_deref() == Some(block.id.as_str());
        let state = app
            .chat
            .block_ui
            .get(&block.id)
            .cloned()
            .unwrap_or_default();
        match block.kind {
            ConversationBlockKind::UserMessage(content) => {
                lines.push(Line::from(Span::styled(
                    if focused { "▸ user" } else { ">" },
                    Style::default()
                        .fg(colors::user_prefix())
                        .add_modifier(if focused {
                            ratatui::style::Modifier::BOLD
                        } else {
                            ratatui::style::Modifier::empty()
                        }),
                )));
                for line in content.lines() {
                    lines.push(Line::from(Span::styled(
                        line.to_string(),
                        Style::default().fg(colors::user_prefix()),
                    )));
                }
            }
            ConversationBlockKind::AssistantMarkdown { content, streaming } => {
                if focused {
                    lines.push(Line::from(Span::styled(
                        if streaming {
                            "▸ assistant · streaming"
                        } else {
                            "▸ assistant"
                        },
                        Style::default()
                            .fg(colors::brand())
                            .add_modifier(ratatui::style::Modifier::BOLD),
                    )));
                }
                let rendered = markdown::render_markdown(content, width);
                lines.extend(rendered);
            }
            ConversationBlockKind::Reasoning { content, streaming } => {
                let reasoning_key = format!("{}:detail", block.id);
                let (count, _) = app.chat.inspector_slice(
                    &reasoning_key,
                    content,
                    width.saturating_sub(1) as usize,
                    0,
                    0,
                );
                lines.push(Line::from(Span::styled(
                    format!(
                        "{} thinking{} · {count} lines",
                        if focused { "▸" } else { "──" },
                        if streaming { " · streaming" } else { "" }
                    ),
                    Style::default().fg(colors::thinking()),
                )));
                if state.expanded {
                    let start = state
                        .scroll
                        .min(count.saturating_sub(CONVERSATION_DETAIL_VIEWPORT));
                    let (_, detail) = app.chat.inspector_slice(
                        &reasoning_key,
                        content,
                        width.saturating_sub(1) as usize,
                        start,
                        CONVERSATION_DETAIL_VIEWPORT,
                    );
                    if start > 0 {
                        lines.push(Line::from(Span::styled(
                            format!(" ↑ {start} earlier lines"),
                            Style::default().fg(colors::subtle()),
                        )));
                    }
                    for line in &detail {
                        lines.push(Line::from(Span::styled(
                            format!(" {line}"),
                            Style::default().fg(colors::subtle()),
                        )));
                    }
                    let remaining = count.saturating_sub(start + CONVERSATION_DETAIL_VIEWPORT);
                    if remaining > 0 {
                        lines.push(Line::from(Span::styled(
                            format!(" ↓ {remaining} later lines"),
                            Style::default().fg(colors::subtle()),
                        )));
                    }
                } else {
                    lines.push(Line::from(Span::styled(
                        format!(" {count} reasoning lines hidden — focus then Enter to show"),
                        Style::default().fg(colors::subtle()),
                    )));
                }
                if focused {
                    lines.push(Line::from(Span::styled(
                        if state.expanded {
                            " Enter hide · j/k scroll · y copy"
                        } else {
                            " Enter show · y copy"
                        },
                        Style::default().fg(colors::subtle()),
                    )));
                }
            }
            ConversationBlockKind::ToolCall { tool, streaming } => {
                let tick = app.spinner_tick % theme::BRAILLE_SPINNER.len();
                let (icon, style) = match tool.phase.as_str() {
                    "complete" => ("✓", Style::default().fg(colors::tool_done())),
                    "error" => ("✗", Style::default().fg(colors::tool_error())),
                    _ if streaming => (
                        theme::BRAILLE_SPINNER[tick],
                        Style::default().fg(colors::tool_running()),
                    ),
                    _ => ("●", Style::default().fg(colors::tool_running())),
                };
                lines.push(Line::from(Span::styled(
                    format!(
                        " {} {icon} {} · {}",
                        if focused { "▸" } else { " " },
                        tool.tool_name,
                        tool.phase
                    ),
                    style,
                )));
                push_tool_detail(&mut lines, app, &block.id, tool, &state, focused, width);
            }
            ConversationBlockKind::SubAgent { child, streaming } => {
                let tick = app.spinner_tick % theme::BRAILLE_SPINNER.len();
                let (icon, style) = match child.status.as_str() {
                    "running" | "running_in_background" | "queued" | "starting" | "in_progress" => {
                        (
                            theme::BRAILLE_SPINNER[tick],
                            Style::default().fg(colors::tool_running()),
                        )
                    }
                    "completed" => ("✓", Style::default().fg(colors::tool_done())),
                    "error" | "cancelled" => ("✗", Style::default().fg(colors::tool_error())),
                    _ => ("·", Style::default().fg(colors::inactive())),
                };
                let label = child.title.as_deref().unwrap_or("sub-agent");
                lines.push(Line::from(Span::styled(
                    format!(
                        " {} {icon} {label} ({}) · {}",
                        if focused { "▸" } else { " " },
                        child.status,
                        child.child_session_id
                    ),
                    style,
                )));
                if state.expanded {
                    if let Some(error) = &child.error {
                        lines.push(Line::from(Span::styled(
                            format!("   Error: {error}"),
                            Style::default().fg(colors::tool_error()),
                        )));
                    }
                }
                if focused {
                    lines.push(Line::from(Span::styled(
                        if streaming {
                            "   Enter expand/collapse · child opens after parent run · y copy"
                        } else {
                            "   Enter open child · Ctrl+X expand/collapse · y copy"
                        },
                        Style::default().fg(colors::subtle()),
                    )));
                }
            }
            ConversationBlockKind::Question {
                question,
                source,
                submitting,
                dismissed,
            } => {
                let kind = if source.is_some_and(|source| {
                    let source = source.to_ascii_lowercase();
                    source.contains("permission") || source.contains("approval")
                }) {
                    "approval"
                } else {
                    "question"
                };
                let status = if submitting {
                    "submitting"
                } else if dismissed {
                    "dismissed · Ctrl+Q reopen"
                } else {
                    "answer in modal"
                };
                lines.push(Line::from(Span::styled(
                    format!(
                        " {} ? {kind} · {status}: {question}",
                        if focused { "▸" } else { " " }
                    ),
                    Style::default().fg(colors::warning()),
                )));
            }
            ConversationBlockKind::TerminalStatus(status) => {
                lines.push(Line::from(Span::styled(
                    format!(" {} ─ {status}", if focused { "▸" } else { " " }),
                    Style::default().fg(if status.starts_with("error") {
                        colors::tool_error()
                    } else {
                        colors::inactive()
                    }),
                )));
            }
        }
        let block_visual_lines = Paragraph::new(lines[first_logical_line..].to_vec())
            .wrap(Wrap { trim: false })
            .line_count(width.max(1));
        visual_line_count = visual_line_count.saturating_add(block_visual_lines);
        let end = visual_line_count.saturating_sub(1);
        ranges.push(ConversationBlockLineRange {
            id: block.id,
            start: u16::try_from(start).unwrap_or(u16::MAX),
            end: u16::try_from(end).unwrap_or(u16::MAX),
        });
        lines.push(Line::raw(""));
        visual_line_count = visual_line_count.saturating_add(1);
    }

    // Empty state
    if lines.is_empty() {
        lines.push(Line::from(Span::styled(
            "  No messages yet. Type a message below to start.",
            Style::default().fg(colors::inactive()),
        )));
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "  Tip: Use --model <name> to set the model.",
            Style::default().fg(colors::subtle()),
        )));
        visual_line_count = Paragraph::new(lines.clone())
            .wrap(Wrap { trim: false })
            .line_count(width.max(1));
    }

    RenderedConversation {
        lines,
        ranges,
        visual_line_count: u16::try_from(visual_line_count).unwrap_or(u16::MAX),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::BambooClient;
    use crate::app::{ChatMessage, MessageRole, SubAgentDisplay};
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    fn tc(args: &str, result: Option<&str>) -> ToolCallDisplay {
        ToolCallDisplay {
            id: "call-1".into(),
            tool_name: "Read".into(),
            arguments: args.into(),
            result: result.map(String::from),
            stream_output: String::new(),
            error: None,
            phase: "complete".into(),
        }
    }

    #[test]
    fn tool_detail_truncates_then_expands() {
        let t = tc("{\"path\":\"x\"}", Some("l1\nl2\nl3\nl4\nl5"));
        let app = App::new(BambooClient::new("http://127.0.0.1:0"));

        let mut lines: Vec<Line> = Vec::new();
        push_tool_detail(
            &mut lines,
            &app,
            "test:tool",
            &t,
            &ConversationBlockUiState::default(),
            false,
            100,
        );
        // args(1) + 3 result lines + "N more" marker(1) = 5
        assert_eq!(lines.len(), 5);

        let mut lines: Vec<Line> = Vec::new();
        push_tool_detail(
            &mut lines,
            &app,
            "test:tool",
            &t,
            &ConversationBlockUiState {
                expanded: true,
                scroll: 0,
            },
            false,
            100,
        );
        // args(1) + all 5 result lines = 6
        assert_eq!(lines.len(), 6);
    }

    #[test]
    fn empty_args_and_no_result_render_nothing() {
        let app = App::new(BambooClient::new("http://127.0.0.1:0"));
        let mut lines: Vec<Line> = Vec::new();
        push_tool_detail(
            &mut lines,
            &app,
            "test:tool",
            &tc("{}", None),
            &ConversationBlockUiState::default(),
            false,
            100,
        );
        assert!(lines.is_empty());
    }

    fn rendered_text(rendered: &RenderedConversation) -> String {
        rendered
            .lines
            .iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn per_block_reasoning_and_tool_inspectors_are_independent_and_bounded() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        let output = (0..30)
            .map(|index| format!("result-{index}"))
            .collect::<Vec<_>>()
            .join("\n");
        app.chat.messages.push(ChatMessage {
            id: "message-1".to_string(),
            role: MessageRole::Assistant,
            content: "answer".to_string(),
            reasoning: Some(
                (0..25)
                    .map(|index| format!("reason-{index}"))
                    .collect::<Vec<_>>()
                    .join("\n"),
            ),
            tool_calls: vec![ToolCallDisplay {
                id: "call-1".to_string(),
                tool_name: "Read".to_string(),
                arguments: "{}".to_string(),
                result: Some(output),
                stream_output: String::new(),
                error: None,
                phase: "complete".to_string(),
            }],
            sub_agents: vec![SubAgentDisplay {
                child_session_id: "child-123456789".to_string(),
                title: Some("research".to_string()),
                status: "completed".to_string(),
                error: None,
            }],
            terminal_status: Some("completed".to_string()),
        });
        app.chat.block_ui.insert(
            "message-1:tool:call-1".to_string(),
            ConversationBlockUiState {
                expanded: true,
                scroll: 5,
            },
        );
        app.chat.block_ui.insert(
            "message-1:reasoning".to_string(),
            ConversationBlockUiState::default(),
        );
        app.chat.focused_block = Some("message-1:tool:call-1".to_string());

        let rendered = build_conversation_lines(&app, 100);
        let text = rendered_text(&rendered);
        assert!(text.contains("25 reasoning lines hidden"));
        assert!(!text.contains("reason-0"));
        assert!(text.contains("result-5"));
        assert!(text.contains("result-14"));
        assert!(!text.contains("result-4"));
        assert!(!text.contains("result-15"));
        assert!(text.contains("15 later lines"));
        assert!(text.contains("research (completed) · child-123456789"));
        assert!(text.contains("Enter collapse · j/k scroll · y copy"));
        assert!(rendered
            .ranges
            .iter()
            .any(|range| range.id == "message-1:tool:call-1"));
    }

    #[test]
    fn narrow_long_single_lines_use_visual_ranges_and_bounded_inspectors() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.messages.push(ChatMessage {
            id: "message-narrow".to_string(),
            role: MessageRole::Assistant,
            content: "answer".to_string(),
            reasoning: Some("界".repeat(80)),
            tool_calls: vec![ToolCallDisplay {
                id: "call-narrow".to_string(),
                tool_name: "Read".to_string(),
                arguments: "x".repeat(120),
                result: Some("y".repeat(200)),
                stream_output: String::new(),
                error: None,
                phase: "complete".to_string(),
            }],
            sub_agents: Vec::new(),
            terminal_status: Some("completed".to_string()),
        });
        for id in [
            "message-narrow:reasoning",
            "message-narrow:tool:call-narrow",
        ] {
            app.chat.block_ui.insert(
                id.to_string(),
                ConversationBlockUiState {
                    expanded: true,
                    scroll: 0,
                },
            );
        }

        let width = 20;
        let rendered = build_conversation_lines(&app, width);
        let exact_visual_count = Paragraph::new(rendered.lines.clone())
            .wrap(Wrap { trim: false })
            .line_count(width);
        assert_eq!(rendered.visual_line_count as usize, exact_visual_count);
        let text = rendered_text(&rendered);
        assert!(text.contains("later lines"));
        assert!(text.contains("more argument lines"));
        assert!(
            rendered.lines.len() < 40,
            "long no-newline payloads must stay bounded"
        );
        assert!(rendered.visual_line_count as usize > rendered.lines.len());
    }

    #[test]
    fn testbackend_keeps_streaming_composer_visible_with_structured_rows() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.session_id = Some("parent".to_string());
        app.chat.streaming = true;
        app.chat.current_turn_id = Some("run:1".to_string());
        for character in "draft during run".chars() {
            app.chat.textarea.input(crossterm::event::KeyEvent::new(
                crossterm::event::KeyCode::Char(character),
                crossterm::event::KeyModifiers::empty(),
            ));
        }
        app.chat.current_tool_calls.push(ToolCallDisplay {
            id: "tool-live".to_string(),
            tool_name: "Shell".to_string(),
            arguments: "{\"cmd\":\"pwd\"}".to_string(),
            result: None,
            stream_output: "workspace".to_string(),
            error: None,
            phase: "streaming".to_string(),
        });
        app.chat.sub_agents.push(SubAgentDisplay {
            child_session_id: "child-live".to_string(),
            title: Some("reviewer".to_string()),
            status: "running".to_string(),
            error: None,
        });

        let backend = TestBackend::new(100, 30);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| crate::ui::render(frame, &app))
            .unwrap();
        let text = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(text.contains("draft during run"));
        assert!(text.contains("Shell · streaming"));
        assert!(text.contains("reviewer (running)"));
        assert!(text.contains("draft editable; Enter sends after run"));
        assert!(!text.contains("Waiting for response"));
    }
}
