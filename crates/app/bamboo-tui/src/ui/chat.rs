use ratatui::layout::{Alignment, Margin, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Paragraph, Wrap};
use ratatui::Frame;

use crate::app::{
    App, ComposerAttachment, ConversationBlockKind, ConversationBlockLineRange,
    ConversationBlockUiState, ToolCallDisplay, CONVERSATION_DETAIL_VIEWPORT,
};
use crate::components::markdown;
use crate::file_change::FileChangeState;
use crate::file_reference::file_name;
use crate::keymap::{ActionContext, ActionId};
use crate::text::{clip_tail_cells, display_width, hard_wrap};
use crate::theme::{self, colors, ThemePalette};

const COLLAPSED_DETAIL_LINES: usize = 3;
const COLLAPSED_DIFF_LINES: usize = 4;

fn push_file_change_detail(
    lines: &mut Vec<Line<'static>>,
    app: &App,
    block_id: &str,
    tc: &ToolCallDisplay,
    state: &ConversationBlockUiState,
    focused: bool,
    width: u16,
) -> bool {
    let Some(change) = app.chat.file_change_view(block_id, tc) else {
        return false;
    };
    lines.extend(change.summary_lines(
        FileChangeState::from_phase(&tc.phase),
        "   ",
        width as usize,
    ));

    let rows = change.rendered_rows(width.saturating_sub(3) as usize, state.diff_wrap);
    let limit = if state.expanded {
        CONVERSATION_DETAIL_VIEWPORT
    } else {
        COLLAPSED_DIFF_LINES
    };
    let max_start = rows.len().saturating_sub(limit);
    let start = if state.expanded {
        state.scroll.min(max_start)
    } else {
        0
    };
    if start > 0 {
        lines.push(Line::from(Span::styled(
            format!("   ↑ {start} earlier diff rows"),
            Style::default().fg(colors::subtle()),
        )));
    }
    lines.extend(
        rows.iter()
            .skip(start)
            .take(limit)
            .map(|row| row.styled_line("   ")),
    );
    let hidden_after = rows.len().saturating_sub(start + limit);
    if hidden_after > 0 {
        lines.push(Line::from(Span::styled(
            if state.expanded {
                format!("   ↓ {hidden_after} later diff rows")
            } else {
                format!(
                    "   … {hidden_after} more — focus then {} to inspect",
                    app.key_hint(ActionContext::ConversationBlock, ActionId::Activate)
                )
            },
            Style::default().fg(colors::subtle()),
        )));
    }
    if focused {
        lines.push(Line::from(Span::styled(
            if state.expanded {
                format!(
                    "   {} collapse · {}/{} scroll · {}/{} hunks · {} {} · {} copy exact diff",
                    app.key_hint(ActionContext::ConversationBlock, ActionId::Activate),
                    app.key_hint(ActionContext::ConversationBlock, ActionId::ScrollBlockUp),
                    app.key_hint(ActionContext::ConversationBlock, ActionId::ScrollBlockDown),
                    app.key_hint(ActionContext::ConversationBlock, ActionId::PreviousDiffHunk),
                    app.key_hint(ActionContext::ConversationBlock, ActionId::NextDiffHunk),
                    app.key_hint(ActionContext::ConversationBlock, ActionId::ToggleDiffWrap),
                    if state.diff_wrap { "clip" } else { "wrap" },
                    app.key_hint(ActionContext::ConversationBlock, ActionId::CopyValue),
                )
            } else {
                format!(
                    "   {} expand · {} copy exact diff",
                    app.key_hint(ActionContext::ConversationBlock, ActionId::Activate),
                    app.key_hint(ActionContext::ConversationBlock, ActionId::CopyValue),
                )
            },
            Style::default().fg(colors::subtle()),
        )));
    }
    true
}

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
    if push_file_change_detail(lines, app, block_id, tc, state, focused, width) {
        return;
    }

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
                    format!(
                        "   … {hidden_after} more — focus then {} to inspect",
                        app.key_hint(ActionContext::ConversationBlock, ActionId::Activate)
                    )
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
                format!(
                    "   … {extra} more error lines · {} copies exact text",
                    app.key_hint(ActionContext::ConversationBlock, ActionId::CopyValue)
                ),
                Style::default().fg(colors::tool_error()),
            )));
        }
    }
    if focused {
        lines.push(Line::from(Span::styled(
            if state.expanded {
                format!(
                    "   {} collapse · {}/{} scroll · {} copy",
                    app.key_hint(ActionContext::ConversationBlock, ActionId::Activate),
                    app.key_hint(ActionContext::ConversationBlock, ActionId::ScrollBlockUp),
                    app.key_hint(ActionContext::ConversationBlock, ActionId::ScrollBlockDown),
                    app.key_hint(ActionContext::ConversationBlock, ActionId::CopyValue),
                )
            } else {
                format!(
                    "   {} expand · {} copy",
                    app.key_hint(ActionContext::ConversationBlock, ActionId::Activate),
                    app.key_hint(ActionContext::ConversationBlock, ActionId::CopyValue),
                )
            },
            Style::default().fg(colors::subtle()),
        )));
    }
}

pub fn render(f: &mut Frame, content: Rect, input: Rect, app: &App) {
    render_transcript(f, content, app, false);

    // Editing remains available while a run is active. The action reducer
    // owns when Enter can send a draft.
    let draft = render_composer_attachments(f, input, app);
    f.render_widget(&app.chat.textarea, draft);
}

pub(super) fn render_workspace(f: &mut Frame, content: Rect, input: Rect, app: &App) {
    let content = content.inner(Margin::new(1, 0));
    render_transcript(f, content, app, true);

    let style = super::workspace::surface_style(app.theme);
    f.render_widget(Block::default().style(style), input);
    let draft = render_composer_attachments(
        f,
        Rect::new(
            input.x + 3,
            input.y,
            input.width.saturating_sub(4),
            input.height,
        ),
        app,
    );
    f.render_widget(
        Paragraph::new(" › ").style(style),
        Rect::new(input.x, draft.y, 3, draft.height),
    );
    f.render_widget(&app.chat.textarea, draft);
}

fn file_reference_style(theme: ThemePalette) -> Style {
    Style::default()
        .fg(theme.color(theme::ColorRole::UserPrefix))
        .bg(theme.canvas())
        .add_modifier(Modifier::BOLD)
}

fn render_composer_attachments(f: &mut Frame, area: Rect, app: &App) -> Rect {
    app.chat.attachment_remove_buttons.borrow_mut().clear();
    if app.chat.composer_attachments.is_empty() || area.height < 2 {
        return area;
    }
    let row = composer_attachment_row(area, app);
    f.render_widget(
        Paragraph::new(row),
        Rect::new(area.x, area.y, area.width, 1),
    );
    Rect::new(area.x, area.y + 1, area.width, area.height - 1)
}

fn composer_attachment_row(area: Rect, app: &App) -> Line<'static> {
    let attachments = &app.chat.composer_attachments;
    let theme = app.theme;
    let width = area.width as usize;
    let mut spans = Vec::new();
    let mut remaining = width;
    let mut remove_buttons = app.chat.attachment_remove_buttons.borrow_mut();
    // Keep the screenshot and its remove control visible beside long file names.
    let images = attachments
        .iter()
        .filter(|item| matches!(item, ComposerAttachment::Image(_)));
    let files = attachments
        .iter()
        .filter(|item| matches!(item, ComposerAttachment::File(_)));
    for (index, attachment) in images.chain(files).enumerate() {
        let (name, padding) = match attachment {
            ComposerAttachment::File(file) => (&file.name, 2),
            ComposerAttachment::Image(image) => (&image.label, 4),
        };
        let after = attachments.len() - index - 1;
        let reserve = if after == 0 {
            0
        } else {
            format!(" +{after}").len()
        };
        let max_name = remaining.saturating_sub(reserve + padding);
        if max_name == 0 || (index > 0 && display_width(name) > max_name) {
            spans.push(Span::styled(
                format!("+{}", attachments.len() - index),
                Style::default().fg(theme.color(theme::ColorRole::Inactive)),
            ));
            break;
        }
        let label = clip_tail_cells(name, max_name);
        let label_width = display_width(&label);
        let style = file_reference_style(theme);
        spans.push(Span::styled(format!(" {label} "), style));
        if let ComposerAttachment::Image(image) = attachment {
            let left = area.x + (width - remaining + label_width + 2) as u16;
            remove_buttons.push((Rect::new(left, area.y, 1, 1), image.clone()));
            spans.push(Span::styled(
                "× ",
                style.fg(theme.color(theme::ColorRole::Inactive)),
            ));
        }
        remaining -= label_width + padding;
        if after > 0 {
            spans.push(Span::raw(" "));
            remaining -= 1;
        }
    }
    Line::from(spans)
}

fn image_attachment_label(count: usize) -> String {
    if count == 1 {
        "Image attachment".to_string()
    } else {
        format!("{count} image attachments")
    }
}

fn render_transcript(f: &mut Frame, content: Rect, app: &App, workspace: bool) {
    let rendered = build_conversation_lines(app, content.width, workspace);
    let total_lines = rendered.visual_line_count;

    let visible_height = content.height;
    let max_scroll = total_lines.saturating_sub(visible_height);
    // Recorded every frame so key/mouse handlers (which only see `&mut App`,
    // not this frame's layout) can clamp `scroll_offset` — see
    // `ChatState::max_scroll`'s doc comment.
    app.chat.max_scroll.set(max_scroll);
    app.chat.content_height.set(visible_height);
    app.chat.content_width.set(content.width);
    app.chat.transcript_area.set(content);
    *app.chat.block_line_ranges.borrow_mut() = rendered.ranges;
    let scroll_offset = if app.chat.auto_scroll {
        max_scroll
    } else {
        app.chat.scroll_offset.min(max_scroll)
    };

    let mut messages = Paragraph::new(rendered.lines)
        .wrap(Wrap { trim: false })
        .scroll((scroll_offset, 0));
    if workspace {
        messages = messages.style(super::workspace::canvas_style(app.theme));
    }

    f.render_widget(messages, content);
}

struct RenderedConversation {
    lines: Vec<Line<'static>>,
    ranges: Vec<ConversationBlockLineRange>,
    visual_line_count: u16,
}

fn build_conversation_lines(app: &App, width: u16, workspace: bool) -> RenderedConversation {
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
            ConversationBlockKind::UserMessage {
                content,
                image_count,
            } if workspace => {
                let width = width.saturating_sub(4).clamp(1, 76) as usize;
                let style = super::workspace::surface_style(app.theme);
                let mut marker = if focused { "▸" } else { "›" };
                for line in content.split('\n').filter(|_| !content.is_empty()) {
                    let filename = file_name(line);
                    let value = filename.as_deref().unwrap_or(line);
                    let row_style = if filename.is_some() {
                        file_reference_style(app.theme).bg(app.theme.surface())
                    } else {
                        style
                    };
                    for row in hard_wrap(value, width) {
                        lines.push(
                            Line::from(vec![
                                Span::styled(format!(" {marker} "), style),
                                Span::styled(row, row_style),
                                Span::styled(" ", style),
                            ])
                            .alignment(Alignment::Right),
                        );
                        marker = " ";
                    }
                }
                if image_count > 0 {
                    lines.push(
                        Line::from(vec![
                            Span::styled(format!(" {marker} "), style),
                            Span::styled(
                                format!("{} ", image_attachment_label(image_count)),
                                file_reference_style(app.theme).bg(app.theme.surface()),
                            ),
                        ])
                        .alignment(Alignment::Right),
                    );
                }
            }
            ConversationBlockKind::UserMessage {
                content,
                image_count,
            } => {
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
                    let name = file_name(line);
                    lines.push(Line::from(Span::styled(
                        name.as_deref().unwrap_or(line).to_string(),
                        if name.is_some() {
                            file_reference_style(app.theme)
                        } else {
                            Style::default().fg(colors::user_prefix())
                        },
                    )));
                }
                if image_count > 0 {
                    lines.push(Line::from(Span::styled(
                        image_attachment_label(image_count),
                        file_reference_style(app.theme),
                    )));
                }
            }
            ConversationBlockKind::AssistantMarkdown { content, streaming } => {
                if workspace {
                    lines.push(Line::styled(
                        if streaming {
                            "◆ Bamboo · streaming"
                        } else {
                            "◆ Bamboo"
                        },
                        Style::default().add_modifier(Modifier::BOLD),
                    ));
                } else if focused {
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
                let rendered = if streaming {
                    markdown::render_streaming_markdown(content, width)
                } else {
                    markdown::render_markdown(content, width)
                };
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
                        format!(
                            " {count} reasoning lines hidden — focus then {} to show",
                            app.key_hint(ActionContext::ConversationBlock, ActionId::Activate)
                        ),
                        Style::default().fg(colors::subtle()),
                    )));
                }
                if focused {
                    lines.push(Line::from(Span::styled(
                        if state.expanded {
                            format!(
                                " {} hide · {}/{} scroll · {} copy",
                                app.key_hint(ActionContext::ConversationBlock, ActionId::Activate),
                                app.key_hint(
                                    ActionContext::ConversationBlock,
                                    ActionId::ScrollBlockUp
                                ),
                                app.key_hint(
                                    ActionContext::ConversationBlock,
                                    ActionId::ScrollBlockDown
                                ),
                                app.key_hint(ActionContext::ConversationBlock, ActionId::CopyValue),
                            )
                        } else {
                            format!(
                                " {} show · {} copy",
                                app.key_hint(ActionContext::ConversationBlock, ActionId::Activate),
                                app.key_hint(ActionContext::ConversationBlock, ActionId::CopyValue),
                            )
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
            ConversationBlockKind::SubAgent { child } => {
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
                let marker = if focused { "▸" } else { " " };
                if workspace {
                    const OPEN_CHAT: &str = "  Open chat ›";
                    let short_id = crate::subagents::short_session_id(&child.child_session_id);
                    let label = child.title.as_deref().unwrap_or(&short_id);
                    let summary = format!(" {marker} {icon} {label} · {}", child.status);
                    lines.push(Line::from(vec![
                        Span::styled(
                            crate::text::clip_cells(
                                &summary,
                                usize::from(width).saturating_sub(OPEN_CHAT.chars().count()),
                            ),
                            style,
                        ),
                        Span::styled(OPEN_CHAT, Style::default().fg(colors::thinking())),
                    ]));
                } else {
                    lines.push(Line::styled(
                        format!(
                            " {marker} {icon} {label} ({}) · {}",
                            child.status, child.child_session_id
                        ),
                        style,
                    ));
                }
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
                        format!(
                            "   {} open child · {} expand/collapse · {} copy",
                            app.key_hint(ActionContext::ConversationBlock, ActionId::Activate),
                            app.key_hint(ActionContext::ConversationBlock, ActionId::ToggleDetails),
                            app.key_hint(ActionContext::ConversationBlock, ActionId::CopyValue),
                        ),
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
                    "submitting".to_string()
                } else if dismissed {
                    format!(
                        "dismissed · {} reopen",
                        app.key_hint(ActionContext::Global, ActionId::ReopenPendingQuestion)
                    )
                } else {
                    "answer in modal".to_string()
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

    if lines.is_empty() {
        if workspace {
            let sessions = app.primary_key_hint(ActionContext::Global, ActionId::OpenSessionPicker);
            lines.push(Line::styled(
                format!(" Type a message, or use {sessions} to open recent sessions."),
                Style::default().fg(colors::inactive()),
            ));
        } else {
            lines.extend([
                Line::styled(
                    "  No messages yet. Type a message below to start.",
                    Style::default().fg(colors::inactive()),
                ),
                Line::raw(""),
                Line::styled(
                    "  Tip: Use --model <name> to set the model.",
                    Style::default().fg(colors::subtle()),
                ),
            ]);
        }
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
    use crate::api::types::{HistoryFunctionCall, HistoryMessage, HistoryToolCall};
    use crate::api::BambooClient;
    use crate::app::{ChatMessage, MessageRole, SubAgentDisplay};
    use crate::history::map_history;
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
    fn rendered_file_reference_is_bold_and_retains_the_original_message() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        let path = "/work/media/Bamboo draft.png";
        app.chat.messages.push(ChatMessage {
            id: "user-file".into(),
            role: MessageRole::User,
            content: path.into(),
            tool_calls: Vec::new(),
            reasoning: None,
            sub_agents: Vec::new(),
            image_count: 0,
            terminal_status: None,
        });
        let view = build_conversation_lines(&app, 60, true);
        assert!(view.lines[0]
            .spans
            .iter()
            .all(|span| !span.content.contains("/work")));
        let filename = view.lines[0]
            .spans
            .iter()
            .find(|span| span.content == "Bamboo draft.png")
            .unwrap();
        assert!(filename.style.add_modifier.contains(Modifier::BOLD));
        assert!(matches!(
            app.conversation_blocks()[0].kind,
            ConversationBlockKind::UserMessage { content, .. } if content == path
        ));
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
                diff_wrap: true,
            },
            false,
            100,
        );
        // args(1) + all 5 result lines = 6
        assert_eq!(lines.len(), 6);
    }

    #[test]
    fn expanded_reloaded_browser_approval_renders_only_safe_display_copy() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.messages = map_history(vec![
            HistoryMessage {
                id: "assistant-1".to_string(),
                role: "assistant".to_string(),
                tool_calls: Some(vec![HistoryToolCall {
                    id: "browser-call".to_string(),
                    function: HistoryFunctionCall {
                        name: "browser".to_string(),
                        arguments:
                            r#"{"action":"type","text":"private input","expected_epoch":17}"#
                                .to_string(),
                    },
                }]),
                ..Default::default()
            },
            HistoryMessage {
                role: "tool".to_string(),
                tool_call_id: Some("browser-call".to_string()),
                content: r#"{"status":"awaiting_permission_approval","question":"Approve private input?","permission_request":{"tool_name":"browser","resource":"browser:17:type:focused:opaque"}}"#.to_string(),
                tool_success: Some(true),
                ..Default::default()
            },
        ]);
        app.chat.block_ui.insert(
            "assistant-1:tool:browser-call".to_string(),
            ConversationBlockUiState {
                expanded: true,
                ..Default::default()
            },
        );
        let rendered = rendered_text(&build_conversation_lines(&app, 100, false));
        assert!(rendered.contains("Browser input awaiting permission approval"));
        assert!(rendered.contains("[redacted]"));
        assert!(!rendered.contains("private input"));
        assert!(!rendered.contains("opaque"));
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

    fn file_change_payload(truncated: bool) -> String {
        serde_json::json!({
            "operation": "Edit",
            "message": "Edited file",
            "file_path": "/workspace/界/demo.rs",
            "workspace": "/workspace/界",
            "checkpoint": {
                "created": true,
                "id": "checkpoint-1",
                "path": "/checkpoints/demo.rs",
                "size_bytes": 4
            },
            "diagnostics": {"format": "rust", "valid": true},
            "diff": {
                "format": "unified",
                "unified": if truncated {
                    "--- a/demo.rs\n+++ b/demo.rs\n@@ -1,1 +1,1 @@\n-old\n+new\n... diff truncated (20 more lines)"
                } else {
                    "--- a/demo.rs\n+++ b/demo.rs\n@@ -1,1 +1,1 @@\n-old\n+new"
                },
                "old_line_count": 1,
                "new_line_count": 1,
                "added_lines": 1,
                "removed_lines": 1,
                "old_trailing_newline": true,
                "new_trailing_newline": false,
                "truncated": truncated
            }
        })
        .to_string()
    }

    fn tool_detail_text(
        tool: &ToolCallDisplay,
        state: &ConversationBlockUiState,
        width: u16,
    ) -> String {
        let app = App::new(BambooClient::new("http://127.0.0.1:0"));
        let mut lines = Vec::new();
        push_tool_detail(
            &mut lines,
            &app,
            "test:file-change",
            tool,
            state,
            false,
            width,
        );
        lines
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
    fn canonical_file_changes_render_specialized_states_and_raw_fallbacks() {
        let base = ToolCallDisplay {
            id: "change".to_string(),
            tool_name: "Edit".to_string(),
            arguments: r#"{"file_path":"/workspace/界/demo.rs"}"#.to_string(),
            result: Some(file_change_payload(false)),
            stream_output: String::new(),
            error: None,
            phase: "complete".to_string(),
        };
        let state = ConversationBlockUiState {
            expanded: true,
            ..Default::default()
        };
        let applied = tool_detail_text(&base, &state, 80);
        for expected in [
            "APPLIED · Edit · /workspace/界/demo.rs",
            "+1 added · -1 removed",
            "checkpoint saved",
            "diagnostics rust valid",
            "- old",
            "+ new",
        ] {
            assert!(
                applied.contains(expected),
                "missing {expected:?}: {applied}"
            );
        }
        assert!(!applied.contains("\"operation\""));

        let mut proposed = base.clone();
        proposed.phase = "running".to_string();
        assert!(tool_detail_text(&proposed, &state, 80).contains("PROPOSED"));
        let mut failed = base.clone();
        failed.phase = "error".to_string();
        assert!(tool_detail_text(&failed, &state, 80).contains("FAILED"));
        let mut truncated = base.clone();
        truncated.result = Some(file_change_payload(true));
        assert!(tool_detail_text(&truncated, &state, 80).contains("TRUNCATED"));

        let malformed = ToolCallDisplay {
            result: Some("{malformed file result".to_string()),
            ..base
        };
        let fallback = tool_detail_text(&malformed, &state, 80);
        assert!(fallback.contains("{malformed file result"));
        assert!(fallback.contains("args:"));
    }

    #[test]
    fn file_change_rendering_is_equivalent_for_history_and_live_tools() {
        use crate::api::types::{HistoryFunctionCall, HistoryMessage, HistoryToolCall};
        use crate::history::map_history;

        let payload = file_change_payload(false);
        let mapped = map_history(vec![
            HistoryMessage {
                id: "assistant-history".to_string(),
                role: "assistant".to_string(),
                tool_calls: Some(vec![HistoryToolCall {
                    id: "change-1".to_string(),
                    function: HistoryFunctionCall {
                        name: "Edit".to_string(),
                        arguments: "{}".to_string(),
                    },
                }]),
                ..Default::default()
            },
            HistoryMessage {
                role: "tool".to_string(),
                content: payload.clone(),
                tool_call_id: Some("change-1".to_string()),
                tool_success: Some(true),
                ..Default::default()
            },
        ]);
        let history = &mapped[0].tool_calls[0];
        let live = ToolCallDisplay {
            id: "change-1".to_string(),
            tool_name: "Edit".to_string(),
            arguments: "{}".to_string(),
            result: Some(payload),
            stream_output: String::new(),
            error: None,
            phase: "complete".to_string(),
        };
        let state = ConversationBlockUiState {
            expanded: true,
            ..Default::default()
        };
        assert_eq!(
            tool_detail_text(history, &state, 80),
            tool_detail_text(&live, &state, 80)
        );
    }

    #[test]
    fn monochrome_diff_rows_stay_textually_distinct_at_supported_widths() {
        let tool = ToolCallDisplay {
            id: "change".to_string(),
            tool_name: "Edit".to_string(),
            arguments: "{}".to_string(),
            result: Some(file_change_payload(false)),
            stream_output: String::new(),
            error: None,
            phase: "complete".to_string(),
        };
        let state = ConversationBlockUiState {
            expanded: true,
            ..Default::default()
        };
        crate::theme::with_palette(crate::theme::ThemePalette::NoColor, || {
            for width in [60, 80, 120] {
                let rendered = tool_detail_text(&tool, &state, width);
                assert!(rendered.contains("- old"));
                assert!(rendered.contains("+ new"));
                assert!(rendered
                    .lines()
                    .all(|line| crate::text::display_width(line) <= width as usize));
            }
        });
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
            image_count: 0,
            terminal_status: Some("completed".to_string()),
        });
        app.chat.block_ui.insert(
            "message-1:tool:call-1".to_string(),
            ConversationBlockUiState {
                expanded: true,
                scroll: 5,
                diff_wrap: true,
            },
        );
        app.chat.block_ui.insert(
            "message-1:reasoning".to_string(),
            ConversationBlockUiState::default(),
        );
        app.chat.focused_block = Some("message-1:tool:call-1".to_string());

        let rendered = build_conversation_lines(&app, 100, false);
        let text = rendered_text(&rendered);
        assert!(text.contains("25 reasoning lines hidden"));
        assert!(!text.contains("reason-0"));
        assert!(text.contains("result-5"));
        assert!(text.contains("result-14"));
        assert!(!text.contains("result-4"));
        assert!(!text.contains("result-15"));
        assert!(text.contains("15 later lines"));
        assert!(text.contains("research (completed) · child-123456789"));
        let expected_hint = format!(
            "{} collapse · {}/{} scroll · {} copy",
            app.key_hint(ActionContext::ConversationBlock, ActionId::Activate),
            app.key_hint(ActionContext::ConversationBlock, ActionId::ScrollBlockUp),
            app.key_hint(ActionContext::ConversationBlock, ActionId::ScrollBlockDown),
            app.key_hint(ActionContext::ConversationBlock, ActionId::CopyValue),
        );
        assert!(text.contains(&expected_hint));
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
            image_count: 0,
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
                    diff_wrap: true,
                },
            );
        }

        let width = 20;
        let rendered = build_conversation_lines(&app, width, false);
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

    fn transcript_rows(
        terminal: &mut Terminal<TestBackend>,
        app: &App,
        workspace: bool,
    ) -> Vec<String> {
        terminal
            .draw(|frame| render_transcript(frame, frame.area(), app, workspace))
            .unwrap();
        let buffer = terminal.backend().buffer();
        buffer
            .content()
            .chunks(usize::from(buffer.area().width))
            .map(|row| row.iter().map(|cell| cell.symbol()).collect())
            .collect()
    }

    fn visible_transcript_row(terminal: &Terminal<TestBackend>, row: usize) -> String {
        // A wide glyph covers its continuation cell even when TestBackend
        // retains a symbol there from the previous draw.
        let buffer = terminal.backend().buffer();
        let width = usize::from(buffer.area().width);
        let cells = &buffer.content()[row * width..(row + 1) * width];
        let mut text = String::new();
        let mut column = 0;
        while let Some(cell) = cells.get(column) {
            let symbol = cell.symbol();
            text.push_str(symbol);
            column += crate::text::grapheme_cell_width(symbol).max(1);
        }
        text
    }

    fn fence_rows<'a>(rows: &'a [String], language: &str) -> &'a [String] {
        let top = rows
            .iter()
            .position(|row| {
                row.starts_with('┌')
                    && row.ends_with('┐')
                    && row.trim_matches(['┌', '┐', '─', ' ']) == language
            })
            .unwrap_or_else(|| panic!("missing {language} fence:\n{}", rows.join("\n")));
        let bottom = top
            + 1
            + rows[top + 1..]
                .iter()
                .position(|row| row.starts_with('└') && row.ends_with('┘'))
                .expect("fence closes within the transcript width");
        let fence = &rows[top..=bottom];
        assert!(fence[1..fence.len() - 1]
            .iter()
            .all(|row| row.starts_with('│') && row.ends_with('│')));
        fence
    }

    #[test]
    fn testbackend_renders_complete_assistant_fences_and_resumes_prose_at_each_width() {
        let source = concat!(
            "Before the examples.\n\n",
            "```rust linenos\nlet label = \"中文 🧭\";\n```\n\n",
            "Between the examples.\n\n",
            "~~~~mermaid theme=neutral\ngraph TD\n  A[图 🧪] --> B[完成]\n~~~~\n\n",
            "After the examples.\n\n",
            "Metric | Value\n--- | ---\nFence | Ready\n",
        );
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.messages = map_history(vec![HistoryMessage {
            id: "assistant-with-fences".into(),
            role: "assistant".into(),
            content: source.into(),
            ..Default::default()
        }]);

        for width in [60, 80, 120] {
            let mut terminal = Terminal::new(TestBackend::new(width, 24)).unwrap();
            let rows = transcript_rows(&mut terminal, &app, false);
            if width == 60 && std::env::var_os("BAMBOO_TUI_CAPTURE_MARKDOWN_FENCES").is_some() {
                eprintln!(
                    "MARKDOWN FENCE PREVIEW\n{}\nEND MARKDOWN FENCE PREVIEW",
                    rows.join("\n")
                );
            }
            let rust = fence_rows(&rows, "rust");
            assert_eq!(rust.len(), 3);
            // Wide graphemes reserve continuation cells in the TestBackend buffer.
            assert!(rust[1].replace(' ', "").contains("letlabel=\"中文🧭\";"));
            let mermaid = fence_rows(&rows, "mermaid");
            assert_eq!(mermaid.len(), 4);
            assert!(mermaid[1].contains("graph TD"));
            assert!(mermaid[2].replace(' ', "").contains("A[图🧪]-->B[完成]"));

            let row_for = |text: &str| rows.iter().position(|row| row.contains(text)).unwrap();
            assert!(row_for("Before the examples.") < row_for("let label"));
            assert!(row_for("let label") < row_for("Between the examples."));
            assert!(row_for("Between the examples.") < row_for("graph TD"));
            assert!(row_for("graph TD") < row_for("After the examples."));
            let table = &rows[row_for("Metric")];
            assert!(table.contains("Value") && !table.contains('|'));
            assert!(rows[row_for("Ready")].contains("Fence"));
            assert_eq!(app.chat.messages[0].content, source);
        }
    }

    #[test]
    fn testbackend_keeps_streaming_assistant_fences_stable_when_they_close() {
        for (marker, language, code) in [
            ("```", "rust", "let label = \"输入 🧭\";"),
            ("~~~", "mermaid", "A[输入 🧭] --> B[完成]"),
        ] {
            let source = format!("Leading prose.\n\n{marker}{language}\n{code}");
            let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
            app.chat.streaming = true;
            app.chat.current_turn_id = Some("run:preview".into());
            for width in [60, 80, 120] {
                let mut terminal = Terminal::new(TestBackend::new(width, 24)).unwrap();
                app.chat.current_response = source.clone();
                let open_rows = transcript_rows(&mut terminal, &app, true);
                let open = fence_rows(&open_rows, language);
                assert_eq!(open.len(), 3);
                assert!(open[1].replace(' ', "").contains("输入🧭"));
                assert!(open_rows
                    .iter()
                    .any(|row| row.contains("Bamboo · streaming")));

                let closing_delta = format!("\n{marker}\n\nProse after the fence.");
                app.chat.current_response.push_str(&closing_delta);
                let closed_rows = transcript_rows(&mut terminal, &app, true);
                assert_eq!(fence_rows(&closed_rows, language), open);
                assert!(closed_rows
                    .iter()
                    .any(|row| row.starts_with("Prose after the fence.")));
                let blocks = app.conversation_blocks();
                assert_eq!(blocks[0].id, "run:preview:assistant");
                assert!(matches!(
                    blocks[0].kind,
                    ConversationBlockKind::AssistantMarkdown {
                        streaming: true,
                        ..
                    }
                ));
            }
        }
    }

    fn assistant_transcript(source: &str) -> App {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.messages = map_history(vec![HistoryMessage {
            id: "assistant-mermaid".into(),
            role: "assistant".into(),
            content: source.into(),
            ..Default::default()
        }]);
        app
    }

    #[test]
    fn testbackend_renders_settled_mermaid_td_paths_at_each_width() {
        for (fence, header, edges, labels) in [
            (
                "```",
                "flowchart TD",
                "A[开始 🧪] --> B[验证]\nB --> C[完成 🧭]",
                &["开始🧪", "验证", "完成🧭"][..],
            ),
            (
                "~~~",
                "graph TD",
                "A[Start] --> B[Finish]",
                &["Start", "Finish"][..],
            ),
        ] {
            let source = format!(
                "Before the diagram.\n\n{fence}mermaid\n{header}\n{edges}\n{fence}\n\nAfter the diagram."
            );
            let app = assistant_transcript(&source);
            for width in [60, 80, 120] {
                let mut terminal = Terminal::new(TestBackend::new(width, 24)).unwrap();
                let rows = transcript_rows(&mut terminal, &app, false);
                if width == 60
                    && header == "flowchart TD"
                    && std::env::var_os("BAMBOO_TUI_CAPTURE_MERMAID_TD").is_some()
                {
                    eprintln!(
                        "MERMAID TD PREVIEW\n{}\nEND MERMAID TD PREVIEW",
                        rows.join("\n")
                    );
                }
                let row_for = |text: &str| rows.iter().position(|row| row.contains(text)).unwrap();
                let first = row_for("┌");
                assert!(row_for("Before the diagram.") < first);
                assert!(row_for("After the diagram.") > first + labels.len() * 4 - 2);
                assert!(!rows
                    .iter()
                    .any(|row| row.contains(header) || row.contains("-->")));

                let buffer = terminal.backend().buffer();
                let width = usize::from(width);
                let cell_row = |row| &buffer.content()[row * width..(row + 1) * width];
                let first_top = cell_row(first);
                let left = first_top
                    .iter()
                    .position(|cell| cell.symbol() == "┌")
                    .unwrap();
                let right = first_top
                    .iter()
                    .position(|cell| cell.symbol() == "┐")
                    .unwrap();
                let center = cell_row(first + 2)
                    .iter()
                    .position(|cell| cell.symbol() == "┬")
                    .unwrap();
                assert!((left..=right).contains(&center));
                assert!((center - left).abs_diff(right - center) <= 1);

                for (index, label) in labels.iter().enumerate() {
                    let top = first + index * 4;
                    let joins_previous = index != 0;
                    let joins_next = index + 1 != labels.len();
                    assert_eq!(cell_row(top)[left].symbol(), "┌");
                    assert_eq!(cell_row(top)[right].symbol(), "┐");
                    assert_eq!(
                        cell_row(top)[center].symbol(),
                        if joins_previous { "┴" } else { "─" }
                    );
                    let contents: String = cell_row(top + 1)[left..=right]
                        .iter()
                        .map(|cell| cell.symbol())
                        .collect();
                    assert_eq!(contents.replace(' ', ""), format!("│{label}│"));
                    assert_eq!(cell_row(top + 2)[left].symbol(), "└");
                    assert_eq!(cell_row(top + 2)[right].symbol(), "┘");
                    assert_eq!(
                        cell_row(top + 2)[center].symbol(),
                        if joins_next { "┬" } else { "─" }
                    );
                    if joins_next {
                        assert_eq!(cell_row(top + 3)[center].symbol(), "↓");
                        assert_eq!(rows[top + 3].trim(), "↓");
                    }
                }
                assert_eq!(app.chat.messages[0].content, source);
            }
        }
    }

    #[test]
    fn testbackend_keeps_mermaid_source_visible_until_the_message_settles() {
        let prefix = "Before the diagram.\n\n```mermaid\nflowchart TD\nA[Start] --> B[Finish]\n";
        let partial = format!("{prefix}``");
        let eof = format!("{prefix}```");
        let closed = format!("{prefix}```\r\n\nAfter the diagram.");
        let invalid = format!("{prefix}``` suffix");
        let mut live = App::new(BambooClient::new("http://127.0.0.1:0"));
        live.chat.streaming = true;
        live.chat.current_turn_id = Some("run:mermaid:preview".into());
        let saved = assistant_transcript(&closed);
        let saved_at_eof = assistant_transcript(&eof);
        let saved_invalid = assistant_transcript(&invalid);

        for width in [60, 80, 120] {
            let draw = |app: &App| {
                let mut terminal = Terminal::new(TestBackend::new(width, 18)).unwrap();
                transcript_rows(&mut terminal, app, true)
            };
            for (source, literal_closer) in [
                (prefix, None),
                (&partial, Some("``")),
                (&eof, None),
                (&invalid, Some("``` suffix")),
                (&closed, None),
            ] {
                live.chat.current_response = source.into();
                let rows = draw(&live);
                let body = fence_rows(&rows, "mermaid");
                assert!(body[1].contains("flowchart TD"));
                assert!(body[2].contains("A[Start] --> B[Finish]"));
                assert!(!rows
                    .iter()
                    .any(|row| row.contains("↓") || row.contains("Mermaid source")));
                if let Some(closer) = literal_closer {
                    assert!(body.iter().any(|row| row.contains(closer)));
                }
                assert_eq!(live.chat.current_response, source);
            }
            let rows = draw(&saved_invalid);
            assert!(fence_rows(&rows, "mermaid")
                .iter()
                .any(|row| row.contains("``` suffix")));
            assert!(!rows
                .iter()
                .any(|row| row.contains('↓') || row.contains("Mermaid source")));
            for app in [&saved, &saved_at_eof] {
                let rows = draw(app);
                assert!(rows.iter().any(|row| row.trim() == "↓"));
                assert!(rows
                    .iter()
                    .any(|row| row.replace(' ', "").contains("│Start│")));
                assert!(rows
                    .iter()
                    .any(|row| row.replace(' ', "").contains("│Finish│")));
                assert!(!rows.iter().any(|row| row.contains("flowchart TD")));
            }
            assert!(draw(&saved)
                .iter()
                .any(|row| row.contains("After the diagram.")));
        }
        assert_eq!(saved.chat.messages[0].content, closed);
    }

    #[test]
    fn testbackend_keeps_unsupported_and_wide_mermaid_source_readable() {
        let unsupported = assistant_transcript(concat!(
            "```mermaid\nflowchart TD\nA[Start] --> B[Finish]\nA --> C[Other]\n```\n\nAfter the diagram."
        ));
        let wide = assistant_transcript(&format!(
            "```mermaid\nflowchart TD\nA[{}] --> B[Finish]\n```\n\nAfter the diagram.",
            "W".repeat(40)
        ));
        for (width, app, explanation, source) in [
            (
                60,
                &unsupported,
                "Mermaid source · unsupported diagram",
                "A --> C[Other]",
            ),
            (38, &wide, "Mermaid source · diagram too wide", "A[WWWW"),
        ] {
            let mut terminal = Terminal::new(TestBackend::new(width, 18)).unwrap();
            let rows = transcript_rows(&mut terminal, app, false);
            assert!(rows.iter().any(|row| row.trim() == explanation));
            let body = fence_rows(&rows, "mermaid");
            assert!(body.iter().any(|row| row.contains("flowchart TD")));
            assert!(body.iter().any(|row| row.contains(source)));
            assert!(rows.iter().any(|row| row.contains("After the diagram.")));
            assert!(!rows.iter().any(|row| row.contains('↓')));
        }
    }

    const MERMAID_LR_LABELS: [&str; 3] = ["开始 🧪", "Review report", "完成 🧭"];

    fn mermaid_lr_source(fence: &str, header: &str) -> String {
        let [first, middle, final_label] = MERMAID_LR_LABELS;
        format!(
            "Before the diagram.\n\n{fence}mermaid\n{header}\nB[{middle}] --> C[{final_label}]\nA[{first}] --> B\n{fence}\n\nAfter the diagram."
        )
    }

    fn assert_mermaid_lr_path(rows: &[String]) {
        let rendered = rows.join("\n");
        let row_for = |text: &str| {
            rows.iter()
                .position(|row| row.contains(text))
                .unwrap_or_else(|| panic!("missing {text:?} in transcript\n{rendered}"))
        };
        let first = row_for("┌");
        assert!(row_for("Before the diagram.") < first);
        assert!(row_for("After the diagram.") > first + 2);
        assert!(!rendered.contains("Mermaid source"));
        assert!(!rendered.contains(" LR") && !rendered.contains("-->"));

        let border = |left, right| {
            MERMAID_LR_LABELS
                .map(|label| {
                    let rule = "─".repeat(crate::text::grapheme_cell_width(label) + 2);
                    format!("{left}{rule}{right}")
                })
                .join("   ")
        };
        assert_eq!(rows[first].trim(), border('┌', '┐'));
        assert_eq!(
            rows[first + 1].replace(' ', ""),
            "│开始🧪├──→┤Reviewreport├──→┤完成🧭│"
        );
        assert_eq!(rows[first + 2].trim(), border('└', '┘'));
    }

    #[test]
    fn testbackend_renders_settled_mermaid_lr_paths_at_each_width() {
        for (fence, header) in [("```", "flowchart LR"), ("~~~", "graph LR")] {
            let source = mermaid_lr_source(fence, header);
            let app = assistant_transcript(&source);
            for width in [60, 80, 120] {
                let mut terminal = Terminal::new(TestBackend::new(width, 18)).unwrap();
                let rows = transcript_rows(&mut terminal, &app, false);
                assert_mermaid_lr_path(&rows);
                assert_eq!(app.chat.messages[0].content, source);
                if width == 60
                    && header == "flowchart LR"
                    && std::env::var_os("BAMBOO_TUI_CAPTURE_MERMAID_LR").is_some()
                {
                    eprintln!(
                        "MERMAID LR PREVIEW\n{}\nEND MERMAID LR PREVIEW",
                        rows.join("\n")
                    );
                }
            }
        }
    }

    #[test]
    fn testbackend_keeps_mermaid_lr_source_live_and_redraws_settled_graph_after_resize() {
        let source = mermaid_lr_source("```", "flowchart LR");
        let mut live = App::new(BambooClient::new("http://127.0.0.1:0"));
        live.chat.current_turn_id = Some("run:mermaid-lr".into());
        live.chat.streaming = true;
        live.chat.current_response = source.clone();
        let mut live_terminal = Terminal::new(TestBackend::new(80, 18)).unwrap();
        let live_rows = transcript_rows(&mut live_terminal, &live, true);
        let live_fence = fence_rows(&live_rows, "mermaid");
        assert!(live_fence[1].contains("flowchart LR"));
        assert!(live_fence[3].replace(' ', "").contains("A[开始🧪]-->B"));
        assert!(!live_rows
            .iter()
            .any(|row| row.contains('→') || row.contains("Mermaid source")));
        assert!(live_rows
            .iter()
            .any(|row| row.contains("After the diagram.")));
        assert_eq!(live.chat.current_response, source);

        let box_cells: usize = MERMAID_LR_LABELS
            .iter()
            .map(|label| crate::text::grapheme_cell_width(label) + 4)
            .sum();
        let required_width = box_cells + (MERMAID_LR_LABELS.len() - 1) * 3;
        let narrow_width = u16::try_from(required_width - 1).unwrap();
        let saved = assistant_transcript(&source);
        let mut terminal = Terminal::new(TestBackend::new(narrow_width, 18)).unwrap();
        let narrow = transcript_rows(&mut terminal, &saved, false);
        assert!(narrow
            .iter()
            .any(|row| row.trim() == "Mermaid source · diagram too wide"));
        assert!(fence_rows(&narrow, "mermaid")[1].contains("flowchart LR"));
        assert!(!narrow.iter().any(|row| row.contains('→')));
        terminal.backend_mut().resize(80, 18);
        let wide = transcript_rows(&mut terminal, &saved, false);
        assert_mermaid_lr_path(&wide);
        assert_eq!(saved.chat.messages[0].content, source);
    }

    #[test]
    fn testbackend_previews_rounded_mermaid_nodes_only_after_settling() {
        let edges = "A(\"计划 🧭\") --> B[Review]\r\nB --> C(Done)\r\n";
        for (direction, connector) in [("TD", '↓'), ("LR", '→')] {
            let body = format!("flowchart {direction}\r\n{edges}");
            let source = format!(
                "Before the diagram.\r\n\r\n~~~mermaid\r\n{body}~~~\r\n\r\nAfter the diagram."
            );
            let mut live = App::new(BambooClient::new("http://127.0.0.1:0"));
            live.chat.streaming = true;
            live.chat.current_turn_id = Some("run:mermaid-rounded".into());
            live.chat.current_response = source.clone();
            let mut terminal = Terminal::new(TestBackend::new(60, 24)).unwrap();
            let rows = transcript_rows(&mut terminal, &live, true);
            let fence = fence_rows(&rows, "mermaid");
            assert!(fence[1].contains(direction));
            assert!(fence[2]
                .replace(' ', "")
                .contains("A(\"计划🧭\")-->B[Review]"));
            assert!(!rows
                .iter()
                .any(|row| row.contains(connector) || row.contains('╭')));

            let saved = assistant_transcript(&source);
            if direction == "LR" {
                terminal.backend_mut().resize(34, 24);
                let narrow = transcript_rows(&mut terminal, &saved, false);
                assert!(narrow
                    .iter()
                    .any(|row| row.trim() == "Mermaid source · diagram too wide"));
                assert!(fence_rows(&narrow, "mermaid")[1].contains("flowchart LR"));
                assert!(!narrow
                    .iter()
                    .any(|row| row.contains(connector) || row.contains('╭')));
            }

            for width in [60, 120] {
                terminal.backend_mut().resize(width, 24);
                let mut rows = transcript_rows(&mut terminal, &saved, false);
                let top = rows
                    .iter()
                    .position(|row| row.starts_with('╭'))
                    .unwrap_or_else(|| {
                        panic!("missing rounded diagram at {width}:\n{}", rows.join("\n"))
                    });
                rows[top + 1] = visible_transcript_row(&terminal, top + 1);
                let rendered = rows.join("\n");
                if direction == "TD" {
                    assert_eq!(rows[top].trim_end(), "╭─────────╮");
                    assert_eq!(rows[top + 1].replace(' ', ""), "│计划🧭│");
                    assert_eq!(rows[top + 2].trim_end(), "╰────┬────╯");
                    assert_eq!(rows[top + 3].trim_end(), "     ↓");
                    assert_eq!(rows[top + 4].trim_end(), "┌────┴────┐");
                    assert_eq!(rows[top + 8].trim_end(), "╭────┴────╮");
                    assert_eq!(rows[top + 10].trim_end(), "╰─────────╯");
                } else {
                    assert_eq!(rows[top].trim_end(), "╭─────────╮   ┌────────┐   ╭──────╮");
                    assert_eq!(
                        rows[top + 1].replace(' ', ""),
                        "│计划🧭├──→┤Review├──→┤Done│"
                    );
                    assert_eq!(
                        rows[top + 2].trim_end(),
                        "╰─────────╯   └────────┘   ╰──────╯"
                    );
                }
                assert!(rendered.contains("After the diagram."));
                assert!(!rendered.contains("Mermaid source") && !rendered.contains("-->"));
                if width == 60 && std::env::var_os("BAMBOO_TUI_CAPTURE_MERMAID_ROUNDED").is_some() {
                    eprintln!(
                        "MERMAID ROUNDED {direction} PREVIEW\n{rendered}\nEND MERMAID ROUNDED PREVIEW"
                    );
                }
            }

            assert_eq!(saved.chat.messages[0].content, source);
            let (_, fences) = markdown::copyable_fences(&saved.chat.messages[0].content);
            assert_eq!(
                fences,
                [markdown::CopyableFence {
                    language: Some("mermaid"),
                    source: &body,
                }]
            );
        }
    }

    #[test]
    fn testbackend_previews_mermaid_edge_labels_only_after_settling_and_when_they_fit() {
        const LABEL: &str = "需要复核 🧭";
        for (direction, connector) in [("TD", '↓'), ("LR", '→')] {
            let body = format!(
                "flowchart {direction}\r\nA(Collect) -->|{LABEL}| B[Review]\r\nB --> C(Done)\r\n"
            );
            let source = format!(
                "Before the diagram.\r\n\r\n~~~mermaid\r\n{body}~~~\r\n\r\nAfter the diagram."
            );
            let mut live = App::new(BambooClient::new("http://127.0.0.1:0"));
            live.chat.streaming = true;
            live.chat.current_turn_id = Some("run:mermaid-edge-label".into());
            live.chat.current_response = source.clone();
            let mut terminal = Terminal::new(TestBackend::new(60, 24)).unwrap();
            let streaming = transcript_rows(&mut terminal, &live, true);
            let fence = fence_rows(&streaming, "mermaid");
            assert!(fence[2]
                .replace(' ', "")
                .contains("A(Collect)-->|需要复核🧭|B[Review]"));
            assert!(!streaming.iter().any(|row| row.contains(connector)));

            let saved = assistant_transcript(&source);
            if direction == "LR" {
                terminal.backend_mut().resize(38, 24);
                let narrow = transcript_rows(&mut terminal, &saved, false);
                assert!(narrow
                    .iter()
                    .any(|row| row.trim() == "Mermaid source · diagram too wide"));
                assert!(fence_rows(&narrow, "mermaid")[1].contains("flowchart LR"));
                assert!(!narrow.iter().any(|row| row.contains(connector)));
            }

            for width in [60, 120] {
                terminal.backend_mut().resize(width, 24);
                let mut rows = transcript_rows(&mut terminal, &saved, false);
                let top = rows
                    .iter()
                    .position(|row| row.starts_with('╭'))
                    .unwrap_or_else(|| {
                        panic!(
                            "missing labeled {direction} at {width}:\n{}",
                            rows.join("\n")
                        )
                    });
                let label_row = if direction == "TD" { top + 3 } else { top };
                rows[label_row] = visible_transcript_row(&terminal, label_row);
                if direction == "TD" {
                    assert_eq!(rows[label_row].trim(), format!("↓ {LABEL}"));
                    assert!(rows[top + 4].starts_with('┌'));
                    assert_eq!(rows[top + 7].trim(), "↓");
                } else {
                    assert!(rows[label_row].contains(&format!("╮ {LABEL} ┌")));
                    let dashes = "─".repeat(crate::text::grapheme_cell_width(LABEL) + 1);
                    assert!(rows[top + 1].contains(&format!("├{dashes}→┤ Review ├──→┤")));
                }
                let rendered = rows.join("\n");
                assert!(rendered.contains("After the diagram."));
                assert!(!rendered.contains("Mermaid source") && !rendered.contains("-->"));
                if width == 60
                    && std::env::var_os("BAMBOO_TUI_CAPTURE_MERMAID_EDGE_LABEL").is_some()
                {
                    eprintln!(
                        "MERMAID EDGE {direction} PREVIEW\n{rendered}\nEND MERMAID EDGE PREVIEW"
                    );
                }
            }
            assert_eq!(saved.chat.messages[0].content, source);
            assert_eq!(
                markdown::copyable_fences(&saved.chat.messages[0].content).1,
                [markdown::CopyableFence {
                    language: Some("mermaid"),
                    source: &body,
                }]
            );
        }
    }

    #[test]
    fn testbackend_renders_assistant_checklist_and_neighboring_markdown_at_each_width() {
        let source = concat!(
            "# Launch checklist\n\n",
            "-  [ ] Verify **接口** 🧪\n",
            "  *\t[x] Build **组件** 🧭\n",
            "+ [x] Ship sample\n",
            "7)   [X] Review `日志` ✅\n",
            "- Plain bullet\n",
            "- [x]no marker boundary\n",
            "- [x](https://example.test/check) remains a link.\n",
            "Read [release notes](https://example.test/notes).\n\n",
            "```text\n- [x] literal inside fence\n```\n",
        );
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.messages = map_history(vec![HistoryMessage {
            id: "assistant-checklist".into(),
            role: "assistant".into(),
            content: source.into(),
            ..Default::default()
        }]);

        for width in [60, 80, 120] {
            let mut terminal = Terminal::new(TestBackend::new(width, 16)).unwrap();
            let rows = transcript_rows(&mut terminal, &app, false);
            if width == 60 && std::env::var_os("BAMBOO_TUI_CAPTURE_MARKDOWN_CHECKLIST").is_some() {
                eprintln!(
                    "MARKDOWN CHECKLIST PREVIEW\n{}\nEND MARKDOWN CHECKLIST PREVIEW",
                    rows.join("\n")
                );
            }

            let row_for = |text: &str| {
                rows.iter()
                    .find(|row| row.contains(text))
                    .unwrap_or_else(|| {
                        panic!("missing {text:?} at width {width}:\n{}", rows.join("\n"))
                    })
            };
            let pending = row_for("Verify");
            assert!(pending.contains("☐ Verify "));
            assert!(pending.replace(' ', "").contains("接口🧪"));
            let nested = row_for("Build");
            assert!(nested.contains("☑ Build "));
            assert!(nested.find('☑').unwrap() > pending.find('☐').unwrap());
            assert!(nested.replace(' ', "").contains("组件🧭"));
            assert!(row_for("Ship sample").contains("☑ Ship sample"));
            let ordered = row_for("Review");
            assert!(ordered.contains("7) ☑ Review "));
            assert!(ordered.replace(' ', "").contains("日志✅"));

            assert!(row_for("Plain bullet").contains("• Plain bullet"));
            assert!(row_for("no marker boundary").contains("• [x]no marker boundary"));
            assert!(row_for("remains a link").contains("• x (https://example.test/check)"));
            assert!(row_for("release notes").contains("release notes (https://example.test/notes)"));
            assert!(fence_rows(&rows, "text")[1].contains("- [x] literal inside fence"));
            assert!(terminal
                .backend()
                .buffer()
                .content()
                .iter()
                .any(|cell| { cell.symbol() == "接" && cell.modifier.contains(Modifier::BOLD) }));
            assert_eq!(app.chat.messages[0].content, source);
        }
    }

    #[test]
    fn testbackend_renders_checkboxes_only_after_streamed_markers_are_complete() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.streaming = true;
        app.chat.current_turn_id = Some("run:checklist".into());
        for width in [60, 80, 120] {
            let mut terminal = Terminal::new(TestBackend::new(width, 16)).unwrap();
            app.chat.current_response = "Progress\n\n- [x".into();
            let partial = transcript_rows(&mut terminal, &app, true);
            assert!(partial.iter().any(|row| row.contains("• [x")));
            assert!(partial
                .iter()
                .all(|row| !row.contains('☑') && !row.contains('☐')));

            app.chat
                .current_response
                .push_str("] Verify 输入 🧭\n  - [ ");
            let checked = transcript_rows(&mut terminal, &app, true);
            let checked_row = checked.iter().find(|row| row.contains("☑ Verify")).unwrap();
            assert!(checked_row.replace(' ', "").contains("输入🧭"));
            assert!(checked.iter().any(|row| row.trim() == "• ["));
            assert!(checked.iter().all(|row| !row.contains('☐')));

            app.chat.current_response.push_str("] Document 输出 🧪");
            let complete = transcript_rows(&mut terminal, &app, true);
            assert!(complete.contains(checked_row));
            let new_row = complete
                .iter()
                .find(|row| row.contains("☐ Document"))
                .unwrap();
            assert!(new_row.replace(' ', "").contains("输出🧪"));
            assert!(complete
                .iter()
                .any(|row| row.contains("Bamboo · streaming")));
        }
    }

    #[test]
    fn testbackend_renders_thematic_breaks_among_assistant_markdown_at_each_width() {
        let source = concat!(
            "Delivery notes for **输入** 🧭 and _output_.\n\n",
            "---\n",
            "- Real bullet\n",
            "2. Real ordered item\n\n",
            "***\n",
            "Examples follow.\n\n",
            "___\n",
            "Setext heading\n",
            "---\n\n",
            "- - -\n",
            "* * *\n",
            "_ _ _\n",
            "Metric | Value\n",
            "--- | ---\n",
            "Table | Ready\n",
            "***\n",
            "Prepared 输出 🧪\n\n",
            "```text\n---\n* * *\n___\n```\n",
            "After the fenced text.\n",
        );
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.messages = map_history(vec![HistoryMessage {
            id: "assistant-thematic-breaks".into(),
            role: "assistant".into(),
            content: source.into(),
            ..Default::default()
        }]);

        for width in [60, 80, 120] {
            let mut terminal = Terminal::new(TestBackend::new(width, 30)).unwrap();
            let rows = transcript_rows(&mut terminal, &app, false);
            let rendered = rows.join("\n");
            if width == 60 && std::env::var_os("BAMBOO_TUI_CAPTURE_THEMATIC_BREAKS").is_some() {
                eprintln!("THEMATIC BREAK PREVIEW\n{rendered}\nEND THEMATIC BREAK PREVIEW");
            }

            let divider = "─".repeat(usize::from(width));
            let divider_rows: Vec<_> = rows
                .iter()
                .enumerate()
                .filter_map(|(index, row)| (row == &divider).then_some(index))
                .collect();
            assert_eq!(divider_rows.len(), 7, "{width} columns\n{rendered}");
            let row_for = |text: &str| {
                rows.iter()
                    .position(|row| row.contains(text))
                    .unwrap_or_else(|| panic!("missing {text:?} at {width} columns\n{rendered}"))
            };
            let prose = row_for("Delivery notes");
            assert!(rows[prose].replace(' ', "").contains("输入🧭"));
            assert!(rows[prose].contains("and output."));
            assert!(prose < divider_rows[0]);
            assert!(rows[row_for("Real bullet")].contains("• Real bullet"));
            assert!(rows[row_for("Real ordered item")].contains("2. Real ordered item"));
            let heading = row_for("Setext heading");
            assert!(rows[heading + 1].trim().is_empty());
            assert!(divider_rows[2] < heading && heading < divider_rows[3]);

            let table_header = row_for("Metric");
            let table_body = row_for("Ready");
            assert!(rows[table_header].contains("Value") && !rows[table_header].contains('|'));
            let table_separator = rows[table_header + 1].trim_end();
            assert!(table_separator.contains('─') && table_separator.contains(' '));
            assert!(table_separator
                .chars()
                .all(|cell| cell == '─' || cell == ' '));
            assert_eq!(table_body, table_header + 2);
            assert_eq!(divider_rows[6], table_body + 1);
            assert!(rows[table_body + 2].replace(' ', "").contains("输出🧪"));
            let fence = fence_rows(&rows, "text");
            assert_eq!(fence.len(), 5);
            for (row, literal) in fence[1..4].iter().zip(["---", "* * *", "___"]) {
                assert_eq!(row.trim_matches(['│', ' ']), literal);
            }
            assert!(row_for("After the fenced text.") > row_for("Prepared"));
            let cells = terminal.backend().buffer().content();
            assert!(cells
                .iter()
                .any(|cell| cell.symbol() == "入" && cell.modifier.contains(Modifier::BOLD)));
            assert!(cells
                .iter()
                .any(|cell| cell.symbol() == "o" && cell.modifier.contains(Modifier::ITALIC)));
            assert_eq!(app.chat.messages[0].content, source);
        }
    }

    #[test]
    fn testbackend_preserves_atx_heading_style_around_inline_formatting() {
        let source = concat!(
            "# Build **输出** 🧪 with `cargo`\n",
            "## Plan *next* steps\n",
            "### Inspect `trace` details\n",
            "###### Keep *final* checks\n",
            "### Release_v2\n\n",
            "Report the result.\n",
        );
        let mut saved = App::new(BambooClient::new("http://127.0.0.1:0"));
        saved.chat.messages = map_history(vec![HistoryMessage {
            id: "assistant-atx-headings".into(),
            role: "assistant".into(),
            content: source.into(),
            ..Default::default()
        }]);
        let mut live = App::new(BambooClient::new("http://127.0.0.1:0"));
        live.chat.streaming = true;
        live.chat.current_turn_id = Some("run:atx".into());
        live.chat.current_response = source.into();

        for width in [60, 80, 120] {
            let mut terminal = Terminal::new(TestBackend::new(width, 10)).unwrap();
            for (app, streaming) in [(&saved, false), (&live, true)] {
                let rows = transcript_rows(&mut terminal, app, true);
                let rendered = rows.join("\n");
                let row_for = |prefix: &str| {
                    rows.iter()
                        .position(|row| row.starts_with(prefix))
                        .unwrap_or_else(|| {
                            panic!("missing {prefix:?} at {width} columns\n{rendered}")
                        })
                };
                let cell_rows: Vec<_> = terminal
                    .backend()
                    .buffer()
                    .content()
                    .chunks(usize::from(width))
                    .collect();
                let primary = row_for("Build ");
                let secondary = row_for("Plan next steps");
                let mut visible_title = String::new();
                let mut column = 0;
                while let Some(cell) = cell_rows[primary].get(column) {
                    let symbol = cell.symbol();
                    visible_title.push_str(symbol);
                    column += display_width(symbol).max(1);
                }
                assert_eq!(
                    visible_title.trim_end(),
                    "Build 输出 🧪 with cargo",
                    "{width} columns, streaming: {streaming}\n{rendered}"
                );
                for (row, expected_color) in [
                    (primary, colors::brand()),
                    (secondary, app.theme.text()),
                    (row_for("Inspect trace details"), colors::inactive()),
                    (row_for("Keep final checks"), colors::inactive()),
                    (row_for("Release_v2"), colors::inactive()),
                ] {
                    let cell = &cell_rows[row][0];
                    assert_eq!(cell.fg, expected_color, "{width} columns\n{rendered}");
                    assert!(cell.modifier.contains(Modifier::BOLD), "{rendered}");
                }
                assert!(cell_rows[primary].iter().any(|cell| {
                    cell.symbol() == "c"
                        && cell.fg == colors::inactive()
                        && cell.modifier.contains(Modifier::REVERSED)
                }));
                assert!(cell_rows[secondary]
                    .iter()
                    .any(|cell| cell.symbol() == "x" && cell.modifier.contains(Modifier::ITALIC)));
                assert_eq!(rows[row_for("Release_v2")].trim_end(), "Release_v2");
                assert!(matches!(
                    app.conversation_blocks()[0].kind,
                    ConversationBlockKind::AssistantMarkdown { content, streaming: current }
                        if content == source && current == streaming
                ));

                if width == 60
                    && !streaming
                    && std::env::var_os("BAMBOO_TUI_CAPTURE_ATX_HEADING").is_some()
                {
                    let rgb = |color| match color {
                        ratatui::style::Color::Rgb(red, green, blue) => Some([red, green, blue]),
                        _ => None,
                    };
                    let capture: Vec<Vec<_>> = cell_rows
                        .iter()
                        .map(|row| {
                            row.iter()
                                .map(|cell| {
                                    let modifiers = [
                                        Modifier::BOLD,
                                        Modifier::ITALIC,
                                        Modifier::REVERSED,
                                        Modifier::UNDERLINED,
                                    ];
                                    (
                                        cell.symbol(),
                                        rgb(cell.fg),
                                        rgb(cell.bg),
                                        modifiers.map(|modifier| cell.modifier.contains(modifier)),
                                    )
                                })
                                .collect()
                        })
                        .collect();
                    let capture = serde_json::to_string(&capture).unwrap();
                    eprintln!("ATX HEADING CELLS\n{capture}\nEND ATX HEADING CELLS");
                }
            }
        }
    }

    #[test]
    fn testbackend_renders_assistant_setext_headings_at_each_width() {
        let source = concat!(
            "Build **输出** 🧪 and `plan`\n=====\n\n",
            "Next _steps_\n---\n---\n\n",
            "```text\nLiteral title\n====\n---\n```\n\n",
            "Metric | Value\n--- | ---\nBuild | Ready\n***\n",
            "This paragraph has\ntwo lines.\n---\n",
        );
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.messages = map_history(vec![HistoryMessage {
            id: "assistant-setext-headings".into(),
            role: "assistant".into(),
            content: source.into(),
            ..Default::default()
        }]);

        for width in [60, 80, 120] {
            let mut terminal = Terminal::new(TestBackend::new(width, 22)).unwrap();
            let rows = transcript_rows(&mut terminal, &app, true);
            let rendered = rows.join("\n");
            if width == 60 && std::env::var_os("BAMBOO_TUI_CAPTURE_SETEXT_HEADING").is_some() {
                eprintln!("SETEXT HEADING PREVIEW\n{rendered}\nEND SETEXT HEADING PREVIEW");
            }
            let row_for = |prefix: &str| {
                rows.iter()
                    .position(|row| row.starts_with(prefix))
                    .unwrap_or_else(|| panic!("missing {prefix:?} at {width} columns\n{rendered}"))
            };
            let primary = row_for("Build ");
            let secondary = row_for("Next steps");
            let divider = "─".repeat(usize::from(width));
            assert_eq!(rows[primary].replace(' ', ""), "Build输出🧪andplan");
            assert!(rows[primary + 1].trim().is_empty());
            assert_eq!(rows[secondary + 1], divider);

            let cells = terminal.backend().buffer().content();
            let cell_rows = cells.chunks(usize::from(width)).collect::<Vec<_>>();
            assert_eq!(cell_rows[primary][0].fg, colors::brand());
            assert!(cell_rows[primary][0].modifier.contains(Modifier::BOLD));
            assert!(cell_rows[primary]
                .iter()
                .any(|cell| cell.symbol() == "p" && cell.modifier.contains(Modifier::REVERSED)));
            assert!(cell_rows[secondary][0].modifier.contains(Modifier::BOLD));
            assert!(cell_rows[secondary]
                .iter()
                .any(|cell| cell.symbol() == "s" && cell.modifier.contains(Modifier::ITALIC)));

            let fence = fence_rows(&rows, "text");
            assert_eq!(fence[2].trim_matches(['│', ' ']), "====");
            assert_eq!(fence[3].trim_matches(['│', ' ']), "---");
            let table = row_for("Metric");
            assert!(rows[table].contains("Value") && !rows[table].contains('|'));
            assert!(rows[table + 2].contains("Ready"));
            assert_eq!(rows[table + 3], divider);
            assert_eq!(rows[row_for("two lines.") + 1].trim(), "---");
            assert_eq!(app.chat.messages[0].content, source);
        }
    }

    #[test]
    fn testbackend_defers_streamed_setext_until_newline_or_completion() {
        for width in [60, 80, 120] {
            let mut terminal = Terminal::new(TestBackend::new(width, 8)).unwrap();
            for (title, marker) in [("Build outline", "="), ("Next steps", "-")] {
                let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
                app.chat.streaming = true;
                app.chat.current_turn_id = Some("run:setext".into());
                let source = format!("{title}\r\n   {}\t", marker.repeat(3));
                for partial in [format!("{title}\r\n{marker}"), source.clone()] {
                    app.chat.current_response = partial.clone();
                    let rows = transcript_rows(&mut terminal, &app, true);
                    let first = rows.iter().position(|row| row.trim() == title).unwrap();
                    assert_eq!(
                        rows[first + 1].trim(),
                        partial.lines().last().unwrap().trim()
                    );
                    let cell = &terminal.backend().buffer().content()[first * usize::from(width)];
                    assert!(!cell.modifier.contains(Modifier::BOLD));
                    assert_eq!(app.chat.current_response, partial);
                }

                app.chat.current_response.push('\n');
                let rows = transcript_rows(&mut terminal, &app, true);
                let first = rows.iter().position(|row| row.trim() == title).unwrap();
                assert!(rows[first + 1].trim().is_empty());
                let cell = &terminal.backend().buffer().content()[first * usize::from(width)];
                assert!(cell.modifier.contains(Modifier::BOLD));
                assert_eq!(app.chat.current_response, format!("{source}\n"));

                app.chat.current_response.pop();
                let rows = transcript_rows(&mut terminal, &app, true);
                assert_eq!(rows[first + 1].trim(), marker.repeat(3));
                app.chat.streaming = false;
                let rows = transcript_rows(&mut terminal, &app, true);
                assert_eq!(rows[first].trim(), title);
                assert!(rows[first + 1].trim().is_empty());
                let cell = &terminal.backend().buffer().content()[first * usize::from(width)];
                assert!(cell.modifier.contains(Modifier::BOLD));
                assert_eq!(app.chat.current_response, source);
            }
        }
    }

    #[test]
    fn testbackend_keeps_streamed_thematic_break_markers_literal_until_the_third() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.streaming = true;
        app.chat.current_turn_id = Some("run:thematic-breaks".into());
        for width in [60, 80, 120] {
            let mut terminal = Terminal::new(TestBackend::new(width, 10)).unwrap();
            let divider = "─".repeat(usize::from(width));
            for (first, next) in [
                ("-", "-"),
                ("*", "*"),
                ("_", "_"),
                ("-", " -"),
                ("*", " *"),
                ("_", " _"),
            ] {
                app.chat.current_response = format!("Input 输入 🧭\n\n{first}");
                for incomplete in [first.to_owned(), format!("{first}{next}")] {
                    let rows = transcript_rows(&mut terminal, &app, true);
                    assert!(
                        rows.iter().any(|row| row.trim() == incomplete),
                        "missing literal {incomplete:?} at {width} columns\n{}",
                        rows.join("\n")
                    );
                    assert!(
                        !rows.contains(&divider),
                        "unexpected rule for {incomplete:?}"
                    );
                    app.chat.current_response.push_str(next);
                }

                let at_threshold = transcript_rows(&mut terminal, &app, true);
                assert_eq!(
                    at_threshold.iter().filter(|row| *row == &divider).count(),
                    1
                );
                app.chat.current_response.push_str("\nOutput 输出 🧪");
                let continued = transcript_rows(&mut terminal, &app, true);
                let rule = continued.iter().position(|row| row == &divider).unwrap();
                assert_eq!(continued[rule], at_threshold[rule]);
                assert!(continued[..rule]
                    .iter()
                    .any(|row| row.replace(' ', "").contains("输入🧭")));
                assert!(continued[rule + 1].replace(' ', "").contains("输出🧪"));
                assert!(continued
                    .iter()
                    .any(|row| row.contains("Bamboo · streaming")));
                assert_eq!(
                    app.chat.current_response,
                    format!("Input 输入 🧭\n\n{first}{next}{next}\nOutput 输出 🧪")
                );
            }
        }
    }

    #[test]
    fn testbackend_keeps_streamed_break_markers_literal_after_a_pipe_table() {
        const TABLE: &str = "| Name | Status |\n| --- | --- |\n| Alpha | Ready |\n";
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.streaming = true;
        app.chat.current_turn_id = Some("run:table-thematic-break".into());
        for width in [60, 80, 120] {
            let mut terminal = Terminal::new(TestBackend::new(width, 10)).unwrap();
            let divider = "─".repeat(usize::from(width));
            app.chat.current_response = TABLE.into();
            let baseline = transcript_rows(&mut terminal, &app, true);
            let header = baseline
                .iter()
                .position(|row| row.contains("Name"))
                .unwrap();
            let alpha = header + 2;
            assert!(baseline[header].contains("Status"));
            assert!(baseline[alpha].contains("Alpha") && baseline[alpha].contains("Ready"));
            let table = &baseline[header..=alpha];

            for (first, next) in [("*", "*"), ("_", "_"), ("*", " *"), ("_", " _")] {
                app.chat.current_response = format!("{TABLE}{first}");
                for literal in [first.to_owned(), format!("{first}{next}")] {
                    let partial = transcript_rows(&mut terminal, &app, true);
                    assert_eq!(&partial[header..=alpha], table);
                    assert_eq!(
                        partial[alpha + 1].trim(),
                        literal,
                        "{width} columns\n{}",
                        partial.join("\n")
                    );
                    assert!(!partial.contains(&divider));
                    assert_eq!(app.chat.current_response, format!("{TABLE}{literal}"));
                    app.chat.current_response.push_str(next);
                }

                let completed = transcript_rows(&mut terminal, &app, true);
                assert_eq!(&completed[header..=alpha], table);
                assert_eq!(completed[alpha + 1], divider);
                assert_eq!(
                    app.chat.current_response,
                    format!("{TABLE}{first}{next}{next}")
                );
            }
        }
    }

    #[test]
    fn testbackend_renders_assistant_image_alt_text_alongside_links_and_code() {
        let source = concat!(
            "Build report\n\n",
            "![build_route 输出 🧪](assets/run(2).png) · [notes](https://example.test/notes)\n",
            "![See [guide](https://example.test/guide)](assets/chart.png)\n",
            "Missing description: ![](assets/empty.png)\n",
            "Inline example: `![inline](assets/code.png)`\n\n",
            "```md\n![fenced](assets/fence.png)\n```\n",
        );
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.messages = map_history(vec![HistoryMessage {
            id: "assistant-image-alt".into(),
            role: "assistant".into(),
            content: source.into(),
            ..Default::default()
        }]);

        for width in [60, 80, 120] {
            let mut terminal = Terminal::new(TestBackend::new(width, 14)).unwrap();
            let rows = transcript_rows(&mut terminal, &app, false);
            let rendered = rows.join("\n");
            if width == 60 && std::env::var_os("BAMBOO_TUI_CAPTURE_IMAGE_ALT").is_some() {
                eprintln!("IMAGE ALT PREVIEW\n{rendered}\nEND IMAGE ALT PREVIEW");
            }
            let description = rows
                .iter()
                .find(|row| row.contains("build_route"))
                .unwrap_or_else(|| {
                    panic!("missing image description at {width} columns\n{rendered}")
                });
            assert_eq!(
                description.replace(' ', ""),
                "build_route输出🧪·notes(https://example.test/notes)"
            );
            assert!(rows.iter().any(|row| row.trim_end() == "See guide"));
            assert!(rendered.contains("Missing description: [Image]"));
            assert!(!rendered.contains("assets/run(2).png"));
            assert!(!rendered.contains("https://example.test/guide"));
            assert!(!rendered.contains("assets/chart.png"));
            assert!(!rendered.contains("assets/empty.png"));
            assert!(rendered.contains("Inline example: ![inline](assets/code.png)"));
            assert!(fence_rows(&rows, "md")[1].contains("![fenced](assets/fence.png)"));
            assert_eq!(app.chat.messages[0].content, source);
        }
    }

    #[test]
    fn testbackend_keeps_streamed_image_syntax_visible_through_the_outer_close() {
        const INTRO: &str = "Build report [notes](https://example.test/notes)\n\n";
        const ALT: &str = "![build_route 输出 🧪";
        const DESTINATION: &str = "](assets/run(2).png";
        const LINKED_ALT_PARTIAL: &str =
            "![See [guide](https://example.test/guide)](assets/chart.png";
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.streaming = true;
        app.chat.current_turn_id = Some("run:image-alt".into());
        for width in [60, 80, 120] {
            let mut terminal = Terminal::new(TestBackend::new(width, 12)).unwrap();
            let partial_destination = format!("{ALT}{DESTINATION}");
            for literal in [ALT, partial_destination.as_str()] {
                let source = format!("{INTRO}{literal}");
                app.chat.current_response = source.clone();
                let rows = transcript_rows(&mut terminal, &app, true);
                let expected = literal.replace(' ', "");
                assert!(
                    rows.iter().any(|row| row.replace(' ', "") == expected),
                    "missing literal {literal:?} at {width} columns\n{}",
                    rows.join("\n")
                );
                assert!(rows
                    .iter()
                    .any(|row| row.contains("notes (https://example.test/notes)")));
                assert_eq!(app.chat.current_response, source);
            }

            app.chat.current_response.push(')');
            let completed = transcript_rows(&mut terminal, &app, true);
            let description = completed
                .iter()
                .position(|row| row.replace(' ', "") == "build_route输出🧪")
                .unwrap_or_else(|| {
                    panic!(
                        "missing completed image at {width} columns\n{}",
                        completed.join("\n")
                    )
                });
            assert!(completed.iter().all(|row| !row.contains("assets/run")));

            app.chat.current_response.push_str(" ready.");
            let continued = transcript_rows(&mut terminal, &app, true);
            assert_eq!(
                continued[description].replace(' ', ""),
                "build_route输出🧪ready."
            );
            assert!(continued
                .iter()
                .any(|row| row.contains("Bamboo · streaming")));
            assert_eq!(
                app.chat.current_response,
                format!("{INTRO}{ALT}{DESTINATION}) ready.")
            );

            app.chat.current_response = format!("{INTRO}{LINKED_ALT_PARTIAL}");
            let partial_linked_image = transcript_rows(&mut terminal, &app, true);
            assert!(
                partial_linked_image
                    .iter()
                    .any(|row| row.trim_end() == LINKED_ALT_PARTIAL),
                "missing literal linked description at {width} columns\n{}",
                partial_linked_image.join("\n")
            );
            app.chat.current_response.push(')');
            let completed_linked_image = transcript_rows(&mut terminal, &app, true);
            assert!(completed_linked_image
                .iter()
                .any(|row| row.trim_end() == "See guide"));
            assert!(completed_linked_image.iter().all(|row| {
                !row.contains("https://example.test/guide") && !row.contains("assets/chart.png")
            }));
            assert_eq!(
                app.chat.current_response,
                format!("{INTRO}{LINKED_ALT_PARTIAL})")
            );
        }
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
