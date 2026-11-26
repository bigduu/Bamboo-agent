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
