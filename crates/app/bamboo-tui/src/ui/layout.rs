use std::cell::RefCell;

use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use ratatui::Frame;
use unicode_width::UnicodeWidthChar;

use crate::app::{
    App, CommandPaletteEntry, CommandPaletteHitbox, CommandPaletteTrigger, NoticeLevel,
    QuestionOptionHitbox, SessionPickerMode, Tab,
};
use crate::theme::{self, colors};
use crate::ui::sessions::{session_row_line, truncate_cells};

pub struct AppLayout {
    pub content: Rect,
    pub input: Rect,
    pub status_info: Rect,
    pub status_tabs: Rect,
}

pub fn app_layout(area: Rect, app: &App) -> AppLayout {
    let show_input = app.tab == Tab::Chat;

    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints(if show_input {
            vec![
                Constraint::Min(10),   // content
                Constraint::Length(3), // input
                Constraint::Length(1), // status info
                Constraint::Length(1), // status tabs
            ]
        } else {
            vec![
                Constraint::Min(10),   // content
                Constraint::Length(1), // status info
                Constraint::Length(1), // status tabs
            ]
        })
        .split(area);

    if show_input {
        AppLayout {
            content: vertical[0],
            input: vertical[1],
            status_info: vertical[2],
            status_tabs: vertical[3],
        }
    } else {
        AppLayout {
            content: vertical[0],
            input: Rect::default(),
            status_info: vertical[1],
            status_tabs: vertical[2],
        }
    }
}

pub fn render_status_info(f: &mut Frame, area: Rect, app: &App) {
    let mut spans = vec![];

    // Streaming indicator
    if app.chat.streaming {
        let tick = app.spinner_tick % theme::BRAILLE_SPINNER.len();
        let spinner = theme::BRAILLE_SPINNER[tick];
        spans.push(Span::styled(
            format!(" {} Streaming {}", spinner, spinner),
            Style::default().fg(colors::TOOL_RUNNING),
        ));
        spans.push(Span::styled(" · ", Style::default().fg(colors::SUBTLE)));
    }

    // Model
    if !app.chat.model.is_empty() {
        spans.push(Span::styled(
            format!(" {}", app.chat.model),
            Style::default().fg(colors::INACTIVE),
        ));
        spans.push(Span::styled(" · ", Style::default().fg(colors::SUBTLE)));
    }

    // Token usage
    if let Some(usage) = &app.chat.token_usage {
        spans.push(Span::styled(
            format!(" {}/{}", usage.completion_tokens, usage.total_tokens),
            Style::default().fg(colors::INACTIVE),
        ));
        spans.push(Span::styled(" · ", Style::default().fg(colors::SUBTLE)));
    }

    // Session
    if let Some(sid) = &app.chat.session_id {
        let short: String = sid.chars().take(8).collect();
        spans.push(Span::styled(
            format!(" {}...", short),
            Style::default().fg(colors::INACTIVE),
        ));
        spans.push(Span::styled(" · ", Style::default().fg(colors::SUBTLE)));
    }

    // Plan mode indicator
    if app.chat.plan_mode {
        spans.push(Span::styled(
            " PLAN ",
            Style::default()
                .fg(colors::WARNING)
                .add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::styled(" · ", Style::default().fg(colors::SUBTLE)));
    }

    // Unseen warning/error badge (Ctrl+L to view the log).
    if app.unseen_alerts > 0 {
        spans.push(Span::styled(
            format!(" ⚠ {} ", app.unseen_alerts),
            Style::default()
                .fg(colors::WARNING)
                .add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::styled(" · ", Style::default().fg(colors::SUBTLE)));
    }

    // Connection indicator
    if app.connected {
        spans.push(Span::styled(" ● ", Style::default().fg(colors::SUCCESS)));
    } else {
        spans.push(Span::styled(" ○ ", Style::default().fg(colors::ERROR)));
    }

    // Status message (right-aligned as remaining text)
    if !app.status_message.is_empty() {
        spans.push(Span::styled(
            format!(" {}", app.status_message),
            Style::default().fg(colors::INACTIVE),
        ));
    }

    let line = Line::from(spans);
    let status = Paragraph::new(line);
    f.render_widget(status, area);
}

pub fn render_tab_bar(f: &mut Frame, area: Rect, app: &App) {
    let titles: Vec<Span> = Tab::ALL
        .iter()
        .enumerate()
        .flat_map(|(i, tab)| {
            let style = if *tab == app.tab {
                Style::default()
                    .fg(colors::BRAND)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(colors::INACTIVE)
            };
            let mut spans = vec![Span::styled(format!(" [{}]{} ", i + 1, tab.title()), style)];
            if i < Tab::ALL.len() - 1 {
                spans.push(Span::raw(" "));
            }
            spans
        })
        .collect();

    let line = Line::from(titles);
    let tabs = Paragraph::new(line);
    f.render_widget(tabs, area);
}

/// Every binding `App::handle_key`/the per-tab handlers respond to, paired
/// into two side-by-side columns so the overlay stays to one screen instead
/// of scrolling off a normal-height terminal. Keep in sync with `app.rs` —
/// this is the single source of truth for "what can I press right now".
const HELP_LEFT: &[(&str, &str)] = &[
    ("1-6", "Switch tab (Chat types digits)"),
    ("Tab / Shift+Tab", "Next / previous tab"),
    ("Enter", "Send / select / resume session"),
    ("Alt+Enter", "Insert newline (Chat)"),
    ("\u{2191}/\u{2193}, Wheel", "Move selection (lists)"),
    ("j/k, Wheel", "Scroll (Chat/Config)"),
    ("PgUp/PgDn", "Scroll by page (Chat/Config)"),
    ("g / G", "Jump to top / bottom (Chat)"),
];
const HELP_RIGHT: &[(&str, &str)] = &[
    ("Ctrl+K", "Command palette"),
    ("Ctrl+N", "New session"),
    ("Ctrl+O", "Model picker (Chat)"),
    ("Ctrl+P", "Session picker (Chat)"),
    ("Ctrl+Q", "Reopen pending question"),
    ("Ctrl+C", "Quit / stop streaming"),
    ("Ctrl+S", "Stop agent execution"),
    ("Ctrl+X", "Expand/collapse tool detail"),
    ("Ctrl+L", "Notification log"),
    ("] / [", "Next / previous page (Sessions)"),
    ("d", "Delete, with confirm (Sessions/Schedules)"),
    ("n / e", "New schedule / edit config"),
    ("r / t", "Refresh / run · refresh MCP tools"),
    ("F1 / ?", "Toggle this help (? not on Chat)"),
];

pub fn render_help(f: &mut Frame) {
    let screen = f.area();
    const KEY_COL: usize = 17;
    let mut lines: Vec<Line> = vec![
        Line::from(Span::styled(
            " Keybindings",
            Style::default()
                .fg(colors::BRAND)
                .add_modifier(Modifier::BOLD),
        )),
        Line::raw(""),
    ];
    let rows = HELP_LEFT.len().max(HELP_RIGHT.len());
    for i in 0..rows {
        let (lk, ld) = HELP_LEFT.get(i).copied().unwrap_or(("", ""));
        let (rk, rd) = HELP_RIGHT.get(i).copied().unwrap_or(("", ""));
        lines.push(Line::raw(format!(
            "  {lk:<KEY_COL$}{ld:<34}{rk:<KEY_COL$}{rd}"
        )));
    }
    lines.push(Line::raw(""));
    lines.push(Line::raw("  Press any key to close"));

    let height = (lines.len() as u16 + 2).min(screen.height);
    let area = centered_rect(90, height, screen);
    f.render_widget(Clear, area);
    let help = Paragraph::new(lines).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(colors::BRAND)),
    );
    f.render_widget(help, area);
}

/// Notification-log overlay (`Ctrl+L`): recent status messages newest-first,
/// colored by level, so errors/warnings aren't lost when the status line is
/// overwritten. Dismissed by any key.
pub fn render_notifications(f: &mut Frame, app: &App) {
    let screen = f.area();
    let mut lines: Vec<Line> = vec![
        Line::from(Span::styled(
            " Notifications",
            Style::default()
                .fg(colors::BRAND)
                .add_modifier(Modifier::BOLD),
        )),
        Line::raw(""),
    ];

    if app.notifications.is_empty() {
        lines.push(Line::raw("  (nothing yet)"));
    } else {
        // Newest first; cap to what a reasonably tall modal can show.
        let max_rows = (screen.height.saturating_sub(6)).min(30) as usize;
        for n in app.notifications.iter().rev().take(max_rows) {
            let (tag, color) = match n.level {
                NoticeLevel::Info => ("info", colors::INACTIVE),
                NoticeLevel::Warn => ("warn", colors::WARNING),
                NoticeLevel::Error => ("err ", colors::ERROR),
            };
            lines.push(Line::from(vec![
                Span::styled(
                    format!("  {} ", n.at.format("%H:%M:%S")),
                    Style::default().fg(colors::SUBTLE),
                ),
                Span::styled(format!("{tag}  "), Style::default().fg(color)),
                Span::styled(n.text.clone(), Style::default().fg(color)),
            ]));
        }
    }
    lines.push(Line::raw(""));
    lines.push(Line::raw("  Press any key to close"));

    let height = (lines.len() as u16 + 2).min(screen.height);
    let area = centered_rect(70, height, screen);
    f.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(colors::BRAND))
        .title(" Log ");
    f.render_widget(
        Paragraph::new(lines)
            .block(block)
            .wrap(Wrap { trim: false }),
        area,
    );
}

/// Startup-only y/n prompt offered when the initial health check fails
/// against a loopback URL and auto-serve wasn't forced on (`--auto-serve`)
/// or off (`--no-auto-serve`). See `App::serve_offer` / `AutoServeMode`.
/// Precedence-wise this is checked *before* `render_question` and the other
/// exclusive modals below (see `App::handle_key`'s doc comment) since it can
/// only ever be open before any of them exist.
pub fn render_serve_offer(f: &mut Frame, app: &App) {
    let Some(offer) = &app.serve_offer else {
        return;
    };

    let lines = vec![
        Line::from(Span::styled(
            " Local server not reachable",
            Style::default()
                .fg(colors::WARNING)
                .add_modifier(Modifier::BOLD),
        )),
        Line::raw(""),
        Line::raw(format!("  {}", offer.url)),
        Line::raw(""),
        Line::raw("  Start a local `bamboo serve`?"),
        Line::raw(""),
        Line::raw("  y / Enter start  ·  n / Esc skip"),
    ];

    let height = (lines.len() as u16 + 2).min(f.area().height);
    let area = centered_rect(56, height, f.area());
    f.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(colors::WARNING))
        .title(" Auto-serve ");
    f.render_widget(
        Paragraph::new(lines)
            .block(block)
            .wrap(Wrap { trim: false }),
        area,
    );
}

/// Footer line shown in the question modal while the answer POST is in
/// flight, replacing the interactive key hints (input is disabled — see
/// `ActiveQuestion::submitting`).
fn submitting_hint() -> Line<'static> {
    Line::from(Span::styled(
        "  Submitting answer\u{2026}",
        Style::default().fg(colors::WARNING),
    ))
}

fn identity_syncing_hint() -> Line<'static> {
    Line::from(Span::styled(
        "  Synchronizing exact question identity\u{2026}",
        Style::default().fg(colors::WARNING),
    ))
}

fn hard_wrap_preview(value: &str, width: usize, max_lines: usize) -> (Vec<String>, bool) {
    let width = width.max(1);
    if max_lines == 0 {
        return (Vec::new(), !value.is_empty());
    }
    let mut output = Vec::new();
    let mut logical_lines = value.split('\n').peekable();
    while let Some(logical) = logical_lines.next() {
        let mut current = String::new();
        let mut current_width = 0;
        for ch in logical.chars() {
            let char_width = UnicodeWidthChar::width(ch).unwrap_or(0);
            if !current.is_empty() && current_width + char_width > width {
                output.push(std::mem::take(&mut current));
                if output.len() == max_lines {
                    return (output, true);
                }
                current_width = 0;
            }
            current.push(ch);
            current_width += char_width;
        }
        output.push(current);
        if output.len() == max_lines {
            return (output, logical_lines.peek().is_some());
        }
    }
    if output.is_empty() {
        output.push(String::new());
    }
    (output, false)
}

fn ellipsize(value: &str, width: usize) -> String {
    let width = width.max(1);
    let prefix_width_limit = width.saturating_sub(1);
    let mut prefix = String::new();
    let mut prefix_width = 0;
    let mut total_width = 0;
    let mut truncated = false;
    for ch in value.chars() {
        if ch == '\n' {
            truncated = true;
            break;
        }
        let char_width = UnicodeWidthChar::width(ch).unwrap_or(0);
        if total_width + char_width > width {
            truncated = true;
            break;
        }
        total_width += char_width;
        if prefix_width + char_width <= prefix_width_limit {
            prefix.push(ch);
            prefix_width += char_width;
        }
    }
    if truncated {
        prefix.push('…');
        prefix
    } else {
        value.to_string()
    }
}

/// Typed clarification modal. The compact view never changes an option's
/// underlying value; `v` opens a scrollable full-text inspector for the exact
/// question or selected option, including on narrow terminals.
pub fn render_question(f: &mut Frame, app: &App) {
    let Some(q) = &app.pending_question else {
        return;
    };
    q.option_hitboxes.borrow_mut().clear();
    let screen = f.area();

    if q.inspecting {
        let height = screen.height.clamp(6, 24);
        let area = centered_rect(90, height, screen);
        f.render_widget(Clear, area);
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(colors::BRAND))
            .title(" Clarification text inspector ");
        let inner = block.inner(area);
        f.render_widget(block, area);
        let sections = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(2),
                Constraint::Min(1),
                Constraint::Length(3),
            ])
            .split(inner);
        let (label, value) = if q.inspect_option {
            (
                format!("Selected option {} (exact value)", q.selected + 1),
                q.options.get(q.selected).map(String::as_str).unwrap_or(""),
            )
        } else {
            ("Question (full text)".to_string(), q.question.as_str())
        };
        let context = q
            .tool_name
            .as_deref()
            .map(|tool| format!("tool: {tool}"))
            .unwrap_or_else(|| "tool: unknown".to_string());
        f.render_widget(
            Paragraph::new(vec![
                Line::from(Span::styled(
                    format!(" {label}"),
                    Style::default()
                        .fg(colors::BRAND)
                        .add_modifier(Modifier::BOLD),
                )),
                Line::raw(format!(" {context}")),
            ]),
            sections[0],
        );
        let paragraph = Paragraph::new(value).wrap(Wrap { trim: false });
        let wrapped_count = paragraph.line_count(sections[1].width);
        let max_scroll = u16::try_from(wrapped_count.saturating_sub(sections[1].height as usize))
            .unwrap_or(u16::MAX);
        q.inspect_max_scroll.set(max_scroll);
        f.render_widget(
            paragraph.scroll((q.inspect_scroll.min(max_scroll), 0)),
            sections[1],
        );
        let footer = if q.options.is_empty() {
            vec![
                Line::raw(" ↑/↓/PgUp/PgDn scroll"),
                Line::raw(" y copy exact"),
                Line::raw(" v/Esc back"),
            ]
        } else {
            vec![
                Line::raw(" ↑/↓/PgUp/PgDn scroll"),
                Line::raw(" Tab question/option"),
                Line::raw(" y copy exact  ·  v/Esc back"),
            ]
        };
        f.render_widget(Paragraph::new(footer), sections[2]);
        return;
    }

    let popup_width = (screen.width as usize * 80 / 100).max(1);
    let text_width = popup_width.saturating_sub(6).max(1);
    let mut header = vec![Line::from(Span::styled(
        " Clarification needed",
        Style::default()
            .fg(colors::BRAND)
            .add_modifier(Modifier::BOLD),
    ))];
    if q.tool_name.is_some() || q.source.is_some() {
        let context = format!(
            "  tool: {}  ·  source: {}",
            q.tool_name.as_deref().unwrap_or("unknown"),
            q.source.as_deref().unwrap_or("unknown")
        );
        header.push(Line::raw(ellipsize(&context, text_width + 2)));
    }
    header.push(Line::raw(""));
    const QUESTION_PREVIEW_LINES: usize = 4;
    let (wrapped_question, question_truncated) =
        hard_wrap_preview(&q.question, text_width, QUESTION_PREVIEW_LINES);
    for line in &wrapped_question {
        header.push(Line::raw(format!("  {line}")));
    }
    if question_truncated {
        header.push(Line::raw("  …  (v inspect full question)"));
    }
    header.push(Line::raw(""));

    let mut body = Vec::new();
    let mut option_line_positions = Vec::new();
    if let Some(entry) = &q.number_entry {
        body.push(Line::raw(format!("  Go to option #: {entry}▏")));
        body.push(Line::raw(""));
        body.push(Line::raw("  digits type  ·  Enter select"));
        body.push(Line::raw("  Backspace edit  ·  Esc cancel"));
    } else if let Some(buf) = &q.custom {
        body.push(Line::raw("  Custom answer:"));
        body.push(Line::from(Span::styled(
            format!("  > {}▏", ellipsize(buf, text_width.saturating_sub(2))),
            Style::default().fg(colors::BRAND),
        )));
        body.push(Line::raw(""));
        if q.identity_syncing {
            body.push(identity_syncing_hint());
        } else if q.submitting {
            body.push(submitting_hint());
        } else if q.options.is_empty() {
            body.push(Line::raw("  Enter answer  ·  Esc dismiss"));
            body.push(Line::raw("  Ctrl+V inspect/copy question"));
        } else {
            body.push(Line::raw("  Enter answer  ·  Esc options"));
            body.push(Line::raw("  Ctrl+V inspect/copy question"));
        }
    } else if q.options.is_empty() {
        body.push(Line::from(Span::styled(
            "  No selectable answers were supplied and custom input is disabled.",
            Style::default().fg(colors::WARNING),
        )));
        body.push(Line::raw(""));
        body.push(Line::raw("  v inspect/copy question  ·  Esc dismiss"));
    } else {
        let max_h = screen.height.min(24);
        let reserved =
            2 + header.len() + 7 + usize::from(q.allow_custom) + usize::from(q.error.is_some());
        let budget = (max_h as usize).saturating_sub(reserved).max(1);
        let total = q.options.len();
        let start = if total <= budget {
            0
        } else {
            q.selected
                .saturating_sub(budget / 2)
                .min(total.saturating_sub(budget))
        };
        let end = (start + budget).min(total);
        if start > 0 {
            body.push(Line::raw(format!("  ↑ {start} more")));
        }
        for i in start..end {
            let selected = i == q.selected;
            let marker = if selected { "›" } else { " " };
            let style = if selected {
                Style::default()
                    .fg(colors::BRAND)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            let prefix = format!("  {marker} {}. ", i + 1);
            let option_width = text_width.saturating_sub(prefix.chars().count()).max(1);
            option_line_positions.push((body.len(), i));
            body.push(Line::from(Span::styled(
                format!("{prefix}{}", ellipsize(&q.options[i], option_width)),
                style,
            )));
        }
        if end < total {
            body.push(Line::raw(format!("  ↓ {} more", total - end)));
        }
        body.push(Line::raw(""));
        if q.identity_syncing {
            body.push(identity_syncing_hint());
        } else if q.submitting {
            body.push(submitting_hint());
        } else {
            body.push(Line::raw("  click option answer  ·  ↑/↓/wheel select"));
            body.push(Line::raw("  Enter answer  ·  Esc dismiss  ·  1-9 quick"));
            body.push(Line::raw("  g number  ·  v inspect  ·  y copy"));
            if q.allow_custom {
                body.push(Line::raw("  c custom answer"));
            }
        }
    }
    if let Some(error) = &q.error {
        body.push(Line::from(Span::styled(
            format!(
                "  Error: {}",
                ellipsize(error, text_width.saturating_sub(7))
            ),
            Style::default().fg(colors::ERROR),
        )));
    }
    let header_len = header.len();
    let mut lines = header;
    lines.extend(body);
    let height = (lines.len() as u16 + 2).min(screen.height);
    let area = centered_rect(80, height, screen);
    let option_x = area.x.saturating_add(1);
    let option_width = area.width.saturating_sub(2);
    let option_bottom = area.y.saturating_add(area.height).saturating_sub(1);
    *q.option_hitboxes.borrow_mut() = option_line_positions
        .into_iter()
        .filter_map(|(body_line, index)| {
            let y = area
                .y
                .saturating_add(1)
                .saturating_add(header_len as u16)
                .saturating_add(body_line as u16);
            (y < option_bottom).then_some(QuestionOptionHitbox {
                x: option_x,
                y,
                width: option_width,
                index,
            })
        })
        .collect();
    f.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(colors::BRAND))
        .title(" Clarification ");
    f.render_widget(Paragraph::new(lines).block(block), area);
}

/// Modal confirming a session delete (`d` on the Sessions tab). Mirrors the
/// question modal's key rationale: a destructive action must not fire on a
/// single stray keystroke, so it stops here until `y`/Enter or `n`/Esc.
pub fn render_delete_confirm(f: &mut Frame, app: &App) {
    let Some((_, title)) = &app.pending_delete else {
        return;
    };
    let display_title: &str = if title.is_empty() {
        "(untitled)"
    } else {
        title
    };

    let lines = vec![
        Line::from(Span::styled(
            " Delete session?",
            Style::default()
                .fg(colors::ERROR)
                .add_modifier(Modifier::BOLD),
        )),
        Line::raw(""),
        Line::raw(format!("  \"{}\"", display_title)),
        Line::raw(""),
        Line::raw("  This cannot be undone."),
        Line::raw(""),
        Line::raw("  y / Enter confirm  ·  n / Esc cancel"),
    ];

    let height = (lines.len() as u16 + 2).min(f.area().height);
    let area = centered_rect(50, height, f.area());
    f.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(colors::ERROR))
        .title(" Confirm ");
    f.render_widget(
        Paragraph::new(lines)
            .block(block)
            .wrap(Wrap { trim: false }),
        area,
    );
}

/// Modal form for creating a new schedule (opened with `n` on the Schedules tab).
pub fn render_schedule_form(f: &mut Frame, app: &App) {
    let Some(form) = &app.schedule_form else {
        return;
    };
    let fields = [
        ("Name", &form.name),
        ("Cron", &form.cron),
        ("Prompt", &form.prompt),
    ];
    let mut lines: Vec<Line> = vec![
        Line::from(Span::styled(
            " New schedule",
            Style::default()
                .fg(colors::BRAND)
                .add_modifier(Modifier::BOLD),
        )),
        Line::raw(""),
    ];
    for (i, (label, val)) in fields.iter().enumerate() {
        let focused = i == form.field;
        let cursor = if focused { "\u{258f}" } else { "" };
        let style = if focused {
            Style::default().fg(colors::BRAND)
        } else {
            Style::default().fg(colors::INACTIVE)
        };
        lines.push(Line::from(vec![
            Span::styled(
                format!("  {label:<7}: "),
                Style::default().fg(colors::SUBTLE),
            ),
            Span::styled(format!("{val}{cursor}"), style),
        ]));
    }
    lines.push(Line::raw(""));
    lines.push(Line::raw(
        "  Tab / \u{2191}\u{2193} field  \u{b7}  Enter create  \u{b7}  Esc cancel",
    ));

    let height = (lines.len() as u16 + 2).min(f.area().height);
    let area = centered_rect(60, height, f.area());
    f.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(colors::BRAND))
        .title(" Schedule ");
    f.render_widget(
        Paragraph::new(lines)
            .block(block)
            .wrap(Wrap { trim: false }),
        area,
    );
}

/// Contextual session picker (`Ctrl+P` on Chat). It renders over the
/// transcript rather than switching to the management tab, so dismissing it
/// restores the exact composer draft/cursor and transcript scroll beneath it.
pub fn render_session_picker(f: &mut Frame, app: &App) {
    let Some(picker) = &app.session_picker else {
        return;
    };
    let screen = f.area();
    let popup_width = ((screen.width as u32 * 94 / 100) as u16).max(1);
    let row_width = popup_width.saturating_sub(4);
    let mut lines = vec![Line::from(Span::styled(
        " Sessions",
        Style::default()
            .fg(colors::BRAND)
            .add_modifier(Modifier::BOLD),
    ))];
    // Search owns keyboard focus only in browse mode. Rename has its own
    // title editor and cursor, while pinning owns all input as an action
    // state; showing the search cursor in those modes would imply two active
    // fields even though keystrokes can reach only one of them.
    if matches!(&picker.mode, SessionPickerMode::Browse) {
        lines.push(Line::from(vec![
            Span::styled(" Search: ", Style::default().fg(colors::SUBTLE)),
            Span::styled(
                format!(
                    "{}▏",
                    clip_tail_cells(&picker.query, row_width.saturating_sub(10).max(1) as usize)
                ),
                Style::default().fg(colors::BRAND),
            ),
        ]));
    }

    match &picker.mode {
        SessionPickerMode::Browse => {
            lines.push(Line::raw(""));
            if picker.visible.is_empty() {
                let message = if picker.loading {
                    "  Loading sessions..."
                } else if picker.query.is_empty() {
                    "  No sessions found"
                } else {
                    "  No matches in loaded sessions"
                };
                lines.push(Line::raw(message));
            } else {
                let max_height = screen.height.min(24);
                let budget = (max_height as usize).saturating_sub(9).max(1);
                let total = picker.visible.len();
                let start = if total <= budget {
                    0
                } else {
                    picker
                        .selected
                        .saturating_sub(budget / 2)
                        .min(total.saturating_sub(budget))
                };
                let end = (start + budget).min(total);
                if start > 0 {
                    lines.push(Line::raw(format!("  ↑ {start} more")));
                }
                for visible_index in start..end {
                    if let Some(session) = picker
                        .visible
                        .get(visible_index)
                        .and_then(|index| picker.sessions.get(*index))
                    {
                        lines.push(session_row_line(
                            session,
                            visible_index == picker.selected,
                            row_width,
                        ));
                    }
                }
                if end < total {
                    lines.push(Line::raw(format!("  ↓ {} more", total - end)));
                }
            }
            if let Some(error) = &picker.error {
                lines.push(Line::from(Span::styled(
                    format!(
                        "  {}",
                        clip_cells(error, row_width.saturating_sub(2) as usize)
                    ),
                    Style::default().fg(colors::ERROR),
                )));
            }
            let cap = if picker.sessions.len() >= 1_000 && picker.sessions.len() < picker.total {
                " · memory cap reached"
            } else {
                ""
            };
            lines.push(Line::from(Span::styled(
                format!(
                    "  loaded {} / {}{}{}",
                    picker.sessions.len(),
                    picker.total,
                    if picker.loading { " · loading" } else { "" },
                    cap
                ),
                Style::default().fg(colors::SUBTLE),
            )));
            if row_width < 70 {
                lines.push(Line::raw("  ↑/↓/wheel · Enter open · F2 rename · F3 pin"));
                lines.push(Line::raw(
                    "  Del delete · ] more · Ctrl+R retry · Esc cancel",
                ));
            } else {
                lines.push(Line::raw(
                    "  ↑/↓/wheel select · Enter open · F2 rename · F3 pin",
                ));
                lines.push(Line::raw(
                    "  Ctrl+D/Delete delete · ] load more · Ctrl+R retry · Esc cancel",
                ));
            }
        }
        SessionPickerMode::Rename {
            draft,
            loading_version,
            submitting,
            error,
            ..
        } => {
            lines.push(Line::raw(""));
            lines.push(Line::from(Span::styled(
                " Rename session",
                Style::default()
                    .fg(colors::BRAND)
                    .add_modifier(Modifier::BOLD),
            )));
            lines.push(Line::from(vec![
                Span::styled("  Title: ", Style::default().fg(colors::SUBTLE)),
                Span::styled(
                    if *submitting {
                        clip_tail_cells(draft, row_width.saturating_sub(12) as usize)
                    } else {
                        format!(
                            "{}▏",
                            clip_tail_cells(draft, row_width.saturating_sub(12) as usize)
                        )
                    },
                    Style::default().fg(colors::BRAND),
                ),
            ]));
            if *loading_version {
                lines.push(Line::raw("  Fetching current version..."));
            } else if *submitting {
                lines.push(Line::raw("  Saving..."));
            }
            if let Some(error) = error {
                lines.push(Line::from(Span::styled(
                    format!(
                        "  {}",
                        clip_cells(error, row_width.saturating_sub(2) as usize)
                    ),
                    Style::default().fg(colors::ERROR),
                )));
            }
            if !*submitting {
                lines.push(Line::raw(""));
                lines.push(Line::raw(if row_width < 70 {
                    "  Enter save · Ctrl+R retry · Esc cancel"
                } else {
                    "  Enter save · Ctrl+R refetch/retry · Esc keep old title"
                }));
            }
        }
        SessionPickerMode::Pinning {
            target,
            loading_version,
            submitting,
            error,
            ..
        } => {
            lines.push(Line::raw(""));
            lines.push(Line::raw(if *target {
                "  Pinning selected session..."
            } else {
                "  Unpinning selected session..."
            }));
            if *loading_version {
                lines.push(Line::raw("  Fetching current version..."));
            } else if *submitting {
                lines.push(Line::raw("  Saving..."));
            }
            if let Some(error) = error {
                lines.push(Line::from(Span::styled(
                    format!(
                        "  {}",
                        clip_cells(error, row_width.saturating_sub(2) as usize)
                    ),
                    Style::default().fg(colors::ERROR),
                )));
            }
            if !*submitting {
                lines.push(Line::raw(""));
                lines.push(Line::raw("  Ctrl+R refetch/retry · Esc cancel"));
            }
        }
    }

    let height = (lines.len() as u16 + 2).min(screen.height);
    let area = centered_rect(94, height, screen);
    f.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(colors::BRAND))
        .title(" Session picker ");
    f.render_widget(
        Paragraph::new(lines)
            .block(block)
            .wrap(Wrap { trim: false }),
        area,
    );
}

/// Model picker modal (`Ctrl+O` on the Chat tab): pick a model from the
/// provider catalog. Mirrors `render_question`'s option-list windowing so the
/// selection stays visible (and the modal never overflows the screen) no
/// matter how many models the catalog reports.
pub fn render_model_picker(f: &mut Frame, app: &App) {
    let Some(picker) = &app.model_picker else {
        return;
    };

    let screen = f.area();
    let popup_width = ((screen.width as u32 * 92 / 100) as u16).max(1);
    let row_width = popup_width.saturating_sub(4) as usize;
    let header: Vec<Line> = vec![
        Line::from(Span::styled(
            " Select a model",
            Style::default()
                .fg(colors::BRAND)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(vec![
            Span::styled(" Search: ", Style::default().fg(colors::SUBTLE)),
            Span::styled(
                format!(
                    "{}▏",
                    clip_tail_cells(&picker.query, row_width.saturating_sub(10).max(1))
                ),
                Style::default().fg(colors::BRAND),
            ),
        ]),
        Line::raw(""),
    ];

    let mut body: Vec<Line> = Vec::new();
    if picker.loading && picker.models.is_empty() {
        body.push(Line::raw("  Loading models..."));
        body.push(Line::raw(""));
        body.push(Line::raw("  Esc cancel"));
    } else if picker.loading && picker.visible.is_empty() {
        body.push(Line::raw("  Refreshing model catalog..."));
        body.push(Line::raw(""));
        body.push(Line::raw("  Esc cancel"));
    } else if picker.visible.is_empty() {
        body.push(Line::raw(if picker.query.is_empty() {
            "  No models available"
        } else {
            "  No models match this search"
        }));
        body.push(Line::raw(""));
        body.push(Line::raw(if picker.models.is_empty() {
            "  Edit search · Ctrl+R retry load · Esc cancel"
        } else {
            "  Edit search · Ctrl+U clear · Ctrl+R refresh · Esc cancel"
        }));
    } else {
        let max_h = screen.height.min(22);
        let total = picker.visible.len();
        let groups = picker
            .visible
            .iter()
            .filter_map(|index| picker.models.get(*index))
            .map(|model| app.model_group_label(model))
            .collect::<Vec<_>>();

        // A group heading costs a terminal row too. Grow a balanced window
        // around the selection using the actual row cost, while reserving two
        // lines for the possible above/below indicators. This keeps the
        // highlighted model visible even with 100 providers on a short screen.
        let line_budget = (max_h as usize)
            .saturating_sub(2 + header.len() + 2)
            .saturating_sub(2)
            .max(2);
        let selected = picker.selected.min(total.saturating_sub(1));
        let mut start = selected;
        let mut end = selected + 1;
        let mut used = 2; // first group heading + selected model row
        loop {
            let mut expanded = false;
            if start > 0 {
                let candidate = start - 1;
                let cost = 1 + usize::from(groups[candidate] != groups[start]);
                if used + cost <= line_budget {
                    start = candidate;
                    used += cost;
                    expanded = true;
                }
            }
            if end < total {
                let cost = 1 + usize::from(groups[end] != groups[end - 1]);
                if used + cost <= line_budget {
                    used += cost;
                    end += 1;
                    expanded = true;
                }
            }
            if !expanded {
                break;
            }
        }
        if start > 0 {
            body.push(Line::raw(format!("  \u{2191} {start} more")));
        }
        let mut previous_group: Option<&str> = None;
        for i in start..end {
            let Some(m) = picker
                .visible
                .get(i)
                .and_then(|index| picker.models.get(*index))
            else {
                continue;
            };
            let group = groups.get(i).map(String::as_str).unwrap_or("Provider");
            if previous_group != Some(group) {
                body.push(Line::from(Span::styled(
                    clip_cells(&format!("  {group}"), row_width),
                    Style::default()
                        .fg(colors::SUBTLE)
                        .add_modifier(Modifier::BOLD),
                )));
                previous_group = Some(group);
            }
            let selected = i == picker.selected;
            let marker = if selected { "\u{203a}" } else { " " };
            let style = if selected {
                Style::default()
                    .fg(colors::BRAND)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            body.push(Line::from(Span::styled(
                truncate_cells(
                    &format!(
                        "  {marker} {} · {} · {}/{}",
                        m.display_name,
                        m.provider_display_name,
                        m.reference.provider,
                        m.reference.model,
                    ),
                    row_width,
                ),
                style,
            )));
        }
        if end < total {
            body.push(Line::raw(format!("  \u{2193} {} more", total - end)));
        }
        body.push(Line::raw(""));
        body.push(Line::raw(if picker.applying {
            "  Applying model..."
        } else {
            "  \u{2191}/\u{2193}/wheel select · Enter apply · Esc cancel"
        }));
    }
    if let Some(error) = &picker.error {
        body.push(Line::from(Span::styled(
            clip_cells(&format!("  {error}"), row_width),
            Style::default().fg(colors::ERROR),
        )));
    }

    let mut lines = header;
    lines.extend(body);
    let height = (lines.len() as u16 + 2).min(screen.height);
    let area = centered_rect(92, height, screen);
    f.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(colors::BRAND))
        .title(" Model ");
    let para = Paragraph::new(lines)
        .block(block)
        .wrap(Wrap { trim: false });
    f.render_widget(para, area);
}

/// Combined built-in and session-aware command palette. Each result owns two
/// fixed terminal rows, which makes both keyboard windowing and mouse hitboxes
/// deterministic even when descriptions are long or the terminal is narrow.
/// The list is clipped instead of wrapped so a row can never push the footer
/// below the popup or move beneath the pointer between mouse-down/up events.
pub fn render_command_palette(f: &mut Frame, app: &App) {
    let Some(palette) = &app.command_palette else {
        return;
    };
    let disabled_reasons = palette
        .entries
        .iter()
        .map(|entry| {
            app.command_palette_disabled_reason(entry)
                .map(str::to_string)
        })
        .collect::<Vec<_>>();
    let view = CommandPaletteView {
        trigger: palette.trigger,
        input: &palette.input,
        entries: &palette.entries,
        visible: &palette.visible,
        selected: palette.selected,
        loading: palette.loading,
        resolving: palette.resolving,
        error: palette.error.as_deref(),
        disabled_reasons: &disabled_reasons,
    };
    render_command_palette_view(f, view, Some(&palette.hitboxes));
}

struct CommandPaletteView<'a> {
    trigger: CommandPaletteTrigger,
    input: &'a str,
    entries: &'a [CommandPaletteEntry],
    visible: &'a [usize],
    selected: usize,
    loading: bool,
    resolving: bool,
    error: Option<&'a str>,
    disabled_reasons: &'a [Option<String>],
}

struct CommandPaletteRender {
    lines: Vec<Line<'static>>,
    /// `(visible index, first content-row, height)` for the rows actually
    /// present in this frame. The content row is relative to the block's
    /// inner top edge and becomes an absolute hitbox after centering.
    item_rows: Vec<(usize, u16, u16)>,
}

fn render_command_palette_view(
    f: &mut Frame,
    view: CommandPaletteView<'_>,
    hitboxes: Option<&RefCell<Vec<CommandPaletteHitbox>>>,
) {
    let screen = f.area();
    let popup_width = ((screen.width as u32 * 94 / 100) as u16).max(1);
    let row_width = popup_width.saturating_sub(4) as usize;
    let rendered = command_palette_lines(&view, screen.height, row_width);
    let height = (rendered.lines.len() as u16 + 2).min(screen.height);
    let area = centered_rect(94, height, screen);

    if let Some(hitboxes) = hitboxes {
        let width = area.width.saturating_sub(2);
        *hitboxes.borrow_mut() = rendered
            .item_rows
            .iter()
            .filter_map(|(index, row, row_height)| {
                let y = area.y.saturating_add(1).saturating_add(*row);
                let available = area
                    .y
                    .saturating_add(area.height.saturating_sub(1))
                    .saturating_sub(y);
                let height = (*row_height).min(available);
                (width > 0 && height > 0).then_some(CommandPaletteHitbox {
                    index: *index,
                    x: area.x.saturating_add(1),
                    y,
                    width,
                    height,
                })
            })
            .collect();
    }

    f.render_widget(Clear, area);
    let title = match view.trigger {
        CommandPaletteTrigger::Slash => " Slash commands ",
        CommandPaletteTrigger::Global => " Command palette ",
    };
    let border_color = if view.resolving {
        colors::WARNING
    } else {
        colors::BRAND
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(border_color))
        .title(title);
    f.render_widget(Paragraph::new(rendered.lines).block(block), area);
}

fn command_palette_lines(
    view: &CommandPaletteView<'_>,
    screen_height: u16,
    row_width: usize,
) -> CommandPaletteRender {
    let title = match view.trigger {
        CommandPaletteTrigger::Slash => " Slash commands",
        CommandPaletteTrigger::Global => " Command palette",
    };
    let query_prefix = if matches!(view.trigger, CommandPaletteTrigger::Slash) {
        "/"
    } else {
        ""
    };
    let search_width = row_width.saturating_sub(10).max(1);
    let mut lines = vec![
        Line::from(Span::styled(
            title,
            Style::default()
                .fg(colors::BRAND)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(vec![
            Span::styled(" Search: ", Style::default().fg(colors::SUBTLE)),
            Span::styled(
                format!(
                    "{}{}▏",
                    query_prefix,
                    clip_tail_cells(view.input, search_width.saturating_sub(query_prefix.len()))
                ),
                Style::default().fg(colors::BRAND),
            ),
        ]),
        Line::raw(""),
    ];

    if view.resolving {
        lines.push(Line::from(Span::styled(
            "  Resolving preview…",
            Style::default().fg(colors::WARNING),
        )));
    } else if view.loading {
        lines.push(Line::from(Span::styled(
            clip_cells(
                "  Loading session commands… built-ins remain available",
                row_width,
            ),
            Style::default().fg(colors::INACTIVE),
        )));
    }

    let status_rows = usize::from(view.loading || view.resolving);
    let error_rows = usize::from(view.error.is_some());
    // Border (2), header (3), status/error, footer (3), and at most two
    // above/below indicators are reserved before selecting the two-row item
    // window. The selected item is therefore always fully visible at 60,
    // 80, and 120 columns on ordinary 24-row terminals.
    let max_content_rows = screen_height.min(26).saturating_sub(2) as usize;
    let non_list_rows = 3 + status_rows + error_rows + 3;
    let list_rows = max_content_rows.saturating_sub(non_list_rows);
    let max_items = list_rows.saturating_sub(2).max(2) / 2;
    let total = view.visible.len();
    let mut item_rows = Vec::new();

    if total == 0 {
        lines.push(Line::from(Span::styled(
            if view.entries.is_empty() && !view.loading {
                "  No commands available"
            } else if view.loading {
                "  Waiting for commands…"
            } else {
                "  No commands match this search"
            },
            Style::default().fg(colors::INACTIVE),
        )));
    } else {
        let selected = view.selected.min(total.saturating_sub(1));
        let window_len = max_items.max(1).min(total);
        let start = selected
            .saturating_sub(window_len / 2)
            .min(total.saturating_sub(window_len));
        let end = (start + window_len).min(total);
        if start > 0 {
            lines.push(Line::from(Span::styled(
                format!("  ↑ {start} more"),
                Style::default().fg(colors::SUBTLE),
            )));
        }

        for visible_index in start..end {
            let Some(entry_index) = view.visible.get(visible_index) else {
                continue;
            };
            let Some(entry) = view.entries.get(*entry_index) else {
                continue;
            };
            let first_row = lines.len() as u16;
            let is_selected = visible_index == selected;
            let marker = if is_selected { "›" } else { " " };
            let name_style = if is_selected {
                Style::default()
                    .fg(colors::BRAND)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            let badge = clip_cells(
                &format!("[{} · {}]", entry.type_label(), entry.source_label()),
                (row_width / 2).max(1),
            );
            let badge_width: usize = badge
                .chars()
                .map(|c| UnicodeWidthChar::width(c).unwrap_or(0))
                .sum();
            let name_width = row_width.saturating_sub(badge_width + 6);
            lines.push(Line::from(vec![
                Span::styled(format!("  {marker} "), name_style),
                Span::styled(
                    clip_cells(entry.display_name(), name_width.max(1)),
                    name_style,
                ),
                Span::raw("  "),
                Span::styled(badge, palette_type_style(entry.type_label())),
            ]));

            let disabled = view.disabled_reasons.get(*entry_index).cloned().flatten();
            let description = disabled
                .as_ref()
                .map(|reason| format!("Disabled: {reason}"))
                .unwrap_or_else(|| {
                    let description = entry.description().trim();
                    if description.is_empty() {
                        "No description".to_string()
                    } else {
                        description.to_string()
                    }
                });
            lines.push(Line::from(Span::styled(
                clip_cells(&format!("      {description}"), row_width),
                if disabled.is_some() {
                    Style::default().fg(colors::ERROR)
                } else {
                    Style::default().fg(colors::INACTIVE)
                },
            )));
            item_rows.push((visible_index, first_row, 2));
        }
        if end < total {
            lines.push(Line::from(Span::styled(
                format!("  ↓ {} more", total - end),
                Style::default().fg(colors::SUBTLE),
            )));
        }
    }

    if let Some(error) = view.error {
        lines.push(Line::from(Span::styled(
            clip_cells(&format!("  {error}"), row_width),
            Style::default().fg(colors::ERROR),
        )));
    }
    lines.push(Line::raw(""));
    if row_width < 70 {
        lines.push(Line::raw("  ↑/↓/wheel select · Enter use · Esc cancel"));
        lines.push(Line::raw("  Ctrl+R retry/refresh · Ctrl+U clear"));
    } else {
        lines.push(Line::raw(
            "  ↑/↓/PgUp/PgDn/wheel select · Enter use · Esc cancel",
        ));
        lines.push(Line::raw(
            "  Type to search · Ctrl+R refresh · Ctrl+U clear",
        ));
    }

    CommandPaletteRender { lines, item_rows }
}

fn palette_type_style(command_type: &str) -> Style {
    let color = match command_type {
        "prompt" => colors::BRAND,
        "workflow" => colors::SUCCESS,
        "skill" => colors::WARNING,
        "mcp" => colors::TOOL_RUNNING,
        _ => colors::INACTIVE,
    };
    Style::default().fg(color)
}

fn centered_rect(percent_x: u16, height: u16, r: Rect) -> Rect {
    // u32 math so a very wide terminal (width ≥ 820) can't overflow the u16
    // multiply of `r.width * percent_x`.
    let popup_width = (r.width as u32 * percent_x as u32 / 100) as u16;
    let x = (r.width.saturating_sub(popup_width)) / 2;
    let y = (r.height.saturating_sub(height)) / 2;
    Rect::new(
        r.x + x,
        r.y + y,
        popup_width.min(r.width),
        height.min(r.height),
    )
}

fn clip_cells(value: &str, max_width: usize) -> String {
    if max_width == 0 {
        return String::new();
    }
    let mut output = String::new();
    let mut used = 0;
    for character in value.chars() {
        let character_width = UnicodeWidthChar::width(character).unwrap_or(0);
        if used + character_width > max_width {
            while used > max_width.saturating_sub(1) {
                let Some(removed) = output.pop() else {
                    break;
                };
                used = used.saturating_sub(UnicodeWidthChar::width(removed).unwrap_or(0));
            }
            output.push('…');
            return output;
        }
        output.push(character);
        used += character_width;
    }
    output
}

fn clip_tail_cells(value: &str, max_width: usize) -> String {
    if max_width == 0 {
        return String::new();
    }
    let width = value
        .chars()
        .map(|character| UnicodeWidthChar::width(character).unwrap_or(0))
        .sum::<usize>();
    if width <= max_width {
        return value.to_string();
    }

    let target = max_width.saturating_sub(1);
    let mut suffix = Vec::new();
    let mut used = 0;
    for character in value.chars().rev() {
        let character_width = UnicodeWidthChar::width(character).unwrap_or(0);
        if used + character_width > target {
            break;
        }
        suffix.push(character);
        used += character_width;
    }
    suffix.reverse();
    format!("…{}", suffix.into_iter().collect::<String>())
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::{
        clip_cells, clip_tail_cells, command_palette_lines, render_command_palette_view,
        CommandPaletteView,
    };
    use crate::api::types::CommandItem;
    use crate::api::BambooClient;
    use crate::app::{
        App, BuiltinPaletteAction, CommandPaletteEntry, CommandPaletteHitbox, CommandPaletteTrigger,
    };
    use ratatui::backend::TestBackend;
    use ratatui::text::Line;
    use ratatui::Terminal;
    use unicode_width::UnicodeWidthStr;

    fn command(
        name: impl Into<String>,
        command_type: &str,
        source: &str,
        description: impl Into<String>,
    ) -> CommandPaletteEntry {
        let name = name.into();
        CommandPaletteEntry::Server(CommandItem {
            id: format!("{command_type}:{name}"),
            display_name: name.clone(),
            name,
            description: description.into(),
            command_type: command_type.to_string(),
            category: None,
            tags: None,
            metadata: serde_json::json!({ "source": source }),
        })
    }

    fn line_text(line: &Line<'_>) -> String {
        line.spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect()
    }

    fn palette_text(lines: &[Line<'_>]) -> String {
        lines.iter().map(line_text).collect::<Vec<_>>().join("\n")
    }

    fn terminal_text(terminal: &Terminal<TestBackend>) -> String {
        let buffer = terminal.backend().buffer();
        let area = buffer.area;
        (area.y..area.y.saturating_add(area.height))
            .map(|row| {
                (area.x..area.x.saturating_add(area.width))
                    .map(|column| buffer[(column, row)].symbol())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// The two-column help overlay must fit a normal-height terminal without
    /// vertical clipping and must still mention the headline bindings — the
    /// single-column version it replaced listed 29 lines in a fixed 25-row
    /// modal and silently clipped the bottom entries on anything but a tall
    /// terminal.
    #[test]
    fn help_overlay_fits_one_screen_and_lists_bindings() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.help_visible = true;

        let backend = TestBackend::new(100, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| crate::ui::render(f, &app)).unwrap();

        let buf = terminal.backend().buffer().clone();
        let text: String = buf.content().iter().map(|c| c.symbol()).collect();
        for needle in [
            "Ctrl+K",
            "Ctrl+N",
            "Ctrl+O",
            "Ctrl+P",
            "Ctrl+Q",
            "Ctrl+C",
            "Ctrl+S",
            "Ctrl+X",
            "Ctrl+L",
            "Alt+Enter",
            "F1",
            "Press any key to close",
        ] {
            assert!(text.contains(needle), "help overlay missing {needle:?}");
        }
    }

    #[test]
    fn cell_clipping_handles_unicode_and_keeps_the_input_tail_visible() {
        assert_eq!(clip_cells("abcdef", 4), "abc…");
        assert_eq!(clip_cells("会话标题", 5), "会话…");
        assert_eq!(clip_cells("界", 1), "…");
        assert_eq!(clip_tail_cells("abcdef", 4), "…def");
        assert_eq!(clip_tail_cells("会话标题", 5), "…标题");
        assert_eq!(clip_tail_cells("界", 1), "…");
    }

    #[test]
    fn command_palette_render_snapshots_are_responsive_at_60_80_120() {
        let mut entries = vec![
            CommandPaletteEntry::Builtin(BuiltinPaletteAction::NewSession),
            CommandPaletteEntry::Builtin(BuiltinPaletteAction::Stop),
            CommandPaletteEntry::Builtin(BuiltinPaletteAction::ToggleDetails),
        ];
        entries.extend((0..12).map(|index| {
            if index == 7 {
                command(
                    "Deploy production",
                    "workflow",
                    "workspace",
                    "Preview a deploy workflow without sending it",
                )
            } else {
                command(
                    format!("Command {index}"),
                    if index % 2 == 0 { "prompt" } else { "skill" },
                    if index % 3 == 0 { "project" } else { "global" },
                    format!("Description for command {index}"),
                )
            }
        }));
        let visible = (0..entries.len()).collect::<Vec<_>>();
        let selected = 10; // `Deploy production` after the three built-ins.
        let disabled_reasons = vec![None; entries.len()];

        for width in [60, 80, 120] {
            let hitboxes = RefCell::<Vec<CommandPaletteHitbox>>::new(Vec::new());
            let view = CommandPaletteView {
                trigger: CommandPaletteTrigger::Slash,
                input: "dep production",
                entries: &entries,
                visible: &visible,
                selected,
                loading: false,
                resolving: false,
                error: None,
                disabled_reasons: &disabled_reasons,
            };
            let backend = TestBackend::new(width, 24);
            let mut terminal = Terminal::new(backend).unwrap();
            terminal
                .draw(|frame| render_command_palette_view(frame, view, Some(&hitboxes)))
                .unwrap();

            let text = terminal_text(&terminal);
            for needle in [
                "Slash commands",
                "/dep production",
                "Deploy production",
                "workflow · workspace",
                "Enter use",
                "Ctrl+R",
            ] {
                assert!(
                    text.contains(needle),
                    "{width}-column palette missing {needle:?}:\n{text}"
                );
            }
            assert!(text.contains("↑ "), "selected window lost its upper marker");
            assert!(text.contains("↓ "), "selected window lost its lower marker");

            let hitboxes = hitboxes.borrow();
            assert!(
                hitboxes.iter().any(|hitbox| hitbox.index == selected),
                "{width}-column palette did not expose the selected row hitbox"
            );
            assert!(hitboxes.iter().all(|hitbox| {
                hitbox.height == 2
                    && hitbox.x.saturating_add(hitbox.width) <= width
                    && hitbox.y.saturating_add(hitbox.height) <= 24
            }));

            let row_width = ((width as u32 * 94 / 100) as usize).saturating_sub(4);
            let pure = command_palette_lines(
                &CommandPaletteView {
                    trigger: CommandPaletteTrigger::Slash,
                    input: "dep production",
                    entries: &entries,
                    visible: &visible,
                    selected,
                    loading: false,
                    resolving: false,
                    error: None,
                    disabled_reasons: &disabled_reasons,
                },
                24,
                row_width,
            );
            assert!(pure
                .lines
                .iter()
                .map(line_text)
                .all(|line| UnicodeWidthStr::width(line.as_str()) <= row_width));
        }
    }

    #[test]
    fn command_palette_renders_loading_error_empty_and_resolving_states() {
        let entries = vec![command("Review", "prompt", "workspace", "Review changes")];
        let disabled_reasons = vec![None; entries.len()];
        let empty_visible = Vec::new();
        let loading = command_palette_lines(
            &CommandPaletteView {
                trigger: CommandPaletteTrigger::Global,
                input: "missing",
                entries: &entries,
                visible: &empty_visible,
                selected: 0,
                loading: true,
                resolving: false,
                error: Some("API unavailable — Ctrl+R to retry"),
                disabled_reasons: &disabled_reasons,
            },
            24,
            72,
        );
        let text = palette_text(&loading.lines);
        assert!(text.contains("Loading session commands"));
        assert!(text.contains("Waiting for commands"));
        assert!(text.contains("API unavailable"));

        let visible = vec![0];
        let resolving = command_palette_lines(
            &CommandPaletteView {
                trigger: CommandPaletteTrigger::Global,
                input: "review",
                entries: &entries,
                visible: &visible,
                selected: 0,
                loading: false,
                resolving: true,
                error: None,
                disabled_reasons: &disabled_reasons,
            },
            24,
            72,
        );
        assert!(palette_text(&resolving.lines).contains("Resolving preview"));

        let no_commands = command_palette_lines(
            &CommandPaletteView {
                trigger: CommandPaletteTrigger::Global,
                input: "",
                entries: &[],
                visible: &[],
                selected: 0,
                loading: false,
                resolving: false,
                error: None,
                disabled_reasons: &[],
            },
            24,
            72,
        );
        assert!(palette_text(&no_commands.lines).contains("No commands available"));

        let no_matches = command_palette_lines(
            &CommandPaletteView {
                trigger: CommandPaletteTrigger::Global,
                input: "missing",
                entries: &entries,
                visible: &[],
                selected: 0,
                loading: false,
                resolving: false,
                error: None,
                disabled_reasons: &disabled_reasons,
            },
            24,
            72,
        );
        assert!(palette_text(&no_matches.lines).contains("No commands match"));
    }

    #[test]
    fn disabled_reasons_match_runtime_availability_and_label_type_source() {
        let entries = vec![
            CommandPaletteEntry::Builtin(BuiltinPaletteAction::NewSession),
            CommandPaletteEntry::Builtin(BuiltinPaletteAction::Stop),
            command(
                "Deploy production",
                "workflow",
                "workspace",
                "Preview deploy workflow",
            ),
        ];
        let visible = vec![0, 1, 2];
        let disabled_reasons = vec![
            Some("Unavailable while an agent run is active".to_string()),
            None,
            Some("Composer commands are unavailable while a run is active".to_string()),
        ];
        let rendered = command_palette_lines(
            &CommandPaletteView {
                trigger: CommandPaletteTrigger::Global,
                input: "",
                entries: &entries,
                visible: &visible,
                selected: 1,
                loading: false,
                resolving: false,
                error: None,
                disabled_reasons: &disabled_reasons,
            },
            24,
            80,
        );
        let lines = rendered.lines.iter().map(line_text).collect::<Vec<_>>();
        let description_for = |visible_index| {
            let (_, first_row, _) = rendered
                .item_rows
                .iter()
                .find(|(index, _, _)| *index == visible_index)
                .copied()
                .unwrap();
            &lines[first_row as usize + 1]
        };

        assert!(description_for(0).contains("Disabled: Unavailable"));
        assert_eq!(
            description_for(1).trim(),
            BuiltinPaletteAction::Stop.description()
        );
        assert!(description_for(2).contains("Disabled: Composer commands"));
        assert!(description_for(2).contains("run is active"));
        assert!(palette_text(&rendered.lines).contains("workflow · workspace"));
    }
}
