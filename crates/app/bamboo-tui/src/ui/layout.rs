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
use crate::keymap::{ActionContext, ActionId};
use crate::theme::{self, colors};
use crate::ui::sessions::{session_row_line, truncate_cells};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayoutMode {
    Compact,
    Regular,
    Wide,
}

pub fn layout_mode(area: Rect) -> LayoutMode {
    if area.width < 80 || area.height < 24 {
        LayoutMode::Compact
    } else if area.width < 120 || area.height < 40 {
        LayoutMode::Regular
    } else {
        LayoutMode::Wide
    }
}

pub struct AppLayout {
    pub content: Rect,
    pub input: Rect,
    pub status_info: Rect,
    pub status_tabs: Rect,
}

pub fn app_layout(area: Rect, app: &App) -> AppLayout {
    let show_input = app.tab == Tab::Chat;

    let input_height = match layout_mode(area) {
        LayoutMode::Compact => 2,
        LayoutMode::Regular | LayoutMode::Wide => 3,
    };

    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints(if show_input {
            vec![
                Constraint::Min(1),
                Constraint::Length(input_height),
                Constraint::Length(1), // status info
                Constraint::Length(1), // status tabs
            ]
        } else {
            vec![
                Constraint::Min(1),
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

pub fn render_terminal_too_small(f: &mut Frame, app: &App) {
    let area = f.area();
    let lines = vec![
        Line::from(Span::styled(
            "Terminal too small",
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::raw(format!(
            "Current: {}x{} · minimum: {}x{}",
            area.width,
            area.height,
            crate::ui::MIN_TERMINAL_WIDTH,
            crate::ui::MIN_TERMINAL_HEIGHT
        )),
        Line::raw(format!(
            "Resize the terminal to continue · {} quits",
            app.key_hint(ActionContext::Global, ActionId::QuitOrStop)
        )),
    ];
    let width = area.width.min(48);
    let height = area.height.min(5);
    let popup = Rect::new(
        area.x + area.width.saturating_sub(width) / 2,
        area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    );
    f.render_widget(Clear, popup);
    f.render_widget(
        Paragraph::new(lines)
            .block(Block::default().borders(Borders::ALL))
            .wrap(Wrap { trim: false }),
        popup,
    );
}

pub fn render_status_info(f: &mut Frame, area: Rect, app: &App) {
    let available = area.width as usize;
    if available == 0 {
        return;
    }
    let mut items: Vec<(String, Style)> = Vec::new();

    // Urgent run/question/error state comes first. Lower-priority model,
    // tokens and session metadata are appended only while cells remain.
    if app.chat.streaming {
        let tick = app.spinner_tick % theme::BRAILLE_SPINNER.len();
        let spinner = theme::BRAILLE_SPINNER[tick];
        items.push((
            format!("{spinner} RUNNING"),
            Style::default().fg(colors::tool_running()),
        ));
        items.push((
            "draft editable; Enter sends after run".to_string(),
            Style::default().fg(colors::inactive()),
        ));
    } else if app.pending_question.is_some() || app.dismissed_question.is_some() {
        items.push((
            "? QUESTION".to_string(),
            Style::default()
                .fg(colors::warning())
                .add_modifier(Modifier::BOLD),
        ));
    }

    if app.chat.unseen_updates > 0 {
        items.push((
            format!("↓ {} NEW", app.chat.unseen_updates),
            Style::default()
                .fg(colors::warning())
                .add_modifier(Modifier::BOLD),
        ));
    }

    if app.chat.plan_mode {
        items.push((
            "PLAN".to_string(),
            Style::default()
                .fg(colors::warning())
                .add_modifier(Modifier::BOLD),
        ));
    }

    if app.unseen_alerts > 0 {
        items.push((
            format!("! {} ALERTS", app.unseen_alerts),
            Style::default()
                .fg(colors::warning())
                .add_modifier(Modifier::BOLD),
        ));
    }

    let (connection, connection_style) = if app.connected {
        ("● ONLINE", Style::default().fg(colors::success()))
    } else {
        ("○ OFFLINE", Style::default().fg(colors::error()))
    };
    items.push((connection.to_string(), connection_style));

    if !app.status_message.is_empty() {
        items.push((
            app.status_message.clone(),
            Style::default().fg(colors::inactive()),
        ));
    }

    if !app.chat.model.is_empty() {
        items.push((
            format!("model {}", app.chat.model),
            Style::default().fg(colors::inactive()),
        ));
    }
    if let Some(usage) = &app.chat.token_usage {
        items.push((
            format!("tokens {}/{}", usage.completion_tokens, usage.total_tokens),
            Style::default().fg(colors::inactive()),
        ));
    }
    if let Some(sid) = &app.chat.session_id {
        items.push((
            format!("session {}", sid.chars().take(8).collect::<String>()),
            Style::default().fg(colors::inactive()),
        ));
    }

    let mut spans = Vec::new();
    let mut used = 1_usize;
    spans.push(Span::raw(" "));
    for (index, (text, style)) in items.into_iter().enumerate() {
        let separator = usize::from(index > 0) * 3;
        if used + separator >= available {
            break;
        }
        if index > 0 {
            spans.push(Span::styled(" · ", Style::default().fg(colors::subtle())));
            used += 3;
        }
        let remaining = available.saturating_sub(used);
        let clipped = clip_cells(&text, remaining);
        used += display_width(&clipped);
        spans.push(Span::styled(clipped, style));
        if used >= available {
            break;
        }
    }
    f.render_widget(Paragraph::new(Line::from(spans)), area);
}

pub fn render_tab_bar(f: &mut Frame, area: Rect, app: &App) {
    let mode = layout_mode(area);
    let full_width = Tab::ALL
        .iter()
        .enumerate()
        .map(|(index, tab)| {
            display_width(&format!(
                " [{}]{} ",
                app.primary_key_hint(ActionContext::Navigation, tab_switch_action(index)),
                tab.title()
            ))
        })
        .sum::<usize>()
        + Tab::ALL.len().saturating_sub(1);
    let use_full = mode != LayoutMode::Compact && full_width <= area.width as usize;
    let mut spans = Vec::new();

    if use_full {
        for (index, tab) in Tab::ALL.iter().enumerate() {
            let style = tab_style(*tab == app.tab);
            spans.push(Span::styled(
                format!(
                    " [{}]{} ",
                    app.primary_key_hint(ActionContext::Navigation, tab_switch_action(index)),
                    tab.title()
                ),
                style,
            ));
            if index < Tab::ALL.len() - 1 {
                spans.push(Span::raw(" "));
            }
        }
    } else {
        let active_index = Tab::ALL.iter().position(|tab| *tab == app.tab).unwrap_or(0);
        let active = format!(
            " [{}] {} ",
            app.primary_key_hint(ActionContext::Navigation, tab_switch_action(active_index)),
            app.tab.title()
        );
        let hint = format!(
            " · {}/{} views · {} help",
            app.key_hint(ActionContext::Global, ActionId::NextTab),
            app.key_hint(ActionContext::Global, ActionId::PreviousTab),
            app.key_hint(ActionContext::Global, ActionId::ShowHelp),
        );
        let active_width = display_width(&active);
        let remaining = area.width.saturating_sub(active_width as u16) as usize;
        spans.push(Span::styled(active, tab_style(true)));
        if remaining > 0 {
            spans.push(Span::styled(
                clip_cells(&hint, remaining),
                Style::default().fg(colors::inactive()),
            ));
        }
    }

    f.render_widget(Paragraph::new(Line::from(spans)), area);
}

fn tab_switch_action(index: usize) -> ActionId {
    [
        ActionId::SwitchTab1,
        ActionId::SwitchTab2,
        ActionId::SwitchTab3,
        ActionId::SwitchTab4,
        ActionId::SwitchTab5,
        ActionId::SwitchTab6,
    ]
    .get(index)
    .copied()
    .unwrap_or(ActionId::SwitchTab1)
}

fn tab_style(active: bool) -> Style {
    if active {
        Style::default()
            .fg(colors::brand())
            .add_modifier(Modifier::BOLD | Modifier::UNDERLINED)
    } else {
        Style::default().fg(colors::inactive())
    }
}

pub fn render_help(f: &mut Frame, app: &App) {
    let screen = f.area();
    const KEY_COL: usize = 24;
    let popup_width = ((screen.width as u32 * 94 / 100) as u16).max(1);
    let content_width = popup_width.saturating_sub(4) as usize;
    let two_columns = content_width >= 110;
    let resolved = app.help_entries();
    let mut entries = Vec::new();
    if two_columns {
        let midpoint = resolved.len().div_ceil(2);
        let (left, right) = resolved.split_at(midpoint);
        for index in 0..left.len().max(right.len()) {
            let left = left.get(index);
            let right = right.get(index);
            let lk = left.map(|entry| entry.keys.as_str()).unwrap_or("");
            let ld = left.map(|entry| entry.description.as_str()).unwrap_or("");
            let rk = right.map(|entry| entry.keys.as_str()).unwrap_or("");
            let rd = right.map(|entry| entry.description.as_str()).unwrap_or("");
            entries.push(format!("  {lk:<KEY_COL$}{ld:<36}{rk:<KEY_COL$}{rd}"));
        }
    } else {
        for entry in resolved {
            entries.push(format!("  {:<KEY_COL$}{}", entry.keys, entry.description));
        }
    }

    let height = screen.height.min(26);
    // The popup, not the terminal, is the limiting viewport. Reserve its two
    // border rows plus header/footer and both possible overflow indicators so
    // the close/scroll actions can never be pushed out on a tall narrow PTY.
    let inner_height = height.saturating_sub(2) as usize;
    let viewport = inner_height.saturating_sub(4).max(1);
    let max_scroll = entries.len().saturating_sub(viewport);
    let max_scroll = u16::try_from(max_scroll).unwrap_or(u16::MAX);
    app.help_max_scroll.set(max_scroll);
    let start = app.help_scroll.min(max_scroll) as usize;
    let end = (start + viewport).min(entries.len());
    let mut lines = vec![Line::from(Span::styled(
        format!(
            " Keybindings · rows {}-{} of {}",
            if entries.is_empty() { 0 } else { start + 1 },
            end,
            entries.len()
        ),
        Style::default()
            .fg(colors::brand())
            .add_modifier(Modifier::BOLD),
    ))];
    if start > 0 {
        lines.push(Line::raw(format!("  ↑ {start} earlier")));
    }
    lines.extend(
        entries[start..end]
            .iter()
            .map(|line| Line::raw(clip_cells(line, content_width))),
    );
    if end < entries.len() {
        lines.push(Line::raw(format!("  ↓ {} later", entries.len() - end)));
    }
    lines.push(Line::raw(format!(
        "  {}/{} · {}/{} · {} close",
        app.key_hint(ActionContext::Help, ActionId::NavigateUp),
        app.key_hint(ActionContext::Help, ActionId::NavigateDown),
        app.key_hint(ActionContext::Help, ActionId::PageUp),
        app.key_hint(ActionContext::Help, ActionId::PageDown),
        app.key_hint(ActionContext::Help, ActionId::Cancel),
    )));

    let area = centered_rect(94, height, screen);
    f.render_widget(Clear, area);
    let help = Paragraph::new(lines).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(colors::brand())),
    );
    f.render_widget(help, area);
}

/// Notification-log overlay (`Ctrl+L`): recent status messages newest-first,
/// colored by level, so errors/warnings aren't lost when the status line is
/// overwritten. Esc/q closes it; navigation keys scroll internally.
pub fn render_notifications(f: &mut Frame, app: &App) {
    let screen = f.area();
    let popup_width = ((screen.width as u32 * 94 / 100) as u16).max(1);
    let content_width = popup_width.saturating_sub(4) as usize;
    let mut entries: Vec<Line> = Vec::new();
    {
        let mut push_full_value = |label: &str, value: &str, color| {
            if value.is_empty() {
                return;
            }
            let prefix = format!("  {label}: ");
            let continuation = " ".repeat(display_width(&prefix));
            let text_width = content_width.saturating_sub(display_width(&prefix)).max(1);
            let wrapped = crate::text::wrapped_lines(value, text_width);
            entries.push(Line::from(vec![
                Span::styled(prefix, Style::default().fg(colors::subtle())),
                Span::styled(
                    wrapped.first().cloned().unwrap_or_default(),
                    Style::default().fg(color),
                ),
            ]));
            entries.extend(wrapped.into_iter().skip(1).map(|line| {
                Line::from(vec![
                    Span::raw(continuation.clone()),
                    Span::styled(line, Style::default().fg(color)),
                ])
            }));
        };

        push_full_value(
            "session",
            app.chat.session_id.as_deref().unwrap_or(""),
            colors::inactive(),
        );
        push_full_value("model", &app.chat.model, colors::inactive());
        if let Some(provider) = app.chat.provider.as_deref() {
            push_full_value("model provider", provider, colors::inactive());
        }
        push_full_value("status", &app.status_message, colors::inactive());
        if let Some((id, title)) = &app.pending_delete {
            push_full_value("delete session id", id, colors::error());
            push_full_value("delete session title", title, colors::error());
        }
        if let Some((id, name)) = &app.pending_schedule_delete {
            push_full_value("delete schedule id", id, colors::error());
            push_full_value("delete schedule name", name, colors::error());
        }
        if let Some(offer) = &app.serve_offer {
            push_full_value("server URL", &offer.url, colors::warning());
        }
        if let Some(session) = app
            .session_picker
            .as_ref()
            .and_then(|picker| picker.selected_session())
        {
            push_full_value("selected session id", &session.id, colors::inactive());
            push_full_value("selected session title", &session.title, colors::inactive());
            push_full_value("selected session model", &session.model, colors::inactive());
        }
        if let Some(model) = app
            .model_picker
            .as_ref()
            .and_then(|picker| picker.selected_model())
        {
            push_full_value(
                "selected model name",
                &model.display_name,
                colors::inactive(),
            );
            push_full_value(
                "selected model provider",
                &model.provider_display_name,
                colors::inactive(),
            );
            push_full_value(
                "selected model id",
                &format!("{}/{}", model.reference.provider, model.reference.model),
                colors::inactive(),
            );
        }
        let current_error = match app.tab {
            Tab::Chat => None,
            Tab::Sessions => app.sessions.error.as_deref(),
            Tab::Mcp => app.mcp.error.as_deref(),
            Tab::Schedules => app.schedules.error.as_deref(),
            Tab::Skills => app.skills.error.as_deref(),
            Tab::Config => app.config.error.as_deref(),
        };
        if let Some(error) = current_error {
            push_full_value("view error", error, colors::error());
        }
        if let Some(error) = app
            .pending_question
            .as_ref()
            .and_then(|question| question.error.as_deref())
        {
            push_full_value("question error", error, colors::error());
        }
        if let Some(error) = app
            .model_picker
            .as_ref()
            .and_then(|picker| picker.error.as_deref())
        {
            push_full_value("model error", error, colors::error());
        }
        if let Some(error) = app
            .session_picker
            .as_ref()
            .and_then(|picker| picker.error.as_deref())
        {
            push_full_value("session error", error, colors::error());
        }
        if let Some(picker) = &app.session_picker {
            let mutation_error = match &picker.mode {
                SessionPickerMode::Rename { error, .. }
                | SessionPickerMode::Pinning { error, .. } => error.as_deref(),
                SessionPickerMode::Browse => None,
            };
            if let Some(error) = mutation_error {
                push_full_value("session mutation error", error, colors::error());
            }
        }
        if let Some(error) = app
            .command_palette
            .as_ref()
            .and_then(|palette| palette.error.as_deref())
        {
            push_full_value("command error", error, colors::error());
        }
        if let Some(error) = app
            .config_editor
            .as_ref()
            .and_then(|editor| editor.error.as_deref())
        {
            push_full_value("editor error", error, colors::error());
        }
        if let Some(error) = app
            .schedule_form
            .as_ref()
            .and_then(|form| form.error.as_deref())
        {
            push_full_value("form error", error, colors::error());
        }
    }

    if !entries.is_empty() && !app.notifications.is_empty() {
        entries.push(Line::raw(""));
    }
    if app.notifications.is_empty() && entries.is_empty() {
        entries.push(Line::raw("  (nothing yet)"));
    } else {
        for n in app.notifications.iter().rev() {
            let (tag, color) = match n.level {
                NoticeLevel::Info => ("info", colors::inactive()),
                NoticeLevel::Warn => ("warn", colors::warning()),
                NoticeLevel::Error => ("err ", colors::error()),
            };
            let marker = match n.level {
                NoticeLevel::Info => "i",
                NoticeLevel::Warn => "!",
                NoticeLevel::Error => "x",
            };
            let prefix = format!("  {} {marker} {tag}  ", n.at.format("%H:%M:%S"));
            let continuation = " ".repeat(display_width(&prefix));
            let text_width = content_width.saturating_sub(display_width(&prefix)).max(1);
            let wrapped = crate::text::wrapped_lines(&n.text, text_width);
            entries.push(Line::from(vec![
                Span::styled(prefix, Style::default().fg(colors::subtle())),
                Span::styled(
                    wrapped.first().cloned().unwrap_or_default(),
                    Style::default().fg(color),
                ),
            ]));
            entries.extend(wrapped.into_iter().skip(1).map(|line| {
                Line::from(vec![
                    Span::raw(continuation.clone()),
                    Span::styled(line, Style::default().fg(color)),
                ])
            }));
        }
    }

    let height = screen.height.min(32);
    let inner_height = height.saturating_sub(2) as usize;
    // Header, fixed action footer and both optional overflow indicators.
    let viewport_rows = inner_height.saturating_sub(4).max(1);
    let max_scroll = entries.len().saturating_sub(viewport_rows);
    let max_scroll = u16::try_from(max_scroll).unwrap_or(u16::MAX);
    app.notification_max_scroll.set(max_scroll);
    let start = app.notification_scroll.min(max_scroll) as usize;
    let end = (start + viewport_rows).min(entries.len());
    let mut lines = vec![Line::from(Span::styled(
        format!(
            " Notifications · rows {}-{} of {}",
            if entries.is_empty() { 0 } else { start + 1 },
            end,
            entries.len()
        ),
        Style::default()
            .fg(colors::brand())
            .add_modifier(Modifier::BOLD),
    ))];
    if start > 0 {
        lines.push(Line::raw(format!("  ↑ {start} newer lines")));
    }
    lines.extend(entries[start..end].iter().cloned());
    if end < entries.len() {
        lines.push(Line::raw(format!(
            "  ↓ {} older lines",
            entries.len() - end
        )));
    }
    lines.push(Line::raw(format!(
        "  {}/{} · {}/{} · {} close",
        app.key_hint(ActionContext::Notifications, ActionId::NavigateUp),
        app.key_hint(ActionContext::Notifications, ActionId::NavigateDown),
        app.key_hint(ActionContext::Notifications, ActionId::PageUp),
        app.key_hint(ActionContext::Notifications, ActionId::PageDown),
        app.key_hint(ActionContext::Notifications, ActionId::Cancel),
    )));

    let area = centered_rect(94, height, screen);
    f.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(colors::brand()))
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

    let popup_width = ((f.area().width as u32 * 90 / 100) as u16).max(1);
    let value_width = popup_width.saturating_sub(6) as usize;
    let lines = vec![
        Line::from(Span::styled(
            " Local server not reachable",
            Style::default()
                .fg(colors::warning())
                .add_modifier(Modifier::BOLD),
        )),
        Line::raw(""),
        Line::raw(format!(
            "  {}",
            crate::text::clip_cells(&offer.url, value_width)
        )),
        Line::raw(format!(
            "  {} inspect full URL",
            app.key_hint(ActionContext::Global, ActionId::ShowNotifications)
        )),
        Line::raw(""),
        Line::raw("  Start a local `bamboo serve`?"),
        Line::raw(""),
        Line::raw(format!(
            "  {} start",
            app.key_hint(ActionContext::ServeOffer, ActionId::Confirm)
        )),
        Line::raw(format!(
            "  {} skip",
            app.key_hint(ActionContext::ServeOffer, ActionId::Reject)
        )),
    ];

    let height = (lines.len() as u16 + 2).min(f.area().height);
    let area = centered_rect(90, height, f.area());
    f.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(colors::warning()))
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
        Style::default().fg(colors::warning()),
    ))
}

fn identity_syncing_hint() -> Line<'static> {
    Line::from(Span::styled(
        "  Synchronizing exact question identity\u{2026}",
        Style::default().fg(colors::warning()),
    ))
}

fn hard_wrap_preview(value: &str, width: usize, max_lines: usize) -> (Vec<String>, bool) {
    if max_lines == 0 {
        return (Vec::new(), !value.is_empty());
    }
    let mut output = crate::text::hard_wrap(value, width);
    let truncated = output.len() > max_lines;
    output.truncate(max_lines);
    (output, truncated)
}

fn ellipsize(value: &str, width: usize) -> String {
    crate::text::clip_cells(value, width.max(1))
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
            .border_style(Style::default().fg(colors::brand()))
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
                        .fg(colors::brand())
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
        let inspect_scroll = format!(
            " {}/{} or {}/{} scroll",
            app.key_hint(ActionContext::QuestionInspect, ActionId::NavigateUp),
            app.key_hint(ActionContext::QuestionInspect, ActionId::NavigateDown),
            app.key_hint(ActionContext::QuestionInspect, ActionId::PageUp),
            app.key_hint(ActionContext::QuestionInspect, ActionId::PageDown),
        );
        let footer = if q.options.is_empty() {
            vec![
                Line::raw(inspect_scroll),
                Line::raw(format!(
                    " {} copy exact",
                    app.key_hint(ActionContext::QuestionInspect, ActionId::CopyValue)
                )),
                Line::raw(format!(
                    " {} back",
                    app.key_hint(ActionContext::QuestionInspect, ActionId::Cancel)
                )),
            ]
        } else {
            vec![
                Line::raw(inspect_scroll),
                Line::raw(format!(
                    " {} question/option",
                    app.key_hint(
                        ActionContext::QuestionInspect,
                        ActionId::ToggleInspectorPane
                    )
                )),
                Line::raw(format!(
                    " {} copy exact · {} back",
                    app.key_hint(ActionContext::QuestionInspect, ActionId::CopyValue),
                    app.key_hint(ActionContext::QuestionInspect, ActionId::Cancel),
                )),
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
            .fg(colors::brand())
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
        header.push(Line::raw(format!(
            "  …  ({} inspect full question)",
            app.key_hint(ActionContext::QuestionOptions, ActionId::InspectValue)
        )));
    }
    header.push(Line::raw(""));

    let mut body = Vec::new();
    let mut option_line_positions = Vec::new();
    if let Some(entry) = &q.number_entry {
        body.push(Line::raw(format!("  Go to option #: {entry}▏")));
        body.push(Line::raw(""));
        body.push(Line::raw(format!(
            "  digits type · {} select",
            app.key_hint(ActionContext::QuestionNumber, ActionId::Activate)
        )));
        body.push(Line::raw(format!(
            "  {} edit · {} cancel",
            app.key_hint(ActionContext::QuestionNumber, ActionId::Backspace),
            app.key_hint(ActionContext::QuestionNumber, ActionId::Cancel),
        )));
    } else if let Some(buf) = &q.custom {
        body.push(Line::raw("  Custom answer:"));
        body.push(Line::from(Span::styled(
            format!(
                "  > {}▏",
                crate::text::clip_tail_cells(buf, text_width.saturating_sub(1))
            ),
            Style::default().fg(colors::brand()),
        )));
        body.push(Line::raw(""));
        if q.identity_syncing {
            body.push(identity_syncing_hint());
        } else if q.submitting {
            body.push(submitting_hint());
        } else if q.options.is_empty() {
            body.push(Line::raw(format!(
                "  {} answer · {} dismiss",
                app.key_hint(ActionContext::QuestionCustom, ActionId::Activate),
                app.key_hint(ActionContext::QuestionCustom, ActionId::Cancel),
            )));
            body.push(Line::raw(format!(
                "  {} inspect/copy question",
                app.key_hint(ActionContext::QuestionCustom, ActionId::InspectValue)
            )));
        } else {
            body.push(Line::raw(format!(
                "  {} answer · {} options",
                app.key_hint(ActionContext::QuestionCustom, ActionId::Activate),
                app.key_hint(ActionContext::QuestionCustom, ActionId::Cancel),
            )));
            body.push(Line::raw(format!(
                "  {} inspect/copy question",
                app.key_hint(ActionContext::QuestionCustom, ActionId::InspectValue)
            )));
        }
    } else if q.options.is_empty() {
        body.push(Line::from(Span::styled(
            "  No selectable answers were supplied and custom input is disabled.",
            Style::default().fg(colors::warning()),
        )));
        body.push(Line::raw(""));
        body.push(Line::raw(format!(
            "  {} inspect/copy question · {} dismiss",
            app.key_hint(ActionContext::QuestionOptions, ActionId::InspectValue),
            app.key_hint(ActionContext::QuestionOptions, ActionId::Cancel),
        )));
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
                    .fg(colors::brand())
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
            body.push(Line::raw(format!(
                "  click option · {}/{} or wheel select",
                app.key_hint(ActionContext::QuestionOptions, ActionId::NavigateUp),
                app.key_hint(ActionContext::QuestionOptions, ActionId::NavigateDown),
            )));
            body.push(Line::raw(format!(
                "  {} answer · {} dismiss · quick {}…{}",
                app.key_hint(ActionContext::QuestionOptions, ActionId::Activate),
                app.key_hint(ActionContext::QuestionOptions, ActionId::Cancel),
                app.key_hint(ActionContext::QuestionOptions, ActionId::QuickAnswer1),
                app.key_hint(ActionContext::QuestionOptions, ActionId::QuickAnswer9),
            )));
            body.push(Line::raw(format!(
                "  {} number · {} inspect · {} copy",
                app.key_hint(ActionContext::QuestionOptions, ActionId::NumberAnswer),
                app.key_hint(ActionContext::QuestionOptions, ActionId::InspectValue),
                app.key_hint(ActionContext::QuestionOptions, ActionId::CopyValue),
            )));
            if q.allow_custom {
                body.push(Line::raw(format!(
                    "  {} custom answer",
                    app.key_hint(ActionContext::QuestionOptions, ActionId::CustomAnswer)
                )));
            }
        }
    }
    if let Some(error) = &q.error {
        body.push(Line::from(Span::styled(
            format!(
                "  Error: {}",
                ellipsize(error, text_width.saturating_sub(7))
            ),
            Style::default().fg(colors::error()),
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
        .border_style(Style::default().fg(colors::brand()))
        .title(" Clarification ");
    f.render_widget(Paragraph::new(lines).block(block), area);
}

/// Modal confirming a destructive delete from the Sessions or Schedules tab.
/// A destructive action must not fire on a single stray keystroke, so it
/// stops here until `y`/Enter or `n`/Esc.
pub fn render_delete_confirm(f: &mut Frame, app: &App) {
    let (kind, title) = if let Some((_, title)) = &app.pending_delete {
        ("session", title)
    } else if let Some((_, name)) = &app.pending_schedule_delete {
        ("schedule", name)
    } else {
        return;
    };
    let display_title: &str = if title.is_empty() {
        "(untitled)"
    } else {
        title
    };

    let screen = f.area();
    let popup_width = ((screen.width as u32 * 90 / 100) as u16).max(1);
    let text_width = popup_width.saturating_sub(6) as usize;
    let (title_lines, title_truncated) = hard_wrap_preview(display_title, text_width, 4);
    let mut lines = vec![
        Line::from(Span::styled(
            format!(" Delete {kind}?"),
            Style::default()
                .fg(colors::error())
                .add_modifier(Modifier::BOLD),
        )),
        Line::raw(""),
    ];
    lines.extend(
        title_lines
            .into_iter()
            .map(|line| Line::raw(format!("  {line}"))),
    );
    if title_truncated {
        lines.push(Line::raw("  … title shortened for this screen"));
    }
    lines.push(Line::raw(""));
    lines.push(Line::raw("  This cannot be undone."));
    lines.push(Line::raw(""));
    let context = if kind == "session" {
        ActionContext::SessionDeleteConfirm
    } else {
        ActionContext::ScheduleDeleteConfirm
    };
    lines.push(Line::raw(format!(
        "  {} confirm · {} cancel",
        app.key_hint(context, ActionId::Confirm),
        app.key_hint(context, ActionId::Reject),
    )));

    let height = (lines.len() as u16 + 2).min(screen.height);
    let area = centered_rect(90, height, screen);
    f.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(colors::error()))
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
    let screen = f.area();
    let popup_width = ((screen.width as u32 * 90 / 100) as u16).max(1);
    let value_width = popup_width.saturating_sub(16) as usize;
    let fields = [
        ("Name", &form.name),
        ("Cron", &form.cron),
        ("Prompt", &form.prompt),
    ];
    let mut lines: Vec<Line> = vec![
        Line::from(Span::styled(
            " New schedule",
            Style::default()
                .fg(colors::brand())
                .add_modifier(Modifier::BOLD),
        )),
        Line::raw(""),
    ];
    for (i, (label, val)) in fields.iter().enumerate() {
        let focused = i == form.field;
        let cursor = if focused { "\u{258f}" } else { "" };
        let style = if focused {
            Style::default().fg(colors::brand())
        } else {
            Style::default().fg(colors::inactive())
        };
        lines.push(Line::from(vec![
            Span::styled(
                format!("  {label:<7}: "),
                Style::default().fg(colors::subtle()),
            ),
            Span::styled(
                if focused {
                    format!(
                        "{}{}",
                        clip_tail_cells(val, value_width.saturating_sub(1)),
                        cursor
                    )
                } else {
                    clip_cells(val, value_width)
                },
                style,
            ),
        ]));
    }
    if let Some(error) = &form.error {
        lines.push(Line::from(Span::styled(
            format!(
                "  ! {}",
                clip_cells(error, popup_width.saturating_sub(6) as usize)
            ),
            Style::default().fg(colors::error()),
        )));
    }
    lines.push(Line::raw(""));
    lines.push(Line::raw(format!(
        "  {}/{} field · {} create · {} cancel",
        app.primary_key_hint(ActionContext::ScheduleForm, ActionId::NextField),
        app.primary_key_hint(ActionContext::ScheduleForm, ActionId::PreviousField),
        app.primary_key_hint(ActionContext::ScheduleForm, ActionId::Activate),
        app.primary_key_hint(ActionContext::ScheduleForm, ActionId::Cancel),
    )));

    let height = (lines.len() as u16 + 2).min(screen.height);
    let area = centered_rect(90, height, screen);
    f.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(colors::brand()))
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
            .fg(colors::brand())
            .add_modifier(Modifier::BOLD),
    ))];
    // Search owns keyboard focus only in browse mode. Rename has its own
    // title editor and cursor, while pinning owns all input as an action
    // state; showing the search cursor in those modes would imply two active
    // fields even though keystrokes can reach only one of them.
    if matches!(&picker.mode, SessionPickerMode::Browse) {
        lines.push(Line::from(vec![
            Span::styled(" Search: ", Style::default().fg(colors::subtle())),
            Span::styled(
                format!(
                    "{}▏",
                    clip_tail_cells(&picker.query, row_width.saturating_sub(10).max(1) as usize)
                ),
                Style::default().fg(colors::brand()),
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
                let total = picker.visible.len();
                // Reserve the border, header/search/blank, optional error,
                // loaded count, both fixed action rows, and both possible
                // above/below indicators before allocating list rows.
                let fixed_rows = 2 // border
                    + lines.len()
                    + usize::from(picker.error.is_some())
                    + 3; // loaded count + two action rows
                let remaining = (screen.height as usize).saturating_sub(fixed_rows);
                let selected = picker.selected.min(total.saturating_sub(1));
                let mut start = selected;
                let mut end = selected + 1;
                loop {
                    let indicators = usize::from(start > 0) + usize::from(end < total);
                    let mut expanded = false;
                    if start > 0 && end - (start - 1) + indicators <= remaining {
                        start -= 1;
                        expanded = true;
                    }
                    let indicators = usize::from(start > 0) + usize::from(end < total);
                    if end < total && (end + 1) - start + indicators <= remaining {
                        end += 1;
                        expanded = true;
                    }
                    if !expanded {
                        break;
                    }
                }
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
                    Style::default().fg(colors::error()),
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
                Style::default().fg(colors::subtle()),
            )));
            lines.push(Line::raw(format!(
                "  {}/{} · {} open · {} rename · {} pin",
                app.primary_key_hint(ActionContext::SessionPickerBrowse, ActionId::NavigateUp),
                app.primary_key_hint(ActionContext::SessionPickerBrowse, ActionId::NavigateDown),
                app.primary_key_hint(ActionContext::SessionPickerBrowse, ActionId::Activate),
                app.primary_key_hint(ActionContext::SessionPickerBrowse, ActionId::RenameSession),
                app.primary_key_hint(
                    ActionContext::SessionPickerBrowse,
                    ActionId::ToggleSessionPin
                ),
            )));
            lines.push(Line::raw(format!(
                "  {} del · {} more · {} retry · {}",
                app.primary_key_hint(
                    ActionContext::SessionPickerBrowse,
                    ActionId::DeleteSelection
                ),
                app.primary_key_hint(ActionContext::SessionPickerBrowse, ActionId::LoadMore),
                app.primary_key_hint(ActionContext::SessionPickerBrowse, ActionId::Refresh),
                app.primary_key_hint(ActionContext::SessionPickerBrowse, ActionId::Cancel),
            )));
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
                    .fg(colors::brand())
                    .add_modifier(Modifier::BOLD),
            )));
            lines.push(Line::from(vec![
                Span::styled("  Title: ", Style::default().fg(colors::subtle())),
                Span::styled(
                    if *submitting {
                        clip_tail_cells(draft, row_width.saturating_sub(12) as usize)
                    } else {
                        format!(
                            "{}▏",
                            clip_tail_cells(draft, row_width.saturating_sub(12) as usize)
                        )
                    },
                    Style::default().fg(colors::brand()),
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
                    Style::default().fg(colors::error()),
                )));
            }
            if !*submitting {
                lines.push(Line::raw(""));
                lines.push(Line::raw(format!(
                    "  {} save · {} refetch/retry · {} keep old title",
                    app.key_hint(ActionContext::SessionPickerRename, ActionId::Activate),
                    app.key_hint(ActionContext::SessionPickerRename, ActionId::Refresh),
                    app.key_hint(ActionContext::SessionPickerRename, ActionId::Cancel),
                )));
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
                    Style::default().fg(colors::error()),
                )));
            }
            if !*submitting {
                lines.push(Line::raw(""));
                lines.push(Line::raw(format!(
                    "  {} refetch/retry · {} cancel",
                    app.key_hint(ActionContext::SessionPickerPinning, ActionId::Refresh),
                    app.key_hint(ActionContext::SessionPickerPinning, ActionId::Cancel),
                )));
            }
        }
    }

    let height = (lines.len() as u16 + 2).min(screen.height);
    let area = centered_rect(94, height, screen);
    f.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(colors::brand()))
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
                .fg(colors::brand())
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(vec![
            Span::styled(" Search: ", Style::default().fg(colors::subtle())),
            Span::styled(
                format!(
                    "{}▏",
                    clip_tail_cells(&picker.query, row_width.saturating_sub(10).max(1))
                ),
                Style::default().fg(colors::brand()),
            ),
        ]),
        Line::raw(""),
    ];

    let mut body: Vec<Line> = Vec::new();
    if picker.loading && picker.models.is_empty() {
        body.push(Line::raw("  Loading models..."));
        body.push(Line::raw(""));
        body.push(Line::raw(format!(
            "  {} cancel",
            app.key_hint(ActionContext::ModelPicker, ActionId::Cancel)
        )));
    } else if picker.loading && picker.visible.is_empty() {
        body.push(Line::raw("  Refreshing model catalog..."));
        body.push(Line::raw(""));
        body.push(Line::raw(format!(
            "  {} cancel",
            app.key_hint(ActionContext::ModelPicker, ActionId::Cancel)
        )));
    } else if picker.visible.is_empty() {
        body.push(Line::raw(if picker.query.is_empty() {
            "  No models available"
        } else {
            "  No models match this search"
        }));
        body.push(Line::raw(""));
        if row_width < 70 && picker.models.is_empty() {
            body.push(Line::raw(format!(
                "  Edit search · {} retry load",
                app.key_hint(ActionContext::ModelPicker, ActionId::Refresh)
            )));
            body.push(Line::raw(format!(
                "  {} cancel",
                app.key_hint(ActionContext::ModelPicker, ActionId::Cancel)
            )));
        } else if row_width < 70 {
            body.push(Line::raw(format!(
                "  Edit search · {} clear",
                app.key_hint(ActionContext::ModelPicker, ActionId::ClearInput)
            )));
            body.push(Line::raw(format!(
                "  {} refresh · {} cancel",
                app.key_hint(ActionContext::ModelPicker, ActionId::Refresh),
                app.key_hint(ActionContext::ModelPicker, ActionId::Cancel),
            )));
        } else {
            body.push(Line::raw(if picker.models.is_empty() {
                format!(
                    "  Edit search · {} retry load · {} cancel",
                    app.key_hint(ActionContext::ModelPicker, ActionId::Refresh),
                    app.key_hint(ActionContext::ModelPicker, ActionId::Cancel),
                )
            } else {
                format!(
                    "  Edit search · {} clear · {} refresh · {} cancel",
                    app.key_hint(ActionContext::ModelPicker, ActionId::ClearInput),
                    app.key_hint(ActionContext::ModelPicker, ActionId::Refresh),
                    app.key_hint(ActionContext::ModelPicker, ActionId::Cancel),
                )
            }));
        }
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
        // around the selection using the actual row cost, while reserving
        // border, indicators, action footer, and an optional error. This keeps
        // both the highlighted model and recovery action visible on a short
        // screen even with 100 providers.
        let line_budget = (max_h as usize)
            .saturating_sub(2 + header.len() + 2 + usize::from(picker.error.is_some()))
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
                        .fg(colors::subtle())
                        .add_modifier(Modifier::BOLD),
                )));
                previous_group = Some(group);
            }
            let selected = i == picker.selected;
            let marker = if selected { "\u{203a}" } else { " " };
            let style = if selected {
                Style::default()
                    .fg(colors::brand())
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
            "  Applying model...".to_string()
        } else {
            format!(
                "  {}/{} or wheel select · {} apply · {} cancel",
                app.key_hint(ActionContext::ModelPicker, ActionId::NavigateUp),
                app.key_hint(ActionContext::ModelPicker, ActionId::NavigateDown),
                app.key_hint(ActionContext::ModelPicker, ActionId::Activate),
                app.key_hint(ActionContext::ModelPicker, ActionId::Cancel),
            )
        }));
    }
    if let Some(error) = &picker.error {
        body.push(Line::from(Span::styled(
            clip_cells(&format!("  {error}"), row_width),
            Style::default().fg(colors::error()),
        )));
    }

    let mut lines = header;
    lines.extend(body);
    let height = (lines.len() as u16 + 2).min(screen.height);
    let area = centered_rect(92, height, screen);
    f.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(colors::brand()))
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
    let binding_hints = palette
        .entries
        .iter()
        .map(|entry| match entry {
            CommandPaletteEntry::Builtin(action) => app.action_hint(*action),
            CommandPaletteEntry::Server(_) => None,
        })
        .collect::<Vec<_>>();
    let footer = [
        format!(
            "  {}/{} or wheel select · {} use · {} cancel",
            app.key_hint(ActionContext::CommandPalette, ActionId::NavigateUp),
            app.key_hint(ActionContext::CommandPalette, ActionId::NavigateDown),
            app.key_hint(ActionContext::CommandPalette, ActionId::Activate),
            app.key_hint(ActionContext::CommandPalette, ActionId::Cancel),
        ),
        format!(
            "  Type to search · {} refresh · {} clear",
            app.key_hint(ActionContext::CommandPalette, ActionId::Refresh),
            app.key_hint(ActionContext::CommandPalette, ActionId::ClearInput),
        ),
        format!(
            "  {} cancel",
            app.key_hint(ActionContext::CommandPalette, ActionId::Cancel)
        ),
    ];
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
        binding_hints: &binding_hints,
        footer: Some(&footer),
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
    binding_hints: &'a [Option<String>],
    footer: Option<&'a [String; 3]>,
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
        if view.resolving {
            hitboxes.borrow_mut().clear();
        } else {
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
    }

    f.render_widget(Clear, area);
    let title = match view.trigger {
        CommandPaletteTrigger::Slash => " Slash commands ",
        CommandPaletteTrigger::Global => " Command palette ",
    };
    let border_color = if view.resolving {
        colors::warning()
    } else {
        colors::brand()
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
    let search_cursor = if view.resolving { "" } else { "▏" };
    let search_style = if view.resolving {
        Style::default().fg(colors::inactive())
    } else {
        Style::default().fg(colors::brand())
    };
    let mut lines = vec![
        Line::from(Span::styled(
            title,
            Style::default()
                .fg(colors::brand())
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(vec![
            Span::styled(" Search: ", Style::default().fg(colors::subtle())),
            Span::styled(
                format!(
                    "{}{}{}",
                    query_prefix,
                    clip_tail_cells(view.input, search_width.saturating_sub(query_prefix.len())),
                    search_cursor
                ),
                search_style,
            ),
        ]),
        Line::raw(""),
    ];

    if view.resolving {
        lines.push(Line::from(Span::styled(
            "  Resolving preview…",
            Style::default().fg(colors::warning()),
        )));
    } else if view.loading {
        lines.push(Line::from(Span::styled(
            clip_cells(
                "  Loading session commands… built-ins remain available",
                row_width,
            ),
            Style::default().fg(colors::inactive()),
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
            Style::default().fg(colors::inactive()),
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
                Style::default().fg(colors::subtle()),
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
            let is_selected = !view.resolving && visible_index == selected;
            let marker = if is_selected { "›" } else { " " };
            let name_style = if view.resolving {
                Style::default().fg(colors::inactive())
            } else if is_selected {
                Style::default()
                    .fg(colors::brand())
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
            let description = view
                .binding_hints
                .get(*entry_index)
                .and_then(Option::as_deref)
                .map(|hint| format!("[{hint}] {description}"))
                .unwrap_or(description);
            lines.push(Line::from(Span::styled(
                clip_cells(&format!("      {description}"), row_width),
                if disabled.is_some() {
                    Style::default().fg(colors::error())
                } else {
                    Style::default().fg(colors::inactive())
                },
            )));
            if !view.resolving {
                item_rows.push((visible_index, first_row, 2));
            }
        }
        if end < total {
            lines.push(Line::from(Span::styled(
                format!("  ↓ {} more", total - end),
                Style::default().fg(colors::subtle()),
            )));
        }
    }

    if let Some(error) = view.error {
        lines.push(Line::from(Span::styled(
            clip_cells(&format!("  {error}"), row_width),
            Style::default().fg(colors::error()),
        )));
    }
    lines.push(Line::raw(""));
    if view.resolving {
        lines.push(Line::raw("  Input paused while the preview resolves"));
        lines.push(Line::raw(
            view.footer
                .map(|footer| footer[2].clone())
                .unwrap_or_else(|| "  Esc cancel".to_string()),
        ));
    } else if row_width < 70 {
        lines.push(Line::raw(
            view.footer
                .map(|footer| footer[0].clone())
                .unwrap_or_else(|| "  ↑/↓/wheel select · Enter use · Esc cancel".to_string()),
        ));
        lines.push(Line::raw(
            view.footer
                .map(|footer| footer[1].clone())
                .unwrap_or_else(|| "  Ctrl+R retry/refresh · Ctrl+U clear".to_string()),
        ));
    } else {
        lines.push(Line::raw(
            view.footer
                .map(|footer| footer[0].clone())
                .unwrap_or_else(|| {
                    "  ↑/↓/PgUp/PgDn/wheel select · Enter use · Esc cancel".to_string()
                }),
        ));
        lines.push(Line::raw(
            view.footer
                .map(|footer| footer[1].clone())
                .unwrap_or_else(|| "  Type to search · Ctrl+R refresh · Ctrl+U clear".to_string()),
        ));
    }

    CommandPaletteRender { lines, item_rows }
}

fn palette_type_style(command_type: &str) -> Style {
    let color = match command_type {
        "prompt" => colors::brand(),
        "workflow" => colors::success(),
        "skill" => colors::warning(),
        "mcp" => colors::tool_running(),
        _ => colors::inactive(),
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

fn display_width(value: &str) -> usize {
    crate::text::display_width(value)
}

fn clip_cells(value: &str, max_width: usize) -> String {
    crate::text::clip_cells(value, max_width)
}

fn clip_tail_cells(value: &str, max_width: usize) -> String {
    crate::text::clip_tail_cells(value, max_width)
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
    use crate::app::{App, CommandPaletteEntry, CommandPaletteHitbox, CommandPaletteTrigger};
    use crate::keymap::{ActionId, Keymap};
    use ratatui::backend::TestBackend;
    use ratatui::style::Color;
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

    /// Stable FNV-1a digest over every rendered cell (symbol, colours and
    /// modifiers). Unlike substring smoke assertions, a border shift, clipped
    /// footer or semantic-style regression changes the checked-in golden.
    fn buffer_fingerprint(terminal: &Terminal<TestBackend>) -> u64 {
        let buffer = terminal.backend().buffer();
        let mut hash = 0xcbf29ce484222325_u64;
        let mut feed = |bytes: &[u8]| {
            for byte in bytes {
                hash ^= u64::from(*byte);
                hash = hash.wrapping_mul(0x100000001b3);
            }
        };
        feed(&buffer.area.width.to_le_bytes());
        feed(&buffer.area.height.to_le_bytes());
        for cell in buffer.content() {
            feed(cell.symbol().as_bytes());
            feed(&[0]);
            feed(format!("{:?}|{:?}|{:?}", cell.fg, cell.bg, cell.modifier).as_bytes());
            feed(&[0xff]);
        }
        hash
    }

    #[test]
    fn tab_bar_uses_the_resolved_navigation_binding() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        let keymap = Keymap::from_json(
            r#"{"bindings":[
                {"context":"navigation","action":"switch-tab-1","keys":["F8"]}
            ]}"#,
        )
        .unwrap();
        app.set_keymap(keymap, None);

        let backend = TestBackend::new(120, 1);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| super::render_tab_bar(frame, frame.area(), &app))
            .unwrap();
        let text = terminal_text(&terminal);
        assert!(text.contains("[F8] Chat"), "{text}");
        assert!(!text.contains("[1] Chat"), "{text}");
    }

    #[test]
    fn testbackend_goldens_cover_every_view_across_the_size_matrix() {
        let sizes = [(50, 15), (60, 20), (80, 24), (120, 40), (200, 60)];
        // Row-major: size, then `Tab::ALL`. Regenerate deliberately only when
        // the complete rendered buffers have been reviewed.
        let expected: [[u64; 6]; 5] = [
            [
                6_299_991_410_263_413_121,
                6_299_991_410_263_413_121,
                6_299_991_410_263_413_121,
                6_299_991_410_263_413_121,
                6_299_991_410_263_413_121,
                6_299_991_410_263_413_121,
            ],
            [
                4_879_774_826_948_300_008,
                11_066_184_476_930_803_934,
                17_031_078_274_172_910_387,
                1_143_711_117_080_246_334,
                7_234_780_119_466_243_444,
                9_041_972_994_219_815_988,
            ],
            [
                7_389_017_022_641_217_323,
                16_952_033_003_801_489_607,
                16_463_315_415_164_233_496,
                5_166_663_855_769_301_145,
                6_250_935_511_946_659_016,
                16_254_299_977_599_291_151,
            ],
            [
                15_031_301_406_181_041_675,
                4_865_931_917_107_133_743,
                15_642_408_912_939_561_496,
                12_364_154_055_185_087_953,
                8_288_241_310_337_659_808,
                16_620_483_292_143_505_655,
            ],
            [
                10_507_887_664_888_193_887,
                7_972_046_481_146_137_035,
                48_418_775_223_067_332,
                9_519_166_218_506_624_621,
                6_377_689_178_460_246_140,
                14_361_221_358_384_919_491,
            ],
        ];
        let mut actual = [[0_u64; 6]; 5];
        for (size_index, (width, height)) in sizes.into_iter().enumerate() {
            for (tab_index, tab) in crate::app::Tab::ALL.into_iter().enumerate() {
                let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
                app.tab = tab;
                app.connected = true;
                app.chat.model = "provider/模型🧭e\u{301}".to_string();
                app.chat.session_id = Some("session-full-identifier".to_string());
                app.status_message = "deterministic status".to_string();
                let backend = TestBackend::new(width, height);
                let mut terminal = Terminal::new(backend).unwrap();
                terminal
                    .draw(|frame| crate::ui::render(frame, &app))
                    .unwrap();
                let golden = terminal_text(&terminal);
                actual[size_index][tab_index] = buffer_fingerprint(&terminal);
                assert!(!golden.is_empty(), "empty {width}x{height} {tab:?}");
                if width < crate::ui::MIN_TERMINAL_WIDTH || height < crate::ui::MIN_TERMINAL_HEIGHT
                {
                    assert!(golden.contains("Terminal too small"));
                    assert!(golden.contains("50x15"));
                } else {
                    assert!(
                        golden.contains(tab.title()),
                        "{width}x{height} {tab:?}:\n{golden}"
                    );
                    assert!(golden.contains("ONLINE"));
                    assert!(golden.contains("help") || golden.contains("[1]Chat"));
                }
            }
        }
        assert_eq!(actual, expected, "full-buffer size/view golden changed");
    }

    #[test]
    fn theme_variant_goldens_preserve_text_and_change_only_semantic_colors() {
        let mut texts = Vec::new();
        let mut fingerprints = Vec::new();
        for palette in [
            crate::theme::ThemePalette::TrueColor,
            crate::theme::ThemePalette::System,
            crate::theme::ThemePalette::NoColor,
        ] {
            let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
            app.set_theme(palette);
            app.connected = false;
            app.unseen_alerts = 2;
            app.status_message = "full state meaning".to_string();
            let backend = TestBackend::new(80, 24);
            let mut terminal = Terminal::new(backend).unwrap();
            terminal
                .draw(|frame| crate::ui::render(frame, &app))
                .unwrap();
            let buffer = terminal.backend().buffer();
            let text = terminal_text(&terminal);
            fingerprints.push(buffer_fingerprint(&terminal));
            assert!(text.contains("OFFLINE"));
            assert!(text.contains("2 ALERTS"));
            match palette {
                crate::theme::ThemePalette::TrueColor => assert!(buffer
                    .content()
                    .iter()
                    .any(|cell| matches!(cell.fg, Color::Rgb(_, _, _)))),
                crate::theme::ThemePalette::System => assert!(buffer
                    .content()
                    .iter()
                    .all(|cell| !matches!(cell.fg, Color::Rgb(_, _, _)))),
                crate::theme::ThemePalette::NoColor => assert!(buffer
                    .content()
                    .iter()
                    .all(|cell| cell.fg == Color::Reset && cell.bg == Color::Reset)),
            }
            texts.push(text);
        }
        assert_eq!(texts[0], texts[1]);
        assert_eq!(texts[1], texts[2]);
        assert_eq!(
            fingerprints,
            [
                12_798_691_881_099_513_966,
                8_738_156_666_465_401_879,
                14_357_062_234_359_601_905,
            ],
            "full-buffer theme golden changed"
        );
    }

    #[test]
    fn help_overlay_is_scrollable_and_reaches_every_binding_at_minimum_size() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.help_visible = true;

        let backend = TestBackend::new(60, 20);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| crate::ui::render(f, &app)).unwrap();
        let first = terminal_text(&terminal);
        assert!(first.contains("Alt+Enter"));
        assert!(first.contains("PageUp"));
        assert!(app.help_max_scroll.get() > 0);

        let generated = app.help_entries();
        for needle in ["Ctrl+K", "Ctrl+P", "Ctrl+C", "Ctrl+L", "F1", "?"] {
            assert!(
                generated.iter().any(|entry| entry.keys.contains(needle)),
                "generated help omitted {needle:?}"
            );
        }

        app.help_scroll = app.help_max_scroll.get();
        terminal.draw(|f| crate::ui::render(f, &app)).unwrap();
        let last = terminal_text(&terminal);
        assert!(last.contains("Command palette"));
        assert!(last.contains("Esc/q/F1 close"));
    }

    #[test]
    fn help_footer_stays_visible_and_last_binding_reachable_on_tall_narrow_terminals() {
        for (width, height) in [(60, 40), (80, 40)] {
            let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
            app.help_visible = true;
            let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
            terminal
                .draw(|frame| crate::ui::render(frame, &app))
                .unwrap();
            assert!(app.help_max_scroll.get() > 0);
            app.help_scroll = app.help_max_scroll.get();
            terminal
                .draw(|frame| crate::ui::render(frame, &app))
                .unwrap();
            let text = terminal_text(&terminal);
            assert!(
                text.contains("Command palette"),
                "{width}x{height}:\n{text}"
            );
            assert!(text.contains("Esc/q/F1 close"), "{width}x{height}:\n{text}");
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
            CommandPaletteEntry::Builtin(ActionId::NewSession),
            CommandPaletteEntry::Builtin(ActionId::StopRun),
            CommandPaletteEntry::Builtin(ActionId::ToggleDetails),
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
                binding_hints: &[],
                footer: None,
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
                    binding_hints: &[],
                    footer: None,
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
                binding_hints: &[],
                footer: None,
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
                binding_hints: &[],
                footer: None,
            },
            24,
            72,
        );
        let resolving_text = palette_text(&resolving.lines);
        assert!(resolving_text.contains("Resolving preview"));
        assert!(resolving_text.contains("Input paused"));
        assert!(resolving_text.contains("Esc cancel"));
        assert!(!resolving_text.contains('▏'));
        assert!(!resolving_text.contains("Enter use"));
        assert!(!resolving_text.contains("Ctrl+R"));
        assert!(resolving.item_rows.is_empty());

        let hitboxes = RefCell::new(vec![CommandPaletteHitbox {
            index: 0,
            x: 1,
            y: 1,
            width: 10,
            height: 2,
        }]);
        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| {
                render_command_palette_view(
                    frame,
                    CommandPaletteView {
                        trigger: CommandPaletteTrigger::Global,
                        input: "review",
                        entries: &entries,
                        visible: &visible,
                        selected: 0,
                        loading: false,
                        resolving: true,
                        error: None,
                        disabled_reasons: &disabled_reasons,
                        binding_hints: &[],
                        footer: None,
                    },
                    Some(&hitboxes),
                )
            })
            .unwrap();
        assert!(hitboxes.borrow().is_empty());

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
                binding_hints: &[],
                footer: None,
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
                binding_hints: &[],
                footer: None,
            },
            24,
            72,
        );
        assert!(palette_text(&no_matches.lines).contains("No commands match"));
    }

    #[test]
    fn disabled_reasons_match_runtime_availability_and_label_type_source() {
        let entries = vec![
            CommandPaletteEntry::Builtin(ActionId::NewSession),
            CommandPaletteEntry::Builtin(ActionId::StopRun),
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
                binding_hints: &[],
                footer: None,
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
        assert_eq!(description_for(1).trim(), ActionId::StopRun.description());
        assert!(description_for(2).contains("Disabled: Composer commands"));
        assert!(description_for(2).contains("run is active"));
        assert!(palette_text(&rendered.lines).contains("workflow · workspace"));
    }
}
