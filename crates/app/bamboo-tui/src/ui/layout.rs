use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use ratatui::Frame;

use crate::app::{App, NoticeLevel, Tab};
use crate::theme::{self, colors};

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
    ("Ctrl+N", "New session"),
    ("Ctrl+O", "Model picker (Chat)"),
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

/// Modal for an agent question (permission gate / clarification): the operator
/// selects an option or types a free-text answer. Rendered over everything when
/// `app.pending_question` is set.
pub fn render_question(f: &mut Frame, app: &App) {
    let Some(q) = &app.pending_question else {
        return;
    };

    let screen = f.area();
    // Header: title + blank + (wrapped) question + blank. Cap the question to a
    // few lines so a very long prompt can't push the options/footer off-screen.
    let mut header: Vec<Line> = vec![
        Line::from(Span::styled(
            " Agent needs your input",
            Style::default()
                .fg(colors::BRAND)
                .add_modifier(Modifier::BOLD),
        )),
        Line::raw(""),
    ];
    const MAX_QUESTION_LINES: usize = 6;
    for l in q.question.lines().take(MAX_QUESTION_LINES) {
        header.push(Line::raw(format!("  {l}")));
    }
    if q.question.lines().count() > MAX_QUESTION_LINES {
        header.push(Line::raw("  …"));
    }
    header.push(Line::raw(""));

    let mut body: Vec<Line> = Vec::new();
    match &q.custom {
        Some(buf) => {
            body.push(Line::raw("  Type your answer:"));
            body.push(Line::from(Span::styled(
                format!("  > {buf}\u{258f}"),
                Style::default().fg(colors::BRAND),
            )));
            body.push(Line::raw(""));
            if q.submitting {
                body.push(submitting_hint());
            } else {
                body.push(Line::raw(if q.options.is_empty() {
                    "  Enter answer  ·  Esc dismiss"
                } else {
                    "  Enter answer  ·  Esc back to options"
                }));
            }
        }
        None => {
            // Window the option list around the selection so it stays visible and
            // the modal never overflows the screen, no matter how many options.
            let max_h = screen.height.min(22);
            // rows available for options = modal height - borders(2) - header - footer(1)
            let budget = (max_h as usize).saturating_sub(2 + header.len() + 1).max(1);
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
                body.push(Line::raw(format!("  \u{2191} {start} more")));
            }
            for i in start..end {
                let selected = i == q.selected;
                let marker = if selected { "\u{203a}" } else { " " };
                let style = if selected {
                    Style::default()
                        .fg(colors::BRAND)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                body.push(Line::from(Span::styled(
                    format!("  {marker} {}. {}", i + 1, q.options[i]),
                    style,
                )));
            }
            if end < total {
                body.push(Line::raw(format!("  \u{2193} {} more", total - end)));
            }
            body.push(Line::raw(""));
            if q.submitting {
                body.push(submitting_hint());
            } else {
                body.push(Line::raw(
                    "  \u{2191}/\u{2193} select  ·  Enter answer  ·  1-9 quick  ·  c custom  ·  Esc dismiss",
                ));
            }
        }
    }

    let mut lines = header;
    lines.extend(body);
    let height = (lines.len() as u16 + 2).min(screen.height);
    let area = centered_rect(60, height, screen);
    f.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(colors::BRAND))
        .title(" Question ");
    let para = Paragraph::new(lines)
        .block(block)
        .wrap(Wrap { trim: false });
    f.render_widget(para, area);
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

/// Model picker modal (`Ctrl+O` on the Chat tab): pick a model from the
/// provider catalog. Mirrors `render_question`'s option-list windowing so the
/// selection stays visible (and the modal never overflows the screen) no
/// matter how many models the catalog reports.
pub fn render_model_picker(f: &mut Frame, app: &App) {
    let Some(picker) = &app.model_picker else {
        return;
    };

    let screen = f.area();
    let header: Vec<Line> = vec![
        Line::from(Span::styled(
            " Select a model",
            Style::default()
                .fg(colors::BRAND)
                .add_modifier(Modifier::BOLD),
        )),
        Line::raw(""),
    ];

    let mut body: Vec<Line> = Vec::new();
    if picker.loading {
        body.push(Line::raw("  Loading models..."));
        body.push(Line::raw(""));
        body.push(Line::raw("  Esc cancel"));
    } else if picker.models.is_empty() {
        // Reachable only transiently — an empty catalog closes the picker
        // and notifies (`AppEvent::CatalogLoaded`) rather than leaving this
        // rendered — but keep a safe fallback instead of an empty modal.
        body.push(Line::raw("  No models available"));
        body.push(Line::raw(""));
        body.push(Line::raw("  Esc cancel"));
    } else {
        let max_h = screen.height.min(22);
        // rows available for models = modal height - borders(2) - header - footer(1)
        let budget = (max_h as usize).saturating_sub(2 + header.len() + 1).max(1);
        let total = picker.models.len();
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
            body.push(Line::raw(format!("  \u{2191} {start} more")));
        }
        for i in start..end {
            let m = &picker.models[i];
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
                format!(
                    "  {marker} {}  ({})",
                    m.display_name, m.provider_display_name
                ),
                style,
            )));
        }
        if end < total {
            body.push(Line::raw(format!("  \u{2193} {} more", total - end)));
        }
        body.push(Line::raw(""));
        body.push(Line::raw(
            "  \u{2191}/\u{2193} select  \u{b7}  Enter apply  \u{b7}  Esc cancel",
        ));
    }

    let mut lines = header;
    lines.extend(body);
    let height = (lines.len() as u16 + 2).min(screen.height);
    let area = centered_rect(60, height, screen);
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

#[cfg(test)]
mod tests {
    use crate::api::BambooClient;
    use crate::app::App;
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

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
            "Ctrl+N",
            "Ctrl+O",
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
}
