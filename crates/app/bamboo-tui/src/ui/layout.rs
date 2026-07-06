use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use ratatui::Frame;

use crate::app::{App, Tab};
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

pub fn render_help(f: &mut Frame) {
    let area = centered_rect(50, 16, f.area());
    let help_text = vec![
        Line::from(Span::styled(
            " Keybindings",
            Style::default()
                .fg(colors::BRAND)
                .add_modifier(Modifier::BOLD),
        )),
        Line::raw(""),
        Line::raw("  1-6         Switch tab"),
        Line::raw("  Tab         Next tab"),
        Line::raw("  Shift+Tab   Previous tab"),
        Line::raw("  Enter       Send message / Select item"),
        Line::raw("  Ctrl+C      Quit / Stop streaming"),
        Line::raw("  Ctrl+S      Stop agent execution"),
        Line::raw("  j/k         Scroll down/up"),
        Line::raw("  d           Delete (with context)"),
        Line::raw("  r           Refresh / Run schedule"),
        Line::raw("  t           Refresh MCP tools"),
        Line::raw("  ?           Toggle this help"),
        Line::raw(""),
        Line::raw("  Press any key to close"),
    ];

    let help = Paragraph::new(help_text).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(colors::BRAND)),
    );
    f.render_widget(help, area);
}

/// Modal for an agent question (permission gate / clarification): the operator
/// selects an option or types a free-text answer. Rendered over everything when
/// `app.pending_question` is set.
pub fn render_question(f: &mut Frame, app: &App) {
    let Some(q) = &app.pending_question else {
        return;
    };

    let mut lines: Vec<Line> = vec![
        Line::from(Span::styled(
            " Agent needs your input",
            Style::default()
                .fg(colors::BRAND)
                .add_modifier(Modifier::BOLD),
        )),
        Line::raw(""),
    ];
    for l in q.question.lines() {
        lines.push(Line::raw(format!("  {l}")));
    }
    lines.push(Line::raw(""));

    match &q.custom {
        Some(buf) => {
            lines.push(Line::raw("  Type your answer:"));
            lines.push(Line::from(Span::styled(
                format!("  > {buf}\u{258f}"),
                Style::default().fg(colors::BRAND),
            )));
            lines.push(Line::raw(""));
            lines.push(Line::raw(if q.options.is_empty() {
                "  Enter answer  ·  Esc dismiss"
            } else {
                "  Enter answer  ·  Esc back to options"
            }));
        }
        None => {
            for (i, opt) in q.options.iter().enumerate() {
                let selected = i == q.selected;
                let marker = if selected { "\u{203a}" } else { " " };
                let style = if selected {
                    Style::default()
                        .fg(colors::BRAND)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                lines.push(Line::from(Span::styled(
                    format!("  {marker} {}. {opt}", i + 1),
                    style,
                )));
            }
            lines.push(Line::raw(""));
            lines.push(Line::raw(
                "  \u{2191}/\u{2193} select  ·  Enter answer  ·  1-9 quick  ·  c custom  ·  Esc dismiss",
            ));
        }
    }

    let height = (lines.len() as u16 + 2).min(f.area().height);
    let area = centered_rect(60, height, f.area());
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

fn centered_rect(percent_x: u16, height: u16, r: Rect) -> Rect {
    let popup_width = r.width * percent_x / 100;
    let x = (r.width.saturating_sub(popup_width)) / 2;
    let y = (r.height.saturating_sub(height)) / 2;
    Rect::new(
        r.x + x,
        r.y + y,
        popup_width.min(r.width),
        height.min(r.height),
    )
}
