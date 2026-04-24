use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
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
        let short = if sid.len() > 8 { &sid[..8] } else { sid };
        spans.push(Span::styled(
            format!(" {}...", short),
            Style::default().fg(colors::INACTIVE),
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
