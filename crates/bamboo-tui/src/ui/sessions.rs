use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

use crate::app::App;
use crate::theme::colors;

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    if app.sessions.loading && app.sessions.sessions.is_empty() {
        let loading =
            Paragraph::new("Loading sessions...").style(Style::default().fg(colors::INACTIVE));
        f.render_widget(loading, area);
        return;
    }

    if let Some(err) = &app.sessions.error {
        let error =
            Paragraph::new(format!("Error: {}", err)).style(Style::default().fg(colors::ERROR));
        f.render_widget(error, area);
        return;
    }

    if app.sessions.sessions.is_empty() {
        let empty = Paragraph::new("No sessions found.\n\nPress 'r' to refresh.")
            .style(Style::default().fg(colors::INACTIVE));
        f.render_widget(empty, area);
        return;
    }

    // Split into list and detail.
    let chunks = ratatui::layout::Layout::default()
        .direction(ratatui::layout::Direction::Vertical)
        .constraints([
            ratatui::layout::Constraint::Length(1), // header
            ratatui::layout::Constraint::Min(5),    // list
            ratatui::layout::Constraint::Length(5), // detail
            ratatui::layout::Constraint::Length(1), // footer
        ])
        .split(area);

    // Header
    let header = Paragraph::new(Line::from(vec![
        Span::styled(
            " Sessions",
            Style::default()
                .fg(colors::BRAND)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("   "),
        Span::styled("[r] Refresh", Style::default().fg(colors::INACTIVE)),
        Span::raw("  "),
        Span::styled("[d] Delete", Style::default().fg(colors::INACTIVE)),
        Span::raw("  "),
        Span::styled("[Enter] Open", Style::default().fg(colors::INACTIVE)),
    ]));
    f.render_widget(header, chunks[0]);

    // Session list
    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(vec![Span::styled(
        " ID                Model                    Msgs  Status",
        Style::default().fg(colors::SUBTLE),
    )]));

    for (i, session) in app.sessions.sessions.iter().enumerate() {
        let style = if i == app.sessions.selected {
            Style::default()
                .fg(colors::BRAND)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(colors::INACTIVE)
        };

        let id_short = if session.id.len() > 16 {
            &session.id[..16]
        } else {
            &session.id
        };
        let model = session
            .model
            .as_deref()
            .unwrap_or("-")
            .chars()
            .take(22)
            .collect::<String>();
        let msgs = session
            .message_count
            .map(|c| c.to_string())
            .unwrap_or_else(|| "-".to_string());
        let status = session.status.as_deref().unwrap_or("-");

        lines.push(Line::from(Span::styled(
            format!(" {:16}  {:22}  {:4}  {}", id_short, model, msgs, status),
            style,
        )));
    }

    let list = Paragraph::new(lines);
    f.render_widget(list, chunks[1]);

    // Detail
    if let Some(session) = app.sessions.sessions.get(app.sessions.selected) {
        let detail_lines = vec![
            Line::from(Span::styled(" Details", Style::default().fg(colors::BRAND))),
            Line::from(format!("  ID: {}", session.id)),
            Line::from(format!(
                "  Model: {}",
                session.model.as_deref().unwrap_or("-")
            )),
            Line::from(format!(
                "  Created: {}",
                session
                    .created_at
                    .map(|t| t.format("%Y-%m-%d %H:%M").to_string())
                    .unwrap_or_else(|| "-".to_string())
            )),
        ];
        let detail = Paragraph::new(detail_lines);
        f.render_widget(detail, chunks[2]);
    }

    // Footer
    let footer = Paragraph::new(" [Enter] Open in Chat · [d] Delete · [r] Refresh")
        .style(Style::default().fg(colors::INACTIVE));
    f.render_widget(footer, chunks[3]);
}
