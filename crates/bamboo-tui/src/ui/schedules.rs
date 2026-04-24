use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

use crate::app::App;
use crate::theme::colors;

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    if app.schedules.loading && app.schedules.schedules.is_empty() {
        let loading = Paragraph::new("Loading schedules...")
            .style(Style::default().fg(colors::INACTIVE));
        f.render_widget(loading, area);
        return;
    }

    if let Some(err) = &app.schedules.error {
        let error = Paragraph::new(format!("Error: {}", err))
            .style(Style::default().fg(colors::ERROR));
        f.render_widget(error, area);
        return;
    }

    let chunks = ratatui::layout::Layout::default()
        .direction(ratatui::layout::Direction::Vertical)
        .constraints([
            ratatui::layout::Constraint::Length(1),
            ratatui::layout::Constraint::Min(5),
            ratatui::layout::Constraint::Length(1),
        ])
        .split(area);

    // Header
    let header = Paragraph::new(Line::from(vec![
        Span::styled(" Schedules", Style::default().fg(colors::BRAND).add_modifier(Modifier::BOLD)),
        Span::raw("   "),
        Span::styled("[d] Delete", Style::default().fg(colors::INACTIVE)),
        Span::raw("  "),
        Span::styled("[r] Run now", Style::default().fg(colors::INACTIVE)),
    ]));
    f.render_widget(header, chunks[0]);

    // Schedule list
    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(Span::styled(
        " Name              Cron              Enabled  Last Run",
        Style::default().fg(colors::SUBTLE),
    )));

    for (i, schedule) in app.schedules.schedules.iter().enumerate() {
        let style = if i == app.schedules.selected {
            Style::default()
                .fg(colors::BRAND)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(colors::INACTIVE)
        };

        let name = schedule
            .name
            .as_deref()
            .unwrap_or(&schedule.id)
            .chars()
            .take(18)
            .collect::<String>();
        let cron = schedule
            .cron
            .as_deref()
            .unwrap_or("-")
            .chars()
            .take(18)
            .collect::<String>();
        let enabled = if schedule.enabled.unwrap_or(false) {
            "Yes"
        } else {
            "No"
        };
        let last_run = schedule
            .last_run
            .map(|t| t.format("%m-%d %H:%M").to_string())
            .unwrap_or_else(|| "-".to_string());

        lines.push(Line::from(Span::styled(
            format!(" {:18} {:18} {:8} {}", name, cron, enabled, last_run),
            style,
        )));
    }

    if app.schedules.schedules.is_empty() {
        lines.push(Line::from(Span::styled(
            "  No schedules configured.",
            Style::default().fg(colors::INACTIVE),
        )));
    }

    let list = Paragraph::new(lines);
    f.render_widget(list, chunks[1]);

    // Footer
    let footer = Paragraph::new(" [d] Delete · [r] Run now")
        .style(Style::default().fg(colors::INACTIVE));
    f.render_widget(footer, chunks[2]);
}
