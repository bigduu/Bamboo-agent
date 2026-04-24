use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

use crate::app::App;
use crate::theme::colors;

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    if app.skills.loading && app.skills.skills.is_empty() {
        let loading =
            Paragraph::new("Loading skills...").style(Style::default().fg(colors::INACTIVE));
        f.render_widget(loading, area);
        return;
    }

    if let Some(err) = &app.skills.error {
        let error =
            Paragraph::new(format!("Error: {}", err)).style(Style::default().fg(colors::ERROR));
        f.render_widget(error, area);
        return;
    }

    let chunks = ratatui::layout::Layout::default()
        .direction(ratatui::layout::Direction::Vertical)
        .constraints([
            ratatui::layout::Constraint::Length(1),
            ratatui::layout::Constraint::Min(5),
            ratatui::layout::Constraint::Min(3),
            ratatui::layout::Constraint::Length(1),
        ])
        .split(area);

    // Header
    let header = Paragraph::new(Line::from(Span::styled(
        " Skills",
        Style::default()
            .fg(colors::BRAND)
            .add_modifier(Modifier::BOLD),
    )));
    f.render_widget(header, chunks[0]);

    // Skill list
    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(Span::styled(
        " ID                Name              Description",
        Style::default().fg(colors::SUBTLE),
    )));

    for (i, skill) in app.skills.skills.iter().enumerate() {
        let style = if i == app.skills.selected {
            Style::default()
                .fg(colors::BRAND)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(colors::INACTIVE)
        };

        let id = skill.id.chars().take(18).collect::<String>();
        let name = skill.name.chars().take(18).collect::<String>();
        let desc = skill
            .description
            .as_deref()
            .unwrap_or("-")
            .chars()
            .take(40)
            .collect::<String>();

        lines.push(Line::from(Span::styled(
            format!(" {:18} {:18} {}", id, name, desc),
            style,
        )));
    }

    if app.skills.skills.is_empty() {
        lines.push(Line::from(Span::styled(
            "  No skills available.",
            Style::default().fg(colors::INACTIVE),
        )));
    }

    let list = Paragraph::new(lines);
    f.render_widget(list, chunks[1]);

    // Detail
    if let Some(detail) = &app.skills.detail {
        let mut detail_lines = vec![Line::from(Span::styled(
            format!(" {} - {}", detail.name, detail.id),
            Style::default().fg(colors::BRAND),
        ))];
        if let Some(desc) = &detail.description {
            detail_lines.push(Line::from(format!("  {}", desc)));
        }
        if let Some(tools) = &detail.tools {
            detail_lines.push(Line::from(format!("  Tools: {}", tools.join(", "))));
        }
        let detail_widget = Paragraph::new(detail_lines);
        f.render_widget(detail_widget, chunks[2]);
    }

    // Footer
    let footer =
        Paragraph::new(" [Enter] View details").style(Style::default().fg(colors::INACTIVE));
    f.render_widget(footer, chunks[3]);
}
