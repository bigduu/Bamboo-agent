use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Paragraph, Wrap};
use ratatui::Frame;

use crate::app::App;
use crate::theme::colors;

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    if app.config.loading && app.config.config.is_none() {
        let loading =
            Paragraph::new("Loading config...").style(Style::default().fg(colors::INACTIVE));
        f.render_widget(loading, area);
        return;
    }

    if let Some(err) = &app.config.error {
        let error =
            Paragraph::new(format!("Error: {}", err)).style(Style::default().fg(colors::ERROR));
        f.render_widget(error, area);
        return;
    }

    let config_text = match &app.config.config {
        Some(val) => {
            serde_json::to_string_pretty(val).unwrap_or_else(|_| "Invalid JSON".to_string())
        }
        None => "No config loaded".to_string(),
    };

    let lines: Vec<Line> = config_text
        .lines()
        .skip(app.config.scroll_offset as usize)
        .map(|line| {
            let trimmed = line.trim();
            if trimmed.starts_with('"') && trimmed.contains(':') {
                Line::from(Span::styled(
                    line.to_string(),
                    Style::default().fg(colors::BRAND),
                ))
            } else if trimmed.starts_with('"') && trimmed.ends_with('"') {
                Line::from(Span::styled(
                    line.to_string(),
                    Style::default().fg(colors::SUCCESS),
                ))
            } else if trimmed == "true" || trimmed == "false" {
                Line::from(Span::styled(
                    line.to_string(),
                    Style::default().fg(colors::WARNING),
                ))
            } else if trimmed.parse::<f64>().is_ok() {
                Line::from(Span::styled(
                    line.to_string(),
                    Style::default().fg(colors::USER_PREFIX),
                ))
            } else {
                Line::from(Span::raw(line.to_string()))
            }
        })
        .collect();

    let config = Paragraph::new(lines)
        .block(Block::default().title(Span::styled(
            " Config (j/k to scroll)",
            Style::default().fg(colors::BRAND),
        )))
        .wrap(Wrap { trim: false });
    f.render_widget(config, area);
}
