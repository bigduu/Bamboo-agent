use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
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

    let total_lines = config_text.lines().count() as u16;
    // Recorded every frame so key handlers can clamp `scroll_offset` — see
    // `ChatState::max_scroll`'s doc comment (same `Cell`-through-`&App`
    // rationale applies here).
    app.config
        .max_scroll
        .set(total_lines.saturating_sub(area.height));

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
            " Config (j/k scroll · e edit)",
            Style::default().fg(colors::BRAND),
        )))
        .wrap(Wrap { trim: false });
    f.render_widget(config, area);
}

/// Modal raw-JSON editor, rendered over everything when `app.config_editor` is
/// set. `Ctrl+S` saves (after JSON validation), `Esc` cancels.
pub fn render_editor(f: &mut Frame, app: &App) {
    let Some(editor) = &app.config_editor else {
        return;
    };
    let screen = f.area();
    let area = centered(screen, 80, 80);
    f.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(colors::BRAND))
        .title(" Edit config · Ctrl+S save · Esc cancel ");
    let inner = block.inner(area);
    f.render_widget(block, area);
    f.render_widget(&editor.textarea, inner);
}

/// Rect covering `pw`%×`ph`% of `r`, centered. Percentage math is done in u32
/// so a very wide terminal (width ≥ 820) can't overflow the u16 multiply.
fn centered(r: Rect, pw: u16, ph: u16) -> Rect {
    let w = ((r.width as u32 * pw as u32 / 100) as u16).min(r.width);
    let h = ((r.height as u32 * ph as u32 / 100) as u16).min(r.height);
    let x = r.x + r.width.saturating_sub(w) / 2;
    let y = r.y + r.height.saturating_sub(h) / 2;
    Rect::new(x, y, w, h)
}
