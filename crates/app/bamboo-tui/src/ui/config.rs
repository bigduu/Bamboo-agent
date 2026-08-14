use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use ratatui::Frame;

use crate::app::App;
use crate::theme::colors;

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    if app.config.loading && app.config.config.is_none() {
        let loading =
            Paragraph::new("Loading config...").style(Style::default().fg(colors::inactive()));
        f.render_widget(loading, area);
        return;
    }

    if let Some(err) = &app.config.error {
        let error =
            Paragraph::new(format!("Error: {}", err)).style(Style::default().fg(colors::error()));
        f.render_widget(error, area);
        return;
    }

    let config_text = match &app.config.config {
        Some(val) => {
            serde_json::to_string_pretty(val).unwrap_or_else(|_| "Invalid JSON".to_string())
        }
        None => "No config loaded".to_string(),
    };

    let logical_lines: Vec<(String, Style)> = config_text
        .lines()
        .map(|line| {
            let trimmed = line.trim();
            if trimmed.starts_with('"') && trimmed.contains(':') {
                (line.to_string(), Style::default().fg(colors::brand()))
            } else if trimmed.starts_with('"') && trimmed.ends_with('"') {
                (line.to_string(), Style::default().fg(colors::success()))
            } else if trimmed == "true" || trimmed == "false" {
                (line.to_string(), Style::default().fg(colors::warning()))
            } else if trimmed.parse::<f64>().is_ok() {
                (line.to_string(), Style::default().fg(colors::user_prefix()))
            } else {
                (line.to_string(), Style::default())
            }
        })
        .collect();

    // `Paragraph::wrap` intentionally avoids splitting some long words. JSON
    // values can contain arbitrarily long tokens, so pre-wrap every logical
    // line by grapheme/display cells and keep the original semantic style.
    let lines: Vec<Line> = logical_lines
        .into_iter()
        .flat_map(|(line, style)| {
            crate::text::hard_wrap(&line, area.width.max(1) as usize)
                .into_iter()
                .map(move |part| Line::from(Span::styled(part, style)))
        })
        .collect();

    let visual_lines = lines.len();
    let block = Block::default().title(Span::styled(
        " Config (j/k scroll · e edit)",
        Style::default().fg(colors::brand()),
    ));
    let viewport_height = block.inner(area).height as usize;
    let config = Paragraph::new(lines).block(block);
    // Scroll by terminal rows, not JSON logical lines. A single long token
    // can wrap across multiple screens and must still have a reachable tail.
    let max_scroll =
        u16::try_from(visual_lines.saturating_sub(viewport_height)).unwrap_or(u16::MAX);
    app.config.max_scroll.set(max_scroll);
    f.render_widget(
        config.scroll((app.config.scroll_offset.min(max_scroll), 0)),
        area,
    );
}

/// Modal raw-JSON editor, rendered over everything when `app.config_editor` is
/// set. `Ctrl+S` saves (after JSON validation), `Esc` cancels.
pub fn render_editor(f: &mut Frame, app: &App) {
    let Some(editor) = &app.config_editor else {
        return;
    };
    let screen = f.area();
    let area = centered(screen, 94, 90);
    f.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(colors::brand()))
        .title(" Edit config ");
    let inner = block.inner(area);
    f.render_widget(block, area);
    let error_height = u16::from(editor.error.is_some());
    let chunks = ratatui::layout::Layout::vertical([
        ratatui::layout::Constraint::Min(1),
        ratatui::layout::Constraint::Length(error_height),
        ratatui::layout::Constraint::Length(1),
    ])
    .split(inner);
    f.render_widget(&editor.textarea, chunks[0]);
    if let Some(error) = &editor.error {
        f.render_widget(
            Paragraph::new(crate::text::clip_cells(error, chunks[1].width as usize))
                .style(Style::default().fg(colors::error())),
            chunks[1],
        );
    }
    f.render_widget(
        Paragraph::new(" Ctrl+S save · Esc cancel").style(Style::default().fg(colors::inactive())),
        chunks[2],
    );
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

#[cfg(test)]
mod tests {
    use crate::api::BambooClient;
    use crate::app::{App, Tab};
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    #[test]
    fn long_unbroken_config_value_scrolls_to_its_visual_tail() {
        let sentinel = "CONFIG_VISUAL_TAIL_终点🧭";
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Config;
        app.config.config = Some(serde_json::json!({
            "token": format!("{}{}", "a".repeat(2_000), sentinel)
        }));
        let backend = TestBackend::new(60, 20);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| crate::ui::render(frame, &app))
            .unwrap();
        assert!(app.config.max_scroll.get() > 0);

        app.config.scroll_offset = app.config.max_scroll.get();
        terminal
            .draw(|frame| crate::ui::render(frame, &app))
            .unwrap();
        let text = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(
            text.contains("CONFIG_VISUAL_TAIL_")
                && text.contains('终')
                && text.contains('点')
                && text.contains('🧭'),
            "visual tail unreachable:\n{text}"
        );
        assert!(
            terminal
                .backend()
                .buffer()
                .content()
                .iter()
                .rev()
                .any(|cell| cell.symbol() == "}"),
            "the final JSON row is unreachable:\n{text}"
        );
    }
}
