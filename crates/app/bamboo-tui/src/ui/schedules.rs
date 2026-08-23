use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

use crate::app::App;
use crate::keymap::{ActionContext, ActionId};
use crate::theme::colors;
use crate::ui::sessions::{truncate_cells, visible_window};

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    if app.schedules.loading && app.schedules.schedules.is_empty() {
        let loading =
            Paragraph::new("Loading schedules...").style(Style::default().fg(colors::inactive()));
        f.render_widget(loading, area);
        return;
    }

    if let Some(err) = &app.schedules.error {
        let error =
            Paragraph::new(format!("Error: {}", err)).style(Style::default().fg(colors::error()));
        f.render_widget(error, area);
        return;
    }

    let compact = area.width < 80 || area.height < 18;
    let chunks = ratatui::layout::Layout::default()
        .direction(ratatui::layout::Direction::Vertical)
        .constraints([
            ratatui::layout::Constraint::Length(1),
            ratatui::layout::Constraint::Min(5),
            ratatui::layout::Constraint::Length(1),
        ])
        .split(area);

    // Header
    let header = if compact {
        Line::from(Span::styled(
            format!(" Schedules · {} configured", app.schedules.schedules.len()),
            Style::default()
                .fg(colors::brand())
                .add_modifier(Modifier::BOLD),
        ))
    } else {
        Line::from(vec![
            Span::styled(
                " Schedules",
                Style::default()
                    .fg(colors::brand())
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("   "),
            Span::styled(
                format!(
                    "[{}] New",
                    app.key_hint(ActionContext::Schedules, ActionId::NewSchedule)
                ),
                Style::default().fg(colors::inactive()),
            ),
            Span::raw("  "),
            Span::styled(
                format!(
                    "[{}] Delete",
                    app.key_hint(ActionContext::Schedules, ActionId::DeleteSelection)
                ),
                Style::default().fg(colors::inactive()),
            ),
            Span::raw("  "),
            Span::styled(
                format!(
                    "[{}] Run now",
                    app.key_hint(ActionContext::Schedules, ActionId::RunSchedule)
                ),
                Style::default().fg(colors::inactive()),
            ),
        ])
    };
    let header = Paragraph::new(header);
    f.render_widget(header, chunks[0]);

    // Schedule list
    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(Span::styled(
        truncate_cells(
            if compact {
                "  Name · status · schedule"
            } else {
                "  Name  Schedule  Status  Last run"
            },
            chunks[1].width as usize,
        ),
        Style::default().fg(colors::subtle()),
    )));

    let selected = app
        .schedules
        .selected
        .min(app.schedules.schedules.len().saturating_sub(1));
    let capacity = chunks[1].height.saturating_sub(1) as usize;
    let visible = visible_window(app.schedules.schedules.len(), selected, capacity);
    for (i, schedule) in app
        .schedules
        .schedules
        .iter()
        .enumerate()
        .take(visible.end)
        .skip(visible.start)
    {
        lines.push(schedule_row_line(schedule, i == selected, chunks[1].width));
    }

    if app.schedules.schedules.is_empty() {
        lines.push(Line::from(Span::styled(
            "  No schedules configured.",
            Style::default().fg(colors::inactive()),
        )));
    }

    let list = Paragraph::new(lines);
    f.render_widget(list, chunks[1]);

    // Footer
    let footer_text = format!(
        " {} new · {} delete · {} run now",
        app.key_hint(ActionContext::Schedules, ActionId::NewSchedule),
        app.key_hint(ActionContext::Schedules, ActionId::DeleteSelection),
        app.key_hint(ActionContext::Schedules, ActionId::RunSchedule),
    );
    let footer = Paragraph::new(footer_text).style(Style::default().fg(colors::inactive()));
    f.render_widget(footer, chunks[2]);
}

fn schedule_row_line(
    schedule: &crate::api::types::Schedule,
    selected: bool,
    width: u16,
) -> Line<'static> {
    let row_style = if selected {
        Style::default()
            .fg(colors::brand())
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(colors::inactive())
    };
    let enabled = if schedule.enabled.unwrap_or(false) {
        "enabled"
    } else {
        "disabled"
    };
    let status_style = if schedule.enabled.unwrap_or(false) {
        Style::default().fg(colors::success())
    } else {
        Style::default().fg(colors::inactive())
    };
    let marker = if selected { "›" } else { " " };
    let name = schedule.name.as_deref().unwrap_or(&schedule.id);
    let cron = schedule.cron.as_deref().unwrap_or("-");

    if width < 80 {
        // marker + two separators + status consume 16 cells; split the rest
        // between the human name and the schedule expression.
        let available = width.saturating_sub(16).max(2) as usize;
        let name_width = (available * 3 / 5).max(1);
        let cron_width = available.saturating_sub(name_width).max(1);
        Line::from(vec![
            Span::styled(format!("{marker} "), row_style),
            Span::styled(truncate_cells(name, name_width), row_style),
            Span::styled(" · ", row_style),
            Span::styled(truncate_cells(enabled, 8), status_style),
            Span::styled(" · ", row_style),
            Span::styled(truncate_cells(cron, cron_width), row_style),
        ])
    } else {
        let last_run = schedule
            .last_run
            .map(|time| time.format("%m-%d %H:%M").to_string())
            .unwrap_or_else(|| "-".to_string());
        let name_width = width.saturating_sub(45).max(1) as usize;
        Line::from(vec![
            Span::styled(format!("{marker} "), row_style),
            Span::styled(truncate_cells(name, name_width), row_style),
            Span::styled("  ", row_style),
            Span::styled(truncate_cells(cron, 18), row_style),
            Span::styled("  ", row_style),
            Span::styled(truncate_cells(enabled, 8), status_style),
            Span::styled("  ", row_style),
            Span::styled(truncate_cells(&last_run, 11), row_style),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::types::Schedule;
    use crate::api::BambooClient;
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    fn schedule(index: usize) -> Schedule {
        Schedule {
            id: format!("schedule-{index}"),
            name: Some(format!("普通计划 {index}")),
            cron: Some("*/15 * * * *".to_string()),
            enabled: Some(index.is_multiple_of(2)),
            prompt: None,
            last_run: None,
            next_run: None,
        }
    }

    #[test]
    fn compact_unicode_schedule_keeps_selection_status_and_actions_visible() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = crate::app::Tab::Schedules;
        app.schedules.schedules = (0..30).map(schedule).collect();
        app.schedules.selected = 24;
        app.schedules.schedules[24].name = Some("selected-计划🧭e\u{301}".to_string());
        app.schedules.schedules[24].enabled = Some(true);

        let row = schedule_row_line(&app.schedules.schedules[24], true, 50);
        assert!(row.width() <= 50);

        let backend = TestBackend::new(60, 20);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| crate::ui::render(frame, &app))
            .unwrap();
        let text: String = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect();

        assert!(text.contains("selected-"), "selected row missing:\n{text}");
        assert!(text.contains('🧭'), "Unicode fixture missing:\n{text}");
        assert!(text.contains("enabled"), "text status missing:\n{text}");
        assert!(
            text.contains("r run now"),
            "compact action footer missing:\n{text}"
        );
        assert!(text.contains("n new"), "new action footer missing:\n{text}");
        assert!(text.contains('›'), "selected-row glyph missing:\n{text}");
    }
}
