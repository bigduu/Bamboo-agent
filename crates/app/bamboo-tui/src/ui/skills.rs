use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

use crate::app::App;
use crate::theme::colors;
use crate::ui::sessions::{truncate_cells, visible_window};

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    if app.skills.loading && app.skills.skills.is_empty() {
        let loading =
            Paragraph::new("Loading skills...").style(Style::default().fg(colors::inactive()));
        f.render_widget(loading, area);
        return;
    }

    if let Some(err) = &app.skills.error {
        let error =
            Paragraph::new(format!("Error: {}", err)).style(Style::default().fg(colors::error()));
        f.render_widget(error, area);
        return;
    }

    let compact = area.width < 80 || area.height < 18;
    let detail_height = match (&app.skills.detail, compact) {
        (Some(_), true) => 3,
        (Some(_), false) => 5,
        (None, _) => 0,
    };
    let chunks = ratatui::layout::Layout::default()
        .direction(ratatui::layout::Direction::Vertical)
        .constraints([
            ratatui::layout::Constraint::Length(1),
            ratatui::layout::Constraint::Min(1),
            ratatui::layout::Constraint::Length(detail_height),
            ratatui::layout::Constraint::Length(1),
        ])
        .split(area);

    // Header
    let header = Paragraph::new(Line::from(Span::styled(
        if compact {
            format!(" Skills · {} available", app.skills.skills.len())
        } else {
            " Skills".to_string()
        },
        Style::default()
            .fg(colors::brand())
            .add_modifier(Modifier::BOLD),
    )));
    f.render_widget(header, chunks[0]);

    // Skill list
    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(Span::styled(
        truncate_cells(
            if compact {
                "   Name · status · description"
            } else {
                "   ID  Name  Status  Description"
            },
            chunks[1].width as usize,
        ),
        Style::default().fg(colors::subtle()),
    )));

    let selected = app
        .skills
        .selected
        .min(app.skills.skills.len().saturating_sub(1));
    let capacity = chunks[1].height.saturating_sub(1) as usize;
    let visible = visible_window(app.skills.skills.len(), selected, capacity);
    for (i, skill) in app
        .skills
        .skills
        .iter()
        .enumerate()
        .take(visible.end)
        .skip(visible.start)
    {
        lines.push(skill_row_line(skill, i == selected, chunks[1].width));
    }

    if app.skills.skills.is_empty() {
        lines.push(Line::from(Span::styled(
            "  No skills available.",
            Style::default().fg(colors::inactive()),
        )));
    }

    let list = Paragraph::new(lines);
    f.render_widget(list, chunks[1]);

    // Detail
    if let Some(detail) = &app.skills.detail {
        let mut detail_lines = vec![Line::from(Span::styled(
            truncate_cells(
                &format!(" {} - {}", detail.name, detail.id),
                chunks[2].width as usize,
            ),
            Style::default().fg(colors::brand()),
        ))];
        if let Some(desc) = &detail.description {
            detail_lines.push(Line::from(truncate_cells(
                &format!("  {}", desc),
                chunks[2].width as usize,
            )));
        }
        if let Some(tools) = &detail.tools {
            detail_lines.push(Line::from(truncate_cells(
                &format!("  Tools: {}", tools.join(", ")),
                chunks[2].width as usize,
            )));
        }
        let detail_widget = Paragraph::new(detail_lines);
        f.render_widget(detail_widget, chunks[2]);
    }

    // Footer
    let footer_text = if compact {
        " Enter details"
    } else {
        " [Enter] View details"
    };
    let footer = Paragraph::new(footer_text).style(Style::default().fg(colors::inactive()));
    f.render_widget(footer, chunks[3]);
}

fn skill_row_line(skill: &crate::api::types::Skill, selected: bool, width: u16) -> Line<'static> {
    let row_style = if selected {
        Style::default()
            .fg(colors::brand())
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(colors::inactive())
    };
    let (status_glyph, status, status_style) = match skill.enabled {
        Some(true) => ("✓", "enabled", Style::default().fg(colors::success())),
        Some(false) => ("○", "disabled", Style::default().fg(colors::inactive())),
        None => ("?", "unknown", Style::default().fg(colors::warning())),
    };
    let marker = if selected { "›" } else { " " };
    let description = skill.description.as_deref().unwrap_or("-");

    if width < 80 {
        // marker/glyph + separators + status consume 18 cells; split the rest
        // between the skill name and description.
        let available = width.saturating_sub(18).max(2) as usize;
        let name_width = (available * 3 / 5).max(1);
        let description_width = available.saturating_sub(name_width).max(1);
        Line::from(vec![
            Span::styled(format!("{marker} {status_glyph} "), status_style),
            Span::styled(truncate_cells(&skill.name, name_width), row_style),
            Span::styled(" · ", row_style),
            Span::styled(truncate_cells(status, 8), status_style),
            Span::styled(" · ", row_style),
            Span::styled(truncate_cells(description, description_width), row_style),
        ])
    } else {
        let description_width = width.saturating_sub(54).max(1) as usize;
        Line::from(vec![
            Span::styled(format!("{marker} {status_glyph} "), status_style),
            Span::styled(truncate_cells(&skill.id, 18), row_style),
            Span::styled("  ", row_style),
            Span::styled(truncate_cells(&skill.name, 18), row_style),
            Span::styled("  ", row_style),
            Span::styled(truncate_cells(status, 8), status_style),
            Span::styled("  ", row_style),
            Span::styled(truncate_cells(description, description_width), row_style),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::types::Skill;
    use crate::api::BambooClient;
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    fn skill(index: usize) -> Skill {
        Skill {
            id: format!("skill-{index}"),
            name: format!("普通技能 {index}"),
            description: Some("处理 Unicode 描述和很长的文字".to_string()),
            enabled: Some(index.is_multiple_of(2)),
        }
    }

    #[test]
    fn compact_unicode_skill_keeps_selection_status_and_actions_visible() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = crate::app::Tab::Skills;
        app.skills.skills = (0..30).map(skill).collect();
        app.skills.selected = 24;
        app.skills.skills[24].name = "selected-技能🧭e\u{301}".to_string();
        app.skills.skills[24].enabled = Some(true);

        let row = skill_row_line(&app.skills.skills[24], true, 50);
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
            text.contains("Enter details"),
            "compact action footer missing:\n{text}"
        );
        assert!(text.contains('›'), "selected-row glyph missing:\n{text}");
    }
}
