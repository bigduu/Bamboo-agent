use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use ratatui::Frame;

use crate::app::{App, ScheduleRunsContent};
use crate::keymap::{ActionContext, ActionId};
use crate::text::clip_cells;
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

    let body = chunks[1];
    let selected = app
        .schedules
        .selected
        .min(app.schedules.schedules.len().saturating_sub(1));
    let detail_width = body.width.saturating_sub(2);
    let detail_limit = body.height.saturating_sub(2).min(3);
    let detail = app
        .schedules
        .schedules
        .get(selected)
        .and_then(|schedule| schedule.cadence_detail.as_deref())
        .filter(|text| !text.is_empty() && detail_width > 0 && detail_limit > 0)
        .map(|text| {
            let width = usize::from(detail_width) * usize::from(detail_limit);
            Paragraph::new(Line::from(vec![
                Span::styled("Cadence  ", Style::default().fg(colors::brand())),
                Span::raw(clip_schedule_text(text, width.saturating_sub(9))),
            ]))
            .wrap(Wrap { trim: true })
        });
    let detail_height = detail.as_ref().map_or(0, |paragraph| {
        paragraph
            .line_count(detail_width)
            .min(usize::from(detail_limit)) as u16
    });
    let list_area = Rect {
        height: body.height - detail_height,
        ..body
    };

    // Schedule list
    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(Span::styled(
        truncate_cells(
            if compact {
                "  Name · status · schedule"
            } else {
                "  Name  Schedule  Status  Last run"
            },
            usize::from(list_area.width),
        ),
        Style::default().fg(colors::subtle()),
    )));

    let capacity = usize::from(list_area.height.saturating_sub(1));
    let visible = visible_window(app.schedules.schedules.len(), selected, capacity);
    for (i, schedule) in app
        .schedules
        .schedules
        .iter()
        .enumerate()
        .take(visible.end)
        .skip(visible.start)
    {
        lines.push(schedule_row_line(schedule, i == selected, list_area.width));
    }

    if app.schedules.schedules.is_empty() {
        lines.push(Line::from(Span::styled(
            "  No schedules configured.",
            Style::default().fg(colors::inactive()),
        )));
    }

    let list = Paragraph::new(lines);
    f.render_widget(list, list_area);
    if let Some(detail) = detail {
        f.render_widget(
            detail,
            Rect::new(
                body.x.saturating_add(2),
                list_area.bottom(),
                detail_width,
                detail_height,
            ),
        );
    }

    // Footer
    let footer_text = format!(
        " {} runs · {} new · {} delete · {} run now",
        app.primary_key_hint(ActionContext::Schedules, ActionId::Activate),
        app.key_hint(ActionContext::Schedules, ActionId::NewSchedule),
        app.key_hint(ActionContext::Schedules, ActionId::DeleteSelection),
        app.key_hint(ActionContext::Schedules, ActionId::RunSchedule),
    );
    let footer = Paragraph::new(footer_text).style(Style::default().fg(colors::inactive()));
    f.render_widget(footer, chunks[2]);
}

pub(super) fn render_runs(f: &mut Frame, app: &App) {
    let Some(pane) = &app.schedule_runs else {
        return;
    };
    let screen = f.area();
    let width = screen.width.saturating_sub(2).min(72);
    let height = screen.height.saturating_sub(2).min(16);
    let area = Rect::new(
        screen.x + (screen.width - width) / 2,
        screen.y + (screen.height - height) / 2,
        width,
        height,
    );
    let content_width = usize::from(width.saturating_sub(2));
    let content_height = usize::from(height.saturating_sub(2));
    let key = |action| app.primary_key_hint(ActionContext::ScheduleRuns, action);
    let mut close = format!("  {} close", key(ActionId::Cancel));
    let mut footer = Vec::new();
    let mut lines = vec![Line::from(Span::styled(
        format!(
            "  {}",
            clip_schedule_text(&pane.name, content_width.saturating_sub(2))
        ),
        Style::default()
            .fg(colors::brand())
            .add_modifier(Modifier::BOLD),
    ))];
    let muted = Style::default().fg(colors::subtle());
    match &pane.content {
        ScheduleRunsContent::Loading => {
            lines.push(Line::styled("  Loading runs…", muted));
        }
        ScheduleRunsContent::Failed(error) => {
            lines.push(Line::styled(
                format!(
                    "  {}",
                    clip_schedule_text(error, content_width.saturating_sub(2))
                ),
                Style::default().fg(colors::error()),
            ));
        }
        ScheduleRunsContent::Loaded(runs) if runs.is_empty() => {
            lines.push(Line::styled("  No runs recorded.", muted));
        }
        ScheduleRunsContent::Loaded(runs) => {
            let opening = app.schedule_run_session_is_opening();
            if opening {
                lines.push(Line::styled("  Opening Session…", muted));
            } else if let Some(error) = pane.session_open_error {
                lines.push(Line::styled(
                    format!(
                        "  {}",
                        clip_schedule_text(error, content_width.saturating_sub(2))
                    ),
                    Style::default().fg(colors::error()),
                ));
            }
            if !opening && pane.selected_session_id().is_some() {
                close = format!(
                    "  {} open session · {} close",
                    app.keymap
                        .primary_hint(ActionContext::ScheduleRuns, ActionId::Activate),
                    key(ActionId::Cancel)
                );
            }
            footer.push(format!(
                "  {}/{} move · {}/{} page",
                key(ActionId::NavigateUp),
                key(ActionId::NavigateDown),
                key(ActionId::PageUp),
                key(ActionId::PageDown),
            ));
            lines.push(Line::styled(
                format!("  {:<19}  Status", "Scheduled (UTC)"),
                muted,
            ));
            let capacity = content_height.saturating_sub(lines.len() + footer.len() + 1);
            let selected = pane.selected.min(runs.len() - 1);
            let visible = visible_window(runs.len(), selected, capacity);
            pane.rows_per_page.set(visible.len().max(1));
            for index in visible {
                let run = &runs[index];
                let is_selected = index == selected;
                let marker = if is_selected { '›' } else { ' ' };
                let time = format!(
                    "{marker} {}  ",
                    run.scheduled_for.format("%Y-%m-%d %H:%M:%S")
                );
                let time_style = if is_selected {
                    Style::default()
                        .fg(colors::brand())
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(colors::inactive())
                };
                let status_color = match run.status.as_str() {
                    "success" => colors::success(),
                    "running" => colors::tool_running(),
                    "failed" => colors::error(),
                    "missed" | "skipped" => colors::warning(),
                    _ => colors::inactive(),
                };
                lines.push(Line::from(vec![
                    Span::styled(time, time_style),
                    Span::styled(
                        clip_schedule_text(&run.status, content_width.saturating_sub(23)),
                        Style::default().fg(status_color),
                    ),
                ]));
            }
        }
    }
    footer.push(close);
    lines.resize_with(content_height.saturating_sub(footer.len()), || {
        Line::raw("")
    });
    lines.extend(
        footer
            .into_iter()
            .map(|hint| Line::styled(clip_cells(&hint, content_width), muted)),
    );

    f.render_widget(Clear, area);
    f.render_widget(
        Paragraph::new(lines).block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(colors::brand()))
                .title(" Recent runs "),
        ),
        area,
    );
}

fn clip_schedule_text(value: &str, width: usize) -> String {
    let text: String = value
        .chars()
        .filter(|character| !character.is_control())
        .take(width.saturating_mul(4))
        .collect();
    clip_cells(&text, width)
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
    use chrono::{TimeDelta, TimeZone, Utc};

    use crate::api::types::{Schedule, ScheduleRunView};
    use crate::api::BambooClient;
    use crate::app::{ScheduleRunsPane, Tab};
    use crate::keymap::Keymap;
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
            cadence_detail: None,
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
        assert!(
            text.contains("Enter runs"),
            "runs action footer missing:\n{text}"
        );
        assert!(text.contains('›'), "selected-row glyph missing:\n{text}");
        assert!(!text.contains("Cadence"), "{text}");
    }

    fn terminal_rows(terminal: &Terminal<TestBackend>) -> String {
        let buffer = terminal.backend().buffer();
        buffer
            .content()
            .chunks(usize::from(buffer.area().width))
            .map(|row| row.iter().map(|cell| cell.symbol()).collect::<String>())
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn selected_schedule_shows_saved_cadence_without_displacing_its_row_or_actions() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Schedules;
        app.schedules.schedules = (0..28).map(schedule).collect();
        for schedule in [
            serde_json::json!({
                "id": "future",
                "name": "Future schedule",
                "trigger": {"type": "future", "unsupported_payload": "RAW_TRIGGER_VALUE"},
                "timezone": "RAW_ZONE_VALUE"
            }),
            serde_json::json!({
                "id": "weekly",
                "name": "Release 计划 🧭",
                "enabled": true,
                "trigger": {
                    "type": "weekly",
                    "weekdays": ["mon", "tue", "wed", "thu", "fri", "sat", "sun"],
                    "hour": 8,
                    "minute": 30,
                    "second": 5
                },
                "timezone": "America/Argentina/Buenos_Aires"
            }),
        ] {
            app.schedules.schedules.push(Schedule::from_view(
                serde_json::from_value(schedule).unwrap(),
            ));
        }

        for width in [60, 80, 120] {
            let mut terminal = Terminal::new(TestBackend::new(width, 20)).unwrap();
            app.schedules.selected = 29;
            terminal
                .draw(|frame| crate::ui::render(frame, &app))
                .unwrap();
            let text = terminal_rows(&terminal);
            if width == 60 && std::env::var_os("BAMBOO_TUI_CAPTURE_SCHEDULE_CADENCE").is_some() {
                eprintln!("SCHEDULE CADENCE PREVIEW\n{text}\nEND SCHEDULE CADENCE PREVIEW");
            }
            let selected = text.lines().find(|row| row.contains("› Release")).unwrap();
            assert!(
                selected.replace(' ', "").contains("Release计划🧭"),
                "{text}"
            );
            for expected in [
                "Weekly Mon, Tue, Wed, Thu, Fri, Sat, Sun",
                "08:30:05",
                "America/Argentina/Buenos_Aires",
                "r run now",
            ] {
                assert!(text.contains(expected), "missing {expected:?}\n{text}");
            }
            let row_for = |needle: &str| text.lines().position(|row| row.contains(needle)).unwrap();
            let detail = row_for("Cadence");
            let footer = row_for("Enter runs");
            assert!(row_for("› Release") < detail);
            assert!((1..=3).contains(&(footer - detail)), "{text}");

            app.schedules.selected = 28;
            terminal
                .draw(|frame| crate::ui::render(frame, &app))
                .unwrap();
            let fallback = terminal_rows(&terminal);
            assert!(fallback.contains("› Future schedule"), "{fallback}");
            assert!(fallback.contains("Cadence  Unknown trigger"), "{fallback}");
            assert!(fallback.contains("Enter runs"), "{fallback}");
            for hidden in ["RAW_TRIGGER_VALUE", "RAW_ZONE_VALUE", "Buenos_Aires"] {
                assert!(!fallback.contains(hidden), "{fallback}");
            }
        }
    }

    #[test]
    fn monthly_schedule_keeps_all_days_time_and_zone_visible_at_minimum_width() {
        let view = serde_json::from_value(serde_json::json!({
            "id": "monthly",
            "name": "Monthly archive",
            "enabled": true,
            "trigger": {
                "type": "monthly",
                "days": (1..=31).collect::<Vec<_>>(),
                "hour": 8,
                "minute": 30,
                "second": 5
            },
            "timezone": "America/Argentina/ComodRivadavia"
        }))
        .unwrap();
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Schedules;
        app.schedules.schedules = (0..15).map(schedule).collect();
        app.schedules.schedules.push(Schedule::from_view(view));
        app.schedules.selected = 15;
        let mut terminal = Terminal::new(TestBackend::new(60, 20)).unwrap();
        terminal
            .draw(|frame| crate::ui::render(frame, &app))
            .unwrap();
        let text = terminal_rows(&terminal);
        for expected in [
            "› Monthly archive",
            "Monthly 1–31 at 08:30:05",
            "America/Argentina/ComodRivadavia",
            "Enter runs",
        ] {
            assert!(text.contains(expected), "missing {expected:?}\n{text}");
        }
    }

    #[test]
    fn recent_runs_keeps_the_selected_unknown_status_and_shortcuts_visible() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Schedules;
        let mut pane = ScheduleRunsPane::new("schedule-1".into(), "Daily review".into(), 1);
        let time = Utc.with_ymd_and_hms(2026, 11, 26, 8, 0, 30).unwrap();
        pane.content = ScheduleRunsContent::Loaded(
            (0..30)
                .map(|index| ScheduleRunView {
                    run_id: format!("run-{index}"),
                    scheduled_for: time - TimeDelta::seconds(index),
                    status: if index == 24 {
                        "future_status"
                    } else {
                        "success"
                    }
                    .into(),
                    session_id: None,
                })
                .collect(),
        );
        pane.selected = 24;
        app.schedule_runs = Some(pane);
        let mut terminal = Terminal::new(TestBackend::new(50, 15)).unwrap();
        terminal.draw(|frame| render_runs(frame, &app)).unwrap();
        let text = terminal_rows(&terminal);
        if std::env::var_os("BAMBOO_TUI_CAPTURE_SCHEDULE_RUNS").is_some() {
            println!("Schedule runs preview\n{text}");
        }
        for expected in [
            "Recent runs",
            "Daily review",
            "Scheduled (UTC)",
            "Status",
            "› 2026-11-26 08:00:06  future_status",
            "Up/Down move",
            "PageUp/PageDown page",
            "Esc close",
        ] {
            assert!(text.contains(expected), "expected {expected:?}\n{text}");
        }
        assert!(
            text.find("08:00:07").unwrap() < text.find("08:00:06").unwrap(),
            "{text}"
        );
        assert!(!text.contains("08:00:30"), "{text}");
        assert_eq!(app.schedule_runs.as_ref().unwrap().rows_per_page.get(), 7);
    }

    #[test]
    fn recent_runs_advertises_open_session_for_the_selected_linked_run() {
        for (width, height, open_key) in [(60, 20, "Enter"), (80, 24, "/")] {
            let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
            app.tab = Tab::Schedules;
            if open_key == "/" {
                app.set_keymap(
                    Keymap::from_json(
                        r#"{"bindings":[
                            {"context":"schedule-runs","action":"activate","keys":["/","F8"]}
                        ]}"#,
                    )
                    .unwrap(),
                    None,
                );
            }
            let mut entry = schedule(1);
            entry.name = Some("Daily review".into());
            app.schedules.schedules = vec![entry];
            let mut pane = ScheduleRunsPane::new("schedule-1".into(), "Daily review".into(), 1);
            let time = Utc.with_ymd_and_hms(2026, 11, 26, 8, 0, 0).unwrap();
            pane.content = ScheduleRunsContent::Loaded(vec![
                ScheduleRunView {
                    run_id: "linked-run".into(),
                    scheduled_for: time,
                    status: "success".into(),
                    session_id: Some("session-1".into()),
                },
                ScheduleRunView {
                    run_id: "unlinked-run".into(),
                    scheduled_for: time - TimeDelta::days(1),
                    status: "missed".into(),
                    session_id: None,
                },
            ]);
            app.schedule_runs = Some(pane);
            let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
            let draw = |terminal: &mut Terminal<TestBackend>, app: &App| {
                terminal
                    .draw(|frame| crate::ui::render(frame, app))
                    .unwrap();
                terminal_rows(terminal)
            };

            let linked = draw(&mut terminal, &app);
            if width == 60 && std::env::var_os("BAMBOO_TUI_CAPTURE_SCHEDULE_RUN_SESSION").is_some()
            {
                eprintln!(
                    "SCHEDULE RUN SESSION PREVIEW\n{linked}\nEND SCHEDULE RUN SESSION PREVIEW"
                );
            }
            let open_hint = format!("{open_key} open session · Esc close");
            assert!(linked.contains(&open_hint), "{linked}");
            assert!(
                linked.contains("› 2026-11-26 08:00:00  success"),
                "{linked}"
            );
            assert!(linked.contains("PageUp/PageDown page"), "{linked}");

            let pane = app.schedule_runs.as_mut().unwrap();
            pane.session_open_error = Some("Could not open this session");
            let failed = draw(&mut terminal, &app);
            assert!(failed.contains("Could not open this session"), "{failed}");
            assert!(failed.contains(&open_hint), "{failed}");
            assert!(
                failed.contains("› 2026-11-26 08:00:00  success"),
                "{failed}"
            );

            let pane = app.schedule_runs.as_mut().unwrap();
            pane.selected = 1;
            pane.session_open_error = None;
            let unlinked = draw(&mut terminal, &app);
            assert!(
                unlinked.contains("› 2026-11-25 08:00:00  missed"),
                "{unlinked}"
            );
            assert!(!unlinked.contains("open session"), "{unlinked}");
            assert!(unlinked.contains("PageUp/PageDown page"), "{unlinked}");
            assert!(unlinked.contains("Esc close"), "{unlinked}");
        }
    }

    #[test]
    fn recent_run_states_remain_closable_and_list_advertises_remapped_action() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Schedules;
        app.set_keymap(
            Keymap::from_json(
                r#"{"bindings":[{"context":"schedules","action":"activate","keys":["F8"]}]}"#,
            )
            .unwrap(),
            None,
        );
        let mut terminal = Terminal::new(TestBackend::new(60, 20)).unwrap();
        terminal
            .draw(|frame| crate::ui::render(frame, &app))
            .unwrap();
        let text = terminal_rows(&terminal);
        assert!(
            text.contains("F8 runs") && text.contains("r run now"),
            "{text}"
        );
        let mut pane = ScheduleRunsPane::new("schedule-1".into(), "Daily review".into(), 1);
        for (content, expected) in [
            (ScheduleRunsContent::Loading, "Loading runs"),
            (ScheduleRunsContent::Loaded(Vec::new()), "No runs recorded"),
            (
                ScheduleRunsContent::Failed("Unable to load runs.".into()),
                "Unable to load runs.",
            ),
        ] {
            pane.content = content;
            app.schedule_runs = Some(pane);
            terminal
                .draw(|frame| crate::ui::render(frame, &app))
                .unwrap();
            let text = terminal_rows(&terminal);
            assert!(
                text.contains("Recent runs")
                    && text.contains(expected)
                    && text.contains("Esc close"),
                "{text}"
            );
            pane = app.schedule_runs.take().unwrap();
        }
    }
}
