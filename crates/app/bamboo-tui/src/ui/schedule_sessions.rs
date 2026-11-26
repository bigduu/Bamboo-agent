use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use ratatui::Frame;

use crate::app::{App, ScheduleSessionsContent};
use crate::keymap::{ActionContext, ActionId};
use crate::theme::colors;

use super::schedules::clip_schedule_text;
use super::sessions::{session_row_line_with_activity, visible_window};

pub(super) fn render(frame: &mut Frame, app: &App) {
    let Some(pane) = &app.schedule_sessions else {
        return;
    };
    let screen = frame.area();
    let width = screen.width.saturating_sub(2).min(72);
    let height = screen.height.saturating_sub(2).min(16);
    let area = Rect::new(
        screen.x + (screen.width - width) / 2,
        screen.y + (screen.height - height) / 2,
        width,
        height,
    );
    let content_width = width.saturating_sub(2);
    let content_height = usize::from(height.saturating_sub(2));
    let text_width = usize::from(content_width.saturating_sub(2));
    let label = |text: &str, style| {
        Line::styled(format!("  {}", clip_schedule_text(text, text_width)), style)
    };
    let muted = Style::default().fg(colors::subtle());
    let error = Style::default().fg(colors::error());
    let key = |action| {
        app.keymap
            .primary_hint(ActionContext::ScheduleSessions, action)
    };
    let opening = app.schedule_session_is_opening();
    let close = format!("{} close", key(ActionId::Cancel));

    let mut lines = vec![label(
        &pane.name,
        Style::default()
            .fg(colors::brand())
            .add_modifier(Modifier::BOLD),
    )];
    match &pane.content {
        ScheduleSessionsContent::Loading => {
            lines.push(label("Loading sessions…", muted));
            lines.push(label(&close, muted));
        }
        ScheduleSessionsContent::Failed(message) => {
            lines.push(label(message, error));
            lines.push(label(&close, muted));
        }
        ScheduleSessionsContent::Loaded(sessions) if sessions.is_empty() => {
            lines.push(label("No Sessions created by this schedule.", muted));
            lines.push(label(&close, muted));
        }
        ScheduleSessionsContent::Loaded(sessions) => {
            if opening {
                lines.push(label("Opening Session…", muted));
            } else if let Some(message) = pane.session_open_error {
                lines.push(label(message, error));
            }
            let capacity = content_height.saturating_sub(lines.len() + 2);
            pane.rows_per_page.set(capacity.max(1));
            for index in visible_window(sessions.len(), pane.selected, capacity) {
                lines.push(session_row_line_with_activity(
                    &sessions[index],
                    index == pane.selected,
                    content_width,
                    None,
                ));
            }
            lines.push(label(
                &format!(
                    "{}/{} move · {}/{} page",
                    key(ActionId::NavigateUp),
                    key(ActionId::NavigateDown),
                    key(ActionId::PageUp),
                    key(ActionId::PageDown),
                ),
                muted,
            ));
            if opening || pane.selected_session().is_none() {
                lines.push(label(&close, muted));
            } else {
                lines.push(label(
                    &format!("{} open · {close}", key(ActionId::Activate)),
                    muted,
                ));
            }
        }
    }

    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(lines).block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(colors::brand()))
                .title(" Schedule Sessions "),
        ),
        area,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::BambooClient;
    use crate::app::{ScheduleSessionsPane, Tab};
    use crate::keymap::Keymap;
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    fn preview(terminal: &mut Terminal<TestBackend>, app: &App) -> String {
        terminal
            .draw(|frame| crate::ui::render(frame, app))
            .unwrap();
        terminal
            .backend()
            .buffer()
            .content()
            .chunks(60)
            .map(|row| row.iter().map(|cell| cell.symbol()).collect::<String>())
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn compact_schedule_sessions_keep_scope_and_selected_history_visible() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Schedules;
        let mut pane =
            ScheduleSessionsPane::new("schedule-release".into(), "Release checks".into(), 1);
        pane.content = ScheduleSessionsContent::Loaded(
            (1..=25)
                .map(|index| {
                    serde_json::from_value(serde_json::json!({
                        "id": format!("run-{index:02}"),
                        "title": format!("Build check {index:02}"),
                        "is_running": index == 25,
                    }))
                    .unwrap()
                })
                .collect(),
        );
        pane.selected = 24;
        app.schedule_sessions = Some(pane);
        let mut terminal = Terminal::new(TestBackend::new(60, 20)).unwrap();
        let loaded = preview(&mut terminal, &app);
        if std::env::var_os("BAMBOO_TUI_CAPTURE_SCHEDULE_SESSIONS").is_some() {
            eprintln!("SCHEDULE SESSIONS PREVIEW\n{loaded}\nEND SCHEDULE SESSIONS PREVIEW");
        }
        for expected in [
            "Schedule Sessions",
            "Release checks",
            "Up/Down move",
            "PageUp/PageDown page",
            "Enter open · Esc close",
        ] {
            assert!(loaded.contains(expected), "missing {expected:?}\n{loaded}");
        }
        assert!(
            loaded
                .lines()
                .any(|row| row.contains('›') && row.contains("Build check 25")),
            "{loaded}"
        );
        assert!(!loaded.contains("Build check 01"), "{loaded}");
        assert_eq!(
            app.schedule_sessions.as_ref().unwrap().rows_per_page.get(),
            11
        );

        app.set_keymap(
            Keymap::from_json(
                r#"{"bindings":[
            {"context":"schedule-sessions","action":"activate","keys":["F8"]}
        ]}"#,
            )
            .unwrap(),
            None,
        );
        let pane = app.schedule_sessions.as_mut().unwrap();
        pane.session_open_error = Some("Could not open this session");
        let refused = preview(&mut terminal, &app);
        assert!(refused.contains("Could not open this session"), "{refused}");
        assert!(refused.contains("F8 open · Esc close"), "{refused}");
        assert!(
            refused
                .lines()
                .any(|row| row.contains('›') && row.contains("Build check 25")),
            "{refused}"
        );
    }

    #[test]
    fn compact_schedule_sessions_show_close_while_loading_empty_or_failed() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Schedules;
        app.schedule_sessions = Some(ScheduleSessionsPane::new(
            "schedule".into(),
            "Release checks".into(),
            1,
        ));
        let mut terminal = Terminal::new(TestBackend::new(60, 20)).unwrap();
        for (content, message) in [
            (ScheduleSessionsContent::Loading, "Loading sessions…"),
            (
                ScheduleSessionsContent::Loaded(Vec::new()),
                "No Sessions created by this schedule.",
            ),
            (
                ScheduleSessionsContent::Failed("Could not load schedule Sessions".into()),
                "Could not load schedule Sessions",
            ),
        ] {
            app.schedule_sessions.as_mut().unwrap().content = content;
            let text = preview(&mut terminal, &app);
            assert!(text.contains("Release checks"), "{text}");
            assert!(text.contains(message), "{text}");
            assert!(text.contains("Esc close"), "{text}");
            assert!(!text.contains("Enter open"), "{text}");
        }
    }
}
