use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

use crate::api::types::SessionSummary;
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

    let chunks = ratatui::layout::Layout::default()
        .direction(ratatui::layout::Direction::Vertical)
        .constraints([
            ratatui::layout::Constraint::Length(1), // header
            ratatui::layout::Constraint::Min(5),    // list
            ratatui::layout::Constraint::Length(1), // page info
            ratatui::layout::Constraint::Length(1), // key footer
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
    lines.push(Line::from(Span::styled(
        "   Title                          Model                 Msgs  Updated       ",
        Style::default().fg(colors::SUBTLE),
    )));

    for (i, session) in app.sessions.sessions.iter().enumerate() {
        let row_style = if i == app.sessions.selected {
            Style::default()
                .fg(colors::BRAND)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(colors::INACTIVE)
        };

        let title: String = if session.title.is_empty() {
            "(untitled)".to_string()
        } else {
            session.title.chars().take(30).collect()
        };
        let model: String = session.model.chars().take(21).collect();
        let updated = session
            .updated_at
            .map(|t| t.format("%m-%d %H:%M").to_string())
            .unwrap_or_else(|| "-".to_string());
        let (glyph, glyph_color) = status_glyph(session);

        lines.push(Line::from(vec![
            Span::styled(format!(" {} ", glyph), Style::default().fg(glyph_color)),
            Span::styled(
                format!(
                    "{:30}  {:21}  {:>4}  {}",
                    title, model, session.message_count, updated
                ),
                row_style,
            ),
        ]));
    }

    let list = Paragraph::new(lines);
    f.render_widget(list, chunks[1]);

    // Page info: `page X/Y · total N · [ ] to page`.
    let limit = app.sessions.page_limit.max(1);
    let page = app.sessions.offset / limit + 1;
    let pages = app.sessions.total.saturating_sub(1) / limit + 1;
    let page_info = Paragraph::new(format!(
        " page {}/{} · total {} · [ ] to page",
        page, pages, app.sessions.total
    ))
    .style(Style::default().fg(colors::SUBTLE));
    f.render_widget(page_info, chunks[2]);

    // Footer
    let footer = Paragraph::new(" [Enter] Open in Chat · [d] Delete · [r] Refresh")
        .style(Style::default().fg(colors::INACTIVE));
    f.render_widget(footer, chunks[3]);
}

/// Status glyph + color for a session row, in priority order: a running run
/// outranks a pending question, which outranks a stale error from the last
/// run — a session can usefully show only one at a time.
fn status_glyph(session: &SessionSummary) -> (&'static str, Color) {
    if session.is_running {
        ("▶", colors::SUCCESS)
    } else if session.has_pending_question {
        ("?", colors::WARNING)
    } else if is_error_status(session.last_run_status.as_deref()) {
        ("✗", colors::ERROR)
    } else {
        (" ", colors::INACTIVE)
    }
}

/// Whether `last_run_status` reads as a failure. The engine writes lowercase
/// tags (`"completed"`, `"cancelled"`, `"error"`, …); matched case-insensitively
/// and loosely (`contains`) so a future `"error: timeout"`-style detail still
/// lights the glyph.
fn is_error_status(status: Option<&str>) -> bool {
    status.is_some_and(|s| {
        let s = s.to_ascii_lowercase();
        s.contains("error") || s.contains("fail")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    fn session(id: &str) -> SessionSummary {
        SessionSummary {
            id: id.into(),
            project_id: None,
            title: String::new(),
            title_generated: true,
            model: String::new(),
            is_running: false,
            has_pending_question: false,
            last_run_status: None,
            updated_at: None,
            message_count: 0,
            pinned: false,
        }
    }

    #[test]
    fn status_glyph_priority_running_beats_question_beats_error() {
        let mut s = session("s1");
        s.is_running = true;
        s.has_pending_question = true;
        s.last_run_status = Some("error".to_string());
        assert_eq!(status_glyph(&s).0, "▶");

        s.is_running = false;
        assert_eq!(status_glyph(&s).0, "?");

        s.has_pending_question = false;
        assert_eq!(status_glyph(&s).0, "✗");

        s.last_run_status = Some("completed".to_string());
        assert_eq!(status_glyph(&s).0, " ");
    }

    #[test]
    fn is_error_status_matches_loosely_and_case_insensitively() {
        assert!(is_error_status(Some("error")));
        assert!(is_error_status(Some("Error")));
        assert!(is_error_status(Some("error: timeout")));
        assert!(is_error_status(Some("FAILED")));
        assert!(!is_error_status(Some("completed")));
        assert!(!is_error_status(None));
    }

    /// Smoke test: the table renders with the new columns and page info without
    /// panicking, and the rendered buffer contains a title, a status glyph, and
    /// the page-info footer.
    #[test]
    fn renders_table_with_columns_and_page_info() {
        use crate::api::BambooClient;
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        let mut running = session("s1");
        running.title = "Investigate flaky test".to_string();
        running.model = "claude-sonnet-5".to_string();
        running.is_running = true;
        running.message_count = 7;
        app.sessions.sessions = vec![running, session("s2")];
        app.sessions.total = 5;
        app.sessions.page_limit = 2;
        app.sessions.offset = 0;
        app.sessions.next_offset = Some(2);
        app.tab = crate::app::Tab::Sessions;

        let backend = TestBackend::new(100, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| crate::ui::render(f, &app)).unwrap();

        let buf = terminal.backend().buffer().clone();
        let text: String = buf.content().iter().map(|c| c.symbol()).collect();
        assert!(text.contains("Investigate flaky test"), "title missing");
        assert!(text.contains("claude-sonnet-5"), "model missing");
        assert!(text.contains("▶"), "running glyph missing");
        assert!(text.contains("page 1/3"), "page info missing");
        assert!(text.contains("total 5"), "total missing");
    }

    /// A single-page result (`total <= limit`) renders as `page 1/1` — the
    /// empty-state guard means `render` never divides by a zero `page_limit`
    /// (it's `.max(1)`-clamped before use either way).
    #[test]
    fn renders_page_one_of_one_when_everything_fits() {
        use crate::api::BambooClient;
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.sessions.sessions = vec![session("s1")];
        app.sessions.total = 1;
        app.sessions.page_limit = 200;
        app.sessions.offset = 0;
        app.sessions.next_offset = None;
        app.tab = crate::app::Tab::Sessions;

        let backend = TestBackend::new(100, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| crate::ui::render(f, &app)).unwrap();

        let buf = terminal.backend().buffer().clone();
        let text: String = buf.content().iter().map(|c| c.symbol()).collect();
        assert!(text.contains("page 1/1"), "page info missing");
    }
}
