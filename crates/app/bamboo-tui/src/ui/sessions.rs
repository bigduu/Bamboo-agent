use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::Frame;
use unicode_width::UnicodeWidthChar;

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
    lines.push(session_header(chunks[1].width));

    for (i, session) in app.sessions.sessions.iter().enumerate() {
        lines.push(session_row_line(
            session,
            i == app.sessions.selected,
            chunks[1].width,
        ));
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
pub(crate) fn status_glyph(session: &SessionSummary) -> (&'static str, Color) {
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
pub(crate) fn is_error_status(status: Option<&str>) -> bool {
    status.is_some_and(|s| {
        let s = s.to_ascii_lowercase();
        s.contains("error") || s.contains("fail")
    })
}

pub(crate) fn session_status_label(session: &SessionSummary) -> &'static str {
    if session.is_running {
        "running"
    } else if session.has_pending_question {
        "question"
    } else if is_error_status(session.last_run_status.as_deref()) {
        "error"
    } else {
        "idle"
    }
}

fn session_header(width: u16) -> Line<'static> {
    let label = if width >= 90 {
        "   Title                          Model                 Msgs  Updated"
    } else if width >= 60 {
        "   Session                     Model / status"
    } else {
        "   Session"
    };
    Line::from(Span::styled(
        label.to_string(),
        Style::default().fg(colors::SUBTLE),
    ))
}

/// Adaptive row shared by the full Sessions tab and the contextual overlay.
/// Width is measured in terminal cells so long Unicode titles never shift the
/// model/status columns or make a 60-column picker unusable.
pub(crate) fn session_row_line(
    session: &SessionSummary,
    selected: bool,
    width: u16,
) -> Line<'static> {
    let row_style = if selected {
        Style::default()
            .fg(colors::BRAND)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(colors::INACTIVE)
    };
    let title = if session.title.trim().is_empty() {
        "(untitled)"
    } else {
        session.title.as_str()
    };
    let (glyph, glyph_color) = status_glyph(session);
    let pin = if session.pinned { "★" } else { " " };
    let marker = if selected { "›" } else { " " };

    let body = if width >= 90 {
        let updated = session
            .updated_at
            .map(|time| time.format("%m-%d %H:%M").to_string())
            .unwrap_or_else(|| "-".to_string());
        format!(
            "{marker}{pin} {}  {}  {:>4}  {updated}",
            truncate_cells(title, 30),
            truncate_cells(&session.model, 20),
            session.message_count,
        )
    } else if width >= 60 {
        format!(
            "{marker}{pin} {}  {} · {}",
            truncate_cells(title, 24),
            truncate_cells(&session.model, 16),
            session_status_label(session),
        )
    } else if width >= 40 {
        // Prefix + markers + separators + longest status + short id consume
        // 28 cells. Give every remaining cell to the title so the row never
        // wraps at the 60-column acceptance width (overlay row width: 52).
        let available = width.saturating_sub(28) as usize;
        let short_id = session.id.chars().take(8).collect::<String>();
        format!(
            "{marker}{pin} {} · {} · {short_id}",
            truncate_cells(title, available.max(1)),
            session_status_label(session),
        )
    } else if width >= 20 {
        let available = width.saturating_sub(17) as usize;
        format!(
            "{marker}{pin} {} · {}",
            truncate_cells(title, available.max(1)),
            session_status_label(session),
        )
    } else {
        let available = width.saturating_sub(6) as usize;
        format!("{marker}{pin} {}", truncate_cells(title, available.max(1)),)
    };

    Line::from(vec![
        Span::styled(format!(" {glyph} "), Style::default().fg(glyph_color)),
        Span::styled(body, row_style),
    ])
}

pub(crate) fn truncate_cells(value: &str, max_width: usize) -> String {
    if max_width == 0 {
        return String::new();
    }
    let width = value
        .chars()
        .map(|character| UnicodeWidthChar::width(character).unwrap_or(0))
        .sum::<usize>();
    if width <= max_width {
        let mut output = value.to_string();
        output.extend(std::iter::repeat_n(' ', max_width - width));
        return output;
    }

    let target = max_width.saturating_sub(1);
    let mut output = String::new();
    let mut used = 0;
    for character in value.chars() {
        let character_width = UnicodeWidthChar::width(character).unwrap_or(0);
        if used + character_width > target {
            break;
        }
        output.push(character);
        used += character_width;
    }
    output.push('…');
    while used + 1 < max_width {
        output.push(' ');
        used += 1;
    }
    output
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

    #[test]
    fn adaptive_unicode_rows_never_exceed_their_terminal_width() {
        let mut s = session("session-12345678");
        s.title = "很长的 Unicode 会话标题 with an even longer suffix".to_string();
        s.model = "provider/model-with-a-long-identity".to_string();
        s.is_running = true;
        s.pinned = true;

        for width in [18_u16, 32, 40, 52, 60, 90, 120] {
            let line = session_row_line(&s, true, width);
            assert!(
                line.width() <= width as usize,
                "row width {} exceeded terminal width {width}",
                line.width()
            );
        }
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
