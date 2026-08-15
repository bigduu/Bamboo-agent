use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

use crate::api::types::SessionSummary;
use crate::app::{App, SessionActivity, SessionActivityStatus};
use crate::keymap::{ActionContext, ActionId};
use crate::text;
use crate::theme::colors;

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    if app.sessions.loading && app.sessions.sessions.is_empty() {
        let loading =
            Paragraph::new("Loading sessions...").style(Style::default().fg(colors::inactive()));
        f.render_widget(loading, area);
        return;
    }

    if let Some(err) = &app.sessions.error {
        let error =
            Paragraph::new(format!("Error: {}", err)).style(Style::default().fg(colors::error()));
        f.render_widget(error, area);
        return;
    }

    if app.sessions.sessions.is_empty() {
        let empty = Paragraph::new(format!(
            "No sessions found.\n\nPress {} to refresh.",
            app.key_hint(ActionContext::Sessions, ActionId::Refresh)
        ))
        .style(Style::default().fg(colors::inactive()));
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
    let header = if area.width < 80 {
        Line::from(Span::styled(
            format!(" Sessions · {} total", app.sessions.total),
            Style::default()
                .fg(colors::brand())
                .add_modifier(Modifier::BOLD),
        ))
    } else {
        Line::from(vec![
            Span::styled(
                " Sessions",
                Style::default()
                    .fg(colors::brand())
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("   "),
            Span::styled(
                format!(
                    "[{}] Refresh",
                    app.key_hint(ActionContext::Sessions, ActionId::Refresh)
                ),
                Style::default().fg(colors::inactive()),
            ),
            Span::raw("  "),
            Span::styled(
                format!(
                    "[{}] Delete",
                    app.key_hint(ActionContext::Sessions, ActionId::DeleteSelection)
                ),
                Style::default().fg(colors::inactive()),
            ),
            Span::raw("  "),
            Span::styled(
                format!(
                    "[{}] Open",
                    app.key_hint(ActionContext::Sessions, ActionId::Activate)
                ),
                Style::default().fg(colors::inactive()),
            ),
        ])
    };
    let header = Paragraph::new(header);
    f.render_widget(header, chunks[0]);

    // Session list
    let mut lines: Vec<Line> = Vec::new();
    lines.push(session_header(chunks[1].width));

    let selected = app
        .sessions
        .selected
        .min(app.sessions.sessions.len().saturating_sub(1));
    let capacity = chunks[1].height.saturating_sub(1) as usize;
    let visible = visible_window(app.sessions.sessions.len(), selected, capacity);
    for (i, session) in app
        .sessions
        .sessions
        .iter()
        .enumerate()
        .take(visible.end)
        .skip(visible.start)
    {
        lines.push(session_row_line_with_activity(
            session,
            i == selected,
            chunks[1].width,
            app.session_activity_for_summary(session),
        ));
    }

    let list = Paragraph::new(lines);
    f.render_widget(list, chunks[1]);

    // Page info: `page X/Y · total N · [ ] to page`.
    let limit = app.sessions.page_limit.max(1);
    let page = app.sessions.offset / limit + 1;
    let pages = app.sessions.total.saturating_sub(1) / limit + 1;
    let page_info = Paragraph::new(format!(
        " page {}/{} · total {} · {}/{} to page",
        page,
        pages,
        app.sessions.total,
        app.key_hint(ActionContext::Sessions, ActionId::PreviousPage),
        app.key_hint(ActionContext::Sessions, ActionId::NextPage),
    ))
    .style(Style::default().fg(colors::subtle()));
    f.render_widget(page_info, chunks[2]);

    // Footer
    let footer_text = format!(
        " {} open · {} delete · {} refresh",
        app.key_hint(ActionContext::Sessions, ActionId::Activate),
        app.key_hint(ActionContext::Sessions, ActionId::DeleteSelection),
        app.key_hint(ActionContext::Sessions, ActionId::Refresh),
    );
    let footer = Paragraph::new(footer_text).style(Style::default().fg(colors::inactive()));
    f.render_widget(footer, chunks[3]);
}

/// Select the contiguous list window that keeps `selected` visible.
///
/// Lists use this after reserving one line for their column header. Keeping the
/// selected item at the bottom while moving forward is predictable and leaves
/// the maximum amount of preceding context at compact terminal heights.
pub(crate) fn visible_window(
    item_count: usize,
    selected: usize,
    capacity: usize,
) -> std::ops::Range<usize> {
    if item_count == 0 || capacity == 0 {
        return 0..0;
    }
    let selected = selected.min(item_count - 1);
    let capacity = capacity.min(item_count);
    let start = selected
        .saturating_add(1)
        .saturating_sub(capacity)
        .min(item_count - capacity);
    start..start + capacity
}

/// Status glyph + color for a session row, in priority order: a running run
/// outranks a pending question, which outranks a stale error from the last
/// run — a session can usefully show only one at a time.
pub(crate) fn status_glyph(session: &SessionSummary) -> (&'static str, Color) {
    if session.is_running {
        ("▶", colors::success())
    } else if session.has_pending_question {
        ("?", colors::warning())
    } else if is_error_status(session.last_run_status.as_deref()) {
        ("✗", colors::error())
    } else {
        (" ", colors::inactive())
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
        Style::default().fg(colors::subtle()),
    ))
}

/// Adaptive row shared by the full Sessions tab and the contextual overlay.
/// Width is measured in terminal cells so long Unicode titles never shift the
/// model/status columns or make a 60-column picker unusable.
#[cfg(test)]
pub(crate) fn session_row_line(
    session: &SessionSummary,
    selected: bool,
    width: u16,
) -> Line<'static> {
    session_row_line_with_activity(session, selected, width, None)
}

pub(crate) fn session_row_line_with_activity(
    session: &SessionSummary,
    selected: bool,
    width: u16,
    activity: Option<SessionActivity>,
) -> Line<'static> {
    let row_style = if selected {
        Style::default()
            .fg(colors::brand())
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(colors::inactive())
    };
    let title = if session.title.trim().is_empty() {
        "(untitled)"
    } else {
        session.title.as_str()
    };
    let (glyph, glyph_color, status, unread) = if let Some(activity) = activity {
        let color = match activity.status {
            SessionActivityStatus::Running => colors::tool_running(),
            SessionActivityStatus::Waiting => colors::warning(),
            SessionActivityStatus::Completed => colors::success(),
            SessionActivityStatus::Failed => colors::error(),
            SessionActivityStatus::Disconnected => colors::warning(),
            SessionActivityStatus::Idle => colors::inactive(),
        };
        (
            activity.status.glyph(),
            color,
            activity.status.label(),
            activity.unread,
        )
    } else {
        let (glyph, color) = status_glyph(session);
        (glyph, color, session_status_label(session), 0)
    };
    let status = if unread > 0 {
        format!("{status}+{}", unread.min(99))
    } else {
        status.to_string()
    };
    let pin = if session.pinned { "★" } else { " " };
    let marker = if selected { "›" } else { " " };

    let body = if width >= 90 {
        let updated = session
            .updated_at
            .map(|time| time.format("%m-%d %H:%M").to_string())
            .unwrap_or_else(|| "-".to_string());
        format!(
            "{marker}{pin} {}  {}  {:>4}  {updated}",
            truncate_cells(
                &if unread > 0 {
                    format!("{title} · {unread} unread")
                } else {
                    title.to_string()
                },
                30,
            ),
            truncate_cells(&session.model, 20),
            session.message_count,
        )
    } else if width >= 60 {
        let available = width.saturating_sub((27 + status.chars().count()) as u16) as usize;
        format!(
            "{marker}{pin} {}  {} · {}",
            truncate_cells(title, available.max(1)),
            truncate_cells(&session.model, 16),
            text::clip_cells(&status, 12),
        )
    } else if width >= 40 {
        // Prefix + markers + separators + longest status + short id consume
        // 28 cells. Give every remaining cell to the title so the row never
        // wraps at the 60-column acceptance width (overlay row width: 52).
        let available = width.saturating_sub((20 + status.chars().count()) as u16) as usize;
        let short_id = session.id.chars().take(8).collect::<String>();
        format!(
            "{marker}{pin} {} · {} · {short_id}",
            truncate_cells(title, available.max(1)),
            text::clip_cells(&status, 12),
        )
    } else if width >= 20 {
        let available = width.saturating_sub((9 + status.chars().count()) as u16) as usize;
        format!(
            "{marker}{pin} {} · {}",
            truncate_cells(title, available.max(1)),
            text::clip_cells(&status, 12),
        )
    } else {
        let available = width.saturating_sub(6) as usize;
        format!("{marker}{pin} {}", truncate_cells(title, available.max(1)),)
    };

    let prefix = format!(" {glyph} ");
    let body = text::clip_cells(
        &body,
        (width as usize).saturating_sub(text::display_width(&prefix)),
    );
    Line::from(vec![
        Span::styled(prefix, Style::default().fg(glyph_color)),
        Span::styled(body, row_style),
    ])
}

pub(crate) fn truncate_cells(value: &str, max_width: usize) -> String {
    text::truncate_cells(value, max_width)
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
            model_ref: None,
            provider: None,
            is_running: false,
            has_pending_question: false,
            running_child_count: 0,
            last_run_status: None,
            updated_at: None,
            message_count: 0,
            pinned: false,
            permission_mode: crate::api::types::SessionPermissionMode::Default,
            bypass_permissions: false,
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

    #[test]
    fn visible_window_keeps_a_deep_selection_on_screen() {
        assert_eq!(visible_window(20, 0, 5), 0..5);
        assert_eq!(visible_window(20, 11, 5), 7..12);
        assert_eq!(visible_window(3, 99, 5), 0..3);
        assert_eq!(visible_window(20, 11, 0), 0..0);
    }

    #[test]
    fn compact_unicode_session_keeps_selection_status_and_actions_visible() {
        use crate::api::BambooClient;

        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = crate::app::Tab::Sessions;
        app.sessions.sessions = (0..30)
            .map(|index| {
                let mut value = session(&format!("session-{index:02}"));
                value.title = format!("普通会话 {index}");
                value.model = "provider/模型-alpha".to_string();
                value
            })
            .collect();
        app.sessions.selected = 24;
        app.sessions.sessions[24].title = "selected-会话🧭e\u{301}".to_string();
        app.sessions.sessions[24].is_running = true;
        app.sessions.total = 30;
        app.sessions.page_limit = 30;

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
        assert!(text.contains("running"), "text status missing:\n{text}");
        assert!(
            text.contains("Enter open"),
            "compact action footer missing:\n{text}"
        );
        assert!(text.contains('›'), "selected-row glyph missing:\n{text}");
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
