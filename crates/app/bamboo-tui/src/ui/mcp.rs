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
    if app.mcp.loading && app.mcp.servers.is_empty() {
        let loading =
            Paragraph::new("Loading MCP servers...").style(Style::default().fg(colors::inactive()));
        f.render_widget(loading, area);
        return;
    }

    if let Some(err) = &app.mcp.error {
        let error =
            Paragraph::new(format!("Error: {}", err)).style(Style::default().fg(colors::error()));
        f.render_widget(error, area);
        return;
    }

    let compact = area.width < 80 || area.height < 18;
    let tools_height = if compact { 3 } else { 5 };
    let chunks = ratatui::layout::Layout::default()
        .direction(ratatui::layout::Direction::Vertical)
        .constraints([
            ratatui::layout::Constraint::Length(1),
            ratatui::layout::Constraint::Min(1),
            ratatui::layout::Constraint::Length(tools_height),
            ratatui::layout::Constraint::Length(1),
        ])
        .split(area);

    // Header
    let header = if compact {
        Line::from(Span::styled(
            format!(" MCP servers · {} configured", app.mcp.servers.len()),
            Style::default()
                .fg(colors::brand())
                .add_modifier(Modifier::BOLD),
        ))
    } else {
        Line::from(vec![
            Span::styled(
                " MCP Servers",
                Style::default()
                    .fg(colors::brand())
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("   "),
            Span::styled(
                format!(
                    "[{}] Refresh",
                    app.key_hint(ActionContext::Mcp, ActionId::Refresh)
                ),
                Style::default().fg(colors::inactive()),
            ),
            Span::raw("  "),
            Span::styled(
                format!(
                    "[{}] Tools",
                    app.key_hint(ActionContext::Mcp, ActionId::ShowTools)
                ),
                Style::default().fg(colors::inactive()),
            ),
            Span::raw("  "),
            Span::styled(
                format!(
                    "[{}] Connect/Disc",
                    app.key_hint(ActionContext::Mcp, ActionId::Activate)
                ),
                Style::default().fg(colors::inactive()),
            ),
        ])
    };
    let header = Paragraph::new(header);
    f.render_widget(header, chunks[0]);

    // Server list
    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(Span::styled(
        truncate_cells(
            if compact {
                "   Name · connection · enabled"
            } else {
                "   Name  Transport  Connection  Enabled"
            },
            chunks[1].width as usize,
        ),
        Style::default().fg(colors::subtle()),
    )));

    let selected = app
        .mcp
        .selected
        .min(app.mcp.servers.len().saturating_sub(1));
    let capacity = chunks[1].height.saturating_sub(1) as usize;
    let visible = visible_window(app.mcp.servers.len(), selected, capacity);
    for (i, server) in app
        .mcp
        .servers
        .iter()
        .enumerate()
        .take(visible.end)
        .skip(visible.start)
    {
        lines.push(server_row_line(server, i == selected, chunks[1].width));
    }

    if app.mcp.servers.is_empty() {
        lines.push(Line::from(Span::styled(
            "  No MCP servers configured.",
            Style::default().fg(colors::inactive()),
        )));
    }

    let list = Paragraph::new(lines);
    f.render_widget(list, chunks[1]);

    // Tools for selected server
    let mut tool_lines: Vec<Line> = Vec::new();
    if !app.mcp.tools.is_empty() {
        tool_lines.push(Line::from(Span::styled(
            " Tools",
            Style::default().fg(colors::brand()),
        )));
        for tool in app
            .mcp
            .tools
            .iter()
            .take(chunks[2].height.saturating_sub(1) as usize)
        {
            let desc = tool.description.as_deref().unwrap_or("");
            tool_lines.push(Line::from(truncate_cells(
                &format!("  {} - {}", tool.name, desc),
                chunks[2].width as usize,
            )));
        }
    } else {
        tool_lines.push(Line::from(Span::styled(
            truncate_cells(
                &format!(
                    " Select a server and press {} for tools",
                    app.key_hint(ActionContext::Mcp, ActionId::ShowTools)
                ),
                chunks[2].width as usize,
            ),
            Style::default().fg(colors::inactive()),
        )));
    }
    let tools = Paragraph::new(tool_lines);
    f.render_widget(tools, chunks[2]);

    // Footer
    let footer_text = format!(
        " {} connect · {} tools · {} refresh",
        app.key_hint(ActionContext::Mcp, ActionId::Activate),
        app.key_hint(ActionContext::Mcp, ActionId::ShowTools),
        app.key_hint(ActionContext::Mcp, ActionId::Refresh),
    );
    let footer = Paragraph::new(footer_text).style(Style::default().fg(colors::inactive()));
    f.render_widget(footer, chunks[3]);
}

fn server_row_line(
    server: &crate::api::types::McpServer,
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
    let connected = server.connected.unwrap_or(false);
    let (connection_glyph, connection, connection_style) = if connected {
        ("●", "connected", Style::default().fg(colors::success()))
    } else {
        ("○", "disconnected", Style::default().fg(colors::error()))
    };
    let enabled = if server.enabled {
        "enabled"
    } else {
        "disabled"
    };
    let marker = if selected { "›" } else { " " };
    let name = server.name.as_deref().unwrap_or(&server.id);

    if width < 80 {
        // marker/glyph + two separators + status columns consume 30 cells.
        let name_width = width.saturating_sub(30).max(1) as usize;
        Line::from(vec![
            Span::styled(format!("{marker} {connection_glyph} "), row_style),
            Span::styled(truncate_cells(name, name_width), row_style),
            Span::styled(" · ", row_style),
            Span::styled(truncate_cells(connection, 12), connection_style),
            Span::styled(" · ", row_style),
            Span::styled(truncate_cells(enabled, 8), row_style),
        ])
    } else {
        let name_width = width.saturating_sub(40).max(1) as usize;
        let transport = server
            .transport
            .get("type")
            .and_then(serde_json::Value::as_str)
            .or_else(|| server.transport.as_str())
            .unwrap_or("stdio");
        Line::from(vec![
            Span::styled(format!("{marker} "), row_style),
            Span::styled(truncate_cells(name, name_width), row_style),
            Span::styled("  ", row_style),
            Span::styled(truncate_cells(transport, 12), row_style),
            Span::styled("  ", row_style),
            Span::styled(truncate_cells(connection, 12), connection_style),
            Span::styled("  ", row_style),
            Span::styled(truncate_cells(enabled, 8), row_style),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::types::McpServer;
    use crate::api::BambooClient;
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    fn server(index: usize) -> McpServer {
        McpServer {
            id: format!("server-{index}"),
            name: Some(format!("普通服务 {index}")),
            enabled: index.is_multiple_of(2),
            transport: serde_json::json!({"type": "stdio"}),
            connected: Some(index.is_multiple_of(3)),
        }
    }

    #[test]
    fn compact_unicode_mcp_keeps_selection_status_and_actions_visible() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = crate::app::Tab::Mcp;
        app.mcp.servers = (0..30).map(server).collect();
        app.mcp.selected = 24;
        app.mcp.servers[24].name = Some("selected-服务🧭e\u{301}".to_string());
        app.mcp.servers[24].connected = Some(false);
        app.mcp.servers[24].enabled = false;

        let row = server_row_line(&app.mcp.servers[24], true, 50);
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
        assert!(
            text.contains("disconnected"),
            "text status missing:\n{text}"
        );
        assert!(text.contains("disabled"), "enabled state missing:\n{text}");
        assert!(
            text.contains("Enter connect"),
            "compact action footer missing:\n{text}"
        );
        assert!(text.contains('›'), "selected-row glyph missing:\n{text}");
    }
}
