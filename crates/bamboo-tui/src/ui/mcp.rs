use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

use crate::app::App;
use crate::theme::colors;

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    if app.mcp.loading && app.mcp.servers.is_empty() {
        let loading =
            Paragraph::new("Loading MCP servers...").style(Style::default().fg(colors::INACTIVE));
        f.render_widget(loading, area);
        return;
    }

    if let Some(err) = &app.mcp.error {
        let error =
            Paragraph::new(format!("Error: {}", err)).style(Style::default().fg(colors::ERROR));
        f.render_widget(error, area);
        return;
    }

    let chunks = ratatui::layout::Layout::default()
        .direction(ratatui::layout::Direction::Vertical)
        .constraints([
            ratatui::layout::Constraint::Length(1),
            ratatui::layout::Constraint::Min(5),
            ratatui::layout::Constraint::Min(3),
            ratatui::layout::Constraint::Length(1),
        ])
        .split(area);

    // Header
    let header = Paragraph::new(Line::from(vec![
        Span::styled(
            " MCP Servers",
            Style::default()
                .fg(colors::BRAND)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("   "),
        Span::styled("[r] Refresh", Style::default().fg(colors::INACTIVE)),
        Span::raw("  "),
        Span::styled("[t] Tools", Style::default().fg(colors::INACTIVE)),
        Span::raw("  "),
        Span::styled(
            "[Enter] Connect/Disc",
            Style::default().fg(colors::INACTIVE),
        ),
    ]));
    f.render_widget(header, chunks[0]);

    // Server list
    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(Span::styled(
        " Name              Transport     Status      Enabled",
        Style::default().fg(colors::SUBTLE),
    )));

    for (i, server) in app.mcp.servers.iter().enumerate() {
        let style = if i == app.mcp.selected {
            Style::default()
                .fg(colors::BRAND)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(colors::INACTIVE)
        };

        let name = server
            .name
            .as_deref()
            .unwrap_or(&server.id)
            .chars()
            .take(18)
            .collect::<String>();
        let connected = server.connected.unwrap_or(false);
        let status_str = if connected {
            "Connected"
        } else {
            "Disconnected"
        };
        let status_style = if connected {
            Style::default().fg(colors::SUCCESS)
        } else {
            Style::default().fg(colors::ERROR)
        };

        lines.push(Line::from(vec![
            Span::styled(format!(" {:18}  ", name), style),
            Span::styled(format!("{:13}  ", "stdio"), style),
            Span::styled(format!("{:11} ", status_str), status_style),
            Span::styled(
                format!(" {}", if server.enabled { "Yes" } else { "No" }),
                style,
            ),
        ]));
    }

    if app.mcp.servers.is_empty() {
        lines.push(Line::from(Span::styled(
            "  No MCP servers configured.",
            Style::default().fg(colors::INACTIVE),
        )));
    }

    let list = Paragraph::new(lines);
    f.render_widget(list, chunks[1]);

    // Tools for selected server
    let mut tool_lines: Vec<Line> = Vec::new();
    if !app.mcp.tools.is_empty() {
        tool_lines.push(Line::from(Span::styled(
            " Tools",
            Style::default().fg(colors::BRAND),
        )));
        for tool in &app.mcp.tools {
            let desc = tool.description.as_deref().unwrap_or("");
            tool_lines.push(Line::from(format!("  {} - {}", tool.name, desc)));
        }
    } else {
        tool_lines.push(Line::from(Span::styled(
            " Select a server and press 't' to view tools",
            Style::default().fg(colors::INACTIVE),
        )));
    }
    let tools = Paragraph::new(tool_lines);
    f.render_widget(tools, chunks[2]);

    // Footer
    let footer = Paragraph::new(" [Enter] Connect/Disconnect · [t] Refresh Tools · [r] Refresh")
        .style(Style::default().fg(colors::INACTIVE));
    f.render_widget(footer, chunks[3]);
}
