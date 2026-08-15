use chrono::{DateTime, Utc};
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use ratatui::Frame;

use crate::api::types::SessionTreeKind;
use crate::app::App;
use crate::keymap::{ActionContext, ActionId};
use crate::subagents::{short_session_id, SubagentTreeNode, SubagentTreeState, SubagentTreeStatus};
use crate::theme::colors;

pub fn render(f: &mut Frame, app: &App) {
    let Some(tree) = &app.subagent_tree else {
        return;
    };
    let screen = f.area();
    let area = centered_rect(96, screen.height.saturating_sub(2).max(1), screen);
    let inner_width = area.width.saturating_sub(2) as usize;
    let footer = [
        format!(
            "{} / {} move · {} / {} branch · {} open",
            app.primary_key_hint(ActionContext::SubagentTree, ActionId::NavigateUp),
            app.primary_key_hint(ActionContext::SubagentTree, ActionId::NavigateDown),
            app.primary_key_hint(ActionContext::SubagentTree, ActionId::CollapseTreeNode),
            app.primary_key_hint(ActionContext::SubagentTree, ActionId::ExpandTreeNode),
            app.primary_key_hint(ActionContext::SubagentTree, ActionId::Activate),
        ),
        format!(
            "{} pending · {} refresh · {} close",
            app.primary_key_hint(ActionContext::SubagentTree, ActionId::OpenPendingRequest),
            app.primary_key_hint(ActionContext::SubagentTree, ActionId::Refresh),
            app.primary_key_hint(ActionContext::SubagentTree, ActionId::Cancel),
        ),
    ];
    let lines = tree_lines(
        tree,
        inner_width,
        area.height.saturating_sub(2) as usize,
        Utc::now(),
        &footer,
    );

    f.render_widget(Clear, area);
    f.render_widget(
        Paragraph::new(lines).block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(colors::brand()))
                .title(" Sub-agents "),
        ),
        area,
    );
}

fn tree_lines<'a>(
    tree: &SubagentTreeState,
    width: usize,
    height: usize,
    now: DateTime<Utc>,
    footer: &'a [String; 2],
) -> Vec<Line<'a>> {
    let compact = width < 76;
    let selected_path = tree
        .selected_id()
        .map(|id| tree.breadcrumb(id).join(" › "))
        .unwrap_or_else(|| "loading".to_string());
    let active_path = tree.breadcrumb(&tree.active_session_id).join(" › ");
    let mut lines = vec![Line::from(vec![
        Span::styled(
            " Session graph ",
            Style::default()
                .fg(colors::brand())
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("{} nodes", tree.graph_node_count()),
            Style::default().fg(colors::subtle()),
        ),
    ])];
    lines.push(Line::from(vec![
        Span::styled(" Active path: ", Style::default().fg(colors::subtle())),
        Span::styled(
            truncate(&active_path, width.saturating_sub(14)),
            Style::default().fg(colors::inactive()),
        ),
    ]));

    if tree.loading_root {
        lines.push(Line::raw(" Loading active session metadata..."));
    } else if tree.loading_page {
        lines.push(Line::raw(format!(
            " Scanning session index: {} / {}...",
            tree.scanned, tree.total
        )));
    } else {
        lines.push(Line::raw(format!(
            " Scanned {} / {}{}",
            tree.scanned,
            tree.total,
            if tree.capped {
                " · memory cap reached"
            } else {
                ""
            }
        )));
    }
    if let Some(error) = &tree.error {
        lines.push(Line::from(Span::styled(
            format!(" ! {}", truncate(error, width.saturating_sub(3))),
            Style::default().fg(colors::error()),
        )));
    }

    let detail_rows = tree
        .selected_node()
        .map(|node| 4 + usize::from(compact || node.error().is_some()))
        .unwrap_or(1);
    let fixed_rows = lines.len() + detail_rows + footer.len() + 1;
    // Reserve both overflow indicators. Otherwise a long tree can push the
    // selected details or keyboard footer below the viewport by two rows.
    let list_capacity = height.saturating_sub(fixed_rows.saturating_add(2)).max(1);
    let total = tree.visible.len();
    let selected = tree.selected.min(total.saturating_sub(1));
    let mut start = selected.saturating_sub(list_capacity / 2);
    if start + list_capacity > total {
        start = total.saturating_sub(list_capacity);
    }
    let end = (start + list_capacity).min(total);
    if start > 0 {
        lines.push(Line::from(Span::styled(
            format!(" ↑ {start} hidden"),
            Style::default().fg(colors::subtle()),
        )));
    }
    for (index, row) in tree
        .visible
        .iter()
        .enumerate()
        .skip(start)
        .take(end.saturating_sub(start))
    {
        let Some(node) = tree.nodes.get(&row.session_id) else {
            continue;
        };
        lines.push(tree_row(
            node,
            row.depth,
            row.has_children,
            row.expanded,
            row.session_id == tree.active_session_id,
            index == selected,
            width,
            compact,
        ));
    }
    if end < total {
        lines.push(Line::from(Span::styled(
            format!(" ↓ {} hidden", total - end),
            Style::default().fg(colors::subtle()),
        )));
    }

    lines.push(Line::raw(""));
    if let Some(node) = tree.selected_node() {
        lines.extend(selected_details(
            node,
            &tree.root_session_id,
            &selected_path,
            width,
            compact,
            now,
        ));
    } else {
        lines.push(Line::raw(" No session rows loaded"));
    }

    for footer in footer {
        lines.push(Line::from(Span::styled(
            format!(" {}", truncate(footer, width.saturating_sub(1))),
            Style::default().fg(colors::subtle()),
        )));
    }
    lines
}

#[allow(clippy::too_many_arguments)]
fn tree_row<'a>(
    node: &SubagentTreeNode,
    depth: usize,
    has_children: bool,
    expanded: bool,
    active: bool,
    selected: bool,
    width: usize,
    compact: bool,
) -> Line<'a> {
    let status = node.status();
    let branch = if has_children {
        if expanded {
            "▾"
        } else {
            "▸"
        }
    } else {
        "•"
    };
    let depth = depth.min(6);
    let indent = if depth < 6 {
        "  ".repeat(depth)
    } else {
        "… ".to_string() + &"  ".repeat(4)
    };
    let active = if active { "*" } else { " " };
    let status_text = if compact {
        format!("{} {}", status.glyph(), status.label())
    } else {
        format!("{} {:<22}", status.glyph(), status.label())
    };
    let prefix = format!(" {active}{indent}{branch} {status_text} ");
    let id = short_session_id(&node.summary.id);
    let suffix = format!("{} [{id}]", node.title());
    let style = if selected {
        Style::default().add_modifier(Modifier::REVERSED | Modifier::BOLD)
    } else {
        Style::default()
    };
    let status_style = style.fg(status_color(status));
    Line::from(vec![
        Span::styled(truncate(&prefix, width), status_style),
        Span::styled(
            truncate(&suffix, width.saturating_sub(display_width(&prefix))),
            style,
        ),
    ])
}

fn selected_details<'a>(
    node: &SubagentTreeNode,
    root_session_id: &str,
    selected_path: &str,
    width: usize,
    compact: bool,
    now: DateTime<Utc>,
) -> Vec<Line<'a>> {
    let role = node
        .summary
        .subagent_type
        .as_deref()
        .unwrap_or(match node.summary.kind {
            SessionTreeKind::Root => "root",
            SessionTreeKind::Child => "child",
            SessionTreeKind::Unknown => "unknown role",
        });
    let lifecycle = match (
        node.summary.lifecycle.as_deref(),
        node.summary.resident_name.as_deref(),
    ) {
        (Some("resident"), Some(name)) => format!("resident:{name}"),
        (Some(value), _) => value.to_string(),
        (None, _) if node.summary.kind == SessionTreeKind::Child => "one-shot".to_string(),
        (None, _) => "root".to_string(),
    };
    let placement = match (
        node.summary.placement.kind.trim(),
        node.summary.placement.host.trim(),
    ) {
        ("", "") => "placement unknown".to_string(),
        ("", host) => host.to_string(),
        (kind, "") => kind.to_string(),
        (kind, host) => format!("{kind}@{host}"),
    };
    let updated = node
        .last_update()
        .map(|at| relative_age(now, at))
        .unwrap_or_else(|| "update unknown".to_string());
    let round = node
        .round_count
        .map(|round| format!("round {round}"))
        .or_else(|| node.activity.clone())
        .unwrap_or_else(|| "no live activity".to_string());
    let metadata = if node.metadata_incomplete(root_session_id) {
        " · legacy metadata incomplete"
    } else {
        ""
    };
    let mut lines = vec![Line::from(vec![
        Span::styled(" Selected path: ", Style::default().fg(colors::subtle())),
        Span::raw(truncate(selected_path, width.saturating_sub(16))),
    ])];
    if compact {
        lines.push(Line::raw(format!(
            " {} · {}{}",
            truncate(&format!("{role} · {lifecycle}"), width.saturating_sub(2)),
            node.status().label(),
            metadata,
        )));
        lines.push(Line::raw(format!(
            " {}",
            truncate(
                &format!("{placement} · model {}", fallback(&node.summary.model)),
                width.saturating_sub(1)
            )
        )));
        lines.push(Line::raw(format!(
            " {} · {updated}",
            truncate(&round, width / 2)
        )));
    } else {
        lines.push(Line::raw(format!(
            " Role/lifecycle: {role} · {lifecycle}{metadata}"
        )));
        lines.push(Line::raw(format!(
            " Placement/model: {}",
            truncate(
                &format!("{placement} · {}", fallback(&node.summary.model)),
                width.saturating_sub(18)
            )
        )));
        lines.push(Line::raw(format!(" Activity: {round} · {updated}")));
    }
    if let Some(error) = node.error() {
        lines.push(Line::from(Span::styled(
            format!(" Error: {}", truncate(error, width.saturating_sub(8))),
            Style::default().fg(colors::error()),
        )));
    } else if compact {
        lines.push(Line::raw(""));
    }
    lines
}

fn fallback(value: &str) -> &str {
    if value.trim().is_empty() {
        "unknown"
    } else {
        value
    }
}

fn relative_age(now: DateTime<Utc>, at: DateTime<Utc>) -> String {
    let seconds = now.signed_duration_since(at).num_seconds().max(0);
    if seconds < 5 {
        "updated now".to_string()
    } else if seconds < 60 {
        format!("updated {seconds}s ago")
    } else if seconds < 3_600 {
        format!("updated {}m ago", seconds / 60)
    } else if seconds < 86_400 {
        format!("updated {}h ago", seconds / 3_600)
    } else {
        format!("updated {}d ago", seconds / 86_400)
    }
}

fn status_color(status: SubagentTreeStatus) -> ratatui::style::Color {
    match status {
        SubagentTreeStatus::Running => colors::tool_running(),
        SubagentTreeStatus::WaitingForInput | SubagentTreeStatus::WaitingForPermission => {
            colors::warning()
        }
        SubagentTreeStatus::Completed => colors::success(),
        SubagentTreeStatus::Cancelled => colors::inactive(),
        SubagentTreeStatus::Error => colors::error(),
        SubagentTreeStatus::Idle => colors::subtle(),
    }
}

fn centered_rect(percent_x: u16, height: u16, area: Rect) -> Rect {
    let width = (area.width as u32 * percent_x as u32 / 100) as u16;
    let x = area.width.saturating_sub(width) / 2;
    let y = area.height.saturating_sub(height) / 2;
    Rect::new(
        area.x + x,
        area.y + y,
        width.min(area.width),
        height.min(area.height),
    )
}

fn truncate(value: &str, width: usize) -> String {
    crate::text::truncate_cells(value, width)
}

fn display_width(value: &str) -> usize {
    crate::text::display_width(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::types::{SessionTreeKind, SessionTreeSummary};
    use crate::api::BambooClient;
    use crate::app::App;
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    fn summary(id: &str, parent: Option<&str>, depth: u32) -> SessionTreeSummary {
        let mut value = SessionTreeSummary::placeholder(id);
        value.kind = if parent.is_some() {
            SessionTreeKind::Child
        } else {
            SessionTreeKind::Root
        };
        value.title = match id {
            "root" => "Root session",
            "child" => "Research child",
            _ => "Nested reviewer",
        }
        .to_string();
        value.parent_session_id = parent.map(str::to_string);
        value.root_session_id = "root".to_string();
        value.spawn_depth = depth;
        value.model = "gpt-5".to_string();
        value
    }

    fn app_with_tree() -> App {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.session_id = Some("child".to_string());
        let mut tree = SubagentTreeState::new(1, "child".to_string());
        tree.install_root(summary("child", Some("root"), 1));
        let mut child = summary("child", Some("root"), 1);
        child.subagent_type = Some("researcher".to_string());
        child.lifecycle = Some("resident".to_string());
        child.resident_name = Some("research".to_string());
        child.placement.kind = "ssh".to_string();
        child.placement.host = "worker.example".to_string();
        child.has_pending_question = true;
        tree.install_page(
            vec![
                summary("root", None, 0),
                child,
                summary("grandchild", Some("child"), 2),
            ],
            3,
            100,
            0,
            None,
        );
        app.subagent_tree = Some(tree);
        app
    }

    #[test]
    fn tree_overlay_is_responsive_and_never_hides_identity_behind_color() {
        for width in [60, 80, 120] {
            let app = app_with_tree();
            let mut terminal = Terminal::new(TestBackend::new(width, 24)).unwrap();
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
            assert!(text.contains("Active path"), "{width}: {text}");
            assert!(text.contains("waiting for input"), "{width}: {text}");
            assert!(text.contains("Research child"), "{width}: {text}");
        }
    }

    #[test]
    fn long_tree_keeps_details_and_keyboard_footer_inside_the_viewport() {
        let mut tree = SubagentTreeState::new(1, "root".to_string());
        tree.install_root(summary("root", None, 0));
        let mut sessions = vec![summary("root", None, 0)];
        sessions.extend((0..30).map(|index| {
            let id = format!("child-{index:02}");
            summary(&id, Some("root"), 1)
        }));
        tree.install_page(sessions, 31, 100, 0, None);
        tree.select_last();
        let footer = [
            "move · branch · open".to_string(),
            "pending · refresh · close".to_string(),
        ];
        let height = 18;

        let lines = tree_lines(&tree, 58, height, Utc::now(), &footer);

        assert!(
            lines.len() <= height,
            "{} lines exceeded {height}",
            lines.len()
        );
        let text = lines
            .iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("Selected path"), "{text}");
        assert!(text.contains("pending · refresh · close"), "{text}");
    }
}
