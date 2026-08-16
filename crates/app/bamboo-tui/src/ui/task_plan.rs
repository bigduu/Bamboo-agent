use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use ratatui::Frame;

use crate::api::types::{PlanModeStatus, TaskItem, TaskItemStatus};
use crate::app::{App, ConnectionStatus, PlanRunStatus};
use crate::keymap::{ActionContext, ActionId};
use crate::task_plan::{TaskPlanPane, TaskProgressState};
use crate::text::{clip_cells, hard_wrap};
use crate::theme::colors;

pub fn render(f: &mut Frame, app: &App) {
    let Some(overlay) = &app.task_plan else {
        return;
    };
    let screen = f.area();
    let area = centered_rect(96, screen.height.saturating_sub(2).max(1), screen);
    let inner_width = area.width.saturating_sub(2) as usize;
    let inner_height = area.height.saturating_sub(2) as usize;
    let lines = task_plan_lines(app, inner_width, inner_height);

    f.render_widget(Clear, area);
    f.render_widget(
        Paragraph::new(lines).block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(colors::brand()))
                .title(match overlay.pane {
                    TaskPlanPane::Tasks => " Tasks / Plan · Tasks ",
                    TaskPlanPane::Plan => " Tasks / Plan · Plan ",
                }),
        ),
        area,
    );
}

fn task_plan_lines(app: &App, width: usize, height: usize) -> Vec<Line<'static>> {
    let Some(overlay) = &app.task_plan else {
        return Vec::new();
    };
    let footer = [
        format!(
            "{} / {} move · {} switch pane · {} refresh",
            app.primary_key_hint(ActionContext::TaskPlan, ActionId::NavigateUp),
            app.primary_key_hint(ActionContext::TaskPlan, ActionId::NavigateDown),
            app.primary_key_hint(ActionContext::TaskPlan, ActionId::ToggleInspectorPane),
            app.primary_key_hint(ActionContext::TaskPlan, ActionId::Refresh),
        ),
        format!(
            "{} / {} page · {} close",
            app.primary_key_hint(ActionContext::TaskPlan, ActionId::PageUp),
            app.primary_key_hint(ActionContext::TaskPlan, ActionId::PageDown),
            app.primary_key_hint(ActionContext::TaskPlan, ActionId::Cancel),
        ),
    ];
    let mut lines = vec![Line::from(vec![
        Span::styled(
            if overlay.pane == TaskPlanPane::Tasks {
                " [Tasks] "
            } else {
                "  Tasks  "
            },
            pane_style(overlay.pane == TaskPlanPane::Tasks),
        ),
        Span::styled(
            if overlay.pane == TaskPlanPane::Plan {
                " [Plan] "
            } else {
                "  Plan  "
            },
            pane_style(overlay.pane == TaskPlanPane::Plan),
        ),
        Span::styled(
            format!(
                "  session {}",
                short_session(app.chat.session_id.as_deref())
            ),
            Style::default().fg(colors::subtle()),
        ),
    ])];

    let connection = app.active_connection_status();
    if connection == ConnectionStatus::Offline {
        lines.push(Line::from(Span::styled(
            " ○ Offline — showing the last authoritative snapshot; refresh will retry",
            Style::default().fg(colors::warning()),
        )));
    }
    if app.chat.task_progress.loading {
        lines.push(Line::from(Span::styled(
            " ◌ Synchronizing task and plan state...",
            Style::default().fg(colors::inactive()),
        )));
    }
    if let Some(error) = &app.chat.task_progress.error {
        lines.push(Line::from(Span::styled(
            format!(" ! {}", clip_cells(error, width.saturating_sub(3))),
            Style::default().fg(colors::error()),
        )));
    }

    let content_height = height.saturating_sub(lines.len() + footer.len());
    match overlay.pane {
        TaskPlanPane::Tasks => lines.extend(task_lines(
            &app.chat.task_progress,
            overlay.selected,
            width,
            content_height,
        )),
        TaskPlanPane::Plan => lines.extend(plan_lines(
            &app.chat.run_status.plan,
            overlay.detail_scroll,
            width,
            content_height,
        )),
    }
    while lines.len() + footer.len() < height {
        lines.push(Line::raw(""));
    }
    lines.extend(
        footer
            .into_iter()
            .map(|line| Line::from(Span::styled(clip_cells(&line, width), footer_style()))),
    );
    lines.truncate(height);
    lines
}

fn task_lines(
    state: &TaskProgressState,
    selected: usize,
    width: usize,
    height: usize,
) -> Vec<Line<'static>> {
    let ordered = state.ordered_items();
    let mut lines = vec![Line::from(vec![
        Span::styled(" Progress ", Style::default().fg(colors::subtle())),
        Span::styled(
            format!(
                "{}/{} · {}% · version {}",
                state.progress.completed,
                state.progress.total,
                state.progress.percentage,
                state.version
            ),
            Style::default().fg(if state.completed {
                colors::success()
            } else {
                colors::inactive()
            }),
        ),
    ])];
    if let Some(evaluation) = &state.evaluation {
        lines.push(Line::raw(format!(
            " Evaluation: {}",
            clip_cells(evaluation, width.saturating_sub(13))
        )));
    }
    if let Some(completion) = &state.completion_summary {
        lines.push(Line::from(Span::styled(
            format!(
                " Completion: {}",
                clip_cells(completion, width.saturating_sub(13))
            ),
            Style::default().fg(colors::success()),
        )));
    }
    if ordered.is_empty() {
        lines.push(Line::from(Span::styled(
            " No task list has been created for this session.",
            Style::default().fg(colors::inactive()),
        )));
        return lines;
    }

    let selected = selected.min(ordered.len().saturating_sub(1));
    let compact = width < 78;
    let detail_budget = if compact { 7 } else { 9 }.min(height.saturating_sub(lines.len() + 2));
    let list_budget = height
        .saturating_sub(lines.len() + detail_budget + 1)
        .max(1);
    let mut start = selected.saturating_sub(list_budget / 2);
    if start + list_budget > ordered.len() {
        start = ordered.len().saturating_sub(list_budget);
    }
    for (row, (depth, item)) in ordered.iter().enumerate().skip(start).take(list_budget) {
        let selected_row = row == selected;
        let indent = "  ".repeat((*depth).min(5));
        let dependency = if compact || item.depends_on.is_empty() {
            String::new()
        } else {
            format!(" · deps {}", item.depends_on.join(","))
        };
        let prefix = format!(
            " {}{} {:<11} {}",
            if selected_row { "›" } else { " " },
            status_icon(item.status),
            task_status_label(item.status),
            indent
        );
        let available = width.saturating_sub(crate::text::display_width(&prefix));
        let label = clip_cells(&format!("{}{}", task_label(item), dependency), available);
        lines.push(Line::from(vec![
            Span::styled(prefix, status_style(item.status, selected_row)),
            Span::styled(
                label,
                if selected_row {
                    Style::default().add_modifier(Modifier::REVERSED)
                } else {
                    Style::default()
                },
            ),
        ]));
    }
    lines.push(Line::from(Span::styled(
        " ─ Selected task ─",
        Style::default().fg(colors::subtle()),
    )));
    lines.extend(task_detail_lines(ordered[selected].1, width, detail_budget));
    lines
}

fn task_detail_lines(item: &TaskItem, width: usize, budget: usize) -> Vec<Line<'static>> {
    let mut values = Vec::new();
    values.push(format!(
        " ID: {} · phase {:?} · priority {:?}",
        item.id, item.phase, item.priority
    ));
    if !item.depends_on.is_empty() {
        values.push(format!(" Depends on: {}", item.depends_on.join(", ")));
    }
    if let Some(parent) = &item.parent_id {
        values.push(format!(" Parent: {parent}"));
    }
    if !item.completion_criteria.is_empty() {
        values.push(format!(
            " Completion criteria: {}",
            item.completion_criteria.join(" · ")
        ));
    }
    for blocker in &item.blockers {
        values.push(format!(
            " BLOCKED: {}{}",
            blocker.summary,
            blocker
                .waiting_on
                .as_ref()
                .map(|target| format!(" · waiting on {target}"))
                .unwrap_or_default()
        ));
    }
    if !item.notes.trim().is_empty() {
        values.push(format!(" Notes: {}", item.notes.trim()));
    }
    if let Some(evidence) = item.evidence.last() {
        values.push(format!(" Evidence: {}", evidence.summary));
    }
    if let Some(transition) = item.transitions.last() {
        let reason = transition
            .reason
            .as_ref()
            .map(|reason| format!(" · {reason}"))
            .unwrap_or_default();
        let round = transition
            .round
            .map(|round| format!(" · round {round}"))
            .unwrap_or_default();
        let changed_at = if transition.changed_at.is_empty() {
            String::new()
        } else {
            format!(" · {}", transition.changed_at)
        };
        values.push(format!(
            " Transition: {} → {}{reason}{round}{changed_at}",
            task_status_label(transition.from_status),
            task_status_label(transition.to_status),
        ));
    }
    if values.len() == 1 {
        values.push(" No blockers, notes, or evidence recorded.".to_string());
    }
    values
        .into_iter()
        .flat_map(|value| hard_wrap(&value, width.max(1)))
        .take(budget)
        .map(Line::raw)
        .collect()
}

fn plan_lines(
    plan: &PlanRunStatus,
    scroll: usize,
    width: usize,
    height: usize,
) -> Vec<Line<'static>> {
    let phase = plan
        .status
        .map(plan_status_label)
        .unwrap_or(if plan.active { "active" } else { "inactive" });
    let mut values = vec![format!(
        " {} Plan mode · {phase}",
        if plan.active { "●" } else { "○" }
    )];
    if let Some(outcome) = &plan.last_outcome {
        values.push(format!(" Outcome: {outcome}"));
    }
    if let Some(reason) = &plan.reason {
        values.push(format!(" Reason: {reason}"));
    }
    if let Some(path) = &plan.file_path {
        values.push(format!(" File: {path}"));
    }
    if let Some(entered_at) = &plan.entered_at {
        values.push(format!(" Entered: {entered_at}"));
    }
    if let Some(mode) = &plan.pre_permission_mode {
        values.push(format!(" Restores permission mode: {mode}"));
    }
    if let Some(summary) = &plan.content_summary {
        values.push(format!(" Summary: {summary}"));
    }
    if let Some(content) = &plan.plan {
        values.push(" ─ Reviewed plan ─".to_string());
        values.extend(content.lines().map(|line| format!(" {line}")));
    }
    if values.len() == 1 && !plan.active {
        values.push(" No plan lifecycle has been recorded for this session.".to_string());
    }
    let wrapped = values
        .into_iter()
        .flat_map(|value| hard_wrap(&value, width.max(1)))
        .collect::<Vec<_>>();
    let max_start = wrapped.len().saturating_sub(height);
    let start = scroll.min(max_start);
    wrapped
        .into_iter()
        .skip(start)
        .take(height)
        .map(Line::raw)
        .collect()
}

fn task_label(item: &TaskItem) -> &str {
    item.active_form
        .as_deref()
        .filter(|_| item.status == TaskItemStatus::InProgress)
        .unwrap_or(&item.description)
}

fn status_icon(status: TaskItemStatus) -> &'static str {
    match status {
        TaskItemStatus::Pending => "○",
        TaskItemStatus::InProgress => "●",
        TaskItemStatus::Completed => "✓",
        TaskItemStatus::Blocked => "!",
        TaskItemStatus::Unknown => "?",
    }
}

fn task_status_label(status: TaskItemStatus) -> &'static str {
    match status {
        TaskItemStatus::Pending => "pending",
        TaskItemStatus::InProgress => "in progress",
        TaskItemStatus::Completed => "completed",
        TaskItemStatus::Blocked => "blocked",
        TaskItemStatus::Unknown => "unknown",
    }
}

fn status_style(status: TaskItemStatus, selected: bool) -> Style {
    let color = match status {
        TaskItemStatus::Completed => colors::success(),
        TaskItemStatus::Blocked => colors::error(),
        TaskItemStatus::InProgress => colors::brand(),
        TaskItemStatus::Pending | TaskItemStatus::Unknown => colors::inactive(),
    };
    let style = Style::default().fg(color);
    if selected {
        style.add_modifier(Modifier::BOLD)
    } else {
        style
    }
}

fn plan_status_label(status: PlanModeStatus) -> &'static str {
    match status {
        PlanModeStatus::Exploring => "exploring",
        PlanModeStatus::Designing => "designing",
        PlanModeStatus::Reviewing => "reviewing",
        PlanModeStatus::Finalizing => "finalizing",
        PlanModeStatus::AwaitingApproval => "awaiting approval",
        PlanModeStatus::Unknown => "unknown",
    }
}

fn pane_style(active: bool) -> Style {
    if active {
        Style::default()
            .fg(colors::brand())
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(colors::inactive())
    }
}

fn footer_style() -> Style {
    Style::default().fg(colors::subtle())
}

fn short_session(session_id: Option<&str>) -> String {
    session_id
        .map(|id| id.chars().take(12).collect())
        .unwrap_or_else(|| "not started".to_string())
}

fn centered_rect(percent_x: u16, height: u16, area: Rect) -> Rect {
    let width = area.width.saturating_mul(percent_x).saturating_div(100);
    let height = height.min(area.height);
    Rect::new(
        area.x + area.width.saturating_sub(width) / 2,
        area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::types::TaskList;
    use crate::api::BambooClient;
    use crate::task_plan::TaskPlanOverlayState;
    use bamboo_client_core::{TaskBlocker, TaskBlockerKind, TaskTransition};
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    fn app_with_tasks() -> App {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:1"));
        app.connected = true;
        app.chat.session_id = Some("session-unicode".to_string());
        app.chat.task_progress.requested_session_id = app.chat.session_id.clone();
        app.chat.task_progress.owner_session_id = app.chat.session_id.clone();
        app.chat.task_progress.task_list = Some(TaskList {
            session_id: "session-unicode".to_string(),
            title: "Release".to_string(),
            items: vec![
                TaskItem {
                    id: "root".to_string(),
                    description: "实现非常长的 Unicode 任务 👨‍👩‍👧‍👦，并验证不会破坏终端布局".repeat(3),
                    status: TaskItemStatus::InProgress,
                    active_form: Some("正在验证任务进度视图".to_string()),
                    depends_on: vec!["setup".to_string()],
                    ..TaskItem::default()
                },
                TaskItem {
                    id: "child".to_string(),
                    description: "等待 API".to_string(),
                    status: TaskItemStatus::Blocked,
                    parent_id: Some("root".to_string()),
                    blockers: vec![TaskBlocker {
                        kind: TaskBlockerKind::Dependency,
                        summary: "schema review".to_string(),
                        waiting_on: Some("backend contract".to_string()),
                    }],
                    ..TaskItem::default()
                },
            ],
            ..TaskList::default()
        });
        app.chat.task_progress.progress.total = 2;
        app.chat.task_progress.version = 9;
        app.task_plan = Some(TaskPlanOverlayState {
            selected: 1,
            ..TaskPlanOverlayState::default()
        });
        app
    }

    #[test]
    fn task_overlay_renders_at_minimum_and_wide_widths() {
        for width in [60, 80, 120] {
            let app = app_with_tasks();
            let backend = TestBackend::new(width, 20);
            let mut terminal = Terminal::new(backend).unwrap();
            terminal.draw(|frame| render(frame, &app)).unwrap();
            let rendered: String = terminal
                .backend()
                .buffer()
                .content()
                .iter()
                .map(|cell| cell.symbol())
                .collect();
            assert!(rendered.contains("Tasks / Plan"));
            assert!(rendered.contains("blocked"));
            assert!(rendered.contains("waiting on backend contract"));
        }
    }

    #[test]
    fn plan_pane_distinguishes_approval_and_rejection() {
        let mut app = app_with_tasks();
        app.task_plan.as_mut().unwrap().pane = TaskPlanPane::Plan;
        app.chat.run_status.plan.last_outcome = Some("rejected or exited".to_string());
        app.chat.run_status.plan.status = Some(PlanModeStatus::AwaitingApproval);
        let lines = task_plan_lines(&app, 58, 18);
        let text = lines
            .iter()
            .map(Line::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("awaiting approval"));
        assert!(text.contains("rejected or exited"));
    }

    #[test]
    fn task_details_render_completion_criteria_and_latest_transition() {
        let mut task = TaskItem {
            id: "verify".to_string(),
            description: "Verify release".to_string(),
            status: TaskItemStatus::Completed,
            completion_criteria: vec!["all checks pass".to_string()],
            ..TaskItem::default()
        };
        task.transitions.push(TaskTransition {
            from_status: TaskItemStatus::InProgress,
            to_status: TaskItemStatus::Completed,
            reason: Some("CI succeeded".to_string()),
            round: Some(4),
            changed_at: "2026-08-16T00:00:00Z".to_string(),
        });

        let text = task_detail_lines(&task, 120, 10)
            .iter()
            .map(Line::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("Completion criteria: all checks pass"));
        assert!(text.contains("Transition: in progress → completed"));
        assert!(text.contains("CI succeeded · round 4"));
    }
}
