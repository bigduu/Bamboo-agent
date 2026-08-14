use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Wrap};
use ratatui::Frame;

use crate::app::{App, PermissionEditorMode};
use crate::keymap::{ActionContext, ActionId};
use crate::theme::colors;

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    let state = &app.config.permissions;
    let chunks = Layout::vertical([
        Constraint::Length(2),
        Constraint::Min(4),
        Constraint::Length(3),
        Constraint::Length(1),
    ])
    .split(area);

    let header = match &state.snapshot {
        Some(snapshot) => format!(
            "Policy rev {} · enabled {} · mode {} · {} durable / {} runtime grants",
            snapshot.revision,
            snapshot.policy.enabled,
            snapshot
                .policy
                .mode
                .map(|mode| mode.label())
                .unwrap_or("configured default"),
            snapshot.policy.durable_rules.len(),
            snapshot.temporary_grants.len(),
        ),
        None if state.loading => "Loading typed permission policy...".to_string(),
        None => "No permission policy loaded".to_string(),
    };
    let session = format!(
        "Session posture: {}{}",
        app.chat.permission_mode.label(),
        if app.chat.bypass_permissions {
            "  ⚠ BYPASS ACTIVE"
        } else {
            ""
        }
    );
    f.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(header, Style::default().fg(colors::brand()))),
            Line::from(Span::styled(
                session,
                if app.chat.bypass_permissions {
                    Style::default()
                        .fg(colors::error())
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(colors::inactive())
                },
            )),
        ]),
        chunks[0],
    );

    let rules = state
        .snapshot
        .as_ref()
        .map(|snapshot| {
            snapshot
                .policy
                .durable_rules
                .iter()
                .map(|rule| {
                    ListItem::new(Line::from(vec![
                        Span::styled(
                            format!("{} ", rule.id),
                            Style::default().fg(colors::brand()),
                        ),
                        Span::raw(format!(
                            "{:?}/{:?} · {} = {}",
                            rule.effect,
                            rule.scope,
                            rule.matcher.kind.label(),
                            crate::text::clip_cells(&rule.matcher.value, 72),
                        )),
                    ]))
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut list_state = ListState::default().with_selected(
        (!rules.is_empty()).then_some(state.selected.min(rules.len().saturating_sub(1))),
    );
    let title = if let Some(error) = &state.error {
        format!(
            " Permission rules · error: {} ",
            crate::text::clip_cells(error, 60)
        )
    } else {
        " Permission rules ".to_string()
    };
    f.render_stateful_widget(
        List::new(rules)
            .block(Block::default().borders(Borders::ALL).title(title))
            .highlight_style(
                Style::default()
                    .fg(colors::brand())
                    .add_modifier(Modifier::REVERSED),
            )
            .highlight_symbol("› "),
        chunks[1],
        &mut list_state,
    );

    let diagnosis = state
        .diagnosis
        .as_ref()
        .and_then(|value| serde_json::to_string_pretty(value).ok())
        .unwrap_or_else(|| {
            "No diagnosis result (x opens a non-consuming evaluator request).".into()
        });
    let diagnosis_lines = crate::text::hard_wrap(
        &diagnosis,
        chunks[2].width.saturating_sub(2).max(1) as usize,
    )
    .into_iter()
    .map(Line::from)
    .collect::<Vec<_>>();
    f.render_widget(
        Paragraph::new(diagnosis_lines)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" Last diagnosis "),
            )
            .wrap(Wrap { trim: false }),
        chunks[2],
    );

    let footer = format!(
        "{} new · {} edit · {} delete · {} diagnose · {} bypass · {} refresh · {} back",
        app.primary_key_hint(ActionContext::Permissions, ActionId::NewPermissionRule),
        app.primary_key_hint(ActionContext::Permissions, ActionId::EditPermissionRule),
        app.primary_key_hint(ActionContext::Permissions, ActionId::DeleteSelection),
        app.primary_key_hint(ActionContext::Permissions, ActionId::DiagnosePermission),
        app.primary_key_hint(ActionContext::Permissions, ActionId::TogglePermissionBypass),
        app.primary_key_hint(ActionContext::Permissions, ActionId::Refresh),
        app.primary_key_hint(ActionContext::Permissions, ActionId::Cancel),
    );
    f.render_widget(
        Paragraph::new(crate::text::clip_cells(&footer, chunks[3].width as usize))
            .style(Style::default().fg(colors::inactive())),
        chunks[3],
    );
}

pub fn render_editor(f: &mut Frame, app: &App) {
    let Some(editor) = &app.permission_editor else {
        return;
    };
    let area = centered(f.area(), 94, 90);
    f.render_widget(Clear, area);
    let title = match &editor.mode {
        PermissionEditorMode::Create => " Create permission rule ",
        PermissionEditorMode::Edit { .. } => " Edit permission rule ",
        PermissionEditorMode::Diagnose => " Diagnose permission (read-only) ",
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(colors::brand()))
        .title(title);
    let inner = block.inner(area);
    f.render_widget(block, area);
    let chunks = Layout::vertical([
        Constraint::Min(1),
        Constraint::Length(u16::from(editor.error.is_some())),
        Constraint::Length(1),
    ])
    .split(inner);
    f.render_widget(&editor.textarea, chunks[0]);
    if let Some(error) = &editor.error {
        f.render_widget(
            Paragraph::new(crate::text::clip_cells(error, chunks[1].width as usize))
                .style(Style::default().fg(colors::error())),
            chunks[1],
        );
    }
    let status = if editor.submitting {
        "Submitting...".to_string()
    } else {
        format!(
            "{} submit · {} cancel · expected policy rev {}",
            app.primary_key_hint(
                ActionContext::PermissionEditor,
                ActionId::SavePermissionForm
            ),
            app.primary_key_hint(ActionContext::PermissionEditor, ActionId::Cancel),
            editor.expected_revision,
        )
    };
    f.render_widget(
        Paragraph::new(crate::text::clip_cells(&status, chunks[2].width as usize))
            .style(Style::default().fg(colors::inactive())),
        chunks[2],
    );
}

pub fn render_rule_confirm(f: &mut Frame, app: &App) {
    let Some(confirm) = &app.permission_rule_confirm else {
        return;
    };
    let area = centered(f.area(), 92, 86);
    f.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(colors::error()))
        .title(" ⚠ CONFIRM GLOBAL ALLOW RULE ");
    let inner = block.inner(area);
    f.render_widget(block, area);
    let chunks = Layout::vertical([
        Constraint::Length(3),
        Constraint::Min(1),
        Constraint::Length(3),
    ])
    .split(inner);
    let mut warning = vec![
        Line::from(Span::styled(
            format!(
                "This {:?} grants authority in every session and workspace at policy revision {}.",
                confirm.mode, confirm.expected_revision
            ),
            Style::default()
                .fg(colors::error())
                .add_modifier(Modifier::BOLD),
        )),
        Line::raw("Review the complete exact rule below before the independent confirmation."),
    ];
    if let Some(error) = &confirm.error {
        warning.push(Line::from(Span::styled(
            error.clone(),
            Style::default().fg(colors::error()),
        )));
    }
    f.render_widget(
        Paragraph::new(warning).wrap(Wrap { trim: false }),
        chunks[0],
    );

    let exact = confirm.exact_text();
    let paragraph = Paragraph::new(exact).wrap(Wrap { trim: false });
    let wrapped_count = paragraph.line_count(chunks[1].width);
    let max_scroll =
        u16::try_from(wrapped_count.saturating_sub(chunks[1].height as usize)).unwrap_or(u16::MAX);
    confirm.max_scroll.set(max_scroll);
    f.render_widget(
        paragraph.scroll((confirm.scroll.min(max_scroll), 0)),
        chunks[1],
    );

    let status = if confirm.submitting {
        "Submitting confirmed global rule...".to_string()
    } else {
        format!(
            "{}/{} scroll · {} confirm · {} cancel",
            app.key_hint(ActionContext::PermissionRuleConfirm, ActionId::NavigateUp),
            app.key_hint(ActionContext::PermissionRuleConfirm, ActionId::NavigateDown),
            app.primary_key_hint(ActionContext::PermissionRuleConfirm, ActionId::Confirm),
            app.primary_key_hint(ActionContext::PermissionRuleConfirm, ActionId::Reject),
        )
    };
    f.render_widget(
        Paragraph::new(vec![
            Line::raw(status),
            Line::raw("Confirmation is invalidated by any policy revision conflict."),
        ])
        .wrap(Wrap { trim: false }),
        chunks[2],
    );
}

pub fn render_delete_confirm(f: &mut Frame, app: &App) {
    let Some(confirm) = &app.permission_delete else {
        return;
    };
    let area = centered(f.area(), 82, 65);
    f.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(colors::error()))
        .title(" Delete permission rule ");
    let inner = block.inner(area);
    f.render_widget(block, area);
    let mut lines = vec![
        Line::from(Span::styled(
            format!(
                "Delete '{}' at policy revision {}?",
                confirm.rule_id, confirm.expected_revision
            ),
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from(format!(
            "type/effect/scope: {} / {:?} / {:?}",
            confirm.rule.permission_type.label(),
            confirm.rule.effect,
            confirm.rule.scope
        )),
        Line::from(format!(
            "matcher: {} = {}",
            confirm.rule.matcher.kind.label(),
            confirm.rule.matcher.value
        )),
        Line::from(format!(
            "workspace: {} · source: {:?}",
            confirm.rule.workspace_path.as_deref().unwrap_or("global"),
            confirm.rule.source
        )),
        Line::from("This changes durable authorization policy."),
    ];
    if let Some(error) = &confirm.error {
        lines.push(Line::from(Span::styled(
            error.clone(),
            Style::default().fg(colors::error()),
        )));
    }
    lines.push(Line::from(if confirm.submitting {
        "Deleting...".to_string()
    } else {
        format!(
            "{} confirm · {} cancel",
            app.primary_key_hint(ActionContext::PermissionDeleteConfirm, ActionId::Confirm),
            app.primary_key_hint(ActionContext::PermissionDeleteConfirm, ActionId::Reject),
        )
    }));
    f.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), inner);
}

pub fn render_mode_confirm(f: &mut Frame, app: &App) {
    let Some(confirm) = &app.permission_mode_confirm else {
        return;
    };
    let area = centered(f.area(), 82, 48);
    f.render_widget(Clear, area);
    let enabling = confirm.to == crate::api::types::SessionPermissionMode::Bypass;
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(if enabling {
            colors::error()
        } else {
            colors::warning()
        }))
        .title(if enabling {
            " ⚠ ENABLE SESSION BYPASS "
        } else {
            " Disable session bypass "
        });
    let inner = block.inner(area);
    f.render_widget(block, area);
    let mut lines = vec![
        Line::from(Span::styled(
            format!(
                "Session {}: {} → {}",
                confirm.session_id,
                confirm.from.label(),
                confirm.to.label()
            ),
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from(if enabling {
            "Bypass skips ordinary approval checks. Forced confirmations and hard denials remain enforced."
        } else {
            "Ordinary permission prompts will be restored for this session."
        }),
        Line::from("This requires this separate confirmation and a fresh session revision."),
    ];
    if let Some(error) = &confirm.error {
        lines.push(Line::from(Span::styled(
            error.clone(),
            Style::default().fg(colors::error()),
        )));
    }
    lines.push(Line::from(if confirm.submitting {
        "Applying...".to_string()
    } else {
        format!(
            "{} confirm · {} cancel · {} refetch",
            app.primary_key_hint(ActionContext::PermissionModeConfirm, ActionId::Confirm),
            app.primary_key_hint(ActionContext::PermissionModeConfirm, ActionId::Reject),
            app.primary_key_hint(ActionContext::PermissionModeConfirm, ActionId::Refresh),
        )
    }));
    f.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), inner);
}

fn centered(r: Rect, width_percent: u16, height_percent: u16) -> Rect {
    let width = ((r.width as u32 * width_percent as u32 / 100) as u16).min(r.width);
    let height = ((r.height as u32 * height_percent as u32 / 100) as u16).min(r.height);
    Rect::new(
        r.x + r.width.saturating_sub(width) / 2,
        r.y + r.height.saturating_sub(height) / 2,
        width,
        height,
    )
}
