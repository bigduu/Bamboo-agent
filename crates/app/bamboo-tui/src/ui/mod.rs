pub mod chat;
mod code_fences;
pub mod config;
pub mod layout;
pub mod mcp;
pub mod permissions;
pub mod schedules;
pub mod sessions;
pub mod skills;
pub mod subagents;
pub mod task_plan;
pub(crate) mod workspace;

use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::Frame;

use crate::app::App;

pub const MIN_TERMINAL_WIDTH: u16 = 60;
pub const MIN_TERMINAL_HEIGHT: u16 = 20;

pub fn render(f: &mut Frame, app: &App) {
    crate::theme::with_palette(app.theme, || render_with_palette(f, app));
}

fn render_with_palette(f: &mut Frame, app: &App) {
    if f.area().width < MIN_TERMINAL_WIDTH || f.area().height < MIN_TERMINAL_HEIGHT {
        layout::render_terminal_too_small(f, app);
        return;
    }

    let chunks = layout::app_layout(f.area(), app);

    // Content area
    match app.tab {
        crate::app::Tab::Chat => {
            chat::render(f, chunks.content, chunks.input, app);
            layout::render_session_strip(f, chunks.session_strip, app);
        }
        _ => render_tab(f, chunks.content, app),
    }

    layout::render_status_info(f, chunks.status_info, app);
    layout::render_tab_bar(f, chunks.status_tabs, app);
    render_overlays(f, app, None, None);
}

pub(crate) fn render_workspace(f: &mut Frame, app: &App, body: Rect, status: Rect) {
    crate::theme::with_palette(app.theme, || {
        let (anchor, inline_approval) = if app.tab == crate::app::Tab::Chat {
            let input_height = app.chat.textarea.lines().len().clamp(2, 5) as u16
                + u16::from(!app.chat.composer_attachments.is_empty());
            let approval_height = layout::inline_approval_height(
                app,
                body.width,
                body.height.saturating_sub(input_height + 2),
            );
            let [transcript, approval, input] = Layout::vertical([
                Constraint::Min(1),
                Constraint::Length(approval_height),
                Constraint::Length(input_height),
            ])
            .areas(body);
            chat::render_workspace(f, transcript, input, app);
            (
                Rect::new(body.x, body.y, body.width, input.y.saturating_sub(body.y)),
                (approval_height > 0).then_some(approval),
            )
        } else {
            render_tab(f, body, app);
            (body, None)
        };
        layout::render_status_info(f, status, app);
        render_overlays(f, app, Some(anchor), inline_approval);
    });
}

fn render_tab(f: &mut Frame, area: Rect, app: &App) {
    match app.tab {
        crate::app::Tab::Chat => {}
        crate::app::Tab::Sessions => sessions::render(f, area, app),
        crate::app::Tab::Mcp => mcp::render(f, area, app),
        crate::app::Tab::Schedules => schedules::render(f, area, app),
        crate::app::Tab::Skills => skills::render(f, area, app),
        crate::app::Tab::Config if app.config.view == crate::app::ConfigView::Permissions => {
            permissions::render(f, area, app)
        }
        crate::app::Tab::Config => config::render(f, area, app),
    }
}

fn render_overlays(f: &mut Frame, app: &App, anchor: Option<Rect>, inline_approval: Option<Rect>) {
    // Help overlay
    if app.help_visible {
        layout::render_help(f, app);
    }

    // A clarification can arrive asynchronously while a local editor or
    // confirmation is already open. Draw from lowest to highest input
    // precedence so the modal that owns the keyboard is always the visible
    // frontmost modal (the reverse of `App::handle_key`'s routing order).
    if app.terminal_settings.is_some() {
        workspace::render_settings(f, app, anchor.unwrap_or_else(|| f.area()));
    }
    if app.config_editor.is_some() {
        config::render_editor(f, app);
    }

    if app.permission_editor.is_some() {
        permissions::render_editor(f, app);
    }

    if app.permission_rule_confirm.is_some() {
        permissions::render_rule_confirm(f, app);
    }

    code_fences::render(f, app);
    schedules::render_runs(f, app);

    if app.schedule_form.is_some() {
        layout::render_schedule_form(f, app);
    }

    if app.command_palette.is_some() {
        layout::render_command_palette_at(f, app, anchor);
    }

    if app.model_picker.is_some() {
        if let Some(anchor) = anchor {
            layout::render_model_popover(f, app, anchor);
        } else {
            layout::render_model_picker(f, app);
        }
    }

    if app.session_picker.is_some() {
        layout::render_session_picker(f, app);
    }

    if app.subagent_tree.is_some() {
        subagents::render(f, app);
    }

    if app.task_plan.is_some() {
        task_plan::render(f, app);
    }

    if app.pending_delete.is_some() || app.pending_schedule_delete.is_some() {
        layout::render_delete_confirm(f, app);
    }

    if app.permission_mode_confirm.is_some() {
        permissions::render_mode_confirm(f, app);
    }

    if app.permission_delete.is_some() {
        permissions::render_delete_confirm(f, app);
    }

    if app.pending_question.is_some() {
        if let Some(area) = inline_approval {
            layout::render_inline_approval(f, app, area);
        } else {
            layout::render_question(f, app);
        }
    }

    if app.serve_offer.is_some() {
        layout::render_serve_offer(f, app);
    }

    // The full-value log is deliberately last: Ctrl+L remains available over
    // every modal so clipped session/model/error text can always be inspected
    // without discarding the underlying draft or confirmation.
    if app.notifications_visible {
        layout::render_notifications(f, app);
    }
}
