pub mod chat;
pub mod config;
pub mod layout;
pub mod mcp;
pub mod permissions;
pub mod schedules;
pub mod sessions;
pub mod skills;

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
        crate::app::Tab::Chat => chat::render(f, chunks.content, chunks.input, app),
        crate::app::Tab::Sessions => sessions::render(f, chunks.content, app),
        crate::app::Tab::Mcp => mcp::render(f, chunks.content, app),
        crate::app::Tab::Schedules => schedules::render(f, chunks.content, app),
        crate::app::Tab::Skills => skills::render(f, chunks.content, app),
        crate::app::Tab::Config if app.config.view == crate::app::ConfigView::Permissions => {
            permissions::render(f, chunks.content, app)
        }
        crate::app::Tab::Config => config::render(f, chunks.content, app),
    }

    // Status bar (2 lines)
    layout::render_status_info(f, chunks.status_info, app);
    layout::render_tab_bar(f, chunks.status_tabs, app);

    // Help overlay
    if app.help_visible {
        layout::render_help(f, app);
    }

    // A clarification can arrive asynchronously while a local editor or
    // confirmation is already open. Draw from lowest to highest input
    // precedence so the modal that owns the keyboard is always the visible
    // frontmost modal (the reverse of `App::handle_key`'s routing order).
    if app.config_editor.is_some() {
        config::render_editor(f, app);
    }

    if app.permission_editor.is_some() {
        permissions::render_editor(f, app);
    }

    if app.permission_rule_confirm.is_some() {
        permissions::render_rule_confirm(f, app);
    }

    if app.schedule_form.is_some() {
        layout::render_schedule_form(f, app);
    }

    if app.command_palette.is_some() {
        layout::render_command_palette(f, app);
    }

    if app.model_picker.is_some() {
        layout::render_model_picker(f, app);
    }

    if app.session_picker.is_some() {
        layout::render_session_picker(f, app);
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
        layout::render_question(f, app);
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
