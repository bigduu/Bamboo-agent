pub mod chat;
pub mod config;
pub mod layout;
pub mod mcp;
pub mod schedules;
pub mod sessions;
pub mod skills;

use ratatui::Frame;

use crate::app::App;

pub fn render(f: &mut Frame, app: &App) {
    let chunks = layout::app_layout(f.area(), app);

    // Content area
    match app.tab {
        crate::app::Tab::Chat => chat::render(f, chunks.content, chunks.input, app),
        crate::app::Tab::Sessions => sessions::render(f, chunks.content, app),
        crate::app::Tab::Mcp => mcp::render(f, chunks.content, app),
        crate::app::Tab::Schedules => schedules::render(f, chunks.content, app),
        crate::app::Tab::Skills => skills::render(f, chunks.content, app),
        crate::app::Tab::Config => config::render(f, chunks.content, app),
    }

    // Status bar (2 lines)
    layout::render_status_info(f, chunks.status_info, app);
    layout::render_tab_bar(f, chunks.status_tabs, app);

    // Help overlay
    if app.help_visible {
        layout::render_help(f);
    }

    // Notification-log overlay
    if app.notifications_visible {
        layout::render_notifications(f, app);
    }

    // Exclusive modals — at most one of these is ever `Some` at a time (see
    // the precedence comment on `App::handle_key`), so draw order only
    // matters for visually layering over the help/notification overlays
    // above; kept in the same 0-5 precedence order as the key routing.
    if app.serve_offer.is_some() {
        layout::render_serve_offer(f, app);
    }

    if app.pending_question.is_some() {
        layout::render_question(f, app);
    }

    if app.pending_delete.is_some() {
        layout::render_delete_confirm(f, app);
    }

    if app.model_picker.is_some() {
        layout::render_model_picker(f, app);
    }

    if app.schedule_form.is_some() {
        layout::render_schedule_form(f, app);
    }

    if app.config_editor.is_some() {
        config::render_editor(f, app);
    }
}
