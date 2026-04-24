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
}
