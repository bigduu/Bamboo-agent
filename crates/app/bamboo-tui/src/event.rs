use crossterm::event::{KeyEvent, MouseEvent};

use crate::api::types::{McpServer, Schedule, SessionSummary, Skill, ToolInfo};

/// Result of a background API call, delivered back to the event loop so the call
/// never blocks the UI thread. `Err` carries a display string.
type Loaded<T> = Result<T, String>;

pub enum AppEvent {
    Key(KeyEvent),
    Mouse(MouseEvent),
    /// Terminal resized; the next loop iteration redraws at the new size (the
    /// dimensions themselves aren't needed — ratatui re-measures on draw).
    Resize,

    // ── Non-blocking API results (posted by spawned tasks) ──
    SessionsLoaded(Loaded<Vec<SessionSummary>>),
    McpServersLoaded(Loaded<Vec<McpServer>>),
    McpToolsLoaded(Loaded<Vec<ToolInfo>>),
    SchedulesLoaded(Loaded<Vec<Schedule>>),
    SkillsLoaded(Loaded<Vec<Skill>>),
    ConfigLoaded(Loaded<serde_json::Value>),
    /// A background mutation finished; the outcome is `Ok(success message)` or
    /// `Err(failure message)` so the receiver can classify it (info vs error)
    /// without sniffing the display text. `reload_tab` reloads the current tab.
    ActionDone {
        outcome: Loaded<String>,
        reload_tab: bool,
    },
    /// A chat turn was created + started; carries the new session id.
    ChatStarted(Loaded<String>),
}
