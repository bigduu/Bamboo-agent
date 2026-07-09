use crossterm::event::{KeyEvent, MouseEvent};

use crate::api::types::{
    ListSessionsEnvelope, McpServer, PendingQuestion, ProviderCatalog, Schedule, Skill,
    SkillDetail, ToolInfo,
};
use crate::app::OpenedSession;

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
    SessionsLoaded(Loaded<ListSessionsEnvelope>),
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
    /// The `execute` POST that kicks off a run failed (server down, 4xx/5xx).
    /// Since no SSE terminal event will ever arrive for a run that never
    /// started, this is how `chat.streaming` gets unstuck.
    ExecuteFailed(String),
    /// The background `stop` POST finished; `Err` still finalizes streaming
    /// locally (see `stop_streaming`) so the operator regains control even if
    /// the server is unreachable.
    StopFinished(Loaded<()>),
    /// A skill's detail view finished loading (`Enter` on the Skills tab).
    SkillDetailLoaded(Loaded<SkillDetail>),
    /// A session resume (Sessions-tab `Enter` or `--session-id` at startup)
    /// finished fetching history + summary (+ pending question, if any).
    /// Carries `session_id` alongside the result so the handler can still
    /// report which session failed to open.
    SessionOpened {
        session_id: String,
        result: Result<OpenedSession, String>,
    },
    /// `Ctrl+Q` with no cached dismissed question found one on the server (or
    /// confirmed there isn't one).
    PendingQuestionChecked(Loaded<PendingQuestion>),
    /// `Ctrl+O`'s provider-catalog fetch finished. Dropped by the handler if
    /// `model_picker` was already closed (Esc) before this arrived.
    CatalogLoaded(Loaded<ProviderCatalog>),
    /// The auto-serve health-poll waiter finished: `Ok(pid)` once
    /// `client.health()` succeeded (carries the spawned server's pid, for the
    /// confirmation notice); `Err(message)` if it never became healthy within
    /// the poll deadline (`message` names the server log path so the operator
    /// can diagnose it). See `App::spawn_local_server`.
    LocalServerReady(Loaded<u32>),
}
