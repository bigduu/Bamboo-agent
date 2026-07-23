use std::cell::Cell;
use std::path::{Path, PathBuf};

use anyhow::Result;
use chrono::{DateTime, Utc};
use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyModifiers};
use futures::StreamExt;
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
use tui_textarea::TextArea;

use crate::api::sse::SseStream;
use crate::api::types::*;
use crate::api::BambooClient;
use crate::event::AppEvent;
use crate::history::map_history;
use crate::ui;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Tab {
    Chat,
    Sessions,
    Mcp,
    Schedules,
    Skills,
    Config,
}

impl Tab {
    pub const ALL: [Tab; 6] = [
        Tab::Chat,
        Tab::Sessions,
        Tab::Mcp,
        Tab::Schedules,
        Tab::Skills,
        Tab::Config,
    ];

    pub fn title(self) -> &'static str {
        match self {
            Tab::Chat => "Chat",
            Tab::Sessions => "Sessions",
            Tab::Mcp => "MCP",
            Tab::Schedules => "Schedules",
            Tab::Skills => "Skills",
            Tab::Config => "Config",
        }
    }

    fn next(self) -> Tab {
        let idx = Self::ALL.iter().position(|&t| t == self).unwrap_or(0);
        Self::ALL[(idx + 1) % Self::ALL.len()]
    }

    fn prev(self) -> Tab {
        let idx = Self::ALL.iter().position(|&t| t == self).unwrap_or(0);
        if idx == 0 {
            Self::ALL[Self::ALL.len() - 1]
        } else {
            Self::ALL[idx - 1]
        }
    }

    fn from_index(i: usize) -> Option<Tab> {
        Self::ALL.get(i).copied()
    }
}

// ── Chat state ──

#[derive(Debug, Clone)]
pub enum MessageRole {
    User,
    Assistant,
}

#[derive(Debug, Clone)]
pub struct ToolCallDisplay {
    /// `tool_call_id` from the SSE stream. Tool events (Complete/Error/
    /// Lifecycle) are paired by this id, not list position — with parallel
    /// tool calls, position-based pairing lands results on the wrong entry.
    pub id: String,
    pub tool_name: String,
    pub arguments: String,
    pub result: Option<String>,
    pub error: Option<String>,
    pub phase: String,
}

#[derive(Debug, Clone)]
pub struct SubAgentDisplay {
    pub child_session_id: String,
    pub title: Option<String>,
    /// "running" | "completed" | "cancelled" | "error" | "skipped".
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct ChatMessage {
    pub role: MessageRole,
    pub content: String,
    pub tool_calls: Vec<ToolCallDisplay>,
    pub reasoning: Option<String>,
}

/// Textarea placeholder shown on an empty Chat input, kept as one constant so
/// the initial state and the post-send reset (`handle_chat_key`) can't drift.
const CHAT_PLACEHOLDER: &str = "Type a message... (Enter send · Alt+Enter newline)";

pub struct ChatState {
    pub session_id: Option<String>,
    /// Project inherited from the opened/selected session and propagated when
    /// Ctrl+N creates a new root session.
    pub project_id: Option<String>,
    pub messages: Vec<ChatMessage>,
    pub textarea: TextArea<'static>,
    pub scroll_offset: u16,
    /// Largest meaningful `scroll_offset` for the transcript as last rendered
    /// (`total_lines - visible_height`), refreshed every frame by
    /// `ui::chat::render`. `App` only has `&App` at render time, so this is a
    /// `Cell` — it lets the render function record the bound through a shared
    /// reference instead of needing `&mut App` there. Key/mouse handlers clamp
    /// against `.get()` so scrolling past the bottom can't require an equal
    /// number of up-presses to "catch up" before the view visibly moves.
    pub max_scroll: Cell<u16>,
    pub auto_scroll: bool,
    pub streaming: bool,
    pub current_response: String,
    pub current_tool_calls: Vec<ToolCallDisplay>,
    pub current_reasoning: String,
    pub model: String,
    pub token_usage: Option<TokenUsage>,
    pub plan_mode: bool,
    /// When true, tool-call arguments and results render in full instead
    /// of truncated. Toggled with `x` on the Chat tab.
    pub expand_tools: bool,
    /// Sub-agents spawned by the current run (child lifecycle).
    pub sub_agents: Vec<SubAgentDisplay>,
}

impl ChatState {
    pub fn new() -> Self {
        let mut textarea = TextArea::default();
        textarea.set_placeholder_text(CHAT_PLACEHOLDER);
        Self {
            session_id: None,
            project_id: None,
            messages: Vec::new(),
            textarea,
            scroll_offset: 0,
            max_scroll: Cell::new(0),
            auto_scroll: true,
            streaming: false,
            current_response: String::new(),
            current_tool_calls: Vec::new(),
            current_reasoning: String::new(),
            model: String::new(),
            token_usage: None,
            plan_mode: false,
            expand_tools: false,
            sub_agents: Vec::new(),
        }
    }
}

/// Result of a session resume (history + summary + pending question fetched
/// off the event loop), posted back as `AppEvent::SessionOpened`. Kept
/// separate from `ChatState` since it's a one-shot transfer object, not
/// long-lived UI state.
pub struct OpenedSession {
    pub messages: Vec<ChatMessage>,
    pub model: String,
    pub project_id: Option<String>,
    pub is_running: bool,
    pub pending: Option<PendingQuestion>,
    /// The server dropped older messages to stay under its cold-fetch cap.
    pub truncated: bool,
    /// Pre-cap message count, for the "showing last N of M" notice.
    pub total_message_count: usize,
}

// ── Per-tab states ──

pub struct SessionsState {
    pub sessions: Vec<SessionSummary>,
    pub selected: usize,
    pub loading: bool,
    pub error: Option<String>,
    /// Total sessions across all pages, as reported by the server envelope
    /// (#421). Used to render `page X/Y · total N`.
    pub total: usize,
    /// Offset of the currently-loaded page. Advanced/retreated by `]`/`[`.
    pub offset: usize,
    /// The page size the server actually applied (its clamped default, or the
    /// client-requested value echoed back) — used both to render the page
    /// number and to step `offset` by a full page on `[`. `0` until the first
    /// page loads, at which point `list_sessions` is asked for the server's
    /// own default instead of an arbitrary client guess.
    pub page_limit: usize,
    /// Offset to request for the next page, or `None` on the last page —
    /// gates whether `]` does anything.
    pub next_offset: Option<usize>,
}

impl SessionsState {
    pub fn new() -> Self {
        Self {
            sessions: Vec::new(),
            selected: 0,
            loading: false,
            error: None,
            total: 0,
            offset: 0,
            page_limit: 0,
            next_offset: None,
        }
    }
}

pub struct McpState {
    pub servers: Vec<McpServer>,
    pub selected: usize,
    pub tools: Vec<ToolInfo>,
    pub loading: bool,
    pub error: Option<String>,
}

impl McpState {
    pub fn new() -> Self {
        Self {
            servers: Vec::new(),
            selected: 0,
            tools: Vec::new(),
            loading: false,
            error: None,
        }
    }
}

pub struct SchedulesState {
    pub schedules: Vec<Schedule>,
    pub selected: usize,
    pub loading: bool,
    pub error: Option<String>,
}

impl SchedulesState {
    pub fn new() -> Self {
        Self {
            schedules: Vec::new(),
            selected: 0,
            loading: false,
            error: None,
        }
    }
}

pub struct SkillsState {
    pub skills: Vec<Skill>,
    pub selected: usize,
    pub detail: Option<SkillDetail>,
    pub loading: bool,
    pub error: Option<String>,
}

impl SkillsState {
    pub fn new() -> Self {
        Self {
            skills: Vec::new(),
            selected: 0,
            detail: None,
            loading: false,
            error: None,
        }
    }
}

pub struct ConfigState {
    pub config: Option<serde_json::Value>,
    pub loading: bool,
    pub error: Option<String>,
    pub scroll_offset: u16,
    /// Largest meaningful `scroll_offset` for the raw config view as last
    /// rendered; see `ChatState::max_scroll` for the `Cell`-through-`&App`
    /// rationale.
    pub max_scroll: Cell<u16>,
}

impl ConfigState {
    pub fn new() -> Self {
        Self {
            config: None,
            loading: false,
            error: None,
            scroll_offset: 0,
            max_scroll: Cell::new(0),
        }
    }
}

// ── Notifications ──

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoticeLevel {
    Info,
    Warn,
    Error,
}

/// One entry in the notification scrollback (viewed with `Ctrl+L`). Errors and
/// warnings would otherwise be lost when the next status message overwrites the
/// single status line.
#[derive(Debug, Clone)]
pub struct Notification {
    pub level: NoticeLevel,
    pub text: String,
    pub at: DateTime<Utc>,
}

// ── Interactive question (permission gate / clarification) ──

/// An agent question awaiting the operator's answer, driven by the modal.
pub struct ActiveQuestion {
    pub question: String,
    /// Preset choices; empty means free-text only.
    pub options: Vec<String>,
    /// Highlighted option index (option-select mode).
    pub selected: usize,
    /// `Some(buf)` = free-text entry mode (typing into `buf`); `None` =
    /// option-select mode. Starts `Some("")` when there are no options.
    pub custom: Option<String>,
    /// An answer POST is in flight for this question: the modal renders a
    /// "Submitting answer…" state and `handle_question_key` swallows every
    /// key (preventing a double-submit on repeated Enter, and preventing the
    /// modal from being dismissed/mutated out from under the request) until
    /// `AppEvent::AnswerSubmitted` lands. Cleared on failure so the operator
    /// can retry; on success the whole question is dropped.
    pub submitting: bool,
}

impl ActiveQuestion {
    /// Build the modal state from a `GET .../pending` response. Mirrors the
    /// SSE `NeedClarification` handler: no preset options opens straight into
    /// free-text entry instead of an empty option list.
    fn from_pending(pending: &PendingQuestion) -> Self {
        let options = pending.options.clone().unwrap_or_default();
        let custom = if options.is_empty() {
            Some(String::new())
        } else {
            None
        };
        Self {
            question: pending.question.clone(),
            options,
            selected: 0,
            custom,
            submitting: false,
        }
    }
}

/// In-progress "new schedule" form (opened with `n` on the Schedules tab).
#[derive(Default)]
pub struct ScheduleForm {
    pub name: String,
    pub cron: String,
    pub prompt: String,
    /// Focused field: 0 = name, 1 = cron, 2 = prompt.
    pub field: usize,
}

impl ScheduleForm {
    fn current_mut(&mut self) -> &mut String {
        match self.field {
            0 => &mut self.name,
            1 => &mut self.cron,
            _ => &mut self.prompt,
        }
    }
}

/// In-place editor for the raw config JSON (opened with `e` on the Config tab).
/// The buffer is a multi-line `TextArea`; on save it must parse as JSON before
/// it is PATCHed to the server.
pub struct ConfigEditor {
    pub textarea: TextArea<'static>,
}

/// In-progress model picker (`Ctrl+O` on the Chat tab). Opens immediately with
/// `loading: true` and an empty list; the provider-catalog fetch runs off the
/// event loop and lands via `AppEvent::CatalogLoaded`.
pub struct ModelPicker {
    pub models: Vec<CatalogModel>,
    pub selected: usize,
    pub loading: bool,
}

// ── Auto-serve (local `bamboo serve` bootstrap) ──

/// How `App::run`'s startup connectivity check should react when it fails
/// against a loopback URL (see [`is_loopback_url`]); irrelevant for a remote
/// URL, which always just warns regardless of this. Set once from CLI flags
/// (`--auto-serve` / `--no-auto-serve`, mutually exclusive) and consumed by
/// `run` before the main loop starts — nothing later re-reads it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AutoServeMode {
    /// `--auto-serve`: skip the y/n offer and start spawning immediately.
    Auto,
    /// Default: open the [`ServeOffer`] y/n modal.
    Prompt,
    /// `--no-auto-serve`: never offer or spawn; just warn, like a remote URL.
    Off,
}

/// Startup-only "start a local server?" y/n prompt (`AutoServeMode::Prompt`).
/// Exists ONLY before `App::run`'s main loop starts driving any other
/// modal — nothing after startup ever sets it again — which is why it is
/// checked first in `handle_key`'s modal-precedence chain.
pub struct ServeOffer {
    pub url: String,
}

/// The `serve` subcommand's own compiled-in defaults (`bamboo-config`'s
/// `default_port`/`default_bind`). Duplicated here rather than imported —
/// `bamboo-tui` intentionally has no dependency on the server crates (see
/// `Cargo.toml`) — and used only to decide whether the auto-spawned server
/// needs explicit `--port`/`--bind` overrides at all.
const DEFAULT_SERVER_PORT: u16 = 9562;
const DEFAULT_SERVER_BIND: &str = "127.0.0.1";

/// Split an http(s) URL's authority into `(host, port)`. Pure string
/// parsing — no new deps, no DNS resolution. Handles a bracketed IPv6 host
/// (`[::1]:9562`), userinfo (`user:pass@host`), and a missing port (`None`,
/// meaning the server's own default applies).
fn parse_authority(url: &str) -> (String, Option<u16>) {
    let without_scheme = url.split_once("://").map(|(_, rest)| rest).unwrap_or(url);
    // Drop any path/query/fragment after the authority.
    let authority = without_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or(without_scheme);
    // Drop userinfo, if present.
    let authority = authority
        .rsplit_once('@')
        .map(|(_, host)| host)
        .unwrap_or(authority);

    if let Some(rest) = authority.strip_prefix('[') {
        // Bracketed IPv6: `[::1]:9562` or bare `[::1]`.
        let mut parts = rest.splitn(2, ']');
        let host = parts.next().unwrap_or(rest).to_string();
        let port = parts
            .next()
            .and_then(|tail| tail.strip_prefix(':'))
            .and_then(|p| p.parse().ok());
        (host, port)
    } else {
        match authority.rsplit_once(':') {
            Some((host, port)) => (host.to_string(), port.parse().ok()),
            None => (authority.to_string(), None),
        }
    }
}

/// Whether `url`'s host is loopback (`127.0.0.1`/`127.x.x.x`, `localhost`,
/// `::1` — bracketed or not) — i.e. safe to offer auto-starting a local
/// `bamboo serve` for. A hostname other than the literal `localhost` is
/// never resolved/DNS-checked; this is a syntactic check only, matching
/// `parse_authority`'s no-new-deps string parsing. The `127.x.x.x` case
/// parses `host` as a strict `Ipv4Addr` literal (std, no new dep) rather
/// than `starts_with("127.")` — the latter would also match a non-loopback
/// hostname like `127.0.0.1.evil.example.com`, wrongly offering/auto-
/// starting a local server for what the operator meant as a remote URL.
pub fn is_loopback_url(url: &str) -> bool {
    let (host, _) = parse_authority(url);
    host == "localhost"
        || host == "::1"
        || host
            .parse::<std::net::Ipv4Addr>()
            .is_ok_and(|ip| ip.octets()[0] == 127)
}

/// `bamboo`'s platform binary name, used both by `discover_bamboo_bin` and
/// its tests.
fn bamboo_bin_name() -> &'static str {
    if cfg!(windows) {
        "bamboo.exe"
    } else {
        "bamboo"
    }
}

/// Resolve the `bamboo` binary to spawn for auto-serve, in precedence order:
/// (a) `$BAMBOO_BIN`, if set — even when the path doesn't exist, so a
///     typo'd override fails loudly at spawn time instead of silently
///     falling through to a different binary; (b) a `bamboo`/`bamboo.exe`
///     binary sitting next to the running `bamboo-tui` executable (the
///     layout when both ship together, e.g. bodhi's sidecar bundle); (c) the
///     first `bamboo` found on `$PATH`.
///
/// Takes its inputs as plain parameters instead of reading
/// `std::env`/`std::env::current_exe` itself — the caller
/// (`App::spawn_local_server`) snapshots the environment once, which keeps
/// this function pure and its tests parallel-safe (no process-global env
/// mutation to serialize against).
pub fn discover_bamboo_bin(
    env_override: Option<PathBuf>,
    exe_dir: Option<&Path>,
    path_var: Option<&str>,
) -> Option<PathBuf> {
    if let Some(path) = env_override {
        return Some(path);
    }
    let name = bamboo_bin_name();
    if let Some(dir) = exe_dir {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    if let Some(path_var) = path_var {
        for dir in std::env::split_paths(path_var) {
            let candidate = dir.join(name);
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

// ── Main App ──

pub struct App {
    pub running: bool,
    pub tab: Tab,
    pub client: BambooClient,
    pub chat: ChatState,
    pub sessions: SessionsState,
    pub mcp: McpState,
    pub schedules: SchedulesState,
    pub skills: SkillsState,
    pub config: ConfigState,
    pub status_message: String,
    pub connected: bool,
    pub help_visible: bool,
    pub spinner_tick: usize,
    /// Startup-only "start a local server?" y/n offer; see [`ServeOffer`].
    /// Set at most once, by `run`'s initial health check, and cleared by
    /// `handle_serve_offer_key` before anything else can run — see the
    /// modal-precedence doc on `handle_key`.
    pub serve_offer: Option<ServeOffer>,
    /// The `bamboo serve` child this TUI spawned via auto-serve, if any.
    /// Held with `kill_on_drop(true)` so it dies when `App` (and hence this
    /// field) is dropped — the *graceful* death-link, covering a clean quit
    /// or panic-unwind. The *crash-safe* backstop (SIGKILL, force-quit — no
    /// Rust destructor runs at all) is the `--parent-pid <this pid>` flag
    /// passed to the child in `spawn_local_server`, which self-exits when it
    /// notices this process is gone — the same double death-link bodhi's
    /// sidecar uses (`bodhi/src-tauri/src/sidecar.rs`).
    pub spawned_server: Option<Child>,
    /// The agent's pending question (permission gate / clarification). When
    /// `Some`, a modal captures the answer and keystrokes route to it.
    pub pending_question: Option<ActiveQuestion>,
    /// The most recently *dismissed* (Esc'd) pending question, kept around
    /// because dismissing the modal does NOT tell the server to stop
    /// waiting — the run is still blocked on an answer. `Ctrl+Q` restores it
    /// without a round-trip; cleared once answered/resumed or on a fresh
    /// session (`Ctrl+N` / opening a different session).
    pub dismissed_question: Option<ActiveQuestion>,
    /// In-progress new-schedule form (Schedules tab). When `Some`, a modal
    /// captures the fields.
    pub schedule_form: Option<ScheduleForm>,
    /// In-progress raw-JSON config editor (Config tab). When `Some`, a modal
    /// textarea captures all keystrokes.
    pub config_editor: Option<ConfigEditor>,
    /// In-progress model picker (`Ctrl+O`, Chat tab). When `Some`, a modal
    /// captures navigation/apply keystrokes.
    pub model_picker: Option<ModelPicker>,
    /// Pending session-delete confirmation (Sessions tab, `d`): `(id, title)`
    /// of the session awaiting `y`/Enter confirm or `n`/Esc cancel. Kept as a
    /// modal (rather than deleting immediately) so a stray `d` can't destroy a
    /// session, and the actual DELETE runs off the event loop like every other
    /// mutation.
    pub pending_delete: Option<(String, String)>,
    /// Capped scrollback of past status messages so errors/warnings aren't lost
    /// when the single status line is overwritten. Viewed with `Ctrl+L`.
    pub notifications: Vec<Notification>,
    /// Whether the notification-log overlay is open.
    pub notifications_visible: bool,
    /// Count of warn/error notifications since the log was last opened; shown as
    /// a badge in the status bar.
    pub unseen_alerts: usize,
    /// Monotonic epoch for answer submissions (see
    /// `AppEvent::AnswerSubmitted`): bumped by `submit_answer` when it spawns
    /// the POST (the in-flight task carries a copy) and by
    /// `supersede_pending_answer` whenever the question context changes
    /// underneath it, so a late response for a superseded question is
    /// discarded instead of applied to the wrong question/session.
    answer_epoch: u64,
    /// Sender into the main event loop, used to post results of background API
    /// calls (so those calls never block the UI thread). Set in [`run`].
    event_tx: Option<mpsc::UnboundedSender<AppEvent>>,
    sse_tx: Option<mpsc::UnboundedSender<AgentEvent>>,
    sse_rx: Option<mpsc::UnboundedReceiver<AgentEvent>>,
}

/// Move a list selection by `delta` (positive = down, negative = up),
/// clamped to `[0, len-1]` (or `0` when the list is empty). Shared by every
/// list tab's mouse-wheel handling in `App::handle_mouse`.
fn scroll_selection(selected: usize, len: usize, delta: i32) -> usize {
    if len == 0 {
        return 0;
    }
    let next = selected as i64 + delta as i64;
    next.clamp(0, (len - 1) as i64) as usize
}

impl App {
    pub fn new(client: BambooClient) -> Self {
        Self {
            running: true,
            tab: Tab::Chat,
            client,
            chat: ChatState::new(),
            sessions: SessionsState::new(),
            mcp: McpState::new(),
            schedules: SchedulesState::new(),
            skills: SkillsState::new(),
            config: ConfigState::new(),
            status_message: String::new(),
            connected: false,
            help_visible: false,
            spinner_tick: 0,
            serve_offer: None,
            spawned_server: None,
            pending_question: None,
            dismissed_question: None,
            schedule_form: None,
            config_editor: None,
            model_picker: None,
            pending_delete: None,
            notifications: Vec::new(),
            notifications_visible: false,
            unseen_alerts: 0,
            answer_epoch: 0,
            event_tx: None,
            sse_tx: None,
            sse_rx: None,
        }
    }

    pub async fn run(
        &mut self,
        terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
        auto_serve_mode: AutoServeMode,
    ) -> Result<()> {
        let (event_tx, mut event_rx) = mpsc::unbounded_channel::<AppEvent>();
        // Keep a sender so background API tasks can post their results back.
        // Set BEFORE the initial health check below: for `AutoServeMode::Auto`
        // that check may call `spawn_local_server`, which needs this sender to
        // post its health-poll waiter's result back as `LocalServerReady`.
        self.event_tx = Some(event_tx.clone());

        self.connected = self.client.health().await.unwrap_or(false);
        if self.connected {
            self.status_message = "Connected".to_string();
        } else {
            let url = self.client.base_url.clone();
            match (is_loopback_url(&url), auto_serve_mode) {
                (true, AutoServeMode::Auto) => self.spawn_local_server(),
                (true, AutoServeMode::Prompt) => {
                    self.status_message = format!(
                        "Bamboo server is not reachable at {url}. Start a local server? (y/n)"
                    );
                    self.serve_offer = Some(ServeOffer { url });
                }
                // Loopback but auto-serve explicitly disabled, or a remote URL
                // (never auto-started regardless of flags): keep the previous
                // "just warn" behavior, improved to name the URL.
                (true, AutoServeMode::Off) | (false, _) => {
                    self.notify(
                        NoticeLevel::Warn,
                        format!("Cannot connect to server at {url}"),
                    );
                }
            }
        }

        // Kick off the initial tab's data load without blocking startup.
        self.load_tab_data();

        // `--session-id` on the command line: resume it the same way `Enter`
        // on the Sessions tab does (history replay + live reattach + pending
        // question recovery), not a bespoke startup-only path.
        if let Some(session_id) = self.chat.session_id.clone() {
            self.resume_session(session_id);
        }

        // Spawn crossterm event reader.
        let tx = event_tx.clone();
        tokio::spawn(async move {
            let mut reader = EventStream::new();
            while let Some(event) = reader.next().await {
                match event {
                    Ok(Event::Key(key)) if tx.send(AppEvent::Key(key)).is_err() => {
                        break;
                    }
                    Ok(Event::Mouse(mouse)) if tx.send(AppEvent::Mouse(mouse)).is_err() => {
                        break;
                    }
                    Ok(Event::Resize(_, _)) if tx.send(AppEvent::Resize).is_err() => {
                        break;
                    }
                    _ => {}
                }
            }
        });

        // Redraw ticker: the loop below only otherwise iterates (and redraws)
        // when a key/mouse event or an SSE event arrives, so a long tool call
        // with no token traffic would freeze the braille spinner and make the
        // UI look hung. Created ONCE here (not inside the loop) so `tick()`
        // correctly accounts for elapsed time across iterations instead of
        // firing immediately every time.
        let mut redraw_interval = tokio::time::interval(std::time::Duration::from_millis(120));

        // Main event loop.
        while self.running {
            self.poll_sse();
            self.spinner_tick = self.spinner_tick.wrapping_add(1);
            terminal.draw(|f| ui::render(f, self))?;

            tokio::select! {
                event = event_rx.recv() => {
                    if let Some(event) = event {
                        self.handle_event(event).await?;
                    }
                }
                sse_event = async {
                    if let Some(rx) = &mut self.sse_rx {
                        rx.recv().await
                    } else {
                        std::future::pending().await
                    }
                } => {
                    if let Some(event) = sse_event {
                        if let Err(e) = self.handle_sse_event(event) {
                            self.notify(NoticeLevel::Error, format!("SSE error: {e}"));
                        }
                    }
                }
                _ = redraw_interval.tick() => {
                    // No new state — just a steady redraw so the spinner animates.
                }
            }
        }

        Ok(())
    }

    fn poll_sse(&mut self) {
        let events: Vec<AgentEvent> = if let Some(rx) = &mut self.sse_rx {
            std::iter::from_fn(|| rx.try_recv().ok()).collect()
        } else {
            Vec::new()
        };
        for event in events {
            if let Err(e) = self.handle_sse_event(event) {
                self.notify(NoticeLevel::Error, format!("SSE error: {e}"));
            }
        }
    }

    async fn handle_event(&mut self, event: AppEvent) -> Result<()> {
        if self.help_visible {
            if let AppEvent::Key(_) = &event {
                self.help_visible = false;
                return Ok(());
            }
        }

        // The notification-log overlay is dismissed by any key.
        if self.notifications_visible {
            if let AppEvent::Key(_) = &event {
                self.notifications_visible = false;
                return Ok(());
            }
        }

        match event {
            AppEvent::Key(key) => self.handle_key(key).await?,
            AppEvent::Mouse(mouse) => self.handle_mouse(mouse),
            AppEvent::SessionsLoaded(r) => {
                self.sessions.loading = false;
                match r {
                    Ok(envelope) => {
                        self.sessions.selected = self
                            .sessions
                            .selected
                            .min(envelope.sessions.len().saturating_sub(1));
                        self.sessions.total = envelope.total;
                        self.sessions.offset = envelope.offset;
                        self.sessions.page_limit = envelope.limit;
                        self.sessions.next_offset = envelope.next_offset;
                        self.sessions.sessions = envelope.sessions;
                        self.sessions.error = None;
                    }
                    Err(e) => self.sessions.error = Some(e),
                }
            }
            AppEvent::McpServersLoaded(r) => {
                self.mcp.loading = false;
                match r {
                    Ok(s) => {
                        self.mcp.selected = self.mcp.selected.min(s.len().saturating_sub(1));
                        self.mcp.servers = s;
                        self.mcp.error = None;
                    }
                    Err(e) => self.mcp.error = Some(e),
                }
            }
            AppEvent::McpToolsLoaded(r) => {
                self.mcp.loading = false;
                match r {
                    Ok(t) => {
                        self.mcp.tools = t;
                        self.mcp.error = None;
                    }
                    Err(e) => self.mcp.error = Some(e),
                }
            }
            AppEvent::SchedulesLoaded(r) => {
                self.schedules.loading = false;
                match r {
                    Ok(s) => {
                        self.schedules.selected =
                            self.schedules.selected.min(s.len().saturating_sub(1));
                        self.schedules.schedules = s;
                        self.schedules.error = None;
                    }
                    Err(e) => self.schedules.error = Some(e),
                }
            }
            AppEvent::SkillsLoaded(r) => {
                self.skills.loading = false;
                match r {
                    Ok(s) => {
                        self.skills.skills = s;
                        self.skills.error = None;
                    }
                    Err(e) => self.skills.error = Some(e),
                }
            }
            AppEvent::ConfigLoaded(r) => {
                self.config.loading = false;
                match r {
                    Ok(c) => {
                        self.config.config = Some(c);
                        self.config.error = None;
                    }
                    Err(e) => self.config.error = Some(e),
                }
            }
            AppEvent::ActionDone {
                outcome,
                reload_tab,
            } => {
                match outcome {
                    Ok(msg) => self.notify(NoticeLevel::Info, msg),
                    Err(msg) => self.notify(NoticeLevel::Error, msg),
                }
                if reload_tab {
                    self.load_tab_data();
                }
            }
            AppEvent::ChatStarted(r) => match r {
                Ok(session_id) => {
                    self.chat.session_id = Some(session_id.clone());
                    self.status_message = "Streaming...".to_string();
                    self.start_stream_and_execute(session_id);
                }
                Err(e) => {
                    self.chat.streaming = false;
                    self.notify(NoticeLevel::Error, format!("Error: {e}"));
                }
            },
            AppEvent::ExecuteFailed(msg) => {
                // The POST that starts the run never succeeded, so no SSE
                // terminal event is ever coming — finalize here or
                // `chat.streaming` spins forever.
                self.notify(NoticeLevel::Error, format!("Failed to start run: {msg}"));
                self.finalize_streaming();
            }
            AppEvent::StopFinished(r) => {
                // Finalize regardless of outcome: even if the stop request
                // failed (server down/unreachable), the operator must regain
                // control of the input instead of being stuck waiting for a
                // terminal SSE event that a dead server will never send.
                // `finalize_streaming` resets `status_message` to "Ready"
                // internally, so the outcome-specific message is set AFTER it
                // (same ordering the old synchronous `stop_streaming` used to
                // get "Stopped" to stick instead of being overwritten).
                self.finalize_streaming();
                match r {
                    Ok(()) => self.status_message = "Stopped".to_string(),
                    Err(e) => self.notify(NoticeLevel::Error, format!("Stop failed: {e}")),
                }
            }
            AppEvent::SkillDetailLoaded(r) => match r {
                Ok(detail) => {
                    self.skills.detail = Some(detail);
                    self.skills.error = None;
                }
                Err(e) => self.skills.error = Some(e),
            },
            AppEvent::SessionOpened { session_id, result } => match result {
                Ok(opened) => {
                    // Reset every per-run scratch field `finalize_streaming`
                    // would otherwise leave behind from whatever was open
                    // before — a resumed session must not inherit stale
                    // in-flight-turn state from a prior chat.
                    self.chat.session_id = Some(session_id.clone());
                    self.chat.model = opened.model;
                    self.chat.project_id = opened.project_id;
                    self.chat.current_response.clear();
                    self.chat.current_tool_calls.clear();
                    self.chat.current_reasoning.clear();
                    self.chat.sub_agents.clear();
                    self.chat.token_usage = None;
                    self.chat.scroll_offset = 0;
                    self.chat.streaming = false;
                    self.supersede_pending_answer();
                    self.pending_question = None;
                    self.dismissed_question = None;

                    let shown = opened.messages.len();
                    self.chat.messages = opened.messages;
                    self.chat.auto_scroll = true;
                    self.tab = Tab::Chat;
                    self.status_message = "Session resumed".to_string();

                    if opened.truncated {
                        self.notify(
                            NoticeLevel::Info,
                            format!(
                                "Showing last {shown} of {} messages",
                                opened.total_message_count
                            ),
                        );
                    }

                    if opened.is_running {
                        self.attach_stream(session_id);
                        self.chat.streaming = true;
                        self.status_message = "Reattached — streaming".to_string();
                    }

                    if let Some(pending) = &opened.pending {
                        self.status_message =
                            format!("Question: {} (answer in the dialog)", pending.question);
                        self.pending_question = Some(ActiveQuestion::from_pending(pending));
                    }
                }
                Err(e) => {
                    self.notify(NoticeLevel::Error, format!("Failed to open session: {e}"));
                }
            },
            AppEvent::PendingQuestionChecked(r) => match r {
                Ok(pending) if pending.has_pending_question => {
                    self.supersede_pending_answer();
                    self.pending_question = Some(ActiveQuestion::from_pending(&pending));
                    self.status_message = "Question reopened".to_string();
                }
                Ok(_) => {
                    self.notify(NoticeLevel::Info, "No pending question on the server");
                }
                Err(e) => {
                    self.notify(
                        NoticeLevel::Error,
                        format!("Failed to check pending question: {e}"),
                    );
                }
            },
            AppEvent::AnswerSubmitted {
                epoch,
                answer,
                result,
            } => {
                // A late response for a question that has since been
                // superseded (new question arrived, session switched, run
                // finalized, modal reopened — every one of those bumps
                // `answer_epoch` via `supersede_pending_answer`) must be
                // discarded outright: applying it would clear or resume state
                // that belongs to a different question than the one this
                // answer was for.
                if epoch != self.answer_epoch {
                    return Ok(());
                }
                match result {
                    Ok(status) => {
                        self.pending_question = None;
                        // Only keep the spinner on if a run is actually
                        // running: the server returns 200 even when it did
                        // NOT resume (e.g. the session already `completed`),
                        // so a blind `streaming = true` would spin forever
                        // with no events behind it. No SSE reattach here —
                        // the stream opened for the run stays attached across
                        // the question, exactly as before.
                        if matches!(status.as_str(), "started" | "already_running") {
                            self.status_message = format!("Answered: {answer} — resuming");
                            self.chat.streaming = true;
                        } else {
                            self.status_message = format!("Answered: {answer} ({status})");
                            self.finalize_streaming();
                        }
                    }
                    Err(e) => {
                        // Keep the modal open — with input re-enabled — so
                        // the operator can pick a valid option or retry.
                        if let Some(q) = self.pending_question.as_mut() {
                            q.submitting = false;
                        }
                        self.notify(NoticeLevel::Error, format!("Answer rejected: {e}"));
                    }
                }
            }
            AppEvent::CatalogLoaded(r) => {
                // The picker may already have been closed (Esc) before this
                // fetch returned — drop the result instead of reopening it.
                let Some(picker) = self.model_picker.as_mut() else {
                    return Ok(());
                };
                match r {
                    Ok(catalog) if catalog.models.is_empty() => {
                        self.model_picker = None;
                        self.notify(NoticeLevel::Warn, "No models in provider catalog");
                    }
                    Ok(catalog) => {
                        picker.models = catalog.models;
                        picker.selected = 0;
                        picker.loading = false;
                        self.status_message = "Ready".to_string();
                    }
                    Err(e) => {
                        self.model_picker = None;
                        self.notify(
                            NoticeLevel::Error,
                            format!("Failed to load provider catalog: {e}"),
                        );
                    }
                }
            }
            AppEvent::LocalServerReady(Ok(pid)) => {
                self.connected = true;
                self.notify(
                    NoticeLevel::Info,
                    format!("Local server started (pid {pid})"),
                );
                self.load_tab_data();
                // The `--session-id` startup resume (in `run`) ran while
                // disconnected and so left `chat.session_id` set but
                // `chat.messages` empty — retry it now that the server is up.
                if self.chat.messages.is_empty() {
                    if let Some(session_id) = self.chat.session_id.clone() {
                        self.resume_session(session_id);
                    }
                }
            }
            AppEvent::LocalServerReady(Err(e)) => {
                self.notify(
                    NoticeLevel::Error,
                    format!("Local server failed to start: {e}"),
                );
                if let Some(mut child) = self.spawned_server.take() {
                    let _ = child.start_kill();
                }
                self.connected = false;
            }
            _ => {}
        }
        Ok(())
    }

    /// Mouse wheel: on the two scrollable-text tabs (Chat/Config) it scrolls
    /// the view; everywhere else (the four list tabs) there's nothing to
    /// scroll independently of the selection, so the wheel moves the
    /// selection instead (3 rows per notch, clamped) rather than being a
    /// dead input.
    fn handle_mouse(&mut self, mouse: crossterm::event::MouseEvent) {
        use crossterm::event::MouseEventKind;
        let delta: i32 = match mouse.kind {
            MouseEventKind::ScrollUp => -3,
            MouseEventKind::ScrollDown => 3,
            _ => return,
        };
        match self.tab {
            Tab::Chat => {
                if delta < 0 {
                    self.chat_scroll_up(delta.unsigned_abs() as u16);
                } else {
                    self.chat_scroll_down(delta as u16);
                }
            }
            Tab::Config => {
                if delta < 0 {
                    self.config_scroll_up(delta.unsigned_abs() as u16);
                } else {
                    self.config_scroll_down(delta as u16);
                }
            }
            Tab::Sessions => {
                self.sessions.selected =
                    scroll_selection(self.sessions.selected, self.sessions.sessions.len(), delta);
            }
            Tab::Mcp => {
                self.mcp.selected =
                    scroll_selection(self.mcp.selected, self.mcp.servers.len(), delta);
            }
            Tab::Schedules => {
                self.schedules.selected = scroll_selection(
                    self.schedules.selected,
                    self.schedules.schedules.len(),
                    delta,
                );
            }
            Tab::Skills => {
                self.skills.selected =
                    scroll_selection(self.skills.selected, self.skills.skills.len(), delta);
            }
        }
    }

    /// Scroll the chat transcript down by `delta` lines, clamped to
    /// `chat.max_scroll` (see its doc comment) so repeated presses past the
    /// bottom don't leave the opposite key needing an equal number of presses
    /// before the view visibly moves.
    fn chat_scroll_down(&mut self, delta: u16) {
        self.chat.auto_scroll = false;
        self.chat.scroll_offset = self
            .chat
            .scroll_offset
            .saturating_add(delta)
            .min(self.chat.max_scroll.get());
    }

    /// Scroll the chat transcript up by `delta` lines; naturally bounded at 0.
    fn chat_scroll_up(&mut self, delta: u16) {
        self.chat.auto_scroll = false;
        self.chat.scroll_offset = self.chat.scroll_offset.saturating_sub(delta);
    }

    /// `g`: jump to the top of the transcript.
    fn chat_scroll_top(&mut self) {
        self.chat.auto_scroll = false;
        self.chat.scroll_offset = 0;
    }

    /// `G`: jump to the bottom and resume auto-scroll.
    fn chat_scroll_bottom(&mut self) {
        self.chat.auto_scroll = true;
    }

    /// Scroll the raw config view down by `delta` lines, clamped to
    /// `config.max_scroll` — same rationale as `chat_scroll_down`.
    fn config_scroll_down(&mut self, delta: u16) {
        self.config.scroll_offset = self
            .config
            .scroll_offset
            .saturating_add(delta)
            .min(self.config.max_scroll.get());
    }

    /// Scroll the raw config view up by `delta` lines; naturally bounded at 0.
    fn config_scroll_up(&mut self, delta: u16) {
        self.config.scroll_offset = self.config.scroll_offset.saturating_sub(delta);
    }

    /// Whether an exclusive modal currently owns the keyboard. `F1`/Ctrl+`?`/
    /// Ctrl+L are gated on this so they can't stack a second overlay on top
    /// of one of these six — see the precedence comment on `handle_key`.
    fn any_modal_open(&self) -> bool {
        self.serve_offer.is_some()
            || self.pending_question.is_some()
            || self.pending_delete.is_some()
            || self.model_picker.is_some()
            || self.schedule_form.is_some()
            || self.config_editor.is_some()
    }

    /// Route one key event.
    ///
    /// Modal precedence (checked top to bottom, each returning early — so at
    /// most one modal ever owns the keyboard, and every one of them runs
    /// before the global bindings further down: Ctrl+N/Ctrl+O/Ctrl+Q, `?`,
    /// digit tab-switching, Tab/Shift+Tab):
    ///   0. `serve_offer`      — startup-only "start a local server?" offer
    ///   1. `pending_question` — agent permission/clarification gate
    ///   2. `pending_delete`   — session delete confirmation
    ///   3. `model_picker`     — Ctrl+O provider-catalog picker
    ///   4. `schedule_form`    — new-schedule authoring form
    ///   5. `config_editor`    — raw config JSON editor
    ///
    /// `serve_offer` can never actually coexist with 1-5 in practice — `run`
    /// sets it (if at all) before the main loop starts driving any of the
    /// others, and it's always cleared before the loop's first redraw — but
    /// it is still checked first, for the same "whichever modal is open gets
    /// first refusal at every key" reason as the rest of this list.
    ///
    /// Ctrl+C (stop/quit) always preempts every modal. F1/Ctrl+`?`/Ctrl+L
    /// (help/notifications) are gated on `any_modal_open` below instead: with
    /// a modal open they fall straight through to that modal's own handler,
    /// so a stray F1 can't eat the keystroke the modal was waiting for or
    /// stack the help overlay on top of it.
    async fn handle_key(&mut self, key: KeyEvent) -> Result<()> {
        match (key.modifiers, key.code) {
            (KeyModifiers::CONTROL, KeyCode::Char('c')) => {
                if self.chat.streaming {
                    self.stop_streaming();
                    return Ok(());
                }
                self.running = false;
                return Ok(());
            }
            (KeyModifiers::CONTROL, KeyCode::Char('?')) if !self.any_modal_open() => {
                // Most terminals never deliver Ctrl+Shift+/ as Ctrl+'?' (it
                // maps elsewhere), so this is kept only as a harmless extra —
                // F1 below and plain `?` (further down, non-Chat tabs) are
                // the bindings that are actually reachable.
                self.help_visible = true;
                return Ok(());
            }
            (_, KeyCode::F(1)) if !self.any_modal_open() => {
                // F1 opens help on every tab, including Chat, regardless of
                // modifiers — unlike `?` it never collides with typing.
                self.help_visible = true;
                return Ok(());
            }
            (KeyModifiers::CONTROL, KeyCode::Char('l')) if !self.any_modal_open() => {
                self.notifications_visible = true;
                self.unseen_alerts = 0;
                return Ok(());
            }
            _ => {}
        }

        // 0. The startup "start a local server?" offer captures all input
        // (Ctrl+C above still quits) until answered — see `ServeOffer`.
        if self.serve_offer.is_some() {
            self.handle_serve_offer_key(key);
            return Ok(());
        }

        // 1. A pending agent question captures all input (Ctrl+C above still
        // stops the run) until it is answered or dismissed.
        if self.pending_question.is_some() {
            return self.handle_question_key(key).await;
        }

        // 2. The delete-confirmation modal likewise captures all input before
        // anything else (including tab-switching digits) can reach it.
        if self.pending_delete.is_some() {
            return self.handle_delete_confirm_key(key).await;
        }

        // 3. The model picker likewise captures all input (navigation/apply)
        // before the global bindings below — same pattern as the other
        // modals.
        if self.model_picker.is_some() {
            return self.handle_model_picker_key(key).await;
        }

        // 4. The schedule-authoring modal likewise captures all input: Tab moves
        // between fields and digits belong in cron expressions, so it must run
        // before the global Tab/1-6 tab-switching below (which would otherwise
        // swallow those keys and never reach the form).
        if self.schedule_form.is_some() {
            self.handle_schedule_form_key(key);
            return Ok(());
        }

        // 5. The config editor is a full multi-line text buffer, so it must claim
        // every key (digits, Tab, Enter/newlines) before the global navigation
        // below — same rationale as the schedule form.
        if self.config_editor.is_some() {
            return self.handle_config_editor_key(key);
        }

        // Ctrl+N / Ctrl+Q: global, but only reachable once every modal above
        // has had first refusal at the key (each of those branches returns
        // early) — so getting here already means no modal is open, matching
        // the "no modal open" requirement for Ctrl+N specifically.
        if key.modifiers.contains(KeyModifiers::CONTROL) {
            match key.code {
                KeyCode::Char('n') if !self.chat.streaming => {
                    self.new_session();
                    return Ok(());
                }
                KeyCode::Char('q') => {
                    self.reopen_pending_question();
                    return Ok(());
                }
                KeyCode::Char('o') if self.tab == Tab::Chat && !self.chat.streaming => {
                    self.open_model_picker();
                    return Ok(());
                }
                _ => {}
            }
        }

        // `?` opens help everywhere EXCEPT Chat, where it must type into the
        // textarea instead (mirrors the digit rule right below: Chat's
        // textarea wins over global single-key bindings). F1 above is the
        // Chat-safe way to reach help.
        if key.code == KeyCode::Char('?') && self.tab != Tab::Chat {
            self.help_visible = true;
            return Ok(());
        }

        // 1-6 switch tabs EXCEPT on Chat, where digits must type into the
        // message instead — otherwise typing e.g. "top 3 issues" silently
        // jumps to the Config tab on the '3'. Shift+digit (many keyboards'
        // symbol row) never switches tabs, matching the pre-existing rule.
        if let KeyCode::Char(c) = key.code {
            if let Some(digit) = c.to_digit(10) {
                if (1..=6).contains(&digit)
                    && !key.modifiers.contains(KeyModifiers::SHIFT)
                    && self.tab != Tab::Chat
                {
                    self.tab = Tab::from_index((digit - 1) as usize).unwrap_or(self.tab);
                    self.load_tab_data();
                    return Ok(());
                }
            }
        }

        match key.code {
            KeyCode::Tab => {
                self.tab = self.tab.next();
                self.load_tab_data();
            }
            KeyCode::BackTab => {
                self.tab = self.tab.prev();
                self.load_tab_data();
            }
            _ => {
                self.handle_tab_key(key).await?;
            }
        }
        Ok(())
    }

    /// Drive the startup "start a local server?" offer (`ServeOffer`):
    /// `y`/Enter starts the spawn flow (`spawn_local_server`); `n`/Esc
    /// dismisses without spawning, leaving the operator to restart with
    /// `--auto-serve` or start `bamboo serve` themselves. Every other key is
    /// swallowed — see the modal-precedence doc on `handle_key`.
    fn handle_serve_offer_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Char('y') | KeyCode::Enter => {
                self.serve_offer = None;
                self.spawn_local_server();
            }
            KeyCode::Char('n') | KeyCode::Esc => {
                self.serve_offer = None;
                self.status_message =
                    "Not connected — restart with --auto-serve or start 'bamboo serve' yourself"
                        .to_string();
            }
            _ => {}
        }
    }

    /// Resolve the `bamboo` binary and spawn `bamboo serve` as an auto-serve
    /// child. Everything here is off the UI loop except the actual
    /// `Command::spawn()` call, which is synchronous but non-blocking (it
    /// forks/execs and returns immediately, it doesn't wait on the child) —
    /// only the health-poll wait *after* the spawn is a `tokio::spawn`'d
    /// task. The `Child` is created and stored here (not inside that task)
    /// so it lands in `self.spawned_server` — and hence gets `kill_on_drop`
    /// protection — before this function returns, rather than floating in a
    /// detached task with no owner in the meantime.
    fn spawn_local_server(&mut self) {
        let Some(bin) = discover_bamboo_bin(
            std::env::var_os("BAMBOO_BIN").map(PathBuf::from),
            std::env::current_exe()
                .ok()
                .as_deref()
                .and_then(Path::parent),
            std::env::var("PATH").ok().as_deref(),
        ) else {
            self.notify(
                NoticeLevel::Error,
                "Can't find a `bamboo` binary to auto-start — set BAMBOO_BIN or add it to PATH",
            );
            return;
        };

        // `serve`'s stdout is NOT quiet (tracing_subscriber fmt lines plus a
        // raw `println!`) — inheriting it here would corrupt this
        // alternate-screen TUI, so both streams are redirected to a log file
        // instead (mirrors bodhi's sidecar, which drains and re-logs rather
        // than inheriting).
        let log_path = std::env::temp_dir().join("bamboo-tui-server.log");
        let (stdout_file, stderr_file) = match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .and_then(|f| Ok((f.try_clone()?, f)))
        {
            Ok(files) => files,
            Err(e) => {
                self.notify(
                    NoticeLevel::Error,
                    format!("Failed to open server log {}: {e}", log_path.display()),
                );
                return;
            }
        };

        let mut args = vec![
            "serve".to_string(),
            // Crash-safe orphan guard: `serve` exits if this TUI process
            // disappears without running any cleanup (SIGKILL) — the second
            // line of defense behind `spawned_server`'s `kill_on_drop`.
            "--parent-pid".to_string(),
            std::process::id().to_string(),
        ];
        let (host, port) = parse_authority(&self.client.base_url);
        if let Some(port) = port {
            if port != DEFAULT_SERVER_PORT {
                args.push("--port".to_string());
                args.push(port.to_string());
                args.push("--bind".to_string());
                args.push(if host.is_empty() {
                    DEFAULT_SERVER_BIND.to_string()
                } else {
                    host
                });
            }
        }

        let mut command = Command::new(&bin);
        command
            .args(&args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::from(stdout_file))
            .stderr(std::process::Stdio::from(stderr_file))
            .kill_on_drop(true);

        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(e) => {
                self.notify(
                    NoticeLevel::Error,
                    format!(
                        "Failed to spawn {} serve: {e} (log: {})",
                        bin.display(),
                        log_path.display()
                    ),
                );
                return;
            }
        };
        // `Child::id()` only returns `None` once the child has already been
        // reaped (`wait`ed on) — never true immediately after a successful
        // `spawn()`, so `0` here is unreachable in practice, not a silently
        // wrong pid.
        let pid = child.id().unwrap_or(0);

        let Some(tx) = self.event_tx.clone() else {
            // No event loop to report back to (shouldn't happen — `run` sets
            // this before the health check that leads here) — best-effort
            // kill so an orphaned child isn't left behind either.
            let _ = child.start_kill();
            return;
        };
        self.spawned_server = Some(child);
        self.status_message = "Starting local server...".to_string();

        let client = self.client.clone();
        tokio::spawn(async move {
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
            loop {
                if matches!(client.health().await, Ok(true)) {
                    let _ = tx.send(AppEvent::LocalServerReady(Ok(pid)));
                    return;
                }
                if tokio::time::Instant::now() >= deadline {
                    let _ = tx.send(AppEvent::LocalServerReady(Err(format!(
                        "local server did not become healthy within 20s (log: {})",
                        log_path.display()
                    ))));
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            }
        });
    }

    /// Drive the pending-question modal. Returns an answer to submit (if the
    /// keystroke commits one) without holding a borrow across the async submit.
    async fn handle_question_key(&mut self, key: KeyEvent) -> Result<()> {
        enum QAction {
            None,
            Dismiss,
            Submit(String),
        }

        let action = {
            let Some(q) = self.pending_question.as_mut() else {
                return Ok(());
            };
            if q.submitting {
                // An answer POST is in flight: swallow every key. This both
                // prevents a double-submit on repeated Enter and keeps the
                // question from being dismissed/mutated out from under the
                // request (Ctrl+C at the top of `handle_key` still preempts).
                return Ok(());
            }
            if let Some(buf) = q.custom.as_mut() {
                // Free-text entry mode.
                match key.code {
                    KeyCode::Enter => {
                        let answer = buf.trim().to_string();
                        if answer.is_empty() {
                            QAction::None
                        } else {
                            QAction::Submit(answer)
                        }
                    }
                    KeyCode::Esc => {
                        // Back to option-select if there were options, else dismiss.
                        if q.options.is_empty() {
                            QAction::Dismiss
                        } else {
                            q.custom = None;
                            QAction::None
                        }
                    }
                    KeyCode::Backspace => {
                        buf.pop();
                        QAction::None
                    }
                    KeyCode::Char(c) => {
                        buf.push(c);
                        QAction::None
                    }
                    _ => QAction::None,
                }
            } else {
                // Option-select mode.
                match key.code {
                    KeyCode::Up | KeyCode::Char('k') => {
                        q.selected = q.selected.saturating_sub(1);
                        QAction::None
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        if q.selected + 1 < q.options.len() {
                            q.selected += 1;
                        }
                        QAction::None
                    }
                    KeyCode::Char('c') => {
                        // Switch to free-text entry (for allow-custom questions).
                        q.custom = Some(String::new());
                        QAction::None
                    }
                    KeyCode::Enter => q
                        .options
                        .get(q.selected)
                        .cloned()
                        .map(QAction::Submit)
                        .unwrap_or(QAction::None),
                    KeyCode::Char(d) if ('1'..='9').contains(&d) => {
                        let idx = d as usize - '1' as usize;
                        q.options
                            .get(idx)
                            .cloned()
                            .map(QAction::Submit)
                            .unwrap_or(QAction::None)
                    }
                    KeyCode::Esc => QAction::Dismiss,
                    _ => QAction::None,
                }
            }
        };

        match action {
            QAction::Submit(answer) => self.submit_answer(answer),
            QAction::Dismiss => {
                // Keep it, not just drop it: dismissing does NOT tell the
                // server to stop waiting, so the run is still blocked on an
                // answer — Ctrl+Q brings the modal back without a round-trip.
                self.supersede_pending_answer();
                self.dismissed_question = self.pending_question.take();
                self.status_message = "Question dismissed (still pending on the server — \
                    Ctrl+Q to reopen, Ctrl+C stops the run)"
                    .to_string();
            }
            QAction::None => {}
        }
        Ok(())
    }

    /// Invalidate any in-flight answer POST by bumping `answer_epoch`. Called
    /// from every site that changes the pending-question context (a new
    /// question arriving, a session switch/resume, the run finalizing, the
    /// modal being dismissed or reopened) so that a late
    /// `AppEvent::AnswerSubmitted` carrying an older epoch is discarded in
    /// `handle_event` instead of applied to a question it doesn't belong to.
    fn supersede_pending_answer(&mut self) {
        self.answer_epoch = self.answer_epoch.wrapping_add(1);
    }

    /// Submit an answer to the agent's pending question WITHOUT blocking the
    /// event loop: the `respond` POST is spawned off the UI thread (this used
    /// to be awaited inline inside `handle_event`, freezing every redraw/key/
    /// SSE drain until the server replied) and its outcome comes back as
    /// `AppEvent::AnswerSubmitted`. Until then the modal stays open in a
    /// "Submitting answer…" state with input disabled.
    fn submit_answer(&mut self, answer: String) {
        let Some(session_id) = self.chat.session_id.clone() else {
            self.notify(NoticeLevel::Warn, "No active chat session to answer");
            self.supersede_pending_answer();
            self.pending_question = None;
            return;
        };
        let Some(tx) = self.event_tx.clone() else {
            return;
        };
        let Some(q) = self.pending_question.as_mut() else {
            return;
        };
        if q.submitting {
            // Belt-and-braces double-submit guard: `handle_question_key`
            // already swallows every key while in flight.
            return;
        }
        q.submitting = true;

        // Claim a fresh epoch for this submission; the spawned task carries a
        // copy so the handler can tell whether the response still belongs to
        // the current question when it lands.
        self.supersede_pending_answer();
        let epoch = self.answer_epoch;
        self.status_message = format!("Submitting answer: {answer}…");

        let client = self.client.clone();
        tokio::spawn(async move {
            let result = client
                .respond(&session_id, &answer)
                .await
                .map_err(|e| e.to_string());
            let _ = tx.send(AppEvent::AnswerSubmitted {
                epoch,
                answer,
                result,
            });
        });
    }

    /// Drive the delete-confirmation modal (`d` on the Sessions tab). `y`/Enter
    /// confirms and spawns the DELETE off the event loop (never `?` on the UI
    /// thread — a failed delete must surface via `notify`, not tear down the
    /// TUI); `n`/Esc cancels without calling the server.
    async fn handle_delete_confirm_key(&mut self, key: KeyEvent) -> Result<()> {
        let Some((id, _title)) = self.pending_delete.clone() else {
            return Ok(());
        };
        match key.code {
            KeyCode::Char('y') | KeyCode::Enter => {
                self.pending_delete = None;
                if let Some(tx) = self.event_tx.clone() {
                    let client = self.client.clone();
                    tokio::spawn(async move {
                        let outcome = match client.delete_session(&id).await {
                            Ok(()) => Ok("Session deleted".to_string()),
                            Err(e) => Err(format!("Delete failed: {e}")),
                        };
                        let _ = tx.send(AppEvent::ActionDone {
                            outcome,
                            reload_tab: true,
                        });
                    });
                }
            }
            KeyCode::Char('n') | KeyCode::Esc => {
                self.pending_delete = None;
            }
            _ => {}
        }
        Ok(())
    }

    /// Load the current tab's data WITHOUT blocking the event loop: mark the
    /// tab loading and spawn the fetch, which posts its result back as an
    /// `AppEvent` handled in `handle_event`.
    fn load_tab_data(&mut self) {
        let Some(tx) = self.event_tx.clone() else {
            return;
        };
        let client = self.client.clone();
        match self.tab {
            Tab::Chat => {}
            Tab::Sessions => {
                self.sessions.loading = true;
                let offset = self.sessions.offset;
                // `0` means "no page size established yet" — let the server
                // pick its own bounded default rather than guessing one here.
                let limit = (self.sessions.page_limit > 0).then_some(self.sessions.page_limit);
                tokio::spawn(async move {
                    let r = client
                        .list_sessions(limit, Some(offset))
                        .await
                        .map_err(|e| e.to_string());
                    let _ = tx.send(AppEvent::SessionsLoaded(r));
                });
            }
            Tab::Mcp => {
                self.mcp.loading = true;
                tokio::spawn(async move {
                    let r = client.list_mcp_servers().await.map_err(|e| e.to_string());
                    let _ = tx.send(AppEvent::McpServersLoaded(r));
                });
            }
            Tab::Schedules => {
                self.schedules.loading = true;
                tokio::spawn(async move {
                    let r = client.list_schedules().await.map_err(|e| e.to_string());
                    let _ = tx.send(AppEvent::SchedulesLoaded(r));
                });
            }
            Tab::Skills => {
                self.skills.loading = true;
                tokio::spawn(async move {
                    let r = client.list_skills().await.map_err(|e| e.to_string());
                    let _ = tx.send(AppEvent::SkillsLoaded(r));
                });
            }
            Tab::Config => {
                self.config.loading = true;
                tokio::spawn(async move {
                    let r = client.get_config().await.map_err(|e| e.to_string());
                    let _ = tx.send(AppEvent::ConfigLoaded(r));
                });
            }
        }
    }

    async fn handle_tab_key(&mut self, key: KeyEvent) -> Result<()> {
        match self.tab {
            Tab::Chat => self.handle_chat_key(key).await?,
            Tab::Sessions => self.handle_sessions_key(key).await?,
            Tab::Mcp => self.handle_mcp_key(key).await?,
            Tab::Schedules => self.handle_schedules_key(key).await?,
            Tab::Skills => self.handle_skills_key(key).await?,
            Tab::Config => self.handle_config_key(key),
        }
        Ok(())
    }

    async fn handle_chat_key(&mut self, key: KeyEvent) -> Result<()> {
        if self.chat.streaming {
            match key.code {
                KeyCode::Char('s') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    self.stop_streaming();
                }
                KeyCode::Char('x') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    self.chat.expand_tools = !self.chat.expand_tools;
                }
                KeyCode::Char('j') | KeyCode::Down => self.chat_scroll_down(3),
                KeyCode::Char('k') | KeyCode::Up => self.chat_scroll_up(3),
                KeyCode::PageDown => self.chat_scroll_down(10),
                KeyCode::PageUp => self.chat_scroll_up(10),
                KeyCode::Char('g') => self.chat_scroll_top(),
                KeyCode::Char('G') => self.chat_scroll_bottom(),
                _ => {}
            }
            return Ok(());
        }

        match key.code {
            // Alt+Enter (and Shift+Enter, on the kitty-protocol terminals
            // that report it — plain crossterm terminals mostly don't, so
            // this arm is harmless there) inserts a newline instead of
            // sending, since plain Enter always sends.
            KeyCode::Enter
                if key.modifiers.contains(KeyModifiers::ALT)
                    || key.modifiers.contains(KeyModifiers::SHIFT) =>
            {
                self.chat.textarea.insert_newline();
            }
            KeyCode::Enter => {
                let input = self.chat.textarea.lines().join("\n");
                let input = input.trim().to_string();
                if input.is_empty() {
                    return Ok(());
                }
                self.chat.textarea = TextArea::default();
                self.chat.textarea.set_placeholder_text(CHAT_PLACEHOLDER);
                self.send_message(input);
            }
            KeyCode::Char('j') | KeyCode::Down => self.chat_scroll_down(3),
            KeyCode::Char('k') | KeyCode::Up => self.chat_scroll_up(3),
            KeyCode::PageDown => self.chat_scroll_down(10),
            KeyCode::PageUp => self.chat_scroll_up(10),
            KeyCode::Char('g') => self.chat_scroll_top(),
            KeyCode::Char('G') => self.chat_scroll_bottom(),
            KeyCode::Char('x') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.chat.expand_tools = !self.chat.expand_tools;
            }
            _ => {
                self.chat.textarea.input(key);
            }
        }
        Ok(())
    }

    /// Send a chat message WITHOUT blocking the event loop: the user turn is
    /// shown immediately (optimistic), and the `chat` POST runs on a task that
    /// posts `ChatStarted` back — the handler then opens the SSE stream and
    /// spawns `execute` once the session id is known.
    fn send_message(&mut self, message: String) {
        let Some(tx) = self.event_tx.clone() else {
            return;
        };
        let model = if self.chat.model.is_empty() {
            "default".to_string()
        } else {
            self.chat.model.clone()
        };

        // Optimistic UI: show the user's turn and switch to streaming right away.
        self.chat.messages.push(ChatMessage {
            role: MessageRole::User,
            content: message.clone(),
            tool_calls: Vec::new(),
            reasoning: None,
        });
        self.chat.auto_scroll = true;
        self.chat.streaming = true;
        self.chat.current_response.clear();
        self.chat.current_tool_calls.clear();
        self.chat.current_reasoning.clear();
        self.status_message = "Sending...".to_string();

        let client = self.client.clone();
        let existing_session = self.chat.session_id.clone();
        let project_id = self.chat.project_id.clone();
        tokio::spawn(async move {
            let req = ChatRequest {
                message,
                session_id: existing_session,
                project_id,
                model: Some(model),
            };
            let result = client
                .chat(req)
                .await
                .map(|resp| resp.session_id)
                .map_err(|e| e.to_string());
            let _ = tx.send(AppEvent::ChatStarted(result));
        });
    }

    /// Open the SSE stream for `session_id`, wiring `sse_tx`/`sse_rx` so
    /// `poll_sse`/`run`'s select loop starts receiving events. Shared by a
    /// freshly-started run (`start_stream_and_execute`, before `execute` so no
    /// early event is missed) and reattaching to an already-running session on
    /// resume (`AppEvent::SessionOpened` with `is_running: true`) — in both
    /// cases the server replays cached critical events then live-tails, so
    /// connecting mid-flight doesn't lose anything. Returns whether the
    /// connection was opened; on failure it has already `notify`'d.
    fn attach_stream(&mut self, session_id: String) -> bool {
        let (sse_tx, sse_rx) = mpsc::unbounded_channel();
        self.sse_tx = Some(sse_tx.clone());
        self.sse_rx = Some(sse_rx);
        let base_url = self.client.base_url.clone();
        if let Err(e) = SseStream::start(&base_url, &session_id, sse_tx) {
            self.notify(NoticeLevel::Error, format!("SSE start failed: {e}"));
            return false;
        }
        true
    }

    /// After `chat` returns a session id, open the SSE stream (before execute, so
    /// no early event is missed) and spawn the agent run.
    fn start_stream_and_execute(&mut self, session_id: String) {
        if !self.attach_stream(session_id.clone()) {
            return;
        }
        let Some(tx) = self.event_tx.clone() else {
            return;
        };
        let client = self.client.clone();
        let model = self.chat.model.clone();
        tokio::spawn(async move {
            let model = if model.is_empty() { None } else { Some(model) };
            // If this POST fails (server down, 4xx/5xx), no SSE terminal event
            // will ever arrive for a run that never started — report it back so
            // the handler can finalize `chat.streaming` instead of spinning
            // forever waiting for events behind a run that doesn't exist.
            if let Err(e) = client.execute(&session_id, model.as_deref()).await {
                let _ = tx.send(AppEvent::ExecuteFailed(e.to_string()));
            }
        });
    }

    /// Resume a session fully off the event loop: fetch its history + summary
    /// (+ pending question, when the summary reports one), map the history
    /// into chat messages, and post a single `SessionOpened` event. Used both
    /// by `Enter` on the Sessions tab and by `--session-id` at startup so
    /// there's exactly one resume code path.
    fn resume_session(&mut self, session_id: String) {
        let Some(tx) = self.event_tx.clone() else {
            return;
        };
        let client = self.client.clone();
        self.status_message = "Resuming session...".to_string();
        tokio::spawn(async move {
            let history = match client.get_history(&session_id).await {
                Ok(h) => h,
                Err(e) => {
                    let _ = tx.send(AppEvent::SessionOpened {
                        session_id,
                        result: Err(e.to_string()),
                    });
                    return;
                }
            };
            let summary = match client.get_session(&session_id).await {
                Ok(s) => s,
                Err(e) => {
                    let _ = tx.send(AppEvent::SessionOpened {
                        session_id,
                        result: Err(e.to_string()),
                    });
                    return;
                }
            };
            // Only fetch the pending question when the summary says there is
            // one — saves a round-trip for the common case, and a failure
            // here is non-fatal: the session still opens, just without the
            // modal pre-populated (Ctrl+Q can fetch it again).
            let pending = if summary.has_pending_question {
                client
                    .get_pending_question(&session_id)
                    .await
                    .ok()
                    .filter(|p| p.has_pending_question)
            } else {
                None
            };
            let opened = OpenedSession {
                messages: map_history(history.messages),
                model: summary.model,
                project_id: summary.project_id,
                is_running: summary.is_running,
                pending,
                truncated: history.truncated,
                total_message_count: history.total_message_count,
            };
            let _ = tx.send(AppEvent::SessionOpened {
                session_id,
                result: Ok(opened),
            });
        });
    }

    /// `Ctrl+N`: start a fresh session. Keeps the current model and Project
    /// membership (the operator picked both deliberately) while dropping
    /// per-session conversation state.
    fn new_session(&mut self) {
        self.chat.session_id = None;
        self.chat.messages.clear();
        self.chat.current_response.clear();
        self.chat.current_tool_calls.clear();
        self.chat.current_reasoning.clear();
        self.chat.sub_agents.clear();
        self.chat.token_usage = None;
        self.chat.scroll_offset = 0;
        self.chat.auto_scroll = true;
        self.chat.plan_mode = false;
        self.supersede_pending_answer();
        self.pending_question = None;
        self.dismissed_question = None;
        self.status_message = "New session".to_string();
    }

    /// `Ctrl+Q`: restore the last dismissed question if one is cached; else,
    /// if there's an active session, check the server off the event loop (the
    /// result comes back as `AppEvent::PendingQuestionChecked`).
    fn reopen_pending_question(&mut self) {
        if let Some(q) = self.dismissed_question.take() {
            // Reopening counts as a new question context too: any answer
            // somehow still in flight for the pre-dismissal modal must not
            // land on the reopened one.
            self.supersede_pending_answer();
            self.pending_question = Some(q);
            self.status_message = "Question reopened".to_string();
            return;
        }
        let Some(session_id) = self.chat.session_id.clone() else {
            self.notify(NoticeLevel::Info, "No pending question to reopen");
            return;
        };
        let Some(tx) = self.event_tx.clone() else {
            return;
        };
        let client = self.client.clone();
        self.status_message = "Checking for a pending question...".to_string();
        tokio::spawn(async move {
            let r = client
                .get_pending_question(&session_id)
                .await
                .map_err(|e| e.to_string());
            let _ = tx.send(AppEvent::PendingQuestionChecked(r));
        });
    }

    /// Find the in-progress tool call matching `tool_call_id`. Tool events are
    /// paired by this server-assigned id rather than list position/name so
    /// that parallel tool calls (multiple in-flight at once) each get their
    /// own Complete/Error/Lifecycle update instead of clobbering whichever
    /// entry happens to be last in the list.
    fn find_tool_mut(&mut self, tool_call_id: &str) -> Option<&mut ToolCallDisplay> {
        self.chat
            .current_tool_calls
            .iter_mut()
            .find(|t| t.id == tool_call_id)
    }

    fn handle_sse_event(&mut self, event: AgentEvent) -> Result<()> {
        if self.chat.auto_scroll {
            // Any incoming event means new content; auto_scroll will reposition.
        }
        match event {
            AgentEvent::Token { content } => {
                self.chat.current_response.push_str(&content);
                self.chat.auto_scroll = true;
            }
            AgentEvent::ReasoningToken { content } => {
                self.chat.current_reasoning.push_str(&content);
            }
            AgentEvent::ToolStart {
                tool_call_id,
                tool_name,
                arguments,
            } => {
                self.chat.current_tool_calls.push(ToolCallDisplay {
                    id: tool_call_id,
                    tool_name,
                    arguments: serde_json::to_string(&arguments).unwrap_or_default(),
                    result: None,
                    error: None,
                    phase: "running".to_string(),
                });
            }
            AgentEvent::ToolComplete {
                tool_call_id,
                result,
            } => match self.find_tool_mut(&tool_call_id) {
                Some(tc) => {
                    tc.result = Some(result.result);
                    tc.phase = "complete".to_string();
                }
                None => {
                    // No matching ToolStart (dropped/out-of-order) — surface it
                    // defensively instead of silently losing the result.
                    self.chat.current_tool_calls.push(ToolCallDisplay {
                        id: tool_call_id,
                        tool_name: "unknown".to_string(),
                        arguments: String::new(),
                        result: Some(result.result),
                        error: None,
                        phase: "complete".to_string(),
                    });
                }
            },
            AgentEvent::ToolError {
                tool_call_id,
                error,
            } => match self.find_tool_mut(&tool_call_id) {
                Some(tc) => {
                    tc.error = Some(error);
                    tc.phase = "error".to_string();
                }
                None => {
                    self.chat.current_tool_calls.push(ToolCallDisplay {
                        id: tool_call_id,
                        tool_name: "unknown".to_string(),
                        arguments: String::new(),
                        result: None,
                        error: Some(error),
                        phase: "error".to_string(),
                    });
                }
            },
            AgentEvent::ToolLifecycle {
                tool_call_id,
                phase,
                summary,
                error,
                ..
            } => {
                // Lifecycle phases ("begin"/"executing"/"finished"/"cancelled")
                // are supplementary progress strings, not the UI's terminal
                // vocabulary ("complete"/"error") — once ToolComplete/ToolError
                // has set one of those, a later Lifecycle event must not
                // overwrite it back to a non-terminal phase (the UI's ✓/✗ icon
                // is keyed on those exact strings).
                if let Some(tc) = self.find_tool_mut(&tool_call_id) {
                    if tc.phase != "complete" && tc.phase != "error" {
                        tc.phase = phase;
                        if let Some(s) = summary {
                            tc.result = Some(s);
                        }
                        if let Some(e) = error {
                            tc.error = Some(e);
                        }
                    }
                }
                // No matching entry: a Lifecycle event with no known Start is
                // dropped (it carries only supplementary progress info, unlike
                // Complete/Error's definitive terminal result).
            }
            AgentEvent::NeedClarification {
                question, options, ..
            } => {
                let options = options.unwrap_or_default();
                // No preset options ⇒ open straight into free-text entry.
                let custom = if options.is_empty() {
                    Some(String::new())
                } else {
                    None
                };
                self.status_message = format!("Question: {} (answer in the dialog)", question);
                // A new question supersedes any answer still in flight for a
                // previous one — a late response must not clear this modal.
                self.supersede_pending_answer();
                self.pending_question = Some(ActiveQuestion {
                    question,
                    options,
                    selected: 0,
                    custom,
                    submitting: false,
                });
            }
            AgentEvent::Complete { usage } => {
                self.finalize_streaming();
                self.chat.token_usage = Some(usage);
            }
            AgentEvent::Cancelled { message } => {
                self.status_message = message.unwrap_or_else(|| "Cancelled".to_string());
                self.finalize_streaming();
            }
            AgentEvent::Error { message } => {
                self.notify(NoticeLevel::Error, format!("Error: {message}"));
                self.finalize_streaming();
            }
            AgentEvent::BudgetExceeded {
                kind,
                limit,
                actual,
            } => {
                // Precedes the run's normal `Complete`/`Cancelled` terminal
                // event (see bamboo_agent_core::AgentEvent::BudgetExceeded) —
                // just surface why the run is about to stop; the terminal
                // event still finalizes streaming.
                self.notify(
                    NoticeLevel::Warn,
                    format!("Run budget exceeded ({kind}: {actual}/{limit}) — stopping."),
                );
            }
            AgentEvent::ToolToken { content, .. } => {
                // Deliberately not routed into the matching ToolCallDisplay by
                // `tool_call_id`: today's rendering already prints tool output
                // inline with the response text, and threading a per-call
                // streaming buffer through to the UI is beyond this fix's
                // scope. Kept as the simpler, behavior-preserving option.
                self.chat.current_response.push_str(&content);
            }
            AgentEvent::ContextCompressionStatus { phase, status } => {
                self.status_message = format!("Compression: {} ({})", status, phase);
            }
            AgentEvent::PlanModeEntered { reason, .. } => {
                self.chat.plan_mode = true;
                self.status_message = format!(
                    "Plan mode active{}",
                    reason
                        .as_ref()
                        .map(|r| format!(": {}", r))
                        .unwrap_or_default()
                );
            }
            AgentEvent::PlanModeExited { approved, .. } => {
                self.chat.plan_mode = false;
                self.status_message = if approved {
                    "Plan mode exited (approved)".to_string()
                } else {
                    "Plan mode exited".to_string()
                };
            }
            AgentEvent::PlanFileUpdated { file_path, .. } => {
                self.status_message = format!("Plan updated: {}", file_path);
            }
            AgentEvent::SubAgentStarted {
                child_session_id,
                title,
            } => {
                if !self
                    .chat
                    .sub_agents
                    .iter()
                    .any(|s| s.child_session_id == child_session_id)
                {
                    self.chat.sub_agents.push(SubAgentDisplay {
                        child_session_id,
                        title,
                        status: "running".to_string(),
                    });
                }
            }
            AgentEvent::SubAgentHeartbeat { .. } => {}
            AgentEvent::SubAgentCompleted {
                child_session_id,
                status,
                ..
            } => {
                if let Some(sa) = self
                    .chat
                    .sub_agents
                    .iter_mut()
                    .find(|s| s.child_session_id == child_session_id)
                {
                    sa.status = status;
                }
            }
        }
        Ok(())
    }

    fn finalize_streaming(&mut self) {
        self.chat.streaming = false;
        if !self.chat.current_response.is_empty() || !self.chat.current_tool_calls.is_empty() {
            self.chat.messages.push(ChatMessage {
                role: MessageRole::Assistant,
                content: std::mem::take(&mut self.chat.current_response),
                tool_calls: std::mem::take(&mut self.chat.current_tool_calls),
                reasoning: if self.chat.current_reasoning.is_empty() {
                    None
                } else {
                    Some(std::mem::take(&mut self.chat.current_reasoning))
                },
            });
        }
        self.status_message = "Ready".to_string();
        self.sse_tx = None;
        self.sse_rx = None;
        self.chat.sub_agents.clear();
        // A run that ended (completed / cancelled / stopped) can no longer accept
        // an answer, so drop any open (or dismissed-but-cached) question modal
        // to avoid answering a dead session — and invalidate any answer POST
        // still in flight for it.
        self.supersede_pending_answer();
        self.pending_question = None;
        self.dismissed_question = None;
    }

    /// Stop the current run WITHOUT blocking the event loop: the `stop` POST
    /// is spawned off the UI thread and its outcome comes back as
    /// `AppEvent::StopFinished` (handled in `handle_event`, which finalizes
    /// streaming either way). Previously this awaited `client.stop()` and
    /// `?`-propagated a network error — a dead server hit at the worst
    /// possible moment (pressing Ctrl+C to stop a run) tore down the whole
    /// TUI instead of just failing the stop.
    fn stop_streaming(&mut self) {
        let Some(sid) = self.chat.session_id.clone() else {
            // Nothing to stop server-side; still clear local streaming state.
            self.finalize_streaming();
            self.status_message = "Stopped".to_string();
            return;
        };
        let Some(tx) = self.event_tx.clone() else {
            self.finalize_streaming();
            return;
        };
        self.status_message = "Stopping...".to_string();
        let client = self.client.clone();
        tokio::spawn(async move {
            let r = client.stop(&sid).await.map_err(|e| e.to_string());
            let _ = tx.send(AppEvent::StopFinished(r));
        });
    }

    async fn handle_sessions_key(&mut self, key: KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Down if !self.sessions.sessions.is_empty() => {
                self.sessions.selected =
                    (self.sessions.selected + 1).min(self.sessions.sessions.len() - 1);
            }
            KeyCode::Up => {
                self.sessions.selected = self.sessions.selected.saturating_sub(1);
            }
            KeyCode::Enter => {
                // Full resume, off the event loop: history replay + live
                // reattach (if the run is still going) + pending-question
                // recovery, all landing in one `SessionOpened` event.
                if let Some(session) = self.sessions.sessions.get(self.sessions.selected) {
                    self.resume_session(session.id.clone());
                }
            }
            KeyCode::Char('d') => {
                // Open a confirmation modal instead of deleting immediately — the
                // actual DELETE is fired from `handle_delete_confirm_key` off the
                // event loop, never `?`-propagated on the UI thread.
                if let Some(session) = self.sessions.sessions.get(self.sessions.selected) {
                    self.pending_delete = Some((session.id.clone(), session.title.clone()));
                }
            }
            KeyCode::Char('r') => {
                self.load_tab_data();
            }
            KeyCode::Char(']') => {
                if let Some(next) = self.sessions.next_offset {
                    self.sessions.offset = next;
                    self.sessions.selected = 0;
                    self.load_tab_data();
                }
            }
            KeyCode::Char('[') if self.sessions.offset > 0 => {
                let step = self.sessions.page_limit.max(1);
                self.sessions.offset = self.sessions.offset.saturating_sub(step);
                self.sessions.selected = 0;
                self.load_tab_data();
            }
            _ => {}
        }
        Ok(())
    }

    async fn handle_mcp_key(&mut self, key: KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Down if !self.mcp.servers.is_empty() => {
                self.mcp.selected = (self.mcp.selected + 1).min(self.mcp.servers.len() - 1);
            }
            KeyCode::Up => {
                self.mcp.selected = self.mcp.selected.saturating_sub(1);
            }
            KeyCode::Enter => {
                if let (Some(server), Some(tx)) = (
                    self.mcp.servers.get(self.mcp.selected),
                    self.event_tx.clone(),
                ) {
                    let id = server.id.clone();
                    let connected = server.connected.unwrap_or(false);
                    let client = self.client.clone();
                    self.mcp.loading = true;
                    tokio::spawn(async move {
                        let res = if connected {
                            client.disconnect_mcp(&id).await
                        } else {
                            client.connect_mcp(&id).await
                        };
                        let outcome = match res {
                            Ok(()) => Ok(if connected {
                                "Disconnected".to_string()
                            } else {
                                "Connected".to_string()
                            }),
                            Err(e) => Err(format!("MCP action failed: {e}")),
                        };
                        let _ = tx.send(AppEvent::ActionDone {
                            outcome,
                            reload_tab: true,
                        });
                    });
                }
            }
            KeyCode::Char('t') => {
                if let (Some(server), Some(tx)) = (
                    self.mcp.servers.get(self.mcp.selected),
                    self.event_tx.clone(),
                ) {
                    let id = server.id.clone();
                    let client = self.client.clone();
                    tokio::spawn(async move {
                        let r = client.get_mcp_tools(&id).await.map_err(|e| e.to_string());
                        let _ = tx.send(AppEvent::McpToolsLoaded(r));
                    });
                }
            }
            KeyCode::Char('r') => {
                self.load_tab_data();
            }
            _ => {}
        }
        Ok(())
    }

    async fn handle_schedules_key(&mut self, key: KeyEvent) -> Result<()> {
        // Note: when `schedule_form` is open, `handle_key` routes every key
        // straight to `handle_schedule_form_key` before reaching here.
        match key.code {
            KeyCode::Char('n') => {
                self.schedule_form = Some(ScheduleForm::default());
            }
            KeyCode::Down if !self.schedules.schedules.is_empty() => {
                self.schedules.selected =
                    (self.schedules.selected + 1).min(self.schedules.schedules.len() - 1);
            }
            KeyCode::Up => {
                self.schedules.selected = self.schedules.selected.saturating_sub(1);
            }
            KeyCode::Char('d') => {
                // Spawned off the event loop like every other mutation here
                // (see `handle_mcp_key`'s connect/disconnect) — an inline
                // `.await` on the UI thread would freeze the whole app
                // (spinner, redraw, SSE receipt) for the round-trip.
                if let (Some(schedule), Some(tx)) = (
                    self.schedules.schedules.get(self.schedules.selected),
                    self.event_tx.clone(),
                ) {
                    let id = schedule.id.clone();
                    let client = self.client.clone();
                    tokio::spawn(async move {
                        let outcome = match client.delete_schedule(&id).await {
                            Ok(()) => Ok("Schedule deleted".to_string()),
                            Err(e) => Err(format!("Delete failed: {e}")),
                        };
                        let _ = tx.send(AppEvent::ActionDone {
                            outcome,
                            reload_tab: true,
                        });
                    });
                }
            }
            KeyCode::Char('r') => {
                if let (Some(schedule), Some(tx)) = (
                    self.schedules.schedules.get(self.schedules.selected),
                    self.event_tx.clone(),
                ) {
                    let id = schedule.id.clone();
                    let client = self.client.clone();
                    tokio::spawn(async move {
                        let outcome = match client.run_schedule_now(&id).await {
                            Ok(()) => Ok("Schedule triggered".to_string()),
                            Err(e) => Err(format!("Run failed: {e}")),
                        };
                        let _ = tx.send(AppEvent::ActionDone {
                            outcome,
                            reload_tab: false,
                        });
                    });
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Drive the new-schedule form modal. On Enter (all fields filled) it POSTs
    /// create_schedule off the event loop and reloads the tab.
    fn handle_schedule_form_key(&mut self, key: KeyEvent) {
        let Some(form) = self.schedule_form.as_mut() else {
            return;
        };
        match key.code {
            KeyCode::Esc => {
                self.schedule_form = None;
            }
            KeyCode::Tab | KeyCode::Down => {
                form.field = (form.field + 1) % 3;
            }
            KeyCode::BackTab | KeyCode::Up => {
                form.field = (form.field + 2) % 3;
            }
            KeyCode::Backspace => {
                form.current_mut().pop();
            }
            KeyCode::Enter => {
                if form.name.trim().is_empty()
                    || form.cron.trim().is_empty()
                    || form.prompt.trim().is_empty()
                {
                    self.status_message = "Fill in name, cron, and prompt".to_string();
                    return;
                }
                let req = CreateScheduleRequest {
                    name: form.name.trim().to_string(),
                    trigger: ScheduleTriggerReq::Cron {
                        expr: form.cron.trim().to_string(),
                    },
                    enabled: true,
                    run_config: ScheduleRunConfigReq {
                        project_id: self
                            .sessions
                            .sessions
                            .get(self.sessions.selected)
                            .and_then(|session| session.project_id.clone()),
                        task_message: Some(form.prompt.trim().to_string()),
                        auto_execute: true,
                    },
                };
                self.schedule_form = None;
                if let Some(tx) = self.event_tx.clone() {
                    let client = self.client.clone();
                    tokio::spawn(async move {
                        let outcome = match client.create_schedule(req).await {
                            Ok(()) => Ok("Schedule created".to_string()),
                            Err(e) => Err(format!("Create failed: {e}")),
                        };
                        let _ = tx.send(AppEvent::ActionDone {
                            outcome,
                            reload_tab: true,
                        });
                    });
                }
            }
            KeyCode::Char(c) => {
                form.current_mut().push(c);
            }
            _ => {}
        }
    }

    async fn handle_skills_key(&mut self, key: KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Down if !self.skills.skills.is_empty() => {
                self.skills.selected = (self.skills.selected + 1).min(self.skills.skills.len() - 1);
            }
            KeyCode::Up => {
                self.skills.selected = self.skills.selected.saturating_sub(1);
            }
            KeyCode::Enter => {
                // Fetch the detail off the event loop: `get_skill` used to be
                // awaited right here on the UI thread, so a slow/unreachable
                // server froze every keystroke until it returned.
                if let (Some(skill), Some(tx)) = (
                    self.skills.skills.get(self.skills.selected),
                    self.event_tx.clone(),
                ) {
                    let id = skill.id.clone();
                    let client = self.client.clone();
                    tokio::spawn(async move {
                        let r = client.get_skill(&id).await.map_err(|e| e.to_string());
                        let _ = tx.send(AppEvent::SkillDetailLoaded(r));
                    });
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Record a notification in the scrollback log and mirror it to the
    /// transient status line. Warn/error entries bump the unseen-alert badge
    /// until the log is opened. The log is capped so it can't grow unbounded.
    fn notify(&mut self, level: NoticeLevel, text: impl Into<String>) {
        let text = text.into();
        if matches!(level, NoticeLevel::Warn | NoticeLevel::Error) {
            self.unseen_alerts = self.unseen_alerts.saturating_add(1);
        }
        self.status_message = text.clone();
        self.notifications.push(Notification {
            level,
            text,
            at: Utc::now(),
        });
        const CAP: usize = 200;
        if self.notifications.len() > CAP {
            let excess = self.notifications.len() - CAP;
            self.notifications.drain(0..excess);
        }
    }

    fn handle_config_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Down => self.config_scroll_down(1),
            KeyCode::Up => self.config_scroll_up(1),
            KeyCode::PageDown => self.config_scroll_down(10),
            KeyCode::PageUp => self.config_scroll_up(10),
            KeyCode::Char('e') => {
                // Open the raw-JSON editor prefilled with the current config.
                match &self.config.config {
                    Some(val) => {
                        let pretty =
                            serde_json::to_string_pretty(val).unwrap_or_else(|_| val.to_string());
                        let lines: Vec<String> = pretty.lines().map(String::from).collect();
                        self.config_editor = Some(ConfigEditor {
                            textarea: TextArea::new(lines),
                        });
                    }
                    None => {
                        self.notify(NoticeLevel::Warn, "No config loaded to edit");
                    }
                }
            }
            _ => {}
        }
    }

    /// Drive the raw-JSON config editor modal. `Ctrl+S` validates the buffer as
    /// JSON and, if valid, PATCHes it to the server off the event loop; `Esc`
    /// cancels. Every other key edits the text buffer.
    fn handle_config_editor_key(&mut self, key: KeyEvent) -> Result<()> {
        if key.code == KeyCode::Esc {
            self.config_editor = None;
            self.status_message = "Edit cancelled".to_string();
            return Ok(());
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('s') {
            let text = self
                .config_editor
                .as_ref()
                .map(|e| e.textarea.lines().join("\n"))
                .unwrap_or_default();
            match serde_json::from_str::<serde_json::Value>(&text) {
                Ok(val) => {
                    self.config_editor = None;
                    self.status_message = "Saving config...".to_string();
                    if let Some(tx) = self.event_tx.clone() {
                        let client = self.client.clone();
                        tokio::spawn(async move {
                            let outcome = match client.set_config(&val).await {
                                Ok(()) => Ok("Config saved".to_string()),
                                Err(e) => Err(format!("Save failed: {e}")),
                            };
                            let _ = tx.send(AppEvent::ActionDone {
                                outcome,
                                reload_tab: true,
                            });
                        });
                    }
                }
                Err(e) => {
                    self.notify(NoticeLevel::Error, format!("Invalid JSON: {e}"));
                }
            }
            return Ok(());
        }
        if let Some(editor) = self.config_editor.as_mut() {
            editor.textarea.input(key);
        }
        Ok(())
    }

    /// `Ctrl+O` on the Chat tab: open the model picker and load the provider
    /// catalog off the event loop — the modal opens immediately (`loading:
    /// true`, empty list) and populates when `AppEvent::CatalogLoaded` lands.
    fn open_model_picker(&mut self) {
        self.model_picker = Some(ModelPicker {
            models: Vec::new(),
            selected: 0,
            loading: true,
        });
        self.status_message = "Loading models...".to_string();
        let Some(tx) = self.event_tx.clone() else {
            return;
        };
        let client = self.client.clone();
        tokio::spawn(async move {
            let r = client
                .get_provider_catalog()
                .await
                .map_err(|e| e.to_string());
            let _ = tx.send(AppEvent::CatalogLoaded(r));
        });
    }

    /// Drive the model picker modal: `↑/↓`/`j`/`k` move the selection, `Enter`
    /// applies the highlighted model (a no-op while the catalog is still
    /// loading — the list is empty, so there's nothing to apply), `Esc`
    /// closes without changes.
    async fn handle_model_picker_key(&mut self, key: KeyEvent) -> Result<()> {
        let Some(picker) = self.model_picker.as_mut() else {
            return Ok(());
        };
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => {
                picker.selected = picker.selected.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if picker.selected + 1 < picker.models.len() {
                    picker.selected += 1;
                }
            }
            KeyCode::Enter => {
                if let Some(model) = picker.models.get(picker.selected).cloned() {
                    self.model_picker = None;
                    self.apply_model(model);
                }
            }
            KeyCode::Esc => {
                self.model_picker = None;
                self.status_message = "Ready".to_string();
            }
            _ => {}
        }
        Ok(())
    }

    /// Apply a model picked from the catalog. `chat.model` gets the plain
    /// model id — the string form `ChatRequest.model` / `ExecuteRequest.model`
    /// / `PatchSessionRequest.model` actually resolve on the server (see
    /// `execute::types::ExecuteRequest`'s doc comment: `request.model` is a
    /// bare id, not a `provider/model` pair; that pairing only exists via the
    /// separate `model_ref` field, which this picker deliberately doesn't
    /// use). If a session is already active, also fires a fire-and-forget
    /// PATCH so the server-side session record doesn't drift from what the
    /// next turn will actually send.
    fn apply_model(&mut self, model: CatalogModel) {
        let model_id = model.reference.model.clone();
        self.chat.model = model_id.clone();
        self.status_message = format!("Model: {}", model.display_name);

        if let (Some(session_id), Some(tx)) = (self.chat.session_id.clone(), self.event_tx.clone())
        {
            let client = self.client.clone();
            tokio::spawn(async move {
                let outcome = match client.patch_session_model(&session_id, &model_id).await {
                    Ok(()) => Ok("Session model updated".to_string()),
                    Err(e) => Err(format!("Failed to update session model: {e}")),
                };
                let _ = tx.send(AppEvent::ActionDone {
                    outcome,
                    reload_tab: false,
                });
            });
        }
    }
}

#[cfg(test)]
mod question_tests {
    use super::*;
    use bamboo_client_core::ToolResult;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::empty())
    }

    fn app_with_question(options: Vec<&str>) -> App {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        let options: Vec<String> = options.into_iter().map(String::from).collect();
        let custom = if options.is_empty() {
            Some(String::new())
        } else {
            None
        };
        app.pending_question = Some(ActiveQuestion {
            question: "Run this command?".to_string(),
            options,
            selected: 0,
            custom,
            submitting: false,
        });
        app
    }

    #[tokio::test]
    async fn option_navigation_and_custom_toggle() {
        let mut app = app_with_question(vec!["Approve", "Deny"]);

        // Down moves the selection, clamped; Up moves back, clamped at 0.
        app.handle_question_key(key(KeyCode::Down)).await.unwrap();
        assert_eq!(app.pending_question.as_ref().unwrap().selected, 1);
        app.handle_question_key(key(KeyCode::Down)).await.unwrap();
        assert_eq!(app.pending_question.as_ref().unwrap().selected, 1); // clamped
        app.handle_question_key(key(KeyCode::Up)).await.unwrap();
        assert_eq!(app.pending_question.as_ref().unwrap().selected, 0);

        // `c` switches to free-text; typing fills the buffer; Esc returns to options.
        app.handle_question_key(key(KeyCode::Char('c')))
            .await
            .unwrap();
        assert!(app.pending_question.as_ref().unwrap().custom.is_some());
        app.handle_question_key(key(KeyCode::Char('h')))
            .await
            .unwrap();
        app.handle_question_key(key(KeyCode::Char('i')))
            .await
            .unwrap();
        assert_eq!(
            app.pending_question.as_ref().unwrap().custom.as_deref(),
            Some("hi")
        );
        app.handle_question_key(key(KeyCode::Backspace))
            .await
            .unwrap();
        assert_eq!(
            app.pending_question.as_ref().unwrap().custom.as_deref(),
            Some("h")
        );
        app.handle_question_key(key(KeyCode::Esc)).await.unwrap();
        assert!(app.pending_question.as_ref().unwrap().custom.is_none());
    }

    #[tokio::test]
    async fn esc_in_option_mode_dismisses() {
        let mut app = app_with_question(vec!["Approve", "Deny"]);
        app.handle_question_key(key(KeyCode::Esc)).await.unwrap();
        assert!(app.pending_question.is_none());
    }

    #[tokio::test]
    async fn submitting_without_a_session_clears_the_question() {
        // No chat session ⇒ submit short-circuits (no network) and clears the modal.
        let mut app = app_with_question(vec!["Approve", "Deny"]);
        assert!(app.chat.session_id.is_none());
        app.handle_question_key(key(KeyCode::Enter)).await.unwrap();
        assert!(app.pending_question.is_none());
    }

    #[tokio::test]
    async fn no_options_opens_in_free_text_mode() {
        let app = app_with_question(vec![]);
        assert!(app.pending_question.as_ref().unwrap().custom.is_some());
    }

    #[test]
    fn question_modal_renders_without_panicking() {
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;

        let app = app_with_question(vec!["Approve", "Deny"]);
        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| crate::ui::render(f, &app)).unwrap();

        // The rendered buffer should contain the question and an option label.
        let buf = terminal.backend().buffer().clone();
        let text: String = buf.content().iter().map(|c| c.symbol()).collect();
        assert!(text.contains("Run this command?"), "question text missing");
        assert!(text.contains("Approve"), "option label missing");
    }

    /// Enter dispatches the answer POST off the event loop: the modal stays
    /// open in a submitting state with input disabled (no double-submit on
    /// repeated Enter, no dismissal out from under the request), and the
    /// spawned task posts exactly one `AnswerSubmitted` back through
    /// `event_tx`.
    #[tokio::test]
    async fn submit_dispatches_async_and_sets_in_flight() {
        let mut app = app_with_question(vec!["Approve", "Deny"]);
        app.chat.session_id = Some("sess-1".to_string());
        let (tx, mut rx) = mpsc::unbounded_channel();
        app.event_tx = Some(tx);

        app.handle_question_key(key(KeyCode::Enter)).await.unwrap();

        // The modal stays open, marked in flight — NOT cleared synchronously
        // (the old inline-await path cleared it only after the server reply).
        let q = app.pending_question.as_ref().expect("modal stays open");
        assert!(q.submitting, "submitting flag must be set");
        assert!(app.status_message.contains("Submitting"));

        // While in flight, every key is swallowed: navigation is frozen, a
        // repeated Enter is a no-op, and Esc cannot dismiss the modal.
        app.handle_question_key(key(KeyCode::Down)).await.unwrap();
        assert_eq!(app.pending_question.as_ref().unwrap().selected, 0);
        app.handle_question_key(key(KeyCode::Enter)).await.unwrap();
        app.handle_question_key(key(KeyCode::Esc)).await.unwrap();
        assert!(
            app.pending_question.is_some(),
            "Esc is disabled while submitting"
        );

        // The spawned task posts its result back (the POST itself fails fast
        // here — nothing listens on port 0 — which is fine: the dispatch and
        // its epoch/answer payload are what's under test).
        let event = tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv())
            .await
            .expect("async result must be posted back")
            .expect("channel open");
        let AppEvent::AnswerSubmitted {
            epoch,
            answer,
            result,
        } = event
        else {
            panic!("expected AnswerSubmitted");
        };
        assert_eq!(answer, "Approve");
        assert_eq!(epoch, app.answer_epoch, "in-flight epoch is current");
        assert!(result.is_err(), "no server behind port 0");
        // The swallowed repeat-Enter must not have dispatched a second POST.
        assert!(rx.try_recv().is_err(), "exactly one dispatch expected");
    }

    /// A late `AnswerSubmitted` whose epoch no longer matches (the question
    /// was superseded mid-flight by a new one arriving over SSE) is discarded
    /// outright: it must neither clear the new modal nor flip streaming.
    #[tokio::test]
    async fn stale_epoch_answer_response_is_discarded() {
        let mut app = app_with_question(vec!["Approve", "Deny"]);
        app.chat.session_id = Some("sess-1".to_string());
        let (tx, _rx) = mpsc::unbounded_channel();
        app.event_tx = Some(tx);

        // Submit → the in-flight POST carries this epoch.
        app.handle_question_key(key(KeyCode::Enter)).await.unwrap();
        let stale_epoch = app.answer_epoch;

        // A NEW question arrives before the response lands — supersedes it.
        app.handle_sse_event(AgentEvent::NeedClarification {
            question: "Second question?".to_string(),
            options: Some(vec!["A".to_string(), "B".to_string()]),
        })
        .unwrap();
        assert_ne!(app.answer_epoch, stale_epoch, "supersede bumps the epoch");

        // The stale success response must be discarded, not applied.
        app.handle_event(AppEvent::AnswerSubmitted {
            epoch: stale_epoch,
            answer: "Approve".to_string(),
            result: Ok("started".to_string()),
        })
        .await
        .unwrap();

        let q = app.pending_question.as_ref().expect("new question stays");
        assert_eq!(q.question, "Second question?");
        assert!(!q.submitting, "the new question is not in flight");
        assert!(
            !app.chat.streaming,
            "stale response must not flip streaming"
        );

        // Session switch / run finalization also supersede an in-flight answer.
        app.handle_question_key(key(KeyCode::Enter)).await.unwrap();
        let stale_epoch = app.answer_epoch;
        app.finalize_streaming();
        assert_ne!(app.answer_epoch, stale_epoch);
        app.handle_event(AppEvent::AnswerSubmitted {
            epoch: stale_epoch,
            answer: "A".to_string(),
            result: Ok("started".to_string()),
        })
        .await
        .unwrap();
        assert!(!app.chat.streaming, "answer for a finalized run discarded");
    }

    /// A failed submit re-enables the modal (question kept, `submitting`
    /// cleared so Enter can retry) and surfaces the error through the
    /// notification overlay.
    #[tokio::test]
    async fn failed_submit_restores_question_and_notifies() {
        let mut app = app_with_question(vec!["Approve", "Deny"]);
        app.chat.session_id = Some("sess-1".to_string());
        let (tx, _rx) = mpsc::unbounded_channel();
        app.event_tx = Some(tx);

        app.handle_question_key(key(KeyCode::Enter)).await.unwrap();
        assert!(app.pending_question.as_ref().unwrap().submitting);

        app.handle_event(AppEvent::AnswerSubmitted {
            epoch: app.answer_epoch,
            answer: "Approve".to_string(),
            result: Err("boom".to_string()),
        })
        .await
        .unwrap();

        let q = app
            .pending_question
            .as_ref()
            .expect("question kept for retry");
        assert!(!q.submitting, "input re-enabled for retry");
        let last = app.notifications.last().expect("notified");
        assert_eq!(last.level, NoticeLevel::Error);
        assert!(last.text.contains("boom"));

        // And a retry dispatches again.
        app.handle_question_key(key(KeyCode::Enter)).await.unwrap();
        assert!(app.pending_question.as_ref().unwrap().submitting);
    }

    /// Success preserves the pre-existing post-submit semantics: the modal
    /// clears, and the `auto_resume_status` gate decides whether the spinner
    /// stays on (`started`/`already_running` — the SSE stream opened for the
    /// run is still attached) or streaming is finalized (any other status).
    #[tokio::test]
    async fn successful_submit_applies_auto_resume_gate() {
        // `started` → modal cleared, streaming on.
        let mut app = app_with_question(vec!["Approve", "Deny"]);
        app.chat.session_id = Some("sess-1".to_string());
        let (tx, _rx) = mpsc::unbounded_channel();
        app.event_tx = Some(tx);
        app.handle_question_key(key(KeyCode::Enter)).await.unwrap();
        app.handle_event(AppEvent::AnswerSubmitted {
            epoch: app.answer_epoch,
            answer: "Approve".to_string(),
            result: Ok("started".to_string()),
        })
        .await
        .unwrap();
        assert!(app.pending_question.is_none(), "modal clears on success");
        assert!(app.chat.streaming, "resuming run keeps the spinner on");
        assert!(app.status_message.contains("resuming"));

        // `completed` (nothing resumed server-side) → modal cleared,
        // streaming finalized instead of spinning forever.
        let mut app = app_with_question(vec!["Approve", "Deny"]);
        app.chat.session_id = Some("sess-1".to_string());
        let (tx, _rx) = mpsc::unbounded_channel();
        app.event_tx = Some(tx);
        app.handle_question_key(key(KeyCode::Enter)).await.unwrap();
        app.handle_event(AppEvent::AnswerSubmitted {
            epoch: app.answer_epoch,
            answer: "Approve".to_string(),
            result: Ok("completed".to_string()),
        })
        .await
        .unwrap();
        assert!(app.pending_question.is_none());
        assert!(!app.chat.streaming, "non-resuming status must not spin");
    }

    /// While the POST is out, the modal renders the submitting state instead
    /// of the interactive key hints.
    #[test]
    fn question_modal_shows_submitting_state() {
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;

        let mut app = app_with_question(vec!["Approve", "Deny"]);
        app.pending_question.as_mut().unwrap().submitting = true;
        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| crate::ui::render(f, &app)).unwrap();

        let buf = terminal.backend().buffer().clone();
        let text: String = buf.content().iter().map(|c| c.symbol()).collect();
        assert!(
            text.contains("Submitting answer"),
            "submitting hint missing"
        );
        assert!(
            !text.contains("Enter answer"),
            "interactive hints must be hidden while in flight"
        );
    }

    #[test]
    fn many_options_window_keeps_selection_visible() {
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;

        let opts: Vec<String> = (1..=30).map(|n| format!("opt{n}")).collect();
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.pending_question = Some(ActiveQuestion {
            question: "Pick one".to_string(),
            options: opts,
            selected: 24, // "25. opt25", deep in the list
            custom: None,
            submitting: false,
        });

        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| crate::ui::render(f, &app)).unwrap();
        let buf = terminal.backend().buffer().clone();
        let text: String = buf.content().iter().map(|c| c.symbol()).collect();

        // The selected option is visible even though it's far down the list, and
        // an overflow marker indicates hidden options above.
        assert!(text.contains("opt25"), "selected option not in the window");
        assert!(text.contains("more"), "overflow marker missing");
    }

    fn bare_session(id: &str) -> SessionSummary {
        SessionSummary {
            id: id.into(),
            project_id: None,
            title: String::new(),
            model: String::new(),
            is_running: false,
            has_pending_question: false,
            last_run_status: None,
            updated_at: None,
            message_count: 0,
            pinned: false,
        }
    }

    #[tokio::test]
    async fn loaded_event_applies_to_state_and_clears_loading() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.sessions.loading = true;
        app.handle_event(AppEvent::SessionsLoaded(Ok(ListSessionsEnvelope {
            sessions: vec![bare_session("s1")],
            total: 1,
            limit: 200,
            offset: 0,
            next_offset: None,
        })))
        .await
        .unwrap();
        assert!(!app.sessions.loading, "loading flag must clear");
        assert_eq!(app.sessions.sessions.len(), 1);
        assert_eq!(app.sessions.total, 1);
        assert_eq!(app.sessions.page_limit, 200);
        assert!(app.sessions.next_offset.is_none());
        assert!(app.sessions.error.is_none());

        // Error result surfaces on the tab and clears loading.
        app.sessions.loading = true;
        app.handle_event(AppEvent::SessionsLoaded(Err("boom".into())))
            .await
            .unwrap();
        assert!(!app.sessions.loading);
        assert_eq!(app.sessions.error.as_deref(), Some("boom"));
    }

    /// `]` advances to the next page only when the server reported one; `[`
    /// steps back by a full page and clamps at 0. Selection resets on every
    /// page change.
    #[tokio::test]
    async fn sessions_paging_keys_respect_next_offset_and_clamp() {
        let k = |c| KeyEvent::new(c, KeyModifiers::empty());
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Sessions;
        app.sessions.sessions = vec![bare_session("s1"), bare_session("s2")];
        app.sessions.selected = 1;
        app.sessions.total = 5;
        app.sessions.page_limit = 2;
        app.sessions.offset = 0;
        app.sessions.next_offset = Some(2);

        // No event_tx wired (tests construct App directly), so `load_tab_data`
        // is a no-op past the state mutation — enough to assert the key logic.
        app.handle_sessions_key(k(KeyCode::Char(']')))
            .await
            .unwrap();
        assert_eq!(app.sessions.offset, 2, "] advances by the reported page");
        assert_eq!(app.sessions.selected, 0, "selection resets on page change");

        // On the last page (`next_offset` is `None`), `]` is a no-op.
        app.sessions.next_offset = None;
        app.sessions.selected = 1;
        app.handle_sessions_key(k(KeyCode::Char(']')))
            .await
            .unwrap();
        assert_eq!(app.sessions.offset, 2, "] does nothing past the last page");
        assert_eq!(app.sessions.selected, 1, "no page change, no reset");

        // `[` steps back by a full page.
        app.handle_sessions_key(k(KeyCode::Char('[')))
            .await
            .unwrap();
        assert_eq!(app.sessions.offset, 0);
        assert_eq!(app.sessions.selected, 0);

        // Already at offset 0: `[` clamps and does not go negative (saturating).
        app.sessions.selected = 1;
        app.handle_sessions_key(k(KeyCode::Char('[')))
            .await
            .unwrap();
        assert_eq!(app.sessions.offset, 0);
        assert_eq!(
            app.sessions.selected, 1,
            "no page change at offset 0, no reset"
        );
    }

    /// `d` on the Sessions tab opens a confirmation modal instead of deleting
    /// immediately; `Esc`/`n` cancel it without touching `event_tx`.
    #[tokio::test]
    async fn sessions_delete_opens_confirm_modal_and_cancel_clears_it() {
        let k = |c| KeyEvent::new(c, KeyModifiers::empty());
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Sessions;
        app.sessions.sessions = vec![bare_session("s1")];
        app.sessions.selected = 0;

        app.handle_key(k(KeyCode::Char('d'))).await.unwrap();
        assert_eq!(
            app.pending_delete.as_ref().map(|(id, _)| id.as_str()),
            Some("s1")
        );

        // While the modal is open, keys route to it — not tab switching.
        app.handle_key(k(KeyCode::Char('1'))).await.unwrap();
        assert_eq!(app.tab, Tab::Sessions, "digit must not switch tabs");
        assert!(app.pending_delete.is_some());

        app.handle_key(k(KeyCode::Esc)).await.unwrap();
        assert!(app.pending_delete.is_none(), "Esc cancels");
    }

    /// Confirming (`y`) clears the modal and posts the delete off the event
    /// loop; without `run()` having wired `event_tx`, there's nothing to spawn
    /// into, so this only asserts the modal itself is cleared synchronously.
    #[tokio::test]
    async fn sessions_delete_confirm_clears_modal() {
        let k = |c| KeyEvent::new(c, KeyModifiers::empty());
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.pending_delete = Some(("s1".to_string(), "My session".to_string()));
        app.handle_key(k(KeyCode::Char('y'))).await.unwrap();
        assert!(app.pending_delete.is_none());
    }

    /// F1/Ctrl+L must not stack the help/notification overlay on top of an
    /// already-open modal — `any_modal_open` gates them so the keystroke
    /// falls through to the modal's own handler instead (a no-op there,
    /// since neither key means anything to the delete-confirm modal).
    #[tokio::test]
    async fn f1_and_ctrl_l_are_suppressed_while_a_modal_is_open() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.pending_delete = Some(("s1".to_string(), "My session".to_string()));

        app.handle_key(KeyEvent::new(KeyCode::F(1), KeyModifiers::empty()))
            .await
            .unwrap();
        assert!(!app.help_visible, "F1 must not open help over a modal");
        assert!(app.pending_delete.is_some(), "the modal must stay open");

        app.handle_key(KeyEvent::new(KeyCode::Char('l'), KeyModifiers::CONTROL))
            .await
            .unwrap();
        assert!(
            !app.notifications_visible,
            "Ctrl+L must not open the log over a modal"
        );
        assert!(app.pending_delete.is_some(), "the modal must stay open");
    }

    #[test]
    fn mouse_wheel_scrolls_chat() {
        use crossterm::event::{MouseEvent, MouseEventKind};
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Chat;
        app.chat.scroll_offset = 10;
        // Simulates a render having already established the bound (a real
        // frame always renders before the first input is handled).
        app.chat.max_scroll.set(50);
        let ev = |k| MouseEvent {
            kind: k,
            column: 0,
            row: 0,
            modifiers: crossterm::event::KeyModifiers::empty(),
        };
        app.handle_mouse(ev(MouseEventKind::ScrollUp));
        assert_eq!(app.chat.scroll_offset, 7);
        assert!(!app.chat.auto_scroll);
        app.handle_mouse(ev(MouseEventKind::ScrollDown));
        assert_eq!(app.chat.scroll_offset, 10);
    }

    /// Scrolling down is clamped to `max_scroll` (set by the render function
    /// each frame): spamming `j` past the bottom must not overshoot into a
    /// dead zone that then eats several `k` presses before the view moves.
    #[tokio::test]
    async fn chat_scroll_down_clamps_to_max_scroll() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let k = |c| KeyEvent::new(c, KeyModifiers::empty());
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Chat;
        app.chat.max_scroll.set(5);

        for _ in 0..50 {
            app.chat_scroll_down(3);
        }
        assert_eq!(
            app.chat.scroll_offset, 5,
            "scrolling down must clamp at max_scroll, not grow unbounded"
        );

        // One `k` must immediately move the view (not be swallowed catching
        // up from an overshot offset).
        app.handle_key(k(KeyCode::Char('k'))).await.unwrap();
        assert_eq!(app.chat.scroll_offset, 2);
    }

    #[test]
    fn schedule_form_cycles_and_validates() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        fn k(c: KeyCode) -> KeyEvent {
            KeyEvent::new(c, KeyModifiers::empty())
        }

        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.schedule_form = Some(ScheduleForm::default());

        // Type into the name field, then Tab to cron.
        app.handle_schedule_form_key(k(KeyCode::Char('h')));
        app.handle_schedule_form_key(k(KeyCode::Char('i')));
        assert_eq!(app.schedule_form.as_ref().unwrap().name, "hi");
        app.handle_schedule_form_key(k(KeyCode::Tab));
        assert_eq!(app.schedule_form.as_ref().unwrap().field, 1);

        // Enter with empty cron/prompt does NOT submit (form stays open).
        app.handle_schedule_form_key(k(KeyCode::Enter));
        assert!(
            app.schedule_form.is_some(),
            "incomplete form must not submit"
        );

        // Esc cancels.
        app.handle_schedule_form_key(k(KeyCode::Esc));
        assert!(app.schedule_form.is_none());
    }

    /// Regression: while the form is open, `handle_key` must route Tab and the
    /// 1-6 digit keys to the form instead of switching app tabs (cron
    /// expressions are full of digits, Tab moves between fields).
    #[tokio::test]
    async fn schedule_form_captures_keys_through_handle_key() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let k = |c| KeyEvent::new(c, KeyModifiers::empty());

        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Schedules;
        app.handle_key(k(KeyCode::Char('n'))).await.unwrap();
        assert!(app.schedule_form.is_some(), "`n` opens the form");

        // A digit must be typed into the focused field, not switch to tab N.
        app.handle_key(k(KeyCode::Char('1'))).await.unwrap();
        assert_eq!(app.tab, Tab::Schedules, "digit must not switch tabs");
        assert_eq!(app.schedule_form.as_ref().unwrap().name, "1");

        // Tab must advance the form field, not switch app tab.
        app.handle_key(k(KeyCode::Tab)).await.unwrap();
        assert_eq!(app.tab, Tab::Schedules, "Tab must not switch tabs");
        assert_eq!(app.schedule_form.as_ref().unwrap().field, 1);
    }

    #[tokio::test]
    async fn config_editor_opens_validates_and_cancels() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let plain = |c| KeyEvent::new(c, KeyModifiers::empty());
        let ctrl_s = KeyEvent::new(KeyCode::Char('s'), KeyModifiers::CONTROL);

        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Config;

        // `e` with no config loaded just posts a status, opens nothing.
        app.handle_key(plain(KeyCode::Char('e'))).await.unwrap();
        assert!(app.config_editor.is_none());

        // With a config, `e` opens the editor prefilled.
        app.config.config = Some(serde_json::json!({ "a": 1 }));
        app.handle_key(plain(KeyCode::Char('e'))).await.unwrap();
        assert!(app.config_editor.is_some());

        // Corrupt the buffer (prepend a stray char) → Ctrl+S must NOT close it.
        app.handle_key(plain(KeyCode::Char('x'))).await.unwrap();
        app.handle_key(ctrl_s).await.unwrap();
        assert!(
            app.config_editor.is_some(),
            "invalid JSON must keep the editor open"
        );
        assert!(app.status_message.contains("Invalid JSON"));

        // Esc cancels.
        app.handle_key(plain(KeyCode::Esc)).await.unwrap();
        assert!(app.config_editor.is_none());

        // A clean open + immediate Ctrl+S (buffer is valid) closes the editor.
        app.handle_key(plain(KeyCode::Char('e'))).await.unwrap();
        assert!(app.config_editor.is_some());
        app.handle_key(ctrl_s).await.unwrap();
        assert!(app.config_editor.is_none(), "valid JSON saves and closes");
    }

    #[tokio::test]
    async fn notifications_log_and_badge() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));

        // An error records to the log, mirrors to the status line, bumps badge.
        app.notify(NoticeLevel::Error, "boom");
        assert_eq!(app.notifications.len(), 1);
        assert_eq!(app.status_message, "boom");
        assert_eq!(app.unseen_alerts, 1);

        // Info records but does not bump the alert badge.
        app.notify(NoticeLevel::Info, "just fyi");
        assert_eq!(app.unseen_alerts, 1);
        assert_eq!(app.notifications.len(), 2);

        // Ctrl+L opens the log and clears the badge.
        app.handle_key(KeyEvent::new(KeyCode::Char('l'), KeyModifiers::CONTROL))
            .await
            .unwrap();
        assert!(app.notifications_visible);
        assert_eq!(app.unseen_alerts, 0);

        // Any key dismisses the overlay.
        app.handle_event(AppEvent::Key(KeyEvent::new(
            KeyCode::Char('q'),
            KeyModifiers::empty(),
        )))
        .await
        .unwrap();
        assert!(!app.notifications_visible);
    }

    #[test]
    fn notifications_log_is_capped() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        for i in 0..250 {
            app.notify(NoticeLevel::Info, format!("n{i}"));
        }
        assert_eq!(app.notifications.len(), 200, "log is capped at 200");
        // Oldest entries are dropped; the newest is retained.
        assert_eq!(app.notifications.last().unwrap().text, "n249");
        assert_eq!(app.notifications.first().unwrap().text, "n50");
    }

    #[tokio::test]
    async fn subagent_lifecycle_tracks_children() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.streaming = true;
        app.handle_sse_event(AgentEvent::SubAgentStarted {
            child_session_id: "c1".into(),
            title: Some("research".into()),
        })
        .unwrap();
        assert_eq!(app.chat.sub_agents.len(), 1);
        assert_eq!(app.chat.sub_agents[0].status, "running");

        // Duplicate start is ignored.
        app.handle_sse_event(AgentEvent::SubAgentStarted {
            child_session_id: "c1".into(),
            title: None,
        })
        .unwrap();
        assert_eq!(app.chat.sub_agents.len(), 1);

        app.handle_sse_event(AgentEvent::SubAgentCompleted {
            child_session_id: "c1".into(),
            status: "completed".into(),
            error: None,
        })
        .unwrap();
        assert_eq!(app.chat.sub_agents[0].status, "completed");
    }

    /// Parallel tool calls: a `ToolComplete` must land on the entry whose
    /// `tool_call_id` it names, not on whichever entry is last in the list.
    #[tokio::test]
    async fn tool_events_pair_by_id_not_position() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.streaming = true;
        app.handle_sse_event(AgentEvent::ToolStart {
            tool_call_id: "a".into(),
            tool_name: "Read".into(),
            arguments: serde_json::json!({}),
        })
        .unwrap();
        app.handle_sse_event(AgentEvent::ToolStart {
            tool_call_id: "b".into(),
            tool_name: "Write".into(),
            arguments: serde_json::json!({}),
        })
        .unwrap();

        // "b" was started most recently (last in the list), but the Complete
        // event names "a" — position-based pairing would wrongly land this on
        // "b".
        app.handle_sse_event(AgentEvent::ToolComplete {
            tool_call_id: "a".into(),
            result: ToolResult {
                success: true,
                result: "a-result".into(),
            },
        })
        .unwrap();

        let calls = &app.chat.current_tool_calls;
        assert_eq!(calls.len(), 2, "no entry is dropped or duplicated");
        let a = calls.iter().find(|t| t.id == "a").unwrap();
        let b = calls.iter().find(|t| t.id == "b").unwrap();
        assert_eq!(a.result.as_deref(), Some("a-result"));
        assert_eq!(a.phase, "complete");
        assert!(b.result.is_none(), "b's result must be untouched");
        assert_eq!(b.phase, "running", "b must still be running");
    }

    /// A `ToolComplete`/`ToolError` for an id with no matching `ToolStart` is
    /// surfaced defensively (not silently dropped).
    #[tokio::test]
    async fn tool_complete_for_unknown_id_inserts_defensively() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.streaming = true;
        app.handle_sse_event(AgentEvent::ToolComplete {
            tool_call_id: "ghost".into(),
            result: ToolResult {
                success: true,
                result: "surprise".into(),
            },
        })
        .unwrap();

        assert_eq!(app.chat.current_tool_calls.len(), 1);
        let tc = &app.chat.current_tool_calls[0];
        assert_eq!(tc.id, "ghost");
        assert_eq!(tc.tool_name, "unknown");
        assert_eq!(tc.result.as_deref(), Some("surprise"));
        assert_eq!(tc.phase, "complete");
    }

    /// `AppEvent::ExecuteFailed` (posted when the `execute` POST itself fails)
    /// must clear `chat.streaming` even though no SSE terminal event ever
    /// arrived for the run.
    #[tokio::test]
    async fn execute_failed_event_clears_streaming() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.streaming = true;
        app.chat.current_response = "partial".to_string();

        app.handle_event(AppEvent::ExecuteFailed("connection refused".to_string()))
            .await
            .unwrap();

        assert!(
            !app.chat.streaming,
            "streaming must clear on execute failure"
        );
        // notify() runs before finalize_streaming() (mirroring the existing
        // AgentEvent::Error handler), so the transient status line ends up
        // "Ready" — the failure detail is preserved in the notification log
        // instead, which is what Ctrl+L surfaces.
        let last = app.notifications.last().expect("notify logged an entry");
        assert!(last.text.contains("connection refused"));
        assert_eq!(last.level, NoticeLevel::Error);
    }

    /// `StopFinished(Err)` must still finalize streaming locally so the
    /// operator regains control of the input even when the stop request
    /// itself failed (e.g. the server is unreachable) — `App::running` stays
    /// `true` (the app itself does not exit).
    #[tokio::test]
    async fn stop_failure_still_finalizes_and_keeps_app_running() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.streaming = true;
        app.chat.session_id = Some("s1".to_string());

        app.handle_event(AppEvent::StopFinished(
            Err("server unreachable".to_string()),
        ))
        .await
        .unwrap();

        assert!(
            !app.chat.streaming,
            "streaming must clear despite the error"
        );
        assert!(app.running, "a failed stop must not tear down the app");
        assert!(app.status_message.contains("server unreachable"));
    }

    /// `StopFinished(Ok)` finalizes streaming and reports "Stopped".
    #[tokio::test]
    async fn stop_success_finalizes_with_stopped_status() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.streaming = true;
        app.chat.session_id = Some("s1".to_string());

        app.handle_event(AppEvent::StopFinished(Ok(())))
            .await
            .unwrap();

        assert!(!app.chat.streaming);
        assert_eq!(app.status_message, "Stopped");
    }

    // ── Session resume (WP3) ──

    fn opened(messages: Vec<ChatMessage>) -> OpenedSession {
        OpenedSession {
            messages,
            model: "claude-sonnet-5".to_string(),
            project_id: None,
            is_running: false,
            pending: None,
            truncated: false,
            total_message_count: 0,
        }
    }

    fn asst_msg(content: &str) -> ChatMessage {
        ChatMessage {
            role: MessageRole::Assistant,
            content: content.to_string(),
            tool_calls: Vec::new(),
            reasoning: None,
        }
    }

    /// A successful resume installs the mapped history, the model, and the
    /// session id; switches to the Chat tab; and leaves `streaming` false
    /// when the session isn't currently running.
    #[tokio::test]
    async fn session_opened_installs_state_and_switches_tab() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Sessions;
        // Leftover scratch state from a previous session — must be wiped.
        app.chat.current_response = "stale partial".to_string();
        app.chat.token_usage = Some(TokenUsage {
            prompt_tokens: 1,
            completion_tokens: 1,
            total_tokens: 2,
        });

        app.handle_event(AppEvent::SessionOpened {
            session_id: "s1".to_string(),
            result: Ok(opened(vec![asst_msg("hello again")])),
        })
        .await
        .unwrap();

        assert_eq!(app.chat.session_id.as_deref(), Some("s1"));
        assert_eq!(app.chat.model, "claude-sonnet-5");
        assert_eq!(app.chat.messages.len(), 1);
        assert_eq!(app.tab, Tab::Chat);
        assert!(app.chat.auto_scroll);
        assert!(
            !app.chat.streaming,
            "is_running: false must not start streaming"
        );
        assert!(app.chat.current_response.is_empty(), "scratch state wiped");
        assert!(app.chat.token_usage.is_none(), "scratch state wiped");
    }

    /// `is_running: true` reattaches the SSE stream and sets `streaming`.
    /// `event_tx` isn't wired in a bare `App::new`, so `attach_stream`'s
    /// `SseStream::start` call still runs (it only spawns a task, no network
    /// yet) — this asserts the flag flip, not a real connection.
    #[tokio::test]
    async fn session_opened_reattaches_when_running() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));

        app.handle_event(AppEvent::SessionOpened {
            session_id: "s1".to_string(),
            result: Ok(OpenedSession {
                is_running: true,
                ..opened(vec![])
            }),
        })
        .await
        .unwrap();

        assert!(app.chat.streaming, "is_running: true must reattach");
        assert_eq!(app.status_message, "Reattached — streaming");
    }

    /// A truncated resume surfaces "showing last N of M" as an Info
    /// notification.
    #[tokio::test]
    async fn session_opened_truncated_notifies() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));

        app.handle_event(AppEvent::SessionOpened {
            session_id: "s1".to_string(),
            result: Ok(OpenedSession {
                truncated: true,
                total_message_count: 5000,
                ..opened(vec![asst_msg("a"), asst_msg("b")])
            }),
        })
        .await
        .unwrap();

        let last = app.notifications.last().expect("truncation notified");
        assert!(last.text.contains("Showing last 2 of 5000 messages"));
    }

    /// A resumed session with a pending question opens the modal, matching
    /// the SSE `NeedClarification` free-text-when-no-options rule.
    #[tokio::test]
    async fn session_opened_with_pending_question_opens_modal() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));

        app.handle_event(AppEvent::SessionOpened {
            session_id: "s1".to_string(),
            result: Ok(OpenedSession {
                pending: Some(PendingQuestion {
                    has_pending_question: true,
                    question: "Proceed?".to_string(),
                    options: None,
                    allow_custom: true,
                    ..Default::default()
                }),
                ..opened(vec![])
            }),
        })
        .await
        .unwrap();

        let q = app.pending_question.as_ref().expect("modal opened");
        assert_eq!(q.question, "Proceed?");
        assert!(q.options.is_empty());
        assert!(q.custom.is_some(), "no options ⇒ free-text entry");
    }

    /// A failed resume surfaces an error and touches nothing else.
    #[tokio::test]
    async fn session_opened_failure_notifies_and_leaves_chat_untouched() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.session_id = Some("old".to_string());

        app.handle_event(AppEvent::SessionOpened {
            session_id: "s1".to_string(),
            result: Err("not found".to_string()),
        })
        .await
        .unwrap();

        assert_eq!(
            app.chat.session_id.as_deref(),
            Some("old"),
            "a failed resume must not clobber the current session"
        );
        let last = app.notifications.last().expect("failure notified");
        assert!(last.text.contains("not found"));
        assert_eq!(last.level, NoticeLevel::Error);
    }

    /// `Ctrl+N` clears every session-scoped field but keeps the model and
    /// stable Project membership.
    #[tokio::test]
    async fn ctrl_n_clears_session_but_keeps_model() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.session_id = Some("s1".to_string());
        app.chat.model = "claude-sonnet-5".to_string();
        app.chat.project_id = Some("project-tui".to_string());
        app.chat.messages = vec![asst_msg("leftover")];
        app.chat.current_response = "partial".to_string();
        app.chat.token_usage = Some(TokenUsage {
            prompt_tokens: 1,
            completion_tokens: 1,
            total_tokens: 2,
        });
        app.dismissed_question = Some(ActiveQuestion {
            question: "q".to_string(),
            options: vec![],
            selected: 0,
            custom: Some(String::new()),
            submitting: false,
        });

        app.handle_key(KeyEvent::new(KeyCode::Char('n'), KeyModifiers::CONTROL))
            .await
            .unwrap();

        assert!(app.chat.session_id.is_none());
        assert!(app.chat.messages.is_empty());
        assert!(app.chat.current_response.is_empty());
        assert!(app.chat.token_usage.is_none());
        assert!(app.pending_question.is_none());
        assert!(
            app.dismissed_question.is_none(),
            "a stale cached question from the old session must not survive"
        );
        assert_eq!(
            app.chat.model, "claude-sonnet-5",
            "model must survive a new session"
        );
        assert_eq!(
            app.chat.project_id.as_deref(),
            Some("project-tui"),
            "Project membership must survive a new root session"
        );
        assert_eq!(app.status_message, "New session");
    }

    /// `Ctrl+N` is a no-op while a run is streaming (must stop it explicitly
    /// first, same as every other destructive Sessions/Chat action).
    #[tokio::test]
    async fn ctrl_n_is_noop_while_streaming() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.streaming = true;
        app.chat.session_id = Some("s1".to_string());

        app.handle_key(KeyEvent::new(KeyCode::Char('n'), KeyModifiers::CONTROL))
            .await
            .unwrap();

        assert_eq!(app.chat.session_id.as_deref(), Some("s1"));
    }

    /// Esc dismisses the question modal but caches it; `Ctrl+Q` brings it
    /// straight back without a network round-trip.
    #[tokio::test]
    async fn esc_then_ctrl_q_restores_the_dismissed_question() {
        let mut app = app_with_question(vec!["Approve", "Deny"]);
        app.chat.session_id = Some("s1".to_string());

        app.handle_question_key(key(KeyCode::Esc)).await.unwrap();
        assert!(app.pending_question.is_none());
        assert!(app.dismissed_question.is_some());
        assert!(app.status_message.contains("Ctrl+Q"));

        app.handle_key(KeyEvent::new(KeyCode::Char('q'), KeyModifiers::CONTROL))
            .await
            .unwrap();

        let q = app.pending_question.as_ref().expect("question restored");
        assert_eq!(q.question, "Run this command?");
        assert_eq!(q.options, vec!["Approve".to_string(), "Deny".to_string()]);
        assert!(app.dismissed_question.is_none(), "cache consumed on reopen");
    }

    /// With nothing cached and no active session, `Ctrl+Q` just notifies
    /// instead of spawning a doomed fetch.
    #[tokio::test]
    async fn ctrl_q_with_nothing_cached_and_no_session_notifies() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.handle_key(KeyEvent::new(KeyCode::Char('q'), KeyModifiers::CONTROL))
            .await
            .unwrap();
        assert!(app.pending_question.is_none());
        let last = app.notifications.last().expect("notified");
        assert!(last.text.contains("No pending question"));
    }

    /// `PendingQuestionChecked` (Ctrl+Q's server round-trip when nothing was
    /// cached) opens the modal when the server reports one waiting.
    #[tokio::test]
    async fn pending_question_checked_opens_when_present() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.handle_event(AppEvent::PendingQuestionChecked(Ok(PendingQuestion {
            has_pending_question: true,
            question: "Still there?".to_string(),
            options: Some(vec!["Yes".to_string()]),
            ..Default::default()
        })))
        .await
        .unwrap();
        assert_eq!(
            app.pending_question.as_ref().map(|q| q.question.as_str()),
            Some("Still there?")
        );
    }

    /// ...and reports there's nothing to reopen when the server agrees.
    #[tokio::test]
    async fn pending_question_checked_notifies_when_absent() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.handle_event(AppEvent::PendingQuestionChecked(Ok(PendingQuestion {
            has_pending_question: false,
            ..Default::default()
        })))
        .await
        .unwrap();
        assert!(app.pending_question.is_none());
        let last = app.notifications.last().expect("notified");
        assert!(last.text.contains("No pending question"));
    }

    /// Regression (mirrors `schedule_form_captures_keys_through_handle_key`):
    /// on the Chat tab, idle, a digit must type into the message textarea —
    /// not switch tabs — or something like "top 3 issues" silently jumps to
    /// the Mcp tab on the '3'.
    #[tokio::test]
    async fn digit_on_chat_tab_types_into_textarea_not_tab_switch() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let k = |c| KeyEvent::new(c, KeyModifiers::empty());

        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Chat;
        app.handle_key(k(KeyCode::Char('3'))).await.unwrap();
        assert_eq!(
            app.tab,
            Tab::Chat,
            "digit must not switch tabs while composing on Chat"
        );
        assert_eq!(app.chat.textarea.lines().join("\n"), "3");
    }

    /// ...but on every other tab digits still switch tabs (unchanged).
    #[tokio::test]
    async fn digit_on_sessions_tab_still_switches_tab() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let k = |c| KeyEvent::new(c, KeyModifiers::empty());

        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Sessions;
        app.handle_key(k(KeyCode::Char('3'))).await.unwrap();
        assert_eq!(
            app.tab,
            Tab::Mcp,
            "digit '3' switches to tab index 2 (Mcp) outside Chat"
        );
    }

    /// Alt+Enter inserts a newline into the compose textarea; it must not
    /// send the message the way plain Enter does.
    #[tokio::test]
    async fn alt_enter_inserts_newline_without_sending() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Chat;

        app.handle_key(KeyEvent::new(KeyCode::Char('h'), KeyModifiers::empty()))
            .await
            .unwrap();
        app.handle_key(KeyEvent::new(KeyCode::Char('i'), KeyModifiers::empty()))
            .await
            .unwrap();
        assert_eq!(app.chat.textarea.lines().len(), 1);

        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::ALT))
            .await
            .unwrap();
        assert_eq!(
            app.chat.textarea.lines().len(),
            2,
            "Alt+Enter must grow the textarea by a line, not send"
        );
        assert_eq!(app.chat.textarea.lines()[0], "hi");
        assert!(!app.chat.streaming, "Alt+Enter must not send the message");
        assert!(app.chat.messages.is_empty());
    }

    /// `?` is reachable as help everywhere except Chat, where it must type
    /// into the textarea instead (F1 is the Chat-safe way to reach help).
    #[tokio::test]
    async fn question_mark_opens_help_except_on_chat() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let k = || KeyEvent::new(KeyCode::Char('?'), KeyModifiers::empty());

        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Sessions;
        app.handle_key(k()).await.unwrap();
        assert!(app.help_visible, "'?' opens help on non-Chat tabs");

        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Chat;
        app.handle_key(k()).await.unwrap();
        assert!(
            !app.help_visible,
            "'?' must type into the chat textarea, not open help"
        );
        assert_eq!(app.chat.textarea.lines().join("\n"), "?");
    }

    /// F1 opens help on every tab, including Chat, unlike `?`.
    #[tokio::test]
    async fn f1_opens_help_on_chat_tab() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Chat;
        app.handle_key(KeyEvent::new(KeyCode::F(1), KeyModifiers::empty()))
            .await
            .unwrap();
        assert!(app.help_visible);
        assert!(
            app.chat.textarea.lines().join("\n").is_empty(),
            "F1 must not leak into the textarea"
        );
    }

    /// Mouse wheel on a list tab (Sessions here) moves the selection instead
    /// of being a dead input, 3 rows per notch and clamped to the list.
    #[test]
    fn mouse_wheel_on_sessions_moves_selection() {
        use crossterm::event::{MouseEvent, MouseEventKind};
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Sessions;
        app.sessions.sessions = vec![
            bare_session("s1"),
            bare_session("s2"),
            bare_session("s3"),
            bare_session("s4"),
            bare_session("s5"),
        ];
        app.sessions.selected = 0;
        let ev = |k| MouseEvent {
            kind: k,
            column: 0,
            row: 0,
            modifiers: crossterm::event::KeyModifiers::empty(),
        };

        app.handle_mouse(ev(MouseEventKind::ScrollDown));
        assert_eq!(app.sessions.selected, 3, "3 rows per notch");
        app.handle_mouse(ev(MouseEventKind::ScrollDown));
        assert_eq!(app.sessions.selected, 4, "clamped to the last index");
        app.handle_mouse(ev(MouseEventKind::ScrollUp));
        assert_eq!(app.sessions.selected, 1);
    }

    // ── Model picker (WP5) ──

    fn catalog_model(
        provider: &str,
        model: &str,
        display: &str,
        provider_display: &str,
    ) -> CatalogModel {
        CatalogModel {
            reference: CatalogModelRef {
                provider: provider.to_string(),
                model: model.to_string(),
            },
            display_name: display.to_string(),
            provider_display_name: provider_display.to_string(),
        }
    }

    /// `Ctrl+O` opens the picker only on the Chat tab, and only when idle —
    /// same rationale as `Ctrl+N` being a no-op while streaming.
    #[tokio::test]
    async fn ctrl_o_opens_model_picker_on_chat_tab_only() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Sessions;
        app.handle_key(KeyEvent::new(KeyCode::Char('o'), KeyModifiers::CONTROL))
            .await
            .unwrap();
        assert!(app.model_picker.is_none(), "Ctrl+O is Chat-tab only");

        app.tab = Tab::Chat;
        app.handle_key(KeyEvent::new(KeyCode::Char('o'), KeyModifiers::CONTROL))
            .await
            .unwrap();
        let picker = app.model_picker.as_ref().expect("Ctrl+O opens on Chat");
        assert!(picker.loading, "opens immediately with loading: true");
        assert!(picker.models.is_empty());
    }

    #[tokio::test]
    async fn ctrl_o_ignored_while_streaming() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Chat;
        app.chat.streaming = true;
        app.handle_key(KeyEvent::new(KeyCode::Char('o'), KeyModifiers::CONTROL))
            .await
            .unwrap();
        assert!(app.model_picker.is_none());
    }

    /// `↑/↓` move the selection (clamped); `Enter` applies the highlighted
    /// model — `chat.model` gets the plain model id (NOT `provider/model`),
    /// matching what `ChatRequest`/`ExecuteRequest`/`PatchSessionRequest.model`
    /// resolve on the server.
    #[tokio::test]
    async fn model_picker_navigation_and_enter_applies() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.model_picker = Some(ModelPicker {
            models: vec![
                catalog_model("openai", "gpt-4.1", "GPT-4.1", "OpenAI"),
                catalog_model(
                    "anthropic",
                    "claude-sonnet-5",
                    "Claude Sonnet 5",
                    "Anthropic",
                ),
            ],
            selected: 0,
            loading: false,
        });

        app.handle_model_picker_key(key(KeyCode::Down))
            .await
            .unwrap();
        assert_eq!(app.model_picker.as_ref().unwrap().selected, 1);
        app.handle_model_picker_key(key(KeyCode::Down))
            .await
            .unwrap();
        assert_eq!(
            app.model_picker.as_ref().unwrap().selected,
            1,
            "clamped at the last index"
        );
        app.handle_model_picker_key(key(KeyCode::Up)).await.unwrap();
        assert_eq!(app.model_picker.as_ref().unwrap().selected, 0);

        app.handle_model_picker_key(key(KeyCode::Down))
            .await
            .unwrap();
        app.handle_model_picker_key(key(KeyCode::Enter))
            .await
            .unwrap();

        assert!(app.model_picker.is_none(), "Enter closes the picker");
        assert_eq!(
            app.chat.model, "claude-sonnet-5",
            "chat.model gets the plain model id, not provider/model"
        );
        assert_eq!(app.status_message, "Model: Claude Sonnet 5");
    }

    /// `Enter` while the catalog is still loading (empty list) is a no-op —
    /// there's nothing to apply, and the picker stays open.
    #[tokio::test]
    async fn model_picker_enter_while_loading_is_noop() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.model_picker = Some(ModelPicker {
            models: vec![],
            selected: 0,
            loading: true,
        });
        app.handle_model_picker_key(key(KeyCode::Enter))
            .await
            .unwrap();
        assert!(
            app.model_picker.is_some(),
            "no models to apply yet — picker stays open"
        );
    }

    /// `Esc` closes the picker without touching `chat.model`.
    #[tokio::test]
    async fn model_picker_esc_leaves_model_unchanged() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.model = "old-model".to_string();
        app.model_picker = Some(ModelPicker {
            models: vec![catalog_model("openai", "gpt-4.1", "GPT-4.1", "OpenAI")],
            selected: 0,
            loading: false,
        });

        app.handle_model_picker_key(key(KeyCode::Esc))
            .await
            .unwrap();

        assert!(app.model_picker.is_none());
        assert_eq!(app.chat.model, "old-model", "Esc must not change the model");
    }

    /// `CatalogLoaded(Err(...))` notifies and closes the picker instead of
    /// leaving it stuck on "Loading models...".
    #[tokio::test]
    async fn catalog_loaded_err_notifies_and_closes_picker() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.model_picker = Some(ModelPicker {
            models: vec![],
            selected: 0,
            loading: true,
        });

        app.handle_event(AppEvent::CatalogLoaded(Err(
            "connection refused".to_string()
        )))
        .await
        .unwrap();

        assert!(app.model_picker.is_none());
        let last = app.notifications.last().expect("notified");
        assert!(last.text.contains("connection refused"));
        assert_eq!(last.level, NoticeLevel::Error);
    }

    /// An empty catalog (no providers configured) notifies a warning instead
    /// of leaving an empty modal open.
    #[tokio::test]
    async fn catalog_loaded_empty_notifies_warn_and_closes_picker() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.model_picker = Some(ModelPicker {
            models: vec![],
            selected: 0,
            loading: true,
        });

        app.handle_event(AppEvent::CatalogLoaded(Ok(ProviderCatalog {
            models: vec![],
        })))
        .await
        .unwrap();

        assert!(app.model_picker.is_none());
        let last = app.notifications.last().expect("notified");
        assert_eq!(last.text, "No models in provider catalog");
        assert_eq!(last.level, NoticeLevel::Warn);
    }

    /// A catalog fetch that lands after the picker was already dismissed
    /// (`Esc`) must not reopen it.
    #[tokio::test]
    async fn catalog_loaded_dropped_if_picker_already_closed() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        assert!(app.model_picker.is_none());

        app.handle_event(AppEvent::CatalogLoaded(Ok(ProviderCatalog {
            models: vec![catalog_model("openai", "gpt-4.1", "GPT-4.1", "OpenAI")],
        })))
        .await
        .unwrap();

        assert!(
            app.model_picker.is_none(),
            "a stale fetch must not reopen a closed picker"
        );
    }

    #[test]
    fn model_picker_renders_model_row() {
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;

        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.model_picker = Some(ModelPicker {
            models: vec![catalog_model("openai", "gpt-4.1", "GPT-4.1", "OpenAI")],
            selected: 0,
            loading: false,
        });

        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| crate::ui::render(f, &app)).unwrap();

        let buf = terminal.backend().buffer().clone();
        let text: String = buf.content().iter().map(|c| c.symbol()).collect();
        assert!(text.contains("GPT-4.1"), "model display name missing");
        assert!(text.contains("OpenAI"), "provider display name missing");
    }
}

#[cfg(test)]
mod auto_serve_tests {
    use super::*;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use std::sync::Mutex;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::empty())
    }

    // ── is_loopback_url ──

    #[test]
    fn is_loopback_url_table() {
        let cases: &[(&str, bool)] = &[
            ("http://127.0.0.1:9562", true),
            ("http://localhost:9562", true),
            ("http://[::1]:9562", true),
            ("https://127.0.0.1:9562", true),
            ("https://127.0.0.1", true),              // no port
            ("http://127.0.0.1/api/v1/health", true), // path after authority
            ("http://127.5.0.9:9562", true),          // any 127.x.x.x is loopback
            ("http://example.com:9562", false),       // remote host
            ("https://bamboo.example.com", false),    // remote, https, no port
            ("http://192.168.1.20:9562", false),      // LAN host, not loopback
            // Regression: a hostname merely *starting with* "127." is not an
            // IPv4 127.x.x.x literal and must not be treated as loopback.
            ("http://127.0.0.1.evil.example.com:9562", false),
            // Regression: an out-of-range octet is not a valid IPv4 literal
            // at all, even though the string starts with "127.".
            ("http://127.256.0.1:9562", false),
        ];
        for (url, expected) in cases {
            assert_eq!(is_loopback_url(url), *expected, "url={url}");
        }
    }

    // ── discover_bamboo_bin ──

    /// Real files under a fresh, uniquely-named temp dir (no tempfile crate —
    /// no new deps). Parallel-safe: each call gets its own subdirectory.
    fn unique_temp_dir(label: &str) -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "bamboo-tui-test-{}-{label}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn touch_bin(dir: &Path) -> PathBuf {
        let path = dir.join(bamboo_bin_name());
        std::fs::write(&path, b"").expect("write fake binary");
        path
    }

    #[test]
    fn discover_bamboo_bin_env_override_wins_even_if_missing() {
        let sibling_dir = unique_temp_dir("env-override-sibling");
        touch_bin(&sibling_dir); // a real sibling binary exists...
        let path_dir = unique_temp_dir("env-override-path");
        touch_bin(&path_dir); // ...and a real PATH binary exists too...
        let missing = unique_temp_dir("env-override-missing").join("bamboo-does-not-exist");

        // ...but the env override still wins, even though it doesn't exist.
        let result = discover_bamboo_bin(
            Some(missing.clone()),
            Some(&sibling_dir),
            Some(&path_dir.to_string_lossy()),
        );
        assert_eq!(result, Some(missing));
    }

    #[test]
    fn discover_bamboo_bin_sibling_beats_path() {
        let exe_dir = unique_temp_dir("sibling-wins-exe");
        let sibling = touch_bin(&exe_dir);
        let path_dir = unique_temp_dir("sibling-wins-path");
        touch_bin(&path_dir);

        let result = discover_bamboo_bin(None, Some(&exe_dir), Some(&path_dir.to_string_lossy()));
        assert_eq!(result, Some(sibling));
    }

    #[test]
    fn discover_bamboo_bin_falls_back_to_path() {
        let exe_dir = unique_temp_dir("path-fallback-exe"); // no sibling binary
        let path_dir = unique_temp_dir("path-fallback-path");
        let on_path = touch_bin(&path_dir);

        let result = discover_bamboo_bin(None, Some(&exe_dir), Some(&path_dir.to_string_lossy()));
        assert_eq!(result, Some(on_path));
    }

    #[test]
    fn discover_bamboo_bin_none_when_nothing_found() {
        let exe_dir = unique_temp_dir("none-exe"); // empty
        let path_dir = unique_temp_dir("none-path"); // empty

        let result = discover_bamboo_bin(None, Some(&exe_dir), Some(&path_dir.to_string_lossy()));
        assert_eq!(result, None);
        // No exe_dir/PATH at all either.
        assert_eq!(discover_bamboo_bin(None, None, None), None);
    }

    // ── serve_offer modal key routing ──

    fn app_with_offer() -> App {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.serve_offer = Some(ServeOffer {
            url: "http://127.0.0.1:9562".to_string(),
        });
        app
    }

    #[test]
    fn serve_offer_n_dismisses_without_spawning() {
        let mut app = app_with_offer();
        app.handle_serve_offer_key(key(KeyCode::Char('n')));
        assert!(app.serve_offer.is_none());
        assert!(app.spawned_server.is_none());
        assert!(app.status_message.contains("--auto-serve"));
    }

    #[test]
    fn serve_offer_esc_dismisses_without_spawning() {
        let mut app = app_with_offer();
        app.handle_serve_offer_key(key(KeyCode::Esc));
        assert!(app.serve_offer.is_none());
        assert!(app.spawned_server.is_none());
    }

    /// The offer must swallow every other key too (e.g. digits, which
    /// elsewhere switch tabs) — verified through the full `handle_key` path
    /// so the modal-precedence routing itself (not just the leaf handler) is
    /// exercised.
    #[tokio::test]
    async fn serve_offer_captures_digits_instead_of_switching_tabs() {
        let mut app = app_with_offer();
        let starting_tab = app.tab;

        app.handle_key(key(KeyCode::Char('3'))).await.unwrap();

        assert_eq!(app.tab, starting_tab);
        assert!(app.serve_offer.is_some());
    }

    /// Single crate-wide lock serializing tests that mutate the
    /// process-global `BAMBOO_BIN` env var (the repo's convention for
    /// unavoidable env-manipulating tests — see e.g.
    /// `bamboo-config/src/paths.rs`'s `env_cache_lock_acquire`).
    static BAMBOO_BIN_ENV_LOCK: Mutex<()> = Mutex::new(());

    /// `y` starts the spawn flow; pointing `BAMBOO_BIN` at a path that
    /// doesn't exist exercises the resolve-succeeds-but-`Command::spawn`-
    /// fails path without touching any real process, and must notify an
    /// error and clear the offer rather than leaving it open or panicking.
    #[tokio::test]
    async fn serve_offer_y_with_broken_bamboo_bin_notifies_error_and_clears_offer() {
        let _guard = BAMBOO_BIN_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let original = std::env::var_os("BAMBOO_BIN");
        let missing = unique_temp_dir("broken-bin").join("bamboo-does-not-exist");
        std::env::set_var("BAMBOO_BIN", &missing);

        let mut app = app_with_offer();
        app.handle_serve_offer_key(key(KeyCode::Char('y')));

        match original {
            Some(v) => std::env::set_var("BAMBOO_BIN", v),
            None => std::env::remove_var("BAMBOO_BIN"),
        }

        assert!(app.serve_offer.is_none());
        assert!(app.spawned_server.is_none());
        let last = app.notifications.last().expect("notified");
        assert_eq!(last.level, NoticeLevel::Error);
        assert!(last.text.contains("Failed to spawn"));
    }

    // ── AppEvent::LocalServerReady ──

    #[tokio::test]
    async fn local_server_ready_ok_connects_and_resumes_pending_session() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        // Simulates the `--session-id`-at-startup-while-disconnected case:
        // `resume_session` already ran once (in `run`) and failed, leaving
        // `session_id` set but `messages` empty.
        app.chat.session_id = Some("sess-1".to_string());
        assert!(app.chat.messages.is_empty());
        let (tx, _rx) = mpsc::unbounded_channel();
        app.event_tx = Some(tx);

        app.handle_event(AppEvent::LocalServerReady(Ok(4242)))
            .await
            .unwrap();

        assert!(app.connected);
        // `resume_session` sets this status immediately (its fetch itself is
        // off the event loop) — proof a resume was actually initiated.
        assert_eq!(app.status_message, "Resuming session...");
        assert!(app.notifications.iter().any(|n| n.text.contains("4242")));
    }

    #[tokio::test]
    async fn local_server_ready_ok_does_not_resume_when_messages_already_present() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.session_id = Some("sess-1".to_string());
        app.chat.messages.push(ChatMessage {
            role: MessageRole::User,
            content: "hi".to_string(),
            tool_calls: Vec::new(),
            reasoning: None,
        });
        let (tx, _rx) = mpsc::unbounded_channel();
        app.event_tx = Some(tx);

        app.handle_event(AppEvent::LocalServerReady(Ok(1)))
            .await
            .unwrap();

        assert!(app.connected);
        assert_ne!(app.status_message, "Resuming session...");
    }

    #[tokio::test]
    async fn local_server_ready_err_notifies_and_stays_disconnected() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        let (tx, _rx) = mpsc::unbounded_channel();
        app.event_tx = Some(tx);

        app.handle_event(AppEvent::LocalServerReady(Err("boom".to_string())))
            .await
            .unwrap();

        assert!(!app.connected);
        assert!(app.spawned_server.is_none());
        let last = app.notifications.last().expect("notified");
        assert_eq!(last.level, NoticeLevel::Error);
        assert!(last.text.contains("boom"));
    }

    /// The offer modal renders through the normal `ui::render` overlay chain
    /// without panicking, and shows the URL it's offering to serve.
    #[test]
    fn serve_offer_renders_without_panicking() {
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;

        let app = app_with_offer();
        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| crate::ui::render(f, &app)).unwrap();

        let buf = terminal.backend().buffer().clone();
        let text: String = buf.content().iter().map(|c| c.symbol()).collect();
        assert!(text.contains("127.0.0.1:9562"), "offer URL missing");
        assert!(text.contains("Start a local"), "offer prompt missing");
    }
}
