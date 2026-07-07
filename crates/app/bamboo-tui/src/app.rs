use anyhow::Result;
use chrono::{DateTime, Utc};
use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyModifiers};
use futures::StreamExt;
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use tokio::sync::mpsc;
use tui_textarea::TextArea;

use crate::api::sse::SseStream;
use crate::api::types::*;
use crate::api::BambooClient;
use crate::event::AppEvent;
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
    pub timestamp: DateTime<Utc>,
}

pub struct ChatState {
    pub session_id: Option<String>,
    pub messages: Vec<ChatMessage>,
    pub textarea: TextArea<'static>,
    pub scroll_offset: u16,
    pub auto_scroll: bool,
    pub content_lines: u16,
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
        textarea.set_placeholder_text("Type a message... (Enter to send)");
        Self {
            session_id: None,
            messages: Vec::new(),
            textarea,
            scroll_offset: 0,
            auto_scroll: true,
            content_lines: 0,
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

// ── Per-tab states ──

pub struct SessionsState {
    pub sessions: Vec<SessionSummary>,
    pub selected: usize,
    pub loading: bool,
    pub error: Option<String>,
}

impl SessionsState {
    pub fn new() -> Self {
        Self {
            sessions: Vec::new(),
            selected: 0,
            loading: false,
            error: None,
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
}

impl ConfigState {
    pub fn new() -> Self {
        Self {
            config: None,
            loading: false,
            error: None,
            scroll_offset: 0,
        }
    }
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
    /// The agent's pending question (permission gate / clarification). When
    /// `Some`, a modal captures the answer and keystrokes route to it.
    pub pending_question: Option<ActiveQuestion>,
    /// In-progress new-schedule form (Schedules tab). When `Some`, a modal
    /// captures the fields.
    pub schedule_form: Option<ScheduleForm>,
    /// Sender into the main event loop, used to post results of background API
    /// calls (so those calls never block the UI thread). Set in [`run`].
    event_tx: Option<mpsc::UnboundedSender<AppEvent>>,
    sse_tx: Option<mpsc::UnboundedSender<AgentEvent>>,
    sse_rx: Option<mpsc::UnboundedReceiver<AgentEvent>>,
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
            pending_question: None,
            schedule_form: None,
            event_tx: None,
            sse_tx: None,
            sse_rx: None,
        }
    }

    pub async fn run(
        &mut self,
        terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    ) -> Result<()> {
        self.connected = self.client.health().await.unwrap_or(false);
        if self.connected {
            self.status_message = "Connected".to_string();
        } else {
            self.status_message = "Cannot connect to server".to_string();
        }

        let (event_tx, mut event_rx) = mpsc::unbounded_channel::<AppEvent>();
        // Keep a sender so background API tasks can post their results back.
        self.event_tx = Some(event_tx.clone());
        // Kick off the initial tab's data load without blocking startup.
        self.load_tab_data();

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
                    Ok(Event::Resize(w, h)) if tx.send(AppEvent::Resize(w, h)).is_err() => {
                        break;
                    }
                    _ => {}
                }
            }
        });

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
                            self.status_message = format!("SSE error: {}", e);
                        }
                    }
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
                self.status_message = format!("SSE error: {}", e);
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

        match event {
            AppEvent::Key(key) => self.handle_key(key).await?,
            AppEvent::SseEvent(agent_event) => self.handle_sse_event(agent_event)?,
            AppEvent::ApiError(msg) => {
                self.status_message = format!("Error: {}", msg);
            }
            AppEvent::Mouse(mouse) => self.handle_mouse(mouse),
            AppEvent::SessionsLoaded(r) => {
                self.sessions.loading = false;
                match r {
                    Ok(s) => {
                        self.sessions.sessions = s;
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
            AppEvent::ActionDone { status, reload_tab } => {
                self.status_message = status;
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
                    self.status_message = format!("Error: {e}");
                }
            },
            _ => {}
        }
        Ok(())
    }

    /// Mouse wheel scrolls the active scrollable view (chat transcript / config).
    fn handle_mouse(&mut self, mouse: crossterm::event::MouseEvent) {
        use crossterm::event::MouseEventKind;
        let delta = match mouse.kind {
            MouseEventKind::ScrollUp => -3i32,
            MouseEventKind::ScrollDown => 3i32,
            _ => return,
        };
        match self.tab {
            Tab::Chat => {
                self.chat.auto_scroll = false;
                self.chat.scroll_offset =
                    self.chat.scroll_offset.saturating_add_signed(delta as i16);
            }
            Tab::Config => {
                self.config.scroll_offset = self
                    .config
                    .scroll_offset
                    .saturating_add_signed(delta as i16);
            }
            _ => {}
        }
    }

    async fn handle_key(&mut self, key: KeyEvent) -> Result<()> {
        match (key.modifiers, key.code) {
            (KeyModifiers::CONTROL, KeyCode::Char('c')) => {
                if self.chat.streaming {
                    self.stop_streaming().await?;
                    return Ok(());
                }
                self.running = false;
                return Ok(());
            }
            (KeyModifiers::CONTROL, KeyCode::Char('?')) => {
                self.help_visible = true;
                return Ok(());
            }
            _ => {}
        }

        // A pending agent question captures all input (Ctrl+C above still
        // stops the run) until it is answered or dismissed.
        if self.pending_question.is_some() {
            return self.handle_question_key(key).await;
        }

        // The schedule-authoring modal likewise captures all input: Tab moves
        // between fields and digits belong in cron expressions, so it must run
        // before the global Tab/1-6 tab-switching below (which would otherwise
        // swallow those keys and never reach the form).
        if self.schedule_form.is_some() {
            self.handle_schedule_form_key(key);
            return Ok(());
        }

        if let KeyCode::Char(c) = key.code {
            if let Some(digit) = c.to_digit(10) {
                if (1..=6).contains(&digit)
                    && !key.modifiers.contains(KeyModifiers::SHIFT)
                    && (!self.chat.streaming || self.tab != Tab::Chat)
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
            QAction::Submit(answer) => self.submit_answer(answer).await?,
            QAction::Dismiss => {
                self.pending_question = None;
                self.status_message =
                    "Question dismissed (still pending on the server — Ctrl+C stops the run)"
                        .to_string();
            }
            QAction::None => {}
        }
        Ok(())
    }

    /// Submit an answer to the agent's pending question and resume the run.
    async fn submit_answer(&mut self, answer: String) -> Result<()> {
        let Some(session_id) = self.chat.session_id.clone() else {
            self.status_message = "No active chat session to answer".to_string();
            self.pending_question = None;
            return Ok(());
        };
        match self.client.respond(&session_id, &answer).await {
            Ok(status) => {
                self.pending_question = None;
                // Only keep the spinner on if a run is actually running: the
                // server returns 200 even when it did NOT resume (e.g. the
                // session already `completed`), so a blind `streaming = true`
                // would spin forever with no events behind it.
                if matches!(status.as_str(), "started" | "already_running") {
                    self.status_message = format!("Answered: {answer} — resuming");
                    self.chat.streaming = true;
                } else {
                    self.status_message = format!("Answered: {answer} ({status})");
                    self.finalize_streaming();
                }
            }
            Err(e) => {
                // Keep the modal open so the operator can pick a valid option.
                self.status_message = format!("Answer rejected: {e}");
            }
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
                tokio::spawn(async move {
                    let r = client.list_sessions().await.map_err(|e| e.to_string());
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
                    self.stop_streaming().await?;
                }
                KeyCode::Char('x') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    self.chat.expand_tools = !self.chat.expand_tools;
                }
                KeyCode::Char('j') | KeyCode::Down => {
                    self.chat.auto_scroll = false;
                    self.chat.scroll_offset = self.chat.scroll_offset.saturating_add(3);
                }
                KeyCode::Char('k') | KeyCode::Up => {
                    self.chat.auto_scroll = false;
                    self.chat.scroll_offset = self.chat.scroll_offset.saturating_sub(3);
                }
                KeyCode::Char('G') => {
                    self.chat.auto_scroll = true;
                }
                _ => {}
            }
            return Ok(());
        }

        match key.code {
            KeyCode::Enter => {
                let input = self.chat.textarea.lines().join("\n");
                let input = input.trim().to_string();
                if input.is_empty() {
                    return Ok(());
                }
                self.chat.textarea = TextArea::default();
                self.chat
                    .textarea
                    .set_placeholder_text("Type a message... (Enter to send)");
                self.send_message(input);
            }
            KeyCode::Char('j') | KeyCode::Down => {
                self.chat.auto_scroll = false;
                self.chat.scroll_offset = self.chat.scroll_offset.saturating_add(3);
            }
            KeyCode::Char('k') | KeyCode::Up => {
                self.chat.auto_scroll = false;
                self.chat.scroll_offset = self.chat.scroll_offset.saturating_sub(3);
            }
            KeyCode::Char('G') => {
                self.chat.auto_scroll = true;
            }
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
            timestamp: Utc::now(),
        });
        self.chat.auto_scroll = true;
        self.chat.streaming = true;
        self.chat.current_response.clear();
        self.chat.current_tool_calls.clear();
        self.chat.current_reasoning.clear();
        self.status_message = "Sending...".to_string();

        let client = self.client.clone();
        let existing_session = self.chat.session_id.clone();
        tokio::spawn(async move {
            let req = ChatRequest {
                message,
                session_id: existing_session,
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

    /// After `chat` returns a session id, open the SSE stream (before execute, so
    /// no early event is missed) and spawn the agent run.
    fn start_stream_and_execute(&mut self, session_id: String) {
        let (sse_tx, sse_rx) = mpsc::unbounded_channel();
        self.sse_tx = Some(sse_tx.clone());
        self.sse_rx = Some(sse_rx);
        let base_url = self.client.base_url.clone();
        if let Err(e) = SseStream::start(&base_url, &session_id, sse_tx) {
            self.status_message = format!("SSE start failed: {e}");
            return;
        }
        let client = self.client.clone();
        let model = self.chat.model.clone();
        tokio::spawn(async move {
            let model = if model.is_empty() { None } else { Some(model) };
            let _ = client.execute(&session_id, model.as_deref()).await;
        });
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
                tool_name,
                arguments,
                ..
            } => {
                self.chat.current_tool_calls.push(ToolCallDisplay {
                    tool_name,
                    arguments: serde_json::to_string(&arguments).unwrap_or_default(),
                    result: None,
                    error: None,
                    phase: "running".to_string(),
                });
            }
            AgentEvent::ToolComplete { result, .. } => {
                if let Some(tc) = self.chat.current_tool_calls.last_mut() {
                    tc.result = Some(result.result);
                    tc.phase = "complete".to_string();
                }
            }
            AgentEvent::ToolError { error, .. } => {
                if let Some(tc) = self.chat.current_tool_calls.last_mut() {
                    tc.error = Some(error);
                    tc.phase = "error".to_string();
                }
            }
            AgentEvent::ToolLifecycle {
                tool_name,
                phase,
                summary,
                ..
            } => {
                if let Some(tc) = self.chat.current_tool_calls.iter_mut().rev().find(|t| {
                    t.tool_name == tool_name && t.phase != "complete" && t.phase != "error"
                }) {
                    tc.phase = phase;
                    if let Some(s) = summary {
                        tc.result = Some(s);
                    }
                }
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
                self.pending_question = Some(ActiveQuestion {
                    question,
                    options,
                    selected: 0,
                    custom,
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
                self.status_message = format!("Error: {}", message);
                self.finalize_streaming();
            }
            AgentEvent::ToolToken { content, .. } => {
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
                timestamp: Utc::now(),
            });
        }
        self.status_message = "Ready".to_string();
        self.sse_tx = None;
        self.sse_rx = None;
        self.chat.sub_agents.clear();
        // A run that ended (completed / cancelled / stopped) can no longer accept
        // an answer, so drop any open question modal to avoid answering a dead
        // session.
        self.pending_question = None;
    }

    async fn stop_streaming(&mut self) -> Result<()> {
        if let Some(sid) = &self.chat.session_id {
            self.client.stop(sid).await?;
        }
        self.finalize_streaming();
        self.status_message = "Stopped".to_string();
        Ok(())
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
                if let Some(session) = self.sessions.sessions.get(self.sessions.selected) {
                    self.chat.session_id = Some(session.id.clone());
                    if let Some(model) = &session.model {
                        self.chat.model = model.clone();
                    }
                    self.tab = Tab::Chat;
                }
            }
            KeyCode::Char('d') => {
                if let Some(session) = self.sessions.sessions.get(self.sessions.selected) {
                    let id = session.id.clone();
                    self.client.delete_session(&id).await?;
                    self.load_tab_data();
                }
            }
            KeyCode::Char('r') => {
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
                        let status = match res {
                            Ok(()) => if connected {
                                "Disconnected"
                            } else {
                                "Connected"
                            }
                            .to_string(),
                            Err(e) => format!("MCP action failed: {e}"),
                        };
                        let _ = tx.send(AppEvent::ActionDone {
                            status,
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
                if let Some(schedule) = self.schedules.schedules.get(self.schedules.selected) {
                    let id = schedule.id.clone();
                    self.client.delete_schedule(&id).await?;
                    self.load_tab_data();
                }
            }
            KeyCode::Char('r') => {
                if let Some(schedule) = self.schedules.schedules.get(self.schedules.selected) {
                    self.client.run_schedule_now(&schedule.id).await?;
                    self.status_message = "Schedule triggered".to_string();
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
                        task_message: Some(form.prompt.trim().to_string()),
                        auto_execute: true,
                    },
                };
                self.schedule_form = None;
                if let Some(tx) = self.event_tx.clone() {
                    let client = self.client.clone();
                    tokio::spawn(async move {
                        let status = match client.create_schedule(req).await {
                            Ok(_) => "Schedule created".to_string(),
                            Err(e) => format!("Create failed: {e}"),
                        };
                        let _ = tx.send(AppEvent::ActionDone {
                            status,
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
                if let Some(skill) = self.skills.skills.get(self.skills.selected) {
                    match self.client.get_skill(&skill.id).await {
                        Ok(detail) => self.skills.detail = Some(detail),
                        Err(e) => self.skills.error = Some(e.to_string()),
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn handle_config_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Down => {
                self.config.scroll_offset = self.config.scroll_offset.saturating_add(1);
            }
            KeyCode::Up => {
                self.config.scroll_offset = self.config.scroll_offset.saturating_sub(1);
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod question_tests {
    use super::*;
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

    #[tokio::test]
    async fn loaded_event_applies_to_state_and_clears_loading() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.sessions.loading = true;
        app.handle_event(AppEvent::SessionsLoaded(Ok(vec![SessionSummary {
            id: "s1".into(),
            title: None,
            model: None,
            created_at: None,
            updated_at: None,
            message_count: None,
            status: None,
        }])))
        .await
        .unwrap();
        assert!(!app.sessions.loading, "loading flag must clear");
        assert_eq!(app.sessions.sessions.len(), 1);
        assert!(app.sessions.error.is_none());

        // Error result surfaces on the tab and clears loading.
        app.sessions.loading = true;
        app.handle_event(AppEvent::SessionsLoaded(Err("boom".into())))
            .await
            .unwrap();
        assert!(!app.sessions.loading);
        assert_eq!(app.sessions.error.as_deref(), Some("boom"));
    }

    #[test]
    fn mouse_wheel_scrolls_chat() {
        use crossterm::event::{MouseEvent, MouseEventKind};
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Chat;
        app.chat.scroll_offset = 10;
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
}
