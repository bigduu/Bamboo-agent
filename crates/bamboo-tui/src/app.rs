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

#[derive(Clone, Copy, PartialEq, Eq)]
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

        // Spawn crossterm event reader.
        let tx = event_tx.clone();
        tokio::spawn(async move {
            let mut reader = EventStream::new();
            while let Some(event) = reader.next().await {
                match event {
                    Ok(Event::Key(key)) => {
                        if tx.send(AppEvent::Key(key)).is_err() {
                            break;
                        }
                    }
                    Ok(Event::Mouse(mouse)) => {
                        if tx.send(AppEvent::Mouse(mouse)).is_err() {
                            break;
                        }
                    }
                    Ok(Event::Resize(w, h)) => {
                        if tx.send(AppEvent::Resize(w, h)).is_err() {
                            break;
                        }
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
            _ => {}
        }
        Ok(())
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

        if let KeyCode::Char(c) = key.code {
            if let Some(digit) = c.to_digit(10) {
                if (1..=6).contains(&digit)
                    && !key.modifiers.contains(KeyModifiers::SHIFT)
                    && (!self.chat.streaming || self.tab != Tab::Chat)
                {
                    self.tab = Tab::from_index((digit - 1) as usize).unwrap_or(self.tab);
                    self.load_tab_data().await?;
                    return Ok(());
                }
            }
        }

        match key.code {
            KeyCode::Tab => {
                self.tab = self.tab.next();
                self.load_tab_data().await?;
            }
            KeyCode::BackTab => {
                self.tab = self.tab.prev();
                self.load_tab_data().await?;
            }
            _ => {
                self.handle_tab_key(key).await?;
            }
        }
        Ok(())
    }

    async fn load_tab_data(&mut self) -> Result<()> {
        match self.tab {
            Tab::Chat => {}
            Tab::Sessions => {
                self.sessions.loading = true;
                match self.client.list_sessions().await {
                    Ok(s) => {
                        self.sessions.sessions = s;
                        self.sessions.error = None;
                    }
                    Err(e) => self.sessions.error = Some(e.to_string()),
                }
                self.sessions.loading = false;
            }
            Tab::Mcp => {
                self.mcp.loading = true;
                match self.client.list_mcp_servers().await {
                    Ok(s) => {
                        self.mcp.servers = s;
                        self.mcp.error = None;
                    }
                    Err(e) => self.mcp.error = Some(e.to_string()),
                }
                self.mcp.loading = false;
            }
            Tab::Schedules => {
                self.schedules.loading = true;
                match self.client.list_schedules().await {
                    Ok(s) => {
                        self.schedules.schedules = s;
                        self.schedules.error = None;
                    }
                    Err(e) => self.schedules.error = Some(e.to_string()),
                }
                self.schedules.loading = false;
            }
            Tab::Skills => {
                self.skills.loading = true;
                match self.client.list_skills().await {
                    Ok(s) => {
                        self.skills.skills = s;
                        self.skills.error = None;
                    }
                    Err(e) => self.skills.error = Some(e.to_string()),
                }
                self.skills.loading = false;
            }
            Tab::Config => {
                self.config.loading = true;
                match self.client.get_config().await {
                    Ok(c) => {
                        self.config.config = Some(c);
                        self.config.error = None;
                    }
                    Err(e) => self.config.error = Some(e.to_string()),
                }
                self.config.loading = false;
            }
        }
        Ok(())
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
                KeyCode::Char('s')
                    if key.modifiers.contains(KeyModifiers::CONTROL) =>
                {
                    self.stop_streaming().await?;
                }
                KeyCode::Char('j') | KeyCode::Down => {
                    self.chat.auto_scroll = false;
                    self.chat.scroll_offset = self.chat.scroll_offset.saturating_add(3);
                }
                KeyCode::Char('k') | KeyCode::Up => {
                    self.chat.auto_scroll = false;
                    self.chat.scroll_offset =
                        self.chat.scroll_offset.saturating_sub(3);
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
                self.send_message(input).await?;
            }
            KeyCode::Char('j') | KeyCode::Down => {
                self.chat.auto_scroll = false;
                self.chat.scroll_offset = self.chat.scroll_offset.saturating_add(3);
            }
            KeyCode::Char('k') | KeyCode::Up => {
                self.chat.auto_scroll = false;
                self.chat.scroll_offset =
                    self.chat.scroll_offset.saturating_sub(3);
            }
            KeyCode::Char('G') => {
                self.chat.auto_scroll = true;
            }
            _ => {
                self.chat.textarea.input(key);
            }
        }
        Ok(())
    }

    async fn send_message(&mut self, message: String) -> Result<()> {
        let model = if self.chat.model.is_empty() {
            "default".to_string()
        } else {
            self.chat.model.clone()
        };

        self.chat.messages.push(ChatMessage {
            role: MessageRole::User,
            content: message.clone(),
            tool_calls: Vec::new(),
            reasoning: None,
            timestamp: Utc::now(),
        });
        self.chat.auto_scroll = true;

        let req = ChatRequest {
            message,
            session_id: self.chat.session_id.clone(),
            model: model.clone(),
        };

        match self.client.chat(req).await {
            Ok(resp) => {
                self.chat.session_id = Some(resp.session_id.clone());
                self.chat.streaming = true;
                self.chat.current_response.clear();
                self.chat.current_tool_calls.clear();
                self.chat.current_reasoning.clear();
                self.status_message = "Streaming...".to_string();

                // Start SSE stream BEFORE execute so we don't miss early events.
                let (sse_tx, sse_rx) = mpsc::unbounded_channel();
                self.sse_tx = Some(sse_tx.clone());
                self.sse_rx = Some(sse_rx);
                let base_url = self.client.base_url.clone();
                SseStream::start(&base_url, &resp.session_id, sse_tx)?;

                // Now start agent execution.
                let _ = self
                    .client
                    .execute(&resp.session_id, Some(&model))
                    .await;
            }
            Err(e) => {
                self.status_message = format!("Error: {}", e);
            }
        }

        Ok(())
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
                if let Some(tc) = self
                    .chat
                    .current_tool_calls
                    .iter_mut()
                    .rev()
                    .find(|t| {
                        t.tool_name == tool_name
                            && t.phase != "complete"
                            && t.phase != "error"
                    })
                {
                    tc.phase = phase;
                    if let Some(s) = summary {
                        tc.result = Some(s);
                    }
                }
            }
            AgentEvent::NeedClarification { question, options } => {
                self.status_message = format!("Question: {}", question);
                if let Some(opts) = options {
                    self.status_message
                        .push_str(&format!(" [{}]", opts.join(", ")));
                }
            }
            AgentEvent::Complete { usage } => {
                self.finalize_streaming();
                self.chat.token_usage = Some(usage);
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
        }
        Ok(())
    }

    fn finalize_streaming(&mut self) {
        self.chat.streaming = false;
        if !self.chat.current_response.is_empty()
            || !self.chat.current_tool_calls.is_empty()
        {
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
            KeyCode::Down => {
                if !self.sessions.sessions.is_empty() {
                    self.sessions.selected =
                        (self.sessions.selected + 1).min(self.sessions.sessions.len() - 1);
                }
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
                    self.load_tab_data().await?;
                }
            }
            KeyCode::Char('r') => {
                self.load_tab_data().await?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn handle_mcp_key(&mut self, key: KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Down => {
                if !self.mcp.servers.is_empty() {
                    self.mcp.selected =
                        (self.mcp.selected + 1).min(self.mcp.servers.len() - 1);
                }
            }
            KeyCode::Up => {
                self.mcp.selected = self.mcp.selected.saturating_sub(1);
            }
            KeyCode::Enter => {
                if let Some(server) = self.mcp.servers.get(self.mcp.selected) {
                    let id = server.id.clone();
                    let connected = server.connected.unwrap_or(false);
                    if connected {
                        self.client.disconnect_mcp(&id).await?;
                    } else {
                        self.client.connect_mcp(&id).await?;
                    }
                    self.load_tab_data().await?;
                }
            }
            KeyCode::Char('t') => {
                if let Some(server) = self.mcp.servers.get(self.mcp.selected) {
                    match self.client.get_mcp_tools(&server.id).await {
                        Ok(tools) => self.mcp.tools = tools,
                        Err(e) => self.mcp.error = Some(e.to_string()),
                    }
                }
            }
            KeyCode::Char('r') => {
                self.load_tab_data().await?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn handle_schedules_key(&mut self, key: KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Down => {
                if !self.schedules.schedules.is_empty() {
                    self.schedules.selected =
                        (self.schedules.selected + 1).min(self.schedules.schedules.len() - 1);
                }
            }
            KeyCode::Up => {
                self.schedules.selected = self.schedules.selected.saturating_sub(1);
            }
            KeyCode::Char('d') => {
                if let Some(schedule) = self.schedules.schedules.get(self.schedules.selected) {
                    let id = schedule.id.clone();
                    self.client.delete_schedule(&id).await?;
                    self.load_tab_data().await?;
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

    async fn handle_skills_key(&mut self, key: KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Down => {
                if !self.skills.skills.is_empty() {
                    self.skills.selected =
                        (self.skills.selected + 1).min(self.skills.skills.len() - 1);
                }
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
