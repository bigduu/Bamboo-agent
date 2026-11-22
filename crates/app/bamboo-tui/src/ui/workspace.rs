//! Native Shell owns the workspace layout and controls. Bamboo keeps the
//! application state, keyboard routing, and terminal-native transcript widgets.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use crossterm::event::{KeyModifiers, MouseButton as TerminalButton, MouseEvent, MouseEventKind};
use nsh_core::prelude::*;
use nsh_platform_tui::{
    AppOptions, AppView, ColorDepth, Handled, Input, Modifiers, MouseButton, MouseInput, MouseKind,
    NativeComponent, Root,
};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders, Paragraph};
use ratatui::Frame;
use tokio::sync::{mpsc, watch, Notify};

use crate::app::{ActiveQuestionKind, App, ConfigView, MessageRole, Tab};
use crate::event::AppEvent;
use crate::file_reference::file_name;
use crate::keymap::{ActionContext, ActionId};
use crate::text::clip_cells;
use crate::theme::{self, ThemePalette};

const BODY: &str = "bamboo.workspace.body";
const STATUS: &str = "bamboo.workspace.status";
const CELL_WIDTH: f32 = 8.0;
const CELL_HEIGHT: f32 = 16.0;
const READING_WIDTH: f32 = 108.0 * CELL_WIDTH;

pub(crate) struct WorkspaceAction {
    pub(crate) tab: Tab,
    pub(crate) context_id: u64,
    pub(crate) action: ActionId,
}

#[derive(Clone, PartialEq, Eq)]
struct WorkspaceState {
    tab: Tab,
    context_id: u64,
    theme: ThemePalette,
    width: u16,
    title: String,
    has_subagents: bool,
    has_parent: bool,
    model: String,
    effort: String,
    task_progress: Option<String>,
    streaming: bool,
    has_draft: bool,
    command_hint: String,
    session_hint: String,
    hints: bool,
    parked_question: Option<&'static str>,
}

impl WorkspaceState {
    fn read(app: &App, width: u16) -> Self {
        let has_parent = app.parent_session_id().is_some();
        let has_subagents = app.has_visible_subagents();
        let title_width = match (width, has_parent && has_subagents) {
            (..80, true) => 9,
            (..80, false) => 18,
            (..96, true) => 22,
            _ => 30,
        };
        let title = if app.tab == Tab::Chat {
            let session = app.chat.session_id.as_deref();
            let session_title = session.and_then(|id| {
                app.sessions
                    .sessions
                    .iter()
                    .find(|summary| summary.id == id)
                    .map(|summary| summary.title.as_str())
                    .filter(|title| !title.is_empty())
            });
            let prompt = app
                .chat
                .messages
                .iter()
                .find(|message| matches!(message.role, MessageRole::User))
                .and_then(|message| message.content.lines().next());
            let label = session_title
                .or(prompt)
                .or(session)
                .unwrap_or("New session");
            let name = file_name(label);
            short_label(name.as_deref().unwrap_or(label), title_width)
        } else if app.tab == Tab::Config && app.config.view == ConfigView::Permissions {
            "Permission policy".to_string()
        } else {
            app.tab.title().to_string()
        };
        let progress = &app.chat.task_progress.progress;
        let parked_question = app.dismissed_question.as_ref().map(|question| {
            if matches!(question.kind, ActiveQuestionKind::Clarification) {
                "Question!"
            } else {
                "Approval!"
            }
        });
        let model_width = match width {
            ..80 if parked_question.is_some() && app.chat.streaming => 9,
            ..80 => 13,
            _ => 20,
        };
        Self {
            tab: app.tab,
            context_id: app.workspace_context_id(),
            theme: app.theme,
            width,
            title,
            has_subagents,
            has_parent,
            model: short_label(
                if app.chat.model.is_empty() {
                    "Model"
                } else {
                    &app.chat.model
                },
                model_width,
            ),
            effort: app
                .chat
                .reasoning_effort
                .map_or("default", |effort| effort.as_str())
                .to_string(),
            task_progress: (progress.total > 0)
                .then(|| format!("{}/{} tasks", progress.completed, progress.total)),
            streaming: app.chat.streaming,
            has_draft: app.chat.has_composer_draft(),
            command_hint: app.primary_key_hint(ActionContext::Global, ActionId::OpenCommandPalette),
            session_hint: app.primary_key_hint(ActionContext::Global, ActionId::OpenSessionPicker),
            hints: app.workspace_hints,
            parked_question,
        }
    }
}

fn short_label(value: &str, width: usize) -> String {
    let clean: String = value
        .chars()
        .filter(|character| !character.is_control())
        .take(width.saturating_mul(4))
        .collect();
    clip_cells(clean.trim(), width)
}

struct WorkspaceRoot;

struct WorkspaceChannels {
    updates: watch::Receiver<WorkspaceState>,
    events: mpsc::UnboundedSender<AppEvent>,
}

#[derive(Clone)]
struct WorkspaceProps(Option<Arc<WorkspaceChannels>>);

impl PartialEq for WorkspaceProps {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            (None, None) => true,
            _ => false,
        }
    }
}

impl Props for WorkspaceProps {
    fn initial() -> Self {
        Self(None)
    }

    fn spec(_: &mut nsh_core::element::TsTypes) -> Vec<nsh_core::element::PropSpec> {
        Vec::new()
    }

    fn set(
        &mut self,
        name: &str,
        value: Option<nsh_core::element::AnyValue>,
    ) -> std::result::Result<bool, nsh_core::element::PropError> {
        <() as Props>::set(&mut (), name, value)
    }

    fn set_slot(&mut self, _: &str, _: Option<nsh_core::element::Slot>) -> bool {
        false
    }

    fn set_children(&mut self, _: Children) -> bool {
        false
    }
}

impl Component for WorkspaceRoot {
    type Props = WorkspaceProps;
    type Event = NoEvent;

    fn view(props: Self::Props, _: Scope<Self::Event>) -> impl IntoElement {
        let Some(channels) = props.0 else {
            return view().into_element();
        };
        let mut updates = channels.updates.clone();
        let state = signal(updates.borrow_and_update().clone());
        let subscriber = spawn(async move {
            while updates.changed().await.is_ok() {
                state.set(updates.borrow_and_update().clone());
            }
        });
        cleanup(move || drop(subscriber));
        view()
            .flex_1()
            .child(dynamic(move || render_shell(state.get(), &channels.events)))
            .into_element()
    }
}

fn render_shell(state: WorkspaceState, events: &mpsc::UnboundedSender<AppEvent>) -> AnyElement {
    let action = |label: String, action: ActionId| {
        let events = events.clone();
        let tab = state.tab;
        let context_id = state.context_id;
        view()
            .h(px(CELL_HEIGHT))
            .px(px(CELL_WIDTH))
            .rounded(px(CELL_WIDTH))
            .role(AccessibilityRole::Button)
            .on_press(move |_: &()| {
                let _ = events.send(AppEvent::WorkspaceAction(WorkspaceAction {
                    action,
                    tab,
                    context_id,
                }));
            })
            .child(text(label).number_of_lines(1))
    };
    let chat = state.tab == Tab::Chat;
    let roomy = state.width >= 80;
    let wide = state.width >= 100;
    let sessions = if state.hints && state.width >= 96 {
        format!("Switch session ▾ {}", short_label(&state.session_hint, 10))
    } else if roomy {
        "Switch session ▾".into()
    } else {
        "Switch session".into()
    };
    let mut navigation = view()
        .flex_shrink_0()
        .flex_row()
        .items_center()
        .gap(px(CELL_WIDTH));
    if state.has_parent {
        navigation = navigation.child(
            action("← Parent".into(), ActionId::OpenParentSession)
                .flex_shrink_0()
                .label("Back to parent session"),
        );
    }
    if state.has_subagents {
        navigation = navigation
            .child(action("Sub-agents ›".into(), ActionId::OpenSubagentTree).flex_shrink_0());
    }
    navigation = navigation.child(action(sessions, ActionId::OpenSessionPicker).flex_shrink_0());
    let header = view()
        .w_full()
        .max_w(px(READING_WIDTH))
        .h(px(2.0 * CELL_HEIGHT))
        .flex_shrink_0()
        .px(px(CELL_WIDTH))
        .flex_row()
        .items_center()
        .justify_between()
        .child(if chat {
            view()
                .flex_row()
                .min_w(px(0.0))
                .items_center()
                .gap(px(CELL_WIDTH))
                .child(text("Bamboo").font_weight(FontWeight::BOLD))
                .child(text(state.title.clone()).number_of_lines(1))
                .into_element()
        } else {
            text("Bamboo").font_weight(FontWeight::BOLD).into_element()
        })
        .child(if chat {
            navigation.into_element()
        } else {
            text(state.title.clone()).into_element()
        });

    let functions = if wide && state.hints && !state.streaming {
        format!("/ Functions {}", state.command_hint)
    } else {
        "/ Functions".into()
    };
    let mut toolbar = view()
        .w_full()
        .max_w(px(READING_WIDTH))
        .h(px(CELL_HEIGHT))
        .flex_shrink_0()
        .flex_row()
        .items_center()
        .child(action(functions, ActionId::OpenCommandPalette))
        .child(action("Settings".into(), ActionId::OpenTerminalSettings));
    if chat {
        if let Some(question) = state.parked_question {
            toolbar = toolbar.child(action(question.into(), ActionId::ReopenPendingQuestion));
        } else if roomy {
            toolbar = toolbar.child(action("Permissions".into(), ActionId::OpenPermissionPolicy));
        }
        toolbar = toolbar.child(view().flex_1());
        if let Some(tasks) = state.task_progress.as_ref().filter(|_| wide) {
            toolbar = toolbar.child(action(tasks.clone(), ActionId::OpenTaskPlan));
        }
        let effort = if wide {
            format!("Reasoning {}", state.effort)
        } else {
            state.effort.clone()
        };
        toolbar = toolbar
            .child(action(state.model.clone(), ActionId::OpenModelPicker))
            .child(action(effort, ActionId::OpenModelPicker));
        toolbar = if state.streaming {
            toolbar.child(action("Stop".into(), ActionId::StopRun))
        } else {
            let mut send = action("↑".into(), ActionId::SendMessage)
                .label("Send message")
                .disabled(!state.has_draft);
            if !state.has_draft {
                if let Some(color) = nsh_color(state.theme.color(theme::ColorRole::Inactive)) {
                    send = send.text_color(color);
                }
            }
            toolbar.child(send)
        };
        if let Some(rgb) = nsh_color(state.theme.surface()) {
            toolbar = toolbar.bg(rgb);
        }
    } else {
        toolbar = toolbar.child(action("Chat".into(), ActionId::OpenChatTab));
    }

    view()
        .flex_1()
        .flex_col()
        .items_center()
        .child(header)
        .child(
            nsh_core::element::native(BODY)
                .w_full()
                .max_w(px(READING_WIDTH))
                .min_h(px(CELL_HEIGHT))
                .flex_1(),
        )
        .child(toolbar)
        .child(
            nsh_core::element::native(STATUS)
                .w_full()
                .max_w(px(READING_WIDTH))
                .h(px(CELL_HEIGHT))
                .flex_shrink_0(),
        )
        .into_element()
}

fn nsh_color(color: Color) -> Option<Hsla> {
    match color {
        Color::Rgb(r, g, b) => Some(nsh_core::color::rgb(u32::from_be_bytes([0, r, g, b])).into()),
        _ => None,
    }
}

#[derive(Default)]
struct WorkspaceAreas {
    body: Rc<Cell<Rect>>,
    status: Rc<Cell<Rect>>,
}

struct AreaSlot(Rc<Cell<Rect>>);

impl NativeComponent for AreaSlot {
    fn render(&mut self, area: Rect, _buffer: &mut Buffer) {
        self.0.set(area);
    }

    fn set_props(&mut self, _: &[(Arc<str>, Option<nsh_core::JsonValue>)]) {}

    fn command(&mut self, _: &nsh_core::Command) -> bool {
        false
    }
}

pub(crate) struct Workspace {
    view: AppView,
    updates: watch::Sender<WorkspaceState>,
    areas: WorkspaceAreas,
    error: Rc<RefCell<Option<String>>>,
    wake: Arc<Notify>,
    events: mpsc::UnboundedSender<AppEvent>,
    palette: ThemePalette,
    focused: bool,
    pointer_started: bool,
}

impl Workspace {
    pub(crate) fn new(
        app: &App,
        events: mpsc::UnboundedSender<AppEvent>,
        wake: Arc<Notify>,
    ) -> Result<Self> {
        let areas = WorkspaceAreas::default();
        let error = Rc::new(RefCell::new(None));
        let (updates, receiver) = watch::channel(WorkspaceState::read(app, 80));
        let view = Self::create_view(app.theme, &areas, &error, receiver, &events, &wake)?;
        Ok(Self {
            view,
            updates,
            areas,
            error,
            wake,
            events,
            palette: app.theme,
            focused: true,
            pointer_started: false,
        })
    }

    fn create_view(
        palette: ThemePalette,
        areas: &WorkspaceAreas,
        error: &Rc<RefCell<Option<String>>>,
        updates: watch::Receiver<WorkspaceState>,
        events: &mpsc::UnboundedSender<AppEvent>,
        wake: &Arc<Notify>,
    ) -> Result<AppView> {
        let mut options = AppOptions::default();
        options.capabilities.colors = match palette {
            ThemePalette::TrueColor | ThemePalette::Light => ColorDepth::TrueColor,
            ThemePalette::System => ColorDepth::Ansi16,
            ThemePalette::NoColor => ColorDepth::Monochrome,
        };
        if let Color::Rgb(r, g, b) = palette.canvas() {
            options.capabilities.background_color = Some([r, g, b]);
        }
        if let Color::Rgb(r, g, b) = palette.text() {
            options.capabilities.foreground_color = Some([r, g, b]);
        }
        options.appearance = match palette {
            ThemePalette::Light => Appearance::Light,
            ThemePalette::TrueColor => Appearance::Dark,
            _ => Appearance::System,
        };
        options.cache = nsh_platform_tui::CacheOptions::off();
        let wake = wake.clone();
        options.wake = Some(Arc::new(move || wake.notify_one()));
        let mut view = AppView::new(options);
        for (name, slot) in [(BODY, &areas.body), (STATUS, &areas.status)] {
            let slot = slot.clone();
            view.register_component(name, Box::new(move || Box::new(AreaSlot(slot.clone()))))
                .map_err(|cause| anyhow!("Register Bamboo workspace area: {cause}"))?;
        }
        let error = error.clone();
        view.on_error(move |message| *error.borrow_mut() = Some(message.to_owned()));
        let channels = Arc::new(WorkspaceChannels {
            updates,
            events: events.clone(),
        });
        view.load(Root::new::<WorkspaceRoot>(move || {
            WorkspaceProps(Some(channels.clone()))
        }))?;
        Ok(view)
    }

    pub(crate) fn check_error(&self) -> Result<()> {
        match self.error.borrow_mut().take() {
            Some(error) => Err(anyhow!("Bamboo workspace: {error}")),
            None => Ok(()),
        }
    }

    pub(crate) fn render(&mut self, frame: &mut Frame, app: &App) {
        let screen = frame.area();
        if screen.width < super::MIN_TERMINAL_WIDTH || screen.height < super::MIN_TERMINAL_HEIGHT {
            self.set_focused(false);
            super::render(frame, app);
            return;
        }
        let next = WorkspaceState::read(app, screen.width);
        self.updates.send_if_modified(|current| {
            if *current == next {
                return false;
            }
            *current = next;
            true
        });
        if app.theme != self.palette {
            match Self::create_view(
                app.theme,
                &self.areas,
                &self.error,
                self.updates.subscribe(),
                &self.events,
                &self.wake,
            ) {
                Ok(view) => {
                    self.view = view;
                    self.palette = app.theme;
                    self.focused = true;
                    self.pointer_started = false;
                }
                Err(error) => {
                    *self.error.borrow_mut() = Some(error.to_string());
                    return;
                }
            }
        }
        self.set_focused(app.workspace_controls_enabled());
        self.areas.body.set(Rect::default());
        self.areas.status.set(Rect::default());
        frame.render_widget(Block::default().style(canvas_style(app.theme)), screen);
        self.view.render(frame, screen);
        let body = self.areas.body.get();
        let status = self.areas.status.get();
        if body.is_empty() || status.is_empty() {
            // NSH commits arrive on its engine thread. Draw a usable view
            // while the first layout, or a newly sized layout, is in flight.
            super::render(frame, app);
        } else {
            super::render_workspace(frame, app, body, status);
        }
    }

    fn set_focused(&mut self, focused: bool) {
        if self.focused != focused {
            self.focused = focused;
            self.pointer_started = false;
            self.view.set_focused(focused);
        }
    }

    pub(crate) fn handle_mouse(&mut self, event: MouseEvent) -> bool {
        if !self.focused {
            return false;
        }
        let position = ratatui::layout::Position::new(event.column, event.row);
        let chrome = !self.areas.body.get().contains(position)
            && !self.areas.status.get().contains(position);
        let kind = match event.kind {
            MouseEventKind::Down(TerminalButton::Left) if chrome => {
                self.pointer_started = true;
                MouseKind::Down(MouseButton::Left)
            }
            MouseEventKind::Up(TerminalButton::Left) if self.pointer_started => {
                self.pointer_started = false;
                MouseKind::Up(MouseButton::Left)
            }
            MouseEventKind::Drag(TerminalButton::Left) if self.pointer_started => MouseKind::Move,
            MouseEventKind::Moved => MouseKind::Move,
            _ => return false,
        };
        let mut input = MouseInput::default();
        input.kind = kind;
        input.column = event.column;
        input.row = event.row;
        input.modifiers = Modifiers {
            ctrl: event.modifiers.contains(KeyModifiers::CONTROL),
            alt: event.modifiers.contains(KeyModifiers::ALT),
            shift: event.modifiers.contains(KeyModifiers::SHIFT),
            cmd: event.modifiers.contains(KeyModifiers::SUPER),
        };
        matches!(self.view.handle(Input::Mouse(input)), Handled::Yes)
    }
}

pub(super) fn canvas_style(palette: ThemePalette) -> Style {
    Style::default().fg(palette.text()).bg(palette.canvas())
}

pub(super) fn surface_style(palette: ThemePalette) -> Style {
    Style::default().fg(palette.text()).bg(palette.surface())
}

/// The hitbox margins match the inset of the existing terminal panels.
pub(super) fn panel(frame: &mut Frame, area: Rect, title: &str, palette: ThemePalette) -> Rect {
    let mut block = Block::default().style(surface_style(palette));
    if palette.surface() == Color::Reset {
        block = block
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .title(title);
    }
    frame.render_widget(ratatui::widgets::Clear, area);
    frame.render_widget(block, area);
    if !title.is_empty() && palette.surface() != Color::Reset {
        frame.render_widget(
            Paragraph::new(Line::from(Span::styled(
                title,
                Style::default().add_modifier(Modifier::BOLD),
            )))
            .style(surface_style(palette)),
            Rect::new(area.x + 1, area.y, area.width.saturating_sub(2), 1),
        );
    }
    area.inner(ratatui::layout::Margin::new(1, 1))
}

#[derive(Clone, Copy)]
pub(crate) enum TerminalSetting {
    Appearance,
    ToolDetails,
    KeyHints,
    ServerConfig,
    Permissions,
}

const SETTINGS: [TerminalSetting; 5] = [
    TerminalSetting::Appearance,
    TerminalSetting::ToolDetails,
    TerminalSetting::KeyHints,
    TerminalSetting::ServerConfig,
    TerminalSetting::Permissions,
];

#[derive(Default)]
pub(crate) struct SettingsMenu {
    selected: usize,
    pressed: Option<usize>,
    rows: Cell<Rect>,
}

impl SettingsMenu {
    pub(crate) fn selected(&self) -> TerminalSetting {
        SETTINGS[self.selected]
    }

    pub(crate) fn move_selection(&mut self, delta: isize) {
        self.selected = self
            .selected
            .saturating_add_signed(delta)
            .min(SETTINGS.len() - 1);
    }

    pub(crate) fn handle_mouse(&mut self, event: MouseEvent) -> Option<TerminalSetting> {
        let area = self.rows.get();
        let index = (event.column >= area.x
            && event.column < area.right()
            && event.row >= area.y
            && event.row < area.bottom())
        .then(|| usize::from(event.row - area.y))
        .filter(|index| *index < SETTINGS.len());
        match event.kind {
            MouseEventKind::Down(TerminalButton::Left) => {
                self.pressed = index;
            }
            MouseEventKind::Up(TerminalButton::Left) => {
                let pressed = self.pressed.take();
                if let Some(index) = index.filter(|index| Some(*index) == pressed) {
                    self.selected = index;
                    return Some(self.selected());
                }
            }
            _ => self.pressed = None,
        }
        None
    }
}

pub(super) fn render_settings(frame: &mut Frame, app: &App, anchor: Rect) {
    let Some(settings) = &app.terminal_settings else {
        return;
    };
    let outer = popover_area(anchor, 57, 9, true);
    let inner = panel(frame, outer, " Settings ", app.theme);
    settings.rows.set(inner);
    let appearance = match app.theme {
        ThemePalette::TrueColor => "Dark",
        ThemePalette::Light => "Light",
        ThemePalette::System => "Terminal ANSI",
        ThemePalette::NoColor => "No color",
    };
    let rows = [
        ("Appearance", appearance),
        (
            "New tool details",
            if app.chat.expand_tools {
                "expanded"
            } else {
                "collapsed"
            },
        ),
        (
            "Toolbar hints",
            if app.workspace_hints {
                "shown"
            } else {
                "hidden"
            },
        ),
        ("Server configuration", "open ›"),
        ("Permission policy", "open ›"),
    ];
    let mut lines = rows
        .into_iter()
        .enumerate()
        .map(|(index, (name, value))| {
            let selected = index == settings.selected;
            Line::styled(
                format!(" {} {name:<22} {value}", if selected { "›" } else { " " }),
                if selected {
                    Style::default().add_modifier(Modifier::BOLD | Modifier::REVERSED)
                } else {
                    Style::default()
                },
            )
        })
        .collect::<Vec<_>>();
    lines.push(Line::raw(""));
    let hint = |action| app.primary_key_hint(ActionContext::TerminalSettings, action);
    lines.push(Line::styled(
        clip_cells(
            &format!(
                " {}/{} select · {} change · {} close",
                hint(ActionId::NavigateUp),
                hint(ActionId::NavigateDown),
                hint(ActionId::Activate),
                hint(ActionId::Cancel)
            ),
            inner.width.into(),
        ),
        Style::default().fg(theme::colors::inactive()),
    ));
    frame.render_widget(Paragraph::new(lines).style(surface_style(app.theme)), inner);
}

pub(super) fn popover_area(anchor: Rect, width: u16, height: u16, right: bool) -> Rect {
    let width = width.min(anchor.width);
    let height = height.min(anchor.height);
    let x = if right {
        anchor.right() - width
    } else {
        anchor.x
    };
    Rect::new(x, anchor.bottom() - height, width, height)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::BambooClient;
    use crate::app::SubAgentDisplay;
    use ratatui::{backend::TestBackend, Terminal};
    use std::time::Duration;

    fn position(buffer: &Buffer, label: &str) -> Option<(u16, u16)> {
        (0..buffer.area.height).find_map(|row| {
            let text: String = (0..buffer.area.width)
                .map(|column| buffer[(column, row)].symbol())
                .collect();
            text.find(label)
                .map(|column| (text[..column].chars().count() as u16, row))
        })
    }

    async fn draw_until(
        workspace: &mut Workspace,
        terminal: &mut Terminal<TestBackend>,
        app: &App,
        wake: &Notify,
        label: &str,
    ) -> (u16, u16) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                terminal.draw(|frame| workspace.render(frame, app)).unwrap();
                workspace.check_error().unwrap();
                if let Some(position) = position(terminal.backend().buffer(), label) {
                    return position;
                }
                wake.notified().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("workspace did not render {label:?}"))
    }

    #[tokio::test]
    async fn native_shell_renders_and_reconciles_toolbar_in_supported_terminals() {
        for (width, height) in [(60, 20), (120, 35)] {
            let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
            let (events, mut receiver) = mpsc::unbounded_channel();
            let wake = Arc::new(Notify::new());
            let mut workspace = Workspace::new(&app, events, wake.clone()).unwrap();
            let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
            draw_until(&mut workspace, &mut terminal, &app, &wake, "/ Functions").await;
            assert!(position(terminal.backend().buffer(), "New session").is_some());
            assert!(position(terminal.backend().buffer(), "Switch session").is_some());
            assert!(position(terminal.backend().buffer(), "← Parent").is_none());
            assert!(position(terminal.backend().buffer(), "Sub-agents").is_none());
            assert!(workspace.areas.body.get().height >= 14);

            app.chat.model = "fresh-model".into();
            draw_until(&mut workspace, &mut terminal, &app, &wake, "fresh-model").await;
            app.chat.session_id = Some("current-child-session".into());
            app.chat.parent_session_id = Some("parent-session".into());
            app.chat.sub_agents.push(SubAgentDisplay {
                child_session_id: "nested-child".into(),
                title: None,
                status: "running".into(),
                error: None,
            });
            draw_until(&mut workspace, &mut terminal, &app, &wake, "← Parent").await;
            for (label, expected) in [
                ("Settings", ActionId::OpenTerminalSettings),
                ("Switch session", ActionId::OpenSessionPicker),
                ("Sub-agents", ActionId::OpenSubagentTree),
                ("← Parent", ActionId::OpenParentSession),
            ] {
                let (column, row) = position(terminal.backend().buffer(), label)
                    .unwrap_or_else(|| panic!("{label} was clipped at {width}×{height}"));
                for kind in [
                    MouseEventKind::Down(TerminalButton::Left),
                    MouseEventKind::Up(TerminalButton::Left),
                ] {
                    workspace.handle_mouse(MouseEvent {
                        kind,
                        column,
                        row,
                        modifiers: KeyModifiers::NONE,
                    });
                }
                let event = tokio::time::timeout(Duration::from_secs(3), receiver.recv())
                    .await
                    .expect("workspace control did not emit a press");
                assert!(matches!(
                    event,
                    Some(AppEvent::WorkspaceAction(WorkspaceAction {
                        action,
                        tab: Tab::Chat,
                        ..
                    })) if action == expected
                ));
            }
        }
    }
}
