//! Bamboo TUI — a full-screen terminal client for a running Bamboo server
//! (chat, sessions, MCP, schedules, skills, config).
//!
//! This crate is both a standalone binary (`bamboo-tui`) and a library: the
//! main `bamboo` binary embeds it as the `bamboo tui` subcommand, so one
//! binary on PATH exposes serve + CLI + TUI. The entry point is [`run`] — a
//! plain `async fn` (no runtime of its own) so it composes with the caller's
//! existing tokio runtime; both binaries provide one via `#[tokio::main]`.

use anyhow::Result;
use crossterm::event::{DisableMouseCapture, EnableMouseCapture};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use std::io::{self, IsTerminal};
use std::path::PathBuf;

mod api;
mod app;
mod components;
mod event;
mod history;
mod keymap;
mod search;
mod subagents;
mod text;
mod theme;
mod ui;

pub use app::AutoServeMode;
pub use theme::ThemePalette;

/// Default `--server-url`: the concrete loopback IPv4 (not `localhost`, which
/// resolves to `::1` first on dual-stack hosts while the server default-binds
/// `127.0.0.1` only → ECONNREFUSED).
pub const DEFAULT_SERVER_URL: &str = "http://127.0.0.1:9562";

/// Options for [`run`] — the library-level mirror of the CLI flags shared by
/// the standalone `bamboo-tui` binary and the `bamboo tui` subcommand.
pub struct TuiOptions {
    /// Base URL of the Bamboo server to talk to (see [`DEFAULT_SERVER_URL`]).
    pub server_url: String,
    /// Session ID to resume on startup (optional).
    pub session_id: Option<String>,
    /// Model override for the chat (optional).
    pub model: Option<String>,
    /// How an unreachable loopback `server_url` is handled at startup:
    /// spawn a local `bamboo serve` automatically, offer y/n, or never.
    pub auto_serve: AutoServeMode,
    /// Terminal colour strategy. `None` preserves true colour unless the
    /// conventional `NO_COLOR` environment variable is present.
    pub theme: Option<ThemePalette>,
    /// Optional JSON keymap override. Invalid maps are reported inside the
    /// TUI and fall back atomically to the conflict-safe defaults.
    pub keymap: Option<PathBuf>,
}

impl Default for TuiOptions {
    fn default() -> Self {
        Self {
            server_url: DEFAULT_SERVER_URL.to_string(),
            session_id: None,
            model: None,
            auto_serve: AutoServeMode::Prompt,
            theme: None,
            keymap: None,
        }
    }
}

/// Run the full-screen TUI until the user quits.
///
/// Requires an interactive terminal: on a non-TTY stdin/stdout this fails
/// closed with a clear error *before* touching terminal state — entering raw
/// mode + the alternate screen on a pipe would only garble the caller's
/// output. On a TTY it sets the terminal up, drives the app, and restores the
/// terminal (raw mode off, main screen back) before returning.
pub async fn run(opts: TuiOptions) -> Result<()> {
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        anyhow::bail!("bamboo tui requires an interactive terminal (stdin/stdout is not a TTY)");
    }

    let palette =
        theme::resolve_initial_palette(opts.theme, std::env::var_os("NO_COLOR").is_some());
    let keymap_path = opts
        .keymap
        .or_else(|| std::env::var_os("BAMBOO_TUI_KEYMAP").map(PathBuf::from));
    let (keymap, keymap_warning) = keymap::Keymap::load(keymap_path.as_deref());

    // Setup terminal.
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Create client and app.
    let client = api::BambooClient::new(&opts.server_url);
    let mut app = app::App::new(client);
    app.set_theme(palette);
    app.set_keymap(keymap, keymap_warning);

    // Apply options.
    if let Some(session_id) = opts.session_id {
        app.chat.session_id = Some(session_id);
    }
    if let Some(model) = opts.model {
        app.chat.model = model;
    }

    // Run app.
    let result = app.run(&mut terminal, opts.auto_serve).await;

    // Restore terminal.
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    result
}
