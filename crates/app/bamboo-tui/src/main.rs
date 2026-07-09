use anyhow::Result;
use clap::Parser;
use crossterm::event::{DisableMouseCapture, EnableMouseCapture};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use std::io;

mod api;
mod app;
mod components;
mod event;
mod history;
mod theme;
mod ui;

#[derive(Parser)]
#[command(name = "bamboo-tui")]
#[command(about = "Terminal UI client for the Bamboo agent runtime. \
    For a loopback --server-url, use --auto-serve to auto-start `bamboo serve`.")]
#[command(version)]
struct Cli {
    /// Bamboo server URL. Defaults to the concrete loopback IPv4 (not
    /// `localhost`, which resolves to `::1` first on dual-stack hosts while the
    /// server default-binds `127.0.0.1` only → ECONNREFUSED).
    #[arg(long, default_value = "http://127.0.0.1:9562")]
    server_url: String,

    /// Session ID to resume (optional)
    #[arg(long)]
    session_id: Option<String>,

    /// Model to use
    #[arg(short, long)]
    model: Option<String>,

    /// If `--server-url` is unreachable and loopback, start a local `bamboo
    /// serve` automatically instead of asking (y/n). No effect for a remote
    /// (non-loopback) `--server-url` — that always just warns.
    #[arg(long, conflicts_with = "no_auto_serve")]
    auto_serve: bool,

    /// Never offer/auto-start a local server, even for an unreachable
    /// loopback `--server-url` — just warn, like a remote URL always does.
    #[arg(long, conflicts_with = "auto_serve")]
    no_auto_serve: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Setup terminal.
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Create client and app.
    let client = api::BambooClient::new(&cli.server_url);
    let mut app = app::App::new(client);

    // Apply CLI args.
    if let Some(session_id) = cli.session_id {
        app.chat.session_id = Some(session_id);
    }
    if let Some(model) = cli.model {
        app.chat.model = model;
    }
    let auto_serve_mode = if cli.auto_serve {
        app::AutoServeMode::Auto
    } else if cli.no_auto_serve {
        app::AutoServeMode::Off
    } else {
        app::AutoServeMode::Prompt
    };

    // Run app.
    let result = app.run(&mut terminal, auto_serve_mode).await;

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
