#![allow(dead_code)]

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
mod theme;
mod ui;

#[derive(Parser)]
#[command(name = "bamboo-tui")]
#[command(about = "Terminal UI client for the Bamboo agent runtime")]
#[command(version)]
struct Cli {
    /// Bamboo server URL
    #[arg(long, default_value = "http://localhost:9562")]
    server_url: String,

    /// Session ID to resume (optional)
    #[arg(long)]
    session_id: Option<String>,

    /// Model to use
    #[arg(short, long)]
    model: Option<String>,
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

    // Run app.
    let result = app.run(&mut terminal).await;

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
