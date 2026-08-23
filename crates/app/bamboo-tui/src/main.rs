//! Standalone `bamboo-tui` binary — a thin clap wrapper over the `bamboo_tui`
//! library. The same TUI ships inside the main `bamboo` binary as the
//! `bamboo tui` subcommand; keep the flag surfaces in lock-step.

use anyhow::Result;
use bamboo_tui::{AutoServeMode, ThemePalette, TuiOptions};
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "bamboo-tui")]
#[command(about = "Terminal UI client for the Bamboo agent runtime. \
    For a loopback --server-url, use --auto-serve to auto-start `bamboo serve`.")]
#[command(version)]
struct Cli {
    /// Bamboo server URL. Defaults to the concrete loopback IPv4 (not
    /// `localhost`, which resolves to `::1` first on dual-stack hosts while the
    /// server default-binds `127.0.0.1` only → ECONNREFUSED).
    #[arg(long, default_value = bamboo_tui::DEFAULT_SERVER_URL)]
    server_url: String,

    /// Session ID to resume (optional)
    #[arg(long)]
    session_id: Option<String>,

    /// Model to use
    #[arg(short, long)]
    model: Option<String>,

    /// Colour palette: truecolor, system (terminal ANSI colours), or no-color.
    /// `NO_COLOR` selects no-color when this flag is omitted.
    #[arg(long)]
    theme: Option<ThemePalette>,

    /// JSON keymap override with per-context bindings, leader sequences, and
    /// unbind support. Invalid maps fall back to safe defaults.
    #[arg(long, value_name = "PATH")]
    keymap: Option<PathBuf>,

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

    let auto_serve = if cli.auto_serve {
        AutoServeMode::Auto
    } else if cli.no_auto_serve {
        AutoServeMode::Off
    } else {
        AutoServeMode::Prompt
    };

    bamboo_tui::run(TuiOptions {
        server_url: cli.server_url,
        session_id: cli.session_id,
        model: cli.model,
        auto_serve,
        theme: cli.theme,
        keymap: cli.keymap,
    })
    .await
}
