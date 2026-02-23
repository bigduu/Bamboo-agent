//! Bamboo binary entry point
//!
//! Standalone HTTP server for Bamboo

use clap::{Parser, Subcommand};
use bamboo_agent::{BambooBuilder, BambooConfig};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "bamboo")]
#[command(about = "A fully self-contained AI agent backend framework", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the Bamboo HTTP server
    Serve {
        /// Port to listen on
        #[arg(short, long, default_value = "8080")]
        port: u16,

        /// Bind address
        #[arg(short, long, default_value = "127.0.0.1")]
        bind: String,

        /// Data directory (defaults to XDG_DATA_HOME/bamboo)
        #[arg(short, long)]
        data_dir: Option<PathBuf>,

        /// Static files directory (for Docker mode)
        #[arg(short, long)]
        static_dir: Option<PathBuf>,

        /// Number of worker threads
        #[arg(short, long, default_value = "10")]
        workers: usize,
    },

    /// Show Bamboo configuration
    Config {
        /// Show config file path
        #[arg(short, long)]
        path: bool,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Initialize logging
    tracing_subscriber::fmt::init();

    match cli.command {
        Commands::Serve {
            port,
            bind,
            data_dir,
            static_dir,
            workers,
        } => {
            let mut builder = BambooBuilder::new()
                .port(port)
                .bind(&bind);

            if let Some(dir) = data_dir {
                builder = builder.data_dir(dir);
            }

            // Note: static_dir and workers need to be added to BambooConfig

            match builder.build() {
                Ok(server) => {
                    println!("Starting Bamboo server at {}", server.server_addr());
                    if let Err(e) = server.start().await {
                        eprintln!("Server error: {}", e);
                        std::process::exit(1);
                    }
                }
                Err(e) => {
                    eprintln!("Failed to build server: {}", e);
                    std::process::exit(1);
                }
            }
        }

        Commands::Config { path } => {
            if path {
                println!("{}", bamboo_agent::config::bamboo_config_file().display());
            } else {
                match BambooConfig::load() {
                    Ok(config) => {
                        println!("{}", serde_json::to_string_pretty(&config).unwrap());
                    }
                    Err(e) => {
                        eprintln!("Failed to load config: {}", e);
                        std::process::exit(1);
                    }
                }
            }
        }
    }
}
