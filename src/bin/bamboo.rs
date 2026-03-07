//! Bamboo binary entry point
//!
//! Standalone HTTP server for Bamboo

use clap::{Parser, Subcommand};
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
        /// Port to listen on (overrides config file)
        #[arg(short, long)]
        port: Option<u16>,

        /// Bind address (overrides config file)
        #[arg(short, long)]
        bind: Option<String>,

        /// Data directory (overrides config file)
        #[arg(short, long)]
        data_dir: Option<PathBuf>,

        /// Static files directory (for Docker mode)
        #[arg(short, long)]
        static_dir: Option<PathBuf>,

        /// Number of worker threads (overrides config file)
        #[arg(short, long)]
        workers: Option<usize>,
    },

    /// Show Bamboo configuration
    Config {
        /// Show config file path
        #[arg(short, long)]
        path: bool,

        /// Show sensitive values (API keys, etc.)
        #[arg(long)]
        show_secrets: bool,
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
            let bamboo_home_dir = data_dir
                .clone()
                .unwrap_or_else(bamboo_agent::core::paths::resolve_bamboo_dir);
            // Stabilize the data dir for the lifetime of this process.
            bamboo_agent::core::paths::init_bamboo_dir(bamboo_home_dir.clone());
            // Keep runtime path resolution consistent: most helpers derive their base dir from
            // BAMBOO_DATA_DIR / `${HOME}/.bamboo` via `core::paths::bamboo_dir()`.
            std::env::set_var("BAMBOO_DATA_DIR", bamboo_home_dir.as_os_str());

            // Load config (with env var overrides already applied)
            // If --data-dir is specified, load from that directory.
            let mut config =
                bamboo_agent::core::Config::from_data_dir(Some(bamboo_home_dir.clone()));

            // Apply CLI argument overrides (highest priority)
            if let Some(p) = port {
                config.server.port = p;
            }
            if let Some(b) = bind {
                config.server.bind = b;
            }
            if let Some(s) = static_dir {
                config.server.static_dir = Some(s);
            }
            if let Some(w) = workers {
                config.server.workers = w;
            }

            // Map config-level worker count into an env var that the server entrypoints can
            // consume without requiring breaking signature changes.
            if workers.is_some() || std::env::var("BAMBOO_WORKERS").is_err() {
                std::env::set_var("BAMBOO_WORKERS", config.server.workers.to_string());
            }

            // Start server using the unified config
            println!("Starting Bamboo server at {}", config.server_addr());
            let result = if config.server.static_dir.is_some() {
                bamboo_agent::server::run_with_bind_and_static(
                    bamboo_home_dir,
                    config.server.port,
                    &config.server.bind,
                    config.server.static_dir.clone(),
                )
                .await
            } else {
                bamboo_agent::server::run_with_bind(
                    bamboo_home_dir,
                    config.server.port,
                    &config.server.bind,
                )
                .await
            };

            if let Err(e) = result {
                eprintln!("Failed to start server: {}", e);
                std::process::exit(1);
            }
        }

        Commands::Config { path, show_secrets } => {
            if path {
                println!(
                    "{}",
                    bamboo_agent::core::paths::config_json_path().display()
                );
            } else {
                let config = bamboo_agent::core::Config::new();
                let mut config_value = serde_json::to_value(&config).unwrap();

                if !show_secrets {
                    // Redact sensitive fields
                    if let Some(providers) = config_value.get_mut("providers") {
                        if let Some(providers_obj) = providers.as_object_mut() {
                            for (_, provider) in providers_obj.iter_mut() {
                                if let Some(provider_obj) = provider.as_object_mut() {
                                    if provider_obj.contains_key("api_key") {
                                        provider_obj.insert(
                                            "api_key".to_string(),
                                            serde_json::json!("***REDACTED***"),
                                        );
                                    }
                                }
                            }
                        }
                    }
                }

                println!("{}", serde_json::to_string_pretty(&config_value).unwrap());
            }
        }
    }
}
